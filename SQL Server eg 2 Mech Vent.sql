---**************************************************************************************************************************************************************************
--Author: Paul Junker
--Original Date: 12/18/17
--Revision Date: 11/05/18 ver 2
--Revision Date: 06/13/19 ver 3 Added '4804'-- KP	MCP CRITICAL CARE unit
--Revision Date: 04/27/20 ver 4 Added Covid-19 units, enabled calculation BEFORE pt disch from ICU, removed NIV values, 
                              --improved trach accuracy, removed invalid episodes.
--Revision Date: 12/21/20 ver 5 Updated to account for new flowsheet value 'vent'
--Revision Date: 4/3/21 ver 6 Added HUP-Cedar ICU (dept id = 4191) and news trach flowsheet values 'TC','HFTC'.
--Revision Date: 11/2/21 ver 7 Added HUP Pavilion ICUs
--Revision Date: 2/14/22 ver 8 Added new PAH ICU 4559 PAH SICU WIDENER 3
--Revision Date: 1/31/23 ver 9 Changed to using x_adt_ip_unit_activity. Added Extubation Type & Owner. Removed HSP_ACCOUNT_ID & PAT_ID (because data not always valid)
		--changes implemented by Kinnari Patel & Paul Junker

--This program determines when a patient is on a mechanical vent. The final output is in the table: X_ICU_MECHANICAL_VENT. It runs daily.

/**************************** This program implements the below algorithm developed by Steve Gudowski, Julie Jablonski, Corey Chivers, Barry Fuchs, and Paul Junker ************************************
Patient Selection
1. Patients were in an ICU and considered 'Critcal Care' based on each entities' rules
2. Admitted >= 7/1/2017
3. Patient had invasive mechanical ventilation
4. Patient has been extubated (ie, currently ventilated patients are excluded)
   Note Trach patients need to be out of the ICU for 48 hrs before being processed to allow time for the "48-hr rule"

Tracheostomy Patient Flag
Flag patient as having a Tracheal Tube by 
1. IP_LDA_NOADDSINGLE.FLO_MEAS_ID IN (7070173, 1607070173) “Surgical Airway”
2. OR Oxygen Device like ('tracheal collar%','TC','HFTC') occurs 6+ times (the LDA data is often lacking)
3. Patients that have a trach PRIOR to admission are particularly tricky & this program attempts to correctly capture their data

Vent ON timestamp: 
1. 3040104328        Ventilator initiated = Yes
2. OR 3040311130         Vent Mode is not null (FYI okay if value is “SPONT”), and NOT IN ('NIV/PC','NIV/PS','NIPPV')
3. OR 1120100067        Oxygen Device  in ("ventilator", "Sipap") (may add a couple others for ICN in the future)
Note - we need TWO "ON" entries in a row to turn vent on. Only one "ON" entry surrounded by two 'off' entries is considered a data entry error.
Note - Mechanical vent intubation/extubation orders do not have reliable statuses (IE completed vs. canceled) or time stamps. They should not be used.
 
Vent OFF timestamp:
Non-Tracheostomy Patient
1. 3040104329        Ventilator Off        = Yes
2. OR 1120100067        Oxygen Device not in ("ventilator", "Sipap")
3. OR 3040311130         Vent Mode  IN ('NIV/PC','NIV/PS','NIPPV'). These are non-invasive ventilations
4. OR if no end time & patient Discharged, use discharge datetime (this captures patients that die)
Note - we need TWO "OFF" entries in a row to turn vent off. Only one "OFF" entry surrounded by two 'on' entries is considered a data entry error.

Vent OFF timestamp (TRACH):
Tracheostomy Patient
1. 3040104329        Ventilator Off        = Yes 
2. OR 1120100067        Oxygen Device not it ("ventilator", "Sipap")
3. OR 3040311130         Vent Mode IN ('NIV/PC','NIV/PS','NIPPV'). These are non-invasive ventilations
4. OR if no end time & patient Discharged, use discharge datetime (this captures patients that die)
If patient returns to being on a ventilator <= 48 hrs, ignore the off time (and count the time "off" the vent as being "on" the vent) 

OTHER FLOWSHEET SELECTION CRITERIA
Only flowsheet rows >= the patient's admission to the ICU are considered. Emergency Room & OR entries prior to ICU admission are excluded.
Only patients ADMITTED >= 7/1/17 (Final Epic live date) are included
"SPONT" entries are ignored if there is another flowsheet row with exact same timestamp
Any other flowsheet entry with the same timestamp as the "VENT OFF" is ignored
The last vent OFF entry for an EXPIRED patient IS USED even if there are not two "off" entries in a row
The last vent OFF entry for an EXPIRED TRACH patient IS USED and the 48-hr rule is not applied in this case
Flowsheet rows with timestamps AFTER discharge dttm are removed
Episodes with no documentation by an RT are removed as the patient was usually not truly on a vent
Episodes without a trach, with <=2hr duration, no vent init record, no vent off record, no O2 device = ventilator & no ETT LDA record are removed. (approx 0.4% of cases).
*/

--*******   SET THE BEGINNING AND ENDING DISCHARGE DATES   **************************************************************************************
--We process data for last 3 days.
use clarity;

SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED

DECLARE @BEG_DISCH_DATE AS DATE = dateadd(DAY, -4, cast(getdate() as date))  
DECLARE @END_DISCH_DATE AS DATE = cast(getdate() as date)					 --This gets pts discharged as of yesterday (b/c of Clarity loading schedule)


--*********************************** Create list of patients discharged from hosp OR ICU ****************************** 
--Get ICU patients, the beginning of their FIRST visit to the ICU, and the end of their LAST visit
IF OBJECT_ID('tempdb..#PJ_MV_ICU_PT_LIST') IS NOT NULL BEGIN DROP TABLE #PJ_MV_ICU_PT_LIST END
select x.PAT_ENC_CSN
	, min(x.UNIT_IN_DTTM) as MIN_IN_DTTM
	, cast(null as smalldatetime) as MAX_OUT_DTTM  --can't use max b/c pts in a unit have nulls for out_dttm. Will update next step.
	, sub.HOSP_DISCH_TIME
	, max(x.line) as max_icu_line  --to update MAX_OUT_DTTM
into #PJ_MV_ICU_PT_LIST
from clarity_custom_tables.dbo.X_ADT_IP_UNIT_ACTIVITY x
	join 
	(
	select PAT_ENC_CSN, peh.HOSP_DISCH_TIME, max(adt.line) as max_line
	from clarity_custom_tables.dbo.X_ADT_IP_UNIT_ACTIVITY adt
		join PAT_ENC_HSP peh on adt.PAT_ENC_CSN = peh.PAT_ENC_CSN_ID
	where 1=1
		and adt.UNIT_STAY_TYPE = 'CRITICAL CARE'
		and peh.HOSP_ADMSN_TIME >= '3/4/2017'
		and
		--we capture patients if there has been any movemement
		(
		peh.HOSP_DISCH_TIME between @BEG_DISCH_DATE AND DATEADD(d, 1, @END_DISCH_DATE)  --discharged patients
		or adt.UNIT_IN_DTTM between @BEG_DISCH_DATE AND DATEADD(d, 1, @END_DISCH_DATE)  --patients admitted to the ICU
		or adt.UNIT_OUT_DTTM between @BEG_DISCH_DATE AND DATEADD(d, 1, @END_DISCH_DATE)  --patients discharged from ICU
		or adt.UNIT_OUT_DTTM is null  --patients still in the ICU (they could have been admitted long ago)
		)
	group by adt.PAT_ENC_CSN, peh.HOSP_DISCH_TIME
	) sub on x.PAT_ENC_CSN = sub.PAT_ENC_CSN
where 1=1
	and x.UNIT_STAY_TYPE = 'CRITICAL CARE'
group by x.PAT_ENC_CSN, sub.HOSP_DISCH_TIME

--update the max out dttm
update #PJ_MV_ICU_PT_LIST
set MAX_OUT_DTTM = adt.UNIT_OUT_DTTM
from #PJ_MV_ICU_PT_LIST x
	join clarity_custom_tables.dbo.X_ADT_IP_UNIT_ACTIVITY adt on x.PAT_ENC_CSN = adt.PAT_ENC_CSN and x.max_icu_line = adt.LINE


--************************************************ Get flowsheet data  *****************************************************************************************************
IF OBJECT_ID('tempdb..#PJ_MV_FLWSHT_DATA') IS NOT NULL BEGIN DROP TABLE #PJ_MV_FLWSHT_DATA END
SELECT DISTINCT
		HSP.PAT_ENC_CSN_ID
		--,HSP.HSP_ACCOUNT_ID
		,HSP.PAT_ID
		,HSP.HOSP_ADMSN_TIME
		,HSP.HOSP_DISCH_TIME
		,FMEA.FLO_MEAS_ID
		,FDAT.DISP_NAME 
		,FMEA.MEAS_VALUE 
		,FMEA.RECORDED_TIME
   		,CASE
			WHEN DISP_NAME = 'Ventilator initiated' AND MEAS_VALUE = 'Yes' THEN 1							--vent on (1)
			--WHEN DISP_NAME = 'Vent Mode'			AND MEAS_VALUE is not null THEN 1	--removed 2/12/20 b/c NIV vent values are now being captured
			WHEN DISP_NAME = 'Vent Mode'			AND MEAS_VALUE NOT IN ('NIV/PC','NIV/PS','NIPPV') THEN 1		--vent on (1)
			WHEN DISP_NAME = 'Vent Mode'			AND MEAS_VALUE IN ('NIV/PC','NIV/PS','NIPPV') THEN -1			--vent off (-1). This is non-invasive vent.
			WHEN DISP_NAME =  'Oxygen Device'		AND MEAS_VALUE IN ('ventilator','vent'/*,'Sipap'*/) THEN 1			--vent on (1)
			WHEN DISP_NAME =  'Ventilator Off'		AND MEAS_VALUE = 'Yes' THEN -1							--vent off (-1)
			WHEN DISP_NAME =  'Oxygen Device'		AND MEAS_VALUE NOT IN ('ventilator','vent','Sipap') THEN -1	--vent on (1)
			ELSE 0			--This would be a flowsheet value we did not code for. I don't want nulls.
			END AS "VENT_ON1_OFFNEG1"
		,PREV_VENT_ON1_OFFNEG1 = 0
		,NEXT_VENT_ON1_OFFNEG1 = 0
		,TOTAL_VENT_STATUS = 0
		,CAST(NULL as DATETIME) as "MIN_IN_DTTM"  
		,CASE
			WHEN FMEA.RECORDED_TIME < icu_pts.MIN_IN_DTTM THEN 'Exclude: Before Pt in ICU'    --Marks lines that occurred before pt admitted to icu, such as in ED
			ELSE CAST(NULL as varchar(35))
			END AS "LINE_COMMENT"
		,CASE
			WHEN HSP.DISCH_DISP_C IN (10, 71, 8, 35) 
				AND HSP.HOSP_DISCH_TIME IS NOT NULL --sometimes the disch disp is filled in when a pt is IN HOUSE. May be cancelled discharges?
				THEN 1   
			ELSE 0
			END AS "EXPIRED_HOSPICE_FLAG"
		,icu_pts.MAX_OUT_DTTM
INTO #PJ_MV_FLWSHT_DATA
FROM   
		IP_FLWSHT_REC AS FREC2
		INNER JOIN IP_FLWSHT_MEAS AS FMEA ON FMEA.FSD_ID = FREC2.FSD_ID and FMEA.MEAS_VALUE is not null 
		INNER JOIN IP_FLO_GP_DATA AS FDAT ON FDAT.FLO_MEAS_ID = FMEA.FLO_MEAS_ID
		INNER JOIN PAT_ENC_HSP HSP ON HSP.INPATIENT_DATA_ID = FREC2.INPATIENT_DATA_ID
		INNER JOIN clarity_custom_tables.dbo.X_ADT_IP_UNIT_ACTIVITY ADT ON HSP.PAT_ENC_CSN_ID = ADT.PAT_ENC_CSN AND  adt.UNIT_STAY_TYPE ='CRITICAL CARE'
		JOIN #PJ_MV_ICU_PT_LIST icu_pts ON HSP.PAT_ENC_CSN_ID = icu_pts.PAT_ENC_CSN
			
WHERE 1=1
	AND FMEA.FLO_MEAS_ID in('3040311130','1120100067','3040104328','3040104329')
	AND FMEA.RECORDED_TIME <= COALESCE(icu_pts.HOSP_DISCH_TIME, icu_pts.MAX_OUT_DTTM, getdate())  --avoid records after "discharge" time stamp
ORDER BY HSP.PAT_ENC_CSN_ID, FMEA.RECORDED_TIME
GO

----     Get data for extubated date/time documentation     ----
--get extubated date
IF OBJECT_ID('tempdb..#pj_extubated_dttm') IS NOT NULL BEGIN DROP TABLE #pj_extubated_dttm END --455021
SELECT DISTINCT
       HSP.PAT_ENC_CSN_ID
	   ,HSP.PAT_ID
       ,HSP.HOSP_ADMSN_TIME
       ,HSP.HOSP_DISCH_TIME
       ,FMEA.FLO_MEAS_ID
       ,FDAT.DISP_NAME 
       ,FMEA.MEAS_VALUE 
       ,FMEA.RECORDED_TIME
	   ,VENT_ON1_OFFNEG1 = -1
	   ,PREV_VENT_ON1_OFFNEG1 = 0
		,NEXT_VENT_ON1_OFFNEG1 = 0
		,TOTAL_VENT_STATUS = 0
		,CAST(NULL as DATETIME) as "MIN_IN_DTTM"  
		,CASE
			WHEN FMEA.RECORDED_TIME < HSP.HOSP_ADMSN_TIME THEN 'Exclude: Before Pt Admitted'    --Marks lines that occurred before pt admitted to icu, such as in ED
			ELSE CAST(NULL as varchar(35))
			END AS "LINE_COMMENT"
		
		,CASE
			WHEN HSP.DISCH_DISP_C IN (10, 71, 8, 35) 
				AND HSP.HOSP_DISCH_TIME IS NOT NULL --sometimes the disch disp is filled in when a pt is IN HOUSE. May be cancelled discharges?
				THEN 1   
			ELSE 0
			END AS "EXPIRED_HOSPICE_FLAG"
		,icu_pts.MAX_OUT_DTTM
		,d.CALENDAR_DT as meas_value_extub_date
		,cast(null as varchar(10)) as meas_value_extub_time_raw
		,cast(null as smalldatetime) as meas_value_extub_time_revised
		,fmea.FSD_ID
INTO #pj_extubated_dttm
FROM   
       IP_FLWSHT_REC AS FREC2
       INNER JOIN IP_FLWSHT_MEAS AS FMEA ON FMEA.FSD_ID = FREC2.FSD_ID and FMEA.MEAS_VALUE is not null 
       INNER JOIN IP_FLO_GP_DATA AS FDAT ON FDAT.FLO_MEAS_ID = FMEA.FLO_MEAS_ID
       INNER JOIN PAT_ENC_HSP HSP ON HSP.INPATIENT_DATA_ID = FREC2.INPATIENT_DATA_ID
       INNER JOIN clarity_custom_tables.dbo.X_ADT_IP_UNIT_ACTIVITY ADT ON HSP.PAT_ENC_CSN_ID = ADT.PAT_ENC_CSN AND  ADT.UNIT_STAY_TYPE ='CRITICAL CARE'
	  JOIN #PJ_MV_ICU_PT_LIST icu_pts ON HSP.PAT_ENC_CSN_ID = icu_pts.PAT_ENC_CSN
	   JOIN DATE_DIMENSION d on FMEA.MEAS_VALUE = d.EPIC_DTE  --to convert date integer value to true date
WHERE 1=1
	AND FMEA.FLO_MEAS_ID in('19943')
	AND FMEA.RECORDED_TIME <= COALESCE(icu_pts.HOSP_DISCH_TIME, icu_pts.MAX_OUT_DTTM, getdate())  --avoid records after "discharge" time stamp
ORDER BY HSP.PAT_ENC_CSN_ID, FMEA.RECORDED_TIME
GO

--update extubated time
update #pj_extubated_dttm
set meas_value_extub_time_raw = FMEA.MEAS_VALUE,
	meas_value_extub_time_revised = 
		--sometimes the minute calculation will return the invalid '60', so the first statment adjusts this 
		case when right('0' + convert(varchar(2), cast(round((cast(FMEA.MEAS_VALUE as numeric(16,0))/60/60 - floor(cast(FMEA.MEAS_VALUE as numeric(16,0))/60/60))*60,0) as int)),2) = '60'
			then --when minutes incorrectly = 60
				convert(smalldatetime,
				convert(char(8), meas_value_extub_date, 112)   --date
				+' '
				+cast(floor(cast(FMEA.MEAS_VALUE as numeric(16,2))/60/60) as char(8)) --+ ':00:00'  --hour
				+':59'  --minutes
				)
			else --when minutes calculation is okay
				convert(smalldatetime,
				convert(char(8), meas_value_extub_date, 112)   --date
				+' '
				+cast(floor(cast(FMEA.MEAS_VALUE as numeric(16,2))/60/60) as char(8)) --+ ':00:00'  --hour
				+':' + right('0' + convert(varchar(2), cast(round((cast(FMEA.MEAS_VALUE as numeric(16,0))/60/60 - floor(cast(FMEA.MEAS_VALUE as numeric(16,0))/60/60))*60,0) as int)),2) --minutes
				)
			end
from #pj_extubated_dttm x 
		JOIN IP_FLWSHT_MEAS AS FMEA ON x.FSD_ID = FMEA.FSD_ID 
			and FMEA.MEAS_VALUE is not null 
       		and FMEA.RECORDED_TIME = x.RECORDED_TIME
WHERE 1=1
	AND FMEA.FLO_MEAS_ID in ('3040367446')


--add extubated time to main flowsheet data table
--note, we're putting the time the RT "entered" as the "recorded time"
insert into #PJ_MV_FLWSHT_DATA 
select
	PAT_ENC_CSN_ID,		PAT_ID,	HOSP_ADMSN_TIME,	HOSP_DISCH_TIME,	FLO_MEAS_ID,	DISP_NAME,	MEAS_VALUE,	meas_value_extub_time_revised,	VENT_ON1_OFFNEG1,	PREV_VENT_ON1_OFFNEG1,	NEXT_VENT_ON1_OFFNEG1,	TOTAL_VENT_STATUS,	MIN_IN_DTTM,	LINE_COMMENT,	EXPIRED_HOSPICE_FLAG,	MAX_OUT_DTTM
from #pj_extubated_dttm

/*******************************************  Flag Invalid SPONT Entries   ************************************* 
There is a common data entry issue b/c of the way Epic works that results in incorrect SPONT entries being "accepted",
by nurses not realizing this value is hidden below the visible screen. These are signaling that patients are 
ON the vent when they are actually OFF the vent. This query DELETES those lines. 

The rule is:
If Vent Mode = "SPONT" and somthing else is entered AT THE SAME EXACT TIMESTAMP, 
exclude the SPONT record. Leave the other record. */

UPDATE #PJ_MV_FLWSHT_DATA
SET LINE_COMMENT = 'Exclude: Dup SPONT Entry'
FROM #PJ_MV_FLWSHT_DATA flw_outer
JOIN
	(
	SELECT flw_middle.PAT_ENC_CSN_ID, flw_middle.RECORDED_TIME
	FROM #PJ_MV_FLWSHT_DATA flw_middle
		JOIN
		(
			SELECT flw.PAT_ENC_CSN_ID, flw.RECORDED_TIME as "SPONT_TIMESTAMP"
			FROM #PJ_MV_FLWSHT_DATA flw
				--JOIN #PJ_MV_PT_LIST pt ON flw.PAT_ENC_CSN_ID = pt.PAT_ENC_CSN_ID    --Limits query to just our patients
			WHERE 1=1
				AND flw.DISP_NAME = 'Vent Mode'
				AND flw.MEAS_VALUE = 'SPONT'
				AND flw.LINE_COMMENT is null
		) subquery																	--gets all the timestamps for SPONT values
			on flw_middle.PAT_ENC_CSN_ID = subquery.PAT_ENC_CSN_ID
				and flw_middle.RECORDED_TIME = subquery.SPONT_TIMESTAMP
	GROUP BY flw_middle.PAT_ENC_CSN_ID, RECORDED_TIME
	HAVING COUNT(*) >= 2	--finds cases where there are more than 1 entry at the same time as the SPONT entry
	) sub_middle 
		on flw_outer.PAT_ENC_CSN_ID = sub_middle.PAT_ENC_CSN_ID
			and flw_outer.RECORDED_TIME = sub_middle.RECORDED_TIME
WHERE flw_outer.MEAS_VALUE in ('SPONT')		--we want to flag the SPONT value, not the other values
GO

/*******************************************  Flag Misleading Entries when vent turned off  *********************************** 
There is a RARE data entry practice where users are entering "ventilator" data at the SAME TIMESTAMP as the ventilator off
entry. From an EMR status, that's fine. But it's causing issues when calculating the vent times. So we are flagging those
other entries that CONFLICT with the vent off stamp. Note, we are not removing a DISP_NAME =  'Ventilator On' stamp as we 
weighted each of those equally. 

The rule is:
If DISP_NAME = "Ventilator Off" and somthing else is entered AT THE SAME EXACT TIMESTAMP, 
exclude the OTHER record. */


UPDATE #PJ_MV_FLWSHT_DATA
SET LINE_COMMENT = 'Exclude: Vent Off Overrides'
FROM #PJ_MV_FLWSHT_DATA flw_outer
JOIN
	(
	SELECT flw_middle.PAT_ENC_CSN_ID, flw_middle.RECORDED_TIME
	FROM #PJ_MV_FLWSHT_DATA flw_middle
		JOIN
		(
			SELECT flw.PAT_ENC_CSN_ID, flw.RECORDED_TIME as "VENT_OFF_TIMESTAMP"
			FROM #PJ_MV_FLWSHT_DATA flw
				--JOIN #PJ_MV_PT_LIST pt ON flw.PAT_ENC_CSN_ID = pt.PAT_ENC_CSN_ID    --Limits query to just our patients
			WHERE 1=1
				AND DISP_NAME =  'Ventilator Off'
				AND MEAS_VALUE = 'Yes'
				AND flw.LINE_COMMENT is null								--we should not pull in lines that are already excluded
		) subquery																	--gets all the timestamps for Vent Off values
			on flw_middle.PAT_ENC_CSN_ID = subquery.PAT_ENC_CSN_ID
				and flw_middle.RECORDED_TIME = subquery.VENT_OFF_TIMESTAMP
	GROUP BY flw_middle.PAT_ENC_CSN_ID, RECORDED_TIME
	HAVING COUNT(*) >= 2	--finds cases where there are more than 1 entry at the same time as the Vent Off entry
	) sub_middle 
		on flw_outer.PAT_ENC_CSN_ID = sub_middle.PAT_ENC_CSN_ID
			and flw_outer.RECORDED_TIME = sub_middle.RECORDED_TIME
WHERE 	(DISP_NAME = 'Vent Mode' AND MEAS_VALUE is not null)							--flag "on" stamp
		OR (DISP_NAME =  'Oxygen Device' AND MEAS_VALUE IN ('ventilator','Sipap'))   --flag "on" stamp
GO

/******************************************* Flag "Other" data values  ****************************************************************
--we can't determine if these are vents on or off, so we exclude them */
UPDATE #PJ_MV_FLWSHT_DATA
SET LINE_COMMENT = 'Exclude: other (comment)'
WHERE MEAS_VALUE = 'other (comment)'
GO 

/************************************************ Populate Vent Status fields  *********************************************************
--The goal of these fields is to determine if we have TWO LINES IN A ROW of the "same" result, ie, 2 vent on's or 2 vent off's.
--This prevents a single data entry error from turning the vent on or off.
--Technically, these fields track whether the line BEFORE and the line AFTER the current line were on the vent. On = 1 and Off = -1.
--All 3 fields (current, previous, next) are summed in to TOTAL_VENT_STATUS. If this field's value is >= 1,the vent is on. If it is
--<= -1, the vent is off. A single entry that does not fit the pattern will get ignored below.
*/


UPDATE #PJ_MV_FLWSHT_DATA
SET PREV_VENT_ON1_OFFNEG1 = subquery.PREV_VENT_ON1_OFFNEG1,
	NEXT_VENT_ON1_OFFNEG1 = subquery.NEXT_VENT_ON1_OFFNEG1,
	TOTAL_VENT_STATUS = subquery.VENT_ON1_OFFNEG1 + subquery.PREV_VENT_ON1_OFFNEG1 + subquery.NEXT_VENT_ON1_OFFNEG1
FROM #PJ_MV_FLWSHT_DATA JOIN
	(SELECT PAT_ENC_CSN_ID
		,FLO_MEAS_ID
		,RECORDED_TIME
		,VENT_ON1_OFFNEG1
		,LAG (VENT_ON1_OFFNEG1, 1, 0) OVER (PARTITION BY PAT_ENC_CSN_ID ORDER BY RECORDED_TIME, VENT_ON1_OFFNEG1) AS PREV_VENT_ON1_OFFNEG1  --4/23/20 re-ordered by "on/off" instead of by ID. Improved results.
		,LEAD (VENT_ON1_OFFNEG1, 1, 0) OVER (PARTITION BY PAT_ENC_CSN_ID ORDER BY RECORDED_TIME, VENT_ON1_OFFNEG1) AS NEXT_VENT_ON1_OFFNEG1
FROM #PJ_MV_FLWSHT_DATA
		WHERE LINE_COMMENT is null) subquery
	ON #PJ_MV_FLWSHT_DATA.PAT_ENC_CSN_ID = subquery.PAT_ENC_CSN_ID
		AND #PJ_MV_FLWSHT_DATA.FLO_MEAS_ID = subquery.FLO_MEAS_ID
		AND #PJ_MV_FLWSHT_DATA.RECORDED_TIME = subquery.RECORDED_TIME;
GO

/********************** Flag rows where the PREVIOUS AND the NEXT rows are DIFFERENT than current row  *************************************/
UPDATE #PJ_MV_FLWSHT_DATA
SET LINE_COMMENT = 'Exclude: Rows b4 and aft different'
WHERE 
	(TOTAL_VENT_STATUS >= 1 AND VENT_ON1_OFFNEG1 = -1)
	OR
	(TOTAL_VENT_STATUS <= -1 AND VENT_ON1_OFFNEG1 = 1)
GO

/********************** Update LAST row for EXPIRED patients  *************************************
We found that expired patients often have ONE last entry to remove them from the vent. Because of the two-in-a-row rule,
this "off" value was being ignored. These queries update the last rows to avoid that problem. */

--Change the "TOTAL" to -1 signaling that the vent should be considered "off"
UPDATE #PJ_MV_FLWSHT_DATA
SET TOTAL_VENT_STATUS = -1,
	LINE_COMMENT = 'Include: Expired Pt Last Line'
FROM #PJ_MV_FLWSHT_DATA flw  
WHERE EXPIRED_HOSPICE_FLAG = 1 
	AND RECORDED_TIME = (select max(recorded_time)			--we only want last row
						from #PJ_MV_FLWSHT_DATA flw_inner
						where flw.pat_enc_csn_id = flw_inner.pat_enc_csn_id)
	AND flw.VENT_ON1_OFFNEG1 = -1 
	AND TOTAL_VENT_STATUS >= 0
GO

--Flag the last "on" value to be excluded
UPDATE #PJ_MV_FLWSHT_DATA
SET LINE_COMMENT = 'Exclude: Last line with ON value'
FROM #PJ_MV_FLWSHT_DATA flw_outer
	JOIN
	(
	SELECT PAT_ENC_CSN_ID, RECORDED_TIME, sum(VENT_ON1_OFFNEG1) as "SUM_ON_OFF_ENTRIES", count(*) as "RowCount"
	FROM #PJ_MV_FLWSHT_DATA flw  
	WHERE EXPIRED_HOSPICE_FLAG = 1 
		AND RECORDED_TIME = (select max(recorded_time)			--we only want last row
							from #PJ_MV_FLWSHT_DATA flw_inner
							where flw.pat_enc_csn_id = flw_inner.pat_enc_csn_id)
	GROUP BY PAT_ENC_CSN_ID, RECORDED_TIME
	HAVING count(*) > 1) flw_inner  --Find cases with >1 row
	ON flw_outer.PAT_ENC_CSN_ID = flw_inner.PAT_ENC_CSN_ID AND flw_outer.RECORDED_TIME = flw_inner.RECORDED_TIME
WHERE VENT_ON1_OFFNEG1 = 1			--vent "on" rows. We want to comment these out.
GO

--************************************************ Create Patient / Admission List  ****************************************************************
IF OBJECT_ID('tempdb..#PJ_MV_PT_LIST') IS NOT NULL BEGIN DROP TABLE #PJ_MV_PT_LIST END
SELECT DISTINCT
		f.PAT_ENC_CSN_ID
		--,f.
		,f.PAT_ID
		,0 AS "TRACH_FLAG"
		,CAST(NULL as datetime) AS "TRACH_START_DTTM"
		,f.HOSP_ADMSN_TIME
		,f.HOSP_DISCH_TIME
		,f.MAX_OUT_DTTM
INTO #PJ_MV_PT_LIST
FROM #PJ_MV_FLWSHT_DATA f

--************************************************ Update Trach flag with data from LDAs, PART 1  ************************************************************
--This finds patients with a tracheostomy documented under LDAs
UPDATE #PJ_MV_PT_LIST
SET TRACH_FLAG = 1, TRACH_START_DTTM = subquery.MIN_PLACEMENT_INSTANT
FROM #PJ_MV_PT_LIST
	JOIN
		(
		SELECT lda.PAT_ENC_CSN_ID, MIN(lda.PLACEMENT_INSTANT) AS "MIN_PLACEMENT_INSTANT"
		FROM /*Clarity_Snapshot_db.dbo.*/IP_LDA_NOADDSINGLE lda
			JOIN #PJ_MV_PT_LIST pt ON lda.PAT_ENC_CSN_ID = pt.PAT_ENC_CSN_ID    --Limits query to just our patients
		WHERE 1=1
			AND lda.FLO_MEAS_ID  IN ('7070173', '1607070173')   --"Surgical Airway" = a patient with a tracheostomy
			AND lda.PLACEMENT_INSTANT is not null				--probably not needed, but avoids a null entry from crashing the program
		GROUP BY lda.PAT_ENC_CSN_ID) subquery
		ON #PJ_MV_PT_LIST.PAT_ENC_CSN_ID = subquery.PAT_ENC_CSN_ID

/************************************************ Update Trach flag with data from Flowsheets  ************************************************************
A few patients with tracheostomies do not have them listed under LDAs. These are generally patients ADMITTED with a trach. These 2 queries
address those situations.

In this query, there is NO LDA recorded, so we set the trach start time = admission dttm. */
UPDATE #PJ_MV_PT_LIST
--SET TRACH_FLAG = 1, TRACH_START_DTTM = subquery.HOSP_ADMSN_TIME
SET TRACH_FLAG = 1, TRACH_START_DTTM = First_trach_collar_time  --changed to first trach time as hosp time was turning out to be too early
FROM #PJ_MV_PT_LIST
JOIN
(
SELECT pt.PAT_ENC_CSN_ID, flw.HOSP_ADMSN_TIME, count(*) as "count", min(flw.recorded_time) as "First_trach_collar_time"
FROM #PJ_MV_FLWSHT_DATA flw
	JOIN #PJ_MV_PT_LIST pt ON flw.PAT_ENC_CSN_ID = pt.PAT_ENC_CSN_ID    --Limits query to just our patients
WHERE 1=1
	AND flw.DISP_NAME = 'Oxygen Device'
	AND (flw.MEAS_VALUE like '%tracheal collar%' or flw.MEAS_VALUE in ('TC','HFTC'))
	AND TRACH_FLAG = 0
GROUP BY pt.PAT_ENC_CSN_ID, flw.HOSP_ADMSN_TIME
HAVING COUNT(*) > 5  --added 4/22/20 to make sure trach collar listed at least 6 times to avoid documentation errors.
) subquery
ON #PJ_MV_PT_LIST.PAT_ENC_CSN_ID = subquery.PAT_ENC_CSN_ID;
GO


/*Some patients come in with a trach, and then have ANOTHER trach put in later. In these cases, the timestamp we get is 
too late. This query finds patients that have trach activity BEFORE the lda placement, and grabs that timestamp as the
start of the trach*/
UPDATE #PJ_MV_PT_LIST
SET TRACH_START_DTTM = subquery.MIN_RECORDED_TIME
FROM #PJ_MV_PT_LIST
JOIN
(SELECT pt.PAT_ENC_CSN_ID, min(flw.RECORDED_TIME) AS "MIN_RECORDED_TIME"
FROM #PJ_MV_FLWSHT_DATA flw
	JOIN #PJ_MV_PT_LIST pt ON flw.PAT_ENC_CSN_ID = pt.PAT_ENC_CSN_ID    --Limits query to just our patients
WHERE 1=1
	AND flw.DISP_NAME = 'Oxygen Device'
	AND (flw.MEAS_VALUE like '%tracheal collar%' or flw.MEAS_VALUE in ('TC','HFTC'))
	AND TRACH_FLAG = 1
	AND pt.TRACH_START_DTTM > flw.RECORDED_TIME
GROUP BY  pt.PAT_ENC_CSN_ID
) subquery
ON #PJ_MV_PT_LIST.PAT_ENC_CSN_ID = subquery.PAT_ENC_CSN_ID;
GO

/* Finally, some patients come in on a trach, and either leave with a trach or have the trach taken out while in house, and never have flowsheet rows with trach data. 
This captures those few patients ****/
--Note, we're getting an error message about Nulls being eliminated, but it's working fine.
UPDATE #PJ_MV_PT_LIST
SET TRACH_FLAG = 1, TRACH_START_DTTM = subquery.MIN_PLACEMENT_INSTANT
FROM #PJ_MV_PT_LIST x
JOIN
	(
	SELECT lda.PAT_ID, ISNULL(MIN(lda.PLACEMENT_INSTANT), pt.HOSP_ADMSN_TIME) AS "MIN_PLACEMENT_INSTANT"--if we don't know placement dttm, we set it to admit dttm
	FROM /*Clarity_Snapshot_db.dbo.*/IP_LDA_NOADDSINGLE lda
		JOIN #PJ_MV_PT_LIST pt ON lda.PAT_ID = pt.PAT_ID    --Limits query to just our patients
	WHERE 1=1
		AND lda.FLO_MEAS_ID  IN ('7070173', '1607070173')   --"Surgical Airway" = a patient with a tracheostomy
		AND (lda.PLACEMENT_INSTANT < COALESCE(pt.HOSP_DISCH_TIME, pt.MAX_OUT_DTTM, getdate())	or lda.PLACEMENT_INSTANT is null)	--Avoid FUTURE TRACH placements that are still in. Plus catch cases where placement date is not known.
		AND (lda.REMOVAL_DTTM is null or lda.REMOVAL_DTTM between pt.HOSP_ADMSN_TIME and COALESCE(pt.HOSP_DISCH_TIME, pt.MAX_OUT_DTTM, getdate()))
		AND pt.TRACH_START_DTTM is null
	GROUP BY lda.PAT_ID, pt.HOSP_ADMSN_TIME) subquery ON x.PAT_ID = subquery.PAT_ID
WHERE MIN_PLACEMENT_INSTANT < ISNULL(x.HOSP_DISCH_TIME, x.MAX_OUT_DTTM)	--needed b/c a patient can have multiple admissions, and the subquery is by PAT_ID

--************************************************ Create an EMPTY table to hold MV data  ************************************************************
IF OBJECT_ID('tempdb..#PJ_MV_OUTPUT') IS NOT NULL BEGIN DROP TABLE #PJ_MV_OUTPUT END
GO

CREATE TABLE #PJ_MV_OUTPUT
    (PAT_ENC_CSN_ID	VARCHAR(25) NOT NULL
	--,HSP_ACCOUNT_ID	VARCHAR(25) NOT NULL
	,PAT_ID			VARCHAR(25) NOT NULL
	,LINE			INT NOT NULL
	,MV_START_DTTM	DATETIME
	,MV_END_DTTM		DATETIME
	,MV_TIME_ON_VENT_HRS	DECIMAL(16,2)
	,TRACH_FLAG		INT
	,TRACH_FLAG_EPISODE INT DEFAULT 0
	,TRACH_START_DTTM DATETIME
	,HOSP			VARCHAR(25)
	,UNIT			VARCHAR(50)
	,DEPARTMENT_ID	NUMERIC(18,0)
	,UNIT_STAY_TYPE VARCHAR(50)
	,EXPIRED_HOSPICE_FLAG INT 
	,DISCH_ON_VENT_FLAG INT DEFAULT 0
	,EXTUBATION_DEPT_ID VARCHAR(10)
	,EXTUBATED_IN_ICU INT DEFAULT 0
	,PATIENT_IN_HOUSE INT DEFAULT 0
	,EXTUBATION_TYPE varchar(25)
	)

--************************************************ Run Mechanical Vent Algorithm Cursor****************************************************
IF OBJECT_ID('tempdb..#PJ_MV_OUTPUT') IS NOT NULL BEGIN TRUNCATE TABLE #PJ_MV_OUTPUT END   --generally not needed, but useful when programming just to empty table each time we run


--Declare all variables
DECLARE @CSN as VARCHAR(25); 
--DECLARE @HSP_ACCOUNT_ID as VARCHAR(25);
DECLARE @PAT_ID as VARCHAR(25);
DECLARE @HOSP_DISCH_TIME as DATETIME;
DECLARE @DISP_NAME as VARCHAR(25);
DECLARE @MEAS_VALUE as VARCHAR(25);
DECLARE @RECORDED_TIME as DATETIME;
DECLARE @TRACH_FLAG as INT;
DECLARE @TOTAL_VENT_STATUS as INT;			-->=1 means pt is on vent, <= -1 means pt is off vent
DECLARE @TRACH_START_DTTM as DATETIME;		--the time the trach was inserted or admit time if pt came in with trach
DECLARE @xVENT_STATUS as VARCHAR(1) = 'N';  --Used to track the current state of whether the vent is on or off
DECLARE @xLINE_COUNT as INT = 0;			--Used to keep track of the line number
DECLARE @CSN_PREV_RECORD as VARCHAR(25);	--Used to store previous CSN to determine if we have changed encounters
DECLARE @xLINE_COUNT_PREV_RECORD as INT;	--Used to store previous Line number in order to update correct row in #PJ_MV_OUTPUT
DECLARE @HOSP_or_UNIT_DISCH_TM_PREV_REC as DATETIME;	--Used to store previous Disch time to update end time if MV is "ON"
DECLARE @TRACH_START_DTTM_PREV_RECORD as DATETIME;  --Used to store previous Trach Start time to update trach_flag_episode if MV is "ON"
DECLARE @PATIENT_IN_HOUSE AS INT;
DECLARE @MAX_OUT_DTTM as DATETIME;				--Stores the last time pt discharged FROM ICU

DECLARE @xVENT_48HR_STATUS as INT = 0;
DECLARE @LAST_TRACH_1ST_OFF_DTTM as DATETIME; --Used to measure HOW LONG a patient has been OFF the vent. For trach patients, this needs to be 48 hours or we ignore the off line

--Set up Cursor
DECLARE @PJ_MV_CURSOR CURSOR 
SET @PJ_MV_CURSOR = CURSOR LOCAL FAST_FORWARD FOR  --LOCAL FAST_FORWARD is the least resource intensive cursor. See: https://sqlperformance.com/2012/09/t-sql-queries/cursor-options. 
SELECT
		flw.PAT_ENC_CSN_ID
	   ,flw.PAT_ID
       ,flw.HOSP_DISCH_TIME
       ,flw.DISP_NAME 
       ,flw.MEAS_VALUE 
       ,flw.RECORDED_TIME
	   ,pt.TRACH_FLAG
	   ,pt.TRACH_START_DTTM
	   ,flw.TOTAL_VENT_STATUS
	   ,PATIENT_IN_HOUSE_CURSOR = CASE WHEN flw.HOSP_DISCH_TIME IS NULL THEN 1 ELSE 0 END
	   ,pt.MAX_OUT_DTTM
FROM #PJ_MV_FLWSHT_DATA flw
	JOIN #PJ_MV_PT_LIST pt ON flw.PAT_ENC_CSN_ID = pt.PAT_ENC_CSN_ID
WHERE LINE_COMMENT is null OR LINE_COMMENT like 'Include%'
ORDER BY PAT_ENC_CSN_ID, RECORDED_TIME, DISP_NAME;
OPEN @PJ_MV_CURSOR;

--Fetch the first record
FETCH NEXT FROM @PJ_MV_CURSOR INTO @CSN,  @PAT_ID, @HOSP_DISCH_TIME, @DISP_NAME, @MEAS_VALUE, @RECORDED_TIME, @TRACH_FLAG, @TRACH_START_DTTM, @TOTAL_VENT_STATUS, @PATIENT_IN_HOUSE, @MAX_OUT_DTTM;   --fetch the 1st row

--Loop through the SECOND to the LAST record. Note, cursor loaded with next record at the bottom of the loop.
WHILE @@FETCH_STATUS = 0
BEGIN

	--Logic to follow if VENT is OFF.
	IF @xVENT_STATUS = 'N'
		BEGIN
		IF @TOTAL_VENT_STATUS >= 1   --1 means vent is now on
			BEGIN
			SET @xVENT_STATUS = 'Y';
			SET @xLINE_COUNT = @xLINE_COUNT + 1;
			INSERT INTO #PJ_MV_OUTPUT (PAT_ENC_CSN_ID,  PAT_ID, LINE, MV_START_DTTM, TRACH_FLAG, TRACH_START_DTTM, PATIENT_IN_HOUSE)
			VALUES (@CSN, @PAT_ID, @xLINE_COUNT, @RECORDED_TIME, @TRACH_FLAG, @TRACH_START_DTTM, @PATIENT_IN_HOUSE)
			--PRINT @xVENT_STATUS + ' ' +@DISP_NAME + ' ' + @MEAS_VALUE + ' ' + convert(varchar(25), @RECORDED_TIME, 120) + ' ' + 'vent turned on'
		END
		
		Goto Cont    --Move to the end of the loop so we skip the vent status = 'Y' section
		END --end of logic for when vent is OFF

	--Logic to follow if VENT is ON and patient is NOT on a trach at this time
	IF @xVENT_STATUS = 'Y' AND (@TRACH_FLAG = 0 or @RECORDED_TIME < @TRACH_START_DTTM)
		BEGIN
		--IF @DISP_NAME = 'Ventilator Off' AND @MEAS_VALUE = 'Yes'
		IF @TOTAL_VENT_STATUS <= -1   -- -1 means vent is now off
			BEGIN
			SET @xVENT_STATUS = 'N';
				--IF @RECORDED_TIME <= ISNULL(@HOSP_DISCH_TIME, @MAX_OUT_DTTM)  --*** what if I added max pat_out_dttm  --removed 4/24/20
				IF @RECORDED_TIME <= ISNULL(@HOSP_DISCH_TIME, @MAX_OUT_DTTM) OR @MAX_OUT_DTTM IS NULL --*** what if I added max pat_out_dttm
					BEGIN
					UPDATE #PJ_MV_OUTPUT SET MV_END_DTTM = @RECORDED_TIME WHERE PAT_ENC_CSN_ID = @CSN AND LINE = @xLINE_COUNT
					END
					ELSE IF @RECORDED_TIME > ISNULL(@HOSP_DISCH_TIME, @MAX_OUT_DTTM) 
					BEGIN
					UPDATE #PJ_MV_OUTPUT SET MV_END_DTTM = ISNULL(@HOSP_DISCH_TIME, @RECORDED_TIME)	--if pt is IN HOUSE & recorded time is AFTER discharge from ICU, we keep RECORDED TIME even though its later
						 WHERE PAT_ENC_CSN_ID = @CSN AND LINE = @xLINE_COUNT
					END
			END

		END --end of logic for when vent is ON

	--Logic to follow if VENT is ON and a TRACH pt AFTER TRACH TURNED ON
	IF @xVENT_STATUS = 'Y' AND @TRACH_FLAG = 1 AND @RECORDED_TIME > @TRACH_START_DTTM
		BEGIN
		 
	--Determine if vent should be turned OFF for a TRACH PATIENT. This is the FIRST RECORD vent should be off. 
	--Note, we still need to do 48 hr test
	IF @TOTAL_VENT_STATUS <= -1 AND @xVENT_48HR_STATUS = 0  -- -1 means vent is now off
		BEGIN
			SET @xVENT_48HR_STATUS = 1  --setting = 1 to track that vent is now off, but we need to wait 48hrs befor updating #PJ_MV_OUTPUT
			SET @LAST_TRACH_1ST_OFF_DTTM = @RECORDED_TIME
			Goto Cont    --Move to the end of the loop so we skip the @xVENT_48HR_STATUS = 1 section
			END
	
	--Determine if vent has been turned BACK ON for a TRACH PATIENT during the 48 test
		IF @TOTAL_VENT_STATUS >=1 
			BEGIN
			SET @xVENT_48HR_STATUS = 0;  --to end the 48 hr test
			--PRINT @xVENT_STATUS + ' ' +@DISP_NAME + ' ' + @MEAS_VALUE + ' ' + convert(varchar(25), @RECORDED_TIME, 120) + ' ' + 'On code for trach'
			Goto Cont    --Move to the end of the loop so we skip the @xVENT_48HR_STATUS = 1 section
			END
		
	--Determine if vent should be turned OFF for a TRACH PATIENT. This is AFTER it's been OFF for 48 hrs
	IF @TOTAL_VENT_STATUS <= -1 
		AND DATEDIFF(mi, @LAST_TRACH_1ST_OFF_DTTM, @RECORDED_TIME) > 2880  --2880 = 48 hrs in minutes
			BEGIN
			SET @xVENT_STATUS = 'N';
			SET @xVENT_48HR_STATUS = 0;
			UPDATE #PJ_MV_OUTPUT 
				SET MV_END_DTTM = @LAST_TRACH_1ST_OFF_DTTM, 
					TRACH_FLAG_EPISODE = 1				--means this vent EPISODE had a trach. Prior vent episodes could have been before trach was put in
				WHERE PAT_ENC_CSN_ID = @CSN AND LINE = @xLINE_COUNT
			END
		END --end of logic for when vent is ON
	
	--FETCH next record
	cont: 
		SET @CSN_PREV_RECORD = @CSN								--Used after next record fetched to determine if we have CHANGED ENCOUNTERS
		SET @xLINE_COUNT_PREV_RECORD = @xLINE_COUNT				--Used to determine exact record to update in table #PJ_MV_OUTPUT
		--SET @HOSP_or_UNIT_DISCH_TM_PREV_REC = @HOSP_DISCH_TIME		--Captures values we'll need if vent not turned "off" before patient discharged
		SET @HOSP_or_UNIT_DISCH_TM_PREV_REC = ISNULL(@HOSP_DISCH_TIME, @MAX_OUT_DTTM)		--Captures values we'll need if vent not turned "off" before patient discharged
		SET @TRACH_START_DTTM_PREV_RECORD = @TRACH_START_DTTM	--Captures values we'll need if vent not turned "off" before patient discharged
		FETCH NEXT FROM @PJ_MV_CURSOR INTO @CSN,  @PAT_ID, @HOSP_DISCH_TIME, @DISP_NAME, @MEAS_VALUE, @RECORDED_TIME, @TRACH_FLAG, @TRACH_START_DTTM, @TOTAL_VENT_STATUS, @PATIENT_IN_HOUSE, @MAX_OUT_DTTM;
		--PRINT @xVENT_STATUS + ' ' +@DISP_NAME + ' ' + @MEAS_VALUE + ' ' + convert(varchar(25), @RECORDED_TIME, 120) + ' ' + @CSN_PREV_RECORD + ' ' + @CSN + ' ' + ISNULL(convert(varchar(25), @HOSP_DISCH_TIME, 120), 'NULL_DISCH') + ' ' + ISNULL(convert(varchar(25), @HOSP_or_UNIT_DISCH_TM_PREV_REC, 120), 'null') + ' ' + 'CURRENT DATA'
	
	--if we've reached the END OF THE CURSOR, make sure vent is turned off and the patient has been DISCHARGED
	IF @@FETCH_STATUS = -1
		BEGIN 
			IF @xVENT_STATUS = 'Y' --and @HOSP_DISCH_TIME is not null --If vent is "ON", we need to update #PJ_MV_OUTPUT to turn off
			BEGIN 
				UPDATE #PJ_MV_OUTPUT 
					SET MV_END_DTTM = @HOSP_or_UNIT_DISCH_TM_PREV_REC 
						,TRACH_FLAG_EPISODE = CASE --removed 4/24/20 when including pts still in the ICU
									WHEN TRACH_FLAG = 1 and @HOSP_or_UNIT_DISCH_TM_PREV_REC > @TRACH_START_DTTM_PREV_RECORD THEN 1 --confirm trach pt and time is AFTER trach start (2nd clause prob not needed)
									ELSE 0
									END
						/*,TRACH_FLAG_EPISODE = CASE 
									WHEN TRACH_FLAG = 1 
										and MV_START_DTTM > @TRACH_START_DTTM_PREV_RECORD
										THEN 1 --confirm trach pt and time is AFTER trach start (2nd clause prob not needed)
									ELSE 0
									END */
					WHERE PAT_ENC_CSN_ID = @CSN_PREV_RECORD AND LINE = @xLINE_COUNT_PREV_RECORD			
			END
		END  --
	
	--If NEW CSN, check vent status. If vent is on and pt DISCHARGED, turn it off = disch date. Reset necessary variables.
	IF @CSN_PREV_RECORD <> @CSN
		BEGIN
			--Vent is STILL ON, so we need to send end time to discharge date
			IF @xVENT_STATUS = 'Y' --and @HOSP_DISCH_TIME is not null 
				BEGIN 
					UPDATE #PJ_MV_OUTPUT 
						SET MV_END_DTTM = @HOSP_or_UNIT_DISCH_TM_PREV_REC
							,TRACH_FLAG_EPISODE = CASE --removed 4/24/20 when including pts still in the ICU
									WHEN TRACH_FLAG = 1 
										and @HOSP_or_UNIT_DISCH_TM_PREV_REC > @TRACH_START_DTTM_PREV_RECORD THEN 1 --confirm trach pt and time is AFTER trach start (2nd clause prob not needed)
									ELSE 0
									END 
						WHERE PAT_ENC_CSN_ID = @CSN_PREV_RECORD AND LINE = @xLINE_COUNT_PREV_RECORD			
				--PRINT @xVENT_STATUS + ' ' +@DISP_NAME + ' ' + @MEAS_VALUE + ' ' + cast(@RECORDED_TIME as varchar(25)) + ' ' + 'new csn and vent on'
				END
			
			--Reset tracking variables
			--PRINT @HOSP_or_UNIT_DISCH_TM_PREV_REC
			SET @xLINE_COUNT = 0	--reset line count
			SET @xVENT_STATUS = 'N' --reset vent status
			SET @xVENT_48HR_STATUS = 0 --reset 48hr variable for TRACH pts
		END  --end of checking vent status when moving to a new patient
END;  

--Clean up the cursor
CLOSE @PJ_MV_CURSOR;
DEALLOCATE @PJ_MV_CURSOR;
GO


/****************************** Delete records where patients are still on vents ****************************************/
delete #PJ_MV_OUTPUT
where mv_end_dttm is null

/****************************** update the time on vent.  ****************************************/
--It's a lot less code to do it at the end as opposed to during each update.
UPDATE #PJ_MV_OUTPUT
SET MV_TIME_ON_VENT_HRS = ROUND(CONVERT(numeric(10,2), datediff(MI, MV_START_DTTM, MV_END_DTTM)) / 60 , 2);
GO

/****************************** Delete records where patients are still on vents ****************************************/
delete #PJ_MV_OUTPUT
where MV_TIME_ON_VENT_HRS = 0


/********************************************* Populate the Unit ****************************************/
--The below query works to update most units.
update #PJ_MV_OUTPUT
set UNIT = subquery.ADT_DEPARTMENT_NAME, DEPARTMENT_ID = subquery.ADT_DEPARTMENT_ID
,UNIT_STAY_TYPE = SUBQUERY.UNIT_STAY_TYPE
from #PJ_MV_OUTPUT mv_outer
	JOIN
	(select mv.PAT_ENC_CSN_ID, mv.MV_START_DTTM, D.DEPARTMENT_NAME ADT_DEPARTMENT_NAME, adt.ADT_DEPARTMENT_ID,adt.UNIT_STAY_TYPE
	from #PJ_MV_OUTPUT mv 
		left join clarity_custom_tables.dbo.X_ADT_IP_UNIT_ACTIVITY adt
			on mv.PAT_ENC_CSN_ID = adt.PAT_ENC_CSN
			AND mv.MV_END_DTTM > adt.UNIT_IN_DTTM
			AND mv.MV_END_DTTM <= isnull(adt.UNIT_OUT_DTTM, adt.EXTRACTION_DTTM_APPROX)
		LEFT JOIN CLARITY_DEP D ON D.DEPARTMENT_ID = ADT.ADT_DEPARTMENT_ID
	where adt.ADT_DEPARTMENT_ID is not null
	) subquery
	ON mv_outer.PAT_ENC_CSN_ID = subquery.PAT_ENC_CSN_ID
		AND mv_outer.MV_START_DTTM = subquery.MV_START_DTTM;


GO

--Find patients assigned a NON-ICU unit. 
--These are typically trach patients that are discharged <48 hrs before removal of trach. Therefore, they still fall under 48-hr rule.
IF OBJECT_ID('tempdb..#PJ_MV_OUTPUT_UNIT_ISSUE') IS NOT NULL BEGIN DROP TABLE #PJ_MV_OUTPUT_UNIT_ISSUE END
select distinct mv.PAT_ENC_CSN_ID, mv.MV_END_DTTM, mv.DEPARTMENT_ID, mv.UNIT
into #PJ_MV_OUTPUT_UNIT_ISSUE
from #PJ_MV_OUTPUT mv
where 1=1
	AND(mv.UNIT_STAY_TYPE IS null OR   mv.UNIT_STAY_TYPE <> 'CRITICAL CARE') --DEPARTMENT_ID not in (select department_id from #PJ_MV_UNIT_LIST)
GO

--Replace the non-ICU units with LAST ICU the patient was in. We use the LAST dttm instead of the first b/c we've already screened out flowsheet rows
--that occured before the patient was in the ICU and the problem now tends to be with the end of stay, so the last one is more accurate. 
--It's also a rare case that the patient was in multiple ICUs.
UPDATE #PJ_MV_OUTPUT
SET UNIT = last_icu.department_name,
	DEPARTMENT_ID = last_icu.DEPARTMENT_ID
FROM #PJ_MV_OUTPUT mv_outer
OUTER APPLY (
	select TOP 1 
		 dep.DEPARTMENT_NAME, dep.DEPARTMENT_ID
	from #PJ_MV_OUTPUT_UNIT_ISSUE mv JOIN
		clarity_custom_tables.dbo.X_ADT_IP_UNIT_ACTIVITY adt
				on mv.PAT_ENC_CSN_ID = adt.PAT_ENC_CSN
		JOIN CLARITY_DEP dep on adt.ADT_DEPARTMENT_ID = dep.DEPARTMENT_ID  AND  adt.UNIT_STAY_TYPE ='CRITICAL CARE'
	where 1=1
		AND mv.PAT_ENC_CSN_ID = mv_outer.PAT_ENC_CSN_ID
		AND mv.MV_END_DTTM = mv_outer.MV_END_DTTM
	order by adt.UNIT_OUT_DTTM DESC) last_icu
WHERE last_icu.department_name is not null;
GO


--Find patients that do NOT have a unit assigned. These are typically patients that die and have flowsheet rows existing AFTER the discharge date.
IF OBJECT_ID('tempdb..#PJ_MV_OUTPUT_UNIT_UNDETERMINED') IS NOT NULL BEGIN DROP TABLE #PJ_MV_OUTPUT_UNIT_UNDETERMINED END
select distinct mv.PAT_ENC_CSN_ID, mv.MV_END_DTTM, mv.DEPARTMENT_ID, mv.UNIT
into #PJ_MV_OUTPUT_UNIT_UNDETERMINED
from #PJ_MV_OUTPUT mv
where 1=1
	AND mv.UNIT is null;
GO

--Assign the LAST ICU the patient was in. We use the LAST dttm instead of the first b/c we've already screened out flowsheet rows
--that occured before the patient was in the ICU and the problem now tends to be with the end of stay, so the last one is more accurate. 
--It's also a rare case that the patient was in multiple ICUs.
UPDATE #PJ_MV_OUTPUT
SET UNIT = last_icu.department_name,	DEPARTMENT_ID = last_icu.DEPARTMENT_ID
FROM #PJ_MV_OUTPUT mv_outer
OUTER APPLY (
	select TOP 1 
		 dep.DEPARTMENT_NAME, dep.DEPARTMENT_ID
	from #PJ_MV_OUTPUT_UNIT_UNDETERMINED mv JOIN
		clarity_custom_tables.dbo.X_ADT_IP_UNIT_ACTIVITY adt
				on mv.PAT_ENC_CSN_ID = adt.PAT_ENC_CSN AND  ADT.UNIT_STAY_TYPE ='CRITICAL CARE'
		JOIN CLARITY_DEP dep on adt.ADT_DEPARTMENT_ID = dep.DEPARTMENT_ID
	where 1=1
		AND mv.PAT_ENC_CSN_ID = mv_outer.PAT_ENC_CSN_ID
		AND mv.MV_END_DTTM = mv_outer.MV_END_DTTM
	order by adt.UNIT_OUT_DTTM DESC) last_icu
WHERE last_icu.department_name is not null;
GO

--Update those Units where we can not accurately determine the correct unit. Should be very rare.
update #PJ_MV_OUTPUT
set UNIT = 'Undetermined', DEPARTMENT_ID = 0
where UNIT is null;
GO

/********************************************* Populate the Hospital ****************************************/
update #PJ_MV_OUTPUT
set HOSP = subquery.HOSPITAL
from #PJ_MV_OUTPUT mv_outer
	JOIN
	(select mv.PAT_ENC_CSN_ID, gploc.NAME as "HOSPITAL"
		from #PJ_MV_OUTPUT mv 
		LEFT OUTER JOIN CLARITY_DEP dep ON mv.DEPARTMENT_ID = dep.DEPARTMENT_ID
		/* Get the Parent, then the Grandparent */ 
		LEFT OUTER JOIN CLARITY_LOC loc on dep.REV_LOC_ID = loc.LOC_ID 
		LEFT OUTER JOIN CLARITY_LOC parloc ON loc.HOSP_PARENT_LOC_ID = parloc.LOC_ID--??????????????????? --parent (like 19106/19252/19251 for PAH) 
		LEFT OUTER JOIN ZC_LOC_RPT_GRP_7 gploc ON parloc.RPT_GRP_SEVEN = gploc.RPT_GRP_SEVEN--??????????? --grandparent (Entity like 30 for HUP) 
	) subquery
	ON mv_outer.PAT_ENC_CSN_ID = subquery.PAT_ENC_CSN_ID
		;
GO

/********************************************* Update the Expired Hospice Flag in MV table  ****************************************/
UPDATE #PJ_MV_OUTPUT
SET EXPIRED_HOSPICE_FLAG = subquery.EXPIRED_HOSPICE_FLAG
FROM #PJ_MV_OUTPUT mv
	JOIN
	(
	select PAT_ENC_CSN_ID, MAX(EXPIRED_HOSPICE_FLAG) as "EXPIRED_HOSPICE_FLAG"
	from #PJ_MV_FLWSHT_DATA 
	group by PAT_ENC_CSN_ID) subquery
	ON mv.PAT_ENC_CSN_ID = subquery.PAT_ENC_CSN_ID
;
GO

/***************************** Update MV End Time for TRACH pts that EXPIRE ********************************/
--Some trach patients are taken off the vent to expire. This finds those patients and updates their vent
--end time to reflect when they were taken off the vent.

IF OBJECT_ID('tempdb..#PJ_MV_EXPIRED_TRACH_PTS') IS NOT NULL BEGIN DROP TABLE #PJ_MV_EXPIRED_TRACH_PTS END
--This finds the patients
SELECT flw_outer.PAT_ENC_CSN_ID, min(flw_outer.RECORDED_TIME) as "MIN_RECORDED_TIME_VENT_OFF", subquery.MV_END_DTTM
INTO #PJ_MV_EXPIRED_TRACH_PTS
FROM #PJ_MV_FLWSHT_DATA flw_outer
	JOIN
	(
	--Get list of trach patients that expired 
	select mv.PAT_ENC_CSN_ID, mv.MV_START_DTTM, mv.MV_END_DTTM, MAX(flw.RECORDED_TIME) as MAX_RECORDED_TIME_VENT_ON
	from #PJ_MV_OUTPUT mv
		JOIN /*Clarity_Snapshot_db.dbo.*/PAT_ENC_HSP hsp 
			ON mv.PAT_ENC_CSN_ID = hsp.PAT_ENC_CSN_ID
			AND mv.MV_END_DTTM = hsp.HOSP_DISCH_TIME    --MV "ended" at the discharge time, suggesting there might be an issue
		JOIN #PJ_MV_FLWSHT_DATA flw
			ON mv.PAT_ENC_CSN_ID = flw.PAT_ENC_CSN_ID
	where 1=1
		and mv.TRACH_FLAG = 1
		and mv.EXPIRED_HOSPICE_FLAG = 1
		and flw.TOTAL_VENT_STATUS > 0					--We only want vent on readings. In the next step, we get the min vent off AFTER this
		and (flw.LINE_COMMENT NOT LIKE 'Exclude%' or flw.LINE_COMMENT is null)
	group by mv.PAT_ENC_CSN_ID, mv.MV_END_DTTM, mv.MV_START_DTTM
	) subquery
		ON flw_outer.PAT_ENC_CSN_ID = subquery.PAT_ENC_CSN_ID
			AND flw_outer.RECORDED_TIME > subquery.MAX_RECORDED_TIME_VENT_ON --we want FIRST "off" flowsheet row AFTER last "on" value
			AND TOTAL_VENT_STATUS < 0										--we only want "OFF" values. There might be multiple.
			AND (LINE_COMMENT NOT LIKE 'Exclude%' or LINE_COMMENT is null)  --we do NOT want excluded values
GROUP BY flw_outer.PAT_ENC_CSN_ID, subquery.MV_END_DTTM
ORDER BY 1;
GO

--Update the vent end time
UPDATE #PJ_MV_OUTPUT 
SET MV_END_DTTM = trach.MIN_RECORDED_TIME_VENT_OFF  --last flowsheet row 
FROM #PJ_MV_OUTPUT mv
	JOIN #PJ_MV_EXPIRED_TRACH_PTS trach
		ON mv.PAT_ENC_CSN_ID = trach.PAT_ENC_CSN_ID
		AND mv.MV_END_DTTM = trach.MV_END_DTTM;
GO

/*********************update the time on vent. It's a lot less code to do it at the end as opposed to during each update. ***/
UPDATE #PJ_MV_OUTPUT
SET MV_TIME_ON_VENT_HRS = ROUND(CONVERT(numeric(10,2), datediff(MI, MV_START_DTTM, MV_END_DTTM)) / 60 , 2);
GO

/************************************** Remove Invalid MV Episodes - no RT Documentation ****************************************/
--We found episodes that have ONLY nursing documentation and no resp therapy documentation are usually invalid. This deletes those.

--Find cases that HAVE resp therapy documentation
IF OBJECT_ID('tempdb..#PJ_MV_RT_DOCUMENTATION_EXISTS') IS NOT NULL BEGIN DROP TABLE #PJ_MV_RT_DOCUMENTATION_EXISTS END
select distinct x.PAT_ENC_CSN_ID, x.LINE
into #PJ_MV_RT_DOCUMENTATION_EXISTS
from #PJ_MV_OUTPUT x
	 join #PJ_MV_FLWSHT_DATA y
		ON x.PAT_ENC_CSN_ID = y.PAT_ENC_CSN_ID and RECORDED_TIME between x.MV_START_DTTM and x.MV_END_DTTM
where 1=1
	and FLO_MEAS_ID in (3040104328,3040311130, 3040104329)   --RT flowsheet documentation rows
	and (LINE_COMMENT like ('Include%') or LINE_COMMENT is null )

--delete rows that do not have resp therapy documentation
delete #PJ_MV_OUTPUT
from #PJ_MV_OUTPUT x
	left join #PJ_MV_RT_DOCUMENTATION_EXISTS y on x.PAT_ENC_CSN_ID = y.PAT_ENC_CSN_ID and x.LINE = y.LINE
where y.PAT_ENC_CSN_ID is null   --Finds cases MISSING resp therapy documentation. IE, there is ONLY nursing documentation

--****************************  REMOVE PROBABLE INCORRECT EVENTS  **********************************
--These are typically NIV or erroneous documentation cases
/* Delete cases where
	i.	Duration < 2 hrs 
	ii.	No vent init documentation found within 5 min of vent start 
	iii.No vent off documentation found within 5 min of vent stop 
	iv.	No O2 device = ventilator found during mv episode
	v.	No ETT found in the LDA data during MV episode 
*/
--create temp table with cases that have MV duration <= 2.0 hours
IF OBJECT_ID('tempdb..#pj_mv_short_durations') IS NOT NULL BEGIN DROP TABLE #pj_mv_short_durations END
select *,
	cast(0 as int) as "VENT_INIT_DOCUMENTED_FLAG",
	cast(0 as int) as "VENT_OFF_DOCUMENTED_FLAG",
	cast(0 as int) as "VENT_O2_DEVICE_DOCUMENTED_FLAG",
	cast(0 as int) as "VENT_ETT_LDA_FLAG"
into #pj_mv_short_durations
from #PJ_MV_OUTPUT 
where 1=1
	and mv_time_on_vent_hrs <= 2
	and trach_flag_episode = 0

--update ventilator initiated flag if within 1 minutes of MV start time
update #pj_mv_short_durations
set VENT_INIT_DOCUMENTED_FLAG = 1
from #pj_mv_short_durations x
	join #PJ_MV_FLWSHT_DATA y on x.PAT_ENC_CSN_ID = y.PAT_ENC_CSN_ID
		and x.MV_START_DTTM between DATEADD(MINUTE, -1, y.RECORDED_TIME) and DATEADD(MINUTE, 1, y.RECORDED_TIME)
		and y.FLO_MEAS_ID = 3040104328
where 1=1
	and x.TRACH_FLAG_EPISODE = 0

--update ventilator off flag if within 1 minutes of MV start time
update #pj_mv_short_durations
set VENT_OFF_DOCUMENTED_FLAG = 1
from #pj_mv_short_durations x
	join #PJ_MV_FLWSHT_DATA y on x.PAT_ENC_CSN_ID = y.PAT_ENC_CSN_ID
		and x.MV_END_DTTM between DATEADD(MINUTE, -1, y.RECORDED_TIME) and DATEADD(MINUTE, 1, y.RECORDED_TIME)
		and y.FLO_MEAS_ID = 3040104329
where 1=1
	and x.TRACH_FLAG_EPISODE = 0
   
--update o2 device flag if ventilator found during MV episode
update #pj_mv_short_durations
set VENT_O2_DEVICE_DOCUMENTED_FLAG = 1
from #pj_mv_short_durations x
	join #PJ_MV_FLWSHT_DATA y on x.PAT_ENC_CSN_ID = y.PAT_ENC_CSN_ID
		and y.FLO_MEAS_ID = 1120100067
		and y.MEAS_VALUE = 'ventilator'
		and y.RECORDED_TIME between x.MV_START_DTTM and x.MV_END_DTTM
where 1=1
	and x.TRACH_FLAG_EPISODE = 0

--update in an ETT LDA found within 5 minutes of MV episode
UPDATE #pj_mv_short_durations
set VENT_ETT_LDA_FLAG = 1
FROM IP_LDA_NOADDSINGLE lda
	JOIN #pj_mv_short_durations pt ON lda.PAT_ID = pt.PAT_ID    --Limits query to just our patients
		and 
			(lda.PLACEMENT_INSTANT between DATEADD(MINUTE, -5, pt.mv_start_dttm) and pt.MV_END_DTTM
			or lda.REMOVAL_DTTM between pt.MV_START_DTTM and DATEADD(MINUTE, 5, pt.mv_end_dttm))
WHERE 1=1
	AND lda.FLO_MEAS_ID  IN ('1124006552')   --ETTs

--identify/delete cases that meet ALL FIVE criteria
delete #pj_mv_output
from #pj_mv_output mv
	join #pj_mv_short_durations x  --only cases < 2 hr duration and not on a trach
		on mv.pat_enc_csn_id = x.pat_enc_csn_id
		and mv.line = x.line
where 1=1 
	and x.VENT_INIT_DOCUMENTED_FLAG = 0			--no vent init documented
	and x.VENT_OFF_DOCUMENTED_FLAG = 0			--no vent off documented
	and x.VENT_O2_DEVICE_DOCUMENTED_FLAG = 0	--no O2 device = vent documented
	and x.VENT_ETT_LDA_FLAG = 0					--no ETT LDA found

/************************************** Remove Interface = Non-invasive ONLY cases *****************************/
--create table of interface values
IF OBJECT_ID('tempdb..#pj_mv_noninvasive_cases') IS NOT NULL BEGIN DROP TABLE #pj_mv_noninvasive_cases END
SELECT 
       HSP.PAT_ENC_CSN_ID
	   ,x.LINE
	   ,x.MV_START_DTTM
	   ,x.MV_END_DTTM
	   ,FMEA.FLO_MEAS_ID
       ,FDAT.DISP_NAME 
       ,FMEA.MEAS_VALUE 
       ,MIN(FMEA.RECORDED_TIME) as MIN_RECORDED_TIME
	   ,MAX(FMEA.RECORDED_TIME) as MAX_RECORDED_TIME
INTO #pj_mv_noninvasive_cases
FROM   
       IP_FLWSHT_REC AS FREC2
       INNER JOIN IP_FLWSHT_MEAS AS FMEA ON FMEA.FSD_ID = FREC2.FSD_ID and FMEA.MEAS_VALUE is not null 
       INNER JOIN IP_FLO_GP_DATA AS FDAT ON FDAT.FLO_MEAS_ID = FMEA.FLO_MEAS_ID
       INNER JOIN PAT_ENC_HSP HSP ON HSP.INPATIENT_DATA_ID = FREC2.INPATIENT_DATA_ID
	   INNER JOIN #PJ_MV_OUTPUT x on HSP.PAT_ENC_CSN_ID = x.PAT_ENC_CSN_ID
WHERE 1=1
	AND FMEA.FLO_MEAS_ID = '3040102610'
	AND FMEA.RECORDED_TIME between x.MV_START_DTTM and x.MV_END_DTTM
GROUP BY HSP.PAT_ENC_CSN_ID, x.LINE, x.MV_START_DTTM, x.MV_END_DTTM, FMEA.FLO_MEAS_ID, FDAT.DISP_NAME, FMEA.MEAS_VALUE 

--delete cases that had ANY invasive interface values. Note, often cases will have both "invasive" and 
--"non-invasive" values. Usually those are invasive ventilations, and the LAST entry is non-invasive. 
DELETE #pj_mv_noninvasive_cases
FROM
	#pj_mv_noninvasive_cases x
	JOIN
		(select distinct pat_enc_csn_id, line
		from #pj_mv_noninvasive_cases 
		where meas_value = 'Invasive') sub on x.pat_enc_csn_id = sub.pat_enc_csn_id
											and x.line = sub.line
--delete cases from MV table
DELETE #PJ_MV_OUTPUT
FROM #PJ_MV_OUTPUT x
	JOIN 
	(select pat_enc_csn_id, line
	from #pj_mv_noninvasive_cases) sub on x.pat_enc_csn_id = sub.pat_enc_csn_id
											and x.line = sub.line

/************************************** Remove Invalid MV Episodes - trach's ended < 48 hrs *****************************/
--For IN-HOUSE pts, if a trach pt is transferred out of ICU, the MV episode is ended. If that was less than 2 days ago
--the 48-hr rule has not had enough time to be applied. The pt could be put back on MV. So we delete these records.
--if the patient stays off the vent 2 more days, we'll capture them at that point.
delete from #PJ_MV_OUTPUT
where 1=1
	and PATIENT_IN_HOUSE = 1	--we only want in-house pts
	and TRACH_FLAG_EPISODE = 1	--we only want trach pts
	and MV_END_DTTM > DATEADD(d, -2, getdate())  --if the trach ended w/i last 2 days, not enough time has passed for 48 hr rule to be applied


/********************************************* Assign (new) LINE numbers  ****************************************/
update #PJ_MV_OUTPUT
set LINE = sub.NEW_LINE
from #PJ_MV_OUTPUT mv_outer
	JOIN
	(
	select PAT_ENC_CSN_ID, LINE as "OLD_LINE", MV_START_DTTM 
		,row_number() over (partition by PAT_ENC_CSN_ID order by MV_START_DTTM asc,PAT_ENC_CSN_ID) as NEW_LINE
	from #PJ_MV_OUTPUT mv_inner
	) sub on mv_outer.PAT_ENC_CSN_ID = sub.PAT_ENC_CSN_ID 
				and mv_outer.MV_START_DTTM = sub.MV_START_DTTM

/********************************************* Flag patients that are discharged (ie NOT extubated) on a vent  ****************************************/
UPDATE #PJ_MV_OUTPUT
SET DISCH_ON_VENT_FLAG = 1
FROM  #PJ_MV_OUTPUT x
	JOIN /*Clarity_Snapshot_db.dbo.*/PAT_ENC_HSP peh on x.PAT_ENC_CSN_ID = peh.PAT_ENC_CSN_ID
WHERE 1=1
	and x.mv_end_dttm = peh.HOSP_DISCH_TIME

/********************************************* Update unit patient in when extubated  ****************************************/
update #PJ_MV_OUTPUT
SET EXTUBATION_DEPT_ID = adt.ADT_DEPARTMENT_ID,
	EXTUBATED_IN_ICU = CASE WHEN 
						--adt.ADT_DEPARTMENT_ID in ('4041','4001','4016','4045','4030','4612','4613','4419','4421','4422','4412','4420','4485','4487','4489','4231','4135','4804'/*'4169','4170','4480','4029'*/) --added '4169','4170' vkp 4/15/2020
						--adt.ADT_DEPARTMENT_ID in (select department_id from #PJ_MV_UNIT_LIST)
						  ADT.UNIT_STAY_TYPE ='CRITICAL CARE'
						THEN 1
						ELSE 0
						END
from #PJ_MV_OUTPUT x
	join clarity_custom_tables.dbo.X_ADT_IP_UNIT_ACTIVITY adt on x.PAT_ENC_CSN_ID = adt.PAT_ENC_CSN
		and x.MV_END_DTTM > adt.UNIT_IN_DTTM and x.MV_END_DTTM <= isnull(adt.UNIT_OUT_DTTM, EXTRACTION_DTTM_APPROX)

/********************************************* Extubation Type  ****************************************/
UPDATE #PJ_MV_OUTPUT
SET EXTUBATION_TYPE = sub.MEAS_VALUE
FROM
	#PJ_MV_OUTPUT x
	OUTER APPLY
	(select top 1 fmea.MEAS_VALUE, fmea.TAKEN_USER_ID, emp.NAME
	 from PAT_ENC_HSP peh 
			JOIN IP_FLWSHT_REC frec on peh.INPATIENT_DATA_ID = frec.INPATIENT_DATA_ID
			JOIN IP_FLWSHT_MEAS fmea ON frec.FSD_ID = fmea.FSD_ID
			JOIN IP_FLO_GP_DATA fdat ON fmea.FLO_MEAS_ID = fdat.FLO_MEAS_ID
			LEFT JOIN CLARITY_EMP emp on fmea.TAKEN_USER_ID = emp.USER_ID
	where x.PAT_ENC_CSN_ID = peh.PAT_ENC_CSN_ID
		and fmea.FLO_MEAS_ID = '3040367655'    --Extubation type
		and fmea.MEAS_VALUE is not null			--there are occassional NULL entries that can prevent true entries from being captured.
		and fmea.RECORDED_TIME between x.MV_START_DTTM and dateadd(minute, 30, x.MV_END_DTTM) --look 30 minutes past "extubation" to account for small timing issues
	order by fmea.RECORDED_TIME desc) sub
GO


/********************************************* delete dup values ****************************************/
--We run a few days of data to catch any missing data if for some reason (like an upgrade) this process is not run for a day or two.
--This deletes the "duplicate" data that might already exist in the table.

DELETE FROM Clarity_Custom_Tables.dbo.X_ICU_MECHANICAL_VENT
FROM clarity_custom_tables.dbo.X_ICU_MECHANICAL_VENT x
	JOIN  #PJ_MV_OUTPUT mv ON x.PAT_ENC_CSN_ID = mv.PAT_ENC_CSN_ID;
GO


/********************************************* Append Results to permanent table ****************************************/

INSERT INTO Clarity_Custom_Tables.dbo.X_ICU_MECHANICAL_VENT 
	(
	PAT_ENC_CSN_ID
	,LINE
	,Unique_ID
	,MV_START_DTTM,MV_END_DTTM,MV_TIME_ON_VENT_HRS
	,TRACH_FLAG,	TRACH_FLAG_EPISODE
	,TRACH_START_DTTM
	,HOSP
	,UNIT	
	,DEPARTMENT_ID
	,EXPIRED_HOSPICE_FLAG
	,DISCH_ON_VENT_FLAG
	,EXTUBATION_DEPT_ID	
	,EXTUBATED_IN_ICU
	,PATIENT_IN_HOUSE
	,EXTUBATION_TYPE
	,TABLE_OWNER
	)
select 
	PAT_ENC_CSN_ID
	,LINE
	,PAT_ENC_CSN_ID + '_' + convert(varchar,LINE) AS 'Unique_ID'
	,MV_START_DTTM,MV_END_DTTM,MV_TIME_ON_VENT_HRS
	,TRACH_FLAG,	TRACH_FLAG_EPISODE
	,TRACH_START_DTTM
	,HOSP
	,UNIT	
	,DEPARTMENT_ID
	,EXPIRED_HOSPICE_FLAG
	,DISCH_ON_VENT_FLAG
	,EXTUBATION_DEPT_ID	
	,EXTUBATED_IN_ICU
	,PATIENT_IN_HOUSE
	,EXTUBATION_TYPE
	,'HUP: P. Junker' AS TABLE_OWNER
FROM #PJ_MV_OUTPUT
ORDER BY 1,2


/********************************************* Clean up temp tables that are no longer needed ****************************************/

IF OBJECT_ID('tempdb..#PJ_MV_FLWSHT_DATA') IS NOT NULL BEGIN DROP TABLE #PJ_MV_FLWSHT_DATA END
IF OBJECT_ID('tempdb..#PJ_MV_PT_LIST') IS NOT NULL BEGIN DROP TABLE #PJ_MV_PT_LIST END
IF OBJECT_ID('tempdb..#PJ_MV_OUTPUT_UNIT_ISSUE') IS NOT NULL BEGIN DROP TABLE #PJ_MV_OUTPUT_UNIT_ISSUE END
IF OBJECT_ID('tempdb..#PJ_MV_OUTPUT_UNIT_UNDETERMINED') IS NOT NULL BEGIN DROP TABLE #PJ_MV_OUTPUT_UNIT_UNDETERMINED END
IF OBJECT_ID('tempdb..#PJ_MV_RT_DOCUMENTATION_EXISTS') IS NOT NULL BEGIN DROP TABLE #PJ_MV_RT_DOCUMENTATION_EXISTS END
IF OBJECT_ID('tempdb..#PJ_MV_OUTPUT') IS NOT NULL BEGIN DROP TABLE #PJ_MV_OUTPUT END
IF OBJECT_ID('tempdb..#PJ_MV_UNIT_LIST') IS NOT NULL BEGIN DROP TABLE #PJ_MV_UNIT_LIST END
IF OBJECT_ID('tempdb..#PJ_MV_ICU_PT_LIST_INHOUSE') IS NOT NULL BEGIN DROP TABLE #PJ_MV_ICU_PT_LIST_INHOUSE END
IF OBJECT_ID('tempdb..#pj_mv_short_durations') IS NOT NULL BEGIN DROP TABLE #pj_mv_short_durations END

