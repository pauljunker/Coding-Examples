---**************************************************************************************************************************************************************************
--Author: Paul Junker
--Date: 2/11/2022
--Change Tracking
--Updated: 8/23/22 to fix duplicate issue with OP to IP combined accounts, usually PEC (perinatal evaluation center)
--Updated: 10/19/22 to capture missing floppers & process some segments less often
--Updated: 5/31/23 to improve efficiency mostly by adding indexes to temp tables & forcing one query to use an existing index

--FYI: this was not written as a stored procedure because non-IS employees do not have privlidges to create SPs, which makes testing extremely difficult

/*
DESCRIPTION
The objective of this program is:
	1) to track activity on ACUTE INPATIENT units. It captures inpatients AND all patients on IP units billed as OPs, such as observation & 23 hour stay ESRA patients
	2) track true unit admissions, discharges, unit start times, unit end times, and LOS. To do this, if a patient goes to an ancillary dept and returns to same unit
		, that time is ignored - the patient is considered on the unit the entire time.
	3) capture "churn", which is related to patient movement to ancillary departments (without impacting the above statistics)
	4) allow easy determination of the first IP department a patient goes to
	5) track patient flow from unit to unit
	6) for mixed acuity units (ie ICU & ward level), correctly identify patients most of the time using level of care as follows (as defined by entities) :
		PPMC: all patients in its ICUs are considered Critical Care
		HUP-Cedar: assign unit stay type in the ICU based on the level of care assignment. If a patient change L.O.C. during stay, stay type changes
		HUP: PAV 14 CITY ICU & PAV 8 CAMPUS ICU - assign unit_stay_type based on the level of care when patient ADMITTED to the unit (OR if there's a bed movement)
		HUP-Main & other Pavilion ICUs: all patients considered Critical Care
		PAH: assign unit_stay_type based on the level of care when patient ADMITTED to the ICU (OR if there's a bed movement)
		CCH / Princeton: assign unit stay type in the ICUs based on the level of care assignment. If a patient change L.O.C. during stay, stay type changes	
	7) for Cedar/Spruce transfers, which are really the same patient admission, these data are combined in the "HAR" fields and left separate in the "CSN" fields
	8) track ICU bounce back times, including for the Cedar/Spruce transfers
	9) allow easy identification (& exclusion of) of normal newborns. It could be argued that these would be better entirely excluded.
	10) it is valid starting with admissions 7/1/2017
	11) it runs daily

Other notes
NEXT_IP_UNIT_STAY_TYPE_HAR = for HUP/Cedar transfers, this specifies that the patient was transferred. For other combined accounts, it's the same as NEXT_IP_UNIT_STAY_TYPE_CSN

Things this program does NOT do:
	1) it does not track purely OP cases, like ED treat & street cases, OP procedures like endoscopies, OP radiology visits etc. 
		These should NEVER be included in this table.
	2) it does not include pure rehab & ltach cases. These cases are usually NOT wanted, and including them would have caused more problems than it would have helped. 
			We may revist this in the future.
	3) it does not include EDOU cases billed as OP's. We may revisit this in the future.

*/
--set statistics time, io on

use clarity;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED


--********************************   set begining & ending times   *************************************************************

DECLARE @BEG_DATE AS DATE = dateadd(day, -3, cast(getdate() as date)) 
DECLARE @END_DATE AS DATE = cast(getdate() as date)					 --By putting current date, this retrieves pts discharged as of yesterday (b/c of Clarity data pulled around midnight)
DECLARE @END_DATE_ADJUSTED AS DATE = dateadd(d, 1, @END_DATE)
DECLARE @BEG_DATE_1_YR_AGO AS DATE = dateadd(DAY, -365, @BEG_DATE)


--***********************************    Create table with list of CSNs    *********************************** 
--Get INPATIENT cases 3046
IF OBJECT_ID('tempdb..#pj_unittbl_csn_list') IS NOT NULL BEGIN DROP TABLE #pj_unittbl_csn_list END;
CREATE TABLE #pj_unittbl_csn_list (
	    pat_enc_csn numeric(18,0)
	  , hsp_account_id numeric(18,0)
	  , delete_flag int
	  , billed_as_op int
	)

--find accounts with recent activity
insert into #pj_unittbl_csn_list
select distinct x.pat_enc_csn, peh.hsp_account_id, 1 as delete_flag, 0 as billed_as_op
--into #pj_unittbl_csn_list
from V_PAT_ADT_LOCATION_HX x
	JOIN PAT_ENC_HSP peh on x.pat_enc_csn = peh.pat_enc_csn_id
	LEFT OUTER JOIN CLARITY_DEP dep ON peh.DEPARTMENT_ID = dep.DEPARTMENT_ID 
	LEFT OUTER JOIN HSP_ACCOUNT har on peh.HSP_ACCOUNT_ID = har.HSP_ACCOUNT_ID
where 
	(x.out_dttm between @BEG_DATE and @END_DATE_ADJUSTED --time they left unit in date range
		or x.in_dttm between @BEG_DATE and @END_DATE_ADJUSTED) --time they went into unit in date range
	and peh.adt_pat_class_c NOT in (103,104)  --excluding ED & EDOU b/c they are often labeled IP but are not really, but rather being associated with an IP stay b4 or aft ED visit. If truly IP, they are added later.
	and har.acct_basecls_ha_c = 1  --inpatient
	and peh.hosp_admsn_time > '7/1/2017'  --Epic go live at HUP was march 2017. Data before July-2017 is of limited quality.
	and x.in_dttm > '7/1/2017'            --had some odd (incorrect) old records this helps eliminate

CREATE INDEX pj_idx_unittbl_csn_list ON #pj_unittbl_csn_list (pat_enc_csn);

/****************************************************************************************************************************************************/
/****************************************************************************************************************************************************/
/****************************************************************************************************************************************************/
--BEGINNING: COMPARISONS TO EXISTING DATA. COMMENT OUT WHEN RUNNING HISTORICAL PULLS

--**** CAN COMMENT OUT WHEN RUNNING HISTORICAL PULLS!*******

--Find cases where the discharge date changed
if DATEPART(day, getdate()) not in (1,2)  --don't run on first 2 days of month
	INSERT INTO #pj_unittbl_csn_list
	select distinct peh.PAT_ENC_CSN_ID, peh.hsp_account_id, 1 as delete_flag, 0 as billed_as_op
	from clarity_custom_tables.dbo.x_adt_ip_unit_activity x 
		join pat_enc_hsp peh on x.pat_enc_csn = peh.PAT_ENC_CSN_ID
		left join #pj_unittbl_csn_list csn on x.pat_enc_csn = csn.PAT_ENC_CSN
	where 1=1
		and x.unit_status = 'DISCHARGED'  --existing unit flow data has patient discharged
		and x.unit_out_dttm <> peh.HOSP_DISCH_TIME	--existing unit flow data has a different discharge time
		and peh.HOSP_DISCH_TIME > dateadd(day,-45,getdate()) --patient discharged in the last 45 days
		and csn.PAT_ENC_CSN is null	--we are not already processing the csn this run

--find flippers 30 seconds
--if DATEPART(day, getdate()) in (4,11,18,25)  --only run on select days
	INSERT INTO #pj_unittbl_csn_list
	select distinct peh.PAT_ENC_CSN_ID, peh.hsp_account_id, 1 as delete_flag, 1 as billed_as_op
	from clarity_custom_tables.dbo.x_adt_ip_unit_activity x 
		join pat_enc_hsp peh on x.pat_enc_csn = peh.PAT_ENC_CSN_ID
		join hsp_account har on peh.hsp_account_id = har.HSP_ACCOUNT_ID
		left join #pj_unittbl_csn_list csn on x.pat_enc_csn = csn.PAT_ENC_CSN
	where 1=1
		and x.billed_as_op = 0			--existing data says patient billed as IP
		and har.ACCT_BASECLS_HA_C <> 1	--patient no longer actually billed as an IP
		and peh.HOSP_DISCH_TIME > dateadd(day,-180,getdate()) --patient discharged in the last 6 months
		and csn.PAT_ENC_CSN is null		--we are not already processing the csn this run

--find floppers
--if DATEPART(day, getdate()) in (4,11,19,25)  --only run on select days (note, this caused problems further in query, so removed!)
	INSERT INTO #pj_unittbl_csn_list
	select distinct peh.PAT_ENC_CSN_ID, peh.hsp_account_id, 1 as delete_flag, 0 as billed_as_op
	from clarity_custom_tables.dbo.x_adt_ip_unit_activity x 
		join pat_enc_hsp peh on x.pat_enc_csn = peh.PAT_ENC_CSN_ID
		join hsp_account har on peh.hsp_account_id = har.HSP_ACCOUNT_ID
		left join #pj_unittbl_csn_list csn on x.pat_enc_csn = csn.PAT_ENC_CSN
	where 1=1
		and x.billed_as_op = 1			--existing data says patient billed as OP
		and har.ACCT_BASECLS_HA_C = 1 	--patient now billed as an IP
		and peh.HOSP_DISCH_TIME > dateadd(day,-180,getdate()) --patient discharged in the last 6 months
		and csn.PAT_ENC_CSN is null		--we are not already processing the csn this run
	;

--END: COMPARING STORED DATA TO CURRENT DATA. 

--add csn's related to combined accounts, 2nd account combined into 1st account
INSERT INTO #pj_unittbl_csn_list --4
select distinct x.pat_enc_csn, peh.hsp_account_id, 1 as delete_flag, 0 as billed_as_op
from V_PAT_ADT_LOCATION_HX x
	JOIN PAT_ENC_HSP peh on x.pat_enc_csn = peh.pat_enc_csn_id
	LEFT OUTER JOIN CLARITY_DEP dep ON peh.DEPARTMENT_ID = dep.DEPARTMENT_ID 
	LEFT OUTER JOIN HSP_ACCOUNT har on peh.HSP_ACCOUNT_ID = har.HSP_ACCOUNT_ID
	--get combined accounts
	JOIN
	(select har.pat_id, har.PRIM_ENC_CSN_ID, har.HSP_ACCOUNT_ID, har.COMBINE_ACCT_ID, 1 as delete_flag, 0 as billed_as_op
	from hsp_account har WITH (INDEX = EIX_HSP_ACCOUNT_ADDATI)
		left join #pj_unittbl_csn_list csn on har.PRIM_ENC_CSN_ID = csn.PAT_ENC_CSN
	where  1=1
		--and ACCT_BASECLS_HA_C = 1	--only want IP's for this  --8/10/22 removed b/c we were missing OP PEC cases that were later combined
		and har.ADM_DATE_TIME > @BEG_DATE_1_YR_AGO  --used to access adm_date index. TY Erik Hossain!
		and har.COMBINE_DATE_TIME between @BEG_DATE and @END_DATE_ADJUSTED
		and csn.PAT_ENC_CSN is null	--want cases NOT already in the table
	) combined_accts on peh.HSP_ACCOUNT_ID = combined_accts.COMBINE_ACCT_ID or peh.HSP_ACCOUNT_ID = combined_accts.HSP_ACCOUNT_ID
	left join #pj_unittbl_csn_list csn on peh.PAT_ENC_CSN_ID = csn.PAT_ENC_CSN
where 1=1
	and peh.adt_pat_class_c NOT in (103,104)  --excluding ED & EDOU b/c they are often labeled IP but are not really, but rather being associated with an IP stay b4 or aft ED visit. If truly IP, they are added later.
	and har.acct_basecls_ha_c = 1  --inpatient
	and peh.hosp_admsn_time > '7/1/2017'  --Epic go live at HUP was march. Data before this date of limited quality. This is needed when doing retroactive pulls.
	and peh.hosp_admsn_time > @BEG_DATE_1_YR_AGO  --just to limit lookback period
	and x.in_dttm > '7/1/2017'            --had some odd (incorrect) old records this helps eliminate. This is needed when doing retroactive pulls.
	and csn.PAT_ENC_CSN is null				--avoid adding dups

/*
--OLD CODE
DECLARE @BEG_DATE AS DATE = '4/16/2023'   --dateadd(day, -3, cast(getdate() as date)) 
DECLARE @END_DATE AS DATE = cast(getdate() as date)					 --By putting current date, this retrieves pts discharged as of yesterday (b/c of Clarity data pulled around midnight)


INSERT INTO #pj_unittbl_csn_list --4
select distinct x.pat_enc_csn, peh.hsp_account_id, 1 as delete_flag, 0 as billed_as_op
from V_PAT_ADT_LOCATION_HX x
	JOIN PAT_ENC_HSP peh on x.pat_enc_csn = peh.pat_enc_csn_id
	LEFT OUTER JOIN CLARITY_DEP dep ON peh.DEPARTMENT_ID = dep.DEPARTMENT_ID 
	LEFT OUTER JOIN HSP_ACCOUNT har on peh.HSP_ACCOUNT_ID = har.HSP_ACCOUNT_ID
	--get combined accounts
	JOIN
	(select har.pat_id, har.PRIM_ENC_CSN_ID, har.HSP_ACCOUNT_ID, har.COMBINE_ACCT_ID, 1 as delete_flag, 0 as billed_as_op
	from hsp_account har 
		left join #pj_unittbl_csn_list csn on har.PRIM_ENC_CSN_ID = csn.PAT_ENC_CSN
	where  1=1
		--and ACCT_BASECLS_HA_C = 1	--only want IP's for this  --8/10/22 removed b/c we were missing OP PEC cases that were later combined
		and har.COMBINE_DATE_TIME between @BEG_DATE and dateadd(d, 1, @END_DATE)
		and csn.PAT_ENC_CSN is null	--want cases NOT already in the table
	) combined_accts on peh.HSP_ACCOUNT_ID = combined_accts.COMBINE_ACCT_ID or peh.HSP_ACCOUNT_ID = combined_accts.HSP_ACCOUNT_ID
	left join #pj_unittbl_csn_list csn on peh.PAT_ENC_CSN_ID = csn.PAT_ENC_CSN
where 1=1
	and peh.adt_pat_class_c NOT in (103,104)  --excluding ED & EDOU b/c they are often labeled IP but are not really, but rather being associated with an IP stay b4 or aft ED visit. If truly IP, they are added later.
	and har.acct_basecls_ha_c = 1  --inpatient
	and peh.hosp_admsn_time > '7/1/2017'  --Epic go live at HUP was march. Data before this date of limited quality. This is needed when doing retroactive pulls.
	and peh.hosp_admsn_time > dateadd(DAY, -365, @BEG_DATE)  --just to limit lookback period
	and x.in_dttm > '7/1/2017'            --had some odd (incorrect) old records this helps eliminate. This is needed when doing retroactive pulls.
	and csn.PAT_ENC_CSN is null				--avoid adding dups
*/

--add CSNs before processing time period combined accounts so we have all their data
INSERT INTO #pj_unittbl_csn_list
select peh.pat_enc_csn_id, peh.hsp_account_id, 1 as delete_flag, 0 as billed_as_op
from PAT_ENC_HSP peh
	join
	(
	select x.hsp_account_id
	from #pj_unittbl_csn_list x
		join hsp_account har on x.hsp_account_id = har.combine_acct_id  --find hars that have been combined into another account
--		left outer join CLARITY_DEP dep ON har.DISCH_DEPT_ID = dep.DEPARTMENT_ID 
	where har.acct_basecls_ha_c = 1 --sometimes one of these inp types gets billed as op. maybe floppers.
	group by x.hsp_account_id
	) combined_hars on peh.hsp_account_id = combined_hars.hsp_account_id
	left join #pj_unittbl_csn_list csnlist on peh.pat_enc_csn_id = csnlist.pat_enc_csn	
where 1=1
	and peh.adt_pat_class_c NOT in (103,104)  --excluding ED & EDOU b/c they are often labeled IP but are not really, but rather being associated with an IP stay b4 or aft ED visit
	and csnlist.pat_enc_csn is null  --only bring in CSNs not already in the table


--mark cases to delete cases with no time on an IP floor
update #pj_unittbl_csn_list
set delete_flag = 0
from #pj_unittbl_csn_list x
	join  V_PAT_ADT_LOCATION_HX adt on x.PAT_ENC_CSN = adt.PAT_ENC_CSN
	join clarity_dep d on adt.ADT_DEPARTMENT_ID = d.DEPARTMENT_ID  --make sure encounter was on a floor. 
	join pat_enc_hsp peh on x.PAT_ENC_CSN = peh.PAT_ENC_CSN_ID
where d.RPT_GRP_ELEVEN_C in (1,2,3,4) --(2,3,4,5,7) --excluding 5-rehab & 7-ltach. 1 is psych for HUP-Cedar
	or peh.adt_pat_class_c in (101,102,125)    --124 not in this list because the cases we want excluded ARE being excluded already. We also don't want 105-rehab & 122-LTACH
	or peh.adt_pat_class_c = 10014  --pysch IP cases at MCP have rpt_grp_eleven = 6 (not 1 like other hospitals) and were getting missed. This captures them.


--delete cases with no time on an IP floor. These are generally cases that did not make it to the floor for some reason. 
--Some of these will be added back later if the patient was an IP in the ED or OR, but didn't go to a floor. This allows reconciliation to total admissions.
delete #pj_unittbl_csn_list
where delete_flag = 1

--add OBSERVATION patients on an inpatient floor (usually these are flippers)
INSERT INTO #pj_unittbl_csn_list
select distinct x.pat_enc_csn, peh.hsp_account_id, 0 as delete_flag, 1 as billed_as_op--, peh.adt_pat_class_c
from V_PAT_ADT_LOCATION_HX x
	JOIN PAT_ENC_HSP peh on x.pat_enc_csn = peh.pat_enc_csn_id
	LEFT OUTER JOIN CLARITY_DEP dep ON peh.DEPARTMENT_ID = dep.DEPARTMENT_ID 
	LEFT OUTER JOIN HSP_ACCOUNT har on peh.HSP_ACCOUNT_ID = har.HSP_ACCOUNT_ID
	LEFT OUTER JOIN #pj_unittbl_csn_list csn on x.PAT_ENC_CSN =  csn.pat_enc_csn
where 1=1
	and (x.out_dttm between @BEG_DATE and @END_DATE_ADJUSTED
		or x.in_dttm between @BEG_DATE and @END_DATE_ADJUSTED)
	and har.acct_basecls_ha_c in (2,3)  --OP & ED (ED billed cases are very rare) 
	and peh.hosp_admsn_time > '7/1/2017'  --Epic go live at HUP
	and x.in_dttm > '7/1/2017'            --had some odd (incorrect) old records this helps eliminate
	and dep.RPT_GRP_ELEVEN_C in (1,2,3,4)  --patient was on an IP unit. We don't want rehab or ltach (5,7)
	and peh.adt_pat_class_c <> 138		--We don't want L&D OP which has a lot of PEC
	and csn.pat_enc_csn is null --to avoid duplication
GO

--add missed cases from history. note, a lot of these cases are floppers that were were initially BILLED as OPs, 
--then changed to IP, but never made it to an IP unit, so they were missed. IE, they maybe died in OR.
--if DATEPART(day, getdate()) in (4,11,18,25)  --only run on select days
	INSERT INTO #pj_unittbl_csn_list
	select har.prim_enc_csn_id, har.HSP_ACCOUNT_ID, 0 as delete_flag, 0 as billed_as_op
	from hsp_account har
		left join (select distinct hsp_account_id from clarity_custom_tables.dbo.X_ADT_IP_UNIT_ACTIVITY) sub on har.HSP_ACCOUNT_ID = sub.HSP_ACCOUNT_ID
		left join #pj_unittbl_csn_list csn on har.PRIM_ENC_CSN_ID = csn.pat_enc_csn
	where har.ACCT_CLASS_HA_C = 101  --avoids lots of rehab cases we don't want
		and TOT_CHGS > 1000  --avoid cases that are not really inpatients
		and har.ADM_DATE_TIME > dateadd(day, -6*30, getdate())
		and sub.HSP_ACCOUNT_ID is null --only want cases that are not already in the permanent table
		and PRIM_ENC_CSN_ID is not null --no good to us if no CSN
		and csn.pat_enc_csn is null --only want cases we have not already captured above
GO

--***********************************    Create ADT DETAIL table *********************************** 
--create detail of inp lines
IF OBJECT_ID('tempdb..#pj_adt_detail') IS NOT NULL BEGIN DROP TABLE #pj_adt_detail END;
select adt.PAT_ENC_CSN, x.HSP_ACCOUNT_ID, IN_DTTM, adt.PAT_OUT_DTTM, d.RPT_GRP_ELEVEN_C, adt.ADT_DEPARTMENT_ID
	,CAST(NULL as int) as ROWNUM
	,CAST(NULL as varchar) as UNIT_ADMIT_STATUS
	,CAST(NULL as varchar) as UNIT_DISCHARGE_STATUS
	,CAST(NULL as varchar(50)) as "UNIT_STAY_TYPE"
	,CAST(NULL as int) as "NON_IP_LOC"
	,adt2.PAT_LVL_OF_CARE_C
	,x.billed_as_op
	,cast(null as int) as "MAX_ROW_NUM"  --used to find rows that are missing a pat_out_dttm timestamp
	,peh.INP_ADM_DATE
	,gploc.name as "HOSP"
	,adt.EVENT_ID
	,cast(0 as int) as LOC_CHANGE_FLAG
into #pj_adt_detail
from #pj_unittbl_csn_list x
	join V_PAT_ADT_LOCATION_HX adt on x.PAT_ENC_CSN = adt.PAT_ENC_CSN
	join CLARITY_DEP d on d.DEPARTMENT_ID = adt.ADT_DEPARTMENT_ID 
	join CLARITY_ADT adt2 on adt.EVENT_ID = adt2.EVENT_ID
	join pat_enc_hsp peh on x.PAT_ENC_CSN = peh.PAT_ENC_CSN_ID
	/*  Get the Parent, then the Grandparent */
	LEFT OUTER JOIN CLARITY_LOC loc on d.REV_LOC_ID = loc.LOC_ID 
	LEFT OUTER JOIN CLARITY_LOC parloc ON loc.HOSP_PARENT_LOC_ID = parloc.LOC_ID                    --parent (like 19106/19252/19251 for PAH) 
	LEFT OUTER JOIN ZC_LOC_RPT_GRP_7 gploc ON parloc.RPT_GRP_SEVEN = gploc.RPT_GRP_SEVEN            --grandparent (Entity like 30 for HUP) 
where 1=1
	and d.RPT_GRP_ELEVEN_C in (1,2,3,4) 
	and IN_DTTM <> OUT_DTTM  --avoid bogus entries
	and IN_DTTM > '7/1/2017'            --had some odd (incorrect) old records this helps eliminate
GO

CREATE INDEX pj_idx_adt_detail ON #pj_adt_detail (pat_enc_csn, UNIT_DISCHARGE_STATUS, PAT_OUT_DTTM, ROWNUM, LOC_CHANGE_FLAG
	, ADT_DEPARTMENT_ID, UNIT_ADMIT_STATUS, billed_as_op, HSP_ACCOUNT_ID);

--update unit types
--HUP-Main and PPMC do NOT use level of care to specify CC stays
--CCH, PAH, MCP, HUP-Cedar, and 2 units at HUP-Pavilion DO use level of care to specify CC stays
update #pj_adt_detail
set UNIT_STAY_TYPE = CASE 
		WHEN y.RPT_GRP_ELEVEN_C = 1 THEN 'PSYCHIATRY'
		WHEN y.RPT_GRP_ELEVEN_C = 2 THEN 'WOMENS SERVICES'  --left out the ' in women's b/c it caused problems
		
		--hospitals with mixed ICU/Progressive care units
		WHEN y.RPT_GRP_ELEVEN_C = 4 
			--and x.pat_lvl_of_care_c = 37  --critical care
			and x.pat_lvl_of_care_c in (36,37)  --hospice, critical care
			and x.HOSP in ('CCH','MCP','PAH')
			THEN 'CRITICAL CARE'
		WHEN y.RPT_GRP_ELEVEN_C = 4 
			--and x.pat_lvl_of_care_c <> 37  --not critical care
			and x.pat_lvl_of_care_c not in (36,37)  --not hospice or critical care
			and x.HOSP in ('CCH','MCP','PAH')
			THEN 'MED/SURG'
		
		--mixed units at HUP
		WHEN y.RPT_GRP_ELEVEN_C = 4 
			--and x.pat_lvl_of_care_c = 37 --critical care
			and x.pat_lvl_of_care_c in (36,37)  --hospice, critical care
			and x.HOSP in ('HUP')
			and 
				(y.RPT_GRP_FOUR = 'HUP54'  --HUP-Cedar's ICU considers L.O.C.
				 or y.DEPARTMENT_ID in (4161, --HUP PAV 8 CAMPUS ICU. Per Jablonski 9/28/21 "The new HUP Pavilion, 8 Campus Cardiology ICU (H825-H848) will be a mixed acuity unit. They prefer to use Level of Care for metrics."
										4146) --Per discussion with Julie, we should also consider PAV 14 CITY ICU a mixed unit since this department_id also has ONC patients    
				 )
			THEN 'CRITICAL CARE'
		WHEN y.RPT_GRP_ELEVEN_C = 4 
			--and x.pat_lvl_of_care_c <> 37 --not critical care
			and x.pat_lvl_of_care_c not in (36,37)  --not in hospice, critical care
			and x.HOSP in ('HUP')
			and 
				(y.RPT_GRP_FOUR = 'HUP54'  --HUP-Cedar's ICU considers L.O.C.
				 or y.DEPARTMENT_ID in (4161, --HUP PAV 8 CAMPUS ICU. Per Jablonski 9/28/21 "The new HUP Pavilion, 8 Campus Cardiology ICU (H825-H848) will be a mixed acuity unit. They prefer to use Level of Care for metrics."
										4146) --Per discussion with Julie, we should also consider PAV 14 CITY ICU a mixed unit since this department_id also has ONC patients    )    --HUP PAV 8 CAMPUS ICU. Per Jablonski 9/28/21 "The new HUP Pavilion, 8 Campus Cardiology ICU (H825-H848) will be a mixed acuity unit. They prefer to use Level of Care for metrics."
				 )
			THEN 'MED/SURG'
		--PPMC & the rest of HUP
		WHEN y.RPT_GRP_ELEVEN_C = 4 --all remaining hospitals, which should only be HUP & PPMC. Any new hospitals will default to NOT considering L.O.C.
			THEN 'CRITICAL CARE'
		
		WHEN y.RPT_GRP_ELEVEN_C = 3 THEN 'MED/SURG'
		WHEN y.RPT_GRP_ELEVEN_C = 5 THEN 'REHAB'
		WHEN y.RPT_GRP_ELEVEN_C = 6 THEN 'NON-CENSUS AREAS'
		WHEN y.RPT_GRP_ELEVEN_C = 7 THEN 'LTACH'
		WHEN y.RPT_GRP_ELEVEN_C = 8 THEN 'HOSPICE'
		ELSE 'review' END
from #pj_adt_detail x
	join Clarity_Custom_Tables.dbo.x_clarity_dep_history y on x.ADT_DEPARTMENT_ID = y.DEPARTMENT_ID
	--join ##x_clarity_dep_history y on x.ADT_DEPARTMENT_ID = y.DEPARTMENT_ID
		and x.IN_DTTM between y.START_DATE and y.END_DATE
GO

--****************************      find patients in mixed units where the level of care changes during the stay    *****************
--We loop through this 9 times to pick up all possible changes. FYI - I was worried to put in a clause to keep going until there were no discrepancies left because 
--unexpected data could have lead to an infinite loop.
DECLARE @cnt INT = 0;
WHILE @cnt < 10
BEGIN
	--create table tracking ALL level of care timestamps for patients in mixed units. 6514
	IF OBJECT_ID('tempdb..#pj_adt_loc_changes_1') IS NOT NULL BEGIN DROP TABLE #pj_adt_loc_changes_1 END;
	select x.pat_enc_csn, x.event_id, x.hosp, x.adt_department_id, x.UNIT_STAY_TYPE, x.in_dttm, x.pat_out_dttm, adt.event_id as adt_event_id, adt.effective_time, adt.pat_lvl_of_care_c
		--avoid cases with the same timestamp when patient admitted to unit related to the PRIOR level of care
		, adt_unit_stay_type = case	
		when adt.pat_lvl_of_care_c = 37 then 'CRITICAL CARE'
		when adt.pat_lvl_of_care_c = 36 then 'CRITICAL CARE'  --this is hospice, but for this, we consider hospice critical care to capture them in mortality
		when adt.pat_lvl_of_care_c is null then null
		else 'MED/SURG'
		end
		, case when x.in_dttm = adt.effective_time		--the record we pulled from v_pat_adt_pt_history has the same time stamp as clarity_adt
					and x.event_id <> adt.event_id		--it was not the record pulled into v_pat_adt_pt_history. This is the one we do NOT want
					then 1
				when x.pat_out_dttm = adt.effective_time	--exclude rows with the same effective timestamp as when the patient left the unit
					then 1
			else 0 end as exclude_row 
	into #pj_adt_loc_changes_1
	from #pj_adt_detail x
		join clarity_adt adt on x.pat_enc_csn = adt.pat_enc_csn_id
			and adt.effective_time between dateadd(minute,1,x.in_dttm) --add 1 minute to avoid records with the same timestamp was pt went into unit
				and isnull(dateadd(minute,-1,x.pat_out_dttm), getdate())  --the isnull helps pull in patients currently in the department. -1 minute to avoid dup entries with same timestamp
		join Clarity_Custom_Tables.dbo.x_clarity_dep_history d on x.ADT_DEPARTMENT_ID = d.DEPARTMENT_ID
			and x.IN_DTTM between d.START_DATE and d.end_date
	where 1=1
		and d.rpt_grp_eleven_c = 4	--unit an ICU
		and (x.hosp in ('CCH'
						, 'MCP'
						--, 'PAH'				--per John Regan, we get better results when we do NOT look at unit changes that occur after the unit admission
						) 
			--or d.DEPARTMENT_ID in (4161, 4146)  --HUP Pavilion's mixed units  (Decided we did NOT want to look at L.O.C. changes after the admission or bed change, which are already captured above
			or d.RPT_GRP_FOUR = 'HUP54')		--HUP Cedar
		and adt.EVENT_SUBTYPE_C <> 2			--excluded cancelled events
	order by 1, x.in_dttm, adt.effective_time

	--cases with ACTUAL level of care changes 438
	IF OBJECT_ID('tempdb..#pj_adt_loc_changes_2') IS NOT NULL BEGIN DROP TABLE #pj_adt_loc_changes_2 END;
	select pat_enc_csn, adt_department_id, unit_stay_type, adt_unit_stay_type, in_dttm, pat_out_dttm, min(effective_time) as min_effective_time
	into #pj_adt_loc_changes_2
	from #pj_adt_loc_changes_1 x
	where unit_stay_type <> adt_unit_stay_type	--unit stay type changed
		and exclude_row = 0						--don't want rows that should be excluded
	group by pat_enc_csn, adt_department_id, unit_stay_type, adt_unit_stay_type, in_dttm, pat_out_dttm
	order by pat_enc_csn, in_dttm,  min(effective_time)

	--update LOC_CHANGE_FLAG
	update #pj_adt_detail
	set LOC_CHANGE_FLAG = 1
	from #pj_adt_detail x
		join #pj_adt_loc_changes_2 y
			on x.pat_enc_csn = y.pat_enc_csn and y.min_effective_time between x.in_dttm and isnull(x.pat_out_dttm, getdate())

	--create new lines for pts on mixed units that change L.O.C. This is the FIRST line with a new OUT dttm
	IF OBJECT_ID('tempdb..#pj_adt_loc_changes_new_lines') IS NOT NULL BEGIN DROP TABLE #pj_adt_loc_changes_new_lines END;
	select x.*, y.adt_unit_stay_type, y.min_effective_time
		, x.UNIT_STAY_TYPE as new_unit_stay_type		--no change here, but it's clearer to put in a new column
		, x.pat_lvl_of_care_c as new_pat_lvl_of_care_c		--no change here, but it's clearer to put in a new column
		, x.in_dttm as new_in_dttm						--no change here, but it's clearer to put in a new column
		, y.min_effective_time as new_pat_out_dttm		--this is changing to the effective date of the change
		, -1 as new_loc_change_flag						--just to help track that these are inserted lines
	into #pj_adt_loc_changes_new_lines
	from #pj_adt_detail x
		join #pj_adt_loc_changes_2 y
			on x.pat_enc_csn = y.pat_enc_csn and y.min_effective_time between x.in_dttm and isnull(x.pat_out_dttm, getdate())
	order by x.pat_enc_csn, x.in_dttm

	--This is the SECOND line with a new IN dttm
	insert into #pj_adt_loc_changes_new_lines
	select x.*, y.adt_unit_stay_type, y.min_effective_time
		, y.adt_unit_stay_type as new_unit_stay_type		--the NEW stay type
		, cast(-1 as int) as new_pat_lvl_of_care_c			--putting NULL in b/c capture the exact new L.O.C is harder to pull and not needed
		, y.min_effective_time as new_in_dttm				--this is changing to the effective date of the change
		, x.pat_out_dttm  as new_pat_out_dttm				--no change here, but it's clearer to put in a new column
		, -1 as new_loc_change_flag						--just to help track that these are inserted lines
	from #pj_adt_detail x
		join #pj_adt_loc_changes_2 y
			on x.pat_enc_csn = y.pat_enc_csn and y.min_effective_time between x.in_dttm and isnull(x.pat_out_dttm, getdate())
	order by x.pat_enc_csn, x.in_dttm

	--remove lines from adt_detail that we are replacing
	delete #pj_adt_detail
	where LOC_CHANGE_FLAG = 1

	--add new lines to adt_detail table
	insert into #pj_adt_detail
	select PAT_ENC_CSN,	HSP_ACCOUNT_ID,	new_in_dttm,	new_pat_out_dttm,	RPT_GRP_ELEVEN_C,	ADT_DEPARTMENT_ID,	ROWNUM,	UNIT_ADMIT_STATUS,	UNIT_DISCHARGE_STATUS,	new_UNIT_STAY_TYPE,	NON_IP_LOC,	new_PAT_LVL_OF_CARE_C,	billed_as_op,	MAX_ROW_NUM,	INP_ADM_DATE,	HOSP,	EVENT_ID,	new_LOC_CHANGE_FLAG
	from #pj_adt_loc_changes_new_lines x 

 SET @cnt = @cnt + 1;  --increment counter by 1
END;	--END OF LOOP


/*** update unit stay type */
--create temp table
IF OBJECT_ID('tempdb..#pj_unit_stay_type') IS NOT NULL BEGIN DROP TABLE #pj_unit_stay_type END;
select x.pat_enc_csn
, x.in_dttm
, x.ADT_DEPARTMENT_ID
, UNIT_ADMIT_STATUS = CASE 
		WHEN ADT_DEPARTMENT_ID <> LAG(ADT_DEPARTMENT_ID, 1, 0) OVER (PARTITION BY PAT_ENC_CSN ORDER BY IN_DTTM) THEN 'ADMIT TO UNIT'	--different dept id -> new unit admit
		WHEN ADT_DEPARTMENT_ID = LAG(ADT_DEPARTMENT_ID, 1, 0) OVER (PARTITION BY PAT_ENC_CSN ORDER BY IN_DTTM)							--same dept id, but stay type changed -> new unit admit
			AND UNIT_STAY_TYPE <> LAG(UNIT_STAY_TYPE, 1, 0) OVER (PARTITION BY PAT_ENC_CSN ORDER BY IN_DTTM) 
		THEN 'ADMIT TO UNIT' END
, UNIT_DISCHARGE_STATUS = CASE
		WHEN ADT_DEPARTMENT_ID <> LEAD(ADT_DEPARTMENT_ID, 1, 0) OVER (PARTITION BY PAT_ENC_CSN ORDER BY IN_DTTM)						--different dept id -> pt transferred out of unit
			AND pat_out_dttm is not null
			THEN 'TRANSFER FROM UNIT'
		WHEN ADT_DEPARTMENT_ID = LEAD(ADT_DEPARTMENT_ID, 1, 0) OVER (PARTITION BY PAT_ENC_CSN ORDER BY IN_DTTM)							--same dept id, but stay changed -> pt transferred out of unit
			AND UNIT_STAY_TYPE <> LEAD(UNIT_STAY_TYPE, 1, 0) OVER (PARTITION BY PAT_ENC_CSN ORDER BY IN_DTTM)
			AND pat_out_dttm is not null
			THEN 'TRANSFER FROM UNIT'
		WHEN pat_out_dttm is null
			THEN 'PATIENT IN UNIT' END 
into #pj_unit_stay_type
from #pj_adt_detail x
GO

CREATE INDEX pj_idx_unit_stay_type ON #pj_unit_stay_type (pat_enc_csn, in_dttm, ADT_DEPARTMENT_ID, UNIT_ADMIT_STATUS);

--update ##pj_adt_detail
update #pj_adt_detail
set	  UNIT_ADMIT_STATUS = y.UNIT_ADMIT_STATUS
	, UNIT_DISCHARGE_STATUS = y.UNIT_DISCHARGE_STATUS
from #pj_adt_detail x 
	join #pj_unit_stay_type y on x.PAT_ENC_CSN = y.PAT_ENC_CSN and x.IN_DTTM = y.IN_DTTM and x.ADT_DEPARTMENT_ID = y.ADT_DEPARTMENT_ID
GO

--add rows that occur BEFORE first inpatient unit, like ED, OR, IR 10120
insert into #pj_adt_detail
select adt.PAT_ENC_CSN, x.HSP_ACCOUNT_ID, adt.IN_DTTM, adt.PAT_OUT_DTTM, d.RPT_GRP_ELEVEN_C, /*adt.ADT_DEPARTMENT_NAME, */ adt.ADT_DEPARTMENT_ID
	,ROWNUM = NULL
	,UNIT_ADMIT_STATUS = CASE 
		WHEN ADT.ADT_DEPARTMENT_ID <> LAG(adt.ADT_DEPARTMENT_ID, 1, 0) OVER (PARTITION BY adt.PAT_ENC_CSN ORDER BY adt.IN_DTTM, adt.out_dttm)
			THEN 'ADMIT TO UNIT'
		END
	,UNIT_DISCHARGE_STATUS = CASE
		WHEN ADT.ADT_DEPARTMENT_ID <> LEAD(adt.ADT_DEPARTMENT_ID, 1, 0) OVER (PARTITION BY adt.PAT_ENC_CSN ORDER BY adt.IN_DTTM, adt.out_dttm)
		AND adt.pat_out_dttm is not null
			THEN 'TRANSFER FROM UNIT'
		WHEN adt.pat_out_dttm is null
			THEN 'PATIENT IN UNIT'
		END
	,UNIT_STAY_TYPE = 'NON IP UNIT'
	,NON_IP_LOC = NULL
	,PAT_LVL_OF_CARE_C = NULL  
	,x.billed_as_op
	,cast(null as int) as "MAX_ROW_NUM"  --used to find rows that are missing a pat_out_dttm timestamp
	,peh.INP_ADM_DATE
	,gploc.NAME as "HOSP"
	,cast(null as numeric) as event_id		--not needed for these lines of data, but included to keep columns consistent
	,cast(null as int) as loc_change_flag	--not needed for these lines of data, but included to keep columns consistent
from #pj_unittbl_csn_list x
	join V_PAT_ADT_LOCATION_HX adt on x.PAT_ENC_CSN = adt.PAT_ENC_CSN
	join CLARITY_DEP d on d.DEPARTMENT_ID = adt.ADT_DEPARTMENT_ID 
	join (select pat_enc_csn, min(in_dttm) as min_in_dttm from #pj_adt_detail group by pat_enc_csn) sub on  --get rows before pt on IP floor, like ED
			x.pat_enc_csn = sub.pat_enc_csn
			and adt.in_dttm < sub.min_in_dttm
	join pat_enc_hsp peh on x.PAT_ENC_CSN = peh.PAT_ENC_CSN_ID
	/*  Get the Parent, then the Grandparent */ 
	LEFT OUTER JOIN CLARITY_LOC loc on d.REV_LOC_ID = loc.LOC_ID 
	LEFT OUTER JOIN CLARITY_LOC parloc ON loc.HOSP_PARENT_LOC_ID = parloc.LOC_ID                    --parent (like 19106/19252/19251 for PAH) 
	LEFT OUTER JOIN ZC_LOC_RPT_GRP_7 gploc ON parloc.RPT_GRP_SEVEN = gploc.RPT_GRP_SEVEN            --grandparent (Entity like 30 for HUP) 
WHERE IN_DTTM <> OUT_DTTM  --avoid bogus entries
GO

--add rows that occur AFTER last IP unit (for patients that die in the OR etc)
insert into #pj_adt_detail
select adt.PAT_ENC_CSN, x.HSP_ACCOUNT_ID, adt.IN_DTTM, adt.PAT_OUT_DTTM, d.RPT_GRP_ELEVEN_C, /*adt.ADT_DEPARTMENT_NAME, */ adt.ADT_DEPARTMENT_ID
	,ROWNUM = NULL
	,UNIT_ADMIT_STATUS = CASE 
		WHEN ADT.ADT_DEPARTMENT_ID <> LAG(adt.ADT_DEPARTMENT_ID, 1, 0) OVER (PARTITION BY adt.PAT_ENC_CSN ORDER BY adt.IN_DTTM, adt.out_dttm)
			THEN 'ADMIT TO UNIT'
		END
	,UNIT_DISCHARGE_STATUS = CASE
		WHEN ADT.ADT_DEPARTMENT_ID <> LEAD(adt.ADT_DEPARTMENT_ID, 1, 0) OVER (PARTITION BY adt.PAT_ENC_CSN ORDER BY adt.IN_DTTM, adt.out_dttm)
		AND adt.pat_out_dttm is not null
			THEN 'TRANSFER FROM UNIT'
		WHEN adt.pat_out_dttm is null
			THEN 'PATIENT IN UNIT'
		END
	,UNIT_STAY_TYPE = 'NON IP UNIT'
	,NON_IP_LOC = NULL
	,PAT_LVL_OF_CARE_C = NULL  --not needed for these lines of data, but included to keep columns consistent
	,x.billed_as_op
	,cast(null as int) as "MAX_ROW_NUM"  --used to find rows that are missing a pat_out_dttm timestamp
	,peh.INP_ADM_DATE
	,gploc.NAME as "HOSP"
	,cast(null as numeric) as event_id		--not needed for these lines of data, but included to keep columns consistent
	,cast(null as int) as loc_change_flag	--not needed for these lines of data, but included to keep columns consistent

	--,sub.max_out_dttm
from #pj_unittbl_csn_list x
	join V_PAT_ADT_LOCATION_HX adt on x.PAT_ENC_CSN = adt.PAT_ENC_CSN
	join CLARITY_DEP d on d.DEPARTMENT_ID = adt.ADT_DEPARTMENT_ID 
	join (select pat_enc_csn, 
			max(case when pat_out_dttm is null then '12/31/9999' else pat_out_dttm end) as max_out_dttm --if the pt is in unit, pat_out_dttm is null and doesn't get included in max calc. This fixes that.
			from #pj_adt_detail group by pat_enc_csn) sub on  --get rows after pt on IP floor, like ED
			x.pat_enc_csn = sub.pat_enc_csn
			and adt.IN_DTTM >= sub.max_out_dttm  --there was a non-ip row AFTER the last IP row, like an OR.
	join pat_enc_hsp peh on x.PAT_ENC_CSN = peh.PAT_ENC_CSN_ID
	/*  Get the Parent, then the Grandparent */ 
	LEFT OUTER JOIN CLARITY_LOC loc on d.REV_LOC_ID = loc.LOC_ID 
	LEFT OUTER JOIN CLARITY_LOC parloc ON loc.HOSP_PARENT_LOC_ID = parloc.LOC_ID                    --parent (like 19106/19252/19251 for PAH) 
	LEFT OUTER JOIN ZC_LOC_RPT_GRP_7 gploc ON parloc.RPT_GRP_SEVEN = gploc.RPT_GRP_SEVEN            --grandparent (Entity like 30 for HUP) 
WHERE IN_DTTM <> OUT_DTTM  --avoid bogus entries
GO

/****************************                    Add ED & OR Inpatients    **************************/
--Get cases that we pulled no lines for. These are typically cases that were billed ONLY in the ED and OR, but billed as IPs
insert into #pj_adt_detail
select adt.PAT_ENC_CSN, x.HSP_ACCOUNT_ID, adt.IN_DTTM, adt.PAT_OUT_DTTM, d.RPT_GRP_ELEVEN_C, /*adt.ADT_DEPARTMENT_NAME, */ adt.ADT_DEPARTMENT_ID
	,ROWNUM = NULL
	,UNIT_ADMIT_STATUS = CASE 
		WHEN ADT.ADT_DEPARTMENT_ID <> LAG(adt.ADT_DEPARTMENT_ID, 1, 0) OVER (PARTITION BY adt.PAT_ENC_CSN ORDER BY adt.IN_DTTM, adt.out_dttm)
			THEN 'ADMIT TO UNIT'
		END
	,UNIT_DISCHARGE_STATUS = CASE
		WHEN ADT.ADT_DEPARTMENT_ID <> LEAD(adt.ADT_DEPARTMENT_ID, 1, 0) OVER (PARTITION BY adt.PAT_ENC_CSN ORDER BY adt.IN_DTTM, adt.out_dttm)
		AND adt.pat_out_dttm is not null
			THEN 'TRANSFER FROM UNIT'
		WHEN adt.pat_out_dttm is null
			THEN 'PATIENT IN UNIT'
		END
	,UNIT_STAY_TYPE = 'NON IP UNIT'
	,NON_IP_LOC = NULL
	,PAT_LVL_OF_CARE_C = NULL			--not needed for these lines of data, but included to keep columns consistent
	,x.billed_as_op
	,cast(null as int) as "MAX_ROW_NUM"  --used to find rows that are missing a pat_out_dttm timestamp
	,peh.INP_ADM_DATE
	,gploc.NAME as "HOSP"
	,cast(null as numeric) as event_id		--not needed for these lines of data, but included to keep columns consistent
	,cast(null as int) as loc_change_flag	--not needed for these lines of data, but included to keep columns consistent

from #pj_unittbl_csn_list x
	join V_PAT_ADT_LOCATION_HX adt on x.PAT_ENC_CSN = adt.PAT_ENC_CSN
	join CLARITY_DEP d on d.DEPARTMENT_ID = adt.ADT_DEPARTMENT_ID 
	join pat_enc_hsp peh on x.PAT_ENC_CSN = peh.PAT_ENC_CSN_ID
	left join (select distinct PAT_ENC_CSN from #pj_adt_detail) sub on x.pat_enc_csn = sub.pat_enc_csn

	/*  Get the Parent, then the Grandparent */ 
	LEFT OUTER JOIN CLARITY_LOC loc on d.REV_LOC_ID = loc.LOC_ID 
	LEFT OUTER JOIN CLARITY_LOC parloc ON loc.HOSP_PARENT_LOC_ID = parloc.LOC_ID                    --parent (like 19106/19252/19251 for PAH) 
	LEFT OUTER JOIN ZC_LOC_RPT_GRP_7 gploc ON parloc.RPT_GRP_SEVEN = gploc.RPT_GRP_SEVEN            --grandparent (Entity like 30 for HUP) 
where 
	sub.pat_enc_csn is null
GO

--Update Non-Ip Unit Types
update #pj_adt_detail
set UNIT_STAY_TYPE = case
	when ADT_DEPARTMENT_ID in (5602,5836,5412) or DEPARTMENT_NAME like '% EMERGENCY DEPARTMENT%' then 'NON IP UNIT: ED'  --ED's
	when ADT_DEPARTMENT_ID in (4056,4140,4143) and UNIT_STAY_TYPE = 'NON IP UNIT' then 'NON IP UNIT: EDOU'  --HUP EDOU's
	when DEPARTMENT_NAME like '% OR' then 'NON IP UNIT: OR'  --OR's
	when ADT_DEPARTMENT_ID = 4015 then 'NON IP UNIT: LD'  --HUP LD
	else 'NON IP UNIT: ' + d.specialty   --default
	end
from #pj_adt_detail x
	join clarity_dep d on x.ADT_DEPARTMENT_ID = d.DEPARTMENT_ID
where UNIT_STAY_TYPE = 'NON IP UNIT'
GO

--add rownum
IF OBJECT_ID('tempdb..#pj_adt_rownum') IS NOT NULL BEGIN DROP TABLE #pj_adt_rownum END;
select x.pat_enc_csn, x.adt_department_id, x.in_dttm, ROWNUM = ROW_NUMBER() over (partition by PAT_ENC_CSN order by in_dttm)
into #pj_adt_rownum
from #pj_adt_detail x
GO

update #pj_adt_detail
set ROWNUM = y.ROWNUM,
	MAX_ROW_NUM = max_row.max_row_num
from #pj_adt_detail x
	join #pj_adt_rownum y on
		x.pat_enc_csn = y.pat_enc_csn
		and x.adt_department_id = y.adt_department_id
		and x.in_dttm = y.in_dttm
	join 
	(select pat_enc_csn, max(rownum) as "max_row_num" from #pj_adt_rownum group by pat_enc_csn) max_row on x.PAT_ENC_CSN = max_row.PAT_ENC_CSN
GO

--mark row AFTER "orphan" row to as "admit_to_unit" to get correct hosp adm time
update #pj_adt_detail
set UNIT_ADMIT_STATUS = 'ADMIT TO UNIT'
from #pj_adt_detail x
	join
	(
	select PAT_ENC_CSN, ROWNUM + 1 as "next_row"
	from #pj_adt_detail
	where PAT_OUT_DTTM is null 
		and ROWNUM < MAX_ROW_NUM
	) sub_next_row on x.PAT_ENC_CSN = sub_next_row.PAT_ENC_CSN and x.ROWNUM = sub_next_row.next_row
where UNIT_ADMIT_STATUS is null
GO

--remove "orphan" rows without an end time
delete #pj_adt_detail
where PAT_OUT_DTTM is null 
	and ROWNUM < MAX_ROW_NUM
GO

--***********************        create summary table with admit rows        ************************************************************
IF OBJECT_ID('tempdb..##pj_adt_summary') IS NOT NULL BEGIN DROP TABLE ##pj_adt_summary END;
select x.PAT_ENC_CSN, x.HSP_ACCOUNT_ID, x.UNIT_STAY_TYPE, x.ADT_DEPARTMENT_ID
	,IN_DTTM as "UNIT_IN_DTTM"
	,CAST(NULL as datetime) as "UNIT_OUT_DTTM"
	,CAST(NULL as varchar) as "UNIT_STATUS"
	,CAST(NULL as int) as "IP_UNIT_TRACKER_CSN"
	,CAST(NULL as int) as "ICU_TRACKER_CSN"
	,x.ROWNUM as "ROWNUM_DETAIL"
	,NEXT_ADMIT_ROWNUM = CASE
		WHEN LEAD(x.ROWNUM, 1, 0) OVER (PARTITION BY x.PAT_ENC_CSN ORDER BY x.IN_DTTM) <> 0  --next row is not null (ie diff pt)
			THEN LEAD(x.ROWNUM, 1, 0) OVER (PARTITION BY x.PAT_ENC_CSN ORDER BY x.IN_DTTM)
		ELSE 9999
		END
	,cast(NULL as varchar(25)) as "NEXT_IP_UNIT_STAY_TYPE_CSN"
	,cast(NULL as varchar(25)) as "NEXT_IP_DEPT_ID"
	,cast(NULL as smalldatetime) as "PREVIOUS_UNIT_OUT_DTTM"
	,x.BILLED_AS_OP
	,cast(null as int) as rownum_summary
	,cast(null as decimal(16,2)) as "ICU_BOUNCE_BACK_HRS_CSN"
	,cast(null as decimal(16,2)) as "ICU_BOUNCE_BACK_HRS_HAR"
	,cast(null as varchar(20)) as "HUP_SITE"
	,cast(0 as int) as "ANCILLARY_VISITS"
	,cast(null as smalldatetime) as "EXTRACTION_DTTM_APPROX"  --the max out_time to be used for calculations for patients still in units at extraction time
	,cast(NULL as int) as "IP_UNIT_TRACKER_HAR"
	,CAST(NULL as int) as "ICU_TRACKER_HAR"
	,cast(NULL as varchar(50)) as "NEXT_IP_UNIT_STAY_TYPE_HAR"
	,cast(0 as int) as "CEDAR_SPRUCE_TXN_YN"
	,cast(0 as int) as "NORMAL_NEWBORN_YN"
	,cast(0 as int) as "HOSPICE_ADMISSION_YN"
	,cast(null as varchar(10)) as "HOSP"
into ##pj_adt_summary
from #pj_adt_detail x
where x.unit_admit_status is not null 
GO

CREATE INDEX pj_idx_adt_summary ON ##pj_adt_summary (pat_enc_csn, PREVIOUS_UNIT_OUT_DTTM, HSP_ACCOUNT_ID, UNIT_STAY_TYPE,
	ADT_DEPARTMENT_ID, UNIT_IN_DTTM, BILLED_AS_OP, UNIT_STATUS, ROWNUM_DETAIL, rownum_summary, ICU_TRACKER_CSN,
	HUP_SITE, HOSP);

--add data for when patient LEFT unit
update ##pj_adt_summary
--select x.*, y.PAT_OUT_DTTM, y.ROWNUM, y.UNIT_DISCHARGE_STATUS
set unit_out_dttm = y.PAT_OUT_DTTM				--update the last out_dttm for this row's unit
	,unit_status = y.UNIT_DISCHARGE_STATUS		--update the last status for this row's unit
from ##pj_adt_summary x
	join #pj_adt_detail y on x.pat_enc_csn = y.pat_enc_csn
		and y.rownum between (x.ROWNUM_DETAIL) and (x.next_admit_rownum-1)  --next_admit_rownum is the NEXT unit, so "-1" is the last row of CURRENT unit
where y.UNIT_DISCHARGE_STATUS in ('TRANSFER FROM UNIT','PATIENT IN UNIT')  --this avoids the rows with NULL values which are neither admits nor transfers
GO

--fix unit status when pat_out_dttm is not populated (and should be). This happens occassionally with women's health
update ##pj_adt_summary
set UNIT_STATUS='TRANSFER FROM UNIT'
where unit_out_dttm is not null and unit_status = 'PATIENT IN UNIT'
GO

--**********************   Add transfer to ancilliary units ONLY when patient goes to another unit   **********************************
--create temp table
IF OBJECT_ID('tempdb..#pj_adt_prev_unit_out_dttm') IS NOT NULL BEGIN DROP TABLE #pj_adt_prev_unit_out_dttm END;
select pat_enc_csn, unit_in_dttm
	,LAG(unit_out_dttm, 1, 0) OVER (PARTITION BY PAT_ENC_CSN ORDER BY unit_in_dttm) as previous_unit_out_dttm
into #pj_adt_prev_unit_out_dttm
from ##pj_adt_summary
GO

--update summary table with previous transfer dttm
update ##pj_adt_summary
set previous_unit_out_dttm = y.previous_unit_out_dttm
--select x.pat_enc_csn, x.unit_in_dttm, y.previous_unit_out_dttm
from ##pj_adt_summary x
	join #pj_adt_prev_unit_out_dttm y
		on x.pat_enc_csn = y.pat_enc_csn
		and x.unit_in_dttm = y.unit_in_dttm
where y.previous_unit_out_dttm <> 0  --excludes first row (probably not needed)
	and y.previous_unit_out_dttm < x.unit_in_dttm  --excludes cases where there is NO GAP
GO

--create detail table for gap units
IF OBJECT_ID('tempdb..#pj_adt_detail_gap_units') IS NOT NULL BEGIN DROP TABLE #pj_adt_detail_gap_units END;
select adt.PAT_ENC_CSN, x.HSP_ACCOUNT_ID, adt.IN_DTTM, adt.PAT_OUT_DTTM, d.RPT_GRP_ELEVEN_C, /*adt.ADT_DEPARTMENT_NAME, */adt.ADT_DEPARTMENT_ID
	,ROW_NUMBER() over (partition by adt.PAT_ENC_CSN order by adt.in_dttm) as ROWNUM
	,UNIT_ADMIT_STATUS = 'ADMIT TO UNIT' --ancillary units VERY RARELY have extra rows we need to worry about
	,UNIT_DISCHARGE_STATUS = CASE
		WHEN adt.pat_out_dttm is null THEN 'PATIENT IN UNIT'
		ELSE 'TRANSFER FROM UNIT'
		END
	--,'NON IP UNIT' as "UNIT_STAY_TYPE"
	,UNIT_STAY_TYPE = case
	when x.ADT_DEPARTMENT_ID in (5602,5836, 5412) or DEPARTMENT_NAME like '% EMERGENCY DEPARTMENT%' then 'NON IP UNIT: ED'  --ED's
	when x.ADT_DEPARTMENT_ID in (4056, 4140, 4143) and UNIT_STAY_TYPE = 'NON IP UNIT' then 'NON IP UNIT: EDOU'  --HUP EDOU's
	when DEPARTMENT_NAME like '% OR' then 'NON IP UNIT: OR'  --OR's
	when x.ADT_DEPARTMENT_ID = 4015 then 'NON IP UNIT: LD'  --HUP LD
	else 'NON IP UNIT: ' + d.specialty   --default
	end
	,CAST(NULL as int) as "NON_IP_LOC"
	,x.billed_as_op
into #pj_adt_detail_gap_units
from ##pj_adt_summary x
	join V_PAT_ADT_LOCATION_HX adt on x.PAT_ENC_CSN = adt.PAT_ENC_CSN
		and adt.in_dttm between x.previous_unit_out_dttm and dateadd(minute,-1, x.unit_in_dttm)  --gets the units in the gap
	join CLARITY_DEP d on d.DEPARTMENT_ID = adt.ADT_DEPARTMENT_ID 
where x.previous_unit_out_dttm is not null
GO

--create summary table for gap units
IF OBJECT_ID('tempdb..##pj_adt_summary_gap_units') IS NOT NULL BEGIN DROP TABLE ##pj_adt_summary_gap_units END;
select x.PAT_ENC_CSN, x.HSP_ACCOUNT_ID, x.UNIT_STAY_TYPE, /*x.ADT_DEPARTMENT_NAME, */x.ADT_DEPARTMENT_ID
	,IN_DTTM as "UNIT_IN_DTTM"
	,CAST(NULL as datetime) as "UNIT_OUT_DTTM"
	,CAST(NULL as varchar) as "UNIT_STATUS"
	,CAST(NULL as int) as "IP_UNIT_TRACKER_CSN"
	,CAST(NULL as int) as "ICU_TRACKER_CSN"
	,ROWNUM
	,NEXT_ADMIT_ROWNUM = CASE
		WHEN LEAD(x.ROWNUM, 1, 0) OVER (PARTITION BY x.PAT_ENC_CSN ORDER BY x.IN_DTTM) <> 0  --next row is not null (ie diff pt)
			THEN LEAD(x.ROWNUM, 1, 0) OVER (PARTITION BY x.PAT_ENC_CSN ORDER BY x.IN_DTTM)
		ELSE 9999
		END
	,cast(NULL as varchar(25)) as "NEXT_IP_UNIT_STAY_TYPE_CSN"
	,cast(NULL as smalldatetime) as "PREVIOUS_UNIT_OUT_DTTM"
	,x.billed_as_op
into ##pj_adt_summary_gap_units
from #pj_adt_detail_gap_units x
where x.unit_admit_status is not null 
GO


--add data for when patient LEFT unit
update ##pj_adt_summary_gap_units
--select x.*, y.PAT_OUT_DTTM, y.ROWNUM, y.UNIT_DISCHARGE_STATUS
set unit_out_dttm = y.PAT_OUT_DTTM				--update the last out_dttm for this row's unit
	,unit_status = y.UNIT_DISCHARGE_STATUS		--update the last status for this row's unit
from ##pj_adt_summary_gap_units x
	join #pj_adt_detail_gap_units y on x.pat_enc_csn = y.pat_enc_csn
		and y.rownum between (x.rownum) and (x.next_admit_rownum-1)  --next_admit_rownum is the NEXT unit, so "-1" is the last row of CURRENT unit
where y.UNIT_DISCHARGE_STATUS in ('TRANSFER FROM UNIT','PATIENT IN UNIT')  --this avoids the rows with NULL values which are neither admits nor transfers
GO

--add gap rows to summary table
insert into ##pj_adt_summary
	(pat_enc_csn, hsp_account_id, UNIT_STAY_TYPE, /*adt_department_name, */ adt_department_id, unit_in_dttm, unit_out_dttm, unit_status, billed_as_op)
select x.pat_enc_csn, hsp_account_id, x.UNIT_STAY_TYPE, /*x.adt_department_name,*/ x.adt_department_id, x.unit_in_dttm, x.unit_out_dttm, x.unit_status, x.billed_as_op
from ##pj_adt_summary_gap_units x
GO

--update weird cases (1 in 5000) where we've got a gap caused by an inappropriate null in pat_out_dttm
IF OBJECT_ID('tempdb..#pj_weird_units') IS NOT NULL BEGIN DROP TABLE #pj_weird_units END;
select x.PAT_ENC_CSN, x.UNIT_IN_DTTM, cast(null as smalldatetime) as unit_out_dttm_new
into #pj_weird_units
from ##pj_adt_summary x
	join
	(
	select PAT_ENC_CSN, max(unit_in_dttm) as "max_unit_in_dttm"
	from ##pj_adt_summary
	group by PAT_ENC_CSN
	) max_in on x.PAT_ENC_CSN = max_in.PAT_ENC_CSN 
		and x.UNIT_IN_DTTM < max_in.max_unit_in_dttm
		and x.UNIT_STATUS = 'PATIENT IN UNIT'
GO

--update the summary table for the weird cases
update ##pj_adt_summary
set UNIT_OUT_DTTM = sub.unit_out_dttm_new,
	UNIT_STATUS = 'TRANSFER FROM UNIT'
from  ##pj_adt_summary x_outer
	join
	(
	select x.PAT_ENC_CSN, x.UNIT_IN_DTTM,
		CASE WHEN x.UNIT_IN_DTTM = y.UNIT_IN_DTTM
			then LEAD(y.UNIT_IN_DTTM, 1, 0) OVER (PARTITION BY y.PAT_ENC_CSN ORDER BY y.UNIT_IN_DTTM) end as "unit_out_dttm_new"
	from #pj_weird_units x
		join ##pj_adt_summary y on x.PAT_ENC_CSN = y.pat_enc_csn
	) sub on x_outer.PAT_ENC_CSN = sub.PAT_ENC_CSN 
		and x_outer.UNIT_IN_DTTM = sub.UNIT_IN_DTTM
		and sub.unit_out_dttm_new is not null
GO

--update rownum_summary (which will be used later to assign next units)
update ##pj_adt_summary
set rownum_summary = s.ROWNUM_summary
from ##pj_adt_summary x
	join
	(
	select pat_enc_csn, UNIT_IN_DTTM
		,ROW_NUMBER() over (partition by PAT_ENC_CSN order by unit_in_dttm) as ROWNUM_summary
	from ##pj_adt_summary
	) s on x.PAT_ENC_CSN = s.PAT_ENC_CSN and x.UNIT_IN_DTTM = s.UNIT_IN_DTTM
GO

--*************************          update TRACKERS - CSN       **************************************************
--create temp table
IF OBJECT_ID('tempdb..#pj_IP_UNIT_TRACKER_CSN') IS NOT NULL BEGIN DROP TABLE #pj_IP_UNIT_TRACKER_CSN END;
select x.PAT_ENC_CSN
	,ROW_NUMBER() over (partition by x.PAT_ENC_CSN order by x.unit_in_dttm) as "IP_UNIT_TRACKER_CSN"
	,x.UNIT_IN_DTTM, x.ADT_DEPARTMENT_ID
into #pj_IP_UNIT_TRACKER_CSN
from ##pj_adt_summary x
where UNIT_STAY_TYPE not like  ('NON IP%')
GO


--update icu admission tracker
update ##pj_adt_summary
set IP_UNIT_TRACKER_CSN = y.IP_UNIT_TRACKER_CSN
from ##pj_adt_summary x
	join #pj_IP_UNIT_TRACKER_CSN y on
		x.PAT_ENC_CSN = y.PAT_ENC_CSN and
		x.UNIT_IN_DTTM = y.UNIT_IN_DTTM and
		x.ADT_DEPARTMENT_ID = y.ADT_DEPARTMENT_ID
GO

--update icu tracker, create temp table
IF OBJECT_ID('tempdb..#pj_covid_icu_adm_tracker') IS NOT NULL BEGIN DROP TABLE #pj_covid_icu_adm_tracker END;
select x.PAT_ENC_CSN
	,ROW_NUMBER() over (partition by x.PAT_ENC_CSN order by x.unit_in_dttm) as "ICU_ADMISSION_TRACKER"
	,x.UNIT_IN_DTTM, x.ADT_DEPARTMENT_ID
into #pj_covid_icu_adm_tracker
from ##pj_adt_summary x
where UNIT_STAY_TYPE = 'CRITICAL CARE'
GO

--update icu admission tracker
update ##pj_adt_summary
set ICU_TRACKER_CSN = y.ICU_ADMISSION_TRACKER
from ##pj_adt_summary x
	join #pj_covid_icu_adm_tracker y on
		x.PAT_ENC_CSN = y.PAT_ENC_CSN and
		x.UNIT_IN_DTTM = y.UNIT_IN_DTTM and
		x.ADT_DEPARTMENT_ID = y.ADT_DEPARTMENT_ID
GO

--*************************          update TRACKERS - HAR       **************************************************
--create temp table
IF OBJECT_ID('tempdb..#pj_IP_UNIT_TRACKER_HAR') IS NOT NULL BEGIN DROP TABLE #pj_IP_UNIT_TRACKER_HAR END;
select x.HSP_ACCOUNT_ID
	,ROW_NUMBER() over (partition by x.HSP_ACCOUNT_ID order by x.unit_in_dttm) as "IP_UNIT_TRACKER_HAR"
	,x.UNIT_IN_DTTM, x.ADT_DEPARTMENT_ID
into #pj_IP_UNIT_TRACKER_HAR
from ##pj_adt_summary x
where UNIT_STAY_TYPE not like  ('NON IP%')
GO

--update icu admission tracker
update ##pj_adt_summary
set IP_UNIT_TRACKER_HAR = y.IP_UNIT_TRACKER_HAR
from ##pj_adt_summary x
	join #pj_IP_UNIT_TRACKER_HAR y on
		x.HSP_ACCOUNT_ID = y.HSP_ACCOUNT_ID and
		x.UNIT_IN_DTTM = y.UNIT_IN_DTTM and
		x.ADT_DEPARTMENT_ID = y.ADT_DEPARTMENT_ID
GO

--update icu tracker, create temp table
IF OBJECT_ID('tempdb..#pj_covid_icu_adm_tracker_har') IS NOT NULL BEGIN DROP TABLE #pj_covid_icu_adm_tracker_har END;
select x.HSP_ACCOUNT_ID
	,ROW_NUMBER() over (partition by x.HSP_ACCOUNT_ID order by x.unit_in_dttm) as "ICU_ADMISSION_TRACKER"
	,x.UNIT_IN_DTTM, x.ADT_DEPARTMENT_ID
into #pj_covid_icu_adm_tracker_har
from ##pj_adt_summary x
where UNIT_STAY_TYPE = 'CRITICAL CARE'
GO

--update icu admission tracker
update ##pj_adt_summary
set ICU_TRACKER_HAR = y.ICU_ADMISSION_TRACKER
from ##pj_adt_summary x
	join #pj_covid_icu_adm_tracker_har y on
		x.HSP_ACCOUNT_ID = y.HSP_ACCOUNT_ID and
		x.UNIT_IN_DTTM = y.UNIT_IN_DTTM and
		x.ADT_DEPARTMENT_ID = y.ADT_DEPARTMENT_ID
GO

--************************************     Update next unit - CSN   ************************************************************
IF OBJECT_ID('tempdb..#pj_adt_next_unit') IS NOT NULL BEGIN DROP TABLE #pj_adt_next_unit END;
select x.pat_enc_csn, ROWNUM_DETAIL
	,NEXT_IP_UNIT_STAY_TYPE_CSN = CASE 
		WHEN LEAD(UNIT_STAY_TYPE, 1, 0) OVER (PARTITION BY PAT_ENC_CSN ORDER BY ROWNUM_DETAIL) in ('LTACH','CRITICAL CARE','WOMENS SERVICES','MED/SURG','REHAB') THEN
			LEAD(UNIT_STAY_TYPE, 1, 0) OVER (PARTITION BY PAT_ENC_CSN ORDER BY ROWNUM_DETAIL)
		WHEN UNIT_STATUS = 'PATIENT IN UNIT' THEN 'PATIENT CURRENTLY IN UNIT'
		WHEN UNIT_STATUS = 'TRANSFER FROM UNIT' and peh.hosp_disch_time = x.unit_out_dttm THEN 'DISCHARGED'--'DISCHARGE FROM HOSPITAL'
		WHEN UNIT_STATUS = 'TRANSFER FROM UNIT' THEN 'PT TXN TO NON-IP LOC'  --patient in OR or something
		END
	,NEXT_IP_DEPT_ID = CASE 
		WHEN LEAD(UNIT_STAY_TYPE, 1, 0) OVER (PARTITION BY PAT_ENC_CSN ORDER BY ROWNUM_DETAIL) in ('LTACH','CRITICAL CARE','WOMENS SERVICES','MED/SURG','REHAB') THEN
			LEAD(ADT_DEPARTMENT_ID, 1, 0) OVER (PARTITION BY PAT_ENC_CSN ORDER BY ROWNUM_DETAIL)
		ELSE NULL
		END
		,hosp_disch_time
into #pj_adt_next_unit
from ##pj_adt_summary x
	join pat_enc_hsp peh on x.pat_enc_csn = peh.pat_enc_csn_id
where 
	UNIT_STAY_TYPE not like ('NON IP%')  --We don't want OR's & things to be the next unit
	or ADT_DEPARTMENT_ID in (5602,4140,4056, 4143)  --known ED & EDOU depts. Probably not needed, just to be extra sure
GO

--update next unit
update ##pj_adt_summary
set NEXT_IP_UNIT_STAY_TYPE_CSN = y.NEXT_IP_UNIT_STAY_TYPE_CSN,
	NEXT_IP_DEPT_ID = y.NEXT_IP_DEPT_ID,
	UNIT_STATUS = case when y.NEXT_IP_UNIT_STAY_TYPE_CSN = 'DISCHARGED' then 'DISCHARGED' else UNIT_STATUS end
from ##pj_adt_summary x
	join #pj_adt_next_unit y on x.pat_enc_csn = y.pat_enc_csn and x.ROWNUM_DETAIL = y.ROWNUM_DETAIL
GO

--update NON-IP next unit info. 
IF OBJECT_ID('tempdb..#pj_adt_next_unit_2') IS NOT NULL BEGIN DROP TABLE #pj_adt_next_unit_2 END;
select x.pat_enc_csn, rownum_summary
	,NEXT_IP_UNIT_STAY_TYPE_CSN_new = CASE 
		WHEN NEXT_IP_UNIT_STAY_TYPE_CSN is null 
			and UNIT_STATUS = 'TRANSFER FROM UNIT' 
			and LEAD(UNIT_STAY_TYPE, 1, 0) OVER (PARTITION BY PAT_ENC_CSN ORDER BY ROWNUM_summary) in ('LTACH','CRITICAL CARE','WOMENS SERVICES','MED/SURG','REHAB')  --only want IP unit types
				THEN LEAD(UNIT_STAY_TYPE, 1, 0) OVER (PARTITION BY PAT_ENC_CSN ORDER BY ROWNUM_summary)
		WHEN NEXT_IP_UNIT_STAY_TYPE_CSN is null                         --get the SECOND next unit type if the 1st one is NOT an IP type
			and UNIT_STATUS = 'TRANSFER FROM UNIT' 
			and LEAD(UNIT_STAY_TYPE,2, 0) OVER (PARTITION BY PAT_ENC_CSN ORDER BY ROWNUM_summary) in ('LTACH','CRITICAL CARE','WOMENS SERVICES','MED/SURG','REHAB')  --only want IP unit types
				THEN LEAD(UNIT_STAY_TYPE, 2, 0) OVER (PARTITION BY PAT_ENC_CSN ORDER BY ROWNUM_summary)
		WHEN NEXT_IP_UNIT_STAY_TYPE_CSN is null                         --get the THIRD next unit type if the 1st TWO are NOT an IP type
			and UNIT_STATUS = 'TRANSFER FROM UNIT' 
			and LEAD(UNIT_STAY_TYPE,3, 0) OVER (PARTITION BY PAT_ENC_CSN ORDER BY ROWNUM_summary) in ('LTACH','CRITICAL CARE','WOMENS SERVICES','MED/SURG','REHAB')  --only want IP unit types
				THEN LEAD(UNIT_STAY_TYPE, 3, 0) OVER (PARTITION BY PAT_ENC_CSN ORDER BY ROWNUM_summary)
		WHEN NEXT_IP_UNIT_STAY_TYPE_CSN is null                         --get the FOURTH next unit type if the 1st THREE are NOT an IP type
			and UNIT_STATUS = 'TRANSFER FROM UNIT' 
			and LEAD(UNIT_STAY_TYPE,4, 0) OVER (PARTITION BY PAT_ENC_CSN ORDER BY ROWNUM_summary) in ('LTACH','CRITICAL CARE','WOMENS SERVICES','MED/SURG','REHAB')  --only want IP unit types
				THEN LEAD(UNIT_STAY_TYPE, 4, 0) OVER (PARTITION BY PAT_ENC_CSN ORDER BY ROWNUM_summary)
		END
	,NEXT_IP_DEPT_ID_new = CASE 
		WHEN NEXT_IP_UNIT_STAY_TYPE_CSN is null 
			and UNIT_STATUS = 'TRANSFER FROM UNIT' 
			then LEAD(ADT_DEPARTMENT_ID, 1, 0) OVER (PARTITION BY PAT_ENC_CSN ORDER BY ROWNUM_summary)
		END
into #pj_adt_next_unit_2
from ##pj_adt_summary x
--order by 1,UNIT_IN_DTTM
GO


--update next unit for NON-IP units
update ##pj_adt_summary
set NEXT_IP_UNIT_STAY_TYPE_CSN = y.NEXT_IP_UNIT_STAY_TYPE_CSN_new,
	NEXT_IP_DEPT_ID = y.NEXT_IP_DEPT_ID_new
--select x.*, y.*
from ##pj_adt_summary x
	join #pj_adt_next_unit_2 y on x.pat_enc_csn = y.pat_enc_csn and x.rownum_summary = y.rownum_summary
where y.NEXT_IP_UNIT_STAY_TYPE_CSN_new is not null
	and y.NEXT_IP_UNIT_STAY_TYPE_CSN_new in ('LTACH','CRITICAL CARE','WOMENS SERVICES','MED/SURG','REHAB') --don't want to update next unit type with a non-IP location
GO

--**************************************       update icu bounce back hours CSN    *******************************************
--create temp table
IF OBJECT_ID('tempdb..#pj_icu_bounce_back') IS NOT NULL BEGIN DROP TABLE #pj_icu_bounce_back END;
select x.PAT_ENC_CSN, x.ICU_TRACKER_CSN, x.UNIT_IN_DTTM, x.UNIT_OUT_DTTM, x.UNIT_STATUS, x.NEXT_IP_UNIT_STAY_TYPE_CSN,
	ICU_BOUNCE_BACK_HRS_CSN = case
		when x.NEXT_IP_UNIT_STAY_TYPE_CSN not in ('CRITICAL CARE','PATIENT CURRENTLY IN UNIT')
			and LEAD(x.UNIT_IN_DTTM, 1, 0) OVER (PARTITION BY x.PAT_ENC_CSN ORDER BY x.ICU_TRACKER_CSN) <> 0
			then round(datediff(MINUTE, x.UNIT_OUT_DTTM, LEAD(x.UNIT_IN_DTTM, 1, 0) OVER (PARTITION BY x.PAT_ENC_CSN ORDER BY x.ICU_TRACKER_CSN)) / 60.0,1)
			end
into #pj_icu_bounce_back
from ##pj_adt_summary x
where x.ICU_TRACKER_CSN is not null
GO

update ##pj_adt_summary
set ICU_BOUNCE_BACK_HRS_CSN = y.ICU_BOUNCE_BACK_HRS_CSN
from ##pj_adt_summary x
	join #pj_icu_bounce_back y on x.PAT_ENC_CSN = y.PAT_ENC_CSN and x.ICU_TRACKER_CSN = y.ICU_TRACKER_CSN
where y.ICU_BOUNCE_BACK_HRS_CSN is not null
GO

--**************************           update hospital     ****************************************************************
update ##pj_adt_summary
set HOSP = gploc.NAME
from ##pj_adt_summary x
	LEFT OUTER JOIN CLARITY_DEP dep ON x.ADT_DEPARTMENT_ID = dep.DEPARTMENT_ID 

	/*  Get the Parent, then the Grandparent */ 
	LEFT OUTER JOIN CLARITY_LOC loc on dep.REV_LOC_ID = loc.LOC_ID 
	LEFT OUTER JOIN CLARITY_LOC parloc ON loc.HOSP_PARENT_LOC_ID = parloc.LOC_ID                    --parent (like 19106/19252/19251 for PAH) 
	LEFT OUTER JOIN ZC_LOC_RPT_GRP_7 gploc ON parloc.RPT_GRP_SEVEN = gploc.RPT_GRP_SEVEN            --grandparent (Entity like 30 for HUP) 
GO

--**************************           update HUP site     ****************************************************************
--this needs to be done BEFORE updating Cedar Spruce transfers
update ##pj_adt_summary
set HUP_SITE = CASE
		when d.RPT_GRP_FOUR = 'HUPW' then 'HUP-Main'
		when d.REV_LOC_ID = 10345 then 'HUP-Cedar_Psych'
		when d.RPT_GRP_FOUR = 'HUP54' then 'HUP-Cedar'
		when d.RPT_GRP_FOUR = 'HUPE' then 'HUP-Pavilion'
		when d.REV_LOC_ID in (10103,10107) then 'HUP-Main'  --when HUP-EAST is live, we may need to edit this
		when d.REV_LOC_ID in (10243,10242) then 'Rittenhouse'
		else 'Needs Investigation'
		end
from ##pj_adt_summary x
	--join clarity_dep d on x.ADT_DEPARTMENT_ID = d.DEPARTMENT_ID
	join Clarity_Custom_Tables.dbo.X_CLARITY_DEP_HISTORY d on x.ADT_DEPARTMENT_ID = d.DEPARTMENT_ID
		and x.UNIT_IN_DTTM between d.START_DATE and d.END_DATE
where HOSP = 'HUP'
GO

--update any non-IP departments that fell through the cracks
update ##pj_adt_summary
set HUP_SITE = CASE
		when d.RPT_GRP_FOUR = 'HUPW' then 'HUP-Main'
		when d.REV_LOC_ID = 10345 then 'HUP-Cedar_Psych'
		when d.RPT_GRP_FOUR = 'HUP54' then 'HUP-Cedar'
		when d.RPT_GRP_FOUR = 'HUPE' then 'HUP-Pavilion'
		when d.REV_LOC_ID in (10103,10107) then 'HUP-Main'  --when HUP-EAST is live, we may need to edit this
		when d.REV_LOC_ID in (10243,10242) then 'Rittenhouse'
		else 'Needs Investigation'
		end
from ##pj_adt_summary x
	join clarity_dep d on x.ADT_DEPARTMENT_ID = d.DEPARTMENT_ID
	--join Clarity_Custom_Tables.dbo.X_CLARITY_DEP_HISTORY d on x.ADT_DEPARTMENT_ID = d.DEPARTMENT_ID
		--and x.UNIT_IN_DTTM between d.START_DATE and d.END_DATE
where HOSP = 'HUP' and HUP_SITE is null
GO


--************************************     Update Cedar / Spruce Transfers   ************************************************************
--update Cedar / Spruce Transfers
update ##pj_adt_summary 
set CEDAR_SPRUCE_TXN_YN = 1
from ##pj_adt_summary x
	join
	(
	select hsp_account_id
	from 
		(
				select hsp_account_id, hup_location = case when hup_site = 'HUP-Cedar' then hup_site else 'HUP-Spruce/Pavilion' end --18252
				from ##pj_adt_summary 
				where HUP_SITE in ('HUP-Cedar','HUP-Main','HUP-Pavilion') 
				group by hsp_account_id, case when hup_site = 'HUP-Cedar' then hup_site else 'HUP-Spruce/Pavilion' end
				--order by 1
			) sub_inner 
	group by hsp_account_id
	having count(*) > 1
	) sub on x.hsp_account_id = sub.hsp_account_id
GO

--update non-cedar transfers
update ##pj_adt_summary 
set CEDAR_SPRUCE_TXN_YN = 0
where CEDAR_SPRUCE_TXN_YN <> 1 or CEDAR_SPRUCE_TXN_YN is null
GO

--************************************     Update next unit - HAR   ************************************************************
--first, just update the HAR value to the CSN value
update ##pj_adt_summary
set NEXT_IP_UNIT_STAY_TYPE_HAR = NEXT_IP_UNIT_STAY_TYPE_CSN
GO

--create temp table
IF OBJECT_ID('tempdb..#pj_cedar_spruce_txn') IS NOT NULL BEGIN DROP TABLE #pj_cedar_spruce_txn END;
select x.hsp_account_id, x.unit_in_dttm,
next_unit_upd = case when 
		unit_status = 'DISCHARGED'  
		and LEAD(HUP_SITE, 1, 0) OVER (PARTITION BY HSP_ACCOUNT_ID ORDER BY UNIT_IN_DTTM) <> '0'
		then 'TRANSFER TO ' 
			+ UPPER(LEAD(HUP_SITE, 1, 0) OVER (PARTITION BY HSP_ACCOUNT_ID ORDER BY UNIT_IN_DTTM))
			+ '; '
			+ LEAD(UNIT_STAY_TYPE, 1, 0) OVER (PARTITION BY HSP_ACCOUNT_ID ORDER BY UNIT_IN_DTTM)
		else NEXT_IP_UNIT_STAY_TYPE_CSN end
into #pj_cedar_spruce_txn
from ##pj_adt_summary x
where CEDAR_SPRUCE_TXN_YN = 1
	and (unit_status = 'DISCHARGED' or UNIT_STAY_TYPE in ('LTACH','CRITICAL CARE','WOMENS SERVICES','MED/SURG','REHAB'))
GO

--update summary table with Cedar Spruce Transfers
update ##pj_adt_summary
set NEXT_IP_UNIT_STAY_TYPE_HAR = next_unit_upd
from ##pj_adt_summary x
	join #pj_cedar_spruce_txn y on x.hsp_account_id = y.hsp_account_id and x.unit_in_dttm = y.unit_in_dttm
where x.NEXT_IP_UNIT_STAY_TYPE_HAR <> y.next_unit_upd
GO


--**************************************       update icu bounce back hours HAR    *******************************************
--create temp table
IF OBJECT_ID('tempdb..#pj_icu_bounce_back_har') IS NOT NULL BEGIN DROP TABLE #pj_icu_bounce_back_har END;
select x.HSP_ACCOUNT_ID, x.ICU_TRACKER_HAR, x.UNIT_IN_DTTM, x.UNIT_OUT_DTTM, x.UNIT_STATUS, x.NEXT_IP_UNIT_STAY_TYPE_HAR,
	ICU_BOUNCE_BACK_HRS_HAR = case
		when x.NEXT_IP_UNIT_STAY_TYPE_HAR not in ('CRITICAL CARE','PATIENT CURRENTLY IN UNIT','TRANSFER TO HUP-MAIN; CRITICAL CARE','TRANSFER TO HUP-CEDAR; CRITICAL CARE')
			and LEAD(x.UNIT_IN_DTTM, 1, 0) OVER (PARTITION BY x.HSP_ACCOUNT_ID ORDER BY x.ICU_TRACKER_HAR) <> 0
			then round(datediff(MINUTE, x.UNIT_OUT_DTTM, LEAD(x.UNIT_IN_DTTM, 1, 0) OVER (PARTITION BY x.HSP_ACCOUNT_ID ORDER BY x.ICU_TRACKER_HAR)) / 60.0,1)
			end
into #pj_icu_bounce_back_har
from ##pj_adt_summary x
where x.ICU_TRACKER_HAR is not null
GO

update ##pj_adt_summary
set ICU_BOUNCE_BACK_HRS_HAR = y.ICU_BOUNCE_BACK_HRS_HAR
from ##pj_adt_summary x
	join #pj_icu_bounce_back_har y on x.HSP_ACCOUNT_ID = y.HSP_ACCOUNT_ID and x.ICU_TRACKER_HAR = y.ICU_TRACKER_HAR
where y.ICU_BOUNCE_BACK_HRS_HAR is not null
GO

--*****************************           update ancillary visits (aka churn)   *************************************************************
--update ancillary visits
update ##pj_adt_summary
set ANCILLARY_VISITS = sub.anc_visits
from ##pj_adt_summary x_outer
	join
	(
	select x.pat_enc_csn, x.rownum_summary, count(*) as anc_visits--, x.unit_out_dttm, x.adt_department_id, d.rpt_grp_eleven_c, adt.*
	from ##pj_adt_summary x
		join v_pat_adt_location_hx adt on x.pat_enc_csn = adt.pat_enc_csn
			and adt.in_dttm between x.unit_in_dttm and dateadd(minute,-1, x.unit_out_dttm)
			and x.adt_department_id <> adt.adt_department_id		--don't include if dept id is the same. this avoids "NON-IP" visits that get their own row
		left join clarity_dep d on adt.adt_department_id = d.department_id
	where adt.adt_department_id not in (5602)	--don't want ED
		and d.rpt_grp_eleven_c = 6				--only non-IP units. Avoids some weird cases
	group by x.pat_enc_csn, x.rownum_summary
	) sub on x_outer.pat_enc_csn = sub.pat_enc_csn and x_outer.rownum_summary = sub.rownum_summary
GO

--update null ancillary_visit counts
update ##pj_adt_summary
set ANCILLARY_VISITS = 0
where ANCILLARY_VISITS is null
GO

/***************************               update normal newborns          ***********************************************
The PennDiver definition is rev code 0171-LEVEL 1 ONLY is Normal
     if 0173-LEVEL 2; 0174-LEVEL 4 is billed, then it's a NICU baby
	 Equivalently, if the baby spends the night in the HUP ICN, it's a NICU baby (which generates the 0173/0174 charge */

--create table with ALL patients billed with normal newborn rev code 171
IF OBJECT_ID('tempdb..#pj_normal_newborns') IS NOT NULL BEGIN DROP TABLE #pj_normal_newborns END;
select x.hsp_account_id, 1 as normal_newborn_171_only--, sum(htr.QUANTITY), sum(htr.TX_AMOUNT)--, peh.hosp_serv_c, svc.name
into #pj_normal_newborns
from ##pj_adt_summary x
	--join pat_enc_hsp peh on x.pat_enc_csn = peh.pat_Enc_csn_id
	join HSP_TRANSACTIONS Htr on x.hsp_account_id = htr.hsp_account_id
	join CL_UB_REV_CODE Htrub ON Htr.UB_REV_CODE_ID = Htrub.UB_REV_CODE_ID
	--left join ZC_PAT_SERVICE svc on peh.hosp_serv_c = svc.hosp_serv_c
where Htrub.REVENUE_CODE = '0171'
group by x.hsp_account_id--, peh.hosp_serv_c, svc.name
having sum(htr.TX_AMOUNT) > 0   --make sure charges were not reversed
GO

--remove patients that also had NICU charges 173/174 as these are NOT considered normal. This logic is from DI hospital operations dashboard
update #pj_normal_newborns
set normal_newborn_171_only = 0
from #pj_normal_newborns x
	join HSP_TRANSACTIONS Htr on x.hsp_account_id = htr.hsp_account_id
	join CL_UB_REV_CODE Htrub ON Htr.UB_REV_CODE_ID = Htrub.UB_REV_CODE_ID
where Htrub.REVENUE_CODE in ('0173','0174')
GO

--update summary table with
update ##pj_adt_summary
set NORMAL_NEWBORN_YN = 1
--select x.*
from ##pj_adt_summary x
	join #pj_normal_newborns y on x.hsp_account_id = y.hsp_account_id
where y.normal_newborn_171_only = 1   --patients that had ONLY rev code 171 billed
GO

--*****************             update hospice admission yn   *************************************************
update ##pj_adt_summary
set HOSPICE_ADMISSION_YN = 1
from ##pj_adt_summary x
	join pat_enc_hsp peh on x.PAT_ENC_CSN = peh.PAT_ENC_CSN_ID
where peh.ADT_PAT_CLASS_C in (153, 121)  --hospice
GO


--*****************             delete OP cases billed as IPs, but still in unit   *************************************************
--A lot of these cases end up getting billed as OPs. We'll pick them up again at their next movement if they do get billed as IP.
delete ##pj_adt_summary
where UNIT_STAY_TYPE like 'NON IP UNIT%'
	and UNIT_STATUS = 'PATIENT IN UNIT'
	and rownum_summary = 1
GO


--*****************             update extraction time   *************************************************
--For certain calculations, it's good to know when the data was pulled, often as an end dttm to determining activity. To approximate that, we'll use the maximum out time.
update ##pj_adt_summary 
set EXTRACTION_DTTM_APPROX = (select max(UNIT_OUT_DTTM) as max_out_dttm from ##pj_adt_summary)
GO

--*****************             update OP cases billed as IPs with discharge date  *************************************************
update ##pj_adt_summary
set UNIT_STATUS = 'DISCHARGED',
	NEXT_IP_UNIT_STAY_TYPE_CSN = 'DISCHARGED',
	NEXT_IP_UNIT_STAY_TYPE_HAR = 'DISCHARGED'
from ##pj_adt_summary x
	join pat_enc_hsp peh on x.PAT_ENC_CSN = peh.PAT_ENC_CSN_ID
	join
	(select PAT_ENC_CSN, max(rownum_summary) as max_line
	from ##pj_adt_summary
	group by PAT_ENC_CSN) sub on x.pat_enc_csn = sub.pat_enc_csn
								and x.rownum_summary = sub.max_line
where x.UNIT_STATUS not in ('DISCHARGED','PATIENT IN UNIT')  --pt wasn't discharged or currently in the unit
	and NEXT_IP_UNIT_STAY_TYPE_CSN is null  
	and x.UNIT_OUT_DTTM = peh.HOSP_DISCH_TIME
GO
--*******************************      update TEST table      *****************************************************************************
/*
--delete duplicate
DELETE ##X_ADT_IP_UNIT_ACTIVITY
FROM ##X_ADT_IP_UNIT_ACTIVITY x
	JOIN
	(select distinct PAT_ENC_CSN from ##pj_adt_summary) sub	on x.PAT_ENC_CSN = sub.PAT_ENC_CSN  --csns in this pull


--add new records to x_adt_ip_unit_activity
INSERT INTO ##X_ADT_IP_UNIT_ACTIVITY
SELECT PAT_ENC_CSN, rownum_summary as "LINE",	HSP_ACCOUNT_ID,	UNIT_STAY_TYPE,	ADT_DEPARTMENT_ID,	UNIT_IN_DTTM,	UNIT_OUT_DTTM,	UNIT_STATUS,	IP_UNIT_TRACKER_CSN,	ICU_TRACKER_CSN,	NEXT_IP_DEPT_ID,			NEXT_IP_UNIT_STAY_TYPE_CSN,	IP_UNIT_TRACKER_HAR,	ICU_TRACKER_HAR,	NEXT_IP_UNIT_STAY_TYPE_HAR,	ANCILLARY_VISITS,		ICU_BOUNCE_BACK_HRS_CSN,	ICU_BOUNCE_BACK_HRS_HAR,	BILLED_AS_OP,	CEDAR_SPRUCE_TXN_YN,	NORMAL_NEWBORN_YN,	EXTRACTION_DTTM_APPROX,	HOSP,	HUP_SITE, HOSPICE_ADMISSION_YN, TABLE_OWNER = 'HUP: P. Junker'
--INTO clarity_custom_tables.dbo.x_adt_ip_unit_activity
from ##pj_adt_summary
order by 1, 2

IF OBJECT_ID('tempdb..#pj_unit_flow_duplicates') IS NOT NULL BEGIN DROP TABLE #pj_unit_flow_duplicates END;
CREATE TABLE #pj_unit_flow_duplicates (
	  pat_enc_csn_id numeric(18,0)
	)


INSERT INTO #pj_unit_flow_duplicates
select distinct x.pat_enc_csn
from /* clarity_custom_tables.dbo.X_ADT_IP_UNIT_ACTIVITY x */ ##X_ADT_IP_UNIT_ACTIVITY x
	JOIN
	(select HSP_ACCOUNT_ID
	from /* clarity_custom_tables.dbo.X_ADT_IP_UNIT_ACTIVITY */ ##X_ADT_IP_UNIT_ACTIVITY x
	where IP_UNIT_TRACKER_HAR = 1
	group by HSP_ACCOUNT_ID 
	having count(distinct PAT_ENC_CSN) > 1) sub 
	on x.HSP_ACCOUNT_ID = sub.HSP_ACCOUNT_ID
--create temp table
IF OBJECT_ID('tempdb..#pj_IP_UNIT_TRACKER_HAR_2') IS NOT NULL BEGIN DROP TABLE #pj_IP_UNIT_TRACKER_HAR_2 END;
select x.HSP_ACCOUNT_ID
	,ROW_NUMBER() over (partition by x.HSP_ACCOUNT_ID order by x.unit_in_dttm) as "IP_UNIT_TRACKER_HAR"
	,x.UNIT_IN_DTTM, x.ADT_DEPARTMENT_ID
into #pj_IP_UNIT_TRACKER_HAR_2
from /* clarity_custom_tables.dbo.X_ADT_IP_UNIT_ACTIVITY x */ ##X_ADT_IP_UNIT_ACTIVITY x
	join #pj_unit_flow_duplicates y 
		on x.PAT_ENC_CSN = y.pat_enc_csn_id  --only want accounts with HAR Dups
where UNIT_STAY_TYPE not like  ('NON IP%')



--update admission tracker
update /* clarity_custom_tables.dbo.X_ADT_IP_UNIT_ACTIVITY */ ##X_ADT_IP_UNIT_ACTIVITY
set IP_UNIT_TRACKER_HAR = y.IP_UNIT_TRACKER_HAR
from /* clarity_custom_tables.dbo.X_ADT_IP_UNIT_ACTIVITY x */ ##X_ADT_IP_UNIT_ACTIVITY x
	join #pj_IP_UNIT_TRACKER_HAR_2 y on
		x.HSP_ACCOUNT_ID = y.HSP_ACCOUNT_ID and
		x.UNIT_IN_DTTM = y.UNIT_IN_DTTM and
		x.ADT_DEPARTMENT_ID = y.ADT_DEPARTMENT_ID


--update icu tracker, create temp table
IF OBJECT_ID('tempdb..#pj_covid_icu_adm_tracker_har_2') IS NOT NULL BEGIN DROP TABLE #pj_covid_icu_adm_tracker_har_2 END;
select x.HSP_ACCOUNT_ID
	,ROW_NUMBER() over (partition by x.HSP_ACCOUNT_ID order by x.unit_in_dttm) as "ICU_ADMISSION_TRACKER"
	,x.UNIT_IN_DTTM, x.ADT_DEPARTMENT_ID
into #pj_covid_icu_adm_tracker_har_2
from /* clarity_custom_tables.dbo.X_ADT_IP_UNIT_ACTIVITY x */ ##X_ADT_IP_UNIT_ACTIVITY x
	join #pj_unit_flow_duplicates y on x.PAT_ENC_CSN = y.pat_enc_csn_id  --only want accounts with HAR Dups 
where UNIT_STAY_TYPE = 'CRITICAL CARE'


--update icu admission tracker
update /* clarity_custom_tables.dbo.X_ADT_IP_UNIT_ACTIVITY */ ##X_ADT_IP_UNIT_ACTIVITY
set ICU_TRACKER_HAR = y.ICU_ADMISSION_TRACKER
from /* clarity_custom_tables.dbo.X_ADT_IP_UNIT_ACTIVITY x */ ##X_ADT_IP_UNIT_ACTIVITY x
	join #pj_covid_icu_adm_tracker_har_2 y on
		x.HSP_ACCOUNT_ID = y.HSP_ACCOUNT_ID and
		x.UNIT_IN_DTTM = y.UNIT_IN_DTTM and
		x.ADT_DEPARTMENT_ID = y.ADT_DEPARTMENT_ID


*/

--*******************************      update permanent table      *****************************************************************************
--delete duplicates
DELETE clarity_custom_tables.dbo.X_ADT_IP_UNIT_ACTIVITY
FROM clarity_custom_tables.dbo.X_ADT_IP_UNIT_ACTIVITY x
	JOIN
	(select distinct PAT_ENC_CSN from ##pj_adt_summary) sub	on x.PAT_ENC_CSN = sub.PAT_ENC_CSN  --csns in this pull


--add new records to x_adt_ip_unit_activity
INSERT INTO clarity_custom_tables.dbo.x_adt_ip_unit_activity
SELECT PAT_ENC_CSN, rownum_summary as "LINE",	HSP_ACCOUNT_ID,	UNIT_STAY_TYPE,	ADT_DEPARTMENT_ID,	UNIT_IN_DTTM,	UNIT_OUT_DTTM,	UNIT_STATUS,	IP_UNIT_TRACKER_CSN,	ICU_TRACKER_CSN,	NEXT_IP_DEPT_ID,			NEXT_IP_UNIT_STAY_TYPE_CSN,	IP_UNIT_TRACKER_HAR,	ICU_TRACKER_HAR,	NEXT_IP_UNIT_STAY_TYPE_HAR,	ANCILLARY_VISITS,		ICU_BOUNCE_BACK_HRS_CSN,	ICU_BOUNCE_BACK_HRS_HAR,	BILLED_AS_OP,	CEDAR_SPRUCE_TXN_YN,	NORMAL_NEWBORN_YN,	EXTRACTION_DTTM_APPROX,	HOSP,	HUP_SITE, HOSPICE_ADMISSION_YN, TABLE_OWNER = 'HUP: P. Junker'
from ##pj_adt_summary

GO


--***********************************    Create table with CSNs that get "duplicate HARs"    *********************************** 
--Note, these occur because of weird issues with combined accounts. We were not able to always catch the problems with logic, 
--but this worked well.
IF OBJECT_ID('tempdb..#pj_unit_flow_duplicates') IS NOT NULL BEGIN DROP TABLE #pj_unit_flow_duplicates END;
CREATE TABLE #pj_unit_flow_duplicates (
	  pat_enc_csn_id numeric(18,0)
	)

--this might be able to be written more efficiently, but it runs super fast & it's hard to test since 
--the data are fixed each day
INSERT INTO #pj_unit_flow_duplicates
select distinct x.pat_enc_csn
from clarity_custom_tables.dbo.X_ADT_IP_UNIT_ACTIVITY x
	JOIN
	(select HSP_ACCOUNT_ID
	from clarity_custom_tables.dbo.X_ADT_IP_UNIT_ACTIVITY
	where IP_UNIT_TRACKER_HAR = 1
	group by HSP_ACCOUNT_ID 
	having count(distinct PAT_ENC_CSN) > 1) sub 
	on x.HSP_ACCOUNT_ID = sub.HSP_ACCOUNT_ID


--*************************          update trackers TO FIX DUP ACCOUNTS - HAR       **************************************************
--create temp table
IF OBJECT_ID('tempdb..#pj_IP_UNIT_TRACKER_HAR_2') IS NOT NULL BEGIN DROP TABLE #pj_IP_UNIT_TRACKER_HAR_2 END;
select x.HSP_ACCOUNT_ID
	,ROW_NUMBER() over (partition by x.HSP_ACCOUNT_ID order by x.unit_in_dttm) as "IP_UNIT_TRACKER_HAR"
	,x.UNIT_IN_DTTM, x.ADT_DEPARTMENT_ID
into #pj_IP_UNIT_TRACKER_HAR_2
from clarity_custom_tables.dbo.X_ADT_IP_UNIT_ACTIVITY x
	join #pj_unit_flow_duplicates y 
		on x.PAT_ENC_CSN = y.pat_enc_csn_id  --only want accounts with HAR Dups
where UNIT_STAY_TYPE not like  ('NON IP%')
GO

--update admission tracker
update clarity_custom_tables.dbo.X_ADT_IP_UNIT_ACTIVITY
set IP_UNIT_TRACKER_HAR = y.IP_UNIT_TRACKER_HAR
from clarity_custom_tables.dbo.X_ADT_IP_UNIT_ACTIVITY x
	join #pj_IP_UNIT_TRACKER_HAR_2 y on
		x.HSP_ACCOUNT_ID = y.HSP_ACCOUNT_ID and
		x.UNIT_IN_DTTM = y.UNIT_IN_DTTM and
		x.ADT_DEPARTMENT_ID = y.ADT_DEPARTMENT_ID
GO

--update icu tracker, create temp table
IF OBJECT_ID('tempdb..#pj_covid_icu_adm_tracker_har_2') IS NOT NULL BEGIN DROP TABLE #pj_covid_icu_adm_tracker_har_2 END;
select x.HSP_ACCOUNT_ID
	,ROW_NUMBER() over (partition by x.HSP_ACCOUNT_ID order by x.unit_in_dttm) as "ICU_ADMISSION_TRACKER"
	,x.UNIT_IN_DTTM, x.ADT_DEPARTMENT_ID
into #pj_covid_icu_adm_tracker_har_2
from clarity_custom_tables.dbo.X_ADT_IP_UNIT_ACTIVITY x
	join #pj_unit_flow_duplicates y on x.PAT_ENC_CSN = y.pat_enc_csn_id  --only want accounts with HAR Dups 
where UNIT_STAY_TYPE = 'CRITICAL CARE'
GO

--update icu admission tracker
update clarity_custom_tables.dbo.X_ADT_IP_UNIT_ACTIVITY
set ICU_TRACKER_HAR = y.ICU_ADMISSION_TRACKER
from clarity_custom_tables.dbo.X_ADT_IP_UNIT_ACTIVITY x
	join #pj_covid_icu_adm_tracker_har_2 y on
		x.HSP_ACCOUNT_ID = y.HSP_ACCOUNT_ID and
		x.UNIT_IN_DTTM = y.UNIT_IN_DTTM and
		x.ADT_DEPARTMENT_ID = y.ADT_DEPARTMENT_ID
GO
