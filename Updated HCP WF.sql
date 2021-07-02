/*Drop table TMP_WF_HCP_PARTY
Drop table TMP_WF_HCP_MARKET
Drop table TMP_WF_HCP_MERGE
Drop table TMP_WF_HCP_SOURCE
Drop table TMP_WF_MS_HCP_COMB */
----------------------------------------------------------------------------------------------------------------------------
Create Table TMP_WF_HCP_PARTY as
--Select distinct PARTY_TYPE, HCP_TYPE, PRACTICE_TYPE, HCP_STATUS, RECORD_STATE_CD from
(
Select distinct HCP_Party.PARTY_SK,
  HCP_Party.MDM_KEY,
  HCP_Party.PARTY_TYPE_CD_FK,
  HCP_Party.FIRST_NAME,
  HCP_Party.MIDDLE_NAME,
  HCP_Party.LAST_NAME,
  HCP_Party.TITLE,
  HCP_Party.HCP_TYPE_CD_FK,
  HCP_Party.PRACTICE_TYPE_CD_FK,
  HCP_Party.HCP_STATUS_CD_FK,
   HCP_Party.AMA_OPT_OUT_FLAG,
 HCP_Party.RECORD_STATE_CD_FK,
 CREATED_DT as Create_Dt,
 Code1.Code_Name as Party_Type,
 Code2.Code_Name as HCP_Type,
 Code3.Code_Name as Practice_Type,
 Code4.Code_Name as HCP_Status,
 Code5.Code_Name as Record_State_Cd,
 SMA_SEGMENT__C as SMA_HCP_Segment
 --case when PROF_SK is not null then 'NBA HCP' else 'Non-NBA HCP' end as HCP_Source,
 --NBA.Email as Email_Address
 from
(
SELECT DISTINCT PRTY.PARTY_SK,
  PRTY.MDM_KEY,
  PRTY.PARTY_TYPE_CD_FK,
  PRTY_HCP.SALUTATION_CD_FK,
  PRTY_HCP.FIRST_NAME,
  PRTY_HCP.MIDDLE_NAME,
  PRTY_HCP.LAST_NAME,
  PRTY_HCP.TITLE,
  PRTY_HCP.PROFESSIONAL_TITLE_CD_FK,
  PRTY_HCP.GENDER_CD_FK,
  PRTY_HCP.ACADEMIC_TITLE_CD_FK,
  PRTY_HCP.EMPLOYMENT_PLACE_CD_FK,
  PRTY_HCP.PROFESSIONAL_LEVEL_CD_FK,
  PRTY_HCP.PRIMARY_MED_DEG_CD_FK,
  PRTY_HCP.MAJOR_PRFNL_ACT_CD_FK,
  PRTY_HCP.SECONDARY_MED_DEG_CD_FK,
  PRTY_HCP.HCP_TYPE_CD_FK,
  PRTY_HCP.PRACTICE_TYPE_CD_FK,
  PRTY_HCP.HCP_STATUS_CD_FK,
  COUN.COUNTRY_NAME,
  PRTY_HCP.THOUGHT_LEADER_TIER,
  PRTY_HCP.SALUTATION,
 PRTY.CREATED_DT,
 PRTY_FL.ALLOWED_FLG AMA_OPT_OUT_FLAG,
 PRTY_HCP.RECORD_STATE_CD_FK
FROM DH_IDS.IDS_PARTY PRTY
INNER JOIN DH_IDS.IDS_PARTY_HCP PRTY_HCP
ON PRTY.PARTY_SK    = PRTY_HCP.PARTY_FK
LEFT OUTER JOIN 
(
SELECT PARTY_FK, ALLOWED_FLG FROM
DH_IDS.IDS_PARTY_FLAG PRTY_FL
INNER JOIN DH_IDS.IDS_CODE FL_CD
ON FL_CD.CODE_SK = PRTY_FL.FLAG_TYPE_CD_FK
WHERE FL_CD.CODE_NAME = 'AMA_DO_NOT_CONTACT_FLG'
)PRTY_FL
ON PRTY.PARTY_SK = PRTY_FL.PARTY_FK

INNER JOIN
--New PARTY_PM
(
SELECT CASE
WHEN ta_parent.THERAPEUTIC_AREA IN ('HEMOPHILIA','HEMATOLOGY') THEN 'HEM'
WHEN ta_parent.THERAPEUTIC_AREA IN ('NEUROLOGY','MULTIPLE SCLEROSIS','CNS') THEN 'MS'
ELSE NULL END THERAPEUTIC_AREA
, PM.PARTY_FK
FROM DH_IDS.IDS_PARTY_MARKET PM
INNER JOIN DH_IDS.IDS_THERAPEUTIC_AREA ta_child
ON PM.THERAPEUTIC_AREA_FK = ta_child.THERAPEUTIC_AREA_SK
LEFT OUTER JOIN  DH_IDS.IDS_THERAPEUTIC_AREA ta_parent
ON ta_child.PARENT_THERAPEUTIC_AREA_FK = ta_parent.THERAPEUTIC_AREA_SK
) PRTY_PM
/*(
SELECT CASE 
WHEN TA.THERAPEUTIC_AREA IN ('HEMOPHILIA','HEMATOLOGY') THEN 'HEM'
WHEN TA.THERAPEUTIC_AREA IN ('NEUROLOGY','MULTIPLE SCLEROSIS','CNS) THEN 'MS'
ELSE NULL END THERAPEUTIC_AREA
, PM.PARTY_FK
FROM DH_IDS.IDS_PARTY_MARKET PM
INNER JOIN DH_IDS.IDS_THERAPEUTIC_AREA TA
ON TA.THERAPEUTIC_AREA_SK = PM.THERAPEUTIC_AREA_FK
--INNER JOIN DH_IDS.IDS_CODE STATE_CD
--ON STATE_CD.CODE_SK = PM.RECORD_STATE_CD_FK
--WHERE STATE_CD.CODE_NAME = '$$RECORD_STATE_ACTIVE'
)PRTY_PM*/
ON PRTY_PM.PARTY_FK = PRTY_HCP.PARTY_FK

INNER JOIN DH_IDS.IDS_COUNTRY_CODE COUN
ON PRTY_HCP.PRIMARY_COUNTRY_CD_FK = COUN.COUNTRY_SK


INNER JOIN 
(SELECT COD.CODE_SK, COD.CODE_NAME
FROM 
DH_IDS.IDS_CODE COD
INNER JOIN
DH_IDS.IDS_DOMAIN_CODE DOM 
ON COD.DOMAIN_FK = DOM.DOMAIN_SK
WHERE DOM.DOMAIN_NAME ='Record State'
AND ((COD.CODE_NAME  IN ('ACTIVE') AND (SELECT EXTRACT_DATE FROM DH_ABC.T_DATE_PARAM_CTRL WHERE ETL_NAME = 'ETL_US_PM_DATE_PARAM_EXTGT_OUT_PARTY_HCP') = TO_DATE('01/01/2014','MM/DD/YYYY'))
  OR (COD.CODE_NAME IN ('ACTIVE','INACTIVE','DELETED') AND (SELECT EXTRACT_DATE FROM DH_ABC.T_DATE_PARAM_CTRL WHERE ETL_NAME = 'ETL_US_PM_DATE_PARAM_EXTGT_OUT_PARTY_HCP') > 
  TO_DATE('01/01/2014','MM/DD/YYYY'))) )RS_CD
ON PRTY_HCP.RECORD_STATE_CD_FK = RS_CD.CODE_SK

WHERE PRTY.PARTY_SK > 0
AND  PRTY.MDM_KEY IS NOT NULL
AND COUN.ISO_3_CD IN ('USA')
AND PRTY_PM.THERAPEUTIC_AREA IN ('HEM','MS')
)
 HCP_Party,
dh_ids.ids_code code1,dh_ids.ids_code code2 ,dh_ids.ids_code code3,dh_ids.ids_code code4,dh_ids.ids_code code5, 
(select substr(MDM_ID,-18) as MDM_ID,SMA_SEGMENT__C from (
select distinct MDM_ID,SMA_SEGMENT__C,
ROW_NUMBER() OVER(PARTITION BY MDM_ID ORDER BY SMA_SEGMENT__C) AS Row_id
from dm_sma.sma_hcp_hco_raw) 
where Row_id = 1 )
SMA_HCP
--, CIM_RPT.NBA_HCP_VIEW NBA
where HCP_Party.PARTY_TYPE_CD_FK = Code1.Code_Sk(+)
and HCP_Party.HCP_TYPE_CD_FK = Code2.Code_Sk(+)
and HCP_Party.PRACTICE_TYPE_CD_FK = Code3.Code_Sk(+)
and HCP_Party.HCP_STATUS_CD_FK = Code4.Code_Sk(+)
and HCP_Party.RECORD_STATE_CD_FK = Code5.Code_Sk(+)
and HCP_Party.MDM_KEY = SMA_HCP.MDM_ID(+)
--and HCP_Party.MDM_KEY = NBA.PROF_SK(+)
)

----------------------------------------------------------------------------------------------------
Create Table TMP_WF_HCP_MARKET as (
Select Distinct MDM_ID,PARTY_MARKET_ID,CHILD_TA from
(SELECT DISTINCT 
                'HCP' AS FEED_ORIGIN,
                PARTY_MARKET_SK AS PARTY_MARKET_ID,
                PARTY.MDM_KEY              AS MDM_ID,
                TA_PARENT.THERAPEUTIC_AREA AS PARENT_TA,
                TA_CHILD.THERAPEUTIC_AREA  AS CHILD_TA,
                PARTY_MKT.SOURCE AS SOURCE,
                MARKET.MARKET_NAME AS MARKET,
                PARTY_MKT.RECORD_STATE_CD AS RECORD_STATE_CD
  FROM DH_IDS.V_IDS_PARTY            PARTY,
       DH_IDS.V_IDS_PARTY_HCP        HCP,
       DH_IDS.V_IDS_PARTY_MARKET     PARTY_MKT,
       DH_IDS.V_IDS_THERAPEUTIC_AREA TA_CHILD,
       DH_IDS.V_IDS_THERAPEUTIC_AREA TA_PARENT,
       DH_IDS.IDS_PRODUCT_MARKET     MARKET,
       (SELECT COD.CODE_SK, COD.CODE_NAME
			FROM DH_IDS.IDS_CODE COD
			INNER JOIN DH_IDS.IDS_DOMAIN_CODE DOM
			ON COD.DOMAIN_FK = DOM.DOMAIN_SK
			WHERE DOM.DOMAIN_NAME ='Record State'
			AND ((COD.CODE_NAME  IN ('ACTIVE') AND (SELECT EXTRACT_DATE FROM DH_ABC.T_DATE_PARAM_CTRL WHERE ETL_NAME = 'ETL_US_PM_DATE_PARAM_EXTGT_OUT_PARTY_MKT') = TO_DATE('01/01/2014','MM/DD/YYYY'))
  				OR (COD.CODE_NAME IN ('ACTIVE','INACTIVE','DELETED') AND (SELECT EXTRACT_DATE FROM DH_ABC.T_DATE_PARAM_CTRL WHERE ETL_NAME = 'ETL_US_PM_DATE_PARAM_EXTGT_OUT_PARTY_MKT') > TO_DATE('01/01/2014','MM/DD/YYYY'))) )RS_CD	   
 WHERE PARTY.PARTY_SK = HCP.PARTY_FK
   AND PARTY.PARTY_SK > 0
   AND PARTY.MDM_KEY IS NOT NULL
   AND HCP.PRIMARY_COUNTRY_CD = 'USA'
   AND HCP.PARTY_FK = PARTY_MKT.PARTY_FK(+)
   AND PARTY_MKT.THERAPEUTIC_AREA_FK = TA_CHILD.THERAPEUTIC_AREA_SK(+)
   AND TA_CHILD.PARENT_THERAPEUTIC_AREA_FK =  TA_PARENT.THERAPEUTIC_AREA_SK(+)  
   AND TA_PARENT.THERAPEUTIC_AREA IN ('HEMATOLOGY','NEUROLOGY')   
   AND PARTY_MKT.PRODUCT_MARKET_FK = MARKET.PRODUCT_MARKET_SK(+)
   AND HCP.RECORD_STATE_CD = RS_CD.CODE_NAME
   
   ) where upper(Child_Ta) in ('MULTIPLE SCLEROSIS','SPINAL MUSCULAR ATROPHY') --and upper(Record_State_Cd) = 'ACTIVE'
   )
--select distinct Child_Ta,count(distinct MDM_ID) from TMP_WF_MS_HCP_MARKET group by Child_Ta order by 2 desc
--select * from TMP_WF_MS_HCP_MARKET where MDM_ID = '243237904816538625'  and Child_Ta = 'MULTIPLE SCLEROSIS'

--------------------------------------------------------------------------------------------------------------------------------

Create Table TMP_WF_HCP_MERGE as 
(
SELECT DISTINCT
  PRTY_SV.MDM_KEY NEW_MDM_ID
, PRTY_NSV.MDM_KEY EXISTING_MDM_ID
, UPPER(SUBSTR(PRTY_TYP_CD.CODE_NAME,1,3)) PARTY_TYPE
, PRTY_MP.SRC_MERGE_DT
, CODE.CODE_NAME AS RECORD_STATUS_CD
FROM DH_IDS.IDS_PARTY_MERGE_PURGE_MAP PRTY_MP
INNER JOIN
DH_IDS.IDS_PARTY PRTY_SV
ON PRTY_SV.PARTY_SK=PRTY_MP.SURVIVOR_PARTY_FK
LEFT OUTER JOIN
DH_IDS.IDS_PARTY PRTY_NSV
ON PRTY_NSV.PARTY_SK=PRTY_MP.NON_SURVIVOR_PARTY_FK
LEFT OUTER JOIN DH_IDS.IDS_CODE PRTY_TYP_CD
ON PRTY_SV.PARTY_TYPE_CD_FK=PRTY_TYP_CD.CODE_SK
 LEFT OUTER JOIN DH_IDS.IDS_CODE CODE
  ON PRTY_MP.RECORD_STATE_CD_FK = CODE.CODE_SK

INNER JOIN DH_IDS.IDS_PARTY_HCP PRTY_HCP
ON PRTY_SV.PARTY_SK = PRTY_HCP.PARTY_FK

INNER JOIN DH_IDS.IDS_COUNTRY_CODE COUN
ON PRTY_HCP.PRIMARY_COUNTRY_CD_FK = COUN.COUNTRY_SK

INNER JOIN 
(SELECT COD.CODE_SK, COD.CODE_NAME
FROM DH_IDS.IDS_CODE COD
INNER JOIN DH_IDS.IDS_DOMAIN_CODE DOM 
ON COD.DOMAIN_FK = DOM.DOMAIN_SK
WHERE DOM.DOMAIN_NAME ='Record State'
AND ((COD.CODE_NAME  IN ('ACTIVE') AND (SELECT EXTRACT_DATE FROM DH_ABC.T_DATE_PARAM_CTRL WHERE ETL_NAME = 'ETL_US_PM_DATE_PARAM_EXTGT_OUT_SRC_MDM_MERGE') = TO_DATE('01/01/2014','MM/DD/YYYY'))
  OR (COD.CODE_NAME IN ('ACTIVE','INACTIVE','DELETED') AND (SELECT EXTRACT_DATE FROM DH_ABC.T_DATE_PARAM_CTRL WHERE ETL_NAME = 'ETL_US_PM_DATE_PARAM_EXTGT_OUT_SRC_MDM_MERGE') > 
  TO_DATE('01/01/2014','MM/DD/YYYY'))) )RS_CD
ON PRTY_HCP.RECORD_STATE_CD_FK = RS_CD.CODE_SK

INNER JOIN
--New PARTY_PM
(
SELECT CASE
WHEN ta_parent.THERAPEUTIC_AREA IN ('HEMOPHILIA','HEMATOLOGY') THEN 'HEM'
WHEN ta_parent.THERAPEUTIC_AREA IN ('NEUROLOGY','MULTIPLE SCLEROSIS','CNS') THEN 'MS'
ELSE NULL END THERAPEUTIC_AREA
, PM.PARTY_FK
FROM DH_IDS.IDS_PARTY_MARKET PM
INNER JOIN DH_IDS.IDS_THERAPEUTIC_AREA ta_child
ON PM.THERAPEUTIC_AREA_FK = ta_child.THERAPEUTIC_AREA_SK
LEFT OUTER JOIN  DH_IDS.IDS_THERAPEUTIC_AREA ta_parent
ON ta_child.PARENT_THERAPEUTIC_AREA_FK = ta_parent.THERAPEUTIC_AREA_SK
) PRTY_PM
ON PRTY_PM.PARTY_FK = PRTY_HCP.PARTY_FK

WHERE PRTY_SV.PARTY_SK > 0
AND PRTY_SV.MDM_KEY IS NOT NULL
AND PRTY_TYP_CD.CODE_NAME IN ('HCP')
AND COUN.ISO_3_CD IN ('USA')
AND PRTY_PM.THERAPEUTIC_AREA IN ('HEM','MS')
)

--------------------------------------------------------------------------------------------------------------------------------
Create Table  TMP_WF_HCP_SOURCE as 

(
select hcp_source.*, CASE WHEN mktg_prod.product_name = 'MULTIPLE SCLEROSIS' THEN 'ABOVE MS'
WHEN mktg_prod.product_name = 'SPINAL MUSCULAR ATROPHY' THEN 'TOGETHER IN SMA' ELSE mktg_prod.product_name END AS PRODUCT_NAME 
from 
(
(Select distinct  to_number(LS.PARTY_FK) as Party_Fk ,nvl(LS.TRUE_SOURCE,LS.source_name) as Lead_Source,LS.CREATE_DT as Source_Dt, CHILD_TA,nvl(Comm_email.COMM_VALUE ,PD.EMAIL_ADDR) as Party_Email_Address, '' as NBA_Email_Address,
'SFMC HCP' as HCP_Source
from 
TMP_PF_MKTG_PARTY_FIRST_SRC LS,TMP_WF_HCP_MARKET HCP_Market, DM_MS.T_PARTY_DIM PD, DH_IDS.TMP_COMM_EMAIL Comm_Email
--, TMP_WF_HCP_PARTY HCP_Party
where LS.PARTY_FK = PD.Party_Sk and
'VNID' || PD.VNID =  'VNID' || HCP_Market.MDM_ID 
and Comm_Email.Party_Fk(+) = LS.PARTY_FK
--and LS.PARTY_FK = HCP_Party.Party_Sk
and CHILD_TA = 'MULTIPLE SCLEROSIS') 

union all

(Select distinct Final_party_Key,Lead_Source,Source_Dt,CHILD_TA,EMAIL_ADDR,NBA_Email_Address,HCP_Source from ( 
Select to_number(LS.PARTY_FK) as Party_Sk ,to_number(NBA.PROF_SK) as NBA_MDM_KEY, to_number(nvl(LS.PARTY_FK,NBA.PROF_SK)) as Final_party_Key
,nvl(nvl(LS.TRUE_SOURCE,LS.source_name),'NBA HCP') as Lead_Source,nvl(nvl(LS.CREATE_DT,HCP_Party.Create_Dt),to_date('01/01/2020','MM/DD/YYYY')) as Source_Dt,'MULTIPLE SCLEROSIS' as CHILD_TA,nvl(Comm_email.COMM_VALUE ,PD.EMAIL_ADDR) as EMAIL_ADDR ,NBA.Email as NBA_Email_Address, 
'NBA HCP' as HCP_Source
from 
TMP_PF_MKTG_PARTY_FIRST_SRC LS,CIM_RPT.NBA_HCP_VIEW NBA , DM_MS.T_PARTY_DIM PD, TMP_WF_HCP_PARTY HCP_Party, DH_IDS.TMP_COMM_EMAIL Comm_Email
where NBA.PROF_SK = PD.VNID(+)
and HCP_Party.MDM_KEY(+) = NBA.PROF_SK
and LS.PARTY_FK(+) = HCP_Party.Party_Sk
and Comm_Email.Party_Fk(+) = LS.PARTY_FK
--and LS.PARTY_FK = PD.Party_Sk
)
) 

union all 

(Select distinct  to_number(LS.PARTY_FK) as Party_Fk ,nvl(LS.TRUE_SOURCE,LS.source_name) as Lead_Source,LS.CREATE_DT as Source_Dt,CHILD_TA,nvl(Comm_email.COMM_VALUE ,PD.EMAIL_ADDR) as Party_Email_Address, '' as NBA_Email_Address,
'' as HCP_Source
from 
TMP_PF_MKTG_PARTY_FIRST_SRC LS,TMP_WF_HCP_MARKET HCP_Market, DM_MS.T_PARTY_DIM PD, DH_IDS.TMP_COMM_EMAIL Comm_Email
--, TMP_WF_HCP_PARTY HCP_Party
where LS.PARTY_FK = PD.Party_Sk and
'VNID' || PD.VNID =  'VNID' || HCP_Market.MDM_ID 
and Comm_Email.Party_Fk(+) = LS.PARTY_FK
--and LS.PARTY_FK = HCP_Party.Party_Sk
and CHILD_TA = 'SPINAL MUSCULAR ATROPHY')
) hcp_source

LEFT JOIN (SELECT DISTINCT PRODUCT_NAME,THERAPEUTIC_AREA FROM DM_MKTG.T_HCP_MKTG_ACTIVITY_NEW) mktg_prod
ON hcp_source.CHILD_TA = mktg_prod.THERAPEUTIC_AREA
AND PRODUCT_NAME IN ('AVONEX','PLEGRIDY','TYSABRI','TECFIDERA','VUMERITY','MULTIPLE SCLEROSIS','SPINAL MUSCULAR ATROPHY','SPINRAZA')
)


--------------------------------------------------------------------------------------------------------------------------------
Create TABLE TMP_WF_HCP_COMB as 



Select TMP_WF3.*,
case when SUPPRESSION_CRITERIA in ('Blank/Invalid Email Address','Kaiser Suppression') and SOURCE_DT < SUPPRESSION_DATE then SOURCE_DT else SUPPRESSION_DATE end as FINAL_SUPPRESSION_DATE,
case when SOURCE_DT > SUPPRESSION_DATE then SUPPRESSION_DATE else SOURCE_DT end as FINAL_SOURCE_DT,
  'HCP' as WF_Customer_Type from 
(
Select TMP_WF2.*, case when Party_Suppression_Date < Opt_Out_Date and Party_Suppression_Date < Merge_Record_Date and Party_Suppression_Date < Bounce_Date then Party_Suppression_Date
when Opt_Out_Date < Party_Suppression_Date and Opt_Out_Date < Merge_Record_Date and Opt_Out_Date < Bounce_Date and Opt_Out_Flag = 'Y' then Opt_Out_Date
when Merge_Record_Date < Party_Suppression_Date and Merge_Record_Date < Opt_Out_Date and Merge_Record_Date < Bounce_Date and ET_Active = 'N' then Merge_Record_Date
when Bounce_Date < Party_Suppression_Date and Bounce_Date < Opt_Out_Date and Bounce_Date < Merge_Record_Date and Bounce_Flag = 1 then Bounce_Date
when (Party_Suppression_Date is null and Opt_Out_Date is null and Merge_Record_Date is null and  Bounce_Date is null) then Source_Dt else Source_Dt end as Suppression_Date,

case when Party_Suppression_Date < Opt_Out_Date and Party_Suppression_Date < Merge_Record_Date and Party_Suppression_Date < Bounce_Date then TMP_Suppression_Criteria
when Opt_Out_Date < Party_Suppression_Date and Opt_Out_Date < Merge_Record_Date and Opt_Out_Date < Bounce_Date and Opt_Out_Flag = 'Y' then Product_Name || ' Opt Out'
when Merge_Record_Date < Party_Suppression_Date and Merge_Record_Date < Opt_Out_Date and Merge_Record_Date < Bounce_Date and ET_Active = 'N' then 'Merged Record'
when Bounce_Date < Party_Suppression_Date and Bounce_Date < Opt_Out_Date and Bounce_Date < Merge_Record_Date and Bounce_Flag = 1  then 'Held Record'
when (Party_Suppression_Date is null and Opt_Out_Date is null and Merge_Record_Date is null and  Bounce_Date is null) then 'Emailable Record' else TMP_Suppression_Criteria end as Suppression_Criteria,

case --when CREATE_DATE < Opt_Out_Date and CREATE_DATE < Merge_Record_Date and CREATE_DATE < Bounce_Date then 'Party_table'
when Opt_Out_Date < Party_Suppression_Date and Opt_Out_Date < Merge_Record_Date and Opt_Out_Date < Bounce_Date and Opt_Out_Flag = 'Y' then 'Opt table'
when Merge_Record_Date < Party_Suppression_Date and Merge_Record_Date < Opt_Out_Date and Merge_Record_Date < Bounce_Date and ET_Active = 'N' then 'Merge Table'
when Bounce_Date < Party_Suppression_Date and Bounce_Date < Opt_Out_Date and Bounce_Date < Merge_Record_Date and Bounce_Flag = 1 then 'Bounce Table'
when TMP_Suppression_Criteria <> 'Emailable Record' /*and Opt_Out_Flag <> 'Y' and ET_Active <> 'N' and Bounce_Flag <> 1*/  then 'Party Table'
when (Party_Suppression_Date is null and Opt_Out_Date is null and Merge_Record_Date is null and  Bounce_Date is null) then 'No Suppression' else 'No Suppression' end as Suppression_Source
from
(
Select TMP_WF1.*, case when Tmp_Suppression_Criteria not in ('Record State Inactive','Status Inactive','Non Prescribers','AMA Opt Out','Blank/Invalid Email Address','Kaiser Suppression')
then to_date('12/31/9999','MM/DD/YYYY') else CREATE_DT end as Party_Suppression_Date from
(
Select distinct PARTY_FK, MDM_KEY, AMA_OPT_OUT_FLAG, CREATE_DT,
PARTY_TYPE, HCP_TYPE,HCP_Source, PRACTICE_TYPE, HCP_STATUS, RECORD_STATE_CD,SMA_HCP_Segment, LEAD_SOURCE, SOURCE_DT, CHILD_TA, PARTY_EMAIL_ADDRESS, NBA_EMAIL_ADDRESS, nvl(ET_ACTIVE,'Y') as ET_ACTIVE , BOUNCE_FLAG, IW_Flg,OPT_OUT_FLAG, PRODUCT_NAME,
case when HCP_Status <> 'Active' then 'Status Inactive'
when RECORD_STATE_CD <> 'ACTIVE' then 'Record State Inactive' 
when AMA_OPT_OUT_FLAG = 'Y' /*or AMA_OPT_OUT_FLAG is null*/ then 'AMA Opt Out'
when HCP_Type not in ('Non-Prescribing Health Care Professional','Prescriber') then 'Non Prescribers'
when (HCP_Source = 'NBA HCP' and (NBA_EMAIL_ADDRESS is  null
OR  INSTR(LTRIM(RTRIM(NBA_EMAIL_ADDRESS)),' ') > 0
OR  SUBSTR(LTRIM(NBA_EMAIL_ADDRESS),1) = '@'
OR SUBSTR(RTRIM(NBA_EMAIL_ADDRESS),-1) = '.'
OR INSTR(NBA_EMAIL_ADDRESS,'.', INSTR('@',NBA_EMAIL_ADDRESS))- INSTR('@',NBA_EMAIL_ADDRESS ) = 1
OR LENGTH(LTRIM(RTRIM(NBA_EMAIL_ADDRESS )))- LENGTH(REPLACE(LTRIM(RTRIM(NBA_EMAIL_ADDRESS)),'@','')) > 1 
OR INSTR(REVERSE(LTRIM(RTRIM(NBA_EMAIL_ADDRESS))),'.') < 2
OR (INSTR('.@',NBA_EMAIL_ADDRESS) > 0
OR INSTR('..',NBA_EMAIL_ADDRESS) >0 ))) then 'Blank/Invalid Email Address' 
when (HCP_Source = 'SFMC HCP' and (PARTY_EMAIL_ADDRESS is  null
OR  INSTR(LTRIM(RTRIM(PARTY_EMAIL_ADDRESS)),' ') > 0
OR  SUBSTR(LTRIM(PARTY_EMAIL_ADDRESS),1) = '@'
OR SUBSTR(RTRIM(PARTY_EMAIL_ADDRESS),-1) = '.'
OR INSTR(PARTY_EMAIL_ADDRESS,'.', INSTR('@',PARTY_EMAIL_ADDRESS))- INSTR('@',PARTY_EMAIL_ADDRESS ) = 1
OR LENGTH(LTRIM(RTRIM(PARTY_EMAIL_ADDRESS )))- LENGTH(REPLACE(LTRIM(RTRIM(PARTY_EMAIL_ADDRESS)),'@','')) > 1 
OR INSTR(REVERSE(LTRIM(RTRIM(PARTY_EMAIL_ADDRESS))),'.') < 2
OR (INSTR('.@',PARTY_EMAIL_ADDRESS) > 0
OR INSTR('..',PARTY_EMAIL_ADDRESS) >0 ))) then 'Blank/Invalid Email Address'
when( HCP_Source = 'NBA HCP' and (upper(NBA_EMAIL_ADDRESS) LIKE  '%KP.ORG%' OR upper(NBA_EMAIL_ADDRESS) LIKE  '%KAISERPERMANENTE%' OR upper(NBA_EMAIL_ADDRESS) LIKE  '%KPCHR%' OR upper(NBA_EMAIL_ADDRESS) LIKE  '%KPEXPERIENCE.NET%' OR upper(NBA_EMAIL_ADDRESS) LIKE  '%KPONLINE.ORG%' 
OR upper(NBA_EMAIL_ADDRESS) LIKE  '%KAISERPERMANENTEJOBS.ORG%' OR upper(NBA_EMAIL_ADDRESS) LIKE  '%KPPP.COM%')) then 'Kaiser Suppression'
when( HCP_Source <> 'NBA HCP' and (upper(PARTY_EMAIL_ADDRESS) LIKE  '%KP.ORG%' OR upper(PARTY_EMAIL_ADDRESS) LIKE  '%KAISERPERMANENTE%' OR upper(PARTY_EMAIL_ADDRESS) LIKE  '%KPCHR%' OR upper(PARTY_EMAIL_ADDRESS) LIKE  '%KPEXPERIENCE.NET%' OR upper(PARTY_EMAIL_ADDRESS) LIKE  '%KPONLINE.ORG%' 
OR upper(PARTY_EMAIL_ADDRESS) LIKE  '%KAISERPERMANENTEJOBS.ORG%' OR upper(PARTY_EMAIL_ADDRESS) LIKE  '%KPPP.COM%')) then 'Kaiser Suppression'
when ET_Active = 'N' then 'Merged Record' 
when Bounce_Flag = 1 then 'Held Record' 
when Opt_Out_Flag = 'Y' then PRODUCT_NAME || ' Opt Out'
else 'Emailable Record' end  Tmp_Suppression_Criteria,
case when Merge_Record_Date is null or  ET_Active <> 'N' then to_date('12/31/9999','MM/DD/YYYY') else Merge_Record_Date end as Merge_Record_Date,
--nvl(Merge_Record_Date,to_date('12/31/9999','MM/DD/YYYY')) as Merge_Record_Date,
--nvl(Bounce_Date,to_date('12/31/9999','MM/DD/YYYY')) as Bounce_Date
case when Bounce_Date is null or  Bounce_Flag <> 1 then to_date('12/31/9999','MM/DD/YYYY') else Bounce_Date end as Bounce_Date,
case when OPT_OUT_DATE is null or  Opt_Out_Flag <> 'Y' then to_date('12/31/9999','MM/DD/YYYY') else OPT_OUT_DATE end as OPT_OUT_DATE from
(Select distinct HCP_Source.Party_Fk, HCP_Source.MDM_ID AS MDM_KEY,HCP_Source,HCP_Party.AMA_OPT_OUT_FLAG,HCP_Party.CREATE_DT, HCP_Party.PARTY_TYPE, HCP_Party.HCP_TYPE, HCP_Party.PRACTICE_TYPE, HCP_Party.HCP_STATUS, HCP_Party.RECORD_STATE_CD, SMA_HCP_Segment,
Lead_Source,Source_Dt,CHILD_TA,PARTY_EMAIL_ADDRESS, NBA_EMAIL_ADDRESS ,ET_Active,Merge_Record_Date,Bounce_Flag,Bounce_date,IW_FLG,Opt_Out_Flag, Opt_Out_Date,HCP_SOURCE.PRODUCT_NAME
from TMP_WF_HCP_SOURCE HCP_Source
LEFT JOIN TMP_WF_HCP_PARTY HCP_Party ON HCP_Party.PARTY_SK = HCP_Source.Party_Fk
LEFT JOIN
(Select distinct EXISTING_MDM_ID,Merge_Record_Date,'N' as ET_Active from (
select distinct EXISTING_MDM_ID,SRC_MERGE_DT as Merge_Record_Date,
ROW_NUMBER() OVER (PARTITION BY EXISTING_MDM_ID ORDER BY SRC_MERGE_DT) AS RANK
from TMP_WF_MS_HCP_MERGE ) where RANK = 1) Merge_Table ON HCP_SOURCE.MDM_ID = Merge_Table.EXISTING_MDM_ID
LEFT JOIN 
(Select Customer_fk,Hard_Bounce_Count,Soft_Bounce_Count,Bounce_date,case when (Hard_Bounce_Count > 0 or Soft_Bounce_Count > 2) then 1 else 0 end as Bounce_Flag  from (
Select distinct Customer_fk, count(distinct (case when ATTRIBUTE_5_NAME  = 'BOUNCE_CATEGORY' and ATTRIBUTE_5_VALUE = 'Hard bounce' then Integration_Key end)) as Hard_Bounce_Count,
count(distinct (case when ATTRIBUTE_5_NAME  = 'BOUNCE_CATEGORY' and ATTRIBUTE_5_VALUE = 'Soft bounce' then Integration_Key end)) as Soft_Bounce_Count,
min(case when activity_type_fk = 245 and ATTRIBUTE_5_NAME  = 'BOUNCE_CATEGORY' and(ATTRIBUTE_5_VALUE = 'Soft bounce' or ATTRIBUTE_5_VALUE = 'Hard bounce') then Time_dt end) as Bounce_date
from DH_MKTG_IDS.MKTG_CUSTOMER_ACTIVITY_FACT MAF
join CDW.T_TIME_DIM TD on MAF.TIME_FK = TD.TIME_SK
group by Customer_fk
)) T_Bounce ON HCP_SOURCE.PARTY_FK = T_Bounce.Customer_fk
LEFT JOIN
(Select distinct PARTY_FK,IW_PROGRAM_FK,IW_TOPIC_FK,IW_FLG,PROGRAM_NAME,Topic_Name,Opt_Out_Date, 
case when IW_FLG = 'N' then 'Y' else 'N' end as Opt_Out_Flag from (
Select distinct OPT.PARTY_FK,OPT.IW_PROGRAM_FK, OPT.IW_TOPIC_FK, OPT.IW_FLG,P.PROGRAM_NAME,T.Topic_Name,OPT.LAST_LOAD_TS as Opt_Out_Date,
ROW_NUMBER() OVER(PARTITION BY OPT.PARTY_FK,OPT.IW_PROGRAM_FK, OPT.IW_TOPIC_FK,P.PROGRAM_NAME,T.Topic_Name ORDER BY OPT.LAST_LOAD_TS DESC) AS Row_id
from DH_IDS.V_IDS_IW_SNAPSHOT OPT,DH_IDS.V_IDS_IW_PROGRAM P, DH_IDS.V_IDS_IW_TOPIC T
 where IW_PROGRAM_FK = IW_PROGRAM_SK
 and IW_TOPIC_FK = IW_TOPIC_SK
 and Upper(Channel_Cd) = 'EMAIL'
 and Upper(Topic_Name) = 'INFORMATIONAL'
) where Row_id = 1 ) Opt_table ON HCP_SOURCE.PARTY_FK = Opt_table.PARTY_FK AND HCP_SOURCE.PRODUCT_NAME = Opt_table.PROGRAM_NAME
/*where HCP_Party.PARTY_SK = HCP_Source.Party_Fk
and Merge_Table.EXISTING_MDM_ID(+) = HCP_Party.MDM_KEY
and T_Bounce.Customer_fk(+) = HCP_Party.PARTY_SK
and Opt_table.Party_Fk(+) = HCP_Party.PARTY_SK
and Opt_table.PROGRAM_NAME(+) = HCP_Source.Product_Name*/
)
)TMP_WF1
)TMP_WF2
)TMP_WF3
