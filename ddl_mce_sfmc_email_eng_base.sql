/*
SFMC_EMAIL_ENG_BASE
Purpose
    - SFMC Email Data of HCP and Patients with additional tactic code attributes added 
Change Log
    - 2021-06-02 - vshetiya - Created
    - 2021-07-28 - vshetiya - Updated field names and added lead source field

*/

USE ROLE __SNOWFLAKE_NGCA_CIM_DEPLOY_RL__; 

CREATE OR REPLACE VIEW __SNOWFLAKE_NGCA_DB__.MCE.SFMC_EMAIL_ENG_BASE 
COMMENT = 'SFMC EMAIL DATA' AS 

(
  SELECT
        act.ALIGNMENT_STRING AS MDM_ID,
        act.ORIGIN_TACTIC_CODE_OF_RESENDS AS TACTIC_CODE,
        act.ACTIVITY_OFFER_DESCRIPTION,
        act.SOURCE_CODE AS ACTIVITY_SOURCE,
        act.PRODUCT_NAME,
        act.THERAPEUTIC_AREA,
        act.ACTIVITY_TYPE,
        --act.DATE_NBR,
        --SUBSTR(act.DATE_NBR,1,6) MONTH_NBR,
		TO_DATE(DATE_TRUNC('MONTH',act.TIME_DATE)) AS MONTH_DATE,
		TO_DATE(DATE_TRUNC('WEEK',act.TIME_DATE)) AS WEEK_BEGIN_DATE,
        act.TIME_DATE,
        act.IDENTIFIED_FLAG,
        CASE WHEN UPPER(act.CUSTOMER_TYPE) =  'CONTACT' THEN 'PATIENT' ELSE act.CUSTOMER_TYPE END AS CUSTOMER_TYPE, 
        act.PARTY_CLASS,
        act.PARTY_SUBTYPE,
        act.RESEND_FLAG,
        act.ACTUAL_TACTIC_CODE,
        first_source.LEAD_SOURCE,
        web_lead_source.WEB_LEAD_SOURCE,
        CASE WHEN  UPPER(act.TACTIC_CODE) LIKE '%BIOGEN-%' THEN SUBSTR(act.ACTUAL_TACTIC_CODE,1,12) ELSE SUBSTR(act.ACTUAL_TACTIC_CODE,1,11) END BASE_TACTIC,
        CASE WHEN (ROW_NUMBER() OVER(PARTITION BY act.MDM_ID, act.ACTIVITY_TYPE, BASE_TACTIC, MONTH_DATE ORDER BY act.DATE_NBR )) > 1
		     THEN 1 ELSE 0 END MONTHLY_DUPLICATE_FLAG  ,
        CASE WHEN (ROW_NUMBER() OVER(PARTITION BY act.MDM_ID, act.ACTIVITY_TYPE, BASE_TACTIC,WEEK_BEGIN_DATE ORDER BY act.DATE_NBR )) > 1 
		     THEN 1 ELSE 0 END WEEKLY_DUPLICATE_FLAG,
        CASE WHEN (ROW_NUMBER() OVER(PARTITION BY act.MDM_ID, act.ACTIVITY_TYPE, BASE_TACTIC ORDER BY act.DATE_NBR )) > 1
		     THEN 1 ELSE 0 END OVERALL_DUPLICATE_FLAG,
        NVL(UPPER(act.ACTIVITY_EM_CATEGORY),'UNMAPPED') AS TACTIC_CATEGORY,
        NVL(UPPER(act.ACTIVITY_EM_STREAM),'UNMAPPED') AS TACTIC_STREAM,
        NVL(UPPER(act.ACTIVITY_SUB_STREAM),'UNMAPPED') AS TACTIC_SUB_STREAM,
        act.ACTIVITY_EM_NUMBER,
        act.ACTIVITY_EM_NUMBER_WITH_AB_SPLIT,
        act.DESTINATION_ID_TYPE,
        act.DESTINATION_ID_VALUE,
        act.UNSUBCRIBE_CLICKS_FLAG,
        act.FIREWALL_CLICK_FLAG,
        act.IS_ACTIVE,
        act.GEO_REGION_CODE,
        rank.MS_DIGITAL_SEGMENT,
        rank.SMA_DIGITAL_SEGMENT,
        rank.ALZ_DIGITAL_SEGMENT
        
FROM __SNOWFLAKE_NGCA_DB__.MCE.MARKETING_ACTIVITY act

LEFT JOIN __SNOWFLAKE_NGCA_DB__.MCE.HCP_MS_RANKING rank ON act.MDM_ID = rank.RDM_KEY
  
LEFT JOIN (SELECT CASE WHEN MK_ACCOUNT_VNT_ID = '-1' and MK_PATIENT_GNE_ID = '-1' THEN CUSTOMER_IDENTITY_VALUE WHEN UPPER(CUSTOMER_TYPE) = 'HCP' THEN 'VNID'|| MK_ACCOUNT_VNT_ID ELSE 'LH'||MK_PATIENT_GNE_ID END AS MDM_ID, 
           ATTRIBUTE_2_VALUE AS WEB_LEAD_SOURCE FROM __SNOWFLAKE_MKHB_DB__.SEMANTIC.MKTG_CUSTOMER_ACTIVITY_FACT
           WHERE UPPER(SOURCE_CD) = 'WEB_NEURO' AND UPPER(ACTIVITY_TYPE_NAME) = 'WEB REG' 
		   QUALIFY ROW_NUMBER() OVER (PARTITION BY MDM_ID,SOURCE_CD ,ACTIVITY_TYPE_NAME ORDER BY ACTIVITY_TIME) =1 
) web_lead_source ON act.ALIGNMENT_STRING = web_lead_source.MDM_ID	

LEFT JOIN __SNOWFLAKE_NGCA_DB__.MCE.PARTY_FIRST_SOURCE first_source ON act.ALIGNMENT_STRING = first_source.MDM_ID
 
 
WHERE UPPER(act.ACTIVITY_SUBCHANNEL) = 'EMAIL'
        AND UPPER(act.SOURCE_CODE) = 'SFMC'
        AND UPPER(act.CUSTOMER_TYPE) IN ('PATIENT','HCP','CONTACT')  
        AND TO_DATE(DATE_TRUNC('MONTH',act.TIME_DATE)) >= '2019-01-01'

);
