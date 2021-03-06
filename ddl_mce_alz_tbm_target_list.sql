/*
ALZ_TBM_TARGET_LIST
Purpose
    - View automating the ALZ TBM Target List process
Change Log
    - 2021-07-26 - vshetiya - Created

*/

USE ROLE __SNOWFLAKE_NGCA_CIM_DEPLOY_RL__; 

CREATE OR REPLACE VIEW __SNOWFLAKE_NGCA_DB__.MCE.ALZ_TBM_TARGET_LIST
COMMENT = 'VIEW CONTAINING THE ALZ TBM TARGET LIST'
AS 
(
SELECT ad_account.ACCOUNT_NAME AS HCP,
       ad_account.BIIB_GROUP__C AS TARGET_GROUP,
       ad_account.SITE_NAME AS AD_PRIMARY_ACCOUNT,
       ad_account.IDN_NAME AS HEALTH_SYSTEM,
       ad_account.INSIGHT_STARTED AS INSIGHT_STARTED,
       ad_account.ALL_KEY_QUES_ANSWRD_FLG AS ALL_KEY_QUESTIONS_ANSWERED,
       CASE WHEN alz_hcp.PATIENT_VOLUME = '' OR alz_hcp.PATIENT_VOLUME = '-' THEN NULL ELSE alz_hcp.PATIENT_VOLUME END AS PATIENT_VOLUME,
       ROUND(ad_account.PERC_INSIGHTS_COMPLETED) || '%' AS INSIGHTS_COMPLETE_KEY_QUESTIONS,
       ad_account.HCP_TARGET_STATUS AS TARGET_STATUS,
       CASE WHEN NEUROS = '1' THEN 'Neuro' WHEN PCPS = '1' THEN 'PCP' ELSE NULL END AS HCP_TYPE, 
       ad_account.PRIMARY_SPECIALTY_NAME AS PRIMARY_SPECIALTY,
       CASE WHEN alz_hcp.INITIATOR_TREATOR = '' OR alz_hcp.INITIATOR_TREATOR = '-' THEN NULL ELSE alz_hcp.INITIATOR_TREATOR END AS INITIATOR_TREATOR,
       CASE WHEN alz_hcp.TOTAL_INITIATIONS = '' OR alz_hcp.TOTAL_INITIATIONS = '-' THEN NULL ELSE alz_hcp.TOTAL_INITIATIONS END AS TOTAL_INITIATIONS,
       CASE WHEN alz_hcp.SYMPTOMATIC_TRX = ''  OR alz_hcp.SYMPTOMATIC_TRX = '-' THEN NULL ELSE alz_hcp.SYMPTOMATIC_TRX END AS SYMPTOMATIC_TRX,
       CASE WHEN alz_hcp.COMPOSITE_DECILE = '' OR alz_hcp.COMPOSITE_DECILE = '-' THEN NULL ELSE alz_hcp.COMPOSITE_DECILE END AS COMPOSITE_DECILE,
       ad_account.BIIB_KME__C AS KME,
       ad_account.AMA_DO_NOT_CONTACT_FLG AS AMA_DO_NOT_CONTACT,
       ad_account.TBM_TERR_NAME AS TBM_TERRITORY,
       ad_account.MK_ACCOUNT_VNT_ID AS VNID,
       ad_account.NPI_NUM AS NPI_NUMBER,      
       CASE WHEN alz_hcp.SEGMENT = '' THEN NULL ELSE alz_hcp.SEGMENT END AS SEGMENT,
       ad_account.ADDRESS_LINE_1__V AS ADDRESS,
       ad_account.CITY,
       ad_account.AAL_EMPLOYEE_NAME AS AAL,
       ad_account.STATE_CD AS STATE,
       ad_account.POSTAL_CODE AS ZIP,
       ad_account.ADRM_EMPLOYEE_NAME AS ADRM,
       ad_account.TBM_EMPLOYEE_NAME AS TBM,       
       email.EMAIL_1,
       email.EMAIL_2
FROM __SNOWFLAKE_NGCA_DB__.CIM.ACCOUNT_AD_DIM ad_account
LEFT JOIN __SNOWFLAKE_NGCA_DB__.MCE.ALZ_TARGET_HCP_LIST alz_hcp
          ON alz_hcp.NPI = ad_account.NPI_NUM
LEFT JOIN __SNOWFLAKE_NGDW_DB__.SEMANTIC.HCP_DIM email   
          ON email.MK_ACCOUNT_VNT_ID = ad_account.MK_ACCOUNT_VNT_ID
  
WHERE 
     ad_account.ACCOUNT_CLASS = 'HCP' 
     AND ad_account.BIIB_Group__C IN ('A','B','C','D', 'NEW')
     AND ad_account.MK_ACCOUNT_VNT_ID IS NOT NULL
          
	 
);

GRANT SELECT ON __SNOWFLAKE_NGCA_DB__.MCE.ALZ_TBM_TARGET_LIST TO ROLE IHUB_QRY_RL; 
