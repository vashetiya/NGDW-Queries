/*
ADU_MT_HCP_REP_ALIGNMENT
Purpose
    - The data of HCPs aligned with REPs with territory details used for Field Suggestions
Change Log
    - 2021-07-14 - vshetiya - Created

*/

USE ROLE __SNOWFLAKE_NGCA_CIM_DEPLOY_RL__; 

CREATE OR REPLACE VIEW __SNOWFLAKE_NGCA_DB__.MCE.ADU_MT_HCP_REP_ALIGNMENT
COMMENT = 'VIEW CONTAINING THE HCPs TO REPs ALIGNMENT DATA' 

AS
(  
  WITH rep_meta_data AS 
  (
    SELECT REP_NAME,
           TERRITORY_ID,
           TERRITORY_NAME,
           REP_ID
           
    FROM __SNOWFLAKE_NGCA_DB__.MCE.AD_MT_REP_META_DATA
	GROUP BY REP_NAME,TERRITORY_ID,TERRITORY_NAME,REP_ID
  ),
  account_terr AS 
    (
        SELECT
            MK_ACCOUNT_VNT_ID AS CUSTOMER_ID,
            TERRITORY_NUMBER,
            'ADUHELM' AS PRODUCT_NAME
        FROM __SNOWFLAKE_NGCA_DB__.CIM.ALIGNMENT_ACCOUNT_TO_TERRITORY 
        WHERE 
            UPPER(ALIGNMENT_TYPE) = 'TRUE_CURR'
            AND UPPER(FIELD_FORCE_NAME) IN ('AD','AD-AAL','AD-ADRM','AD-MSL','AD-TBM')
			AND MK_ACCOUNT_VNT_ID <> '-1'
      )
( 
SELECT 
	account_terr.CUSTOMER_ID,
    account_terr.PRODUCT_NAME,
    rep_meta_data.REP_ID,         
    rep_meta_data.REP_NAME,
    rep_meta_data.TERRITORY_ID, 
    rep_meta_data.TERRITORY_NAME
    FROM account_terr
    INNER JOIN rep_meta_data ON rep_meta_data.TERRITORY_ID = account_terr.TERRITORY_NUMBER  
)
);  


GRANT SELECT ON __SNOWFLAKE_NGCA_DB__.MCE.ADU_MT_HCP_REP_ALIGNMENT TO ROLE IHUB_QRY_RL; 