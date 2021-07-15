/*
ADU_MT_PROMOTIONAL_METRICS
Purpose
    - Promotional metrics data at rep level for field suggestions.
Change Log
    - 2021-07-14 - vshetiya - Created

*/

USE ROLE __SNOWFLAKE_NGCA_CIM_DEPLOY_RL__; 

CREATE OR REPLACE VIEW __SNOWFLAKE_NGCA_DB__.MCE.ADU_MT_PROMOTIONAL_METRICS
COMMENT = 'VIEW CONTAINING THE PROMOTIONAL METRICS DATA'
AS 
(
 WITH emp_details AS
    (
        SELECT 
            salesforce_user.ID AS REP_ID, 
            salesforce_user.MK_WORKER_BAM_ID 
        FROM 
            __SNOWFLAKE_NGDW_DB__.INTEGRATED.VCR_SF_USER_SCD1 salesforce_user
        WHERE 
            UPPER(salesforce_user.COUNTRY) IN ('US','United States')
            AND salesforce_user.ISACTIVE > 0
    ),
    call_activity_fact AS 
    (
                
        SELECT
            MK_ACCOUNT_VNT_ID CUSTOMER_ID,
            TO_DATE(CALL_START_DATE) DATE_OF_ACTIVITY,
            ACCOUNT_TYPE,
            EMPLOYEE_LOGIN,
            EMPLOYEE_ID,
            FIRST_PRODUCT PRODUCT_NAME
        FROM 
            __SNOWFLAKE_NGCA_DB__.CIM.FIELD_CALL_ACTIVITY_FACT  
        WHERE (CALL_START_DATE) <= (SYSDATE()) 
        AND   UPPER(FIRST_PRODUCT) = 'ADUHELM' 
        AND   UPPER(FIELD_FORCE_NAME) IN ('AD','AD-AAL','AD-ADRM','AD-MSL','AD-TBM')
    )
    SELECT 
        call_activity_fact.CUSTOMER_ID,
        call_activity_fact.PRODUCT_NAME,
        emp_details.REP_ID,
        'M105' AS METRIC_ID,
        'HCP_REP_Last_Called_Date' AS METRIC_NAME,
        MAX(DATE_OF_ACTIVITY) AS METRIC_VALUE
  
    FROM         
        call_activity_fact 
    INNER JOIN emp_details  ON emp_details.MK_WORKER_BAM_ID = call_activity_fact.EMPLOYEE_ID
    WHERE ACCOUNT_TYPE='HCP'
    GROUP BY CUSTOMER_ID,REP_ID,PRODUCT_NAME    
 
);

GRANT SELECT ON __SNOWFLAKE_NGCA_DB__.MCE.ADU_MT_PROMOTIONAL_METRICS TO ROLE IHUB_QRY_RL; 