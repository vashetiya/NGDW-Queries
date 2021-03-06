/*
ADU_MT_REP_METADATA
Purpose
    - The data of REP with their territory details,Team name and login details used for field suggestions.
Change Log
    - 2021-07-14 - vshetiya - Created

*/

USE ROLE __SNOWFLAKE_NGCA_CIM_DEPLOY_RL__; 


CREATE OR REPLACE VIEW __SNOWFLAKE_NGCA_DB__.MCE.ADU_MT_REP_METADATA
COMMENT = 'VIEW CONTAINING THE REP DETAILS'
AS 
(
WITH emp_to_terr AS
   (
        SELECT EMPLOYEE_NAME,TERRITORY_NUMBER,TERRITORY_NAME,FIELD_FORCE_NAME,
	           MK_WORKER_BAM_ID,TERRITORY_HIERARCHY_LEVEL,REGION_NAME,EMPLOYEE_NETWORK_ID,
               REGION_NUMBER,DIVISION_NUMBER,ZONE_NUMBER,NATION_NUMBER
        FROM __SNOWFLAKE_NGCA_DB__.CIM.ALIGNMENT_EMPLOYEE_TO_TERRITORY
        WHERE 
		      UPPER(FIELD_FORCE_NAME) IN ('AD-AAL','AD-ADRM','AD-MSL','AD-TBM')
              AND TERRITORY_ACTIVE_FLG = 'Y'
              AND EMPLOYEE_TERRITORY_RANK = 1
              AND UPPER(POSITION_STATUS) = 'ACTIVE'
   ),
   emp_details AS
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
  geo AS
  (
        SELECT
            POSTAL_CODE,
            TERRITORY_NAME,
            REGION_NAME,
            FIELD_FORCE_NAME
        FROM
            __SNOWFLAKE_NGCA_DB__.CIM.ALIGNMENT_GEO_TO_TERRITORY
        WHERE UPPER(ALIGNMENT_TYPE) = 'TRUE_CURR'
   )

(
SELECT 
  emp_details.REP_ID,
  terr.EMPLOYEE_NAME AS REP_NAME,
  terr.FIELD_FORCE_NAME AS TEAM_NAME,
  terr.TERRITORY_NUMBER AS TERRITORY_ID,
  terr.TERRITORY_NAME AS TERRITORY_NAME,
  terr.REGION_NAME AS REGION_NAME,
  geo.POSTAL_CODE AS ZIP_CODE,
  terr.EMPLOYEE_NETWORK_ID AS REP_LOGIN,
  region.EMPLOYEE_NETWORK_ID AS SUPERVISOR_LEVEL1_LOGIN,
  region.EMPLOYEE_NAME AS SUPERVISOR_LEVEL1_NAME,
  COALESCE(division.EMPLOYEE_NETWORK_ID, nation.EMPLOYEE_NETWORK_ID, zone.EMPLOYEE_NETWORK_ID) AS SUPERVISOR_LEVEL2_LOGIN,
  COALESCE(division.EMPLOYEE_NAME, nation.EMPLOYEE_NAME, zone.EMPLOYEE_NAME) AS SUPERVISOR_LEVEL2_NAME
FROM emp_to_terr terr
INNER JOIN emp_details ON terr.MK_WORKER_BAM_ID = emp_details.MK_WORKER_BAM_ID  
LEFT JOIN emp_to_terr region ON terr.FIELD_FORCE_NAME = region.FIELD_FORCE_NAME AND terr.REGION_NUMBER = region.REGION_NUMBER
AND region.TERRITORY_HIERARCHY_LEVEL = 'region__aln'
LEFT JOIN emp_to_terr division ON terr.FIELD_FORCE_NAME = division.FIELD_FORCE_NAME AND terr.DIVISION_NUMBER = division.DIVISION_NUMBER
AND division.TERRITORY_HIERARCHY_LEVEL = 'division__aln'
LEFT JOIN emp_to_terr nation ON terr.FIELD_FORCE_NAME = nation.FIELD_FORCE_NAME AND terr.NATION_NUMBER = nation.NATION_NUMBER
AND nation.TERRITORY_HIERARCHY_LEVEL = 'nation__aln'
LEFT JOIN emp_to_terr zone ON terr.FIELD_FORCE_NAME = zone.FIELD_FORCE_NAME AND terr.ZONE_NUMBER = zone.ZONE_NUMBER
AND zone.TERRITORY_HIERARCHY_LEVEL = 'zone__c'

LEFT JOIN 
    geo ON UPPER(TRIM(terr.REGION_NAME)) = UPPER(TRIM(geo.REGION_NAME))
        AND UPPER(TRIM(terr.TERRITORY_NAME)) = UPPER(TRIM(geo.TERRITORY_NAME))
        AND UPPER(terr.FIELD_FORCE_NAME) = UPPER(geo.FIELD_FORCE_NAME)
  
WHERE terr.TERRITORY_HIERARCHY_LEVEL = 'territory__aln'  
)
);

GRANT SELECT ON __SNOWFLAKE_NGCA_DB__.MCE.ADU_MT_REP_METADATA TO ROLE IHUB_QRY_RL; 