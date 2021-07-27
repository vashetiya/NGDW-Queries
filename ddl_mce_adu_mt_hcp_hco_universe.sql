/*
ADU_MT_HCP_HCO_UNIVERSE
Purpose
    - Universe of the AD HCP with their details like ID,Name,Speciality, etc with their active flag and customer target  
Change Log
    - 2021-07-26 - vshetiya - Created

*/

USE ROLE __SNOWFLAKE_NGCA_CIM_DEPLOY_RL__; 

CREATE OR REPLACE VIEW __SNOWFLAKE_NGCA_DB__.MCE.ADU_MT_HCP_HCO_UNIVERSE
COMMENT = 'VIEW CONTAINING THE ADU HCP-HCO DETAILS'
AS 
(

SELECT 
    act_dim.MK_ACCOUNT_VNT_ID AS CUSTOMER_ID,
	NVL(hcp.FIRST_NAME__V,act_dim.ACCOUNT_NAME) AS CUSTOMER_FIRST_NAME,
	NVL(hcp.LAST_NAME__V,act_dim.ACCOUNT_NAME) AS CUSTOMER_LAST_NAME,
	act_dim.ACCOUNT_CLASS AS CUSTOMER_TYPE,
	hcp.GENDER__V AS GENDER,
	hcp.BIRTH_DATE__V AS DATE_OF_BIRTH,
	act_dim.PRIMARY_SPECIALTY_NAME AS PRIMARY_SPECIALTY,
	act_dim.SECONDARY_SPECIALTY_NAME AS SECONDARY_SPECIALTY,
	act_dim.ADDRESS_LINE_1__V ADDRESS_1,
	act_dim.ADDRESS_LINE_2__V ADDRESS_2,
	act_dim.CITY,
	act_dim.STATE_CD STATE,
	act_dim.COUNTRY__V COUNTRY,
	act_dim.POSTAL_CODE ZIP_CODE,
	act_dim.WILLINGNESS_CAPABILITY_FLAG AS ACCOUNT_READINESS_FLAG,
	NVL(act_dim.PDRP_OPTOUT, 'N') AS PDRP_OPT_OUT_FLAG,
	alz_target_list.SEGMENT AS ATTITUDINAL_SEGMENT,
	alz_target_list.COMPOSITE_DECILE AS DECILE,
	act_dim.BIIB_GROUP__C AS HCP_SEGMENT,    
	CASE WHEN alz_target_list.SEGMENT IN ('N1','N2','P1') THEN 'Priority1'
		 WHEN alz_target_list.SEGMENT IN ('N3','N4','P2','P3') THEN 'Priority2' 
		 ELSE NULL END AS SEGMENT_CATEGORY,
	act_dim.EMAIL_1 AS CUSTOMER_EMAIL,
	act_dim.ACCOUNT_STATUS_NAME AS IS_ACTIVE_FLAG,
	hcp_interaction.CUSTOMER_TARGET,
/* Adding flex columns to ease the integration process if we need to add additional fields in the data */  
    NULL AS ATTRIBUTE_NAME_1,
    NULL AS ATTRIBUTE_VALUE_1,
    NULL AS ATTRIBUTE_NAME_2,
    NULL AS ATTRIBUTE_VALUE_2,
    NULL AS ATTRIBUTE_NAME_3,
    NULL AS ATTRIBUTE_VALUE_3,
    NULL AS ATTRIBUTE_NAME_4,
    NULL AS ATTRIBUTE_VALUE_4,
    NULL AS ATTRIBUTE_NAME_5,
    NULL AS ATTRIBUTE_VALUE_5
  
FROM 
	__SNOWFLAKE_NGCA_DB__.CIM.ACCOUNT_AD_DIM act_dim 
LEFT JOIN 
	__SNOWFLAKE_NGDW_DB__.INTEGRATED.VNT_HCP_SCD1 hcp 
	ON act_dim.MK_ACCOUNT_VNT_ID = hcp.MK_ACCOUNT_VNT_ID
        AND UPPER(RECORD_STATE__V) = 'VALID'
        AND UPPER(HCP_STATUS__V) = 'A'
LEFT JOIN 
	__SNOWFLAKE_NGCA_DB__.MCE.ALZ_TBM_TARGET_LIST alz_target_list 
	ON act_dim.MK_ACCOUNT_VNT_ID = alz_target_list.VNID
LEFT JOIN 
    (
	SELECT 
	      HCP_ID,
          CASE WHEN SUM(TEMP_FLAG) = 1 THEN 'F' 
		       WHEN SUM(TEMP_FLAG) = 2 THEN 'E' 
		       WHEN SUM(TEMP_FLAG) = 3 THEN 'B' 
		       ELSE '' END CUSTOMER_TARGET 
          FROM
              (
			   SELECT 
					HCP_ID,
					CASE WHEN UPPER(MARKETING_VENDOR) = 'VEEVA' THEN 1 
					     WHEN UPPER(MARKETING_VENDOR) = 'SFMC' THEN 2 
					     ELSE 0 END AS TEMP_FLAG 
                    FROM 
						__SNOWFLAKE_NGCA_DB__.MCE.ADU_MT_HCP_HCO_INTERACTIONS
						QUALIFY ROW_NUMBER() OVER(PARTITION BY HCP_ID,MARKETING_VENDOR ORDER BY DATE_OF_ACTIVITY DESC) = 1
			  )
      
    GROUP BY HCP_ID
      
    )hcp_interaction 
  
	ON act_dim.MK_ACCOUNT_VNT_ID = hcp_interaction.HCP_ID
  
WHERE 
	act_dim.MK_ACCOUNT_VNT_ID IS NOT NULL
AND act_dim.MK_ACCOUNT_VNT_ID <> '-1'
 
);

GRANT SELECT ON __SNOWFLAKE_NGCA_DB__.MCE.ADU_MT_HCP_HCO_UNIVERSE TO ROLE IHUB_QRY_RL; 
