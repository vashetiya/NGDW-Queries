/*
MKTG_ACTIVITY_COMBINED
Purpose
    - Combining Sales Call, Event and Non-Personal Promotion Data
Change Log
    - 2021-06-29 - vshetiya - Created

*/

USE ROLE __SNOWFLAKE_NGCA_CIM_DEPLOY_RL__; 


CREATE OR REPLACE VIEW __SNOWFLAKE_NGCA_DB__.MCE.MKTG_ACTIVITY_COMBINED 
COMMENT = 'VIEW COMBINING THE SALES FORCE, EVENTS AND NON-PERSONAL PROMOTION MARKETING DATA FOR HCPS AND PATIENTS' 
AS


(

    WITH sf_target AS
	(
		SELECT 
			MK_ACCOUNT_VNT_ID, 
			CYCLE_PLAN_START_DATE,
            CYCLE_PLAN_END_DATE,
			LEAD(CYCLE_PLAN_START_DATE) OVER (PARTITION BY MK_ACCOUNT_VNT_ID ORDER BY CYCLE_PLAN_START_DATE) AS LEADING_START_DATE,
			CASE WHEN LEADING_START_DATE IS NULL THEN CYCLE_PLAN_END_DATE 
				 WHEN LEADING_START_DATE <= CYCLE_PLAN_END_DATE THEN DATEADD(DAY, -1, LEADING_START_DATE) 
				 ELSE CYCLE_PLAN_END_DATE END AS UPDATED_CYCLE_PLAN_END_DATE 
		FROM
			(
				SELECT
					 MK_ACCOUNT_VNT_ID,
					 CYCLE_PLAN_START_DATE,
					 CYCLE_PLAN_END_DATE,
					 LAG(CYCLE_PLAN_END_DATE) OVER (PARTITION BY MK_ACCOUNT_VNT_ID ORDER BY CYCLE_PLAN_START_DATE) LAGGING_END_DATE 
				FROM	
					(
						SELECT 
							 MK_ACCOUNT_VNT_ID,    	
                             TO_DATE(CYCLE_PLAN_START_DATE) AS CYCLE_PLAN_START_DATE,
                             TO_DATE(CYCLE_PLAN_END_DATE) AS CYCLE_PLAN_END_DATE
				        FROM     
					        __SNOWFLAKE_NGDW_DB__.SEMANTIC.CYCLE_PLAN_FACT 
				        WHERE	 
						    PLANNED_CALL_COUNT > 0
							QUALIFY ROW_NUMBER() OVER (PARTITION BY MK_ACCOUNT_VNT_ID,CYCLE_PLAN_START_DATE ORDER BY CYCLE_PLAN_END_DATE DESC) =1 
					)
			)		
		WHERE LAGGING_END_DATE IS NULL 
			  OR CYCLE_PLAN_END_DATE > LAGGING_END_DATE
	),

	brand AS 
	(
		SELECT 
			MK_PRODUCT_BRAND_PM_ID,	
			PRODUCT_BRAND_NAME
		FROM 
			__SNOWFLAKE_NGDW_DB__.SEMANTIC.PRODUCT_BRAND_MASTER
	),
	
	sent_email AS 
	(
		SELECT 
			ID,	
			EMAIL_SENT_DATE_VOD__C,
			ACCOUNT_VOD__C,
			APPROVED_EMAIL_TEMPLATE_VOD__C,
			PRODUCT_VOD__C,
			STATUS_VOD__C,
			OWNERID,
            PRODUCT_DISPLAY_VOD__C
		FROM 
			__SNOWFLAKE_NGDW_DB__.INTEGRATED.VCR_SF_SENT_EMAIL_VOD__C_SCD1
	),
	
	account AS 
	(
		SELECT 
			ID,	
			EXTERNAL_ID_VOD__C
		FROM 
			__SNOWFLAKE_NGDW_DB__.INTEGRATED.VCR_SF_ACCOUNT_SCD1
	),

	document AS 
	(
		SELECT 
			ID,	
			NAME,
			RECORDTYPEID
		FROM 
			__SNOWFLAKE_NGDW_DB__.INTEGRATED.VCR_SF_APPROVED_DOCUMENT_VOD__C_SCD1
	),	

	product AS 
	(
		SELECT 
			NAME,	
			THERAPEUTIC_AREA_VOD__C,
			ID
		FROM 
			__SNOWFLAKE_NGDW_DB__.INTEGRATED.VCR_SF_PRODUCT_VOD__C_SCD1
	),
	
	def AS 
	(
		SELECT 
			DEEP_FLAG,	
			LIGHT_FLAG,
			CONTACTED_FLAG,
			CAE_FLAG,
			METRIC_TYPE,
			ACTIVITY_TYPE_KEY2,
			ACTIVITY_SOURCE_KEY1,
			END_DATE
		FROM 
			__SNOWFLAKE_NGCA_DB__.MCE.KPI_DEFINITION
	),
    
    party_dim AS 
  (
		SELECT 
			'LH'|| MK_PATIENT_GNE_ID AS RDM_KEY, 
			UPPER(PATIENT_STATUS_NAME) AS PARTY_STATUS
		FROM 
			__SNOWFLAKE_NGDW_DB__.SEMANTIC.PATIENT_DIM 
		WHERE 
			MK_PATIENT_GNE_ID IS NOT NULL AND MK_PATIENT_GNE_ID <> '-1'
		GROUP BY MK_PATIENT_GNE_ID,PATIENT_STATUS_NAME
 
	UNION ALL

		SELECT 
			'VNID'|| MK_ACCOUNT_VNT_ID, 
			UPPER(RECORD_STATE) 
		FROM 
			__SNOWFLAKE_NGDW_DB__.SEMANTIC.VNT_ACCOUNT_DIM 
		WHERE 
			MK_ACCOUNT_VNT_ID IS NOT NULL 
			AND MK_ACCOUNT_VNT_ID <>  '-1'
		GROUP BY MK_ACCOUNT_VNT_ID, RECORD_STATE 
		
	)
  
	SELECT 
		TO_CHAR(CALL_ID) AS RECORD_ID,
		1 AS ACTIVITY_SEQ_NUMBER,
		'VNID'||MK_ACCOUNT_VNT_ID AS ALIGNMENT_STRING,
		TO_DATE(CALL_START_DATE) AS ACTIVITY_DATE,
		CALL_START_DATE AS TIMESTAMP,
		PRIMARY_PRODUCT_BRAND_ID AS PRODUCT_ID,
		UPPER(FIRST_PRODUCT) AS PRODUCT_NAME,
		'Personal' AS ACTIVITY_CLASS,
		'SALES FORCE' AS SOURCE_TYPE,
		'SALES FORCE' AS ACTIVITY_SOURCE,
		'SALES FORCE' AS ACTIVITY_SOURCE_CHANNEL,
		NULL AS ACTIVITY_CHANNEL,
		NULL AS ACTIVITY_SUBCHANNEL,
		NULL AS TACTIC_TYPE,
		NULL TACTIC_TITLE,
		NULL TACTIC_CODE,
		NULL AS TACTIC_CODE_DESC,
		NULL AS HCP_TACTIC_TYPE,  
		NULL AS SITEORIGIN,
		'Y' AS IDENTIFIED_FLAG,
		CALL_PLAN_TARGET_FLAG AS SALES_FORCE_TARGET_FLAG,
		'Y' AS DEEP_ENGAGEMENT_FLAG,
		'N' AS LIGHT_ENGAGEMENT_FLAG,
		'Y' AS CONTACTED_FLAG,
		'N' AS CAE_FLAG,
		'Deep Engagement' AS ENGAGEMENT_TYPE,
		'Y' LIGHT_DEEP_ENGAGEMENT_FLAG,
		UPPER(PRIMARY_THERAPEUTIC_AREA) AS THERAPEUTIC_AREA,
		'N' FIREWALL_CLICK_FLAG,
		NULL AS ACTIVITY_EM_STREAM,
		NULL AS ACTIVITY_EM_CATEGORY,
		NULL AS ACTIVITY_SUB_STREAM,
		NULL AS ACTIVITY_EM_NUMBER,
		NULL AS ACTIVITY_EM_NUMBER_WITH_AB_SPLIT, 
		NULL AS ACTIVITY_TYPE,
		NULL AS ACTIVITY_OFFER_CODE,
		NULL AS ACTIVITY_OFFER_DESCRIPTION,
		NULL AS NEW_DESC,
		NULL as ATTENDEE_ROLE,
		UPPER(ACCOUNT_TYPE) AS PARTY_CLASS,
		UPPER(ACCOUNT_TYPE) AS PARTY_SUBTYPE,  
		UPPER(ACCOUNT_TYPE) AS CUSTOMER_TYPE,
		0 AS BOUNCE_FLAG,
		'N' AS MONTHLY_RESENDS_EXCLUDE_FLAG,
		'N' AS OVERALL_RESENDS_EXCLUDE_FLAG,
		NULL AS DESTINATION_ID_VALUE,
		'N' AS UNSUBCRIBE_CLICKS_FLAG,
		NULL AS METRIC_TYPE,
		0 AS METRIC_TYPE_NULL_FLAG,
		CASE WHEN party_dim.PARTY_STATUS IN ('VALID','ACTIVE') THEN 'Y'
             ELSE 'N' END AS IS_ACTIVE,
		NULL AS GEO_REGION_CODE
 
	FROM 
		__SNOWFLAKE_NGDW_DB__.SEMANTIC.CALL_PLAN_ACTIVITY_FACT fca
    LEFT JOIN 
        party_dim ON 'VNID'||fca.MK_ACCOUNT_VNT_ID = party_dim.RDM_KEY
	WHERE 
		UPPER(ACCOUNT_TYPE) = 'HCP'
        AND TO_DATE(CALL_START_DATE) >= '2019-01-01' 
		AND CALL_START_DATE < SYSDATE()

UNION ALL

	SELECT  
		attendee_dim.MDM_ID || attendee_dim.EVENT_ID AS RECORD_ID,  
		1 AS ACTIVITY_SEQ_NUMBER,
		CASE WHEN LEN(attendee_dim.MDM_ID) >= 17 THEN 'VNID'||attendee_dim.MDM_ID 
			 ELSE attendee_dim.MDM_ID END AS ALIGNMENT_STRING, 
		TO_DATE(attendee_dim.CREATED_DATE,'YYYYMMDD') AS ACTIVITY_DATE,  
		TO_DATE(attendee_dim.CREATED_DATE,'YYYYMMDD') AS TIMESTAMP,
		brand.MK_PRODUCT_BRAND_PM_ID AS PRODUCT_ID,
		CASE WHEN UPPER(event_dim.PRODUCT_CODE) = 'FRANCHISE PEP' THEN 'PLEGRIDY' 
             ELSE UPPER(event_dim.PRODUCT_CODE) END AS PRODUCT_NAME,
		'Personal' AS ACTIVITY_CLASS,
		'MEDFORCE' AS SOURCE_TYPE,
		'MEDFORCE' AS ACTIVITY_SOURCE,
		'MEDFORCE' AS ACTIVITY_SOURCE_CHANNEL,
		'EVENTS' AS ACTIVITY_CHANNEL,
		event_dim.EVENT_SUB_TYPE_CODE AS ACTIVITY_SUBCHANNEL,
		event_dim.EVENT_FORMAT_CODE AS TACTIC_TYPE,
		event_dim.EVENT_NAME,
		event_dim.EVENT_ID,
		event_dim.EVENT_NAME||' ('||event_dim.EVENT_ID||')' AS TACTIC_CODE_DESC,
		NULL AS HCP_TACTIC_TYPE,   
		NULL AS SITEORIGIN,
		CASE WHEN attendee_dim.MDM_ID IS NULL OR attendee_dim.MDM_ID = 'NA' THEN 'N' 
		     ELSE 'Y' END AS IDENTIFIED_FLAG,
		CASE WHEN sf_target.MK_ACCOUNT_VNT_ID IS NOT NULL THEN 'Y' 
			 ELSE 'N' END AS SF_TARGET,    
		'Y' AS DEEP_ENGAGEMENT_FLAG,
		'N' AS LIGHT_ENGAGEMENT_FLAG,
		'Y' AS CONTACTED_FLAG,
		'N' AS CAE_FLAG,
		'Deep Engagement' AS ENGAGEMENT_TYPE,
		'Y' LIGHT_DEEP_ENGAGEMENT_FLAG,
		CASE WHEN UPPER(event_dim.THERAPEUTIC_AREA_CODE) = 'SMA' THEN 'SPINAL MUSCULAR ATROPHY' 
             ELSE UPPER(event_dim.THERAPEUTIC_AREA_CODE) END AS THERAPEUTIC_AREA,
		'N' FIREWALL_CLICK_FLAG,
		NULL AS ACTIVITY_EM_STREAM,
		NULL AS ACTIVITY_EM_CATEGORY,
		NULL AS ACTIVITY_SUB_STREAM,
		NULL AS ACTIVITY_EM_NUMBER,
		NULL AS ACTIVITY_EM_NUMBER_WITH_AB_SPLIT,  
		attendee_dim.ATTENDEE_STATUS AS ACTIVITY_TYPE,
		NULL AS ACTIVITY_OFFER_CODE,
		NULL AS ACTIVITY_OFFER_DESCRIPTION,
		event_dim.EVENT_NAME||' ('||event_dim.EVENT_ID||')' AS NEW_DESC,
		attendee_dim.ATTENDEE_ROLE AS ATTENDEE_ROLE_CODE,
		CASE WHEN UPPER (attendee_dim.ATTENDEE_TYPE) = 'CAREGIVER' THEN 'PATIENT' ELSE attendee_dim.ATTENDEE_TYPE END AS PARTY_CLASS,
		UPPER (attendee_dim.ATTENDEE_TYPE) AS PARTY_SUBTYPE,
		UPPER (attendee_dim.ATTENDEE_TYPE) AS CUSTOMER_TYPE,
		0 AS BOUNCE_FLAG,
		'N' AS MONTHLY_RESENDS_EXCLUDE_FLAG,
		'N' AS OVERALL_RESENDS_EXCLUDE_FLAG,
		NULL AS DESTINATION_ID_VALUE,
		'N' AS UNSUBCRIBE_CLICKS_FLAG,
		NULL AS METRIC_TYPE,
		0 AS METRIC_TYPE_NULL_FLAG,
		CASE WHEN party_dim.PARTY_STATUS IN ('VALID','ACTIVE') THEN 'Y'
             ELSE 'N' END AS IS_ACTIVE,
		NULL AS GEO_REGION_CODE
		
	FROM 
		__SNOWFLAKE_NGDW_DB__.INTEGRATED.MFR_MF_ATTENDEE_SCD1 attendee_dim
	INNER JOIN  
		__SNOWFLAKE_NGDW_DB__.INTEGRATED.MFR_MF_EVENT_SCD1 event_dim ON event_dim.EVENT_ID = attendee_dim.EVENT_ID
	LEFT JOIN   
		 brand ON UPPER(event_dim.PRODUCT_CODE) = UPPER(brand.PRODUCT_BRAND_NAME)
    LEFT JOIN 
        party_dim ON CASE WHEN LEN(attendee_dim.MDM_ID) >= 17 THEN 'VNID'||attendee_dim.MDM_ID 
			         ELSE attendee_dim.MDM_ID END = party_dim.RDM_KEY
    

	LEFT JOIN   
		sf_target ON attendee_dim.MDM_ID = sf_target.MK_ACCOUNT_VNT_ID 
				  AND attendee_dim.CREATED_DATE BETWEEN sf_target.CYCLE_PLAN_START_DATE 
				  AND sf_target.UPDATED_CYCLE_PLAN_END_DATE  

 
	WHERE 
		UPPER(event_dim.EVENT_STATUS_CODE) = 'CLOSED'
		AND UPPER(attendee_dim.ATTENDEE_STATUS) = 'ATND'
        AND TO_DATE(attendee_dim.CREATED_DATE,'YYYYMMDD') >= '2019-01-01'
		AND TO_DATE(attendee_dim.CREATED_DATE,'YYYYMMDD') < SYSDATE() 

UNION ALL 
	
	SELECT 
		INTEGRATION_KEY AS RECORD_ID,
		CASE WHEN UPPER(ACTIVITY_CHANNEL) = 'EMAIL & ALERTS' THEN ACTIVITY_SEQ_NUMBER 
			 ELSE 1 END AS ACTIVITY_SEQ_NUMBER,
		ALIGNMENT_STRING,
		TIME_DATE,
		DATE_TIMESTAMP,
		MK_PRODUCT_BRAND_PM_ID,
		UPPER(PRODUCT_NAME) AS PRODUCT_NAME,
		ACTIVITY_CLASS,
		SOURCE_TYPE,
		SOURCE_CODE AS ACTIVITY_SOURCE,
		ACTIVITY_SOURCE_CHANNEL,
		ACTIVITY_CHANNEL,
		ACTIVITY_SUBCHANNEL,
		TACTIC_EM_TYPE,
		TACTIC_TITLE,
		ACTUAL_TACTIC_CODE,
		TACTIC_TITLE||'('||TACTIC_CODE||')' AS TACTIC_CODE_DESC,
		NULL AS HCP_TACTIC_TYPE,    
		CASE WHEN UPPER(ATTRIBUTE_2_NAME) = 'SITEORIGIN' THEN ATTRIBUTE_2_VALUE ELSE NULL END AS SITEORIGIN,
		IDENTIFIED_FLAG,
		SF_TARGET,
		DEEP_FLAG,
		LIGHT_FLAG,
		CONTACTED_FLAG,
		CAE_FLAG,
		CASE WHEN UPPER(DEEP_FLAG) = 'Y' THEN 'Deep Engagement'
             WHEN UPPER(LIGHT_FLAG) = 'Y' THEN 'Light Engagement'
             WHEN UPPER(CONTACTED_FLAG) = 'Y' THEN 'Contacted'
             ELSE NULL END AS ENGAGEMENT_TYPE,
		CASE WHEN UPPER(DEEP_FLAG) = 'Y' OR UPPER(LIGHT_FLAG) = 'Y' THEN 'Y' 
			 ELSE 'N' END AS LIGHT_DEEP_ENGAGEMENT_FLAG,
		UPPER(THERAPEUTIC_AREA) AS THERAPEUTIC_AREA,
		FIREWALL_CLICK_FLAG,
		ACTIVITY_EM_STREAM,
		ACTIVITY_EM_CATEGORY,
		ACTIVITY_SUB_STREAM,
		ACTIVITY_EM_NUMBER,
		ACTIVITY_EM_NUMBER_WITH_AB_SPLIT,  
		ACTIVITY_TYPE,
		ACTIVITY_OFFER_CODE,
		ACTIVITY_OFFER_DESCRIPTION,
		CASE WHEN ACTIVITY_OFFER_DESCRIPTION IS NULL THEN TACTIC_TITLE||'('||TACTIC_CODE||')' ELSE ACTIVITY_OFFER_DESCRIPTION||'('||TACTIC_CODE||')' END AS NEW_DESC,
		NULL as ATTENDEE_ROLE,
		PARTY_CLASS,
		PARTY_SUBTYPE,
		CUSTOMER_TYPE,
		BOUNCE_FLAG,
		MONTHLY_RESENDS_EXCLUDE_FLAG,
		OVERALL_RESENDS_EXCLUDE_FLAG,
		DESTINATION_ID_VALUE,
		UNSUBCRIBE_CLICKS_FLAG,
		METRIC_TYPE,
		METRIC_TYPE_NULL_FLAG,
		IS_ACTIVE,
		GEO_REGION_CODE
	FROM  
		__SNOWFLAKE_NGCA_DB__.MCE.MKTG_ACTIVITY_WITH_BOUNCE_FLAG 
	WHERE 
    TIME_DATE >= '2019-01-01'
	AND TIME_DATE < SYSDATE()
/*	AND UPPER(UNSUBCRIBE_CLICKS_FLAG) = 'N' AND UPPER(FIREWALL_CLICK_FLAG) = 'N'  -- Commenting as per new request */


UNION ALL 
  SELECT	
		email_activity.ID AS RECORD_ID,
		1 AS ACTIVITY_SEQ_NUMBER,
		'VNID'||account.EXTERNAL_ID_VOD__C AS ALIGNMENT_STRING,
		TO_DATE(email_activity.ACTIVITY_DATETIME_VOD__C),
		email_activity.ACTIVITY_DATETIME_VOD__C,
		brand.MK_PRODUCT_BRAND_PM_ID,
		UPPER(product.NAME) AS PRODUCT_NAME,
		'Non-Personal' AS ACTIVITY_CLASS,
		'RTE' AS SOURCE_TYPE,
		'SFDC_US' AS ACTIVITY_SOURCE,
		'REP TRIGGERED EMAILS' AS ACTIVITY_SOURCE_CHANNEL,
		'EMAIL AND ALERTS' AS ACTIVITY_CHANNEL,
		'EMAIL' AS ACTIVITY_SUBCHANNEL,
		NULL AS TACTIC_EM_TYPE,
		NULL AS TACTIC_TITLE,
		NULL AS ACTUAL_TACTIC_CODE,
		DOCUMENT.NAME AS TACTIC_CODE_DESC,
		NULL AS HCP_TACTIC_TYPE,    
		NULL AS SITEORIGIN,
		CASE WHEN account.EXTERNAL_ID_VOD__C IS NULL THEN 'N' 
             ELSE 'Y' END AS IDENTIFIED_FLAG,
		CASE WHEN sf_target.MK_ACCOUNT_VNT_ID IS NOT NULL THEN 'Y' ELSE 'N' END AS SF_TARGET,
		CASE WHEN def.DEEP_FLAG = '1' THEN 'Y' ELSE 'N' END DEEP_FLAG,
		CASE WHEN def.LIGHT_FLAG = '1' THEN 'Y' ELSE 'N' END LIGHT_FLAG,
		CASE WHEN def.CONTACTED_FLAG = '1' THEN 'Y' ELSE 'N' END CONTACTED_FLAG,
		CASE WHEN def.CAE_FLAG = '1' THEN 'Y' ELSE 'N' END CAE_FLAG,
		CASE WHEN def.DEEP_FLAG = '1' THEN 'Deep Engagement'
			 WHEN def.LIGHT_FLAG = '1' THEN 'Light Engagement'
			 WHEN def.CONTACTED_FLAG = '1' THEN 'Contacted'
			 ELSE NULL END AS ENGAGEMENT_TYPE,
		CASE WHEN def.DEEP_FLAG = '1' OR def.LIGHT_FLAG = '1' THEN 'Y' 
			 ELSE 'N' END AS LIGHT_DEEP_ENGAGEMENT_FLAG,
		CASE WHEN UPPER(product.NAME) IN ('AVONEX','PLEGRIDY','TECFIDERA','TYSABRI','VUMERITY','ZINBRYTA','TECFIDERA CORE') THEN 'MULTIPLE SCLEROSIS'
             WHEN UPPER(product.NAME) = 'SPINRAZA' THEN 'SPINAL MUSCULAR ATROPHY'
             WHEN (UPPER(product.NAME) IN ('ADUHELM','ADUCANUMAB') OR UPPER(product.NAME) LIKE '%ALZ%') THEN 'ALZHEIMER???S DISEASE'
             WHEN UPPER(product.NAME) = 'FRANCHISE NEUROLOGY' THEN 'FRANCHISE NEUROLOGY'
             WHEN sent_email.PRODUCT_DISPLAY_VOD__C LIKE '%|%' THEN RIGHT(UPPER(sent_email.PRODUCT_DISPLAY_VOD__C),LEN(PRODUCT_DISPLAY_VOD__C) -CHARINDEX('|',UPPER(sent_email.PRODUCT_DISPLAY_VOD__C))) 
             ELSE NVL(UPPER(product.THERAPEUTIC_AREA_VOD__C),UPPER(product.NAME)) END AS THERAPEUTIC_AREA,
		'N' AS FIREWALL_CLICK_FLAG,
		NULL AS ACTIVITY_EM_STREAM,
		NULL AS ACTIVITY_EM_CATEGORY,
		NULL AS ACTIVITY_SUB_STREAM,
		NULL AS ACTIVITY_EM_NUMBER,
		NULL AS ACTIVITY_EM_NUMBER_WITH_AB_SPLIT,  
		'EMAIL ' || UPPER(TRIM(REGEXP_SUBSTR(email_activity.EVENT_TYPE_VOD__C, '[^_]+', 1))) AS ACTIVITY_TYPE,
		NULL AS ACTIVITY_OFFER_CODE,
		NULL AS ACTIVITY_OFFER_DESCRIPTION,
		document.NAME AS NEW_DESC,
		NULL AS ATTENDEE_ROLE,
		'HCP' AS PARTY_CLASS,
		'HCP' AS PARTY_SUBTYPE,
		'HCP' AS CUSTOMER_TYPE,
		 0 AS BOUNCE_FLAG,
		'N' AS MONTHLY_RESENDS_EXCLUDE_FLAG,
		'N' AS OVERALL_RESENDS_EXCLUDE_FLAG,
		NULL AS DESTINATION_ID_VALUE,
		'N' AS UNSUBCRIBE_CLICKS_FLAG,
		def.METRIC_TYPE,
		CASE WHEN def.METRIC_TYPE IS NULL THEN 1 
			 ELSE 0 END  METRIC_TYPE_NULL_FLAG,
		CASE WHEN party_dim.PARTY_STATUS IN ('VALID','ACTIVE') THEN 'Y'
             ELSE 'N' END AS IS_ACTIVE,
		'US' AS GEO_REGION_CODE

	FROM
		__SNOWFLAKE_NGDW_DB__.INTEGRATED.VCR_SF_EMAIL_ACTIVITY_VOD__C_SCD1 email_activity
	LEFT JOIN 
		 sent_email ON sent_email.ID = email_activity.SENT_EMAIL_VOD__C
	LEFT JOIN 
		 account ON sent_email.ACCOUNT_VOD__C = account.ID
	LEFT JOIN  
		document ON sent_email.APPROVED_EMAIL_TEMPLATE_VOD__C = document.ID
	LEFT JOIN 
		 product ON sent_email.PRODUCT_VOD__C = product.ID
	
	LEFT JOIN 
		 def ON 'EMAIL '|| UPPER(TRIM(REGEXP_SUBSTR(email_activity.EVENT_TYPE_VOD__C, '[^_]+', 1))) = UPPER(TRIM(def.ACTIVITY_TYPE_KEY2)) 
		     AND UPPER(TRIM(def.ACTIVITY_SOURCE_KEY1)) = 'VEEVA APPROVED EMAIL' AND TO_CHAR(def.END_DATE,'YY') = '99'
    
    LEFT JOIN   
		brand ON UPPER(product.NAME) = UPPER(brand.PRODUCT_BRAND_NAME)

	LEFT JOIN 
		sf_target ON account.EXTERNAL_ID_VOD__C = sf_target.MK_ACCOUNT_VNT_ID 
				  AND TO_DATE(email_activity.ACTIVITY_DATETIME_VOD__C) BETWEEN sf_target.CYCLE_PLAN_START_DATE
				  AND sf_target.UPDATED_CYCLE_PLAN_END_DATE  	
    LEFT JOIN   
		party_dim ON 'VNID'||account.EXTERNAL_ID_VOD__C = party_dim.RDM_KEY

	WHERE
		email_activity.ACTIVITY_DATETIME_VOD__C IS NOT NULL
        AND TO_DATE(email_activity.ACTIVITY_DATETIME_VOD__C) >= '2019-01-01'
			 

UNION ALL 

	SELECT	
		sent_email.ID AS RECORD_ID,
		1 AS ACTIVITY_SEQ_NUMBER,
		'VNID'||account.EXTERNAL_ID_VOD__C AS ALIGNMENT_STRING,
		TO_DATE(sent_email.EMAIL_SENT_DATE_VOD__C),
		sent_email.EMAIL_SENT_DATE_VOD__C,
		brand.MK_PRODUCT_BRAND_PM_ID,
		UPPER(product.NAME) AS PRODUCT_NAME,
		'Non-Personal' AS ACTIVITY_CLASS,
		'RTE' AS SOURCE_TYPE,
		'SFDC_US' AS ACTIVITY_SOURCE,
		'REP TRIGGERED EMAILS' AS ACTIVITY_SOURCE_CHANNEL,
		'EMAIL AND ALERTS' AS ACTIVITY_CHANNEL,
		'EMAIL' AS ACTIVITY_SUBCHANNEL,
		NULL AS TACTIC_EM_TYPE,
		NULL AS TACTIC_TITLE,
		NULL AS ACTUAL_TACTIC_CODE,
		DOCUMENT.NAME AS TACTIC_CODE_DESC,
		NULL AS HCP_TACTIC_TYPE,    
		NULL AS SITEORIGIN,
		CASE WHEN account.EXTERNAL_ID_VOD__C IS NULL THEN 'N' 
             ELSE 'Y' END AS IDENTIFIED_FLAG,
		CASE WHEN sf_target.MK_ACCOUNT_VNT_ID IS NOT NULL THEN 'Y' 
			 ELSE 'N' END AS SF_TARGET,
		CASE WHEN def.DEEP_FLAG = '1' THEN 'Y' 
		     ELSE 'N' END DEEP_FLAG,
		CASE WHEN def.LIGHT_FLAG = '1' THEN 'Y' 
			 ELSE 'N' END LIGHT_FLAG,
		CASE WHEN def.CONTACTED_FLAG = '1' THEN 'Y' 
			 ELSE 'N' END CONTACTED_FLAG,
		CASE WHEN def.CAE_FLAG = '1' THEN 'Y' 
		     ELSE 'N' END CAE_FLAG,
		CASE WHEN def.DEEP_FLAG = '1' THEN 'Deep Engagement'
			 WHEN def.LIGHT_FLAG = '1' THEN 'Light Engagement'
			 WHEN def.CONTACTED_FLAG = '1' THEN 'Contacted'
			 ELSE NULL END AS ENGAGEMENT_TYPE,
		CASE WHEN def.DEEP_FLAG = '1' OR def.LIGHT_FLAG = '1' THEN 'Y' 
			 ELSE 'N' END AS LIGHT_DEEP_ENGAGEMENT_FLAG,
		CASE WHEN UPPER(product.NAME) IN ('AVONEX','PLEGRIDY','TECFIDERA','TYSABRI','VUMERITY','ZINBRYTA','TECFIDERA CORE') THEN 'MULTIPLE SCLEROSIS'
             WHEN UPPER(product.NAME) = 'SPINRAZA' THEN 'SPINAL MUSCULAR ATROPHY'
             WHEN (UPPER(product.NAME) IN ('ADUHELM','ADUCANUMAB') OR UPPER(product.NAME) LIKE '%ALZ%') THEN 'ALZHEIMER???S DISEASE'
             WHEN UPPER(product.NAME) = 'FRANCHISE NEUROLOGY' THEN 'FRANCHISE NEUROLOGY'
             WHEN sent_email.PRODUCT_DISPLAY_VOD__C LIKE '%|%' THEN RIGHT(UPPER(sent_email.PRODUCT_DISPLAY_VOD__C),LEN(PRODUCT_DISPLAY_VOD__C) -CHARINDEX('|',UPPER(sent_email.PRODUCT_DISPLAY_VOD__C))) 
             ELSE NVL(UPPER(product.THERAPEUTIC_AREA_VOD__C),UPPER(product.NAME)) END AS THERAPEUTIC_AREA,  
		'N' AS FIREWALL_CLICK_FLAG,
		NULL AS ACTIVITY_EM_STREAM,
		NULL AS ACTIVITY_EM_CATEGORY,
		NULL AS ACTIVITY_SUB_STREAM,
		NULL AS ACTIVITY_EM_NUMBER,
		NULL AS ACTIVITY_EM_NUMBER_WITH_AB_SPLIT,  
		'EMAIL ' || UPPER(TRIM(REGEXP_SUBSTR(sent_email.STATUS_VOD__C, '[^_]+', 1))) AS ACTIVITY_TYPE,
		NULL AS ACTIVITY_OFFER_CODE,
		NULL AS ACTIVITY_OFFER_DESCRIPTION,
		document.NAME AS NEW_DESC,
		NULL AS ATTENDEE_ROLE,
		'HCP' AS PARTY_CLASS,
		'HCP' AS PARTY_SUBTYPE,
		'HCP' AS CUSTOMER_TYPE,
		 0 AS BOUNCE_FLAG,
		'N' AS MONTHLY_RESENDS_EXCLUDE_FLAG,
		'N' AS OVERALL_RESENDS_EXCLUDE_FLAG,
		NULL AS DESTINATION_ID_VALUE,
		'N' AS UNSUBCRIBE_CLICKS_FLAG,
		def.METRIC_TYPE,
		CASE WHEN def.METRIC_TYPE IS NULL THEN 1 
			 ELSE 0 END  METRIC_TYPE_NULL_FLAG,
		CASE WHEN party_dim.PARTY_STATUS IN ('VALID','ACTIVE') THEN 'Y'
             ELSE 'N' END AS IS_ACTIVE,
		'US' AS GEO_REGION_CODE

	FROM 
		sent_email 
	INNER JOIN 
		account ON sent_email.ACCOUNT_VOD__C = account.ID
	INNER JOIN 
		__SNOWFLAKE_NGDW_DB__.INTEGRATED.VCR_SF_USER_SCD1 salesforce_user 
		ON sent_email.OWNERID = salesforce_user.ID
	INNER JOIN 
		document ON sent_email.APPROVED_EMAIL_TEMPLATE_VOD__C = document.ID
	INNER JOIN 
		__SNOWFLAKE_NGDW_DB__.INTEGRATED.VCR_SF_RECORDTYPE_SCD1 recordtype 
		ON document.RECORDTYPEID = recordtype.ID
	LEFT JOIN 
		product ON sent_email.PRODUCT_VOD__C = product.ID
	
	LEFT JOIN 
		def ON 'EMAIL '|| UPPER(TRIM(REGEXP_SUBSTR(sent_email.STATUS_VOD__C, '[^_]+', 1))) = UPPER(TRIM(def.ACTIVITY_TYPE_KEY2)) 
		    AND UPPER(TRIM(def.ACTIVITY_SOURCE_KEY1)) = 'VEEVA APPROVED EMAIL' AND TO_CHAR(def.END_DATE,'YY') = '99'
    
    LEFT JOIN   
		 brand ON UPPER(product.NAME) = UPPER(brand.PRODUCT_BRAND_NAME)

	LEFT JOIN 
		sf_target ON account.EXTERNAL_ID_VOD__C = sf_target.MK_ACCOUNT_VNT_ID 
		          AND TO_DATE(sent_email.EMAIL_SENT_DATE_VOD__C) BETWEEN sf_target.CYCLE_PLAN_START_DATE 
				  AND sf_target.UPDATED_CYCLE_PLAN_END_DATE  	
    LEFT JOIN   
		 party_dim ON 'VNID'||account.EXTERNAL_ID_VOD__C = party_dim.RDM_KEY

  
	WHERE
		sent_email.ID not IN (SELECT NVL(SENT_EMAIL_VOD__C,'_') FROM __SNOWFLAKE_NGDW_DB__.INTEGRATED.VCR_SF_EMAIL_ACTIVITY_VOD__C_SCD1)  /* will not include records present in VCR_SF_EMAIL_ACTIVITY_VOD__C_SCD1 */
		AND sent_email.EMAIL_SENT_DATE_VOD__C IS NOT NULL
        AND TO_DATE(sent_email.EMAIL_SENT_DATE_VOD__C) >= '2019-01-01'
 
 
);


