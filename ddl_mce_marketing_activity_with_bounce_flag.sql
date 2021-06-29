
/*
MKTG_ACTIVITY_WITH_BOUNCE_FLAG
Purpose
    - Merging the bounce flag with Marketing Data
Change Log
    - 2021-06-01 - vshetiya - Created

*/

USE ROLE __SNOWFLAKE_NGCA_CIM_DEPLOY_RL__; 

CREATE OR REPLACE VIEW __SNOWFLAKE_NGCA_DB__.MCE.MKTG_ACTIVITY_WITH_BOUNCE_FLAG
COMMENT = 'VIEW CONTAINING THE MARKETING DATA WITH BOUNCE FLAG' 
AS 


WITH bounce AS	
	(
		SELECT 
			MDM_ID,
			ACTUAL_TACTIC_CODE,
			NVL(EMAIL_OPENED,0) AS EMAIL_OPENED ,
			NVL(EMAIL_BOUNCED,0) AS EMAIL_BOUNCED,
			NVL(EMAIL_SENT,0) AS EMAIL_SENT,
			NVL(CLICKED_LINK_ON_EMAIL,0) AS CLICKED_LINK_ON_EMAIL,
            CASE WHEN NVL(EMAIL_BOUNCED,0) >= NVL(EMAIL_SENT,0) AND CLICKED_LINK_ON_EMAIL IS NULL AND EMAIL_OPENED IS NULL THEN 1
			ELSE 0 END BOUNCE_FLAG
		FROM
            (
				SELECT 
					MDM_ID,
					ACTUAL_TACTIC_CODE,
					UPPER(ACTIVITY_TYPE) AS ACTIVITY_TYPE,
					DATE_NBR  
				FROM 
					__SNOWFLAKE_NGCA_DB__.MCE.MARKETING_ACTIVITY
				WHERE UPPER(SOURCE_CODE) = 'SFMC' 
					  AND UPPER(ACTIVITY_SUBCHANNEL) = 'EMAIL' 
					  AND BOUNCE_ACTIVITY_SEQ_NUMBER = 1
			)
             PIVOT
				(
				  SUM(DATE_NBR) 
				  FOR ACTIVITY_TYPE IN ('EMAIL OPENED','EMAIL BOUNCED','EMAIL SENT','CLICKED LINK ON EMAIL')
				)
                 AS PIVOT (MDM_ID,ACTUAL_TACTIC_CODE,EMAIL_OPENED,EMAIL_BOUNCED,EMAIL_SENT,CLICKED_LINK_ON_EMAIL)
	)


(
 SELECT 
	marketing_activity.ACTED_FLAG,
	marketing_activity.ACTIVITY_CHANNEL,
	marketing_activity.ACTIVITY_CLASS,
	marketing_activity.ACTIVITY_EM_CATEGORY,
	marketing_activity.ACTIVITY_EM_STREAM,
	marketing_activity.ACTIVITY_OFFER_CODE,
	marketing_activity.ACTIVITY_OFFER_DESCRIPTION,
	marketing_activity.ACTIVITY_SEQ_NUMBER,
	marketing_activity.ACTIVITY_SOURCE_CHANNEL,
	marketing_activity.ACTIVITY_SUB_STREAM,
	marketing_activity.ACTIVITY_SUBCHANNEL,
	marketing_activity.ACTIVITY_TYPE,
	marketing_activity.ACTIVITY_VENDOR,
	marketing_activity.ACTUAL_TACTIC_CODE,
	marketing_activity.ALIGNMENT_STRING,
	marketing_activity.ATTRIBUTE_1_NAME,
	marketing_activity.ATTRIBUTE_1_VALUE,
	marketing_activity.ATTRIBUTE_13_NAME,
	marketing_activity.ATTRIBUTE_13_VALUE,
	marketing_activity.ATTRIBUTE_2_NAME,
	marketing_activity.ATTRIBUTE_2_VALUE,
	marketing_activity.BOUNCE_ACTIVITY_SEQ_NUMBER,
	marketing_activity.CAE_FLAG,
	marketing_activity.CONTACTED_FLAG,
	marketing_activity.CUSTOMER_IDENTITY_TYPE,
	marketing_activity.CUSTOMER_IDENTITY_VALUE,
	marketing_activity.CUSTOMER_TYPE,
	marketing_activity.DATE_NBR,
	marketing_activity.DATE_TIMESTAMP,
	marketing_activity.DEEP_FLAG,
	marketing_activity.DESTINATION_ID_TYPE,
	marketing_activity.DESTINATION_ID_VALUE,
	marketing_activity.ENGAGED_FLAG,
	marketing_activity.FIREWALL_CLICK_FLAG,
	marketing_activity.GEO_REGION_CODE,
	marketing_activity.IDENTIFIED_FLAG,
	marketing_activity.INTEGRATION_KEY,
	marketing_activity.IS_ACTIVE,
	marketing_activity.LIGHT_FLAG,
	marketing_activity.MDM_ID,
	marketing_activity.METRIC_TYPE,
	marketing_activity.METRIC_TYPE_NULL_FLAG,
	marketing_activity.HCP_TACTIC_TYPE, 
	marketing_activity.OFFER_CODE2,
	marketing_activity.OFFER_CODE3,
	marketing_activity.OFFER_CODE4,
	marketing_activity.OFFER_CODE5,
	marketing_activity.ORIG_TACTIC_CODE_OF_RESENDS,
	marketing_activity.PRODUCT_NAME,
	marketing_activity.RDM_KEY,
	marketing_activity.REP_ID,
	marketing_activity.RESEND_FLAG,
	marketing_activity.SESSION_ID,
	marketing_activity.SF_TARGET,
	marketing_activity.SOURCE_CODE,
	marketing_activity.SOURCE_TYPE,
	marketing_activity.TACTIC_CODE,
	marketing_activity.TACTIC_DESCRIPTION,
	marketing_activity.TACTIC_DOCUMENT_NAME,
	marketing_activity.TACTIC_EM_TYPE,
	marketing_activity.TACTIC_PRODUCT,
	marketing_activity.TACTIC_TITLE,
	marketing_activity.TACTIC_TYPE,
	marketing_activity.TERRITORY_ID,
	marketing_activity.THERAPEUTIC_AREA,
	marketing_activity.TIME_DATE,
	marketing_activity.TIMESTAMP,
	marketing_activity.UNSUBCRIBE_CLICKS_FLAG,
	marketing_activity.MK_PRODUCT_BRAND_PM_ID,
	marketing_activity.MK_THERAPEUTIC_CLASS_PM_ID,
	marketing_activity.PARTY_CLASS,
	marketing_activity.PARTY_SUBTYPE, 
	marketing_activity.ACTIVITY_EM_NUMBER,
	marketing_activity.ACTIVITY_EM_NUMBER_WITH_AB_SPLIT,  
	CASE WHEN (ROW_NUMBER() OVER(PARTITION BY marketing_activity.ALIGNMENT_STRING, marketing_activity.ACTIVITY_TYPE , marketing_activity.BASE_TACTIC, DATE_TRUNC('MONTH',marketing_activity.TIME_DATE)  
			                ORDER BY bounce.BOUNCE_FLAG ,marketing_activity.DATE_TIMESTAMP,marketing_activity.INTEGRATION_KEY )) > 1 
               AND UPPER(marketing_activity.ACTIVITY_TYPE) = 'EMAIL SENT' 
		       AND UPPER(marketing_activity.SOURCE_CODE) IN ('SFDC_US','SFDC_EU','SFMC') 
	     THEN 'Y' 
		 ELSE 'N' END MONTHLY_RESENDS_EXCLUDE_FLAG,
	CASE WHEN (ROW_NUMBER() OVER(PARTITION BY marketing_activity.ALIGNMENT_STRING, marketing_activity.ACTIVITY_TYPE , marketing_activity.BASE_TACTIC  
			                ORDER BY bounce.BOUNCE_FLAG ,marketing_activity.DATE_TIMESTAMP,marketing_activity.INTEGRATION_KEY )) > 1 
               AND UPPER(marketing_activity.ACTIVITY_TYPE) = 'EMAIL SENT' 
		       AND UPPER(marketing_activity.SOURCE_CODE) IN ('SFDC_US','SFDC_EU','SFMC') 
	     THEN 'Y' 
		 ELSE 'N' END OVERALL_RESENDS_EXCLUDE_FLAG,     
	NVL(bounce.BOUNCE_FLAG, 0) AS BOUNCE_FLAG 
 FROM 
	__SNOWFLAKE_NGCA_DB__.MCE.MARKETING_ACTIVITY marketing_activity
 LEFT JOIN 
	bounce ON marketing_activity.MDM_ID = bounce.MDM_ID AND UPPER(marketing_activity.ACTUAL_TACTIC_CODE) = UPPER(bounce.ACTUAL_TACTIC_CODE)  

);
