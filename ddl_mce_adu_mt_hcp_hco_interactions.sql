/*
ADU_MT_HCP_HCO_INTERACTIONS
Purpose
    - View containing the interaction details for call,emails,events for HCP and HCO
Change Log
    - 2021-07-26 - vshetiya - Created

*/

USE ROLE __SNOWFLAKE_NGCA_CIM_DEPLOY_RL__; 

CREATE OR REPLACE VIEW __SNOWFLAKE_NGCA_DB__.MCE.ADU_MT_HCP_HCO_INTERACTIONS
COMMENT = 'VIEW CONTAINING THE INTERACTIONS DETAILS FOR HCPS AND HCOS ACROSS MULTIPLE SOURCES'
AS 
(
WITH emp_details AS
	(
		SELECT 
			salesforce_user.ID, 
			salesforce_user.MK_WORKER_BAM_ID 
		FROM 
			__SNOWFLAKE_NGDW_DB__.INTEGRATED.VCR_SF_USER_SCD1 salesforce_user
		WHERE 
			UPPER(salesforce_user.COUNTRY) IN ('US','UNITED STATES')
            AND salesforce_user.ISACTIVE > 0      
	),
	email_activity AS 
	( 
	    SELECT
		    ACTIVITY_DATETIME_VOD__C,
			EVENT_TYPE_VOD__C,
			SENT_EMAIL_VOD__C
		FROM __SNOWFLAKE_NGDW_DB__.INTEGRATED.VCR_SF_EMAIL_ACTIVITY_VOD__C_SCD1
		WHERE ACTIVITY_DATETIME_VOD__C IS NOT NULL
	),
/** Included in CTE to improve performance**/  
    ad_dimension AS 
    (
        SELECT 
            MK_ACCOUNT_VNT_ID,
            ACCOUNT_CLASS,
            ACCOUNT_STATUS_NAME
        FROM __SNOWFLAKE_NGCA_DB__.CIM.ACCOUNT_AD_DIM
    )
  
/** MARKETING TACTICS - except KNIPPER and SFDC_US **/

SELECT
	marketing_activity_fact.MDM_ID AS HCP_ID,
	'ADUHELM' AS PRODUCT_NAME,
	marketing_activity_fact.SOURCE_CODE AS MARKETING_VENDOR,
	marketing_activity_fact.SOURCE_CHANNEL_LVL2_NAME AS CHANNEL_ID,
	marketing_activity_fact.ACTUAL_TACTIC_CODE AS CONTENT_ID,  
    marketing_activity_fact.ACTIVITY_OFFER_DESCRIPTION AS OFFER_DESCRIPTION, 
    CASE 
		WHEN UPPER(marketing_activity_fact.SOURCE_CODE) = 'WEB_NEURO' THEN UPPER(TRIM(marketing_activity_fact.ATTRIBUTE_2_VALUE))
	ELSE NULL END AS WEBSITE,	
	marketing_activity_fact.TIME_DATE AS DATE_OF_ACTIVITY,
	marketing_activity_fact.ACTIVITY_TYPE,
	NULL AS EVENT_TYPE, 
	CASE 
		WHEN UPPER(marketing_activity_fact.DESTINATION_ID_TYPE) = 'URL' THEN marketing_activity_fact.DESTINATION_ID_VALUE 
	ELSE NULL END AS CLICK_THROUGH_URL,
	marketing_activity_fact.SESSION_ID AS SEND_ID,
	NULL AS REP_ID,
	marketing_activity_fact.TACTIC_TITLE AS TITLE,
	marketing_activity_fact.TACTIC_DOCUMENT_NAME AS DOCUMENT_NAME,
	marketing_activity_fact.APPROVEDFORDISTRIBUTION_DATE AS APPROVED_FOR_DISTR_DATE,
	ad_dimension.ACCOUNT_STATUS_NAME AS IS_ACTIVE_FLAG
FROM 
	__SNOWFLAKE_NGCA_DB__.MCE.MARKETING_ACTIVITY marketing_activity_fact
INNER JOIN
	ad_dimension
	ON marketing_activity_fact.MDM_ID = ad_dimension.MK_ACCOUNT_VNT_ID 
WHERE 
	UPPER(ad_dimension.ACCOUNT_CLASS) IN ('HCP','HCO')
	AND UPPER(marketing_activity_fact.SOURCE_CODE) NOT IN ('KNIPPER','SFDC_US')
    AND UPPER(marketing_activity_fact.PRODUCT_NAME) IN ('ADUHELM','ADUCANUMAB')
GROUP BY
	marketing_activity_fact.MDM_ID,
	marketing_activity_fact.PRODUCT_NAME,
	marketing_activity_fact.SOURCE_CODE,
	marketing_activity_fact.SOURCE_CHANNEL_LVL2_NAME,
	marketing_activity_fact.ACTUAL_TACTIC_CODE,
	marketing_activity_fact.TIME_DATE,
	marketing_activity_fact.ACTIVITY_TYPE,
	WEBSITE,
	CLICK_THROUGH_URL,
	marketing_activity_fact.SESSION_ID,
	marketing_activity_fact.TACTIC_TITLE,
	marketing_activity_fact.TACTIC_DOCUMENT_NAME,
	marketing_activity_fact.APPROVEDFORDISTRIBUTION_DATE,
	marketing_activity_fact.IS_ACTIVE,
	marketing_activity_fact.ACTIVITY_OFFER_DESCRIPTION,
    ad_dimension.ACCOUNT_STATUS_NAME
	
UNION ALL 

/** SFDC MARKETING - EMAIL_ACTIVITY **/

SELECT
	account.EXTERNAL_ID_VOD__C AS HCP_ID,
	UPPER(product.NAME) AS PRODUCT_NAME,
	'SFDC_US' AS MARKETING_VENDOR,
	'EMAIL' AS CHANNEL_ID,
	document.NAME AS CONTENT_ID,
	NULL AS OFFER_DESCRIPTION,
	NULL AS WEBSITE,
	TO_DATE(email_activity.ACTIVITY_DATETIME_VOD__C) AS DATE_OF_ACTIVITY,
	'EMAIL ' || TRIM(REGEXP_SUBSTR(email_activity.EVENT_TYPE_VOD__C, '[^_]+', 1)) AS ACTIVITY_TYPE,
	NULL AS EVENT_TYPE,
	NULL AS CLICK_THROUGH_URL,
	NULL AS SEND_ID,
	NULL AS REP_ID,
	NULL AS TITLE,
	NULL AS DOCUMENT_NAME,
	NULL AS APPROVED_FOR_DISTR_DATE,
	ad_dimension.ACCOUNT_STATUS_NAME AS IS_ACTIVE_FLAG
	
FROM 
	email_activity /* keeping all records from email_activity */
INNER JOIN 
    __SNOWFLAKE_NGDW_DB__.INTEGRATED.VCR_SF_SENT_EMAIL_VOD__C_SCD1 sent_email 
	ON sent_email.ID = email_activity.SENT_EMAIL_VOD__C
INNER JOIN 
	__SNOWFLAKE_NGDW_DB__.INTEGRATED.VCR_SF_ACCOUNT_SCD1 account 
	ON sent_email.ACCOUNT_VOD__C = account.ID
INNER JOIN 
	__SNOWFLAKE_NGDW_DB__.INTEGRATED.VCR_SF_APPROVED_DOCUMENT_VOD__C_SCD1 document 
	ON sent_email.APPROVED_EMAIL_TEMPLATE_VOD__C = document.ID
INNER JOIN 
    __SNOWFLAKE_NGDW_DB__.INTEGRATED.VCR_SF_PRODUCT_VOD__C_SCD1 product 
	ON sent_email.PRODUCT_VOD__C = product.ID
INNER JOIN
    ad_dimension
    ON account.EXTERNAL_ID_VOD__C = ad_dimension.MK_ACCOUNT_VNT_ID
  
WHERE 
   UPPER(product.NAME) = 'ADUHELM'

GROUP BY 
	account.EXTERNAL_ID_VOD__C,
	product.NAME,
	document.NAME,
	TO_DATE(email_activity.ACTIVITY_DATETIME_VOD__C),
	email_activity.EVENT_TYPE_VOD__C,
    ad_dimension.ACCOUNT_STATUS_NAME

UNION ALL

/** SFDC MARKETING - SENT_EMAIL **/

SELECT 
	account.EXTERNAL_ID_VOD__C AS HCP_ID,
	UPPER(product.NAME) AS PRODUCT_NAME,
	'SFDC_US' AS MARKETING_VENDOR,
	'EMAIL' AS CHANNEL_ID,
	document.NAME AS CONTENT_ID,
	NULL AS OFFER_DESCRIPTION,
	NULL AS WEBSITE,
	TO_DATE(sent_email.EMAIL_SENT_DATE_VOD__C) AS DATE_OF_ACTIVITY,
	'EMAIL ' || TRIM(REGEXP_SUBSTR(sent_email.STATUS_VOD__C, '[^_]+', 1)) AS ACTIVITY_TYPE,
	NULL AS EVENT_TYPE,
	NULL AS CLICK_THROUGH_URL,
	NULL AS SEND_ID,
	NULL AS REP_ID,
	NULL AS TITLE,
	NULL AS DOCUMENT_NAME,
	NULL AS APPROVED_FOR_DISTR_DATE,
	ad_dimension.ACCOUNT_STATUS_NAME AS IS_ACTIVE_FLAG
	
FROM 
	__SNOWFLAKE_NGDW_DB__.INTEGRATED.VCR_SF_SENT_EMAIL_VOD__C_SCD1 sent_email 
INNER JOIN 
	__SNOWFLAKE_NGDW_DB__.INTEGRATED.VCR_SF_ACCOUNT_SCD1 account 
	ON sent_email.ACCOUNT_VOD__C = account.ID
INNER JOIN 
	__SNOWFLAKE_NGDW_DB__.INTEGRATED.VCR_SF_USER_SCD1 salesforce_user 
	ON sent_email.OWNERID = salesforce_user.ID
INNER JOIN 
	__SNOWFLAKE_NGDW_DB__.INTEGRATED.VCR_SF_APPROVED_DOCUMENT_VOD__C_SCD1 document 
	ON sent_email.APPROVED_EMAIL_TEMPLATE_VOD__C = document.ID
INNER JOIN 
	__SNOWFLAKE_NGDW_DB__.INTEGRATED.VCR_SF_RECORDTYPE_SCD1 recordtype 
	ON document.RECORDTYPEID = recordtype.ID
INNER JOIN
    ad_dimension
    ON account.EXTERNAL_ID_VOD__C = ad_dimension.MK_ACCOUNT_VNT_ID
LEFT JOIN 
	__SNOWFLAKE_NGDW_DB__.INTEGRATED.VCR_SF_PRODUCT_VOD__C_SCD1 product 
	ON sent_email.PRODUCT_VOD__C = product.ID
WHERE
	sent_email.ID NOT IN (SELECT SENT_EMAIL_VOD__C FROM email_activity WHERE email_activity.SENT_EMAIL_VOD__C IS NOT NULL)  /* will not include records present in EMAIL_ACTIVITY */
	AND sent_email.EMAIL_SENT_DATE_VOD__C IS NOT NULL
    AND UPPER(product.NAME) = 'ADUHELM'

GROUP BY 
	account.EXTERNAL_ID_VOD__C,
	product.NAME,
	document.NAME,
	TO_DATE(sent_email.EMAIL_SENT_DATE_VOD__C),
	sent_email.STATUS_VOD__C,
    ad_dimension.ACCOUNT_STATUS_NAME
	
UNION ALL

/** FIELD CALLS **/

SELECT
	call_activity_fact.MK_ACCOUNT_VNT_ID AS HCP_ID,
	'ADUHELM' AS PRODUCT_NAME,
	'VEEVA' AS MARKETING_VENDOR,
	'ACTUAL CALL' AS CHANNEL_ID,
	NULL AS TOUCH_POINT_ID,
	NULL AS OFFER_DESCRIPTION,
	NULL AS WEBSITE,
	TO_DATE(call_activity_fact.CALL_START_DATE) AS DATE_OF_ACTIVITY,
	call_activity_fact.CALL_TYPE AS ACTIVITY_TYPE,
	NULL AS EVENT_TYPE,
	NULL AS CLICK_THROUGH_URL,
	NULL AS SEND_ID,
	emp_details.ID AS REP_ID,
	NULL AS TITLE,
	NULL AS DOCUMENT_NAME,
	NULL AS APPROVED_FOR_DISTR_DATE,
	ad_dimension.ACCOUNT_STATUS_NAME AS IS_ACTIVE_FLAG
	
FROM 
	__SNOWFLAKE_NGCA_DB__.CIM.FIELD_CALL_ACTIVITY_FACT call_activity_fact
INNER JOIN 
	ad_dimension
	ON call_activity_fact.MK_ACCOUNT_VNT_ID = ad_dimension.MK_ACCOUNT_VNT_ID 
LEFT JOIN 
	emp_details
	ON emp_details.MK_WORKER_BAM_ID = call_activity_fact.EMPLOYEE_ID
WHERE 
	call_activity_fact.MK_ACCOUNT_VNT_ID IS NOT NULL 
	AND UPPER(ad_dimension.ACCOUNT_CLASS) IN ('HCP','HCO')
	AND call_activity_fact.CALL_START_DATE IS NOT NULL
    AND (
         UPPER(call_activity_fact.FIRST_PRODUCT) = 'ADUHELM' OR UPPER(call_activity_fact.SECOND_PRODUCT) = 'ADUHELM' OR 
         UPPER(call_activity_fact.THIRD_PRODUCT) = 'ADUHELM' OR UPPER(call_activity_fact.FOURTH_PRODUCT) ='ADUHELM'
        )
	AND UPPER(call_activity_fact.FIELD_FORCE_NAME) IN ('AD','AD-AAL','AD-ADRM','AD-MSL','AD-TBM','AD MSL')	

GROUP BY
      call_activity_fact.MK_ACCOUNT_VNT_ID,
      DATE_OF_ACTIVITY,
      call_activity_fact.CALL_TYPE,
      emp_details.ID,
      ad_dimension.ACCOUNT_STATUS_NAME         
	
UNION ALL

/** PROGRAMS **/

SELECT
	ad_dimension.MK_ACCOUNT_VNT_ID AS HCP_ID,
	UPPER(medforce_event.PRODUCT_CODE) AS PRODUCT_NAME,
	medforce_attendee.SOURCE_NAME AS MARKETING_VENDOR,
	subtype_map.TRGT_VALUE AS CHANNEL_ID,
	medforce_event.EVENT_ID AS CONTENT_ID,
	NULL AS OFFER_DESCRIPTION,
	NULL AS WEBSITE,
	TO_DATE(medforce_event.EVENT_START_DATE_TIME, 'YYYYMMDDSSSSSS') AS DATE_OF_ACTIVITY,
	NVL(attendeerole_map.TRGT_VALUE, medforce_attendee.ATTENDEE_ROLE) AS ACTIVITY_TYPE,
	NVL(eventtype_map.TRGT_VALUE, medforce_event.EVENT_TYPE) AS EVENT_TYPE,
	NULL AS CLICK_THROUGH_URL,
	NULL AS SEND_ID,
	NULL AS REP_ID,
	NULL AS TITLE,
	NULL AS DOCUMENT_NAME,
	NULL AS APPROVED_FOR_DISTR_DATE,
	ad_dimension.ACCOUNT_STATUS_NAME AS IS_ACTIVE_FLAG
	
FROM 
	__SNOWFLAKE_NGDW_DB__.INTEGRATED.MFR_MF_ATTENDEE_SCD1 medforce_attendee
INNER JOIN
	__SNOWFLAKE_NGDW_DB__.INTEGRATED.MFR_MF_EVENT_SCD1 medforce_event
	ON medforce_attendee.EVENT_ID = medforce_event.EVENT_ID 
INNER JOIN 
	ad_dimension
	ON ad_dimension.MK_ACCOUNT_VNT_ID = medforce_attendee.MK_ACCOUNT_VNT_ID 
INNER JOIN 
	__SNOWFLAKE_NGCA_DB__.MCE.BI_REFERENCE_META attendeestatus_map
	ON attendeestatus_map.SRC_VALUE = medforce_attendee.ATTENDEE_STATUS
	AND UPPER(attendeestatus_map.SRC_ENTITY) = 'MFR_MF_EVENT_ATTENDEE' AND UPPER(attendeestatus_map.SRC_ENTITY_TYPE) = 'ATTENDEE_STATUS'
INNER JOIN 
	__SNOWFLAKE_NGCA_DB__.MCE.BI_REFERENCE_META eventstatus_map
	ON eventstatus_map.SRC_VALUE = medforce_event.EVENT_STATUS_CODE
	AND UPPER(eventstatus_map.SRC_ENTITY) = 'MFR_MF_EVENT' AND UPPER(eventstatus_map.SRC_ENTITY_TYPE) = 'EVENT_STATUS'
INNER JOIN 
	__SNOWFLAKE_NGCA_DB__.MCE.BI_REFERENCE_META subtype_map
	ON subtype_map.SRC_VALUE = medforce_event.EVENT_SUB_TYPE_CODE
	AND UPPER(subtype_map.SRC_ENTITY) = 'MFR_MF_EVENT' AND UPPER(subtype_map.SRC_ENTITY_TYPE) = 'EVENT_SUB_TYPE'
LEFT JOIN 
	__SNOWFLAKE_NGCA_DB__.MCE.BI_REFERENCE_META eventformat_map
	ON eventformat_map.SRC_VALUE = medforce_event.EVENT_FORMAT_CODE
	AND UPPER(eventformat_map.SRC_ENTITY) = 'MFR_MF_EVENT' AND UPPER(eventformat_map.SRC_ENTITY_TYPE) = 'EVENT_FORMAT'
LEFT JOIN 
	__SNOWFLAKE_NGCA_DB__.MCE.BI_REFERENCE_META attendeerole_map
	ON attendeerole_map.SRC_VALUE = medforce_attendee.ATTENDEE_ROLE
	AND UPPER(attendeerole_map.SRC_ENTITY) = 'MFR_MF_EVENT_ATTENDEE' AND UPPER(attendeerole_map.SRC_ENTITY_TYPE) = 'ATTENDEE_ROLE'
LEFT JOIN 
	__SNOWFLAKE_NGCA_DB__.MCE.BI_REFERENCE_META eventtype_map
	ON eventtype_map.SRC_VALUE = medforce_event.EVENT_TYPE
	AND UPPER(eventtype_map.SRC_ENTITY) = 'MFR_MF_EVENT' AND UPPER(eventtype_map.SRC_ENTITY_TYPE) = 'EVENT_TYPE'
WHERE 
	UPPER(attendeestatus_map.TRGT_VALUE) = 'ATTENDED'
	AND UPPER(subtype_map.TRGT_VALUE) = 'HCP PROGRAM'
	AND UPPER(eventstatus_map.TRGT_VALUE) IN ('OCCURED','CLOSED')
	AND UPPER(ad_dimension.ACCOUNT_CLASS) IN ('HCP','HCO')
	AND ad_dimension.MK_ACCOUNT_VNT_ID IS NOT NULL
    AND UPPER(medforce_event.PRODUCT_CODE) = 'ADUHELM'
GROUP BY
	ad_dimension.MK_ACCOUNT_VNT_ID,
	medforce_event.PRODUCT_CODE,
	medforce_attendee.SOURCE_NAME,
	subtype_map.TRGT_VALUE,
	medforce_event.EVENT_ID,
	TO_DATE(medforce_event.EVENT_START_DATE_TIME, 'YYYYMMDDSSSSSS'),
	attendeerole_map.TRGT_VALUE, 
    medforce_attendee.ATTENDEE_ROLE,
	eventtype_map.TRGT_VALUE, 
    medforce_event.EVENT_TYPE,
	ad_dimension.ACCOUNT_STATUS_NAME

);  

GRANT SELECT ON __SNOWFLAKE_NGCA_DB__.MCE.ADU_MT_HCP_HCO_INTERACTIONS TO ROLE IHUB_QRY_RL; 
