CREATE OR REPLACE PROCEDURE CY_IMOSPHERE_WORKSPACE.TRANSFER_OBSERVATION(run_id INT64)
OPTIONS (strict_mode=false)
BEGIN
    DECLARE max_current_id INT64;

    /*
        --[tbl_SRVisit] Transfer
    */
    SET max_current_id = COALESCE((SELECT observation_id FROM `CY_IMOSPHERE_CDM_531.observation` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.observation`
    (
        observation_id,
        person_id,
        observation_concept_id,
        observation_source_value,
        observation_date,
        observation_datetime,
        observation_type_concept_id,
        value_as_number,
        value_as_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as observation_id,
        o.person_id,
        o.observation_concept_id,
        o.observation_source_value,
        o.observation_date,
        o.observation_datetime,
        o.observation_type_concept_id,
        o.value_as_number,
        o.value_as_concept_id,
        o.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            o.person_id person_id,
            44786638 observation_concept_id, --Visit Duration
            'Visit Duration' observation_source_value,
            EXTRACT(DATE FROM DateBooked) observation_date,
            DateBooked observation_datetime,
            32817 observation_type_concept_id, --EHR
            CAST(Duration as FLOAT64) value_as_number,
            8550 value_as_concept_id, --minute,
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.tbl_SRVisit` o
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'tbl_SRVisit') vl ON vl.source_value = CONCAT(CAST(o.RowIdentifier as STRING),'_', o.person_id)
        WHERE o.DateBooked IS NOT NULL
        AND o.person_id IS NOT NULL
    ) o
    LEFT JOIN `CY_IMOSPHERE_CDM_531.observation` oo ON 
        oo.person_id = o.person_id
        AND oo.observation_concept_id = o.observation_concept_id
        AND oo.observation_source_value = o.observation_source_value
        AND COALESCE(oo.observation_source_concept_id, -1) = -1
        AND oo.observation_datetime = o.observation_datetime
        AND oo.observation_type_concept_id = o.observation_type_concept_id
        AND COALESCE(oo.provider_id, -1) = -1
        AND COALESCE(oo.visit_occurrence_id, -1) = COALESCE(o.visit_occurrence_id, -1)
        AND COALESCE(oo.visit_detail_id, -1) = -1
        AND COALESCE(oo.value_as_concept_id, -1) = COALESCE(o.value_as_concept_id, -1)
        AND COALESCE(CAST(oo.value_as_number AS STRING), '') = COALESCE(CAST(o.value_as_number AS STRING), '')
        AND COALESCE(oo.qualifier_concept_id, -1) = -1
        AND COALESCE(oo.unit_concept_id, -1) = -1
        AND COALESCE(oo.qualifier_source_value, '') = ''
        AND COALESCE(oo.unit_source_value, '') = ''
    WHERE oo.observation_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added observation', run_id, 'tbl_SRVisit -> observation', @@row_count, CURRENT_DATETIME();

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    ------------[tbl_SRReferralIn]
    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

    /*
        --[tbl_SRReferralIn - PrimaryReason] Transfer
    */
    SET max_current_id = COALESCE((SELECT observation_id FROM `CY_IMOSPHERE_CDM_531.observation` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.observation`
    (
        observation_id,
        person_id,
        observation_concept_id,
        observation_source_value,
        observation_date,
        observation_datetime,
        observation_type_concept_id,
        value_as_concept_id,
        qualifier_concept_id,
        qualifier_source_value,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as observation_id,
        o.person_id,
        o.observation_concept_id,
        o.observation_source_value,
        o.observation_date,
        o.observation_datetime,
        o.observation_type_concept_id,
        o.value_as_concept_id,
        o.qualifier_concept_id,
        o.qualifier_source_value,
        o.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            o.person_id person_id,
            4144684 observation_concept_id,  --Patient referral
            PrimaryReason observation_source_value,
            datereferral observation_date,
            PARSE_DATETIME('%Y-%m-%d%H%M%S', CONCAT(o.datereferral ,'000000')) observation_datetime,
            32817 observation_type_concept_id, --EHR
            value.concept_id value_as_concept_id,
            4260904 qualifier_concept_id, --Reason for 
            'Reason for' qualifier_source_value,
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.tbl_SRReferralIn` o
        LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_SRVisit`v ON v.idreferralin = o.RowIdentifier AND v.person_id = o.person_id
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'tbl_SRVisit') vl ON vl.source_value = CONCAT(CAST(v.RowIdentifier as STRING),'_', v.person_id)
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` value on value.source_value = o.PrimaryReason AND value.destination_table = 'OBSERVATION' AND value.source_table = 'tbl_SRReferralIn' AND value.source_column = 'PrimaryReason' 
        WHERE o.datereferral IS NOT NULL
        AND o.person_id IS NOT NULL
    ) o
    LEFT JOIN `CY_IMOSPHERE_CDM_531.observation` oo ON 
        oo.person_id = o.person_id
        AND oo.observation_concept_id = o.observation_concept_id
        AND oo.observation_source_value = o.observation_source_value
        AND COALESCE(oo.observation_source_concept_id, -1) = -1
        AND oo.observation_datetime = o.observation_datetime
        AND oo.observation_type_concept_id = o.observation_type_concept_id
        AND COALESCE(oo.provider_id, -1) = -1
        AND COALESCE(oo.visit_occurrence_id, -1) = COALESCE(o.visit_occurrence_id, -1)
        AND COALESCE(oo.visit_detail_id, -1) = -1
        AND COALESCE(oo.value_as_concept_id, -1) = COALESCE(o.value_as_concept_id, -1)
        AND COALESCE(CAST(oo.value_as_number AS STRING), '') = ''
        AND COALESCE(oo.qualifier_concept_id, -1) = COALESCE(o.qualifier_concept_id, -1)
        AND COALESCE(oo.unit_concept_id, -1) = -1
        AND COALESCE(oo.qualifier_source_value, '') = COALESCE(o.qualifier_source_value ,'')
        AND COALESCE(oo.unit_source_value, '') = ''
    WHERE oo.observation_id IS NULL;
         
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added observation', run_id, 'tbl_SRReferralIn - PrimaryReason -> observation', @@row_count, CURRENT_DATETIME();

    /*
        --[tbl_SRReferralIn - Source] Transfer
    */
    SET max_current_id = COALESCE((SELECT observation_id FROM `CY_IMOSPHERE_CDM_531.observation` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.observation`
    (
        observation_id,
        person_id,
        observation_concept_id,
        observation_source_value,
        observation_date,
        observation_datetime,
        observation_type_concept_id,
        value_as_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as observation_id,
        o.person_id,
        o.observation_concept_id,
        o.observation_source_value,
        o.observation_date,
        o.observation_datetime,
        o.observation_type_concept_id,
        o.value_as_concept_id,
        o.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            o.person_id person_id,
            4287771 observation_concept_id, --Referral source
            Source observation_source_value,
            datereferral observation_date,
            PARSE_DATETIME('%Y-%m-%d%H%M%S', CONCAT(o.datereferral ,'000000')) observation_datetime,
            32817 observation_type_concept_id, --EHR
            value.concept_id value_as_concept_id,
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.tbl_SRReferralIn` o
        LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_SRVisit`v ON v.idreferralin = o.RowIdentifier AND v.person_id = o.person_id
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'tbl_SRVisit') vl ON vl.source_value = CONCAT(CAST(v.RowIdentifier as STRING),'_', v.person_id)
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` value on value.source_value = o.Source AND value.destination_table = 'OBSERVATION' AND value.source_table = 'tbl_SRReferralIn' AND value.source_column = 'Source'
        WHERE o.datereferral IS NOT NULL
        AND o.person_id IS NOT NULL
    ) o
    LEFT JOIN `CY_IMOSPHERE_CDM_531.observation` oo ON 
        oo.person_id = o.person_id
        AND oo.observation_concept_id = o.observation_concept_id
        AND oo.observation_source_value = o.observation_source_value
        AND COALESCE(oo.observation_source_concept_id, -1) = -1
        AND oo.observation_datetime = o.observation_datetime
        AND oo.observation_type_concept_id = o.observation_type_concept_id
        AND COALESCE(oo.provider_id, -1) = -1
        AND COALESCE(oo.visit_occurrence_id, -1) = COALESCE(o.visit_occurrence_id, -1)
        AND COALESCE(oo.visit_detail_id, -1) = -1
        AND COALESCE(oo.value_as_concept_id, -1) = COALESCE(o.value_as_concept_id, -1)
        AND COALESCE(CAST(oo.value_as_number AS STRING), '') = ''
        AND COALESCE(oo.qualifier_concept_id, -1) = -1
        AND COALESCE(oo.unit_concept_id, -1) = -1
        AND COALESCE(oo.qualifier_source_value, '') = ''
        AND COALESCE(oo.unit_source_value, '') = ''
    WHERE oo.observation_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added observation', run_id, 'tbl_SRReferralIn - Source -> observation', @@row_count, CURRENT_DATETIME();

    /*
        --[tbl_SRPatient] Transfer
    */
    SET max_current_id = COALESCE((SELECT observation_id FROM `CY_IMOSPHERE_CDM_531.observation` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.observation`
    (
        observation_id,
        person_id,
        observation_concept_id,
        observation_source_value,
        observation_date,
        observation_datetime,
        observation_type_concept_id,
        value_as_concept_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as observation_id,
        o.person_id,
        o.observation_concept_id,
        o.observation_source_value,
        o.observation_date,
        o.observation_datetime,
        o.observation_type_concept_id,
        o.value_as_concept_id
    FROM
    (
        SELECT DISTINCT
            o.person_id person_id,
            44803968 observation_concept_id,
            o.ethnicity observation_source_value,
            CAST(PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(o.DateBirth ,'01000000')) AS DATE) observation_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(o.DateBirth ,'01000000')) observation_datetime,
            32817 observation_type_concept_id, --EHR
            oc.concept_id value_as_concept_id
        FROM `CY_IMOSPHERE_WORKSPACE.tbl_SRPatient` o
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on oc.source_value = o.ethnicity AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'tbl_SRPatient' AND oc.source_column = 'ethnicity'
        WHERE o.DateBirth IS NOT NULL
        AND o.person_id IS NOT NULL
    ) o
    LEFT JOIN `CY_IMOSPHERE_CDM_531.observation` oo ON 
        oo.person_id = o.person_id
        AND oo.observation_concept_id = o.observation_concept_id
        AND oo.observation_source_value = o.observation_source_value
        AND COALESCE(oo.observation_source_concept_id, -1) = -1
        AND oo.observation_datetime = o.observation_datetime
        AND oo.observation_type_concept_id = o.observation_type_concept_id
        AND COALESCE(oo.provider_id, -1) = -1
        AND COALESCE(oo.visit_occurrence_id, -1) = -1
        AND COALESCE(oo.visit_detail_id, -1) = -1
        AND COALESCE(oo.value_as_concept_id, -1) = COALESCE(o.value_as_concept_id, -1)
        AND COALESCE(CAST(oo.value_as_number AS STRING), '') = ''
        AND COALESCE(oo.qualifier_concept_id, -1) = -1
        AND COALESCE(oo.unit_concept_id, -1) = -1
        AND COALESCE(oo.qualifier_source_value, '') = ''
        AND COALESCE(oo.unit_source_value, '') = ''
    WHERE oo.observation_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added observation', run_id, 'tbl_SRPatient -> observation', @@row_count, CURRENT_DATETIME();
        
    /*
        --[tbl_SRCode] Transfer
    */
    SET max_current_id = COALESCE((SELECT observation_id FROM `CY_IMOSPHERE_CDM_531.observation` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.observation`
    (
        observation_id,
        person_id,
        observation_concept_id,
        observation_source_value,
        observation_source_concept_id,
        observation_date,
        observation_datetime,
        observation_type_concept_id,
        value_as_number,
        unit_concept_id,
        unit_source_value,
        provider_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as observation_id,
        o.person_id,
        o.observation_concept_id,
        o.observation_source_value,
        o.observation_source_concept_id,
        o.observation_date,
        o.observation_datetime,
        o.observation_type_concept_id,
        o.value_as_number,
        o.unit_concept_id,
        o.unit_source_value,
        o.provider_id
    FROM
    (
        SELECT DISTINCT
            o.person_id person_id,
            dc.concept_id observation_concept_id,
            o.ctv3code observation_source_value,
            sc.concept_id observation_source_concept_id,
            EXTRACT(DATE FROM dateevent) observation_date,
            dateevent observation_datetime,
            32817 observation_type_concept_id, --EHR
            SAFE_CAST(numericvalue as FLOAT64) value_as_number,
            unit.concept_id unit_concept_id,
            numericunit unit_source_value,
            p.provider_id
        FROM `CY_IMOSPHERE_WORKSPACE.tbl_SRCode` o
        JOIN `CY_LOOKUPS.tbl_CTV3ToSnomed_Map` sm ON sm.ctv3code = o.ctv3code
        JOIN `CY_CDM_VOCAB.concept` sc ON sc.concept_code = sm.SNOMEDCode
        JOIN `CY_CDM_VOCAB.concept_relationship` cr ON cr.concept_id_1 = sc.concept_id
        JOIN `CY_CDM_VOCAB.concept` dc ON dc.concept_id = cr.concept_id_2
        LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` unit on unit.source_value = o.numericunit AND unit.destination_table = 'OBSERVATION' AND unit.source_table = 'tbl_SRCode' AND unit.source_column = 'numericunit'
        LEFT JOIN `CY_IMOSPHERE_CDM_531.provider` p ON p.provider_id = CAST(iddoneby AS INT64)
        WHERE sc.vocabulary_id = 'SNOMED'
        AND cr.relationship_id = 'Maps to'
        AND dc.standard_concept = 'S'
        AND dc.domain_id NOT IN ('Condition', 'Procedure', 'Drug', 'Measurement', 'Device')
        AND o.dateevent IS NOT NULL
        AND o.person_id IS NOT NULL
    ) o
    LEFT JOIN `CY_IMOSPHERE_CDM_531.observation` oo ON 
        oo.person_id = o.person_id
        AND oo.observation_concept_id = o.observation_concept_id
        AND oo.observation_source_value = o.observation_source_value
        AND COALESCE(oo.observation_source_concept_id, -1) = COALESCE(o.observation_source_concept_id, -1)
        AND oo.observation_datetime = o.observation_datetime
        AND oo.observation_type_concept_id = o.observation_type_concept_id
        AND COALESCE(oo.provider_id, -1) = COALESCE(o.provider_id, -1)
        AND COALESCE(oo.visit_occurrence_id, -1) = -1
        AND COALESCE(oo.visit_detail_id, -1) = -1
        AND COALESCE(oo.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(oo.value_as_number AS STRING), '') = COALESCE(CAST(o.value_as_number AS STRING), '')
        AND COALESCE(oo.qualifier_concept_id, -1) = -1
        AND COALESCE(oo.unit_concept_id, -1) =  COALESCE(o.unit_concept_id, -1)
        AND COALESCE(oo.qualifier_source_value, '') = ''
        AND COALESCE(oo.unit_source_value, '') =  COALESCE(o.unit_source_value, '')
    WHERE oo.observation_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added observation', run_id, 'tbl_SRCode -> observation', @@row_count, CURRENT_DATETIME();

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    ------------[SUS_BRI_OP_010415_to_300619]
    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    /*
        --[SUS_BRI_OP_010415_to_300619 - Ethnic_Category] Transfer
    */
    SET max_current_id = COALESCE((SELECT observation_id FROM `CY_IMOSPHERE_CDM_531.observation` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.observation`
    (
        observation_id,
        person_id,
        observation_concept_id,
        observation_source_value,
        observation_date,
        observation_datetime,
        observation_type_concept_id,
        value_as_concept_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as observation_id,
        o.person_id,
        o.observation_concept_id,
        o.observation_source_value,
        o.observation_date,
        o.observation_datetime,
        o.observation_type_concept_id,
        o.value_as_concept_id
    FROM
    (
        SELECT DISTINCT
            o.person_id person_id,
            44803968 observation_concept_id, --Ethnicity
            o.Ethnic_Category observation_source_value,
            PARSE_DATE('%Y%m%d', CONCAT(o.Date_of_Birth ,'01')) observation_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(o.Date_of_Birth ,'01000000')) observation_datetime,
            32817 observation_type_concept_id, --EHR
            oc.concept_id value_as_concept_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_OP_010415_to_300619` o
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on oc.source_value = o.Ethnic_Category AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_OP_010415_to_300619' AND oc.source_column = 'Ethnic_Category'
        WHERE (o.Date_of_Birth IS NOT NULL AND o.Date_of_Birth NOT LIKE '% %')
        AND o.person_id IS NOT NULL
    ) o
    LEFT JOIN `CY_IMOSPHERE_CDM_531.observation` oo ON 
        oo.person_id = o.person_id
        AND oo.observation_concept_id = o.observation_concept_id
        AND oo.observation_source_value = o.observation_source_value
        AND COALESCE(oo.observation_source_concept_id, -1) = -1
        AND oo.observation_datetime = o.observation_datetime
        AND oo.observation_type_concept_id = o.observation_type_concept_id
        AND COALESCE(oo.provider_id, -1) = -1
        AND COALESCE(oo.visit_occurrence_id, -1) = -1
        AND COALESCE(oo.visit_detail_id, -1) = -1
        AND COALESCE(oo.value_as_concept_id, -1) = COALESCE(o.value_as_concept_id, -1)
        AND COALESCE(CAST(oo.value_as_number AS STRING), '') = ''
        AND COALESCE(oo.qualifier_concept_id, -1) = -1
        AND COALESCE(oo.unit_concept_id, -1) =  -1
        AND COALESCE(oo.qualifier_source_value, '') = ''
        AND COALESCE(oo.unit_source_value, '') =  ''
    WHERE oo.observation_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added observation', run_id, 'SUS_BRI_OP_010415_to_300619 - Ethnic_Category -> observation', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_OP_010415_to_300619 - Attended_Or_Did_Not_Attend] Transfer
    */
    SET max_current_id = COALESCE((SELECT observation_id FROM `CY_IMOSPHERE_CDM_531.observation` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.observation`
    (
        observation_id,
        person_id,
        observation_concept_id,
        observation_source_value,
        observation_date,
        observation_datetime,
        observation_type_concept_id,
        value_as_concept_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as observation_id,
        o.person_id,
        o.observation_concept_id,
        o.observation_source_value,
        o.observation_date,
        o.observation_datetime,
        o.observation_type_concept_id,
        o.value_as_concept_id
    FROM
    (
        SELECT DISTINCT
            o.person_id person_id,
            443364 observation_concept_id, --Patient encounter status
            o.Attended_Or_Did_Not_Attend observation_source_value,
            PARSE_DATE('%Y%m%d', o.Appointment_Date) observation_date,
            CASE WHEN Appointment_Time LIKE '% %' OR Appointment_Time IS NULL
                THEN PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(Appointment_Date, '000000'))
                ELSE PARSE_DATETIME('%Y%m%d%H:%M%S', CONCAT(Appointment_Date, Appointment_Time, '00'))
            END observation_datetime,
            32817 observation_type_concept_id, --EHR
            oc.concept_id value_as_concept_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_OP_010415_to_300619` o
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on oc.source_value = o.Attended_Or_Did_Not_Attend AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_OP_010415_to_300619' AND oc.source_column = 'Attended_Or_Did_Not_Attend'
        WHERE o.Appointment_Date IS NOT NULL
        AND o.person_id IS NOT NULL
    ) o
    LEFT JOIN `CY_IMOSPHERE_CDM_531.observation` oo ON 
        oo.person_id = o.person_id
        AND oo.observation_concept_id = o.observation_concept_id
        AND oo.observation_source_value = o.observation_source_value
        AND COALESCE(oo.observation_source_concept_id, -1) = -1
        AND oo.observation_datetime = o.observation_datetime
        AND oo.observation_type_concept_id = o.observation_type_concept_id
        AND COALESCE(oo.provider_id, -1) = -1
        AND COALESCE(oo.visit_occurrence_id, -1) = -1
        AND COALESCE(oo.visit_detail_id, -1) = -1
        AND COALESCE(oo.value_as_concept_id, -1) = COALESCE(o.value_as_concept_id, -1)
        AND COALESCE(CAST(oo.value_as_number AS STRING), '') = ''
        AND COALESCE(oo.qualifier_concept_id, -1) = -1
        AND COALESCE(oo.unit_concept_id, -1) =  -1
        AND COALESCE(oo.qualifier_source_value, '') = ''
        AND COALESCE(oo.unit_source_value, '') =  ''
    WHERE oo.observation_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added observation', run_id, 'SUS_BRI_OP_010415_to_300619 - Attended_Or_Did_Not_Attend -> observation', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_OP_010415_to_300619 - Source_Of_Referral_For_Outpatients] Transfer
    */
    SET max_current_id = COALESCE((SELECT observation_id FROM `CY_IMOSPHERE_CDM_531.observation` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.observation`
    (
        observation_id,
        person_id,
        observation_concept_id,
        observation_source_value,
        observation_date,
        observation_datetime,
        observation_type_concept_id,
        value_as_concept_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as observation_id,
        o.person_id,
        o.observation_concept_id,
        o.observation_source_value,
        o.observation_date,
        o.observation_datetime,
        o.observation_type_concept_id,
        o.value_as_concept_id
    FROM
    (
        SELECT DISTINCT
            o.person_id person_id,
            4287771 observation_concept_id, --Referral source
            o.Source_Of_Referral_For_Outpatients observation_source_value,
            PARSE_DATE('%Y%m%d', o.Referral_Request_Received_Date) observation_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(o.Referral_Request_Received_Date ,'000000')) observation_datetime,
            32817 observation_type_concept_id, --EHR
            oc.concept_id value_as_concept_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_OP_010415_to_300619` o
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on oc.source_value = o.Source_Of_Referral_For_Outpatients AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_OP_010415_to_300619' AND oc.source_column = 'Source_Of_Referral_For_Outpatients'
        WHERE (o.Referral_Request_Received_Date IS NOT NULL AND o.Referral_Request_Received_Date NOT LIKE '% %')
        AND o.person_id IS NOT NULL
    ) o
    LEFT JOIN `CY_IMOSPHERE_CDM_531.observation` oo ON 
        oo.person_id = o.person_id
        AND oo.observation_concept_id = o.observation_concept_id
        AND oo.observation_source_value = o.observation_source_value
        AND COALESCE(oo.observation_source_concept_id, -1) = -1
        AND oo.observation_datetime = o.observation_datetime
        AND oo.observation_type_concept_id = o.observation_type_concept_id
        AND COALESCE(oo.provider_id, -1) = -1
        AND COALESCE(oo.visit_occurrence_id, -1) = -1
        AND COALESCE(oo.visit_detail_id, -1) = -1
        AND COALESCE(oo.value_as_concept_id, -1) = COALESCE(o.value_as_concept_id, -1)
        AND COALESCE(CAST(oo.value_as_number AS STRING), '') = ''
        AND COALESCE(oo.qualifier_concept_id, -1) = -1
        AND COALESCE(oo.unit_concept_id, -1) =  -1
        AND COALESCE(oo.qualifier_source_value, '') = ''
        AND COALESCE(oo.unit_source_value, '') =  ''
    WHERE oo.observation_id IS NULL;
    
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added observation', run_id, 'SUS_BRI_OP_010415_to_300619 - Source_Of_Referral_For_Outpatients -> observation', @@row_count, CURRENT_DATETIME();

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    ------------[SUS_BRI_APC_010415_to_300619_P1]
    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

    /*
        --[SUS_BRI_APC_010415_to_300619_P1 - Ethnic_Category] Transfer
    */
    SET max_current_id = COALESCE((SELECT observation_id FROM `CY_IMOSPHERE_CDM_531.observation` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.observation`
    (
        observation_id,
        person_id,
        observation_concept_id,
        observation_source_value,
        observation_date,
        observation_datetime,
        observation_type_concept_id,
        value_as_concept_id,
        visit_occurrence_id,
        visit_detail_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as observation_id,
        o.person_id,
        o.observation_concept_id,
        o.observation_source_value,
        o.observation_date,
        o.observation_datetime,
        o.observation_type_concept_id,
        o.value_as_concept_id,
        o.visit_occurrence_id,
        o.visit_detail_id
    FROM
    (
        SELECT DISTINCT
            o.person_id person_id,
            44803968 observation_concept_id, --Ethnicity
            o.Ethnic_Group observation_source_value,
            PARSE_DATE('%Y%m%d', CONCAT(o.Date_of_Birth ,'01')) observation_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(o.Date_of_Birth ,'01000000')) observation_datetime,
            32817 observation_type_concept_id, --EHR
            oc.concept_id value_as_concept_id,
            vl.id visit_occurrence_id,
            vdl.id visit_detail_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1` o
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on oc.source_value = o.Ethnic_Group AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_APC_010415_to_300619_P1' AND oc.source_column = 'Ethnic_Group'
        LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups` vdl ON vdl.source_value = CONCAT(CAST(o.Hospital_Provider_Spell_Number as STRING),'_', o.person_id, '_', o.Episode_Number)
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = CONCAT(CAST(o.Hospital_Provider_Spell_Number as STRING),'_', o.person_id) 
        WHERE (o.Date_of_Birth IS NOT NULL AND o.Date_of_Birth NOT LIKE '% %')
        AND o.person_id IS NOT NULL
    ) o
    LEFT JOIN `CY_IMOSPHERE_CDM_531.observation` oo ON 
        oo.person_id = o.person_id
        AND oo.observation_concept_id = o.observation_concept_id
        AND oo.observation_source_value = o.observation_source_value
        AND COALESCE(oo.observation_source_concept_id, -1) = -1
        AND oo.observation_datetime = o.observation_datetime
        AND oo.observation_type_concept_id = o.observation_type_concept_id
        AND COALESCE(oo.provider_id, -1) = -1
        AND COALESCE(oo.visit_occurrence_id, -1) = COALESCE(o.visit_occurrence_id, -1)
        AND COALESCE(oo.visit_detail_id, -1) = COALESCE(o.visit_detail_id, -1)
        AND COALESCE(oo.value_as_concept_id, -1) = COALESCE(o.value_as_concept_id, -1)
        AND COALESCE(CAST(oo.value_as_number AS STRING), '') = ''
        AND COALESCE(oo.qualifier_concept_id, -1) = -1
        AND COALESCE(oo.unit_concept_id, -1) =  -1
        AND COALESCE(oo.qualifier_source_value, '') = ''
        AND COALESCE(oo.unit_source_value, '') =  ''
    WHERE oo.observation_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added observation', run_id, 'SUS_BRI_APC_010415_to_300619_P1 - Ethnic_Category -> observation', @@row_count, CURRENT_DATETIME();
        
    /*
        --[SUS_BRI_APC_010415_to_300619_P1 - Discharge_Method_Hospital_Provider_Spell] Transfer   
    */
    SET max_current_id = COALESCE((SELECT observation_id FROM `CY_IMOSPHERE_CDM_531.observation` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.observation`
    (
        observation_id,
        person_id,
        observation_concept_id,
        observation_source_value,
        observation_date,
        observation_datetime,
        observation_type_concept_id,
        value_as_concept_id,
        visit_occurrence_id,
        visit_detail_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as observation_id,
        o.person_id,
        o.observation_concept_id,
        o.observation_source_value,
        o.observation_date,
        o.observation_datetime,
        o.observation_type_concept_id,
        o.value_as_concept_id,
        o.visit_occurrence_id,
        o.visit_detail_id
    FROM
    (
        SELECT DISTINCT
            o.person_id person_id,
            4240723 observation_concept_id, --Patient discharge
            o.Discharge_Method_Hospital_Provider_Spell observation_source_value,
            PARSE_DATE('%Y%m%d', o.Discharge_Date_From_Hospital_Provider_Spell) observation_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(o.Discharge_Date_From_Hospital_Provider_Spell ,'000000')) observation_datetime,
            32817 observation_type_concept_id, --EHR
            oc.concept_id value_as_concept_id,
            vl.id visit_occurrence_id,
            vdl.id visit_detail_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1` o
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on oc.source_value = o.Discharge_Method_Hospital_Provider_Spell AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_APC_010415_to_300619_P1' AND oc.source_column =  'Discharge_Method_Hospital_Provider_Spell'
        LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups` vdl ON vdl.source_value = CONCAT(CAST(o.Hospital_Provider_Spell_Number as STRING),'_', o.person_id, '_', o.Episode_Number)
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = CONCAT(CAST(o.Hospital_Provider_Spell_Number as STRING),'_', o.person_id) 
        WHERE (o.Discharge_Date_From_Hospital_Provider_Spell IS NOT NULL AND o.Discharge_Date_From_Hospital_Provider_Spell NOT LIKE '% %')
        AND o.person_id IS NOT NULL
    ) o
    LEFT JOIN `CY_IMOSPHERE_CDM_531.observation` oo ON 
        oo.person_id = o.person_id
        AND oo.observation_concept_id = o.observation_concept_id
        AND oo.observation_source_value = o.observation_source_value
        AND COALESCE(oo.observation_source_concept_id, -1) = -1
        AND oo.observation_datetime = o.observation_datetime
        AND oo.observation_type_concept_id = o.observation_type_concept_id
        AND COALESCE(oo.provider_id, -1) = -1
        AND COALESCE(oo.visit_occurrence_id, -1) = COALESCE(o.visit_occurrence_id, -1)
        AND COALESCE(oo.visit_detail_id, -1) = COALESCE(o.visit_detail_id, -1)
        AND COALESCE(oo.value_as_concept_id, -1) = COALESCE(o.value_as_concept_id, -1)
        AND COALESCE(CAST(oo.value_as_number AS STRING), '') = ''
        AND COALESCE(oo.qualifier_concept_id, -1) = -1
        AND COALESCE(oo.unit_concept_id, -1) =  -1
        AND COALESCE(oo.qualifier_source_value, '') = ''
        AND COALESCE(oo.unit_source_value, '') =  ''
    WHERE oo.observation_id IS NULL;
    
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added observation',  run_id, 'SUS_BRI_APC_010415_to_300619_P1 - Discharge_Method_Hospital_Provider_Spell -> observation', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_APC_010415_to_300619_P1 - Admission_Method_Hospital_Provider_Spell] Transfer   
    */
    SET max_current_id = COALESCE((SELECT observation_id FROM `CY_IMOSPHERE_CDM_531.observation` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.observation`
    (
        observation_id,
        person_id,
        observation_concept_id,
        observation_source_value,
        observation_date,
        observation_datetime,
        observation_type_concept_id,
        value_as_concept_id,
        visit_occurrence_id,
        visit_detail_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as observation_id,
        o.person_id,
        o.observation_concept_id,
        o.observation_source_value,
        o.observation_date,
        o.observation_datetime,
        o.observation_type_concept_id,
        o.value_as_concept_id,
        o.visit_occurrence_id,
        o.visit_detail_id
    FROM
    (
        SELECT DISTINCT
            o.person_id person_id,
            8715 observation_concept_id, --Hospital Admission
            o.Admission_Method_Hospital_Provider_Spell observation_source_value,
            PARSE_DATE('%Y%m%d', o.Start_Date_Hospital_Provider_Spell) observation_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(o.Start_Date_Hospital_Provider_Spell ,'000000')) observation_datetime,
            32817 observation_type_concept_id, --EHR
            oc.concept_id value_as_concept_id,
            vl.id visit_occurrence_id,
            vdl.id visit_detail_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1` o
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on oc.source_value = o.Admission_Method_Hospital_Provider_Spell AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_APC_010415_to_300619_P1' AND oc.source_column =  'Admission_Method_Hospital_Provider_Spell'
        LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups` vdl ON vdl.source_value = CONCAT(CAST(o.Hospital_Provider_Spell_Number as STRING),'_', o.person_id, '_', o.Episode_Number)
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = CONCAT(CAST(o.Hospital_Provider_Spell_Number as STRING),'_', o.person_id) 
        WHERE (o.Start_Date_Hospital_Provider_Spell IS NOT NULL AND o.Start_Date_Hospital_Provider_Spell NOT LIKE '% %')
        AND o.person_id IS NOT NULL
    ) o
    LEFT JOIN `CY_IMOSPHERE_CDM_531.observation` oo ON 
        oo.person_id = o.person_id
        AND oo.observation_concept_id = o.observation_concept_id
        AND oo.observation_source_value = o.observation_source_value
        AND COALESCE(oo.observation_source_concept_id, -1) = -1
        AND oo.observation_datetime = o.observation_datetime
        AND oo.observation_type_concept_id = o.observation_type_concept_id
        AND COALESCE(oo.provider_id, -1) = -1
        AND COALESCE(oo.visit_occurrence_id, -1) = COALESCE(o.visit_occurrence_id, -1)
        AND COALESCE(oo.visit_detail_id, -1) = COALESCE(o.visit_detail_id, -1)
        AND COALESCE(oo.value_as_concept_id, -1) = COALESCE(o.value_as_concept_id, -1)
        AND COALESCE(CAST(oo.value_as_number AS STRING), '') = ''
        AND COALESCE(oo.qualifier_concept_id, -1) = -1
        AND COALESCE(oo.unit_concept_id, -1) =  -1
        AND COALESCE(oo.qualifier_source_value, '') = ''
        AND COALESCE(oo.unit_source_value, '') =  ''
    WHERE oo.observation_id IS NULL;
         
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added observation', run_id, 'SUS_BRI_APC_010415_to_300619_P1 - Admission_Method_Hospital_Provider_Spell -> observation', @@row_count, CURRENT_DATETIME();


    /*
        --[SUS_BRI_APC_010415_to_300619_P1 - Discharge_Destination_Hospital_Provider_Spell] Transfer   
    */
    SET max_current_id = COALESCE((SELECT observation_id FROM `CY_IMOSPHERE_CDM_531.observation` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.observation`
    (
        observation_id,
        person_id,
        observation_concept_id,
        observation_source_value,
        observation_date,
        observation_datetime,
        observation_type_concept_id,
        value_as_concept_id,
        visit_occurrence_id,
        visit_detail_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as observation_id,
        o.person_id,
        o.observation_concept_id,
        o.observation_source_value,
        o.observation_date,
        o.observation_datetime,
        o.observation_type_concept_id,
        o.value_as_concept_id,
        o.visit_occurrence_id,
        o.visit_detail_id
    FROM
    (
        SELECT DISTINCT
            o.person_id person_id,
            4240723 observation_concept_id, --Patient discharge
            o.Discharge_Destination_Hospital_Provider_Spell observation_source_value,
            PARSE_DATE('%Y%m%d', o.Discharge_Date_From_Hospital_Provider_Spell) observation_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(o.Discharge_Date_From_Hospital_Provider_Spell ,'000000')) observation_datetime,
            32817 observation_type_concept_id, --EHR
            oc.concept_id value_as_concept_id,
            vl.id visit_occurrence_id,
            vdl.id visit_detail_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1` o
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on oc.source_value = o.Discharge_Destination_Hospital_Provider_Spell AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_APC_010415_to_300619_P1' AND oc.source_column =  'Discharge_Destination_Hospital_Provider_Spell'
        LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups` vdl ON vdl.source_value = CONCAT(CAST(o.Hospital_Provider_Spell_Number as STRING),'_', o.person_id, '_', o.Episode_Number)
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = CONCAT(CAST(o.Hospital_Provider_Spell_Number as STRING),'_', o.person_id) 
        WHERE (o.Discharge_Date_From_Hospital_Provider_Spell IS NOT NULL AND o.Discharge_Date_From_Hospital_Provider_Spell NOT LIKE '% %')
        AND o.person_id IS NOT NULL
    ) o
    LEFT JOIN `CY_IMOSPHERE_CDM_531.observation` oo ON 
        oo.person_id = o.person_id
        AND oo.observation_concept_id = o.observation_concept_id
        AND oo.observation_source_value = o.observation_source_value
        AND COALESCE(oo.observation_source_concept_id, -1) = -1
        AND oo.observation_datetime = o.observation_datetime
        AND oo.observation_type_concept_id = o.observation_type_concept_id
        AND COALESCE(oo.provider_id, -1) = -1
        AND COALESCE(oo.visit_occurrence_id, -1) = COALESCE(o.visit_occurrence_id, -1)
        AND COALESCE(oo.visit_detail_id, -1) = COALESCE(o.visit_detail_id, -1)
        AND COALESCE(oo.value_as_concept_id, -1) = COALESCE(o.value_as_concept_id, -1)
        AND COALESCE(CAST(oo.value_as_number AS STRING), '') = ''
        AND COALESCE(oo.qualifier_concept_id, -1) = -1
        AND COALESCE(oo.unit_concept_id, -1) =  -1
        AND COALESCE(oo.qualifier_source_value, '') = ''
        AND COALESCE(oo.unit_source_value, '') =  ''
    WHERE oo.observation_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added observation', run_id, 'SUS_BRI_APC_010415_to_300619_P1 - Discharge_Destination_Hospital_Provider_Spell -> observation', @@row_count, CURRENT_DATETIME();

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    ------------[src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2]
    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
        
    /*
        --[src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2 - Ethnic_Group] Transfer
    */
    SET max_current_id = COALESCE((SELECT observation_id FROM `CY_IMOSPHERE_CDM_531.observation` ORDER BY 1 DESC LIMIT 1), 0);
    
    INSERT INTO `CY_IMOSPHERE_CDM_531.observation`
    (
        observation_id,
        person_id,
        observation_concept_id,
        observation_source_value,
        observation_date,
        observation_datetime,
        observation_type_concept_id,
        value_as_concept_id,
        visit_occurrence_id,
        visit_detail_id
    )

    WITH dob_cte AS
    (
      SELECT DISTINCT        
        p.person_id person_id,
        CASE WHEN CAST(SPLIT(Date_of_Birth_mm_yy, '-')[OFFSET(1)] AS INT64) BETWEEN 00 AND 22 THEN CONCAT('20', CAST(SPLIT(Date_of_Birth_mm_yy, '-')[OFFSET(1)] as STRING))
        ELSE CONCAT('19', CAST(SPLIT(Date_of_Birth_mm_yy, '-')[OFFSET(1)] as STRING))      
        END year_of_birth,
        FORMAT_DATE( "%m", PARSE_DATE("%b-%y",Date_of_Birth_mm_yy )) month_of_birth
      FROM `CY_IMOSPHERE_WORKSPACE.src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2` p
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as observation_id,
        o.person_id,
        o.observation_concept_id,
        o.observation_source_value,
        o.observation_date,
        o.observation_datetime,
        o.observation_type_concept_id,
        o.value_as_concept_id,
        o.visit_occurrence_id,
        o.visit_detail_id
    FROM
    (
        SELECT DISTINCT
            o.person_id person_id,
            44803968 observation_concept_id, --Ethnicity
            o.Ethnic_Group observation_source_value,
            PARSE_DATE('%Y%m%d', CONCAT(dob.year_of_birth, dob.month_of_birth ,'01')) observation_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(dob.year_of_birth, dob.month_of_birth ,'01000000')) observation_datetime,
            32817 observation_type_concept_id, --EHR
            oc.concept_id value_as_concept_id,
            vl.id visit_occurrence_id,
            vdl.id visit_detail_id
        FROM `CY_IMOSPHERE_WORKSPACE.src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2` o
        JOIN dob_cte dob ON dob.person_id = o.person_id
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on oc.source_value = o.Ethnic_Group AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2' AND oc.source_column = 'Ethnic_Group'
        LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups` vdl ON vdl.source_value = CONCAT(CAST(o.Hospital_Provider_Spell_Number as STRING),'_', o.person_id, '_', o.Episode_Number)
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = CONCAT(CAST(o.Hospital_Provider_Spell_Number as STRING),'_', o.person_id) 
        WHERE (o.Date_of_Birth_mm_yy IS NOT NULL AND o.Date_of_Birth_mm_yy NOT LIKE '% %')
        AND o.person_id IS NOT NULL
    ) o
    LEFT JOIN `CY_IMOSPHERE_CDM_531.observation` oo ON 
        oo.person_id = o.person_id
        AND oo.observation_concept_id = o.observation_concept_id
        AND oo.observation_source_value = o.observation_source_value
        AND COALESCE(oo.observation_source_concept_id, -1) = -1
        AND oo.observation_datetime = o.observation_datetime
        AND oo.observation_type_concept_id = o.observation_type_concept_id
        AND COALESCE(oo.provider_id, -1) = -1
        AND COALESCE(oo.visit_occurrence_id, -1) = COALESCE(o.visit_occurrence_id, -1)
        AND COALESCE(oo.visit_detail_id, -1) = COALESCE(o.visit_detail_id, -1)
        AND COALESCE(oo.value_as_concept_id, -1) = COALESCE(o.value_as_concept_id, -1)
        AND COALESCE(CAST(oo.value_as_number AS STRING), '') = ''
        AND COALESCE(oo.qualifier_concept_id, -1) = -1
        AND COALESCE(oo.unit_concept_id, -1) =  -1
        AND COALESCE(oo.qualifier_source_value, '') = ''
        AND COALESCE(oo.unit_source_value, '') =  ''
    WHERE oo.observation_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added observation', run_id, 'src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2 - Ethnic_Group -> observation', @@row_count, CURRENT_DATETIME();

    /*
        --[src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2 - Discharge_Method_Hospital_Provider_Spell] Transfer   
    */
    SET max_current_id = COALESCE((SELECT observation_id FROM `CY_IMOSPHERE_CDM_531.observation` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.observation`
    (
        observation_id,
        person_id,
        observation_concept_id,
        observation_source_value,
        observation_date,
        observation_datetime,
        observation_type_concept_id,
        value_as_concept_id,
        visit_occurrence_id,
        visit_detail_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as observation_id,
        o.person_id,
        o.observation_concept_id,
        o.observation_source_value,
        o.observation_date,
        o.observation_datetime,
        o.observation_type_concept_id,
        o.value_as_concept_id,
        o.visit_occurrence_id,
        o.visit_detail_id
    FROM
    (
        SELECT DISTINCT
            o.person_id person_id,
            4240723 observation_concept_id, --Patient discharge
            o.Discharge_Method_Hospital_Provider_Spell observation_source_value,
            PARSE_DATE('%Y%m%d', o.Discharge_Date_From_Hospital_Provider_Spell) observation_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(o.Discharge_Date_From_Hospital_Provider_Spell ,'000000')) observation_datetime,
            32817 observation_type_concept_id, --EHR
            oc.concept_id value_as_concept_id,
            vl.id visit_occurrence_id,
            vdl.id visit_detail_id
        FROM `CY_IMOSPHERE_WORKSPACE.src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2` o
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on oc.source_value = o.Discharge_Method_Hospital_Provider_Spell AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2' AND oc.source_column = 'Discharge_Method_Hospital_Provider_Spell'
        LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups` vdl ON vdl.source_value = CONCAT(CAST(o.Hospital_Provider_Spell_Number as STRING),'_', o.person_id, '_', o.Episode_Number)
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = CONCAT(CAST(o.Hospital_Provider_Spell_Number as STRING),'_', o.person_id) 
        WHERE (o.Discharge_Date_From_Hospital_Provider_Spell IS NOT NULL AND o.Discharge_Date_From_Hospital_Provider_Spell NOT LIKE '% %')
        AND o.person_id IS NOT NULL
    ) o
    LEFT JOIN `CY_IMOSPHERE_CDM_531.observation` oo ON 
        oo.person_id = o.person_id
        AND oo.observation_concept_id = o.observation_concept_id
        AND oo.observation_source_value = o.observation_source_value
        AND COALESCE(oo.observation_source_concept_id, -1) = -1
        AND oo.observation_datetime = o.observation_datetime
        AND oo.observation_type_concept_id = o.observation_type_concept_id
        AND COALESCE(oo.provider_id, -1) = -1
        AND COALESCE(oo.visit_occurrence_id, -1) = COALESCE(o.visit_occurrence_id, -1)
        AND COALESCE(oo.visit_detail_id, -1) = COALESCE(o.visit_detail_id, -1)
        AND COALESCE(oo.value_as_concept_id, -1) = COALESCE(o.value_as_concept_id, -1)
        AND COALESCE(CAST(oo.value_as_number AS STRING), '') = ''
        AND COALESCE(oo.qualifier_concept_id, -1) = -1
        AND COALESCE(oo.unit_concept_id, -1) =  -1
        AND COALESCE(oo.qualifier_source_value, '') = ''
        AND COALESCE(oo.unit_source_value, '') =  ''
    WHERE oo.observation_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added observation', run_id, 'src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2 - Discharge_Method_Hospital_Provider_Spell -> observation', @@row_count, CURRENT_DATETIME();

    /*
        --[src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2 - Admission_Method_Hospital_Provider_Spell] Transfer   
    */
    SET max_current_id = COALESCE((SELECT observation_id FROM `CY_IMOSPHERE_CDM_531.observation` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.observation`
    (
        observation_id,
        person_id,
        observation_concept_id,
        observation_source_value,
        observation_date,
        observation_datetime,
        observation_type_concept_id,
        value_as_concept_id,
        visit_occurrence_id,
        visit_detail_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as observation_id,
        o.person_id,
        o.observation_concept_id,
        o.observation_source_value,
        o.observation_date,
        o.observation_datetime,
        o.observation_type_concept_id,
        o.value_as_concept_id,
        o.visit_occurrence_id,
        o.visit_detail_id
    FROM
    (
        SELECT DISTINCT
            o.person_id person_id,
            8715 observation_concept_id, --Hospital Admission
            o.Admission_Method_Hospital_Provider_Spell observation_source_value,
            PARSE_DATE('%Y%m%d', o.Start_Date_Hospital_Provider_Spell) observation_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(o.Start_Date_Hospital_Provider_Spell ,'000000')) observation_datetime,
            32817 observation_type_concept_id, --EHR
            oc.concept_id value_as_concept_id,
            vl.id visit_occurrence_id,
            vdl.id visit_detail_id
        FROM `CY_IMOSPHERE_WORKSPACE.src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2` o
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on oc.source_value = o.Admission_Method_Hospital_Provider_Spell AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_APC_010415_to_300619_P1' AND oc.source_column =  'Admission_Method_Hospital_Provider_Spell'
        LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups` vdl ON vdl.source_value = CONCAT(CAST(o.Hospital_Provider_Spell_Number as STRING),'_', o.person_id, '_', o.Episode_Number)
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = CONCAT(CAST(o.Hospital_Provider_Spell_Number as STRING),'_', o.person_id) 
        WHERE (o.Start_Date_Hospital_Provider_Spell IS NOT NULL AND o.Start_Date_Hospital_Provider_Spell NOT LIKE '% %')
        AND o.person_id IS NOT NULL
    ) o
    LEFT JOIN `CY_IMOSPHERE_CDM_531.observation` oo ON 
        oo.person_id = o.person_id
        AND oo.observation_concept_id = o.observation_concept_id
        AND oo.observation_source_value = o.observation_source_value
        AND COALESCE(oo.observation_source_concept_id, -1) = -1
        AND oo.observation_datetime = o.observation_datetime
        AND oo.observation_type_concept_id = o.observation_type_concept_id
        AND COALESCE(oo.provider_id, -1) = -1
        AND COALESCE(oo.visit_occurrence_id, -1) = COALESCE(o.visit_occurrence_id, -1)
        AND COALESCE(oo.visit_detail_id, -1) = COALESCE(o.visit_detail_id, -1)
        AND COALESCE(oo.value_as_concept_id, -1) = COALESCE(o.value_as_concept_id, -1)
        AND COALESCE(CAST(oo.value_as_number AS STRING), '') = ''
        AND COALESCE(oo.qualifier_concept_id, -1) = -1
        AND COALESCE(oo.unit_concept_id, -1) =  -1
        AND COALESCE(oo.qualifier_source_value, '') = ''
        AND COALESCE(oo.unit_source_value, '') =  ''
    WHERE oo.observation_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added observation', run_id, 'src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2 - Admission_Method_Hospital_Provider_Spell -> observation', @@row_count, CURRENT_DATETIME();

    /*
        --[src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2 - Discharge_Destination_Hospital_Provider_Spell] Transfer   
    */
    SET max_current_id = COALESCE((SELECT observation_id FROM `CY_IMOSPHERE_CDM_531.observation` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.observation`
    (
        observation_id,
        person_id,
        observation_concept_id,
        observation_source_value,
        observation_date,
        observation_datetime,
        observation_type_concept_id,
        value_as_concept_id,
        visit_occurrence_id,
        visit_detail_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as observation_id,
        o.person_id,
        o.observation_concept_id,
        o.observation_source_value,
        o.observation_date,
        o.observation_datetime,
        o.observation_type_concept_id,
        o.value_as_concept_id,
        o.visit_occurrence_id,
        o.visit_detail_id
    FROM
    (
        SELECT DISTINCT
            o.person_id person_id,
            4240723 observation_concept_id, --Patient discharge
            o.Discharge_Destination_Hospital_Provider_Spell observation_source_value,
            PARSE_DATE('%Y%m%d', o.Discharge_Date_From_Hospital_Provider_Spell) observation_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(o.Discharge_Date_From_Hospital_Provider_Spell ,'000000')) observation_datetime,
            32817 observation_type_concept_id, --EHR
            CASE 
              WHEN Discharge_Destination_Hospital_Provider_Spell = '19' THEN 4140634 --Discharge to home
              WHEN Discharge_Destination_Hospital_Provider_Spell = '79' THEN 4194377 --Patient discharge, deceased, no autopsy
            END value_as_concept_id,
            vl.id visit_occurrence_id,
            vdl.id visit_detail_id
        FROM `CY_IMOSPHERE_WORKSPACE.src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2` o
        LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups` vdl ON vdl.source_value = CONCAT(CAST(o.Hospital_Provider_Spell_Number as STRING),'_', o.person_id, '_', o.Episode_Number)
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = CONCAT(CAST(o.Hospital_Provider_Spell_Number as STRING),'_', o.person_id) 
        WHERE (o.Discharge_Date_From_Hospital_Provider_Spell IS NOT NULL AND o.Discharge_Date_From_Hospital_Provider_Spell NOT LIKE '% %')
        AND o.person_id IS NOT NULL
        AND o.Discharge_Destination_Hospital_Provider_Spell IN ('19', '79')
    ) o
    LEFT JOIN `CY_IMOSPHERE_CDM_531.observation` oo ON 
        oo.person_id = o.person_id
        AND oo.observation_concept_id = o.observation_concept_id
        AND oo.observation_source_value = o.observation_source_value
        AND COALESCE(oo.observation_source_concept_id, -1) = -1
        AND oo.observation_datetime = o.observation_datetime
        AND oo.observation_type_concept_id = o.observation_type_concept_id
        AND COALESCE(oo.provider_id, -1) = -1
        AND COALESCE(oo.visit_occurrence_id, -1) = COALESCE(o.visit_occurrence_id, -1)
        AND COALESCE(oo.visit_detail_id, -1) = COALESCE(o.visit_detail_id, -1)
        AND COALESCE(oo.value_as_concept_id, -1) = COALESCE(o.value_as_concept_id, -1)
        AND COALESCE(CAST(oo.value_as_number AS STRING), '') = ''
        AND COALESCE(oo.qualifier_concept_id, -1) = -1
        AND COALESCE(oo.unit_concept_id, -1) =  -1
        AND COALESCE(oo.qualifier_source_value, '') = ''
        AND COALESCE(oo.unit_source_value, '') =  ''
    WHERE oo.observation_id IS NULL;
         
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added observation', run_id, 'src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2 - Discharge_Destination_Hospital_Provider_Spell -> observation', @@row_count, CURRENT_DATETIME();

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    ------------[SUS_BRI_AE_010415_to_300619]
    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

    /*
        --[SUS_BRI_AE_010415_to_300619 - Ethnic_Category] Transfer   
    */
    SET max_current_id = COALESCE((SELECT observation_id FROM `CY_IMOSPHERE_CDM_531.observation` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.observation`
    (
        observation_id,
        person_id,
        observation_concept_id,
        observation_source_value,
        observation_date,
        observation_datetime,
        observation_type_concept_id,
        value_as_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as observation_id,
        o.person_id,
        o.observation_concept_id,
        o.observation_source_value,
        o.observation_date,
        o.observation_datetime,
        o.observation_type_concept_id,
        o.value_as_concept_id,
        o.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            o.person_id person_id,
            44803968 observation_concept_id, --Ethnicity
            o.Ethnic_Category observation_source_value,
            PARSE_DATE("%Y%m%d", CONCAT(Date_of_Birth ,'01')) observation_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(Date_of_Birth ,'01000000')) observation_datetime,
            32817 observation_type_concept_id, --EHR
            oc.concept_id value_as_concept_id,
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` o
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on oc.source_value = o.Ethnic_Category AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_AE_010415_to_300619' AND oc.source_column = 'Ethnic_Category'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(o.AandE_Attendance_Number as STRING),'_', o.person_id) 
        WHERE (o.Date_of_Birth IS NOT NULL AND o.Date_of_Birth NOT LIKE '% %')
        AND o.person_id IS NOT NULL
    ) o
    LEFT JOIN `CY_IMOSPHERE_CDM_531.observation` oo ON 
        oo.person_id = o.person_id
        AND oo.observation_concept_id = o.observation_concept_id
        AND oo.observation_source_value = o.observation_source_value
        AND COALESCE(oo.observation_source_concept_id, -1) = -1
        AND oo.observation_datetime = o.observation_datetime
        AND oo.observation_type_concept_id = o.observation_type_concept_id
        AND COALESCE(oo.provider_id, -1) = -1
        AND COALESCE(oo.visit_occurrence_id, -1) = COALESCE(o.visit_occurrence_id, -1)
        AND COALESCE(oo.visit_detail_id, -1) = -1
        AND COALESCE(oo.value_as_concept_id, -1) = COALESCE(o.value_as_concept_id, -1)
        AND COALESCE(CAST(oo.value_as_number AS STRING), '') = ''
        AND COALESCE(oo.qualifier_concept_id, -1) = -1
        AND COALESCE(oo.unit_concept_id, -1) =  -1
        AND COALESCE(oo.qualifier_source_value, '') = ''
        AND COALESCE(oo.unit_source_value, '') =  ''
    WHERE oo.observation_id IS NULL;
         
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added observation', run_id, 'SUS_BRI_AE_010415_to_300619 - Ethnic_Category -> observation', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Source_of_Referral_For_AandE] Transfer   
    */
    SET max_current_id = COALESCE((SELECT observation_id FROM `CY_IMOSPHERE_CDM_531.observation` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.observation`
    (
        observation_id,
        person_id,
        observation_concept_id,
        observation_source_value,
        observation_date,
        observation_datetime,
        observation_type_concept_id,
        value_as_concept_id,
        visit_occurrence_id,
        qualifier_concept_id,
        qualifier_source_value
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as observation_id,
        o.person_id,
        o.observation_concept_id,
        o.observation_source_value,
        o.observation_date,
        o.observation_datetime,
        o.observation_type_concept_id,
        o.value_as_concept_id,
        o.visit_occurrence_id,
        o.qualifier_concept_id,
        o.qualifier_source_value
    FROM
    (
        SELECT DISTINCT
            o.person_id person_id,
            4287771 observation_concept_id, --Referral source
            o.Source_of_Referral_For_AandE observation_source_value,
            PARSE_DATE('%Y%m%d', o.Arrival_Date) observation_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(o.Arrival_Date ,'000000')) observation_datetime,
            32817 observation_type_concept_id, --EHR
            oc.concept_id value_as_concept_id,
            vl.id visit_occurrence_id,
            44804205 qualifier_concept_id,
            'Emergency Services' qualifier_source_value
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` o
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on oc.source_value = o.Source_of_Referral_For_AandE AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_AE_010415_to_300619' AND oc.source_column = 'Source_of_Referral_For_AandE'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(o.AandE_Attendance_Number as STRING),'_', o.person_id) 
        WHERE (o.Arrival_Date IS NOT NULL AND o.Arrival_Date NOT LIKE '% %')
        AND o.person_id IS NOT NULL
    ) o
    LEFT JOIN `CY_IMOSPHERE_CDM_531.observation` oo ON 
        oo.person_id = o.person_id
        AND oo.observation_concept_id = o.observation_concept_id
        AND oo.observation_source_value = o.observation_source_value
        AND COALESCE(oo.observation_source_concept_id, -1) = -1
        AND oo.observation_datetime = o.observation_datetime
        AND oo.observation_type_concept_id = o.observation_type_concept_id
        AND COALESCE(oo.provider_id, -1) = -1
        AND COALESCE(oo.visit_occurrence_id, -1) = COALESCE(o.visit_occurrence_id, -1)
        AND COALESCE(oo.visit_detail_id, -1) = -1
        AND COALESCE(oo.value_as_concept_id, -1) = COALESCE(o.value_as_concept_id, -1)
        AND COALESCE(CAST(oo.value_as_number AS STRING), '') = ''
        AND COALESCE(oo.qualifier_concept_id, -1) = COALESCE(o.qualifier_concept_id, -1)
        AND COALESCE(oo.unit_concept_id, -1) =  -1
        AND COALESCE(oo.qualifier_source_value, '') = COALESCE(o.qualifier_source_value, '')
        AND COALESCE(oo.unit_source_value, '') =  ''
    WHERE oo.observation_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added observation', run_id, 'SUS_BRI_AE_010415_to_300619 - Source_of_Referral_For_AandE -> observation', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_First] Transfer   
    */
    SET max_current_id = COALESCE((SELECT observation_id FROM `CY_IMOSPHERE_CDM_531.observation` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.observation`
    (
        observation_id,
        person_id,
        observation_concept_id,
        observation_source_value,
        observation_date,
        observation_datetime,
        observation_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as observation_id,
        o.person_id,
        o.observation_concept_id,
        o.observation_source_value,
        o.observation_date,
        o.observation_datetime,
        o.observation_type_concept_id,
        o.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            o.person_id person_id,
            oc.concept_id observation_concept_id,
            o.Accident_And_Emergency_Investigation_First observation_source_value,
            PARSE_DATE('%Y%m%d', o.Arrival_Date) observation_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(o.Arrival_Date ,'000000')) observation_datetime,
            32817 observation_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` o
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on TRIM(oc.source_value) = TRIM(o.Accident_And_Emergency_Investigation_First) AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_AE_010415_to_300619' AND oc.source_column = 'Accident_And_Emergency_Investigation_First'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(o.AandE_Attendance_Number as STRING),'_', o.person_id) 
        WHERE (o.Arrival_Date IS NOT NULL AND o.Arrival_Date NOT LIKE '% %')
        AND o.person_id IS NOT NULL
    ) o
    LEFT JOIN `CY_IMOSPHERE_CDM_531.observation` oo ON 
        oo.person_id = o.person_id
        AND oo.observation_concept_id = o.observation_concept_id
        AND oo.observation_source_value = o.observation_source_value
        AND COALESCE(oo.observation_source_concept_id, -1) = -1
        AND oo.observation_datetime = o.observation_datetime
        AND oo.observation_type_concept_id = o.observation_type_concept_id
        AND COALESCE(oo.provider_id, -1) = -1
        AND COALESCE(oo.visit_occurrence_id, -1) = COALESCE(o.visit_occurrence_id, -1)
        AND COALESCE(oo.visit_detail_id, -1) = -1
        AND COALESCE(oo.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(oo.value_as_number AS STRING), '') = ''
        AND COALESCE(oo.qualifier_concept_id, -1) = -1
        AND COALESCE(oo.unit_concept_id, -1) =  -1
        AND COALESCE(oo.qualifier_source_value, '') = ''
        AND COALESCE(oo.unit_source_value, '') =  ''
    WHERE oo.observation_id IS NULL;
         
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added observation', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_First -> observation', @@row_count, CURRENT_DATETIME();

    
    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Second] Transfer   
    */
    SET max_current_id = COALESCE((SELECT observation_id FROM `CY_IMOSPHERE_CDM_531.observation` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.observation`
    (
        observation_id,
        person_id,
        observation_concept_id,
        observation_source_value,
        observation_date,
        observation_datetime,
        observation_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as observation_id,
        o.person_id,
        o.observation_concept_id,
        o.observation_source_value,
        o.observation_date,
        o.observation_datetime,
        o.observation_type_concept_id,
        o.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            o.person_id person_id,
            oc.concept_id observation_concept_id,
            o.Accident_And_Emergency_Investigation_Second observation_source_value,
            PARSE_DATE('%Y%m%d', o.Arrival_Date) observation_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(o.Arrival_Date ,'000000')) observation_datetime,
            32817 observation_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` o
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on TRIM(oc.source_value) = TRIM(o.Accident_And_Emergency_Investigation_Second) AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_AE_010415_to_300619' AND oc.source_column = 'Accident_And_Emergency_Investigation_Second'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(o.AandE_Attendance_Number as STRING),'_', o.person_id) 
        WHERE (o.Arrival_Date IS NOT NULL AND o.Arrival_Date NOT LIKE '% %')
        AND o.person_id IS NOT NULL
    ) o
    LEFT JOIN `CY_IMOSPHERE_CDM_531.observation` oo ON 
        oo.person_id = o.person_id
        AND oo.observation_concept_id = o.observation_concept_id
        AND oo.observation_source_value = o.observation_source_value
        AND COALESCE(oo.observation_source_concept_id, -1) = -1
        AND oo.observation_datetime = o.observation_datetime
        AND oo.observation_type_concept_id = o.observation_type_concept_id
        AND COALESCE(oo.provider_id, -1) = -1
        AND COALESCE(oo.visit_occurrence_id, -1) = COALESCE(o.visit_occurrence_id, -1)
        AND COALESCE(oo.visit_detail_id, -1) = -1
        AND COALESCE(oo.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(oo.value_as_number AS STRING), '') = ''
        AND COALESCE(oo.qualifier_concept_id, -1) = -1
        AND COALESCE(oo.unit_concept_id, -1) =  -1
        AND COALESCE(oo.qualifier_source_value, '') = ''
        AND COALESCE(oo.unit_source_value, '') =  ''
    WHERE oo.observation_id IS NULL;
         
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added observation', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Second -> observation', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Third] Transfer   
    */
    SET max_current_id = COALESCE((SELECT observation_id FROM `CY_IMOSPHERE_CDM_531.observation` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.observation`
    (
        observation_id,
        person_id,
        observation_concept_id,
        observation_source_value,
        observation_date,
        observation_datetime,
        observation_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as observation_id,
        o.person_id,
        o.observation_concept_id,
        o.observation_source_value,
        o.observation_date,
        o.observation_datetime,
        o.observation_type_concept_id,
        o.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            o.person_id person_id,
            oc.concept_id observation_concept_id,
            o.Accident_And_Emergency_Investigation_Third observation_source_value,
            PARSE_DATE('%Y%m%d', o.Arrival_Date) observation_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(o.Arrival_Date ,'000000')) observation_datetime,
            32817 observation_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` o
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on TRIM(oc.source_value) = TRIM(o.Accident_And_Emergency_Investigation_Third) AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_AE_010415_to_300619' AND oc.source_column = 'Accident_And_Emergency_Investigation_Third'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(o.AandE_Attendance_Number as STRING),'_', o.person_id) 
        WHERE (o.Arrival_Date IS NOT NULL AND o.Arrival_Date NOT LIKE '% %')
        AND o.person_id IS NOT NULL
    ) o
    LEFT JOIN `CY_IMOSPHERE_CDM_531.observation` oo ON 
        oo.person_id = o.person_id
        AND oo.observation_concept_id = o.observation_concept_id
        AND oo.observation_source_value = o.observation_source_value
        AND COALESCE(oo.observation_source_concept_id, -1) = -1
        AND oo.observation_datetime = o.observation_datetime
        AND oo.observation_type_concept_id = o.observation_type_concept_id
        AND COALESCE(oo.provider_id, -1) = -1
        AND COALESCE(oo.visit_occurrence_id, -1) = COALESCE(o.visit_occurrence_id, -1)
        AND COALESCE(oo.visit_detail_id, -1) = -1
        AND COALESCE(oo.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(oo.value_as_number AS STRING), '') = ''
        AND COALESCE(oo.qualifier_concept_id, -1) = -1
        AND COALESCE(oo.unit_concept_id, -1) =  -1
        AND COALESCE(oo.qualifier_source_value, '') = ''
        AND COALESCE(oo.unit_source_value, '') =  ''
    WHERE oo.observation_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added observation', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Third -> observation', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Fourth] Transfer   
    */
    SET max_current_id = COALESCE((SELECT observation_id FROM `CY_IMOSPHERE_CDM_531.observation` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.observation`
    (
        observation_id,
        person_id,
        observation_concept_id,
        observation_source_value,
        observation_date,
        observation_datetime,
        observation_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as observation_id,
        o.person_id,
        o.observation_concept_id,
        o.observation_source_value,
        o.observation_date,
        o.observation_datetime,
        o.observation_type_concept_id,
        o.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            o.person_id person_id,
            oc.concept_id observation_concept_id,
            o.Accident_And_Emergency_Investigation_Fourth observation_source_value,
            PARSE_DATE('%Y%m%d', o.Arrival_Date) observation_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(o.Arrival_Date ,'000000')) observation_datetime,
            32817 observation_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` o
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on TRIM(oc.source_value) = TRIM(o.Accident_And_Emergency_Investigation_Fourth) AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_AE_010415_to_300619' AND oc.source_column = 'Accident_And_Emergency_Investigation_Fourth'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(o.AandE_Attendance_Number as STRING),'_', o.person_id) 
        WHERE (o.Arrival_Date IS NOT NULL AND o.Arrival_Date NOT LIKE '% %')
        AND o.person_id IS NOT NULL
    ) o
    LEFT JOIN `CY_IMOSPHERE_CDM_531.observation` oo ON 
        oo.person_id = o.person_id
        AND oo.observation_concept_id = o.observation_concept_id
        AND oo.observation_source_value = o.observation_source_value
        AND COALESCE(oo.observation_source_concept_id, -1) = -1
        AND oo.observation_datetime = o.observation_datetime
        AND oo.observation_type_concept_id = o.observation_type_concept_id
        AND COALESCE(oo.provider_id, -1) = -1
        AND COALESCE(oo.visit_occurrence_id, -1) = COALESCE(o.visit_occurrence_id, -1)
        AND COALESCE(oo.visit_detail_id, -1) = -1
        AND COALESCE(oo.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(oo.value_as_number AS STRING), '') = ''
        AND COALESCE(oo.qualifier_concept_id, -1) = -1
        AND COALESCE(oo.unit_concept_id, -1) =  -1
        AND COALESCE(oo.qualifier_source_value, '') = ''
        AND COALESCE(oo.unit_source_value, '') =  ''
    WHERE oo.observation_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added observation', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Fourth -> observation', @@row_count, CURRENT_DATETIME();
        
    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Fifth] Transfer   
    */
    SET max_current_id = COALESCE((SELECT observation_id FROM `CY_IMOSPHERE_CDM_531.observation` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.observation`
    (
        observation_id,
        person_id,
        observation_concept_id,
        observation_source_value,
        observation_date,
        observation_datetime,
        observation_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as observation_id,
        o.person_id,
        o.observation_concept_id,
        o.observation_source_value,
        o.observation_date,
        o.observation_datetime,
        o.observation_type_concept_id,
        o.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            o.person_id person_id,
            oc.concept_id observation_concept_id,
            o.Accident_And_Emergency_Investigation_Fifth observation_source_value,
            PARSE_DATE('%Y%m%d', o.Arrival_Date) observation_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(o.Arrival_Date ,'000000')) observation_datetime,
            32817 observation_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` o
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on TRIM(oc.source_value) = TRIM(o.Accident_And_Emergency_Investigation_Fifth) AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_AE_010415_to_300619' AND oc.source_column = 'Accident_And_Emergency_Investigation_Fifth'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(o.AandE_Attendance_Number as STRING),'_', o.person_id) 
        WHERE (o.Arrival_Date IS NOT NULL AND o.Arrival_Date NOT LIKE '% %')
        AND o.person_id IS NOT NULL
    ) o
    LEFT JOIN `CY_IMOSPHERE_CDM_531.observation` oo ON 
        oo.person_id = o.person_id
        AND oo.observation_concept_id = o.observation_concept_id
        AND oo.observation_source_value = o.observation_source_value
        AND COALESCE(oo.observation_source_concept_id, -1) = -1
        AND oo.observation_datetime = o.observation_datetime
        AND oo.observation_type_concept_id = o.observation_type_concept_id
        AND COALESCE(oo.provider_id, -1) = -1
        AND COALESCE(oo.visit_occurrence_id, -1) = COALESCE(o.visit_occurrence_id, -1)
        AND COALESCE(oo.visit_detail_id, -1) = -1
        AND COALESCE(oo.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(oo.value_as_number AS STRING), '') = ''
        AND COALESCE(oo.qualifier_concept_id, -1) = -1
        AND COALESCE(oo.unit_concept_id, -1) =  -1
        AND COALESCE(oo.qualifier_source_value, '') = ''
        AND COALESCE(oo.unit_source_value, '') =  ''
    WHERE oo.observation_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added observation', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Fifth -> observation', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Sixth] Transfer   
    */
    SET max_current_id = COALESCE((SELECT observation_id FROM `CY_IMOSPHERE_CDM_531.observation` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.observation`
    (
        observation_id,
        person_id,
        observation_concept_id,
        observation_source_value,
        observation_date,
        observation_datetime,
        observation_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as observation_id,
        o.person_id,
        o.observation_concept_id,
        o.observation_source_value,
        o.observation_date,
        o.observation_datetime,
        o.observation_type_concept_id,
        o.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            o.person_id person_id,
            oc.concept_id observation_concept_id,
            o.Accident_And_Emergency_Investigation_Sixth observation_source_value,
            PARSE_DATE('%Y%m%d', o.Arrival_Date) observation_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(o.Arrival_Date ,'000000')) observation_datetime,
            32817 observation_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` o
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on TRIM(oc.source_value) = TRIM(o.Accident_And_Emergency_Investigation_Sixth) AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_AE_010415_to_300619' AND oc.source_column = 'Accident_And_Emergency_Investigation_Sixth'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(o.AandE_Attendance_Number as STRING),'_', o.person_id) 
        WHERE (o.Arrival_Date IS NOT NULL AND o.Arrival_Date NOT LIKE '% %')
        AND o.person_id IS NOT NULL
    ) o
    LEFT JOIN `CY_IMOSPHERE_CDM_531.observation` oo ON 
        oo.person_id = o.person_id
        AND oo.observation_concept_id = o.observation_concept_id
        AND oo.observation_source_value = o.observation_source_value
        AND COALESCE(oo.observation_source_concept_id, -1) = -1
        AND oo.observation_datetime = o.observation_datetime
        AND oo.observation_type_concept_id = o.observation_type_concept_id
        AND COALESCE(oo.provider_id, -1) = -1
        AND COALESCE(oo.visit_occurrence_id, -1) = COALESCE(o.visit_occurrence_id, -1)
        AND COALESCE(oo.visit_detail_id, -1) = -1
        AND COALESCE(oo.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(oo.value_as_number AS STRING), '') = ''
        AND COALESCE(oo.qualifier_concept_id, -1) = -1
        AND COALESCE(oo.unit_concept_id, -1) =  -1
        AND COALESCE(oo.qualifier_source_value, '') = ''
        AND COALESCE(oo.unit_source_value, '') =  ''
    WHERE oo.observation_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added observation', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Sixth -> observation', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Seventh] Transfer   
    */
    SET max_current_id = COALESCE((SELECT observation_id FROM `CY_IMOSPHERE_CDM_531.observation` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.observation`
    (
        observation_id,
        person_id,
        observation_concept_id,
        observation_source_value,
        observation_date,
        observation_datetime,
        observation_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as observation_id,
        o.person_id,
        o.observation_concept_id,
        o.observation_source_value,
        o.observation_date,
        o.observation_datetime,
        o.observation_type_concept_id,
        o.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            o.person_id person_id,
            oc.concept_id observation_concept_id,
            o.Accident_And_Emergency_Investigation_Seventh observation_source_value,
            PARSE_DATE('%Y%m%d', o.Arrival_Date) observation_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(o.Arrival_Date ,'000000')) observation_datetime,
            32817 observation_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` o
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on TRIM(oc.source_value) = TRIM(o.Accident_And_Emergency_Investigation_Seventh) AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_AE_010415_to_300619' AND oc.source_column = 'Accident_And_Emergency_Investigation_Seventh'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(o.AandE_Attendance_Number as STRING),'_', o.person_id) 
        WHERE (o.Arrival_Date IS NOT NULL AND o.Arrival_Date NOT LIKE '% %')
        AND o.person_id IS NOT NULL
    ) o
    LEFT JOIN `CY_IMOSPHERE_CDM_531.observation` oo ON 
        oo.person_id = o.person_id
        AND oo.observation_concept_id = o.observation_concept_id
        AND oo.observation_source_value = o.observation_source_value
        AND COALESCE(oo.observation_source_concept_id, -1) = -1
        AND oo.observation_datetime = o.observation_datetime
        AND oo.observation_type_concept_id = o.observation_type_concept_id
        AND COALESCE(oo.provider_id, -1) = -1
        AND COALESCE(oo.visit_occurrence_id, -1) = COALESCE(o.visit_occurrence_id, -1)
        AND COALESCE(oo.visit_detail_id, -1) = -1
        AND COALESCE(oo.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(oo.value_as_number AS STRING), '') = ''
        AND COALESCE(oo.qualifier_concept_id, -1) = -1
        AND COALESCE(oo.unit_concept_id, -1) =  -1
        AND COALESCE(oo.qualifier_source_value, '') = ''
        AND COALESCE(oo.unit_source_value, '') =  ''
    WHERE oo.observation_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added observation', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Seventh -> observation', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Eighth] Transfer   
    */
    SET max_current_id = COALESCE((SELECT observation_id FROM `CY_IMOSPHERE_CDM_531.observation` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.observation`
    (
        observation_id,
        person_id,
        observation_concept_id,
        observation_source_value,
        observation_date,
        observation_datetime,
        observation_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as observation_id,
        o.person_id,
        o.observation_concept_id,
        o.observation_source_value,
        o.observation_date,
        o.observation_datetime,
        o.observation_type_concept_id,
        o.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            o.person_id person_id,
            oc.concept_id observation_concept_id,
            o.Accident_And_Emergency_Investigation_Eighth observation_source_value,
            PARSE_DATE('%Y%m%d', o.Arrival_Date) observation_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(o.Arrival_Date ,'000000')) observation_datetime,
            32817 observation_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` o
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on TRIM(oc.source_value) = TRIM(o.Accident_And_Emergency_Investigation_Eighth) AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_AE_010415_to_300619' AND oc.source_column = 'Accident_And_Emergency_Investigation_Eighth'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(o.AandE_Attendance_Number as STRING),'_', o.person_id) 
        WHERE (o.Arrival_Date IS NOT NULL AND o.Arrival_Date NOT LIKE '% %')
        AND o.person_id IS NOT NULL
    ) o
    LEFT JOIN `CY_IMOSPHERE_CDM_531.observation` oo ON 
        oo.person_id = o.person_id
        AND oo.observation_concept_id = o.observation_concept_id
        AND oo.observation_source_value = o.observation_source_value
        AND COALESCE(oo.observation_source_concept_id, -1) = -1
        AND oo.observation_datetime = o.observation_datetime
        AND oo.observation_type_concept_id = o.observation_type_concept_id
        AND COALESCE(oo.provider_id, -1) = -1
        AND COALESCE(oo.visit_occurrence_id, -1) = COALESCE(o.visit_occurrence_id, -1)
        AND COALESCE(oo.visit_detail_id, -1) = -1
        AND COALESCE(oo.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(oo.value_as_number AS STRING), '') = ''
        AND COALESCE(oo.qualifier_concept_id, -1) = -1
        AND COALESCE(oo.unit_concept_id, -1) =  -1
        AND COALESCE(oo.qualifier_source_value, '') = ''
        AND COALESCE(oo.unit_source_value, '') =  ''
    WHERE oo.observation_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added observation', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Eighth -> observation', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Ninth] Transfer   
    */
    SET max_current_id = COALESCE((SELECT observation_id FROM `CY_IMOSPHERE_CDM_531.observation` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.observation`
    (
        observation_id,
        person_id,
        observation_concept_id,
        observation_source_value,
        observation_date,
        observation_datetime,
        observation_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as observation_id,
        o.person_id,
        o.observation_concept_id,
        o.observation_source_value,
        o.observation_date,
        o.observation_datetime,
        o.observation_type_concept_id,
        o.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            o.person_id person_id,
            oc.concept_id observation_concept_id,
            o.Accident_And_Emergency_Investigation_Ninth observation_source_value,
            PARSE_DATE('%Y%m%d', o.Arrival_Date) observation_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(o.Arrival_Date ,'000000')) observation_datetime,
            32817 observation_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` o
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on TRIM(oc.source_value) = TRIM(o.Accident_And_Emergency_Investigation_Ninth) AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_AE_010415_to_300619' AND oc.source_column = 'Accident_And_Emergency_Investigation_Ninth'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(o.AandE_Attendance_Number as STRING),'_', o.person_id) 
        WHERE (o.Arrival_Date IS NOT NULL AND o.Arrival_Date NOT LIKE '% %')
        AND o.person_id IS NOT NULL
    ) o
    LEFT JOIN `CY_IMOSPHERE_CDM_531.observation` oo ON 
        oo.person_id = o.person_id
        AND oo.observation_concept_id = o.observation_concept_id
        AND oo.observation_source_value = o.observation_source_value
        AND COALESCE(oo.observation_source_concept_id, -1) = -1
        AND oo.observation_datetime = o.observation_datetime
        AND oo.observation_type_concept_id = o.observation_type_concept_id
        AND COALESCE(oo.provider_id, -1) = -1
        AND COALESCE(oo.visit_occurrence_id, -1) = COALESCE(o.visit_occurrence_id, -1)
        AND COALESCE(oo.visit_detail_id, -1) = -1
        AND COALESCE(oo.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(oo.value_as_number AS STRING), '') = ''
        AND COALESCE(oo.qualifier_concept_id, -1) = -1
        AND COALESCE(oo.unit_concept_id, -1) =  -1
        AND COALESCE(oo.qualifier_source_value, '') = ''
        AND COALESCE(oo.unit_source_value, '') =  ''
    WHERE oo.observation_id IS NULL;

     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added observation', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Ninth -> observation', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Tenth] Transfer   
    */
    SET max_current_id = COALESCE((SELECT observation_id FROM `CY_IMOSPHERE_CDM_531.observation` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.observation`
    (
        observation_id,
        person_id,
        observation_concept_id,
        observation_source_value,
        observation_date,
        observation_datetime,
        observation_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as observation_id,
        o.person_id,
        o.observation_concept_id,
        o.observation_source_value,
        o.observation_date,
        o.observation_datetime,
        o.observation_type_concept_id,
        o.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            o.person_id person_id,
            oc.concept_id observation_concept_id,
            o.Accident_And_Emergency_Investigation_Tenth observation_source_value,
            PARSE_DATE('%Y%m%d', o.Arrival_Date) observation_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(o.Arrival_Date ,'000000')) observation_datetime,
            32817 observation_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` o
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on TRIM(oc.source_value) = TRIM(o.Accident_And_Emergency_Investigation_Tenth) AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_AE_010415_to_300619' AND oc.source_column = 'Accident_And_Emergency_Investigation_Tenth'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(o.AandE_Attendance_Number as STRING),'_', o.person_id) 
        WHERE (o.Arrival_Date IS NOT NULL AND o.Arrival_Date NOT LIKE '% %')
        AND o.person_id IS NOT NULL
    ) o
    LEFT JOIN `CY_IMOSPHERE_CDM_531.observation` oo ON 
        oo.person_id = o.person_id
        AND oo.observation_concept_id = o.observation_concept_id
        AND oo.observation_source_value = o.observation_source_value
        AND COALESCE(oo.observation_source_concept_id, -1) = -1
        AND oo.observation_datetime = o.observation_datetime
        AND oo.observation_type_concept_id = o.observation_type_concept_id
        AND COALESCE(oo.provider_id, -1) = -1
        AND COALESCE(oo.visit_occurrence_id, -1) = COALESCE(o.visit_occurrence_id, -1)
        AND COALESCE(oo.visit_detail_id, -1) = -1
        AND COALESCE(oo.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(oo.value_as_number AS STRING), '') = ''
        AND COALESCE(oo.qualifier_concept_id, -1) = -1
        AND COALESCE(oo.unit_concept_id, -1) =  -1
        AND COALESCE(oo.qualifier_source_value, '') = ''
        AND COALESCE(oo.unit_source_value, '') =  ''
    WHERE oo.observation_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added observation', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Tenth -> observation', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Eleventh] Transfer   
    */
    SET max_current_id = COALESCE((SELECT observation_id FROM `CY_IMOSPHERE_CDM_531.observation` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.observation`
    (
        observation_id,
        person_id,
        observation_concept_id,
        observation_source_value,
        observation_date,
        observation_datetime,
        observation_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as observation_id,
        o.person_id,
        o.observation_concept_id,
        o.observation_source_value,
        o.observation_date,
        o.observation_datetime,
        o.observation_type_concept_id,
        o.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            o.person_id person_id,
            oc.concept_id observation_concept_id,
            o.Accident_And_Emergency_Investigation_Eleventh observation_source_value,
            PARSE_DATE('%Y%m%d', o.Arrival_Date) observation_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(o.Arrival_Date ,'000000')) observation_datetime,
            32817 observation_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` o
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on TRIM(oc.source_value) = TRIM(o.Accident_And_Emergency_Investigation_Eleventh) AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_AE_010415_to_300619' AND oc.source_column = 'Accident_And_Emergency_Investigation_Eleventh'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(o.AandE_Attendance_Number as STRING),'_', o.person_id) 
        WHERE (o.Arrival_Date IS NOT NULL AND o.Arrival_Date NOT LIKE '% %')
        AND o.person_id IS NOT NULL
    ) o
    LEFT JOIN `CY_IMOSPHERE_CDM_531.observation` oo ON 
        oo.person_id = o.person_id
        AND oo.observation_concept_id = o.observation_concept_id
        AND oo.observation_source_value = o.observation_source_value
        AND COALESCE(oo.observation_source_concept_id, -1) = -1
        AND oo.observation_datetime = o.observation_datetime
        AND oo.observation_type_concept_id = o.observation_type_concept_id
        AND COALESCE(oo.provider_id, -1) = -1
        AND COALESCE(oo.visit_occurrence_id, -1) = COALESCE(o.visit_occurrence_id, -1)
        AND COALESCE(oo.visit_detail_id, -1) = -1
        AND COALESCE(oo.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(oo.value_as_number AS STRING), '') = ''
        AND COALESCE(oo.qualifier_concept_id, -1) = -1
        AND COALESCE(oo.unit_concept_id, -1) =  -1
        AND COALESCE(oo.qualifier_source_value, '') = ''
        AND COALESCE(oo.unit_source_value, '') =  ''
    WHERE oo.observation_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added observation', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Eleventh -> observation', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Twelfth] Transfer   
    */
    SET max_current_id = COALESCE((SELECT observation_id FROM `CY_IMOSPHERE_CDM_531.observation` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.observation`
    (
        observation_id,
        person_id,
        observation_concept_id,
        observation_source_value,
        observation_date,
        observation_datetime,
        observation_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as observation_id,
        o.person_id,
        o.observation_concept_id,
        o.observation_source_value,
        o.observation_date,
        o.observation_datetime,
        o.observation_type_concept_id,
        o.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            o.person_id person_id,
            oc.concept_id observation_concept_id,
            o.Accident_And_Emergency_Investigation_Twelfth observation_source_value,
            PARSE_DATE('%Y%m%d', o.Arrival_Date) observation_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(o.Arrival_Date ,'000000')) observation_datetime,
            32817 observation_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` o
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on TRIM(oc.source_value) = TRIM(o.Accident_And_Emergency_Investigation_Twelfth) AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_AE_010415_to_300619' AND oc.source_column = 'Accident_And_Emergency_Investigation_Twelfth'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(o.AandE_Attendance_Number as STRING),'_', o.person_id) 
        WHERE (o.Arrival_Date IS NOT NULL AND o.Arrival_Date NOT LIKE '% %')
        AND o.person_id IS NOT NULL
    ) o
    LEFT JOIN `CY_IMOSPHERE_CDM_531.observation` oo ON 
        oo.person_id = o.person_id
        AND oo.observation_concept_id = o.observation_concept_id
        AND oo.observation_source_value = o.observation_source_value
        AND COALESCE(oo.observation_source_concept_id, -1) = -1
        AND oo.observation_datetime = o.observation_datetime
        AND oo.observation_type_concept_id = o.observation_type_concept_id
        AND COALESCE(oo.provider_id, -1) = -1
        AND COALESCE(oo.visit_occurrence_id, -1) = COALESCE(o.visit_occurrence_id, -1)
        AND COALESCE(oo.visit_detail_id, -1) = -1
        AND COALESCE(oo.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(oo.value_as_number AS STRING), '') = ''
        AND COALESCE(oo.qualifier_concept_id, -1) = -1
        AND COALESCE(oo.unit_concept_id, -1) =  -1
        AND COALESCE(oo.qualifier_source_value, '') = ''
        AND COALESCE(oo.unit_source_value, '') =  ''
    WHERE oo.observation_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added observation', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Twelfth -> observation', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_First] Transfer   
    */
    SET max_current_id = COALESCE((SELECT observation_id FROM `CY_IMOSPHERE_CDM_531.observation` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.observation`
    (
        observation_id,
        person_id,
        observation_concept_id,
        observation_source_value,
        observation_date,
        observation_datetime,
        observation_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as observation_id,
        o.person_id,
        o.observation_concept_id,
        o.observation_source_value,
        o.observation_date,
        o.observation_datetime,
        o.observation_type_concept_id,
        o.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            o.person_id person_id,
            oc.concept_id observation_concept_id,
            o.Accident_And_Emergency_Treatment_First observation_source_value,
            PARSE_DATE('%Y%m%d', o.Arrival_Date) observation_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(o.Arrival_Date ,'000000')) observation_datetime,
            32817 observation_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` o
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on TRIM(oc.source_value) = TRIM(o.Accident_And_Emergency_Treatment_First) AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_AE_010415_to_300619' AND oc.source_column = 'Accident_And_Emergency_Treatment_First'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(o.AandE_Attendance_Number as STRING),'_', o.person_id) 
        WHERE (o.Arrival_Date IS NOT NULL AND o.Arrival_Date NOT LIKE '% %')
        AND o.person_id IS NOT NULL
    ) o
    LEFT JOIN `CY_IMOSPHERE_CDM_531.observation` oo ON 
        oo.person_id = o.person_id
        AND oo.observation_concept_id = o.observation_concept_id
        AND oo.observation_source_value = o.observation_source_value
        AND COALESCE(oo.observation_source_concept_id, -1) = -1
        AND oo.observation_datetime = o.observation_datetime
        AND oo.observation_type_concept_id = o.observation_type_concept_id
        AND COALESCE(oo.provider_id, -1) = -1
        AND COALESCE(oo.visit_occurrence_id, -1) = COALESCE(o.visit_occurrence_id, -1)
        AND COALESCE(oo.visit_detail_id, -1) = -1
        AND COALESCE(oo.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(oo.value_as_number AS STRING), '') = ''
        AND COALESCE(oo.qualifier_concept_id, -1) = -1
        AND COALESCE(oo.unit_concept_id, -1) =  -1
        AND COALESCE(oo.qualifier_source_value, '') = ''
        AND COALESCE(oo.unit_source_value, '') =  ''
    WHERE oo.observation_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added observation', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_First -> observation', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Second] Transfer   
    */
    SET max_current_id = COALESCE((SELECT observation_id FROM `CY_IMOSPHERE_CDM_531.observation` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.observation`
    (
        observation_id,
        person_id,
        observation_concept_id,
        observation_source_value,
        observation_date,
        observation_datetime,
        observation_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as observation_id,
        o.person_id,
        o.observation_concept_id,
        o.observation_source_value,
        o.observation_date,
        o.observation_datetime,
        o.observation_type_concept_id,
        o.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            o.person_id person_id,
            oc.concept_id observation_concept_id,
            o.Accident_And_Emergency_Treatment_Second observation_source_value,
            PARSE_DATE('%Y%m%d', o.Arrival_Date) observation_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(o.Arrival_Date ,'000000')) observation_datetime,
            32817 observation_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` o
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on TRIM(oc.source_value) = TRIM(o.Accident_And_Emergency_Treatment_Second) AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_AE_010415_to_300619' AND oc.source_column = 'Accident_And_Emergency_Treatment_Second'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(o.AandE_Attendance_Number as STRING),'_', o.person_id) 
        WHERE (o.Arrival_Date IS NOT NULL AND o.Arrival_Date NOT LIKE '% %')
        AND o.person_id IS NOT NULL
    ) o
    LEFT JOIN `CY_IMOSPHERE_CDM_531.observation` oo ON 
        oo.person_id = o.person_id
        AND oo.observation_concept_id = o.observation_concept_id
        AND oo.observation_source_value = o.observation_source_value
        AND COALESCE(oo.observation_source_concept_id, -1) = -1
        AND oo.observation_datetime = o.observation_datetime
        AND oo.observation_type_concept_id = o.observation_type_concept_id
        AND COALESCE(oo.provider_id, -1) = -1
        AND COALESCE(oo.visit_occurrence_id, -1) = COALESCE(o.visit_occurrence_id, -1)
        AND COALESCE(oo.visit_detail_id, -1) = -1
        AND COALESCE(oo.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(oo.value_as_number AS STRING), '') = ''
        AND COALESCE(oo.qualifier_concept_id, -1) = -1
        AND COALESCE(oo.unit_concept_id, -1) =  -1
        AND COALESCE(oo.qualifier_source_value, '') = ''
        AND COALESCE(oo.unit_source_value, '') =  ''
    WHERE oo.observation_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added observation', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Second -> observation', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Third] Transfer   
    */
    SET max_current_id = COALESCE((SELECT observation_id FROM `CY_IMOSPHERE_CDM_531.observation` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.observation`
    (
        observation_id,
        person_id,
        observation_concept_id,
        observation_source_value,
        observation_date,
        observation_datetime,
        observation_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as observation_id,
        o.person_id,
        o.observation_concept_id,
        o.observation_source_value,
        o.observation_date,
        o.observation_datetime,
        o.observation_type_concept_id,
        o.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            o.person_id person_id,
            oc.concept_id observation_concept_id,
            o.Accident_And_Emergency_Treatment_Third observation_source_value,
            PARSE_DATE('%Y%m%d', o.Arrival_Date) observation_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(o.Arrival_Date ,'000000')) observation_datetime,
            32817 observation_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` o
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on TRIM(oc.source_value) = TRIM(o.Accident_And_Emergency_Treatment_Third) AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_AE_010415_to_300619' AND oc.source_column = 'Accident_And_Emergency_Treatment_Third'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(o.AandE_Attendance_Number as STRING),'_', o.person_id) 
        WHERE (o.Arrival_Date IS NOT NULL AND o.Arrival_Date NOT LIKE '% %')
        AND o.person_id IS NOT NULL
    ) o
    LEFT JOIN `CY_IMOSPHERE_CDM_531.observation` oo ON 
        oo.person_id = o.person_id
        AND oo.observation_concept_id = o.observation_concept_id
        AND oo.observation_source_value = o.observation_source_value
        AND COALESCE(oo.observation_source_concept_id, -1) = -1
        AND oo.observation_datetime = o.observation_datetime
        AND oo.observation_type_concept_id = o.observation_type_concept_id
        AND COALESCE(oo.provider_id, -1) = -1
        AND COALESCE(oo.visit_occurrence_id, -1) = COALESCE(o.visit_occurrence_id, -1)
        AND COALESCE(oo.visit_detail_id, -1) = -1
        AND COALESCE(oo.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(oo.value_as_number AS STRING), '') = ''
        AND COALESCE(oo.qualifier_concept_id, -1) = -1
        AND COALESCE(oo.unit_concept_id, -1) =  -1
        AND COALESCE(oo.qualifier_source_value, '') = ''
        AND COALESCE(oo.unit_source_value, '') =  ''
    WHERE oo.observation_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added observation', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Third -> observation', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Fourth] Transfer   
    */
    SET max_current_id = COALESCE((SELECT observation_id FROM `CY_IMOSPHERE_CDM_531.observation` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.observation`
    (
        observation_id,
        person_id,
        observation_concept_id,
        observation_source_value,
        observation_date,
        observation_datetime,
        observation_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as observation_id,
        o.person_id,
        o.observation_concept_id,
        o.observation_source_value,
        o.observation_date,
        o.observation_datetime,
        o.observation_type_concept_id,
        o.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            o.person_id person_id,
            oc.concept_id observation_concept_id,
            o.Accident_And_Emergency_Treatment_Fourth observation_source_value,
            PARSE_DATE('%Y%m%d', o.Arrival_Date) observation_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(o.Arrival_Date ,'000000')) observation_datetime,
            32817 observation_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` o
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on TRIM(oc.source_value) = TRIM(o.Accident_And_Emergency_Treatment_Fourth) AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_AE_010415_to_300619' AND oc.source_column = 'Accident_And_Emergency_Treatment_Fourth'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(o.AandE_Attendance_Number as STRING),'_', o.person_id) 
        WHERE (o.Arrival_Date IS NOT NULL AND o.Arrival_Date NOT LIKE '% %')
        AND o.person_id IS NOT NULL
    ) o
    LEFT JOIN `CY_IMOSPHERE_CDM_531.observation` oo ON 
        oo.person_id = o.person_id
        AND oo.observation_concept_id = o.observation_concept_id
        AND oo.observation_source_value = o.observation_source_value
        AND COALESCE(oo.observation_source_concept_id, -1) = -1
        AND oo.observation_datetime = o.observation_datetime
        AND oo.observation_type_concept_id = o.observation_type_concept_id
        AND COALESCE(oo.provider_id, -1) = -1
        AND COALESCE(oo.visit_occurrence_id, -1) = COALESCE(o.visit_occurrence_id, -1)
        AND COALESCE(oo.visit_detail_id, -1) = -1
        AND COALESCE(oo.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(oo.value_as_number AS STRING), '') = ''
        AND COALESCE(oo.qualifier_concept_id, -1) = -1
        AND COALESCE(oo.unit_concept_id, -1) =  -1
        AND COALESCE(oo.qualifier_source_value, '') = ''
        AND COALESCE(oo.unit_source_value, '') =  ''
    WHERE oo.observation_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added observation', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Fourth -> observation', @@row_count, CURRENT_DATETIME();
        
    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Fifth] Transfer   
    */
    SET max_current_id = COALESCE((SELECT observation_id FROM `CY_IMOSPHERE_CDM_531.observation` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.observation`
    (
        observation_id,
        person_id,
        observation_concept_id,
        observation_source_value,
        observation_date,
        observation_datetime,
        observation_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as observation_id,
        o.person_id,
        o.observation_concept_id,
        o.observation_source_value,
        o.observation_date,
        o.observation_datetime,
        o.observation_type_concept_id,
        o.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            o.person_id person_id,
            oc.concept_id observation_concept_id,
            o.Accident_And_Emergency_Treatment_Fifth observation_source_value,
            PARSE_DATE('%Y%m%d', o.Arrival_Date) observation_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(o.Arrival_Date ,'000000')) observation_datetime,
            32817 observation_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` o
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on TRIM(oc.source_value) = TRIM(o.Accident_And_Emergency_Treatment_Fifth) AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_AE_010415_to_300619' AND oc.source_column = 'Accident_And_Emergency_Treatment_Fifth'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(o.AandE_Attendance_Number as STRING),'_', o.person_id) 
        WHERE (o.Arrival_Date IS NOT NULL AND o.Arrival_Date NOT LIKE '% %')
        AND o.person_id IS NOT NULL
    ) o
    LEFT JOIN `CY_IMOSPHERE_CDM_531.observation` oo ON 
        oo.person_id = o.person_id
        AND oo.observation_concept_id = o.observation_concept_id
        AND oo.observation_source_value = o.observation_source_value
        AND COALESCE(oo.observation_source_concept_id, -1) = -1
        AND oo.observation_datetime = o.observation_datetime
        AND oo.observation_type_concept_id = o.observation_type_concept_id
        AND COALESCE(oo.provider_id, -1) = -1
        AND COALESCE(oo.visit_occurrence_id, -1) = COALESCE(o.visit_occurrence_id, -1)
        AND COALESCE(oo.visit_detail_id, -1) = -1
        AND COALESCE(oo.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(oo.value_as_number AS STRING), '') = ''
        AND COALESCE(oo.qualifier_concept_id, -1) = -1
        AND COALESCE(oo.unit_concept_id, -1) =  -1
        AND COALESCE(oo.qualifier_source_value, '') = ''
        AND COALESCE(oo.unit_source_value, '') =  ''
    WHERE oo.observation_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added observation', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Fifth -> observation', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Sixth] Transfer   
    */
    SET max_current_id = COALESCE((SELECT observation_id FROM `CY_IMOSPHERE_CDM_531.observation` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.observation`
    (
        observation_id,
        person_id,
        observation_concept_id,
        observation_source_value,
        observation_date,
        observation_datetime,
        observation_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as observation_id,
        o.person_id,
        o.observation_concept_id,
        o.observation_source_value,
        o.observation_date,
        o.observation_datetime,
        o.observation_type_concept_id,
        o.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            o.person_id person_id,
            oc.concept_id observation_concept_id,
            o.Accident_And_Emergency_Treatment_Sixth observation_source_value,
            PARSE_DATE('%Y%m%d', o.Arrival_Date) observation_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(o.Arrival_Date ,'000000')) observation_datetime,
            32817 observation_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` o
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on TRIM(oc.source_value) = TRIM(o.Accident_And_Emergency_Treatment_Sixth) AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_AE_010415_to_300619' AND oc.source_column = 'Accident_And_Emergency_Treatment_Sixth'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(o.AandE_Attendance_Number as STRING),'_', o.person_id) 
        WHERE (o.Arrival_Date IS NOT NULL AND o.Arrival_Date NOT LIKE '% %')
        AND o.person_id IS NOT NULL
    ) o
    LEFT JOIN `CY_IMOSPHERE_CDM_531.observation` oo ON 
        oo.person_id = o.person_id
        AND oo.observation_concept_id = o.observation_concept_id
        AND oo.observation_source_value = o.observation_source_value
        AND COALESCE(oo.observation_source_concept_id, -1) = -1
        AND oo.observation_datetime = o.observation_datetime
        AND oo.observation_type_concept_id = o.observation_type_concept_id
        AND COALESCE(oo.provider_id, -1) = -1
        AND COALESCE(oo.visit_occurrence_id, -1) = COALESCE(o.visit_occurrence_id, -1)
        AND COALESCE(oo.visit_detail_id, -1) = -1
        AND COALESCE(oo.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(oo.value_as_number AS STRING), '') = ''
        AND COALESCE(oo.qualifier_concept_id, -1) = -1
        AND COALESCE(oo.unit_concept_id, -1) =  -1
        AND COALESCE(oo.qualifier_source_value, '') = ''
        AND COALESCE(oo.unit_source_value, '') =  ''
    WHERE oo.observation_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added observation', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Sixth -> observation', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Seventh] Transfer   
    */
    SET max_current_id = COALESCE((SELECT observation_id FROM `CY_IMOSPHERE_CDM_531.observation` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.observation`
    (
        observation_id,
        person_id,
        observation_concept_id,
        observation_source_value,
        observation_date,
        observation_datetime,
        observation_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as observation_id,
        o.person_id,
        o.observation_concept_id,
        o.observation_source_value,
        o.observation_date,
        o.observation_datetime,
        o.observation_type_concept_id,
        o.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            o.person_id person_id,
            oc.concept_id observation_concept_id,
            o.Accident_And_Emergency_Treatment_Seventh observation_source_value,
            PARSE_DATE('%Y%m%d', o.Arrival_Date) observation_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(o.Arrival_Date ,'000000')) observation_datetime,
            32817 observation_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` o
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on TRIM(oc.source_value) = TRIM(o.Accident_And_Emergency_Treatment_Seventh) AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_AE_010415_to_300619' AND oc.source_column = 'Accident_And_Emergency_Treatment_Seventh'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(o.AandE_Attendance_Number as STRING),'_', o.person_id) 
        WHERE (o.Arrival_Date IS NOT NULL AND o.Arrival_Date NOT LIKE '% %')
        AND o.person_id IS NOT NULL
    ) o
    LEFT JOIN `CY_IMOSPHERE_CDM_531.observation` oo ON 
        oo.person_id = o.person_id
        AND oo.observation_concept_id = o.observation_concept_id
        AND oo.observation_source_value = o.observation_source_value
        AND COALESCE(oo.observation_source_concept_id, -1) = -1
        AND oo.observation_datetime = o.observation_datetime
        AND oo.observation_type_concept_id = o.observation_type_concept_id
        AND COALESCE(oo.provider_id, -1) = -1
        AND COALESCE(oo.visit_occurrence_id, -1) = COALESCE(o.visit_occurrence_id, -1)
        AND COALESCE(oo.visit_detail_id, -1) = -1
        AND COALESCE(oo.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(oo.value_as_number AS STRING), '') = ''
        AND COALESCE(oo.qualifier_concept_id, -1) = -1
        AND COALESCE(oo.unit_concept_id, -1) =  -1
        AND COALESCE(oo.qualifier_source_value, '') = ''
        AND COALESCE(oo.unit_source_value, '') =  ''
    WHERE oo.observation_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added observation', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Seventh -> observation', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Eighth] Transfer   
    */
    SET max_current_id = COALESCE((SELECT observation_id FROM `CY_IMOSPHERE_CDM_531.observation` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.observation`
    (
        observation_id,
        person_id,
        observation_concept_id,
        observation_source_value,
        observation_date,
        observation_datetime,
        observation_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as observation_id,
        o.person_id,
        o.observation_concept_id,
        o.observation_source_value,
        o.observation_date,
        o.observation_datetime,
        o.observation_type_concept_id,
        o.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            o.person_id person_id,
            oc.concept_id observation_concept_id,
            o.Accident_And_Emergency_Treatment_Eighth observation_source_value,
            PARSE_DATE('%Y%m%d', o.Arrival_Date) observation_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(o.Arrival_Date ,'000000')) observation_datetime,
            32817 observation_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` o
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on TRIM(oc.source_value) = TRIM(o.Accident_And_Emergency_Treatment_Eighth) AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_AE_010415_to_300619' AND oc.source_column = 'Accident_And_Emergency_Treatment_Eighth'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(o.AandE_Attendance_Number as STRING),'_', o.person_id) 
        WHERE (o.Arrival_Date IS NOT NULL AND o.Arrival_Date NOT LIKE '% %')
        AND o.person_id IS NOT NULL
    ) o
    LEFT JOIN `CY_IMOSPHERE_CDM_531.observation` oo ON 
        oo.person_id = o.person_id
        AND oo.observation_concept_id = o.observation_concept_id
        AND oo.observation_source_value = o.observation_source_value
        AND COALESCE(oo.observation_source_concept_id, -1) = -1
        AND oo.observation_datetime = o.observation_datetime
        AND oo.observation_type_concept_id = o.observation_type_concept_id
        AND COALESCE(oo.provider_id, -1) = -1
        AND COALESCE(oo.visit_occurrence_id, -1) = COALESCE(o.visit_occurrence_id, -1)
        AND COALESCE(oo.visit_detail_id, -1) = -1
        AND COALESCE(oo.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(oo.value_as_number AS STRING), '') = ''
        AND COALESCE(oo.qualifier_concept_id, -1) = -1
        AND COALESCE(oo.unit_concept_id, -1) =  -1
        AND COALESCE(oo.qualifier_source_value, '') = ''
        AND COALESCE(oo.unit_source_value, '') =  ''
    WHERE oo.observation_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added observation', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Eighth -> observation', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Ninth] Transfer   
    */
    SET max_current_id = COALESCE((SELECT observation_id FROM `CY_IMOSPHERE_CDM_531.observation` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.observation`
    (
        observation_id,
        person_id,
        observation_concept_id,
        observation_source_value,
        observation_date,
        observation_datetime,
        observation_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as observation_id,
        o.person_id,
        o.observation_concept_id,
        o.observation_source_value,
        o.observation_date,
        o.observation_datetime,
        o.observation_type_concept_id,
        o.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            o.person_id person_id,
            oc.concept_id observation_concept_id,
            o.Accident_And_Emergency_Treatment_Ninth observation_source_value,
            PARSE_DATE('%Y%m%d', o.Arrival_Date) observation_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(o.Arrival_Date ,'000000')) observation_datetime,
            32817 observation_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` o
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on TRIM(oc.source_value) = TRIM(o.Accident_And_Emergency_Treatment_Ninth) AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_AE_010415_to_300619' AND oc.source_column = 'Accident_And_Emergency_Treatment_Ninth'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(o.AandE_Attendance_Number as STRING),'_', o.person_id) 
        WHERE (o.Arrival_Date IS NOT NULL AND o.Arrival_Date NOT LIKE '% %')
        AND o.person_id IS NOT NULL
    ) o
    LEFT JOIN `CY_IMOSPHERE_CDM_531.observation` oo ON 
        oo.person_id = o.person_id
        AND oo.observation_concept_id = o.observation_concept_id
        AND oo.observation_source_value = o.observation_source_value
        AND COALESCE(oo.observation_source_concept_id, -1) = -1
        AND oo.observation_datetime = o.observation_datetime
        AND oo.observation_type_concept_id = o.observation_type_concept_id
        AND COALESCE(oo.provider_id, -1) = -1
        AND COALESCE(oo.visit_occurrence_id, -1) = COALESCE(o.visit_occurrence_id, -1)
        AND COALESCE(oo.visit_detail_id, -1) = -1
        AND COALESCE(oo.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(oo.value_as_number AS STRING), '') = ''
        AND COALESCE(oo.qualifier_concept_id, -1) = -1
        AND COALESCE(oo.unit_concept_id, -1) =  -1
        AND COALESCE(oo.qualifier_source_value, '') = ''
        AND COALESCE(oo.unit_source_value, '') =  ''
    WHERE oo.observation_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added observation', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Ninth -> observation', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Tenth] Transfer   
    */
    SET max_current_id = COALESCE((SELECT observation_id FROM `CY_IMOSPHERE_CDM_531.observation` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.observation`
    (
        observation_id,
        person_id,
        observation_concept_id,
        observation_source_value,
        observation_date,
        observation_datetime,
        observation_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as observation_id,
        o.person_id,
        o.observation_concept_id,
        o.observation_source_value,
        o.observation_date,
        o.observation_datetime,
        o.observation_type_concept_id,
        o.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            o.person_id person_id,
            oc.concept_id observation_concept_id,
            o.Accident_And_Emergency_Treatment_Tenth observation_source_value,
            PARSE_DATE('%Y%m%d', o.Arrival_Date) observation_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(o.Arrival_Date ,'000000')) observation_datetime,
            32817 observation_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` o
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on TRIM(oc.source_value) = TRIM(o.Accident_And_Emergency_Treatment_Tenth) AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_AE_010415_to_300619' AND oc.source_column = 'Accident_And_Emergency_Treatment_Tenth'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(o.AandE_Attendance_Number as STRING),'_', o.person_id) 
        WHERE (o.Arrival_Date IS NOT NULL AND o.Arrival_Date NOT LIKE '% %')
        AND o.person_id IS NOT NULL
    ) o
    LEFT JOIN `CY_IMOSPHERE_CDM_531.observation` oo ON 
        oo.person_id = o.person_id
        AND oo.observation_concept_id = o.observation_concept_id
        AND oo.observation_source_value = o.observation_source_value
        AND COALESCE(oo.observation_source_concept_id, -1) = -1
        AND oo.observation_datetime = o.observation_datetime
        AND oo.observation_type_concept_id = o.observation_type_concept_id
        AND COALESCE(oo.provider_id, -1) = -1
        AND COALESCE(oo.visit_occurrence_id, -1) = COALESCE(o.visit_occurrence_id, -1)
        AND COALESCE(oo.visit_detail_id, -1) = -1
        AND COALESCE(oo.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(oo.value_as_number AS STRING), '') = ''
        AND COALESCE(oo.qualifier_concept_id, -1) = -1
        AND COALESCE(oo.unit_concept_id, -1) =  -1
        AND COALESCE(oo.qualifier_source_value, '') = ''
        AND COALESCE(oo.unit_source_value, '') =  ''
    WHERE oo.observation_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added observation', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Tenth -> observation', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Eleventh] Transfer   
    */
    SET max_current_id = COALESCE((SELECT observation_id FROM `CY_IMOSPHERE_CDM_531.observation` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.observation`
    (
        observation_id,
        person_id,
        observation_concept_id,
        observation_source_value,
        observation_date,
        observation_datetime,
        observation_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as observation_id,
        o.person_id,
        o.observation_concept_id,
        o.observation_source_value,
        o.observation_date,
        o.observation_datetime,
        o.observation_type_concept_id,
        o.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            o.person_id person_id,
            oc.concept_id observation_concept_id,
            o.Accident_And_Emergency_Treatment_Eleventh observation_source_value,
            PARSE_DATE('%Y%m%d', o.Arrival_Date) observation_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(o.Arrival_Date ,'000000')) observation_datetime,
            32817 observation_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` o
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on TRIM(oc.source_value) = TRIM(o.Accident_And_Emergency_Treatment_Eleventh) AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_AE_010415_to_300619' AND oc.source_column = 'Accident_And_Emergency_Treatment_Eleventh'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(o.AandE_Attendance_Number as STRING),'_', o.person_id) 
        WHERE (o.Arrival_Date IS NOT NULL AND o.Arrival_Date NOT LIKE '% %')
        AND o.person_id IS NOT NULL
    ) o
    LEFT JOIN `CY_IMOSPHERE_CDM_531.observation` oo ON 
        oo.person_id = o.person_id
        AND oo.observation_concept_id = o.observation_concept_id
        AND oo.observation_source_value = o.observation_source_value
        AND COALESCE(oo.observation_source_concept_id, -1) = -1
        AND oo.observation_datetime = o.observation_datetime
        AND oo.observation_type_concept_id = o.observation_type_concept_id
        AND COALESCE(oo.provider_id, -1) = -1
        AND COALESCE(oo.visit_occurrence_id, -1) = COALESCE(o.visit_occurrence_id, -1)
        AND COALESCE(oo.visit_detail_id, -1) = -1
        AND COALESCE(oo.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(oo.value_as_number AS STRING), '') = ''
        AND COALESCE(oo.qualifier_concept_id, -1) = -1
        AND COALESCE(oo.unit_concept_id, -1) =  -1
        AND COALESCE(oo.qualifier_source_value, '') = ''
        AND COALESCE(oo.unit_source_value, '') =  ''
    WHERE oo.observation_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added observation', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Eleventh -> observation', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Twelfth] Transfer   
    */
    SET max_current_id = COALESCE((SELECT observation_id FROM `CY_IMOSPHERE_CDM_531.observation` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.observation`
    (
        observation_id,
        person_id,
        observation_concept_id,
        observation_source_value,
        observation_date,
        observation_datetime,
        observation_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as observation_id,
        o.person_id,
        o.observation_concept_id,
        o.observation_source_value,
        o.observation_date,
        o.observation_datetime,
        o.observation_type_concept_id,
        o.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            o.person_id person_id,
            oc.concept_id observation_concept_id,
            o.Accident_And_Emergency_Treatment_Twelfth observation_source_value,
            PARSE_DATE('%Y%m%d', o.Arrival_Date) observation_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(o.Arrival_Date ,'000000')) observation_datetime,
            32817 observation_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` o
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on TRIM(oc.source_value) = TRIM(o.Accident_And_Emergency_Treatment_Twelfth) AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_AE_010415_to_300619' AND oc.source_column = 'Accident_And_Emergency_Treatment_Twelfth'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(o.AandE_Attendance_Number as STRING),'_', o.person_id) 
        WHERE (o.Arrival_Date IS NOT NULL AND o.Arrival_Date NOT LIKE '% %')
        AND o.person_id IS NOT NULL
    ) o
    LEFT JOIN `CY_IMOSPHERE_CDM_531.observation` oo ON 
        oo.person_id = o.person_id
        AND oo.observation_concept_id = o.observation_concept_id
        AND oo.observation_source_value = o.observation_source_value
        AND COALESCE(oo.observation_source_concept_id, -1) = -1
        AND oo.observation_datetime = o.observation_datetime
        AND oo.observation_type_concept_id = o.observation_type_concept_id
        AND COALESCE(oo.provider_id, -1) = -1
        AND COALESCE(oo.visit_occurrence_id, -1) = COALESCE(o.visit_occurrence_id, -1)
        AND COALESCE(oo.visit_detail_id, -1) = -1
        AND COALESCE(oo.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(oo.value_as_number AS STRING), '') = ''
        AND COALESCE(oo.qualifier_concept_id, -1) = -1
        AND COALESCE(oo.unit_concept_id, -1) =  -1
        AND COALESCE(oo.qualifier_source_value, '') = ''
        AND COALESCE(oo.unit_source_value, '') =  ''
    WHERE oo.observation_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added observation', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Twelfth -> observation', @@row_count, CURRENT_DATETIME();
END;