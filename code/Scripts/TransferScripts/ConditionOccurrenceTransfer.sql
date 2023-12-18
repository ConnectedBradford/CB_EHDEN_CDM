CREATE OR REPLACE PROCEDURE CY_IMOSPHERE_WORKSPACE.TRANSFER_CONDITION_OCCURRENCE(run_id INT64)
OPTIONS (strict_mode=false)
BEGIN
    DECLARE max_current_id INT64;
    SET max_current_id = COALESCE((SELECT condition_occurrence_id FROM `CY_IMOSPHERE_CDM_531.condition_occurrence` ORDER BY 1 DESC LIMIT 1), 0);

    /*
        --[tbl_SRPatient] Transfer
    */

    INSERT INTO `CY_IMOSPHERE_CDM_531.condition_occurrence`
    (
        condition_occurrence_id,
        person_id,
        condition_concept_id,
        condition_source_value,
        condition_start_date,
        condition_start_datetime,
        condition_type_concept_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as condition_occurrence_id,
        c.person_id,
        c.condition_concept_id,
        c.condition_source_value,
        c.condition_start_date,
        c.condition_start_datetime,
        c.condition_type_concept_id
    FROM
    (
        SELECT DISTINCT
            c.person_id person_id,
            46270485 condition_concept_id, --Indeterminate sex.
            c.Gender condition_source_value, 
            CAST(PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(c.DateBirth ,'01000000')) AS DATE) condition_start_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(c.DateBirth ,'01000000')) condition_start_datetime,
            32817 condition_type_concept_id --EHR
        FROM `CY_IMOSPHERE_WORKSPACE.tbl_SRPatient` c
        WHERE c.DateBirth IS NOT NULL
        AND c.person_id IS NOT NULL
        AND c.Gender = 'I'
    ) c
    LEFT JOIN `CY_IMOSPHERE_CDM_531.condition_occurrence` oc ON 
        oc.person_id = c.person_id 
        AND oc.condition_concept_id = c.condition_concept_id
        AND oc.condition_source_value = c.condition_source_value
        AND COALESCE(oc.condition_source_concept_id, -1) = -1
        AND oc.condition_start_datetime = c.condition_start_datetime
        AND oc.condition_type_concept_id = c.condition_type_concept_id
        AND COALESCE(oc.provider_id, -1) = -1
        AND COALESCE(oc.condition_status_concept_id, -1) = -1
        AND COALESCE(oc.visit_occurrence_id, -1) = -1
        AND COALESCE(oc.visit_detail_id, -1) = -1
    WHERE oc.condition_occurrence_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id,  related_table, related_count, logged_at) 
    SELECT 'Added condition_occurrence', run_id,'tbl_SRPatient -> condition_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[tbl_SRCode] Transfer
    */

    SET max_current_id = COALESCE((SELECT condition_occurrence_id FROM `CY_IMOSPHERE_CDM_531.condition_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.condition_occurrence`
    (
        condition_occurrence_id,
        person_id,
        condition_concept_id,
        condition_source_value,
        condition_source_concept_id,
        condition_start_date,
        condition_start_datetime,
        condition_type_concept_id,
        provider_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as condition_occurrence_id,
        c.person_id,
        c.condition_concept_id,
        c.condition_source_value,
        c.condition_source_concept_id,
        c.condition_start_date,
        c.condition_start_datetime,
        c.condition_type_concept_id,
        c.provider_id
    FROM
    (
        SELECT DISTINCT
            c.person_id person_id,
            dc.concept_id condition_concept_id, 
            c.ctv3code condition_source_value, 
            sc.concept_id condition_source_concept_id,
            CAST(dateevent AS DATE) condition_start_date,
            dateevent condition_start_datetime,
            32817 condition_type_concept_id, --EHR
            p.provider_id provider_id
        FROM `CY_IMOSPHERE_WORKSPACE.tbl_SRCode` c
        JOIN `CY_LOOKUPS.tbl_CTV3ToSnomed_Map` sm ON sm.ctv3code = c.ctv3code
        JOIN `CY_CDM_VOCAB.concept` sc ON sc.concept_code = sm.SNOMEDCode
        JOIN `CY_CDM_VOCAB.concept_relationship` cr ON cr.concept_id_1 = sc.concept_id
        JOIN `CY_CDM_VOCAB.concept` dc ON dc.concept_id = cr.concept_id_2
        LEFT JOIN `CY_IMOSPHERE_CDM_531.provider` p ON p.provider_id = CAST(iddoneby AS INT64)
        WHERE sc.vocabulary_id = 'SNOMED'
        AND cr.relationship_id = 'Maps to'
        AND dc.standard_concept = 'S'
        AND dc.domain_id = 'Condition'
        AND c.dateevent IS NOT NULL
        AND c.person_id IS NOT NULL
    ) c
    LEFT JOIN `CY_IMOSPHERE_CDM_531.condition_occurrence` oc ON 
        oc.person_id = c.person_id
        AND oc.condition_concept_id = c.condition_concept_id
        AND oc.condition_source_value = c.condition_source_value
        AND COALESCE(oc.condition_source_concept_id, -1) = COALESCE(c.condition_source_concept_id, -1)
        AND oc.condition_start_datetime = c.condition_start_datetime
        AND oc.condition_type_concept_id = c.condition_type_concept_id
        AND COALESCE(oc.provider_id, -1) = COALESCE(c.provider_id, -1)
        AND COALESCE(oc.condition_status_concept_id, -1) = -1
        AND COALESCE(oc.visit_occurrence_id, -1) = -1
        AND COALESCE(oc.visit_detail_id, -1) = -1
    WHERE oc.condition_occurrence_id IS NULL;


    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id,  related_table, related_count, logged_at) 
    SELECT 'Added condition_occurrence', run_id, 'tbl_SRCode -> condition_occurrence', @@row_count, CURRENT_DATETIME();

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    ------------[SUS_BRI_APC_010415_to_300619_P1] AND [src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2]
    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

    /*
        --[Diagnosis_Primary_ICD] Transfer
    */

    SET max_current_id = COALESCE((SELECT condition_occurrence_id FROM `CY_IMOSPHERE_CDM_531.condition_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.condition_occurrence`
    (
        condition_occurrence_id,
        person_id,
        condition_concept_id,
        condition_source_value,
        condition_source_concept_id,
        condition_start_date,
        condition_start_datetime,
        condition_type_concept_id,
        condition_status_concept_id,
        visit_occurrence_id,
        visit_detail_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() condition_occurrence_id,
        c.person_id,
        c.condition_concept_id,
        c.condition_source_value,
        c.condition_source_concept_id,
        c.condition_start_date,
        c.condition_start_datetime,
        c.condition_type_concept_id,
        c.condition_status_concept_id,
        c.visit_occurrence_id,
        c.visit_detail_id
    FROM
    (
        SELECT DISTINCT
            person_id,
            dc.concept_id condition_concept_id,
            Diagnosis_Primary_ICD condition_source_value,
            sc.concept_id  condition_source_concept_id,
            PARSE_DATE('%Y%m%d', Start_Date_Consultant_Episode) condition_start_date,
            CAST(PARSE_DATE('%Y%m%d', Start_Date_Consultant_Episode) AS DATETIME) condition_start_datetime,
            32817 condition_type_concept_id, --EHR
            32902 condition_status_concept_id, --Primary diagnosis
            vl.id visit_occurrence_id,
            vdl.id visit_detail_id
        FROM
        (    
            SELECT DISTINCT
                person_id,
                Start_Date_Consultant_Episode,
                Diagnosis_Primary_ICD,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id) visit_occurrence_source_value,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id, '_', Episode_Number) visit_detail_source_value
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1`
            UNION DISTINCT
            SELECT DISTINCT
                person_id,
                Start_Date_Consultant_Episode,
                Diagnosis_Primary_ICD,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id) visit_occurrence_source_value,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id, '_', Episode_Number) visit_detail_source_value
            FROM `CY_IMOSPHERE_WORKSPACE.src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2`
        ) c
        LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups` vdl ON vdl.source_value = visit_detail_source_value
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = c.visit_occurrence_source_value
        JOIN `CY_CDM_VOCAB.concept` sc on REPLACE(sc.concept_code, '.', '') = c.Diagnosis_Primary_ICD
        JOIN `CY_CDM_VOCAB.concept_relationship` cr on cr.concept_id_1 = sc.concept_id
        JOIN `CY_CDM_VOCAB.concept` dc on dc.concept_id = cr.concept_id_2
        WHERE c.Start_Date_Consultant_Episode IS NOT NULL
        AND c.person_id IS NOT NULL
        AND cr.relationship_id = 'Maps to'
        AND dc.standard_concept = 'S'
        AND dc.domain_id = 'Condition'
        AND sc.vocabulary_id = 'ICD10'
        AND c.Diagnosis_Primary_ICD IS NOT NULL
        AND dc.vocabulary_id = 'SNOMED'
    ) c
    LEFT JOIN `CY_IMOSPHERE_CDM_531.condition_occurrence` oc ON 
        oc.person_id = c.person_id 
        AND oc.condition_concept_id = c.condition_concept_id
        AND oc.condition_source_value = c.condition_source_value
        AND COALESCE(oc.condition_source_concept_id, -1) = c.condition_source_concept_id
        AND oc.condition_start_datetime = c.condition_start_datetime
        AND oc.condition_type_concept_id = c.condition_type_concept_id
        AND COALESCE(oc.provider_id, -1) = -1
        AND COALESCE(oc.condition_status_concept_id, -1) = COALESCE(c.condition_status_concept_id, -1) 
        AND COALESCE(oc.visit_occurrence_id, -1) = c.visit_occurrence_id
        AND COALESCE(oc.visit_detail_id, -1) = c.visit_detail_id
    WHERE oc.condition_occurrence_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id,  related_table, related_count, logged_at) 
    SELECT 'Added condition_occurrence', run_id, 'src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2, SUS_BRI_APC_010415_to_300619_P1  - Diagnosis_Primary_ICD -> condition_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[Diagnosis_1st_Secondary_ICD] Transfer
    */

    SET max_current_id = COALESCE((SELECT condition_occurrence_id FROM `CY_IMOSPHERE_CDM_531.condition_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.condition_occurrence`
    (
        condition_occurrence_id,
        person_id,
        condition_concept_id,
        condition_source_value,
        condition_source_concept_id,
        condition_start_date,
        condition_start_datetime,
        condition_type_concept_id,
        condition_status_concept_id,
        visit_occurrence_id,
        visit_detail_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() condition_occurrence_id,
        c.person_id,
        c.condition_concept_id,
        c.condition_source_value,
        c.condition_source_concept_id,
        c.condition_start_date,
        c.condition_start_datetime,
        c.condition_type_concept_id,
        c.condition_status_concept_id,
        c.visit_occurrence_id,
        c.visit_detail_id
    FROM
    (
        SELECT DISTINCT
            person_id,
            dc.concept_id condition_concept_id,
            Diagnosis_1st_Secondary_ICD condition_source_value,
            sc.concept_id  condition_source_concept_id,
            PARSE_DATE('%Y%m%d', Start_Date_Consultant_Episode) condition_start_date,
            CAST(PARSE_DATE('%Y%m%d', Start_Date_Consultant_Episode) AS DATETIME) condition_start_datetime,
            32817 condition_type_concept_id, --EHR
            32908 condition_status_concept_id, --Secondary diagnosis
            vl.id visit_occurrence_id,
            vdl.id visit_detail_id
        FROM
        (    
            SELECT DISTINCT
                person_id,
                Start_Date_Consultant_Episode,
                Diagnosis_1st_Secondary_ICD,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id) visit_occurrence_source_value,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id, '_', Episode_Number) visit_detail_source_value
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1`
            UNION DISTINCT
            SELECT DISTINCT
                person_id,
                Start_Date_Consultant_Episode,
                Diagnosis_1st_Secondary_ICD,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id) visit_occurrence_source_value,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id, '_', Episode_Number) visit_detail_source_value
            FROM `CY_IMOSPHERE_WORKSPACE.src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2`
        ) c
        LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups` vdl ON vdl.source_value = visit_detail_source_value
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = c.visit_occurrence_source_value
        JOIN `CY_CDM_VOCAB.concept` sc on REPLACE(sc.concept_code, '.', '') = c.Diagnosis_1st_Secondary_ICD
        JOIN `CY_CDM_VOCAB.concept_relationship` cr on cr.concept_id_1 = sc.concept_id
        JOIN `CY_CDM_VOCAB.concept` dc on dc.concept_id = cr.concept_id_2
        WHERE c.Start_Date_Consultant_Episode IS NOT NULL
        AND c.person_id IS NOT NULL
        AND cr.relationship_id = 'Maps to'
        AND dc.standard_concept = 'S'
        AND dc.domain_id = 'Condition'
        AND sc.vocabulary_id = 'ICD10'
        AND c.Diagnosis_1st_Secondary_ICD IS NOT NULL
        AND dc.vocabulary_id = 'SNOMED'
    ) c
    LEFT JOIN `CY_IMOSPHERE_CDM_531.condition_occurrence` oc ON 
        oc.person_id = c.person_id 
        AND oc.condition_concept_id = c.condition_concept_id
        AND oc.condition_source_value = c.condition_source_value
        AND COALESCE(oc.condition_source_concept_id, -1) = c.condition_source_concept_id
        AND oc.condition_start_datetime = c.condition_start_datetime
        AND oc.condition_type_concept_id = c.condition_type_concept_id
        AND COALESCE(oc.provider_id, -1) = -1
        AND COALESCE(oc.condition_status_concept_id, -1) = COALESCE(c.condition_status_concept_id, -1)
        AND COALESCE(oc.visit_occurrence_id, -1) = COALESCE(c.visit_occurrence_id, -1)
        AND COALESCE(oc.visit_occurrence_id, -1) = c.visit_occurrence_id
        AND COALESCE(oc.visit_detail_id, -1) = c.visit_detail_id
    WHERE oc.condition_occurrence_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id,  related_table, related_count, logged_at) 
    SELECT 'Added condition_occurrence', run_id, 'src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2, SUS_BRI_APC_010415_to_300619_P1  - Diagnosis_1st_Secondary_ICD -> condition_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[Diagnosis_2nd_Secondary_ICD] Transfer
    */

    SET max_current_id = COALESCE((SELECT condition_occurrence_id FROM `CY_IMOSPHERE_CDM_531.condition_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.condition_occurrence`
    (
        condition_occurrence_id,
        person_id,
        condition_concept_id,
        condition_source_value,
        condition_source_concept_id,
        condition_start_date,
        condition_start_datetime,
        condition_type_concept_id,
        condition_status_concept_id,
        visit_occurrence_id,
        visit_detail_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() condition_occurrence_id,
        c.person_id,
        c.condition_concept_id,
        c.condition_source_value,
        c.condition_source_concept_id,
        c.condition_start_date,
        c.condition_start_datetime,
        c.condition_type_concept_id,
        c.condition_status_concept_id,
        c.visit_occurrence_id,
        c.visit_detail_id
    FROM
    (
        SELECT DISTINCT
            person_id,
            dc.concept_id condition_concept_id,
            Diagnosis_2nd_Secondary_ICD condition_source_value,
            sc.concept_id  condition_source_concept_id,
            PARSE_DATE('%Y%m%d', Start_Date_Consultant_Episode) condition_start_date,
            CAST(PARSE_DATE('%Y%m%d', Start_Date_Consultant_Episode) AS DATETIME) condition_start_datetime,
            32817 condition_type_concept_id, --EHR
            32908 condition_status_concept_id, --Secondary diagnosis
            vl.id visit_occurrence_id,
            vdl.id visit_detail_id
        FROM
        (    
            SELECT DISTINCT
                person_id,
                Start_Date_Consultant_Episode,
                Diagnosis_2nd_Secondary_ICD,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id) visit_occurrence_source_value,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id, '_', Episode_Number) visit_detail_source_value
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1`
            UNION DISTINCT
            SELECT DISTINCT
                person_id,
                Start_Date_Consultant_Episode,
                Diagnosis_2nd_Secondary_ICD,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id) visit_occurrence_source_value,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id, '_', Episode_Number) visit_detail_source_value
            FROM `CY_IMOSPHERE_WORKSPACE.src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2`
        ) c
        LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups` vdl ON vdl.source_value = visit_detail_source_value
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = c.visit_occurrence_source_value
        JOIN `CY_CDM_VOCAB.concept` sc on REPLACE(sc.concept_code, '.', '') = c.Diagnosis_2nd_Secondary_ICD
        JOIN `CY_CDM_VOCAB.concept_relationship` cr on cr.concept_id_1 = sc.concept_id
        JOIN `CY_CDM_VOCAB.concept` dc on dc.concept_id = cr.concept_id_2
        WHERE c.Start_Date_Consultant_Episode IS NOT NULL
        AND c.person_id IS NOT NULL
        AND cr.relationship_id = 'Maps to'
        AND dc.standard_concept = 'S'
        AND dc.domain_id = 'Condition'
        AND sc.vocabulary_id = 'ICD10'
        AND c.Diagnosis_2nd_Secondary_ICD IS NOT NULL
        AND dc.vocabulary_id = 'SNOMED'
    ) c
    LEFT JOIN `CY_IMOSPHERE_CDM_531.condition_occurrence` oc ON 
        oc.person_id = c.person_id 
        AND oc.condition_concept_id = c.condition_concept_id
        AND oc.condition_source_value = c.condition_source_value
        AND COALESCE(oc.condition_source_concept_id, -1) = c.condition_source_concept_id
        AND oc.condition_start_datetime = c.condition_start_datetime
        AND oc.condition_type_concept_id = c.condition_type_concept_id
        AND COALESCE(oc.provider_id, -1) = -1
        AND COALESCE(oc.condition_status_concept_id, -1) = COALESCE(c.condition_status_concept_id, -1)
        AND COALESCE(oc.visit_occurrence_id, -1) = COALESCE(c.visit_occurrence_id, -1)
        AND COALESCE(oc.visit_occurrence_id, -1) = c.visit_occurrence_id
        AND COALESCE(oc.visit_detail_id, -1) = c.visit_detail_id
    WHERE oc.condition_occurrence_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id,  related_table, related_count, logged_at) 
    SELECT 'Added condition_occurrence', run_id, 'src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2, SUS_BRI_APC_010415_to_300619_P1  - Diagnosis_2nd_Secondary_ICD -> condition_occurrence', @@row_count, CURRENT_DATETIME();
     
    /*
        --[Diagnosis_3rd_Secondary_ICD] Transfer
    */

    SET max_current_id = COALESCE((SELECT condition_occurrence_id FROM `CY_IMOSPHERE_CDM_531.condition_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.condition_occurrence`
    (
        condition_occurrence_id,
        person_id,
        condition_concept_id,
        condition_source_value,
        condition_source_concept_id,
        condition_start_date,
        condition_start_datetime,
        condition_type_concept_id,
        condition_status_concept_id,
        visit_occurrence_id,
        visit_detail_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() condition_occurrence_id,
        c.person_id,
        c.condition_concept_id,
        c.condition_source_value,
        c.condition_source_concept_id,
        c.condition_start_date,
        c.condition_start_datetime,
        c.condition_type_concept_id,
        c.condition_status_concept_id,
        c.visit_occurrence_id,
        c.visit_detail_id
    FROM
    (
        SELECT DISTINCT
            person_id,
            dc.concept_id condition_concept_id,
            Diagnosis_3rd_Secondary_ICD condition_source_value,
            sc.concept_id  condition_source_concept_id,
            PARSE_DATE('%Y%m%d', Start_Date_Consultant_Episode) condition_start_date,
            CAST(PARSE_DATE('%Y%m%d', Start_Date_Consultant_Episode) AS DATETIME) condition_start_datetime,
            32817 condition_type_concept_id, --EHR
            32908 condition_status_concept_id, --Secondary diagnosis
            vl.id visit_occurrence_id,
            vdl.id visit_detail_id
        FROM
        (    
            SELECT DISTINCT
                person_id,
                Start_Date_Consultant_Episode,
                Diagnosis_3rd_Secondary_ICD,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id) visit_occurrence_source_value,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id, '_', Episode_Number) visit_detail_source_value
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1`
            UNION DISTINCT
            SELECT DISTINCT
                person_id,
                Start_Date_Consultant_Episode,
                Diagnosis_3rd_Secondary_ICD,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id) visit_occurrence_source_value,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id, '_', Episode_Number) visit_detail_source_value
            FROM `CY_IMOSPHERE_WORKSPACE.src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2`
        ) c
        LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups` vdl ON vdl.source_value = visit_detail_source_value
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = c.visit_occurrence_source_value
        JOIN `CY_CDM_VOCAB.concept` sc on REPLACE(sc.concept_code, '.', '') = c.Diagnosis_3rd_Secondary_ICD
        JOIN `CY_CDM_VOCAB.concept_relationship` cr on cr.concept_id_1 = sc.concept_id
        JOIN `CY_CDM_VOCAB.concept` dc on dc.concept_id = cr.concept_id_2
        WHERE c.Start_Date_Consultant_Episode IS NOT NULL
        AND c.person_id IS NOT NULL
        AND cr.relationship_id = 'Maps to'
        AND dc.standard_concept = 'S'
        AND dc.domain_id = 'Condition'
        AND sc.vocabulary_id = 'ICD10'
        AND c.Diagnosis_3rd_Secondary_ICD IS NOT NULL
        AND dc.vocabulary_id = 'SNOMED'
    ) c
    LEFT JOIN `CY_IMOSPHERE_CDM_531.condition_occurrence` oc ON 
        oc.person_id = c.person_id 
        AND oc.condition_concept_id = c.condition_concept_id
        AND oc.condition_source_value = c.condition_source_value
        AND COALESCE(oc.condition_source_concept_id, -1) = c.condition_source_concept_id
        AND oc.condition_start_datetime = c.condition_start_datetime
        AND oc.condition_type_concept_id = c.condition_type_concept_id
        AND COALESCE(oc.provider_id, -1) = -1
        AND COALESCE(oc.condition_status_concept_id, -1) = COALESCE(c.condition_status_concept_id, -1)
        AND COALESCE(oc.visit_occurrence_id, -1) = COALESCE(c.visit_occurrence_id, -1)
        AND COALESCE(oc.visit_occurrence_id, -1) = c.visit_occurrence_id
        AND COALESCE(oc.visit_detail_id, -1) = c.visit_detail_id
    WHERE oc.condition_occurrence_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id,  related_table, related_count, logged_at) 
    SELECT 'Added condition_occurrence', run_id, 'src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2, SUS_BRI_APC_010415_to_300619_P1  - Diagnosis_3rd_Secondary_ICD -> condition_occurrence', @@row_count, CURRENT_DATETIME();
     
    /*
        --[Diagnosis_4th_Secondary_ICD] Transfer
    */
    SET max_current_id = COALESCE((SELECT condition_occurrence_id FROM `CY_IMOSPHERE_CDM_531.condition_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.condition_occurrence`
    (
        condition_occurrence_id,
        person_id,
        condition_concept_id,
        condition_source_value,
        condition_source_concept_id,
        condition_start_date,
        condition_start_datetime,
        condition_type_concept_id,
        condition_status_concept_id,
        visit_occurrence_id,
        visit_detail_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() condition_occurrence_id,
        c.person_id,
        c.condition_concept_id,
        c.condition_source_value,
        c.condition_source_concept_id,
        c.condition_start_date,
        c.condition_start_datetime,
        c.condition_type_concept_id,
        c.condition_status_concept_id,
        c.visit_occurrence_id,
        c.visit_detail_id
    FROM
    (
        SELECT DISTINCT
            person_id,
            dc.concept_id condition_concept_id,
            Diagnosis_4th_Secondary_ICD condition_source_value,
            sc.concept_id  condition_source_concept_id,
            PARSE_DATE('%Y%m%d', Start_Date_Consultant_Episode) condition_start_date,
            CAST(PARSE_DATE('%Y%m%d', Start_Date_Consultant_Episode) AS DATETIME) condition_start_datetime,
            32817 condition_type_concept_id, --EHR
            32908 condition_status_concept_id, --Secondary diagnosis
            vl.id visit_occurrence_id,
            vdl.id visit_detail_id
        FROM
        (    
            SELECT DISTINCT
                person_id,
                Start_Date_Consultant_Episode,
                Diagnosis_4th_Secondary_ICD,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id) visit_occurrence_source_value,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id, '_', Episode_Number) visit_detail_source_value
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1`
            UNION DISTINCT
            SELECT DISTINCT
                person_id,
                Start_Date_Consultant_Episode,
                Diagnosis_4th_Secondary_ICD,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id) visit_occurrence_source_value,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id, '_', Episode_Number) visit_detail_source_value
            FROM `CY_IMOSPHERE_WORKSPACE.src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2`
        ) c
        LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups` vdl ON vdl.source_value = visit_detail_source_value
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = c.visit_occurrence_source_value
        JOIN `CY_CDM_VOCAB.concept` sc on REPLACE(sc.concept_code, '.', '') = c.Diagnosis_4th_Secondary_ICD
        JOIN `CY_CDM_VOCAB.concept_relationship` cr on cr.concept_id_1 = sc.concept_id
        JOIN `CY_CDM_VOCAB.concept` dc on dc.concept_id = cr.concept_id_2
        WHERE c.Start_Date_Consultant_Episode IS NOT NULL
        AND c.person_id IS NOT NULL
        AND cr.relationship_id = 'Maps to'
        AND dc.standard_concept = 'S'
        AND dc.domain_id = 'Condition'
        AND sc.vocabulary_id = 'ICD10'
        AND c.Diagnosis_4th_Secondary_ICD IS NOT NULL
        AND dc.vocabulary_id = 'SNOMED'
    ) c
    LEFT JOIN `CY_IMOSPHERE_CDM_531.condition_occurrence` oc ON 
        oc.person_id = c.person_id 
        AND oc.condition_concept_id = c.condition_concept_id
        AND oc.condition_source_value = c.condition_source_value
        AND COALESCE(oc.condition_source_concept_id, -1) = c.condition_source_concept_id
        AND oc.condition_start_datetime = c.condition_start_datetime
        AND oc.condition_type_concept_id = c.condition_type_concept_id
        AND COALESCE(oc.provider_id, -1) = -1
        AND COALESCE(oc.condition_status_concept_id, -1) = COALESCE(c.condition_status_concept_id, -1)
        AND COALESCE(oc.visit_occurrence_id, -1) = COALESCE(c.visit_occurrence_id, -1)
        AND COALESCE(oc.visit_occurrence_id, -1) = c.visit_occurrence_id
        AND COALESCE(oc.visit_detail_id, -1) = c.visit_detail_id
    WHERE oc.condition_occurrence_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id,  related_table, related_count, logged_at) 
    SELECT 'Added condition_occurrence', run_id, 'src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2, SUS_BRI_APC_010415_to_300619_P1  - Diagnosis_4th_Secondary_ICD -> condition_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[Diagnosis_5th_Secondary_ICD] Transfer
    */
    SET max_current_id = COALESCE((SELECT condition_occurrence_id FROM `CY_IMOSPHERE_CDM_531.condition_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.condition_occurrence`
    (
        condition_occurrence_id,
        person_id,
        condition_concept_id,
        condition_source_value,
        condition_source_concept_id,
        condition_start_date,
        condition_start_datetime,
        condition_type_concept_id,
        condition_status_concept_id,
        visit_occurrence_id,
        visit_detail_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() condition_occurrence_id,
        c.person_id,
        c.condition_concept_id,
        c.condition_source_value,
        c.condition_source_concept_id,
        c.condition_start_date,
        c.condition_start_datetime,
        c.condition_type_concept_id,
        c.condition_status_concept_id,
        c.visit_occurrence_id,
        c.visit_detail_id
    FROM
    (
        SELECT DISTINCT
            person_id,
            dc.concept_id condition_concept_id,
            Diagnosis_5th_Secondary_ICD condition_source_value,
            sc.concept_id  condition_source_concept_id,
            PARSE_DATE('%Y%m%d', Start_Date_Consultant_Episode) condition_start_date,
            CAST(PARSE_DATE('%Y%m%d', Start_Date_Consultant_Episode) AS DATETIME) condition_start_datetime,
            32817 condition_type_concept_id, --EHR
            32908 condition_status_concept_id, --Secondary diagnosis
            vl.id visit_occurrence_id,
            vdl.id visit_detail_id
        FROM
        (    
            SELECT DISTINCT
                person_id,
                Start_Date_Consultant_Episode,
                Diagnosis_5th_Secondary_ICD,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id) visit_occurrence_source_value,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id, '_', Episode_Number) visit_detail_source_value
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1`
            UNION DISTINCT
            SELECT DISTINCT
                person_id,
                Start_Date_Consultant_Episode,
                Diagnosis_5th_Secondary_ICD,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id) visit_occurrence_source_value,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id, '_', Episode_Number) visit_detail_source_value
            FROM `CY_IMOSPHERE_WORKSPACE.src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2`
        ) c
        LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups` vdl ON vdl.source_value = visit_detail_source_value
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = c.visit_occurrence_source_value
        JOIN `CY_CDM_VOCAB.concept` sc on REPLACE(sc.concept_code, '.', '') = c.Diagnosis_5th_Secondary_ICD
        JOIN `CY_CDM_VOCAB.concept_relationship` cr on cr.concept_id_1 = sc.concept_id
        JOIN `CY_CDM_VOCAB.concept` dc on dc.concept_id = cr.concept_id_2
        WHERE c.Start_Date_Consultant_Episode IS NOT NULL
        AND c.person_id IS NOT NULL
        AND cr.relationship_id = 'Maps to'
        AND dc.standard_concept = 'S'
        AND dc.domain_id = 'Condition'
        AND sc.vocabulary_id = 'ICD10'
        AND c.Diagnosis_5th_Secondary_ICD IS NOT NULL
        AND dc.vocabulary_id = 'SNOMED'
    ) c
    LEFT JOIN `CY_IMOSPHERE_CDM_531.condition_occurrence` oc ON 
        oc.person_id = c.person_id 
        AND oc.condition_concept_id = c.condition_concept_id
        AND oc.condition_source_value = c.condition_source_value
        AND COALESCE(oc.condition_source_concept_id, -1) = c.condition_source_concept_id
        AND oc.condition_start_datetime = c.condition_start_datetime
        AND oc.condition_type_concept_id = c.condition_type_concept_id
        AND COALESCE(oc.provider_id, -1) = -1
        AND COALESCE(oc.condition_status_concept_id, -1) = COALESCE(c.condition_status_concept_id, -1)
        AND COALESCE(oc.visit_occurrence_id, -1) = COALESCE(c.visit_occurrence_id, -1)
        AND COALESCE(oc.visit_occurrence_id, -1) = c.visit_occurrence_id
        AND COALESCE(oc.visit_detail_id, -1) = c.visit_detail_id
    WHERE oc.condition_occurrence_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id,  related_table, related_count, logged_at) 
    SELECT 'Added condition_occurrence', run_id, 'src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2, SUS_BRI_APC_010415_to_300619_P1  - Diagnosis_5th_Secondary_ICD -> condition_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[Diagnosis_6th_Secondary_ICD] Transfer
    */
    SET max_current_id = COALESCE((SELECT condition_occurrence_id FROM `CY_IMOSPHERE_CDM_531.condition_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.condition_occurrence`
    (
        condition_occurrence_id,
        person_id,
        condition_concept_id,
        condition_source_value,
        condition_source_concept_id,
        condition_start_date,
        condition_start_datetime,
        condition_type_concept_id,
        condition_status_concept_id,
        visit_occurrence_id,
        visit_detail_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() condition_occurrence_id,
        c.person_id,
        c.condition_concept_id,
        c.condition_source_value,
        c.condition_source_concept_id,
        c.condition_start_date,
        c.condition_start_datetime,
        c.condition_type_concept_id,
        c.condition_status_concept_id,
        c.visit_occurrence_id,
        c.visit_detail_id
    FROM
    (
        SELECT DISTINCT
            person_id,
            dc.concept_id condition_concept_id,
            Diagnosis_6th_Secondary_ICD condition_source_value,
            sc.concept_id  condition_source_concept_id,
            PARSE_DATE('%Y%m%d', Start_Date_Consultant_Episode) condition_start_date,
            CAST(PARSE_DATE('%Y%m%d', Start_Date_Consultant_Episode) AS DATETIME) condition_start_datetime,
            32817 condition_type_concept_id, --EHR
            32908 condition_status_concept_id, --Secondary diagnosis
            vl.id visit_occurrence_id,
            vdl.id visit_detail_id
        FROM
        (    
            SELECT DISTINCT
                person_id,
                Start_Date_Consultant_Episode,
                Diagnosis_6th_Secondary_ICD,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id) visit_occurrence_source_value,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id, '_', Episode_Number) visit_detail_source_value
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1`
            UNION DISTINCT
            SELECT DISTINCT
                person_id,
                Start_Date_Consultant_Episode,
                Diagnosis_6th_Secondary_ICD,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id) visit_occurrence_source_value,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id, '_', Episode_Number) visit_detail_source_value
            FROM `CY_IMOSPHERE_WORKSPACE.src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2`
        ) c
        LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups` vdl ON vdl.source_value = visit_detail_source_value
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = c.visit_occurrence_source_value
        JOIN `CY_CDM_VOCAB.concept` sc on REPLACE(sc.concept_code, '.', '') = c.Diagnosis_6th_Secondary_ICD
        JOIN `CY_CDM_VOCAB.concept_relationship` cr on cr.concept_id_1 = sc.concept_id
        JOIN `CY_CDM_VOCAB.concept` dc on dc.concept_id = cr.concept_id_2
        WHERE c.Start_Date_Consultant_Episode IS NOT NULL
        AND c.person_id IS NOT NULL
        AND cr.relationship_id = 'Maps to'
        AND dc.standard_concept = 'S'
        AND dc.domain_id = 'Condition'
        AND sc.vocabulary_id = 'ICD10'
        AND c.Diagnosis_6th_Secondary_ICD IS NOT NULL
        AND dc.vocabulary_id = 'SNOMED'
    ) c
    LEFT JOIN `CY_IMOSPHERE_CDM_531.condition_occurrence` oc ON 
        oc.person_id = c.person_id 
        AND oc.condition_concept_id = c.condition_concept_id
        AND oc.condition_source_value = c.condition_source_value
        AND COALESCE(oc.condition_source_concept_id, -1) = c.condition_source_concept_id
        AND oc.condition_start_datetime = c.condition_start_datetime
        AND oc.condition_type_concept_id = c.condition_type_concept_id
        AND COALESCE(oc.provider_id, -1) = -1
        AND COALESCE(oc.condition_status_concept_id, -1) = COALESCE(c.condition_status_concept_id, -1)
        AND COALESCE(oc.visit_occurrence_id, -1) = COALESCE(c.visit_occurrence_id, -1)
        AND COALESCE(oc.visit_occurrence_id, -1) = c.visit_occurrence_id
        AND COALESCE(oc.visit_detail_id, -1) = c.visit_detail_id
    WHERE oc.condition_occurrence_id IS NULL;

   INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id,  related_table, related_count, logged_at) 
   SELECT 'Added condition_occurrence', run_id, 'src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2, SUS_BRI_APC_010415_to_300619_P1  - Diagnosis_6th_Secondary_ICD -> condition_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[Diagnosis_7th_Secondary_ICD] Transfer
    */

    SET max_current_id = COALESCE((SELECT condition_occurrence_id FROM `CY_IMOSPHERE_CDM_531.condition_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.condition_occurrence`
    (
        condition_occurrence_id,
        person_id,
        condition_concept_id,
        condition_source_value,
        condition_source_concept_id,
        condition_start_date,
        condition_start_datetime,
        condition_type_concept_id,
        condition_status_concept_id,
        visit_occurrence_id,
        visit_detail_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() condition_occurrence_id,
        c.person_id,
        c.condition_concept_id,
        c.condition_source_value,
        c.condition_source_concept_id,
        c.condition_start_date,
        c.condition_start_datetime,
        c.condition_type_concept_id,
        c.condition_status_concept_id,
        c.visit_occurrence_id,
        c.visit_detail_id
    FROM
    (
        SELECT DISTINCT
            person_id,
            dc.concept_id condition_concept_id,
            Diagnosis_7th_Secondary_ICD condition_source_value,
            sc.concept_id  condition_source_concept_id,
            PARSE_DATE('%Y%m%d', Start_Date_Consultant_Episode) condition_start_date,
            CAST(PARSE_DATE('%Y%m%d', Start_Date_Consultant_Episode) AS DATETIME) condition_start_datetime,
            32817 condition_type_concept_id, --EHR
            32908 condition_status_concept_id, --Secondary diagnosis
            vl.id visit_occurrence_id,
            vdl.id visit_detail_id
        FROM
        (    
            SELECT DISTINCT
                person_id,
                Start_Date_Consultant_Episode,
                Diagnosis_7th_Secondary_ICD,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id) visit_occurrence_source_value,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id, '_', Episode_Number) visit_detail_source_value
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1`
            UNION DISTINCT
            SELECT DISTINCT
                person_id,
                Start_Date_Consultant_Episode,
                Diagnosis_7th_Secondary_ICD,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id) visit_occurrence_source_value,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id, '_', Episode_Number) visit_detail_source_value
            FROM `CY_IMOSPHERE_WORKSPACE.src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2`
        ) c
        LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups` vdl ON vdl.source_value = visit_detail_source_value
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = c.visit_occurrence_source_value
        JOIN `CY_CDM_VOCAB.concept` sc on REPLACE(sc.concept_code, '.', '') = c.Diagnosis_7th_Secondary_ICD
        JOIN `CY_CDM_VOCAB.concept_relationship` cr on cr.concept_id_1 = sc.concept_id
        JOIN `CY_CDM_VOCAB.concept` dc on dc.concept_id = cr.concept_id_2
        WHERE c.Start_Date_Consultant_Episode IS NOT NULL
        AND c.person_id IS NOT NULL
        AND cr.relationship_id = 'Maps to'
        AND dc.standard_concept = 'S'
        AND dc.domain_id = 'Condition'
        AND sc.vocabulary_id = 'ICD10'
        AND c.Diagnosis_7th_Secondary_ICD IS NOT NULL
        AND dc.vocabulary_id = 'SNOMED'
    ) c
    LEFT JOIN `CY_IMOSPHERE_CDM_531.condition_occurrence` oc ON 
        oc.person_id = c.person_id 
        AND oc.condition_concept_id = c.condition_concept_id
        AND oc.condition_source_value = c.condition_source_value
        AND COALESCE(oc.condition_source_concept_id, -1) = c.condition_source_concept_id
        AND oc.condition_start_datetime = c.condition_start_datetime
        AND oc.condition_type_concept_id = c.condition_type_concept_id
        AND COALESCE(oc.provider_id, -1) = -1
        AND COALESCE(oc.condition_status_concept_id, -1) = COALESCE(c.condition_status_concept_id, -1)
        AND COALESCE(oc.visit_occurrence_id, -1) = COALESCE(c.visit_occurrence_id, -1)
        AND COALESCE(oc.visit_occurrence_id, -1) = c.visit_occurrence_id
        AND COALESCE(oc.visit_detail_id, -1) = c.visit_detail_id
    WHERE oc.condition_occurrence_id IS NULL;
    
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id,  related_table, related_count, logged_at) 
    SELECT 'Added condition_occurrence', run_id, 'src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2, SUS_BRI_APC_010415_to_300619_P1  - Diagnosis_7th_Secondary_ICD -> condition_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[Diagnosis_8th_Secondary_ICD] Transfer
    */
    SET max_current_id = COALESCE((SELECT condition_occurrence_id FROM `CY_IMOSPHERE_CDM_531.condition_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.condition_occurrence`
    (
        condition_occurrence_id,
        person_id,
        condition_concept_id,
        condition_source_value,
        condition_source_concept_id,
        condition_start_date,
        condition_start_datetime,
        condition_type_concept_id,
        condition_status_concept_id,
        visit_occurrence_id,
        visit_detail_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() condition_occurrence_id,
        c.person_id,
        c.condition_concept_id,
        c.condition_source_value,
        c.condition_source_concept_id,
        c.condition_start_date,
        c.condition_start_datetime,
        c.condition_type_concept_id,
        c.condition_status_concept_id,
        c.visit_occurrence_id,
        c.visit_detail_id
    FROM
    (
        SELECT DISTINCT
            person_id,
            dc.concept_id condition_concept_id,
            Diagnosis_8th_Secondary_ICD condition_source_value,
            sc.concept_id  condition_source_concept_id,
            PARSE_DATE('%Y%m%d', Start_Date_Consultant_Episode) condition_start_date,
            CAST(PARSE_DATE('%Y%m%d', Start_Date_Consultant_Episode) AS DATETIME) condition_start_datetime,
            32817 condition_type_concept_id, --EHR
            32908 condition_status_concept_id, --Secondary diagnosis
            vl.id visit_occurrence_id,
            vdl.id visit_detail_id
        FROM
        (    
            SELECT DISTINCT
                person_id,
                Start_Date_Consultant_Episode,
                Diagnosis_8th_Secondary_ICD,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id) visit_occurrence_source_value,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id, '_', Episode_Number) visit_detail_source_value
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1`
            UNION DISTINCT
            SELECT DISTINCT
                person_id,
                Start_Date_Consultant_Episode,
                Diagnosis_8th_Secondary_ICD,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id) visit_occurrence_source_value,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id, '_', Episode_Number) visit_detail_source_value
            FROM `CY_IMOSPHERE_WORKSPACE.src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2`
        ) c
        LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups` vdl ON vdl.source_value = visit_detail_source_value
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = c.visit_occurrence_source_value
        JOIN `CY_CDM_VOCAB.concept` sc on REPLACE(sc.concept_code, '.', '') = c.Diagnosis_8th_Secondary_ICD
        JOIN `CY_CDM_VOCAB.concept_relationship` cr on cr.concept_id_1 = sc.concept_id
        JOIN `CY_CDM_VOCAB.concept` dc on dc.concept_id = cr.concept_id_2
        WHERE c.Start_Date_Consultant_Episode IS NOT NULL
        AND c.person_id IS NOT NULL
        AND cr.relationship_id = 'Maps to'
        AND dc.standard_concept = 'S'
        AND dc.domain_id = 'Condition'
        AND sc.vocabulary_id = 'ICD10'
        AND c.Diagnosis_8th_Secondary_ICD IS NOT NULL
        AND dc.vocabulary_id = 'SNOMED'
    ) c
    LEFT JOIN `CY_IMOSPHERE_CDM_531.condition_occurrence` oc ON 
        oc.person_id = c.person_id 
        AND oc.condition_concept_id = c.condition_concept_id
        AND oc.condition_source_value = c.condition_source_value
        AND COALESCE(oc.condition_source_concept_id, -1) = c.condition_source_concept_id
        AND oc.condition_start_datetime = c.condition_start_datetime
        AND oc.condition_type_concept_id = c.condition_type_concept_id
        AND COALESCE(oc.provider_id, -1) = -1
        AND COALESCE(oc.condition_status_concept_id, -1) = COALESCE(c.condition_status_concept_id, -1)
        AND COALESCE(oc.visit_occurrence_id, -1) = COALESCE(c.visit_occurrence_id, -1)
        AND COALESCE(oc.visit_occurrence_id, -1) = c.visit_occurrence_id
        AND COALESCE(oc.visit_detail_id, -1) = c.visit_detail_id
    WHERE oc.condition_occurrence_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id,  related_table, related_count, logged_at) 
    SELECT 'Added condition_occurrence', run_id, 'src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2, SUS_BRI_APC_010415_to_300619_P1  - Diagnosis_8th_Secondary_ICD -> condition_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[Diagnosis_9th_Secondary_ICD] Transfer
    */
    SET max_current_id = COALESCE((SELECT condition_occurrence_id FROM `CY_IMOSPHERE_CDM_531.condition_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.condition_occurrence`
    (
        condition_occurrence_id,
        person_id,
        condition_concept_id,
        condition_source_value,
        condition_source_concept_id,
        condition_start_date,
        condition_start_datetime,
        condition_type_concept_id,
        condition_status_concept_id,
        visit_occurrence_id,
        visit_detail_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() condition_occurrence_id,
        c.person_id,
        c.condition_concept_id,
        c.condition_source_value,
        c.condition_source_concept_id,
        c.condition_start_date,
        c.condition_start_datetime,
        c.condition_type_concept_id,
        c.condition_status_concept_id,
        c.visit_occurrence_id,
        c.visit_detail_id
    FROM
    (
        SELECT DISTINCT
            person_id,
            dc.concept_id condition_concept_id,
            Diagnosis_9th_Secondary_ICD condition_source_value,
            sc.concept_id  condition_source_concept_id,
            PARSE_DATE('%Y%m%d', Start_Date_Consultant_Episode) condition_start_date,
            CAST(PARSE_DATE('%Y%m%d', Start_Date_Consultant_Episode) AS DATETIME) condition_start_datetime,
            32817 condition_type_concept_id, --EHR
            32908 condition_status_concept_id, --Secondary diagnosis
            vl.id visit_occurrence_id,
            vdl.id visit_detail_id
        FROM
        (    
            SELECT DISTINCT
                person_id,
                Start_Date_Consultant_Episode,
                Diagnosis_9th_Secondary_ICD,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id) visit_occurrence_source_value,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id, '_', Episode_Number) visit_detail_source_value
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1`
            UNION DISTINCT
            SELECT DISTINCT
                person_id,
                Start_Date_Consultant_Episode,
                Diagnosis_9th_Secondary_ICD,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id) visit_occurrence_source_value,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id, '_', Episode_Number) visit_detail_source_value
            FROM `CY_IMOSPHERE_WORKSPACE.src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2`
        ) c
        LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups` vdl ON vdl.source_value = visit_detail_source_value
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = c.visit_occurrence_source_value
        JOIN `CY_CDM_VOCAB.concept` sc on REPLACE(sc.concept_code, '.', '') = c.Diagnosis_9th_Secondary_ICD
        JOIN `CY_CDM_VOCAB.concept_relationship` cr on cr.concept_id_1 = sc.concept_id
        JOIN `CY_CDM_VOCAB.concept` dc on dc.concept_id = cr.concept_id_2
        WHERE c.Start_Date_Consultant_Episode IS NOT NULL
        AND c.person_id IS NOT NULL
        AND cr.relationship_id = 'Maps to'
        AND dc.standard_concept = 'S'
        AND dc.domain_id = 'Condition'
        AND sc.vocabulary_id = 'ICD10'
        AND c.Diagnosis_9th_Secondary_ICD IS NOT NULL
        AND dc.vocabulary_id = 'SNOMED'
    ) c
    LEFT JOIN `CY_IMOSPHERE_CDM_531.condition_occurrence` oc ON 
        oc.person_id = c.person_id 
        AND oc.condition_concept_id = c.condition_concept_id
        AND oc.condition_source_value = c.condition_source_value
        AND COALESCE(oc.condition_source_concept_id, -1) = c.condition_source_concept_id
        AND oc.condition_start_datetime = c.condition_start_datetime
        AND oc.condition_type_concept_id = c.condition_type_concept_id
        AND COALESCE(oc.provider_id, -1) = -1
        AND COALESCE(oc.condition_status_concept_id, -1) = COALESCE(c.condition_status_concept_id, -1)
        AND COALESCE(oc.visit_occurrence_id, -1) = COALESCE(c.visit_occurrence_id, -1)
        AND COALESCE(oc.visit_occurrence_id, -1) = c.visit_occurrence_id
        AND COALESCE(oc.visit_detail_id, -1) = c.visit_detail_id
    WHERE oc.condition_occurrence_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id,  related_table, related_count, logged_at) 
    SELECT 'Added condition_occurrence', run_id, 'src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2, SUS_BRI_APC_010415_to_300619_P1  - Diagnosis_9th_Secondary_ICD -> condition_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[Diagnosis_10th_Secondary_ICD] Transfer
    */
    SET max_current_id = COALESCE((SELECT condition_occurrence_id FROM `CY_IMOSPHERE_CDM_531.condition_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.condition_occurrence`
    (
        condition_occurrence_id,
        person_id,
        condition_concept_id,
        condition_source_value,
        condition_source_concept_id,
        condition_start_date,
        condition_start_datetime,
        condition_type_concept_id,
        condition_status_concept_id,
        visit_occurrence_id,
        visit_detail_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() condition_occurrence_id,
        c.person_id,
        c.condition_concept_id,
        c.condition_source_value,
        c.condition_source_concept_id,
        c.condition_start_date,
        c.condition_start_datetime,
        c.condition_type_concept_id,
        c.condition_status_concept_id,
        c.visit_occurrence_id,
        c.visit_detail_id
    FROM
    (
        SELECT DISTINCT
            person_id,
            dc.concept_id condition_concept_id,
            Diagnosis_10th_Secondary_ICD condition_source_value,
            sc.concept_id  condition_source_concept_id,
            PARSE_DATE('%Y%m%d', Start_Date_Consultant_Episode) condition_start_date,
            CAST(PARSE_DATE('%Y%m%d', Start_Date_Consultant_Episode) AS DATETIME) condition_start_datetime,
            32817 condition_type_concept_id, --EHR
            32908 condition_status_concept_id, --Secondary diagnosis
            vl.id visit_occurrence_id,
            vdl.id visit_detail_id
        FROM
        (    
            SELECT DISTINCT
                person_id,
                Start_Date_Consultant_Episode,
                Diagnosis_10th_Secondary_ICD,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id) visit_occurrence_source_value,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id, '_', Episode_Number) visit_detail_source_value
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1`
            UNION DISTINCT
            SELECT DISTINCT
                person_id,
                Start_Date_Consultant_Episode,
                Diagnosis_10th_Secondary_ICD,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id) visit_occurrence_source_value,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id, '_', Episode_Number) visit_detail_source_value
            FROM `CY_IMOSPHERE_WORKSPACE.src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2`
        ) c
        LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups` vdl ON vdl.source_value = visit_detail_source_value
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = c.visit_occurrence_source_value
        JOIN `CY_CDM_VOCAB.concept` sc on REPLACE(sc.concept_code, '.', '') = c.Diagnosis_10th_Secondary_ICD
        JOIN `CY_CDM_VOCAB.concept_relationship` cr on cr.concept_id_1 = sc.concept_id
        JOIN `CY_CDM_VOCAB.concept` dc on dc.concept_id = cr.concept_id_2
        WHERE c.Start_Date_Consultant_Episode IS NOT NULL
        AND c.person_id IS NOT NULL
        AND cr.relationship_id = 'Maps to'
        AND dc.standard_concept = 'S'
        AND dc.domain_id = 'Condition'
        AND sc.vocabulary_id = 'ICD10'
        AND c.Diagnosis_10th_Secondary_ICD IS NOT NULL
        AND dc.vocabulary_id = 'SNOMED'
    ) c
    LEFT JOIN `CY_IMOSPHERE_CDM_531.condition_occurrence` oc ON 
        oc.person_id = c.person_id 
        AND oc.condition_concept_id = c.condition_concept_id
        AND oc.condition_source_value = c.condition_source_value
        AND COALESCE(oc.condition_source_concept_id, -1) = c.condition_source_concept_id
        AND oc.condition_start_datetime = c.condition_start_datetime
        AND oc.condition_type_concept_id = c.condition_type_concept_id
        AND COALESCE(oc.provider_id, -1) = -1
        AND COALESCE(oc.condition_status_concept_id, -1) = COALESCE(c.condition_status_concept_id, -1)
        AND COALESCE(oc.visit_occurrence_id, -1) = COALESCE(c.visit_occurrence_id, -1)
        AND COALESCE(oc.visit_occurrence_id, -1) = c.visit_occurrence_id
        AND COALESCE(oc.visit_detail_id, -1) = c.visit_detail_id
    WHERE oc.condition_occurrence_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id,  related_table, related_count, logged_at) 
    SELECT 'Added condition_occurrence', run_id, 'src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2, SUS_BRI_APC_010415_to_300619_P1  - Diagnosis_10th_Secondary_ICD -> condition_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[Diagnosis_11th_Secondary_ICD] Transfer
    */
    SET max_current_id = COALESCE((SELECT condition_occurrence_id FROM `CY_IMOSPHERE_CDM_531.condition_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.condition_occurrence`
    (
        condition_occurrence_id,
        person_id,
        condition_concept_id,
        condition_source_value,
        condition_source_concept_id,
        condition_start_date,
        condition_start_datetime,
        condition_type_concept_id,
        condition_status_concept_id,
        visit_occurrence_id,
        visit_detail_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() condition_occurrence_id,
        c.person_id,
        c.condition_concept_id,
        c.condition_source_value,
        c.condition_source_concept_id,
        c.condition_start_date,
        c.condition_start_datetime,
        c.condition_type_concept_id,
        c.condition_status_concept_id,
        c.visit_occurrence_id,
        c.visit_detail_id
    FROM
    (
        SELECT DISTINCT
            person_id,
            dc.concept_id condition_concept_id,
            Diagnosis_11th_Secondary_ICD condition_source_value,
            sc.concept_id  condition_source_concept_id,
            PARSE_DATE('%Y%m%d', Start_Date_Consultant_Episode) condition_start_date,
            CAST(PARSE_DATE('%Y%m%d', Start_Date_Consultant_Episode) AS DATETIME) condition_start_datetime,
            32817 condition_type_concept_id, --EHR
            32908 condition_status_concept_id, --Secondary diagnosis
            vl.id visit_occurrence_id,
            vdl.id visit_detail_id
        FROM
        (    
            SELECT DISTINCT
                person_id,
                Start_Date_Consultant_Episode,
                Diagnosis_11th_Secondary_ICD,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id) visit_occurrence_source_value,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id, '_', Episode_Number) visit_detail_source_value
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1`
            UNION DISTINCT
            SELECT DISTINCT
                person_id,
                Start_Date_Consultant_Episode,
                Diagnosis_11th_Secondary_ICD,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id) visit_occurrence_source_value,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id, '_', Episode_Number) visit_detail_source_value
            FROM `CY_IMOSPHERE_WORKSPACE.src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2`
        ) c
        LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups` vdl ON vdl.source_value = visit_detail_source_value
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = c.visit_occurrence_source_value
        JOIN `CY_CDM_VOCAB.concept` sc on REPLACE(sc.concept_code, '.', '') = c.Diagnosis_11th_Secondary_ICD
        JOIN `CY_CDM_VOCAB.concept_relationship` cr on cr.concept_id_1 = sc.concept_id
        JOIN `CY_CDM_VOCAB.concept` dc on dc.concept_id = cr.concept_id_2
        WHERE c.Start_Date_Consultant_Episode IS NOT NULL
        AND c.person_id IS NOT NULL
        AND cr.relationship_id = 'Maps to'
        AND dc.standard_concept = 'S'
        AND dc.domain_id = 'Condition'
        AND sc.vocabulary_id = 'ICD10'
        AND c.Diagnosis_11th_Secondary_ICD IS NOT NULL
        AND dc.vocabulary_id = 'SNOMED'
    ) c
    LEFT JOIN `CY_IMOSPHERE_CDM_531.condition_occurrence` oc ON 
        oc.person_id = c.person_id 
        AND oc.condition_concept_id = c.condition_concept_id
        AND oc.condition_source_value = c.condition_source_value
        AND COALESCE(oc.condition_source_concept_id, -1) = c.condition_source_concept_id
        AND oc.condition_start_datetime = c.condition_start_datetime
        AND oc.condition_type_concept_id = c.condition_type_concept_id
        AND COALESCE(oc.provider_id, -1) = -1
        AND COALESCE(oc.condition_status_concept_id, -1) = COALESCE(c.condition_status_concept_id, -1)
        AND COALESCE(oc.visit_occurrence_id, -1) = COALESCE(c.visit_occurrence_id, -1)
        AND COALESCE(oc.visit_occurrence_id, -1) = c.visit_occurrence_id
        AND COALESCE(oc.visit_detail_id, -1) = c.visit_detail_id
    WHERE oc.condition_occurrence_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id,  related_table, related_count, logged_at) 
    SELECT 'Added condition_occurrence', run_id, 'src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2, SUS_BRI_APC_010415_to_300619_P1  - Diagnosis_11th_Secondary_ICD -> condition_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[Diagnosis_12th_Secondary_ICD] Transfer
    */
    SET max_current_id = COALESCE((SELECT condition_occurrence_id FROM `CY_IMOSPHERE_CDM_531.condition_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.condition_occurrence`
    (
        condition_occurrence_id,
        person_id,
        condition_concept_id,
        condition_source_value,
        condition_source_concept_id,
        condition_start_date,
        condition_start_datetime,
        condition_type_concept_id,
        condition_status_concept_id,
        visit_occurrence_id,
        visit_detail_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() condition_occurrence_id,
        c.person_id,
        c.condition_concept_id,
        c.condition_source_value,
        c.condition_source_concept_id,
        c.condition_start_date,
        c.condition_start_datetime,
        c.condition_type_concept_id,
        c.condition_status_concept_id,
        c.visit_occurrence_id,
        c.visit_detail_id
    FROM
    (
        SELECT DISTINCT
            person_id,
            dc.concept_id condition_concept_id,
            Diagnosis_12th_Secondary_ICD condition_source_value,
            sc.concept_id  condition_source_concept_id,
            PARSE_DATE('%Y%m%d', Start_Date_Consultant_Episode) condition_start_date,
            CAST(PARSE_DATE('%Y%m%d', Start_Date_Consultant_Episode) AS DATETIME) condition_start_datetime,
            32817 condition_type_concept_id, --EHR
            32908 condition_status_concept_id, --Secondary diagnosis
            vl.id visit_occurrence_id,
            vdl.id visit_detail_id
        FROM
        (    
            SELECT DISTINCT
                person_id,
                Start_Date_Consultant_Episode,
                Diagnosis_12th_Secondary_ICD,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id) visit_occurrence_source_value,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id, '_', Episode_Number) visit_detail_source_value
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1`
            UNION DISTINCT
            SELECT DISTINCT
                person_id,
                Start_Date_Consultant_Episode,
                Diagnosis_12th_Secondary_ICD,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id) visit_occurrence_source_value,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id, '_', Episode_Number) visit_detail_source_value
            FROM `CY_IMOSPHERE_WORKSPACE.src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2`
        ) c
        LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups` vdl ON vdl.source_value = visit_detail_source_value
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = c.visit_occurrence_source_value
        JOIN `CY_CDM_VOCAB.concept` sc on REPLACE(sc.concept_code, '.', '') = c.Diagnosis_12th_Secondary_ICD
        JOIN `CY_CDM_VOCAB.concept_relationship` cr on cr.concept_id_1 = sc.concept_id
        JOIN `CY_CDM_VOCAB.concept` dc on dc.concept_id = cr.concept_id_2
        WHERE c.Start_Date_Consultant_Episode IS NOT NULL
        AND c.person_id IS NOT NULL
        AND cr.relationship_id = 'Maps to'
        AND dc.standard_concept = 'S'
        AND dc.domain_id = 'Condition'
        AND sc.vocabulary_id = 'ICD10'
        AND c.Diagnosis_12th_Secondary_ICD IS NOT NULL
        AND dc.vocabulary_id = 'SNOMED'
    ) c
    LEFT JOIN `CY_IMOSPHERE_CDM_531.condition_occurrence` oc ON 
        oc.person_id = c.person_id 
        AND oc.condition_concept_id = c.condition_concept_id
        AND oc.condition_source_value = c.condition_source_value
        AND COALESCE(oc.condition_source_concept_id, -1) = c.condition_source_concept_id
        AND oc.condition_start_datetime = c.condition_start_datetime
        AND oc.condition_type_concept_id = c.condition_type_concept_id
        AND COALESCE(oc.provider_id, -1) = -1
        AND COALESCE(oc.condition_status_concept_id, -1) = COALESCE(c.condition_status_concept_id, -1)
        AND COALESCE(oc.visit_occurrence_id, -1) = COALESCE(c.visit_occurrence_id, -1)
        AND COALESCE(oc.visit_occurrence_id, -1) = c.visit_occurrence_id
        AND COALESCE(oc.visit_detail_id, -1) = c.visit_detail_id
    WHERE oc.condition_occurrence_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id,  related_table, related_count, logged_at) 
    SELECT 'Added condition_occurrence', run_id, 'src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2, SUS_BRI_APC_010415_to_300619_P1  - Diagnosis_12th_Secondary_ICD -> condition_occurrence', @@row_count, CURRENT_DATETIME();
END;