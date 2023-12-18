CREATE OR REPLACE PROCEDURE CY_IMOSPHERE_WORKSPACE.TRANSFER_PROCEDURE_OCCURRENCE(run_id INT64)
OPTIONS (strict_mode=false)
BEGIN
    DECLARE max_current_id INT64;

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    ------------[SUS_BRI_AE_010415_to_300619]
    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_First] Transfer
    */
    SET max_current_id = COALESCE((SELECT procedure_occurrence_id FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.procedure_occurrence`
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_source_value,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as procedure_occurrence_id,
        p.person_id,
        p.procedure_concept_id,
        p.procedure_source_value,
        p.procedure_date,
        p.procedure_datetime,
        p.procedure_type_concept_id,
        p.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
                p.person_id,
                pc.concept_id procedure_concept_id,
                Accident_And_Emergency_Investigation_First procedure_source_value,	
                PARSE_DATE('%Y%m%d', p.Arrival_Date) procedure_date,
                PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.Arrival_Date ,'000000')) procedure_datetime,
                32817 procedure_type_concept_id, --EHR
                vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` p
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` pc on pc.source_value = REPLACE(p.Accident_And_Emergency_Investigation_First, ' ', '') AND pc.destination_table = 'PROCEDURE_OCCURRENCE' AND pc.source_table = 'SUS_BRI_AE_010415_to_300619' AND pc.source_column = 'Accident_And_Emergency_Investigation_First'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl ON vl.source_value = CONCAT(CAST(p.AandE_Attendance_Number as STRING),'_', p.person_id)
        WHERE p.Arrival_Date IS NOT NULL
        AND p.person_id IS NOT NULL
        AND p.Accident_And_Emergency_Investigation_First IS NOT NULL
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.procedure_occurrence` op ON 
        op.person_id = p.person_id 
        AND op.procedure_concept_id = p.procedure_concept_id
        AND op.procedure_source_value = p.procedure_source_value
        AND COALESCE(op.procedure_source_concept_id, -1) = -1
        AND op.procedure_datetime = p.procedure_datetime
        AND op.procedure_type_concept_id = p.procedure_type_concept_id
        AND COALESCE(op.provider_id, -1) = -1
        AND COALESCE(op.visit_occurrence_id, -1) = COALESCE(p.visit_occurrence_id, -1)
        AND COALESCE(op.visit_detail_id, -1) = -1
    WHERE op.procedure_occurrence_id IS NULL;     

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added procedure_occurrence', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_First -> procedure_occurrence', @@row_count, CURRENT_DATETIME();
    
    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Second] Transfer
    */
    SET max_current_id = COALESCE((SELECT procedure_occurrence_id FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.procedure_occurrence`
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_source_value,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as procedure_occurrence_id,
        p.person_id,
        p.procedure_concept_id,
        p.procedure_source_value,
        p.procedure_date,
        p.procedure_datetime,
        p.procedure_type_concept_id,
        p.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            p.person_id,
            pc.concept_id procedure_concept_id,
            Accident_And_Emergency_Investigation_Second procedure_source_value,	
            PARSE_DATE('%Y%m%d', p.Arrival_Date) procedure_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.Arrival_Date ,'000000')) procedure_datetime,
            32817 procedure_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` p
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` pc on pc.source_value = REPLACE(p.Accident_And_Emergency_Investigation_Second, ' ', '') AND pc.destination_table = 'PROCEDURE_OCCURRENCE' AND pc.source_table = 'SUS_BRI_AE_010415_to_300619' AND pc.source_column = 'Accident_And_Emergency_Investigation_Second'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl ON vl.source_value = CONCAT(CAST(p.AandE_Attendance_Number as STRING),'_', p.person_id)
        WHERE p.Arrival_Date IS NOT NULL
        AND p.person_id IS NOT NULL
        AND p.Accident_And_Emergency_Investigation_Second IS NOT NULL
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.procedure_occurrence` op ON 
        op.person_id = p.person_id 
        AND op.procedure_concept_id = p.procedure_concept_id
        AND op.procedure_source_value = p.procedure_source_value
        AND COALESCE(op.procedure_source_concept_id, -1) = -1
        AND op.procedure_datetime = p.procedure_datetime
        AND op.procedure_type_concept_id = p.procedure_type_concept_id
        AND COALESCE(op.provider_id, -1) = -1
        AND COALESCE(op.visit_occurrence_id, -1) = COALESCE(p.visit_occurrence_id, -1)
        AND COALESCE(op.visit_detail_id, -1) = -1
    WHERE op.procedure_occurrence_id IS NULL;     

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added procedure_occurrence', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Second -> procedure_occurrence', @@row_count, CURRENT_DATETIME();
     
    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Third] Transfer
    */
    SET max_current_id = COALESCE((SELECT procedure_occurrence_id FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.procedure_occurrence`
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_source_value,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as procedure_occurrence_id,
        p.person_id,
        p.procedure_concept_id,
        p.procedure_source_value,
        p.procedure_date,
        p.procedure_datetime,
        p.procedure_type_concept_id,
        p.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            p.person_id,
            pc.concept_id procedure_concept_id,
            Accident_And_Emergency_Investigation_Third procedure_source_value,	
            PARSE_DATE('%Y%m%d', p.Arrival_Date) procedure_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.Arrival_Date ,'000000')) procedure_datetime,
            32817 procedure_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` p
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` pc on pc.source_value = REPLACE(p.Accident_And_Emergency_Investigation_Third, ' ', '') AND pc.destination_table = 'PROCEDURE_OCCURRENCE' AND pc.source_table = 'SUS_BRI_AE_010415_to_300619' AND pc.source_column = 'Accident_And_Emergency_Investigation_Third'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl ON vl.source_value = CONCAT(CAST(p.AandE_Attendance_Number as STRING),'_', p.person_id)
        WHERE p.Arrival_Date IS NOT NULL
        AND p.person_id IS NOT NULL
        AND p.Accident_And_Emergency_Investigation_Third IS NOT NULL
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.procedure_occurrence` op ON 
        op.person_id = p.person_id 
        AND op.procedure_concept_id = p.procedure_concept_id
        AND op.procedure_source_value = p.procedure_source_value
        AND COALESCE(op.procedure_source_concept_id, -1) = -1
        AND op.procedure_datetime = p.procedure_datetime
        AND op.procedure_type_concept_id = p.procedure_type_concept_id
        AND COALESCE(op.provider_id, -1) = -1
        AND COALESCE(op.visit_occurrence_id, -1) = COALESCE(p.visit_occurrence_id, -1)
        AND COALESCE(op.visit_detail_id, -1) = -1
    WHERE op.procedure_occurrence_id IS NULL;   

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added procedure_occurrence', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Third -> procedure_occurrence', @@row_count, CURRENT_DATETIME();


    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Fourth] Transfer
    */
    SET max_current_id = COALESCE((SELECT procedure_occurrence_id FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.procedure_occurrence`
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_source_value,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as procedure_occurrence_id,
        p.person_id,
        p.procedure_concept_id,
        p.procedure_source_value,
        p.procedure_date,
        p.procedure_datetime,
        p.procedure_type_concept_id,
        p.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            p.person_id,
            pc.concept_id procedure_concept_id,
            Accident_And_Emergency_Investigation_Fourth procedure_source_value,	
            PARSE_DATE('%Y%m%d', p.Arrival_Date) procedure_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.Arrival_Date ,'000000')) procedure_datetime,
            32817 procedure_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` p
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` pc on pc.source_value = REPLACE(p.Accident_And_Emergency_Investigation_Fourth, ' ', '') AND pc.destination_table = 'PROCEDURE_OCCURRENCE' AND pc.source_table = 'SUS_BRI_AE_010415_to_300619' AND pc.source_column = 'Accident_And_Emergency_Investigation_Fourth'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl ON vl.source_value = CONCAT(CAST(p.AandE_Attendance_Number as STRING),'_', p.person_id)
        WHERE p.Arrival_Date IS NOT NULL
        AND p.person_id IS NOT NULL
        AND p.Accident_And_Emergency_Investigation_Fourth IS NOT NULL
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.procedure_occurrence` op ON 
        op.person_id = p.person_id 
        AND op.procedure_concept_id = p.procedure_concept_id
        AND op.procedure_source_value = p.procedure_source_value
        AND COALESCE(op.procedure_source_concept_id, -1) = -1
        AND op.procedure_datetime = p.procedure_datetime
        AND op.procedure_type_concept_id = p.procedure_type_concept_id
        AND COALESCE(op.provider_id, -1) = -1
        AND COALESCE(op.visit_occurrence_id, -1) = COALESCE(p.visit_occurrence_id, -1)
        AND COALESCE(op.visit_detail_id, -1) = -1
    WHERE op.procedure_occurrence_id IS NULL;       

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added procedure_occurrence', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Fourth -> procedure_occurrence', @@row_count, CURRENT_DATETIME();


    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Five] Transfer
    */
    SET max_current_id = COALESCE((SELECT procedure_occurrence_id FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.procedure_occurrence`
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_source_value,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as procedure_occurrence_id,
        p.person_id,
        p.procedure_concept_id,
        p.procedure_source_value,
        p.procedure_date,
        p.procedure_datetime,
        p.procedure_type_concept_id,
        p.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            p.person_id,
            pc.concept_id procedure_concept_id,
            Accident_And_Emergency_Investigation_Fifth procedure_source_value,	
            PARSE_DATE('%Y%m%d', p.Arrival_Date) procedure_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.Arrival_Date ,'000000')) procedure_datetime,
            32817 procedure_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` p
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` pc on pc.source_value = REPLACE(p.Accident_And_Emergency_Investigation_Fifth, ' ', '') AND pc.destination_table = 'PROCEDURE_OCCURRENCE' AND pc.source_table = 'SUS_BRI_AE_010415_to_300619' AND pc.source_column = 'Accident_And_Emergency_Investigation_Fifth'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl ON vl.source_value = CONCAT(CAST(p.AandE_Attendance_Number as STRING),'_', p.person_id)
        WHERE p.Arrival_Date IS NOT NULL
        AND p.person_id IS NOT NULL
        AND p.Accident_And_Emergency_Investigation_Fifth IS NOT NULL
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.procedure_occurrence` op ON 
        op.person_id = p.person_id 
        AND op.procedure_concept_id = p.procedure_concept_id
        AND op.procedure_source_value = p.procedure_source_value
        AND COALESCE(op.procedure_source_concept_id, -1) = -1
        AND op.procedure_datetime = p.procedure_datetime
        AND op.procedure_type_concept_id = p.procedure_type_concept_id
        AND COALESCE(op.provider_id, -1) = -1
        AND COALESCE(op.visit_occurrence_id, -1) = COALESCE(p.visit_occurrence_id, -1)
        AND COALESCE(op.visit_detail_id, -1) = -1
    WHERE op.procedure_occurrence_id IS NULL;         

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added procedure_occurrence', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Fifth -> procedure_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Sixth] Transfer
    */
    SET max_current_id = COALESCE((SELECT procedure_occurrence_id FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.procedure_occurrence`
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_source_value,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as procedure_occurrence_id,
        p.person_id,
        p.procedure_concept_id,
        p.procedure_source_value,
        p.procedure_date,
        p.procedure_datetime,
        p.procedure_type_concept_id,
        p.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            p.person_id,
            pc.concept_id procedure_concept_id,
            Accident_And_Emergency_Investigation_Sixth procedure_source_value,	
            PARSE_DATE('%Y%m%d', p.Arrival_Date) procedure_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.Arrival_Date ,'000000')) procedure_datetime,
            32817 procedure_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` p
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` pc on pc.source_value = REPLACE(p.Accident_And_Emergency_Investigation_Sixth, ' ', '') AND pc.destination_table = 'PROCEDURE_OCCURRENCE' AND pc.source_table = 'SUS_BRI_AE_010415_to_300619' AND pc.source_column = 'Accident_And_Emergency_Investigation_Sixth'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl ON vl.source_value = CONCAT(CAST(p.AandE_Attendance_Number as STRING),'_', p.person_id)
        WHERE p.Arrival_Date IS NOT NULL
        AND p.person_id IS NOT NULL
        AND p.Accident_And_Emergency_Investigation_Sixth IS NOT NULL
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.procedure_occurrence` op ON 
        op.person_id = p.person_id 
        AND op.procedure_concept_id = p.procedure_concept_id
        AND op.procedure_source_value = p.procedure_source_value
        AND COALESCE(op.procedure_source_concept_id, -1) = -1
        AND op.procedure_datetime = p.procedure_datetime
        AND op.procedure_type_concept_id = p.procedure_type_concept_id
        AND COALESCE(op.provider_id, -1) = -1
        AND COALESCE(op.visit_occurrence_id, -1) = COALESCE(p.visit_occurrence_id, -1)
        AND COALESCE(op.visit_detail_id, -1) = -1
    WHERE op.procedure_occurrence_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added procedure_occurrence', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Sixth -> procedure_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Seventh] Transfer
    */
    SET max_current_id = COALESCE((SELECT procedure_occurrence_id FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.procedure_occurrence`
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_source_value,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as procedure_occurrence_id,
        p.person_id,
        p.procedure_concept_id,
        p.procedure_source_value,
        p.procedure_date,
        p.procedure_datetime,
        p.procedure_type_concept_id,
        p.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            p.person_id,
            pc.concept_id procedure_concept_id,
            Accident_And_Emergency_Investigation_Seventh procedure_source_value,	
            PARSE_DATE('%Y%m%d', p.Arrival_Date) procedure_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.Arrival_Date ,'000000')) procedure_datetime,
            32817 procedure_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` p
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` pc on pc.source_value = REPLACE(p.Accident_And_Emergency_Investigation_Seventh, ' ', '') AND pc.destination_table = 'PROCEDURE_OCCURRENCE' AND pc.source_table = 'SUS_BRI_AE_010415_to_300619' AND pc.source_column = 'Accident_And_Emergency_Investigation_Seventh'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl ON vl.source_value = CONCAT(CAST(p.AandE_Attendance_Number as STRING),'_', p.person_id)
        WHERE p.Arrival_Date IS NOT NULL
        AND p.person_id IS NOT NULL
        AND p.Accident_And_Emergency_Investigation_Seventh IS NOT NULL
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.procedure_occurrence` op ON 
        op.person_id = p.person_id 
        AND op.procedure_concept_id = p.procedure_concept_id
        AND op.procedure_source_value = p.procedure_source_value
        AND COALESCE(op.procedure_source_concept_id, -1) = -1
        AND op.procedure_datetime = p.procedure_datetime
        AND op.procedure_type_concept_id = p.procedure_type_concept_id
        AND COALESCE(op.provider_id, -1) = -1
        AND COALESCE(op.visit_occurrence_id, -1) = COALESCE(p.visit_occurrence_id, -1)
        AND COALESCE(op.visit_detail_id, -1) = -1
    WHERE op.procedure_occurrence_id IS NULL;      

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added procedure_occurrence', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Seventh -> procedure_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Eighth] Transfer
    */
    SET max_current_id = COALESCE((SELECT procedure_occurrence_id FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.procedure_occurrence`
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_source_value,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as procedure_occurrence_id,
        p.person_id,
        p.procedure_concept_id,
        p.procedure_source_value,
        p.procedure_date,
        p.procedure_datetime,
        p.procedure_type_concept_id,
        p.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            p.person_id,
            pc.concept_id procedure_concept_id,
            Accident_And_Emergency_Investigation_Eighth procedure_source_value,	
            PARSE_DATE('%Y%m%d', p.Arrival_Date) procedure_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.Arrival_Date ,'000000')) procedure_datetime,
            32817 procedure_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` p
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` pc on pc.source_value = REPLACE(p.Accident_And_Emergency_Investigation_Eighth, ' ', '') AND pc.destination_table = 'PROCEDURE_OCCURRENCE' AND pc.source_table = 'SUS_BRI_AE_010415_to_300619' AND pc.source_column = 'Accident_And_Emergency_Investigation_Eighth'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl ON vl.source_value = CONCAT(CAST(p.AandE_Attendance_Number as STRING),'_', p.person_id)
        WHERE p.Arrival_Date IS NOT NULL
        AND p.person_id IS NOT NULL
        AND p.Accident_And_Emergency_Investigation_Eighth IS NOT NULL
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.procedure_occurrence` op ON 
        op.person_id = p.person_id 
        AND op.procedure_concept_id = p.procedure_concept_id
        AND op.procedure_source_value = p.procedure_source_value
        AND COALESCE(op.procedure_source_concept_id, -1) = -1
        AND op.procedure_datetime = p.procedure_datetime
        AND op.procedure_type_concept_id = p.procedure_type_concept_id
        AND COALESCE(op.provider_id, -1) = -1
        AND COALESCE(op.visit_occurrence_id, -1) = COALESCE(p.visit_occurrence_id, -1)
        AND COALESCE(op.visit_detail_id, -1) = -1
    WHERE op.procedure_occurrence_id IS NULL;     

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added procedure_occurrence', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Eighth -> procedure_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Ninth] Transfer
    */
    SET max_current_id = COALESCE((SELECT procedure_occurrence_id FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.procedure_occurrence`
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_source_value,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as procedure_occurrence_id,
        p.person_id,
        p.procedure_concept_id,
        p.procedure_source_value,
        p.procedure_date,
        p.procedure_datetime,
        p.procedure_type_concept_id,
        p.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            p.person_id,
            pc.concept_id procedure_concept_id,
            Accident_And_Emergency_Investigation_Ninth procedure_source_value,	
            PARSE_DATE('%Y%m%d', p.Arrival_Date) procedure_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.Arrival_Date ,'000000')) procedure_datetime,
            32817 procedure_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` p
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` pc on pc.source_value = REPLACE(p.Accident_And_Emergency_Investigation_Ninth, ' ', '') AND pc.destination_table = 'PROCEDURE_OCCURRENCE' AND pc.source_table = 'SUS_BRI_AE_010415_to_300619' AND pc.source_column = 'Accident_And_Emergency_Investigation_Ninth'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl ON vl.source_value = CONCAT(CAST(p.AandE_Attendance_Number as STRING),'_', p.person_id)
        WHERE p.Arrival_Date IS NOT NULL
        AND p.person_id IS NOT NULL
        AND p.Accident_And_Emergency_Investigation_Ninth IS NOT NULL
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.procedure_occurrence` op ON 
        op.person_id = p.person_id 
        AND op.procedure_concept_id = p.procedure_concept_id
        AND op.procedure_source_value = p.procedure_source_value
        AND COALESCE(op.procedure_source_concept_id, -1) = -1
        AND op.procedure_datetime = p.procedure_datetime
        AND op.procedure_type_concept_id = p.procedure_type_concept_id
        AND COALESCE(op.provider_id, -1) = -1
        AND COALESCE(op.visit_occurrence_id, -1) = COALESCE(p.visit_occurrence_id, -1)
        AND COALESCE(op.visit_detail_id, -1) = -1
    WHERE op.procedure_occurrence_id IS NULL;       

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added procedure_occurrence', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Ninth -> procedure_occurrence', @@row_count, CURRENT_DATETIME();
     
    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Tenth] Transfer
    */
    SET max_current_id = COALESCE((SELECT procedure_occurrence_id FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.procedure_occurrence`
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_source_value,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as procedure_occurrence_id,
        p.person_id,
        p.procedure_concept_id,
        p.procedure_source_value,
        p.procedure_date,
        p.procedure_datetime,
        p.procedure_type_concept_id,
        p.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            p.person_id,
            pc.concept_id procedure_concept_id,
            Accident_And_Emergency_Investigation_Tenth procedure_source_value,	
            PARSE_DATE('%Y%m%d', p.Arrival_Date) procedure_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.Arrival_Date ,'000000')) procedure_datetime,
            32817 procedure_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` p
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` pc on pc.source_value = REPLACE(p.Accident_And_Emergency_Investigation_Tenth, ' ', '') AND pc.destination_table = 'PROCEDURE_OCCURRENCE' AND pc.source_table = 'SUS_BRI_AE_010415_to_300619' AND pc.source_column = 'Accident_And_Emergency_Investigation_Tenth'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl ON vl.source_value = CONCAT(CAST(p.AandE_Attendance_Number as STRING),'_', p.person_id)
        WHERE p.Arrival_Date IS NOT NULL
        AND p.person_id IS NOT NULL
        AND p.Accident_And_Emergency_Investigation_Tenth IS NOT NULL
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.procedure_occurrence` op ON 
        op.person_id = p.person_id 
        AND op.procedure_concept_id = p.procedure_concept_id
        AND op.procedure_source_value = p.procedure_source_value
        AND COALESCE(op.procedure_source_concept_id, -1) = -1
        AND op.procedure_datetime = p.procedure_datetime
        AND op.procedure_type_concept_id = p.procedure_type_concept_id
        AND COALESCE(op.provider_id, -1) = -1
        AND COALESCE(op.visit_occurrence_id, -1) = COALESCE(p.visit_occurrence_id, -1)
        AND COALESCE(op.visit_detail_id, -1) = -1
    WHERE op.procedure_occurrence_id IS NULL;      

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added procedure_occurrence', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Tenth -> procedure_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Eleventh] Transfer
    */
    SET max_current_id = COALESCE((SELECT procedure_occurrence_id FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.procedure_occurrence`
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_source_value,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as procedure_occurrence_id,
        p.person_id,
        p.procedure_concept_id,
        p.procedure_source_value,
        p.procedure_date,
        p.procedure_datetime,
        p.procedure_type_concept_id,
        p.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            p.person_id,
            pc.concept_id procedure_concept_id,
            Accident_And_Emergency_Investigation_Eleventh procedure_source_value,	
            PARSE_DATE('%Y%m%d', p.Arrival_Date) procedure_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.Arrival_Date ,'000000')) procedure_datetime,
            32817 procedure_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` p
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` pc on pc.source_value = REPLACE(p.Accident_And_Emergency_Investigation_Eleventh, ' ', '') AND pc.destination_table = 'PROCEDURE_OCCURRENCE' AND pc.source_table = 'SUS_BRI_AE_010415_to_300619' AND pc.source_column = 'Accident_And_Emergency_Investigation_Eleventh'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl ON vl.source_value = CONCAT(CAST(p.AandE_Attendance_Number as STRING),'_', p.person_id)
        WHERE p.Arrival_Date IS NOT NULL
        AND p.person_id IS NOT NULL
        AND p.Accident_And_Emergency_Investigation_Eleventh IS NOT NULL
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.procedure_occurrence` op ON 
        op.person_id = p.person_id 
        AND op.procedure_concept_id = p.procedure_concept_id
        AND op.procedure_source_value = p.procedure_source_value
        AND COALESCE(op.procedure_source_concept_id, -1) = -1
        AND op.procedure_datetime = p.procedure_datetime
        AND op.procedure_type_concept_id = p.procedure_type_concept_id
        AND COALESCE(op.provider_id, -1) = -1
        AND COALESCE(op.visit_occurrence_id, -1) = COALESCE(p.visit_occurrence_id, -1)
        AND COALESCE(op.visit_detail_id, -1) = -1
    WHERE op.procedure_occurrence_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added procedure_occurrence', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Eleventh -> procedure_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Twelfth] Transfer
    */
    SET max_current_id = COALESCE((SELECT procedure_occurrence_id FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.procedure_occurrence`
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_source_value,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as procedure_occurrence_id,
        p.person_id,
        p.procedure_concept_id,
        p.procedure_source_value,
        p.procedure_date,
        p.procedure_datetime,
        p.procedure_type_concept_id,
        p.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            p.person_id,
            pc.concept_id procedure_concept_id,
            Accident_And_Emergency_Investigation_Twelfth procedure_source_value,	
            PARSE_DATE('%Y%m%d', p.Arrival_Date) procedure_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.Arrival_Date ,'000000')) procedure_datetime,
            32817 procedure_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` p
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` pc on pc.source_value = REPLACE(p.Accident_And_Emergency_Investigation_Twelfth, ' ', '') AND pc.destination_table = 'PROCEDURE_OCCURRENCE' AND pc.source_table = 'SUS_BRI_AE_010415_to_300619' AND pc.source_column = 'Accident_And_Emergency_Investigation_Twelfth'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl ON vl.source_value = CONCAT(CAST(p.AandE_Attendance_Number as STRING),'_', p.person_id)
        WHERE p.Arrival_Date IS NOT NULL
        AND p.person_id IS NOT NULL
        AND p.Accident_And_Emergency_Investigation_Twelfth IS NOT NULL
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.procedure_occurrence` op ON 
        op.person_id = p.person_id 
        AND op.procedure_concept_id = p.procedure_concept_id
        AND op.procedure_source_value = p.procedure_source_value
        AND COALESCE(op.procedure_source_concept_id, -1) = -1
        AND op.procedure_datetime = p.procedure_datetime
        AND op.procedure_type_concept_id = p.procedure_type_concept_id
        AND COALESCE(op.provider_id, -1) = -1
        AND COALESCE(op.visit_occurrence_id, -1) = COALESCE(p.visit_occurrence_id, -1)
        AND COALESCE(op.visit_detail_id, -1) = -1
    WHERE op.procedure_occurrence_id IS NULL;         

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added procedure_occurrence', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Twelfth -> procedure_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_First] Transfer
    */
    SET max_current_id = COALESCE((SELECT procedure_occurrence_id FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.procedure_occurrence`
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_source_value,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as procedure_occurrence_id,
        p.person_id,
        p.procedure_concept_id,
        p.procedure_source_value,
        p.procedure_date,
        p.procedure_datetime,
        p.procedure_type_concept_id,
        p.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
                p.person_id,
                pc.concept_id procedure_concept_id,
                Accident_And_Emergency_Treatment_First procedure_source_value,	
                PARSE_DATE('%Y%m%d', p.Arrival_Date) procedure_date,
                PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.Arrival_Date ,'000000')) procedure_datetime,
                32817 procedure_type_concept_id, --EHR
                vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` p
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` pc on pc.source_value = REPLACE(p.Accident_And_Emergency_Treatment_First, ' ', '') AND pc.destination_table = 'PROCEDURE_OCCURRENCE' AND pc.source_table = 'SUS_BRI_AE_010415_to_300619' AND pc.source_column = 'Accident_And_Emergency_Treatment_First'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl ON vl.source_value = CONCAT(CAST(p.AandE_Attendance_Number as STRING),'_', p.person_id)
        WHERE p.Arrival_Date IS NOT NULL
        AND p.person_id IS NOT NULL
        AND p.Accident_And_Emergency_Treatment_First IS NOT NULL
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.procedure_occurrence` op ON 
        op.person_id = p.person_id 
        AND op.procedure_concept_id = p.procedure_concept_id
        AND op.procedure_source_value = p.procedure_source_value
        AND COALESCE(op.procedure_source_concept_id, -1) = -1
        AND op.procedure_datetime = p.procedure_datetime
        AND op.procedure_type_concept_id = p.procedure_type_concept_id
        AND COALESCE(op.provider_id, -1) = -1
        AND COALESCE(op.visit_occurrence_id, -1) = COALESCE(p.visit_occurrence_id, -1)
        AND COALESCE(op.visit_detail_id, -1) = -1
    WHERE op.procedure_occurrence_id IS NULL;      

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added procedure_occurrence', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_First -> procedure_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Second] Transfer
    */
    SET max_current_id = COALESCE((SELECT procedure_occurrence_id FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.procedure_occurrence`
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_source_value,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as procedure_occurrence_id,
        p.person_id,
        p.procedure_concept_id,
        p.procedure_source_value,
        p.procedure_date,
        p.procedure_datetime,
        p.procedure_type_concept_id,
        p.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            p.person_id,
            pc.concept_id procedure_concept_id,
            Accident_And_Emergency_Treatment_Second procedure_source_value,	
            PARSE_DATE('%Y%m%d', p.Arrival_Date) procedure_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.Arrival_Date ,'000000')) procedure_datetime,
            32817 procedure_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` p
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` pc on pc.source_value = REPLACE(p.Accident_And_Emergency_Treatment_Second, ' ', '') AND pc.destination_table = 'PROCEDURE_OCCURRENCE' AND pc.source_table = 'SUS_BRI_AE_010415_to_300619' AND pc.source_column = 'Accident_And_Emergency_Treatment_Second'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl ON vl.source_value = CONCAT(CAST(p.AandE_Attendance_Number as STRING),'_', p.person_id)
        WHERE p.Arrival_Date IS NOT NULL
        AND p.person_id IS NOT NULL
        AND p.Accident_And_Emergency_Treatment_Second IS NOT NULL
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.procedure_occurrence` op ON 
        op.person_id = p.person_id 
        AND op.procedure_concept_id = p.procedure_concept_id
        AND op.procedure_source_value = p.procedure_source_value
        AND COALESCE(op.procedure_source_concept_id, -1) = -1
        AND op.procedure_datetime = p.procedure_datetime
        AND op.procedure_type_concept_id = p.procedure_type_concept_id
        AND COALESCE(op.provider_id, -1) = -1
        AND COALESCE(op.visit_occurrence_id, -1) = COALESCE(p.visit_occurrence_id, -1)
        AND COALESCE(op.visit_detail_id, -1) = -1
    WHERE op.procedure_occurrence_id IS NULL;   

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added procedure_occurrence', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Second -> procedure_occurrence', @@row_count, CURRENT_DATETIME();
     
    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Third] Transfer
    */
    SET max_current_id = COALESCE((SELECT procedure_occurrence_id FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.procedure_occurrence`
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_source_value,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as procedure_occurrence_id,
        p.person_id,
        p.procedure_concept_id,
        p.procedure_source_value,
        p.procedure_date,
        p.procedure_datetime,
        p.procedure_type_concept_id,
        p.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            p.person_id,
            pc.concept_id procedure_concept_id,
            Accident_And_Emergency_Treatment_Third procedure_source_value,	
            PARSE_DATE('%Y%m%d', p.Arrival_Date) procedure_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.Arrival_Date ,'000000')) procedure_datetime,
            32817 procedure_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` p
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` pc on pc.source_value = REPLACE(p.Accident_And_Emergency_Treatment_Third, ' ', '') AND pc.destination_table = 'PROCEDURE_OCCURRENCE' AND pc.source_table = 'SUS_BRI_AE_010415_to_300619' AND pc.source_column = 'Accident_And_Emergency_Treatment_Third'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl ON vl.source_value = CONCAT(CAST(p.AandE_Attendance_Number as STRING),'_', p.person_id)
        WHERE p.Arrival_Date IS NOT NULL
        AND p.person_id IS NOT NULL
        AND p.Accident_And_Emergency_Treatment_Third IS NOT NULL
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.procedure_occurrence` op ON 
        op.person_id = p.person_id 
        AND op.procedure_concept_id = p.procedure_concept_id
        AND op.procedure_source_value = p.procedure_source_value
        AND COALESCE(op.procedure_source_concept_id, -1) = -1
        AND op.procedure_datetime = p.procedure_datetime
        AND op.procedure_type_concept_id = p.procedure_type_concept_id
        AND COALESCE(op.provider_id, -1) = -1
        AND COALESCE(op.visit_occurrence_id, -1) = COALESCE(p.visit_occurrence_id, -1)
        AND COALESCE(op.visit_detail_id, -1) = -1
    WHERE op.procedure_occurrence_id IS NULL; 

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added procedure_occurrence', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Third -> procedure_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Fourth] Transfer
    */
    SET max_current_id = COALESCE((SELECT procedure_occurrence_id FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.procedure_occurrence`
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_source_value,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as procedure_occurrence_id,
        p.person_id,
        p.procedure_concept_id,
        p.procedure_source_value,
        p.procedure_date,
        p.procedure_datetime,
        p.procedure_type_concept_id,
        p.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            p.person_id,
            pc.concept_id procedure_concept_id,
            Accident_And_Emergency_Treatment_Fourth procedure_source_value,	
            PARSE_DATE('%Y%m%d', p.Arrival_Date) procedure_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.Arrival_Date ,'000000')) procedure_datetime,
            32817 procedure_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` p
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` pc on pc.source_value = REPLACE(p.Accident_And_Emergency_Treatment_Fourth, ' ', '') AND pc.destination_table = 'PROCEDURE_OCCURRENCE' AND pc.source_table = 'SUS_BRI_AE_010415_to_300619' AND pc.source_column = 'Accident_And_Emergency_Treatment_Fourth'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl ON vl.source_value = CONCAT(CAST(p.AandE_Attendance_Number as STRING),'_', p.person_id)
        WHERE p.Arrival_Date IS NOT NULL
        AND p.person_id IS NOT NULL
        AND p.Accident_And_Emergency_Treatment_Fourth IS NOT NULL
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.procedure_occurrence` op ON 
        op.person_id = p.person_id 
        AND op.procedure_concept_id = p.procedure_concept_id
        AND op.procedure_source_value = p.procedure_source_value
        AND COALESCE(op.procedure_source_concept_id, -1) = -1
        AND op.procedure_datetime = p.procedure_datetime
        AND op.procedure_type_concept_id = p.procedure_type_concept_id
        AND COALESCE(op.provider_id, -1) = -1
        AND COALESCE(op.visit_occurrence_id, -1) = COALESCE(p.visit_occurrence_id, -1)
        AND COALESCE(op.visit_detail_id, -1) = -1
    WHERE op.procedure_occurrence_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added procedure_occurrence', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Fourth -> procedure_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Five] Transfer
    */
    SET max_current_id = COALESCE((SELECT procedure_occurrence_id FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.procedure_occurrence`
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_source_value,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as procedure_occurrence_id,
        p.person_id,
        p.procedure_concept_id,
        p.procedure_source_value,
        p.procedure_date,
        p.procedure_datetime,
        p.procedure_type_concept_id,
        p.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            p.person_id,
            pc.concept_id procedure_concept_id,
            Accident_And_Emergency_Treatment_Fifth procedure_source_value,	
            PARSE_DATE('%Y%m%d', p.Arrival_Date) procedure_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.Arrival_Date ,'000000')) procedure_datetime,
            32817 procedure_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` p
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` pc on pc.source_value = REPLACE(p.Accident_And_Emergency_Treatment_Fifth, ' ', '') AND pc.destination_table = 'PROCEDURE_OCCURRENCE' AND pc.source_table = 'SUS_BRI_AE_010415_to_300619' AND pc.source_column = 'Accident_And_Emergency_Treatment_Fifth'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl ON vl.source_value = CONCAT(CAST(p.AandE_Attendance_Number as STRING),'_', p.person_id)
        WHERE p.Arrival_Date IS NOT NULL
        AND p.person_id IS NOT NULL
        AND p.Accident_And_Emergency_Treatment_Fifth IS NOT NULL
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.procedure_occurrence` op ON 
        op.person_id = p.person_id 
        AND op.procedure_concept_id = p.procedure_concept_id
        AND op.procedure_source_value = p.procedure_source_value
        AND COALESCE(op.procedure_source_concept_id, -1) = -1
        AND op.procedure_datetime = p.procedure_datetime
        AND op.procedure_type_concept_id = p.procedure_type_concept_id
        AND COALESCE(op.provider_id, -1) = -1
        AND COALESCE(op.visit_occurrence_id, -1) = COALESCE(p.visit_occurrence_id, -1)
        AND COALESCE(op.visit_detail_id, -1) = -1
    WHERE op.procedure_occurrence_id IS NULL;  

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added procedure_occurrence', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Five -> procedure_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Sixth] Transfer
    */
    SET max_current_id = COALESCE((SELECT procedure_occurrence_id FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.procedure_occurrence`
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_source_value,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as procedure_occurrence_id,
        p.person_id,
        p.procedure_concept_id,
        p.procedure_source_value,
        p.procedure_date,
        p.procedure_datetime,
        p.procedure_type_concept_id,
        p.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            p.person_id,
            pc.concept_id procedure_concept_id,
            Accident_And_Emergency_Treatment_Sixth procedure_source_value,	
            PARSE_DATE('%Y%m%d', p.Arrival_Date) procedure_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.Arrival_Date ,'000000')) procedure_datetime,
            32817 procedure_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` p
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` pc on pc.source_value = REPLACE(p.Accident_And_Emergency_Treatment_Sixth, ' ', '') AND pc.destination_table = 'PROCEDURE_OCCURRENCE' AND pc.source_table = 'SUS_BRI_AE_010415_to_300619' AND pc.source_column = 'Accident_And_Emergency_Treatment_Sixth'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl ON vl.source_value = CONCAT(CAST(p.AandE_Attendance_Number as STRING),'_', p.person_id)
        WHERE p.Arrival_Date IS NOT NULL
        AND p.person_id IS NOT NULL
        AND p.Accident_And_Emergency_Treatment_Sixth IS NOT NULL
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.procedure_occurrence` op ON 
        op.person_id = p.person_id 
        AND op.procedure_concept_id = p.procedure_concept_id
        AND op.procedure_source_value = p.procedure_source_value
        AND COALESCE(op.procedure_source_concept_id, -1) = -1
        AND op.procedure_datetime = p.procedure_datetime
        AND op.procedure_type_concept_id = p.procedure_type_concept_id
        AND COALESCE(op.provider_id, -1) = -1
        AND COALESCE(op.visit_occurrence_id, -1) = COALESCE(p.visit_occurrence_id, -1)
        AND COALESCE(op.visit_detail_id, -1) = -1
    WHERE op.procedure_occurrence_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added procedure_occurrence', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Sixth -> procedure_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Seventh] Transfer
    */
    SET max_current_id = COALESCE((SELECT procedure_occurrence_id FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.procedure_occurrence`
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_source_value,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as procedure_occurrence_id,
        p.person_id,
        p.procedure_concept_id,
        p.procedure_source_value,
        p.procedure_date,
        p.procedure_datetime,
        p.procedure_type_concept_id,
        p.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            p.person_id,
            pc.concept_id procedure_concept_id,
            Accident_And_Emergency_Treatment_Seventh procedure_source_value,	
            PARSE_DATE('%Y%m%d', p.Arrival_Date) procedure_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.Arrival_Date ,'000000')) procedure_datetime,
            32817 procedure_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` p
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` pc on pc.source_value = REPLACE(p.Accident_And_Emergency_Treatment_Seventh, ' ', '') AND pc.destination_table = 'PROCEDURE_OCCURRENCE' AND pc.source_table = 'SUS_BRI_AE_010415_to_300619' AND pc.source_column = 'Accident_And_Emergency_Treatment_Seventh'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl ON vl.source_value = CONCAT(CAST(p.AandE_Attendance_Number as STRING),'_', p.person_id)
        WHERE p.Arrival_Date IS NOT NULL
        AND p.person_id IS NOT NULL
        AND p.Accident_And_Emergency_Treatment_Seventh IS NOT NULL
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.procedure_occurrence` op ON 
        op.person_id = p.person_id 
        AND op.procedure_concept_id = p.procedure_concept_id
        AND op.procedure_source_value = p.procedure_source_value
        AND COALESCE(op.procedure_source_concept_id, -1) = -1
        AND op.procedure_datetime = p.procedure_datetime
        AND op.procedure_type_concept_id = p.procedure_type_concept_id
        AND COALESCE(op.provider_id, -1) = -1
        AND COALESCE(op.visit_occurrence_id, -1) = COALESCE(p.visit_occurrence_id, -1)
        AND COALESCE(op.visit_detail_id, -1) = -1
    WHERE op.procedure_occurrence_id IS NULL;     

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added procedure_occurrence', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Seventh -> procedure_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Eighth] Transfer
    */
    SET max_current_id = COALESCE((SELECT procedure_occurrence_id FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.procedure_occurrence`
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_source_value,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as procedure_occurrence_id,
        p.person_id,
        p.procedure_concept_id,
        p.procedure_source_value,
        p.procedure_date,
        p.procedure_datetime,
        p.procedure_type_concept_id,
        p.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            p.person_id,
            pc.concept_id procedure_concept_id,
            Accident_And_Emergency_Treatment_Eighth procedure_source_value,	
            PARSE_DATE('%Y%m%d', p.Arrival_Date) procedure_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.Arrival_Date ,'000000')) procedure_datetime,
            32817 procedure_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` p
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` pc on pc.source_value = REPLACE(p.Accident_And_Emergency_Treatment_Eighth, ' ', '') AND pc.destination_table = 'PROCEDURE_OCCURRENCE' AND pc.source_table = 'SUS_BRI_AE_010415_to_300619' AND pc.source_column = 'Accident_And_Emergency_Treatment_Eighth'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl ON vl.source_value = CONCAT(CAST(p.AandE_Attendance_Number as STRING),'_', p.person_id)
        WHERE p.Arrival_Date IS NOT NULL
        AND p.person_id IS NOT NULL
        AND p.Accident_And_Emergency_Treatment_Eighth IS NOT NULL
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.procedure_occurrence` op ON 
        op.person_id = p.person_id 
        AND op.procedure_concept_id = p.procedure_concept_id
        AND op.procedure_source_value = p.procedure_source_value
        AND COALESCE(op.procedure_source_concept_id, -1) = -1
        AND op.procedure_datetime = p.procedure_datetime
        AND op.procedure_type_concept_id = p.procedure_type_concept_id
        AND COALESCE(op.provider_id, -1) = -1
        AND COALESCE(op.visit_occurrence_id, -1) = COALESCE(p.visit_occurrence_id, -1)
        AND COALESCE(op.visit_detail_id, -1) = -1
    WHERE op.procedure_occurrence_id IS NULL; 

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added procedure_occurrence', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Eighth -> procedure_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Ninth] Transfer
    */
    SET max_current_id = COALESCE((SELECT procedure_occurrence_id FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.procedure_occurrence`
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_source_value,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as procedure_occurrence_id,
        p.person_id,
        p.procedure_concept_id,
        p.procedure_source_value,
        p.procedure_date,
        p.procedure_datetime,
        p.procedure_type_concept_id,
        p.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            p.person_id,
            pc.concept_id procedure_concept_id,
            Accident_And_Emergency_Treatment_Ninth procedure_source_value,	
            PARSE_DATE('%Y%m%d', p.Arrival_Date) procedure_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.Arrival_Date ,'000000')) procedure_datetime,
            32817 procedure_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` p
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` pc on pc.source_value = REPLACE(p.Accident_And_Emergency_Treatment_Ninth, ' ', '') AND pc.destination_table = 'PROCEDURE_OCCURRENCE' AND pc.source_table = 'SUS_BRI_AE_010415_to_300619' AND pc.source_column = 'Accident_And_Emergency_Treatment_Ninth'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl ON vl.source_value = CONCAT(CAST(p.AandE_Attendance_Number as STRING),'_', p.person_id)
        WHERE p.Arrival_Date IS NOT NULL
        AND p.person_id IS NOT NULL
        AND p.Accident_And_Emergency_Treatment_Ninth IS NOT NULL
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.procedure_occurrence` op ON 
        op.person_id = p.person_id 
        AND op.procedure_concept_id = p.procedure_concept_id
        AND op.procedure_source_value = p.procedure_source_value
        AND COALESCE(op.procedure_source_concept_id, -1) = -1
        AND op.procedure_datetime = p.procedure_datetime
        AND op.procedure_type_concept_id = p.procedure_type_concept_id
        AND COALESCE(op.provider_id, -1) = -1
        AND COALESCE(op.visit_occurrence_id, -1) = COALESCE(p.visit_occurrence_id, -1)
        AND COALESCE(op.visit_detail_id, -1) = -1
    WHERE op.procedure_occurrence_id IS NULL;  

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added procedure_occurrence', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Ninth -> procedure_occurrence', @@row_count, CURRENT_DATETIME();
     
    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Tenth] Transfer
    */
    SET max_current_id = COALESCE((SELECT procedure_occurrence_id FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.procedure_occurrence`
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_source_value,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as procedure_occurrence_id,
        p.person_id,
        p.procedure_concept_id,
        p.procedure_source_value,
        p.procedure_date,
        p.procedure_datetime,
        p.procedure_type_concept_id,
        p.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            p.person_id,
            pc.concept_id procedure_concept_id,
            Accident_And_Emergency_Treatment_Tenth procedure_source_value,	
            PARSE_DATE('%Y%m%d', p.Arrival_Date) procedure_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.Arrival_Date ,'000000')) procedure_datetime,
            32817 procedure_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` p
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` pc on pc.source_value = REPLACE(p.Accident_And_Emergency_Treatment_Tenth, ' ', '') AND pc.destination_table = 'PROCEDURE_OCCURRENCE' AND pc.source_table = 'SUS_BRI_AE_010415_to_300619' AND pc.source_column = 'Accident_And_Emergency_Treatment_Tenth'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl ON vl.source_value = CONCAT(CAST(p.AandE_Attendance_Number as STRING),'_', p.person_id)
        WHERE p.Arrival_Date IS NOT NULL
        AND p.person_id IS NOT NULL
        AND p.Accident_And_Emergency_Treatment_Tenth IS NOT NULL
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.procedure_occurrence` op ON 
        op.person_id = p.person_id 
        AND op.procedure_concept_id = p.procedure_concept_id
        AND op.procedure_source_value = p.procedure_source_value
        AND COALESCE(op.procedure_source_concept_id, -1) = -1
        AND op.procedure_datetime = p.procedure_datetime
        AND op.procedure_type_concept_id = p.procedure_type_concept_id
        AND COALESCE(op.provider_id, -1) = -1
        AND COALESCE(op.visit_occurrence_id, -1) = COALESCE(p.visit_occurrence_id, -1)
        AND COALESCE(op.visit_detail_id, -1) = -1
    WHERE op.procedure_occurrence_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added procedure_occurrence', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Tenth -> procedure_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Eleventh] Transfer
    */
    SET max_current_id = COALESCE((SELECT procedure_occurrence_id FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.procedure_occurrence`
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_source_value,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as procedure_occurrence_id,
        p.person_id,
        p.procedure_concept_id,
        p.procedure_source_value,
        p.procedure_date,
        p.procedure_datetime,
        p.procedure_type_concept_id,
        p.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            p.person_id,
            pc.concept_id procedure_concept_id,
            Accident_And_Emergency_Treatment_Eleventh procedure_source_value,	
            PARSE_DATE('%Y%m%d', p.Arrival_Date) procedure_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.Arrival_Date ,'000000')) procedure_datetime,
            32817 procedure_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` p
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` pc on pc.source_value = REPLACE(p.Accident_And_Emergency_Treatment_Eleventh, ' ', '') AND pc.destination_table = 'PROCEDURE_OCCURRENCE' AND pc.source_table = 'SUS_BRI_AE_010415_to_300619' AND pc.source_column = 'Accident_And_Emergency_Treatment_Eleventh'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl ON vl.source_value = CONCAT(CAST(p.AandE_Attendance_Number as STRING),'_', p.person_id)
        WHERE p.Arrival_Date IS NOT NULL
        AND p.person_id IS NOT NULL
        AND p.Accident_And_Emergency_Treatment_Eleventh IS NOT NULL
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.procedure_occurrence` op ON 
        op.person_id = p.person_id 
        AND op.procedure_concept_id = p.procedure_concept_id
        AND op.procedure_source_value = p.procedure_source_value
        AND COALESCE(op.procedure_source_concept_id, -1) = -1
        AND op.procedure_datetime = p.procedure_datetime
        AND op.procedure_type_concept_id = p.procedure_type_concept_id
        AND COALESCE(op.provider_id, -1) = -1
        AND COALESCE(op.visit_occurrence_id, -1) = COALESCE(p.visit_occurrence_id, -1)
        AND COALESCE(op.visit_detail_id, -1) = -1
    WHERE op.procedure_occurrence_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added procedure_occurrence', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Eleventh -> procedure_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Twelfth] Transfer
    */
    SET max_current_id = COALESCE((SELECT procedure_occurrence_id FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.procedure_occurrence`
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_source_value,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as procedure_occurrence_id,
        p.person_id,
        p.procedure_concept_id,
        p.procedure_source_value,
        p.procedure_date,
        p.procedure_datetime,
        p.procedure_type_concept_id,
        p.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            p.person_id,
            pc.concept_id procedure_concept_id,
            Accident_And_Emergency_Treatment_Twelfth procedure_source_value,	
            PARSE_DATE('%Y%m%d', p.Arrival_Date) procedure_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.Arrival_Date ,'000000')) procedure_datetime,
            32817 procedure_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` p
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` pc on pc.source_value = REPLACE(p.Accident_And_Emergency_Treatment_Twelfth, ' ', '') AND pc.destination_table = 'PROCEDURE_OCCURRENCE' AND pc.source_table = 'SUS_BRI_AE_010415_to_300619' AND pc.source_column = 'Accident_And_Emergency_Treatment_Twelfth'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl ON vl.source_value = CONCAT(CAST(p.AandE_Attendance_Number as STRING),'_', p.person_id)
        WHERE p.Arrival_Date IS NOT NULL
        AND p.person_id IS NOT NULL
        AND p.Accident_And_Emergency_Treatment_Twelfth IS NOT NULL
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.procedure_occurrence` op ON 
        op.person_id = p.person_id 
        AND op.procedure_concept_id = p.procedure_concept_id
        AND op.procedure_source_value = p.procedure_source_value
        AND COALESCE(op.procedure_source_concept_id, -1) = -1
        AND op.procedure_datetime = p.procedure_datetime
        AND op.procedure_type_concept_id = p.procedure_type_concept_id
        AND COALESCE(op.provider_id, -1) = -1
        AND COALESCE(op.visit_occurrence_id, -1) = COALESCE(p.visit_occurrence_id, -1)
        AND COALESCE(op.visit_detail_id, -1) = -1
    WHERE op.procedure_occurrence_id IS NULL; 

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added procedure_occurrence', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Twelfth -> procedure_occurrence', @@row_count, CURRENT_DATETIME();

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    ------------[SUS_BRI_APC_010415_to_300619_P1]
    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

    /*
        --[SUS_BRI_APC_010415_to_300619_P1 - Primary_Procedure_OPCS] Transfer
    */
    SET max_current_id = COALESCE((SELECT procedure_occurrence_id FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.procedure_occurrence`
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_source_value,
        procedure_source_concept_id,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        visit_occurrence_id,
        visit_detail_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as procedure_occurrence_id,
        p.person_id,
        p.procedure_concept_id,
        p.procedure_source_value,
        p.procedure_source_concept_id,
        p.procedure_date,
        p.procedure_datetime,
        p.procedure_type_concept_id,
        p.visit_occurrence_id,
        p.visit_detail_id
    FROM
    (
        SELECT DISTINCT
            p.person_id person_id,
            dc.concept_id procedure_concept_id, 
            Primary_Procedure_OPCS procedure_source_value, 
            sc.concept_id  procedure_source_concept_id,
            CASE WHEN LENGTH(Primary_Procedure_Date_OPCS) = 6
                    THEN PARSE_DATE('%Y%m%d', CONCAT(p.Primary_Procedure_Date_OPCS, '01'))
                WHEN LENGTH(Primary_Procedure_Date_OPCS) = 8
                    THEN PARSE_DATE('%Y%m%d', p.Primary_Procedure_Date_OPCS)
            END procedure_date,
            CASE WHEN LENGTH(Primary_Procedure_Date_OPCS) = 6
                    THEN PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.Primary_Procedure_Date_OPCS ,'01000000'))
                WHEN LENGTH(Primary_Procedure_Date_OPCS) = 8
                    THEN PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.Primary_Procedure_Date_OPCS ,'000000'))
            END procedure_datetime,       
            32817 procedure_type_concept_id, --EHR
            vl.id visit_occurrence_id,
            vdl.id visit_detail_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1` p   
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = CONCAT(CAST(p.Hospital_Provider_Spell_Number as STRING),'_', p.person_id)
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups`) vdl On vdl.source_value = CONCAT(CAST(p.Hospital_Provider_Spell_Number as STRING),'_', p.person_id, '_', p.Episode_Number)
        JOIN `CY_CDM_VOCAB.concept` sc on REPLACE(sc.concept_code, '.', '') = p.Primary_Procedure_OPCS
        JOIN `CY_CDM_VOCAB.concept_relationship` cr on cr.concept_id_1 = sc.concept_id
        JOIN `CY_CDM_VOCAB.concept` dc on dc.concept_id = cr.concept_id_2    
        WHERE (p.Primary_Procedure_Date_OPCS IS NOT NULL AND Primary_Procedure_Date_OPCS NOT LIKE '% %' AND (LENGTH(Primary_Procedure_Date_OPCS) =6 OR LENGTH(Primary_Procedure_Date_OPCS) =8))
        AND p.person_id IS NOT NULL
        AND p.Primary_Procedure_OPCS IS NOT NULL
        AND cr.relationship_id = 'Maps to'
        AND sc.vocabulary_id = 'OPCS4'
        AND dc.vocabulary_id = 'SNOMED'
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.procedure_occurrence` op ON 
        op.person_id = p.person_id 
        AND op.procedure_concept_id = p.procedure_concept_id
        AND op.procedure_source_value = p.procedure_source_value
        AND COALESCE(op.procedure_source_concept_id, -1) = COALESCE(p.procedure_source_concept_id, -1)
        AND op.procedure_datetime = p.procedure_datetime
        AND op.procedure_type_concept_id = p.procedure_type_concept_id
        AND COALESCE(op.provider_id, -1) = -1
        AND COALESCE(op.visit_occurrence_id, -1) = COALESCE(p.visit_occurrence_id, -1)
        AND COALESCE(op.visit_detail_id, -1) = COALESCE(p.visit_detail_id, -1)
    WHERE op.procedure_occurrence_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added procedure_occurrence', run_id, 'SUS_BRI_APC_010415_to_300619_P1 - Primary_Procedure_OPCS -> procedure_occurrence', @@row_count, CURRENT_DATETIME();
    
    /*
        --[SUS_BRI_APC_010415_to_300619_P1 - a2nd_Procedure_OPCS] Transfer
    */
    SET max_current_id = COALESCE((SELECT procedure_occurrence_id FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.procedure_occurrence`
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_source_value,
        procedure_source_concept_id,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        visit_occurrence_id,
        visit_detail_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as procedure_occurrence_id,
        p.person_id,
        p.procedure_concept_id,
        p.procedure_source_value,
        p.procedure_source_concept_id,
        p.procedure_date,
        p.procedure_datetime,
        p.procedure_type_concept_id,
        p.visit_occurrence_id,
        p.visit_detail_id
    FROM
    (
        SELECT DISTINCT
            p.person_id person_id,
            dc.concept_id procedure_concept_id, 
            a2nd_Procedure_OPCS procedure_source_value, 
            sc.concept_id  procedure_source_concept_id,
            CASE WHEN LENGTH(a2nd_Procedure_Date_OPCS) = 6
                    THEN PARSE_DATE('%Y%m%d', CONCAT(p.a2nd_Procedure_Date_OPCS, '01'))
                WHEN LENGTH(a2nd_Procedure_Date_OPCS) = 8
                    THEN PARSE_DATE('%Y%m%d', p.a2nd_Procedure_Date_OPCS)
            END procedure_date,
            CASE WHEN LENGTH(a2nd_Procedure_Date_OPCS) = 6
                    THEN PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.a2nd_Procedure_Date_OPCS ,'01000000'))
                WHEN LENGTH(a2nd_Procedure_Date_OPCS) = 8
                    THEN PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.a2nd_Procedure_Date_OPCS ,'000000'))
            END procedure_datetime,       
            32817 procedure_type_concept_id, --EHR
            vl.id visit_occurrence_id,
            vdl.id visit_detail_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1` p   
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = CONCAT(CAST(p.Hospital_Provider_Spell_Number as STRING),'_', p.person_id)
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups`) vdl On vdl.source_value = CONCAT(CAST(p.Hospital_Provider_Spell_Number as STRING),'_', p.person_id, '_', p.Episode_Number)
        JOIN `CY_CDM_VOCAB.concept` sc on REPLACE(sc.concept_code, '.', '') = p.a2nd_Procedure_OPCS
        JOIN `CY_CDM_VOCAB.concept_relationship` cr on cr.concept_id_1 = sc.concept_id
        JOIN `CY_CDM_VOCAB.concept` dc on dc.concept_id = cr.concept_id_2    
        WHERE (p.a2nd_Procedure_Date_OPCS IS NOT NULL AND a2nd_Procedure_Date_OPCS NOT LIKE '% %' AND (LENGTH(a2nd_Procedure_OPCS) =6 OR LENGTH(a2nd_Procedure_Date_OPCS) =8))
        AND p.person_id IS NOT NULL
        AND p.a2nd_Procedure_OPCS IS NOT NULL
        AND cr.relationship_id = 'Maps to'
        AND sc.vocabulary_id = 'OPCS4'
        AND dc.vocabulary_id = 'SNOMED'
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.procedure_occurrence` op ON 
        op.person_id = p.person_id 
        AND op.procedure_concept_id = p.procedure_concept_id
        AND op.procedure_source_value = p.procedure_source_value
        AND COALESCE(op.procedure_source_concept_id, -1) = COALESCE(p.procedure_source_concept_id, -1)
        AND op.procedure_datetime = p.procedure_datetime
        AND op.procedure_type_concept_id = p.procedure_type_concept_id
        AND COALESCE(op.provider_id, -1) = -1
        AND COALESCE(op.visit_occurrence_id, -1) = COALESCE(p.visit_occurrence_id, -1)
        AND COALESCE(op.visit_detail_id, -1) = COALESCE(p.visit_detail_id, -1)
    WHERE op.procedure_occurrence_id IS NULL; 

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added procedure_occurrence', run_id, 'SUS_BRI_APC_010415_to_300619_P1 - a2nd_Procedure_OPCS -> procedure_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_APC_010415_to_300619_P1 - a3rd_Procedure_OPCS] Transfer
    */
    SET max_current_id = COALESCE((SELECT procedure_occurrence_id FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.procedure_occurrence`
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_source_value,
        procedure_source_concept_id,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        visit_occurrence_id,
        visit_detail_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as procedure_occurrence_id,
        p.person_id,
        p.procedure_concept_id,
        p.procedure_source_value,
        p.procedure_source_concept_id,
        p.procedure_date,
        p.procedure_datetime,
        p.procedure_type_concept_id,
        p.visit_occurrence_id,
        p.visit_detail_id
    FROM
    (
        SELECT DISTINCT
            p.person_id person_id,
            dc.concept_id procedure_concept_id, 
            p.a3rd_Procedure_OPCS procedure_source_value, 
            sc.concept_id  procedure_source_concept_id,
            CASE WHEN LENGTH(p.a3rd_Procedure_Date_OPCS) = 6
                    THEN PARSE_DATE('%Y%m%d', CONCAT(p.a3rd_Procedure_Date_OPCS, '01'))
                WHEN LENGTH(a3rd_Procedure_Date_OPCS) = 8
                    THEN PARSE_DATE('%Y%m%d', p.a3rd_Procedure_Date_OPCS)
            END procedure_date,
            CASE WHEN LENGTH(a3rd_Procedure_Date_OPCS) = 6
                    THEN PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.a3rd_Procedure_Date_OPCS ,'01000000'))
                WHEN LENGTH(a3rd_Procedure_Date_OPCS) = 8
                    THEN PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.a3rd_Procedure_Date_OPCS ,'000000'))
            END procedure_datetime,       
            32817 procedure_type_concept_id, --EHR
            vl.id visit_occurrence_id,
            vdl.id visit_detail_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1` p   
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = CONCAT(CAST(p.Hospital_Provider_Spell_Number as STRING),'_', p.person_id)
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups`) vdl On vdl.source_value = CONCAT(CAST(p.Hospital_Provider_Spell_Number as STRING),'_', p.person_id, '_', p.Episode_Number)
        JOIN `CY_CDM_VOCAB.concept` sc on REPLACE(sc.concept_code, '.', '') = p.a3rd_Procedure_OPCS
        JOIN `CY_CDM_VOCAB.concept_relationship` cr on cr.concept_id_1 = sc.concept_id
        JOIN `CY_CDM_VOCAB.concept` dc on dc.concept_id = cr.concept_id_2    
        WHERE (p.a3rd_Procedure_Date_OPCS IS NOT NULL AND p.a3rd_Procedure_Date_OPCS NOT LIKE '% %' AND (LENGTH(p.a3rd_Procedure_Date_OPCS) =6 OR LENGTH(p.a3rd_Procedure_Date_OPCS) =8))
        AND p.person_id IS NOT NULL
        AND p.a3rd_Procedure_OPCS IS NOT NULL
        AND cr.relationship_id = 'Maps to'
        AND sc.vocabulary_id = 'OPCS4'
        AND dc.vocabulary_id = 'SNOMED'
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.procedure_occurrence` op ON 
        op.person_id = p.person_id 
        AND op.procedure_concept_id = p.procedure_concept_id
        AND op.procedure_source_value = p.procedure_source_value
        AND COALESCE(op.procedure_source_concept_id, -1) = COALESCE(p.procedure_source_concept_id, -1)
        AND op.procedure_datetime = p.procedure_datetime
        AND op.procedure_type_concept_id = p.procedure_type_concept_id
        AND COALESCE(op.provider_id, -1) = -1
        AND COALESCE(op.visit_occurrence_id, -1) = COALESCE(p.visit_occurrence_id, -1)
        AND COALESCE(op.visit_detail_id, -1) = COALESCE(p.visit_detail_id, -1)
    WHERE op.procedure_occurrence_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added procedure_occurrence', run_id, 'SUS_BRI_APC_010415_to_300619_P1 - a3rd_Procedure_OPCS -> procedure_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_APC_010415_to_300619_P1 - a4th_Procedure_OPCS] Transfer
    */
    SET max_current_id = COALESCE((SELECT procedure_occurrence_id FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.procedure_occurrence`
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_source_value,
        procedure_source_concept_id,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        visit_occurrence_id,
        visit_detail_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as procedure_occurrence_id,
        p.person_id,
        p.procedure_concept_id,
        p.procedure_source_value,
        p.procedure_source_concept_id,
        p.procedure_date,
        p.procedure_datetime,
        p.procedure_type_concept_id,
        p.visit_occurrence_id,
        p.visit_detail_id
    FROM
    (
        SELECT DISTINCT
            p.person_id person_id,
            dc.concept_id procedure_concept_id, 
            p.a4th_Procedure_OPCS procedure_source_value, 
            sc.concept_id  procedure_source_concept_id,
            CASE WHEN LENGTH(p.a4th_Procedure_Date_OPCS) = 6
                    THEN PARSE_DATE('%Y%m%d', CONCAT(p.a4th_Procedure_Date_OPCS, '01'))
                WHEN LENGTH(a4th_Procedure_Date_OPCS) = 8
                    THEN PARSE_DATE('%Y%m%d', p.a4th_Procedure_Date_OPCS)
            END procedure_date,
            CASE WHEN LENGTH(a4th_Procedure_Date_OPCS) = 6
                    THEN PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.a4th_Procedure_Date_OPCS ,'01000000'))
                WHEN LENGTH(a4th_Procedure_Date_OPCS) = 8
                    THEN PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.a4th_Procedure_Date_OPCS ,'000000'))
            END procedure_datetime,       
            32817 procedure_type_concept_id, --EHR
            vl.id visit_occurrence_id,
            vdl.id visit_detail_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1` p   
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = CONCAT(CAST(p.Hospital_Provider_Spell_Number as STRING),'_', p.person_id)
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups`) vdl On vdl.source_value = CONCAT(CAST(p.Hospital_Provider_Spell_Number as STRING),'_', p.person_id, '_', p.Episode_Number)
        JOIN `CY_CDM_VOCAB.concept` sc on REPLACE(sc.concept_code, '.', '') = p.a4th_Procedure_OPCS
        JOIN `CY_CDM_VOCAB.concept_relationship` cr on cr.concept_id_1 = sc.concept_id
        JOIN `CY_CDM_VOCAB.concept` dc on dc.concept_id = cr.concept_id_2    
        WHERE (p.a4th_Procedure_Date_OPCS IS NOT NULL AND p.a4th_Procedure_Date_OPCS NOT LIKE '% %' AND (LENGTH(p.a4th_Procedure_Date_OPCS) =6 OR LENGTH(p.a4th_Procedure_Date_OPCS) =8))
        AND p.person_id IS NOT NULL
        AND p.a4th_Procedure_OPCS IS NOT NULL
        AND cr.relationship_id = 'Maps to'
        AND sc.vocabulary_id = 'OPCS4'
        AND dc.vocabulary_id = 'SNOMED'
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.procedure_occurrence` op ON 
        op.person_id = p.person_id 
        AND op.procedure_concept_id = p.procedure_concept_id
        AND op.procedure_source_value = p.procedure_source_value
        AND COALESCE(op.procedure_source_concept_id, -1) = COALESCE(p.procedure_source_concept_id, -1)
        AND op.procedure_datetime = p.procedure_datetime
        AND op.procedure_type_concept_id = p.procedure_type_concept_id
        AND COALESCE(op.provider_id, -1) = -1
        AND COALESCE(op.visit_occurrence_id, -1) = COALESCE(p.visit_occurrence_id, -1)
        AND COALESCE(op.visit_detail_id, -1) = COALESCE(p.visit_detail_id, -1)
    WHERE op.procedure_occurrence_id IS NULL;


    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added procedure_occurrence', run_id, 'SUS_BRI_APC_010415_to_300619_P1 - a4th_Procedure_OPCS -> procedure_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_APC_010415_to_300619_P1 - a5th_Procedure_OPCS] Transfer
    */
    SET max_current_id = COALESCE((SELECT procedure_occurrence_id FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.procedure_occurrence`
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_source_value,
        procedure_source_concept_id,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        visit_occurrence_id,
        visit_detail_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as procedure_occurrence_id,
        p.person_id,
        p.procedure_concept_id,
        p.procedure_source_value,
        p.procedure_source_concept_id,
        p.procedure_date,
        p.procedure_datetime,
        p.procedure_type_concept_id,
        p.visit_occurrence_id,
        p.visit_detail_id
    FROM
    (
        SELECT DISTINCT
            p.person_id person_id,
            dc.concept_id procedure_concept_id, 
            p.a5th_Procedure_OPCS procedure_source_value, 
            sc.concept_id  procedure_source_concept_id,
            CASE WHEN LENGTH(p.a5th_Procedure_Date_OPCS) = 6
                    THEN PARSE_DATE('%Y%m%d', CONCAT(p.a5th_Procedure_Date_OPCS, '01'))
                WHEN LENGTH(a5th_Procedure_Date_OPCS) = 8
                    THEN PARSE_DATE('%Y%m%d', p.a5th_Procedure_Date_OPCS)
            END procedure_date,
            CASE WHEN LENGTH(a5th_Procedure_Date_OPCS) = 6
                    THEN PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.a5th_Procedure_Date_OPCS ,'01000000'))
                WHEN LENGTH(a5th_Procedure_Date_OPCS) = 8
                    THEN PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.a5th_Procedure_Date_OPCS ,'000000'))
            END procedure_datetime,       
            32817 procedure_type_concept_id, --EHR
            vl.id visit_occurrence_id,
            vdl.id visit_detail_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1` p   
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = CONCAT(CAST(p.Hospital_Provider_Spell_Number as STRING),'_', p.person_id)
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups`) vdl On vdl.source_value = CONCAT(CAST(p.Hospital_Provider_Spell_Number as STRING),'_', p.person_id, '_', p.Episode_Number)
        JOIN `CY_CDM_VOCAB.concept` sc on REPLACE(sc.concept_code, '.', '') = p.a5th_Procedure_OPCS
        JOIN `CY_CDM_VOCAB.concept_relationship` cr on cr.concept_id_1 = sc.concept_id
        JOIN `CY_CDM_VOCAB.concept` dc on dc.concept_id = cr.concept_id_2    
        WHERE (p.a5th_Procedure_Date_OPCS IS NOT NULL AND p.a5th_Procedure_Date_OPCS NOT LIKE '% %' AND (LENGTH(p.a5th_Procedure_Date_OPCS) =6 OR LENGTH(p.a5th_Procedure_Date_OPCS) =8))
        AND p.person_id IS NOT NULL
        AND p.a5th_Procedure_OPCS IS NOT NULL
        AND cr.relationship_id = 'Maps to'
        AND sc.vocabulary_id = 'OPCS4'
        AND dc.vocabulary_id = 'SNOMED'
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.procedure_occurrence` op ON 
        op.person_id = p.person_id 
        AND op.procedure_concept_id = p.procedure_concept_id
        AND op.procedure_source_value = p.procedure_source_value
        AND COALESCE(op.procedure_source_concept_id, -1) = COALESCE(p.procedure_source_concept_id, -1)
        AND op.procedure_datetime = p.procedure_datetime
        AND op.procedure_type_concept_id = p.procedure_type_concept_id
        AND COALESCE(op.provider_id, -1) = -1
        AND COALESCE(op.visit_occurrence_id, -1) = COALESCE(p.visit_occurrence_id, -1)
        AND COALESCE(op.visit_detail_id, -1) = COALESCE(p.visit_detail_id, -1)
    WHERE op.procedure_occurrence_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added procedure_occurrence', run_id, 'SUS_BRI_APC_010415_to_300619_P1 - a5th_Procedure_OPCS -> procedure_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_APC_010415_to_300619_P1 - a6th_Procedure_OPCS] Transfer
    */
    SET max_current_id = COALESCE((SELECT procedure_occurrence_id FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.procedure_occurrence`
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_source_value,
        procedure_source_concept_id,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        visit_occurrence_id,
        visit_detail_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as procedure_occurrence_id,
        p.person_id,
        p.procedure_concept_id,
        p.procedure_source_value,
        p.procedure_source_concept_id,
        p.procedure_date,
        p.procedure_datetime,
        p.procedure_type_concept_id,
        p.visit_occurrence_id,
        p.visit_detail_id
    FROM
    (
        SELECT DISTINCT
            p.person_id person_id,
            dc.concept_id procedure_concept_id, 
            p.a6th_Procedure_OPCS procedure_source_value, 
            sc.concept_id  procedure_source_concept_id,
            CASE WHEN LENGTH(p.a6th_Procedure_Date_OPCS) = 6
                    THEN PARSE_DATE('%Y%m%d', CONCAT(p.a6th_Procedure_Date_OPCS, '01'))
                WHEN LENGTH(a6th_Procedure_Date_OPCS) = 8
                    THEN PARSE_DATE('%Y%m%d', p.a6th_Procedure_Date_OPCS)
            END procedure_date,
            CASE WHEN LENGTH(a6th_Procedure_Date_OPCS) = 6
                    THEN PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.a6th_Procedure_Date_OPCS ,'01000000'))
                WHEN LENGTH(a6th_Procedure_Date_OPCS) = 8
                    THEN PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.a6th_Procedure_Date_OPCS ,'000000'))
            END procedure_datetime,       
            32817 procedure_type_concept_id, --EHR
            vl.id visit_occurrence_id,
            vdl.id visit_detail_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1` p   
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = CONCAT(CAST(p.Hospital_Provider_Spell_Number as STRING),'_', p.person_id)
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups`) vdl On vdl.source_value = CONCAT(CAST(p.Hospital_Provider_Spell_Number as STRING),'_', p.person_id, '_', p.Episode_Number)
        JOIN `CY_CDM_VOCAB.concept` sc on REPLACE(sc.concept_code, '.', '') = p.a6th_Procedure_OPCS
        JOIN `CY_CDM_VOCAB.concept_relationship` cr on cr.concept_id_1 = sc.concept_id
        JOIN `CY_CDM_VOCAB.concept` dc on dc.concept_id = cr.concept_id_2    
        WHERE (p.a6th_Procedure_Date_OPCS IS NOT NULL AND p.a6th_Procedure_Date_OPCS NOT LIKE '% %' AND (LENGTH(p.a6th_Procedure_Date_OPCS) =6 OR LENGTH(p.a6th_Procedure_Date_OPCS) =8))
        AND p.person_id IS NOT NULL
        AND p.a6th_Procedure_OPCS IS NOT NULL
        AND cr.relationship_id = 'Maps to'
        AND sc.vocabulary_id = 'OPCS4'
        AND dc.vocabulary_id = 'SNOMED'
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.procedure_occurrence` op ON 
        op.person_id = p.person_id 
        AND op.procedure_concept_id = p.procedure_concept_id
        AND op.procedure_source_value = p.procedure_source_value
        AND COALESCE(op.procedure_source_concept_id, -1) = COALESCE(p.procedure_source_concept_id, -1)
        AND op.procedure_datetime = p.procedure_datetime
        AND op.procedure_type_concept_id = p.procedure_type_concept_id
        AND COALESCE(op.provider_id, -1) = -1
        AND COALESCE(op.visit_occurrence_id, -1) = COALESCE(p.visit_occurrence_id, -1)
        AND COALESCE(op.visit_detail_id, -1) = COALESCE(p.visit_detail_id, -1)
    WHERE op.procedure_occurrence_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added procedure_occurrence', run_id, 'SUS_BRI_APC_010415_to_300619_P1 - a6th_Procedure_OPCS -> procedure_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_APC_010415_to_300619_P1 - a7th_Procedure_OPCS] Transfer
    */
    SET max_current_id = COALESCE((SELECT procedure_occurrence_id FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.procedure_occurrence`
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_source_value,
        procedure_source_concept_id,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        visit_occurrence_id,
        visit_detail_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as procedure_occurrence_id,
        p.person_id,
        p.procedure_concept_id,
        p.procedure_source_value,
        p.procedure_source_concept_id,
        p.procedure_date,
        p.procedure_datetime,
        p.procedure_type_concept_id,
        p.visit_occurrence_id,
        p.visit_detail_id
    FROM
    (
        SELECT DISTINCT
            p.person_id person_id,
            dc.concept_id procedure_concept_id, 
            p.a7th_Procedure_OPCS procedure_source_value, 
            sc.concept_id  procedure_source_concept_id,
            CASE WHEN LENGTH(p.a7th_Procedure_Date_OPCS) = 6
                    THEN PARSE_DATE('%Y%m%d', CONCAT(p.a7th_Procedure_Date_OPCS, '01'))
                WHEN LENGTH(a7th_Procedure_Date_OPCS) = 8
                    THEN PARSE_DATE('%Y%m%d', p.a7th_Procedure_Date_OPCS)
            END procedure_date,
            CASE WHEN LENGTH(a7th_Procedure_Date_OPCS) = 6
                    THEN PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.a7th_Procedure_Date_OPCS ,'01000000'))
                WHEN LENGTH(a7th_Procedure_Date_OPCS) = 8
                    THEN PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.a7th_Procedure_Date_OPCS ,'000000'))
            END procedure_datetime,       
            32817 procedure_type_concept_id, --EHR
            vl.id visit_occurrence_id,
            vdl.id visit_detail_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1` p   
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = CONCAT(CAST(p.Hospital_Provider_Spell_Number as STRING),'_', p.person_id)
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups`) vdl On vdl.source_value = CONCAT(CAST(p.Hospital_Provider_Spell_Number as STRING),'_', p.person_id, '_', p.Episode_Number)
        JOIN `CY_CDM_VOCAB.concept` sc on REPLACE(sc.concept_code, '.', '') = p.a7th_Procedure_OPCS
        JOIN `CY_CDM_VOCAB.concept_relationship` cr on cr.concept_id_1 = sc.concept_id
        JOIN `CY_CDM_VOCAB.concept` dc on dc.concept_id = cr.concept_id_2    
        WHERE (p.a7th_Procedure_Date_OPCS IS NOT NULL AND p.a7th_Procedure_Date_OPCS NOT LIKE '% %' AND (LENGTH(p.a7th_Procedure_Date_OPCS) =6 OR LENGTH(p.a7th_Procedure_Date_OPCS) =8))
        AND p.person_id IS NOT NULL
        AND p.a7th_Procedure_OPCS IS NOT NULL
        AND cr.relationship_id = 'Maps to'
        AND sc.vocabulary_id = 'OPCS4'
        AND dc.vocabulary_id = 'SNOMED'
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.procedure_occurrence` op ON 
        op.person_id = p.person_id 
        AND op.procedure_concept_id = p.procedure_concept_id
        AND op.procedure_source_value = p.procedure_source_value
        AND COALESCE(op.procedure_source_concept_id, -1) = COALESCE(p.procedure_source_concept_id, -1)
        AND op.procedure_datetime = p.procedure_datetime
        AND op.procedure_type_concept_id = p.procedure_type_concept_id
        AND COALESCE(op.provider_id, -1) = -1
        AND COALESCE(op.visit_occurrence_id, -1) = COALESCE(p.visit_occurrence_id, -1)
        AND COALESCE(op.visit_detail_id, -1) = COALESCE(p.visit_detail_id, -1)
    WHERE op.procedure_occurrence_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added procedure_occurrence', run_id, 'SUS_BRI_APC_010415_to_300619_P1 - a7th_Procedure_OPCS -> procedure_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_APC_010415_to_300619_P1 - a8th_Procedure_OPCS] Transfer
    */
    SET max_current_id = COALESCE((SELECT procedure_occurrence_id FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.procedure_occurrence`
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_source_value,
        procedure_source_concept_id,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        visit_occurrence_id,
        visit_detail_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as procedure_occurrence_id,
        p.person_id,
        p.procedure_concept_id,
        p.procedure_source_value,
        p.procedure_source_concept_id,
        p.procedure_date,
        p.procedure_datetime,
        p.procedure_type_concept_id,
        p.visit_occurrence_id,
        p.visit_detail_id
    FROM
    (
        SELECT DISTINCT
            p.person_id person_id,
            dc.concept_id procedure_concept_id, 
            p.a8th_Procedure_OPCS procedure_source_value, 
            sc.concept_id  procedure_source_concept_id,
            CASE WHEN LENGTH(p.a8th_Procedure_Date_OPCS) = 6
                    THEN PARSE_DATE('%Y%m%d', CONCAT(p.a8th_Procedure_Date_OPCS, '01'))
                WHEN LENGTH(a8th_Procedure_Date_OPCS) = 8
                    THEN PARSE_DATE('%Y%m%d', p.a8th_Procedure_Date_OPCS)
            END procedure_date,
            CASE WHEN LENGTH(a8th_Procedure_Date_OPCS) = 6
                    THEN PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.a8th_Procedure_Date_OPCS ,'01000000'))
                WHEN LENGTH(a8th_Procedure_Date_OPCS) = 8
                    THEN PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.a8th_Procedure_Date_OPCS ,'000000'))
            END procedure_datetime,       
            32817 procedure_type_concept_id, --EHR
            vl.id visit_occurrence_id,
            vdl.id visit_detail_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1` p   
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = CONCAT(CAST(p.Hospital_Provider_Spell_Number as STRING),'_', p.person_id)
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups`) vdl On vdl.source_value = CONCAT(CAST(p.Hospital_Provider_Spell_Number as STRING),'_', p.person_id, '_', p.Episode_Number)
        JOIN `CY_CDM_VOCAB.concept` sc on REPLACE(sc.concept_code, '.', '') = p.a8th_Procedure_OPCS
        JOIN `CY_CDM_VOCAB.concept_relationship` cr on cr.concept_id_1 = sc.concept_id
        JOIN `CY_CDM_VOCAB.concept` dc on dc.concept_id = cr.concept_id_2    
        WHERE (p.a8th_Procedure_Date_OPCS IS NOT NULL AND p.a8th_Procedure_Date_OPCS NOT LIKE '% %' AND (LENGTH(p.a8th_Procedure_Date_OPCS) =6 OR LENGTH(p.a8th_Procedure_Date_OPCS) =8))
        AND p.person_id IS NOT NULL
        AND p.a8th_Procedure_OPCS IS NOT NULL
        AND cr.relationship_id = 'Maps to'
        AND sc.vocabulary_id = 'OPCS4'
        AND dc.vocabulary_id = 'SNOMED'
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.procedure_occurrence` op ON 
        op.person_id = p.person_id 
        AND op.procedure_concept_id = p.procedure_concept_id
        AND op.procedure_source_value = p.procedure_source_value
        AND COALESCE(op.procedure_source_concept_id, -1) = COALESCE(p.procedure_source_concept_id, -1)
        AND op.procedure_datetime = p.procedure_datetime
        AND op.procedure_type_concept_id = p.procedure_type_concept_id
        AND COALESCE(op.provider_id, -1) = -1
        AND COALESCE(op.visit_occurrence_id, -1) = COALESCE(p.visit_occurrence_id, -1)
        AND COALESCE(op.visit_detail_id, -1) = COALESCE(p.visit_detail_id, -1)
    WHERE op.procedure_occurrence_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added procedure_occurrence', run_id, 'SUS_BRI_APC_010415_to_300619_P1 - a8th_Procedure_OPCS -> procedure_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_APC_010415_to_300619_P1 - a9th_Procedure_OPCS] Transfer
    */
    SET max_current_id = COALESCE((SELECT procedure_occurrence_id FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.procedure_occurrence`
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_source_value,
        procedure_source_concept_id,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        visit_occurrence_id,
        visit_detail_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as procedure_occurrence_id,
        p.person_id,
        p.procedure_concept_id,
        p.procedure_source_value,
        p.procedure_source_concept_id,
        p.procedure_date,
        p.procedure_datetime,
        p.procedure_type_concept_id,
        p.visit_occurrence_id,
        p.visit_detail_id
    FROM
    (
        SELECT DISTINCT
            p.person_id person_id,
            dc.concept_id procedure_concept_id, 
            p.a9th_Procedure_OPCS procedure_source_value, 
            sc.concept_id procedure_source_concept_id,
            CASE WHEN LENGTH(p.a9th_Procedure_Date_OPCS) = 6
                    THEN PARSE_DATE('%Y%m%d', CONCAT(p.a9th_Procedure_Date_OPCS, '01'))
                WHEN LENGTH(a9th_Procedure_Date_OPCS) = 8
                    THEN PARSE_DATE('%Y%m%d', p.a9th_Procedure_Date_OPCS)
            END procedure_date,
            CASE WHEN LENGTH(a9th_Procedure_Date_OPCS) = 6
                    THEN PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.a9th_Procedure_Date_OPCS ,'01000000'))
                WHEN LENGTH(a9th_Procedure_Date_OPCS) = 8
                    THEN PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.a9th_Procedure_Date_OPCS ,'000000'))
            END procedure_datetime,       
            32817 procedure_type_concept_id, --EHR
            vl.id visit_occurrence_id,
            vdl.id visit_detail_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1` p   
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = CONCAT(CAST(p.Hospital_Provider_Spell_Number as STRING),'_', p.person_id)
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups`) vdl On vdl.source_value = CONCAT(CAST(p.Hospital_Provider_Spell_Number as STRING),'_', p.person_id, '_', p.Episode_Number)
        JOIN `CY_CDM_VOCAB.concept` sc on REPLACE(sc.concept_code, '.', '') = p.a9th_Procedure_OPCS
        JOIN `CY_CDM_VOCAB.concept_relationship` cr on cr.concept_id_1 = sc.concept_id
        JOIN `CY_CDM_VOCAB.concept` dc on dc.concept_id = cr.concept_id_2    
        WHERE (p.a9th_Procedure_Date_OPCS IS NOT NULL AND p.a9th_Procedure_Date_OPCS NOT LIKE '% %' AND (LENGTH(p.a9th_Procedure_Date_OPCS) =6 OR LENGTH(p.a9th_Procedure_Date_OPCS) =8))
        AND p.person_id IS NOT NULL
        AND p.a9th_Procedure_OPCS IS NOT NULL
        AND cr.relationship_id = 'Maps to'
        AND sc.vocabulary_id = 'OPCS4'
        AND dc.vocabulary_id = 'SNOMED'
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.procedure_occurrence` op ON 
        op.person_id = p.person_id 
        AND op.procedure_concept_id = p.procedure_concept_id
        AND op.procedure_source_value = p.procedure_source_value
        AND COALESCE(op.procedure_source_concept_id, -1) = COALESCE(p.procedure_source_concept_id, -1)
        AND op.procedure_datetime = p.procedure_datetime
        AND op.procedure_type_concept_id = p.procedure_type_concept_id
        AND COALESCE(op.provider_id, -1) = -1
        AND COALESCE(op.visit_occurrence_id, -1) = COALESCE(p.visit_occurrence_id, -1)
        AND COALESCE(op.visit_detail_id, -1) = COALESCE(p.visit_detail_id, -1)
    WHERE op.procedure_occurrence_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added procedure_occurrence', run_id, 'SUS_BRI_APC_010415_to_300619_P1 - a9th_Procedure_OPCS -> procedure_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_APC_010415_to_300619_P1 - a10th_Procedure_OPCS] Transfer
    */
    SET max_current_id = COALESCE((SELECT procedure_occurrence_id FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.procedure_occurrence`
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_source_value,
        procedure_source_concept_id,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        visit_occurrence_id,
        visit_detail_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as procedure_occurrence_id,
        p.person_id,
        p.procedure_concept_id,
        p.procedure_source_value,
        p.procedure_source_concept_id,
        p.procedure_date,
        p.procedure_datetime,
        p.procedure_type_concept_id,
        p.visit_occurrence_id,
        p.visit_detail_id
    FROM
    (
        SELECT DISTINCT
            p.person_id person_id,
            dc.concept_id procedure_concept_id, 
            p.a10th_Procedure_OPCS procedure_source_value, 
            sc.concept_id procedure_source_concept_id,
            CASE WHEN LENGTH(p.a10th_Procedure_Date_OPCS) = 6
                    THEN PARSE_DATE('%Y%m%d', CONCAT(p.a10th_Procedure_Date_OPCS, '01'))
                WHEN LENGTH(a10th_Procedure_Date_OPCS) = 8
                    THEN PARSE_DATE('%Y%m%d', p.a10th_Procedure_Date_OPCS)
            END procedure_date,
            CASE WHEN LENGTH(a10th_Procedure_Date_OPCS) = 6
                    THEN PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.a10th_Procedure_Date_OPCS ,'01000000'))
                WHEN LENGTH(a10th_Procedure_Date_OPCS) = 8
                    THEN PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.a10th_Procedure_Date_OPCS ,'000000'))
            END procedure_datetime,       
            32817 procedure_type_concept_id, --EHR
            vl.id visit_occurrence_id,
            vdl.id visit_detail_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1` p   
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = CONCAT(CAST(p.Hospital_Provider_Spell_Number as STRING),'_', p.person_id)
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups`) vdl On vdl.source_value = CONCAT(CAST(p.Hospital_Provider_Spell_Number as STRING),'_', p.person_id, '_', p.Episode_Number)
        JOIN `CY_CDM_VOCAB.concept` sc on REPLACE(sc.concept_code, '.', '') = p.a10th_Procedure_OPCS
        JOIN `CY_CDM_VOCAB.concept_relationship` cr on cr.concept_id_1 = sc.concept_id
        JOIN `CY_CDM_VOCAB.concept` dc on dc.concept_id = cr.concept_id_2    
        WHERE (p.a10th_Procedure_Date_OPCS IS NOT NULL AND p.a10th_Procedure_Date_OPCS NOT LIKE '% %' AND (LENGTH(p.a10th_Procedure_Date_OPCS) =6 OR LENGTH(p.a10th_Procedure_Date_OPCS) =8))
        AND p.person_id IS NOT NULL
        AND p.a10th_Procedure_OPCS IS NOT NULL
        AND cr.relationship_id = 'Maps to'
        AND sc.vocabulary_id = 'OPCS4'
        AND dc.vocabulary_id = 'SNOMED'
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.procedure_occurrence` op ON 
        op.person_id = p.person_id 
        AND op.procedure_concept_id = p.procedure_concept_id
        AND op.procedure_source_value = p.procedure_source_value
        AND COALESCE(op.procedure_source_concept_id, -1) = COALESCE(p.procedure_source_concept_id, -1)
        AND op.procedure_datetime = p.procedure_datetime
        AND op.procedure_type_concept_id = p.procedure_type_concept_id
        AND COALESCE(op.provider_id, -1) = -1
        AND COALESCE(op.visit_occurrence_id, -1) = COALESCE(p.visit_occurrence_id, -1)
        AND COALESCE(op.visit_detail_id, -1) = COALESCE(p.visit_detail_id, -1)
    WHERE op.procedure_occurrence_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added procedure_occurrence', run_id, 'SUS_BRI_APC_010415_to_300619_P1 - a10th_Procedure_OPCS -> procedure_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_APC_010415_to_300619_P1 - a11th_Procedure_OPCS] Transfer
    */
    SET max_current_id = COALESCE((SELECT procedure_occurrence_id FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.procedure_occurrence`
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_source_value,
        procedure_source_concept_id,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        visit_occurrence_id,
        visit_detail_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as procedure_occurrence_id,
        p.person_id,
        p.procedure_concept_id,
        p.procedure_source_value,
        p.procedure_source_concept_id,
        p.procedure_date,
        p.procedure_datetime,
        p.procedure_type_concept_id,
        p.visit_occurrence_id,
        p.visit_detail_id
    FROM
    (
        SELECT DISTINCT
            p.person_id person_id,
            dc.concept_id procedure_concept_id, 
            p.a11th_Procedure_OPCS procedure_source_value, 
            sc.concept_id procedure_source_concept_id,
            CASE WHEN LENGTH(p.a11th_Procedure_Date_OPCS) = 6
                    THEN PARSE_DATE('%Y%m%d', CONCAT(p.a11th_Procedure_Date_OPCS, '01'))
                WHEN LENGTH(a11th_Procedure_Date_OPCS) = 8
                    THEN PARSE_DATE('%Y%m%d', p.a11th_Procedure_Date_OPCS)
            END procedure_date,
            CASE WHEN LENGTH(a11th_Procedure_Date_OPCS) = 6
                    THEN PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.a11th_Procedure_Date_OPCS ,'01000000'))
                WHEN LENGTH(a11th_Procedure_Date_OPCS) = 8
                    THEN PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.a11th_Procedure_Date_OPCS ,'000000'))
            END procedure_datetime,       
            32817 procedure_type_concept_id, --EHR
            vl.id visit_occurrence_id,
            vdl.id visit_detail_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1` p   
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = CONCAT(CAST(p.Hospital_Provider_Spell_Number as STRING),'_', p.person_id)
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups`) vdl On vdl.source_value = CONCAT(CAST(p.Hospital_Provider_Spell_Number as STRING),'_', p.person_id, '_', p.Episode_Number)
        JOIN `CY_CDM_VOCAB.concept` sc on REPLACE(sc.concept_code, '.', '') = p.a11th_Procedure_OPCS
        JOIN `CY_CDM_VOCAB.concept_relationship` cr on cr.concept_id_1 = sc.concept_id
        JOIN `CY_CDM_VOCAB.concept` dc on dc.concept_id = cr.concept_id_2    
        WHERE (p.a11th_Procedure_Date_OPCS IS NOT NULL AND p.a11th_Procedure_Date_OPCS NOT LIKE '% %' AND (LENGTH(p.a11th_Procedure_Date_OPCS) =6 OR LENGTH(p.a11th_Procedure_Date_OPCS) =8))
        AND p.person_id IS NOT NULL
        AND p.a11th_Procedure_OPCS IS NOT NULL
        AND cr.relationship_id = 'Maps to'
        AND sc.vocabulary_id = 'OPCS4'
        AND dc.vocabulary_id = 'SNOMED'
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.procedure_occurrence` op ON 
        op.person_id = p.person_id 
        AND op.procedure_concept_id = p.procedure_concept_id
        AND op.procedure_source_value = p.procedure_source_value
        AND COALESCE(op.procedure_source_concept_id, -1) = COALESCE(p.procedure_source_concept_id, -1)
        AND op.procedure_datetime = p.procedure_datetime
        AND op.procedure_type_concept_id = p.procedure_type_concept_id
        AND COALESCE(op.provider_id, -1) = -1
        AND COALESCE(op.visit_occurrence_id, -1) = COALESCE(p.visit_occurrence_id, -1)
        AND COALESCE(op.visit_detail_id, -1) = COALESCE(p.visit_detail_id, -1)
    WHERE op.procedure_occurrence_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added procedure_occurrence', run_id, 'SUS_BRI_APC_010415_to_300619_P1 - a11th_Procedure_OPCS -> procedure_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_APC_010415_to_300619_P1 - a12th_Procedure_OPCS] Transfer
    */
    SET max_current_id = COALESCE((SELECT procedure_occurrence_id FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.procedure_occurrence`
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_source_value,
        procedure_source_concept_id,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        visit_occurrence_id,
        visit_detail_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as procedure_occurrence_id,
        p.person_id,
        p.procedure_concept_id,
        p.procedure_source_value,
        p.procedure_source_concept_id,
        p.procedure_date,
        p.procedure_datetime,
        p.procedure_type_concept_id,
        p.visit_occurrence_id,
        p.visit_detail_id
    FROM
    (
        SELECT DISTINCT
            p.person_id person_id,
            dc.concept_id procedure_concept_id, 
            p.a12th_Procedure_OPCS procedure_source_value, 
            sc.concept_id procedure_source_concept_id,
            CASE WHEN LENGTH(p.a12th_Procedure_Date_OPCS) = 6
                    THEN PARSE_DATE('%Y%m%d', CONCAT(p.a12th_Procedure_Date_OPCS, '01'))
                WHEN LENGTH(a12th_Procedure_Date_OPCS) = 8
                    THEN PARSE_DATE('%Y%m%d', p.a12th_Procedure_Date_OPCS)
            END procedure_date,
            CASE WHEN LENGTH(a12th_Procedure_Date_OPCS) = 6
                    THEN PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.a12th_Procedure_Date_OPCS ,'01000000'))
                WHEN LENGTH(a12th_Procedure_Date_OPCS) = 8
                    THEN PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.a12th_Procedure_Date_OPCS ,'000000'))
            END procedure_datetime,       
            32817 procedure_type_concept_id, --EHR
            vl.id visit_occurrence_id,
            vdl.id visit_detail_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1` p   
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = CONCAT(CAST(p.Hospital_Provider_Spell_Number as STRING),'_', p.person_id)
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups`) vdl On vdl.source_value = CONCAT(CAST(p.Hospital_Provider_Spell_Number as STRING),'_', p.person_id, '_', p.Episode_Number)
        JOIN `CY_CDM_VOCAB.concept` sc on REPLACE(sc.concept_code, '.', '') = p.a12th_Procedure_OPCS
        JOIN `CY_CDM_VOCAB.concept_relationship` cr on cr.concept_id_1 = sc.concept_id
        JOIN `CY_CDM_VOCAB.concept` dc on dc.concept_id = cr.concept_id_2    
        WHERE (p.a12th_Procedure_Date_OPCS IS NOT NULL AND p.a12th_Procedure_Date_OPCS NOT LIKE '% %' AND (LENGTH(p.a12th_Procedure_Date_OPCS) =6 OR LENGTH(p.a12th_Procedure_Date_OPCS) =8))
        AND p.person_id IS NOT NULL
        AND p.a12th_Procedure_OPCS IS NOT NULL
        AND cr.relationship_id = 'Maps to'
        AND sc.vocabulary_id = 'OPCS4'
        AND dc.vocabulary_id = 'SNOMED'
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.procedure_occurrence` op ON 
        op.person_id = p.person_id 
        AND op.procedure_concept_id = p.procedure_concept_id
        AND op.procedure_source_value = p.procedure_source_value
        AND COALESCE(op.procedure_source_concept_id, -1) = COALESCE(p.procedure_source_concept_id, -1)
        AND op.procedure_datetime = p.procedure_datetime
        AND op.procedure_type_concept_id = p.procedure_type_concept_id
        AND COALESCE(op.provider_id, -1) = -1
        AND COALESCE(op.visit_occurrence_id, -1) = COALESCE(p.visit_occurrence_id, -1)
        AND COALESCE(op.visit_detail_id, -1) = COALESCE(p.visit_detail_id, -1)
    WHERE op.procedure_occurrence_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added procedure_occurrence', run_id, 'SUS_BRI_APC_010415_to_300619_P1 - a12th_Procedure_OPCS -> procedure_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_APC_010415_to_300619_P1 - a13th_Procedure_OPCS] Transfer
    */
    SET max_current_id = COALESCE((SELECT procedure_occurrence_id FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.procedure_occurrence`
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_source_value,
        procedure_source_concept_id,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        visit_occurrence_id,
        visit_detail_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as procedure_occurrence_id,
        p.person_id,
        p.procedure_concept_id,
        p.procedure_source_value,
        p.procedure_source_concept_id,
        p.procedure_date,
        p.procedure_datetime,
        p.procedure_type_concept_id,
        p.visit_occurrence_id,
        p.visit_detail_id
    FROM
    (
        SELECT DISTINCT
            p.person_id person_id,
            dc.concept_id procedure_concept_id, 
            p.a13th_Procedure_OPCS procedure_source_value, 
            sc.concept_id procedure_source_concept_id,
            CASE WHEN LENGTH(p.a13th_Procedure_Date_OPCS) = 6
                    THEN PARSE_DATE('%Y%m%d', CONCAT(p.a13th_Procedure_Date_OPCS, '01'))
                WHEN LENGTH(a13th_Procedure_Date_OPCS) = 8
                    THEN PARSE_DATE('%Y%m%d', p.a13th_Procedure_Date_OPCS)
            END procedure_date,
            CASE WHEN LENGTH(a13th_Procedure_Date_OPCS) = 6
                    THEN PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.a13th_Procedure_Date_OPCS ,'01000000'))
                WHEN LENGTH(a13th_Procedure_Date_OPCS) = 8
                    THEN PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.a13th_Procedure_Date_OPCS ,'000000'))
            END procedure_datetime,       
            32817 procedure_type_concept_id, --EHR
            vl.id visit_occurrence_id,
            vdl.id visit_detail_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1` p   
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = CONCAT(CAST(p.Hospital_Provider_Spell_Number as STRING),'_', p.person_id)
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups`) vdl On vdl.source_value = CONCAT(CAST(p.Hospital_Provider_Spell_Number as STRING),'_', p.person_id, '_', p.Episode_Number)
        JOIN `CY_CDM_VOCAB.concept` sc on REPLACE(sc.concept_code, '.', '') = p.a13th_Procedure_OPCS
        JOIN `CY_CDM_VOCAB.concept_relationship` cr on cr.concept_id_1 = sc.concept_id
        JOIN `CY_CDM_VOCAB.concept` dc on dc.concept_id = cr.concept_id_2    
        WHERE (p.a13th_Procedure_Date_OPCS IS NOT NULL AND p.a13th_Procedure_Date_OPCS NOT LIKE '% %' AND (LENGTH(p.a13th_Procedure_Date_OPCS) =6 OR LENGTH(p.a13th_Procedure_Date_OPCS) =8))
        AND p.person_id IS NOT NULL
        AND p.a13th_Procedure_OPCS IS NOT NULL
        AND cr.relationship_id = 'Maps to'
        AND sc.vocabulary_id = 'OPCS4'
        AND dc.vocabulary_id = 'SNOMED'
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.procedure_occurrence` op ON 
        op.person_id = p.person_id 
        AND op.procedure_concept_id = p.procedure_concept_id
        AND op.procedure_source_value = p.procedure_source_value
        AND COALESCE(op.procedure_source_concept_id, -1) = COALESCE(p.procedure_source_concept_id, -1)
        AND op.procedure_datetime = p.procedure_datetime
        AND op.procedure_type_concept_id = p.procedure_type_concept_id
        AND COALESCE(op.provider_id, -1) = -1
        AND COALESCE(op.visit_occurrence_id, -1) = COALESCE(p.visit_occurrence_id, -1)
        AND COALESCE(op.visit_detail_id, -1) = COALESCE(p.visit_detail_id, -1)
    WHERE op.procedure_occurrence_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added procedure_occurrence', run_id, 'SUS_BRI_APC_010415_to_300619_P1 - a13th_Procedure_OPCS -> procedure_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_APC_010415_to_300619_P1 - a14th_Procedure_OPCS] Transfer
    */
    SET max_current_id = COALESCE((SELECT procedure_occurrence_id FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.procedure_occurrence`
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_source_value,
        procedure_source_concept_id,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        visit_occurrence_id,
        visit_detail_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as procedure_occurrence_id,
        p.person_id,
        p.procedure_concept_id,
        p.procedure_source_value,
        p.procedure_source_concept_id,
        p.procedure_date,
        p.procedure_datetime,
        p.procedure_type_concept_id,
        p.visit_occurrence_id,
        p.visit_detail_id
    FROM
    (
        SELECT DISTINCT
            p.person_id person_id,
            dc.concept_id procedure_concept_id, 
            p.a14th_Procedure_OPCS procedure_source_value, 
            sc.concept_id procedure_source_concept_id,
            CASE WHEN LENGTH(p.a14th_Procedure_Date_OPCS) = 6
                    THEN PARSE_DATE('%Y%m%d', CONCAT(p.a14th_Procedure_Date_OPCS, '01'))
                WHEN LENGTH(a14th_Procedure_Date_OPCS) = 8
                    THEN PARSE_DATE('%Y%m%d', p.a14th_Procedure_Date_OPCS)
            END procedure_date,
            CASE WHEN LENGTH(a14th_Procedure_Date_OPCS) = 6
                    THEN PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.a14th_Procedure_Date_OPCS ,'01000000'))
                WHEN LENGTH(a14th_Procedure_Date_OPCS) = 8
                    THEN PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.a14th_Procedure_Date_OPCS ,'000000'))
            END procedure_datetime,       
            32817 procedure_type_concept_id, --EHR
            vl.id visit_occurrence_id,
            vdl.id visit_detail_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1` p   
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = CONCAT(CAST(p.Hospital_Provider_Spell_Number as STRING),'_', p.person_id)
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups`) vdl On vdl.source_value = CONCAT(CAST(p.Hospital_Provider_Spell_Number as STRING),'_', p.person_id, '_', p.Episode_Number)
        JOIN `CY_CDM_VOCAB.concept` sc on REPLACE(sc.concept_code, '.', '') = p.a14th_Procedure_OPCS
        JOIN `CY_CDM_VOCAB.concept_relationship` cr on cr.concept_id_1 = sc.concept_id
        JOIN `CY_CDM_VOCAB.concept` dc on dc.concept_id = cr.concept_id_2    
        WHERE (p.a14th_Procedure_Date_OPCS IS NOT NULL AND p.a14th_Procedure_Date_OPCS NOT LIKE '% %' AND (LENGTH(p.a14th_Procedure_Date_OPCS) =6 OR LENGTH(p.a14th_Procedure_Date_OPCS) =8))
        AND p.person_id IS NOT NULL
        AND p.a14th_Procedure_OPCS IS NOT NULL
        AND cr.relationship_id = 'Maps to'
        AND sc.vocabulary_id = 'OPCS4'
        AND dc.vocabulary_id = 'SNOMED'
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.procedure_occurrence` op ON 
        op.person_id = p.person_id 
        AND op.procedure_concept_id = p.procedure_concept_id
        AND op.procedure_source_value = p.procedure_source_value
        AND COALESCE(op.procedure_source_concept_id, -1) = COALESCE(p.procedure_source_concept_id, -1)
        AND op.procedure_datetime = p.procedure_datetime
        AND op.procedure_type_concept_id = p.procedure_type_concept_id
        AND COALESCE(op.provider_id, -1) = -1
        AND COALESCE(op.visit_occurrence_id, -1) = COALESCE(p.visit_occurrence_id, -1)
        AND COALESCE(op.visit_detail_id, -1) = COALESCE(p.visit_detail_id, -1)
    WHERE op.procedure_occurrence_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added procedure_occurrence', run_id, 'SUS_BRI_APC_010415_to_300619_P1 - a14th_Procedure_OPCS -> procedure_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_APC_010415_to_300619_P1 - a15th_Procedure_OPCS] Transfer
    */
    SET max_current_id = COALESCE((SELECT procedure_occurrence_id FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.procedure_occurrence`
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_source_value,
        procedure_source_concept_id,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        visit_occurrence_id,
        visit_detail_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as procedure_occurrence_id,
        p.person_id,
        p.procedure_concept_id,
        p.procedure_source_value,
        p.procedure_source_concept_id,
        p.procedure_date,
        p.procedure_datetime,
        p.procedure_type_concept_id,
        p.visit_occurrence_id,
        p.visit_detail_id
    FROM
    (
        SELECT DISTINCT
            p.person_id person_id,
            dc.concept_id procedure_concept_id, 
            p.a15th_Procedure_OPCS procedure_source_value, 
            sc.concept_id procedure_source_concept_id,
            CASE WHEN LENGTH(p.a15th_Procedure_Date_OPCS) = 6
                    THEN PARSE_DATE('%Y%m%d', CONCAT(p.a15th_Procedure_Date_OPCS, '01'))
                WHEN LENGTH(a15th_Procedure_Date_OPCS) = 8
                    THEN PARSE_DATE('%Y%m%d', p.a15th_Procedure_Date_OPCS)
            END procedure_date,
            CASE WHEN LENGTH(a15th_Procedure_Date_OPCS) = 6
                    THEN PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.a15th_Procedure_Date_OPCS ,'01000000'))
                WHEN LENGTH(a15th_Procedure_Date_OPCS) = 8
                    THEN PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.a15th_Procedure_Date_OPCS ,'000000'))
            END procedure_datetime,       
            32817 procedure_type_concept_id, --EHR
            vl.id visit_occurrence_id,
            vdl.id visit_detail_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1` p   
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = CONCAT(CAST(p.Hospital_Provider_Spell_Number as STRING),'_', p.person_id)
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups`) vdl On vdl.source_value = CONCAT(CAST(p.Hospital_Provider_Spell_Number as STRING),'_', p.person_id, '_', p.Episode_Number)
        JOIN `CY_CDM_VOCAB.concept` sc on REPLACE(sc.concept_code, '.', '') = p.a15th_Procedure_OPCS
        JOIN `CY_CDM_VOCAB.concept_relationship` cr on cr.concept_id_1 = sc.concept_id
        JOIN `CY_CDM_VOCAB.concept` dc on dc.concept_id = cr.concept_id_2    
        WHERE (p.a15th_Procedure_Date_OPCS IS NOT NULL AND p.a15th_Procedure_Date_OPCS NOT LIKE '% %' AND (LENGTH(p.a15th_Procedure_Date_OPCS) =6 OR LENGTH(p.a15th_Procedure_Date_OPCS) =8))
        AND p.person_id IS NOT NULL
        AND p.a15th_Procedure_OPCS IS NOT NULL
        AND cr.relationship_id = 'Maps to'
        AND sc.vocabulary_id = 'OPCS4'
        AND dc.vocabulary_id = 'SNOMED'
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.procedure_occurrence` op ON 
        op.person_id = p.person_id 
        AND op.procedure_concept_id = p.procedure_concept_id
        AND op.procedure_source_value = p.procedure_source_value
        AND COALESCE(op.procedure_source_concept_id, -1) = COALESCE(p.procedure_source_concept_id, -1)
        AND op.procedure_datetime = p.procedure_datetime
        AND op.procedure_type_concept_id = p.procedure_type_concept_id
        AND COALESCE(op.provider_id, -1) = -1
        AND COALESCE(op.visit_occurrence_id, -1) = COALESCE(p.visit_occurrence_id, -1)
        AND COALESCE(op.visit_detail_id, -1) = COALESCE(p.visit_detail_id, -1)
    WHERE op.procedure_occurrence_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added procedure_occurrence', run_id, 'SUS_BRI_APC_010415_to_300619_P1 - a15th_Procedure_OPCS -> procedure_occurrence', @@row_count, CURRENT_DATETIME();

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
------------[SUS_BRI_OP_010415_to_300619]
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

    /*
        --[SUS_BRI_OP_010415_to_300619 - Primary_Procedure_OPCS] Transfer
    */
    SET max_current_id = COALESCE((SELECT procedure_occurrence_id FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.procedure_occurrence`
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_source_value,
        procedure_source_concept_id,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as procedure_occurrence_id,
        p.person_id,
        p.procedure_concept_id,
        p.procedure_source_value,
        p.procedure_source_concept_id,
        p.procedure_date,
        p.procedure_datetime,
        p.procedure_type_concept_id,
        p.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            p.person_id person_id,
            dc.concept_id procedure_concept_id, 
            p.Primary_Procedure_OPCS procedure_source_value, 
            sc.concept_id procedure_source_concept_id,
            PARSE_DATE("%Y%m%d", Appointment_Date) procedure_date,
              CASE WHEN Appointment_Time LIKE '% %' OR Appointment_Time IS NULL
              THEN PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(Appointment_Date, '000000'))
              ELSE PARSE_DATETIME('%Y%m%d%H:%M%S', CONCAT(Appointment_Date, Appointment_Time, '00'))
            END procedure_datetime,     
            32817 procedure_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_OP_010415_to_300619` p   
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_OP_010415_to_300619') vl On vl.source_value = CONCAT(CAST(p.Generated_Record_Identifier as STRING),'_', p.person_id)
        JOIN `CY_CDM_VOCAB.concept` sc on REPLACE(sc.concept_code, '.', '') = REPLACE(p.Primary_Procedure_OPCS, ' ', '')
        JOIN `CY_CDM_VOCAB.concept_relationship` cr on cr.concept_id_1 = sc.concept_id
        JOIN `CY_CDM_VOCAB.concept` dc on dc.concept_id = cr.concept_id_2    
        WHERE p.Appointment_Date IS NOT NULL
        AND p.person_id IS NOT NULL
        AND (p.Primary_Procedure_OPCS IS NOT NULL AND p.Primary_Procedure_OPCS != '     ' )
        AND cr.relationship_id = 'Maps to'
        AND sc.vocabulary_id = 'OPCS4'
        AND dc.vocabulary_id = 'SNOMED'
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.procedure_occurrence` op ON 
        op.person_id = p.person_id 
        AND op.procedure_concept_id = p.procedure_concept_id
        AND op.procedure_source_value = p.procedure_source_value
        AND COALESCE(op.procedure_source_concept_id, -1) = COALESCE(p.procedure_source_concept_id, -1)
        AND op.procedure_datetime = p.procedure_datetime
        AND op.procedure_type_concept_id = p.procedure_type_concept_id
        AND COALESCE(op.provider_id, -1) = -1
        AND COALESCE(op.visit_occurrence_id, -1) = COALESCE(p.visit_occurrence_id, -1)
        AND COALESCE(op.visit_detail_id, -1) = -1
    WHERE op.procedure_occurrence_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added procedure_occurrence', run_id, 'SUS_BRI_OP_010415_to_300619 - Primary_Procedure_OPCS -> procedure_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_OP_010415_to_300619 - A2nd_Procedure_OPCS] Transfer
    */
    SET max_current_id = COALESCE((SELECT procedure_occurrence_id FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.procedure_occurrence`
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_source_value,
        procedure_source_concept_id,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as procedure_occurrence_id,
        p.person_id,
        p.procedure_concept_id,
        p.procedure_source_value,
        p.procedure_source_concept_id,
        p.procedure_date,
        p.procedure_datetime,
        p.procedure_type_concept_id,
        p.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            p.person_id person_id,
            dc.concept_id procedure_concept_id, 
            p.A2nd_Procedure_OPCS procedure_source_value, 
            sc.concept_id procedure_source_concept_id,
            PARSE_DATE("%Y%m%d", Appointment_Date) procedure_date,
              CASE WHEN Appointment_Time LIKE '% %' OR Appointment_Time IS NULL
              THEN PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(Appointment_Date, '000000'))
              ELSE PARSE_DATETIME('%Y%m%d%H:%M%S', CONCAT(Appointment_Date, Appointment_Time, '00'))
            END procedure_datetime,     
            32817 procedure_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_OP_010415_to_300619` p   
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_OP_010415_to_300619') vl On vl.source_value = CONCAT(CAST(p.Generated_Record_Identifier as STRING),'_', p.person_id)
        JOIN `CY_CDM_VOCAB.concept` sc on REPLACE(sc.concept_code, '.', '') = REPLACE(p.A2nd_Procedure_OPCS, ' ', '')
        JOIN `CY_CDM_VOCAB.concept_relationship` cr on cr.concept_id_1 = sc.concept_id
        JOIN `CY_CDM_VOCAB.concept` dc on dc.concept_id = cr.concept_id_2    
        WHERE p.Appointment_Date IS NOT NULL
        AND p.person_id IS NOT NULL
        AND (p.A2nd_Procedure_OPCS IS NOT NULL AND p.A2nd_Procedure_OPCS != '     ' )
        AND cr.relationship_id = 'Maps to'
        AND sc.vocabulary_id = 'OPCS4'
        AND dc.vocabulary_id = 'SNOMED'
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.procedure_occurrence` op ON 
        op.person_id = p.person_id 
        AND op.procedure_concept_id = p.procedure_concept_id
        AND op.procedure_source_value = p.procedure_source_value
        AND COALESCE(op.procedure_source_concept_id, -1) = COALESCE(p.procedure_source_concept_id, -1)
        AND op.procedure_datetime = p.procedure_datetime
        AND op.procedure_type_concept_id = p.procedure_type_concept_id
        AND COALESCE(op.provider_id, -1) = -1
        AND COALESCE(op.visit_occurrence_id, -1) = COALESCE(p.visit_occurrence_id, -1)
        AND COALESCE(op.visit_detail_id, -1) = -1
    WHERE op.procedure_occurrence_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added procedure_occurrence', run_id, 'SUS_BRI_OP_010415_to_300619 - A2nd_Procedure_OPCS -> procedure_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_OP_010415_to_300619 - A3rd_Procedure_OPCS] Transfer
    */
    SET max_current_id = COALESCE((SELECT procedure_occurrence_id FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.procedure_occurrence`
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_source_value,
        procedure_source_concept_id,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as procedure_occurrence_id,
        p.person_id,
        p.procedure_concept_id,
        p.procedure_source_value,
        p.procedure_source_concept_id,
        p.procedure_date,
        p.procedure_datetime,
        p.procedure_type_concept_id,
        p.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            p.person_id person_id,
            dc.concept_id procedure_concept_id, 
            p.A3rd_Procedure_OPCS procedure_source_value, 
            sc.concept_id procedure_source_concept_id,
            PARSE_DATE("%Y%m%d", Appointment_Date) procedure_date,
              CASE WHEN Appointment_Time LIKE '% %' OR Appointment_Time IS NULL
              THEN PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(Appointment_Date, '000000'))
              ELSE PARSE_DATETIME('%Y%m%d%H:%M%S', CONCAT(Appointment_Date, Appointment_Time, '00'))
            END procedure_datetime,     
            32817 procedure_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_OP_010415_to_300619` p   
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_OP_010415_to_300619') vl On vl.source_value = CONCAT(CAST(p.Generated_Record_Identifier as STRING),'_', p.person_id)
        JOIN `CY_CDM_VOCAB.concept` sc on REPLACE(sc.concept_code, '.', '') = REPLACE(p.A3rd_Procedure_OPCS, ' ', '')
        JOIN `CY_CDM_VOCAB.concept_relationship` cr on cr.concept_id_1 = sc.concept_id
        JOIN `CY_CDM_VOCAB.concept` dc on dc.concept_id = cr.concept_id_2    
        WHERE p.Appointment_Date IS NOT NULL
        AND p.person_id IS NOT NULL
        AND (p.A3rd_Procedure_OPCS IS NOT NULL AND p.A3rd_Procedure_OPCS != '     ' )
        AND cr.relationship_id = 'Maps to'
        AND sc.vocabulary_id = 'OPCS4'
        AND dc.vocabulary_id = 'SNOMED'
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.procedure_occurrence` op ON 
        op.person_id = p.person_id 
        AND op.procedure_concept_id = p.procedure_concept_id
        AND op.procedure_source_value = p.procedure_source_value
        AND COALESCE(op.procedure_source_concept_id, -1) = COALESCE(p.procedure_source_concept_id, -1)
        AND op.procedure_datetime = p.procedure_datetime
        AND op.procedure_type_concept_id = p.procedure_type_concept_id
        AND COALESCE(op.provider_id, -1) = -1
        AND COALESCE(op.visit_occurrence_id, -1) = COALESCE(p.visit_occurrence_id, -1)
        AND COALESCE(op.visit_detail_id, -1) = -1
    WHERE op.procedure_occurrence_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added procedure_occurrence', run_id, 'SUS_BRI_OP_010415_to_300619 - A3rd_Procedure_OPCS -> procedure_occurrence', @@row_count, CURRENT_DATETIME();
    
    /*
        --[SUS_BRI_OP_010415_to_300619 - A4th_Procedure_OPCS] Transfer
    */
    SET max_current_id = COALESCE((SELECT procedure_occurrence_id FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.procedure_occurrence`
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_source_value,
        procedure_source_concept_id,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as procedure_occurrence_id,
        p.person_id,
        p.procedure_concept_id,
        p.procedure_source_value,
        p.procedure_source_concept_id,
        p.procedure_date,
        p.procedure_datetime,
        p.procedure_type_concept_id,
        p.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            p.person_id person_id,
            dc.concept_id procedure_concept_id, 
            p.A4th_Procedure_OPCS procedure_source_value, 
            sc.concept_id procedure_source_concept_id,
            PARSE_DATE("%Y%m%d", Appointment_Date) procedure_date,
              CASE WHEN Appointment_Time LIKE '% %' OR Appointment_Time IS NULL
              THEN PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(Appointment_Date, '000000'))
              ELSE PARSE_DATETIME('%Y%m%d%H:%M%S', CONCAT(Appointment_Date, Appointment_Time, '00'))
            END procedure_datetime,     
            32817 procedure_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_OP_010415_to_300619` p   
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_OP_010415_to_300619') vl On vl.source_value = CONCAT(CAST(p.Generated_Record_Identifier as STRING),'_', p.person_id)
        JOIN `CY_CDM_VOCAB.concept` sc on REPLACE(sc.concept_code, '.', '') = REPLACE(p.A4th_Procedure_OPCS, ' ', '')
        JOIN `CY_CDM_VOCAB.concept_relationship` cr on cr.concept_id_1 = sc.concept_id
        JOIN `CY_CDM_VOCAB.concept` dc on dc.concept_id = cr.concept_id_2    
        WHERE p.Appointment_Date IS NOT NULL
        AND p.person_id IS NOT NULL
        AND (p.A4th_Procedure_OPCS IS NOT NULL AND p.A4th_Procedure_OPCS != '     ' )
        AND cr.relationship_id = 'Maps to'
        AND sc.vocabulary_id = 'OPCS4'
        AND dc.vocabulary_id = 'SNOMED'
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.procedure_occurrence` op ON 
        op.person_id = p.person_id 
        AND op.procedure_concept_id = p.procedure_concept_id
        AND op.procedure_source_value = p.procedure_source_value
        AND COALESCE(op.procedure_source_concept_id, -1) = COALESCE(p.procedure_source_concept_id, -1)
        AND op.procedure_datetime = p.procedure_datetime
        AND op.procedure_type_concept_id = p.procedure_type_concept_id
        AND COALESCE(op.provider_id, -1) = -1
        AND COALESCE(op.visit_occurrence_id, -1) = COALESCE(p.visit_occurrence_id, -1)
        AND COALESCE(op.visit_detail_id, -1) = -1
    WHERE op.procedure_occurrence_id IS NULL;
  
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added procedure_occurrence', run_id, 'SUS_BRI_OP_010415_to_300619 - A4th_Procedure_OPCS -> procedure_occurrence', @@row_count, CURRENT_DATETIME();
    
    /*
        --[SUS_BRI_OP_010415_to_300619 - A5th_Procedure_OPCS] Transfer
    */
    SET max_current_id = COALESCE((SELECT procedure_occurrence_id FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.procedure_occurrence`
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_source_value,
        procedure_source_concept_id,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as procedure_occurrence_id,
        p.person_id,
        p.procedure_concept_id,
        p.procedure_source_value,
        p.procedure_source_concept_id,
        p.procedure_date,
        p.procedure_datetime,
        p.procedure_type_concept_id,
        p.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            p.person_id person_id,
            dc.concept_id procedure_concept_id, 
            p.A5th_Procedure_OPCS procedure_source_value, 
            sc.concept_id procedure_source_concept_id,
            PARSE_DATE("%Y%m%d", Appointment_Date) procedure_date,
              CASE WHEN Appointment_Time LIKE '% %' OR Appointment_Time IS NULL
              THEN PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(Appointment_Date, '000000'))
              ELSE PARSE_DATETIME('%Y%m%d%H:%M%S', CONCAT(Appointment_Date, Appointment_Time, '00'))
            END procedure_datetime,     
            32817 procedure_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_OP_010415_to_300619` p   
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_OP_010415_to_300619') vl On vl.source_value = CONCAT(CAST(p.Generated_Record_Identifier as STRING),'_', p.person_id)
        JOIN `CY_CDM_VOCAB.concept` sc on REPLACE(sc.concept_code, '.', '') = REPLACE(p.A5th_Procedure_OPCS, ' ', '')
        JOIN `CY_CDM_VOCAB.concept_relationship` cr on cr.concept_id_1 = sc.concept_id
        JOIN `CY_CDM_VOCAB.concept` dc on dc.concept_id = cr.concept_id_2    
        WHERE p.Appointment_Date IS NOT NULL
        AND p.person_id IS NOT NULL
        AND (p.A5th_Procedure_OPCS IS NOT NULL AND p.A5th_Procedure_OPCS != '     ' )
        AND cr.relationship_id = 'Maps to'
        AND sc.vocabulary_id = 'OPCS4'
        AND dc.vocabulary_id = 'SNOMED'
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.procedure_occurrence` op ON 
        op.person_id = p.person_id 
        AND op.procedure_concept_id = p.procedure_concept_id
        AND op.procedure_source_value = p.procedure_source_value
        AND COALESCE(op.procedure_source_concept_id, -1) = COALESCE(p.procedure_source_concept_id, -1)
        AND op.procedure_datetime = p.procedure_datetime
        AND op.procedure_type_concept_id = p.procedure_type_concept_id
        AND COALESCE(op.provider_id, -1) = -1
        AND COALESCE(op.visit_occurrence_id, -1) = COALESCE(p.visit_occurrence_id, -1)
        AND COALESCE(op.visit_detail_id, -1) = -1
    WHERE op.procedure_occurrence_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added procedure_occurrence', run_id, 'SUS_BRI_OP_010415_to_300619 - A5th_Procedure_OPCS -> procedure_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_OP_010415_to_300619 - A6th_Procedure_OPCS] Transfer
    */
    SET max_current_id = COALESCE((SELECT procedure_occurrence_id FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.procedure_occurrence`
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_source_value,
        procedure_source_concept_id,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as procedure_occurrence_id,
        p.person_id,
        p.procedure_concept_id,
        p.procedure_source_value,
        p.procedure_source_concept_id,
        p.procedure_date,
        p.procedure_datetime,
        p.procedure_type_concept_id,
        p.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            p.person_id person_id,
            dc.concept_id procedure_concept_id, 
            p.A6th_Procedure_OPCS procedure_source_value, 
            sc.concept_id procedure_source_concept_id,
            PARSE_DATE("%Y%m%d", Appointment_Date) procedure_date,
              CASE WHEN Appointment_Time LIKE '% %' OR Appointment_Time IS NULL
              THEN PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(Appointment_Date, '000000'))
              ELSE PARSE_DATETIME('%Y%m%d%H:%M%S', CONCAT(Appointment_Date, Appointment_Time, '00'))
            END procedure_datetime,     
            32817 procedure_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_OP_010415_to_300619` p   
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_OP_010415_to_300619') vl On vl.source_value = CONCAT(CAST(p.Generated_Record_Identifier as STRING),'_', p.person_id)
        JOIN `CY_CDM_VOCAB.concept` sc on REPLACE(sc.concept_code, '.', '') = REPLACE(p.A6th_Procedure_OPCS, ' ', '')
        JOIN `CY_CDM_VOCAB.concept_relationship` cr on cr.concept_id_1 = sc.concept_id
        JOIN `CY_CDM_VOCAB.concept` dc on dc.concept_id = cr.concept_id_2    
        WHERE p.Appointment_Date IS NOT NULL
        AND p.person_id IS NOT NULL
        AND (p.A6th_Procedure_OPCS IS NOT NULL AND p.A6th_Procedure_OPCS != '     ' )
        AND cr.relationship_id = 'Maps to'
        AND sc.vocabulary_id = 'OPCS4'
        AND dc.vocabulary_id = 'SNOMED'
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.procedure_occurrence` op ON 
        op.person_id = p.person_id 
        AND op.procedure_concept_id = p.procedure_concept_id
        AND op.procedure_source_value = p.procedure_source_value
        AND COALESCE(op.procedure_source_concept_id, -1) = COALESCE(p.procedure_source_concept_id, -1)
        AND op.procedure_datetime = p.procedure_datetime
        AND op.procedure_type_concept_id = p.procedure_type_concept_id
        AND COALESCE(op.provider_id, -1) = -1
        AND COALESCE(op.visit_occurrence_id, -1) = COALESCE(p.visit_occurrence_id, -1)
        AND COALESCE(op.visit_detail_id, -1) = -1
    WHERE op.procedure_occurrence_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added procedure_occurrence', run_id, 'SUS_BRI_OP_010415_to_300619 - A6th_Procedure_OPCS -> procedure_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[tbl_SRCode] Transfer
    */
    SET max_current_id = COALESCE((SELECT procedure_occurrence_id FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.procedure_occurrence`
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_source_value,
        procedure_source_concept_id,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        provider_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as procedure_occurrence_id,
        p.person_id,
        p.procedure_concept_id,
        p.procedure_source_value,
        p.procedure_source_concept_id,
        p.procedure_date,
        p.procedure_datetime,
        p.procedure_type_concept_id,
        p.provider_id
    FROM
    (
        SELECT DISTINCT
            p.person_id person_id,
            dc.concept_id procedure_concept_id, 
            p.ctv3code procedure_source_value, 
            sc.concept_id procedure_source_concept_id,
            EXTRACT(DATE FROM dateevent) procedure_date,
            dateevent procedure_datetime,
            32817 procedure_type_concept_id, --EHR
            pr.provider_id provider_id
        FROM `CY_IMOSPHERE_WORKSPACE.tbl_SRCode` p
        JOIN `CY_LOOKUPS.tbl_CTV3ToSnomed_Map` sm ON sm.ctv3code = p.ctv3code
        JOIN `CY_CDM_VOCAB.concept` sc ON sc.concept_code = sm.SNOMEDCode
        JOIN `CY_CDM_VOCAB.concept_relationship` cr ON cr.concept_id_1 = sc.concept_id
        JOIN `CY_CDM_VOCAB.concept` dc ON dc.concept_id = cr.concept_id_2
        LEFT JOIN `CY_IMOSPHERE_CDM_531.provider` pr ON pr.provider_id = CAST(iddoneby AS INT64)
        WHERE sc.vocabulary_id = 'SNOMED'
        AND cr.relationship_id = 'Maps to'
        AND dc.standard_concept = 'S'
        AND dc.domain_id = 'Procedure'
        AND p.dateevent IS NOT NULL
        AND p.person_id IS NOT NULL
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.procedure_occurrence` op ON 
        op.person_id = p.person_id
        AND op.procedure_concept_id = p.procedure_concept_id
        AND op.procedure_source_value = p.procedure_source_value
        AND COALESCE(op.procedure_source_concept_id, -1) = COALESCE(p.procedure_source_concept_id, -1)
        AND op.procedure_datetime = p.procedure_datetime
        AND op.procedure_type_concept_id = p.procedure_type_concept_id
        AND COALESCE(op.provider_id, -1) = COALESCE(p.provider_id, -1)
        AND COALESCE(op.visit_occurrence_id, -1) = -1
        AND COALESCE(op.visit_detail_id, -1) = -1
    WHERE op.procedure_occurrence_id IS NULL;

     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added procedure_occurrence', run_id, 'tbl_SRCode -> procedure_occurrence', @@row_count, CURRENT_DATETIME();
END;