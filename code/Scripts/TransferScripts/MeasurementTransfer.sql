CREATE OR REPLACE PROCEDURE CY_IMOSPHERE_WORKSPACE.TRANSFER_MEASUREMENT(run_id INT64)
OPTIONS (strict_mode=false)
BEGIN
    DECLARE max_current_id INT64;

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    ------------[SUS_BRI_AE_010415_to_300619]
    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_First] Transfer   
    */
    SET max_current_id = COALESCE((SELECT measurement_id FROM `CY_IMOSPHERE_CDM_531.measurement` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.measurement`
    (
        measurement_id,
        person_id,
        measurement_concept_id,
        measurement_source_value,
        measurement_date,
        measurement_datetime,
        measurement_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as measurement_id,
        m.person_id,
        m.measurement_concept_id,
        m.measurement_source_value,
        m.measurement_date,
        m.measurement_datetime,
        m.measurement_type_concept_id,
        m.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            m.person_id person_id,
            mc.concept_id measurement_concept_id,
            m.Accident_And_Emergency_Investigation_First measurement_source_value,
            PARSE_DATE('%Y%m%d', m.Arrival_Date) measurement_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(m.Arrival_Date ,'000000')) measurement_datetime,
            32817 measurement_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` m
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` mc on TRIM(mc.source_value) = TRIM(m.Accident_And_Emergency_Investigation_First) AND mc.destination_table = 'MEASUREMENT' AND mc.source_table = 'SUS_BRI_AE_010415_to_300619' AND mc.source_column = 'Accident_And_Emergency_Investigation_First'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(m.AandE_Attendance_Number as STRING),'_', m.person_id) 
        WHERE (m.Arrival_Date IS NOT NULL AND m.Arrival_Date NOT LIKE '% %')
        AND m.person_id IS NOT NULL
    ) m
    LEFT JOIN `CY_IMOSPHERE_CDM_531.measurement` om ON 
        om.person_id = m.person_id
        AND om.measurement_concept_id = m.measurement_concept_id
        AND om.measurement_source_value = m.measurement_source_value
        AND COALESCE(om.measurement_source_concept_id, -1) = -1
        AND om.measurement_datetime = m.measurement_datetime
        AND om.measurement_type_concept_id = m.measurement_type_concept_id
        AND COALESCE(om.provider_id, -1) = -1
        AND COALESCE(om.visit_occurrence_id, -1) = COALESCE(m.visit_occurrence_id, -1)
        AND COALESCE(om.visit_detail_id, -1) = -1
        AND COALESCE(om.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(om.value_as_number AS STRING), '') = ''
        AND COALESCE(om.unit_concept_id, -1) =  -1
        AND COALESCE(om.unit_source_value, '') =  ''
        AND COALESCE(value_source_value, '') = ''
    WHERE om.measurement_id IS NULL;
         
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added measurement', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_First -> measurement', @@row_count, CURRENT_DATETIME();

    
    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Second] Transfer   
    */
    SET max_current_id = COALESCE((SELECT measurement_id FROM `CY_IMOSPHERE_CDM_531.measurement` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.measurement`
    (
        measurement_id,
        person_id,
        measurement_concept_id,
        measurement_source_value,
        measurement_date,
        measurement_datetime,
        measurement_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as measurement_id,
        m.person_id,
        m.measurement_concept_id,
        m.measurement_source_value,
        m.measurement_date,
        m.measurement_datetime,
        m.measurement_type_concept_id,
        m.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            m.person_id person_id,
            mc.concept_id measurement_concept_id,
            m.Accident_And_Emergency_Investigation_Second measurement_source_value,
            PARSE_DATE('%Y%m%d', m.Arrival_Date) measurement_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(m.Arrival_Date ,'000000')) measurement_datetime,
            32817 measurement_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` m
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` mc on TRIM(mc.source_value) = TRIM(m.Accident_And_Emergency_Investigation_Second) AND mc.destination_table = 'MEASUREMENT' AND mc.source_table = 'SUS_BRI_AE_010415_to_300619' AND mc.source_column = 'Accident_And_Emergency_Investigation_Second'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(m.AandE_Attendance_Number as STRING),'_', m.person_id) 
        WHERE (m.Arrival_Date IS NOT NULL AND m.Arrival_Date NOT LIKE '% %')
        AND m.person_id IS NOT NULL
    ) m
    LEFT JOIN `CY_IMOSPHERE_CDM_531.measurement` om ON 
        om.person_id = m.person_id
        AND om.measurement_concept_id = m.measurement_concept_id
        AND om.measurement_source_value = m.measurement_source_value
        AND COALESCE(om.measurement_source_concept_id, -1) = -1
        AND om.measurement_datetime = m.measurement_datetime
        AND om.measurement_type_concept_id = m.measurement_type_concept_id
        AND COALESCE(om.provider_id, -1) = -1
        AND COALESCE(om.visit_occurrence_id, -1) = COALESCE(m.visit_occurrence_id, -1)
        AND COALESCE(om.visit_detail_id, -1) = -1
        AND COALESCE(om.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(om.value_as_number AS STRING), '') = ''
        AND COALESCE(om.unit_concept_id, -1) =  -1
        AND COALESCE(om.unit_source_value, '') =  ''
        AND COALESCE(value_source_value, '') = ''
    WHERE om.measurement_id IS NULL;
         
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added measurement', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Second -> measurement', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Third] Transfer   
    */
    SET max_current_id = COALESCE((SELECT measurement_id FROM `CY_IMOSPHERE_CDM_531.measurement` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.measurement`
    (
        measurement_id,
        person_id,
        measurement_concept_id,
        measurement_source_value,
        measurement_date,
        measurement_datetime,
        measurement_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as measurement_id,
        m.person_id,
        m.measurement_concept_id,
        m.measurement_source_value,
        m.measurement_date,
        m.measurement_datetime,
        m.measurement_type_concept_id,
        m.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            m.person_id person_id,
            mc.concept_id measurement_concept_id,
            m.Accident_And_Emergency_Investigation_Third measurement_source_value,
            PARSE_DATE('%Y%m%d', m.Arrival_Date) measurement_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(m.Arrival_Date ,'000000')) measurement_datetime,
            32817 measurement_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` m
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` mc on TRIM(mc.source_value) = TRIM(m.Accident_And_Emergency_Investigation_Third) AND mc.destination_table = 'MEASUREMENT' AND mc.source_table = 'SUS_BRI_AE_010415_to_300619' AND mc.source_column = 'Accident_And_Emergency_Investigation_Third'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(m.AandE_Attendance_Number as STRING),'_', m.person_id) 
        WHERE (m.Arrival_Date IS NOT NULL AND m.Arrival_Date NOT LIKE '% %')
        AND m.person_id IS NOT NULL
    ) m
    LEFT JOIN `CY_IMOSPHERE_CDM_531.measurement` om ON 
        om.person_id = m.person_id
        AND om.measurement_concept_id = m.measurement_concept_id
        AND om.measurement_source_value = m.measurement_source_value
        AND COALESCE(om.measurement_source_concept_id, -1) = -1
        AND om.measurement_datetime = m.measurement_datetime
        AND om.measurement_type_concept_id = m.measurement_type_concept_id
        AND COALESCE(om.provider_id, -1) = -1
        AND COALESCE(om.visit_occurrence_id, -1) = COALESCE(m.visit_occurrence_id, -1)
        AND COALESCE(om.visit_detail_id, -1) = -1
        AND COALESCE(om.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(om.value_as_number AS STRING), '') = ''
        AND COALESCE(om.unit_concept_id, -1) =  -1
        AND COALESCE(om.unit_source_value, '') =  ''
        AND COALESCE(value_source_value, '') = ''
    WHERE om.measurement_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added measurement', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Third -> measurement', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Fourth] Transfer   
    */
    SET max_current_id = COALESCE((SELECT measurement_id FROM `CY_IMOSPHERE_CDM_531.measurement` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.measurement`
    (
        measurement_id,
        person_id,
        measurement_concept_id,
        measurement_source_value,
        measurement_date,
        measurement_datetime,
        measurement_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as measurement_id,
        m.person_id,
        m.measurement_concept_id,
        m.measurement_source_value,
        m.measurement_date,
        m.measurement_datetime,
        m.measurement_type_concept_id,
        m.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            m.person_id person_id,
            mc.concept_id measurement_concept_id,
            m.Accident_And_Emergency_Investigation_Fourth measurement_source_value,
            PARSE_DATE('%Y%m%d', m.Arrival_Date) measurement_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(m.Arrival_Date ,'000000')) measurement_datetime,
            32817 measurement_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` m
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` mc on TRIM(mc.source_value) = TRIM(m.Accident_And_Emergency_Investigation_Fourth) AND mc.destination_table = 'MEASUREMENT' AND mc.source_table = 'SUS_BRI_AE_010415_to_300619' AND mc.source_column = 'Accident_And_Emergency_Investigation_Fourth'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(m.AandE_Attendance_Number as STRING),'_', m.person_id) 
        WHERE (m.Arrival_Date IS NOT NULL AND m.Arrival_Date NOT LIKE '% %')
        AND m.person_id IS NOT NULL
    ) m
    LEFT JOIN `CY_IMOSPHERE_CDM_531.measurement` om ON 
        om.person_id = m.person_id
        AND om.measurement_concept_id = m.measurement_concept_id
        AND om.measurement_source_value = m.measurement_source_value
        AND COALESCE(om.measurement_source_concept_id, -1) = -1
        AND om.measurement_datetime = m.measurement_datetime
        AND om.measurement_type_concept_id = m.measurement_type_concept_id
        AND COALESCE(om.provider_id, -1) = -1
        AND COALESCE(om.visit_occurrence_id, -1) = COALESCE(m.visit_occurrence_id, -1)
        AND COALESCE(om.visit_detail_id, -1) = -1
        AND COALESCE(om.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(om.value_as_number AS STRING), '') = ''
        AND COALESCE(om.unit_concept_id, -1) =  -1
        AND COALESCE(om.unit_source_value, '') =  ''
        AND COALESCE(value_source_value, '') = ''
    WHERE om.measurement_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added measurement', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Fourth -> measurement', @@row_count, CURRENT_DATETIME();
        
    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Fifth] Transfer   
    */
    SET max_current_id = COALESCE((SELECT measurement_id FROM `CY_IMOSPHERE_CDM_531.measurement` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.measurement`
    (
        measurement_id,
        person_id,
        measurement_concept_id,
        measurement_source_value,
        measurement_date,
        measurement_datetime,
        measurement_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as measurement_id,
        m.person_id,
        m.measurement_concept_id,
        m.measurement_source_value,
        m.measurement_date,
        m.measurement_datetime,
        m.measurement_type_concept_id,
        m.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            m.person_id person_id,
            mc.concept_id measurement_concept_id,
            m.Accident_And_Emergency_Investigation_Fifth measurement_source_value,
            PARSE_DATE('%Y%m%d', m.Arrival_Date) measurement_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(m.Arrival_Date ,'000000')) measurement_datetime,
            32817 measurement_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` m
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` mc on TRIM(mc.source_value) = TRIM(m.Accident_And_Emergency_Investigation_Fifth) AND mc.destination_table = 'MEASUREMENT' AND mc.source_table = 'SUS_BRI_AE_010415_to_300619' AND mc.source_column = 'Accident_And_Emergency_Investigation_Fifth'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(m.AandE_Attendance_Number as STRING),'_', m.person_id) 
        WHERE (m.Arrival_Date IS NOT NULL AND m.Arrival_Date NOT LIKE '% %')
        AND m.person_id IS NOT NULL
    ) m
    LEFT JOIN `CY_IMOSPHERE_CDM_531.measurement` om ON 
        om.person_id = m.person_id
        AND om.measurement_concept_id = m.measurement_concept_id
        AND om.measurement_source_value = m.measurement_source_value
        AND COALESCE(om.measurement_source_concept_id, -1) = -1
        AND om.measurement_datetime = m.measurement_datetime
        AND om.measurement_type_concept_id = m.measurement_type_concept_id
        AND COALESCE(om.provider_id, -1) = -1
        AND COALESCE(om.visit_occurrence_id, -1) = COALESCE(m.visit_occurrence_id, -1)
        AND COALESCE(om.visit_detail_id, -1) = -1
        AND COALESCE(om.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(om.value_as_number AS STRING), '') = ''
        AND COALESCE(om.unit_concept_id, -1) =  -1
        AND COALESCE(om.unit_source_value, '') =  ''
        AND COALESCE(value_source_value, '') = ''
    WHERE om.measurement_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added measurement', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Fifth -> measurement', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Sixth] Transfer   
    */
    SET max_current_id = COALESCE((SELECT measurement_id FROM `CY_IMOSPHERE_CDM_531.measurement` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.measurement`
    (
        measurement_id,
        person_id,
        measurement_concept_id,
        measurement_source_value,
        measurement_date,
        measurement_datetime,
        measurement_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as measurement_id,
        m.person_id,
        m.measurement_concept_id,
        m.measurement_source_value,
        m.measurement_date,
        m.measurement_datetime,
        m.measurement_type_concept_id,
        m.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            m.person_id person_id,
            mc.concept_id measurement_concept_id,
            m.Accident_And_Emergency_Investigation_Sixth measurement_source_value,
            PARSE_DATE('%Y%m%d', m.Arrival_Date) measurement_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(m.Arrival_Date ,'000000')) measurement_datetime,
            32817 measurement_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` m
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` mc on TRIM(mc.source_value) = TRIM(m.Accident_And_Emergency_Investigation_Sixth) AND mc.destination_table = 'MEASUREMENT' AND mc.source_table = 'SUS_BRI_AE_010415_to_300619' AND mc.source_column = 'Accident_And_Emergency_Investigation_Sixth'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(m.AandE_Attendance_Number as STRING),'_', m.person_id) 
        WHERE (m.Arrival_Date IS NOT NULL AND m.Arrival_Date NOT LIKE '% %')
        AND m.person_id IS NOT NULL
    ) m
    LEFT JOIN `CY_IMOSPHERE_CDM_531.measurement` om ON 
        om.person_id = m.person_id
        AND om.measurement_concept_id = m.measurement_concept_id
        AND om.measurement_source_value = m.measurement_source_value
        AND COALESCE(om.measurement_source_concept_id, -1) = -1
        AND om.measurement_datetime = m.measurement_datetime
        AND om.measurement_type_concept_id = m.measurement_type_concept_id
        AND COALESCE(om.provider_id, -1) = -1
        AND COALESCE(om.visit_occurrence_id, -1) = COALESCE(m.visit_occurrence_id, -1)
        AND COALESCE(om.visit_detail_id, -1) = -1
        AND COALESCE(om.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(om.value_as_number AS STRING), '') = ''
        AND COALESCE(om.unit_concept_id, -1) =  -1
        AND COALESCE(om.unit_source_value, '') =  ''
        AND COALESCE(value_source_value, '') = ''
    WHERE om.measurement_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added measurement', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Sixth -> measurement', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Seventh] Transfer   
    */
    SET max_current_id = COALESCE((SELECT measurement_id FROM `CY_IMOSPHERE_CDM_531.measurement` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.measurement`
    (
        measurement_id,
        person_id,
        measurement_concept_id,
        measurement_source_value,
        measurement_date,
        measurement_datetime,
        measurement_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as measurement_id,
        m.person_id,
        m.measurement_concept_id,
        m.measurement_source_value,
        m.measurement_date,
        m.measurement_datetime,
        m.measurement_type_concept_id,
        m.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            m.person_id person_id,
            mc.concept_id measurement_concept_id,
            m.Accident_And_Emergency_Investigation_Seventh measurement_source_value,
            PARSE_DATE('%Y%m%d', m.Arrival_Date) measurement_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(m.Arrival_Date ,'000000')) measurement_datetime,
            32817 measurement_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` m
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` mc on TRIM(mc.source_value) = TRIM(m.Accident_And_Emergency_Investigation_Seventh) AND mc.destination_table = 'MEASUREMENT' AND mc.source_table = 'SUS_BRI_AE_010415_to_300619' AND mc.source_column = 'Accident_And_Emergency_Investigation_Seventh'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(m.AandE_Attendance_Number as STRING),'_', m.person_id) 
        WHERE (m.Arrival_Date IS NOT NULL AND m.Arrival_Date NOT LIKE '% %')
        AND m.person_id IS NOT NULL
    ) m
    LEFT JOIN `CY_IMOSPHERE_CDM_531.measurement` om ON 
        om.person_id = m.person_id
        AND om.measurement_concept_id = m.measurement_concept_id
        AND om.measurement_source_value = m.measurement_source_value
        AND COALESCE(om.measurement_source_concept_id, -1) = -1
        AND om.measurement_datetime = m.measurement_datetime
        AND om.measurement_type_concept_id = m.measurement_type_concept_id
        AND COALESCE(om.provider_id, -1) = -1
        AND COALESCE(om.visit_occurrence_id, -1) = COALESCE(m.visit_occurrence_id, -1)
        AND COALESCE(om.visit_detail_id, -1) = -1
        AND COALESCE(om.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(om.value_as_number AS STRING), '') = ''
        AND COALESCE(om.unit_concept_id, -1) =  -1
        AND COALESCE(om.unit_source_value, '') =  ''
        AND COALESCE(value_source_value, '') = ''
    WHERE om.measurement_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added measurement', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Seventh -> measurement', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Eighth] Transfer   
    */
    SET max_current_id = COALESCE((SELECT measurement_id FROM `CY_IMOSPHERE_CDM_531.measurement` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.measurement`
    (
        measurement_id,
        person_id,
        measurement_concept_id,
        measurement_source_value,
        measurement_date,
        measurement_datetime,
        measurement_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as measurement_id,
        m.person_id,
        m.measurement_concept_id,
        m.measurement_source_value,
        m.measurement_date,
        m.measurement_datetime,
        m.measurement_type_concept_id,
        m.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            m.person_id person_id,
            mc.concept_id measurement_concept_id,
            m.Accident_And_Emergency_Investigation_Eighth measurement_source_value,
            PARSE_DATE('%Y%m%d', m.Arrival_Date) measurement_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(m.Arrival_Date ,'000000')) measurement_datetime,
            32817 measurement_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` m
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` mc on TRIM(mc.source_value) = TRIM(m.Accident_And_Emergency_Investigation_Eighth) AND mc.destination_table = 'MEASUREMENT' AND mc.source_table = 'SUS_BRI_AE_010415_to_300619' AND mc.source_column = 'Accident_And_Emergency_Investigation_Eighth'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(m.AandE_Attendance_Number as STRING),'_', m.person_id) 
        WHERE (m.Arrival_Date IS NOT NULL AND m.Arrival_Date NOT LIKE '% %')
        AND m.person_id IS NOT NULL
    ) m
    LEFT JOIN `CY_IMOSPHERE_CDM_531.measurement` om ON 
        om.person_id = m.person_id
        AND om.measurement_concept_id = m.measurement_concept_id
        AND om.measurement_source_value = m.measurement_source_value
        AND COALESCE(om.measurement_source_concept_id, -1) = -1
        AND om.measurement_datetime = m.measurement_datetime
        AND om.measurement_type_concept_id = m.measurement_type_concept_id
        AND COALESCE(om.provider_id, -1) = -1
        AND COALESCE(om.visit_occurrence_id, -1) = COALESCE(m.visit_occurrence_id, -1)
        AND COALESCE(om.visit_detail_id, -1) = -1
        AND COALESCE(om.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(om.value_as_number AS STRING), '') = ''
        AND COALESCE(om.unit_concept_id, -1) =  -1
        AND COALESCE(om.unit_source_value, '') =  ''
        AND COALESCE(value_source_value, '') = ''
    WHERE om.measurement_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added measurement', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Eighth -> measurement', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Ninth] Transfer   
    */
    SET max_current_id = COALESCE((SELECT measurement_id FROM `CY_IMOSPHERE_CDM_531.measurement` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.measurement`
    (
        measurement_id,
        person_id,
        measurement_concept_id,
        measurement_source_value,
        measurement_date,
        measurement_datetime,
        measurement_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as measurement_id,
        m.person_id,
        m.measurement_concept_id,
        m.measurement_source_value,
        m.measurement_date,
        m.measurement_datetime,
        m.measurement_type_concept_id,
        m.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            m.person_id person_id,
            mc.concept_id measurement_concept_id,
            m.Accident_And_Emergency_Investigation_Ninth measurement_source_value,
            PARSE_DATE('%Y%m%d', m.Arrival_Date) measurement_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(m.Arrival_Date ,'000000')) measurement_datetime,
            32817 measurement_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` m
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` mc on TRIM(mc.source_value) = TRIM(m.Accident_And_Emergency_Investigation_Ninth) AND mc.destination_table = 'MEASUREMENT' AND mc.source_table = 'SUS_BRI_AE_010415_to_300619' AND mc.source_column = 'Accident_And_Emergency_Investigation_Ninth'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(m.AandE_Attendance_Number as STRING),'_', m.person_id) 
        WHERE (m.Arrival_Date IS NOT NULL AND m.Arrival_Date NOT LIKE '% %')
        AND m.person_id IS NOT NULL
    ) m
    LEFT JOIN `CY_IMOSPHERE_CDM_531.measurement` om ON 
        om.person_id = m.person_id
        AND om.measurement_concept_id = m.measurement_concept_id
        AND om.measurement_source_value = m.measurement_source_value
        AND COALESCE(om.measurement_source_concept_id, -1) = -1
        AND om.measurement_datetime = m.measurement_datetime
        AND om.measurement_type_concept_id = m.measurement_type_concept_id
        AND COALESCE(om.provider_id, -1) = -1
        AND COALESCE(om.visit_occurrence_id, -1) = COALESCE(m.visit_occurrence_id, -1)
        AND COALESCE(om.visit_detail_id, -1) = -1
        AND COALESCE(om.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(om.value_as_number AS STRING), '') = ''
        AND COALESCE(om.unit_concept_id, -1) =  -1
        AND COALESCE(om.unit_source_value, '') =  ''
        AND COALESCE(value_source_value, '') = ''
    WHERE om.measurement_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added measurement', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Ninth -> measurement', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Tenth] Transfer   
    */
    SET max_current_id = COALESCE((SELECT measurement_id FROM `CY_IMOSPHERE_CDM_531.measurement` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.measurement`
    (
        measurement_id,
        person_id,
        measurement_concept_id,
        measurement_source_value,
        measurement_date,
        measurement_datetime,
        measurement_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as measurement_id,
        m.person_id,
        m.measurement_concept_id,
        m.measurement_source_value,
        m.measurement_date,
        m.measurement_datetime,
        m.measurement_type_concept_id,
        m.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            m.person_id person_id,
            mc.concept_id measurement_concept_id,
            m.Accident_And_Emergency_Investigation_Tenth measurement_source_value,
            PARSE_DATE('%Y%m%d', m.Arrival_Date) measurement_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(m.Arrival_Date ,'000000')) measurement_datetime,
            32817 measurement_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` m
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` mc on TRIM(mc.source_value) = TRIM(m.Accident_And_Emergency_Investigation_Tenth) AND mc.destination_table = 'MEASUREMENT' AND mc.source_table = 'SUS_BRI_AE_010415_to_300619' AND mc.source_column = 'Accident_And_Emergency_Investigation_Tenth'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(m.AandE_Attendance_Number as STRING),'_', m.person_id) 
        WHERE (m.Arrival_Date IS NOT NULL AND m.Arrival_Date NOT LIKE '% %')
        AND m.person_id IS NOT NULL
    ) m
    LEFT JOIN `CY_IMOSPHERE_CDM_531.measurement` om ON 
        om.person_id = m.person_id
        AND om.measurement_concept_id = m.measurement_concept_id
        AND om.measurement_source_value = m.measurement_source_value
        AND COALESCE(om.measurement_source_concept_id, -1) = -1
        AND om.measurement_datetime = m.measurement_datetime
        AND om.measurement_type_concept_id = m.measurement_type_concept_id
        AND COALESCE(om.provider_id, -1) = -1
        AND COALESCE(om.visit_occurrence_id, -1) = COALESCE(m.visit_occurrence_id, -1)
        AND COALESCE(om.visit_detail_id, -1) = -1
        AND COALESCE(om.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(om.value_as_number AS STRING), '') = ''
        AND COALESCE(om.unit_concept_id, -1) =  -1
        AND COALESCE(om.unit_source_value, '') =  ''
        AND COALESCE(value_source_value, '') = ''
    WHERE om.measurement_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added measurement', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Tenth -> measurement', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Eleventh] Transfer   
    */
    SET max_current_id = COALESCE((SELECT measurement_id FROM `CY_IMOSPHERE_CDM_531.measurement` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.measurement`
    (
        measurement_id,
        person_id,
        measurement_concept_id,
        measurement_source_value,
        measurement_date,
        measurement_datetime,
        measurement_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as measurement_id,
        m.person_id,
        m.measurement_concept_id,
        m.measurement_source_value,
        m.measurement_date,
        m.measurement_datetime,
        m.measurement_type_concept_id,
        m.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            m.person_id person_id,
            mc.concept_id measurement_concept_id,
            m.Accident_And_Emergency_Investigation_Eleventh measurement_source_value,
            PARSE_DATE('%Y%m%d', m.Arrival_Date) measurement_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(m.Arrival_Date ,'000000')) measurement_datetime,
            32817 measurement_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` m
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` mc on TRIM(mc.source_value) = TRIM(m.Accident_And_Emergency_Investigation_Eleventh) AND mc.destination_table = 'MEASUREMENT' AND mc.source_table = 'SUS_BRI_AE_010415_to_300619' AND mc.source_column = 'Accident_And_Emergency_Investigation_Eleventh'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(m.AandE_Attendance_Number as STRING),'_', m.person_id) 
        WHERE (m.Arrival_Date IS NOT NULL AND m.Arrival_Date NOT LIKE '% %')
        AND m.person_id IS NOT NULL
    ) m
    LEFT JOIN `CY_IMOSPHERE_CDM_531.measurement` om ON 
        om.person_id = m.person_id
        AND om.measurement_concept_id = m.measurement_concept_id
        AND om.measurement_source_value = m.measurement_source_value
        AND COALESCE(om.measurement_source_concept_id, -1) = -1
        AND om.measurement_datetime = m.measurement_datetime
        AND om.measurement_type_concept_id = m.measurement_type_concept_id
        AND COALESCE(om.provider_id, -1) = -1
        AND COALESCE(om.visit_occurrence_id, -1) = COALESCE(m.visit_occurrence_id, -1)
        AND COALESCE(om.visit_detail_id, -1) = -1
        AND COALESCE(om.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(om.value_as_number AS STRING), '') = ''
        AND COALESCE(om.unit_concept_id, -1) =  -1
        AND COALESCE(om.unit_source_value, '') =  ''
        AND COALESCE(value_source_value, '') = ''
    WHERE om.measurement_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added measurement', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Eleventh -> measurement', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Twelfth] Transfer   
    */
    SET max_current_id = COALESCE((SELECT measurement_id FROM `CY_IMOSPHERE_CDM_531.measurement` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.measurement`
    (
        measurement_id,
        person_id,
        measurement_concept_id,
        measurement_source_value,
        measurement_date,
        measurement_datetime,
        measurement_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as measurement_id,
        m.person_id,
        m.measurement_concept_id,
        m.measurement_source_value,
        m.measurement_date,
        m.measurement_datetime,
        m.measurement_type_concept_id,
        m.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            m.person_id person_id,
            mc.concept_id measurement_concept_id,
            m.Accident_And_Emergency_Investigation_Twelfth measurement_source_value,
            PARSE_DATE('%Y%m%d', m.Arrival_Date) measurement_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(m.Arrival_Date ,'000000')) measurement_datetime,
            32817 measurement_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` m
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` mc on TRIM(mc.source_value) = TRIM(m.Accident_And_Emergency_Investigation_Twelfth) AND mc.destination_table = 'MEASUREMENT' AND mc.source_table = 'SUS_BRI_AE_010415_to_300619' AND mc.source_column = 'Accident_And_Emergency_Investigation_Twelfth'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(m.AandE_Attendance_Number as STRING),'_', m.person_id) 
        WHERE (m.Arrival_Date IS NOT NULL AND m.Arrival_Date NOT LIKE '% %')
        AND m.person_id IS NOT NULL
    ) m
    LEFT JOIN `CY_IMOSPHERE_CDM_531.measurement` om ON 
        om.person_id = m.person_id
        AND om.measurement_concept_id = m.measurement_concept_id
        AND om.measurement_source_value = m.measurement_source_value
        AND COALESCE(om.measurement_source_concept_id, -1) = -1
        AND om.measurement_datetime = m.measurement_datetime
        AND om.measurement_type_concept_id = m.measurement_type_concept_id
        AND COALESCE(om.provider_id, -1) = -1
        AND COALESCE(om.visit_occurrence_id, -1) = COALESCE(m.visit_occurrence_id, -1)
        AND COALESCE(om.visit_detail_id, -1) = -1
        AND COALESCE(om.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(om.value_as_number AS STRING), '') = ''
        AND COALESCE(om.unit_concept_id, -1) =  -1
        AND COALESCE(om.unit_source_value, '') =  ''
        AND COALESCE(value_source_value, '') = ''
    WHERE om.measurement_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added measurement', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Twelfth -> measurement', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_First] Transfer   
    */
    SET max_current_id = COALESCE((SELECT measurement_id FROM `CY_IMOSPHERE_CDM_531.measurement` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.measurement`
    (
        measurement_id,
        person_id,
        measurement_concept_id,
        measurement_source_value,
        measurement_date,
        measurement_datetime,
        measurement_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as measurement_id,
        m.person_id,
        m.measurement_concept_id,
        m.measurement_source_value,
        m.measurement_date,
        m.measurement_datetime,
        m.measurement_type_concept_id,
        m.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            m.person_id person_id,
            mc.concept_id measurement_concept_id,
            m.Accident_And_Emergency_Treatment_First measurement_source_value,
            PARSE_DATE('%Y%m%d', m.Arrival_Date) measurement_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(m.Arrival_Date ,'000000')) measurement_datetime,
            32817 measurement_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` m
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` mc on TRIM(mc.source_value) = TRIM(m.Accident_And_Emergency_Treatment_First) AND mc.destination_table = 'MEASUREMENT' AND mc.source_table = 'SUS_BRI_AE_010415_to_300619' AND mc.source_column = 'Accident_And_Emergency_Treatment_First'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(m.AandE_Attendance_Number as STRING),'_', m.person_id) 
        WHERE (m.Arrival_Date IS NOT NULL AND m.Arrival_Date NOT LIKE '% %')
        AND m.person_id IS NOT NULL
    ) m
    LEFT JOIN `CY_IMOSPHERE_CDM_531.measurement` om ON 
        om.person_id = m.person_id
        AND om.measurement_concept_id = m.measurement_concept_id
        AND om.measurement_source_value = m.measurement_source_value
        AND COALESCE(om.measurement_source_concept_id, -1) = -1
        AND om.measurement_datetime = m.measurement_datetime
        AND om.measurement_type_concept_id = m.measurement_type_concept_id
        AND COALESCE(om.provider_id, -1) = -1
        AND COALESCE(om.visit_occurrence_id, -1) = COALESCE(m.visit_occurrence_id, -1)
        AND COALESCE(om.visit_detail_id, -1) = -1
        AND COALESCE(om.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(om.value_as_number AS STRING), '') = ''
        AND COALESCE(om.unit_concept_id, -1) =  -1
        AND COALESCE(om.unit_source_value, '') =  ''
        AND COALESCE(value_source_value, '') = ''
    WHERE om.measurement_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added measurement', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_First -> measurement', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Second] Transfer   
    */
    SET max_current_id = COALESCE((SELECT measurement_id FROM `CY_IMOSPHERE_CDM_531.measurement` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.measurement`
    (
        measurement_id,
        person_id,
        measurement_concept_id,
        measurement_source_value,
        measurement_date,
        measurement_datetime,
        measurement_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as measurement_id,
        m.person_id,
        m.measurement_concept_id,
        m.measurement_source_value,
        m.measurement_date,
        m.measurement_datetime,
        m.measurement_type_concept_id,
        m.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            m.person_id person_id,
            mc.concept_id measurement_concept_id,
            m.Accident_And_Emergency_Treatment_Second measurement_source_value,
            PARSE_DATE('%Y%m%d', m.Arrival_Date) measurement_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(m.Arrival_Date ,'000000')) measurement_datetime,
            32817 measurement_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` m
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` mc on TRIM(mc.source_value) = TRIM(m.Accident_And_Emergency_Treatment_Second) AND mc.destination_table = 'MEASUREMENT' AND mc.source_table = 'SUS_BRI_AE_010415_to_300619' AND mc.source_column = 'Accident_And_Emergency_Treatment_Second'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(m.AandE_Attendance_Number as STRING),'_', m.person_id) 
        WHERE (m.Arrival_Date IS NOT NULL AND m.Arrival_Date NOT LIKE '% %')
        AND m.person_id IS NOT NULL
    ) m
    LEFT JOIN `CY_IMOSPHERE_CDM_531.measurement` om ON 
        om.person_id = m.person_id
        AND om.measurement_concept_id = m.measurement_concept_id
        AND om.measurement_source_value = m.measurement_source_value
        AND COALESCE(om.measurement_source_concept_id, -1) = -1
        AND om.measurement_datetime = m.measurement_datetime
        AND om.measurement_type_concept_id = m.measurement_type_concept_id
        AND COALESCE(om.provider_id, -1) = -1
        AND COALESCE(om.visit_occurrence_id, -1) = COALESCE(m.visit_occurrence_id, -1)
        AND COALESCE(om.visit_detail_id, -1) = -1
        AND COALESCE(om.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(om.value_as_number AS STRING), '') = ''
        AND COALESCE(om.unit_concept_id, -1) =  -1
        AND COALESCE(om.unit_source_value, '') =  ''
        AND COALESCE(value_source_value, '') = ''
    WHERE om.measurement_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added measurement', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Second -> measurement', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Third] Transfer   
    */
    SET max_current_id = COALESCE((SELECT measurement_id FROM `CY_IMOSPHERE_CDM_531.measurement` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.measurement`
    (
        measurement_id,
        person_id,
        measurement_concept_id,
        measurement_source_value,
        measurement_date,
        measurement_datetime,
        measurement_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as measurement_id,
        m.person_id,
        m.measurement_concept_id,
        m.measurement_source_value,
        m.measurement_date,
        m.measurement_datetime,
        m.measurement_type_concept_id,
        m.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            m.person_id person_id,
            mc.concept_id measurement_concept_id,
            m.Accident_And_Emergency_Treatment_Third measurement_source_value,
            PARSE_DATE('%Y%m%d', m.Arrival_Date) measurement_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(m.Arrival_Date ,'000000')) measurement_datetime,
            32817 measurement_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` m
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` mc on TRIM(mc.source_value) = TRIM(m.Accident_And_Emergency_Treatment_Third) AND mc.destination_table = 'MEASUREMENT' AND mc.source_table = 'SUS_BRI_AE_010415_to_300619' AND mc.source_column = 'Accident_And_Emergency_Treatment_Third'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(m.AandE_Attendance_Number as STRING),'_', m.person_id) 
        WHERE (m.Arrival_Date IS NOT NULL AND m.Arrival_Date NOT LIKE '% %')
        AND m.person_id IS NOT NULL
    ) m
    LEFT JOIN `CY_IMOSPHERE_CDM_531.measurement` om ON 
        om.person_id = m.person_id
        AND om.measurement_concept_id = m.measurement_concept_id
        AND om.measurement_source_value = m.measurement_source_value
        AND COALESCE(om.measurement_source_concept_id, -1) = -1
        AND om.measurement_datetime = m.measurement_datetime
        AND om.measurement_type_concept_id = m.measurement_type_concept_id
        AND COALESCE(om.provider_id, -1) = -1
        AND COALESCE(om.visit_occurrence_id, -1) = COALESCE(m.visit_occurrence_id, -1)
        AND COALESCE(om.visit_detail_id, -1) = -1
        AND COALESCE(om.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(om.value_as_number AS STRING), '') = ''
        AND COALESCE(om.unit_concept_id, -1) =  -1
        AND COALESCE(om.unit_source_value, '') =  ''
        AND COALESCE(value_source_value, '') = ''
    WHERE om.measurement_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added measurement', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Third -> measurement', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Fourth] Transfer   
    */
    SET max_current_id = COALESCE((SELECT measurement_id FROM `CY_IMOSPHERE_CDM_531.measurement` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.measurement`
    (
        measurement_id,
        person_id,
        measurement_concept_id,
        measurement_source_value,
        measurement_date,
        measurement_datetime,
        measurement_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as measurement_id,
        m.person_id,
        m.measurement_concept_id,
        m.measurement_source_value,
        m.measurement_date,
        m.measurement_datetime,
        m.measurement_type_concept_id,
        m.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            m.person_id person_id,
            mc.concept_id measurement_concept_id,
            m.Accident_And_Emergency_Treatment_Fourth measurement_source_value,
            PARSE_DATE('%Y%m%d', m.Arrival_Date) measurement_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(m.Arrival_Date ,'000000')) measurement_datetime,
            32817 measurement_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` m
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` mc on TRIM(mc.source_value) = TRIM(m.Accident_And_Emergency_Treatment_Fourth) AND mc.destination_table = 'MEASUREMENT' AND mc.source_table = 'SUS_BRI_AE_010415_to_300619' AND mc.source_column = 'Accident_And_Emergency_Treatment_Fourth'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(m.AandE_Attendance_Number as STRING),'_', m.person_id) 
        WHERE (m.Arrival_Date IS NOT NULL AND m.Arrival_Date NOT LIKE '% %')
        AND m.person_id IS NOT NULL
    ) m
    LEFT JOIN `CY_IMOSPHERE_CDM_531.measurement` om ON 
        om.person_id = m.person_id
        AND om.measurement_concept_id = m.measurement_concept_id
        AND om.measurement_source_value = m.measurement_source_value
        AND COALESCE(om.measurement_source_concept_id, -1) = -1
        AND om.measurement_datetime = m.measurement_datetime
        AND om.measurement_type_concept_id = m.measurement_type_concept_id
        AND COALESCE(om.provider_id, -1) = -1
        AND COALESCE(om.visit_occurrence_id, -1) = COALESCE(m.visit_occurrence_id, -1)
        AND COALESCE(om.visit_detail_id, -1) = -1
        AND COALESCE(om.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(om.value_as_number AS STRING), '') = ''
        AND COALESCE(om.unit_concept_id, -1) =  -1
        AND COALESCE(om.unit_source_value, '') =  ''
        AND COALESCE(value_source_value, '') = ''
    WHERE om.measurement_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added measurement', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Fourth -> measurement', @@row_count, CURRENT_DATETIME();
        
    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Fifth] Transfer   
    */
    SET max_current_id = COALESCE((SELECT measurement_id FROM `CY_IMOSPHERE_CDM_531.measurement` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.measurement`
    (
        measurement_id,
        person_id,
        measurement_concept_id,
        measurement_source_value,
        measurement_date,
        measurement_datetime,
        measurement_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as measurement_id,
        m.person_id,
        m.measurement_concept_id,
        m.measurement_source_value,
        m.measurement_date,
        m.measurement_datetime,
        m.measurement_type_concept_id,
        m.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            m.person_id person_id,
            mc.concept_id measurement_concept_id,
            m.Accident_And_Emergency_Treatment_Fifth measurement_source_value,
            PARSE_DATE('%Y%m%d', m.Arrival_Date) measurement_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(m.Arrival_Date ,'000000')) measurement_datetime,
            32817 measurement_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` m
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` mc on TRIM(mc.source_value) = TRIM(m.Accident_And_Emergency_Treatment_Fifth) AND mc.destination_table = 'MEASUREMENT' AND mc.source_table = 'SUS_BRI_AE_010415_to_300619' AND mc.source_column = 'Accident_And_Emergency_Treatment_Fifth'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(m.AandE_Attendance_Number as STRING),'_', m.person_id) 
        WHERE (m.Arrival_Date IS NOT NULL AND m.Arrival_Date NOT LIKE '% %')
        AND m.person_id IS NOT NULL
    ) m
    LEFT JOIN `CY_IMOSPHERE_CDM_531.measurement` om ON 
        om.person_id = m.person_id
        AND om.measurement_concept_id = m.measurement_concept_id
        AND om.measurement_source_value = m.measurement_source_value
        AND COALESCE(om.measurement_source_concept_id, -1) = -1
        AND om.measurement_datetime = m.measurement_datetime
        AND om.measurement_type_concept_id = m.measurement_type_concept_id
        AND COALESCE(om.provider_id, -1) = -1
        AND COALESCE(om.visit_occurrence_id, -1) = COALESCE(m.visit_occurrence_id, -1)
        AND COALESCE(om.visit_detail_id, -1) = -1
        AND COALESCE(om.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(om.value_as_number AS STRING), '') = ''
        AND COALESCE(om.unit_concept_id, -1) =  -1
        AND COALESCE(om.unit_source_value, '') =  ''
        AND COALESCE(value_source_value, '') = ''
    WHERE om.measurement_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added measurement', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Fifth -> measurement', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Sixth] Transfer   
    */
    SET max_current_id = COALESCE((SELECT measurement_id FROM `CY_IMOSPHERE_CDM_531.measurement` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.measurement`
    (
        measurement_id,
        person_id,
        measurement_concept_id,
        measurement_source_value,
        measurement_date,
        measurement_datetime,
        measurement_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as measurement_id,
        m.person_id,
        m.measurement_concept_id,
        m.measurement_source_value,
        m.measurement_date,
        m.measurement_datetime,
        m.measurement_type_concept_id,
        m.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            m.person_id person_id,
            mc.concept_id measurement_concept_id,
            m.Accident_And_Emergency_Treatment_Sixth measurement_source_value,
            PARSE_DATE('%Y%m%d', m.Arrival_Date) measurement_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(m.Arrival_Date ,'000000')) measurement_datetime,
            32817 measurement_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` m
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` mc on TRIM(mc.source_value) = TRIM(m.Accident_And_Emergency_Treatment_Sixth) AND mc.destination_table = 'MEASUREMENT' AND mc.source_table = 'SUS_BRI_AE_010415_to_300619' AND mc.source_column = 'Accident_And_Emergency_Treatment_Sixth'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(m.AandE_Attendance_Number as STRING),'_', m.person_id) 
        WHERE (m.Arrival_Date IS NOT NULL AND m.Arrival_Date NOT LIKE '% %')
        AND m.person_id IS NOT NULL
    ) m
    LEFT JOIN `CY_IMOSPHERE_CDM_531.measurement` om ON 
        om.person_id = m.person_id
        AND om.measurement_concept_id = m.measurement_concept_id
        AND om.measurement_source_value = m.measurement_source_value
        AND COALESCE(om.measurement_source_concept_id, -1) = -1
        AND om.measurement_datetime = m.measurement_datetime
        AND om.measurement_type_concept_id = m.measurement_type_concept_id
        AND COALESCE(om.provider_id, -1) = -1
        AND COALESCE(om.visit_occurrence_id, -1) = COALESCE(m.visit_occurrence_id, -1)
        AND COALESCE(om.visit_detail_id, -1) = -1
        AND COALESCE(om.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(om.value_as_number AS STRING), '') = ''
        AND COALESCE(om.unit_concept_id, -1) =  -1
        AND COALESCE(om.unit_source_value, '') =  ''
        AND COALESCE(value_source_value, '') = ''
    WHERE om.measurement_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added measurement', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Sixth -> measurement', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Seventh] Transfer   
    */
    SET max_current_id = COALESCE((SELECT measurement_id FROM `CY_IMOSPHERE_CDM_531.measurement` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.measurement`
    (
        measurement_id,
        person_id,
        measurement_concept_id,
        measurement_source_value,
        measurement_date,
        measurement_datetime,
        measurement_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as measurement_id,
        m.person_id,
        m.measurement_concept_id,
        m.measurement_source_value,
        m.measurement_date,
        m.measurement_datetime,
        m.measurement_type_concept_id,
        m.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            m.person_id person_id,
            mc.concept_id measurement_concept_id,
            m.Accident_And_Emergency_Treatment_Seventh measurement_source_value,
            PARSE_DATE('%Y%m%d', m.Arrival_Date) measurement_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(m.Arrival_Date ,'000000')) measurement_datetime,
            32817 measurement_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` m
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` mc on TRIM(mc.source_value) = TRIM(m.Accident_And_Emergency_Treatment_Seventh) AND mc.destination_table = 'MEASUREMENT' AND mc.source_table = 'SUS_BRI_AE_010415_to_300619' AND mc.source_column = 'Accident_And_Emergency_Treatment_Seventh'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(m.AandE_Attendance_Number as STRING),'_', m.person_id) 
        WHERE (m.Arrival_Date IS NOT NULL AND m.Arrival_Date NOT LIKE '% %')
        AND m.person_id IS NOT NULL
    ) m
    LEFT JOIN `CY_IMOSPHERE_CDM_531.measurement` om ON 
        om.person_id = m.person_id
        AND om.measurement_concept_id = m.measurement_concept_id
        AND om.measurement_source_value = m.measurement_source_value
        AND COALESCE(om.measurement_source_concept_id, -1) = -1
        AND om.measurement_datetime = m.measurement_datetime
        AND om.measurement_type_concept_id = m.measurement_type_concept_id
        AND COALESCE(om.provider_id, -1) = -1
        AND COALESCE(om.visit_occurrence_id, -1) = COALESCE(m.visit_occurrence_id, -1)
        AND COALESCE(om.visit_detail_id, -1) = -1
        AND COALESCE(om.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(om.value_as_number AS STRING), '') = ''
        AND COALESCE(om.unit_concept_id, -1) =  -1
        AND COALESCE(om.unit_source_value, '') =  ''
        AND COALESCE(value_source_value, '') = ''
    WHERE om.measurement_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added measurement', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Seventh -> measurement', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Eighth] Transfer   
    */
    SET max_current_id = COALESCE((SELECT measurement_id FROM `CY_IMOSPHERE_CDM_531.measurement` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.measurement`
    (
        measurement_id,
        person_id,
        measurement_concept_id,
        measurement_source_value,
        measurement_date,
        measurement_datetime,
        measurement_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as measurement_id,
        m.person_id,
        m.measurement_concept_id,
        m.measurement_source_value,
        m.measurement_date,
        m.measurement_datetime,
        m.measurement_type_concept_id,
        m.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            m.person_id person_id,
            mc.concept_id measurement_concept_id,
            m.Accident_And_Emergency_Treatment_Eighth measurement_source_value,
            PARSE_DATE('%Y%m%d', m.Arrival_Date) measurement_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(m.Arrival_Date ,'000000')) measurement_datetime,
            32817 measurement_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` m
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` mc on TRIM(mc.source_value) = TRIM(m.Accident_And_Emergency_Treatment_Eighth) AND mc.destination_table = 'MEASUREMENT' AND mc.source_table = 'SUS_BRI_AE_010415_to_300619' AND mc.source_column = 'Accident_And_Emergency_Treatment_Eighth'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(m.AandE_Attendance_Number as STRING),'_', m.person_id) 
        WHERE (m.Arrival_Date IS NOT NULL AND m.Arrival_Date NOT LIKE '% %')
        AND m.person_id IS NOT NULL
    ) m
    LEFT JOIN `CY_IMOSPHERE_CDM_531.measurement` om ON 
        om.person_id = m.person_id
        AND om.measurement_concept_id = m.measurement_concept_id
        AND om.measurement_source_value = m.measurement_source_value
        AND COALESCE(om.measurement_source_concept_id, -1) = -1
        AND om.measurement_datetime = m.measurement_datetime
        AND om.measurement_type_concept_id = m.measurement_type_concept_id
        AND COALESCE(om.provider_id, -1) = -1
        AND COALESCE(om.visit_occurrence_id, -1) = COALESCE(m.visit_occurrence_id, -1)
        AND COALESCE(om.visit_detail_id, -1) = -1
        AND COALESCE(om.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(om.value_as_number AS STRING), '') = ''
        AND COALESCE(om.unit_concept_id, -1) =  -1
        AND COALESCE(om.unit_source_value, '') =  ''
        AND COALESCE(value_source_value, '') = ''
    WHERE om.measurement_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added measurement', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Eighth -> measurement', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Ninth] Transfer   
    */
    SET max_current_id = COALESCE((SELECT measurement_id FROM `CY_IMOSPHERE_CDM_531.measurement` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.measurement`
    (
        measurement_id,
        person_id,
        measurement_concept_id,
        measurement_source_value,
        measurement_date,
        measurement_datetime,
        measurement_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as measurement_id,
        m.person_id,
        m.measurement_concept_id,
        m.measurement_source_value,
        m.measurement_date,
        m.measurement_datetime,
        m.measurement_type_concept_id,
        m.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            m.person_id person_id,
            mc.concept_id measurement_concept_id,
            m.Accident_And_Emergency_Treatment_Ninth measurement_source_value,
            PARSE_DATE('%Y%m%d', m.Arrival_Date) measurement_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(m.Arrival_Date ,'000000')) measurement_datetime,
            32817 measurement_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` m
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` mc on TRIM(mc.source_value) = TRIM(m.Accident_And_Emergency_Treatment_Ninth) AND mc.destination_table = 'MEASUREMENT' AND mc.source_table = 'SUS_BRI_AE_010415_to_300619' AND mc.source_column = 'Accident_And_Emergency_Treatment_Ninth'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(m.AandE_Attendance_Number as STRING),'_', m.person_id) 
        WHERE (m.Arrival_Date IS NOT NULL AND m.Arrival_Date NOT LIKE '% %')
        AND m.person_id IS NOT NULL
    ) m
    LEFT JOIN `CY_IMOSPHERE_CDM_531.measurement` om ON 
        om.person_id = m.person_id
        AND om.measurement_concept_id = m.measurement_concept_id
        AND om.measurement_source_value = m.measurement_source_value
        AND COALESCE(om.measurement_source_concept_id, -1) = -1
        AND om.measurement_datetime = m.measurement_datetime
        AND om.measurement_type_concept_id = m.measurement_type_concept_id
        AND COALESCE(om.provider_id, -1) = -1
        AND COALESCE(om.visit_occurrence_id, -1) = COALESCE(m.visit_occurrence_id, -1)
        AND COALESCE(om.visit_detail_id, -1) = -1
        AND COALESCE(om.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(om.value_as_number AS STRING), '') = ''
        AND COALESCE(om.unit_concept_id, -1) =  -1
        AND COALESCE(om.unit_source_value, '') =  ''
        AND COALESCE(value_source_value, '') = ''
    WHERE om.measurement_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added measurement', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Ninth -> measurement', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Tenth] Transfer   
    */
    SET max_current_id = COALESCE((SELECT measurement_id FROM `CY_IMOSPHERE_CDM_531.measurement` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.measurement`
    (
        measurement_id,
        person_id,
        measurement_concept_id,
        measurement_source_value,
        measurement_date,
        measurement_datetime,
        measurement_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as measurement_id,
        m.person_id,
        m.measurement_concept_id,
        m.measurement_source_value,
        m.measurement_date,
        m.measurement_datetime,
        m.measurement_type_concept_id,
        m.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            m.person_id person_id,
            mc.concept_id measurement_concept_id,
            m.Accident_And_Emergency_Treatment_Tenth measurement_source_value,
            PARSE_DATE('%Y%m%d', m.Arrival_Date) measurement_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(m.Arrival_Date ,'000000')) measurement_datetime,
            32817 measurement_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` m
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` mc on TRIM(mc.source_value) = TRIM(m.Accident_And_Emergency_Treatment_Tenth) AND mc.destination_table = 'MEASUREMENT' AND mc.source_table = 'SUS_BRI_AE_010415_to_300619' AND mc.source_column = 'Accident_And_Emergency_Treatment_Tenth'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(m.AandE_Attendance_Number as STRING),'_', m.person_id) 
        WHERE (m.Arrival_Date IS NOT NULL AND m.Arrival_Date NOT LIKE '% %')
        AND m.person_id IS NOT NULL
    ) m
    LEFT JOIN `CY_IMOSPHERE_CDM_531.measurement` om ON 
        om.person_id = m.person_id
        AND om.measurement_concept_id = m.measurement_concept_id
        AND om.measurement_source_value = m.measurement_source_value
        AND COALESCE(om.measurement_source_concept_id, -1) = -1
        AND om.measurement_datetime = m.measurement_datetime
        AND om.measurement_type_concept_id = m.measurement_type_concept_id
        AND COALESCE(om.provider_id, -1) = -1
        AND COALESCE(om.visit_occurrence_id, -1) = COALESCE(m.visit_occurrence_id, -1)
        AND COALESCE(om.visit_detail_id, -1) = -1
        AND COALESCE(om.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(om.value_as_number AS STRING), '') = ''
        AND COALESCE(om.unit_concept_id, -1) =  -1
        AND COALESCE(om.unit_source_value, '') =  ''
        AND COALESCE(value_source_value, '') = ''
    WHERE om.measurement_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added measurement', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Tenth -> measurement', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Eleventh] Transfer   
    */
    SET max_current_id = COALESCE((SELECT measurement_id FROM `CY_IMOSPHERE_CDM_531.measurement` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.measurement`
    (
        measurement_id,
        person_id,
        measurement_concept_id,
        measurement_source_value,
        measurement_date,
        measurement_datetime,
        measurement_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as measurement_id,
        m.person_id,
        m.measurement_concept_id,
        m.measurement_source_value,
        m.measurement_date,
        m.measurement_datetime,
        m.measurement_type_concept_id,
        m.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            m.person_id person_id,
            mc.concept_id measurement_concept_id,
            m.Accident_And_Emergency_Treatment_Eleventh measurement_source_value,
            PARSE_DATE('%Y%m%d', m.Arrival_Date) measurement_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(m.Arrival_Date ,'000000')) measurement_datetime,
            32817 measurement_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` m
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` mc on TRIM(mc.source_value) = TRIM(m.Accident_And_Emergency_Treatment_Eleventh) AND mc.destination_table = 'MEASUREMENT' AND mc.source_table = 'SUS_BRI_AE_010415_to_300619' AND mc.source_column = 'Accident_And_Emergency_Treatment_Eleventh'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(m.AandE_Attendance_Number as STRING),'_', m.person_id) 
        WHERE (m.Arrival_Date IS NOT NULL AND m.Arrival_Date NOT LIKE '% %')
        AND m.person_id IS NOT NULL
    ) m
    LEFT JOIN `CY_IMOSPHERE_CDM_531.measurement` om ON 
        om.person_id = m.person_id
        AND om.measurement_concept_id = m.measurement_concept_id
        AND om.measurement_source_value = m.measurement_source_value
        AND COALESCE(om.measurement_source_concept_id, -1) = -1
        AND om.measurement_datetime = m.measurement_datetime
        AND om.measurement_type_concept_id = m.measurement_type_concept_id
        AND COALESCE(om.provider_id, -1) = -1
        AND COALESCE(om.visit_occurrence_id, -1) = COALESCE(m.visit_occurrence_id, -1)
        AND COALESCE(om.visit_detail_id, -1) = -1
        AND COALESCE(om.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(om.value_as_number AS STRING), '') = ''
        AND COALESCE(om.unit_concept_id, -1) =  -1
        AND COALESCE(om.unit_source_value, '') =  ''
        AND COALESCE(value_source_value, '') = ''
    WHERE om.measurement_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added measurement', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Eleventh -> measurement', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Twelfth] Transfer   
    */
    SET max_current_id = COALESCE((SELECT measurement_id FROM `CY_IMOSPHERE_CDM_531.measurement` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.measurement`
    (
        measurement_id,
        person_id,
        measurement_concept_id,
        measurement_source_value,
        measurement_date,
        measurement_datetime,
        measurement_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as measurement_id,
        m.person_id,
        m.measurement_concept_id,
        m.measurement_source_value,
        m.measurement_date,
        m.measurement_datetime,
        m.measurement_type_concept_id,
        m.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            m.person_id person_id,
            mc.concept_id measurement_concept_id,
            m.Accident_And_Emergency_Treatment_Twelfth measurement_source_value,
            PARSE_DATE('%Y%m%d', m.Arrival_Date) measurement_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(m.Arrival_Date ,'000000')) measurement_datetime,
            32817 measurement_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` m
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` mc on TRIM(mc.source_value) = TRIM(m.Accident_And_Emergency_Treatment_Twelfth) AND mc.destination_table = 'MEASUREMENT' AND mc.source_table = 'SUS_BRI_AE_010415_to_300619' AND mc.source_column = 'Accident_And_Emergency_Treatment_Twelfth'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(m.AandE_Attendance_Number as STRING),'_', m.person_id) 
        WHERE (m.Arrival_Date IS NOT NULL AND m.Arrival_Date NOT LIKE '% %')
        AND m.person_id IS NOT NULL
    ) m
    LEFT JOIN `CY_IMOSPHERE_CDM_531.measurement` om ON 
        om.person_id = m.person_id
        AND om.measurement_concept_id = m.measurement_concept_id
        AND om.measurement_source_value = m.measurement_source_value
        AND COALESCE(om.measurement_source_concept_id, -1) = -1
        AND om.measurement_datetime = m.measurement_datetime
        AND om.measurement_type_concept_id = m.measurement_type_concept_id
        AND COALESCE(om.provider_id, -1) = -1
        AND COALESCE(om.visit_occurrence_id, -1) = COALESCE(m.visit_occurrence_id, -1)
        AND COALESCE(om.visit_detail_id, -1) = -1
        AND COALESCE(om.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(om.value_as_number AS STRING), '') = ''
        AND COALESCE(om.unit_concept_id, -1) =  -1
        AND COALESCE(om.unit_source_value, '') =  ''
        AND COALESCE(value_source_value, '') = ''
    WHERE om.measurement_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added measurement', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Twelfth -> measurement', @@row_count, CURRENT_DATETIME();
    
    /*
        --[tbl_SRCode] Transfer
    */
    SET max_current_id = COALESCE((SELECT measurement_id FROM `CY_IMOSPHERE_CDM_531.measurement` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.measurement`
    (
        measurement_id,
        person_id,
        measurement_concept_id,
        measurement_source_value,
        measurement_source_concept_id,
        measurement_date,
        measurement_datetime,
        measurement_time,
        measurement_type_concept_id,
        value_as_number,
        value_source_value,
        unit_concept_id,
        unit_source_value,
        provider_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as measurement_id,
        m.person_id,
        m.measurement_concept_id,
        m.measurement_source_value,
        m.measurement_source_concept_id,
        m.measurement_date,
        m.measurement_datetime,
        m.measurement_time,
        m.measurement_type_concept_id,
        m.value_as_number,
        m.value_source_value,
        m.unit_concept_id,
        m.unit_source_value,
        m.provider_id

    FROM
    (
        SELECT DISTINCT
            m.person_id person_id,
            dc.concept_id measurement_concept_id,
            m.ctv3code measurement_source_value,
            sc.concept_id measurement_source_concept_id,
            EXTRACT(DATE FROM dateevent) measurement_date,
            dateevent measurement_datetime,
            CAST(EXTRACT(TIME FROM dateevent) AS STRING) measurement_time,
            32817 measurement_type_concept_id, --EHR
            CAST(numericvalue AS FLOAT64) value_as_number,
            numericvalue value_source_value,
            unit.concept_id unit_concept_id,
            numericunit unit_source_value,
            p.provider_id provider_id
        FROM `CY_IMOSPHERE_WORKSPACE.tbl_SRCode` m
        JOIN `CY_LOOKUPS.tbl_CTV3ToSnomed_Map` sm ON sm.ctv3code = m.ctv3code
        JOIN `CY_CDM_VOCAB.concept` sc ON sc.concept_code = sm.SNOMEDCode
        JOIN `CY_CDM_VOCAB.concept_relationship` cr ON cr.concept_id_1 = sc.concept_id
        JOIN `CY_CDM_VOCAB.concept` dc ON dc.concept_id = cr.concept_id_2
        LEFT JOIN `CY_IMOSPHERE_CDM_531.provider` p ON p.provider_id = CAST(iddoneby AS INT64)
        LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` unit on unit.source_value = TRIM(m.numericunit) AND unit.destination_table = 'MEASUREMENT' AND unit.source_table = 'tbl_SRCode' AND unit.source_column = 'numericunit'
        WHERE sc.vocabulary_id = 'SNOMED'
        AND cr.relationship_id = 'Maps to'
        AND dc.standard_concept = 'S'
        AND dc.domain_id = 'Measurement'
        AND m.dateevent IS NOT NULL
        AND m.person_id IS NOT NULL
    ) m
    LEFT JOIN `CY_IMOSPHERE_CDM_531.measurement` om ON 
        om.person_id = m.person_id
        AND om.measurement_concept_id = m.measurement_concept_id
        AND om.measurement_source_value = m.measurement_source_value
        AND COALESCE(om.measurement_source_concept_id, -1) = COALESCE(m.measurement_source_concept_id, -1)
        AND om.measurement_datetime = m.measurement_datetime
        AND om.measurement_type_concept_id = m.measurement_type_concept_id
        AND COALESCE(om.provider_id, -1) = COALESCE(m.provider_id, -1)
        AND COALESCE(om.visit_occurrence_id, -1) = -1
        AND COALESCE(om.visit_detail_id, -1) = -1
        AND COALESCE(om.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(om.value_as_number AS STRING), '') = COALESCE(CAST(m.value_as_number AS STRING), '')
        AND COALESCE(om.unit_concept_id, -1) = COALESCE(m.unit_concept_id, -1)
        AND COALESCE(om.unit_source_value, '') =  COALESCE(m.unit_source_value, '')
        AND COALESCE(om.value_source_value, '') = COALESCE(m.value_source_value, '')
    WHERE om.measurement_id IS NULL;

     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added measurement', run_id, 'tbl_SRCode -> measurement', @@row_count, CURRENT_DATETIME();
END;