    
CREATE OR REPLACE PROCEDURE CY_IMOSPHERE_WORKSPACE.TRANSFER_VISIT_DETAIL(run_id INT64)
OPTIONS (strict_mode=false)
BEGIN 
    /*
        Build mapping tables. These are required because there's no universal unique source id.
    */

    DECLARE max_current_id INT64;
    CREATE TABLE IF NOT EXISTS CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups(id INT64, source_value STRING);

    SET max_current_id = COALESCE((SELECT id FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups` ORDER BY 1 DESC LIMIT 1), 0);
    
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups` (id, source_value)
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() id,
        vd.source_value
    FROM
    (
        SELECT DISTINCT
            vd.source_value
        FROM
        (
            SELECT DISTINCT
                CONCAT(CAST(vd.Hospital_Provider_Spell_Number as STRING),'_', vd.person_id, '_', vd.Episode_Number) source_value          
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1` vd
            WHERE vd.Hospital_Provider_Spell_Number IS NOT NULL
            AND vd.person_id IS NOT NULL
            AND vd.Episode_Number IS NOT NULL
            AND (Start_Date_Consultant_Episode IS NOT NULL AND Start_Date_Consultant_Episode != '20')
            UNION DISTINCT
            SELECT DISTINCT
                CONCAT(CAST(vd.Hospital_Provider_Spell_Number as STRING),'_', vd.person_id, '_', vd.Episode_Number) source_value 
            FROM `CY_IMOSPHERE_WORKSPACE.src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2` vd
            WHERE vd.Hospital_Provider_Spell_Number IS NOT NULL
            AND vd.person_id IS NOT NULL
            AND vd.Episode_Number IS NOT NULL
            AND (Start_Date_Consultant_Episode IS NOT NULL AND Start_Date_Consultant_Episode != '20')
        ) vd
    ) vd
    WHERE NOT EXISTS (SELECT * FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups` l WHERE l.source_value = vd.source_value);
        
    /*
        --[SUS_BRI_APC_010415_to_300619_P1], [src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2] Transfer
    */

    INSERT INTO `CY_IMOSPHERE_CDM_531.visit_detail`
    (
        visit_detail_id,
        person_id,
        visit_detail_concept_id,
        visit_detail_start_date,
        visit_detail_start_datetime,
        visit_detail_end_date,
        visit_detail_end_datetime,
        visit_detail_type_concept_id,
        provider_id,
        visit_occurrence_id
    )
    SELECT
        vdl.id visit_detail_id,
        person_id,
        9201 visit_detail_concept_id, --Inpatient Visit
        MIN(PARSE_DATE("%Y%m%d", Start_Date_Consultant_Episode)) visit_detail_start_date,
        MIN(PARSE_DATETIME('%Y%m%d%H:%M:%S', CONCAT(Start_Date_Consultant_Episode, Start_Time_Episode))) visit_detail_start_datetime,
        MAX(PARSE_DATE("%Y%m%d", End_Date_Consultant_Episode)) visit_detail_end_date,
        MAX(PARSE_DATETIME('%Y%m%d%H:%M:%S', CONCAT(End_Date_Consultant_Episode, End_Time_Episode))) visit_detail_end_datetime,
        32817 visit_detail_type_concept_id, --EHR
        array_agg(struct(provider_id) ignore nulls order by Start_Date_Consultant_Episode asc limit 1)[offset(0)].provider_id provider_id,
        vl.id visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            Consultant_Code,
            Start_Date_Consultant_Episode,
            person_id,
            Start_Time_Episode,
            End_Date_Consultant_Episode,
            End_Time_Episode,
            CONCAT(CAST(vd.Hospital_Provider_Spell_Number as STRING),'_', person_id) visit_occurrence_source_value,
            CONCAT(CAST(vd.Hospital_Provider_Spell_Number as STRING),'_', vd.person_id, '_', vd.Episode_Number) source_value 
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1` vd
        UNION DISTINCT
        SELECT DISTINCT
            Consultant_Code,
            Start_Date_Consultant_Episode,
            person_id,
            Start_Time_Episode,
            End_Date_Consultant_Episode,
            End_Time_Episode,
            CONCAT(CAST(vd.Hospital_Provider_Spell_Number as STRING),'_', person_id) visit_occurrence_source_value,
            CONCAT(CAST(vd.Hospital_Provider_Spell_Number as STRING),'_', vd.person_id, '_', vd.Episode_Number) source_value 
        FROM `CY_IMOSPHERE_WORKSPACE.src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2` vd
    ) vd
    JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = vd.visit_occurrence_source_value
    JOIN `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups` vdl ON vdl.source_value = vd.source_value
    LEFT JOIN `CY_IMOSPHERE_CDM_531.provider` p ON p.provider_source_value = vd.Consultant_Code 
    WHERE (Start_Date_Consultant_Episode IS NOT NULL AND Start_Date_Consultant_Episode != '20')
    AND vd.person_id IS NOT NULL
    AND NOT EXISTS (SELECT * FROM `CY_IMOSPHERE_CDM_531.visit_detail` ovd WHERE ovd.visit_detail_id = vdl.id)
    GROUP BY vdl.id, person_id, vl.id;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id,  related_table, related_count, logged_at) 
    SELECT 'Added visit_detail', run_id, 'SUS_BRI_APC_010415_to_300619_P1, src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2 -> visit_detail', @@row_count, CURRENT_DATETIME();
END;