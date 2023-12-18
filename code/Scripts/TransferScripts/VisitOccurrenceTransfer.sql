CREATE OR REPLACE PROCEDURE CY_IMOSPHERE_WORKSPACE.TRANSFER_VISIT_OCCURRENCE(run_id INT64)
OPTIONS (strict_mode=false)
BEGIN 
    /*
        Build mapping tables. These are required because there's no universal unique source id. This is done using the minimal set of data to get a unique ids.
    */

    DECLARE max_current_id INT64;
    CREATE TABLE IF NOT EXISTS CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups(id INT64, source_value STRING, source_table STRING);

    --tbl_SRVisit ids
    SET max_current_id = COALESCE((SELECT id FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` (id, source_value, source_table)
    SELECT
        max_current_id + ROW_NUMBER() OVER() id,
        source_value,
        source_table
    FROM
    (
        SELECT DISTINCT
            CONCAT(CAST(vo.RowIdentifier as STRING),'_', person_id) source_value,
            'tbl_SRVisit' source_table
        FROM `CY_IMOSPHERE_WORKSPACE.tbl_SRVisit` vo
        WHERE vo.person_id IS NOT NULL
        AND vo.RowIdentifier IS NOT NULL
        AND NOT EXISTS (SELECT * FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_value = CONCAT(CAST(vo.RowIdentifier as STRING),'_', person_id) AND l.source_table = 'tbl_SRVisit')
    ) i;

    --SUS_BRI_OP_010415_to_300619 ids
    SET max_current_id = COALESCE((SELECT id FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` (id, source_value, source_table)
    SELECT
        max_current_id + ROW_NUMBER() OVER() id,
        source_value,
        source_table
    FROM
    (
        SELECT DISTINCT
            CONCAT(CAST(vo.Generated_Record_Identifier as STRING),'_', person_id) source_value,
            'SUS_BRI_OP_010415_to_300619' source_table
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_OP_010415_to_300619` vo
        WHERE vo.person_id IS NOT NULL
        AND vo.Generated_Record_Identifier IS NOT NULL
        AND NOT EXISTS (SELECT * FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_value = CONCAT(CAST(vo.Generated_Record_Identifier as STRING),'_', person_id) AND l.source_table = 'SUS_BRI_OP_010415_to_300619')
    ) i;

    --SUS_BRI_APC_010415_to_300619_P1, src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2 ids
    SET max_current_id = COALESCE((SELECT id FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` (id, source_value, source_table)
    SELECT
        max_current_id + ROW_NUMBER() OVER() id,
        source_value,
        'spell_number' source_table
    FROM
    (
        SELECT DISTINCT
            CONCAT(CAST(vo.Hospital_Provider_Spell_Number as STRING),'_', person_id) source_value           
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1` vo
        WHERE vo.Start_Date_Hospital_Provider_Spell IS NOT NULL
        AND vo.person_id IS NOT NULL
        UNION DISTINCT
        SELECT DISTINCT
          CONCAT(CAST(vo.Hospital_Provider_Spell_Number as STRING),'_', person_id) source_value
        FROM `CY_IMOSPHERE_WORKSPACE.src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2` vo
        WHERE vo.Start_Date_Hospital_Provider_Spell IS NOT NULL
        AND vo.person_id IS NOT NULL
    ) i
    WHERE NOT EXISTS (SELECT * FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_value = i.source_value  AND l.source_table = 'spell_number');

    --SUS_BRI_AE_010415_to_300619 ids
    SET max_current_id = COALESCE((SELECT id FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` (id, source_value, source_table)
    SELECT
        max_current_id + ROW_NUMBER() OVER() id,
        i.source_value,
        'SUS_BRI_AE_010415_to_300619' source_table
    FROM
    (
        SELECT DISTINCT
            CONCAT(CAST(vo.AandE_Attendance_Number as STRING),'_', person_id) source_value           
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` vo
        WHERE vo.Arrival_Date IS NOT NULL
        AND vo.person_id IS NOT NULL
        AND vo.AandE_Attendance_Number IS NOT NULL
        AND vo.AandE_Arrival_Mode IS NOT NULL
    ) i
    WHERE NOT EXISTS (SELECT * FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_value = i.source_value AND l.source_table = 'SUS_BRI_AE_010415_to_300619');

    /*
        --[tbl_SRVisit] Transfer
    */

    INSERT INTO `CY_IMOSPHERE_CDM_531.visit_occurrence`
    (
        visit_occurrence_id,
        person_id,
        visit_concept_id,
        visit_start_date,
        visit_start_datetime,
        visit_end_date,
        visit_end_datetime,
        visit_type_concept_id,
        care_site_id
    )
    SELECT DISTINCT     
        vl.id visit_occurrence_id,
        vo.person_id person_id,
        9202 visit_concept_id, --Outpatient Visit
        CAST(DateBooked AS DATE)  visit_start_date,
        DateBooked visit_start_datetime,
        CAST(DateBooked AS DATE)  visit_end_date,
        DateBooked visit_end_datetime,
        32817 visit_type_concept_id, --EHR
        cs.care_site_id care_site_id
    FROM `CY_IMOSPHERE_WORKSPACE.tbl_SRVisit` vo
    JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'tbl_SRVisit') vl ON vl.source_value = CONCAT(CAST(vo.RowIdentifier as STRING),'_', person_id)
    LEFT JOIN `CY_IMOSPHERE_CDM_531.care_site` cs ON cs.care_site_source_value = vo.idorganisation
    LEFT JOIN `CY_IMOSPHERE_CDM_531.visit_occurrence` ovo ON ovo.visit_occurrence_id = vl.id
    WHERE vo.DateBooked IS NOT NULL
    AND vo.person_id IS NOT NULL
    AND vo.RowIdentifier IS NOT NULL
    AND ovo.visit_occurrence_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id,  related_table, related_count, logged_at) 
    SELECT 'Added visit_occurrence', run_id, 'tbl_SRVisit -> visit_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_OP_010415_to_300619] Transfer
    */

    INSERT INTO `CY_IMOSPHERE_CDM_531.visit_occurrence`
    (
        visit_occurrence_id,
        person_id,
        visit_concept_id,
        visit_start_date,
        visit_start_datetime,
        visit_end_date,
        visit_end_datetime,
        provider_id,
        visit_type_concept_id
    )
    SELECT DISTINCT
        vl.id visit_occurrence_id,
        vo.person_id,
        9202 visit_concept_id, --Outpatient Visit
        PARSE_DATE("%Y%m%d", Appointment_Date) visit_start_date,
        CASE WHEN Appointment_Time LIKE '% %' OR Appointment_Time IS NULL
            THEN PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(Appointment_Date, '000000'))
            ELSE PARSE_DATETIME('%Y%m%d%H:%M%S', CONCAT(Appointment_Date, Appointment_Time, '00'))
        END visit_start_datetime,
        PARSE_DATE("%Y%m%d", Appointment_Date) visit_end_date,
        CASE WHEN Appointment_Time LIKE '% %' OR Appointment_Time IS NULL
            THEN PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(Appointment_Date, '000000'))
            ELSE PARSE_DATETIME('%Y%m%d%H:%M%S', CONCAT(Appointment_Date, Appointment_Time, '00'))
        END visit_end_datetime,
        p.provider_id provider_id,
        32817 visit_type_concept_id, --EHR
    FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_OP_010415_to_300619` vo
    JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_OP_010415_to_300619') vl ON vl.source_value = CONCAT(CAST(vo.Generated_Record_Identifier as STRING),'_', person_id)
    LEFT JOIN `CY_IMOSPHERE_CDM_531.provider` p ON p.provider_source_value = Consultant_Code 
    LEFT JOIN `CY_IMOSPHERE_CDM_531.visit_occurrence` ovo ON ovo.visit_occurrence_id = vl.id
    WHERE vo.Appointment_Date IS NOT NULL
    AND vo.person_id IS NOT NULL
    AND vo.Generated_Record_Identifier IS NOT NULL
    AND ovo.visit_occurrence_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id,  related_table, related_count, logged_at) 
    SELECT 'Added visit_occurrence', run_id, 'SUS_BRI_OP_010415_to_300619 -> visit_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_APC_010415_to_300619_P1] Transfer, [src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2] Transfer
    */

    INSERT INTO `CY_IMOSPHERE_CDM_531.visit_occurrence`
    (
        visit_occurrence_id,
        person_id,
        visit_concept_id,
        visit_start_date,
        visit_start_datetime,
        visit_end_date,
        visit_end_datetime,
        visit_type_concept_id,
        provider_id,
        admitting_source_concept_id,
        admitting_source_value,
        discharge_to_concept_id,
        discharge_to_source_value
    )          
    SELECT 
        vl.id visit_occurrence_id,
        person_id,
        9201 visit_concept_id,--Inpatient Visit
        MIN(PARSE_DATE("%Y%m%d", Start_Date_Hospital_Provider_Spell)) visit_start_date,
        MIN (CASE WHEN Start_Time_Hospital_Provider_Spell LIKE '% %' OR Start_Time_Hospital_Provider_Spell IS NULL
                THEN PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(REPLACE(Start_Date_Hospital_Provider_Spell, ' ', ''),'000000'))
                WHEN Start_Time_Hospital_Provider_Spell LIKE '%:'
                THEN PARSE_DATETIME('%Y%m%d%H:%M:%S', CONCAT(REPLACE(Start_Date_Hospital_Provider_Spell, ' ', ''), Start_Time_Hospital_Provider_Spell, '00'))
                ELSE PARSE_DATETIME('%Y%m%d%H:%M:%S', CONCAT(Start_Date_Hospital_Provider_Spell, Start_Time_Hospital_Provider_Spell))
            END) visit_start_datetime,
        MAX(PARSE_DATE("%Y%m%d", Discharge_Date_From_Hospital_Provider_Spell)) visit_end_date,
        MAX(CASE WHEN Discharge_Time_Hospital_Provider_Spell LIKE '% %' OR Discharge_Time_Hospital_Provider_Spell IS NULL
              THEN PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(REPLACE(Discharge_Date_From_Hospital_Provider_Spell, ' ', ''),'000000'))
              WHEN Discharge_Time_Hospital_Provider_Spell LIKE '%:'
              THEN PARSE_DATETIME('%Y%m%d%H:%M:%S', CONCAT(REPLACE(Discharge_Date_From_Hospital_Provider_Spell, ' ', ''), Discharge_Time_Hospital_Provider_Spell, '00'))
              ELSE PARSE_DATETIME('%Y%m%d%H:%M:%S', CONCAT(Discharge_Date_From_Hospital_Provider_Spell, Discharge_Time_Hospital_Provider_Spell))
          END) visit_end_datetime,
          32817 visit_type_concept_id, --EHR
          array_agg(struct(provider_id) ignore nulls order by Start_Date_Hospital_Provider_Spell asc limit 1)[offset(0)].provider_id provider_id,
          array_agg(struct(am.concept_id) ignore nulls order by Start_Date_Hospital_Provider_Spell asc limit 1)[offset(0)].concept_id admitting_source_concept_id,
          array_agg(struct(am.source_value) ignore nulls order by Start_Date_Hospital_Provider_Spell asc limit 1)[offset(0)].source_value admitting_source_value,
          array_agg(struct(dm.concept_id) ignore nulls order by Discharge_Date_From_Hospital_Provider_Spell desc limit 1)[offset(0)].concept_id discharge_to_concept_id,
          array_agg(struct(dm.source_value) ignore nulls order by Discharge_Date_From_Hospital_Provider_Spell desc limit 1)[offset(0)].source_value discharge_source_value
    FROM
    (
      SELECT
      person_id, 
        CONCAT(CAST(vo.Hospital_Provider_Spell_Number as STRING),'_', person_id) source_value,
        Start_Date_Hospital_Provider_Spell,
        Start_Time_Hospital_Provider_Spell,
        CASE WHEN Discharge_Date_From_Hospital_Provider_Spell IS NULL OR Discharge_Date_From_Hospital_Provider_Spell = 'NULL' OR Discharge_Date_From_Hospital_Provider_Spell LIKE '%  %' THEN Start_Date_Hospital_Provider_Spell ELSE Discharge_Date_From_Hospital_Provider_Spell END Discharge_Date_From_Hospital_Provider_Spell,
        CASE WHEN Discharge_Date_From_Hospital_Provider_Spell IS NULL OR Discharge_Date_From_Hospital_Provider_Spell = 'NULL' OR Discharge_Date_From_Hospital_Provider_Spell LIKE '%  %' THEN Start_Time_Hospital_Provider_Spell ELSE Discharge_Time_Hospital_Provider_Spell END Discharge_Time_Hospital_Provider_Spell,
        Consultant_Code,
        Source_of_Admission_Hospital_Provider_Spell,
        Discharge_Destination_Hospital_Provider_Spell
      FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1` vo
      UNION DISTINCT
      SELECT
        person_id,
        CONCAT(CAST(vo.Hospital_Provider_Spell_Number as STRING),'_', person_id) source_value,
        Start_Date_Hospital_Provider_Spell,
        Start_Time_Hospital_Provider_Spell,
        CASE WHEN Discharge_Date_From_Hospital_Provider_Spell IS NULL OR Discharge_Date_From_Hospital_Provider_Spell = 'NULL' OR Discharge_Date_From_Hospital_Provider_Spell LIKE '%  %' THEN Start_Date_Hospital_Provider_Spell ELSE Discharge_Date_From_Hospital_Provider_Spell END Discharge_Date_From_Hospital_Provider_Spell,
        CASE WHEN Discharge_Date_From_Hospital_Provider_Spell IS NULL OR Discharge_Date_From_Hospital_Provider_Spell = 'NULL' OR Discharge_Date_From_Hospital_Provider_Spell LIKE '%  %' THEN Start_Time_Hospital_Provider_Spell ELSE Discharge_Time_Hospital_Provider_Spell END Discharge_Time_Hospital_Provider_Spell,
        Consultant_Code,
        Source_of_Admission_Hospital_Provider_Spell,
        Discharge_Destination_Hospital_Provider_Spell
      FROM `CY_IMOSPHERE_WORKSPACE.src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2` vo
    ) vo
    JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = vo.source_value
    LEFT JOIN `CY_IMOSPHERE_CDM_531.provider` p ON p.provider_source_value = Consultant_Code
    LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` am on am.source_value = vo.Source_of_Admission_Hospital_Provider_Spell AND am.destination_table = 'VISIT_OCCURRENCE' AND am.source_table = 'src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2' AND am.source_column = 'Source_of_Admission_Hospital_Provider_Spell'
    LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` dm on dm.source_value = vo.Discharge_Destination_Hospital_Provider_Spell AND dm.destination_table = 'VISIT_OCCURRENCE' AND dm.source_table = 'src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2' AND dm.source_column = 'Discharge_Destination_Hospital_Provider_Spell'
    WHERE vo.Start_Date_Hospital_Provider_Spell IS NOT NULL
    AND person_id IS NOT NULL
    AND NOT EXISTS (SELECT * FROM `CY_IMOSPHERE_CDM_531.visit_occurrence` ovo WHERE ovo.visit_occurrence_id = vl.id)
    GROUP BY vl.id, person_id;
   
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id,  related_table, related_count, logged_at) 
    SELECT 'Added visit_occurrence', run_id, 'SUS_BRI_APC_010415_to_300619_P1, src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2  -> visit_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619] Transfer
    */

    INSERT INTO `CY_IMOSPHERE_CDM_531.visit_occurrence` 
    (
        visit_occurrence_id,
        person_id,
        visit_concept_id,
        visit_start_date,
        visit_start_datetime,
        visit_end_date,
        visit_end_datetime,
        visit_type_concept_id
    )
    SELECT      
        vl.id visit_occurrence_id,
        vo.person_id person_id,
        CASE WHEN AandE_Arrival_Mode = '1' 
            THEN 38004353  --Ambulance
            WHEN AandE_Arrival_Mode = '2'
            THEN 9203 --Emergency Room Visit
        END visit_concept_id,
        MIN(PARSE_DATE("%Y%m%d", Arrival_Date)) visit_start_date,
        MIN(PARSE_DATETIME('%Y%m%d%H:%M:%S', CONCAT(Arrival_Date, Arrival_Time, ':00'))) visit_start_datetime,
        MAX(CASE WHEN PARSE_DATE("%Y%m%d", AandE_Departure_Date) IS NOT NULL THEN PARSE_DATE("%Y%m%d", AandE_Departure_Date) 
            WHEN PARSE_DATE("%Y%m%d", AandE_Departure_Date) IS NULL AND cast(timestamp_diff(PARSE_DATETIME('%Y%m%d%H:%M:%S', CONCAT(Arrival_Date, Arrival_Time, ':00')) , PARSE_DATETIME('%Y%m%d%H:%M:%S', CONCAT(Arrival_Date, AandE_Departure_Time)), minute) as INT64) > 0
            THEN DATE_ADD(DATE (PARSE_DATE("%Y%m%d", Arrival_Date)), INTERVAL 1 DAY)
                WHEN AandE_Departure_Date IS NULL AND CAST(timestamp_diff(PARSE_DATETIME('%Y%m%d%H:%M:%S', CONCAT(Arrival_Date, Arrival_Time, ':00')) , PARSE_DATETIME('%Y%m%d%H:%M:%S', CONCAT(Arrival_Date, AandE_Departure_Time)), minute) AS INT64) < 0
                THEN PARSE_DATE("%Y%m%d", Arrival_Date)
        ELSE PARSE_DATE("%Y%m%d", Arrival_Date)
        END) visit_end_date,
        MAX(CASE WHEN PARSE_DATE("%Y%m%d", AandE_Departure_Date) IS NOT NULL THEN PARSE_DATETIME('%Y%m%d%H:%M:%S', CONCAT(AandE_Departure_Date, AandE_Departure_Time))
            WHEN PARSE_DATE("%Y%m%d", AandE_Departure_Date) IS NULL AND cast(timestamp_diff(PARSE_DATETIME('%Y%m%d%H:%M:%S', CONCAT(Arrival_Date, Arrival_Time, ':00')) , PARSE_DATETIME('%Y%m%d%H:%M:%S', CONCAT(Arrival_Date, AandE_Departure_Time)), minute) as INT64) > 0
            THEN PARSE_DATETIME('%Y-%m-%d%H:%M:%S', CONCAT(DATE_ADD(DATE (PARSE_DATE("%Y%m%d", Arrival_Date)), INTERVAL 1 DAY),  AandE_Departure_Time))
                WHEN AandE_Departure_Date IS NULL AND CAST(timestamp_diff(PARSE_DATETIME('%Y%m%d%H:%M:%S', CONCAT(Arrival_Date, Arrival_Time, ':00')) , PARSE_DATETIME('%Y%m%d%H:%M:%S', CONCAT(Arrival_Date, AandE_Departure_Time)), minute) AS INT64) < 0
                THEN PARSE_DATETIME('%Y%m%d%H:%M:%S', CONCAT(Arrival_Date, AandE_Departure_Time)) 
        ELSE PARSE_DATETIME('%Y%m%d%H:%M:%S', CONCAT(Arrival_Date, AandE_Departure_Time)) 
        END) visit_end_datetime,
        32817 visit_type_concept_id --EHR
    FROM 
    (
        SELECT
            *,
            CONCAT(CAST(vo.AandE_Attendance_Number as STRING),'_', person_id) source_value  
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` vo
    ) vo
    JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl ON vl.source_value = vo.source_value
    WHERE vo.Arrival_Date IS NOT NULL
    AND vo.person_id IS NOT NULL
    AND vo.AandE_Attendance_Number IS NOT NULL
    AND vo.AandE_Arrival_Mode IS NOT NULL
    AND NOT EXISTS (SELECT * FROM `CY_IMOSPHERE_CDM_531.visit_occurrence` ovo WHERE ovo.visit_occurrence_id = vl.id)
    GROUP BY vl.id, person_id, AandE_Arrival_Mode;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id,  related_table, related_count, logged_at) 
    SELECT 'Added visit_occurrence', run_id, 'SUS_BRI_AE_010415_to_300619 -> visit_occurrence', @@row_count, CURRENT_DATETIME();
END;