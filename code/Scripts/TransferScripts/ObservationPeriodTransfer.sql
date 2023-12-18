CREATE OR REPLACE PROCEDURE CY_IMOSPHERE_WORKSPACE.TRANSFER_OBSERVATION_PERIOD(run_id INT64)
OPTIONS (strict_mode=false)
BEGIN
    DECLARE max_current_id INT64;

    SET max_current_id = COALESCE((SELECT observation_period_id FROM `CY_IMOSPHERE_CDM_531.observation_period` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.observation_period`
    (
        observation_period_id,
        person_id,
        observation_period_start_date,
        observation_period_end_date,
        period_type_concept_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as observation_period_id,
        op.person_id,
        op.observation_period_start_date,
        op.observation_period_end_date,
        op.period_type_concept_id
    FROM
    (
        SELECT DISTINCT
            person_id,
            MIN(visit_start_date) observation_period_start_date,
            MAX(visit_end_date) observation_period_end_date,
            32817 period_type_concept_id --EHR
        FROM `CY_IMOSPHERE_CDM_531.visit_occurrence` op
        GROUP BY person_id
    ) op
    LEFT JOIN `CY_IMOSPHERE_CDM_531.observation_period` oop ON oop.person_id = op.person_id
    WHERE oop.observation_period_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added observation period', run_id, 'visit_occurrence -> observation_period', @@row_count, CURRENT_DATETIME();

    SET max_current_id = COALESCE((SELECT observation_period_id FROM `CY_IMOSPHERE_CDM_531.observation_period` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.observation_period`
    (
        observation_period_id,
        person_id,
        observation_period_start_date,
        observation_period_end_date,
        period_type_concept_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as observation_period_id,
        op.person_id,
        op.observation_period_start_date,
        op.observation_period_end_date,
        op.period_type_concept_id
    FROM
    (
        SELECT DISTINCT
            person_id,
            MIN(EXTRACT(DATE FROM DateEvent)) observation_period_start_date,
            MAX(EXTRACT(DATE FROM DateEvent)) observation_period_end_date,
            32817 period_type_concept_id --EHR
        FROM `CY_IMOSPHERE_WORKSPACE.tbl_SRCode` op
        GROUP BY person_id
    ) op
    LEFT JOIN `CY_IMOSPHERE_CDM_531.observation_period` oop ON oop.person_id = op.person_id
    WHERE oop.observation_period_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added observation period', run_id, 'tbl_SRCode -> observation_period', @@row_count, CURRENT_DATETIME();
END;