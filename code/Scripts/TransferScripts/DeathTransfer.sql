CREATE OR REPLACE PROCEDURE CY_IMOSPHERE_WORKSPACE.TRANSFER_DEATH(run_id INT64)
OPTIONS (strict_mode=false)
BEGIN    
    /*
        Pending: rules for choosing death date are currently under review, currently it is the first valid record, but this a placeholder only.
    */

    /*
        --[tbl_SRPatient] Transfer
    */

    INSERT INTO `CY_IMOSPHERE_CDM_531.death`
    (
        person_id,
        death_date,
        death_datetime,
        death_type_concept_id
    )
    SELECT DISTINCT      
        d.person_id person_id,
        PARSE_DATE("%Y%m%d", CONCAT(pd.DateDeath ,'01'))  death_date,
        PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(pd.datedeath ,'01000000')) death_datetime,
        32817 death_type_concept_id --EHR
        FROM 
        (
            SELECT
                person_id,
                ARRAY_AGG(STRUCT(DateDeath) ORDER BY person_id LIMIT 1)[OFFSET(0)] pd
            FROM `CY_IMOSPHERE_WORKSPACE.tbl_SRPatient` d
            WHERE DateDeath IS NOT NULL
            GROUP BY person_id
        ) d
    LEFT JOIN `CY_IMOSPHERE_CDM_531.death` od ON od.person_id = d.person_id
    WHERE d.person_id IS NOT NULL
    AND od.person_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added death', run_id, 'tbl_SRPatient -> death', @@row_count, CURRENT_DATETIME();
END;