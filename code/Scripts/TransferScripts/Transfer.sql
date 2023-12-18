CREATE OR REPLACE PROCEDURE CY_IMOSPHERE_WORKSPACE.ETL_TRANSFER()
OPTIONS (strict_mode=false)
BEGIN
    DECLARE current_run INT64;
    SET current_run = COALESCE((SELECT run_id FROM `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` ORDER BY 1 DESC LIMIT 1), 0) + 1;
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (run_id, log_message) VALUES (current_run,'ETL started');

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (run_id, log_message) VALUES (current_run,'Start populate spell number lookups');
    CALL CY_IMOSPHERE_WORKSPACE.POPULATE_SPELLNUMBER_LOOKUPS();
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (run_id, log_message) VALUES (current_run,'End populate spell number lookups');

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (run_id, log_message) VALUES (current_run,'Start transfer locations');
    CALL CY_IMOSPHERE_WORKSPACE.TRANSFER_LOCATION(current_run);
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (run_id, log_message) VALUES (current_run,'End transfer locations');

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (run_id, log_message) VALUES (current_run,'Start transfer care sites');
    CALL CY_IMOSPHERE_WORKSPACE.TRANSFER_CARE_SITE(current_run);
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (run_id, log_message) VALUES (current_run,'End transfer care sites');

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (run_id, log_message) VALUES (current_run,'Start transfer people');
    CALL CY_IMOSPHERE_WORKSPACE.TRANSFER_PERSON(current_run);
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (run_id, log_message) VALUES (current_run,'End transfer people');

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (run_id, log_message) VALUES (current_run,'Start transfer death');
    CALL CY_IMOSPHERE_WORKSPACE.TRANSFER_DEATH(current_run);
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (run_id, log_message) VALUES (current_run,'End transfer death');

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (run_id, log_message) VALUES (current_run,'Start transfer provider');
    CALL CY_IMOSPHERE_WORKSPACE.TRANSFER_PROVIDER(current_run);
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (run_id, log_message) VALUES (current_run,'End transfer provider');

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (run_id, log_message) VALUES (current_run,'Start transfer visit occurrence');
    CALL CY_IMOSPHERE_WORKSPACE.TRANSFER_VISIT_OCCURRENCE(current_run);
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (run_id, log_message) VALUES (current_run,'End transfer visit occurrence');

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (run_id, log_message) VALUES (current_run,'Start transfer observation period');
    CALL CY_IMOSPHERE_WORKSPACE.TRANSFER_OBSERVATION_PERIOD(current_run);
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (run_id, log_message) VALUES (current_run,'End transfer observation period');

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (run_id, log_message) VALUES (current_run,'Start transfer visit detail');
    CALL CY_IMOSPHERE_WORKSPACE.TRANSFER_VISIT_DETAIL(current_run);
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (run_id, log_message) VALUES (current_run,'End transfer visit detail');

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (run_id, log_message) VALUES (current_run,'Start transfer condition occurrence');
    CALL CY_IMOSPHERE_WORKSPACE.TRANSFER_CONDITION_OCCURRENCE(current_run);
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (run_id, log_message) VALUES (current_run,'End transfer condition occurrence');

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (run_id, log_message) VALUES (current_run,'Start transfer procedure occurrence');
    CALL CY_IMOSPHERE_WORKSPACE.TRANSFER_PROCEDURE_OCCURRENCE(current_run);
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (run_id, log_message) VALUES (current_run,'End transfer procedure occurrence');

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (run_id, log_message) VALUES (current_run,'Start transfer observation');
    CALL CY_IMOSPHERE_WORKSPACE.TRANSFER_OBSERVATION(current_run);
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (run_id, log_message) VALUES (current_run,'End transfer observation');

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (run_id, log_message) VALUES (current_run,'Start transfer measurement');
    CALL CY_IMOSPHERE_WORKSPACE.TRANSFER_MEASUREMENT(current_run);
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (run_id, log_message) VALUES (current_run,'End transfer measurement');

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (run_id, log_message) VALUES (current_run,'Start transfer drug exposure');
    CALL CY_IMOSPHERE_WORKSPACE.TRANSFER_DRUG_EXPOSURE(current_run);
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (run_id, log_message) VALUES (current_run,'End transfer drug exposure');

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (run_id, log_message) VALUES (current_run,'Start transfer CDM source');
    CALL CY_IMOSPHERE_WORKSPACE.TRANSFER_CDM_SOURCE(current_run);
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (run_id, log_message) VALUES (current_run,'End transfer CDM source');

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (run_id, log_message) VALUES (current_run,'ETL finsihed');
END;