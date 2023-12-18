CREATE OR REPLACE PROCEDURE CY_IMOSPHERE_WORKSPACE.ETL_SETUP()
OPTIONS (strict_mode=false)
BEGIN
    /*
        CREATE ETL LOG TABLE
    */

    CREATE TABLE IF NOT EXISTS `yhcr-prd-phm-bia-core.CY_IMOSPHERE_WORKSPACE.tbl_etl_log`
    (
        run_id INT NOT NULL,  
        log_message STRING,
        related_table STRING,
        related_column STRING,
        related_value STRING,
        related_count INT,
        logged_at DATETIME DEFAULT CURRENT_DATETIME(),   ---SET AS CURRENT_DATETIME() IN TRANSFER SCRIPTS
        are_unmapped_records BOOL
    );

    /*
        CREATE ETL MAPPING TABLE
    */

    CREATE TABLE IF NOT EXISTS `yhcr-prd-phm-bia-core.CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings`
    (
        mapping_id INT64 ,
        source_table STRING,
        source_column STRING,
        source_value STRING,
        source_value_description STRING,
        destination_table STRING,
        destination_column STRING,
        concept_id INT64,
        concept_description STRING,
        map_on_presence BOOL ,
        override_source_data STRING,
        mapping_type STRING,
        mapping_criteria_value STRING,
        mapping_notes STRING,
        mapping_logic STRING
    );
END;