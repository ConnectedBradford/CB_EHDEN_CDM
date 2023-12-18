CREATE OR REPLACE PROCEDURE CY_IMOSPHERE_WORKSPACE.TRANSFER_LOCATION(run_id INT64)
OPTIONS (strict_mode=false)
BEGIN
    /*
        --[tbl_srorganisation] Transfer
    */

    DECLARE max_current_id INT64;

    SET max_current_id = COALESCE((SELECT location_id FROM `CY_IMOSPHERE_CDM_531.location` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.location`
    (
        location_id,
        address_1,
        address_2,
        city,
        zip,
        county,
        location_source_value
    )
    SELECT       
        max_current_id + ROW_NUMBER() OVER() as location_id,
        address_1,
        address_2,
        city,
        zip,
        county,
        location_source_value
    FROM 
    (
        SELECT DISTINCT
          address_1,
          address_2,
          city,
          zip,
          county,
          CONCAT('SRORG', TO_HEX(SHA256(CONCAT(address_1, address_2, city, zip, county)))) location_source_value
        FROM
        (
          SELECT
            LTRIM(CONCAT(REPLACE(HouseName, 'NULL', ''),  ' ' , REPLACE(HouseNumber, 'NULL', ''),  ' ' , REPLACE(NameOfRoad, 'NULL', ''))) address_1,
            NameOfLocality address_2,
            NameOfTown city,
            FullPostcode zip,
            NameOfCounty county
          FROM `CY_IMOSPHERE_WORKSPACE.tbl_srorganisation` l
          WHERE RowIdentifier IS NOT NULL
          AND (NameOfRoad IS NOT NULL AND NameOfRoad != 'NULL') 
          AND FullPostcode IS NOT NULL
        ) l
        WHERE NOT EXISTS (SELECT * FROM `CY_IMOSPHERE_CDM_531.location` ol WHERE ol.location_source_value = CONCAT('SRORG', TO_HEX(SHA256(CONCAT(address_1, address_2, city, zip, county)))))       
    ) l;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id,  related_table, related_count, logged_at) 
    SELECT 'Added location', run_id, 'tbl_srorganisation -> location', @@row_count, CURRENT_DATETIME();
    
    /*
        --[tbl_SRPatientAddressHistory] Transfer
    */
    
    SET max_current_id = COALESCE((SELECT location_id FROM `CY_IMOSPHERE_CDM_531.location` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.location`
    (
        location_id,
        zip,
        location_source_value
    )
    SELECT   
        max_current_id + ROW_NUMBER() OVER() as location_id,
        zip,
        location_source_value
    FROM 
    (
        SELECT DISTINCT         
            partialpostcode zip,
            l.partialpostcode location_source_value
        FROM `CY_IMOSPHERE_WORKSPACE.tbl_SRPatientAddressHistory` l
        WHERE (partialpostcode IS NOT NULL AND partialpostcode != 'NULL')
        AND NOT EXISTS (SELECT * FROM `CY_IMOSPHERE_CDM_531.location` ol WHERE ol.location_source_value = l.partialpostcode)
    ) l;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id,  related_table, related_count, logged_at)
    SELECT 'Added location', run_id, 'tbl_SRPatientAddressHistory -> location', @@row_count, CURRENT_DATETIME();
    
    /*
        --[src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2] Transfer
    */

    SET max_current_id = COALESCE((SELECT location_id FROM `CY_IMOSPHERE_CDM_531.location` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.location`
    (
        location_id,
        zip,
        location_source_value
    )
    SELECT 
        max_current_id + ROW_NUMBER() OVER() as location_id,
        zip,
        location_source_value
    FROM 
   (
        SELECT DISTINCT            
            Postcode_area_only zip,
            Postcode_area_only location_source_value
        FROM `CY_IMOSPHERE_WORKSPACE.src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2` l
        WHERE (Postcode_area_only IS NOT NULL AND Postcode_area_only != 'NULL')
        AND NOT EXISTS (SELECT * FROM `CY_IMOSPHERE_CDM_531.location` ol WHERE ol.location_source_value = l.Postcode_area_only)
    ) l;

     INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id,  related_table, related_count, logged_at)
     SELECT 'Added location', run_id, 'src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2 -> location', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619] Transfer
    */

    SET max_current_id = COALESCE((SELECT location_id FROM `CY_IMOSPHERE_CDM_531.location` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.location`
    (
        location_id,
        zip,
        location_source_value
    )
    SELECT     
        max_current_id + ROW_NUMBER() OVER() as location_id,
        zip,
        location_source_value
    FROM 
   (
        SELECT DISTINCT           
            Postcode zip,
            Postcode location_source_value
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` l
        LEFT JOIN `CY_IMOSPHERE_CDM_531.location` ol ON ol.location_source_value = l.Postcode
        WHERE (Postcode IS NOT NULL AND Postcode != 'NULL')
        AND NOT EXISTS (SELECT * FROM `CY_IMOSPHERE_CDM_531.location` ol WHERE ol.location_source_value = l.Postcode)
   ) l;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id,  related_table, related_count, logged_at)
    SELECT 'Added location', run_id, 'SUS_BRI_AE_010415_to_300619 -> location', @@row_count, CURRENT_DATETIME();
    
    /*
        --[SUS_BRI_OP_010415_to_300619] Transfer
    */

    SET max_current_id = COALESCE((SELECT location_id FROM `CY_IMOSPHERE_CDM_531.location` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.location`
    (
        location_id,
        zip,
        location_source_value
    )
    SELECT    
        max_current_id + ROW_NUMBER() OVER() as location_id,
        zip,
        location_source_value
    FROM 
    (
        SELECT DISTINCT           
            Postcode zip,
            Postcode location_source_value
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_OP_010415_to_300619` l
        LEFT JOIN `CY_IMOSPHERE_CDM_531.location` ol ON ol.location_source_value = l.Postcode
        WHERE(Postcode IS NOT NULL AND Postcode != 'NULL')
        AND NOT EXISTS (SELECT * FROM `CY_IMOSPHERE_CDM_531.location` ol WHERE ol.location_source_value = l.Postcode)
    ) l;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id,  related_table, related_count, logged_at)
    SELECT 'Added location', run_id, 'SUS_BRI_OP_010415_to_300619 -> location', @@row_count, CURRENT_DATETIME();

    /*
    --[SUS_BRI_APC_010415_to_300619_P1] Transfer
    */

    SET max_current_id = COALESCE((SELECT location_id FROM `CY_IMOSPHERE_CDM_531.location` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.location`
    (
        location_id,
        zip,
        location_source_value
    )
    SELECT        
        max_current_id + ROW_NUMBER() OVER() as location_id, 
        zip,
        location_source_value
    FROM 
    (
        SELECT DISTINCT          
            Postcode zip,
            Postcode location_source_value
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1` l
        LEFT JOIN `CY_IMOSPHERE_CDM_531.location` ol ON ol.location_source_value = l.Postcode
        WHERE(Postcode IS NOT NULL AND Postcode != 'NULL')
        AND NOT EXISTS (SELECT * FROM `CY_IMOSPHERE_CDM_531.location` ol WHERE ol.location_source_value = l.Postcode)
    ) l;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id,  related_table, related_count, logged_at)
    SELECT 'Added location', run_id, 'SUS_BRI_APC_010415_to_300619_P1 -> location', @@row_count, CURRENT_DATETIME();
END;