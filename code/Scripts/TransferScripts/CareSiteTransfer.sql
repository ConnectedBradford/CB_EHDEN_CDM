CREATE OR REPLACE PROCEDURE CY_IMOSPHERE_WORKSPACE.TRANSFER_CARE_SITE(run_id INT64)
OPTIONS (strict_mode=false)
BEGIN
    DECLARE max_current_id INT64;

    /*
        --[tbl_srorganisation] Transfer
    */
    SET max_current_id = COALESCE((SELECT care_site_id FROM `CY_IMOSPHERE_CDM_531.care_site` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.care_site`
    (
        care_site_id,
        care_site_name,
        place_of_service_concept_id,
        location_id,
        care_site_source_value
    )
    SELECT       
        ROW_NUMBER() OVER () care_site_id,
        Name care_site_name,
        9202 place_of_service_concept_id, --Outpatient visit  
        l.location_id location_id,
        ID care_site_source_value
    FROM
    (
        SELECT DISTINCT
            cs.Id,
            o.Name,
            o.location_Id
        FROM `CY_IMOSPHERE_WORKSPACE.tbl_srorganisation` cs
        JOIN (SELECT Id, MAX(RowIdentifier) RowIdentifier, MIN(Name) Name, MIN(CONCAT('SRORG', TO_HEX(SHA256(CONCAT(LTRIM(CONCAT(REPLACE(HouseName, 'NULL', ''),  ' ' , REPLACE(HouseNumber, 'NULL', ''),  ' ' , REPLACE(NameOfRoad, 'NULL', ''))), NameOfLocality, NameOfTown, FullPostcode, NameOfCounty))))) Location_Id FROM `CY_IMOSPHERE_WORKSPACE.tbl_srorganisation` GROUP BY Id) o ON o.RowIdentifier = cs.RowIdentifier AND o.Id = cs.Id
    ) cs
    LEFT JOIN `CY_IMOSPHERE_CDM_531.location` l ON l.location_source_value = cs.location_id
    WHERE Id IS NOT NULL AND Id != 'NULL'
    AND NOT EXISTS (SELECT * FROM `CY_IMOSPHERE_CDM_531.care_site` ocs WHERE ocs.care_site_source_value = cs.Id);

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id,  related_table, related_count, logged_at) 
    SELECT 'Added care_site', run_id, 'tbl_srorganisation -> care_site', @@row_count, CURRENT_DATETIME();
END;