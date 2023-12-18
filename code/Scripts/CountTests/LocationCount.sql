CREATE OR REPLACE PROCEDURE CY_IMOSPHERE_WORKSPACE.COUNT_LOCATION()
OPTIONS (strict_mode=false)
BEGIN
   /*
        --location Count  
   */

    DECLARE location_source_count INT64;
    DECLARE omop_location_count INT64;
    
    SET location_source_count =
    (
        SELECT COUNT(*) FROM
        (
            SELECT DISTINCT
                CONCAT('SRORG', TO_HEX(SHA256(CONCAT(LTRIM(CONCAT(REPLACE(HouseName, 'NULL', ''),  ' ' , REPLACE(HouseNumber, 'NULL', ''),  ' ' , REPLACE(NameOfRoad, 'NULL', ''))), NameOfLocality, NameOfTown, FullPostcode, NameOfCounty)))) location_source_value
            FROM `CY_IMOSPHERE_WORKSPACE.tbl_srorganisation` 
            WHERE FullPostcode IS NOT NULL AND (NameOfRoad IS NOT NULL AND NameOfRoad != 'NULL') AND RowIdentifier IS NOT NULL
            UNION DISTINCT
            SELECT DISTINCT 
              partialpostcode 
            FROM `CY_IMOSPHERE_WORKSPACE.tbl_SRPatientAddressHistory` WHERE partialpostcode IS NOT NULL AND partialpostcode != 'NULL'
            UNION DISTINCT
            SELECT DISTINCT 
              Postcode_area_only 
            FROM `CY_IMOSPHERE_WORKSPACE.src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2` WHERE Postcode_area_only IS NOT NULL AND Postcode_area_only != 'NULL'    
            UNION DISTINCT
            SELECT DISTINCT 
              Postcode FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` 
            WHERE Postcode IS NOT NULL AND Postcode != 'NULL'
            UNION DISTINCT
            SELECT DISTINCT 
              Postcode FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_OP_010415_to_300619` 
            WHERE Postcode IS NOT NULL AND Postcode != 'NULL'
            UNION DISTINCT
            SELECT DISTINCT 
              Postcode FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1` 
            WHERE Postcode IS NOT NULL AND Postcode != 'NULL'
        ) dc
    ) ;

    SET omop_location_count = (SELECT COUNT(*) FROM `CY_IMOSPHERE_CDM_531.location`);

    SELECT 
      'Location' Type,
      location_source_count SourceCount,
      omop_location_count CDMCount
    FROM
    (
      SELECT location_source_count, omop_location_count
    )
    WHERE location_source_count != omop_location_count;
END;