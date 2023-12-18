CREATE OR REPLACE PROCEDURE CY_IMOSPHERE_WORKSPACE.COUNT_PROVIDER()
OPTIONS (strict_mode=false)
BEGIN
    /*
        --provider Count 
    */

    DECLARE provider_count INT64;
    DECLARE omop_provider_count INT64;    
    
    SET provider_count = (SELECT COUNT(*) FROM 
    (
        SELECT DISTINCT iddoneby FROM `CY_IMOSPHERE_WORKSPACE.tbl_SRCode` WHERE iddoneby IS NOT NULL AND iddoneby NOT IN ('-1')
        UNION DISTINCT
        SELECT DISTINCT Consultant_Code FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1` WHERE Consultant_Code IS NOT NULL
        AND (CAST(
            CASE WHEN Consultant_Code LIKE 'C%' AND Consultant_Code NOT LIKE 'CD%' THEN REPLACE(Consultant_Code, 'C', '101')
                 WHEN Consultant_Code LIKE 'CD%' THEN REPLACE(Consultant_Code, 'CD', '202')
                 WHEN Consultant_Code LIKE 'N%' THEN REPLACE(Consultant_Code, 'N', '303')
                 WHEN Consultant_Code LIKE 'H%' THEN REPLACE(Consultant_Code, 'H', '404')
                 WHEN Consultant_Code LIKE 'G%' THEN REPLACE(Consultant_Code, 'G', '505')                                                                                 
                 WHEN Consultant_Code LIKE 'D%' THEN REPLACE(Consultant_Code, 'D', '606')
                 WHEN Consultant_Code LIKE 'M%' THEN REPLACE(Consultant_Code, 'M', '707')     
                 WHEN Consultant_Code LIKE 'S%' THEN REPLACE(Consultant_Code, 'S', '808')   
            END AS INT64)) IS NOT NULL
        UNION DISTINCT
        SELECT DISTINCT Consultant_Code FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_OP_010415_to_300619` WHERE Consultant_Code IS NOT NULL
        AND (CAST(
            CASE WHEN Consultant_Code LIKE 'C%' AND Consultant_Code NOT LIKE 'CD%' THEN REPLACE(Consultant_Code, 'C', '101')
                 WHEN Consultant_Code LIKE 'CD%' THEN REPLACE(Consultant_Code, 'CD', '202')
                 WHEN Consultant_Code LIKE 'N%' THEN REPLACE(Consultant_Code, 'N', '303')
                 WHEN Consultant_Code LIKE 'H%' THEN REPLACE(Consultant_Code, 'H', '404')
                 WHEN Consultant_Code LIKE 'G%' THEN REPLACE(Consultant_Code, 'G', '505')                                                                                 
                 WHEN Consultant_Code LIKE 'D%' THEN REPLACE(Consultant_Code, 'D', '606')
                 WHEN Consultant_Code LIKE 'M%' THEN REPLACE(Consultant_Code, 'M', '707')     
                 WHEN Consultant_Code LIKE 'S%' THEN REPLACE(Consultant_Code, 'S', '808')   
            END AS INT64)) IS NOT NULL
        UNION DISTINCT
        SELECT DISTINCT Consultant_Code FROM `CY_IMOSPHERE_WORKSPACE.src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2` WHERE Consultant_Code IS NOT NULL
        AND (CAST(
        CASE WHEN Consultant_Code LIKE 'C%' AND Consultant_Code NOT LIKE 'CD%' THEN REPLACE(Consultant_Code, 'C', '101')
                WHEN Consultant_Code LIKE 'CD%' THEN REPLACE(Consultant_Code, 'CD', '202')
                WHEN Consultant_Code LIKE 'N%' THEN REPLACE(Consultant_Code, 'N', '303')
                WHEN Consultant_Code LIKE 'H%' THEN REPLACE(Consultant_Code, 'H', '404')
                WHEN Consultant_Code LIKE 'G%' THEN REPLACE(Consultant_Code, 'G', '505')                                                                                 
                WHEN Consultant_Code LIKE 'D%' THEN REPLACE(Consultant_Code, 'D', '606')
                WHEN Consultant_Code LIKE 'M%' THEN REPLACE(Consultant_Code, 'M', '707')     
                WHEN Consultant_Code LIKE 'S%' THEN REPLACE(Consultant_Code, 'S', '808')   
        END AS INT64)) IS NOT NULL
    ));

    SET omop_provider_count = (SELECT COUNT(provider_id) FROM `CY_IMOSPHERE_CDM_531.provider`);

    SELECT 
      'Provider' Type,
      provider_count SourceCount,
      omop_provider_count CDMnCount
    FROM
    (
      SELECT provider_count, omop_provider_count
    )
    WHERE provider_count != omop_provider_count;
END;