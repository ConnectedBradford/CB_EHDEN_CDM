CREATE OR REPLACE PROCEDURE CY_IMOSPHERE_WORKSPACE.TRANSFER_PROVIDER(run_id INT64)
OPTIONS (strict_mode=false)
BEGIN
   /*
        --[tbl_SRCode] Transfer
    */

    INSERT INTO `CY_IMOSPHERE_CDM_531.provider`
    (
        provider_id,
        provider_source_value
    )
    SELECT DISTINCT    
        CAST(iddoneby AS INT64) provider_id,
        iddoneby provider_source_value
        FROM `CY_IMOSPHERE_WORKSPACE.tbl_SRCode` p
        LEFT JOIN `CY_IMOSPHERE_CDM_531.provider` op ON op.provider_id = CAST(p.iddoneby AS INT64)
    WHERE iddoneby IS NOT NULL
    AND iddoneby NOT IN ('-1')
    AND op.provider_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added provider', run_id, 'tbl_SRCode -> provider', @@row_count, CURRENT_DATETIME();
     

    /*
        --[src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2] Transfer
    */

    INSERT INTO `CY_IMOSPHERE_CDM_531.provider`
    (
        provider_id,
        specialty_concept_id,
        provider_source_value
    )
    SELECT DISTINCT           
        CAST(CASE WHEN Consultant_Code LIKE 'C%' AND Consultant_Code NOT LIKE 'CD%' 
                THEN REPLACE(Consultant_Code, 'C', '101')

                WHEN Consultant_Code LIKE 'CD%' 
                THEN REPLACE(Consultant_Code, 'CD', '202')

                WHEN Consultant_Code LIKE 'N%' 
                THEN REPLACE(Consultant_Code, 'N', '303')            
                                                                                  
                WHEN Consultant_Code LIKE 'H%' 
                THEN REPLACE(Consultant_Code, 'H', '404')

                WHEN Consultant_Code LIKE 'G%' 
                THEN REPLACE(Consultant_Code, 'G', '505') 
                                                                                  
                WHEN Consultant_Code LIKE 'D%' 
                THEN REPLACE(Consultant_Code, 'D', '606')

                WHEN Consultant_Code LIKE 'M%' 
                THEN REPLACE(Consultant_Code, 'M', '707')     
                                                                                  
                WHEN Consultant_Code LIKE 'S%' 
                THEN REPLACE(Consultant_Code, 'S', '808')   
        END AS INT64) provider_id,
        cd.concept_id specialty_concept_id,
        Consultant_Code provider_source_value
    FROM 
    (
        SELECT
            Consultant_Code,
            ARRAY_AGG(STRUCT(concept_id) ORDER BY concept_id DESC LIMIT 1)[OFFSET(0)] cd
        FROM 
        (
            SELECT
                Consultant_Code,
                concept_id
            FROM `CY_IMOSPHERE_WORKSPACE.src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2` p
            LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` MainSpecCo on MainSpecCo.source_value = p.Main_Specialty_Code  AND MainSpecCo.destination_table = 'PROVIDER' AND MainSpecCo.source_table = 'src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2' AND MainSpecCo.source_column = 'Main_Specialty_Code'
            UNION DISTINCT
            SELECT
                Consultant_Code,
                concept_id
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_OP_010415_to_300619` p
            LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` MainSpecCo on MainSpecCo.source_value = p.Main_Specialty_Code  AND MainSpecCo.destination_table = 'PROVIDER' AND MainSpecCo.source_table = 'CY_IMOSPHERE_WORKSPACE.SUS_BRI_OP_010415_to_300619' AND MainSpecCo.source_column = 'Main_Specialty_Code'
        ) p
        GROUP BY Consultant_Code
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.provider` op ON op.provider_id = CAST(CASE WHEN Consultant_Code LIKE 'C%' AND Consultant_Code NOT LIKE 'CD%' 
                                                                                THEN REPLACE(Consultant_Code, 'C', '101')

                                                                                WHEN Consultant_Code LIKE 'CD%' 
                                                                                THEN REPLACE(Consultant_Code, 'CD', '202')

                                                                                WHEN Consultant_Code LIKE 'N%' 
                                                                                THEN REPLACE(Consultant_Code, 'N', '303')            
                                                                                  
                                                                                WHEN Consultant_Code LIKE 'H%' 
                                                                                THEN REPLACE(Consultant_Code, 'H', '404')

                                                                                WHEN Consultant_Code LIKE 'G%' 
                                                                                THEN REPLACE(Consultant_Code, 'G', '505') 
                                                                                  
                                                                                WHEN Consultant_Code LIKE 'D%' 
                                                                                THEN REPLACE(Consultant_Code, 'D', '606')

                                                                                WHEN Consultant_Code LIKE 'M%' 
                                                                                THEN REPLACE(Consultant_Code, 'M', '707')     
                                                                                  
                                                                                WHEN Consultant_Code LIKE 'S%' 
                                                                                THEN REPLACE(Consultant_Code, 'S', '808')                                                                              
                                                                            END AS INT64)
    WHERE Consultant_Code IS NOT NULL
    AND (CAST(CASE WHEN Consultant_Code LIKE 'C%' AND Consultant_Code NOT LIKE 'CD%' 
                    THEN REPLACE(Consultant_Code, 'C', '101')

                    WHEN Consultant_Code LIKE 'CD%' 
                    THEN REPLACE(Consultant_Code, 'CD', '202')

                    WHEN Consultant_Code LIKE 'N%' 
                    THEN REPLACE(Consultant_Code, 'N', '303')            
                                                                                  
                    WHEN Consultant_Code LIKE 'H%' 
                    THEN REPLACE(Consultant_Code, 'H', '404')

                    WHEN Consultant_Code LIKE 'G%' 
                    THEN REPLACE(Consultant_Code, 'G', '505') 
                                                                                  
                    WHEN Consultant_Code LIKE 'D%' 
                    THEN REPLACE(Consultant_Code, 'D', '606')

                    WHEN Consultant_Code LIKE 'M%' 
                    THEN REPLACE(Consultant_Code, 'M', '707')     
                                                                                  
                    WHEN Consultant_Code LIKE 'S%' 
                    THEN REPLACE(Consultant_Code, 'S', '808')   
            END AS INT64)) IS NOT NULL
    AND op.provider_id IS NULL;
        
    /*
        --[SUS_BRI_APC_010415_to_300619_P1] Transfer
    */

    INSERT INTO `CY_IMOSPHERE_CDM_531.provider`
    (
        provider_id,
        provider_source_value
    )
    SELECT DISTINCT        
        CAST(CASE WHEN Consultant_Code LIKE 'C%' AND Consultant_Code NOT LIKE 'CD%' 
                THEN REPLACE(Consultant_Code, 'C', '101')

                WHEN Consultant_Code LIKE 'CD%' 
                THEN REPLACE(Consultant_Code, 'CD', '202')

                WHEN Consultant_Code LIKE 'N%' 
                THEN REPLACE(Consultant_Code, 'N', '303')            
                                                                                  
                WHEN Consultant_Code LIKE 'H%' 
                THEN REPLACE(Consultant_Code, 'H', '404')

                WHEN Consultant_Code LIKE 'G%' 
                THEN REPLACE(Consultant_Code, 'G', '505') 
                                                                                  
                WHEN Consultant_Code LIKE 'D%' 
                THEN REPLACE(Consultant_Code, 'D', '606')

                WHEN Consultant_Code LIKE 'M%' 
                THEN REPLACE(Consultant_Code, 'M', '707')     
                                                                                  
                WHEN Consultant_Code LIKE 'S%' 
                THEN REPLACE(Consultant_Code, 'S', '808')   
        END AS INT64) provider_id,
        Consultant_Code provider_source_value
    FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1` p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.provider` op ON op.provider_id = CAST(CASE WHEN Consultant_Code LIKE 'C%' AND Consultant_Code NOT LIKE 'CD%' 
                                                                                THEN REPLACE(Consultant_Code, 'C', '101')

                                                                                WHEN Consultant_Code LIKE 'CD%' 
                                                                                THEN REPLACE(Consultant_Code, 'CD', '202')

                                                                                WHEN Consultant_Code LIKE 'N%' 
                                                                                THEN REPLACE(Consultant_Code, 'N', '303')            
                                                                                  
                                                                                WHEN Consultant_Code LIKE 'H%' 
                                                                                THEN REPLACE(Consultant_Code, 'H', '404')

                                                                                WHEN Consultant_Code LIKE 'G%' 
                                                                                THEN REPLACE(Consultant_Code, 'G', '505') 
                                                                                  
                                                                                WHEN Consultant_Code LIKE 'D%' 
                                                                                THEN REPLACE(Consultant_Code, 'D', '606')

                                                                                WHEN Consultant_Code LIKE 'M%' 
                                                                                THEN REPLACE(Consultant_Code, 'M', '707')     
                                                                                  
                                                                                WHEN Consultant_Code LIKE 'S%' 
                                                                                THEN REPLACE(Consultant_Code, 'S', '808')   
                                                                            END AS INT64)
    WHERE Consultant_Code IS NOT NULL
    AND (CAST(CASE WHEN Consultant_Code LIKE 'C%' AND Consultant_Code NOT LIKE 'CD%' 
                    THEN REPLACE(Consultant_Code, 'C', '101')

                    WHEN Consultant_Code LIKE 'CD%' 
                    THEN REPLACE(Consultant_Code, 'CD', '202')

                    WHEN Consultant_Code LIKE 'N%' 
                    THEN REPLACE(Consultant_Code, 'N', '303')            
                                                                                  
                    WHEN Consultant_Code LIKE 'H%' 
                    THEN REPLACE(Consultant_Code, 'H', '404')

                    WHEN Consultant_Code LIKE 'G%' 
                    THEN REPLACE(Consultant_Code, 'G', '505') 
                                                                                  
                    WHEN Consultant_Code LIKE 'D%' 
                    THEN REPLACE(Consultant_Code, 'D', '606')

                    WHEN Consultant_Code LIKE 'M%' 
                    THEN REPLACE(Consultant_Code, 'M', '707')     
                                                                                  
                    WHEN Consultant_Code LIKE 'S%' 
                    THEN REPLACE(Consultant_Code, 'S', '808')   
            END AS INT64)) IS NOT NULL
    AND op.provider_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added provider', run_id, 'SUS_BRI_APC_010415_to_300619_P1 -> provider', @@row_count, CURRENT_DATETIME();
END;