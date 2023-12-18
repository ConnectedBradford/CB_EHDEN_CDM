CREATE OR REPLACE PROCEDURE CY_IMOSPHERE_WORKSPACE.COUNT_PERSON()
OPTIONS (strict_mode=false)
BEGIN
    DECLARE person_count INT64;
    DECLARE omop_person_count INT64;   
    
    SET person_count = (
    SELECT
        COUNT(DISTINCT person_id),
    FROM
    (
        SELECT DISTINCT
            person_id
        FROM `CY_IMOSPHERE_WORKSPACE.tbl_SRPatient` p
        WHERE p.person_id IS NOT NULL AND p.DateBirth IS NOT NULL
        UNION DISTINCT
        SELECT DISTINCT
            person_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` p
        WHERE p.person_id IS NOT NULL AND p.Date_of_Birth IS NOT NULL
        UNION DISTINCT
        SELECT DISTINCT
            person_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1` p
        WHERE p.person_id IS NOT NULL AND p.Date_of_Birth IS NOT NULL AND (p.Date_of_Birth NOT LIKE '%-%' AND p.Date_of_Birth NOT LIKE '% %')
        UNION DISTINCT
        SELECT DISTINCT
            person_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_OP_010415_to_300619` p
        WHERE p.person_id IS NOT NULL AND p.Date_of_Birth IS NOT NULL AND (p.Date_of_Birth NOT LIKE '%-%' AND p.Date_of_Birth NOT LIKE '% %')
        UNION DISTINCT
        SELECT DISTINCT
            person_id
        FROM `CY_IMOSPHERE_WORKSPACE.src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2` p
        WHERE p.person_id IS NOT NULL AND p.Date_of_Birth_mm_yy IS NOT NULL
    ) p);

    SET omop_person_count = (SELECT COUNT(person_id) FROM `CY_IMOSPHERE_CDM_531.person`);

    SELECT 
      'Person' Type,
      NULL Id,
      person_count SourceCount,
      omop_person_count DestinationCount
    FROM
    (
        SELECT person_count, omop_person_count
    )
    WHERE person_count != omop_person_count;
END;