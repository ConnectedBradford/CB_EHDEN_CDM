CREATE OR REPLACE PROCEDURE CY_IMOSPHERE_WORKSPACE.COUNT_DEATH()
OPTIONS (strict_mode=false)
BEGIN
   /*
       --death Count
   */

    DECLARE death_count INT64;
    DECLARE omop_death_count INT64;    
    
    SET death_count = (SELECT COUNT(DISTINCT person_id) FROM `CY_IMOSPHERE_WORKSPACE.tbl_SRPatient` WHERE DateDeath IS NOT NULL);
    SET omop_death_count = (SELECT COUNT(person_id) FROM `CY_IMOSPHERE_CDM_531.death`);

    SELECT 
      'Death' Type,
      death_count SourceCount,
      omop_death_count CDMCount
    FROM
    (
        SELECT death_count, omop_death_count
    )
    WHERE death_count != omop_death_count;
END;