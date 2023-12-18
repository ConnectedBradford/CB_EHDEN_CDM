CREATE OR REPLACE PROCEDURE CY_IMOSPHERE_WORKSPACE.COUNT_CARE_SITE()
OPTIONS (strict_mode=false)
BEGIN

   /*
       --care_site Count
   */

    DECLARE care_site_count INT64;
    DECLARE omop_care_site_count INT64;    
    
    SET care_site_count = (SELECT COUNT(*) FROM (SELECT DISTINCT cs.Id FROM `CY_IMOSPHERE_WORKSPACE.tbl_srorganisation` cs JOIN (SELECT Id, MAX(RowIdentifier) RowIdentifier FROM `CY_IMOSPHERE_WORKSPACE.tbl_srorganisation` GROUP BY Id) o ON o.RowIdentifier = cs.RowIdentifier AND o.Id = cs.Id WHERE cs.Id IS NOT NULL AND cs.Id != 'NULL'));
    SET omop_care_site_count = (SELECT COUNT(care_site_id) FROM `CY_IMOSPHERE_CDM_531.care_site`);

    SELECT 
      'Care site' Type,
      care_site_count SourceCount,
      omop_care_site_count CDMCount
    FROM
    (
        SELECT care_site_count, omop_care_site_count
    )
    WHERE care_site_count != omop_care_site_count;
END;