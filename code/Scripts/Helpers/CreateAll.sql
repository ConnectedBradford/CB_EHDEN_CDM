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

CREATE OR REPLACE PROCEDURE CY_IMOSPHERE_WORKSPACE.POPULATE_SPELLNUMBER_LOOKUPS()
OPTIONS (strict_mode=false)
BEGIN 
    
    /*
        CREATE DISTINCT SPELL NUMBER TABLE
    */

    CREATE TABLE IF NOT EXISTS `yhcr-prd-phm-bia-core.CY_IMOSPHERE_WORKSPACE.tbl_etl_spell_number`
    (
        spell_number STRING NOT NULL
    );

    
    /*
        [src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2] Transfer
    */

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_spell_number`
    (
        spell_number 
    )
    SELECT
        DISTINCT CAST(Hospital_Provider_Spell_Number AS STRING)
    FROM `CY_IMOSPHERE_WORKSPACE.src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2` vo
    LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_spell_number` ol ON ol.spell_number = vo.Hospital_Provider_Spell_Number
    WHERE Hospital_Provider_Spell_Number IS NOT NULL
    AND ol.spell_number IS NULL;


    /*
        [SUS_BRI_APC_010415_to_300619_P1] Transfer
    */

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_spell_number`
    (
        spell_number 
    )
    SELECT
        DISTINCT CAST(Hospital_Provider_Spell_Number AS STRING)
    FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1` vo
    LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_spell_number` ol ON ol.spell_number = vo.Hospital_Provider_Spell_Number
    WHERE Hospital_Provider_Spell_Number IS NOT NULL
    AND ol.spell_number IS NULL;
END;

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

CREATE OR REPLACE PROCEDURE CY_IMOSPHERE_WORKSPACE.TRANSFER_PERSON(run_id INT64)
OPTIONS (strict_mode=false)
BEGIN   
    /*
        This is a bit complicated as it collects data from each table in order. Once a valid value is found for a field, further records will be ignored. 

        Pending: rules for choosing data are currently under review, currently it is the first valid record, but this a placeholder only.
    */
    CREATE TEMP TABLE PERSON
    (
        person_id INT64,
        birth_datetime DATETIME,
        gender_concept_id INT64,
        gender_source_value STRING,
        race_concept_id INT64,
        race_source_value STRING,
        care_site_id INT64,
        location_id INT64
    );

    INSERT INTO PERSON (person_id)
    SELECT DISTINCT
        person_id,
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
    ) p
    WHERE NOT EXISTS (SELECT * FROM `CY_IMOSPHERE_CDM_531.person` op WHERE op.person_id = p.person_id);

   /*
        --[tbl_SRPatient] 
   */

    CREATE TEMP TABLE PERSON_tbl_SRPatient
    (
        person_id INT64,
        birth_datetime DATETIME,
        gender_concept_id INT64,
        gender_source_value STRING,
        race_concept_id INT64,
        race_source_value STRING,
        care_site_id INT64,
        location_id INT64
    );

    INSERT INTO PERSON_tbl_SRPatient
    (
        person_id,
        birth_datetime,
        gender_concept_id,
        gender_source_value,
        race_concept_id,
        race_source_value,
        care_site_id,
        location_id
    )
    WITH care_site_cte AS
    (
        select t.person_id, t.DateDeRegistration, o.ID
        from `CY_IMOSPHERE_WORKSPACE.tbl_SRPatientRegistration` t
        inner join (select person_id, max(DateDeRegistration) as DateDeRegistration
                    from `CY_IMOSPHERE_WORKSPACE.tbl_SRPatientRegistration` 
                    group by person_id) a
        on (t.person_id = a.person_id and t.DateDeRegistration = a.DateDeRegistration)
        LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_srorganisation` o  on o.RowIdentifier = t.RowIdentifier
        WHERE t.RegistrationStatus <> 'Applied'
        AND o.RowIdentifier IS NOT NULL
        AND o.ID <> 'NULL'
    ),
    location_cte AS
    ( 
        select t.person_id, t.PartialPostCode, t.DateEventRecorded
        from `CY_IMOSPHERE_WORKSPACE.tbl_SRPatientAddressHistory` t
        inner join (select person_id, max(DateEventRecorded) as DateEventRecorded
                    from `CY_IMOSPHERE_WORKSPACE.tbl_SRPatientAddressHistory` 
                    group by person_id) a
        on (t.person_id = a.person_id and t.DateEventRecorded = a.DateEventRecorded)
        WHERE t.AddressType = '55831'
        GROUP BY t.person_id, t.PartialPostCode, t.DateEventRecorded
    )
    SELECT DISTINCT
        p.person_id person_id,
        PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(DateBirth ,'01000000')) birth_datetime,
        COALESCE(gender.concept_id, 0) gender_concept_id,
        gender.source_value gender_source_value,	
        COALESCE(ethnicity.concept_id, 0) race_concept_id,
        ethnicity.source_value race_source_value,
        cs.care_site_id care_site_id,
        l.location_id location_id
    FROM `CY_IMOSPHERE_WORKSPACE.tbl_SRPatient` p
    JOIN PERSON op ON op.person_id = p.person_id     
    LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` gender on gender.source_value = p.Gender AND gender.destination_table = 'PERSON' AND gender.source_table = 'tbl_SRPatient' AND gender.source_column = 'Gender'
    LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` ethnicity on ethnicity.source_value = p.Ethnicity AND ethnicity.destination_table = 'PERSON' AND ethnicity.source_table = 'tbl_SRPatient' AND ethnicity.source_column = 'Ethnicity'
    LEFT JOIN care_site_cte cs_cte on  cs_cte.person_id = p.person_id
    LEFT JOIN `CY_IMOSPHERE_CDM_531.care_site` cs  on cs.care_site_source_value = cs_cte.ID
    LEFT JOIN location_cte l_cte on l_cte.person_id = p.person_id
    LEFT JOIN `CY_IMOSPHERE_CDM_531.location` l  on l.location_source_value = l_cte.PartialPostCode
    WHERE p.person_id IS NOT NULL
    AND p.DateBirth IS NOT NULL;

    -- Date of birth
    UPDATE PERSON op SET
        birth_datetime = p.dt.birth_datetime
    FROM
    (
        SELECT
            person_id,
            ARRAY_AGG(STRUCT(birth_datetime) ORDER BY person_id LIMIT 1)[OFFSET(0)] dt
        FROM PERSON_tbl_SRPatient GROUP BY person_id
    ) p
    WHERE op.person_id = p.person_id;

    -- Gender
    UPDATE PERSON op SET
        gender_concept_id = p.g.gender_concept_id,
        gender_source_value = p.g.gender_source_value
    FROM
    (
        SELECT
            person_id,
            ARRAY_AGG(STRUCT(gender_concept_id, gender_source_value) ORDER BY gender_concept_id DESC LIMIT 1)[OFFSET(0)] g
        FROM PERSON_tbl_SRPatient GROUP BY person_id
    ) p
    WHERE op.person_id = p.person_id;

    -- Race
    UPDATE PERSON op SET
        race_concept_id = p.r.race_concept_id,
        race_source_value = p.r.race_source_value
    FROM
    (
        SELECT
            person_id,
            ARRAY_AGG(STRUCT(race_concept_id, race_source_value) ORDER BY race_concept_id DESC LIMIT 1)[OFFSET(0)] r
        FROM PERSON_tbl_SRPatient GROUP BY person_id
    ) p
    WHERE op.person_id = p.person_id;

    -- Care site
    UPDATE PERSON op SET
        care_site_id = p.cs.care_site_id
    FROM
    (
        SELECT
            person_id,
            ARRAY_AGG(STRUCT(care_site_id) ORDER BY person_id LIMIT 1)[OFFSET(0)] cs
        FROM PERSON_tbl_SRPatient GROUP BY person_id
    ) p
    WHERE op.person_id = p.person_id;

    -- Location
    UPDATE PERSON op SET
        location_id = p.l.location_id
    FROM
    (
        SELECT
            person_id,
            ARRAY_AGG(STRUCT(location_id) ORDER BY person_id LIMIT 1)[OFFSET(0)] l
        FROM PERSON_tbl_SRPatient GROUP BY person_id
    ) p
    WHERE op.person_id = p.person_id;

    /*
        --[SUS_BRI_AE_010415_to_300619] 
    */

    CREATE TEMP TABLE PERSON_SUS_BRI_AE_010415_to_300619
    (
        person_id INT64,
        birth_datetime DATETIME,
        gender_concept_id INT64,
        gender_source_value STRING,
        race_concept_id INT64,
        race_source_value STRING,
        location_id INT64
    );

    INSERT INTO PERSON_SUS_BRI_AE_010415_to_300619
        (
        person_id,
        birth_datetime,
        gender_concept_id,
        gender_source_value,
        race_concept_id,
        race_source_value,
        location_id   
    )
    SELECT DISTINCT
        p.person_id person_id,
        PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(Date_of_Birth ,'01000000')) birth_datetime,
        COALESCE(gender.concept_id, 0) gender_concept_id,
        gender.source_value gender_source_value,	
        COALESCE(ethnicity.concept_id, 0) race_concept_id,
        ethnicity.source_value race_source_value,
        l.location_id location_id
    FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` p 
    JOIN PERSON op ON op.person_id = p.person_id 
    LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` gender on gender.source_value = p.Sex AND gender.destination_table = 'PERSON' AND gender.source_table = 'SUS_BRI_AE_010415_to_300619' AND gender.source_column = 'Sex'
    LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` ethnicity on ethnicity.source_value = p.Ethnic_Category AND ethnicity.destination_table = 'PERSON' AND ethnicity.source_table = 'SUS_BRI_AE_010415_to_300619' AND ethnicity.source_column = 'Ethnic_Category'
    LEFT JOIN `CY_IMOSPHERE_CDM_531.location` l  on l.location_source_value = p.Postcode
    WHERE p.person_id IS NOT NULL
    AND p.Date_of_Birth IS NOT NULL;

    -- Date of birth
    UPDATE PERSON op SET
        birth_datetime = p.dt.birth_datetime
    FROM
    (
        SELECT
            person_id,
            ARRAY_AGG(STRUCT(birth_datetime) ORDER BY person_id LIMIT 1)[OFFSET(0)] dt
        FROM PERSON_SUS_BRI_AE_010415_to_300619 GROUP BY person_id
    ) p
    WHERE op.person_id = p.person_id 
    AND op.birth_datetime IS NULL;

    -- Gender
    UPDATE PERSON op SET
        gender_concept_id = p.g.gender_concept_id,
        gender_source_value = p.g.gender_source_value
    FROM
    (
        SELECT
            person_id,
            ARRAY_AGG(STRUCT(gender_concept_id, gender_source_value) ORDER BY gender_concept_id DESC LIMIT 1)[OFFSET(0)] g
        FROM PERSON_SUS_BRI_AE_010415_to_300619 GROUP BY person_id
    ) p
    WHERE op.person_id = p.person_id 
    AND (op.gender_concept_id IS NULL OR (op.gender_concept_id = 0 AND p.g.gender_concept_id !=0))
    AND p.g.gender_concept_id IS NOT NULL;

    -- Race
    UPDATE PERSON op SET
        race_concept_id = p.r.race_concept_id,
        race_source_value = p.r.race_source_value
    FROM
    (
        SELECT
            person_id,
            ARRAY_AGG(STRUCT(race_concept_id, race_source_value) ORDER BY race_concept_id DESC LIMIT 1)[OFFSET(0)] r
        FROM PERSON_SUS_BRI_AE_010415_to_300619 GROUP BY person_id
    ) p
    WHERE op.person_id = p.person_id
    AND (op.race_concept_id IS NULL OR (op.race_concept_id = 0 AND p.r.race_concept_id !=0))
    AND p.r.race_concept_id IS NOT NULL;

    -- Location
    UPDATE PERSON op SET
        location_id = p.l.location_id
    FROM
    (
        SELECT
            person_id,
            ARRAY_AGG(STRUCT(location_id) ORDER BY person_id LIMIT 1)[OFFSET(0)] l
        FROM PERSON_SUS_BRI_AE_010415_to_300619 GROUP BY person_id
    ) p
    WHERE op.person_id = p.person_id
    AND op.location_id IS NULL
    AND p.l.location_id IS NOT NULL;
    
    /*
        --[SUS_BRI_APC_010415_to_300619_P1]
    */
    
    CREATE TEMP TABLE PERSON_SUS_BRI_APC_010415_to_300619_P1
    (
        person_id INT64,
        birth_datetime DATETIME,
        gender_concept_id INT64,
        gender_source_value STRING,
        race_concept_id INT64,
        race_source_value STRING,
        location_id INT64
    );

    INSERT INTO PERSON_SUS_BRI_APC_010415_to_300619_P1
    (
        person_id,
        birth_datetime,
        gender_concept_id,
        gender_source_value,
        race_concept_id,
        race_source_value,
        location_id
    )
    SELECT DISTINCT    
            p.person_id person_id,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(Date_of_Birth ,'01000000')) birth_datetime,
            COALESCE(gender.concept_id, 0) gender_concept_id,
            gender.source_value gender_source_value,	
            COALESCE(ethnicity.concept_id, 0) race_concept_id,
            ethnicity.source_value race_source_value,
            l.location_id location_id
    FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1` p
    JOIN PERSON op ON op.person_id = p.person_id
    LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` gender on gender.source_value = p.Sex AND gender.destination_table = 'PERSON' AND gender.source_table = 'SUS_BRI_APC_010415_to_300619_P1' AND gender.source_column = 'Sex'
    LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` ethnicity on ethnicity.source_value = p.Ethnic_Group AND ethnicity.destination_table = 'PERSON' AND ethnicity.source_table = 'SUS_BRI_APC_010415_to_300619_P1' AND ethnicity.source_column = 'Ethnic_Group'
    LEFT JOIN `CY_IMOSPHERE_CDM_531.location` l  on l.location_source_value = p.Postcode
    WHERE p.person_id IS NOT NULL
    AND p.Date_of_Birth IS NOT NULL 
    AND (p.Date_of_Birth NOT LIKE '%-%' AND p.Date_of_Birth NOT LIKE '% %');

    -- Date of birth
    UPDATE PERSON op SET
        birth_datetime = p.dt.birth_datetime
    FROM
    (
        SELECT
            person_id,
            ARRAY_AGG(STRUCT(birth_datetime) ORDER BY person_id LIMIT 1)[OFFSET(0)] dt
        FROM PERSON_SUS_BRI_APC_010415_to_300619_P1 GROUP BY person_id
    ) p
    WHERE op.person_id = p.person_id 
    AND op.birth_datetime IS NULL;

    -- Gender
    UPDATE PERSON op SET
        gender_concept_id = p.g.gender_concept_id,
        gender_source_value = p.g.gender_source_value
    FROM
    (
        SELECT
            person_id,
            ARRAY_AGG(STRUCT(gender_concept_id, gender_source_value) ORDER BY gender_concept_id DESC LIMIT 1)[OFFSET(0)] g
        FROM PERSON_SUS_BRI_APC_010415_to_300619_P1 GROUP BY person_id
    ) p
    WHERE op.person_id = p.person_id 
    AND (op.gender_concept_id IS NULL OR (op.gender_concept_id = 0 AND p.g.gender_concept_id !=0))
    AND p.g.gender_concept_id IS NOT NULL;

    -- Race
    UPDATE PERSON op SET
        race_concept_id = p.r.race_concept_id,
        race_source_value = p.r.race_source_value
    FROM
    (
        SELECT
            person_id,
            ARRAY_AGG(STRUCT(race_concept_id, race_source_value) ORDER BY race_concept_id DESC LIMIT 1)[OFFSET(0)] r
        FROM PERSON_SUS_BRI_APC_010415_to_300619_P1 GROUP BY person_id
    ) p
    WHERE op.person_id = p.person_id
    AND (op.race_concept_id IS NULL OR (op.race_concept_id = 0 AND p.r.race_concept_id !=0))
    AND p.r.race_concept_id IS NOT NULL;

    -- Location
    UPDATE PERSON op SET
        location_id = p.l.location_id
    FROM
    (
        SELECT
            person_id,
            ARRAY_AGG(STRUCT(location_id) ORDER BY person_id LIMIT 1)[OFFSET(0)] l
        FROM PERSON_SUS_BRI_APC_010415_to_300619_P1 GROUP BY person_id
    ) p
    WHERE op.person_id = p.person_id
    AND op.location_id IS NULL
    AND p.l.location_id IS NOT NULL;

    /*
        --[SUS_BRI_OP_010415_to_300619]
    */
    CREATE TEMP TABLE PERSON_SUS_BRI_OP_010415_to_300619
    (
        person_id INT64,
        birth_datetime DATETIME,
        gender_concept_id INT64,
        gender_source_value STRING,
        race_concept_id INT64,
        race_source_value STRING,
        location_id INT64
    );

    INSERT INTO PERSON_SUS_BRI_OP_010415_to_300619
    (
        person_id,
        birth_datetime,
        gender_concept_id,
        gender_source_value,
        race_concept_id,
        race_source_value,
        location_id
    )
    SELECT DISTINCT  
        p.person_id person_id,
        PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(Date_of_Birth ,'01000000')) birth_datetime,
        COALESCE(gender.concept_id, 0) gender_concept_id,
        gender.source_value gender_source_value,	
        COALESCE(ethnicity.concept_id, 0) race_concept_id,
        ethnicity.source_value race_source_value,
        l.location_id location_id
    FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_OP_010415_to_300619` p
    JOIN PERSON op ON op.person_id = p.person_id   
    LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` gender on gender.source_value = p.Sex AND gender.destination_table = 'PERSON' AND gender.source_table = 'SUS_BRI_OP_010415_to_300619' AND gender.source_column = 'Sex'
    LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` ethnicity on ethnicity.source_value = p.Ethnic_Category AND ethnicity.destination_table = 'PERSON' AND ethnicity.source_table = 'SUS_BRI_OP_010415_to_300619' AND ethnicity.source_column = 'Ethnic_Category'
    LEFT JOIN `CY_IMOSPHERE_CDM_531.location` l  on l.location_source_value = p.Postcode
    WHERE p.person_id IS NOT NULL
    AND p.Date_of_Birth IS NOT NULl
    AND p.Date_of_Birth NOT LIKE '% %';

    -- Date of birth
    UPDATE PERSON op SET
        birth_datetime = p.dt.birth_datetime
    FROM
    (
        SELECT
            person_id,
            ARRAY_AGG(STRUCT(birth_datetime) ORDER BY person_id LIMIT 1)[OFFSET(0)] dt
        FROM PERSON_SUS_BRI_OP_010415_to_300619 GROUP BY person_id
    ) p
    WHERE op.person_id = p.person_id 
    AND op.birth_datetime IS NULL;

    -- Gender
    UPDATE PERSON op SET
        gender_concept_id = p.g.gender_concept_id,
        gender_source_value = p.g.gender_source_value
    FROM
    (
        SELECT
            person_id,
            ARRAY_AGG(STRUCT(gender_concept_id, gender_source_value) ORDER BY gender_concept_id DESC LIMIT 1)[OFFSET(0)] g
        FROM PERSON_SUS_BRI_OP_010415_to_300619 GROUP BY person_id
    ) p
    WHERE op.person_id = p.person_id 
    AND (op.gender_concept_id IS NULL OR (op.gender_concept_id = 0 AND p.g.gender_concept_id !=0))
    AND p.g.gender_concept_id IS NOT NULL;

    -- Race
    UPDATE PERSON op SET
        race_concept_id = p.r.race_concept_id,
        race_source_value = p.r.race_source_value
    FROM
    (
        SELECT
            person_id,
            ARRAY_AGG(STRUCT(race_concept_id, race_source_value) ORDER BY race_concept_id DESC LIMIT 1)[OFFSET(0)] r
        FROM PERSON_SUS_BRI_OP_010415_to_300619 GROUP BY person_id
    ) p
    WHERE op.person_id = p.person_id
    AND (op.race_concept_id IS NULL OR (op.race_concept_id = 0 AND p.r.race_concept_id !=0))
    AND p.r.race_concept_id IS NOT NULL;

    -- Location
    UPDATE PERSON op SET
        location_id = p.l.location_id
    FROM
    (
        SELECT
            person_id,
            ARRAY_AGG(STRUCT(location_id) ORDER BY person_id LIMIT 1)[OFFSET(0)] l
        FROM PERSON_SUS_BRI_OP_010415_to_300619 GROUP BY person_id
    ) p
    WHERE op.person_id = p.person_id
    AND op.location_id IS NULL
    AND p.l.location_id IS NOT NULL;

    /*
        --[src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2]
    */
    CREATE TEMP TABLE PERSON_src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2
    (
        person_id INT64,
        birth_datetime DATETIME,
        gender_concept_id INT64,
        gender_source_value STRING,
        race_concept_id INT64,
        race_source_value STRING,
        location_id INT64
    );

    INSERT INTO PERSON_src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2
    (
        person_id,
        birth_datetime,
        gender_concept_id,
        gender_source_value,
        race_concept_id,
        race_source_value,
        location_id
    )
    WITH dob_cte AS
    (
       SELECT DISTINCT        
            p.person_id person_id,
            CASE WHEN CAST(SPLIT(Date_of_Birth_mm_yy, '-')[OFFSET(1)] AS INT64) BETWEEN 00 AND 22 THEN CONCAT('20', CAST(SPLIT(Date_of_Birth_mm_yy, '-')[OFFSET(1)] as STRING))
              ELSE CONCAT('19', CAST(SPLIT(Date_of_Birth_mm_yy, '-')[OFFSET(1)] as STRING))      
            END year_of_birth,
            FORMAT_DATE( "%m", PARSE_DATE("%b-%y",Date_of_Birth_mm_yy )) month_of_birth
        FROM `CY_IMOSPHERE_WORKSPACE.src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2` p
    )
    SELECT DISTINCT  
        p.person_id person_id,
        PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(dob.year_of_birth, dob.month_of_birth,'01000000')) birth_datetime,
        COALESCE(gender.concept_id, 0) gender_concept_id,
        gender.source_value gender_source_value,	
        COALESCE(ethnicity.concept_id, 0) race_concept_id,
        ethnicity.source_value race_source_value,
        l.location_id location_id
    FROM `CY_IMOSPHERE_WORKSPACE.src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2` p
    JOIN PERSON op ON op.person_id = p.person_id   
    LEFT JOIN dob_cte dob ON dob.person_id = p.person_id    
    LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` gender on gender.source_value = p.Sex AND gender.destination_table = 'PERSON' AND gender.source_table = 'src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2' AND gender.source_column = 'Sex'
    LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` ethnicity on ethnicity.source_value = p.Ethnic_Group AND ethnicity.destination_table = 'PERSON' AND ethnicity.source_table = 'src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2' AND ethnicity.source_column = 'Ethnic_Group'
    LEFT JOIN `CY_IMOSPHERE_CDM_531.location` l  on l.location_source_value = p.Postcode_area_only
    WHERE p.person_id IS NOT NULL
    AND p.Date_of_Birth_mm_yy IS NOT NULL;

    -- Date of birth
    UPDATE PERSON op SET
        birth_datetime = p.dt.birth_datetime
    FROM
    (
        SELECT
            person_id,
            ARRAY_AGG(STRUCT(birth_datetime) ORDER BY person_id LIMIT 1)[OFFSET(0)] dt
        FROM PERSON_src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2 GROUP BY person_id
    ) p
    WHERE op.person_id = p.person_id 
    AND op.birth_datetime IS NULL;

    -- Gender
    UPDATE PERSON op SET
        gender_concept_id = p.g.gender_concept_id,
        gender_source_value = p.g.gender_source_value
    FROM
    (
        SELECT
            person_id,
            ARRAY_AGG(STRUCT(gender_concept_id, gender_source_value) ORDER BY gender_concept_id DESC LIMIT 1)[OFFSET(0)] g
        FROM PERSON_src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2 GROUP BY person_id
    ) p
    WHERE op.person_id = p.person_id 
    AND (op.gender_concept_id IS NULL OR (op.gender_concept_id = 0 AND p.g.gender_concept_id !=0))
    AND p.g.gender_concept_id IS NOT NULL;

    -- Race
    UPDATE PERSON op SET
        race_concept_id = p.r.race_concept_id,
        race_source_value = p.r.race_source_value
    FROM
    (
        SELECT
            person_id,
            ARRAY_AGG(STRUCT(race_concept_id, race_source_value) ORDER BY race_concept_id DESC LIMIT 1)[OFFSET(0)] r
        FROM PERSON_src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2 GROUP BY person_id
    ) p
    WHERE op.person_id = p.person_id
    AND (op.race_concept_id IS NULL OR (op.race_concept_id = 0 AND p.r.race_concept_id !=0))
    AND p.r.race_concept_id IS NOT NULL;

    -- Location
    UPDATE PERSON op SET
        location_id = p.l.location_id
    FROM
    (
        SELECT
            person_id,
            ARRAY_AGG(STRUCT(location_id) ORDER BY person_id LIMIT 1)[OFFSET(0)] l
        FROM PERSON_src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2 GROUP BY person_id
    ) p
    WHERE op.person_id = p.person_id
    AND op.location_id IS NULL
    AND p.l.location_id IS NOT NULL;

    /*
        -- Patient Transfer
    */
    INSERT INTO `CY_IMOSPHERE_CDM_531.person`
        (
        person_id,
        person_source_value,
        year_of_birth,
        month_of_birth,
        birth_datetime,
        gender_concept_id,
        gender_source_value,
        race_concept_id,
        race_source_value,
        care_site_id,
        location_id,                
        ethnicity_concept_id
    )
    SELECT DISTINCT
        p.person_id person_id,
        CAST(p.person_id AS STRING) person_source_value,
        EXTRACT(YEAR FROM p.birth_datetime) year_of_birth,
        EXTRACT(MONTH FROM p.birth_datetime) month_of_birth,
        p.birth_datetime birth_datetime,
        p.gender_concept_id,
        p.gender_source_value,	
        p.race_concept_id,
        p.race_source_value,
        p.care_site_id,
        p.location_id,
	    0 ethnicity_concept_id --No matching concept
    FROM PERSON p;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id,  related_table, related_count, logged_at) 
    SELECT 'Added person', run_id, 'tbl_SRPatient, SUS_BRI_AE_010415_to_300619, SUS_BRI_APC_010415_to_300619_P1, SUS_BRI_OP_010415_to_300619, src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2 -> person', @@row_count, CURRENT_DATETIME();
END;

CREATE OR REPLACE PROCEDURE CY_IMOSPHERE_WORKSPACE.TRANSFER_DEATH(run_id INT64)
OPTIONS (strict_mode=false)
BEGIN    
    /*
        Pending: rules for choosing death date are currently under review, currently it is the first valid record, but this a placeholder only.
    */

    /*
        --[tbl_SRPatient] Transfer
    */

    INSERT INTO `CY_IMOSPHERE_CDM_531.death`
    (
        person_id,
        death_date,
        death_datetime,
        death_type_concept_id
    )
    SELECT DISTINCT      
        d.person_id person_id,
        PARSE_DATE("%Y%m%d", CONCAT(pd.DateDeath ,'01'))  death_date,
        PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(pd.datedeath ,'01000000')) death_datetime,
        32817 death_type_concept_id --EHR
        FROM 
        (
            SELECT
                person_id,
                ARRAY_AGG(STRUCT(DateDeath) ORDER BY person_id LIMIT 1)[OFFSET(0)] pd
            FROM `CY_IMOSPHERE_WORKSPACE.tbl_SRPatient` d
            WHERE DateDeath IS NOT NULL
            GROUP BY person_id
        ) d
    LEFT JOIN `CY_IMOSPHERE_CDM_531.death` od ON od.person_id = d.person_id
    WHERE d.person_id IS NOT NULL
    AND od.person_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added death', run_id, 'tbl_SRPatient -> death', @@row_count, CURRENT_DATETIME();
END;

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

CREATE OR REPLACE PROCEDURE CY_IMOSPHERE_WORKSPACE.TRANSFER_VISIT_OCCURRENCE(run_id INT64)
OPTIONS (strict_mode=false)
BEGIN 
    /*
        Build mapping tables. These are required because there's no universal unique source id. This is done using the minimal set of data to get a unique ids.
    */

    DECLARE max_current_id INT64;
    CREATE TABLE IF NOT EXISTS CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups(id INT64, source_value STRING, source_table STRING);

    --tbl_SRVisit ids
    SET max_current_id = COALESCE((SELECT id FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` (id, source_value, source_table)
    SELECT
        max_current_id + ROW_NUMBER() OVER() id,
        source_value,
        source_table
    FROM
    (
        SELECT DISTINCT
            CONCAT(CAST(vo.RowIdentifier as STRING),'_', person_id) source_value,
            'tbl_SRVisit' source_table
        FROM `CY_IMOSPHERE_WORKSPACE.tbl_SRVisit` vo
        WHERE vo.person_id IS NOT NULL
        AND vo.RowIdentifier IS NOT NULL
        AND NOT EXISTS (SELECT * FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_value = CONCAT(CAST(vo.RowIdentifier as STRING),'_', person_id) AND l.source_table = 'tbl_SRVisit')
    ) i;

    --SUS_BRI_OP_010415_to_300619 ids
    SET max_current_id = COALESCE((SELECT id FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` (id, source_value, source_table)
    SELECT
        max_current_id + ROW_NUMBER() OVER() id,
        source_value,
        source_table
    FROM
    (
        SELECT DISTINCT
            CONCAT(CAST(vo.Generated_Record_Identifier as STRING),'_', person_id) source_value,
            'SUS_BRI_OP_010415_to_300619' source_table
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_OP_010415_to_300619` vo
        WHERE vo.person_id IS NOT NULL
        AND vo.Generated_Record_Identifier IS NOT NULL
        AND NOT EXISTS (SELECT * FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_value = CONCAT(CAST(vo.Generated_Record_Identifier as STRING),'_', person_id) AND l.source_table = 'SUS_BRI_OP_010415_to_300619')
    ) i;

    --SUS_BRI_APC_010415_to_300619_P1, src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2 ids
    SET max_current_id = COALESCE((SELECT id FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` (id, source_value, source_table)
    SELECT
        max_current_id + ROW_NUMBER() OVER() id,
        source_value,
        'spell_number' source_table
    FROM
    (
        SELECT DISTINCT
            CONCAT(CAST(vo.Hospital_Provider_Spell_Number as STRING),'_', person_id) source_value           
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1` vo
        WHERE vo.Start_Date_Hospital_Provider_Spell IS NOT NULL
        AND vo.person_id IS NOT NULL
        UNION DISTINCT
        SELECT DISTINCT
          CONCAT(CAST(vo.Hospital_Provider_Spell_Number as STRING),'_', person_id) source_value
        FROM `CY_IMOSPHERE_WORKSPACE.src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2` vo
        WHERE vo.Start_Date_Hospital_Provider_Spell IS NOT NULL
        AND vo.person_id IS NOT NULL
    ) i
    WHERE NOT EXISTS (SELECT * FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_value = i.source_value  AND l.source_table = 'spell_number');

    --SUS_BRI_AE_010415_to_300619 ids
    SET max_current_id = COALESCE((SELECT id FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` (id, source_value, source_table)
    SELECT
        max_current_id + ROW_NUMBER() OVER() id,
        i.source_value,
        'SUS_BRI_AE_010415_to_300619' source_table
    FROM
    (
        SELECT DISTINCT
            CONCAT(CAST(vo.AandE_Attendance_Number as STRING),'_', person_id) source_value           
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` vo
        WHERE vo.Arrival_Date IS NOT NULL
        AND vo.person_id IS NOT NULL
        AND vo.AandE_Attendance_Number IS NOT NULL
        AND vo.AandE_Arrival_Mode IS NOT NULL
    ) i
    WHERE NOT EXISTS (SELECT * FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_value = i.source_value AND l.source_table = 'SUS_BRI_AE_010415_to_300619');

    /*
        --[tbl_SRVisit] Transfer
    */

    INSERT INTO `CY_IMOSPHERE_CDM_531.visit_occurrence`
    (
        visit_occurrence_id,
        person_id,
        visit_concept_id,
        visit_start_date,
        visit_start_datetime,
        visit_end_date,
        visit_end_datetime,
        visit_type_concept_id,
        care_site_id
    )
    SELECT DISTINCT     
        vl.id visit_occurrence_id,
        vo.person_id person_id,
        9202 visit_concept_id, --Outpatient Visit
        CAST(DateBooked AS DATE)  visit_start_date,
        DateBooked visit_start_datetime,
        CAST(DateBooked AS DATE)  visit_end_date,
        DateBooked visit_end_datetime,
        32817 visit_type_concept_id, --EHR
        cs.care_site_id care_site_id
    FROM `CY_IMOSPHERE_WORKSPACE.tbl_SRVisit` vo
    JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'tbl_SRVisit') vl ON vl.source_value = CONCAT(CAST(vo.RowIdentifier as STRING),'_', person_id)
    LEFT JOIN `CY_IMOSPHERE_CDM_531.care_site` cs ON cs.care_site_source_value = vo.idorganisation
    LEFT JOIN `CY_IMOSPHERE_CDM_531.visit_occurrence` ovo ON ovo.visit_occurrence_id = vl.id
    WHERE vo.DateBooked IS NOT NULL
    AND vo.person_id IS NOT NULL
    AND vo.RowIdentifier IS NOT NULL
    AND ovo.visit_occurrence_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id,  related_table, related_count, logged_at) 
    SELECT 'Added visit_occurrence', run_id, 'tbl_SRVisit -> visit_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_OP_010415_to_300619] Transfer
    */

    INSERT INTO `CY_IMOSPHERE_CDM_531.visit_occurrence`
    (
        visit_occurrence_id,
        person_id,
        visit_concept_id,
        visit_start_date,
        visit_start_datetime,
        visit_end_date,
        visit_end_datetime,
        provider_id,
        visit_type_concept_id
    )
    SELECT DISTINCT
        vl.id visit_occurrence_id,
        vo.person_id,
        9202 visit_concept_id, --Outpatient Visit
        PARSE_DATE("%Y%m%d", Appointment_Date) visit_start_date,
        CASE WHEN Appointment_Time LIKE '% %' OR Appointment_Time IS NULL
            THEN PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(Appointment_Date, '000000'))
            ELSE PARSE_DATETIME('%Y%m%d%H:%M%S', CONCAT(Appointment_Date, Appointment_Time, '00'))
        END visit_start_datetime,
        PARSE_DATE("%Y%m%d", Appointment_Date) visit_end_date,
        CASE WHEN Appointment_Time LIKE '% %' OR Appointment_Time IS NULL
            THEN PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(Appointment_Date, '000000'))
            ELSE PARSE_DATETIME('%Y%m%d%H:%M%S', CONCAT(Appointment_Date, Appointment_Time, '00'))
        END visit_end_datetime,
        p.provider_id provider_id,
        32817 visit_type_concept_id, --EHR
    FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_OP_010415_to_300619` vo
    JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_OP_010415_to_300619') vl ON vl.source_value = CONCAT(CAST(vo.Generated_Record_Identifier as STRING),'_', person_id)
    LEFT JOIN `CY_IMOSPHERE_CDM_531.provider` p ON p.provider_source_value = Consultant_Code 
    LEFT JOIN `CY_IMOSPHERE_CDM_531.visit_occurrence` ovo ON ovo.visit_occurrence_id = vl.id
    WHERE vo.Appointment_Date IS NOT NULL
    AND vo.person_id IS NOT NULL
    AND vo.Generated_Record_Identifier IS NOT NULL
    AND ovo.visit_occurrence_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id,  related_table, related_count, logged_at) 
    SELECT 'Added visit_occurrence', run_id, 'SUS_BRI_OP_010415_to_300619 -> visit_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_APC_010415_to_300619_P1] Transfer, [src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2] Transfer
    */

    INSERT INTO `CY_IMOSPHERE_CDM_531.visit_occurrence`
    (
        visit_occurrence_id,
        person_id,
        visit_concept_id,
        visit_start_date,
        visit_start_datetime,
        visit_end_date,
        visit_end_datetime,
        visit_type_concept_id,
        provider_id,
        admitting_source_concept_id,
        admitting_source_value,
        discharge_to_concept_id,
        discharge_to_source_value
    )          
    SELECT 
        vl.id visit_occurrence_id,
        person_id,
        9201 visit_concept_id,--Inpatient Visit
        MIN(PARSE_DATE("%Y%m%d", Start_Date_Hospital_Provider_Spell)) visit_start_date,
        MIN (CASE WHEN Start_Time_Hospital_Provider_Spell LIKE '% %' OR Start_Time_Hospital_Provider_Spell IS NULL
                THEN PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(REPLACE(Start_Date_Hospital_Provider_Spell, ' ', ''),'000000'))
                WHEN Start_Time_Hospital_Provider_Spell LIKE '%:'
                THEN PARSE_DATETIME('%Y%m%d%H:%M:%S', CONCAT(REPLACE(Start_Date_Hospital_Provider_Spell, ' ', ''), Start_Time_Hospital_Provider_Spell, '00'))
                ELSE PARSE_DATETIME('%Y%m%d%H:%M:%S', CONCAT(Start_Date_Hospital_Provider_Spell, Start_Time_Hospital_Provider_Spell))
            END) visit_start_datetime,
        MAX(PARSE_DATE("%Y%m%d", Discharge_Date_From_Hospital_Provider_Spell)) visit_end_date,
        MAX(CASE WHEN Discharge_Time_Hospital_Provider_Spell LIKE '% %' OR Discharge_Time_Hospital_Provider_Spell IS NULL
              THEN PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(REPLACE(Discharge_Date_From_Hospital_Provider_Spell, ' ', ''),'000000'))
              WHEN Discharge_Time_Hospital_Provider_Spell LIKE '%:'
              THEN PARSE_DATETIME('%Y%m%d%H:%M:%S', CONCAT(REPLACE(Discharge_Date_From_Hospital_Provider_Spell, ' ', ''), Discharge_Time_Hospital_Provider_Spell, '00'))
              ELSE PARSE_DATETIME('%Y%m%d%H:%M:%S', CONCAT(Discharge_Date_From_Hospital_Provider_Spell, Discharge_Time_Hospital_Provider_Spell))
          END) visit_end_datetime,
          32817 visit_type_concept_id, --EHR
          array_agg(struct(provider_id) ignore nulls order by Start_Date_Hospital_Provider_Spell asc limit 1)[offset(0)].provider_id provider_id,
          array_agg(struct(am.concept_id) ignore nulls order by Start_Date_Hospital_Provider_Spell asc limit 1)[offset(0)].concept_id admitting_source_concept_id,
          array_agg(struct(am.source_value) ignore nulls order by Start_Date_Hospital_Provider_Spell asc limit 1)[offset(0)].source_value admitting_source_value,
          array_agg(struct(dm.concept_id) ignore nulls order by Discharge_Date_From_Hospital_Provider_Spell desc limit 1)[offset(0)].concept_id discharge_to_concept_id,
          array_agg(struct(dm.source_value) ignore nulls order by Discharge_Date_From_Hospital_Provider_Spell desc limit 1)[offset(0)].source_value discharge_source_value
    FROM
    (
      SELECT
      person_id, 
        CONCAT(CAST(vo.Hospital_Provider_Spell_Number as STRING),'_', person_id) source_value,
        Start_Date_Hospital_Provider_Spell,
        Start_Time_Hospital_Provider_Spell,
        CASE WHEN Discharge_Date_From_Hospital_Provider_Spell IS NULL OR Discharge_Date_From_Hospital_Provider_Spell = 'NULL' OR Discharge_Date_From_Hospital_Provider_Spell LIKE '%  %' THEN Start_Date_Hospital_Provider_Spell ELSE Discharge_Date_From_Hospital_Provider_Spell END Discharge_Date_From_Hospital_Provider_Spell,
        CASE WHEN Discharge_Date_From_Hospital_Provider_Spell IS NULL OR Discharge_Date_From_Hospital_Provider_Spell = 'NULL' OR Discharge_Date_From_Hospital_Provider_Spell LIKE '%  %' THEN Start_Time_Hospital_Provider_Spell ELSE Discharge_Time_Hospital_Provider_Spell END Discharge_Time_Hospital_Provider_Spell,
        Consultant_Code,
        Source_of_Admission_Hospital_Provider_Spell,
        Discharge_Destination_Hospital_Provider_Spell
      FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1` vo
      UNION DISTINCT
      SELECT
        person_id,
        CONCAT(CAST(vo.Hospital_Provider_Spell_Number as STRING),'_', person_id) source_value,
        Start_Date_Hospital_Provider_Spell,
        Start_Time_Hospital_Provider_Spell,
        CASE WHEN Discharge_Date_From_Hospital_Provider_Spell IS NULL OR Discharge_Date_From_Hospital_Provider_Spell = 'NULL' OR Discharge_Date_From_Hospital_Provider_Spell LIKE '%  %' THEN Start_Date_Hospital_Provider_Spell ELSE Discharge_Date_From_Hospital_Provider_Spell END Discharge_Date_From_Hospital_Provider_Spell,
        CASE WHEN Discharge_Date_From_Hospital_Provider_Spell IS NULL OR Discharge_Date_From_Hospital_Provider_Spell = 'NULL' OR Discharge_Date_From_Hospital_Provider_Spell LIKE '%  %' THEN Start_Time_Hospital_Provider_Spell ELSE Discharge_Time_Hospital_Provider_Spell END Discharge_Time_Hospital_Provider_Spell,
        Consultant_Code,
        Source_of_Admission_Hospital_Provider_Spell,
        Discharge_Destination_Hospital_Provider_Spell
      FROM `CY_IMOSPHERE_WORKSPACE.src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2` vo
    ) vo
    JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = vo.source_value
    LEFT JOIN `CY_IMOSPHERE_CDM_531.provider` p ON p.provider_source_value = Consultant_Code
    LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` am on am.source_value = vo.Source_of_Admission_Hospital_Provider_Spell AND am.destination_table = 'VISIT_OCCURRENCE' AND am.source_table = 'src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2' AND am.source_column = 'Source_of_Admission_Hospital_Provider_Spell'
    LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` dm on dm.source_value = vo.Discharge_Destination_Hospital_Provider_Spell AND dm.destination_table = 'VISIT_OCCURRENCE' AND dm.source_table = 'src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2' AND dm.source_column = 'Discharge_Destination_Hospital_Provider_Spell'
    WHERE vo.Start_Date_Hospital_Provider_Spell IS NOT NULL
    AND person_id IS NOT NULL
    AND NOT EXISTS (SELECT * FROM `CY_IMOSPHERE_CDM_531.visit_occurrence` ovo WHERE ovo.visit_occurrence_id = vl.id)
    GROUP BY vl.id, person_id;
   
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id,  related_table, related_count, logged_at) 
    SELECT 'Added visit_occurrence', run_id, 'SUS_BRI_APC_010415_to_300619_P1, src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2  -> visit_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619] Transfer
    */

    INSERT INTO `CY_IMOSPHERE_CDM_531.visit_occurrence` 
    (
        visit_occurrence_id,
        person_id,
        visit_concept_id,
        visit_start_date,
        visit_start_datetime,
        visit_end_date,
        visit_end_datetime,
        visit_type_concept_id
    )
    SELECT      
        vl.id visit_occurrence_id,
        vo.person_id person_id,
        CASE WHEN AandE_Arrival_Mode = '1' 
            THEN 38004353  --Ambulance
            WHEN AandE_Arrival_Mode = '2'
            THEN 9203 --Emergency Room Visit
        END visit_concept_id,
        MIN(PARSE_DATE("%Y%m%d", Arrival_Date)) visit_start_date,
        MIN(PARSE_DATETIME('%Y%m%d%H:%M:%S', CONCAT(Arrival_Date, Arrival_Time, ':00'))) visit_start_datetime,
        MAX(CASE WHEN PARSE_DATE("%Y%m%d", AandE_Departure_Date) IS NOT NULL THEN PARSE_DATE("%Y%m%d", AandE_Departure_Date) 
            WHEN PARSE_DATE("%Y%m%d", AandE_Departure_Date) IS NULL AND cast(timestamp_diff(PARSE_DATETIME('%Y%m%d%H:%M:%S', CONCAT(Arrival_Date, Arrival_Time, ':00')) , PARSE_DATETIME('%Y%m%d%H:%M:%S', CONCAT(Arrival_Date, AandE_Departure_Time)), minute) as INT64) > 0
            THEN DATE_ADD(DATE (PARSE_DATE("%Y%m%d", Arrival_Date)), INTERVAL 1 DAY)
                WHEN AandE_Departure_Date IS NULL AND CAST(timestamp_diff(PARSE_DATETIME('%Y%m%d%H:%M:%S', CONCAT(Arrival_Date, Arrival_Time, ':00')) , PARSE_DATETIME('%Y%m%d%H:%M:%S', CONCAT(Arrival_Date, AandE_Departure_Time)), minute) AS INT64) < 0
                THEN PARSE_DATE("%Y%m%d", Arrival_Date)
        ELSE PARSE_DATE("%Y%m%d", Arrival_Date)
        END) visit_end_date,
        MAX(CASE WHEN PARSE_DATE("%Y%m%d", AandE_Departure_Date) IS NOT NULL THEN PARSE_DATETIME('%Y%m%d%H:%M:%S', CONCAT(AandE_Departure_Date, AandE_Departure_Time))
            WHEN PARSE_DATE("%Y%m%d", AandE_Departure_Date) IS NULL AND cast(timestamp_diff(PARSE_DATETIME('%Y%m%d%H:%M:%S', CONCAT(Arrival_Date, Arrival_Time, ':00')) , PARSE_DATETIME('%Y%m%d%H:%M:%S', CONCAT(Arrival_Date, AandE_Departure_Time)), minute) as INT64) > 0
            THEN PARSE_DATETIME('%Y-%m-%d%H:%M:%S', CONCAT(DATE_ADD(DATE (PARSE_DATE("%Y%m%d", Arrival_Date)), INTERVAL 1 DAY),  AandE_Departure_Time))
                WHEN AandE_Departure_Date IS NULL AND CAST(timestamp_diff(PARSE_DATETIME('%Y%m%d%H:%M:%S', CONCAT(Arrival_Date, Arrival_Time, ':00')) , PARSE_DATETIME('%Y%m%d%H:%M:%S', CONCAT(Arrival_Date, AandE_Departure_Time)), minute) AS INT64) < 0
                THEN PARSE_DATETIME('%Y%m%d%H:%M:%S', CONCAT(Arrival_Date, AandE_Departure_Time)) 
        ELSE PARSE_DATETIME('%Y%m%d%H:%M:%S', CONCAT(Arrival_Date, AandE_Departure_Time)) 
        END) visit_end_datetime,
        32817 visit_type_concept_id --EHR
    FROM 
    (
        SELECT
            *,
            CONCAT(CAST(vo.AandE_Attendance_Number as STRING),'_', person_id) source_value  
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` vo
    ) vo
    JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl ON vl.source_value = vo.source_value
    WHERE vo.Arrival_Date IS NOT NULL
    AND vo.person_id IS NOT NULL
    AND vo.AandE_Attendance_Number IS NOT NULL
    AND vo.AandE_Arrival_Mode IS NOT NULL
    AND NOT EXISTS (SELECT * FROM `CY_IMOSPHERE_CDM_531.visit_occurrence` ovo WHERE ovo.visit_occurrence_id = vl.id)
    GROUP BY vl.id, person_id, AandE_Arrival_Mode;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id,  related_table, related_count, logged_at) 
    SELECT 'Added visit_occurrence', run_id, 'SUS_BRI_AE_010415_to_300619 -> visit_occurrence', @@row_count, CURRENT_DATETIME();
END;

CREATE OR REPLACE PROCEDURE CY_IMOSPHERE_WORKSPACE.TRANSFER_OBSERVATION_PERIOD(run_id INT64)
OPTIONS (strict_mode=false)
BEGIN
    DECLARE max_current_id INT64;

    SET max_current_id = COALESCE((SELECT observation_period_id FROM `CY_IMOSPHERE_CDM_531.observation_period` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.observation_period`
    (
        observation_period_id,
        person_id,
        observation_period_start_date,
        observation_period_end_date,
        period_type_concept_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as observation_period_id,
        op.person_id,
        op.observation_period_start_date,
        op.observation_period_end_date,
        op.period_type_concept_id
    FROM
    (
        SELECT DISTINCT
            person_id,
            MIN(visit_start_date) observation_period_start_date,
            MAX(visit_end_date) observation_period_end_date,
            32817 period_type_concept_id --EHR
        FROM `CY_IMOSPHERE_CDM_531.visit_occurrence` op
        GROUP BY person_id
    ) op
    LEFT JOIN `CY_IMOSPHERE_CDM_531.observation_period` oop ON oop.person_id = op.person_id
    WHERE oop.observation_period_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added observation period', run_id, 'visit_occurrence -> observation_period', @@row_count, CURRENT_DATETIME();

    SET max_current_id = COALESCE((SELECT observation_period_id FROM `CY_IMOSPHERE_CDM_531.observation_period` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.observation_period`
    (
        observation_period_id,
        person_id,
        observation_period_start_date,
        observation_period_end_date,
        period_type_concept_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as observation_period_id,
        op.person_id,
        op.observation_period_start_date,
        op.observation_period_end_date,
        op.period_type_concept_id
    FROM
    (
        SELECT DISTINCT
            person_id,
            MIN(EXTRACT(DATE FROM DateEvent)) observation_period_start_date,
            MAX(EXTRACT(DATE FROM DateEvent)) observation_period_end_date,
            32817 period_type_concept_id --EHR
        FROM `CY_IMOSPHERE_WORKSPACE.tbl_SRCode` op
        GROUP BY person_id
    ) op
    LEFT JOIN `CY_IMOSPHERE_CDM_531.observation_period` oop ON oop.person_id = op.person_id
    WHERE oop.observation_period_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added observation period', run_id, 'tbl_SRCode -> observation_period', @@row_count, CURRENT_DATETIME();
END;

    
CREATE OR REPLACE PROCEDURE CY_IMOSPHERE_WORKSPACE.TRANSFER_VISIT_DETAIL(run_id INT64)
OPTIONS (strict_mode=false)
BEGIN 
    /*
        Build mapping tables. These are required because there's no universal unique source id.
    */

    DECLARE max_current_id INT64;
    CREATE TABLE IF NOT EXISTS CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups(id INT64, source_value STRING);

    SET max_current_id = COALESCE((SELECT id FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups` ORDER BY 1 DESC LIMIT 1), 0);
    
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups` (id, source_value)
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() id,
        vd.source_value
    FROM
    (
        SELECT DISTINCT
            vd.source_value
        FROM
        (
            SELECT DISTINCT
                CONCAT(CAST(vd.Hospital_Provider_Spell_Number as STRING),'_', vd.person_id, '_', vd.Episode_Number) source_value          
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1` vd
            WHERE vd.Hospital_Provider_Spell_Number IS NOT NULL
            AND vd.person_id IS NOT NULL
            AND vd.Episode_Number IS NOT NULL
            AND (Start_Date_Consultant_Episode IS NOT NULL AND Start_Date_Consultant_Episode != '20')
            UNION DISTINCT
            SELECT DISTINCT
                CONCAT(CAST(vd.Hospital_Provider_Spell_Number as STRING),'_', vd.person_id, '_', vd.Episode_Number) source_value 
            FROM `CY_IMOSPHERE_WORKSPACE.src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2` vd
            WHERE vd.Hospital_Provider_Spell_Number IS NOT NULL
            AND vd.person_id IS NOT NULL
            AND vd.Episode_Number IS NOT NULL
            AND (Start_Date_Consultant_Episode IS NOT NULL AND Start_Date_Consultant_Episode != '20')
        ) vd
    ) vd
    WHERE NOT EXISTS (SELECT * FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups` l WHERE l.source_value = vd.source_value);
        
    /*
        --[SUS_BRI_APC_010415_to_300619_P1], [src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2] Transfer
    */

    INSERT INTO `CY_IMOSPHERE_CDM_531.visit_detail`
    (
        visit_detail_id,
        person_id,
        visit_detail_concept_id,
        visit_detail_start_date,
        visit_detail_start_datetime,
        visit_detail_end_date,
        visit_detail_end_datetime,
        visit_detail_type_concept_id,
        provider_id,
        visit_occurrence_id
    )
    SELECT
        vdl.id visit_detail_id,
        person_id,
        9201 visit_detail_concept_id, --Inpatient Visit
        MIN(PARSE_DATE("%Y%m%d", Start_Date_Consultant_Episode)) visit_detail_start_date,
        MIN(PARSE_DATETIME('%Y%m%d%H:%M:%S', CONCAT(Start_Date_Consultant_Episode, Start_Time_Episode))) visit_detail_start_datetime,
        MAX(PARSE_DATE("%Y%m%d", End_Date_Consultant_Episode)) visit_detail_end_date,
        MAX(PARSE_DATETIME('%Y%m%d%H:%M:%S', CONCAT(End_Date_Consultant_Episode, End_Time_Episode))) visit_detail_end_datetime,
        32817 visit_detail_type_concept_id, --EHR
        array_agg(struct(provider_id) ignore nulls order by Start_Date_Consultant_Episode asc limit 1)[offset(0)].provider_id provider_id,
        vl.id visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            Consultant_Code,
            Start_Date_Consultant_Episode,
            person_id,
            Start_Time_Episode,
            End_Date_Consultant_Episode,
            End_Time_Episode,
            CONCAT(CAST(vd.Hospital_Provider_Spell_Number as STRING),'_', person_id) visit_occurrence_source_value,
            CONCAT(CAST(vd.Hospital_Provider_Spell_Number as STRING),'_', vd.person_id, '_', vd.Episode_Number) source_value 
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1` vd
        UNION DISTINCT
        SELECT DISTINCT
            Consultant_Code,
            Start_Date_Consultant_Episode,
            person_id,
            Start_Time_Episode,
            End_Date_Consultant_Episode,
            End_Time_Episode,
            CONCAT(CAST(vd.Hospital_Provider_Spell_Number as STRING),'_', person_id) visit_occurrence_source_value,
            CONCAT(CAST(vd.Hospital_Provider_Spell_Number as STRING),'_', vd.person_id, '_', vd.Episode_Number) source_value 
        FROM `CY_IMOSPHERE_WORKSPACE.src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2` vd
    ) vd
    JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = vd.visit_occurrence_source_value
    JOIN `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups` vdl ON vdl.source_value = vd.source_value
    LEFT JOIN `CY_IMOSPHERE_CDM_531.provider` p ON p.provider_source_value = vd.Consultant_Code 
    WHERE (Start_Date_Consultant_Episode IS NOT NULL AND Start_Date_Consultant_Episode != '20')
    AND vd.person_id IS NOT NULL
    AND NOT EXISTS (SELECT * FROM `CY_IMOSPHERE_CDM_531.visit_detail` ovd WHERE ovd.visit_detail_id = vdl.id)
    GROUP BY vdl.id, person_id, vl.id;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id,  related_table, related_count, logged_at) 
    SELECT 'Added visit_detail', run_id, 'SUS_BRI_APC_010415_to_300619_P1, src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2 -> visit_detail', @@row_count, CURRENT_DATETIME();
END;

CREATE OR REPLACE PROCEDURE CY_IMOSPHERE_WORKSPACE.TRANSFER_CONDITION_OCCURRENCE(run_id INT64)
OPTIONS (strict_mode=false)
BEGIN
    DECLARE max_current_id INT64;
    SET max_current_id = COALESCE((SELECT condition_occurrence_id FROM `CY_IMOSPHERE_CDM_531.condition_occurrence` ORDER BY 1 DESC LIMIT 1), 0);

    /*
        --[tbl_SRPatient] Transfer
    */

    INSERT INTO `CY_IMOSPHERE_CDM_531.condition_occurrence`
    (
        condition_occurrence_id,
        person_id,
        condition_concept_id,
        condition_source_value,
        condition_start_date,
        condition_start_datetime,
        condition_type_concept_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as condition_occurrence_id,
        c.person_id,
        c.condition_concept_id,
        c.condition_source_value,
        c.condition_start_date,
        c.condition_start_datetime,
        c.condition_type_concept_id
    FROM
    (
        SELECT DISTINCT
            c.person_id person_id,
            46270485 condition_concept_id, --Indeterminate sex.
            c.Gender condition_source_value, 
            CAST(PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(c.DateBirth ,'01000000')) AS DATE) condition_start_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(c.DateBirth ,'01000000')) condition_start_datetime,
            32817 condition_type_concept_id --EHR
        FROM `CY_IMOSPHERE_WORKSPACE.tbl_SRPatient` c
        WHERE c.DateBirth IS NOT NULL
        AND c.person_id IS NOT NULL
        AND c.Gender = 'I'
    ) c
    LEFT JOIN `CY_IMOSPHERE_CDM_531.condition_occurrence` oc ON 
        oc.person_id = c.person_id 
        AND oc.condition_concept_id = c.condition_concept_id
        AND oc.condition_source_value = c.condition_source_value
        AND COALESCE(oc.condition_source_concept_id, -1) = -1
        AND oc.condition_start_datetime = c.condition_start_datetime
        AND oc.condition_type_concept_id = c.condition_type_concept_id
        AND COALESCE(oc.provider_id, -1) = -1
        AND COALESCE(oc.condition_status_concept_id, -1) = -1
        AND COALESCE(oc.visit_occurrence_id, -1) = -1
        AND COALESCE(oc.visit_detail_id, -1) = -1
    WHERE oc.condition_occurrence_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id,  related_table, related_count, logged_at) 
    SELECT 'Added condition_occurrence', run_id,'tbl_SRPatient -> condition_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[tbl_SRCode] Transfer
    */

    SET max_current_id = COALESCE((SELECT condition_occurrence_id FROM `CY_IMOSPHERE_CDM_531.condition_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.condition_occurrence`
    (
        condition_occurrence_id,
        person_id,
        condition_concept_id,
        condition_source_value,
        condition_source_concept_id,
        condition_start_date,
        condition_start_datetime,
        condition_type_concept_id,
        provider_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as condition_occurrence_id,
        c.person_id,
        c.condition_concept_id,
        c.condition_source_value,
        c.condition_source_concept_id,
        c.condition_start_date,
        c.condition_start_datetime,
        c.condition_type_concept_id,
        c.provider_id
    FROM
    (
        SELECT DISTINCT
            c.person_id person_id,
            dc.concept_id condition_concept_id, 
            c.ctv3code condition_source_value, 
            sc.concept_id condition_source_concept_id,
            CAST(dateevent AS DATE) condition_start_date,
            dateevent condition_start_datetime,
            32817 condition_type_concept_id, --EHR
            p.provider_id provider_id
        FROM `CY_IMOSPHERE_WORKSPACE.tbl_SRCode` c
        JOIN `CY_LOOKUPS.tbl_CTV3ToSnomed_Map` sm ON sm.ctv3code = c.ctv3code
        JOIN `CY_CDM_VOCAB.concept` sc ON sc.concept_code = sm.SNOMEDCode
        JOIN `CY_CDM_VOCAB.concept_relationship` cr ON cr.concept_id_1 = sc.concept_id
        JOIN `CY_CDM_VOCAB.concept` dc ON dc.concept_id = cr.concept_id_2
        LEFT JOIN `CY_IMOSPHERE_CDM_531.provider` p ON p.provider_id = CAST(iddoneby AS INT64)
        WHERE sc.vocabulary_id = 'SNOMED'
        AND cr.relationship_id = 'Maps to'
        AND dc.standard_concept = 'S'
        AND dc.domain_id = 'Condition'
        AND c.dateevent IS NOT NULL
        AND c.person_id IS NOT NULL
    ) c
    LEFT JOIN `CY_IMOSPHERE_CDM_531.condition_occurrence` oc ON 
        oc.person_id = c.person_id
        AND oc.condition_concept_id = c.condition_concept_id
        AND oc.condition_source_value = c.condition_source_value
        AND COALESCE(oc.condition_source_concept_id, -1) = COALESCE(c.condition_source_concept_id, -1)
        AND oc.condition_start_datetime = c.condition_start_datetime
        AND oc.condition_type_concept_id = c.condition_type_concept_id
        AND COALESCE(oc.provider_id, -1) = COALESCE(c.provider_id, -1)
        AND COALESCE(oc.condition_status_concept_id, -1) = -1
        AND COALESCE(oc.visit_occurrence_id, -1) = -1
        AND COALESCE(oc.visit_detail_id, -1) = -1
    WHERE oc.condition_occurrence_id IS NULL;


    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id,  related_table, related_count, logged_at) 
    SELECT 'Added condition_occurrence', run_id, 'tbl_SRCode -> condition_occurrence', @@row_count, CURRENT_DATETIME();

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    ------------[SUS_BRI_APC_010415_to_300619_P1] AND [src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2]
    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

    /*
        --[Diagnosis_Primary_ICD] Transfer
    */

    SET max_current_id = COALESCE((SELECT condition_occurrence_id FROM `CY_IMOSPHERE_CDM_531.condition_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.condition_occurrence`
    (
        condition_occurrence_id,
        person_id,
        condition_concept_id,
        condition_source_value,
        condition_source_concept_id,
        condition_start_date,
        condition_start_datetime,
        condition_type_concept_id,
        condition_status_concept_id,
        visit_occurrence_id,
        visit_detail_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() condition_occurrence_id,
        c.person_id,
        c.condition_concept_id,
        c.condition_source_value,
        c.condition_source_concept_id,
        c.condition_start_date,
        c.condition_start_datetime,
        c.condition_type_concept_id,
        c.condition_status_concept_id,
        c.visit_occurrence_id,
        c.visit_detail_id
    FROM
    (
        SELECT DISTINCT
            person_id,
            dc.concept_id condition_concept_id,
            Diagnosis_Primary_ICD condition_source_value,
            sc.concept_id  condition_source_concept_id,
            PARSE_DATE('%Y%m%d', Start_Date_Consultant_Episode) condition_start_date,
            CAST(PARSE_DATE('%Y%m%d', Start_Date_Consultant_Episode) AS DATETIME) condition_start_datetime,
            32817 condition_type_concept_id, --EHR
            32902 condition_status_concept_id, --Primary diagnosis
            vl.id visit_occurrence_id,
            vdl.id visit_detail_id
        FROM
        (    
            SELECT DISTINCT
                person_id,
                Start_Date_Consultant_Episode,
                Diagnosis_Primary_ICD,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id) visit_occurrence_source_value,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id, '_', Episode_Number) visit_detail_source_value
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1`
            UNION DISTINCT
            SELECT DISTINCT
                person_id,
                Start_Date_Consultant_Episode,
                Diagnosis_Primary_ICD,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id) visit_occurrence_source_value,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id, '_', Episode_Number) visit_detail_source_value
            FROM `CY_IMOSPHERE_WORKSPACE.src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2`
        ) c
        LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups` vdl ON vdl.source_value = visit_detail_source_value
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = c.visit_occurrence_source_value
        JOIN `CY_CDM_VOCAB.concept` sc on REPLACE(sc.concept_code, '.', '') = c.Diagnosis_Primary_ICD
        JOIN `CY_CDM_VOCAB.concept_relationship` cr on cr.concept_id_1 = sc.concept_id
        JOIN `CY_CDM_VOCAB.concept` dc on dc.concept_id = cr.concept_id_2
        WHERE c.Start_Date_Consultant_Episode IS NOT NULL
        AND c.person_id IS NOT NULL
        AND cr.relationship_id = 'Maps to'
        AND dc.standard_concept = 'S'
        AND dc.domain_id = 'Condition'
        AND sc.vocabulary_id = 'ICD10'
        AND c.Diagnosis_Primary_ICD IS NOT NULL
        AND dc.vocabulary_id = 'SNOMED'
    ) c
    LEFT JOIN `CY_IMOSPHERE_CDM_531.condition_occurrence` oc ON 
        oc.person_id = c.person_id 
        AND oc.condition_concept_id = c.condition_concept_id
        AND oc.condition_source_value = c.condition_source_value
        AND COALESCE(oc.condition_source_concept_id, -1) = c.condition_source_concept_id
        AND oc.condition_start_datetime = c.condition_start_datetime
        AND oc.condition_type_concept_id = c.condition_type_concept_id
        AND COALESCE(oc.provider_id, -1) = -1
        AND COALESCE(oc.condition_status_concept_id, -1) = COALESCE(c.condition_status_concept_id, -1) 
        AND COALESCE(oc.visit_occurrence_id, -1) = c.visit_occurrence_id
        AND COALESCE(oc.visit_detail_id, -1) = c.visit_detail_id
    WHERE oc.condition_occurrence_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id,  related_table, related_count, logged_at) 
    SELECT 'Added condition_occurrence', run_id, 'src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2, SUS_BRI_APC_010415_to_300619_P1  - Diagnosis_Primary_ICD -> condition_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[Diagnosis_1st_Secondary_ICD] Transfer
    */

    SET max_current_id = COALESCE((SELECT condition_occurrence_id FROM `CY_IMOSPHERE_CDM_531.condition_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.condition_occurrence`
    (
        condition_occurrence_id,
        person_id,
        condition_concept_id,
        condition_source_value,
        condition_source_concept_id,
        condition_start_date,
        condition_start_datetime,
        condition_type_concept_id,
        condition_status_concept_id,
        visit_occurrence_id,
        visit_detail_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() condition_occurrence_id,
        c.person_id,
        c.condition_concept_id,
        c.condition_source_value,
        c.condition_source_concept_id,
        c.condition_start_date,
        c.condition_start_datetime,
        c.condition_type_concept_id,
        c.condition_status_concept_id,
        c.visit_occurrence_id,
        c.visit_detail_id
    FROM
    (
        SELECT DISTINCT
            person_id,
            dc.concept_id condition_concept_id,
            Diagnosis_1st_Secondary_ICD condition_source_value,
            sc.concept_id  condition_source_concept_id,
            PARSE_DATE('%Y%m%d', Start_Date_Consultant_Episode) condition_start_date,
            CAST(PARSE_DATE('%Y%m%d', Start_Date_Consultant_Episode) AS DATETIME) condition_start_datetime,
            32817 condition_type_concept_id, --EHR
            32908 condition_status_concept_id, --Secondary diagnosis
            vl.id visit_occurrence_id,
            vdl.id visit_detail_id
        FROM
        (    
            SELECT DISTINCT
                person_id,
                Start_Date_Consultant_Episode,
                Diagnosis_1st_Secondary_ICD,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id) visit_occurrence_source_value,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id, '_', Episode_Number) visit_detail_source_value
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1`
            UNION DISTINCT
            SELECT DISTINCT
                person_id,
                Start_Date_Consultant_Episode,
                Diagnosis_1st_Secondary_ICD,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id) visit_occurrence_source_value,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id, '_', Episode_Number) visit_detail_source_value
            FROM `CY_IMOSPHERE_WORKSPACE.src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2`
        ) c
        LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups` vdl ON vdl.source_value = visit_detail_source_value
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = c.visit_occurrence_source_value
        JOIN `CY_CDM_VOCAB.concept` sc on REPLACE(sc.concept_code, '.', '') = c.Diagnosis_1st_Secondary_ICD
        JOIN `CY_CDM_VOCAB.concept_relationship` cr on cr.concept_id_1 = sc.concept_id
        JOIN `CY_CDM_VOCAB.concept` dc on dc.concept_id = cr.concept_id_2
        WHERE c.Start_Date_Consultant_Episode IS NOT NULL
        AND c.person_id IS NOT NULL
        AND cr.relationship_id = 'Maps to'
        AND dc.standard_concept = 'S'
        AND dc.domain_id = 'Condition'
        AND sc.vocabulary_id = 'ICD10'
        AND c.Diagnosis_1st_Secondary_ICD IS NOT NULL
        AND dc.vocabulary_id = 'SNOMED'
    ) c
    LEFT JOIN `CY_IMOSPHERE_CDM_531.condition_occurrence` oc ON 
        oc.person_id = c.person_id 
        AND oc.condition_concept_id = c.condition_concept_id
        AND oc.condition_source_value = c.condition_source_value
        AND COALESCE(oc.condition_source_concept_id, -1) = c.condition_source_concept_id
        AND oc.condition_start_datetime = c.condition_start_datetime
        AND oc.condition_type_concept_id = c.condition_type_concept_id
        AND COALESCE(oc.provider_id, -1) = -1
        AND COALESCE(oc.condition_status_concept_id, -1) = COALESCE(c.condition_status_concept_id, -1)
        AND COALESCE(oc.visit_occurrence_id, -1) = COALESCE(c.visit_occurrence_id, -1)
        AND COALESCE(oc.visit_occurrence_id, -1) = c.visit_occurrence_id
        AND COALESCE(oc.visit_detail_id, -1) = c.visit_detail_id
    WHERE oc.condition_occurrence_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id,  related_table, related_count, logged_at) 
    SELECT 'Added condition_occurrence', run_id, 'src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2, SUS_BRI_APC_010415_to_300619_P1  - Diagnosis_1st_Secondary_ICD -> condition_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[Diagnosis_2nd_Secondary_ICD] Transfer
    */

    SET max_current_id = COALESCE((SELECT condition_occurrence_id FROM `CY_IMOSPHERE_CDM_531.condition_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.condition_occurrence`
    (
        condition_occurrence_id,
        person_id,
        condition_concept_id,
        condition_source_value,
        condition_source_concept_id,
        condition_start_date,
        condition_start_datetime,
        condition_type_concept_id,
        condition_status_concept_id,
        visit_occurrence_id,
        visit_detail_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() condition_occurrence_id,
        c.person_id,
        c.condition_concept_id,
        c.condition_source_value,
        c.condition_source_concept_id,
        c.condition_start_date,
        c.condition_start_datetime,
        c.condition_type_concept_id,
        c.condition_status_concept_id,
        c.visit_occurrence_id,
        c.visit_detail_id
    FROM
    (
        SELECT DISTINCT
            person_id,
            dc.concept_id condition_concept_id,
            Diagnosis_2nd_Secondary_ICD condition_source_value,
            sc.concept_id  condition_source_concept_id,
            PARSE_DATE('%Y%m%d', Start_Date_Consultant_Episode) condition_start_date,
            CAST(PARSE_DATE('%Y%m%d', Start_Date_Consultant_Episode) AS DATETIME) condition_start_datetime,
            32817 condition_type_concept_id, --EHR
            32908 condition_status_concept_id, --Secondary diagnosis
            vl.id visit_occurrence_id,
            vdl.id visit_detail_id
        FROM
        (    
            SELECT DISTINCT
                person_id,
                Start_Date_Consultant_Episode,
                Diagnosis_2nd_Secondary_ICD,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id) visit_occurrence_source_value,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id, '_', Episode_Number) visit_detail_source_value
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1`
            UNION DISTINCT
            SELECT DISTINCT
                person_id,
                Start_Date_Consultant_Episode,
                Diagnosis_2nd_Secondary_ICD,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id) visit_occurrence_source_value,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id, '_', Episode_Number) visit_detail_source_value
            FROM `CY_IMOSPHERE_WORKSPACE.src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2`
        ) c
        LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups` vdl ON vdl.source_value = visit_detail_source_value
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = c.visit_occurrence_source_value
        JOIN `CY_CDM_VOCAB.concept` sc on REPLACE(sc.concept_code, '.', '') = c.Diagnosis_2nd_Secondary_ICD
        JOIN `CY_CDM_VOCAB.concept_relationship` cr on cr.concept_id_1 = sc.concept_id
        JOIN `CY_CDM_VOCAB.concept` dc on dc.concept_id = cr.concept_id_2
        WHERE c.Start_Date_Consultant_Episode IS NOT NULL
        AND c.person_id IS NOT NULL
        AND cr.relationship_id = 'Maps to'
        AND dc.standard_concept = 'S'
        AND dc.domain_id = 'Condition'
        AND sc.vocabulary_id = 'ICD10'
        AND c.Diagnosis_2nd_Secondary_ICD IS NOT NULL
        AND dc.vocabulary_id = 'SNOMED'
    ) c
    LEFT JOIN `CY_IMOSPHERE_CDM_531.condition_occurrence` oc ON 
        oc.person_id = c.person_id 
        AND oc.condition_concept_id = c.condition_concept_id
        AND oc.condition_source_value = c.condition_source_value
        AND COALESCE(oc.condition_source_concept_id, -1) = c.condition_source_concept_id
        AND oc.condition_start_datetime = c.condition_start_datetime
        AND oc.condition_type_concept_id = c.condition_type_concept_id
        AND COALESCE(oc.provider_id, -1) = -1
        AND COALESCE(oc.condition_status_concept_id, -1) = COALESCE(c.condition_status_concept_id, -1)
        AND COALESCE(oc.visit_occurrence_id, -1) = COALESCE(c.visit_occurrence_id, -1)
        AND COALESCE(oc.visit_occurrence_id, -1) = c.visit_occurrence_id
        AND COALESCE(oc.visit_detail_id, -1) = c.visit_detail_id
    WHERE oc.condition_occurrence_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id,  related_table, related_count, logged_at) 
    SELECT 'Added condition_occurrence', run_id, 'src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2, SUS_BRI_APC_010415_to_300619_P1  - Diagnosis_2nd_Secondary_ICD -> condition_occurrence', @@row_count, CURRENT_DATETIME();
     
    /*
        --[Diagnosis_3rd_Secondary_ICD] Transfer
    */

    SET max_current_id = COALESCE((SELECT condition_occurrence_id FROM `CY_IMOSPHERE_CDM_531.condition_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.condition_occurrence`
    (
        condition_occurrence_id,
        person_id,
        condition_concept_id,
        condition_source_value,
        condition_source_concept_id,
        condition_start_date,
        condition_start_datetime,
        condition_type_concept_id,
        condition_status_concept_id,
        visit_occurrence_id,
        visit_detail_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() condition_occurrence_id,
        c.person_id,
        c.condition_concept_id,
        c.condition_source_value,
        c.condition_source_concept_id,
        c.condition_start_date,
        c.condition_start_datetime,
        c.condition_type_concept_id,
        c.condition_status_concept_id,
        c.visit_occurrence_id,
        c.visit_detail_id
    FROM
    (
        SELECT DISTINCT
            person_id,
            dc.concept_id condition_concept_id,
            Diagnosis_3rd_Secondary_ICD condition_source_value,
            sc.concept_id  condition_source_concept_id,
            PARSE_DATE('%Y%m%d', Start_Date_Consultant_Episode) condition_start_date,
            CAST(PARSE_DATE('%Y%m%d', Start_Date_Consultant_Episode) AS DATETIME) condition_start_datetime,
            32817 condition_type_concept_id, --EHR
            32908 condition_status_concept_id, --Secondary diagnosis
            vl.id visit_occurrence_id,
            vdl.id visit_detail_id
        FROM
        (    
            SELECT DISTINCT
                person_id,
                Start_Date_Consultant_Episode,
                Diagnosis_3rd_Secondary_ICD,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id) visit_occurrence_source_value,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id, '_', Episode_Number) visit_detail_source_value
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1`
            UNION DISTINCT
            SELECT DISTINCT
                person_id,
                Start_Date_Consultant_Episode,
                Diagnosis_3rd_Secondary_ICD,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id) visit_occurrence_source_value,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id, '_', Episode_Number) visit_detail_source_value
            FROM `CY_IMOSPHERE_WORKSPACE.src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2`
        ) c
        LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups` vdl ON vdl.source_value = visit_detail_source_value
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = c.visit_occurrence_source_value
        JOIN `CY_CDM_VOCAB.concept` sc on REPLACE(sc.concept_code, '.', '') = c.Diagnosis_3rd_Secondary_ICD
        JOIN `CY_CDM_VOCAB.concept_relationship` cr on cr.concept_id_1 = sc.concept_id
        JOIN `CY_CDM_VOCAB.concept` dc on dc.concept_id = cr.concept_id_2
        WHERE c.Start_Date_Consultant_Episode IS NOT NULL
        AND c.person_id IS NOT NULL
        AND cr.relationship_id = 'Maps to'
        AND dc.standard_concept = 'S'
        AND dc.domain_id = 'Condition'
        AND sc.vocabulary_id = 'ICD10'
        AND c.Diagnosis_3rd_Secondary_ICD IS NOT NULL
        AND dc.vocabulary_id = 'SNOMED'
    ) c
    LEFT JOIN `CY_IMOSPHERE_CDM_531.condition_occurrence` oc ON 
        oc.person_id = c.person_id 
        AND oc.condition_concept_id = c.condition_concept_id
        AND oc.condition_source_value = c.condition_source_value
        AND COALESCE(oc.condition_source_concept_id, -1) = c.condition_source_concept_id
        AND oc.condition_start_datetime = c.condition_start_datetime
        AND oc.condition_type_concept_id = c.condition_type_concept_id
        AND COALESCE(oc.provider_id, -1) = -1
        AND COALESCE(oc.condition_status_concept_id, -1) = COALESCE(c.condition_status_concept_id, -1)
        AND COALESCE(oc.visit_occurrence_id, -1) = COALESCE(c.visit_occurrence_id, -1)
        AND COALESCE(oc.visit_occurrence_id, -1) = c.visit_occurrence_id
        AND COALESCE(oc.visit_detail_id, -1) = c.visit_detail_id
    WHERE oc.condition_occurrence_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id,  related_table, related_count, logged_at) 
    SELECT 'Added condition_occurrence', run_id, 'src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2, SUS_BRI_APC_010415_to_300619_P1  - Diagnosis_3rd_Secondary_ICD -> condition_occurrence', @@row_count, CURRENT_DATETIME();
     
    /*
        --[Diagnosis_4th_Secondary_ICD] Transfer
    */
    SET max_current_id = COALESCE((SELECT condition_occurrence_id FROM `CY_IMOSPHERE_CDM_531.condition_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.condition_occurrence`
    (
        condition_occurrence_id,
        person_id,
        condition_concept_id,
        condition_source_value,
        condition_source_concept_id,
        condition_start_date,
        condition_start_datetime,
        condition_type_concept_id,
        condition_status_concept_id,
        visit_occurrence_id,
        visit_detail_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() condition_occurrence_id,
        c.person_id,
        c.condition_concept_id,
        c.condition_source_value,
        c.condition_source_concept_id,
        c.condition_start_date,
        c.condition_start_datetime,
        c.condition_type_concept_id,
        c.condition_status_concept_id,
        c.visit_occurrence_id,
        c.visit_detail_id
    FROM
    (
        SELECT DISTINCT
            person_id,
            dc.concept_id condition_concept_id,
            Diagnosis_4th_Secondary_ICD condition_source_value,
            sc.concept_id  condition_source_concept_id,
            PARSE_DATE('%Y%m%d', Start_Date_Consultant_Episode) condition_start_date,
            CAST(PARSE_DATE('%Y%m%d', Start_Date_Consultant_Episode) AS DATETIME) condition_start_datetime,
            32817 condition_type_concept_id, --EHR
            32908 condition_status_concept_id, --Secondary diagnosis
            vl.id visit_occurrence_id,
            vdl.id visit_detail_id
        FROM
        (    
            SELECT DISTINCT
                person_id,
                Start_Date_Consultant_Episode,
                Diagnosis_4th_Secondary_ICD,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id) visit_occurrence_source_value,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id, '_', Episode_Number) visit_detail_source_value
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1`
            UNION DISTINCT
            SELECT DISTINCT
                person_id,
                Start_Date_Consultant_Episode,
                Diagnosis_4th_Secondary_ICD,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id) visit_occurrence_source_value,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id, '_', Episode_Number) visit_detail_source_value
            FROM `CY_IMOSPHERE_WORKSPACE.src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2`
        ) c
        LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups` vdl ON vdl.source_value = visit_detail_source_value
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = c.visit_occurrence_source_value
        JOIN `CY_CDM_VOCAB.concept` sc on REPLACE(sc.concept_code, '.', '') = c.Diagnosis_4th_Secondary_ICD
        JOIN `CY_CDM_VOCAB.concept_relationship` cr on cr.concept_id_1 = sc.concept_id
        JOIN `CY_CDM_VOCAB.concept` dc on dc.concept_id = cr.concept_id_2
        WHERE c.Start_Date_Consultant_Episode IS NOT NULL
        AND c.person_id IS NOT NULL
        AND cr.relationship_id = 'Maps to'
        AND dc.standard_concept = 'S'
        AND dc.domain_id = 'Condition'
        AND sc.vocabulary_id = 'ICD10'
        AND c.Diagnosis_4th_Secondary_ICD IS NOT NULL
        AND dc.vocabulary_id = 'SNOMED'
    ) c
    LEFT JOIN `CY_IMOSPHERE_CDM_531.condition_occurrence` oc ON 
        oc.person_id = c.person_id 
        AND oc.condition_concept_id = c.condition_concept_id
        AND oc.condition_source_value = c.condition_source_value
        AND COALESCE(oc.condition_source_concept_id, -1) = c.condition_source_concept_id
        AND oc.condition_start_datetime = c.condition_start_datetime
        AND oc.condition_type_concept_id = c.condition_type_concept_id
        AND COALESCE(oc.provider_id, -1) = -1
        AND COALESCE(oc.condition_status_concept_id, -1) = COALESCE(c.condition_status_concept_id, -1)
        AND COALESCE(oc.visit_occurrence_id, -1) = COALESCE(c.visit_occurrence_id, -1)
        AND COALESCE(oc.visit_occurrence_id, -1) = c.visit_occurrence_id
        AND COALESCE(oc.visit_detail_id, -1) = c.visit_detail_id
    WHERE oc.condition_occurrence_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id,  related_table, related_count, logged_at) 
    SELECT 'Added condition_occurrence', run_id, 'src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2, SUS_BRI_APC_010415_to_300619_P1  - Diagnosis_4th_Secondary_ICD -> condition_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[Diagnosis_5th_Secondary_ICD] Transfer
    */
    SET max_current_id = COALESCE((SELECT condition_occurrence_id FROM `CY_IMOSPHERE_CDM_531.condition_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.condition_occurrence`
    (
        condition_occurrence_id,
        person_id,
        condition_concept_id,
        condition_source_value,
        condition_source_concept_id,
        condition_start_date,
        condition_start_datetime,
        condition_type_concept_id,
        condition_status_concept_id,
        visit_occurrence_id,
        visit_detail_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() condition_occurrence_id,
        c.person_id,
        c.condition_concept_id,
        c.condition_source_value,
        c.condition_source_concept_id,
        c.condition_start_date,
        c.condition_start_datetime,
        c.condition_type_concept_id,
        c.condition_status_concept_id,
        c.visit_occurrence_id,
        c.visit_detail_id
    FROM
    (
        SELECT DISTINCT
            person_id,
            dc.concept_id condition_concept_id,
            Diagnosis_5th_Secondary_ICD condition_source_value,
            sc.concept_id  condition_source_concept_id,
            PARSE_DATE('%Y%m%d', Start_Date_Consultant_Episode) condition_start_date,
            CAST(PARSE_DATE('%Y%m%d', Start_Date_Consultant_Episode) AS DATETIME) condition_start_datetime,
            32817 condition_type_concept_id, --EHR
            32908 condition_status_concept_id, --Secondary diagnosis
            vl.id visit_occurrence_id,
            vdl.id visit_detail_id
        FROM
        (    
            SELECT DISTINCT
                person_id,
                Start_Date_Consultant_Episode,
                Diagnosis_5th_Secondary_ICD,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id) visit_occurrence_source_value,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id, '_', Episode_Number) visit_detail_source_value
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1`
            UNION DISTINCT
            SELECT DISTINCT
                person_id,
                Start_Date_Consultant_Episode,
                Diagnosis_5th_Secondary_ICD,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id) visit_occurrence_source_value,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id, '_', Episode_Number) visit_detail_source_value
            FROM `CY_IMOSPHERE_WORKSPACE.src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2`
        ) c
        LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups` vdl ON vdl.source_value = visit_detail_source_value
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = c.visit_occurrence_source_value
        JOIN `CY_CDM_VOCAB.concept` sc on REPLACE(sc.concept_code, '.', '') = c.Diagnosis_5th_Secondary_ICD
        JOIN `CY_CDM_VOCAB.concept_relationship` cr on cr.concept_id_1 = sc.concept_id
        JOIN `CY_CDM_VOCAB.concept` dc on dc.concept_id = cr.concept_id_2
        WHERE c.Start_Date_Consultant_Episode IS NOT NULL
        AND c.person_id IS NOT NULL
        AND cr.relationship_id = 'Maps to'
        AND dc.standard_concept = 'S'
        AND dc.domain_id = 'Condition'
        AND sc.vocabulary_id = 'ICD10'
        AND c.Diagnosis_5th_Secondary_ICD IS NOT NULL
        AND dc.vocabulary_id = 'SNOMED'
    ) c
    LEFT JOIN `CY_IMOSPHERE_CDM_531.condition_occurrence` oc ON 
        oc.person_id = c.person_id 
        AND oc.condition_concept_id = c.condition_concept_id
        AND oc.condition_source_value = c.condition_source_value
        AND COALESCE(oc.condition_source_concept_id, -1) = c.condition_source_concept_id
        AND oc.condition_start_datetime = c.condition_start_datetime
        AND oc.condition_type_concept_id = c.condition_type_concept_id
        AND COALESCE(oc.provider_id, -1) = -1
        AND COALESCE(oc.condition_status_concept_id, -1) = COALESCE(c.condition_status_concept_id, -1)
        AND COALESCE(oc.visit_occurrence_id, -1) = COALESCE(c.visit_occurrence_id, -1)
        AND COALESCE(oc.visit_occurrence_id, -1) = c.visit_occurrence_id
        AND COALESCE(oc.visit_detail_id, -1) = c.visit_detail_id
    WHERE oc.condition_occurrence_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id,  related_table, related_count, logged_at) 
    SELECT 'Added condition_occurrence', run_id, 'src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2, SUS_BRI_APC_010415_to_300619_P1  - Diagnosis_5th_Secondary_ICD -> condition_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[Diagnosis_6th_Secondary_ICD] Transfer
    */
    SET max_current_id = COALESCE((SELECT condition_occurrence_id FROM `CY_IMOSPHERE_CDM_531.condition_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.condition_occurrence`
    (
        condition_occurrence_id,
        person_id,
        condition_concept_id,
        condition_source_value,
        condition_source_concept_id,
        condition_start_date,
        condition_start_datetime,
        condition_type_concept_id,
        condition_status_concept_id,
        visit_occurrence_id,
        visit_detail_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() condition_occurrence_id,
        c.person_id,
        c.condition_concept_id,
        c.condition_source_value,
        c.condition_source_concept_id,
        c.condition_start_date,
        c.condition_start_datetime,
        c.condition_type_concept_id,
        c.condition_status_concept_id,
        c.visit_occurrence_id,
        c.visit_detail_id
    FROM
    (
        SELECT DISTINCT
            person_id,
            dc.concept_id condition_concept_id,
            Diagnosis_6th_Secondary_ICD condition_source_value,
            sc.concept_id  condition_source_concept_id,
            PARSE_DATE('%Y%m%d', Start_Date_Consultant_Episode) condition_start_date,
            CAST(PARSE_DATE('%Y%m%d', Start_Date_Consultant_Episode) AS DATETIME) condition_start_datetime,
            32817 condition_type_concept_id, --EHR
            32908 condition_status_concept_id, --Secondary diagnosis
            vl.id visit_occurrence_id,
            vdl.id visit_detail_id
        FROM
        (    
            SELECT DISTINCT
                person_id,
                Start_Date_Consultant_Episode,
                Diagnosis_6th_Secondary_ICD,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id) visit_occurrence_source_value,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id, '_', Episode_Number) visit_detail_source_value
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1`
            UNION DISTINCT
            SELECT DISTINCT
                person_id,
                Start_Date_Consultant_Episode,
                Diagnosis_6th_Secondary_ICD,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id) visit_occurrence_source_value,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id, '_', Episode_Number) visit_detail_source_value
            FROM `CY_IMOSPHERE_WORKSPACE.src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2`
        ) c
        LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups` vdl ON vdl.source_value = visit_detail_source_value
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = c.visit_occurrence_source_value
        JOIN `CY_CDM_VOCAB.concept` sc on REPLACE(sc.concept_code, '.', '') = c.Diagnosis_6th_Secondary_ICD
        JOIN `CY_CDM_VOCAB.concept_relationship` cr on cr.concept_id_1 = sc.concept_id
        JOIN `CY_CDM_VOCAB.concept` dc on dc.concept_id = cr.concept_id_2
        WHERE c.Start_Date_Consultant_Episode IS NOT NULL
        AND c.person_id IS NOT NULL
        AND cr.relationship_id = 'Maps to'
        AND dc.standard_concept = 'S'
        AND dc.domain_id = 'Condition'
        AND sc.vocabulary_id = 'ICD10'
        AND c.Diagnosis_6th_Secondary_ICD IS NOT NULL
        AND dc.vocabulary_id = 'SNOMED'
    ) c
    LEFT JOIN `CY_IMOSPHERE_CDM_531.condition_occurrence` oc ON 
        oc.person_id = c.person_id 
        AND oc.condition_concept_id = c.condition_concept_id
        AND oc.condition_source_value = c.condition_source_value
        AND COALESCE(oc.condition_source_concept_id, -1) = c.condition_source_concept_id
        AND oc.condition_start_datetime = c.condition_start_datetime
        AND oc.condition_type_concept_id = c.condition_type_concept_id
        AND COALESCE(oc.provider_id, -1) = -1
        AND COALESCE(oc.condition_status_concept_id, -1) = COALESCE(c.condition_status_concept_id, -1)
        AND COALESCE(oc.visit_occurrence_id, -1) = COALESCE(c.visit_occurrence_id, -1)
        AND COALESCE(oc.visit_occurrence_id, -1) = c.visit_occurrence_id
        AND COALESCE(oc.visit_detail_id, -1) = c.visit_detail_id
    WHERE oc.condition_occurrence_id IS NULL;

   INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id,  related_table, related_count, logged_at) 
   SELECT 'Added condition_occurrence', run_id, 'src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2, SUS_BRI_APC_010415_to_300619_P1  - Diagnosis_6th_Secondary_ICD -> condition_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[Diagnosis_7th_Secondary_ICD] Transfer
    */

    SET max_current_id = COALESCE((SELECT condition_occurrence_id FROM `CY_IMOSPHERE_CDM_531.condition_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.condition_occurrence`
    (
        condition_occurrence_id,
        person_id,
        condition_concept_id,
        condition_source_value,
        condition_source_concept_id,
        condition_start_date,
        condition_start_datetime,
        condition_type_concept_id,
        condition_status_concept_id,
        visit_occurrence_id,
        visit_detail_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() condition_occurrence_id,
        c.person_id,
        c.condition_concept_id,
        c.condition_source_value,
        c.condition_source_concept_id,
        c.condition_start_date,
        c.condition_start_datetime,
        c.condition_type_concept_id,
        c.condition_status_concept_id,
        c.visit_occurrence_id,
        c.visit_detail_id
    FROM
    (
        SELECT DISTINCT
            person_id,
            dc.concept_id condition_concept_id,
            Diagnosis_7th_Secondary_ICD condition_source_value,
            sc.concept_id  condition_source_concept_id,
            PARSE_DATE('%Y%m%d', Start_Date_Consultant_Episode) condition_start_date,
            CAST(PARSE_DATE('%Y%m%d', Start_Date_Consultant_Episode) AS DATETIME) condition_start_datetime,
            32817 condition_type_concept_id, --EHR
            32908 condition_status_concept_id, --Secondary diagnosis
            vl.id visit_occurrence_id,
            vdl.id visit_detail_id
        FROM
        (    
            SELECT DISTINCT
                person_id,
                Start_Date_Consultant_Episode,
                Diagnosis_7th_Secondary_ICD,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id) visit_occurrence_source_value,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id, '_', Episode_Number) visit_detail_source_value
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1`
            UNION DISTINCT
            SELECT DISTINCT
                person_id,
                Start_Date_Consultant_Episode,
                Diagnosis_7th_Secondary_ICD,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id) visit_occurrence_source_value,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id, '_', Episode_Number) visit_detail_source_value
            FROM `CY_IMOSPHERE_WORKSPACE.src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2`
        ) c
        LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups` vdl ON vdl.source_value = visit_detail_source_value
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = c.visit_occurrence_source_value
        JOIN `CY_CDM_VOCAB.concept` sc on REPLACE(sc.concept_code, '.', '') = c.Diagnosis_7th_Secondary_ICD
        JOIN `CY_CDM_VOCAB.concept_relationship` cr on cr.concept_id_1 = sc.concept_id
        JOIN `CY_CDM_VOCAB.concept` dc on dc.concept_id = cr.concept_id_2
        WHERE c.Start_Date_Consultant_Episode IS NOT NULL
        AND c.person_id IS NOT NULL
        AND cr.relationship_id = 'Maps to'
        AND dc.standard_concept = 'S'
        AND dc.domain_id = 'Condition'
        AND sc.vocabulary_id = 'ICD10'
        AND c.Diagnosis_7th_Secondary_ICD IS NOT NULL
        AND dc.vocabulary_id = 'SNOMED'
    ) c
    LEFT JOIN `CY_IMOSPHERE_CDM_531.condition_occurrence` oc ON 
        oc.person_id = c.person_id 
        AND oc.condition_concept_id = c.condition_concept_id
        AND oc.condition_source_value = c.condition_source_value
        AND COALESCE(oc.condition_source_concept_id, -1) = c.condition_source_concept_id
        AND oc.condition_start_datetime = c.condition_start_datetime
        AND oc.condition_type_concept_id = c.condition_type_concept_id
        AND COALESCE(oc.provider_id, -1) = -1
        AND COALESCE(oc.condition_status_concept_id, -1) = COALESCE(c.condition_status_concept_id, -1)
        AND COALESCE(oc.visit_occurrence_id, -1) = COALESCE(c.visit_occurrence_id, -1)
        AND COALESCE(oc.visit_occurrence_id, -1) = c.visit_occurrence_id
        AND COALESCE(oc.visit_detail_id, -1) = c.visit_detail_id
    WHERE oc.condition_occurrence_id IS NULL;
    
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id,  related_table, related_count, logged_at) 
    SELECT 'Added condition_occurrence', run_id, 'src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2, SUS_BRI_APC_010415_to_300619_P1  - Diagnosis_7th_Secondary_ICD -> condition_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[Diagnosis_8th_Secondary_ICD] Transfer
    */
    SET max_current_id = COALESCE((SELECT condition_occurrence_id FROM `CY_IMOSPHERE_CDM_531.condition_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.condition_occurrence`
    (
        condition_occurrence_id,
        person_id,
        condition_concept_id,
        condition_source_value,
        condition_source_concept_id,
        condition_start_date,
        condition_start_datetime,
        condition_type_concept_id,
        condition_status_concept_id,
        visit_occurrence_id,
        visit_detail_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() condition_occurrence_id,
        c.person_id,
        c.condition_concept_id,
        c.condition_source_value,
        c.condition_source_concept_id,
        c.condition_start_date,
        c.condition_start_datetime,
        c.condition_type_concept_id,
        c.condition_status_concept_id,
        c.visit_occurrence_id,
        c.visit_detail_id
    FROM
    (
        SELECT DISTINCT
            person_id,
            dc.concept_id condition_concept_id,
            Diagnosis_8th_Secondary_ICD condition_source_value,
            sc.concept_id  condition_source_concept_id,
            PARSE_DATE('%Y%m%d', Start_Date_Consultant_Episode) condition_start_date,
            CAST(PARSE_DATE('%Y%m%d', Start_Date_Consultant_Episode) AS DATETIME) condition_start_datetime,
            32817 condition_type_concept_id, --EHR
            32908 condition_status_concept_id, --Secondary diagnosis
            vl.id visit_occurrence_id,
            vdl.id visit_detail_id
        FROM
        (    
            SELECT DISTINCT
                person_id,
                Start_Date_Consultant_Episode,
                Diagnosis_8th_Secondary_ICD,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id) visit_occurrence_source_value,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id, '_', Episode_Number) visit_detail_source_value
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1`
            UNION DISTINCT
            SELECT DISTINCT
                person_id,
                Start_Date_Consultant_Episode,
                Diagnosis_8th_Secondary_ICD,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id) visit_occurrence_source_value,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id, '_', Episode_Number) visit_detail_source_value
            FROM `CY_IMOSPHERE_WORKSPACE.src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2`
        ) c
        LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups` vdl ON vdl.source_value = visit_detail_source_value
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = c.visit_occurrence_source_value
        JOIN `CY_CDM_VOCAB.concept` sc on REPLACE(sc.concept_code, '.', '') = c.Diagnosis_8th_Secondary_ICD
        JOIN `CY_CDM_VOCAB.concept_relationship` cr on cr.concept_id_1 = sc.concept_id
        JOIN `CY_CDM_VOCAB.concept` dc on dc.concept_id = cr.concept_id_2
        WHERE c.Start_Date_Consultant_Episode IS NOT NULL
        AND c.person_id IS NOT NULL
        AND cr.relationship_id = 'Maps to'
        AND dc.standard_concept = 'S'
        AND dc.domain_id = 'Condition'
        AND sc.vocabulary_id = 'ICD10'
        AND c.Diagnosis_8th_Secondary_ICD IS NOT NULL
        AND dc.vocabulary_id = 'SNOMED'
    ) c
    LEFT JOIN `CY_IMOSPHERE_CDM_531.condition_occurrence` oc ON 
        oc.person_id = c.person_id 
        AND oc.condition_concept_id = c.condition_concept_id
        AND oc.condition_source_value = c.condition_source_value
        AND COALESCE(oc.condition_source_concept_id, -1) = c.condition_source_concept_id
        AND oc.condition_start_datetime = c.condition_start_datetime
        AND oc.condition_type_concept_id = c.condition_type_concept_id
        AND COALESCE(oc.provider_id, -1) = -1
        AND COALESCE(oc.condition_status_concept_id, -1) = COALESCE(c.condition_status_concept_id, -1)
        AND COALESCE(oc.visit_occurrence_id, -1) = COALESCE(c.visit_occurrence_id, -1)
        AND COALESCE(oc.visit_occurrence_id, -1) = c.visit_occurrence_id
        AND COALESCE(oc.visit_detail_id, -1) = c.visit_detail_id
    WHERE oc.condition_occurrence_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id,  related_table, related_count, logged_at) 
    SELECT 'Added condition_occurrence', run_id, 'src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2, SUS_BRI_APC_010415_to_300619_P1  - Diagnosis_8th_Secondary_ICD -> condition_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[Diagnosis_9th_Secondary_ICD] Transfer
    */
    SET max_current_id = COALESCE((SELECT condition_occurrence_id FROM `CY_IMOSPHERE_CDM_531.condition_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.condition_occurrence`
    (
        condition_occurrence_id,
        person_id,
        condition_concept_id,
        condition_source_value,
        condition_source_concept_id,
        condition_start_date,
        condition_start_datetime,
        condition_type_concept_id,
        condition_status_concept_id,
        visit_occurrence_id,
        visit_detail_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() condition_occurrence_id,
        c.person_id,
        c.condition_concept_id,
        c.condition_source_value,
        c.condition_source_concept_id,
        c.condition_start_date,
        c.condition_start_datetime,
        c.condition_type_concept_id,
        c.condition_status_concept_id,
        c.visit_occurrence_id,
        c.visit_detail_id
    FROM
    (
        SELECT DISTINCT
            person_id,
            dc.concept_id condition_concept_id,
            Diagnosis_9th_Secondary_ICD condition_source_value,
            sc.concept_id  condition_source_concept_id,
            PARSE_DATE('%Y%m%d', Start_Date_Consultant_Episode) condition_start_date,
            CAST(PARSE_DATE('%Y%m%d', Start_Date_Consultant_Episode) AS DATETIME) condition_start_datetime,
            32817 condition_type_concept_id, --EHR
            32908 condition_status_concept_id, --Secondary diagnosis
            vl.id visit_occurrence_id,
            vdl.id visit_detail_id
        FROM
        (    
            SELECT DISTINCT
                person_id,
                Start_Date_Consultant_Episode,
                Diagnosis_9th_Secondary_ICD,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id) visit_occurrence_source_value,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id, '_', Episode_Number) visit_detail_source_value
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1`
            UNION DISTINCT
            SELECT DISTINCT
                person_id,
                Start_Date_Consultant_Episode,
                Diagnosis_9th_Secondary_ICD,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id) visit_occurrence_source_value,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id, '_', Episode_Number) visit_detail_source_value
            FROM `CY_IMOSPHERE_WORKSPACE.src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2`
        ) c
        LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups` vdl ON vdl.source_value = visit_detail_source_value
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = c.visit_occurrence_source_value
        JOIN `CY_CDM_VOCAB.concept` sc on REPLACE(sc.concept_code, '.', '') = c.Diagnosis_9th_Secondary_ICD
        JOIN `CY_CDM_VOCAB.concept_relationship` cr on cr.concept_id_1 = sc.concept_id
        JOIN `CY_CDM_VOCAB.concept` dc on dc.concept_id = cr.concept_id_2
        WHERE c.Start_Date_Consultant_Episode IS NOT NULL
        AND c.person_id IS NOT NULL
        AND cr.relationship_id = 'Maps to'
        AND dc.standard_concept = 'S'
        AND dc.domain_id = 'Condition'
        AND sc.vocabulary_id = 'ICD10'
        AND c.Diagnosis_9th_Secondary_ICD IS NOT NULL
        AND dc.vocabulary_id = 'SNOMED'
    ) c
    LEFT JOIN `CY_IMOSPHERE_CDM_531.condition_occurrence` oc ON 
        oc.person_id = c.person_id 
        AND oc.condition_concept_id = c.condition_concept_id
        AND oc.condition_source_value = c.condition_source_value
        AND COALESCE(oc.condition_source_concept_id, -1) = c.condition_source_concept_id
        AND oc.condition_start_datetime = c.condition_start_datetime
        AND oc.condition_type_concept_id = c.condition_type_concept_id
        AND COALESCE(oc.provider_id, -1) = -1
        AND COALESCE(oc.condition_status_concept_id, -1) = COALESCE(c.condition_status_concept_id, -1)
        AND COALESCE(oc.visit_occurrence_id, -1) = COALESCE(c.visit_occurrence_id, -1)
        AND COALESCE(oc.visit_occurrence_id, -1) = c.visit_occurrence_id
        AND COALESCE(oc.visit_detail_id, -1) = c.visit_detail_id
    WHERE oc.condition_occurrence_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id,  related_table, related_count, logged_at) 
    SELECT 'Added condition_occurrence', run_id, 'src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2, SUS_BRI_APC_010415_to_300619_P1  - Diagnosis_9th_Secondary_ICD -> condition_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[Diagnosis_10th_Secondary_ICD] Transfer
    */
    SET max_current_id = COALESCE((SELECT condition_occurrence_id FROM `CY_IMOSPHERE_CDM_531.condition_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.condition_occurrence`
    (
        condition_occurrence_id,
        person_id,
        condition_concept_id,
        condition_source_value,
        condition_source_concept_id,
        condition_start_date,
        condition_start_datetime,
        condition_type_concept_id,
        condition_status_concept_id,
        visit_occurrence_id,
        visit_detail_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() condition_occurrence_id,
        c.person_id,
        c.condition_concept_id,
        c.condition_source_value,
        c.condition_source_concept_id,
        c.condition_start_date,
        c.condition_start_datetime,
        c.condition_type_concept_id,
        c.condition_status_concept_id,
        c.visit_occurrence_id,
        c.visit_detail_id
    FROM
    (
        SELECT DISTINCT
            person_id,
            dc.concept_id condition_concept_id,
            Diagnosis_10th_Secondary_ICD condition_source_value,
            sc.concept_id  condition_source_concept_id,
            PARSE_DATE('%Y%m%d', Start_Date_Consultant_Episode) condition_start_date,
            CAST(PARSE_DATE('%Y%m%d', Start_Date_Consultant_Episode) AS DATETIME) condition_start_datetime,
            32817 condition_type_concept_id, --EHR
            32908 condition_status_concept_id, --Secondary diagnosis
            vl.id visit_occurrence_id,
            vdl.id visit_detail_id
        FROM
        (    
            SELECT DISTINCT
                person_id,
                Start_Date_Consultant_Episode,
                Diagnosis_10th_Secondary_ICD,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id) visit_occurrence_source_value,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id, '_', Episode_Number) visit_detail_source_value
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1`
            UNION DISTINCT
            SELECT DISTINCT
                person_id,
                Start_Date_Consultant_Episode,
                Diagnosis_10th_Secondary_ICD,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id) visit_occurrence_source_value,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id, '_', Episode_Number) visit_detail_source_value
            FROM `CY_IMOSPHERE_WORKSPACE.src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2`
        ) c
        LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups` vdl ON vdl.source_value = visit_detail_source_value
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = c.visit_occurrence_source_value
        JOIN `CY_CDM_VOCAB.concept` sc on REPLACE(sc.concept_code, '.', '') = c.Diagnosis_10th_Secondary_ICD
        JOIN `CY_CDM_VOCAB.concept_relationship` cr on cr.concept_id_1 = sc.concept_id
        JOIN `CY_CDM_VOCAB.concept` dc on dc.concept_id = cr.concept_id_2
        WHERE c.Start_Date_Consultant_Episode IS NOT NULL
        AND c.person_id IS NOT NULL
        AND cr.relationship_id = 'Maps to'
        AND dc.standard_concept = 'S'
        AND dc.domain_id = 'Condition'
        AND sc.vocabulary_id = 'ICD10'
        AND c.Diagnosis_10th_Secondary_ICD IS NOT NULL
        AND dc.vocabulary_id = 'SNOMED'
    ) c
    LEFT JOIN `CY_IMOSPHERE_CDM_531.condition_occurrence` oc ON 
        oc.person_id = c.person_id 
        AND oc.condition_concept_id = c.condition_concept_id
        AND oc.condition_source_value = c.condition_source_value
        AND COALESCE(oc.condition_source_concept_id, -1) = c.condition_source_concept_id
        AND oc.condition_start_datetime = c.condition_start_datetime
        AND oc.condition_type_concept_id = c.condition_type_concept_id
        AND COALESCE(oc.provider_id, -1) = -1
        AND COALESCE(oc.condition_status_concept_id, -1) = COALESCE(c.condition_status_concept_id, -1)
        AND COALESCE(oc.visit_occurrence_id, -1) = COALESCE(c.visit_occurrence_id, -1)
        AND COALESCE(oc.visit_occurrence_id, -1) = c.visit_occurrence_id
        AND COALESCE(oc.visit_detail_id, -1) = c.visit_detail_id
    WHERE oc.condition_occurrence_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id,  related_table, related_count, logged_at) 
    SELECT 'Added condition_occurrence', run_id, 'src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2, SUS_BRI_APC_010415_to_300619_P1  - Diagnosis_10th_Secondary_ICD -> condition_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[Diagnosis_11th_Secondary_ICD] Transfer
    */
    SET max_current_id = COALESCE((SELECT condition_occurrence_id FROM `CY_IMOSPHERE_CDM_531.condition_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.condition_occurrence`
    (
        condition_occurrence_id,
        person_id,
        condition_concept_id,
        condition_source_value,
        condition_source_concept_id,
        condition_start_date,
        condition_start_datetime,
        condition_type_concept_id,
        condition_status_concept_id,
        visit_occurrence_id,
        visit_detail_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() condition_occurrence_id,
        c.person_id,
        c.condition_concept_id,
        c.condition_source_value,
        c.condition_source_concept_id,
        c.condition_start_date,
        c.condition_start_datetime,
        c.condition_type_concept_id,
        c.condition_status_concept_id,
        c.visit_occurrence_id,
        c.visit_detail_id
    FROM
    (
        SELECT DISTINCT
            person_id,
            dc.concept_id condition_concept_id,
            Diagnosis_11th_Secondary_ICD condition_source_value,
            sc.concept_id  condition_source_concept_id,
            PARSE_DATE('%Y%m%d', Start_Date_Consultant_Episode) condition_start_date,
            CAST(PARSE_DATE('%Y%m%d', Start_Date_Consultant_Episode) AS DATETIME) condition_start_datetime,
            32817 condition_type_concept_id, --EHR
            32908 condition_status_concept_id, --Secondary diagnosis
            vl.id visit_occurrence_id,
            vdl.id visit_detail_id
        FROM
        (    
            SELECT DISTINCT
                person_id,
                Start_Date_Consultant_Episode,
                Diagnosis_11th_Secondary_ICD,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id) visit_occurrence_source_value,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id, '_', Episode_Number) visit_detail_source_value
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1`
            UNION DISTINCT
            SELECT DISTINCT
                person_id,
                Start_Date_Consultant_Episode,
                Diagnosis_11th_Secondary_ICD,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id) visit_occurrence_source_value,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id, '_', Episode_Number) visit_detail_source_value
            FROM `CY_IMOSPHERE_WORKSPACE.src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2`
        ) c
        LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups` vdl ON vdl.source_value = visit_detail_source_value
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = c.visit_occurrence_source_value
        JOIN `CY_CDM_VOCAB.concept` sc on REPLACE(sc.concept_code, '.', '') = c.Diagnosis_11th_Secondary_ICD
        JOIN `CY_CDM_VOCAB.concept_relationship` cr on cr.concept_id_1 = sc.concept_id
        JOIN `CY_CDM_VOCAB.concept` dc on dc.concept_id = cr.concept_id_2
        WHERE c.Start_Date_Consultant_Episode IS NOT NULL
        AND c.person_id IS NOT NULL
        AND cr.relationship_id = 'Maps to'
        AND dc.standard_concept = 'S'
        AND dc.domain_id = 'Condition'
        AND sc.vocabulary_id = 'ICD10'
        AND c.Diagnosis_11th_Secondary_ICD IS NOT NULL
        AND dc.vocabulary_id = 'SNOMED'
    ) c
    LEFT JOIN `CY_IMOSPHERE_CDM_531.condition_occurrence` oc ON 
        oc.person_id = c.person_id 
        AND oc.condition_concept_id = c.condition_concept_id
        AND oc.condition_source_value = c.condition_source_value
        AND COALESCE(oc.condition_source_concept_id, -1) = c.condition_source_concept_id
        AND oc.condition_start_datetime = c.condition_start_datetime
        AND oc.condition_type_concept_id = c.condition_type_concept_id
        AND COALESCE(oc.provider_id, -1) = -1
        AND COALESCE(oc.condition_status_concept_id, -1) = COALESCE(c.condition_status_concept_id, -1)
        AND COALESCE(oc.visit_occurrence_id, -1) = COALESCE(c.visit_occurrence_id, -1)
        AND COALESCE(oc.visit_occurrence_id, -1) = c.visit_occurrence_id
        AND COALESCE(oc.visit_detail_id, -1) = c.visit_detail_id
    WHERE oc.condition_occurrence_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id,  related_table, related_count, logged_at) 
    SELECT 'Added condition_occurrence', run_id, 'src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2, SUS_BRI_APC_010415_to_300619_P1  - Diagnosis_11th_Secondary_ICD -> condition_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[Diagnosis_12th_Secondary_ICD] Transfer
    */
    SET max_current_id = COALESCE((SELECT condition_occurrence_id FROM `CY_IMOSPHERE_CDM_531.condition_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.condition_occurrence`
    (
        condition_occurrence_id,
        person_id,
        condition_concept_id,
        condition_source_value,
        condition_source_concept_id,
        condition_start_date,
        condition_start_datetime,
        condition_type_concept_id,
        condition_status_concept_id,
        visit_occurrence_id,
        visit_detail_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() condition_occurrence_id,
        c.person_id,
        c.condition_concept_id,
        c.condition_source_value,
        c.condition_source_concept_id,
        c.condition_start_date,
        c.condition_start_datetime,
        c.condition_type_concept_id,
        c.condition_status_concept_id,
        c.visit_occurrence_id,
        c.visit_detail_id
    FROM
    (
        SELECT DISTINCT
            person_id,
            dc.concept_id condition_concept_id,
            Diagnosis_12th_Secondary_ICD condition_source_value,
            sc.concept_id  condition_source_concept_id,
            PARSE_DATE('%Y%m%d', Start_Date_Consultant_Episode) condition_start_date,
            CAST(PARSE_DATE('%Y%m%d', Start_Date_Consultant_Episode) AS DATETIME) condition_start_datetime,
            32817 condition_type_concept_id, --EHR
            32908 condition_status_concept_id, --Secondary diagnosis
            vl.id visit_occurrence_id,
            vdl.id visit_detail_id
        FROM
        (    
            SELECT DISTINCT
                person_id,
                Start_Date_Consultant_Episode,
                Diagnosis_12th_Secondary_ICD,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id) visit_occurrence_source_value,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id, '_', Episode_Number) visit_detail_source_value
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1`
            UNION DISTINCT
            SELECT DISTINCT
                person_id,
                Start_Date_Consultant_Episode,
                Diagnosis_12th_Secondary_ICD,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id) visit_occurrence_source_value,
                CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id, '_', Episode_Number) visit_detail_source_value
            FROM `CY_IMOSPHERE_WORKSPACE.src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2`
        ) c
        LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups` vdl ON vdl.source_value = visit_detail_source_value
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = c.visit_occurrence_source_value
        JOIN `CY_CDM_VOCAB.concept` sc on REPLACE(sc.concept_code, '.', '') = c.Diagnosis_12th_Secondary_ICD
        JOIN `CY_CDM_VOCAB.concept_relationship` cr on cr.concept_id_1 = sc.concept_id
        JOIN `CY_CDM_VOCAB.concept` dc on dc.concept_id = cr.concept_id_2
        WHERE c.Start_Date_Consultant_Episode IS NOT NULL
        AND c.person_id IS NOT NULL
        AND cr.relationship_id = 'Maps to'
        AND dc.standard_concept = 'S'
        AND dc.domain_id = 'Condition'
        AND sc.vocabulary_id = 'ICD10'
        AND c.Diagnosis_12th_Secondary_ICD IS NOT NULL
        AND dc.vocabulary_id = 'SNOMED'
    ) c
    LEFT JOIN `CY_IMOSPHERE_CDM_531.condition_occurrence` oc ON 
        oc.person_id = c.person_id 
        AND oc.condition_concept_id = c.condition_concept_id
        AND oc.condition_source_value = c.condition_source_value
        AND COALESCE(oc.condition_source_concept_id, -1) = c.condition_source_concept_id
        AND oc.condition_start_datetime = c.condition_start_datetime
        AND oc.condition_type_concept_id = c.condition_type_concept_id
        AND COALESCE(oc.provider_id, -1) = -1
        AND COALESCE(oc.condition_status_concept_id, -1) = COALESCE(c.condition_status_concept_id, -1)
        AND COALESCE(oc.visit_occurrence_id, -1) = COALESCE(c.visit_occurrence_id, -1)
        AND COALESCE(oc.visit_occurrence_id, -1) = c.visit_occurrence_id
        AND COALESCE(oc.visit_detail_id, -1) = c.visit_detail_id
    WHERE oc.condition_occurrence_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id,  related_table, related_count, logged_at) 
    SELECT 'Added condition_occurrence', run_id, 'src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2, SUS_BRI_APC_010415_to_300619_P1  - Diagnosis_12th_Secondary_ICD -> condition_occurrence', @@row_count, CURRENT_DATETIME();
END;

CREATE OR REPLACE PROCEDURE CY_IMOSPHERE_WORKSPACE.TRANSFER_PROCEDURE_OCCURRENCE(run_id INT64)
OPTIONS (strict_mode=false)
BEGIN
    DECLARE max_current_id INT64;

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    ------------[SUS_BRI_AE_010415_to_300619]
    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_First] Transfer
    */
    SET max_current_id = COALESCE((SELECT procedure_occurrence_id FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.procedure_occurrence`
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_source_value,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as procedure_occurrence_id,
        p.person_id,
        p.procedure_concept_id,
        p.procedure_source_value,
        p.procedure_date,
        p.procedure_datetime,
        p.procedure_type_concept_id,
        p.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
                p.person_id,
                pc.concept_id procedure_concept_id,
                Accident_And_Emergency_Investigation_First procedure_source_value,	
                PARSE_DATE('%Y%m%d', p.Arrival_Date) procedure_date,
                PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.Arrival_Date ,'000000')) procedure_datetime,
                32817 procedure_type_concept_id, --EHR
                vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` p
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` pc on pc.source_value = REPLACE(p.Accident_And_Emergency_Investigation_First, ' ', '') AND pc.destination_table = 'PROCEDURE_OCCURRENCE' AND pc.source_table = 'SUS_BRI_AE_010415_to_300619' AND pc.source_column = 'Accident_And_Emergency_Investigation_First'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl ON vl.source_value = CONCAT(CAST(p.AandE_Attendance_Number as STRING),'_', p.person_id)
        WHERE p.Arrival_Date IS NOT NULL
        AND p.person_id IS NOT NULL
        AND p.Accident_And_Emergency_Investigation_First IS NOT NULL
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.procedure_occurrence` op ON 
        op.person_id = p.person_id 
        AND op.procedure_concept_id = p.procedure_concept_id
        AND op.procedure_source_value = p.procedure_source_value
        AND COALESCE(op.procedure_source_concept_id, -1) = -1
        AND op.procedure_datetime = p.procedure_datetime
        AND op.procedure_type_concept_id = p.procedure_type_concept_id
        AND COALESCE(op.provider_id, -1) = -1
        AND COALESCE(op.visit_occurrence_id, -1) = COALESCE(p.visit_occurrence_id, -1)
        AND COALESCE(op.visit_detail_id, -1) = -1
    WHERE op.procedure_occurrence_id IS NULL;     

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added procedure_occurrence', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_First -> procedure_occurrence', @@row_count, CURRENT_DATETIME();
    
    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Second] Transfer
    */
    SET max_current_id = COALESCE((SELECT procedure_occurrence_id FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.procedure_occurrence`
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_source_value,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as procedure_occurrence_id,
        p.person_id,
        p.procedure_concept_id,
        p.procedure_source_value,
        p.procedure_date,
        p.procedure_datetime,
        p.procedure_type_concept_id,
        p.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            p.person_id,
            pc.concept_id procedure_concept_id,
            Accident_And_Emergency_Investigation_Second procedure_source_value,	
            PARSE_DATE('%Y%m%d', p.Arrival_Date) procedure_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.Arrival_Date ,'000000')) procedure_datetime,
            32817 procedure_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` p
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` pc on pc.source_value = REPLACE(p.Accident_And_Emergency_Investigation_Second, ' ', '') AND pc.destination_table = 'PROCEDURE_OCCURRENCE' AND pc.source_table = 'SUS_BRI_AE_010415_to_300619' AND pc.source_column = 'Accident_And_Emergency_Investigation_Second'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl ON vl.source_value = CONCAT(CAST(p.AandE_Attendance_Number as STRING),'_', p.person_id)
        WHERE p.Arrival_Date IS NOT NULL
        AND p.person_id IS NOT NULL
        AND p.Accident_And_Emergency_Investigation_Second IS NOT NULL
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.procedure_occurrence` op ON 
        op.person_id = p.person_id 
        AND op.procedure_concept_id = p.procedure_concept_id
        AND op.procedure_source_value = p.procedure_source_value
        AND COALESCE(op.procedure_source_concept_id, -1) = -1
        AND op.procedure_datetime = p.procedure_datetime
        AND op.procedure_type_concept_id = p.procedure_type_concept_id
        AND COALESCE(op.provider_id, -1) = -1
        AND COALESCE(op.visit_occurrence_id, -1) = COALESCE(p.visit_occurrence_id, -1)
        AND COALESCE(op.visit_detail_id, -1) = -1
    WHERE op.procedure_occurrence_id IS NULL;     

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added procedure_occurrence', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Second -> procedure_occurrence', @@row_count, CURRENT_DATETIME();
     
    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Third] Transfer
    */
    SET max_current_id = COALESCE((SELECT procedure_occurrence_id FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.procedure_occurrence`
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_source_value,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as procedure_occurrence_id,
        p.person_id,
        p.procedure_concept_id,
        p.procedure_source_value,
        p.procedure_date,
        p.procedure_datetime,
        p.procedure_type_concept_id,
        p.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            p.person_id,
            pc.concept_id procedure_concept_id,
            Accident_And_Emergency_Investigation_Third procedure_source_value,	
            PARSE_DATE('%Y%m%d', p.Arrival_Date) procedure_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.Arrival_Date ,'000000')) procedure_datetime,
            32817 procedure_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` p
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` pc on pc.source_value = REPLACE(p.Accident_And_Emergency_Investigation_Third, ' ', '') AND pc.destination_table = 'PROCEDURE_OCCURRENCE' AND pc.source_table = 'SUS_BRI_AE_010415_to_300619' AND pc.source_column = 'Accident_And_Emergency_Investigation_Third'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl ON vl.source_value = CONCAT(CAST(p.AandE_Attendance_Number as STRING),'_', p.person_id)
        WHERE p.Arrival_Date IS NOT NULL
        AND p.person_id IS NOT NULL
        AND p.Accident_And_Emergency_Investigation_Third IS NOT NULL
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.procedure_occurrence` op ON 
        op.person_id = p.person_id 
        AND op.procedure_concept_id = p.procedure_concept_id
        AND op.procedure_source_value = p.procedure_source_value
        AND COALESCE(op.procedure_source_concept_id, -1) = -1
        AND op.procedure_datetime = p.procedure_datetime
        AND op.procedure_type_concept_id = p.procedure_type_concept_id
        AND COALESCE(op.provider_id, -1) = -1
        AND COALESCE(op.visit_occurrence_id, -1) = COALESCE(p.visit_occurrence_id, -1)
        AND COALESCE(op.visit_detail_id, -1) = -1
    WHERE op.procedure_occurrence_id IS NULL;   

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added procedure_occurrence', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Third -> procedure_occurrence', @@row_count, CURRENT_DATETIME();


    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Fourth] Transfer
    */
    SET max_current_id = COALESCE((SELECT procedure_occurrence_id FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.procedure_occurrence`
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_source_value,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as procedure_occurrence_id,
        p.person_id,
        p.procedure_concept_id,
        p.procedure_source_value,
        p.procedure_date,
        p.procedure_datetime,
        p.procedure_type_concept_id,
        p.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            p.person_id,
            pc.concept_id procedure_concept_id,
            Accident_And_Emergency_Investigation_Fourth procedure_source_value,	
            PARSE_DATE('%Y%m%d', p.Arrival_Date) procedure_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.Arrival_Date ,'000000')) procedure_datetime,
            32817 procedure_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` p
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` pc on pc.source_value = REPLACE(p.Accident_And_Emergency_Investigation_Fourth, ' ', '') AND pc.destination_table = 'PROCEDURE_OCCURRENCE' AND pc.source_table = 'SUS_BRI_AE_010415_to_300619' AND pc.source_column = 'Accident_And_Emergency_Investigation_Fourth'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl ON vl.source_value = CONCAT(CAST(p.AandE_Attendance_Number as STRING),'_', p.person_id)
        WHERE p.Arrival_Date IS NOT NULL
        AND p.person_id IS NOT NULL
        AND p.Accident_And_Emergency_Investigation_Fourth IS NOT NULL
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.procedure_occurrence` op ON 
        op.person_id = p.person_id 
        AND op.procedure_concept_id = p.procedure_concept_id
        AND op.procedure_source_value = p.procedure_source_value
        AND COALESCE(op.procedure_source_concept_id, -1) = -1
        AND op.procedure_datetime = p.procedure_datetime
        AND op.procedure_type_concept_id = p.procedure_type_concept_id
        AND COALESCE(op.provider_id, -1) = -1
        AND COALESCE(op.visit_occurrence_id, -1) = COALESCE(p.visit_occurrence_id, -1)
        AND COALESCE(op.visit_detail_id, -1) = -1
    WHERE op.procedure_occurrence_id IS NULL;       

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added procedure_occurrence', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Fourth -> procedure_occurrence', @@row_count, CURRENT_DATETIME();


    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Five] Transfer
    */
    SET max_current_id = COALESCE((SELECT procedure_occurrence_id FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.procedure_occurrence`
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_source_value,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as procedure_occurrence_id,
        p.person_id,
        p.procedure_concept_id,
        p.procedure_source_value,
        p.procedure_date,
        p.procedure_datetime,
        p.procedure_type_concept_id,
        p.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            p.person_id,
            pc.concept_id procedure_concept_id,
            Accident_And_Emergency_Investigation_Fifth procedure_source_value,	
            PARSE_DATE('%Y%m%d', p.Arrival_Date) procedure_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.Arrival_Date ,'000000')) procedure_datetime,
            32817 procedure_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` p
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` pc on pc.source_value = REPLACE(p.Accident_And_Emergency_Investigation_Fifth, ' ', '') AND pc.destination_table = 'PROCEDURE_OCCURRENCE' AND pc.source_table = 'SUS_BRI_AE_010415_to_300619' AND pc.source_column = 'Accident_And_Emergency_Investigation_Fifth'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl ON vl.source_value = CONCAT(CAST(p.AandE_Attendance_Number as STRING),'_', p.person_id)
        WHERE p.Arrival_Date IS NOT NULL
        AND p.person_id IS NOT NULL
        AND p.Accident_And_Emergency_Investigation_Fifth IS NOT NULL
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.procedure_occurrence` op ON 
        op.person_id = p.person_id 
        AND op.procedure_concept_id = p.procedure_concept_id
        AND op.procedure_source_value = p.procedure_source_value
        AND COALESCE(op.procedure_source_concept_id, -1) = -1
        AND op.procedure_datetime = p.procedure_datetime
        AND op.procedure_type_concept_id = p.procedure_type_concept_id
        AND COALESCE(op.provider_id, -1) = -1
        AND COALESCE(op.visit_occurrence_id, -1) = COALESCE(p.visit_occurrence_id, -1)
        AND COALESCE(op.visit_detail_id, -1) = -1
    WHERE op.procedure_occurrence_id IS NULL;         

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added procedure_occurrence', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Fifth -> procedure_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Sixth] Transfer
    */
    SET max_current_id = COALESCE((SELECT procedure_occurrence_id FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.procedure_occurrence`
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_source_value,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as procedure_occurrence_id,
        p.person_id,
        p.procedure_concept_id,
        p.procedure_source_value,
        p.procedure_date,
        p.procedure_datetime,
        p.procedure_type_concept_id,
        p.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            p.person_id,
            pc.concept_id procedure_concept_id,
            Accident_And_Emergency_Investigation_Sixth procedure_source_value,	
            PARSE_DATE('%Y%m%d', p.Arrival_Date) procedure_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.Arrival_Date ,'000000')) procedure_datetime,
            32817 procedure_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` p
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` pc on pc.source_value = REPLACE(p.Accident_And_Emergency_Investigation_Sixth, ' ', '') AND pc.destination_table = 'PROCEDURE_OCCURRENCE' AND pc.source_table = 'SUS_BRI_AE_010415_to_300619' AND pc.source_column = 'Accident_And_Emergency_Investigation_Sixth'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl ON vl.source_value = CONCAT(CAST(p.AandE_Attendance_Number as STRING),'_', p.person_id)
        WHERE p.Arrival_Date IS NOT NULL
        AND p.person_id IS NOT NULL
        AND p.Accident_And_Emergency_Investigation_Sixth IS NOT NULL
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.procedure_occurrence` op ON 
        op.person_id = p.person_id 
        AND op.procedure_concept_id = p.procedure_concept_id
        AND op.procedure_source_value = p.procedure_source_value
        AND COALESCE(op.procedure_source_concept_id, -1) = -1
        AND op.procedure_datetime = p.procedure_datetime
        AND op.procedure_type_concept_id = p.procedure_type_concept_id
        AND COALESCE(op.provider_id, -1) = -1
        AND COALESCE(op.visit_occurrence_id, -1) = COALESCE(p.visit_occurrence_id, -1)
        AND COALESCE(op.visit_detail_id, -1) = -1
    WHERE op.procedure_occurrence_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added procedure_occurrence', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Sixth -> procedure_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Seventh] Transfer
    */
    SET max_current_id = COALESCE((SELECT procedure_occurrence_id FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.procedure_occurrence`
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_source_value,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as procedure_occurrence_id,
        p.person_id,
        p.procedure_concept_id,
        p.procedure_source_value,
        p.procedure_date,
        p.procedure_datetime,
        p.procedure_type_concept_id,
        p.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            p.person_id,
            pc.concept_id procedure_concept_id,
            Accident_And_Emergency_Investigation_Seventh procedure_source_value,	
            PARSE_DATE('%Y%m%d', p.Arrival_Date) procedure_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.Arrival_Date ,'000000')) procedure_datetime,
            32817 procedure_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` p
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` pc on pc.source_value = REPLACE(p.Accident_And_Emergency_Investigation_Seventh, ' ', '') AND pc.destination_table = 'PROCEDURE_OCCURRENCE' AND pc.source_table = 'SUS_BRI_AE_010415_to_300619' AND pc.source_column = 'Accident_And_Emergency_Investigation_Seventh'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl ON vl.source_value = CONCAT(CAST(p.AandE_Attendance_Number as STRING),'_', p.person_id)
        WHERE p.Arrival_Date IS NOT NULL
        AND p.person_id IS NOT NULL
        AND p.Accident_And_Emergency_Investigation_Seventh IS NOT NULL
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.procedure_occurrence` op ON 
        op.person_id = p.person_id 
        AND op.procedure_concept_id = p.procedure_concept_id
        AND op.procedure_source_value = p.procedure_source_value
        AND COALESCE(op.procedure_source_concept_id, -1) = -1
        AND op.procedure_datetime = p.procedure_datetime
        AND op.procedure_type_concept_id = p.procedure_type_concept_id
        AND COALESCE(op.provider_id, -1) = -1
        AND COALESCE(op.visit_occurrence_id, -1) = COALESCE(p.visit_occurrence_id, -1)
        AND COALESCE(op.visit_detail_id, -1) = -1
    WHERE op.procedure_occurrence_id IS NULL;      

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added procedure_occurrence', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Seventh -> procedure_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Eighth] Transfer
    */
    SET max_current_id = COALESCE((SELECT procedure_occurrence_id FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.procedure_occurrence`
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_source_value,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as procedure_occurrence_id,
        p.person_id,
        p.procedure_concept_id,
        p.procedure_source_value,
        p.procedure_date,
        p.procedure_datetime,
        p.procedure_type_concept_id,
        p.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            p.person_id,
            pc.concept_id procedure_concept_id,
            Accident_And_Emergency_Investigation_Eighth procedure_source_value,	
            PARSE_DATE('%Y%m%d', p.Arrival_Date) procedure_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.Arrival_Date ,'000000')) procedure_datetime,
            32817 procedure_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` p
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` pc on pc.source_value = REPLACE(p.Accident_And_Emergency_Investigation_Eighth, ' ', '') AND pc.destination_table = 'PROCEDURE_OCCURRENCE' AND pc.source_table = 'SUS_BRI_AE_010415_to_300619' AND pc.source_column = 'Accident_And_Emergency_Investigation_Eighth'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl ON vl.source_value = CONCAT(CAST(p.AandE_Attendance_Number as STRING),'_', p.person_id)
        WHERE p.Arrival_Date IS NOT NULL
        AND p.person_id IS NOT NULL
        AND p.Accident_And_Emergency_Investigation_Eighth IS NOT NULL
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.procedure_occurrence` op ON 
        op.person_id = p.person_id 
        AND op.procedure_concept_id = p.procedure_concept_id
        AND op.procedure_source_value = p.procedure_source_value
        AND COALESCE(op.procedure_source_concept_id, -1) = -1
        AND op.procedure_datetime = p.procedure_datetime
        AND op.procedure_type_concept_id = p.procedure_type_concept_id
        AND COALESCE(op.provider_id, -1) = -1
        AND COALESCE(op.visit_occurrence_id, -1) = COALESCE(p.visit_occurrence_id, -1)
        AND COALESCE(op.visit_detail_id, -1) = -1
    WHERE op.procedure_occurrence_id IS NULL;     

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added procedure_occurrence', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Eighth -> procedure_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Ninth] Transfer
    */
    SET max_current_id = COALESCE((SELECT procedure_occurrence_id FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.procedure_occurrence`
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_source_value,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as procedure_occurrence_id,
        p.person_id,
        p.procedure_concept_id,
        p.procedure_source_value,
        p.procedure_date,
        p.procedure_datetime,
        p.procedure_type_concept_id,
        p.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            p.person_id,
            pc.concept_id procedure_concept_id,
            Accident_And_Emergency_Investigation_Ninth procedure_source_value,	
            PARSE_DATE('%Y%m%d', p.Arrival_Date) procedure_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.Arrival_Date ,'000000')) procedure_datetime,
            32817 procedure_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` p
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` pc on pc.source_value = REPLACE(p.Accident_And_Emergency_Investigation_Ninth, ' ', '') AND pc.destination_table = 'PROCEDURE_OCCURRENCE' AND pc.source_table = 'SUS_BRI_AE_010415_to_300619' AND pc.source_column = 'Accident_And_Emergency_Investigation_Ninth'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl ON vl.source_value = CONCAT(CAST(p.AandE_Attendance_Number as STRING),'_', p.person_id)
        WHERE p.Arrival_Date IS NOT NULL
        AND p.person_id IS NOT NULL
        AND p.Accident_And_Emergency_Investigation_Ninth IS NOT NULL
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.procedure_occurrence` op ON 
        op.person_id = p.person_id 
        AND op.procedure_concept_id = p.procedure_concept_id
        AND op.procedure_source_value = p.procedure_source_value
        AND COALESCE(op.procedure_source_concept_id, -1) = -1
        AND op.procedure_datetime = p.procedure_datetime
        AND op.procedure_type_concept_id = p.procedure_type_concept_id
        AND COALESCE(op.provider_id, -1) = -1
        AND COALESCE(op.visit_occurrence_id, -1) = COALESCE(p.visit_occurrence_id, -1)
        AND COALESCE(op.visit_detail_id, -1) = -1
    WHERE op.procedure_occurrence_id IS NULL;       

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added procedure_occurrence', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Ninth -> procedure_occurrence', @@row_count, CURRENT_DATETIME();
     
    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Tenth] Transfer
    */
    SET max_current_id = COALESCE((SELECT procedure_occurrence_id FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.procedure_occurrence`
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_source_value,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as procedure_occurrence_id,
        p.person_id,
        p.procedure_concept_id,
        p.procedure_source_value,
        p.procedure_date,
        p.procedure_datetime,
        p.procedure_type_concept_id,
        p.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            p.person_id,
            pc.concept_id procedure_concept_id,
            Accident_And_Emergency_Investigation_Tenth procedure_source_value,	
            PARSE_DATE('%Y%m%d', p.Arrival_Date) procedure_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.Arrival_Date ,'000000')) procedure_datetime,
            32817 procedure_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` p
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` pc on pc.source_value = REPLACE(p.Accident_And_Emergency_Investigation_Tenth, ' ', '') AND pc.destination_table = 'PROCEDURE_OCCURRENCE' AND pc.source_table = 'SUS_BRI_AE_010415_to_300619' AND pc.source_column = 'Accident_And_Emergency_Investigation_Tenth'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl ON vl.source_value = CONCAT(CAST(p.AandE_Attendance_Number as STRING),'_', p.person_id)
        WHERE p.Arrival_Date IS NOT NULL
        AND p.person_id IS NOT NULL
        AND p.Accident_And_Emergency_Investigation_Tenth IS NOT NULL
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.procedure_occurrence` op ON 
        op.person_id = p.person_id 
        AND op.procedure_concept_id = p.procedure_concept_id
        AND op.procedure_source_value = p.procedure_source_value
        AND COALESCE(op.procedure_source_concept_id, -1) = -1
        AND op.procedure_datetime = p.procedure_datetime
        AND op.procedure_type_concept_id = p.procedure_type_concept_id
        AND COALESCE(op.provider_id, -1) = -1
        AND COALESCE(op.visit_occurrence_id, -1) = COALESCE(p.visit_occurrence_id, -1)
        AND COALESCE(op.visit_detail_id, -1) = -1
    WHERE op.procedure_occurrence_id IS NULL;      

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added procedure_occurrence', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Tenth -> procedure_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Eleventh] Transfer
    */
    SET max_current_id = COALESCE((SELECT procedure_occurrence_id FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.procedure_occurrence`
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_source_value,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as procedure_occurrence_id,
        p.person_id,
        p.procedure_concept_id,
        p.procedure_source_value,
        p.procedure_date,
        p.procedure_datetime,
        p.procedure_type_concept_id,
        p.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            p.person_id,
            pc.concept_id procedure_concept_id,
            Accident_And_Emergency_Investigation_Eleventh procedure_source_value,	
            PARSE_DATE('%Y%m%d', p.Arrival_Date) procedure_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.Arrival_Date ,'000000')) procedure_datetime,
            32817 procedure_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` p
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` pc on pc.source_value = REPLACE(p.Accident_And_Emergency_Investigation_Eleventh, ' ', '') AND pc.destination_table = 'PROCEDURE_OCCURRENCE' AND pc.source_table = 'SUS_BRI_AE_010415_to_300619' AND pc.source_column = 'Accident_And_Emergency_Investigation_Eleventh'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl ON vl.source_value = CONCAT(CAST(p.AandE_Attendance_Number as STRING),'_', p.person_id)
        WHERE p.Arrival_Date IS NOT NULL
        AND p.person_id IS NOT NULL
        AND p.Accident_And_Emergency_Investigation_Eleventh IS NOT NULL
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.procedure_occurrence` op ON 
        op.person_id = p.person_id 
        AND op.procedure_concept_id = p.procedure_concept_id
        AND op.procedure_source_value = p.procedure_source_value
        AND COALESCE(op.procedure_source_concept_id, -1) = -1
        AND op.procedure_datetime = p.procedure_datetime
        AND op.procedure_type_concept_id = p.procedure_type_concept_id
        AND COALESCE(op.provider_id, -1) = -1
        AND COALESCE(op.visit_occurrence_id, -1) = COALESCE(p.visit_occurrence_id, -1)
        AND COALESCE(op.visit_detail_id, -1) = -1
    WHERE op.procedure_occurrence_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added procedure_occurrence', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Eleventh -> procedure_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Twelfth] Transfer
    */
    SET max_current_id = COALESCE((SELECT procedure_occurrence_id FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.procedure_occurrence`
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_source_value,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as procedure_occurrence_id,
        p.person_id,
        p.procedure_concept_id,
        p.procedure_source_value,
        p.procedure_date,
        p.procedure_datetime,
        p.procedure_type_concept_id,
        p.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            p.person_id,
            pc.concept_id procedure_concept_id,
            Accident_And_Emergency_Investigation_Twelfth procedure_source_value,	
            PARSE_DATE('%Y%m%d', p.Arrival_Date) procedure_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.Arrival_Date ,'000000')) procedure_datetime,
            32817 procedure_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` p
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` pc on pc.source_value = REPLACE(p.Accident_And_Emergency_Investigation_Twelfth, ' ', '') AND pc.destination_table = 'PROCEDURE_OCCURRENCE' AND pc.source_table = 'SUS_BRI_AE_010415_to_300619' AND pc.source_column = 'Accident_And_Emergency_Investigation_Twelfth'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl ON vl.source_value = CONCAT(CAST(p.AandE_Attendance_Number as STRING),'_', p.person_id)
        WHERE p.Arrival_Date IS NOT NULL
        AND p.person_id IS NOT NULL
        AND p.Accident_And_Emergency_Investigation_Twelfth IS NOT NULL
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.procedure_occurrence` op ON 
        op.person_id = p.person_id 
        AND op.procedure_concept_id = p.procedure_concept_id
        AND op.procedure_source_value = p.procedure_source_value
        AND COALESCE(op.procedure_source_concept_id, -1) = -1
        AND op.procedure_datetime = p.procedure_datetime
        AND op.procedure_type_concept_id = p.procedure_type_concept_id
        AND COALESCE(op.provider_id, -1) = -1
        AND COALESCE(op.visit_occurrence_id, -1) = COALESCE(p.visit_occurrence_id, -1)
        AND COALESCE(op.visit_detail_id, -1) = -1
    WHERE op.procedure_occurrence_id IS NULL;         

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added procedure_occurrence', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Twelfth -> procedure_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_First] Transfer
    */
    SET max_current_id = COALESCE((SELECT procedure_occurrence_id FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.procedure_occurrence`
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_source_value,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as procedure_occurrence_id,
        p.person_id,
        p.procedure_concept_id,
        p.procedure_source_value,
        p.procedure_date,
        p.procedure_datetime,
        p.procedure_type_concept_id,
        p.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
                p.person_id,
                pc.concept_id procedure_concept_id,
                Accident_And_Emergency_Treatment_First procedure_source_value,	
                PARSE_DATE('%Y%m%d', p.Arrival_Date) procedure_date,
                PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.Arrival_Date ,'000000')) procedure_datetime,
                32817 procedure_type_concept_id, --EHR
                vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` p
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` pc on pc.source_value = REPLACE(p.Accident_And_Emergency_Treatment_First, ' ', '') AND pc.destination_table = 'PROCEDURE_OCCURRENCE' AND pc.source_table = 'SUS_BRI_AE_010415_to_300619' AND pc.source_column = 'Accident_And_Emergency_Treatment_First'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl ON vl.source_value = CONCAT(CAST(p.AandE_Attendance_Number as STRING),'_', p.person_id)
        WHERE p.Arrival_Date IS NOT NULL
        AND p.person_id IS NOT NULL
        AND p.Accident_And_Emergency_Treatment_First IS NOT NULL
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.procedure_occurrence` op ON 
        op.person_id = p.person_id 
        AND op.procedure_concept_id = p.procedure_concept_id
        AND op.procedure_source_value = p.procedure_source_value
        AND COALESCE(op.procedure_source_concept_id, -1) = -1
        AND op.procedure_datetime = p.procedure_datetime
        AND op.procedure_type_concept_id = p.procedure_type_concept_id
        AND COALESCE(op.provider_id, -1) = -1
        AND COALESCE(op.visit_occurrence_id, -1) = COALESCE(p.visit_occurrence_id, -1)
        AND COALESCE(op.visit_detail_id, -1) = -1
    WHERE op.procedure_occurrence_id IS NULL;      

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added procedure_occurrence', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_First -> procedure_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Second] Transfer
    */
    SET max_current_id = COALESCE((SELECT procedure_occurrence_id FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.procedure_occurrence`
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_source_value,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as procedure_occurrence_id,
        p.person_id,
        p.procedure_concept_id,
        p.procedure_source_value,
        p.procedure_date,
        p.procedure_datetime,
        p.procedure_type_concept_id,
        p.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            p.person_id,
            pc.concept_id procedure_concept_id,
            Accident_And_Emergency_Treatment_Second procedure_source_value,	
            PARSE_DATE('%Y%m%d', p.Arrival_Date) procedure_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.Arrival_Date ,'000000')) procedure_datetime,
            32817 procedure_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` p
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` pc on pc.source_value = REPLACE(p.Accident_And_Emergency_Treatment_Second, ' ', '') AND pc.destination_table = 'PROCEDURE_OCCURRENCE' AND pc.source_table = 'SUS_BRI_AE_010415_to_300619' AND pc.source_column = 'Accident_And_Emergency_Treatment_Second'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl ON vl.source_value = CONCAT(CAST(p.AandE_Attendance_Number as STRING),'_', p.person_id)
        WHERE p.Arrival_Date IS NOT NULL
        AND p.person_id IS NOT NULL
        AND p.Accident_And_Emergency_Treatment_Second IS NOT NULL
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.procedure_occurrence` op ON 
        op.person_id = p.person_id 
        AND op.procedure_concept_id = p.procedure_concept_id
        AND op.procedure_source_value = p.procedure_source_value
        AND COALESCE(op.procedure_source_concept_id, -1) = -1
        AND op.procedure_datetime = p.procedure_datetime
        AND op.procedure_type_concept_id = p.procedure_type_concept_id
        AND COALESCE(op.provider_id, -1) = -1
        AND COALESCE(op.visit_occurrence_id, -1) = COALESCE(p.visit_occurrence_id, -1)
        AND COALESCE(op.visit_detail_id, -1) = -1
    WHERE op.procedure_occurrence_id IS NULL;   

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added procedure_occurrence', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Second -> procedure_occurrence', @@row_count, CURRENT_DATETIME();
     
    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Third] Transfer
    */
    SET max_current_id = COALESCE((SELECT procedure_occurrence_id FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.procedure_occurrence`
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_source_value,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as procedure_occurrence_id,
        p.person_id,
        p.procedure_concept_id,
        p.procedure_source_value,
        p.procedure_date,
        p.procedure_datetime,
        p.procedure_type_concept_id,
        p.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            p.person_id,
            pc.concept_id procedure_concept_id,
            Accident_And_Emergency_Treatment_Third procedure_source_value,	
            PARSE_DATE('%Y%m%d', p.Arrival_Date) procedure_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.Arrival_Date ,'000000')) procedure_datetime,
            32817 procedure_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` p
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` pc on pc.source_value = REPLACE(p.Accident_And_Emergency_Treatment_Third, ' ', '') AND pc.destination_table = 'PROCEDURE_OCCURRENCE' AND pc.source_table = 'SUS_BRI_AE_010415_to_300619' AND pc.source_column = 'Accident_And_Emergency_Treatment_Third'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl ON vl.source_value = CONCAT(CAST(p.AandE_Attendance_Number as STRING),'_', p.person_id)
        WHERE p.Arrival_Date IS NOT NULL
        AND p.person_id IS NOT NULL
        AND p.Accident_And_Emergency_Treatment_Third IS NOT NULL
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.procedure_occurrence` op ON 
        op.person_id = p.person_id 
        AND op.procedure_concept_id = p.procedure_concept_id
        AND op.procedure_source_value = p.procedure_source_value
        AND COALESCE(op.procedure_source_concept_id, -1) = -1
        AND op.procedure_datetime = p.procedure_datetime
        AND op.procedure_type_concept_id = p.procedure_type_concept_id
        AND COALESCE(op.provider_id, -1) = -1
        AND COALESCE(op.visit_occurrence_id, -1) = COALESCE(p.visit_occurrence_id, -1)
        AND COALESCE(op.visit_detail_id, -1) = -1
    WHERE op.procedure_occurrence_id IS NULL; 

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added procedure_occurrence', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Third -> procedure_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Fourth] Transfer
    */
    SET max_current_id = COALESCE((SELECT procedure_occurrence_id FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.procedure_occurrence`
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_source_value,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as procedure_occurrence_id,
        p.person_id,
        p.procedure_concept_id,
        p.procedure_source_value,
        p.procedure_date,
        p.procedure_datetime,
        p.procedure_type_concept_id,
        p.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            p.person_id,
            pc.concept_id procedure_concept_id,
            Accident_And_Emergency_Treatment_Fourth procedure_source_value,	
            PARSE_DATE('%Y%m%d', p.Arrival_Date) procedure_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.Arrival_Date ,'000000')) procedure_datetime,
            32817 procedure_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` p
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` pc on pc.source_value = REPLACE(p.Accident_And_Emergency_Treatment_Fourth, ' ', '') AND pc.destination_table = 'PROCEDURE_OCCURRENCE' AND pc.source_table = 'SUS_BRI_AE_010415_to_300619' AND pc.source_column = 'Accident_And_Emergency_Treatment_Fourth'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl ON vl.source_value = CONCAT(CAST(p.AandE_Attendance_Number as STRING),'_', p.person_id)
        WHERE p.Arrival_Date IS NOT NULL
        AND p.person_id IS NOT NULL
        AND p.Accident_And_Emergency_Treatment_Fourth IS NOT NULL
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.procedure_occurrence` op ON 
        op.person_id = p.person_id 
        AND op.procedure_concept_id = p.procedure_concept_id
        AND op.procedure_source_value = p.procedure_source_value
        AND COALESCE(op.procedure_source_concept_id, -1) = -1
        AND op.procedure_datetime = p.procedure_datetime
        AND op.procedure_type_concept_id = p.procedure_type_concept_id
        AND COALESCE(op.provider_id, -1) = -1
        AND COALESCE(op.visit_occurrence_id, -1) = COALESCE(p.visit_occurrence_id, -1)
        AND COALESCE(op.visit_detail_id, -1) = -1
    WHERE op.procedure_occurrence_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added procedure_occurrence', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Fourth -> procedure_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Five] Transfer
    */
    SET max_current_id = COALESCE((SELECT procedure_occurrence_id FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.procedure_occurrence`
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_source_value,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as procedure_occurrence_id,
        p.person_id,
        p.procedure_concept_id,
        p.procedure_source_value,
        p.procedure_date,
        p.procedure_datetime,
        p.procedure_type_concept_id,
        p.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            p.person_id,
            pc.concept_id procedure_concept_id,
            Accident_And_Emergency_Treatment_Fifth procedure_source_value,	
            PARSE_DATE('%Y%m%d', p.Arrival_Date) procedure_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.Arrival_Date ,'000000')) procedure_datetime,
            32817 procedure_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` p
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` pc on pc.source_value = REPLACE(p.Accident_And_Emergency_Treatment_Fifth, ' ', '') AND pc.destination_table = 'PROCEDURE_OCCURRENCE' AND pc.source_table = 'SUS_BRI_AE_010415_to_300619' AND pc.source_column = 'Accident_And_Emergency_Treatment_Fifth'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl ON vl.source_value = CONCAT(CAST(p.AandE_Attendance_Number as STRING),'_', p.person_id)
        WHERE p.Arrival_Date IS NOT NULL
        AND p.person_id IS NOT NULL
        AND p.Accident_And_Emergency_Treatment_Fifth IS NOT NULL
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.procedure_occurrence` op ON 
        op.person_id = p.person_id 
        AND op.procedure_concept_id = p.procedure_concept_id
        AND op.procedure_source_value = p.procedure_source_value
        AND COALESCE(op.procedure_source_concept_id, -1) = -1
        AND op.procedure_datetime = p.procedure_datetime
        AND op.procedure_type_concept_id = p.procedure_type_concept_id
        AND COALESCE(op.provider_id, -1) = -1
        AND COALESCE(op.visit_occurrence_id, -1) = COALESCE(p.visit_occurrence_id, -1)
        AND COALESCE(op.visit_detail_id, -1) = -1
    WHERE op.procedure_occurrence_id IS NULL;  

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added procedure_occurrence', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Five -> procedure_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Sixth] Transfer
    */
    SET max_current_id = COALESCE((SELECT procedure_occurrence_id FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.procedure_occurrence`
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_source_value,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as procedure_occurrence_id,
        p.person_id,
        p.procedure_concept_id,
        p.procedure_source_value,
        p.procedure_date,
        p.procedure_datetime,
        p.procedure_type_concept_id,
        p.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            p.person_id,
            pc.concept_id procedure_concept_id,
            Accident_And_Emergency_Treatment_Sixth procedure_source_value,	
            PARSE_DATE('%Y%m%d', p.Arrival_Date) procedure_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.Arrival_Date ,'000000')) procedure_datetime,
            32817 procedure_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` p
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` pc on pc.source_value = REPLACE(p.Accident_And_Emergency_Treatment_Sixth, ' ', '') AND pc.destination_table = 'PROCEDURE_OCCURRENCE' AND pc.source_table = 'SUS_BRI_AE_010415_to_300619' AND pc.source_column = 'Accident_And_Emergency_Treatment_Sixth'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl ON vl.source_value = CONCAT(CAST(p.AandE_Attendance_Number as STRING),'_', p.person_id)
        WHERE p.Arrival_Date IS NOT NULL
        AND p.person_id IS NOT NULL
        AND p.Accident_And_Emergency_Treatment_Sixth IS NOT NULL
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.procedure_occurrence` op ON 
        op.person_id = p.person_id 
        AND op.procedure_concept_id = p.procedure_concept_id
        AND op.procedure_source_value = p.procedure_source_value
        AND COALESCE(op.procedure_source_concept_id, -1) = -1
        AND op.procedure_datetime = p.procedure_datetime
        AND op.procedure_type_concept_id = p.procedure_type_concept_id
        AND COALESCE(op.provider_id, -1) = -1
        AND COALESCE(op.visit_occurrence_id, -1) = COALESCE(p.visit_occurrence_id, -1)
        AND COALESCE(op.visit_detail_id, -1) = -1
    WHERE op.procedure_occurrence_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added procedure_occurrence', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Sixth -> procedure_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Seventh] Transfer
    */
    SET max_current_id = COALESCE((SELECT procedure_occurrence_id FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.procedure_occurrence`
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_source_value,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as procedure_occurrence_id,
        p.person_id,
        p.procedure_concept_id,
        p.procedure_source_value,
        p.procedure_date,
        p.procedure_datetime,
        p.procedure_type_concept_id,
        p.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            p.person_id,
            pc.concept_id procedure_concept_id,
            Accident_And_Emergency_Treatment_Seventh procedure_source_value,	
            PARSE_DATE('%Y%m%d', p.Arrival_Date) procedure_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.Arrival_Date ,'000000')) procedure_datetime,
            32817 procedure_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` p
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` pc on pc.source_value = REPLACE(p.Accident_And_Emergency_Treatment_Seventh, ' ', '') AND pc.destination_table = 'PROCEDURE_OCCURRENCE' AND pc.source_table = 'SUS_BRI_AE_010415_to_300619' AND pc.source_column = 'Accident_And_Emergency_Treatment_Seventh'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl ON vl.source_value = CONCAT(CAST(p.AandE_Attendance_Number as STRING),'_', p.person_id)
        WHERE p.Arrival_Date IS NOT NULL
        AND p.person_id IS NOT NULL
        AND p.Accident_And_Emergency_Treatment_Seventh IS NOT NULL
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.procedure_occurrence` op ON 
        op.person_id = p.person_id 
        AND op.procedure_concept_id = p.procedure_concept_id
        AND op.procedure_source_value = p.procedure_source_value
        AND COALESCE(op.procedure_source_concept_id, -1) = -1
        AND op.procedure_datetime = p.procedure_datetime
        AND op.procedure_type_concept_id = p.procedure_type_concept_id
        AND COALESCE(op.provider_id, -1) = -1
        AND COALESCE(op.visit_occurrence_id, -1) = COALESCE(p.visit_occurrence_id, -1)
        AND COALESCE(op.visit_detail_id, -1) = -1
    WHERE op.procedure_occurrence_id IS NULL;     

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added procedure_occurrence', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Seventh -> procedure_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Eighth] Transfer
    */
    SET max_current_id = COALESCE((SELECT procedure_occurrence_id FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.procedure_occurrence`
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_source_value,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as procedure_occurrence_id,
        p.person_id,
        p.procedure_concept_id,
        p.procedure_source_value,
        p.procedure_date,
        p.procedure_datetime,
        p.procedure_type_concept_id,
        p.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            p.person_id,
            pc.concept_id procedure_concept_id,
            Accident_And_Emergency_Treatment_Eighth procedure_source_value,	
            PARSE_DATE('%Y%m%d', p.Arrival_Date) procedure_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.Arrival_Date ,'000000')) procedure_datetime,
            32817 procedure_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` p
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` pc on pc.source_value = REPLACE(p.Accident_And_Emergency_Treatment_Eighth, ' ', '') AND pc.destination_table = 'PROCEDURE_OCCURRENCE' AND pc.source_table = 'SUS_BRI_AE_010415_to_300619' AND pc.source_column = 'Accident_And_Emergency_Treatment_Eighth'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl ON vl.source_value = CONCAT(CAST(p.AandE_Attendance_Number as STRING),'_', p.person_id)
        WHERE p.Arrival_Date IS NOT NULL
        AND p.person_id IS NOT NULL
        AND p.Accident_And_Emergency_Treatment_Eighth IS NOT NULL
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.procedure_occurrence` op ON 
        op.person_id = p.person_id 
        AND op.procedure_concept_id = p.procedure_concept_id
        AND op.procedure_source_value = p.procedure_source_value
        AND COALESCE(op.procedure_source_concept_id, -1) = -1
        AND op.procedure_datetime = p.procedure_datetime
        AND op.procedure_type_concept_id = p.procedure_type_concept_id
        AND COALESCE(op.provider_id, -1) = -1
        AND COALESCE(op.visit_occurrence_id, -1) = COALESCE(p.visit_occurrence_id, -1)
        AND COALESCE(op.visit_detail_id, -1) = -1
    WHERE op.procedure_occurrence_id IS NULL; 

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added procedure_occurrence', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Eighth -> procedure_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Ninth] Transfer
    */
    SET max_current_id = COALESCE((SELECT procedure_occurrence_id FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.procedure_occurrence`
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_source_value,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as procedure_occurrence_id,
        p.person_id,
        p.procedure_concept_id,
        p.procedure_source_value,
        p.procedure_date,
        p.procedure_datetime,
        p.procedure_type_concept_id,
        p.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            p.person_id,
            pc.concept_id procedure_concept_id,
            Accident_And_Emergency_Treatment_Ninth procedure_source_value,	
            PARSE_DATE('%Y%m%d', p.Arrival_Date) procedure_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.Arrival_Date ,'000000')) procedure_datetime,
            32817 procedure_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` p
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` pc on pc.source_value = REPLACE(p.Accident_And_Emergency_Treatment_Ninth, ' ', '') AND pc.destination_table = 'PROCEDURE_OCCURRENCE' AND pc.source_table = 'SUS_BRI_AE_010415_to_300619' AND pc.source_column = 'Accident_And_Emergency_Treatment_Ninth'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl ON vl.source_value = CONCAT(CAST(p.AandE_Attendance_Number as STRING),'_', p.person_id)
        WHERE p.Arrival_Date IS NOT NULL
        AND p.person_id IS NOT NULL
        AND p.Accident_And_Emergency_Treatment_Ninth IS NOT NULL
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.procedure_occurrence` op ON 
        op.person_id = p.person_id 
        AND op.procedure_concept_id = p.procedure_concept_id
        AND op.procedure_source_value = p.procedure_source_value
        AND COALESCE(op.procedure_source_concept_id, -1) = -1
        AND op.procedure_datetime = p.procedure_datetime
        AND op.procedure_type_concept_id = p.procedure_type_concept_id
        AND COALESCE(op.provider_id, -1) = -1
        AND COALESCE(op.visit_occurrence_id, -1) = COALESCE(p.visit_occurrence_id, -1)
        AND COALESCE(op.visit_detail_id, -1) = -1
    WHERE op.procedure_occurrence_id IS NULL;  

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added procedure_occurrence', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Ninth -> procedure_occurrence', @@row_count, CURRENT_DATETIME();
     
    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Tenth] Transfer
    */
    SET max_current_id = COALESCE((SELECT procedure_occurrence_id FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.procedure_occurrence`
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_source_value,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as procedure_occurrence_id,
        p.person_id,
        p.procedure_concept_id,
        p.procedure_source_value,
        p.procedure_date,
        p.procedure_datetime,
        p.procedure_type_concept_id,
        p.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            p.person_id,
            pc.concept_id procedure_concept_id,
            Accident_And_Emergency_Treatment_Tenth procedure_source_value,	
            PARSE_DATE('%Y%m%d', p.Arrival_Date) procedure_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.Arrival_Date ,'000000')) procedure_datetime,
            32817 procedure_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` p
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` pc on pc.source_value = REPLACE(p.Accident_And_Emergency_Treatment_Tenth, ' ', '') AND pc.destination_table = 'PROCEDURE_OCCURRENCE' AND pc.source_table = 'SUS_BRI_AE_010415_to_300619' AND pc.source_column = 'Accident_And_Emergency_Treatment_Tenth'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl ON vl.source_value = CONCAT(CAST(p.AandE_Attendance_Number as STRING),'_', p.person_id)
        WHERE p.Arrival_Date IS NOT NULL
        AND p.person_id IS NOT NULL
        AND p.Accident_And_Emergency_Treatment_Tenth IS NOT NULL
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.procedure_occurrence` op ON 
        op.person_id = p.person_id 
        AND op.procedure_concept_id = p.procedure_concept_id
        AND op.procedure_source_value = p.procedure_source_value
        AND COALESCE(op.procedure_source_concept_id, -1) = -1
        AND op.procedure_datetime = p.procedure_datetime
        AND op.procedure_type_concept_id = p.procedure_type_concept_id
        AND COALESCE(op.provider_id, -1) = -1
        AND COALESCE(op.visit_occurrence_id, -1) = COALESCE(p.visit_occurrence_id, -1)
        AND COALESCE(op.visit_detail_id, -1) = -1
    WHERE op.procedure_occurrence_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added procedure_occurrence', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Tenth -> procedure_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Eleventh] Transfer
    */
    SET max_current_id = COALESCE((SELECT procedure_occurrence_id FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.procedure_occurrence`
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_source_value,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as procedure_occurrence_id,
        p.person_id,
        p.procedure_concept_id,
        p.procedure_source_value,
        p.procedure_date,
        p.procedure_datetime,
        p.procedure_type_concept_id,
        p.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            p.person_id,
            pc.concept_id procedure_concept_id,
            Accident_And_Emergency_Treatment_Eleventh procedure_source_value,	
            PARSE_DATE('%Y%m%d', p.Arrival_Date) procedure_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.Arrival_Date ,'000000')) procedure_datetime,
            32817 procedure_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` p
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` pc on pc.source_value = REPLACE(p.Accident_And_Emergency_Treatment_Eleventh, ' ', '') AND pc.destination_table = 'PROCEDURE_OCCURRENCE' AND pc.source_table = 'SUS_BRI_AE_010415_to_300619' AND pc.source_column = 'Accident_And_Emergency_Treatment_Eleventh'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl ON vl.source_value = CONCAT(CAST(p.AandE_Attendance_Number as STRING),'_', p.person_id)
        WHERE p.Arrival_Date IS NOT NULL
        AND p.person_id IS NOT NULL
        AND p.Accident_And_Emergency_Treatment_Eleventh IS NOT NULL
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.procedure_occurrence` op ON 
        op.person_id = p.person_id 
        AND op.procedure_concept_id = p.procedure_concept_id
        AND op.procedure_source_value = p.procedure_source_value
        AND COALESCE(op.procedure_source_concept_id, -1) = -1
        AND op.procedure_datetime = p.procedure_datetime
        AND op.procedure_type_concept_id = p.procedure_type_concept_id
        AND COALESCE(op.provider_id, -1) = -1
        AND COALESCE(op.visit_occurrence_id, -1) = COALESCE(p.visit_occurrence_id, -1)
        AND COALESCE(op.visit_detail_id, -1) = -1
    WHERE op.procedure_occurrence_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added procedure_occurrence', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Eleventh -> procedure_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Twelfth] Transfer
    */
    SET max_current_id = COALESCE((SELECT procedure_occurrence_id FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.procedure_occurrence`
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_source_value,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as procedure_occurrence_id,
        p.person_id,
        p.procedure_concept_id,
        p.procedure_source_value,
        p.procedure_date,
        p.procedure_datetime,
        p.procedure_type_concept_id,
        p.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            p.person_id,
            pc.concept_id procedure_concept_id,
            Accident_And_Emergency_Treatment_Twelfth procedure_source_value,	
            PARSE_DATE('%Y%m%d', p.Arrival_Date) procedure_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.Arrival_Date ,'000000')) procedure_datetime,
            32817 procedure_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` p
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` pc on pc.source_value = REPLACE(p.Accident_And_Emergency_Treatment_Twelfth, ' ', '') AND pc.destination_table = 'PROCEDURE_OCCURRENCE' AND pc.source_table = 'SUS_BRI_AE_010415_to_300619' AND pc.source_column = 'Accident_And_Emergency_Treatment_Twelfth'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl ON vl.source_value = CONCAT(CAST(p.AandE_Attendance_Number as STRING),'_', p.person_id)
        WHERE p.Arrival_Date IS NOT NULL
        AND p.person_id IS NOT NULL
        AND p.Accident_And_Emergency_Treatment_Twelfth IS NOT NULL
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.procedure_occurrence` op ON 
        op.person_id = p.person_id 
        AND op.procedure_concept_id = p.procedure_concept_id
        AND op.procedure_source_value = p.procedure_source_value
        AND COALESCE(op.procedure_source_concept_id, -1) = -1
        AND op.procedure_datetime = p.procedure_datetime
        AND op.procedure_type_concept_id = p.procedure_type_concept_id
        AND COALESCE(op.provider_id, -1) = -1
        AND COALESCE(op.visit_occurrence_id, -1) = COALESCE(p.visit_occurrence_id, -1)
        AND COALESCE(op.visit_detail_id, -1) = -1
    WHERE op.procedure_occurrence_id IS NULL; 

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added procedure_occurrence', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Twelfth -> procedure_occurrence', @@row_count, CURRENT_DATETIME();

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    ------------[SUS_BRI_APC_010415_to_300619_P1]
    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

    /*
        --[SUS_BRI_APC_010415_to_300619_P1 - Primary_Procedure_OPCS] Transfer
    */
    SET max_current_id = COALESCE((SELECT procedure_occurrence_id FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.procedure_occurrence`
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_source_value,
        procedure_source_concept_id,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        visit_occurrence_id,
        visit_detail_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as procedure_occurrence_id,
        p.person_id,
        p.procedure_concept_id,
        p.procedure_source_value,
        p.procedure_source_concept_id,
        p.procedure_date,
        p.procedure_datetime,
        p.procedure_type_concept_id,
        p.visit_occurrence_id,
        p.visit_detail_id
    FROM
    (
        SELECT DISTINCT
            p.person_id person_id,
            dc.concept_id procedure_concept_id, 
            Primary_Procedure_OPCS procedure_source_value, 
            sc.concept_id  procedure_source_concept_id,
            CASE WHEN LENGTH(Primary_Procedure_Date_OPCS) = 6
                    THEN PARSE_DATE('%Y%m%d', CONCAT(p.Primary_Procedure_Date_OPCS, '01'))
                WHEN LENGTH(Primary_Procedure_Date_OPCS) = 8
                    THEN PARSE_DATE('%Y%m%d', p.Primary_Procedure_Date_OPCS)
            END procedure_date,
            CASE WHEN LENGTH(Primary_Procedure_Date_OPCS) = 6
                    THEN PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.Primary_Procedure_Date_OPCS ,'01000000'))
                WHEN LENGTH(Primary_Procedure_Date_OPCS) = 8
                    THEN PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.Primary_Procedure_Date_OPCS ,'000000'))
            END procedure_datetime,       
            32817 procedure_type_concept_id, --EHR
            vl.id visit_occurrence_id,
            vdl.id visit_detail_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1` p   
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = CONCAT(CAST(p.Hospital_Provider_Spell_Number as STRING),'_', p.person_id)
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups`) vdl On vdl.source_value = CONCAT(CAST(p.Hospital_Provider_Spell_Number as STRING),'_', p.person_id, '_', p.Episode_Number)
        JOIN `CY_CDM_VOCAB.concept` sc on REPLACE(sc.concept_code, '.', '') = p.Primary_Procedure_OPCS
        JOIN `CY_CDM_VOCAB.concept_relationship` cr on cr.concept_id_1 = sc.concept_id
        JOIN `CY_CDM_VOCAB.concept` dc on dc.concept_id = cr.concept_id_2    
        WHERE (p.Primary_Procedure_Date_OPCS IS NOT NULL AND Primary_Procedure_Date_OPCS NOT LIKE '% %' AND (LENGTH(Primary_Procedure_Date_OPCS) =6 OR LENGTH(Primary_Procedure_Date_OPCS) =8))
        AND p.person_id IS NOT NULL
        AND p.Primary_Procedure_OPCS IS NOT NULL
        AND cr.relationship_id = 'Maps to'
        AND sc.vocabulary_id = 'OPCS4'
        AND dc.vocabulary_id = 'SNOMED'
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.procedure_occurrence` op ON 
        op.person_id = p.person_id 
        AND op.procedure_concept_id = p.procedure_concept_id
        AND op.procedure_source_value = p.procedure_source_value
        AND COALESCE(op.procedure_source_concept_id, -1) = COALESCE(p.procedure_source_concept_id, -1)
        AND op.procedure_datetime = p.procedure_datetime
        AND op.procedure_type_concept_id = p.procedure_type_concept_id
        AND COALESCE(op.provider_id, -1) = -1
        AND COALESCE(op.visit_occurrence_id, -1) = COALESCE(p.visit_occurrence_id, -1)
        AND COALESCE(op.visit_detail_id, -1) = COALESCE(p.visit_detail_id, -1)
    WHERE op.procedure_occurrence_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added procedure_occurrence', run_id, 'SUS_BRI_APC_010415_to_300619_P1 - Primary_Procedure_OPCS -> procedure_occurrence', @@row_count, CURRENT_DATETIME();
    
    /*
        --[SUS_BRI_APC_010415_to_300619_P1 - a2nd_Procedure_OPCS] Transfer
    */
    SET max_current_id = COALESCE((SELECT procedure_occurrence_id FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.procedure_occurrence`
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_source_value,
        procedure_source_concept_id,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        visit_occurrence_id,
        visit_detail_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as procedure_occurrence_id,
        p.person_id,
        p.procedure_concept_id,
        p.procedure_source_value,
        p.procedure_source_concept_id,
        p.procedure_date,
        p.procedure_datetime,
        p.procedure_type_concept_id,
        p.visit_occurrence_id,
        p.visit_detail_id
    FROM
    (
        SELECT DISTINCT
            p.person_id person_id,
            dc.concept_id procedure_concept_id, 
            a2nd_Procedure_OPCS procedure_source_value, 
            sc.concept_id  procedure_source_concept_id,
            CASE WHEN LENGTH(a2nd_Procedure_Date_OPCS) = 6
                    THEN PARSE_DATE('%Y%m%d', CONCAT(p.a2nd_Procedure_Date_OPCS, '01'))
                WHEN LENGTH(a2nd_Procedure_Date_OPCS) = 8
                    THEN PARSE_DATE('%Y%m%d', p.a2nd_Procedure_Date_OPCS)
            END procedure_date,
            CASE WHEN LENGTH(a2nd_Procedure_Date_OPCS) = 6
                    THEN PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.a2nd_Procedure_Date_OPCS ,'01000000'))
                WHEN LENGTH(a2nd_Procedure_Date_OPCS) = 8
                    THEN PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.a2nd_Procedure_Date_OPCS ,'000000'))
            END procedure_datetime,       
            32817 procedure_type_concept_id, --EHR
            vl.id visit_occurrence_id,
            vdl.id visit_detail_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1` p   
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = CONCAT(CAST(p.Hospital_Provider_Spell_Number as STRING),'_', p.person_id)
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups`) vdl On vdl.source_value = CONCAT(CAST(p.Hospital_Provider_Spell_Number as STRING),'_', p.person_id, '_', p.Episode_Number)
        JOIN `CY_CDM_VOCAB.concept` sc on REPLACE(sc.concept_code, '.', '') = p.a2nd_Procedure_OPCS
        JOIN `CY_CDM_VOCAB.concept_relationship` cr on cr.concept_id_1 = sc.concept_id
        JOIN `CY_CDM_VOCAB.concept` dc on dc.concept_id = cr.concept_id_2    
        WHERE (p.a2nd_Procedure_Date_OPCS IS NOT NULL AND a2nd_Procedure_Date_OPCS NOT LIKE '% %' AND (LENGTH(a2nd_Procedure_OPCS) =6 OR LENGTH(a2nd_Procedure_Date_OPCS) =8))
        AND p.person_id IS NOT NULL
        AND p.a2nd_Procedure_OPCS IS NOT NULL
        AND cr.relationship_id = 'Maps to'
        AND sc.vocabulary_id = 'OPCS4'
        AND dc.vocabulary_id = 'SNOMED'
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.procedure_occurrence` op ON 
        op.person_id = p.person_id 
        AND op.procedure_concept_id = p.procedure_concept_id
        AND op.procedure_source_value = p.procedure_source_value
        AND COALESCE(op.procedure_source_concept_id, -1) = COALESCE(p.procedure_source_concept_id, -1)
        AND op.procedure_datetime = p.procedure_datetime
        AND op.procedure_type_concept_id = p.procedure_type_concept_id
        AND COALESCE(op.provider_id, -1) = -1
        AND COALESCE(op.visit_occurrence_id, -1) = COALESCE(p.visit_occurrence_id, -1)
        AND COALESCE(op.visit_detail_id, -1) = COALESCE(p.visit_detail_id, -1)
    WHERE op.procedure_occurrence_id IS NULL; 

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added procedure_occurrence', run_id, 'SUS_BRI_APC_010415_to_300619_P1 - a2nd_Procedure_OPCS -> procedure_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_APC_010415_to_300619_P1 - a3rd_Procedure_OPCS] Transfer
    */
    SET max_current_id = COALESCE((SELECT procedure_occurrence_id FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.procedure_occurrence`
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_source_value,
        procedure_source_concept_id,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        visit_occurrence_id,
        visit_detail_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as procedure_occurrence_id,
        p.person_id,
        p.procedure_concept_id,
        p.procedure_source_value,
        p.procedure_source_concept_id,
        p.procedure_date,
        p.procedure_datetime,
        p.procedure_type_concept_id,
        p.visit_occurrence_id,
        p.visit_detail_id
    FROM
    (
        SELECT DISTINCT
            p.person_id person_id,
            dc.concept_id procedure_concept_id, 
            p.a3rd_Procedure_OPCS procedure_source_value, 
            sc.concept_id  procedure_source_concept_id,
            CASE WHEN LENGTH(p.a3rd_Procedure_Date_OPCS) = 6
                    THEN PARSE_DATE('%Y%m%d', CONCAT(p.a3rd_Procedure_Date_OPCS, '01'))
                WHEN LENGTH(a3rd_Procedure_Date_OPCS) = 8
                    THEN PARSE_DATE('%Y%m%d', p.a3rd_Procedure_Date_OPCS)
            END procedure_date,
            CASE WHEN LENGTH(a3rd_Procedure_Date_OPCS) = 6
                    THEN PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.a3rd_Procedure_Date_OPCS ,'01000000'))
                WHEN LENGTH(a3rd_Procedure_Date_OPCS) = 8
                    THEN PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.a3rd_Procedure_Date_OPCS ,'000000'))
            END procedure_datetime,       
            32817 procedure_type_concept_id, --EHR
            vl.id visit_occurrence_id,
            vdl.id visit_detail_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1` p   
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = CONCAT(CAST(p.Hospital_Provider_Spell_Number as STRING),'_', p.person_id)
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups`) vdl On vdl.source_value = CONCAT(CAST(p.Hospital_Provider_Spell_Number as STRING),'_', p.person_id, '_', p.Episode_Number)
        JOIN `CY_CDM_VOCAB.concept` sc on REPLACE(sc.concept_code, '.', '') = p.a3rd_Procedure_OPCS
        JOIN `CY_CDM_VOCAB.concept_relationship` cr on cr.concept_id_1 = sc.concept_id
        JOIN `CY_CDM_VOCAB.concept` dc on dc.concept_id = cr.concept_id_2    
        WHERE (p.a3rd_Procedure_Date_OPCS IS NOT NULL AND p.a3rd_Procedure_Date_OPCS NOT LIKE '% %' AND (LENGTH(p.a3rd_Procedure_Date_OPCS) =6 OR LENGTH(p.a3rd_Procedure_Date_OPCS) =8))
        AND p.person_id IS NOT NULL
        AND p.a3rd_Procedure_OPCS IS NOT NULL
        AND cr.relationship_id = 'Maps to'
        AND sc.vocabulary_id = 'OPCS4'
        AND dc.vocabulary_id = 'SNOMED'
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.procedure_occurrence` op ON 
        op.person_id = p.person_id 
        AND op.procedure_concept_id = p.procedure_concept_id
        AND op.procedure_source_value = p.procedure_source_value
        AND COALESCE(op.procedure_source_concept_id, -1) = COALESCE(p.procedure_source_concept_id, -1)
        AND op.procedure_datetime = p.procedure_datetime
        AND op.procedure_type_concept_id = p.procedure_type_concept_id
        AND COALESCE(op.provider_id, -1) = -1
        AND COALESCE(op.visit_occurrence_id, -1) = COALESCE(p.visit_occurrence_id, -1)
        AND COALESCE(op.visit_detail_id, -1) = COALESCE(p.visit_detail_id, -1)
    WHERE op.procedure_occurrence_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added procedure_occurrence', run_id, 'SUS_BRI_APC_010415_to_300619_P1 - a3rd_Procedure_OPCS -> procedure_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_APC_010415_to_300619_P1 - a4th_Procedure_OPCS] Transfer
    */
    SET max_current_id = COALESCE((SELECT procedure_occurrence_id FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.procedure_occurrence`
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_source_value,
        procedure_source_concept_id,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        visit_occurrence_id,
        visit_detail_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as procedure_occurrence_id,
        p.person_id,
        p.procedure_concept_id,
        p.procedure_source_value,
        p.procedure_source_concept_id,
        p.procedure_date,
        p.procedure_datetime,
        p.procedure_type_concept_id,
        p.visit_occurrence_id,
        p.visit_detail_id
    FROM
    (
        SELECT DISTINCT
            p.person_id person_id,
            dc.concept_id procedure_concept_id, 
            p.a4th_Procedure_OPCS procedure_source_value, 
            sc.concept_id  procedure_source_concept_id,
            CASE WHEN LENGTH(p.a4th_Procedure_Date_OPCS) = 6
                    THEN PARSE_DATE('%Y%m%d', CONCAT(p.a4th_Procedure_Date_OPCS, '01'))
                WHEN LENGTH(a4th_Procedure_Date_OPCS) = 8
                    THEN PARSE_DATE('%Y%m%d', p.a4th_Procedure_Date_OPCS)
            END procedure_date,
            CASE WHEN LENGTH(a4th_Procedure_Date_OPCS) = 6
                    THEN PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.a4th_Procedure_Date_OPCS ,'01000000'))
                WHEN LENGTH(a4th_Procedure_Date_OPCS) = 8
                    THEN PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.a4th_Procedure_Date_OPCS ,'000000'))
            END procedure_datetime,       
            32817 procedure_type_concept_id, --EHR
            vl.id visit_occurrence_id,
            vdl.id visit_detail_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1` p   
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = CONCAT(CAST(p.Hospital_Provider_Spell_Number as STRING),'_', p.person_id)
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups`) vdl On vdl.source_value = CONCAT(CAST(p.Hospital_Provider_Spell_Number as STRING),'_', p.person_id, '_', p.Episode_Number)
        JOIN `CY_CDM_VOCAB.concept` sc on REPLACE(sc.concept_code, '.', '') = p.a4th_Procedure_OPCS
        JOIN `CY_CDM_VOCAB.concept_relationship` cr on cr.concept_id_1 = sc.concept_id
        JOIN `CY_CDM_VOCAB.concept` dc on dc.concept_id = cr.concept_id_2    
        WHERE (p.a4th_Procedure_Date_OPCS IS NOT NULL AND p.a4th_Procedure_Date_OPCS NOT LIKE '% %' AND (LENGTH(p.a4th_Procedure_Date_OPCS) =6 OR LENGTH(p.a4th_Procedure_Date_OPCS) =8))
        AND p.person_id IS NOT NULL
        AND p.a4th_Procedure_OPCS IS NOT NULL
        AND cr.relationship_id = 'Maps to'
        AND sc.vocabulary_id = 'OPCS4'
        AND dc.vocabulary_id = 'SNOMED'
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.procedure_occurrence` op ON 
        op.person_id = p.person_id 
        AND op.procedure_concept_id = p.procedure_concept_id
        AND op.procedure_source_value = p.procedure_source_value
        AND COALESCE(op.procedure_source_concept_id, -1) = COALESCE(p.procedure_source_concept_id, -1)
        AND op.procedure_datetime = p.procedure_datetime
        AND op.procedure_type_concept_id = p.procedure_type_concept_id
        AND COALESCE(op.provider_id, -1) = -1
        AND COALESCE(op.visit_occurrence_id, -1) = COALESCE(p.visit_occurrence_id, -1)
        AND COALESCE(op.visit_detail_id, -1) = COALESCE(p.visit_detail_id, -1)
    WHERE op.procedure_occurrence_id IS NULL;


    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added procedure_occurrence', run_id, 'SUS_BRI_APC_010415_to_300619_P1 - a4th_Procedure_OPCS -> procedure_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_APC_010415_to_300619_P1 - a5th_Procedure_OPCS] Transfer
    */
    SET max_current_id = COALESCE((SELECT procedure_occurrence_id FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.procedure_occurrence`
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_source_value,
        procedure_source_concept_id,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        visit_occurrence_id,
        visit_detail_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as procedure_occurrence_id,
        p.person_id,
        p.procedure_concept_id,
        p.procedure_source_value,
        p.procedure_source_concept_id,
        p.procedure_date,
        p.procedure_datetime,
        p.procedure_type_concept_id,
        p.visit_occurrence_id,
        p.visit_detail_id
    FROM
    (
        SELECT DISTINCT
            p.person_id person_id,
            dc.concept_id procedure_concept_id, 
            p.a5th_Procedure_OPCS procedure_source_value, 
            sc.concept_id  procedure_source_concept_id,
            CASE WHEN LENGTH(p.a5th_Procedure_Date_OPCS) = 6
                    THEN PARSE_DATE('%Y%m%d', CONCAT(p.a5th_Procedure_Date_OPCS, '01'))
                WHEN LENGTH(a5th_Procedure_Date_OPCS) = 8
                    THEN PARSE_DATE('%Y%m%d', p.a5th_Procedure_Date_OPCS)
            END procedure_date,
            CASE WHEN LENGTH(a5th_Procedure_Date_OPCS) = 6
                    THEN PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.a5th_Procedure_Date_OPCS ,'01000000'))
                WHEN LENGTH(a5th_Procedure_Date_OPCS) = 8
                    THEN PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.a5th_Procedure_Date_OPCS ,'000000'))
            END procedure_datetime,       
            32817 procedure_type_concept_id, --EHR
            vl.id visit_occurrence_id,
            vdl.id visit_detail_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1` p   
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = CONCAT(CAST(p.Hospital_Provider_Spell_Number as STRING),'_', p.person_id)
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups`) vdl On vdl.source_value = CONCAT(CAST(p.Hospital_Provider_Spell_Number as STRING),'_', p.person_id, '_', p.Episode_Number)
        JOIN `CY_CDM_VOCAB.concept` sc on REPLACE(sc.concept_code, '.', '') = p.a5th_Procedure_OPCS
        JOIN `CY_CDM_VOCAB.concept_relationship` cr on cr.concept_id_1 = sc.concept_id
        JOIN `CY_CDM_VOCAB.concept` dc on dc.concept_id = cr.concept_id_2    
        WHERE (p.a5th_Procedure_Date_OPCS IS NOT NULL AND p.a5th_Procedure_Date_OPCS NOT LIKE '% %' AND (LENGTH(p.a5th_Procedure_Date_OPCS) =6 OR LENGTH(p.a5th_Procedure_Date_OPCS) =8))
        AND p.person_id IS NOT NULL
        AND p.a5th_Procedure_OPCS IS NOT NULL
        AND cr.relationship_id = 'Maps to'
        AND sc.vocabulary_id = 'OPCS4'
        AND dc.vocabulary_id = 'SNOMED'
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.procedure_occurrence` op ON 
        op.person_id = p.person_id 
        AND op.procedure_concept_id = p.procedure_concept_id
        AND op.procedure_source_value = p.procedure_source_value
        AND COALESCE(op.procedure_source_concept_id, -1) = COALESCE(p.procedure_source_concept_id, -1)
        AND op.procedure_datetime = p.procedure_datetime
        AND op.procedure_type_concept_id = p.procedure_type_concept_id
        AND COALESCE(op.provider_id, -1) = -1
        AND COALESCE(op.visit_occurrence_id, -1) = COALESCE(p.visit_occurrence_id, -1)
        AND COALESCE(op.visit_detail_id, -1) = COALESCE(p.visit_detail_id, -1)
    WHERE op.procedure_occurrence_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added procedure_occurrence', run_id, 'SUS_BRI_APC_010415_to_300619_P1 - a5th_Procedure_OPCS -> procedure_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_APC_010415_to_300619_P1 - a6th_Procedure_OPCS] Transfer
    */
    SET max_current_id = COALESCE((SELECT procedure_occurrence_id FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.procedure_occurrence`
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_source_value,
        procedure_source_concept_id,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        visit_occurrence_id,
        visit_detail_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as procedure_occurrence_id,
        p.person_id,
        p.procedure_concept_id,
        p.procedure_source_value,
        p.procedure_source_concept_id,
        p.procedure_date,
        p.procedure_datetime,
        p.procedure_type_concept_id,
        p.visit_occurrence_id,
        p.visit_detail_id
    FROM
    (
        SELECT DISTINCT
            p.person_id person_id,
            dc.concept_id procedure_concept_id, 
            p.a6th_Procedure_OPCS procedure_source_value, 
            sc.concept_id  procedure_source_concept_id,
            CASE WHEN LENGTH(p.a6th_Procedure_Date_OPCS) = 6
                    THEN PARSE_DATE('%Y%m%d', CONCAT(p.a6th_Procedure_Date_OPCS, '01'))
                WHEN LENGTH(a6th_Procedure_Date_OPCS) = 8
                    THEN PARSE_DATE('%Y%m%d', p.a6th_Procedure_Date_OPCS)
            END procedure_date,
            CASE WHEN LENGTH(a6th_Procedure_Date_OPCS) = 6
                    THEN PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.a6th_Procedure_Date_OPCS ,'01000000'))
                WHEN LENGTH(a6th_Procedure_Date_OPCS) = 8
                    THEN PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.a6th_Procedure_Date_OPCS ,'000000'))
            END procedure_datetime,       
            32817 procedure_type_concept_id, --EHR
            vl.id visit_occurrence_id,
            vdl.id visit_detail_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1` p   
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = CONCAT(CAST(p.Hospital_Provider_Spell_Number as STRING),'_', p.person_id)
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups`) vdl On vdl.source_value = CONCAT(CAST(p.Hospital_Provider_Spell_Number as STRING),'_', p.person_id, '_', p.Episode_Number)
        JOIN `CY_CDM_VOCAB.concept` sc on REPLACE(sc.concept_code, '.', '') = p.a6th_Procedure_OPCS
        JOIN `CY_CDM_VOCAB.concept_relationship` cr on cr.concept_id_1 = sc.concept_id
        JOIN `CY_CDM_VOCAB.concept` dc on dc.concept_id = cr.concept_id_2    
        WHERE (p.a6th_Procedure_Date_OPCS IS NOT NULL AND p.a6th_Procedure_Date_OPCS NOT LIKE '% %' AND (LENGTH(p.a6th_Procedure_Date_OPCS) =6 OR LENGTH(p.a6th_Procedure_Date_OPCS) =8))
        AND p.person_id IS NOT NULL
        AND p.a6th_Procedure_OPCS IS NOT NULL
        AND cr.relationship_id = 'Maps to'
        AND sc.vocabulary_id = 'OPCS4'
        AND dc.vocabulary_id = 'SNOMED'
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.procedure_occurrence` op ON 
        op.person_id = p.person_id 
        AND op.procedure_concept_id = p.procedure_concept_id
        AND op.procedure_source_value = p.procedure_source_value
        AND COALESCE(op.procedure_source_concept_id, -1) = COALESCE(p.procedure_source_concept_id, -1)
        AND op.procedure_datetime = p.procedure_datetime
        AND op.procedure_type_concept_id = p.procedure_type_concept_id
        AND COALESCE(op.provider_id, -1) = -1
        AND COALESCE(op.visit_occurrence_id, -1) = COALESCE(p.visit_occurrence_id, -1)
        AND COALESCE(op.visit_detail_id, -1) = COALESCE(p.visit_detail_id, -1)
    WHERE op.procedure_occurrence_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added procedure_occurrence', run_id, 'SUS_BRI_APC_010415_to_300619_P1 - a6th_Procedure_OPCS -> procedure_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_APC_010415_to_300619_P1 - a7th_Procedure_OPCS] Transfer
    */
    SET max_current_id = COALESCE((SELECT procedure_occurrence_id FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.procedure_occurrence`
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_source_value,
        procedure_source_concept_id,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        visit_occurrence_id,
        visit_detail_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as procedure_occurrence_id,
        p.person_id,
        p.procedure_concept_id,
        p.procedure_source_value,
        p.procedure_source_concept_id,
        p.procedure_date,
        p.procedure_datetime,
        p.procedure_type_concept_id,
        p.visit_occurrence_id,
        p.visit_detail_id
    FROM
    (
        SELECT DISTINCT
            p.person_id person_id,
            dc.concept_id procedure_concept_id, 
            p.a7th_Procedure_OPCS procedure_source_value, 
            sc.concept_id  procedure_source_concept_id,
            CASE WHEN LENGTH(p.a7th_Procedure_Date_OPCS) = 6
                    THEN PARSE_DATE('%Y%m%d', CONCAT(p.a7th_Procedure_Date_OPCS, '01'))
                WHEN LENGTH(a7th_Procedure_Date_OPCS) = 8
                    THEN PARSE_DATE('%Y%m%d', p.a7th_Procedure_Date_OPCS)
            END procedure_date,
            CASE WHEN LENGTH(a7th_Procedure_Date_OPCS) = 6
                    THEN PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.a7th_Procedure_Date_OPCS ,'01000000'))
                WHEN LENGTH(a7th_Procedure_Date_OPCS) = 8
                    THEN PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.a7th_Procedure_Date_OPCS ,'000000'))
            END procedure_datetime,       
            32817 procedure_type_concept_id, --EHR
            vl.id visit_occurrence_id,
            vdl.id visit_detail_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1` p   
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = CONCAT(CAST(p.Hospital_Provider_Spell_Number as STRING),'_', p.person_id)
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups`) vdl On vdl.source_value = CONCAT(CAST(p.Hospital_Provider_Spell_Number as STRING),'_', p.person_id, '_', p.Episode_Number)
        JOIN `CY_CDM_VOCAB.concept` sc on REPLACE(sc.concept_code, '.', '') = p.a7th_Procedure_OPCS
        JOIN `CY_CDM_VOCAB.concept_relationship` cr on cr.concept_id_1 = sc.concept_id
        JOIN `CY_CDM_VOCAB.concept` dc on dc.concept_id = cr.concept_id_2    
        WHERE (p.a7th_Procedure_Date_OPCS IS NOT NULL AND p.a7th_Procedure_Date_OPCS NOT LIKE '% %' AND (LENGTH(p.a7th_Procedure_Date_OPCS) =6 OR LENGTH(p.a7th_Procedure_Date_OPCS) =8))
        AND p.person_id IS NOT NULL
        AND p.a7th_Procedure_OPCS IS NOT NULL
        AND cr.relationship_id = 'Maps to'
        AND sc.vocabulary_id = 'OPCS4'
        AND dc.vocabulary_id = 'SNOMED'
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.procedure_occurrence` op ON 
        op.person_id = p.person_id 
        AND op.procedure_concept_id = p.procedure_concept_id
        AND op.procedure_source_value = p.procedure_source_value
        AND COALESCE(op.procedure_source_concept_id, -1) = COALESCE(p.procedure_source_concept_id, -1)
        AND op.procedure_datetime = p.procedure_datetime
        AND op.procedure_type_concept_id = p.procedure_type_concept_id
        AND COALESCE(op.provider_id, -1) = -1
        AND COALESCE(op.visit_occurrence_id, -1) = COALESCE(p.visit_occurrence_id, -1)
        AND COALESCE(op.visit_detail_id, -1) = COALESCE(p.visit_detail_id, -1)
    WHERE op.procedure_occurrence_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added procedure_occurrence', run_id, 'SUS_BRI_APC_010415_to_300619_P1 - a7th_Procedure_OPCS -> procedure_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_APC_010415_to_300619_P1 - a8th_Procedure_OPCS] Transfer
    */
    SET max_current_id = COALESCE((SELECT procedure_occurrence_id FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.procedure_occurrence`
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_source_value,
        procedure_source_concept_id,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        visit_occurrence_id,
        visit_detail_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as procedure_occurrence_id,
        p.person_id,
        p.procedure_concept_id,
        p.procedure_source_value,
        p.procedure_source_concept_id,
        p.procedure_date,
        p.procedure_datetime,
        p.procedure_type_concept_id,
        p.visit_occurrence_id,
        p.visit_detail_id
    FROM
    (
        SELECT DISTINCT
            p.person_id person_id,
            dc.concept_id procedure_concept_id, 
            p.a8th_Procedure_OPCS procedure_source_value, 
            sc.concept_id  procedure_source_concept_id,
            CASE WHEN LENGTH(p.a8th_Procedure_Date_OPCS) = 6
                    THEN PARSE_DATE('%Y%m%d', CONCAT(p.a8th_Procedure_Date_OPCS, '01'))
                WHEN LENGTH(a8th_Procedure_Date_OPCS) = 8
                    THEN PARSE_DATE('%Y%m%d', p.a8th_Procedure_Date_OPCS)
            END procedure_date,
            CASE WHEN LENGTH(a8th_Procedure_Date_OPCS) = 6
                    THEN PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.a8th_Procedure_Date_OPCS ,'01000000'))
                WHEN LENGTH(a8th_Procedure_Date_OPCS) = 8
                    THEN PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.a8th_Procedure_Date_OPCS ,'000000'))
            END procedure_datetime,       
            32817 procedure_type_concept_id, --EHR
            vl.id visit_occurrence_id,
            vdl.id visit_detail_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1` p   
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = CONCAT(CAST(p.Hospital_Provider_Spell_Number as STRING),'_', p.person_id)
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups`) vdl On vdl.source_value = CONCAT(CAST(p.Hospital_Provider_Spell_Number as STRING),'_', p.person_id, '_', p.Episode_Number)
        JOIN `CY_CDM_VOCAB.concept` sc on REPLACE(sc.concept_code, '.', '') = p.a8th_Procedure_OPCS
        JOIN `CY_CDM_VOCAB.concept_relationship` cr on cr.concept_id_1 = sc.concept_id
        JOIN `CY_CDM_VOCAB.concept` dc on dc.concept_id = cr.concept_id_2    
        WHERE (p.a8th_Procedure_Date_OPCS IS NOT NULL AND p.a8th_Procedure_Date_OPCS NOT LIKE '% %' AND (LENGTH(p.a8th_Procedure_Date_OPCS) =6 OR LENGTH(p.a8th_Procedure_Date_OPCS) =8))
        AND p.person_id IS NOT NULL
        AND p.a8th_Procedure_OPCS IS NOT NULL
        AND cr.relationship_id = 'Maps to'
        AND sc.vocabulary_id = 'OPCS4'
        AND dc.vocabulary_id = 'SNOMED'
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.procedure_occurrence` op ON 
        op.person_id = p.person_id 
        AND op.procedure_concept_id = p.procedure_concept_id
        AND op.procedure_source_value = p.procedure_source_value
        AND COALESCE(op.procedure_source_concept_id, -1) = COALESCE(p.procedure_source_concept_id, -1)
        AND op.procedure_datetime = p.procedure_datetime
        AND op.procedure_type_concept_id = p.procedure_type_concept_id
        AND COALESCE(op.provider_id, -1) = -1
        AND COALESCE(op.visit_occurrence_id, -1) = COALESCE(p.visit_occurrence_id, -1)
        AND COALESCE(op.visit_detail_id, -1) = COALESCE(p.visit_detail_id, -1)
    WHERE op.procedure_occurrence_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added procedure_occurrence', run_id, 'SUS_BRI_APC_010415_to_300619_P1 - a8th_Procedure_OPCS -> procedure_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_APC_010415_to_300619_P1 - a9th_Procedure_OPCS] Transfer
    */
    SET max_current_id = COALESCE((SELECT procedure_occurrence_id FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.procedure_occurrence`
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_source_value,
        procedure_source_concept_id,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        visit_occurrence_id,
        visit_detail_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as procedure_occurrence_id,
        p.person_id,
        p.procedure_concept_id,
        p.procedure_source_value,
        p.procedure_source_concept_id,
        p.procedure_date,
        p.procedure_datetime,
        p.procedure_type_concept_id,
        p.visit_occurrence_id,
        p.visit_detail_id
    FROM
    (
        SELECT DISTINCT
            p.person_id person_id,
            dc.concept_id procedure_concept_id, 
            p.a9th_Procedure_OPCS procedure_source_value, 
            sc.concept_id procedure_source_concept_id,
            CASE WHEN LENGTH(p.a9th_Procedure_Date_OPCS) = 6
                    THEN PARSE_DATE('%Y%m%d', CONCAT(p.a9th_Procedure_Date_OPCS, '01'))
                WHEN LENGTH(a9th_Procedure_Date_OPCS) = 8
                    THEN PARSE_DATE('%Y%m%d', p.a9th_Procedure_Date_OPCS)
            END procedure_date,
            CASE WHEN LENGTH(a9th_Procedure_Date_OPCS) = 6
                    THEN PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.a9th_Procedure_Date_OPCS ,'01000000'))
                WHEN LENGTH(a9th_Procedure_Date_OPCS) = 8
                    THEN PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.a9th_Procedure_Date_OPCS ,'000000'))
            END procedure_datetime,       
            32817 procedure_type_concept_id, --EHR
            vl.id visit_occurrence_id,
            vdl.id visit_detail_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1` p   
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = CONCAT(CAST(p.Hospital_Provider_Spell_Number as STRING),'_', p.person_id)
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups`) vdl On vdl.source_value = CONCAT(CAST(p.Hospital_Provider_Spell_Number as STRING),'_', p.person_id, '_', p.Episode_Number)
        JOIN `CY_CDM_VOCAB.concept` sc on REPLACE(sc.concept_code, '.', '') = p.a9th_Procedure_OPCS
        JOIN `CY_CDM_VOCAB.concept_relationship` cr on cr.concept_id_1 = sc.concept_id
        JOIN `CY_CDM_VOCAB.concept` dc on dc.concept_id = cr.concept_id_2    
        WHERE (p.a9th_Procedure_Date_OPCS IS NOT NULL AND p.a9th_Procedure_Date_OPCS NOT LIKE '% %' AND (LENGTH(p.a9th_Procedure_Date_OPCS) =6 OR LENGTH(p.a9th_Procedure_Date_OPCS) =8))
        AND p.person_id IS NOT NULL
        AND p.a9th_Procedure_OPCS IS NOT NULL
        AND cr.relationship_id = 'Maps to'
        AND sc.vocabulary_id = 'OPCS4'
        AND dc.vocabulary_id = 'SNOMED'
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.procedure_occurrence` op ON 
        op.person_id = p.person_id 
        AND op.procedure_concept_id = p.procedure_concept_id
        AND op.procedure_source_value = p.procedure_source_value
        AND COALESCE(op.procedure_source_concept_id, -1) = COALESCE(p.procedure_source_concept_id, -1)
        AND op.procedure_datetime = p.procedure_datetime
        AND op.procedure_type_concept_id = p.procedure_type_concept_id
        AND COALESCE(op.provider_id, -1) = -1
        AND COALESCE(op.visit_occurrence_id, -1) = COALESCE(p.visit_occurrence_id, -1)
        AND COALESCE(op.visit_detail_id, -1) = COALESCE(p.visit_detail_id, -1)
    WHERE op.procedure_occurrence_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added procedure_occurrence', run_id, 'SUS_BRI_APC_010415_to_300619_P1 - a9th_Procedure_OPCS -> procedure_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_APC_010415_to_300619_P1 - a10th_Procedure_OPCS] Transfer
    */
    SET max_current_id = COALESCE((SELECT procedure_occurrence_id FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.procedure_occurrence`
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_source_value,
        procedure_source_concept_id,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        visit_occurrence_id,
        visit_detail_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as procedure_occurrence_id,
        p.person_id,
        p.procedure_concept_id,
        p.procedure_source_value,
        p.procedure_source_concept_id,
        p.procedure_date,
        p.procedure_datetime,
        p.procedure_type_concept_id,
        p.visit_occurrence_id,
        p.visit_detail_id
    FROM
    (
        SELECT DISTINCT
            p.person_id person_id,
            dc.concept_id procedure_concept_id, 
            p.a10th_Procedure_OPCS procedure_source_value, 
            sc.concept_id procedure_source_concept_id,
            CASE WHEN LENGTH(p.a10th_Procedure_Date_OPCS) = 6
                    THEN PARSE_DATE('%Y%m%d', CONCAT(p.a10th_Procedure_Date_OPCS, '01'))
                WHEN LENGTH(a10th_Procedure_Date_OPCS) = 8
                    THEN PARSE_DATE('%Y%m%d', p.a10th_Procedure_Date_OPCS)
            END procedure_date,
            CASE WHEN LENGTH(a10th_Procedure_Date_OPCS) = 6
                    THEN PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.a10th_Procedure_Date_OPCS ,'01000000'))
                WHEN LENGTH(a10th_Procedure_Date_OPCS) = 8
                    THEN PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.a10th_Procedure_Date_OPCS ,'000000'))
            END procedure_datetime,       
            32817 procedure_type_concept_id, --EHR
            vl.id visit_occurrence_id,
            vdl.id visit_detail_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1` p   
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = CONCAT(CAST(p.Hospital_Provider_Spell_Number as STRING),'_', p.person_id)
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups`) vdl On vdl.source_value = CONCAT(CAST(p.Hospital_Provider_Spell_Number as STRING),'_', p.person_id, '_', p.Episode_Number)
        JOIN `CY_CDM_VOCAB.concept` sc on REPLACE(sc.concept_code, '.', '') = p.a10th_Procedure_OPCS
        JOIN `CY_CDM_VOCAB.concept_relationship` cr on cr.concept_id_1 = sc.concept_id
        JOIN `CY_CDM_VOCAB.concept` dc on dc.concept_id = cr.concept_id_2    
        WHERE (p.a10th_Procedure_Date_OPCS IS NOT NULL AND p.a10th_Procedure_Date_OPCS NOT LIKE '% %' AND (LENGTH(p.a10th_Procedure_Date_OPCS) =6 OR LENGTH(p.a10th_Procedure_Date_OPCS) =8))
        AND p.person_id IS NOT NULL
        AND p.a10th_Procedure_OPCS IS NOT NULL
        AND cr.relationship_id = 'Maps to'
        AND sc.vocabulary_id = 'OPCS4'
        AND dc.vocabulary_id = 'SNOMED'
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.procedure_occurrence` op ON 
        op.person_id = p.person_id 
        AND op.procedure_concept_id = p.procedure_concept_id
        AND op.procedure_source_value = p.procedure_source_value
        AND COALESCE(op.procedure_source_concept_id, -1) = COALESCE(p.procedure_source_concept_id, -1)
        AND op.procedure_datetime = p.procedure_datetime
        AND op.procedure_type_concept_id = p.procedure_type_concept_id
        AND COALESCE(op.provider_id, -1) = -1
        AND COALESCE(op.visit_occurrence_id, -1) = COALESCE(p.visit_occurrence_id, -1)
        AND COALESCE(op.visit_detail_id, -1) = COALESCE(p.visit_detail_id, -1)
    WHERE op.procedure_occurrence_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added procedure_occurrence', run_id, 'SUS_BRI_APC_010415_to_300619_P1 - a10th_Procedure_OPCS -> procedure_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_APC_010415_to_300619_P1 - a11th_Procedure_OPCS] Transfer
    */
    SET max_current_id = COALESCE((SELECT procedure_occurrence_id FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.procedure_occurrence`
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_source_value,
        procedure_source_concept_id,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        visit_occurrence_id,
        visit_detail_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as procedure_occurrence_id,
        p.person_id,
        p.procedure_concept_id,
        p.procedure_source_value,
        p.procedure_source_concept_id,
        p.procedure_date,
        p.procedure_datetime,
        p.procedure_type_concept_id,
        p.visit_occurrence_id,
        p.visit_detail_id
    FROM
    (
        SELECT DISTINCT
            p.person_id person_id,
            dc.concept_id procedure_concept_id, 
            p.a11th_Procedure_OPCS procedure_source_value, 
            sc.concept_id procedure_source_concept_id,
            CASE WHEN LENGTH(p.a11th_Procedure_Date_OPCS) = 6
                    THEN PARSE_DATE('%Y%m%d', CONCAT(p.a11th_Procedure_Date_OPCS, '01'))
                WHEN LENGTH(a11th_Procedure_Date_OPCS) = 8
                    THEN PARSE_DATE('%Y%m%d', p.a11th_Procedure_Date_OPCS)
            END procedure_date,
            CASE WHEN LENGTH(a11th_Procedure_Date_OPCS) = 6
                    THEN PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.a11th_Procedure_Date_OPCS ,'01000000'))
                WHEN LENGTH(a11th_Procedure_Date_OPCS) = 8
                    THEN PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.a11th_Procedure_Date_OPCS ,'000000'))
            END procedure_datetime,       
            32817 procedure_type_concept_id, --EHR
            vl.id visit_occurrence_id,
            vdl.id visit_detail_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1` p   
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = CONCAT(CAST(p.Hospital_Provider_Spell_Number as STRING),'_', p.person_id)
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups`) vdl On vdl.source_value = CONCAT(CAST(p.Hospital_Provider_Spell_Number as STRING),'_', p.person_id, '_', p.Episode_Number)
        JOIN `CY_CDM_VOCAB.concept` sc on REPLACE(sc.concept_code, '.', '') = p.a11th_Procedure_OPCS
        JOIN `CY_CDM_VOCAB.concept_relationship` cr on cr.concept_id_1 = sc.concept_id
        JOIN `CY_CDM_VOCAB.concept` dc on dc.concept_id = cr.concept_id_2    
        WHERE (p.a11th_Procedure_Date_OPCS IS NOT NULL AND p.a11th_Procedure_Date_OPCS NOT LIKE '% %' AND (LENGTH(p.a11th_Procedure_Date_OPCS) =6 OR LENGTH(p.a11th_Procedure_Date_OPCS) =8))
        AND p.person_id IS NOT NULL
        AND p.a11th_Procedure_OPCS IS NOT NULL
        AND cr.relationship_id = 'Maps to'
        AND sc.vocabulary_id = 'OPCS4'
        AND dc.vocabulary_id = 'SNOMED'
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.procedure_occurrence` op ON 
        op.person_id = p.person_id 
        AND op.procedure_concept_id = p.procedure_concept_id
        AND op.procedure_source_value = p.procedure_source_value
        AND COALESCE(op.procedure_source_concept_id, -1) = COALESCE(p.procedure_source_concept_id, -1)
        AND op.procedure_datetime = p.procedure_datetime
        AND op.procedure_type_concept_id = p.procedure_type_concept_id
        AND COALESCE(op.provider_id, -1) = -1
        AND COALESCE(op.visit_occurrence_id, -1) = COALESCE(p.visit_occurrence_id, -1)
        AND COALESCE(op.visit_detail_id, -1) = COALESCE(p.visit_detail_id, -1)
    WHERE op.procedure_occurrence_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added procedure_occurrence', run_id, 'SUS_BRI_APC_010415_to_300619_P1 - a11th_Procedure_OPCS -> procedure_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_APC_010415_to_300619_P1 - a12th_Procedure_OPCS] Transfer
    */
    SET max_current_id = COALESCE((SELECT procedure_occurrence_id FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.procedure_occurrence`
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_source_value,
        procedure_source_concept_id,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        visit_occurrence_id,
        visit_detail_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as procedure_occurrence_id,
        p.person_id,
        p.procedure_concept_id,
        p.procedure_source_value,
        p.procedure_source_concept_id,
        p.procedure_date,
        p.procedure_datetime,
        p.procedure_type_concept_id,
        p.visit_occurrence_id,
        p.visit_detail_id
    FROM
    (
        SELECT DISTINCT
            p.person_id person_id,
            dc.concept_id procedure_concept_id, 
            p.a12th_Procedure_OPCS procedure_source_value, 
            sc.concept_id procedure_source_concept_id,
            CASE WHEN LENGTH(p.a12th_Procedure_Date_OPCS) = 6
                    THEN PARSE_DATE('%Y%m%d', CONCAT(p.a12th_Procedure_Date_OPCS, '01'))
                WHEN LENGTH(a12th_Procedure_Date_OPCS) = 8
                    THEN PARSE_DATE('%Y%m%d', p.a12th_Procedure_Date_OPCS)
            END procedure_date,
            CASE WHEN LENGTH(a12th_Procedure_Date_OPCS) = 6
                    THEN PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.a12th_Procedure_Date_OPCS ,'01000000'))
                WHEN LENGTH(a12th_Procedure_Date_OPCS) = 8
                    THEN PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.a12th_Procedure_Date_OPCS ,'000000'))
            END procedure_datetime,       
            32817 procedure_type_concept_id, --EHR
            vl.id visit_occurrence_id,
            vdl.id visit_detail_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1` p   
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = CONCAT(CAST(p.Hospital_Provider_Spell_Number as STRING),'_', p.person_id)
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups`) vdl On vdl.source_value = CONCAT(CAST(p.Hospital_Provider_Spell_Number as STRING),'_', p.person_id, '_', p.Episode_Number)
        JOIN `CY_CDM_VOCAB.concept` sc on REPLACE(sc.concept_code, '.', '') = p.a12th_Procedure_OPCS
        JOIN `CY_CDM_VOCAB.concept_relationship` cr on cr.concept_id_1 = sc.concept_id
        JOIN `CY_CDM_VOCAB.concept` dc on dc.concept_id = cr.concept_id_2    
        WHERE (p.a12th_Procedure_Date_OPCS IS NOT NULL AND p.a12th_Procedure_Date_OPCS NOT LIKE '% %' AND (LENGTH(p.a12th_Procedure_Date_OPCS) =6 OR LENGTH(p.a12th_Procedure_Date_OPCS) =8))
        AND p.person_id IS NOT NULL
        AND p.a12th_Procedure_OPCS IS NOT NULL
        AND cr.relationship_id = 'Maps to'
        AND sc.vocabulary_id = 'OPCS4'
        AND dc.vocabulary_id = 'SNOMED'
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.procedure_occurrence` op ON 
        op.person_id = p.person_id 
        AND op.procedure_concept_id = p.procedure_concept_id
        AND op.procedure_source_value = p.procedure_source_value
        AND COALESCE(op.procedure_source_concept_id, -1) = COALESCE(p.procedure_source_concept_id, -1)
        AND op.procedure_datetime = p.procedure_datetime
        AND op.procedure_type_concept_id = p.procedure_type_concept_id
        AND COALESCE(op.provider_id, -1) = -1
        AND COALESCE(op.visit_occurrence_id, -1) = COALESCE(p.visit_occurrence_id, -1)
        AND COALESCE(op.visit_detail_id, -1) = COALESCE(p.visit_detail_id, -1)
    WHERE op.procedure_occurrence_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added procedure_occurrence', run_id, 'SUS_BRI_APC_010415_to_300619_P1 - a12th_Procedure_OPCS -> procedure_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_APC_010415_to_300619_P1 - a13th_Procedure_OPCS] Transfer
    */
    SET max_current_id = COALESCE((SELECT procedure_occurrence_id FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.procedure_occurrence`
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_source_value,
        procedure_source_concept_id,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        visit_occurrence_id,
        visit_detail_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as procedure_occurrence_id,
        p.person_id,
        p.procedure_concept_id,
        p.procedure_source_value,
        p.procedure_source_concept_id,
        p.procedure_date,
        p.procedure_datetime,
        p.procedure_type_concept_id,
        p.visit_occurrence_id,
        p.visit_detail_id
    FROM
    (
        SELECT DISTINCT
            p.person_id person_id,
            dc.concept_id procedure_concept_id, 
            p.a13th_Procedure_OPCS procedure_source_value, 
            sc.concept_id procedure_source_concept_id,
            CASE WHEN LENGTH(p.a13th_Procedure_Date_OPCS) = 6
                    THEN PARSE_DATE('%Y%m%d', CONCAT(p.a13th_Procedure_Date_OPCS, '01'))
                WHEN LENGTH(a13th_Procedure_Date_OPCS) = 8
                    THEN PARSE_DATE('%Y%m%d', p.a13th_Procedure_Date_OPCS)
            END procedure_date,
            CASE WHEN LENGTH(a13th_Procedure_Date_OPCS) = 6
                    THEN PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.a13th_Procedure_Date_OPCS ,'01000000'))
                WHEN LENGTH(a13th_Procedure_Date_OPCS) = 8
                    THEN PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.a13th_Procedure_Date_OPCS ,'000000'))
            END procedure_datetime,       
            32817 procedure_type_concept_id, --EHR
            vl.id visit_occurrence_id,
            vdl.id visit_detail_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1` p   
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = CONCAT(CAST(p.Hospital_Provider_Spell_Number as STRING),'_', p.person_id)
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups`) vdl On vdl.source_value = CONCAT(CAST(p.Hospital_Provider_Spell_Number as STRING),'_', p.person_id, '_', p.Episode_Number)
        JOIN `CY_CDM_VOCAB.concept` sc on REPLACE(sc.concept_code, '.', '') = p.a13th_Procedure_OPCS
        JOIN `CY_CDM_VOCAB.concept_relationship` cr on cr.concept_id_1 = sc.concept_id
        JOIN `CY_CDM_VOCAB.concept` dc on dc.concept_id = cr.concept_id_2    
        WHERE (p.a13th_Procedure_Date_OPCS IS NOT NULL AND p.a13th_Procedure_Date_OPCS NOT LIKE '% %' AND (LENGTH(p.a13th_Procedure_Date_OPCS) =6 OR LENGTH(p.a13th_Procedure_Date_OPCS) =8))
        AND p.person_id IS NOT NULL
        AND p.a13th_Procedure_OPCS IS NOT NULL
        AND cr.relationship_id = 'Maps to'
        AND sc.vocabulary_id = 'OPCS4'
        AND dc.vocabulary_id = 'SNOMED'
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.procedure_occurrence` op ON 
        op.person_id = p.person_id 
        AND op.procedure_concept_id = p.procedure_concept_id
        AND op.procedure_source_value = p.procedure_source_value
        AND COALESCE(op.procedure_source_concept_id, -1) = COALESCE(p.procedure_source_concept_id, -1)
        AND op.procedure_datetime = p.procedure_datetime
        AND op.procedure_type_concept_id = p.procedure_type_concept_id
        AND COALESCE(op.provider_id, -1) = -1
        AND COALESCE(op.visit_occurrence_id, -1) = COALESCE(p.visit_occurrence_id, -1)
        AND COALESCE(op.visit_detail_id, -1) = COALESCE(p.visit_detail_id, -1)
    WHERE op.procedure_occurrence_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added procedure_occurrence', run_id, 'SUS_BRI_APC_010415_to_300619_P1 - a13th_Procedure_OPCS -> procedure_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_APC_010415_to_300619_P1 - a14th_Procedure_OPCS] Transfer
    */
    SET max_current_id = COALESCE((SELECT procedure_occurrence_id FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.procedure_occurrence`
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_source_value,
        procedure_source_concept_id,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        visit_occurrence_id,
        visit_detail_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as procedure_occurrence_id,
        p.person_id,
        p.procedure_concept_id,
        p.procedure_source_value,
        p.procedure_source_concept_id,
        p.procedure_date,
        p.procedure_datetime,
        p.procedure_type_concept_id,
        p.visit_occurrence_id,
        p.visit_detail_id
    FROM
    (
        SELECT DISTINCT
            p.person_id person_id,
            dc.concept_id procedure_concept_id, 
            p.a14th_Procedure_OPCS procedure_source_value, 
            sc.concept_id procedure_source_concept_id,
            CASE WHEN LENGTH(p.a14th_Procedure_Date_OPCS) = 6
                    THEN PARSE_DATE('%Y%m%d', CONCAT(p.a14th_Procedure_Date_OPCS, '01'))
                WHEN LENGTH(a14th_Procedure_Date_OPCS) = 8
                    THEN PARSE_DATE('%Y%m%d', p.a14th_Procedure_Date_OPCS)
            END procedure_date,
            CASE WHEN LENGTH(a14th_Procedure_Date_OPCS) = 6
                    THEN PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.a14th_Procedure_Date_OPCS ,'01000000'))
                WHEN LENGTH(a14th_Procedure_Date_OPCS) = 8
                    THEN PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.a14th_Procedure_Date_OPCS ,'000000'))
            END procedure_datetime,       
            32817 procedure_type_concept_id, --EHR
            vl.id visit_occurrence_id,
            vdl.id visit_detail_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1` p   
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = CONCAT(CAST(p.Hospital_Provider_Spell_Number as STRING),'_', p.person_id)
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups`) vdl On vdl.source_value = CONCAT(CAST(p.Hospital_Provider_Spell_Number as STRING),'_', p.person_id, '_', p.Episode_Number)
        JOIN `CY_CDM_VOCAB.concept` sc on REPLACE(sc.concept_code, '.', '') = p.a14th_Procedure_OPCS
        JOIN `CY_CDM_VOCAB.concept_relationship` cr on cr.concept_id_1 = sc.concept_id
        JOIN `CY_CDM_VOCAB.concept` dc on dc.concept_id = cr.concept_id_2    
        WHERE (p.a14th_Procedure_Date_OPCS IS NOT NULL AND p.a14th_Procedure_Date_OPCS NOT LIKE '% %' AND (LENGTH(p.a14th_Procedure_Date_OPCS) =6 OR LENGTH(p.a14th_Procedure_Date_OPCS) =8))
        AND p.person_id IS NOT NULL
        AND p.a14th_Procedure_OPCS IS NOT NULL
        AND cr.relationship_id = 'Maps to'
        AND sc.vocabulary_id = 'OPCS4'
        AND dc.vocabulary_id = 'SNOMED'
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.procedure_occurrence` op ON 
        op.person_id = p.person_id 
        AND op.procedure_concept_id = p.procedure_concept_id
        AND op.procedure_source_value = p.procedure_source_value
        AND COALESCE(op.procedure_source_concept_id, -1) = COALESCE(p.procedure_source_concept_id, -1)
        AND op.procedure_datetime = p.procedure_datetime
        AND op.procedure_type_concept_id = p.procedure_type_concept_id
        AND COALESCE(op.provider_id, -1) = -1
        AND COALESCE(op.visit_occurrence_id, -1) = COALESCE(p.visit_occurrence_id, -1)
        AND COALESCE(op.visit_detail_id, -1) = COALESCE(p.visit_detail_id, -1)
    WHERE op.procedure_occurrence_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added procedure_occurrence', run_id, 'SUS_BRI_APC_010415_to_300619_P1 - a14th_Procedure_OPCS -> procedure_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_APC_010415_to_300619_P1 - a15th_Procedure_OPCS] Transfer
    */
    SET max_current_id = COALESCE((SELECT procedure_occurrence_id FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.procedure_occurrence`
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_source_value,
        procedure_source_concept_id,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        visit_occurrence_id,
        visit_detail_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as procedure_occurrence_id,
        p.person_id,
        p.procedure_concept_id,
        p.procedure_source_value,
        p.procedure_source_concept_id,
        p.procedure_date,
        p.procedure_datetime,
        p.procedure_type_concept_id,
        p.visit_occurrence_id,
        p.visit_detail_id
    FROM
    (
        SELECT DISTINCT
            p.person_id person_id,
            dc.concept_id procedure_concept_id, 
            p.a15th_Procedure_OPCS procedure_source_value, 
            sc.concept_id procedure_source_concept_id,
            CASE WHEN LENGTH(p.a15th_Procedure_Date_OPCS) = 6
                    THEN PARSE_DATE('%Y%m%d', CONCAT(p.a15th_Procedure_Date_OPCS, '01'))
                WHEN LENGTH(a15th_Procedure_Date_OPCS) = 8
                    THEN PARSE_DATE('%Y%m%d', p.a15th_Procedure_Date_OPCS)
            END procedure_date,
            CASE WHEN LENGTH(a15th_Procedure_Date_OPCS) = 6
                    THEN PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.a15th_Procedure_Date_OPCS ,'01000000'))
                WHEN LENGTH(a15th_Procedure_Date_OPCS) = 8
                    THEN PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.a15th_Procedure_Date_OPCS ,'000000'))
            END procedure_datetime,       
            32817 procedure_type_concept_id, --EHR
            vl.id visit_occurrence_id,
            vdl.id visit_detail_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1` p   
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = CONCAT(CAST(p.Hospital_Provider_Spell_Number as STRING),'_', p.person_id)
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups`) vdl On vdl.source_value = CONCAT(CAST(p.Hospital_Provider_Spell_Number as STRING),'_', p.person_id, '_', p.Episode_Number)
        JOIN `CY_CDM_VOCAB.concept` sc on REPLACE(sc.concept_code, '.', '') = p.a15th_Procedure_OPCS
        JOIN `CY_CDM_VOCAB.concept_relationship` cr on cr.concept_id_1 = sc.concept_id
        JOIN `CY_CDM_VOCAB.concept` dc on dc.concept_id = cr.concept_id_2    
        WHERE (p.a15th_Procedure_Date_OPCS IS NOT NULL AND p.a15th_Procedure_Date_OPCS NOT LIKE '% %' AND (LENGTH(p.a15th_Procedure_Date_OPCS) =6 OR LENGTH(p.a15th_Procedure_Date_OPCS) =8))
        AND p.person_id IS NOT NULL
        AND p.a15th_Procedure_OPCS IS NOT NULL
        AND cr.relationship_id = 'Maps to'
        AND sc.vocabulary_id = 'OPCS4'
        AND dc.vocabulary_id = 'SNOMED'
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.procedure_occurrence` op ON 
        op.person_id = p.person_id 
        AND op.procedure_concept_id = p.procedure_concept_id
        AND op.procedure_source_value = p.procedure_source_value
        AND COALESCE(op.procedure_source_concept_id, -1) = COALESCE(p.procedure_source_concept_id, -1)
        AND op.procedure_datetime = p.procedure_datetime
        AND op.procedure_type_concept_id = p.procedure_type_concept_id
        AND COALESCE(op.provider_id, -1) = -1
        AND COALESCE(op.visit_occurrence_id, -1) = COALESCE(p.visit_occurrence_id, -1)
        AND COALESCE(op.visit_detail_id, -1) = COALESCE(p.visit_detail_id, -1)
    WHERE op.procedure_occurrence_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added procedure_occurrence', run_id, 'SUS_BRI_APC_010415_to_300619_P1 - a15th_Procedure_OPCS -> procedure_occurrence', @@row_count, CURRENT_DATETIME();

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
------------[SUS_BRI_OP_010415_to_300619]
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

    /*
        --[SUS_BRI_OP_010415_to_300619 - Primary_Procedure_OPCS] Transfer
    */
    SET max_current_id = COALESCE((SELECT procedure_occurrence_id FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.procedure_occurrence`
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_source_value,
        procedure_source_concept_id,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as procedure_occurrence_id,
        p.person_id,
        p.procedure_concept_id,
        p.procedure_source_value,
        p.procedure_source_concept_id,
        p.procedure_date,
        p.procedure_datetime,
        p.procedure_type_concept_id,
        p.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            p.person_id person_id,
            dc.concept_id procedure_concept_id, 
            p.Primary_Procedure_OPCS procedure_source_value, 
            sc.concept_id procedure_source_concept_id,
            PARSE_DATE("%Y%m%d", Appointment_Date) procedure_date,
              CASE WHEN Appointment_Time LIKE '% %' OR Appointment_Time IS NULL
              THEN PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(Appointment_Date, '000000'))
              ELSE PARSE_DATETIME('%Y%m%d%H:%M%S', CONCAT(Appointment_Date, Appointment_Time, '00'))
            END procedure_datetime,     
            32817 procedure_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_OP_010415_to_300619` p   
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_OP_010415_to_300619') vl On vl.source_value = CONCAT(CAST(p.Generated_Record_Identifier as STRING),'_', p.person_id)
        JOIN `CY_CDM_VOCAB.concept` sc on REPLACE(sc.concept_code, '.', '') = REPLACE(p.Primary_Procedure_OPCS, ' ', '')
        JOIN `CY_CDM_VOCAB.concept_relationship` cr on cr.concept_id_1 = sc.concept_id
        JOIN `CY_CDM_VOCAB.concept` dc on dc.concept_id = cr.concept_id_2    
        WHERE p.Appointment_Date IS NOT NULL
        AND p.person_id IS NOT NULL
        AND (p.Primary_Procedure_OPCS IS NOT NULL AND p.Primary_Procedure_OPCS != '     ' )
        AND cr.relationship_id = 'Maps to'
        AND sc.vocabulary_id = 'OPCS4'
        AND dc.vocabulary_id = 'SNOMED'
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.procedure_occurrence` op ON 
        op.person_id = p.person_id 
        AND op.procedure_concept_id = p.procedure_concept_id
        AND op.procedure_source_value = p.procedure_source_value
        AND COALESCE(op.procedure_source_concept_id, -1) = COALESCE(p.procedure_source_concept_id, -1)
        AND op.procedure_datetime = p.procedure_datetime
        AND op.procedure_type_concept_id = p.procedure_type_concept_id
        AND COALESCE(op.provider_id, -1) = -1
        AND COALESCE(op.visit_occurrence_id, -1) = COALESCE(p.visit_occurrence_id, -1)
        AND COALESCE(op.visit_detail_id, -1) = -1
    WHERE op.procedure_occurrence_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added procedure_occurrence', run_id, 'SUS_BRI_OP_010415_to_300619 - Primary_Procedure_OPCS -> procedure_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_OP_010415_to_300619 - A2nd_Procedure_OPCS] Transfer
    */
    SET max_current_id = COALESCE((SELECT procedure_occurrence_id FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.procedure_occurrence`
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_source_value,
        procedure_source_concept_id,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as procedure_occurrence_id,
        p.person_id,
        p.procedure_concept_id,
        p.procedure_source_value,
        p.procedure_source_concept_id,
        p.procedure_date,
        p.procedure_datetime,
        p.procedure_type_concept_id,
        p.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            p.person_id person_id,
            dc.concept_id procedure_concept_id, 
            p.A2nd_Procedure_OPCS procedure_source_value, 
            sc.concept_id procedure_source_concept_id,
            PARSE_DATE("%Y%m%d", Appointment_Date) procedure_date,
              CASE WHEN Appointment_Time LIKE '% %' OR Appointment_Time IS NULL
              THEN PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(Appointment_Date, '000000'))
              ELSE PARSE_DATETIME('%Y%m%d%H:%M%S', CONCAT(Appointment_Date, Appointment_Time, '00'))
            END procedure_datetime,     
            32817 procedure_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_OP_010415_to_300619` p   
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_OP_010415_to_300619') vl On vl.source_value = CONCAT(CAST(p.Generated_Record_Identifier as STRING),'_', p.person_id)
        JOIN `CY_CDM_VOCAB.concept` sc on REPLACE(sc.concept_code, '.', '') = REPLACE(p.A2nd_Procedure_OPCS, ' ', '')
        JOIN `CY_CDM_VOCAB.concept_relationship` cr on cr.concept_id_1 = sc.concept_id
        JOIN `CY_CDM_VOCAB.concept` dc on dc.concept_id = cr.concept_id_2    
        WHERE p.Appointment_Date IS NOT NULL
        AND p.person_id IS NOT NULL
        AND (p.A2nd_Procedure_OPCS IS NOT NULL AND p.A2nd_Procedure_OPCS != '     ' )
        AND cr.relationship_id = 'Maps to'
        AND sc.vocabulary_id = 'OPCS4'
        AND dc.vocabulary_id = 'SNOMED'
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.procedure_occurrence` op ON 
        op.person_id = p.person_id 
        AND op.procedure_concept_id = p.procedure_concept_id
        AND op.procedure_source_value = p.procedure_source_value
        AND COALESCE(op.procedure_source_concept_id, -1) = COALESCE(p.procedure_source_concept_id, -1)
        AND op.procedure_datetime = p.procedure_datetime
        AND op.procedure_type_concept_id = p.procedure_type_concept_id
        AND COALESCE(op.provider_id, -1) = -1
        AND COALESCE(op.visit_occurrence_id, -1) = COALESCE(p.visit_occurrence_id, -1)
        AND COALESCE(op.visit_detail_id, -1) = -1
    WHERE op.procedure_occurrence_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added procedure_occurrence', run_id, 'SUS_BRI_OP_010415_to_300619 - A2nd_Procedure_OPCS -> procedure_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_OP_010415_to_300619 - A3rd_Procedure_OPCS] Transfer
    */
    SET max_current_id = COALESCE((SELECT procedure_occurrence_id FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.procedure_occurrence`
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_source_value,
        procedure_source_concept_id,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as procedure_occurrence_id,
        p.person_id,
        p.procedure_concept_id,
        p.procedure_source_value,
        p.procedure_source_concept_id,
        p.procedure_date,
        p.procedure_datetime,
        p.procedure_type_concept_id,
        p.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            p.person_id person_id,
            dc.concept_id procedure_concept_id, 
            p.A3rd_Procedure_OPCS procedure_source_value, 
            sc.concept_id procedure_source_concept_id,
            PARSE_DATE("%Y%m%d", Appointment_Date) procedure_date,
              CASE WHEN Appointment_Time LIKE '% %' OR Appointment_Time IS NULL
              THEN PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(Appointment_Date, '000000'))
              ELSE PARSE_DATETIME('%Y%m%d%H:%M%S', CONCAT(Appointment_Date, Appointment_Time, '00'))
            END procedure_datetime,     
            32817 procedure_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_OP_010415_to_300619` p   
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_OP_010415_to_300619') vl On vl.source_value = CONCAT(CAST(p.Generated_Record_Identifier as STRING),'_', p.person_id)
        JOIN `CY_CDM_VOCAB.concept` sc on REPLACE(sc.concept_code, '.', '') = REPLACE(p.A3rd_Procedure_OPCS, ' ', '')
        JOIN `CY_CDM_VOCAB.concept_relationship` cr on cr.concept_id_1 = sc.concept_id
        JOIN `CY_CDM_VOCAB.concept` dc on dc.concept_id = cr.concept_id_2    
        WHERE p.Appointment_Date IS NOT NULL
        AND p.person_id IS NOT NULL
        AND (p.A3rd_Procedure_OPCS IS NOT NULL AND p.A3rd_Procedure_OPCS != '     ' )
        AND cr.relationship_id = 'Maps to'
        AND sc.vocabulary_id = 'OPCS4'
        AND dc.vocabulary_id = 'SNOMED'
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.procedure_occurrence` op ON 
        op.person_id = p.person_id 
        AND op.procedure_concept_id = p.procedure_concept_id
        AND op.procedure_source_value = p.procedure_source_value
        AND COALESCE(op.procedure_source_concept_id, -1) = COALESCE(p.procedure_source_concept_id, -1)
        AND op.procedure_datetime = p.procedure_datetime
        AND op.procedure_type_concept_id = p.procedure_type_concept_id
        AND COALESCE(op.provider_id, -1) = -1
        AND COALESCE(op.visit_occurrence_id, -1) = COALESCE(p.visit_occurrence_id, -1)
        AND COALESCE(op.visit_detail_id, -1) = -1
    WHERE op.procedure_occurrence_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added procedure_occurrence', run_id, 'SUS_BRI_OP_010415_to_300619 - A3rd_Procedure_OPCS -> procedure_occurrence', @@row_count, CURRENT_DATETIME();
    
    /*
        --[SUS_BRI_OP_010415_to_300619 - A4th_Procedure_OPCS] Transfer
    */
    SET max_current_id = COALESCE((SELECT procedure_occurrence_id FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.procedure_occurrence`
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_source_value,
        procedure_source_concept_id,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as procedure_occurrence_id,
        p.person_id,
        p.procedure_concept_id,
        p.procedure_source_value,
        p.procedure_source_concept_id,
        p.procedure_date,
        p.procedure_datetime,
        p.procedure_type_concept_id,
        p.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            p.person_id person_id,
            dc.concept_id procedure_concept_id, 
            p.A4th_Procedure_OPCS procedure_source_value, 
            sc.concept_id procedure_source_concept_id,
            PARSE_DATE("%Y%m%d", Appointment_Date) procedure_date,
              CASE WHEN Appointment_Time LIKE '% %' OR Appointment_Time IS NULL
              THEN PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(Appointment_Date, '000000'))
              ELSE PARSE_DATETIME('%Y%m%d%H:%M%S', CONCAT(Appointment_Date, Appointment_Time, '00'))
            END procedure_datetime,     
            32817 procedure_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_OP_010415_to_300619` p   
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_OP_010415_to_300619') vl On vl.source_value = CONCAT(CAST(p.Generated_Record_Identifier as STRING),'_', p.person_id)
        JOIN `CY_CDM_VOCAB.concept` sc on REPLACE(sc.concept_code, '.', '') = REPLACE(p.A4th_Procedure_OPCS, ' ', '')
        JOIN `CY_CDM_VOCAB.concept_relationship` cr on cr.concept_id_1 = sc.concept_id
        JOIN `CY_CDM_VOCAB.concept` dc on dc.concept_id = cr.concept_id_2    
        WHERE p.Appointment_Date IS NOT NULL
        AND p.person_id IS NOT NULL
        AND (p.A4th_Procedure_OPCS IS NOT NULL AND p.A4th_Procedure_OPCS != '     ' )
        AND cr.relationship_id = 'Maps to'
        AND sc.vocabulary_id = 'OPCS4'
        AND dc.vocabulary_id = 'SNOMED'
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.procedure_occurrence` op ON 
        op.person_id = p.person_id 
        AND op.procedure_concept_id = p.procedure_concept_id
        AND op.procedure_source_value = p.procedure_source_value
        AND COALESCE(op.procedure_source_concept_id, -1) = COALESCE(p.procedure_source_concept_id, -1)
        AND op.procedure_datetime = p.procedure_datetime
        AND op.procedure_type_concept_id = p.procedure_type_concept_id
        AND COALESCE(op.provider_id, -1) = -1
        AND COALESCE(op.visit_occurrence_id, -1) = COALESCE(p.visit_occurrence_id, -1)
        AND COALESCE(op.visit_detail_id, -1) = -1
    WHERE op.procedure_occurrence_id IS NULL;
  
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added procedure_occurrence', run_id, 'SUS_BRI_OP_010415_to_300619 - A4th_Procedure_OPCS -> procedure_occurrence', @@row_count, CURRENT_DATETIME();
    
    /*
        --[SUS_BRI_OP_010415_to_300619 - A5th_Procedure_OPCS] Transfer
    */
    SET max_current_id = COALESCE((SELECT procedure_occurrence_id FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.procedure_occurrence`
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_source_value,
        procedure_source_concept_id,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as procedure_occurrence_id,
        p.person_id,
        p.procedure_concept_id,
        p.procedure_source_value,
        p.procedure_source_concept_id,
        p.procedure_date,
        p.procedure_datetime,
        p.procedure_type_concept_id,
        p.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            p.person_id person_id,
            dc.concept_id procedure_concept_id, 
            p.A5th_Procedure_OPCS procedure_source_value, 
            sc.concept_id procedure_source_concept_id,
            PARSE_DATE("%Y%m%d", Appointment_Date) procedure_date,
              CASE WHEN Appointment_Time LIKE '% %' OR Appointment_Time IS NULL
              THEN PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(Appointment_Date, '000000'))
              ELSE PARSE_DATETIME('%Y%m%d%H:%M%S', CONCAT(Appointment_Date, Appointment_Time, '00'))
            END procedure_datetime,     
            32817 procedure_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_OP_010415_to_300619` p   
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_OP_010415_to_300619') vl On vl.source_value = CONCAT(CAST(p.Generated_Record_Identifier as STRING),'_', p.person_id)
        JOIN `CY_CDM_VOCAB.concept` sc on REPLACE(sc.concept_code, '.', '') = REPLACE(p.A5th_Procedure_OPCS, ' ', '')
        JOIN `CY_CDM_VOCAB.concept_relationship` cr on cr.concept_id_1 = sc.concept_id
        JOIN `CY_CDM_VOCAB.concept` dc on dc.concept_id = cr.concept_id_2    
        WHERE p.Appointment_Date IS NOT NULL
        AND p.person_id IS NOT NULL
        AND (p.A5th_Procedure_OPCS IS NOT NULL AND p.A5th_Procedure_OPCS != '     ' )
        AND cr.relationship_id = 'Maps to'
        AND sc.vocabulary_id = 'OPCS4'
        AND dc.vocabulary_id = 'SNOMED'
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.procedure_occurrence` op ON 
        op.person_id = p.person_id 
        AND op.procedure_concept_id = p.procedure_concept_id
        AND op.procedure_source_value = p.procedure_source_value
        AND COALESCE(op.procedure_source_concept_id, -1) = COALESCE(p.procedure_source_concept_id, -1)
        AND op.procedure_datetime = p.procedure_datetime
        AND op.procedure_type_concept_id = p.procedure_type_concept_id
        AND COALESCE(op.provider_id, -1) = -1
        AND COALESCE(op.visit_occurrence_id, -1) = COALESCE(p.visit_occurrence_id, -1)
        AND COALESCE(op.visit_detail_id, -1) = -1
    WHERE op.procedure_occurrence_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added procedure_occurrence', run_id, 'SUS_BRI_OP_010415_to_300619 - A5th_Procedure_OPCS -> procedure_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_OP_010415_to_300619 - A6th_Procedure_OPCS] Transfer
    */
    SET max_current_id = COALESCE((SELECT procedure_occurrence_id FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.procedure_occurrence`
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_source_value,
        procedure_source_concept_id,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as procedure_occurrence_id,
        p.person_id,
        p.procedure_concept_id,
        p.procedure_source_value,
        p.procedure_source_concept_id,
        p.procedure_date,
        p.procedure_datetime,
        p.procedure_type_concept_id,
        p.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            p.person_id person_id,
            dc.concept_id procedure_concept_id, 
            p.A6th_Procedure_OPCS procedure_source_value, 
            sc.concept_id procedure_source_concept_id,
            PARSE_DATE("%Y%m%d", Appointment_Date) procedure_date,
              CASE WHEN Appointment_Time LIKE '% %' OR Appointment_Time IS NULL
              THEN PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(Appointment_Date, '000000'))
              ELSE PARSE_DATETIME('%Y%m%d%H:%M%S', CONCAT(Appointment_Date, Appointment_Time, '00'))
            END procedure_datetime,     
            32817 procedure_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_OP_010415_to_300619` p   
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_OP_010415_to_300619') vl On vl.source_value = CONCAT(CAST(p.Generated_Record_Identifier as STRING),'_', p.person_id)
        JOIN `CY_CDM_VOCAB.concept` sc on REPLACE(sc.concept_code, '.', '') = REPLACE(p.A6th_Procedure_OPCS, ' ', '')
        JOIN `CY_CDM_VOCAB.concept_relationship` cr on cr.concept_id_1 = sc.concept_id
        JOIN `CY_CDM_VOCAB.concept` dc on dc.concept_id = cr.concept_id_2    
        WHERE p.Appointment_Date IS NOT NULL
        AND p.person_id IS NOT NULL
        AND (p.A6th_Procedure_OPCS IS NOT NULL AND p.A6th_Procedure_OPCS != '     ' )
        AND cr.relationship_id = 'Maps to'
        AND sc.vocabulary_id = 'OPCS4'
        AND dc.vocabulary_id = 'SNOMED'
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.procedure_occurrence` op ON 
        op.person_id = p.person_id 
        AND op.procedure_concept_id = p.procedure_concept_id
        AND op.procedure_source_value = p.procedure_source_value
        AND COALESCE(op.procedure_source_concept_id, -1) = COALESCE(p.procedure_source_concept_id, -1)
        AND op.procedure_datetime = p.procedure_datetime
        AND op.procedure_type_concept_id = p.procedure_type_concept_id
        AND COALESCE(op.provider_id, -1) = -1
        AND COALESCE(op.visit_occurrence_id, -1) = COALESCE(p.visit_occurrence_id, -1)
        AND COALESCE(op.visit_detail_id, -1) = -1
    WHERE op.procedure_occurrence_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added procedure_occurrence', run_id, 'SUS_BRI_OP_010415_to_300619 - A6th_Procedure_OPCS -> procedure_occurrence', @@row_count, CURRENT_DATETIME();

    /*
        --[tbl_SRCode] Transfer
    */
    SET max_current_id = COALESCE((SELECT procedure_occurrence_id FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.procedure_occurrence`
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_source_value,
        procedure_source_concept_id,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        provider_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as procedure_occurrence_id,
        p.person_id,
        p.procedure_concept_id,
        p.procedure_source_value,
        p.procedure_source_concept_id,
        p.procedure_date,
        p.procedure_datetime,
        p.procedure_type_concept_id,
        p.provider_id
    FROM
    (
        SELECT DISTINCT
            p.person_id person_id,
            dc.concept_id procedure_concept_id, 
            p.ctv3code procedure_source_value, 
            sc.concept_id procedure_source_concept_id,
            EXTRACT(DATE FROM dateevent) procedure_date,
            dateevent procedure_datetime,
            32817 procedure_type_concept_id, --EHR
            pr.provider_id provider_id
        FROM `CY_IMOSPHERE_WORKSPACE.tbl_SRCode` p
        JOIN `CY_LOOKUPS.tbl_CTV3ToSnomed_Map` sm ON sm.ctv3code = p.ctv3code
        JOIN `CY_CDM_VOCAB.concept` sc ON sc.concept_code = sm.SNOMEDCode
        JOIN `CY_CDM_VOCAB.concept_relationship` cr ON cr.concept_id_1 = sc.concept_id
        JOIN `CY_CDM_VOCAB.concept` dc ON dc.concept_id = cr.concept_id_2
        LEFT JOIN `CY_IMOSPHERE_CDM_531.provider` pr ON pr.provider_id = CAST(iddoneby AS INT64)
        WHERE sc.vocabulary_id = 'SNOMED'
        AND cr.relationship_id = 'Maps to'
        AND dc.standard_concept = 'S'
        AND dc.domain_id = 'Procedure'
        AND p.dateevent IS NOT NULL
        AND p.person_id IS NOT NULL
    ) p
    LEFT JOIN `CY_IMOSPHERE_CDM_531.procedure_occurrence` op ON 
        op.person_id = p.person_id
        AND op.procedure_concept_id = p.procedure_concept_id
        AND op.procedure_source_value = p.procedure_source_value
        AND COALESCE(op.procedure_source_concept_id, -1) = COALESCE(p.procedure_source_concept_id, -1)
        AND op.procedure_datetime = p.procedure_datetime
        AND op.procedure_type_concept_id = p.procedure_type_concept_id
        AND COALESCE(op.provider_id, -1) = COALESCE(p.provider_id, -1)
        AND COALESCE(op.visit_occurrence_id, -1) = -1
        AND COALESCE(op.visit_detail_id, -1) = -1
    WHERE op.procedure_occurrence_id IS NULL;

     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added procedure_occurrence', run_id, 'tbl_SRCode -> procedure_occurrence', @@row_count, CURRENT_DATETIME();
END;

CREATE OR REPLACE PROCEDURE CY_IMOSPHERE_WORKSPACE.TRANSFER_OBSERVATION(run_id INT64)
OPTIONS (strict_mode=false)
BEGIN
    DECLARE max_current_id INT64;

    /*
        --[tbl_SRVisit] Transfer
    */
    SET max_current_id = COALESCE((SELECT observation_id FROM `CY_IMOSPHERE_CDM_531.observation` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.observation`
    (
        observation_id,
        person_id,
        observation_concept_id,
        observation_source_value,
        observation_date,
        observation_datetime,
        observation_type_concept_id,
        value_as_number,
        value_as_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as observation_id,
        o.person_id,
        o.observation_concept_id,
        o.observation_source_value,
        o.observation_date,
        o.observation_datetime,
        o.observation_type_concept_id,
        o.value_as_number,
        o.value_as_concept_id,
        o.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            o.person_id person_id,
            44786638 observation_concept_id, --Visit Duration
            'Visit Duration' observation_source_value,
            EXTRACT(DATE FROM DateBooked) observation_date,
            DateBooked observation_datetime,
            32817 observation_type_concept_id, --EHR
            CAST(Duration as FLOAT64) value_as_number,
            8550 value_as_concept_id, --minute,
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.tbl_SRVisit` o
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'tbl_SRVisit') vl ON vl.source_value = CONCAT(CAST(o.RowIdentifier as STRING),'_', o.person_id)
        WHERE o.DateBooked IS NOT NULL
        AND o.person_id IS NOT NULL
    ) o
    LEFT JOIN `CY_IMOSPHERE_CDM_531.observation` oo ON 
        oo.person_id = o.person_id
        AND oo.observation_concept_id = o.observation_concept_id
        AND oo.observation_source_value = o.observation_source_value
        AND COALESCE(oo.observation_source_concept_id, -1) = -1
        AND oo.observation_datetime = o.observation_datetime
        AND oo.observation_type_concept_id = o.observation_type_concept_id
        AND COALESCE(oo.provider_id, -1) = -1
        AND COALESCE(oo.visit_occurrence_id, -1) = COALESCE(o.visit_occurrence_id, -1)
        AND COALESCE(oo.visit_detail_id, -1) = -1
        AND COALESCE(oo.value_as_concept_id, -1) = COALESCE(o.value_as_concept_id, -1)
        AND COALESCE(CAST(oo.value_as_number AS STRING), '') = COALESCE(CAST(o.value_as_number AS STRING), '')
        AND COALESCE(oo.qualifier_concept_id, -1) = -1
        AND COALESCE(oo.unit_concept_id, -1) = -1
        AND COALESCE(oo.qualifier_source_value, '') = ''
        AND COALESCE(oo.unit_source_value, '') = ''
    WHERE oo.observation_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added observation', run_id, 'tbl_SRVisit -> observation', @@row_count, CURRENT_DATETIME();

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    ------------[tbl_SRReferralIn]
    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

    /*
        --[tbl_SRReferralIn - PrimaryReason] Transfer
    */
    SET max_current_id = COALESCE((SELECT observation_id FROM `CY_IMOSPHERE_CDM_531.observation` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.observation`
    (
        observation_id,
        person_id,
        observation_concept_id,
        observation_source_value,
        observation_date,
        observation_datetime,
        observation_type_concept_id,
        value_as_concept_id,
        qualifier_concept_id,
        qualifier_source_value,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as observation_id,
        o.person_id,
        o.observation_concept_id,
        o.observation_source_value,
        o.observation_date,
        o.observation_datetime,
        o.observation_type_concept_id,
        o.value_as_concept_id,
        o.qualifier_concept_id,
        o.qualifier_source_value,
        o.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            o.person_id person_id,
            4144684 observation_concept_id,  --Patient referral
            PrimaryReason observation_source_value,
            datereferral observation_date,
            PARSE_DATETIME('%Y-%m-%d%H%M%S', CONCAT(o.datereferral ,'000000')) observation_datetime,
            32817 observation_type_concept_id, --EHR
            value.concept_id value_as_concept_id,
            4260904 qualifier_concept_id, --Reason for 
            'Reason for' qualifier_source_value,
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.tbl_SRReferralIn` o
        LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_SRVisit`v ON v.idreferralin = o.RowIdentifier AND v.person_id = o.person_id
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'tbl_SRVisit') vl ON vl.source_value = CONCAT(CAST(v.RowIdentifier as STRING),'_', v.person_id)
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` value on value.source_value = o.PrimaryReason AND value.destination_table = 'OBSERVATION' AND value.source_table = 'tbl_SRReferralIn' AND value.source_column = 'PrimaryReason' 
        WHERE o.datereferral IS NOT NULL
        AND o.person_id IS NOT NULL
    ) o
    LEFT JOIN `CY_IMOSPHERE_CDM_531.observation` oo ON 
        oo.person_id = o.person_id
        AND oo.observation_concept_id = o.observation_concept_id
        AND oo.observation_source_value = o.observation_source_value
        AND COALESCE(oo.observation_source_concept_id, -1) = -1
        AND oo.observation_datetime = o.observation_datetime
        AND oo.observation_type_concept_id = o.observation_type_concept_id
        AND COALESCE(oo.provider_id, -1) = -1
        AND COALESCE(oo.visit_occurrence_id, -1) = COALESCE(o.visit_occurrence_id, -1)
        AND COALESCE(oo.visit_detail_id, -1) = -1
        AND COALESCE(oo.value_as_concept_id, -1) = COALESCE(o.value_as_concept_id, -1)
        AND COALESCE(CAST(oo.value_as_number AS STRING), '') = ''
        AND COALESCE(oo.qualifier_concept_id, -1) = COALESCE(o.qualifier_concept_id, -1)
        AND COALESCE(oo.unit_concept_id, -1) = -1
        AND COALESCE(oo.qualifier_source_value, '') = COALESCE(o.qualifier_source_value ,'')
        AND COALESCE(oo.unit_source_value, '') = ''
    WHERE oo.observation_id IS NULL;
         
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added observation', run_id, 'tbl_SRReferralIn - PrimaryReason -> observation', @@row_count, CURRENT_DATETIME();

    /*
        --[tbl_SRReferralIn - Source] Transfer
    */
    SET max_current_id = COALESCE((SELECT observation_id FROM `CY_IMOSPHERE_CDM_531.observation` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.observation`
    (
        observation_id,
        person_id,
        observation_concept_id,
        observation_source_value,
        observation_date,
        observation_datetime,
        observation_type_concept_id,
        value_as_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as observation_id,
        o.person_id,
        o.observation_concept_id,
        o.observation_source_value,
        o.observation_date,
        o.observation_datetime,
        o.observation_type_concept_id,
        o.value_as_concept_id,
        o.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            o.person_id person_id,
            4287771 observation_concept_id, --Referral source
            Source observation_source_value,
            datereferral observation_date,
            PARSE_DATETIME('%Y-%m-%d%H%M%S', CONCAT(o.datereferral ,'000000')) observation_datetime,
            32817 observation_type_concept_id, --EHR
            value.concept_id value_as_concept_id,
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.tbl_SRReferralIn` o
        LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_SRVisit`v ON v.idreferralin = o.RowIdentifier AND v.person_id = o.person_id
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'tbl_SRVisit') vl ON vl.source_value = CONCAT(CAST(v.RowIdentifier as STRING),'_', v.person_id)
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` value on value.source_value = o.Source AND value.destination_table = 'OBSERVATION' AND value.source_table = 'tbl_SRReferralIn' AND value.source_column = 'Source'
        WHERE o.datereferral IS NOT NULL
        AND o.person_id IS NOT NULL
    ) o
    LEFT JOIN `CY_IMOSPHERE_CDM_531.observation` oo ON 
        oo.person_id = o.person_id
        AND oo.observation_concept_id = o.observation_concept_id
        AND oo.observation_source_value = o.observation_source_value
        AND COALESCE(oo.observation_source_concept_id, -1) = -1
        AND oo.observation_datetime = o.observation_datetime
        AND oo.observation_type_concept_id = o.observation_type_concept_id
        AND COALESCE(oo.provider_id, -1) = -1
        AND COALESCE(oo.visit_occurrence_id, -1) = COALESCE(o.visit_occurrence_id, -1)
        AND COALESCE(oo.visit_detail_id, -1) = -1
        AND COALESCE(oo.value_as_concept_id, -1) = COALESCE(o.value_as_concept_id, -1)
        AND COALESCE(CAST(oo.value_as_number AS STRING), '') = ''
        AND COALESCE(oo.qualifier_concept_id, -1) = -1
        AND COALESCE(oo.unit_concept_id, -1) = -1
        AND COALESCE(oo.qualifier_source_value, '') = ''
        AND COALESCE(oo.unit_source_value, '') = ''
    WHERE oo.observation_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added observation', run_id, 'tbl_SRReferralIn - Source -> observation', @@row_count, CURRENT_DATETIME();

    /*
        --[tbl_SRPatient] Transfer
    */
    SET max_current_id = COALESCE((SELECT observation_id FROM `CY_IMOSPHERE_CDM_531.observation` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.observation`
    (
        observation_id,
        person_id,
        observation_concept_id,
        observation_source_value,
        observation_date,
        observation_datetime,
        observation_type_concept_id,
        value_as_concept_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as observation_id,
        o.person_id,
        o.observation_concept_id,
        o.observation_source_value,
        o.observation_date,
        o.observation_datetime,
        o.observation_type_concept_id,
        o.value_as_concept_id
    FROM
    (
        SELECT DISTINCT
            o.person_id person_id,
            44803968 observation_concept_id,
            o.ethnicity observation_source_value,
            CAST(PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(o.DateBirth ,'01000000')) AS DATE) observation_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(o.DateBirth ,'01000000')) observation_datetime,
            32817 observation_type_concept_id, --EHR
            oc.concept_id value_as_concept_id
        FROM `CY_IMOSPHERE_WORKSPACE.tbl_SRPatient` o
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on oc.source_value = o.ethnicity AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'tbl_SRPatient' AND oc.source_column = 'ethnicity'
        WHERE o.DateBirth IS NOT NULL
        AND o.person_id IS NOT NULL
    ) o
    LEFT JOIN `CY_IMOSPHERE_CDM_531.observation` oo ON 
        oo.person_id = o.person_id
        AND oo.observation_concept_id = o.observation_concept_id
        AND oo.observation_source_value = o.observation_source_value
        AND COALESCE(oo.observation_source_concept_id, -1) = -1
        AND oo.observation_datetime = o.observation_datetime
        AND oo.observation_type_concept_id = o.observation_type_concept_id
        AND COALESCE(oo.provider_id, -1) = -1
        AND COALESCE(oo.visit_occurrence_id, -1) = -1
        AND COALESCE(oo.visit_detail_id, -1) = -1
        AND COALESCE(oo.value_as_concept_id, -1) = COALESCE(o.value_as_concept_id, -1)
        AND COALESCE(CAST(oo.value_as_number AS STRING), '') = ''
        AND COALESCE(oo.qualifier_concept_id, -1) = -1
        AND COALESCE(oo.unit_concept_id, -1) = -1
        AND COALESCE(oo.qualifier_source_value, '') = ''
        AND COALESCE(oo.unit_source_value, '') = ''
    WHERE oo.observation_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added observation', run_id, 'tbl_SRPatient -> observation', @@row_count, CURRENT_DATETIME();
        
    /*
        --[tbl_SRCode] Transfer
    */
    SET max_current_id = COALESCE((SELECT observation_id FROM `CY_IMOSPHERE_CDM_531.observation` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.observation`
    (
        observation_id,
        person_id,
        observation_concept_id,
        observation_source_value,
        observation_source_concept_id,
        observation_date,
        observation_datetime,
        observation_type_concept_id,
        value_as_number,
        unit_concept_id,
        unit_source_value,
        provider_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as observation_id,
        o.person_id,
        o.observation_concept_id,
        o.observation_source_value,
        o.observation_source_concept_id,
        o.observation_date,
        o.observation_datetime,
        o.observation_type_concept_id,
        o.value_as_number,
        o.unit_concept_id,
        o.unit_source_value,
        o.provider_id
    FROM
    (
        SELECT DISTINCT
            o.person_id person_id,
            dc.concept_id observation_concept_id,
            o.ctv3code observation_source_value,
            sc.concept_id observation_source_concept_id,
            EXTRACT(DATE FROM dateevent) observation_date,
            dateevent observation_datetime,
            32817 observation_type_concept_id, --EHR
            SAFE_CAST(numericvalue as FLOAT64) value_as_number,
            unit.concept_id unit_concept_id,
            numericunit unit_source_value,
            p.provider_id
        FROM `CY_IMOSPHERE_WORKSPACE.tbl_SRCode` o
        JOIN `CY_LOOKUPS.tbl_CTV3ToSnomed_Map` sm ON sm.ctv3code = o.ctv3code
        JOIN `CY_CDM_VOCAB.concept` sc ON sc.concept_code = sm.SNOMEDCode
        JOIN `CY_CDM_VOCAB.concept_relationship` cr ON cr.concept_id_1 = sc.concept_id
        JOIN `CY_CDM_VOCAB.concept` dc ON dc.concept_id = cr.concept_id_2
        LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` unit on unit.source_value = o.numericunit AND unit.destination_table = 'OBSERVATION' AND unit.source_table = 'tbl_SRCode' AND unit.source_column = 'numericunit'
        LEFT JOIN `CY_IMOSPHERE_CDM_531.provider` p ON p.provider_id = CAST(iddoneby AS INT64)
        WHERE sc.vocabulary_id = 'SNOMED'
        AND cr.relationship_id = 'Maps to'
        AND dc.standard_concept = 'S'
        AND dc.domain_id NOT IN ('Condition', 'Procedure', 'Drug', 'Measurement', 'Device')
        AND o.dateevent IS NOT NULL
        AND o.person_id IS NOT NULL
    ) o
    LEFT JOIN `CY_IMOSPHERE_CDM_531.observation` oo ON 
        oo.person_id = o.person_id
        AND oo.observation_concept_id = o.observation_concept_id
        AND oo.observation_source_value = o.observation_source_value
        AND COALESCE(oo.observation_source_concept_id, -1) = COALESCE(o.observation_source_concept_id, -1)
        AND oo.observation_datetime = o.observation_datetime
        AND oo.observation_type_concept_id = o.observation_type_concept_id
        AND COALESCE(oo.provider_id, -1) = COALESCE(o.provider_id, -1)
        AND COALESCE(oo.visit_occurrence_id, -1) = -1
        AND COALESCE(oo.visit_detail_id, -1) = -1
        AND COALESCE(oo.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(oo.value_as_number AS STRING), '') = COALESCE(CAST(o.value_as_number AS STRING), '')
        AND COALESCE(oo.qualifier_concept_id, -1) = -1
        AND COALESCE(oo.unit_concept_id, -1) =  COALESCE(o.unit_concept_id, -1)
        AND COALESCE(oo.qualifier_source_value, '') = ''
        AND COALESCE(oo.unit_source_value, '') =  COALESCE(o.unit_source_value, '')
    WHERE oo.observation_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added observation', run_id, 'tbl_SRCode -> observation', @@row_count, CURRENT_DATETIME();

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    ------------[SUS_BRI_OP_010415_to_300619]
    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    /*
        --[SUS_BRI_OP_010415_to_300619 - Ethnic_Category] Transfer
    */
    SET max_current_id = COALESCE((SELECT observation_id FROM `CY_IMOSPHERE_CDM_531.observation` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.observation`
    (
        observation_id,
        person_id,
        observation_concept_id,
        observation_source_value,
        observation_date,
        observation_datetime,
        observation_type_concept_id,
        value_as_concept_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as observation_id,
        o.person_id,
        o.observation_concept_id,
        o.observation_source_value,
        o.observation_date,
        o.observation_datetime,
        o.observation_type_concept_id,
        o.value_as_concept_id
    FROM
    (
        SELECT DISTINCT
            o.person_id person_id,
            44803968 observation_concept_id, --Ethnicity
            o.Ethnic_Category observation_source_value,
            PARSE_DATE('%Y%m%d', CONCAT(o.Date_of_Birth ,'01')) observation_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(o.Date_of_Birth ,'01000000')) observation_datetime,
            32817 observation_type_concept_id, --EHR
            oc.concept_id value_as_concept_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_OP_010415_to_300619` o
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on oc.source_value = o.Ethnic_Category AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_OP_010415_to_300619' AND oc.source_column = 'Ethnic_Category'
        WHERE (o.Date_of_Birth IS NOT NULL AND o.Date_of_Birth NOT LIKE '% %')
        AND o.person_id IS NOT NULL
    ) o
    LEFT JOIN `CY_IMOSPHERE_CDM_531.observation` oo ON 
        oo.person_id = o.person_id
        AND oo.observation_concept_id = o.observation_concept_id
        AND oo.observation_source_value = o.observation_source_value
        AND COALESCE(oo.observation_source_concept_id, -1) = -1
        AND oo.observation_datetime = o.observation_datetime
        AND oo.observation_type_concept_id = o.observation_type_concept_id
        AND COALESCE(oo.provider_id, -1) = -1
        AND COALESCE(oo.visit_occurrence_id, -1) = -1
        AND COALESCE(oo.visit_detail_id, -1) = -1
        AND COALESCE(oo.value_as_concept_id, -1) = COALESCE(o.value_as_concept_id, -1)
        AND COALESCE(CAST(oo.value_as_number AS STRING), '') = ''
        AND COALESCE(oo.qualifier_concept_id, -1) = -1
        AND COALESCE(oo.unit_concept_id, -1) =  -1
        AND COALESCE(oo.qualifier_source_value, '') = ''
        AND COALESCE(oo.unit_source_value, '') =  ''
    WHERE oo.observation_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added observation', run_id, 'SUS_BRI_OP_010415_to_300619 - Ethnic_Category -> observation', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_OP_010415_to_300619 - Attended_Or_Did_Not_Attend] Transfer
    */
    SET max_current_id = COALESCE((SELECT observation_id FROM `CY_IMOSPHERE_CDM_531.observation` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.observation`
    (
        observation_id,
        person_id,
        observation_concept_id,
        observation_source_value,
        observation_date,
        observation_datetime,
        observation_type_concept_id,
        value_as_concept_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as observation_id,
        o.person_id,
        o.observation_concept_id,
        o.observation_source_value,
        o.observation_date,
        o.observation_datetime,
        o.observation_type_concept_id,
        o.value_as_concept_id
    FROM
    (
        SELECT DISTINCT
            o.person_id person_id,
            443364 observation_concept_id, --Patient encounter status
            o.Attended_Or_Did_Not_Attend observation_source_value,
            PARSE_DATE('%Y%m%d', o.Appointment_Date) observation_date,
            CASE WHEN Appointment_Time LIKE '% %' OR Appointment_Time IS NULL
                THEN PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(Appointment_Date, '000000'))
                ELSE PARSE_DATETIME('%Y%m%d%H:%M%S', CONCAT(Appointment_Date, Appointment_Time, '00'))
            END observation_datetime,
            32817 observation_type_concept_id, --EHR
            oc.concept_id value_as_concept_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_OP_010415_to_300619` o
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on oc.source_value = o.Attended_Or_Did_Not_Attend AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_OP_010415_to_300619' AND oc.source_column = 'Attended_Or_Did_Not_Attend'
        WHERE o.Appointment_Date IS NOT NULL
        AND o.person_id IS NOT NULL
    ) o
    LEFT JOIN `CY_IMOSPHERE_CDM_531.observation` oo ON 
        oo.person_id = o.person_id
        AND oo.observation_concept_id = o.observation_concept_id
        AND oo.observation_source_value = o.observation_source_value
        AND COALESCE(oo.observation_source_concept_id, -1) = -1
        AND oo.observation_datetime = o.observation_datetime
        AND oo.observation_type_concept_id = o.observation_type_concept_id
        AND COALESCE(oo.provider_id, -1) = -1
        AND COALESCE(oo.visit_occurrence_id, -1) = -1
        AND COALESCE(oo.visit_detail_id, -1) = -1
        AND COALESCE(oo.value_as_concept_id, -1) = COALESCE(o.value_as_concept_id, -1)
        AND COALESCE(CAST(oo.value_as_number AS STRING), '') = ''
        AND COALESCE(oo.qualifier_concept_id, -1) = -1
        AND COALESCE(oo.unit_concept_id, -1) =  -1
        AND COALESCE(oo.qualifier_source_value, '') = ''
        AND COALESCE(oo.unit_source_value, '') =  ''
    WHERE oo.observation_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added observation', run_id, 'SUS_BRI_OP_010415_to_300619 - Attended_Or_Did_Not_Attend -> observation', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_OP_010415_to_300619 - Source_Of_Referral_For_Outpatients] Transfer
    */
    SET max_current_id = COALESCE((SELECT observation_id FROM `CY_IMOSPHERE_CDM_531.observation` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.observation`
    (
        observation_id,
        person_id,
        observation_concept_id,
        observation_source_value,
        observation_date,
        observation_datetime,
        observation_type_concept_id,
        value_as_concept_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as observation_id,
        o.person_id,
        o.observation_concept_id,
        o.observation_source_value,
        o.observation_date,
        o.observation_datetime,
        o.observation_type_concept_id,
        o.value_as_concept_id
    FROM
    (
        SELECT DISTINCT
            o.person_id person_id,
            4287771 observation_concept_id, --Referral source
            o.Source_Of_Referral_For_Outpatients observation_source_value,
            PARSE_DATE('%Y%m%d', o.Referral_Request_Received_Date) observation_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(o.Referral_Request_Received_Date ,'000000')) observation_datetime,
            32817 observation_type_concept_id, --EHR
            oc.concept_id value_as_concept_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_OP_010415_to_300619` o
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on oc.source_value = o.Source_Of_Referral_For_Outpatients AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_OP_010415_to_300619' AND oc.source_column = 'Source_Of_Referral_For_Outpatients'
        WHERE (o.Referral_Request_Received_Date IS NOT NULL AND o.Referral_Request_Received_Date NOT LIKE '% %')
        AND o.person_id IS NOT NULL
    ) o
    LEFT JOIN `CY_IMOSPHERE_CDM_531.observation` oo ON 
        oo.person_id = o.person_id
        AND oo.observation_concept_id = o.observation_concept_id
        AND oo.observation_source_value = o.observation_source_value
        AND COALESCE(oo.observation_source_concept_id, -1) = -1
        AND oo.observation_datetime = o.observation_datetime
        AND oo.observation_type_concept_id = o.observation_type_concept_id
        AND COALESCE(oo.provider_id, -1) = -1
        AND COALESCE(oo.visit_occurrence_id, -1) = -1
        AND COALESCE(oo.visit_detail_id, -1) = -1
        AND COALESCE(oo.value_as_concept_id, -1) = COALESCE(o.value_as_concept_id, -1)
        AND COALESCE(CAST(oo.value_as_number AS STRING), '') = ''
        AND COALESCE(oo.qualifier_concept_id, -1) = -1
        AND COALESCE(oo.unit_concept_id, -1) =  -1
        AND COALESCE(oo.qualifier_source_value, '') = ''
        AND COALESCE(oo.unit_source_value, '') =  ''
    WHERE oo.observation_id IS NULL;
    
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added observation', run_id, 'SUS_BRI_OP_010415_to_300619 - Source_Of_Referral_For_Outpatients -> observation', @@row_count, CURRENT_DATETIME();

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    ------------[SUS_BRI_APC_010415_to_300619_P1]
    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

    /*
        --[SUS_BRI_APC_010415_to_300619_P1 - Ethnic_Category] Transfer
    */
    SET max_current_id = COALESCE((SELECT observation_id FROM `CY_IMOSPHERE_CDM_531.observation` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.observation`
    (
        observation_id,
        person_id,
        observation_concept_id,
        observation_source_value,
        observation_date,
        observation_datetime,
        observation_type_concept_id,
        value_as_concept_id,
        visit_occurrence_id,
        visit_detail_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as observation_id,
        o.person_id,
        o.observation_concept_id,
        o.observation_source_value,
        o.observation_date,
        o.observation_datetime,
        o.observation_type_concept_id,
        o.value_as_concept_id,
        o.visit_occurrence_id,
        o.visit_detail_id
    FROM
    (
        SELECT DISTINCT
            o.person_id person_id,
            44803968 observation_concept_id, --Ethnicity
            o.Ethnic_Group observation_source_value,
            PARSE_DATE('%Y%m%d', CONCAT(o.Date_of_Birth ,'01')) observation_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(o.Date_of_Birth ,'01000000')) observation_datetime,
            32817 observation_type_concept_id, --EHR
            oc.concept_id value_as_concept_id,
            vl.id visit_occurrence_id,
            vdl.id visit_detail_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1` o
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on oc.source_value = o.Ethnic_Group AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_APC_010415_to_300619_P1' AND oc.source_column = 'Ethnic_Group'
        LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups` vdl ON vdl.source_value = CONCAT(CAST(o.Hospital_Provider_Spell_Number as STRING),'_', o.person_id, '_', o.Episode_Number)
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = CONCAT(CAST(o.Hospital_Provider_Spell_Number as STRING),'_', o.person_id) 
        WHERE (o.Date_of_Birth IS NOT NULL AND o.Date_of_Birth NOT LIKE '% %')
        AND o.person_id IS NOT NULL
    ) o
    LEFT JOIN `CY_IMOSPHERE_CDM_531.observation` oo ON 
        oo.person_id = o.person_id
        AND oo.observation_concept_id = o.observation_concept_id
        AND oo.observation_source_value = o.observation_source_value
        AND COALESCE(oo.observation_source_concept_id, -1) = -1
        AND oo.observation_datetime = o.observation_datetime
        AND oo.observation_type_concept_id = o.observation_type_concept_id
        AND COALESCE(oo.provider_id, -1) = -1
        AND COALESCE(oo.visit_occurrence_id, -1) = COALESCE(o.visit_occurrence_id, -1)
        AND COALESCE(oo.visit_detail_id, -1) = COALESCE(o.visit_detail_id, -1)
        AND COALESCE(oo.value_as_concept_id, -1) = COALESCE(o.value_as_concept_id, -1)
        AND COALESCE(CAST(oo.value_as_number AS STRING), '') = ''
        AND COALESCE(oo.qualifier_concept_id, -1) = -1
        AND COALESCE(oo.unit_concept_id, -1) =  -1
        AND COALESCE(oo.qualifier_source_value, '') = ''
        AND COALESCE(oo.unit_source_value, '') =  ''
    WHERE oo.observation_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added observation', run_id, 'SUS_BRI_APC_010415_to_300619_P1 - Ethnic_Category -> observation', @@row_count, CURRENT_DATETIME();
        
    /*
        --[SUS_BRI_APC_010415_to_300619_P1 - Discharge_Method_Hospital_Provider_Spell] Transfer   
    */
    SET max_current_id = COALESCE((SELECT observation_id FROM `CY_IMOSPHERE_CDM_531.observation` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.observation`
    (
        observation_id,
        person_id,
        observation_concept_id,
        observation_source_value,
        observation_date,
        observation_datetime,
        observation_type_concept_id,
        value_as_concept_id,
        visit_occurrence_id,
        visit_detail_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as observation_id,
        o.person_id,
        o.observation_concept_id,
        o.observation_source_value,
        o.observation_date,
        o.observation_datetime,
        o.observation_type_concept_id,
        o.value_as_concept_id,
        o.visit_occurrence_id,
        o.visit_detail_id
    FROM
    (
        SELECT DISTINCT
            o.person_id person_id,
            4240723 observation_concept_id, --Patient discharge
            o.Discharge_Method_Hospital_Provider_Spell observation_source_value,
            PARSE_DATE('%Y%m%d', o.Discharge_Date_From_Hospital_Provider_Spell) observation_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(o.Discharge_Date_From_Hospital_Provider_Spell ,'000000')) observation_datetime,
            32817 observation_type_concept_id, --EHR
            oc.concept_id value_as_concept_id,
            vl.id visit_occurrence_id,
            vdl.id visit_detail_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1` o
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on oc.source_value = o.Discharge_Method_Hospital_Provider_Spell AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_APC_010415_to_300619_P1' AND oc.source_column =  'Discharge_Method_Hospital_Provider_Spell'
        LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups` vdl ON vdl.source_value = CONCAT(CAST(o.Hospital_Provider_Spell_Number as STRING),'_', o.person_id, '_', o.Episode_Number)
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = CONCAT(CAST(o.Hospital_Provider_Spell_Number as STRING),'_', o.person_id) 
        WHERE (o.Discharge_Date_From_Hospital_Provider_Spell IS NOT NULL AND o.Discharge_Date_From_Hospital_Provider_Spell NOT LIKE '% %')
        AND o.person_id IS NOT NULL
    ) o
    LEFT JOIN `CY_IMOSPHERE_CDM_531.observation` oo ON 
        oo.person_id = o.person_id
        AND oo.observation_concept_id = o.observation_concept_id
        AND oo.observation_source_value = o.observation_source_value
        AND COALESCE(oo.observation_source_concept_id, -1) = -1
        AND oo.observation_datetime = o.observation_datetime
        AND oo.observation_type_concept_id = o.observation_type_concept_id
        AND COALESCE(oo.provider_id, -1) = -1
        AND COALESCE(oo.visit_occurrence_id, -1) = COALESCE(o.visit_occurrence_id, -1)
        AND COALESCE(oo.visit_detail_id, -1) = COALESCE(o.visit_detail_id, -1)
        AND COALESCE(oo.value_as_concept_id, -1) = COALESCE(o.value_as_concept_id, -1)
        AND COALESCE(CAST(oo.value_as_number AS STRING), '') = ''
        AND COALESCE(oo.qualifier_concept_id, -1) = -1
        AND COALESCE(oo.unit_concept_id, -1) =  -1
        AND COALESCE(oo.qualifier_source_value, '') = ''
        AND COALESCE(oo.unit_source_value, '') =  ''
    WHERE oo.observation_id IS NULL;
    
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added observation',  run_id, 'SUS_BRI_APC_010415_to_300619_P1 - Discharge_Method_Hospital_Provider_Spell -> observation', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_APC_010415_to_300619_P1 - Admission_Method_Hospital_Provider_Spell] Transfer   
    */
    SET max_current_id = COALESCE((SELECT observation_id FROM `CY_IMOSPHERE_CDM_531.observation` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.observation`
    (
        observation_id,
        person_id,
        observation_concept_id,
        observation_source_value,
        observation_date,
        observation_datetime,
        observation_type_concept_id,
        value_as_concept_id,
        visit_occurrence_id,
        visit_detail_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as observation_id,
        o.person_id,
        o.observation_concept_id,
        o.observation_source_value,
        o.observation_date,
        o.observation_datetime,
        o.observation_type_concept_id,
        o.value_as_concept_id,
        o.visit_occurrence_id,
        o.visit_detail_id
    FROM
    (
        SELECT DISTINCT
            o.person_id person_id,
            8715 observation_concept_id, --Hospital Admission
            o.Admission_Method_Hospital_Provider_Spell observation_source_value,
            PARSE_DATE('%Y%m%d', o.Start_Date_Hospital_Provider_Spell) observation_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(o.Start_Date_Hospital_Provider_Spell ,'000000')) observation_datetime,
            32817 observation_type_concept_id, --EHR
            oc.concept_id value_as_concept_id,
            vl.id visit_occurrence_id,
            vdl.id visit_detail_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1` o
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on oc.source_value = o.Admission_Method_Hospital_Provider_Spell AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_APC_010415_to_300619_P1' AND oc.source_column =  'Admission_Method_Hospital_Provider_Spell'
        LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups` vdl ON vdl.source_value = CONCAT(CAST(o.Hospital_Provider_Spell_Number as STRING),'_', o.person_id, '_', o.Episode_Number)
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = CONCAT(CAST(o.Hospital_Provider_Spell_Number as STRING),'_', o.person_id) 
        WHERE (o.Start_Date_Hospital_Provider_Spell IS NOT NULL AND o.Start_Date_Hospital_Provider_Spell NOT LIKE '% %')
        AND o.person_id IS NOT NULL
    ) o
    LEFT JOIN `CY_IMOSPHERE_CDM_531.observation` oo ON 
        oo.person_id = o.person_id
        AND oo.observation_concept_id = o.observation_concept_id
        AND oo.observation_source_value = o.observation_source_value
        AND COALESCE(oo.observation_source_concept_id, -1) = -1
        AND oo.observation_datetime = o.observation_datetime
        AND oo.observation_type_concept_id = o.observation_type_concept_id
        AND COALESCE(oo.provider_id, -1) = -1
        AND COALESCE(oo.visit_occurrence_id, -1) = COALESCE(o.visit_occurrence_id, -1)
        AND COALESCE(oo.visit_detail_id, -1) = COALESCE(o.visit_detail_id, -1)
        AND COALESCE(oo.value_as_concept_id, -1) = COALESCE(o.value_as_concept_id, -1)
        AND COALESCE(CAST(oo.value_as_number AS STRING), '') = ''
        AND COALESCE(oo.qualifier_concept_id, -1) = -1
        AND COALESCE(oo.unit_concept_id, -1) =  -1
        AND COALESCE(oo.qualifier_source_value, '') = ''
        AND COALESCE(oo.unit_source_value, '') =  ''
    WHERE oo.observation_id IS NULL;
         
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added observation', run_id, 'SUS_BRI_APC_010415_to_300619_P1 - Admission_Method_Hospital_Provider_Spell -> observation', @@row_count, CURRENT_DATETIME();


    /*
        --[SUS_BRI_APC_010415_to_300619_P1 - Discharge_Destination_Hospital_Provider_Spell] Transfer   
    */
    SET max_current_id = COALESCE((SELECT observation_id FROM `CY_IMOSPHERE_CDM_531.observation` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.observation`
    (
        observation_id,
        person_id,
        observation_concept_id,
        observation_source_value,
        observation_date,
        observation_datetime,
        observation_type_concept_id,
        value_as_concept_id,
        visit_occurrence_id,
        visit_detail_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as observation_id,
        o.person_id,
        o.observation_concept_id,
        o.observation_source_value,
        o.observation_date,
        o.observation_datetime,
        o.observation_type_concept_id,
        o.value_as_concept_id,
        o.visit_occurrence_id,
        o.visit_detail_id
    FROM
    (
        SELECT DISTINCT
            o.person_id person_id,
            4240723 observation_concept_id, --Patient discharge
            o.Discharge_Destination_Hospital_Provider_Spell observation_source_value,
            PARSE_DATE('%Y%m%d', o.Discharge_Date_From_Hospital_Provider_Spell) observation_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(o.Discharge_Date_From_Hospital_Provider_Spell ,'000000')) observation_datetime,
            32817 observation_type_concept_id, --EHR
            oc.concept_id value_as_concept_id,
            vl.id visit_occurrence_id,
            vdl.id visit_detail_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1` o
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on oc.source_value = o.Discharge_Destination_Hospital_Provider_Spell AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_APC_010415_to_300619_P1' AND oc.source_column =  'Discharge_Destination_Hospital_Provider_Spell'
        LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups` vdl ON vdl.source_value = CONCAT(CAST(o.Hospital_Provider_Spell_Number as STRING),'_', o.person_id, '_', o.Episode_Number)
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = CONCAT(CAST(o.Hospital_Provider_Spell_Number as STRING),'_', o.person_id) 
        WHERE (o.Discharge_Date_From_Hospital_Provider_Spell IS NOT NULL AND o.Discharge_Date_From_Hospital_Provider_Spell NOT LIKE '% %')
        AND o.person_id IS NOT NULL
    ) o
    LEFT JOIN `CY_IMOSPHERE_CDM_531.observation` oo ON 
        oo.person_id = o.person_id
        AND oo.observation_concept_id = o.observation_concept_id
        AND oo.observation_source_value = o.observation_source_value
        AND COALESCE(oo.observation_source_concept_id, -1) = -1
        AND oo.observation_datetime = o.observation_datetime
        AND oo.observation_type_concept_id = o.observation_type_concept_id
        AND COALESCE(oo.provider_id, -1) = -1
        AND COALESCE(oo.visit_occurrence_id, -1) = COALESCE(o.visit_occurrence_id, -1)
        AND COALESCE(oo.visit_detail_id, -1) = COALESCE(o.visit_detail_id, -1)
        AND COALESCE(oo.value_as_concept_id, -1) = COALESCE(o.value_as_concept_id, -1)
        AND COALESCE(CAST(oo.value_as_number AS STRING), '') = ''
        AND COALESCE(oo.qualifier_concept_id, -1) = -1
        AND COALESCE(oo.unit_concept_id, -1) =  -1
        AND COALESCE(oo.qualifier_source_value, '') = ''
        AND COALESCE(oo.unit_source_value, '') =  ''
    WHERE oo.observation_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added observation', run_id, 'SUS_BRI_APC_010415_to_300619_P1 - Discharge_Destination_Hospital_Provider_Spell -> observation', @@row_count, CURRENT_DATETIME();

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    ------------[src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2]
    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
        
    /*
        --[src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2 - Ethnic_Group] Transfer
    */
    SET max_current_id = COALESCE((SELECT observation_id FROM `CY_IMOSPHERE_CDM_531.observation` ORDER BY 1 DESC LIMIT 1), 0);
    
    INSERT INTO `CY_IMOSPHERE_CDM_531.observation`
    (
        observation_id,
        person_id,
        observation_concept_id,
        observation_source_value,
        observation_date,
        observation_datetime,
        observation_type_concept_id,
        value_as_concept_id,
        visit_occurrence_id,
        visit_detail_id
    )

    WITH dob_cte AS
    (
      SELECT DISTINCT        
        p.person_id person_id,
        CASE WHEN CAST(SPLIT(Date_of_Birth_mm_yy, '-')[OFFSET(1)] AS INT64) BETWEEN 00 AND 22 THEN CONCAT('20', CAST(SPLIT(Date_of_Birth_mm_yy, '-')[OFFSET(1)] as STRING))
        ELSE CONCAT('19', CAST(SPLIT(Date_of_Birth_mm_yy, '-')[OFFSET(1)] as STRING))      
        END year_of_birth,
        FORMAT_DATE( "%m", PARSE_DATE("%b-%y",Date_of_Birth_mm_yy )) month_of_birth
      FROM `CY_IMOSPHERE_WORKSPACE.src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2` p
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as observation_id,
        o.person_id,
        o.observation_concept_id,
        o.observation_source_value,
        o.observation_date,
        o.observation_datetime,
        o.observation_type_concept_id,
        o.value_as_concept_id,
        o.visit_occurrence_id,
        o.visit_detail_id
    FROM
    (
        SELECT DISTINCT
            o.person_id person_id,
            44803968 observation_concept_id, --Ethnicity
            o.Ethnic_Group observation_source_value,
            PARSE_DATE('%Y%m%d', CONCAT(dob.year_of_birth, dob.month_of_birth ,'01')) observation_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(dob.year_of_birth, dob.month_of_birth ,'01000000')) observation_datetime,
            32817 observation_type_concept_id, --EHR
            oc.concept_id value_as_concept_id,
            vl.id visit_occurrence_id,
            vdl.id visit_detail_id
        FROM `CY_IMOSPHERE_WORKSPACE.src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2` o
        JOIN dob_cte dob ON dob.person_id = o.person_id
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on oc.source_value = o.Ethnic_Group AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2' AND oc.source_column = 'Ethnic_Group'
        LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups` vdl ON vdl.source_value = CONCAT(CAST(o.Hospital_Provider_Spell_Number as STRING),'_', o.person_id, '_', o.Episode_Number)
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = CONCAT(CAST(o.Hospital_Provider_Spell_Number as STRING),'_', o.person_id) 
        WHERE (o.Date_of_Birth_mm_yy IS NOT NULL AND o.Date_of_Birth_mm_yy NOT LIKE '% %')
        AND o.person_id IS NOT NULL
    ) o
    LEFT JOIN `CY_IMOSPHERE_CDM_531.observation` oo ON 
        oo.person_id = o.person_id
        AND oo.observation_concept_id = o.observation_concept_id
        AND oo.observation_source_value = o.observation_source_value
        AND COALESCE(oo.observation_source_concept_id, -1) = -1
        AND oo.observation_datetime = o.observation_datetime
        AND oo.observation_type_concept_id = o.observation_type_concept_id
        AND COALESCE(oo.provider_id, -1) = -1
        AND COALESCE(oo.visit_occurrence_id, -1) = COALESCE(o.visit_occurrence_id, -1)
        AND COALESCE(oo.visit_detail_id, -1) = COALESCE(o.visit_detail_id, -1)
        AND COALESCE(oo.value_as_concept_id, -1) = COALESCE(o.value_as_concept_id, -1)
        AND COALESCE(CAST(oo.value_as_number AS STRING), '') = ''
        AND COALESCE(oo.qualifier_concept_id, -1) = -1
        AND COALESCE(oo.unit_concept_id, -1) =  -1
        AND COALESCE(oo.qualifier_source_value, '') = ''
        AND COALESCE(oo.unit_source_value, '') =  ''
    WHERE oo.observation_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added observation', run_id, 'src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2 - Ethnic_Group -> observation', @@row_count, CURRENT_DATETIME();

    /*
        --[src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2 - Discharge_Method_Hospital_Provider_Spell] Transfer   
    */
    SET max_current_id = COALESCE((SELECT observation_id FROM `CY_IMOSPHERE_CDM_531.observation` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.observation`
    (
        observation_id,
        person_id,
        observation_concept_id,
        observation_source_value,
        observation_date,
        observation_datetime,
        observation_type_concept_id,
        value_as_concept_id,
        visit_occurrence_id,
        visit_detail_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as observation_id,
        o.person_id,
        o.observation_concept_id,
        o.observation_source_value,
        o.observation_date,
        o.observation_datetime,
        o.observation_type_concept_id,
        o.value_as_concept_id,
        o.visit_occurrence_id,
        o.visit_detail_id
    FROM
    (
        SELECT DISTINCT
            o.person_id person_id,
            4240723 observation_concept_id, --Patient discharge
            o.Discharge_Method_Hospital_Provider_Spell observation_source_value,
            PARSE_DATE('%Y%m%d', o.Discharge_Date_From_Hospital_Provider_Spell) observation_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(o.Discharge_Date_From_Hospital_Provider_Spell ,'000000')) observation_datetime,
            32817 observation_type_concept_id, --EHR
            oc.concept_id value_as_concept_id,
            vl.id visit_occurrence_id,
            vdl.id visit_detail_id
        FROM `CY_IMOSPHERE_WORKSPACE.src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2` o
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on oc.source_value = o.Discharge_Method_Hospital_Provider_Spell AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2' AND oc.source_column = 'Discharge_Method_Hospital_Provider_Spell'
        LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups` vdl ON vdl.source_value = CONCAT(CAST(o.Hospital_Provider_Spell_Number as STRING),'_', o.person_id, '_', o.Episode_Number)
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = CONCAT(CAST(o.Hospital_Provider_Spell_Number as STRING),'_', o.person_id) 
        WHERE (o.Discharge_Date_From_Hospital_Provider_Spell IS NOT NULL AND o.Discharge_Date_From_Hospital_Provider_Spell NOT LIKE '% %')
        AND o.person_id IS NOT NULL
    ) o
    LEFT JOIN `CY_IMOSPHERE_CDM_531.observation` oo ON 
        oo.person_id = o.person_id
        AND oo.observation_concept_id = o.observation_concept_id
        AND oo.observation_source_value = o.observation_source_value
        AND COALESCE(oo.observation_source_concept_id, -1) = -1
        AND oo.observation_datetime = o.observation_datetime
        AND oo.observation_type_concept_id = o.observation_type_concept_id
        AND COALESCE(oo.provider_id, -1) = -1
        AND COALESCE(oo.visit_occurrence_id, -1) = COALESCE(o.visit_occurrence_id, -1)
        AND COALESCE(oo.visit_detail_id, -1) = COALESCE(o.visit_detail_id, -1)
        AND COALESCE(oo.value_as_concept_id, -1) = COALESCE(o.value_as_concept_id, -1)
        AND COALESCE(CAST(oo.value_as_number AS STRING), '') = ''
        AND COALESCE(oo.qualifier_concept_id, -1) = -1
        AND COALESCE(oo.unit_concept_id, -1) =  -1
        AND COALESCE(oo.qualifier_source_value, '') = ''
        AND COALESCE(oo.unit_source_value, '') =  ''
    WHERE oo.observation_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added observation', run_id, 'src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2 - Discharge_Method_Hospital_Provider_Spell -> observation', @@row_count, CURRENT_DATETIME();

    /*
        --[src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2 - Admission_Method_Hospital_Provider_Spell] Transfer   
    */
    SET max_current_id = COALESCE((SELECT observation_id FROM `CY_IMOSPHERE_CDM_531.observation` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.observation`
    (
        observation_id,
        person_id,
        observation_concept_id,
        observation_source_value,
        observation_date,
        observation_datetime,
        observation_type_concept_id,
        value_as_concept_id,
        visit_occurrence_id,
        visit_detail_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as observation_id,
        o.person_id,
        o.observation_concept_id,
        o.observation_source_value,
        o.observation_date,
        o.observation_datetime,
        o.observation_type_concept_id,
        o.value_as_concept_id,
        o.visit_occurrence_id,
        o.visit_detail_id
    FROM
    (
        SELECT DISTINCT
            o.person_id person_id,
            8715 observation_concept_id, --Hospital Admission
            o.Admission_Method_Hospital_Provider_Spell observation_source_value,
            PARSE_DATE('%Y%m%d', o.Start_Date_Hospital_Provider_Spell) observation_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(o.Start_Date_Hospital_Provider_Spell ,'000000')) observation_datetime,
            32817 observation_type_concept_id, --EHR
            oc.concept_id value_as_concept_id,
            vl.id visit_occurrence_id,
            vdl.id visit_detail_id
        FROM `CY_IMOSPHERE_WORKSPACE.src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2` o
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on oc.source_value = o.Admission_Method_Hospital_Provider_Spell AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_APC_010415_to_300619_P1' AND oc.source_column =  'Admission_Method_Hospital_Provider_Spell'
        LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups` vdl ON vdl.source_value = CONCAT(CAST(o.Hospital_Provider_Spell_Number as STRING),'_', o.person_id, '_', o.Episode_Number)
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = CONCAT(CAST(o.Hospital_Provider_Spell_Number as STRING),'_', o.person_id) 
        WHERE (o.Start_Date_Hospital_Provider_Spell IS NOT NULL AND o.Start_Date_Hospital_Provider_Spell NOT LIKE '% %')
        AND o.person_id IS NOT NULL
    ) o
    LEFT JOIN `CY_IMOSPHERE_CDM_531.observation` oo ON 
        oo.person_id = o.person_id
        AND oo.observation_concept_id = o.observation_concept_id
        AND oo.observation_source_value = o.observation_source_value
        AND COALESCE(oo.observation_source_concept_id, -1) = -1
        AND oo.observation_datetime = o.observation_datetime
        AND oo.observation_type_concept_id = o.observation_type_concept_id
        AND COALESCE(oo.provider_id, -1) = -1
        AND COALESCE(oo.visit_occurrence_id, -1) = COALESCE(o.visit_occurrence_id, -1)
        AND COALESCE(oo.visit_detail_id, -1) = COALESCE(o.visit_detail_id, -1)
        AND COALESCE(oo.value_as_concept_id, -1) = COALESCE(o.value_as_concept_id, -1)
        AND COALESCE(CAST(oo.value_as_number AS STRING), '') = ''
        AND COALESCE(oo.qualifier_concept_id, -1) = -1
        AND COALESCE(oo.unit_concept_id, -1) =  -1
        AND COALESCE(oo.qualifier_source_value, '') = ''
        AND COALESCE(oo.unit_source_value, '') =  ''
    WHERE oo.observation_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added observation', run_id, 'src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2 - Admission_Method_Hospital_Provider_Spell -> observation', @@row_count, CURRENT_DATETIME();

    /*
        --[src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2 - Discharge_Destination_Hospital_Provider_Spell] Transfer   
    */
    SET max_current_id = COALESCE((SELECT observation_id FROM `CY_IMOSPHERE_CDM_531.observation` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.observation`
    (
        observation_id,
        person_id,
        observation_concept_id,
        observation_source_value,
        observation_date,
        observation_datetime,
        observation_type_concept_id,
        value_as_concept_id,
        visit_occurrence_id,
        visit_detail_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as observation_id,
        o.person_id,
        o.observation_concept_id,
        o.observation_source_value,
        o.observation_date,
        o.observation_datetime,
        o.observation_type_concept_id,
        o.value_as_concept_id,
        o.visit_occurrence_id,
        o.visit_detail_id
    FROM
    (
        SELECT DISTINCT
            o.person_id person_id,
            4240723 observation_concept_id, --Patient discharge
            o.Discharge_Destination_Hospital_Provider_Spell observation_source_value,
            PARSE_DATE('%Y%m%d', o.Discharge_Date_From_Hospital_Provider_Spell) observation_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(o.Discharge_Date_From_Hospital_Provider_Spell ,'000000')) observation_datetime,
            32817 observation_type_concept_id, --EHR
            CASE 
              WHEN Discharge_Destination_Hospital_Provider_Spell = '19' THEN 4140634 --Discharge to home
              WHEN Discharge_Destination_Hospital_Provider_Spell = '79' THEN 4194377 --Patient discharge, deceased, no autopsy
            END value_as_concept_id,
            vl.id visit_occurrence_id,
            vdl.id visit_detail_id
        FROM `CY_IMOSPHERE_WORKSPACE.src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2` o
        LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups` vdl ON vdl.source_value = CONCAT(CAST(o.Hospital_Provider_Spell_Number as STRING),'_', o.person_id, '_', o.Episode_Number)
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = CONCAT(CAST(o.Hospital_Provider_Spell_Number as STRING),'_', o.person_id) 
        WHERE (o.Discharge_Date_From_Hospital_Provider_Spell IS NOT NULL AND o.Discharge_Date_From_Hospital_Provider_Spell NOT LIKE '% %')
        AND o.person_id IS NOT NULL
        AND o.Discharge_Destination_Hospital_Provider_Spell IN ('19', '79')
    ) o
    LEFT JOIN `CY_IMOSPHERE_CDM_531.observation` oo ON 
        oo.person_id = o.person_id
        AND oo.observation_concept_id = o.observation_concept_id
        AND oo.observation_source_value = o.observation_source_value
        AND COALESCE(oo.observation_source_concept_id, -1) = -1
        AND oo.observation_datetime = o.observation_datetime
        AND oo.observation_type_concept_id = o.observation_type_concept_id
        AND COALESCE(oo.provider_id, -1) = -1
        AND COALESCE(oo.visit_occurrence_id, -1) = COALESCE(o.visit_occurrence_id, -1)
        AND COALESCE(oo.visit_detail_id, -1) = COALESCE(o.visit_detail_id, -1)
        AND COALESCE(oo.value_as_concept_id, -1) = COALESCE(o.value_as_concept_id, -1)
        AND COALESCE(CAST(oo.value_as_number AS STRING), '') = ''
        AND COALESCE(oo.qualifier_concept_id, -1) = -1
        AND COALESCE(oo.unit_concept_id, -1) =  -1
        AND COALESCE(oo.qualifier_source_value, '') = ''
        AND COALESCE(oo.unit_source_value, '') =  ''
    WHERE oo.observation_id IS NULL;
         
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added observation', run_id, 'src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2 - Discharge_Destination_Hospital_Provider_Spell -> observation', @@row_count, CURRENT_DATETIME();

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    ------------[SUS_BRI_AE_010415_to_300619]
    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

    /*
        --[SUS_BRI_AE_010415_to_300619 - Ethnic_Category] Transfer   
    */
    SET max_current_id = COALESCE((SELECT observation_id FROM `CY_IMOSPHERE_CDM_531.observation` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.observation`
    (
        observation_id,
        person_id,
        observation_concept_id,
        observation_source_value,
        observation_date,
        observation_datetime,
        observation_type_concept_id,
        value_as_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as observation_id,
        o.person_id,
        o.observation_concept_id,
        o.observation_source_value,
        o.observation_date,
        o.observation_datetime,
        o.observation_type_concept_id,
        o.value_as_concept_id,
        o.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            o.person_id person_id,
            44803968 observation_concept_id, --Ethnicity
            o.Ethnic_Category observation_source_value,
            PARSE_DATE("%Y%m%d", CONCAT(Date_of_Birth ,'01')) observation_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(Date_of_Birth ,'01000000')) observation_datetime,
            32817 observation_type_concept_id, --EHR
            oc.concept_id value_as_concept_id,
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` o
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on oc.source_value = o.Ethnic_Category AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_AE_010415_to_300619' AND oc.source_column = 'Ethnic_Category'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(o.AandE_Attendance_Number as STRING),'_', o.person_id) 
        WHERE (o.Date_of_Birth IS NOT NULL AND o.Date_of_Birth NOT LIKE '% %')
        AND o.person_id IS NOT NULL
    ) o
    LEFT JOIN `CY_IMOSPHERE_CDM_531.observation` oo ON 
        oo.person_id = o.person_id
        AND oo.observation_concept_id = o.observation_concept_id
        AND oo.observation_source_value = o.observation_source_value
        AND COALESCE(oo.observation_source_concept_id, -1) = -1
        AND oo.observation_datetime = o.observation_datetime
        AND oo.observation_type_concept_id = o.observation_type_concept_id
        AND COALESCE(oo.provider_id, -1) = -1
        AND COALESCE(oo.visit_occurrence_id, -1) = COALESCE(o.visit_occurrence_id, -1)
        AND COALESCE(oo.visit_detail_id, -1) = -1
        AND COALESCE(oo.value_as_concept_id, -1) = COALESCE(o.value_as_concept_id, -1)
        AND COALESCE(CAST(oo.value_as_number AS STRING), '') = ''
        AND COALESCE(oo.qualifier_concept_id, -1) = -1
        AND COALESCE(oo.unit_concept_id, -1) =  -1
        AND COALESCE(oo.qualifier_source_value, '') = ''
        AND COALESCE(oo.unit_source_value, '') =  ''
    WHERE oo.observation_id IS NULL;
         
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added observation', run_id, 'SUS_BRI_AE_010415_to_300619 - Ethnic_Category -> observation', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Source_of_Referral_For_AandE] Transfer   
    */
    SET max_current_id = COALESCE((SELECT observation_id FROM `CY_IMOSPHERE_CDM_531.observation` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.observation`
    (
        observation_id,
        person_id,
        observation_concept_id,
        observation_source_value,
        observation_date,
        observation_datetime,
        observation_type_concept_id,
        value_as_concept_id,
        visit_occurrence_id,
        qualifier_concept_id,
        qualifier_source_value
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as observation_id,
        o.person_id,
        o.observation_concept_id,
        o.observation_source_value,
        o.observation_date,
        o.observation_datetime,
        o.observation_type_concept_id,
        o.value_as_concept_id,
        o.visit_occurrence_id,
        o.qualifier_concept_id,
        o.qualifier_source_value
    FROM
    (
        SELECT DISTINCT
            o.person_id person_id,
            4287771 observation_concept_id, --Referral source
            o.Source_of_Referral_For_AandE observation_source_value,
            PARSE_DATE('%Y%m%d', o.Arrival_Date) observation_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(o.Arrival_Date ,'000000')) observation_datetime,
            32817 observation_type_concept_id, --EHR
            oc.concept_id value_as_concept_id,
            vl.id visit_occurrence_id,
            44804205 qualifier_concept_id,
            'Emergency Services' qualifier_source_value
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` o
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on oc.source_value = o.Source_of_Referral_For_AandE AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_AE_010415_to_300619' AND oc.source_column = 'Source_of_Referral_For_AandE'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(o.AandE_Attendance_Number as STRING),'_', o.person_id) 
        WHERE (o.Arrival_Date IS NOT NULL AND o.Arrival_Date NOT LIKE '% %')
        AND o.person_id IS NOT NULL
    ) o
    LEFT JOIN `CY_IMOSPHERE_CDM_531.observation` oo ON 
        oo.person_id = o.person_id
        AND oo.observation_concept_id = o.observation_concept_id
        AND oo.observation_source_value = o.observation_source_value
        AND COALESCE(oo.observation_source_concept_id, -1) = -1
        AND oo.observation_datetime = o.observation_datetime
        AND oo.observation_type_concept_id = o.observation_type_concept_id
        AND COALESCE(oo.provider_id, -1) = -1
        AND COALESCE(oo.visit_occurrence_id, -1) = COALESCE(o.visit_occurrence_id, -1)
        AND COALESCE(oo.visit_detail_id, -1) = -1
        AND COALESCE(oo.value_as_concept_id, -1) = COALESCE(o.value_as_concept_id, -1)
        AND COALESCE(CAST(oo.value_as_number AS STRING), '') = ''
        AND COALESCE(oo.qualifier_concept_id, -1) = COALESCE(o.qualifier_concept_id, -1)
        AND COALESCE(oo.unit_concept_id, -1) =  -1
        AND COALESCE(oo.qualifier_source_value, '') = COALESCE(o.qualifier_source_value, '')
        AND COALESCE(oo.unit_source_value, '') =  ''
    WHERE oo.observation_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added observation', run_id, 'SUS_BRI_AE_010415_to_300619 - Source_of_Referral_For_AandE -> observation', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_First] Transfer   
    */
    SET max_current_id = COALESCE((SELECT observation_id FROM `CY_IMOSPHERE_CDM_531.observation` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.observation`
    (
        observation_id,
        person_id,
        observation_concept_id,
        observation_source_value,
        observation_date,
        observation_datetime,
        observation_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as observation_id,
        o.person_id,
        o.observation_concept_id,
        o.observation_source_value,
        o.observation_date,
        o.observation_datetime,
        o.observation_type_concept_id,
        o.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            o.person_id person_id,
            oc.concept_id observation_concept_id,
            o.Accident_And_Emergency_Investigation_First observation_source_value,
            PARSE_DATE('%Y%m%d', o.Arrival_Date) observation_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(o.Arrival_Date ,'000000')) observation_datetime,
            32817 observation_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` o
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on TRIM(oc.source_value) = TRIM(o.Accident_And_Emergency_Investigation_First) AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_AE_010415_to_300619' AND oc.source_column = 'Accident_And_Emergency_Investigation_First'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(o.AandE_Attendance_Number as STRING),'_', o.person_id) 
        WHERE (o.Arrival_Date IS NOT NULL AND o.Arrival_Date NOT LIKE '% %')
        AND o.person_id IS NOT NULL
    ) o
    LEFT JOIN `CY_IMOSPHERE_CDM_531.observation` oo ON 
        oo.person_id = o.person_id
        AND oo.observation_concept_id = o.observation_concept_id
        AND oo.observation_source_value = o.observation_source_value
        AND COALESCE(oo.observation_source_concept_id, -1) = -1
        AND oo.observation_datetime = o.observation_datetime
        AND oo.observation_type_concept_id = o.observation_type_concept_id
        AND COALESCE(oo.provider_id, -1) = -1
        AND COALESCE(oo.visit_occurrence_id, -1) = COALESCE(o.visit_occurrence_id, -1)
        AND COALESCE(oo.visit_detail_id, -1) = -1
        AND COALESCE(oo.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(oo.value_as_number AS STRING), '') = ''
        AND COALESCE(oo.qualifier_concept_id, -1) = -1
        AND COALESCE(oo.unit_concept_id, -1) =  -1
        AND COALESCE(oo.qualifier_source_value, '') = ''
        AND COALESCE(oo.unit_source_value, '') =  ''
    WHERE oo.observation_id IS NULL;
         
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added observation', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_First -> observation', @@row_count, CURRENT_DATETIME();

    
    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Second] Transfer   
    */
    SET max_current_id = COALESCE((SELECT observation_id FROM `CY_IMOSPHERE_CDM_531.observation` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.observation`
    (
        observation_id,
        person_id,
        observation_concept_id,
        observation_source_value,
        observation_date,
        observation_datetime,
        observation_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as observation_id,
        o.person_id,
        o.observation_concept_id,
        o.observation_source_value,
        o.observation_date,
        o.observation_datetime,
        o.observation_type_concept_id,
        o.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            o.person_id person_id,
            oc.concept_id observation_concept_id,
            o.Accident_And_Emergency_Investigation_Second observation_source_value,
            PARSE_DATE('%Y%m%d', o.Arrival_Date) observation_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(o.Arrival_Date ,'000000')) observation_datetime,
            32817 observation_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` o
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on TRIM(oc.source_value) = TRIM(o.Accident_And_Emergency_Investigation_Second) AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_AE_010415_to_300619' AND oc.source_column = 'Accident_And_Emergency_Investigation_Second'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(o.AandE_Attendance_Number as STRING),'_', o.person_id) 
        WHERE (o.Arrival_Date IS NOT NULL AND o.Arrival_Date NOT LIKE '% %')
        AND o.person_id IS NOT NULL
    ) o
    LEFT JOIN `CY_IMOSPHERE_CDM_531.observation` oo ON 
        oo.person_id = o.person_id
        AND oo.observation_concept_id = o.observation_concept_id
        AND oo.observation_source_value = o.observation_source_value
        AND COALESCE(oo.observation_source_concept_id, -1) = -1
        AND oo.observation_datetime = o.observation_datetime
        AND oo.observation_type_concept_id = o.observation_type_concept_id
        AND COALESCE(oo.provider_id, -1) = -1
        AND COALESCE(oo.visit_occurrence_id, -1) = COALESCE(o.visit_occurrence_id, -1)
        AND COALESCE(oo.visit_detail_id, -1) = -1
        AND COALESCE(oo.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(oo.value_as_number AS STRING), '') = ''
        AND COALESCE(oo.qualifier_concept_id, -1) = -1
        AND COALESCE(oo.unit_concept_id, -1) =  -1
        AND COALESCE(oo.qualifier_source_value, '') = ''
        AND COALESCE(oo.unit_source_value, '') =  ''
    WHERE oo.observation_id IS NULL;
         
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added observation', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Second -> observation', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Third] Transfer   
    */
    SET max_current_id = COALESCE((SELECT observation_id FROM `CY_IMOSPHERE_CDM_531.observation` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.observation`
    (
        observation_id,
        person_id,
        observation_concept_id,
        observation_source_value,
        observation_date,
        observation_datetime,
        observation_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as observation_id,
        o.person_id,
        o.observation_concept_id,
        o.observation_source_value,
        o.observation_date,
        o.observation_datetime,
        o.observation_type_concept_id,
        o.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            o.person_id person_id,
            oc.concept_id observation_concept_id,
            o.Accident_And_Emergency_Investigation_Third observation_source_value,
            PARSE_DATE('%Y%m%d', o.Arrival_Date) observation_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(o.Arrival_Date ,'000000')) observation_datetime,
            32817 observation_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` o
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on TRIM(oc.source_value) = TRIM(o.Accident_And_Emergency_Investigation_Third) AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_AE_010415_to_300619' AND oc.source_column = 'Accident_And_Emergency_Investigation_Third'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(o.AandE_Attendance_Number as STRING),'_', o.person_id) 
        WHERE (o.Arrival_Date IS NOT NULL AND o.Arrival_Date NOT LIKE '% %')
        AND o.person_id IS NOT NULL
    ) o
    LEFT JOIN `CY_IMOSPHERE_CDM_531.observation` oo ON 
        oo.person_id = o.person_id
        AND oo.observation_concept_id = o.observation_concept_id
        AND oo.observation_source_value = o.observation_source_value
        AND COALESCE(oo.observation_source_concept_id, -1) = -1
        AND oo.observation_datetime = o.observation_datetime
        AND oo.observation_type_concept_id = o.observation_type_concept_id
        AND COALESCE(oo.provider_id, -1) = -1
        AND COALESCE(oo.visit_occurrence_id, -1) = COALESCE(o.visit_occurrence_id, -1)
        AND COALESCE(oo.visit_detail_id, -1) = -1
        AND COALESCE(oo.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(oo.value_as_number AS STRING), '') = ''
        AND COALESCE(oo.qualifier_concept_id, -1) = -1
        AND COALESCE(oo.unit_concept_id, -1) =  -1
        AND COALESCE(oo.qualifier_source_value, '') = ''
        AND COALESCE(oo.unit_source_value, '') =  ''
    WHERE oo.observation_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added observation', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Third -> observation', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Fourth] Transfer   
    */
    SET max_current_id = COALESCE((SELECT observation_id FROM `CY_IMOSPHERE_CDM_531.observation` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.observation`
    (
        observation_id,
        person_id,
        observation_concept_id,
        observation_source_value,
        observation_date,
        observation_datetime,
        observation_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as observation_id,
        o.person_id,
        o.observation_concept_id,
        o.observation_source_value,
        o.observation_date,
        o.observation_datetime,
        o.observation_type_concept_id,
        o.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            o.person_id person_id,
            oc.concept_id observation_concept_id,
            o.Accident_And_Emergency_Investigation_Fourth observation_source_value,
            PARSE_DATE('%Y%m%d', o.Arrival_Date) observation_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(o.Arrival_Date ,'000000')) observation_datetime,
            32817 observation_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` o
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on TRIM(oc.source_value) = TRIM(o.Accident_And_Emergency_Investigation_Fourth) AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_AE_010415_to_300619' AND oc.source_column = 'Accident_And_Emergency_Investigation_Fourth'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(o.AandE_Attendance_Number as STRING),'_', o.person_id) 
        WHERE (o.Arrival_Date IS NOT NULL AND o.Arrival_Date NOT LIKE '% %')
        AND o.person_id IS NOT NULL
    ) o
    LEFT JOIN `CY_IMOSPHERE_CDM_531.observation` oo ON 
        oo.person_id = o.person_id
        AND oo.observation_concept_id = o.observation_concept_id
        AND oo.observation_source_value = o.observation_source_value
        AND COALESCE(oo.observation_source_concept_id, -1) = -1
        AND oo.observation_datetime = o.observation_datetime
        AND oo.observation_type_concept_id = o.observation_type_concept_id
        AND COALESCE(oo.provider_id, -1) = -1
        AND COALESCE(oo.visit_occurrence_id, -1) = COALESCE(o.visit_occurrence_id, -1)
        AND COALESCE(oo.visit_detail_id, -1) = -1
        AND COALESCE(oo.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(oo.value_as_number AS STRING), '') = ''
        AND COALESCE(oo.qualifier_concept_id, -1) = -1
        AND COALESCE(oo.unit_concept_id, -1) =  -1
        AND COALESCE(oo.qualifier_source_value, '') = ''
        AND COALESCE(oo.unit_source_value, '') =  ''
    WHERE oo.observation_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added observation', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Fourth -> observation', @@row_count, CURRENT_DATETIME();
        
    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Fifth] Transfer   
    */
    SET max_current_id = COALESCE((SELECT observation_id FROM `CY_IMOSPHERE_CDM_531.observation` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.observation`
    (
        observation_id,
        person_id,
        observation_concept_id,
        observation_source_value,
        observation_date,
        observation_datetime,
        observation_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as observation_id,
        o.person_id,
        o.observation_concept_id,
        o.observation_source_value,
        o.observation_date,
        o.observation_datetime,
        o.observation_type_concept_id,
        o.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            o.person_id person_id,
            oc.concept_id observation_concept_id,
            o.Accident_And_Emergency_Investigation_Fifth observation_source_value,
            PARSE_DATE('%Y%m%d', o.Arrival_Date) observation_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(o.Arrival_Date ,'000000')) observation_datetime,
            32817 observation_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` o
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on TRIM(oc.source_value) = TRIM(o.Accident_And_Emergency_Investigation_Fifth) AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_AE_010415_to_300619' AND oc.source_column = 'Accident_And_Emergency_Investigation_Fifth'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(o.AandE_Attendance_Number as STRING),'_', o.person_id) 
        WHERE (o.Arrival_Date IS NOT NULL AND o.Arrival_Date NOT LIKE '% %')
        AND o.person_id IS NOT NULL
    ) o
    LEFT JOIN `CY_IMOSPHERE_CDM_531.observation` oo ON 
        oo.person_id = o.person_id
        AND oo.observation_concept_id = o.observation_concept_id
        AND oo.observation_source_value = o.observation_source_value
        AND COALESCE(oo.observation_source_concept_id, -1) = -1
        AND oo.observation_datetime = o.observation_datetime
        AND oo.observation_type_concept_id = o.observation_type_concept_id
        AND COALESCE(oo.provider_id, -1) = -1
        AND COALESCE(oo.visit_occurrence_id, -1) = COALESCE(o.visit_occurrence_id, -1)
        AND COALESCE(oo.visit_detail_id, -1) = -1
        AND COALESCE(oo.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(oo.value_as_number AS STRING), '') = ''
        AND COALESCE(oo.qualifier_concept_id, -1) = -1
        AND COALESCE(oo.unit_concept_id, -1) =  -1
        AND COALESCE(oo.qualifier_source_value, '') = ''
        AND COALESCE(oo.unit_source_value, '') =  ''
    WHERE oo.observation_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added observation', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Fifth -> observation', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Sixth] Transfer   
    */
    SET max_current_id = COALESCE((SELECT observation_id FROM `CY_IMOSPHERE_CDM_531.observation` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.observation`
    (
        observation_id,
        person_id,
        observation_concept_id,
        observation_source_value,
        observation_date,
        observation_datetime,
        observation_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as observation_id,
        o.person_id,
        o.observation_concept_id,
        o.observation_source_value,
        o.observation_date,
        o.observation_datetime,
        o.observation_type_concept_id,
        o.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            o.person_id person_id,
            oc.concept_id observation_concept_id,
            o.Accident_And_Emergency_Investigation_Sixth observation_source_value,
            PARSE_DATE('%Y%m%d', o.Arrival_Date) observation_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(o.Arrival_Date ,'000000')) observation_datetime,
            32817 observation_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` o
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on TRIM(oc.source_value) = TRIM(o.Accident_And_Emergency_Investigation_Sixth) AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_AE_010415_to_300619' AND oc.source_column = 'Accident_And_Emergency_Investigation_Sixth'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(o.AandE_Attendance_Number as STRING),'_', o.person_id) 
        WHERE (o.Arrival_Date IS NOT NULL AND o.Arrival_Date NOT LIKE '% %')
        AND o.person_id IS NOT NULL
    ) o
    LEFT JOIN `CY_IMOSPHERE_CDM_531.observation` oo ON 
        oo.person_id = o.person_id
        AND oo.observation_concept_id = o.observation_concept_id
        AND oo.observation_source_value = o.observation_source_value
        AND COALESCE(oo.observation_source_concept_id, -1) = -1
        AND oo.observation_datetime = o.observation_datetime
        AND oo.observation_type_concept_id = o.observation_type_concept_id
        AND COALESCE(oo.provider_id, -1) = -1
        AND COALESCE(oo.visit_occurrence_id, -1) = COALESCE(o.visit_occurrence_id, -1)
        AND COALESCE(oo.visit_detail_id, -1) = -1
        AND COALESCE(oo.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(oo.value_as_number AS STRING), '') = ''
        AND COALESCE(oo.qualifier_concept_id, -1) = -1
        AND COALESCE(oo.unit_concept_id, -1) =  -1
        AND COALESCE(oo.qualifier_source_value, '') = ''
        AND COALESCE(oo.unit_source_value, '') =  ''
    WHERE oo.observation_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added observation', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Sixth -> observation', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Seventh] Transfer   
    */
    SET max_current_id = COALESCE((SELECT observation_id FROM `CY_IMOSPHERE_CDM_531.observation` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.observation`
    (
        observation_id,
        person_id,
        observation_concept_id,
        observation_source_value,
        observation_date,
        observation_datetime,
        observation_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as observation_id,
        o.person_id,
        o.observation_concept_id,
        o.observation_source_value,
        o.observation_date,
        o.observation_datetime,
        o.observation_type_concept_id,
        o.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            o.person_id person_id,
            oc.concept_id observation_concept_id,
            o.Accident_And_Emergency_Investigation_Seventh observation_source_value,
            PARSE_DATE('%Y%m%d', o.Arrival_Date) observation_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(o.Arrival_Date ,'000000')) observation_datetime,
            32817 observation_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` o
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on TRIM(oc.source_value) = TRIM(o.Accident_And_Emergency_Investigation_Seventh) AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_AE_010415_to_300619' AND oc.source_column = 'Accident_And_Emergency_Investigation_Seventh'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(o.AandE_Attendance_Number as STRING),'_', o.person_id) 
        WHERE (o.Arrival_Date IS NOT NULL AND o.Arrival_Date NOT LIKE '% %')
        AND o.person_id IS NOT NULL
    ) o
    LEFT JOIN `CY_IMOSPHERE_CDM_531.observation` oo ON 
        oo.person_id = o.person_id
        AND oo.observation_concept_id = o.observation_concept_id
        AND oo.observation_source_value = o.observation_source_value
        AND COALESCE(oo.observation_source_concept_id, -1) = -1
        AND oo.observation_datetime = o.observation_datetime
        AND oo.observation_type_concept_id = o.observation_type_concept_id
        AND COALESCE(oo.provider_id, -1) = -1
        AND COALESCE(oo.visit_occurrence_id, -1) = COALESCE(o.visit_occurrence_id, -1)
        AND COALESCE(oo.visit_detail_id, -1) = -1
        AND COALESCE(oo.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(oo.value_as_number AS STRING), '') = ''
        AND COALESCE(oo.qualifier_concept_id, -1) = -1
        AND COALESCE(oo.unit_concept_id, -1) =  -1
        AND COALESCE(oo.qualifier_source_value, '') = ''
        AND COALESCE(oo.unit_source_value, '') =  ''
    WHERE oo.observation_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added observation', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Seventh -> observation', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Eighth] Transfer   
    */
    SET max_current_id = COALESCE((SELECT observation_id FROM `CY_IMOSPHERE_CDM_531.observation` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.observation`
    (
        observation_id,
        person_id,
        observation_concept_id,
        observation_source_value,
        observation_date,
        observation_datetime,
        observation_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as observation_id,
        o.person_id,
        o.observation_concept_id,
        o.observation_source_value,
        o.observation_date,
        o.observation_datetime,
        o.observation_type_concept_id,
        o.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            o.person_id person_id,
            oc.concept_id observation_concept_id,
            o.Accident_And_Emergency_Investigation_Eighth observation_source_value,
            PARSE_DATE('%Y%m%d', o.Arrival_Date) observation_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(o.Arrival_Date ,'000000')) observation_datetime,
            32817 observation_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` o
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on TRIM(oc.source_value) = TRIM(o.Accident_And_Emergency_Investigation_Eighth) AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_AE_010415_to_300619' AND oc.source_column = 'Accident_And_Emergency_Investigation_Eighth'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(o.AandE_Attendance_Number as STRING),'_', o.person_id) 
        WHERE (o.Arrival_Date IS NOT NULL AND o.Arrival_Date NOT LIKE '% %')
        AND o.person_id IS NOT NULL
    ) o
    LEFT JOIN `CY_IMOSPHERE_CDM_531.observation` oo ON 
        oo.person_id = o.person_id
        AND oo.observation_concept_id = o.observation_concept_id
        AND oo.observation_source_value = o.observation_source_value
        AND COALESCE(oo.observation_source_concept_id, -1) = -1
        AND oo.observation_datetime = o.observation_datetime
        AND oo.observation_type_concept_id = o.observation_type_concept_id
        AND COALESCE(oo.provider_id, -1) = -1
        AND COALESCE(oo.visit_occurrence_id, -1) = COALESCE(o.visit_occurrence_id, -1)
        AND COALESCE(oo.visit_detail_id, -1) = -1
        AND COALESCE(oo.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(oo.value_as_number AS STRING), '') = ''
        AND COALESCE(oo.qualifier_concept_id, -1) = -1
        AND COALESCE(oo.unit_concept_id, -1) =  -1
        AND COALESCE(oo.qualifier_source_value, '') = ''
        AND COALESCE(oo.unit_source_value, '') =  ''
    WHERE oo.observation_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added observation', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Eighth -> observation', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Ninth] Transfer   
    */
    SET max_current_id = COALESCE((SELECT observation_id FROM `CY_IMOSPHERE_CDM_531.observation` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.observation`
    (
        observation_id,
        person_id,
        observation_concept_id,
        observation_source_value,
        observation_date,
        observation_datetime,
        observation_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as observation_id,
        o.person_id,
        o.observation_concept_id,
        o.observation_source_value,
        o.observation_date,
        o.observation_datetime,
        o.observation_type_concept_id,
        o.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            o.person_id person_id,
            oc.concept_id observation_concept_id,
            o.Accident_And_Emergency_Investigation_Ninth observation_source_value,
            PARSE_DATE('%Y%m%d', o.Arrival_Date) observation_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(o.Arrival_Date ,'000000')) observation_datetime,
            32817 observation_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` o
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on TRIM(oc.source_value) = TRIM(o.Accident_And_Emergency_Investigation_Ninth) AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_AE_010415_to_300619' AND oc.source_column = 'Accident_And_Emergency_Investigation_Ninth'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(o.AandE_Attendance_Number as STRING),'_', o.person_id) 
        WHERE (o.Arrival_Date IS NOT NULL AND o.Arrival_Date NOT LIKE '% %')
        AND o.person_id IS NOT NULL
    ) o
    LEFT JOIN `CY_IMOSPHERE_CDM_531.observation` oo ON 
        oo.person_id = o.person_id
        AND oo.observation_concept_id = o.observation_concept_id
        AND oo.observation_source_value = o.observation_source_value
        AND COALESCE(oo.observation_source_concept_id, -1) = -1
        AND oo.observation_datetime = o.observation_datetime
        AND oo.observation_type_concept_id = o.observation_type_concept_id
        AND COALESCE(oo.provider_id, -1) = -1
        AND COALESCE(oo.visit_occurrence_id, -1) = COALESCE(o.visit_occurrence_id, -1)
        AND COALESCE(oo.visit_detail_id, -1) = -1
        AND COALESCE(oo.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(oo.value_as_number AS STRING), '') = ''
        AND COALESCE(oo.qualifier_concept_id, -1) = -1
        AND COALESCE(oo.unit_concept_id, -1) =  -1
        AND COALESCE(oo.qualifier_source_value, '') = ''
        AND COALESCE(oo.unit_source_value, '') =  ''
    WHERE oo.observation_id IS NULL;

     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added observation', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Ninth -> observation', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Tenth] Transfer   
    */
    SET max_current_id = COALESCE((SELECT observation_id FROM `CY_IMOSPHERE_CDM_531.observation` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.observation`
    (
        observation_id,
        person_id,
        observation_concept_id,
        observation_source_value,
        observation_date,
        observation_datetime,
        observation_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as observation_id,
        o.person_id,
        o.observation_concept_id,
        o.observation_source_value,
        o.observation_date,
        o.observation_datetime,
        o.observation_type_concept_id,
        o.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            o.person_id person_id,
            oc.concept_id observation_concept_id,
            o.Accident_And_Emergency_Investigation_Tenth observation_source_value,
            PARSE_DATE('%Y%m%d', o.Arrival_Date) observation_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(o.Arrival_Date ,'000000')) observation_datetime,
            32817 observation_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` o
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on TRIM(oc.source_value) = TRIM(o.Accident_And_Emergency_Investigation_Tenth) AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_AE_010415_to_300619' AND oc.source_column = 'Accident_And_Emergency_Investigation_Tenth'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(o.AandE_Attendance_Number as STRING),'_', o.person_id) 
        WHERE (o.Arrival_Date IS NOT NULL AND o.Arrival_Date NOT LIKE '% %')
        AND o.person_id IS NOT NULL
    ) o
    LEFT JOIN `CY_IMOSPHERE_CDM_531.observation` oo ON 
        oo.person_id = o.person_id
        AND oo.observation_concept_id = o.observation_concept_id
        AND oo.observation_source_value = o.observation_source_value
        AND COALESCE(oo.observation_source_concept_id, -1) = -1
        AND oo.observation_datetime = o.observation_datetime
        AND oo.observation_type_concept_id = o.observation_type_concept_id
        AND COALESCE(oo.provider_id, -1) = -1
        AND COALESCE(oo.visit_occurrence_id, -1) = COALESCE(o.visit_occurrence_id, -1)
        AND COALESCE(oo.visit_detail_id, -1) = -1
        AND COALESCE(oo.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(oo.value_as_number AS STRING), '') = ''
        AND COALESCE(oo.qualifier_concept_id, -1) = -1
        AND COALESCE(oo.unit_concept_id, -1) =  -1
        AND COALESCE(oo.qualifier_source_value, '') = ''
        AND COALESCE(oo.unit_source_value, '') =  ''
    WHERE oo.observation_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added observation', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Tenth -> observation', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Eleventh] Transfer   
    */
    SET max_current_id = COALESCE((SELECT observation_id FROM `CY_IMOSPHERE_CDM_531.observation` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.observation`
    (
        observation_id,
        person_id,
        observation_concept_id,
        observation_source_value,
        observation_date,
        observation_datetime,
        observation_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as observation_id,
        o.person_id,
        o.observation_concept_id,
        o.observation_source_value,
        o.observation_date,
        o.observation_datetime,
        o.observation_type_concept_id,
        o.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            o.person_id person_id,
            oc.concept_id observation_concept_id,
            o.Accident_And_Emergency_Investigation_Eleventh observation_source_value,
            PARSE_DATE('%Y%m%d', o.Arrival_Date) observation_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(o.Arrival_Date ,'000000')) observation_datetime,
            32817 observation_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` o
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on TRIM(oc.source_value) = TRIM(o.Accident_And_Emergency_Investigation_Eleventh) AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_AE_010415_to_300619' AND oc.source_column = 'Accident_And_Emergency_Investigation_Eleventh'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(o.AandE_Attendance_Number as STRING),'_', o.person_id) 
        WHERE (o.Arrival_Date IS NOT NULL AND o.Arrival_Date NOT LIKE '% %')
        AND o.person_id IS NOT NULL
    ) o
    LEFT JOIN `CY_IMOSPHERE_CDM_531.observation` oo ON 
        oo.person_id = o.person_id
        AND oo.observation_concept_id = o.observation_concept_id
        AND oo.observation_source_value = o.observation_source_value
        AND COALESCE(oo.observation_source_concept_id, -1) = -1
        AND oo.observation_datetime = o.observation_datetime
        AND oo.observation_type_concept_id = o.observation_type_concept_id
        AND COALESCE(oo.provider_id, -1) = -1
        AND COALESCE(oo.visit_occurrence_id, -1) = COALESCE(o.visit_occurrence_id, -1)
        AND COALESCE(oo.visit_detail_id, -1) = -1
        AND COALESCE(oo.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(oo.value_as_number AS STRING), '') = ''
        AND COALESCE(oo.qualifier_concept_id, -1) = -1
        AND COALESCE(oo.unit_concept_id, -1) =  -1
        AND COALESCE(oo.qualifier_source_value, '') = ''
        AND COALESCE(oo.unit_source_value, '') =  ''
    WHERE oo.observation_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added observation', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Eleventh -> observation', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Twelfth] Transfer   
    */
    SET max_current_id = COALESCE((SELECT observation_id FROM `CY_IMOSPHERE_CDM_531.observation` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.observation`
    (
        observation_id,
        person_id,
        observation_concept_id,
        observation_source_value,
        observation_date,
        observation_datetime,
        observation_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as observation_id,
        o.person_id,
        o.observation_concept_id,
        o.observation_source_value,
        o.observation_date,
        o.observation_datetime,
        o.observation_type_concept_id,
        o.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            o.person_id person_id,
            oc.concept_id observation_concept_id,
            o.Accident_And_Emergency_Investigation_Twelfth observation_source_value,
            PARSE_DATE('%Y%m%d', o.Arrival_Date) observation_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(o.Arrival_Date ,'000000')) observation_datetime,
            32817 observation_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` o
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on TRIM(oc.source_value) = TRIM(o.Accident_And_Emergency_Investigation_Twelfth) AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_AE_010415_to_300619' AND oc.source_column = 'Accident_And_Emergency_Investigation_Twelfth'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(o.AandE_Attendance_Number as STRING),'_', o.person_id) 
        WHERE (o.Arrival_Date IS NOT NULL AND o.Arrival_Date NOT LIKE '% %')
        AND o.person_id IS NOT NULL
    ) o
    LEFT JOIN `CY_IMOSPHERE_CDM_531.observation` oo ON 
        oo.person_id = o.person_id
        AND oo.observation_concept_id = o.observation_concept_id
        AND oo.observation_source_value = o.observation_source_value
        AND COALESCE(oo.observation_source_concept_id, -1) = -1
        AND oo.observation_datetime = o.observation_datetime
        AND oo.observation_type_concept_id = o.observation_type_concept_id
        AND COALESCE(oo.provider_id, -1) = -1
        AND COALESCE(oo.visit_occurrence_id, -1) = COALESCE(o.visit_occurrence_id, -1)
        AND COALESCE(oo.visit_detail_id, -1) = -1
        AND COALESCE(oo.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(oo.value_as_number AS STRING), '') = ''
        AND COALESCE(oo.qualifier_concept_id, -1) = -1
        AND COALESCE(oo.unit_concept_id, -1) =  -1
        AND COALESCE(oo.qualifier_source_value, '') = ''
        AND COALESCE(oo.unit_source_value, '') =  ''
    WHERE oo.observation_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added observation', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Twelfth -> observation', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_First] Transfer   
    */
    SET max_current_id = COALESCE((SELECT observation_id FROM `CY_IMOSPHERE_CDM_531.observation` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.observation`
    (
        observation_id,
        person_id,
        observation_concept_id,
        observation_source_value,
        observation_date,
        observation_datetime,
        observation_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as observation_id,
        o.person_id,
        o.observation_concept_id,
        o.observation_source_value,
        o.observation_date,
        o.observation_datetime,
        o.observation_type_concept_id,
        o.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            o.person_id person_id,
            oc.concept_id observation_concept_id,
            o.Accident_And_Emergency_Treatment_First observation_source_value,
            PARSE_DATE('%Y%m%d', o.Arrival_Date) observation_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(o.Arrival_Date ,'000000')) observation_datetime,
            32817 observation_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` o
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on TRIM(oc.source_value) = TRIM(o.Accident_And_Emergency_Treatment_First) AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_AE_010415_to_300619' AND oc.source_column = 'Accident_And_Emergency_Treatment_First'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(o.AandE_Attendance_Number as STRING),'_', o.person_id) 
        WHERE (o.Arrival_Date IS NOT NULL AND o.Arrival_Date NOT LIKE '% %')
        AND o.person_id IS NOT NULL
    ) o
    LEFT JOIN `CY_IMOSPHERE_CDM_531.observation` oo ON 
        oo.person_id = o.person_id
        AND oo.observation_concept_id = o.observation_concept_id
        AND oo.observation_source_value = o.observation_source_value
        AND COALESCE(oo.observation_source_concept_id, -1) = -1
        AND oo.observation_datetime = o.observation_datetime
        AND oo.observation_type_concept_id = o.observation_type_concept_id
        AND COALESCE(oo.provider_id, -1) = -1
        AND COALESCE(oo.visit_occurrence_id, -1) = COALESCE(o.visit_occurrence_id, -1)
        AND COALESCE(oo.visit_detail_id, -1) = -1
        AND COALESCE(oo.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(oo.value_as_number AS STRING), '') = ''
        AND COALESCE(oo.qualifier_concept_id, -1) = -1
        AND COALESCE(oo.unit_concept_id, -1) =  -1
        AND COALESCE(oo.qualifier_source_value, '') = ''
        AND COALESCE(oo.unit_source_value, '') =  ''
    WHERE oo.observation_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added observation', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_First -> observation', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Second] Transfer   
    */
    SET max_current_id = COALESCE((SELECT observation_id FROM `CY_IMOSPHERE_CDM_531.observation` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.observation`
    (
        observation_id,
        person_id,
        observation_concept_id,
        observation_source_value,
        observation_date,
        observation_datetime,
        observation_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as observation_id,
        o.person_id,
        o.observation_concept_id,
        o.observation_source_value,
        o.observation_date,
        o.observation_datetime,
        o.observation_type_concept_id,
        o.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            o.person_id person_id,
            oc.concept_id observation_concept_id,
            o.Accident_And_Emergency_Treatment_Second observation_source_value,
            PARSE_DATE('%Y%m%d', o.Arrival_Date) observation_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(o.Arrival_Date ,'000000')) observation_datetime,
            32817 observation_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` o
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on TRIM(oc.source_value) = TRIM(o.Accident_And_Emergency_Treatment_Second) AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_AE_010415_to_300619' AND oc.source_column = 'Accident_And_Emergency_Treatment_Second'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(o.AandE_Attendance_Number as STRING),'_', o.person_id) 
        WHERE (o.Arrival_Date IS NOT NULL AND o.Arrival_Date NOT LIKE '% %')
        AND o.person_id IS NOT NULL
    ) o
    LEFT JOIN `CY_IMOSPHERE_CDM_531.observation` oo ON 
        oo.person_id = o.person_id
        AND oo.observation_concept_id = o.observation_concept_id
        AND oo.observation_source_value = o.observation_source_value
        AND COALESCE(oo.observation_source_concept_id, -1) = -1
        AND oo.observation_datetime = o.observation_datetime
        AND oo.observation_type_concept_id = o.observation_type_concept_id
        AND COALESCE(oo.provider_id, -1) = -1
        AND COALESCE(oo.visit_occurrence_id, -1) = COALESCE(o.visit_occurrence_id, -1)
        AND COALESCE(oo.visit_detail_id, -1) = -1
        AND COALESCE(oo.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(oo.value_as_number AS STRING), '') = ''
        AND COALESCE(oo.qualifier_concept_id, -1) = -1
        AND COALESCE(oo.unit_concept_id, -1) =  -1
        AND COALESCE(oo.qualifier_source_value, '') = ''
        AND COALESCE(oo.unit_source_value, '') =  ''
    WHERE oo.observation_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added observation', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Second -> observation', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Third] Transfer   
    */
    SET max_current_id = COALESCE((SELECT observation_id FROM `CY_IMOSPHERE_CDM_531.observation` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.observation`
    (
        observation_id,
        person_id,
        observation_concept_id,
        observation_source_value,
        observation_date,
        observation_datetime,
        observation_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as observation_id,
        o.person_id,
        o.observation_concept_id,
        o.observation_source_value,
        o.observation_date,
        o.observation_datetime,
        o.observation_type_concept_id,
        o.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            o.person_id person_id,
            oc.concept_id observation_concept_id,
            o.Accident_And_Emergency_Treatment_Third observation_source_value,
            PARSE_DATE('%Y%m%d', o.Arrival_Date) observation_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(o.Arrival_Date ,'000000')) observation_datetime,
            32817 observation_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` o
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on TRIM(oc.source_value) = TRIM(o.Accident_And_Emergency_Treatment_Third) AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_AE_010415_to_300619' AND oc.source_column = 'Accident_And_Emergency_Treatment_Third'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(o.AandE_Attendance_Number as STRING),'_', o.person_id) 
        WHERE (o.Arrival_Date IS NOT NULL AND o.Arrival_Date NOT LIKE '% %')
        AND o.person_id IS NOT NULL
    ) o
    LEFT JOIN `CY_IMOSPHERE_CDM_531.observation` oo ON 
        oo.person_id = o.person_id
        AND oo.observation_concept_id = o.observation_concept_id
        AND oo.observation_source_value = o.observation_source_value
        AND COALESCE(oo.observation_source_concept_id, -1) = -1
        AND oo.observation_datetime = o.observation_datetime
        AND oo.observation_type_concept_id = o.observation_type_concept_id
        AND COALESCE(oo.provider_id, -1) = -1
        AND COALESCE(oo.visit_occurrence_id, -1) = COALESCE(o.visit_occurrence_id, -1)
        AND COALESCE(oo.visit_detail_id, -1) = -1
        AND COALESCE(oo.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(oo.value_as_number AS STRING), '') = ''
        AND COALESCE(oo.qualifier_concept_id, -1) = -1
        AND COALESCE(oo.unit_concept_id, -1) =  -1
        AND COALESCE(oo.qualifier_source_value, '') = ''
        AND COALESCE(oo.unit_source_value, '') =  ''
    WHERE oo.observation_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added observation', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Third -> observation', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Fourth] Transfer   
    */
    SET max_current_id = COALESCE((SELECT observation_id FROM `CY_IMOSPHERE_CDM_531.observation` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.observation`
    (
        observation_id,
        person_id,
        observation_concept_id,
        observation_source_value,
        observation_date,
        observation_datetime,
        observation_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as observation_id,
        o.person_id,
        o.observation_concept_id,
        o.observation_source_value,
        o.observation_date,
        o.observation_datetime,
        o.observation_type_concept_id,
        o.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            o.person_id person_id,
            oc.concept_id observation_concept_id,
            o.Accident_And_Emergency_Treatment_Fourth observation_source_value,
            PARSE_DATE('%Y%m%d', o.Arrival_Date) observation_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(o.Arrival_Date ,'000000')) observation_datetime,
            32817 observation_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` o
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on TRIM(oc.source_value) = TRIM(o.Accident_And_Emergency_Treatment_Fourth) AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_AE_010415_to_300619' AND oc.source_column = 'Accident_And_Emergency_Treatment_Fourth'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(o.AandE_Attendance_Number as STRING),'_', o.person_id) 
        WHERE (o.Arrival_Date IS NOT NULL AND o.Arrival_Date NOT LIKE '% %')
        AND o.person_id IS NOT NULL
    ) o
    LEFT JOIN `CY_IMOSPHERE_CDM_531.observation` oo ON 
        oo.person_id = o.person_id
        AND oo.observation_concept_id = o.observation_concept_id
        AND oo.observation_source_value = o.observation_source_value
        AND COALESCE(oo.observation_source_concept_id, -1) = -1
        AND oo.observation_datetime = o.observation_datetime
        AND oo.observation_type_concept_id = o.observation_type_concept_id
        AND COALESCE(oo.provider_id, -1) = -1
        AND COALESCE(oo.visit_occurrence_id, -1) = COALESCE(o.visit_occurrence_id, -1)
        AND COALESCE(oo.visit_detail_id, -1) = -1
        AND COALESCE(oo.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(oo.value_as_number AS STRING), '') = ''
        AND COALESCE(oo.qualifier_concept_id, -1) = -1
        AND COALESCE(oo.unit_concept_id, -1) =  -1
        AND COALESCE(oo.qualifier_source_value, '') = ''
        AND COALESCE(oo.unit_source_value, '') =  ''
    WHERE oo.observation_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added observation', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Fourth -> observation', @@row_count, CURRENT_DATETIME();
        
    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Fifth] Transfer   
    */
    SET max_current_id = COALESCE((SELECT observation_id FROM `CY_IMOSPHERE_CDM_531.observation` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.observation`
    (
        observation_id,
        person_id,
        observation_concept_id,
        observation_source_value,
        observation_date,
        observation_datetime,
        observation_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as observation_id,
        o.person_id,
        o.observation_concept_id,
        o.observation_source_value,
        o.observation_date,
        o.observation_datetime,
        o.observation_type_concept_id,
        o.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            o.person_id person_id,
            oc.concept_id observation_concept_id,
            o.Accident_And_Emergency_Treatment_Fifth observation_source_value,
            PARSE_DATE('%Y%m%d', o.Arrival_Date) observation_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(o.Arrival_Date ,'000000')) observation_datetime,
            32817 observation_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` o
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on TRIM(oc.source_value) = TRIM(o.Accident_And_Emergency_Treatment_Fifth) AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_AE_010415_to_300619' AND oc.source_column = 'Accident_And_Emergency_Treatment_Fifth'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(o.AandE_Attendance_Number as STRING),'_', o.person_id) 
        WHERE (o.Arrival_Date IS NOT NULL AND o.Arrival_Date NOT LIKE '% %')
        AND o.person_id IS NOT NULL
    ) o
    LEFT JOIN `CY_IMOSPHERE_CDM_531.observation` oo ON 
        oo.person_id = o.person_id
        AND oo.observation_concept_id = o.observation_concept_id
        AND oo.observation_source_value = o.observation_source_value
        AND COALESCE(oo.observation_source_concept_id, -1) = -1
        AND oo.observation_datetime = o.observation_datetime
        AND oo.observation_type_concept_id = o.observation_type_concept_id
        AND COALESCE(oo.provider_id, -1) = -1
        AND COALESCE(oo.visit_occurrence_id, -1) = COALESCE(o.visit_occurrence_id, -1)
        AND COALESCE(oo.visit_detail_id, -1) = -1
        AND COALESCE(oo.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(oo.value_as_number AS STRING), '') = ''
        AND COALESCE(oo.qualifier_concept_id, -1) = -1
        AND COALESCE(oo.unit_concept_id, -1) =  -1
        AND COALESCE(oo.qualifier_source_value, '') = ''
        AND COALESCE(oo.unit_source_value, '') =  ''
    WHERE oo.observation_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added observation', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Fifth -> observation', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Sixth] Transfer   
    */
    SET max_current_id = COALESCE((SELECT observation_id FROM `CY_IMOSPHERE_CDM_531.observation` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.observation`
    (
        observation_id,
        person_id,
        observation_concept_id,
        observation_source_value,
        observation_date,
        observation_datetime,
        observation_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as observation_id,
        o.person_id,
        o.observation_concept_id,
        o.observation_source_value,
        o.observation_date,
        o.observation_datetime,
        o.observation_type_concept_id,
        o.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            o.person_id person_id,
            oc.concept_id observation_concept_id,
            o.Accident_And_Emergency_Treatment_Sixth observation_source_value,
            PARSE_DATE('%Y%m%d', o.Arrival_Date) observation_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(o.Arrival_Date ,'000000')) observation_datetime,
            32817 observation_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` o
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on TRIM(oc.source_value) = TRIM(o.Accident_And_Emergency_Treatment_Sixth) AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_AE_010415_to_300619' AND oc.source_column = 'Accident_And_Emergency_Treatment_Sixth'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(o.AandE_Attendance_Number as STRING),'_', o.person_id) 
        WHERE (o.Arrival_Date IS NOT NULL AND o.Arrival_Date NOT LIKE '% %')
        AND o.person_id IS NOT NULL
    ) o
    LEFT JOIN `CY_IMOSPHERE_CDM_531.observation` oo ON 
        oo.person_id = o.person_id
        AND oo.observation_concept_id = o.observation_concept_id
        AND oo.observation_source_value = o.observation_source_value
        AND COALESCE(oo.observation_source_concept_id, -1) = -1
        AND oo.observation_datetime = o.observation_datetime
        AND oo.observation_type_concept_id = o.observation_type_concept_id
        AND COALESCE(oo.provider_id, -1) = -1
        AND COALESCE(oo.visit_occurrence_id, -1) = COALESCE(o.visit_occurrence_id, -1)
        AND COALESCE(oo.visit_detail_id, -1) = -1
        AND COALESCE(oo.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(oo.value_as_number AS STRING), '') = ''
        AND COALESCE(oo.qualifier_concept_id, -1) = -1
        AND COALESCE(oo.unit_concept_id, -1) =  -1
        AND COALESCE(oo.qualifier_source_value, '') = ''
        AND COALESCE(oo.unit_source_value, '') =  ''
    WHERE oo.observation_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added observation', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Sixth -> observation', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Seventh] Transfer   
    */
    SET max_current_id = COALESCE((SELECT observation_id FROM `CY_IMOSPHERE_CDM_531.observation` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.observation`
    (
        observation_id,
        person_id,
        observation_concept_id,
        observation_source_value,
        observation_date,
        observation_datetime,
        observation_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as observation_id,
        o.person_id,
        o.observation_concept_id,
        o.observation_source_value,
        o.observation_date,
        o.observation_datetime,
        o.observation_type_concept_id,
        o.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            o.person_id person_id,
            oc.concept_id observation_concept_id,
            o.Accident_And_Emergency_Treatment_Seventh observation_source_value,
            PARSE_DATE('%Y%m%d', o.Arrival_Date) observation_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(o.Arrival_Date ,'000000')) observation_datetime,
            32817 observation_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` o
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on TRIM(oc.source_value) = TRIM(o.Accident_And_Emergency_Treatment_Seventh) AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_AE_010415_to_300619' AND oc.source_column = 'Accident_And_Emergency_Treatment_Seventh'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(o.AandE_Attendance_Number as STRING),'_', o.person_id) 
        WHERE (o.Arrival_Date IS NOT NULL AND o.Arrival_Date NOT LIKE '% %')
        AND o.person_id IS NOT NULL
    ) o
    LEFT JOIN `CY_IMOSPHERE_CDM_531.observation` oo ON 
        oo.person_id = o.person_id
        AND oo.observation_concept_id = o.observation_concept_id
        AND oo.observation_source_value = o.observation_source_value
        AND COALESCE(oo.observation_source_concept_id, -1) = -1
        AND oo.observation_datetime = o.observation_datetime
        AND oo.observation_type_concept_id = o.observation_type_concept_id
        AND COALESCE(oo.provider_id, -1) = -1
        AND COALESCE(oo.visit_occurrence_id, -1) = COALESCE(o.visit_occurrence_id, -1)
        AND COALESCE(oo.visit_detail_id, -1) = -1
        AND COALESCE(oo.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(oo.value_as_number AS STRING), '') = ''
        AND COALESCE(oo.qualifier_concept_id, -1) = -1
        AND COALESCE(oo.unit_concept_id, -1) =  -1
        AND COALESCE(oo.qualifier_source_value, '') = ''
        AND COALESCE(oo.unit_source_value, '') =  ''
    WHERE oo.observation_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added observation', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Seventh -> observation', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Eighth] Transfer   
    */
    SET max_current_id = COALESCE((SELECT observation_id FROM `CY_IMOSPHERE_CDM_531.observation` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.observation`
    (
        observation_id,
        person_id,
        observation_concept_id,
        observation_source_value,
        observation_date,
        observation_datetime,
        observation_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as observation_id,
        o.person_id,
        o.observation_concept_id,
        o.observation_source_value,
        o.observation_date,
        o.observation_datetime,
        o.observation_type_concept_id,
        o.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            o.person_id person_id,
            oc.concept_id observation_concept_id,
            o.Accident_And_Emergency_Treatment_Eighth observation_source_value,
            PARSE_DATE('%Y%m%d', o.Arrival_Date) observation_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(o.Arrival_Date ,'000000')) observation_datetime,
            32817 observation_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` o
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on TRIM(oc.source_value) = TRIM(o.Accident_And_Emergency_Treatment_Eighth) AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_AE_010415_to_300619' AND oc.source_column = 'Accident_And_Emergency_Treatment_Eighth'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(o.AandE_Attendance_Number as STRING),'_', o.person_id) 
        WHERE (o.Arrival_Date IS NOT NULL AND o.Arrival_Date NOT LIKE '% %')
        AND o.person_id IS NOT NULL
    ) o
    LEFT JOIN `CY_IMOSPHERE_CDM_531.observation` oo ON 
        oo.person_id = o.person_id
        AND oo.observation_concept_id = o.observation_concept_id
        AND oo.observation_source_value = o.observation_source_value
        AND COALESCE(oo.observation_source_concept_id, -1) = -1
        AND oo.observation_datetime = o.observation_datetime
        AND oo.observation_type_concept_id = o.observation_type_concept_id
        AND COALESCE(oo.provider_id, -1) = -1
        AND COALESCE(oo.visit_occurrence_id, -1) = COALESCE(o.visit_occurrence_id, -1)
        AND COALESCE(oo.visit_detail_id, -1) = -1
        AND COALESCE(oo.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(oo.value_as_number AS STRING), '') = ''
        AND COALESCE(oo.qualifier_concept_id, -1) = -1
        AND COALESCE(oo.unit_concept_id, -1) =  -1
        AND COALESCE(oo.qualifier_source_value, '') = ''
        AND COALESCE(oo.unit_source_value, '') =  ''
    WHERE oo.observation_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added observation', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Eighth -> observation', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Ninth] Transfer   
    */
    SET max_current_id = COALESCE((SELECT observation_id FROM `CY_IMOSPHERE_CDM_531.observation` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.observation`
    (
        observation_id,
        person_id,
        observation_concept_id,
        observation_source_value,
        observation_date,
        observation_datetime,
        observation_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as observation_id,
        o.person_id,
        o.observation_concept_id,
        o.observation_source_value,
        o.observation_date,
        o.observation_datetime,
        o.observation_type_concept_id,
        o.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            o.person_id person_id,
            oc.concept_id observation_concept_id,
            o.Accident_And_Emergency_Treatment_Ninth observation_source_value,
            PARSE_DATE('%Y%m%d', o.Arrival_Date) observation_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(o.Arrival_Date ,'000000')) observation_datetime,
            32817 observation_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` o
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on TRIM(oc.source_value) = TRIM(o.Accident_And_Emergency_Treatment_Ninth) AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_AE_010415_to_300619' AND oc.source_column = 'Accident_And_Emergency_Treatment_Ninth'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(o.AandE_Attendance_Number as STRING),'_', o.person_id) 
        WHERE (o.Arrival_Date IS NOT NULL AND o.Arrival_Date NOT LIKE '% %')
        AND o.person_id IS NOT NULL
    ) o
    LEFT JOIN `CY_IMOSPHERE_CDM_531.observation` oo ON 
        oo.person_id = o.person_id
        AND oo.observation_concept_id = o.observation_concept_id
        AND oo.observation_source_value = o.observation_source_value
        AND COALESCE(oo.observation_source_concept_id, -1) = -1
        AND oo.observation_datetime = o.observation_datetime
        AND oo.observation_type_concept_id = o.observation_type_concept_id
        AND COALESCE(oo.provider_id, -1) = -1
        AND COALESCE(oo.visit_occurrence_id, -1) = COALESCE(o.visit_occurrence_id, -1)
        AND COALESCE(oo.visit_detail_id, -1) = -1
        AND COALESCE(oo.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(oo.value_as_number AS STRING), '') = ''
        AND COALESCE(oo.qualifier_concept_id, -1) = -1
        AND COALESCE(oo.unit_concept_id, -1) =  -1
        AND COALESCE(oo.qualifier_source_value, '') = ''
        AND COALESCE(oo.unit_source_value, '') =  ''
    WHERE oo.observation_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added observation', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Ninth -> observation', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Tenth] Transfer   
    */
    SET max_current_id = COALESCE((SELECT observation_id FROM `CY_IMOSPHERE_CDM_531.observation` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.observation`
    (
        observation_id,
        person_id,
        observation_concept_id,
        observation_source_value,
        observation_date,
        observation_datetime,
        observation_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as observation_id,
        o.person_id,
        o.observation_concept_id,
        o.observation_source_value,
        o.observation_date,
        o.observation_datetime,
        o.observation_type_concept_id,
        o.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            o.person_id person_id,
            oc.concept_id observation_concept_id,
            o.Accident_And_Emergency_Treatment_Tenth observation_source_value,
            PARSE_DATE('%Y%m%d', o.Arrival_Date) observation_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(o.Arrival_Date ,'000000')) observation_datetime,
            32817 observation_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` o
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on TRIM(oc.source_value) = TRIM(o.Accident_And_Emergency_Treatment_Tenth) AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_AE_010415_to_300619' AND oc.source_column = 'Accident_And_Emergency_Treatment_Tenth'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(o.AandE_Attendance_Number as STRING),'_', o.person_id) 
        WHERE (o.Arrival_Date IS NOT NULL AND o.Arrival_Date NOT LIKE '% %')
        AND o.person_id IS NOT NULL
    ) o
    LEFT JOIN `CY_IMOSPHERE_CDM_531.observation` oo ON 
        oo.person_id = o.person_id
        AND oo.observation_concept_id = o.observation_concept_id
        AND oo.observation_source_value = o.observation_source_value
        AND COALESCE(oo.observation_source_concept_id, -1) = -1
        AND oo.observation_datetime = o.observation_datetime
        AND oo.observation_type_concept_id = o.observation_type_concept_id
        AND COALESCE(oo.provider_id, -1) = -1
        AND COALESCE(oo.visit_occurrence_id, -1) = COALESCE(o.visit_occurrence_id, -1)
        AND COALESCE(oo.visit_detail_id, -1) = -1
        AND COALESCE(oo.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(oo.value_as_number AS STRING), '') = ''
        AND COALESCE(oo.qualifier_concept_id, -1) = -1
        AND COALESCE(oo.unit_concept_id, -1) =  -1
        AND COALESCE(oo.qualifier_source_value, '') = ''
        AND COALESCE(oo.unit_source_value, '') =  ''
    WHERE oo.observation_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added observation', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Tenth -> observation', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Eleventh] Transfer   
    */
    SET max_current_id = COALESCE((SELECT observation_id FROM `CY_IMOSPHERE_CDM_531.observation` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.observation`
    (
        observation_id,
        person_id,
        observation_concept_id,
        observation_source_value,
        observation_date,
        observation_datetime,
        observation_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as observation_id,
        o.person_id,
        o.observation_concept_id,
        o.observation_source_value,
        o.observation_date,
        o.observation_datetime,
        o.observation_type_concept_id,
        o.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            o.person_id person_id,
            oc.concept_id observation_concept_id,
            o.Accident_And_Emergency_Treatment_Eleventh observation_source_value,
            PARSE_DATE('%Y%m%d', o.Arrival_Date) observation_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(o.Arrival_Date ,'000000')) observation_datetime,
            32817 observation_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` o
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on TRIM(oc.source_value) = TRIM(o.Accident_And_Emergency_Treatment_Eleventh) AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_AE_010415_to_300619' AND oc.source_column = 'Accident_And_Emergency_Treatment_Eleventh'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(o.AandE_Attendance_Number as STRING),'_', o.person_id) 
        WHERE (o.Arrival_Date IS NOT NULL AND o.Arrival_Date NOT LIKE '% %')
        AND o.person_id IS NOT NULL
    ) o
    LEFT JOIN `CY_IMOSPHERE_CDM_531.observation` oo ON 
        oo.person_id = o.person_id
        AND oo.observation_concept_id = o.observation_concept_id
        AND oo.observation_source_value = o.observation_source_value
        AND COALESCE(oo.observation_source_concept_id, -1) = -1
        AND oo.observation_datetime = o.observation_datetime
        AND oo.observation_type_concept_id = o.observation_type_concept_id
        AND COALESCE(oo.provider_id, -1) = -1
        AND COALESCE(oo.visit_occurrence_id, -1) = COALESCE(o.visit_occurrence_id, -1)
        AND COALESCE(oo.visit_detail_id, -1) = -1
        AND COALESCE(oo.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(oo.value_as_number AS STRING), '') = ''
        AND COALESCE(oo.qualifier_concept_id, -1) = -1
        AND COALESCE(oo.unit_concept_id, -1) =  -1
        AND COALESCE(oo.qualifier_source_value, '') = ''
        AND COALESCE(oo.unit_source_value, '') =  ''
    WHERE oo.observation_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added observation', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Eleventh -> observation', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Twelfth] Transfer   
    */
    SET max_current_id = COALESCE((SELECT observation_id FROM `CY_IMOSPHERE_CDM_531.observation` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.observation`
    (
        observation_id,
        person_id,
        observation_concept_id,
        observation_source_value,
        observation_date,
        observation_datetime,
        observation_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as observation_id,
        o.person_id,
        o.observation_concept_id,
        o.observation_source_value,
        o.observation_date,
        o.observation_datetime,
        o.observation_type_concept_id,
        o.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            o.person_id person_id,
            oc.concept_id observation_concept_id,
            o.Accident_And_Emergency_Treatment_Twelfth observation_source_value,
            PARSE_DATE('%Y%m%d', o.Arrival_Date) observation_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(o.Arrival_Date ,'000000')) observation_datetime,
            32817 observation_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` o
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on TRIM(oc.source_value) = TRIM(o.Accident_And_Emergency_Treatment_Twelfth) AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_AE_010415_to_300619' AND oc.source_column = 'Accident_And_Emergency_Treatment_Twelfth'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(o.AandE_Attendance_Number as STRING),'_', o.person_id) 
        WHERE (o.Arrival_Date IS NOT NULL AND o.Arrival_Date NOT LIKE '% %')
        AND o.person_id IS NOT NULL
    ) o
    LEFT JOIN `CY_IMOSPHERE_CDM_531.observation` oo ON 
        oo.person_id = o.person_id
        AND oo.observation_concept_id = o.observation_concept_id
        AND oo.observation_source_value = o.observation_source_value
        AND COALESCE(oo.observation_source_concept_id, -1) = -1
        AND oo.observation_datetime = o.observation_datetime
        AND oo.observation_type_concept_id = o.observation_type_concept_id
        AND COALESCE(oo.provider_id, -1) = -1
        AND COALESCE(oo.visit_occurrence_id, -1) = COALESCE(o.visit_occurrence_id, -1)
        AND COALESCE(oo.visit_detail_id, -1) = -1
        AND COALESCE(oo.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(oo.value_as_number AS STRING), '') = ''
        AND COALESCE(oo.qualifier_concept_id, -1) = -1
        AND COALESCE(oo.unit_concept_id, -1) =  -1
        AND COALESCE(oo.qualifier_source_value, '') = ''
        AND COALESCE(oo.unit_source_value, '') =  ''
    WHERE oo.observation_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added observation', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Twelfth -> observation', @@row_count, CURRENT_DATETIME();
END;

CREATE OR REPLACE PROCEDURE CY_IMOSPHERE_WORKSPACE.TRANSFER_MEASUREMENT(run_id INT64)
OPTIONS (strict_mode=false)
BEGIN
    DECLARE max_current_id INT64;

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    ------------[SUS_BRI_AE_010415_to_300619]
    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_First] Transfer   
    */
    SET max_current_id = COALESCE((SELECT measurement_id FROM `CY_IMOSPHERE_CDM_531.measurement` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.measurement`
    (
        measurement_id,
        person_id,
        measurement_concept_id,
        measurement_source_value,
        measurement_date,
        measurement_datetime,
        measurement_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as measurement_id,
        m.person_id,
        m.measurement_concept_id,
        m.measurement_source_value,
        m.measurement_date,
        m.measurement_datetime,
        m.measurement_type_concept_id,
        m.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            m.person_id person_id,
            mc.concept_id measurement_concept_id,
            m.Accident_And_Emergency_Investigation_First measurement_source_value,
            PARSE_DATE('%Y%m%d', m.Arrival_Date) measurement_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(m.Arrival_Date ,'000000')) measurement_datetime,
            32817 measurement_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` m
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` mc on TRIM(mc.source_value) = TRIM(m.Accident_And_Emergency_Investigation_First) AND mc.destination_table = 'MEASUREMENT' AND mc.source_table = 'SUS_BRI_AE_010415_to_300619' AND mc.source_column = 'Accident_And_Emergency_Investigation_First'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(m.AandE_Attendance_Number as STRING),'_', m.person_id) 
        WHERE (m.Arrival_Date IS NOT NULL AND m.Arrival_Date NOT LIKE '% %')
        AND m.person_id IS NOT NULL
    ) m
    LEFT JOIN `CY_IMOSPHERE_CDM_531.measurement` om ON 
        om.person_id = m.person_id
        AND om.measurement_concept_id = m.measurement_concept_id
        AND om.measurement_source_value = m.measurement_source_value
        AND COALESCE(om.measurement_source_concept_id, -1) = -1
        AND om.measurement_datetime = m.measurement_datetime
        AND om.measurement_type_concept_id = m.measurement_type_concept_id
        AND COALESCE(om.provider_id, -1) = -1
        AND COALESCE(om.visit_occurrence_id, -1) = COALESCE(m.visit_occurrence_id, -1)
        AND COALESCE(om.visit_detail_id, -1) = -1
        AND COALESCE(om.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(om.value_as_number AS STRING), '') = ''
        AND COALESCE(om.unit_concept_id, -1) =  -1
        AND COALESCE(om.unit_source_value, '') =  ''
        AND COALESCE(value_source_value, '') = ''
    WHERE om.measurement_id IS NULL;
         
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added measurement', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_First -> measurement', @@row_count, CURRENT_DATETIME();

    
    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Second] Transfer   
    */
    SET max_current_id = COALESCE((SELECT measurement_id FROM `CY_IMOSPHERE_CDM_531.measurement` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.measurement`
    (
        measurement_id,
        person_id,
        measurement_concept_id,
        measurement_source_value,
        measurement_date,
        measurement_datetime,
        measurement_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as measurement_id,
        m.person_id,
        m.measurement_concept_id,
        m.measurement_source_value,
        m.measurement_date,
        m.measurement_datetime,
        m.measurement_type_concept_id,
        m.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            m.person_id person_id,
            mc.concept_id measurement_concept_id,
            m.Accident_And_Emergency_Investigation_Second measurement_source_value,
            PARSE_DATE('%Y%m%d', m.Arrival_Date) measurement_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(m.Arrival_Date ,'000000')) measurement_datetime,
            32817 measurement_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` m
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` mc on TRIM(mc.source_value) = TRIM(m.Accident_And_Emergency_Investigation_Second) AND mc.destination_table = 'MEASUREMENT' AND mc.source_table = 'SUS_BRI_AE_010415_to_300619' AND mc.source_column = 'Accident_And_Emergency_Investigation_Second'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(m.AandE_Attendance_Number as STRING),'_', m.person_id) 
        WHERE (m.Arrival_Date IS NOT NULL AND m.Arrival_Date NOT LIKE '% %')
        AND m.person_id IS NOT NULL
    ) m
    LEFT JOIN `CY_IMOSPHERE_CDM_531.measurement` om ON 
        om.person_id = m.person_id
        AND om.measurement_concept_id = m.measurement_concept_id
        AND om.measurement_source_value = m.measurement_source_value
        AND COALESCE(om.measurement_source_concept_id, -1) = -1
        AND om.measurement_datetime = m.measurement_datetime
        AND om.measurement_type_concept_id = m.measurement_type_concept_id
        AND COALESCE(om.provider_id, -1) = -1
        AND COALESCE(om.visit_occurrence_id, -1) = COALESCE(m.visit_occurrence_id, -1)
        AND COALESCE(om.visit_detail_id, -1) = -1
        AND COALESCE(om.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(om.value_as_number AS STRING), '') = ''
        AND COALESCE(om.unit_concept_id, -1) =  -1
        AND COALESCE(om.unit_source_value, '') =  ''
        AND COALESCE(value_source_value, '') = ''
    WHERE om.measurement_id IS NULL;
         
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added measurement', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Second -> measurement', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Third] Transfer   
    */
    SET max_current_id = COALESCE((SELECT measurement_id FROM `CY_IMOSPHERE_CDM_531.measurement` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.measurement`
    (
        measurement_id,
        person_id,
        measurement_concept_id,
        measurement_source_value,
        measurement_date,
        measurement_datetime,
        measurement_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as measurement_id,
        m.person_id,
        m.measurement_concept_id,
        m.measurement_source_value,
        m.measurement_date,
        m.measurement_datetime,
        m.measurement_type_concept_id,
        m.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            m.person_id person_id,
            mc.concept_id measurement_concept_id,
            m.Accident_And_Emergency_Investigation_Third measurement_source_value,
            PARSE_DATE('%Y%m%d', m.Arrival_Date) measurement_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(m.Arrival_Date ,'000000')) measurement_datetime,
            32817 measurement_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` m
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` mc on TRIM(mc.source_value) = TRIM(m.Accident_And_Emergency_Investigation_Third) AND mc.destination_table = 'MEASUREMENT' AND mc.source_table = 'SUS_BRI_AE_010415_to_300619' AND mc.source_column = 'Accident_And_Emergency_Investigation_Third'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(m.AandE_Attendance_Number as STRING),'_', m.person_id) 
        WHERE (m.Arrival_Date IS NOT NULL AND m.Arrival_Date NOT LIKE '% %')
        AND m.person_id IS NOT NULL
    ) m
    LEFT JOIN `CY_IMOSPHERE_CDM_531.measurement` om ON 
        om.person_id = m.person_id
        AND om.measurement_concept_id = m.measurement_concept_id
        AND om.measurement_source_value = m.measurement_source_value
        AND COALESCE(om.measurement_source_concept_id, -1) = -1
        AND om.measurement_datetime = m.measurement_datetime
        AND om.measurement_type_concept_id = m.measurement_type_concept_id
        AND COALESCE(om.provider_id, -1) = -1
        AND COALESCE(om.visit_occurrence_id, -1) = COALESCE(m.visit_occurrence_id, -1)
        AND COALESCE(om.visit_detail_id, -1) = -1
        AND COALESCE(om.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(om.value_as_number AS STRING), '') = ''
        AND COALESCE(om.unit_concept_id, -1) =  -1
        AND COALESCE(om.unit_source_value, '') =  ''
        AND COALESCE(value_source_value, '') = ''
    WHERE om.measurement_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added measurement', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Third -> measurement', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Fourth] Transfer   
    */
    SET max_current_id = COALESCE((SELECT measurement_id FROM `CY_IMOSPHERE_CDM_531.measurement` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.measurement`
    (
        measurement_id,
        person_id,
        measurement_concept_id,
        measurement_source_value,
        measurement_date,
        measurement_datetime,
        measurement_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as measurement_id,
        m.person_id,
        m.measurement_concept_id,
        m.measurement_source_value,
        m.measurement_date,
        m.measurement_datetime,
        m.measurement_type_concept_id,
        m.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            m.person_id person_id,
            mc.concept_id measurement_concept_id,
            m.Accident_And_Emergency_Investigation_Fourth measurement_source_value,
            PARSE_DATE('%Y%m%d', m.Arrival_Date) measurement_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(m.Arrival_Date ,'000000')) measurement_datetime,
            32817 measurement_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` m
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` mc on TRIM(mc.source_value) = TRIM(m.Accident_And_Emergency_Investigation_Fourth) AND mc.destination_table = 'MEASUREMENT' AND mc.source_table = 'SUS_BRI_AE_010415_to_300619' AND mc.source_column = 'Accident_And_Emergency_Investigation_Fourth'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(m.AandE_Attendance_Number as STRING),'_', m.person_id) 
        WHERE (m.Arrival_Date IS NOT NULL AND m.Arrival_Date NOT LIKE '% %')
        AND m.person_id IS NOT NULL
    ) m
    LEFT JOIN `CY_IMOSPHERE_CDM_531.measurement` om ON 
        om.person_id = m.person_id
        AND om.measurement_concept_id = m.measurement_concept_id
        AND om.measurement_source_value = m.measurement_source_value
        AND COALESCE(om.measurement_source_concept_id, -1) = -1
        AND om.measurement_datetime = m.measurement_datetime
        AND om.measurement_type_concept_id = m.measurement_type_concept_id
        AND COALESCE(om.provider_id, -1) = -1
        AND COALESCE(om.visit_occurrence_id, -1) = COALESCE(m.visit_occurrence_id, -1)
        AND COALESCE(om.visit_detail_id, -1) = -1
        AND COALESCE(om.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(om.value_as_number AS STRING), '') = ''
        AND COALESCE(om.unit_concept_id, -1) =  -1
        AND COALESCE(om.unit_source_value, '') =  ''
        AND COALESCE(value_source_value, '') = ''
    WHERE om.measurement_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added measurement', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Fourth -> measurement', @@row_count, CURRENT_DATETIME();
        
    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Fifth] Transfer   
    */
    SET max_current_id = COALESCE((SELECT measurement_id FROM `CY_IMOSPHERE_CDM_531.measurement` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.measurement`
    (
        measurement_id,
        person_id,
        measurement_concept_id,
        measurement_source_value,
        measurement_date,
        measurement_datetime,
        measurement_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as measurement_id,
        m.person_id,
        m.measurement_concept_id,
        m.measurement_source_value,
        m.measurement_date,
        m.measurement_datetime,
        m.measurement_type_concept_id,
        m.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            m.person_id person_id,
            mc.concept_id measurement_concept_id,
            m.Accident_And_Emergency_Investigation_Fifth measurement_source_value,
            PARSE_DATE('%Y%m%d', m.Arrival_Date) measurement_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(m.Arrival_Date ,'000000')) measurement_datetime,
            32817 measurement_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` m
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` mc on TRIM(mc.source_value) = TRIM(m.Accident_And_Emergency_Investigation_Fifth) AND mc.destination_table = 'MEASUREMENT' AND mc.source_table = 'SUS_BRI_AE_010415_to_300619' AND mc.source_column = 'Accident_And_Emergency_Investigation_Fifth'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(m.AandE_Attendance_Number as STRING),'_', m.person_id) 
        WHERE (m.Arrival_Date IS NOT NULL AND m.Arrival_Date NOT LIKE '% %')
        AND m.person_id IS NOT NULL
    ) m
    LEFT JOIN `CY_IMOSPHERE_CDM_531.measurement` om ON 
        om.person_id = m.person_id
        AND om.measurement_concept_id = m.measurement_concept_id
        AND om.measurement_source_value = m.measurement_source_value
        AND COALESCE(om.measurement_source_concept_id, -1) = -1
        AND om.measurement_datetime = m.measurement_datetime
        AND om.measurement_type_concept_id = m.measurement_type_concept_id
        AND COALESCE(om.provider_id, -1) = -1
        AND COALESCE(om.visit_occurrence_id, -1) = COALESCE(m.visit_occurrence_id, -1)
        AND COALESCE(om.visit_detail_id, -1) = -1
        AND COALESCE(om.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(om.value_as_number AS STRING), '') = ''
        AND COALESCE(om.unit_concept_id, -1) =  -1
        AND COALESCE(om.unit_source_value, '') =  ''
        AND COALESCE(value_source_value, '') = ''
    WHERE om.measurement_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added measurement', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Fifth -> measurement', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Sixth] Transfer   
    */
    SET max_current_id = COALESCE((SELECT measurement_id FROM `CY_IMOSPHERE_CDM_531.measurement` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.measurement`
    (
        measurement_id,
        person_id,
        measurement_concept_id,
        measurement_source_value,
        measurement_date,
        measurement_datetime,
        measurement_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as measurement_id,
        m.person_id,
        m.measurement_concept_id,
        m.measurement_source_value,
        m.measurement_date,
        m.measurement_datetime,
        m.measurement_type_concept_id,
        m.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            m.person_id person_id,
            mc.concept_id measurement_concept_id,
            m.Accident_And_Emergency_Investigation_Sixth measurement_source_value,
            PARSE_DATE('%Y%m%d', m.Arrival_Date) measurement_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(m.Arrival_Date ,'000000')) measurement_datetime,
            32817 measurement_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` m
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` mc on TRIM(mc.source_value) = TRIM(m.Accident_And_Emergency_Investigation_Sixth) AND mc.destination_table = 'MEASUREMENT' AND mc.source_table = 'SUS_BRI_AE_010415_to_300619' AND mc.source_column = 'Accident_And_Emergency_Investigation_Sixth'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(m.AandE_Attendance_Number as STRING),'_', m.person_id) 
        WHERE (m.Arrival_Date IS NOT NULL AND m.Arrival_Date NOT LIKE '% %')
        AND m.person_id IS NOT NULL
    ) m
    LEFT JOIN `CY_IMOSPHERE_CDM_531.measurement` om ON 
        om.person_id = m.person_id
        AND om.measurement_concept_id = m.measurement_concept_id
        AND om.measurement_source_value = m.measurement_source_value
        AND COALESCE(om.measurement_source_concept_id, -1) = -1
        AND om.measurement_datetime = m.measurement_datetime
        AND om.measurement_type_concept_id = m.measurement_type_concept_id
        AND COALESCE(om.provider_id, -1) = -1
        AND COALESCE(om.visit_occurrence_id, -1) = COALESCE(m.visit_occurrence_id, -1)
        AND COALESCE(om.visit_detail_id, -1) = -1
        AND COALESCE(om.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(om.value_as_number AS STRING), '') = ''
        AND COALESCE(om.unit_concept_id, -1) =  -1
        AND COALESCE(om.unit_source_value, '') =  ''
        AND COALESCE(value_source_value, '') = ''
    WHERE om.measurement_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added measurement', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Sixth -> measurement', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Seventh] Transfer   
    */
    SET max_current_id = COALESCE((SELECT measurement_id FROM `CY_IMOSPHERE_CDM_531.measurement` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.measurement`
    (
        measurement_id,
        person_id,
        measurement_concept_id,
        measurement_source_value,
        measurement_date,
        measurement_datetime,
        measurement_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as measurement_id,
        m.person_id,
        m.measurement_concept_id,
        m.measurement_source_value,
        m.measurement_date,
        m.measurement_datetime,
        m.measurement_type_concept_id,
        m.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            m.person_id person_id,
            mc.concept_id measurement_concept_id,
            m.Accident_And_Emergency_Investigation_Seventh measurement_source_value,
            PARSE_DATE('%Y%m%d', m.Arrival_Date) measurement_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(m.Arrival_Date ,'000000')) measurement_datetime,
            32817 measurement_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` m
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` mc on TRIM(mc.source_value) = TRIM(m.Accident_And_Emergency_Investigation_Seventh) AND mc.destination_table = 'MEASUREMENT' AND mc.source_table = 'SUS_BRI_AE_010415_to_300619' AND mc.source_column = 'Accident_And_Emergency_Investigation_Seventh'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(m.AandE_Attendance_Number as STRING),'_', m.person_id) 
        WHERE (m.Arrival_Date IS NOT NULL AND m.Arrival_Date NOT LIKE '% %')
        AND m.person_id IS NOT NULL
    ) m
    LEFT JOIN `CY_IMOSPHERE_CDM_531.measurement` om ON 
        om.person_id = m.person_id
        AND om.measurement_concept_id = m.measurement_concept_id
        AND om.measurement_source_value = m.measurement_source_value
        AND COALESCE(om.measurement_source_concept_id, -1) = -1
        AND om.measurement_datetime = m.measurement_datetime
        AND om.measurement_type_concept_id = m.measurement_type_concept_id
        AND COALESCE(om.provider_id, -1) = -1
        AND COALESCE(om.visit_occurrence_id, -1) = COALESCE(m.visit_occurrence_id, -1)
        AND COALESCE(om.visit_detail_id, -1) = -1
        AND COALESCE(om.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(om.value_as_number AS STRING), '') = ''
        AND COALESCE(om.unit_concept_id, -1) =  -1
        AND COALESCE(om.unit_source_value, '') =  ''
        AND COALESCE(value_source_value, '') = ''
    WHERE om.measurement_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added measurement', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Seventh -> measurement', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Eighth] Transfer   
    */
    SET max_current_id = COALESCE((SELECT measurement_id FROM `CY_IMOSPHERE_CDM_531.measurement` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.measurement`
    (
        measurement_id,
        person_id,
        measurement_concept_id,
        measurement_source_value,
        measurement_date,
        measurement_datetime,
        measurement_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as measurement_id,
        m.person_id,
        m.measurement_concept_id,
        m.measurement_source_value,
        m.measurement_date,
        m.measurement_datetime,
        m.measurement_type_concept_id,
        m.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            m.person_id person_id,
            mc.concept_id measurement_concept_id,
            m.Accident_And_Emergency_Investigation_Eighth measurement_source_value,
            PARSE_DATE('%Y%m%d', m.Arrival_Date) measurement_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(m.Arrival_Date ,'000000')) measurement_datetime,
            32817 measurement_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` m
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` mc on TRIM(mc.source_value) = TRIM(m.Accident_And_Emergency_Investigation_Eighth) AND mc.destination_table = 'MEASUREMENT' AND mc.source_table = 'SUS_BRI_AE_010415_to_300619' AND mc.source_column = 'Accident_And_Emergency_Investigation_Eighth'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(m.AandE_Attendance_Number as STRING),'_', m.person_id) 
        WHERE (m.Arrival_Date IS NOT NULL AND m.Arrival_Date NOT LIKE '% %')
        AND m.person_id IS NOT NULL
    ) m
    LEFT JOIN `CY_IMOSPHERE_CDM_531.measurement` om ON 
        om.person_id = m.person_id
        AND om.measurement_concept_id = m.measurement_concept_id
        AND om.measurement_source_value = m.measurement_source_value
        AND COALESCE(om.measurement_source_concept_id, -1) = -1
        AND om.measurement_datetime = m.measurement_datetime
        AND om.measurement_type_concept_id = m.measurement_type_concept_id
        AND COALESCE(om.provider_id, -1) = -1
        AND COALESCE(om.visit_occurrence_id, -1) = COALESCE(m.visit_occurrence_id, -1)
        AND COALESCE(om.visit_detail_id, -1) = -1
        AND COALESCE(om.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(om.value_as_number AS STRING), '') = ''
        AND COALESCE(om.unit_concept_id, -1) =  -1
        AND COALESCE(om.unit_source_value, '') =  ''
        AND COALESCE(value_source_value, '') = ''
    WHERE om.measurement_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added measurement', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Eighth -> measurement', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Ninth] Transfer   
    */
    SET max_current_id = COALESCE((SELECT measurement_id FROM `CY_IMOSPHERE_CDM_531.measurement` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.measurement`
    (
        measurement_id,
        person_id,
        measurement_concept_id,
        measurement_source_value,
        measurement_date,
        measurement_datetime,
        measurement_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as measurement_id,
        m.person_id,
        m.measurement_concept_id,
        m.measurement_source_value,
        m.measurement_date,
        m.measurement_datetime,
        m.measurement_type_concept_id,
        m.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            m.person_id person_id,
            mc.concept_id measurement_concept_id,
            m.Accident_And_Emergency_Investigation_Ninth measurement_source_value,
            PARSE_DATE('%Y%m%d', m.Arrival_Date) measurement_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(m.Arrival_Date ,'000000')) measurement_datetime,
            32817 measurement_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` m
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` mc on TRIM(mc.source_value) = TRIM(m.Accident_And_Emergency_Investigation_Ninth) AND mc.destination_table = 'MEASUREMENT' AND mc.source_table = 'SUS_BRI_AE_010415_to_300619' AND mc.source_column = 'Accident_And_Emergency_Investigation_Ninth'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(m.AandE_Attendance_Number as STRING),'_', m.person_id) 
        WHERE (m.Arrival_Date IS NOT NULL AND m.Arrival_Date NOT LIKE '% %')
        AND m.person_id IS NOT NULL
    ) m
    LEFT JOIN `CY_IMOSPHERE_CDM_531.measurement` om ON 
        om.person_id = m.person_id
        AND om.measurement_concept_id = m.measurement_concept_id
        AND om.measurement_source_value = m.measurement_source_value
        AND COALESCE(om.measurement_source_concept_id, -1) = -1
        AND om.measurement_datetime = m.measurement_datetime
        AND om.measurement_type_concept_id = m.measurement_type_concept_id
        AND COALESCE(om.provider_id, -1) = -1
        AND COALESCE(om.visit_occurrence_id, -1) = COALESCE(m.visit_occurrence_id, -1)
        AND COALESCE(om.visit_detail_id, -1) = -1
        AND COALESCE(om.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(om.value_as_number AS STRING), '') = ''
        AND COALESCE(om.unit_concept_id, -1) =  -1
        AND COALESCE(om.unit_source_value, '') =  ''
        AND COALESCE(value_source_value, '') = ''
    WHERE om.measurement_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added measurement', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Ninth -> measurement', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Tenth] Transfer   
    */
    SET max_current_id = COALESCE((SELECT measurement_id FROM `CY_IMOSPHERE_CDM_531.measurement` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.measurement`
    (
        measurement_id,
        person_id,
        measurement_concept_id,
        measurement_source_value,
        measurement_date,
        measurement_datetime,
        measurement_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as measurement_id,
        m.person_id,
        m.measurement_concept_id,
        m.measurement_source_value,
        m.measurement_date,
        m.measurement_datetime,
        m.measurement_type_concept_id,
        m.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            m.person_id person_id,
            mc.concept_id measurement_concept_id,
            m.Accident_And_Emergency_Investigation_Tenth measurement_source_value,
            PARSE_DATE('%Y%m%d', m.Arrival_Date) measurement_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(m.Arrival_Date ,'000000')) measurement_datetime,
            32817 measurement_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` m
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` mc on TRIM(mc.source_value) = TRIM(m.Accident_And_Emergency_Investigation_Tenth) AND mc.destination_table = 'MEASUREMENT' AND mc.source_table = 'SUS_BRI_AE_010415_to_300619' AND mc.source_column = 'Accident_And_Emergency_Investigation_Tenth'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(m.AandE_Attendance_Number as STRING),'_', m.person_id) 
        WHERE (m.Arrival_Date IS NOT NULL AND m.Arrival_Date NOT LIKE '% %')
        AND m.person_id IS NOT NULL
    ) m
    LEFT JOIN `CY_IMOSPHERE_CDM_531.measurement` om ON 
        om.person_id = m.person_id
        AND om.measurement_concept_id = m.measurement_concept_id
        AND om.measurement_source_value = m.measurement_source_value
        AND COALESCE(om.measurement_source_concept_id, -1) = -1
        AND om.measurement_datetime = m.measurement_datetime
        AND om.measurement_type_concept_id = m.measurement_type_concept_id
        AND COALESCE(om.provider_id, -1) = -1
        AND COALESCE(om.visit_occurrence_id, -1) = COALESCE(m.visit_occurrence_id, -1)
        AND COALESCE(om.visit_detail_id, -1) = -1
        AND COALESCE(om.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(om.value_as_number AS STRING), '') = ''
        AND COALESCE(om.unit_concept_id, -1) =  -1
        AND COALESCE(om.unit_source_value, '') =  ''
        AND COALESCE(value_source_value, '') = ''
    WHERE om.measurement_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added measurement', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Tenth -> measurement', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Eleventh] Transfer   
    */
    SET max_current_id = COALESCE((SELECT measurement_id FROM `CY_IMOSPHERE_CDM_531.measurement` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.measurement`
    (
        measurement_id,
        person_id,
        measurement_concept_id,
        measurement_source_value,
        measurement_date,
        measurement_datetime,
        measurement_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as measurement_id,
        m.person_id,
        m.measurement_concept_id,
        m.measurement_source_value,
        m.measurement_date,
        m.measurement_datetime,
        m.measurement_type_concept_id,
        m.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            m.person_id person_id,
            mc.concept_id measurement_concept_id,
            m.Accident_And_Emergency_Investigation_Eleventh measurement_source_value,
            PARSE_DATE('%Y%m%d', m.Arrival_Date) measurement_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(m.Arrival_Date ,'000000')) measurement_datetime,
            32817 measurement_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` m
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` mc on TRIM(mc.source_value) = TRIM(m.Accident_And_Emergency_Investigation_Eleventh) AND mc.destination_table = 'MEASUREMENT' AND mc.source_table = 'SUS_BRI_AE_010415_to_300619' AND mc.source_column = 'Accident_And_Emergency_Investigation_Eleventh'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(m.AandE_Attendance_Number as STRING),'_', m.person_id) 
        WHERE (m.Arrival_Date IS NOT NULL AND m.Arrival_Date NOT LIKE '% %')
        AND m.person_id IS NOT NULL
    ) m
    LEFT JOIN `CY_IMOSPHERE_CDM_531.measurement` om ON 
        om.person_id = m.person_id
        AND om.measurement_concept_id = m.measurement_concept_id
        AND om.measurement_source_value = m.measurement_source_value
        AND COALESCE(om.measurement_source_concept_id, -1) = -1
        AND om.measurement_datetime = m.measurement_datetime
        AND om.measurement_type_concept_id = m.measurement_type_concept_id
        AND COALESCE(om.provider_id, -1) = -1
        AND COALESCE(om.visit_occurrence_id, -1) = COALESCE(m.visit_occurrence_id, -1)
        AND COALESCE(om.visit_detail_id, -1) = -1
        AND COALESCE(om.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(om.value_as_number AS STRING), '') = ''
        AND COALESCE(om.unit_concept_id, -1) =  -1
        AND COALESCE(om.unit_source_value, '') =  ''
        AND COALESCE(value_source_value, '') = ''
    WHERE om.measurement_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added measurement', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Eleventh -> measurement', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Twelfth] Transfer   
    */
    SET max_current_id = COALESCE((SELECT measurement_id FROM `CY_IMOSPHERE_CDM_531.measurement` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.measurement`
    (
        measurement_id,
        person_id,
        measurement_concept_id,
        measurement_source_value,
        measurement_date,
        measurement_datetime,
        measurement_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as measurement_id,
        m.person_id,
        m.measurement_concept_id,
        m.measurement_source_value,
        m.measurement_date,
        m.measurement_datetime,
        m.measurement_type_concept_id,
        m.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            m.person_id person_id,
            mc.concept_id measurement_concept_id,
            m.Accident_And_Emergency_Investigation_Twelfth measurement_source_value,
            PARSE_DATE('%Y%m%d', m.Arrival_Date) measurement_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(m.Arrival_Date ,'000000')) measurement_datetime,
            32817 measurement_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` m
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` mc on TRIM(mc.source_value) = TRIM(m.Accident_And_Emergency_Investigation_Twelfth) AND mc.destination_table = 'MEASUREMENT' AND mc.source_table = 'SUS_BRI_AE_010415_to_300619' AND mc.source_column = 'Accident_And_Emergency_Investigation_Twelfth'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(m.AandE_Attendance_Number as STRING),'_', m.person_id) 
        WHERE (m.Arrival_Date IS NOT NULL AND m.Arrival_Date NOT LIKE '% %')
        AND m.person_id IS NOT NULL
    ) m
    LEFT JOIN `CY_IMOSPHERE_CDM_531.measurement` om ON 
        om.person_id = m.person_id
        AND om.measurement_concept_id = m.measurement_concept_id
        AND om.measurement_source_value = m.measurement_source_value
        AND COALESCE(om.measurement_source_concept_id, -1) = -1
        AND om.measurement_datetime = m.measurement_datetime
        AND om.measurement_type_concept_id = m.measurement_type_concept_id
        AND COALESCE(om.provider_id, -1) = -1
        AND COALESCE(om.visit_occurrence_id, -1) = COALESCE(m.visit_occurrence_id, -1)
        AND COALESCE(om.visit_detail_id, -1) = -1
        AND COALESCE(om.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(om.value_as_number AS STRING), '') = ''
        AND COALESCE(om.unit_concept_id, -1) =  -1
        AND COALESCE(om.unit_source_value, '') =  ''
        AND COALESCE(value_source_value, '') = ''
    WHERE om.measurement_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added measurement', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_Twelfth -> measurement', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_First] Transfer   
    */
    SET max_current_id = COALESCE((SELECT measurement_id FROM `CY_IMOSPHERE_CDM_531.measurement` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.measurement`
    (
        measurement_id,
        person_id,
        measurement_concept_id,
        measurement_source_value,
        measurement_date,
        measurement_datetime,
        measurement_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as measurement_id,
        m.person_id,
        m.measurement_concept_id,
        m.measurement_source_value,
        m.measurement_date,
        m.measurement_datetime,
        m.measurement_type_concept_id,
        m.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            m.person_id person_id,
            mc.concept_id measurement_concept_id,
            m.Accident_And_Emergency_Treatment_First measurement_source_value,
            PARSE_DATE('%Y%m%d', m.Arrival_Date) measurement_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(m.Arrival_Date ,'000000')) measurement_datetime,
            32817 measurement_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` m
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` mc on TRIM(mc.source_value) = TRIM(m.Accident_And_Emergency_Treatment_First) AND mc.destination_table = 'MEASUREMENT' AND mc.source_table = 'SUS_BRI_AE_010415_to_300619' AND mc.source_column = 'Accident_And_Emergency_Treatment_First'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(m.AandE_Attendance_Number as STRING),'_', m.person_id) 
        WHERE (m.Arrival_Date IS NOT NULL AND m.Arrival_Date NOT LIKE '% %')
        AND m.person_id IS NOT NULL
    ) m
    LEFT JOIN `CY_IMOSPHERE_CDM_531.measurement` om ON 
        om.person_id = m.person_id
        AND om.measurement_concept_id = m.measurement_concept_id
        AND om.measurement_source_value = m.measurement_source_value
        AND COALESCE(om.measurement_source_concept_id, -1) = -1
        AND om.measurement_datetime = m.measurement_datetime
        AND om.measurement_type_concept_id = m.measurement_type_concept_id
        AND COALESCE(om.provider_id, -1) = -1
        AND COALESCE(om.visit_occurrence_id, -1) = COALESCE(m.visit_occurrence_id, -1)
        AND COALESCE(om.visit_detail_id, -1) = -1
        AND COALESCE(om.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(om.value_as_number AS STRING), '') = ''
        AND COALESCE(om.unit_concept_id, -1) =  -1
        AND COALESCE(om.unit_source_value, '') =  ''
        AND COALESCE(value_source_value, '') = ''
    WHERE om.measurement_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added measurement', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_First -> measurement', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Second] Transfer   
    */
    SET max_current_id = COALESCE((SELECT measurement_id FROM `CY_IMOSPHERE_CDM_531.measurement` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.measurement`
    (
        measurement_id,
        person_id,
        measurement_concept_id,
        measurement_source_value,
        measurement_date,
        measurement_datetime,
        measurement_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as measurement_id,
        m.person_id,
        m.measurement_concept_id,
        m.measurement_source_value,
        m.measurement_date,
        m.measurement_datetime,
        m.measurement_type_concept_id,
        m.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            m.person_id person_id,
            mc.concept_id measurement_concept_id,
            m.Accident_And_Emergency_Treatment_Second measurement_source_value,
            PARSE_DATE('%Y%m%d', m.Arrival_Date) measurement_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(m.Arrival_Date ,'000000')) measurement_datetime,
            32817 measurement_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` m
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` mc on TRIM(mc.source_value) = TRIM(m.Accident_And_Emergency_Treatment_Second) AND mc.destination_table = 'MEASUREMENT' AND mc.source_table = 'SUS_BRI_AE_010415_to_300619' AND mc.source_column = 'Accident_And_Emergency_Treatment_Second'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(m.AandE_Attendance_Number as STRING),'_', m.person_id) 
        WHERE (m.Arrival_Date IS NOT NULL AND m.Arrival_Date NOT LIKE '% %')
        AND m.person_id IS NOT NULL
    ) m
    LEFT JOIN `CY_IMOSPHERE_CDM_531.measurement` om ON 
        om.person_id = m.person_id
        AND om.measurement_concept_id = m.measurement_concept_id
        AND om.measurement_source_value = m.measurement_source_value
        AND COALESCE(om.measurement_source_concept_id, -1) = -1
        AND om.measurement_datetime = m.measurement_datetime
        AND om.measurement_type_concept_id = m.measurement_type_concept_id
        AND COALESCE(om.provider_id, -1) = -1
        AND COALESCE(om.visit_occurrence_id, -1) = COALESCE(m.visit_occurrence_id, -1)
        AND COALESCE(om.visit_detail_id, -1) = -1
        AND COALESCE(om.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(om.value_as_number AS STRING), '') = ''
        AND COALESCE(om.unit_concept_id, -1) =  -1
        AND COALESCE(om.unit_source_value, '') =  ''
        AND COALESCE(value_source_value, '') = ''
    WHERE om.measurement_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added measurement', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Second -> measurement', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Third] Transfer   
    */
    SET max_current_id = COALESCE((SELECT measurement_id FROM `CY_IMOSPHERE_CDM_531.measurement` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.measurement`
    (
        measurement_id,
        person_id,
        measurement_concept_id,
        measurement_source_value,
        measurement_date,
        measurement_datetime,
        measurement_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as measurement_id,
        m.person_id,
        m.measurement_concept_id,
        m.measurement_source_value,
        m.measurement_date,
        m.measurement_datetime,
        m.measurement_type_concept_id,
        m.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            m.person_id person_id,
            mc.concept_id measurement_concept_id,
            m.Accident_And_Emergency_Treatment_Third measurement_source_value,
            PARSE_DATE('%Y%m%d', m.Arrival_Date) measurement_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(m.Arrival_Date ,'000000')) measurement_datetime,
            32817 measurement_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` m
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` mc on TRIM(mc.source_value) = TRIM(m.Accident_And_Emergency_Treatment_Third) AND mc.destination_table = 'MEASUREMENT' AND mc.source_table = 'SUS_BRI_AE_010415_to_300619' AND mc.source_column = 'Accident_And_Emergency_Treatment_Third'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(m.AandE_Attendance_Number as STRING),'_', m.person_id) 
        WHERE (m.Arrival_Date IS NOT NULL AND m.Arrival_Date NOT LIKE '% %')
        AND m.person_id IS NOT NULL
    ) m
    LEFT JOIN `CY_IMOSPHERE_CDM_531.measurement` om ON 
        om.person_id = m.person_id
        AND om.measurement_concept_id = m.measurement_concept_id
        AND om.measurement_source_value = m.measurement_source_value
        AND COALESCE(om.measurement_source_concept_id, -1) = -1
        AND om.measurement_datetime = m.measurement_datetime
        AND om.measurement_type_concept_id = m.measurement_type_concept_id
        AND COALESCE(om.provider_id, -1) = -1
        AND COALESCE(om.visit_occurrence_id, -1) = COALESCE(m.visit_occurrence_id, -1)
        AND COALESCE(om.visit_detail_id, -1) = -1
        AND COALESCE(om.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(om.value_as_number AS STRING), '') = ''
        AND COALESCE(om.unit_concept_id, -1) =  -1
        AND COALESCE(om.unit_source_value, '') =  ''
        AND COALESCE(value_source_value, '') = ''
    WHERE om.measurement_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added measurement', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Third -> measurement', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Fourth] Transfer   
    */
    SET max_current_id = COALESCE((SELECT measurement_id FROM `CY_IMOSPHERE_CDM_531.measurement` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.measurement`
    (
        measurement_id,
        person_id,
        measurement_concept_id,
        measurement_source_value,
        measurement_date,
        measurement_datetime,
        measurement_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as measurement_id,
        m.person_id,
        m.measurement_concept_id,
        m.measurement_source_value,
        m.measurement_date,
        m.measurement_datetime,
        m.measurement_type_concept_id,
        m.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            m.person_id person_id,
            mc.concept_id measurement_concept_id,
            m.Accident_And_Emergency_Treatment_Fourth measurement_source_value,
            PARSE_DATE('%Y%m%d', m.Arrival_Date) measurement_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(m.Arrival_Date ,'000000')) measurement_datetime,
            32817 measurement_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` m
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` mc on TRIM(mc.source_value) = TRIM(m.Accident_And_Emergency_Treatment_Fourth) AND mc.destination_table = 'MEASUREMENT' AND mc.source_table = 'SUS_BRI_AE_010415_to_300619' AND mc.source_column = 'Accident_And_Emergency_Treatment_Fourth'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(m.AandE_Attendance_Number as STRING),'_', m.person_id) 
        WHERE (m.Arrival_Date IS NOT NULL AND m.Arrival_Date NOT LIKE '% %')
        AND m.person_id IS NOT NULL
    ) m
    LEFT JOIN `CY_IMOSPHERE_CDM_531.measurement` om ON 
        om.person_id = m.person_id
        AND om.measurement_concept_id = m.measurement_concept_id
        AND om.measurement_source_value = m.measurement_source_value
        AND COALESCE(om.measurement_source_concept_id, -1) = -1
        AND om.measurement_datetime = m.measurement_datetime
        AND om.measurement_type_concept_id = m.measurement_type_concept_id
        AND COALESCE(om.provider_id, -1) = -1
        AND COALESCE(om.visit_occurrence_id, -1) = COALESCE(m.visit_occurrence_id, -1)
        AND COALESCE(om.visit_detail_id, -1) = -1
        AND COALESCE(om.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(om.value_as_number AS STRING), '') = ''
        AND COALESCE(om.unit_concept_id, -1) =  -1
        AND COALESCE(om.unit_source_value, '') =  ''
        AND COALESCE(value_source_value, '') = ''
    WHERE om.measurement_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added measurement', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Fourth -> measurement', @@row_count, CURRENT_DATETIME();
        
    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Fifth] Transfer   
    */
    SET max_current_id = COALESCE((SELECT measurement_id FROM `CY_IMOSPHERE_CDM_531.measurement` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.measurement`
    (
        measurement_id,
        person_id,
        measurement_concept_id,
        measurement_source_value,
        measurement_date,
        measurement_datetime,
        measurement_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as measurement_id,
        m.person_id,
        m.measurement_concept_id,
        m.measurement_source_value,
        m.measurement_date,
        m.measurement_datetime,
        m.measurement_type_concept_id,
        m.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            m.person_id person_id,
            mc.concept_id measurement_concept_id,
            m.Accident_And_Emergency_Treatment_Fifth measurement_source_value,
            PARSE_DATE('%Y%m%d', m.Arrival_Date) measurement_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(m.Arrival_Date ,'000000')) measurement_datetime,
            32817 measurement_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` m
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` mc on TRIM(mc.source_value) = TRIM(m.Accident_And_Emergency_Treatment_Fifth) AND mc.destination_table = 'MEASUREMENT' AND mc.source_table = 'SUS_BRI_AE_010415_to_300619' AND mc.source_column = 'Accident_And_Emergency_Treatment_Fifth'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(m.AandE_Attendance_Number as STRING),'_', m.person_id) 
        WHERE (m.Arrival_Date IS NOT NULL AND m.Arrival_Date NOT LIKE '% %')
        AND m.person_id IS NOT NULL
    ) m
    LEFT JOIN `CY_IMOSPHERE_CDM_531.measurement` om ON 
        om.person_id = m.person_id
        AND om.measurement_concept_id = m.measurement_concept_id
        AND om.measurement_source_value = m.measurement_source_value
        AND COALESCE(om.measurement_source_concept_id, -1) = -1
        AND om.measurement_datetime = m.measurement_datetime
        AND om.measurement_type_concept_id = m.measurement_type_concept_id
        AND COALESCE(om.provider_id, -1) = -1
        AND COALESCE(om.visit_occurrence_id, -1) = COALESCE(m.visit_occurrence_id, -1)
        AND COALESCE(om.visit_detail_id, -1) = -1
        AND COALESCE(om.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(om.value_as_number AS STRING), '') = ''
        AND COALESCE(om.unit_concept_id, -1) =  -1
        AND COALESCE(om.unit_source_value, '') =  ''
        AND COALESCE(value_source_value, '') = ''
    WHERE om.measurement_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added measurement', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Fifth -> measurement', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Sixth] Transfer   
    */
    SET max_current_id = COALESCE((SELECT measurement_id FROM `CY_IMOSPHERE_CDM_531.measurement` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.measurement`
    (
        measurement_id,
        person_id,
        measurement_concept_id,
        measurement_source_value,
        measurement_date,
        measurement_datetime,
        measurement_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as measurement_id,
        m.person_id,
        m.measurement_concept_id,
        m.measurement_source_value,
        m.measurement_date,
        m.measurement_datetime,
        m.measurement_type_concept_id,
        m.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            m.person_id person_id,
            mc.concept_id measurement_concept_id,
            m.Accident_And_Emergency_Treatment_Sixth measurement_source_value,
            PARSE_DATE('%Y%m%d', m.Arrival_Date) measurement_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(m.Arrival_Date ,'000000')) measurement_datetime,
            32817 measurement_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` m
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` mc on TRIM(mc.source_value) = TRIM(m.Accident_And_Emergency_Treatment_Sixth) AND mc.destination_table = 'MEASUREMENT' AND mc.source_table = 'SUS_BRI_AE_010415_to_300619' AND mc.source_column = 'Accident_And_Emergency_Treatment_Sixth'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(m.AandE_Attendance_Number as STRING),'_', m.person_id) 
        WHERE (m.Arrival_Date IS NOT NULL AND m.Arrival_Date NOT LIKE '% %')
        AND m.person_id IS NOT NULL
    ) m
    LEFT JOIN `CY_IMOSPHERE_CDM_531.measurement` om ON 
        om.person_id = m.person_id
        AND om.measurement_concept_id = m.measurement_concept_id
        AND om.measurement_source_value = m.measurement_source_value
        AND COALESCE(om.measurement_source_concept_id, -1) = -1
        AND om.measurement_datetime = m.measurement_datetime
        AND om.measurement_type_concept_id = m.measurement_type_concept_id
        AND COALESCE(om.provider_id, -1) = -1
        AND COALESCE(om.visit_occurrence_id, -1) = COALESCE(m.visit_occurrence_id, -1)
        AND COALESCE(om.visit_detail_id, -1) = -1
        AND COALESCE(om.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(om.value_as_number AS STRING), '') = ''
        AND COALESCE(om.unit_concept_id, -1) =  -1
        AND COALESCE(om.unit_source_value, '') =  ''
        AND COALESCE(value_source_value, '') = ''
    WHERE om.measurement_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added measurement', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Sixth -> measurement', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Seventh] Transfer   
    */
    SET max_current_id = COALESCE((SELECT measurement_id FROM `CY_IMOSPHERE_CDM_531.measurement` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.measurement`
    (
        measurement_id,
        person_id,
        measurement_concept_id,
        measurement_source_value,
        measurement_date,
        measurement_datetime,
        measurement_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as measurement_id,
        m.person_id,
        m.measurement_concept_id,
        m.measurement_source_value,
        m.measurement_date,
        m.measurement_datetime,
        m.measurement_type_concept_id,
        m.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            m.person_id person_id,
            mc.concept_id measurement_concept_id,
            m.Accident_And_Emergency_Treatment_Seventh measurement_source_value,
            PARSE_DATE('%Y%m%d', m.Arrival_Date) measurement_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(m.Arrival_Date ,'000000')) measurement_datetime,
            32817 measurement_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` m
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` mc on TRIM(mc.source_value) = TRIM(m.Accident_And_Emergency_Treatment_Seventh) AND mc.destination_table = 'MEASUREMENT' AND mc.source_table = 'SUS_BRI_AE_010415_to_300619' AND mc.source_column = 'Accident_And_Emergency_Treatment_Seventh'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(m.AandE_Attendance_Number as STRING),'_', m.person_id) 
        WHERE (m.Arrival_Date IS NOT NULL AND m.Arrival_Date NOT LIKE '% %')
        AND m.person_id IS NOT NULL
    ) m
    LEFT JOIN `CY_IMOSPHERE_CDM_531.measurement` om ON 
        om.person_id = m.person_id
        AND om.measurement_concept_id = m.measurement_concept_id
        AND om.measurement_source_value = m.measurement_source_value
        AND COALESCE(om.measurement_source_concept_id, -1) = -1
        AND om.measurement_datetime = m.measurement_datetime
        AND om.measurement_type_concept_id = m.measurement_type_concept_id
        AND COALESCE(om.provider_id, -1) = -1
        AND COALESCE(om.visit_occurrence_id, -1) = COALESCE(m.visit_occurrence_id, -1)
        AND COALESCE(om.visit_detail_id, -1) = -1
        AND COALESCE(om.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(om.value_as_number AS STRING), '') = ''
        AND COALESCE(om.unit_concept_id, -1) =  -1
        AND COALESCE(om.unit_source_value, '') =  ''
        AND COALESCE(value_source_value, '') = ''
    WHERE om.measurement_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added measurement', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Seventh -> measurement', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Eighth] Transfer   
    */
    SET max_current_id = COALESCE((SELECT measurement_id FROM `CY_IMOSPHERE_CDM_531.measurement` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.measurement`
    (
        measurement_id,
        person_id,
        measurement_concept_id,
        measurement_source_value,
        measurement_date,
        measurement_datetime,
        measurement_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as measurement_id,
        m.person_id,
        m.measurement_concept_id,
        m.measurement_source_value,
        m.measurement_date,
        m.measurement_datetime,
        m.measurement_type_concept_id,
        m.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            m.person_id person_id,
            mc.concept_id measurement_concept_id,
            m.Accident_And_Emergency_Treatment_Eighth measurement_source_value,
            PARSE_DATE('%Y%m%d', m.Arrival_Date) measurement_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(m.Arrival_Date ,'000000')) measurement_datetime,
            32817 measurement_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` m
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` mc on TRIM(mc.source_value) = TRIM(m.Accident_And_Emergency_Treatment_Eighth) AND mc.destination_table = 'MEASUREMENT' AND mc.source_table = 'SUS_BRI_AE_010415_to_300619' AND mc.source_column = 'Accident_And_Emergency_Treatment_Eighth'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(m.AandE_Attendance_Number as STRING),'_', m.person_id) 
        WHERE (m.Arrival_Date IS NOT NULL AND m.Arrival_Date NOT LIKE '% %')
        AND m.person_id IS NOT NULL
    ) m
    LEFT JOIN `CY_IMOSPHERE_CDM_531.measurement` om ON 
        om.person_id = m.person_id
        AND om.measurement_concept_id = m.measurement_concept_id
        AND om.measurement_source_value = m.measurement_source_value
        AND COALESCE(om.measurement_source_concept_id, -1) = -1
        AND om.measurement_datetime = m.measurement_datetime
        AND om.measurement_type_concept_id = m.measurement_type_concept_id
        AND COALESCE(om.provider_id, -1) = -1
        AND COALESCE(om.visit_occurrence_id, -1) = COALESCE(m.visit_occurrence_id, -1)
        AND COALESCE(om.visit_detail_id, -1) = -1
        AND COALESCE(om.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(om.value_as_number AS STRING), '') = ''
        AND COALESCE(om.unit_concept_id, -1) =  -1
        AND COALESCE(om.unit_source_value, '') =  ''
        AND COALESCE(value_source_value, '') = ''
    WHERE om.measurement_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added measurement', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Eighth -> measurement', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Ninth] Transfer   
    */
    SET max_current_id = COALESCE((SELECT measurement_id FROM `CY_IMOSPHERE_CDM_531.measurement` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.measurement`
    (
        measurement_id,
        person_id,
        measurement_concept_id,
        measurement_source_value,
        measurement_date,
        measurement_datetime,
        measurement_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as measurement_id,
        m.person_id,
        m.measurement_concept_id,
        m.measurement_source_value,
        m.measurement_date,
        m.measurement_datetime,
        m.measurement_type_concept_id,
        m.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            m.person_id person_id,
            mc.concept_id measurement_concept_id,
            m.Accident_And_Emergency_Treatment_Ninth measurement_source_value,
            PARSE_DATE('%Y%m%d', m.Arrival_Date) measurement_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(m.Arrival_Date ,'000000')) measurement_datetime,
            32817 measurement_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` m
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` mc on TRIM(mc.source_value) = TRIM(m.Accident_And_Emergency_Treatment_Ninth) AND mc.destination_table = 'MEASUREMENT' AND mc.source_table = 'SUS_BRI_AE_010415_to_300619' AND mc.source_column = 'Accident_And_Emergency_Treatment_Ninth'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(m.AandE_Attendance_Number as STRING),'_', m.person_id) 
        WHERE (m.Arrival_Date IS NOT NULL AND m.Arrival_Date NOT LIKE '% %')
        AND m.person_id IS NOT NULL
    ) m
    LEFT JOIN `CY_IMOSPHERE_CDM_531.measurement` om ON 
        om.person_id = m.person_id
        AND om.measurement_concept_id = m.measurement_concept_id
        AND om.measurement_source_value = m.measurement_source_value
        AND COALESCE(om.measurement_source_concept_id, -1) = -1
        AND om.measurement_datetime = m.measurement_datetime
        AND om.measurement_type_concept_id = m.measurement_type_concept_id
        AND COALESCE(om.provider_id, -1) = -1
        AND COALESCE(om.visit_occurrence_id, -1) = COALESCE(m.visit_occurrence_id, -1)
        AND COALESCE(om.visit_detail_id, -1) = -1
        AND COALESCE(om.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(om.value_as_number AS STRING), '') = ''
        AND COALESCE(om.unit_concept_id, -1) =  -1
        AND COALESCE(om.unit_source_value, '') =  ''
        AND COALESCE(value_source_value, '') = ''
    WHERE om.measurement_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added measurement', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Ninth -> measurement', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Tenth] Transfer   
    */
    SET max_current_id = COALESCE((SELECT measurement_id FROM `CY_IMOSPHERE_CDM_531.measurement` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.measurement`
    (
        measurement_id,
        person_id,
        measurement_concept_id,
        measurement_source_value,
        measurement_date,
        measurement_datetime,
        measurement_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as measurement_id,
        m.person_id,
        m.measurement_concept_id,
        m.measurement_source_value,
        m.measurement_date,
        m.measurement_datetime,
        m.measurement_type_concept_id,
        m.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            m.person_id person_id,
            mc.concept_id measurement_concept_id,
            m.Accident_And_Emergency_Treatment_Tenth measurement_source_value,
            PARSE_DATE('%Y%m%d', m.Arrival_Date) measurement_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(m.Arrival_Date ,'000000')) measurement_datetime,
            32817 measurement_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` m
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` mc on TRIM(mc.source_value) = TRIM(m.Accident_And_Emergency_Treatment_Tenth) AND mc.destination_table = 'MEASUREMENT' AND mc.source_table = 'SUS_BRI_AE_010415_to_300619' AND mc.source_column = 'Accident_And_Emergency_Treatment_Tenth'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(m.AandE_Attendance_Number as STRING),'_', m.person_id) 
        WHERE (m.Arrival_Date IS NOT NULL AND m.Arrival_Date NOT LIKE '% %')
        AND m.person_id IS NOT NULL
    ) m
    LEFT JOIN `CY_IMOSPHERE_CDM_531.measurement` om ON 
        om.person_id = m.person_id
        AND om.measurement_concept_id = m.measurement_concept_id
        AND om.measurement_source_value = m.measurement_source_value
        AND COALESCE(om.measurement_source_concept_id, -1) = -1
        AND om.measurement_datetime = m.measurement_datetime
        AND om.measurement_type_concept_id = m.measurement_type_concept_id
        AND COALESCE(om.provider_id, -1) = -1
        AND COALESCE(om.visit_occurrence_id, -1) = COALESCE(m.visit_occurrence_id, -1)
        AND COALESCE(om.visit_detail_id, -1) = -1
        AND COALESCE(om.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(om.value_as_number AS STRING), '') = ''
        AND COALESCE(om.unit_concept_id, -1) =  -1
        AND COALESCE(om.unit_source_value, '') =  ''
        AND COALESCE(value_source_value, '') = ''
    WHERE om.measurement_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added measurement', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Tenth -> measurement', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Eleventh] Transfer   
    */
    SET max_current_id = COALESCE((SELECT measurement_id FROM `CY_IMOSPHERE_CDM_531.measurement` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.measurement`
    (
        measurement_id,
        person_id,
        measurement_concept_id,
        measurement_source_value,
        measurement_date,
        measurement_datetime,
        measurement_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as measurement_id,
        m.person_id,
        m.measurement_concept_id,
        m.measurement_source_value,
        m.measurement_date,
        m.measurement_datetime,
        m.measurement_type_concept_id,
        m.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            m.person_id person_id,
            mc.concept_id measurement_concept_id,
            m.Accident_And_Emergency_Treatment_Eleventh measurement_source_value,
            PARSE_DATE('%Y%m%d', m.Arrival_Date) measurement_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(m.Arrival_Date ,'000000')) measurement_datetime,
            32817 measurement_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` m
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` mc on TRIM(mc.source_value) = TRIM(m.Accident_And_Emergency_Treatment_Eleventh) AND mc.destination_table = 'MEASUREMENT' AND mc.source_table = 'SUS_BRI_AE_010415_to_300619' AND mc.source_column = 'Accident_And_Emergency_Treatment_Eleventh'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(m.AandE_Attendance_Number as STRING),'_', m.person_id) 
        WHERE (m.Arrival_Date IS NOT NULL AND m.Arrival_Date NOT LIKE '% %')
        AND m.person_id IS NOT NULL
    ) m
    LEFT JOIN `CY_IMOSPHERE_CDM_531.measurement` om ON 
        om.person_id = m.person_id
        AND om.measurement_concept_id = m.measurement_concept_id
        AND om.measurement_source_value = m.measurement_source_value
        AND COALESCE(om.measurement_source_concept_id, -1) = -1
        AND om.measurement_datetime = m.measurement_datetime
        AND om.measurement_type_concept_id = m.measurement_type_concept_id
        AND COALESCE(om.provider_id, -1) = -1
        AND COALESCE(om.visit_occurrence_id, -1) = COALESCE(m.visit_occurrence_id, -1)
        AND COALESCE(om.visit_detail_id, -1) = -1
        AND COALESCE(om.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(om.value_as_number AS STRING), '') = ''
        AND COALESCE(om.unit_concept_id, -1) =  -1
        AND COALESCE(om.unit_source_value, '') =  ''
        AND COALESCE(value_source_value, '') = ''
    WHERE om.measurement_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added measurement', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Eleventh -> measurement', @@row_count, CURRENT_DATETIME();

    /*
        --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Twelfth] Transfer   
    */
    SET max_current_id = COALESCE((SELECT measurement_id FROM `CY_IMOSPHERE_CDM_531.measurement` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.measurement`
    (
        measurement_id,
        person_id,
        measurement_concept_id,
        measurement_source_value,
        measurement_date,
        measurement_datetime,
        measurement_type_concept_id,
        visit_occurrence_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as measurement_id,
        m.person_id,
        m.measurement_concept_id,
        m.measurement_source_value,
        m.measurement_date,
        m.measurement_datetime,
        m.measurement_type_concept_id,
        m.visit_occurrence_id
    FROM
    (
        SELECT DISTINCT
            m.person_id person_id,
            mc.concept_id measurement_concept_id,
            m.Accident_And_Emergency_Treatment_Twelfth measurement_source_value,
            PARSE_DATE('%Y%m%d', m.Arrival_Date) measurement_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(m.Arrival_Date ,'000000')) measurement_datetime,
            32817 measurement_type_concept_id, --EHR
            vl.id visit_occurrence_id
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` m
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` mc on TRIM(mc.source_value) = TRIM(m.Accident_And_Emergency_Treatment_Twelfth) AND mc.destination_table = 'MEASUREMENT' AND mc.source_table = 'SUS_BRI_AE_010415_to_300619' AND mc.source_column = 'Accident_And_Emergency_Treatment_Twelfth'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(m.AandE_Attendance_Number as STRING),'_', m.person_id) 
        WHERE (m.Arrival_Date IS NOT NULL AND m.Arrival_Date NOT LIKE '% %')
        AND m.person_id IS NOT NULL
    ) m
    LEFT JOIN `CY_IMOSPHERE_CDM_531.measurement` om ON 
        om.person_id = m.person_id
        AND om.measurement_concept_id = m.measurement_concept_id
        AND om.measurement_source_value = m.measurement_source_value
        AND COALESCE(om.measurement_source_concept_id, -1) = -1
        AND om.measurement_datetime = m.measurement_datetime
        AND om.measurement_type_concept_id = m.measurement_type_concept_id
        AND COALESCE(om.provider_id, -1) = -1
        AND COALESCE(om.visit_occurrence_id, -1) = COALESCE(m.visit_occurrence_id, -1)
        AND COALESCE(om.visit_detail_id, -1) = -1
        AND COALESCE(om.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(om.value_as_number AS STRING), '') = ''
        AND COALESCE(om.unit_concept_id, -1) =  -1
        AND COALESCE(om.unit_source_value, '') =  ''
        AND COALESCE(value_source_value, '') = ''
    WHERE om.measurement_id IS NULL;
     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added measurement', run_id, 'SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment_Twelfth -> measurement', @@row_count, CURRENT_DATETIME();
    
    /*
        --[tbl_SRCode] Transfer
    */
    SET max_current_id = COALESCE((SELECT measurement_id FROM `CY_IMOSPHERE_CDM_531.measurement` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.measurement`
    (
        measurement_id,
        person_id,
        measurement_concept_id,
        measurement_source_value,
        measurement_source_concept_id,
        measurement_date,
        measurement_datetime,
        measurement_time,
        measurement_type_concept_id,
        value_as_number,
        value_source_value,
        unit_concept_id,
        unit_source_value,
        provider_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as measurement_id,
        m.person_id,
        m.measurement_concept_id,
        m.measurement_source_value,
        m.measurement_source_concept_id,
        m.measurement_date,
        m.measurement_datetime,
        m.measurement_time,
        m.measurement_type_concept_id,
        m.value_as_number,
        m.value_source_value,
        m.unit_concept_id,
        m.unit_source_value,
        m.provider_id

    FROM
    (
        SELECT DISTINCT
            m.person_id person_id,
            dc.concept_id measurement_concept_id,
            m.ctv3code measurement_source_value,
            sc.concept_id measurement_source_concept_id,
            EXTRACT(DATE FROM dateevent) measurement_date,
            dateevent measurement_datetime,
            CAST(EXTRACT(TIME FROM dateevent) AS STRING) measurement_time,
            32817 measurement_type_concept_id, --EHR
            CAST(numericvalue AS FLOAT64) value_as_number,
            numericvalue value_source_value,
            unit.concept_id unit_concept_id,
            numericunit unit_source_value,
            p.provider_id provider_id
        FROM `CY_IMOSPHERE_WORKSPACE.tbl_SRCode` m
        JOIN `CY_LOOKUPS.tbl_CTV3ToSnomed_Map` sm ON sm.ctv3code = m.ctv3code
        JOIN `CY_CDM_VOCAB.concept` sc ON sc.concept_code = sm.SNOMEDCode
        JOIN `CY_CDM_VOCAB.concept_relationship` cr ON cr.concept_id_1 = sc.concept_id
        JOIN `CY_CDM_VOCAB.concept` dc ON dc.concept_id = cr.concept_id_2
        LEFT JOIN `CY_IMOSPHERE_CDM_531.provider` p ON p.provider_id = CAST(iddoneby AS INT64)
        LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` unit on unit.source_value = TRIM(m.numericunit) AND unit.destination_table = 'MEASUREMENT' AND unit.source_table = 'tbl_SRCode' AND unit.source_column = 'numericunit'
        WHERE sc.vocabulary_id = 'SNOMED'
        AND cr.relationship_id = 'Maps to'
        AND dc.standard_concept = 'S'
        AND dc.domain_id = 'Measurement'
        AND m.dateevent IS NOT NULL
        AND m.person_id IS NOT NULL
    ) m
    LEFT JOIN `CY_IMOSPHERE_CDM_531.measurement` om ON 
        om.person_id = m.person_id
        AND om.measurement_concept_id = m.measurement_concept_id
        AND om.measurement_source_value = m.measurement_source_value
        AND COALESCE(om.measurement_source_concept_id, -1) = COALESCE(m.measurement_source_concept_id, -1)
        AND om.measurement_datetime = m.measurement_datetime
        AND om.measurement_type_concept_id = m.measurement_type_concept_id
        AND COALESCE(om.provider_id, -1) = COALESCE(m.provider_id, -1)
        AND COALESCE(om.visit_occurrence_id, -1) = -1
        AND COALESCE(om.visit_detail_id, -1) = -1
        AND COALESCE(om.value_as_concept_id, -1) = -1
        AND COALESCE(CAST(om.value_as_number AS STRING), '') = COALESCE(CAST(m.value_as_number AS STRING), '')
        AND COALESCE(om.unit_concept_id, -1) = COALESCE(m.unit_concept_id, -1)
        AND COALESCE(om.unit_source_value, '') =  COALESCE(m.unit_source_value, '')
        AND COALESCE(om.value_source_value, '') = COALESCE(m.value_source_value, '')
    WHERE om.measurement_id IS NULL;

     
    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added measurement', run_id, 'tbl_SRCode -> measurement', @@row_count, CURRENT_DATETIME();
END;

CREATE OR REPLACE PROCEDURE CY_IMOSPHERE_WORKSPACE.TRANSFER_DRUG_EXPOSURE(run_id INT64)
OPTIONS (strict_mode=false)
BEGIN
    DECLARE max_current_id INT64;   

    /*
        --[tbl_SRImmunisation] Transfer
    */
    SET max_current_id = COALESCE((SELECT drug_exposure_id FROM `CY_IMOSPHERE_CDM_531.drug_exposure` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.drug_exposure`
    (
        drug_exposure_id,
        person_id,
        drug_concept_id,
        drug_source_value,
        drug_exposure_start_date,
        drug_exposure_start_datetime,
        drug_exposure_end_date,
        drug_exposure_end_datetime,
        drug_type_concept_id
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as drug_exposure_id,
        d.person_id,
        d.drug_concept_id,
        d.drug_source_value,
        d.drug_exposure_start_date,
        d.drug_exposure_start_datetime,
        d.drug_exposure_end_date,
        d.drug_exposure_end_datetime,
        d.drug_type_concept_id
    FROM
    (
        SELECT DISTINCT
            d.person_id person_id,
            dc.concept_id drug_concept_id,
            d.idimmunisationcontent drug_source_value,
            EXTRACT(DATE FROM COALESCE(dateevent, dateeventrecorded)) drug_exposure_start_date,
            COALESCE(dateevent, dateeventrecorded)drug_exposure_start_datetime,	
            EXTRACT(DATE FROM COALESCE(dateevent, dateeventrecorded)) drug_exposure_end_date,	
            COALESCE(dateevent, dateeventrecorded) drug_exposure_end_datetime,
            32817 drug_type_concept_id
        FROM `CY_IMOSPHERE_WORKSPACE.tbl_SRImmunisation` d
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` dc on dc.source_value = TRIM(d.idimmunisationcontent) AND dc.destination_table = 'DRUG_EXPOSURE' AND dc.source_table = 'tbl_SRImmunisation' AND dc.source_column = 'idimmunisationcontent'
        WHERE (d.dateevent IS NOT NULL OR d.dateeventrecorded IS NOT NULL)
        AND d.person_id IS NOT NULL
    ) d
    LEFT JOIN `CY_IMOSPHERE_CDM_531.drug_exposure` od ON 
        od.person_id = d.person_id
        AND od.drug_concept_id = d.drug_concept_id
        AND od.drug_source_value = d.drug_source_value
        AND COALESCE(od.drug_source_concept_id, -1) = -1
        AND od.drug_exposure_start_datetime = d.drug_exposure_start_datetime
        AND od.drug_exposure_end_datetime = d.drug_exposure_end_datetime
        AND od.drug_type_concept_id = d.drug_type_concept_id
        AND COALESCE(CAST(od.quantity AS STRING), '') = ''
    WHERE od.drug_exposure_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added drug_exposure', run_id, 'tbl_SRImmunisation -> drug_exposure', @@row_count, CURRENT_DATETIME();
         
    /*
        --[tbl_SRPrimaryCareMedication] Transfer
    */
    SET max_current_id = COALESCE((SELECT drug_exposure_id FROM `CY_IMOSPHERE_CDM_531.drug_exposure` ORDER BY 1 DESC LIMIT 1), 0);
    INSERT INTO `CY_IMOSPHERE_CDM_531.drug_exposure`
    (
        drug_exposure_id,
        person_id,
        drug_concept_id,
        drug_source_value,
        drug_source_concept_id,
        drug_exposure_start_date,
        drug_exposure_start_datetime,
        drug_exposure_end_date,
        drug_exposure_end_datetime,
        drug_type_concept_id,
        quantity
    )
    SELECT DISTINCT
        max_current_id + ROW_NUMBER() OVER() as drug_exposure_id,
        d.person_id,
        d.drug_concept_id,
        d.drug_source_value,
        d.drug_source_concept_id,
        d.drug_exposure_start_date,
        d.drug_exposure_start_datetime,
        d.drug_exposure_end_date,
        d.drug_exposure_end_datetime,
        d.drug_type_concept_id,
        d.quantity
    FROM
    (
        SELECT DISTINCT
            d.person_id person_id,
            dc.concept_id drug_concept_id,
            d.idmultilexdmd drug_source_value,
            cd.Source_Concept drug_source_concept_id,
            EXTRACT(DATE FROM d.DateMedicationStart) drug_exposure_start_date,
            d.DateMedicationStart drug_exposure_start_datetime,		
            EXTRACT(DATE FROM COALESCE(d.DateMedicationEnd, d.DateMedicationStart)) drug_exposure_end_date,
            COALESCE(d.DateMedicationEnd, d.DateMedicationStart) drug_exposure_end_datetime,
            32817 drug_type_concept_id,
            SAFE_CAST(SPLIT(REGEXP_REPLACE(medicationquantity,'[^0-9 ]',''), ' ')[OFFSET(0)] AS FLOAT64) quantity
        FROM `CY_IMOSPHERE_WORKSPACE.tbl_SRPrimaryCareMedication` d
        JOIN
        (
            SELECT
                sc.concept_code,
                sc.concept_id Source_Concept,
                ARRAY_AGG(STRUCT(dc.concept_id) ORDER BY dc.vocabulary_id LIMIT 1) dc
            FROM `CY_CDM_VOCAB.concept` sc
            JOIN `CY_CDM_VOCAB.concept_relationship` cr ON cr.concept_id_1 = sc.concept_id
            JOIN `CY_CDM_VOCAB.concept` dc ON dc.concept_id = cr.concept_id_2
            WHERE sc.vocabulary_id = 'dm+d'
            AND cr.relationship_id = 'Maps to'
            AND dc.standard_concept = 'S'
            GROUP BY sc.concept_code, sc.concept_id
        ) cd ON cd.concept_code = d.IDMultiLexDMD, UNNEST(dc) dc 
        WHERE (d.DateMedicationStart IS NOT NULL OR d.DateMedicationEnd IS NOT NULL)
        AND d.person_id IS NOT NULL
    ) d
    LEFT JOIN `CY_IMOSPHERE_CDM_531.drug_exposure` od ON 
        od.person_id = d.person_id
        AND od.drug_concept_id = d.drug_concept_id
        AND od.drug_source_value = d.drug_source_value
        AND COALESCE(od.drug_source_concept_id, -1) = COALESCE(d.drug_source_concept_id, -1)
        AND od.drug_exposure_start_datetime = d.drug_exposure_start_datetime
        AND od.drug_exposure_end_datetime = d.drug_exposure_end_datetime
        AND od.drug_type_concept_id = d.drug_type_concept_id
        AND COALESCE(CAST(od.quantity AS STRING), '') = COALESCE(CAST(d.quantity AS STRING), '')
    WHERE od.drug_exposure_id IS NULL;

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_log` (log_message, run_id, related_table, related_count, logged_at) 
    SELECT 'Added drug_exposure', run_id, 'tbl_SRPrimaryCareMedication -> drug_exposure', @@row_count, CURRENT_DATETIME();
END;

CREATE OR REPLACE PROCEDURE CY_IMOSPHERE_WORKSPACE.TRANSFER_CDM_SOURCE(run_id INT64)
OPTIONS (strict_mode=false)
BEGIN
	DELETE FROM `CY_IMOSPHERE_CDM_531.cdm_source` WHERE 1=1;
	INSERT INTO `CY_IMOSPHERE_CDM_531.cdm_source`
	(
		cdm_source_name,
		cdm_source_abbreviation,
		cdm_holder,
		source_description,
		source_documentation_reference,
		source_release_date,
		cdm_release_date,
		cdm_version,
		vocabulary_version
	)
	VALUES
	(
		'Connected Yorkshire Research Database',
		'CYRD',
		'Bradford Teaching Hospital Foundation Trust',
		'Bradford Districts Primary Care TPP System One',
		'https://connectedhealthcities.github.io/assets/connected-yorkshire/Section%206.3_CONNECTED_BRADFORD_JUNE2019.pdf',
		PARSE_DATE('%d/%m/%Y','31/08/2017'),
		PARSE_DATE('%d/%m/%Y','01/12/2022'),
		'5.3',
		(SELECT vocabulary_version FROM `CY_CDM_VOCAB.vocabulary` WHERE vocabulary_id = 'None')
	);
END;

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

CREATE OR REPLACE PROCEDURE CY_IMOSPHERE_WORKSPACE.COUNT_VISIT_OCCURRENCE()
OPTIONS (strict_mode=false)
BEGIN
    CREATE TEMP TABLE VISIT_OCCURRENCE_COUNTS (SourceCount INT64, CDMCount INT64);
    INSERT INTO VISIT_OCCURRENCE_COUNTS VALUES (0,0);

    /* From sources */
    --tbl_SRVisit
    UPDATE VISIT_OCCURRENCE_COUNTS SET SourceCount = SourceCount +
        (SELECT COUNT (DISTINCT id)     
        FROM (SELECT DISTINCT RowIdentifier, DateBooked, person_id FROM `CY_IMOSPHERE_WORKSPACE.tbl_SRVisit`) vo
        JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'tbl_SRVisit') vl ON vl.source_value = CONCAT(CAST(vo.RowIdentifier as STRING),'_', person_id)
        WHERE vo.DateBooked IS NOT NULL
        AND vo.person_id IS NOT NULL
        AND vo.RowIdentifier IS NOT NULL)
    WHERE SourceCount IS NOT NULL;

    --tbl_SRVisit
    UPDATE VISIT_OCCURRENCE_COUNTS SET SourceCount = SourceCount +
        (SELECT COUNT (DISTINCT id) 
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_OP_010415_to_300619` vo
        JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_OP_010415_to_300619') vl ON vl.source_value = CONCAT(CAST(vo.Generated_Record_Identifier as STRING),'_', person_id)
        WHERE vo.Appointment_Date IS NOT NULL
        AND vo.person_id IS NOT NULL
        AND vo.Generated_Record_Identifier IS NOT NULL)
    WHERE SourceCount IS NOT NULL;

    --SUS_BRI_APC_010415_to_300619_P1, src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2
    UPDATE VISIT_OCCURRENCE_COUNTS SET SourceCount = SourceCount +
        (    SELECT 
        COUNT(DISTINCT Id) 
        FROM 
        (
            SELECT DISTINCT 
                CONCAT(CAST(vo.Hospital_Provider_Spell_Number as STRING),'_', person_id) source_value
            FROM 
            `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1` vo
            WHERE vo.Start_Date_Hospital_Provider_Spell IS NOT NULL
            AND vo.person_id IS NOT NULL
            UNION DISTINCT
            SELECT DISTINCT
                CONCAT(CAST(vo.Hospital_Provider_Spell_Number as STRING),'_', person_id) source_value
            FROM `CY_IMOSPHERE_WORKSPACE.src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2` vo
            WHERE vo.Start_Date_Hospital_Provider_Spell IS NOT NULL
            AND vo.person_id IS NOT NULL
        ) vo
        JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl ON vl.source_value = vo.source_value)
    WHERE SourceCount IS NOT NULL;

    --SUS_BRI_AE_010415_to_300619
    UPDATE VISIT_OCCURRENCE_COUNTS SET SourceCount = SourceCount +
       (SELECT COUNT (DISTINCT id)
       FROM
       (
           SELECT
               CONCAT(CAST(vo.AandE_Attendance_Number as STRING),'_', person_id) source_value
           FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` vo
           WHERE vo.Arrival_Date IS NOT NULL
           AND vo.person_id IS NOT NULL
           AND vo.AandE_Attendance_Number IS NOT NULL
           AND vo.AandE_Arrival_Mode IS NOT NULL
       ) vo
       JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl ON vl.source_value = vo.source_value
       )
    WHERE SourceCount IS NOT NULL;

    /* From CDM */
    UPDATE VISIT_OCCURRENCE_COUNTS SET CDMCount = (SELECT COUNT(*) FROM `CY_IMOSPHERE_CDM_531.visit_occurrence`) WHERE CDMCount IS NOT NULL;

    SELECT 
        'Visit Occurrence' Type,
         SourceCount SourceCount,
         CDMCount DestinationCount
    FROM VISIT_OCCURRENCE_COUNTS
    WHERE SourceCount != CDMCount;
END;

CREATE OR REPLACE PROCEDURE CY_IMOSPHERE_WORKSPACE.COUNT_OBSERVATION_PERIOD()
OPTIONS (strict_mode=false)
BEGIN
    SELECT 
        'Observation Period - missing' Type,
        COUNT(person_id) PersonsWithoutAnObservationPeriod 
    FROM `CY_IMOSPHERE_CDM_531.person` p WHERE NOT EXISTS (SELECT * FROM `CY_IMOSPHERE_CDM_531.observation_period` op WHERE op.person_id = p.person_id);
END;

CREATE OR REPLACE PROCEDURE CY_IMOSPHERE_WORKSPACE.COUNT_VISIT_DETAIL()
OPTIONS (strict_mode=false)
BEGIN
    CREATE TEMP TABLE VISIT_DETAIL_COUNTS (SourceCount INT64, CDMCount INT64);
    INSERT INTO VISIT_DETAIL_COUNTS VALUES (0,0);
    
    /* From sources */
    UPDATE VISIT_DETAIL_COUNTS SET SourceCount = SourceCount +
    (SELECT COUNT (DISTINCT id)
    FROM
    (
        SELECT DISTINCT
            id
        FROM
        (
            SELECT DISTINCT
                CONCAT(CAST(vd.Hospital_Provider_Spell_Number as STRING),'_', vd.person_id, '_', vd.Episode_Number) source_value 
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1` vd
            WHERE (Start_Date_Consultant_Episode IS NOT NULL AND Start_Date_Consultant_Episode != '20')
            AND vd.person_id IS NOT NULL
            UNION DISTINCT
            SELECT DISTINCT
                    CONCAT(CAST(vd.Hospital_Provider_Spell_Number as STRING),'_', vd.person_id, '_', vd.Episode_Number) source_value 
            FROM `CY_IMOSPHERE_WORKSPACE.src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2` vd
            WHERE (Start_Date_Consultant_Episode IS NOT NULL AND Start_Date_Consultant_Episode != '20')
            AND vd.person_id IS NOT NULL
        ) vd
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups` vdl ON vdl.source_value = vd.source_value
        ))
    WHERE SourceCount IS NOT NULL;

    /* From CDM */
    UPDATE VISIT_DETAIL_COUNTS SET CDMCount = (SELECT COUNT(*) FROM `CY_IMOSPHERE_CDM_531.visit_detail`) WHERE CDMCount IS NOT NULL;

    SELECT 
        'Visit Detail' Type,
         SourceCount SourceCount,
         CDMCount DestinationCount
    FROM VISIT_DETAIL_COUNTS
    WHERE SourceCount != CDMCount;
END;

CREATE OR REPLACE PROCEDURE CY_IMOSPHERE_WORKSPACE.COUNT_CONDITION_OCCURRENCE()
OPTIONS (strict_mode=false)
BEGIN
    DROP TABLE IF EXISTS _SESSION.SOURCE_COUNTS;
    CREATE TEMP TABLE SOURCE_COUNTS (SourceValue STRING, SourceCount INT64);
    CREATE TEMP TABLE CONDITON_OCCURRENCE_COUNTS (SourceValue STRING, SourceCount INT64, CDMCount INT64);
    
    /* From sources */

    --[tbl_SRPatient]
    INSERT INTO SOURCE_COUNTS
    SELECT
        c.Gender SourceValue,
        Count(*) SourceCount
    FROM `CY_IMOSPHERE_WORKSPACE.tbl_SRPatient` c
    WHERE c.DateBirth IS NOT NULL
    AND c.person_id IS NOT NULL
    AND c.Gender = 'I'
    GROUP BY c.Gender;

    --[tbl_SRCode]
    INSERT INTO SOURCE_COUNTS
    SELECT
        c.condition_source_value SourceValue,
        Count(*) SourceCount
    FROM
    (
        SELECT DISTINCT
            c.person_id person_id,
            dc.concept_id condition_concept_id, 
            c.ctv3code condition_source_value, 
            sc.concept_id condition_source_concept_id,
            dateevent condition_start_date,
            dateevent condition_start_datetime,
            32817 condition_type_concept_id, --EHR
            p.provider_id provider_id
        FROM `CY_IMOSPHERE_WORKSPACE.tbl_SRCode` c
        JOIN `CY_LOOKUPS.tbl_CTV3ToSnomed_Map` sm ON sm.ctv3code = c.ctv3code
        JOIN `CY_CDM_VOCAB.concept` sc ON sc.concept_code = sm.SNOMEDCode
        JOIN `CY_CDM_VOCAB.concept_relationship` cr ON cr.concept_id_1 = sc.concept_id
        JOIN `CY_CDM_VOCAB.concept` dc ON dc.concept_id = cr.concept_id_2
        LEFT JOIN `CY_IMOSPHERE_CDM_531.provider` p ON p.provider_id = CAST(iddoneby AS INT64)
        WHERE sc.vocabulary_id = 'SNOMED'
        AND cr.relationship_id = 'Maps to'
        AND dc.standard_concept = 'S'
        AND dc.domain_id = 'Condition'
        AND c.dateevent IS NOT NULL
        AND c.person_id IS NOT NULL
    ) c
    GROUP BY c.condition_source_value;
    
    --[src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2] AND [SUS_BRI_APC_010415_to_300619_P1]

    --[Diagnosis_Primary_ICD]
    INSERT INTO SOURCE_COUNTS
    SELECT
        c.Diagnosis_Primary_ICD SourceValue,
        Count(*) SourceCount
    FROM
    (    
        SELECT DISTINCT
            person_id,
            Start_Date_Consultant_Episode,
            Diagnosis_Primary_ICD,
            CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id) visit_occurrence_source_value,
            CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id, '_', Episode_Number) visit_detail_source_value
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1`
        UNION DISTINCT
        SELECT DISTINCT
            person_id,
            Start_Date_Consultant_Episode,
            Diagnosis_Primary_ICD,
            CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id) visit_occurrence_source_value,
            CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id, '_', Episode_Number) visit_detail_source_value
        FROM `CY_IMOSPHERE_WORKSPACE.src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2`
    ) c
    LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups` vdl ON vdl.source_value = visit_detail_source_value
    LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = c.visit_occurrence_source_value
    JOIN `CY_CDM_VOCAB.concept` sc on REPLACE(sc.concept_code, '.', '') = c.Diagnosis_Primary_ICD
    JOIN `CY_CDM_VOCAB.concept_relationship` cr on cr.concept_id_1 = sc.concept_id
    JOIN `CY_CDM_VOCAB.concept` dc on dc.concept_id = cr.concept_id_2
    WHERE c.Start_Date_Consultant_Episode IS NOT NULL
    AND c.person_id IS NOT NULL
    AND cr.relationship_id = 'Maps to'
    AND dc.standard_concept = 'S'
    AND dc.domain_id = 'Condition'
    AND sc.vocabulary_id = 'ICD10'
    AND c.Diagnosis_Primary_ICD IS NOT NULL
    AND dc.vocabulary_id = 'SNOMED'
    GROUP BY c.Diagnosis_Primary_ICD;

    -- [Diagnosis_1st_Secondary_ICD]
    INSERT INTO SOURCE_COUNTS
    SELECT
        c.Diagnosis_1st_Secondary_ICD SourceValue,
        Count(*) SourceCount
    FROM
    (    
        SELECT DISTINCT
            person_id,
            Start_Date_Consultant_Episode,
            Diagnosis_1st_Secondary_ICD,
            CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id) visit_occurrence_source_value,
            CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id, '_', Episode_Number) visit_detail_source_value
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1`
        UNION DISTINCT
        SELECT DISTINCT
            person_id,
            Start_Date_Consultant_Episode,
            Diagnosis_1st_Secondary_ICD,
            CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id) visit_occurrence_source_value,
            CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id, '_', Episode_Number) visit_detail_source_value
        FROM `CY_IMOSPHERE_WORKSPACE.src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2`
    ) c
    LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups` vdl ON vdl.source_value = visit_detail_source_value
    LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = c.visit_occurrence_source_value
    JOIN `CY_CDM_VOCAB.concept` sc on REPLACE(sc.concept_code, '.', '') = c.Diagnosis_1st_Secondary_ICD
    JOIN `CY_CDM_VOCAB.concept_relationship` cr on cr.concept_id_1 = sc.concept_id
    JOIN `CY_CDM_VOCAB.concept` dc on dc.concept_id = cr.concept_id_2
    WHERE c.Start_Date_Consultant_Episode IS NOT NULL
    AND c.person_id IS NOT NULL
    AND cr.relationship_id = 'Maps to'
    AND dc.standard_concept = 'S'
    AND dc.domain_id = 'Condition'
    AND sc.vocabulary_id = 'ICD10'
    AND c.Diagnosis_1st_Secondary_ICD IS NOT NULL
    AND dc.vocabulary_id = 'SNOMED'
    GROUP BY c.Diagnosis_1st_Secondary_ICD;

     -- [Diagnosis_2nd_Secondary_ICD]
    INSERT INTO SOURCE_COUNTS
    SELECT
        c.Diagnosis_2nd_Secondary_ICD SourceValue,
        Count(*) SourceCount
    FROM
    (    
        SELECT DISTINCT
            person_id,
            Start_Date_Consultant_Episode,
            Diagnosis_1st_Secondary_ICD,
            Diagnosis_2nd_Secondary_ICD,
            CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id) visit_occurrence_source_value,
            CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id, '_', Episode_Number) visit_detail_source_value
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1`
        UNION DISTINCT
        SELECT DISTINCT
            person_id,
            Start_Date_Consultant_Episode,
            Diagnosis_1st_Secondary_ICD,
            Diagnosis_2nd_Secondary_ICD,
            CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id) visit_occurrence_source_value,
            CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id, '_', Episode_Number) visit_detail_source_value
        FROM `CY_IMOSPHERE_WORKSPACE.src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2`
    ) c
    LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups` vdl ON vdl.source_value = visit_detail_source_value
    LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = c.visit_occurrence_source_value
    JOIN `CY_CDM_VOCAB.concept` sc on REPLACE(sc.concept_code, '.', '') = c.Diagnosis_2nd_Secondary_ICD
    JOIN `CY_CDM_VOCAB.concept_relationship` cr on cr.concept_id_1 = sc.concept_id
    JOIN `CY_CDM_VOCAB.concept` dc on dc.concept_id = cr.concept_id_2
    WHERE c.Start_Date_Consultant_Episode IS NOT NULL
    AND c.person_id IS NOT NULL
    AND cr.relationship_id = 'Maps to'
    AND dc.standard_concept = 'S'
    AND dc.domain_id = 'Condition'
    AND sc.vocabulary_id = 'ICD10'
    AND c.Diagnosis_2nd_Secondary_ICD IS NOT NULL
    AND dc.vocabulary_id = 'SNOMED'
    AND c.Diagnosis_2nd_Secondary_ICD != c.Diagnosis_1st_Secondary_ICD
    GROUP BY c.Diagnosis_2nd_Secondary_ICD;
    
    -- [Diagnosis_3rd_Secondary_ICD]
    INSERT INTO SOURCE_COUNTS
    SELECT
        c.Diagnosis_3rd_Secondary_ICD SourceValue,
        Count(*) SourceCount
    FROM
    (    
        SELECT DISTINCT
            person_id,
            Start_Date_Consultant_Episode,
            Diagnosis_1st_Secondary_ICD,
            Diagnosis_2nd_Secondary_ICD,
            Diagnosis_3rd_Secondary_ICD,
            CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id) visit_occurrence_source_value,
            CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id, '_', Episode_Number) visit_detail_source_value
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1`
        UNION DISTINCT
        SELECT DISTINCT
            person_id,
            Start_Date_Consultant_Episode,
            Diagnosis_1st_Secondary_ICD,
            Diagnosis_2nd_Secondary_ICD,
            Diagnosis_3rd_Secondary_ICD,
            CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id) visit_occurrence_source_value,
            CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id, '_', Episode_Number) visit_detail_source_value
        FROM `CY_IMOSPHERE_WORKSPACE.src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2`
    ) c
    LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups` vdl ON vdl.source_value = visit_detail_source_value
    LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = c.visit_occurrence_source_value
    JOIN `CY_CDM_VOCAB.concept` sc on REPLACE(sc.concept_code, '.', '') = c.Diagnosis_3rd_Secondary_ICD
    JOIN `CY_CDM_VOCAB.concept_relationship` cr on cr.concept_id_1 = sc.concept_id
    JOIN `CY_CDM_VOCAB.concept` dc on dc.concept_id = cr.concept_id_2
    WHERE c.Start_Date_Consultant_Episode IS NOT NULL
    AND c.person_id IS NOT NULL
    AND cr.relationship_id = 'Maps to'
    AND dc.standard_concept = 'S'
    AND dc.domain_id = 'Condition'
    AND sc.vocabulary_id = 'ICD10'
    AND c.Diagnosis_2nd_Secondary_ICD IS NOT NULL
    AND dc.vocabulary_id = 'SNOMED'
    AND c.Diagnosis_3rd_Secondary_ICD != c.Diagnosis_1st_Secondary_ICD
    AND c.Diagnosis_3rd_Secondary_ICD != c.Diagnosis_2nd_Secondary_ICD
    GROUP BY c.Diagnosis_3rd_Secondary_ICD;

    -- [Diagnosis_4th_Secondary_ICD]
    INSERT INTO SOURCE_COUNTS
    SELECT
        c.Diagnosis_4th_Secondary_ICD SourceValue,
        Count(*) SourceCount
    FROM
    (    
        SELECT DISTINCT
            person_id,
            Start_Date_Consultant_Episode,
            Diagnosis_1st_Secondary_ICD,
            Diagnosis_2nd_Secondary_ICD,
            Diagnosis_3rd_Secondary_ICD,
            Diagnosis_4th_Secondary_ICD,
            CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id) visit_occurrence_source_value,
            CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id, '_', Episode_Number) visit_detail_source_value
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1`
        UNION DISTINCT
        SELECT DISTINCT
            person_id,
            Start_Date_Consultant_Episode,
            Diagnosis_1st_Secondary_ICD,
            Diagnosis_2nd_Secondary_ICD,
            Diagnosis_3rd_Secondary_ICD,
            Diagnosis_4th_Secondary_ICD,
            CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id) visit_occurrence_source_value,
            CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id, '_', Episode_Number) visit_detail_source_value
        FROM `CY_IMOSPHERE_WORKSPACE.src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2`
    ) c
    LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups` vdl ON vdl.source_value = visit_detail_source_value
    LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = c.visit_occurrence_source_value
    JOIN `CY_CDM_VOCAB.concept` sc on REPLACE(sc.concept_code, '.', '') = c.Diagnosis_4th_Secondary_ICD
    JOIN `CY_CDM_VOCAB.concept_relationship` cr on cr.concept_id_1 = sc.concept_id
    JOIN `CY_CDM_VOCAB.concept` dc on dc.concept_id = cr.concept_id_2
    WHERE c.Start_Date_Consultant_Episode IS NOT NULL
    AND c.person_id IS NOT NULL
    AND cr.relationship_id = 'Maps to'
    AND dc.standard_concept = 'S'
    AND dc.domain_id = 'Condition'
    AND sc.vocabulary_id = 'ICD10'
    AND c.Diagnosis_4th_Secondary_ICD IS NOT NULL
    AND dc.vocabulary_id = 'SNOMED'
    AND c.Diagnosis_4th_Secondary_ICD != c.Diagnosis_1st_Secondary_ICD
    AND c.Diagnosis_4th_Secondary_ICD != c.Diagnosis_2nd_Secondary_ICD
    AND c.Diagnosis_4th_Secondary_ICD != c.Diagnosis_3rd_Secondary_ICD
    GROUP BY c.Diagnosis_4th_Secondary_ICD;

    -- [Diagnosis_5th_Secondary_ICD]
    INSERT INTO SOURCE_COUNTS
    SELECT
        c.Diagnosis_5th_Secondary_ICD SourceValue,
        Count(*) SourceCount
    FROM
    (    
        SELECT DISTINCT
            person_id,
            Start_Date_Consultant_Episode,
            Diagnosis_1st_Secondary_ICD,
            Diagnosis_2nd_Secondary_ICD,
            Diagnosis_3rd_Secondary_ICD,
            Diagnosis_4th_Secondary_ICD,
            Diagnosis_5th_Secondary_ICD,
            CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id) visit_occurrence_source_value,
            CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id, '_', Episode_Number) visit_detail_source_value
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1`
        UNION DISTINCT
        SELECT DISTINCT
            person_id,
            Start_Date_Consultant_Episode,
            Diagnosis_1st_Secondary_ICD,
            Diagnosis_2nd_Secondary_ICD,
            Diagnosis_3rd_Secondary_ICD,
            Diagnosis_4th_Secondary_ICD,
            Diagnosis_5th_Secondary_ICD,
            CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id) visit_occurrence_source_value,
            CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id, '_', Episode_Number) visit_detail_source_value
        FROM `CY_IMOSPHERE_WORKSPACE.src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2`
    ) c
    LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups` vdl ON vdl.source_value = visit_detail_source_value
    LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = c.visit_occurrence_source_value
    JOIN `CY_CDM_VOCAB.concept` sc on REPLACE(sc.concept_code, '.', '') = c.Diagnosis_5th_Secondary_ICD
    JOIN `CY_CDM_VOCAB.concept_relationship` cr on cr.concept_id_1 = sc.concept_id
    JOIN `CY_CDM_VOCAB.concept` dc on dc.concept_id = cr.concept_id_2
    WHERE c.Start_Date_Consultant_Episode IS NOT NULL
    AND c.person_id IS NOT NULL
    AND cr.relationship_id = 'Maps to'
    AND dc.standard_concept = 'S'
    AND dc.domain_id = 'Condition'
    AND sc.vocabulary_id = 'ICD10'
    AND c.Diagnosis_5th_Secondary_ICD IS NOT NULL
    AND dc.vocabulary_id = 'SNOMED'
    AND c.Diagnosis_5th_Secondary_ICD != c.Diagnosis_1st_Secondary_ICD
    AND c.Diagnosis_5th_Secondary_ICD != c.Diagnosis_2nd_Secondary_ICD
    AND c.Diagnosis_5th_Secondary_ICD != c.Diagnosis_3rd_Secondary_ICD
    AND c.Diagnosis_5th_Secondary_ICD != c.Diagnosis_4th_Secondary_ICD
    GROUP BY c.Diagnosis_5th_Secondary_ICD;

    -- [Diagnosis_6th_Secondary_ICD]
    INSERT INTO SOURCE_COUNTS
    SELECT
        c.Diagnosis_6th_Secondary_ICD SourceValue,
        Count(*) SourceCount
    FROM
    (    
        SELECT DISTINCT
            person_id,
            Start_Date_Consultant_Episode,
            Diagnosis_1st_Secondary_ICD,
            Diagnosis_2nd_Secondary_ICD,
            Diagnosis_3rd_Secondary_ICD,
            Diagnosis_4th_Secondary_ICD,
            Diagnosis_5th_Secondary_ICD,
            Diagnosis_6th_Secondary_ICD,
            CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id) visit_occurrence_source_value,
            CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id, '_', Episode_Number) visit_detail_source_value
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1`
        UNION DISTINCT
        SELECT DISTINCT
            person_id,
            Start_Date_Consultant_Episode,
            Diagnosis_1st_Secondary_ICD,
            Diagnosis_2nd_Secondary_ICD,
            Diagnosis_3rd_Secondary_ICD,
            Diagnosis_4th_Secondary_ICD,
            Diagnosis_5th_Secondary_ICD,
            Diagnosis_6th_Secondary_ICD,
            CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id) visit_occurrence_source_value,
            CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id, '_', Episode_Number) visit_detail_source_value
        FROM `CY_IMOSPHERE_WORKSPACE.src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2`
    ) c
    LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups` vdl ON vdl.source_value = visit_detail_source_value
    LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = c.visit_occurrence_source_value
    JOIN `CY_CDM_VOCAB.concept` sc on REPLACE(sc.concept_code, '.', '') = c.Diagnosis_6th_Secondary_ICD
    JOIN `CY_CDM_VOCAB.concept_relationship` cr on cr.concept_id_1 = sc.concept_id
    JOIN `CY_CDM_VOCAB.concept` dc on dc.concept_id = cr.concept_id_2
    WHERE c.Start_Date_Consultant_Episode IS NOT NULL
    AND c.person_id IS NOT NULL
    AND cr.relationship_id = 'Maps to'
    AND dc.standard_concept = 'S'
    AND dc.domain_id = 'Condition'
    AND sc.vocabulary_id = 'ICD10'
    AND c.Diagnosis_6th_Secondary_ICD IS NOT NULL
    AND dc.vocabulary_id = 'SNOMED'
    AND c.Diagnosis_6th_Secondary_ICD != c.Diagnosis_1st_Secondary_ICD
    AND c.Diagnosis_6th_Secondary_ICD != c.Diagnosis_2nd_Secondary_ICD
    AND c.Diagnosis_6th_Secondary_ICD != c.Diagnosis_3rd_Secondary_ICD
    AND c.Diagnosis_6th_Secondary_ICD != c.Diagnosis_4th_Secondary_ICD
    AND c.Diagnosis_6th_Secondary_ICD != c.Diagnosis_5th_Secondary_ICD
    GROUP BY c.Diagnosis_6th_Secondary_ICD;

    -- [Diagnosis_7th_Secondary_ICD]
    INSERT INTO SOURCE_COUNTS
    SELECT
        c.Diagnosis_7th_Secondary_ICD SourceValue,
        Count(*) SourceCount
    FROM
    (    
        SELECT DISTINCT
            person_id,
            Start_Date_Consultant_Episode,
            Diagnosis_1st_Secondary_ICD,
            Diagnosis_2nd_Secondary_ICD,
            Diagnosis_3rd_Secondary_ICD,
            Diagnosis_4th_Secondary_ICD,
            Diagnosis_5th_Secondary_ICD,
            Diagnosis_6th_Secondary_ICD,
            Diagnosis_7th_Secondary_ICD,
            CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id) visit_occurrence_source_value,
            CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id, '_', Episode_Number) visit_detail_source_value
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1`
        UNION DISTINCT
        SELECT DISTINCT
            person_id,
            Start_Date_Consultant_Episode,
            Diagnosis_1st_Secondary_ICD,
            Diagnosis_2nd_Secondary_ICD,
            Diagnosis_3rd_Secondary_ICD,
            Diagnosis_4th_Secondary_ICD,
            Diagnosis_5th_Secondary_ICD,
            Diagnosis_6th_Secondary_ICD,
            Diagnosis_7th_Secondary_ICD,
            CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id) visit_occurrence_source_value,
            CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id, '_', Episode_Number) visit_detail_source_value
        FROM `CY_IMOSPHERE_WORKSPACE.src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2`
    ) c
    LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups` vdl ON vdl.source_value = visit_detail_source_value
    LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = c.visit_occurrence_source_value
    JOIN `CY_CDM_VOCAB.concept` sc on REPLACE(sc.concept_code, '.', '') = c.Diagnosis_7th_Secondary_ICD
    JOIN `CY_CDM_VOCAB.concept_relationship` cr on cr.concept_id_1 = sc.concept_id
    JOIN `CY_CDM_VOCAB.concept` dc on dc.concept_id = cr.concept_id_2
    WHERE c.Start_Date_Consultant_Episode IS NOT NULL
    AND c.person_id IS NOT NULL
    AND cr.relationship_id = 'Maps to'
    AND dc.standard_concept = 'S'
    AND dc.domain_id = 'Condition'
    AND sc.vocabulary_id = 'ICD10'
    AND c.Diagnosis_7th_Secondary_ICD IS NOT NULL
    AND dc.vocabulary_id = 'SNOMED'
    AND c.Diagnosis_7th_Secondary_ICD != c.Diagnosis_1st_Secondary_ICD
    AND c.Diagnosis_7th_Secondary_ICD != c.Diagnosis_2nd_Secondary_ICD
    AND c.Diagnosis_7th_Secondary_ICD != c.Diagnosis_3rd_Secondary_ICD
    AND c.Diagnosis_7th_Secondary_ICD != c.Diagnosis_4th_Secondary_ICD
    AND c.Diagnosis_7th_Secondary_ICD != c.Diagnosis_5th_Secondary_ICD
    AND c.Diagnosis_7th_Secondary_ICD != c.Diagnosis_6th_Secondary_ICD
    GROUP BY c.Diagnosis_7th_Secondary_ICD;

    -- [Diagnosis_8th_Secondary_ICD]
    INSERT INTO SOURCE_COUNTS
    SELECT
        c.Diagnosis_8th_Secondary_ICD SourceValue,
        Count(*) SourceCount
    FROM
    (    
        SELECT DISTINCT
            person_id,
            Start_Date_Consultant_Episode,
            Diagnosis_1st_Secondary_ICD,
            Diagnosis_2nd_Secondary_ICD,
            Diagnosis_3rd_Secondary_ICD,
            Diagnosis_4th_Secondary_ICD,
            Diagnosis_5th_Secondary_ICD,
            Diagnosis_6th_Secondary_ICD,
            Diagnosis_7th_Secondary_ICD,
            Diagnosis_8th_Secondary_ICD,
            CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id) visit_occurrence_source_value,
            CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id, '_', Episode_Number) visit_detail_source_value
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1`
        UNION DISTINCT
        SELECT DISTINCT
            person_id,
            Start_Date_Consultant_Episode,
            Diagnosis_1st_Secondary_ICD,
            Diagnosis_2nd_Secondary_ICD,
            Diagnosis_3rd_Secondary_ICD,
            Diagnosis_4th_Secondary_ICD,
            Diagnosis_5th_Secondary_ICD,
            Diagnosis_6th_Secondary_ICD,
            Diagnosis_7th_Secondary_ICD,
            Diagnosis_8th_Secondary_ICD,
            CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id) visit_occurrence_source_value,
            CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id, '_', Episode_Number) visit_detail_source_value
        FROM `CY_IMOSPHERE_WORKSPACE.src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2`
    ) c
    LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups` vdl ON vdl.source_value = visit_detail_source_value
    LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = c.visit_occurrence_source_value
    JOIN `CY_CDM_VOCAB.concept` sc on REPLACE(sc.concept_code, '.', '') = c.Diagnosis_8th_Secondary_ICD
    JOIN `CY_CDM_VOCAB.concept_relationship` cr on cr.concept_id_1 = sc.concept_id
    JOIN `CY_CDM_VOCAB.concept` dc on dc.concept_id = cr.concept_id_2
    WHERE c.Start_Date_Consultant_Episode IS NOT NULL
    AND c.person_id IS NOT NULL
    AND cr.relationship_id = 'Maps to'
    AND dc.standard_concept = 'S'
    AND dc.domain_id = 'Condition'
    AND sc.vocabulary_id = 'ICD10'
    AND c.Diagnosis_8th_Secondary_ICD IS NOT NULL
    AND dc.vocabulary_id = 'SNOMED'
    AND c.Diagnosis_8th_Secondary_ICD != c.Diagnosis_1st_Secondary_ICD
    AND c.Diagnosis_8th_Secondary_ICD != c.Diagnosis_2nd_Secondary_ICD
    AND c.Diagnosis_8th_Secondary_ICD != c.Diagnosis_3rd_Secondary_ICD
    AND c.Diagnosis_8th_Secondary_ICD != c.Diagnosis_4th_Secondary_ICD
    AND c.Diagnosis_8th_Secondary_ICD != c.Diagnosis_5th_Secondary_ICD
    AND c.Diagnosis_8th_Secondary_ICD != c.Diagnosis_6th_Secondary_ICD
    AND c.Diagnosis_8th_Secondary_ICD != c.Diagnosis_7th_Secondary_ICD
    GROUP BY c.Diagnosis_8th_Secondary_ICD;

    -- [Diagnosis_9th_Secondary_ICD]
    INSERT INTO SOURCE_COUNTS
    SELECT
        c.Diagnosis_9th_Secondary_ICD SourceValue,
        Count(*) SourceCount
    FROM
    (    
        SELECT DISTINCT
            person_id,
            Start_Date_Consultant_Episode,
            Diagnosis_1st_Secondary_ICD,
            Diagnosis_2nd_Secondary_ICD,
            Diagnosis_3rd_Secondary_ICD,
            Diagnosis_4th_Secondary_ICD,
            Diagnosis_5th_Secondary_ICD,
            Diagnosis_6th_Secondary_ICD,
            Diagnosis_7th_Secondary_ICD,
            Diagnosis_8th_Secondary_ICD,
            Diagnosis_9th_Secondary_ICD,
            CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id) visit_occurrence_source_value,
            CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id, '_', Episode_Number) visit_detail_source_value
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1`
        UNION DISTINCT
        SELECT DISTINCT
            person_id,
            Start_Date_Consultant_Episode,
            Diagnosis_1st_Secondary_ICD,
            Diagnosis_2nd_Secondary_ICD,
            Diagnosis_3rd_Secondary_ICD,
            Diagnosis_4th_Secondary_ICD,
            Diagnosis_5th_Secondary_ICD,
            Diagnosis_6th_Secondary_ICD,
            Diagnosis_7th_Secondary_ICD,
            Diagnosis_8th_Secondary_ICD,
            Diagnosis_9th_Secondary_ICD,
            CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id) visit_occurrence_source_value,
            CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id, '_', Episode_Number) visit_detail_source_value
        FROM `CY_IMOSPHERE_WORKSPACE.src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2`
    ) c
    LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups` vdl ON vdl.source_value = visit_detail_source_value
    LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = c.visit_occurrence_source_value
    JOIN `CY_CDM_VOCAB.concept` sc on REPLACE(sc.concept_code, '.', '') = c.Diagnosis_9th_Secondary_ICD
    JOIN `CY_CDM_VOCAB.concept_relationship` cr on cr.concept_id_1 = sc.concept_id
    JOIN `CY_CDM_VOCAB.concept` dc on dc.concept_id = cr.concept_id_2
    WHERE c.Start_Date_Consultant_Episode IS NOT NULL
    AND c.person_id IS NOT NULL
    AND cr.relationship_id = 'Maps to'
    AND dc.standard_concept = 'S'
    AND dc.domain_id = 'Condition'
    AND sc.vocabulary_id = 'ICD10'
    AND c.Diagnosis_9th_Secondary_ICD IS NOT NULL
    AND dc.vocabulary_id = 'SNOMED'
    AND c.Diagnosis_9th_Secondary_ICD != c.Diagnosis_1st_Secondary_ICD
    AND c.Diagnosis_9th_Secondary_ICD != c.Diagnosis_2nd_Secondary_ICD
    AND c.Diagnosis_9th_Secondary_ICD != c.Diagnosis_3rd_Secondary_ICD
    AND c.Diagnosis_9th_Secondary_ICD != c.Diagnosis_4th_Secondary_ICD
    AND c.Diagnosis_9th_Secondary_ICD != c.Diagnosis_5th_Secondary_ICD
    AND c.Diagnosis_9th_Secondary_ICD != c.Diagnosis_6th_Secondary_ICD
    AND c.Diagnosis_9th_Secondary_ICD != c.Diagnosis_7th_Secondary_ICD
    AND c.Diagnosis_9th_Secondary_ICD != c.Diagnosis_8th_Secondary_ICD
    GROUP BY c.Diagnosis_9th_Secondary_ICD;

    -- [Diagnosis_10th_Secondary_ICD]
    INSERT INTO SOURCE_COUNTS
    SELECT
        c.Diagnosis_10th_Secondary_ICD SourceValue,
        Count(*) SourceCount
    FROM
    (    
        SELECT DISTINCT
            person_id,
            Start_Date_Consultant_Episode,
            Diagnosis_1st_Secondary_ICD,
            Diagnosis_2nd_Secondary_ICD,
            Diagnosis_3rd_Secondary_ICD,
            Diagnosis_4th_Secondary_ICD,
            Diagnosis_5th_Secondary_ICD,
            Diagnosis_6th_Secondary_ICD,
            Diagnosis_7th_Secondary_ICD,
            Diagnosis_8th_Secondary_ICD,
            Diagnosis_9th_Secondary_ICD,
            Diagnosis_10th_Secondary_ICD,
            CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id) visit_occurrence_source_value,
            CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id, '_', Episode_Number) visit_detail_source_value
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1`
        UNION DISTINCT
        SELECT DISTINCT
            person_id,
            Start_Date_Consultant_Episode,
            Diagnosis_1st_Secondary_ICD,
            Diagnosis_2nd_Secondary_ICD,
            Diagnosis_3rd_Secondary_ICD,
            Diagnosis_4th_Secondary_ICD,
            Diagnosis_5th_Secondary_ICD,
            Diagnosis_6th_Secondary_ICD,
            Diagnosis_7th_Secondary_ICD,
            Diagnosis_8th_Secondary_ICD,
            Diagnosis_9th_Secondary_ICD,
            Diagnosis_10th_Secondary_ICD,
            CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id) visit_occurrence_source_value,
            CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id, '_', Episode_Number) visit_detail_source_value
        FROM `CY_IMOSPHERE_WORKSPACE.src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2`
    ) c
    LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups` vdl ON vdl.source_value = visit_detail_source_value
    LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = c.visit_occurrence_source_value
    JOIN `CY_CDM_VOCAB.concept` sc on REPLACE(sc.concept_code, '.', '') = c.Diagnosis_10th_Secondary_ICD
    JOIN `CY_CDM_VOCAB.concept_relationship` cr on cr.concept_id_1 = sc.concept_id
    JOIN `CY_CDM_VOCAB.concept` dc on dc.concept_id = cr.concept_id_2
    WHERE c.Start_Date_Consultant_Episode IS NOT NULL
    AND c.person_id IS NOT NULL
    AND cr.relationship_id = 'Maps to'
    AND dc.standard_concept = 'S'
    AND dc.domain_id = 'Condition'
    AND sc.vocabulary_id = 'ICD10'
    AND c.Diagnosis_10th_Secondary_ICD IS NOT NULL
    AND dc.vocabulary_id = 'SNOMED'
    AND c.Diagnosis_10th_Secondary_ICD != c.Diagnosis_1st_Secondary_ICD
    AND c.Diagnosis_10th_Secondary_ICD != c.Diagnosis_2nd_Secondary_ICD
    AND c.Diagnosis_10th_Secondary_ICD != c.Diagnosis_3rd_Secondary_ICD
    AND c.Diagnosis_10th_Secondary_ICD != c.Diagnosis_4th_Secondary_ICD
    AND c.Diagnosis_10th_Secondary_ICD != c.Diagnosis_5th_Secondary_ICD
    AND c.Diagnosis_10th_Secondary_ICD != c.Diagnosis_6th_Secondary_ICD
    AND c.Diagnosis_10th_Secondary_ICD != c.Diagnosis_7th_Secondary_ICD
    AND c.Diagnosis_10th_Secondary_ICD != c.Diagnosis_8th_Secondary_ICD
    AND c.Diagnosis_10th_Secondary_ICD != c.Diagnosis_9th_Secondary_ICD
    GROUP BY c.Diagnosis_10th_Secondary_ICD;

    -- [Diagnosis_11th_Secondary_ICD]
    INSERT INTO SOURCE_COUNTS
    SELECT
        c.Diagnosis_11th_Secondary_ICD SourceValue,
        Count(*) SourceCount
    FROM
    (    
        SELECT DISTINCT
            person_id,
            Start_Date_Consultant_Episode,
            Diagnosis_1st_Secondary_ICD,
            Diagnosis_2nd_Secondary_ICD,
            Diagnosis_3rd_Secondary_ICD,
            Diagnosis_4th_Secondary_ICD,
            Diagnosis_5th_Secondary_ICD,
            Diagnosis_6th_Secondary_ICD,
            Diagnosis_7th_Secondary_ICD,
            Diagnosis_8th_Secondary_ICD,
            Diagnosis_9th_Secondary_ICD,
            Diagnosis_10th_Secondary_ICD,
            Diagnosis_11th_Secondary_ICD,
            CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id) visit_occurrence_source_value,
            CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id, '_', Episode_Number) visit_detail_source_value
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1`
        UNION DISTINCT
        SELECT DISTINCT
            person_id,
            Start_Date_Consultant_Episode,
            Diagnosis_1st_Secondary_ICD,
            Diagnosis_2nd_Secondary_ICD,
            Diagnosis_3rd_Secondary_ICD,
            Diagnosis_4th_Secondary_ICD,
            Diagnosis_5th_Secondary_ICD,
            Diagnosis_6th_Secondary_ICD,
            Diagnosis_7th_Secondary_ICD,
            Diagnosis_8th_Secondary_ICD,
            Diagnosis_9th_Secondary_ICD,
            Diagnosis_10th_Secondary_ICD,
            Diagnosis_11th_Secondary_ICD,
            CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id) visit_occurrence_source_value,
            CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id, '_', Episode_Number) visit_detail_source_value
        FROM `CY_IMOSPHERE_WORKSPACE.src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2`
    ) c
    LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups` vdl ON vdl.source_value = visit_detail_source_value
    LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = c.visit_occurrence_source_value
    JOIN `CY_CDM_VOCAB.concept` sc on REPLACE(sc.concept_code, '.', '') = c.Diagnosis_11th_Secondary_ICD
    JOIN `CY_CDM_VOCAB.concept_relationship` cr on cr.concept_id_1 = sc.concept_id
    JOIN `CY_CDM_VOCAB.concept` dc on dc.concept_id = cr.concept_id_2
    WHERE c.Start_Date_Consultant_Episode IS NOT NULL
    AND c.person_id IS NOT NULL
    AND cr.relationship_id = 'Maps to'
    AND dc.standard_concept = 'S'
    AND dc.domain_id = 'Condition'
    AND sc.vocabulary_id = 'ICD10'
    AND c.Diagnosis_11th_Secondary_ICD IS NOT NULL
    AND dc.vocabulary_id = 'SNOMED'
    AND c.Diagnosis_11th_Secondary_ICD != c.Diagnosis_1st_Secondary_ICD
    AND c.Diagnosis_11th_Secondary_ICD != c.Diagnosis_2nd_Secondary_ICD
    AND c.Diagnosis_11th_Secondary_ICD != c.Diagnosis_3rd_Secondary_ICD
    AND c.Diagnosis_11th_Secondary_ICD != c.Diagnosis_4th_Secondary_ICD
    AND c.Diagnosis_11th_Secondary_ICD != c.Diagnosis_5th_Secondary_ICD
    AND c.Diagnosis_11th_Secondary_ICD != c.Diagnosis_6th_Secondary_ICD
    AND c.Diagnosis_11th_Secondary_ICD != c.Diagnosis_7th_Secondary_ICD
    AND c.Diagnosis_11th_Secondary_ICD != c.Diagnosis_8th_Secondary_ICD
    AND c.Diagnosis_11th_Secondary_ICD != c.Diagnosis_9th_Secondary_ICD
    AND c.Diagnosis_11th_Secondary_ICD != c.Diagnosis_10th_Secondary_ICD
    GROUP BY c.Diagnosis_11th_Secondary_ICD;

    -- [Diagnosis_12th_Secondary_ICD]
    INSERT INTO SOURCE_COUNTS
    SELECT
        c.Diagnosis_12th_Secondary_ICD SourceValue,
        Count(*) SourceCount
    FROM
    (    
        SELECT DISTINCT
            person_id,
            Start_Date_Consultant_Episode,
            Diagnosis_1st_Secondary_ICD,
            Diagnosis_2nd_Secondary_ICD,
            Diagnosis_3rd_Secondary_ICD,
            Diagnosis_4th_Secondary_ICD,
            Diagnosis_5th_Secondary_ICD,
            Diagnosis_6th_Secondary_ICD,
            Diagnosis_7th_Secondary_ICD,
            Diagnosis_8th_Secondary_ICD,
            Diagnosis_9th_Secondary_ICD,
            Diagnosis_10th_Secondary_ICD,
            Diagnosis_11th_Secondary_ICD,
            Diagnosis_12th_Secondary_ICD,
            CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id) visit_occurrence_source_value,
            CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id, '_', Episode_Number) visit_detail_source_value
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1`
        UNION DISTINCT
        SELECT DISTINCT
            person_id,
            Start_Date_Consultant_Episode,
            Diagnosis_1st_Secondary_ICD,
            Diagnosis_2nd_Secondary_ICD,
            Diagnosis_3rd_Secondary_ICD,
            Diagnosis_4th_Secondary_ICD,
            Diagnosis_5th_Secondary_ICD,
            Diagnosis_6th_Secondary_ICD,
            Diagnosis_7th_Secondary_ICD,
            Diagnosis_8th_Secondary_ICD,
            Diagnosis_9th_Secondary_ICD,
            Diagnosis_10th_Secondary_ICD,
            Diagnosis_11th_Secondary_ICD,
            Diagnosis_12th_Secondary_ICD,
            CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id) visit_occurrence_source_value,
            CONCAT(CAST(Hospital_Provider_Spell_Number as STRING),'_', person_id, '_', Episode_Number) visit_detail_source_value
        FROM `CY_IMOSPHERE_WORKSPACE.src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2`
    ) c
    LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups` vdl ON vdl.source_value = visit_detail_source_value
    LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = c.visit_occurrence_source_value
    JOIN `CY_CDM_VOCAB.concept` sc on REPLACE(sc.concept_code, '.', '') = c.Diagnosis_12th_Secondary_ICD
    JOIN `CY_CDM_VOCAB.concept_relationship` cr on cr.concept_id_1 = sc.concept_id
    JOIN `CY_CDM_VOCAB.concept` dc on dc.concept_id = cr.concept_id_2
    WHERE c.Start_Date_Consultant_Episode IS NOT NULL
    AND c.person_id IS NOT NULL
    AND cr.relationship_id = 'Maps to'
    AND dc.standard_concept = 'S'
    AND dc.domain_id = 'Condition'
    AND sc.vocabulary_id = 'ICD10'
    AND c.Diagnosis_12th_Secondary_ICD IS NOT NULL
    AND dc.vocabulary_id = 'SNOMED'
    AND c.Diagnosis_12th_Secondary_ICD != c.Diagnosis_1st_Secondary_ICD
    AND c.Diagnosis_12th_Secondary_ICD != c.Diagnosis_2nd_Secondary_ICD
    AND c.Diagnosis_12th_Secondary_ICD != c.Diagnosis_3rd_Secondary_ICD
    AND c.Diagnosis_12th_Secondary_ICD != c.Diagnosis_4th_Secondary_ICD
    AND c.Diagnosis_12th_Secondary_ICD != c.Diagnosis_5th_Secondary_ICD
    AND c.Diagnosis_12th_Secondary_ICD != c.Diagnosis_6th_Secondary_ICD
    AND c.Diagnosis_12th_Secondary_ICD != c.Diagnosis_7th_Secondary_ICD
    AND c.Diagnosis_12th_Secondary_ICD != c.Diagnosis_8th_Secondary_ICD
    AND c.Diagnosis_12th_Secondary_ICD != c.Diagnosis_9th_Secondary_ICD
    AND c.Diagnosis_12th_Secondary_ICD != c.Diagnosis_10th_Secondary_ICD
    AND c.Diagnosis_12th_Secondary_ICD != c.Diagnosis_11th_Secondary_ICD
    GROUP BY c.Diagnosis_12th_Secondary_ICD;

    INSERT INTO CONDITON_OCCURRENCE_COUNTS
    SELECT
        SourceValue,
        SUM(SourceCount) SourceCount,
        0 CDMCount
    FROM SOURCE_COUNTS
    GROUP BY SourceValue;

    /* From CDM */
    UPDATE CONDITON_OCCURRENCE_COUNTS SET
        CDMCount = Cnt
    FROM 
    (
            SELECT 
                condition_source_value,
                Count(*) Cnt
            FROM `CY_IMOSPHERE_CDM_531.condition_occurrence` co
            JOIN CONDITON_OCCURRENCE_COUNTS c ON c.SourceValue = co.condition_source_value
            GROUP BY condition_source_value
    ) UpdCnt
    WHERE SourceValue = UpdCnt.condition_source_value;

    INSERT INTO CONDITON_OCCURRENCE_COUNTS
    SELECT
        condition_source_value,
        0 SourceCount,
        COUNT(*) CDMCount
    FROM `CY_IMOSPHERE_CDM_531.condition_occurrence` co
    WHERE NOT EXISTS (SELECT * FROM CONDITON_OCCURRENCE_COUNTS c WHERE c.SourceValue = co.condition_source_value)
    GROUP BY condition_source_value;
    
    SELECT 
        'Condition Occurrence' Type,
        SourceValue Id,
        SourceCount,
        CDMCount
    FROM CONDITON_OCCURRENCE_COUNTS 
    WHERE CDMCount != SourceCount;
END;

CREATE OR REPLACE PROCEDURE CY_IMOSPHERE_WORKSPACE.COUNT_PROCEDURE_OCCURRENCE()
OPTIONS (strict_mode=false)
BEGIN
    DROP TABLE IF EXISTS _SESSION.SOURCE_COUNTS;
    CREATE TEMP TABLE SOURCE_COUNTS (SourceValue STRING, SourceCount INT64);
    CREATE TEMP TABLE PROCEDURE_OCCURRENCE_COUNTS (SourceValue STRING, SourceCount INT64, CDMCount INT64);

    --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation_First]
    INSERT INTO SOURCE_COUNTS
    SELECT
        p.procedure_source_value SourceValue,
        Count(*) SourceCount
    FROM
    (
        SELECT * FROM 
        (
            SELECT DISTINCT
                Arrival_Date,
                p.person_id,
                Accident_And_Emergency_Investigation_First procedure_source_value,
                AandE_Attendance_Number,	
                PARSE_DATE('%Y%m%d', p.Arrival_Date) procedure_date,
                PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.Arrival_Date ,'000000')) procedure_datetime,
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` p
            JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` pc on pc.source_value = REPLACE(p.Accident_And_Emergency_Investigation_First, ' ', '') AND pc.destination_table = 'PROCEDURE_OCCURRENCE' AND pc.source_table = 'SUS_BRI_AE_010415_to_300619' AND pc.source_column = 'Accident_And_Emergency_Investigation_First'
            UNION DISTINCT
            SELECT DISTINCT
                Arrival_Date,
                p.person_id,
                Accident_And_Emergency_Investigation_Second procedure_source_value,
                AandE_Attendance_Number,	
                PARSE_DATE('%Y%m%d', p.Arrival_Date) procedure_date,
                PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.Arrival_Date ,'000000')) procedure_datetime,
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` p
            JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` pc on pc.source_value = REPLACE(p.Accident_And_Emergency_Investigation_Second, ' ', '') AND pc.destination_table = 'PROCEDURE_OCCURRENCE' AND pc.source_table = 'SUS_BRI_AE_010415_to_300619' AND pc.source_column = 'Accident_And_Emergency_Investigation_Second'
            UNION DISTINCT
            SELECT DISTINCT
                Arrival_Date,
                p.person_id,
                Accident_And_Emergency_Investigation_Third procedure_source_value,
                AandE_Attendance_Number,	
                PARSE_DATE('%Y%m%d', p.Arrival_Date) procedure_date,
                PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.Arrival_Date ,'000000')) procedure_datetime,
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` p
            JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` pc on pc.source_value = REPLACE(p.Accident_And_Emergency_Investigation_Third, ' ', '') AND pc.destination_table = 'PROCEDURE_OCCURRENCE' AND pc.source_table = 'SUS_BRI_AE_010415_to_300619' AND pc.source_column = 'Accident_And_Emergency_Investigation_Third'
            UNION DISTINCT
            SELECT DISTINCT
                Arrival_Date,
                p.person_id,
                Accident_And_Emergency_Investigation_Fourth procedure_source_value,
                AandE_Attendance_Number,	
                PARSE_DATE('%Y%m%d', p.Arrival_Date) procedure_date,
                PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.Arrival_Date ,'000000')) procedure_datetime,
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` p
            JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` pc on pc.source_value = REPLACE(p.Accident_And_Emergency_Investigation_Fourth, ' ', '') AND pc.destination_table = 'PROCEDURE_OCCURRENCE' AND pc.source_table = 'SUS_BRI_AE_010415_to_300619' AND pc.source_column = 'Accident_And_Emergency_Investigation_Fourth'
            UNION DISTINCT
            SELECT DISTINCT
                Arrival_Date,
                p.person_id,
                Accident_And_Emergency_Investigation_Fifth procedure_source_value,
                AandE_Attendance_Number,	
                PARSE_DATE('%Y%m%d', p.Arrival_Date) procedure_date,
                PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.Arrival_Date ,'000000')) procedure_datetime,
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` p
            JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` pc on pc.source_value = REPLACE(p.Accident_And_Emergency_Investigation_Fifth, ' ', '') AND pc.destination_table = 'PROCEDURE_OCCURRENCE' AND pc.source_table = 'SUS_BRI_AE_010415_to_300619' AND pc.source_column = 'Accident_And_Emergency_Investigation_Fifth'
            UNION DISTINCT
            SELECT DISTINCT
                Arrival_Date,
                p.person_id,
                Accident_And_Emergency_Investigation_Sixth procedure_source_value,
                AandE_Attendance_Number,	
                PARSE_DATE('%Y%m%d', p.Arrival_Date) procedure_date,
                PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.Arrival_Date ,'000000')) procedure_datetime,
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` p
            JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` pc on pc.source_value = REPLACE(p.Accident_And_Emergency_Investigation_Sixth, ' ', '') AND pc.destination_table = 'PROCEDURE_OCCURRENCE' AND pc.source_table = 'SUS_BRI_AE_010415_to_300619' AND pc.source_column = 'Accident_And_Emergency_Investigation_Sixth'
            UNION DISTINCT
            SELECT DISTINCT
                Arrival_Date,
                p.person_id,
                Accident_And_Emergency_Investigation_Seventh procedure_source_value,
                AandE_Attendance_Number,	
                PARSE_DATE('%Y%m%d', p.Arrival_Date) procedure_date,
                PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.Arrival_Date ,'000000')) procedure_datetime,
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` p
            JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` pc on pc.source_value = REPLACE(p.Accident_And_Emergency_Investigation_Seventh, ' ', '') AND pc.destination_table = 'PROCEDURE_OCCURRENCE' AND pc.source_table = 'SUS_BRI_AE_010415_to_300619' AND pc.source_column = 'Accident_And_Emergency_Investigation_Seventh'
            UNION DISTINCT
            SELECT DISTINCT
                Arrival_Date,
                p.person_id,
                Accident_And_Emergency_Investigation_Eighth procedure_source_value,
                AandE_Attendance_Number,	
                PARSE_DATE('%Y%m%d', p.Arrival_Date) procedure_date,
                PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.Arrival_Date ,'000000')) procedure_datetime,
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` p
            JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` pc on pc.source_value = REPLACE(p.Accident_And_Emergency_Investigation_Eighth, ' ', '') AND pc.destination_table = 'PROCEDURE_OCCURRENCE' AND pc.source_table = 'SUS_BRI_AE_010415_to_300619' AND pc.source_column = 'Accident_And_Emergency_Investigation_Eighth'
            UNION DISTINCT
            SELECT DISTINCT
                Arrival_Date,
                p.person_id,
                Accident_And_Emergency_Investigation_Ninth procedure_source_value,
                AandE_Attendance_Number,	
                PARSE_DATE('%Y%m%d', p.Arrival_Date) procedure_date,
                PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.Arrival_Date ,'000000')) procedure_datetime,
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` p
            JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` pc on pc.source_value = REPLACE(p.Accident_And_Emergency_Investigation_Ninth, ' ', '') AND pc.destination_table = 'PROCEDURE_OCCURRENCE' AND pc.source_table = 'SUS_BRI_AE_010415_to_300619' AND pc.source_column = 'Accident_And_Emergency_Investigation_Ninth'
            UNION DISTINCT
            SELECT DISTINCT
                Arrival_Date,
                p.person_id,
                Accident_And_Emergency_Investigation_Tenth procedure_source_value,
                AandE_Attendance_Number,	
                PARSE_DATE('%Y%m%d', p.Arrival_Date) procedure_date,
                PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.Arrival_Date ,'000000')) procedure_datetime,
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` p
            JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` pc on pc.source_value = REPLACE(p.Accident_And_Emergency_Investigation_Tenth, ' ', '') AND pc.destination_table = 'PROCEDURE_OCCURRENCE' AND pc.source_table = 'SUS_BRI_AE_010415_to_300619' AND pc.source_column = 'Accident_And_Emergency_Investigation_Tenth'
            UNION DISTINCT
            SELECT DISTINCT
                Arrival_Date,
                p.person_id,
                Accident_And_Emergency_Investigation_Eleventh procedure_source_value,
                AandE_Attendance_Number,	
                PARSE_DATE('%Y%m%d', p.Arrival_Date) procedure_date,
                PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.Arrival_Date ,'000000')) procedure_datetime,
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` p
            JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` pc on pc.source_value = REPLACE(p.Accident_And_Emergency_Investigation_Eleventh, ' ', '') AND pc.destination_table = 'PROCEDURE_OCCURRENCE' AND pc.source_table = 'SUS_BRI_AE_010415_to_300619' AND pc.source_column = 'Accident_And_Emergency_Investigation_Eleventh'
            UNION DISTINCT
            SELECT DISTINCT
                Arrival_Date,
                p.person_id,
                Accident_And_Emergency_Investigation_Twelfth procedure_source_value,
                AandE_Attendance_Number,	
                PARSE_DATE('%Y%m%d', p.Arrival_Date) procedure_date,
                PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.Arrival_Date ,'000000')) procedure_datetime,
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` p
            JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` pc on pc.source_value = REPLACE(p.Accident_And_Emergency_Investigation_Twelfth, ' ', '') AND pc.destination_table = 'PROCEDURE_OCCURRENCE' AND pc.source_table = 'SUS_BRI_AE_010415_to_300619' AND pc.source_column = 'Accident_And_Emergency_Investigation_Twelfth'
        ) p
        WHERE p.Arrival_Date IS NOT NULL
        AND p.person_id IS NOT NULL
        AND procedure_source_value IS NOT NULL
    ) p
    GROUP BY p.procedure_source_value;

    --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment]
    INSERT INTO SOURCE_COUNTS
    SELECT
        p.procedure_source_value SourceValue,
        Count(*) SourceCount
    FROM
    (
        SELECT * FROM 
        (
            SELECT DISTINCT
                Arrival_Date,
                p.person_id,
                Accident_And_Emergency_Treatment_First procedure_source_value,
                AandE_Attendance_Number,	
                PARSE_DATE('%Y%m%d', p.Arrival_Date) procedure_date,
                PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.Arrival_Date ,'000000')) procedure_datetime,
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` p
            JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` pc on pc.source_value = REPLACE(p.Accident_And_Emergency_Treatment_First, ' ', '') AND pc.destination_table = 'PROCEDURE_OCCURRENCE' AND pc.source_table = 'SUS_BRI_AE_010415_to_300619' AND pc.source_column = 'Accident_And_Emergency_Treatment_First'
            UNION DISTINCT
            SELECT DISTINCT
                Arrival_Date,
                p.person_id,
                Accident_And_Emergency_Treatment_Second procedure_source_value,
                AandE_Attendance_Number,	
                PARSE_DATE('%Y%m%d', p.Arrival_Date) procedure_date,
                PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.Arrival_Date ,'000000')) procedure_datetime,
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` p
            JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` pc on pc.source_value = REPLACE(p.Accident_And_Emergency_Treatment_Second, ' ', '') AND pc.destination_table = 'PROCEDURE_OCCURRENCE' AND pc.source_table = 'SUS_BRI_AE_010415_to_300619' AND pc.source_column = 'Accident_And_Emergency_Treatment_Second'
            UNION DISTINCT
            SELECT DISTINCT
                Arrival_Date,
                p.person_id,
                Accident_And_Emergency_Treatment_Third procedure_source_value,
                AandE_Attendance_Number,	
                PARSE_DATE('%Y%m%d', p.Arrival_Date) procedure_date,
                PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.Arrival_Date ,'000000')) procedure_datetime,
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` p
            JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` pc on pc.source_value = REPLACE(p.Accident_And_Emergency_Treatment_Third, ' ', '') AND pc.destination_table = 'PROCEDURE_OCCURRENCE' AND pc.source_table = 'SUS_BRI_AE_010415_to_300619' AND pc.source_column = 'Accident_And_Emergency_Treatment_Third'
            UNION DISTINCT
            SELECT DISTINCT
                Arrival_Date,
                p.person_id,
                Accident_And_Emergency_Treatment_Fourth procedure_source_value,
                AandE_Attendance_Number,	
                PARSE_DATE('%Y%m%d', p.Arrival_Date) procedure_date,
                PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.Arrival_Date ,'000000')) procedure_datetime,
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` p
            JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` pc on pc.source_value = REPLACE(p.Accident_And_Emergency_Treatment_Fourth, ' ', '') AND pc.destination_table = 'PROCEDURE_OCCURRENCE' AND pc.source_table = 'SUS_BRI_AE_010415_to_300619' AND pc.source_column = 'Accident_And_Emergency_Treatment_Fourth'
            UNION DISTINCT
            SELECT DISTINCT
                Arrival_Date,
                p.person_id,
                Accident_And_Emergency_Treatment_Fifth procedure_source_value,
                AandE_Attendance_Number,	
                PARSE_DATE('%Y%m%d', p.Arrival_Date) procedure_date,
                PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.Arrival_Date ,'000000')) procedure_datetime,
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` p
            JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` pc on pc.source_value = REPLACE(p.Accident_And_Emergency_Treatment_Fifth, ' ', '') AND pc.destination_table = 'PROCEDURE_OCCURRENCE' AND pc.source_table = 'SUS_BRI_AE_010415_to_300619' AND pc.source_column = 'Accident_And_Emergency_Treatment_Fifth'
            UNION DISTINCT
            SELECT DISTINCT
                Arrival_Date,
                p.person_id,
                Accident_And_Emergency_Treatment_Sixth procedure_source_value,
                AandE_Attendance_Number,	
                PARSE_DATE('%Y%m%d', p.Arrival_Date) procedure_date,
                PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.Arrival_Date ,'000000')) procedure_datetime,
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` p
            JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` pc on pc.source_value = REPLACE(p.Accident_And_Emergency_Treatment_Sixth, ' ', '') AND pc.destination_table = 'PROCEDURE_OCCURRENCE' AND pc.source_table = 'SUS_BRI_AE_010415_to_300619' AND pc.source_column = 'Accident_And_Emergency_Treatment_Sixth'
            UNION DISTINCT
            SELECT DISTINCT
                Arrival_Date,
                p.person_id,
                Accident_And_Emergency_Treatment_Seventh procedure_source_value,
                AandE_Attendance_Number,	
                PARSE_DATE('%Y%m%d', p.Arrival_Date) procedure_date,
                PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.Arrival_Date ,'000000')) procedure_datetime,
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` p
            JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` pc on pc.source_value = REPLACE(p.Accident_And_Emergency_Treatment_Seventh, ' ', '') AND pc.destination_table = 'PROCEDURE_OCCURRENCE' AND pc.source_table = 'SUS_BRI_AE_010415_to_300619' AND pc.source_column = 'Accident_And_Emergency_Treatment_Seventh'
            UNION DISTINCT
            SELECT DISTINCT
                Arrival_Date,
                p.person_id,
                Accident_And_Emergency_Treatment_Eighth procedure_source_value,
                AandE_Attendance_Number,	
                PARSE_DATE('%Y%m%d', p.Arrival_Date) procedure_date,
                PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.Arrival_Date ,'000000')) procedure_datetime,
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` p
            JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` pc on pc.source_value = REPLACE(p.Accident_And_Emergency_Treatment_Eighth, ' ', '') AND pc.destination_table = 'PROCEDURE_OCCURRENCE' AND pc.source_table = 'SUS_BRI_AE_010415_to_300619' AND pc.source_column = 'Accident_And_Emergency_Treatment_Eighth'
            UNION DISTINCT
            SELECT DISTINCT
                Arrival_Date,
                p.person_id,
                Accident_And_Emergency_Treatment_Ninth procedure_source_value,
                AandE_Attendance_Number,	
                PARSE_DATE('%Y%m%d', p.Arrival_Date) procedure_date,
                PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.Arrival_Date ,'000000')) procedure_datetime,
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` p
            JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` pc on pc.source_value = REPLACE(p.Accident_And_Emergency_Treatment_Ninth, ' ', '') AND pc.destination_table = 'PROCEDURE_OCCURRENCE' AND pc.source_table = 'SUS_BRI_AE_010415_to_300619' AND pc.source_column = 'Accident_And_Emergency_Treatment_Ninth'
            UNION DISTINCT
            SELECT DISTINCT
                Arrival_Date,
                p.person_id,
                Accident_And_Emergency_Treatment_Tenth procedure_source_value,
                AandE_Attendance_Number,	
                PARSE_DATE('%Y%m%d', p.Arrival_Date) procedure_date,
                PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.Arrival_Date ,'000000')) procedure_datetime,
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` p
            JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` pc on pc.source_value = REPLACE(p.Accident_And_Emergency_Treatment_Tenth, ' ', '') AND pc.destination_table = 'PROCEDURE_OCCURRENCE' AND pc.source_table = 'SUS_BRI_AE_010415_to_300619' AND pc.source_column = 'Accident_And_Emergency_Treatment_Tenth'
            UNION DISTINCT
            SELECT DISTINCT
                Arrival_Date,
                p.person_id,
                Accident_And_Emergency_Treatment_Eleventh procedure_source_value,
                AandE_Attendance_Number,	
                PARSE_DATE('%Y%m%d', p.Arrival_Date) procedure_date,
                PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.Arrival_Date ,'000000')) procedure_datetime,
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` p
            JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` pc on pc.source_value = REPLACE(p.Accident_And_Emergency_Treatment_Eleventh, ' ', '') AND pc.destination_table = 'PROCEDURE_OCCURRENCE' AND pc.source_table = 'SUS_BRI_AE_010415_to_300619' AND pc.source_column = 'Accident_And_Emergency_Treatment_Eleventh'
            UNION DISTINCT
            SELECT DISTINCT
                Arrival_Date,
                p.person_id,
                Accident_And_Emergency_Treatment_Twelfth procedure_source_value,
                AandE_Attendance_Number,	
                PARSE_DATE('%Y%m%d', p.Arrival_Date) procedure_date,
                PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(p.Arrival_Date ,'000000')) procedure_datetime,
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` p
            JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` pc on pc.source_value = REPLACE(p.Accident_And_Emergency_Treatment_Twelfth, ' ', '') AND pc.destination_table = 'PROCEDURE_OCCURRENCE' AND pc.source_table = 'SUS_BRI_AE_010415_to_300619' AND pc.source_column = 'Accident_And_Emergency_Treatment_Twelfth'
        ) p
        WHERE p.Arrival_Date IS NOT NULL
        AND p.person_id IS NOT NULL
        AND procedure_source_value IS NOT NULL
    ) p
    GROUP BY p.procedure_source_value;
    
    --[SUS_BRI_APC_010415_to_300619_P1 - Procedure_OPCS]
    INSERT INTO SOURCE_COUNTS
    SELECT
        p.procedure_source_value SourceValue,
        Count(*) SourceCount
    FROM
    (
        SELECT
            p.*,
            vl.id visit_occurrence_id,
            vdl.id visit_detail_id
        FROM 
        (
            SELECT DISTINCT
                p.person_id person_id,
                p.Primary_Procedure_Date_OPCS procedure_date,
                p.Primary_Procedure_OPCS procedure_source_value,  
                p.Hospital_Provider_Spell_Number,
                p.Episode_Number
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1` p   
            UNION DISTINCT
            SELECT DISTINCT
                p.person_id person_id,
                p.a2nd_Procedure_Date_OPCS procedure_date,
                p.a2nd_Procedure_OPCS procedure_source_value,  
                p.Hospital_Provider_Spell_Number,
                p.Episode_Number
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1` p
            UNION DISTINCT
            SELECT DISTINCT
                p.person_id person_id,
                p.a3rd_Procedure_Date_OPCS procedure_date,
                p.a3rd_Procedure_OPCS procedure_source_value,  
                p.Hospital_Provider_Spell_Number,
                p.Episode_Number
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1` p 
            UNION DISTINCT
            SELECT DISTINCT
                p.person_id person_id,
                p.a4th_Procedure_Date_OPCS procedure_date,
                p.a4th_Procedure_OPCS procedure_source_value,  
                p.Hospital_Provider_Spell_Number,
                p.Episode_Number
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1` p 
            UNION DISTINCT
            SELECT DISTINCT
                p.person_id person_id,
                p.a5th_Procedure_Date_OPCS procedure_date,
                p.a5th_Procedure_OPCS procedure_source_value,  
                p.Hospital_Provider_Spell_Number,
                p.Episode_Number
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1` p
            UNION DISTINCT
            SELECT DISTINCT
                p.person_id person_id,
                p.a6th_Procedure_Date_OPCS procedure_date,
                p.a6th_Procedure_OPCS procedure_source_value,  
                p.Hospital_Provider_Spell_Number,
                p.Episode_Number
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1` p
            UNION DISTINCT
            SELECT DISTINCT
                p.person_id person_id,
                p.a7th_Procedure_Date_OPCS procedure_date,
                p.a7th_Procedure_OPCS procedure_source_value,  
                p.Hospital_Provider_Spell_Number,
                p.Episode_Number
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1` p
            UNION DISTINCT
            SELECT DISTINCT
                p.person_id person_id,
                p.a8th_Procedure_Date_OPCS procedure_date,
                p.a8th_Procedure_OPCS procedure_source_value,  
                p.Hospital_Provider_Spell_Number,
                p.Episode_Number
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1` p
            UNION DISTINCT
            SELECT DISTINCT
                p.person_id person_id,
                p.a9th_Procedure_Date_OPCS procedure_date,
                p.a9th_Procedure_OPCS procedure_source_value,  
                p.Hospital_Provider_Spell_Number,
                p.Episode_Number
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1` p
            UNION DISTINCT
            SELECT DISTINCT
                p.person_id person_id,
                p.a10th_Procedure_Date_OPCS procedure_date,
                p.a10th_Procedure_OPCS procedure_source_value,  
                p.Hospital_Provider_Spell_Number,
                p.Episode_Number
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1` p
            UNION DISTINCT
            SELECT DISTINCT
                p.person_id person_id,
                p.a11th_Procedure_Date_OPCS procedure_date,
                p.a11th_Procedure_OPCS procedure_source_value,  
                p.Hospital_Provider_Spell_Number,
                p.Episode_Number
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1` p
            UNION DISTINCT
            SELECT DISTINCT
                p.person_id person_id,
                p.a12th_Procedure_Date_OPCS procedure_date,
                p.a12th_Procedure_OPCS procedure_source_value,  
                p.Hospital_Provider_Spell_Number,
                p.Episode_Number
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1` p
            UNION DISTINCT
            SELECT DISTINCT
                p.person_id person_id,
                p.a13th_Procedure_Date_OPCS procedure_date,
                p.a13th_Procedure_OPCS procedure_source_value,  
                p.Hospital_Provider_Spell_Number,
                p.Episode_Number
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1` p
            UNION DISTINCT
            SELECT DISTINCT
                p.person_id person_id,
                p.a14th_Procedure_Date_OPCS procedure_date,
                p.a14th_Procedure_OPCS procedure_source_value,  
                p.Hospital_Provider_Spell_Number,
                p.Episode_Number
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1` p
            UNION DISTINCT
            SELECT DISTINCT
                p.person_id person_id,
                p.a15th_Procedure_Date_OPCS procedure_date,
                p.a15th_Procedure_OPCS procedure_source_value,  
                p.Hospital_Provider_Spell_Number,
                p.Episode_Number
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1` p
        ) p
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = CONCAT(CAST(p.Hospital_Provider_Spell_Number as STRING),'_', p.person_id)
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups`) vdl On vdl.source_value = CONCAT(CAST(p.Hospital_Provider_Spell_Number as STRING),'_', p.person_id, '_', p.Episode_Number)
        JOIN `CY_CDM_VOCAB.concept` sc on REPLACE(sc.concept_code, '.', '') = p.procedure_source_value
        JOIN `CY_CDM_VOCAB.concept_relationship` cr on cr.concept_id_1 = sc.concept_id
        JOIN `CY_CDM_VOCAB.concept` dc on dc.concept_id = cr.concept_id_2    
        WHERE (p.procedure_date IS NOT NULL AND procedure_date NOT LIKE '% %' AND (LENGTH(procedure_date) =6 OR LENGTH(procedure_date) =8))
        AND p.person_id IS NOT NULL
        AND p.procedure_source_value IS NOT NULL
        AND cr.relationship_id = 'Maps to'
        AND sc.vocabulary_id = 'OPCS4'
        AND dc.vocabulary_id = 'SNOMED'
    ) p
    GROUP BY p.procedure_source_value;

    --[SUS_BRI_OP_010415_to_300619 - Procedure_OPCS]
    INSERT INTO SOURCE_COUNTS
    SELECT
        p.procedure_source_value SourceValue,
        Count(*) SourceCount
    FROM
    (
        SELECT
            p.*,
            vl.id visit_occurrence_id
        FROM 
        (
            SELECT DISTINCT
                p.person_id person_id,
                p.Appointment_Date procedure_date,
                p.Primary_Procedure_OPCS procedure_source_value,  
                p.Generated_Record_Identifier
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_OP_010415_to_300619` p
            UNION DISTINCT
            SELECT DISTINCT
                p.person_id person_id,
                p.Appointment_Date procedure_date,
                p.A2nd_Procedure_OPCS procedure_source_value,  
                p.Generated_Record_Identifier
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_OP_010415_to_300619` p
            UNION DISTINCT
            SELECT DISTINCT
                p.person_id person_id,
                p.Appointment_Date procedure_date,
                p.A3rd_Procedure_OPCS procedure_source_value,  
                p.Generated_Record_Identifier
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_OP_010415_to_300619` p
            UNION DISTINCT
            SELECT DISTINCT
                p.person_id person_id,
                p.Appointment_Date procedure_date,
                p.A4th_Procedure_OPCS procedure_source_value,  
                p.Generated_Record_Identifier
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_OP_010415_to_300619` p
            UNION DISTINCT
            SELECT DISTINCT
                p.person_id person_id,
                p.Appointment_Date procedure_date,
                p.A5th_Procedure_OPCS procedure_source_value,  
                p.Generated_Record_Identifier
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_OP_010415_to_300619` p
            UNION DISTINCT
            SELECT DISTINCT
                p.person_id person_id,
                p.Appointment_Date procedure_date,
                p.A6th_Procedure_OPCS procedure_source_value,  
                p.Generated_Record_Identifier
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_OP_010415_to_300619` p
        ) p
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = CONCAT(CAST(p.Generated_Record_Identifier as STRING),'_', p.person_id)
        JOIN `CY_CDM_VOCAB.concept` sc on REPLACE(sc.concept_code, '.', '') = REPLACE(p.procedure_source_value, ' ', '')
        JOIN `CY_CDM_VOCAB.concept_relationship` cr on cr.concept_id_1 = sc.concept_id
        JOIN `CY_CDM_VOCAB.concept` dc on dc.concept_id = cr.concept_id_2    
        WHERE p.procedure_date IS NOT NULL
        AND p.person_id IS NOT NULL
        AND (p.procedure_source_value IS NOT NULL AND p.procedure_source_value != '     ' )
        AND cr.relationship_id = 'Maps to'
        AND sc.vocabulary_id = 'OPCS4'
        AND dc.vocabulary_id = 'SNOMED'
    ) p
    GROUP BY p.procedure_source_value;
    
    --[tbl_SRCode]
    INSERT INTO SOURCE_COUNTS
    SELECT
        p.procedure_source_value SourceValue,
        Count(*) SourceCount
    FROM
    (
        SELECT DISTINCT
            p.person_id person_id,
            dc.concept_id procedure_concept_id, 
            p.ctv3code procedure_source_value, 
            sc.concept_id procedure_source_concept_id,
            dateevent procedure_date,
            dateevent procedure_datetime,
            pr.provider_id provider_id
        FROM `CY_IMOSPHERE_WORKSPACE.tbl_SRCode` p
        JOIN `CY_LOOKUPS.tbl_CTV3ToSnomed_Map` sm ON sm.ctv3code = p.ctv3code
        JOIN `CY_CDM_VOCAB.concept` sc ON sc.concept_code = sm.SNOMEDCode
        JOIN `CY_CDM_VOCAB.concept_relationship` cr ON cr.concept_id_1 = sc.concept_id
        JOIN `CY_CDM_VOCAB.concept` dc ON dc.concept_id = cr.concept_id_2
        LEFT JOIN `CY_IMOSPHERE_CDM_531.provider` pr ON pr.provider_id = CAST(iddoneby AS INT64)
        WHERE sc.vocabulary_id = 'SNOMED'
        AND cr.relationship_id = 'Maps to'
        AND dc.standard_concept = 'S'
        AND dc.domain_id = 'Procedure'
        AND p.dateevent IS NOT NULL
        AND p.person_id IS NOT NULL
    ) p
    GROUP BY p.procedure_source_value;

    INSERT INTO PROCEDURE_OCCURRENCE_COUNTS
    SELECT
        SourceValue,
        SUM(SourceCount) SourceCount,
        0 CDMCount
    FROM SOURCE_COUNTS
    GROUP BY SourceValue;

    /* From CDM */
    UPDATE PROCEDURE_OCCURRENCE_COUNTS SET
        CDMCount = Cnt
    FROM 
    (
            SELECT 
                procedure_source_value,
                Count(*) Cnt
            FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` po
            JOIN PROCEDURE_OCCURRENCE_COUNTS c ON c.SourceValue = po.procedure_source_value
            GROUP BY procedure_source_value
    ) UpdCnt
    WHERE SourceValue = UpdCnt.procedure_source_value;

    INSERT INTO PROCEDURE_OCCURRENCE_COUNTS
    SELECT
        procedure_source_value,
        0 SourceCount,
        COUNT(*) CDMCount
    FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` po
    WHERE NOT EXISTS (SELECT * FROM PROCEDURE_OCCURRENCE_COUNTS c WHERE c.SourceValue = po.procedure_source_value)
    GROUP BY procedure_source_value;
    
    SELECT 
        'Procedure Occurrence' Type,
        SourceValue Id,
        SourceCount,
        CDMCount
    FROM PROCEDURE_OCCURRENCE_COUNTS 
    WHERE CDMCount != SourceCount;
END;

CREATE OR REPLACE PROCEDURE CY_IMOSPHERE_WORKSPACE.COUNT_OBSERVATION()
OPTIONS (strict_mode=false)
BEGIN
    DROP TABLE IF EXISTS _SESSION.SOURCE_COUNTS;
    CREATE TEMP TABLE SOURCE_COUNTS (SourceValue STRING, SourceCount INT64);
    CREATE TEMP TABLE OBSERVATION_COUNTS (SourceValue STRING, SourceCount INT64, CDMCount INT64);
    
    --[tbl_SRVisit]
    INSERT INTO SOURCE_COUNTS
    SELECT
        o.observation_source_value SourceValue,
        Count(*) SourceCount
    FROM
    (        
        SELECT DISTINCT
            o.person_id person_id,
            DateBooked observation_datetime,
            Duration value_as_number,
            vl.id visit_occurrence_id,
            ARRAY_TO_STRING(['Visit Duration', '8550', Duration, NULL, NULL], '_') observation_source_value
        FROM `CY_IMOSPHERE_WORKSPACE.tbl_SRVisit` o
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'tbl_SRVisit') vl ON vl.source_value = CONCAT(CAST(o.RowIdentifier as STRING),'_', o.person_id)
        WHERE o.DateBooked IS NOT NULL
        AND o.person_id IS NOT NULL
    ) o
    GROUP BY o.observation_source_value;
        
    --[tbl_SRReferralIn - PrimaryReason]    
    INSERT INTO SOURCE_COUNTS
    SELECT
        o.observation_source_value SourceValue,
        Count(*) SourceCount
    FROM
    (        
        SELECT DISTINCT
            o.person_id person_id,
            datereferral observation_datetime,
            vl.id visit_occurrence_id,
            ARRAY_TO_STRING([PrimaryReason, CAST(value.concept_id AS STRING), NULL, 'Reason for', NULL], '_') observation_source_value
        FROM `CY_IMOSPHERE_WORKSPACE.tbl_SRReferralIn` o
        LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_SRVisit`v ON v.idreferralin = o.RowIdentifier AND v.person_id = o.person_id
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'tbl_SRVisit') vl ON vl.source_value = CONCAT(CAST(v.RowIdentifier as STRING),'_', v.person_id)
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` value on value.source_value = o.PrimaryReason AND value.destination_table = 'OBSERVATION' AND value.source_table = 'tbl_SRReferralIn' AND value.source_column = 'PrimaryReason' 
        WHERE o.datereferral IS NOT NULL
        AND o.person_id IS NOT NULL
    ) o
    GROUP BY o.observation_source_value;

    --[tbl_SRReferralIn - Source]
    INSERT INTO SOURCE_COUNTS
    SELECT
        o.observation_source_value SourceValue,
        Count(*) SourceCount
    FROM
    (        
        SELECT DISTINCT
            o.person_id person_id,
            datereferral observation_datetime,
            vl.id visit_occurrence_id,
            ARRAY_TO_STRING([Source, CAST(value.concept_id AS STRING), NULL, NULL, NULL], '_') observation_source_value
        FROM `CY_IMOSPHERE_WORKSPACE.tbl_SRReferralIn` o
        LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_SRVisit`v ON v.idreferralin = o.RowIdentifier AND v.person_id = o.person_id
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'tbl_SRVisit') vl ON vl.source_value = CONCAT(CAST(v.RowIdentifier as STRING),'_', v.person_id)
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` value on value.source_value = o.Source AND value.destination_table = 'OBSERVATION' AND value.source_table = 'tbl_SRReferralIn' AND value.source_column = 'Source' 
        WHERE o.datereferral IS NOT NULL
        AND o.person_id IS NOT NULL
    ) o
    GROUP BY o.observation_source_value;

    --[tbl_SRPatient]
    INSERT INTO SOURCE_COUNTS
    SELECT
        o.observation_source_value SourceValue,
        Count(*) SourceCount
    FROM
    (        
        SELECT DISTINCT
            o.person_id person_id,
            o.ethnicity,
            o.DateBirth observation_datetime,
            ARRAY_TO_STRING([o.ethnicity, CAST(oc.concept_id AS STRING), NULL, NULL, NULL], '_') observation_source_value
        FROM `CY_IMOSPHERE_WORKSPACE.tbl_SRPatient` o
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on oc.source_value = o.ethnicity AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'tbl_SRPatient' AND oc.source_column = 'ethnicity' 
        WHERE o.DateBirth IS NOT NULL
        AND o.person_id IS NOT NULL
    ) o
    GROUP BY o.observation_source_value;
    
    --[tbl_SRCode]
    INSERT INTO SOURCE_COUNTS
    SELECT
        o.observation_source_value SourceValue,
        Count(*) SourceCount
    FROM
    (        
        SELECT DISTINCT
            o.person_id person_id,
            o.ctv3code,
            dateevent observation_datetime,
            p.provider_id,
            ARRAY_TO_STRING([o.ctv3code, NULL, CAST(CAST(numericvalue AS FLOAT64) AS STRING), NULL, numericunit], '_') observation_source_value
        FROM `CY_IMOSPHERE_WORKSPACE.tbl_SRCode` o
        JOIN `CY_LOOKUPS.tbl_CTV3ToSnomed_Map` sm ON sm.ctv3code = o.ctv3code
        JOIN `CY_CDM_VOCAB.concept` sc ON sc.concept_code = sm.SNOMEDCode
        JOIN `CY_CDM_VOCAB.concept_relationship` cr ON cr.concept_id_1 = sc.concept_id
        JOIN `CY_CDM_VOCAB.concept` dc ON dc.concept_id = cr.concept_id_2
        LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` unit on unit.source_value = o.numericunit AND unit.destination_table = 'OBSERVATION' AND unit.source_table = 'tbl_SRCode' AND unit.source_column = 'numericunit'
        LEFT JOIN `CY_IMOSPHERE_CDM_531.provider` p ON p.provider_id = CAST(iddoneby AS INT64) 
        WHERE sc.vocabulary_id = 'SNOMED'
        AND cr.relationship_id = 'Maps to'
        AND dc.standard_concept = 'S'
        AND dc.domain_id NOT IN ('Condition', 'Procedure', 'Drug', 'Measurement', 'Device')
        AND o.dateevent IS NOT NULL
        AND o.person_id IS NOT NULL
    ) o
    GROUP BY o.observation_source_value;
    
    --[SUS_BRI_OP_010415_to_300619 - Ethnic_Category]
    INSERT INTO SOURCE_COUNTS
    SELECT
        o.observation_source_value SourceValue,
        Count(*) SourceCount
    FROM
    (        
        SELECT DISTINCT
            o.person_id person_id,
            o.Ethnic_Category,
            o.Date_of_Birth observation_datetime,
            ARRAY_TO_STRING([o.Ethnic_Category, CAST(oc.concept_id AS STRING), NULL, NULL, NULL], '_') observation_source_value
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_OP_010415_to_300619` o
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on oc.source_value = o.Ethnic_Category AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_OP_010415_to_300619' AND oc.source_column = 'Ethnic_Category'
        WHERE (o.Date_of_Birth IS NOT NULL AND o.Date_of_Birth NOT LIKE '% %')
        AND o.person_id IS NOT NULL
    ) o
    GROUP BY o.observation_source_value;

    --[SUS_BRI_OP_010415_to_300619 - Attended_Or_Did_Not_Attend]
    INSERT INTO SOURCE_COUNTS
    SELECT
        o.observation_source_value SourceValue,
        Count(*) SourceCount
    FROM
    (        
        SELECT DISTINCT
            o.person_id person_id,
            o.Attended_Or_Did_Not_Attend,
            CASE WHEN Appointment_Time LIKE '% %' OR Appointment_Time IS NULL
                THEN PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(Appointment_Date, '000000'))
                ELSE PARSE_DATETIME('%Y%m%d%H:%M%S', CONCAT(Appointment_Date, Appointment_Time, '00'))
            END observation_datetime,
            ARRAY_TO_STRING([o.Attended_Or_Did_Not_Attend, CAST(oc.concept_id AS STRING), NULL, NULL, NULL], '_') observation_source_value
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_OP_010415_to_300619` o
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on oc.source_value = o.Attended_Or_Did_Not_Attend AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_OP_010415_to_300619' AND oc.source_column = 'Attended_Or_Did_Not_Attend'
        WHERE o.Appointment_Date IS NOT NULL
        AND o.person_id IS NOT NULL
    ) o
    GROUP BY o.observation_source_value;
    
    --[SUS_BRI_OP_010415_to_300619 - Source_Of_Referral_For_Outpatients]
    INSERT INTO SOURCE_COUNTS
    SELECT
        o.observation_source_value SourceValue,
        Count(*) SourceCount
    FROM
    (        
        SELECT DISTINCT
            o.person_id person_id,
            o.Source_Of_Referral_For_Outpatients,
            PARSE_DATE('%Y%m%d', o.Referral_Request_Received_Date) observation_date,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(o.Referral_Request_Received_Date ,'000000')) observation_datetime,
            ARRAY_TO_STRING([o.Source_Of_Referral_For_Outpatients, CAST(oc.concept_id AS STRING), NULL, NULL, NULL], '_') observation_source_value
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_OP_010415_to_300619` o
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on oc.source_value = o.Source_Of_Referral_For_Outpatients AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_OP_010415_to_300619' AND oc.source_column = 'Source_Of_Referral_For_Outpatients'
        WHERE (o.Referral_Request_Received_Date IS NOT NULL AND o.Referral_Request_Received_Date NOT LIKE '% %')
        AND o.person_id IS NOT NULL
    ) o
    GROUP BY o.observation_source_value;

    --[SUS_BRI_APC_010415_to_300619_P1 - Ethnic_Category]
    INSERT INTO SOURCE_COUNTS
    SELECT
        o.observation_source_value SourceValue,
        Count(*) SourceCount
    FROM
    (        
        SELECT DISTINCT
            o.person_id person_id,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(o.Date_of_Birth ,'01000000')) observation_datetime,
            vl.id visit_occurrence_id,
            vdl.id visit_detail_id,
            ARRAY_TO_STRING([o.Ethnic_Group, CAST(oc.concept_id AS STRING), NULL, NULL, NULL], '_') observation_source_value
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1` o
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on oc.source_value = o.Ethnic_Group AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_APC_010415_to_300619_P1' AND oc.source_column = 'Ethnic_Group'
        LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups` vdl ON vdl.source_value = CONCAT(CAST(o.Hospital_Provider_Spell_Number as STRING),'_', o.person_id, '_', o.Episode_Number)
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = CONCAT(CAST(o.Hospital_Provider_Spell_Number as STRING),'_', o.person_id) 
        WHERE (o.Date_of_Birth IS NOT NULL AND o.Date_of_Birth NOT LIKE '% %')
        AND o.person_id IS NOT NULL
    ) o
    GROUP BY o.observation_source_value;

    --[SUS_BRI_APC_010415_to_300619_P1 - Discharge_Method_Hospital_Provider_Spell]
    INSERT INTO SOURCE_COUNTS
    SELECT
        o.observation_source_value SourceValue,
        Count(*) SourceCount
    FROM
    (        
        SELECT DISTINCT
            o.person_id person_id,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(o.Discharge_Date_From_Hospital_Provider_Spell ,'000000')) observation_datetime,
            vl.id visit_occurrence_id,
            vdl.id visit_detail_id,
            ARRAY_TO_STRING([o.Discharge_Method_Hospital_Provider_Spell, CAST(oc.concept_id AS STRING), NULL, NULL, NULL], '_') observation_source_value
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1` o
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on oc.source_value = o.Discharge_Method_Hospital_Provider_Spell AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_APC_010415_to_300619_P1' AND oc.source_column =  'Discharge_Method_Hospital_Provider_Spell'
        LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups` vdl ON vdl.source_value = CONCAT(CAST(o.Hospital_Provider_Spell_Number as STRING),'_', o.person_id, '_', o.Episode_Number)
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = CONCAT(CAST(o.Hospital_Provider_Spell_Number as STRING),'_', o.person_id) 
        WHERE (o.Discharge_Date_From_Hospital_Provider_Spell IS NOT NULL AND o.Discharge_Date_From_Hospital_Provider_Spell NOT LIKE '% %')
        AND o.person_id IS NOT NULL
    ) o
    GROUP BY o.observation_source_value;

    --[SUS_BRI_APC_010415_to_300619_P1 - Admission_Method_Hospital_Provider_Spell]
    INSERT INTO SOURCE_COUNTS
    SELECT
        o.observation_source_value SourceValue,
        Count(*) SourceCount
    FROM
    (        
        SELECT DISTINCT
            o.person_id person_id,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(o.Start_Date_Hospital_Provider_Spell ,'000000')) observation_datetime,
            vl.id visit_occurrence_id,
            vdl.id visit_detail_id,
            ARRAY_TO_STRING([o.Admission_Method_Hospital_Provider_Spell, CAST(oc.concept_id AS STRING), NULL, NULL, NULL], '_') observation_source_value
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1` o
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on oc.source_value = o.Admission_Method_Hospital_Provider_Spell AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_APC_010415_to_300619_P1' AND oc.source_column =  'Admission_Method_Hospital_Provider_Spell'
        LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups` vdl ON vdl.source_value = CONCAT(CAST(o.Hospital_Provider_Spell_Number as STRING),'_', o.person_id, '_', o.Episode_Number)
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = CONCAT(CAST(o.Hospital_Provider_Spell_Number as STRING),'_', o.person_id) 
        WHERE (o.Start_Date_Hospital_Provider_Spell IS NOT NULL AND o.Start_Date_Hospital_Provider_Spell NOT LIKE '% %')
        AND o.person_id IS NOT NULL
    ) o
    GROUP BY o.observation_source_value;

    --[SUS_BRI_APC_010415_to_300619_P1 - Discharge_Destination_Hospital_Provider_Spell]
    INSERT INTO SOURCE_COUNTS
    SELECT
        o.observation_source_value SourceValue,
        Count(*) SourceCount
    FROM
    (        
        SELECT DISTINCT
            o.person_id person_id,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(o.Discharge_Date_From_Hospital_Provider_Spell ,'000000')) observation_datetime,
            vl.id visit_occurrence_id,
            vdl.id visit_detail_id,
            ARRAY_TO_STRING([o.Discharge_Destination_Hospital_Provider_Spell, CAST(oc.concept_id AS STRING), NULL, NULL, NULL], '_') observation_source_value
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1` o
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on oc.source_value = o.Discharge_Destination_Hospital_Provider_Spell AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_APC_010415_to_300619_P1' AND oc.source_column =  'Discharge_Destination_Hospital_Provider_Spell'
        LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups` vdl ON vdl.source_value = CONCAT(CAST(o.Hospital_Provider_Spell_Number as STRING),'_', o.person_id, '_', o.Episode_Number)
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = CONCAT(CAST(o.Hospital_Provider_Spell_Number as STRING),'_', o.person_id) 
        WHERE (o.Discharge_Date_From_Hospital_Provider_Spell IS NOT NULL AND o.Discharge_Date_From_Hospital_Provider_Spell NOT LIKE '% %')
        AND o.person_id IS NOT NULL
    ) o
    GROUP BY o.observation_source_value;

    --[src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2 - Ethnic_Group]
    INSERT INTO SOURCE_COUNTS
    SELECT
        o.observation_source_value SourceValue,
        Count(*) SourceCount
    FROM
    (        
        WITH dob_cte AS
        (
        SELECT DISTINCT        
            p.person_id person_id,
            CASE WHEN CAST(SPLIT(Date_of_Birth_mm_yy, '-')[OFFSET(1)] AS INT64) BETWEEN 00 AND 22 THEN CONCAT('20', CAST(SPLIT(Date_of_Birth_mm_yy, '-')[OFFSET(1)] as STRING))
            ELSE CONCAT('19', CAST(SPLIT(Date_of_Birth_mm_yy, '-')[OFFSET(1)] as STRING))      
            END year_of_birth,
            FORMAT_DATE( "%m", PARSE_DATE("%b-%y",Date_of_Birth_mm_yy )) month_of_birth
        FROM `CY_IMOSPHERE_WORKSPACE.src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2` p
        )
        SELECT DISTINCT
            o.person_id person_id,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(dob.year_of_birth, dob.month_of_birth ,'01000000')) observation_datetime,
            vl.id visit_occurrence_id,
            vdl.id visit_detail_id,
            ARRAY_TO_STRING([o.Ethnic_Group, CAST(oc.concept_id AS STRING), NULL, NULL, NULL], '_') observation_source_value
        FROM `CY_IMOSPHERE_WORKSPACE.src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2` o
        JOIN dob_cte dob ON dob.person_id = o.person_id
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on oc.source_value = o.Ethnic_Group AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2' AND oc.source_column = 'Ethnic_Group'
        LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups` vdl ON vdl.source_value = CONCAT(CAST(o.Hospital_Provider_Spell_Number as STRING),'_', o.person_id, '_', o.Episode_Number)
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = CONCAT(CAST(o.Hospital_Provider_Spell_Number as STRING),'_', o.person_id) 
        WHERE (o.Date_of_Birth_mm_yy IS NOT NULL AND o.Date_of_Birth_mm_yy NOT LIKE '% %')
        AND o.person_id IS NOT NULL
    ) o
    GROUP BY o.observation_source_value;

    --[src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2 - Discharge_Method_Hospital_Provider_Spell]
    INSERT INTO SOURCE_COUNTS
    SELECT
        o.observation_source_value SourceValue,
        Count(*) SourceCount
    FROM
    (        
        SELECT DISTINCT
            o.person_id person_id,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(o.Discharge_Date_From_Hospital_Provider_Spell ,'000000')) observation_datetime,
            vl.id visit_occurrence_id,
            vdl.id visit_detail_id,
            ARRAY_TO_STRING([o.Discharge_Method_Hospital_Provider_Spell, CAST(oc.concept_id AS STRING), NULL, NULL, NULL], '_') observation_source_value
        FROM `CY_IMOSPHERE_WORKSPACE.src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2` o
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on oc.source_value = o.Discharge_Method_Hospital_Provider_Spell AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2' AND oc.source_column = 'Discharge_Method_Hospital_Provider_Spell'
        LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups` vdl ON vdl.source_value = CONCAT(CAST(o.Hospital_Provider_Spell_Number as STRING),'_', o.person_id, '_', o.Episode_Number)
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = CONCAT(CAST(o.Hospital_Provider_Spell_Number as STRING),'_', o.person_id) 
        WHERE (o.Discharge_Date_From_Hospital_Provider_Spell IS NOT NULL AND o.Discharge_Date_From_Hospital_Provider_Spell NOT LIKE '% %')
        AND o.person_id IS NOT NULL
    ) o
    GROUP BY o.observation_source_value;

    --[src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2 - Admission_Method_Hospital_Provider_Spell]
    INSERT INTO SOURCE_COUNTS
    SELECT
        o.observation_source_value SourceValue,
        Count(*) SourceCount
    FROM
    (        
        SELECT DISTINCT
            o.person_id person_id,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(o.Start_Date_Hospital_Provider_Spell ,'000000')) observation_datetime,
            vl.id visit_occurrence_id,
            vdl.id visit_detail_id,
            ARRAY_TO_STRING([o.Admission_Method_Hospital_Provider_Spell, CAST(oc.concept_id AS STRING), NULL, NULL, NULL], '_') observation_source_value
        FROM `CY_IMOSPHERE_WORKSPACE.src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2` o
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on oc.source_value = o.Admission_Method_Hospital_Provider_Spell AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_APC_010415_to_300619_P1' AND oc.source_column =  'Admission_Method_Hospital_Provider_Spell'
        LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups` vdl ON vdl.source_value = CONCAT(CAST(o.Hospital_Provider_Spell_Number as STRING),'_', o.person_id, '_', o.Episode_Number)
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = CONCAT(CAST(o.Hospital_Provider_Spell_Number as STRING),'_', o.person_id) 
        WHERE (o.Start_Date_Hospital_Provider_Spell IS NOT NULL AND o.Start_Date_Hospital_Provider_Spell NOT LIKE '% %')
        AND o.person_id IS NOT NULL
    ) o
    GROUP BY o.observation_source_value;

    --[src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2 - Discharge_Destination_Hospital_Provider_Spell]
    INSERT INTO SOURCE_COUNTS
    SELECT
        o.observation_source_value SourceValue,
        Count(*) SourceCount
    FROM
    (        
        SELECT DISTINCT
            o.person_id person_id,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(o.Discharge_Date_From_Hospital_Provider_Spell ,'000000')) observation_datetime,
            vl.id visit_occurrence_id,
            vdl.id visit_detail_id,
            ARRAY_TO_STRING([o.Discharge_Destination_Hospital_Provider_Spell, CASE WHEN Discharge_Destination_Hospital_Provider_Spell = '19' THEN '4140634' WHEN Discharge_Destination_Hospital_Provider_Spell = '79' THEN '4194377' END, NULL, NULL, NULL], '_') observation_source_value
        FROM `CY_IMOSPHERE_WORKSPACE.src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2` o
        LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_visit_detail_lookups` vdl ON vdl.source_value = CONCAT(CAST(o.Hospital_Provider_Spell_Number as STRING),'_', o.person_id, '_', o.Episode_Number)
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'spell_number') vl On vl.source_value = CONCAT(CAST(o.Hospital_Provider_Spell_Number as STRING),'_', o.person_id) 
        WHERE (o.Discharge_Date_From_Hospital_Provider_Spell IS NOT NULL AND o.Discharge_Date_From_Hospital_Provider_Spell NOT LIKE '% %')
        AND o.person_id IS NOT NULL
        AND o.Discharge_Destination_Hospital_Provider_Spell IN ('19', '79')
    ) o
    GROUP BY o.observation_source_value;

    --[SUS_BRI_AE_010415_to_300619 - Ethnic_Category]
    INSERT INTO SOURCE_COUNTS
    SELECT
        o.observation_source_value SourceValue,
        Count(*) SourceCount
    FROM
    (        
        SELECT DISTINCT
            o.person_id person_id,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(Date_of_Birth ,'01000000')) observation_datetime,
            vl.id visit_occurrence_id,
            ARRAY_TO_STRING([o.Ethnic_Category, CAST(oc.concept_id AS STRING), NULL, NULL, NULL], '_') observation_source_value
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` o
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on oc.source_value = o.Ethnic_Category AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_AE_010415_to_300619' AND oc.source_column = 'Ethnic_Category'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(o.AandE_Attendance_Number as STRING),'_', o.person_id) 
        WHERE (o.Date_of_Birth IS NOT NULL AND o.Date_of_Birth NOT LIKE '% %')
        AND o.person_id IS NOT NULL
    ) o
    GROUP BY o.observation_source_value;

    --[SUS_BRI_AE_010415_to_300619 - Source_of_Referral_For_AandE]
    INSERT INTO SOURCE_COUNTS
    SELECT
        o.observation_source_value SourceValue,
        Count(*) SourceCount
    FROM
    (        
        SELECT DISTINCT
            o.person_id person_id,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(o.Arrival_Date ,'000000')) observation_datetime,
            vl.id visit_occurrence_id,
            ARRAY_TO_STRING([o.Source_of_Referral_For_AandE, CAST(oc.concept_id AS STRING), NULL, 'Emergency Services', NULL], '_') observation_source_value
        FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` o
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on oc.source_value = o.Source_of_Referral_For_AandE AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_AE_010415_to_300619' AND oc.source_column = 'Source_of_Referral_For_AandE'
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(o.AandE_Attendance_Number as STRING),'_', o.person_id) 
        WHERE (o.Arrival_Date IS NOT NULL AND o.Arrival_Date NOT LIKE '% %')
        AND o.person_id IS NOT NULL
    ) o
    GROUP BY o.observation_source_value;

    --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation]
    INSERT INTO SOURCE_COUNTS
    SELECT
        o.observation_source_value SourceValue,
        Count(*) SourceCount
    FROM
    (        
        SELECT DISTINCT
            o.person_id person_id,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(o.Arrival_Date ,'000000')) observation_datetime,
            vl.id visit_occurrence_id,
            ARRAY_TO_STRING([o.Accident_And_Emergency_Investigation, NULL, NULL, NULL, NULL], '_') observation_source_value
        FROM 
        (
            SELECT DISTINCT
                o.person_id,
                o.Arrival_Date,
                o.AandE_Attendance_Number,
                o.Accident_And_Emergency_Investigation_First Accident_And_Emergency_Investigation
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` o
            JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on oc.source_value = TRIM(o.Accident_And_Emergency_Investigation_First) AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_AE_010415_to_300619' AND oc.source_column = 'Accident_And_Emergency_Investigation_First'
            UNION DISTINCT
            SELECT DISTINCT
                o.person_id,
                o.Arrival_Date,
                o.AandE_Attendance_Number,
                o.Accident_And_Emergency_Investigation_Second Accident_And_Emergency_Investigation
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` o
            JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on oc.source_value = TRIM(o.Accident_And_Emergency_Investigation_Second) AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_AE_010415_to_300619' AND oc.source_column = 'Accident_And_Emergency_Investigation_Second'
            UNION DISTINCT
            SELECT DISTINCT
                o.person_id,
                o.Arrival_Date,
                o.AandE_Attendance_Number,
                o.Accident_And_Emergency_Investigation_Third Accident_And_Emergency_Investigation
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` o
            JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on oc.source_value = TRIM(o.Accident_And_Emergency_Investigation_Third) AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_AE_010415_to_300619' AND oc.source_column = 'Accident_And_Emergency_Investigation_Third'
            UNION DISTINCT
            SELECT DISTINCT
                o.person_id,
                o.Arrival_Date,
                o.AandE_Attendance_Number,
                o.Accident_And_Emergency_Investigation_Fourth Accident_And_Emergency_Investigation
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` o
            JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on oc.source_value = TRIM(o.Accident_And_Emergency_Investigation_Fourth) AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_AE_010415_to_300619' AND oc.source_column = 'Accident_And_Emergency_Investigation_Fourth'
            UNION DISTINCT
            SELECT DISTINCT
                o.person_id,
                o.Arrival_Date,
                o.AandE_Attendance_Number,
                o.Accident_And_Emergency_Investigation_Fifth Accident_And_Emergency_Investigation
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` o
            JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on oc.source_value = TRIM(o.Accident_And_Emergency_Investigation_Fifth) AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_AE_010415_to_300619' AND oc.source_column = 'Accident_And_Emergency_Investigation_Fifth'
            UNION DISTINCT
            SELECT DISTINCT
                o.person_id,
                o.Arrival_Date,
                o.AandE_Attendance_Number,
                o.Accident_And_Emergency_Investigation_Sixth Accident_And_Emergency_Investigation
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` o
            JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on oc.source_value = TRIM(o.Accident_And_Emergency_Investigation_Sixth) AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_AE_010415_to_300619' AND oc.source_column = 'Accident_And_Emergency_Investigation_Sixth'
            UNION DISTINCT
            SELECT DISTINCT
                o.person_id,
                o.Arrival_Date,
                o.AandE_Attendance_Number,
                o.Accident_And_Emergency_Investigation_Seventh Accident_And_Emergency_Investigation
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` o
            JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on oc.source_value = TRIM(o.Accident_And_Emergency_Investigation_Seventh) AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_AE_010415_to_300619' AND oc.source_column = 'Accident_And_Emergency_Investigation_Seventh'
            UNION DISTINCT
            SELECT DISTINCT
                o.person_id,
                o.Arrival_Date,
                o.AandE_Attendance_Number,
                o.Accident_And_Emergency_Investigation_Eighth Accident_And_Emergency_Investigation
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` o
            JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on oc.source_value = TRIM(o.Accident_And_Emergency_Investigation_Eighth) AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_AE_010415_to_300619' AND oc.source_column = 'Accident_And_Emergency_Investigation_Eighth'
            UNION DISTINCT
            SELECT DISTINCT
                o.person_id,
                o.Arrival_Date,
                o.AandE_Attendance_Number,
                o.Accident_And_Emergency_Investigation_Ninth Accident_And_Emergency_Investigation
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` o
            JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on oc.source_value = TRIM(o.Accident_And_Emergency_Investigation_Ninth) AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_AE_010415_to_300619' AND oc.source_column = 'Accident_And_Emergency_Investigation_Ninth'
            UNION DISTINCT
            SELECT DISTINCT
                o.person_id,
                o.Arrival_Date,
                o.AandE_Attendance_Number,
                o.Accident_And_Emergency_Investigation_Tenth Accident_And_Emergency_Investigation
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` o
            JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on oc.source_value = TRIM(o.Accident_And_Emergency_Investigation_Tenth) AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_AE_010415_to_300619' AND oc.source_column = 'Accident_And_Emergency_Investigation_Tenth'
            UNION DISTINCT
            SELECT DISTINCT
                o.person_id,
                o.Arrival_Date,
                o.AandE_Attendance_Number,
                o.Accident_And_Emergency_Investigation_Eleventh Accident_And_Emergency_Investigation
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` o
            JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on oc.source_value = TRIM(o.Accident_And_Emergency_Investigation_Eleventh) AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_AE_010415_to_300619' AND oc.source_column = 'Accident_And_Emergency_Investigation_Eleventh'
            UNION DISTINCT
            SELECT DISTINCT
                o.person_id,
                o.Arrival_Date,
                o.AandE_Attendance_Number,
                o.Accident_And_Emergency_Investigation_Twelfth Accident_And_Emergency_Investigation
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` o
            JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on oc.source_value = TRIM(o.Accident_And_Emergency_Investigation_Twelfth) AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_AE_010415_to_300619' AND oc.source_column = 'Accident_And_Emergency_Investigation_Twelfth'
        ) o
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(o.AandE_Attendance_Number as STRING),'_', o.person_id) 
        WHERE (o.Arrival_Date IS NOT NULL AND o.Arrival_Date NOT LIKE '% %')
        AND o.person_id IS NOT NULL
    ) o
    GROUP BY o.observation_source_value;

    --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment]
    INSERT INTO SOURCE_COUNTS
    SELECT
        o.observation_source_value SourceValue,
        Count(*) SourceCount
    FROM
    (        
        SELECT DISTINCT
            o.person_id person_id,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(o.Arrival_Date ,'000000')) observation_datetime,
            vl.id visit_occurrence_id,
            ARRAY_TO_STRING([o.Accident_And_Emergency_Treatment, NULL, NULL, NULL, NULL], '_') observation_source_value
        FROM 
        (
            SELECT DISTINCT
                o.person_id,
                o.Arrival_Date,
                o.AandE_Attendance_Number,
                o.Accident_And_Emergency_Treatment_First Accident_And_Emergency_Treatment
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` o
            JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on oc.source_value = TRIM(o.Accident_And_Emergency_Treatment_First) AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_AE_010415_to_300619' AND oc.source_column = 'Accident_And_Emergency_Treatment_First'
                    UNION DISTINCT
            SELECT DISTINCT
                o.person_id,
                o.Arrival_Date,
                o.AandE_Attendance_Number,
                o.Accident_And_Emergency_Treatment_Second Accident_And_Emergency_Treatment
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` o
            JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on oc.source_value = TRIM(o.Accident_And_Emergency_Treatment_Second) AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_AE_010415_to_300619' AND oc.source_column = 'Accident_And_Emergency_Treatment_Second'
            UNION DISTINCT
            SELECT DISTINCT
                o.person_id,
                o.Arrival_Date,
                o.AandE_Attendance_Number,
                o.Accident_And_Emergency_Treatment_Third Accident_And_Emergency_Treatment
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` o
            JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on oc.source_value = TRIM(o.Accident_And_Emergency_Treatment_Third) AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_AE_010415_to_300619' AND oc.source_column = 'Accident_And_Emergency_Treatment_Third'
            UNION DISTINCT
            SELECT DISTINCT
                o.person_id,
                o.Arrival_Date,
                o.AandE_Attendance_Number,
                o.Accident_And_Emergency_Treatment_Fourth Accident_And_Emergency_Treatment
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` o
            JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on oc.source_value = TRIM(o.Accident_And_Emergency_Treatment_Fourth) AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_AE_010415_to_300619' AND oc.source_column = 'Accident_And_Emergency_Treatment_Fourth'
            UNION DISTINCT
            SELECT DISTINCT
                o.person_id,
                o.Arrival_Date,
                o.AandE_Attendance_Number,
                o.Accident_And_Emergency_Treatment_Fifth Accident_And_Emergency_Treatment
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` o
            JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on oc.source_value = TRIM(o.Accident_And_Emergency_Treatment_Fifth) AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_AE_010415_to_300619' AND oc.source_column = 'Accident_And_Emergency_Treatment_Fifth'
            UNION DISTINCT
            SELECT DISTINCT
                o.person_id,
                o.Arrival_Date,
                o.AandE_Attendance_Number,
                o.Accident_And_Emergency_Treatment_Sixth Accident_And_Emergency_Treatment
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` o
            JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on oc.source_value = TRIM(o.Accident_And_Emergency_Treatment_Sixth) AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_AE_010415_to_300619' AND oc.source_column = 'Accident_And_Emergency_Treatment_Sixth'
            UNION DISTINCT
            SELECT DISTINCT
                o.person_id,
                o.Arrival_Date,
                o.AandE_Attendance_Number,
                o.Accident_And_Emergency_Treatment_Seventh Accident_And_Emergency_Treatment
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` o
            JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on oc.source_value = TRIM(o.Accident_And_Emergency_Treatment_Seventh) AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_AE_010415_to_300619' AND oc.source_column = 'Accident_And_Emergency_Treatment_Seventh'
            UNION DISTINCT
            SELECT DISTINCT
                o.person_id,
                o.Arrival_Date,
                o.AandE_Attendance_Number,
                o.Accident_And_Emergency_Treatment_Eighth Accident_And_Emergency_Treatment
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` o
            JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on oc.source_value = TRIM(o.Accident_And_Emergency_Treatment_Eighth) AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_AE_010415_to_300619' AND oc.source_column = 'Accident_And_Emergency_Treatment_Eighth'
            UNION DISTINCT
            SELECT DISTINCT
                o.person_id,
                o.Arrival_Date,
                o.AandE_Attendance_Number,
                o.Accident_And_Emergency_Treatment_Ninth Accident_And_Emergency_Treatment
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` o
            JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on oc.source_value = TRIM(o.Accident_And_Emergency_Treatment_Ninth) AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_AE_010415_to_300619' AND oc.source_column = 'Accident_And_Emergency_Treatment_Ninth'
            UNION DISTINCT
            SELECT DISTINCT
                o.person_id,
                o.Arrival_Date,
                o.AandE_Attendance_Number,
                o.Accident_And_Emergency_Treatment_Tenth Accident_And_Emergency_Treatment
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` o
            JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on oc.source_value = TRIM(o.Accident_And_Emergency_Treatment_Tenth) AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_AE_010415_to_300619' AND oc.source_column = 'Accident_And_Emergency_Treatment_Tenth'
            UNION DISTINCT
            SELECT DISTINCT
                o.person_id,
                o.Arrival_Date,
                o.AandE_Attendance_Number,
                o.Accident_And_Emergency_Treatment_Eleventh Accident_And_Emergency_Treatment
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` o
            JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on oc.source_value = TRIM(o.Accident_And_Emergency_Treatment_Eleventh) AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_AE_010415_to_300619' AND oc.source_column = 'Accident_And_Emergency_Treatment_Eleventh'
            UNION DISTINCT
            SELECT DISTINCT
                o.person_id,
                o.Arrival_Date,
                o.AandE_Attendance_Number,
                o.Accident_And_Emergency_Treatment_Twelfth Accident_And_Emergency_Treatment
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` o
            JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` oc on oc.source_value = TRIM(o.Accident_And_Emergency_Treatment_Twelfth) AND oc.destination_table = 'OBSERVATION' AND oc.source_table = 'SUS_BRI_AE_010415_to_300619' AND oc.source_column = 'Accident_And_Emergency_Treatment_Twelfth'
        ) o
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(o.AandE_Attendance_Number as STRING),'_', o.person_id) 
        WHERE (o.Arrival_Date IS NOT NULL AND o.Arrival_Date NOT LIKE '% %')
        AND o.person_id IS NOT NULL
    ) o
    GROUP BY o.observation_source_value;

    INSERT INTO OBSERVATION_COUNTS
    SELECT
        SourceValue,
        SUM(SourceCount) SourceCount,
        0 CDMCount
    FROM SOURCE_COUNTS
    GROUP BY SourceValue;

    /* From CDM */
    UPDATE OBSERVATION_COUNTS SET
        CDMCount = Cnt
    FROM 
    (
            SELECT 
                observation_source_value,
                Count(*) Cnt
            FROM
            (
                SELECT
                    ARRAY_TO_STRING([observation_source_value, CAST(value_as_concept_id AS STRING), CAST(value_as_number AS STRING), qualifier_source_value, unit_source_value], '_') observation_source_value
                FROM `CY_IMOSPHERE_CDM_531.observation` o
            ) o
            JOIN OBSERVATION_COUNTS c ON c.SourceValue = o.observation_source_value
            GROUP BY observation_source_value
    ) UpdCnt
    WHERE SourceValue = UpdCnt.observation_source_value;

    INSERT INTO OBSERVATION_COUNTS  
    SELECT
        o.observation_source_value,
        0 SourceCount,
        COUNT(*) CDMCount
    FROM
    (
        SELECT
            ARRAY_TO_STRING([observation_source_value, CAST(value_as_concept_id AS STRING), CAST(value_as_number AS STRING), qualifier_source_value, unit_source_value], '_') observation_source_value
        FROM `CY_IMOSPHERE_CDM_531.observation` o
    ) o
    WHERE NOT EXISTS (SELECT * FROM OBSERVATION_COUNTS c WHERE c.SourceValue = o.observation_source_value)
    GROUP BY o.observation_source_value;
    
    SELECT 
        'Observation' Type,
        SourceValue Id,
        SourceCount,
        CDMCount
    FROM OBSERVATION_COUNTS 
    WHERE CDMCount != SourceCount;
END;

CREATE OR REPLACE PROCEDURE CY_IMOSPHERE_WORKSPACE.COUNT_MEASUREMENT()
OPTIONS (strict_mode=false)
BEGIN
    DROP TABLE IF EXISTS _SESSION.SOURCE_COUNTS;
    CREATE TEMP TABLE SOURCE_COUNTS (SourceValue STRING, SourceCount INT64);
    CREATE TEMP TABLE MEASUREMENT_COUNTS (SourceValue STRING, SourceCount INT64, CDMCount INT64);

    --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Investigation]
    INSERT INTO SOURCE_COUNTS
    SELECT
        m.measurement_source_value SourceValue,
        Count(*) SourceCount
    FROM
    (
        SELECT DISTINCT
            m.person_id person_id,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(m.Arrival_Date ,'000000')) measurement_datetime,
            vl.id visit_occurrence_id,
            ARRAY_TO_STRING([m.Accident_And_Emergency_Investigation, NULL, NULL, NULL], '_') measurement_source_value
        FROM
        (
            SELECT DISTINCT
                m.person_id,
                m.Arrival_Date,
                m.AandE_Attendance_Number,
                m.Accident_And_Emergency_Investigation_First Accident_And_Emergency_Investigation
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` m
            JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` mc on mc.source_value = TRIM(m.Accident_And_Emergency_Investigation_First) AND mc.destination_table = 'MEASUREMENT' AND mc.source_table = 'SUS_BRI_AE_010415_to_300619' AND mc.source_column = 'Accident_And_Emergency_Investigation_First'
            UNION DISTINCT
            SELECT DISTINCT
                m.person_id,
                m.Arrival_Date,
                m.AandE_Attendance_Number,
                m.Accident_And_Emergency_Investigation_Second Accident_And_Emergency_Investigation
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` m
            JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` mc on mc.source_value = TRIM(m.Accident_And_Emergency_Investigation_Second) AND mc.destination_table = 'MEASUREMENT' AND mc.source_table = 'SUS_BRI_AE_010415_to_300619' AND mc.source_column = 'Accident_And_Emergency_Investigation_Second'
            UNION DISTINCT
            SELECT DISTINCT
                m.person_id,
                m.Arrival_Date,
                m.AandE_Attendance_Number,
                m.Accident_And_Emergency_Investigation_Third Accident_And_Emergency_Investigation
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` m
            JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` mc on mc.source_value = TRIM(m.Accident_And_Emergency_Investigation_Third) AND mc.destination_table = 'MEASUREMENT' AND mc.source_table = 'SUS_BRI_AE_010415_to_300619' AND mc.source_column = 'Accident_And_Emergency_Investigation_Third'
            UNION DISTINCT
            SELECT DISTINCT
                m.person_id,
                m.Arrival_Date,
                m.AandE_Attendance_Number,
                m.Accident_And_Emergency_Investigation_Fourth Accident_And_Emergency_Investigation
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` m
            JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` mc on mc.source_value = TRIM(m.Accident_And_Emergency_Investigation_Fourth) AND mc.destination_table = 'MEASUREMENT' AND mc.source_table = 'SUS_BRI_AE_010415_to_300619' AND mc.source_column = 'Accident_And_Emergency_Investigation_Fourth'
            UNION DISTINCT
            SELECT DISTINCT
                m.person_id,
                m.Arrival_Date,
                m.AandE_Attendance_Number,
                m.Accident_And_Emergency_Investigation_Fifth Accident_And_Emergency_Investigation
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` m
            JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` mc on mc.source_value = TRIM(m.Accident_And_Emergency_Investigation_Fifth) AND mc.destination_table = 'MEASUREMENT' AND mc.source_table = 'SUS_BRI_AE_010415_to_300619' AND mc.source_column = 'Accident_And_Emergency_Investigation_Fifth'
            UNION DISTINCT
            SELECT DISTINCT
                m.person_id,
                m.Arrival_Date,
                m.AandE_Attendance_Number,
                m.Accident_And_Emergency_Investigation_Sixth Accident_And_Emergency_Investigation
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` m
            JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` mc on mc.source_value = TRIM(m.Accident_And_Emergency_Investigation_Sixth) AND mc.destination_table = 'MEASUREMENT' AND mc.source_table = 'SUS_BRI_AE_010415_to_300619' AND mc.source_column = 'Accident_And_Emergency_Investigation_Sixth'
            UNION DISTINCT
            SELECT DISTINCT
                m.person_id,
                m.Arrival_Date,
                m.AandE_Attendance_Number,
                m.Accident_And_Emergency_Investigation_Seventh Accident_And_Emergency_Investigation
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` m
            JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` mc on mc.source_value = TRIM(m.Accident_And_Emergency_Investigation_Seventh) AND mc.destination_table = 'MEASUREMENT' AND mc.source_table = 'SUS_BRI_AE_010415_to_300619' AND mc.source_column = 'Accident_And_Emergency_Investigation_Seventh'
            UNION DISTINCT
            SELECT DISTINCT
                m.person_id,
                m.Arrival_Date,
                m.AandE_Attendance_Number,
                m.Accident_And_Emergency_Investigation_Eighth Accident_And_Emergency_Investigation
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` m
            JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` mc on mc.source_value = TRIM(m.Accident_And_Emergency_Investigation_Eighth) AND mc.destination_table = 'MEASUREMENT' AND mc.source_table = 'SUS_BRI_AE_010415_to_300619' AND mc.source_column = 'Accident_And_Emergency_Investigation_Eighth'
            UNION DISTINCT
            SELECT DISTINCT
                m.person_id,
                m.Arrival_Date,
                m.AandE_Attendance_Number,
                m.Accident_And_Emergency_Investigation_Ninth Accident_And_Emergency_Investigation
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` m
            JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` mc on mc.source_value = TRIM(m.Accident_And_Emergency_Investigation_Ninth) AND mc.destination_table = 'MEASUREMENT' AND mc.source_table = 'SUS_BRI_AE_010415_to_300619' AND mc.source_column = 'Accident_And_Emergency_Investigation_Ninth'
            UNION DISTINCT
            SELECT DISTINCT
                m.person_id,
                m.Arrival_Date,
                m.AandE_Attendance_Number,
                m.Accident_And_Emergency_Investigation_Tenth Accident_And_Emergency_Investigation
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` m
            JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` mc on mc.source_value = TRIM(m.Accident_And_Emergency_Investigation_Tenth) AND mc.destination_table = 'MEASUREMENT' AND mc.source_table = 'SUS_BRI_AE_010415_to_300619' AND mc.source_column = 'Accident_And_Emergency_Investigation_Tenth'
            UNION DISTINCT
            SELECT DISTINCT
                m.person_id,
                m.Arrival_Date,
                m.AandE_Attendance_Number,
                m.Accident_And_Emergency_Investigation_Eleventh Accident_And_Emergency_Investigation
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` m
            JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` mc on mc.source_value = TRIM(m.Accident_And_Emergency_Investigation_Eleventh) AND mc.destination_table = 'MEASUREMENT' AND mc.source_table = 'SUS_BRI_AE_010415_to_300619' AND mc.source_column = 'Accident_And_Emergency_Investigation_Eleventh'
            UNION DISTINCT
            SELECT DISTINCT
                m.person_id,
                m.Arrival_Date,
                m.AandE_Attendance_Number,
                m.Accident_And_Emergency_Investigation_Twelfth Accident_And_Emergency_Investigation
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` m
            JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` mc on mc.source_value = TRIM(m.Accident_And_Emergency_Investigation_Twelfth) AND mc.destination_table = 'MEASUREMENT' AND mc.source_table = 'SUS_BRI_AE_010415_to_300619' AND mc.source_column = 'Accident_And_Emergency_Investigation_Twelfth'
        ) m
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(m.AandE_Attendance_Number as STRING),'_', m.person_id)
        WHERE (m.Arrival_Date IS NOT NULL AND m.Arrival_Date NOT LIKE '% %')
        AND m.person_id IS NOT NULL
    ) m
    GROUP BY m.measurement_source_value;

    --[SUS_BRI_AE_010415_to_300619 - Accident_And_Emergency_Treatment]
    INSERT INTO SOURCE_COUNTS
    SELECT
        m.measurement_source_value SourceValue,
        Count(*) SourceCount
    FROM
    (
        SELECT DISTINCT
            m.person_id person_id,
            PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(m.Arrival_Date ,'000000')) measurement_datetime,
            vl.id visit_occurrence_id,
            ARRAY_TO_STRING([m.Accident_And_Emergency_Treatment, NULL, NULL, NULL], '_') measurement_source_value
        FROM
        (
            SELECT DISTINCT
                m.person_id,
                m.Arrival_Date,
                m.AandE_Attendance_Number,
                m.Accident_And_Emergency_Treatment_First Accident_And_Emergency_Treatment
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` m
            JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` mc on mc.source_value = TRIM(m.Accident_And_Emergency_Treatment_First) AND mc.destination_table = 'MEASUREMENT' AND mc.source_table = 'SUS_BRI_AE_010415_to_300619' AND mc.source_column = 'Accident_And_Emergency_Treatment_First'
                    UNION DISTINCT
            SELECT DISTINCT
                m.person_id,
                m.Arrival_Date,
                m.AandE_Attendance_Number,
                m.Accident_And_Emergency_Treatment_Second Accident_And_Emergency_Treatment
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` m
            JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` mc on mc.source_value = TRIM(m.Accident_And_Emergency_Treatment_Second) AND mc.destination_table = 'MEASUREMENT' AND mc.source_table = 'SUS_BRI_AE_010415_to_300619' AND mc.source_column = 'Accident_And_Emergency_Treatment_Second'
            UNION DISTINCT
            SELECT DISTINCT
                m.person_id,
                m.Arrival_Date,
                m.AandE_Attendance_Number,
                m.Accident_And_Emergency_Treatment_Third Accident_And_Emergency_Treatment
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` m
            JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` mc on mc.source_value = TRIM(m.Accident_And_Emergency_Treatment_Third) AND mc.destination_table = 'MEASUREMENT' AND mc.source_table = 'SUS_BRI_AE_010415_to_300619' AND mc.source_column = 'Accident_And_Emergency_Treatment_Third'
            UNION DISTINCT
            SELECT DISTINCT
                m.person_id,
                m.Arrival_Date,
                m.AandE_Attendance_Number,
                m.Accident_And_Emergency_Treatment_Fourth Accident_And_Emergency_Treatment
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` m
            JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` mc on mc.source_value = TRIM(m.Accident_And_Emergency_Treatment_Fourth) AND mc.destination_table = 'MEASUREMENT' AND mc.source_table = 'SUS_BRI_AE_010415_to_300619' AND mc.source_column = 'Accident_And_Emergency_Treatment_Fourth'
            UNION DISTINCT
            SELECT DISTINCT
                m.person_id,
                m.Arrival_Date,
                m.AandE_Attendance_Number,
                m.Accident_And_Emergency_Treatment_Fifth Accident_And_Emergency_Treatment
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` m
            JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` mc on mc.source_value = TRIM(m.Accident_And_Emergency_Treatment_Fifth) AND mc.destination_table = 'MEASUREMENT' AND mc.source_table = 'SUS_BRI_AE_010415_to_300619' AND mc.source_column = 'Accident_And_Emergency_Treatment_Fifth'
            UNION DISTINCT
            SELECT DISTINCT
                m.person_id,
                m.Arrival_Date,
                m.AandE_Attendance_Number,
                m.Accident_And_Emergency_Treatment_Sixth Accident_And_Emergency_Treatment
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` m
            JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` mc on mc.source_value = TRIM(m.Accident_And_Emergency_Treatment_Sixth) AND mc.destination_table = 'MEASUREMENT' AND mc.source_table = 'SUS_BRI_AE_010415_to_300619' AND mc.source_column = 'Accident_And_Emergency_Treatment_Sixth'
            UNION DISTINCT
            SELECT DISTINCT
                m.person_id,
                m.Arrival_Date,
                m.AandE_Attendance_Number,
                m.Accident_And_Emergency_Treatment_Seventh Accident_And_Emergency_Treatment
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` m
            JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` mc on mc.source_value = TRIM(m.Accident_And_Emergency_Treatment_Seventh) AND mc.destination_table = 'MEASUREMENT' AND mc.source_table = 'SUS_BRI_AE_010415_to_300619' AND mc.source_column = 'Accident_And_Emergency_Treatment_Seventh'
            UNION DISTINCT
            SELECT DISTINCT
                m.person_id,
                m.Arrival_Date,
                m.AandE_Attendance_Number,
                m.Accident_And_Emergency_Treatment_Eighth Accident_And_Emergency_Treatment
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` m
            JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` mc on mc.source_value = TRIM(m.Accident_And_Emergency_Treatment_Eighth) AND mc.destination_table = 'MEASUREMENT' AND mc.source_table = 'SUS_BRI_AE_010415_to_300619' AND mc.source_column = 'Accident_And_Emergency_Treatment_Eighth'
            UNION DISTINCT
            SELECT DISTINCT
                m.person_id,
                m.Arrival_Date,
                m.AandE_Attendance_Number,
                m.Accident_And_Emergency_Treatment_Ninth Accident_And_Emergency_Treatment
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` m
            JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` mc on mc.source_value = TRIM(m.Accident_And_Emergency_Treatment_Ninth) AND mc.destination_table = 'MEASUREMENT' AND mc.source_table = 'SUS_BRI_AE_010415_to_300619' AND mc.source_column = 'Accident_And_Emergency_Treatment_Ninth'
            UNION DISTINCT
            SELECT DISTINCT
                m.person_id,
                m.Arrival_Date,
                m.AandE_Attendance_Number,
                m.Accident_And_Emergency_Treatment_Tenth Accident_And_Emergency_Treatment
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` m
            JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` mc on mc.source_value = TRIM(m.Accident_And_Emergency_Treatment_Tenth) AND mc.destination_table = 'MEASUREMENT' AND mc.source_table = 'SUS_BRI_AE_010415_to_300619' AND mc.source_column = 'Accident_And_Emergency_Treatment_Tenth'
            UNION DISTINCT
            SELECT DISTINCT
                m.person_id,
                m.Arrival_Date,
                m.AandE_Attendance_Number,
                m.Accident_And_Emergency_Treatment_Eleventh Accident_And_Emergency_Treatment
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` m
            JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` mc on mc.source_value = TRIM(m.Accident_And_Emergency_Treatment_Eleventh) AND mc.destination_table = 'MEASUREMENT' AND mc.source_table = 'SUS_BRI_AE_010415_to_300619' AND mc.source_column = 'Accident_And_Emergency_Treatment_Eleventh'
            UNION DISTINCT
            SELECT DISTINCT
                m.person_id,
                m.Arrival_Date,
                m.AandE_Attendance_Number,
                m.Accident_And_Emergency_Treatment_Twelfth Accident_And_Emergency_Treatment
            FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_AE_010415_to_300619` m
            JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` mc on mc.source_value = TRIM(m.Accident_And_Emergency_Treatment_Twelfth) AND mc.destination_table = 'MEASUREMENT' AND mc.source_table = 'SUS_BRI_AE_010415_to_300619' AND mc.source_column = 'Accident_And_Emergency_Treatment_Twelfth'
        ) m
        LEFT JOIN (SELECT id, source_value FROM `CY_IMOSPHERE_WORKSPACE.tbl_visit_occurrence_lookups` l WHERE l.source_table = 'SUS_BRI_AE_010415_to_300619') vl On vl.source_value = CONCAT(CAST(m.AandE_Attendance_Number as STRING),'_', m.person_id)
        WHERE (m.Arrival_Date IS NOT NULL AND m.Arrival_Date NOT LIKE '% %')
        AND m.person_id IS NOT NULL
    ) m
    GROUP BY m.measurement_source_value;

    --[tbl_SRCode]
    INSERT INTO SOURCE_COUNTS
    SELECT
        m.measurement_source_value SourceValue,
        Count(*) SourceCount
    FROM
    (
        SELECT DISTINCT
            m.person_id person_id,
            dateevent condition_start_datetime,
            p.provider_id provider_id,
            ARRAY_TO_STRING([m.ctv3code, numericvalue, CAST(CAST(numericvalue AS FLOAT64) AS STRING), numericunit], '_') measurement_source_value
        FROM `CY_IMOSPHERE_WORKSPACE.tbl_SRCode` m
        JOIN `CY_LOOKUPS.tbl_CTV3ToSnomed_Map` sm ON sm.ctv3code = m.ctv3code
        JOIN `CY_CDM_VOCAB.concept` sc ON sc.concept_code = sm.SNOMEDCode
        JOIN `CY_CDM_VOCAB.concept_relationship` cr ON cr.concept_id_1 = sc.concept_id
        JOIN `CY_CDM_VOCAB.concept` dc ON dc.concept_id = cr.concept_id_2
        LEFT JOIN `CY_IMOSPHERE_CDM_531.provider` p ON p.provider_id = CAST(iddoneby AS INT64)
        LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` unit on unit.source_value = TRIM(m.numericunit) AND unit.destination_table = 'MEASUREMENT' AND unit.source_table = 'tbl_SRCode' AND unit.source_column = 'numericunit'
        WHERE sc.vocabulary_id = 'SNOMED'
        AND cr.relationship_id = 'Maps to'
        AND dc.standard_concept = 'S'
        AND dc.domain_id = 'Measurement'
        AND m.dateevent IS NOT NULL
        AND m.person_id IS NOT NULL
    ) m
    GROUP BY m.measurement_source_value;

    INSERT INTO MEASUREMENT_COUNTS
    SELECT
        SourceValue,
        SUM(SourceCount) SourceCount,
        0 CDMCount
    FROM SOURCE_COUNTS
    GROUP BY SourceValue;

    /* From CDM */
    UPDATE MEASUREMENT_COUNTS SET
        CDMCount = Cnt
    FROM
    (
            SELECT
                measurement_source_value,
                Count(*) Cnt
            FROM
            (
                SELECT
                    ARRAY_TO_STRING([measurement_source_value, value_source_value, CAST(value_as_number AS STRING), unit_source_value], '_') measurement_source_value
                FROM `CY_IMOSPHERE_CDM_531.measurement` m
            ) m
            JOIN MEASUREMENT_COUNTS c ON c.SourceValue = m.measurement_source_value
            GROUP BY measurement_source_value
    ) UpdCnt
    WHERE SourceValue = UpdCnt.measurement_source_value;

    INSERT INTO MEASUREMENT_COUNTS
    SELECT
        m.measurement_source_value,
        0 SourceCount,
        COUNT(*) CDMCount
    FROM
    (
        SELECT
            ARRAY_TO_STRING([measurement_source_value, value_source_value, CAST(value_as_number AS STRING), unit_source_value], '_') measurement_source_value
        FROM `CY_IMOSPHERE_CDM_531.measurement` m
    ) m
    WHERE NOT EXISTS (SELECT * FROM MEASUREMENT_COUNTS c WHERE c.SourceValue = m.measurement_source_value)
    GROUP BY m.measurement_source_value;

    SELECT 
        'Measurement' Type,
        SourceValue Id,
        SourceCount,
        CDMCount
    FROM MEASUREMENT_COUNTS 
    WHERE CDMCount != SourceCount;
END;

CREATE OR REPLACE PROCEDURE CY_IMOSPHERE_WORKSPACE.COUNT_DRUG_EXPOSURE()
OPTIONS (strict_mode=false)
BEGIN
    DROP TABLE IF EXISTS _SESSION.SOURCE_COUNTS;
    CREATE TEMP TABLE SOURCE_COUNTS (SourceValue STRING, SourceCount INT64);
    CREATE TEMP TABLE DRUG_COUNTS (SourceValue STRING, SourceCount INT64, CDMCount INT64);

    --[tbl_SRImmunisation]
    INSERT INTO SOURCE_COUNTS
    SELECT
        d.drug_source_value SourceValue,
        Count(*) SourceCount
    FROM
    (
        SELECT DISTINCT
            d.person_id person_id,
            COALESCE(dateevent, dateeventrecorded)drug_exposure_start_datetime,	
            ARRAY_TO_STRING([idimmunisationcontent, NULL], '_') drug_source_value
        FROM `CY_IMOSPHERE_WORKSPACE.tbl_SRImmunisation` d
        JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_mappings` dc on dc.source_value = TRIM(d.idimmunisationcontent) AND dc.destination_table = 'DRUG_EXPOSURE' AND dc.source_table = 'tbl_SRImmunisation' AND dc.source_column = 'idimmunisationcontent'
        WHERE (d.dateevent IS NOT NULL OR d.dateeventrecorded IS NOT NULL)
        AND d.person_id IS NOT NULL
    ) d
    GROUP BY d.drug_source_value;
    
    --[tbl_SRPrimaryCareMedication]
    INSERT INTO SOURCE_COUNTS
    SELECT
        d.drug_source_value SourceValue,
        Count(*) SourceCount
    FROM
    (
        SELECT DISTINCT
            d.person_id person_id,
            d.DateMedicationStart drug_exposure_start_datetime,
            COALESCE(d.DateMedicationEnd, d.DateMedicationStart) drug_exposure_end_datetime,
            ARRAY_TO_STRING([d.idmultilexdmd, CAST(SAFE_CAST(SPLIT(REGEXP_REPLACE(medicationquantity,'[^0-9 ]',''), ' ')[OFFSET(0)] AS FLOAT64) AS STRING)], '_') drug_source_value
        FROM `CY_IMOSPHERE_WORKSPACE.tbl_SRPrimaryCareMedication` d
        JOIN `CY_CDM_VOCAB.concept` sc ON sc.concept_code = d.idmultilexdmd 
        JOIN `CY_CDM_VOCAB.concept_relationship` cr ON cr.concept_id_1 = sc.concept_id
        JOIN `CY_CDM_VOCAB.concept` dc ON dc.concept_id = cr.concept_id_2
        WHERE (d.DateMedicationStart IS NOT NULL OR d.DateMedicationEnd IS NOT NULL)
        AND sc.vocabulary_id = 'dm+d'
        AND dc.standard_concept = 'S'
        AND cr.relationship_id = 'Maps to'
        AND d.person_id IS NOT NULL
    ) d
    GROUP BY d.drug_source_value;

    INSERT INTO DRUG_COUNTS
    SELECT
        SourceValue,
        SUM(SourceCount) SourceCount,
        0 CDMCount
    FROM SOURCE_COUNTS
    GROUP BY SourceValue;

    /* From CDM */
    UPDATE DRUG_COUNTS SET
        CDMCount = Cnt
    FROM
    (
            SELECT
                drug_source_value,
                Count(*) Cnt
            FROM
            (
                SELECT
                    ARRAY_TO_STRING([drug_source_value, CAST(quantity AS STRING)], '_') drug_source_value
                FROM `CY_IMOSPHERE_CDM_531.drug_exposure` d
            ) m
            JOIN DRUG_COUNTS c ON c.SourceValue = m.drug_source_value
            GROUP BY drug_source_value
    ) UpdCnt
    WHERE SourceValue = UpdCnt.drug_source_value;

    INSERT INTO DRUG_COUNTS
    SELECT
        m.drug_source_value,
        0 SourceCount,
        COUNT(*) CDMCount
    FROM
    (
        SELECT
            ARRAY_TO_STRING([drug_source_value, CAST(quantity AS STRING)], '_') drug_source_value
        FROM `CY_IMOSPHERE_CDM_531.drug_exposure` d
    ) m
    WHERE NOT EXISTS (SELECT * FROM DRUG_COUNTS c WHERE c.SourceValue = m.drug_source_value)
    GROUP BY m.drug_source_value;

    SELECT 
        'Drug exposure' Type,
        SourceValue Id,
        SourceCount,
        CDMCount
    FROM DRUG_COUNTS 
    WHERE CDMCount != SourceCount;
END;

CREATE OR REPLACE PROCEDURE CY_IMOSPHERE_WORKSPACE.ETL_COUNTS()
OPTIONS (strict_mode=false)
BEGIN
    CALL CY_IMOSPHERE_WORKSPACE.COUNT_LOCATION();
    CALL CY_IMOSPHERE_WORKSPACE.COUNT_CARE_SITE();
    CALL CY_IMOSPHERE_WORKSPACE.COUNT_PERSON();
    CALL CY_IMOSPHERE_WORKSPACE.COUNT_DEATH();
    CALL CY_IMOSPHERE_WORKSPACE.COUNT_PROVIDER();
    CALL CY_IMOSPHERE_WORKSPACE.COUNT_VISIT_OCCURRENCE();
    CALL CY_IMOSPHERE_WORKSPACE.COUNT_OBSERVATION_PERIOD();
    CALL CY_IMOSPHERE_WORKSPACE.COUNT_VISIT_DETAIL();
    CALL CY_IMOSPHERE_WORKSPACE.COUNT_CONDITION_OCCURRENCE();
    CALL CY_IMOSPHERE_WORKSPACE.COUNT_OBSERVATION();
    CALL CY_IMOSPHERE_WORKSPACE.COUNT_MEASUREMENT();
    CALL CY_IMOSPHERE_WORKSPACE.COUNT_DRUG_EXPOSURE();
END;

CREATE OR REPLACE PROCEDURE CY_IMOSPHERE_WORKSPACE.ETL_CLEAR_CDM()
OPTIONS (strict_mode=false)
BEGIN
    DELETE FROM `CY_IMOSPHERE_CDM_531.location` WHERE 1 = 1;
    DELETE FROM `CY_IMOSPHERE_CDM_531.care_site` WHERE 1 = 1;
    DELETE FROM `CY_IMOSPHERE_CDM_531.person` WHERE 1 = 1;
    DELETE FROM `CY_IMOSPHERE_CDM_531.death` WHERE 1 = 1;
    DELETE FROM `CY_IMOSPHERE_CDM_531.provider` WHERE 1 = 1;
    DELETE FROM `CY_IMOSPHERE_CDM_531.visit_occurrence` WHERE 1 = 1;
    DELETE FROM `CY_IMOSPHERE_CDM_531.observation_period` WHERE 1 = 1;
    DELETE FROM `CY_IMOSPHERE_CDM_531.visit_detail` WHERE 1 = 1;
    DELETE FROM `CY_IMOSPHERE_CDM_531.condition_occurrence` WHERE 1 = 1;
    DELETE FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` WHERE 1 = 1;
    DELETE FROM `CY_IMOSPHERE_CDM_531.observation` WHERE 1 = 1;
    DELETE FROM `CY_IMOSPHERE_CDM_531.measurement` WHERE 1 = 1;
    DELETE FROM `CY_IMOSPHERE_CDM_531.drug_exposure` WHERE 1 = 1;
    DELETE FROM `CY_IMOSPHERE_CDM_531.cdm_source` WHERE 1 = 1;
END;

