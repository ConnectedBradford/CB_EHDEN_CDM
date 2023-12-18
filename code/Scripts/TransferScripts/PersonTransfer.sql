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