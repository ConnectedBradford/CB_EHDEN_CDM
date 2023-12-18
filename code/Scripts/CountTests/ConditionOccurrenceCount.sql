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