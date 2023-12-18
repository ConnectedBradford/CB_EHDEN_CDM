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