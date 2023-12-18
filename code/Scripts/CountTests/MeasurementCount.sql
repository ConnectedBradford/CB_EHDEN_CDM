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