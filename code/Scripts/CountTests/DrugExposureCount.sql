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