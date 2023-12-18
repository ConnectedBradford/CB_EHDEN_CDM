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