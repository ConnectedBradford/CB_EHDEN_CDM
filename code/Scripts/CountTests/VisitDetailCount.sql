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