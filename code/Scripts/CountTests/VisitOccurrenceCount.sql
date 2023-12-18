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