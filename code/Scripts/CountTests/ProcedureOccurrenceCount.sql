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