CREATE OR ALTER PROCEDURE [dbo].[PopulateCVT3]
AS
BEGIN
    /* 
    This process load the CVT3 (Read V3 codes) that are not already mapped via Read V2.

    The current set or rules:
        * All rules are per domain, so multiple entries per code can be created
        * All codes that have single standard code are mapped first
        * Any unmapped codes whose parent code is exist are rolled up to the parent level
        * The first common parent for any umapped codes is used next. This can be several levels higher in hierarchy
        * When code mappings conflict, the most common mapping is used next
        * When there's no common mapping, then a single mapping is picked by order

    */
    -- Add vocabulary concept
    INSERT INTO [$$(OMOP)$$]..concept
    SELECT 
        nc.*
    FROM (SELECT 2000000001 concept_id, 'NHS UK Read Codes Version 3' concept_name, 'Metadata' domain_id, 'Vocabulary' vocabulary_id, 'Vocabulary' concept_class_id, NULL standard_concept, 'Custom' concept_code, '1970-01-01' valid_start_date, '2099-12-31' valid_end_date, NULL invalid_reason) nc
    WHERE NOT EXISTS (SELECT * FROM [$$(OMOP)$$]..concept c WHERE c.concept_id = nc.concept_id);

    -- Add vocabulary
    INSERT INTO [$$(OMOP)$$]..vocabulary
    SELECT 
        nv.*
    FROM (SELECT 'ReadV3' vocabulary_id, 'NHS UK Read Codes Version 3' vocabulary_name,'' vocabulary_reference, '' vocabulary_version, 2000000001 vocabulary_concept_id) nv
    WHERE NOT EXISTS (SELECT * FROM [$$(OMOP)$$]..vocabulary v WHERE v.vocabulary_concept_id = nv.vocabulary_concept_id);


    /* Only include the lastest valid mappings */
    INSERT INTO [$$(OMOP)$$]..concept
    SELECT
       2000000001 + ROW_NUMBER() OVER (Order by Code.ReadCodeV3) concept_id,
       c.concept_name,
       c.domain_id,
       'ReadV3' vocabulary_id,
       'Read' concept_class_id,
       NULL standard_concept,
       Code.ReadCodeV3 concept_code,
       MAX(sc.effective_date) valid_start_date,
       CAST('2099-12-31' as date) valid_end_Date,
       NULL invalid_reason
    FROM ctv3_to_snomed_ct sc    
    JOIN (SELECT ctv3_concept_id, MAX(effective_date) latest_date FROM ctv3_to_snomed_ct WHERE map_status = 1 AND is_assured = 1 GROUP BY ctv3_concept_id) Latest on Latest.ctv3_concept_id = sc.ctv3_concept_id and Latest.latest_date = sc.effective_date
    CROSS APPLY (SELECT sc.ctv3_concept_id + '00' COLLATE SQL_Latin1_General_CP1_CS_AS ReadCodeV3) Code
    JOIN UsedReadCodes urc on urc.code_id = Code.ReadCodeV3
    JOIN (SELECT * FROM [$$(OMOP)$$]..concept c WHERE vocabulary_id = 'SNOMED') c on c.concept_code = sc.sct_concept_id
    WHERE NOT EXISTS (SELECT concept_id, concept_code FROM [$$(OMOP)$$]..concept rc WHERE vocabulary_id = 'Read' AND rc.concept_code = Code.ReadCodeV3) 
    AND NOT EXISTS (SELECT * FROM [$$(OMOP)$$]..concept cc WHERE cc.concept_code = Code.ReadCodeV3 AND cc.valid_start_date = cc.valid_start_date AND cc.concept_name = c.concept_name and cc.vocabulary_id = 'ReadV3')
    AND sc.map_status = 1
    AND sc.is_assured = 1
    GROUP BY c.concept_name, c.domain_id, Code.ReadCodeV3
    ORDER BY Code.ReadCodeV3;

    -- Temporary mappings table
    DROP TABLE IF EXISTS #StagedMappings 
    CREATE TABLE #StagedMappings(
        ctv3_concept_id string COLLATE SQL_Latin1_General_CP1_CS_AS, 
        concept_id int64, 
        concept_name string, 
        domain_id string, 
        Mapped bit);

    CREATE INDEX idx_t_SM_cid ON #StagedMappings (ctv3_concept_id);
    CREATE INDEX idx_t_SM_cid2 ON #StagedMappings (concept_id);

    INSERT INTO #StagedMappings 
    SELECT
        ctv3_concept_id,
        tc.concept_id,
        tc.concept_name,
        tc.domain_id,
        0
    FROM ctv3_to_snomed_ct sc
    JOIN [$$(OMOP)$$]..concept mc ON mc.concept_code = sc.sct_concept_id
    JOIN [$$(OMOP)$$]..concept_relationship cr ON cr.concept_id_1 = mc.concept_id
    JOIN [$$(OMOP)$$]..concept tc ON cr.concept_id_2 = tc.concept_id
    WHERE tc.standard_concept = 'S'
    AND cr.relationship_id = 'Maps To'
    AND sc.map_status = 1
    AND sc.is_assured = 1;

    -- Hierarchy table (only the first matching parent is used)
    DROP TABLE IF EXISTS #Hierarchy
    CREATE TABLE #Hierarchy (
        concept_id_1 int64, 
        concept_name_1 string, 
        domain_id_1 string, 
        concept_id_2 int64, 
        concept_name_2 string, 
        domain_id_2 string);
    INSERT INTO #Hierarchy 
    SELECT DISTINCT
        concept_id_1,
        concept_name_1,
        domain_id_1,
        concept_id_2,
        concept_name_2,
        domain_id_2
    FROM
    (
        SELECT 
            concept_id_1,
            c.concept_name concept_name_1,
            c.domain_id domain_id_1,
            concept_id_2,
            c2.concept_name concept_name_2,
            c2.domain_id domain_id_2,
            ROW_NUMBER() OVER (PARTITION BY cr.concept_id_1 ORDER BY concept_id_2) Rw
        FROM [$$(OMOP)$$]..concept_relationship cr
        JOIN [$$(OMOP)$$]..concept c on c.concept_id = cr.concept_id_1
        JOIN [$$(OMOP)$$]..concept c2 on c2.concept_id = cr.concept_id_2
        WHERE cr.relationship_id = 'Is a'
        AND c2.standard_concept = 'S'
    ) h
    JOIN #StagedMappings sm on sm.concept_id = h.concept_id_1
    AND Rw = 1;

    -- Step 1: Single items by domain
    UPDATE sm SET
        Mapped = 1
    FROM #StagedMappings sm
    JOIN (
    SELECT  
        ctv3_concept_id
    FROM #StagedMappings sm
    JOIN [$$(OMOP)$$]..concept tc ON tc.concept_id = sm.concept_id
    GROUP BY ctv3_concept_id, tc.domain_id 
    HAVING COUNT(DISTINCT tc.concept_id) = 1) dm on dm.ctv3_concept_id = sm.ctv3_concept_id

    -- Step 2: Roll up child records to existing parent records
    DROP TABLE IF EXISTS #UpdatedSM
    CREATE TABLE #UpdatedSM (
        ctv3_concept_id string COLLATE SQL_Latin1_General_CP1_CS_AS, 
        concept_id int64, 
        domain_id string);

    WHILE (1 = 1)
    BEGIN
        WHILE (1 = 1)
        BEGIN
            UPDATE sm SET
                concept_id = lsm.concept_id,
                concept_name = lsm.concept_name,
                domain_id = lsm.domain_id
            OUTPUT inserted.ctv3_concept_id, inserted.concept_id, inserted.domain_id INTO #UpdatedSM
            FROM #StagedMappings sm
            JOIN #Hierarchy h on h.concept_id_1 = sm.concept_id
            JOIN #StagedMappings lsm ON lsm.concept_id = h.concept_id_2 AND lsm.ctv3_concept_id = sm.ctv3_concept_id AND sm.domain_id = lsm.domain_id
            WHERE sm.Mapped = 0 
            IF (@@ROWCOUNT = 0)
            BEGIN
                BREAK;
            END
        END

        UPDATE sm SET
            concept_id = lsm.parent_concept_id,
            concept_name = lsm.parent_concept_name,
            domain_id = lsm.parent_domain_id
        OUTPUT inserted.ctv3_concept_id, inserted.concept_id, inserted.domain_id INTO #UpdatedSM
        FROM #StagedMappings sm
        JOIN #Hierarchy h on h.concept_id_1 = sm.concept_id
        JOIN 
        (
            SELECT 
                sm.ctv3_concept_id,
                sm.concept_id,
                h.concept_id_2 parent_concept_id,
                h.concept_name_2 parent_concept_name,
                h.domain_id_2 parent_domain_id
            FROM #StagedMappings sm
            JOIN #Hierarchy h on h.concept_id_1 = sm.concept_id
        ) lsm on lsm.ctv3_concept_id = sm.ctv3_concept_id AND lsm.concept_id != sm.concept_id AND lsm.parent_concept_id = h.concept_id_2
        IF (@@ROWCOUNT = 0)
        BEGIN
            -- Run single update again
            UPDATE sm SET
                Mapped = 1
            FROM #StagedMappings sm
            JOIN (
            SELECT  
                ctv3_concept_id
            FROM #StagedMappings sm
            JOIN [$$(OMOP)$$]..concept tc ON tc.concept_id = sm.concept_id
            WHERE Mapped = 0
            GROUP BY ctv3_concept_id, tc.domain_id 
            HAVING COUNT(DISTINCT tc.concept_id) = 1) dm on dm.ctv3_concept_id = sm.ctv3_concept_id
            BREAK;
        END
    END

    -- Step 3: Roll up conflicting records to common parent records
    ;WITH CTE AS (
        SELECT
            sm.ctv3_concept_id,
            sm.concept_id,
            h.domain_id_2 domain_id,
            h.concept_id_1 child_id,
            h.concept_id_2 parent_concept,
            1 Lvl
        FROM #StagedMappings sm
        JOIN #Hierarchy h on h.concept_id_1 = sm.concept_id
        WHERE sm.Mapped = 0
        UNION ALL
        SELECT
            CTE.ctv3_concept_id,
            CTE.concept_id,
            CTE.domain_id,
            h.concept_id_1 child_concept,
            h.concept_id_2 parent_concept,
            CTE.Lvl + 1 Lvl
        FROM CTE
        JOIN #Hierarchy h ON h.concept_id_1 = CTE.parent_concept AND h.domain_id_2 = CTE.domain_id)
    UPDATE sm SET
        concept_id = mapped.parent_concept,
        concept_name = c.concept_name,
        domain_id = mapped.domain_id
    OUTPUT inserted.ctv3_concept_id, inserted.concept_id, inserted.domain_id INTO #UpdatedSM
    FROM #StagedMappings sm
    JOIN (
    SELECT
        c.ctv3_concept_id,
        c.concept_id,
        c.domain_id,
        c.parent_concept,
        c.Lvl
    FROM CTE c
    CROSS APPLY (
        SELECT 
            COUNT(*) PCnt 
        FROM CTE ct 
        WHERE ct.ctv3_concept_id = c.ctv3_concept_id 
        AND ct.domain_id = c.domain_id 
        AND ct.parent_concept = c.parent_concept) Cnt
    JOIN (
        SELECT 
            ctv3_concept_id, 
            domain_id, 
            COUNT(DISTINCT concept_id) Cnt 
        FROM CTE GROUP BY ctv3_concept_id, domain_id) tc 
    ON tc.ctv3_concept_id = c.ctv3_concept_id 
    AND tc.domain_id = c.domain_id 
    AND tc.Cnt = Cnt.PCnt
    JOIN (
        SELECT
            c.ctv3_concept_id,
            c.concept_id,
            c.domain_id,
            MIN(c.Lvl) UseLvL
        FROM CTE c
        CROSS APPLY (
            SELECT 
                COUNT(*) PCnt 
            FROM CTE ct 
            WHERE ct.ctv3_concept_id = c.ctv3_concept_id 
            AND ct.domain_id = c.domain_id 
            AND ct.parent_concept = c.parent_concept) Cnt
        JOIN (
            SELECT 
                ctv3_concept_id, domain_id, 
                COUNT(DISTINCT concept_id) Cnt 
            FROM CTE 
            GROUP BY 
                ctv3_concept_id, 
                domain_id) tc 
        ON tc.ctv3_concept_id = c.ctv3_concept_id 
        AND tc.domain_id = c.domain_id 
        AND tc.Cnt = Cnt.PCnt
        GROUP BY 
            c.ctv3_concept_id, 
            c.concept_id, 
            c.domain_id) Lx 
    ON Lx.UseLvL = c.Lvl 
    AND lx.ctv3_concept_id = c.ctv3_concept_id 
    AND lx.concept_id = c.concept_id 
    AND lx.domain_id = c.domain_id) mapped
    ON mapped.ctv3_concept_id = sm.ctv3_concept_id
    AND mapped.concept_id = sm.concept_id
    AND mapped.domain_id = sm.domain_id
    JOIN [$$(OMOP)$$]..concept c on c.concept_id = mapped.parent_concept
    WHERE Mapped = 0;

    UPDATE sm
        Set Mapped = 1
    FROM #StagedMappings sm
    JOIN #UpdatedSM usm 
    ON sm.concept_id = usm.concept_id
    AND sm.ctv3_concept_id = usm.ctv3_concept_id
    AND sm.domain_id = usm.domain_id;

    -- Step 4: Remove mappings where there is an existing mapped record
    DELETE sm
    FROM #StagedMappings sm
    JOIN #StagedMappings sm2
        ON sm2.ctv3_concept_id = sm.ctv3_concept_id
        AND sm2.domain_id = sm.domain_id
    WHERE sm2.Mapped = 1
    AND sm.Mapped = 0;

    -- Step 5: Remove observerable entity class when there is an alternative map availble then check single maps
    DELETE sm
    FROM #StagedMappings sm
    JOIN #StagedMappings sm2
        ON sm2.ctv3_concept_id = sm.ctv3_concept_id
        AND sm2.domain_id = sm.domain_id
    JOIN [$$(OMOP)$$]..concept c1
        ON c1.concept_id =  sm.concept_id
    WHERE sm2.Mapped = 0
    AND sm.Mapped = 0
    AND c1.concept_class_id = 'Observable Entity';

    UPDATE sm SET
        Mapped = 1
    FROM #StagedMappings sm
    JOIN (
    SELECT  
        ctv3_concept_id
    FROM #StagedMappings sm
    JOIN [$$(OMOP)$$]..concept tc ON tc.concept_id = sm.concept_id
    WHERE Mapped = 0
    GROUP BY ctv3_concept_id, tc.domain_id 
    HAVING COUNT(DISTINCT tc.concept_id) = 1) dm on dm.ctv3_concept_id = sm.ctv3_concept_id

    -- Step 6: Select the most common mapping
    DELETE sm
    FROM #StagedMappings sm
    JOIN (
        SELECT
            sm.ctv3_concept_id,
            sm.concept_id,
            sm.domain_id,
            COUNT(*) Recs
        FROM #StagedMappings sm
        WHERE Mapped = 0
        GROUP BY     
            sm.ctv3_concept_id,
            sm.concept_id,
            sm.domain_id) Cnts
    ON Cnts.ctv3_concept_id = sm.ctv3_concept_id
    AND Cnts.concept_id = sm.concept_id
    AND Cnts.domain_id = sm.domain_id
    JOIN (
        SELECT 
            ctv3_concept_id,
            domain_id,
            Max(Recs) MaxCnt
        FROM (
            SELECT
                sm.ctv3_concept_id,
                sm.concept_id,
                sm.domain_id,
                COUNT(*) Recs
            FROM #StagedMappings sm
            WHERE Mapped = 0
            GROUP BY     
                ctv3_concept_id,
                concept_id,
                domain_id) Cnts
        GROUP BY
            ctv3_concept_id,
            domain_id
        HAVING Max(Recs) >1) MaxCnts
    ON MaxCnts.ctv3_concept_id = sm.ctv3_concept_id
    AND MaxCnts.domain_id = sm.domain_id
    AND Cnts.Recs != MaxCnts.MaxCnt

    UPDATE sm SET
        Mapped = 1
    FROM #StagedMappings sm
    JOIN (
    SELECT  
        ctv3_concept_id
    FROM #StagedMappings sm
    JOIN [$$(OMOP)$$]..concept tc ON tc.concept_id = sm.concept_id
    WHERE Mapped = 0
    GROUP BY ctv3_concept_id, tc.domain_id 
    HAVING COUNT(DISTINCT tc.concept_id) = 1) dm on dm.ctv3_concept_id = sm.ctv3_concept_id

    -- Step 7: Select first avaialble map for remaining records
    UPDATE sm SET
        concept_id = Rws.concept_id,
        concept_name = Rws.concept_name,
        domain_id = Rws.domain_id
    FROM #StagedMappings sm
    JOIN (
    SELECT
        ctv3_concept_id,
        concept_id,
        concept_name,
        domain_id,
        ROW_NUMBER() OVER (PARTITION BY ctv3_concept_id ORDER BY concept_id) Rw
    FROM #StagedMappings) Rws on Rws.ctv3_concept_id = sm.ctv3_concept_id
    WHERE sm.Mapped = 0 AND Rws.Rw = 1;

    UPDATE sm SET
        Mapped = 1
    FROM #StagedMappings sm
    JOIN (
    SELECT  
        ctv3_concept_id
    FROM #StagedMappings sm
    JOIN [$$(OMOP)$$]..concept tc ON tc.concept_id = sm.concept_id
    WHERE Mapped = 0
    GROUP BY ctv3_concept_id, tc.domain_id 
    HAVING COUNT(DISTINCT tc.concept_id) = 1) dm on dm.ctv3_concept_id = sm.ctv3_concept_id

    /* Get standard maps */
    INSERT INTO [$$(OMOP)$$]..concept_relationship
    SELECT DISTINCT
      c.concept_id concept_id_1,
      sm.concept_id concept_id_2,
      'Maps To' relationship_id,
      c2.valid_start_date,
      c2.valid_end_date,
      c2.invalid_reason
    FROM #StagedMappings sm
    CROSS APPLY (SELECT sm.ctv3_concept_id + '00' COLLATE SQL_Latin1_General_CP1_CS_AS ReadCodeV3) Code
    JOIN (SELECT * FROM [$$(OMOP)$$]..concept c WHERE vocabulary_id = 'READV3') c ON c.concept_code = Code.ReadCodeV3 AND c.vocabulary_id = 'ReadV3' AND c.domain_id = sm.domain_id
    JOIN [$$(OMOP)$$]..concept c2 on c.concept_id = c2.concept_id
    WHERE NOT EXISTS (SELECT * FROM [$$(OMOP)$$]..concept_relationship cr WHERE cr.concept_id_1 = c.concept_id AND cr.concept_id_2 = sm.concept_id AND cr.relationship_id = 'Maps to')
    order by c.concept_id

    /* Get alternate maps */
    INSERT INTO [$$(OMOP)$$]..concept_relationship
    SELECT DISTINCT
        c.concept_id concept_id_1,
        c2.concept_id concept_id_2,
        'Maps to value' relationship_id,
        c2.valid_start_date,
        c2.valid_end_date,
        c2.invalid_reason
    FROM codes_with_alternate_maps ca
    CROSS APPLY (SELECT ca.CTV3 + '00' COLLATE SQL_Latin1_General_CP1_CS_AS ReadCodeV3) Code
    JOIN (SELECT * FROM [$$(OMOP)$$]..concept c WHERE vocabulary_id = 'READV3') c ON c.concept_code = Code.ReadCodeV3 AND c.vocabulary_id = 'ReadV3'
    JOIN [$$(OMOP)$$]..concept c2 ON c2.concept_code = ca.observable_ConceptId
    WHERE ca.Use_Alternate LIKE '%Y%'
    AND c2.standard_concept = 'S'
    AND c2.vocabulary_id = 'SNOMED'
    AND NOT EXISTS (SELECT * FROM [$$(OMOP)$$]..concept_relationship cr WHERE cr.concept_id_1 = c.concept_id AND cr.concept_id_2 = c2.concept_id AND cr.relationship_id = 'Maps to value')
    ORDER BY c.concept_id
END
