CREATE OR REPLACE PROCEDURE CY_IMOSPHERE_WORKSPACE.POPULATE_SPELLNUMBER_LOOKUPS()
OPTIONS (strict_mode=false)
BEGIN 
    
    /*
        CREATE DISTINCT SPELL NUMBER TABLE
    */

    CREATE TABLE IF NOT EXISTS `yhcr-prd-phm-bia-core.CY_IMOSPHERE_WORKSPACE.tbl_etl_spell_number`
    (
        spell_number STRING NOT NULL
    );

    
    /*
        [src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2] Transfer
    */

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_spell_number`
    (
        spell_number 
    )
    SELECT
        DISTINCT CAST(Hospital_Provider_Spell_Number AS STRING)
    FROM `CY_IMOSPHERE_WORKSPACE.src_BDCT_CDS130InpatientFCEdata010419_310320Final_V2` vo
    LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_spell_number` ol ON ol.spell_number = vo.Hospital_Provider_Spell_Number
    WHERE Hospital_Provider_Spell_Number IS NOT NULL
    AND ol.spell_number IS NULL;


    /*
        [SUS_BRI_APC_010415_to_300619_P1] Transfer
    */

    INSERT INTO `CY_IMOSPHERE_WORKSPACE.tbl_etl_spell_number`
    (
        spell_number 
    )
    SELECT
        DISTINCT CAST(Hospital_Provider_Spell_Number AS STRING)
    FROM `CY_IMOSPHERE_WORKSPACE.SUS_BRI_APC_010415_to_300619_P1` vo
    LEFT JOIN `CY_IMOSPHERE_WORKSPACE.tbl_etl_spell_number` ol ON ol.spell_number = vo.Hospital_Provider_Spell_Number
    WHERE Hospital_Provider_Spell_Number IS NOT NULL
    AND ol.spell_number IS NULL;
END;