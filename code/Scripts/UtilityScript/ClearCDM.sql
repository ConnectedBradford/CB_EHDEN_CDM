﻿CREATE OR REPLACE PROCEDURE CY_IMOSPHERE_WORKSPACE.ETL_CLEAR_CDM()
OPTIONS (strict_mode=false)
BEGIN
    DELETE FROM `CY_IMOSPHERE_CDM_531.location` WHERE 1 = 1;
    DELETE FROM `CY_IMOSPHERE_CDM_531.care_site` WHERE 1 = 1;
    DELETE FROM `CY_IMOSPHERE_CDM_531.person` WHERE 1 = 1;
    DELETE FROM `CY_IMOSPHERE_CDM_531.death` WHERE 1 = 1;
    DELETE FROM `CY_IMOSPHERE_CDM_531.provider` WHERE 1 = 1;
    DELETE FROM `CY_IMOSPHERE_CDM_531.visit_occurrence` WHERE 1 = 1;
    DELETE FROM `CY_IMOSPHERE_CDM_531.observation_period` WHERE 1 = 1;
    DELETE FROM `CY_IMOSPHERE_CDM_531.visit_detail` WHERE 1 = 1;
    DELETE FROM `CY_IMOSPHERE_CDM_531.condition_occurrence` WHERE 1 = 1;
    DELETE FROM `CY_IMOSPHERE_CDM_531.procedure_occurrence` WHERE 1 = 1;
    DELETE FROM `CY_IMOSPHERE_CDM_531.observation` WHERE 1 = 1;
    DELETE FROM `CY_IMOSPHERE_CDM_531.measurement` WHERE 1 = 1;
    DELETE FROM `CY_IMOSPHERE_CDM_531.drug_exposure` WHERE 1 = 1;
    DELETE FROM `CY_IMOSPHERE_CDM_531.cdm_source` WHERE 1 = 1;
END;