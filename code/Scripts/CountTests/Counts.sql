﻿CREATE OR REPLACE PROCEDURE CY_IMOSPHERE_WORKSPACE.ETL_COUNTS()
OPTIONS (strict_mode=false)
BEGIN
    CALL CY_IMOSPHERE_WORKSPACE.COUNT_LOCATION();
    CALL CY_IMOSPHERE_WORKSPACE.COUNT_CARE_SITE();
    CALL CY_IMOSPHERE_WORKSPACE.COUNT_PERSON();
    CALL CY_IMOSPHERE_WORKSPACE.COUNT_DEATH();
    CALL CY_IMOSPHERE_WORKSPACE.COUNT_PROVIDER();
    CALL CY_IMOSPHERE_WORKSPACE.COUNT_VISIT_OCCURRENCE();
    CALL CY_IMOSPHERE_WORKSPACE.COUNT_OBSERVATION_PERIOD();
    CALL CY_IMOSPHERE_WORKSPACE.COUNT_VISIT_DETAIL();
    CALL CY_IMOSPHERE_WORKSPACE.COUNT_CONDITION_OCCURRENCE();
    CALL CY_IMOSPHERE_WORKSPACE.COUNT_OBSERVATION();
    CALL CY_IMOSPHERE_WORKSPACE.COUNT_MEASUREMENT();
    CALL CY_IMOSPHERE_WORKSPACE.COUNT_DRUG_EXPOSURE();
END;