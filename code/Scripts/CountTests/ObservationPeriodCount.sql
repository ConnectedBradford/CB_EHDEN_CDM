CREATE OR REPLACE PROCEDURE CY_IMOSPHERE_WORKSPACE.COUNT_OBSERVATION_PERIOD()
OPTIONS (strict_mode=false)
BEGIN
    SELECT 
        'Observation Period - missing' Type,
        COUNT(person_id) PersonsWithoutAnObservationPeriod 
    FROM `CY_IMOSPHERE_CDM_531.person` p WHERE NOT EXISTS (SELECT * FROM `CY_IMOSPHERE_CDM_531.observation_period` op WHERE op.person_id = p.person_id);
END;