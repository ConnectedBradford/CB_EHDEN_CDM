DROP TABLE IF EXISTS cb_imosphere_cdm_531.catalogue_analysis;
CREATE TABLE cb_imosphere_cdm_531.catalogue_analysis
 AS WITH cte_analyses
as
(
  select    0 as analysis_id, 'Source name' as analysis_name,
                           '' as stratum_1_name, '' as stratum_2_name,
                           '' as stratum_3_name, '' as stratum_4_name,
                           'NA' as stratum_5_name 
union all
 select    1 as analysis_id, 'Number of persons' as analysis_name,
                           '' as stratum_1_name, '' as stratum_2_name,
                           '' as stratum_3_name, '' as stratum_4_name,
                           'NA' as stratum_5_name 
union all
 select    2 as analysis_id, 'Number of persons by gender' as analysis_name,
                           'gender_concept_id' as stratum_1_name, '' as stratum_2_name,
                           '' as stratum_3_name, '' as stratum_4_name,
                           'NA' as stratum_5_name 
union all
 select    3 as analysis_id, 'Number of persons by year of birth' as analysis_name,
                           'year_of_birth' as stratum_1_name, '' as stratum_2_name,
                           '' as stratum_3_name, '' as stratum_4_name,
                           'NA' as stratum_5_name 
union all
 select  101 as analysis_id, 'Number of persons by age, with age at first observation period' as analysis_name,
                           'age' as stratum_1_name, '' as stratum_2_name,
                           '' as stratum_3_name, '' as stratum_4_name,
                           'NA' as stratum_5_name 
union all
 select  102 as analysis_id, 'Number of persons by gender by age, with age at first observation period' as analysis_name,
                           'gender_concept_id' as stratum_1_name, 'age' as stratum_2_name,
                           '' as stratum_3_name, '' as stratum_4_name,
                           'NA' as stratum_5_name 
union all
 select  103 as analysis_id, 'Distribution of age at first observation period' as analysis_name,
                           '' as stratum_1_name, '' as stratum_2_name,
                           '' as stratum_3_name, '' as stratum_4_name,
                           'NA' as stratum_5_name 
union all
 select  104 as analysis_id, 'Distribution of age at first observation period by gender' as analysis_name,
                           'gender_concept_id' as stratum_1_name, '' as stratum_2_name,
                           '' as stratum_3_name, '' as stratum_4_name,
                           'NA' as stratum_5_name 
union all
 select  105 as analysis_id, 'Length of observation (days) of first observation period' as analysis_name,
                           '' as stratum_1_name, '' as stratum_2_name,
                           '' as stratum_3_name, '' as stratum_4_name,
                           'NA' as stratum_5_name 
union all
 select  106 as analysis_id, 'Length of observation (days) of first observation period by gender' as analysis_name,
                           'gender_concept_id' as stratum_1_name, '' as stratum_2_name,
                           '' as stratum_3_name, '' as stratum_4_name,
                           'NA' as stratum_5_name 
union all
 select  107 as analysis_id, 'Length of observation (days) of first observation period by age decile' as analysis_name,
                           'age decile' as stratum_1_name, '' as stratum_2_name,
                           '' as stratum_3_name, '' as stratum_4_name,
                           'NA' as stratum_5_name 
union all
 select  108 as analysis_id, 'Number of persons by length of observation period, in 30d increments' as analysis_name,
                           'Observation period length 30d increments' as stratum_1_name, '' as stratum_2_name,
                           '' as stratum_3_name, '' as stratum_4_name,
                           'NA' as stratum_5_name 
union all
 select  109 as analysis_id, 'Number of persons with continuous observation in each year' as analysis_name,
                           'calendar year' as stratum_1_name, '' as stratum_2_name,
                           '' as stratum_3_name, '' as stratum_4_name,
                           'NA' as stratum_5_name 
union all
 select  110 as analysis_id, 'Number of persons with continuous observation in each month' as analysis_name,
                           'calendar month' as stratum_1_name, '' as stratum_2_name,
                           '' as stratum_3_name, '' as stratum_4_name,
                           'NA' as stratum_5_name 
union all
 select  111 as analysis_id, 'Number of persons by observation period start month' as analysis_name,
                           'calendar month' as stratum_1_name, '' as stratum_2_name,
                           '' as stratum_3_name, '' as stratum_4_name,
                           'NA' as stratum_5_name 
union all
 select  112 as analysis_id, 'Number of persons by observation period end month' as analysis_name,
                           'calendar month' as stratum_1_name, '' as stratum_2_name,
                           '' as stratum_3_name, '' as stratum_4_name,
                           'NA' as stratum_5_name 
union all
 select  113 as analysis_id, 'Number of persons by number of observation periods' as analysis_name,
                           'number of observation periods' as stratum_1_name, '' as stratum_2_name,
                           '' as stratum_3_name, '' as stratum_4_name,
                           'NA' as stratum_5_name 
union all
 select  117 as analysis_id, 'Number of persons with at least one day of observation in each month' as analysis_name,
                           'calendar month' as stratum_1_name, '' as stratum_2_name,
                           '' as stratum_3_name, '' as stratum_4_name,
                           'NA' as stratum_5_name 
union all
 select  200 as analysis_id, 'Number of persons with at least one visit occurrence, by visit_concept_id' as analysis_name,
                           'visit_concept_id' as stratum_1_name, '' as stratum_2_name,
                           '' as stratum_3_name, '' as stratum_4_name,
                           'NA' as stratum_5_name 
union all
 select  201 as analysis_id, 'Number of visit occurrence records, by visit_concept_id' as analysis_name,
                           'visit_concept_id' as stratum_1_name, '' as stratum_2_name,
                           '' as stratum_3_name, '' as stratum_4_name,
                           'NA' as stratum_5_name 
union all
 select  203 as analysis_id, 'Number of distinct visit occurrence concepts per person' as analysis_name,
                           '' as stratum_1_name, '' as stratum_2_name,
                           '' as stratum_3_name, '' as stratum_4_name,
                           'NA' as stratum_5_name 
union all
 select  206 as analysis_id, 'Distribution of age by visit_concept_id' as analysis_name,
                           'visit_concept_id' as stratum_1_name, 'gender_concept_id' as stratum_2_name,
                           '' as stratum_3_name, '' as stratum_4_name,
                           'NA' as stratum_5_name 
union all
 select  211 as analysis_id, 'Distribution of length of stay by visit_concept_id' as analysis_name,
                           'visit_concept_id' as stratum_1_name, '' as stratum_2_name,
                           '' as stratum_3_name, '' as stratum_4_name,
                           'NA' as stratum_5_name 
union all
 select  220 as analysis_id, 'Number of visit occurrence records by visit occurrence start month' as analysis_name,
                           'calendar month' as stratum_1_name, '' as stratum_2_name,
                           '' as stratum_3_name, '' as stratum_4_name,
                           'NA' as stratum_5_name 
union all
 select  400 as analysis_id, 'Number of persons with at least one condition occurrence, by condition_concept_id' as analysis_name,
                           'condition_concept_id' as stratum_1_name, '' as stratum_2_name,
                           '' as stratum_3_name, '' as stratum_4_name,
                           'NA' as stratum_5_name 
union all
 select  401 as analysis_id, 'Number of condition occurrence records, by condition_concept_id' as analysis_name,
                           'condition_concept_id' as stratum_1_name, '' as stratum_2_name,
                           '' as stratum_3_name, '' as stratum_4_name,
                           'NA' as stratum_5_name 
union all
 select  403 as analysis_id, 'Number of distinct condition occurrence concepts per person' as analysis_name,
                           '' as stratum_1_name, '' as stratum_2_name,
                           '' as stratum_3_name, '' as stratum_4_name,
                           'NA' as stratum_5_name 
union all
 select  405 as analysis_id, 'Number of condition occurrence records, by condition_concept_id by condition_type_concept_id' as analysis_name,
                           'condition_concept_id' as stratum_1_name, 'condition_type_concept_id' as stratum_2_name,
                           '' as stratum_3_name, '' as stratum_4_name,
                           'NA' as stratum_5_name 
union all
 select  406 as analysis_id, 'Distribution of age by condition_concept_id' as analysis_name,
                           'condition_concept_id' as stratum_1_name, 'gender_concept_id' as stratum_2_name,
                           '' as stratum_3_name, '' as stratum_4_name,
                           'NA' as stratum_5_name 
union all
 select  420 as analysis_id, 'Number of condition occurrence records by condition occurrence start month' as analysis_name,
                           'calendar month' as stratum_1_name, '' as stratum_2_name,
                           '' as stratum_3_name, '' as stratum_4_name,
                           'NA' as stratum_5_name 
union all
 select  430 as analysis_id, 'Number of descendant condition occurrence records by concept_id' as analysis_name,
                           ' concept_id' as stratum_1_name, '' as stratum_2_name,
                           '' as stratum_3_name, '' as stratum_4_name,
                           'NA' as stratum_5_name 
union all
 select  501 as analysis_id, 'Number of records of death, by cause_concept_id' as analysis_name,
                           'cause_concept_id' as stratum_1_name, '' as stratum_2_name,
                           '' as stratum_3_name, '' as stratum_4_name,
                           'NA' as stratum_5_name 
union all
 select  502 as analysis_id, 'Number of persons by death month' as analysis_name,
                           'calendar month' as stratum_1_name, '' as stratum_2_name,
                           '' as stratum_3_name, '' as stratum_4_name,
                           'NA' as stratum_5_name 
union all
 select  506 as analysis_id, 'Distribution of age at death by gender' as analysis_name,
                           'gender_concept_id' as stratum_1_name, '' as stratum_2_name,
                           '' as stratum_3_name, '' as stratum_4_name,
                           'NA' as stratum_5_name 
union all
 select  600 as analysis_id, 'Number of persons with at least one procedure occurrence, by procedure_concept_id' as analysis_name,
                           'procedure_concept_id' as stratum_1_name, '' as stratum_2_name,
                           '' as stratum_3_name, '' as stratum_4_name,
                           'NA' as stratum_5_name 
union all
 select  601 as analysis_id, 'Number of procedure occurrence records, by procedure_concept_id' as analysis_name,
                           'procedure_concept_id' as stratum_1_name, '' as stratum_2_name,
                           '' as stratum_3_name, '' as stratum_4_name,
                           'NA' as stratum_5_name 
union all
 select  603 as analysis_id, 'Number of distinct procedure occurrence concepts per person' as analysis_name,
                           '' as stratum_1_name, '' as stratum_2_name,
                           '' as stratum_3_name, '' as stratum_4_name,
                           'NA' as stratum_5_name 
union all
 select  605 as analysis_id, 'Number of procedure4 occurrence records, by procedure_concept_id by procedure_type_concept_id' as analysis_name,
                           'procedure_concept_id' as stratum_1_name, 'procedure_type_concept_id' as stratum_2_name,
                           '' as stratum_3_name, '' as stratum_4_name,
                           'NA' as stratum_5_name 
union all
 select  606 as analysis_id, 'Distribution of age by procedure_concept_id' as analysis_name,
                           'procedure_concept_id' as stratum_1_name, 'gender_concept_id' as stratum_2_name,
                           '' as stratum_3_name, '' as stratum_4_name,
                           'NA' as stratum_5_name 
union all
 select  620 as analysis_id, 'Number of procedure occurrence records  by procedure occurrence start month' as analysis_name,
                           'calendar month' as stratum_1_name, '' as stratum_2_name,
                           '' as stratum_3_name, '' as stratum_4_name,
                           'NA' as stratum_5_name 
union all
 select  630 as analysis_id, 'Number of descendant procedure occurrence records by concept_id' as analysis_name,
                           ' concept_id' as stratum_1_name, '' as stratum_2_name,
                           '' as stratum_3_name, '' as stratum_4_name,
                           'NA' as stratum_5_name 
union all
 select  700 as analysis_id, 'Number of persons with at least one drug exposure, by drug_concept_id' as analysis_name,
                           'drug_concept_id' as stratum_1_name, '' as stratum_2_name,
                           '' as stratum_3_name, '' as stratum_4_name,
                           'NA' as stratum_5_name 
union all
 select  701 as analysis_id, 'Number of drug exposure records, by drug_concept_id' as analysis_name,
                           'drug_concept_id' as stratum_1_name, '' as stratum_2_name,
                           '' as stratum_3_name, '' as stratum_4_name,
                           'NA' as stratum_5_name 
union all
 select  703 as analysis_id, 'Number of distinct drug exposure concepts per person' as analysis_name,
                           '' as stratum_1_name, '' as stratum_2_name,
                           '' as stratum_3_name, '' as stratum_4_name,
                           'NA' as stratum_5_name 
union all
 select  705 as analysis_id, 'Number of drug exposure records, by drug_concept_id by drug_type_concept_id' as analysis_name,
                           'drug_concept_id' as stratum_1_name, 'drug_type_concept_id' as stratum_2_name,
                           '' as stratum_3_name, '' as stratum_4_name,
                           'NA' as stratum_5_name 
union all
 select  706 as analysis_id, 'Distribution of age by drug_concept_id' as analysis_name,
                           'drug_concept_id' as stratum_1_name, 'gender_concept_id' as stratum_2_name,
                           '' as stratum_3_name, '' as stratum_4_name,
                           'NA' as stratum_5_name 
union all
 select  715 as analysis_id, 'Distribution of days_supply by drug_concept_id' as analysis_name,
                           'drug_concept_id' as stratum_1_name, '' as stratum_2_name,
                           '' as stratum_3_name, '' as stratum_4_name,
                           'NA' as stratum_5_name 
union all
 select  716 as analysis_id, 'Distribution of refills by drug_concept_id' as analysis_name,
                           'drug_concept_id' as stratum_1_name, '' as stratum_2_name,
                           '' as stratum_3_name, '' as stratum_4_name,
                           'NA' as stratum_5_name 
union all
 select  717 as analysis_id, 'Distribution of quantity by drug_concept_id' as analysis_name,
                           'drug_concept_id' as stratum_1_name, '' as stratum_2_name,
                           '' as stratum_3_name, '' as stratum_4_name,
                           'NA' as stratum_5_name 
union all
 select  720 as analysis_id, 'Number of drug exposure records  by drug exposure start month' as analysis_name,
                           'calendar month' as stratum_1_name, '' as stratum_2_name,
                           '' as stratum_3_name, '' as stratum_4_name,
                           'NA' as stratum_5_name 
union all
 select  730 as analysis_id, 'Number of descendant drug exposure records by concept_id' as analysis_name,
                           ' concept_id' as stratum_1_name, '' as stratum_2_name,
                           '' as stratum_3_name, '' as stratum_4_name,
                           'NA' as stratum_5_name 
union all
 select  800 as analysis_id, 'Number of persons with at least one observation occurrence, by observation_concept_id' as analysis_name,
                           'observation_concept_id' as stratum_1_name, '' as stratum_2_name,
                           '' as stratum_3_name, '' as stratum_4_name,
                           'NA' as stratum_5_name 
union all
 select  801 as analysis_id, 'Number of observation occurrence records, by observation_concept_id' as analysis_name,
                           'observation_concept_id' as stratum_1_name, '' as stratum_2_name,
                           '' as stratum_3_name, '' as stratum_4_name,
                           'NA' as stratum_5_name 
union all
 select  803 as analysis_id, 'Number of distinct observation occurrence concepts per person' as analysis_name,
                           '' as stratum_1_name, '' as stratum_2_name,
                           '' as stratum_3_name, '' as stratum_4_name,
                           'NA' as stratum_5_name 
union all
 select  805 as analysis_id, 'Number of observation occurrence records, by observation_concept_id by observation_type_concept_id' as analysis_name,
                           'observation_concept_id' as stratum_1_name, 'observation_type_concept_id' as stratum_2_name,
                           '' as stratum_3_name, '' as stratum_4_name,
                           'NA' as stratum_5_name 
union all
 select  806 as analysis_id, 'Distribution of age by observation_concept_id' as analysis_name,
                           'observation_concept_id' as stratum_1_name, 'gender_concept_id' as stratum_2_name,
                           '' as stratum_3_name, '' as stratum_4_name,
                           'NA' as stratum_5_name 
union all
 select  815 as analysis_id, 'Distribution of numeric values, by observation_concept_id and unit_concept_id' as analysis_name,
                           '' as stratum_1_name, '' as stratum_2_name,
                           '' as stratum_3_name, '' as stratum_4_name,
                           'NA' as stratum_5_name 
union all
 select  820 as analysis_id, 'Number of observation records  by observation start month' as analysis_name,
                           'calendar month' as stratum_1_name, '' as stratum_2_name,
                           '' as stratum_3_name, '' as stratum_4_name,
                           'NA' as stratum_5_name 
union all
 select  830 as analysis_id, 'Number of descendant observation records by concept_id' as analysis_name,
                           ' concept_id' as stratum_1_name, '' as stratum_2_name,
                           '' as stratum_3_name, '' as stratum_4_name,
                           'NA' as stratum_5_name 
union all
 select  901 as analysis_id, 'Number of drug era records, by drug_concept_id' as analysis_name,
                           'drug_concept_id' as stratum_1_name, '' as stratum_2_name,
                           '' as stratum_3_name, '' as stratum_4_name,
                           'NA' as stratum_5_name 
union all
 select  920 as analysis_id, 'Number of drug era records  by drug era start month' as analysis_name,
                           'calendar month' as stratum_1_name, '' as stratum_2_name,
                           '' as stratum_3_name, '' as stratum_4_name,
                           'NA' as stratum_5_name 
union all
 select 1001 as analysis_id, 'Number of condition era records, by condition_concept_id' as analysis_name,
                           'condition_concept_id' as stratum_1_name, '' as stratum_2_name,
                           '' as stratum_3_name, '' as stratum_4_name,
                           'NA' as stratum_5_name 
union all
 select 1020 as analysis_id, 'Number of condition era records by condition era start month' as analysis_name,
                           'calendar month' as stratum_1_name, '' as stratum_2_name,
                           '' as stratum_3_name, '' as stratum_4_name,
                           'NA' as stratum_5_name 
union all
 select 1800 as analysis_id, 'Number of persons with at least one measurement occurrence, by measurement_concept_id' as analysis_name,
                           'measurement_concept_id' as stratum_1_name, '' as stratum_2_name,
                           '' as stratum_3_name, '' as stratum_4_name,
                           'NA' as stratum_5_name 
union all
 select 1801 as analysis_id, 'Number of measurement occurrence records, by measurement_concept_id' as analysis_name,
                           'measurement_concept_id' as stratum_1_name, '' as stratum_2_name,
                           '' as stratum_3_name, '' as stratum_4_name,
                           'NA' as stratum_5_name 
union all
 select 1803 as analysis_id, 'Number of distinct mesurement occurrence concepts per person' as analysis_name,
                           '' as stratum_1_name, '' as stratum_2_name,
                           '' as stratum_3_name, '' as stratum_4_name,
                           'NA' as stratum_5_name 
union all
 select 1805 as analysis_id, 'Number of measurement occurrence records, by measurement_concept_id by measurement_type_concept_id' as analysis_name,
                           'measurement_concept_id' as stratum_1_name, 'measurement_type_concept_id' as stratum_2_name,
                           '' as stratum_3_name, '' as stratum_4_name,
                           'NA' as stratum_5_name 
union all
 select 1806 as analysis_id, 'Distribution of age by measurement_concept_id' as analysis_name,
                           'measurement_concept_id' as stratum_1_name, 'gender_concept_id' as stratum_2_name,
                           '' as stratum_3_name, '' as stratum_4_name,
                           'NA' as stratum_5_name 
union all
 select 1815 as analysis_id, 'Distribution of numeric values, by measurement_concept_id and unit_concept_id' as analysis_name,
                           '' as stratum_1_name, '' as stratum_2_name,
                           '' as stratum_3_name, '' as stratum_4_name,
                           'NA' as stratum_5_name 
union all
 select 1816 as analysis_id, 'Distribution of low range, by measurement_concept_id and unit_concept_id' as analysis_name,
                           '' as stratum_1_name, '' as stratum_2_name,
                           '' as stratum_3_name, '' as stratum_4_name,
                           'NA' as stratum_5_name 
union all
 select 1817 as analysis_id, 'Distribution of high range, by measurement_concept_id and unit_concept_id' as analysis_name,
                           '' as stratum_1_name, '' as stratum_2_name,
                           '' as stratum_3_name, '' as stratum_4_name,
                           'NA' as stratum_5_name 
union all
 select 1820 as analysis_id, 'Number of measurement records  by measurement start month' as analysis_name,
                           'calendar month' as stratum_1_name, '' as stratum_2_name,
                           '' as stratum_3_name, '' as stratum_4_name,
                           'NA' as stratum_5_name 
union all
 select 1830 as analysis_id, 'Number of descendant measurement records by concept_id' as analysis_name,
                           ' concept_id' as stratum_1_name, '' as stratum_2_name,
                           '' as stratum_3_name, '' as stratum_4_name,
                           'NA' as stratum_5_name 
union all
 select 2100 as analysis_id, 'Number of persons with at least one device exposure, by device_concept_id' as analysis_name,
                           'device_concept_id' as stratum_1_name, '' as stratum_2_name,
                           '' as stratum_3_name, '' as stratum_4_name,
                           'NA' as stratum_5_name 
union all
 select 2101 as analysis_id, 'Number of device exposure records, by device_concept_id' as analysis_name,
                           'device_concept_id' as stratum_1_name, '' as stratum_2_name,
                           '' as stratum_3_name, '' as stratum_4_name,
                           'NA' as stratum_5_name 
union all
 select 2105 as analysis_id, 'Number of device exposure records, by device_concept_id by device_type_concept_id' as analysis_name,
                           'device_concept_id' as stratum_1_name, 'device_type_concept_id' as stratum_2_name,
                           '' as stratum_3_name, '' as stratum_4_name,
                           'NA' as stratum_5_name 
union all
 select 2120 as analysis_id, 'Number of device exposure records by start month' as analysis_name,
                           'calendar month' as stratum_1_name, '' as stratum_2_name,
                           '' as stratum_3_name, '' as stratum_4_name,
                           'NA' as stratum_5_name 
union all
 select 2130 as analysis_id, 'Number of descendant device exposure records by concept_id' as analysis_name,
                           ' concept_id' as stratum_1_name, '' as stratum_2_name,
                           '' as stratum_3_name, '' as stratum_4_name,
                           'NA' as stratum_5_name 
union all
 select 2201 as analysis_id, 'Number of note records, by note_type_concept_id' as analysis_name,
                           'note_type_concept_id' as stratum_1_name, '' as stratum_2_name,
                           '' as stratum_3_name, '' as stratum_4_name,
                           'NA' as stratum_5_name 
union all
 select 5000 as analysis_id, 'Source and CDM release date; Vocabulary and CDM version' as analysis_name,
                           'Source Release date' as stratum_1_name, 'CDM Release date' as stratum_2_name,
                           'CDM version' as stratum_3_name, 'Vocabulary version' as stratum_4_name,
                           'NA' as stratum_5_name
)
 SELECT analysis_id,
	analysis_name,
	stratum_1_name,
	stratum_2_name,
	stratum_3_name,
	stratum_4_name,
	stratum_5_name
 FROM cte_analyses;

-- 0	cdm name, version of Achilles and date when pre-computations were executed
--HINT DISTRIBUTE_ON_KEY(stratum_1)
CREATE TABLE cb_imosphere_cdm_531.tmpach_0
 AS
SELECT
0 as analysis_id,  cast('Connected Yorkshire Research Database' as STRING) as stratum_1, cast('1.0.2' as STRING) as stratum_2, 
convert(STRING,CURRENT_DATE(),112) as stratum_3,
cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
COUNT(distinct person_id) as count_value
FROM
cb_imosphere_cdm_531.person;
-- HINT DISTRIBUTE_ON_KEY(stratum_1)
--select 0 as analysis_id, CAST('Connected Yorkshire Research Database' AS VARCHAR(255)) as stratum_1, 
--cast(null as varchar(255)) as stratum_2, cast(null as varchar(255)) as stratum_3, cast(null as varchar(255)) as stratum_4, cast(null as varchar(255)) as stratum_5,
--COUNT_BIG(distinct person_id) as count_value, 
--  cast(null as float) as min_value,
--	cast(null as float) as max_value,
--	cast(null as float) as avg_value,
--	cast(null as float) as stdev_value,
--	cast(null as float) as median_value,
--	cast(null as float) as p10_value,
--	cast(null as float) as p25_value,
--	cast(null as float) as p75_value,
--	cast(null as float) as p90_value
-- into cb_imosphere_cdm_531.tmpach_dist_0
-- from cb_imosphere_cdm_531.person;


-- 1	Number of persons
CREATE TABLE cb_imosphere_cdm_531.tmpach_1
 AS
SELECT
1 as analysis_id,  
cast(null as STRING) as stratum_1, cast(null as STRING) as stratum_2, cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
COUNT(distinct person_id) as count_value
FROM
cb_imosphere_cdm_531.person;


-- 2	Number of persons by gender
--HINT DISTRIBUTE_ON_KEY(stratum_1)
 CREATE TABLE cb_imosphere_cdm_531.tmpach_2
  AS
SELECT
2 as analysis_id, 
cast(gender_concept_id as STRING) as stratum_1, 
cast(null as STRING) as stratum_2, cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
COUNT(distinct person_id) as count_value
FROM
cb_imosphere_cdm_531.person
 group by  2 ;


-- 3	Number of persons by year of birth
--HINT DISTRIBUTE_ON_KEY(stratum_1)
 CREATE TABLE cb_imosphere_cdm_531.tmpach_3
  AS
SELECT
3 as analysis_id,  cast(year_of_birth as STRING) as stratum_1, 
cast(null as STRING) as stratum_2, cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
COUNT(distinct person_id) as count_value
FROM
cb_imosphere_cdm_531.person
 group by  2 ;


-- 101	Number of persons by age, with age at first observation period
--HINT DISTRIBUTE_ON_KEY(stratum_1)
CREATE TABLE cb_imosphere_cdm_531.tmpach_101
 AS WITH rawdata as (
   select EXTRACT(YEAR from op1.index_date) - p1.year_of_birth as stratum_1,
    COUNT(p1.person_id) as count_value
   from cb_imosphere_cdm_531.person p1
    inner join ( select person_id, min(observation_period_start_date) as index_date  from cb_imosphere_cdm_531.observation_period  group by  1 ) op1
    on p1.person_id = op1.person_id
   group by  1 )
 SELECT 101 as analysis_id,
  cast(stratum_1 as STRING) as stratum_1,
  cast(null as STRING) as stratum_2,
  cast(null as STRING) as stratum_3,
  cast(null as STRING) as stratum_4,
  cast(null as STRING) as stratum_5,
  count_value
 FROM rawdata;

-- 102	Number of persons by gender by age, with age at first observation period
--HINT DISTRIBUTE_ON_KEY(stratum_1)
CREATE TABLE cb_imosphere_cdm_531.tmpach_102
 AS WITH rawdata as (
   select p1.gender_concept_id as stratum_1,
    EXTRACT(YEAR from op1.index_date) - p1.year_of_birth as stratum_2,
    COUNT(p1.person_id) as count_value
   from cb_imosphere_cdm_531.person p1
    inner join ( select person_id, min(observation_period_start_date) as index_date  from cb_imosphere_cdm_531.observation_period  group by  1 ) op1
    on p1.person_id = op1.person_id
   group by  p1.gender_concept_id, 2 )
 SELECT 102 as analysis_id,
  cast(stratum_1 as STRING) as stratum_1,
  cast(stratum_2 as STRING) as stratum_2,
  cast(null as STRING) as stratum_3,
  cast(null as STRING) as stratum_4,
  cast(null as STRING) as stratum_5,
  count_value
 FROM rawdata;


-- 103	Distribution of age at first observation period
--HINT DISTRIBUTE_ON_KEY(count_value)
CREATE TABLE cb_imosphere_cdm_531.tmpach_dist_103
 AS WITH rawdata   as ( select p.person_id as person_id,min(EXTRACT(YEAR from observation_period_start_date)) - p.year_of_birth  as age_value  from cb_imosphere_cdm_531.person p
  join cb_imosphere_cdm_531.observation_period op on p.person_id = op.person_id
   group by  p.person_id, p.year_of_birth
 ), overallstats   as (select cast(avg(1.0 * age_value)  as float64)  as avg_value,cast(STDDEV(age_value)  as float64)  as stdev_value,min(age_value)  as min_value,max(age_value)  as max_value,COUNT(*)  as total from rawdata
), agestats   as ( select age_value as age_value,COUNT(*)  as total,row_number() over (order by age_value)  as rn  from rawdata
   group by  1 ), agestatsprior   as ( select s.age_value as age_value,s.total as total,sum(p.total)  as accumulated  from agestats s
  join agestats p on p.rn <= s.rn
   group by  s.age_value, s.total, s.rn
 ),
tempresults as
(
   select 103 as analysis_id,
    floor((COUNT(o.total)+99)/100)*100 as count_value,
  	o.min_value,
  	o.max_value,
  	o.avg_value,
  	o.stdev_value,
  	min(case when p.accumulated >= .50 * o.total then age_value end) as median_value,
  	min(case when p.accumulated >= .10 * o.total then age_value end) as p10_value,
  	min(case when p.accumulated >= .25 * o.total then age_value end) as p25_value,
  	min(case when p.accumulated >= .75 * o.total then age_value end) as p75_value,
  	min(case when p.accumulated >= .90 * o.total then age_value end) as p90_value
  --INTO #tempResults
   from agestatsprior p
  cross join overallstats o
   group by  o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
 )
 SELECT analysis_id, 
cast(null as STRING) as stratum_1, cast(null as STRING) as stratum_2, cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5, 
count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
 FROM tempresults
;


-- 104	Distribution of age at first observation period by gender
--HINT DISTRIBUTE_ON_KEY(stratum_1) 
CREATE TABLE cb_imosphere_cdm_531.r140dv3utempresults_104
  AS WITH rawdata   as ( select p.gender_concept_id as gender_concept_id,min(EXTRACT(YEAR from observation_period_start_date)) - p.year_of_birth  as age_value  from cb_imosphere_cdm_531.person p
	join cb_imosphere_cdm_531.observation_period op on p.person_id = op.person_id
	 group by  p.person_id, p.gender_concept_id, p.year_of_birth
 ), overallstats   as ( select gender_concept_id as gender_concept_id,cast(avg(1.0 * age_value)  as float64)  as avg_value,cast(STDDEV(age_value)  as float64)  as stdev_value,min(age_value)  as min_value,max(age_value)  as max_value,COUNT(*)  as total  from rawdata
   group by  1 ), agestats   as ( select gender_concept_id as gender_concept_id,age_value as age_value,COUNT(*)  as total,row_number() over (order by age_value)  as rn  from rawdata
   group by  1, 2 ), agestatsprior   as ( select s.gender_concept_id as gender_concept_id,s.age_value as age_value,s.total as total,sum(p.total)  as accumulated  from agestats s
  join agestats p on s.gender_concept_id = p.gender_concept_id and p.rn <= s.rn
   group by  s.gender_concept_id, s.age_value, s.total, s.rn
 )
  SELECT 104 as analysis_id,
  cast(o.gender_concept_id as STRING) as stratum_1,
  floor((COUNT(o.total)+99)/100)*100 as count_value, 
  o.min_value,
	o.max_value,
	o.avg_value,
	o.stdev_value,
	min(case when p.accumulated >= .50 * o.total then age_value end) as median_value,
	min(case when p.accumulated >= .10 * o.total then age_value end) as p10_value,
	min(case when p.accumulated >= .25 * o.total then age_value end) as p25_value,
	min(case when p.accumulated >= .75 * o.total then age_value end) as p75_value,
	min(case when p.accumulated >= .90 * o.total then age_value end) as p90_value
 FROM agestatsprior p
join overallstats o on p.gender_concept_id = o.gender_concept_id
 group by  o.gender_concept_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
 ;
--HINT DISTRIBUTE_ON_KEY(stratum_1) 
CREATE TABLE cb_imosphere_cdm_531.tmpach_dist_104
 AS
SELECT
analysis_id, stratum_1, 
cast(null as STRING) as stratum_2, cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
FROM
cb_imosphere_cdm_531.r140dv3utempresults_104
;
DELETE FROM cb_imosphere_cdm_531.r140dv3utempresults_104 WHERE True;
drop table cb_imosphere_cdm_531.r140dv3utempresults_104;


-- 105	Length of observation (days) of first observation period
--HINT DISTRIBUTE_ON_KEY(count_value)
CREATE TABLE cb_imosphere_cdm_531.r140dv3utempobs_105
 AS
SELECT
count_value, rn 
FROM
(
  select DATE_DIFF(IF(SAFE_CAST(op.observation_period_end_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(op.observation_period_end_date  AS STRING)),SAFE_CAST(op.observation_period_end_date  AS DATE)), IF(SAFE_CAST(op.observation_period_start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(op.observation_period_start_date  AS STRING)),SAFE_CAST(op.observation_period_start_date  AS DATE)), DAY) as count_value,
	  row_number() over (partition by op.person_id order by op.observation_period_start_date asc) as rn
  from cb_imosphere_cdm_531.observation_period op
) a
where rn = 1;
 CREATE TABLE cb_imosphere_cdm_531.r140dv3ustatsview_105
  AS
SELECT
count_value, COUNT(*) as total, row_number() over (order by count_value) as rn
FROM
cb_imosphere_cdm_531.r140dv3utempobs_105
 group by  1 ;
--HINT DISTRIBUTE_ON_KEY(count_value)
CREATE TABLE cb_imosphere_cdm_531.r140dv3utempresults_105
  AS WITH overallstats   as (select cast(avg(1.0 * count_value)  as float64)  as avg_value,cast(STDDEV(count_value)  as float64)  as stdev_value,min(count_value)  as min_value,max(count_value)  as max_value,COUNT(*)  as total from cb_imosphere_cdm_531.r140dv3utempobs_105
), priorstats   as ( select s.count_value as count_value,s.total as total,sum(p.total)  as accumulated  from cb_imosphere_cdm_531.r140dv3ustatsview_105 s
  join cb_imosphere_cdm_531.r140dv3ustatsview_105 p on p.rn <= s.rn
   group by  s.count_value, s.total, s.rn
 )
  SELECT 105 as analysis_id,
  floor((COUNT(o.total)+99)/100)*100 as count_value,
  o.min_value,
	o.max_value,
	o.avg_value,
	o.stdev_value,
	min(case when p.accumulated >= .50 * o.total then count_value end) as median_value,
	min(case when p.accumulated >= .10 * o.total then count_value end) as p10_value,
	min(case when p.accumulated >= .25 * o.total then count_value end) as p25_value,
	min(case when p.accumulated >= .75 * o.total then count_value end) as p75_value,
	min(case when p.accumulated >= .90 * o.total then count_value end) as p90_value
 FROM priorstats p
cross join overallstats o
 group by  o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
 ;
--HINT DISTRIBUTE_ON_KEY(count_value)
CREATE TABLE cb_imosphere_cdm_531.tmpach_dist_105
 AS
SELECT
analysis_id,
cast(null as STRING) as stratum_1, cast(null as STRING) as stratum_2, cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5, count_value,
min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
FROM
cb_imosphere_cdm_531.r140dv3utempresults_105
;
DELETE FROM cb_imosphere_cdm_531.r140dv3utempobs_105 WHERE True;
drop table cb_imosphere_cdm_531.r140dv3utempobs_105;
DELETE FROM cb_imosphere_cdm_531.r140dv3ustatsview_105 WHERE True;
drop table cb_imosphere_cdm_531.r140dv3ustatsview_105;
DELETE FROM cb_imosphere_cdm_531.r140dv3utempresults_105 WHERE True;
drop table cb_imosphere_cdm_531.r140dv3utempresults_105;


-- 106	Length of observation (days) of first observation period by gender
--HINT DISTRIBUTE_ON_KEY(gender_concept_id)
CREATE TABLE cb_imosphere_cdm_531.r140dv3urawdata_106
 AS
SELECT
p.gender_concept_id, op.count_value
FROM
(
  select person_id, DATE_DIFF(IF(SAFE_CAST(op.observation_period_end_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(op.observation_period_end_date  AS STRING)),SAFE_CAST(op.observation_period_end_date  AS DATE)), IF(SAFE_CAST(op.observation_period_start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(op.observation_period_start_date  AS STRING)),SAFE_CAST(op.observation_period_start_date  AS DATE)), DAY) as count_value,
    row_number() over (partition by op.person_id order by op.observation_period_start_date asc) as rn
  from cb_imosphere_cdm_531.observation_period op
) op
join cb_imosphere_cdm_531.person p on op.person_id = p.person_id
where op.rn = 1
;
--HINT DISTRIBUTE_ON_KEY(gender_concept_id)
CREATE TABLE cb_imosphere_cdm_531.r140dv3utempresults_106
  AS WITH overallstats   as ( select gender_concept_id as gender_concept_id,cast(avg(1.0 * count_value)  as float64)  as avg_value,cast(STDDEV(count_value)  as float64)  as stdev_value,min(count_value)  as min_value,max(count_value)  as max_value,COUNT(*)  as total  from cb_imosphere_cdm_531.r140dv3urawdata_106
   group by  1 ), statsview   as ( select gender_concept_id as gender_concept_id,count_value as count_value,COUNT(*)  as total,row_number() over (order by count_value)  as rn  from cb_imosphere_cdm_531.r140dv3urawdata_106
   group by  1, 2 ), priorstats   as ( select s.gender_concept_id as gender_concept_id,s.count_value as count_value,s.total as total,sum(p.total)  as accumulated  from statsview s
  join statsview p on s.gender_concept_id = p.gender_concept_id and p.rn <= s.rn
   group by  s.gender_concept_id, s.count_value, s.total, s.rn
 )
  SELECT 106 as analysis_id,
  cast(o.gender_concept_id as STRING) as gender_concept_id,
  floor((COUNT(o.total)+99)/100)*100 as count_value,
  o.min_value,
	o.max_value,
	o.avg_value,
	o.stdev_value,
	min(case when p.accumulated >= .50 * o.total then count_value end) as median_value,
	min(case when p.accumulated >= .10 * o.total then count_value end) as p10_value,
	min(case when p.accumulated >= .25 * o.total then count_value end) as p25_value,
	min(case when p.accumulated >= .75 * o.total then count_value end) as p75_value,
	min(case when p.accumulated >= .90 * o.total then count_value end) as p90_value
 FROM priorstats p
join overallstats o on p.gender_concept_id = o.gender_concept_id
 group by  o.gender_concept_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
 ;
--HINT DISTRIBUTE_ON_KEY(stratum_1)
CREATE TABLE cb_imosphere_cdm_531.tmpach_dist_106
 AS
SELECT
analysis_id, gender_concept_id as stratum_1, 
cast(null as STRING) as stratum_2, cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
FROM
cb_imosphere_cdm_531.r140dv3utempresults_106
;
DELETE FROM cb_imosphere_cdm_531.r140dv3urawdata_106 WHERE True;
drop table cb_imosphere_cdm_531.r140dv3urawdata_106;
DELETE FROM cb_imosphere_cdm_531.r140dv3utempresults_106 WHERE True;
drop table cb_imosphere_cdm_531.r140dv3utempresults_106;


-- 107	Length of observation (days) of first observation period by age decile
--HINT DISTRIBUTE_ON_KEY(age_decile)
CREATE TABLE cb_imosphere_cdm_531.r140dv3utempresults_107
  AS WITH rawdata   as (select floor((EXTRACT(YEAR from op.observation_period_start_date) - p.year_of_birth)/10)  as age_decile,DATE_DIFF(IF(SAFE_CAST(op.observation_period_end_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(op.observation_period_end_date  AS STRING)),SAFE_CAST(op.observation_period_end_date  AS DATE)), IF(SAFE_CAST(op.observation_period_start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(op.observation_period_start_date  AS STRING)),SAFE_CAST(op.observation_period_start_date  AS DATE)), DAY)  as count_value from (
    select person_id, 
  		op.observation_period_start_date,
  		op.observation_period_end_date,
      row_number() over (partition by op.person_id order by op.observation_period_start_date asc) as rn
    from cb_imosphere_cdm_531.observation_period op
  ) op
  join cb_imosphere_cdm_531.person p on op.person_id = p.person_id
  where op.rn = 1
), overallstats   as ( select age_decile as age_decile,cast(avg(1.0 * count_value)  as float64)  as avg_value,cast(STDDEV(count_value)  as float64)  as stdev_value,min(count_value)  as min_value,max(count_value)  as max_value,COUNT(*)  as total  from rawdata
   group by  1 ), statsview   as ( select age_decile as age_decile,count_value as count_value,COUNT(*)  as total,row_number() over (order by count_value)  as rn  from rawdata
   group by  1, 2 ), priorstats   as ( select s.age_decile as age_decile,s.count_value as count_value,s.total as total,sum(p.total)  as accumulated  from statsview s
  join statsview p on s.age_decile = p.age_decile and p.rn <= s.rn
   group by  s.age_decile, s.count_value, s.total, s.rn
 )
  SELECT 107 as analysis_id,
  cast(o.age_decile as STRING) as age_decile,
  floor((COUNT(o.total)+99)/100)*100 as count_value,
  o.min_value,
	o.max_value,
	o.avg_value,
	o.stdev_value,
	min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
	min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
	min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
	min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
	min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
 FROM priorstats p
join overallstats o on p.age_decile = o.age_decile
 group by  o.age_decile, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
 ;
--HINT DISTRIBUTE_ON_KEY(stratum_1)
CREATE TABLE cb_imosphere_cdm_531.tmpach_dist_107
 AS
SELECT
analysis_id, age_decile as stratum_1, 
cast(null as STRING) as stratum_2, cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
FROM
cb_imosphere_cdm_531.r140dv3utempresults_107
;
DELETE FROM cb_imosphere_cdm_531.r140dv3utempresults_107 WHERE True;
drop table cb_imosphere_cdm_531.r140dv3utempresults_107;


-- 108	Number of persons by length of observation period, in 30d increments
--HINT DISTRIBUTE_ON_KEY(stratum_1)
CREATE TABLE cb_imosphere_cdm_531.tmpach_108
 AS WITH rawdata as (
   select floor(DATE_DIFF(IF(SAFE_CAST(op1.observation_period_end_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(op1.observation_period_end_date  AS STRING)),SAFE_CAST(op1.observation_period_end_date  AS DATE)), IF(SAFE_CAST(op1.observation_period_start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(op1.observation_period_start_date  AS STRING)),SAFE_CAST(op1.observation_period_start_date  AS DATE)), DAY)/30) as stratum_1,
    COUNT(distinct p1.person_id) as count_value
   from cb_imosphere_cdm_531.person p1
    inner join
    (select person_id,
      observation_period_start_date,
      observation_period_end_date,
      row_number() over (partition by person_id order by observation_period_start_date asc) as rn1
       from cb_imosphere_cdm_531.observation_period
    ) op1
    on p1.person_id = op1.person_id
    where op1.rn1 = 1
   group by  1 )
 SELECT 108 as analysis_id,
  cast(stratum_1 as STRING) as stratum_1,
  cast(null as STRING) as stratum_2,
  cast(null as STRING) as stratum_3,
  cast(null as STRING) as stratum_4,
  cast(null as STRING) as stratum_5,
  count_value
 FROM rawdata;


-- 109	Number of persons with continuous observation in each year
-- Note: using temp table instead of nested query because this gives vastly improved performance in Oracle
--HINT DISTRIBUTE_ON_KEY(obs_year)
CREATE TABLE cb_imosphere_cdm_531.r140dv3utemp_dates_109
 AS
SELECT
distinct 
  EXTRACT(YEAR from observation_period_start_date) as obs_year,
  DATE(EXTRACT(YEAR from observation_period_start_date), 1, 1) as obs_year_start,
  DATE(EXTRACT(YEAR from observation_period_start_date), 12, 31) as obs_year_end
FROM
cb_imosphere_cdm_531.observation_period
;
--HINT DISTRIBUTE_ON_KEY(stratum_1)
 CREATE TABLE cb_imosphere_cdm_531.tmpach_109
  AS
SELECT
109 as analysis_id,  
	cast(obs_year as STRING) as stratum_1,
	cast(null as STRING) as stratum_2, cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
	COUNT(distinct person_id) as count_value
FROM
cb_imosphere_cdm_531.observation_period,
	cb_imosphere_cdm_531.r140dv3utemp_dates_109
where  
		observation_period_start_date <= obs_year_start
	and 
		observation_period_end_date >= obs_year_end
 group by  2 ;
DELETE FROM cb_imosphere_cdm_531.r140dv3utemp_dates_109 WHERE True;
drop table cb_imosphere_cdm_531.r140dv3utemp_dates_109;


-- 110	Number of persons with continuous observation in each month
-- Note: using temp table instead of nested query because this gives vastly improved performance in Oracle
--HINT DISTRIBUTE_ON_KEY(stratum_1)
 CREATE TABLE cb_imosphere_cdm_531.tmpach_110
  AS
SELECT
110 as analysis_id,  
	cast(t1.obs_month as STRING) as stratum_1,
	cast(null as STRING) as stratum_2, cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
	COUNT(distinct op1.person_id) as count_value
FROM
cb_imosphere_cdm_531.observation_period op1
join 
(
  select distinct 
    EXTRACT(YEAR from observation_period_start_date)*100 + EXTRACT(MONTH from observation_period_start_date) as obs_month,
    DATE(EXTRACT(YEAR from observation_period_start_date), EXTRACT(MONTH from observation_period_start_date), 1)
    as obs_month_start,
    DATE_SUB(DATE_TRUNC(DATE_ADD(observation_period_start_date, INTERVAL 1 MONTH), MONTH), INTERVAL 1 DAY) as obs_month_end
  from cb_imosphere_cdm_531.observation_period
) t1 on	op1.observation_period_start_date <= t1.obs_month_start
	and	op1.observation_period_end_date >= t1.obs_month_end
 group by  t1.obs_month ;


-- 111	Number of persons by observation period start month
--HINT DISTRIBUTE_ON_KEY(stratum_1)
CREATE TABLE cb_imosphere_cdm_531.tmpach_111
 AS WITH rawdata as (
   select EXTRACT(YEAR from observation_period_start_date)*100 + EXTRACT(MONTH from observation_period_start_date) as stratum_1,
    COUNT(distinct op1.person_id) as count_value
   from cb_imosphere_cdm_531.observation_period op1
   group by  1 )
 SELECT 111 as analysis_id,
  cast(stratum_1 as STRING) as stratum_1,
  cast(null as STRING) as stratum_2,
  cast(null as STRING) as stratum_3,
  cast(null as STRING) as stratum_4,
  cast(null as STRING) as stratum_5,
  count_value
 FROM rawdata;


-- 112	Number of persons by observation period end month
--HINT DISTRIBUTE_ON_KEY(stratum_1)
CREATE TABLE cb_imosphere_cdm_531.tmpach_112
 AS WITH rawdata as (
   select EXTRACT(YEAR from observation_period_end_date)*100 + EXTRACT(MONTH from observation_period_end_date) as stratum_1,
    COUNT(distinct op1.person_id) as count_value
   from cb_imosphere_cdm_531.observation_period op1
   group by  1 )
 SELECT 112 as analysis_id,
  cast(stratum_1 as STRING) as stratum_1,
  cast(null as STRING) as stratum_2,
  cast(null as STRING) as stratum_3,
  cast(null as STRING) as stratum_4,
  cast(null as STRING) as stratum_5,
  count_value
 FROM rawdata;

-- 113	Number of persons by number of observation periods
--HINT DISTRIBUTE_ON_KEY(stratum_1)
 CREATE TABLE cb_imosphere_cdm_531.tmpach_113
  AS
SELECT
113 as analysis_id,  
	cast(op1.num_periods as STRING) as stratum_1, 
	cast(null as STRING) as stratum_2, cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
	COUNT(distinct op1.person_id) as count_value
FROM
( select person_id, COUNT(observation_period_start_date) as num_periods  from cb_imosphere_cdm_531.observation_period  group by  1 ) op1
 group by  op1.num_periods
 ;


-- 117	Number of persons with at least one day of observation in each year by gender and age decile
-- Note: using temp table instead of nested query because this gives vastly improved performance in Oracle
--HINT DISTRIBUTE_ON_KEY(stratum_1)
 CREATE TABLE cb_imosphere_cdm_531.tmpach_117
  AS
SELECT
117 as analysis_id,  
	cast(t1.obs_month as STRING) as stratum_1,
	cast(null as STRING) as stratum_2, cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
	COUNT(distinct op1.person_id) as count_value
FROM
cb_imosphere_cdm_531.observation_period op1
join 
(
  select distinct 
    EXTRACT(YEAR from observation_period_start_date)*100 + EXTRACT(MONTH from observation_period_start_date)  as obs_month
  from 
    cb_imosphere_cdm_531.observation_period
) t1 on EXTRACT(YEAR from op1.observation_period_start_date)*100 + EXTRACT(MONTH from op1.observation_period_start_date) <= t1.obs_month
	and EXTRACT(YEAR from op1.observation_period_end_date)*100 + EXTRACT(MONTH from op1.observation_period_end_date) >= t1.obs_month
 group by  t1.obs_month ;

-- 200	Number of persons with at least one visit occurrence, by visit_concept_id
-- restricted to visits overlapping with observation period
--HINT DISTRIBUTE_ON_KEY(stratum_1)
 CREATE TABLE cb_imosphere_cdm_531.tmpach_200
  AS
SELECT
200 as analysis_id, 
	cast(vo1.visit_concept_id as STRING) as stratum_1,
	cast(null as STRING) as stratum_2,
	cast(null as STRING) as stratum_3,
	cast(null as STRING) as stratum_4,
	cast(null as STRING) as stratum_5,
	floor((COUNT(distinct vo1.person_id)+99)/100)*100 as count_value
FROM
cb_imosphere_cdm_531.visit_occurrence vo1 inner join 
  cb_imosphere_cdm_531.observation_period op on vo1.person_id = op.person_id
  -- only include events that occur during observation period
  where vo1.visit_start_date <= op.observation_period_end_date and
  IFNULL(vo1.visit_end_date,vo1.visit_start_date) >= op.observation_period_start_date
 group by  vo1.visit_concept_id
 ;


-- 201	Number of visit occurrence records, by visit_concept_id
-- restricted to visits overlapping with observation period
--HINT DISTRIBUTE_ON_KEY(stratum_1)
 CREATE TABLE cb_imosphere_cdm_531.tmpach_201
  AS
SELECT
201 as analysis_id, 
	cast(vo1.visit_concept_id as STRING) as stratum_1,
	cast(null as STRING) as stratum_2,
	cast(null as STRING) as stratum_3,
	cast(null as STRING) as stratum_4,
	cast(null as STRING) as stratum_5,
	floor((COUNT(vo1.person_id)+99)/100)*100 as count_value
FROM
cb_imosphere_cdm_531.visit_occurrence vo1 inner join 
  cb_imosphere_cdm_531.observation_period op on vo1.person_id = op.person_id
  -- only include events that occur during observation period
  where vo1.visit_start_date <= op.observation_period_end_date and
  IFNULL(vo1.visit_end_date,vo1.visit_start_date) >= op.observation_period_start_date
 group by  vo1.visit_concept_id
 ;


-- 203	Number of distinct visit occurrence concepts per person
--HINT DISTRIBUTE_ON_KEY(count_value)
CREATE TABLE cb_imosphere_cdm_531.r140dv3utempresults_203
  AS WITH rawdata  as ( select vo1.person_id as person_id,COUNT(distinct vo1.visit_concept_id)  as count_value  from cb_imosphere_cdm_531.visit_occurrence vo1
		 group by  vo1.person_id
 ), overallstats   as (select cast(avg(1.0 * count_value)  as float64)  as avg_value,cast(STDDEV(count_value)  as float64)  as stdev_value,min(count_value)  as min_value,max(count_value)  as max_value,COUNT(*)  as total from rawdata
), statsview   as ( select count_value as count_value,COUNT(*)  as total,row_number() over (order by count_value)  as rn  from rawdata
   group by  1 ), priorstats   as ( select s.count_value as count_value,s.total as total,sum(p.total)  as accumulated  from statsview s
  join statsview p on p.rn <= s.rn
   group by  s.count_value, s.total, s.rn
 )
  SELECT 203 as analysis_id,
  floor((COUNT(o.total)+99)/100)*100 as count_value,
  o.min_value,
	o.max_value,
	o.avg_value,
	o.stdev_value,
	min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
	min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
	min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
	min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
	min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
 FROM priorstats p
cross join overallstats o
 group by  o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
 ;
--HINT DISTRIBUTE_ON_KEY(count_value)
CREATE TABLE cb_imosphere_cdm_531.tmpach_dist_203
 AS
SELECT
analysis_id, 
cast(null as STRING) as stratum_1, cast(null as STRING) as stratum_2, cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
FROM
cb_imosphere_cdm_531.r140dv3utempresults_203
;
DELETE FROM cb_imosphere_cdm_531.r140dv3utempresults_203 WHERE True;
drop table cb_imosphere_cdm_531.r140dv3utempresults_203;


-- 206	Distribution of age by visit_concept_id
--HINT DISTRIBUTE_ON_KEY(stratum1_id)
CREATE TABLE cb_imosphere_cdm_531.r140dv3utempresults_206
  AS WITH rawdata   as (select vo1.visit_concept_id  as stratum1_id,p1.gender_concept_id  as stratum2_id,vo1.visit_start_year - p1.year_of_birth  as count_value from cb_imosphere_cdm_531.person p1
	inner join 
  (
		 select vo.person_id, vo.visit_concept_id, min(EXTRACT(YEAR from vo.visit_start_date)) as visit_start_year
		 from cb_imosphere_cdm_531.visit_occurrence vo
		inner join 
  cb_imosphere_cdm_531.observation_period op on vo.person_id = op.person_id
  -- only include events that occur during observation period
  where vo.visit_start_date <= op.observation_period_end_date and
  IFNULL(vo.visit_end_date,vo.visit_start_date) >= op.observation_period_start_date
		 group by  vo.person_id, vo.visit_concept_id
	 ) vo1 on p1.person_id = vo1.person_id
), overallstats   as ( select stratum1_id as stratum1_id,stratum2_id as stratum2_id,cast(avg(1.0 * count_value)  as float64)  as avg_value,cast(STDDEV(count_value)  as float64)  as stdev_value,min(count_value)  as min_value,max(count_value)  as max_value,COUNT(*)  as total  from rawdata
	 group by  1, 2 ), statsview   as ( select stratum1_id as stratum1_id,stratum2_id as stratum2_id,count_value as count_value,COUNT(*)  as total,row_number() over (partition by stratum1_id, stratum2_id order by count_value)  as rn  from rawdata
   group by  1, 2, 3 ), priorstats   as ( select s.stratum1_id as stratum1_id,s.stratum2_id as stratum2_id,s.count_value as count_value,s.total as total,sum(p.total)  as accumulated  from statsview s
  join statsview p on s.stratum1_id = p.stratum1_id and s.stratum2_id = p.stratum2_id and p.rn <= s.rn
   group by  s.stratum1_id, s.stratum2_id, s.count_value, s.total, s.rn
 )
  SELECT 206 as analysis_id,
  cast(o.stratum1_id as STRING) as stratum1_id,
  cast(o.stratum2_id as STRING) as stratum2_id,
  floor((o.total+99)/100)*100 as count_value,
  o.min_value,
	o.max_value,
	o.avg_value,
	o.stdev_value,
	min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
	min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
	min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
	min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
	min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
 FROM priorstats p
join overallstats o on p.stratum1_id = o.stratum1_id and p.stratum2_id = o.stratum2_id 
 group by  o.stratum1_id, o.stratum2_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
 ;
--HINT DISTRIBUTE_ON_KEY(stratum_1)
CREATE TABLE cb_imosphere_cdm_531.tmpach_dist_206
 AS
SELECT
analysis_id, stratum1_id as stratum_1, stratum2_id as stratum_2, 
cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
FROM
cb_imosphere_cdm_531.r140dv3utempresults_206
;
DELETE FROM cb_imosphere_cdm_531.r140dv3utempresults_206 WHERE True;
drop table cb_imosphere_cdm_531.r140dv3utempresults_206;


-- 211	Distribution of length of stay by visit_concept_id
-- restrict to visits inside observation period
--HINT DISTRIBUTE_ON_KEY(stratum_id) 
CREATE TABLE cb_imosphere_cdm_531.r140dv3utempresults_211
  AS WITH rawdata  as (select visit_concept_id  as stratum_id,DATE_DIFF(IF(SAFE_CAST(visit_end_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(visit_end_date  AS STRING)),SAFE_CAST(visit_end_date  AS DATE)), IF(SAFE_CAST(visit_start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(visit_start_date  AS STRING)),SAFE_CAST(visit_start_date  AS DATE)), DAY)  as count_value from cb_imosphere_cdm_531.visit_occurrence vo inner join 
  cb_imosphere_cdm_531.observation_period op on vo.person_id = op.person_id
  -- only include events that occur during observation period
  where vo.visit_start_date >= op.observation_period_start_date and
  IFNULL(vo.visit_end_date,vo.visit_start_date) <= op.observation_period_end_date
), overallstats   as ( select stratum_id as stratum_id,cast(avg(1.0 * count_value)  as float64)  as avg_value,cast(STDDEV(count_value)  as float64)  as stdev_value,min(count_value)  as min_value,max(count_value)  as max_value,COUNT(*)  as total  from rawdata
   group by  1 ), statsview   as ( select stratum_id as stratum_id,count_value as count_value,COUNT(*)  as total,row_number() over (order by count_value)  as rn  from rawdata
   group by  1, 2 ), priorstats   as ( select s.stratum_id as stratum_id,s.count_value as count_value,s.total as total,sum(p.total)  as accumulated  from statsview s
  join statsview p on s.stratum_id = p.stratum_id and p.rn <= s.rn
   group by  s.stratum_id, s.count_value, s.total, s.rn
 )
  SELECT 211 as analysis_id,
  cast(o.stratum_id as STRING) as stratum_id,
  floor((COUNT(o.total)+99)/100)*100 as count_value,
  o.min_value,
	o.max_value,
	o.avg_value,
	o.stdev_value,
	min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
	min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
	min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
	min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
	min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
 FROM priorstats p
join overallstats o on p.stratum_id = o.stratum_id
 group by  o.stratum_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
 ;
--HINT DISTRIBUTE_ON_KEY(stratum_1) 
CREATE TABLE cb_imosphere_cdm_531.tmpach_dist_211
 AS
SELECT
analysis_id, stratum_id as stratum_1, 
cast(null as STRING) as stratum_2, cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
FROM
cb_imosphere_cdm_531.r140dv3utempresults_211
;
DELETE FROM cb_imosphere_cdm_531.r140dv3utempresults_211 WHERE True;
drop table cb_imosphere_cdm_531.r140dv3utempresults_211;


-- 220	Number of visit occurrence records by visit occurrence start month
--HINT DISTRIBUTE_ON_KEY(stratum_1)
CREATE TABLE cb_imosphere_cdm_531.tmpach_220
 AS WITH rawdata as (
   select EXTRACT(YEAR from vo1.visit_start_date)*100 + EXTRACT(MONTH from vo1.visit_start_date) as stratum_1,
    COUNT(vo1.person_id) as count_value
   from cb_imosphere_cdm_531.visit_occurrence vo1 inner join 
  cb_imosphere_cdm_531.observation_period op on vo1.person_id = op.person_id
  -- only include events that occur during observation period
  where vo1.visit_start_date <= op.observation_period_end_date and
  vo1.visit_start_date >= op.observation_period_start_date
   group by  1 )
 SELECT 220 as analysis_id,
  cast(stratum_1 as STRING) as stratum_1,
  cast(null as STRING) as stratum_2,
  cast(null as STRING) as stratum_3,
  cast(null as STRING) as stratum_4,
  cast(null as STRING) as stratum_5,
  count_value
 FROM rawdata;


-- 400	Number of persons with at least one condition occurrence, by condition_concept_id
--HINT DISTRIBUTE_ON_KEY(stratum_1)
 CREATE TABLE cb_imosphere_cdm_531.tmpach_400
  AS
SELECT
400 as analysis_id, 
	cast(co1.condition_concept_id as STRING) as stratum_1,
	cast(null as STRING) as stratum_2,
	cast(null as STRING) as stratum_3, 
	cast(null as STRING) as stratum_4, 
	cast(null as STRING) as stratum_5,
  floor((COUNT(distinct co1.person_id)+99)/100)*100 as count_value
FROM
cb_imosphere_cdm_531.condition_occurrence co1
 group by  co1.condition_concept_id
 ;


-- 401	Number of condition occurrence records, by condition_concept_id
--HINT DISTRIBUTE_ON_KEY(stratum_1)
 CREATE TABLE cb_imosphere_cdm_531.tmpach_401
  AS
SELECT
401 as analysis_id, 
	cast(co1.condition_concept_id as STRING) as stratum_1,
  cast(null as STRING) as stratum_2,
	cast(null as STRING) as stratum_3, 
	cast(null as STRING) as stratum_4, 
	cast(null as STRING) as stratum_5,
	floor((COUNT(co1.person_id)+99)/100)*100 as count_value
FROM
cb_imosphere_cdm_531.condition_occurrence co1
 group by  co1.condition_concept_id
 ;


-- 403	Number of distinct condition occurrence concepts per person
--HINT DISTRIBUTE_ON_KEY(count_value)
CREATE TABLE cb_imosphere_cdm_531.r140dv3utempresults_403
  AS WITH rawdata  as ( select co.person_id as person_id,COUNT(distinct co.condition_concept_id)  as count_value  from cb_imosphere_cdm_531.condition_occurrence co inner join 
  cb_imosphere_cdm_531.observation_period op on co.person_id = op.person_id
  -- only include events that occur during observation period
  where co.condition_start_date <= op.observation_period_end_date and
  IFNULL(co.condition_end_date,co.condition_start_date) >= op.observation_period_start_date
	 group by  co.person_id
 ), overallstats   as (select cast(avg(1.0 * count_value)  as float64)  as avg_value,cast(STDDEV(count_value)  as float64)  as stdev_value,min(count_value)  as min_value,max(count_value)  as max_value,COUNT(*)  as total from rawdata
), statsview   as ( select count_value as count_value,COUNT(*)  as total,row_number() over (order by count_value)  as rn  from rawdata
   group by  1 ), priorstats   as ( select s.count_value as count_value,s.total as total,sum(p.total)  as accumulated  from statsview s
  join statsview p on p.rn <= s.rn
   group by  s.count_value, s.total, s.rn
 )
  SELECT 403 as analysis_id,
  floor((COUNT(o.total)+99)/100)*100 as count_value,
  o.min_value,
	o.max_value,
	o.avg_value,
	o.stdev_value,
	min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
	min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
	min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
	min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
	min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
 FROM priorstats p
cross join overallstats o
 group by  o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
 ;
--HINT DISTRIBUTE_ON_KEY(count_value)
CREATE TABLE cb_imosphere_cdm_531.tmpach_dist_403
 AS
SELECT
analysis_id, 
cast(null as STRING) as stratum_1, cast(null as STRING) as stratum_2, cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
FROM
cb_imosphere_cdm_531.r140dv3utempresults_403
;
DELETE FROM cb_imosphere_cdm_531.r140dv3utempresults_403 WHERE True;
drop table cb_imosphere_cdm_531.r140dv3utempresults_403;


-- 405	Number of condition occurrence records, by condition_concept_id by condition_type_concept_id
--HINT DISTRIBUTE_ON_KEY(stratum_1)
 CREATE TABLE cb_imosphere_cdm_531.tmpach_405
  AS
SELECT
405 as analysis_id, 
	cast(co1.condition_concept_id as STRING) as stratum_1,
	cast(co1.condition_type_concept_id as STRING) as stratum_2,
	cast(null as STRING) as stratum_3,
	cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
	floor((COUNT(co1.person_id)+99)/100)*100 as count_value
FROM
cb_imosphere_cdm_531.condition_occurrence co1
 group by  co1.condition_concept_id, co1.condition_type_concept_id
 ;


-- 406	Distribution of age by condition_concept_id
--HINT DISTRIBUTE_ON_KEY(subject_id)
CREATE TABLE cb_imosphere_cdm_531.r140dv3urawdata_406
 AS
SELECT
co1.condition_concept_id as subject_id,
  p1.gender_concept_id,
	(co1.condition_start_year - p1.year_of_birth) as count_value
FROM
cb_imosphere_cdm_531.person p1
inner join 
(
	 select person_id, condition_concept_id, min(EXTRACT(YEAR from condition_start_date)) as condition_start_year
	 from cb_imosphere_cdm_531.condition_occurrence
	 group by  1, 2 ) co1 on p1.person_id = co1.person_id
;
--HINT DISTRIBUTE_ON_KEY(stratum1_id)
CREATE TABLE cb_imosphere_cdm_531.r140dv3utempresults_406
  AS WITH overallstats   as ( select subject_id  as stratum1_id,gender_concept_id  as stratum2_id,cast(avg(1.0 * count_value)  as float64)  as avg_value,cast(STDDEV(count_value)  as float64)  as stdev_value,min(count_value)  as min_value,max(count_value)  as max_value,COUNT(*)  as total  from cb_imosphere_cdm_531.r140dv3urawdata_406
	 group by  1, 2 ), statsview   as ( select subject_id  as stratum1_id,gender_concept_id  as stratum2_id,count_value as count_value,COUNT(*)  as total,row_number() over (partition by subject_id, gender_concept_id order by count_value)  as rn  from cb_imosphere_cdm_531.r140dv3urawdata_406
   group by  1, 2, 3 ), priorstats   as ( select s.stratum1_id as stratum1_id,s.stratum2_id as stratum2_id,s.count_value as count_value,s.total as total,sum(p.total)  as accumulated  from statsview s
  join statsview p on s.stratum1_id = p.stratum1_id and s.stratum2_id = p.stratum2_id and p.rn <= s.rn
   group by  s.stratum1_id, s.stratum2_id, s.count_value, s.total, s.rn
 )
  SELECT 406 as analysis_id,
  cast(o.stratum1_id as STRING) as stratum1_id,
  cast(o.stratum2_id as STRING) as stratum2_id,
  floor((COUNT(o.total)+99)/100)*100 as count_value,
  o.min_value,
	o.max_value,
	o.avg_value,
	o.stdev_value,
	min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
	min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
	min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
	min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
	min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
 FROM priorstats p
join overallstats o on p.stratum1_id = o.stratum1_id and p.stratum2_id = o.stratum2_id 
 group by  o.stratum1_id, o.stratum2_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
 ;
--HINT DISTRIBUTE_ON_KEY(stratum_1)
CREATE TABLE cb_imosphere_cdm_531.tmpach_dist_406
 AS
SELECT
analysis_id, stratum1_id as stratum_1, stratum2_id as stratum_2, 
cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
FROM
cb_imosphere_cdm_531.r140dv3utempresults_406
;
DELETE FROM cb_imosphere_cdm_531.r140dv3utempresults_406 WHERE True;
drop table cb_imosphere_cdm_531.r140dv3utempresults_406;
DELETE FROM cb_imosphere_cdm_531.r140dv3urawdata_406 WHERE True;
drop table cb_imosphere_cdm_531.r140dv3urawdata_406;


-- 420	Number of condition occurrence records by condition occurrence start month
--HINT DISTRIBUTE_ON_KEY(stratum_1)
CREATE TABLE cb_imosphere_cdm_531.tmpach_420
 AS WITH rawdata as (
   select EXTRACT(YEAR from co1.condition_start_date)*100 + EXTRACT(MONTH from co1.condition_start_date) as stratum_1,
    COUNT(co1.person_id) as count_value
   from cb_imosphere_cdm_531.condition_occurrence co1 inner join 
  cb_imosphere_cdm_531.observation_period op on co1.person_id = op.person_id
  -- only include events that occur during observation period
  where co1.condition_start_date <= op.observation_period_end_date and
  co1.condition_start_date >= op.observation_period_start_date
   group by  1 )
 SELECT 420 as analysis_id,
  cast(stratum_1 as STRING) as stratum_1,
  cast(null as STRING) as stratum_2,
  cast(null as STRING) as stratum_3,
  cast(null as STRING) as stratum_4,
  cast(null as STRING) as stratum_5,
  count_value
 FROM rawdata;


-- 430	Number of descendant condition occurrence records,by condition_concept_id
--HINT DISTRIBUTE_ON_KEY(stratum_1)
CREATE TABLE cb_imosphere_cdm_531.tmpach_430
  AS WITH cte_condition as (
	 select ca.ancestor_concept_id as concept_id, COUNT(*)
 as drc
	 from cb_imosphere_cdm_531.condition_occurrence co
		join cb_imosphere_cdm_531.concept_ancestor ca
			on ca.descendant_concept_id = co.condition_concept_id
	 group by  ca.ancestor_concept_id
 )
  SELECT 430 as analysis_id,
  cast(co.condition_concept_id as STRING) as stratum_1,
  cast(null as STRING) as stratum_2,
  cast(null as STRING) as stratum_3,
  cast(null as STRING) as stratum_4,
  cast(null as STRING) as stratum_5,
  floor((c.drc+99)/100)*100 as count_value
 FROM cb_imosphere_cdm_531.condition_occurrence co
	join cte_condition c
		on c.concept_id = co.condition_concept_id
 group by  co.condition_concept_id, c.drc
 ;


-- 501	Number of records of death, by cause_concept_id
--HINT DISTRIBUTE_ON_KEY(stratum_1)
 CREATE TABLE cb_imosphere_cdm_531.tmpach_501
  AS
SELECT
501 as analysis_id, 
	cast(d1.cause_concept_id as STRING) as stratum_1,
	cast(null as STRING) as stratum_2,
	cast(null as STRING) as stratum_3,
	cast(null as STRING) as stratum_4,
	cast(null as STRING) as stratum_5,
	floor((COUNT(d1.person_id)+99)/100)*100 as count_value
FROM
cb_imosphere_cdm_531.death d1
 group by  d1.cause_concept_id
 ;


-- 502	Number of persons by death month
--HINT DISTRIBUTE_ON_KEY(stratum_1)
CREATE TABLE cb_imosphere_cdm_531.tmpach_502
 AS WITH rawdata as (
   select EXTRACT(YEAR from d1.death_date)*100 + EXTRACT(MONTH from d1.death_date) as stratum_1,
    COUNT(distinct d1.person_id) as count_value
   from cb_imosphere_cdm_531.death d1 inner join 
  cb_imosphere_cdm_531.observation_period op on d1.person_id = op.person_id
  -- only include events that occur during observation period
  where d1.death_date <= op.observation_period_end_date and
    d1.death_date >= op.observation_period_start_date
   group by  1 )
 SELECT 502 as analysis_id,
  cast(stratum_1 as STRING) as stratum_1,
  cast(null as STRING) as stratum_2,
  cast(null as STRING) as stratum_3,
  cast(null as STRING) as stratum_4,
  cast(null as STRING) as stratum_5,
  count_value
 FROM rawdata;

-- 506	Distribution of age at death by gender
--HINT DISTRIBUTE_ON_KEY(stratum_id)
CREATE TABLE cb_imosphere_cdm_531.r140dv3utempresults_506
  AS WITH rawdata  as (select p1.gender_concept_id  as stratum_id,d1.death_year - p1.year_of_birth  as count_value from cb_imosphere_cdm_531.person p1
  inner join
  ( select person_id, min(EXTRACT(YEAR from death_date)) as death_year
   from cb_imosphere_cdm_531.death
   group by  1 ) d1
  on p1.person_id = d1.person_id
), overallstats   as ( select stratum_id as stratum_id,cast(avg(1.0 * count_value)  as float64)  as avg_value,cast(STDDEV(count_value)  as float64)  as stdev_value,min(count_value)  as min_value,max(count_value)  as max_value,COUNT(*)  as total  from rawdata
   group by  1 ), statsview   as ( select stratum_id as stratum_id,count_value as count_value,COUNT(*)  as total,row_number() over (order by count_value)  as rn  from rawdata
   group by  1, 2 ), priorstats   as ( select s.stratum_id as stratum_id,s.count_value as count_value,s.total as total,sum(p.total)  as accumulated  from statsview s
  join statsview p on s.stratum_id = p.stratum_id and p.rn <= s.rn
   group by  s.stratum_id, s.count_value, s.total, s.rn
 )
  SELECT 506 as analysis_id,
  cast(o.stratum_id as STRING) as stratum_id,
  floor((COUNT(o.total)+99)/100)*100 as count_value,
  o.min_value,
	o.max_value,
	o.avg_value,
	o.stdev_value,
	min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
	min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
	min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
	min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
	min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
 FROM priorstats p
join overallstats o on p.stratum_id = o.stratum_id
 group by  o.stratum_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
 ;
--HINT DISTRIBUTE_ON_KEY(stratum_1)
CREATE TABLE cb_imosphere_cdm_531.tmpach_dist_506
 AS
SELECT
analysis_id, stratum_id as stratum_1, 
cast(null as STRING) as stratum_2, cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
FROM
cb_imosphere_cdm_531.r140dv3utempresults_506
;
DELETE FROM cb_imosphere_cdm_531.r140dv3utempresults_506 WHERE True;
drop table cb_imosphere_cdm_531.r140dv3utempresults_506;


-- 600	Number of persons with at least one procedure occurrence, by procedure_concept_id
--HINT DISTRIBUTE_ON_KEY(stratum_1)
 CREATE TABLE cb_imosphere_cdm_531.tmpach_600
  AS
SELECT
600 as analysis_id, 
	cast(po1.procedure_concept_id as STRING) as stratum_1,
	cast(null as STRING) as stratum_2,
	cast(null as STRING) as stratum_3,
	cast(null as STRING) as stratum_4,
	cast(null as STRING) as stratum_5,
	floor((COUNT(distinct po1.person_id)+99)/100)*100 as count_value
FROM
cb_imosphere_cdm_531.procedure_occurrence po1
 group by  po1.procedure_concept_id
 ;


-- 601	Number of procedure occurrence records, by procedure_concept_id
--HINT DISTRIBUTE_ON_KEY(stratum_1)
 CREATE TABLE cb_imosphere_cdm_531.tmpach_601
  AS
SELECT
601 as analysis_id, 
	cast(po1.procedure_concept_id as STRING) as stratum_1,
	cast(null as STRING) as stratum_2,
	cast(null as STRING) as stratum_3,
	cast(null as STRING) as stratum_4,
	cast(null as STRING) as stratum_5,
	floor((COUNT(po1.person_id)+99)/100)*100 as count_value
FROM
cb_imosphere_cdm_531.procedure_occurrence po1
 group by  po1.procedure_concept_id
 ;


-- 603	Number of distinct procedure occurrence concepts per person
--HINT DISTRIBUTE_ON_KEY(count_value)
CREATE TABLE cb_imosphere_cdm_531.r140dv3utempresults_603
  AS WITH rawdata  as ( select COUNT(distinct po.procedure_concept_id)  as count_value  from cb_imosphere_cdm_531.procedure_occurrence po inner join 
  cb_imosphere_cdm_531.observation_period op on po.person_id = op.person_id
  -- only include events that occur during observation period
  where po.procedure_date <= op.observation_period_end_date and
  po.procedure_date >= op.observation_period_start_date
	 group by  po.person_id
 ), overallstats   as (select cast(avg(1.0 * count_value)  as float64)  as avg_value,cast(STDDEV(count_value)  as float64)  as stdev_value,min(count_value)  as min_value,max(count_value)  as max_value,COUNT(*)  as total from rawdata
), statsview   as ( select count_value as count_value,COUNT(*)  as total,row_number() over (order by count_value)  as rn  from rawdata
   group by  1 ), priorstats   as ( select s.count_value as count_value,s.total as total,sum(p.total)  as accumulated  from statsview s
  join statsview p on p.rn <= s.rn
   group by  s.count_value, s.total, s.rn
 )
  SELECT 603 as analysis_id,
  floor((COUNT(o.total)+99)/100)*100 as count_value,
  o.min_value,
	o.max_value,
	o.avg_value,
	o.stdev_value,
	min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
	min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
	min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
	min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
	min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
 FROM priorstats p
cross join overallstats o
 group by  o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
 ;
--HINT DISTRIBUTE_ON_KEY(count_value)
CREATE TABLE cb_imosphere_cdm_531.tmpach_dist_603
 AS
SELECT
analysis_id, 
cast(null as STRING) as stratum_1, cast(null as STRING) as stratum_2, cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
FROM
cb_imosphere_cdm_531.r140dv3utempresults_603
;
DELETE FROM cb_imosphere_cdm_531.r140dv3utempresults_603 WHERE True;
drop table cb_imosphere_cdm_531.r140dv3utempresults_603;


-- 605	Number of procedure occurrence records, by procedure_concept_id by procedure_type_concept_id
--HINT DISTRIBUTE_ON_KEY(stratum_1)
 CREATE TABLE cb_imosphere_cdm_531.tmpach_605
  AS
SELECT
605 as analysis_id, 
	cast(po1.procedure_concept_id as STRING) as stratum_1,
	cast(po1.procedure_type_concept_id as STRING) as stratum_2,
	cast(null as STRING) as stratum_3, 
	cast(null as STRING) as stratum_4, 
	cast(null as STRING) as stratum_5,
	floor((COUNT(po1.person_id)+99)/100)*100 as count_value
FROM
cb_imosphere_cdm_531.procedure_occurrence po1
 group by  po1.procedure_concept_id, po1.procedure_type_concept_id
 ;


-- 606	Distribution of age by procedure_concept_id
--HINT DISTRIBUTE_ON_KEY(subject_id)
CREATE TABLE cb_imosphere_cdm_531.r140dv3urawdata_606
 AS
SELECT
po1.procedure_concept_id as subject_id,
  p1.gender_concept_id,
	po1.procedure_start_year - p1.year_of_birth as count_value
FROM
cb_imosphere_cdm_531.person p1
inner join
(
	 select person_id, procedure_concept_id, min(EXTRACT(YEAR from procedure_date)) as procedure_start_year
	 from cb_imosphere_cdm_531.procedure_occurrence
	 group by  1, 2 ) po1 on p1.person_id = po1.person_id
;
--HINT DISTRIBUTE_ON_KEY(stratum1_id)
CREATE TABLE cb_imosphere_cdm_531.r140dv3utempresults_606
  AS WITH overallstats   as ( select subject_id  as stratum1_id,gender_concept_id  as stratum2_id,cast(avg(1.0 * count_value)  as float64)  as avg_value,cast(STDDEV(count_value)  as float64)  as stdev_value,min(count_value)  as min_value,max(count_value)  as max_value,COUNT(*)  as total  from cb_imosphere_cdm_531.r140dv3urawdata_606
	 group by  1, 2 ), statsview   as ( select subject_id  as stratum1_id,gender_concept_id  as stratum2_id,count_value as count_value,COUNT(*)  as total,row_number() over (partition by subject_id, gender_concept_id order by count_value)  as rn  from cb_imosphere_cdm_531.r140dv3urawdata_606
   group by  1, 2, 3 ), priorstats   as ( select s.stratum1_id as stratum1_id,s.stratum2_id as stratum2_id,s.count_value as count_value,s.total as total,sum(p.total)  as accumulated  from statsview s
  join statsview p on s.stratum1_id = p.stratum1_id and s.stratum2_id = p.stratum2_id and p.rn <= s.rn
   group by  s.stratum1_id, s.stratum2_id, s.count_value, s.total, s.rn
 )
  SELECT 606 as analysis_id,
  cast(o.stratum1_id as STRING) as stratum1_id,
  cast(o.stratum2_id as STRING) as stratum2_id,
  floor((COUNT(o.total)+99)/100)*100 as count_value,
  o.min_value,
	o.max_value,
	o.avg_value,
	o.stdev_value,
	min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
	min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
	min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
	min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
	min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
 FROM priorstats p
join overallstats o on p.stratum1_id = o.stratum1_id and p.stratum2_id = o.stratum2_id 
 group by  o.stratum1_id, o.stratum2_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
 ;
--HINT DISTRIBUTE_ON_KEY(stratum_1)
CREATE TABLE cb_imosphere_cdm_531.tmpach_dist_606
 AS
SELECT
analysis_id, stratum1_id as stratum_1, stratum2_id as stratum_2, 
cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
FROM
cb_imosphere_cdm_531.r140dv3utempresults_606
;
DELETE FROM cb_imosphere_cdm_531.r140dv3utempresults_606 WHERE True;
drop table cb_imosphere_cdm_531.r140dv3utempresults_606;
DELETE FROM cb_imosphere_cdm_531.r140dv3urawdata_606 WHERE True;
drop table cb_imosphere_cdm_531.r140dv3urawdata_606;


-- 620	Number of procedure occurrence records by condition occurrence start month
--HINT DISTRIBUTE_ON_KEY(stratum_1)
CREATE TABLE cb_imosphere_cdm_531.tmpach_620
 AS WITH rawdata as (
   select EXTRACT(YEAR from po1.procedure_date)*100 + EXTRACT(MONTH from po1.procedure_date) as stratum_1,
    COUNT(po1.person_id) as count_value
   from cb_imosphere_cdm_531.procedure_occurrence po1 inner join 
  cb_imosphere_cdm_531.observation_period op on po1.person_id = op.person_id
  -- only include events that occur during observation period
  where po1.procedure_date <= op.observation_period_end_date and
  po1.procedure_date >= op.observation_period_start_date
   group by  1 )
 SELECT 620 as analysis_id,
  cast(stratum_1 as STRING) as stratum_1,
  cast(null as STRING) as stratum_2,
  cast(null as STRING) as stratum_3,
  cast(null as STRING) as stratum_4,
  cast(null as STRING) as stratum_5,
  count_value
 FROM rawdata;


-- 630	Number of descendant procedure occurrence records,by procedure_concept_id
--HINT DISTRIBUTE_ON_KEY(stratum_1)
CREATE TABLE cb_imosphere_cdm_531.tmpach_630
  AS WITH cte_procedure as (
	 select ca.ancestor_concept_id as concept_id, COUNT(*)
 as drc
	 from cb_imosphere_cdm_531.procedure_occurrence co
		join cb_imosphere_cdm_531.concept_ancestor ca
			on ca.descendant_concept_id = co.procedure_concept_id
	 group by  ca.ancestor_concept_id
 )
  SELECT 630 as analysis_id,
  cast(co.procedure_concept_id as STRING) as stratum_1,
  cast(null as STRING) as stratum_2,
  cast(null as STRING) as stratum_3,
  cast(null as STRING) as stratum_4,
  cast(null as STRING) as stratum_5,
  floor((c.drc+99)/100)*100 as count_value
 FROM cb_imosphere_cdm_531.procedure_occurrence co
	join cte_procedure c
		on c.concept_id = co.procedure_concept_id
 group by  co.procedure_concept_id, c.drc
 ;


-- 700	Number of persons with at least one drug occurrence, by drug_concept_id
--HINT DISTRIBUTE_ON_KEY(stratum_1)
 CREATE TABLE cb_imosphere_cdm_531.tmpach_700
  AS
SELECT
700 as analysis_id, 
	cast(de1.drug_concept_id as STRING) as stratum_1,
	cast(null as STRING) as stratum_2,
	cast(null as STRING) as stratum_3, 
	cast(null as STRING) as stratum_4, 
	cast(null as STRING) as stratum_5,
	floor((COUNT(distinct de1.person_id)+99)/100)*100 as count_value
FROM
cb_imosphere_cdm_531.drug_exposure de1
 group by  de1.drug_concept_id
 ;


-- 701	Number of drug occurrence records, by drug_concept_id
--HINT DISTRIBUTE_ON_KEY(stratum_1)
 CREATE TABLE cb_imosphere_cdm_531.tmpach_701
  AS
SELECT
701 as analysis_id, 
	cast(de1.drug_concept_id as STRING) as stratum_1,
	cast(null as STRING) as stratum_2,
	cast(null as STRING) as stratum_3, 
	cast(null as STRING) as stratum_4, 
	cast(null as STRING) as stratum_5,
	floor((COUNT(de1.person_id)+99)/100)*100 as count_value
FROM
cb_imosphere_cdm_531.drug_exposure de1
 group by  de1.drug_concept_id
 ;


-- 703	Number of distinct drug exposure concepts per person
--HINT DISTRIBUTE_ON_KEY(count_value)
CREATE TABLE cb_imosphere_cdm_531.r140dv3utempresults_703
  AS WITH rawdata  as (select num_drugs  as count_value from (
		 select de1.person_id, COUNT(distinct de1.drug_concept_id) as num_drugs
		 from cb_imosphere_cdm_531.drug_exposure de1 inner join 
  cb_imosphere_cdm_531.observation_period op on de1.person_id = op.person_id
  -- only include events that occur during observation period
  where de1.drug_exposure_start_date <= op.observation_period_end_date and
  IFNULL(de1.drug_exposure_end_date,de1.drug_exposure_start_date) >= op.observation_period_start_date
		 group by  de1.person_id
	 ) t0
), overallstats   as (select cast(avg(1.0 * count_value)  as float64)  as avg_value,cast(STDDEV(count_value)  as float64)  as stdev_value,min(count_value)  as min_value,max(count_value)  as max_value,COUNT(*)  as total from rawdata
), statsview   as ( select count_value as count_value,COUNT(*)  as total,row_number() over (order by count_value)  as rn  from rawdata
   group by  1 ), priorstats   as ( select s.count_value as count_value,s.total as total,sum(p.total)  as accumulated  from statsview s
  join statsview p on p.rn <= s.rn
   group by  s.count_value, s.total, s.rn
 )
  SELECT 703 as analysis_id,
  floor((COUNT(o.total)+99)/100)*100 as count_value,
  o.min_value,
	o.max_value,
	o.avg_value,
	o.stdev_value,
	min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
	min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
	min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
	min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
	min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
 FROM priorstats p
cross join overallstats o
 group by  o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
 ;
--HINT DISTRIBUTE_ON_KEY(count_value)
CREATE TABLE cb_imosphere_cdm_531.tmpach_dist_703
 AS
SELECT
analysis_id, 
cast(null as STRING) as stratum_1, cast(null as STRING) as stratum_2, cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
FROM
cb_imosphere_cdm_531.r140dv3utempresults_703
;
DELETE FROM cb_imosphere_cdm_531.r140dv3utempresults_703 WHERE True;
drop table cb_imosphere_cdm_531.r140dv3utempresults_703;


-- 705	Number of drug occurrence records, by drug_concept_id by drug_type_concept_id
--HINT DISTRIBUTE_ON_KEY(stratum_1)
 CREATE TABLE cb_imosphere_cdm_531.tmpach_705
  AS
SELECT
705 as analysis_id, 
	cast(de1.drug_concept_id as STRING) as stratum_1,
	cast(de1.drug_type_concept_id as STRING) as stratum_2,
	cast(null as STRING) as stratum_3,
	cast(null as STRING) as stratum_4, 
	cast(null as STRING) as stratum_5,
	floor((COUNT(de1.person_id)+99)/100)*100 as count_value
FROM
cb_imosphere_cdm_531.drug_exposure de1
 group by  de1.drug_concept_id, de1.drug_type_concept_id
 ;


-- 706	Distribution of age by drug_concept_id
--HINT DISTRIBUTE_ON_KEY(subject_id)
CREATE TABLE cb_imosphere_cdm_531.r140dv3urawdata_706
 AS
SELECT
de1.drug_concept_id as subject_id,
  p1.gender_concept_id,
	de1.drug_start_year - p1.year_of_birth as count_value
FROM
cb_imosphere_cdm_531.person p1
inner join
(
	 select person_id, drug_concept_id, min(EXTRACT(YEAR from drug_exposure_start_date)) as drug_start_year
	 from cb_imosphere_cdm_531.drug_exposure
	 group by  1, 2 ) de1 on p1.person_id = de1.person_id
;
--HINT DISTRIBUTE_ON_KEY(stratum1_id)
CREATE TABLE cb_imosphere_cdm_531.r140dv3utempresults_706
  AS WITH overallstats   as ( select subject_id  as stratum1_id,gender_concept_id  as stratum2_id,cast(avg(1.0 * count_value)  as float64)  as avg_value,cast(STDDEV(count_value)  as float64)  as stdev_value,min(count_value)  as min_value,max(count_value)  as max_value,COUNT(*)  as total  from cb_imosphere_cdm_531.r140dv3urawdata_706
	 group by  1, 2 ), statsview   as ( select subject_id  as stratum1_id,gender_concept_id  as stratum2_id,count_value as count_value,COUNT(*)  as total,row_number() over (partition by subject_id, gender_concept_id order by count_value)  as rn  from cb_imosphere_cdm_531.r140dv3urawdata_706
   group by  1, 2, 3 ), priorstats   as ( select s.stratum1_id as stratum1_id,s.stratum2_id as stratum2_id,s.count_value as count_value,s.total as total,sum(p.total)  as accumulated  from statsview s
  join statsview p on s.stratum1_id = p.stratum1_id and s.stratum2_id = p.stratum2_id and p.rn <= s.rn
   group by  s.stratum1_id, s.stratum2_id, s.count_value, s.total, s.rn
 )
  SELECT 706 as analysis_id,
  cast(o.stratum1_id as STRING) as stratum1_id,
  cast(o.stratum2_id as STRING) as stratum2_id,
  floor((COUNT(o.total)+99)/100)*100 as count_value,
  o.min_value,
	o.max_value,
	o.avg_value,
	o.stdev_value,
	min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
	min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
	min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
	min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
	min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
 FROM priorstats p
join overallstats o on p.stratum1_id = o.stratum1_id and p.stratum2_id = o.stratum2_id 
 group by  o.stratum1_id, o.stratum2_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
 ;
--HINT DISTRIBUTE_ON_KEY(stratum_1)
CREATE TABLE cb_imosphere_cdm_531.tmpach_dist_706
 AS
SELECT
analysis_id, stratum1_id as stratum_1, stratum2_id as stratum_2, 
cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
FROM
cb_imosphere_cdm_531.r140dv3utempresults_706
;
DELETE FROM cb_imosphere_cdm_531.r140dv3urawdata_706 WHERE True;
drop table cb_imosphere_cdm_531.r140dv3urawdata_706;
DELETE FROM cb_imosphere_cdm_531.r140dv3utempresults_706 WHERE True;
drop table cb_imosphere_cdm_531.r140dv3utempresults_706;


-- 715	Distribution of days_supply by drug_concept_id
--HINT DISTRIBUTE_ON_KEY(stratum_id)
CREATE TABLE cb_imosphere_cdm_531.r140dv3utempresults_715
  AS WITH rawdata  as (select drug_concept_id  as stratum_id,days_supply  as count_value from cb_imosphere_cdm_531.drug_exposure 
	where days_supply is not null
), overallstats   as ( select stratum_id as stratum_id,cast(avg(1.0 * count_value)  as float64)  as avg_value,cast(STDDEV(count_value)  as float64)  as stdev_value,min(count_value)  as min_value,max(count_value)  as max_value,COUNT(*)  as total  from rawdata
	 group by  1 ), statsview   as ( select stratum_id as stratum_id,count_value as count_value,COUNT(*)  as total,row_number() over (partition by stratum_id order by count_value)  as rn  from rawdata
   group by  1, 2 ), priorstats   as ( select s.stratum_id as stratum_id,s.count_value as count_value,s.total as total,sum(p.total)  as accumulated  from statsview s
  join statsview p on s.stratum_id = p.stratum_id and p.rn <= s.rn
   group by  s.stratum_id, s.count_value, s.total, s.rn
 )
  SELECT 715 as analysis_id,
  cast(o.stratum_id as STRING) as stratum_id,
  floor((COUNT(o.total)+99)/100)*100 as count_value,
  o.min_value,
	o.max_value,
	o.avg_value,
	o.stdev_value,
	min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
	min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
	min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
	min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
	min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
 FROM priorstats p
join overallstats o on p.stratum_id = o.stratum_id
 group by  o.stratum_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
 ;
--HINT DISTRIBUTE_ON_KEY(stratum_1)
CREATE TABLE cb_imosphere_cdm_531.tmpach_dist_715
 AS
SELECT
analysis_id, stratum_id as stratum_1, 
cast(null as STRING) as stratum_2, cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
FROM
cb_imosphere_cdm_531.r140dv3utempresults_715
;
DELETE FROM cb_imosphere_cdm_531.r140dv3utempresults_715 WHERE True;
drop table cb_imosphere_cdm_531.r140dv3utempresults_715;


-- 716	Distribution of refills by drug_concept_id
--HINT DISTRIBUTE_ON_KEY(stratum_id)
CREATE TABLE cb_imosphere_cdm_531.r140dv3utempresults_716
  AS WITH rawdata  as (select drug_concept_id  as stratum_id,refills  as count_value from cb_imosphere_cdm_531.drug_exposure 
	where refills is not null
), overallstats   as ( select stratum_id as stratum_id,cast(avg(1.0 * count_value)  as float64)  as avg_value,cast(STDDEV(count_value)  as float64)  as stdev_value,min(count_value)  as min_value,max(count_value)  as max_value,COUNT(*)  as total  from rawdata
	 group by  1 ), statsview   as ( select stratum_id as stratum_id,count_value as count_value,COUNT(*)  as total,row_number() over (partition by stratum_id order by count_value)  as rn  from rawdata
   group by  1, 2 ), priorstats   as ( select s.stratum_id as stratum_id,s.count_value as count_value,s.total as total,sum(p.total)  as accumulated  from statsview s
  join statsview p on s.stratum_id = p.stratum_id and p.rn <= s.rn
   group by  s.stratum_id, s.count_value, s.total, s.rn
 )
  SELECT 716 as analysis_id,
  cast(o.stratum_id as STRING) as stratum_id,
  floor((COUNT(o.total)+99)/100)*100 as count_value,
  o.min_value,
	o.max_value,
	o.avg_value,
	o.stdev_value,
	min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
	min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
	min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
	min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
	min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
 FROM priorstats p
join overallstats o on p.stratum_id = o.stratum_id
 group by  o.stratum_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
 ;
--HINT DISTRIBUTE_ON_KEY(stratum_1)
CREATE TABLE cb_imosphere_cdm_531.tmpach_dist_716
 AS
SELECT
analysis_id, stratum_id as stratum_1, 
cast(null as STRING) as stratum_2, cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
FROM
cb_imosphere_cdm_531.r140dv3utempresults_716
;
DELETE FROM cb_imosphere_cdm_531.r140dv3utempresults_716 WHERE True;
drop table cb_imosphere_cdm_531.r140dv3utempresults_716;


-- 717	Distribution of quantity by drug_concept_id
--HINT DISTRIBUTE_ON_KEY(stratum_id)
CREATE TABLE cb_imosphere_cdm_531.r140dv3utempresults_717
  AS WITH rawdata  as (select drug_concept_id  as stratum_id,cast(quantity  as float64)  as count_value from cb_imosphere_cdm_531.drug_exposure 
	where quantity is not null
), overallstats   as ( select stratum_id as stratum_id,cast(avg(1.0 * count_value)  as float64)  as avg_value,cast(STDDEV(count_value)  as float64)  as stdev_value,min(count_value)  as min_value,max(count_value)  as max_value,COUNT(*)  as total  from rawdata
	 group by  1 ), statsview   as ( select stratum_id as stratum_id,count_value as count_value,COUNT(*)  as total,row_number() over (order by count_value)  as rn  from rawdata
   group by  1, 2 ), priorstats   as ( select s.stratum_id as stratum_id,s.count_value as count_value,s.total as total,sum(p.total)  as accumulated  from statsview s
  join statsview p on s.stratum_id = p.stratum_id and p.rn <= s.rn
   group by  s.stratum_id, s.count_value, s.total, s.rn
 )
  SELECT 717 as analysis_id,
  cast(o.stratum_id as STRING) as stratum_id,
  floor((COUNT(o.total)+99)/100)*100 as count_value,
  o.min_value,
	o.max_value,
	o.avg_value,
	o.stdev_value,
	min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
	min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
	min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
	min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
	min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
 FROM priorstats p
join overallstats o on p.stratum_id = o.stratum_id
 group by  o.stratum_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
 ;
--HINT DISTRIBUTE_ON_KEY(stratum_1)
CREATE TABLE cb_imosphere_cdm_531.tmpach_dist_717
 AS
SELECT
analysis_id, stratum_id as stratum_1, 
cast(null as STRING) as stratum_2, cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
FROM
cb_imosphere_cdm_531.r140dv3utempresults_717
;
DELETE FROM cb_imosphere_cdm_531.r140dv3utempresults_717 WHERE True;
drop table cb_imosphere_cdm_531.r140dv3utempresults_717;


-- 720	Number of drug exposure records by condition occurrence start month
--HINT DISTRIBUTE_ON_KEY(stratum_1)
CREATE TABLE cb_imosphere_cdm_531.tmpach_720
 AS WITH rawdata as (
   select EXTRACT(YEAR from de1.drug_exposure_start_date)*100 + EXTRACT(MONTH from de1.drug_exposure_start_date) as stratum_1,
    COUNT(de1.person_id) as count_value
   from cb_imosphere_cdm_531.drug_exposure de1 inner join 
  cb_imosphere_cdm_531.observation_period op on de1.person_id = op.person_id
  -- only include events that occur during observation period
  where de1.drug_exposure_start_date <= op.observation_period_end_date and
  de1.drug_exposure_start_date >= op.observation_period_start_date
   group by  1 )
 SELECT 720 as analysis_id,
  cast(stratum_1 as STRING) as stratum_1,
  cast(null as STRING) as stratum_2,
  cast(null as STRING) as stratum_3,
  cast(null as STRING) as stratum_4,
  cast(null as STRING) as stratum_5,
  count_value
 FROM rawdata;

-- 730	Number of descendant drug exposure records,by procedure_concept_id
--HINT DISTRIBUTE_ON_KEY(stratum_1)
CREATE TABLE cb_imosphere_cdm_531.tmpach_730
  AS WITH cte_procedure as (
	 select ca.ancestor_concept_id as concept_id, COUNT(*)
 as drc
	 from cb_imosphere_cdm_531.drug_exposure co
		join cb_imosphere_cdm_531.concept_ancestor ca
			on ca.descendant_concept_id = co.drug_concept_id
	 group by  ca.ancestor_concept_id
 )
  SELECT 730 as analysis_id,
  cast(co.drug_concept_id as STRING) as stratum_1,
  cast(null as STRING) as stratum_2,
  cast(null as STRING) as stratum_3,
  cast(null as STRING) as stratum_4,
  cast(null as STRING) as stratum_5,
  floor((c.drc+99)/100)*100 as count_value
 FROM cb_imosphere_cdm_531.drug_exposure co
	join cte_procedure c
		on c.concept_id = co.drug_concept_id
 group by  co.drug_concept_id, c.drc
 ;


-- 800	Number of persons with at least one observation occurrence, by observation_concept_id
--HINT DISTRIBUTE_ON_KEY(stratum_1)
 CREATE TABLE cb_imosphere_cdm_531.tmpach_800
  AS
SELECT
800 as analysis_id, 
	cast(o1.observation_concept_id as STRING) as stratum_1,
	cast(null as STRING) as stratum_2,
	cast(null as STRING) as stratum_3, 
	cast(null as STRING) as stratum_4, 
	cast(null as STRING) as stratum_5,
	floor((COUNT(distinct o1.person_id)+99)/100)*100 as count_value
FROM
cb_imosphere_cdm_531.observation o1
 group by  o1.observation_concept_id
 ;


-- 801	Number of observation occurrence records, by observation_concept_id
--HINT DISTRIBUTE_ON_KEY(stratum_1)
 CREATE TABLE cb_imosphere_cdm_531.tmpach_801
  AS
SELECT
801 as analysis_id, 
	cast(o1.observation_concept_id as STRING) as stratum_1,
	cast(null as STRING) as stratum_2,
	cast(null as STRING) as stratum_3, 
	cast(null as STRING) as stratum_4, 
	cast(null as STRING) as stratum_5,
	floor((COUNT(o1.person_id)+99)/100)*100 as count_value
FROM
cb_imosphere_cdm_531.observation o1
 group by  o1.observation_concept_id
 ;


-- 803	Number of distinct observation occurrence concepts per person
--HINT DISTRIBUTE_ON_KEY(count_value)
CREATE TABLE cb_imosphere_cdm_531.r140dv3utempresults_803
  AS WITH rawdata  as (select num_observations  as count_value from (
  	 select o1.person_id, COUNT(distinct o1.observation_concept_id) as num_observations
  	 from cb_imosphere_cdm_531.observation o1 inner join cb_imosphere_cdm_531.observation_period op 
  	  on o1.person_id = op.person_id
    -- only include events that occur during observation period
    where o1.observation_date <= op.observation_period_end_date and
      o1.observation_date >= op.observation_period_start_date
     group by  o1.person_id
	 ) t0
), overallstats   as (select cast(avg(1.0 * count_value)  as float64)  as avg_value,cast(STDDEV(count_value)  as float64)  as stdev_value,min(count_value)  as min_value,max(count_value)  as max_value,COUNT(*)  as total from rawdata
), statsview   as ( select count_value as count_value,COUNT(*)  as total,row_number() over (order by count_value)  as rn  from rawdata
   group by  1 ), priorstats   as ( select s.count_value as count_value,s.total as total,sum(p.total)  as accumulated  from statsview s
  join statsview p on p.rn <= s.rn
   group by  s.count_value, s.total, s.rn
 )
  SELECT 803 as analysis_id,
  floor((COUNT(o.total)+99)/100)*100 as count_value,
  o.min_value,
	o.max_value,
	o.avg_value,
	o.stdev_value,
	min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
	min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
	min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
	min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
	min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
 FROM priorstats p
cross join overallstats o
 group by  o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
 ;
--HINT DISTRIBUTE_ON_KEY(count_value)
CREATE TABLE cb_imosphere_cdm_531.tmpach_dist_803
 AS
SELECT
analysis_id, 
cast(null as STRING) as stratum_1, cast(null as STRING) as stratum_2, cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
FROM
cb_imosphere_cdm_531.r140dv3utempresults_803
;
DELETE FROM cb_imosphere_cdm_531.r140dv3utempresults_803 WHERE True;
drop table cb_imosphere_cdm_531.r140dv3utempresults_803;


-- 805	Number of observation occurrence records, by observation_concept_id by observation_type_concept_id
--HINT DISTRIBUTE_ON_KEY(stratum_1)
 CREATE TABLE cb_imosphere_cdm_531.tmpach_805
  AS
SELECT
805 as analysis_id, 
	cast(o1.observation_concept_id as STRING) as stratum_1,
	cast(o1.observation_type_concept_id as STRING) as stratum_2,
	cast(null as STRING) as stratum_3,
	cast(null as STRING) as stratum_4,
	cast(null as STRING) as stratum_5,
	floor((COUNT(o1.person_id)+99)/100)*100 as count_value
FROM
cb_imosphere_cdm_531.observation o1
 group by  o1.observation_concept_id, o1.observation_type_concept_id
 ;


-- 806	Distribution of age by observation_concept_id
--HINT DISTRIBUTE_ON_KEY(subject_id)
CREATE TABLE cb_imosphere_cdm_531.r140dv3urawdata_806
 AS
SELECT
o1.observation_concept_id as subject_id,
  p1.gender_concept_id,
	o1.observation_start_year - p1.year_of_birth as count_value
FROM
cb_imosphere_cdm_531.person p1
inner join
(
	 select person_id, observation_concept_id, min(EXTRACT(YEAR from observation_date)) as observation_start_year
	 from cb_imosphere_cdm_531.observation
	 group by  1, 2 ) o1
on p1.person_id = o1.person_id
;
--HINT DISTRIBUTE_ON_KEY(stratum1_id)
CREATE TABLE cb_imosphere_cdm_531.r140dv3utempresults_806
  AS WITH overallstats   as ( select subject_id  as stratum1_id,gender_concept_id  as stratum2_id,cast(avg(1.0 * count_value)  as float64)  as avg_value,cast(STDDEV(count_value)  as float64)  as stdev_value,min(count_value)  as min_value,max(count_value)  as max_value,COUNT(*)  as total  from cb_imosphere_cdm_531.r140dv3urawdata_806
	 group by  1, 2 ), statsview   as ( select subject_id  as stratum1_id,gender_concept_id  as stratum2_id,count_value as count_value,COUNT(*)  as total,row_number() over (partition by subject_id, gender_concept_id order by count_value)  as rn  from cb_imosphere_cdm_531.r140dv3urawdata_806
   group by  1, 2, 3 ), priorstats   as ( select s.stratum1_id as stratum1_id,s.stratum2_id as stratum2_id,s.count_value as count_value,s.total as total,sum(p.total)  as accumulated  from statsview s
  join statsview p on s.stratum1_id = p.stratum1_id and s.stratum2_id = p.stratum2_id and p.rn <= s.rn
   group by  s.stratum1_id, s.stratum2_id, s.count_value, s.total, s.rn
 )
  SELECT 806 as analysis_id,
  cast(o.stratum1_id as STRING) as stratum1_id,
  cast(o.stratum2_id as STRING) as stratum2_id,
  floor((COUNT(o.total)+99)/100)*100 as count_value,
  o.min_value,
	o.max_value,
	o.avg_value,
	o.stdev_value,
	min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
	min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
	min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
	min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
	min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
 FROM priorstats p
join overallstats o on p.stratum1_id = o.stratum1_id and p.stratum2_id = o.stratum2_id 
 group by  o.stratum1_id, o.stratum2_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
 ;
--HINT DISTRIBUTE_ON_KEY(stratum_1)
CREATE TABLE cb_imosphere_cdm_531.tmpach_dist_806
 AS
SELECT
analysis_id, stratum1_id as stratum_1, stratum2_id as stratum_2, 
cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
FROM
cb_imosphere_cdm_531.r140dv3utempresults_806
;
DELETE FROM cb_imosphere_cdm_531.r140dv3urawdata_806 WHERE True;
drop table cb_imosphere_cdm_531.r140dv3urawdata_806;
DELETE FROM cb_imosphere_cdm_531.r140dv3utempresults_806 WHERE True;
drop table cb_imosphere_cdm_531.r140dv3utempresults_806;


--HINT DISTRIBUTE_ON_KEY(stratum1_id)
 CREATE TABLE cb_imosphere_cdm_531.r140dv3uoverallstats_815
    AS
SELECT
subject_id as stratum1_id,
  unit_concept_id as stratum2_id,
  cast(avg(1.0 * count_value)  as float64) as avg_value,
  cast(STDDEV(count_value)  as float64) as stdev_value,
  min(count_value) as min_value,
  max(count_value) as max_value,
  COUNT(*) as total
FROM
(
    select observation_concept_id as subject_id, 
  	unit_concept_id,
  	cast(value_as_number  as float64) as count_value
    from cb_imosphere_cdm_531.observation o1
    where o1.unit_concept_id is not null
  	  and o1.value_as_number is not null
  ) a
	 group by  1, 2 ;
--HINT DISTRIBUTE_ON_KEY(stratum1_id)
 CREATE TABLE cb_imosphere_cdm_531.r140dv3ustatsview_815
  AS
SELECT
subject_id as stratum1_id, unit_concept_id as stratum2_id, count_value, COUNT(*) as total, row_number() over (partition by subject_id, unit_concept_id order by count_value) as rn
FROM
(
  select observation_concept_id as subject_id, 
	unit_concept_id,
	cast(value_as_number  as float64) as count_value
  from cb_imosphere_cdm_531.observation o1
  where o1.unit_concept_id is not null
	  and o1.value_as_number is not null
) a
 group by  1, 2, 3 ;
--HINT DISTRIBUTE_ON_KEY(stratum1_id)
CREATE TABLE cb_imosphere_cdm_531.r140dv3utempresults_815
  AS WITH priorstats   as ( select s.stratum1_id as stratum1_id,s.stratum2_id as stratum2_id,s.count_value as count_value,s.total as total,sum(p.total)  as accumulated  from cb_imosphere_cdm_531.r140dv3ustatsview_815 s
  join cb_imosphere_cdm_531.r140dv3ustatsview_815 p on s.stratum1_id = p.stratum1_id and s.stratum2_id = p.stratum2_id and p.rn <= s.rn
   group by  s.stratum1_id, s.stratum2_id, s.count_value, s.total, s.rn
 )
  SELECT 815 as analysis_id,
  cast(o.stratum1_id as STRING) as stratum1_id,
  cast(o.stratum2_id as STRING) as stratum2_id,
  floor((COUNT(o.total)+99)/100)*100 as count_value,
  o.min_value,
	o.max_value,
	o.avg_value,
	o.stdev_value,
	min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
	min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
	min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
	min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
	min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
 FROM priorstats p
join cb_imosphere_cdm_531.r140dv3uoverallstats_815 o on p.stratum1_id = o.stratum1_id and p.stratum2_id = o.stratum2_id 
 group by  o.stratum1_id, o.stratum2_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
 ;
--HINT DISTRIBUTE_ON_KEY(stratum_1)
CREATE TABLE cb_imosphere_cdm_531.tmpach_dist_815
 AS
SELECT
analysis_id, stratum1_id as stratum_1, stratum2_id as stratum_2, 
cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
FROM
cb_imosphere_cdm_531.r140dv3utempresults_815
;
DELETE FROM cb_imosphere_cdm_531.r140dv3uoverallstats_815 WHERE True;
drop table cb_imosphere_cdm_531.r140dv3uoverallstats_815;
DELETE FROM cb_imosphere_cdm_531.r140dv3ustatsview_815 WHERE True;
drop table cb_imosphere_cdm_531.r140dv3ustatsview_815;
DELETE FROM cb_imosphere_cdm_531.r140dv3utempresults_815 WHERE True;
drop table cb_imosphere_cdm_531.r140dv3utempresults_815;


-- 820	Number of observation records by condition occurrence start month
--HINT DISTRIBUTE_ON_KEY(stratum_1)
CREATE TABLE cb_imosphere_cdm_531.tmpach_820
 AS WITH rawdata as (
   select EXTRACT(YEAR from o1.observation_date)*100 + EXTRACT(MONTH from o1.observation_date) as stratum_1,
    COUNT(o1.person_id) as count_value
   from cb_imosphere_cdm_531.observation o1 inner join 
  cb_imosphere_cdm_531.observation_period op on o1.person_id = op.person_id
  -- only include events that occur during observation period
  where o1.observation_date <= op.observation_period_end_date and
  o1.observation_date >= op.observation_period_start_date
   group by  1 )
 SELECT 820 as analysis_id,
  cast(stratum_1 as STRING) as stratum_1,
  cast(null as STRING) as stratum_2,
  cast(null as STRING) as stratum_3,
  cast(null as STRING) as stratum_4,
  cast(null as STRING) as stratum_5,
  count_value
 FROM rawdata;


-- 830	Number of descendant observation occurrence records,by procedure_concept_id
--HINT DISTRIBUTE_ON_KEY(stratum_1)
CREATE TABLE cb_imosphere_cdm_531.tmpach_830
  AS WITH cte_procedure as (
	 select ca.ancestor_concept_id as concept_id, COUNT(*)
 as drc
	 from cb_imosphere_cdm_531.observation co
		join cb_imosphere_cdm_531.concept_ancestor ca
			on ca.descendant_concept_id = co.observation_concept_id
	 group by  ca.ancestor_concept_id
 )
  SELECT 830 as analysis_id,
  cast(co.observation_concept_id as STRING) as stratum_1,
  cast(null as STRING) as stratum_2,
  cast(null as STRING) as stratum_3,
  cast(null as STRING) as stratum_4,
  cast(null as STRING) as stratum_5,
  floor((c.drc+99)/100)*100 as count_value
 FROM cb_imosphere_cdm_531.observation co
	join cte_procedure c
		on c.concept_id = co.observation_concept_id
 group by  co.observation_concept_id, c.drc
 ;


-- 901	Number of drug occurrence records, by drug_concept_id
--HINT DISTRIBUTE_ON_KEY(stratum_1)
 CREATE TABLE cb_imosphere_cdm_531.tmpach_901
  AS
SELECT
901 as analysis_id, 
	cast(de1.drug_concept_id as STRING) as stratum_1,
	cast(null as STRING) as stratum_2,
	cast(null as STRING) as stratum_3, 
	cast(null as STRING) as stratum_4, 
	cast(null as STRING) as stratum_5,
	floor((COUNT(drug_concept_id)+99)/100)*100 as count_value
FROM
cb_imosphere_cdm_531.drug_era de1
 group by  de1.drug_concept_id
 ;


-- 920	Number of drug era records by drug era start month
--HINT DISTRIBUTE_ON_KEY(stratum_1)
CREATE TABLE cb_imosphere_cdm_531.tmpach_920
 AS WITH rawdata as (
   select EXTRACT(YEAR from de1.drug_era_start_date)*100 + EXTRACT(MONTH from de1.drug_era_start_date) as stratum_1,
    COUNT(de1.person_id) as count_value
   from cb_imosphere_cdm_531.drug_era de1 inner join 
  cb_imosphere_cdm_531.observation_period op on de1.person_id = op.person_id
  -- only include events that occur during observation period
  where de1.drug_era_start_date <= op.observation_period_end_date and
  de1.drug_era_start_date >= op.observation_period_start_date
   group by  1 )
 SELECT 920 as analysis_id,
  cast(stratum_1 as STRING) as stratum_1,
  cast(null as STRING) as stratum_2,
  cast(null as STRING) as stratum_3,
  cast(null as STRING) as stratum_4,
  cast(null as STRING) as stratum_5,
  count_value
 FROM rawdata;


-- 1001	Number of condition occurrence records, by condition_concept_id
--HINT DISTRIBUTE_ON_KEY(stratum_1)
 CREATE TABLE cb_imosphere_cdm_531.tmpach_1001
  AS
SELECT
1001 as analysis_id, 
	cast(ce1.condition_concept_id as STRING) as stratum_1,
	cast(null as STRING) as stratum_2,
	cast(null as STRING) as stratum_3,
	cast(null as STRING) as stratum_4,
	cast(null as STRING) as stratum_5,
	floor((COUNT(ce1.person_id)+99)/100)*100 as count_value
FROM
cb_imosphere_cdm_531.condition_era ce1
 group by  ce1.condition_concept_id
 ;


-- 1020	Number of condition era records by condition era start month
--HINT DISTRIBUTE_ON_KEY(stratum_1)
CREATE TABLE cb_imosphere_cdm_531.tmpach_1020
 AS WITH rawdata as (
   select EXTRACT(YEAR from ce1.condition_era_start_date)*100 + EXTRACT(MONTH from ce1.condition_era_start_date) as stratum_1,
    COUNT(ce1.person_id) as count_value
   from cb_imosphere_cdm_531.condition_era ce1 inner join 
  cb_imosphere_cdm_531.observation_period op on ce1.person_id = op.person_id
  -- only include events that occur during observation period
  where ce1.condition_era_start_date <= op.observation_period_end_date and
  ce1.condition_era_start_date >= op.observation_period_start_date
   group by  1 )
 SELECT 1020 as analysis_id,
  cast(stratum_1 as STRING) as stratum_1,
  cast(null as STRING) as stratum_2,
  cast(null as STRING) as stratum_3,
  cast(null as STRING) as stratum_4,
  cast(null as STRING) as stratum_5,
  count_value
 FROM rawdata;

-- 1800	Number of persons with at least one measurement occurrence, by measurement_concept_id
--HINT DISTRIBUTE_ON_KEY(stratum_1)
 CREATE TABLE cb_imosphere_cdm_531.tmpach_1800
  AS
SELECT
1800 as analysis_id, 
	cast(m.measurement_concept_id as STRING) as stratum_1,
	cast(null as STRING) as stratum_2,
	cast(null as STRING) as stratum_3,
	cast(null as STRING) as stratum_4,
	cast(null as STRING) as stratum_5,
	floor((COUNT(distinct m.person_id)+99)/100)*100 as count_value
FROM
cb_imosphere_cdm_531.measurement m
 group by  m.measurement_concept_id
 ;


-- 1801	Number of measurement occurrence records, by measurement_concept_id
--HINT DISTRIBUTE_ON_KEY(stratum_1)
 CREATE TABLE cb_imosphere_cdm_531.tmpach_1801
  AS
SELECT
1801 as analysis_id, 
	cast(m.measurement_concept_id as STRING) as stratum_1,
	cast(null as STRING)  as stratum_2,
	cast(null as STRING) as stratum_3,
	cast(null as STRING) as stratum_4,
	cast(null as STRING) as stratum_5,
	floor((COUNT(m.person_id)+99)/100)*100 as count_value
FROM
cb_imosphere_cdm_531.measurement m
 group by  m.measurement_concept_id
 ;


-- 1803	Number of distinct measurement occurrence concepts per person
--HINT DISTRIBUTE_ON_KEY(count_value)
CREATE TABLE cb_imosphere_cdm_531.r140dv3utempresults_1803
  AS WITH rawdata  as (select num_measurements  as count_value from (
  	 select m.person_id, COUNT(distinct m.measurement_concept_id) as num_measurements
  	 from cb_imosphere_cdm_531.measurement m
  	 group by  m.person_id
	 ) t0
), overallstats   as (select cast(avg(1.0 * count_value)  as float64)  as avg_value,cast(STDDEV(count_value)  as float64)  as stdev_value,min(count_value)  as min_value,max(count_value)  as max_value,COUNT(*)  as total from rawdata
), statsview   as ( select count_value as count_value,COUNT(*)  as total,row_number() over (order by count_value)  as rn  from rawdata
   group by  1 ), priorstats   as ( select s.count_value as count_value,s.total as total,sum(p.total)  as accumulated  from statsview s
  join statsview p on p.rn <= s.rn
   group by  s.count_value, s.total, s.rn
 )
  SELECT 1803 as analysis_id,
  floor((COUNT(o.total)+99)/100)*100 as count_value,
  o.min_value,
	o.max_value,
	o.avg_value,
	o.stdev_value,
	min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
	min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
	min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
	min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
	min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
 FROM priorstats p
cross join overallstats o
 group by  o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
 ;
--HINT DISTRIBUTE_ON_KEY(count_value)
CREATE TABLE cb_imosphere_cdm_531.tmpach_dist_1803
 AS
SELECT
analysis_id, 
cast(null as STRING) as stratum_1, cast(null as STRING) as stratum_2, cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
FROM
cb_imosphere_cdm_531.r140dv3utempresults_1803
;
DELETE FROM cb_imosphere_cdm_531.r140dv3utempresults_1803 WHERE True;
drop table cb_imosphere_cdm_531.r140dv3utempresults_1803;


-- 1805	Number of measurement records, by measurement_concept_id by measurement_type_concept_id
--HINT DISTRIBUTE_ON_KEY(stratum_1)
 CREATE TABLE cb_imosphere_cdm_531.tmpach_1805
  AS
SELECT
1805 as analysis_id, 
	cast(m.measurement_concept_id as STRING) as stratum_1,
	cast(m.measurement_type_concept_id as STRING) as stratum_2,
	cast(null as STRING) as stratum_3,
	cast(null as STRING) as stratum_4,
	cast(null as STRING) as stratum_5,
	floor((COUNT(m.person_id)+99)/100)*100 as count_value
FROM
cb_imosphere_cdm_531.measurement m
 group by  m.measurement_concept_id, m.measurement_type_concept_id
 ;


-- 1806	Distribution of age by measurement_concept_id
--HINT DISTRIBUTE_ON_KEY(subject_id)
CREATE TABLE cb_imosphere_cdm_531.r140dv3urawdata_1806
 AS
SELECT
o1.measurement_concept_id as subject_id,
  p1.gender_concept_id,
	o1.measurement_start_year - p1.year_of_birth as count_value
FROM
cb_imosphere_cdm_531.person p1
inner join
(
	 select person_id, measurement_concept_id, min(EXTRACT(YEAR from measurement_date)) as measurement_start_year
	 from cb_imosphere_cdm_531.measurement
	 group by  1, 2 ) o1
on p1.person_id = o1.person_id
;
--HINT DISTRIBUTE_ON_KEY(stratum1_id)
CREATE TABLE cb_imosphere_cdm_531.r140dv3utempresults_1806
  AS WITH overallstats   as ( select subject_id  as stratum1_id,gender_concept_id  as stratum2_id,cast(avg(1.0 * count_value)  as float64)  as avg_value,cast(STDDEV(count_value)  as float64)  as stdev_value,min(count_value)  as min_value,max(count_value)  as max_value,COUNT(*)  as total  from cb_imosphere_cdm_531.r140dv3urawdata_1806
	 group by  1, 2 ), statsview   as ( select subject_id  as stratum1_id,gender_concept_id  as stratum2_id,count_value as count_value,COUNT(*)  as total,row_number() over (partition by subject_id, gender_concept_id order by count_value)  as rn  from cb_imosphere_cdm_531.r140dv3urawdata_1806
   group by  1, 2, 3 ), priorstats   as ( select s.stratum1_id as stratum1_id,s.stratum2_id as stratum2_id,s.count_value as count_value,s.total as total,sum(p.total)  as accumulated  from statsview s
  join statsview p on s.stratum1_id = p.stratum1_id and s.stratum2_id = p.stratum2_id and p.rn <= s.rn
   group by  s.stratum1_id, s.stratum2_id, s.count_value, s.total, s.rn
 )
  SELECT 1806 as analysis_id,
  cast(o.stratum1_id as STRING) as stratum1_id,
  cast(o.stratum2_id as STRING) as stratum2_id,
  floor((COUNT(o.total)+99)/100)*100 as count_value,
  o.min_value,
	o.max_value,
	o.avg_value,
	o.stdev_value,
	min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
	min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
	min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
	min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
	min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
 FROM priorstats p
join overallstats o on p.stratum1_id = o.stratum1_id and p.stratum2_id = o.stratum2_id 
 group by  o.stratum1_id, o.stratum2_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
 ;
--HINT DISTRIBUTE_ON_KEY(stratum_1)
CREATE TABLE cb_imosphere_cdm_531.tmpach_dist_1806
 AS
SELECT
analysis_id, stratum1_id as stratum_1, stratum2_id as stratum_2, 
cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
FROM
cb_imosphere_cdm_531.r140dv3utempresults_1806
;
DELETE FROM cb_imosphere_cdm_531.r140dv3urawdata_1806 WHERE True;
drop table cb_imosphere_cdm_531.r140dv3urawdata_1806;
DELETE FROM cb_imosphere_cdm_531.r140dv3utempresults_1806 WHERE True;
drop table cb_imosphere_cdm_531.r140dv3utempresults_1806;


-- 1815  Distribution of numeric values, by measurement_concept_id and unit_concept_id
--HINT DISTRIBUTE_ON_KEY(stratum1_id)
 CREATE TABLE cb_imosphere_cdm_531.r140dv3ustatsview_1815
  AS
SELECT
subject_id as stratum1_id, unit_concept_id as stratum2_id, count_value, COUNT(*) as total, row_number() over (partition by subject_id, unit_concept_id order by count_value) as rn
FROM
(
  select measurement_concept_id as subject_id, 
	unit_concept_id,
	cast(value_as_number  as float64) as count_value
  from cb_imosphere_cdm_531.measurement m
  where m.unit_concept_id is not null
	and m.value_as_number is not null
) a
 group by  1, 2, 3 ;
--HINT DISTRIBUTE_ON_KEY(stratum1_id)
 CREATE TABLE cb_imosphere_cdm_531.r140dv3utempresults_1815
  AS
SELECT
1815 as analysis_id,
  cast(o.stratum1_id as STRING) as stratum1_id,
  cast(o.stratum2_id as STRING) as stratum2_id,
  floor((COUNT(o.total)+99)/100)*100 as count_value,
  o.min_value,
	o.max_value,
	o.avg_value,
	o.stdev_value,
	min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
	min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
	min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
	min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
	min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
FROM
(
   select s.stratum1_id, s.stratum2_id, s.count_value, s.total, sum(p.total) as accumulated
   from cb_imosphere_cdm_531.r140dv3ustatsview_1815 s
  join cb_imosphere_cdm_531.r140dv3ustatsview_1815 p on s.stratum1_id = p.stratum1_id and s.stratum2_id = p.stratum2_id and p.rn <= s.rn
   group by  s.stratum1_id, s.stratum2_id, s.count_value, s.total, s.rn
 ) p
join 
(
	 select subject_id as stratum1_id,
	  unit_concept_id as stratum2_id,
	  cast(avg(1.0 * count_value)  as float64) as avg_value,
	  cast(STDDEV(count_value)  as float64) as stdev_value,
	  min(count_value) as min_value,
	  max(count_value) as max_value,
	  COUNT(*) as total
	 from (
	  select measurement_concept_id as subject_id, 
		unit_concept_id,
		cast(value_as_number  as float64) as count_value
	  from cb_imosphere_cdm_531.measurement m
	  where m.unit_concept_id is not null
		and m.value_as_number is not null
	) a
	 group by  1, 2 ) o on p.stratum1_id = o.stratum1_id and p.stratum2_id = o.stratum2_id 
 group by  o.stratum1_id, o.stratum2_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
 ;
--HINT DISTRIBUTE_ON_KEY(stratum_1)
CREATE TABLE cb_imosphere_cdm_531.tmpach_dist_1815
 AS
SELECT
analysis_id, stratum1_id as stratum_1, stratum2_id as stratum_2, 
cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
FROM
cb_imosphere_cdm_531.r140dv3utempresults_1815
;
DELETE FROM cb_imosphere_cdm_531.r140dv3ustatsview_1815 WHERE True;
drop table cb_imosphere_cdm_531.r140dv3ustatsview_1815;
DELETE FROM cb_imosphere_cdm_531.r140dv3utempresults_1815 WHERE True;
drop table cb_imosphere_cdm_531.r140dv3utempresults_1815;


-- 1816	Distribution of low range, by measurement_concept_id and unit_concept_id
--HINT DISTRIBUTE_ON_KEY(stratum1_id)
 CREATE TABLE cb_imosphere_cdm_531.r140dv3uoverallstats_1816
  AS
SELECT
subject_id as stratum1_id,
  unit_concept_id as stratum2_id,
  cast(avg(1.0 * count_value)  as float64) as avg_value,
  cast(STDDEV(count_value)  as float64) as stdev_value,
  min(count_value) as min_value,
  max(count_value) as max_value,
  COUNT(*) as total
FROM
(
  select measurement_concept_id as subject_id, 
	unit_concept_id,
	cast(range_low  as float64) as count_value
  from cb_imosphere_cdm_531.measurement m
  where m.unit_concept_id is not null
  	and m.value_as_number is not null
  	and m.range_low is not null
  	and m.range_high is not null
) a
 group by  1, 2 ;
--HINT DISTRIBUTE_ON_KEY(stratum1_id)
 CREATE TABLE cb_imosphere_cdm_531.r140dv3ustatsview_1816
  AS
SELECT
subject_id as stratum1_id, 
  unit_concept_id as stratum2_id, 
  count_value, COUNT(*) as total, 
  row_number() over (partition by subject_id, unit_concept_id order by count_value) as rn
FROM
(
  select measurement_concept_id as subject_id, 
	unit_concept_id,
	cast(range_low  as float64) as count_value
  from cb_imosphere_cdm_531.measurement m
  where m.unit_concept_id is not null
  	and m.value_as_number is not null
  	and m.range_low is not null
  	and m.range_high is not null
) a
 group by  1, 2, 3 ;
--HINT DISTRIBUTE_ON_KEY(stratum1_id)
 CREATE TABLE cb_imosphere_cdm_531.r140dv3utempresults_1816
  AS
SELECT
1816 as analysis_id,
  cast(o.stratum1_id as STRING) as stratum1_id,
  cast(o.stratum2_id as STRING) as stratum2_id,
  floor((COUNT(o.total)+99)/100)*100 as count_value,
  o.min_value,
	o.max_value,
	o.avg_value,
	o.stdev_value,
	min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
	min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
	min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
	min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
	min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
FROM
(
   select s.stratum1_id, s.stratum2_id, s.count_value, s.total, sum(p.total) as accumulated
   from cb_imosphere_cdm_531.r140dv3ustatsview_1816 s
  join cb_imosphere_cdm_531.r140dv3ustatsview_1816 p on s.stratum1_id = p.stratum1_id and s.stratum2_id = p.stratum2_id and p.rn <= s.rn
   group by  s.stratum1_id, s.stratum2_id, s.count_value, s.total, s.rn
 ) p
join cb_imosphere_cdm_531.r140dv3uoverallstats_1816 o on p.stratum1_id = o.stratum1_id and p.stratum2_id = o.stratum2_id 
 group by  o.stratum1_id, o.stratum2_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
 ;
--HINT DISTRIBUTE_ON_KEY(stratum_1)
CREATE TABLE cb_imosphere_cdm_531.tmpach_dist_1816
 AS
SELECT
analysis_id, stratum1_id as stratum_1, stratum2_id as stratum_2, 
  cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
  count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
FROM
cb_imosphere_cdm_531.r140dv3utempresults_1816
;
DELETE FROM cb_imosphere_cdm_531.r140dv3uoverallstats_1816 WHERE True;
drop table cb_imosphere_cdm_531.r140dv3uoverallstats_1816;
DELETE FROM cb_imosphere_cdm_531.r140dv3ustatsview_1816 WHERE True;
drop table cb_imosphere_cdm_531.r140dv3ustatsview_1816;
DELETE FROM cb_imosphere_cdm_531.r140dv3utempresults_1816 WHERE True;
drop table cb_imosphere_cdm_531.r140dv3utempresults_1816;


-- 1817	Distribution of high range, by observation_concept_id and unit_concept_id
--HINT DISTRIBUTE_ON_KEY(stratum1_id)
 CREATE TABLE cb_imosphere_cdm_531.r140dv3uoverallstats_1817
  AS
SELECT
subject_id as stratum1_id,
  unit_concept_id as stratum2_id,
  cast(avg(1.0 * count_value)  as float64) as avg_value,
  cast(STDDEV(count_value)  as float64) as stdev_value,
  min(count_value) as min_value,
  max(count_value) as max_value,
  COUNT(*) as total
FROM
(
  select measurement_concept_id as subject_id, 
	unit_concept_id,
	cast(range_high  as float64) as count_value
  from cb_imosphere_cdm_531.measurement m
  where m.unit_concept_id is not null
	and m.value_as_number is not null
	and m.range_low is not null
	and m.range_high is not null
) a
 group by  1, 2 ;
--HINT DISTRIBUTE_ON_KEY(stratum1_id)
 CREATE TABLE cb_imosphere_cdm_531.r140dv3ustatsview_1817
  AS
SELECT
subject_id as stratum1_id, 
  unit_concept_id as stratum2_id, 
  count_value, COUNT(*) as total, 
  row_number() over (partition by subject_id, unit_concept_id order by count_value) as rn
FROM
(
  select measurement_concept_id as subject_id, 
	unit_concept_id,
	cast(range_high  as float64) as count_value
  from cb_imosphere_cdm_531.measurement m
  where m.unit_concept_id is not null
	and m.value_as_number is not null
	and m.range_low is not null
	and m.range_high is not null
) a
 group by  1, 2, 3 ;
--HINT DISTRIBUTE_ON_KEY(stratum1_id)
 CREATE TABLE cb_imosphere_cdm_531.r140dv3utempresults_1817
  AS
SELECT
1817 as analysis_id,
  cast(o.stratum1_id as STRING) as stratum1_id,
  cast(o.stratum2_id as STRING) as stratum2_id,
  floor((COUNT(o.total)+99)/100)*100 as count_value,
  o.min_value,
	o.max_value,
	o.avg_value,
	o.stdev_value,
	min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
	min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
	min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
	min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
	min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
FROM
(
   select s.stratum1_id, s.stratum2_id, s.count_value, s.total, sum(p.total) as accumulated
   from cb_imosphere_cdm_531.r140dv3ustatsview_1817 s
  join cb_imosphere_cdm_531.r140dv3ustatsview_1817 p on s.stratum1_id = p.stratum1_id and s.stratum2_id = p.stratum2_id and p.rn <= s.rn
   group by  s.stratum1_id, s.stratum2_id, s.count_value, s.total, s.rn
 ) p
join cb_imosphere_cdm_531.r140dv3uoverallstats_1817 o on p.stratum1_id = o.stratum1_id and p.stratum2_id = o.stratum2_id
 group by  o.stratum1_id, o.stratum2_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
 ;
--HINT DISTRIBUTE_ON_KEY(stratum_1)
CREATE TABLE cb_imosphere_cdm_531.tmpach_dist_1817
 AS
SELECT
analysis_id, stratum1_id as stratum_1, stratum2_id as stratum_2, 
  cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
  count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
FROM
cb_imosphere_cdm_531.r140dv3utempresults_1817
;
DELETE FROM cb_imosphere_cdm_531.r140dv3uoverallstats_1817 WHERE True;
drop table cb_imosphere_cdm_531.r140dv3uoverallstats_1817;
DELETE FROM cb_imosphere_cdm_531.r140dv3ustatsview_1817 WHERE True;
drop table cb_imosphere_cdm_531.r140dv3ustatsview_1817;
DELETE FROM cb_imosphere_cdm_531.r140dv3utempresults_1817 WHERE True;
drop table cb_imosphere_cdm_531.r140dv3utempresults_1817;


-- 1820	Number of observation records by condition occurrence start month
--HINT DISTRIBUTE_ON_KEY(stratum_1)
CREATE TABLE cb_imosphere_cdm_531.tmpach_1820
 AS WITH rawdata as (
   select EXTRACT(YEAR from measurement_date)*100 + EXTRACT(MONTH from measurement_date) as stratum_1,
    COUNT(m.person_id) as count_value
   from cb_imosphere_cdm_531.measurement m
  inner join 
  cb_imosphere_cdm_531.observation_period op on m.person_id = op.person_id
  -- only include events that occur during observation period
  where m.measurement_date <= op.observation_period_end_date and
  m.measurement_date >= op.observation_period_start_date
   group by  1 )
 SELECT 1820 as analysis_id,
  cast(stratum_1 as STRING) as stratum_1,
  cast(null as STRING) as stratum_2,
  cast(null as STRING) as stratum_3,
  cast(null as STRING) as stratum_4,
  cast(null as STRING) as stratum_5,
  count_value
 FROM rawdata;


-- 830	Number of descendant measurement occurrence records,by procedure_concept_id
--HINT DISTRIBUTE_ON_KEY(stratum_1)
CREATE TABLE cb_imosphere_cdm_531.tmpach_1830
  AS WITH cte_procedure as (
	 select ca.ancestor_concept_id as concept_id, COUNT(*)
 as drc
	 from cb_imosphere_cdm_531.measurement co
		join cb_imosphere_cdm_531.concept_ancestor ca
			on ca.descendant_concept_id = co.measurement_concept_id
	 group by  ca.ancestor_concept_id
 )
  SELECT 1830 as analysis_id,
  cast(co.measurement_concept_id as STRING) as stratum_1,
  cast(null as STRING) as stratum_2,
  cast(null as STRING) as stratum_3,
  cast(null as STRING) as stratum_4,
  cast(null as STRING) as stratum_5,
  floor((c.drc+99)/100)*100 as count_value
 FROM cb_imosphere_cdm_531.measurement co
	join cte_procedure c
		on c.concept_id = co.measurement_concept_id
 group by  co.measurement_concept_id, c.drc
 ;


-- 2100	Number of persons with at least one device exposure , by device_concept_id
--HINT DISTRIBUTE_ON_KEY(stratum_1)
 CREATE TABLE cb_imosphere_cdm_531.tmpach_2100
  AS
SELECT
2100 as analysis_id, 
  cast(m.device_concept_id as STRING) as stratum_1,
	cast(null as STRING) as stratum_2,
	cast(null as STRING) as stratum_3,
	cast(null as STRING) as stratum_4,
	cast(null as STRING) as stratum_5,
	floor((COUNT(distinct m.person_id)+99)/100)*100 as count_value
FROM
cb_imosphere_cdm_531.device_exposure m
 group by  m.device_concept_id
 ;


-- 2101	Number of device exposure  records, by device_concept_id
--HINT DISTRIBUTE_ON_KEY(stratum_1)
 CREATE TABLE cb_imosphere_cdm_531.tmpach_2101
  AS
SELECT
2101 as analysis_id, 
  cast(m.device_concept_id as STRING) as stratum_1,
	cast(null as STRING) as stratum_2,
	cast(null as STRING) as stratum_3,
	cast(null as STRING) as stratum_4, 
	cast(null as STRING) as stratum_5,
	floor((COUNT(m.person_id)+99)/100)*100 as count_value
FROM
cb_imosphere_cdm_531.device_exposure m
 group by  m.device_concept_id
 ;


-- 2105	Number of exposure records by device_concept_id by device_type_concept_id
--HINT DISTRIBUTE_ON_KEY(stratum_1)
 CREATE TABLE cb_imosphere_cdm_531.tmpach_2105
  AS
SELECT
2105 as analysis_id, 
	cast(m.device_concept_id as STRING) as stratum_1,
	cast(m.device_type_concept_id as STRING) as stratum_2,
	cast(null as STRING)  as stratum_3,
	cast(null as STRING) as stratum_4,
	cast(null as STRING) as stratum_5,
	floor((COUNT(m.person_id)+99)/100)*100 as count_value
FROM
cb_imosphere_cdm_531.device_exposure m
 group by  m.device_concept_id, m.device_type_concept_id
 ;


-- 2120	Number of device_exposure records by start month
--HINT DISTRIBUTE_ON_KEY(stratum_1)
CREATE TABLE cb_imosphere_cdm_531.tmpach_2120
 AS WITH rawdata as (
   select EXTRACT(YEAR from ce1.device_exposure_start_date)*100 + EXTRACT(MONTH from ce1.device_exposure_start_date) as stratum_1,
    COUNT(ce1.person_id) as count_value
   from cb_imosphere_cdm_531.device_exposure ce1 inner join 
  cb_imosphere_cdm_531.observation_period op on ce1.person_id = op.person_id
  -- only include events that occur during observation period
  where ce1.device_exposure_start_date <= op.observation_period_end_date and
  ce1.device_exposure_start_date >= op.observation_period_start_date
   group by  1 )
 SELECT 2120 as analysis_id,
  cast(stratum_1 as STRING) as stratum_1,
  cast(null as STRING) as stratum_2,
  cast(null as STRING) as stratum_3,
  cast(null as STRING) as stratum_4,
  cast(null as STRING) as stratum_5,
  count_value
 FROM rawdata;

-- 2130	Number of descendant device exposure records,by device_concept_id
--HINT DISTRIBUTE_ON_KEY(stratum_1)
CREATE TABLE cb_imosphere_cdm_531.tmpach_2130
  AS WITH cte_procedure as (
	 select ca.ancestor_concept_id as concept_id, COUNT(*)
 as drc
	 from cb_imosphere_cdm_531.device_exposure co
		join cb_imosphere_cdm_531.concept_ancestor ca
			on ca.descendant_concept_id = co.device_concept_id
	 group by  ca.ancestor_concept_id
 )
  SELECT 2130 as analysis_id,
  cast(co.device_concept_id as STRING) as stratum_1,
  cast(null as STRING) as stratum_2,
  cast(null as STRING) as stratum_3,
  cast(null as STRING) as stratum_4,
  cast(null as STRING) as stratum_5,
  floor((c.drc+99)/100)*100 as count_value
 FROM cb_imosphere_cdm_531.device_exposure co
	join cte_procedure c
		on c.concept_id = co.device_concept_id
 group by  co.device_concept_id, c.drc
 ;


-- 2201	Number of device exposure  records, by device_concept_id
--HINT DISTRIBUTE_ON_KEY(stratum_1)
 CREATE TABLE cb_imosphere_cdm_531.tmpach_2201
  AS
SELECT
2201 as analysis_id, 
    cast(m.note_type_concept_id as STRING) as stratum_1,
	cast(null as STRING) as stratum_2, cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
	COUNT(m.person_id) as count_value
FROM
cb_imosphere_cdm_531.note m
 group by  m.note_type_concept_id
 ;


-- 5000	cdm name, cdm release date, cdm_version, vocabulary_version
--HINT DISTRIBUTE_ON_KEY(stratum_1)
CREATE TABLE cb_imosphere_cdm_531.tmpach_5000
 AS
SELECT
5000 as analysis_id,  cast('Connected Yorkshire Research Database' as STRING) as stratum_1, 
source_release_date as stratum_2, 
cdm_release_date as stratum_3, 
cdm_version as stratum_4,
vocabulary_version as stratum_5, 
9999 as count_value
FROM
cb_imosphere_cdm_531.cdm_source;
-- HINT DISTRIBUTE_ON_KEY(stratum_1)
--select 5000 as analysis_id, CAST('Connected Yorkshire Research Database' AS VARCHAR(255)) as stratum_1, 
--cast(null as varchar(255)) as stratum_2, cast(null as varchar(255)) as stratum_3, cast(null as varchar(255)) as stratum_4, cast(null as varchar(255)) as stratum_5,
--9999 as count_value, 
--  cast(null as float) as min_value,
--	cast(null as float) as max_value,
--	cast(null as float) as avg_value,
--	cast(null as float) as stdev_value,
--	cast(null as float) as median_value,
--	cast(null as float) as p10_value,
--	cast(null as float) as p25_value,
--	cast(null as float) as p75_value,
--	cast(null as float) as p90_value
--into cb_imosphere_cdm_531.tmpach_dist_5000
-- from cb_imosphere_cdm_531.person;


  DROP TABLE IF EXISTS cb_imosphere_cdm_531.catalogue_results;
--HINT DISTRIBUTE_ON_KEY(analysis_id)
CREATE TABLE cb_imosphere_cdm_531.catalogue_results
 AS
SELECT
analysis_id, stratum_1, stratum_2, stratum_3, stratum_4, stratum_5, count_value
FROM
(
  select cast(analysis_id  as int64) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value  as int64) as count_value from 
                                     cb_imosphere_cdm_531.tmpach_0 
union all
 select cast(analysis_id  as int64) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value  as int64) as count_value from 
                                     cb_imosphere_cdm_531.tmpach_1 
union all
 select cast(analysis_id  as int64) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value  as int64) as count_value from 
                                     cb_imosphere_cdm_531.tmpach_2 
union all
 select cast(analysis_id  as int64) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value  as int64) as count_value from 
                                     cb_imosphere_cdm_531.tmpach_3 
union all
 select cast(analysis_id  as int64) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value  as int64) as count_value from 
                                     cb_imosphere_cdm_531.tmpach_101 
union all
 select cast(analysis_id  as int64) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value  as int64) as count_value from 
                                     cb_imosphere_cdm_531.tmpach_102 
union all
 select cast(analysis_id  as int64) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value  as int64) as count_value from 
                                     cb_imosphere_cdm_531.tmpach_108 
union all
 select cast(analysis_id  as int64) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value  as int64) as count_value from 
                                     cb_imosphere_cdm_531.tmpach_109 
union all
 select cast(analysis_id  as int64) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value  as int64) as count_value from 
                                     cb_imosphere_cdm_531.tmpach_110 
union all
 select cast(analysis_id  as int64) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value  as int64) as count_value from 
                                     cb_imosphere_cdm_531.tmpach_111 
union all
 select cast(analysis_id  as int64) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value  as int64) as count_value from 
                                     cb_imosphere_cdm_531.tmpach_112 
union all
 select cast(analysis_id  as int64) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value  as int64) as count_value from 
                                     cb_imosphere_cdm_531.tmpach_113 
union all
 select cast(analysis_id  as int64) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value  as int64) as count_value from 
                                     cb_imosphere_cdm_531.tmpach_117 
union all
 select cast(analysis_id  as int64) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value  as int64) as count_value from 
                                     cb_imosphere_cdm_531.tmpach_200 
union all
 select cast(analysis_id  as int64) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value  as int64) as count_value from 
                                     cb_imosphere_cdm_531.tmpach_201 
union all
 select cast(analysis_id  as int64) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value  as int64) as count_value from 
                                     cb_imosphere_cdm_531.tmpach_220 
union all
 select cast(analysis_id  as int64) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value  as int64) as count_value from 
                                     cb_imosphere_cdm_531.tmpach_400 
union all
 select cast(analysis_id  as int64) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value  as int64) as count_value from 
                                     cb_imosphere_cdm_531.tmpach_401 
union all
 select cast(analysis_id  as int64) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value  as int64) as count_value from 
                                     cb_imosphere_cdm_531.tmpach_405 
union all
 select cast(analysis_id  as int64) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value  as int64) as count_value from 
                                     cb_imosphere_cdm_531.tmpach_420 
union all
 select cast(analysis_id  as int64) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value  as int64) as count_value from 
                                     cb_imosphere_cdm_531.tmpach_430 
union all
 select cast(analysis_id  as int64) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value  as int64) as count_value from 
                                     cb_imosphere_cdm_531.tmpach_501 
union all
 select cast(analysis_id  as int64) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value  as int64) as count_value from 
                                     cb_imosphere_cdm_531.tmpach_502 
union all
 select cast(analysis_id  as int64) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value  as int64) as count_value from 
                                     cb_imosphere_cdm_531.tmpach_600 
union all
 select cast(analysis_id  as int64) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value  as int64) as count_value from 
                                     cb_imosphere_cdm_531.tmpach_601 
union all
 select cast(analysis_id  as int64) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value  as int64) as count_value from 
                                     cb_imosphere_cdm_531.tmpach_605 
union all
 select cast(analysis_id  as int64) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value  as int64) as count_value from 
                                     cb_imosphere_cdm_531.tmpach_620 
union all
 select cast(analysis_id  as int64) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value  as int64) as count_value from 
                                     cb_imosphere_cdm_531.tmpach_630 
union all
 select cast(analysis_id  as int64) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value  as int64) as count_value from 
                                     cb_imosphere_cdm_531.tmpach_700 
union all
 select cast(analysis_id  as int64) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value  as int64) as count_value from 
                                     cb_imosphere_cdm_531.tmpach_701 
union all
 select cast(analysis_id  as int64) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value  as int64) as count_value from 
                                     cb_imosphere_cdm_531.tmpach_705 
union all
 select cast(analysis_id  as int64) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value  as int64) as count_value from 
                                     cb_imosphere_cdm_531.tmpach_720 
union all
 select cast(analysis_id  as int64) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value  as int64) as count_value from 
                                     cb_imosphere_cdm_531.tmpach_730 
union all
 select cast(analysis_id  as int64) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value  as int64) as count_value from 
                                     cb_imosphere_cdm_531.tmpach_800 
union all
 select cast(analysis_id  as int64) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value  as int64) as count_value from 
                                     cb_imosphere_cdm_531.tmpach_801 
union all
 select cast(analysis_id  as int64) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value  as int64) as count_value from 
                                     cb_imosphere_cdm_531.tmpach_805 
union all
 select cast(analysis_id  as int64) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value  as int64) as count_value from 
                                     cb_imosphere_cdm_531.tmpach_820 
union all
 select cast(analysis_id  as int64) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value  as int64) as count_value from 
                                     cb_imosphere_cdm_531.tmpach_830 
union all
 select cast(analysis_id  as int64) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value  as int64) as count_value from 
                                     cb_imosphere_cdm_531.tmpach_901 
union all
 select cast(analysis_id  as int64) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value  as int64) as count_value from 
                                     cb_imosphere_cdm_531.tmpach_920 
union all
 select cast(analysis_id  as int64) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value  as int64) as count_value from 
                                     cb_imosphere_cdm_531.tmpach_1001 
union all
 select cast(analysis_id  as int64) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value  as int64) as count_value from 
                                     cb_imosphere_cdm_531.tmpach_1020 
union all
 select cast(analysis_id  as int64) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value  as int64) as count_value from 
                                     cb_imosphere_cdm_531.tmpach_1800 
union all
 select cast(analysis_id  as int64) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value  as int64) as count_value from 
                                     cb_imosphere_cdm_531.tmpach_1801 
union all
 select cast(analysis_id  as int64) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value  as int64) as count_value from 
                                     cb_imosphere_cdm_531.tmpach_1805 
union all
 select cast(analysis_id  as int64) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value  as int64) as count_value from 
                                     cb_imosphere_cdm_531.tmpach_1820 
union all
 select cast(analysis_id  as int64) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value  as int64) as count_value from 
                                     cb_imosphere_cdm_531.tmpach_1830 
union all
 select cast(analysis_id  as int64) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value  as int64) as count_value from 
                                     cb_imosphere_cdm_531.tmpach_2100 
union all
 select cast(analysis_id  as int64) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value  as int64) as count_value from 
                                     cb_imosphere_cdm_531.tmpach_2101 
union all
 select cast(analysis_id  as int64) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value  as int64) as count_value from 
                                     cb_imosphere_cdm_531.tmpach_2105 
union all
 select cast(analysis_id  as int64) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value  as int64) as count_value from 
                                     cb_imosphere_cdm_531.tmpach_2120 
union all
 select cast(analysis_id  as int64) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value  as int64) as count_value from 
                                     cb_imosphere_cdm_531.tmpach_2130 
union all
 select cast(analysis_id  as int64) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value  as int64) as count_value from 
                                     cb_imosphere_cdm_531.tmpach_2201 
union all
 select cast(analysis_id  as int64) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value  as int64) as count_value from 
                                     cb_imosphere_cdm_531.tmpach_5000
) q
  where count_value > 5
;

  DROP TABLE IF EXISTS cb_imosphere_cdm_531.catalogue_results_dist;
--HINT DISTRIBUTE_ON_KEY(analysis_id)
CREATE TABLE cb_imosphere_cdm_531.catalogue_results_dist
 AS
SELECT
analysis_id, stratum_1, stratum_2, stratum_3, stratum_4, stratum_5, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
FROM
(
  select cast(analysis_id  as int64) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value  as int64) as count_value, cast(min_value  as float64) as min_value, cast(max_value  as float64) as max_value, cast(avg_value  as float64) as avg_value, cast(stdev_value  as float64) as stdev_value, cast(median_value  as float64) as median_value, cast(p10_value  as float64) as p10_value, cast(p25_value  as float64) as p25_value, cast(p75_value  as float64) as p75_value, cast(p90_value  as float64) as p90_value from 
                                     cb_imosphere_cdm_531.tmpach_dist_103 
union all
 select cast(analysis_id  as int64) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value  as int64) as count_value, cast(min_value  as float64) as min_value, cast(max_value  as float64) as max_value, cast(avg_value  as float64) as avg_value, cast(stdev_value  as float64) as stdev_value, cast(median_value  as float64) as median_value, cast(p10_value  as float64) as p10_value, cast(p25_value  as float64) as p25_value, cast(p75_value  as float64) as p75_value, cast(p90_value  as float64) as p90_value from 
                                     cb_imosphere_cdm_531.tmpach_dist_104 
union all
 select cast(analysis_id  as int64) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value  as int64) as count_value, cast(min_value  as float64) as min_value, cast(max_value  as float64) as max_value, cast(avg_value  as float64) as avg_value, cast(stdev_value  as float64) as stdev_value, cast(median_value  as float64) as median_value, cast(p10_value  as float64) as p10_value, cast(p25_value  as float64) as p25_value, cast(p75_value  as float64) as p75_value, cast(p90_value  as float64) as p90_value from 
                                     cb_imosphere_cdm_531.tmpach_dist_105 
union all
 select cast(analysis_id  as int64) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value  as int64) as count_value, cast(min_value  as float64) as min_value, cast(max_value  as float64) as max_value, cast(avg_value  as float64) as avg_value, cast(stdev_value  as float64) as stdev_value, cast(median_value  as float64) as median_value, cast(p10_value  as float64) as p10_value, cast(p25_value  as float64) as p25_value, cast(p75_value  as float64) as p75_value, cast(p90_value  as float64) as p90_value from 
                                     cb_imosphere_cdm_531.tmpach_dist_106 
union all
 select cast(analysis_id  as int64) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value  as int64) as count_value, cast(min_value  as float64) as min_value, cast(max_value  as float64) as max_value, cast(avg_value  as float64) as avg_value, cast(stdev_value  as float64) as stdev_value, cast(median_value  as float64) as median_value, cast(p10_value  as float64) as p10_value, cast(p25_value  as float64) as p25_value, cast(p75_value  as float64) as p75_value, cast(p90_value  as float64) as p90_value from 
                                     cb_imosphere_cdm_531.tmpach_dist_107 
union all
 select cast(analysis_id  as int64) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value  as int64) as count_value, cast(min_value  as float64) as min_value, cast(max_value  as float64) as max_value, cast(avg_value  as float64) as avg_value, cast(stdev_value  as float64) as stdev_value, cast(median_value  as float64) as median_value, cast(p10_value  as float64) as p10_value, cast(p25_value  as float64) as p25_value, cast(p75_value  as float64) as p75_value, cast(p90_value  as float64) as p90_value from 
                                     cb_imosphere_cdm_531.tmpach_dist_203 
union all
 select cast(analysis_id  as int64) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value  as int64) as count_value, cast(min_value  as float64) as min_value, cast(max_value  as float64) as max_value, cast(avg_value  as float64) as avg_value, cast(stdev_value  as float64) as stdev_value, cast(median_value  as float64) as median_value, cast(p10_value  as float64) as p10_value, cast(p25_value  as float64) as p25_value, cast(p75_value  as float64) as p75_value, cast(p90_value  as float64) as p90_value from 
                                     cb_imosphere_cdm_531.tmpach_dist_206 
union all
 select cast(analysis_id  as int64) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value  as int64) as count_value, cast(min_value  as float64) as min_value, cast(max_value  as float64) as max_value, cast(avg_value  as float64) as avg_value, cast(stdev_value  as float64) as stdev_value, cast(median_value  as float64) as median_value, cast(p10_value  as float64) as p10_value, cast(p25_value  as float64) as p25_value, cast(p75_value  as float64) as p75_value, cast(p90_value  as float64) as p90_value from 
                                     cb_imosphere_cdm_531.tmpach_dist_211 
union all
 select cast(analysis_id  as int64) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value  as int64) as count_value, cast(min_value  as float64) as min_value, cast(max_value  as float64) as max_value, cast(avg_value  as float64) as avg_value, cast(stdev_value  as float64) as stdev_value, cast(median_value  as float64) as median_value, cast(p10_value  as float64) as p10_value, cast(p25_value  as float64) as p25_value, cast(p75_value  as float64) as p75_value, cast(p90_value  as float64) as p90_value from 
                                     cb_imosphere_cdm_531.tmpach_dist_403 
union all
 select cast(analysis_id  as int64) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value  as int64) as count_value, cast(min_value  as float64) as min_value, cast(max_value  as float64) as max_value, cast(avg_value  as float64) as avg_value, cast(stdev_value  as float64) as stdev_value, cast(median_value  as float64) as median_value, cast(p10_value  as float64) as p10_value, cast(p25_value  as float64) as p25_value, cast(p75_value  as float64) as p75_value, cast(p90_value  as float64) as p90_value from 
                                     cb_imosphere_cdm_531.tmpach_dist_406 
union all
 select cast(analysis_id  as int64) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value  as int64) as count_value, cast(min_value  as float64) as min_value, cast(max_value  as float64) as max_value, cast(avg_value  as float64) as avg_value, cast(stdev_value  as float64) as stdev_value, cast(median_value  as float64) as median_value, cast(p10_value  as float64) as p10_value, cast(p25_value  as float64) as p25_value, cast(p75_value  as float64) as p75_value, cast(p90_value  as float64) as p90_value from 
                                     cb_imosphere_cdm_531.tmpach_dist_506 
union all
 select cast(analysis_id  as int64) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value  as int64) as count_value, cast(min_value  as float64) as min_value, cast(max_value  as float64) as max_value, cast(avg_value  as float64) as avg_value, cast(stdev_value  as float64) as stdev_value, cast(median_value  as float64) as median_value, cast(p10_value  as float64) as p10_value, cast(p25_value  as float64) as p25_value, cast(p75_value  as float64) as p75_value, cast(p90_value  as float64) as p90_value from 
                                     cb_imosphere_cdm_531.tmpach_dist_603 
union all
 select cast(analysis_id  as int64) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value  as int64) as count_value, cast(min_value  as float64) as min_value, cast(max_value  as float64) as max_value, cast(avg_value  as float64) as avg_value, cast(stdev_value  as float64) as stdev_value, cast(median_value  as float64) as median_value, cast(p10_value  as float64) as p10_value, cast(p25_value  as float64) as p25_value, cast(p75_value  as float64) as p75_value, cast(p90_value  as float64) as p90_value from 
                                     cb_imosphere_cdm_531.tmpach_dist_606 
union all
 select cast(analysis_id  as int64) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value  as int64) as count_value, cast(min_value  as float64) as min_value, cast(max_value  as float64) as max_value, cast(avg_value  as float64) as avg_value, cast(stdev_value  as float64) as stdev_value, cast(median_value  as float64) as median_value, cast(p10_value  as float64) as p10_value, cast(p25_value  as float64) as p25_value, cast(p75_value  as float64) as p75_value, cast(p90_value  as float64) as p90_value from 
                                     cb_imosphere_cdm_531.tmpach_dist_703 
union all
 select cast(analysis_id  as int64) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value  as int64) as count_value, cast(min_value  as float64) as min_value, cast(max_value  as float64) as max_value, cast(avg_value  as float64) as avg_value, cast(stdev_value  as float64) as stdev_value, cast(median_value  as float64) as median_value, cast(p10_value  as float64) as p10_value, cast(p25_value  as float64) as p25_value, cast(p75_value  as float64) as p75_value, cast(p90_value  as float64) as p90_value from 
                                     cb_imosphere_cdm_531.tmpach_dist_706 
union all
 select cast(analysis_id  as int64) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value  as int64) as count_value, cast(min_value  as float64) as min_value, cast(max_value  as float64) as max_value, cast(avg_value  as float64) as avg_value, cast(stdev_value  as float64) as stdev_value, cast(median_value  as float64) as median_value, cast(p10_value  as float64) as p10_value, cast(p25_value  as float64) as p25_value, cast(p75_value  as float64) as p75_value, cast(p90_value  as float64) as p90_value from 
                                     cb_imosphere_cdm_531.tmpach_dist_715 
union all
 select cast(analysis_id  as int64) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value  as int64) as count_value, cast(min_value  as float64) as min_value, cast(max_value  as float64) as max_value, cast(avg_value  as float64) as avg_value, cast(stdev_value  as float64) as stdev_value, cast(median_value  as float64) as median_value, cast(p10_value  as float64) as p10_value, cast(p25_value  as float64) as p25_value, cast(p75_value  as float64) as p75_value, cast(p90_value  as float64) as p90_value from 
                                     cb_imosphere_cdm_531.tmpach_dist_716 
union all
 select cast(analysis_id  as int64) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value  as int64) as count_value, cast(min_value  as float64) as min_value, cast(max_value  as float64) as max_value, cast(avg_value  as float64) as avg_value, cast(stdev_value  as float64) as stdev_value, cast(median_value  as float64) as median_value, cast(p10_value  as float64) as p10_value, cast(p25_value  as float64) as p25_value, cast(p75_value  as float64) as p75_value, cast(p90_value  as float64) as p90_value from 
                                     cb_imosphere_cdm_531.tmpach_dist_717 
union all
 select cast(analysis_id  as int64) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value  as int64) as count_value, cast(min_value  as float64) as min_value, cast(max_value  as float64) as max_value, cast(avg_value  as float64) as avg_value, cast(stdev_value  as float64) as stdev_value, cast(median_value  as float64) as median_value, cast(p10_value  as float64) as p10_value, cast(p25_value  as float64) as p25_value, cast(p75_value  as float64) as p75_value, cast(p90_value  as float64) as p90_value from 
                                     cb_imosphere_cdm_531.tmpach_dist_803 
union all
 select cast(analysis_id  as int64) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value  as int64) as count_value, cast(min_value  as float64) as min_value, cast(max_value  as float64) as max_value, cast(avg_value  as float64) as avg_value, cast(stdev_value  as float64) as stdev_value, cast(median_value  as float64) as median_value, cast(p10_value  as float64) as p10_value, cast(p25_value  as float64) as p25_value, cast(p75_value  as float64) as p75_value, cast(p90_value  as float64) as p90_value from 
                                     cb_imosphere_cdm_531.tmpach_dist_806 
union all
 select cast(analysis_id  as int64) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value  as int64) as count_value, cast(min_value  as float64) as min_value, cast(max_value  as float64) as max_value, cast(avg_value  as float64) as avg_value, cast(stdev_value  as float64) as stdev_value, cast(median_value  as float64) as median_value, cast(p10_value  as float64) as p10_value, cast(p25_value  as float64) as p25_value, cast(p75_value  as float64) as p75_value, cast(p90_value  as float64) as p90_value from 
                                     cb_imosphere_cdm_531.tmpach_dist_815 
union all
 select cast(analysis_id  as int64) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value  as int64) as count_value, cast(min_value  as float64) as min_value, cast(max_value  as float64) as max_value, cast(avg_value  as float64) as avg_value, cast(stdev_value  as float64) as stdev_value, cast(median_value  as float64) as median_value, cast(p10_value  as float64) as p10_value, cast(p25_value  as float64) as p25_value, cast(p75_value  as float64) as p75_value, cast(p90_value  as float64) as p90_value from 
                                     cb_imosphere_cdm_531.tmpach_dist_1803 
union all
 select cast(analysis_id  as int64) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value  as int64) as count_value, cast(min_value  as float64) as min_value, cast(max_value  as float64) as max_value, cast(avg_value  as float64) as avg_value, cast(stdev_value  as float64) as stdev_value, cast(median_value  as float64) as median_value, cast(p10_value  as float64) as p10_value, cast(p25_value  as float64) as p25_value, cast(p75_value  as float64) as p75_value, cast(p90_value  as float64) as p90_value from 
                                     cb_imosphere_cdm_531.tmpach_dist_1806 
union all
 select cast(analysis_id  as int64) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value  as int64) as count_value, cast(min_value  as float64) as min_value, cast(max_value  as float64) as max_value, cast(avg_value  as float64) as avg_value, cast(stdev_value  as float64) as stdev_value, cast(median_value  as float64) as median_value, cast(p10_value  as float64) as p10_value, cast(p25_value  as float64) as p25_value, cast(p75_value  as float64) as p75_value, cast(p90_value  as float64) as p90_value from 
                                     cb_imosphere_cdm_531.tmpach_dist_1815 
union all
 select cast(analysis_id  as int64) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value  as int64) as count_value, cast(min_value  as float64) as min_value, cast(max_value  as float64) as max_value, cast(avg_value  as float64) as avg_value, cast(stdev_value  as float64) as stdev_value, cast(median_value  as float64) as median_value, cast(p10_value  as float64) as p10_value, cast(p25_value  as float64) as p25_value, cast(p75_value  as float64) as p75_value, cast(p90_value  as float64) as p90_value from 
                                     cb_imosphere_cdm_531.tmpach_dist_1816 
union all
 select cast(analysis_id  as int64) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value  as int64) as count_value, cast(min_value  as float64) as min_value, cast(max_value  as float64) as max_value, cast(avg_value  as float64) as avg_value, cast(stdev_value  as float64) as stdev_value, cast(median_value  as float64) as median_value, cast(p10_value  as float64) as p10_value, cast(p25_value  as float64) as p25_value, cast(p75_value  as float64) as p75_value, cast(p90_value  as float64) as p90_value from 
                                     cb_imosphere_cdm_531.tmpach_dist_1817
) q
  where count_value > 5
;

/* INDEX CREATION SKIPPED, INDICES NOT SUPPORTED IN BIGQUERY */