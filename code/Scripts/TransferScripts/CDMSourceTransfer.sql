CREATE OR REPLACE PROCEDURE CY_IMOSPHERE_WORKSPACE.TRANSFER_CDM_SOURCE(run_id INT64)
OPTIONS (strict_mode=false)
BEGIN
	DELETE FROM `CY_IMOSPHERE_CDM_531.cdm_source` WHERE 1=1;
	INSERT INTO `CY_IMOSPHERE_CDM_531.cdm_source`
	(
		cdm_source_name,
		cdm_source_abbreviation,
		cdm_holder,
		source_description,
		source_documentation_reference,
		source_release_date,
		cdm_release_date,
		cdm_version,
		vocabulary_version
	)
	VALUES
	(
		'Connected Yorkshire Research Database',
		'CYRD',
		'Bradford Teaching Hospital Foundation Trust',
		'Bradford Districts Primary Care TPP System One',
		'https://connectedhealthcities.github.io/assets/connected-yorkshire/Section%206.3_CONNECTED_BRADFORD_JUNE2019.pdf',
		PARSE_DATE('%d/%m/%Y','31/08/2017'),
		PARSE_DATE('%d/%m/%Y','01/12/2022'),
		'5.3',
		(SELECT vocabulary_version FROM `CY_CDM_VOCAB.vocabulary` WHERE vocabulary_id = 'None')
	);
END;