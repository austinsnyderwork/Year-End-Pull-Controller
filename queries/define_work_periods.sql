WITH WorksiteHistoryNoDel AS ( SELECT * FROM WorksiteHistory WHERE TransactionId != 'DEL')
-- Here we determine the appropriate previous WorksiteHistoryId for each CHG and TRF transaction
, DefinePrevTransaction AS (
	SELECT
		WorksiteHistoryId,
		CASE
			WHEN TransactionId = 'TRF' THEN LEAD(WorksiteHistoryId) OVER(PARTITION BY HcpId ORDER BY EffectDate DESC, WorksiteHistoryId DESC)
			ELSE NULL END
		AS PrevWorksiteHistoryId
	FROM WorksiteHistoryNoDel
	WHERE WorksiteType = 'P'

	UNION ALL

	SELECT
		WorksiteHistoryId,
		CASE
			WHEN TransactionId = 'CHG' THEN LEAD(WorksiteHistoryId) OVER(PARTITION BY HcpId, WorksiteId ORDER BY EffectDate DESC, WorksiteHistoryId DESC)
			ELSE NULL END
		AS PrevWorksiteHistoryId
	FROM WorksiteHistoryNoDel
)
-- Now we transform each CHG and TRF transaction pair (PrevWorksiteHistoryId and (Current)WorksiteHistoryId) into a DEL into an ADD
, TransformChgTrf AS (
	-- The first synthetic transaction is always a DEL
	SELECT
		wh_start.WorksiteHistoryId,

		-- It is extremely important to note that EffectDate and TransactionId are the ONLY columns that don't align with the previous WorksiteHistoryId.
		-- Everything else such as WkHours, WkWeeks, SpecialtyName, etc should be pulled with the previous WorksiteHistoryId.
		wh_end.EffectDate,
		'DEL' AS TransactionId,

		-- We need to define IsFirstSynthetic so that we can order our transactions correctly in the next CTE, since the synthetic DEL will have the same WorksiteHistoryId
		-- as the previous transaction
		CAST(1 AS BIT) AS IsFirstSynthetic
	FROM DefinePrevTransaction dpt
	INNER JOIN WorksiteHistory wh_start
		ON dpt.PrevWorksiteHistoryId = wh_start.WorksiteHistoryId
	INNER JOIN WorksiteHistory wh_end
		ON dpt.WorksiteHistoryId = wh_end.WorksiteHistoryId
	WHERE dpt.PrevWorksiteHistoryId IS NOT NULL

	UNION ALL

	-- The second synthetic transaction is always an ADD
	SELECT
		wh_end.WorksiteHistoryId,

		wh_end.EffectDate,
		'ADD' AS TransactionId,

		CAST(0 AS BIT) AS IsFirstSynthetic
	FROM DefinePrevTransaction dpt
	INNER JOIN WorksiteHistory wh_end
		ON dpt.WorksiteHistoryId = wh_end.WorksiteHistoryId
	WHERE dpt.PrevWorksiteHistoryId IS NOT NULL

	UNION ALL

	SELECT
		WorksiteHistoryId,
		EffectDate,

		TransactionId,
		CAST(0 AS BIT) AS IsFirstSynthetic
	FROM WorksiteHistory
	WHERE TransactionId IN ('ADD', 'DEL')
)
-- Coming into this CTE, TransformChgTrf is now just rows containing ONLY ADDs and DEL pairs for worksites.
-- The only HcpId worksites that don't have a DEL are those whose assignment hasn't ended yet
, AddDelPaired AS (
	SELECT
		wh.HcpId,
		tct.TransactionId AS AddTransactionId,
		tct.EffectDate AS AddEffectDate,
		tct.WorksiteHistoryId AS AddWorksiteHistoryId,

		-- When ordering a genuine previous WorksiteHistory transaction and a synthetic DEL, the genuine transaction comes first followed by the synthetic DEL. They just share a
		-- WorksiteHistoryId so that we can look up data on the synthetic DEL if need be
		LEAD(tct.WorksiteHistoryId) OVER(PARTITION BY wh.HcpId, wh.WorksiteId ORDER BY tct.EffectDate ASC, tct.WorksiteHistoryId ASC, tct.IsFirstSynthetic ASC) AS DelWorksiteHistoryId,
		LEAD(tct.TransactionId) OVER(PARTITION BY wh.HcpId, wh.WorksiteId ORDER BY tct.EffectDate ASC, tct.WorksiteHistoryId ASC, tct.IsFirstSynthetic ASC) AS DelTransactionId,
		LEAD(tct.EffectDate) OVER(PARTITION BY wh.HcpId, wh.WorksiteId ORDER BY tct.EffectDate ASC, tct.WorksiteHistoryId ASC, tct.IsFirstSynthetic ASC) AS DelEffectDate
	FROM TransformChgTrf tct
	INNER JOIN WorksiteHistory wh
		ON tct.WorksiteHistoryId = wh.WorksiteHistoryId
)
, PeriodsDefined AS (
	SELECT
		HcpId,
		AddWorksiteHistoryId,
		DelWorksiteHistoryId,
		DATEDIFF(MONTH, DATEFROMPARTS(?, 12, 31), AddEffectDate) AS MonthsSinceAdd,
		DATEDIFF(MONTH, DATEFROMPARTS(?, 12, 31), DelEffectDate) AS MonthsSinceDel
	FROM AddDelPaired
	WHERE
		-- We only care about the transactions that start with an ADD, because those represent the periods of activity
		AddTransactionId = 'ADD'
)
SELECT HcpId, MonthsSinceAdd, MonthsSinceDel, AddWorksiteHistoryId, DelWorksiteHistoryId
FROM PeriodsDefined