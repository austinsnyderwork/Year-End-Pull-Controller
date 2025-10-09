-- This whole query just transforms odd transactions such as CHG from a primary to secondary or vice versa and TRF into explicit ADDs and DELs for the purpose of
-- determining activity of providers. Without this query, those transactions are very difficult to query with as they present edge cases that must be explicitly
-- accounted for. With the query, you can partition by WorksiteId for each provider when determining where they are working.

DECLARE @WorkPeriods TABLE (
	HcpId INT,
	MonthsSinceAdd INT,
	MonthsSinceDel INT,
	AddWorksiteHistoryId INT,
	DelWorksiteHistoryId INT
)

DECLARE @OriginYear INT = 1900

-- Convert CHG to ADDs and DELs. Have to do this before converting the TRFs so that we can accurately determine what the previous Worksite was
;WITH WorksiteHistoryNoDel AS ( SELECT * FROM WorksiteHistory WHERE TransactionId != 'DEL')
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
		DATEDIFF(MONTH, DATEFROMPARTS(@OriginYear, 12, 31), AddEffectDate) AS MonthsSinceAdd,
		DATEDIFF(MONTH, DATEFROMPARTS(@OriginYear, 12, 31), DelEffectDate) AS MonthsSinceDel
	FROM AddDelPaired
	WHERE
		-- We only care about the transactions that start with an ADD, because those represent the periods of activity
		AddTransactionId = 'ADD'
)
INSERT INTO @WorkPeriods (HcpId, MonthsSinceAdd, MonthsSinceDel, AddWorksiteHistoryId, DelWorksiteHistoryId)
SELECT HcpId, MonthsSinceAdd, MonthsSinceDel, AddWorksiteHistoryId, DelWorksiteHistoryId
FROM PeriodsDefined

-- EVERYTHING above here should stay the same unless there is a purely technical reason to change it


-- Use these years to define what years you're wanting to pull.
-- Everything below here is specifically designed to accomodate for the "year end" pull format we do now.
-- If we change to quarterly pulls or some other pull format in the future then the SQL code below will have to change accordingly.
DECLARE @StartYear INT = ?
DECLARE @EndYear INT = ?

DECLARE @YearlyAssignments TABLE (
	Year INT,
	ReferenceDate DATE,
	WorksiteHistoryId INT
)

;WITH year_series AS (
    SELECT @StartYear AS Year
    UNION ALL
    SELECT Year + 1
    FROM year_series
    WHERE Year + 1 <= @EndYear
),
pull_months AS (
	SELECT
		Year,
		DATEDIFF(MONTH, DATEFROMPARTS(@OriginYear, 12, 31), DATEFROMPARTS(Year, 12, 31)) AS MonthsSinceOrigin
	FROM year_series
)
INSERT INTO @YearlyAssignments (Year, ReferenceDate, WorksiteHistoryId)
SELECT pm.Year, DATEFROMPARTS(pm.Year, 12, 31), wp.AddWorksiteHistoryId
FROM pull_months pm
LEFT JOIN @WorkPeriods wp
	ON wp.MonthsSinceAdd <= pm.MonthsSinceOrigin AND (wp.MonthsSinceDel >= (pm.MonthsSinceOrigin + 1) OR wp.MonthsSinceDel IS NULL)


-- Year ends table
SELECT
	ya.Year,
	ya.WorksiteHistoryId,
	wh.SiteLinkId,
	wh.EffectDate,
	wh.AdminDate,
	wh.HcpId,
	hcp.LicenseNumber,
	hcp.BirthDate,
	DATEDIFF(YEAR, hcp.BirthDate, ya.ReferenceDate)
        - CASE
            WHEN DATEADD(YEAR, DATEDIFF(YEAR, hcp.BirthDate, ya.ReferenceDate), hcp.BirthDate) > ya.ReferenceDate
            THEN 1 ELSE 0
            END as Age,
	hcp.BirthState,
	hcp.BirthCountry,
	hcp.Gender,
	hcp.TypeId,
	wh.WorksiteId,
	w.WorksiteName,
	wh.WorksiteType,
	wh.StatusId,
	wh.TransactionId,
	wh.TrfCountry as RelCountry,
	wh.TrfState as RelState,
	wh.WkHours,
	wh.WkWeeks,
	wh.SpecialtyId,
	s.SpecialtyName,
	wh.ActId,
	a.ActivityName,
	wh.ArrId as PracticeArrId,
	pa.PracticeArrName,
	wd.SiteTypeId,
	st.SiteTypeName,
	wh.Fte,
	w.City,
	city.Population as CityPop,
	w.CountyId,
	county.CountyName,
	county.Population as CountyPop,
	w.State,
	e.SchoolId,
	school.SchoolName,
	school.State as SchoolState,
	e.DegreeId,
	e.GradYear,
	hcp.Title,
	wh.FaceTime,
	wh.PctMedicaid,
	wh.PctSlidingFee
FROM @YearlyAssignments ya

LEFT JOIN WorksiteHistory wh
	ON ya.WorksiteHistoryId = wh.WorksiteHistoryId

LEFT JOIN Hcp hcp
	ON wh.HcpId = hcp.HcpId
LEFT JOIN Education e
	ON hcp.HcpId = e.HcpId
	AND hcp.TypeId = e.TypeId
	AND e.TerminalDegFlag = 'Y'
LEFT JOIN School school
	ON e.SchoolId = school.Schoolid
	AND hcp.TypeId = school.TypeId

LEFT JOIN Worksite w
	ON wh.WorksiteId = w.WorksiteId
LEFT JOIN City city
	ON w.City = city.City
LEFT JOIN County county
	ON w.CountyId = county.CountyId
LEFT JOIN WorksiteDetail wd
	ON wh.WorksiteId = wd.WorksiteId
	AND wh.TypeId = wd.TypeId
LEFT JOIN PracticeArr pa
	ON wd.ArrId = pa.PracticeArrId

LEFT JOIN SiteType st
	ON wd.SiteTypeId = st.SiteTypeId

LEFT JOIN Specialty s
	ON wh.SpecialtyId = s.SpId
	AND wh.TypeId = s.TypeId

LEFT JOIN Activity a
	ON wh.ActId = a.ActId
	AND wh.TypeId = a.TypeId


-- Transactions table
SELECT
	YEAR(wh.EffectDate) AS Year,
	wh.WorksiteHistoryId,
	wh.SiteLinkId,
	wh.EffectDate,
	wh.AdminDate,
	wh.HcpId,
	hcp.LicenseNumber,
	hcp.BirthDate,
	DATEDIFF(YEAR, hcp.BirthDate, wh.EffectDate)
        - CASE
            WHEN DATEADD(YEAR, DATEDIFF(YEAR, hcp.BirthDate, wh.EffectDate), hcp.BirthDate) > wh.EffectDate
            THEN 1 ELSE 0
            END as AgeAtTransaction,
	hcp.BirthState,
	hcp.BirthCountry,
	hcp.Gender,
	hcp.TypeId,
	wh.WorksiteId,
	w.WorksiteName,
	wh.WorksiteType,
	wh.StatusId,
	wh.TransactionId,
	wh.TrfCountry as RelCountry,
	wh.TrfState as RelState,
	wh.WkHours,
	wh.WkWeeks,
	wh.SpecialtyId,
	s.SpecialtyName,
	wh.ActId,
	a.ActivityName,
	wh.ArrId as PracticeArrId,
	pa.PracticeArrName,
	wd.SiteTypeId,
	st.SiteTypeName,
	wh.Fte,
	w.City,
	city.Population as CityPop,
	w.CountyId,
	county.CountyName,
	county.Population as CountyPopulation,
	w.State,
	e.SchoolId,
	school.SchoolName,
	school.State as SchoolState,
	e.DegreeId,
	e.GradYear,
	hcp.Title,
	wh.FaceTime,
	wh.PctMedicaid,
	wh.PctSlidingFee
FROM WorksiteHistory wh

LEFT JOIN Hcp hcp
	ON wh.HcpId = hcp.HcpId
LEFT JOIN Education e
	ON hcp.HcpId = e.HcpId
	AND hcp.TypeId = e.TypeId
	AND e.TerminalDegFlag = 'Y'
LEFT JOIN School school
	ON e.SchoolId = school.Schoolid
	AND hcp.TypeId = school.TypeId

LEFT JOIN Worksite w
	ON wh.WorksiteId = w.WorksiteId
LEFT JOIN City city
	ON w.City = city.City
LEFT JOIN County county
	ON w.CountyId = county.CountyId
LEFT JOIN WorksiteDetail wd
	ON wh.WorksiteId = wd.WorksiteId
	AND wh.TypeId = wd.TypeId
LEFT JOIN SiteType st
	ON wd.SiteTypeId = st.SiteTypeId
LEFT JOIN PracticeArr pa
	ON wd.ArrId = pa.PracticeArrId

LEFT JOIN Specialty s
	ON wh.SpecialtyId = s.SpId
	AND wh.TypeId = s.TypeId

LEFT JOIN Activity a
	ON wh.ActId = a.ActId
	AND wh.TypeId = a.TypeId
