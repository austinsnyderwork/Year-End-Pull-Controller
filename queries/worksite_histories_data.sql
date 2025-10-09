
SELECT
	wh.SiteLinkId,
	wh.EffectDate,
	wh.AdminDate,
	wh.HcpId,
	hcp.LicenseNumber,
	hcp.BirthDate,
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