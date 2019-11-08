-- Search the ID of family which write down number of cellphones in year 2010
SELECT m.hhx 
FROM `deep-byte-252822.National_Center_for_Health_Statistics_staging.Family_2010` m
where not (m.cellout is null);

-- Search the ID of household which didn't write down number of cellphones in year 2011 with increasing order by household ID
SELECT m.hhx 
FROM `deep-byte-252822.National_Center_for_Health_Statistics_staging.Family_2011` m
where (m.cellout is null)
order by m.hhx;

-- Search whether a household use cellphone when it claims to have no telephone in year 2012 
SELECT m.telceln 
FROM `deep-byte-252822.National_Center_for_Health_Statistics_staging.Family_2012` m
where (teln_flg = "2 No telephone");

-- Search the living quarters of households catagorized by regions from area 4 to area 1 in year 2012 
SELECT m.livqrt, m.region
FROM `deep-byte-252822.National_Center_for_Health_Statistics_staging.Household_2011` m
order by region DESC;

-- Search household ID if is has person who has both walk and memory issue in year 2011 with increasing order by household ID
SELECT m.hhx
FROM `deep-byte-252822.National_Center_for_Health_Statistics_staging.Person_2011` m
where m.plawalk = "1 Yes" or m.plaremem="1 Yes"
order by m.hhx;
