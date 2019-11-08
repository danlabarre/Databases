-- Search the IDs of households which receive the survey in both year 2010 and 2011 ordered by increasing household ID in 2010
SELECT distinct m.household_id
FROM `deep-byte-252822.National_Center_for_Health_Statistics_Modeled.family` m join `deep-byte-252822.National_Center_for_Health_Statistics_Modeled.family` n on m.household_id = n.household_id
where (m.year =2010 and n.year = 2011) or (m.year=2011 and n.year=2010)
order by m.household_id

-- Search the IDs of households which receive the survey in both year 2011 and 2012 ordered by increasing household ID in 2011
SELECT distinct m.household_id
from `deep-byte-252822.National_Center_for_Health_Statistics_Modeled.family` m join `deep-byte-252822.National_Center_for_Health_Statistics_Modeled.family` n on m.household_id = n.household_id
Where m.year= 2011 and n.year=2012
order by m.household_id;

-- Search the IDs of households which wrote "household survey" but did not wrote "family survey" in year 2011 ordered by increasing household ID in household survey. 
SELECT distinct m.household_id, n.region
from `deep-byte-252822.National_Center_for_Health_Statistics_Modeled.family` m left join `deep-byte-252822.National_Center_for_Health_Statistics_Modeled.household` n on m.household_id = n.household_id
Where m.year= 2011 
order by m.household_id;

-- Search the living region of households surveyed in Family_2011 ordered by increasing household ID. If one household didn't write down region info, the region for it would be Null.
SELECT distinct n.household_id
from `deep-byte-252822.National_Center_for_Health_Statistics_Modeled.family` m right outer join `deep-byte-252822.National_Center_for_Health_Statistics_Modeled.household` n on m.household_id = n.household_id
Where m.year=2011 and m.household_id is Null
order by n.household_id;

-- Search in which household no person wrote "person survey" and people who didn't belong to any household surveyed in year 2011. Person ID with lack of household will be showed at first.
SELECT m.household_id, n.person_id
from `deep-byte-252822.National_Center_for_Health_Statistics_Modeled.family` m full join `deep-byte-252822.National_Center_for_Health_Statistics_Modeled.person` n on m.household_id = n.househould_id 
Where m.year=2011 and (m.household_id is Null or n.person_id is Null)
ORDER by (CASE 
              WHEN m.household_id IS NULL THEN 0 
              ELSE 99999 
          END), m.household_id;

-- Search which household in "family survey" in 2011 and took "household survy" also took "family survey" in 2012 again.  
SELECT m.household_id 
from (`deep-byte-252822.National_Center_for_Health_Statistics_Modeled.family` m join `deep-byte-252822.National_Center_for_Health_Statistics_Modeled.household` n on m.household_id = n.household_id ) join `deep-byte-252822.National_Center_for_Health_Statistics_Modeled.family` l on m.household_id = l.household_id 
Where (m.year=2011 and l.year=2012) 
ORDER by m.household_id;
