
--List all the household that have a very large family size
SELECT household_id, family_id, avg(family_size)
from `National_Center_for_Health_Statistics_Modeled.family` 
group by household_id, family_id
having avg(family_size) > 10;


--Gives us overall health score for each family per year
SELECT year, household_id, family_id, SUM(excellent * 5 + very_good * 4 + good * 3 + fair * 2 + poor)/SUM(excellent + very_good + good + fair + poor)
from `National_Center_for_Health_Statistics_Modeled.family_health` 
group by household_id, family_id, year
having sum(excellent + very_good + good + fair + poor) > 0
order by household_id, family_id, year;


--Count of each income level for each family structure
SELECT family_structure_1, SUM(Case when family_income = "$0 - $34,999" then 1 else 0 end), 
SUM(Case when family_income = "$35,000 - $49,999" then 1 else 0 end),
SUM(Case when family_income = "$50,000 - $74,999" then 1 else 0 end),
SUM(Case when family_income = "$75,000 - $99,999" then 1 else 0 end),
SUM(Case when family_income = "$100,000 and over" then 1 else 0 end)
from `National_Center_for_Health_Statistics_Modeled.family` 
group by family_structure_1;

--See a list of household's who have had a large change in family size
SELECT household_id, family_id, MAX(family_size), MIN(family_size)
from `National_Center_for_Health_Statistics_Modeled.family` 
group by family_id, household_id
having MAX(family_size) - MIN(family_size) > 5;

--Gives average amount of elders per healthcare_cost
SELECT healthcare_cost, avg(elders)
from `National_Center_for_Health_Statistics_Modeled.family` 
where healthcare_cost <> "Don't know" and healthcare_cost <> "Refused" and healthcare_cost <> "Not ascertained"
group by healthcare_cost
order by healthcare_cost;

--Gives the Avg amound of elders for the sum of limitations for a household
SELECT (walk_difficulty + memory_problem + general_limitation) as score, avg(elders)
from `National_Center_for_Health_Statistics_Modeled.family` 
where healthcare_cost <> "Don't know" and healthcare_cost <> "Refused" and healthcare_cost <> "Not ascertained"
group by score
order by score;