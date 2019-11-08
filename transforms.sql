(SELECT Family_2010.srvy_yr as year, Family_2010.hhx as household_id, Family_2010.fmx as family_id, Family_2010.wtfa_fam as final_annual_weight, Family_2010.wrkceln as cell_phones, Family_2010.fm_size as family_size, Family_2010.fm_kids as kids, Family_2010.fm_eldr as elders, SUBSTR(Family_2010.fm_type, 3) as family_type, SUBSTR(Family_2010.fm_strcp, 3) as family_structure_1, SUBSTR(Family_2010.fm_strp, 3) as family_structure_2, SUBSTR(Family_2010.fm_educ1, 3) as family_education, Family_2010.fwalkct as walk_difficulty, Family_2010.frememct as memory_problem, Family_2010.fanylct as general_limitation, SUBSTR(Family_2010.fsrunout, 3) as food_runout, Family_2010.incgrp2 as food_shortage, SUBSTR(Family_2010.fsbalanc, 3) as balance_meals, SUBSTR(Family_2010.fhicost, 3) as healthcare_cost, SUBSTR(Family_2010.incgrp2, 4) as family_income
from National_Center_for_Health_Statistics_staging.Family_2010
inner join National_Center_for_Health_Statistics_staging.Family_2011
on Family_2010.hhx = Family_2011.hhx
where Family_2011.wtfa_fam is not null and (Family_2011.curwrkn = '1 Yes' or Family_2011.curwrkn = '2 No') and Family_2011.fm_size > 0 and Family_2011.fm_strcp <> '99 Unknown' and Family_2011.fm_strp <> '99 Unknown' and Family_2011.fm_educ1 <> '97 Refused' and Family_2011.fm_educ1 <> '98 Not ascertained' and Family_2011.fm_educ1 <> "99 Don't know" and Family_2011.fwalkyn <> '7 Refused' and Family_2011.fwalkyn <> "9 Don’t Know" and Family_2011.frememyn <> '7 Refused' and Family_2011.frememyn <> "9 Don’t Know" and Family_2011.fanylyn <> '7 Refused' and Family_2011.fanylyn <> "9 Don’t Know" and Family_2011.fsrunout <> '7 Refused' and Family_2011.fsrunout <> "9 Don’t Know" and Family_2011.fslast <> '7 Refused' and Family_2011.fslast<> "9 Don’t Know" and Family_2011.fsbalanc <> '7 Refused' and Family_2011.fsbalanc <> "9 Don’t Know" and Family_2011.fhicost <> '7 Refused' and Family_2011.fhicost <> "9 Don’t Know" and Family_2011.incgrp2 <> '99 Unknown'
order by Family_2010.hhx)
UNION ALL
(SELECT Family_2012.srvy_yr as year, Family_2012.hhx as household_id, Family_2012.fmx as family_id, Family_2012.wtfa_fam as final_annual_weight, Family_2012.wrkceln as cell_phones, Family_2012.fm_size as family_size, Family_2012.fm_kids as kids, Family_2012.fm_eldr as elders, SUBSTR(Family_2012.fm_type, 3) as family_type, SUBSTR(Family_2012.fm_strcp, 3) as family_structure_1, SUBSTR(Family_2012.fm_strp, 3) as family_structure_2, SUBSTR(Family_2012.fm_educ1, 3) as family_education, Family_2012.fwalkct as walk_difficulty, Family_2012.frememct as memory_problem, Family_2012.fanylct as general_limitation, SUBSTR(Family_2012.fsrunout, 3) as food_runout, SUBSTR(Family_2012.fslast, 3) as food_shortage, SUBSTR(Family_2012.fsbalanc, 3) as balance_meals, SUBSTR(Family_2012.fhicost, 3) as healthcare_cost, SUBSTR(Family_2012.incgrp2, 4) as family_income
from National_Center_for_Health_Statistics_staging.Family_2012
inner join National_Center_for_Health_Statistics_staging.Family_2011
on Family_2012.hhx = Family_2011.hhx
where Family_2011.wtfa_fam is not null and (Family_2011.curwrkn = '1 Yes' or Family_2011.curwrkn = '2 No') and Family_2011.fm_size > 0 and Family_2011.fm_strcp <> '99 Unknown' and Family_2011.fm_strp <> '99 Unknown' and Family_2011.fm_educ1 <> '97 Refused' and Family_2011.fm_educ1 <> '98 Not ascertained' and Family_2011.fm_educ1 <> "99 Don't know" and Family_2011.fwalkyn <> '7 Refused' and Family_2011.fwalkyn <> "9 Don’t Know" and Family_2011.frememyn <> '7 Refused' and Family_2011.frememyn <> "9 Don’t Know" and Family_2011.fanylyn <> '7 Refused' and Family_2011.fanylyn <> "9 Don’t Know" and Family_2011.fsrunout <> '7 Refused' and Family_2011.fsrunout <> "9 Don’t Know" and Family_2011.fslast <> '7 Refused' and Family_2011.fslast<> "9 Don’t Know" and Family_2011.fsbalanc <> '7 Refused' and Family_2011.fsbalanc <> "9 Don’t Know" and Family_2011.fhicost <> '7 Refused' and Family_2011.fhicost <> "9 Don’t Know" and Family_2011.incgrp2 <> '99 Unknown'
order by Family_2012.hhx)
UNION ALL
(SELECT srvy_yr as year, hhx as household_id, fmx as family_id, wtfa_fam as final_annual_weight, wrkceln as cell_phones, fm_size as family_size, fm_kids as kids, fm_eldr as elders, SUBSTR(fm_type, 3) as family_type, SUBSTR(fm_strcp, 3) as family_structure_1, SUBSTR(fm_strp, 3) as family_structure_2, SUBSTR(fm_educ1, 3) as family_education, fwalkct as walk_difficulty, frememct as memory_problem, fanylct as general_limitation, SUBSTR(fsrunout, 3) as food_runout, SUBSTR(fslast, 3) as food_shortage, SUBSTR(fsbalanc, 3) as balance_meals, SUBSTR(fhicost, 3) as healthcare_cost, SUBSTR(incgrp2, 4) as family_income
from National_Center_for_Health_Statistics_staging.Family_2011
where wtfa_fam is not null and (curwrkn = '1 Yes' or curwrkn = '2 No') and fm_size > 0 and fm_strcp <> '99 Unknown' and fm_strp <> '99 Unknown' and fm_educ1 <> '97 Refused' and fm_educ1 <> '98 Not ascertained' and fm_educ1 <> "99 Don't know" and fwalkyn <> '7 Refused' and fwalkyn <> "9 Don’t Know" and frememyn <> '7 Refused' and frememyn <> "9 Don’t Know" and fanylyn <> '7 Refused' and fanylyn <> "9 Don’t Know" and fsrunout <> '7 Refused' and fsrunout <> "9 Don’t Know" and fslast <> '7 Refused' and fslast<> "9 Don’t Know" and fsbalanc <> '7 Refused' and fsbalanc <> "9 Don’t Know" and fhicost <> '7 Refused' and fhicost <> "9 Don’t Know" and incgrp2 <> '99 Unknown'
order by hhx)
order by household_id;




SELECT Person_2011.hhx as househould_id, Person_2011.fmx as family_id, Person_2011.fpx as person_id, Person_2011.sex, SUBSTR(Person_2011.racerpi2, 3) as race, SUBSTR(Person_2011.rrp, 3) as household_relationship, Person_2011.age_p as age, SUBSTR(Person_2011.plawklim, 3) as work_limited, SUBSTR(Person_2011.plawalk, 3) as walking_issues, SUBSTR(Person_2011.plaremem, 3) as memory_issues, Person_2011.phstat as health
from National_Center_for_Health_Statistics_staging.Person_2011
inner join National_Center_for_Health_Statistics_staging.Family_2011
on Person_2011.hhx = Family_2011.hhx and Person_2011.fmx = Family_2011.fmx
where Family_2011.wtfa_fam is not null and (Family_2011.curwrkn = '1 Yes' or Family_2011.curwrkn = '2 No') and Family_2011.fm_size > 0 and Family_2011.fm_strcp <> '99 Unknown' and Family_2011.fm_strp <> '99 Unknown' and Family_2011.fm_educ1 <> '97 Refused' and Family_2011.fm_educ1 <> '98 Not ascertained' and Family_2011.fm_educ1 <> "99 Don't know" and Family_2011.fwalkyn <> '7 Refused' and Family_2011.fwalkyn <> "9 Don’t Know" and Family_2011.frememyn <> '7 Refused' and Family_2011.frememyn <> "9 Don’t Know" and Family_2011.fanylyn <> '7 Refused' and Family_2011.fanylyn <> "9 Don’t Know" and Family_2011.fsrunout <> '7 Refused' and Family_2011.fsrunout <> "9 Don’t Know" and Family_2011.fslast <> '7 Refused' and Family_2011.fslast<> "9 Don’t Know" and Family_2011.fsbalanc <> '7 Refused' and Family_2011.fsbalanc <> "9 Don’t Know" and Family_2011.fhicost <> '7 Refused' and Family_2011.fhicost <> "9 Don’t Know" and Family_2011.incgrp2 <> '99 Unknown';




SELECT hhx as household_id, wtfa_hh as final_weight, SUBSTR(livqrt,4) as house_type, acpt_fam as responsive_family, rej_fam as non_responsive_family, 
acpt_per as responsive_people, rej_per as non_responsive_people, acptchld as responsive_children, SUBSTR(region, 3) as region
from National_Center_for_Health_Statistics_staging.Household_2011
where non_intv is null
order by household_id;




((SELECT hhx as household_id, fhstatex as excellent, fhstatvg as very_good, fhstatg as good, fhstatfr as fair, fhstatpr as poor, srvy_yr as year
from National_Center_for_Health_Statistics_staging.Family_2011
where wtfa_fam is not null and (curwrkn = '1 Yes' or curwrkn = '2 No') and fm_size > 0 and fm_strcp <> '99 Unknown' and fm_strp <> '99 Unknown' and fm_educ1 <> '97 Refused' and fm_educ1 <> '98 Not ascertained' and fm_educ1 <> "99 Don't know" and fwalkyn <> '7 Refused' and fwalkyn <> "9 Don’t Know" and frememyn <> '7 Refused' and frememyn <> "9 Don’t Know" and fanylyn <> '7 Refused' and fanylyn <> "9 Don’t Know" and fsrunout <> '7 Refused' and fsrunout <> "9 Don’t Know" and fslast <> '7 Refused' and fslast<> "9 Don’t Know" and fsbalanc <> '7 Refused' and fsbalanc <> "9 Don’t Know" and fhicost <> '7 Refused' and fhicost <> "9 Don’t Know" and incgrp2 <> '99 Unknown'
order by hhx)
UNION ALL
(SELECT Family_2012.hhx as household_id, Family_2012.fhstatex as excellent, Family_2012.fhstatvg as very_good, Family_2012.fhstatg as good, Family_2012.fhstatfr as fair, Family_2012.fhstatpr as poor, Family_2012.srvy_yr as year
from National_Center_for_Health_Statistics_staging.Family_2012
inner join National_Center_for_Health_Statistics_staging.Family_2011
on Family_2012.hhx = Family_2011.hhx
order by Family_2012.hhx)
UNION ALL
(SELECT Family_2010.hhx as household_id, Family_2010.fhstatex as excellent, Family_2010.fhstatvg as very_good, Family_2010.fhstatg as good, Family_2010.fhstatfr as fair, Family_2010.fhstatpr as poor, Family_2010.srvy_yr as year
from National_Center_for_Health_Statistics_staging.Family_2010
inner join National_Center_for_Health_Statistics_staging.Family_2011
on Family_2010.hhx = Family_2011.hhx
order by Family_2010.hhx))
order by household_id;






