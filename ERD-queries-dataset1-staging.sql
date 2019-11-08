SELECT count(distinct hhx) FROM National_Center_for_Health_Statistics_staging.Household_2011;
SELECT count(*) FROM National_Center_for_Health_Statistics_staging.Household_2011;

SELECT h.hhx as hhx1, f.hhx as hhx2 FROM National_Center_for_Health_Statistics_staging.Household_2011 as h
left join National_Center_for_Health_Statistics_staging.Family_2011 as f
on h.hhx = f.hhx;

SELECT f1.hhx as hhx1, f2.hhx as hhx2 FROM National_Center_for_Health_Statistics_staging.Family_2011 as f1
left join National_Center_for_Health_Statistics_staging.Family_2010 as f2
on f1.hhx = f2.hhx;

SELECT f1.hhx as hhx1, f2.hhx as hhx2 FROM National_Center_for_Health_Statistics_staging.Family_2011 as f1
left join National_Center_for_Health_Statistics_staging.Family_2012 as f2
on f1.hhx = f2.hhx;

SELECT f.hhx as hhx1, f.fmx as fmx1, p.hhx as hhx2, p.fmx as fmx2 FROM National_Center_for_Health_Statistics_staging.Family_2011 as f
left join National_Center_for_Health_Statistics_staging.Person_2011 as p
on f.hhx = p.hhx and f.fmx =p.fmx;
