--Top 5 Stadiums by capacity
SELECT top 5 rank, stadium, capacity FROM stadiums
ORDER BY capacity DESC

--Average capacity of the stadiums by region
SELECT region, AVG(capacity) AS "Average_Capacity" FROM stadiums
GROUP BY region
ORDER BY "Average_Capacity" DESC

--Count the number of stadiums in each country
SELECT country, COUNT(country) AS "stadium_count" FROM stadiums
GROUP BY country
ORDER BY "stadium_count" DESC;

--Stadium ranking with each region
SELECT rank, region, stadium, capacity,
  RANK() OVER(PARTITION BY region ORDER BY capacity DESC) AS "region_rank"
FROM stadiums;

--Top 3 stadium ranking with each region
SELECT rank, region, stadium, capacity, "region_rank" FROM (
SELECT rank, region, stadium, capacity,
  RANK() OVER(PARTITION BY region ORDER BY capacity DESC) AS "region_rank"
FROM stadiums) AS "ranked_stadiums"
WHERE region_rank <= 3;

--Stadiums with capacity above the average
SELECT stadium, t2.region, capacity, avg_capacity
FROM stadiums, (SELECT region, AVG(capacity) AS avg_capacity FROM stadiums GROUP BY region) t2
WHERE t2.region = stadiums.region
AND capacity > avg_capacity;

--Stadiums with closes capacity to regional median
WITH MedianCTE AS (
    SELECT region, PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY capacity) OVER (PARTITION BY region) AS median_capacity
    FROM stadiums
)
SELECT rank, stadium, region, capacity, ranked_stadiums.median_capacity, ranked_stadiums.median_rank
FROM (
    SELECT s.rank, s.stadium, s.region, s.capacity,m.median_capacity,
    ROW_NUMBER() OVER (PARTITION BY s.region ORDER BY ABS(s.capacity - m.median_capacity)) AS median_rank
    FROM stadiums s JOIN MedianCTE m ON s.region = m.region
) ranked_stadiums
WHERE median_rank = 1;
