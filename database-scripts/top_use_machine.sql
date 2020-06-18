-- check the top use machine in the past hour
-- TO DO: result josin

SELECT household_id, machine_id,  SUM(usage) AS sum_of_usage FROM testing
WHERE timestamp >= TO_TIMESTAMP('2015-01-01 2:30:20','YYYY-MM-DD HH:MI:SS') - INTERVAL '1 HOUR'
GROUP BY household_id, machine_id
ORDER BY sum_of_usage DESC
LIMIT 10; 