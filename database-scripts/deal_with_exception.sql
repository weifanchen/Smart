-- delete abnormal event


BEGIN;
-- let's create a temp table to bulk data into
create temporary table temp_json (values text) on commit drop;
copy temp_json from '/home/ubuntu/machine_porfile.json';

-- uncomment the line above to insert records into your table
-- insert into tbl_staging_eventlog1 ("EId", "Category", "Mac", "Path", "ID") 

select values->>'EId' as EId,
       values->>'Category' as Category,
       values->>'Mac' as Mac,
       values->>'Path' as Path,
       values->>'ID' as ID      
from   (
           select json_array_elements(replace(values,'\','\\')::json) as values 
           from   temp_json
       ) a; 
COMMIT;

COPY events(timestamp,machine_id,household_id,usage) 
FROM '/home/ubuntu/usage_newschema.csv' DELIMITER ',' CSV HEADER;