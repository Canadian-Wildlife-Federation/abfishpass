----------------------------------------------------------------------------------------------
-- @Date: Jan 26, 2023
-- @Author: Katherine O'Hearn

-- this set of queries will generate a set of all barriers and their
-- downstream barriers given a limit on the count of barriers downstream
-- and the limit of barriers to be selected

-- the limit on barriers to be selected can be determined in QGIS using the Statistics tool
-- simply set the field to func_upstr_hab_all and toggle on 'calculate for selected features'
-- you can then select the top X barriers required to reach the desired amount of habitat

-- if you want to manually select barriers and get all the barriers
-- downstream of them, simply comment out the creation of the hydro.barrier_limit[x] table
-- and edit / use the remaining queries to select the downstream barrier ids from a
-- manually created table instead
----------------------------------------------------------------------------------------------

CREATE TABLE hydro.berland_wildhay_allbarriers
AS (
	SELECT * FROM ws17010301.barriers
	UNION
	SELECT * FROM ws17010302.barriers
	);

ALTER TABLE hydro.berland_wildhay_allbarriers DROP COLUMN original_point;

CREATE TABLE hydro.priority_barrier_limit4
AS (
    SELECT * FROM hydro.berland_wildhay_allbarriers
    WHERE passability_status != 'PASSABLE'
    and func_upstr_hab_all > 0
    and barrier_cnt_downstr <= 4 --choose your limit on the count of barriers downstream
    ORDER BY func_upstr_hab_all DESC LIMIT 15 --pick the X highest values in the func_upstr_hab_all column (see instructions above)
);

-- SELECT * FROM hydro.priority_barrier_limit4;

WITH additional AS (
    SELECT unnest(barriers_downstr) AS ds FROM hydro.priority_barrier_limit4
),

ds_ids AS (
SELECT DISTINCT id
FROM hydro.berland_wildhay_allbarriers
WHERE id::varchar IN (SELECT ds FROM additional)
AND id NOT IN (SELECT id FROM hydro.priority_barrier_limit4)
)

-- uncomment to get count and sum of additional functional upstream habitat before inserting into table
-- SELECT count(*) AS count, SUM(DISTINCT func_upstr_hab_all) AS sum_habitat FROM hydro.berland_wildhay_allbarriers WHERE id IN (SELECT id FROM ds_ids);

INSERT INTO hydro.priority_barrier_limit4
(SELECT * FROM hydro.berland_wildhay_allbarriers WHERE id IN (SELECT id FROM ds_ids));

ALTER TABLE hydro.priority_barrier_limit4 ADD CONSTRAINT limit4_pkey PRIMARY KEY (id);

-- *******assign barrier groups manually now, then move on to next step*******

-- calculate average gain after assigning groups
SELECT "group", COUNT(distinct id) as b_count, sum(func_upstr_hab_all) AS sum_habitat
FROM hydro.priority_barrier_limit4
GROUP BY "group";