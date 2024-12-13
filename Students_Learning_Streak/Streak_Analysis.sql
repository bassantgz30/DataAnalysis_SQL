USE learning_streaks;

/* 
************************************************************************

GOAL:
	To identify the top learners in an online learning platform based on daily streaks.

Columns:
	 streak_id: Unique id for each streak record.
     student_id: Unique id for each student.
     streak_active: Boolean, indicating whether the streak is currently active (1) or not (0).
     streak_frozen: Boolean, indicating whether the streak is currently frozen (1) or not (0).
     streak_created: The date when the streak was started or updated.
     
Metric's Calculation:
	- The streak duration increments each day the student remains active, and they haven’t frozen their streak. 
    - The duration is not extended when the student is not active or when the streak is frozen.
************************************************************************
*/


-- ===================================
-- (1) Understand the Data Structure:
-- ===================================
DESCRIBE students_streaks;

SELECT * FROM students_streaks;  -- 20174

SELECT COUNT(DISTINCT streak_id) FROM students_streaks;  -- 20174  >> streaks are unique.
SELECT COUNT(distinct student_id) FROM students_streaks;  -- 6638 >> one student may have different streaks.

-- =====================
-- (2) Data Cleaning:
-- =====================

-- Staging step: Creating a copy of the raw data to work on. No changes will be applied to the raw data.
DROP TABLE IF EXISTS students_streaks_staging;
CREATE TABLE students_streaks_staging
LIKE students_streaks;

INSERT INTO students_streaks_staging
SELECT *
FROM students_streaks;

-- Replace empty strings with NULL values (only the streak_created has a text datatype).
UPDATE students_streaks_staging
SET streak_created = NULL
WHERE streak_created = '';

-- Count null values
SELECT 
	COUNT(*) - COUNT(streak_id) As streak_id, 
    COUNT(*) - COUNT(student_id) As student_id, 
    COUNT(*) - COUNT(streak_active) As streak_active,
	COUNT(*) - COUNT(streak_frozen) As streak_frozen, 
    COUNT(*) - COUNT(streak_created) As streak_created
FROM students_streaks_staging;

-- When streak_active is null, set it to 0
UPDATE students_streaks_staging
SET streak_active = 0
WHERE streak_active IS NULL;


-- Set the streak_created column to 'Date' type
UPDATE students_streaks_staging
SET streak_created = STR_TO_DATE(streak_created, '%m/%d/%Y %H:%i');

ALTER TABLE students_streaks_staging
MODIFY COLUMN `streak_created` DATETIME;

UPDATE students_streaks_staging
SET streak_created = DATE(streak_created);

ALTER TABLE students_streaks_staging
MODIFY COLUMN `streak_created` DATE;

SELECT * FROM students_streaks_staging;


-- ==============================
-- (3) Finding the top students:
-- ==============================

-- Staging step: Creating a copy of students_streaks_staging but sorted by student_id and streak_created date.
DROP TABLE IF EXISTS students_streaks_staging_sorted;
CREATE TABLE students_streaks_staging_sorted
LIKE students_streaks_staging;

INSERT INTO students_streaks_staging_sorted
SELECT *
FROM students_streaks_staging
ORDER BY student_id, streak_created;

SELECT * FROM students_streaks_staging_sorted;


-- Each student may have different streaks,
/*
## Senarios are:
	- active, not frozen, consicutive days.
    - active, not frozen, separated days.
    - active, frozen
    - not active
    
    We are interested in the first case only for now, so reset streak to 1 for any other case.
    
## We need variables to track the previous date, id, active state.
*/
-- =========================

-- Definig the variables:
SET @prev_student_id = 0;
SET @prev_streak_active = 0;
SET @prev_streak_created = NULL;

DROP TABLE IF EXISTS active_non_frozen_daily_streaks;
CREATE TEMPORARY TABLE active_non_frozen_daily_streaks AS (
  SELECT 
    student_id,
    streak_created,
    streak_active,
    streak_frozen,
    (
      -- Check if the same student has an active streak continuing from the previous day and not frozen
      CASE
        WHEN @prev_student_id = student_id AND 
			@prev_streak_active = 1 AND 
            streak_active = 1 AND 
            streak_frozen = 0 AND 
            DATEDIFF(streak_created, @prev_streak_created) = 1
        THEN @current_streak := @current_streak + 1

        -- Reset the streak to 1
        ELSE @current_streak := 1
      END
    ) AS streak_length,
    
    -- Update the values of previous row variables
    @prev_student_id := student_id,
    @prev_streak_active := streak_active,
    @prev_streak_created := streak_created
  FROM
   students_streaks_staging_sorted  
  ORDER BY
    student_id, streak_created
);

SELECT * FROM active_non_frozen_daily_streaks;

-- Get the max daily streak for each student
SELECT student_id, 
	MAX(streak_length) AS max_streak
FROM active_non_frozen_daily_streaks
GROUP BY student_id
ORDER BY max_streak DESC;

-- Get the longest three daily streaks
SELECT DISTINCT streak_length as top_streaks
FROM active_non_frozen_daily_streaks
ORDER BY top_streaks DESC
LIMIT 3;


