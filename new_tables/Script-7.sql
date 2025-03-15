WITH s2 AS (
    SELECT
        s.id AS codehour_student_id,
        COALESCE(s.first_name, '') || ' ' || COALESCE(s.last_name, '') AS codehour_student_nm,
        s.username AS codehour_username,
        TO_TIMESTAMP(s.created_at) AS codehour_student_created_dt,
        s.branch_id AS codehour_branch_id,
        SPLIT_PART(REPLACE(SPLIT_PART(SPLIT_PART(s.info::VARCHAR, 'Город: ', 2), 'Пол: ', 1), 'Город: ', ''), ',', 1) AS codehour_city,
        SPLIT_PART(REPLACE(REPLACE(SPLIT_PART(SPLIT_PART(s.info::VARCHAR, 'Класс: ', 2), 'Телефон: ', 1), SPLIT_PART(s.info::VARCHAR, 'Класс: ', 1), ''), 'Класс: ', ''), ',', 1) AS codehour_grade,
        s.email AS codehour_email,
        SPLIT_PART(REPLACE(REPLACE(SPLIT_PART(SPLIT_PART(s.info::VARCHAR, 'Телефон: ', 2), 'Учится в Цифровых навыках: ', 1), SPLIT_PART(s.info::VARCHAR, 'Телефон: ', 1), ''), 'Телефон: ', ''), ',', 1) AS codehour_phone,
        SPLIT_PART(REPLACE(REPLACE(SPLIT_PART(SPLIT_PART(s.info::VARCHAR, 'Учится в Цифровых навыках: ', 2), 'Логин в Цифровых навыках:', 1), SPLIT_PART(s.info::VARCHAR, 'Учится в Цифровых навыках: ', 1), ''), 'Учится в Цифровых навыках: ', ''), ',', 1) AS codehour_is_in_cn,
        SPLIT_PART(REPLACE(REPLACE(SPLIT_PART(SPLIT_PART(s.info::VARCHAR, 'Логин в Цифровых навыках: ', 2), 'Код реферала:', 1), SPLIT_PART(s.info::VARCHAR, 'Логин в Цифровых навыках: ', 1), ''), 'Логин в Цифровых навыках: ', ''), ',', 1) AS codehour_login_in_cn,
        REPLACE(REPLACE(SPLIT_PART(s.info::VARCHAR, 'Код реферала:', 2), SPLIT_PART(s.info::VARCHAR, 'Код реферала:', 1), ''), 'Код реферала:', '') AS codehour_referral_code,
        SPLIT_PART(REPLACE(SPLIT_PART(SPLIT_PART(s.info::VARCHAR, 'Школа: ', 2), 'Класс: ', 1), 'Школа: ', ''), ',', 1) AS codehour_school
    FROM raw_lms.student s
    WHERE s.branch_id = 7767
),
sol AS (
    SELECT
        student_id,
        course_id,
        page_level_type_end AS lesson_type, 
        SUM(msls.page_level_score_completed) / SUM(msls.page_level_score_total) AS solution_rate_scores
    FROM met_oper_dd.main_student_lesson_solution msls
    WHERE msls.is_attended_platform = 1
        AND msls.page_level_score_total > 0
    GROUP BY student_id, course_id, page_level_type_end
),
gs AS (
    SELECT *
    FROM (
        SELECT group_id, student_id, start_time, end_time, ROW_NUMBER() OVER (PARTITION BY student_id ORDER BY start_time DESC) AS rn
        FROM raw_lms.group_student gs
        WHERE gs.status = 0 AND NOW() BETWEEN gs.start_time AND gs.end_time
    ) gs
    WHERE gs.rn = 1
),
gc AS (
    SELECT gc.group_id, gc.course_id, c.name AS course_nm
    FROM (
        SELECT group_id, course_id, created_at, ROW_NUMBER() OVER (PARTITION BY group_id ORDER BY created_at DESC) AS rn
        FROM raw_lms.group_course gc
        WHERE gc.is_active = 1
    ) gc
    LEFT JOIN raw_lms.course c ON c.id = gc.course_id
    WHERE gc.rn = 1
)
SELECT
    s2.*,
    gs.group_id AS codehour_group_id,
    g.title AS codehour_group_nm,
    gc.course_id AS codehour_course_id,
    gc.course_nm AS codehour_course_nm,
    sol2.solution_rate_scores AS codehour_solution_rate_scores,
    CASE
        WHEN sol2.solution_rate_scores > 0 THEN s2.codehour_student_id
        ELSE NULL
    END AS codehour_active_student_id,
    CASE
        WHEN sol2.solution_rate_scores >= 0.95 THEN '95-100'
        WHEN sol2.solution_rate_scores >= 0.90 THEN '90-95'
        WHEN sol2.solution_rate_scores >= 0.85 THEN '85-90'
        WHEN sol2.solution_rate_scores >= 0.80 THEN '80-85'
        WHEN sol2.solution_rate_scores >= 0.75 THEN '75-80'
        WHEN sol2.solution_rate_scores >= 0.70 THEN '70-75'
        WHEN sol2.solution_rate_scores >= 0.65 THEN '65-70'
        WHEN sol2.solution_rate_scores >= 0.60 THEN '60-65'
        WHEN sol2.solution_rate_scores >= 0.55 THEN '55-60'
        WHEN sol2.solution_rate_scores >= 0.50 THEN '50-55'
        WHEN sol2.solution_rate_scores >= 0.40 THEN '40-50'
        WHEN sol2.solution_rate_scores >= 0.30 THEN '30-40'
        WHEN sol2.solution_rate_scores >= 0.20 THEN '20-30'
        WHEN sol2.solution_rate_scores >= 0.10 THEN '10-20'
        WHEN sol2.solution_rate_scores > 0 THEN '00-10'
        ELSE NULL
    END AS codehour_solution_rate_category,
    sol2.lesson_type AS codehour_lesson_type 
FROM s2
LEFT JOIN gs ON gs.student_id = s2.codehour_student_id
LEFT JOIN raw_lms.group g ON g.id = gs.group_id
LEFT JOIN gc ON gc.group_id = gs.group_id
LEFT JOIN sol sol2 ON sol2.student_id = s2.codehour_student_id AND sol2.course_id = gc.course_id;