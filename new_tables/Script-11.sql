DROP VIEW IF EXISTS az_oper_rep.az_codehour_cn_multiusers;
CREATE OR REPLACE VIEW az_oper_rep.az_codehour_cn_multiusers AS
WITH s1 AS (
    SELECT
        s.id AS student_id,
        COALESCE(s.first_name, '') || ' ' || COALESCE(s.last_name, '') AS student_nm,
        s.username,
        s.age,
        SPLIT_PART(REPLACE(SPLIT_PART(SPLIT_PART(s.info::VARCHAR, 'Город: ', 2), 'Пол: ', 1), 'Город: ', ''), ',', 1) AS city,
        SPLIT_PART(REPLACE(REPLACE(SPLIT_PART(SPLIT_PART(s.info::VARCHAR, 'Класс: ', 2), 'Телефон: ', 1), SPLIT_PART(s.info::VARCHAR, 'Класс: ', 1), ''), 'Класс: ', ''), ',', 1) AS grade,
        s.email,
        SPLIT_PART(REPLACE(REPLACE(SPLIT_PART(SPLIT_PART(s.info::VARCHAR, 'Телефон: ', 2), 'Учится в Цифровых навыках: ', 1), SPLIT_PART(s.info::VARCHAR, 'Телефон: ', 1), ''), 'Телефон: ', ''), ',', 1) AS phone,
        SPLIT_PART(REPLACE(REPLACE(SPLIT_PART(SPLIT_PART(s.info::VARCHAR, 'Учится в Цифровых навыках: ', 2), 'Логин в Цифровых навыках:', 1), SPLIT_PART(s.info::VARCHAR, 'Учится в Цифровых навыках: ', 1), ''), 'Учится в Цифровых навыках: ', ''), ',', 1) AS is_in_cn,
        SPLIT_PART(REPLACE(REPLACE(SPLIT_PART(SPLIT_PART(s.info::VARCHAR, 'Логин в Цифровых навыках: ', 2), 'Код реферала:', 1), SPLIT_PART(s.info::VARCHAR, 'Логин в Цифровых навыках: ', 1), ''), 'Логин в Цифровых навыках: ', ''), ',', 1) AS login_in_cn,
        REPLACE(REPLACE(SPLIT_PART(s.info::VARCHAR, 'Код реферала:', 2), SPLIT_PART(s.info::VARCHAR, 'Код реферала:', 1), ''), 'Код реферала:', '') AS referral_code,
        SPLIT_PART(REPLACE(SPLIT_PART(SPLIT_PART(s.info::VARCHAR, 'Школа: ', 2), 'Класс: ', 1), 'Школа: ', ''), ',', 1) AS school,
        s.status,
        s.password_text AS password,
        TO_TIMESTAMP(s.created_at) AS student_created_dt,
        sr.group_id,
        sr.group_nm,
        sr.course_id,
        sr.course_nm,
        sr.course_language,
        sr.school_grade,
        sr.school_type,
        sr.user_id AS teacher_id,
        sr.user_nm AS teacher_nm,
        sr.sub_3_branch_id,
        sr.sub_4_branch_id,
        sr.sub_3_branch_title,
        sr.sub_4_branch_title,
        s.utis_code
    FROM az_oper_dd.lms_student_readiness sr
    JOIN raw_lms.student s ON s.id = sr.student_id
    WHERE sr.group_id IS NOT NULL
        AND sr.course_id IS NOT NULL
        AND sr.school_type IN ('Средняя школа', 'Старшая школа')
),
s2 AS (
    SELECT
        s.id AS codehour_student_id,
        COALESCE(s.first_name, '') || ' ' || COALESCE(s.last_name, '') AS codehour_student_nm,
        s.username AS codehour_username,
        s.age AS codehour_age,
        SPLIT_PART(REPLACE(SPLIT_PART(SPLIT_PART(s.info::VARCHAR, 'Город: ', 2), 'Пол: ', 1), 'Город: ', ''), ',', 1) AS codehour_city,
        SPLIT_PART(REPLACE(REPLACE(SPLIT_PART(SPLIT_PART(s.info::VARCHAR, 'Класс: ', 2), 'Телефон: ', 1), SPLIT_PART(s.info::VARCHAR, 'Класс: ', 1), ''), 'Класс: ', ''), ',', 1) AS codehour_grade,
        s.email AS codehour_email,
        SPLIT_PART(REPLACE(REPLACE(SPLIT_PART(SPLIT_PART(s.info::VARCHAR, 'Телефон: ', 2), 'Учится в Цифровых навыках: ', 1), SPLIT_PART(s.info::VARCHAR, 'Телефон: ', 1), ''), 'Телефон: ', ''), ',', 1) AS codehour_phone,
        SPLIT_PART(REPLACE(REPLACE(SPLIT_PART(SPLIT_PART(s.info::VARCHAR, 'Учится в Цифровых навыках: ', 2), 'Логин в Цифровых навыках:', 1), SPLIT_PART(s.info::VARCHAR, 'Учится в Цифровых навыках: ', 1), ''), 'Учится в Цифровых навыках: ', ''), ',', 1) AS codehour_is_in_cn,
        SPLIT_PART(REPLACE(REPLACE(SPLIT_PART(SPLIT_PART(s.info::VARCHAR, 'Логин в Цифровых навыках: ', 2), 'Код реферала:', 1), SPLIT_PART(s.info::VARCHAR, 'Логин в Цифровых навыках: ', 1), ''), 'Логин в Цифровых навыках: ', ''), ',', 1) AS codehour_login_in_cn,
        REPLACE(REPLACE(SPLIT_PART(s.info::VARCHAR, 'Код реферала:', 2), SPLIT_PART(s.info::VARCHAR, 'Код реферала:', 1), ''), 'Код реферала:', '') AS codehour_referral_code,
        SPLIT_PART(REPLACE(SPLIT_PART(SPLIT_PART(s.info::VARCHAR, 'Школа: ', 2), 'Класс: ', 1), 'Школа: ', ''), ',', 1) AS codehour_school,
        s.status AS codehour_status,
        s.password_text AS codehour_password,
        TO_TIMESTAMP(s.created_at) AS codehour_student_created_dt,
        s.branch_id AS codehour_branch_id,
        s.utis_code AS codehour_utis_code
    FROM raw_lms.student s
    WHERE s.branch_id = 7833
),
sol AS (
    SELECT
        msls.student_id,
        msls.course_id,
        cl.is_bonus, 
        SUM(msls.page_level_score_completed) / SUM(msls.page_level_score_total) AS solution_rate_scores
    FROM met_oper_dd.main_student_lesson_solution msls
    LEFT JOIN raw_lms.course_lesson cl ON msls.course_id = cl.course_id AND msls.lesson_id = cl.lesson_id 
    WHERE msls.is_attended_platform = 1
        AND msls.page_level_score_total > 0
    GROUP BY msls.student_id, msls.course_id, cl.is_bonus 
),
raw AS (
    SELECT
        s1.*,
        sol1.solution_rate_scores AS cn_solution_rate_scores,
        s2.*,
        gs.group_id AS codehour_group_id,
        g.title AS codehour_group_nm,
        gc.course_id AS codehour_course_id,
        gc.course_nm AS codehour_course_nm,
        sol2.solution_rate_scores AS codehour_solution_rate_scores,
        ROW_NUMBER() OVER (PARTITION BY s1.student_id ORDER BY COALESCE(sol2.solution_rate_scores, 0) DESC, s2.codehour_student_id DESC) AS rn,
        CASE
            WHEN sol2.is_bonus = 0 THEN 'Обычный'
            WHEN sol2.is_bonus = 1 THEN 'Бонусный'
            ELSE 'Неизвестно'
        END AS codehour_lesson_type 
    FROM s1
    LEFT JOIN (
        SELECT *
        FROM raw_lms.multiuser_profile mp1
        WHERE owner_type = 'student'
    ) mp1 ON mp1.owner_id = s1.student_id
    LEFT JOIN (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY multiuser_id ORDER BY id DESC) AS rn
        FROM raw_lms.multiuser_profile mp2
        WHERE owner_type = 'student'
    ) mp2 ON mp1.multiuser_id = mp2.multiuser_id AND mp1.owner_id != mp2.owner_id
    LEFT JOIN s2 ON mp2.owner_id = s2.codehour_student_id
    LEFT JOIN (
        SELECT *
        FROM (
            SELECT group_id, student_id, start_time, end_time, ROW_NUMBER() OVER (PARTITION BY student_id ORDER BY start_time DESC) AS rn
            FROM raw_lms.group_student gs
            WHERE gs.status = 0 AND NOW() BETWEEN gs.start_time AND gs.end_time
        ) gs
        WHERE gs.rn = 1
    ) gs ON gs.student_id = s2.codehour_student_id
    LEFT JOIN raw_lms.group g ON g.id = gs.group_id
    LEFT JOIN (
        SELECT gc.group_id, gc.course_id, c.name AS course_nm
        FROM (
            SELECT group_id, course_id, created_at, ROW_NUMBER() OVER (PARTITION BY group_id ORDER BY created_at DESC) AS rn
            FROM raw_lms.group_course gc
            WHERE gc.is_active = 1
        ) gc
        LEFT JOIN raw_lms.course c ON c.id = gc.course_id
        WHERE gc.rn = 1
    ) gc ON gc.group_id = gs.group_id
    LEFT JOIN sol sol1 ON sol1.student_id = s1.student_id AND sol1.course_id = s1.course_id
    LEFT JOIN sol sol2 ON sol2.student_id = s2.codehour_student_id AND sol2.course_id = gc.course_id
)
SELECT *,
    CASE
        WHEN codehour_solution_rate_scores > 0 THEN codehour_student_id
        ELSE NULL
    END AS codehour_active_student_id,
    CASE
        WHEN codehour_solution_rate_scores >= 0.95 THEN '95-100'
        WHEN codehour_solution_rate_scores >= 0.90 THEN '90-95'
        WHEN codehour_solution_rate_scores >= 0.85 THEN '85-90'
        WHEN codehour_solution_rate_scores >= 0.80 THEN '80-85'
        WHEN codehour_solution_rate_scores >= 0.75 THEN '75-80'
        WHEN codehour_solution_rate_scores >= 0.70 THEN '70-75'
        WHEN codehour_solution_rate_scores >= 0.65 THEN '65-70'
        WHEN codehour_solution_rate_scores >= 0.60 THEN '60-65'
        WHEN codehour_solution_rate_scores >= 0.55 THEN '55-60'
        WHEN codehour_solution_rate_scores >= 0.50 THEN '50-55'
        WHEN codehour_solution_rate_scores >= 0.40 THEN '40-50'
        WHEN codehour_solution_rate_scores >= 0.30 THEN '30-40'
        WHEN codehour_solution_rate_scores >= 0.20 THEN '20-30'
        WHEN codehour_solution_rate_scores >= 0.10 THEN '10-20'
        WHEN codehour_solution_rate_scores > 0 THEN '00-10'
        ELSE NULL
    END AS codehour_solution_rate_category
FROM raw
WHERE rn = 1;

