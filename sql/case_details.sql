--case_Details for DAU message by modality wise count with rework cases and last 30 days
WITH rd AS (
  SELECT 
    toDate(r.created_at) AS rework_date,
    r.id AS rework_id,
    r.study_fk AS study_id
  FROM `transform`.Reworks r
  WHERE r.status = 'COMPLETED' 
  AND r.by_user_type IN ('CLIENT', 'ACCOUNT_MANAGER')
  AND r.complete_reason_fk IS NULL
), 
overall_case_details AS (
  SELECT 
    s.client_fk AS client_id,
    c.client_name AS client_name,
    s.id AS study_id,
    rd.study_id AS rework_study_id,
    rd.rework_id AS rework_id,
    p.overall_tat AS tat_min,
    replace((JSONExtractArrayRaw(simpleJSONExtractRaw(assumeNotNull(REPLACE(s.rules, '\\' , '')),'list'))[1]), '"', '') AS Modality
  FROM `transform`.Studies s
  LEFT JOIN rd ON s.id = rd.study_id
  LEFT JOIN `metrics`.phase_level_tat p ON s.id = p.study_id
  LEFT JOIN `transform`.Clients c ON c.id = s.client_fk
  WHERE s.status = 'COMPLETED'
), 
total AS (
  SELECT 
    ocd.client_id AS client_id,
    ocd.client_name AS client_name,
    COUNT(CASE WHEN ocd.Modality = 'XRAY' THEN ocd.study_id END) AS XRAY_Count,
    COUNT(CASE WHEN ocd.Modality = 'XRAY' AND ocd.tat_min <= 60 THEN ocd.tat_min END) AS Within_TAT_XRAY,
    COUNT(CASE WHEN ocd.Modality = 'CT' THEN ocd.study_id END) AS CT_Count,
    COUNT(CASE WHEN ocd.Modality = 'CT' AND ocd.tat_min <= 120 THEN ocd.tat_min END) AS Within_TAT_CT,
    COUNT(CASE WHEN ocd.Modality = 'MRI' THEN ocd.study_id END) AS MRI_Count,
    COUNT(CASE WHEN ocd.Modality = 'MRI' AND ocd.tat_min <= 180 THEN ocd.tat_min END) AS Within_TAT_MRI,
    COUNT(CASE WHEN ocd.Modality = 'NM' THEN ocd.study_id END) AS NM_Count,
    COUNT(CASE WHEN ocd.Modality = 'NM' AND ocd.tat_min <= 180 THEN ocd.tat_min END) AS Within_TAT_NM,
    COUNT(CASE WHEN ocd.Modality = 'XRAY' AND ocd.rework_id <>0 THEN ocd.rework_id END) AS XRAY_Rework,
    COUNT(CASE WHEN ocd.Modality = 'CT' AND ocd.rework_id <>0 THEN ocd.rework_id END) AS CT_Rework,
    COUNT(CASE WHEN ocd.Modality = 'MRI' AND ocd.rework_id <>0 THEN ocd.rework_id END) AS MRI_Rework,
    COUNT(CASE WHEN ocd.Modality = 'NM' AND ocd.rework_id <>0 THEN ocd.rework_id END) AS NM_Rework
  FROM overall_case_details ocd
  GROUP BY ocd.client_id, ocd.client_name
),
last_30_days AS (
  SELECT 
    s.client_fk AS client_id,
    c.client_name AS client_name,
    s.id AS study_id,
    rd.study_id AS rework_study_id,
    rd.rework_id AS rework_id,
    p.overall_tat AS tat_min,
    replace((JSONExtractArrayRaw(simpleJSONExtractRaw(assumeNotNull(REPLACE(s.rules, '\\' , '')),'list'))[1]), '"', '') AS Modality,
    DATE(s.created_at)
  FROM `transform`.Studies s
  LEFT JOIN rd ON s.id = rd.study_id
  LEFT JOIN `metrics`.phase_level_tat p ON s.id = p.study_id
  LEFT JOIN `transform`.Clients c ON c.id = s.client_fk
  WHERE toDate(s.created_at) >=(today() - INTERVAL 30 DAY)
),
last_30_day_counts AS (
  SELECT 
    l30.client_id AS client_id,
    l30.client_name AS client_name,
    COUNT(CASE WHEN l30.Modality = 'XRAY' THEN l30.study_id END) AS Last_30_XRAY_Count,
    COUNT(CASE WHEN l30.Modality = 'XRAY' AND l30.tat_min <= 60 THEN l30.tat_min END) AS Last_30_Within_TAT_XRAY,
    COUNT(CASE WHEN l30.Modality = 'CT' THEN l30.study_id END) AS Last_30_CT_Count,
    COUNT(CASE WHEN l30.Modality = 'CT' AND l30.tat_min <= 120 THEN l30.tat_min END) AS Last_30_Within_TAT_CT,
    COUNT(CASE WHEN l30.Modality = 'MRI' THEN l30.study_id END) AS Last_30_MRI_Count,
    COUNT(CASE WHEN l30.Modality = 'MRI' AND l30.tat_min <= 180 THEN l30.tat_min END) AS Last_30_Within_TAT_MRI,
    COUNT(CASE WHEN l30.Modality = 'NM' THEN l30.study_id END) AS Last_30_NM_Count,
    COUNT(CASE WHEN l30.Modality = 'NM' AND l30.tat_min <= 180 THEN l30.tat_min END) AS Last_30_Within_TAT_NM,
    COUNT(CASE WHEN l30.Modality = 'XRAY' AND l30.rework_id <>0 THEN l30.rework_id END) AS Last_30_XRAY_Rework,
    COUNT(CASE WHEN l30.Modality = 'CT' AND l30.rework_id <>0 THEN l30.rework_id END) AS Last_30_CT_Rework,
    COUNT(CASE WHEN l30.Modality = 'MRI' AND l30.rework_id <>0 THEN l30.rework_id END) AS Last_30_MRI_Rework,
    COUNT(CASE WHEN l30.Modality = 'NM' AND l30.rework_id <>0 THEN l30.rework_id END) AS Last_30_NM_Rework
  FROM last_30_days l30
  GROUP BY l30.client_id, l30.client_name
)
SELECT 
  t.client_id as client_fk,
  t.client_name,
  t.XRAY_Count,
  t.Within_TAT_XRAY,
  t.XRAY_Rework,
  t.CT_Count,
  t.Within_TAT_CT,
  t.CT_Rework,
  t.MRI_Count,
  t.Within_TAT_MRI,
  t.MRI_Rework,
  t.NM_Count,
  t.Within_TAT_NM,
  t.NM_Rework,
  l30.Last_30_XRAY_Count,
  l30.Last_30_Within_TAT_XRAY,
  l30.Last_30_XRAY_Rework,
  l30.Last_30_CT_Count,
  l30.Last_30_Within_TAT_CT,
  l30.Last_30_CT_Rework,
  l30.Last_30_MRI_Count,
  l30.Last_30_Within_TAT_MRI,
  l30.Last_30_MRI_Rework,
  l30.Last_30_NM_Count,
  l30.Last_30_Within_TAT_NM,
  l30.Last_30_NM_Rework
FROM total t
LEFT JOIN last_30_day_counts l30 ON t.client_id = l30.client_id
WHERE t.client_id NOT IN (1040, 1433, 1474, 2350, 3741, 118, 519, 579, 1912, 1139, 743, 1449, 2988, 2991, 2909, 2071, 1876, 730);





-- --case_Details for DAU message by modality wise count
-- with overall_case_details as(
-- select 
--    s.client_fk as client_id,
--    c.client_name as client_name,
--    s.id as study_id,
--    p.overall_tat as tat_min,
--    replace((JSONExtractArrayRaw(simpleJSONExtractRaw(assumeNotNull(REPLACE(s.rules, '\\' , '')),'list'))[1]),'"','') as Modality
-- from `transform`.Studies s
-- left join `metrics`.phase_level_tat p on s.id = p.study_id
-- Left join `transform`.Clients c on c.id = s.client_fk
-- WHERE s.status = 'COMPLETED'
-- ),
-- total as (
-- select 
--    ocd.client_id as client_id,
--    ocd.client_name as client_name,
--    count(CASE WHEN ocd.Modality = 'XRAY' THEN ocd.study_id END) AS XRAY_Count,
--    count(CASE WHEN ocd.Modality = 'XRAY' AND ocd.tat_min <=60 THEN ocd.tat_min END ) AS Within_TAT_XRAY,
--    count(CASE WHEN ocd.Modality = 'CT' THEN ocd.study_id END) AS CT_Count,
--    count(CASE WHEN ocd.Modality = 'CT' AND ocd.tat_min <=120 THEN ocd.tat_min END ) AS Within_TAT_CT,
--    count(CASE WHEN ocd.Modality = 'MRI' THEN ocd.study_id END) AS MRI_Count,
--    count(CASE WHEN ocd.Modality = 'MRI' AND ocd.tat_min <=180 THEN ocd.tat_min END ) AS Within_TAT_MRI,
--    count(CASE WHEN ocd.Modality = 'NM' THEN ocd.study_id END) AS NM_Count,
--    count(CASE WHEN ocd.Modality = 'NM' AND ocd.tat_min <=180 THEN ocd.tat_min END ) AS Within_TAT_NM
-- from overall_case_details ocd
-- group by ocd.client_id,ocd.client_name
-- ),
-- current_month as(
-- select 
--    s.client_fk as client_id,
--    c.client_name as client_name,
--    s.id as study_id,
--    p.overall_tat as tat_min,
--    replace((JSONExtractArrayRaw(simpleJSONExtractRaw(assumeNotNull(REPLACE(s.rules, '\\' , '')),'list'))[1]),'"','') as Modality,
--    date(s.created_at)
-- from `transform`.Studies s
-- left join `metrics`.phase_level_tat p on s.id = p.study_id
-- Left join `transform`.Clients c on c.id = s.client_fk
-- where toDate(s.created_at) BETWEEN DATE_FORMAT(CURDATE(), '%Y-%m-01') AND LAST_DAY(CURDATE())             
-- ),
-- months as (
-- select 
--    cm.client_id as client_id,
--    cm.client_name as client_name,
--    count(CASE WHEN cm.Modality = 'XRAY' THEN cm.study_id END) AS Current_XRAY_Count,
--    count(CASE WHEN cm.Modality = 'XRAY' AND cm.tat_min <=60 THEN cm.tat_min END ) AS Current_Within_TAT_XRAY,
--    count(CASE WHEN cm.Modality = 'CT' THEN cm.study_id END) AS Current_CT_Count,
--    count(CASE WHEN cm.Modality = 'CT' AND cm.tat_min <=120 THEN cm.tat_min END ) AS Current_Within_TAT_CT,
--    count(CASE WHEN cm.Modality = 'MRI' THEN cm.study_id END) AS Current_MRI_Count,
--    count(CASE WHEN cm.Modality = 'MRI' AND cm.tat_min <=180 THEN cm.tat_min END ) AS Current_Within_TAT_MRI,
--    count(CASE WHEN cm.Modality = 'NM' THEN cm.study_id END) AS Current_NM_Count,
--    count(CASE WHEN cm.Modality = 'NM' AND cm.tat_min <=180 THEN cm.tat_min END ) AS Current_Within_TAT_NM
-- from current_month cm
-- group by cm.client_id,cm.client_name
-- )
-- select 
--    t.client_id,
--    t.client_name,
--    t.XRAY_Count,
--    t.Within_TAT_XRAY,
--    t.CT_Count,
--    t.Within_TAT_CT,
--    t.MRI_Count,
--    t.Within_TAT_MRI,
--    t.NM_Count,
--    t.Within_TAT_NM,
--    m.Current_XRAY_Count,
--    m.Current_Within_TAT_XRAY,
--    m.Current_CT_Count,
--    m.Current_Within_TAT_CT,
--    m.Current_MRI_Count,
--    m.Current_Within_TAT_MRI,
--    m.Current_NM_Count,
--    m.Current_Within_TAT_NM
-- from total t 
-- left join months m on t.client_id = m.client_id
-- WHERE t.client_id NOT IN (1040, 1433, 1474, 2350, 3741, 118, 519, 579, 1912, 1139, 743, 1449, 2988, 2991, 2909, 2071, 1876, 730);