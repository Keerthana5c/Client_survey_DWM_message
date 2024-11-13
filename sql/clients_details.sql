with sd as 
(
 select 
   s.id as case_id,
   s.client_fk as client_fk,
   toDate(s.created_at) as case_receiving_date,
   --(toTimeZone(toDateTime(s.created_at), 'UTC'), '%Y-%m-%d %H:%i:%S.%f') as case_receiving_date,
   REPLACE((JSONExtractArrayRaw(simpleJSONExtractRaw(assumeNotNull(REPLACE(rules, '\\\\' , '')),'list'))[1]),'"','') as Modality,
   (replaceAll(
           replaceAll(
               replaceAll(
                   arrayStringConcat(
                       arrayMap(
                           x -> arrayStringConcat(
                               splitByChar(',', JSONExtractString(x, 'list')),
                               ' '
                           ),
                           JSONExtractArrayRaw(s.rules)
                       ),
                       ','
                   ),
                   '"',
                   ''
               ),
               '[',
               ''
           ),
           ']',
           ''
       )) AS Study_name,
   sd.is_demo as demo_case
from `transform`.Studies s 
left join `transform`.StudyDetails sd on s.id=sd.study_fk
where s.status = 'COMPLETED'
and toDate(s.created_at)>=toDate('2024-04-01')
and sd.is_demo <> True
), ct as 
(
 SELECT 
   distinct c.id as client_id,
   c.client_name client_name,
   toDate(c.onboarded_at) as client_onboarded_at,
   cd.client_source as client_source,
   cs.enable_demo as demo,
   sd.case_id as case_id,
   sd.case_receiving_date as case_received_date,
   sd.Modality as Modality,
   sd.Study_name AS Study_name,
   sd.demo_case as demo_case,
   ctm.modality,
   ctm.tat_min as tat_min
FROM 
   `transform`.Clients c  
left join `transform`.ClientDetails cd on c.id = cd.client_fk
left join `transform`.ClientSettings cs on  c.id=cs.client_fk
left join  sd on c.id=sd.client_fk
left join  metrics.client_tat_metrics ctm on sd.case_id= ctm.study_id
where 
--toDate(c.onboarded_at) >= '2024-04-01' and
c.saas_only<>True and
cs.billing_type <> 'demo'
and client_id not in (3680, 3681, 3682, 3683, 1040, 1433, 1474, 2350, 3741, 118, 519, 579, 1912, 1139, 743, 1449, 2988, 2991, 2909, 2071, 1876, 730, 519, 1738)
AND sd.case_receiving_date <> '1970-01-01'
AND (c.oms_partner=0 or (c.oms_partner = 1 and sd.case_id<>0))
and cs.enable_demo <> True
order by client_id
)SELECT
  ct.client_id as client_fk,
  ct.client_name,
  ct.client_onboarded_at AS onboarded_date,
  ct.case_received_date AS case_receiving_date,
  ct.client_source,
  count(ct.case_id) as total_case,
  Count(CASE WHEN ct.Modality = 'XRAY' AND ct.tat_min <=60 THEN ct.tat_min  
          WHEN ct.Modality = 'CT' AND ct.tat_min <=120 THEN ct.tat_min 
          WHEN ct.Modality = 'MRI' AND ct.tat_min <=180 THEN ct.tat_min END)
    AS Within_TAT,
  Count(CASE WHEN ct.Modality = 'XRAY' AND ct.tat_min >60 THEN ct.tat_min  
          WHEN ct.Modality = 'CT' AND ct.tat_min >120 THEN ct.tat_min 
          WHEN ct.Modality = 'MRI' AND ct.tat_min >180 THEN ct.tat_min END)
    AS tat_breach
FROM ct
GROUP BY ct.client_id, ct.client_name, ct.client_onboarded_at, ct.case_received_date, ct.client_source;