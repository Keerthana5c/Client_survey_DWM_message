SELECT distinct
   c.id AS client_fk,
   cc.id as contact_fk,
   c.unique_id AS unique_id,
   c.client_name AS client_name,
   case when cc.phone is null or cc.phone= 'NA' then NULL else cc.phone end AS phone_number,
    cc.email AS email,
   replaceAll(replaceAll(replaceAll(cc.designation, '[', ''), ']', ''), '"', '') AS designation,
   cc.persona_type 
FROM `transform`.Clients c 
LEFT JOIN `transform`.ClientContacts cc ON c.id = cc.client_fk 
WHERE 
c.id not in (1040, 1433, 1474, 2350, 3741, 118, 519, 579, 1912, 1139, 743, 1449, 2988, 2991, 2909, 2071, 1876, 730, 519)
union all 
SELECT distinct
   c.id AS client_fk,
   0 as contact_fk,
   c.unique_id AS unique_id,
   c.client_name AS client_name,
   null as phone_number,
    c.email AS email,
    null as designation ,
    null as persona_type 
FROM `transform`.Clients c 
WHERE 
c.id not in (1040, 1433, 1474, 2350, 3741, 118, 519, 579, 1912, 1139, 743, 1449, 2988, 2991, 2909, 2071, 1876, 730, 519);




-- SELECT distinct
--    c.id AS client_id,
--    cc.id AS contact_fk,
--    c.unique_id AS unique_id,
--    c.client_name AS client_name,
--    cc.phone AS phone_number,
--    c.email AS email,
--    replaceAll(replaceAll(replaceAll(cc.designation, '[', ''), ']', ''), '"', '') AS designation,
--    cc.persona_type 
-- FROM `transform`.Clients c 
-- LEFT JOIN `transform`.ClientContacts cc ON c.id = cc.client_fk 
-- WHERE 
-- c.id not in (1040, 1433, 1474, 2350, 3741, 118, 519, 579, 1912, 1139, 743, 1449, 2988, 2991, 2909, 2071, 1876, 730, 519)
-- --and cc.phone= 'NA' ;