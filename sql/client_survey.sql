select 
   created_at::date as Date,
   id as survey_id,
   client_fk as client_fk,
   contact_fk as contact_id,
   client_active_type as client_active_type,
   churn_type,
   response, 
   trigger
from public.client_survey cs 