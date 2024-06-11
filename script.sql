CREATE OR REPLACE NETWORK RULE sample_network_rule
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = ('www.swg.usace.army.mil','www.swf.usace.army.mil');


CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION sample_access_integration
  ALLOWED_NETWORK_RULES = (sample_network_rule)
  ENABLED = TRUE;  