// Isolate node
MATCH (a)-[r{{':'+selected_type}}]-()
WHERE a.{property} = "{expected_a_value}"
DELETE r