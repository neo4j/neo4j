// Relate nodes
MATCH (a),(b)
WHERE a.{property} = "{expected_a_value}" 
AND b.{property} = "{expected_b_value}"
CREATE (a)-[r{{':'+selected_type}}]->(b)
RETURN a,r,b