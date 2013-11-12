// Shortest path
MATCH p = shortestPath( (a)-[{{':'+selected_type}}*..4]->(b) )
WHERE a.name={a_name} AND b.name={b_name}
RETURN p