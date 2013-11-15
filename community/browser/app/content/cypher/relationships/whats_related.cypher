// Whats related
MATCH (a)-[r{{':'+selected_type}}]-(b) 
RETURN DISTINCT head(labels(a)), type(r), head(labels(b))