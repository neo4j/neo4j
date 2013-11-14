// Delete a node
// Replace:
//   'propertyKey' - with the property to look for
//   'expected_value' - with the property value which will match the node to delete
START n=node(*) 
MATCH (n)-[r?]-() 
WHERE n.propertyKey = "expected_value"
DELETE n,r