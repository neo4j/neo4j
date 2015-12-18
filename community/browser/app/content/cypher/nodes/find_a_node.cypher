// Find a node
// Replace:
//   'LabelName' - with the label (if any) of the node to find
//   'propertyKey' - with a property to look for
//   'expected_value' - with the property value to find
MATCH (n:LabelName) 
WHERE n.propertyKey = "expected_value" RETURN n
