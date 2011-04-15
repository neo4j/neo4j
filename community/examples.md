Select single node by id
--
	FROM node = NODE(17)
	SELECT node

Returns a list of nodes with the length 1




Select multiple nodes by id
--
FROM node = NODE(17,34,665)
SELECT node
--
Returns a list of nodes




Select a property from a node
--
FROM node = NODE(1)
SELECT node.name
--
Returns a list of string




Select all nodes that are KNOWN by the start node
--
FROM start = NODE(1)
WHERE start -KNOWS> x
SELECT x
--
Returns a list of nodes




Select all KNOWS relationships from a start node
--
FROM start = NODE(1)
WHERE
	start -z> _ AND
	z.TYPE = KNOWS
SELECT z
--
The underscore is generally a placeholder for something we won't filter or order on. In this case, we don't care about who is known by the start node.
When a relationship is used as a variable, you can specify the relationship type with TYPE.





Select all KNOWS paths three steps out, from a start node
--
FROM start = NODE(1)
WHERE
	start (-KNOWS> _)*.v AND
	v.LENGTH = 3
SELECT v
--
Will return a list of paths. 




Select all relationships three steps out from a start node. Keeps following the same type as the first outgoing relationship all the way
--
FROM start = NODE(1)
WHERE 
	start ( -r> _)*.v AND
	v.LENGTH = 3 AND
	v.r[FIRST].TYPE = v.r[ALL].TYPE
SELECT v
--
