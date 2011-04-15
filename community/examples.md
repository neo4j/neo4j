Introduction
==
Cypher has a few syntaxes that should be explained before we jump into the examples.

Relationships are shown like this: -->, or <--, or even --- when you don't care about the direction of the relationship.
If you want a specific type, you write like this: -KNOWS>, <FRIEND- , or -RELATED-
Note that these are all uppercase. If you write them lowercase, it's a variable name instead, that can be used for further filtering or outputting.

A traversal can be constructed using regexp-like syntax. Examples:

Follow all relationships from z, and then follow KNOWS relationships from x
	z --> x -KNOWS> y

Follow all KNOWS relationships. v will contain paths	
	z ( -KNOWS> x )*.v
	
	



Examples
==

Select single node by id
--
	FROM node = NODE(17)
	SELECT node

Returns a list of nodes with the length 1




Select multiple nodes by id
--
	FROM node = NODE(17,34,665)
	SELECT node

Returns a list of nodes




Select a property from a node
--
	FROM node = NODE(1)
	SELECT node.name

Returns a list of string




Select all nodes that are KNOWN by the start node
--
	FROM start = NODE(1)
	WHERE start -KNOWS> x
	SELECT x
	
Returns a list of nodes




Select all KNOWS relationships from a start node
--
	FROM start = NODE(1)
	WHERE
		start -z> _ AND
		z.TYPE = KNOWS
	SELECT z

The underscore is generally a placeholder for something we won't filter or order on. In this case, we don't care about who is known by the start node.
When a relationship is used as a variable, you can specify the relationship type with TYPE.





Select all KNOWS paths three steps out, from a start node
--
	FROM start = NODE(1)
	WHERE
		start (-KNOWS> _)*.v AND
		v.LENGTH = 3
	SELECT v

Will return a list of paths. 




Select all relationships three steps out from a start node, following the same reltype. 
--
	FROM start = NODE(1)
	WHERE 
		start ( -r> _)*.v AND
		v.LENGTH = 3 AND
		v.r[FIRST].TYPE = v.r[ALL].TYPE
	SELECT v
	



Select nodes from index hits
--
	FROM node = NODE_IDX(name, "Kevin Bacon")
	SELECT node

