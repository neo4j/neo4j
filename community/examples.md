Introduction
==
Sunshine has a few syntaxes that should be explained before we jump into the examples.


FROM _from_expr_ [, _from_expr_ ...]
[WHERE _where_cond_ [, _where_cond_ ...]]
SELECT _select_expr_


Queries are broken down into different parts:

FROM
--
This is where the start points of the query come in. If you are going to introduce variables - this is where you do it.

WHERE
--
Here we filter down the resulting rows

GROUP BY
--
If any grouping is to be done, here we define how to group things. This step is not strictly necessary. One alternative is to have aggregate functions, and if they are used in the SELECT part, everything that is not an aggregate function would be the grouping key. Need to think more on this.

ORDER BY
--
Sorting.

SELECT
--
This final stage is what the user wants back. The user seldom wants the whole path, so in this part we can say what part of the path we want - the whole path, one or more nodes/relationships, or maybe a property from a node. Or the user might want a combination - the first node of a path, the length of a path, and a property from the end node. Here we could also limit - just give me the top 10 hits.


Traversal descriptions
--
Relationships are shown like this: --->, or <---, or even --- when you don't care about the direction of the relationship.
If you want a specific type, you write like this: -KNOWS->, <-FRIEND- , or -RELATED-
Note that these are all uppercase. If you write them lowercase, it's a variable name instead, that can be used for further filtering or outputting.

A traversal can be constructed using regexp-like syntax. Examples:

Follow all relationships from z, and then follow KNOWS relationships from x
	z --> x -KNOWS-> y

Follow all KNOWS relationships. v will contain paths	
	z ( -KNOWS-> x )*.v
	
	



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



Filter on a node property
--
	FROM node = NODE(1,2)
	WHERE node.name = "Andres"
	SELECT node

Returns a list of nodes with a property name that matches "Andres"


Returns the cartesian product
--
	FROM n1 = NODE(1), n2 = NODE(2)
	SELECT n1, n2
	
Returns both nodes in a single row. Cartesian products are the size n*m, and since both n and m are one, the is one.


Select all nodes that are KNOWN by the start node
--
	FROM NODE(1) -KNOWS-> x
	SELECT x
	
Returns a list of nodes




Select all KNOWS relationships from a start node
--
	FROM NODE(1) -z-> _ AND
		z.TYPE = KNOWS
	SELECT z

The underscore is generally a placeholder for something we won't filter or order on. In this case, we don't care about who is known by the start node.
When a relationship is used as a variable, you can specify the relationship type with TYPE.





Select all KNOWS paths three steps out, from a start node
--
	FROM NODE(1) (-KNOWS-> _)*.v
		AND v.LENGTH = 3
	SELECT v

Will return a list of paths. 




Select all relationships three steps out from a start node, following the same reltype. 
--
	FROM start = NODE(1),
		v = start -rel 3..10-> _ 
	WHERE v.rel[FIRST].TYPE = v.rel[ALL].TYPE
	SELECT v
	



Select nodes from index hits
--
	FROM node = NODE_IDX(name, "Kevin Bacon")
	SELECT node


Get my friends of friends, that are not me, or my friends.
--
	FROM start = NODE(1),
		start -FRIEND 1-> _	 -interesting 1-> x
	WHERE start != x AND NOT(x --> start) 
	SELECT x as FoF, COUNT(*) AS c
	ORDER BY c