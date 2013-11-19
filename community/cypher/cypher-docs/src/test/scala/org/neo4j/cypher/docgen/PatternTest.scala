/**
 * Copyright (c) 2002-2013 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.docgen

import org.neo4j.cypher.ExecutionResult

class PatternTest extends ArticleTest {
  override val indexProps: List[String] = List("name")

  def assert(name: String, result: ExecutionResult) {}

  val graphDescription = List("A KNOWS B", "A KNOWS C", "A KNOWS D", "B KNOWS E", "C KNOWS E", "D KNOWS F")

  override val properties = Map(
    "A" -> Map("name" -> "Anders"),
    "B" -> Map("name" -> "Becky"),
    "C" -> Map("name" -> "Cesar"),
    "D" -> Map("name" -> "Dilshad"),
    "E" -> Map("name" -> "Emil"),
    "F" -> Map("name" -> "Filipa"))

  val title = "Pattern"
  val section = "Introduction"
  val text =
    """
Patterns
========

Patterns and pattern-matching are at the very heart of Cypher.
For that reason, it's important to understand patterns to be effective with Cypher.

Using patterns, you describe the shape of the data you're looking for in the `MATCH` clause.
You describe the pattern, and Cypher will figure out how to get that data for you.
The idea is for you to draw your query on a whiteboard, naming the interesting parts of the pattern.
Then you can use values from these parts to create the result set you are looking for.

Patterns can have bound points, or starting points.
They are the parts of the pattern that are already ``bound'' to a set of graph nodes or relationships.
All parts of the pattern must be directly or indirectly connected to a starting point.
A pattern where parts of the pattern are not reachable from any starting point will be rejected.

[options="header", cols=">s,^,^,^,^"]
|===================
|Clause        |Multiple rel. types  |Varlength |Paths |Maps
|Match         |Yes                  |Yes       |Yes   |-
|Create        |-                    |-         |Yes   |Yes
|Create Unique |-                    |-         |Yes   |Yes
|Expressions   |Yes                  |Yes       |-     |-
|===================

== Patterns for related nodes ==

The description of the pattern is made up of one or more paths, separated by commas.
A path is a sequence of nodes and relationships that always start and end in nodes.
An example path would be:

+`(a)-->(b)`+

This is a path starting from the pattern node `a`, with an outgoing relationship from it to pattern node `b`.

Paths can be of arbitrary length, and the same node may appear in multiple places in the path.
Path patterns are expressions, and since these expressions are collections, they can also be used as predicates (where a non-empty collection signifies true).

Node identifiers that don't specify labels or properties may omit surrounding parentheses.
The following match is semantically identical to the one we saw above -- the difference is purely aesthetic.

+`a-->b`+

If you don't care about a node, you don't need to name it.
Empty parentheses are used for these nodes, like so:

+`a-->()<--b`+

== Labels ==

You can declare that nodes should have a certain label in your pattern.

+`(a:User)-->b`+

Or that it should have multiple labels:

+`(a:User:Admin)-->b`+

== Working with relationships ==

If you need to work with the relationship between two nodes, you can name it.

+`(a)-[r]->(b)`+

If you don't know the direction of the relationship, or you want to include both directions, you can omit the arrow at either end of the relationship, like this:

+`(a)--(b)`+

Relationships have types. When you are only interested in a specific relationship type, you can specify this like so:

+`(a)-[:REL_TYPE]->(b)`+

If multiple relationship types are acceptable, you can list them, separating them with the pipe symbol `|` like this:

+`(a)-[r:TYPE1|TYPE2]->(b)`+

This pattern matches a relationship of type +TYPE1+ or +TYPE2+, going from `a` to `b`.
The relationship is named `r`.
Multiple relationship types can not be used with `CREATE` or `CREATE UNIQUE`.

[NOTE]
The use of `?` for optional relationships has been removed in Cypher 2.0 in favor of the newly introduced `OPTIONAL MATCH` clause.
See <<query-optional-match>> for more information.

== Properties ==

Both nodes and relationships in patterns can have properties on them.
Cypher allows specifying properties in the pattern, by using the literal map syntax - `{ key: value }`.
The value can be any expression.

A node with properties is expressed like so:

+`(a {name:'Joe'})`+

A relationship with properties is written out like so:

+`(a)-[ {blocked:false} ]->(b)`+

A variable length relationship with properties defined on in it means that _all_ relationships in the path must have the property set to the given value.

+`(a)-[*2..5 {blocked:false} ]->(b)`+

In the query above, all relationships between `a` and `b` must have the property blocked set to the boolean value `false`.

When combining with other qualifiers, properties go last, for example:

+`(a)-[r:REL_TYPE*2..5 {blocked:false} ]->(b)`+

== Controlling depth ==

A pattern relationship can span multiple graph relationships.
These are called variable length relationships, and are marked as such using an asterisk (`*`):

+`(a)-[*]->(b)`+

This signifies a path starting on the pattern node `a`, following only outgoing relationships, until it reaches pattern node `b`.
Any number of relationships can be followed searching for a path to `b`, so this can be a very expensive query, depending on what your graph looks like.

You can set a minimum set of steps that can be taken, and/or the maximum number of steps:

+`(a)-[*3..5]->(b)`+

This is a variable length relationship containing at least three graph relationships, and at most five.

Variable length relationships can not be used with `CREATE` and `CREATE UNIQUE`.

As a simple example, let's take the query below:

###
MATCH (me { name: "Filipa" })-[:KNOWS*1..2]-(remote_friend)
RETURN remote_friend###

This query starts from one node, and follows `KNOWS` relationships one or two steps out, and then stops.

== Assigning to path identifiers ==

In a graph database, a path is a very important concept.
A path is a collection of nodes and relationships, that describe a path in the graph.
To assign a path to a path identifier, you simply assign a path pattern to an identifier, like so:

+`p = (a)-[*3..5]->(b)`+

You can do this in `MATCH`, `CREATE` and `CREATE UNIQUE`, but not when using patterns as expressions.
Example of the three in a single query:

###no-results
MATCH p1 =(me { name: "Cesar" })-[*2]-(friendOfFriend)
CREATE p2 =(me)-[:MARRIED_TO]->(wife { name:"Gunhild" })
CREATE UNIQUE p3 =(wife)-[:KNOWS]-(friendOfFriend)
RETURN p1, p2, p3###

== Setting properties ==

Nodes and relationships are important, but Neo4j uses properties on both of these to allow for far richer graph models.

Properties are expressed in patterns using a map-construct, simply curly brackets surrounding a number of key-expression pairs, separated by commas.
For example: `{ name: "Andres", sport: "Brazilian Ju-Jitsu" }`.
If the map is supplied through a parameter, the normal parameter expression is used: `{ paramName }`.

Patterns are also used to mutate the graph with `CREATE` and `CREATE UNIQUE`.
Maps are only used by `CREATE` and `CREATE UNIQUE`.
In `CREATE` they are used to set the properties on the newly created nodes and relationships.
When used with `CREATE UNIQUE`, they are used to try to match a pattern element with the corresponding graph element.
The match is successful if the properties on the pattern element can be matched exactly against properties on the graph elements.
The graph element can have additional properties, and they do not affect the match.
If Neo4j fails to find matching graph elements, the maps are used to set the properties on the newly created elements.

"""
}

