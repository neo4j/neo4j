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
    "F" -> Map("name" -> "Filipa")
  )

  val title = "Pattern"
  val section = "Introduction"
  val text =
    """
Patterns
========

Patterns are at the very core of Cypher, and are used in a lot of different places.
Using patterns, you describe the shape of the data that you are looking for.
Patterns are used in the `MATCH` clause. Path patterns are expressions.
Since these expressions are collections, they can also be used as predicates (a non-empty collection signifies true).
They are also used to `CREATE`/`CREATE UNIQUE` the graph.

So, understanding patterns is important, to be able to be effective with Cypher.

You describe the pattern, and Cypher will figure out how to get that data for you. The idea is for you to draw your
query on a whiteboard, naming the interesting parts of the pattern, so you can then use values from these parts
to create the result set you are looking for.

Patterns have bound points, or starting points. They are the parts of the pattern that are already ``bound'' to a set of
graph nodes or relationships. All parts of the pattern must be directly or indirectly connected to a starting point -- a pattern
where parts of the pattern are not reachable from any starting point will be rejected.

[options="header", cols=">s,^,^,^,^,^,^", width="100%"]
|===================
|Clause|Optional|Multiple rel. types|Varlength|Paths|Maps|Label OR syntax
|Match|Yes|Yes|Yes|Yes|-|Yes
|Create|-|-|-|Yes|Yes|-
|Create Unique|-|-|-|Yes|Yes|-
|Expressions|-|Yes|Yes|-|-|Yes
|===================

== Patterns for related nodes ==

The description of the pattern is made up of one or more paths, separated by commas. A path is a sequence of nodes and
relationships that always start and end in nodes. An example path would be:

+`(a)-->(b)`+

This is a path starting from the pattern node `a`, with an outgoing relationship from it to pattern node `b`.

Paths can be of arbitrary length, and the same node may appear in multiple places in the path.

Node identifiers can be used with or without surrounding parenthesis. The following match is semantically identical to
the one we saw above -- the difference is purely aesthetic.

+`a-->b`+

If you don't care about a node, you don't need to name it. Empty parenthesis are used for these nodes, like so:

+`a-->()<--b`+

== Labels ==

You can declare that nodes should have a certain label in your pattern.

+`a:User-->b`+

Or that it should have multiple labels:

+`a:User:Admin-->b`+

== Working with relationships ==

If you need to work with the relationship between two nodes, you can name it.

+`a-[r]->b`+

If you don't care about the direction of the relationship, you can omit the arrow at either end of the relationship, like this:

+`a--b`+

Relationships have types. When you are only interested in a specific relationship type, you can specify this like so:

+`a-[:REL_TYPE]->b`+

If multiple relationship types are acceptable, you can list them, separating them with the pipe symbol `|` like this:

+`a-[r:TYPE1|TYPE2]->b`+

This pattern matches a relationship of type +TYPE1+ or +TYPE2+, going from `a` to `b`. The relationship is named `r`.
Multiple relationship types can not be used with `CREATE` or `CREATE UNIQUE`.

== Optional relationships ==

An optional relationship is matched when it is found, but replaced by a `null` otherwise.
Normally, if no matching relationship is found, that sub-graph is not matched.
Optional relationships could be called the Cypher equivalent of the outer join in SQL.

They can only be used in `MATCH`.

Optional relationships are marked with a question mark.
They allow you to write queries like this one:

###no-results
START me=node(*)
MATCH me-->friend-[?]->friend_of_friend
RETURN friend, friend_of_friend###

The query above says ``for every person, give me all their friends, and their friends friends, if they have any.''

Optionality is transitive -- if a part of the pattern can only be reached from a bound point through an optional relationship,
that part is also optional. In the pattern above, the only bound point in the pattern is `me`. Since the relationship
between `friend` and `children` is optional, `children` is an optional part of the graph.

Also, named paths that contain optional parts are also optional -- if any part of the path is
`null`, the whole path is `null`.

In the following examples, `b` and `p` are all optional and can contain `null`:

###no-results
START a=node(%A%)
MATCH p = a-[?]->b
RETURN b###

###no-results
START a=node(%A%)
MATCH p = a-[?*]->b
RETURN b###

###no-results
START a=node(%A%)
MATCH p = a-[?]->x-->b
RETURN b###

###no-results
START a=node(%A%), x=node(%F%)
MATCH p = shortestPath( a-[?*]->x )
RETURN p###

== Controlling depth ==

A pattern relationship can span multiple graph relationships. These are called variable length relationships, and are
marked as such using an asterisk (`*`):

+`(a)-[*]->(b)`+

This signifies a path starting on the pattern node `a`, following only outgoing relationships, until it reaches pattern
node `b`. Any number of relationships can be followed searching for a path to `b`, so this can be a very expensive query,
depending on what your graph looks like.

You can set a minimum set of steps that can be taken, and/or the maximum number of steps:

+`(a)-[*3..5]->(b)`+

This is a variable length relationship containing at least three graph relationships, and at most five.

Variable length relationships can not be used with `CREATE` and `CREATE UNIQUE`.

As a simple example, let's take the query below:

###
START me=node(%F%)
MATCH me-[:KNOWS*1..2]-remote_friend
RETURN remote_friend###

This query starts from one node, and follows `KNOWS` relationships two or three steps out, and then stops.

== Assigning to path identifiers ==

In a graph database, a path is a very important concept. A path is a collection of nodes and relationships,
that describe a path in the graph. To assign a path to a path identifier, you simply assign a path pattern to an
identifier, like so:

+`p = (a)-[*3..5]->(b)`+

You can do this in `MATCH`, `CREATE` and `CREATE UNIQUE`, but not when using patterns as expressions. Example of the
three in a single query:

###no-results
START me=node(%F%)
MATCH p1 = me-[*2]-friendOfFriend
CREATE p2 = me-[:MARRIED_TO]->(wife {name:"Gunhild"})
CREATE UNIQUE p3 = wife-[:KNOWS]-friendOfFriend
RETURN p1,p2,p3###

== Setting properties ==

Nodes and relationships are important, but Neo4j uses properties on both of these to allow for far denser graphs models.

Properties are expressed in patterns using the map-construct, which is simply curly brackets surrounding a number of
key-expression pairs, separated by commas, e.g. `{ name: "Andres", sport: "BJJ" }`. If the map is supplied through a
parameter, the normal parameter expression is used: `{ paramName }`.

Maps are only used by `CREATE` and `CREATE UNIQUE`. In `CREATE` they are used to set the properties on the newly created
nodes and relationships.

When used with `CREATE UNIQUE`, they are used to try to match a pattern element with the corresponding graph element.
The match is successful if the properties on the pattern element can be matched exactly against properties on the graph
elements. The graph element can have additional properties, and they do not affect the match. If Neo4j fails to find
matching graph elements, the maps is used to set the properties on the newly created elements.
    """
}


