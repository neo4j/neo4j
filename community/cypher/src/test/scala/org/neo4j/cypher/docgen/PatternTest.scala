/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import org.junit.Assert._
import org.junit.Test

class PatternTest extends DocumentingTestBase {
  override def indexProps: List[String] = List("name")

  def graphDescription = List("A KNOWS B", "A BLOCKS C", "D KNOWS A", "B KNOWS E", "C KNOWS E", "B BLOCKS D")

  override val properties = Map(
    "A" -> Map("name" -> "Anders"),
    "B" -> Map("name" -> "Bossman"),
    "C" -> Map("name" -> "Cesar"),
    "D" -> Map("name" -> "David"),
    "E" -> Map("name" -> "Emil")
  )

  def section: String = "PATTERN"

  def optionalExample = """START me=node(1)
MATCH me-->friend-[?:parent_of]->children
RETURN friend, children"""

  def optionalQ1 = """START a=node(1)
MATCH p = a-[?]->b
RETURN b"""

  def optionalQ2 = """START a=node(1)
MATCH p = a-[?*]->b
RETURN b"""

  def optionalQ3 = """START a=node(1)
MATCH p = a-[?]->x-->b
RETURN b"""

  def optionalQ4 = """START a=node(1), x=node(2)
MATCH p = shortestPath( a-[?*]->x )
RETURN p"""


  @Test def intro() {
    testQuery(
      title = "Patterns",
      text = """
=== Introduction ===

Patterns are at the very core of Cypher, and are used in a lot of different places.
The pattern is used to describe the shape of the data that we are looking for.
Patterns are used in the `MATCH` clause. Path patterns are expressions.
Since these expressions are collections, they can also be used as
predicates (a non-empty collection signifies true). They are also used to `CREATE` the graph, and by the `RELATE`
clause.

So, understanding patterns is important, to be able to be effective with Cypher.

You describe the pattern, and Cypher will figure out how to get that data for you. The idea is for you to draw your
query on a whiteboard, naming the interesting parts of the pattern, so you can then use values from these parts
to create the result set you are looking for.

Patterns have bound points, or starting points. They are the parts of the pattern that are already ``bound'' to a set of
graph nodes or relationships. All parts of the pattern must be directly or indirectly connected to a starting point -- a pattern
where parts of the pattern are not reachable from any starting point will be rejected.

=== Patterns for related nodes ===

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

=== Working with relationships ===

If you need to work with the relationship between two nodes, you can name it.

+`a-[r]->b`+

If you don't care about the direction of the relationship, you can omit the arrow at either end of the relationship, like this:

+`a--b`+

Relationships have types. When you are only interested in a specific relationship type, you can specify this like so:

+`a-[:REL_TYPE]->b`+

If multiple relationship types are acceptable, you can list them, separating them with the pipe symbol `|` like this:

+`a-[r:TYPE1|TYPE2]->b`+

This pattern matches a relationship of type +TYPE1+ or +TYPE2+, going from `a` to `b`. The relationship is named `r`.
Multiple relationship types can not be used with `CREATE` or `RELATE`.

=== Optional relationships === 

An optional relationship is matched when it is found, but replaced by a `null` otherwise.
Normally, if no matching relationship is found, that sub-graph is not matched.
Optional relationships could be called the Cypher equivalent of the outer join in SQL.

Optional relationships are marked with a question mark.      
They allow you to write queries like this one:

[source,cypher]
----
"""
        + optionalExample +
"""
----

The query above says ``give me all my friends, and their children, if they have any.''

Optionality is transitive -- if a part of the pattern can only be reached from a bound point through an optional relationship,
that part is also optional. In the pattern above, the only bound point in the pattern is `me`. Since the relationship
between `friend` and `children` is optional, `children` is an optional part of the graph.

Also, named paths that contain optional parts are also optional -- if any part of the path is
`null`, the whole path is `null`.

In the following examples, `b` and `p` are all optional and can contain `null`:

[source,cypher]
----
"""
        + optionalQ1 +

"""
----

[source,cypher]
----
"""
        + optionalQ2 +

"""
----

[source,cypher]
----
"""
        + optionalQ3 +

"""
----

[source,cypher]
----
"""
        + optionalQ4 +

"""
----

=== Controlling depth ===

A pattern relationship can span multiple graph relationships. These are called variable length relationships, and are
marked as such using an asterisk (`*`):

+`(a)-[*]->(b)`+

This signifies a path starting on the pattern node `a`, following only outgoing relationships, until it reaches pattern
node `b`. Any number of relationships can be followed searching for a path to `b`, so this can be a very expensive query,
depending on what your graph looks like.

You can set a minimum set of steps that can be taken, and/or the maximum number of steps:

+`(a)-[*3..5]->(b)`+

This is a variable length relationship containing at least three graph relationships, and at most five.

Variable length relationships can not be used with `CREATE` and `RELATE`.

As a simple example, let's take the query below, executed on this graph:

include::cypher-pattern-graph.txt[]

""",
      queryText = """START me=node(1)
MATCH me-[:KNOWS*2]-friendOfFriend
RETURN friendOfFriend""",
      returns = "This query returns the friends of my friends, and stops at that depth.",
      assertions = p => assertTrue(true)
    )
  }

  @Test def runQueries() {
    testWithoutDocs(optionalQ1)
    testWithoutDocs(optionalQ2)
    testWithoutDocs(optionalQ3)
    testWithoutDocs(optionalQ4)
  }
}
