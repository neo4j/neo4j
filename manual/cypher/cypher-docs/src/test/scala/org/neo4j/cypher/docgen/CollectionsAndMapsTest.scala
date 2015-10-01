/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import org.neo4j.cypher.internal.compiler.v3_1.executionplan.InternalExecutionResult

class CollectionsAndMapsTest extends ArticleTest {
  def assert(name: String, result: InternalExecutionResult) {}

  override def graphDescription = List(
    "Charlie:Person ACTED_IN WallStreet:Movie",
    "Charlie ACTED_IN RedDawn:Movie",
    "Charlie ACTED_IN ApocalypseNow:Movie",
    "Martin:Person ACTED_IN ApocalypseNow:Movie",
    "Martin ACTED_IN WallStreet"
  )

  override val properties = Map(
    "Charlie" -> Map("name" -> "Charlie Sheen", "realName" -> "Carlos Irwin Estévez"),
    "Martin" -> Map("name" -> "Martin Sheen"),
    "WallStreet" -> Map("title" -> "Wall Street", "year" -> 1987),
    "RedDawn" -> Map("title" -> "Red Dawn", "year" -> 1984),
    "ApocalypseNow" -> Map("title" -> "Apocalypse Now", "year" -> 1979)
  )

  val title = "Lists"
  val section = "Syntax"
  val text =
    """
Lists
=====

Cypher has good support for lists.

== Lists in general ==

A literal list is created by using brackets and separating the elements in the list with commas.

###
RETURN [0,1,2,3,4,5,6,7,8,9] as list###

In our examples, we'll use the range function.
It gives you a list containing all numbers between given start and end numbers.
Range is inclusive in both ends.

To access individual elements in the list, we use the square brackets again.
This will extract from the start index and up to but not including the end index.

###
RETURN range(0,10)[3]###

You can also use negative numbers, to start from the end of the list instead.

###
RETURN range(0,10)[-3]###

Finally, you can use ranges inside the brackets to return ranges of the list.

###
RETURN range(0,10)[0..3]###
###
RETURN range(0,10)[0..-5]###
###
RETURN range(0,10)[-5..]###
###
RETURN range(0,10)[..4]###

NOTE: Out-of-bound slices are simply truncated, but out-of-bound single elements return +NULL+.

###
RETURN range(0,10)[15]###

###
RETURN range(0,10)[5..15]###

You can get the size of a list like this:

###
RETURN size(range(0,10)[0..3])###

== List comprehension ==

List comprehension is a syntactic construct available in Cypher for creating a list based on existing lists.
It follows the form of the mathematical set-builder notation (set comprehension) instead of the use of map
and filter functions.

###
RETURN [x IN range(0,10) WHERE x % 2 = 0 | x^3 ] AS result###

Either the +WHERE+ part, or the expression, can be omitted, if you only want to filter or map respectively.

###
RETURN [x IN range(0,10) WHERE x % 2 = 0 ] AS result###
###
RETURN [x IN range(0,10) | x^3 ] AS result###


== Literal maps ==

From Cypher, you can also construct maps. Through REST you will get JSON objects; in Java they will be `java.util.Map<String,Object>`.

###
RETURN { key : "Value", listKey: [ { inner: "Map1" }, { inner: "Map2" } ] }###

== Map Projection ==
Cypher supports a concept called "map projections".
It allows for easily constructing map projections from nodes, relationships and other map values.

A map projection begins with the variable bound to the graph entity to be projected from, and contains a body of coma separated map elements, enclosed by `{` and  `}`.

`map_variable { map_element, [, ...n] }`

A map element projects one or more key value pair to the map projection.
There exists four different types of map projection elements:

* Property selector - Projects the property name as the key, and the value from the `map_variable` as the value for the projection.

* Literal entry - This is a key value pair, with the value being arbitrary expression `key : <expression>`.

* Variable selector - Projects an variable, with the variable name as the key, and the value the variable is pointing to as the value of the projection.
It's syntax is just the variable.

* All-properties selector - projects all key value pair from the `map_variable` value.

Note that if the `map_variable` points to a null value, the whole map projection will evaluate to null.

=== Examples of map projections ===

Find Charlie Sheen and return data about him and the movies he has acted in.
This example shows an example of map projection with a literal entry, which in turn also uses map projection inside the aggregating `collect()`.

###
MATCH (actor:Person {name:'Charlie Sheen'})-[:ACTED_IN]->(movie:Movie)
RETURN actor{
        .name,
        .realName,
         movies: collect(movie{ .title, .year })
      }###


Find all persons that have acted in movies, and show number for each.
This example introduces an variable with the count, and uses an variable selector to project the value.

###
MATCH (actor:Person)-[:ACTED_IN]->(movie:Movie)
WITH actor, count(movie) as nrOfMovies
RETURN actor{
        .name,
         nrOfMovies
      }###

Again, focusing on Charlie Sheen, this time returning all properties from the node.
Here we use an all-properties selector to project all the node properties, and additionally, explicitly project the property `age`.
Since this property does not exist on the node, a null value is projected instead.

###
MATCH (actor:Person {name:'Charlie Sheen'})
RETURN actor{.*, .age}###
"""
}
