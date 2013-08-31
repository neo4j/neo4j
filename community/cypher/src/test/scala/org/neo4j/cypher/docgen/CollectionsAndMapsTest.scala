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


class CollectionsAndMapsTest extends ArticleTest {
  def assert(name: String, result: ExecutionResult) {}

  val graphDescription = List()

  val title = "Collections"
  val section = "Syntax"
  val text =
    """
Collections
===========

Cypher has good support for collections.

A literal collection is created by using brackets and separating the elements in the collection with comas.

###
RETURN [0,1,2,3,4,5,6,7,8,9] as collection###

In our examples, we'll use the range function, that gives you a collection containing all numbers between given start
and end numbers.

To access individual elements in the collection, we use the square brackets again.

###
RETURN range(0,10)[3]###

You can also use negative numbers, to start from the end of the collection instead.

###
RETURN range(0,10)[-3]###

Finally, you can use ranges inside the brackets to return ranges of the collection.

###
RETURN range(0,10)[0..3]###
###
RETURN range(0,10)[0..-5]###
###
RETURN range(0,10)[-5..]###
###
RETURN range(0,10)[..4]###

== List comprehension ==

List comprehension is a syntactic construct available in Cypher for creating a collection based on existing collections.
It follows the form of the mathematical set-builder notation (set comprehension) instead of the use of map
and filter functions.

###
RETURN [x in range(0,10) WHERE x % 2 = 0 | x^3 ]###

Either the WHERE part, or the expression, can be omitted, if you only want to filter or map respectively.

###
RETURN [x in range(0,10) WHERE x % 2 = 0 ]###
###
RETURN [x in range(0,10) | x^3 ]###


== Literal maps ==

From Cypher, you can also construct maps. In Java, they will be java.util.Map<String,Object>, through REST
you will get JSON objects.

###
RETURN { key : "Value", collectionKey: [ { inner: "Map1" }, { inner: "Map2" } ] }###
"""
}

