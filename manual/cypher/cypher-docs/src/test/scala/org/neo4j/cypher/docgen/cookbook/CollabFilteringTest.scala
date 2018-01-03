/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.docgen.cookbook

import org.junit.Test
import org.junit.Assert._
import org.neo4j.cypher.docgen.DocumentingTestBase
import org.neo4j.visualization.graphviz.GraphStyle
import org.neo4j.visualization.graphviz.AsciiDocSimpleStyle

class CollabFilteringTest extends DocumentingTestBase {
  override def graphDescription = List("Joe knows Bill", "Joe knows Sara", "Sara knows Bill", "Sara knows Ian", "Bill knows Derrick",
    "Bill knows Ian", "Sara knows Jill")

  def section = "cookbook"
  override val noTitle = true;

  override protected def getGraphvizStyle: GraphStyle = {
    AsciiDocSimpleStyle.withAutomaticRelationshipTypeColors()
  }

  @Test def basicCollborativeFiltering() {
    testQuery(
      title = "Simple Friend Finder",
      text = """To find out the friends of Joe's friends that are not already his friends, the query looks like this:""",
      queryText =
        "match (joe {name: 'Joe'})-[:knows*2..2]-(friend_of_friend) " +
          "where not (joe)-[:knows]-(friend_of_friend) " +
          "return friend_of_friend.name, COUNT(*) " +
          "order by COUNT(*) DESC, friend_of_friend.name",
      optionalResultExplanation = "This returns a list of friends-of-friends ordered by the number of connections to them, and secondly by their name.",
      assertions = (p) => assertEquals(List(
        Map("friend_of_friend.name" -> "Ian", "COUNT(*)" -> 2),
        Map("friend_of_friend.name" -> "Derrick", "COUNT(*)" -> 1),
        Map("friend_of_friend.name" -> "Jill", "COUNT(*)" -> 1)), p.toList))
  }
}

