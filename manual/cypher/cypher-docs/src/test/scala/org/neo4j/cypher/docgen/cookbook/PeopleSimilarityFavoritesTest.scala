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


class PeopleSimilarityFavoritesTest extends DocumentingTestBase {
  override def graphDescription = List("Joe favorite Cats", "Joe favorite Bikes",
    "Joe friend Sara",
    "Sara favorite Cats",
    "Sara favorite Bikes",
    "Derrick favorite Cats",
    "Derrick favorite Bikes",
    "Jill favorite Bikes")

  def section = "cookbook"
  override val noTitle = true;

  override protected def getGraphvizStyle: GraphStyle = {
    AsciiDocSimpleStyle.withAutomaticRelationshipTypeColors()
  }

  @Test def peopleSimilarityFavorites() {
    testQuery(
      title = "Find people based on similar favorites",
      text = """To find out the possible new friends based on them liking similar things as the asking person, use a query like this:""",
      queryText = "MATCH (me {name: 'Joe'})-[:favorite]->(stuff)<-[:favorite]-(person) " +
                  "WHERE NOT (me)-[:friend]-(person) " +
                  "RETURN person.name, count(stuff) " +
                  "ORDER BY count(stuff) DESC",
      optionalResultExplanation = "The list of possible friends ranked by them liking similar stuff that are not yet friends is returned.",
      assertions = (p) => assertEquals(List(Map("person.name" -> "Derrick", "count(stuff)" -> 2),
        Map("person.name" -> "Jill", "count(stuff)" -> 1)), p.toList))
  }
}
