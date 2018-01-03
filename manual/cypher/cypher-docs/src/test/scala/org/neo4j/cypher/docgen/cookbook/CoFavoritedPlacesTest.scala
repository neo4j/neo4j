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

class CoFavoritedPlacesTest extends DocumentingTestBase {
  override def graphDescription = List("Joe favorite CoffeeShop1",
    "Joe favorite SaunaX",
    "Joe favorite MelsPlace",
    "Jill favorite CoffeeShop1",
    "Jill favorite MelsPlace",
    "CoffeeShop2 tagged Cool",
    "CoffeeShop1 tagged Cool",
    "CoffeeShop1 tagged Cosy",
    "CoffeeShop3 tagged Cosy",
    "MelsPlace tagged Cosy",
    "MelsPlace tagged Cool",
    "Jill favorite CoffeShop2")

  def section = "cookbook"

  override protected def getGraphvizStyle: GraphStyle = {
    AsciiDocSimpleStyle.withAutomaticRelationshipTypeColors()
  }

  @Test def coFavoritedPlaces() {
    testQuery(
      title = "Co-favorited places -- users who like x also like y",
      text = """Find places that people also like who favorite this place:

* Determine who has favorited place x.
* What else have they favorited that is not place x.""",
      queryText = """MATCH (place)<-[:favorite]-(person)-[:favorite]->(stuff)
          WHERE place.name = 'CoffeeShop1'
      		RETURN stuff.name, count(*)
      		ORDER BY count(*) DESC, stuff.name""",
      optionalResultExplanation = "The list of places that are favorited by people that favorited the start place.",
      assertions = (p) => assertEquals(Set(Map("stuff.name" -> "MelsPlace", "count(*)" -> 2),
        Map("stuff.name" -> "CoffeShop2", "count(*)" -> 1),
        Map("stuff.name" -> "SaunaX", "count(*)" -> 1)), p.toSet))
  }

  @Test def coTaggedPlaces() {
    testQuery(
      title = "Co-Tagged places -- places related through tags",
      text = """Find places that are tagged with the same tags:

* Determine the tags for place x.
* What else is tagged the same as x that is not x.""",
      queryText = """MATCH (place)-[:tagged]->(tag)<-[:tagged]-(otherPlace)
WHERE place.name = 'CoffeeShop1'
RETURN otherPlace.name, collect(tag.name)
ORDER BY length(collect(tag.name)) DESC, otherPlace.name""",
      optionalResultExplanation = "This query returns other places than CoffeeShop1 which share the same tags; they are ranked by the number of tags.",
      assertions = (p) => {
        assertEquals(List(
          Map("otherPlace.name" -> "MelsPlace", "collect(tag.name)" -> List("Cosy", "Cool")),
          Map("otherPlace.name" -> "CoffeeShop2", "collect(tag.name)" -> List("Cool")),
          Map("otherPlace.name" -> "CoffeeShop3", "collect(tag.name)" -> List("Cosy"))), p.toList)
      })
  }
}
