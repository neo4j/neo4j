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
package org.neo4j.cypher.docgen.cookbook

import org.junit.Test
import org.junit.Assert._
import org.neo4j.cypher.docgen.DocumentingTestBase
import org.neo4j.visualization.graphviz.GraphStyle
import org.neo4j.visualization.graphviz.AsciiDocSimpleStyle


class CoFavoritedPlacesTest extends DocumentingTestBase {
  def graphDescription = List("Joe favorite CoffeeShop1", 
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
      title = "Co-Favorited Places - Users Who Like x Also Like y",
      text = """Find places that people also like who favorite this place:

* Determine who has favorited place x.
* What else have they favorited that is not place x.""",
      queryText = """START place=node:node_auto_index(name = "CoffeeShop1")
      		MATCH place<-[:favorite]-person-[:favorite]->stuff 
      		RETURN stuff.name, count(*) 
      		ORDER BY count(*) DESC, stuff.name""",
      returns = "The list of places that are favorited by people that favorited the start place.",
      assertions = (p) => assertEquals(List(Map("stuff.name" -> "MelsPlace", "count(*)" -> 2),
          Map("stuff.name" -> "CoffeShop2", "count(*)" -> 1),
          Map("stuff.name" -> "SaunaX", "count(*)" -> 1)),p.toList))
  } 
  
  @Test def coTaggedPlaces() {
    testQuery(
      title = "Co-Tagged Places - Places Related through Tags",
      text = """Find places that are tagged with the same tags:

* Determine the tags for place x.
* What else is tagged the same as x that is not x.""",
      queryText = """START place=node:node_auto_index(name = "CoffeeShop1") 
      		MATCH place-[:tagged]->tag<-[:tagged]-otherPlace
      		RETURN otherPlace.name, collect(tag.name) 
      		ORDER BY otherPlace.name DESC""",
      returns = "The list of possible friends ranked by them liking similar stuff that are not yet friends is returned.",
      assertions = (p) => {
        assertEquals(List(Map("otherPlace.name" -> "MelsPlace", "collect(tag.name)" -> List("Cool", "Cosy")),
                Map("otherPlace.name" -> "CoffeeShop3", "collect(tag.name)" -> List("Cosy")),
                Map("otherPlace.name" -> "CoffeeShop2", "collect(tag.name)" -> List("Cool"))),p.toList)
      })
  } 
}
