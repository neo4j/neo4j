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

import org.junit.Assert._
import org.junit.Test
import org.neo4j.cypher.docgen.DocumentingTestBase

//modeled after a mailing list suggestion in
//http://groups.google.com/group/neo4j/browse_thread/thread/57dbec6e904fca42

class MealTestIgnored extends DocumentingTestBase {
  override def graphDescription = List(
    "Peter eats Potatoes",
    "Peter eats Burger",
    "Burger composed_of Meat",
    "Burger composed_of Bread",
    "Bread composed_of Salt",
    "Bread composed_of Flour",
    "Bread composed_of Cereals")

  override val properties = Map(
    "Potatoes" -> Map("weight" -> 10),
    "Meat" -> Map("weight" -> 40),
    "Salt" -> Map("weight" -> 10),
    "Flour" -> Map("weight" -> 30),
    "Cereals" -> Map("weight" -> 80))

  //TODO:
  //Rels props:
  //burger--meat //quantity=2
  //burger--bread //quantity=2
  //bread--salt //quantity=2
  //bread--flour //quantity=1
  //bread--cereals //quantity=3
  //peter.weight = (potatoes.weight) + ( (meal.weight*composed_of.quantity) + ( (salt.weight*composed_of.quantity) + (flour.weight*composed_of.quantity) (cereal.weight*composed_of.quantity) ) )

  def section = "cookbook"

  @Test def weightedMeal() {
    testQuery(
      title = "Longest Paths -- find the leaf ingredients",
      text = """From the root, find the paths to all the leaf ingredients in order to return the paths for the weight calculation""",
      queryText = "MATCH (me)-[:eats]->(meal), " +
        "path=(meal)-[r:composed_of*0..]->(ingredient) " +
        "WHERE me.name='Peter' and not (ingredient) --> ()" +
        "RETURN ingredient.name ",
      optionalResultExplanation = "",
      assertions = (p) => {
        val result = p.toList
        assertEquals(Set(Map("ingredient.name" -> "Potatoes"),
          Map("ingredient.name" -> "Meat"),
          Map("ingredient.name" -> "Salt"),
          Map("ingredient.name" -> "Flour"),
          Map("ingredient.name" -> "Cereals")), result.toSet)
        assertEquals(result.size, 5)
      })
  }
}
