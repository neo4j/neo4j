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

import org.junit.Test
import org.neo4j.graphdb.Node
import org.junit.Assert._
import org.neo4j.visualization.graphviz.GraphStyle
import org.neo4j.visualization.graphviz.AsciiDocSimpleStyle

class WithTest extends DocumentingTestBase {
  override def graphDescription = List("A KNOWS B", "A BLOCKS C", "D KNOWS A", "B KNOWS E", "C KNOWS E", "B BLOCKS D")

  override protected def getGraphvizStyle: GraphStyle =
    AsciiDocSimpleStyle.withAutomaticRelationshipTypeColors()

  override val properties = Map(
    "A" -> Map("name" -> "Anders"),
    "B" -> Map("name" -> "Bossman"),
    "C" -> Map("name" -> "Ceasar"),
    "D" -> Map("name" -> "David"),
    "E" -> Map("name" -> "Emil")
  )

  def section = "With"

  @Test def filter_on_aggregate_functions_results() {
    testQuery(
      title = "Filter on aggregate function results",
      text = "Aggregated results have to pass through a `WITH` clause to be able to filter on.",
      queryText = """MATCH (david {name: 'David'})--(otherPerson)-->() WITH otherPerson, count(*) AS foaf WHERE foaf > 1 RETURN otherPerson""",
      optionalResultExplanation = """The person connected to David with the at least more than one outgoing relationship will be returned by the query.""",
      assertions = (p) => assertEquals(List(node("A")), p.columnAs[Node]("otherPerson").toList)
    )
  }

  @Test def sort_collect_results() {
    testQuery(
      title = "Sort results before using collect on them",
      text = "You can sort your results before passing them to collect, thus sorting the resulting list.",
      queryText = """MATCH (n) WITH n ORDER BY n.name DESC LIMIT 3 RETURN collect(n.name)""",
      optionalResultExplanation = """A list of the names of people in reverse order, limited to 3, in a list.""",
      assertions = (p) => assertEquals(List(List("Emil", "David", "Ceasar")), p.columnAs[Seq[String]]("collect(n.name)").toList))
  }

  @Test def limit_branching() {
    testQuery(
      title = "Limit branching of your path search",
      text = "You can match paths, limit to a certain number, and then match again using those paths as a base As well as any number of similar limited searches.",
      queryText = """MATCH (n {name: 'Anders'})--(m) WITH m ORDER BY m.name DESC LIMIT 1 MATCH (m)--(o) RETURN o.name""",
      optionalResultExplanation = """Starting at Anders, find all matching nodes, order by name descending and get the top result, then find all the nodes connected to that top result, and return their names.""",
      assertions = (p) => assertEquals(List("Bossman", "Anders"), p.columnAs[String]("o.name").toList))
  }
}
