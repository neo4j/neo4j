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
package org.neo4j.cypher.docgen

import org.junit.Assert._
import org.junit.Test
import org.neo4j.cypher.QueryStatisticsTestSupport
import org.neo4j.graphdb.Node
import org.neo4j.visualization.graphviz.{AsciiDocSimpleStyle, GraphStyle}

class DocumentationTestBaseTest extends DocumentingTestBase with QueryStatisticsTestSupport {
  override def graphDescription = List("A KNOWS B", "A BLOCKS C", "D KNOWS A", "B KNOWS E", "C KNOWS E", "B BLOCKS D")

  override protected def getGraphvizStyle: GraphStyle =
    AsciiDocSimpleStyle.withAutomaticRelationshipTypeColors()

  override val properties = Map(
    "A" -> Map("name" -> "Anders"),
    "B" -> Map("name" -> "Bossman"),
    "C" -> Map("name" -> "Ceasar"),
    "D" -> Map("name" -> "David"),
    "E" -> Map("name" -> "Emil"))

  def section = "InternalTesting"

  @Test def test_documentation_test_base() {
    testQuery(
      title = "Test DocumentationTestBase",
      text = "Aggregated results have to pass through a `WITH` clause to be able to filter on.",
      queryText = """match (david)--(otherPerson)-->() where david.name='David' with otherPerson, count(*) as foaf where foaf > 1 return otherPerson""",
      optionalResultExplanation = """The person connected to David with the at least more than one outgoing relationship will be returned by the query.""",
      assertions = (p) => assertEquals(List(node("A")), p.columnAs[Node]("otherPerson").toList))

    // ensure that the result/graph is actually printed to the file and not empty.

    val resultSource = io.Source.fromFile("target/docs/dev/ql/internaltesting/includes/internaltesting-test-documentationtestbase.result.asciidoc", "utf-8")
    val resultLines = resultSource.mkString
    resultSource.close()
    assert(resultLines.contains("Anders"))
    assert(resultLines.contains("otherPerson"))

    val graphSource = io.Source.fromFile("target/docs/dev/ql/internaltesting/includes/cypher-documentationbase-graph.asciidoc", "utf-8")
    val graphLines = graphSource.mkString
    graphSource.close()
    assert(graphLines.contains("Anders"))
    assert(graphLines.contains("David"))
    assert(graphLines.contains("Emil"))
    assert(graphLines.contains("Bossman"))
    assert(graphLines.contains("Ceasar"))
    assert(graphLines.contains("KNOWS"))
    assert(graphLines.contains("BLOCKS"))
  }

  @Test def create_mutiple_nodes_from_maps() {
    testQuery(
      title = "Create multiple nodes with parameters for properties",
      text = """
By providing Cypher an array of maps, it will create a node for each map.
_When you do this, you can't create anything else in the same +CREATE+ statement_.
""",
      parameters =
        Map("props" -> List(
          Map("name" -> "Andres", "position" -> "Developer"),
          Map("name" -> "Michael", "position" -> "Developer")))
      ,
      queryText = "create ({props})",
      optionalResultExplanation = "",
      assertions = (p) => assertStats(p, nodesCreated = 2, propertiesSet = 4))

    // ensure that the parameters are printed
    val resultSource = io.Source.fromFile("target/docs/dev/ql/internaltesting/create-multiple-nodes-with-parameters-for-properties.asciidoc", "utf-8")
    val resultLines = resultSource.mkString
    resultSource.close()
    assert(resultLines.contains("\n.Parameters\n"))
    assert(resultLines.contains("\"props\""))
    assert(resultLines.contains("\"name\""))
    assert(resultLines.contains("\"Michael\""))
  }
}
