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

import org.junit.Test
import org.junit.Assert._
import org.neo4j.graphdb.Node
import org.neo4j.visualization.graphviz.GraphStyle
import org.neo4j.visualization.graphviz.AsciiDocSimpleStyle

class ReturnTest extends DocumentingTestBase {
  override def graphDescription = List("A KNOWS B", "A BLOCKS B")

  override protected def getGraphvizStyle: GraphStyle =
    AsciiDocSimpleStyle.withAutomaticRelationshipTypeColors()

  def section = "Return"

  override val properties =
    Map("A" -> Map[String, Any]("happy" -> "Yes!", "age" -> 55))

  @Test def returnNode() {
    testQuery(
      title = "Return nodes",
      text = "To return a node, list it in the `RETURN` statement.",
      queryText = """match (n {name: "B"}) return n""",
      optionalResultExplanation = """The example will return the node.""",
      assertions = (p) => assertEquals(List(Map("n" -> node("B"))), p.toList))
  }

  @Test def returnRelationship() {
    testQuery(
      title = "Return relationships",
      text = "To return a relationship, just include it in the `RETURN` list.",
      queryText = """match (n {name: "A"})-[r:KNOWS]->(c) return r""",
      optionalResultExplanation = """The relationship is returned by the example.""",
      assertions = (p) => assertEquals(1, p.size))
  }

  @Test def returnProperty() {
    testQuery(
      title = "Return property",
      text = "To return a property, use the dot separator, like this:",
      queryText = """match (n {name: "A"}) return n.name""",
      optionalResultExplanation = """The value of the property `name` gets returned.""",
      assertions = (p) => assertEquals(List(Map("n.name" -> "A")), p.toList))
  }


  @Test def weird_variable_names() {
    testQuery(
      title = "Identifier with uncommon characters",
      text = """To introduce a placeholder that is made up of characters that are
      outside of the english alphabet, you can use the +`+ to enclose the identifier, like this:""",
      queryText = """match (`This isn't a common identifier`) where `This isn't a common identifier`.name='A'
return `This isn't a common identifier`.happy""",
      optionalResultExplanation = """The node with name "A" is returned""",
      assertions = (p) => assertEquals(List(Map("`This isn't a common identifier`.happy" -> "Yes!")), p.toList))
  }

  @Test def nullable_properties() {
    testQuery(
      title = "Optional properties",
      text = """If a property might or might not be there, you can still select it as usual. It will be treated as +NULL+ if it is missing""",
      queryText = """match (n) return n.age""",
      optionalResultExplanation = """This example returns the age when the node has that property, or +null+ if the property is not there.""",
      assertions = (p) => assertEquals(List(55, null), p.columnAs[Int]("n.age").toList))
  }

  @Test def distinct_output() {
    testQuery(
      title = "Unique results",
      text = """`DISTINCT` retrieves only unique rows depending on the columns that have been selected to output.""",
      queryText = """match (a {name: "A"})-->(b) return distinct b""",
      optionalResultExplanation = """The node named B is returned by the query, but only once.""",
      assertions = (p) => assertEquals(List(node("B")), p.columnAs[Node]("b").toList))
  }

  @Test def column_aliasing() {
    testQuery(
      title = "Column alias",
      text = """If the name of the column should be different from the expression used, you can rename it by using `AS` <new name>.""",
      queryText = """match (a {name: "A"}) return a.age AS SomethingTotallyDifferent""",
      optionalResultExplanation = """Returns the age property of a node, but renames the column.""",
      assertions = (p) => assertEquals(List(55), p.columnAs[Node]("SomethingTotallyDifferent").toList))
  }

  @Test def other_expressions() {
    testQuery(
      title = "Other expressions",
      text = "Any expression can be used as a return item -- literals, predicates, properties, functions, and everything else.",
      queryText = """match (a {name: "A"}) return a.age > 30, "I'm a literal", (a)-->()""",
      optionalResultExplanation = "Returns a predicate, a literal and function call with a pattern expression parameter.",
      assertions = (p) => {
        val row: Map[String, Any] = p.toList.head

        assertEquals("I'm a literal", row("\"I'm a literal\""))
        assertEquals(true, row("a.age > 30"))
        assert(row("(a)-->()").asInstanceOf[Seq[_]].size === 2)
      })
  }

  @Test def return_all_identifiers() {
    testQuery(
      title = "Return all elements",
      text = """When you want to return all nodes, relationships and paths found in a query, you can use the `*` symbol.""",
      queryText = """match p=(a {name: "A"})-[r]->(b) return *""",
      optionalResultExplanation = """This returns the two nodes, the relationship and the path used in the query.""",
      assertions = (p) => assertEquals(p.toList.head.keys, Set("a", "b", "r", "p")))
  }
}
