/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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

class ReturnTest extends DocumentingTestBase {
  def graphDescription = List("A KNOWS B", "A BLOCKS B")

  def section = "Return"

  override val properties = Map("A" -> Map("<<!!__??>>" -> "Yes!", "age" -> 55))

  @Test def returnNode() {
    testQuery(
      title = "Return nodes",
      text = "To return a node, list it in the return statemenet.",
      queryText = """start n=(%B%) return n""",
      returns = """The node.""",
      (p) => assertEquals(List(Map("n" -> node("B"))), p.toList))
  }

  @Test def returnRelationship() {
    testQuery(
      title = "Return relationships",
      text = "To return a relationship, just include it in the return list.",
      queryText = """start n=(%A%) match (n)-[r:KNOWS]->(c) return r""",
      returns = """The relationship.""",
      (p) => assertEquals(1, p.size))
  }

  @Test def returnProperty() {
    testQuery(
      title = "Return property",
      text = "To return a property, use the dot separator, like this:",
      queryText = """start n=(%A%) return n.name""",
      returns = """The the value of the property 'name'.""",
      (p) => assertEquals(List(Map("n.name" -> "A")), p.toList))
  }


  @Test def weird_variable_names() {
    testQuery(
      title = "Identifier with uncommon characters",
      text = """To introduce a placeholder that is made up of characters that are
      outside of the english alphabet, you can use the ` to enclose the identifier, like this:""",
      queryText = """start `This isn't a common identifier`=(%A%)
return `This isn't a common identifier`.`<<!!__??>>`""",
      returns = """The node indexed with name "A" is returned""",
      (p) => assertEquals(List(Map("This isn't a common identifier.<<!!__??>>" -> "Yes!")), p.toList))
  }


  @Test def nullable_properties() {
    testQuery(
      title = "Optional properties",
      text = """If a property might or might not be there, you can select it optionally by adding a questionmark to the identifier,
like this:""",
      queryText = """start n=(%A%, %B%) return n.age?""",
      returns = """The age when the node has that property, or null if the property is not there.""",
      (p) => assertEquals(List(55, null), p.columnAs[Int]("n.age").toList))
  }

  @Test def distinct_output() {
    testQuery(
      title = "Unique results",
      text = """DISTINCT retrieves only unique rows depending on the columns that have been selected to output.""",
      queryText = """start a=(%A%) match (a)-->(b) return distinct b""",
      returns = """The node named B, but only once.""",
      (p) => assertEquals(List(node("B")), p.columnAs[Node]("b").toList))
  }
}