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

/**
 * @author ata
 * @since 6/4/11
 */

class WhereTest extends DocumentingTestBase
{
  def indexProps = List()

  def graphDescription = List("Andres KNOWS Tobias")

  override val properties = Map(
    "Andres" -> Map("age" -> 36l),
    "Tobias" -> Map("age" -> 25l)
  )

  def section = "Where"

  @Test def filter_on_property()
  {
    testQuery(
      title = "Filter on node property",
      text = "To filter on a property, write your clause after the WHERE keyword..",
      queryText = """start n=(%Andres%, %Tobias%) where n.age < 30 return n""",
      returns = """The node.""",
      (p) => assertEquals(List(node("Tobias")), p.columnAs[Node]("n").toList)
    )
  }

  @Test def boolean_operations()
  {
    testQuery(
      title = "Boolean operations",
      text = "You can use the expected boolean operators AND and OR, and also the boolean function NOT().",
      queryText = """start n=(%Andres%, %Tobias%) where (n.age < 30 and n.name = "Tobias") or not(n.name="Tobias")  return n""",
      returns = """The node.""",
      (p) => assertEquals(List(node("Andres"), node("Tobias")), p.columnAs[Node]("n").toList)
    )
  }

  @Test def regular_expressions()
  {
    testQuery(
      title = "Regular expressions",
      text = "You can match on regular expressions by using =~ /regexp/, like this:",
      queryText = """start n=(%Andres%, %Tobias%) where n.name =~ /Tob.*/ return n""",
      returns = """The node named Tobias.""",
      (p) => assertEquals(List(node("Tobias")), p.columnAs[Node]("n").toList)
    )
  }
}