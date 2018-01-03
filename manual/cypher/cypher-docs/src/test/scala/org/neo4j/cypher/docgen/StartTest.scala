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

import org.junit.Assert.assertEquals
import org.junit.Test
import org.neo4j.kernel.GraphDatabaseAPI

class StartTest extends DocumentingTestBase {
  override def graphDescription = List("A KNOWS B", "A KNOWS C")
  override def indexProps = List("name")

  def section: String = "Start"

  @Test def nodes_by_index() {
    generateConsole = false
    testQuery(
      title = "Node by index seek",
      text = "When the starting point can be found by using index seeks, it can be done like this: `node:index-name(key = \"value\")`. In this example, there exists a node index named `nodes`.",
      queryText = """start n=node:nodes(name = "A") return n""",
      optionalResultExplanation = """The query returns the node indexed with the name "+A+".""",
      assertions = (p) => assertEquals(List(Map("n" -> node("A"))), p.toList))
  }

  @Test def relationships_by_index() {
    generateConsole = false
    val setPropertyAndUpdateLegacyIndex = (db: GraphDatabaseAPI) => db.inTx {
      val r = db.getRelationshipById(0)
      val property = "name"
      val value = "Andrés"
      r.setProperty(property, value)
      db.index().forRelationships("rels").add(r, property, value)
    }

    // TODO this should be changed to use the standard graph for this section somehow.
    prepareAndTestQuery(
      title = "Relationship by index seek",
      text = "When the starting point can be found by using index seeks, it can be done like this: `relationship:index-name(key = \"value\")`.",
      queryText = """start r=relationship:rels(name = "Andrés") return r""",
      optionalResultExplanation = """The relationship indexed with the +name+ property set to "+Andrés+" is returned by the query.""",
      assertions = (p) => assertEquals(List(Map("r" -> rel(0))), p.toList),
      prepare = setPropertyAndUpdateLegacyIndex)
  }

  @Test def nodes_by_index_query() {
    generateConsole = false
    testQuery(
      title = "Node by index query",
      text = "When the starting point can be found by more complex Lucene queries, this is the syntax to use: `node:index-name(\"query\")`." +
        "This allows you to write more advanced index queries.",
      queryText = """start n=node:nodes("name:A") return n""",
      optionalResultExplanation = """The node indexed with name "A" is returned by the query.""",
      assertions = (p) => assertEquals(List(Map("n" -> node("A"))), p.toList))
  }

}
