/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher.internal.mutation

import org.junit.{After, Before, Test}
import org.neo4j.cypher.GraphDatabaseTestBase
import org.neo4j.cypher.internal.commands.expressions.Literal
import org.neo4j.cypher.internal.pipes.{QueryStateHelper, QueryState}
import org.neo4j.graphdb.{PropertyContainer, Transaction}
import org.neo4j.cypher.internal.ExecutionContext

class MapPropertySetActionTest extends GraphDatabaseTestBase {

  var tx: Transaction = null
  var state: QueryState = null

  @Before
  def init() {
    tx = graph.beginTx()
    state = QueryStateHelper.queryStateFrom(graph)
  }

  @After
  def teardown() {
    tx.failure()
    tx.finish()
  }

  @Test def set_single_value_on_node() {
    val a = createNode()
    val m = Map("meaning_of_life" -> 420)

    setThatShit(a, m)

    assert(a.getProperty("meaning_of_life") === 420)
    assert(state.getStatistics.propertiesSet === 1)
  }

  @Test def set_multiple_properties() {
    val a = createNode()
    val m = Map("A" -> 1, "b" -> 2)

    setThatShit(a, m)

    assert(a.getProperty("A") === 1)
    assert(a.getProperty("b") === 2)
    assert(state.getStatistics.propertiesSet === 2)
  }

  @Test def set_properties_on_relationship() {
    val a = createNode()
    val r = relate(refNode, a)

    val m = Map("A" -> 1, "b" -> 2)

    setThatShit(r, m)

    assert(r.getProperty("A") === 1)
    assert(r.getProperty("b") === 2)
    assert(state.getStatistics.propertiesSet === 2)
  }

  @Test def transfer_properties_from_node_to_node() {
    val from = createNode("foo" -> "bar", "buzz" -> 42)
    val to = createNode()

    setThatShit(to, from)

    assert(to.getProperty("foo") === "bar")
    assert(to.getProperty("buzz") === 42)
    assert(state.getStatistics.propertiesSet === 2)
  }

  @Test def remove_properties_from_node() {
    val from = Map("a" -> 1)
    val to = createNode("b" -> 2)

    setThatShit(to, from)

    assert(to.getProperty("a") === 1)
    assert(to.hasProperty("b") === false, "Expected the `b` property to removed")
    assert(state.getStatistics.propertiesSet === 2)
  }

  @Test def should_overwrite_values() {
    val from = Map("a" -> 1)
    val to = createNode("a" -> "apa")

    setThatShit(to, from)

    assert(to.getProperty("a") === 1)
    assert(state.getStatistics.propertiesSet === 1)
  }

  private def setThatShit(a: PropertyContainer, m: Any) {
    val setter = MapPropertySetAction(Literal(a), Literal(m))
    setter.exec(ExecutionContext.empty, state)
  }
}