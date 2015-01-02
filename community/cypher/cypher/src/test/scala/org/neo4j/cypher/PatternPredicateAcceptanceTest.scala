/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher

import org.scalatest.Matchers
import org.junit.Test
import org.neo4j.graphdb.Node

class PatternPredicateAcceptanceTest extends ExecutionEngineJUnitSuite with Matchers {
  
  @Test
  def should_filter_relationships_with_properties() {
    // given
    relate(createNode(), createNode(), "id" -> 1)
    relate(createNode(), createNode(), "id" -> 2)

    // when
    val result = execute("match (n) where (n)-[{id: 1}]->() return n").columnAs[Node]("n").toList

    // then
    result.size should be(1)
  }

  @Test
  def should_filter_var_length_relationships_with_properties() {
    // Given a graph with two paths from the :Start node - one with all props having the 42 value, and one where not all rels have this property

    def createPath(value: Any): Node = {
      val node0 = createLabeledNode("Start")
      val node1 = createNode()
      
      relate(node0, node1, "prop" -> value)
      relate(node1, createNode(), "prop" -> value)
      
      node0
    }

    val start1 = createPath(42)
    createPath(666)

    // when
    val result = executeScalar[Node]("match (n:Start) where (n)-[*2 {prop: 42}]->() return n")

    // then
    assert(start1 == result)
  }
}
