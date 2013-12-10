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
package org.neo4j.cypher

import org.scalatest.Matchers
import org.junit.Test
import org.neo4j.graphdb.Node

class PatternPredicateAcceptanceTest extends ExecutionEngineHelper with Matchers {

  @Test
  def shouldHandlePropertiesInRelationships() {
    // given
    relate(createNode(), createNode(), "id" -> 1)
    relate(createNode(), createNode(), "id" -> 2)

    // when
    val result = execute("match (n)-[{id: 1}]->() return n").columnAs[Node]("n").toList

    // then
    result.size should be(1)
  }

  @Test
  def shouldHandlePropertiesInVarLengthRelationships() {
    // given
    val m = createNode()
    relate(createNode(), m, "id" -> 1)
    relate(m, createNode(), "id" -> 1)
    relate(createNode(), createNode(), "id" -> 2)

    // when
    val result = execute("match (n)-[*2 {id: 1}]->() return n").columnAs[Node]("n").toList

    // then
    result.size should be(1)
  }
}
