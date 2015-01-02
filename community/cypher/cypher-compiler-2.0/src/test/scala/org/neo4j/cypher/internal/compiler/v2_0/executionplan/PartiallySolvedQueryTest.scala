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
package org.neo4j.cypher.internal.compiler.v2_0.executionplan

import org.junit.Test
import org.neo4j.cypher.internal.compiler.v2_0.commands.{SingleNode, AllIdentifiers, CreateNodeStartItem, Query}
import org.neo4j.cypher.internal.compiler.v2_0.mutation.{DeleteEntityAction, CreateNode}
import org.scalatest.Assertions
import org.neo4j.cypher.internal.compiler.v2_0.executionplan.builders.Unsolved
import org.neo4j.cypher.internal.compiler.v2_0.commands.expressions.Identifier

class PartiallySolvedQueryTest extends Assertions {
  @Test def should_compact_query() {
    // Given CREATE a1 WITH * CREATE a2 WITH * CREATE a3
    val q1 = Query.start(createNode("a1")).returns()
    val q2 = Query.start(createNode("a2")).tail(q1).returns(AllIdentifiers())
    val q3 = Query.start(createNode("a3")).tail(q2).returns(AllIdentifiers())

    // When
    val psq = PartiallySolvedQuery(q3)

    // Then CREATE a1,a2,a3
    assert(psq.start.toSet === Set(Unsolved(createNode("a1")), Unsolved(createNode("a2")), Unsolved(createNode("a3"))))
  }

  @Test def should_not_compact_query() {
    // Given MATCH (a) WITH a DELETE a WITH a CREATE (:Person)
    val deleteAction = DeleteEntityAction(Identifier("a"))
    val q3 = Query.start(createNode("a3")).returns()
    val q2 = Query.updates(deleteAction).tail(q3).returns(AllIdentifiers())
    val q1 = Query.matches(SingleNode("a")).tail(q2).returns(AllIdentifiers())

    // When
    val psq = PartiallySolvedQuery(q1)

    // Then First query part doesn't contain updates
    assert(psq.updates.isEmpty)

    // Second query part contains no create node
    assert(psq.tail.get.updates.toList === List(Unsolved(deleteAction)))
    assert(psq.tail.get.start.isEmpty)

    // Third part contains the create node
    assert(psq.tail.get.tail.get.start.toList === List(Unsolved(createNode("a3"))))

  }

  private def createNode(name: String) = CreateNodeStartItem(CreateNode(name, Map.empty, Seq.empty))
}
