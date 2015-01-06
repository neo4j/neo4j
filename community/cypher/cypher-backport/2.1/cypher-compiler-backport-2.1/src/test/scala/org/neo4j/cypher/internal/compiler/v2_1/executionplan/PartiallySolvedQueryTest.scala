/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.executionplan

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_1.commands.expressions.Identifier
import org.neo4j.cypher.internal.compiler.v2_1.commands.{AllIdentifiers, CreateNodeStartItem, Query, SingleNode}
import org.neo4j.cypher.internal.compiler.v2_1.executionplan.builders.Unsolved
import org.neo4j.cypher.internal.compiler.v2_1.mutation.{CreateNode, DeleteEntityAction}

class PartiallySolvedQueryTest extends CypherFunSuite {

  test("should_compact_query") {
    // Given CREATE a1 WITH * CREATE a2 WITH * CREATE a3
    val q1 = Query.start(createNode("a1")).returns()
    val q2 = Query.start(createNode("a2")).tail(q1).returns(AllIdentifiers())
    val q3 = Query.start(createNode("a3")).tail(q2).returns(AllIdentifiers())

    // When
    val psq = PartiallySolvedQuery(q3)

    // Then CREATE a1,a2,a3
    psq.start.toSet should equal(Set(Unsolved(createNode("a1")), Unsolved(createNode("a2")), Unsolved(createNode("a3"))))
  }

  test("should_not_compact_query") {
    // Given MATCH (a) WITH a DELETE a WITH a CREATE (:Person)
    val deleteAction = DeleteEntityAction(Identifier("a"))
    val q3 = Query.start(createNode("a3")).returns()
    val q2 = Query.updates(deleteAction).tail(q3).returns(AllIdentifiers())
    val q1 = Query.matches(SingleNode("a")).tail(q2).returns(AllIdentifiers())

    // When
    val psq = PartiallySolvedQuery(q1)

    // Then First query part doesn't contain updates
    psq.updates shouldBe empty

    // Second query part contains no create node
    psq.tail.get.updates.toList should equal(List(Unsolved(deleteAction)))
    psq.tail.get.start shouldBe empty

    // Third part contains the create node
    psq.tail.get.tail.get.start.toList should equal(List(Unsolved(createNode("a3"))))

  }

  private def createNode(name: String) = CreateNodeStartItem(CreateNode(name, Map.empty, Seq.empty))
}
