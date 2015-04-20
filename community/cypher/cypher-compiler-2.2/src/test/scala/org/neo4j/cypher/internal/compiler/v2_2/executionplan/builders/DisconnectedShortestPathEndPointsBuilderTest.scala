/*
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
package org.neo4j.cypher.internal.compiler.v2_2.executionplan.builders

import org.neo4j.cypher.internal.compiler.v2_2.commands._
import org.neo4j.cypher.internal.compiler.v2_2.symbols._
import org.neo4j.cypher.internal.compiler.v2_2.executionplan.PartiallySolvedQuery
import org.neo4j.cypher.internal.compiler.v2_2.mutation.UpdateAction
import org.neo4j.cypher.internal.compiler.v2_2.pipes.FakePipe
import org.neo4j.graphdb.Direction
import org.scalatest.mock.MockitoSugar

class DisconnectedShortestPathEndPointsBuilderTest extends BuilderTest with MockitoSugar {
  def builder = new DisconnectedShortestPathEndPointsBuilder

  val identifier = "n"
  val otherIdentifier = "p"
  val shortestPath = ShortestPath("p",
    SingleNode(identifier), SingleNode(otherIdentifier), Seq.empty, Direction.OUTGOING, false, None, single = true, None)

  test("should_add_nodes_for_shortest_path") {
    // Given
    val query = q(
      patterns = Seq(shortestPath)
    )

    // When
    val step1 = assertAccepts(query)
    // The builder only solves a single node at the time, we we run it through twice
    val plan = assertAccepts(step1.pipe, step1.query)

    // Then
    assert(plan.query.start.toList ===
           Seq(
             Unsolved(AllNodes(identifier)),
             Unsolved(AllNodes(otherIdentifier))))
  }

  test("should_not_add_nodes_when_they_already_exist") {
    // Given
    val query = q(
      start = Seq(
        AllNodes(identifier),
        AllNodes(otherIdentifier)),
      patterns = Seq(shortestPath)
    )

    // When Then
    assertRejects(query)
  }

  test("should_add_the_missing_nodes_when_one_end_is_done") {
    // Given
    val query = q(
      start = Seq(
        AllNodes(identifier)
      ),
      patterns = Seq(shortestPath)
    )

    // When
    val plan = assertAccepts(query)

    // Then
    assert(plan.query.start.toList ===
           Seq(
             Unsolved(AllNodes(identifier)),
             Unsolved(AllNodes(otherIdentifier))))
  }

  test("should_add_one_node_when_the_incoming_pipe_is_missing_a_node") {
    // Given
    val query = q(
      patterns = Seq(shortestPath)
    )

    val pipe = new FakePipe(Iterator.empty, identifier -> CTNode)


    // When
    val plan = assertAccepts(pipe, query)

    // Then
    assert(plan.query.start.toList === Seq(Unsolved(AllNodes(otherIdentifier))))
  }

  private def q(start: Seq[StartItem] = Seq(),
                where: Seq[Predicate] = Seq(),
                updates: Seq[UpdateAction] = Seq(),
                patterns: Seq[Pattern] = Seq(),
                returns: Seq[ReturnColumn] = Seq(),
                tail: Option[PartiallySolvedQuery] = None) =
    PartiallySolvedQuery().copy(
      start = start.map(Unsolved(_)),
      where = where.map(Unsolved(_)),
      patterns = patterns.map(Unsolved(_)),
      returns = returns.map(Unsolved(_)),
      updates = updates.map(Unsolved(_)),
      tail = tail
    )
}
