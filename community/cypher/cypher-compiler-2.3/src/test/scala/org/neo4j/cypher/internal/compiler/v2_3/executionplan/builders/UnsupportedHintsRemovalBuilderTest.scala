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
package org.neo4j.cypher.internal.compiler.v2_3.executionplan.builders

import org.neo4j.cypher.internal.compiler.v2_3.commands.{NodeById, NodeJoin, RelationshipById, StartItem}
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.PartiallySolvedQuery
import org.neo4j.cypher.internal.compiler.v2_3.pipes.PipeMonitor

class UnsupportedHintsRemovalBuilderTest extends BuilderTest {

  def builder = new UnsupportedHintsRemovalBuilder

  test("should accept query with unsolved join hint") {
    val query = newQuery(
      start = Seq[StartItem](NodeById("a", 1), NodeById("b", 2), NodeJoin("c"), RelationshipById("d", 3))
    )

    assertAccepts(query)
  }

  test("should reject empty list of start items") {
    val query = newQuery()

    assertRejects(query)
  }

  test("should reject query without unsolved join hint") {
    val query = newQuery(
      start = Seq[StartItem](NodeById("a", 1), NodeById("b", 2), RelationshipById("c", 3), RelationshipById("d", 4))
    )

    assertRejects(query)
  }

  test("should filter out the unsolved join hint") {
    val query = newQuery(
      start = Seq[StartItem](NodeById("a", 1), NodeJoin("c"), RelationshipById("d", 3))
    )

    startItemsAfterBuilder(query) shouldEqual Seq(Unsolved(NodeById("a", 1)), Unsolved(RelationshipById("d", 3)))
  }

  test("should not modify start items when no join hint is present") {
    val query = newQuery(
      start = Seq[StartItem](NodeById("a", 1), NodeById("b", 2), RelationshipById("c", 3))
    )

    startItemsAfterBuilder(query) shouldEqual Seq(Unsolved(NodeById("a", 1)), Unsolved(NodeById("b", 2)), Unsolved(RelationshipById("c", 3)))
  }

  test("should do nothing given empty list of start items") {
    val query = newQuery()

    startItemsAfterBuilder(query) shouldBe empty
  }

  private def startItemsAfterBuilder(query: PartiallySolvedQuery) =
    builder.apply(plan(query), context)(mock[PipeMonitor]).query.start
}
