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
package org.neo4j.cypher.internal.compiler.v2_3.executionplan.builders

import org.neo4j.cypher.internal.compiler.v2_3.commands._
import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.{CollectionSliceExpression, Identifier, Literal}
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.PartiallySolvedQuery
import org.neo4j.cypher.internal.compiler.v2_3.mutation._

class UpdateActionBuilderTest extends BuilderTest {

  val builder = new UpdateActionBuilder()

  test("does_not_offer_to_solve_queries_without_start_items") {
    val q = PartiallySolvedQuery().
      copy(start = Seq(Unsolved(NodeById("s", 0))))

    assertRejects(q)
  }

  test("does_offer_to_solve_queries_without_start_items") {
    val q = PartiallySolvedQuery().
      copy(start = Seq(Unsolved(CreateNodeStartItem(CreateNode("r", Map(), Seq.empty)))))

    assertAccepts(q)
  }

  test("full_path") {
    val q = PartiallySolvedQuery().copy(start = Seq(
      Unsolved(CreateRelationshipStartItem(CreateRelationship("r1",
        RelationshipEndpoint(Identifier("a"), Map(), Seq.empty),
        RelationshipEndpoint(Identifier("  UNNAMED1"), Map(), Seq.empty), "KNOWS", Map()))),
      Unsolved(CreateRelationshipStartItem(CreateRelationship("r2",
        RelationshipEndpoint(Identifier("b"), Map(),  Seq.empty),
        RelationshipEndpoint(Identifier("  UNNAMED1"), Map(), Seq.empty), "LOVES", Map())))))


    val startPipe = createPipe(Seq("a", "b"))

    assertAccepts(startPipe, q)
  }

  test("single_relationship_missing_nodes") {
    val q = PartiallySolvedQuery().copy(start = Seq(
      Unsolved(CreateRelationshipStartItem(CreateRelationship("r",
        RelationshipEndpoint(Identifier("a"), Map(), Seq.empty),
        RelationshipEndpoint(Identifier("b"), Map(), Seq.empty), "LOVES", Map())))))

    assertAccepts(q)
  }

  test("single_relationship_missing_nodes_with_expression") {
    val q = PartiallySolvedQuery().copy(updates = Seq(
      Unsolved(CreateRelationship("r",
        RelationshipEndpoint(CollectionSliceExpression(Identifier("p"), Some(Literal(0)), Some(Literal(1))), Map(), Seq.empty),
        RelationshipEndpoint(Identifier("b"), Map(), Seq.empty), "LOVES", Map()))))

    assertRejects(q)
  }

  test("does_not_offer_to_solve_done_queries") {
    val q = PartiallySolvedQuery().
      copy(updates = Seq(Solved(DeleteEntityAction(Identifier("x"), forced = false))))

    assertRejects(q)
  }

  test("offers_to_solve_queries") {
    val q = PartiallySolvedQuery().
      copy(updates = Seq(Unsolved(DeleteEntityAction(Identifier("x"), forced = false))))
    val pipe = createPipe(nodes = Seq("x"))

    val resultPlan = assertAccepts(pipe, q)

    resultPlan.query should equal(q.copy(updates = q.updates.map(_.solve)))
    resultPlan.isUpdating should equal(true)
  }

  test("does_not_offer_to_delete_something_not_yet_there") {
    val q = PartiallySolvedQuery().
      copy(updates = Seq(Unsolved(DeleteEntityAction(Identifier("x"), forced = false))))

    assertRejects(q)
  }
}
