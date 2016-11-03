/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_1.executionplan.builders

import org.mockito.Mockito._
import org.neo4j.cypher.internal.compiler.v3_1.commands.expressions._
import org.neo4j.cypher.internal.compiler.v3_1.commands.predicates.HasLabel
import org.neo4j.cypher.internal.compiler.v3_1.commands.values.KeyToken.Unresolved
import org.neo4j.cypher.internal.compiler.v3_1.commands.values.TokenType._
import org.neo4j.cypher.internal.compiler.v3_1.commands.values.{KeyToken, TokenType}
import org.neo4j.cypher.internal.compiler.v3_1.mutation.{ForeachAction, MergeNodeAction}
import org.neo4j.cypher.internal.compiler.v3_1.pipes.FakePipe
import org.neo4j.cypher.internal.compiler.v3_1.spi.PlanContext
import org.neo4j.cypher.internal.frontend.v3_1.symbols._

class MergeStartPointBuilderTest extends BuilderTest {
  def builder = new MergeStartPointBuilder

  context = mock[PlanContext]
  val variable = "n"
  val otherVariable = "p"
  val label = "Person"
  val property = "prop"
  val propertyKey = PropertyKey(property)
  val otherProperty = "prop2"
  val otherPropertyKey = PropertyKey(otherProperty)
  val expression = Literal(42)
  val mergeNodeAction = MergeNodeAction("x", Map.empty, Seq(Label("Label")), Seq(HasLabel(Variable("x"), KeyToken.Unresolved("Label", TokenType.Label))), Seq.empty, Seq.empty, None)

  test("should_solved_merge_node_start_points") {
    // Given MERGE (x:Label)
    val pipe = new FakePipe(Iterator.empty, variable -> CTNode)
    val query = newQuery(
      updates = Seq(mergeNodeAction)
    )
    when(context.getOptLabelId("Label")).thenReturn(Some(42))

    // When
    val plan = assertAccepts(pipe, query)

    // Then
    plan.query.updates match {
      case Seq(Unsolved(MergeNodeAction("x", _, _, Seq(), Seq(), Seq(), _))) =>
      case _                                                                 =>
        fail("Expected something else, but got this: " + plan.query.start)
    }
  }

  test("should_solved_merge_node_start_points_inside_foreach") {
    // Given FOREACH(x in [1,2,3] | MERGE (x:Label {prop:x}))
    val pipe = new FakePipe(Iterator.empty, variable -> CTNode)
    val collection = ListLiteral(Literal(1), Literal(2), Literal(3))
    val prop = Unresolved("prop", TokenType.PropertyKey)
    val query = newQuery(
      updates = Seq(ForeachAction(collection, "x", Seq(mergeNodeAction.copy(props = Map(prop -> Variable("x"))))))
    )
    when(context.getOptLabelId("Label")).thenReturn(Some(42))
    when(context.getUniquenessConstraint("Label", "prop")).thenReturn(None)

    // When
    val plan = assertAccepts(pipe, query)

    // Then
    plan.query.updates match {
      case Seq(Unsolved(ForeachAction(_, _, Seq(MergeNodeAction(_,_,_,_,_,_,producer))))) if producer.nonEmpty =>
      case _                                                                 =>
        fail("Expected something else, but got this: " + plan.query.updates)
    }
  }
}
