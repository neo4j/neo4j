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
package org.neo4j.cypher.internal.compiler.v2_0.executionplan.builders

import org.neo4j.cypher.internal.compiler.v2_0._
import commands._
import commands.expressions._
import commands.values.{KeyToken, TokenType}
import commands.values.TokenType._
import commands.values.KeyToken.Unresolved
import executionplan.PartiallySolvedQuery
import mutation.{ForeachAction, UpdateAction, MergeNodeAction}
import pipes.FakePipe
import spi.PlanContext
import symbols._
import org.junit.Test
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar

class MergeStartPointBuilderTest extends BuilderTest with MockitoSugar {
  def builder = new MergeStartPointBuilder

  override val context = mock[PlanContext]
  val identifier = "n"
  val otherIdentifier = "p"
  val label = "Person"
  val property = "prop"
  val propertyKey = PropertyKey(property)
  val otherProperty = "prop2"
  val otherPropertyKey = PropertyKey(otherProperty)
  val expression = Literal(42)
  val mergeNodeAction = MergeNodeAction("x", Map.empty, Seq(Label("Label")), Seq(HasLabel(Identifier("x"), KeyToken.Unresolved("Label", TokenType.Label))), Seq.empty, Seq.empty, None)

  @Test
  def should_solved_merge_node_start_points() {
    // Given MERGE (x:Label)
    val pipe = new FakePipe(Iterator.empty, identifier -> CTNode)
    val query = q(
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

  @Test
  def should_solved_merge_node_start_points_inside_foreach() {
    // Given FOREACH(x in [1,2,3] | MERGE (x:Label {prop:x}))
    val pipe = new FakePipe(Iterator.empty, identifier -> CTNode)
    val collection = Collection(Literal(1), Literal(2), Literal(3))
    val prop = Unresolved("prop", TokenType.PropertyKey)
    val query = q(
      updates = Seq(ForeachAction(collection, "x", Seq(mergeNodeAction.copy(props = Map(prop -> Identifier("x"))))))
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
