/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.runtime.interpreted.ValueComparisonHelper._
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.{Predicate, True}
import org.neo4j.cypher.internal.runtime.interpreted.pipes.VarLengthExpandPipeTest.createVarLengthPredicate
import org.neo4j.cypher.internal.runtime.{ExecutionContext, QueryContext}
import org.neo4j.cypher.internal.v4_0.expressions.SemanticDirection
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.{NodeValue, RelationshipValue}

class VarLengthExpandPipeTest extends CypherFunSuite {

  test("should support var length expand with expansion-stage filtering") {
    // given
    val firstNode = newMockedNode(4)
    val startNode = newMockedNode(1)
    val middleNode = newMockedNode(2)
    val endNode = newMockedNode(3)
    val initialRelationship = newMockedRelationship(4, firstNode, startNode)
    val leftRelationship1 = newMockedRelationship(1, startNode, middleNode)
    val leftRelationship2 = newMockedRelationship(2, startNode, middleNode)
    val rightRelationship = newMockedRelationship(3, middleNode, endNode)
    val query = mock[QueryContext]
    val nodeMapping: Map[(Long, SemanticDirection), Seq[RelationshipValue]] = Map(
      (firstNode.id, SemanticDirection.OUTGOING) -> Seq(initialRelationship),
      (startNode.id, SemanticDirection.INCOMING) -> Seq(initialRelationship),
      (startNode.id, SemanticDirection.OUTGOING) -> Seq(leftRelationship1, leftRelationship2),
      (middleNode.id, SemanticDirection.INCOMING) -> Seq(leftRelationship1, leftRelationship2),
      (middleNode.id, SemanticDirection.OUTGOING) -> Seq(rightRelationship),
      (endNode.id, SemanticDirection.INCOMING) -> Seq(rightRelationship)
    )
    replyWithMap(query, nodeMapping.withDefaultValue(Seq.empty))
    val queryState = QueryStateHelper.emptyWith(query = query)

    val left = mock[Pipe]
    when(left.createResults(queryState)).thenAnswer(new Answer[Iterator[ExecutionContext]]() {
      def answer(invocation: InvocationOnMock): Iterator[ExecutionContext] = Iterator(row("a" -> firstNode))
    })

    val filteringStep = new VarLengthPredicate {

      override def filterNode(row: ExecutionContext,
                              state: QueryState)
                             (node: NodeValue): Boolean = true

      override def filterRelationship(row: ExecutionContext, state: QueryState)(rel: RelationshipValue): Boolean =
        rel.id() != 2

      override def predicateExpressions: Seq[Predicate] = Seq.empty
    }

    // when
    val result = VarLengthExpandPipe(left, "a", "r", "b", SemanticDirection.OUTGOING, SemanticDirection.OUTGOING,
                                     RelationshipTypes.empty, 3, None, nodeInScope = false, filteringStep)().createResults(queryState).toList

    // then
    val single :: Nil = result
    single.getByName("a") should beEquivalentTo(firstNode)
    single.getByName("r") should beEquivalentTo(List(initialRelationship, leftRelationship1, rightRelationship))
    single.getByName("b") should beEquivalentTo(endNode)
  }

  test("should correctly handle nulls from source pipe") {
    // given
    val query = mock[QueryContext]
    val queryState = QueryStateHelper.emptyWith(query = query)

    val source = mock[Pipe]
    when(source.createResults(queryState)).thenReturn(Iterator(row("a" -> Values.NO_VALUE)))

    // when
    val result = VarLengthExpandPipe(source, "a", "r", "b", SemanticDirection.BOTH, SemanticDirection.INCOMING, RelationshipTypes.empty, 1, None, nodeInScope = false)().createResults(queryState).toList

    // then
    val single :: Nil = result
    single.getByName("a").asInstanceOf[AnyRef] should be(Values.NO_VALUE)
    single.getByName("r").asInstanceOf[AnyRef] should be(Values.NO_VALUE)
    single.getByName("b").asInstanceOf[AnyRef] should be(Values.NO_VALUE)
  }

  test("should not overwrite expand into to-node on nulls from source pipe") {
    // given
    val query = mock[QueryContext]
    val queryState = QueryStateHelper.emptyWith(query = query)

    val source = mock[Pipe]
    val toNode = newMockedNode(1)
    when(source.createResults(queryState)).thenAnswer(new Answer[Iterator[ExecutionContext]] {
      def answer(invocation: InvocationOnMock): Iterator[ExecutionContext] = Iterator(row("a" -> Values.NO_VALUE, "b" -> toNode))
    })

    // when
    val result = VarLengthExpandPipe(source, "a", "r", "b", SemanticDirection.BOTH, SemanticDirection.INCOMING, RelationshipTypes.empty, 1, None, nodeInScope = true)().createResults(queryState).toList

    // then
    val single :: Nil = result
    single.getByName("a").asInstanceOf[AnyRef] should be(Values.NO_VALUE)
    single.getByName("r").asInstanceOf[AnyRef] should be(Values.NO_VALUE)
    single.getByName("b") should beEquivalentTo(toNode)
  }

  test("should register owning pipe") {
    val src = new FakePipe(Iterator.empty)
    val pred1 = True()
    val pred2 = True()
    val filteringStep = createVarLengthPredicate(pred1, pred2)
    val pipe = VarLengthExpandPipe(src, "a", "r", "b", SemanticDirection.OUTGOING, SemanticDirection.OUTGOING,
                                   RelationshipTypes.empty, 3, None, nodeInScope = false, filteringStep)()

    pipe.filteringStep.predicateExpressions.foreach(_.owningPipe should equal(pipe))
    pred1.owningPipe should equal(pipe)
    pred2.owningPipe should equal(pipe)
  }

  private def row(values: (String, AnyValue)*) = ExecutionContext.from(values: _*)

  private def newMockedNode(id: Int) = {
    val node = mock[NodeValue]
    when(node.id).thenReturn(id)
    node
  }

  private def newNamedMockedRelationship(id: Int, relName: String, startNode: NodeValue, endNode: NodeValue): RelationshipValue = {
    val rel = newMockedRelationship(id, startNode, endNode)
    when(rel.toString).thenReturn(relName)
    rel
  }

  private def newMockedRelationship(id: Int, startNode: NodeValue, endNode: NodeValue): RelationshipValue = {
    val relationship = mock[RelationshipValue]
    when(relationship.id()).thenReturn(id)
    when(relationship.startNode()).thenReturn(startNode)
    when(relationship.endNode()).thenReturn(endNode)
    when(relationship.otherNode(startNode)).thenReturn(endNode)
    when(relationship.otherNode(endNode)).thenReturn(startNode)
    relationship
  }

  private def replyWithMap(query: QueryContext, mapping: Map[(Long, _ <: SemanticDirection), Seq[RelationshipValue]]) {
    when(query.getRelationshipsForIds(any(), any(), any())).thenAnswer(new Answer[Iterator[RelationshipValue]] {
      def answer(invocation: InvocationOnMock): Iterator[RelationshipValue] = {
        val startNode :: dir :: _ :: Nil = invocation.getArguments.toList
        mapping(startNode.asInstanceOf[Long] -> dir.asInstanceOf[SemanticDirection]).iterator
      }
    })
  }

}

object VarLengthExpandPipeTest {
  def createVarLengthPredicate(nodePredicate: Predicate, relationshipPredicate: Predicate): VarLengthPredicate = {
    new VarLengthPredicate {
      override def filterNode(row: ExecutionContext, state: QueryState)(node: NodeValue): Boolean = {
        val cp = row.copyWith("to", node)
        val result = nodePredicate.isTrue(cp, state)
        result
      }

      override def filterRelationship(row: ExecutionContext, state: QueryState)(rel: RelationshipValue): Boolean = {
        val cp = row.copyWith("r", rel)
        val result = relationshipPredicate.isTrue(cp, state)
        result
      }

      override def predicateExpressions: Seq[Predicate] = Seq(nodePredicate, relationshipPredicate)
    }
  }
}
