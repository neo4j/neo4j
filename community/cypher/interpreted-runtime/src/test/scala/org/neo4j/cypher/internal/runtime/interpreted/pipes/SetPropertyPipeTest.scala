/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.mockito.ArgumentMatchers.any
import org.mockito.ArgumentMatchers.anyBoolean
import org.mockito.ArgumentMatchers.anyInt
import org.mockito.ArgumentMatchers.anyLong
import org.mockito.Mockito.never
import org.mockito.Mockito.verify
import org.mockito.Mockito.when
import org.neo4j.cypher.internal
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.UnPositionedVariable.varFor
import org.neo4j.cypher.internal.planner.spi.ReadTokenContext
import org.neo4j.cypher.internal.runtime.CypherRuntimeConfiguration
import org.neo4j.cypher.internal.runtime.ExpressionCursors
import org.neo4j.cypher.internal.runtime.NodeOperations
import org.neo4j.cypher.internal.runtime.NodeReadOperations
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.RelationshipOperations
import org.neo4j.cypher.internal.runtime.RelationshipReadOperations
import org.neo4j.cypher.internal.runtime.SelectivityTrackerRegistrator
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.CommunityExpressionConverter
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.ExpressionConverters
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.commands.values.KeyToken
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.DummyPosition
import org.neo4j.cypher.internal.util.PropertyKeyId
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.graphdb.Node
import org.neo4j.values.storable.Values
import org.neo4j.values.storable.Values.longValue
import org.neo4j.values.virtual.VirtualRelationshipValue

class SetPropertyPipeTest extends CypherFunSuite with PipeTestSupport {

  private val pos = DummyPosition(0)
  private val entity1 = varFor("x")
  private val entity2 = varFor("y")
  private val property1 = "prop"
  private val property2 = "prop2"
  private val propertyKey1 = PropertyKeyName(property1)(pos)
  private val propertyKey2 = PropertyKeyName(property2)(pos)
  private val literalInt1 = internal.expressions.SignedDecimalIntegerLiteral("1")(pos)

  implicit private val table: SemanticTable = new SemanticTable(
    resolvedPropertyKeyNames = Map(property1 -> PropertyKeyId(1), property2 -> PropertyKeyId(2))
  )

  private val state = mock[QueryState]
  private val qtx = mock[QueryContext]
  when(qtx.getOptPropertyKeyId(property1)).thenReturn(Some(1))
  when(qtx.getOptPropertyKeyId(property2)).thenReturn(Some(2))
  when(qtx.getOrCreatePropertyKeyIds(any())).thenReturn(Array[Int]())
  when(state.query).thenReturn(qtx)
  when(state.decorator).thenReturn(NullPipeDecorator)
  when(state.cursors).thenReturn(mock[ExpressionCursors])
  private val emptyExpression = mock[Expression]
  when(emptyExpression.children).thenReturn(Seq.empty)

  private val expressionConverter =
    new ExpressionConverters(CommunityExpressionConverter(
      ReadTokenContext.EMPTY,
      new AnonymousVariableNameGenerator(),
      new SelectivityTrackerRegistrator(),
      CypherRuntimeConfiguration.defaultConfiguration
    ))

  private def convertExpression(astExpression: internal.expressions.Expression): Expression = {
    def resolveTokens(expr: Expression, ctx: ReadTokenContext): Expression = expr match {
      case keyToken: KeyToken => keyToken.resolve(ctx)
      case _                  => expr
    }

    expressionConverter.toCommandExpression(Id.INVALID_ID, astExpression).rewrite(resolveTokens(_, qtx))
  }

  // match (n) set n.prop = n.prop + 1
  test("should grab an exclusive lock if the rhs reads from the same node property") {
    val mockedSource = newMockedPipe(row(entity1 -> newMockedNode(10)))
    val astRhs: internal.expressions.Expression = internal.expressions.Add(
      internal.expressions.Property(entity1, propertyKey1)(pos),
      literalInt1
    )(pos)
    val needsExclusiveLock = internal.expressions.Expression.hasPropertyReadDependency(entity1, astRhs, propertyKey1)
    val rhs = convertExpression(astRhs)
    val pipe =
      SetPipe(
        mockedSource,
        SetNodePropertyOperation(entity1.name, LazyPropertyKey(propertyKey1), rhs, needsExclusiveLock)
      )()

    val nodeReadOps = mock[NodeReadOperations]
    val nodeWriteOps = mock[NodeOperations]
    when(nodeReadOps.getProperty(10L, 1, null, null, throwOnDeleted = true)).thenReturn(longValue(13L))
    when(qtx.nodeReadOps).thenReturn(nodeReadOps)
    when(qtx.nodeWriteOps).thenReturn(nodeWriteOps)

    needsExclusiveLock shouldBe true

    pipe.createResults(state).toList
    verify(nodeWriteOps).acquireExclusiveLock(10)
    verify(nodeWriteOps).releaseExclusiveLock(10)
  }

  // match ()-[r]-() set r.prop = r.prop + 1
  test("should grab an exclusive lock if the rhs reads from the same relationship property") {
    val mockedSource = newMockedPipe(row(entity1 -> newMockedRelationship(10, mock[Node], mock[Node])))
    val astRhs: internal.expressions.Expression = internal.expressions.Add(
      internal.expressions.Property(entity1, propertyKey1)(pos),
      literalInt1
    )(pos)
    val needsExclusiveLock = internal.expressions.Expression.hasPropertyReadDependency(entity1, astRhs, propertyKey1)
    val rhs = convertExpression(astRhs)
    val pipe = SetPipe(
      mockedSource,
      SetRelationshipPropertyOperation(entity1.name, LazyPropertyKey(propertyKey1), rhs, needsExclusiveLock)
    )()

    val relReadOps = mock[RelationshipReadOperations]
    val relWriteOps = mock[RelationshipOperations]
    when(qtx.relationshipReadOps).thenReturn(relReadOps)
    when(qtx.relationshipWriteOps).thenReturn(relWriteOps)
    when(relReadOps.getProperty(any[VirtualRelationshipValue](), anyInt(), any(), any(), anyBoolean())).thenReturn(
      Values.NO_VALUE
    )

    needsExclusiveLock shouldBe true

    pipe.createResults(state).toList
    verify(relWriteOps).acquireExclusiveLock(10)
    verify(relWriteOps).releaseExclusiveLock(10)
  }

  // match (n) set n.prop2 = n.prop + 1
  test("should not grab an exclusive lock if the rhs reads from another node property") {
    val mockedSource = newMockedPipe(row(entity1 -> newMockedNode(10)))
    val astRhs: internal.expressions.Expression = internal.expressions.Add(
      internal.expressions.Property(entity1, propertyKey1)(pos),
      literalInt1
    )(pos)
    val needsExclusiveLock = internal.expressions.Expression.hasPropertyReadDependency(entity1, astRhs, propertyKey2)
    val rhs = convertExpression(astRhs)
    val pipe =
      SetPipe(
        mockedSource,
        SetNodePropertyOperation(entity1.name, LazyPropertyKey(propertyKey2), rhs, needsExclusiveLock)
      )()

    val nodeReadOps = mock[NodeReadOperations]
    val nodeWriteOps = mock[NodeOperations]
    when(nodeReadOps.getProperty(10L, 1, null, null, throwOnDeleted = true)).thenReturn(longValue(13L))
    when(qtx.nodeReadOps).thenReturn(nodeReadOps)
    when(qtx.nodeWriteOps).thenReturn(nodeWriteOps)

    needsExclusiveLock shouldBe false

    pipe.createResults(state).toList
    verify(nodeWriteOps, never()).acquireExclusiveLock(10)
    verify(nodeWriteOps, never()).releaseExclusiveLock(10)
  }

  // match ()-[r]-() set r.prop2 = r.prop + 1
  test("should not grab an exclusive lock if the rhs reads from another relationship property") {
    val mockedSource = newMockedPipe(row(entity1 -> newMockedRelationship(10, mock[Node], mock[Node])))

    val astRhs: internal.expressions.Expression = internal.expressions.Add(
      internal.expressions.Property(entity1, propertyKey1)(pos),
      internal.expressions.SignedDecimalIntegerLiteral("1")(pos)
    )(pos)
    val needsExclusiveLock = internal.expressions.Expression.hasPropertyReadDependency(entity1, astRhs, propertyKey2)
    val rhs = convertExpression(astRhs)
    val pipe = SetPipe(
      mockedSource,
      SetRelationshipPropertyOperation(entity1.name, LazyPropertyKey(propertyKey2), rhs, needsExclusiveLock)
    )()

    val relReadOps = mock[RelationshipReadOperations]
    val relWriteOps = mock[RelationshipOperations]
    when(qtx.relationshipReadOps).thenReturn(relReadOps)
    when(qtx.relationshipWriteOps).thenReturn(relWriteOps)
    when(relReadOps.getProperty(any[VirtualRelationshipValue](), anyInt(), any(), any(), anyBoolean())).thenReturn(
      Values.NO_VALUE
    )

    needsExclusiveLock shouldBe false

    pipe.createResults(state).toList
    verify(relWriteOps, never()).acquireExclusiveLock(10)
    verify(relWriteOps, never()).releaseExclusiveLock(10)
  }

  // match (n), (n2) set n2.prop = n.prop + 1
  test("should not grab an exclusive lock if the rhs reads from the same node property on another node") {
    val mockedSource = newMockedPipe(row(entity1 -> newMockedNode(10), entity2 -> newMockedNode(20)))
    val astRhs: internal.expressions.Expression = internal.expressions.Add(
      internal.expressions.Property(entity1, propertyKey1)(pos),
      internal.expressions.SignedDecimalIntegerLiteral("1")(pos)
    )(pos)
    val needsExclusiveLock = internal.expressions.Expression.hasPropertyReadDependency(entity2, astRhs, propertyKey1)
    val rhs = convertExpression(astRhs)
    val pipe =
      SetPipe(
        mockedSource,
        SetNodePropertyOperation(entity2.name, LazyPropertyKey(propertyKey1), rhs, needsExclusiveLock)
      )()

    val nodeReadOps = mock[NodeReadOperations]
    val nodeWriteOps = mock[NodeOperations]
    when(nodeReadOps.getProperty(10L, 1, null, null, throwOnDeleted = true)).thenReturn(longValue(13L))
    when(qtx.nodeReadOps).thenReturn(nodeReadOps)
    when(qtx.nodeWriteOps).thenReturn(nodeWriteOps)

    needsExclusiveLock shouldBe false

    pipe.createResults(state).toList
    verify(nodeWriteOps, never()).acquireExclusiveLock(10)
    verify(nodeWriteOps, never()).releaseExclusiveLock(10)
    verify(nodeWriteOps, never()).acquireExclusiveLock(20)
    verify(nodeWriteOps, never()).releaseExclusiveLock(20)
  }

  // match ()-[r]-(), ()-[r2]-() set r2.prop = r.prop + 1
  test(
    "should not grab an exclusive lock if the rhs reads from the same relationship property on another relationship"
  ) {
    val mockedSource = newMockedPipe(row(
      entity1 -> newMockedRelationship(10, mock[Node], mock[Node]),
      entity2 -> newMockedRelationship(20, mock[Node], mock[Node])
    ))
    val astRhs: internal.expressions.Expression = internal.expressions.Add(
      internal.expressions.Property(entity1, propertyKey1)(pos),
      internal.expressions.SignedDecimalIntegerLiteral("1")(pos)
    )(pos)
    val needsExclusiveLock = internal.expressions.Expression.hasPropertyReadDependency(entity2, astRhs, propertyKey1)
    val rhs = convertExpression(astRhs)
    val pipe = SetPipe(
      mockedSource,
      SetRelationshipPropertyOperation(entity2.name, LazyPropertyKey(propertyKey1), rhs, needsExclusiveLock)
    )()

    val relReadOps = mock[RelationshipReadOperations]
    val relWriteOps = mock[RelationshipOperations]
    when(qtx.relationshipReadOps).thenReturn(relReadOps)
    when(qtx.relationshipWriteOps).thenReturn(relWriteOps)

    when(relReadOps.getProperty(any[VirtualRelationshipValue](), anyInt(), any(), any(), anyBoolean())).thenReturn(
      Values.NO_VALUE
    )

    needsExclusiveLock shouldBe false

    pipe.createResults(state).toList
    verify(relWriteOps, never()).acquireExclusiveLock(10)
    verify(relWriteOps, never()).releaseExclusiveLock(10)
    verify(relWriteOps, never()).acquireExclusiveLock(20)
    verify(relWriteOps, never()).releaseExclusiveLock(20)
  }

  // match n set n += { prop: n.prop + 1 }
  test("should grab an exclusive lock when setting node props from a map with dependencies") {
    val mockedSource = newMockedPipe(row(entity1 -> newMockedNode(10)))

    val innerAst = internal.expressions.Add(
      internal.expressions.Property(entity1, propertyKey1)(pos),
      internal.expressions.SignedDecimalIntegerLiteral("1")(pos)
    )(pos)
    val astRhs = internal.expressions.MapExpression(Seq(propertyKey1 -> innerAst))(pos)
    val needsExclusiveLock = internal.expressions.Expression.mapExpressionHasPropertyReadDependency(entity1, astRhs)
    val rhs = convertExpression(astRhs)
    val pipe = SetPipe(
      mockedSource,
      SetNodePropertyFromMapOperation(entity1.name, rhs, removeOtherProps = false, needsExclusiveLock)
    )()

    val nodeReadOps = mock[NodeReadOperations]
    val nodeWriteOps = mock[NodeOperations]
    when(nodeReadOps.getProperty(10L, 1, null, null, throwOnDeleted = true)).thenReturn(longValue(13L))
    when(qtx.nodeReadOps).thenReturn(nodeReadOps)
    when(qtx.nodeWriteOps).thenReturn(nodeWriteOps)
    when(qtx.getOptPropertyKeyId(property1)).thenReturn(None)
    when(nodeReadOps.propertyKeyIds(10, null, null)).thenReturn(Array.empty[Int])

    needsExclusiveLock shouldBe true

    pipe.createResults(state).toList
    verify(nodeWriteOps).acquireExclusiveLock(10)
    verify(nodeWriteOps).releaseExclusiveLock(10)
  }

  // match ()-[r]-() set r = { prop: r.prop + 1 }
  test("should grab an exclusive lock when setting rel props from a map with dependencies") {
    val mockedSource = newMockedPipe(row(entity1 -> newMockedRelationship(10, mock[Node], mock[Node])))
    val innerAst = internal.expressions.Add(
      internal.expressions.Property(entity1, propertyKey1)(pos),
      internal.expressions.SignedDecimalIntegerLiteral("1")(pos)
    )(pos)
    val astRhs = internal.expressions.MapExpression(Seq(propertyKey1 -> innerAst))(pos)
    val needsExclusiveLock = internal.expressions.Expression.mapExpressionHasPropertyReadDependency(entity1, astRhs)
    val rhs = convertExpression(astRhs)

    val pipe = SetPipe(
      mockedSource,
      SetRelationshipPropertyFromMapOperation(entity1.name, rhs, removeOtherProps = true, needsExclusiveLock)
    )()

    val relReadOps = mock[RelationshipReadOperations]
    val relWriteOps = mock[RelationshipOperations]
    when(qtx.relationshipReadOps).thenReturn(relReadOps)
    when(qtx.relationshipWriteOps).thenReturn(relWriteOps)
    when(qtx.getOptPropertyKeyId(property1)).thenReturn(None)
    when(relWriteOps.propertyKeyIds(10, null, null)).thenReturn(Array.empty[Int]) // <--- O_O
    when(relReadOps.getProperty(anyLong(), anyInt(), any(), any(), anyBoolean())).thenReturn(Values.NO_VALUE)

    needsExclusiveLock shouldBe true

    pipe.createResults(state).toList
    verify(relWriteOps).acquireExclusiveLock(10)
    verify(relWriteOps).releaseExclusiveLock(10)
  }

  // match (n), (n2) set n += { prop: n2.prop + 1 }
  test("should not grab an exclusive lock when setting node props from a map with same prop but other node") {
    val mockedSource = newMockedPipe(row(entity1 -> newMockedNode(10), entity2 -> newMockedNode(20)))

    val innerAst = internal.expressions.Add(
      internal.expressions.Property(entity2, propertyKey1)(pos),
      internal.expressions.SignedDecimalIntegerLiteral("1")(pos)
    )(pos)
    val astRhs = internal.expressions.MapExpression(Seq(propertyKey1 -> innerAst))(pos)
    val needsExclusiveLock = internal.expressions.Expression.mapExpressionHasPropertyReadDependency(entity1, astRhs)
    val rhs = convertExpression(astRhs)
    val pipe = SetPipe(
      mockedSource,
      SetNodePropertyFromMapOperation(entity1.name, rhs, removeOtherProps = false, needsExclusiveLock)
    )()

    val nodeReadOps = mock[NodeReadOperations]
    val nodeWriteOps = mock[NodeOperations]
    when(nodeReadOps.getProperty(10L, 1, null, null, throwOnDeleted = true)).thenReturn(longValue(13L))
    when(nodeReadOps.getProperty(20L, 1, null, null, throwOnDeleted = true)).thenReturn(longValue(13L))
    when(qtx.nodeReadOps).thenReturn(nodeReadOps)
    when(qtx.nodeWriteOps).thenReturn(nodeWriteOps)
    when(qtx.getOptPropertyKeyId(property1)).thenReturn(None)
    when(nodeReadOps.propertyKeyIds(10, null, null)).thenReturn(Array.empty[Int])

    needsExclusiveLock shouldBe false

    pipe.createResults(state).toList
    verify(nodeWriteOps, never()).acquireExclusiveLock(10)
    verify(nodeWriteOps, never()).releaseExclusiveLock(10)
    verify(nodeWriteOps, never()).acquireExclusiveLock(20)
    verify(nodeWriteOps, never()).releaseExclusiveLock(20)

  }

  // match ()-[r]-(), ()-[r2]-() set r = { prop: r2.prop + 1 }
  test("should not grab an exclusive lock when setting rel props from a map with same prop but other rel") {
    val mockedSource = newMockedPipe(row(
      entity1 -> newMockedRelationship(10, mock[Node], mock[Node]),
      entity2 -> newMockedRelationship(20, mock[Node], mock[Node])
    ))
    val innerAst = internal.expressions.Add(
      internal.expressions.Property(entity2, propertyKey1)(pos),
      internal.expressions.SignedDecimalIntegerLiteral("1")(pos)
    )(pos)
    val astRhs = internal.expressions.MapExpression(Seq(propertyKey1 -> innerAst))(pos)
    val needsExclusiveLock = internal.expressions.Expression.mapExpressionHasPropertyReadDependency(entity1, astRhs)
    val rhs = convertExpression(astRhs)
    val pipe = SetPipe(
      mockedSource,
      SetRelationshipPropertyFromMapOperation(entity1.name, rhs, removeOtherProps = true, needsExclusiveLock)
    )()

    val relReadOps = mock[RelationshipReadOperations]
    val relWriteOps = mock[RelationshipOperations]
    when(qtx.relationshipReadOps).thenReturn(relReadOps)
    when(qtx.relationshipWriteOps).thenReturn(relWriteOps)
    when(qtx.getOptPropertyKeyId(property1)).thenReturn(None)
    when(relWriteOps.propertyKeyIds(10, null, null)).thenReturn(Array.empty[Int]) // <--- O_O
    when(relReadOps.getProperty(anyLong(), anyInt(), any(), any(), anyBoolean())).thenReturn(Values.NO_VALUE)

    needsExclusiveLock shouldBe false

    pipe.createResults(state).toList
    verify(relWriteOps, never()).acquireExclusiveLock(10)
    verify(relWriteOps, never()).releaseExclusiveLock(10)
    verify(relWriteOps, never()).acquireExclusiveLock(20)
    verify(relWriteOps, never()).releaseExclusiveLock(20)
  }

  // match (n) set n += { prop: n.prop2 + 1 }
  test("should not grab an exclusive lock when setting node props from a map with same node but other prop") {
    val mockedSource = newMockedPipe(row(entity1 -> newMockedNode(10)))

    val innerAst = internal.expressions.Add(
      internal.expressions.Property(entity1, propertyKey2)(pos),
      internal.expressions.SignedDecimalIntegerLiteral("1")(pos)
    )(pos)
    val astRhs = internal.expressions.MapExpression(Seq(propertyKey1 -> innerAst))(pos)
    val needsExclusiveLock = internal.expressions.Expression.mapExpressionHasPropertyReadDependency(entity1, astRhs)
    val rhs = convertExpression(astRhs)

    val pipe = SetPipe(
      mockedSource,
      SetNodePropertyFromMapOperation(entity1.name, rhs, removeOtherProps = false, needsExclusiveLock)
    )()

    val nodeReadOps = mock[NodeReadOperations]
    val nodeWriteOps = mock[NodeOperations]
    when(nodeReadOps.getProperty(10L, 2, null, null, throwOnDeleted = true)).thenReturn(longValue(13L))
    when(qtx.nodeReadOps).thenReturn(nodeReadOps)
    when(qtx.nodeWriteOps).thenReturn(nodeWriteOps)
    when(qtx.getOptPropertyKeyId(property1)).thenReturn(None)
    when(nodeReadOps.propertyKeyIds(10, null, null)).thenReturn(Array.empty[Int])

    needsExclusiveLock shouldBe false

    pipe.createResults(state).toList
    verify(nodeWriteOps, never()).acquireExclusiveLock(10)
    verify(nodeWriteOps, never()).releaseExclusiveLock(10)

  }

  // match ()-[r]-() set r = { prop: r.prop2 + 1 }
  test("should not grab an exclusive lock when setting rel props from a map with same rel but other prop") {
    val mockedSource = newMockedPipe(row(entity1 -> newMockedRelationship(10, mock[Node], mock[Node])))

    val innerAst = internal.expressions.Add(
      internal.expressions.Property(entity1, propertyKey2)(pos),
      internal.expressions.SignedDecimalIntegerLiteral("1")(pos)
    )(pos)
    val astRhs = internal.expressions.MapExpression(Seq(propertyKey1 -> innerAst))(pos)
    val needsExclusiveLock = internal.expressions.Expression.mapExpressionHasPropertyReadDependency(entity1, astRhs)
    val rhs = convertExpression(astRhs)
    val pipe = SetPipe(
      mockedSource,
      SetRelationshipPropertyFromMapOperation(entity1.name, rhs, removeOtherProps = true, needsExclusiveLock)
    )()

    val relReadOps = mock[RelationshipReadOperations]
    val relWriteOps = mock[RelationshipOperations]
    when(qtx.relationshipReadOps).thenReturn(relReadOps)
    when(qtx.relationshipWriteOps).thenReturn(relWriteOps)
    when(qtx.getOptPropertyKeyId(property1)).thenReturn(None)
    when(relWriteOps.propertyKeyIds(10, null, null)).thenReturn(Array.empty[Int]) // <--- O_O
    when(relReadOps.getProperty(any[VirtualRelationshipValue](), anyInt(), any(), any(), anyBoolean())).thenReturn(
      Values.NO_VALUE
    )

    needsExclusiveLock shouldBe false

    pipe.createResults(state).toList
    verify(relWriteOps, never()).acquireExclusiveLock(10)
    verify(relWriteOps, never()).releaseExclusiveLock(10)
  }

}
