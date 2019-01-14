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

import org.mockito.ArgumentMatchers.{anyInt, anyLong}
import org.mockito.Mockito._
import org.neo4j.cypher.internal.frontend.v3_4.semantics.SemanticTable
import org.neo4j.cypher.internal.planner.v3_4.spi.TokenContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.{CommunityExpressionConverter, ExpressionConverters}
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions._
import org.neo4j.cypher.internal.runtime.interpreted.commands.values.KeyToken
import org.neo4j.cypher.internal.runtime.{Operations, QueryContext}
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.v3_4.{DummyPosition, PropertyKeyId}
import org.neo4j.cypher.internal.v3_4.expressions.PropertyKeyName
import org.neo4j.cypher.internal.v3_4.{expressions => ast}
import org.neo4j.graphdb.{Node, Relationship}
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions._
import org.neo4j.cypher.internal.runtime.interpreted.commands.values.{KeyToken, TokenType}
import org.neo4j.cypher.internal.runtime.{Operations, QueryContext}
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.v3_4.{InputPosition, PropertyKeyId}
import org.neo4j.cypher.internal.v3_4.expressions.PropertyKeyName
import org.neo4j.graphdb.Node
import org.neo4j.values.storable.Values
import org.neo4j.values.storable.Values.longValue
import org.neo4j.values.virtual.{RelationshipValue, NodeValue}

class SetPropertyPipeTest extends CypherFunSuite with PipeTestSupport {
  private val pos = DummyPosition(0)
  private val entity1 = "x"
  private val entity2 = "y"
  private val property1 = "prop"
  private val property2 = "prop2"
  private val propertyKey1 = PropertyKeyName(property1)(pos)
  private val propertyKey2 = PropertyKeyName(property2)(pos)
  private val literalInt1 = ast.SignedDecimalIntegerLiteral("1")(pos)

  private implicit val table = new SemanticTable()
  table.resolvedPropertyKeyNames.put(property1, PropertyKeyId(1))
  table.resolvedPropertyKeyNames.put(property2, PropertyKeyId(2))

  private val state = mock[QueryState]
  private val qtx = mock[QueryContext]
  when(qtx.getOptPropertyKeyId(property1)).thenReturn(Some(1))
  when(qtx.getOptPropertyKeyId(property2)).thenReturn(Some(2))
  when(state.query).thenReturn(qtx)
  when(state.decorator).thenReturn(NullPipeDecorator)
  private val emptyExpression = mock[Expression]
  when(emptyExpression.children).thenReturn(Seq.empty)

  private val expressionConverter = new ExpressionConverters(CommunityExpressionConverter)
  private def convertExpression(astExpression: ast.Expression): Expression = {
    def resolveTokens(expr: Expression, ctx: TokenContext): Expression = expr match {
      case (keyToken: KeyToken) => keyToken.resolve(ctx)
      case _ => expr
    }

    expressionConverter.toCommandExpression(astExpression).rewrite(resolveTokens(_, qtx))
  }

  // match (n) set n.prop = n.prop + 1
  test("should grab an exclusive lock if the rhs reads from the same node property") {
    val mockedSource = newMockedPipe(entity1, row(entity1 -> newMockedNode(10)))
    val astRhs: ast.Expression = ast.Add(ast.Property(ast.Variable(entity1)(pos), propertyKey1)(pos), literalInt1)(pos)
    val needsExclusiveLock = ast.Expression.hasPropertyReadDependency(entity1, astRhs, propertyKey1)
    val rhs = convertExpression(astRhs)
    val pipe = SetPipe(mockedSource, SetNodePropertyOperation(entity1, LazyPropertyKey(propertyKey1), rhs, needsExclusiveLock))()

    val nodeOps = mock[Operations[NodeValue]]
    when(nodeOps.getProperty(10L, 1)).thenReturn(longValue(13L))
    when(qtx.nodeOps).thenReturn(nodeOps)

    needsExclusiveLock shouldBe true

    pipe.createResults(state).toVector
    verify(nodeOps).acquireExclusiveLock(10)
    verify(nodeOps).releaseExclusiveLock(10)
  }

  // match ()-[r]-() set r.prop = r.prop + 1
  test("should grab an exclusive lock if the rhs reads from the same relationship property") {
    val mockedSource = newMockedPipe(entity1, row(entity1 -> newMockedRelationship(10, mock[Node], mock[Node])))
    val astRhs: ast.Expression = ast.Add(ast.Property(ast.Variable(entity1)(pos), propertyKey1)(pos), literalInt1)(pos)
    val needsExclusiveLock = ast.Expression.hasPropertyReadDependency(entity1, astRhs, propertyKey1)
    val rhs = convertExpression(astRhs)
    val pipe = SetPipe(mockedSource,
      SetRelationshipPropertyOperation(entity1, LazyPropertyKey(propertyKey1), rhs, needsExclusiveLock))()

    val relOps = mock[Operations[RelationshipValue]]
    when(qtx.relationshipOps).thenReturn(relOps)
    when(relOps.getProperty(anyLong(), anyInt())).thenReturn(Values.NO_VALUE)

    needsExclusiveLock shouldBe true

    pipe.createResults(state).toVector
    verify(relOps).acquireExclusiveLock(10)
    verify(relOps).releaseExclusiveLock(10)
  }

  // match (n) set n.prop2 = n.prop + 1
  test("should not grab an exclusive lock if the rhs reads from another node property") {
    val mockedSource = newMockedPipe(entity1, row(entity1 -> newMockedNode(10)))
    val astRhs: ast.Expression = ast.Add(ast.Property(ast.Variable(entity1)(pos), propertyKey1)(pos), literalInt1)(pos)
    val needsExclusiveLock = ast.Expression.hasPropertyReadDependency(entity1, astRhs, propertyKey2)
    val rhs = convertExpression(astRhs)
    val pipe = SetPipe(mockedSource,
      SetNodePropertyOperation(entity1, LazyPropertyKey(propertyKey2), rhs, needsExclusiveLock))()

    val nodeOps = mock[Operations[NodeValue]]
    when(nodeOps.getProperty(10L, 1)).thenReturn(longValue(13L))
    when(qtx.nodeOps).thenReturn(nodeOps)

    needsExclusiveLock shouldBe false

    pipe.createResults(state).toVector
    verify(nodeOps, never()).acquireExclusiveLock(10)
    verify(nodeOps, never()).releaseExclusiveLock(10)
  }

  // match ()-[r]-() set r.prop2 = r.prop + 1
  test("should not grab an exclusive lock if the rhs reads from another relationship property") {
    val mockedSource = newMockedPipe(entity1, row(entity1 -> newMockedRelationship(10, mock[Node], mock[Node])))

    val astRhs: ast.Expression = ast.Add(ast.Property(ast.Variable(entity1)(pos), propertyKey1)(pos), ast.SignedDecimalIntegerLiteral("1")(pos))(pos)
    val needsExclusiveLock = ast.Expression.hasPropertyReadDependency(entity1, astRhs, propertyKey2)
    val rhs = convertExpression(astRhs)
    val pipe = SetPipe(mockedSource,
      SetRelationshipPropertyOperation(entity1, LazyPropertyKey(propertyKey2), rhs, needsExclusiveLock))()

    val relOps = mock[Operations[RelationshipValue]]
    when(qtx.relationshipOps).thenReturn(relOps)
    when(relOps.getProperty(anyLong(), anyInt())).thenReturn(Values.NO_VALUE)

    needsExclusiveLock shouldBe false

    pipe.createResults(state).toVector
    verify(relOps, never()).acquireExclusiveLock(10)
    verify(relOps, never()).releaseExclusiveLock(10)
  }

  // match (n), (n2) set n2.prop = n.prop + 1
  test("should not grab an exclusive lock if the rhs reads from the same node property on another node") {
    val mockedSource = newMockedPipe(entity1, row(entity1 -> newMockedNode(10), entity2 -> newMockedNode(20)))
    val astRhs: ast.Expression = ast.Add(ast.Property(ast.Variable(entity1)(pos), propertyKey1)(pos), ast.SignedDecimalIntegerLiteral("1")(pos))(pos)
    val needsExclusiveLock = ast.Expression.hasPropertyReadDependency(entity2, astRhs, propertyKey1)
    val rhs = convertExpression(astRhs)
    val pipe = SetPipe(mockedSource,
      SetNodePropertyOperation(entity2, LazyPropertyKey(propertyKey1), rhs, needsExclusiveLock))()

    val nodeOps = mock[Operations[NodeValue]]
    when(nodeOps.getProperty(10L, 1)).thenReturn(longValue(13L))
    when(qtx.nodeOps).thenReturn(nodeOps)

    needsExclusiveLock shouldBe false

    pipe.createResults(state).toVector
    verify(nodeOps, never()).acquireExclusiveLock(10)
    verify(nodeOps, never()).releaseExclusiveLock(10)
    verify(nodeOps, never()).acquireExclusiveLock(20)
    verify(nodeOps, never()).releaseExclusiveLock(20)
  }

  // match ()-[r]-(), ()-[r2]-() set r2.prop = r.prop + 1
  test("should not grab an exclusive lock if the rhs reads from the same relationship property on another relationship") {
    val mockedSource = newMockedPipe(entity1, row(entity1 -> newMockedRelationship(10, mock[Node], mock[Node]),
                                                  entity2 -> newMockedRelationship(20, mock[Node], mock[Node])))
    val astRhs: ast.Expression = ast.Add(ast.Property(ast.Variable(entity1)(pos), propertyKey1)(pos), ast.SignedDecimalIntegerLiteral("1")(pos))(pos)
    val needsExclusiveLock = ast.Expression.hasPropertyReadDependency(entity2, astRhs, propertyKey1)
    val rhs = convertExpression(astRhs)
    val pipe = SetPipe(mockedSource,
      SetRelationshipPropertyOperation(entity2, LazyPropertyKey(propertyKey1), rhs, needsExclusiveLock))()

    val relOps = mock[Operations[RelationshipValue]]
    when(qtx.relationshipOps).thenReturn(relOps)
    when(relOps.getProperty(anyLong(), anyInt())).thenReturn(Values.NO_VALUE)

    needsExclusiveLock shouldBe false

    pipe.createResults(state).toVector
    verify(relOps, never()).acquireExclusiveLock(10)
    verify(relOps, never()).releaseExclusiveLock(10)
    verify(relOps, never()).acquireExclusiveLock(20)
    verify(relOps, never()).releaseExclusiveLock(20)
  }

  // match n set n += { prop: n.prop + 1 }
  test("should grab an exclusive lock when setting node props from a map with dependencies") {
    val mockedSource = newMockedPipe(entity1, row(entity1 -> newMockedNode(10)))

    val innerAst = ast.Add(ast.Property(ast.Variable(entity1)(pos), propertyKey1)(pos), ast.SignedDecimalIntegerLiteral("1")(pos))(pos)
    val astRhs = ast.MapExpression(Seq(propertyKey1 -> innerAst))(pos)
    val needsExclusiveLock = ast.Expression.mapExpressionHasPropertyReadDependency(entity1, astRhs)
    val rhs = convertExpression(astRhs)
    val pipe = SetPipe(mockedSource, SetNodePropertyFromMapOperation(entity1, rhs, removeOtherProps = false, needsExclusiveLock))()

    val nodeOps = mock[Operations[NodeValue]]
    when(nodeOps.getProperty(10L, 1)).thenReturn(longValue(13L))
    when(qtx.nodeOps).thenReturn(nodeOps)
    when(qtx.getOptPropertyKeyId(property1)).thenReturn(None)
    when(nodeOps.propertyKeyIds(10)).thenReturn(Iterator.empty)

    needsExclusiveLock shouldBe true

    pipe.createResults(state).toVector
    verify(nodeOps).acquireExclusiveLock(10)
    verify(nodeOps).releaseExclusiveLock(10)
  }

  // match ()-[r]-() set r = { prop: r.prop + 1 }
  test("should grab an exclusive lock when setting rel props from a map with dependencies") {
    val mockedSource = newMockedPipe(entity1, row(entity1 -> newMockedRelationship(10, mock[Node], mock[Node])))
    val innerAst = ast.Add(ast.Property(ast.Variable(entity1)(pos), propertyKey1)(pos), ast.SignedDecimalIntegerLiteral("1")(pos))(pos)
    val astRhs = ast.MapExpression(Seq(propertyKey1 -> innerAst))(pos)
    val needsExclusiveLock = ast.Expression.mapExpressionHasPropertyReadDependency(entity1, astRhs)
    val rhs = convertExpression(astRhs)

    val pipe = SetPipe(mockedSource,
      SetRelationshipPropertyFromMapOperation(entity1, rhs, removeOtherProps = true, needsExclusiveLock))()

    val relOps = mock[Operations[RelationshipValue]]
    when(qtx.relationshipOps).thenReturn(relOps)
    when(qtx.getOptPropertyKeyId(property1)).thenReturn(None)
    when(relOps.propertyKeyIds(10)).thenReturn(Iterator.empty)
    when(relOps.getProperty(anyLong(), anyInt())).thenReturn(Values.NO_VALUE)

    needsExclusiveLock shouldBe true

    pipe.createResults(state).toVector
    verify(relOps).acquireExclusiveLock(10)
    verify(relOps).releaseExclusiveLock(10)
  }

  // match (n), (n2) set n += { prop: n2.prop + 1 }
  test("should not grab an exclusive lock when setting node props from a map with same prop but other node") {
    val mockedSource = newMockedPipe(entity1, row(entity1 -> newMockedNode(10), entity2 -> newMockedNode(20)))

    val innerAst = ast.Add(ast.Property(ast.Variable(entity2)(pos), propertyKey1)(pos), ast.SignedDecimalIntegerLiteral("1")(pos))(pos)
    val astRhs = ast.MapExpression(Seq(propertyKey1 -> innerAst))(pos)
    val needsExclusiveLock = ast.Expression.mapExpressionHasPropertyReadDependency(entity1, astRhs)
    val rhs = convertExpression(astRhs)
    val pipe = SetPipe(mockedSource,
      SetNodePropertyFromMapOperation(entity1, rhs, removeOtherProps = false, needsExclusiveLock))()

    val nodeOps = mock[Operations[NodeValue]]
    when(nodeOps.getProperty(10L, 1)).thenReturn(longValue(13L))
    when(nodeOps.getProperty(20L, 1)).thenReturn(longValue(13L))
    when(qtx.nodeOps).thenReturn(nodeOps)
    when(qtx.getOptPropertyKeyId(property1)).thenReturn(None)
    when(nodeOps.propertyKeyIds(10)).thenReturn(Iterator.empty)

    needsExclusiveLock shouldBe false

    pipe.createResults(state).toVector
    verify(nodeOps, never()).acquireExclusiveLock(10)
    verify(nodeOps, never()).releaseExclusiveLock(10)
    verify(nodeOps, never()).acquireExclusiveLock(20)
    verify(nodeOps, never()).releaseExclusiveLock(20)

  }

  // match ()-[r]-(), ()-[r2]-() set r = { prop: r2.prop + 1 }
  test("should not grab an exclusive lock when setting rel props from a map with same prop but other rel") {
    val mockedSource = newMockedPipe(entity1, row(entity1 -> newMockedRelationship(10, mock[Node], mock[Node]),
                                                  entity2 -> newMockedRelationship(20, mock[Node], mock[Node])))
    val innerAst = ast.Add(ast.Property(ast.Variable(entity2)(pos), propertyKey1)(pos), ast.SignedDecimalIntegerLiteral("1")(pos))(pos)
    val astRhs = ast.MapExpression(Seq(propertyKey1 -> innerAst))(pos)
    val needsExclusiveLock = ast.Expression.mapExpressionHasPropertyReadDependency(entity1, astRhs)
    val rhs = convertExpression(astRhs)
    val pipe = SetPipe(mockedSource,
      SetRelationshipPropertyFromMapOperation(entity1, rhs, removeOtherProps = true, needsExclusiveLock))()

    val relOps = mock[Operations[RelationshipValue]]
    when(qtx.relationshipOps).thenReturn(relOps)
    when(qtx.getOptPropertyKeyId(property1)).thenReturn(None)
    when(relOps.propertyKeyIds(10)).thenReturn(Iterator.empty)
    when(relOps.getProperty(anyLong(), anyInt())).thenReturn(Values.NO_VALUE)

    needsExclusiveLock shouldBe false

    pipe.createResults(state).toVector
    verify(relOps, never()).acquireExclusiveLock(10)
    verify(relOps, never()).releaseExclusiveLock(10)
    verify(relOps, never()).acquireExclusiveLock(20)
    verify(relOps, never()).releaseExclusiveLock(20)
  }

  // match (n) set n += { prop: n.prop2 + 1 }
  test("should not grab an exclusive lock when setting node props from a map with same node but other prop") {
    val mockedSource = newMockedPipe(entity1, row(entity1 -> newMockedNode(10)))

    val innerAst = ast.Add(ast.Property(ast.Variable(entity1)(pos), propertyKey2)(pos), ast.SignedDecimalIntegerLiteral("1")(pos))(pos)
    val astRhs = ast.MapExpression(Seq(propertyKey1 -> innerAst))(pos)
    val needsExclusiveLock = ast.Expression.mapExpressionHasPropertyReadDependency(entity1, astRhs)
    val rhs = convertExpression(astRhs)

    val pipe = SetPipe(mockedSource,
      SetNodePropertyFromMapOperation(entity1, rhs, removeOtherProps = false, needsExclusiveLock))()

    val nodeOps = mock[Operations[NodeValue]]
    when(nodeOps.getProperty(10L, 2)).thenReturn(longValue(13L))
    when(qtx.nodeOps).thenReturn(nodeOps)
    when(qtx.getOptPropertyKeyId(property1)).thenReturn(None)
    when(nodeOps.propertyKeyIds(10)).thenReturn(Iterator.empty)

    needsExclusiveLock shouldBe false

    pipe.createResults(state).toVector
    verify(nodeOps, never()).acquireExclusiveLock(10)
    verify(nodeOps, never()).releaseExclusiveLock(10)

  }

  // match ()-[r]-() set r = { prop: r.prop2 + 1 }
  test("should not grab an exclusive lock when setting rel props from a map with same rel but other prop") {
    val mockedSource = newMockedPipe(entity1, row(entity1 -> newMockedRelationship(10, mock[Node], mock[Node])))

    val innerAst = ast.Add(ast.Property(ast.Variable(entity1)(pos), propertyKey2)(pos), ast.SignedDecimalIntegerLiteral("1")(pos))(pos)
    val astRhs = ast.MapExpression(Seq(propertyKey1 -> innerAst))(pos)
    val needsExclusiveLock = ast.Expression.mapExpressionHasPropertyReadDependency(entity1, astRhs)
    val rhs = convertExpression(astRhs)
    val pipe = SetPipe(mockedSource,
      SetRelationshipPropertyFromMapOperation(entity1, rhs, removeOtherProps = true, needsExclusiveLock))()

    val relOps = mock[Operations[RelationshipValue]]
    when(qtx.relationshipOps).thenReturn(relOps)
    when(qtx.getOptPropertyKeyId(property1)).thenReturn(None)
    when(relOps.propertyKeyIds(10)).thenReturn(Iterator.empty)
    when(relOps.getProperty(anyLong(), anyInt())).thenReturn(Values.NO_VALUE)

    needsExclusiveLock shouldBe false

    pipe.createResults(state).toVector
    verify(relOps, never()).acquireExclusiveLock(10)
    verify(relOps, never()).releaseExclusiveLock(10)
  }

}
