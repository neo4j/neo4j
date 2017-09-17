/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.pipes

import org.mockito.ArgumentMatchers.{anyInt, anyLong}
import org.mockito.Mockito._
import org.neo4j.cypher.internal.apa.v3_4.InputPosition
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.commands.expressions._
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.commands.values.{KeyToken, TokenType}
import org.neo4j.cypher.internal.apa.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.frontend.v3_4.PropertyKeyId
import org.neo4j.cypher.internal.frontend.v3_4.semantics.SemanticTable
import org.neo4j.cypher.internal.spi.v3_4.{Operations, QueryContext}
import org.neo4j.cypher.internal.v3_4.expressions.PropertyKeyName
import org.neo4j.graphdb.{Node, Relationship}
import org.neo4j.values.storable.Values
import org.neo4j.values.storable.Values.longValue

class SetPropertyPipeTest extends CypherFunSuite with PipeTestSupport {

  implicit val table = new SemanticTable()
  table.resolvedPropertyKeyNames.put("prop", PropertyKeyId(1))
  table.resolvedPropertyKeyNames.put("prop2", PropertyKeyId(2))

  val state = mock[QueryState]
  val qtx = mock[QueryContext]
  when(state.query).thenReturn(qtx)
  when(state.decorator).thenReturn(NullPipeDecorator)
  val emptyExpression = mock[Expression]
  when(emptyExpression.children).thenReturn(Seq.empty)

  // match (n) set n.prop = n.prop + 1
  test("should grab an exclusive lock if the rhs reads from the same node property") {
    val rhs = Add(Property(Variable("n"), KeyToken.Resolved("prop", 1, TokenType.PropertyKey)), Literal(1))
    val mockedSource = newMockedPipe("n", row("n" -> newMockedNode(10)))
    val pipe = SetPipe(mockedSource, SetNodePropertyOperation("n", LazyPropertyKey(PropertyKeyName("prop")(InputPosition.NONE)), rhs))()

    val nodeOps = mock[Operations[Node]]
    when(nodeOps.getProperty(10L, 1)).thenReturn(longValue(13L))
    when(qtx.nodeOps).thenReturn(nodeOps)

    pipe.createResults(state).toVector
    verify(nodeOps).acquireExclusiveLock(10)
    verify(nodeOps).releaseExclusiveLock(10)
  }

  // match ()-[r]-() set r.prop = r.prop + 1
  test("should grab an exclusive lock if the rhs reads from the same relationship property") {
    val rhs = Add(Property(Variable("r"), KeyToken.Resolved("prop", 1, TokenType.PropertyKey)), Literal(1))
    val mockedSource = newMockedPipe("r", row("r" -> newMockedRelationship(10, mock[Node], mock[Node])))
    val pipe = SetPipe(mockedSource, SetRelationshipPropertyOperation("r", LazyPropertyKey(PropertyKeyName("prop")(InputPosition.NONE)), rhs))()

    val relOps = mock[Operations[Relationship]]
    when(qtx.relationshipOps).thenReturn(relOps)
    when(relOps.getProperty(anyLong(), anyInt())).thenReturn(Values.NO_VALUE)

    pipe.createResults(state).toVector
    verify(relOps).acquireExclusiveLock(10)
    verify(relOps).releaseExclusiveLock(10)
  }

  // match (n) set n.prop2 = n.prop + 1
  test("should not grab an exclusive lock if the rhs reads from another node property") {
    val rhs = Add(Property(Variable("n"), KeyToken.Resolved("prop", 1, TokenType.PropertyKey)), Literal(1))
    val mockedSource = newMockedPipe("n", row("n" -> newMockedNode(10)))
    val pipe = SetPipe(mockedSource, SetNodePropertyOperation("n", LazyPropertyKey(PropertyKeyName("prop2")(InputPosition.NONE)), rhs))()

    val nodeOps = mock[Operations[Node]]
    when(nodeOps.getProperty(10L, 1)).thenReturn(longValue(13L))
    when(qtx.nodeOps).thenReturn(nodeOps)

    pipe.createResults(state).toVector
    verify(nodeOps, never()).acquireExclusiveLock(10)
    verify(nodeOps, never()).releaseExclusiveLock(10)
  }

  // match ()-[r]-() set r.prop2 = r.prop + 1
  test("should not grab an exclusive lock if the rhs reads from another relationship property") {
    val rhs = Add(Property(Variable("r"), KeyToken.Resolved("prop", 1, TokenType.PropertyKey)), Literal(1))
    val mockedSource = newMockedPipe("r", row("r" -> newMockedRelationship(10, mock[Node], mock[Node])))
    val pipe = SetPipe(mockedSource, SetRelationshipPropertyOperation("r", LazyPropertyKey(PropertyKeyName("prop2")(InputPosition.NONE)), rhs))()

    val relOps = mock[Operations[Relationship]]
    when(qtx.relationshipOps).thenReturn(relOps)
    when(relOps.getProperty(anyLong(), anyInt())).thenReturn(Values.NO_VALUE)

    pipe.createResults(state).toVector
    verify(relOps, never()).acquireExclusiveLock(10)
    verify(relOps, never()).releaseExclusiveLock(10)
  }

  // match (n), (n2) set n2.prop = n.prop + 1
  test("should not grab an exclusive lock if the rhs reads from the same node property on another node") {
    val rhs = Add(Property(Variable("n"), KeyToken.Resolved("prop", 1, TokenType.PropertyKey)), Literal(1))
    val mockedSource = newMockedPipe("n", row("n" -> newMockedNode(10), "n2" -> newMockedNode(20)))
    val pipe = SetPipe(mockedSource, SetNodePropertyOperation("n2", LazyPropertyKey(PropertyKeyName("prop")(InputPosition.NONE)), rhs))()

    val nodeOps = mock[Operations[Node]]
    when(nodeOps.getProperty(10L, 1)).thenReturn(longValue(13L))
    when(qtx.nodeOps).thenReturn(nodeOps)

    pipe.createResults(state).toVector
    verify(nodeOps, never()).acquireExclusiveLock(10)
    verify(nodeOps, never()).releaseExclusiveLock(10)
    verify(nodeOps, never()).acquireExclusiveLock(20)
    verify(nodeOps, never()).releaseExclusiveLock(20)
  }

  // match ()-[r]-(), ()-[r2]-() set r2.prop = r.prop + 1
  test("should not grab an exclusive lock if the rhs reads from the same relationship property on another relationship") {
    val rhs = Add(Property(Variable("r"), KeyToken.Resolved("prop", 1, TokenType.PropertyKey)), Literal(1))
    val mockedSource = newMockedPipe("r", row("r" -> newMockedRelationship(10, mock[Node], mock[Node]),
                                              "r2" -> newMockedRelationship(20, mock[Node], mock[Node])))
    val pipe = SetPipe(mockedSource, SetRelationshipPropertyOperation("r2", LazyPropertyKey(PropertyKeyName("prop")(InputPosition.NONE)), rhs))()

    val relOps = mock[Operations[Relationship]]
    when(qtx.relationshipOps).thenReturn(relOps)
    when(relOps.getProperty(anyLong(), anyInt())).thenReturn(Values.NO_VALUE)

    pipe.createResults(state).toVector
    verify(relOps, never()).acquireExclusiveLock(10)
    verify(relOps, never()).releaseExclusiveLock(10)
    verify(relOps, never()).acquireExclusiveLock(20)
    verify(relOps, never()).releaseExclusiveLock(20)
  }

  // match n set n += { prop: n.prop + 1 }
  test("should grab an exclusive lock when setting node props from a map with dependencies") {
    val rhs = LiteralMap(Map("prop" -> Add(Property(Variable("n"), KeyToken.Resolved("prop", 1, TokenType.PropertyKey)), Literal(1))))
    val mockedSource = newMockedPipe("n", row("n" -> newMockedNode(10)))
    val pipe = SetPipe(mockedSource, SetNodePropertyFromMapOperation("n", rhs, removeOtherProps = false))()

    val nodeOps = mock[Operations[Node]]
    when(nodeOps.getProperty(10L, 1)).thenReturn(longValue(13L))
    when(qtx.nodeOps).thenReturn(nodeOps)
    when(qtx.getOptPropertyKeyId("prop")).thenReturn(None)
    when(nodeOps.propertyKeyIds(10)).thenReturn(Iterator.empty)

    pipe.createResults(state).toVector
    verify(nodeOps).acquireExclusiveLock(10)
    verify(nodeOps).releaseExclusiveLock(10)
  }

  // match ()-[r]-() set r = { prop: r.prop + 1 }
  test("should grab an exclusive lock when setting rel props from a map with dependencies") {
    val rhs = LiteralMap(Map("prop" -> Add(Property(Variable("r"), KeyToken.Resolved("prop", 1, TokenType.PropertyKey)), Literal(1))))
    val mockedSource = newMockedPipe("r", row("r" -> newMockedRelationship(10, mock[Node], mock[Node])))
    val pipe = SetPipe(mockedSource, SetRelationshipPropertyFromMapOperation("r", rhs, removeOtherProps = true))()

    val relOps = mock[Operations[Relationship]]
    when(qtx.relationshipOps).thenReturn(relOps)
    when(qtx.getOptPropertyKeyId("prop")).thenReturn(None)
    when(relOps.propertyKeyIds(10)).thenReturn(Iterator.empty)
    when(relOps.getProperty(anyLong(), anyInt())).thenReturn(Values.NO_VALUE)

    pipe.createResults(state).toVector
    verify(relOps).acquireExclusiveLock(10)
    verify(relOps).releaseExclusiveLock(10)
  }

  // match (n), (n2) set n += { prop: n2.prop + 1 }
  test("should not grab an exclusive lock when setting node props from a map with same prop but other node") {
    val rhs = LiteralMap(Map("prop" -> Add(Property(Variable("n2"), KeyToken.Resolved("prop", 1, TokenType.PropertyKey)), Literal(1))))
    val mockedSource = newMockedPipe("n", row("n" -> newMockedNode(10), "n2" -> newMockedNode(20)))
    val pipe = SetPipe(mockedSource, SetNodePropertyFromMapOperation("n", rhs, removeOtherProps = false))()

    val nodeOps = mock[Operations[Node]]
    when(nodeOps.getProperty(10L, 1)).thenReturn(longValue(13L))
    when(nodeOps.getProperty(20L, 1)).thenReturn(longValue(13L))
    when(qtx.nodeOps).thenReturn(nodeOps)
    when(qtx.getOptPropertyKeyId("prop")).thenReturn(None)
    when(nodeOps.propertyKeyIds(10)).thenReturn(Iterator.empty)

    pipe.createResults(state).toVector
    verify(nodeOps, never()).acquireExclusiveLock(10)
    verify(nodeOps, never()).releaseExclusiveLock(10)
    verify(nodeOps, never()).acquireExclusiveLock(20)
    verify(nodeOps, never()).releaseExclusiveLock(20)

  }

  // match ()-[r]-(), ()-[r2]-() set r = { prop: r2.prop + 1 }
  test("should not grab an exclusive lock when setting rel props from a map with same prop but other rel") {
    val rhs = LiteralMap(Map("prop" -> Add(Property(Variable("r2"), KeyToken.Resolved("prop", 1, TokenType.PropertyKey)), Literal(1))))
    val mockedSource = newMockedPipe("r", row("r" -> newMockedRelationship(10, mock[Node], mock[Node]),
                                              "r2" -> newMockedRelationship(20, mock[Node], mock[Node])))
    val pipe = SetPipe(mockedSource, SetRelationshipPropertyFromMapOperation("r", rhs, removeOtherProps = true))()

    val relOps = mock[Operations[Relationship]]
    when(qtx.relationshipOps).thenReturn(relOps)
    when(qtx.getOptPropertyKeyId("prop")).thenReturn(None)
    when(relOps.propertyKeyIds(10)).thenReturn(Iterator.empty)
    when(relOps.getProperty(anyLong(), anyInt())).thenReturn(Values.NO_VALUE)

    pipe.createResults(state).toVector
    verify(relOps, never()).acquireExclusiveLock(10)
    verify(relOps, never()).releaseExclusiveLock(10)
    verify(relOps, never()).acquireExclusiveLock(20)
    verify(relOps, never()).releaseExclusiveLock(20)
  }

  // match (n) set n += { prop: n.prop2 + 1 }
  test("should not grab an exclusive lock when setting node props from a map with same node but other prop") {
    val rhs = LiteralMap(Map("prop" -> Add(Property(Variable("n"), KeyToken.Resolved("prop2", 2, TokenType.PropertyKey)), Literal(1))))
    val mockedSource = newMockedPipe("n", row("n" -> newMockedNode(10)))
    val pipe = SetPipe(mockedSource, SetNodePropertyFromMapOperation("n", rhs, removeOtherProps = false))()

    val nodeOps = mock[Operations[Node]]
    when(nodeOps.getProperty(10L, 2)).thenReturn(longValue(13L))
    when(qtx.nodeOps).thenReturn(nodeOps)
    when(qtx.getOptPropertyKeyId("prop")).thenReturn(None)
    when(nodeOps.propertyKeyIds(10)).thenReturn(Iterator.empty)

    pipe.createResults(state).toVector
    verify(nodeOps, never()).acquireExclusiveLock(10)
    verify(nodeOps, never()).releaseExclusiveLock(10)

  }

  // match ()-[r]-() set r = { prop: r.prop2 + 1 }
  test("should not grab an exclusive lock when setting rel props from a map with same rel but other prop") {
    val rhs = LiteralMap(Map("prop" -> Add(Property(Variable("r"), KeyToken.Resolved("prop2", 2, TokenType.PropertyKey)), Literal(1))))
    val mockedSource = newMockedPipe("r", row("r" -> newMockedRelationship(10, mock[Node], mock[Node])))
    val pipe = SetPipe(mockedSource, SetRelationshipPropertyFromMapOperation("r", rhs, removeOtherProps = true))()

    val relOps = mock[Operations[Relationship]]
    when(qtx.relationshipOps).thenReturn(relOps)
    when(qtx.getOptPropertyKeyId("prop")).thenReturn(None)
    when(relOps.propertyKeyIds(10)).thenReturn(Iterator.empty)
    when(relOps.getProperty(anyLong(), anyInt())).thenReturn(Values.NO_VALUE)

    pipe.createResults(state).toVector
    verify(relOps, never()).acquireExclusiveLock(10)
    verify(relOps, never()).releaseExclusiveLock(10)
  }

}
