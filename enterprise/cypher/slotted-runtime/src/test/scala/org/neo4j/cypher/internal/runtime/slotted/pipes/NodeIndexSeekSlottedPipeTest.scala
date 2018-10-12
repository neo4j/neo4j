/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.mockito.Mockito._
import org.neo4j.cypher.internal.compatibility.v3_5.runtime.SlotConfiguration.Size
import org.neo4j.cypher.internal.compatibility.v3_5.runtime.{SlotConfiguration, SlottedIndexedProperty}
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.{ListLiteral, Literal}
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{IndexMockingHelp, LockingUniqueIndexSeek}
import org.neo4j.cypher.internal.runtime.interpreted.{ExecutionContext, ImplicitDummyPos, QueryStateHelper}
import org.neo4j.cypher.internal.runtime.slotted.{SlottedExecutionContext, SlottedExecutionContextFactory}
import org.neo4j.cypher.internal.v3_5.logical.plans.{CompositeQueryExpression, IndexOrderNone, ManyQueryExpression}
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.NodeValue
import org.opencypher.v9_0.expressions.{LabelName, LabelToken, PropertyKeyName, PropertyKeyToken}
import org.opencypher.v9_0.util.symbols._
import org.opencypher.v9_0.util.test_helpers.CypherFunSuite
import org.opencypher.v9_0.util.{LabelId, PropertyKeyId}

class NodeIndexSeekSlottedPipeTest extends CypherFunSuite with ImplicitDummyPos with SlottedPipeTestHelper with IndexMockingHelp {

  private val label = LabelToken(LabelName("LabelName") _, LabelId(11))
  private val propertyKey = Seq(PropertyKeyToken(PropertyKeyName("PropertyName") _, PropertyKeyId(10)))
  override val propertyKeys = propertyKey :+ PropertyKeyToken(PropertyKeyName("prop2") _, PropertyKeyId(11))
  private val node = nodeValue(1)
  private val node2 = nodeValue(2)

  private def nodeValue(id: Long) = {
    val node = mock[NodeValue]
    when(node.id()).thenReturn(id)
    node
  }

  test("should only use argument columns of initial context") {
    // given
    val queryState = QueryStateHelper.emptyWith(
      query = indexFor[ExecutionContext]())

    // when
    val slots = SlotConfiguration.empty
                .newLong("n", nullable = false, CTNode)
                .newReference("r", nullable = false, CTAny)

    val initialContextSlots = slots.copy()
                              .newReference("other", nullable = true, CTAny)
    val initialContext = SlottedExecutionContext(initialContextSlots)

    // argument size should be smaller than slot configuration
    val argumentSize = Size(0,1)

    val properties = IndexedSeq(SlottedIndexedProperty(0, None))
    val pipe = NodeIndexSeekSlottedPipe("n", label,  properties, ManyQueryExpression(ListLiteral(
      Literal("hello"),
      Literal("bye")
    )),
      indexOrder = IndexOrderNone,
      slots = slots,
      argumentSize = argumentSize)()

    pipe.setExecutionContextFactory(SlottedExecutionContextFactory(slots))

    val result = pipe.createResults(queryState.withInitialContext(initialContext))

    // then
    val list: Iterator[ExecutionContext] = result
    testableResult(list, slots) should equal(List())
  }

  test("should use index provided values when available") {
    // given
    val queryState = QueryStateHelper.emptyWith(
      query = indexFor[ExecutionContext](
        Seq("hello") -> Seq(nodeValueHit(node, "hello")),
        Seq("bye") -> Seq(nodeValueHit(node2, "bye"))
      )
    )

    // when
    val slots = SlotConfiguration.empty.newLong("n", nullable = false, CTNode)
      .newReference("n." + propertyKey(0).name, nullable = false, CTAny)
    val properties = propertyKey.map(pk => SlottedIndexedProperty(pk.nameId.id, Some(slots.getReferenceOffsetFor("n." + pk.name)))).toArray
    val pipe = NodeIndexSeekSlottedPipe("n", label, properties, ManyQueryExpression(ListLiteral(
      Literal("hello"),
      Literal("bye")
    )),
      indexOrder = IndexOrderNone,
      slots = slots,
      argumentSize = slots.size())()
    val result = pipe.createResults(queryState)

    // then
    val list: Iterator[ExecutionContext] = result
    testableResult(list, slots) should equal(List(
      Map("n" -> node.id, "n." + propertyKey(0).name -> Values.stringValue("hello")),
      Map("n" -> node2.id, "n." + propertyKey(0).name -> Values.stringValue("bye"))
    ))
  }

  test("should use composite index provided values when available") {
    // given
    val queryState = QueryStateHelper.emptyWith(
      query = indexFor[ExecutionContext](
        Seq("hello", "world") -> Seq(nodeValueHit(node, "hello", "world")),
        Seq("bye", "cruel") -> Seq(nodeValueHit(node2, "bye", "cruel"))
      )
    )

    // when
    val slots = SlotConfiguration.empty.newLong("n", nullable = false, CTNode)
      .newReference("n." + propertyKeys(0).name, nullable = false, CTAny)
      .newReference("n." + propertyKeys(1).name, nullable = false, CTAny)
    val properties = propertyKeys.map(pk => SlottedIndexedProperty(pk.nameId.id, Some(slots.getReferenceOffsetFor("n." + pk.name)))).toArray
    val pipe = NodeIndexSeekSlottedPipe("n", label, properties,
      CompositeQueryExpression(Seq(
        ManyQueryExpression(ListLiteral(
          Literal("hello"), Literal("bye")
        )),
        ManyQueryExpression(ListLiteral(
          Literal("world"), Literal("cruel")
        )))),
        indexOrder = IndexOrderNone,
        slots = slots,
        argumentSize = slots.size())()
    val result = pipe.createResults(queryState)

    // then
    val list: Iterator[ExecutionContext] = result
    testableResult(list, slots) should equal(List(
      Map("n" -> node.id, "n." + propertyKeys(0).name -> Values.stringValue("hello"), "n." + propertyKeys(1).name -> Values.stringValue("world")),
      Map("n" -> node2.id, "n." + propertyKeys(0).name -> Values.stringValue("bye"), "n." + propertyKeys(1).name -> Values.stringValue("cruel"))
    ))
  }

  test("should use locking unique index provided values when available") {
    // given
    val queryState = QueryStateHelper.emptyWith(
      query = indexFor[ExecutionContext](
        Seq("hello") -> Seq(nodeValueHit(node, "hello")),
        Seq("world") -> Seq(nodeValueHit(node2, "bye"))
      )
    )

    // when
    val slots = SlotConfiguration.empty.newLong("n", nullable = false, CTNode)
      .newReference("n." + propertyKey(0).name, nullable = false, CTAny)
    val properties = propertyKey.map(pk => SlottedIndexedProperty(pk.nameId.id, Some(slots.getReferenceOffsetFor("n." + pk.name)))).toArray
    val pipe = NodeIndexSeekSlottedPipe("n", label, properties, ManyQueryExpression(ListLiteral(Literal("hello"), Literal("world"))), LockingUniqueIndexSeek,
      IndexOrderNone, slots, slots.size())()
    val result = pipe.createResults(queryState)

    // then
    val list: Iterator[ExecutionContext] = result
    testableResult(list, slots) should equal(List(
      Map("n" -> node.id, "n." + propertyKey(0).name -> Values.stringValue("hello")),
      Map("n" -> node2.id, "n." + propertyKey(0).name -> Values.stringValue("bye"))
    ))
  }
}
