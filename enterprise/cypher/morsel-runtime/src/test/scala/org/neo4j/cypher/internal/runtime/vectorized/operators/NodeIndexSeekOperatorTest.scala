/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.cypher.internal.runtime.vectorized.operators

import org.mockito.Mockito.when
import org.neo4j.cypher.internal.compatibility.v3_5.runtime.SlotConfiguration
import org.neo4j.cypher.internal.runtime.interpreted.ImplicitDummyPos
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.{ListLiteral, Literal}
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{IndexMockingHelp, LockingUniqueIndexSeek}
import org.neo4j.cypher.internal.runtime.slotted.pipes.SlottedIndexedProperty
import org.neo4j.cypher.internal.runtime.vectorized.{Morsel, MorselExecutionContext, QueryState}
import org.neo4j.cypher.internal.v3_5.logical.plans.{CompositeQueryExpression, ManyQueryExpression}
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.NodeValue
import org.opencypher.v9_0.expressions.{LabelName, LabelToken, PropertyKeyName, PropertyKeyToken}
import org.opencypher.v9_0.util.symbols.{CTAny, CTNode}
import org.opencypher.v9_0.util.test_helpers.CypherFunSuite
import org.opencypher.v9_0.util.{LabelId, PropertyKeyId}

class NodeIndexSeekOperatorTest extends CypherFunSuite with ImplicitDummyPos with IndexMockingHelp {

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

  test("should use index provided values when available") {
    // given
    val queryContext = indexFor[NodeWithValues](
      Seq("hello") -> Seq(nodeValueHit(node, "hello")),
      Seq("bye") -> Seq(nodeValueHit(node2, "bye"))
    )

    // input data
    val inputMorsel = new Morsel(new Array[Long](0), new Array[AnyValue](0), 0)
    val inputRow = MorselExecutionContext(inputMorsel, 0, 0)

    // output data
    val numberOfLongs = 1
    val numberOfReferences = 1
    val outputRows = 2
    val outputMorsel = new Morsel(
      new Array[Long](numberOfLongs * outputRows),
      new Array[AnyValue](numberOfReferences * outputRows),
      outputRows)
    val outputRow = MorselExecutionContext(outputMorsel, numberOfLongs, numberOfReferences)

    // operator
    val slots = SlotConfiguration.empty.newLong("n", nullable = false, CTNode)
      .newReference("n." + propertyKey(0).name, nullable = false, CTAny)
    val properties = propertyKey.map(pk => SlottedIndexedProperty(pk.nameId.id, Some(slots.getReferenceOffsetFor("n." + pk.name)))).toArray
    val operator = new NodeIndexSeekOperator(slots.getLongOffsetFor("n"), label, properties, slots.size(),
      ManyQueryExpression(ListLiteral(
        Literal("hello"),
        Literal("bye")
      ))
    )

    // When
    operator.init(queryContext, QueryState.EMPTY, inputRow).operate(outputRow, queryContext, QueryState.EMPTY)

    // then
    outputMorsel.longs should equal(Array(
      node.id, node2.id))
    outputMorsel.refs should equal(Array(
      Values.stringValue("hello"), Values.stringValue("bye")))
    outputMorsel.validRows should equal(2)

  }

  test("should use composite index provided values when available") {
    // given
    val queryContext = indexFor[NodeWithValues](
      Seq("hello", "world") -> Seq(nodeValueHit(node, "hello", "world")),
      Seq("bye", "cruel") -> Seq(nodeValueHit(node2, "bye", "cruel"))
    )

    // input data
    val inputMorsel = new Morsel(new Array[Long](0), new Array[AnyValue](0), 0)
    val inputRow = MorselExecutionContext(inputMorsel, 0, 0)

    // output data
    val numberOfLongs = 1
    val numberOfReferences = 2
    val outputRows = 2
    val outputMorsel = new Morsel(
      new Array[Long](numberOfLongs * outputRows),
      new Array[AnyValue](numberOfReferences * outputRows),
      outputRows)
    val outputRow = MorselExecutionContext(outputMorsel, numberOfLongs, numberOfReferences)

    val slots = SlotConfiguration.empty.newLong("n", nullable = false, CTNode)
      .newReference("n." + propertyKeys(0).name, nullable = false, CTAny)
      .newReference("n." + propertyKeys(1).name, nullable = false, CTAny)
    val properties = propertyKeys.map(pk => SlottedIndexedProperty(pk.nameId.id, Some(slots.getReferenceOffsetFor("n." + pk.name)))).toArray
    val operator = new NodeIndexSeekOperator(slots.getLongOffsetFor("n"), label, properties, slots.size(),
      CompositeQueryExpression(Seq(
        ManyQueryExpression(ListLiteral(
          Literal("hello"), Literal("bye")
        )),
        ManyQueryExpression(ListLiteral(
          Literal("world"), Literal("cruel")
        ))))
    )
    // When
    operator.init(queryContext, QueryState.EMPTY, inputRow).operate(outputRow, queryContext, QueryState.EMPTY)

    // then
    outputMorsel.longs should equal(Array(
      node.id,
      node2.id))
    outputMorsel.refs should equal(Array(
      Values.stringValue("hello"), Values.stringValue("world"),
      Values.stringValue("bye"), Values.stringValue("cruel")))
    outputMorsel.validRows should equal(2)
  }

  test("should use locking unique index provided values when available") {
    // given
    val queryContext = indexFor[NodeWithValues](
        Seq("hello") -> Seq(nodeValueHit(node, "hello")),
        Seq("world") -> Seq(nodeValueHit(node2, "bye"))
    )

    // input data
    val inputMorsel = new Morsel(new Array[Long](0), new Array[AnyValue](0), 0)
    val inputRow = MorselExecutionContext(inputMorsel, 0, 0)

    // output data
    val numberOfLongs = 1
    val numberOfReferences = 1
    val outputRows = 2
    val outputMorsel = new Morsel(
      new Array[Long](numberOfLongs * outputRows),
      new Array[AnyValue](numberOfReferences * outputRows),
      outputRows)
    val outputRow = MorselExecutionContext(outputMorsel, numberOfLongs, numberOfReferences)

    val slots = SlotConfiguration.empty.newLong("n", nullable = false, CTNode)
      .newReference("n." + propertyKey(0).name, nullable = false, CTAny)
    val properties = propertyKey.map(pk => SlottedIndexedProperty(pk.nameId.id, Some(slots.getReferenceOffsetFor("n." + pk.name)))).toArray

    val operator = new NodeIndexSeekOperator(slots.getLongOffsetFor("n"), label, properties, slots.size(),
      ManyQueryExpression(ListLiteral(Literal("hello"), Literal("world"))), LockingUniqueIndexSeek)

    // When
    operator.init(queryContext, QueryState.EMPTY, inputRow).operate(outputRow, queryContext, QueryState.EMPTY)


    // then
    outputMorsel.longs should equal(Array(
      node.id,
      node2.id))
    outputMorsel.refs should equal(Array(
      Values.stringValue("hello"), Values.stringValue("bye")))
    outputMorsel.validRows should equal(2)
  }
}
