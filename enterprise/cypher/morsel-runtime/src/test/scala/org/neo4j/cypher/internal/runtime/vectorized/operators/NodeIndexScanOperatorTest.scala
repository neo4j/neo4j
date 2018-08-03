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

import org.mockito.Mockito.{RETURNS_DEEP_STUBS, when}
import org.neo4j.cypher.internal.compatibility.v3_5.runtime.SlotConfiguration
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.interpreted.ImplicitDummyPos
import org.neo4j.cypher.internal.runtime.vectorized.{Morsel, MorselExecutionContext, QueryState}
import org.neo4j.internal.kernel.api.helpers.StubNodeValueIndexCursor
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.{Value, Values}
import org.neo4j.values.virtual.NodeValue
import org.opencypher.v9_0.expressions.{LabelName, LabelToken, PropertyKeyName, PropertyKeyToken}
import org.opencypher.v9_0.util.symbols.{CTAny, CTNode}
import org.opencypher.v9_0.util.test_helpers.CypherFunSuite
import org.opencypher.v9_0.util.{LabelId, PropertyKeyId}

import scala.collection.JavaConverters._

class NodeIndexScanOperatorTest extends CypherFunSuite with ImplicitDummyPos {

  private val label = LabelToken(LabelName("LabelName") _, LabelId(11))
  private val propertyKey = PropertyKeyToken(PropertyKeyName("PropertyName") _, PropertyKeyId(10))
  private val node = nodeValue(11)

  private def nodeValue(id: Long) = {
    val node = mock[NodeValue]
    when(node.id()).thenReturn(id)
    node
  }

  test("should use index provided values when available") {
    // given
    val queryContext = scanFor(Seq((node.id, Seq(Values.stringValue("hello")))))

    // input data
    val inputMorsel = new Morsel(new Array[Long](0), new Array[AnyValue](0), 0)
    val inputRow = MorselExecutionContext(inputMorsel, 0, 0)

    // output data
    val numberOfLongs = 1
    val numberOfReferences = 1
    val outputRows = 1
    val outputMorsel = new Morsel(
      new Array[Long](numberOfLongs * outputRows),
      new Array[AnyValue](numberOfReferences * outputRows),
      outputRows)
    val outputRow = MorselExecutionContext(outputMorsel, numberOfLongs, numberOfReferences)

    val nDotProp = "n." + propertyKey.name
    val slots = SlotConfiguration.empty.newLong("n", nullable = false, CTNode)
      .newReference(nDotProp, nullable = false, CTAny)
    val operator = new NodeIndexScanOperator(slots.getLongOffsetFor("n"), label.nameId.id, propertyKey.nameId.id,
      Some(slots.getReferenceOffsetFor(nDotProp)), slots.size())

    // When
    operator.init(queryContext, QueryState.EMPTY, inputRow).operate(outputRow, queryContext, QueryState.EMPTY)

    // then
    outputMorsel.longs should equal(Array(
      node.id))
    outputMorsel.refs should equal(Array(
      Values.stringValue("hello")))
    outputMorsel.validRows should equal(1)
  }

  private def scanFor(results: Iterable[(Long, Seq[Value])]): QueryContext = {
    val context = mock[QueryContext](RETURNS_DEEP_STUBS)

   val jIterator =  results.map { case (l,vs) =>  org.neo4j.helpers.collection.Pair.of(new java.lang.Long(l), vs.asJava)}.iterator.asJava

    val cursor = new StubNodeValueIndexCursor(jIterator)
    when(context.transactionalContext.cursors.allocateNodeValueIndexCursor).thenReturn(cursor)
    context
  }

}
