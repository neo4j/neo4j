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

import org.mockito.Mockito._
import org.neo4j.cypher.internal.compatibility.v3_5.runtime.SlotConfiguration
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.vectorized._
import org.neo4j.internal.kernel.api.NodeCursor
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.VirtualValues
import org.opencypher.v9_0.util.test_helpers.CypherFunSuite

class AllNodeScanOperatorTest extends CypherFunSuite {

  test("should copy argument over for every row") {
    // Given

    // input data
    val inputLongs = 3
    val inputRefs = 3
    val inputRows = 2
    val inputMorsel = new Morsel(
      Array[Long](1, 2, 3,
                  4, 5, 6),
      Array[AnyValue](Values.stringValue("a"), Values.stringValue("b"), Values.stringValue("c"),
                      Values.stringValue("d"), Values.stringValue("e"), Values.stringValue("f")),
      inputRows)
    val inputRow = MorselExecutionContext(inputMorsel, inputLongs, inputRefs)

    // output data (that can fit everything)
    val outputLongs = 3
    val outputRefs = 2
    val outputRows = 5
    val outputMorsel = new Morsel(
      new Array[Long](outputLongs * outputRows),
      new Array[AnyValue](outputRefs * outputRows),
      outputRows)
    val outputRow = MorselExecutionContext(outputMorsel, outputLongs, outputRefs)

    // operator and argument size
    val operator = new AllNodeScanOperator(2, SlotConfiguration.Size(2, 2))

    // mock cursor
    val context = mock[QueryContext](RETURNS_DEEP_STUBS)
    val cursor1 = mock[NodeCursor]
    val cursor2 = mock[NodeCursor]
    when(cursor1.next()).thenReturn(true, true, true, true, true, false)
    when(cursor2.next()).thenReturn(true, true, true, true, true, false)
    when(cursor1.nodeReference()).thenReturn(10, 11, 12, 13, 14)
    when(cursor2.nodeReference()).thenReturn(10, 11, 12, 13, 14)
    when(context.transactionalContext.cursors.allocateNodeCursor()).thenReturn(cursor1, cursor2)

    // When
    operator.operate(StartLeafLoop(Iteration(Some(inputRow))), outputRow, context, QueryState(VirtualValues.EMPTY_MAP, null))

    // Then
    outputMorsel.longs should equal(Array(
      1, 2, 10,
      1, 2, 11,
      1, 2, 12,
      1, 2, 13,
      1, 2, 14))
    outputMorsel.refs should equal(Array(
      Values.stringValue("a"), Values.stringValue("b"),
      Values.stringValue("a"), Values.stringValue("b"),
      Values.stringValue("a"), Values.stringValue("b"),
      Values.stringValue("a"), Values.stringValue("b"),
      Values.stringValue("a"), Values.stringValue("b")))
    outputMorsel.validRows should equal(5)

    // And when
    inputRow.moveToNextRow()
    outputRow.resetToFirstRow()
    operator.operate(StartLeafLoop(Iteration(Some(inputRow))), outputRow, context, QueryState(VirtualValues.EMPTY_MAP, null))

    // Then
    outputMorsel.longs should equal(Array(
      4, 5, 10,
      4, 5, 11,
      4, 5, 12,
      4, 5, 13,
      4, 5, 14))
    outputMorsel.refs should equal(Array(
      Values.stringValue("d"), Values.stringValue("e"),
      Values.stringValue("d"), Values.stringValue("e"),
      Values.stringValue("d"), Values.stringValue("e"),
      Values.stringValue("d"), Values.stringValue("e"),
      Values.stringValue("d"), Values.stringValue("e")))
    outputMorsel.validRows should equal(5)
  }

}
