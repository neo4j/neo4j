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

import org.neo4j.cypher.internal.compatibility.v3_5.runtime.SlotConfiguration
import org.neo4j.cypher.internal.runtime.vectorized._
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.VirtualValues
import org.opencypher.v9_0.util.test_helpers.CypherFunSuite

class ArgumentOperatorTest extends CypherFunSuite {

  test("should copy argument over and produce a single row") {
    // Given

    // input data
    val inputLongs = 3
    val inputRefs = 1
    val inputRows = 3
    val inputMorsel = new Morsel(
      Array[Long](1, 2, 3, 4, 5, 6, 7, 8, 9),
      Array[AnyValue](Values.stringValue("a"), Values.stringValue("b"), Values.stringValue("c")),
      inputRows)
    val inputRow = MorselExecutionContext(inputMorsel, inputLongs, inputRefs)

    // output data
    val outputLongs = 2
    val outputRefs = 2
    val outputMorsel = new Morsel(
      new Array[Long](outputLongs),
      new Array[AnyValue](outputRefs),
      1)
    val outputRow = MorselExecutionContext(outputMorsel, outputLongs, outputRefs)

    // operator and argument size
    val operator = new ArgumentOperator(SlotConfiguration.Size(1, 1))

    // When
    operator.init(null, null, inputRow).operate(outputRow, null, QueryState.EMPTY)

    // Then
    outputMorsel.longs should equal(Array(1, 0))
    outputMorsel.refs should equal(Array(Values.stringValue("a"), null))
    outputMorsel.validRows should equal(1)

    // And when
    inputRow.moveToNextRow()
    outputRow.resetToFirstRow()
    operator.init(null, null, inputRow).operate(outputRow, null, QueryState.EMPTY)

    // Then
    outputMorsel.longs should equal(Array(4, 0))
    outputMorsel.refs should equal(Array(Values.stringValue("b"), null))
    outputMorsel.validRows should equal(1)

    // And when
    inputRow.moveToNextRow()
    outputRow.resetToFirstRow()
    operator.init(null, null, inputRow).operate(outputRow, null, QueryState.EMPTY)

    // Then
    outputMorsel.longs should equal(Array(7, 0))
    outputMorsel.refs should equal(Array(Values.stringValue("c"), null))
    outputMorsel.validRows should equal(1)
  }

}
