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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.helpers

import org.mockito.Mockito.verifyZeroInteractions
import org.neo4j.cypher.internal.frontend.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.result.QueryResult.{QueryResultVisitor, Record}
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values.{intValue, stringValue}

import scala.collection.mutable.ArrayBuffer

class RowIteratorVisitationTest extends CypherFunSuite {

  val javaValues = new RuntimeJavaValueConverter(_ => false)
  import javaValues.feedIteratorToVisitable

  test("should convert non-empty iterator to visitor") {
    // Given
    val input = List[Array[AnyValue]](Array(stringValue("1"), intValue(2)), Array(stringValue("11"), intValue(22)))
    val recordingVisitor = RecordingResultVisitor("a", "b")()

    // When
    feedIteratorToVisitable(input.iterator).accept(recordingVisitor)

    // Then
    recordingVisitor.recorded.toList should equal(List("a" -> stringValue("1"), "b" -> intValue(2), "a" -> stringValue("11"), "b" -> intValue(22)))
  }

  test("should stop when visitor asks to stop") {
    // Given
    val rowsToAccept = 2
    val input = List[Array[AnyValue]](Array(stringValue("1")), Array(stringValue("2")), Array(stringValue("3")))
    val recordingVisitor = RecordingResultVisitor("a")(rowsToAccept)

    // When
    feedIteratorToVisitable(input.iterator).accept(recordingVisitor)

    // Then
    recordingVisitor.recorded.toList should equal(List("a" -> stringValue("1"), "a" -> stringValue("2")))
  }

  test("should accept nothing if iterator is empty") {
    // Given
    val visitor = mock[QueryResultVisitor[RuntimeException]]

    // When
    feedIteratorToVisitable(Iterator.empty).accept(visitor)

    // Then
    verifyZeroInteractions(visitor)
  }

  private case class RecordingResultVisitor(columns: String*)(rowsToAccept: Int = Int.MaxValue) extends QueryResultVisitor[RuntimeException] {

    require(rowsToAccept >= 0)

    val recorded = new ArrayBuffer[(String, AnyValue)]()

    def visit(row: Record) = {
      recorded ++= columns.zip(row.fields())
      recorded.size != rowsToAccept
    }
  }
}
