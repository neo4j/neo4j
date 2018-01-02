/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.helpers

import org.mockito.Mockito.verifyZeroInteractions
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite
import org.neo4j.graphdb.Result.{ResultRow, ResultVisitor}

import scala.collection.mutable.ArrayBuffer

class iteratorToVisitableTest extends CypherFunSuite {

  test("should convert non-empty iterator to visitor") {
    // Given
    val input = List[Map[String, Any]](Map("a" -> "1", "b" -> 2), Map("a" -> "11", "b" -> 22))
    val recordingVisitor = RecordingResultVisitor("a", "b")()

    // When
    iteratorToVisitable.accept(input.iterator, recordingVisitor)

    // Then
    recordingVisitor.recorded should equal(input.flatMap(_.toList))
  }

  test("should stop when visitor asks to stop") {
    // Given
    val rowsToAccept = 2
    val input = List(Map("a" -> "1"), Map("a" -> "2"), Map("a" -> "3"))
    val recordingVisitor = RecordingResultVisitor("a")(rowsToAccept)

    // When
    iteratorToVisitable.accept(input.iterator, recordingVisitor)

    // Then
    recordingVisitor.recorded should equal(input.take(rowsToAccept).flatMap(_.toList))
  }

  test("should accept nothing if iterator is empty") {
    // Given
    val visitor = mock[ResultVisitor[RuntimeException]]

    // When
    iteratorToVisitable.accept(Iterator.empty, visitor)

    // Then
    verifyZeroInteractions(visitor)
  }

  private case class RecordingResultVisitor(columns: String*)(rowsToAccept: Int = Int.MaxValue) extends ResultVisitor[RuntimeException] {

    require(rowsToAccept >= 0)

    val recorded = new ArrayBuffer[(String, Any)]()

    def visit(row: ResultRow) = {
      recorded ++= columns.map(name => name -> row.get(name))
      recorded.size != rowsToAccept
    }
  }

}
