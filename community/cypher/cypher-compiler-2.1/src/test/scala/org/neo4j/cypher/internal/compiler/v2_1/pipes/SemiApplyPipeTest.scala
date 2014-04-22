/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.pipes

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_1.symbols.{SymbolTable, CTNumber}
import org.neo4j.cypher.internal.compiler.v2_1.ExecutionContext

class SemiApplyPipeTest extends CypherFunSuite {

  def newMonitor = mock[PipeMonitor]

  test("should only let through") {
    val lhsData = List(Map("a" -> 1), Map("a" -> 2))
    val lhs = new FakePipe(lhsData.iterator, "a" -> CTNumber)

    val rhs = new Pipe {
      protected def internalCreateResults(state: QueryState) = {
        val initalContext = state.initialContext.get
        if (initalContext("a") == 1) Iterator(initalContext) else Iterator.empty
      }

      def exists(pred: (Pipe) => Boolean) = ???
      def executionPlanDescription = ???
      def symbols: SymbolTable = ???
      def monitor: PipeMonitor = newMonitor
    }

    val result = SemiApplyPipe(lhs, rhs)(newMonitor).createResults(QueryStateHelper.empty).toList

    result should equal(List(Map("a" -> 1)))
  }

  test("should not let anything through if rhs is empty") {
    val lhsData = List(Map("a" -> 1), Map("a" -> 2))
    val lhs = new FakePipe(lhsData.iterator, "a" -> CTNumber)
    val rhs = new FakePipe(Iterator.empty)

    val result = SemiApplyPipe(lhs, rhs)(newMonitor).createResults(QueryStateHelper.empty).toList

    result should equal(List.empty)
  }

  test("should let everything through if rhs is nonEmpty") {
    val lhsData = List(Map("a" -> 1), Map("a" -> 2))
    val lhs = new FakePipe(lhsData.iterator, "a" -> CTNumber)
    val rhs = new FakePipe(Iterator(Map("a" -> 1)))

    val result = SemiApplyPipe(lhs, rhs)(newMonitor).createResults(QueryStateHelper.empty).toList

    result should equal(lhsData)
  }

  test("if lhs is empty, rhs should not be touched") {
    val rhs =  new Pipe {
      def internalCreateResults(state: QueryState): Iterator[ExecutionContext] = fail("should not use this")

      def exists(pred: (Pipe) => Boolean) = ???
      def executionPlanDescription = ???
      def symbols: SymbolTable = ???
      def monitor: PipeMonitor = newMonitor
    }

    val lhs = new FakePipe(Iterator.empty)

    // Should not throw
    SemiApplyPipe(lhs, rhs)(newMonitor).createResults(QueryStateHelper.empty).toList
  }
}
