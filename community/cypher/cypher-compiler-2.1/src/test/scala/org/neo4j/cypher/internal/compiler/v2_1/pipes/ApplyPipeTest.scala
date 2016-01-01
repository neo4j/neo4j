/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
import org.neo4j.cypher.internal.compiler.v2_1.ExecutionContext
import org.neo4j.cypher.internal.compiler.v2_1.symbols.{CTNumber, SymbolTable}

class ApplyPipeTest extends CypherFunSuite {

  def newMonitor = mock[PipeMonitor]

  test("should work by applying the identity operator on the rhs") {
    val lhsData = List(Map("a" -> 1), Map("a" -> 2))
    val lhs = new FakePipe(lhsData.iterator, "a" -> CTNumber)

    val rhs = new Pipe {
      protected def internalCreateResults(state: QueryState) = Iterator(state.initialContext.get)

      def exists(pred: (Pipe) => Boolean) = ???
      def planDescription = ???
      def symbols: SymbolTable = ???
      def monitor: PipeMonitor = newMonitor
      def dup(sources: List[Pipe]): Pipe = ???
      def sources: Seq[Pipe] = ???
    }

    val result = ApplyPipe(lhs, rhs)(newMonitor).createResults(QueryStateHelper.empty).toList

    result should equal(lhsData)
  }

  test("should work by applying a  on the rhs") {
    val lhsData = List(Map("a" -> 1, "b" -> 3), Map("a" -> 2, "b" -> 4))
    val lhs = new FakePipe(lhsData.iterator, "a" -> CTNumber, "b" -> CTNumber)
    val rhsData = "c" -> 36

    val rhs = new Pipe {
      protected def internalCreateResults(state: QueryState) =
        Iterator(ExecutionContext.empty += rhsData)

      def exists(pred: (Pipe) => Boolean) = ???
      def planDescription = ???
      def symbols: SymbolTable = ???
      def monitor: PipeMonitor = newMonitor
      def dup(sources: List[Pipe]): Pipe = ???
      def sources: Seq[Pipe] = ???
    }

    val result = ApplyPipe(lhs, rhs)(newMonitor).createResults(QueryStateHelper.empty).toList

    result should equal(lhsData.map(_ + rhsData))
  }

  test("should work even if inner pipe overwrites values") {
    val lhsData = List(Map("a" -> 1, "b" -> 3), Map("a" -> 2, "b" -> 4))
    val lhs = new FakePipe(lhsData.iterator, "a" -> CTNumber, "b" -> CTNumber)

    val rhs = new Pipe {
      protected def internalCreateResults(state: QueryState) =
        Iterator(state.initialContext.get += "b" -> null)

      def exists(pred: (Pipe) => Boolean) = ???
      def planDescription = ???
      def symbols: SymbolTable = ???
      def monitor: PipeMonitor = newMonitor
      def dup(sources: List[Pipe]): Pipe = ???
      def sources: Seq[Pipe] = ???
    }

    val result = ApplyPipe(lhs, rhs)(newMonitor).createResults(QueryStateHelper.empty).toList

    result should equal(lhsData)
  }
}
