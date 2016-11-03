/*
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
package org.neo4j.cypher.internal.compiler.v3_2.pipes

import org.neo4j.cypher.internal.compiler.v3_2.ExecutionContext
import org.neo4j.cypher.internal.compiler.v3_2.commands.expressions.Variable
import org.neo4j.cypher.internal.compiler.v3_2.executionplan.{EagerReadWriteCallMode, LazyReadOnlyCallMode}
import org.neo4j.cypher.internal.compiler.v3_2.spi._
import org.neo4j.cypher.internal.frontend.v3_2.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.frontend.v3_2.symbols._
import org.neo4j.cypher.internal.frontend.v3_2.test_helpers.CypherFunSuite

class ProcedureCallPipeTest
  extends CypherFunSuite
    with PipeTestSupport
    with AstConstructionTestSupport {

  val procedureName = QualifiedName(List.empty, "foo")
  val emptyStringArray = Array.empty[String]

  test("should execute read-only procedure calls") {
    val lhsData = List(Map("a" -> 1), Map("a" -> 2))
    val lhs = new FakePipe(lhsData.iterator, "a" -> CTNumber)

    val pipe = ProcedureCallPipe(
      source = lhs,
      name = procedureName,
      callMode = LazyReadOnlyCallMode(emptyStringArray),
      argExprs = Seq(Variable("a")),
      rowProcessing = FlatMapAndAppendToRow,
      resultSymbols = Seq("r" -> CTString),
      resultIndices = Seq(0 -> "r")
    )()(newMonitor)

    val qtx = new FakeQueryContext(procedureName, resultsTransformer, ProcedureReadOnlyAccess(emptyStringArray))

    pipe.createResults(QueryStateHelper.emptyWith(qtx)).toList should equal(List(
      ExecutionContext.from("a" ->1, "r" -> "take 1/1"),
      ExecutionContext.from("a" ->2, "r" -> "take 1/2"),
      ExecutionContext.from("a" ->2, "r" -> "take 2/2")
    ))
  }

  test("should execute read-write procedure calls") {
    val lhsData = List(Map("a" -> 1), Map("a" -> 2))
    val lhs = new FakePipe(lhsData.iterator, "a" -> CTNumber)

    val pipe = ProcedureCallPipe(
      source = lhs,
      name = procedureName,
      callMode = EagerReadWriteCallMode(emptyStringArray),
      argExprs = Seq(Variable("a")),
      rowProcessing = FlatMapAndAppendToRow,
      resultSymbols = Seq("r" -> CTString),
      resultIndices = Seq(0 -> "r")
    )()(newMonitor)

    val qtx = new FakeQueryContext(procedureName, resultsTransformer, ProcedureReadWriteAccess(emptyStringArray))
    pipe.createResults(QueryStateHelper.emptyWith(qtx)).toList should equal(List(
      ExecutionContext.from("a" -> 1, "r" -> "take 1/1"),
      ExecutionContext.from("a" -> 2, "r" -> "take 1/2"),
      ExecutionContext.from("a" -> 2, "r" -> "take 2/2")
    ))
  }

  test("should execute void procedure calls") {
    val lhsData = List(Map("a" -> 1), Map("a" -> 2))
    val lhs = new FakePipe(lhsData.iterator, "a" -> CTNumber)

    val pipe = ProcedureCallPipe(
      source = lhs,
      name = procedureName,
      callMode = EagerReadWriteCallMode(emptyStringArray),
      argExprs = Seq(Variable("a")),
      rowProcessing = PassThroughRow,
      resultSymbols = Seq.empty,
      resultIndices = Seq.empty
    )()(newMonitor)

    val qtx = new FakeQueryContext(procedureName, _ => Iterator.empty, ProcedureReadWriteAccess(emptyStringArray))
    pipe.createResults(QueryStateHelper.emptyWith(qtx)).toList should equal(List(
      ExecutionContext.from("a" -> 1),
      ExecutionContext.from("a" -> 2)
    ))
  }

  private def resultsTransformer(args: Seq[Any]): Iterator[Array[AnyRef]] = {
    val count = args.head.asInstanceOf[Number].intValue()
    1.to(count).map { i =>
      Array[AnyRef](s"take $i/$count")
    }

  }.toIterator


  class FakeQueryContext(procedureName: QualifiedName, result: Seq[Any] => Iterator[Array[AnyRef]],
                         expectedAccessMode: ProcedureAccessMode) extends QueryContext with QueryContextAdaptation {
    override def isGraphKernelResultValue(v: Any): Boolean = false

    override def callReadOnlyProcedure(name: QualifiedName, args: Seq[Any], allowed: Array[String]) = {
      expectedAccessMode should equal(ProcedureReadOnlyAccess(emptyStringArray))
      doIt(name, args, allowed)
    }

    override def callReadWriteProcedure(name: QualifiedName, args: Seq[Any], allowed: Array[String]): Iterator[Array[AnyRef]] = {
      expectedAccessMode should equal(ProcedureReadWriteAccess(emptyStringArray))
      doIt(name, args, allowed)
    }

    private def doIt(name: QualifiedName, args: Seq[Any], allowed: Array[String]): Iterator[Array[AnyRef]] = {
      name should equal(procedureName)
      args.length should be(1)
      result(args)
    }

  }
}
