/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical.idp

import org.mockito.Mockito.{spy, verify, verifyNoMoreInteractions}
import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2.planner.SemanticTable
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.Metrics._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.steps.LogicalPlanProducer
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.{QueryGraphSolver, Metrics, LogicalPlanningContext, ProjectingSelector}
import org.neo4j.cypher.internal.compiler.v2_2.spi.PlanContext

import scala.collection.immutable.BitSet

class IDPSolverTest extends CypherFunSuite {

  private implicit val context = ()

  test("Solves a small toy problem") {
    val solver = new IDPSolver[Char, String, Unit](
      monitor = mock[IDPSolverMonitor],
      generator = stringAppendingSolverStep,
      projectingSelector = firstLongest,
      maxTableSize = 16
    )

    val seed = Seq(
      Set('a') -> "a",
      Set('b') -> "b",
      Set('c') -> "c",
      Set('d') -> "d"
    )

    val solution = solver(seed, Set('a', 'b', 'c', 'd'))

    solution.toList should equal(List(Set('a', 'b', 'c', 'd') -> "abcd"))
  }

  test("Compacts table at size limit") {
    var table: IDPTable[String] = null
    val monitor = mock[IDPSolverMonitor]
    val solver = new IDPSolver[Char, String, Unit](
      monitor = monitor,
      generator = stringAppendingSolverStep,
      projectingSelector = firstLongest,
      tableFactory = (registry: IdRegistry[Char], seed: Seed[Char, String]) => {
        table = spy(IDPTable(registry, seed))
        table
      },
      maxTableSize = 4
    )

    val seed = Seq(
      Set('a') -> "a",
      Set('b') -> "b",
      Set('c') -> "c",
      Set('d') -> "d",
      Set('e') -> "e",
      Set('f') -> "f",
      Set('g') -> "g",
      Set('h') -> "h"
    )

    solver(seed, Set('a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'))

    verify(monitor).startIteration(1)
    verify(monitor).endIteration(1, 2, 16)
    verify(table).removeAllTracesOf(BitSet(0, 1))
    verify(monitor).startIteration(2)
    verify(monitor).endIteration(2, 2, 14)
    verify(table).removeAllTracesOf(BitSet(2, 8))
    verify(monitor).startIteration(3)
    verify(monitor).endIteration(3, 2, 12)
    verify(table).removeAllTracesOf(BitSet(3, 9))
    verify(monitor).startIteration(4)
    verify(monitor).endIteration(4, 2, 10)
    verify(table).removeAllTracesOf(BitSet(4, 10))
    verify(monitor).startIteration(5)
    verify(monitor).endIteration(5, 2, 8)
    verify(table).removeAllTracesOf(BitSet(5, 11))
    verify(monitor).startIteration(6)
    verify(monitor).endIteration(6, 2, 6)
    verify(table).removeAllTracesOf(BitSet(6, 12))
    verify(monitor).startIteration(7)
    verify(monitor).endIteration(7, 2, 3)
    verify(table).removeAllTracesOf(BitSet(7, 13))
    verify(monitor).foundPlanAfter(7)
    verifyNoMoreInteractions(monitor)
  }

  private object firstLongest extends ProjectingSelector[String] {
    override def apply[X](projector: (X) => String, input: Iterable[X]): Option[X] = {
      val elements = input.iterator
      if (elements.hasNext) Some(elements.maxBy(x => projector(x).length)) else None
    }
  }

  private object stringAppendingSolverStep extends IDPSolverStep[Char, String, Unit] {
    override def apply(registry: IdRegistry[Char], goal: Goal, table: IDPCache[String])
                      (implicit context: Unit): Iterator[String] = {
      val goalSize = goal.size
      for (
        leftGoal <- goal.subsets if leftGoal.size <= goalSize;
        lhs <- table(leftGoal);
        rightGoal = goal &~ leftGoal; // bit set -- operator
        rhs <- table(rightGoal);
        candidate = lhs ++ rhs if isSorted(candidate)
      )
      yield candidate
    }

    def isSorted(chars: String) =
      (chars.length <= 1) || 0.to(chars.length - 2).forall(i => chars.charAt(i).toInt + 1 == chars.charAt(i + 1).toInt)
  }

}
