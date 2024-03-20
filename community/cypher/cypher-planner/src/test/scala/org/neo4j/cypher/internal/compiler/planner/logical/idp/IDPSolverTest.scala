/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.planner.logical.idp

import org.mockito.Mockito.spy
import org.mockito.Mockito.verify
import org.mockito.Mockito.verifyNoMoreInteractions
import org.neo4j.cypher.internal.compiler.helpers.TestCountdownCancellationChecker
import org.neo4j.cypher.internal.compiler.planner.logical.ProjectingSelector
import org.neo4j.cypher.internal.compiler.planner.logical.SelectorHeuristic
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.time.FakeClock
import org.neo4j.time.Stopwatch

import scala.collection.immutable.BitSet

class IDPSolverTest extends CypherFunSuite {

  private val context: Unit = ()
  private val neverTimesOut = () => new FakeClock().startStopWatch()

  test("Solves a small toy problem") {
    val monitor = mock[IDPSolverMonitor]
    val solver = new IDPSolver[Char, String, Unit](
      monitor = monitor,
      generator = stringAppendingSolverStep(),
      projectingSelector = firstLongest,
      maxTableSize = 16,
      extraRequirement = ExtraRequirement.empty,
      iterationDurationLimit = Int.MaxValue,
      stopWatchFactory = neverTimesOut,
      cancellationChecker = CancellationChecker.neverCancelled()
    )

    val seed = Seq(
      (Set('a'), false) -> "a",
      (Set('b'), false) -> "b",
      (Set('c'), false) -> "c",
      (Set('d'), false) -> "d"
    )

    val solution = solver(seed, Seq('a', 'b', 'c', 'd'), context)

    solution should equal(BestResults("abcd", None))
    verify(monitor).foundPlanAfter(1)
  }

  test("Solves a small toy problem with an extra requirement. Best overall plan fulfils requirement.") {
    val monitor = mock[IDPSolverMonitor]
    val capitalization = Capitalization(true)
    val solver = new IDPSolver[Char, String, Unit](
      monitor = monitor,
      generator = stringAppendingSolverStepWithCapitalization(capitalization),
      projectingSelector = firstLongest,
      maxTableSize = 16,
      extraRequirement = CapitalizationRequirement(capitalization),
      iterationDurationLimit = Int.MaxValue,
      stopWatchFactory = neverTimesOut,
      cancellationChecker = CancellationChecker.neverCancelled()
    )

    val seed = Seq(
      (Set('a'), false) -> "a",
      (Set('b'), false) -> "b",
      (Set('c'), false) -> "c",
      (Set('d'), false) -> "d"
    )

    val solution = solver(seed, Seq('a', 'b', 'c', 'd'), context)

    solution should equal(BestResults("ABCD", Some("ABCD")))
    verify(monitor).foundPlanAfter(1)
  }

  test("Solves a small toy problem with an extra requirement. Best overall does not fulfil requirement.") {
    val monitor = mock[IDPSolverMonitor]
    val capitalization = Capitalization(false)
    val solver = new IDPSolver[Char, String, Unit](
      monitor = monitor,
      generator = stringAppendingSolverStepWithCapitalization(capitalization),
      projectingSelector = firstLongest,
      maxTableSize = 16,
      extraRequirement = CapitalizationRequirement(capitalization),
      iterationDurationLimit = Int.MaxValue,
      stopWatchFactory = neverTimesOut,
      cancellationChecker = CancellationChecker.neverCancelled()
    )

    val seed = Seq(
      (Set('A'), false) -> "A",
      (Set('B'), false) -> "B",
      (Set('C'), false) -> "C",
      (Set('D'), false) -> "D"
    )

    val solution = solver(seed, Seq('A', 'B', 'C', 'D'), context)

    solution should equal(BestResults("ABCD", Some("abcd")))
    verify(monitor).foundPlanAfter(1)
  }

  test("Registers solvables in the order given by initial todo") {
    val monitor = mock[IDPSolverMonitor]
    val registry = IdRegistry[Char]
    val solver = new IDPSolver[Char, String, Unit](
      monitor = monitor,
      generator = stringAppendingSolverStep(),
      registryFactory = () => registry,
      projectingSelector = firstLongest,
      maxTableSize = 16,
      extraRequirement = ExtraRequirement.empty,
      iterationDurationLimit = Int.MaxValue,
      stopWatchFactory = neverTimesOut,
      cancellationChecker = CancellationChecker.neverCancelled()
    )

    val seed = Seq(
      (Set('a'), false) -> "a",
      (Set('b'), false) -> "b",
      (Set('c'), false) -> "c",
      (Set('d'), false) -> "d"
    )

    val todo = Seq('b', 'a', 'd', 'c')

    solver(seed, todo, context)

    // Offset by one due to the bit representing sorted
    todo.indices.flatMap(i => registry.lookup(i + 1)) should equal(todo)
  }

  test("Compacts table at size limit") {
    var table: IDPTable[String] = null
    val monitor = mock[IDPSolverMonitor]
    val solver = new IDPSolver[Char, String, Unit](
      monitor = monitor,
      generator = stringAppendingSolverStep(),
      projectingSelector = firstLongest,
      tableFactory = (registry: IdRegistry[Char], seed: Seed[Char, String]) => {
        table = spy(IDPTable(registry, seed))
        table
      },
      maxTableSize = 4,
      extraRequirement = ExtraRequirement.empty,
      iterationDurationLimit = Int.MaxValue,
      stopWatchFactory = neverTimesOut,
      cancellationChecker = CancellationChecker.neverCancelled()
    )

    val seed: Seq[((Set[Char], Boolean), String)] = Seq(
      (Set('a'), false) -> "a",
      (Set('b'), false) -> "b",
      (Set('c'), false) -> "c",
      (Set('d'), false) -> "d",
      (Set('e'), false) -> "e",
      (Set('f'), false) -> "f",
      (Set('g'), false) -> "g",
      (Set('h'), false) -> "h"
    )

    solver(seed, Seq('a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'), context)

    verify(monitor).startIteration(1)
    verify(monitor).endIteration(1, 2, 16)
    verify(table).removeAllTracesOf(Goal(BitSet(1, 2)))
    verify(monitor).startIteration(2)
    verify(monitor).endIteration(2, 2, 14)
    verify(table).removeAllTracesOf(Goal(BitSet(3, 9)))
    verify(monitor).startIteration(3)
    verify(monitor).endIteration(3, 2, 12)
    verify(table).removeAllTracesOf(Goal(BitSet(4, 10)))
    verify(monitor).startIteration(4)
    verify(monitor).endIteration(4, 2, 10)
    verify(table).removeAllTracesOf(Goal(BitSet(5, 11)))
    verify(monitor).startIteration(5)
    verify(monitor).endIteration(5, 2, 8)
    verify(table).removeAllTracesOf(Goal(BitSet(6, 12)))
    verify(monitor).startIteration(6)
    verify(monitor).endIteration(6, 3, 6)
    verify(table).removeAllTracesOf(Goal(BitSet(7, 8, 13)))
    verify(monitor).foundPlanAfter(6)
    verifyNoMoreInteractions(monitor)
  }

  case class TestIDPSolverMonitor() extends IDPSolverMonitor {
    var maxStartIteration = 0
    var foundPlanIteration = 0

    override def startIteration(iteration: Int): Unit = maxStartIteration = iteration

    override def foundPlanAfter(iterations: Int): Unit = foundPlanIteration = iterations

    override def endIteration(iteration: Int, depth: Int, tableSize: Int): Unit = {}
  }

  private def runTimeLimitedSolver(iterationDuration: Int): Int = {
    var table: IDPTable[String] = null
    val monitor = TestIDPSolverMonitor()
    val solver = new IDPSolver[Char, String, Unit](
      monitor = monitor,
      generator = stringAppendingSolverStep(),
      projectingSelector = firstLongest,
      tableFactory = (registry: IdRegistry[Char], seed: Seed[Char, String]) => {
        table = spy(IDPTable(registry, seed))
        table
      },
      maxTableSize = Int.MaxValue,
      extraRequirement = ExtraRequirement.empty,
      iterationDurationLimit = iterationDuration,
      stopWatchFactory = () => Stopwatch.start(),
      cancellationChecker = CancellationChecker.neverCancelled()
    )

    val seed: Seq[((Set[Char], Boolean), String)] =
      ('a'.toInt to 'm'.toInt).foldLeft(Seq.empty[((Set[Char], Boolean), String)]) { (acc, i) =>
        val c = i.toChar
        acc :+ ((Set(c), false) -> c.toString)
      }
    val result = seed.foldLeft(Seq.empty[Char]) { (acc, t) =>
      acc ++ t._1._1
    }

    solver(seed, result, context)

    monitor.maxStartIteration should equal(monitor.foundPlanIteration)
    monitor.maxStartIteration
  }

  test("Compacts table at time limit") {
    val shortSolverIterations = runTimeLimitedSolver(10)
    val longSolverIterations = runTimeLimitedSolver(1000)
    shortSolverIterations should be > longSolverIterations
  }

  test("should check CancellationChecker in IDP loop") {
    def runIdp(cancellationChecker: CancellationChecker): Unit = {
      val solver = new IDPSolver[Char, String, Unit](
        monitor = mock[IDPSolverMonitor],
        generator = stringAppendingSolverStep(),
        projectingSelector = firstLongest,
        maxTableSize = 16,
        extraRequirement = ExtraRequirement.empty,
        iterationDurationLimit = Int.MaxValue,
        stopWatchFactory = neverTimesOut,
        cancellationChecker = cancellationChecker
      )

      val seed = Seq(
        (Set('a'), false) -> "a",
        (Set('b'), false) -> "b",
        (Set('c'), false) -> "c",
        (Set('d'), false) -> "d"
      )

      val solution = solver(seed, Seq('a', 'b', 'c', 'd'), context)
      solution should equal(BestResults("abcd", None))
    }

    noException should be thrownBy runIdp(new TestCountdownCancellationChecker(11))

    val cancellationChecker = new TestCountdownCancellationChecker(10)
    val ex = the[RuntimeException] thrownBy runIdp(cancellationChecker)
    ex should have message cancellationChecker.errorMessage
  }

  /**
   * Longer strings win. If they are the same length, the first in alphabetical order wins.
   * That means upper case wins over lower case.
   */
  private object firstLongest extends ProjectingSelector[String] {

    override def applyWithResolvedPerPlan[X](
      projector: X => String,
      input: Iterable[X],
      resolved: => String,
      resolvedPerPlan: LogicalPlan => String,
      heuristic: SelectorHeuristic
    ): Option[X] = {
      val elements = input.toList.sortBy(x => projector(x))
      if (elements.nonEmpty) Some(elements.maxBy(x => projector(x).length)) else None
    }
  }

  private case class stringAppendingSolverStep() extends IDPSolverStep[Char, String, Unit] {

    override def apply(
      registry: IdRegistry[Char],
      goal: Goal,
      table: IDPCache[String],
      context: Unit
    ): Iterator[String] = {
      val goalSize = goal.size
      for {
        leftGoal <- goal.subGoals if leftGoal.size <= goalSize
        lhs <- table(leftGoal).iterator
        rightGoal = Goal(goal.bitSet &~ leftGoal.bitSet) // bit set -- operator
        rhs <- table(rightGoal).iterator
        candidate = lhs ++ rhs if isSorted(candidate)
      } yield candidate
    }

    def isSorted(chars: String): Boolean =
      (chars.length <= 1) || 0.to(chars.length - 2).forall(i =>
        chars.charAt(i).toInt + 1 == chars.charAt(i + 1).toInt
          || chars.toLowerCase.charAt(i).toInt + 1 == chars.toLowerCase.charAt(i + 1).toInt
      )
  }

  private case class CapitalizationRequirement(capitalization: Capitalization) extends ExtraRequirement[String] {
    override def fulfils(result: String): Boolean = result.equals(capitalization.normalize(result))
  }

  private case class Capitalization(upper: Boolean) {
    def normalize(string: String): String = if (upper) string.toUpperCase else string.toLowerCase
  }

  private case class stringAppendingSolverStepWithCapitalization(capitalization: Capitalization)
      extends IDPSolverStep[Char, String, Unit] {

    override def apply(
      registry: IdRegistry[Char],
      goal: Goal,
      table: IDPCache[String],
      context: Unit
    ): Iterator[String] = {
      stringAppendingSolverStep()(registry, goal, table, context).flatMap { candidate =>
        if (capitalization == null)
          Seq(candidate)
        else
          Seq(candidate, capitalization.normalize(candidate))
      }
    }
  }
}
