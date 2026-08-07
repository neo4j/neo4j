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
import org.neo4j.cypher.internal.compiler.CypherPlannerTestSuite
import org.neo4j.cypher.internal.compiler.helpers.TestCountdownCancellationChecker
import org.neo4j.cypher.internal.compiler.planner.logical.ProjectingSelector
import org.neo4j.cypher.internal.compiler.planner.logical.SelectorHeuristic
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.collection.immutable.ListSet
import org.neo4j.cypher.internal.util.collection.immutable.ListSet.IterableOnceToListSet
import org.neo4j.time.FakeClock
import org.neo4j.time.Stopwatch

import scala.collection.immutable.BitSet

class IDPSolverTest extends CypherPlannerTestSuite {

  implicit val loggableChar: IDPLoggable[Char] = _.toString

  private val context: Unit = ()
  private val neverTimesOut = () => new FakeClock().startStopWatch()

  test("Solves a small toy problem") {
    val monitor = mock[IDPSolverMonitor]
    val solver = new IDPSolver[Char, String, Unit](
      monitor = monitor,
      generator = stringAppendingSolverStep(),
      projectingSelector = firstLongest,
      maxTableSize = 16,
      extraOrderRequirement = ExtraRequirement.empty,
      extraPropertyRequirement = ExtraRequirement.empty,
      iterationDurationLimit = Int.MaxValue,
      stopWatchFactory = neverTimesOut,
      cancellationChecker = CancellationChecker.neverCancelled()
    )

    val seed = Seq(
      SolvableItemWithExtraRequirements(Set('a'), isSorted = false, hasPrefetchedProperties = false) -> "a",
      SolvableItemWithExtraRequirements(Set('b'), isSorted = false, hasPrefetchedProperties = false) -> "b",
      SolvableItemWithExtraRequirements(Set('c'), isSorted = false, hasPrefetchedProperties = false) -> "c",
      SolvableItemWithExtraRequirements(Set('d'), isSorted = false, hasPrefetchedProperties = false) -> "d"
    )

    val solution = solver(seed, ListSet('a', 'b', 'c', 'd'), context)

    solution should equal(BestResults("abcd", None, None))
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
      extraOrderRequirement = CapitalizationRequirement(capitalization),
      extraPropertyRequirement = ExtraRequirement.empty,
      iterationDurationLimit = Int.MaxValue,
      stopWatchFactory = neverTimesOut,
      cancellationChecker = CancellationChecker.neverCancelled()
    )

    val seed = Seq(
      SolvableItemWithExtraRequirements(Set('a'), isSorted = false, hasPrefetchedProperties = false) -> "a",
      SolvableItemWithExtraRequirements(Set('b'), isSorted = false, hasPrefetchedProperties = false) -> "b",
      SolvableItemWithExtraRequirements(Set('c'), isSorted = false, hasPrefetchedProperties = false) -> "c",
      SolvableItemWithExtraRequirements(Set('d'), isSorted = false, hasPrefetchedProperties = false) -> "d"
    )

    val solution = solver(seed, ListSet('a', 'b', 'c', 'd'), context)

    solution should equal(BestResults("ABCD", Some("ABCD"), None))
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
      extraOrderRequirement = CapitalizationRequirement(capitalization),
      extraPropertyRequirement = ExtraRequirement.empty,
      iterationDurationLimit = Int.MaxValue,
      stopWatchFactory = neverTimesOut,
      cancellationChecker = CancellationChecker.neverCancelled()
    )

    val seed = Seq(
      SolvableItemWithExtraRequirements(Set('A'), isSorted = false, hasPrefetchedProperties = false) -> "A",
      SolvableItemWithExtraRequirements(Set('B'), isSorted = false, hasPrefetchedProperties = false) -> "B",
      SolvableItemWithExtraRequirements(Set('C'), isSorted = false, hasPrefetchedProperties = false) -> "C",
      SolvableItemWithExtraRequirements(Set('D'), isSorted = false, hasPrefetchedProperties = false) -> "D"
    )

    val solution = solver(seed, ListSet('A', 'B', 'C', 'D'), context)

    solution should equal(BestResults("ABCD", Some("abcd"), None))
    verify(monitor).foundPlanAfter(1)
  }

  test(
    "Solves a small toy problem with second extra requirement where the best candidate does not fulfil the requirement."
  ) {
    val monitor = mock[IDPSolverMonitor]
    val solver = new IDPSolver[Char, String, Unit](
      monitor = monitor,
      generator = stringToNumericConversionStep(),
      projectingSelector = preferLetter,
      maxTableSize = 16,
      extraOrderRequirement = ExtraRequirement.empty,
      extraPropertyRequirement = NumericRequirement,
      iterationDurationLimit = Int.MaxValue,
      stopWatchFactory = neverTimesOut,
      cancellationChecker = CancellationChecker.neverCancelled()
    )

    val seed = Seq(
      SolvableItemWithExtraRequirements(Set('A'), isSorted = false, hasPrefetchedProperties = false) -> "A",
      SolvableItemWithExtraRequirements(Set('B'), isSorted = false, hasPrefetchedProperties = false) -> "B",
      SolvableItemWithExtraRequirements(Set('C'), isSorted = false, hasPrefetchedProperties = false) -> "C",
      SolvableItemWithExtraRequirements(Set('D'), isSorted = false, hasPrefetchedProperties = false) -> "D"
    )

    val solution = solver(seed, ListSet('A', 'B', 'C', 'D'), context)

    solution should equal(BestResults("ABCD", None, Some("0123")))
    verify(monitor).foundPlanAfter(1)
  }

  test("Solves a small toy problem with second extra requirement where the best candidate fulfils the requirement.") {
    val monitor = mock[IDPSolverMonitor]
    val solver = new IDPSolver[Char, String, Unit](
      monitor = monitor,
      generator = stringToNumericConversionStep(),
      projectingSelector = firstLongest,
      maxTableSize = 16,
      extraOrderRequirement = ExtraRequirement.empty,
      extraPropertyRequirement = NumericRequirement,
      iterationDurationLimit = Int.MaxValue,
      stopWatchFactory = neverTimesOut,
      cancellationChecker = CancellationChecker.neverCancelled()
    )

    val seed = Seq(
      SolvableItemWithExtraRequirements(Set('A'), isSorted = false, hasPrefetchedProperties = false) -> "A",
      SolvableItemWithExtraRequirements(Set('B'), isSorted = false, hasPrefetchedProperties = false) -> "B",
      SolvableItemWithExtraRequirements(Set('C'), isSorted = false, hasPrefetchedProperties = false) -> "C",
      SolvableItemWithExtraRequirements(Set('D'), isSorted = false, hasPrefetchedProperties = false) -> "D"
    )

    val solution = solver(seed, ListSet('A', 'B', 'C', 'D'), context)

    solution should equal(BestResults("0123", None, Some("0123")))
    verify(monitor).foundPlanAfter(1)
  }

  test("Solves a small toy problem with both extra requirements.") {
    val monitor = mock[IDPSolverMonitor]
    val capitalization = Capitalization(false)
    val solver = new IDPSolver[Char, String, Unit](
      monitor = monitor,
      generator = capitalizationAndNumericConversionStep(capitalization),
      projectingSelector = preferLetter,
      maxTableSize = 16,
      extraOrderRequirement = CapitalizationRequirement(capitalization),
      extraPropertyRequirement = NumericRequirement,
      iterationDurationLimit = Int.MaxValue,
      stopWatchFactory = neverTimesOut,
      cancellationChecker = CancellationChecker.neverCancelled()
    )

    val seed = Seq(
      SolvableItemWithExtraRequirements(Set('A'), isSorted = false, hasPrefetchedProperties = false) -> "A",
      SolvableItemWithExtraRequirements(Set('B'), isSorted = false, hasPrefetchedProperties = false) -> "B",
      SolvableItemWithExtraRequirements(Set('C'), isSorted = false, hasPrefetchedProperties = false) -> "C",
      SolvableItemWithExtraRequirements(Set('D'), isSorted = false, hasPrefetchedProperties = false) -> "D"
    )

    val solution = solver(seed, ListSet('A', 'B', 'C', 'D'), context)

    solution should equal(BestResults("ABCD", Some("abcd"), Some("0123")))
    verify(monitor).foundPlanAfter(2)
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
      extraOrderRequirement = ExtraRequirement.empty,
      extraPropertyRequirement = ExtraRequirement.empty,
      iterationDurationLimit = Int.MaxValue,
      stopWatchFactory = neverTimesOut,
      cancellationChecker = CancellationChecker.neverCancelled()
    )

    val seed = Seq(
      SolvableItemWithExtraRequirements(Set('a'), isSorted = false, hasPrefetchedProperties = false) -> "a",
      SolvableItemWithExtraRequirements(Set('b'), isSorted = false, hasPrefetchedProperties = false) -> "b",
      SolvableItemWithExtraRequirements(Set('c'), isSorted = false, hasPrefetchedProperties = false) -> "c",
      SolvableItemWithExtraRequirements(Set('d'), isSorted = false, hasPrefetchedProperties = false) -> "d"
    )

    val todo = Seq('b', 'a', 'd', 'c')

    solver(seed, todo.toListSet, context)

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
      extraOrderRequirement = ExtraRequirement.empty,
      extraPropertyRequirement = ExtraRequirement.empty,
      iterationDurationLimit = Int.MaxValue,
      stopWatchFactory = neverTimesOut,
      cancellationChecker = CancellationChecker.neverCancelled()
    )

    val seed: Seq[(SolvableItemWithExtraRequirements[Char], String)] = Seq(
      SolvableItemWithExtraRequirements(Set('a'), isSorted = false, hasPrefetchedProperties = false) -> "a",
      SolvableItemWithExtraRequirements(Set('b'), isSorted = false, hasPrefetchedProperties = false) -> "b",
      SolvableItemWithExtraRequirements(Set('c'), isSorted = false, hasPrefetchedProperties = false) -> "c",
      SolvableItemWithExtraRequirements(Set('d'), isSorted = false, hasPrefetchedProperties = false) -> "d",
      SolvableItemWithExtraRequirements(Set('e'), isSorted = false, hasPrefetchedProperties = false) -> "e",
      SolvableItemWithExtraRequirements(Set('f'), isSorted = false, hasPrefetchedProperties = false) -> "f",
      SolvableItemWithExtraRequirements(Set('g'), isSorted = false, hasPrefetchedProperties = false) -> "g",
      SolvableItemWithExtraRequirements(Set('h'), isSorted = false, hasPrefetchedProperties = false) -> "h"
    )

    solver(seed, ListSet('a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'), context)

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
      extraOrderRequirement = ExtraRequirement.empty,
      extraPropertyRequirement = ExtraRequirement.empty,
      iterationDurationLimit = iterationDuration,
      stopWatchFactory = () => Stopwatch.start(),
      cancellationChecker = CancellationChecker.neverCancelled()
    )

    val seed: Seq[(SolvableItemWithExtraRequirements[Char], String)] =
      ('a'.toInt to 'm'.toInt).foldLeft(Seq.empty[(SolvableItemWithExtraRequirements[Char], String)]) { (acc, i) =>
        val c = i.toChar
        acc :+ (SolvableItemWithExtraRequirements(
          Set(c),
          isSorted = false,
          hasPrefetchedProperties = false
        ) -> c.toString)
      }
    val result = seed.foldLeft(ListSet.empty[Char]) { (acc, t) =>
      acc ++ t._1.goal
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
        extraOrderRequirement = ExtraRequirement.empty,
        extraPropertyRequirement = ExtraRequirement.empty,
        iterationDurationLimit = Int.MaxValue,
        stopWatchFactory = neverTimesOut,
        cancellationChecker = cancellationChecker
      )

      val seed = Seq(
        SolvableItemWithExtraRequirements(Set('a'), isSorted = false, hasPrefetchedProperties = false) -> "a",
        SolvableItemWithExtraRequirements(Set('b'), isSorted = false, hasPrefetchedProperties = false) -> "b",
        SolvableItemWithExtraRequirements[Char](Set('c'), isSorted = false, hasPrefetchedProperties = false) -> "c",
        SolvableItemWithExtraRequirements[Char](Set('d'), isSorted = false, hasPrefetchedProperties = false) -> "d"
      )

      val solution = solver(seed, ListSet('a', 'b', 'c', 'd'), context)
      solution should equal(BestResults("abcd", None, None))
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
      heuristic: SelectorHeuristic,
      planDescriptor: X => Option[String]
    ): Option[X] = {
      val elements = input.toList.sortBy(x => projector(x))
      if (elements.nonEmpty) Some(elements.maxBy(x => projector(x).length)) else None
    }
  }

  /**
   * Longer strings that is an alphabet wins.
   */
  private object preferLetter extends ProjectingSelector[String] {

    override def applyWithResolvedPerPlan[X](
      projector: X => String,
      input: Iterable[X],
      resolved: => String,
      resolvedPerPlan: LogicalPlan => String,
      heuristic: SelectorHeuristic,
      planDescriptor: X => Option[String]
    ): Option[X] = {
      val (elementsWithLetters, elementsWithNumbers) =
        input.toList.sortBy(x => projector(x)).partition(x => projector(x).forall(_.isLetter))
      if (elementsWithLetters.nonEmpty) Some(elementsWithLetters.maxBy(x => projector(x).length))
      else if (elementsWithNumbers.nonEmpty) Some(elementsWithNumbers.maxBy(x => projector(x).length))
      else None
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

    private def isSorted(chars: String): Boolean =
      (chars.length <= 1) || 0.to(chars.length - 2).forall(i =>
        isConsecutiveAlphabet(chars.charAt(i), chars.charAt(i + 1))
          || isConsecutiveAlphabet(chars.toLowerCase.charAt(i), chars.toLowerCase.charAt(i + 1))
      )

    private def isConsecutiveAlphabet(char1: Char, char2: Char) = char1.toInt + 1 == char2.toInt
  }

  private case class CapitalizationRequirement(capitalization: Capitalization) extends ExtraRequirement[String] {
    override def fulfils(result: String): Boolean = result.equals(capitalization.normalize(result))
  }

  private object NumericRequirement extends ExtraRequirement[String] {
    override def fulfils(result: String): Boolean = result.forall(_.isDigit)
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

  private case class stringToNumericConversionStep()
      extends IDPSolverStep[Char, String, Unit] {

    override def apply(
      registry: IdRegistry[Char],
      goal: Goal,
      table: IDPCache[String],
      context: Unit
    ): Iterator[String] = {
      stringAppendingSolverStep()(registry, goal, table, context)
        .flatMap { candidate =>
          if (candidate.forall(_.isDigit))
            Seq(candidate)
          else
            Seq(candidate, candidate.map(_.toLower.toInt - 'a'.toInt).mkString)

        }
    }
  }

  private case class capitalizationAndNumericConversionStep(capitalization: Capitalization)
      extends IDPSolverStep[Char, String, Unit] {

    override def apply(
      registry: IdRegistry[Char],
      goal: Goal,
      table: IDPCache[String],
      context: Unit
    ): Iterator[String] = {
      stringAppendingSolverStepWithCapitalization(capitalization: Capitalization)(
        registry,
        goal,
        table,
        context
      ).flatMap { candidate =>
        if (candidate.forall(_.isDigit))
          Seq(candidate)
        else
          Seq(candidate, candidate.map(_.toLower.toInt - 'a'.toInt).mkString)
      }
    }
  }

}
