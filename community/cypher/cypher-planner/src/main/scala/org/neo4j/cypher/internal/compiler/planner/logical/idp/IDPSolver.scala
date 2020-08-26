/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.compiler.planner.logical.idp

import java.util.concurrent.TimeUnit

import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.cypher.internal.compiler.helpers.IteratorSupport.RichIterator
import org.neo4j.cypher.internal.compiler.helpers.LazyIterable
import org.neo4j.cypher.internal.compiler.planner.logical.ProjectingSelector
import org.neo4j.cypher.internal.compiler.planner.logical.Selector
import org.neo4j.time.Stopwatch

import scala.collection.immutable.BitSet

trait IDPSolverMonitor {
  def startIteration(iteration: Int)
  def endIteration(iteration: Int, depth: Int, tableSize: Int)
  def foundPlanAfter(iterations: Int)
}

trait ExtraRequirement[-Result] {
  def fulfils(result: Result): Boolean
}

object ExtraRequirement {
  // Logically, everything would fulfil an empty requirement.
  // `false` leads to the same result, though, and is cheaper,
  // since we do not need to keep two different buckets in the table,
  // one for all candidates and one for the fulfilling ones.
  val empty: ExtraRequirement[Any] = (_: Any) => false
}

case class BestResults[+Result](bestResult: Result,
                                bestResultFulfillingReq: Option[Result]) {
  def map[B](f: Result => B): BestResults[B] = BestResults(f(bestResult), bestResultFulfillingReq.map(f))
}

/**
 * Based on the main loop of the IDP1 algorithm described in the paper
 *
 *   "Iterative Dynamic Programming: A New Class of Query Optimization Algorithms"
 *
 * written by Donald Kossmann and Konrad Stocker
 */
class IDPSolver[Solvable, Result, Context](generator: IDPSolverStep[Solvable, Result, Context], // generates candidates at each step
                                           projectingSelector: ProjectingSelector[Result], // pick best from a set of candidates
                                           registryFactory: () => IdRegistry[Solvable] = () => IdRegistry[Solvable], // maps from Set[S] to BitSet
                                           tableFactory: (IdRegistry[Solvable], Seed[Solvable, Result]) => IDPTable[Result] = (registry: IdRegistry[Solvable], seed: Seed[Solvable, Result]) => IDPTable(registry, seed),
                                           maxTableSize: Int, // limits computation effort, reducing result quality
                                           iterationDurationLimit: Long, // limits computation effort, reducing result quality
                                           extraRequirement: ExtraRequirement[Result],
                                           monitor: IDPSolverMonitor,
                                           stopWatchFactory: () => Stopwatch) {

  def apply(seed: Seed[Solvable, Result], initialToDo: Set[Solvable], context: Context): BestResults[Result] = {
    val registry = registryFactory()
    val table = tableFactory(registry, seed)
    var toDo = Goal(registry.registerAll(initialToDo))

    // utility functions
    val goalSelector: Selector[(Goal, Result)] = projectingSelector.apply[(Goal, Result)](_._2, _)

    def generateBestCandidates(maxBlockSize: Int): Int = {
      var largestFinishedIteration = 0
      var blockSize = 1
      var keepGoing = true
      val start = stopWatchFactory()

      while (keepGoing && blockSize <= maxBlockSize) {
        var foundNoCandidate = true
        blockSize += 1
        val goals = toDo.subGoals(blockSize)
        while (keepGoing && goals.hasNext) {
          val goal = goals.next()
          if (table(goal).result.isEmpty) {
            val candidates = LazyIterable(generator(registry, goal, table, context))
            val (extraCandidates, baseCandidates) = candidates.partition(extraRequirement.fulfils)
            val bestExtraCandidate = projectingSelector(extraCandidates)

            // We don't want to compare just the ones that do not fulfil the requirement
            // in isolation, because it could be that the best overall candidate fulfils the requirement.
            // bestExtraCandidate has already been determined to be cheaper than any other extraCandidate,
            // therefore it is enough to cost estimate the bestExtraCandidate against all baseCandidates.
            projectingSelector(baseCandidates ++ bestExtraCandidate.toIterable).foreach { candidate =>
              foundNoCandidate = false
              table.put(goal, sorted = false, candidate)
            }
            // Also add the best candidate from all candidates that fulfil the requirement into the table
            // with `true`.
            bestExtraCandidate.foreach { candidate =>
              foundNoCandidate = false
              table.put(goal, sorted = true, candidate)
            }
            keepGoing = blockSize == 2 ||
              (table.size <= maxTableSize && !start.hasTimedOut(iterationDurationLimit, TimeUnit.MILLISECONDS))
          }
        }
        largestFinishedIteration = if (foundNoCandidate || goals.hasNext) largestFinishedIteration else blockSize
      }
      largestFinishedIteration
    }

    def findBestCandidateInBlock(blockSize: Int): Goal = {
      // Find all candidates that solve the highest number of relationships, ignoring sorted plans.
      val blockCandidates: Iterable[(Goal, Result)] = LazyIterable(table.unsortedPlansOfSize(blockSize)).toIndexedSeq
      // Select the best of those. These candidates solve different things.
      // The best of the candidates is likely to appear in larger plans, so it is a good idea to compact that one.
      val bestInBlock: Option[(Goal, Result)] = goalSelector(blockCandidates)
      val (goal, _) = bestInBlock.getOrElse {
        throw new IllegalStateException(
          s"""Found no solution for block with size $blockSize,
             |$blockCandidates were the selected candidates from the table $table""".stripMargin)
      }
      goal
    }

    def compactBlock(original: Goal): Unit = {
      val newId = registry.compact(original.bitSet)
      val IDPCache.Results(result, sortedResult) = table(original)
      result.foreach { table.put(Goal(BitSet.empty + newId), sorted = false, _) }
      sortedResult.foreach { table.put(Goal(BitSet.empty + newId), sorted = true, _) }
      toDo = Goal(toDo.bitSet -- original.bitSet + newId)
      table.removeAllTracesOf(original)
    }

    // actual algorithm

    var iterations = 0

    while (toDo.size > 1) {
      iterations += 1
      monitor.startIteration(iterations)
      val largestFinished = generateBestCandidates(toDo.size)
      if (largestFinished <= 0) throw new IllegalStateException(
        s"""Unfortunately, the planner was unable to find a plan within the constraints provided.
           |Try increasing the config values `${GraphDatabaseInternalSettings.cypher_idp_solver_table_threshold.name()}`
           |and `${GraphDatabaseInternalSettings.cypher_idp_solver_duration_threshold.name()}` to allow
           |for a larger sub-plan table and longer planning time.""".stripMargin)
      val bestGoal = findBestCandidateInBlock(largestFinished)
      monitor.endIteration(iterations, largestFinished, table.size)
      // Compaction is either done at the very end of the algorithm, or when we hit a table size or time limit.
      // In the latter case, the goal is the one with the best (unsorted) result.
      // In the view of compaction, it does not matter which goal we compact, but is important to keep both the sorted and unsorted
      // results of that goal.
      compactBlock(bestGoal)
    }
    monitor.foundPlanAfter(iterations)

    val (plansFulfillingReq, plans) =  table.plans
      .map { case ((key, fulfilsReq), result) => (registry.explode(key.bitSet), fulfilsReq) -> result }
      .partition { case ((_, fulfilsReq), _) => fulfilsReq }

    val (_, bestResult) = plans
      .toSingleOption
      .getOrElse(throw new AssertionError("Expected a single plan to be left in the plan table"))

    if (plansFulfillingReq.hasNext) {
      val (_, plan) = plansFulfillingReq.toSingleOption
        .getOrElse(throw new AssertionError("Expected a single plan that fulfils the requirements to be left in the plan table"))

      BestResults(bestResult, Some(plan))
    } else {
      BestResults(bestResult, None)
    }
  }
}
