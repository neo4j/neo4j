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
                               bestSortedResult: Option[Result]) {
  def map[B](f: Result => B): BestResults[B] = BestResults(f(bestResult), bestSortedResult.map(f))
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
                                           tableFactory: (IdRegistry[Solvable], Seed[Solvable, Boolean, Result]) => IDPTable[Result, Boolean] = (registry: IdRegistry[Solvable], seed: Seed[Solvable, Boolean, Result]) => IDPTable(registry, seed),
                                           maxTableSize: Int, // limits computation effort, reducing result quality
                                           iterationDurationLimit: Long, // limits computation effort, reducing result quality
                                           extraRequirement: ExtraRequirement[Result],
                                           monitor: IDPSolverMonitor) {

  def apply(seed: Seed[Solvable, Boolean, Result], initialToDo: Set[Solvable], context: Context): BestResults[Result] = {
    val registry = registryFactory()
    val table = tableFactory(registry, seed)
    var toDo = registry.registerAll(initialToDo)

    // utility functions
    val goalSelector: Selector[((Goal, Boolean), Result)] = projectingSelector.apply[((Goal, Boolean), Result)](_._2, _)

    def generateBestCandidates(maxBlockSize: Int): Int = {
      var largestFinishedIteration = 0
      var blockSize = 1
      var keepGoing = true
      val start = Stopwatch.start()

      while (keepGoing && blockSize <= maxBlockSize) {
        var foundNoCandidate = true
        blockSize += 1
        val goals = toDo.subsets(blockSize)
        while (keepGoing && goals.hasNext) {
          val goal = goals.next()
          if (table(goal).isEmpty) {
            val candidates = LazyIterable(generator(registry, goal, table, context))
            val extraCandidates = candidates.filter(extraRequirement.fulfils)
            // From _all_ candidates (even if they fulfil the requirement), put the best into the table
            // with `false`. We don't want to compare just the ones that do not fulfil the requirement
            // in isolation, because it could be that the best overall candidate fulfils the requirement.
            projectingSelector(candidates).foreach { candidate =>
              foundNoCandidate = false
              table.put(goal, false, candidate)
            }
            // Also add the best candidate from all candidates that fulfil the requirement into the table
            // with `true`.
            projectingSelector(extraCandidates).foreach { candidate =>
              foundNoCandidate = false
              table.put(goal, true, candidate)
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
      val blockCandidates: Iterable[((Goal, Boolean), Result)] = LazyIterable(table.plansOfSize(blockSize)).toIndexedSeq
      val bestInBlock: Option[((Goal, Boolean), Result)] = goalSelector(blockCandidates)
      val ((goal, _), _) = bestInBlock.getOrElse {
        throw new IllegalStateException(
          s"""Found no solution for block with size $blockSize,
             |$blockCandidates were the selected candidates from the table $table""".stripMargin)
      }
      goal
    }

    def compactBlock(original: Goal): Unit = {
      val newId = registry.compact(original)
      table(original).foreach {
        case (attribute, result) =>
          table.put(BitSet.empty + newId, attribute, result)
      }
      toDo = toDo -- original + newId
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
      compactBlock(bestGoal)
    }
    monitor.foundPlanAfter(iterations)

    val (sortedPlans, plans) =  table.plans
      .map { case ((key, attribute), result) => (registry.explode(key), attribute) -> result }
      .partition { case ((_, fulfilsAttribute), _) => fulfilsAttribute }

    val (_, bestResult) = plans
      .toSingleOption
      .getOrElse(throw new AssertionError("Expected a single plan to be left in the plan table"))

    if (sortedPlans.hasNext) {
      val (_, sortedPlan) = sortedPlans.toSingleOption
        .getOrElse(throw new AssertionError("Expected a single sorted plan to be left in the plan table"))

      BestResults(bestResult, Some(sortedPlan))
    } else {
      BestResults(bestResult, None)
    }
  }
}
