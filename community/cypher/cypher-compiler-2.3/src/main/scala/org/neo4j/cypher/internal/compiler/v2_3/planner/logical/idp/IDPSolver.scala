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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical.idp

import org.neo4j.cypher.internal.compiler.v2_3.helpers.LazyIterable
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.{ProjectingSelector, Selector}

import scala.collection.immutable.BitSet

trait IDPSolverMonitor {
  def startIteration(iteration: Int)
  def endIteration(iteration: Int, depth: Int, tableSize: Int)
  def foundPlanAfter(iterations: Int)
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
                         monitor: IDPSolverMonitor) {

  def apply(seed: Seed[Solvable, Result], initialToDo: Set[Solvable])(implicit context: Context): Iterator[(Set[Solvable], Result)] = {
    val registry = registryFactory()
    val table = tableFactory(registry, seed)
    var toDo = registry.registerAll(initialToDo)

    // utility functions
    val goalSelector: Selector[(Goal, Result)] = projectingSelector.apply[(Goal, Result)](_._2, _)

    def generateBestCandidates(maxBlockSize: Int): Int = {
      var blockSize = 1
      var keepGoing = true
      val start = System.currentTimeMillis()

      while (keepGoing && blockSize <= maxBlockSize) {
        blockSize += 1
        val goals = toDo.subsets(blockSize)
        while (keepGoing && goals.hasNext) {
          val goal = goals.next()
          if (!table.contains(goal)) {
            val candidates = LazyIterable(generator(registry, goal, table))
            projectingSelector(candidates).foreach(table.put(goal, _))
            keepGoing = blockSize == 2 ||
              (table.size <= maxTableSize && (System.currentTimeMillis() - start) < iterationDurationLimit)
          }
        }
      }
      blockSize - 1
    }

    def findBestCandidateInBlock(blockSize: Int): (Goal, Result) = {
      val blockCandidates: Iterable[(Goal, Result)] = LazyIterable(table.plansOfSize(blockSize)).toSeq
      val bestInBlock = goalSelector(blockCandidates)
      bestInBlock.getOrElse(throw new IllegalStateException("Found no solution for block"))
    }

    def compactBlock(original: Goal, candidate: Result): Unit = {
      val newId = registry.compact(original)
      table.put(BitSet.empty + newId, candidate)
      toDo = toDo -- original + newId
      table.removeAllTracesOf(original)
    }

    // actual algorithm

    var iterations = 0

    while (toDo.size > 1) {
      iterations += 1
      monitor.startIteration(iterations)
      val largestFinished = generateBestCandidates(toDo.size)
      val (bestGoal, bestInBlock) = findBestCandidateInBlock(largestFinished)
      monitor.endIteration(iterations, largestFinished, table.size)
      compactBlock(bestGoal, bestInBlock)
    }
    monitor.foundPlanAfter(iterations)

    table.plans.map { case (k, v) => registry.explode(k) -> v}
  }
}

