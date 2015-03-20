/**
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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical.idp

import org.neo4j.cypher.internal.compiler.v2_3.helpers.LazyIterable
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.{LogicalPlanningContext, Selector, ProjectingSelector}

import scala.collection.immutable.BitSet

/**
 * Based on the main loop of the IDP1 algorithm described in the paper
 *
 *   "Iterative Dynamic Programming: A New Class of Query Optimization Algorithms"
 *
 * written by Donald Kossmann and Konrad Stocker
 */
class IDPSolver[S, P](generator: IDPSolverStep[S, P], // generates candidates at each step
                      projectingSelector: ProjectingSelector[P], // pick best from a set of candidates
                      registryFactory: () => IdRegistry[S] = () => IdRegistry[S], // maps from Set[S] to BitSet
                      tableFactory: (IdRegistry[S], Seed[S, P]) => IDPTable[P] = (registry: IdRegistry[S], seed: Seed[S, P]) => IDPTable(registry, seed),
                      maxTableSize: Int // limits computation effort by reducing result quality
                     ) {

  def apply(seed: Seed[S, P], initialToDo: Set[S])(implicit context: LogicalPlanningContext): Iterator[(Set[S], P)] = {
    val registry = registryFactory()
    val table = tableFactory(registry, seed)
    var toDo = registry.registerAll(initialToDo)

    // utility functions

    val identitySelector: Selector[P] = projectingSelector(_)
    val goalSelector: Selector[(Goal, P)] = projectingSelector.apply[(Goal, P)](_._2, _)

    def generateBestCandidates(maxTableSize: Int, maxBlockSize: Int): Int = {
      var lastStarted = 1
      var keepGoing = true

      while (keepGoing && lastStarted <= maxBlockSize) {
        lastStarted += 1
        val goals = toDo.subsets(lastStarted)
        while (keepGoing && goals.hasNext) {
          val goal = goals.next()
          if (!table.contains(goal)) {
            val candidates = LazyIterable(generator(registry, goal, table))
            projectingSelector(candidates).foreach(table.put(goal, _))
            keepGoing = lastStarted == 2 || table.size <= maxTableSize
          }
        }
      }
      lastStarted - 1
    }

    def findBestCandidateInBlock(blockSize: Int): (Goal, P) = {
      val blockCandidates: Iterable[(Goal, P)] = LazyIterable(table.plansOfSize(blockSize)).toSeq
      val bestInBlock = goalSelector(blockCandidates)
      bestInBlock.getOrElse(throw new IllegalStateException("Found no solution for block"))
    }

    def compactBlock(original: Goal, product: P): Unit = {
      val newId = registry.compact(original)
      table.put(BitSet.empty + newId, product)
      toDo = toDo -- original + newId
      table.removeAllTracesOf(original)
    }

    // actual algorithm

    while (toDo.size > 1) {
      val largestFinished = generateBestCandidates(maxTableSize, toDo.size)
      val (bestGoal, bestInBlock) = findBestCandidateInBlock(largestFinished)
      compactBlock(bestGoal, bestInBlock)
    }

    table.plans.map { case (k, v) => registry.explode(k) -> v }
  }
}

