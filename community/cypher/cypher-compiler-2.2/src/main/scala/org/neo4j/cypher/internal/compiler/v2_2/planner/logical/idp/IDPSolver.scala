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
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical.idp

import org.neo4j.cypher.internal.compiler.v2_2.helpers.LazyIterable
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.{ProjectingSelector, Selector}

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
                      maxSearchDepth: Int = 5 // limits computation effort by reducing result quality
                     ) {

  def apply(seed: Iterable[(Set[S], P)], initialToDo: Set[S]): Iterator[(Set[S], P)] = {
    val registry = registryFactory()
    val table = IDPTable(registry.registerAll, seed)
    var toDo = registry.registerAll(initialToDo)

    // utility functions

    val identitySelector: Selector[P] = projectingSelector(_)
    val goalSelector: Selector[(Goal, P)] = projectingSelector.apply[(Goal, P)](_._2, _)

    def generateBestCandidates(blockSize: Int) {
      for (i <- 2 to blockSize;
           // If we already have an optimal plan, no need to re-plan
           goal <- toDo.subsets(i) if !table.contains(goal);
           candidates = LazyIterable(generator(registry, goal, table));
           best <- projectingSelector(candidates)) {
        table.put(goal, best)
      }
    }

    def findBestCandidateInBlock(blockSize: Int): (Goal, P) = {
      val blockCandidates: Iterable[(Goal, P)] = LazyIterable(table.plansOfSize(blockSize))
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
      val k = Math.min(toDo.size, maxSearchDepth)
      generateBestCandidates(k)
      val (bestGoal, bestInBlock) = findBestCandidateInBlock(k)
      compactBlock(bestGoal, bestInBlock)
    }

    table.plans.map { case (k, v) => k.flatMap(registry.lookup) -> v }
  }
}

