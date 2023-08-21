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

import scala.collection.GenTraversableOnce

object IDPSolverStep {

  def empty[S, P, C] = new IDPSolverStep[S, P, C] {

    override def apply(registry: IdRegistry[S], goal: Goal, cache: IDPCache[P], context: C): Iterator[P] =
      Iterator.empty
  }
}

trait SolverStep[S, P, C] {
  def apply(registry: IdRegistry[S], goal: Goal, cache: IDPCache[P], context: C): Iterator[P]
}

trait IDPSolverStep[S, P, C] extends SolverStep[S, P, C] {
  self =>

  def map(f: P => P): IDPSolverStep[S, P, C] =
    (registry: IdRegistry[S], goal: Goal, cache: IDPCache[P], context: C) => self(registry, goal, cache, context).map(f)

  def flatMap(f: P => GenTraversableOnce[P]): IDPSolverStep[S, P, C] =
    (registry: IdRegistry[S], goal: Goal, cache: IDPCache[P], context: C) =>
      self(registry, goal, cache, context).flatMap(f)

  def ++(next: IDPSolverStep[S, P, C]): IDPSolverStep[S, P, C] =
    (registry: IdRegistry[S], goal: Goal, cache: IDPCache[P], context: C) =>
      self(registry, goal, cache, context) ++ next(registry, goal, cache, context)

  /**
   * Combines two solver steps. If the first yields results, only produce those.
   * If the first yields no results, produce the results from the alternative.
   */
  def ||(alternative: IDPSolverStep[S, P, C]): IDPSolverStep[S, P, C] =
    (registry: IdRegistry[S], goal: Goal, cache: IDPCache[P], context: C) => {
      val firstResult = self(registry, goal, cache, context)
      if (firstResult.hasNext) {
        firstResult
      } else {
        alternative(registry, goal, cache, context)
      }
    }

  /**
   * Filter which goals should be given to the solver step this method is called on.
   * This method returns a new solver step with the following behavior:
   * If the supplied test function returns `true`, give it to this solver step and return the resulting Iterator.
   * Otherwise, return an empty iterator.
   */
  def filterGoals(test: (IdRegistry[S], Goal) => Boolean): IDPSolverStep[S, P, C] =
    (registry: IdRegistry[S], goal: Goal, cache: IDPCache[P], context: C) => {
      if (test(registry, goal)) {
        self(registry, goal, cache, context)
      } else {
        Iterator.empty
      }
    }
}
