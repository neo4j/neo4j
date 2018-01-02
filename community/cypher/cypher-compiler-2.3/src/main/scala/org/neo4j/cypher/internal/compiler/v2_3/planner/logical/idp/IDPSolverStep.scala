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

object IDPSolverStep {
  def empty[S, P, C] = new IDPSolverStep[S, P, C] {
    override def apply(registry: IdRegistry[S], goal: Goal, cache: IDPCache[P])(implicit context: C): Iterator[P] =
      Iterator.empty
  }
}

trait SolverStep[S, P, C] {
  def apply(registry: IdRegistry[S], goal: Goal, cache: IDPCache[P])(implicit context: C):  Iterator[P]
}

trait IDPSolverStep[S, P, C] extends SolverStep[S, P, C] {
  self =>

  def map(f: P => P) = new IDPSolverStep[S, P, C] {
    override def apply(registry: IdRegistry[S], goal: Goal, cache: IDPCache[P])(implicit context: C): Iterator[P] =
      self(registry, goal, cache).map(f)
  }

  def ++(next: IDPSolverStep[S, P, C]) = new IDPSolverStep[S, P, C] {
    override def apply(registry: IdRegistry[S], goal: Goal, cache: IDPCache[P])(implicit context: C): Iterator[P] =
      self(registry, goal, cache) ++ next(registry, goal, cache)
  }
}
