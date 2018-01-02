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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical


import org.neo4j.cypher.internal.compiler.v2_3.planner.QueryGraph
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans.LogicalPlan

trait LogicalPlanningFunction0[+B] {
  def apply(implicit context: LogicalPlanningContext): B
}

trait LogicalPlanningFunction1[-A, +B] {
  def apply(input: A)(implicit context: LogicalPlanningContext): B
}

trait LogicalPlanningFunction2[-A1, -A2, +B] {
  def apply(input1: A1, input2: A2)(implicit context: LogicalPlanningContext): B
}

// TODO: Return Iterator
trait CandidateGenerator[T] extends LogicalPlanningFunction2[T, QueryGraph, Seq[LogicalPlan]]

object CandidateGenerator {
  implicit final class RichCandidateGenerator[T](self: CandidateGenerator[T]) {
    def orElse(other: CandidateGenerator[T]): CandidateGenerator[T] = new CandidateGenerator[T] {
      def apply(input1: T, input2: QueryGraph)(implicit context: LogicalPlanningContext): Seq[LogicalPlan] = {
        val ownCandidates = self(input1, input2)
        if (ownCandidates.isEmpty) other(input1, input2) else ownCandidates
      }
    }

    def +||+(other: CandidateGenerator[T]): CandidateGenerator[T] = new CandidateGenerator[T] {
      override def apply(input1: T, input2: QueryGraph)(implicit context: LogicalPlanningContext): Seq[LogicalPlan] =
        self(input1, input2) ++ other(input1, input2)
    }
  }
}

trait PlanTransformer[-T] extends LogicalPlanningFunction2[LogicalPlan, T, LogicalPlan]

trait CandidateSelector extends ProjectingSelector[LogicalPlan]

trait LeafPlanner extends LogicalPlanningFunction1[QueryGraph, Seq[LogicalPlan]]
