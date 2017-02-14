/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_2.phases

import org.neo4j.cypher.internal.compiler.v3_2.executionplan.ExecutionPlan
import org.neo4j.cypher.internal.compiler.v3_2.planner.UnionQuery
import org.neo4j.cypher.internal.compiler.v3_2.planner.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.frontend.v3_2.ast.{Query, Statement}
import org.neo4j.cypher.internal.frontend.v3_2.phases.{BaseState, Condition}
import org.neo4j.cypher.internal.frontend.v3_2.{InputPosition, InternalException, PlannerName, SemanticState, SemanticTable}
import org.neo4j.cypher.internal.ir.v3_2.PeriodicCommit

/*
This is the state that is used during query compilation. It accumulates more and more values as it passes through
the compiler pipe line, finally ending up containing an execution plan.

Normally, it is created with only the first three params as given, and the rest is built up while passing through
the pipe line
 */
case class CompilationState(queryText: String,
                            startPosition: Option[InputPosition],
                            plannerName: PlannerName,
                            maybeStatement: Option[Statement] = None,
                            maybeSemantics: Option[SemanticState] = None,
                            maybeExtractedParams: Option[Map[String, Any]] = None,
                            maybeSemanticTable: Option[SemanticTable] = None,
                            maybeExecutionPlan: Option[ExecutionPlan] = None,
                            maybeUnionQuery: Option[UnionQuery] = None,
                            maybeLogicalPlan: Option[LogicalPlan] = None,
                            maybePeriodicCommit: Option[Option[PeriodicCommit]] = None,
                            accumulatedConditions: Set[Condition] = Set.empty) extends BaseState {

  def executionPlan = maybeExecutionPlan getOrElse fail("Execution plan")
  def unionQuery = maybeUnionQuery getOrElse fail("Union query")
  def logicalPlan = maybeLogicalPlan getOrElse fail("Logical plan")
  def periodicCommit = maybePeriodicCommit getOrElse fail("Periodic commit")
  def astAsQuery = statement.asInstanceOf[Query]
}

object CompilationState {
  def apply(state: BaseState): CompilationState =
    CompilationState(queryText = state.queryText,
      startPosition = state.startPosition,
      plannerName = state.plannerName,
      maybeStatement = state.maybeStatement,
      maybeSemantics = state.maybeSemantics,
      maybeExtractedParams = state.maybeExtractedParams,
      maybeSemanticTable = state.maybeSemanticTable,
      accumulatedConditions = state.accumulatedConditions)
}


