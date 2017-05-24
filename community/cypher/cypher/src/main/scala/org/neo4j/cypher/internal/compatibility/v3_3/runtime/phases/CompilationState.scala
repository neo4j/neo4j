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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.phases

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.executionplan.ExecutionPlan
import org.neo4j.cypher.internal.compiler.v3_3.phases.LogicalPlanState
import org.neo4j.cypher.internal.frontend.v3_3.ast.Statement
import org.neo4j.cypher.internal.frontend.v3_3.phases.{BaseState, Condition}
import org.neo4j.cypher.internal.frontend.v3_3.{InputPosition, PlannerName, SemanticState, SemanticTable}

class CompilationState(ls: LogicalPlanState,
                           val maybeExecutionPlan: Option[ExecutionPlan] = None) extends LogicalPlanState(ls.queryText, ls.startPosition, ls.plannerName, ls.maybeStatement, ls.maybeSemantics, ls.maybeExtractedParams, ls.maybeSemanticTable, ls.maybeUnionQuery, ls.maybeLogicalPlan, ls.maybePeriodicCommit, ls.accumulatedConditions) {

//  override def queryText: String = ls.queryText
//
//  override def startPosition: Option[InputPosition] = ls.startPosition
//
//  override def plannerName: PlannerName = ls.plannerName
//
//  override def maybeStatement: Option[Statement] = ls.maybeStatement
//
//  override def maybeSemantics: Option[SemanticState] = ls.maybeSemantics
//
//  override def maybeExtractedParams: Option[Map[String, Any]] = ls.maybeExtractedParams
//
//  override def maybeSemanticTable: Option[SemanticTable] = ls.maybeSemanticTable
//
//  override def accumulatedConditions: Set[Condition] = ls.accumulatedConditions
//
//  override def withStatement(s: Statement): BaseState = copy(ls = ls.withStatement(s))
//
//  override def withSemanticTable(s: SemanticTable): BaseState = copy(ls = ls.withSemanticTable(s))
//
//  override def withSemanticState(s: SemanticState): BaseState = copy(ls = ls.withSemanticState(s))
//
//  override def withParams(p: Map[String, Any]): BaseState = copy(ls = ls.withParams(p))

  def withMaybeExecutionPlan(e: Option[ExecutionPlan]): CompilationState = new CompilationState(ls, e)
}
