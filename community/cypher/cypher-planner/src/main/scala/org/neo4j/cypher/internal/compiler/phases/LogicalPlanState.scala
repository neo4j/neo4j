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
package org.neo4j.cypher.internal.compiler.phases

import org.neo4j.cypher.internal.ast.Query
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.expressions.AutoExtractedParameter
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.frontend.PlannerName
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.ir.PlannerQuery
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.options.CypherEagerAnalyzerOption
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.ObfuscationMetadata
import org.neo4j.cypher.internal.util.StepSequencer

/*
This is the state that is used during query compilation. It accumulates more and more values as it passes through
the compiler pipe line, finally ending up containing a logical plan.

Normally, it is created with only the first three params as given, and the rest is built up while passing through
the pipe line
 */
case class LogicalPlanState(
  queryText: String,
  plannerName: PlannerName,
  planningAttributes: PlanningAttributes,
  anonymousVariableNameGenerator: AnonymousVariableNameGenerator,
  maybeProcedureSignatureVersion: Option[Long] = None,
  maybeStatement: Option[Statement] = None,
  maybeSemantics: Option[SemanticState] = None,
  maybeExtractedParams: Option[Map[AutoExtractedParameter, Expression]] = None,
  maybeSemanticTable: Option[SemanticTable] = None,
  maybeQuery: Option[PlannerQuery] = None,
  maybeLogicalPlan: Option[LogicalPlan] = None,
  accumulatedConditions: Set[StepSequencer.Condition] = Set.empty,
  hasLoadCSV: Boolean = false,
  maybeReturnColumns: Option[Seq[String]] = None,
  maybeObfuscationMetadata: Option[ObfuscationMetadata] = None,
  maybeEagerAnalyzerOption: Option[CypherEagerAnalyzerOption] = None
) extends BaseState {

  def query: PlannerQuery = maybeQuery getOrElse fail("The planner query")
  def logicalPlan: LogicalPlan = maybeLogicalPlan getOrElse fail("Logical plan")
  def astAsQuery: Query = statement().asInstanceOf[Query]

  def asCachableLogicalPlanState(): CachableLogicalPlanState = {
    CachableLogicalPlanState(
      queryText,
      plannerName,
      planningAttributes,
      anonymousVariableNameGenerator,
      statement(),
      semanticTable(),
      logicalPlan,
      hasLoadCSV,
      returnColumns()
    )
  }

  override def withStatement(s: Statement): LogicalPlanState = copy(maybeStatement = Some(s))
  override def withReturnColumns(cols: Seq[String]): LogicalPlanState = copy(maybeReturnColumns = Some(cols))
  override def withSemanticTable(s: SemanticTable): LogicalPlanState = copy(maybeSemanticTable = Some(s))
  override def withSemanticState(s: SemanticState): LogicalPlanState = copy(maybeSemantics = Some(s))

  override def withParams(p: Map[AutoExtractedParameter, Expression]): LogicalPlanState =
    copy(maybeExtractedParams = Some(p))

  override def withObfuscationMetadata(o: ObfuscationMetadata): LogicalPlanState =
    copy(maybeObfuscationMetadata = Some(o))

  def withMaybeLogicalPlan(p: Option[LogicalPlan]): LogicalPlanState = copy(maybeLogicalPlan = p)
  def withMaybeQuery(q: Option[PlannerQuery]): LogicalPlanState = copy(maybeQuery = q)

  def withNewPlanningAttributes(attributes: PlanningAttributes): LogicalPlanState =
    copy(planningAttributes = attributes)

  override def withProcedureSignatureVersion(generation: Option[Long]): LogicalPlanState =
    copy(maybeProcedureSignatureVersion = generation)
}

object LogicalPlanState {

  def apply(state: BaseState): LogicalPlanState =
    LogicalPlanState(
      queryText = state.queryText,
      plannerName = state.plannerName,
      planningAttributes = PlanningAttributes.newAttributes,
      maybeProcedureSignatureVersion = state.maybeProcedureSignatureVersion,
      maybeStatement = state.maybeStatement,
      maybeSemantics = state.maybeSemantics,
      maybeExtractedParams = state.maybeExtractedParams,
      maybeSemanticTable = state.maybeSemanticTable,
      accumulatedConditions = state.accumulatedConditions,
      maybeReturnColumns = state.maybeReturnColumns,
      maybeObfuscationMetadata = state.maybeObfuscationMetadata,
      anonymousVariableNameGenerator = state.anonymousVariableNameGenerator
    )
}

/**
 * A subset of the data stored in LogicalPlanState.
 */
case class CachableLogicalPlanState(
  queryText: String,
  plannerName: PlannerName,
  planningAttributes: PlanningAttributes,
  anonymousVariableNameGenerator: AnonymousVariableNameGenerator,
  statement: Statement,
  semanticTable: SemanticTable,
  logicalPlan: LogicalPlan,
  hasLoadCSV: Boolean = false,
  returnColumns: Seq[String]
)
