/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.compatibility.v3_5

import org.neo4j.cypher.CypherVersion.v3_5
import org.neo4j.cypher.InternalException
import org.neo4j.cypher.internal.compiler.v3_5.phases.{LogicalPlanState => LogicalPlanStateV3_5}
import org.neo4j.cypher.internal.compiler.v3_5.{UpdateStrategy => UpdateStrategyV3_5}
import org.neo4j.cypher.internal.compiler.v3_5.{CypherPlannerConfiguration => CypherPlannerConfigurationV3_5}
import org.neo4j.cypher.internal.compiler.v4_0.phases.LogicalPlanState
import org.neo4j.cypher.internal.compiler.v4_0.CypherPlannerConfiguration
import org.neo4j.cypher.internal.compiler.v4_0.UpdateStrategy
import org.neo4j.cypher.internal.compiler.v4_0.defaultUpdateStrategy
import org.neo4j.cypher.internal.compiler.v4_0.eagerUpdateStrategy
import org.neo4j.cypher.internal.compiler.{v3_5 => compilerV3_5}
import org.neo4j.cypher.internal.compiler.{v4_0 => compilerV4_0}
import org.neo4j.cypher.internal.ir.{v3_5 => irV3_5}
import org.neo4j.cypher.internal.ir.{v4_0 => irV4_0}
import org.neo4j.cypher.internal.planner.v3_5.spi.{PlanningAttributes => PlanningAttributesV3_5}
import org.neo4j.cypher.internal.planner.v3_5.{spi => spiV3_5}
import org.neo4j.cypher.internal.planner.v4_0.spi.PlanningAttributes.{ProvidedOrders => ProvidedOrdersV4_0}
import org.neo4j.cypher.internal.planner.v4_0.spi.PlanningAttributes.{Cardinalities => CardinalitiesV4_0}
import org.neo4j.cypher.internal.planner.v4_0.spi.PlanningAttributes.{Solveds => SolvedsV4_0}
import org.neo4j.cypher.internal.planner.v4_0.spi._
import org.neo4j.cypher.internal.planner.v4_0.spi.{PlanningAttributes => PlanningAttributesV4_0}
import org.neo4j.cypher.internal.v4_0.ast.semantics.{SemanticTable => SemanticTableV4_0}
import org.neo4j.cypher.internal.v4_0.frontend.PlannerName
import org.neo4j.cypher.internal.v4_0.frontend.phases.CompilationPhaseTracer
import org.neo4j.cypher.internal.v4_0.frontend.phases.CompilationPhaseTracer.{CompilationPhase => v4_0Phase}
import org.neo4j.cypher.internal.v4_0.logical.plans.{LogicalPlan => LogicalPlanV4_0}
import org.neo4j.cypher.internal.v4_0.util.attribution.Id
import org.neo4j.cypher.internal.v4_0.util.Cardinality
import org.neo4j.cypher.internal.v4_0.util.InputPosition
import org.neo4j.cypher.internal.v4_0.{ast => astV4_0}
import org.neo4j.cypher.internal.v4_0.{util => nfV4_0}
import org.neo4j.kernel.impl.query.QueryExecutionMonitor
import org.neo4j.kernel.impl.query.TransactionalContext
import org.neo4j.cypher.internal.v3_5.expressions.{Expression => ExpressionV3_5}
import org.neo4j.cypher.internal.v3_5.frontend.phases.CompilationPhaseTracer.{CompilationPhase => v3_5Phase}
import org.neo4j.cypher.internal.v3_5.frontend.phases.{CompilationPhaseTracer => CompilationPhaseTracerV3_5}
import org.neo4j.cypher.internal.v3_5.frontend.{PlannerName => PlannerNameV3_5}
import org.neo4j.cypher.internal.v3_5.util.attribution.{Id => IdV3_5}
import org.neo4j.cypher.internal.v3_5.util.{InputPosition => InputPositionV3_5}
import org.neo4j.cypher.internal.v3_5.util.{Cardinality => CardinalityV3_5}
import org.neo4j.cypher.internal.v3_5.{ast => astV3_5}
import org.neo4j.cypher.internal.v3_5.{util => nfV3_5}
import org.neo4j.cypher.internal.v4_0.expressions.{Expression, Property, PropertyKeyName, Variable}

object helpers {
  implicit def monitorFailure(t: Throwable)(implicit monitor: QueryExecutionMonitor, tc: TransactionalContext): Unit = {
    monitor.endFailure(tc.executingQuery(), t)
  }

  def as3_5(config: CypherPlannerConfiguration): CypherPlannerConfigurationV3_5 =
    CypherPlannerConfigurationV3_5(
      config.queryCacheSize,
      as3_5(config.statsDivergenceCalculator),
      config.useErrorsOverWarnings,
      config.idpMaxTableSize,
      config.idpIterationDuration,
      config.errorIfShortestPathFallbackUsedAtRuntime,
      config.errorIfShortestPathHasCommonNodesAtRuntime,
      config.legacyCsvQuoteEscaping,
      config.csvBufferSize,
      config.nonIndexedLabelWarningThreshold,
      config.planWithMinimumCardinalityEstimates,
      lenientCreateRelationship = false)

  /** This is awful but needed until 3_0 is updated no to send in the tracer here */
  def as3_5(tracer: CompilationPhaseTracer): CompilationPhaseTracerV3_5 = {
    new CompilationPhaseTracerV3_5 {
      override def beginPhase(phase: CompilationPhaseTracerV3_5.CompilationPhase): CompilationPhaseTracerV3_5.CompilationPhaseEvent = {
        val wrappedPhase = phase match {
          case v3_5Phase.AST_REWRITE => v4_0Phase.AST_REWRITE
          case v3_5Phase.CODE_GENERATION => v4_0Phase.CODE_GENERATION
          case v3_5Phase.LOGICAL_PLANNING => v4_0Phase.LOGICAL_PLANNING
          case v3_5Phase.PARSING => v4_0Phase.PARSING
          case v3_5Phase.PIPE_BUILDING => v4_0Phase.PIPE_BUILDING
          case v3_5Phase.SEMANTIC_CHECK => v4_0Phase.SEMANTIC_CHECK
          case v3_5Phase.DEPRECATION_WARNINGS => v4_0Phase.DEPRECATION_WARNINGS
          case _ => throw new InternalException(s"Cannot handle $phase in 3.3")
        }

        val wrappedEvent = tracer.beginPhase(wrappedPhase)

        new CompilationPhaseTracerV3_5.CompilationPhaseEvent {
          override def close(): Unit = wrappedEvent.close()
        }
      }
    }
  }

  def as3_5(calc: compilerV4_0.StatsDivergenceCalculator): compilerV3_5.StatsDivergenceCalculator = calc match {
    case compilerV4_0.StatsDivergenceInverseDecayCalculator(initialThreshold, targetThreshold, initialMillis, targetMillis) =>
      compilerV3_5.StatsDivergenceInverseDecayCalculator(initialThreshold, targetThreshold, initialMillis, targetMillis)
    case compilerV4_0.StatsDivergenceExponentialDecayCalculator(initialThreshold, targetThreshold, initialMillis, targetMillis) =>
      compilerV3_5.StatsDivergenceExponentialDecayCalculator(initialThreshold, targetThreshold, initialMillis, targetMillis)
    case compilerV4_0.StatsDivergenceNoDecayCalculator(initialThreshold, initialMillis) =>
      compilerV3_5.StatsDivergenceNoDecayCalculator(initialThreshold, initialMillis)
  }

  def as3_5(pos: InputPosition): InputPositionV3_5 = InputPositionV3_5(pos.offset, pos.line, pos.column)

  def as4_0(pos: InputPositionV3_5): InputPosition = if(pos == null) null else InputPosition(pos.offset, pos.line, pos.column)

  def as4_0(planId: IdV3_5) : Id = Id(planId.x)

  def as4_0(plannerName: PlannerNameV3_5) : PlannerName = plannerName match {
    case spiV3_5.IDPPlannerName => IDPPlannerName
    case spiV3_5.DPPlannerName => DPPlannerName
    case spiV3_5.ProcedurePlannerName => ProcedurePlannerName
  }

  def as4_0(updateStrategy: UpdateStrategyV3_5): UpdateStrategy = updateStrategy match {
    case org.neo4j.cypher.internal.compiler.v3_5.eagerUpdateStrategy => eagerUpdateStrategy
    case org.neo4j.cypher.internal.compiler.v3_5.defaultUpdateStrategy => defaultUpdateStrategy
  }

  def as4_0(periodicCommit: irV3_5.PeriodicCommit): irV4_0.PeriodicCommit = {
    irV4_0.PeriodicCommit(periodicCommit.batchSize)
  }

  def as4_0(cardinality: CardinalityV3_5) : Cardinality = {
    Cardinality(cardinality.amount)
  }

  def as4_0(statement: astV3_5.Statement) : astV4_0.Statement = StatementWrapper(statement)

  def as4_0(providedOrder: irV3_5.ProvidedOrder) : irV4_0.ProvidedOrder = irV4_0.ProvidedOrder(providedOrder.columns.map(as4_0))

  def as4_0(column: irV3_5.ProvidedOrder.Column) : irV4_0.ProvidedOrder.Column = column match {
    // Since 3.5 only provides a string representation of the expression, the real position is not known
    case irV3_5.ProvidedOrder.Asc(id) => irV4_0.ProvidedOrder.Asc(Variable(id)(InputPosition.NONE))
    case irV3_5.ProvidedOrder.Desc(id) => irV4_0.ProvidedOrder.Desc(Variable(id)(InputPosition.NONE))
  }

  def as4_0(notification: nfV3_5.InternalNotification): nfV4_0.InternalNotification = notification match {
    case nfV3_5.DeprecatedStartNotification(position, alternativeQuery) => nfV4_0.DeprecatedStartNotification(as4_0(position), alternativeQuery)
    case nfV3_5.CartesianProductNotification(position, isolatedVariables) => nfV4_0.CartesianProductNotification(as4_0(position), isolatedVariables)
    case nfV3_5.LengthOnNonPathNotification(position) => nfV4_0.LengthOnNonPathNotification(as4_0(position))
    case compilerV3_5.PlannerUnsupportedNotification => compilerV4_0.PlannerUnsupportedNotification
    case compilerV3_5.RuntimeUnsupportedNotification => compilerV4_0.RuntimeUnsupportedNotification
    case compilerV3_5.IndexHintUnfulfillableNotification(label, propertyKeys) => compilerV4_0.IndexHintUnfulfillableNotification(label, propertyKeys)
    case compilerV3_5.JoinHintUnfulfillableNotification(identified) => compilerV4_0.JoinHintUnfulfillableNotification(identified)
    case compilerV3_5.JoinHintUnsupportedNotification(identified) => compilerV4_0.JoinHintUnsupportedNotification(identified)
    case compilerV3_5.IndexLookupUnfulfillableNotification(labels) => compilerV4_0.IndexLookupUnfulfillableNotification(labels)
    case compilerV3_5.EagerLoadCsvNotification => compilerV4_0.EagerLoadCsvNotification
    case compilerV3_5.LargeLabelWithLoadCsvNotification => compilerV4_0.LargeLabelWithLoadCsvNotification
    case compilerV3_5.MissingLabelNotification(position, label) => compilerV4_0.MissingLabelNotification(as4_0(position), label)
    case compilerV3_5.MissingRelTypeNotification(position, relType) => compilerV4_0.MissingRelTypeNotification(as4_0(position), relType)
    case compilerV3_5.MissingPropertyNameNotification(position, name) => compilerV4_0.MissingPropertyNameNotification(as4_0(position), name)
    case nfV3_5.UnboundedShortestPathNotification(position) => nfV4_0.UnboundedShortestPathNotification(as4_0(position))
    case compilerV3_5.ExhaustiveShortestPathForbiddenNotification(position) => compilerV4_0.ExhaustiveShortestPathForbiddenNotification(as4_0(position))
    case nfV3_5.DeprecatedFunctionNotification(position, oldName, newName) => nfV4_0.DeprecatedFunctionNotification(as4_0(position), oldName, newName)
    case compilerV3_5.DeprecatedProcedureNotification(position, oldName, newName) => compilerV4_0.DeprecatedProcedureNotification(as4_0(position), oldName, newName)
    case compilerV3_5.ProcedureWarningNotification(position, procedure, warning) => compilerV4_0.ProcedureWarningNotification(as4_0(position), procedure, warning)
    case compilerV3_5.DeprecatedFieldNotification(position, procedure, field) => compilerV4_0.DeprecatedFieldNotification(as4_0(position), procedure, field)
    case nfV3_5.DeprecatedVarLengthBindingNotification(position, variable) => nfV4_0.DeprecatedVarLengthBindingNotification(as4_0(position), variable)
    case nfV3_5.DeprecatedRelTypeSeparatorNotification(position) => nfV4_0.DeprecatedRelTypeSeparatorNotification(as4_0(position))
    case compilerV3_5.DeprecatedRulePlannerNotification => compilerV4_0.DeprecatedRulePlannerNotification
    case compilerV3_5.ExperimentalFeatureNotification(msg) => compilerV4_0.ExperimentalFeatureNotification(msg)
    case compilerV3_5.SuboptimalIndexForConstainsQueryNotification(label, propertyKeys) => compilerV4_0.SuboptimalIndexForConstainsQueryNotification(label, propertyKeys)
    case compilerV3_5.SuboptimalIndexForEndsWithQueryNotification(label, propertyKeys) => compilerV4_0.SuboptimalIndexForEndsWithQueryNotification(label, propertyKeys)
  }

  def as4_0(logicalPlanState: LogicalPlanStateV3_5) : LogicalPlanState = {
    val startPosition = logicalPlanState.startPosition.map(as4_0)
    // Wrap the planner name to correctly report version 3.5.
    val plannerName = PlannerNameWithVersion(as4_0(logicalPlanState.plannerName), v3_5.name)

    val planningAttributes4_0 = PlanningAttributesV4_0(new SolvedsV4_0, new CardinalitiesV4_0, new ProvidedOrdersV4_0)
    val (plan3_5, semanticTable3_5) = convertLogicalPlan(logicalPlanState, logicalPlanState.planningAttributes, planningAttributes4_0)

    val statement3_5 = logicalPlanState.maybeStatement.get

    LogicalPlanState(logicalPlanState.queryText,
      startPosition,
      plannerName,
      planningAttributes4_0,
      Some(as4_0(statement3_5)),
      None,
      logicalPlanState.maybeExtractedParams,
      Some(semanticTable3_5),
      None,
      Some(plan3_5),
      Some(logicalPlanState.maybePeriodicCommit.flatten.map(x => as4_0(x))),
      Set.empty)
  }

  private def convertLogicalPlan(logicalPlanState: LogicalPlanStateV3_5,
                                 planningAttributes3_5: PlanningAttributesV3_5,
                                 planningAttributes4_0: PlanningAttributesV4_0): (LogicalPlanV4_0, SemanticTableV4_0) = {

    def seenBySemanticTable(expression: ExpressionV3_5) : Boolean =
      logicalPlanState.maybeSemanticTable.exists(_.seen(expression))

    val idConverter = new MaxIdConverter
    val (plan3_5, expressionMap) =
      LogicalPlanConverter.convertLogicalPlan[LogicalPlanV4_0](
        logicalPlanState.maybeLogicalPlan.get,
        planningAttributes3_5,
        planningAttributes4_0,
        idConverter,
        seenBySemanticTable
      )

    val maybeTable = logicalPlanState.maybeSemanticTable
    val semanticTable = if (maybeTable.isDefined) {
      SemanticTableConverter.convertSemanticTable(maybeTable.get, expressionMap)
    } else {
      new SemanticTableV4_0()
    }
    (plan3_5, semanticTable)
  }
}
