/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.cypher.internal.compatibility.v3_3

import org.neo4j.cypher.CypherVersion.v3_3
import org.neo4j.cypher.InternalException
import org.neo4j.cypher.internal.compiler.v3_3.phases.{LogicalPlanState => LogicalPlanStateV3_3}
import org.neo4j.cypher.internal.compiler.v3_3.{CypherCompilerConfiguration => CypherCompilerConfiguration3_3, DPPlannerName => DPPlannerNameV3_3, IDPPlannerName => IDPPlannerNameV3_3, ProcedurePlannerName => ProcedurePlannerNameV3_3, UpdateStrategy => UpdateStrategyV3_3}
import org.neo4j.cypher.internal.compiler.v3_5.phases.LogicalPlanState
import org.neo4j.cypher.internal.compiler.v3_5.{CypherPlannerConfiguration, UpdateStrategy, defaultUpdateStrategy, eagerUpdateStrategy}
import org.neo4j.cypher.internal.compiler.{v3_5 => compilerv3_5}
import org.neo4j.cypher.internal.frontend.v3_3.ast.{Expression => ExpressionV3_3}
import org.neo4j.cypher.internal.frontend.v3_3.phases.CompilationPhaseTracer.{CompilationPhase => v3_3Phase}
import org.neo4j.cypher.internal.frontend.v3_3.phases.{CompilationPhaseTracer => CompilationPhaseTracer3_3}
import org.neo4j.cypher.internal.frontend.v3_3.{InputPosition => InputPositionV3_3, PlannerName => PlannerNameV3_3, ast => astV3_3, notification => nfV3_3, phases => phasesV3_3}
import org.neo4j.cypher.internal.ir.v3_3.{Cardinality => CardinalityV3_3}
import org.neo4j.cypher.internal.ir.{v3_3 => irV3_3, v3_5 => irv3_5}
import org.neo4j.cypher.internal.planner.v3_5.spi.PlanningAttributes.{Cardinalities, Solveds}
import org.neo4j.cypher.internal.planner.v3_5.spi.{DPPlannerName, IDPPlannerName, PlannerNameWithVersion, ProcedurePlannerName}
import org.neo4j.cypher.internal.v3_3.logical.plans.{LogicalPlanId => LogicalPlanIdV3_3}
import org.neo4j.cypher.internal.v3_5.logical.plans.{LogicalPlan => LogicalPlanv3_5}
import org.neo4j.kernel.impl.query.{QueryExecutionMonitor, TransactionalContext}
import org.opencypher.v9_0.ast.semantics.{SemanticTable => SemanticTablev3_5}
import org.opencypher.v9_0.frontend.PlannerName
import org.opencypher.v9_0.frontend.phases.CompilationPhaseTracer
import org.opencypher.v9_0.frontend.phases.CompilationPhaseTracer.{CompilationPhase => v3_5Phase}
import org.opencypher.v9_0.util.attribution.{Attributes, Id}
import org.opencypher.v9_0.util.{Cardinality, InputPosition}
import org.opencypher.v9_0.{ast => astv3_5, util => nfv3_5}

object helpers {
  implicit def monitorFailure(t: Throwable)(implicit monitor: QueryExecutionMonitor, tc: TransactionalContext): Unit = {
    monitor.endFailure(tc.executingQuery(), t)
  }

  def as3_3(config: CypherPlannerConfiguration): CypherCompilerConfiguration3_3 =
    CypherCompilerConfiguration3_3(
      config.queryCacheSize,
      as3_3(config.statsDivergenceCalculator),
      config.useErrorsOverWarnings,
      config.idpMaxTableSize,
      config.idpIterationDuration,
      config.errorIfShortestPathFallbackUsedAtRuntime,
      config.errorIfShortestPathHasCommonNodesAtRuntime,
      config.legacyCsvQuoteEscaping,
      config.nonIndexedLabelWarningThreshold)

  /** This is awful but needed until 3_0 is updated no to send in the tracer here */
  def as3_3(tracer: CompilationPhaseTracer): CompilationPhaseTracer3_3 = {
    new CompilationPhaseTracer3_3 {
      override def beginPhase(phase: CompilationPhaseTracer3_3.CompilationPhase): CompilationPhaseTracer3_3.CompilationPhaseEvent = {
        val wrappedPhase = phase match {
          case v3_3Phase.AST_REWRITE => v3_5Phase.AST_REWRITE
          case v3_3Phase.CODE_GENERATION => v3_5Phase.CODE_GENERATION
          case v3_3Phase.LOGICAL_PLANNING => v3_5Phase.LOGICAL_PLANNING
          case v3_3Phase.PARSING => v3_5Phase.PARSING
          case v3_3Phase.PIPE_BUILDING => v3_5Phase.PIPE_BUILDING
          case v3_3Phase.SEMANTIC_CHECK => v3_5Phase.SEMANTIC_CHECK
          case v3_3Phase.DEPRECATION_WARNINGS => v3_5Phase.DEPRECATION_WARNINGS
          case _ => throw new InternalException(s"Cannot handle $phase in 3.3")
        }

        val wrappedEvent = tracer.beginPhase(wrappedPhase)

        new CompilationPhaseTracer3_3.CompilationPhaseEvent {
          override def close(): Unit = wrappedEvent.close()
        }
      }
    }
  }

  def as3_3(calc: compilerv3_5.StatsDivergenceCalculator): phasesV3_3.StatsDivergenceCalculator = calc match {
    case compilerv3_5.StatsDivergenceInverseDecayCalculator(initialThreshold, targetThreshold, initialMillis, targetMillis) =>
      phasesV3_3.StatsDivergenceInverseDecayCalculator(initialThreshold, targetThreshold, initialMillis, targetMillis)
    case compilerv3_5.StatsDivergenceExponentialDecayCalculator(initialThreshold, targetThreshold, initialMillis, targetMillis) =>
      phasesV3_3.StatsDivergenceExponentialDecayCalculator(initialThreshold, targetThreshold, initialMillis, targetMillis)
    case compilerv3_5.StatsDivergenceNoDecayCalculator(initialThreshold, initialMillis) =>
      phasesV3_3.StatsDivergenceNoDecayCalculator(initialThreshold, initialMillis)
  }

  def as3_3(pos: InputPosition): InputPositionV3_3 = InputPositionV3_3(pos.offset, pos.line, pos.column)

  def as3_5(pos: InputPositionV3_3): InputPosition = if(pos == null) null else InputPosition(pos.offset, pos.line, pos.column)

  def as3_5(planId: LogicalPlanIdV3_3) : Id = Id(planId.underlying)

  def as3_5(plannerName: PlannerNameV3_3) : PlannerName = plannerName match {
    case IDPPlannerNameV3_3 => IDPPlannerName
    case DPPlannerNameV3_3 => DPPlannerName
    case ProcedurePlannerNameV3_3 => ProcedurePlannerName
  }

  def as3_5(updateStrategy: UpdateStrategyV3_3): UpdateStrategy = updateStrategy match {
    case org.neo4j.cypher.internal.compiler.v3_3.eagerUpdateStrategy => eagerUpdateStrategy
    case org.neo4j.cypher.internal.compiler.v3_3.defaultUpdateStrategy => defaultUpdateStrategy
  }

  def as3_5(periodicCommit: irV3_3.PeriodicCommit): irv3_5.PeriodicCommit = {
    irv3_5.PeriodicCommit(periodicCommit.batchSize)
  }

  def as3_5(cardinality: CardinalityV3_3) : Cardinality = {
    Cardinality(cardinality.amount)
  }

  def as3_5(statement: astV3_3.Statement) : astv3_5.Statement = StatementWrapper(statement)

  def as3_5(notification: nfV3_3.InternalNotification): nfv3_5.InternalNotification = notification match {
    case nfV3_3.DeprecatedStartNotification(position, alternativeQuery) => nfv3_5.DeprecatedStartNotification(as3_5(position), alternativeQuery)
    case nfV3_3.CartesianProductNotification(position, isolatedVariables) => nfv3_5.CartesianProductNotification(as3_5(position), isolatedVariables)
    case nfV3_3.LengthOnNonPathNotification(position) => nfv3_5.LengthOnNonPathNotification(as3_5(position))
    case nfV3_3.PlannerUnsupportedNotification => compilerv3_5.PlannerUnsupportedNotification
    case nfV3_3.RuntimeUnsupportedNotification => compilerv3_5.RuntimeUnsupportedNotification
    case nfV3_3.IndexHintUnfulfillableNotification(label, propertyKeys) => compilerv3_5.IndexHintUnfulfillableNotification(label, propertyKeys)
    case nfV3_3.JoinHintUnfulfillableNotification(identified) => compilerv3_5.JoinHintUnfulfillableNotification(identified)
    case nfV3_3.JoinHintUnsupportedNotification(identified) => compilerv3_5.JoinHintUnsupportedNotification(identified)
    case nfV3_3.IndexLookupUnfulfillableNotification(labels) => compilerv3_5.IndexLookupUnfulfillableNotification(labels)
    case nfV3_3.EagerLoadCsvNotification => compilerv3_5.EagerLoadCsvNotification
    case nfV3_3.LargeLabelWithLoadCsvNotification => compilerv3_5.LargeLabelWithLoadCsvNotification
    case nfV3_3.MissingLabelNotification(position, label) => compilerv3_5.MissingLabelNotification(as3_5(position), label)
    case nfV3_3.MissingRelTypeNotification(position, relType) => compilerv3_5.MissingRelTypeNotification(as3_5(position), relType)
    case nfV3_3.MissingPropertyNameNotification(position, name) => compilerv3_5.MissingPropertyNameNotification(as3_5(position), name)
    case nfV3_3.UnboundedShortestPathNotification(position) => nfv3_5.UnboundedShortestPathNotification(as3_5(position))
    case nfV3_3.ExhaustiveShortestPathForbiddenNotification(position) => compilerv3_5.ExhaustiveShortestPathForbiddenNotification(as3_5(position))
    case nfV3_3.DeprecatedFunctionNotification(position, oldName, newName) => nfv3_5.DeprecatedFunctionNotification(as3_5(position), oldName, newName)
    case nfV3_3.DeprecatedProcedureNotification(position, oldName, newName) => compilerv3_5.DeprecatedProcedureNotification(as3_5(position), oldName, newName)
    case nfV3_3.ProcedureWarningNotification(position, procedure, warning) => compilerv3_5.ProcedureWarningNotification(as3_5(position), procedure, warning)
    case nfV3_3.DeprecatedFieldNotification(position, procedure, field) => compilerv3_5.DeprecatedFieldNotification(as3_5(position), procedure, field)
    case nfV3_3.DeprecatedVarLengthBindingNotification(position, variable) => nfv3_5.DeprecatedVarLengthBindingNotification(as3_5(position), variable)
    case nfV3_3.DeprecatedRelTypeSeparatorNotification(position) => nfv3_5.DeprecatedRelTypeSeparatorNotification(as3_5(position))
    case nfV3_3.DeprecatedPlannerNotification => compilerv3_5.DeprecatedPlannerNotification
  }

  def as3_5(logicalPlanState: LogicalPlanStateV3_3) : LogicalPlanState = {
    val startPosition = logicalPlanState.startPosition.map(as3_5)
    // Wrap the planner name to correctly report version 3.3.
    val plannerName = PlannerNameWithVersion(as3_5(logicalPlanState.plannerName), v3_3.name)

    val solveds = new Solveds
    val cardinalities = new Cardinalities
    val (plan3_4, semanticTable3_4) = convertLogicalPlan(logicalPlanState, solveds, cardinalities)

    val statement3_3 = logicalPlanState.maybeStatement.get

    LogicalPlanState(logicalPlanState.queryText,
                     startPosition,
                     plannerName,
                     solveds,
                     cardinalities,
                     Some(as3_5(statement3_3)),
                     None,
                     logicalPlanState.maybeExtractedParams,
                     Some(semanticTable3_4),
                     None,
                     Some(plan3_4),
                     Some(logicalPlanState.maybePeriodicCommit.flatten.map(x => as3_5(x))),
                     Set.empty)
  }

  private def convertLogicalPlan(logicalPlanState: LogicalPlanStateV3_3,
                                 solveds: Solveds,
                                 cardinalities: Cardinalities): (LogicalPlanv3_5, SemanticTablev3_5) = {

    def isImportant(expression: ExpressionV3_3) : Boolean =
      logicalPlanState.maybeSemanticTable.exists(_.seen(expression))

    val idConverter = new MaxIdConverter
    val (plan3_4, expressionMap) =
      LogicalPlanConverter.convertLogicalPlan(
        logicalPlanState.maybeLogicalPlan.get,
        solveds,
        cardinalities,
        idConverter,
        isImportant
      )

    val attributes = Attributes(idConverter.idGenFromMax, solveds, cardinalities)
    val planWithActiveReads = ActiveReadInjector(attributes).apply(plan3_4)
    val maybeTable = logicalPlanState.maybeSemanticTable
    val semanticTable = if (maybeTable.isDefined) {
      SemanticTableConverter.convertSemanticTable(maybeTable.get, expressionMap)
    } else {
      new SemanticTablev3_5()
    }
    (planWithActiveReads, semanticTable)
  }
}
