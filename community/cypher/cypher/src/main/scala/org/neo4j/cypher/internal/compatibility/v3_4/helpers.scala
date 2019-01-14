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
package org.neo4j.cypher.internal.compatibility.v3_4

import org.neo4j.cypher.CypherVersion.v3_4
import org.neo4j.cypher.InternalException
import org.neo4j.cypher.internal.compiler.v3_4.phases.{LogicalPlanState => LogicalPlanStateV3_4}
import org.neo4j.cypher.internal.compiler.v3_4.{CypherCompilerConfiguration => CypherCompilerConfiguration3_4, UpdateStrategy => UpdateStrategyV3_4}
import org.neo4j.cypher.internal.compiler.v3_5.phases.LogicalPlanState
import org.neo4j.cypher.internal.compiler.v3_5.{CypherPlannerConfiguration, UpdateStrategy, defaultUpdateStrategy, eagerUpdateStrategy}
import org.neo4j.cypher.internal.compiler.{v3_4 => compilerV3_4, v3_5 => compilerv3_5}
import org.neo4j.cypher.internal.frontend.v3_4.phases.CompilationPhaseTracer.{CompilationPhase => v3_4Phase}
import org.neo4j.cypher.internal.frontend.v3_4.phases.{CompilationPhaseTracer => CompilationPhaseTracer3_4}
import org.neo4j.cypher.internal.frontend.v3_4.{PlannerName => PlannerNameV3_4, ast => astV3_4, notification => nfV3_4}
import org.neo4j.cypher.internal.ir.{v3_4 => irV3_4, v3_5 => irv3_5}
import org.neo4j.cypher.internal.planner.v3_4.spi.PlanningAttributes.{Cardinalities => CardinalitiesV3_4, Solveds => SolvedsV3_4}
import org.neo4j.cypher.internal.planner.v3_4.{spi => spiV3_4}
import org.neo4j.cypher.internal.planner.v3_5.spi.PlanningAttributes.{ProvidedOrders, Cardinalities => CardinalitiesV3_5, Solveds => SolvedsV3_5}
import org.neo4j.cypher.internal.planner.v3_5.spi._
import org.neo4j.cypher.internal.util.v3_4.attribution.{Id => IdV3_4}
import org.neo4j.cypher.internal.util.v3_4.{Cardinality => CardinalityV3_4, InputPosition => InputPositionV3_4}
import org.neo4j.cypher.internal.v3_4.expressions.{Expression => ExpressionV3_4}
import org.neo4j.cypher.internal.v3_5.logical.plans.{LogicalPlan => LogicalPlanv3_5}
import org.neo4j.kernel.impl.query.{QueryExecutionMonitor, TransactionalContext}
import org.neo4j.cypher.internal.v3_5.ast.semantics.{SemanticTable => SemanticTablev3_5}
import org.neo4j.cypher.internal.v3_5.frontend.PlannerName
import org.neo4j.cypher.internal.v3_5.frontend.phases.CompilationPhaseTracer
import org.neo4j.cypher.internal.v3_5.frontend.phases.CompilationPhaseTracer.{CompilationPhase => v3_5Phase}
import org.neo4j.cypher.internal.v3_5.util.attribution.Id
import org.neo4j.cypher.internal.v3_5.util.{Cardinality, InputPosition}
import org.neo4j.cypher.internal.v3_5.{ast => astv3_5, util => nfv3_5}

object helpers {
  implicit def monitorFailure(t: Throwable)(implicit monitor: QueryExecutionMonitor, tc: TransactionalContext): Unit = {
    monitor.endFailure(tc.executingQuery(), t)
  }

  def as3_4(config: CypherPlannerConfiguration): CypherCompilerConfiguration3_4 =
    CypherCompilerConfiguration3_4(
      config.queryCacheSize,
      as3_4(config.statsDivergenceCalculator),
      config.useErrorsOverWarnings,
      config.idpMaxTableSize,
      config.idpIterationDuration,
      config.errorIfShortestPathFallbackUsedAtRuntime,
      config.errorIfShortestPathHasCommonNodesAtRuntime,
      config.legacyCsvQuoteEscaping,
      config.csvBufferSize,
      config.nonIndexedLabelWarningThreshold,
      config.planWithMinimumCardinalityEstimates,
      config.lenientCreateRelationship)


  /** This is awful but needed until 3_0 is updated no to send in the tracer here */
  def as3_4(tracer: CompilationPhaseTracer): CompilationPhaseTracer3_4 = {
    new CompilationPhaseTracer3_4 {
      override def beginPhase(phase: CompilationPhaseTracer3_4.CompilationPhase): CompilationPhaseTracer3_4.CompilationPhaseEvent = {
        val wrappedPhase = phase match {
          case v3_4Phase.AST_REWRITE => v3_5Phase.AST_REWRITE
          case v3_4Phase.CODE_GENERATION => v3_5Phase.CODE_GENERATION
          case v3_4Phase.LOGICAL_PLANNING => v3_5Phase.LOGICAL_PLANNING
          case v3_4Phase.PARSING => v3_5Phase.PARSING
          case v3_4Phase.PIPE_BUILDING => v3_5Phase.PIPE_BUILDING
          case v3_4Phase.SEMANTIC_CHECK => v3_5Phase.SEMANTIC_CHECK
          case v3_4Phase.DEPRECATION_WARNINGS => v3_5Phase.DEPRECATION_WARNINGS
          case _ => throw new InternalException(s"Cannot handle $phase in 3.3")
        }

        val wrappedEvent = tracer.beginPhase(wrappedPhase)

        new CompilationPhaseTracer3_4.CompilationPhaseEvent {
          override def close(): Unit = wrappedEvent.close()
        }
      }
    }
  }

  def as3_4(calc: compilerv3_5.StatsDivergenceCalculator): compilerV3_4.StatsDivergenceCalculator = calc match {
    case compilerv3_5.StatsDivergenceInverseDecayCalculator(initialThreshold, targetThreshold, initialMillis, targetMillis) =>
      compilerV3_4.StatsDivergenceInverseDecayCalculator(initialThreshold, targetThreshold, initialMillis, targetMillis)
    case compilerv3_5.StatsDivergenceExponentialDecayCalculator(initialThreshold, targetThreshold, initialMillis, targetMillis) =>
      compilerV3_4.StatsDivergenceExponentialDecayCalculator(initialThreshold, targetThreshold, initialMillis, targetMillis)
    case compilerv3_5.StatsDivergenceNoDecayCalculator(initialThreshold, initialMillis) =>
      compilerV3_4.StatsDivergenceNoDecayCalculator(initialThreshold, initialMillis)
  }

  def as3_4(pos: InputPosition): InputPositionV3_4 = InputPositionV3_4(pos.offset, pos.line, pos.column)

  def as3_5(pos: InputPositionV3_4): InputPosition = if(pos == null) null else InputPosition(pos.offset, pos.line, pos.column)

  def as3_5(planId: IdV3_4) : Id = Id(planId.x)

  def as3_5(plannerName: PlannerNameV3_4) : PlannerName = plannerName match {
    case spiV3_4.IDPPlannerName => IDPPlannerName
    case spiV3_4.DPPlannerName => DPPlannerName
    case spiV3_4.ProcedurePlannerName => ProcedurePlannerName
  }

  def as3_5(updateStrategy: UpdateStrategyV3_4): UpdateStrategy = updateStrategy match {
    case org.neo4j.cypher.internal.compiler.v3_4.eagerUpdateStrategy => eagerUpdateStrategy
    case org.neo4j.cypher.internal.compiler.v3_4.defaultUpdateStrategy => defaultUpdateStrategy
  }

  def as3_5(periodicCommit: irV3_4.PeriodicCommit): irv3_5.PeriodicCommit = {
    irv3_5.PeriodicCommit(periodicCommit.batchSize)
  }

  def as3_5(cardinality: CardinalityV3_4) : Cardinality = {
    Cardinality(cardinality.amount)
  }

  def as3_5(statement: astV3_4.Statement) : astv3_5.Statement = StatementWrapper(statement)

  def as3_5(notification: nfV3_4.InternalNotification): nfv3_5.InternalNotification = notification match {
    case nfV3_4.DeprecatedStartNotification(position, alternativeQuery) => nfv3_5.DeprecatedStartNotification(as3_5(position), alternativeQuery)
    case nfV3_4.CartesianProductNotification(position, isolatedVariables) => nfv3_5.CartesianProductNotification(as3_5(position), isolatedVariables)
    case nfV3_4.LengthOnNonPathNotification(position) => nfv3_5.LengthOnNonPathNotification(as3_5(position))
    case nfV3_4.PlannerUnsupportedNotification => compilerv3_5.PlannerUnsupportedNotification
    case nfV3_4.RuntimeUnsupportedNotification => compilerv3_5.RuntimeUnsupportedNotification
    case nfV3_4.IndexHintUnfulfillableNotification(label, propertyKeys) => compilerv3_5.IndexHintUnfulfillableNotification(label, propertyKeys)
    case nfV3_4.JoinHintUnfulfillableNotification(identified) => compilerv3_5.JoinHintUnfulfillableNotification(identified)
    case nfV3_4.JoinHintUnsupportedNotification(identified) => compilerv3_5.JoinHintUnsupportedNotification(identified)
    case nfV3_4.IndexLookupUnfulfillableNotification(labels) => compilerv3_5.IndexLookupUnfulfillableNotification(labels)
    case nfV3_4.EagerLoadCsvNotification => compilerv3_5.EagerLoadCsvNotification
    case nfV3_4.LargeLabelWithLoadCsvNotification => compilerv3_5.LargeLabelWithLoadCsvNotification
    case nfV3_4.MissingLabelNotification(position, label) => compilerv3_5.MissingLabelNotification(as3_5(position), label)
    case nfV3_4.MissingRelTypeNotification(position, relType) => compilerv3_5.MissingRelTypeNotification(as3_5(position), relType)
    case nfV3_4.MissingPropertyNameNotification(position, name) => compilerv3_5.MissingPropertyNameNotification(as3_5(position), name)
    case nfV3_4.UnboundedShortestPathNotification(position) => nfv3_5.UnboundedShortestPathNotification(as3_5(position))
    case nfV3_4.ExhaustiveShortestPathForbiddenNotification(position) => compilerv3_5.ExhaustiveShortestPathForbiddenNotification(as3_5(position))
    case nfV3_4.DeprecatedFunctionNotification(position, oldName, newName) => nfv3_5.DeprecatedFunctionNotification(as3_5(position), oldName, newName)
    case nfV3_4.DeprecatedProcedureNotification(position, oldName, newName) => compilerv3_5.DeprecatedProcedureNotification(as3_5(position), oldName, newName)
    case nfV3_4.ProcedureWarningNotification(position, procedure, warning) => compilerv3_5.ProcedureWarningNotification(as3_5(position), procedure, warning)
    case nfV3_4.DeprecatedFieldNotification(position, procedure, field) => compilerv3_5.DeprecatedFieldNotification(as3_5(position), procedure, field)
    case nfV3_4.DeprecatedVarLengthBindingNotification(position, variable) => nfv3_5.DeprecatedVarLengthBindingNotification(as3_5(position), variable)
    case nfV3_4.DeprecatedRelTypeSeparatorNotification(position) => nfv3_5.DeprecatedRelTypeSeparatorNotification(as3_5(position))
    case nfV3_4.DeprecatedPlannerNotification => compilerv3_5.DeprecatedRulePlannerNotification
    case nfV3_4.ExperimentalFeatureNotification(msg) => compilerv3_5.ExperimentalFeatureNotification(msg)
    case nfV3_4.SuboptimalIndexForContainsQueryNotification(label, propertyKeys) => compilerv3_5.SuboptimalIndexForConstainsQueryNotification(label, propertyKeys)
    case nfV3_4.SuboptimalIndexForEndsWithQueryNotification(label, propertyKeys) => compilerv3_5.SuboptimalIndexForEndsWithQueryNotification(label, propertyKeys)
  }

  def as3_5(logicalPlanState: LogicalPlanStateV3_4) : LogicalPlanState = {
    val startPosition = logicalPlanState.startPosition.map(as3_5)
    // Wrap the planner name to correctly report version 3.4.
    val plannerName = PlannerNameWithVersion(as3_5(logicalPlanState.plannerName), v3_4.name)

    val solveds3_4 = logicalPlanState.solveds
    val cardinalities3_4 = logicalPlanState.cardinalities

    val solveds3_5 = new SolvedsV3_5
    val cardinalities3_5 = new CardinalitiesV3_5
    val providedOrders = new ProvidedOrders
    val (plan3_4, semanticTable3_4) = convertLogicalPlan(logicalPlanState, solveds3_4, cardinalities3_4, solveds3_5, cardinalities3_5)

    val statement3_4 = logicalPlanState.maybeStatement.get

    LogicalPlanState(logicalPlanState.queryText,
                     startPosition,
                     plannerName,
                     PlanningAttributes(solveds3_5, cardinalities3_5, providedOrders),
                     Some(as3_5(statement3_4)),
                     None,
                     logicalPlanState.maybeExtractedParams,
                     Some(semanticTable3_4),
                     None,
                     Some(plan3_4),
                     Some(logicalPlanState.maybePeriodicCommit.flatten.map(x => as3_5(x))),
                     Set.empty)
  }

  private def convertLogicalPlan(logicalPlanState: LogicalPlanStateV3_4,
                                 solveds3_4: SolvedsV3_4,
                                 cardinalities3_4: CardinalitiesV3_4,
                                 solveds3_5: SolvedsV3_5,
                                 cardinalities3_5: CardinalitiesV3_5): (LogicalPlanv3_5, SemanticTablev3_5) = {

    def seenBySemanticTable(expression: ExpressionV3_4) : Boolean =
      logicalPlanState.maybeSemanticTable.exists(_.seen(expression))

    val idConverter = new MaxIdConverter
    val (plan3_4, expressionMap) =
      LogicalPlanConverter.convertLogicalPlan(
        logicalPlanState.maybeLogicalPlan.get,
        solveds3_4,
        cardinalities3_4,
        solveds3_5,
        cardinalities3_5,
        idConverter,
        seenBySemanticTable
      )

    val maybeTable = logicalPlanState.maybeSemanticTable
    val semanticTable = if (maybeTable.isDefined) {
      SemanticTableConverter.convertSemanticTable(maybeTable.get, expressionMap)
    } else {
      new SemanticTablev3_5()
    }
    (plan3_4, semanticTable)
  }
}
