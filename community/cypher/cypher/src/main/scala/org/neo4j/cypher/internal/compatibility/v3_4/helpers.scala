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
package org.neo4j.cypher.internal.compatibility.v3_4

import org.neo4j.cypher.CypherVersion.v3_4
import org.neo4j.cypher.InternalException
import org.neo4j.cypher.internal.compiler.v3_4.phases.{LogicalPlanState => LogicalPlanStateV3_4}
import org.neo4j.cypher.internal.compiler.v3_4.{CypherCompilerConfiguration => CypherCompilerConfiguration3_4, UpdateStrategy => UpdateStrategyV3_4}
import org.neo4j.cypher.internal.compiler.v4_0.phases.LogicalPlanState
import org.neo4j.cypher.internal.compiler.v4_0.{CypherPlannerConfiguration, UpdateStrategy, defaultUpdateStrategy, eagerUpdateStrategy}
import org.neo4j.cypher.internal.compiler.{v3_4 => compilerV3_4, v4_0 => compilerv4_0}
import org.neo4j.cypher.internal.frontend.v3_4.phases.CompilationPhaseTracer.{CompilationPhase => v3_4Phase}
import org.neo4j.cypher.internal.frontend.v3_4.phases.{CompilationPhaseTracer => CompilationPhaseTracer3_4}
import org.neo4j.cypher.internal.frontend.v3_4.{PlannerName => PlannerNameV3_4, ast => astV3_4, notification => nfV3_4}
import org.neo4j.cypher.internal.ir.{v3_4 => irV3_4, v4_0 => irv4_0}
import org.neo4j.cypher.internal.planner.v3_4.spi.PlanningAttributes.{Cardinalities => CardinalitiesV3_4, Solveds => SolvedsV3_4}
import org.neo4j.cypher.internal.planner.v3_4.{spi => spiV3_4}
import org.neo4j.cypher.internal.planner.v4_0.spi.PlanningAttributes.{ProvidedOrders, Cardinalities => Cardinalitiesv4_0, Solveds => Solvedsv4_0}
import org.neo4j.cypher.internal.planner.v4_0.spi._
import org.neo4j.cypher.internal.util.v3_4.attribution.{Id => IdV3_4}
import org.neo4j.cypher.internal.util.v3_4.{Cardinality => CardinalityV3_4, InputPosition => InputPositionV3_4}
import org.neo4j.cypher.internal.v3_4.expressions.{Expression => ExpressionV3_4}
import org.neo4j.cypher.internal.v4_0.logical.plans.{LogicalPlan => LogicalPlanv4_0}
import org.neo4j.kernel.impl.query.{QueryExecutionMonitor, TransactionalContext}
import org.neo4j.cypher.internal.v3_5.ast.semantics.{SemanticTable => SemanticTablev4_0}
import org.neo4j.cypher.internal.v3_5.frontend.PlannerName
import org.neo4j.cypher.internal.v3_5.frontend.phases.CompilationPhaseTracer
import org.neo4j.cypher.internal.v3_5.frontend.phases.CompilationPhaseTracer.{CompilationPhase => v4_0Phase}
import org.neo4j.cypher.internal.v3_5.util.attribution.Id
import org.neo4j.cypher.internal.v3_5.util.{Cardinality, InputPosition}
import org.neo4j.cypher.internal.v3_5.{ast => astv4_0, util => nfv4_0}

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
          case v3_4Phase.AST_REWRITE => v4_0Phase.AST_REWRITE
          case v3_4Phase.CODE_GENERATION => v4_0Phase.CODE_GENERATION
          case v3_4Phase.LOGICAL_PLANNING => v4_0Phase.LOGICAL_PLANNING
          case v3_4Phase.PARSING => v4_0Phase.PARSING
          case v3_4Phase.PIPE_BUILDING => v4_0Phase.PIPE_BUILDING
          case v3_4Phase.SEMANTIC_CHECK => v4_0Phase.SEMANTIC_CHECK
          case v3_4Phase.DEPRECATION_WARNINGS => v4_0Phase.DEPRECATION_WARNINGS
          case _ => throw new InternalException(s"Cannot handle $phase in 3.3")
        }

        val wrappedEvent = tracer.beginPhase(wrappedPhase)

        new CompilationPhaseTracer3_4.CompilationPhaseEvent {
          override def close(): Unit = wrappedEvent.close()
        }
      }
    }
  }

  def as3_4(calc: compilerv4_0.StatsDivergenceCalculator): compilerV3_4.StatsDivergenceCalculator = calc match {
    case compilerv4_0.StatsDivergenceInverseDecayCalculator(initialThreshold, targetThreshold, initialMillis, targetMillis) =>
      compilerV3_4.StatsDivergenceInverseDecayCalculator(initialThreshold, targetThreshold, initialMillis, targetMillis)
    case compilerv4_0.StatsDivergenceExponentialDecayCalculator(initialThreshold, targetThreshold, initialMillis, targetMillis) =>
      compilerV3_4.StatsDivergenceExponentialDecayCalculator(initialThreshold, targetThreshold, initialMillis, targetMillis)
    case compilerv4_0.StatsDivergenceNoDecayCalculator(initialThreshold, initialMillis) =>
      compilerV3_4.StatsDivergenceNoDecayCalculator(initialThreshold, initialMillis)
  }

  def as3_4(pos: InputPosition): InputPositionV3_4 = InputPositionV3_4(pos.offset, pos.line, pos.column)

  def as4_0(pos: InputPositionV3_4): InputPosition = if(pos == null) null else InputPosition(pos.offset, pos.line, pos.column)

  def as4_0(planId: IdV3_4) : Id = Id(planId.x)

  def as4_0(plannerName: PlannerNameV3_4) : PlannerName = plannerName match {
    case spiV3_4.IDPPlannerName => IDPPlannerName
    case spiV3_4.DPPlannerName => DPPlannerName
    case spiV3_4.ProcedurePlannerName => ProcedurePlannerName
  }

  def as4_0(updateStrategy: UpdateStrategyV3_4): UpdateStrategy = updateStrategy match {
    case org.neo4j.cypher.internal.compiler.v3_4.eagerUpdateStrategy => eagerUpdateStrategy
    case org.neo4j.cypher.internal.compiler.v3_4.defaultUpdateStrategy => defaultUpdateStrategy
  }

  def as4_0(periodicCommit: irV3_4.PeriodicCommit): irv4_0.PeriodicCommit = {
    irv4_0.PeriodicCommit(periodicCommit.batchSize)
  }

  def as4_0(cardinality: CardinalityV3_4) : Cardinality = {
    Cardinality(cardinality.amount)
  }

  def as4_0(statement: astV3_4.Statement) : astv4_0.Statement = StatementWrapper(statement)

  def as4_0(notification: nfV3_4.InternalNotification): nfv4_0.InternalNotification = notification match {
    case nfV3_4.DeprecatedStartNotification(position, alternativeQuery) => nfv4_0.DeprecatedStartNotification(as4_0(position), alternativeQuery)
    case nfV3_4.CartesianProductNotification(position, isolatedVariables) => nfv4_0.CartesianProductNotification(as4_0(position), isolatedVariables)
    case nfV3_4.LengthOnNonPathNotification(position) => nfv4_0.LengthOnNonPathNotification(as4_0(position))
    case nfV3_4.PlannerUnsupportedNotification => compilerv4_0.PlannerUnsupportedNotification
    case nfV3_4.RuntimeUnsupportedNotification => compilerv4_0.RuntimeUnsupportedNotification
    case nfV3_4.IndexHintUnfulfillableNotification(label, propertyKeys) => compilerv4_0.IndexHintUnfulfillableNotification(label, propertyKeys)
    case nfV3_4.JoinHintUnfulfillableNotification(identified) => compilerv4_0.JoinHintUnfulfillableNotification(identified)
    case nfV3_4.JoinHintUnsupportedNotification(identified) => compilerv4_0.JoinHintUnsupportedNotification(identified)
    case nfV3_4.IndexLookupUnfulfillableNotification(labels) => compilerv4_0.IndexLookupUnfulfillableNotification(labels)
    case nfV3_4.EagerLoadCsvNotification => compilerv4_0.EagerLoadCsvNotification
    case nfV3_4.LargeLabelWithLoadCsvNotification => compilerv4_0.LargeLabelWithLoadCsvNotification
    case nfV3_4.MissingLabelNotification(position, label) => compilerv4_0.MissingLabelNotification(as4_0(position), label)
    case nfV3_4.MissingRelTypeNotification(position, relType) => compilerv4_0.MissingRelTypeNotification(as4_0(position), relType)
    case nfV3_4.MissingPropertyNameNotification(position, name) => compilerv4_0.MissingPropertyNameNotification(as4_0(position), name)
    case nfV3_4.UnboundedShortestPathNotification(position) => nfv4_0.UnboundedShortestPathNotification(as4_0(position))
    case nfV3_4.ExhaustiveShortestPathForbiddenNotification(position) => compilerv4_0.ExhaustiveShortestPathForbiddenNotification(as4_0(position))
    case nfV3_4.DeprecatedFunctionNotification(position, oldName, newName) => nfv4_0.DeprecatedFunctionNotification(as4_0(position), oldName, newName)
    case nfV3_4.DeprecatedProcedureNotification(position, oldName, newName) => compilerv4_0.DeprecatedProcedureNotification(as4_0(position), oldName, newName)
    case nfV3_4.ProcedureWarningNotification(position, procedure, warning) => compilerv4_0.ProcedureWarningNotification(as4_0(position), procedure, warning)
    case nfV3_4.DeprecatedFieldNotification(position, procedure, field) => compilerv4_0.DeprecatedFieldNotification(as4_0(position), procedure, field)
    case nfV3_4.DeprecatedVarLengthBindingNotification(position, variable) => nfv4_0.DeprecatedVarLengthBindingNotification(as4_0(position), variable)
    case nfV3_4.DeprecatedRelTypeSeparatorNotification(position) => nfv4_0.DeprecatedRelTypeSeparatorNotification(as4_0(position))
    case nfV3_4.DeprecatedPlannerNotification => compilerv4_0.DeprecatedRulePlannerNotification
    case nfV3_4.ExperimentalFeatureNotification(msg) => compilerv4_0.ExperimentalFeatureNotification(msg)
    case nfV3_4.SuboptimalIndexForContainsQueryNotification(label, propertyKeys) => compilerv4_0.SuboptimalIndexForConstainsQueryNotification(label, propertyKeys)
    case nfV3_4.SuboptimalIndexForEndsWithQueryNotification(label, propertyKeys) => compilerv4_0.SuboptimalIndexForEndsWithQueryNotification(label, propertyKeys)
  }

  def as4_0(logicalPlanState: LogicalPlanStateV3_4) : LogicalPlanState = {
    val startPosition = logicalPlanState.startPosition.map(as4_0)
    // Wrap the planner name to correctly report version 3.4.
    val plannerName = PlannerNameWithVersion(as4_0(logicalPlanState.plannerName), v3_4.name)

    val solveds3_4 = logicalPlanState.solveds
    val cardinalities3_4 = logicalPlanState.cardinalities

    val solveds4_0 = new Solvedsv4_0
    val cardinalities4_0 = new Cardinalitiesv4_0
    val providedOrders = new ProvidedOrders
    val (plan3_4, semanticTable3_4) = convertLogicalPlan(logicalPlanState, solveds3_4, cardinalities3_4, solveds4_0, cardinalities4_0)

    val statement3_4 = logicalPlanState.maybeStatement.get

    LogicalPlanState(logicalPlanState.queryText,
                     startPosition,
                     plannerName,
                     PlanningAttributes(solveds4_0, cardinalities4_0, providedOrders),
                     Some(as4_0(statement3_4)),
                     None,
                     logicalPlanState.maybeExtractedParams,
                     Some(semanticTable3_4),
                     None,
                     Some(plan3_4),
                     Some(logicalPlanState.maybePeriodicCommit.flatten.map(x => as4_0(x))),
                     Set.empty)
  }

  private def convertLogicalPlan(logicalPlanState: LogicalPlanStateV3_4,
                                 solveds3_4: SolvedsV3_4,
                                 cardinalities3_4: CardinalitiesV3_4,
                                 solveds4_0: Solvedsv4_0,
                                 cardinalities4_0: Cardinalitiesv4_0): (LogicalPlanv4_0, SemanticTablev4_0) = {

    def seenBySemanticTable(expression: ExpressionV3_4) : Boolean =
      logicalPlanState.maybeSemanticTable.exists(_.seen(expression))

    val idConverter = new MaxIdConverter
    val (plan3_4, expressionMap) =
      LogicalPlanConverter.convertLogicalPlan(
        logicalPlanState.maybeLogicalPlan.get,
        solveds3_4,
        cardinalities3_4,
        solveds4_0,
        cardinalities4_0,
        idConverter,
        seenBySemanticTable
      )

    val maybeTable = logicalPlanState.maybeSemanticTable
    val semanticTable = if (maybeTable.isDefined) {
      SemanticTableConverter.convertSemanticTable(maybeTable.get, expressionMap)
    } else {
      new SemanticTablev4_0()
    }
    (plan3_4, semanticTable)
  }
}
