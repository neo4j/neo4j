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
package org.neo4j.cypher.internal.compatibility.v3_3

import org.neo4j.cypher.InternalException
import org.neo4j.cypher.internal.compiler.v3_3.phases.{LogicalPlanState => LogicalPlanStateV3_3}
import org.neo4j.cypher.internal.compiler.v3_3.{CypherCompilerConfiguration => CypherCompilerConfiguration3_3, DPPlannerName => DPPlannerNameV3_3, IDPPlannerName => IDPPlannerNameV3_3, ProcedurePlannerName => ProcedurePlannerNameV3_3, UpdateStrategy => UpdateStrategyV3_3}
import org.neo4j.cypher.internal.compiler.v3_4.phases.LogicalPlanState
import org.neo4j.cypher.internal.compiler.v3_4.{CypherCompilerConfiguration, UpdateStrategy, defaultUpdateStrategy, eagerUpdateStrategy}
import org.neo4j.cypher.internal.frontend.v3_3.ast.{Expression => ExpressionV3_3}
import org.neo4j.cypher.internal.frontend.v3_3.phases.CompilationPhaseTracer.{CompilationPhase => v3_3Phase}
import org.neo4j.cypher.internal.frontend.v3_3.phases.{CompilationPhaseTracer => CompilationPhaseTracer3_3}
import org.neo4j.cypher.internal.frontend.v3_3.{InputPosition => InputPositionV3_3, PlannerName => PlannerNameV3_3}
import org.neo4j.cypher.internal.frontend.v3_4.PlannerName
import org.neo4j.cypher.internal.frontend.v3_4.phases.CompilationPhaseTracer
import org.neo4j.cypher.internal.frontend.v3_4.phases.CompilationPhaseTracer.{CompilationPhase => v3_4Phase}
import org.neo4j.cypher.internal.ir.v3_3.{Cardinality => CardinalityV3_3}
import org.neo4j.cypher.internal.ir.{v3_3 => irV3_3, v3_4 => irV3_4}
import org.neo4j.cypher.internal.planner.v3_4.spi.{DPPlannerName, IDPPlannerName, ProcedurePlannerName}
import org.neo4j.cypher.internal.util.v3_4.{Cardinality, InputPosition}
import org.neo4j.cypher.internal.v3_3.logical.plans.{LogicalPlanId => LogicalPlanIdV3_3}
import org.neo4j.cypher.internal.v3_4.logical.plans.{LogicalPlanId => LogicalPlanIdV3_4}
import org.neo4j.kernel.impl.query.{QueryExecutionMonitor, TransactionalContext}

object helpers {
  implicit def monitorFailure(t: Throwable)(implicit monitor: QueryExecutionMonitor, tc: TransactionalContext): Unit = {
    monitor.endFailure(tc.executingQuery(), t)
  }
  def as3_3(config: CypherCompilerConfiguration) =
    CypherCompilerConfiguration3_3(
      config.queryCacheSize,
      config.statsDivergenceThreshold,
      config.queryPlanTTL,
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
      override def beginPhase(phase: CompilationPhaseTracer3_3.CompilationPhase) = {
        val wrappedPhase = phase match {
          case v3_3Phase.AST_REWRITE => v3_4Phase.AST_REWRITE
          case v3_3Phase.CODE_GENERATION => v3_4Phase.CODE_GENERATION
          case v3_3Phase.LOGICAL_PLANNING => v3_4Phase.LOGICAL_PLANNING
          case v3_3Phase.PARSING => v3_4Phase.PARSING
          case v3_3Phase.PIPE_BUILDING => v3_4Phase.PIPE_BUILDING
          case v3_3Phase.SEMANTIC_CHECK => v3_4Phase.SEMANTIC_CHECK
          case v3_3Phase.DEPRECATION_WARNINGS => v3_4Phase.DEPRECATION_WARNINGS
          case _ => throw new InternalException(s"Cannot handle $phase in 3.3")
        }

        val wrappedEvent = tracer.beginPhase(wrappedPhase)

        new CompilationPhaseTracer3_3.CompilationPhaseEvent {
          override def close(): Unit = wrappedEvent.close()
        }
      }
    }
  }

  def as3_3(pos: InputPosition): InputPositionV3_3 = InputPositionV3_3(pos.offset, pos.line, pos.column)

  def as3_4(pos: InputPositionV3_3): InputPosition = InputPosition(pos.offset, pos.line, pos.column)

  def as3_4(planId: LogicalPlanIdV3_3) : LogicalPlanIdV3_4 = new LogicalPlanIdV3_4(planId.underlying)

  def as3_4(plannerName: PlannerNameV3_3) : PlannerName = plannerName match {
    case IDPPlannerNameV3_3 => IDPPlannerName
    case DPPlannerNameV3_3 => DPPlannerName
    case ProcedurePlannerNameV3_3 => ProcedurePlannerName
  }

  def as3_4(updateStrategy: UpdateStrategyV3_3): UpdateStrategy = updateStrategy match {
    case org.neo4j.cypher.internal.compiler.v3_3.eagerUpdateStrategy => eagerUpdateStrategy
    case org.neo4j.cypher.internal.compiler.v3_3.defaultUpdateStrategy => defaultUpdateStrategy
  }

  def as3_4(periodicCommit: irV3_3.PeriodicCommit): irV3_4.PeriodicCommit = {
    irV3_4.PeriodicCommit(periodicCommit.batchSize)
  }

  def as3_4(cardinality: CardinalityV3_3) : Cardinality = {
    Cardinality(cardinality.amount)
  }

  def as3_4(logicalPlan: LogicalPlanStateV3_3) : LogicalPlanState = {
    val startPosition = logicalPlan.startPosition.map(as3_4 _)
    val plannerName = as3_4(logicalPlan.plannerName)

    val table3_3 = logicalPlan.maybeSemanticTable.get
    def isImportant(expression: ExpressionV3_3) : Boolean = table3_3.seen(expression)

    val (plan3_4, expressionMap) = LogicalPlanConverter.convertLogicalPlan(logicalPlan.maybeLogicalPlan.get, isImportant)
    LogicalPlanState(logicalPlan.queryText,
      startPosition,
      plannerName,
      None,
      None,
      logicalPlan.maybeExtractedParams,
      Some(SemanticTableConverter.convertSemanticTable(table3_3, expressionMap)),
      None,
      Some(plan3_4),
      Some(logicalPlan.maybePeriodicCommit.flatten.map(x => as3_4(x))),
      Set.empty)
  }

}
