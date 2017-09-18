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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.executionplan.procs

import org.neo4j.cypher.CypherVersion
import org.neo4j.cypher.internal.InternalExecutionResult
import org.neo4j.cypher.internal.compatibility.v3_3.runtime._
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.convert.ExpressionConverters
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.expressions
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.expressions.Literal
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.executionplan.{ExecutionPlan, ProcedureCallMode}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.helpers.Counter
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes.{ExternalCSVResource, QueryState}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.planDescription.InternalPlanDescription.Arguments._
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.planDescription.{Argument, NoChildren, PlanDescriptionImpl}
import org.neo4j.cypher.internal.compiler.v3_3.ProcedurePlannerName
import org.neo4j.cypher.internal.compiler.v3_3.spi.{GraphStatistics, PlanContext}
import org.neo4j.cypher.internal.frontend.v3_4.ast.Expression
import org.neo4j.cypher.internal.frontend.v3_4.notification.InternalNotification
import org.neo4j.cypher.internal.frontend.v3_4.symbols.CypherType
import org.neo4j.cypher.internal.spi.v3_3.QueryContext
import org.neo4j.cypher.internal.v3_4.logical.plans.{LogicalPlanId, ProcedureSignature}
import org.neo4j.graphdb.Notification
import org.neo4j.values.virtual.MapValue

/**
  * Execution plan for calling procedures
  *
  * a procedure can be called in two ways, either `CALL proc(a,b)` or as `CALL proc` with `a` and `b` provided as
  * parameters to `run`. In the former case type checking should be done before instantiating this class, in the
  * latter case we will have to resort to runtime type checking.
  *
  * @param signature the signature of the procedure
  * @param argExprs  the arguments to the procedure
  */
case class ProcedureCallExecutionPlan(signature: ProcedureSignature,
                                      argExprs: Seq[Expression],
                                      resultSymbols: Seq[(String, CypherType)],
                                      resultIndices: Seq[(Int, String)],
                                      notifications: Set[Notification],
                                      converter: ExpressionConverters)
  extends ExecutionPlan {

  assert(resultSymbols.size == resultIndices.size)

  private val resultMappings = resultSymbols.indices.map(i => {
    val r = resultIndices(i)
    (r._1, r._2, resultSymbols(i)._2)
  })

  private val argExprCommands: Seq[expressions.Expression] = argExprs.map(converter.toCommandExpression) ++
    signature.inputSignature.drop(argExprs.size).flatMap(_.default).map(o => Literal(o.value))

  override def run(ctx: QueryContext, planType: ExecutionMode, params: MapValue): InternalExecutionResult = {
    val input = evaluateArguments(ctx, params)

    val taskCloser = new TaskCloser
    taskCloser.addTask(ctx.transactionalContext.close)

    planType match {
      case NormalMode => createNormalExecutionResult(ctx, taskCloser, input, planType)
      case ExplainMode => createExplainedExecutionResult(ctx, taskCloser, input, notifications)
      case ProfileMode => createProfiledExecutionResult(ctx, taskCloser, input, planType)
    }
  }

  private def createNormalExecutionResult(ctx: QueryContext, taskCloser: TaskCloser,
                                          input: Seq[Any], planType: ExecutionMode) = {
    val descriptionGenerator = () => createNormalPlan
    val callMode = ProcedureCallMode.fromAccessMode(signature.accessMode)
    new ProcedureExecutionResult(ctx, taskCloser, signature.name, callMode, input,
                                 resultMappings, descriptionGenerator, planType)
  }

  private def createExplainedExecutionResult(ctx: QueryContext, taskCloser: TaskCloser, input: Seq[Any],
                                             notifications: Set[Notification]) = {
    // close all statements
    taskCloser.close(success = true)
    val callMode = ProcedureCallMode.fromAccessMode(signature.accessMode)
    val columns = signature.outputSignature.map(_.seq.map(_.name).toList).getOrElse(List.empty)
    ExplainExecutionResult(columns.toArray, createNormalPlan, callMode.queryType, notifications)
  }

  private def createProfiledExecutionResult(ctx: QueryContext, taskCloser: TaskCloser,
                                            input: Seq[Any], planType: ExecutionMode) = {
    val rowCounter = Counter()
    val descriptionGenerator = createProfilePlanGenerator(rowCounter)
    val callMode = ProcedureCallMode.fromAccessMode(signature.accessMode)
    new ProcedureExecutionResult(ctx, taskCloser, signature.name, callMode, input,
                                 resultMappings, descriptionGenerator, planType) {
      override protected def executeCall: Iterator[Array[AnyRef]] = rowCounter.track(super.executeCall)
    }
  }

  private def evaluateArguments(ctx: QueryContext, params: MapValue): Seq[Any] = {
    val state = new QueryState(ctx, ExternalCSVResource.empty, params)
    argExprCommands.map(expr => ctx.asObject(expr.apply(ExecutionContext.empty, state)))
  }

  private def createNormalPlan =
    PlanDescriptionImpl(LogicalPlanId.DEFAULT, "ProcedureCall", NoChildren,
                        arguments,
                        resultSymbols.map(_._1).toSet
    )

  private def createProfilePlanGenerator(rowCounter: Counter) = () =>
    PlanDescriptionImpl(LogicalPlanId.DEFAULT, "ProcedureCall", NoChildren,
                        Seq(createSignatureArgument, DbHits(1), Rows(rowCounter.counted)) ++ arguments,
                        resultSymbols.map(_._1).toSet
    )

  private def arguments: Seq[Argument] = Seq(createSignatureArgument,
                                                               Runtime(runtimeUsed.toTextOutput),
                                                               RuntimeImpl(runtimeUsed.name),
                                                               Planner(plannerUsed.toTextOutput),
                                                               PlannerImpl(plannerUsed.name),
                                                               Version(s"CYPHER ${CypherVersion.default.name}"))


  private def createSignatureArgument: Argument =
    Signature(signature.name, Seq.empty, resultSymbols)

  override def notifications(planContext: PlanContext): Seq[InternalNotification] = Seq.empty

  override def isPeriodicCommit: Boolean = false

  override def runtimeUsed = ProcedureRuntimeName

  override def isStale(lastTxId: () => Long, statistics: GraphStatistics) = false

  override def plannerUsed = ProcedurePlannerName
}

