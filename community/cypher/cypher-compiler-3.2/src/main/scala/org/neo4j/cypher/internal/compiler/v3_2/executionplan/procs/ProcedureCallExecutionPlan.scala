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
package org.neo4j.cypher.internal.compiler.v3_2.executionplan.procs

import org.neo4j.cypher.internal.compiler.v3_2.ast.convert.commands.ExpressionConverters._
import org.neo4j.cypher.internal.compiler.v3_2.commands.expressions
import org.neo4j.cypher.internal.compiler.v3_2.commands.expressions.Literal
import org.neo4j.cypher.internal.compiler.v3_2.executionplan.{ExecutionPlan, InternalExecutionResult, ProcedureCallMode, READ_ONLY}
import org.neo4j.cypher.internal.compiler.v3_2.helpers.{Counter, RuntimeJavaValueConverter}
import org.neo4j.cypher.internal.compiler.v3_2.pipes.{ExternalCSVResource, QueryState}
import org.neo4j.cypher.internal.compiler.v3_2.planDescription.InternalPlanDescription.Arguments.{DbHits, Rows, Signature}
import org.neo4j.cypher.internal.compiler.v3_2.planDescription.{Id, NoChildren, PlanDescriptionImpl}
import org.neo4j.cypher.internal.compiler.v3_2.spi.{GraphStatistics, PlanContext, ProcedureSignature, QueryContext}
import org.neo4j.cypher.internal.compiler.v3_2.{ExecutionContext, ExecutionMode, ExplainExecutionResult, ExplainMode, ProcedurePlannerName, ProcedureRuntimeName, TaskCloser, _}
import org.neo4j.cypher.internal.frontend.v3_2.ast.Expression
import org.neo4j.cypher.internal.frontend.v3_2.notification.InternalNotification
import org.neo4j.cypher.internal.frontend.v3_2.symbols.CypherType

/**
  * Execution plan for calling procedures
  *
  * a procedure can be called in two ways, either `CALL proc(a,b)` or as `CALL proc` with `a` and `b` provided as
  * parameters to `run`. In the former case type checking should be done before instantiating this class, in the
  * latter case we will have to resort to runtime type checking.
  *
  * @param signature the signature of the procedure
  * @param argExprs the arguments to the procedure
  */
case class ProcedureCallExecutionPlan(signature: ProcedureSignature,
                                      argExprs: Seq[Expression],
                                      resultSymbols: Seq[(String, CypherType)],
                                      resultIndices: Seq[(Int, String)], notifications: Set[InternalNotification],
                                      publicTypeConverter: Any => Any)
  extends ExecutionPlan {

  private val argExprCommands: Seq[expressions.Expression] =  argExprs.map(toCommandExpression) ++
    signature.inputSignature.drop(argExprs.size).flatMap(_.default).map(o => Literal(o.value))

  override def run(ctx: QueryContext, planType: ExecutionMode, params: Map[String, Any]): InternalExecutionResult = {
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
    new ProcedureExecutionResult(ctx, taskCloser, signature.name, callMode, input, resultIndices, descriptionGenerator, planType)
  }

  private def createExplainedExecutionResult(ctx: QueryContext, taskCloser: TaskCloser, input: Seq[Any],
                                             notifications: Set[InternalNotification]) = {
    // close all statements
    taskCloser.close(success = true)
    val columns = signature.outputSignature.map(_.seq.map(_.name).toList).getOrElse(List.empty)
    ExplainExecutionResult(columns, createNormalPlan, READ_ONLY, notifications)
  }

  private def createProfiledExecutionResult(ctx: QueryContext, taskCloser: TaskCloser,
                                            input: Seq[Any], planType: ExecutionMode) = {
    val rowCounter = Counter()
    val descriptionGenerator = createProfilePlanGenerator(rowCounter)
    val callMode = ProcedureCallMode.fromAccessMode(signature.accessMode)
    new ProcedureExecutionResult(ctx, taskCloser, signature.name, callMode, input, resultIndices, descriptionGenerator, planType) {
      override protected def executeCall: Iterator[Array[AnyRef]] = rowCounter.track(super.executeCall)
    }
  }

  private def evaluateArguments(ctx: QueryContext, params: Map[String, Any]): Seq[Any] = {
    val converter = new RuntimeJavaValueConverter(ctx.isGraphKernelResultValue, publicTypeConverter)
    val state = new QueryState(ctx, ExternalCSVResource.empty, params)
    argExprCommands.map(expr => converter.asDeepJavaValue(expr.apply(ExecutionContext.empty)(state)))
  }

  private def createNormalPlan =
    PlanDescriptionImpl(new Id, "ProcedureCall", NoChildren,
      Seq(createSignatureArgument),
      resultSymbols.map(_._1).toSet
    )

  private def createProfilePlanGenerator(rowCounter: Counter) = () =>
    PlanDescriptionImpl(new Id, "ProcedureCall", NoChildren,
      Seq(createSignatureArgument, DbHits(1), Rows(rowCounter.counted)),
      resultSymbols.map(_._1).toSet
    )

  private def createSignatureArgument =
    Signature(signature.name, Seq.empty, resultSymbols)

  override def notifications(planContext: PlanContext) = Seq.empty
  override def isPeriodicCommit: Boolean = false
  override def runtimeUsed = ProcedureRuntimeName
  override def isStale(lastTxId: () => Long, statistics: GraphStatistics) = false
  override def plannerUsed = ProcedurePlannerName
}

