/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_0.executionplan.procs

import org.neo4j.cypher.internal.compiler.v3_0._
import org.neo4j.cypher.internal.compiler.v3_0.ast.convert.commands.ExpressionConverters._
import org.neo4j.cypher.internal.compiler.v3_0.executionplan.{ExecutionPlan, InternalExecutionResult, READ_ONLY}
import org.neo4j.cypher.internal.compiler.v3_0.helpers.Counter
import org.neo4j.cypher.internal.compiler.v3_0.pipes.{ExternalCSVResource, QueryState}
import org.neo4j.cypher.internal.compiler.v3_0.planDescription.InternalPlanDescription.Arguments.{DbHits, Rows}
import org.neo4j.cypher.internal.compiler.v3_0.planDescription.{Id, NoChildren, PlanDescriptionImpl}
import org.neo4j.cypher.internal.compiler.v3_0.spi._
import org.neo4j.cypher.internal.frontend.v3_0.ParameterNotFoundException
import org.neo4j.cypher.internal.frontend.v3_0.ast.Expression

/**
  * Execution plan for calling procedures
  *
  * a procedure can be called in two ways, either `CALL proc(a,b)` or as `CALL proc` with `a` and `b` provided as
  * parameters to `run`. In the former case type checking should be done before instantiating this class, in the
  * latter case we will have to resort to runtime type checking.
  *
  * @param signature the signature of the procedure
  * @param providedArgExprs the argument to the procedure
  */
case class CallProcedureExecutionPlan(signature: ProcedureSignature, providedArgExprs: Option[Seq[Expression]])
  extends ExecutionPlan {

  private val optArgCommandExprs  = providedArgExprs.map { args => args.map(toCommandExpression) }

  override def run(ctx: QueryContext, planType: ExecutionMode, params: Map[String, Any]): InternalExecutionResult = {
    val input = evaluateArguments(ctx, params)

    val taskCloser = new TaskCloser
    taskCloser.addTask(ctx.transactionalContext.close)

    planType match {
      case NormalMode => createNormalExecutionResult(ctx, taskCloser, input, planType)
      case ExplainMode => createExplainedExecutionResult(ctx, taskCloser, input)
      case ProfileMode => createProfiledExecutionResult(ctx, taskCloser, input, planType)
    }
  }

  private def createNormalExecutionResult(ctx: QueryContext, taskCloser: TaskCloser,
                                          input: Seq[Any], planType: ExecutionMode) = {
    val descriptionGenerator = () => createNormalPlan
    new ProcedureExecutionResult(ctx, taskCloser, signature, input, descriptionGenerator, planType)
  }

  private def createExplainedExecutionResult(ctx: QueryContext, taskCloser: TaskCloser, input: Seq[Any]) = {
    // close all statements
    taskCloser.close(success = true)
    val columns = signature.outputSignature.seq.map(_.name).toList
    new ExplainExecutionResult(columns, createNormalPlan, READ_ONLY, Set.empty)
  }

  private def createProfiledExecutionResult(ctx: QueryContext, taskCloser: TaskCloser,
                                            input: Seq[Any], planType: ExecutionMode) = {
    val rowCounter = Counter()
    val descriptionGenerator = createProfilePlanGenerator(rowCounter)
    new ProcedureExecutionResult(ctx, taskCloser, signature, input, descriptionGenerator, planType) {
      override protected def executeCall: Iterator[Array[AnyRef]] = rowCounter.track(super.executeCall)
    }
  }

  private def evaluateArguments(ctx: QueryContext, params: Map[String, Any]): Seq[Any] = {
    val state = new QueryState(ctx, ExternalCSVResource.empty, params)
    optArgCommandExprs.map { exprs => exprs.map(_.apply(ExecutionContext.empty)(state)) }.getOrElse {
      signature.inputSignature.map { f => params.getOrElse(f.name, fail(f, ctx)) }
    }
  }

  private def createNormalPlan =
    PlanDescriptionImpl(new Id, "ProcedureCall", NoChildren, Seq(), Set.empty)

  private def createProfilePlanGenerator(rowCounter: Counter) = () =>
    PlanDescriptionImpl(new Id, "ProcedureCall", NoChildren, Seq(DbHits(1), Rows(rowCounter.counted)), Set.empty)

  private def fail(f: FieldSignature, ctx: QueryContext) = {
    ctx.transactionalContext.close(success = false)
    throw new ParameterNotFoundException(
      s"""Procedure ${signature.name.name} expected an argument with ${f.name} with type ${f.typ}"""
    )
  }

  override def notifications(planContext: PlanContext) = Seq.empty
  override def isPeriodicCommit: Boolean = false
  override def runtimeUsed = ProcedureRuntimeName
  override def isStale(lastTxId: () => Long, statistics: GraphStatistics) = false
  override def plannerUsed = ProcedurePlannerName
}

