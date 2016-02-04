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

import org.neo4j.cypher.internal.compiler.v3_0.ast.convert.commands.ExpressionConverters._
import org.neo4j.cypher.internal.compiler.v3_0.executionplan.{ExecutionPlan, InternalExecutionResult, READ_ONLY}
import org.neo4j.cypher.internal.compiler.v3_0.pipes.{ExternalCSVResource, QueryState}
import org.neo4j.cypher.internal.compiler.v3_0.planDescription.{Id, NoChildren, PlanDescriptionImpl}
import org.neo4j.cypher.internal.compiler.v3_0.spi._
import org.neo4j.cypher.internal.compiler.v3_0.{ExecutionContext, ExecutionMode, ExplainExecutionResult, ExplainMode, ProcedurePlannerName, ProcedureRuntimeName, TaskCloser}
import org.neo4j.cypher.internal.frontend.v3_0.{ParameterNotFoundException, InvalidArgumentException}
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
case class CallProcedureExecutionPlan(signature: ProcedureSignature, providedArgExprs: Option[Seq[Expression]]) extends ExecutionPlan {

  private val optArgCommandExprs  = providedArgExprs.map { args => args.map(toCommandExpression) }

  override def run(ctx: QueryContext, planType: ExecutionMode,
                   params: Map[String, Any]): InternalExecutionResult = {

    val state = new QueryState(ctx, ExternalCSVResource.empty, params)

    val input = optArgCommandExprs.map { exprs => exprs.map(_.apply(ExecutionContext.empty)(state)) }.getOrElse {
      signature.inputSignature.map { f => params.getOrElse(f.name, fail(f, ctx)) }
    }

    val taskCloser = new TaskCloser
    taskCloser.addTask(ctx.close)
    if (planType == ExplainMode) {
      //close all statements
      taskCloser.close(success = true)
      new ExplainExecutionResult(signature.outputSignature.seq.map(_.name).toList,
        description, READ_ONLY, Set.empty)
    } else {
      val result: ProcedureExecutionResult[Nothing] = ProcedureExecutionResult(taskCloser, ctx, signature, input, description, planType)
      // Naive eagerization for now, refactor as need arises
      if( signature.mode == ProcReadWrite )
        result.toEagerIterableResult(ProcedurePlannerName, ProcedureRuntimeName)
      else result

    }
  }

  private def fail(f: FieldSignature, ctx: QueryContext) = {
    ctx.close(success = false)
    throw new ParameterNotFoundException(
      s"""Procedure ${signature.name.name} expected an argument with ${f.name} with type ${f.typ}"""
    )
  }

  private def description = PlanDescriptionImpl(new Id, "ProcedureCall", NoChildren, Seq(), Set.empty)

  override def notifications = Seq.empty

  override def isPeriodicCommit: Boolean = false

  override def runtimeUsed = ProcedureRuntimeName

  override def isStale(lastTxId: () => Long, statistics: GraphStatistics) = false

  override def plannerUsed = ProcedurePlannerName
}

