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
package org.neo4j.cypher.internal.compatibility.v3_5.runtime.executionplan.procs

import org.neo4j.cypher.internal.compatibility.v3_5.runtime._
import org.neo4j.cypher.internal.compatibility.v3_5.runtime.executionplan.ExecutionPlan
import org.neo4j.cypher.internal.runtime._
import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.ExpressionConverters
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.{Literal, ParameterExpression, Expression => CommandExpression}
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{ExternalCSVResource, QueryState}
import org.neo4j.cypher.internal.runtime.planDescription.Argument
import org.neo4j.cypher.internal.v3_5.expressions.Expression
import org.neo4j.cypher.internal.v3_5.logical.plans.ProcedureSignature
import org.neo4j.cypher.internal.v3_5.util.attribution.Id
import org.neo4j.cypher.internal.v3_5.util.symbols.CypherType
import org.neo4j.cypher.internal.v3_5.util.{InternalNotification, InvalidArgumentException}
import org.neo4j.cypher.result.RuntimeResult
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext
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
                                      var resultIndices: Seq[(Int, (String, String))],
                                      converter: ExpressionConverters,
                                      id: Id)
  extends ExecutionPlan {

  assert(resultSymbols.size == resultIndices.size)

  private val resultMappings = resultSymbols.indices.map(i => {
    try {
      val r = resultIndices(i)
      (r._1, r._2._1, resultSymbols(i)._2)
    }
    catch {
      case _:ClassCastException =>
        // When older planners are used, due to compatibility reasons, resultIndices can in fact be filled with Seq[(Int, String)] even though it should not be possible
        // Thus we need convert resultIndices into the new form. To do that we have to trick scala a bit because its type inference system is wrong here
        this.resultIndices = resultIndices.map(t => (t._1, (t._2.asInstanceOf[String], t._2.asInstanceOf[String])))
        val r = resultIndices(i)
        (r._1, r._2._1, resultSymbols(i)._2)
    }
  })

  private def createProcedureCallContext(): ProcedureCallContext ={
    // getting the original name of the yielded variable
    new ProcedureCallContext( resultIndices.map(_._2._2).toArray, true )
  }

  private val actualArgs: Seq[CommandExpression] =  argExprs.map(converter.toCommandExpression(id, _)) // This list can be shorter than signature.inputSignature.length
  private val parameterArgs: Seq[ParameterExpression] =  signature.inputSignature.map(s => ParameterExpression(s.name))
  private val maybeDefaultArgs: Seq[Option[CommandExpression]] =  signature.inputSignature.map(_.default).map(option => option.map( df => Literal(df.value)))
  private val zippedArgCandidates = actualArgs.map(Some(_)).zipAll(parameterArgs.zip(maybeDefaultArgs), None, null).map { case (a, (b, c)) => (a, b, c)}

  override def run(ctx: QueryContext, doProfile: Boolean, params: MapValue): RuntimeResult = {
    val input = evaluateArguments(ctx, params)
    val callMode = ProcedureCallMode.fromAccessMode(signature.accessMode)
    new ProcedureCallRuntimeResult(ctx, signature.name, signature.id, callMode, input, resultMappings, doProfile, createProcedureCallContext())
  }

  private def evaluateArguments(ctx: QueryContext, params: MapValue): Seq[Any] = {
    val state = new QueryState(ctx, ExternalCSVResource.empty, params)
    val args = zippedArgCandidates.map {
      // an actual argument (or even a parameter that ResolvedCall puts there instead if there is no default value)
      case (Some(actualArg), _, _) => actualArg
      // There is a default value, but also a parameter that should be preferred
      case (_, paramArg@ParameterExpression(name), _) if params.containsKey(name) => paramArg
      // There is a default value
      case (_, _, Some(defaultArg)) => defaultArg
      // There is nothing we can use
      case (_, ParameterExpression(name), _) => throw new InvalidArgumentException(s"Invalid procedure call. Parameter for $name not specified.")
    }

    args.map(expr => ctx.asObject(expr.apply(ExecutionContext.empty, state)))
  }

  override def runtimeName: RuntimeName = ProcedureRuntimeName

  override def metadata: Seq[Argument] = Nil

  override def notifications: Set[InternalNotification] = Set.empty
}

