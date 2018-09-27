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
package org.neo4j.cypher.internal.runtime.slotted.expressions

import org.neo4j.cypher.internal.compatibility.v3_5.runtime.SlotAllocation.PhysicalPlan
import org.neo4j.cypher.internal.planner.v3_5.spi.TokenContext
import org.neo4j.cypher.internal.runtime.compiled.expressions.{CodeGeneration, CompiledExpression, CompiledProjection, IntermediateCodeGeneration}
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.{CommunityExpressionConverter, ExpressionConverter, ExpressionConverters}
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.{Expression, ExtendedExpression, RandFunction}
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{Pipe, QueryState}
import org.neo4j.cypher.internal.runtime.interpreted.{CommandProjection, ExecutionContext}
import org.neo4j.cypher.internal.runtime.slotted.expressions.CompiledExpressionConverter.COMPILE_LIMIT
import org.neo4j.logging.Log
import org.neo4j.values.AnyValue
import org.opencypher.v9_0.expressions.FunctionInvocation
import org.opencypher.v9_0.expressions.functions.AggregatingFunction
import org.opencypher.v9_0.util.attribution.Id
import org.opencypher.v9_0.{expressions => ast}

class CompiledExpressionConverter(log: Log, physicalPlan: PhysicalPlan, tokenContext: TokenContext) extends ExpressionConverter {

  //uses an inner converter to simplify compliance with Expression trait
  private val inner = new ExpressionConverters(SlottedExpressionConverters(physicalPlan), CommunityExpressionConverter(tokenContext))

  override def toCommandExpression(id: Id, expression: ast.Expression,
                                   self: ExpressionConverters): Option[Expression] = expression match {

    //we don't deal with aggregations
    case f: FunctionInvocation if f.function.isInstanceOf[AggregatingFunction] => None

     // don't bother with small expressions, not worth it
    case e if sizeOf(e) > COMPILE_LIMIT => try {
      val ir = new IntermediateCodeGeneration(physicalPlan.slotConfigurations(id)).compileExpression(e)
      if (ir.nonEmpty) {
        log.debug(s"Compiling expression: $e")
      }
      ir.map(i => CompileWrappingExpression(CodeGeneration.compileExpression(i),
                                                         inner.toCommandExpression(id, expression)))
    } catch {
      case t: Throwable =>
        //Something horrible happened, maybe we exceeded the bytecode size or introduced a bug so that we tried
        //to load invalid bytecode, whatever is the case we should silently fallback to the next expression
        //converter
        log.debug(s"Failed to compile expression: $e", t)
        None
    }
    case _ => None
  }

  import org.opencypher.v9_0.util.Foldable._

  private def sizeOf(expression: ast.Expression)= expression.treeCount {
    case _: ast.Expression => true
  }

  override def toCommandProjection(id: Id, projections: Map[String, ast.Expression],
                                   self: ExpressionConverters): Option[CommandProjection] = {
    try {
      val totalSize = projections.values.foldLeft(0)((acc, current) => acc + sizeOf(current))
      if (totalSize > COMPILE_LIMIT) {
        val slots = physicalPlan.slotConfigurations(id)
        val compiler = new IntermediateCodeGeneration(slots)
        val compiled = for {(k, v) <- projections
                            c <- compiler.compileExpression(v)} yield slots.get(k).get.offset -> c
        if (compiled.size < projections.size) None
        else {
          log.debug(s" Compiling projection: $projections")
          Some(CompileWrappingProjection(CodeGeneration.compileProjection(compiler.compileProjection(compiled)),
                                         projections.isEmpty))
        }
      } else None
    }
    catch {
      case t: Throwable =>
        //Something horrible happened, maybe we exceeded the bytecode size or introduced a bug so that we tried
        //to load invalid bytecode, whatever is the case we should silently fallback to the next expression
        //converter
        log.debug(s"Failed to compile projection: $projections", t)
         None
    }
  }
}

object CompiledExpressionConverter {
  private val COMPILE_LIMIT = 2
}

case class CompileWrappingProjection(projection: CompiledProjection, isEmpty: Boolean) extends CommandProjection {

  override def registerOwningPipe(pipe: Pipe): Unit = {}

  override def project(ctx: ExecutionContext, state: QueryState): Unit = projection.project(ctx, state.query, state.params)
}

case class CompileWrappingExpression(ce: CompiledExpression, legacy: Expression) extends ExtendedExpression {

  override def rewrite(f: Expression => Expression): Expression = f(this)

  override def arguments: Seq[Expression] = legacy.arguments

  override def apply(ctx: ExecutionContext, state: QueryState): AnyValue =
    ce.evaluate(ctx, state.query, state.params)

  override def symbolTableDependencies: Set[String] = legacy.symbolTableDependencies

  override def toString: String = legacy.toString

  override val isDeterministic: Boolean = !legacy.exists {
    case RandFunction() => true
    case _              => false
  }
}
