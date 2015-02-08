/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v1_9.commands.expressions

import java.lang.Math
import org.neo4j.cypher.CypherTypeException
import org.neo4j.cypher.internal.compiler.v1_9.symbols._
import org.neo4j.cypher.internal.compiler.v1_9.ExecutionContext
import org.neo4j.cypher.internal.compiler.v1_9.pipes.QueryState

abstract class MathFunction(arg: Expression) extends Expression with NumericHelper {
  def innerExpectedType = NumberType()

  def children = Seq(arg)

  def calculateType(symbols: SymbolTable) = arg.evaluateType(NumberType(), symbols)

  def symbolTableDependencies = arg.symbolTableDependencies
}

trait NumericHelper {
  protected def asDouble(a: Any) = asNumber(a).doubleValue()
  protected def asInt(a: Any) = asNumber(a).intValue()

  private def asNumber(a: Any): Number = try {
    a.asInstanceOf[Number]
  }
  catch {
    case x: ClassCastException => throw new CypherTypeException("Expected a numeric value for " + toString + ", but got: " + a.toString)
  }
}

case class AbsFunction(argument: Expression) extends MathFunction(argument) {
  def apply(ctx: ExecutionContext)(implicit state: QueryState): Any = Math.abs(asDouble(argument(ctx)))

  def rewrite(f: (Expression) => Expression) = f(AbsFunction(argument.rewrite(f)))
}

case class RangeFunction(start: Expression, end: Expression, step: Expression) extends Expression with NumericHelper {
  def apply(ctx: ExecutionContext)(implicit state: QueryState): Any = {
    val startVal = asInt(start(ctx))
    val endVal = asInt(end(ctx))
    val stepVal = asInt(step(ctx))
    new Range(startVal, endVal + 1, stepVal).toList
  }

  def children = Seq(start, end, step)

  def rewrite(f: (Expression) => Expression) = f(RangeFunction(start.rewrite(f), end.rewrite(f), step.rewrite(f)))

  def calculateType(symbols: SymbolTable): CypherType = {
    start.evaluateType(NumberType(), symbols)
    end.evaluateType(NumberType(), symbols)
    step.evaluateType(NumberType(), symbols)
    new CollectionType(NumberType())
  }

  def symbolTableDependencies = start.symbolTableDependencies ++
    end.symbolTableDependencies ++
    step.symbolTableDependencies
}

case class SignFunction(argument: Expression) extends MathFunction(argument) {
  def apply(ctx: ExecutionContext)(implicit state: QueryState): Any = Math.signum(asDouble(argument(ctx)))

  def rewrite(f: (Expression) => Expression) = f(SignFunction(argument.rewrite(f)))
}

case class RoundFunction(expression: Expression) extends MathFunction(expression) {
  def apply(ctx: ExecutionContext)(implicit state: QueryState): Any = math.round(asDouble(expression(ctx)))

  def rewrite(f: (Expression) => Expression) = f(RoundFunction(expression.rewrite(f)))
}

case class SqrtFunction(argument: Expression) extends MathFunction(argument) {
  def apply(ctx: ExecutionContext)(implicit state: QueryState): Any = Math.sqrt(asDouble(argument(ctx)))

  def rewrite(f: (Expression) => Expression) = f(SqrtFunction(argument.rewrite(f)))
}
