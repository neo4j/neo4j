/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_0.commands.expressions

import org.neo4j.cypher.internal.compiler.v2_0._
import pipes.QueryState
import symbols._
import org.neo4j.cypher.CypherTypeException
import java.lang.Math

abstract class MathFunction(arg: Expression) extends Expression with NumericHelper {
  def innerExpectedType = NumberType()

  def arguments = Seq(arg)

  def calculateType(symbols: SymbolTable) = arg.evaluateType(NumberType(), symbols)

  def symbolTableDependencies = arg.symbolTableDependencies
}

trait NumericHelper {
  protected def asDouble(a: Any) = asNumber(a).doubleValue()
  protected def asInt(a: Any) = asNumber(a).intValue()

  private def asNumber(a: Any): Number = a match {
    case null     => throw new CypherTypeException("Expected a numeric value for " + toString + ", but got null")
    case a:Number => a
    case _        => throw new CypherTypeException("Expected a numeric value for " + toString + ", but got: " + a.toString)
  }
}

case class AbsFunction(argument: Expression) extends MathFunction(argument) {
  def apply(ctx: ExecutionContext)(implicit state: QueryState): Any = Math.abs(asDouble(argument(ctx)))

  def rewrite(f: (Expression) => Expression) = f(AbsFunction(argument.rewrite(f)))
}

case class AcosFunction(argument: Expression) extends MathFunction(argument) {
  def apply(ctx: ExecutionContext)(implicit state: QueryState): Any = math.acos(asDouble(argument(ctx)))

  def rewrite(f: (Expression) => Expression) = f(AcosFunction(argument.rewrite(f)))

  override def calculateType(symbols: SymbolTable) = DoubleType()
}

case class AsinFunction(argument: Expression) extends MathFunction(argument) {
  def apply(ctx: ExecutionContext)(implicit state: QueryState): Any = math.asin(asDouble(argument(ctx)))

  def rewrite(f: (Expression) => Expression) = f(AsinFunction(argument.rewrite(f)))

  override def calculateType(symbols: SymbolTable) = DoubleType()
}

case class AtanFunction(argument: Expression) extends MathFunction(argument) {
  def apply(ctx: ExecutionContext)(implicit state: QueryState): Any = math.atan(asDouble(argument(ctx)))

  def rewrite(f: (Expression) => Expression) = f(AtanFunction(argument.rewrite(f)))

  override def calculateType(symbols: SymbolTable) = DoubleType()
}

case class Atan2Function(y: Expression, x: Expression) extends Expression with NumericHelper {
  def apply(ctx: ExecutionContext)(implicit state: QueryState): Any = math.atan2(asDouble(y(ctx)), asDouble(x(ctx)))

  def arguments = Seq(x, y)

  def rewrite(f: (Expression) => Expression) = f(Atan2Function(y.rewrite(f), x.rewrite(f)))

  def calculateType(symbols: SymbolTable): CypherType = DoubleType()

  def symbolTableDependencies = x.symbolTableDependencies ++ y.symbolTableDependencies
}

case class CeilFunction(argument: Expression) extends MathFunction(argument) {
  def apply(ctx: ExecutionContext)(implicit state: QueryState): Any = math.ceil(asDouble(argument(ctx)))

  def rewrite(f: (Expression) => Expression) = f(CeilFunction(argument.rewrite(f)))
}

case class CosFunction(argument: Expression) extends MathFunction(argument) {
  def apply(ctx: ExecutionContext)(implicit state: QueryState): Any = math.cos(asDouble(argument(ctx)))

  def rewrite(f: (Expression) => Expression) = f(CosFunction(argument.rewrite(f)))

  override def calculateType(symbols: SymbolTable) = DoubleType()
}

case class CotFunction(argument: Expression) extends MathFunction(argument) {
  def apply(ctx: ExecutionContext)(implicit state: QueryState): Any = 1.0/math.tan(asDouble(argument(ctx)))

  def rewrite(f: (Expression) => Expression) = f(CotFunction(argument.rewrite(f)))

  override def calculateType(symbols: SymbolTable) = DoubleType()
}

case class DegreesFunction(argument: Expression) extends MathFunction(argument) {
  def apply(ctx: ExecutionContext)(implicit state: QueryState): Any = math.toDegrees(asDouble(argument(ctx)))

  def rewrite(f: (Expression) => Expression) = f(DegreesFunction(argument.rewrite(f)))

  override def calculateType(symbols: SymbolTable) = DoubleType()
}

case class EFunction() extends Expression() {
  def apply(ctx: ExecutionContext)(implicit state: QueryState): Any = math.E

  def arguments = Seq()

  def symbolTableDependencies = Set[String]()

  def rewrite(f: (Expression) => Expression) = f(EFunction())

  def calculateType(symbols: SymbolTable) = DoubleType()
}

case class ExpFunction(argument: Expression) extends MathFunction(argument) {
  def apply(ctx: ExecutionContext)(implicit state: QueryState): Any = math.exp(asDouble(argument(ctx)))

  def rewrite(f: (Expression) => Expression) = f(ExpFunction(argument.rewrite(f)))

  override def calculateType(symbols: SymbolTable) = DoubleType()
}

case class FloorFunction(argument: Expression) extends MathFunction(argument) {
  def apply(ctx: ExecutionContext)(implicit state: QueryState): Any = math.floor(asDouble(argument(ctx)))

  def rewrite(f: (Expression) => Expression) = f(FloorFunction(argument.rewrite(f)))
}

case class LogFunction(argument: Expression) extends MathFunction(argument) {
  def apply(ctx: ExecutionContext)(implicit state: QueryState): Any = math.log(asDouble(argument(ctx)))

  def rewrite(f: (Expression) => Expression) = f(LogFunction(argument.rewrite(f)))

  override def calculateType(symbols: SymbolTable) = DoubleType()
}

case class Log10Function(argument: Expression) extends MathFunction(argument) {
  def apply(ctx: ExecutionContext)(implicit state: QueryState): Any = math.log10(asDouble(argument(ctx)))

  def rewrite(f: (Expression) => Expression) = f(Log10Function(argument.rewrite(f)))

  override def calculateType(symbols: SymbolTable) = DoubleType()
}

case class PiFunction() extends Expression {
  def apply(ctx: ExecutionContext)(implicit state: QueryState): Any = math.Pi

  def arguments = Seq()

  def symbolTableDependencies = Set()

  def rewrite(f: (Expression) => Expression) = f(PiFunction())

  def calculateType(symbols: SymbolTable) = DoubleType()
}

case class RadiansFunction(argument: Expression) extends MathFunction(argument) {
  def apply(ctx: ExecutionContext)(implicit state: QueryState): Any = math.toRadians(asDouble(argument(ctx)))

  def rewrite(f: (Expression) => Expression) = f(RadiansFunction(argument.rewrite(f)))

  override def calculateType(symbols: SymbolTable) = DoubleType()
}

case class SinFunction(argument: Expression) extends MathFunction(argument) {
  def apply(ctx: ExecutionContext)(implicit state: QueryState): Any = math.sin(asDouble(argument(ctx)))

  def rewrite(f: (Expression) => Expression) = f(SinFunction(argument.rewrite(f)))

  override def calculateType(symbols: SymbolTable) = DoubleType()
}

case class HaversinFunction(argument: Expression) extends MathFunction(argument) {

  def apply(ctx: ExecutionContext)(implicit state: QueryState): Any = ( 1.0d - math.cos(asDouble(argument(ctx))) ) / 2

  def rewrite(f: (Expression) => Expression) = f(HaversinFunction(argument.rewrite(f)))

  override def calculateType(symbols: SymbolTable) = DoubleType()
}

case class TanFunction(argument: Expression) extends MathFunction(argument) {
  def apply(ctx: ExecutionContext)(implicit state: QueryState): Any = math.tan(asDouble(argument(ctx)))

  def rewrite(f: (Expression) => Expression) = f(TanFunction(argument.rewrite(f)))

  override def calculateType(symbols: SymbolTable) = DoubleType()
}

case class RandFunction() extends Expression {
  def apply(ctx: ExecutionContext)(implicit state: QueryState): Any = math.random

  def arguments = Seq()

  def symbolTableDependencies = Set[String]()

  def rewrite(f: (Expression) => Expression) = f(RandFunction())

  def calculateType(symbols: SymbolTable) = DoubleType()
}

case class RangeFunction(start: Expression, end: Expression, step: Expression) extends Expression with NumericHelper {
  def apply(ctx: ExecutionContext)(implicit state: QueryState): Any = {
    val startVal = asInt(start(ctx))
    val endVal = asInt(end(ctx))
    val stepVal = asInt(step(ctx))
    new Range(startVal, endVal + 1, stepVal).toList
  }

  def arguments = Seq(start, end, step)

  def rewrite(f: (Expression) => Expression) = f(RangeFunction(start.rewrite(f), end.rewrite(f), step.rewrite(f)))

  def calculateType(symbols: SymbolTable): CypherType = {
    start.evaluateType(NumberType(), symbols)
    end.evaluateType(NumberType(), symbols)
    step.evaluateType(NumberType(), symbols)
    CollectionType(NumberType())
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
