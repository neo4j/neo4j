/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.commands.expressions

import org.neo4j.cypher.internal.compiler.v2_3._
import org.neo4j.cypher.internal.compiler.v2_3.pipes.QueryState
import org.neo4j.cypher.internal.compiler.v2_3.symbols.SymbolTable
import org.neo4j.cypher.internal.frontend.v2_3.symbols._
import org.neo4j.cypher.internal.frontend.v2_3.{CypherTypeException, InvalidArgumentException}

abstract class MathFunction(arg: Expression) extends Expression with NumericHelper {
  def innerExpectedType = CTNumber

  def arguments = Seq(arg)

  def calculateType(symbols: SymbolTable) = arg.evaluateType(CTNumber, symbols)

  def symbolTableDependencies = arg.symbolTableDependencies
}

abstract class NullSafeMathFunction(arg: Expression) extends MathFunction(arg) {
  override def apply(ctx: ExecutionContext)(implicit state: QueryState): Any = {
    val value = arg(ctx)
    if (null == value) null else apply(asDouble(value))
  }

  def apply(value: Double): Double
}

trait NumericHelper {
  protected def asLongEntityId(a: Any): Long = a match {
    case _ if a.isInstanceOf[Double] || a.isInstanceOf[Float] =>
      throw new CypherTypeException("Expected entity id to be an integral value")
    case _  =>
      asLong(a)
  }

  protected def asDouble(a: Any) = asNumber(a).doubleValue()
  protected def asInt(a: Any) = asNumber(a).intValue()
  protected def asLong(a: Any) = asNumber(a).longValue()

  private def asNumber(a: Any): Number = a match {
    case null     => throw new CypherTypeException("Expected a numeric value for " + toString + ", but got null")
    case a:Number => a
    case _        => throw new CypherTypeException("Expected a numeric value for " + toString + ", but got: " + a.toString)
  }
}

case class AbsFunction(argument: Expression) extends NullSafeMathFunction(argument) {
  def apply(value: Double): Double = Math.abs(value)

  def rewrite(f: (Expression) => Expression) = f(AbsFunction(argument.rewrite(f)))
}

case class AcosFunction(argument: Expression) extends NullSafeMathFunction(argument) {
  def apply(value: Double): Double = Math.acos(value)

  def rewrite(f: (Expression) => Expression) = f(AcosFunction(argument.rewrite(f)))

  override def calculateType(symbols: SymbolTable) = CTFloat
}

case class AsinFunction(argument: Expression) extends NullSafeMathFunction(argument) {
  def apply(value: Double): Double = Math.asin(value)

  def rewrite(f: (Expression) => Expression) = f(AsinFunction(argument.rewrite(f)))

  override def calculateType(symbols: SymbolTable) = CTFloat
}

case class AtanFunction(argument: Expression) extends NullSafeMathFunction(argument) {
  def apply(value: Double): Double = Math.atan(value)

  def rewrite(f: (Expression) => Expression) = f(AtanFunction(argument.rewrite(f)))

  override def calculateType(symbols: SymbolTable) = CTFloat
}

case class Atan2Function(y: Expression, x: Expression) extends Expression with NumericHelper {
  def apply(ctx: ExecutionContext)(implicit state: QueryState): Any = {
    val yValue = y(ctx)
    val xValue = x(ctx)
    if (null == yValue || null == xValue)
      null
    else
      math.atan2(asDouble(yValue), asDouble(xValue))
  }

  def arguments = Seq(x, y)

  def rewrite(f: (Expression) => Expression) = f(Atan2Function(y.rewrite(f), x.rewrite(f)))

  def calculateType(symbols: SymbolTable): CypherType = CTFloat

  def symbolTableDependencies = x.symbolTableDependencies ++ y.symbolTableDependencies
}

case class CeilFunction(argument: Expression) extends NullSafeMathFunction(argument) {
  def apply(value: Double) = math.ceil(value)

  def rewrite(f: (Expression) => Expression) = f(CeilFunction(argument.rewrite(f)))
}

case class CosFunction(argument: Expression) extends NullSafeMathFunction(argument) {
  def apply(value: Double): Double = math.cos(value)

  def rewrite(f: (Expression) => Expression) = f(CosFunction(argument.rewrite(f)))

  override def calculateType(symbols: SymbolTable) = CTFloat
}

case class CotFunction(argument: Expression) extends NullSafeMathFunction(argument) {
  def apply(value: Double): Double = 1.0/math.tan(value)

  def rewrite(f: (Expression) => Expression) = f(CotFunction(argument.rewrite(f)))

  override def calculateType(symbols: SymbolTable) = CTFloat
}

case class DegreesFunction(argument: Expression) extends NullSafeMathFunction(argument) {
  def apply(value: Double): Double = math.toDegrees(value)

  def rewrite(f: (Expression) => Expression) = f(DegreesFunction(argument.rewrite(f)))

  override def calculateType(symbols: SymbolTable) = CTFloat
}

case class EFunction() extends Expression() {
  def apply(ctx: ExecutionContext)(implicit state: QueryState): Any = math.E

  def arguments = Seq()

  def symbolTableDependencies = Set[String]()

  def rewrite(f: (Expression) => Expression) = f(EFunction())

  def calculateType(symbols: SymbolTable) = CTFloat
}

case class ExpFunction(argument: Expression) extends NullSafeMathFunction(argument) {
  def apply(value: Double): Double = math.exp(value)

  def rewrite(f: (Expression) => Expression) = f(ExpFunction(argument.rewrite(f)))

  override def calculateType(symbols: SymbolTable) = CTFloat
}

case class FloorFunction(argument: Expression) extends NullSafeMathFunction(argument) {
  def apply(value: Double) =  math.floor(value)

  def rewrite(f: (Expression) => Expression) = f(FloorFunction(argument.rewrite(f)))
}

case class LogFunction(argument: Expression) extends NullSafeMathFunction(argument) {
  def apply(value: Double) = math.log(value)

  def rewrite(f: (Expression) => Expression) = f(LogFunction(argument.rewrite(f)))

  override def calculateType(symbols: SymbolTable) = CTFloat
}

case class Log10Function(argument: Expression) extends NullSafeMathFunction(argument) {
  def apply(value: Double) = math.log10(value)

  def rewrite(f: (Expression) => Expression) = f(Log10Function(argument.rewrite(f)))

  override def calculateType(symbols: SymbolTable) = CTFloat
}

case class PiFunction() extends Expression {
  def apply(ctx: ExecutionContext)(implicit state: QueryState): Any = math.Pi

  def arguments = Seq()

  def symbolTableDependencies = Set()

  def rewrite(f: (Expression) => Expression) = f(PiFunction())

  def calculateType(symbols: SymbolTable) = CTFloat
}

case class RadiansFunction(argument: Expression) extends NullSafeMathFunction(argument) {
  def apply(value: Double) = math.toRadians(value)

  def rewrite(f: (Expression) => Expression) = f(RadiansFunction(argument.rewrite(f)))

  override def calculateType(symbols: SymbolTable) = CTFloat
}

case class SinFunction(argument: Expression) extends NullSafeMathFunction(argument) {
  def apply(value: Double) = math.sin(value)

  def rewrite(f: (Expression) => Expression) = f(SinFunction(argument.rewrite(f)))

  override def calculateType(symbols: SymbolTable) = CTFloat
}

case class HaversinFunction(argument: Expression) extends NullSafeMathFunction(argument) {
  def apply(value: Double) = ( 1.0d - math.cos(value) ) / 2

  def rewrite(f: (Expression) => Expression) = f(HaversinFunction(argument.rewrite(f)))

  override def calculateType(symbols: SymbolTable) = CTFloat
}

case class TanFunction(argument: Expression) extends NullSafeMathFunction(argument) {
  def apply(value: Double) = math.tan(value)

  def rewrite(f: (Expression) => Expression) = f(TanFunction(argument.rewrite(f)))

  override def calculateType(symbols: SymbolTable) = CTFloat
}

case class RandFunction() extends Expression {
  def apply(ctx: ExecutionContext)(implicit state: QueryState): Any = math.random

  def arguments = Seq()

  def symbolTableDependencies = Set[String]()

  def rewrite(f: (Expression) => Expression) = f(RandFunction())

  def calculateType(symbols: SymbolTable) = CTFloat
}

case class RangeFunction(start: Expression, end: Expression, step: Expression) extends Expression with NumericHelper {
  def apply(ctx: ExecutionContext)(implicit state: QueryState): Any = {
    val stepVal = asLong(step(ctx))
    if (stepVal == 0L)
      throw new InvalidArgumentException("step argument to range() cannot be zero")

    val startVal = asLong(start(ctx))
    val inclusiveEndVal = asLong(end(ctx))
    val check: (Long, Long) => Boolean = if (stepVal.signum > 0) _ <= _ else _ >= _

    // due to the limitations of the scala collection library we need to implement iterator on long ranges manually:
    // the scala one cannot be longer than MaxInt in length since it is an IndexedSeq which is indexed by Ints... :(
    new Iterable[Long] {
      override def iterator: Iterator[Long] = new Iterator[Long] {
        private var current = startVal

        override def hasNext: Boolean = check(current, inclusiveEndVal)

        override def next(): Long = {
          val c = current
          current = current + stepVal
          c
        }
      }
    }
  }

  def arguments = Seq(start, end, step)

  def rewrite(f: (Expression) => Expression) = f(RangeFunction(start.rewrite(f), end.rewrite(f), step.rewrite(f)))

  def calculateType(symbols: SymbolTable): CypherType = {
    start.evaluateType(CTNumber, symbols)
    end.evaluateType(CTNumber, symbols)
    step.evaluateType(CTNumber, symbols)
    CTCollection(CTNumber)
  }

  def symbolTableDependencies = start.symbolTableDependencies ++
    end.symbolTableDependencies ++
    step.symbolTableDependencies
}

case class SignFunction(argument: Expression) extends NullSafeMathFunction(argument) {
  def apply(value: Double) =  Math.signum(value)

  def rewrite(f: (Expression) => Expression) = f(SignFunction(argument.rewrite(f)))
}

case class RoundFunction(expression: Expression) extends NullSafeMathFunction(expression) {
  def apply(value: Double) =  math.round(value)

  def rewrite(f: (Expression) => Expression) = f(RoundFunction(expression.rewrite(f)))
}

case class SqrtFunction(argument: Expression) extends NullSafeMathFunction(argument) {
  def apply(value: Double) = Math.sqrt(value)

  def rewrite(f: (Expression) => Expression) = f(SqrtFunction(argument.rewrite(f)))
}
