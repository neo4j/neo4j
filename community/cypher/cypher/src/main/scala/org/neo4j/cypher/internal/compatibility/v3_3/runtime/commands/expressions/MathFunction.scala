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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.expressions

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.ExecutionContext
import org.neo4j.cypher.internal.compatibility.v3_3.runtime._
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes.QueryState
import org.neo4j.cypher.internal.frontend.v3_3.symbols._
import org.neo4j.cypher.internal.frontend.v3_3.{CypherTypeException, InvalidArgumentException}
import org.neo4j.values._
import org.neo4j.values.storable._
import org.neo4j.values.virtual.VirtualValues

abstract class MathFunction(arg: Expression) extends Expression with NumericHelper {

  def innerExpectedType = CTNumber

  override def arguments = Seq(arg)

  override def symbolTableDependencies = arg.symbolTableDependencies
}

abstract class NullSafeMathFunction(arg: Expression) extends MathFunction(arg) {

  override def apply(ctx: ExecutionContext)(implicit state: QueryState): AnyValue = {
    val value = arg(ctx)
    if (Values.NO_VALUE == value) Values.NO_VALUE else Values.doubleValue(apply(asDouble(value).doubleValue()))
  }

  def apply(value: Double): Double
}

trait NumericHelper {

  protected def asLongEntityId(a: AnyValue): LongValue = a match {
    case _ if a.isInstanceOf[FloatValue] =>
      throw new CypherTypeException("Expected entity id to be an integral value")
    case _ =>
      asLong(a)
  }

  protected def asDouble(a: AnyValue): DoubleValue = Values.doubleValue(asNumber(a).doubleValue())

  protected def asInt(a: AnyValue): IntValue = Values.intValue(asNumber(a).longValue().toInt)

  protected def asLong(a: AnyValue): LongValue = Values.longValue(asNumber(a).longValue())

  private def asNumber(a: AnyValue): NumberValue = a match {
    case null => throw new CypherTypeException("Expected a numeric value for " + toString + ", but got null")
    case n if n == Values.NO_VALUE => throw new CypherTypeException("Expected a numeric value for " + toString + ", but got null")
    case n: NumberValue => n
    case _ => throw new CypherTypeException("Expected a numeric value for " + toString + ", but got: " + a.toString)
  }
}

case class AbsFunction(argument: Expression) extends MathFunction(argument) {

  override def apply(ctx: ExecutionContext)(implicit state: QueryState): AnyValue = {
    val value = argument(ctx)
    if (Values.NO_VALUE == value) Values.NO_VALUE
    else value match {
      case f: IntegralValue => Values.longValue(Math.abs(f.longValue()))
      case d: NumberValue =>  Values.doubleValue(Math.abs(d.doubleValue()))
      case x => throw new CypherTypeException("Expected a numeric value for " + toString + ", but got: " + x.toString)
    }
  }

  override def rewrite(f: (Expression) => Expression) = f(AbsFunction(argument.rewrite(f)))
}

case class AcosFunction(argument: Expression) extends NullSafeMathFunction(argument) {

  override def apply(value: Double): Double = Math.acos(value)

  override def rewrite(f: (Expression) => Expression) = f(AcosFunction(argument.rewrite(f)))
}

case class AsinFunction(argument: Expression) extends NullSafeMathFunction(argument) {

  override def apply(value: Double): Double = Math.asin(value)

  override def rewrite(f: (Expression) => Expression) = f(AsinFunction(argument.rewrite(f)))
}

case class AtanFunction(argument: Expression) extends NullSafeMathFunction(argument) {

  override def apply(value: Double): Double = Math.atan(value)

  override def rewrite(f: (Expression) => Expression) = f(AtanFunction(argument.rewrite(f)))
}

case class Atan2Function(y: Expression, x: Expression) extends Expression with NumericHelper {

  def apply(ctx: ExecutionContext)(implicit state: QueryState): AnyValue = {
    val yValue = y(ctx)
    val xValue = x(ctx)
    if (null == yValue || null == xValue)
      null
    else
      Values.doubleValue(math.atan2(asDouble(yValue).doubleValue(), asDouble(xValue).doubleValue()))
  }

  override def arguments = Seq(x, y)

  override def rewrite(f: (Expression) => Expression) = f(Atan2Function(y.rewrite(f), x.rewrite(f)))

  override def symbolTableDependencies = x.symbolTableDependencies ++ y.symbolTableDependencies
}

case class CeilFunction(argument: Expression) extends NullSafeMathFunction(argument) {

  override def apply(value: Double) = math.ceil(value)

  override def rewrite(f: (Expression) => Expression) = f(CeilFunction(argument.rewrite(f)))
}

case class CosFunction(argument: Expression) extends NullSafeMathFunction(argument) {

  override def apply(value: Double): Double = math.cos(value)

  override def rewrite(f: (Expression) => Expression) = f(CosFunction(argument.rewrite(f)))
}

case class CotFunction(argument: Expression) extends NullSafeMathFunction(argument) {

  override def apply(value: Double): Double = 1.0 / math.tan(value)

  override def rewrite(f: (Expression) => Expression) = f(CotFunction(argument.rewrite(f)))
}

case class DegreesFunction(argument: Expression) extends NullSafeMathFunction(argument) {

  override def apply(value: Double): Double = math.toDegrees(value)

  override def rewrite(f: (Expression) => Expression) = f(DegreesFunction(argument.rewrite(f)))
}

case class EFunction() extends Expression() {

  override def apply(ctx: ExecutionContext)(implicit state: QueryState): AnyValue = Values.E

  override def arguments = Seq()

  override def symbolTableDependencies = Set[String]()

  override def rewrite(f: (Expression) => Expression) = f(EFunction())
}

case class ExpFunction(argument: Expression) extends NullSafeMathFunction(argument) {

  override def apply(value: Double): Double = math.exp(value)

  override def rewrite(f: (Expression) => Expression) = f(ExpFunction(argument.rewrite(f)))
}

case class FloorFunction(argument: Expression) extends NullSafeMathFunction(argument) {

  override def apply(value: Double) = math.floor(value)

  override def rewrite(f: (Expression) => Expression) = f(FloorFunction(argument.rewrite(f)))
}

case class LogFunction(argument: Expression) extends NullSafeMathFunction(argument) {

  override def apply(value: Double) = math.log(value)

  override def rewrite(f: (Expression) => Expression) = f(LogFunction(argument.rewrite(f)))
}

case class Log10Function(argument: Expression) extends NullSafeMathFunction(argument) {

  override def apply(value: Double) = math.log10(value)

  override def rewrite(f: (Expression) => Expression) = f(Log10Function(argument.rewrite(f)))
}

case class PiFunction() extends Expression {

  override def apply(ctx: ExecutionContext)(implicit state: QueryState): AnyValue = Values.PI

  override def arguments = Seq()

  override def symbolTableDependencies = Set()

  override def rewrite(f: (Expression) => Expression) = f(PiFunction())
}

case class RadiansFunction(argument: Expression) extends NullSafeMathFunction(argument) {

  override def apply(value: Double) = math.toRadians(value)

  override def rewrite(f: (Expression) => Expression) = f(RadiansFunction(argument.rewrite(f)))
}

case class SinFunction(argument: Expression) extends NullSafeMathFunction(argument) {

  override def apply(value: Double) = math.sin(value)

  override def rewrite(f: (Expression) => Expression) = f(SinFunction(argument.rewrite(f)))
}

case class HaversinFunction(argument: Expression) extends NullSafeMathFunction(argument) {

  override def apply(value: Double) = (1.0d - math.cos(value)) / 2

  override def rewrite(f: (Expression) => Expression) = f(HaversinFunction(argument.rewrite(f)))
}

case class TanFunction(argument: Expression) extends NullSafeMathFunction(argument) {

  override def apply(value: Double) = math.tan(value)

  override def rewrite(f: (Expression) => Expression) = f(TanFunction(argument.rewrite(f)))
}

case class RandFunction() extends Expression {

  override def apply(ctx: ExecutionContext)(implicit state: QueryState): AnyValue = Values.doubleValue(math.random)

  override def arguments = Seq()

  override def symbolTableDependencies = Set[String]()

  override def rewrite(f: (Expression) => Expression) = f(RandFunction())
}

case class RangeFunction(start: Expression, end: Expression, step: Expression) extends Expression with NumericHelper {

  override def apply(ctx: ExecutionContext)(implicit state: QueryState): AnyValue = {
    val stepVal = asLong(step(ctx))
    if (stepVal.longValue() == 0L)
      throw new InvalidArgumentException("step argument to range() cannot be zero")

    val startVal = asLong(start(ctx))
    val inclusiveEndVal = asLong(end(ctx))

    VirtualValues.range(startVal.longValue(), inclusiveEndVal.longValue(), stepVal.longValue())
  }

  override def arguments = Seq(start, end, step)

  override def rewrite(f: (Expression) => Expression) =
    f(RangeFunction(start.rewrite(f), end.rewrite(f), step.rewrite(f)))

  override def symbolTableDependencies = start.symbolTableDependencies ++
    end.symbolTableDependencies ++
    step.symbolTableDependencies
}

case class SignFunction(argument: Expression) extends MathFunction(argument) {

  override def apply(ctx: ExecutionContext)(implicit state: QueryState): AnyValue = {
    val value = argument(ctx)
    if (Values.NO_VALUE == value) Values.NO_VALUE
    else {
      Values.longValue(Math.signum(asDouble(value).doubleValue()).toLong)
    }
  }

  override def rewrite(f: (Expression) => Expression) = f(SignFunction(argument.rewrite(f)))
}

case class RoundFunction(expression: Expression) extends NullSafeMathFunction(expression) {

  override def apply(value: Double) = math.round(value)

  override def rewrite(f: (Expression) => Expression) = f(RoundFunction(expression.rewrite(f)))
}

case class SqrtFunction(argument: Expression) extends NullSafeMathFunction(argument) {

  override def apply(value: Double) = Math.sqrt(value)

  override def rewrite(f: (Expression) => Expression) = f(SqrtFunction(argument.rewrite(f)))
}
