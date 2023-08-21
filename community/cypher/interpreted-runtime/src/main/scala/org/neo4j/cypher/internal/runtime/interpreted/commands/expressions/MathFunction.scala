/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.interpreted.commands.expressions

import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.AstNode
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.util.symbols.CTNumber
import org.neo4j.cypher.internal.util.symbols.NumberType
import org.neo4j.cypher.operations.CypherFunctions
import org.neo4j.exceptions.CypherTypeException
import org.neo4j.kernel.api.StatementConstants
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.DoubleValue
import org.neo4j.values.storable.FloatingPointValue
import org.neo4j.values.storable.IntegralValue
import org.neo4j.values.storable.LongValue
import org.neo4j.values.storable.NumberValue
import org.neo4j.values.storable.NumberValues
import org.neo4j.values.storable.Values
import org.neo4j.values.storable.Values.NO_VALUE

abstract class MathFunction(arg: Expression) extends Expression {

  def innerExpectedType: NumberType = CTNumber

  override def arguments: Seq[Expression] = Seq(arg)

}

abstract class NullSafeMathFunction(arg: Expression) extends MathFunction(arg) {

  override def apply(row: ReadableRow, state: QueryState): AnyValue = {
    val value = arg(row, state)
    if (NO_VALUE eq value) NO_VALUE else Values.doubleValue(apply(NumericHelper.asDouble(value).doubleValue()))
  }

  def apply(value: Double): Double
}

// We need this to be able to call the static functions from compiled code.
class NumericHelper

object NumericHelper {

  def asLongEntityId(a: AnyValue): Option[Long] = a match {
    case i: IntegralValue => Some(i.longValue())
    case f: FloatingPointValue =>
      if (NumberValues.numbersEqual(f.doubleValue(), f.longValue())) Some(f.longValue()) else None
    case _ => None
  }

  def asLongEntityIdPrimitive(a: AnyValue): Long = a match {
    case d: IntegralValue                                                                   => d.longValue()
    case f: FloatingPointValue if NumberValues.numbersEqual(f.doubleValue(), f.longValue()) => f.longValue()
    case _ => StatementConstants.NO_SUCH_ENTITY
  }

  def asDouble(a: AnyValue): DoubleValue = Values.doubleValue(asNumber(a).doubleValue())

  def asPrimitiveInt(a: AnyValue): Int = asNumber(a).longValue().toInt

  def asLong(a: AnyValue): LongValue = Values.longValue(asPrimitiveLong(a))

  def asPrimitiveLong(a: AnyValue): Long = asNumber(a).longValue()

  def asNumber(a: AnyValue): NumberValue = a match {
    case null => throw new CypherTypeException("Expected a numeric value for " + toString + ", but got null")
    case x if x eq NO_VALUE =>
      throw new CypherTypeException("Expected a numeric value for " + toString + ", but got null")
    case n: NumberValue => n
    case _ => throw new CypherTypeException("Expected a numeric value for " + toString + ", but got: " + a.toString)
  }

  def evaluateStaticallyKnownNumber(exp: Expression, state: QueryState): NumberValue = {
    asNumber(exp(CypherRow.empty, state))
  }
}

case class AbsFunction(argument: Expression) extends MathFunction(argument) {

  override def apply(row: ReadableRow, state: QueryState): AnyValue = {
    CypherFunctions.abs(argument(row, state))
  }

  override def rewrite(f: Expression => Expression): Expression = f(AbsFunction(argument.rewrite(f)))

  override def children: Seq[AstNode[_]] = Seq(argument)
}

case class AcosFunction(argument: Expression) extends MathFunction(argument) {

  override def apply(row: ReadableRow, state: QueryState): AnyValue = CypherFunctions.acos(argument(row, state))

  override def rewrite(f: Expression => Expression): Expression = f(AcosFunction(argument.rewrite(f)))

  override def children: Seq[AstNode[_]] = Seq(argument)
}

case class AsinFunction(argument: Expression) extends MathFunction(argument) {

  override def apply(row: ReadableRow, state: QueryState): AnyValue = CypherFunctions.asin(argument(row, state))

  override def rewrite(f: Expression => Expression): Expression = f(AsinFunction(argument.rewrite(f)))

  override def children: Seq[AstNode[_]] = Seq(argument)
}

case class AtanFunction(argument: Expression) extends MathFunction(argument) {

  override def apply(row: ReadableRow, state: QueryState): AnyValue =
    CypherFunctions.atan(argument(row, state))

  override def rewrite(f: Expression => Expression): Expression = f(AtanFunction(argument.rewrite(f)))

  override def children: Seq[AstNode[_]] = Seq(argument)
}

case class Atan2Function(y: Expression, x: Expression) extends Expression {

  def apply(row: ReadableRow, state: QueryState): AnyValue = {
    CypherFunctions.atan2(y(row, state), x(row, state))
  }

  override def arguments: Seq[Expression] = Seq(x, y)

  override def children: Seq[AstNode[_]] = Seq(x, y)

  override def rewrite(f: Expression => Expression): Expression = f(Atan2Function(y.rewrite(f), x.rewrite(f)))
}

case class CeilFunction(argument: Expression) extends MathFunction(argument) {

  override def apply(row: ReadableRow, state: QueryState): AnyValue =
    CypherFunctions.ceil(argument(row, state))

  override def rewrite(f: Expression => Expression): Expression = f(CeilFunction(argument.rewrite(f)))

  override def children: Seq[AstNode[_]] = Seq(argument)
}

case class CosFunction(argument: Expression) extends MathFunction(argument) {

  override def apply(row: ReadableRow, state: QueryState): AnyValue =
    CypherFunctions.cos(argument(row, state))

  override def rewrite(f: Expression => Expression): Expression = f(CosFunction(argument.rewrite(f)))

  override def children: Seq[AstNode[_]] = Seq(argument)
}

case class CotFunction(argument: Expression) extends MathFunction(argument) {

  override def apply(row: ReadableRow, state: QueryState): AnyValue =
    CypherFunctions.cot(argument(row, state))

  override def rewrite(f: Expression => Expression): Expression = f(CotFunction(argument.rewrite(f)))

  override def children: Seq[AstNode[_]] = Seq(argument)
}

case class DegreesFunction(argument: Expression) extends MathFunction(argument) {

  override def apply(row: ReadableRow, state: QueryState): AnyValue = CypherFunctions.toDegrees(argument(row, state))

  override def rewrite(f: Expression => Expression): Expression = f(DegreesFunction(argument.rewrite(f)))

  override def children: Seq[AstNode[_]] = Seq(argument)
}

case class EFunction() extends Expression() {

  override def apply(row: ReadableRow, state: QueryState): AnyValue = Values.E

  override def arguments: Seq[Expression] = Seq.empty

  override def children: Seq[AstNode[_]] = Seq.empty

  override def rewrite(f: Expression => Expression): Expression = f(EFunction())
}

case class ExpFunction(argument: Expression) extends MathFunction(argument) {

  override def apply(row: ReadableRow, state: QueryState): AnyValue = CypherFunctions.exp(argument(row, state))

  override def rewrite(f: Expression => Expression): Expression = f(ExpFunction(argument.rewrite(f)))

  override def children: Seq[AstNode[_]] = Seq(argument)
}

case class FloorFunction(argument: Expression) extends MathFunction(argument) {

  override def apply(row: ReadableRow, state: QueryState): AnyValue = CypherFunctions.floor(argument(row, state))

  override def rewrite(f: Expression => Expression): Expression = f(FloorFunction(argument.rewrite(f)))

  override def children: Seq[AstNode[_]] = Seq(argument)
}

case class IsNaNFunction(argument: Expression) extends MathFunction(argument) {

  override def apply(row: ReadableRow, state: QueryState): AnyValue = CypherFunctions.isNaN(argument(row, state))

  override def rewrite(f: Expression => Expression): Expression = f(IsNaNFunction(argument.rewrite(f)))

  override def children: Seq[AstNode[_]] = Seq(argument)
}

case class LogFunction(argument: Expression) extends MathFunction(argument) {

  override def apply(row: ReadableRow, state: QueryState): AnyValue = CypherFunctions.log(argument(row, state))

  override def rewrite(f: Expression => Expression): Expression = f(LogFunction(argument.rewrite(f)))

  override def children: Seq[AstNode[_]] = Seq(argument)
}

case class Log10Function(argument: Expression) extends MathFunction(argument) {

  override def apply(row: ReadableRow, state: QueryState): AnyValue =
    CypherFunctions.log10(argument(row, state))

  override def rewrite(f: Expression => Expression): Expression = f(Log10Function(argument.rewrite(f)))

  override def children: Seq[AstNode[_]] = Seq(argument)
}

case class PiFunction() extends Expression {

  override def apply(row: ReadableRow, state: QueryState): AnyValue = Values.PI

  override def arguments: Seq[Expression] = Seq.empty

  override def children: Seq[AstNode[_]] = Seq.empty

  override def rewrite(f: Expression => Expression): Expression = f(PiFunction())
}

case class RadiansFunction(argument: Expression) extends MathFunction(argument) {

  override def apply(row: ReadableRow, state: QueryState): AnyValue =
    CypherFunctions.toRadians(argument(row, state))

  override def rewrite(f: Expression => Expression): Expression = f(RadiansFunction(argument.rewrite(f)))

  override def children: Seq[AstNode[_]] = Seq(argument)
}

case class SinFunction(argument: Expression) extends MathFunction(argument) {

  override def apply(row: ReadableRow, state: QueryState): AnyValue =
    CypherFunctions.sin(argument(row, state))

  override def rewrite(f: Expression => Expression): Expression = f(SinFunction(argument.rewrite(f)))

  override def children: Seq[AstNode[_]] = Seq(argument)
}

case class HaversinFunction(argument: Expression) extends MathFunction(argument) {

  override def apply(row: ReadableRow, state: QueryState): AnyValue =
    CypherFunctions.haversin(argument(row, state))

  override def rewrite(f: Expression => Expression): Expression = f(HaversinFunction(argument.rewrite(f)))

  override def children: Seq[AstNode[_]] = Seq(argument)
}

case class TanFunction(argument: Expression) extends MathFunction(argument) {

  override def apply(row: ReadableRow, state: QueryState): AnyValue =
    CypherFunctions.tan(argument(row, state))

  override def rewrite(f: Expression => Expression): Expression = f(TanFunction(argument.rewrite(f)))

  override def children: Seq[AstNode[_]] = Seq(argument)
}

case class RandFunction() extends Expression {

  override def apply(row: ReadableRow, state: QueryState): AnyValue = CypherFunctions.rand()

  override def arguments: Seq[Expression] = Seq.empty

  override def children: Seq[AstNode[_]] = Seq.empty

  override def rewrite(f: Expression => Expression): Expression = f(RandFunction())
}

case class RangeFunction(start: Expression, end: Expression, step: Expression) extends Expression {

  override def apply(row: ReadableRow, state: QueryState): AnyValue =
    CypherFunctions.range(start(row, state), end(row, state), step(row, state))

  override def arguments: Seq[Expression] = Seq(start, end, step)

  override def children: Seq[AstNode[_]] = Seq(start, end, step)

  override def rewrite(f: Expression => Expression): Expression =
    f(RangeFunction(start.rewrite(f), end.rewrite(f), step.rewrite(f)))

}

case class SignFunction(argument: Expression) extends MathFunction(argument) {

  override def apply(row: ReadableRow, state: QueryState): AnyValue = {
    val value = argument(row, state)
    if (NO_VALUE eq value) NO_VALUE
    else {
      Values.longValue(Math.signum(NumericHelper.asDouble(value).doubleValue()).toLong)
    }
  }

  override def rewrite(f: Expression => Expression): Expression = f(SignFunction(argument.rewrite(f)))

  override def children: Seq[AstNode[_]] = Seq(argument)
}

case class RoundFunction(argument: Expression, precision: Expression, mode: Expression, explicitMode: Expression)
    extends MathFunction(argument) {

  override def apply(row: ReadableRow, state: QueryState): AnyValue =
    CypherFunctions.round(argument(row, state), precision(row, state), mode(row, state), explicitMode(row, state))

  override def rewrite(f: Expression => Expression): Expression =
    f(RoundFunction(argument.rewrite(f), precision.rewrite(f), mode.rewrite(f), explicitMode.rewrite(f)))

  override def children: Seq[AstNode[_]] = Seq(argument)
}

case class SqrtFunction(argument: Expression) extends MathFunction(argument) {

  override def apply(row: ReadableRow, state: QueryState): AnyValue = CypherFunctions.sqrt(argument(row, state))

  override def rewrite(f: Expression => Expression): Expression = f(SqrtFunction(argument.rewrite(f)))

  override def children: Seq[AstNode[_]] = Seq(argument)
}
