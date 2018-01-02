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
package org.neo4j.cypher.internal.runtime.interpreted.commands.expressions

import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.util.v3_4.symbols._
import org.neo4j.cypher.internal.util.v3_4.{CypherTypeException, ParameterWrongTypeException}
import org.neo4j.values._
import org.neo4j.values.storable.Values.NO_VALUE
import org.neo4j.values.storable._
import org.neo4j.values.virtual.VirtualValues

abstract class StringFunction(arg: Expression) extends NullInNullOutExpression(arg) {

  def innerExpectedType = CTString

  override def arguments = Seq(arg)

  override def symbolTableDependencies = arg.symbolTableDependencies
}

object StringFunction {

  def notAString(a: Any) = throw new CypherTypeException(
    "Expected a string value for %s, but got: %s; consider converting it to a string with toString()."
      .format(toString, a.toString))
}

case object asString extends (AnyValue => String) {

  override def apply(a: AnyValue): String = a match {
    case NO_VALUE => null
    case x: TextValue => x.stringValue()
    case _ => StringFunction.notAString(a)
  }
}

case class ToStringFunction(argument: Expression) extends StringFunction(argument) {

  override def compute(value: AnyValue, m: ExecutionContext, state: QueryState): AnyValue = argument(m, state) match {
    case v: IntegralValue => Values.stringValue(v.longValue().toString)
    case v: FloatingPointValue => Values.stringValue(v.doubleValue().toString)
    case v: TextValue => v
    case v: BooleanValue => Values.stringValue(v.booleanValue().toString)
    case v =>
      throw new ParameterWrongTypeException("Expected a String, Number or Boolean, got: " + v.toString)
  }

  override def rewrite(f: (Expression) => Expression): Expression = f(ToStringFunction(argument.rewrite(f)))
}

case class ToLowerFunction(argument: Expression) extends StringFunction(argument) {

  override def compute(value: AnyValue, m: ExecutionContext, state: QueryState): AnyValue = value match {
    case t: TextValue => t.toLower
    case _ => StringFunction.notAString(value)
  }

  override def rewrite(f: (Expression) => Expression) = f(ToLowerFunction(argument.rewrite(f)))
}

case class ToUpperFunction(argument: Expression) extends StringFunction(argument) {

  override def compute(value: AnyValue, m: ExecutionContext, state: QueryState): AnyValue =value match {
    case t: TextValue => t.toUpper
    case _ => StringFunction.notAString(value)
  }

  override def rewrite(f: (Expression) => Expression) = f(ToUpperFunction(argument.rewrite(f)))
}

case class LTrimFunction(argument: Expression) extends StringFunction(argument) {

  override def compute(value: AnyValue, m: ExecutionContext, state: QueryState): AnyValue = value match {
    case t: TextValue => t.ltrim()
    case _ => StringFunction.notAString(value)
  }

  override def rewrite(f: (Expression) => Expression) = f(LTrimFunction(argument.rewrite(f)))
}

case class RTrimFunction(argument: Expression) extends StringFunction(argument) {

  override def compute(value: AnyValue, m: ExecutionContext, state: QueryState): AnyValue = value match {
    case t: TextValue => t.rtrim()
    case _ => StringFunction.notAString(value)
  }

  override def rewrite(f: (Expression) => Expression) = f(RTrimFunction(argument.rewrite(f)))
}

case class TrimFunction(argument: Expression) extends StringFunction(argument) {

  override def compute(value: AnyValue, m: ExecutionContext, state: QueryState): AnyValue = value match {
    case t: TextValue => t.trim()
    case _ => StringFunction.notAString(value)
  }

  override def rewrite(f: (Expression) => Expression) = f(TrimFunction(argument.rewrite(f)))
}

case class SubstringFunction(orig: Expression, start: Expression, length: Option[Expression])
  extends NullInNullOutExpression(orig) with NumericHelper {

  override def compute(value: AnyValue, m: ExecutionContext, state: QueryState): AnyValue = value match {
      case text: TextValue =>
        val startVal = asInt(start(m, state)).value()
        length match {
          case None => text.substring(startVal)
          case Some(func) => text.substring(startVal, asInt(func(m, state)).value())
        }
      case _ => StringFunction.notAString(value)
    }

  override def arguments = Seq(orig, start) ++ length

  override def rewrite(f: (Expression) => Expression) = f(
    SubstringFunction(orig.rewrite(f), start.rewrite(f), length.map(_.rewrite(f))))

  override def symbolTableDependencies = {
    val a = orig.symbolTableDependencies ++
      start.symbolTableDependencies

    val b = length.toIndexedSeq.flatMap(_.symbolTableDependencies.toIndexedSeq).toSet

    a ++ b
  }
}

case class ReplaceFunction(orig: Expression, search: Expression, replaceWith: Expression)
  extends NullInNullOutExpression(orig) {

  override def compute(value: AnyValue, m: ExecutionContext, state: QueryState): AnyValue = value match {
    case t: TextValue =>
      val searchVal = asString(search(m, state))
      val replaceWithVal = asString(replaceWith(m, state))
      if (searchVal == null || replaceWithVal == null) NO_VALUE else t.replace(searchVal, replaceWithVal)
    case _ => StringFunction.notAString(value)
  }

  override def arguments = Seq(orig, search, replaceWith)

  override def rewrite(f: (Expression) => Expression) = f(
    ReplaceFunction(orig.rewrite(f), search.rewrite(f), replaceWith.rewrite(f)))

  override def symbolTableDependencies = orig.symbolTableDependencies ++
    search.symbolTableDependencies ++
    replaceWith.symbolTableDependencies
}

case class SplitFunction(orig: Expression, separator: Expression)
  extends NullInNullOutExpression(orig) {

  override def compute(value: AnyValue, m: ExecutionContext, state: QueryState): AnyValue = value match {
    case t: TextValue if t.length() == 0  => VirtualValues.list(Values.EMPTY_STRING)
    case t: TextValue =>
      val separatorVal = asString(separator(m, state))
      if (separatorVal == null) NO_VALUE else t.split(separatorVal)
    case _ => StringFunction.notAString(value)
  }

  override def arguments = Seq(orig, separator)

  override def rewrite(f: (Expression) => Expression) = f(SplitFunction(orig.rewrite(f), separator.rewrite(f)))

  override def symbolTableDependencies = orig.symbolTableDependencies ++ separator.symbolTableDependencies
}

case class LeftFunction(orig: Expression, length: Expression)
  extends NullInNullOutExpression(orig) with NumericHelper {

  override def compute(value: AnyValue, m: ExecutionContext, state: QueryState): AnyValue = value match {
      case origVal: TextValue => origVal.substring(0, asInt(length(m, state)).value())
      case _ => StringFunction.notAString(value)
  }

  override def arguments = Seq(orig, length)

  override def rewrite(f: (Expression) => Expression) = f(LeftFunction(orig.rewrite(f), length.rewrite(f)))

  override def symbolTableDependencies = orig.symbolTableDependencies ++
    length.symbolTableDependencies
}

case class RightFunction(orig: Expression, length: Expression)
  extends NullInNullOutExpression(orig) with NumericHelper {

  override def compute(value: AnyValue, m: ExecutionContext, state: QueryState): AnyValue = value match {
      case origVal: TextValue =>
        // if length goes off the end of the string, let's be nice and handle that.
        val lengthVal = asInt(length(m, state)).value()
        if (lengthVal < 0) throw new IndexOutOfBoundsException(s"negative length")
        val startVal = origVal.length - lengthVal
        origVal.substring(Math.max(0,startVal))
      case _ => StringFunction.notAString(value)
    }

  override def arguments = Seq(orig, length)

  override def rewrite(f: (Expression) => Expression) = f(RightFunction(orig.rewrite(f), length.rewrite(f)))

  override def symbolTableDependencies = orig.symbolTableDependencies ++
    length.symbolTableDependencies
}
