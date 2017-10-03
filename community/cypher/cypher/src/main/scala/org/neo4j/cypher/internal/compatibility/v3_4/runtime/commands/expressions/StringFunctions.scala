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
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.commands.expressions

import org.neo4j.cypher.internal.util.v3_4.{CypherTypeException, ParameterWrongTypeException}
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.ExecutionContext
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.pipes.QueryState
import org.neo4j.cypher.internal.util.v3_4.symbols._
import org.neo4j.values._
import org.neo4j.values.storable._
import org.neo4j.values.virtual.VirtualValues

import scala.annotation.tailrec

abstract class StringFunction(arg: Expression) extends NullInNullOutExpression(arg) {

  def innerExpectedType = CTString

  override def arguments = Seq(arg)

  override def symbolTableDependencies = arg.symbolTableDependencies
}

case object asString extends (AnyValue => String) {

  override def apply(a: AnyValue): String = a match {
    case x if x == Values.NO_VALUE => null
    case x: TextValue => x.stringValue()
    case _ => throw new CypherTypeException(
      "Expected a string value for %s, but got: %s; consider converting it to a string with toString()."
        .format(toString(), a.toString))
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

  override def compute(value: AnyValue, m: ExecutionContext, state: QueryState): AnyValue =
    Values.stringValue(asString(argument(m, state)).toLowerCase)

  override def rewrite(f: (Expression) => Expression) = f(ToLowerFunction(argument.rewrite(f)))
}

case class ToUpperFunction(argument: Expression) extends StringFunction(argument) {

  override def compute(value: AnyValue, m: ExecutionContext, state: QueryState): AnyValue =
    Values.stringValue(asString(argument(m, state)).toUpperCase)

  override def rewrite(f: (Expression) => Expression) = f(ToUpperFunction(argument.rewrite(f)))
}

case class LTrimFunction(argument: Expression) extends StringFunction(argument) {

  override def compute(value: AnyValue, m: ExecutionContext, state: QueryState): AnyValue =
    Values.stringValue(asString(argument(m, state)).replaceAll("^\\s+", ""))

  override def rewrite(f: (Expression) => Expression) = f(LTrimFunction(argument.rewrite(f)))
}

case class RTrimFunction(argument: Expression) extends StringFunction(argument) {

  override def compute(value: AnyValue, m: ExecutionContext, state: QueryState): AnyValue =
    Values.stringValue(asString(argument(m, state)).replaceAll("\\s+$", ""))

  override def rewrite(f: (Expression) => Expression) = f(RTrimFunction(argument.rewrite(f)))
}

case class TrimFunction(argument: Expression) extends StringFunction(argument) {

  override def compute(value: AnyValue, m: ExecutionContext, state: QueryState): AnyValue =
    Values.stringValue(asString(argument(m, state)).trim)

  override def rewrite(f: (Expression) => Expression) = f(TrimFunction(argument.rewrite(f)))
}

case class SubstringFunction(orig: Expression, start: Expression, length: Option[Expression])
  extends NullInNullOutExpression(orig) with NumericHelper {

  override def compute(value: AnyValue, m: ExecutionContext, state: QueryState): AnyValue = {
    val origVal = asString(orig(m, state))

    def noMoreThanMax(maxLength: Int, length: Int): Int =
      if (length > maxLength) {
        maxLength
      } else {
        length
      }

    // if start goes off the end of the string, let's be nice and handle that.
    val startVal = noMoreThanMax(origVal.length, asInt(start(m, state)).value())

    // if length goes off the end of the string, let's be nice and handle that.
    val lengthVal = length match {
      case None => origVal.length - startVal
      case Some(func) => noMoreThanMax(origVal.length - startVal, asInt(func(m, state)).value())
    }

    Values.stringValue(origVal.substring(startVal, startVal + lengthVal))
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

  override def compute(value: AnyValue, m: ExecutionContext, state: QueryState): AnyValue = {
    val origVal = asString(value)
    val searchVal = asString(search(m, state))
    val replaceWithVal = asString(replaceWith(m, state))

    if (searchVal == null || replaceWithVal == null) {
      Values.NO_VALUE
    } else {
      Values.stringValue(origVal.replace(searchVal, replaceWithVal))
    }
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

  override def compute(value: AnyValue, m: ExecutionContext, state: QueryState): AnyValue = {
    val origVal = asString(orig(m, state))
    val separatorVal = asString(separator(m, state))

    if (origVal == null || separatorVal == null) {
      Values.NO_VALUE
    } else {
      if (separatorVal.length > 0) {
        VirtualValues.fromArray(Values.stringArray(split(Vector.empty, origVal, 0, separatorVal).toArray:_*))
      } else if (origVal.isEmpty) {
        VirtualValues.list(Values.EMPTY_STRING)
      } else {
        VirtualValues.fromArray(Values.stringArray(origVal.sliding(1).toArray:_*))
      }
    }
  }

  @tailrec
  private def split(parts: Vector[String], string: String, from: Int, separator: String): Vector[String] = {
    val index = string.indexOf(separator, from)
    if (index < 0)
      parts :+ string.substring(from)
    else
      split(parts :+ string.substring(from, index), string, index + separator.length, separator)
  }

  override def arguments = Seq(orig, separator)

  override def rewrite(f: (Expression) => Expression) = f(SplitFunction(orig.rewrite(f), separator.rewrite(f)))

  override def symbolTableDependencies = orig.symbolTableDependencies ++ separator.symbolTableDependencies
}

case class LeftFunction(orig: Expression, length: Expression)
  extends NullInNullOutExpression(orig) with NumericHelper {

  override def compute(value: AnyValue, m: ExecutionContext, state: QueryState): AnyValue = {
    val origVal = asString(orig(m, state))
    val startVal = 0
    val expectedLength = asInt(length(m, state)).value()
    // if length goes off the end of the string, let's be nice and handle that.
    val lengthVal = if (origVal.length < expectedLength + startVal)
      origVal.length
    else
      expectedLength
    Values.stringValue(origVal.substring(startVal, startVal + lengthVal))
  }

  override def arguments = Seq(orig, length)

  override def rewrite(f: (Expression) => Expression) = f(LeftFunction(orig.rewrite(f), length.rewrite(f)))

  override def symbolTableDependencies = orig.symbolTableDependencies ++
    length.symbolTableDependencies
}

case class RightFunction(orig: Expression, length: Expression)
  extends NullInNullOutExpression(orig) with NumericHelper {

  override def compute(value: AnyValue, m: ExecutionContext, state: QueryState): AnyValue = {
    val origVal = asString(orig(m, state))
    // if length goes off the end of the string, let's be nice and handle that.
    val lengthVal = if (origVal.length < asInt(length(m, state)).value()) origVal.length
    else asInt(length(m, state)).value()
    val startVal = origVal.length - lengthVal
    Values.stringValue(origVal.substring(startVal, startVal + lengthVal))
  }

  override def arguments = Seq(orig, length)

  override def rewrite(f: (Expression) => Expression) = f(RightFunction(orig.rewrite(f), length.rewrite(f)))

  override def symbolTableDependencies = orig.symbolTableDependencies ++
    length.symbolTableDependencies
}
