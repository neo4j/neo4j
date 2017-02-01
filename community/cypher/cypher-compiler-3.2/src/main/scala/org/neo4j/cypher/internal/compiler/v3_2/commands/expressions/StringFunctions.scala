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
package org.neo4j.cypher.internal.compiler.v3_2.commands.expressions

import org.neo4j.cypher.internal.compiler.v3_2._
import org.neo4j.cypher.internal.compiler.v3_2.pipes.QueryState
import org.neo4j.cypher.internal.frontend.v3_2.symbols._
import org.neo4j.cypher.internal.frontend.v3_2.{CypherTypeException, ParameterWrongTypeException}

import scala.annotation.tailrec

abstract class StringFunction(arg: Expression) extends NullInNullOutExpression(arg) {
  def innerExpectedType = CTString
}

case object asString extends (Any => String) {

  override def apply(a: Any): String = a match {
    case null => null
    case x: String => x
    case _ => throw new CypherTypeException(
      "Expected a string value for %s, but got: %s; consider converting it to a string with toString()."
        .format(toString(), a.toString))
  }
}

case class ToStringFunction(argument: Expression) extends StringFunction(argument) {

  override def compute(value: Any, m: ExecutionContext)(implicit state: QueryState): Any = argument(m) match {
    case v: Number => v.toString
    case v: String => v
    case v: Boolean => v.toString
    case v =>
      throw new ParameterWrongTypeException("Expected a String, Number or Boolean, got: " + v.toString)
  }
}

case class ToLowerFunction(argument: Expression) extends StringFunction(argument) {
  override def compute(value: Any, m: ExecutionContext)(implicit state: QueryState): Any = asString(argument(m)).toLowerCase
}

case class ToUpperFunction(argument: Expression) extends StringFunction(argument) {
  override def compute(value: Any, m: ExecutionContext)(implicit state: QueryState): Any = asString(argument(m)).toUpperCase
}

case class LTrimFunction(argument: Expression) extends StringFunction(argument) {
  override def compute(value: Any, m: ExecutionContext)(implicit state: QueryState): Any = asString(argument(m)).replaceAll("^\\s+", "")
}

case class RTrimFunction(argument: Expression) extends StringFunction(argument) {
  override def compute(value: Any, m: ExecutionContext)(implicit state: QueryState): Any = asString(argument(m)).replaceAll("\\s+$", "")
}

case class TrimFunction(argument: Expression) extends StringFunction(argument) {
  override def compute(value: Any, m: ExecutionContext)(implicit state: QueryState): Any = asString(argument(m)).trim
}

case class SubstringFunction(orig: Expression, start: Expression, length: Option[Expression])
  extends NullInNullOutExpression(orig) with NumericHelper {
  override def compute(value: Any, m: ExecutionContext)(implicit state: QueryState): Any = {
    val origVal = asString(orig(m))

    def noMoreThanMax(maxLength: Int, length: Int): Int =
      if (length > maxLength) {
        maxLength
      } else {
        length
      }

    // if start goes off the end of the string, let's be nice and handle that.
    val startVal = noMoreThanMax(origVal.length, asInt(start(m)))

    // if length goes off the end of the string, let's be nice and handle that.
    val lengthVal = length match {
      case None       => origVal.length - startVal
      case Some(func) => noMoreThanMax(origVal.length - startVal, asInt(func(m)))
    }

    origVal.substring(startVal, startVal + lengthVal)
  }
}

case class ReplaceFunction(orig: Expression, search: Expression, replaceWith: Expression)
  extends NullInNullOutExpression(orig) {
  override def compute(value: Any, m: ExecutionContext)(implicit state: QueryState): Any = {
    val origVal = asString(value)
    val searchVal = asString(search(m))
    val replaceWithVal = asString(replaceWith(m))

    if (searchVal == null || replaceWithVal == null) {
      null
    } else {
      origVal.replace(searchVal, replaceWithVal)
    }
  }
}
case class SplitFunction(orig: Expression, separator: Expression)
  extends NullInNullOutExpression(orig) {
  override def compute(value: Any, m: ExecutionContext)(implicit state: QueryState): Any = {
    val origVal = asString(orig(m))
    val separatorVal = asString(separator(m))

    if (origVal == null || separatorVal == null) {
      null
    } else {
      if (separatorVal.length > 0) {
        split(Vector.empty, origVal, 0, separatorVal)
      } else if (origVal.isEmpty) {
        Vector("")
      } else {
        origVal.sliding(1).toList
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
}

case class LeftFunction(orig: Expression, length: Expression)
  extends NullInNullOutExpression(orig) with NumericHelper {
  override def compute(value: Any, m: ExecutionContext)(implicit state: QueryState): Any = {
    val origVal = asString(orig(m))
    val startVal = asInt(0)
    // if length goes off the end of the string, let's be nice and handle that.
    val lengthVal = if (origVal.length < asInt(length(m)) + startVal) origVal.length
    else asInt(length(m))
    origVal.substring(startVal, startVal + lengthVal)
  }
}

case class RightFunction(orig: Expression, length: Expression)
  extends NullInNullOutExpression(orig) with NumericHelper {
  override def compute(value: Any, m: ExecutionContext)(implicit state: QueryState): Any = {
    val origVal = asString(orig(m))
    // if length goes off the end of the string, let's be nice and handle that.
    val lengthVal = if (origVal.length < asInt(length(m))) origVal.length
    else asInt(length(m))
    val startVal = origVal.length - lengthVal
    origVal.substring(startVal, startVal + lengthVal)
  }
}
