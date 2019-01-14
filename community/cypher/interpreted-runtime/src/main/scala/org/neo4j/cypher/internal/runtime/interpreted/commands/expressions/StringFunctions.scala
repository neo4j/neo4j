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
package org.neo4j.cypher.internal.runtime.interpreted.commands.expressions

import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.operations.CypherFunctions
import org.neo4j.values._
import org.neo4j.values.storable.Values.NO_VALUE
import org.neo4j.values.storable._
import org.neo4j.cypher.internal.v3_5.util.CypherTypeException
import org.neo4j.cypher.internal.v3_5.util.symbols._

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

  override def compute(value: AnyValue, m: ExecutionContext, state: QueryState): AnyValue =
    CypherFunctions.toString(argument(m, state))

  override def rewrite(f: (Expression) => Expression): Expression = f(ToStringFunction(argument.rewrite(f)))
}

case class ToLowerFunction(argument: Expression) extends StringFunction(argument) {

  override def compute(value: AnyValue, m: ExecutionContext, state: QueryState): AnyValue = CypherFunctions.toLower(value)

  override def rewrite(f: (Expression) => Expression) = f(ToLowerFunction(argument.rewrite(f)))
}

case class ToUpperFunction(argument: Expression) extends StringFunction(argument) {

  override def compute(value: AnyValue, m: ExecutionContext, state: QueryState): AnyValue = CypherFunctions.toUpper(value)

  override def rewrite(f: (Expression) => Expression) = f(ToUpperFunction(argument.rewrite(f)))
}

case class LTrimFunction(argument: Expression) extends StringFunction(argument) {

  override def compute(value: AnyValue, m: ExecutionContext, state: QueryState): AnyValue =
    CypherFunctions.ltrim(value)

  override def rewrite(f: (Expression) => Expression) = f(LTrimFunction(argument.rewrite(f)))
}

case class RTrimFunction(argument: Expression) extends StringFunction(argument) {

  override def compute(value: AnyValue, m: ExecutionContext, state: QueryState): AnyValue =
    CypherFunctions.rtrim(value)

  override def rewrite(f: (Expression) => Expression) = f(RTrimFunction(argument.rewrite(f)))
}

case class TrimFunction(argument: Expression) extends StringFunction(argument) {

  override def compute(value: AnyValue, m: ExecutionContext, state: QueryState): AnyValue =
    CypherFunctions.trim(value)

  override def rewrite(f: (Expression) => Expression) = f(TrimFunction(argument.rewrite(f)))
}

case class SubstringFunction(orig: Expression, start: Expression, length: Option[Expression])
  extends NullInNullOutExpression(orig) {

  override def compute(value: AnyValue, m: ExecutionContext, state: QueryState): AnyValue = length match {
    case None => CypherFunctions.substring(value, start(m, state))
    case Some(func) => CypherFunctions.substring(value, start(m, state), func(m, state))
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
      val searchVal = search(m, state)
      val replaceWithVal = replaceWith(m, state)
      if (searchVal == NO_VALUE || replaceWithVal == NO_VALUE) NO_VALUE
      else CypherFunctions.replace(value, searchVal, replaceWithVal)
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
    val sep = separator(m, state)
    if (sep == NO_VALUE) NO_VALUE else CypherFunctions.split(value, sep)
  }

  override def arguments = Seq(orig, separator)

  override def rewrite(f: (Expression) => Expression) = f(SplitFunction(orig.rewrite(f), separator.rewrite(f)))

  override def symbolTableDependencies = orig.symbolTableDependencies ++ separator.symbolTableDependencies
}

case class LeftFunction(orig: Expression, length: Expression)
  extends NullInNullOutExpression(orig) with NumericHelper {

  override def compute(value: AnyValue, m: ExecutionContext, state: QueryState): AnyValue =
    CypherFunctions.left(value, length(m, state))

  override def arguments = Seq(orig, length)

  override def rewrite(f: (Expression) => Expression) = f(LeftFunction(orig.rewrite(f), length.rewrite(f)))

  override def symbolTableDependencies = orig.symbolTableDependencies ++
    length.symbolTableDependencies
}

case class RightFunction(orig: Expression, length: Expression)
  extends NullInNullOutExpression(orig) with NumericHelper {

  override def compute(value: AnyValue, m: ExecutionContext, state: QueryState): AnyValue =
    CypherFunctions.right(value, length(m, state))

  override def arguments = Seq(orig, length)

  override def rewrite(f: (Expression) => Expression) = f(RightFunction(orig.rewrite(f), length.rewrite(f)))

  override def symbolTableDependencies = orig.symbolTableDependencies ++
    length.symbolTableDependencies
}
