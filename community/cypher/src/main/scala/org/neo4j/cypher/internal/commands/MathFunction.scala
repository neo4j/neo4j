/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.cypher.internal.commands

import org.neo4j.cypher.internal.symbols.{AnyType, Identifier, NumberType}
import java.lang.Math
import org.neo4j.cypher.CypherTypeException
import collection.Map

abstract class MathFunction(arg: Expression) extends Expression {
  protected def asDouble(a: Any) = try {
    a.asInstanceOf[Number].doubleValue()
  }
  catch {
    case x: ClassCastException => throw new CypherTypeException("Expected a numeric value for " + toString() + ", but got: " + a.toString)
  }

  protected def asInt(a: Any) = asDouble(a).round

  def innerExpectedType = NumberType()

  val identifier = Identifier(toString(), NumberType())

  def declareDependencies(extectedType: AnyType): Seq[Identifier] = arg.dependencies(AnyType())

  protected def name: String

  private def argumentsString: String = arg.identifier.name

  override def toString() = name + "(" + argumentsString + ")"

  def filter(f: (Expression) => Boolean) = if(f(this))
    Seq(this) ++ arg.filter(f)
  else
    arg.filter(f)
}

case class AbsFunction(argument: Expression) extends MathFunction(argument) {
  def compute(m: Map[String, Any]): Any = Math.abs(asDouble(argument(m)))

  protected def name = "abs"

  def rewrite(f: (Expression) => Expression) = f(AbsFunction(argument.rewrite(f)))
}

case class SignFunction(argument: Expression) extends MathFunction(argument) {
  def compute(m: Map[String, Any]): Any = Math.signum(asDouble(argument(m)))

  protected def name = "sign"

  def rewrite(f: (Expression) => Expression) = f(SignFunction(argument.rewrite(f)))
}

case class RoundFunction(expression: Expression) extends MathFunction(expression) {
  def compute(m: Map[String, Any]): Any = math.round(asDouble(expression(m)))

  protected def name = "round"

  def rewrite(f: (Expression) => Expression) = f(RoundFunction(expression.rewrite(f)))
}

case class SqrtFunction(argument: Expression) extends MathFunction(argument) {
  def compute(m: Map[String, Any]): Any = Math.sqrt(asDouble(argument(m))).toInt

  protected def name = "sqrt"

  def rewrite(f: (Expression) => Expression) = f(SqrtFunction(argument.rewrite(f)))
}