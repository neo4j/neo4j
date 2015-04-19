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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans

import org.neo4j.cypher.internal.compiler.v2_3.ast._
import org.neo4j.cypher.internal.compiler.v2_3.ast.convert.commands.ExpressionConverters._
import org.neo4j.cypher.internal.compiler.v2_3.commands.{ManyQueryExpression, QueryExpression, SingleQueryExpression}
import org.neo4j.cypher.internal.compiler.v2_3.functions
import org.neo4j.cypher.internal.compiler.v2_3.helpers.{Many, One, Zero, ZeroOneOrMany}
import org.neo4j.cypher.internal.compiler.v2_3.pipes.{ManySeekArgs, SingleSeekArg, SeekArgs}

object Seekable {
  def unapply(v: Any) = v match {
    case In(lhs, rhs) => Some(lhs -> ManySeekableArgs(rhs))
    case Equals(lhs, rhs) => Some(lhs -> SingleSeekableArg(rhs))
    case _ => None
  }
}

object IdSeekable {
  def unapply(v: Any) = v match {
    case Seekable(func@FunctionInvocation(_, _, IndexedSeq(ident: Identifier)), rhs)
      if func.function == Some(functions.Id) && !rhs.dependencies(ident) =>
      Some(IdSeekable(func, ident, rhs) -> rhs)
    case _ =>
      None
  }
}

object PropertySeekable {
  def unapply(v: Any) = v match {
    case Seekable(prop@Property(ident: Identifier, propertyKey), rhs)
      if !rhs.dependencies(ident) =>
      Some(PropertySeekable(prop, ident, rhs) -> rhs)
    case _ =>
      None
  }
}

object PropertyScannable {
  def unapply(v: Any) = v match {
    case func@FunctionInvocation(_, _, IndexedSeq(property@Property(ident: Identifier, _)))
      if func.function == Some(functions.Has) =>
      Some(PropertyScannable(func, ident, property))
    case _ =>
      None
  }
}

sealed trait Sargable[T <: Expression] {
  def expr: T
  def ident: Identifier

  def name = ident.name
}

sealed trait Seekable[T <: Expression] extends Sargable[T] {
  def args: SeekableArgs
}

case class IdSeekable(expr: FunctionInvocation, ident: Identifier, arg: SeekableArgs)
  extends Sargable[FunctionInvocation]

case class PropertySeekable(expr: Property, ident: Identifier, args: SeekableArgs)
  extends Sargable[Property] {

  def propertyKey = expr.propertyKey
}

sealed trait Scannable[T <: Expression] extends Sargable[T]

case class PropertyScannable(expr: FunctionInvocation, ident: Identifier, property: Property)
  extends Scannable[FunctionInvocation] {

  def propertyKey = property.propertyKey
}

sealed trait SeekableArgs {
  def expr: Expression
  def sizeHint: Option[Int]

  def dependencies: Set[Identifier] = expr.dependencies

  def mapValues(f: Expression => Expression): SeekableArgs

  def asQueryExpression: QueryExpression[Expression]
  def asCommandSeekArgs: SeekArgs
}

case class SingleSeekableArg(expr: Expression) extends SeekableArgs {
  def sizeHint = Some(1)

  override def mapValues(f: Expression => Expression) = copy(f(expr))

  def asQueryExpression: SingleQueryExpression[Expression] = SingleQueryExpression(expr)
  def asCommandSeekArgs: SeekArgs = SingleSeekArg(expr.asCommandExpression)
}

case class ManySeekableArgs(expr: Expression) extends SeekableArgs {
  val sizeHint = expr match {
    case coll: Collection => Some(coll.expressions.size)
    case _ => None
  }

  override def mapValues(f: Expression => Expression) = expr match {
    case coll: Collection => copy(expr = coll.map(f))
    case _ => copy(expr = f(expr))
  }

  def asQueryExpression: QueryExpression[Expression] = expr match {
    case coll: Collection =>
      ZeroOneOrMany(coll.expressions) match {
        case One(value) => SingleQueryExpression(value)
        case _ => ManyQueryExpression(coll)
      }

    case _ =>
      ManyQueryExpression(expr)
  }

  def asCommandSeekArgs: SeekArgs = expr match {
    case coll: Collection =>
      ZeroOneOrMany(coll.expressions) match {
        case Zero => SeekArgs.empty
        case One(value) => SingleSeekArg(value.asCommandExpression)
        case Many(values) => ManySeekArgs(coll.asCommandExpression)
      }

    case _ =>
      ManySeekArgs(expr.asCommandExpression)
  }
}


