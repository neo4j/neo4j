/*
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

import org.neo4j.cypher.internal.compiler.v2_3.ast.convert.commands.ExpressionConverters._
import org.neo4j.cypher.internal.compiler.v2_3.ast.{Equals, _}
import org.neo4j.cypher.internal.compiler.v2_3.commands.{ManyQueryExpression, QueryExpression, RangeQueryExpression, SingleQueryExpression}
import org.neo4j.cypher.internal.compiler.v2_3.functions
import org.neo4j.cypher.internal.compiler.v2_3.helpers.{Many, One, Zero, ZeroOneOrMany}
import org.neo4j.cypher.internal.compiler.v2_3.parser.{LikePatternOp, LikePatternParser, MatchText, WildcardLikePatternOp}
import org.neo4j.cypher.internal.compiler.v2_3.pipes.{ManySeekArgs, SeekArgs, SingleSeekArg}

object WithSeekableArgs {
  def unapply(v: Any) = v match {
    case In(lhs, rhs) => Some(lhs -> ManySeekableArgs(rhs))
    case Equals(lhs, rhs) => Some(lhs -> SingleSeekableArg(rhs))
    case _ => None
  }
}

object AsIdSeekable {
  def unapply(v: Any) = v match {
    case WithSeekableArgs(func@FunctionInvocation(_, _, IndexedSeq(ident: Identifier)), rhs)
      if func.function.contains(functions.Id) && !rhs.dependencies(ident) =>
      Some(IdSeekable(func, ident, rhs))
    case _ =>
      None
  }
}

object AsPropertySeekable {
  def unapply(v: Any) = v match {
    case WithSeekableArgs(prop@Property(ident: Identifier, propertyKey), rhs)
      if !rhs.dependencies(ident) =>
      Some(PropertySeekable(prop, ident, rhs))
    case _ =>
      None
  }
}

object AsPropertyScannable {
  def unapply(v: Any) = v match {
    case func@FunctionInvocation(_, _, IndexedSeq(property@Property(ident: Identifier, _)))
      if func.function.contains(functions.Has) =>
      Some(PropertyScannable(func, ident, property))
    case _ =>
      None
  }
}

object AsStringRangeSeekable {
  def unapply(v: Any): Option[StringRangeSeekable] = v match {
    case like@Like(Property(ident: Identifier, propertyKey), LikePattern(lit@StringLiteral(value)), _)
      if !like.caseInsensitive =>
        for ((range, prefix) <- getRange(value))
          yield {
            val prefixPattern = LikePattern(StringLiteral(prefix)(lit.position))
            val predicate = like.copy(pattern = prefixPattern)(like.position)
            StringRangeSeekable(range, predicate, ident, propertyKey)
          }
    case _ =>
      None
  }

  def getRange(literal: String): Option[(SeekRange[String], String)] = {
    val ops: List[LikePatternOp] = LikePatternParser(literal).compact.ops
    ops match {
      case MatchText(prefix) :: (_: WildcardLikePatternOp) :: tl =>
        Some(PrefixRange(prefix) -> s"$prefix%")
      case _ =>
        None
    }
  }
}

object AsValueRangeSeekable {
  def unapply(v: Any): Option[ValueRangeSeekable] = v match {
    case ineq: InequalityExpression => ineq.lhs match {
      case Property(ident: Identifier, key) => Some(ValueRangeSeekable(ident, key, ineq))
      case _ => None
    }
    case _ => None
  }
}

sealed trait Sargable[T <: Expression] {
  def expr: T
  def ident: Identifier

  def name = ident.name
}

sealed trait Seekable[T <: Expression, A] extends Sargable[T] {

  def args: A
}

sealed trait EqualitySeekable[T <: Expression] extends Seekable[T, SeekableArgs]

case class IdSeekable(expr: FunctionInvocation, ident: Identifier, args: SeekableArgs)
  extends EqualitySeekable[FunctionInvocation]

case class PropertySeekable(expr: Property, ident: Identifier, args: SeekableArgs)
  extends EqualitySeekable[Property] {

  def propertyKey = expr.propertyKey
}

case class StringRangeSeekable(range: SeekRange[String], expr: Like, ident: Identifier, propertyKey: PropertyKeyName)
  extends Seekable[Like, LikePattern] {

  val args = expr.pattern

  def asQueryExpression: QueryExpression[Expression] = RangeQueryExpression(StringSeekRangeWrapper(range)(expr.rhs.position))
}

case class ValueRangeSeekable(ident: Identifier, propertyKeyName: PropertyKeyName, expr: InequalityExpression)
  extends Seekable[InequalityExpression, Expression] {

  val args = expr.rhs

  val range: HalfOpenSeekRange[Expression] = expr match {
    case _: LessThan => RangeLessThan(ExclusiveBound(args))
    case _: LessThanOrEqual => RangeLessThan(InclusiveBound(args))
    case _: GreaterThan => RangeGreaterThan(ExclusiveBound(args))
    case _: GreaterThanOrEqual => RangeGreaterThan(InclusiveBound(args))
  }

  def asQueryExpression: QueryExpression[Expression] =
    RangeQueryExpression(ValueExpressionSeekRangeWrapper(range)(args.position))
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


