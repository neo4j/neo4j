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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans

import org.neo4j.cypher.internal.compiler.v2_3._
import org.neo4j.cypher.internal.compiler.v2_3.ast.convert.commands.ExpressionConverters._
import org.neo4j.cypher.internal.compiler.v2_3.ast.{InequalitySeekRangeWrapper, PrefixSeekRangeWrapper}
import org.neo4j.cypher.internal.compiler.v2_3.commands.{ManyQueryExpression, QueryExpression, RangeQueryExpression, SingleQueryExpression}
import org.neo4j.cypher.internal.compiler.v2_3.helpers._
import org.neo4j.cypher.internal.compiler.v2_3.pipes.{ManySeekArgs, SeekArgs, SingleSeekArg}
import org.neo4j.cypher.internal.frontend.v2_3.ast._
import org.neo4j.cypher.internal.frontend.v2_3.{ExclusiveBound, InclusiveBound}

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
  def unapply(v: Any): Option[Scannable[Expression]] = v match {

    case func@FunctionInvocation(_, _, IndexedSeq(property@Property(ident: Identifier, _)))
      if func.function.contains(functions.Exists) =>
      Some(ExplicitlyPropertyScannable(func, ident, property))

    case expr: Equals =>
      partialPropertyPredicate(expr, expr.lhs)

    case expr: InequalityExpression =>
      partialPropertyPredicate(expr, expr.lhs)

    case startsWith: StartsWith =>
      partialPropertyPredicate(startsWith, startsWith.lhs)

    case regex: RegexMatch =>
      partialPropertyPredicate(regex, regex.lhs)

    case expr: NotEquals =>
      partialPropertyPredicate(expr, expr.lhs)

    case _ =>
      None
  }

  private def partialPropertyPredicate[P <: Expression](predicate: P, lhs: Expression) = lhs match {
    case property@Property(ident: Identifier, _) =>
      PartialPredicate.ifNotEqual(
        FunctionInvocation(FunctionName(functions.Exists.name)(predicate.position), property)(predicate.position),
        predicate
      ).map(ImplicitlyPropertyScannable(_, ident, property))

    case _ =>
      None
  }
}

object AsStringRangeSeekable {
  def unapply(v: Any): Option[PrefixRangeSeekable] = v match {
    case startsWith@StartsWith(Property(ident: Identifier, propertyKey), lit@StringLiteral(prefix)) if prefix.nonEmpty =>
      Some(PrefixRangeSeekable(PrefixRange(lit), startsWith, ident, propertyKey))
    case startsWith@StartsWith(Property(ident: Identifier, propertyKey), rhs) =>
      Some(PrefixRangeSeekable(PrefixRange(rhs), startsWith, ident, propertyKey))
    case _ =>
      None
  }
}

object AsValueRangeSeekable {
  def unapply(v: Any): Option[InequalityRangeSeekable] = v match {
    case inequalities@AndedPropertyInequalities(ident, prop, innerInequalities)
      if innerInequalities.forall( _.rhs.dependencies.isEmpty ) =>
        Some(InequalityRangeSeekable(ident, prop.propertyKey, inequalities))
    case _ =>
      None
  }
}

sealed trait Sargable[+T <: Expression] {
  def expr: T
  def ident: Identifier

  def name = ident.name
}

sealed trait Seekable[T <: Expression] extends Sargable[T] {
  def dependencies: Set[Identifier]
}

sealed trait EqualitySeekable[T <: Expression] extends Seekable[T] {
  def args: SeekableArgs
}

case class IdSeekable(expr: FunctionInvocation, ident: Identifier, args: SeekableArgs)
  extends EqualitySeekable[FunctionInvocation] {

  def dependencies = args.dependencies
}

case class PropertySeekable(expr: Property, ident: Identifier, args: SeekableArgs)
  extends EqualitySeekable[Property] {

  def propertyKey = expr.propertyKey
  def dependencies = args.dependencies
}

sealed trait RangeSeekable[T <: Expression, V] extends Seekable[T] {
  def range: SeekRange[V]
}

case class PrefixRangeSeekable(override val range: PrefixRange[Expression], expr: StartsWith, ident: Identifier, propertyKey: PropertyKeyName)
  extends RangeSeekable[StartsWith, Expression] {

  def dependencies = Set.empty

  def asQueryExpression: QueryExpression[Expression] =
    RangeQueryExpression(PrefixSeekRangeWrapper(range)(expr.rhs.position))
}

case class InequalityRangeSeekable(ident: Identifier, propertyKeyName: PropertyKeyName, expr: AndedPropertyInequalities)
  extends RangeSeekable[AndedPropertyInequalities, Expression] {

  def dependencies = expr.inequalities.map(_.dependencies).toSet.flatten

  def range: InequalitySeekRange[Expression] =
    InequalitySeekRange.fromPartitionedBounds(expr.inequalities.partition {
      case GreaterThan(_, value) => Left(ExclusiveBound(value))
      case GreaterThanOrEqual(_, value) => Left(InclusiveBound(value))
      case LessThan(_, value) => Right(ExclusiveBound(value))
      case LessThanOrEqual(_, value) => Right(InclusiveBound(value))
    })

  def asQueryExpression: QueryExpression[Expression] =
    RangeQueryExpression(InequalitySeekRangeWrapper(range)(ident.position))
}

sealed trait Scannable[+T <: Expression] extends Sargable[T] {
  def ident: Identifier
  def property: Property

  def propertyKey = property.propertyKey
}

case class ExplicitlyPropertyScannable(expr: FunctionInvocation, ident: Identifier, property: Property)
  extends Scannable[FunctionInvocation]

case class ImplicitlyPropertyScannable[+T <: Expression](expr: PartialPredicate[T], ident: Identifier, property: Property)
  extends Scannable[PartialPredicate[T]]

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
  def asCommandSeekArgs: SeekArgs = SingleSeekArg(toCommandExpression(expr))
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
        case One(value) => SingleSeekArg(toCommandExpression(value))
        case Many(values) => ManySeekArgs(toCommandExpression(coll))
      }

    case _ =>
      ManySeekArgs(toCommandExpression(expr))
  }
}


