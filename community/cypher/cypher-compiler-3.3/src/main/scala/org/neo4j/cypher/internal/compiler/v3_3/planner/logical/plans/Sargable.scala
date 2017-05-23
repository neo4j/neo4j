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
package org.neo4j.cypher.internal.compiler.v3_3.planner.logical.plans

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.QueryExpression
import org.neo4j.cypher.internal.compiler.v3_3._
import org.neo4j.cypher.internal.compiler.v3_3.ast.{InequalitySeekRangeWrapper, PrefixSeekRangeWrapper}
import org.neo4j.cypher.internal.compiler.v3_3.helpers._
import org.neo4j.cypher.internal.frontend.v3_3.ast._
import org.neo4j.cypher.internal.frontend.v3_3.{ExclusiveBound, InclusiveBound}

object WithSeekableArgs {
  def unapply(v: Any) = v match {
    case In(lhs, rhs) => Some(lhs -> ManySeekableArgs(rhs))
    case Equals(lhs, rhs) => Some(lhs -> SingleSeekableArg(rhs))
    case _ => None
  }
}

object AsIdSeekable {
  def unapply(v: Any) = v match {
    case WithSeekableArgs(func@FunctionInvocation(_, _, _, IndexedSeq(ident: Variable)), rhs)
      if func.function == functions.Id && !rhs.dependencies(ident) =>
      Some(IdSeekable(func, ident, rhs))
    case _ =>
      None
  }
}

object AsPropertySeekable {
  def unapply(v: Any) = v match {
    case WithSeekableArgs(prop@Property(ident: Variable, propertyKey), rhs)
      if !rhs.dependencies(ident) =>
      Some(PropertySeekable(prop, ident, rhs))
    case _ =>
      None
  }
}

object AsPropertyScannable {
  def unapply(v: Any): Option[Scannable[Expression]] = v match {

    case func@FunctionInvocation(_, _, _, IndexedSeq(property@Property(ident: Variable, _)))
      if func.function == functions.Exists =>
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
    case property@Property(ident: Variable, _) =>
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
    case startsWith@StartsWith(Property(ident: Variable, propertyKey), lit@StringLiteral(prefix)) if prefix.nonEmpty =>
      Some(PrefixRangeSeekable(PrefixRange(lit), startsWith, ident, propertyKey))
    case startsWith@StartsWith(Property(ident: Variable, propertyKey), rhs) =>
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

trait QueryExpression[+T] {

  def expressions: Seq[T]

  def map[R](f: T => R): QueryExpression[R]
}

trait SingleExpression[+T] {

  def expression: T

  def expressions = Seq(expression)
}

case class ScanQueryExpression[T](expression: T) extends QueryExpression[T] with SingleExpression[T] {
  def map[R](f: T => R) = ScanQueryExpression(f(expression))
}

case class SingleQueryExpression[T](expression: T) extends QueryExpression[T] with SingleExpression[T] {
  def map[R](f: T => R) = SingleQueryExpression(f(expression))
}

case class ManyQueryExpression[T](expression: T) extends QueryExpression[T] with SingleExpression[T] {
  def map[R](f: T => R) = ManyQueryExpression(f(expression))
}

case class RangeQueryExpression[T](expression: T) extends QueryExpression[T] with SingleExpression[T] {
  override def map[R](f: T => R) = RangeQueryExpression(f(expression))
}

case class CompositeQueryExpression[T](inner: Seq[QueryExpression[T]]) extends QueryExpression[T] {
  def map[R](f: T => R) = CompositeQueryExpression(inner.map(_.map(f)))

  override def expressions: Seq[T] = inner.flatMap(_.expressions)
}

sealed trait Sargable[+T <: Expression] {
  def expr: T
  def ident: Variable

  def name = ident.name
}

sealed trait Seekable[T <: Expression] extends Sargable[T] {
  def dependencies: Set[Variable]
}

sealed trait EqualitySeekable[T <: Expression] extends Seekable[T] {
  def args: SeekableArgs
}

case class IdSeekable(expr: FunctionInvocation, ident: Variable, args: SeekableArgs)
  extends EqualitySeekable[FunctionInvocation] {

  def dependencies: Set[Variable] = args.dependencies
}

case class PropertySeekable(expr: Property, ident: Variable, args: SeekableArgs)
  extends EqualitySeekable[Property] {

  def propertyKey: PropertyKeyName = expr.propertyKey
  def dependencies: Set[Variable] = args.dependencies
}

sealed trait RangeSeekable[T <: Expression, V] extends Seekable[T] {
  def range: SeekRange[V]
}

case class PrefixRangeSeekable(override val range: PrefixRange[Expression], expr: StartsWith, ident: Variable, propertyKey: PropertyKeyName)
  extends RangeSeekable[StartsWith, Expression] {

  def dependencies: Set[Variable] = Set.empty

  def asQueryExpression: QueryExpression[Expression] =
    RangeQueryExpression(PrefixSeekRangeWrapper(range)(expr.rhs.position))
}

case class InequalityRangeSeekable(ident: Variable, propertyKeyName: PropertyKeyName, expr: AndedPropertyInequalities)
  extends RangeSeekable[AndedPropertyInequalities, Expression] {

  def dependencies: Set[Variable] = expr.inequalities.map(_.dependencies).toSet.flatten

  def range: InequalitySeekRange[Expression] =
    InequalitySeekRange.fromPartitionedBounds(expr.inequalities.partition {
      case GreaterThan(_, value) => Left(ExclusiveBound(value))
      case GreaterThanOrEqual(_, value) => Left(InclusiveBound(value))
      case LessThan(_, value) => Right(ExclusiveBound(value))
      case LessThanOrEqual(_, value) => Right(InclusiveBound(value))
    })

  def hasEquality: Boolean = expr.inequalities.map(_.includeEquality).reduceLeft(_ || _)

  def asQueryExpression: QueryExpression[Expression] =
    RangeQueryExpression(InequalitySeekRangeWrapper(range)(ident.position))
}

sealed trait Scannable[+T <: Expression] extends Sargable[T] {
  def ident: Variable
  def property: Property

  def propertyKey: PropertyKeyName = property.propertyKey
}

case class ExplicitlyPropertyScannable(expr: FunctionInvocation, ident: Variable, property: Property)
  extends Scannable[FunctionInvocation]

case class ImplicitlyPropertyScannable[+T <: Expression](expr: PartialPredicate[T], ident: Variable, property: Property)
  extends Scannable[PartialPredicate[T]]

sealed trait SeekableArgs {
  def expr: Expression
  def sizeHint: Option[Int]

  def dependencies: Set[Variable] = expr.dependencies

  def mapValues(f: Expression => Expression): SeekableArgs
  def asQueryExpression: QueryExpression[Expression]
}

case class SingleSeekableArg(expr: Expression) extends SeekableArgs {
  def sizeHint = Some(1)

  override def mapValues(f: Expression => Expression): SingleSeekableArg = copy(f(expr))

  def asQueryExpression: SingleQueryExpression[Expression] = SingleQueryExpression(expr)
}

case class ManySeekableArgs(expr: Expression) extends SeekableArgs {
  val sizeHint: Option[Int] = expr match {
    case coll: ListLiteral => Some(coll.expressions.size)
    case _ => None
  }

  override def mapValues(f: Expression => Expression): ManySeekableArgs = expr match {
    case coll: ListLiteral => copy(expr = coll.map(f))
    case _ => copy(expr = f(expr))
  }

  def asQueryExpression: QueryExpression[Expression] = expr match {
    case coll: ListLiteral =>
      ZeroOneOrMany(coll.expressions) match {
        case One(value) => SingleQueryExpression(value)
        case _ => ManyQueryExpression(coll)
      }

    case _ =>
      ManyQueryExpression(expr)
  }
}


