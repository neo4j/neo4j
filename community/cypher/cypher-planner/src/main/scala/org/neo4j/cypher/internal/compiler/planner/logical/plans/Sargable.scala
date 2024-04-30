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
package org.neo4j.cypher.internal.compiler.planner.logical.plans

import org.neo4j.cypher.internal.ast.IsNormalized
import org.neo4j.cypher.internal.ast.IsTyped
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.expressions.AndedPropertyInequalities
import org.neo4j.cypher.internal.expressions.CachedProperty
import org.neo4j.cypher.internal.expressions.Contains
import org.neo4j.cypher.internal.expressions.EndsWith
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.FunctionName
import org.neo4j.cypher.internal.expressions.GreaterThan
import org.neo4j.cypher.internal.expressions.GreaterThanOrEqual
import org.neo4j.cypher.internal.expressions.In
import org.neo4j.cypher.internal.expressions.InequalityExpression
import org.neo4j.cypher.internal.expressions.IsNotNull
import org.neo4j.cypher.internal.expressions.LessThan
import org.neo4j.cypher.internal.expressions.LessThanOrEqual
import org.neo4j.cypher.internal.expressions.ListLiteral
import org.neo4j.cypher.internal.expressions.LogicalProperty
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.Namespace
import org.neo4j.cypher.internal.expressions.Not
import org.neo4j.cypher.internal.expressions.PartialPredicate
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.RegexMatch
import org.neo4j.cypher.internal.expressions.StartsWith
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.expressions.functions
import org.neo4j.cypher.internal.logical.plans.ExclusiveBound
import org.neo4j.cypher.internal.logical.plans.InclusiveBound
import org.neo4j.cypher.internal.logical.plans.InequalitySeekRange
import org.neo4j.cypher.internal.logical.plans.InequalitySeekRangeWrapper
import org.neo4j.cypher.internal.logical.plans.ManySeekableArgs
import org.neo4j.cypher.internal.logical.plans.PointBoundingBoxRange
import org.neo4j.cypher.internal.logical.plans.PointBoundingBoxSeekRangeWrapper
import org.neo4j.cypher.internal.logical.plans.PointDistanceRange
import org.neo4j.cypher.internal.logical.plans.PointDistanceSeekRangeWrapper
import org.neo4j.cypher.internal.logical.plans.PrefixRange
import org.neo4j.cypher.internal.logical.plans.PrefixSeekRangeWrapper
import org.neo4j.cypher.internal.logical.plans.QueryExpression
import org.neo4j.cypher.internal.logical.plans.RangeQueryExpression
import org.neo4j.cypher.internal.logical.plans.SeekRange
import org.neo4j.cypher.internal.logical.plans.SeekableArgs
import org.neo4j.cypher.internal.logical.plans.SingleSeekableArg
import org.neo4j.cypher.internal.util.Last
import org.neo4j.cypher.internal.util.NonEmptyList
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTPoint
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.cypher.internal.util.symbols.PointType
import org.neo4j.cypher.internal.util.symbols.StringType
import org.neo4j.cypher.internal.util.symbols.TypeSpec

object WithSeekableArgs {

  def unapply(v: Any): Option[(Expression, SeekableArgs)] = v match {
    case In(lhs, rhs)     => Some(lhs -> ManySeekableArgs(rhs))
    case Equals(lhs, rhs) => Some(lhs -> SingleSeekableArg(rhs))
    case _                => None
  }
}

object AsIdSeekable {

  def unapply(v: Any): Option[IdSeekable] = v match {
    case WithSeekableArgs(func @ FunctionInvocation(_, _, IndexedSeq(ident: LogicalVariable), _, _), rhs)
      if func.function == functions.Id && !rhs.dependencies(ident) =>
      Some(IdSeekable(func, ident, rhs))
    case _ =>
      None
  }
}

object AsElementIdSeekable {

  def unapply(v: Any): Option[IdSeekable] = v match {
    case WithSeekableArgs(func @ FunctionInvocation(_, _, IndexedSeq(ident: LogicalVariable), _, _), rhs)
      if func.function == functions.ElementId && !rhs.dependencies(ident) =>
      Some(IdSeekable(func, ident, rhs))
    case _ =>
      None
  }
}

object AsPropertySeekable {

  def unapply(v: Any): Option[PropertySeekable] = v match {
    case WithSeekableArgs(prop @ Property(ident: LogicalVariable, _), rhs) if !rhs.dependencies(ident) =>
      Some(PropertySeekable(prop, ident, rhs))
    case WithSeekableArgs(prop @ CachedProperty(_, ident: LogicalVariable, _, _, _), rhs) if !rhs.dependencies(ident) =>
      Some(PropertySeekable(prop, ident, rhs))
    case _ =>
      None
  }
}

object AsExplicitlyPropertyScannable {

  def unapply(v: Any): Option[ExplicitlyPropertyScannable] = v match {
    case expr @ IsNotNull(property @ Property(ident: LogicalVariable, _)) =>
      Some(ExplicitlyPropertyScannable(expr, ident, property))
    case expr @ IsNotNull(property @ CachedProperty(_, ident: LogicalVariable, _, _, _)) =>
      Some(ExplicitlyPropertyScannable(expr, ident, property))

    case _ =>
      None
  }
}

object AsPropertyScannable {

  def unapply(v: Any): Option[Scannable[Expression]] = v match {

    case AsExplicitlyPropertyScannable(scannable) =>
      Some(scannable)

    case AsBoundingBoxSeekable(seekable) =>
      partialPropertyPredicate(seekable.expr, seekable.property, cypherType = CTPoint)

    case AsDistanceSeekable(seekable) =>
      partialPropertyPredicate(seekable.expr, seekable.property, cypherType = CTPoint)

    case expr: Equals =>
      partialPropertyPredicate(expr, expr.lhs)

    case expr: In =>
      partialPropertyPredicate(expr, expr.lhs)

    case expr: InequalityExpression =>
      partialPropertyPredicate(expr, expr.lhs)

    case outerExpr @ AndedPropertyInequalities(_, _, NonEmptyList(expr: InequalityExpression)) =>
      partialPropertyPredicate(outerExpr, expr.lhs)

    case startsWith: StartsWith =>
      partialPropertyPredicate(startsWith, startsWith.lhs, cypherType = CTString)

    case contains: Contains =>
      partialPropertyPredicate(contains, contains.lhs, cypherType = CTString)

    case endsWith: EndsWith =>
      partialPropertyPredicate(endsWith, endsWith.lhs, cypherType = CTString)

    case regex: RegexMatch =>
      partialPropertyPredicate(regex, regex.lhs, cypherType = CTString)

    case isTyped @ IsTyped(lhs, cypherType) if !cypherType.isNullable =>
      partialPropertyPredicate(isTyped, lhs, cypherType = cypherType)

    case isNormalized: IsNormalized =>
      partialPropertyPredicate(isNormalized, isNormalized.lhs, cypherType = CTString)

    case not @ Not(AsPropertyScannable(scannable)) =>
      partialPropertyPredicate(not, scannable.property, cypherType = scannable.cypherType)

    case _ =>
      None
  }

  private def partialPropertyPredicate[P <: Expression](
    predicate: P,
    lhs: Expression,
    cypherType: CypherType = CTAny
  ): Option[ImplicitlyPropertyScannable[IsNotNull]] = {
    lhs match {
      case property @ Property(ident: LogicalVariable, _) =>
        PartialPredicate.ifNotEqual(
          IsNotNull(property)(predicate.position),
          predicate
        ).map(ImplicitlyPropertyScannable(_, ident, property, solvesPredicate = false, cypherType = cypherType))

      case _ =>
        None
    }
  }
}

object AsStringRangeSeekable {

  def unapply(v: Any): Option[PrefixRangeSeekable] = v match {
    case startsWith @ StartsWith(prop @ Property(ident: LogicalVariable, _), lit @ StringLiteral(prefix))
      if prefix.nonEmpty =>
      Some(PrefixRangeSeekable(PrefixRange(lit), startsWith, ident, prop))
    case startsWith @ StartsWith(prop @ Property(ident: LogicalVariable, _), rhs) =>
      Some(PrefixRangeSeekable(PrefixRange(rhs), startsWith, ident, prop))
    case _ =>
      None
  }
}

object AsValueRangeSeekable {

  object AsVariableProperty {

    def unapply(v: Any): Option[(LogicalVariable, LogicalProperty)] = v match {
      case property @ Property(v: LogicalVariable, _) => Some(v -> property)
      case cachedProperty: CachedProperty             => Some(cachedProperty.entityVariable -> cachedProperty)
      case _                                          => None
    }
  }

  def unapply(v: Any): Option[InequalityRangeSeekable] = v match {
    case inequalities @ AndedPropertyInequalities(ident, prop, _) =>
      Some(InequalityRangeSeekable(ident, prop, inequalities))
    case inequality @ LessThan(AsVariableProperty(variable, property), _) =>
      Some(InequalityRangeSeekable(variable, property, AndedPropertyInequalities(variable, property, Last(inequality))))
    case inequality @ LessThanOrEqual(AsVariableProperty(variable, property), _) =>
      Some(InequalityRangeSeekable(variable, property, AndedPropertyInequalities(variable, property, Last(inequality))))
    case inequality @ GreaterThan(AsVariableProperty(variable, property), _) =>
      Some(InequalityRangeSeekable(variable, property, AndedPropertyInequalities(variable, property, Last(inequality))))
    case inequality @ GreaterThanOrEqual(AsVariableProperty(variable, property), _) =>
      Some(InequalityRangeSeekable(variable, property, AndedPropertyInequalities(variable, property, Last(inequality))))
    case _ =>
      None
  }
}

// WHERE point.distance(p.prop, otherPoint) < number
// and the like
object AsDistanceSeekable {

  def unapply(v: Any): Option[PointDistanceSeekable] = v match {
    case LessThan(DistanceFunction(prop @ Property(variable: Variable, _), otherPoint), distanceExpr) =>
      Some(PointDistanceSeekable(variable, prop, PointDistanceRange(otherPoint, distanceExpr, inclusive = false)))
    case LessThan(DistanceFunction(otherPoint, prop @ Property(variable: Variable, _)), distanceExpr) =>
      Some(PointDistanceSeekable(variable, prop, PointDistanceRange(otherPoint, distanceExpr, inclusive = false)))
    case LessThanOrEqual(DistanceFunction(prop @ Property(variable: Variable, _), otherPoint), distanceExpr) =>
      Some(PointDistanceSeekable(variable, prop, PointDistanceRange(otherPoint, distanceExpr, inclusive = true)))
    case LessThanOrEqual(DistanceFunction(otherPoint, prop @ Property(variable: Variable, _)), distanceExpr) =>
      Some(PointDistanceSeekable(variable, prop, PointDistanceRange(otherPoint, distanceExpr, inclusive = true)))

    case GreaterThan(distanceExpr, DistanceFunction(prop @ Property(variable: Variable, _), otherPoint)) =>
      Some(PointDistanceSeekable(variable, prop, PointDistanceRange(otherPoint, distanceExpr, inclusive = false)))
    case GreaterThan(distanceExpr, DistanceFunction(otherPoint, prop @ Property(variable: Variable, _))) =>
      Some(PointDistanceSeekable(variable, prop, PointDistanceRange(otherPoint, distanceExpr, inclusive = false)))
    case GreaterThanOrEqual(distanceExpr, DistanceFunction(prop @ Property(variable: Variable, _), otherPoint)) =>
      Some(PointDistanceSeekable(variable, prop, PointDistanceRange(otherPoint, distanceExpr, inclusive = true)))
    case GreaterThanOrEqual(distanceExpr, DistanceFunction(otherPoint, prop @ Property(variable: Variable, _))) =>
      Some(PointDistanceSeekable(variable, prop, PointDistanceRange(otherPoint, distanceExpr, inclusive = true)))

    case AndedPropertyInequalities(_, _, inequalities) if inequalities.size == 1 =>
      inequalities.head match {
        case AsDistanceSeekable(seekable) => Some(seekable)
        case _                            => None
      }

    case _ =>
      None
  }
}

object DistanceFunction {

  def unapply(v: Expression): Option[(Expression, Expression)] = v match {
    case FunctionInvocation(FunctionName(Namespace(List(namespace)), functionName), _, args, _, _)
      if namespace.equalsIgnoreCase("point") && functionName.equalsIgnoreCase("distance") => Some((args.head, args(1)))
    case _ => None
  }
}

object AsBoundingBoxSeekable {

  def unapply(v: Any): Option[PointBoundingBoxSeekable] = v match {
    case f @ FunctionInvocation(
        FunctionName(Namespace(List(namespace)), functionName),
        _,
        Seq(prop @ Property(ident: LogicalVariable, PropertyKeyName(_)), lowerLeft, upperRight),
        _,
        _
      ) if namespace.equalsIgnoreCase("point") && functionName.equalsIgnoreCase("withinbbox") =>
      Some(PointBoundingBoxSeekable(ident, prop, f, PointBoundingBoxRange(lowerLeft, upperRight)))
    case _ =>
      None
  }
}

/**
 * Predicate - or parts of it - the execution of which may be expedited with the use of an index.
 * @see [[https://en.wikipedia.org/wiki/Sargable]]
 */
sealed trait Sargable[+T <: Expression] {
  def expr: T
  def ident: LogicalVariable
}

/**
 * Sargable which enables us to skip parts of the index and therefore _seek_ the first element to return.
 */
sealed trait Seekable[T <: Expression] extends Sargable[T] {
  def dependencies: Set[LogicalVariable]

  /**
   * Return the type of the property that this seekable refers to.
   * E.g., for "n.prop = 5" this would return CTInt
   */
  def propertyValueType(semanticTable: SemanticTable): CypherType
}

sealed trait EqualitySeekable[T <: Expression] extends Seekable[T] {
  def args: SeekableArgs
}

case class IdSeekable(expr: FunctionInvocation, ident: LogicalVariable, args: SeekableArgs)
    extends EqualitySeekable[FunctionInvocation] {

  def dependencies: Set[LogicalVariable] = args.dependencies

  override def propertyValueType(semanticTable: SemanticTable): CypherType = CTAny
}

case class PropertySeekable(expr: LogicalProperty, ident: LogicalVariable, args: SeekableArgs)
    extends EqualitySeekable[LogicalProperty] {

  def propertyKey: PropertyKeyName = expr.propertyKey
  def dependencies: Set[LogicalVariable] = args.dependencies

  override def propertyValueType(semanticTable: SemanticTable): CypherType = {

    def getTypeSpec(expr: Expression): TypeSpec =
      semanticTable.typeFor(expr).typeInfo.getOrElse(TypeSpec.exact(CTAny))

    // TypeSpec.unwrapLists does not cope with Any, so we use this ugly solution. Can be removed on updated front-end.
    def unwrapLists(x: TypeSpec): TypeSpec =
      try {
        x.unwrapLists
      } catch {
        case _: MatchError =>
          TypeSpec.exact(CTAny)
      }

    args match {
      case SingleSeekableArg(seekableExpr) =>
        TypeSpec.cypherTypeForTypeSpec(getTypeSpec(seekableExpr))
      case ManySeekableArgs(seekableExpr) =>
        seekableExpr match {
          // Equality is rewritten to IN AFTER semantic check. Thus, we are lacking type information for the ListLiteral
          case ListLiteral(expressions) =>
            TypeSpec.combineMultipleTypeSpecs(expressions.map(exp => getTypeSpec(exp)))
          // When the query actually contained an IN, the list could be autoparameterized
          case _ =>
            TypeSpec.cypherTypeForTypeSpec(unwrapLists(getTypeSpec(args.expr)))
        }
    }
  }
}

sealed trait RangeSeekable[T <: Expression, V] extends Seekable[T] {
  def range: SeekRange[V]
}

case class PrefixRangeSeekable(
  override val range: PrefixRange[Expression],
  expr: StartsWith,
  ident: LogicalVariable,
  property: Property
) extends RangeSeekable[StartsWith, Expression] {

  def dependencies: Set[LogicalVariable] = expr.rhs.dependencies

  def asQueryExpression: QueryExpression[Expression] =
    RangeQueryExpression(PrefixSeekRangeWrapper(range)(expr.rhs.position))

  override def propertyValueType(semanticTable: SemanticTable): CypherType = CTString

  def propertyKeyName: PropertyKeyName = property.propertyKey
}

case class PointDistanceSeekable(
  ident: LogicalVariable,
  property: LogicalProperty,
  range: PointDistanceRange[Expression]
) extends RangeSeekable[Expression, Expression] {

  override def expr: Expression = range.point

  override def dependencies: Set[LogicalVariable] = range.point.dependencies ++ range.distance.dependencies

  def asQueryExpression: QueryExpression[Expression] =
    RangeQueryExpression(PointDistanceSeekRangeWrapper(range)(range.point.position))

  override def propertyValueType(semanticTable: SemanticTable): CypherType = CTPoint

  def propertyKeyName: PropertyKeyName = property.propertyKey
}

case class PointBoundingBoxSeekable(
  ident: LogicalVariable,
  property: LogicalProperty,
  expr: Expression,
  range: PointBoundingBoxRange[Expression]
) extends RangeSeekable[Expression, Expression] {

  override def dependencies: Set[LogicalVariable] = range.lowerLeft.dependencies ++ range.upperRight.dependencies

  def asQueryExpression: QueryExpression[Expression] =
    RangeQueryExpression(PointBoundingBoxSeekRangeWrapper(range)(expr.position))

  override def propertyValueType(semanticTable: SemanticTable): CypherType = CTPoint

  def propertyKeyName: PropertyKeyName = property.propertyKey
}

case class InequalityRangeSeekable(ident: LogicalVariable, property: LogicalProperty, expr: AndedPropertyInequalities)
    extends RangeSeekable[AndedPropertyInequalities, Expression] {

  def dependencies: Set[LogicalVariable] = expr.inequalities.map(_.rhs.dependencies).toSet.flatten

  def range: InequalitySeekRange[Expression] =
    InequalitySeekRange.fromPartitionedBounds(expr.inequalities.partition {
      case GreaterThan(_, value)        => Left(ExclusiveBound(value))
      case GreaterThanOrEqual(_, value) => Left(InclusiveBound(value))
      case LessThan(_, value)           => Right(ExclusiveBound(value))
      case LessThanOrEqual(_, value)    => Right(InclusiveBound(value))
    })

  def hasEquality: Boolean = expr.inequalities.map(_.includeEquality).reduceLeft(_ || _)

  def asQueryExpression: QueryExpression[Expression] =
    RangeQueryExpression(InequalitySeekRangeWrapper(range)(ident.position))

  override def propertyValueType(semanticTable: SemanticTable): CypherType = {
    TypeSpec.combineMultipleTypeSpecs(expr.inequalities.map(ineq =>
      semanticTable.types.get(ineq.rhs)
        .map(_.actual).getOrElse(TypeSpec.exact(CTAny))
    ).toIndexedSeq)
  }

  def propertyKeyName: PropertyKeyName = property.propertyKey
}

/**
 * Sargable which requires us to return the entirety of all values in an index, as all values fulfill this predicate.
 */
sealed trait Scannable[+T <: Expression] extends Sargable[T] {
  def ident: LogicalVariable
  def property: LogicalProperty
  def solvesPredicate: Boolean
  def cypherType: CypherType

  def propertyKey: PropertyKeyName = property.propertyKey
}

object Scannable {

  def isEquivalentScannable(predicate1: Expression, predicate2: Expression): Boolean = {
    def explicitlyScannableProperty(predicate: Expression) = predicate match {
      case AsExplicitlyPropertyScannable(scannable)              => Some(scannable.property)
      case IsTyped(property: LogicalProperty, StringType(false)) => Some(property)
      case IsTyped(property: LogicalProperty, PointType(false))  => Some(property)
      case _                                                     => None
    }

    explicitlyScannableProperty(predicate1) == explicitlyScannableProperty(predicate2)
  }
}

object ExplicitlyPropertyScannable {

  def apply(expr: FunctionInvocation, ident: LogicalVariable, property: LogicalProperty) =
    new ExplicitlyPropertyScannable(expr, ident, property)

  def apply(expr: IsNotNull, ident: LogicalVariable, property: LogicalProperty) =
    new ExplicitlyPropertyScannable(expr, ident, property)
}

case class ExplicitlyPropertyScannable private (expr: Expression, ident: LogicalVariable, property: LogicalProperty)
    extends Scannable[Expression] {
  override def solvesPredicate = true
  override def cypherType: CypherType = CTAny
}

case class ImplicitlyPropertyScannable[+T <: Expression](
  expr: PartialPredicate[T],
  ident: LogicalVariable,
  property: LogicalProperty,
  solvesPredicate: Boolean,
  cypherType: CypherType
) extends Scannable[PartialPredicate[T]]
