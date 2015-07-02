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
package org.neo4j.cypher.internal.compiler.v2_3.ast.rewriters

import org.neo4j.cypher.internal.compiler.v2_3.{InputPosition, ast}
import org.neo4j.cypher.internal.compiler.v2_3.ast._
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans._

object ConsolidateRangePredicates {

  def apply[P, E](predicates: Seq[P])(implicit toolkit: RangePredicateToolkit[P, E]): Seq[P] = {
    val rangePredicates: Map[Option[E], Seq[Either[P, RangePredicate[P, E]]]] = predicates.map(toolkit.isRangePredicate)
      .groupBy {
      case Left(predicate) => None
      case Right(RangePredicate(target, _, _)) => Some(target)
    }

    val resultPredicates: Seq[P] = rangePredicates.collect {
      case (None, nonRangePredicates) =>
        nonRangePredicates.map {
          case Left(predicate) => predicate
          case Right(range) => range.original
        }

      case (Some(target), ranges) =>
        val greaterThanRanges = ranges.collect {
          case Right(RangePredicate(_, range@RangeGreaterThan(_), predicate)) => predicate -> range
        }
        val lessThanRanges = ranges.collect {
          case Right(RangePredicate(_, range@RangeLessThan(_), predicate)) => predicate -> range
        }

        val greaterThanPredicates = compact(greaterThanRanges)(toolkit.newMaxPredicate(target, _))
        val lessThanPredicates = compact(lessThanRanges)(toolkit.newMinPredicate(target, _))

        greaterThanPredicates ++ lessThanPredicates
    }.flatten.toSeq
    resultPredicates
  }

  private def compact[A, B](elts: Seq[(A, B)])(f: Seq[B] => Seq[A]): Seq[A] =
    if (elts.isEmpty) Seq.empty else if (elts.size == 1) Seq(elts.head._1) else f(elts.map(_._2))
}

trait RangePredicateToolkit[P, E] {

  def newMaxPredicate(target: E, ranges: Seq[RangeGreaterThan[E]]): Seq[P]

  def newMinPredicate(target: E, ranges: Seq[RangeLessThan[E]]): Seq[P]

  def isRangePredicate(predicate: P): Either[P, RangePredicate[P, E]]
}

case class RangePredicate[P, E](target: E, range: HalfOpenSeekRange[E], original: P)

object AstRangePredicateToolkit extends RangePredicateToolkit[ast.Expression, ast.Expression] {

  def isRangePredicate(predicate: ast.Expression): Either[ast.Expression, RangePredicate[ast.Expression, ast.Expression]] = predicate match {
    case original@LessThan(target: Property, expr) => Right(
      RangePredicate(target, RangeLessThan(ExclusiveBound(expr)), original))
    case original@LessThanOrEqual(target: Property, expr) => Right(
      RangePredicate(target, RangeLessThan(InclusiveBound(expr)), original))
    case original@GreaterThanOrEqual(target: Property, expr) => Right(
      RangePredicate(target, RangeGreaterThan(InclusiveBound(expr)), original))
    case original@GreaterThan(target: Property, expr) => Right(
      RangePredicate(target, RangeGreaterThan(ExclusiveBound(expr)), original))
    case _ => Left(predicate)
  }

  def newMaxPredicate(target: Expression, ranges: Seq[RangeGreaterThan[Expression]]): Seq[Expression] = {
    val (extrema, notEquals) = ranges.foldLeft((Seq.empty[Expression], Seq.empty[Expression])) {
      case ((boundsAcc, notEqualsAcc), RangeGreaterThan(ExclusiveBound(bound))) =>
        val newBoundsAcc = boundsAcc :+ bound
        val newNotEqualsAcc = notEqualsAcc :+ Not(Equals(target, bound)(InputPosition.NONE))(InputPosition.NONE)
        newBoundsAcc -> newNotEqualsAcc
      case ((boundsAcc, notEqualsAcc), RangeGreaterThan(InclusiveBound(bound))) =>
        (boundsAcc :+ bound) -> notEqualsAcc
    }
    GreaterThanOrEqual(target, Maximum(extrema))(InputPosition.NONE) +: notEquals
  }

  def newMinPredicate(target: Expression, ranges: Seq[RangeLessThan[Expression]]): Seq[Expression] = {
    val (extrema, notEquals) = ranges.foldLeft((Seq.empty[Expression], Seq.empty[Expression])) {
      case ((boundsAcc, notEqualsAcc), RangeLessThan(ExclusiveBound(bound))) =>
        val newBoundsAcc = boundsAcc :+ bound
        val newNotEqualsAcc = notEqualsAcc :+ Not(Equals(target, bound)(InputPosition.NONE))(InputPosition.NONE)
        newBoundsAcc -> newNotEqualsAcc
      case ((boundsAcc, notEqualsAcc), RangeLessThan(InclusiveBound(bound))) =>
        (boundsAcc :+ bound) -> notEqualsAcc
    }
    LessThanOrEqual(target, Minimum(extrema))(InputPosition.NONE) +: notEquals
  }
}