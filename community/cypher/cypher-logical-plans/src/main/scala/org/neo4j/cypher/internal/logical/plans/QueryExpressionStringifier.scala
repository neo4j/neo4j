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
package org.neo4j.cypher.internal.logical.plans

import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.ListLiteral
import org.neo4j.cypher.internal.expressions.LogicalVariable

/**
 * Selects what syntax [[QueryExpressionStringifier]] renders.
 *
 *  - [[Dialect.PlanBuilder]] (default): plan-builder DSL
 *  - [[Dialect.Cypher]]: valid Cypher
 *
 * Orthogonal to `valueStringifier`, which independently controls how individual value expressions are rendered.
 */
sealed trait Dialect

object Dialect {
  case object PlanBuilder extends Dialect
  case object Cypher extends Dialect
}

class QueryExpressionStringifier(
  exprStringifier: ExpressionStringifier,
  valueStringifier: Option[Expression => String] = None,
  dialect: Dialect = Dialect.PlanBuilder
) {

  private val compositeSeparatorString: String = dialect match {
    case Dialect.PlanBuilder => ", "
    case Dialect.Cypher      => " AND "
  }

  private def existencePredicate(ref: String): String = dialect match {
    case Dialect.PlanBuilder => ref
    case Dialect.Cypher      => s"$ref IS NOT NULL"
  }

  private def nonExistencePredicate(ref: String): String = dialect match {
    case Dialect.PlanBuilder => s"NOT $ref"
    case Dialect.Cypher      => s"$ref IS NULL"
  }

  def apply(valueExpr: QueryExpression[Expression], propNames: Seq[String]): String =
    apply(valueExpr, None, propNames)

  def apply(valueExpr: QueryExpression[Expression], entity: LogicalVariable, propNames: Seq[String]): String =
    apply(valueExpr, Some(entity), propNames)

  private def apply(
    valueExpr: QueryExpression[Expression],
    entity: Option[LogicalVariable],
    propNames: Seq[String]
  ): String = {
    def stringify(expression: Expression): String =
      valueStringifier.getOrElse((e: Expression) => exprStringifier(e))(expression)

    def propRef(propName: String): String = entity match {
      case Some(v) => s"${exprStringifier(v)}.${exprStringifier.backtick(propName)}"
      case None    => exprStringifier.backtick(propName)
    }

    valueExpr match {
      case qe: SingleQueryExpression[?] =>
        s"${propRef(propNames.head)} = ${stringify(qe.expression)}"
      case qe: ManyQueryExpression[?] =>
        qe.expression match {
          case ListLiteral(expressions) =>
            dialect match {
              case Dialect.PlanBuilder =>
                s"${propRef(propNames.head)} = ${expressions.map(stringify).mkString(" OR ")}"
              case Dialect.Cypher =>
                s"${propRef(propNames.head)} IN [${expressions.map(stringify).mkString(", ")}]"
            }
          case expr =>
            s"${propRef(propNames.head)} IN ${stringify(expr)}"
        }
      case ExistenceQueryExpression => existencePredicate(propRef(propNames.head))
      case qe: RangeQueryExpression[?] =>
        qe.expression match {
          case PrefixSeekRangeWrapper(PrefixRange(expression)) =>
            s"${propRef(propNames.head)} STARTS WITH ${stringify(expression)}"
          case InequalitySeekRangeWrapper(range) =>
            rangeStr(range, propRef(propNames.head), stringify)
          case PointBoundingBoxSeekRangeWrapper(PointBoundingBoxRange(lowerLeft, upperRight)) =>
            val llStr = stringify(lowerLeft)
            val urStr = stringify(upperRight)
            s"point.withinBBox(${propRef(propNames.head)}, $llStr, $urStr)"
          case PointDistanceSeekRangeWrapper(PointDistanceRange(point, distance, inclusive)) =>
            val pointStr = stringify(point)
            val distanceStr = stringify(distance)
            val operator = if (inclusive) "<=" else "<"
            s"point.distance(${propRef(propNames.head)}, $pointStr) $operator $distanceStr"
          case other =>
            throw new IllegalStateException(s"Unknown range expression: $other")
        }
      case qe: CompositeQueryExpression[?] =>
        qe.inner.zip(propNames).map { case (innerQe, propName) =>
          apply(innerQe, entity, Seq(propName))
        }.mkString(compositeSeparatorString)
      case AllQueryExpression =>
        dialect match {
          case Dialect.PlanBuilder => propRef(propNames.head)
          case Dialect.Cypher      => "true"
        }
      case NonExistenceQueryExpression => nonExistencePredicate(propRef(propNames.head))
      case other                       => throw new IllegalStateException(s"Unknown query expression: $other")
    }
  }

  private case class RangeStr(pre: Option[(String, String)], expr: String, post: (String, String)) {

    override def toString: String = {
      val preStr = pre match {
        case Some((vl: String, sign: String)) => s"$vl $sign "
        case None                             => ""
      }
      val postStr = s" ${post._1} ${post._2}"
      s"$preStr$expr$postStr"
    }
  }

  private def rangeStr(
    range: InequalitySeekRange[Expression],
    propName: String,
    stringifier: Expression => String
  ): String = {
    // One bound rendered as a complete predicate, e.g. "n.prop >= 5".
    def boundPredicate(bound: Bound[Expression], exclusiveSign: String): String = {
      val (sign, value) = boundStringifier(bound, exclusiveSign, stringifier)
      s"$propName $sign $value"
    }

    // chained form holds one lower & one upper bound, so can only render a range with at most two bounds.
    // larger ranges must emit every bound as a conjunction, otherwise the extra bounds are silently dropped.
    range match {
      case RangeGreaterThan(bounds) if bounds.tail.size <= 1 => chained(range, propName, stringifier).toString
      case RangeGreaterThan(bounds) => bounds.toIndexedSeq.map(boundPredicate(_, ">")).mkString(" AND ")
      case RangeLessThan(bounds) if bounds.tail.size <= 1 => chained(range, propName, stringifier).toString
      case RangeLessThan(bounds) => bounds.toIndexedSeq.map(boundPredicate(_, "<")).mkString(" AND ")
      case RangeBetween(gt, lt) if gt.bounds.tail.isEmpty && lt.bounds.tail.isEmpty =>
        chained(range, propName, stringifier).toString
      case RangeBetween(gt, lt) =>
        (gt.bounds.toIndexedSeq.map(boundPredicate(_, ">")) ++
          lt.bounds.toIndexedSeq.map(boundPredicate(_, "<"))).mkString(" AND ")
    }
  }

  /**
   * Renders the chained comparison form ("n.prop > v", "v0 < n.prop > v1", "lo < n.prop < hi").
   * Valid only for ranges of at most two bounds.
   */
  private def chained(
    range: InequalitySeekRange[Expression],
    propName: String,
    stringifier: Expression => String
  ): RangeStr = {
    range match {
      case RangeGreaterThan(bounds) =>
        if (bounds.tail.isEmpty) {
          val (sign, expr) = boundStringifier(bounds.head, ">", stringifier)
          RangeStr(None, propName, (sign, expr))
        } else {
          val pre = boundStringifier(bounds.head, "<", stringifier)
          val post = boundStringifier(bounds.tail.head, ">", stringifier)
          RangeStr(Some(pre.swap), propName, post)
        }
      case RangeLessThan(bounds) =>
        if (bounds.tail.isEmpty) {
          val (sign, expr) = boundStringifier(bounds.head, "<", stringifier)
          RangeStr(None, propName, (sign, expr))
        } else {
          val pre = boundStringifier(bounds.head, ">", stringifier)
          val post = boundStringifier(bounds.tail.head, "<", stringifier)
          RangeStr(Some(pre.swap), propName, post)
        }
      case RangeBetween(greaterThan, lessThan) =>
        require(
          greaterThan.bounds.tail.isEmpty && lessThan.bounds.tail.isEmpty,
          "Chained supports only single-bound sub-ranges. Multi-bound ranges must use the conjunction form."
        )
        val gt = chained(greaterThan, propName, stringifier)
        val lt = chained(lessThan, propName, stringifier)
        val pre: (String, String) = (gt.post._2, switchInequalitySignString(gt.post._1))
        RangeStr(Some(pre), propName, lt.post)
    }
  }

  private def boundStringifier(
    bound: Bound[Expression],
    exclusiveSign: String,
    stringifier: Expression => String
  ): (String, String) = {
    bound match {
      case InclusiveBound(endPoint) => (exclusiveSign + "=", stringifier(endPoint))
      case ExclusiveBound(endPoint) => (exclusiveSign, stringifier(endPoint))
    }
  }

  private def switchInequalitySignString(s: String): String = switchInequalitySignChar(s.head) +: s.tail

  private def switchInequalitySignChar(c: Char): Char = c match {
    case '>' => '<'
    case '<' => '>'
    case _   => c
  }
}
