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
 * Separator emitted between inner predicates of a [[CompositeQueryExpression]].
 *
 *  - [[CompositeSeparator.Comma]] (default): ", " — used by [[LogicalPlanToPlanBuilderString]] and other plan-builder consumers.
 *  - [[CompositeSeparator.And]]: " AND " — used by callers that embed the rendered string in a Cypher WHERE clause.
 */
sealed trait CompositeSeparator

object CompositeSeparator {
  case object Comma extends CompositeSeparator
  case object And extends CompositeSeparator
}

/**
 * How [[ExistenceQueryExpression]] / [[NonExistenceQueryExpression]] / [[AllQueryExpression]]
 * are rendered.
 *
 *  - [[ExistencePredicateForm.PropertyReference]] (default): emits the bare property reference
 *    ("n.prop" / "NOT n.prop"). NOT valid in a Cypher WHERE-clause predicate; intended for plan-builder (e.g. [[LogicalPlanToPlanBuilderString]]).
 *  - [[ExistencePredicateForm.IsNotNull]]: emits standard Cypher null-check predicates
 *    ("n.prop IS NOT NULL" / "n.prop IS NULL"). Use this when the rendered string must be valid in a WHERE clause.
 */
sealed trait ExistencePredicateForm

object ExistencePredicateForm {
  case object PropertyReference extends ExistencePredicateForm
  case object IsNotNull extends ExistencePredicateForm
}

class QueryExpressionStringifier(
  exprStringifier: ExpressionStringifier,
  valueStringifier: Option[Expression => String] = None,
  compositeSeparator: CompositeSeparator = CompositeSeparator.Comma,
  existencePredicateForm: ExistencePredicateForm = ExistencePredicateForm.PropertyReference
) {

  private val compositeSeparatorString: String = compositeSeparator match {
    case CompositeSeparator.Comma => ", "
    case CompositeSeparator.And   => " AND "
  }

  private def existencePredicate(ref: String): String = existencePredicateForm match {
    case ExistencePredicateForm.PropertyReference => ref
    case ExistencePredicateForm.IsNotNull         => s"$ref IS NOT NULL"
  }

  private def nonExistencePredicate(ref: String): String = existencePredicateForm match {
    case ExistencePredicateForm.PropertyReference => s"NOT $ref"
    case ExistencePredicateForm.IsNotNull         => s"$ref IS NULL"
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
            s"${propRef(propNames.head)} = ${expressions.map(stringify).mkString(" OR ")}"
          case expr =>
            s"${propRef(propNames.head)} IN ${stringify(expr)}"
        }
      case ExistenceQueryExpression => existencePredicate(propRef(propNames.head))
      case qe: RangeQueryExpression[?] =>
        qe.expression match {
          case PrefixSeekRangeWrapper(PrefixRange(expression)) =>
            s"${propRef(propNames.head)} STARTS WITH ${stringify(expression)}"
          case InequalitySeekRangeWrapper(range) =>
            rangeStr(range, propRef(propNames.head), stringify).toString
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
        existencePredicateForm match {
          case ExistencePredicateForm.PropertyReference => propRef(propNames.head)
          case ExistencePredicateForm.IsNotNull         => "true"
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
        val gt = rangeStr(greaterThan, propName, stringifier)
        val lt = rangeStr(lessThan, propName, stringifier)
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
