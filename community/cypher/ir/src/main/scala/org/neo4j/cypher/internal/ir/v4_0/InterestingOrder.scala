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
package org.neo4j.cypher.internal.ir.v4_0

import org.neo4j.cypher.internal.ir.v4_0.InterestingOrder.{Asc, ColumnOrder, Desc}
import org.neo4j.cypher.internal.v4_0.expressions._

import scala.annotation.tailrec

object InterestingOrder {

  sealed trait ColumnOrder {
    // Expression to sort by
    def expression: Expression

    // Projections needed to apply the sort of the expression
    def projections: Map[String, Expression]
  }

  case class Asc(expression: Expression, projections: Map[String, Expression] = Map.empty) extends ColumnOrder

  case class Desc(expression: Expression, projections: Map[String, Expression] = Map.empty) extends ColumnOrder

  val empty = InterestingOrder(RequiredOrderCandidate.empty, Seq.empty)

  def required(candidate: RequiredOrderCandidate): InterestingOrder = InterestingOrder(candidate, Seq.empty)

  def interested(candidate: InterestingOrderCandidate): InterestingOrder = InterestingOrder(RequiredOrderCandidate.empty, Seq(candidate))
}

/**
  * A single PlannerQuery can require an ordering on its results. This ordering can emerge
  * from an ORDER BY, or even from distinct or aggregation. The requirement on the ordering can
  * be strict (it needs to be ordered for correct results) or weak (ordering will allow for optimizations).
  * A weak requirement might in addition not care about column order and sort direction.
  *
  * Right now this type only encodes strict requirements that emerged because of an ORDER BY.
  *
  * @param requiredOrderCandidate     a candidate for required sort directions of columns
  * @param interestingOrderCandidates a sequence of candidates for interesting sort directions of columns
  */
case class InterestingOrder(requiredOrderCandidate: RequiredOrderCandidate,
                            interestingOrderCandidates: Seq[InterestingOrderCandidate] = Seq.empty) {

  import InterestingOrder._

  val isEmpty: Boolean = requiredOrderCandidate.isEmpty && interestingOrderCandidates.forall(_.isEmpty)

  // TODO maybe merge some candidates
  def interested(candidate: InterestingOrderCandidate): InterestingOrder = InterestingOrder(requiredOrderCandidate, interestingOrderCandidates :+ candidate)

  // TODO maybe merge some candidates
  def asInteresting: InterestingOrder =
    if (requiredOrderCandidate.isEmpty) this
    else InterestingOrder(RequiredOrderCandidate.empty,
      interestingOrderCandidates :+ requiredOrderCandidate.asInteresting)

  def withReverseProjectedColumns(projectExpressions: Map[String, Expression], argumentIds: Set[String]): InterestingOrder = {
    def columnIfArgument(expression: Expression, column: ColumnOrder): Option[ColumnOrder] = {
      if (argumentIds.contains(expression.asCanonicalStringVal)) Some(column) else None
    }

    def projectedColumnOrder(column: ColumnOrder, projected: Expression, name: String) = {
      column match {
        case _: Asc => Some(Asc(projected, Map(name -> projectExpressions(name))))
        case _: Desc => Some(Desc(projected, Map(name -> projectExpressions(name))))
      }
    }

    def rename(columns: Seq[ColumnOrder]): Seq[ColumnOrder] = {
      columns.flatMap { column: ColumnOrder =>
        // expression with all incoming projections applied
        val projected = projectExpression(column.expression, column.projections)
        projected match {
          case Property(Variable(prevVarName), _) if projectExpressions.contains(prevVarName) => projectedColumnOrder(column, projected, prevVarName)
          case Variable(prevVarName) if projectExpressions.contains(prevVarName) => projectedColumnOrder(column, projected, prevVarName)
          case _ =>
            columnIfArgument(projected, column)
        }
      }

    }

    InterestingOrder(requiredOrderCandidate.renameColumns(rename),
      interestingOrderCandidates.map(_.renameColumns(rename)).filter(!_.isEmpty))
  }

  private def projectExpression(expression: Expression, projections: Map[String, Expression]): Expression = {
    expression match {
      case Variable(varName) =>
        projections.getOrElse(varName, expression)

      case Property(Variable(varName), propertyKeyName) =>
        if (projections.contains(varName))
          Property(projections(varName), propertyKeyName)(expression.position)
        else
          expression

      case _ => expression
    }
  }

  /**
    * Checks if a RequiredOrder is satisfied by a ProvidedOrder
    */
  def satisfiedBy(providedOrder: ProvidedOrder): Boolean = {
    @tailrec
    def satisfied(providedOrder: Expression, requiredOrder: Expression, projections: Map[String, Expression]): Boolean = {
      val projected = projectExpression(requiredOrder, projections)
      if (providedOrder == requiredOrder || providedOrder == projected)
        true
      else if (projected != requiredOrder) {
        satisfied(providedOrder, projected, projections)
      }
      else
        false
    }
    requiredOrderCandidate.order.zipAll(providedOrder.columns, null, null).forall {
      case (null, _) => true // no required order left
      case (_, null) => false // required order left but no provided
      case (InterestingOrder.Asc(interestedExpr, projections), ProvidedOrder.Asc(providedExpr)) => satisfied(providedExpr, interestedExpr, projections)
      case (InterestingOrder.Desc(interestedExpr, projections), ProvidedOrder.Desc(providedExpr)) => satisfied(providedExpr, interestedExpr, projections)
      case _ => false
    }
  }
}

trait OrderCandidate {
  def order: Seq[ColumnOrder]

  def isEmpty: Boolean = order.isEmpty

  def nonEmpty: Boolean = !isEmpty

  def headOption: Option[ColumnOrder] = order.headOption

  def renameColumns(f: Seq[ColumnOrder] => Seq[ColumnOrder]): OrderCandidate

  def asc(expression: Expression, projections: Map[String, Expression]): OrderCandidate

  def desc(expression: Expression, projections: Map[String, Expression]): OrderCandidate
}

case class RequiredOrderCandidate(order: Seq[ColumnOrder]) extends OrderCandidate {
  def asInteresting: InterestingOrderCandidate = InterestingOrderCandidate(order)

  override def renameColumns(f: Seq[ColumnOrder] => Seq[ColumnOrder]): RequiredOrderCandidate = RequiredOrderCandidate(f(order))

  override def asc(expression: Expression, projections: Map[String, Expression] = Map.empty): RequiredOrderCandidate = RequiredOrderCandidate(order :+ Asc(expression, projections))

  override def desc(expression: Expression, projections: Map[String, Expression] = Map.empty): RequiredOrderCandidate = RequiredOrderCandidate(order :+ Desc(expression, projections))
}

object RequiredOrderCandidate {
  def empty: RequiredOrderCandidate = RequiredOrderCandidate(Seq.empty)

  def asc(expression: Expression, projections: Map[String, Expression] = Map.empty): RequiredOrderCandidate = empty.asc(expression, projections)

  def desc(expression: Expression, projections: Map[String, Expression] = Map.empty): RequiredOrderCandidate = empty.desc(expression, projections)
}

case class InterestingOrderCandidate(order: Seq[ColumnOrder]) extends OrderCandidate {
  override def renameColumns(f: Seq[ColumnOrder] => Seq[ColumnOrder]): InterestingOrderCandidate = InterestingOrderCandidate(f(order))

  override def asc(expression: Expression, projections: Map[String, Expression] = Map.empty): InterestingOrderCandidate = InterestingOrderCandidate(order :+ Asc(expression, projections))

  override def desc(expression: Expression, projections: Map[String, Expression] = Map.empty): InterestingOrderCandidate = InterestingOrderCandidate(order :+ Desc(expression, projections))
}

object InterestingOrderCandidate {
  def empty: InterestingOrderCandidate = InterestingOrderCandidate(Seq.empty)

  def asc(expression: Expression, projections: Map[String, Expression] = Map.empty): InterestingOrderCandidate = empty.asc(expression, projections)

  def desc(expression: Expression, projections: Map[String, Expression] = Map.empty): InterestingOrderCandidate = empty.desc(expression, projections)
}
