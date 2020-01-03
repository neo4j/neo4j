/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.ir

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

  /**
    * An [[InterestingOrder]] can be fully, partially, or not all all satisfied by a [[ProvidedOrder]].
    * This class specifies the satisfied prefix of columns and the missing suffix of columns.
    */
  case class Satisfaction(satisfiedPrefix: Seq[ColumnOrder], missingSuffix: Seq[ColumnOrder]){
    def withSatisfied(columnOrder: ColumnOrder): Satisfaction = this.copy(satisfiedPrefix :+ columnOrder, missingSuffix)
    def withMissing(columnOrder: ColumnOrder): Satisfaction = this.copy(satisfiedPrefix, missingSuffix :+ columnOrder)
  }

  object FullSatisfaction {
    def unapply(s: Satisfaction): Boolean = s.missingSuffix.isEmpty
  }

  object NoSatisfaction {
    def unapply(s: Satisfaction): Boolean = s.satisfiedPrefix.isEmpty && s.missingSuffix.nonEmpty
  }

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
  def interesting(candidate: InterestingOrderCandidate): InterestingOrder = InterestingOrder(requiredOrderCandidate, interestingOrderCandidates :+ candidate)

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
  def satisfiedBy(providedOrder: ProvidedOrder): Satisfaction = {
    @tailrec
    def satisfied(providedOrder: Expression, requiredOrder: Expression, projections: Map[String, Expression]): Boolean = {
      val projected = projectExpression(requiredOrder, projections)
      if (providedOrder == requiredOrder || providedOrder == projected) {
        true
      } else if (projected != requiredOrder) {
        satisfied(providedOrder, projected, projections)
      } else {
        false
      }
    }
    requiredOrderCandidate.order.zipAll(providedOrder.columns, null, null).foldLeft(Satisfaction(Seq.empty, Seq.empty)){
      case (s, (null, _)) => s // no required order left
      case (s@FullSatisfaction(), (satisfiedColumn@InterestingOrder.Asc(requiredExp, projections), ProvidedOrder.Asc(providedExpr))) if satisfied(providedExpr, requiredExp, projections)  => s.withSatisfied(satisfiedColumn)
      case (s@FullSatisfaction(), (satisfiedColumn@InterestingOrder.Desc(requiredExp, projections), ProvidedOrder.Desc(providedExpr))) if satisfied(providedExpr, requiredExp, projections) => s.withSatisfied(satisfiedColumn)
      case (s, (unsatisfiedColumn, _)) => s.withMissing(unsatisfiedColumn) // required order left but no provided or provided not matching or previous column not matching
    }
  }
}
