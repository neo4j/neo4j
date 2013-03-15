/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher.internal.executionplan.builders

import org.neo4j.cypher.internal.executionplan.{PartiallySolvedQuery, PlanBuilder}
import org.neo4j.cypher.internal.spi.PlanContext
import org.neo4j.cypher.internal.commands._
import org.neo4j.cypher.internal.commands.expressions.{Expression, Identifier, Property}
import org.neo4j.cypher.IndexHintException
import values.LabelValue
import org.neo4j.cypher.internal.commands.SchemaIndex
import org.neo4j.cypher.internal.executionplan.ExecutionPlanInProgress
import org.neo4j.cypher.internal.commands.Equals

class IndexLookupBuilder extends PlanBuilder {
  def canWorkWith(plan: ExecutionPlanInProgress, ctx: PlanContext) =
    plan.query.start.exists(interestingFilter)

  def apply(plan: ExecutionPlanInProgress, ctx: PlanContext): ExecutionPlanInProgress = {
    val startItem = extractInterestingStartItem(plan)
    val hint = startItem.token.asInstanceOf[SchemaIndex]
    val foundPredicates = findMatchingPredicates(plan, hint)

    val (originalLabelPred, labelPredicatesAfterThis) = extractLabelPredicates(plan, hint)

    if (foundPredicates.isEmpty)
      throw new IndexHintException(hint, "No useful predicate was found for your index hint. Make sure the" +
        " property expression is alone either side of the equality sign.")

    val (predicate, expression) = foundPredicates.head

    val q: PartiallySolvedQuery = plan.query

    val newHint: Unsolved[StartItem] = Unsolved(hint.copy(query = Some(expression)))
    val newQuery = q.copy(
      where = (q.where.filterNot(x => x == predicate || x == originalLabelPred) :+ predicate.solve) ++ labelPredicatesAfterThis,
      start = q.start.filterNot(_ == startItem) :+ newHint
    )

    plan.copy(query = newQuery)
  }


  private def extractLabelPredicates(plan: ExecutionPlanInProgress, hint: SchemaIndex): (QueryToken[Predicate], Seq[QueryToken[Predicate]]) = {
    val unsolvedHasLabels: Seq[(Unsolved[Predicate], Seq[LabelValue])] = plan.query.where.flatMap {
      case x@Unsolved(HasLabel(Identifier(identifier), labelNames))
        if hint.identifier == identifier && labelNames.exists(_.name == hint.label) => Some((x, labelNames))

      case _ => None
    }

    if (unsolvedHasLabels.isEmpty)
      throw new IndexHintException(hint, "The identifier used in the index hint is not marked with the expected label.")

    val (originalLabelPredicate, labelNames) = unsolvedHasLabels.head
    val (labelValue, rest) = labelNames.partition(_.name == hint.label)
    val remainingLabelPredicates = solveLabelPredicates(hint, labelValue, rest)
    (originalLabelPredicate, remainingLabelPredicates)
  }


  private def solveLabelPredicates(hint: SchemaIndex, labelValue: Seq[LabelValue], rest: Seq[LabelValue]): Seq[QueryToken[Predicate]] = {
    val solvedLabelPredicate: QueryToken[Predicate] = Solved(HasLabel(Identifier(hint.identifier), labelValue))
    val remainingLabelPredicates = Seq(solvedLabelPredicate) ++ (if (rest.nonEmpty)
      Some[QueryToken[Predicate]](Unsolved(HasLabel(Identifier(hint.identifier), rest)))
    else
      None)
    remainingLabelPredicates
  }

  private def findMatchingPredicates(plan: ExecutionPlanInProgress, hint: SchemaIndex): Seq[(Unsolved[Predicate], Expression)] =
    plan.query.where.flatMap {
      case x@Unsolved(Equals(Property(Identifier(id), prop), expression))
        if id == hint.identifier && prop == hint.property => Some((x, expression))

      case x@Unsolved(Equals(expression, Property(Identifier(id), prop)))
        if id == hint.identifier && prop == hint.property => Some((x, expression))

      case _ => None
    }

  private def extractInterestingStartItem(plan: ExecutionPlanInProgress): QueryToken[StartItem] =
    plan.query.start.filter(interestingFilter).head

  private def interestingFilter: PartialFunction[QueryToken[StartItem], Boolean] = {
    case Unsolved(SchemaIndex(_, _, _, None)) => true
    case _                                  => false
  }


  def priority = PlanBuilder.IndexLookup
}
