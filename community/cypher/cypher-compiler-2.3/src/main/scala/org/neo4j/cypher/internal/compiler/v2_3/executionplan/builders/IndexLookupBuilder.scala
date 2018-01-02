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
package org.neo4j.cypher.internal.compiler.v2_3.executionplan.builders

import org.neo4j.cypher.internal.compiler.v2_3._
import org.neo4j.cypher.internal.compiler.v2_3.commands._
import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions._
import org.neo4j.cypher.internal.compiler.v2_3.commands.predicates._
import org.neo4j.cypher.internal.compiler.v2_3.executionplan._
import org.neo4j.cypher.internal.compiler.v2_3.pipes.PipeMonitor
import org.neo4j.cypher.internal.compiler.v2_3.spi.PlanContext
import org.neo4j.cypher.internal.frontend.v2_3.{ExclusiveBound, InclusiveBound, IndexHintException}

class IndexLookupBuilder extends PlanBuilder {
  def canWorkWith(plan: ExecutionPlanInProgress, ctx: PlanContext)(implicit pipeMonitor: PipeMonitor) =
    plan.query.start.exists(interestingFilter)

  def apply(plan: ExecutionPlanInProgress, ctx: PlanContext)(implicit pipeMonitor: PipeMonitor): ExecutionPlanInProgress = {
    val querylessHint: QueryToken[SchemaIndex] = extractInterestingStartItem(plan)
    val hint: SchemaIndex = querylessHint.token

    val propertyPredicates: Seq[(QueryToken[Predicate], QueryExpression[Expression])] =
      findPropertyPredicates(plan, hint) ++ findStartsWithPredicates(plan, hint) ++ findInequalityPredicates(plan, hint)
    val labelPredicates = findLabelPredicates(plan, hint)

    if (propertyPredicates.isEmpty || labelPredicates.isEmpty)
      throw new IndexHintException(hint.identifier, hint.label, hint.property,
        "No useful predicate was found for your index hint. Make sure the" +
          " property expression is alone either side of the equality sign.")

    val (predicate, expression) = propertyPredicates.head

    val q: PartiallySolvedQuery = plan.query

    val queryfullHint: Unsolved[StartItem] = Unsolved(hint.copy(query = Some(expression)))
    val newQuery = q.copy(
      where = q.where.filterNot(x => x == predicate || labelPredicates.contains(x)) ++ labelPredicates.map(_.solve) :+ predicate.solve,
      start = q.start.filterNot(_ == querylessHint) :+ queryfullHint
    )

    plan.copy(query = newQuery)
  }


  private def findLabelPredicates(plan: ExecutionPlanInProgress, hint: SchemaIndex) =
    plan.query.where.collect {
      case predicate@QueryToken(HasLabel(Identifier(identifier), label))
        if identifier == hint.identifier && label.name == hint.label => predicate
    }

  private def findPropertyPredicates(plan: ExecutionPlanInProgress, hint: SchemaIndex) =
    plan.query.where.collect {
      case predicate@QueryToken(Equals(Property(Identifier(id), prop), expression))
        if id == hint.identifier && prop.name == hint.property => (predicate, SingleQueryExpression(expression))

      case predicate@QueryToken(Equals(expression, Property(Identifier(id), prop)))
        if id == hint.identifier && prop.name == hint.property => (predicate, SingleQueryExpression(expression))

      case predicate@QueryToken(AnyInCollection(expression, _, Equals(Property(Identifier(id), prop),Identifier(_))))
        if id == hint.identifier && prop.name == hint.property => (predicate, ManyQueryExpression(expression))

      case predicate@QueryToken(PropertyExists(expr@Identifier(id), prop))
        if id == hint.identifier && prop.name == hint.property => (predicate, ScanQueryExpression(expr))
    }

  private def findStartsWithPredicates(plan: ExecutionPlanInProgress, hint: SchemaIndex) =
    plan.query.where.collect {
      case predicate@QueryToken(StartsWith(Property(Identifier(id), prop), rhs))
       if id == hint.identifier && prop.name == hint.property =>
        (predicate, RangeQueryExpression(PrefixSeekRangeExpression(PrefixRange(rhs))))
    }

  private def findInequalityPredicates(plan: ExecutionPlanInProgress, hint: SchemaIndex) =
    plan.query.where.collect {
      case predicate@QueryToken(AndedPropertyComparablePredicates(Identifier(id), Property(_, prop), comparables))
        if id == hint.identifier && prop.name == hint.property =>
        val seekRange: InequalitySeekRange[Expression] = InequalitySeekRange.fromPartitionedBounds(comparables.partition {
          case GreaterThan(_, value) => Left(ExclusiveBound(value))
          case GreaterThanOrEqual(_, value) => Left(InclusiveBound(value))
          case LessThan(_, value) => Right(ExclusiveBound(value))
          case LessThanOrEqual(_, value) => Right(InclusiveBound(value))
        })
        (predicate, RangeQueryExpression(InequalitySeekRangeExpression(seekRange)))
    }

  private def extractInterestingStartItem(plan: ExecutionPlanInProgress): QueryToken[SchemaIndex] =
    plan.query.start.filter(interestingFilter).head.asInstanceOf[QueryToken[SchemaIndex]]

  private def interestingFilter: PartialFunction[QueryToken[StartItem], Boolean] = {
    case Unsolved(SchemaIndex(_, _, _, _, None)) => true
    case _                                       => false
  }
}
