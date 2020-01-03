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
package org.neo4j.cypher.planmatching

import org.neo4j.cypher.internal.ir.v3_5.ProvidedOrder
import org.neo4j.cypher.internal.runtime.planDescription.InternalPlanDescription.Arguments
import org.neo4j.cypher.internal.runtime.planDescription.InternalPlanDescription.Arguments.{DbHits, EstimatedRows, Order, Rows}
import org.neo4j.cypher.internal.runtime.planDescription._
import org.neo4j.cypher.internal.v3_5.expressions.Expression
import org.neo4j.cypher.internal.v3_5.util.InputPosition
import org.neo4j.cypher.internal.v3_5.util.attribution.Id
import org.scalatest.matchers.{MatchResult, Matcher}

import scala.util.matching.Regex

/**
  * An immutable matcher for plan descriptions. The methods allow to obtain a new PlanMatcher and adding or overriding an assertion of this PlanMatcher.
  */
trait PlanMatcher extends Matcher[InternalPlanDescription] {
  /**
    * @return a PlanDescription that looks like what this Matcher is trying to match.
    */
  def toPlanDescription: InternalPlanDescription

  def withName(name: String): PlanMatcher

  def withName(name: Regex): PlanMatcher

  def withEstimatedRows(estimatedRows: Long): PlanMatcher

  def withEstimatedRowsBetween(min: Long, max: Long): PlanMatcher

  def withRows(rows: Long): PlanMatcher

  def withRowsBetween(min: Long, max: Long): PlanMatcher

  def withDBHits(hits: Long): PlanMatcher

  def withDBHits(): PlanMatcher

  def withDBHitsBetween(min: Long, max: Long): PlanMatcher

  def withExactVariables(variables: String*): PlanMatcher

  def containingVariables(variables: String*): PlanMatcher

  def containingArgument(argument: String*): PlanMatcher

  def containingArgumentRegex(argument: Regex*): PlanMatcher

  def withOrder(providedOrder: ProvidedOrder): PlanMatcher

  def withLHS(lhs: PlanMatcher): PlanMatcher

  def withRHS(rhs: PlanMatcher): PlanMatcher

  def onTopOf(plan: PlanMatcher): PlanMatcher = withLHS(plan)
}

/**
  * Tries to find a matching plan somewhere in the tree.
  *
  * @param inner a PlanMatcher for the plan to find.
  */
case class PlanInTree(inner: PlanMatcher) extends PlanMatcher {

  override def apply(plan: InternalPlanDescription): MatchResult = {
    val exactMatch = inner(plan)
    if (exactMatch.matches) {
      exactMatch
    } else {
      plan.children.toIndexedSeq.map(apply).collectFirst {
        case mr if mr.matches => mr
      }.getOrElse(
        MatchResult(
          matches = false,
          rawFailureMessage = s"Expected to find $toPlanDescription\n but got: \n $plan",
          rawNegatedFailureMessage = s"Expected not to find $toPlanDescription\n but got: \n $plan"
        )
      )
    }
  }

  override def toPlanDescription: InternalPlanDescription = {
    val innerDesc = inner.toPlanDescription
    PlanDescriptionImpl(Id(0), "*", SingleChild(innerDesc), Seq.empty, Set.empty)
  }

  override def withName(name: String): PlanMatcher = copy(inner = inner.withName(name))

  override def withName(name: Regex): PlanMatcher = copy(inner = inner.withName(name))

  override def withRows(rows: Long): PlanMatcher = copy(inner = inner.withRows(rows))

  override def withRowsBetween(min: Long, max: Long): PlanMatcher = copy(inner = inner.withRowsBetween(min, max))

  override def withEstimatedRows(estimatedRows: Long): PlanMatcher = copy(inner = inner.withEstimatedRows(estimatedRows))

  override def withEstimatedRowsBetween(min: Long, max: Long): PlanMatcher = copy(inner = inner.withEstimatedRowsBetween(min, max))

  override def withDBHits(hits: Long): PlanMatcher = copy(inner = inner.withDBHits(hits))

  override def withDBHits(): PlanMatcher = copy(inner = inner.withDBHits())

  override def withDBHitsBetween(min: Long, max: Long): PlanMatcher = copy(inner = inner.withDBHitsBetween(min, max))

  override def withExactVariables(variables: String*): PlanMatcher = copy(inner = inner.withExactVariables(variables: _*))

  override def containingVariables(variables: String*): PlanMatcher = copy(inner = inner.containingVariables(variables: _*))

  override def containingArgument(argument: String*): PlanMatcher = copy(inner = inner.containingArgument(argument: _*))

  override def containingArgumentRegex(argument: Regex*): PlanMatcher = copy(inner = inner.containingArgumentRegex(argument: _*))

  override def withOrder(providedOrder: ProvidedOrder): PlanMatcher = copy(inner = inner.withOrder(providedOrder))

  override def withLHS(lhs: PlanMatcher): PlanMatcher = copy(inner = inner.withLHS(lhs))

  override def withRHS(rhs: PlanMatcher): PlanMatcher = copy(inner = inner.withRHS(rhs))
}

/**
  * Tries to find a matching plan a certain amount of times in the tree.
  *
  * @param expectedCount the expected cound
  * @param inner         a PlanMatcher for the plan to find.
  * @param atLeast       if `expectedValue` is a lower bound or an expact expectation
  */
case class CountInTree(expectedCount: Int, inner: PlanMatcher, atLeast: Boolean = false) extends PlanMatcher {

  def allResults(plan: InternalPlanDescription): Seq[MatchResult] = inner(plan) +: plan.children.toIndexedSeq.flatMap(allResults)

  override def apply(plan: InternalPlanDescription): MatchResult = {
    val matchResults = allResults(plan)
    val count = matchResults.count(_.matches)
    MatchResult(
      matches = if (atLeast) count >= expectedCount else count == expectedCount,
      rawFailureMessage = s"Expected to find $toPlanDescription\n ${if (atLeast) "at least " else ""}$expectedCount times but found it $count times. Got $plan",
      rawNegatedFailureMessage = s"Did not expect to find $toPlanDescription\n ${if (atLeast) s"more than ${expectedCount - 1}" else s"exactly $expectedCount"} times but found it $count times. Got $plan"
    )
  }

  override def toPlanDescription: InternalPlanDescription = {
    val innerDesc = inner.toPlanDescription
    PlanDescriptionImpl(Id(0), s"* (${if (atLeast) "at least " else ""}$expectedCount times)", SingleChild(innerDesc), Seq.empty, Set.empty)
  }

  override def withName(name: String): PlanMatcher = copy(inner = inner.withName(name))

  override def withName(name: Regex): PlanMatcher = copy(inner = inner.withName(name))

  override def withRows(rows: Long): PlanMatcher = copy(inner = inner.withRows(rows))

  override def withRowsBetween(min: Long, max: Long): PlanMatcher = copy(inner = inner.withRowsBetween(min, max))

  override def withEstimatedRows(estimatedRows: Long): PlanMatcher = copy(inner = inner.withEstimatedRows(estimatedRows))

  override def withEstimatedRowsBetween(min: Long, max: Long): PlanMatcher = copy(inner = inner.withEstimatedRowsBetween(min, max))

  override def withDBHits(hits: Long): PlanMatcher = copy(inner = inner.withDBHits(hits))

  override def withDBHits(): PlanMatcher = copy(inner = inner.withDBHits())

  override def withDBHitsBetween(min: Long, max: Long): PlanMatcher = copy(inner = inner.withDBHitsBetween(min, max))

  override def withExactVariables(variables: String*): PlanMatcher = copy(inner = inner.withExactVariables(variables: _*))

  override def containingVariables(variables: String*): PlanMatcher = copy(inner = inner.containingVariables(variables: _*))

  override def containingArgument(argument: String*): PlanMatcher = copy(inner = inner.containingArgument(argument: _*))

  override def containingArgumentRegex(argument: Regex*): PlanMatcher = copy(inner = inner.containingArgumentRegex(argument: _*))

  override def withOrder(providedOrder: ProvidedOrder): PlanMatcher = copy(inner = inner.withOrder(providedOrder))

  override def withLHS(lhs: PlanMatcher): PlanMatcher = copy(inner = inner.withLHS(lhs))

  override def withRHS(rhs: PlanMatcher): PlanMatcher = copy(inner = inner.withRHS(rhs))
}

/**
  * Tries to match exactly against the root plan of a given InternalPlanDescription.
  */
case class ExactPlan(name: Option[PlanNameMatcher] = None,
                     estimatedRows: Option[EstimatedRowsMatcher] = None,
                     rows: Option[ActualRowsMatcher] = None,
                     dbHits: Option[DBHitsMatcher] = None,
                     order: Option[OrderArgumentMatcher] = None,
                     variables: Option[VariablesMatcher] = None,
                     other: Option[StringArgumentsMatcher] = None,
                     lhs: Option[PlanMatcher] = None,
                     rhs: Option[PlanMatcher] = None) extends PlanMatcher {

  override def apply(plan: InternalPlanDescription): MatchResult = {
    val nameResult = name.map(_ (plan))
    val estimatedRowsResult = estimatedRows.map(_ (plan))
    val rowsResult = rows.map(_ (plan))
    val dbHitsResult = dbHits.map(_ (plan))
    val orderResult = order.map(_ (plan))
    val variablesResult = variables.map(_ (plan))
    val otherResult = other.map(_ (plan))

    val maybeLhsPlan = plan.children.toIndexedSeq.headOption
    val maybeRhsPlan = plan.children match {
      case TwoChildren(_, rhsPlan) => Some(rhsPlan)
      case _ => None
    }

    val lhsResult = lhs.map { matcher =>
      maybeLhsPlan match {
        case None => MatchResult(
          matches = false,
          rawFailureMessage = s"Expected $toPlanDescription\n but ${plan.name} does not have a LHS.",
          rawNegatedFailureMessage = "")
        case Some(lhsPlan) => matcher(lhsPlan)
      }
    }
    val rhsResult = rhs.map { matcher =>
      maybeRhsPlan match {
        case None => MatchResult(
          matches = false,
          rawFailureMessage = s"Expected $toPlanDescription\n but ${plan.name} does not have a RHS.",
          rawNegatedFailureMessage = "")
        case Some(rhsPlan) => matcher(rhsPlan)
      }
    }

    val allResults = Seq(nameResult, estimatedRowsResult, rowsResult, dbHitsResult, orderResult, variablesResult, otherResult, lhsResult, rhsResult).flatten
    val firstMatch = allResults.collectFirst {
      case mr if mr.matches => mr
    }
    val firstNonMatch = allResults.collectFirst {
      case mr if !mr.matches => mr
    }

    MatchResult(
      matches = allResults.forall(_.matches),
      rawFailureMessage = firstNonMatch.fold("")(_.rawFailureMessage),
      rawNegatedFailureMessage = firstMatch.fold("")(_.rawNegatedFailureMessage),
      failureMessageArgs = firstNonMatch.fold(IndexedSeq.empty[Any])(_.failureMessageArgs),
      negatedFailureMessageArgs = firstMatch.fold(IndexedSeq.empty[Any])(_.negatedFailureMessageArgs)
    )
  }

  override def toPlanDescription: InternalPlanDescription = {
    val nameDesc = name.fold("???")(_.expectedName)
    val variablesDesc = variables.fold(Set.empty[String])(_.expected)
    val lhsDesc = lhs.map(_.toPlanDescription)
    val rhsDesc = rhs.map(_.toPlanDescription)
    val estRowArg = estimatedRows.map(m => EstimatedRows(m.expectedValue)).toSeq
    val rowArg = rows.map(m => Rows(m.expectedValue)).toSeq
    val dbHitsArg = dbHits.map(m => DbHits(m.expectedValue)).toSeq
    val orderArg = order.map(m => Order(m.expected)).toSeq
    val otherArgs = other.map(_.expected.toSeq.map(str => Arguments.Expression(JustForToStringExpression(str)))).getOrElse(Seq.empty)

    val children = (lhsDesc, rhsDesc) match {
      case (None, None) => NoChildren
      case (Some(l), None) => SingleChild(l)
      case (Some(l), Some(r)) => TwoChildren(l, r)
      case (None, Some(r)) => TwoChildren(PlanDescriptionImpl(Id(0), "???", NoChildren, Seq.empty, Set.empty), r)
    }

    PlanDescriptionImpl(Id(0), nameDesc, children, otherArgs ++ rowArg ++ estRowArg ++ dbHitsArg ++ orderArg, variablesDesc)
  }

  override def withName(name: String): PlanMatcher = copy(name = Some(PlanExactNameMatcher(name)))

  override def withName(name: Regex): PlanMatcher = copy(name = Some(PlanRegexNameMatcher(name)))

  override def withRows(rows: Long): PlanMatcher = copy(rows = Some(new ExactArgumentMatcher(rows) with ActualRowsMatcher))

  override def withRowsBetween(min: Long, max: Long): PlanMatcher = copy(rows = Some(new RangeArgumentMatcher(min, max) with ActualRowsMatcher))

  override def withEstimatedRows(estimatedRows: Long): PlanMatcher = copy(estimatedRows = Some(new ExactArgumentMatcher(estimatedRows) with EstimatedRowsMatcher))

  override def withEstimatedRowsBetween(min: Long, max: Long): PlanMatcher = copy(estimatedRows = Some(new RangeArgumentMatcher(min, max) with EstimatedRowsMatcher))

  override def withDBHits(hits: Long): PlanMatcher = copy(dbHits = Some(new ExactArgumentMatcher(hits) with DBHitsMatcher))

  override def withDBHits(): PlanMatcher = copy(dbHits = Some(new RangeArgumentMatcher(1, Long.MaxValue) with DBHitsMatcher))

  override def withDBHitsBetween(min: Long, max: Long): PlanMatcher = copy(dbHits = Some(new RangeArgumentMatcher(min, max) with DBHitsMatcher))

  override def withExactVariables(variables: String*): PlanMatcher = copy(variables = Some(ExactVariablesMatcher(variables.toSet)))

  override def containingVariables(variables: String*): PlanMatcher = copy(variables = Some(ContainsVariablesMatcher(variables.toSet)))

  override def containingArgument(argument: String*): PlanMatcher = copy(other = Some(ContainsExactStringArgumentsMatcher(argument.toSet)))

  override def containingArgumentRegex(argument: Regex*): PlanMatcher = copy(other = Some(ContainsRegexStringArgumentsMatcher(argument.toSet)))

  override def withOrder(providedOrder: ProvidedOrder): PlanMatcher = copy(order = Some(OrderArgumentMatcher(providedOrder)))

  override def withLHS(lhs: PlanMatcher): PlanMatcher = copy(lhs = Some(lhs))

  override def withRHS(rhs: PlanMatcher): PlanMatcher = copy(rhs = Some(rhs))
}

/**
  * For [[PlanMatcher.toPlanDescription]] we need to sneak in arbitrary strings to the "Other" column.
  * This Expression helps us do that.
  *
  * @param str the string to print in the "Other" column
  */
case class JustForToStringExpression(str: String) extends Expression {
  override def position: InputPosition = InputPosition.NONE

  override def asCanonicalStringVal: String = str
}
