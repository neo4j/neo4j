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
package org.neo4j.cypher.planmatching

import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.ir.ordering.ProvidedOrder
import org.neo4j.cypher.internal.logical.plans.CacheProperties
import org.neo4j.cypher.internal.plandescription.Arguments.DbHits
import org.neo4j.cypher.internal.plandescription.Arguments.Details
import org.neo4j.cypher.internal.plandescription.Arguments.EstimatedRows
import org.neo4j.cypher.internal.plandescription.Arguments.GlobalMemory
import org.neo4j.cypher.internal.plandescription.Arguments.Memory
import org.neo4j.cypher.internal.plandescription.Arguments.PageCacheHits
import org.neo4j.cypher.internal.plandescription.Arguments.PageCacheMisses
import org.neo4j.cypher.internal.plandescription.Arguments.Rows
import org.neo4j.cypher.internal.plandescription.Arguments.Time
import org.neo4j.cypher.internal.plandescription.InternalPlanDescription
import org.neo4j.cypher.internal.plandescription.NoChildren
import org.neo4j.cypher.internal.plandescription.PlanDescriptionImpl
import org.neo4j.cypher.internal.plandescription.PrettyString
import org.neo4j.cypher.internal.plandescription.SingleChild
import org.neo4j.cypher.internal.plandescription.TwoChildren
import org.neo4j.cypher.internal.plandescription.asPrettyString
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.graphdb.schema.IndexType
import org.scalatest.matchers.MatchResult
import org.scalatest.matchers.Matcher

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

  def withTime(): PlanMatcher

  def withMemory(): PlanMatcher

  def withGlobalMemory(): PlanMatcher

  def withDBHits(hits: Long): PlanMatcher

  def withDBHits(): PlanMatcher

  def withDBHitsBetween(min: Long, max: Long): PlanMatcher

  def withPageCacheHits(): PlanMatcher

  def withPageCacheMisses(): PlanMatcher

  def withExactVariables(variables: String*): PlanMatcher

  def containingVariables(variables: String*): PlanMatcher

  def containingVariablesRegex(variables: Regex*): PlanMatcher

  def containingArgument(argument: String*): PlanMatcher

  // DummyImplicit used just to disambiguate `containingArgumentForProjection` after type erasure
  def containingArgumentForProjection(projections: (String, String)*)(implicit d: DummyImplicit): PlanMatcher = {
    val expectedArg = projections
      .map {
        case (k, v) => if (k == v) k else s"$v AS $k"
      }
      .mkString(", ")
    containingArgument(expectedArg)
  }

  def containingArgumentForProjection(keys: String*): PlanMatcher = {
    val k = keys.map(k => s".* AS $k").mkString(", ")
    containingArgumentRegex(k.r)
  }

  def containingArgumentForCachedProperty(varName: String, propName: String): PlanMatcher = {
    containingArgumentRegex(s".*cache\\[$varName\\.$propName].*".r)
  }

  def containingArgumentForNodeIndexPlan(
    varName: String,
    labelName: String,
    properties: Seq[String],
    unique: Boolean = false,
    indexType: IndexType = IndexType.RANGE,
    caches: Boolean = false
  ): PlanMatcher = {
    val i = if (unique) "UNIQUE" else s"${indexType.name()} INDEX"
    val p = properties.mkString(", ")
    val c = if (caches) properties.map(p => s", cache\\[$varName\\.$p]").mkString else ""
    containingArgumentRegex(s"$i $varName:$labelName\\($p\\).*$c".r)
  }

  def containingArgumentForRelIndexPlan(
    varName: String,
    start: String,
    typeName: String,
    end: String,
    properties: Seq[String],
    directed: Boolean,
    indexType: IndexType = IndexType.RANGE,
    caches: Boolean = false
  ): PlanMatcher = {
    val i = s"${indexType.name()} INDEX"
    val p = properties.mkString(", ")
    val c = if (caches) properties.map(p => s", cache\\[$varName\\.$p]").mkString else ""
    val endArrow = if (directed) "->" else "-"
    containingArgumentRegex(s"$i \\($start\\)-\\[$varName:$typeName\\($p\\)\\]$endArrow\\($end\\).*$c".r)
  }

  def containingArgumentRegex(argument: Regex*): PlanMatcher

  def withOrder(providedOrder: ProvidedOrder): PlanMatcher

  def withLHS(lhs: PlanMatcher): PlanMatcher

  def withRHS(rhs: PlanMatcher): PlanMatcher

  def withChildren(a: PlanMatcher, b: PlanMatcher): PlanMatcher

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

  override def withEstimatedRows(estimatedRows: Long): PlanMatcher =
    copy(inner = inner.withEstimatedRows(estimatedRows))

  override def withEstimatedRowsBetween(min: Long, max: Long): PlanMatcher =
    copy(inner = inner.withEstimatedRowsBetween(min, max))

  override def withTime(): PlanMatcher = copy(inner = inner.withTime())

  override def withMemory(): PlanMatcher = copy(inner = inner.withMemory())

  override def withGlobalMemory(): PlanMatcher = copy(inner = inner.withGlobalMemory())

  override def withDBHits(hits: Long): PlanMatcher = copy(inner = inner.withDBHits(hits))

  override def withDBHits(): PlanMatcher = copy(inner = inner.withDBHits())

  override def withDBHitsBetween(min: Long, max: Long): PlanMatcher = copy(inner = inner.withDBHitsBetween(min, max))

  override def withPageCacheHits(): PlanMatcher = copy(inner = inner.withPageCacheHits())

  override def withPageCacheMisses(): PlanMatcher = copy(inner = inner.withPageCacheMisses())

  override def withExactVariables(variables: String*): PlanMatcher =
    copy(inner = inner.withExactVariables(variables: _*))

  override def containingVariables(variables: String*): PlanMatcher =
    copy(inner = inner.containingVariables(variables: _*))

  override def containingVariablesRegex(variables: Regex*): PlanMatcher =
    copy(inner = inner.containingVariablesRegex(variables: _*))

  override def containingArgument(argument: String*): PlanMatcher = copy(inner = inner.containingArgument(argument: _*))

  override def containingArgumentRegex(argument: Regex*): PlanMatcher =
    copy(inner = inner.containingArgumentRegex(argument: _*))

  override def withOrder(providedOrder: ProvidedOrder): PlanMatcher = copy(inner = inner.withOrder(providedOrder))

  override def withLHS(lhs: PlanMatcher): PlanMatcher = copy(inner = inner.withLHS(lhs))

  override def withRHS(rhs: PlanMatcher): PlanMatcher = copy(inner = inner.withRHS(rhs))

  override def withChildren(a: PlanMatcher, b: PlanMatcher): PlanMatcher = copy(inner = inner.withChildren(a, b))
}

/**
 * Tries to find a matching plan a certain amount of times in the tree.
 *
 * @param expectedCount the expected count
 * @param inner         a PlanMatcher for the plan to find.
 * @param atLeast       if `expectedValue` is a lower bound or an expact expectation
 */
case class CountInTree(expectedCount: Int, inner: PlanMatcher, atLeast: Boolean = false) extends PlanMatcher {

  def allResults(plan: InternalPlanDescription): Seq[MatchResult] =
    inner(plan) +: plan.children.toIndexedSeq.flatMap(allResults)

  override def apply(plan: InternalPlanDescription): MatchResult = {
    val matchResults = allResults(plan)
    val count = matchResults.count(_.matches)
    MatchResult(
      matches = if (atLeast) count >= expectedCount else count == expectedCount,
      rawFailureMessage = s"Expected to find $toPlanDescription\n ${if (atLeast) "at least "
        else ""}$expectedCount times but found it $count times. Got $plan",
      rawNegatedFailureMessage =
        s"Did not expect to find $toPlanDescription\n ${if (atLeast) s"more than ${expectedCount - 1}"
          else s"exactly $expectedCount"} times but found it $count times. Got $plan"
    )
  }

  override def toPlanDescription: InternalPlanDescription = {
    val innerDesc = inner.toPlanDescription
    PlanDescriptionImpl(
      Id(0),
      s"* (${if (atLeast) "at least " else ""}$expectedCount times)",
      SingleChild(innerDesc),
      Seq.empty,
      Set.empty
    )
  }

  override def withName(name: String): PlanMatcher = copy(inner = inner.withName(name))

  override def withName(name: Regex): PlanMatcher = copy(inner = inner.withName(name))

  override def withRows(rows: Long): PlanMatcher = copy(inner = inner.withRows(rows))

  override def withRowsBetween(min: Long, max: Long): PlanMatcher = copy(inner = inner.withRowsBetween(min, max))

  override def withEstimatedRows(estimatedRows: Long): PlanMatcher =
    copy(inner = inner.withEstimatedRows(estimatedRows))

  override def withEstimatedRowsBetween(min: Long, max: Long): PlanMatcher =
    copy(inner = inner.withEstimatedRowsBetween(min, max))

  override def withTime(): PlanMatcher = copy(inner = inner.withTime())

  override def withMemory(): PlanMatcher = copy(inner = inner.withMemory())

  override def withGlobalMemory(): PlanMatcher = copy(inner = inner.withGlobalMemory())

  override def withDBHits(hits: Long): PlanMatcher = copy(inner = inner.withDBHits(hits))

  override def withDBHits(): PlanMatcher = copy(inner = inner.withDBHits())

  override def withDBHitsBetween(min: Long, max: Long): PlanMatcher = copy(inner = inner.withDBHitsBetween(min, max))

  override def withPageCacheHits(): PlanMatcher = copy(inner = inner.withPageCacheHits())

  override def withPageCacheMisses(): PlanMatcher = copy(inner = inner.withPageCacheMisses())

  override def withExactVariables(variables: String*): PlanMatcher =
    copy(inner = inner.withExactVariables(variables: _*))

  override def containingVariables(variables: String*): PlanMatcher =
    copy(inner = inner.containingVariables(variables: _*))

  override def containingVariablesRegex(variables: Regex*): PlanMatcher =
    copy(inner = inner.containingVariablesRegex(variables: _*))

  override def containingArgument(argument: String*): PlanMatcher = copy(inner = inner.containingArgument(argument: _*))

  override def containingArgumentRegex(argument: Regex*): PlanMatcher =
    copy(inner = inner.containingArgumentRegex(argument: _*))

  override def withOrder(providedOrder: ProvidedOrder): PlanMatcher = copy(inner = inner.withOrder(providedOrder))

  override def withLHS(lhs: PlanMatcher): PlanMatcher = copy(inner = inner.withLHS(lhs))

  override def withRHS(rhs: PlanMatcher): PlanMatcher = copy(inner = inner.withRHS(rhs))

  override def withChildren(a: PlanMatcher, b: PlanMatcher): PlanMatcher = copy(inner = inner.withChildren(a, b))
}

/**
 * Tries to match exactly against the root plan of a given InternalPlanDescription.
 */
case class ExactPlan(
  name: Option[PlanNameMatcher] = None,
  estimatedRows: Option[EstimatedRowsMatcher] = None,
  rows: Option[ActualRowsMatcher] = None,
  time: Option[TimeMatcher] = None,
  memory: Option[MemoryMatcher] = None,
  globalMemory: Option[GlobalMemoryMatcher] = None,
  dbHits: Option[DBHitsMatcher] = None,
  pageCacheHits: Option[PageCacheHitsMatcher] = None,
  pageCacheMisses: Option[PageCacheMissesMatcher] = None,
  order: Option[OrderArgumentMatcher] = None,
  variables: Option[VariablesMatcher] = None,
  other: Option[StringArgumentsMatcher] = None,
  lhs: Option[PlanMatcher] = None,
  rhs: Option[PlanMatcher] = None,
  children: Option[(PlanMatcher, PlanMatcher)] = None,
  skipCachingPlans: Boolean = false
) extends PlanMatcher {

  override def apply(plan: InternalPlanDescription): MatchResult = {
    val nameResult = name.map(_(plan))
    val estimatedRowsResult = estimatedRows.map(_(plan))
    val rowsResult = rows.map(_(plan))
    val dbHitsResult = dbHits.map(_(plan))
    val pageCacheHitsResult = pageCacheHits.map(_(plan))
    val pageCacheMissesResult = pageCacheMisses.map(_(plan))
    val timeResult = time.map(_(plan))
    val memoryResult = memory.map(_(plan))
    val globalMemoryResult = globalMemory.map(_(plan))
    val orderResult = order.map(_(plan))
    val variablesResult = variables.map(_(plan))
    val otherResult = other.map(_(plan))

    val maybeLhsPlan = {
      val maybeLhsChildPlan = plan.children.toIndexedSeq.headOption
      maybeLhsChildPlan match {
        case Some(lhsPlan) if skipCachingPlans && lhsPlan.name == CacheProperties.toString =>
          lhsPlan.children.toIndexedSeq.headOption
        case _ =>
          maybeLhsChildPlan
      }
    }
    val maybeRhsPlan = plan.children match {
      case TwoChildren(_, rhsPlan) => Some(rhsPlan)
      case _                       => None
    }

    val lhsResult = lhs.map { matcher =>
      maybeLhsPlan match {
        case None => MatchResult(
            matches = false,
            rawFailureMessage = s"Expected $toPlanDescription\n but ${plan.name} does not have a LHS.",
            rawNegatedFailureMessage = ""
          )
        case Some(lhsPlan) => matcher(lhsPlan)
      }
    }
    val rhsResult = rhs.map { matcher =>
      maybeRhsPlan match {
        case None => MatchResult(
            matches = false,
            rawFailureMessage = s"Expected $toPlanDescription\n but ${plan.name} does not have a RHS.",
            rawNegatedFailureMessage = ""
          )
        case Some(rhsPlan) => matcher(rhsPlan)
      }
    }
    val childrenResult = children.map { case (aMatcher, bMatcher) =>
      (maybeLhsPlan, maybeRhsPlan) match {
        case (Some(lhs), Some(rhs)) =>
          val res1a = aMatcher(lhs)
          val res1b = bMatcher(rhs)
          if (res1a.matches && res1b.matches) {
            res1a
          } else {
            val res2a = aMatcher(rhs)
            val res2b = bMatcher(lhs)
            if (res2a.matches && res2b.matches) {
              res2a
            } else {
              MatchResult(
                matches = false,
                rawFailureMessage =
                  s"Expected $toPlanDescription\n but ${plan.name} does not have the expected children.",
                rawNegatedFailureMessage = ""
              )
            }
          }

        case (_, _) =>
          MatchResult(
            matches = false,
            rawFailureMessage = s"Expected $toPlanDescription\n but ${plan.name} does not have two children.",
            rawNegatedFailureMessage = ""
          )
      }
    }

    val allResults = Seq(
      nameResult,
      estimatedRowsResult,
      rowsResult,
      dbHitsResult,
      pageCacheHitsResult,
      pageCacheMissesResult,
      timeResult,
      memoryResult,
      globalMemoryResult,
      orderResult,
      variablesResult,
      otherResult,
      lhsResult,
      rhsResult,
      childrenResult
    ).flatten

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
    val variablesDesc = variables.fold(Set.empty[PrettyString])(_.expected.map(asPrettyString.raw))
    val lhsDesc = lhs.map(_.toPlanDescription)
    val rhsDesc = rhs.map(_.toPlanDescription)
    val estRowArg = estimatedRows.map(m => EstimatedRows(m.expectedValue.toDouble, None)).toSeq
    val rowArg = rows.map(m => Rows(m.expectedValue)).toSeq
    val timeArg = time.map(m => Time(m.expectedValue)).toSeq
    val memoryArg = memory.map(m => Memory(m.expectedValue)).toSeq
    val globalMemoryArg = memory.map(m => GlobalMemory(m.expectedValue)).toSeq
    val dbHitsArg = dbHits.map(m => DbHits(m.expectedValue)).toSeq
    val pageCacheHitsArg = pageCacheHits.map(m => PageCacheHits(m.expectedValue)).toSeq
    val pageCacheMissesArg = pageCacheMisses.map(m => PageCacheMisses(m.expectedValue)).toSeq
    val orderArg = order.map(m => asPrettyString.order(m.expected)).toSeq
    val otherArgs = other
      .map(arg => Seq(Details(arg.expected.toSeq.map(str => asPrettyString(JustForToStringExpression(str))))))
      .getOrElse(Seq.empty)

    val children = (lhsDesc, rhsDesc) match {
      case (None, None)       => NoChildren
      case (Some(l), None)    => SingleChild(l)
      case (Some(l), Some(r)) => TwoChildren(l, r)
      case (None, Some(r))    => TwoChildren(PlanDescriptionImpl(Id(0), "???", NoChildren, Seq.empty, Set.empty), r)
    }

    PlanDescriptionImpl(
      Id(0),
      nameDesc,
      children,
      otherArgs ++ rowArg ++ estRowArg ++ dbHitsArg ++ pageCacheHitsArg ++ pageCacheMissesArg ++ orderArg ++ timeArg ++ memoryArg ++ globalMemoryArg,
      variablesDesc
    )
  }

  override def withName(name: String): PlanMatcher = this.name.fold(copy(name = Some(PlanExactNameMatcher(name))))(_ =>
    throw new IllegalArgumentException("cannot have more than one assertion on name")
  )

  override def withName(name: Regex): PlanMatcher = this.name.fold(copy(name = Some(PlanRegexNameMatcher(name))))(_ =>
    throw new IllegalArgumentException("cannot have more than one> assertion on name")
  )

  override def withRows(rows: Long): PlanMatcher =
    this.rows.fold(copy(rows = Some(new ExactArgumentMatcher(rows) with ActualRowsMatcher)))(_ =>
      throw new IllegalArgumentException("cannot have more than one assertion on rows")
    )

  override def withRowsBetween(min: Long, max: Long): PlanMatcher =
    this.rows.fold(copy(rows = Some(new RangeArgumentMatcher(min, max) with ActualRowsMatcher)))(_ =>
      throw new IllegalArgumentException("cannot have more than one assertion on rows")
    )

  override def withEstimatedRows(estimatedRows: Long): PlanMatcher = this.estimatedRows.fold(copy(estimatedRows =
    Some(new ExactArgumentMatcher(estimatedRows) with EstimatedRowsMatcher)
  ))(_ =>
    throw new IllegalArgumentException("cannot have more than one assertion on estimatedRows")
  )

  override def withEstimatedRowsBetween(min: Long, max: Long): PlanMatcher =
    this.estimatedRows.fold(copy(estimatedRows = Some(new RangeArgumentMatcher(min, max) with EstimatedRowsMatcher)))(
      _ => throw new IllegalArgumentException("cannot have more than one assertion on estimatedRows")
    )

  override def withTime(): PlanMatcher =
    this.time.fold(copy(time = Some(new RangeArgumentMatcher(1, Long.MaxValue) with TimeMatcher)))(_ =>
      throw new IllegalArgumentException("cannot have more than one assertion on time")
    )

  override def withMemory(): PlanMatcher =
    this.memory.fold(copy(memory = Some(new RangeArgumentMatcher(1, Long.MaxValue) with MemoryMatcher)))(_ =>
      throw new IllegalArgumentException("cannot have more than one assertion on memory")
    )

  override def withGlobalMemory(): PlanMatcher = this.globalMemory.fold(copy(globalMemory =
    Some(new RangeArgumentMatcher(1, Long.MaxValue) with GlobalMemoryMatcher)
  ))(_ =>
    throw new IllegalArgumentException("cannot have more than one assertion on global memory")
  )

  override def withDBHits(hits: Long): PlanMatcher =
    this.dbHits.fold(copy(dbHits = Some(new ExactArgumentMatcher(hits) with DBHitsMatcher)))(_ =>
      throw new IllegalArgumentException("cannot have more than one assertion on dbHits")
    )

  override def withDBHits(): PlanMatcher =
    this.dbHits.fold(copy(dbHits = Some(new RangeArgumentMatcher(1, Long.MaxValue) with DBHitsMatcher)))(_ =>
      throw new IllegalArgumentException("cannot have more than one assertion on dbHits")
    )

  override def withDBHitsBetween(min: Long, max: Long): PlanMatcher =
    this.dbHits.fold(copy(dbHits = Some(new RangeArgumentMatcher(min, max) with DBHitsMatcher)))(_ =>
      throw new IllegalArgumentException("cannot have more than one assertion on dbHits")
    )

  override def withPageCacheHits(): PlanMatcher = this.pageCacheHits.fold(copy(pageCacheHits =
    Some(new RangeArgumentMatcher(1, Long.MaxValue) with PageCacheHitsMatcher)
  ))(_ =>
    throw new IllegalArgumentException("cannot have more than one assertion on page cache hits")
  )

  override def withPageCacheMisses(): PlanMatcher = this.pageCacheMisses.fold(copy(pageCacheMisses =
    Some(new RangeArgumentMatcher(1, Long.MaxValue) with PageCacheMissesMatcher)
  ))(_ =>
    throw new IllegalArgumentException("cannot have more than one assertion on page cache misses")
  )

  override def withExactVariables(variables: String*): PlanMatcher =
    this.variables.fold(copy(variables = Some(ExactVariablesMatcher(variables.toSet))))(_ =>
      throw new IllegalArgumentException("cannot have more than one assertion on variables")
    )

  override def containingVariables(variables: String*): PlanMatcher =
    this.variables.fold(copy(variables = Some(ContainsVariablesMatcher(variables.toSet))))(_ =>
      throw new IllegalArgumentException("cannot have more than one assertion on variables")
    )

  override def containingVariablesRegex(variables: Regex*): PlanMatcher =
    this.variables.fold(copy(variables = Some(ContainsRegexVariablesMatcher(variables.toSet))))(_ =>
      throw new IllegalArgumentException("cannot have more than one assertion on variables")
    )

  override def containingArgument(argument: String*): PlanMatcher =
    this.other.fold(copy(other = Some(ContainsExactStringArgumentsMatcher(argument.toSet))))(_ =>
      throw new IllegalArgumentException("cannot have more than one assertion on other")
    )

  override def containingArgumentRegex(argument: Regex*): PlanMatcher =
    this.other.fold(copy(other = Some(ContainsRegexStringArgumentsMatcher(argument.toSet))))(_ =>
      throw new IllegalArgumentException("cannot have more than one assertion on other")
    )

  override def withOrder(providedOrder: ProvidedOrder): PlanMatcher =
    this.order.fold(copy(order = Some(OrderArgumentMatcher(providedOrder))))(_ =>
      throw new IllegalArgumentException("cannot have more than one assertion on order")
    )

  override def withLHS(lhs: PlanMatcher): PlanMatcher = this.lhs.fold(copy(lhs = Some(lhs)))(_ =>
    throw new IllegalArgumentException("cannot have more than one assertion on lhs")
  )

  override def withRHS(rhs: PlanMatcher): PlanMatcher = this.rhs.fold(copy(rhs = Some(rhs)))(_ =>
    throw new IllegalArgumentException("cannot have more than one assertion on rhs")
  )

  override def withChildren(a: PlanMatcher, b: PlanMatcher): PlanMatcher = this.rhs.fold(copy(children = Some((a, b))))(
    _ => throw new IllegalArgumentException("cannot have more than one assertion on children")
  )
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

  override def isConstantForQuery: Boolean = true
}
