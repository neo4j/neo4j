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
package org.neo4j.cypher.internal.compiler.planner.logical.idp

import com.github.benmanes.caffeine.cache.Cache
import org.neo4j.cypher.internal.cache.CacheSize
import org.neo4j.cypher.internal.cache.ExecutorBasedCaffeineCacheFactory
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.planner.logical.idp.CacheBackedQPPInnerPlanner.CacheKeyInner
import org.neo4j.cypher.internal.compiler.planner.logical.idp.CacheBackedQPPInnerPlanner.CacheKeyOuter
import org.neo4j.cypher.internal.compiler.planner.logical.idp.extractQPPPredicates.ExtractedPredicates
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.InterestingOrderConfig
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.IsRepeatTrailUnique
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.UnPositionedVariable.varFor
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.ir.QuantifiedPathPattern
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.NonEmptyList

/**
 * Produces a logical plan for the the inner pattern of a QPP, which is the equivalent of the RHS of the Trail operator.
 * [[QPPInnerPlanner]] has two implementations, where [[CacheBackedQPPInnerPlanner]] simply provides an additional layer
 * of caching on top of the planning capabilities of [[IDPQPPInnerPlanner]].
 */
trait QPPInnerPlanner {

  /**
   * Takes a QPP pattern, a direction, and inlineable predicates. Depending on the direciton, it produces a LogicalPlan
   * with either the leftmost or rightmost node of the inner QPP as argument. The inlineable predicates are pushed down
   * as early as possible.
   *
   * @param qpp                   The QPP pattern containing the inner pattern to plan
   * @param fromLeft              The QPP node we can use as argument
   * @param extractedPredicates   The predicates inlineable for this inner pattern
   * @return
   */
  def planQPP(
    qpp: QuantifiedPathPattern,
    fromLeft: Boolean,
    extractedPredicates: ExtractedPredicates
  ): LogicalPlan

  /**
   * Updates the outer portion of a QPP that was previously planned by inserting arguments, and enabling TrailInto
   * semantics.
   *
   * We need to the insert the same arguments in the QPP as we did when originally planning the inner QPP so that
   * VerifyBestPlan can validate the plan.
   *
   * We currently don't have a TrailInto operator that would allow us to create a LogicalPlan when both juxtaposed
   * nodes of the QPP are bound. Such situations may however occur, and when they occur we need to be careful
   * that Trail doesn't overwrite the variable that was the previously bound juxtaposed end node. To this end, we update
   * the QPP to produce an anonymous variable that we can then do a join on (following the Trail operator).
   *
   * @param qpp               The QPP pattern to update
   * @param fromLeft          The QPP node we can use as argument
   * @param availableSymbols  The previously bound symbols
   * @return
   */
  def updateQpp(
    qpp: QuantifiedPathPattern,
    fromLeft: Boolean,
    availableSymbols: Set[String]
  ): QuantifiedPathPattern
}

/**
 * Serves plans from a cache.
 *
 * The caching strategy does not cache plans based on the set of available symbols. This is because there may be
 * multiple different sets of available symbols that result in the same plan. If a predicate requires symbols
 * { "a", "b" } then it doesn't matter whether the available symbols are { "a", "c" } or { "b", "c" }. In both cases
 * the predicate cannot be solved and thus the same plan will be generated.
 *
 * Instead, the caching strategy does cache plans based on the set of required symbols. This means first calculating
 * the set of solvable predicates based on a set of available symbols, and then calculating the subset of
 * available symbols which are actually required by said predicates. If a predicate requires symbols { "a", "b" }
 * then this strategy allows us to only compute the logical plan once whether the available symbols are
 * { "a", "b", "c"} or { "a", "b", "d" }.
 *
 * We maintain one cache per QPP so that one particularly noisy QPP does not evict all others.
 */
class CacheBackedQPPInnerPlanner(planner: => QPPInnerPlanner) extends QPPInnerPlanner {

  private[idp] var CACHE_MAX_SIZE: Int = 256
  private[idp] var caches: Map[CacheKeyOuter, Cache[CacheKeyInner, LogicalPlan]] = Map.empty

  override def planQPP(
    qpp: QuantifiedPathPattern,
    fromLeft: Boolean,
    extractedPredicates: ExtractedPredicates = ExtractedPredicates(Set.empty, Seq.empty)
  ): LogicalPlan = {
    val cacheKeyOuter = CacheKeyOuter(qpp, fromLeft)
    val cacheMaxSize = CacheSize.Static(CACHE_MAX_SIZE)
    val cache = caches.getOrElse(cacheKeyOuter, ExecutorBasedCaffeineCacheFactory.createCache(cacheMaxSize))
    val cacheKeyInner = CacheKeyInner(extractedPredicates.requiredSymbols)

    Option(cache.getIfPresent(cacheKeyInner)) match {
      case Some(plan) =>
        plan
      case None =>
        val qppInnerPlan = planner.planQPP(
          qpp,
          fromLeft,
          extractedPredicates
        )
        cache.put(cacheKeyInner, qppInnerPlan)
        caches = caches.updated(cacheKeyOuter, cache)
        qppInnerPlan
    }
  }

  override def updateQpp(
    qpp: QuantifiedPathPattern,
    fromLeft: Boolean,
    availableSymbols: Set[String]
  ): QuantifiedPathPattern =
    planner.updateQpp(qpp, fromLeft, availableSymbols)

}

object CacheBackedQPPInnerPlanner {

  case class CacheKeyOuter(qpp: QuantifiedPathPattern, fromLeft: Boolean)

  case class CacheKeyInner(requiredSymbols: Set[LogicalVariable])

}

case class IDPQPPInnerPlanner(context: LogicalPlanningContext) extends QPPInnerPlanner {

  override def planQPP(
    qpp: QuantifiedPathPattern,
    fromLeft: Boolean,
    extractedPredicates: ExtractedPredicates
  ): LogicalPlan = {
    val argumentsIntroducedByExtractedPredicates =
      extractedPredicates.requiredSymbols.map(_.name) -- qpp.argumentIds.map(_.name)
    val additionalArguments = argumentsIntroducedByExtractedPredicates + getQPPStartNode(qpp, fromLeft).name
    val additionalPredicates = extractedPredicates.predicates.map(_.extracted) ++ additionalTrailPredicates(qpp)
    val qg = qpp.asQueryGraph
      .addArgumentIds(additionalArguments)
      .addPredicates(additionalPredicates: _*)

    // We use InterestingOrderConfig.empty because the order from a RHS of Trail is not propagated anyway
    val plan = context.staticComponents.queryGraphSolver.plan(qg, InterestingOrderConfig.empty, context).result
    context.staticComponents.logicalPlanProducer.fixupTrailRhsPlan(
      plan,
      additionalArguments,
      additionalPredicates.toSet
    )
  }

  private def getQPPStartNode(qpp: QuantifiedPathPattern, fromLeft: Boolean): LogicalVariable = {
    if (fromLeft) qpp.leftBinding.inner
    else qpp.rightBinding.inner
  }

  private def additionalTrailPredicates(qpp: QuantifiedPathPattern): NonEmptyList[Expression] =
    qpp.patternRelationships.map(r =>
      IsRepeatTrailUnique(r.variable.asInstanceOf[Variable])(InputPosition.NONE)
    )

  override def updateQpp(
    qpp: QuantifiedPathPattern,
    fromLeft: Boolean,
    availableSymbols: Set[String]
  ): QuantifiedPathPattern = {
    val updated = updateQPPStartNodeArgument(qpp, fromLeft)

    val endNode = if (fromLeft) qpp.right.name else qpp.left.name
    val overlapping = availableSymbols.contains(endNode)
    if (overlapping) {
      updateQppForTrailInto(updated, fromLeft, context)
    } else {
      updated
    }
  }

  private def updateQPPStartNodeArgument(qpp: QuantifiedPathPattern, fromLeft: Boolean): QuantifiedPathPattern = {
    val additionalArgument = getQPPStartNode(qpp, fromLeft)
    qpp.copy(argumentIds = qpp.argumentIds.incl(additionalArgument))
  }

  private def updateQppForTrailInto(
    qpp: QuantifiedPathPattern,
    fromLeft: Boolean,
    context: LogicalPlanningContext
  ): QuantifiedPathPattern = {
    val newVar = varFor(context.staticComponents.anonymousVariableNameGenerator.nextName)
    val qppWithNewEndBindingOuterName =
      if (fromLeft) {
        qpp.copy(rightBinding = qpp.rightBinding.copy(outer = newVar))
      } else {
        qpp.copy(leftBinding = qpp.leftBinding.copy(outer = newVar))
      }
    qppWithNewEndBindingOuterName
  }
}
