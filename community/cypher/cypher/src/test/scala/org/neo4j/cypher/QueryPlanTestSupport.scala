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
package org.neo4j.cypher

import org.neo4j.cypher.internal.ExecutionPlan
import org.neo4j.cypher.internal.InterpretedRuntimeName
import org.neo4j.cypher.internal.RewindableExecutionResult
import org.neo4j.cypher.internal.RuntimeName
import org.neo4j.cypher.internal.plandescription.Argument
import org.neo4j.cypher.internal.runtime.ExecutionMode
import org.neo4j.cypher.internal.runtime.InputDataStream
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.util.InternalNotification
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.planmatching.CountInTree
import org.neo4j.cypher.planmatching.ExactPlan
import org.neo4j.cypher.planmatching.PlanInTree
import org.neo4j.cypher.planmatching.PlanMatcher
import org.neo4j.cypher.result.RuntimeResult
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.values.virtual.MapValue
import org.scalatest.matchers.MatchResult
import org.scalatest.matchers.Matcher

import scala.util.matching.Regex

trait QueryPlanTestSupport {

  /**
   * Allows the syntax
   * `plan should haveAsRoot.aPlan("ProduceResults")`
   */
  object haveAsRoot {
    def aPlan: PlanMatcher = ExactPlan()

    def aPlan(name: String): PlanMatcher = ExactPlan().withName(name)

    def aSourcePlan: PlanMatcher = ExactPlan(skipCachingPlans = true)

    def aSourcePlan(name: String): PlanMatcher = ExactPlan(skipCachingPlans = true).withName(name)
  }

  /**
   * Allows the syntax
   * ```
   * plan should includeSomewhere.aPlan("Filter")
   * plan should includeSomewhere.nTimes(2, aPlan("Filter"))
   * plan should includeSomewhere.atLeastNTimes(2, aPlan("Filter"))
   * ```
   */
  object includeSomewhere {
    def aPlan: PlanMatcher = PlanInTree(ExactPlan())

    def aPlan(name: String): PlanMatcher = PlanInTree(ExactPlan()).withName(name)
    def aPlanEndingWith(name: String): PlanMatcher = aPlan(s"\\w*$name".r)

    def aPlan(name: Regex): PlanMatcher = PlanInTree(ExactPlan()).withName(name)

    def nTimes(n: Int, aPlan: PlanMatcher): PlanMatcher = CountInTree(n, aPlan)

    def atLeastNTimes(n: Int, aPlan: PlanMatcher): PlanMatcher = CountInTree(n, aPlan, atLeast = true)
  }

  /**
   * Allows the syntax
   * ```
   * plan should haveAsRoot.aPlan("ProduceResults")
   * .withLHS(aPlan("Filter"))
   * ```
   */
  object aPlan {
    def apply(): PlanMatcher = haveAsRoot.aPlan

    def apply(name: String): PlanMatcher = haveAsRoot.aPlan(name)

    def apply(name: Regex): PlanMatcher = haveAsRoot.aPlan.withName(name)
  }

  object aPlanEndingWith {
    def apply(name: String): PlanMatcher = aPlan(s"\\w*$name".r)
  }

  /**
   * Same as `aPlan`, but skips over plans that may be sporadically inserted by optimization passes
   * that is of no interest to the test assertion,
   * e.g. CacheProperties.
   * This can make a test more resilient to changes in how those optimizations are applied, that
   * are irrelevant to what is being tested.
   */
  object aSourcePlan {
    def apply(): PlanMatcher = haveAsRoot.aSourcePlan

    def apply(name: String): PlanMatcher = haveAsRoot.aSourcePlan(name)
  }

  def haveCount(count: Int): Matcher[RewindableExecutionResult] = new Matcher[RewindableExecutionResult] {

    override def apply(result: RewindableExecutionResult): MatchResult = {
      MatchResult(
        matches = count == result.size,
        rawFailureMessage = s"Result should have $count rows",
        rawNegatedFailureMessage = s"Result should not have $count rows"
      )
    }
  }
}

object QueryPlanTestSupport {

  case class StubExecutionPlan(
    runtimeName: RuntimeName = InterpretedRuntimeName,
    metadata: Seq[Argument] = Seq.empty[Argument],
    override val operatorMetadata: Id => Seq[Argument] = _ => Seq.empty[Argument],
    notifications: Set[InternalNotification] = Set.empty[InternalNotification]
  ) extends ExecutionPlan {

    override def run(
      queryContext: QueryContext,
      executionMode: ExecutionMode,
      params: MapValue,
      prePopulateResults: Boolean,
      input: InputDataStream,
      subscriber: QuerySubscriber
    ): RuntimeResult = ???
  }
}
