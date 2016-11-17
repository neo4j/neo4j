/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.internal.compiler.v3_2.executionplan.InternalExecutionResult
import org.neo4j.cypher.internal.compiler.v3_2.planDescription.InternalPlanDescription
import org.scalatest.matchers.{MatchResult, Matcher}

trait QueryPlanTestSupport {
  protected final val anonPattern = "([^\\w])anon\\[\\d+\\]".r

  protected def replaceAnonVariables(planText: String) =
    anonPattern.replaceAllIn(planText, "$1anon[*]")

  protected def havePlanLike(expectedPlan: String): Matcher[InternalExecutionResult] = new
      Matcher[InternalExecutionResult] {
    override def apply(result: InternalExecutionResult): MatchResult = {
      val plan: InternalPlanDescription = result.executionPlanDescription()
      val planText = replaceAnonVariables(plan.toString.trim)
      val expectedText = replaceAnonVariables(expectedPlan.trim)
      MatchResult(
        matches = planText.startsWith(expectedText),
        rawFailureMessage = s"Plan does not match expected\n\nPlan:\n$planText\n\nExpected:\n$expectedText",
        rawNegatedFailureMessage = s"Plan unexpected matches expected\n\nPlan:\n$planText\n\nExpected:\n$expectedText")
    }
  }
}
