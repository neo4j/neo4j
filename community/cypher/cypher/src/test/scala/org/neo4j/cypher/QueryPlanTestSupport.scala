/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher

import org.neo4j.cypher.internal.InternalExecutionResult
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.planDescription.InternalPlanDescription
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.planDescription.InternalPlanDescription.Arguments.KeyNames
import org.neo4j.cypher.internal.frontend.v3_3.helpers.StringHelper._
import org.neo4j.cypher.internal.v3_3.logical.plans.NodeHashJoin
import org.scalatest.matchers.{MatchResult, Matcher}

trait QueryPlanTestSupport {
  protected final val anonPattern = "([^\\w])anon\\[\\d+\\]".r

  protected def replaceAnonVariables(planText: String) =
    anonPattern.replaceAllIn(planText, "$1anon[*]")

  protected def havePlanLike(expectedPlan: String): Matcher[InternalExecutionResult] = new
      Matcher[InternalExecutionResult] {
    override def apply(result: InternalExecutionResult): MatchResult = {
      val plan: InternalPlanDescription = result.executionPlanDescription()
      val planText = replaceAnonVariables(plan.toString.trim.fixNewLines)
      val expectedText = replaceAnonVariables(expectedPlan.trim.fixNewLines)
      MatchResult(
        matches = planText.startsWith(expectedText),
        rawFailureMessage = s"Plan does not match expected\n\nPlan:\n$planText\n\nExpected:\n$expectedText",
        rawNegatedFailureMessage = s"Plan unexpected matches expected\n\nPlan:\n$planText\n\nExpected:\n$expectedText")
    }
  }

  def useOperators(operators: String*): Matcher[InternalPlanDescription] = new Matcher[InternalPlanDescription] {
    override def apply(plan: InternalPlanDescription): MatchResult = {
      MatchResult(
        matches = operators.forall(plan.find(_).nonEmpty),
        rawFailureMessage = s"Metadata: ${plan.arguments}\nPlan should use ${operators.mkString(",")}:\n$plan",
        rawNegatedFailureMessage = s"Plan should not use ${operators.mkString(",")}:\n$plan")
    }
  }

  def use(operators: String*): Matcher[InternalExecutionResult] = new Matcher[InternalExecutionResult] {
    override def apply(result: InternalExecutionResult): MatchResult = useOperators(operators:_*)(result.executionPlanDescription())
  }

  def useProjectionWith(otherText: String*): Matcher[InternalExecutionResult]  = new Matcher[InternalExecutionResult] {
    override def apply(result: InternalExecutionResult): MatchResult = useOperatorWithText("Projection", otherText: _*)(result.executionPlanDescription())
  }

  def useOperatorWithText(operator: String, otherText: String*): Matcher[InternalPlanDescription] = new Matcher[InternalPlanDescription] {
    override def apply(plan: InternalPlanDescription): MatchResult = {
      MatchResult(
        matches = otherText.forall(o => plan.find(operator).exists(_.toString.contains(o))),
        rawFailureMessage = s"Plan should use $operator with ${otherText.mkString(",")}:\n$plan",
        rawNegatedFailureMessage = s"Plan should not use $operator with ${otherText.mkString(",")}:\n$plan")
    }
  }

  def useOperatorTimes(operator: String, times: Int): Matcher[InternalPlanDescription] = new Matcher[InternalPlanDescription] {
    override def apply(plan: InternalPlanDescription): MatchResult = {
      MatchResult(
      matches = plan.find(operator).size == times,
      rawFailureMessage = s"Plan should use $operator ${times} times:\n$plan",
      rawNegatedFailureMessage = s"Plan should not use $operator ${times} times:\n$plan")
    }
  }

  def haveCount(count: Int): Matcher[InternalExecutionResult] = new Matcher[InternalExecutionResult] {
    override def apply(result: InternalExecutionResult): MatchResult = {
      MatchResult(
        matches = count == result.toList.length,
        rawFailureMessage = s"Result should have $count rows",
        rawNegatedFailureMessage = s"Result should not have $count rows")
    }
  }

  case class includeOnlyOneHashJoinOn(nodeVariable: String) extends Matcher[InternalPlanDescription] {

    private val hashJoinStr = classOf[NodeHashJoin].getSimpleName

    override def apply(result: InternalPlanDescription): MatchResult = {
      val hashJoins = result.flatten.filter { description =>
        description.name == hashJoinStr && description.arguments.contains(KeyNames(Seq(nodeVariable)))
      }
      val numberOfHashJoins = hashJoins.length

      MatchResult(numberOfHashJoins == 1, matchResultMsg(negated = false, result, numberOfHashJoins), matchResultMsg(negated = true, result, numberOfHashJoins))
    }

    private def matchResultMsg(negated: Boolean, result: InternalPlanDescription, numberOfHashJoins: Integer) =
      s"$hashJoinStr on node '$nodeVariable' should exist only once in the plan description ${if (negated) "" else s", but it occurred $numberOfHashJoins times"}\n $result"
  }

  case class includeOnlyOne[T](operator: Class[T], withVariable: String = "") extends includeOnly(operator, withVariable) {
    override def verifyOccurences(actualOccurences: Int) =
      actualOccurences == 1

    override def matchResultMsg(negated: Boolean, result: InternalPlanDescription, numberOfOperatorOccurences: Integer) =
      s"$joinStr on node '$withVariable' should occur only once in the plan description${if (negated) "" else s", but it occurred $numberOfOperatorOccurences times"}\n $result"
  }

  case class includeAtLeastOne[T](operator: Class[T], withVariable: String = "") extends includeOnly(operator, withVariable) {
    override def verifyOccurences(actualOccurences: Int) =
      actualOccurences >= 1

    override def matchResultMsg(negated: Boolean, result: InternalPlanDescription, numberOfOperatorOccurences: Integer) =
      s"$joinStr on node '$withVariable' should occur at least once in the plan description${if (negated) "" else s", but it was not found\n $result"}"
  }

  abstract class includeOnly[T](operator: Class[T], withVariable: String = "") extends Matcher[InternalPlanDescription] {
    protected val joinStr = operator.getSimpleName

    def verifyOccurences(actualOccurences: Int): Boolean

    def matchResultMsg(negated: Boolean, result: InternalPlanDescription, numberOfOperatorOccurences: Integer): String

    override def apply(result: InternalPlanDescription): MatchResult = {
      val operatorOccurrences = result.flatten.filter { description =>
        val nameCondition = description.name == joinStr
        val variableCondition = withVariable == "" || description.variables.contains(withVariable)
        nameCondition && variableCondition
      }
      val numberOfOperatorOccurrences = operatorOccurrences.length
      val matches = verifyOccurences(numberOfOperatorOccurrences)

      MatchResult(matches, matchResultMsg(negated = false, result, numberOfOperatorOccurrences), matchResultMsg(negated = true, result, numberOfOperatorOccurrences))
    }
  }
}
