/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.internal.cypher.acceptance.comparisonsupport

import org.neo4j.cypher.internal.RewindableExecutionResult
import org.neo4j.cypher.internal.runtime.planDescription.InternalPlanDescription
import org.scalatest.Assertions

import scala.util.Failure
import scala.util.Success
import scala.util.Try

sealed trait PlanComparisonStrategy extends Assertions {
  def compare(expectSucceed: TestConfiguration, scenario: TestScenario, result: RewindableExecutionResult): Unit
}

case object DoNotComparePlans extends PlanComparisonStrategy {
  override def compare(expectSucceed: TestConfiguration, scenario: TestScenario, result: RewindableExecutionResult): Unit = {}
}

case class ComparePlansWithPredicate(predicate: InternalPlanDescription => Boolean,
                                     expectPlansToFailPredicate: TestConfiguration = TestConfiguration.empty,
                                     predicateFailureMessage: String = "") extends PlanComparisonStrategy {
  override def compare(expectSucceed: TestConfiguration, scenario: TestScenario, result: RewindableExecutionResult): Unit = {
    val comparePlans = expectSucceed - expectPlansToFailPredicate
    if (comparePlans.containsScenario(scenario)) {
      if (!predicate(result.executionPlanDescription())) {
        fail(s"plan for ${scenario.name} did not fulfill predicate.\n$predicateFailureMessage\n${result.executionPlanString()}")
      }
    } else {
      if (predicate(result.executionPlanDescription())) {
        fail(s"plan for ${scenario.name} did unexpectedly fulfill predicate\n$predicateFailureMessage\n${result.executionPlanString()}")
      }
    }
  }
}

case class ComparePlansWithAssertion(assertion: InternalPlanDescription => Unit,
                                     expectPlansToFail: TestConfiguration = TestConfiguration.empty) extends PlanComparisonStrategy {
  override def compare(expectSucceed: TestConfiguration, scenario: TestScenario, result: RewindableExecutionResult): Unit = {
    val comparePlans = expectSucceed - expectPlansToFail
    if (comparePlans.containsScenario(scenario)) {
      withClue(s"plan for ${scenario.name}\n") {
        assertion(result.executionPlanDescription())
      }
    } else {
      val tryResult = Try(assertion(result.executionPlanDescription()))
      tryResult match {
        case Success(_) =>
          fail(s"plan for ${scenario.name} did unexpectedly succeed \n${result.executionPlanString()}")
        case Failure(_) =>
        // Expected to fail
      }
    }
  }
}
