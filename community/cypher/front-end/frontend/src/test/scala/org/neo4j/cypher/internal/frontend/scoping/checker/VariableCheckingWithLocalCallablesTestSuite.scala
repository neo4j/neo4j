/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.frontend.scoping.checker

import org.neo4j.cypher.internal.frontend.scoping.Outcome
import org.neo4j.cypher.internal.frontend.scoping.VariableCheckingTestSuite
import org.neo4j.cypher.internal.frontend.scoping.Versioned.ignoreBeforeCypher25

trait VariableCheckingWithLocalCallablesTestSuite extends VariableCheckingTestSuite {

  def testCases(): Seq[TestQuery]

  // for debug purpose of fuzzed test cases
  // set the seed of the test case you like to debug
  // it will run as the first test
  val seedToDebug: Option[Int] = None // Some(123) //

  for {
    seed <- seedToDebug
    TestQuery(query, outcome, _, _) = {
      val tcs = testCases()
      SurroundGivenQueriesWithLocalCallablesDefinition.sampleBySeed(tcs, seed)
    }
  } {
    test(query) {
      check(ignoreBeforeCypher25(Outcome.relaxedForFuzzing(outcome)))
    }
  }

  // regular test cases
  for {
    TestQuery(query, outcome, _, _) <- testCases()
  } {
    test(query) {
      check(outcome)
    }
  }

  // fuzzed test cases
  for {
    TestQuery(query, outcome, _, _) <- {
      val tcs = testCases()
      SurroundGivenQueriesWithLocalCallablesDefinition.sample(
        tcs,
        VariableCheckingWithLocalCallablesTestSuite.getAllTestCases,
        Math.max(3, 5 * Math.round(Math.log(tcs.size.toDouble)).toInt)
      )
    }
  } {
    test(query) {
      check(ignoreBeforeCypher25(Outcome.relaxedForFuzzing(outcome)))
    }
  }
}

object VariableCheckingWithLocalCallablesTestSuite {
  private val allTestCases = scala.collection.mutable.Buffer.empty[() => Seq[TestQuery]]
  def register(testCasesF: () => Seq[TestQuery]): Unit = allTestCases += testCasesF
  def getAllTestCases: Seq[TestQuery] = allTestCases.toSeq.flatMap(_.apply())
}
