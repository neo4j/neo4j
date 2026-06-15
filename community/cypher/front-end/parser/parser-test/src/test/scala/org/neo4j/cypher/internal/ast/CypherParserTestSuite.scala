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
package org.neo4j.cypher.internal.ast

import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuiteWithMacroShadowing
import org.neo4j.cypher.internal.util.test_helpers.CypherScalaCheckDrivenPropertyChecks
import org.neo4j.cypher.internal.util.test_helpers.TestName
import org.scalactic.anyvals.PosInt
import org.scalatest.Args
import org.scalatest.BeforeAndAfterEach
import org.scalatest.Status

trait CypherParserTestSuite extends CypherFunSuiteWithMacroShadowing
    with CypherScalaCheckDrivenPropertyChecks
    with TestName
    with BeforeAndAfterEach {

  def minSuccessful(n: Int): PropertyCheckConfigParam = MinSuccessful(PosInt.ensuringValid(n))

  override protected def runTest(testName: String, args: Args): Status = super.runTest(testName, args)

}
