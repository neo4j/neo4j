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

import org.neo4j.cypher.internal.frontend.scoping.E42N3B
import org.neo4j.cypher.internal.frontend.scoping.Passes
import org.neo4j.cypher.internal.frontend.scoping.Versioned.ignoreBeforeCypher25

/**
 * Test for 42N39 - Incompatible Number Of Return Columns
 */
class GQL_42N3B_IncompatibleNumberOfReturnColumnsTest extends VariableCheckingWithLocalCallablesTestSuite {
  VariableCheckingWithLocalCallablesTestSuite.register(() => testCases())

  override def testCases(): Seq[TestQuery] = Seq(
    // Negative tests
    TestQuery(
      """WHEN true THEN RETURN 1 AS x, 2 AS y
        |WHEN false THEN RETURN 2 AS x
        |ELSE RETURN 2 AS x, 3 AS y""".stripMargin,
      ignoreBeforeCypher25(E42N3B),
      Seq("x", "y")
    ),

    // Positive tests
    TestQuery(
      """WHEN true THEN RETURN 1 AS x, 2 AS y
        |WHEN false THEN RETURN 2 AS x, 1 AS y
        |ELSE RETURN 2 AS x, 3 AS y""".stripMargin,
      ignoreBeforeCypher25(Passes),
      Seq("x", "y")
    )
  )
}
