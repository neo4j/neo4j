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

import org.neo4j.cypher.internal.frontend.scoping.E42N39
import org.neo4j.cypher.internal.frontend.scoping.Passes
import org.neo4j.cypher.internal.frontend.scoping.Versioned.ignoreBeforeCypher25

/**
 * Test for 42N39 - Incompatible Return Column Names
 */
class GQL_42N39_IncompatibleReturnColumnNamesTest extends VariableCheckingWithLocalCallablesTestSuite {
  VariableCheckingWithLocalCallablesTestSuite.register(() => testCases())

  override def testCases(): Seq[TestQuery] = Seq(
    // Negative tests
    TestQuery(
      """RETURN 1 AS x
        |UNION
        |RETURN 2 AS y""".stripMargin,
      E42N39,
      Seq("x")
    ),
    TestQuery(
      """WHEN true THEN RETURN 1 AS x
        |WHEN false THEN RETURN 2 AS x
        |ELSE RETURN 3 AS y""".stripMargin,
      ignoreBeforeCypher25(E42N39),
      Seq("x")
    ),
    TestQuery(
      """WHEN true THEN RETURN 1 AS x
        |WHEN false THEN RETURN 2 AS y
        |ELSE RETURN 3 AS z""".stripMargin,
      ignoreBeforeCypher25(E42N39),
      Seq("x")
    ),
    TestQuery(
      """CALL () {
        |  RETURN 1 as x, 2 AS y
        |  UNION
        |  RETURN 3 AS y, 2 as z
        |}
        |RETURN x, y""".stripMargin,
      E42N39,
      Seq("x", "y")
    ),
    TestQuery(
      """CALL () {
        |  RETURN 2 AS x, 2 AS y, 2 AS z
        |  UNION
        |  RETURN 2 AS y, 2 AS x
        |}
        |RETURN x, y""".stripMargin,
      E42N39,
      Seq("x", "y")
    ),
    TestQuery(
      """CALL db.labels() YIELD label
        |UNION
        |CALL db.labels() YIELD label
        |RETURN label""".stripMargin,
      E42N39,
      Seq("x", "y")
    ),

    // Positive tests
    TestQuery(
      """CALL () {
        |  RETURN 1 as x, 2 AS y
        |  UNION
        |  RETURN 3 AS y, 2 as x
        |}
        |RETURN x, y""".stripMargin,
      Passes,
      Seq("x", "y")
    )
  )
}
