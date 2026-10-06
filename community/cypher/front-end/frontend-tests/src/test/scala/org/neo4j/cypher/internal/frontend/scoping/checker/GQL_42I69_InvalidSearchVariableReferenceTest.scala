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

import org.neo4j.cypher.internal.frontend.scoping.E42I69
import org.neo4j.cypher.internal.frontend.scoping.Passes
import org.neo4j.cypher.internal.frontend.scoping.Versioned.ignoreBeforeCypher25

/**
 * Test for 42I69 - Invalid Search Variable Reference
 */
class GQL_42I69_InvalidSearchVariableReferenceTest extends VariableCheckingWithLocalCallablesTestSuite {
  VariableCheckingWithLocalCallablesTestSuite.register(() => testCases())

  override def testCases(): Seq[TestQuery] = Seq(
    // Negative tests
    TestQuery(
      """MATCH (m:Movie)
        |MATCH (movie:Movie)
        |  SEARCH m IN (
        |    VECTOR INDEX moviePlots
        |    FOR [1, 2, 3]
        |    LIMIT 5
        |  )
        |RETURN movie.title AS title""".stripMargin,
      ignoreBeforeCypher25(E42I69("m")),
      Seq("title")
    ),
    TestQuery(
      """MATCH (movie:Movie)
        |  SEARCH m IN (
        |    VECTOR INDEX moviePlots
        |    FOR [1, 2, 3]
        |    LIMIT 5
        |  )
        |RETURN movie.title AS title""".stripMargin,
      ignoreBeforeCypher25(E42I69("m")),
      Seq("title")
    ),
    // Positive tests
    TestQuery(
      """MATCH (movie:Movie)
        |  SEARCH movie IN (
        |    VECTOR INDEX moviePlots
        |    FOR [1, 2, 3]
        |    LIMIT 5
        |  )
        |RETURN movie.title AS title""".stripMargin,
      ignoreBeforeCypher25(Passes),
      Seq("title")
    )
  )
}
