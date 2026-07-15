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

import org.neo4j.cypher.internal.frontend.scoping.E42N29
import org.neo4j.cypher.internal.frontend.scoping.Versioned.ignoreBeforeCypher25
import org.neo4j.cypher.internal.frontend.scoping.Versioned.passesCypher25Onwards

/**
 * Test for 42N29 - Unbound Variables In Pattern Expression
 */
class GQL_42N29_UnboundVariablesInPatternExpressionTest extends VariableCheckingWithLocalCallablesTestSuite {
  VariableCheckingWithLocalCallablesTestSuite.register(() => testCases())

  override def testCases(): Seq[TestQuery] = Seq(
    // Negative tests
    TestQuery(
      """MATCH (a:A)
        |LET hasNeighbor = (a)-->(b)
        |RETURN a, hasNeighbor""".stripMargin,
      ignoreBeforeCypher25(E42N29("b")),
      Seq("a", "hasNeighbor")
    ),
    TestQuery(
      """MATCH (n)
        |RETURN (n)-[:T]->(b)""".stripMargin,
      E42N29("b"),
      Seq("`(n)-[:T]->(b)`")
    ),
    TestQuery(
      """MATCH (where :Node) WHERE ( WHERE { } )-->()
        |RETURN 1""".stripMargin,
      passesCypher25Onwards(E42N29("WHERE")),
      Seq("`1`")
    )
  )
}
