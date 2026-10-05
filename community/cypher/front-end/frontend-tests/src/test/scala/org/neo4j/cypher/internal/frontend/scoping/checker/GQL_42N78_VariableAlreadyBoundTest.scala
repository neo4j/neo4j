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

import org.neo4j.cypher.internal.frontend.scoping.E42N78
import org.neo4j.cypher.internal.frontend.scoping.Passes
import org.neo4j.cypher.internal.frontend.scoping.Versioned.ignoreBeforeCypher25

/**
 * Test for 42N78 - Variable Already Bound (reported as 42001 caused by 42N59 caused by 42N78)
 */
class GQL_42N78_VariableAlreadyBoundTest extends VariableCheckingWithLocalCallablesTestSuite {
  VariableCheckingWithLocalCallablesTestSuite.register(() => testCases())

  override def testCases(): Seq[TestQuery] = Seq(
    // Negative tests
    TestQuery(
      """CREATE (a)
        |CREATE (a {p: 1})-[:REL]->(b)""".stripMargin,
      E42N78("a", "CREATE"),
      Seq.empty
    ),
    TestQuery(
      """CREATE (a)
        |CREATE (a :A:B)-[:REL]->(b)""".stripMargin,
      E42N78("a", "CREATE"),
      Seq.empty
    ),
    TestQuery(
      """MATCH (a)
        |CREATE (a {p: 1})-[:REL]->(b)""".stripMargin,
      E42N78("a", "CREATE"),
      Seq.empty
    ),
    TestQuery(
      """CREATE (a)
        |MERGE (a {p: 1})-[:REL]->(b)""".stripMargin,
      E42N78("a", "MERGE"),
      Seq.empty
    ),
    TestQuery(
      """CREATE (a)-[:REL]->(a {p: 1})""".stripMargin,
      E42N78("a", "CREATE"),
      Seq.empty
    ),
    TestQuery(
      """CREATE (a {p: 1})-[:REL]->(a {q: 2})""".stripMargin,
      E42N78("a", "CREATE"),
      Seq.empty
    ),
    TestQuery(
      """CREATE (a)
        |CREATE (a {p: 1})-[:REL]->()-[:REL2]->(b)""".stripMargin,
      E42N78("a", "CREATE"),
      Seq.empty
    ),
    TestQuery(
      """CREATE (a :A)-[:REL]->(),
        |       (a :B)-[:REL2]->()""".stripMargin,
      E42N78("a", "CREATE"),
      Seq.empty
    ),
    TestQuery(
      """CREATE (a:Foo)
        |MERGE (a)-[r:KNOWS]->(a:Bar)""".stripMargin,
      E42N78("a", "MERGE"),
      Seq.empty
    ),
    TestQuery(
      """MATCH (n)
        |WITH COLLECT(n) AS ns
        |FOREACH (n IN ns |
        |  CREATE ()-[:R]->(n {p:1})
        |)
        |RETURN ns""".stripMargin,
      E42N78("n", "CREATE"),
      Seq("ns")
    ),
    TestQuery(
      """MATCH (n)
        |WITH COLLECT(n) AS ns
        |FOREACH (n IN ns |
        |  MERGE ()-[:R]->(n {p:1})
        |)
        |RETURN ns""".stripMargin,
      E42N78("n", "MERGE"),
      Seq("ns")
    ),
    TestQuery(
      """MATCH (n)
        |WITH COLLECT(n) AS ns
        |WITH ns, head(ns) AS a
        |FOREACH (a IN ns | // <- shadows the WITH a
        |  CREATE (a {p:1})-[:R]->(n)
        |)
        |RETURN a, ns""".stripMargin,
      E42N78("a", "CREATE"),
      Seq("a", "ns")
    ),
    TestQuery(
      """MATCH (n)
        |WITH COLLECT(n) AS ns
        |WITH ns, head(ns) AS a
        |FOREACH (a IN ns | // <- shadows the WITH a
        |  MERGE (a {p:1})-[:R]->(n)
        |)
        |RETURN a, ns""".stripMargin,
      E42N78("a", "MERGE"),
      Seq("a", "ns")
    ),
    TestQuery(
      """MATCH (n) DETACH DELETE n
        |FINISH NEXT
        |CREATE (:A), (:B), (:C)
        |FINISH NEXT
        |MATCH (n)
        |RETURN COLLECT(n) AS ns
        |NEXT
        |FOREACH (n IN ns |
        |  CREATE ()-[:R]->(n {p:1})
        |)
        |FINISH NEXT
        |MATCH (n) RETURN n""".stripMargin,
      ignoreBeforeCypher25(E42N78("n", "CREATE")),
      Seq("n")
    ),
    TestQuery(
      """MATCH (n) DETACH DELETE n
        |FINISH NEXT
        |CREATE (:A), (:B), (:C)
        |FINISH NEXT
        |MATCH (n)
        |RETURN COLLECT(n) AS ns
        |NEXT
        |FOREACH (n IN ns |
        |  MERGE ()-[:R]->(n {p:1})
        |)
        |FINISH NEXT
        |MATCH (n) RETURN n""".stripMargin,
      ignoreBeforeCypher25(E42N78("n", "MERGE")),
      Seq("n")
    ),
    TestQuery(
      """MATCH (n) DETACH DELETE n
        |FINISH NEXT
        |CREATE (:A), (:B), (:C)
        |FINISH NEXT
        |MATCH (n)
        |RETURN COLLECT(n) AS ns
        |NEXT
        |LET a = head(ns)
        |FOREACH (a IN ns | // <- shadows the LET a
        |  CREATE (a {p:1})-[:R]->(n)
        |)
        |FINISH NEXT
        |MATCH (n) RETURN n""".stripMargin,
      ignoreBeforeCypher25(E42N78("a", "CREATE")),
      Seq("n")
    ),
    TestQuery(
      """MATCH (n) DETACH DELETE n
        |FINISH NEXT
        |CREATE (:A), (:B), (:C)
        |FINISH NEXT
        |MATCH (n)
        |RETURN COLLECT(n) AS ns
        |NEXT
        |LET a = head(ns)
        |FOREACH (a IN ns | // <- shadows the LET a
        |  MERGE (a {p:1})-[:R]->(n)
        |)
        |FINISH NEXT
        |MATCH (n) RETURN n""".stripMargin,
      ignoreBeforeCypher25(E42N78("a", "MERGE")),
      Seq("n")
    ),

    // Positive tests
    TestQuery(
      """CREATE (a)
        |CREATE (a)-[:REL]->(b)""".stripMargin,
      Passes,
      Seq.empty
    ),
    TestQuery(
      """CREATE (a {p: 1})-[:REL]->(a)""".stripMargin,
      Passes,
      Seq.empty
    ),
    TestQuery(
      """MATCH (a)
        |CREATE (a)-[:REL]->(b)""".stripMargin,
      Passes,
      Seq.empty
    ),
    TestQuery(
      """MATCH (n)
        |WITH COLLECT(n) AS ns
        |WITH ns, head(ns) AS a
        |FOREACH (a IN ns | // <- shadows the WITH a
        |  CREATE (a)-[:R]->(n)
        |)
        |RETURN a, ns""".stripMargin,
      Passes,
      Seq("a", "ns")
    ),
    TestQuery(
      """MATCH (n)
        |WITH COLLECT(n) AS ns
        |WITH ns, head(ns) AS a
        |FOREACH (a IN ns | // <- shadows the WITH a
        |  MERGE (a)-[:R]->(n)
        |)
        |RETURN a, ns""".stripMargin,
      Passes,
      Seq("a", "ns")
    )
  )
}
