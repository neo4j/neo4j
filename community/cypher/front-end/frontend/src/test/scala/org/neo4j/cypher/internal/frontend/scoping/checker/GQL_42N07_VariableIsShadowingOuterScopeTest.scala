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

import org.neo4j.cypher.internal.frontend.scoping.E42N07
import org.neo4j.cypher.internal.frontend.scoping.E42N62
import org.neo4j.cypher.internal.frontend.scoping.Exactly
import org.neo4j.cypher.internal.frontend.scoping.Passes
import org.neo4j.cypher.internal.frontend.scoping.Versioned.ignoreBeforeCypher25

/**
 * Test for 42N07 - Variable Is Shadowing Outer Scope
 */
class GQL_42N07_VariableIsShadowingOuterScopeTest extends VariableCheckingWithLocalCallablesTestSuite {
  VariableCheckingWithLocalCallablesTestSuite.register(() => testCases())

  override def testCases(): Seq[TestQuery] = Seq(
    // Negative tests
    TestQuery(
      """LET a = 1
        |CALL (a) {
        |  RETURN x + 1 AS a
        |  UNION
        |  RETURN a + 2 AS a
        |}""".stripMargin,
      ignoreBeforeCypher25(Exactly(E42N07("a"), E42N62("x"))),
      Seq.empty
    ),
    TestQuery(
      """LET a = 1
        |CALL (a) {
        |  RETURN a + 1 AS a
        |  UNION
        |  RETURN x + 2 AS a
        |}""".stripMargin,
      ignoreBeforeCypher25(Exactly(E42N07("a"), E42N62("x"))),
      Seq.empty
    ),
    TestQuery(
      """MATCH (a)
        |CALL (a) {
        |  MATCH (b)
        |  RETURN b AS a
        |  UNION
        |  MATCH (a)
        |  RETURN a
        |}
        |RETURN *""".stripMargin,
      E42N07("a"),
      Seq("a")
    ),
    TestQuery(
      """MATCH (a)
        |CALL (*) {
        |  MATCH (b)
        |  RETURN b AS a
        |  UNION
        |  MATCH (a)
        |  RETURN a
        |}
        |RETURN *""".stripMargin,
      E42N07("a"),
      Seq("a")
    ),
    TestQuery(
      """LET a = 10
        |CALL (a) {
        |  LET a = a
        |  RETURN a AS y
        |}
        |RETURN *""".stripMargin,
      ignoreBeforeCypher25(E42N07("a")),
      Seq("a", "y")
    ),
    TestQuery(
      """LET a = 10
        |CALL (a) {
        |  LET a = a + 0
        |  RETURN a AS y
        |}
        |RETURN *""".stripMargin,
      ignoreBeforeCypher25(E42N07("a")),
      Seq("a", "y")
    ),
    TestQuery(
      """LET a = 10
        |CALL (a) {
        |  LET b = a
        |  RETURN b AS a
        |}
        |RETURN *""".stripMargin,
      ignoreBeforeCypher25(E42N07("a")),
      Seq("a")
    ),
    TestQuery(
      """LET a = 10
        |CALL {
        |  WITH a
        |  LET b = a
        |  RETURN b AS a
        |}
        |RETURN *""".stripMargin,
      ignoreBeforeCypher25(E42N07("a")),
      Seq("a")
    ),
    TestQuery(
      """LET a = 10
        |CALL () {
        |  RETURN 20 AS a
        |}
        |RETURN *""".stripMargin,
      ignoreBeforeCypher25(E42N07("a")),
      Seq("a")
    ),
    TestQuery(
      """LET a = 10
        |CALL {
        |  RETURN 20 AS a
        |}
        |RETURN *""".stripMargin,
      ignoreBeforeCypher25(E42N07("a")),
      Seq("a")
    ),
    TestQuery(
      """WITH 1 AS x
        |CALL {
        |  USE mega.graph1
        |  WITH 2 AS x
        |  RETURN *
        |}
        |RETURN x""".stripMargin,
      E42N07("x"),
      Seq("x")
    ),
    TestQuery(
      """WITH 1 AS x
        |CALL () {
        |  USE mega.graph1
        |  WITH 2 AS x
        |  RETURN *
        |}
        |RETURN x""".stripMargin,
      E42N07("x"),
      Seq("x")
    ),
    TestQuery(
      """MATCH (a)
        |RETURN EXISTS {
        |  MATCH (b)
        |  LET a = b
        |  RETURN a
        |} AS a""".stripMargin,
      ignoreBeforeCypher25(E42N07("a")),
      Seq("a")
    ),
    TestQuery(
      """MATCH (a)
        |RETURN EXISTS {
        |  WITH 1 AS a
        |  RETURN a AS a
        |  NEXT
        |  RETURN a
        |} AS a""".stripMargin,
      ignoreBeforeCypher25(E42N07("a")),
      Seq("a")
    ),
    TestQuery(
      """MATCH (a)
        |RETURN EXISTS {
        |  MATCH (a)
        |  RETURN a
        |  NEXT
        |  RETURN a
        |} AS a""".stripMargin,
      ignoreBeforeCypher25(E42N07("a")),
      Seq("a")
    ),
    TestQuery(
      """MATCH (a)
        |RETURN EXISTS {
        |  MATCH (b)
        |  RETURN b
        |  NEXT
        |  MATCH (b)
        |  RETURN b AS a
        |} AS a""".stripMargin,
      ignoreBeforeCypher25(E42N07("a")),
      Seq("a")
    ),
    TestQuery(
      """MATCH (a)
        |RETURN EXISTS {
        |  MATCH (a)
        |  RETURN a
        |  NEXT
        |  MATCH (b)
        |  RETURN b AS a
        |  NEXT
        |  MATCH (a)
        |  RETURN a
        |} AS a""".stripMargin,
      ignoreBeforeCypher25(E42N07("a")),
      Seq("a")
    ),
    TestQuery(
      """MATCH (a)
        |RETURN EXISTS {
        |  MATCH (a)
        |  RETURN a
        |  NEXT
        |  MATCH (b)
        |  RETURN a AS a
        |  NEXT
        |  MATCH (a)
        |  RETURN a
        |} AS a""".stripMargin,
      ignoreBeforeCypher25(E42N07("a")),
      Seq("a")
    ),
    TestQuery(
      """WITH 7 AS x
        |CALL (x) {
        |  RETURN "hello" AS x
        |  UNION
        |  RETURN x
        |
        |  NEXT
        |
        |  RETURN x + 2 AS y
        |}
        |RETURN x, y""".stripMargin,
      ignoreBeforeCypher25(E42N07("x")),
      Seq("x", "y")
    ),
    TestQuery(
      """RETURN 7 AS x
        |
        |NEXT
        |
        |WHEN true THEN  {
        |  {
        |    RETURN x + 4 AS x
        |
        |    NEXT
        |
        |    RETURN x + 5 AS x
        |  }
        |  UNION
        |  RETURN 2 AS x
        |}""".stripMargin,
      ignoreBeforeCypher25(E42N07("x")),
      Seq("x")
    ),
    TestQuery(
      """UNWIND [1,2,3] AS x
        |RETURN COLLECT {
        |  UNWIND [1,2] AS y
        |  RETURN x, y
        |  NEXT
        |  RETURN x + COUNT(y)
        |} AS x""".stripMargin,
      ignoreBeforeCypher25(E42N07("x")),
      Seq("x")
    ),
    TestQuery(
      """UNWIND [1,2,3] AS x
        |RETURN COLLECT {
        |  UNWIND [1,2] AS y
        |  RETURN x, y
        |  NEXT
        |  RETURN x + y
        |} AS x""".stripMargin,
      ignoreBeforeCypher25(E42N07("x")),
      Seq("x")
    ),
    TestQuery(
      """MATCH (person:Person)
        |CALL (person) {
        |  MATCH (person)-[:HAS_DOG]->(dog:Dog)
        |  RETURN dog.name AS petName
        |}
        |RETURN count(*) AS outerCnt,
        |       COUNT {
        |         MATCH (person)-[:HAS_DOG]->(dog:Dog)
        |         RETURN dog.name AS petName
        |       } AS numPets""".stripMargin,
      E42N07("petName"),
      Seq("outerCnt", "numPets")
    ),
    TestQuery(
      """MATCH (person:Person)
        |CALL (person) {
        |  MATCH (person)-[:HAS_DOG]->(dog:Dog)
        |  RETURN dog.name AS petName
        |}
        |RETURN count(*) AS outerCnt,
        |       COLLECT {
        |         MATCH (person)-[:HAS_DOG]->(dog:Dog)
        |         RETURN dog.name AS petName
        |       } AS numPets""".stripMargin,
      E42N07("petName"),
      Seq("outerCnt", "numPets")
    ),
    TestQuery(
      """MATCH (person:Person)
        |CALL (person) {
        |  MATCH (person)-[:HAS_DOG]->(dog:Dog)
        |  RETURN dog.name AS petName
        |}
        |RETURN count(*) AS outerCnt,
        |       EXISTS {
        |         MATCH (person)-[:HAS_DOG]->(dog:Dog)
        |         RETURN dog.name AS petName
        |       } AS hasPet""".stripMargin,
      E42N07("petName"),
      Seq("outerCnt", "hasPet")
    ),
    TestQuery(
      """MATCH (person:Person)
        |CALL (person) {
        |  MATCH (person)-[:HAS_DOG]->(dog:Dog)
        |  RETURN dog.name AS petName
        |}
        |RETURN person.name AS name,
        |       COUNT {
        |         MATCH (person)-[:HAS_DOG]->(dog:Dog)
        |         RETURN dog.name AS petName
        |       } AS numPets""".stripMargin,
      E42N07("petName"),
      Seq("name", "numPets")
    ),
    TestQuery(
      """MATCH (person:Person)
        |CALL (person) {
        |  MATCH (person)-[:HAS_DOG]->(dog:Dog)
        |  RETURN dog.name AS petName
        |}
        |RETURN count(*) AS outerCnt,
        |       COUNT {
        |         MATCH (person)-[:HAS_DOG]->(dog:Dog)
        |         RETURN dog.name AS subPet
        |       } AS numPets""".stripMargin,
      Passes,
      Seq("outerCnt", "numPets")
    ),

    // Positive tests
    TestQuery(
      """WITH "%s" AS g
        |CALL (g) {
        |  USE graph.byName(g)
        |  WITH 2 AS x
        |  RETURN *
        |}
        |RETURN x""".stripMargin,
      Passes,
      Seq("x")
    ),
    TestQuery(
      """WITH "%s" AS g
        |CALL {
        |  WITH g
        |  USE graph.byName(g)
        |  RETURN 2 AS x
        |}
        |RETURN x""".stripMargin,
      Passes,
      Seq("x")
    ),
    TestQuery(
      """RETURN 1 AS a
        |
        |NEXT
        |{
        |  RETURN a + 4 AS a
        |  UNION
        |  RETURN a + 2 AS a
        |}""".stripMargin,
      ignoreBeforeCypher25(Passes),
      Seq("a")
    ),
    TestQuery(
      """WITH true AS a
        |CALL (*) {
        |  RETURN 8 AS y
        |
        |  NEXT
        |
        |  WHEN EXISTS { RETURN 2 AS x } THEN RETURN 4 AS y
        |}
        |RETURN 1 AS x""".stripMargin,
      ignoreBeforeCypher25(Passes),
      Seq("x")
    ),
    TestQuery(
      """MATCH (a)
        |RETURN COLLECT {
        |  MATCH (a)
        |  RETURN a
        |  UNION
        |  MATCH (a)
        |  RETURN a
        |} AS a""".stripMargin,
      Passes,
      Seq("a")
    ),
    TestQuery(
      """MATCH (a)
        |RETURN EXISTS {
        |  MATCH (a)
        |  RETURN a AS b
        |  NEXT
        |  RETURN a
        |} AS a""".stripMargin,
      ignoreBeforeCypher25(Passes),
      Seq("a")
    ),
    TestQuery(
      """MATCH (a)
        |RETURN EXISTS {
        |  MATCH (a)
        |  RETURN a AS b
        |  NEXT
        |  MATCH (b)
        |  RETURN b AS c
        |  NEXT
        |  MATCH (c)
        |  RETURN a
        |} AS a""".stripMargin,
      ignoreBeforeCypher25(Passes),
      Seq("a")
    ),
    TestQuery(
      """MATCH p = ()-[]->()
        |WITH p, EXISTS {
        |    RETURN p
        |  } AS x
        |RETURN *""".stripMargin,
      ignoreBeforeCypher25(Passes),
      Seq("p", "x")
    ),
    TestQuery(
      """MATCH p = ()-[]->()
        |WITH p, COUNT {
        |    RETURN p
        |  } AS x
        |RETURN *""".stripMargin,
      ignoreBeforeCypher25(Passes),
      Seq("p", "x")
    ),
    TestQuery(
      """MATCH p = ()-[]->()
        |WITH p, COLLECT {
        |    RETURN p
        |  } AS x
        |RETURN *""".stripMargin,
      ignoreBeforeCypher25(Passes),
      Seq("p", "x")
    ),
    TestQuery(
      """MATCH p = ()-[]->()
        |WITH p, EXISTS {
        |    RETURN p AS x
        |  } AS x
        |RETURN *""".stripMargin,
      ignoreBeforeCypher25(Passes),
      Seq("p", "x")
    ),
    TestQuery(
      """MATCH p = ()-[]->()
        |WITH p, COUNT {
        |    RETURN p AS x
        |  } AS x
        |RETURN *""".stripMargin,
      ignoreBeforeCypher25(Passes),
      Seq("p", "x")
    ),
    TestQuery(
      """MATCH p = ()-[]->()
        |WITH p, COLLECT {
        |    RETURN p AS x
        |  } AS x
        |RETURN *""".stripMargin,
      ignoreBeforeCypher25(Passes),
      Seq("p", "x")
    )
  )
}
