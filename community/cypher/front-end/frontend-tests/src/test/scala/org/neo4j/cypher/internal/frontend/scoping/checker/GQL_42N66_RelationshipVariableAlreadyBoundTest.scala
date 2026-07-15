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

import org.neo4j.cypher.internal.frontend.scoping.E42N66
import org.neo4j.cypher.internal.frontend.scoping.Passes
import org.neo4j.cypher.internal.frontend.scoping.Versioned.ignoreBeforeCypher25

/**
 * Test for 42N66 - Relationship Variable Already Bound
 */
class GQL_42N66_RelationshipVariableAlreadyBoundTest extends VariableCheckingWithLocalCallablesTestSuite {
  VariableCheckingWithLocalCallablesTestSuite.register(() => testCases())

  override def testCases(): Seq[TestQuery] = (
    for {
      shortestPathFunc <- Seq("shortestPath", "allShortestPaths")
    } yield {
      Seq(
        // Negative tests
        TestQuery(
          s"""WITH [] AS r LIMIT 1
             |MATCH p = $shortestPathFunc((src)-[r*]->(dst))
             |RETURN src, dst""".stripMargin,
          E42N66,
          Seq("src", "dst")
        ),
        TestQuery(
          s"""WITH [] AS r LIMIT 1
             |CALL (r) {
             |  MATCH p = $shortestPathFunc((src)-[r*]->(dst))
             |  RETURN src, dst
             |}
             |RETURN src, dst""".stripMargin,
          E42N66,
          Seq("src", "dst")
        ),
        TestQuery(
          s"""LET r = []
             |RETURN r
             |
             |NEXT
             |
             |MATCH p = $shortestPathFunc((src)-[r*]->(dst))
             |RETURN src, dst""".stripMargin,
          ignoreBeforeCypher25(E42N66),
          Seq("src", "dst")
        ),
        TestQuery(
          s"""DEFINE PROCEDURE foo(r) {
             |  MATCH p = $shortestPathFunc((src)-[r*]->(dst))
             |  RETURN src, dst
             |}
             |CALL foo([])
             |RETURN src, dst""".stripMargin,
          ignoreBeforeCypher25(E42N66),
          Seq("src", "dst")
        ),
        TestQuery(
          s"""DEFINE FUNCTION foo(r) = COLLECT {
             |  MATCH p = $shortestPathFunc((src)-[r*]->(dst))
             |  RETURN {src: src, dst: dst}
             |}
             |UNWIND foo([]) AS p
             |RETURN p.src AS src, p.dst AS dst""".stripMargin,
          ignoreBeforeCypher25(E42N66),
          Seq("src", "dst")
        ),

        // Positive tests
        TestQuery(
          s"""WITH [] AS r LIMIT 1
             |MATCH p = $shortestPathFunc((src)-[s*]->(dst))
             |RETURN src, dst""".stripMargin,
          Passes,
          Seq("src", "dst")
        ),
        TestQuery(
          s"""WITH [] AS r LIMIT 1
             |CALL () {
             |  MATCH p = $shortestPathFunc((src)-[r*]->(dst))
             |  RETURN src, dst
             |}
             |RETURN src, dst""".stripMargin,
          Passes,
          Seq("src", "dst")
        ),
        TestQuery(
          s"""WITH [] AS r LIMIT 1
             |CALL (r) {
             |  MATCH p = $shortestPathFunc((src)-[s*]->(dst))
             |  RETURN src, dst
             |}
             |RETURN src, dst""".stripMargin,
          Passes,
          Seq("src", "dst")
        ),
        TestQuery(
          s"""LET r = []
             |RETURN r AS s
             |
             |NEXT
             |
             |MATCH p = $shortestPathFunc((src)-[r*]->(dst))
             |RETURN src, dst""".stripMargin,
          ignoreBeforeCypher25(Passes),
          Seq("src", "dst")
        ),
        TestQuery(
          s"""LET r = []
             |RETURN r
             |
             |NEXT
             |
             |MATCH p = $shortestPathFunc((src)-[s*]->(dst))
             |RETURN src, dst""".stripMargin,
          ignoreBeforeCypher25(Passes),
          Seq("src", "dst")
        ),
        TestQuery(
          s"""DEFINE PROCEDURE foo(r) {
             |  MATCH p = $shortestPathFunc((src)-[s*]->(dst))
             |  RETURN src, dst
             |}
             |CALL foo([])
             |RETURN src, dst""".stripMargin,
          ignoreBeforeCypher25(Passes),
          Seq("src", "dst")
        ),
        TestQuery(
          s"""DEFINE FUNCTION foo(r) = COLLECT {
             |  MATCH p = $shortestPathFunc((src)-[s*]->(dst))
             |  RETURN {src: src, dst: dst}
             |}
             |UNWIND foo([]) AS p
             |RETURN p.src AS src, p.dst AS dst""".stripMargin,
          ignoreBeforeCypher25(Passes),
          Seq("src", "dst")
        )
      )
    }
  ).flatten
}
