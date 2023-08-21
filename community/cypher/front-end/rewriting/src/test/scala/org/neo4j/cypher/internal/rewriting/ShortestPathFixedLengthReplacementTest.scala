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
package org.neo4j.cypher.internal.rewriting

import org.neo4j.cypher.internal.rewriting.rewriters.rewriteShortestPathWithFixedLengthRel
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class ShortestPathFixedLengthReplacementTest extends CypherFunSuite with RewriteTest {

  test("MATCH shortestPath((src)-[r]->(dst)) RETURN *") {
    assertRewrite(
      "MATCH shortestPath((src)-[r]->(dst)) RETURN *",
      "MATCH shortestPath((src)-[r*1..1]->(dst)) RETURN *"
    )
  }

  test("MATCH allShortestPaths((src)-[r]->(dst)) RETURN *") {
    assertRewrite(
      "MATCH allShortestPaths((src)-[r]->(dst)) RETURN *",
      "MATCH allShortestPaths((src)-[r*1..1]->(dst)) RETURN *"
    )
  }

  test("MATCH (src), (dst) WITH allShortestPaths((src)-[r]->(dst)) AS p RETURN *") {
    assertRewrite(
      "MATCH (src), (dst) WITH allShortestPaths((src)-[r]->(dst)) AS p RETURN *",
      "MATCH (src), (dst) WITH allShortestPaths((src)-[r*1..1]->(dst)) AS p RETURN *"
    )
  }

  test("MATCH (src), (dst),p = shortestPath((src)-[r]->(dst)) RETURN src, dst, p") {
    assertRewrite(
      "MATCH (src), (dst), p = shortestPath((src)-[r]->(dst)) RETURN src, dst, p",
      "MATCH (src), (dst), p = shortestPath((src)-[r*1..1]->(dst)) RETURN src, dst, p"
    )
  }

  test("MATCH allShortestPaths((src)-[r*]->(dst)) RETURN *") {
    assertIsNotRewritten(
      "MATCH allShortestPaths((src)-[r*]->(dst)) RETURN *"
    )
  }

  test("MATCH (src), (dst), p = allShortestPaths((src)-[r*]->(dst)) RETURN *") {
    assertIsNotRewritten(
      "MATCH (src), (dst), p = allShortestPaths((src)-[r*]->(dst)) RETURN *"
    )
  }

  val rewriterUnderTest: Rewriter = rewriteShortestPathWithFixedLengthRel
}
