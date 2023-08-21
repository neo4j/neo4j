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
package org.neo4j.cypher.internal.rewriting.rewriters

import org.neo4j.cypher.internal.rewriting.RewriteTest
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.test_helpers.TestName

class FixedLengthShortestToAllRewriterTest extends CypherFunSuite with RewriteTest with TestName {
  override def rewriterUnderTest: Rewriter = FixedLengthShortestToAllRewriter.instance

  test("MATCH ANY SHORTEST (a) RETURN count(*)") {
    assertRewrite(testName, "MATCH ALL (a) RETURN count(*)")
  }

  test("MATCH ANY SHORTEST (a:A) RETURN count(*)") {
    assertRewrite(testName, "MATCH ALL (a:A) RETURN count(*)")
  }

  test("MATCH ANY SHORTEST (a WHERE a.prop > 0) RETURN count(*)") {
    assertRewrite(testName, "MATCH ALL (a WHERE a.prop > 0) RETURN count(*)")
  }

  test("MATCH ANY SHORTEST (a)-->() RETURN count(*)") {
    assertIsNotRewritten(testName)
  }

  test("MATCH ALL SHORTEST (a)-[r]->(b) RETURN count(*)") {
    assertRewrite(testName, "MATCH ALL (a)-[r]->(b) RETURN count(*)")
  }

  test("MATCH SHORTEST 1 GROUPS (a)-[r]->(b) RETURN count(*)") {
    assertRewrite(testName, "MATCH ALL (a)-[r]->(b) RETURN count(*)")
  }

  test("MATCH SHORTEST 10 GROUPS (a)-[r]->(b) RETURN count(*)") {
    assertRewrite(testName, "MATCH ALL (a)-[r]->(b) RETURN count(*)")
  }

  test("MATCH ALL SHORTEST (a) RETURN count(*)") {
    assertRewrite(testName, "MATCH ALL (a) RETURN count(*)")
  }

  test("MATCH ALL SHORTEST (a)-[r]->(b)-[r1]->(c) RETURN count(*)") {
    assertRewrite(testName, "MATCH ALL (a)-[r]->(b)-[r1]->(c) RETURN count(*)")
  }

  test("MATCH ALL SHORTEST ((a)--()) RETURN count(*)") {
    assertRewrite(testName, "MATCH ALL ((a)--()) RETURN count(*)")
  }

  test("MATCH ALL SHORTEST ((a)--()){2} RETURN count(*)") {
    assertRewrite(testName, "MATCH ALL ((a)--()){2} RETURN count(*)")
  }

  test("MATCH ALL SHORTEST ((a)--()){2,2} RETURN count(*)") {
    assertRewrite(testName, "MATCH ALL ((a)--()){2,2} RETURN count(*)")
  }

  test("MATCH p = ALL SHORTEST (a)-[r]->(b)-[r1]->(c) (()--()--()){5} (foo) RETURN count(*)") {
    assertRewrite(testName, "MATCH p = ALL (a)-[r]->(b)-[r1]->(c) (()--()--()){5} (foo) RETURN count(*)")
  }

  // Forbidden by SemanticAnalysis anyway
  test("MATCH p = ALL SHORTEST shortestPath((a)-[*]-(b)) RETURN count(*)") {
    assertIsNotRewritten(testName)
  }

  // Forbidden by SemanticAnalysis anyway
  test("MATCH p = ALL SHORTEST allShortestPaths((a)-[*]-(b)) RETURN count(*)") {
    assertIsNotRewritten(testName)
  }

  // Forbidden by SemanticAnalysis anyway
  test("MATCH ALL SHORTEST (a)--(b)-[*]->(c) RETURN count(*)") {
    assertIsNotRewritten(testName)
  }

  test("MATCH ALL SHORTEST (a)--(b)-->*(c) RETURN count(*)") {
    assertIsNotRewritten(testName)
  }

  test("MATCH ALL SHORTEST (a)--(b)-->+(c) RETURN count(*)") {
    assertIsNotRewritten(testName)
  }

  test("MATCH ALL SHORTEST (a)--(b)-->{1,2}(c) RETURN count(*)") {
    assertIsNotRewritten(testName)
  }
}
