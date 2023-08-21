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

class QuantifiedPathPatternNodeInsertRewriterTest extends CypherFunSuite with RewriteTest with TestName {

  override def rewriterUnderTest: Rewriter = QuantifiedPathPatternNodeInsertRewriter.instance

  test("MATCH ((a)-->(b))+ ((c)-->(d))+ RETURN count(*)") {
    assertRewrite(testName, "MATCH () ((a)-->(b))+ () ((c)-->(d))+ () RETURN count(*)")
  }

  test("MATCH ((a)-->(b))+ RETURN count(*)") {
    assertRewrite(testName, "MATCH () ((a)-->(b))+ () RETURN count(*)")
  }

  test("MATCH ((a)-->(b))+ (c) ((d)-->(e))+ RETURN count(*)") {
    assertRewrite(testName, "MATCH () ((a)-->(b))+ (c) ((d)-->(e))+ () RETURN count(*)")
  }

  // Forbidden in semantic analysis, but we can still rewrite it
  test("MATCH ((a)-->(b))+ (c) (d) ((e)-->(f))+ RETURN count(*)") {
    assertRewrite(testName, "MATCH () ((a)-->(b))+ (c) (d) ((e)-->(f))+ () RETURN count(*)")
  }

  test("MATCH ((b)-->(c))+ (a) RETURN count(*)") {
    assertRewrite(testName, "MATCH () ((b)-->(c))+ (a) RETURN count(*)")
  }

  test("MATCH (a) ((b)-->(c))+ RETURN count(*)") {
    assertRewrite(testName, "MATCH (a) ((b)-->(c))+ () RETURN count(*)")
  }

  test("MATCH ((a)-->(b)) ((c)-->(d))+ RETURN count(*)") {
    assertRewrite(testName, "MATCH ((a)-->(b)) ((c)-->(d))+ () RETURN count(*)")
  }

  test("MATCH (a)--() ((b)-->(c))+ RETURN count(*)") {
    assertRewrite(testName, "MATCH (a)--() ((b)-->(c))+ () RETURN count(*)")
  }

  test("MATCH () ((a)-->(b))+ () RETURN count(*)") {
    assertIsNotRewritten(testName)
  }

  test("MATCH (a) RETURN count(*)") {
    assertIsNotRewritten(testName)
  }

  // This is for COUNT expressions
  test("MATCH ((a)-->(b)) WHERE COUNT { ((c)-->(d))+ } RETURN count(*)") {
    assertRewrite(testName, "MATCH ((a)-->(b)) WHERE COUNT { () ((c)-->(d))+ () } RETURN count(*)")
  }

  test("MATCH ((a)-->(b)) WHERE COUNT { ((c)-->(d))+ () } RETURN count(*)") {
    assertRewrite(testName, "MATCH ((a)-->(b)) WHERE COUNT { () ((c)-->(d))+ () } RETURN count(*)")
  }

  test("MATCH ((a)-->(b)) WHERE COUNT { () ((c)-->(d))+ () } RETURN count(*)") {
    assertRewrite(testName, "MATCH ((a)-->(b)) WHERE COUNT { () ((c)-->(d))+ () } RETURN count(*)")
  }

  test("MATCH ((a)-->(b)) WHERE COUNT { ((c)-->(d)) } RETURN count(*)") {
    assertIsNotRewritten(testName)
  }
}
