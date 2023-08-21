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

/**
 * Note: This test is intended to check the rewriter works. To see more detailed normalization tests
 * see CypherTypeNameTest.scala :)
 */
class cypherTypeNormalizationRewriterTest extends CypherFunSuite with RewriteTest {

  override val rewriterUnderTest: Rewriter = cypherTypeNormalizationRewriter.instance

  test("List encapsulation normalization") {
    assertRewrite(
      "RETURN 1 IS :: LIST<BOOL> | LIST<BOOL NOT NULL> AS result",
      "RETURN 1 IS :: LIST<BOOLEAN> AS result"
    )
  }

  test("NULL normalizations") {
    assertRewrite(
      "RETURN 1 IS :: BOOL NOT NULL | NULL | FLOAT AS result",
      "RETURN 1 IS :: BOOLEAN | FLOAT AS result"
    )
  }

  test("Ordered normalization") {
    assertRewrite(
      "RETURN 1 IS :: FLOAT | INTEGER | STRING | BOOLEAN AS result",
      "RETURN 1 IS :: BOOLEAN | STRING | INTEGER | FLOAT AS result"
    )
  }

  test("No changes made during normalization") {
    assertIsNotRewritten(
      "RETURN 1 IS :: BOOLEAN | STRING | INTEGER AS result"
    )
  }
}
