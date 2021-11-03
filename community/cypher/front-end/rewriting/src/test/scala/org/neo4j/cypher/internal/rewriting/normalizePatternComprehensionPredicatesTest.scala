/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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

import org.neo4j.cypher.internal.rewriting.rewriters.normalizePatternComprehensionPredicates
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class normalizePatternComprehensionPredicatesTest extends CypherFunSuite with RewriteTest {

  override def rewriterUnderTest: Rewriter = normalizePatternComprehensionPredicates

  test("move single predicate from node to WHERE") {
    assertRewrite(
      "RETURN [(a WHERE a.prop > 123)-->(b) | a] AS result",
      "RETURN [(a)-->(b) WHERE a.prop > 123 | a] AS result"
    )
  }

  test("move multiple predicates from node to WHERE") {
    assertRewrite(
      "RETURN [(a WHERE a.prop > 123)-->(b WHERE b.prop < 42) | a] AS result",
      "RETURN [(a)-->(b) WHERE a.prop > 123 AND b.prop < 42 | a] AS result"
    )
  }

  test("add multiple predicate from node to WHERE predicate") {
    assertRewrite(
      "RETURN [(a WHERE a.prop < 123)-->(b WHERE b.prop > 42) WHERE a.prop <> b.prop | a] AS result",
      "RETURN [(a)-->(b) WHERE (a.prop < 123 AND b.prop > 42) AND a.prop <> b.prop | a] AS result"
    )
  }

  test("move single relationship pattern predicate from relationship to WHERE") {
    assertRewrite(
      "RETURN [(a)-[r WHERE r.prop > 123]->(b) | r] AS result",
      "RETURN [(a)-[r]->(b) WHERE r.prop > 123 | r] AS result"
    )
  }

  test("move relationship pattern predicates from multiple relationships to WHERE") {
    assertRewrite(
      "RETURN [(a)-[r WHERE r.prop > 123]->()<-[s WHERE s.otherProp = \"ok\"]-(b) | r] AS result",
      "RETURN [(a)-[r]->()<-[s]-(b) WHERE r.prop > 123 AND s.otherProp = \"ok\" | r] AS result"
    )
  }
}
