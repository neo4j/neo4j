/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.v3_5.frontend.phases.rewriting

import org.neo4j.cypher.internal.v3_5.rewriting.rewriters.collapseMultipleInPredicates
import org.neo4j.cypher.internal.v3_5.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v3_5.frontend.phases.CNFNormalizer
import org.neo4j.cypher.internal.v3_5.rewriting.AstRewritingTestSupport

class CollapseInCollectionsTest extends CypherFunSuite with AstRewritingTestSupport {

  test("should collapse collection containing ConstValues for id function") {
    val original = parse("MATCH (a) WHERE id(a) IN [42] OR id(a) IN [13]")
    val expected = parse("MATCH (a) WHERE id(a) IN [42, 13]")

    val result = original.rewrite(collapseMultipleInPredicates)

    result should equal(expected)
  }

  test("should not collapse collections containing ConstValues and nonConstValues for id function") {
    val original = parse("MATCH (a) WHERE id(a) IN [42] OR id(a) IN [rand()]")
    val expected = parse("MATCH (a) WHERE id(a) IN [42, rand()]")

    val result = original.rewrite(collapseMultipleInPredicates)

    result should equal(expected)
  }

  test("should collapse collection containing ConstValues for property") {
    val original = parse("MATCH (a) WHERE a.prop IN [42] OR a.prop IN [13]")
    val expected = parse("MATCH (a) WHERE a.prop IN [42, 13]")

    val result = original.rewrite(collapseMultipleInPredicates)

    result should equal(expected)
  }

  test("should not collapse collections containing ConstValues and nonConstValues for property") {
    val original = parse("MATCH (a) WHERE a.prop IN [42] OR a.prop IN [rand()]")
    val expected = parse("MATCH (a) WHERE a.prop IN [42, rand()]")

    val result = original.rewrite(collapseMultipleInPredicates)

    result should equal(expected)
  }

  private def parse(query: String) = {
    val parsed = parser.parse(query)
    val rewriter = CNFNormalizer.instance(TestContext())
    parsed.endoRewrite(rewriter)
  }
}
