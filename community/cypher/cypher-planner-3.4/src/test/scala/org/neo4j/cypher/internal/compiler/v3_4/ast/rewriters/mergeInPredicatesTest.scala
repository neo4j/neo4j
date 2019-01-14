/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.v3_4.ast.rewriters

import org.neo4j.cypher.internal.compiler.v3_4.planner.AstRewritingTestSupport
import org.neo4j.cypher.internal.compiler.v3_4.test_helpers.ContextHelper
import org.neo4j.cypher.internal.frontend.v3_4.ast.Query
import org.neo4j.cypher.internal.frontend.v3_4.ast.rewriters.{CNFNormalizer, mergeInPredicates}
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite

class mergeInPredicatesTest extends CypherFunSuite with AstRewritingTestSupport {

  test("MATCH (a) WHERE a.prop IN [1,2,3] AND a.prop IN [2,3,4] RETURN *") {
    shouldRewrite(
      "MATCH (a) WHERE a.prop IN [1,2,3] AND a.prop IN [2,3,4] RETURN *",
      "MATCH (a) WHERE a.prop IN [2,3] RETURN *")
  }

  test("MATCH (a) WHERE a.prop IN [1,2,3] AND a.prop IN [4,5,6] RETURN *") {
    shouldRewrite(
      "MATCH (a) WHERE a.prop IN [1,2,3] AND a.prop IN [4,5,6] RETURN *",
      "MATCH (a) WHERE false RETURN *")
  }

  test("MATCH (a) WHERE a.prop IN [1,2,3] AND a.prop IN [2,3,4] AND a.prop IN [3,4,5] RETURN *") {
    shouldRewrite(
      "MATCH (a) WHERE a.prop IN [1,2,3] AND a.prop IN [2,3,4] AND a.prop IN [3,4,5] RETURN *",
      "MATCH (a) WHERE a.prop IN [3] RETURN *")
  }

  test("MATCH (a) WHERE (a.prop IN [1,2,3] AND a.prop IN [2,3,4]) OR (a.prop IN [2,3,4] AND a.prop IN [3,4,5]) RETURN *") {
    shouldRewrite(
      "MATCH (a) WHERE (a.prop IN [1,2,3] AND a.prop IN [2,3,4]) OR (a.prop IN [2,3,4] AND a.prop IN [3,4,5]) RETURN *",
      "MATCH (a) WHERE a.prop IN [2,3,4] RETURN *")
  }

  test("MATCH (a) WHERE a.prop IN [1,2,3] AND a.foo IN ['foo', 'bar'] AND a.prop IN [2,3,4] AND a.foo IN ['bar'] RETURN *") {
    shouldRewrite(
      "MATCH (a) WHERE a.prop IN [1,2,3] AND a.foo IN ['foo','bar'] AND a.prop IN [2,3,4] AND a.foo IN ['bar'] RETURN *",
      "MATCH (a) WHERE a.prop IN [2,3] AND a.foo IN ['bar'] RETURN *")
  }

  test("MATCH (a) WHERE a.prop IN [1,2,3] OR a.prop IN [2,3,4] RETURN *") {
    shouldRewrite(
      "MATCH (a) WHERE a.prop IN [1,2,3] OR a.prop IN [2,3,4] RETURN *",
      "MATCH (a) WHERE a.prop IN [1,2,3,4] RETURN *")
  }

  test("MATCH (a) WHERE a.prop IN [1,2,3] OR a.prop IN [2,3,4] OR a.prop IN [3,4,5] RETURN *") {
    shouldRewrite(
      "MATCH (a) WHERE a.prop IN [1,2,3] OR a.prop IN [2,3,4] OR a.prop IN [3,4,5] RETURN *",
      "MATCH (a) WHERE a.prop IN [1,2,3,4,5] RETURN *")
  }

  test("MATCH (a) WHERE (a.prop IN [1,2,3] OR a.prop IN [2,3,4]) AND (a.prop IN [2,3,4] OR a.prop IN [3,4,5]) RETURN *") {
    shouldRewrite(
      "MATCH (a) WHERE (a.prop IN [1,2,3] OR a.prop IN [2,3,4]) AND (a.prop IN [2,3,4] OR a.prop IN [3,4,5]) RETURN *",
      "MATCH (a) WHERE a.prop IN [2,3,4] RETURN *")
  }

  test("MATCH (a) WHERE a.prop IN [1,2,3] OR a.foo IN ['foo', 'bar'] OR a.prop IN [2,3,4] OR a.foo IN ['bar'] RETURN *") {
    shouldRewrite(
      "MATCH (a) WHERE a.prop IN [1,2,3] OR a.foo IN ['foo','bar'] OR a.prop IN [2,3,4] OR a.foo IN ['bar'] RETURN *",
      "MATCH (a) WHERE a.prop IN [1,2,3,4] OR a.foo IN ['foo','bar'] RETURN *")
  }

  test("MATCH (n) RETURN n.prop IN [1,2,3] AND n.prop IN [3,4,5]") {
    shouldRewrite("MATCH (n) RETURN n.prop IN [1,2,3] AND n.prop IN [3,4,5] AS FOO",
                  "MATCH (n) RETURN n.prop IN [3] AS FOO")
  }

  test("MATCH (n) RETURN (n.prop IN [1,2,3] OR TRUE) AND n.prop IN [3,4,5] AS FOO") {
    shouldNotRewrite("MATCH (n) RETURN (n.prop IN [1,2,3] OR TRUE) AND n.prop IN [3,4,5] AS FOO")
  }

  test("MATCH (n) RETURN (n.prop IN [1,2,3] AND FALSE) OR n.prop IN [3,4,5] AS FOO") {
    shouldNotRewrite("MATCH (n) RETURN (n.prop IN [1,2,3] AND FALSE) OR n.prop IN [3,4,5] AS FOO")
  }

  // Tests for IN combined with NOT

  test( "MATCH (a) WHERE NOT a.prop IN [1, 2, 3] AND NOT a.prop IN [3, 4, 5] RETURN *") {
    shouldRewrite("MATCH (a) WHERE NOT a.prop IN [1, 2, 3] AND NOT a.prop IN [3, 4, 5] RETURN *",
      "MATCH (a) WHERE NOT a.prop IN [1, 2, 3, 4, 5] RETURN *")
  }

  test( "MATCH (a) WHERE NOT a.prop IN [1, 2, 3] OR NOT a.prop IN [3, 4, 5] RETURN *") {
    shouldRewrite("MATCH (a) WHERE NOT a.prop IN [1, 2, 3] OR NOT a.prop IN [3, 4, 5] RETURN *",
      "MATCH (a) WHERE NOT a.prop IN [3] RETURN *")
  }

  test( "MATCH (a) WHERE a.prop IN [1, 2, 3] AND NOT a.prop IN [3, 4, 5]") {
    shouldNotRewrite("MATCH (a) WHERE a.prop IN [1, 2, 3] AND NOT a.prop IN [3, 4, 5]")
  }

  test( "MATCH (a) WHERE NOT a.prop IN [1, 2, 3] OR a.prop IN [3, 4, 5]") {
    shouldNotRewrite("MATCH (a) WHERE a.prop IN [1, 2, 3] OR NOT a.prop IN [3, 4, 5]")
  }

  test("MATCH (a) WHERE a.prop IN [1,2,3] AND a.prop IN [2,3,4] AND NOT a.prop IN [3,4,5] RETURN *") {
    shouldRewrite("MATCH (a) WHERE a.prop IN [1,2,3] AND a.prop IN [2,3,4] AND NOT a.prop IN [3,4,5] RETURN *",
      "MATCH (a) WHERE a.prop IN [2,3] AND NOT a.prop IN [3,4,5] RETURN *"
    )
  }

  // would have been nice to rewrite the order of predicates to allow merging in predicates
  test("MATCH (a) WHERE a.prop IN [1,2,3] AND NOT a.prop IN [3,4,5] AND a.prop IN [2,3,4] RETURN *") {
    shouldNotRewrite("MATCH (a) WHERE a.prop IN [1,2,3] AND NOT a.prop IN [3,4,5] AND a.prop IN [2,3,4] RETURN *")
  }

  test("MATCH (a) WHERE NOT (a.prop IN [1,2] AND a.prop IN [2,3])") {
    shouldRewrite("MATCH (a) WHERE NOT (a.prop IN [1,2] AND a.prop IN [2,3])",
      "MATCH (a) WHERE NOT a.prop IN [2]"
    )
  }

  test("MATCH (a) WHERE NOT (a.prop IN [1,2] AND a.prop IN [2,3]) AND NOT (a.prop IN [3,4] AND a.prop IN [4,5])") {
    shouldRewrite("MATCH (a) WHERE NOT (a.prop IN [1,2] AND a.prop IN [2,3]) AND NOT (a.prop IN [3,4] AND a.prop IN [4,5])",
      "MATCH (a) WHERE NOT a.prop IN [2,4]"
    )
  }


  private def shouldRewrite(from: String, to: String) {
    val original = parser.parse(from).asInstanceOf[Query]
    val expected = parser.parse(to).asInstanceOf[Query]
    val common = CNFNormalizer.instance(ContextHelper.create())
    val result = mergeInPredicates(original)

    common(result) should equal(common(expected))
  }

  private def shouldNotRewrite(query: String) = shouldRewrite(query, query)
}
