/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

  test("MATCH (a) WHERE a.prop IN [1,2,3] OR a.prop IN [2,3,4] RETURN *") {
    shouldNotRewrite("MATCH (a) WHERE a.prop IN [1,2,3] OR a.prop IN [2,3,4] RETURN *")
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
      "MATCH (a) WHERE a.prop IN [2,3] OR a.prop IN [3,4] RETURN *")
  }

  test("MATCH (a) WHERE a.prop IN [1,2,3] AND a.foo IN ['foo', 'bar'] AND a.prop IN [2,3,4] AND a.foo IN ['bar'] RETURN *") {
    shouldRewrite(
      "MATCH (a) WHERE a.prop IN [1,2,3] AND a.foo IN ['foo','bar'] AND a.prop IN [2,3,4] AND a.foo IN ['bar'] RETURN *",
      "MATCH (a) WHERE a.prop IN [2,3] AND a.foo IN ['bar'] RETURN *")
  }

  private def shouldRewrite(from: String, to: String) {
    val original = parser.parse(from).asInstanceOf[Query]
    val expected = parser.parse(to).asInstanceOf[Query]
    val common = CNFNormalizer.instance(ContextHelper.create())
    val result = mergeInPredicates(original)

    common(result) should equal(common(expected))
  }

  private def shouldNotRewrite(q: String) {
    shouldRewrite(q, q)
  }
}
