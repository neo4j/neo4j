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
package org.neo4j.cypher.internal.compiler.v3_2.ast.rewriters

import org.neo4j.cypher.internal.compiler.v3_2.phases.CompilationState
import org.neo4j.cypher.internal.compiler.v3_2.planner.AstRewritingTestSupport
import org.neo4j.cypher.internal.compiler.v3_2.test_helpers.ContextHelper
import org.neo4j.cypher.internal.frontend.v3_2.ast.rewriters.{CNFNormalizer, startClauseRewriter}
import org.neo4j.cypher.internal.frontend.v3_2.ast.{Query, SignedDecimalIntegerLiteral, UnsignedDecimalIntegerLiteral}
import org.neo4j.cypher.internal.frontend.v3_2.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.frontend.v3_2.{Rewriter, topDown}

class StartClauseRewriterTest extends CypherFunSuite with AstRewritingTestSupport {

  test("START n=node(0)") {
    shouldRewrite(
      "START n=node(0)",
      "MATCH (n) WHERE id(n) IN [0]")
  }

  test("START n=node(0,1)") {
    shouldRewrite(
      "START n=node(0,1)",
      "MATCH (n) WHERE id(n) IN [0,1]")
  }

  test("START n=node(0),m=node(1)") {
    shouldRewrite(
      "START n=node(0),m=node(1)",
      "MATCH (n),(m) WHERE id(n) IN [0] AND id(m) IN [1]")
  }

  test("START n=node(*)") {
    shouldRewrite(
      "START n=node(*)",
      "MATCH (n)")
  }

  test("START r=rel(0)") {
    shouldRewrite(
      "START r=rel(0)",
      "MATCH (`  UNNAMED6`)-[r]->(`  UNNAMED7`) WHERE id(r) IN [0]")
  }

  test("START r=rel(0,1)") {
    shouldRewrite(
      "START r=rel(0,1)",
      "MATCH (`  UNNAMED6`)-[r]->(`  UNNAMED7`) WHERE id(r) IN [0,1]")
  }

  test("START r=rel(0),rr=rel(1)") {
    shouldRewrite(
      "START r=rel(0),rr=rel(1)",
      "MATCH (`  UNNAMED6`)-[r]->(`  UNNAMED7`),(`  UNNAMED15`)-[rr]->(`  UNNAMED16`) WHERE id(r) IN [0] AND id(rr) IN [1]")
  }

  test("START r=rel(*)") {
    shouldRewrite(
      "START r=rel(*)",
      "MATCH (`  UNNAMED6`)-[r]->(`  UNNAMED7`)")
  }

  test("START n=node({id})") {
    shouldRewrite("START n=node({id})",
                  "MATCH (n) WHERE id(n) IN {id}")
  }

  private def shouldRewrite(from: String, to: String) {
    //START uses UnsignedDecimalIntegerLiteral whereas IN uses signed
    //this is not important for this test so just ignore by rewriting
    val original = parser.parse(from).asInstanceOf[Query].endoRewrite(topDown(Rewriter.lift {
        case u@UnsignedDecimalIntegerLiteral(v) => SignedDecimalIntegerLiteral(v)(u.position)
      }))
    val expected = parser.parse(to).asInstanceOf[Query].endoRewrite(CNFNormalizer.instance(ContextHelper.create()))

    val input = CompilationState(null, null, null, Some(original))
    val result = startClauseRewriter.transform(input, ContextHelper.create())

    result.statement should equal(expected)
  }

  private def shouldNotRewrite(q: String) {
    shouldRewrite(q, q)
  }
}
