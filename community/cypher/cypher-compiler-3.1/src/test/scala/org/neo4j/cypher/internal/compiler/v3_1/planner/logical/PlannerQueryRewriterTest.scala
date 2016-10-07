/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_1.planner.logical

import org.neo4j.cypher.internal.compiler.v3_1.SyntaxExceptionCreator
import org.neo4j.cypher.internal.compiler.v3_1.ast.convert.plannerQuery.StatementConverters.toUnionQuery
import org.neo4j.cypher.internal.compiler.v3_1.planner._
import org.neo4j.cypher.internal.frontend.v3_1.Rewritable._
import org.neo4j.cypher.internal.frontend.v3_1.ast.Query
import org.neo4j.cypher.internal.frontend.v3_1.helpers.fixedPoint
import org.neo4j.cypher.internal.frontend.v3_1.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.frontend.v3_1.{DummyPosition, SemanticTable}

class PlannerQueryRewriterTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  val rewriter = PlannerQueryRewriter

  assert_that(
    """MATCH (a)
       OPTIONAL MATCH (a)-[r]->(b)
       RETURN distinct a as a""").
    is_rewritten_to(
      """MATCH (a)
         RETURN distinct a as a""")

  assert_that(
    """MATCH (a)
        OPTIONAL MATCH (a)-[r]->(b)
        RETURN DISTINCT a as a, b as b""").
    is_not_rewritten()

  assert_that(
    """MATCH (a)
        OPTIONAL MATCH (a)-[r]->(b)
        OPTIONAL MATCH (b)-[r2]->(c)
        RETURN DISTINCT c as c""").
    is_not_rewritten()

  assert_that(
    """MATCH (a)
        OPTIONAL MATCH (a)-[r]->(b)
        OPTIONAL MATCH (b)-[r2]->(c)
        OPTIONAL MATCH (c)-[r3]->(d)
        RETURN DISTINCT d as d""").
    is_not_rewritten()

  assert_that(
    """MATCH (a)
        OPTIONAL MATCH (a)-[r1]->(b)
        OPTIONAL MATCH (b)-[r2]->(c)
        OPTIONAL MATCH (a)-[r3]->(d)
        RETURN DISTINCT d as d""").
    is_rewritten_to(
      """MATCH (a)
        OPTIONAL MATCH (a)-[r3]->(d)
        RETURN DISTINCT d as d"""
    )

  assert_that(
    """MATCH (a)
        OPTIONAL MATCH (a)-[r1]->(b)
        OPTIONAL MATCH (b)-[r2]->(c)
        OPTIONAL MATCH (a)-[r3]->(d) WHERE c.prop = d.prop
        RETURN DISTINCT d as d""").
    is_not_rewritten()

  assert_that(
    """MATCH (a), (b)
       OPTIONAL MATCH (a)-[r]->(b)
       RETURN DISTINCT a as a, b as b""").
    is_rewritten_to(
      """MATCH (a), (b)
         RETURN DISTINCT a as a, b as b""")

  assert_that(
    """MATCH (a)
        OPTIONAL MATCH (a)-[r]->(b)
        RETURN DISTINCT a as a, r as r""").
    is_not_rewritten()

  ignore("unused optional relationship moved to predicate") {
    // not done
    /*
        MATCH (a)
        OPTIONAL MATCH (a)-->(b)-->(c)
        RETURN DISTINCT a, b

        is equivalent to:

        MATCH (a)
        OPTIONAL MATCH (a)-->(b) WHERE (b)-->(c)
        RETURN DISTINCT a, b
    */
  }

  case class rewriteTester(originalQuery: String) {
    def is_rewritten_to(newQuery: String): Unit =
      test(originalQuery) {
        val expected = getUnionQueryFrom(newQuery.stripMargin)
        val original = getUnionQueryFrom(originalQuery.stripMargin)

        val result = original.endoRewrite(fixedPoint(rewriter))
        assert(result === expected, "\n" + originalQuery)
      }


    def is_not_rewritten(): Unit = test(originalQuery) {
      val query = getUnionQueryFrom(originalQuery.stripMargin)
      query.endoRewrite(rewriter) should equal(query)
    }
  }

  private def assert_that(originalQuery: String): rewriteTester = rewriteTester(originalQuery)

  private def getUnionQueryFrom(query: String): UnionQuery = {
    val ast = parseForRewriting(query)
    val mkException = new SyntaxExceptionCreator(query, Some(DummyPosition(0)))
    val semanticState = semanticChecker.check(query, ast, mkException)
    val table = SemanticTable(types = semanticState.typeTable, recordedScopes = semanticState.recordedScopes)
    toUnionQuery(ast.asInstanceOf[Query], table)
  }

  private def parseForRewriting(queryText: String) = parser.parse(queryText.replace("\r\n", "\n"))

}
