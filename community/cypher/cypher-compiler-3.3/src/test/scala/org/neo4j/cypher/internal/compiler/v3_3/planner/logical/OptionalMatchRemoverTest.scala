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
package org.neo4j.cypher.internal.compiler.v3_3.planner.logical

import org.neo4j.cypher.internal.compiler.v3_3.SyntaxExceptionCreator
import org.neo4j.cypher.internal.compiler.v3_3.ast.convert.plannerQuery.StatementConverters.toUnionQuery
import org.neo4j.cypher.internal.compiler.v3_3.planner._
import org.neo4j.cypher.internal.frontend.v3_3.Rewritable._
import org.neo4j.cypher.internal.frontend.v3_3.ast.Query
import org.neo4j.cypher.internal.frontend.v3_3.ast.rewriters.flattenBooleanOperators
import org.neo4j.cypher.internal.frontend.v3_3.helpers.fixedPoint
import org.neo4j.cypher.internal.frontend.v3_3.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.frontend.v3_3.{DummyPosition, SemanticChecker, SemanticTable}
import org.neo4j.cypher.internal.ir.v3_3.UnionQuery

class OptionalMatchRemoverTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  val rewriter = OptionalMatchRemover.instance(null)

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
        OPTIONAL MATCH (a)-[r]->(b)-[r2]->(c)-[r3]->(d)
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
        OPTIONAL MATCH (a)-[r3]->(d)
        RETURN count(distinct d) as x""").
    is_rewritten_to(
      """MATCH (a)
        OPTIONAL MATCH (a)-[r3]->(d)
        RETURN count(distinct d) as x"""
    )

  assert_that(
    """MATCH (a)
        OPTIONAL MATCH (a)-[r1]->(b)
        OPTIONAL MATCH (b)-[r2]->(c)
        OPTIONAL MATCH (a)-[r3]->(d) WHERE c.prop = d.prop
        RETURN DISTINCT d as d""").
    is_not_rewritten()

  assert_that(
    """MATCH (a)
        OPTIONAL MATCH (a)-[r1]->(b)-[r2]->(c)
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

  assert_that(
    """MATCH (a)
       OPTIONAL MATCH (a)-[r]->(b)
       RETURN count(distinct a) as x""").
    is_rewritten_to(
      """MATCH (a)
         RETURN count(distinct a) as x""")

  assert_that(
    """MATCH (a)
       OPTIONAL MATCH (a)-[r]->(b)-[r2]->(c) WHERE c.prop = b.prop
       RETURN DISTINCT b as b""").
    is_not_rewritten()

  assert_that(
    """OPTIONAL MATCH (f:DoesExist)
       OPTIONAL MATCH (n:DoesNotExist)
       RETURN collect(DISTINCT n.property) AS a, collect(DISTINCT f.property) AS b """).
    is_not_rewritten()

  assert_that(
    """MATCH (a)
          OPTIONAL MATCH (a)-[r:T1]->(b)-[r2:T2]->(c)
          RETURN DISTINCT b as b""").
    is_rewritten_to(
      """MATCH (a)
          OPTIONAL MATCH (a)-[r:T1]->(b) WHERE (b)-[:T2]->()
          RETURN DISTINCT b as b""")

  assert_that(
    """MATCH (a)
          OPTIONAL MATCH (a)-[r:T1]->(b)-[r2:T2]->(c) WHERE b:B
          RETURN DISTINCT b as b""").
    is_rewritten_to(
      """MATCH (a)
          OPTIONAL MATCH (a)-[r:T1]->(b) WHERE b:B and (b)-[:T2]->()
          RETURN DISTINCT b as b""")

  assert_that(
    """MATCH (a)
            OPTIONAL MATCH (a)-[r:T1]->(b)-[r2:T2]->(c) WHERE c.age <> 42
            RETURN DISTINCT b as b""").
    is_not_rewritten()

  assert_that(
    """MATCH (a)
          OPTIONAL MATCH (a)-[r:T1]->(b)-[r2:T2]->(c) WHERE c:A:B and c.id = 42 and c.foo = 'apa'
          RETURN DISTINCT b as b""").
    is_rewritten_to(
      """MATCH (a)
          OPTIONAL MATCH (a)-[r:T1]->(b) WHERE (b)-[:T2]->(:A:B {foo: 'apa', id: 42})
          RETURN DISTINCT b as b""")

  case class RewriteTester(originalQuery: String) {
    def is_rewritten_to(newQuery: String): Unit =
      test(originalQuery) {
        val expected = getUnionQueryFrom(newQuery.stripMargin)
        val original = getUnionQueryFrom(originalQuery.stripMargin)

        val result = original.endoRewrite(fixedPoint(rewriter))
        assert(result === expected, "\nWas not rewritten correctly\n" + originalQuery)
      }

    def is_not_rewritten(): Unit = test(originalQuery) {
      val query = getUnionQueryFrom(originalQuery.stripMargin)
      val result = query.endoRewrite(fixedPoint(rewriter))
      assert(result === query, "\nShould not have been rewritten\n" + originalQuery)
    }
  }

  private def assert_that(originalQuery: String): RewriteTester = RewriteTester(originalQuery)

  private def getUnionQueryFrom(query: String): UnionQuery = {
    val ast = parseForRewriting(query).endoRewrite(flattenBooleanOperators)
    val mkException = new SyntaxExceptionCreator(query, Some(DummyPosition(0)))
    val semanticState = SemanticChecker.check(ast, SyntaxExceptionCreator.throwOnError(mkException))
    val table = SemanticTable(types = semanticState.typeTable, recordedScopes = semanticState.recordedScopes)
    toUnionQuery(ast.asInstanceOf[Query], table)
  }

  private def parseForRewriting(queryText: String) = parser.parse(queryText.replace("\r\n", "\n"))
}
