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
package org.neo4j.cypher.internal.compiler.v3_4.planner.logical

import org.neo4j.cypher.internal.util.v3_4.Rewritable._
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.v3_4.{DummyPosition, Rewriter}
import org.neo4j.cypher.internal.compiler.v3_4.SyntaxExceptionCreator
import org.neo4j.cypher.internal.compiler.v3_4.ast.convert.plannerQuery.StatementConverters.toUnionQuery
import org.neo4j.cypher.internal.compiler.v3_4.planner._
import org.neo4j.cypher.internal.compiler.v3_4.planner.logical.OptionalMatchRemover.smallestGraphIncluding
import org.neo4j.cypher.internal.frontend.v3_4.ast.Query
import org.neo4j.cypher.internal.frontend.v3_4.ast.rewriters.flattenBooleanOperators
import org.neo4j.cypher.internal.frontend.v3_4.helpers.fixedPoint
import org.neo4j.cypher.internal.frontend.v3_4.semantics.{SemanticChecker, SemanticTable}
import org.neo4j.cypher.internal.ir.v3_4._
import org.neo4j.cypher.internal.v3_4.expressions.SemanticDirection.BOTH

class OptionalMatchRemoverTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  val rewriter: Rewriter = OptionalMatchRemover.instance(null)

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

  assert_that(
    """MATCH (a)
      |OPTIONAL MATCH (a)-[r]->(b)
      |DELETE r
      |RETURN DISTINCT a AS a""".stripMargin).
    is_not_rewritten()

  assert_that(
    """MATCH (a)
      |OPTIONAL MATCH (a)-[r]->(b)
      |SET b.foo = 1
      |RETURN DISTINCT a AS a""".stripMargin).
    is_not_rewritten()

  assert_that(
    """MATCH (a)
      |OPTIONAL MATCH (a)-[r]->(b)
      |SET a.foo = b.foo
      |RETURN DISTINCT a AS a""".stripMargin).
    is_not_rewritten()

  assert_that(
    """MATCH (a)
      |OPTIONAL MATCH (a)-[r]->(b)
      |SET r.foo = 1
      |RETURN DISTINCT a AS a""".stripMargin).
    is_not_rewritten()

  assert_that(
    """MATCH (a)
      |OPTIONAL MATCH (a)-[r]->(b)
      |SET b:FOO
      |RETURN DISTINCT a AS a""".stripMargin).
    is_not_rewritten()

  assert_that(
    """MATCH (a)
      |OPTIONAL MATCH (a)<-[r1]-(b)
      |CREATE (c {id: b.prop})
      |RETURN DISTINCT a AS a""".stripMargin).
    is_not_rewritten()

  assert_that(
    """MATCH (a)
      |OPTIONAL MATCH (a)<-[r1]-(b)
      |CREATE (a)-[r:T]->(b)
      |RETURN DISTINCT a AS a""".stripMargin).
    is_not_rewritten()

assert_that(
    """MATCH (a)
      |OPTIONAL MATCH (a)<-[r1]-(b)
      |MERGE (c:X {id: b.prop})
      |RETURN DISTINCT a AS a""".stripMargin).
    is_not_rewritten()

  assert_that(
    """MATCH (a)
      |OPTIONAL MATCH (a)<-[r1]-(b)
      |MERGE (a)-[r:T]->(b)
      |RETURN DISTINCT a AS a""".stripMargin).
    is_not_rewritten()

  assert_that(
    """MATCH (a)
      |OPTIONAL MATCH (a)<-[r1]-(b)
      |FOREACH( x in b.collectionProp |
      |  CREATE (z) )
      |RETURN DISTINCT a AS a""".stripMargin).
    is_not_rewritten()

  val x = "x"
  val n = "n"
  val m = "m"
  val c = "c"
  val r1 = "r1"
  val r2 = "r2"
  val r3 = "r3"


  test("finds shortest path starting from a single element with a single node in the QG") {
    val qg = QueryGraph(patternNodes = Set(n))

    smallestGraphIncluding(qg, Set(n)) should equal(Set(n))
  }

  test("finds shortest path starting from a single element with a single relationship in the QG") {
    val r = PatternRelationship(r1, (n, m), BOTH, Seq.empty, SimplePatternLength)
    val qg = QueryGraph(patternRelationships = Set(r), patternNodes = Set(n, m))

    smallestGraphIncluding(qg, Set(n)) should equal(Set(n))
  }

  test("finds shortest path starting from two nodes with a single relationship in the QG") {
    val r = PatternRelationship(r1, (n, m), BOTH, Seq.empty, SimplePatternLength)
    val qg = QueryGraph(patternRelationships = Set(r), patternNodes = Set(n, m))

    smallestGraphIncluding(qg, Set(n, m)) should equal(Set(n, m, r1))
  }

  test("finds shortest path starting from two nodes with two relationships in the QG") {
    val pattRel1 = PatternRelationship(r1, (n, m), BOTH, Seq.empty, SimplePatternLength)
    val pattRel2 = PatternRelationship(r2, (m, c), BOTH, Seq.empty, SimplePatternLength)
    val qg = QueryGraph(patternRelationships = Set(pattRel1, pattRel2), patternNodes = Set(n, m, c))

    smallestGraphIncluding(qg, Set(n, m)) should equal(Set(n, m, r1))
  }

  test("finds shortest path starting from two nodes with two relationships between the same nodes in the QG") {
    val pattRel1 = PatternRelationship(r1, (n, m), BOTH, Seq.empty, SimplePatternLength)
    val pattRel2 = PatternRelationship(r2, (n, m), BOTH, Seq.empty, SimplePatternLength)
    val qg = QueryGraph(patternRelationships = Set(pattRel1, pattRel2), patternNodes = Set(n, m))

    val result = smallestGraphIncluding(qg, Set(n, m))
    result should contain(n)
    result should contain(m)
    result should contain oneOf (r1, r2)
  }

  test("finds shortest path starting from two nodes with an intermediate relationship in the QG") {
    val pattRel1 = PatternRelationship(r1, (n, m), BOTH, Seq.empty, SimplePatternLength)
    val pattRel2 = PatternRelationship(r2, (m, c), BOTH, Seq.empty, SimplePatternLength)
    val qg = QueryGraph(patternRelationships = Set(pattRel1, pattRel2), patternNodes = Set(n, m, c))

    smallestGraphIncluding(qg, Set(n, c)) should equal(
      Set(n, m, c, r1, r2))
  }

  test("find smallest graph that connect three nodes") { // MATCH (n)-[r1]-(m), (n)-[r2]->(c), (n)-[r3]->(x)
    val pattRel1 = PatternRelationship(r1, (n, m), BOTH, Seq.empty, SimplePatternLength)
    val pattRel2 = PatternRelationship(r2, (n, c), BOTH, Seq.empty, SimplePatternLength)
    val pattRel3 = PatternRelationship(r3, (n, x), BOTH, Seq.empty, SimplePatternLength)
    val qg = QueryGraph(
      patternRelationships = Set(pattRel1, pattRel2, pattRel3),
      patternNodes = Set(n, m, c, x))

    smallestGraphIncluding(qg, Set(n, m, c)) should equal(
      Set(n, m, c, r1, r2))
  }

  test("querygraphs containing only nodes") {
    val qg = QueryGraph(patternNodes = Set(n, m))

    smallestGraphIncluding(qg, Set(n, m)) should equal(Set(n, m))
  }

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
    val onError = SyntaxExceptionCreator.throwOnError(mkException)
    val result = SemanticChecker.check(ast)
    onError(result.errors)
    val table = SemanticTable(types = result.state.typeTable, recordedScopes = result.state.recordedScopes)
    toUnionQuery(ast.asInstanceOf[Query], table)
  }

  private def parseForRewriting(queryText: String) = parser.parse(queryText.replace("\r\n", "\n"))
}
