/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.v3_5.rewriting

import org.neo4j.cypher.internal.v3_5.ast.semantics.{SemanticState, SyntaxExceptionCreator}
import org.neo4j.cypher.internal.v3_5.ast.{AstConstructionTestSupport, Statement}
import org.neo4j.cypher.internal.v3_5.parser.ParserFixture.parser
import org.neo4j.cypher.internal.v3_5.rewriting.rewriters.normalizeWithAndReturnClauses
import org.neo4j.cypher.internal.v3_5.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v3_5.util.{Rewriter, SyntaxException}

class NormalizeWithAndReturnClausesTest extends CypherFunSuite with RewriteTest with AstConstructionTestSupport {
  val mkException = new SyntaxExceptionCreator("<Query>", Some(pos))
  val rewriterUnderTest: Rewriter = normalizeWithAndReturnClauses(mkException)

  test("ensure variables are aliased") {
    assertRewrite(
      """MATCH (n)
        |WITH n
        |RETURN n
      """.stripMargin,
      """MATCH (n)
        |WITH n AS n
        |RETURN n AS n
      """.stripMargin)
  }

  test("WITH: attach ORDER BY expressions to existing aliases") {
    assertRewrite(
      """MATCH (n)
        |WITH n.prop AS prop ORDER BY n.prop
        |RETURN prop
      """.stripMargin,
      """MATCH (n)
        |WITH n.prop AS prop ORDER BY prop
        |RETURN prop AS prop
      """.stripMargin)
  }

  test("RETURN: attach ORDER BY expressions to existing aliases") {
    assertRewrite(
      """MATCH (n)
        |RETURN n.prop AS prop ORDER BY n.prop
      """.stripMargin,
      """MATCH (n)
        |RETURN n.prop AS prop ORDER BY prop
      """.stripMargin)
  }

  test("attach WHERE expression to existing aliases") {
    assertRewrite(
      """MATCH (n)
        |WITH length(n.prop) > 10 AS result WHERE length(n.prop) > 10
        |RETURN result
      """.stripMargin,
      """MATCH (n)
        |WITH length(n.prop) > 10 AS result WHERE result
        |RETURN result AS result
      """.stripMargin)
  }

  test("does not introduce aliases for ORDER BY with existing alias") {
    assertIsNotRewritten(
      """MATCH (n)
        |WITH n.prop AS prop ORDER BY prop
        |RETURN prop AS prop
      """.stripMargin)
  }

  test("does not introduce aliases for WHERE with existing alias") {
    assertIsNotRewritten(
      """MATCH (n)
        |WITH n.prop AS prop WHERE prop
        |RETURN prop AS prop
      """.stripMargin)
  }

  test("WITH: does not introduce aliases for ORDER BY expressions that depend on existing variables") {
    assertRewrite(
      """MATCH (n)
        |WITH n ORDER BY length(n.prop)
        |RETURN n
      """.stripMargin,
      """MATCH (n)
        |WITH n AS n ORDER BY length(n.prop)
        |RETURN n AS n
      """.stripMargin)
  }

  test("RETURN: does not introduce aliases for ORDER BY expressions that depend on existing variables") {
    assertRewrite(
      """MATCH (n)
        |RETURN n ORDER BY length(n.prop)
      """.stripMargin,
      """MATCH (n)
        |RETURN n AS n ORDER BY length(n.prop)
      """.stripMargin)
  }

  test("does not introduce aliases for WHERE expressions that depend on existing variables") {
    assertRewrite(
      """MATCH (n)
        |WITH n WHERE length(n.prop) > 10
        |RETURN n
      """.stripMargin,
      """MATCH (n)
        |WITH n AS n WHERE length(n.prop) > 10
        |RETURN n AS n
      """.stripMargin)
  }

  test("WITH: introduces aliases for ORDER BY expressions that depend on existing aliases") {
    assertRewrite(
      """MATCH (n)
        |WITH n.prop AS prop ORDER BY length(n.prop)
        |RETURN prop
      """.stripMargin,
      """MATCH (n)
        |WITH n.prop AS prop ORDER BY length(prop)
        |RETURN prop AS prop
      """.stripMargin)
  }

  test("RETURN: introduces aliases for ORDER BY expressions that depend on existing aliases") {
    assertRewrite(
      """MATCH (n)
        |RETURN n.prop AS prop ORDER BY length(n.prop)
      """.stripMargin,
      """MATCH (n)
        |RETURN n.prop AS prop ORDER BY length(prop)
      """.stripMargin)
  }

  test("introduces aliases for WHERE expressions that depend on existing aliases") {
    assertRewrite(
      """MATCH (n)
        |WITH n.prop AS prop WHERE length(n.prop) > 10
        |RETURN prop
      """.stripMargin,
      """MATCH (n)
        |WITH n.prop AS prop WHERE length(prop) > 10
        |RETURN prop AS prop
      """.stripMargin)
  }

  test("WITH: renames variables for ORDER BY expressions that depend on existing aliases") {
    assertRewrite(
      """MATCH (n)
        |WITH n AS m ORDER BY n
        |RETURN m
      """.stripMargin,
      """MATCH (n)
        |WITH n AS m ORDER BY m
        |RETURN m AS m
      """.stripMargin)
  }

  test("RETURN: renames variables for ORDER BY expressions that depend on existing aliases") {
    assertRewrite(
      """MATCH (n)
        |RETURN n AS m ORDER BY n
      """.stripMargin,
      """MATCH (n)
        |RETURN n AS m ORDER BY m
      """.stripMargin)
  }

  test("WITH: does not rename variables for ORDER BY from RHS of AS, if they also exist on LHS of AS") {
    assertIsNotRewritten(
      """MATCH (x), (y)
        |WITH x AS y, y as z
        |ORDER BY y
        |RETURN y AS y, z AS z
        |""".stripMargin)
  }

  test("RETURN: does not rename variables for ORDER BY from RHS of AS, if they also exist on LHS of AS") {
    assertIsNotRewritten(
      """MATCH (x), (y)
        |RETURN x AS y, y as z
        |ORDER BY y
        |""".stripMargin)
  }

  test("WITH: does not rename variables for ORDER BY from RHS of AS, if they also exist on LHS of AS. Expression in ORDER BY.") {
    assertIsNotRewritten(
      """MATCH (x), (y)
        |WITH x AS y, y as z
        |ORDER BY foo(y)
        |RETURN y AS y, z AS z
        |""".stripMargin)
  }

  test("RETURN: does not rename variables for ORDER BY from RHS of AS, if they also exist on LHS of AS. Expression in ORDER BY.") {
    assertIsNotRewritten(
      """MATCH (x), (y)
        |RETURN x AS y, y as z
        |ORDER BY foo(y)
        |""".stripMargin)
  }

  test("WITH: renames variables for ORDER BY from LHS of AS, if the RHS also exist on LHS of another AS") {
    assertRewrite(
      """MATCH (x), (y)
        |WITH x AS y, y as z
        |ORDER BY x
        |RETURN y AS y, z AS z
        |""".stripMargin,

      """MATCH (x), (y)
        |WITH x AS y, y as z
        |ORDER BY y
        |RETURN y AS y, z AS z
        |""".stripMargin)
  }

  test("RETURN: renames variables for ORDER BY from LHS of AS, if the RHS also exist on LHS of another AS") {
    assertRewrite(
      """MATCH (x), (y)
        |RETURN x AS y, y as z
        |ORDER BY x
        |""".stripMargin,
      """MATCH (x), (y)
        |RETURN x AS y, y as z
        |ORDER BY y
        |""".stripMargin)
  }

  test("renames variables for WHERE expressions that depend on existing aliases") {
    assertRewrite(
      """UNWIND [true] as n
        |WITH n AS m WHERE n
        |RETURN m
      """.stripMargin,
      """UNWIND [true] as n
        |WITH n AS m WHERE m
        |RETURN m AS m
      """.stripMargin)
  }

  test("WITH: introduces aliases for complex ORDER BY expressions that depend on existing aliases") {
    assertRewrite(
      """MATCH (n)
        |WITH n.prop AS prop ORDER BY length(n.prop[0])
        |RETURN prop
      """.stripMargin,
      """MATCH (n)
        |WITH n.prop AS prop ORDER BY length(prop[0])
        |RETURN prop AS prop
      """.stripMargin)
  }

  test("RETURN: introduces aliases for complex ORDER BY expressions that depend on existing aliases") {
    assertRewrite(
      """MATCH (n)
        |RETURN n.prop AS prop ORDER BY length(n.prop[0])
      """.stripMargin,
      """MATCH (n)
        |RETURN n.prop AS prop ORDER BY length(prop[0])
      """.stripMargin)
  }

  test("introduces aliases for complex WHERE expressions that depend on existing aliases") {
    assertRewrite(
      """MATCH (n)
        |WITH n.prop AS prop WHERE length(n.prop[0]) > 10
        |RETURN prop
      """.stripMargin,
      """MATCH (n)
        |WITH n.prop AS prop WHERE length(prop[0]) > 10
        |RETURN prop AS prop
      """.stripMargin)
  }

  test("does not introduce variables for ORDER BY expressions that depend on non-aliased variables") {
    assertIsNotRewritten(
      """MATCH (n)
        |WITH n.prop AS prop ORDER BY n.foo DESC
        |RETURN prop AS prop
      """.stripMargin)
  }

  test("does not introduce variables for WHERE expressions that depend on non-aliased variables") {
    assertIsNotRewritten(
      """MATCH (n)
        |WITH n.prop AS prop WHERE n.foo > 10
        |RETURN prop AS prop
      """.stripMargin)
  }

  test("does not introduce variables for ORDER BY expressions that depend on non-aliased variables in WITH *") {
    assertIsNotRewritten(
      """MATCH (n)
        |WITH *, n.prop AS prop ORDER BY n.foo DESC
        |RETURN prop AS prop
      """.stripMargin)
  }
  test("does not introduce variables for WHERE expressions that depend on non-aliased variables in WITH *") {
    assertIsNotRewritten(
      """MATCH (n)
        |WITH *, n.prop AS prop WHERE n.foo > 10
        |RETURN prop AS prop
      """.stripMargin)
  }

  test("WITH: introduces variables for ORDER BY expressions that depend on existing aliases in WITH *") {
    assertRewrite(
      """MATCH (n)
        |WITH *, n.prop AS prop ORDER BY n.prop DESC
        |RETURN prop
      """.stripMargin,
      """MATCH (n)
        |WITH *, n.prop AS prop ORDER BY prop DESC
        |RETURN prop AS prop
      """.stripMargin)
  }

  test("RETURN: introduces variables for ORDER BY expressions that depend on existing aliases in WITH *") {
    assertRewrite(
      """MATCH (n)
        |RETURN *, n.prop AS prop ORDER BY n.prop DESC
      """.stripMargin,
      """MATCH (n)
        |RETURN *, n.prop AS prop ORDER BY prop DESC
      """.stripMargin)
  }

  test("introduces variables for WHERE expressions that depend on existing aliases in WITH *") {
    assertRewrite(
      """MATCH (n)
        |WITH *, n.prop AS prop WHERE n.prop > 10
        |RETURN prop
      """.stripMargin,
      """MATCH (n)
        |WITH *, n.prop AS prop WHERE prop > 10
        |RETURN prop AS prop
      """.stripMargin)
  }

  test("WITH: does not attach ORDER BY expressions to unaliased items") {
    // Note: unaliased items in WITH are invalid, and will be caught during semantic check
    assertNotRewritittenAndSemanticErrors(
      """MATCH (n)
        |WITH n.prop ORDER BY n.prop
        |RETURN prop AS prop
      """.stripMargin, "Expression in WITH must be aliased (use AS) (line 2, column 6 (offset: 15))")
  }

  test("RETURN: attaches ORDER BY expressions to unaliased items") {
    assertRewrite(
      """MATCH (n)
        |RETURN n.prop ORDER BY n.prop
      """.stripMargin,
      """MATCH (n)
        |RETURN n.prop AS `n.prop` ORDER BY `n.prop`
      """.stripMargin)
  }

  test("RETURN: aliases complex expression") {
    assertRewrite(
      """MATCH (n)
        |RETURN foo(n.prop[0])
      """.stripMargin,
      """MATCH (n)
        |RETURN foo(n.prop[0]) AS `foo(n.prop[0])`
      """.stripMargin)
  }

  test("does not attach WHERE expression to unaliased items") {
    // Note: unaliased items in WITH are invalid, and will be caught during semantic check
    assertNotRewritittenAndSemanticErrors(
      """MATCH (n)
        |WITH n.prop WHERE n.prop
        |RETURN prop AS prop
      """.stripMargin, "Expression in WITH must be aliased (use AS) (line 2, column 6 (offset: 15))")
  }

  test("rejects use of aggregation in ORDER BY if aggregation is not used in associated WITH") {
    // Note: aggregations in ORDER BY that don't also appear in WITH are invalid
    try {
      rewrite(parseForRewriting(
        """MATCH (n)
          |WITH n.prop AS prop ORDER BY max(n.foo)
          |RETURN prop
        """.stripMargin))
      fail("We shouldn't get here")
    } catch {
      case e: SyntaxException =>
        e.getMessage should equal("Cannot use aggregation in ORDER BY if there are no aggregate expressions in the preceding WITH (line 2, column 1 (offset: 10))")
    }
  }

  test("rejects use of aggregation in ORDER BY if aggregation is not used in associated RETURN") {
    // Note: aggregations in ORDER BY that don't also appear in WITH are invalid
    try {
      rewrite(parseForRewriting(
        """MATCH (n)
          |RETURN n.prop AS prop ORDER BY max(n.foo)
        """.stripMargin))
      fail("We shouldn't get here")
    } catch {
      case e: SyntaxException =>
        e.getMessage should equal("Cannot use aggregation in ORDER BY if there are no aggregate expressions in the preceding RETURN (line 2, column 1 (offset: 10))")
    }
  }

  test("accepts use of aggregation in ORDER BY if aggregation is used in associated WITH") {
    assertRewrite(
      """MATCH (n)
        |WITH n.prop AS prop, max(n.foo) AS m ORDER BY max(n.foo)
        |RETURN prop AS prop, m AS m
      """.stripMargin,
      """MATCH (n)
        |WITH n.prop AS prop, max(n.foo) AS m ORDER BY m
        |RETURN prop AS prop, m AS m
      """.stripMargin
    )
  }

  test("accepts use of aggregation in ORDER BY if aggregation is used in associated RETURN") {
    assertRewrite(
      """MATCH (n)
        |RETURN n.prop AS prop, max(n.foo) AS m ORDER BY max(n.foo)
      """.stripMargin,
      """MATCH (n)
        |RETURN n.prop AS prop, max(n.foo) AS m ORDER BY m
      """.stripMargin
    )
  }

  test("accepts use of aggregation in WHERE if aggregation is used in associated WITH") {
    assertRewrite(
      """MATCH (n)
        |WITH n.prop AS prop, max(n.foo) AS m WHERE max(n.foo) = 10
        |RETURN prop AS prop, m AS m
      """.stripMargin,
      """MATCH (n)
        |WITH n.prop AS prop, max(n.foo) AS m WHERE m = 10
        |RETURN  prop AS prop, m AS m
      """.stripMargin
    )
  }

  test("does not introduce alias for WHERE containing aggregate") {
    // Note: aggregations in WHERE are invalid, and will be caught during semantic check
    assertNotRewritittenAndSemanticErrors(
      """MATCH (n)
        |WITH n.prop AS prop WHERE max(n.foo)
        |RETURN prop AS prop
      """.stripMargin, "Invalid use of aggregating function max(...) in this context (line 2, column 27 (offset: 36))")
  }

  test("preserves SKIP and LIMIT") {
    assertRewrite(
      """MATCH (n)
        |WITH n SKIP 5 LIMIT 2
        |RETURN n SKIP 5 LIMIT 2
      """.stripMargin,
      """MATCH (n)
        |WITH n AS n SKIP 5 LIMIT 2
        |RETURN n AS n SKIP 5 LIMIT 2
      """.stripMargin)
  }

  test("WITH: preserves SKIP and LIMIT with ORDER BY expressions") {
    assertRewrite(
      """MATCH (n)
        |WITH n ORDER BY n.prop SKIP 5 LIMIT 2
        |RETURN n
      """.stripMargin,
      """MATCH (n)
        |WITH n AS n ORDER BY n.prop SKIP 5 LIMIT 2
        |RETURN n AS n
      """.stripMargin)
  }

  test("RETURN: preserves SKIP and LIMIT with ORDER BY expressions") {
    assertRewrite(
      """MATCH (n)
        |RETURN n ORDER BY n.prop SKIP 5 LIMIT 2
      """.stripMargin,
      """MATCH (n)
        |RETURN n AS n ORDER BY n.prop SKIP 5 LIMIT 2
      """.stripMargin)
  }

  test("preserves SKIP and LIMIT with WHERE expression") {
    assertRewrite(
      """MATCH (n)
        |WITH n SKIP 5 LIMIT 2 WHERE n.prop > 10
        |RETURN n
      """.stripMargin,
      """MATCH (n)
        |WITH n AS n SKIP 5 LIMIT 2 WHERE n.prop > 10
        |RETURN n AS n
      """.stripMargin)
  }

  test("WITH: preserves DISTINCT when replacing ORDER BY expressions with alias") {
    assertRewrite(
      """MATCH (n)
        |WITH DISTINCT n.prop AS prop ORDER BY n.prop
        |RETURN prop
      """.stripMargin,
      """MATCH (n)
        |WITH DISTINCT n.prop AS prop ORDER BY prop
        |RETURN prop AS prop
      """.stripMargin)
  }

  test("RETURN: preserves DISTINCT when replacing ORDER BY expressions with alias") {
    assertRewrite(
      """MATCH (n)
        |RETURN DISTINCT n.prop AS prop ORDER BY n.prop
      """.stripMargin,
      """MATCH (n)
        |RETURN DISTINCT n.prop AS prop ORDER BY prop
      """.stripMargin)
  }

  test("preserves DISTINCT when replacing WHERE expressions with alias") {
    assertRewrite(
      """MATCH (n)
        |WITH DISTINCT n.prop AS prop WHERE n.prop
        |RETURN prop
      """.stripMargin,
      """MATCH (n)
        |WITH DISTINCT n.prop AS prop WHERE prop
        |RETURN prop AS prop
      """.stripMargin)
  }

  test("WITH: aggregating: does not change grouping set when introducing aliases for ORDER BY") {
    assertRewrite(
      """MATCH (n)
        |WITH DISTINCT n.prop AS prop ORDER BY length(n.prop)
        |RETURN prop
      """.stripMargin,
      """MATCH (n)
        |WITH DISTINCT n.prop AS prop ORDER BY length(prop)
        |RETURN prop AS prop
      """.stripMargin)

    assertRewrite(
      """MATCH (n)
        |WITH n.prop AS prop, count(*) AS count ORDER BY length(n.prop)
        |RETURN prop, count
      """.stripMargin,
      """MATCH (n)
        |WITH n.prop AS prop, count(*) AS count ORDER BY length(prop)
        |RETURN prop AS prop, count AS count
      """.stripMargin)
  }

  test("RETURN: aggregating: does not change grouping set when introducing aliases for ORDER BY") {
    assertRewrite(
      """MATCH (n)
        |RETURN DISTINCT n.prop AS prop ORDER BY length(n.prop)
      """.stripMargin,
      """MATCH (n)
        |RETURN DISTINCT n.prop AS prop ORDER BY length(prop)
      """.stripMargin)

    assertRewrite(
      """MATCH (n)
        |RETURN n.prop AS prop, count(*) AS count ORDER BY length(n.prop)
      """.stripMargin,
      """MATCH (n)
        |RETURN n.prop AS prop, count(*) AS count ORDER BY length(prop)
      """.stripMargin)
  }

  test("aggregating: does not change grouping set when introducing aliases for WHERE") {
    assertRewrite(
      """MATCH (n)
        |WITH DISTINCT n.prop AS prop WHERE length(n.prop) = 1
        |RETURN prop
      """.stripMargin,
      """MATCH (n)
        |WITH DISTINCT n.prop AS prop WHERE length(prop) = 1
        |RETURN prop AS prop
      """.stripMargin)

    assertRewrite(
      """MATCH (n)
        |WITH n.prop AS prop, count(*) AS count WHERE length(n.prop) = 1
        |RETURN prop, count
      """.stripMargin,
      """MATCH (n)
        |WITH n.prop AS prop, count(*) AS count WHERE length(prop) = 1
        |RETURN prop AS prop, count AS count
      """.stripMargin)
  }

  test("WITH: aggregating: does not change grouping set when introducing aliases for ORDER BY with non-grouping expression") {
    // Note: using a non-grouping expression for ORDER BY when aggregating is invalid, and will be caught during semantic check
    assertNotRewritittenAndSemanticErrors(
      """MATCH (n)
        |WITH DISTINCT n.prop AS prop ORDER BY n.foo
        |RETURN prop AS prop
      """.stripMargin, "In a WITH/RETURN with DISTINCT or an aggregation, it is not possible to access variables declared before the WITH/RETURN: n (line 2, column 39 (offset: 48))")

    assertNotRewritittenAndSemanticErrors(
      """MATCH (n)
        |WITH n.prop AS prop, collect(n.foo) AS foos ORDER BY n.foo
        |RETURN prop AS prop, foos AS foos
      """.stripMargin, "In a WITH/RETURN with DISTINCT or an aggregation, it is not possible to access variables declared before the WITH/RETURN: n (line 2, column 54 (offset: 63))")
  }

  test("RETURN: aggregating: does not change grouping set when introducing aliases for ORDER BY with non-grouping expression") {
    // Note: using a non-grouping expression for ORDER BY when aggregating is invalid, and will be caught during semantic check
    assertNotRewritittenAndSemanticErrors(
      """MATCH (n)
        |RETURN DISTINCT n.prop AS prop ORDER BY n.foo
      """.stripMargin, "In a WITH/RETURN with DISTINCT or an aggregation, it is not possible to access variables declared before the WITH/RETURN: n (line 2, column 41 (offset: 50))")

    assertNotRewritittenAndSemanticErrors(
      """MATCH (n)
        |RETURN n.prop AS prop, collect(n.foo) AS foos ORDER BY n.foo
      """.stripMargin, "In a WITH/RETURN with DISTINCT or an aggregation, it is not possible to access variables declared before the WITH/RETURN: n (line 2, column 56 (offset: 65))")
  }

  test("aggregating: does not change grouping set when introducing aliases for WHERE with non-grouping expression") {
    // Note: using a non-grouping expression for ORDER BY when aggregating is invalid, and will be caught during semantic check
    assertNotRewritittenAndSemanticErrors(
      """MATCH (n)
        |WITH DISTINCT n.prop AS prop WHERE n.foo
        |RETURN prop AS prop
      """.stripMargin, "In a WITH/RETURN with DISTINCT or an aggregation, it is not possible to access variables declared before the WITH/RETURN: n (line 2, column 36 (offset: 45))")


    assertNotRewritittenAndSemanticErrors(
      """MATCH (n)
        |WITH n.prop AS prop, collect(n.foo) AS foos WHERE n.foo
        |RETURN prop AS prop, foos AS foos
      """.stripMargin, "In a WITH/RETURN with DISTINCT or an aggregation, it is not possible to access variables declared before the WITH/RETURN: n (line 2, column 51 (offset: 60))")
  }

  // Below: unordered, exploratory tests

  test("MATCH (u)-[r1]->(v) WITH r1 AS r2, rand() AS c ORDER BY c MATCH (a)-[r2]->(b) RETURN r2 AS rel") {
    assertIsNotRewritten(
      """MATCH (u)-[r1]->(v)
        |WITH r1 AS r2, rand() AS c ORDER BY c
        |MATCH (a)-[r2]->(b)
        |RETURN r2 AS rel
      """.stripMargin)
  }

  test("MATCH (foo) WITH {meh} AS x ORDER BY x.prop DESC LIMIT 4 RETURN x") {
    assertIsNotRewritten(
      """MATCH (foo)
        |WITH {meh} AS x ORDER BY x.prop DESC LIMIT 4
        |RETURN x AS x
      """.stripMargin)
  }

  test("MATCH (n) with n order by n.name ASC skip 2 return n") {
    assertRewrite(
      """MATCH (n)
        |WITH n ORDER BY n.name ASC SKIP 2
        |RETURN n
      """.stripMargin,
      """MATCH (n)
        |WITH n AS n ORDER BY n.name ASC SKIP 2
        |RETURN n AS n
      """.stripMargin)
  }

  test("MATCH (x) WITH DISTINCT x as otherName ORDER BY x.name RETURN otherName") {
    assertRewrite(
      """MATCH (x)
        |WITH DISTINCT x AS otherName ORDER BY x.name
        |RETURN otherName
      """.stripMargin,
      """MATCH (x)
        |WITH DISTINCT x AS otherName ORDER BY otherName.name
        |RETURN otherName AS otherName
      """.stripMargin)
  }

  test("MATCH (x) WITH x as otherName ORDER BY x.name + otherName.name RETURN otherName") {
    assertRewrite(
      """MATCH (x)
        |WITH x AS otherName ORDER BY x.name + otherName.name
        |RETURN otherName
      """.stripMargin,
      """MATCH (x)
        |WITH x AS otherName ORDER BY otherName.name + otherName.name
        |RETURN otherName AS otherName
      """.stripMargin)
  }

  test("MATCH (a)-[r]->(b) WITH a, r, b, rand() AS c ORDER BY c MATCH (a)-[r]->(b) RETURN r AS rel") {
    assertRewrite(
      """MATCH (a)-[r]->(b)
        |WITH a, r, b, rand() AS c ORDER BY c
        |MATCH (a)-[r]->(b)
        |RETURN r AS rel
      """.stripMargin,
      """MATCH (a)-[r]->(b)
        |WITH a AS a, r AS r, b AS b, rand() AS c ORDER BY c
        |MATCH (a)-[r]->(b)
        |RETURN r AS rel
      """.stripMargin
    )
  }

  test("MATCH (n) WHERE id(n) IN [0,1,2,3] WITH n.division AS div, max(n.age) AS age order by max(n.age) RETURN div, age") {
    assertRewrite(
      """MATCH (n) WHERE id(n) IN [0,1,2,3]
        |WITH n.division AS div, max(n.age) AS age ORDER BY max(n.age)
        |RETURN div, age
      """.stripMargin,
      """MATCH (n) WHERE id(n) IN [0,1,2,3]
        |WITH n.division AS div, max(n.age) AS age ORDER BY age
        |RETURN div AS div, age AS age
      """.stripMargin
    )
  }

  test("MATCH (a) WITH a WHERE true return a") {
    assertRewrite(
      """MATCH (a)
        |WITH a WHERE true
        |RETURN a
      """.stripMargin,
      """MATCH (a)
        |WITH a AS a WHERE true
        |RETURN a AS a
      """.stripMargin
    )
  }

  test("MATCH (n) RETURN * ORDER BY id(n)") {
    assertIsNotRewritten(
      """MATCH (n)
        |WITH * ORDER BY id(n)
        |RETURN *
      """.stripMargin)
  }

  test("MATCH (n) WITH n, 0 AS foo WITH n AS n ORDER BY foo, n.bar RETURN n") {
    assertRewrite(
      """MATCH (n)
        |WITH n, 0 AS foo
        |WITH n AS n ORDER BY foo, n.bar
        |RETURN n
      """.stripMargin,
      """MATCH (n)
        |WITH n AS n, 0 AS foo
        |WITH n AS n ORDER BY foo, n.bar
        |RETURN n AS n
      """.stripMargin)
  }

  test("MATCH (n) WITH n AS n ORDER BY max(n) RETURN n") {
    evaluating { rewriting("MATCH (n) WITH n AS n ORDER BY max(n) RETURN n") } should produce[SyntaxException]
  }

  protected override def assertRewrite(originalQuery: String, expectedQuery: String) {
    val original = parseForRewriting(originalQuery.replace("\r\n", "\n"))
    val expected = parseForRewriting(expectedQuery.replace("\r\n", "\n"))
    val result = endoRewrite(original)
    assert(result === expected, s"\n$originalQuery\nshould be rewritten to:\n$expectedQuery\nbut was rewritten to:${prettifier.asString(result.asInstanceOf[Statement])}")

    val checkResult = result.semanticCheck(SemanticState.clean)
    assert(checkResult.errors === Seq())
  }

  protected def assertNotRewritittenAndSemanticErrors(query: String, semanticErrors: String*): Unit = {
    val original = parser.parse(query)
    val result = endoRewrite(original)
    assert(result === original, s"\n$query\nshould not have been rewritten but was to:\n${prettifier.asString(result.asInstanceOf[Statement])}")

    val checkResult = result.semanticCheck(SemanticState.clean)
    val errors = checkResult.errors.map(error => s"${error.msg} (${error.position})").toSet
    semanticErrors.foreach(msg =>
      assert(errors contains msg, s"Error '$msg' not produced (errors: $errors)}")
    )
  }

  protected def rewriting(queryText: String): Unit = {
    endoRewrite(parseForRewriting(queryText))
  }
}
