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
package org.neo4j.cypher.internal.rewriting

import org.neo4j.cypher.internal.ast.semantics.SemanticCheckContext
import org.neo4j.cypher.internal.ast.semantics.SemanticCheckResult
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature.MultipleDatabases
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.rewriting.rewriters.normalizeWithAndReturnClauses
import org.neo4j.cypher.internal.util.OpenCypherExceptionFactory
import org.neo4j.cypher.internal.util.OpenCypherExceptionFactory.SyntaxException
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class NormalizeWithAndReturnClausesTest extends CypherFunSuite with RewriteTest {
  private val exceptionFactory = OpenCypherExceptionFactory(None)
  val rewriterUnderTest: Rewriter = normalizeWithAndReturnClauses(exceptionFactory)

  test("ensure variables are aliased") {
    assertRewrite(
      """MATCH (n)
        |WITH n
        |RETURN n
      """.stripMargin,
      """MATCH (n)
        |WITH n AS n
        |RETURN n AS n
      """.stripMargin
    )
  }

  test("ensure map projections are aliased") {
    assertRewrite(
      """MATCH (n)
        |WITH n {.foo, .bar}
        |RETURN n {.baz, .bar}
      """.stripMargin,
      """MATCH (n)
        |WITH n {.foo, .bar} AS n
        |RETURN n {.baz, .bar} AS n
      """.stripMargin
    )
  }

  test("ensure expressions are aliased in RETURN") {
    assertRewrite(
      """MATCH (n)
        |RETURN n.bar, n
      """.stripMargin,
      """MATCH (n)
        |RETURN n.bar AS `n.bar`, n AS n
      """.stripMargin
    )
  }

  test("ensure all things are are aliased in UNION") {
    assertRewrite(
      """MATCH (n)
        |RETURN n, 3 + 5
        |  UNION
        |MATCH (n)
        |WITH n {.foo, .bar}
        |RETURN n {.baz, .bar}, 3 + 5
      """.stripMargin,
      """MATCH (n)
        |RETURN n AS n, 3 + 5 AS `3 + 5`
        |  UNION
        |MATCH (n)
        |WITH n {.foo, .bar} AS n
        |RETURN n {.baz, .bar} AS n, 3 + 5 AS `3 + 5`
      """.stripMargin
    )
  }

  test("ensure valid things are aliased in subqueries") {
    assertRewrite(
      """CALL {
        |  MATCH (n:N) RETURN n
        |    UNION
        |  MATCH (n:M) RETURN n
        |}
        |CALL {
        |  WITH n
        |  MATCH (n)--(m)--(p)
        |  RETURN p {.foo, .bar}, m
        |}
        |RETURN n, m, p
      """.stripMargin,
      """CALL {
        |  MATCH (n:N) RETURN n AS n
        |    UNION
        |  MATCH (n:M) RETURN n AS n
        |}
        |CALL {
        |  WITH n AS n
        |  MATCH (n)--(m)--(p)
        |  RETURN p {.foo, .bar} AS p, m AS m
        |}
        |RETURN n AS n, m AS m, p AS p
      """.stripMargin
    )
  }

  test("ensure returns are aliased in Exists expressions") {
    assertRewrite(
      """MATCH (n)
        |WHERE EXISTS {
        |  RETURN CASE
        |       WHEN true THEN 1
        |       ELSE 2
        |    END
        |}
        |RETURN n
      """.stripMargin,
      """MATCH (n)
        |  WHERE EXISTS { RETURN CASE WHEN true THEN 1 ELSE 2 END AS `CASE
        |       WHEN true THEN 1
        |       ELSE 2
        |    END` }
        |RETURN n AS n
      """.stripMargin
    )
  }

  test("ensure returns are aliased in Count expressions") {
    assertRewrite(
      """MATCH (n)
        |WHERE COUNT {
        |  RETURN CASE
        |       WHEN true THEN 1
        |       ELSE 2
        |    END
        |} > 1
        |RETURN n
      """.stripMargin,
      """MATCH (n)
        |  WHERE COUNT { RETURN CASE WHEN true THEN 1 ELSE 2 END AS `CASE
        |       WHEN true THEN 1
        |       ELSE 2
        |    END` } > 1
        |RETURN n AS n
      """.stripMargin
    )
  }

  test("ensure returns of a variable are aliased in Count expressions") {
    assertRewrite(
      """MATCH (n)
        |WHERE COUNT {
        |  MATCH (n)
        |  RETURN n
        |} > 1
        |RETURN n
      """.stripMargin,
      """MATCH (n)
        |  WHERE COUNT {
        |  MATCH (n)
        |  RETURN n AS n
        |} > 1
        |RETURN n AS n
      """.stripMargin
    )
  }

  test("ensure returns of a variable are aliased in Exists expressions") {
    assertRewrite(
      """MATCH (n)
        |WHERE EXISTS {
        |  MATCH (n)
        |  RETURN n
        |}
        |RETURN n
      """.stripMargin,
      """MATCH (n)
        |  WHERE EXISTS {
        |  MATCH (n)
        |  RETURN n AS n
        |}
        |RETURN n AS n
      """.stripMargin
    )
  }

  test("ensure variables are aliased for SHOW PRIVILEGES") {
    assertRewrite(
      "SHOW PRIVILEGES YIELD role",
      "SHOW PRIVILEGES YIELD role AS role"
    )
  }

  test("ensure variables are aliased for SHOW USER PRIVILEGES with WHERE") {
    assertRewriteAndSemanticError(
      "SHOW USER neo4j PRIVILEGES YIELD access, resource WHERE role = 'PUBLIC'",
      "SHOW USER neo4j PRIVILEGES YIELD access AS access, resource AS resource WHERE role = 'PUBLIC'",
      "Variable `role` not defined (line 1, column 57 (offset: 56))"
    )
  }

  test("ensure variables are aliased for SHOW USER PRIVILEGES with ORDER BY") {
    assertRewriteAndSemanticError(
      "SHOW USER neo4j PRIVILEGES YIELD access, resource ORDER BY role",
      "SHOW USER neo4j PRIVILEGES YIELD access AS access, resource AS resource ORDER BY role",
      "Variable `role` not defined (line 1, column 60 (offset: 59))"
    )
  }

  test("ensure variables are aliased for SHOW USER") {
    assertRewrite(
      "SHOW USERS YIELD user",
      "SHOW USERS YIELD user AS user"
    )
    assertRewrite(
      "SHOW USERS WITH AUTH YIELD user, provider AS authProvider",
      "SHOW USERS WITH AUTH YIELD user AS user, provider AS authProvider"
    )
  }

  test("ensure variables are aliased for SHOW ROLES") {
    assertRewrite(
      "SHOW ROLES YIELD role",
      "SHOW ROLES YIELD role AS role"
    )
  }

  test("ensure variables are aliased for SHOW DATABASES") {
    assertRewrite(
      "SHOW DATABASES YIELD name",
      "SHOW DATABASES YIELD name AS name"
    )
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
      """.stripMargin
    )
  }

  test("RETURN: Existing alias's should not be used within scoped expressions, list comprehension") {
    assertRewrite(
      """MATCH ()
        |RETURN true AS var0
        |ORDER BY 1 IN [var0 IN [1,2] WHERE true]
      """.stripMargin,
      """MATCH ()
        |RETURN true AS var0
        |ORDER BY 1 IN [var0 IN [1,2] WHERE true]
      """.stripMargin
    )
  }

  test("RETURN: Existing alias's should not be used within scoped expressions, any iterable predicate expression") {
    assertRewrite(
      """MATCH ()
        |RETURN true AS var0
        |ORDER BY any(var0 IN [1, 2] WHERE true)
      """.stripMargin,
      """MATCH ()
        |RETURN true AS var0
        |ORDER BY any(var0 IN [1, 2] WHERE true)
      """.stripMargin
    )
  }

  test("RETURN: Existing alias's should not be used within scoped expressions, none iterable predicate expression") {
    assertRewrite(
      """MATCH ()
        |RETURN true AS var0
        |ORDER BY none(var0 IN [1, 2] WHERE true)
      """.stripMargin,
      """MATCH ()
        |RETURN true AS var0
        |ORDER BY none(var0 IN [1, 2] WHERE true)
      """.stripMargin
    )
  }

  test("WITH: Existing alias's should not be used within scoped expressions, list comprehension") {
    assertRewrite(
      """MATCH ()
        |WITH true AS var0
        |ORDER BY 1 IN [var0 IN [1,2] WHERE true]
        |RETURN var0
      """.stripMargin,
      """MATCH ()
        |WITH true AS var0
        |ORDER BY 1 IN [var0 IN [1,2] WHERE true]
        |RETURN var0 AS var0
      """.stripMargin
    )
  }

  test("WITH: Existing alias's should not be used within scoped expressions, any iterable predicate expression") {
    assertRewrite(
      """MATCH ()
        |RETURN true AS var0
        |ORDER BY any(var0 IN [1, 2] WHERE true)
      """.stripMargin,
      """MATCH ()
        |RETURN true AS var0
        |ORDER BY any(var0 IN [1, 2] WHERE true)
      """.stripMargin
    )
  }

  test("WITH: Existing alias's should not be used within scoped expressions, none iterable predicate expression") {
    assertRewrite(
      """MATCH ()
        |RETURN true AS var0
        |ORDER BY none(var0 IN [1, 2] WHERE true)
      """.stripMargin,
      """MATCH ()
        |RETURN true AS var0
        |ORDER BY none(var0 IN [1, 2] WHERE true)
      """.stripMargin
    )
  }

  test("WITH: attach ORDER BY expressions to existing aliases inside EXISTS subqueries") {
    assertRewrite(
      """RETURN EXISTS {
        | MATCH (n)
        | WITH n.prop AS prop
        | ORDER BY n.prop RETURN prop
        | } AS exists
        |""".stripMargin,
      """RETURN EXISTS {
        | MATCH (n)
        | WITH n.prop AS prop
        | ORDER BY prop RETURN prop AS prop
        | } AS exists
        |""".stripMargin
    )
  }

  test("RETURN: attach ORDER BY expressions to existing aliases") {
    assertRewrite(
      """MATCH (n)
        |RETURN n.prop AS prop ORDER BY n.prop
      """.stripMargin,
      """MATCH (n)
        |RETURN n.prop AS prop ORDER BY prop
      """.stripMargin
    )
  }

  test("attach WHERE expression to existing aliases") {
    assertRewrite(
      """MATCH (n)
        |WITH size(n.prop) > 10 AS result WHERE size(n.prop) > 10
        |RETURN result
      """.stripMargin,
      """MATCH (n)
        |WITH size(n.prop) > 10 AS result WHERE result
        |RETURN result AS result
      """.stripMargin
    )
  }

  test("should rewrite even if contains aliased variable if it not a redefinition") {
    assertRewrite(
      """MATCH (n)
        |RETURN n AS n, n.prop AS `n.prop`
        |ORDER BY n.foo, n.prop * 2 DESC
      """.stripMargin,
      """MATCH (n)
        |RETURN n AS n, n.prop AS `n.prop`
        |ORDER BY n.foo, `n.prop` * 2 DESC
      """.stripMargin
    )
  }

  test("does not introduce aliases for ORDER BY with existing alias") {
    assertIsNotRewritten(
      """MATCH (n)
        |WITH n.prop AS prop ORDER BY prop
        |RETURN prop AS prop
      """.stripMargin
    )
  }

  test("does not rewrite aliases which may be redefined in same WITH") {
    assertIsNotRewritten(
      """WITH -0.5 as pa0
        |WITH 1 AS pa0, pa0 as pa1
        |WHERE -1 = pa0
        |RETURN pa0 AS pa3
      """.stripMargin
    )
  }

  test("does not rewrite aliases which may be redefined even when wrapped with a negation") {
    assertIsNotRewritten(
      """WITH -0.5 as pa0
        |WITH 1 AS pa0, -pa0 as pa1
        |WHERE -1 = -pa0
        |RETURN pa0 AS pa3
      """.stripMargin
    )
  }

  test("does not introduce aliases for WHERE with existing alias") {
    assertIsNotRewritten(
      """MATCH (n)
        |WITH n.prop AS prop WHERE prop
        |RETURN prop AS prop
      """.stripMargin
    )
  }

  test("WITH: does not introduce aliases for ORDER BY expressions that depend on existing variables") {
    assertRewrite(
      """MATCH (n)
        |WITH n ORDER BY size(n.prop)
        |RETURN n
      """.stripMargin,
      """MATCH (n)
        |WITH n AS n ORDER BY size(n.prop)
        |RETURN n AS n
      """.stripMargin
    )
  }

  test("RETURN: does not introduce aliases for ORDER BY expressions that depend on existing variables") {
    assertRewrite(
      """MATCH (n)
        |RETURN n ORDER BY size(n.prop)
      """.stripMargin,
      """MATCH (n)
        |RETURN n AS n ORDER BY size(n.prop)
      """.stripMargin
    )
  }

  test("does not introduce aliases for WHERE expressions that depend on existing variables") {
    assertRewrite(
      """MATCH (n)
        |WITH n WHERE size(n.prop) > 10
        |RETURN n
      """.stripMargin,
      """MATCH (n)
        |WITH n AS n WHERE size(n.prop) > 10
        |RETURN n AS n
      """.stripMargin
    )
  }

  test("WITH: introduces aliases for ORDER BY expressions that depend on existing aliases") {
    assertRewrite(
      """MATCH (n)
        |WITH n.prop AS prop ORDER BY size(n.prop)
        |RETURN prop
      """.stripMargin,
      """MATCH (n)
        |WITH n.prop AS prop ORDER BY size(prop)
        |RETURN prop AS prop
      """.stripMargin
    )
  }

  test("RETURN: introduces aliases for ORDER BY expressions that depend on existing aliases") {
    assertRewrite(
      """MATCH (n)
        |RETURN n.prop AS prop ORDER BY size(n.prop)
      """.stripMargin,
      """MATCH (n)
        |RETURN n.prop AS prop ORDER BY size(prop)
      """.stripMargin
    )
  }

  test("introduces aliases for WHERE expressions that depend on existing aliases") {
    assertRewrite(
      """MATCH (n)
        |WITH n.prop AS prop WHERE size(n.prop) > 10
        |RETURN prop
      """.stripMargin,
      """MATCH (n)
        |WITH n.prop AS prop WHERE size(prop) > 10
        |RETURN prop AS prop
      """.stripMargin
    )
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
      """.stripMargin
    )
  }

  test("RETURN: renames variables for ORDER BY expressions that depend on existing aliases") {
    assertRewrite(
      """MATCH (n)
        |RETURN n AS m ORDER BY n
      """.stripMargin,
      """MATCH (n)
        |RETURN n AS m ORDER BY m
      """.stripMargin
    )
  }

  test("WITH: does not rename variables for ORDER BY from RHS of AS, if they also exist on LHS of AS") {
    assertIsNotRewritten(
      """MATCH (x), (y)
        |WITH x AS y, y as z
        |ORDER BY y
        |RETURN y AS y, z AS z
        |""".stripMargin
    )
  }

  test("RETURN: does not rename variables for ORDER BY from RHS of AS, if they also exist on LHS of AS") {
    assertIsNotRewritten(
      """MATCH (x), (y)
        |RETURN x AS y, y as z
        |ORDER BY y
        |""".stripMargin
    )
  }

  test(
    "WITH: does not rename variables for ORDER BY from RHS of AS, if they also exist on LHS of AS. Expression in ORDER BY."
  ) {
    assertIsNotRewritten(
      """MATCH (x), (y)
        |WITH x AS y, y as z
        |ORDER BY foo(y)
        |RETURN y AS y, z AS z
        |""".stripMargin
    )
  }

  test(
    "RETURN: does not rename variables for ORDER BY from RHS of AS, if they also exist on LHS of AS. Expression in ORDER BY."
  ) {
    assertIsNotRewritten(
      """MATCH (x), (y)
        |RETURN x AS y, y as z
        |ORDER BY foo(y)
        |""".stripMargin
    )
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
        |""".stripMargin
    )
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
        |""".stripMargin
    )
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
      """.stripMargin
    )
  }

  test("WITH: introduces aliases for complex ORDER BY expressions that depend on existing aliases") {
    assertRewrite(
      """MATCH (n)
        |WITH n.prop AS prop ORDER BY size(n.prop[0])
        |RETURN prop
      """.stripMargin,
      """MATCH (n)
        |WITH n.prop AS prop ORDER BY size(prop[0])
        |RETURN prop AS prop
      """.stripMargin
    )
  }

  test("RETURN: introduces aliases for complex ORDER BY expressions that depend on existing aliases") {
    assertRewrite(
      """MATCH (n)
        |RETURN n.prop AS prop ORDER BY size(n.prop[0])
      """.stripMargin,
      """MATCH (n)
        |RETURN n.prop AS prop ORDER BY size(prop[0])
      """.stripMargin
    )
  }

  test("introduces aliases for complex WHERE expressions that depend on existing aliases") {
    assertRewrite(
      """MATCH (n)
        |WITH n.prop AS prop WHERE size(n.prop[0]) > 10
        |RETURN prop
      """.stripMargin,
      """MATCH (n)
        |WITH n.prop AS prop WHERE size(prop[0]) > 10
        |RETURN prop AS prop
      """.stripMargin
    )
  }

  test("does not introduce variables for ORDER BY expressions that depend on non-aliased variables") {
    assertIsNotRewritten(
      """MATCH (n)
        |WITH n.prop AS prop ORDER BY n.foo DESC
        |RETURN prop AS prop
      """.stripMargin
    )
  }

  test("does not introduce variables for WHERE expressions that depend on non-aliased variables") {
    assertIsNotRewritten(
      """MATCH (n)
        |WITH n.prop AS prop WHERE n.foo > 10
        |RETURN prop AS prop
      """.stripMargin
    )
  }

  test("does not introduce variables for ORDER BY expressions that depend on non-aliased variables in WITH *") {
    assertIsNotRewritten(
      """MATCH (n)
        |WITH *, n.prop AS prop ORDER BY n.foo DESC
        |RETURN prop AS prop
      """.stripMargin
    )
  }

  test("does not introduce variables for WHERE expressions that depend on non-aliased variables in WITH *") {
    assertIsNotRewritten(
      """MATCH (n)
        |WITH *, n.prop AS prop WHERE n.foo > 10
        |RETURN prop AS prop
      """.stripMargin
    )
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
      """.stripMargin
    )
  }

  test("RETURN: introduces variables for ORDER BY expressions that depend on existing aliases in WITH *") {
    assertRewrite(
      """MATCH (n)
        |RETURN *, n.prop AS prop ORDER BY n.prop DESC
      """.stripMargin,
      """MATCH (n)
        |RETURN *, n.prop AS prop ORDER BY prop DESC
      """.stripMargin
    )
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
      """.stripMargin
    )
  }

  test("WITH: does not attach ORDER BY expressions to unaliased items") {
    // Note: unaliased items in WITH are invalid, and will be caught during semantic check
    assertNotRewrittenAndSemanticErrors(
      """MATCH (n)
        |WITH n.prop ORDER BY n.prop
        |RETURN prop AS prop
      """.stripMargin,
      "Expression in WITH must be aliased (use AS) (line 2, column 6 (offset: 15))"
    )
  }

  test("should not introduce aliases in subquery return") {
    assertNotRewrittenAndSemanticErrors(
      "CALL { RETURN 1 } RETURN 1 AS one",
      "Expression in CALL { RETURN ... } must be aliased (use AS) (line 1, column 15 (offset: 14))"
    )
  }

  test("should not introduce aliases in union subquery return") {
    assertNotRewrittenAndSemanticErrors(
      "CALL { RETURN 1 UNION RETURN 1 } RETURN 1 AS one",
      "Expression in CALL { RETURN ... } must be aliased (use AS) (line 1, column 15 (offset: 14))",
      "Expression in CALL { RETURN ... } must be aliased (use AS) (line 1, column 30 (offset: 29))"
    )
  }

  test("should not introduce aliases in correlated subquery return") {
    assertNotRewrittenAndSemanticErrors(
      "MATCH (n) CALL { WITH n AS n RETURN 1 } RETURN 1 AS one",
      "Expression in CALL { RETURN ... } must be aliased (use AS) (line 1, column 37 (offset: 36))"
    )
  }

  test("should not introduce aliases in correlated union subquery return") {
    assertNotRewrittenAndSemanticErrors(
      "MATCH (n) CALL { WITH n AS n RETURN 1 UNION WITH n AS n RETURN 1 } RETURN 1 AS one",
      "Expression in CALL { RETURN ... } must be aliased (use AS) (line 1, column 37 (offset: 36))",
      "Expression in CALL { RETURN ... } must be aliased (use AS) (line 1, column 64 (offset: 63))"
    )
  }

  test("RETURN: attaches ORDER BY expressions to unaliased items") {
    assertRewrite(
      """MATCH (n)
        |RETURN n.prop ORDER BY n.prop
      """.stripMargin,
      """MATCH (n)
        |RETURN n.prop AS `n.prop` ORDER BY `n.prop`
      """.stripMargin
    )
  }

  test("RETURN: aliases complex expression") {
    assertRewrite(
      """MATCH (n)
        |RETURN foo(n.prop[0])
      """.stripMargin,
      """MATCH (n)
        |RETURN foo(n.prop[0]) AS `foo(n.prop[0])`
      """.stripMargin
    )
  }

  test("does not attach WHERE expression to unaliased items") {
    // Note: unaliased items in WITH are invalid, and will be caught during semantic check
    assertNotRewrittenAndSemanticErrors(
      """MATCH (n)
        |WITH n.prop WHERE n.prop
        |RETURN prop AS prop
      """.stripMargin,
      "Expression in WITH must be aliased (use AS) (line 2, column 6 (offset: 15))"
    )
  }

  test("rejects use of aggregation in ORDER BY if aggregation is not used in associated WITH") {
    // Note: aggregations in ORDER BY that don't also appear in WITH are invalid
    try {
      rewrite(parseForRewriting(
        """MATCH (n)
          |WITH n.prop AS prop ORDER BY max(n.foo)
          |RETURN prop
        """.stripMargin
      ))
      fail("We shouldn't get here")
    } catch {
      case e: SyntaxException =>
        e.getMessage should equal(
          "Cannot use aggregation in ORDER BY if there are no aggregate expressions in the preceding WITH (line 2, column 1 (offset: 10))"
        )
    }
  }

  test("rejects use of aggregation in ORDER BY if aggregation is not used in associated RETURN") {
    // Note: aggregations in ORDER BY that don't also appear in WITH are invalid
    try {
      rewrite(parseForRewriting(
        """MATCH (n)
          |RETURN n.prop AS prop ORDER BY max(n.foo)
        """.stripMargin
      ))
      fail("We shouldn't get here")
    } catch {
      case e: SyntaxException =>
        e.getMessage should equal(
          "Cannot use aggregation in ORDER BY if there are no aggregate expressions in the preceding RETURN (line 2, column 1 (offset: 10))"
        )
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
    assertNotRewrittenAndSemanticErrors(
      """MATCH (n)
        |WITH n.prop AS prop WHERE max(n.foo)
        |RETURN prop AS prop
      """.stripMargin,
      "Invalid use of aggregating function max(...) in this context (line 2, column 27 (offset: 36))"
    )
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
      """.stripMargin
    )
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
      """.stripMargin
    )
  }

  test("RETURN: preserves SKIP and LIMIT with ORDER BY expressions") {
    assertRewrite(
      """MATCH (n)
        |RETURN n ORDER BY n.prop SKIP 5 LIMIT 2
      """.stripMargin,
      """MATCH (n)
        |RETURN n AS n ORDER BY n.prop SKIP 5 LIMIT 2
      """.stripMargin
    )
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
      """.stripMargin
    )
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
      """.stripMargin
    )
  }

  test("RETURN: preserves DISTINCT when replacing ORDER BY expressions with alias") {
    assertRewrite(
      """MATCH (n)
        |RETURN DISTINCT n.prop AS prop ORDER BY n.prop
      """.stripMargin,
      """MATCH (n)
        |RETURN DISTINCT n.prop AS prop ORDER BY prop
      """.stripMargin
    )
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
      """.stripMargin
    )
  }

  test("WITH: aggregating: does not change grouping set when introducing aliases for ORDER BY") {
    assertRewrite(
      """MATCH (n)
        |WITH DISTINCT n.prop AS prop ORDER BY size(n.prop)
        |RETURN prop
      """.stripMargin,
      """MATCH (n)
        |WITH DISTINCT n.prop AS prop ORDER BY size(prop)
        |RETURN prop AS prop
      """.stripMargin
    )

    assertRewrite(
      """MATCH (n)
        |WITH n.prop AS prop, count(*) AS count ORDER BY size(n.prop)
        |RETURN prop, count
      """.stripMargin,
      """MATCH (n)
        |WITH n.prop AS prop, count(*) AS count ORDER BY size(prop)
        |RETURN prop AS prop, count AS count
      """.stripMargin
    )
  }

  test("RETURN: aggregating: does not change grouping set when introducing aliases for ORDER BY") {
    assertRewrite(
      """MATCH (n)
        |RETURN DISTINCT n.prop AS prop ORDER BY size(n.prop)
      """.stripMargin,
      """MATCH (n)
        |RETURN DISTINCT n.prop AS prop ORDER BY size(prop)
      """.stripMargin
    )

    assertRewrite(
      """MATCH (n)
        |RETURN n.prop AS prop, count(*) AS count ORDER BY size(n.prop)
      """.stripMargin,
      """MATCH (n)
        |RETURN n.prop AS prop, count(*) AS count ORDER BY size(prop)
      """.stripMargin
    )
  }

  test("aggregating: does not change grouping set when introducing aliases for WHERE") {
    assertRewrite(
      """MATCH (n)
        |WITH DISTINCT n.prop AS prop WHERE size(n.prop) = 1
        |RETURN prop
      """.stripMargin,
      """MATCH (n)
        |WITH DISTINCT n.prop AS prop WHERE size(prop) = 1
        |RETURN prop AS prop
      """.stripMargin
    )

    assertRewrite(
      """MATCH (n)
        |WITH n.prop AS prop, count(*) AS count WHERE size(n.prop) = 1
        |RETURN prop, count
      """.stripMargin,
      """MATCH (n)
        |WITH n.prop AS prop, count(*) AS count WHERE size(prop) = 1
        |RETURN prop AS prop, count AS count
      """.stripMargin
    )
  }

  test(
    "WITH: aggregating: does not change grouping set when introducing aliases for ORDER BY with non-grouping expression"
  ) {
    // Note: using a non-grouping expression for ORDER BY when aggregating is invalid, and will be caught during semantic check
    assertNotRewrittenAndSemanticErrors(
      """MATCH (n)
        |WITH DISTINCT n.prop AS prop ORDER BY n.foo
        |RETURN prop AS prop
      """.stripMargin,
      "In a WITH/RETURN with DISTINCT or an aggregation, it is not possible to access variables declared before the WITH/RETURN: n (line 2, column 39 (offset: 48))"
    )

    assertNotRewrittenAndSemanticErrors(
      """MATCH (n)
        |WITH n.prop AS prop, collect(n.foo) AS foos ORDER BY n.foo
        |RETURN prop AS prop, foos AS foos
      """.stripMargin,
      "In a WITH/RETURN with DISTINCT or an aggregation, it is not possible to access variables declared before the WITH/RETURN: n (line 2, column 54 (offset: 63))"
    )
  }

  test(
    "RETURN: aggregating: does not change grouping set when introducing aliases for ORDER BY with non-grouping expression"
  ) {
    // Note: using a non-grouping expression for ORDER BY when aggregating is invalid, and will be caught during semantic check
    assertNotRewrittenAndSemanticErrors(
      """MATCH (n)
        |RETURN DISTINCT n.prop AS prop ORDER BY n.foo
      """.stripMargin,
      "In a WITH/RETURN with DISTINCT or an aggregation, it is not possible to access variables declared before the WITH/RETURN: n (line 2, column 41 (offset: 50))"
    )

    assertNotRewrittenAndSemanticErrors(
      """MATCH (n)
        |RETURN n.prop AS prop, collect(n.foo) AS foos ORDER BY n.foo
      """.stripMargin,
      "In a WITH/RETURN with DISTINCT or an aggregation, it is not possible to access variables declared before the WITH/RETURN: n (line 2, column 56 (offset: 65))"
    )
  }

  test("aggregating: does not change grouping set when introducing aliases for WHERE with non-grouping expression") {
    // Note: using a non-grouping expression for ORDER BY when aggregating is invalid, and will be caught during semantic check
    assertNotRewrittenAndSemanticErrors(
      """MATCH (n)
        |WITH DISTINCT n.prop AS prop WHERE n.foo
        |RETURN prop AS prop
      """.stripMargin,
      "In a WITH/RETURN with DISTINCT or an aggregation, it is not possible to access variables declared before the WITH/RETURN: n (line 2, column 36 (offset: 45))"
    )

    assertNotRewrittenAndSemanticErrors(
      """MATCH (n)
        |WITH n.prop AS prop, collect(n.foo) AS foos WHERE n.foo
        |RETURN prop AS prop, foos AS foos
      """.stripMargin,
      "In a WITH/RETURN with DISTINCT or an aggregation, it is not possible to access variables declared before the WITH/RETURN: n (line 2, column 51 (offset: 60))"
    )
  }

  // Below: unordered, exploratory tests

  test("MATCH (u)-[r1]->(v) WITH r1 AS r2, rand() AS c ORDER BY c MATCH (a)-[r2]->(b) RETURN r2 AS rel") {
    assertIsNotRewritten(
      """MATCH (u)-[r1]->(v)
        |WITH r1 AS r2, rand() AS c ORDER BY c
        |MATCH (a)-[r2]->(b)
        |RETURN r2 AS rel
      """.stripMargin
    )
  }

  test("MATCH (foo) WITH $meh AS x ORDER BY x.prop DESC LIMIT 4 RETURN x") {
    assertIsNotRewritten(
      """MATCH (foo)
        |WITH $meh AS x ORDER BY x.prop DESC LIMIT 4
        |RETURN x AS x
      """.stripMargin
    )
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
      """.stripMargin
    )
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
      """.stripMargin
    )
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
      """.stripMargin
    )
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

  test(
    "MATCH (n) WHERE id(n) IN [0,1,2,3] WITH n.division AS div, max(n.age) AS age order by max(n.age) RETURN div, age"
  ) {
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
      """.stripMargin
    )
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
      """.stripMargin
    )
  }

  test("MATCH (n) WITH n AS n ORDER BY max(n) RETURN n") {
    a[SyntaxException] shouldBe thrownBy {
      rewriting("MATCH (n) WITH n AS n ORDER BY max(n) RETURN n")
    }
  }

  private def rewrite(originalQuery: String, expectedQuery: String): SemanticCheckResult = {
    val original = parseForRewriting(originalQuery.replace("\r\n", "\n"))
    val expected = parseForRewriting(expectedQuery.replace("\r\n", "\n"))
    val result = endoRewrite(original)
    assert(
      result === expected,
      s"""
    $originalQuery
    should be rewritten to:
    $expectedQuery
    but was rewritten to:${prettifier.asString(result)}"""
    )
    result.semanticCheck.run(
      SemanticState.clean.withFeatures(MultipleDatabases),
      SemanticCheckContext.default
    )
  }

  override protected def assertRewrite(originalQuery: String, expectedQuery: String): Unit = {
    val checkResult = rewrite(originalQuery, expectedQuery)
    assert(checkResult.errors === Seq())
  }

  protected def assertRewriteAndSemanticError(
    originalQuery: String,
    expectedQuery: String,
    semanticErrors: String*
  ): Unit = {
    val checkResult = rewrite(originalQuery, expectedQuery)
    val errors = checkResult.errors.map(error => s"${error.msg} (${error.position})").toSet
    semanticErrors.foreach(msg =>
      assert(errors contains msg, s"Error '$msg' not produced (errors: $errors)}")
    )
  }

  protected def assertNotRewrittenAndSemanticErrors(query: String, semanticErrors: String*): Unit = {
    assertRewriteAndSemanticError(query, query, semanticErrors: _*)
  }

  protected def rewriting(queryText: String): Unit = {
    endoRewrite(parseForRewriting(queryText))
  }
}
