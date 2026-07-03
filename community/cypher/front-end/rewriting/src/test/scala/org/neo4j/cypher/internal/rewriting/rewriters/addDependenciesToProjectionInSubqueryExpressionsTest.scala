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
package org.neo4j.cypher.internal.rewriting.rewriters

import org.neo4j.cypher.internal.CypherVersionHelpers
import org.neo4j.cypher.internal.ast.AddedInRewriteGeneral
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.With
import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.ast.prettifier.Prettifier
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.rewriting.AstRewritingTestSupport
import org.neo4j.cypher.internal.rewriting.rewriters.astRewriters.AddDependenciesToProjectionsInSubqueryExpressions
import org.neo4j.cypher.internal.rewriting.rewriters.preparatoryRewriters.NormalizeWithAndReturnClauses
import org.neo4j.cypher.internal.util.Neo4jCypherExceptionFactory
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.bottomUp
import org.neo4j.cypher.internal.util.inSequence
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.test_helpers.TestName

class addDependenciesToProjectionInSubqueryExpressionsTest
    extends CypherFunSuite
    with TestName
    with AstRewritingTestSupport {

  test("""WITH "Bosse" as x
         |MATCH (person:Person)
         |WHERE EXISTS {
         | WITH "Ozzy" AS y
         | MATCH (person)-[:HAS_DOG]->(d:Dog)
         | WHERE d.name = y
         | RETURN person AS person
         |}
         |RETURN person.name AS name""".stripMargin) {
    assertRewrite(
      testName,
      """WITH "Bosse" as x
        |MATCH (person:Person)
        |WHERE EXISTS {
        | WITH "Ozzy" AS y, person AS person
        | MATCH (person)-[:HAS_DOG]->(d:Dog)
        | WHERE d.name = y
        | RETURN person AS person
        |}
        |RETURN person.name AS name""".stripMargin
    )
  }

  test("""MATCH (person:Person)
         |WHERE EXISTS {
         | MATCH (person)-[:HAS_DOG]->(d:Dog)
         | WHERE EXISTS {
         |  WITH "Ozzy" as x
         |  MATCH (person)-[:HAS_DOG]->(d:Dog)
         |  WHERE d.name = x
         |  RETURN person AS person
         | }
         | RETURN person AS person
         |}
         |RETURN person.name AS name""".stripMargin) {
    assertRewrite(
      testName,
      """MATCH (person:Person)
        |WHERE EXISTS {
        | MATCH (person)-[:HAS_DOG]->(d:Dog)
        | WHERE EXISTS {
        |  WITH "Ozzy" as x, d AS d, person AS person
        |  MATCH (person)-[:HAS_DOG]->(d:Dog)
        |  WHERE d.name = x
        |  RETURN person AS person
        | }
        | RETURN person AS person
        |}
        |RETURN person.name AS name""".stripMargin
    )
  }

  test("""MATCH (person:Person)
         |WHERE EXISTS {
         | MATCH (person)-[:HAS_DOG]->(d:Dog)
         | WHERE EXISTS {
         |  WITH "Ozzy" as x
         |  MATCH (person)-[:HAS_DOG]->(dog1:Dog)
         |  WHERE dog1.name = x
         |  WITH "Bosse" as x
         |  MATCH (dog1)-[:HAS_FRIEND]->(dog2:Dog)
         |  WHERE dog2.name = x
         |  RETURN person AS person
         | }
         | RETURN person AS person
         |}
         |RETURN person.name AS name""".stripMargin) {
    assertRewrite(
      testName,
      """MATCH (person:Person)
        |WHERE EXISTS {
        | MATCH (person)-[:HAS_DOG]->(d:Dog)
        | WHERE EXISTS {
        |  WITH "Ozzy" as x, person AS person
        |  MATCH (person)-[:HAS_DOG]->(dog1:Dog)
        |  WHERE dog1.name = x
        |  WITH "Bosse" as x, person AS person
        |  MATCH (dog1)-[:HAS_FRIEND]->(dog2:Dog)
        |  WHERE dog2.name = x
        |  RETURN person AS person
        | }
        | RETURN person AS person
        |}
        |RETURN person.name AS name""".stripMargin
    )
  }

  test("""MATCH (person:Person)
         |WHERE EXISTS {
         | MATCH (person)-[:HAS_DOG]->(d:Dog)
         | WHERE EXISTS {
         |  WITH "Ozzy" AS dogName
         |  MATCH (person)-[:HAS_DOG]->(dog:Dog)
         |  WHERE dog.name = dogName
         |  RETURN dog AS pet
         |  UNION
         |  WITH "Sylvester" AS catName
         |  MATCH (person)-[:HAS_CAT]->(cat:Cat)
         |  WHERE cat.name = catName
         |  RETURN cat AS pet
         | }
         | RETURN person AS person
         |}
         |RETURN person.name AS name""".stripMargin) {
    assertRewrite(
      testName,
      """MATCH (person:Person)
        |WHERE EXISTS {
        | MATCH (person)-[:HAS_DOG]->(d:Dog)
        | WHERE EXISTS {
        |  WITH "Ozzy" AS dogName, person AS person
        |  MATCH (person)-[:HAS_DOG]->(dog:Dog)
        |  WHERE dog.name = dogName
        |  RETURN dog AS pet
        |  UNION
        |  WITH "Sylvester" AS catName, person AS person
        |  MATCH (person)-[:HAS_CAT]->(cat:Cat)
        |  WHERE cat.name = catName
        |  RETURN cat AS pet
        | }
        | RETURN person AS person
        |}
        |RETURN person.name AS name""".stripMargin
    )
  }

  test("""MATCH (person:Person)
         |WHERE EXISTS {
         | MATCH (person)-[:HAS_DOG]->(d:Dog)
         | WHERE EXISTS {
         |  WITH "Ozzy" AS dogName
         |  MATCH (person)-[:HAS_DOG]->(dog:Dog)
         |  WHERE dog.name = dogName
         |  RETURN dog AS pet
         |  UNION ALL
         |  WITH "Sylvester" AS catName
         |  MATCH (person)-[:HAS_CAT]->(cat:Cat)
         |  WHERE cat.name = catName
         |  RETURN cat AS pet
         | }
         | RETURN person AS person
         |}
         |RETURN person.name AS name""".stripMargin) {
    assertRewrite(
      testName,
      """MATCH (person:Person)
        |WHERE EXISTS {
        | MATCH (person)-[:HAS_DOG]->(d:Dog)
        | WHERE EXISTS {
        |  WITH "Ozzy" AS dogName, person AS person
        |  MATCH (person)-[:HAS_DOG]->(dog:Dog)
        |  WHERE dog.name = dogName
        |  RETURN dog AS pet
        |  UNION ALL
        |  WITH "Sylvester" AS catName, person AS person
        |  MATCH (person)-[:HAS_CAT]->(cat:Cat)
        |  WHERE cat.name = catName
        |  RETURN cat AS pet
        | }
        | RETURN person AS person
        |}
        |RETURN person.name AS name""".stripMargin
    )
  }

  test("""MATCH (person:Person)
         |WHERE EXISTS {
         | MATCH (person)-[:HAS_DOG]->(d:Dog)
         | RETURN person AS person
         |}
         |RETURN person.name AS name""".stripMargin) {
    assertIsNotRewritten(testName)
  }

  test("""MATCH (person:Person)
         |WHERE EXISTS {
         | MATCH (person)-[:HAS_DOG]->(d:Dog)
         | CALL {
         |  WITH d AS d
         |  MATCH (d)-[:HAS_FRIEND]-(f:Dog)
         |  RETURN d AS d
         | }
         | RETURN person AS person
         |}
         |RETURN person.name AS name""".stripMargin) {
    assertIsNotRewritten(testName)
  }

  test("""MATCH (person:Person)
         |WHERE EXISTS {
         | MATCH (person)-[:HAS_DOG]->(d:Dog)
         | WHERE EXISTS {
         |  MATCH (person)-[:HAS_DOG]->(dog:Dog)
         |  RETURN dog AS pet
         |  UNION
         |  MATCH (person)-[:HAS_CAT]->(cat:Cat)
         |  RETURN cat AS pet
         | }
         | RETURN person AS person
         |}
         |RETURN person.name AS name""".stripMargin) {
    assertIsNotRewritten(testName)
  }

  test("""WITH "Bosse" as x
         |MATCH (person:Person)
         |WHERE COUNT {
         | WITH "Ozzy" AS y
         | MATCH (person)-[:HAS_DOG]->(d:Dog)
         | WHERE d.name = y
         | RETURN person AS person
         |} > 2
         |RETURN person.name AS name""".stripMargin) {
    assertRewrite(
      testName,
      """WITH "Bosse" as x
        |MATCH (person:Person)
        |WHERE COUNT {
        | WITH "Ozzy" AS y, person AS person
        | MATCH (person)-[:HAS_DOG]->(d:Dog)
        | WHERE d.name = y
        | RETURN person AS person
        |} > 2
        |RETURN person.name AS name""".stripMargin
    )
  }

  test("""MATCH (person:Person)
         |WHERE COUNT {
         | MATCH (person)-[:HAS_DOG]->(d:Dog)
         | WHERE COUNT {
         |  WITH "Ozzy" as x
         |  MATCH (person)-[:HAS_DOG]->(d:Dog)
         |  WHERE d.name = x
         |  RETURN person AS person
         | } > 2
         | RETURN person AS person
         |} > 2
         |RETURN person.name AS name""".stripMargin) {
    assertRewrite(
      testName,
      """MATCH (person:Person)
        |WHERE COUNT {
        | MATCH (person)-[:HAS_DOG]->(d:Dog)
        | WHERE COUNT {
        |  WITH "Ozzy" as x, d AS d, person AS person
        |  MATCH (person)-[:HAS_DOG]->(d:Dog)
        |  WHERE d.name = x
        |  RETURN person AS person
        | } > 2
        | RETURN person AS person
        |} > 2
        |RETURN person.name AS name""".stripMargin
    )
  }

  test("""MATCH (person:Person)
         |WHERE COUNT {
         | MATCH (person)-[:HAS_DOG]->(d:Dog)
         | WHERE COUNT {
         |  WITH "Ozzy" as x
         |  MATCH (person)-[:HAS_DOG]->(dog1:Dog)
         |  WHERE dog1.name = x
         |  WITH "Bosse" as x
         |  MATCH (dog1)-[:HAS_FRIEND]->(dog2:Dog)
         |  WHERE dog2.name = x
         |  RETURN person AS person
         | } > 2
         | RETURN person AS person
         |} > 2
         |RETURN person.name AS name""".stripMargin) {
    assertRewrite(
      testName,
      """MATCH (person:Person)
        |WHERE COUNT {
        | MATCH (person)-[:HAS_DOG]->(d:Dog)
        | WHERE COUNT {
        |  WITH "Ozzy" as x, person AS person
        |  MATCH (person)-[:HAS_DOG]->(dog1:Dog)
        |  WHERE dog1.name = x
        |  WITH "Bosse" as x, person AS person
        |  MATCH (dog1)-[:HAS_FRIEND]->(dog2:Dog)
        |  WHERE dog2.name = x
        |  RETURN person AS person
        | } > 2
        | RETURN person AS person
        |} > 2
        |RETURN person.name AS name""".stripMargin
    )
  }

  test("""MATCH (person:Person)
         |WHERE COUNT {
         | MATCH (person)-[:HAS_DOG]->(d:Dog)
         | WHERE COUNT {
         |  WITH "Ozzy" AS dogName
         |  MATCH (person)-[:HAS_DOG]->(dog:Dog)
         |  WHERE dog.name = dogName
         |  RETURN dog AS pet
         |  UNION
         |  WITH "Sylvester" AS catName
         |  MATCH (person)-[:HAS_CAT]->(cat:Cat)
         |  WHERE cat.name = catName
         |  RETURN cat AS pet
         | } > 2
         | RETURN person AS person
         |} > 2
         |RETURN person.name AS name""".stripMargin) {
    assertRewrite(
      testName,
      """MATCH (person:Person)
        |WHERE COUNT {
        | MATCH (person)-[:HAS_DOG]->(d:Dog)
        | WHERE COUNT {
        |  WITH "Ozzy" AS dogName, person AS person
        |  MATCH (person)-[:HAS_DOG]->(dog:Dog)
        |  WHERE dog.name = dogName
        |  RETURN dog AS pet
        |  UNION
        |  WITH "Sylvester" AS catName, person AS person
        |  MATCH (person)-[:HAS_CAT]->(cat:Cat)
        |  WHERE cat.name = catName
        |  RETURN cat AS pet
        | } > 2
        | RETURN person AS person
        |} > 2
        |RETURN person.name AS name""".stripMargin
    )
  }

  test("""MATCH (person:Person)
         |WHERE COUNT {
         | MATCH (person)-[:HAS_DOG]->(d:Dog)
         | WHERE COUNT {
         |  WITH "Ozzy" AS dogName
         |  MATCH (person)-[:HAS_DOG]->(dog:Dog)
         |  WHERE dog.name = dogName
         |  RETURN dog AS pet
         |  UNION ALL
         |  WITH "Sylvester" AS catName
         |  MATCH (person)-[:HAS_CAT]->(cat:Cat)
         |  WHERE cat.name = catName
         |  RETURN cat AS pet
         | } > 2
         | RETURN person AS person
         |} > 2
         |RETURN person.name AS name""".stripMargin) {
    assertRewrite(
      testName,
      """MATCH (person:Person)
        |WHERE COUNT {
        | MATCH (person)-[:HAS_DOG]->(d:Dog)
        | WHERE COUNT {
        |  WITH "Ozzy" AS dogName, person AS person
        |  MATCH (person)-[:HAS_DOG]->(dog:Dog)
        |  WHERE dog.name = dogName
        |  RETURN dog AS pet
        |  UNION ALL
        |  WITH "Sylvester" AS catName, person AS person
        |  MATCH (person)-[:HAS_CAT]->(cat:Cat)
        |  WHERE cat.name = catName
        |  RETURN cat AS pet
        | } > 2
        | RETURN person AS person
        |} > 2
        |RETURN person.name AS name""".stripMargin
    )
  }

  test("""MATCH (person:Person)
         |WHERE COUNT {
         | MATCH (person)-[:HAS_DOG]->(d:Dog)
         | RETURN person AS person
         |} > 2
         |RETURN person.name AS name""".stripMargin) {
    assertIsNotRewritten(testName)
  }

  test("""MATCH (person:Person)
         |WHERE COUNT {
         | MATCH (person)-[:HAS_DOG]->(d:Dog)
         | CALL {
         |  WITH d AS d
         |  MATCH (d)-[:HAS_FRIEND]-(f:Dog)
         |  RETURN d AS d
         | }
         | RETURN person AS person
         |} > 2
         |RETURN person.name AS name""".stripMargin) {
    assertIsNotRewritten(testName)
  }

  test("""MATCH (person:Person)
         |WHERE COUNT {
         | MATCH (person)-[:HAS_DOG]->(d:Dog)
         | WHERE COUNT {
         |  MATCH (person)-[:HAS_DOG]->(dog:Dog)
         |  RETURN dog AS pet
         |  UNION
         |  MATCH (person)-[:HAS_CAT]->(cat:Cat)
         |  RETURN cat AS pet
         | } > 2
         | RETURN person AS person
         |} > 2
         |RETURN person.name AS name""".stripMargin) {
    assertIsNotRewritten(testName)
  }

  test("should rewrite RETURN ORDER BY into WITH ORDER BY RETURN") {
    assertRewrite(
      """WITH 123 AS x, 321 AS y
        |RETURN COLLECT {
        |  RETURN x + y
        |  ORDER BY x SKIP 10 LIMIT 5
        |} AS result""".stripMargin,
      """WITH 123 AS x, 321 AS y
        |RETURN COLLECT {
        |  WITH x + y AS `x + y`, x AS x, y AS y
        |  ORDER BY x SKIP 10 LIMIT 5
        |  RETURN `x + y` AS `x + y`
        |} AS result""".stripMargin,
      additionalExpectedAstUpdates = expectedStatement => {
        expectedStatement.endoRewrite(bottomUp(Rewriter.lift {
          // The original/rewritten statement will have AddedInRewriteGeneral on the extra WITH,
          // both explicit WITHs in the expected will have DefaultWith
          // so let's update the added WITH before checking the equality
          case w: With if w.returnItems.items.exists(r => r.name.equals("x + y")) =>
            w.copy(withType = AddedInRewriteGeneral())(w.position)
        }))
      }
    )
  }

  test("should rewrite innerquery of SubqueryCall into single WITH") {
    assertRewrite(
      """WITH 1 AS a
        |CALL(a) {
        |WITH 2 AS b
        |WITH 3 AS c
        |RETURN a + c AS res
        |}
        |RETURN res
        |""".stripMargin,
      """WITH 1 AS a
        |CALL(a) {
        |WITH 2 AS b, a AS a
        |WITH 3 AS c, a AS a
        |RETURN a + c AS res
        |}
        |RETURN res AS res
        |""".stripMargin
    )
  }

  test("should rewrite innerquery of SubqueryCall and return not split") {
    assertRewrite(
      """WITH 1 AS a
        |CALL(a) {
        |WITH 2 AS b
        |WITH 3 AS c
        |WITH a + c AS res
        |RETURN res LIMIT 1
        |}
        |RETURN res LIMIT 1
        |""".stripMargin,
      """WITH 1 AS a
        |CALL(a) {
        |WITH 2 AS b, a AS a
        |WITH 3 AS c, a AS a
        |WITH a + c AS res, a AS a
        |RETURN res AS res LIMIT 1
        |}
        |RETURN res AS res LIMIT 1
        |""".stripMargin
    )
  }

  private def assertIsNotRewritten(query: String): Unit = {
    assertRewrite(query, query)
  }

  // additionalExpectedAstUpdates is for updating things that are changed in the rewriter but cannot be expressed in the query,
  // for example the AddedInRewriteGeneral flag on WITH
  private def assertRewrite(
    originalQuery: String,
    expectedQuery: String,
    additionalExpectedAstUpdates: Statement => Statement = statement => statement
  ): Unit = {
    val cypherExceptionFactory = Neo4jCypherExceptionFactory(originalQuery, None)
    val original = parse(originalQuery, cypherExceptionFactory)
    val initialExpected = parse(expectedQuery, cypherExceptionFactory)
    val expected = additionalExpectedAstUpdates(initialExpected)
    val version = CypherVersionHelpers.arbitrarySemanticContext()

    val normalizedWithAndReturnClauses =
      original.endoRewrite(NormalizeWithAndReturnClauses.getRewriter(
        cypherExceptionFactory,
        Some(version.cypherVersion)
      ))
    val checkResult =
      normalizedWithAndReturnClauses.semanticCheck.run(SemanticState.clean, version)
    val rewriter =
      inSequence(
        computeDependenciesForExpressions(checkResult.state),
        AddDependenciesToProjectionsInSubqueryExpressions.subqueryExpressionAndCallClauseRewriter
      )

    val result = normalizedWithAndReturnClauses.rewrite(rewriter)
    withClue(Prettifier(ExpressionStringifier()).asString(result.asInstanceOf[Statement])) {
      assert(result === expected)
    }
  }
}
