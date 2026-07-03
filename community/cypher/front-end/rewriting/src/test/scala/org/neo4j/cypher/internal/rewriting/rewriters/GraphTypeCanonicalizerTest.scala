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

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.AlterCurrentGraphType
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.ast.prettifier.Prettifier
import org.neo4j.cypher.internal.graphtype.GraphTypeTestCase
import org.neo4j.cypher.internal.rewriting.RewriteTest
import org.neo4j.cypher.internal.rewriting.rewriters.astRewriters.GraphTypeCanonicalizer
import org.neo4j.cypher.internal.rewriting.rewriters.preparatoryRewriters.RewriteGraphTypeReferences
import org.neo4j.cypher.internal.util.Neo4jCypherExceptionFactory
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.test_helpers.TestName

class GraphTypeCanonicalizerTest extends CypherFunSuite with RewriteTest with TestName {

  override def rewriterUnderTest: Rewriter = rewriterUnderTest("not used")

  override def rewriterUnderTest(queryString: String): Rewriter =
    RewriteGraphTypeReferences.getRewriter(Neo4jCypherExceptionFactory(
      queryString,
      None
    )).andThen(GraphTypeCanonicalizer.instance)

  override protected def assertRewrite(originalQuery: String, expectedQuery: String): Unit = {
    val (expected, result) = getRewrite(CypherVersion.Cypher25, originalQuery, expectedQuery)
    assert(
      result === expected,
      s"\n$originalQuery\nshould be rewritten to:\n${prettifier.asString(expected)}\nbut was rewritten to:\n${prettifier.asString(result.asInstanceOf[Statement])}"
    )
    Prettifier(ExpressionStringifier()).asString(result.asInstanceOf[AlterCurrentGraphType]) shouldBe expectedQuery
  }

  test("ALTER CURRENT GRAPH TYPE SET { (n: Node => :Another) }") {
    assertRewrite(
      testName,
      """ALTER CURRENT GRAPH TYPE SET {
        |  (:`Node` => :`Another`)
        |}""".stripMargin
    )
  }

  test("ALTER CURRENT GRAPH TYPE SET { ()-[rel:REL => { since :: DATE } ]->() }") {
    assertRewrite(
      testName,
      """ALTER CURRENT GRAPH TYPE SET {
        |  ()-[:`REL` => {`since` :: DATE}]->()
        |}""".stripMargin
    )
  }

  test(
    """ALTER CURRENT GRAPH TYPE SET {
      |  (n: Node => :Another),
      |  (n)-[:REL IMPLIES {}]->(:Node)
      |}""".stripMargin
  ) {
    assertRewrite(
      testName,
      """ALTER CURRENT GRAPH TYPE SET {
        |  (:`Node` => :`Another`),
        |  (:`Node` =>)-[:`REL` =>]->(:`Node` =>)
        |}""".stripMargin
    )
  }

  test(
    """ALTER CURRENT GRAPH TYPE SET {
      |  (:`Node` => { name :: STRING } ),
      |  CONSTRAINT FOR (p:Node) REQUIRE p.name IS KEY
      |}""".stripMargin
  ) {
    assertRewrite(
      testName,
      """ALTER CURRENT GRAPH TYPE SET {
        |  (:`Node` => {`name` :: STRING}),
        |  CONSTRAINT FOR (`n`:`Node` =>) REQUIRE (`n`.`name`) IS KEY
        |}""".stripMargin
    )
  }

  test(
    """ALTER CURRENT GRAPH TYPE SET {
      |  (n:`Node` => { name :: STRING}),
      |  (n)-[:REL IMPLIES { since :: DATE}]->(:Node),
      |  CONSTRAINT FOR ()-[rel:REL]->() REQUIRE p.since IS KEY
      |}""".stripMargin
  ) {
    assertRewrite(
      testName,
      """ALTER CURRENT GRAPH TYPE SET {
        |  (:`Node` => {`name` :: STRING}),
        |  (:`Node` =>)-[:`REL` => {`since` :: DATE}]->(:`Node` =>),
        |  CONSTRAINT FOR ()-[`r`:`REL` =>]->() REQUIRE (`r`.`since`) IS KEY
        |}""".stripMargin
    )
  }

  test(
    """ALTER CURRENT GRAPH TYPE SET {
      |  CONSTRAINT FOR ()-[rel:REL]->() REQUIRE p.since IS KEY,
      |  CONSTRAINT FOR (c:City) REQUIRE c.name IS UNIQUE
      |}""".stripMargin
  ) {
    assertRewrite(
      testName,
      """ALTER CURRENT GRAPH TYPE SET {
        |  CONSTRAINT FOR (`n`:`City`) REQUIRE (`n`.`name`) IS UNIQUE,
        |  CONSTRAINT FOR ()-[`r`:`REL`]->() REQUIRE (`r`.`since`) IS KEY
        |}""".stripMargin
    )
  }

  test(
    """ALTER CURRENT GRAPH TYPE SET {
      |  CONSTRAINT FOR ()-[rel:REL]->() REQUIRE r.since :: ANY<LIST<INTEGER>>,
      |  CONSTRAINT FOR (c:City) REQUIRE c.name IS NOT NULL
      |}""".stripMargin
  ) {
    assertRewrite(
      testName,
      """ALTER CURRENT GRAPH TYPE SET {
        |  CONSTRAINT FOR (`n`:`City`) REQUIRE (`n`.`name`) IS NOT NULL,
        |  CONSTRAINT FOR ()-[`r`:`REL`]->() REQUIRE (`r`.`since`) IS :: LIST<INTEGER>
        |}""".stripMargin
    )
  }

  test("""ALTER CURRENT GRAPH TYPE SET { (s:Student => :Person { name :: STRING NOT NULL, birthday :: DATE, studId :: INT IS KEY })
         |  REQUIRE (s.name, s.birthday) IS UNIQUE OPTIONS { indexProvider: "range-1.0" },
         |(:City => :Location { name :: STRING IS KEY }),
         |(:Site => :Location { name :: STRING }),
         |(s)-[:LIVES_IN =>]->(:City =>),
         |(s)-[:VISITED =>]->(:Location),
         |CONSTRAINT FOR ()-[x:LegacyRel]->() REQUIRE x.foo IS UNIQUE,
         |CONSTRAINT mySiteConstraint FOR (st:Site =>) REQUIRE st.name IS KEY,
         |CONSTRAINT FOR (p:Person) REQUIRE p.age :: INT }""".stripMargin) {
    assertRewrite(
      testName,
      """ALTER CURRENT GRAPH TYPE SET {
        |  (:`City` => :`Location` {`name` :: STRING}),
        |  (:`Site` => :`Location` {`name` :: STRING}),
        |  (:`Student` => :`Person` {`birthday` :: DATE, `name` :: STRING NOT NULL, `studId` :: INTEGER}),
        |  (:`Student` =>)-[:`LIVES_IN` =>]->(:`City` =>),
        |  (:`Student` =>)-[:`VISITED` =>]->(:`Location`),
        |  CONSTRAINT FOR (`n`:`City` =>) REQUIRE (`n`.`name`) IS KEY,
        |  CONSTRAINT `mySiteConstraint` FOR (`n`:`Site` =>) REQUIRE (`n`.`name`) IS KEY,
        |  CONSTRAINT FOR (`n`:`Student` =>) REQUIRE (`n`.`name`, `n`.`birthday`) IS UNIQUE OPTIONS {`indexProvider`: "range-1.0"},
        |  CONSTRAINT FOR (`n`:`Student` =>) REQUIRE (`n`.`studId`) IS KEY,
        |  CONSTRAINT FOR (`n`:`Person`) REQUIRE (`n`.`age`) IS :: INTEGER,
        |  CONSTRAINT FOR ()-[`r`:`LegacyRel`]->() REQUIRE (`r`.`foo`) IS UNIQUE
        |}""".stripMargin
    )
  }

  GraphTypeTestCase.testcases.foreach { testcase =>
    test(testcase.name) {
      assertRewrite(testcase.cypher, "ALTER CURRENT GRAPH TYPE SET " + testcase.canonicalPrettifiedCypher)
    }
  }
}
