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

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.AstConstructionTestSupportWithPosConversion
import org.neo4j.cypher.internal.ast.ShowAliases
import org.neo4j.cypher.internal.ast.ShowAuthRules
import org.neo4j.cypher.internal.ast.ShowColumn
import org.neo4j.cypher.internal.ast.ShowConstraintsClause
import org.neo4j.cypher.internal.ast.ShowCurrentGraphTypeClause
import org.neo4j.cypher.internal.ast.ShowCurrentUser
import org.neo4j.cypher.internal.ast.ShowPrivilegeCommands
import org.neo4j.cypher.internal.ast.ShowPrivileges
import org.neo4j.cypher.internal.ast.ShowRoles
import org.neo4j.cypher.internal.ast.ShowServers
import org.neo4j.cypher.internal.ast.ShowSupportedPrivilegeCommand
import org.neo4j.cypher.internal.ast.ShowUsers
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.rewriting.rewriters.Anonymizer
import org.neo4j.cypher.internal.rewriting.rewriters.anonymizeQuery
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.bottomUp

class AnonymizeQueryTest extends AnonymizerTestBase with AstConstructionTestSupportWithPosConversion {

  val anonymizer: Anonymizer = new Anonymizer {
    override def label(name: String): String = "x" + name
    override def relationshipType(name: String): String = "x" + name
    override def labelOrRelationshipType(name: String): String = "x" + name
    override def propertyKey(name: String): String = "X" + name
    override def variable(name: String): String = "X" + name

    override def unaliasedReturnItemName(anonymizedExpression: Expression, input: String): String =
      prettifier.expr(anonymizedExpression)
    override def parameter(name: String): String = "X" + name
    override def literal(value: String): String = s"string[$value]"
    override def indexName(name: String): String = "X" + name
    override def constraintName(name: String): String = "X" + name
    override def identifierAsString(name: String): String = "X" + name
    override def secretName(name: String): String = "X" + name
  }

  val rewriterUnderTest: Rewriter = anonymizeQuery(anonymizer)

  test("variable") {
    assertRewrite("RETURN 1 AS r", "RETURN 1 AS Xr")
    assertRewrite("WITH 1 AS k RETURN (k + k) - k AS r", "WITH 1 AS Xk RETURN (Xk + Xk) - Xk AS Xr")
  }

  test("return item") {
    assertRewrite("WITH 1 AS k RETURN k + k", "WITH 1 AS Xk RETURN Xk + Xk")
  }

  test("label") {
    assertRewrite("MATCH (:LABEL) RETURN count(*)", "MATCH (:xLABEL) RETURN count(*)")
    assertRewrite("MATCH (:A:B) RETURN count(*)", "MATCH (:xA:xB) RETURN count(*)")
    assertRewrite("MATCH (:A)--(:B) RETURN count(*)", "MATCH (:xA)--(:xB) RETURN count(*)")
    assertRewrite("MATCH (:A)-[r*1..5]-(:B) RETURN count(*)", "MATCH (:xA)-[Xr*1..5]-(:xB) RETURN count(*)")
    assertRewrite("MATCH (n) SET n:LABEL", "MATCH (Xn) SET Xn:xLABEL")
  }

  test("relationship type") {
    assertRewrite("MATCH ()-[:R]-() RETURN count(*)", "MATCH ()-[:xR]-() RETURN count(*)")
    assertRewrite("MATCH ()-[:R*2..4]-() RETURN count(*)", "MATCH ()-[:xR*2..4]-() RETURN count(*)")
    assertRewrite(
      "MATCH shortestPath(()-[:R*2..4]-()) RETURN count(*)",
      "MATCH shortestPath(()-[:xR*2..4]-()) RETURN count(*)"
    )
    assertRewrite("MATCH ()-[r]-() SET r:TYPE", "MATCH ()-[Xr]-() SET Xr:xTYPE")
  }

  test("label or relationship type") {
    assertRewrite(
      "MATCH (n)-[r]->() UNWIND [n, r] AS x WITH x WHERE x:A RETURN n",
      "MATCH (Xn)-[Xr]->() UNWIND [Xn, Xr] AS Xx WITH Xx WHERE Xx:xA RETURN Xn"
    )
    assertRewrite(
      "MATCH (n)-[r]->() UNWIND [n, r] AS x WITH x WHERE x:A:B RETURN n",
      "MATCH (Xn)-[Xr]->() UNWIND [Xn, Xr] AS Xx WITH Xx WHERE Xx:xA:xB RETURN Xn"
    )
  }

  test("label expression") {
    assertRewrite("MATCH (n:A&(B|!C)) RETURN n", "MATCH (Xn:xA&(xB|!xC)) RETURN Xn")
    assertRewrite("MATCH (n) WHERE n:A&(B|!C) RETURN n", "MATCH (Xn) WHERE Xn:xA&(xB|!xC) RETURN Xn")
  }

  test("relationship type expression") {
    assertRewrite("MATCH ()-[r:A&(B|!C)]->() RETURN r", "MATCH ()-[Xr:xA&(xB|!xC)]->() RETURN Xr")
    assertRewrite("MATCH ()-[r]->() WHERE r:A&(B|!C) RETURN r", "MATCH ()-[Xr]->() WHERE Xr:xA&(xB|!xC) RETURN Xr")
  }

  test("property key") {
    assertRewrite("MATCH ({p: 2}) RETURN count(*)", "MATCH ({Xp: 2}) RETURN count(*)")
    assertRewrite("MATCH (n) SET p.n = 2", "MATCH (Xn) SET Xp.Xn = 2")
    assertRewrite("MATCH (n) WHERE p.n > 2 RETURN 1", "MATCH (Xn) WHERE Xp.Xn > 2 RETURN 1")
  }

  test("search clause") {
    assertRewrite(
      CypherVersion.Cypher25,
      "MATCH (n) SEARCH n IN ( VECTOR INDEX name FOR [1,2,3] LIMIT 1 ) SCORE AS score",
      "MATCH (Xn) SEARCH Xn IN ( VECTOR INDEX `Xstring[name]` FOR [1,2,3] LIMIT 1 ) SCORE AS Xscore"
    )
  }

  test("parameter") {
    assertRewrite("RETURN $param1 AS p1, $param2 AS p2", "RETURN $Xparam1 AS Xp1, $Xparam2 AS Xp2")
  }

  test("literals") {
    assertRewrite("RETURN \"hello\"", "RETURN \"string[hello]\"")

  }

  test("index commands") {
    // create range
    assertRewrite(
      "CREATE INDEX name FOR (n:Label) ON (n.prop1, n.prop2)",
      "CREATE INDEX `Xstring[name]` FOR (Xn:xLabel) ON (Xn.Xprop1, Xn.Xprop2)"
    )
    assertRewrite(
      "CREATE INDEX name FOR ()-[r:TYPE]-() ON (r.prop)",
      "CREATE INDEX `Xstring[name]` FOR ()-[Xr:xTYPE]-() ON (Xr.Xprop)"
    )
    assertRewrite(
      "CREATE INDEX $name FOR (n:Label) ON (n.prop1, n.prop2)",
      "CREATE INDEX $Xname FOR (Xn:xLabel) ON (Xn.Xprop1, Xn.Xprop2)"
    )
    assertRewrite(
      CypherVersion.Cypher5,
      "CREATE INDEX name FOR (n:Label) ON n.prop OPTIONS {indexProvider: 'range-1.0'}",
      "CREATE INDEX `Xstring[name]` FOR (Xn:xLabel) ON Xn.Xprop OPTIONS {indexProvider: 'string[range-1.0]'}"
    )

    // create text
    assertRewrite(
      "CREATE TEXT INDEX name FOR (n:Label) ON (n.prop)",
      "CREATE TEXT INDEX `Xstring[name]` FOR (Xn:xLabel) ON (Xn.Xprop)"
    )
    assertRewrite(
      "CREATE TEXT INDEX name FOR ()-[r:TYPE]-() ON (r.prop)",
      "CREATE TEXT INDEX `Xstring[name]` FOR ()-[Xr:xTYPE]-() ON (Xr.Xprop)"
    )

    // create point
    assertRewrite(
      "CREATE POINT INDEX name FOR (n:Label) ON (n.prop)",
      "CREATE POINT INDEX `Xstring[name]` FOR (Xn:xLabel) ON (Xn.Xprop)"
    )
    assertRewrite(
      "CREATE POINT INDEX name FOR ()-[r:TYPE]-() ON (r.prop)",
      "CREATE POINT INDEX `Xstring[name]` FOR ()-[Xr:xTYPE]-() ON (Xr.Xprop)"
    )
    assertRewrite(
      "CREATE POINT INDEX name FOR ()-[r:TYPE]-() ON (r.prop) OPTIONS {indexConfig: {`spatial.cartesian.min`: [-5.0, 5.0]}}",
      "CREATE POINT INDEX `Xstring[name]` FOR ()-[Xr:xTYPE]-() ON (Xr.Xprop) OPTIONS {indexConfig: {`Xspatial.cartesian.min`: [-5.0, 5.0]}}"
    )

    // create vector
    assertRewrite(
      "CREATE VECTOR INDEX name FOR (n:Label) ON (n.prop)",
      "CREATE VECTOR INDEX `Xstring[name]` FOR (Xn:xLabel) ON (Xn.Xprop)"
    )
    assertRewrite(
      CypherVersion.Cypher25,
      "CREATE VECTOR INDEX name FOR ()-[r:TYPE|TYPE2]-() ON (r.prop) WITH [r.prop2]",
      "CREATE VECTOR INDEX `Xstring[name]` FOR ()-[Xr:xTYPE|xTYPE2]-() ON (Xr.Xprop) WITH [Xr.Xprop2]"
    )

    // create fulltext
    assertRewrite(
      "CREATE FULLTEXT INDEX name FOR (n:Label1|Label2) ON EACH [n.prop]",
      "CREATE FULLTEXT INDEX `Xstring[name]` FOR (Xn:xLabel1|xLabel2) ON EACH [Xn.Xprop]"
    )
    assertRewrite(
      "CREATE FULLTEXT INDEX name FOR ()-[r:TYPE]-() ON EACH [r.prop]",
      "CREATE FULLTEXT INDEX `Xstring[name]` FOR ()-[Xr:xTYPE]-() ON EACH [Xr.Xprop]"
    )
    assertRewrite(
      "CREATE FULLTEXT INDEX name FOR (n:Label1|Label2) ON EACH [n.prop] OPTIONS {indexConfig: {`fulltext.analyzer`: 'english'}}",
      "CREATE FULLTEXT INDEX `Xstring[name]` FOR (Xn:xLabel1|xLabel2) ON EACH [Xn.Xprop] OPTIONS {indexConfig: {`Xfulltext.analyzer`: 'string[english]'}}"
    )

    // create token lookup
    assertRewrite(
      "CREATE LOOKUP INDEX name FOR (n) ON EACH labels(n)",
      "CREATE LOOKUP INDEX `Xstring[name]` FOR (Xn) ON EACH labels(Xn)"
    )
    assertRewrite(
      "CREATE LOOKUP INDEX name FOR ()-[r]-() ON EACH type(r)",
      "CREATE LOOKUP INDEX `Xstring[name]` FOR ()-[Xr]-() ON EACH type(Xr)"
    )

    // drop
    assertRewrite("DROP INDEX name", "DROP INDEX `Xstring[name]`")
    assertRewrite("DROP INDEX $name", "DROP INDEX $Xname")
  }

  test("constraint commands") {
    // create
    assertRewrite(
      "CREATE CONSTRAINT name FOR (n:Label) REQUIRE (n.prop1, n.prop2) IS NODE KEY",
      "CREATE CONSTRAINT `Xstring[name]` FOR (Xn:xLabel) REQUIRE (Xn.Xprop1, Xn.Xprop2) IS NODE KEY"
    )
    assertRewrite(
      "CREATE CONSTRAINT name FOR (n:Label) REQUIRE (n.prop) IS UNIQUE",
      "CREATE CONSTRAINT `Xstring[name]` FOR (Xn:xLabel) REQUIRE (Xn.Xprop) IS UNIQUE"
    )
    assertRewrite(
      "CREATE CONSTRAINT name FOR (n:Label) REQUIRE (n.prop) IS NOT NULL",
      "CREATE CONSTRAINT `Xstring[name]` FOR (Xn:xLabel) REQUIRE (Xn.Xprop) IS NOT NULL"
    )
    assertRewrite(
      "CREATE CONSTRAINT name FOR ()-[r:TYPE]-() REQUIRE (r.prop) IS NOT NULL",
      "CREATE CONSTRAINT `Xstring[name]` FOR ()-[Xr:xTYPE]-() REQUIRE (Xr.Xprop) IS NOT NULL"
    )
    assertRewrite(
      "CREATE CONSTRAINT $name FOR ()-[r:TYPE]-() REQUIRE (r.prop) IS :: STRING",
      "CREATE CONSTRAINT $Xname FOR ()-[Xr:xTYPE]-() REQUIRE (Xr.Xprop) IS :: STRING"
    )

    // drop
    assertRewrite("DROP CONSTRAINT name", "DROP CONSTRAINT `Xstring[name]`")
    assertRewrite("DROP CONSTRAINT $name", "DROP CONSTRAINT $Xname")
  }

  test("graph type commands") {
    // set
    assertRewrite(
      CypherVersion.Cypher25,
      "ALTER CURRENT GRAPH TYPE SET { CONSTRAINT name FOR (n:Label) REQUIRE (n.prop1, n.prop2) IS KEY }",
      "ALTER CURRENT GRAPH TYPE SET { CONSTRAINT Xname FOR (Xn:xLabel) REQUIRE (Xn.Xprop1, Xn.Xprop2) IS KEY }"
    )
    assertRewrite(
      CypherVersion.Cypher25,
      "ALTER CURRENT GRAPH TYPE SET { (n:Label1 => :Label2 {prop :: STRING}) }",
      "ALTER CURRENT GRAPH TYPE SET { (Xn:xLabel1 => :xLabel2 {Xprop :: STRING}) }"
    )
    assertRewrite(
      CypherVersion.Cypher25,
      "ALTER CURRENT GRAPH TYPE SET { (n:Label1)-[r:TYPE => {prop1 :: INTEGER, prop2 :: ANY NOT NULL}]->(m:Label2 =>) }",
      "ALTER CURRENT GRAPH TYPE SET { (Xn:xLabel1)-[Xr:xTYPE => {Xprop1 :: INTEGER, Xprop2 :: ANY NOT NULL}]->(Xm:xLabel2 =>) }"
    )

    // add
    assertRewrite(
      CypherVersion.Cypher25,
      "ALTER CURRENT GRAPH TYPE ADD { CONSTRAINT name FOR ()-[r:TYPE =>]->() REQUIRE (r.prop1, r.prop2) IS UNIQUE }",
      "ALTER CURRENT GRAPH TYPE ADD { CONSTRAINT Xname FOR ()-[Xr:xTYPE =>]->() REQUIRE (Xr.Xprop1, Xr.Xprop2) IS UNIQUE }"
    )
    assertRewrite(
      CypherVersion.Cypher25,
      "ALTER CURRENT GRAPH TYPE ADD { (n:Label1 => :Label2&Label3 {prop :: STRING NOT NULL}) }",
      "ALTER CURRENT GRAPH TYPE ADD { (Xn:xLabel1 => :xLabel2&xLabel3 {Xprop :: STRING NOT NULL}) }"
    )
    assertRewrite(
      CypherVersion.Cypher25,
      "ALTER CURRENT GRAPH TYPE ADD { (n:Label1)-[r:TYPE => {prop1 :: INTEGER, prop2 :: ANY NOT NULL}]->(m:Label2 =>) }",
      "ALTER CURRENT GRAPH TYPE ADD { (Xn:xLabel1)-[Xr:xTYPE => {Xprop1 :: INTEGER, Xprop2 :: ANY NOT NULL}]->(Xm:xLabel2 =>) }"
    )

    // alter
    assertRewrite(
      CypherVersion.Cypher25,
      "ALTER CURRENT GRAPH TYPE ALTER { (n:Label1 => :Label2&Label3 {prop :: STRING NOT NULL}) }",
      "ALTER CURRENT GRAPH TYPE ALTER { (Xn:xLabel1 => :xLabel2&xLabel3 {Xprop :: STRING NOT NULL}) }"
    )
    assertRewrite(
      CypherVersion.Cypher25,
      "ALTER CURRENT GRAPH TYPE ALTER { (n:Label1)-[r:TYPE => {prop1 :: INTEGER, prop2 :: ANY NOT NULL}]->(m:Label2 =>) }",
      "ALTER CURRENT GRAPH TYPE ALTER { (Xn:xLabel1)-[Xr:xTYPE => {Xprop1 :: INTEGER, Xprop2 :: ANY NOT NULL}]->(Xm:xLabel2 =>) }"
    )

    // drop
    assertRewrite(
      CypherVersion.Cypher25,
      "ALTER CURRENT GRAPH TYPE DROP { CONSTRAINT name }",
      "ALTER CURRENT GRAPH TYPE DROP { CONSTRAINT Xname }"
    )
    assertRewrite(
      CypherVersion.Cypher25,
      "ALTER CURRENT GRAPH TYPE DROP { (n:Label1 => :Label2&Label3 {prop :: STRING NOT NULL}) }",
      "ALTER CURRENT GRAPH TYPE DROP { (Xn:xLabel1 => :xLabel2&xLabel3 {Xprop :: STRING NOT NULL}) }"
    )
    assertRewrite(
      CypherVersion.Cypher25,
      "ALTER CURRENT GRAPH TYPE DROP { (n:Label1 =>) }",
      "ALTER CURRENT GRAPH TYPE DROP { (Xn:xLabel1 =>) }"
    )
    assertRewrite(
      CypherVersion.Cypher25,
      "ALTER CURRENT GRAPH TYPE DROP { (n:Label1)-[r:TYPE => {prop1 :: INTEGER, prop2 :: ANY NOT NULL}]->(m:Label2 =>) }",
      "ALTER CURRENT GRAPH TYPE DROP { (Xn:xLabel1)-[Xr:xTYPE => {Xprop1 :: INTEGER, Xprop2 :: ANY NOT NULL}]->(Xm:xLabel2 =>) }"
    )
    assertRewrite(
      CypherVersion.Cypher25,
      "ALTER CURRENT GRAPH TYPE DROP { ()-[r:TYPE =>]->() }",
      "ALTER CURRENT GRAPH TYPE DROP { ()-[Xr:xTYPE =>]->() }"
    )
  }

  test("show and terminate commands") {
    assertRewrite(
      "SHOW INDEXES WHERE name = 'my_index'",
      "SHOW INDEXES WHERE Xname = 'string[my_index]'"
    )
    assertRewrite(
      // Default columns differ between Cypher versions so run them separately
      CypherVersion.Cypher25,
      "SHOW CONSTRAINTS YIELD name ORDER BY name WHERE name = $param",
      "SHOW CONSTRAINTS YIELD name AS Xname ORDER BY Xname WHERE Xname = $Xparam",
      additionalExpectedAstUpdates = expectedStatement => {
        expectedStatement.endoRewrite(bottomUp(Rewriter.lift {
          case s: ShowConstraintsClause =>
            // Update WITH clause to 'keep' correct default order
            val updatedWithClause = s.yieldWith.map(w => {
              val updatedReturnItems =
                w.returnItems.copy(defaultOrderOnColumns = Some(List("name")))(w.returnItems.position)
              w.copy(returnItems = updatedReturnItems)(w.position)
            })
            s.copy(yieldWith = updatedWithClause)(s.position)
        }))
      }
    )
    assertRewrite(
      // Default columns differ between Cypher versions so run them separately
      CypherVersion.Cypher5,
      "SHOW CONSTRAINTS YIELD name ORDER BY name WHERE name = $param",
      "SHOW CONSTRAINTS YIELD name AS Xname ORDER BY Xname WHERE Xname = $Xparam",
      additionalExpectedAstUpdates = expectedStatement => {
        expectedStatement.endoRewrite(bottomUp(Rewriter.lift {
          case s: ShowConstraintsClause =>
            // Update WITH clause to 'keep' correct default order
            val updatedWithClause = s.yieldWith.map(w => {
              val updatedReturnItems =
                w.returnItems.copy(defaultOrderOnColumns = Some(List("name")))(w.returnItems.position)
              w.copy(returnItems = updatedReturnItems)(w.position)
            })
            s.copy(yieldWith = updatedWithClause)(s.position)
        }))
      }
    )
    assertRewrite(
      // Command is only available in Cypher 25
      CypherVersion.Cypher25,
      "SHOW CURRENT GRAPH TYPE AS GRAPH YIELD nodes WHERE nodes <> []",
      "SHOW CURRENT GRAPH TYPE AS GRAPH YIELD nodes AS Xnodes WHERE Xnodes <> []",
      additionalExpectedAstUpdates = expectedStatement => {
        expectedStatement.endoRewrite(bottomUp(Rewriter.lift {
          case s: ShowCurrentGraphTypeClause =>
            // Update WITH clause to 'keep' correct default order
            val updatedWithClause = s.yieldWith.map(w => {
              val updatedReturnItems =
                w.returnItems.copy(defaultOrderOnColumns = Some(List("nodes")))(w.returnItems.position)
              w.copy(returnItems = updatedReturnItems)(w.position)
            })
            s.copy(yieldWith = updatedWithClause)(s.position)
        }))
      }
    )
    assertRewrite(
      "SHOW PROCEDURES EXECUTABLE BY user",
      "SHOW PROCEDURES EXECUTABLE BY Xuser"
    )
    assertRewrite(
      "SHOW FUNCTIONS EXECUTABLE BY user WHERE name",
      "SHOW FUNCTIONS EXECUTABLE BY Xuser WHERE Xname"
    )
    assertRewrite(
      "SHOW SETTINGS 'setting'",
      "SHOW SETTINGS 'string[setting]'"
    )
    assertRewrite(
      "SHOW SETTINGS 'setting1', 'setting2'",
      "SHOW SETTINGS 'string[setting1]', 'string[setting2]'"
    )
    assertRewrite(
      "SHOW TRANSACTIONS $param",
      "SHOW TRANSACTIONS $Xparam"
    )
    assertRewrite(
      "SHOW TRANSACTIONS [$param, 'txId']",
      "SHOW TRANSACTIONS [$Xparam, 'string[txId]']"
    )
    assertRewrite(
      "TERMINATE TRANSACTIONS ['txId']",
      "TERMINATE TRANSACTIONS ['string[txId]']"
    )
    assertRewrite(
      "TERMINATE TRANSACTIONS 'txId', 'txId2'",
      "TERMINATE TRANSACTIONS 'string[txId]', 'string[txId2]'"
    )
    assertRewrite(
      // Default columns differ between Cypher versions so run them separately
      CypherVersion.Cypher25,
      "SHOW DATABASE foo",
      "SHOW DATABASE foo"
    )
    assertRewrite(
      // Default columns differ between Cypher versions so run them separately
      CypherVersion.Cypher5,
      "SHOW DATABASE foo",
      "SHOW DATABASE foo"
    )
  }

  test("administration user commands") {
    assertRewrite(
      "SHOW CURRENT USER YIELD home",
      "SHOW CURRENT USER YIELD Xhome",
      additionalExpectedAstUpdates = _ match {
        case s: ShowCurrentUser => s.copy(defaultColumnSet = rewriteAdminShowDefaultColumnSet(s.defaultColumnSet))
        case s                  => s
      }
    )
    assertRewrite(
      "SHOW USERS WHERE user IN ['user1', $userParam]",
      "SHOW USERS WHERE Xuser IN ['string[user1]', $XuserParam]",
      additionalExpectedAstUpdates = _ match {
        case s: ShowUsers => s.copy(defaultColumnSet = rewriteAdminShowDefaultColumnSet(s.defaultColumnSet))
        case s            => s
      }
    )
    assertRewrite(
      // The password is printed as '******' but needs to be the same string for the comparison
      "CREATE USER foo SET PASSWORD 'secret-secret' SET AUTH 'provider' { SET ID 'id' }",
      "CREATE USER `string[foo]` SET PASSWORD 'secret-secret' SET AUTH 'provider' { SET ID 'string[id]' }"
    )
    assertRewrite(
      "ALTER USER foo SET HOME DATABASE neo4j",
      "ALTER USER `string[foo]` SET HOME DATABASE neo4j"
    )
    assertRewrite(
      // Command is only available in Cypher 25
      CypherVersion.Cypher25,
      "ALTER USERS foo, bar SET TAG 'Tag'",
      "ALTER USERS `string[foo]`, `string[bar]` SET TAG 'string[Tag]'"
    )
    assertRewrite(
      // The passwords are printed as '******' but needs to be the same string for the comparison
      "ALTER CURRENT USER SET PASSWORD FROM 'old' TO 'new'",
      "ALTER CURRENT USER SET PASSWORD FROM 'old' TO 'new'"
    )
    assertRewrite(
      "RENAME USER foo TO bar",
      "RENAME USER `string[foo]` TO `string[bar]`"
    )
    assertRewrite(
      "DROP USER foo",
      "DROP USER `string[foo]`"
    )
  }

  test("administration role commands") {
    assertRewrite(
      "SHOW ROLES YIELD role, immutable WHERE role = 'wanted' RETURN immutable",
      "SHOW ROLES YIELD Xrole, Ximmutable WHERE Xrole = 'string[wanted]' RETURN Ximmutable",
      additionalExpectedAstUpdates = _ match {
        case s: ShowRoles => s.copy(defaultColumnSet = rewriteAdminShowDefaultColumnSet(s.defaultColumnSet))
        case s            => s
      }
    )
    assertRewrite(
      "CREATE ROLE `foo` AS COPY OF `bar`",
      "CREATE ROLE `string[foo]` AS COPY OF `string[bar]`"
    )
    assertRewrite(
      "RENAME ROLE foo TO bar",
      "RENAME ROLE `string[foo]` TO `string[bar]`"
    )
    assertRewrite(
      "DROP ROLE foo",
      "DROP ROLE `string[foo]`"
    )
    assertRewrite(
      "GRANT ROLE foo TO bar",
      "GRANT ROLE `string[foo]` TO `string[bar]`"
    )
    assertRewrite(
      // Command is only available in Cypher 25
      CypherVersion.Cypher25,
      "GRANT ROLE foo TO AUTH RULE bar",
      "GRANT ROLE `string[foo]` TO AUTH RULE `string[bar]`"
    )
    assertRewrite(
      "REVOKE ROLE foo FROM bar",
      "REVOKE ROLE `string[foo]` FROM `string[bar]`"
    )
    assertRewrite(
      // Command is only available in Cypher 25
      CypherVersion.Cypher25,
      "REVOKE ROLE foo FROM AUTH RULE bar",
      "REVOKE ROLE `string[foo]` FROM AUTH RULE `string[bar]`"
    )
  }

  test("administration auth rule commands") {
    // These commands are only available in Cypher 25

    assertRewrite(
      CypherVersion.Cypher25,
      "SHOW AUTH RULES YIELD name",
      "SHOW AUTH RULES YIELD Xname",
      additionalExpectedAstUpdates = _ match {
        case s: ShowAuthRules => s.copy(defaultColumnSet = rewriteAdminShowDefaultColumnSet(s.defaultColumnSet))
        case s                => s
      }
    )
    assertRewrite(
      CypherVersion.Cypher25,
      "CREATE AUTH RULE $param SET CONDITION foo = my.func('value', 42)",
      "CREATE AUTH RULE $Xparam SET CONDITION Xfoo = my.func('string[value]', 42)"
    )
    assertRewrite(
      CypherVersion.Cypher25,
      "RENAME AUTH RULE foo TO bar",
      "RENAME AUTH RULE `string[foo]` TO `string[bar]`"
    )
    assertRewrite(
      CypherVersion.Cypher25,
      "DROP AUTH RULE foo",
      "DROP AUTH RULE `string[foo]`"
    )
    assertRewrite(
      CypherVersion.Cypher25,
      "ALTER AUTH RULE foo SET CONDITION true",
      "ALTER AUTH RULE `string[foo]` SET CONDITION true"
    )
  }

  test("administration privilege commands") {
    assertRewrite(
      "SHOW ROLE role PRIVILEGES",
      "SHOW ROLE `string[role]` PRIVILEGES",
      additionalExpectedAstUpdates = _ match {
        case s: ShowPrivileges => s.copy(defaultColumnSet = rewriteAdminShowDefaultColumnSet(s.defaultColumnSet))
        case s                 => s
      }
    )
    assertRewrite(
      "SHOW USER user PRIVILEGES AS COMMANDS WHERE command CONTAINS 'GRANT'",
      "SHOW USER `string[user]` PRIVILEGES AS COMMANDS WHERE Xcommand CONTAINS 'string[GRANT]'",
      additionalExpectedAstUpdates = _ match {
        case s: ShowPrivilegeCommands =>
          s.copy(defaultColumnSet = rewriteAdminShowDefaultColumnSet(s.defaultColumnSet))
        case s => s
      }
    )
    assertRewrite(
      "SHOW SUPPORTED PRIVILEGES YIELD action, description ORDER BY action SKIP 1 LIMIT 2",
      "SHOW SUPPORTED PRIVILEGES YIELD Xaction, Xdescription ORDER BY Xaction SKIP 1 LIMIT 2",
      additionalExpectedAstUpdates = _ match {
        case s: ShowSupportedPrivilegeCommand =>
          s.copy(defaultColumnSet = rewriteAdminShowDefaultColumnSet(s.defaultColumnSet))
        case s => s
      }
    )
    assertRewrite(
      "GRANT READ {foo} ON GRAPH bar NODES Baz TO bao",
      "GRANT READ {foo} ON GRAPH bar NODES Baz TO `string[bao]`"
    )
    assertRewrite(
      "GRANT TRAVERSE ON GRAPH bar FOR (n:Baz {foo: 'available'}) TO bao",
      "GRANT TRAVERSE ON GRAPH bar FOR (Xn:Baz {Xfoo: 'string[available]'}) TO `string[bao]`"
    )
    assertRewrite(
      "DENY EXECUTE PROCEDURE db.* ON DBMS TO bao",
      "DENY EXECUTE PROCEDURE db.* ON DBMS TO `string[bao]`"
    )
    assertRewrite(
      "REVOKE TRANSACTION (user) ON DATABASE bar FROM bao",
      "REVOKE TRANSACTION (`string[user]`) ON DATABASE bar FROM `string[bao]`"
    )
    assertRewrite(
      "GRANT LOAD ON CIDR 'cidr' TO bao",
      "GRANT LOAD ON CIDR 'cidr' TO `string[bao]`"
    )
    assertRewrite(
      CypherVersion.Cypher25,
      "GRANT READ SECRET 'sec1' ON DBMS TO bao",
      "GRANT READ SECRET 'Xsec1' ON DBMS TO `string[bao]`"
    )
    assertRewrite(
      CypherVersion.Cypher25,
      "GRANT READ SECRET $sec ON DBMS TO bao",
      "GRANT READ SECRET $Xsec ON DBMS TO `string[bao]`"
    )
  }

  test("administration server commands") {
    assertRewrite(
      "SHOW SERVERS YIELD * WHERE serverId <> name",
      "SHOW SERVERS YIELD * WHERE XserverId <> Xname",
      additionalExpectedAstUpdates = _ match {
        case s: ShowServers => s.copy(
            defaultColumns = s.defaultColumns.copy(columns = rewriteAdminShowDefaultColumnSet(s.defaultColumnSet))
          )
        case s => s
      }
    )
    assertRewrite(
      "ENABLE SERVER 'foo' OPTIONS {key: 'value'}",
      "ENABLE SERVER 'foo' OPTIONS {key: 'string[value]'}"
    )
    assertRewrite(
      "ALTER SERVER $foo SET OPTIONS {key1: 'value', key2: $param}",
      "ALTER SERVER $Xfoo SET OPTIONS {key1: 'string[value]', key2: $Xparam}"
    )
    assertRewrite(
      "RENAME SERVER 'foo' TO 'bar'",
      "RENAME SERVER 'foo' TO 'bar'"
    )
    assertRewrite(
      "DROP SERVER 'foo'",
      "DROP SERVER 'foo'"
    )
    assertRewrite(
      "DEALLOCATE DATABASES FROM SERVERS 'foo', $bar",
      "DEALLOCATE DATABASES FROM SERVERS 'foo', $Xbar"
    )
    assertRewrite(
      "DRYRUN REALLOCATE DATABASES",
      "DRYRUN REALLOCATE DATABASES"
    )
  }

  test("administration database commands") {
    // SHOW DATABASES are part of 'show and terminate commands'

    assertRewrite(
      "CREATE DATABASE foo TOPOLOGY 1 PRIMARY OPTIONS {key: 'value'} WAIT 10",
      "CREATE DATABASE foo TOPOLOGY 1 PRIMARY OPTIONS {key: 'string[value]'} WAIT 10"
    )
    assertRewrite(
      // Command is only available in Cypher 25
      CypherVersion.Cypher25,
      "CREATE DATABASE foo GRAPH SHARD { TOPOLOGY 2 PRIMARIES } PROPERTY SHARDS { COUNT 3 TOPOLOGY 2 REPLICA }",
      "CREATE DATABASE foo GRAPH SHARD { TOPOLOGY 2 PRIMARIES } PROPERTY SHARDS { COUNT 3 TOPOLOGY 2 REPLICA }"
    )
    assertRewrite(
      "CREATE COMPOSITE DATABASE foo DEFAULT LANGUAGE CYPHER 25 OPTIONS $param",
      "CREATE COMPOSITE DATABASE foo DEFAULT LANGUAGE CYPHER 25 OPTIONS $Xparam"
    )
    assertRewrite(
      "ALTER DATABASE $foo REMOVE OPTION bar WAIT 3",
      "ALTER DATABASE $Xfoo REMOVE OPTION bar WAIT 3"
    )
    assertRewrite(
      "ALTER DATABASE $foo SET OPTION bar 1+'xx' SET TOPOLOGY 1 SECONDARY SET DEFAULT LANGUAGE CYPHER 5",
      "ALTER DATABASE $Xfoo SET OPTION bar 1+'string[xx]' SET TOPOLOGY 1 SECONDARY SET DEFAULT LANGUAGE CYPHER 5"
    )
    assertRewrite(
      // Command is only available in Cypher 25
      CypherVersion.Cypher25,
      "ALTER DATABASE foo SET TOPOLOGY 3 REPLICAS SET GRAPH SHARD { SET TOPOLOGY 1 PRIMARY } SET PROPERTY SHARDS { SET TOPOLOGY 3 REPLICAS }",
      "ALTER DATABASE foo SET TOPOLOGY 3 REPLICAS SET GRAPH SHARD { SET TOPOLOGY 1 PRIMARY } SET PROPERTY SHARDS { SET TOPOLOGY 3 REPLICAS }"
    )
    assertRewrite(
      "DROP DATABASE foo",
      "DROP DATABASE foo"
    )
    assertRewrite(
      "START DATABASE foo WAIT 5 SECONDS",
      "START DATABASE foo WAIT 5 SECONDS"
    )
    assertRewrite(
      "STOP DATABASE foo",
      "STOP DATABASE foo"
    )
  }

  test("administration alias commands") {
    assertRewrite(
      "SHOW ALIAS foo FOR DATABASES YIELD name, database, location",
      "SHOW ALIAS foo FOR DATABASES YIELD Xname, Xdatabase, Xlocation",
      additionalExpectedAstUpdates = _ match {
        case s: ShowAliases => s.copy(
            defaultColumns = s.defaultColumns.copy(columns = rewriteAdminShowDefaultColumnSet(s.defaultColumnSet))
          )
        case s => s
      }
    )
    assertRewrite(
      "CREATE ALIAS foo FOR DATABASE bar PROPERTIES {key: 'value'}",
      "CREATE ALIAS foo FOR DATABASE bar PROPERTIES {key: 'string[value]'}"
    )
    assertRewrite(
      "CREATE ALIAS foo FOR DATABASE bar AT 'url' USER user PASSWORD 'secret' DRIVER {key: 'value'} DEFAULT LANGUAGE CYPHER 25",
      "CREATE ALIAS foo FOR DATABASE bar AT 'url' USER `string[user]` PASSWORD 'secret' DRIVER {key: 'string[value]'} DEFAULT LANGUAGE CYPHER 25"
    )
    assertRewrite(
      "ALTER ALIAS $foo SET DATABASE TARGET bar",
      "ALTER ALIAS $Xfoo SET DATABASE TARGET bar"
    )
    assertRewrite(
      "ALTER ALIAS $foo SET DATABASE TARGET bar AT $url USER user PASSWORD $pw",
      "ALTER ALIAS $Xfoo SET DATABASE TARGET bar AT $Xurl USER `string[user]` PASSWORD $Xpw"
    )
    assertRewrite(
      "ALTER ALIAS foo SET DATABASE DRIVER $param1 PROPERTIES $param2 DEFAULT LANGUAGE CYPHER 5",
      "ALTER ALIAS foo SET DATABASE DRIVER $Xparam1 PROPERTIES $Xparam2 DEFAULT LANGUAGE CYPHER 5"
    )
    assertRewrite(
      "DROP ALIAS foo FOR DATABASE",
      "DROP ALIAS foo FOR DATABASE"
    )
  }

  // Updates default columns for administration show commands to match anonymized names,
  // the anonymization only affect the variables, not the string names
  private def rewriteAdminShowDefaultColumnSet(defaultColumnSet: List[ShowColumn]): List[ShowColumn] = {
    defaultColumnSet.map(c => c.copy(variable = c.variable.renameId("X" + c.variable.name)))
  }
}
