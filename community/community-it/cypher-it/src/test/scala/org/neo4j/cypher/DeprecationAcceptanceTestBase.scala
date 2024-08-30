/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher

import org.neo4j.cypher.internal.javacompat.NotificationTestSupport.TestFunctions
import org.neo4j.cypher.internal.javacompat.NotificationTestSupport.TestProcedures
import org.neo4j.cypher.internal.options.CypherVersion
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.gqlstatus.GqlStatusInfoCodes.STATUS_01N00
import org.neo4j.gqlstatus.GqlStatusInfoCodes.STATUS_01N01
import org.neo4j.gqlstatus.GqlStatusInfoCodes.STATUS_01N02
import org.neo4j.gqlstatus.GqlStatusInfoCodes.STATUS_01N03
import org.neo4j.gqlstatus.GqlStatusInfoCodes.STATUS_01N51
import org.neo4j.gqlstatus.GqlStatusInfoCodes.STATUS_01N60
import org.neo4j.gqlstatus.NotificationClassification
import org.neo4j.graphdb.SeverityLevel
import org.neo4j.kernel.api.impl.schema.TextIndexProvider
import org.neo4j.kernel.api.impl.schema.trigram.TrigramIndexProvider
import org.neo4j.notifications.NotificationCodeWithDescription.deprecatedConnectComponentsPlannerPreParserOption
import org.neo4j.notifications.NotificationCodeWithDescription.deprecatedFunctionField
import org.neo4j.notifications.NotificationCodeWithDescription.deprecatedFunctionWithReplacement
import org.neo4j.notifications.NotificationCodeWithDescription.deprecatedFunctionWithoutReplacement
import org.neo4j.notifications.NotificationCodeWithDescription.deprecatedIdentifierUnicode
import org.neo4j.notifications.NotificationCodeWithDescription.deprecatedIdentifierWhitespaceUnicode
import org.neo4j.notifications.NotificationCodeWithDescription.deprecatedImportingWithInSubqueryCall
import org.neo4j.notifications.NotificationCodeWithDescription.deprecatedNodeOrRelationshipOnRhsSetClause
import org.neo4j.notifications.NotificationCodeWithDescription.deprecatedProcedureField
import org.neo4j.notifications.NotificationCodeWithDescription.deprecatedProcedureReturnField
import org.neo4j.notifications.NotificationCodeWithDescription.deprecatedProcedureWithReplacement
import org.neo4j.notifications.NotificationCodeWithDescription.deprecatedProcedureWithoutReplacement
import org.neo4j.notifications.NotificationCodeWithDescription.deprecatedPropertyReferenceInCreate
import org.neo4j.notifications.NotificationCodeWithDescription.deprecatedPropertyReferenceInMerge
import org.neo4j.notifications.NotificationCodeWithDescription.deprecatedRelationshipTypeSeparator
import org.neo4j.notifications.NotificationCodeWithDescription.deprecatedShortestPathWithFixedLengthRelationship
import org.neo4j.notifications.NotificationCodeWithDescription.deprecatedTextIndexProvider
import org.neo4j.notifications.NotificationCodeWithDescription.unionReturnOrder
import org.neo4j.notifications.NotificationDetail
import org.neo4j.notifications.NotificationDetail.deprecationNotificationDetail
import org.scalatest.BeforeAndAfterAll

abstract class DeprecationAcceptanceTestBase extends CypherFunSuite with BeforeAndAfterAll with DeprecationTestSupport {

  override def beforeAll(): Unit = {
    // Used for testing deprecated procedures
    dbms.registerProcedure(classOf[TestProcedures])
    dbms.registerFunction(classOf[TestFunctions])
    dbms.registerAggregationFunction(classOf[TestFunctions])
  }

  override def afterAll(): Unit = {
    dbms.shutdown()
  }

  // DEPRECATED PROCEDURE THINGS

  test("deprecated procedure calls without replacement") {
    val queries = Seq("CALL oldProcNotReplaced()", "CALL oldProcNotReplaced() RETURN 1")
    val detail = NotificationDetail.deprecatedName("oldProcNotReplaced")
    assertNotification(
      queries,
      shouldContainNotification = true,
      detail,
      (pos, detail) => deprecatedProcedureWithoutReplacement(pos, detail, "oldProcNotReplaced"),
      List(
        TestGqlStatusObject(
          STATUS_01N02.getStatusString,
          "warn: feature deprecated without replacement. `oldProcNotReplaced` is deprecated and will be removed without a replacement.",
          SeverityLevel.WARNING,
          NotificationClassification.DEPRECATION
        ),
        testOmittedResult
      )
    )
  }

  test("deprecated procedure calls with replacement") {
    val queries = Seq("CALL oldProc()", "CALL oldProc() RETURN 1")
    val detail = NotificationDetail.deprecatedName("oldProc", "newProc")
    assertNotification(
      queries,
      shouldContainNotification = true,
      detail,
      (pos, detail) => deprecatedProcedureWithReplacement(pos, detail, "oldProc", "newProc"),
      List(
        TestGqlStatusObject(
          STATUS_01N01.getStatusString,
          "warn: feature deprecated with replacement. `oldProc` is deprecated. It is replaced by `newProc`.",
          SeverityLevel.WARNING,
          NotificationClassification.DEPRECATION
        ),
        testOmittedResult
      )
    )
  }

  test("non-deprecated procedure calls") {
    val queries = Seq("CALL newProc()", "CALL newProc() RETURN 1")

    assertNoDeprecations(queries)
  }

  test("deprecated procedure result field") {
    val query = "CALL changedProc() YIELD oldField RETURN oldField"
    val detail = NotificationDetail.deprecatedField("changedProc", "oldField")
    assertNotification(
      Seq(query),
      shouldContainNotification = true,
      detail,
      (pos, detail) => deprecatedProcedureReturnField(pos, detail, "changedProc", "oldField"),
      List(
        TestGqlStatusObject(
          STATUS_01N03.getStatusString,
          "warn: procedure field deprecated. `oldField` for procedure `changedProc` is deprecated.",
          SeverityLevel.WARNING,
          NotificationClassification.DEPRECATION
        ),
        testOmittedResult
      )
    )
  }

  test("non-deprecated procedure result field") {
    val queries = Seq("CALL changedProc() YIELD newField RETURN newField")

    assertNoDeprecations(queries)
  }

  test("functions with deprecated input fields") {
    val queries = Seq(
      "RETURN org.example.com.FuncWithDepInput(1)",
      "MATCH (n) WHERE org.example.com.FuncWithDepInput(1) = 1 RETURN n",
      "MATCH (n) WHERE toString(org.example.com.FuncWithDepInput(1)) = 1 RETURN n"
    )
    val detail = NotificationDetail.deprecatedInputField("org.example.com.FuncWithDepInput", "value")
    assertNotification(
      queries,
      shouldContainNotification = true,
      detail,
      (pos, detail) => deprecatedFunctionField(pos, detail, "org.example.com.FuncWithDepInput", "value"),
      List(
        TestGqlStatusObject(
          STATUS_01N00.getStatusString,
          "warn: feature deprecated. `value` used by the function `org.example.com.FuncWithDepInput` is deprecated.",
          SeverityLevel.WARNING,
          NotificationClassification.DEPRECATION
        ),
        testOmittedResult
      )
    )
  }

  test("procedures with deprecated input fields") {
    val queries = Seq(
      "CALL changedProc2(1)"
    )
    val detail = NotificationDetail.deprecatedInputField("changedProc2", "value")
    assertNotification(
      queries,
      shouldContainNotification = true,
      detail,
      (pos, detail) => deprecatedProcedureField(pos, detail, "changedProc2", "value"),
      List(
        TestGqlStatusObject(
          STATUS_01N00.getStatusString,
          "warn: feature deprecated. `value` used by the procedure `changedProc2` is deprecated.",
          SeverityLevel.WARNING,
          NotificationClassification.DEPRECATION
        ),
        testOmittedResult
      )
    )
  }

  test("procedures with deprecated input fields and deprecated function") {
    val queries = Seq(
      "CALL changedProc2(org.example.com.oldFuncNotReplaced())"
    )

    val expectedGqlStatusObjects = List(
      TestGqlStatusObject(
        STATUS_01N00.getStatusString,
        "warn: feature deprecated. `value` used by the procedure `changedProc2` is deprecated.",
        SeverityLevel.WARNING,
        NotificationClassification.DEPRECATION
      ),
      TestGqlStatusObject(
        STATUS_01N02.getStatusString,
        "warn: feature deprecated without replacement. `org.example.com.oldFuncNotReplaced` is deprecated and will be removed without a replacement.",
        SeverityLevel.WARNING,
        NotificationClassification.DEPRECATION
      ),
      testOmittedResult
    )

    val detail1 = NotificationDetail.deprecatedInputField("changedProc2", "value")
    assertNotification(
      queries,
      shouldContainNotification = true,
      detail1,
      (pos, detail) => deprecatedProcedureField(pos, detail, "changedProc2", "value"),
      expectedGqlStatusObjects
    )
    val detail2 = NotificationDetail.deprecatedName("org.example.com.oldFuncNotReplaced")
    assertNotification(
      queries,
      shouldContainNotification = true,
      detail2,
      (pos, detail) => deprecatedFunctionWithoutReplacement(pos, detail, "org.example.com.oldFuncNotReplaced"),
      expectedGqlStatusObjects
    )
  }

  test("deprecated function calls without replacement") {
    val queries = Seq(
      "RETURN org.example.com.oldFuncNotReplaced()",
      "MATCH (n) WHERE org.example.com.oldFuncNotReplaced() = 1 RETURN n",
      "MATCH (n) WHERE toString(org.example.com.oldFuncNotReplaced()) = 1 RETURN n"
    )
    val detail = NotificationDetail.deprecatedName("org.example.com.oldFuncNotReplaced")
    assertNotification(
      queries,
      shouldContainNotification = true,
      detail,
      (pos, detail) => deprecatedFunctionWithoutReplacement(pos, detail, "org.example.com.oldFuncNotReplaced"),
      List(
        TestGqlStatusObject(
          STATUS_01N02.getStatusString,
          "warn: feature deprecated without replacement. `org.example.com.oldFuncNotReplaced` is deprecated and will be removed without a replacement.",
          SeverityLevel.WARNING,
          NotificationClassification.DEPRECATION
        ),
        testOmittedResult
      )
    )
  }

  test("deprecated function calls with replacement") {
    val queries = Seq(
      "RETURN org.example.com.oldFunc()",
      "MATCH (n) WHERE org.example.com.oldFunc() = 1 RETURN n",
      "MATCH (n) WHERE toString(org.example.com.oldFunc()) = 1 RETURN n"
    )
    val detail = NotificationDetail.deprecatedName("org.example.com.oldFunc", "org.example.com.newFunc")
    assertNotification(
      queries,
      shouldContainNotification = true,
      detail,
      (pos, detail) =>
        deprecatedFunctionWithReplacement(pos, detail, "org.example.com.oldFunc", "org.example.com.newFunc"),
      List(
        TestGqlStatusObject(
          STATUS_01N01.getStatusString,
          "warn: feature deprecated with replacement. `org.example.com.oldFunc` is deprecated. It is replaced by `org.example.com.newFunc`.",
          SeverityLevel.WARNING,
          NotificationClassification.DEPRECATION
        ),
        testOmittedResult
      )
    )
  }

  test("deprecated aggregation function calls") {
    val queries = Seq(
      "UNWIND [1, 2, 3] AS nums RETURN org.example.com.oldAggFunc(nums)",
      "UNWIND [1, 2, 3] AS nums WITH org.example.com.oldAggFunc(nums) AS aggTest RETURN aggTest"
    )
    val detail = NotificationDetail.deprecatedName("org.example.com.oldAggFunc", "org.example.com.newAggFunc")
    assertNotification(
      queries,
      shouldContainNotification = true,
      detail,
      (pos, detail) =>
        deprecatedFunctionWithReplacement(pos, detail, "org.example.com.oldAggFunc", "org.example.com.newAggFunc"),
      List(
        TestGqlStatusObject(
          STATUS_01N01.getStatusString,
          "warn: feature deprecated with replacement. `org.example.com.oldAggFunc` is deprecated. It is replaced by `org.example.com.newAggFunc`.",
          SeverityLevel.WARNING,
          NotificationClassification.DEPRECATION
        ),
        testOmittedResult
      )
    )
  }

  test("non-deprecated function calls") {
    val queries = Seq(
      "RETURN org.example.com.newFunc()",
      "MATCH (n) WHERE org.example.com.newFunc() = 1 RETURN n",
      "MATCH (n) WHERE toString(org.example.com.newFunc()) = 1 RETURN n"
    )

    assertNoDeprecations(queries)
  }

  test("non-deprecated aggregation function calls") {
    val queries = Seq(
      "UNWIND [1, 2, 3] AS nums RETURN org.example.com.newAggFunc(nums)",
      "UNWIND [1, 2, 3] AS nums WITH org.example.com.newAggFunc(nums) AS aggTest RETURN aggTest"
    )

    assertNoDeprecations(queries)
  }

  // DEPRECATIONS in 5.X

  test("deprecated legacy reltype separator") {

    val queries = Seq(
      "MATCH (a)-[:A|:B|:C]-() RETURN a"
    )

    assertNotification(
      queries,
      shouldContainNotification = true,
      deprecationNotificationDetail(":A|B|C"),
      (pos, detail) => deprecatedRelationshipTypeSeparator(pos, detail, ":A|:B|:C", ":A|B|C"),
      List(
        TestGqlStatusObject(
          STATUS_01N01.getStatusString,
          "warn: feature deprecated with replacement. `:A|:B|:C` is deprecated. It is replaced by `:A|B|C`.",
          SeverityLevel.WARNING,
          NotificationClassification.DEPRECATION
        ),
        TestGqlStatusObject(
          STATUS_01N51.getStatusString,
          "warn: unknown relationship type. The relationship type `A` does not exist. Verify that the spelling is correct.",
          SeverityLevel.WARNING,
          NotificationClassification.UNRECOGNIZED
        ),
        TestGqlStatusObject(
          STATUS_01N51.getStatusString,
          "warn: unknown relationship type. The relationship type `B` does not exist. Verify that the spelling is correct.",
          SeverityLevel.WARNING,
          NotificationClassification.UNRECOGNIZED
        ),
        TestGqlStatusObject(
          STATUS_01N51.getStatusString,
          "warn: unknown relationship type. The relationship type `C` does not exist. Verify that the spelling is correct.",
          SeverityLevel.WARNING,
          NotificationClassification.UNRECOGNIZED
        ),
        testOmittedResult
      )
    )

    // clear caches of the rewritten queries to not keep notifications around
    dbms.clearQueryCaches()
  }

  test("deprecate using nodes/relationships on the RHS of a Set Clause") {
    assertNotification(
      Seq("MATCH (g)-[r:KNOWS]->(k) SET g = r"),
      shouldContainNotification = true,
      pos => deprecatedNodeOrRelationshipOnRhsSetClause(pos, "SET g = r", "SET g = properties(r)"),
      List(
        TestGqlStatusObject(
          STATUS_01N01.getStatusString,
          "warn: feature deprecated with replacement. `SET g = r` is deprecated. It is replaced by `SET g = properties(r)`.",
          SeverityLevel.WARNING,
          NotificationClassification.DEPRECATION
        ),
        testOmittedResult
      )
    )

    assertNotification(
      Seq("MATCH (g)-[r:KNOWS]->(k) SET g = k"),
      shouldContainNotification = true,
      pos => deprecatedNodeOrRelationshipOnRhsSetClause(pos, "SET g = k", "SET g = properties(k)"),
      List(
        TestGqlStatusObject(
          STATUS_01N01.getStatusString,
          "warn: feature deprecated with replacement. `SET g = k` is deprecated. It is replaced by `SET g = properties(k)`.",
          SeverityLevel.WARNING,
          NotificationClassification.DEPRECATION
        ),
        testOmittedResult
      )
    )

    assertNotification(
      Seq("MATCH (g)-[r:KNOWS]->(k) SET g += r"),
      shouldContainNotification = true,
      pos => deprecatedNodeOrRelationshipOnRhsSetClause(pos, "SET g += r", "SET g += properties(r)"),
      List(
        TestGqlStatusObject(
          STATUS_01N01.getStatusString,
          "warn: feature deprecated with replacement. `SET g += r` is deprecated. It is replaced by `SET g += properties(r)`.",
          SeverityLevel.WARNING,
          NotificationClassification.DEPRECATION
        ),
        testOmittedResult
      )
    )

    assertNotification(
      Seq("MATCH (g)-[r:KNOWS]->(k) SET g += k"),
      shouldContainNotification = true,
      pos => deprecatedNodeOrRelationshipOnRhsSetClause(pos, "SET g += k", "SET g += properties(k)"),
      List(
        TestGqlStatusObject(
          STATUS_01N01.getStatusString,
          "warn: feature deprecated with replacement. `SET g += k` is deprecated. It is replaced by `SET g += properties(k)`.",
          SeverityLevel.WARNING,
          NotificationClassification.DEPRECATION
        ),
        testOmittedResult
      )
    )
  }

  test("do not deprecate using map on the RHS of a Set Clause") {
    val query =
      """
        |WITH {id:1} as map
        |CREATE (n:Test)
        |SET n = map""".stripMargin

    assertNoDeprecations(Seq(query))
  }

  test("do not deprecate using additive map on the RHS of a Set Clause") {
    val query =
      """
        |WITH {id:1} as map
        |CREATE (n:Test {prop:'val'})
        |SET n += map""".stripMargin

    assertNoDeprecations(Seq(query))
  }

  test("deprecate fixed length relationships in shortestPath and allShortestPaths") {
    assertNotification(
      Seq("MATCH (a), (b), allShortestPaths((a)-[r]->(b)) RETURN b"),
      shouldContainNotification = true,
      pos =>
        deprecatedShortestPathWithFixedLengthRelationship(
          pos,
          "allShortestPaths((a)-[r]->(b))",
          "allShortestPaths((a)-[r*1..1]->(b))"
        ),
      List(
        TestGqlStatusObject(
          STATUS_01N01.getStatusString,
          "warn: feature deprecated with replacement. `allShortestPaths((a)-[r]->(b))` is deprecated. It is replaced by `allShortestPaths((a)-[r*1..1]->(b))`.",
          SeverityLevel.WARNING,
          NotificationClassification.DEPRECATION
        ),
        testOmittedResult
      )
    )

    assertNotification(
      Seq("MATCH (a), (b), shortestPath((a)<-[r:TYPE]-(b)) RETURN b"),
      shouldContainNotification = true,
      pos =>
        deprecatedShortestPathWithFixedLengthRelationship(
          pos,
          "shortestPath((a)<-[r:TYPE]-(b))",
          "shortestPath((a)<-[r:TYPE*1..1]-(b))"
        ),
      List(
        TestGqlStatusObject(
          STATUS_01N01.getStatusString,
          "warn: feature deprecated with replacement. `shortestPath((a)<-[r:TYPE]-(b))` is deprecated. It is replaced by `shortestPath((a)<-[r:TYPE*1..1]-(b))`.",
          SeverityLevel.WARNING,
          NotificationClassification.DEPRECATION
        ),
        TestGqlStatusObject(
          STATUS_01N51.getStatusString,
          "warn: unknown relationship type. The relationship type `TYPE` does not exist. Verify that the spelling is correct.",
          SeverityLevel.WARNING,
          NotificationClassification.UNRECOGNIZED
        ),
        testOmittedResult
      )
    )
  }

  test("deprecate explicit use of old text index provider") {
    val deprecatedProvider = TextIndexProvider.DESCRIPTOR.name()
    val deprecatedProviderQueries = Seq(
      s"CREATE TEXT INDEX FOR (n:Label) ON (n.prop) OPTIONS {indexProvider : '$deprecatedProvider'}",
      s"CREATE TEXT INDEX FOR ()-[r:TYPE]-() ON (r.prop) OPTIONS {indexProvider : '$deprecatedProvider'}"
    )
    assertNotification(
      deprecatedProviderQueries,
      shouldContainNotification = true,
      deprecatedTextIndexProvider,
      List(
        TestGqlStatusObject(
          STATUS_01N01.getStatusString,
          "warn: feature deprecated with replacement. `text-1.0` is deprecated. It is replaced by `text-2.0`.",
          SeverityLevel.WARNING,
          NotificationClassification.DEPRECATION
        ),
        testOmittedResult
      )
    )

    val validProvider = TrigramIndexProvider.DESCRIPTOR.name()
    val validProviderQueries = Seq(
      s"CREATE TEXT INDEX FOR (n:Label) ON (n.prop) OPTIONS {indexProvider : '$validProvider'}",
      s"CREATE TEXT INDEX FOR ()-[r:TYPE]-() ON (r.prop) OPTIONS {indexProvider : '$validProvider'}"
    )
    assertNoDeprecations(validProviderQueries)
  }

  test("do not deprecate using the same variable name for several variable length relationships in the same pattern") {
    val queries = Seq(
      "MATCH ()-[r*]->(), ()-[r*]->() RETURN r",
      "MATCH ()-[r*..5]->(), ()<-[r*]-() RETURN r",
      "MATCH p = (a)-[r*]->(t), q=(b)-[r*]->(s) RETURN p, q",
      "MATCH p = (a)-[r*]-(t), q=(b)-[r*3..]-(s) RETURN p, q",
      "MATCH p = ()-[r*]->()-[r*]->() RETURN p",
      "MATCH p = ()-[r*2]->()-[r*1..3]->() RETURN p",
      "MATCH ()-[r*]-() WHERE COUNT {()-[r*]-()-[r*]-()} > 2 RETURN r",
      "MATCH ()-[r*]-() WHERE EXISTS {()-[r*]-()-[r*]-()} RETURN r",
      "MATCH ()-[r*]-() RETURN [ ()-[r*]-()-[r*]-() | r ] AS rs"
    )
    assertNoDeprecations(
      queries
    )
  }

  test("do not deprecate using the same variable name for variable length relationships across patterns") {
    val queries = Seq(
      "MATCH ()-[s*]->() MATCH ()-[s*]->() RETURN s",
      "MATCH ()-[s*]->() MATCH ()-[r*]->() MATCH ()-[s*]->() RETURN r, s",
      "MATCH p = ()-[s*]->() MATCH q = ()-[s*]->() RETURN p, q",
      "MATCH ()-[s*]-() WHERE COUNT {()-[s*]-()} > 2 RETURN s",
      "MATCH ()-[s*]-() WHERE EXISTS {()-[s*]-()} RETURN s",
      "MATCH ()-[s*]-() RETURN [ ()-[s*]-() | s ] AS rs",
      """
        |MATCH ()-[r]->()
        |MATCH ()-[q]->()
        |WITH [r,q] AS s
        |MATCH p = ()-[s*]->()
        |RETURN p
        |""".stripMargin
    )
    assertNoDeprecations(
      queries
    )
  }

  test("should not deprecate valid repeat of variable length relationship") {
    val queries = Seq(
      "MATCH ()-[r*]->() RETURN r",
      "MATCH ()-[r*]->() WITH r as s MATCH ()-[r*]->() RETURN r, s",
      """MATCH ()-[r*]->()
        |CALL () {
        | MATCH (a)-[r*]->()
        | RETURN a AS a
        |}
        |RETURN r
        |""".stripMargin,
      """
        |MATCH ()-[r*]->()
        |MATCH ()-[s*]->()
        |WITH [r, s] AS rs
        |RETURN rs
        |""".stripMargin
    )
    assertNoDeprecations(queries)
  }

  test("deprecate using a different order in Union returns") {
    val queries = Seq(
      "MATCH (a)-[]-(b) RETURN a, b UNION MATCH (c)-[]-(d) RETURN c as b, d as a",
      "RETURN 'val' as one, 'val' as two UNION RETURN 'val' as two, 'val' as one",
      "RETURN 'val' as one, 'val' as two UNION RETURN 'val' as one, 'val' as two UNION RETURN 'val' as two, 'val' as one",
      "MATCH (a)-[]-(b) RETURN a, b UNION ALL MATCH (c)-[]-(d) RETURN c as b, d as a",
      "RETURN 'val' as one, 'val' as two UNION ALL RETURN 'val' as two, 'val' as one",
      "RETURN 'val' as one, 'val' as two UNION ALL RETURN 'val' as one, 'val' as two UNION ALL RETURN 'val' as two, 'val' as one",
      "RETURN COUNT { MATCH (a)-[]-(b) RETURN a, b UNION MATCH (a)-[]-(b) RETURN b, a }"
    )
    assertNotification(
      queries,
      shouldContainNotification = true,
      unionReturnOrder,
      List(
        TestGqlStatusObject(
          STATUS_01N00.getStatusString,
          "warn: feature deprecated. All subqueries in a UNION [ALL] should have the same ordering for the return columns. Using differently ordered return items in a UNION [ALL] clause is deprecated and will be removed in a future version.",
          SeverityLevel.WARNING,
          NotificationClassification.DEPRECATION
        ),
        testOmittedResult
      ),
      cypherVersions = Set(CypherVersion.cypher5)
    )
  }

  test("should not deprecate valid Union return orders") {
    val queries = Seq(
      "RETURN 'val' as one, 'val' as two UNION RETURN 'val' as one, 'val' as two",
      "MATCH (a)-[]-(b) RETURN a, b UNION MATCH (c)-[]-(d) RETURN c as a, d as b",
      "MATCH (a)-[]-(b) RETURN * UNION MATCH (a)-[]-(b) RETURN a, b",
      "MATCH (a)-[]-(b) RETURN a, b UNION MATCH (a)-[]-(b) RETURN *",
      "MATCH (a)-[]-(b) RETURN a, b UNION MATCH (a)-[]-(b) RETURN a, b UNION MATCH (a)-[]-(b) RETURN *",
      "MATCH (a)-[]-(b) RETURN a, b UNION MATCH (a)-[]-(b) RETURN * UNION MATCH (a)-[]-(b) RETURN *",
      "MATCH (a)-[]-(b) RETURN a, b UNION MATCH (a)-[]-(b) RETURN * UNION MATCH (a)-[]-(b) RETURN a, b",
      "MATCH (a)-[]-(b) RETURN * UNION MATCH (a)-[]-(b) RETURN * UNION MATCH (a)-[]-(b) RETURN a, b",
      "MATCH (a)-[]-(b) RETURN * UNION MATCH (a)-[]-(b) RETURN a, b UNION MATCH (a)-[]-(b) RETURN a, b",
      "MATCH (a)-[]-(b) RETURN * UNION MATCH (a)-[]-(b) RETURN a, b UNION MATCH (a)-[]-(b) RETURN *",
      "RETURN COUNT { MATCH (a)-[]-(b) RETURN a, b UNION MATCH (a)-[]-(b) RETURN a, b }",
      "MATCH (a)-[]-(b) RETURN * UNION MATCH (a)-[]-(b) RETURN *",
      "RETURN 'val' as one, 'val' as two UNION ALL RETURN 'val' as one, 'val' as two",
      "MATCH (a)-[]-(b) RETURN a, b UNION ALL MATCH (c)-[]-(d) RETURN c as a, d as b",
      "MATCH (a)-[]-(b) RETURN * UNION ALL MATCH (a)-[]-(b) RETURN a, b",
      "MATCH (a)-[]-(b) RETURN a, b UNION ALL MATCH (a)-[]-(b) RETURN *",
      "MATCH (a)-[]-(b) RETURN a, b UNION ALL MATCH (a)-[]-(b) RETURN a, b UNION ALL MATCH (a)-[]-(b) RETURN *",
      "MATCH (a)-[]-(b) RETURN a, b UNION ALL MATCH (a)-[]-(b) RETURN * UNION ALL MATCH (a)-[]-(b) RETURN *",
      "MATCH (a)-[]-(b) RETURN a, b UNION ALL MATCH (a)-[]-(b) RETURN * UNION ALL MATCH (a)-[]-(b) RETURN a, b",
      "MATCH (a)-[]-(b) RETURN * UNION ALL MATCH (a)-[]-(b) RETURN * UNION ALL MATCH (a)-[]-(b) RETURN a, b",
      "MATCH (a)-[]-(b) RETURN * UNION ALL MATCH (a)-[]-(b) RETURN a, b UNION ALL MATCH (a)-[]-(b) RETURN a, b",
      "MATCH (a)-[]-(b) RETURN * UNION ALL MATCH (a)-[]-(b) RETURN a, b UNION ALL MATCH (a)-[]-(b) RETURN *",
      "RETURN COUNT { MATCH (a)-[]-(b) RETURN a, b UNION ALL MATCH (a)-[]-(b) RETURN a, b }",
      "MATCH (a)-[]-(b) RETURN * UNION ALL MATCH (a)-[]-(b) RETURN *"
    )
    assertNoDeprecations(queries)
  }

  test("deprecate using a function id()") {
    val queries = Seq(
      "MATCH (a) RETURN id(a)",
      "MATCH (a) RETURN iD(a)",
      "MATCH (a) RETURN Id(a)",
      "MATCH (a) RETURN ID(a)",
      "RETURN id(null)",
      "MATCH ()-[r]->() RETURN id(r)"
    )
    val detail = NotificationDetail.deprecatedName("id")

    assertNotification(
      queries,
      shouldContainNotification = true,
      detail,
      (pos, detail) => deprecatedFunctionWithoutReplacement(pos, detail, "id"),
      List(
        TestGqlStatusObject(
          STATUS_01N02.getStatusString,
          "warn: feature deprecated without replacement. `id` is deprecated and will be removed without a replacement.",
          SeverityLevel.WARNING,
          NotificationClassification.DEPRECATION
        ),
        testOmittedResult
      )
    )
  }

  test("connectComponentsPlanner pre parser option is deprecated") {
    val queries = Seq(
      "CYPHER connectComponentsPlanner=idp RETURN 1",
      "CYPHER connectComponentsPlanner=greedy RETURN 1"
    )
    assertNotification(
      queries,
      shouldContainNotification = true,
      deprecatedConnectComponentsPlannerPreParserOption,
      List(
        TestGqlStatusObject(
          STATUS_01N02.getStatusString,
          "warn: feature deprecated without replacement. `connectComponentsPlanner` is deprecated and will be removed without a replacement.",
          SeverityLevel.WARNING,
          NotificationClassification.DEPRECATION
        ),
        testOmittedResult
      )
    )
  }

  test("Deprecate property references across patterns in CREATE") {
    val deprecated = Seq(
      "CREATE (a {foo:1}), (b {foo:a.foo})",
      "CREATE (b {prop: a.prop}), (a)",
      "CREATE (a), (b)-[r: REL {prop: a.prop}]->(c)",
      "CREATE (b)-[r: REL {prop: a.prop}]->(c), (a)",
      "CREATE (b)-[a: REL]->(c), (d {prop:a.prop})",
      "CREATE (a), (b {prop: EXISTS {(a)-->()}})",
      "CREATE (b {prop: EXISTS {(a)-->()}}), (a)",
      "CREATE (a), (a)-[:REL]->({prop:a.prop})",
      "CREATE (a), (b {prop: labels(a)})",
      "CREATE (a), (b {prop: true IN [x IN labels(a) | true]})"
    )

    val notDeprecated = Seq(
      "MATCH (n) CREATE (a {prop: n.prop})",
      "MATCH (a) CREATE (a)-[:REL]->({prop:a.prop})",
      "CREATE (a)-[:REL]->(a)",
      "CREATE (a), (a)-[:REL]->(b)",

      // These cases are shadowing and not references so should not be deprecated
      "CREATE (n {prop: true IN [n IN [false] | true]})",
      "CREATE (n {prop: true IN [n IN [false] | n]})",
      "CREATE (a)-[r:R {prop: true IN [r IN [false] | true]}]->(b)",
      "CREATE (a)-[r:R {prop: true IN [r IN [false] | r]}]->(b)",
      "CREATE (a)-[r:R {prop: true IN [a IN [false] | a]}]->(b)",
      "CREATE (a)-[r:R]->(b {prop: true IN [r IN [false] | r]})",
      "CREATE (a)-[r:R]->(b {prop: true IN [a IN [false] | a]})",
      "MATCH p=()-[]->() CREATE (a)-[r:R {prop: true IN [a in nodes(p) | a.prop = 1]}]->(b)"
    )

    assertNotification(
      deprecated,
      shouldContainNotification = true,
      "a",
      deprecatedPropertyReferenceInCreate,
      List(
        TestGqlStatusObject(
          STATUS_01N00.getStatusString,
          "warn: feature deprecated. Creating an entity (a) and referencing that entity in a property definition in the same CREATE is deprecated.",
          SeverityLevel.WARNING,
          NotificationClassification.DEPRECATION
        ),
        testOmittedResult
      ),
      cypherVersions = Set(CypherVersion.cypher5)
    )

    assertNoDeprecations(notDeprecated)
  }

  test("Deprecate property references across patterns in MERGE") {
    val deprecated = Seq(
      "MERGE (a {prop:'p'})-[:T]->(b {prop:a.prop})",
      "MERGE (a {prop:'p'})<-[:T]-(b {prop:a.prop})",
      "MERGE (a {prop:'p'})-[:T]-(b {prop:a.prop})",
      "CREATE ({prop:'p'})-[:T]->({prop:'p'}) MERGE (b {prop:a.prop})-[:T]->(a {prop:'p'})",
      "MERGE (a {prop:'p'})-[b:T {prop:a.prop}]->()",
      "MERGE ()-[a:T {prop:'p'}]->()<-[b :S {prop:a.prop}]-()",
      "FOREACH (x in [1,2,3] | MERGE (a {prop:'p'})-[:R]-(b {prop:a.prop}))"
    )

    val notDeprecated = Seq(
      "MATCH (a {prop:'p'}) MERGE (b {prop:a.prop})",
      "MERGE (a {prop:'p'}) MERGE (a)-[:T]->(b {prop:a.prop})"
    )

    assertNotification(
      deprecated,
      shouldContainNotification = true,
      "a",
      deprecatedPropertyReferenceInMerge,
      List(
        TestGqlStatusObject(
          STATUS_01N00.getStatusString,
          "warn: feature deprecated. Merging an entity (a) and referencing that entity in a property definition in the same MERGE is deprecated.",
          SeverityLevel.WARNING,
          NotificationClassification.DEPRECATION
        ),
        testOmittedResult
      ),
      cypherVersions = Set(CypherVersion.cypher5)
    )

    assertNoDeprecations(notDeprecated)
  }

  test("Deprecate unicode '\\u0085' if used in identifiers") {
    val deprecated = Seq(
      "CREATE (a {f\\u0085oo:1})",
      "CREATE (f\\u0085oo {a:1})",
      "WITH 1 as f\\u0085oo return *"
    )

    val notDeprecated = Seq(
      "CREATE (a {`f\\u0085oo`:1})",
      "CREATE (`f\\u0085oo` {a:1})",
      "WITH 1 as `f\\u0085oo` return *"
    )

    assertNotification(
      deprecated,
      shouldContainNotification = true,
      deprecatedIdentifierWhitespaceUnicode(_, '\u0085', "f\u0085oo"),
      List(
        TestGqlStatusObject(
          STATUS_01N00.getStatusString,
          "warn: feature deprecated. The Unicode character `\\u0085` is deprecated for unescaped identifiers and will be considered as a whitespace character in the future. To continue using it, escape the identifier by adding backticks around the identifier `f\u0085oo`.",
          SeverityLevel.WARNING,
          NotificationClassification.DEPRECATION
        ),
        testOmittedResult
      ),
      Set(CypherVersion.cypher5)
    )

    assertNoDeprecations(notDeprecated)
  }

  // Parser Deprecations: See CIP-120
  private val deprecatedExtendedUnicodeChars = Seq(
    // Category Cc
    '\u0000', '\u0001', '\u0002', '\u0003', '\u0004', '\u0005', '\u0006', '\u0007',
    '\u0008', '\u000E', '\u000F', '\u0010', '\u0011', '\u0012', '\u0013', '\u0014',
    '\u0015', '\u0016', '\u0017', '\u0018', '\u0019', '\u001A', '\u001B', '\u007F',
    '\u0080', '\u0081', '\u0082', '\u0083', '\u0084', '\u0086', '\u0087', '\u0088',
    '\u0089', '\u008A', '\u008B', '\u008C', '\u008D', '\u008E', '\u008F', '\u0090',
    '\u0091', '\u0092', '\u0093', '\u0094', '\u0095', '\u0096', '\u0097', '\u0098',
    '\u0099', '\u009A', '\u009B', '\u009C', '\u009D', '\u009E', '\u009F',
    // Category Sc
    '\u0024', '\u00A2', '\u00A3', '\u00A4', '\u00A5',
    // Category Cf
    '\u00AD', '\u0600', '\u0601', '\u0602', '\u0603', '\u0604', '\u0605', '\u061C',
    '\u06DD', '\u070F', '\u08E2', '\u180E', '\u200B', '\u200C', '\u200D', '\u200E',
    '\u200F', '\u202A', '\u202B', '\u202C', '\u202D', '\u202E', '\u2060', '\u2061',
    '\u2062', '\u2063', '\u2064', '\u2066', '\u2067', '\u2068', '\u2069', '\u206A',
    '\u206B', '\u206C', '\u206D', '\u206E', '\u206F', '\u2E2F', '\uFEFF', '\uFFF9',
    '\uFFFA', '\uFFFB'
  )

  test("Deprecated Unicode Characters in Identifier Extend") {
    // Add label and reltype to the database to avoid unrecognized warnings
    val transaction = dbms.begin()

    // Kernel already errors on the unicode char: \u0000 in tokens, so skip over
    deprecatedExtendedUnicodeChars.filterNot(unicode => unicode == '\u0000').foreach(deprecatedUnicode => {
      transaction.execute(s"CALL db.createLabel('a${deprecatedUnicode}bc')")
      transaction.execute(s"CALL db.createRelationshipType('a${deprecatedUnicode}bc')")
    })
    transaction.commit()

    val deprecatedParamQueries: Seq[(Char, Seq[String])] = deprecatedExtendedUnicodeChars.map { deprecatedUnicodeChar =>
      // Kernel already errors on the unicode char: \u0000 in tokens, so skip over for some queries
      if (deprecatedUnicodeChar == '\u0000') {
        deprecatedUnicodeChar -> Seq(
          s"RETURN { a${deprecatedUnicodeChar}bc : 1 }",
          s"WITH 1 AS a${deprecatedUnicodeChar}bc RETURN 1"
        )
      } else {
        deprecatedUnicodeChar -> Seq(
          s"RETURN { a${deprecatedUnicodeChar}bc : 1 }",
          s"WITH 1 AS a${deprecatedUnicodeChar}bc RETURN 1",
          s"MATCH (b:a${deprecatedUnicodeChar}bc) RETURN b",
          s"MATCH ()-[r:a${deprecatedUnicodeChar}bc]->() RETURN r"
        )
      }

    }

    def expectedGqlStatuses(unicode: Char): List[TestGqlStatusObject] = {
      val unicodeString = f"\\u${Integer.valueOf(unicode)}%04x"

      List(
        TestGqlStatusObject(
          STATUS_01N00.getStatusString,
          s"warn: feature deprecated. The character with the Unicode representation `$unicodeString` is deprecated for unescaped identifiers and will not be supported in the future. " +
            s"To continue using it, escape the identifier by adding backticks around the identifier `a${unicode}bc`.",
          SeverityLevel.WARNING,
          NotificationClassification.DEPRECATION
        ),
        testOmittedResult
      )
    }

    deprecatedParamQueries.foreach { deprecateQueries =>
      assertNotification(
        deprecateQueries._2,
        shouldContainNotification = true,
        deprecatedIdentifierUnicode(_, deprecateQueries._1, s"a${deprecateQueries._1}bc"),
        expectedGqlStatuses(deprecateQueries._1),
        Set(CypherVersion.cypher5)
      )
    }
  }

  test("Deprecated Unicode Characters in Identifier Extend with parameter") {
    val deprecatedParamQueries: Seq[(Char, Seq[String])] = deprecatedExtendedUnicodeChars.map { deprecatedUnicodeChar =>
      deprecatedUnicodeChar -> Seq(s"RETURN $$a${deprecatedUnicodeChar}bc")
    }

    def expectedGqlStatuses(unicode: Char): List[TestGqlStatusObject] = {
      val unicodeString = f"\\u${Integer.valueOf(unicode)}%04x"

      List(
        TestGqlStatusObject(
          STATUS_01N00.getStatusString,
          s"warn: feature deprecated. The character with the Unicode representation `$unicodeString` is deprecated for unescaped identifiers and will not be supported in the future. " +
            s"To continue using it, escape the identifier by adding backticks around the identifier `a${unicode}bc`.",
          SeverityLevel.WARNING,
          NotificationClassification.DEPRECATION
        ),
        TestGqlStatusObject(
          STATUS_01N60.getStatusString,
          s"warn: parameter missing. The query plan cannot be cached and is not executable without `EXPLAIN` due to the undefined parameter(s) `$$a${unicode}bc`. Provide the parameter(s).",
          SeverityLevel.WARNING,
          NotificationClassification.GENERIC
        ),
        testOmittedResult
      )
    }

    deprecatedParamQueries.foreach { deprecateQueries =>
      assertNotification(
        deprecateQueries._2,
        shouldContainNotification = true,
        deprecatedIdentifierUnicode(_, deprecateQueries._1, s"a${deprecateQueries._1}bc"),
        expectedGqlStatuses(deprecateQueries._1),
        Set(CypherVersion.cypher5)
      )
    }
  }

  test("Multiple deprecated Unicode Characters in Identifier") {
    val deprecatedStartUnicodeChar = '\u2e2f'
    val deprecatedExtendedUnicodeChar = '\u206E'

    // Add label and reltype to the database to avoid unrecognized warnings
    val transaction = dbms.begin()
    transaction.execute(s"CALL db.createLabel('${deprecatedStartUnicodeChar}b${deprecatedExtendedUnicodeChar}c')")
    transaction.execute(
      s"CALL db.createRelationshipType('${deprecatedStartUnicodeChar}b${deprecatedExtendedUnicodeChar}c')"
    )
    transaction.commit()

    val queriesWithDeprecatedStartChar = Seq(
      s"RETURN { ${deprecatedStartUnicodeChar}b${deprecatedExtendedUnicodeChar}c : 1 }",
      s"WITH 1 AS ${deprecatedStartUnicodeChar}b${deprecatedExtendedUnicodeChar}c RETURN 1",
      s"MATCH (b:${deprecatedStartUnicodeChar}b${deprecatedExtendedUnicodeChar}c) RETURN b",
      s"MATCH ()-[r:${deprecatedStartUnicodeChar}b${deprecatedExtendedUnicodeChar}c]->() RETURN r"
    )

    val expectedGqlStatuses = List(
      TestGqlStatusObject(
        STATUS_01N00.getStatusString,
        "warn: feature deprecated. The character with the Unicode representation `\\u206e` is deprecated for unescaped identifiers and will not be supported in the future. To continue using it, escape the identifier by adding backticks around the identifier `ⸯb\u206Ec`.",
        SeverityLevel.WARNING,
        NotificationClassification.DEPRECATION
      ),
      TestGqlStatusObject(
        STATUS_01N00.getStatusString,
        "warn: feature deprecated. The character with the Unicode representation `\\u2e2f` is deprecated for unescaped identifiers and will not be supported in the future. To continue using it, escape the identifier by adding backticks around the identifier `ⸯb\u206Ec`.",
        SeverityLevel.WARNING,
        NotificationClassification.DEPRECATION
      ),
      testOmittedResult
    )

    assertNotification(
      queriesWithDeprecatedStartChar,
      shouldContainNotification = true,
      deprecatedIdentifierUnicode(
        _,
        deprecatedStartUnicodeChar,
        s"${deprecatedStartUnicodeChar}b${deprecatedExtendedUnicodeChar}c"
      ),
      expectedGqlStatuses,
      Set(CypherVersion.cypher5)
    )

    assertNotification(
      queriesWithDeprecatedStartChar,
      shouldContainNotification = true,
      deprecatedIdentifierUnicode(
        _,
        deprecatedExtendedUnicodeChar,
        s"${deprecatedStartUnicodeChar}b${deprecatedExtendedUnicodeChar}c"
      ),
      expectedGqlStatuses,
      Set(CypherVersion.cypher5)
    )
  }

  test("Multiple deprecated Unicode Characters in Identifier with parameter") {
    val deprecatedStartUnicodeChar = '\u2e2f'
    val deprecatedExtendedUnicodeChar = '\u206E'

    val queryWithDeprecatedStartChar = s"RETURN $$${deprecatedStartUnicodeChar}b${deprecatedExtendedUnicodeChar}c"

    val expectedGqlStatuses = List(
      TestGqlStatusObject(
        STATUS_01N00.getStatusString,
        "warn: feature deprecated. The character with the Unicode representation `\\u2e2f` is deprecated for unescaped identifiers and will not be supported in the future. To continue using it, escape the identifier by adding backticks around the identifier `\u2e2fb\u206Ec`.",
        SeverityLevel.WARNING,
        NotificationClassification.DEPRECATION
      ),
      TestGqlStatusObject(
        STATUS_01N00.getStatusString,
        "warn: feature deprecated. The character with the Unicode representation `\\u206e` is deprecated for unescaped identifiers and will not be supported in the future. To continue using it, escape the identifier by adding backticks around the identifier `\u2e2fb\u206Ec`.",
        SeverityLevel.WARNING,
        NotificationClassification.DEPRECATION
      ),
      TestGqlStatusObject(
        STATUS_01N60.getStatusString,
        "warn: parameter missing. The query plan cannot be cached and is not executable without `EXPLAIN` due to the undefined parameter(s) `$\u2e2fb\u206Ec`. Provide the parameter(s).",
        SeverityLevel.WARNING,
        NotificationClassification.GENERIC
      ),
      testOmittedResult
    )

    assertNotification(
      Seq(queryWithDeprecatedStartChar),
      shouldContainNotification = true,
      deprecatedIdentifierUnicode(
        _,
        deprecatedStartUnicodeChar,
        s"${deprecatedStartUnicodeChar}b${deprecatedExtendedUnicodeChar}c"
      ),
      expectedGqlStatuses,
      Set(CypherVersion.cypher5)
    )

    assertNotification(
      Seq(queryWithDeprecatedStartChar),
      shouldContainNotification = true,
      deprecatedIdentifierUnicode(
        _,
        deprecatedExtendedUnicodeChar,
        s"${deprecatedStartUnicodeChar}b${deprecatedExtendedUnicodeChar}c"
      ),
      expectedGqlStatuses,
      Set(CypherVersion.cypher5)
    )
  }

  test("Deprecated Unicode Characters in Identifier Start") {
    val deprecatedStartUnicodeChar = '\u2e2f'

    // Add label and reltype to the database to avoid unrecognized warnings
    val transaction = dbms.begin()
    transaction.execute(s"CALL db.createLabel('${deprecatedStartUnicodeChar}bc')")
    transaction.execute(s"CALL db.createRelationshipType('${deprecatedStartUnicodeChar}bc')")
    transaction.commit()

    val queriesWithDeprecatedStartChar = Seq(
      s"RETURN { ${deprecatedStartUnicodeChar}bc : 1 }",
      s"WITH 1 AS ${deprecatedStartUnicodeChar}bc RETURN 1",
      s"MATCH (b:${deprecatedStartUnicodeChar}bc) RETURN b",
      s"MATCH ()-[r:${deprecatedStartUnicodeChar}bc]->() RETURN r"
    )

    assertNotification(
      queriesWithDeprecatedStartChar,
      shouldContainNotification = true,
      deprecatedIdentifierUnicode(_, deprecatedStartUnicodeChar, s"${deprecatedStartUnicodeChar}bc"),
      List(
        TestGqlStatusObject(
          STATUS_01N00.getStatusString,
          "warn: feature deprecated. The character with the Unicode representation `\\u2e2f` is deprecated for unescaped identifiers and will not be supported in the future. To continue using it, escape the identifier by adding backticks around the identifier `\u2e2fbc`.",
          SeverityLevel.WARNING,
          NotificationClassification.DEPRECATION
        ),
        testOmittedResult
      ),
      Set(CypherVersion.cypher5)
    )
  }

  test("Deprecated Unicode Characters in Identifier Start with parameter") {
    val deprecatedStartUnicodeChar = '\u2e2f'
    val queryWithDeprecatedStartChar = s"RETURN $$${deprecatedStartUnicodeChar}bc"

    assertNotification(
      Seq(queryWithDeprecatedStartChar),
      shouldContainNotification = true,
      deprecatedIdentifierUnicode(_, deprecatedStartUnicodeChar, s"${deprecatedStartUnicodeChar}bc"),
      List(
        TestGqlStatusObject(
          STATUS_01N00.getStatusString,
          "warn: feature deprecated. The character with the Unicode representation `\\u2e2f` is deprecated for unescaped identifiers and will not be supported in the future. To continue using it, escape the identifier by adding backticks around the identifier `\u2e2fbc`.",
          SeverityLevel.WARNING,
          NotificationClassification.DEPRECATION
        ),
        TestGqlStatusObject(
          STATUS_01N60.getStatusString,
          "warn: parameter missing. The query plan cannot be cached and is not executable without `EXPLAIN` due to the undefined parameter(s) `$\u2e2fbc`. Provide the parameter(s).",
          SeverityLevel.WARNING,
          NotificationClassification.GENERIC
        ),
        testOmittedResult
      ),
      Set(CypherVersion.cypher5)
    )
  }

  test("deprecated subquerycall without variable clause non-importing") {
    val queries = Seq(
      "CALL{RETURN 1 AS b} RETURN b"
    )
    assertNotification(
      queries,
      shouldContainNotification = true,
      "",
      (pos, detail) => deprecatedImportingWithInSubqueryCall(pos, detail),
      List(
        TestGqlStatusObject(
          STATUS_01N00.getStatusString,
          "warn: feature deprecated. CALL subquery without a variable scope clause is now deprecated. Use CALL () { ... }",
          SeverityLevel.WARNING,
          NotificationClassification.DEPRECATION
        ),
        testOmittedResult
      )
    )
  }

  test("deprecated subquerycall without variable clause importing") {
    val queries = Seq(
      "WITH 1 AS a CALL{WITH a RETURN 1 AS b} RETURN b"
    )
    assertNotification(
      queries,
      shouldContainNotification = true,
      "a",
      (pos, detail) => deprecatedImportingWithInSubqueryCall(pos, detail),
      List(
        TestGqlStatusObject(
          STATUS_01N00.getStatusString,
          "warn: feature deprecated. CALL subquery without a variable scope clause is now deprecated. Use CALL (a) { ... }",
          SeverityLevel.WARNING,
          NotificationClassification.DEPRECATION
        ),
        testOmittedResult
      )
    )
  }
}
