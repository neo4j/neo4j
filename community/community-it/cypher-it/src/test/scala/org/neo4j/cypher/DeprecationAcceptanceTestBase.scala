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

import org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME
import org.neo4j.cypher.CypherITTestSuite
import org.neo4j.cypher.internal.javacompat.NotificationTestSupport.TestFunctions
import org.neo4j.cypher.internal.javacompat.NotificationTestSupport.TestProcedures
import org.neo4j.cypher.internal.options.CypherVersionOption
import org.neo4j.gqlstatus.GqlStatusInfoCodes.STATUS_01N00
import org.neo4j.gqlstatus.GqlStatusInfoCodes.STATUS_01N01
import org.neo4j.gqlstatus.GqlStatusInfoCodes.STATUS_01N02
import org.neo4j.gqlstatus.GqlStatusInfoCodes.STATUS_01N50
import org.neo4j.gqlstatus.GqlStatusInfoCodes.STATUS_01N51
import org.neo4j.gqlstatus.GqlStatusInfoCodes.STATUS_01N52
import org.neo4j.gqlstatus.GqlStatusInfoCodes.STATUS_01N60
import org.neo4j.gqlstatus.GqlStatusInfoCodes.STATUS_01N62
import org.neo4j.gqlstatus.GqlStatusInfoCodes.STATUS_01N84
import org.neo4j.gqlstatus.NotificationClassification
import org.neo4j.graphdb.InputPosition
import org.neo4j.graphdb.Notification
import org.neo4j.graphdb.SeverityLevel
import org.neo4j.internal.schema.AllIndexProviderDescriptors
import org.neo4j.notifications.NotificationCodeWithDescription.deprecatedConnectComponentsPlannerPreParserOption
import org.neo4j.notifications.NotificationCodeWithDescription.deprecatedEagerAnalyzerPreParserOption
import org.neo4j.notifications.NotificationCodeWithDescription.deprecatedFunctionField
import org.neo4j.notifications.NotificationCodeWithDescription.deprecatedFunctionWithReplacement
import org.neo4j.notifications.NotificationCodeWithDescription.deprecatedFunctionWithoutReplacement
import org.neo4j.notifications.NotificationCodeWithDescription.deprecatedIdentifierUnicode
import org.neo4j.notifications.NotificationCodeWithDescription.deprecatedIdentifierWhitespaceUnicode
import org.neo4j.notifications.NotificationCodeWithDescription.deprecatedImportingWithInSubqueryCall
import org.neo4j.notifications.NotificationCodeWithDescription.deprecatedKeywordVariableInWhenOperand
import org.neo4j.notifications.NotificationCodeWithDescription.deprecatedNodeOrRelationshipOnRhsSetClause
import org.neo4j.notifications.NotificationCodeWithDescription.deprecatedPrecedenceOfLabelExpressionPredicate
import org.neo4j.notifications.NotificationCodeWithDescription.deprecatedProcedureField
import org.neo4j.notifications.NotificationCodeWithDescription.deprecatedProcedureReturnField
import org.neo4j.notifications.NotificationCodeWithDescription.deprecatedProcedureWithReplacement
import org.neo4j.notifications.NotificationCodeWithDescription.deprecatedProcedureWithoutReplacement
import org.neo4j.notifications.NotificationCodeWithDescription.deprecatedPropertyReferenceInCreate
import org.neo4j.notifications.NotificationCodeWithDescription.deprecatedPropertyReferenceInMerge
import org.neo4j.notifications.NotificationCodeWithDescription.deprecatedRelationshipTypeSeparator
import org.neo4j.notifications.NotificationCodeWithDescription.deprecatedShortestPathWithFixedLengthRelationship
import org.neo4j.notifications.NotificationCodeWithDescription.deprecatedTextIndexProvider
import org.neo4j.notifications.NotificationCodeWithDescription.deprecatedWhereVariableInNodePattern
import org.neo4j.notifications.NotificationCodeWithDescription.deprecatedWhereVariableInRelationshipPattern
import org.neo4j.notifications.NotificationCodeWithDescription.missingLabel
import org.neo4j.notifications.NotificationCodeWithDescription.procedureWarning
import org.neo4j.notifications.NotificationCodeWithDescription.retiredPlannerVersionPreParserOption
import org.neo4j.notifications.NotificationDetail
import org.neo4j.notifications.NotificationDetail.deprecatedName
import org.neo4j.notifications.NotificationDetail.deprecationNotificationDetail
import org.neo4j.test.TestDatabaseManagementServiceFactorySupplier.isSpd
import org.scalatest.BeforeAndAfterAll

abstract class DeprecationAcceptanceTestBase extends CypherITTestSuite with BeforeAndAfterAll
    with DeprecationTestSupport {

  override def beforeAll(): Unit = {
    // Used for testing deprecated procedures
    dbms.registerProcedure(classOf[TestProcedures])
    dbms.registerFunction(classOf[TestFunctions])
    dbms.registerAggregationFunction(classOf[TestFunctions])
  }

  override def afterAll(): Unit = {
    dbms.shutdown()
  }

  private val dbName = if (isSpd) "neo4j-g000" else "neo4j"
  private val startPosition = new InputPosition(0, 1, 1)

  // DEPRECATED PROCEDURE THINGS

  test("deprecated procedure calls without replacement") {
    val queries = Seq("CALL oldProcNotReplaced()", "CALL oldProcNotReplaced() RETURN 1")
    val detail = NotificationDetail.deprecatedName("oldProcNotReplaced")
    assertNotification(
      queries,
      shouldContainNotification = true,
      detail,
      startPosition,
      (pos, detail) => deprecatedProcedureWithoutReplacement(pos, detail, "oldProcNotReplaced"),
      List(
        TestGqlStatusObject(
          STATUS_01N02.getStatusString,
          "warn: feature deprecated without replacement. oldProcNotReplaced is deprecated and will be removed without a replacement.",
          SeverityLevel.WARNING,
          NotificationClassification.DEPRECATION,
          startPosition
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
      startPosition,
      (pos, detail) => deprecatedProcedureWithReplacement(pos, detail, "oldProc", "newProc"),
      List(
        TestGqlStatusObject(
          STATUS_01N01.getStatusString,
          "warn: feature deprecated with replacement. oldProc is deprecated. It is replaced by newProc.",
          SeverityLevel.WARNING,
          NotificationClassification.DEPRECATION,
          startPosition
        ),
        testOmittedResult
      )
    )
  }

  test("notification on procedure calls with warning") {
    val queries = Seq("CALL procWithWarning()", "CALL procWithWarning() RETURN 1")

    val detail =
      NotificationDetail.procedureWarning("procWithWarning", "This procedure is unsafe, use at your own risk!")
    assertNotification(
      queries,
      shouldContainNotification = true,
      detail,
      startPosition,
      (pos, detail) =>
        procedureWarning(pos, detail, "procWithWarning", "This procedure is unsafe, use at your own risk!"),
      List(
        TestGqlStatusObject(
          STATUS_01N62.getStatusString,
          "warn: procedure or function execution warning. Execution of the procedure procWithWarning() generated the warning This procedure is unsafe, use at your own risk!",
          SeverityLevel.WARNING,
          NotificationClassification.GENERIC,
          startPosition
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
    val position = new InputPosition(25, 1, 26)
    val detail = NotificationDetail.deprecatedField("changedProc", "oldField")
    assertNotification(
      Seq(query),
      shouldContainNotification = true,
      detail,
      position,
      (pos, detail) => deprecatedProcedureReturnField(pos, detail, "changedProc", "oldField"),
      List(
        TestGqlStatusObject(
          STATUS_01N00.getStatusString,
          "warn: feature deprecated. `oldField` returned by the procedure `changedProc` is deprecated.",
          SeverityLevel.WARNING,
          NotificationClassification.DEPRECATION,
          position
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
    val examples = Seq(
      ("RETURN org.example.com.FuncWithDepInput(1)", new InputPosition(7, 1, 8)),
      ("MATCH (n) WHERE org.example.com.FuncWithDepInput(1) = 1 RETURN n", new InputPosition(16, 1, 17)),
      ("MATCH (n) WHERE toString(org.example.com.FuncWithDepInput(1)) = 1 RETURN n", new InputPosition(25, 1, 26))
    )
    val detail = NotificationDetail.deprecatedInputField("org.example.com.FuncWithDepInput", "value")

    examples.foreach {
      case (query, position) =>
        assertNotification(
          Seq(query),
          shouldContainNotification = true,
          detail,
          position,
          (pos, detail) => deprecatedFunctionField(pos, detail, "org.example.com.FuncWithDepInput", "value"),
          List(
            TestGqlStatusObject(
              STATUS_01N00.getStatusString,
              "warn: feature deprecated. `value` used by the function `org.example.com.FuncWithDepInput` is deprecated.",
              SeverityLevel.WARNING,
              NotificationClassification.DEPRECATION,
              position
            ),
            testOmittedResult
          )
        )
    }
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
      startPosition,
      (pos, detail) => deprecatedProcedureField(pos, detail, "changedProc2", "value"),
      List(
        TestGqlStatusObject(
          STATUS_01N00.getStatusString,
          "warn: feature deprecated. `value` used by the procedure `changedProc2` is deprecated.",
          SeverityLevel.WARNING,
          NotificationClassification.DEPRECATION,
          startPosition
        ),
        testOmittedResult
      )
    )
  }

  test("procedures with deprecated input fields and deprecated function") {
    val queries = Seq(
      "CALL changedProc2(org.example.com.oldFuncNotReplaced())"
    )

    val funcPos = new InputPosition(18, 1, 19)

    val expectedGqlStatusObjects = List(
      TestGqlStatusObject(
        STATUS_01N00.getStatusString,
        "warn: feature deprecated. `value` used by the procedure `changedProc2` is deprecated.",
        SeverityLevel.WARNING,
        NotificationClassification.DEPRECATION,
        startPosition
      ),
      TestGqlStatusObject(
        STATUS_01N02.getStatusString,
        "warn: feature deprecated without replacement. org.example.com.oldFuncNotReplaced is deprecated and will be removed without a replacement.",
        SeverityLevel.WARNING,
        NotificationClassification.DEPRECATION,
        funcPos
      ),
      testOmittedResult
    )

    val detail1 = NotificationDetail.deprecatedInputField("changedProc2", "value")
    assertNotification(
      queries,
      shouldContainNotification = true,
      detail1,
      startPosition,
      (pos, detail) => deprecatedProcedureField(pos, detail, "changedProc2", "value"),
      expectedGqlStatusObjects
    )
    val detail2 = NotificationDetail.deprecatedName("org.example.com.oldFuncNotReplaced")
    assertNotification(
      queries,
      shouldContainNotification = true,
      detail2,
      funcPos,
      (pos, detail) => deprecatedFunctionWithoutReplacement(pos, detail, "org.example.com.oldFuncNotReplaced"),
      expectedGqlStatusObjects
    )
  }

  test("deprecated function calls without replacement") {
    val examples = Seq(
      ("RETURN org.example.com.oldFuncNotReplaced()", new InputPosition(7, 1, 8)),
      ("MATCH (n) WHERE org.example.com.oldFuncNotReplaced() = 1 RETURN n", new InputPosition(16, 1, 17)),
      ("MATCH (n) WHERE toString(org.example.com.oldFuncNotReplaced()) = 1 RETURN n", new InputPosition(25, 1, 26))
    )

    val detail = NotificationDetail.deprecatedName("org.example.com.oldFuncNotReplaced")

    examples.foreach {
      case (query, position) =>
        assertNotification(
          Seq(query),
          shouldContainNotification = true,
          detail,
          position,
          (pos, detail) => deprecatedFunctionWithoutReplacement(pos, detail, "org.example.com.oldFuncNotReplaced"),
          List(
            TestGqlStatusObject(
              STATUS_01N02.getStatusString,
              "warn: feature deprecated without replacement. org.example.com.oldFuncNotReplaced is deprecated and will be removed without a replacement.",
              SeverityLevel.WARNING,
              NotificationClassification.DEPRECATION,
              position
            ),
            testOmittedResult
          )
        )

    }
  }

  test("deprecated function calls with replacement") {
    val examples = Seq(
      ("RETURN org.example.com.oldFunc()", new InputPosition(7, 1, 8)),
      ("MATCH (n) WHERE org.example.com.oldFunc() = 1 RETURN n", new InputPosition(16, 1, 17)),
      ("MATCH (n) WHERE toString(org.example.com.oldFunc()) = 1 RETURN n", new InputPosition(25, 1, 26))
    )

    val detail = NotificationDetail.deprecatedName("org.example.com.oldFunc", "org.example.com.newFunc")

    examples.foreach {
      case (query, position) =>
        assertNotification(
          Seq(query),
          shouldContainNotification = true,
          detail,
          position,
          (pos, detail) =>
            deprecatedFunctionWithReplacement(pos, detail, "org.example.com.oldFunc", "org.example.com.newFunc"),
          List(
            TestGqlStatusObject(
              STATUS_01N01.getStatusString,
              "warn: feature deprecated with replacement. org.example.com.oldFunc is deprecated. It is replaced by org.example.com.newFunc.",
              SeverityLevel.WARNING,
              NotificationClassification.DEPRECATION,
              position
            ),
            testOmittedResult
          )
        )
    }
  }

  test("deprecated aggregation function calls") {
    val examples = Seq(
      ("UNWIND [1, 2, 3] AS nums RETURN org.example.com.oldAggFunc(nums)", new InputPosition(32, 1, 33)),
      (
        "UNWIND [1, 2, 3] AS nums WITH org.example.com.oldAggFunc(nums) AS aggTest RETURN aggTest",
        new InputPosition(30, 1, 31)
      )
    )

    val detail = NotificationDetail.deprecatedName("org.example.com.oldAggFunc", "org.example.com.newAggFunc")

    examples.foreach {
      case (query, position) =>
        assertNotification(
          Seq(query),
          shouldContainNotification = true,
          detail,
          position,
          (pos, detail) =>
            deprecatedFunctionWithReplacement(pos, detail, "org.example.com.oldAggFunc", "org.example.com.newAggFunc"),
          List(
            TestGqlStatusObject(
              STATUS_01N01.getStatusString,
              "warn: feature deprecated with replacement. org.example.com.oldAggFunc is deprecated. It is replaced by org.example.com.newAggFunc.",
              SeverityLevel.WARNING,
              NotificationClassification.DEPRECATION,
              position
            ),
            testOmittedResult
          )
        )
    }
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
      new InputPosition(16, 1, 17),
      (pos, detail) => deprecatedRelationshipTypeSeparator(pos, detail, ":A|:B|:C", ":A|B|C"),
      List(
        TestGqlStatusObject(
          STATUS_01N01.getStatusString,
          "warn: feature deprecated with replacement. ':A|:B|:C' is deprecated. It is replaced by ':A|B|C'.",
          SeverityLevel.WARNING,
          NotificationClassification.DEPRECATION,
          new InputPosition(16, 1, 17)
        ),
        TestGqlStatusObject(
          STATUS_01N51.getStatusString,
          s"warn: relationship type does not exist. The relationship type `A` does not exist in database `$dbName`. Verify that the spelling is correct.",
          SeverityLevel.WARNING,
          NotificationClassification.UNRECOGNIZED,
          new InputPosition(12, 1, 13)
        ),
        TestGqlStatusObject(
          STATUS_01N51.getStatusString,
          s"warn: relationship type does not exist. The relationship type `B` does not exist in database `$dbName`. Verify that the spelling is correct.",
          SeverityLevel.WARNING,
          NotificationClassification.UNRECOGNIZED,
          new InputPosition(15, 1, 16)
        ),
        TestGqlStatusObject(
          STATUS_01N51.getStatusString,
          s"warn: relationship type does not exist. The relationship type `C` does not exist in database `$dbName`. Verify that the spelling is correct.",
          SeverityLevel.WARNING,
          NotificationClassification.UNRECOGNIZED,
          new InputPosition(18, 1, 19)
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
      new InputPosition(33, 1, 34),
      pos => deprecatedNodeOrRelationshipOnRhsSetClause(pos, "SET g = r", "SET g = properties(r)"),
      List(
        TestGqlStatusObject(
          STATUS_01N01.getStatusString,
          "warn: feature deprecated with replacement. 'SET g = r' is deprecated. It is replaced by 'SET g = properties(r)'.",
          SeverityLevel.WARNING,
          NotificationClassification.DEPRECATION,
          new InputPosition(33, 1, 34)
        ),
        testOmittedResult
      ),
      cypherVersions = Set(CypherVersionOption.cypher5)
    )

    assertNotification(
      Seq("MATCH (g)-[r:KNOWS]->(k) SET g = k"),
      shouldContainNotification = true,
      new InputPosition(33, 1, 34),
      pos => deprecatedNodeOrRelationshipOnRhsSetClause(pos, "SET g = k", "SET g = properties(k)"),
      List(
        TestGqlStatusObject(
          STATUS_01N01.getStatusString,
          "warn: feature deprecated with replacement. 'SET g = k' is deprecated. It is replaced by 'SET g = properties(k)'.",
          SeverityLevel.WARNING,
          NotificationClassification.DEPRECATION,
          new InputPosition(33, 1, 34)
        ),
        testOmittedResult
      ),
      cypherVersions = Set(CypherVersionOption.cypher5)
    )

    assertNotification(
      Seq("MATCH (g)-[r:KNOWS]->(k) SET g += r"),
      shouldContainNotification = true,
      new InputPosition(34, 1, 35),
      pos => deprecatedNodeOrRelationshipOnRhsSetClause(pos, "SET g += r", "SET g += properties(r)"),
      List(
        TestGqlStatusObject(
          STATUS_01N01.getStatusString,
          "warn: feature deprecated with replacement. 'SET g += r' is deprecated. It is replaced by 'SET g += properties(r)'.",
          SeverityLevel.WARNING,
          NotificationClassification.DEPRECATION,
          new InputPosition(34, 1, 35)
        ),
        testOmittedResult
      ),
      cypherVersions = Set(CypherVersionOption.cypher5)
    )

    assertNotification(
      Seq("MATCH (g)-[r:KNOWS]->(k) SET g += k"),
      shouldContainNotification = true,
      new InputPosition(34, 1, 35),
      pos => deprecatedNodeOrRelationshipOnRhsSetClause(pos, "SET g += k", "SET g += properties(k)"),
      List(
        TestGqlStatusObject(
          STATUS_01N01.getStatusString,
          "warn: feature deprecated with replacement. 'SET g += k' is deprecated. It is replaced by 'SET g += properties(k)'.",
          SeverityLevel.WARNING,
          NotificationClassification.DEPRECATION,
          new InputPosition(34, 1, 35)
        ),
        testOmittedResult
      ),
      cypherVersions = Set(CypherVersionOption.cypher5)
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
      new InputPosition(36, 1, 37),
      pos =>
        deprecatedShortestPathWithFixedLengthRelationship(
          pos,
          "allShortestPaths((a)-[r]->(b))",
          "allShortestPaths((a)-[r*1..1]->(b))"
        ),
      List(
        TestGqlStatusObject(
          STATUS_01N01.getStatusString,
          "warn: feature deprecated with replacement. allShortestPaths((a)-[r]->(b)) is deprecated. It is replaced by allShortestPaths((a)-[r*1..1]->(b)).",
          SeverityLevel.WARNING,
          NotificationClassification.DEPRECATION,
          new InputPosition(36, 1, 37)
        ),
        testOmittedResult
      )
    )

    assertNotification(
      Seq("MATCH (a), (b), shortestPath((a)<-[r:TYPE]-(b)) RETURN b"),
      shouldContainNotification = true,
      new InputPosition(32, 1, 33),
      pos =>
        deprecatedShortestPathWithFixedLengthRelationship(
          pos,
          "shortestPath((a)<-[r:TYPE]-(b))",
          "shortestPath((a)<-[r:TYPE*1..1]-(b))"
        ),
      List(
        TestGqlStatusObject(
          STATUS_01N01.getStatusString,
          "warn: feature deprecated with replacement. shortestPath((a)<-[r:TYPE]-(b)) is deprecated. It is replaced by shortestPath((a)<-[r:TYPE*1..1]-(b)).",
          SeverityLevel.WARNING,
          NotificationClassification.DEPRECATION,
          new InputPosition(32, 1, 33)
        ),
        TestGqlStatusObject(
          STATUS_01N51.getStatusString,
          s"warn: relationship type does not exist. The relationship type `TYPE` does not exist in database `$dbName`. Verify that the spelling is correct.",
          SeverityLevel.WARNING,
          NotificationClassification.UNRECOGNIZED,
          new InputPosition(37, 1, 38)
        ),
        testOmittedResult
      )
    )
  }

  test("deprecate explicit use of old text index provider for cypher 5") {
    val deprecatedProvider = AllIndexProviderDescriptors.TEXT_V1_DESCRIPTOR.name()
    val deprecatedProviderQueries = Seq(
      s"CREATE TEXT INDEX FOR (n:Label) ON (n.prop) OPTIONS {indexProvider : '$deprecatedProvider'}",
      s"CREATE TEXT INDEX FOR ()-[r:TYPE]-() ON (r.prop) OPTIONS {indexProvider : '$deprecatedProvider'}"
    )
    assertNotification(
      deprecatedProviderQueries,
      shouldContainNotification = true,
      startPosition,
      deprecatedTextIndexProvider,
      List(
        TestGqlStatusObject(
          STATUS_01N01.getStatusString,
          "warn: feature deprecated with replacement. text-1.0 is deprecated. It is replaced by text-2.0.",
          SeverityLevel.WARNING,
          NotificationClassification.DEPRECATION,
          startPosition
        ),
        testOmittedResult
      ),
      cypherVersions = Set(CypherVersionOption.cypher5)
    )

    val validProvider = AllIndexProviderDescriptors.TEXT_V3_DESCRIPTOR.name()
    val validProviderQueries = Seq(
      s"CREATE TEXT INDEX FOR (n:Label) ON (n.prop) OPTIONS {indexProvider : '$validProvider'}",
      s"CREATE TEXT INDEX FOR ()-[r:TYPE]-() ON (r.prop) OPTIONS {indexProvider : '$validProvider'}"
    )
    assertNoDeprecations(validProviderQueries, cypherVersions = Set(CypherVersionOption.cypher5))
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

  test("Union return orders are not deprecated") {
    val queries = Seq(
      "MATCH (a)-[]-(b) RETURN a, b UNION MATCH (c)-[]-(d) RETURN c as b, d as a",
      "RETURN 'val' as one, 'val' as two UNION RETURN 'val' as two, 'val' as one",
      "RETURN 'val' as one, 'val' as two UNION RETURN 'val' as one, 'val' as two UNION RETURN 'val' as two, 'val' as one",
      "MATCH (a)-[]-(b) RETURN a, b UNION ALL MATCH (c)-[]-(d) RETURN c as b, d as a",
      "RETURN 'val' as one, 'val' as two UNION ALL RETURN 'val' as two, 'val' as one",
      "RETURN 'val' as one, 'val' as two UNION ALL RETURN 'val' as one, 'val' as two UNION ALL RETURN 'val' as two, 'val' as one",
      "RETURN COUNT { MATCH (a)-[]-(b) RETURN a, b UNION MATCH (a)-[]-(b) RETURN b, a }",
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

  private val propertySetsWithProp = Seq("prop: 123", "prop: (1 + 3) * 2")
  private val warningFillersWithProp = propertySetsWithProp.map(ps => (s"where {$ps}", ps))
  private val warningFillersWithoutProp = propertySetsWithProp.map(_ => (s"where {}", ""))

  private def propWarning(position: InputPosition): TestGqlStatusObject =
    TestGqlStatusObject(
      STATUS_01N52.getStatusString,
      s"warn: property key does not exist. The property `prop` does not exist in database `$dbName`. Verify that the spelling is correct.",
      SeverityLevel.WARNING,
      NotificationClassification.UNRECOGNIZED,
      position
    )

  test(
    "deprecate using unescaped variable named \"where\" in node pattern if directly followed by a property key-value expression only"
  ) {
    val queries = Seq(
      ("MATCH (", ") RETURN *"),
      (s"MATCH (", ")-->() RETURN *"),
      (s"MATCH ()-->(", ")-->() RETURN *"),
      (s"MATCH p=( ()<--(", ")-->() ) {1,3} RETURN *"),
      (s"MATCH ()-->(b), (b) ( ()<--(", ")-->() ) {1,3} RETURN *"),
      (s"MATCH (a) WITH a AS where WHERE ()-->(", ")-->() RETURN *"),
      (s"RETURN EXISTS { ()-->(", ")-->() }"),
      (s"RETURN [ ()-->(", ")-->() | 123 * 5 ]")
    )
    val queriesWithProp =
      for {
        (filler, prop) <- warningFillersWithProp
        (prefix, suffix) <- queries
      } yield (s"$prefix$filler$suffix", prop, new InputPosition(prefix.length, 1, prefix.length + 1))
    val queriesWithoutProp =
      for {
        (filler, prop) <- warningFillersWithoutProp
        (prefix, suffix) <- queries
      } yield (s"$prefix$filler$suffix", prop, new InputPosition(prefix.length, 1, prefix.length + 1))

    def whereWarning(prop: String, position: InputPosition) =
      TestGqlStatusObject(
        STATUS_01N01.getStatusString,
        s"warn: feature deprecated with replacement. (where {$prop}) is deprecated. It is replaced by (`where` {$prop}).",
        SeverityLevel.WARNING,
        NotificationClassification.DEPRECATION,
        position
      )

    queriesWithProp.foreach {
      case (query, prop, position) =>
        assertNotification(
          Seq(query),
          shouldContainNotification = true,
          position,
          (ip: InputPosition) => deprecatedWhereVariableInNodePattern(ip, "where", s"{$prop}"),
          List(
            whereWarning(prop, position),
            propWarning(new InputPosition(position.getOffset + 7, position.getLine, position.getColumn + 7)),
            testOmittedResult
          ),
          cypherVersions = Set(CypherVersionOption.cypher5)
        )
    }
    queriesWithoutProp.foreach {
      case (query, prop, position) =>
        assertNotification(
          Seq(query),
          shouldContainNotification = true,
          position,
          (ip: InputPosition) => deprecatedWhereVariableInNodePattern(ip, "where", s"{$prop}"),
          List(
            whereWarning(prop, position),
            testOmittedResult
          ),
          cypherVersions = Set(CypherVersionOption.cypher5)
        )
    }
  }

  test(
    "deprecate using unescaped variable named \"where\" in relationship pattern if directly followed by a property key-value expression only"
  ) {
    val queries = Seq(
      ("MATCH ()-[", "]->() RETURN *"),
      ("MATCH ()<-[", "]-() RETURN *"),
      ("MATCH ()-[", "]-() RETURN *"),
      ("MATCH ()-->()<-[", "]-() RETURN *"),
      ("MATCH p=( ()-[", "]->() ) {1,3} RETURN *"),
      ("MATCH ()-->(b), (b) ( ()-[", "]->() ) {1,3} RETURN *"),
      ("MATCH ()-[r]->() WITH r AS where WHERE ()-[", "]->() RETURN *"),
      ("RETURN EXISTS { ()-[", "]->() }"),
      ("RETURN [ ()-[", "]->() | 123 * 5 ]")
    )
    val queriesWithProp =
      for {
        (filler, prop) <- warningFillersWithProp
        (prefix, suffix) <- queries
      } yield (s"$prefix$filler$suffix", prop, new InputPosition(prefix.length, 1, prefix.length + 1))
    val queriesWithoutProp =
      for {
        (filler, prop) <- warningFillersWithoutProp
        (prefix, suffix) <- queries
      } yield (s"$prefix$filler$suffix", prop, new InputPosition(prefix.length, 1, prefix.length + 1))

    def whereWarning(prop: String, position: InputPosition) =
      TestGqlStatusObject(
        STATUS_01N01.getStatusString,
        s"warn: feature deprecated with replacement. -[where {$prop}]- is deprecated. It is replaced by -[`where` {$prop}]-.",
        SeverityLevel.WARNING,
        NotificationClassification.DEPRECATION,
        position
      )

    queriesWithProp.foreach {
      case (query, prop, position: InputPosition) =>
        assertNotification(
          Seq(query),
          shouldContainNotification = true,
          position,
          (ip: InputPosition) => deprecatedWhereVariableInRelationshipPattern(ip, "where", s"{$prop}"),
          List(
            whereWarning(prop, position),
            propWarning(new InputPosition(position.getOffset + 7, position.getLine, position.getColumn + 7)),
            testOmittedResult
          ),
          cypherVersions = Set(CypherVersionOption.cypher5)
        )
    }
    queriesWithoutProp.foreach {
      case (query, prop, position) =>
        assertNotification(
          Seq(query),
          shouldContainNotification = true,
          position,
          (ip: InputPosition) => deprecatedWhereVariableInRelationshipPattern(ip, "where", s"{$prop}"),
          List(
            whereWarning(prop, position),
            testOmittedResult
          ),
          cypherVersions = Set(CypherVersionOption.cypher5)
        )
    }
  }

  private val noWarningFillersWithoutWHERE =
    Seq("where", "`where`", "where :Something") ++ ("" +: propertySetsWithProp).flatMap(ps =>
      Seq(
        s"where :Something {$ps}",
        s"`where` {$ps}"
      )
    )
  private val noWarningFillersWithWHERE = Seq("where WHERE true", "where :Something WHERE true")

  test(
    "do not deprecate using unescaped variable named \"where\" in node pattern if not directly followed by a property key-value expression only"
  ) {
    val queries = (noWarningFillersWithoutWHERE ++ noWarningFillersWithWHERE).flatMap(filler =>
      Seq(
        s"MATCH ($filler) RETURN *",
        s"MATCH ($filler)-->() RETURN *",
        s"MATCH ()-->($filler)-->() RETURN *",
        s"MATCH p=( ()<--($filler)-->() ) {1,3} RETURN *",
        s"MATCH ()-->(b), (b) ( ()<--($filler)-->() ) {1,3} RETURN *",
        s"RETURN EXISTS { ()-->($filler)-->() }",
        s"RETURN [ ()-->($filler)-->() | 123 * 5 ]"
      )
        ++ noWarningFillersWithoutWHERE.flatMap(filler =>
          Seq(
            s"MATCH (a) WITH a AS where WHERE ()-->($filler)-->() RETURN *"
          )
        )
    )
    assertNoDeprecations(queries)
  }

  test(
    "do not warn about unescaped variable named \"where\" in relationship pattern if not directly followed by a property key-value expression only"
  ) {
    val queries = (noWarningFillersWithoutWHERE ++ noWarningFillersWithWHERE).flatMap(filler =>
      Seq(
        s"MATCH ()-[$filler]->() RETURN *",
        s"MATCH ()<-[$filler]-() RETURN *",
        s"MATCH ()-[$filler]-() RETURN *",
        s"MATCH ()-->()<-[$filler]-() RETURN *",
        s"MATCH p=( ()-[$filler]->() ) {1,3} RETURN *",
        s"MATCH ()-->(b), (b) ( ()-[$filler]->() ) {1,3} RETURN *",
        s"RETURN EXISTS { ()-[$filler]->() }",
        s"RETURN [ ()-[$filler]->() | 123 * 5 ]"
      )
        ++ noWarningFillersWithoutWHERE.flatMap(filler =>
          Seq(
            s"MATCH ()-[r]->() WITH r AS where WHERE ()-[$filler]->() RETURN *"
          )
        )
    )
    assertNoDeprecations(queries)
  }

  private def labelWarning(label: String, position: InputPosition) =
    TestGqlStatusObject(
      STATUS_01N50.getStatusString,
      s"warn: label does not exist. The label `$label` does not exist in database `$dbName`. Verify that the spelling is correct.",
      SeverityLevel.WARNING,
      NotificationClassification.UNRECOGNIZED,
      position
    )

  test(
    "deprecate unparenthesized label expression predicate as right-hand side operators of `+`"
  ) {
    /*
     * Format: ((labelExpr, labelExprOffset), Seq((label1, label1Offset), (label2, label2Offset))
     * labelExprOffset = the offset of the label expression operator in the labelExpr
     * labelXOffset = the offset of the label relative to labelExprOffset
     */
    val labelExpressionPredicates = Seq(
      (("n:A", 1), Seq(("A", 1))),
      (("n:A&B", 1), Seq(("A", 1), ("B", 3))),
      (("n:A|B", 1), Seq(("A", 1), ("B", 3))),
      (("n:A|(B&!C)", 1), Seq(("A", 1), ("B", 4), ("C", 7))),
      (("n IS D", 2), Seq(("D", 3))),
      (("n IS A&D", 2), Seq(("A", 3), ("D", 5))),
      (("n IS A|D", 2), Seq(("A", 3), ("D", 5))),
      (("n IS A|(D&!E)", 2), Seq(("A", 3), ("D", 6), ("E", 9)))
    )
    def isToColon(labelExpressionPredicate: String) = labelExpressionPredicate.replaceFirst("n IS ", "n:")
    val queries = labelExpressionPredicates.flatMap {
      case ((labelExpressionPredicate, labelExprOffset), labels) =>
        val lex = isToColon(labelExpressionPredicate)
        Seq(
          (
            s"MATCH (n) RETURN [1,'abc',3] + $labelExpressionPredicate AS x",
            lex,
            labels,
            new InputPosition(31 + labelExprOffset, 1, 32 + labelExprOffset)
          ),
          (
            s"MATCH (n) WHERE size([1,'abc',3] + $labelExpressionPredicate) = 4 RETURN 1 AS x",
            lex,
            labels,
            new InputPosition(35 + labelExprOffset, 1, 36 + labelExprOffset)
          ),
          (
            s"WITH [1,'abc',3] AS y MATCH (n) WITH size(y + $labelExpressionPredicate) AS x RETURN x",
            lex,
            labels,
            new InputPosition(46 + labelExprOffset, 1, 47 + labelExprOffset)
          ),
          (
            s"WITH [1,'abc',3] AS y MATCH (n) WITH size(range(2, 18, 3) + y + $labelExpressionPredicate) AS x RETURN x",
            lex,
            labels,
            new InputPosition(64 + labelExprOffset, 1, 65 + labelExprOffset)
          )
        )
    }
    queries.foreach {
      case (query, labelExpressionPredicate, labels, position) =>
        assertNotification(
          Seq(query),
          shouldContainNotification = true,
          position,
          (ip: InputPosition) => deprecatedPrecedenceOfLabelExpressionPredicate(ip, labelExpressionPredicate),
          List(
            TestGqlStatusObject(
              STATUS_01N01.getStatusString,
              s"warn: feature deprecated with replacement. ... + $labelExpressionPredicate is deprecated. It is replaced by ... + ($labelExpressionPredicate).",
              SeverityLevel.WARNING,
              NotificationClassification.DEPRECATION,
              position
            ),
            testOmittedResult
          ) ++ labels.map {
            case (label, labelOffset) => labelWarning(
                label,
                new InputPosition(position.getOffset + labelOffset, position.getLine, position.getColumn + labelOffset)
              )
          },
          cypherVersions = Set(CypherVersionOption.cypher5)
        )
    }
  }

  test(
    "do not warn about parenthesized label expression predicate as right-hand side operators of \"+\""
  ) {
    val labelExpressions = Seq(
      ("n:A", Seq(("A", 2))),
      ("n:A&B", Seq(("A", 2), ("B", 4))),
      ("n:A|B", Seq(("A", 2), ("B", 4))),
      ("n:A|(B&!C)", Seq(("A", 2), ("B", 5), ("C", 8))),
      ("n IS D", Seq(("D", 5))),
      ("n IS A&D", Seq(("A", 5), ("D", 7))),
      ("n IS A|D", Seq(("A", 5), ("D", 7))),
      ("n IS A|(D&!E)", Seq(("A", 5), ("D", 8), ("E", 11)))
    )
    val queries = labelExpressions.flatMap {
      case (labelExpression, labels) =>
        Seq(
          (s"MATCH (n) RETURN [1,'abc',3] + ($labelExpression) AS x", labels, new InputPosition(32, 1, 33)),
          (
            s"MATCH (n) WHERE size([1,'abc',3] + ($labelExpression)) = 4 RETURN 1 AS x",
            labels,
            new InputPosition(36, 1, 37)
          ),
          (
            s"WITH [1,'abc',3] AS y MATCH (n) WITH size(y + ($labelExpression)) AS x RETURN x",
            labels,
            new InputPosition(47, 1, 48)
          ),
          (
            s"WITH [1,'abc',3] AS y MATCH (n) WITH size(range(2, 18, 3) + y + ($labelExpression)) AS x RETURN x",
            labels,
            new InputPosition(65, 1, 66)
          )
        )
    }
    queries.foreach {
      case (query, labels, position) =>
        assertNotificationWithPositionTolerance(
          Seq(query),
          shouldContainNotification = true,
          new InputPosition(position.getOffset + labels.head._2, position.getLine, position.getColumn + labels.head._2),
          (ip: InputPosition) =>
            missingLabel(ip, NotificationDetail.missingLabel(labels.head._1), labels.head._1, DEFAULT_DATABASE_NAME),
          (labels.map {
            case (label, labelOffset) => labelWarning(
                label,
                new InputPosition(position.getOffset + labelOffset, position.getLine, position.getColumn + labelOffset)
              )
          } :+ testOmittedResult).toList,
          cypherVersions = Set(CypherVersionOption.cypher5)
        )
    }
  }

  test(
    "deprecate using unescaped variable named \"is\", \"contains\", or \"in\" in when operand of simple case expression"
  ) {
    def warning(variable: String, rhs: String): (InputPosition => TestGqlStatusObject, InputPosition => Notification) =
      (
        (ip: InputPosition) =>
          TestGqlStatusObject(
            STATUS_01N01.getStatusString,
            s"warn: feature deprecated with replacement. WHEN $variable$rhs is deprecated. It is replaced by WHEN `$variable`$rhs.",
            SeverityLevel.WARNING,
            NotificationClassification.DEPRECATION,
            ip
          ),
        (ip: InputPosition) => deprecatedKeywordVariableInWhenOperand(ip, variable, rhs)
      )
    val whenOperands = Seq(
      ("'abc' AS is", "is :: STRING", warning("is", " :: STRING"), 3),
      ("1 AS contains", "contains + 1", warning("contains", " + 1"), 9),
      ("1 AS contains", "contains + \"abc\"", warning("contains", " + \"abc\""), 9),
      ("1 AS contains", "contains + 5 * 4", warning("contains", " + 5 * 4"), 9),
      ("1 AS contains", "contains - 1", warning("contains", " - 1"), 9),
      ("1 AS contains", "contains - 0.5", warning("contains", " - 0.5"), 9),
      ("1 AS contains", "contains - 5 * 4", warning("contains", " - 5 * 4"), 9),
      ("[1, true] AS in", "in [ 1 ]", warning("in", "[1]"), 3),
      ("[1, true] AS in", "in [ 5 * 4 ]", warning("in", "[5 * 4]"), 3),
      ("{a: 1, abc: true} AS in", "in [ \"a\" ]", warning("in", "[\"a\"]"), 3),
      ("{a: 1, abc: true} AS in", "in [ \"a\" + \"bc\" ]", warning("in", "[\"a\" + \"bc\"]"), 3)
    )
    val caseExpressions = whenOperands.flatMap {
      case (proj, when, expected, operatorOffset) => Seq(
          (proj, s"CASE x WHEN $when THEN 1 END", expected, 12 + operatorOffset),
          (proj, s"CASE x WHEN $when THEN 1 ELSE 2 END", expected, 12 + operatorOffset),
          (proj, s"CASE x WHEN $when THEN 1 WHEN [1, 'abc'][x] THEN 2 ELSE 3 END", expected, 12 + operatorOffset),
          (
            proj,
            s"CASE x WHEN 0 THEN 0 WHEN $when THEN 1 WHEN [1, 'abc'][x] THEN 2 ELSE 3 END",
            expected,
            26 + operatorOffset
          )
        )
    }
    val queries = caseExpressions.map {
      case (proj, caseExpression, expected, whenOffset) =>
        (
          s"WITH 1 AS x, $proj RETURN $caseExpression AS x",
          expected,
          new InputPosition(21 + proj.length + whenOffset, 1, 22 + proj.length + whenOffset)
        )
    }
    queries.foreach {
      case (query, (gqlStatusObject, createNotification), position) =>
        assertNotification(
          Seq(query),
          shouldContainNotification = true,
          position,
          createNotification,
          List(
            gqlStatusObject(position),
            testOmittedResult
          ),
          cypherVersions = Set(CypherVersionOption.cypher5)
        )
    }
  }

  test(
    "do not warn using unescaped variable named `is`, `contains`, or `in` in when operand of simple case expression"
  ) {
    val whenOperands = Seq(
      ("'abc' AS is", "`is` :: STRING"),
      ("'abc' AS is", "(is) :: STRING"),
      ("'abc' AS is", "is IS :: STRING"),
      ("1 AS contains", "`contains` + 1"),
      ("1 AS contains", "(contains) + 1"),
      ("1 AS contains", "`contains` + \"abc\""),
      ("1 AS contains", "(contains) + \"abc\""),
      ("1 AS contains", "`contains` + 5 * 4"),
      ("1 AS contains", "(contains) + 5 * 4"),
      ("1 AS contains", "`contains` - 1"),
      ("1 AS contains", "(contains) - 1"),
      ("1 AS contains", "`contains` - 0.5"),
      ("1 AS contains", "(contains) - 0.5"),
      ("1 AS contains", "`contains` - 5 * 4"),
      ("1 AS contains", "(contains) - 5 * 4"),
      ("[1, true] AS in", "`in` [ 1 ]"),
      ("[1, true] AS in", "(in) [ 1 ]"),
      ("[1, true] AS in", "`in` [ 5 * 4 ]"),
      ("[1, true] AS in", "(in) [ 5 * 4 ]"),
      ("{a: 1, abc: true} AS in", "`in` [ \"a\" ]"),
      ("{a: 1, abc: true} AS in", "(in) [ \"a\" ]"),
      ("{a: 1, abc: true} AS in", "`in` [ \"a\" + \"bc\" ]"),
      ("{a: 1, abc: true} AS in", "(in) [ \"a\" + \"bc\" ]")
    )
    val caseExpressions = whenOperands.flatMap {
      case (proj, when) => Seq(
          (proj, s"CASE x WHEN $when THEN 1 END"),
          (proj, s"CASE x WHEN $when THEN 1 ELSE 2 END"),
          (proj, s"CASE x WHEN $when THEN 1 WHEN [1, 'abc'][x] THEN 2 ELSE 3 END"),
          (proj, s"CASE x WHEN 0 THEN 0 WHEN $when THEN 1 WHEN [1, 'abc'][x] THEN 2 ELSE 3 END")
        )
    }
    val queries = caseExpressions.map {
      case (proj, caseExpression) => s"WITH 1 AS x, $proj RETURN $caseExpression AS x"
    }
    assertNoDeprecations(queries)
  }

  test("deprecate using a function id()") {
    val examples = Seq(
      ("MATCH (a) RETURN id(a)", new InputPosition(17, 1, 18)),
      ("MATCH (a) RETURN iD(a)", new InputPosition(17, 1, 18)),
      ("MATCH (a) RETURN Id(a)", new InputPosition(17, 1, 18)),
      ("MATCH (a) RETURN ID(a)", new InputPosition(17, 1, 18)),
      ("RETURN id(null)", new InputPosition(7, 1, 8)),
      ("MATCH ()-[r]->() RETURN id(r)", new InputPosition(24, 1, 25))
    )
    val detail = NotificationDetail.deprecatedName("id", "elementId or consider using an application-generated id")

    examples.foreach {
      case (query, position) =>
        assertNotification(
          Seq(query),
          shouldContainNotification = true,
          detail,
          position,
          (pos, detail) =>
            deprecatedFunctionWithReplacement(
              pos,
              detail,
              "id",
              "elementId or consider using an application-generated id"
            ),
          List(
            TestGqlStatusObject(
              STATUS_01N01.getStatusString,
              "warn: feature deprecated with replacement. id is deprecated. It is replaced by elementId or consider using an application-generated id.",
              SeverityLevel.WARNING,
              NotificationClassification.DEPRECATION,
              position
            ),
            testOmittedResult
          )
        )
    }
  }

  test("connectComponentsPlanner pre parser option is deprecated") {
    val queries = Seq(
      "CYPHER connectComponentsPlanner=idp RETURN 1",
      "CYPHER connectComponentsPlanner=greedy RETURN 1"
    )
    val position = new InputPosition(7, 1, 8)
    assertNotification(
      queries,
      shouldContainNotification = true,
      position,
      deprecatedConnectComponentsPlannerPreParserOption,
      List(
        TestGqlStatusObject(
          STATUS_01N02.getStatusString,
          "warn: feature deprecated without replacement. connectComponentsPlanner is deprecated and will be removed without a replacement.",
          SeverityLevel.WARNING,
          NotificationClassification.DEPRECATION,
          position
        ),
        testOmittedResult
      )
    )
  }

  test("eagerAnalyzer pre parser option is deprecated and ignored") {
    val queries = Seq(
      "CYPHER eagerAnalyzer=lp RETURN 1",
      "CYPHER eagerAnalyzer=ir RETURN 1"
    )
    val position = new InputPosition(7, 1, 8)

    assertNotification(
      queries,
      shouldContainNotification = true,
      position,
      deprecatedEagerAnalyzerPreParserOption,
      List(
        TestGqlStatusObject(
          STATUS_01N02.getStatusString,
          "warn: feature deprecated without replacement. eagerAnalyzer is deprecated and will be removed without a replacement.",
          SeverityLevel.WARNING,
          NotificationClassification.DEPRECATION,
          position
        ),
        testOmittedResult
      )
    )
  }

  test("plannerVersion pre parser option is retired and falls back to default") {
    val version = "v2026_03"
    val queries = Seq(s"CYPHER plannerVersion=$version RETURN 42")
    val position = new InputPosition(7, 1, 8)

    assertNotification(
      queries,
      shouldContainNotification = true,
      version,
      position,
      (pos, ver) => retiredPlannerVersionPreParserOption(pos, ver),
      List(
        TestGqlStatusObject(
          STATUS_01N84.getStatusString,
          s"warn: unsupported planner version. The Cypher planner version $version is no longer supported. " +
            "The default planner version is used instead.",
          SeverityLevel.WARNING,
          NotificationClassification.UNSUPPORTED,
          position
        ),
        testOmittedResult
      )
    )
  }

  test("Deprecate property references across patterns in CREATE") {
    val deprecated = Seq(
      ("CREATE (a {foo:1}), (b {foo:a.foo})", new InputPosition(28, 1, 29)),
      ("CREATE (b {prop: a.prop}), (a)", new InputPosition(17, 1, 18)),
      ("CREATE (a), (b)-[r: REL {prop: a.prop}]->(c)", new InputPosition(31, 1, 32)),
      ("CREATE (b)-[r: REL {prop: a.prop}]->(c), (a)", new InputPosition(26, 1, 27)),
      ("CREATE (b)-[a: REL]->(c), (d {prop:a.prop})", new InputPosition(35, 1, 36)),
      ("CREATE (a), (b {prop: EXISTS {(a)-->()}})", new InputPosition(31, 1, 32)),
      ("CREATE (b {prop: EXISTS {(a)-->()}}), (a)", new InputPosition(26, 1, 27)),
      ("CREATE (a), (a)-[:REL]->({prop:a.prop})", new InputPosition(31, 1, 32)),
      ("CREATE (a), (b {prop: labels(a)})", new InputPosition(29, 1, 30)),
      ("CREATE (a), (b {prop: true IN [x IN labels(a) | true]})", new InputPosition(43, 1, 44))
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
    deprecated.foreach {
      case (query, position) =>
        assertNotification(
          Seq(query),
          shouldContainNotification = true,
          "a",
          position,
          deprecatedPropertyReferenceInCreate,
          List(
            TestGqlStatusObject(
              STATUS_01N00.getStatusString,
              "warn: feature deprecated. Creating an entity (a) and referencing that entity in a property definition in the same CREATE is deprecated.",
              SeverityLevel.WARNING,
              NotificationClassification.DEPRECATION,
              position
            ),
            testOmittedResult
          ),
          cypherVersions = Set(CypherVersionOption.cypher5)
        )
    }

    assertNoDeprecations(notDeprecated)
  }

  test("Deprecate property references across patterns in MERGE") {
    val deprecated = Seq(
      ("MERGE (a {prop:'p'})-[:T]->(b {prop:a.prop})", new InputPosition(36, 1, 37)),
      ("MERGE (a {prop:'p'})<-[:T]-(b {prop:a.prop})", new InputPosition(36, 1, 37)),
      ("MERGE (a {prop:'p'})-[:T]-(b {prop:a.prop})", new InputPosition(35, 1, 36)),
      (
        "CREATE ({prop:'p'})-[:T]->({prop:'p'}) MERGE (b {prop:a.prop})-[:T]->(a {prop:'p'})",
        new InputPosition(54, 1, 55)
      ),
      ("MERGE (a {prop:'p'})-[b:T {prop:a.prop}]->()", new InputPosition(32, 1, 33)),
      ("MERGE ()-[a:T {prop:'p'}]->()<-[b :S {prop:a.prop}]-()", new InputPosition(43, 1, 44)),
      ("FOREACH (x in [1,2,3] | MERGE (a {prop:'p'})-[:R]-(b {prop:a.prop}))", new InputPosition(59, 1, 60))
    )

    val notDeprecated = Seq(
      "MATCH (a {prop:'p'}) MERGE (b {prop:a.prop})",
      "MERGE (a {prop:'p'}) MERGE (a)-[:T]->(b {prop:a.prop})"
    )

    deprecated.foreach {
      case (query, position) =>
        assertNotification(
          Seq(query),
          shouldContainNotification = true,
          "a",
          position,
          deprecatedPropertyReferenceInMerge,
          List(
            TestGqlStatusObject(
              STATUS_01N00.getStatusString,
              "warn: feature deprecated. Merging an entity (a) and referencing that entity in a property definition in the same MERGE is deprecated.",
              SeverityLevel.WARNING,
              NotificationClassification.DEPRECATION,
              position
            ),
            testOmittedResult
          ),
          cypherVersions = Set(CypherVersionOption.cypher5)
        )
    }

    assertNoDeprecations(notDeprecated)
  }

  test("Deprecate unicode '\\u0085' if used in identifiers") {
    val deprecated = Seq(
      ("CREATE (a {f\\u0085oo:1})", new InputPosition(11, 1, 12)),
      ("CREATE (f\\u0085oo {a:1})", new InputPosition(8, 1, 9)),
      ("WITH 1 as f\\u0085oo return *", new InputPosition(10, 1, 11))
    )

    val notDeprecated = Seq(
      "CREATE (a {`f\\u0085oo`:1})",
      "CREATE (`f\\u0085oo` {a:1})",
      "WITH 1 as `f\\u0085oo` return *"
    )

    deprecated.foreach {
      case (query, position) =>
        assertNotification(
          Seq(query),
          shouldContainNotification = true,
          position,
          deprecatedIdentifierWhitespaceUnicode(_, '\u0085', "f\u0085oo"),
          List(
            TestGqlStatusObject(
              STATUS_01N00.getStatusString,
              "warn: feature deprecated. The Unicode character `\\u0085` is deprecated for unescaped identifiers and will be considered as a whitespace character in the future. To continue using it, escape the identifier by adding backticks around the identifier `f\u0085oo`.",
              SeverityLevel.WARNING,
              NotificationClassification.DEPRECATION,
              position
            ),
            testOmittedResult
          ),
          Set(CypherVersionOption.cypher5)
        )

    }

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

    val deprecatedParamQueries: Seq[(Char, String, InputPosition)] = deprecatedExtendedUnicodeChars.flatMap {
      deprecatedUnicodeChar =>
        // Kernel already errors on the unicode char: \u0000 in tokens, so skip over for some queries
        if (deprecatedUnicodeChar == '\u0000') {
          Seq(
            (deprecatedUnicodeChar, s"RETURN { a${deprecatedUnicodeChar}bc : 1 }", new InputPosition(9, 1, 10)),
            (deprecatedUnicodeChar, s"WITH 1 AS a${deprecatedUnicodeChar}bc RETURN 1", new InputPosition(10, 1, 11))
          )
        } else {
          Seq(
            (deprecatedUnicodeChar, s"RETURN { a${deprecatedUnicodeChar}bc : 1 }", new InputPosition(9, 1, 10)),
            (deprecatedUnicodeChar, s"WITH 1 AS a${deprecatedUnicodeChar}bc RETURN 1", new InputPosition(10, 1, 11)),
            (deprecatedUnicodeChar, s"MATCH (b:a${deprecatedUnicodeChar}bc) RETURN b", new InputPosition(9, 1, 10)),
            (
              deprecatedUnicodeChar,
              s"MATCH ()-[r:a${deprecatedUnicodeChar}bc]->() RETURN r",
              new InputPosition(12, 1, 13)
            )
          )
        }
    }

    def expectedGqlStatuses(unicode: Char, position: InputPosition): List[TestGqlStatusObject] = {
      val unicodeString = f"\\u${Integer.valueOf(unicode)}%04x"

      List(
        TestGqlStatusObject(
          STATUS_01N00.getStatusString,
          s"warn: feature deprecated. The character with the Unicode representation `$unicodeString` is deprecated for unescaped identifiers and will not be supported in the future. " +
            s"To continue using it, escape the identifier by adding backticks around the identifier `a${unicode}bc`.",
          SeverityLevel.WARNING,
          NotificationClassification.DEPRECATION,
          position
        ),
        testOmittedResult
      )
    }

    deprecatedParamQueries.foreach { case (unicode, query, position) =>
      assertNotification(
        Seq(query),
        shouldContainNotification = true,
        position,
        deprecatedIdentifierUnicode(_, unicode, s"a${unicode}bc"),
        expectedGqlStatuses(unicode, position),
        Set(CypherVersionOption.cypher5)
      )
    }
  }

  test("Deprecated Unicode Characters in Identifier Extend with parameter") {
    val deprecatedParamQueries: Seq[(Char, Seq[String])] = deprecatedExtendedUnicodeChars.map { deprecatedUnicodeChar =>
      deprecatedUnicodeChar -> Seq(s"RETURN $$a${deprecatedUnicodeChar}bc")
    }

    val position = new InputPosition(8, 1, 9)

    def expectedGqlStatuses(unicode: Char): List[TestGqlStatusObject] = {
      val unicodeString = f"\\u${Integer.valueOf(unicode)}%04x"

      List(
        TestGqlStatusObject(
          STATUS_01N00.getStatusString,
          s"warn: feature deprecated. The character with the Unicode representation `$unicodeString` is deprecated for unescaped identifiers and will not be supported in the future. " +
            s"To continue using it, escape the identifier by adding backticks around the identifier `a${unicode}bc`.",
          SeverityLevel.WARNING,
          NotificationClassification.DEPRECATION,
          position
        ),
        TestGqlStatusObject(
          STATUS_01N60.getStatusString,
          s"warn: parameter missing. The query plan cannot be cached and is not executable without 'EXPLAIN' due to the undefined parameter(s) $$`a${unicode}bc`. Provide the parameter(s).",
          SeverityLevel.WARNING,
          NotificationClassification.GENERIC,
          InputPosition.empty
        ),
        testOmittedResult
      )
    }

    deprecatedParamQueries.foreach { deprecateQueries =>
      assertNotification(
        deprecateQueries._2,
        shouldContainNotification = true,
        position,
        deprecatedIdentifierUnicode(_, deprecateQueries._1, s"a${deprecateQueries._1}bc"),
        expectedGqlStatuses(deprecateQueries._1),
        Set(CypherVersionOption.cypher5)
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
      (s"RETURN { ${deprecatedStartUnicodeChar}b${deprecatedExtendedUnicodeChar}c : 1 }", new InputPosition(9, 1, 10)),
      (
        s"WITH 1 AS ${deprecatedStartUnicodeChar}b${deprecatedExtendedUnicodeChar}c RETURN 1",
        new InputPosition(10, 1, 11)
      ),
      (
        s"MATCH (b:${deprecatedStartUnicodeChar}b${deprecatedExtendedUnicodeChar}c) RETURN b",
        new InputPosition(9, 1, 10)
      ),
      (
        s"MATCH ()-[r:${deprecatedStartUnicodeChar}b${deprecatedExtendedUnicodeChar}c]->() RETURN r",
        new InputPosition(12, 1, 13)
      )
    )

    def expectedGqlStatuses(position: InputPosition) = List(
      TestGqlStatusObject(
        STATUS_01N00.getStatusString,
        "warn: feature deprecated. The character with the Unicode representation `\\u206e` is deprecated for unescaped identifiers and will not be supported in the future. To continue using it, escape the identifier by adding backticks around the identifier `ⸯb\u206Ec`.",
        SeverityLevel.WARNING,
        NotificationClassification.DEPRECATION,
        position
      ),
      TestGqlStatusObject(
        STATUS_01N00.getStatusString,
        "warn: feature deprecated. The character with the Unicode representation `\\u2e2f` is deprecated for unescaped identifiers and will not be supported in the future. To continue using it, escape the identifier by adding backticks around the identifier `ⸯb\u206Ec`.",
        SeverityLevel.WARNING,
        NotificationClassification.DEPRECATION,
        position
      ),
      testOmittedResult
    )

    queriesWithDeprecatedStartChar.foreach {
      case (query, position) =>
        assertNotification(
          Seq(query),
          shouldContainNotification = true,
          position,
          deprecatedIdentifierUnicode(
            _,
            deprecatedStartUnicodeChar,
            s"${deprecatedStartUnicodeChar}b${deprecatedExtendedUnicodeChar}c"
          ),
          expectedGqlStatuses(position),
          Set(CypherVersionOption.cypher5)
        )

        assertNotification(
          Seq(query),
          shouldContainNotification = true,
          position,
          deprecatedIdentifierUnicode(
            _,
            deprecatedExtendedUnicodeChar,
            s"${deprecatedStartUnicodeChar}b${deprecatedExtendedUnicodeChar}c"
          ),
          expectedGqlStatuses(position),
          Set(CypherVersionOption.cypher5)
        )

    }
  }

  test("Multiple deprecated Unicode Characters in Identifier with parameter") {
    val deprecatedStartUnicodeChar = '\u2e2f'
    val deprecatedExtendedUnicodeChar = '\u206E'

    val queryWithDeprecatedStartChar = s"RETURN $$${deprecatedStartUnicodeChar}b${deprecatedExtendedUnicodeChar}c"
    val position = new InputPosition(8, 1, 9)

    val expectedGqlStatuses = List(
      TestGqlStatusObject(
        STATUS_01N00.getStatusString,
        "warn: feature deprecated. The character with the Unicode representation `\\u2e2f` is deprecated for unescaped identifiers and will not be supported in the future. To continue using it, escape the identifier by adding backticks around the identifier `\u2e2fb\u206Ec`.",
        SeverityLevel.WARNING,
        NotificationClassification.DEPRECATION,
        position
      ),
      TestGqlStatusObject(
        STATUS_01N00.getStatusString,
        "warn: feature deprecated. The character with the Unicode representation `\\u206e` is deprecated for unescaped identifiers and will not be supported in the future. To continue using it, escape the identifier by adding backticks around the identifier `\u2e2fb\u206Ec`.",
        SeverityLevel.WARNING,
        NotificationClassification.DEPRECATION,
        position
      ),
      TestGqlStatusObject(
        STATUS_01N60.getStatusString,
        "warn: parameter missing. The query plan cannot be cached and is not executable without 'EXPLAIN' due to the undefined parameter(s) $`\u2e2fb\u206Ec`. Provide the parameter(s).",
        SeverityLevel.WARNING,
        NotificationClassification.GENERIC,
        InputPosition.empty
      ),
      testOmittedResult
    )

    assertNotification(
      Seq(queryWithDeprecatedStartChar),
      shouldContainNotification = true,
      position,
      deprecatedIdentifierUnicode(
        _,
        deprecatedStartUnicodeChar,
        s"${deprecatedStartUnicodeChar}b${deprecatedExtendedUnicodeChar}c"
      ),
      expectedGqlStatuses,
      Set(CypherVersionOption.cypher5)
    )

    assertNotification(
      Seq(queryWithDeprecatedStartChar),
      shouldContainNotification = true,
      position,
      deprecatedIdentifierUnicode(
        _,
        deprecatedExtendedUnicodeChar,
        s"${deprecatedStartUnicodeChar}b${deprecatedExtendedUnicodeChar}c"
      ),
      expectedGqlStatuses,
      Set(CypherVersionOption.cypher5)
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
      (s"RETURN { ${deprecatedStartUnicodeChar}bc : 1 }", new InputPosition(9, 1, 10)),
      (s"WITH 1 AS ${deprecatedStartUnicodeChar}bc RETURN 1", new InputPosition(10, 1, 11)),
      (s"MATCH (b:${deprecatedStartUnicodeChar}bc) RETURN b", new InputPosition(9, 1, 10)),
      (s"MATCH ()-[r:${deprecatedStartUnicodeChar}bc]->() RETURN r", new InputPosition(12, 1, 13))
    )

    queriesWithDeprecatedStartChar.foreach {
      case (query, position) =>
        assertNotification(
          Seq(query),
          shouldContainNotification = true,
          position,
          deprecatedIdentifierUnicode(_, deprecatedStartUnicodeChar, s"${deprecatedStartUnicodeChar}bc"),
          List(
            TestGqlStatusObject(
              STATUS_01N00.getStatusString,
              "warn: feature deprecated. The character with the Unicode representation `\\u2e2f` is deprecated for unescaped identifiers and will not be supported in the future. To continue using it, escape the identifier by adding backticks around the identifier `\u2e2fbc`.",
              SeverityLevel.WARNING,
              NotificationClassification.DEPRECATION,
              position
            ),
            testOmittedResult
          ),
          Set(CypherVersionOption.cypher5)
        )
    }
  }

  test("Deprecated Unicode Characters in Identifier Start with parameter") {
    val deprecatedStartUnicodeChar = '\u2e2f'
    val queryWithDeprecatedStartChar = s"RETURN $$${deprecatedStartUnicodeChar}bc"
    val position = new InputPosition(8, 1, 9)

    assertNotification(
      Seq(queryWithDeprecatedStartChar),
      shouldContainNotification = true,
      position,
      deprecatedIdentifierUnicode(_, deprecatedStartUnicodeChar, s"${deprecatedStartUnicodeChar}bc"),
      List(
        TestGqlStatusObject(
          STATUS_01N00.getStatusString,
          "warn: feature deprecated. The character with the Unicode representation `\\u2e2f` is deprecated for unescaped identifiers and will not be supported in the future. To continue using it, escape the identifier by adding backticks around the identifier `\u2e2fbc`.",
          SeverityLevel.WARNING,
          NotificationClassification.DEPRECATION,
          position
        ),
        TestGqlStatusObject(
          STATUS_01N60.getStatusString,
          "warn: parameter missing. The query plan cannot be cached and is not executable without 'EXPLAIN' due to the undefined parameter(s) $`\u2e2fbc`. Provide the parameter(s).",
          SeverityLevel.WARNING,
          NotificationClassification.GENERIC,
          InputPosition.empty
        ),
        testOmittedResult
      ),
      Set(CypherVersionOption.cypher5)
    )
  }

  test("deprecated subquerycall without variable clause non-importing") {
    val queries = Seq(
      "CALL{RETURN 1 AS b} RETURN b"
    )
    assertNotification(
      queries,
      shouldContainNotification = true,
      startPosition,
      deprecatedImportingWithInSubqueryCall(_, "CALL", ""),
      List(
        TestGqlStatusObject(
          STATUS_01N00.getStatusString,
          "warn: feature deprecated. CALL subquery without a variable scope clause is deprecated. Use CALL () { ... }",
          SeverityLevel.WARNING,
          NotificationClassification.DEPRECATION,
          startPosition
        ),
        testOmittedResult
      )
    )
  }

  test("deprecated subquerycall without variable clause importing") {
    val queries = Seq(
      "WITH 1 AS a CALL{WITH a RETURN 1 AS b} RETURN b"
    )
    val position = new InputPosition(12, 1, 13)
    assertNotification(
      queries,
      shouldContainNotification = true,
      position,
      deprecatedImportingWithInSubqueryCall(_, "CALL", "a"),
      List(
        TestGqlStatusObject(
          STATUS_01N00.getStatusString,
          "warn: feature deprecated. CALL subquery without a variable scope clause is deprecated. Use CALL (a) { ... }",
          SeverityLevel.WARNING,
          NotificationClassification.DEPRECATION,
          position
        ),
        testOmittedResult
      )
    )
  }

  test("db.index.vector.queryNodes should not be deprecated in Cypher 5") {
    // create a vector index and wait until it is online to avoid procedure failure
    val transaction1 = dbms.begin()
    transaction1.execute("CREATE VECTOR INDEX nodeVectorIdx IF NOT EXISTS FOR (n:Label) ON n.prop")
    transaction1.commit()
    val transaction2 = dbms.begin()
    transaction2.execute("CALL db.awaitIndexes()")
    transaction2.commit()

    val query = "CALL db.index.vector.queryNodes('nodeVectorIdx', 5, [1, 2, 3])"
    assertNoDeprecations(Seq(query), Set(CypherVersionOption.cypher5))
  }

  test("db.index.vector.queryNodes should be deprecated in Cypher 25") {
    // create a vector index and wait until it is online to avoid procedure failure
    val transaction1 = dbms.begin()
    transaction1.execute("CREATE VECTOR INDEX nodeVectorIdx IF NOT EXISTS FOR (n:Label) ON n.prop")
    transaction1.commit()
    val transaction2 = dbms.begin()
    transaction2.execute("CALL db.awaitIndexes()")
    transaction2.commit()

    assertNotification(
      Seq("CALL db.index.vector.queryNodes('nodeVectorIdx', 5, [1, 2, 3])"),
      shouldContainNotification = true,
      startPosition,
      deprecatedProcedureWithReplacement(
        _,
        deprecatedName("db.index.vector.queryNodes", "SEARCH"),
        "db.index.vector.queryNodes",
        "SEARCH"
      ),
      List(
        TestGqlStatusObject(
          STATUS_01N01.getStatusString,
          "warn: feature deprecated with replacement. db.index.vector.queryNodes is deprecated. It is replaced by SEARCH.",
          SeverityLevel.WARNING,
          NotificationClassification.DEPRECATION,
          startPosition
        ),
        testOmittedResult
      ),
      Set(CypherVersionOption.cypher25)
    )
  }

  test("db.index.vector.queryRelationships should not be deprecated in Cypher 5") {
    // create a vector index and wait until it is online to avoid procedure failure
    val transaction1 = dbms.begin()
    transaction1.execute("CREATE VECTOR INDEX relVectorIdx IF NOT EXISTS FOR ()-[r:REL]->() ON r.prop")
    transaction1.commit()
    val transaction2 = dbms.begin()
    transaction2.execute("CALL db.awaitIndexes()")
    transaction2.commit()

    val query = "CALL db.index.vector.queryRelationships('relVectorIdx', 5, [1, 2, 3])"
    assertNoDeprecations(Seq(query), Set(CypherVersionOption.cypher5))
  }

  test("db.index.vector.queryRelationships should be deprecated in Cypher 25") {
    // create a vector index and wait until it is online to avoid procedure failure
    val transaction1 = dbms.begin()
    transaction1.execute("CREATE VECTOR INDEX relVectorIdx IF NOT EXISTS FOR ()-[r:REL]->() ON r.prop")
    transaction1.commit()
    val transaction2 = dbms.begin()
    transaction2.execute("CALL db.awaitIndexes()")
    transaction2.commit()

    assertNotification(
      Seq("CALL db.index.vector.queryRelationships('relVectorIdx', 5, [1, 2, 3])"),
      shouldContainNotification = true,
      startPosition,
      deprecatedProcedureWithReplacement(
        _,
        deprecatedName("db.index.vector.queryRelationships", "SEARCH"),
        "db.index.vector.queryRelationships",
        "SEARCH"
      ),
      List(
        TestGqlStatusObject(
          STATUS_01N01.getStatusString,
          "warn: feature deprecated with replacement. db.index.vector.queryRelationships is deprecated. It is replaced by SEARCH.",
          SeverityLevel.WARNING,
          NotificationClassification.DEPRECATION,
          startPosition
        ),
        testOmittedResult
      ),
      Set(CypherVersionOption.cypher25)
    )
  }
}
