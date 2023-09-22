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
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.graphdb.impl.notification.NotificationCodeWithDescription.deprecatedConnectComponentsPlannerPreParserOption
import org.neo4j.graphdb.impl.notification.NotificationCodeWithDescription.deprecatedFunctionWithReplacement
import org.neo4j.graphdb.impl.notification.NotificationCodeWithDescription.deprecatedFunctionWithoutReplacement
import org.neo4j.graphdb.impl.notification.NotificationCodeWithDescription.deprecatedNodeOrRelationshipOnRhsSetClause
import org.neo4j.graphdb.impl.notification.NotificationCodeWithDescription.deprecatedProcedureReturnField
import org.neo4j.graphdb.impl.notification.NotificationCodeWithDescription.deprecatedProcedureWithReplacement
import org.neo4j.graphdb.impl.notification.NotificationCodeWithDescription.deprecatedProcedureWithoutReplacement
import org.neo4j.graphdb.impl.notification.NotificationCodeWithDescription.deprecatedPropertyReferenceInCreate
import org.neo4j.graphdb.impl.notification.NotificationCodeWithDescription.deprecatedRelationshipTypeSeparator
import org.neo4j.graphdb.impl.notification.NotificationCodeWithDescription.deprecatedShortestPathWithFixedLengthRelationship
import org.neo4j.graphdb.impl.notification.NotificationCodeWithDescription.deprecatedTextIndexProvider
import org.neo4j.graphdb.impl.notification.NotificationCodeWithDescription.unionReturnOrder
import org.neo4j.graphdb.impl.notification.NotificationDetail
import org.neo4j.graphdb.impl.notification.NotificationDetail.deprecationNotificationDetail
import org.neo4j.kernel.api.impl.schema.TextIndexProvider
import org.neo4j.kernel.api.impl.schema.trigram.TrigramIndexProvider
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
      (pos, detail) => deprecatedProcedureWithoutReplacement(pos, detail, "oldProcNotReplaced")
    )
  }

  test("deprecated procedure calls with replacement") {
    val queries = Seq("CALL oldProc()", "CALL oldProc() RETURN 1")
    val detail = NotificationDetail.deprecatedName("oldProc", "newProc")
    assertNotification(
      queries,
      shouldContainNotification = true,
      detail,
      (pos, detail) => deprecatedProcedureWithReplacement(pos, detail, "oldProc", "newProc")
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
      (pos, detail) => deprecatedProcedureReturnField(pos, detail, "changedProc", "oldField")
    )
  }

  test("non-deprecated procedure result field") {
    val queries = Seq("CALL changedProc() YIELD newField RETURN newField")

    assertNoDeprecations(queries)
  }

  test("deprecated function calls without replacement") {
    val queries = Seq(
      "RETURN org.example.com.oldFuncNotReplaced()",
      "MATCH (n) WHERE org.example.com.oldFuncNotReplaced() = 1 RETURN n"
    )
    val detail = NotificationDetail.deprecatedName("org.example.com.oldFuncNotReplaced")
    assertNotification(
      queries,
      shouldContainNotification = true,
      detail,
      (pos, detail) => deprecatedFunctionWithoutReplacement(pos, detail, "org.example.com.oldFuncNotReplaced")
    )
  }

  test("deprecated function calls with replacement") {
    val queries = Seq(
      "RETURN org.example.com.oldFunc()",
      "MATCH (n) WHERE org.example.com.oldFunc() = 1 RETURN n"
    )
    val detail = NotificationDetail.deprecatedName("org.example.com.oldFunc", "org.example.com.newFunc")
    assertNotification(
      queries,
      shouldContainNotification = true,
      detail,
      (pos, detail) =>
        deprecatedFunctionWithReplacement(pos, detail, "org.example.com.oldFunc", "org.example.com.newFunc")
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
        deprecatedFunctionWithReplacement(pos, detail, "org.example.com.oldAggFunc", "org.example.com.newAggFunc")
    )
  }

  test("non-deprecated function calls") {
    val queries = Seq(
      "RETURN org.example.com.newFunc()",
      "MATCH (n) WHERE org.example.com.newFunc() = 1 RETURN n"
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
      (pos, detail) => deprecatedRelationshipTypeSeparator(pos, detail, ":A|:B|:C", ":A|B|C")
    )

    // clear caches of the rewritten queries to not keep notifications around
    dbms.clearQueryCaches()
  }

  test("deprecate using nodes/relationships on the RHS of a Set Clause") {
    assertNotification(
      Seq("MATCH (g)-[r:KNOWS]->(k) SET g = r"),
      shouldContainNotification = true,
      pos => deprecatedNodeOrRelationshipOnRhsSetClause(pos, "SET g = r", "SET g = properties(r)")
    )

    assertNotification(
      Seq("MATCH (g)-[r:KNOWS]->(k) SET g = k"),
      shouldContainNotification = true,
      pos => deprecatedNodeOrRelationshipOnRhsSetClause(pos, "SET g = k", "SET g = properties(k)")
    )

    assertNotification(
      Seq("MATCH (g)-[r:KNOWS]->(k) SET g += r"),
      shouldContainNotification = true,
      pos => deprecatedNodeOrRelationshipOnRhsSetClause(pos, "SET g += r", "SET g += properties(r)")
    )

    assertNotification(
      Seq("MATCH (g)-[r:KNOWS]->(k) SET g += k"),
      shouldContainNotification = true,
      pos => deprecatedNodeOrRelationshipOnRhsSetClause(pos, "SET g += k", "SET g += properties(k)")
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
      deprecatedTextIndexProvider
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
        |CALL {
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
      unionReturnOrder
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
      (pos, detail) => deprecatedFunctionWithoutReplacement(pos, detail, "id")
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
      deprecatedConnectComponentsPlannerPreParserOption
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
      "CREATE (a), (b {prop: labels(a)})"
    )

    val notDeprecated = Seq(
      "MATCH (n) CREATE (a {prop: n.prop})",
      "MATCH (a) CREATE (a)-[:REL]->({prop:a.prop})",
      "CREATE (a)-[:REL]->(a)",
      "CREATE (a), (a)-[:REL]->(b)"
    )

    assertNotification(
      deprecated,
      shouldContainNotification = true,
      "a",
      deprecatedPropertyReferenceInCreate
    )

    assertNoDeprecations(notDeprecated)
  }
}
