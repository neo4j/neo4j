/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher

import org.neo4j.cypher.internal.javacompat.NotificationTestSupport.TestProcedures
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.graphdb.impl.notification.NotificationCode.DEPRECATED_BINDING_VAR_LENGTH_RELATIONSHIP
import org.neo4j.graphdb.impl.notification.NotificationCode.DEPRECATED_BTREE_INDEX_SYNTAX
import org.neo4j.graphdb.impl.notification.NotificationCode.DEPRECATED_COERCION_OF_LIST_TO_BOOLEAN
import org.neo4j.graphdb.impl.notification.NotificationCode.DEPRECATED_HEX_LITERAL_SYNTAX
import org.neo4j.graphdb.impl.notification.NotificationCode.DEPRECATED_OCTAL_LITERAL_SYNTAX
import org.neo4j.graphdb.impl.notification.NotificationCode.DEPRECATED_PROCEDURE
import org.neo4j.graphdb.impl.notification.NotificationCode.DEPRECATED_PROCEDURE_RETURN_FIELD
import org.neo4j.graphdb.impl.notification.NotificationDetail
import org.neo4j.graphdb.schema.IndexSettingImpl.FULLTEXT_EVENTUALLY_CONSISTENT
import org.neo4j.graphdb.schema.IndexSettingImpl.SPATIAL_CARTESIAN_MAX
import org.neo4j.graphdb.schema.IndexSettingImpl.SPATIAL_WGS84_MIN
import org.neo4j.kernel.impl.index.schema.GenericNativeIndexProvider
import org.scalatest.BeforeAndAfterAll

abstract class DeprecationAcceptanceTestBase extends CypherFunSuite with BeforeAndAfterAll with DeprecationTestSupport {

  override def beforeAll(): Unit = {
    // Used for testing deprecated procedures
    dbms.registerProcedure(classOf[TestProcedures])
  }

  override def afterAll(): Unit = {
    dbms.shutdown()
  }

  // DEPRECATED PROCEDURE THINGS

  test("deprecated procedure calls") {
    val queries = Seq("CALL oldProc()", "CALL oldProc() RETURN 1")
    val detail = NotificationDetail.Factory.deprecatedName("oldProc", "newProc")
    assertNotificationInSupportedVersions(queries, DEPRECATED_PROCEDURE, detail)
  }

  test("deprecated procedure result field") {
    val query = "CALL changedProc() YIELD oldField RETURN oldField"
    val detail = NotificationDetail.Factory.deprecatedField("changedProc", "oldField")
    assertNotificationInSupportedVersions(query, DEPRECATED_PROCEDURE_RETURN_FIELD, detail)
  }

  // DEPRECATED INDEX / CONSTRAINT SYNTAX in 4.X

  test("deprecated create btree index syntax") {
    // Note: This index syntax was introduced in 4.X

    // CREATE BTREE INDEX ...
    assertNotificationInSupportedVersions_4_X("CREATE BTREE INDEX FOR (n:Label) ON (n.prop)", DEPRECATED_BTREE_INDEX_SYNTAX)
    assertNotificationInSupportedVersions_4_X("CREATE BTREE INDEX name FOR ()-[r:TYPE]-() ON (r.prop)", DEPRECATED_BTREE_INDEX_SYNTAX)

    // CREATE INDEX ... OPTIONS { <btree options> }
    assertNotificationInSupportedVersions_4_X(
      s"""CREATE INDEX FOR (n:Label) ON (n.prop)
         |OPTIONS {IndexProvider: '${GenericNativeIndexProvider.DESCRIPTOR.name()}'}""".stripMargin,
      DEPRECATED_BTREE_INDEX_SYNTAX
    )
    assertNotificationInSupportedVersions_4_X(
      s"""CREATE INDEX FOR ()-[r:TYPE]-() ON (r.prop)
         |OPTIONS {indexprovider: '${GenericNativeIndexProvider.DESCRIPTOR.name()}'}""".stripMargin,
      DEPRECATED_BTREE_INDEX_SYNTAX
    )
    assertNotificationInSupportedVersions_4_X(
      s"""CREATE INDEX FOR (n:Label) ON (n.prop)
         |OPTIONS {IndexConfig: {`${SPATIAL_CARTESIAN_MAX.getSettingName}`: [40, 60]}}""".stripMargin,
      DEPRECATED_BTREE_INDEX_SYNTAX
    )
    assertNotificationInSupportedVersions_4_X(
      s"""CREATE INDEX FOR ()-[r:TYPE]-() ON (r.prop)
         |OPTIONS {indexconfig: {`${SPATIAL_WGS84_MIN.getSettingName}`: [-40, -60]}}""".stripMargin,
      DEPRECATED_BTREE_INDEX_SYNTAX
    )
    assertNoNotificationInSupportedVersions_4_X(
      s"""CREATE INDEX FOR (n:Label) ON (n.prop)
         |OPTIONS {indexconfig: {`${FULLTEXT_EVENTUALLY_CONSISTENT.getSettingName}`: false}}""".stripMargin,
      DEPRECATED_BTREE_INDEX_SYNTAX
    )
  }

  ignore("deprecated create constraint backed by btree index syntax") {
    // OPTIONS was introduced in 4.2
    // FOR ... REQUIRE was introduced in 4.4 and can therefore not be used in this test (since it tests 4.3)
    // ON ... ASSERT is now removed, hence the ignored test -> will be remove when BTREE is removed
    assertNotificationInSupportedVersions_4_X(
      s"""CREATE CONSTRAINT ON (n:Label)
         |ASSERT (n.prop) IS NODE KEY
         |OPTIONS {IndexProvider: '${GenericNativeIndexProvider.DESCRIPTOR.name()}'}""".stripMargin,
      DEPRECATED_BTREE_INDEX_SYNTAX
    )
    assertNotificationInSupportedVersions_4_X(
      s"""CREATE CONSTRAINT ON (n:Label)
         |ASSERT (n.prop) IS UNIQUE
         |OPTIONS {indexprovider: '${GenericNativeIndexProvider.DESCRIPTOR.name()}'}""".stripMargin,
      DEPRECATED_BTREE_INDEX_SYNTAX
    )
    assertNotificationInSupportedVersions_4_X(
      s"""CREATE CONSTRAINT ON (n:Label)
         |ASSERT (n.prop) IS NODE KEY
         |OPTIONS {IndexConfig: {`${SPATIAL_CARTESIAN_MAX.getSettingName}`: [40, 60]}}""".stripMargin,
      DEPRECATED_BTREE_INDEX_SYNTAX
    )
    assertNotificationInSupportedVersions_4_X(
      s"""CREATE CONSTRAINT ON (n:Label)
         |ASSERT (n.prop) IS UNIQUE
         |OPTIONS {indexconfig: {`${SPATIAL_WGS84_MIN.getSettingName}`: [-40, -60]}}""".stripMargin,
      DEPRECATED_BTREE_INDEX_SYNTAX
    )
    assertNoNotificationInSupportedVersions_4_X(
      s"""CREATE CONSTRAINT ON (n:Label)
         |ASSERT (n.prop) IS UNIQUE
         |OPTIONS {indexconfig: {`${FULLTEXT_EVENTUALLY_CONSISTENT.getSettingName}`: false}}""".stripMargin,
      DEPRECATED_BTREE_INDEX_SYNTAX
    )
  }

  test("deprecated show btree index syntax") {
    // Note: Show indexes was introduced in Neo4j 4.2
    assertNotificationInSupportedVersions_4_X("SHOW BTREE INDEXES", DEPRECATED_BTREE_INDEX_SYNTAX)
  }

  // OTHER DEPRECATIONS IN 4.X

  test("deprecated octal literal syntax") {
    assertNotificationInSupportedVersions("RETURN 0123 AS octal", DEPRECATED_OCTAL_LITERAL_SYNTAX)
  }

  test("deprecated hex literal syntax") {
    assertNotificationInSupportedVersions("RETURN 0X12B AS hex", DEPRECATED_HEX_LITERAL_SYNTAX)
  }

  test("deprecated binding variable length relationship") {
    val query = "MATCH ()-[rs*]-() RETURN rs"
    val detail = NotificationDetail.Factory.bindingVarLengthRelationship("rs")
    assertNotificationInSupportedVersions(query, DEPRECATED_BINDING_VAR_LENGTH_RELATIONSHIP, detail)
  }

  test("not deprecated binding variable length relationship") {
    assertNoNotificationInSupportedVersions("MATCH p = ()-[*]-() RETURN relationships(p) AS rs", DEPRECATED_BINDING_VAR_LENGTH_RELATIONSHIP)
  }

  test("deprecated coercion list to boolean") {
    val queries = Seq(
      "RETURN NOT []",
      "RETURN NOT [1]",
      "RETURN NOT ['a']",
      "RETURN ['a'] OR []",
      "RETURN TRUE OR []",
      "RETURN NOT (TRUE OR [])",
      "RETURN ['a'] AND []",
      "RETURN TRUE AND []",
      "RETURN NOT (TRUE AND [])",
      "MATCH (n) WHERE [] RETURN TRUE",
      "MATCH (n) WHERE range(0, 10) RETURN TRUE",
      "MATCH (n) WHERE range(0, 10) RETURN range(0, 10)",
    )

    assertNotificationInSupportedVersions(queries, DEPRECATED_COERCION_OF_LIST_TO_BOOLEAN)

    assertNoNotificationInSupportedVersions("RETURN NOT TRUE", DEPRECATED_COERCION_OF_LIST_TO_BOOLEAN)
  }

  test("should not deprecate boolean coercion of pattern expressions") {
    val queries = Seq(
      "RETURN NOT ()--()",
      "RETURN ()--() OR ()--()--()",
      "MATCH (n) WHERE (n)-[]->() RETURN n",
      """
      |MATCH (a), (b)
      |WITH a, b
      |WHERE a.id = 0
      |  AND (a)-[:T]->(b:Label1)
      |  OR (a)-[:T*]->(b:Label2)
      |RETURN DISTINCT b
      """.stripMargin,
      """
        |MATCH (a), (b)
        |WITH a, b
        |WHERE a.id = 0
        |  AND exists((a)-[:T]->(b:Label1))
        |  OR exists((a)-[:T*]->(b:Label2))
        |RETURN DISTINCT b
      """.stripMargin,
      "MATCH (n) WHERE NOT (n)-[:REL2]-() RETURN n",
      "MATCH (n) WHERE (n)-[:REL1]-() AND (n)-[:REL3]-() RETURN n",
      "MATCH (n WHERE (n)--()) RETURN n",
      """
        |MATCH (actor:Actor)
        |RETURN actor,
        |  CASE
        |    WHEN (actor)-[:WON]->(:Oscar) THEN 'Oscar winner'
        |    WHEN (actor)-[:WON]->(:GoldenGlobe) THEN 'Golden Globe winner'
        |    ELSE 'None'
        |  END AS accolade
        |""".stripMargin,
      """
        |MATCH (movie:Movie)<-[:ACTED_IN]-(actor:Actor)
        |WITH movie, collect(actor) AS cast
        |WHERE ANY(actor IN cast WHERE (actor)-[:WON]->(:Award))
        |RETURN movie
        |""".stripMargin,
    )
    assertNoDeprecations(queries)
  }

}
