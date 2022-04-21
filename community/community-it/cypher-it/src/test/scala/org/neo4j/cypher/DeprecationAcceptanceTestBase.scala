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
import org.neo4j.graphdb.impl.notification.NotificationCode.DEPRECATED_COERCION_OF_LIST_TO_BOOLEAN
import org.neo4j.graphdb.impl.notification.NotificationCode.DEPRECATED_HEX_LITERAL_SYNTAX
import org.neo4j.graphdb.impl.notification.NotificationCode.DEPRECATED_OCTAL_LITERAL_SYNTAX
import org.neo4j.graphdb.impl.notification.NotificationCode.DEPRECATED_PROCEDURE
import org.neo4j.graphdb.impl.notification.NotificationCode.DEPRECATED_PROCEDURE_RETURN_FIELD
import org.neo4j.graphdb.impl.notification.NotificationDetail
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
    assertNoNotificationInSupportedVersions(
      "MATCH p = ()-[*]-() RETURN relationships(p) AS rs",
      DEPRECATED_BINDING_VAR_LENGTH_RELATIONSHIP
    )
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
      "MATCH (n) WHERE range(0, 10) RETURN range(0, 10)"
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
        |""".stripMargin
    )
    assertNoDeprecations(queries)
  }

}
