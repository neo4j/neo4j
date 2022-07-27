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
import org.neo4j.graphdb.impl.notification.NotificationCode.DEPRECATED_NODE_OR_RELATIONSHIP_ON_RHS_SET_CLAUSE
import org.neo4j.graphdb.impl.notification.NotificationCode.DEPRECATED_PROCEDURE
import org.neo4j.graphdb.impl.notification.NotificationCode.DEPRECATED_PROCEDURE_RETURN_FIELD
import org.neo4j.graphdb.impl.notification.NotificationCode.DEPRECATED_RELATIONSHIP_TYPE_SEPARATOR
import org.neo4j.graphdb.impl.notification.NotificationCode.DEPRECATED_SHORTEST_PATH_WITH_FIXED_LENGTH_RELATIONSHIP
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
    assertNotification(queries, true, DEPRECATED_PROCEDURE, detail)
  }

  test("deprecated procedure result field") {
    val query = "CALL changedProc() YIELD oldField RETURN oldField"
    val detail = NotificationDetail.Factory.deprecatedField("changedProc", "oldField")
    assertNotification(Seq(query), true, DEPRECATED_PROCEDURE_RETURN_FIELD, detail)
  }

  // OTHER DEPRECATIONS IN 4.X

  test("deprecated legacy reltype separator") {
    val queries = Seq(
      "MATCH (a)-[:A|:B|:C]-() RETURN a"
    )

    assertNotification(queries, true, DEPRECATED_RELATIONSHIP_TYPE_SEPARATOR)

    // clear caches of the rewritten queries to not keep notifications around
    dbms.clearQueryCaches()
  }

  // DEPRECATIONS in 5.X

  test("deprecate using nodes/relationships on the RHS of a Set Clause") {
    val queries = Seq(
      "MATCH (g)-[r:KNOWS]->(k) SET g = r",
      "MATCH (g)-[r:KNOWS]->(k) SET g = k",
      "MATCH (g)-[r:KNOWS]->(k) SET g += r",
      "MATCH (g)-[r:KNOWS]->(k) SET g += k"
    )
    assertNotification(queries, true, DEPRECATED_NODE_OR_RELATIONSHIP_ON_RHS_SET_CLAUSE)
  }

  test("deprecate fixed length relationships in shortestPath and allShortestPaths") {
    val queries = Seq(
      "MATCH (a), (b), allShortestPaths((a)-[r]->(b)) RETURN b",
      "MATCH (a), (b), shortestPath((a)-[r]->(b)) RETURN b"
    )
    assertNotification(queries, true, DEPRECATED_SHORTEST_PATH_WITH_FIXED_LENGTH_RELATIONSHIP)
  }
}
