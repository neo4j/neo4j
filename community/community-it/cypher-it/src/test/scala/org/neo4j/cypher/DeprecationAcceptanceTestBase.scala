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
import org.neo4j.graphdb.impl.notification.NotificationCode.DEPRECATED_REPEATED_VAR_LENGTH_RELATIONSHIP
import org.neo4j.graphdb.impl.notification.NotificationCode.DEPRECATED_SHORTEST_PATH_WITH_FIXED_LENGTH_RELATIONSHIP
import org.neo4j.graphdb.impl.notification.NotificationCode.DEPRECATED_TEXT_INDEX_PROVIDER
import org.neo4j.graphdb.impl.notification.NotificationDetail
import org.neo4j.kernel.api.impl.schema.TextIndexProvider
import org.neo4j.kernel.api.impl.schema.trigram.TrigramIndexProvider
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

  // DEPRECATIONS in 5.X

  test("deprecated legacy reltype separator") {
    val queries = Seq(
      "MATCH (a)-[:A|:B|:C]-() RETURN a"
    )

    assertNotification(queries, true, DEPRECATED_RELATIONSHIP_TYPE_SEPARATOR)

    // clear caches of the rewritten queries to not keep notifications around
    dbms.clearQueryCaches()
  }

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

  test("deprecate explicit use of old text index provider") {
    val deprecatedProvider = TextIndexProvider.DESCRIPTOR.name()
    val deprecatedProviderQueries = Seq(
      s"CREATE TEXT INDEX FOR (n:Label) ON (n.prop) OPTIONS {indexProvider : '$deprecatedProvider'}",
      s"CREATE TEXT INDEX FOR ()-[r:TYPE]-() ON (r.prop) OPTIONS {indexProvider : '$deprecatedProvider'}"
    )
    assertNotification(deprecatedProviderQueries, shouldContainNotification = true, DEPRECATED_TEXT_INDEX_PROVIDER)

    val validProvider = TrigramIndexProvider.DESCRIPTOR.name()
    val validProviderQueries = Seq(
      s"CREATE TEXT INDEX FOR (n:Label) ON (n.prop) OPTIONS {indexProvider : '$validProvider'}",
      s"CREATE TEXT INDEX FOR ()-[r:TYPE]-() ON (r.prop) OPTIONS {indexProvider : '$validProvider'}"
    )
    assertNoDeprecations(validProviderQueries)
  }

  test("deprecate using the same variable name for several variable length relationships in the same pattern") {
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
    assertNotification(
      queries,
      true,
      DEPRECATED_REPEATED_VAR_LENGTH_RELATIONSHIP,
      NotificationDetail.Factory.repeatedVarLengthRel("r")
    )
  }

  test("deprecate using the same variable name for variable length relationships across patterns") {
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
    assertNotification(
      queries,
      true,
      DEPRECATED_REPEATED_VAR_LENGTH_RELATIONSHIP,
      NotificationDetail.Factory.repeatedVarLengthRel("s")
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
}
