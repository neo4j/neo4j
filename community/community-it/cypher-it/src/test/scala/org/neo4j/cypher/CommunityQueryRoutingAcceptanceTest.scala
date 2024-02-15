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

import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME
import org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.messages.MessageUtilProvider
import org.neo4j.cypher.testing.api.CypherExecutorException
import org.neo4j.cypher.testing.impl.FeatureDatabaseManagementService
import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.kernel.api.exceptions.Status
import org.neo4j.test.TestDatabaseManagementServiceBuilder
import org.scalatest.BeforeAndAfterAll

class CommunityQueryRoutingBoltAcceptanceTest extends CommunityQueryRoutingAcceptanceTest(java.lang.Boolean.TRUE)
    with FeatureDatabaseManagementService.TestUsingBolt

class CommunityQueryRoutingHttpAcceptanceTest extends CommunityQueryRoutingAcceptanceTest(java.lang.Boolean.TRUE)
    with FeatureDatabaseManagementService.TestUsingHttp

class CommunityQueryRoutingBoltOldStackAcceptanceTest
    extends CommunityQueryRoutingAcceptanceTest(java.lang.Boolean.FALSE)
    with FeatureDatabaseManagementService.TestUsingBolt

abstract class CommunityQueryRoutingAcceptanceTest(newStackEnabled: java.lang.Boolean) extends CypherFunSuite
    with FeatureDatabaseManagementService.TestBase
    with BeforeAndAfterAll {

  val db: FeatureDatabaseManagementService = dbms

  override def beforeAll(): Unit = {
    db.execute(s"CREATE (:Db {name:'$DEFAULT_DATABASE_NAME'})", _.consume())
    db.execute("CREATE INDEX myIndex FOR (db:Db) ON (db.name)", _.consume())
  }

  override def afterAll(): Unit = {
    db.shutdown()
  }

  test("should route a query with leading USE") {
    val query =
      s"""
         |USE $DEFAULT_DATABASE_NAME
         |MATCH (db:Db)
         |RETURN db.name AS db
         |""".stripMargin

    Seq(DEFAULT_DATABASE_NAME, SYSTEM_DATABASE_NAME).foreach { sessionDbName =>
      val result = execute(sessionDbName, query)

      result.toList should equal(List(Map("db" -> "neo4j")))
    }
  }

  test("should not blow up with USE targeting a non-existent graph") {
    val query =
      """
        |USE nonexistent
        |RETURN 1 AS x
        |""".stripMargin

    if (newStackEnabled) {
      Seq(DEFAULT_DATABASE_NAME, SYSTEM_DATABASE_NAME).foreach { sessionDbName =>
        failWithError(
          sessionDbName,
          query,
          Status.Database.DatabaseNotFound,
          "Graph not found: nonexistent"
        )
      }
    } else {
      Seq(DEFAULT_DATABASE_NAME, SYSTEM_DATABASE_NAME).foreach { sessionDbName =>
        failWithError(
          sessionDbName,
          query,
          Status.Statement.EntityNotFound,
          "Graph not found: nonexistent"
        )
      }
    }
  }

  test("should not accept dynamic USE target") {
    val query =
      """
        |WITH 1 AS g
        |USE graph.byElementId(g)
        |RETURN 1
        |""".stripMargin

    if (newStackEnabled) {
      failWithError(
        SYSTEM_DATABASE_NAME,
        query,
        Status.Statement.SyntaxError,
        MessageUtilProvider.createDynamicGraphReferenceUnsupportedError("graph.byElementId(g)")
      )
    } else {
      failWithError(
        SYSTEM_DATABASE_NAME,
        query,
        Status.Statement.SyntaxError,
        "USE clause must be either the first clause in a (sub-)query or preceded by an importing WITH clause in a sub-query."
      )
    }
  }

  test("should route a query with multiple USE targeting the same graph") {
    val query =
      s"""
         |USE $DEFAULT_DATABASE_NAME
         |CALL {
         |  USE $DEFAULT_DATABASE_NAME
         |  MATCH (db:Db)
         |  RETURN db.name AS db
         |}
         |RETURN db
         |""".stripMargin

    Seq(DEFAULT_DATABASE_NAME, SYSTEM_DATABASE_NAME).foreach { sessionDbName =>
      val result = execute(sessionDbName, query)

      result.toList should equal(List(Map("db" -> "neo4j")))
    }
  }

  test("should not accept multiple USE targeting different graphs") {
    val query =
      s"""
         |USE $DEFAULT_DATABASE_NAME
         |CALL {
         |  USE foo
         |  RETURN 1 AS x
         |}
         |RETURN x, 1 AS y
         |""".stripMargin

    Seq(DEFAULT_DATABASE_NAME, SYSTEM_DATABASE_NAME).foreach { sessionDbName =>
      failWithError(
        sessionDbName,
        query,
        Status.Statement.SyntaxError,
        MessageUtilProvider.createMultipleGraphReferencesError("foo")
      )
    }
  }

  test("should accept a combination of ambient and explicit graph selection targeting the session graph") {
    val query =
      s"""
         |CALL {
         |  USE $DEFAULT_DATABASE_NAME
         |  RETURN 1 AS x
         |}
         |RETURN x, 1 AS y
         |""".stripMargin
    val result = execute(DEFAULT_DATABASE_NAME, query)

    result.toList should equal(List(Map("x" -> 1, "y" -> 1)))
  }

  test("should not accept a combination of ambient and explicit graph selection targeting different graphs") {
    val query =
      """
        |CALL {
        |  USE foo
        |  RETURN 1 AS x
        |}
        |RETURN x, 1 AS y
        |""".stripMargin

    if (newStackEnabled) {
      failWithError(
        DEFAULT_DATABASE_NAME,
        query,
        Status.Database.DatabaseNotFound,
        "Database foo not found"
      )
    } else {
      failWithError(
        DEFAULT_DATABASE_NAME,
        query,
        Status.Statement.SyntaxError,
        MessageUtilProvider.createMultipleGraphReferencesError("foo")
      )
    }
  }

  test("should route Administration command to System database") {
    val query = "SHOW DATABASES YIELD name RETURN name"

    Seq(DEFAULT_DATABASE_NAME, SYSTEM_DATABASE_NAME).foreach { sessionDbName =>
      val result = execute(sessionDbName, query)
      result.toList should equal(List(Map("name" -> "neo4j"), Map("name" -> "system")))
    }
  }

  test("should route schema command") {
    val query =
      s"""
         |USE $DEFAULT_DATABASE_NAME
         |SHOW INDEXES YIELD name WHERE name='myIndex' RETURN name
         |""".stripMargin

    Seq(DEFAULT_DATABASE_NAME, SYSTEM_DATABASE_NAME).foreach { sessionDbName =>
      val result = execute(sessionDbName, query)
      result.toList should equal(List(Map("name" -> "myIndex")))
    }
  }

  override def baseConfig: Config.Builder = super.baseConfig
    .set(GraphDatabaseInternalSettings.query_router_new_stack, newStackEnabled)

  override def createBackingDbms(config: Config): DatabaseManagementService =
    new TestDatabaseManagementServiceBuilder().impermanent.setConfig(config).build()

  private def execute(sessionDatabaseName: String, query: String): Seq[Map[String, AnyRef]] = {
    db.executorFactory.executor(sessionDatabaseName).execute(query, Map.empty, result => result.records())
  }

  def failWithError(sessionDatabaseName: String, query: String, status: Status, messageSubstring: String): Unit = {
    val ex = the[CypherExecutorException]
      .thrownBy(execute(sessionDatabaseName, query))

    ex.status.shouldEqual(status)
    ex.getMessage.should(include(messageSubstring))
  }
}
