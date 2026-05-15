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
import org.neo4j.cypher.CypherITTestSuite
import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.util.test_helpers.GqlExceptionMatchers.gqlException
import org.neo4j.cypher.internal.util.test_helpers.GqlExceptionMatchers.gqlStatus
import org.neo4j.cypher.messages.MessageUtilProvider
import org.neo4j.cypher.testing.api.CypherExecutorException
import org.neo4j.cypher.testing.impl.FeatureDatabaseManagementService
import org.neo4j.cypher.testing.impl.FeatureDatabaseManagementService.TestApiKind
import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.driver.exceptions.Neo4jException
import org.neo4j.gqlstatus.GqlStatusInfoCodes
import org.neo4j.kernel.api.exceptions.Status
import org.neo4j.kernel.database.DatabaseReferenceImpl.GraphShard
import org.neo4j.kernel.database.DatabaseReferenceImpl.PropertyShard
import org.neo4j.test.TestDatabaseManagementServiceBuilder
import org.scalatest.BeforeAndAfterAll

class CommunityQueryRoutingBoltAcceptanceTest extends CommunityQueryRoutingAcceptanceTest
    with FeatureDatabaseManagementService.TestUsingBolt

class CommunityQueryRoutingHttpAcceptanceTest extends CommunityQueryRoutingAcceptanceTest
    with FeatureDatabaseManagementService.TestUsingHttp

abstract class CommunityQueryRoutingAcceptanceTest extends CypherITTestSuite
    with FeatureDatabaseManagementService.TestBase
    with BeforeAndAfterAll {

  override def baseConfig: Config.Builder =
    super.baseConfig.set(GraphDatabaseInternalSettings.composable_commands, java.lang.Boolean.TRUE)

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

    Seq(DEFAULT_DATABASE_NAME, SYSTEM_DATABASE_NAME).foreach { sessionDbName =>
      failWithError(
        sessionDbName,
        query,
        Status.Database.DatabaseNotFound,
        "Graph not found: nonexistent"
      )
    }

  }

  test("should not accept dynamic USE target") {
    val query =
      """
        |WITH 1 AS g
        |USE graph.byElementId(g)
        |RETURN 1
        |""".stripMargin

    failWithError(
      SYSTEM_DATABASE_NAME,
      query,
      Status.Statement.SyntaxError,
      MessageUtilProvider.createDynamicGraphReferenceUnsupportedError("graph.byElementId(g)"),
      Some((
        GqlStatusInfoCodes.STATUS_42001,
        "error: syntax error or access rule violation - invalid syntax",
        GqlStatusInfoCodes.STATUS_42N72,
        "error: syntax error or access rule violation - graph function only supported on composite databases. Calling graph functions is only supported on composite databases. Use the name directly or connect to a composite database with the desired constituents."
      ))
    )
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

    failWithError(
      DEFAULT_DATABASE_NAME,
      query,
      Status.Database.DatabaseNotFound,
      "Database foo not found"
    )

  }

  test("should route Administration command to System database") {
    val query = "SHOW USERS YIELD user RETURN user"

    Seq(DEFAULT_DATABASE_NAME, SYSTEM_DATABASE_NAME).foreach { sessionDbName =>
      val result = execute(sessionDbName, query)
      result.toList should equal(List(Map("user" -> "neo4j")))
    }
  }

  test("should route show database command to System database when it's the only command") {
    val query = "SHOW DATABASES YIELD name RETURN name"
    val query2 = "SHOW DATABASES YIELD name"
    val query3 = "SHOW DATABASES"
    val query4 = "SHOW DATABASES WHERE name = 'system'"

    Seq(DEFAULT_DATABASE_NAME, SYSTEM_DATABASE_NAME).foreach { sessionDbName =>
      CypherVersion.values().foreach(cv => {
        Seq(query, query2).foreach { query =>
          withClue(cv.description + " " + query + " " + sessionDbName + ": ") {
            val result = execute(sessionDbName, cv.description + " " + query)
            result.toList should equal(expectedDatabaseNameRows)
          }
        }

        Seq(query3, query4).foreach { query =>
          withClue(cv.description + " " + query + " " + sessionDbName + ": ") {
            val result = execute(sessionDbName, cv.description + " " + query)
            result.size should be > 0
          }
        }
      })
    }
  }

  test("should not route show database command to System database when it's part of a larger query") {
    val prefix = "CYPHER 25 "
    val query1 =
      "SHOW DATABASES YIELD name CALL db.index.fulltext.listAvailableAnalyzers() YIELD analyzer RETURN DISTINCT name"
    val query2 =
      "CALL db.index.fulltext.listAvailableAnalyzers() YIELD analyzer SHOW DATABASES YIELD name RETURN DISTINCT name"

    Seq(query1, query2).foreach { query =>
      withClue(query) {
        // On system
        execute(
          SYSTEM_DATABASE_NAME,
          prefix + query
        ).toList should equal(expectedDatabaseNameRows)

        // On default, with explicit routing
        execute(
          DEFAULT_DATABASE_NAME,
          s"$prefix USE $SYSTEM_DATABASE_NAME $query"
        ).toList should equal(expectedDatabaseNameRows)

        // On default
        val exception = the[CypherExecutorException] thrownBy {
          execute(DEFAULT_DATABASE_NAME, prefix + query)
        }
        exception should be(gqlException(
          "This is an administration command and it should be executed against the system database: SHOW DATABASES",
          gqlStatus(
            GqlStatusInfoCodes.STATUS_50N42,
            "error: general processing exception - unexpected error. Unexpected error has occurred. See debug log for details."
          )
        ))
      }
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

  override def createBackingDbms(config: Config): DatabaseManagementService =
    new TestDatabaseManagementServiceBuilder().impermanent.setConfig(config).build()

  private def execute(sessionDatabaseName: String, query: String): Seq[Map[String, AnyRef]] = {
    db.executorFactory.executor(sessionDatabaseName).execute(query, Map.empty, result => result.records())
  }

  // Default + system, plus the graph and property shards when running under SPD.
  private def expectedDatabaseNameRows: List[Map[String, String]] = {
    val expectedShardCount = if (runOnSpd) 3 else 0
    val names =
      if (runOnSpd)
        List(DEFAULT_DATABASE_NAME, GraphShard.graphShardName(DEFAULT_DATABASE_NAME)) ++
          (0 until expectedShardCount).map(i => PropertyShard.propertyShardName(DEFAULT_DATABASE_NAME, i)) :+
          SYSTEM_DATABASE_NAME
      else
        List(DEFAULT_DATABASE_NAME, SYSTEM_DATABASE_NAME)
    names.map(name => Map("name" -> name))
  }

  def failWithError(
    sessionDatabaseName: String,
    query: String,
    status: Status,
    messageSubstring: String,
    gqlInfo: Option[(GqlStatusInfoCodes, String, GqlStatusInfoCodes, String)] = None
  ): Unit = {
    val ex = the[CypherExecutorException]
      .thrownBy(execute(sessionDatabaseName, query))

    ex.status.shouldEqual(status)
    ex.getMessage.should(include(messageSubstring))

    if (gqlInfo.isDefined) {
      // CypherExecutorException is a test-only construct wrapping driver errors.
      // Its ErrorGqlStatusObject will be null, as there is no logic (yet) for recreating server ErrorGqlStatusObjects
      // from the driver exception.
      // Therefore, we are doing GQLSTATUS checks on the underlying driver exception only.
      ex.original match {
        case e: Neo4jException =>
          e.gqlStatus() should be(gqlInfo.get._1.getStatusString)
          e.statusDescription() should be(gqlInfo.get._2)
          e.gqlCause() should not be empty
          val gqlCause = e.gqlCause().get()
          gqlCause.gqlStatus() should be(gqlInfo.get._3.getStatusString)
          gqlCause.statusDescription() should be(gqlInfo.get._4)

        case _ if testApiKind.equals(TestApiKind.Http) => // Query API is not ported to GQL yet, so do nothing
        case e => fail(s"Expected driver error with GQLSTATUS but was ${e.getClass}")

      }
    }
  }
}
