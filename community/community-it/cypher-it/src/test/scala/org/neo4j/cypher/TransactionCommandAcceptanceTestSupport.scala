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
import org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME
import org.neo4j.configuration.GraphDatabaseSettings.auth_enabled
import org.neo4j.cypher.internal.RewindableExecutionResult
import org.neo4j.cypher.internal.javacompat.GraphDatabaseCypherService
import org.neo4j.graphdb.Result
import org.neo4j.graphdb.config.Setting
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo.EMBEDDED_CONNECTION
import org.neo4j.internal.kernel.api.security.LoginContext
import org.neo4j.kernel.api.KernelTransaction.Type
import org.neo4j.kernel.api.security.AuthManager
import org.neo4j.kernel.impl.query.QueryExecutionConfiguration
import org.neo4j.server.security.auth.SecurityTestUtils
import org.neo4j.test.DoubleLatch
import org.neo4j.test.NamedFunction
import org.neo4j.test.extension.Threading
import org.neo4j.values.storable.DurationValue
import org.scalatest.concurrent.Eventually
import org.scalatest.time.Seconds
import org.scalatest.time.Span

import java.util

import scala.jdk.CollectionConverters.MapHasAsJava

class TransactionCommandAcceptanceTestSupport extends ExecutionEngineFunSuite with GraphDatabaseTestSupport
    with Eventually {

  protected val username = "foo"
  protected val username2 = "bar"
  protected val password = "secretpassword"

  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled(Span(15, Seconds)), interval = scaled(Span(3, Seconds)))

  protected val threading: Threading = new Threading()

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    threading.before()
  }

  override protected def afterEach(): Unit = {
    super.afterEach()
    threading.after()
  }

  override def databaseConfig(): Map[Setting[_], Object] =
    super.databaseConfig() ++ Map(auth_enabled -> java.lang.Boolean.TRUE)

  protected def assertCorrectDefaultMap(
    resultMap: Map[String, AnyRef],
    transactionId: String,
    username: String,
    query: String,
    database: String = DEFAULT_DATABASE_NAME,
    numColumns: Int = 10
  ): Unit = {
    resultMap.keys.size should be(numColumns)
    resultMap("database") should be(database)
    resultMap("transactionId").asInstanceOf[String] should startWith(
      transactionId
    ) // not stable on system database, differs among things between running the test on its own and the whole class
    resultMap("currentQueryId").asInstanceOf[String] should startWith(
      "query-"
    ) // not stable, differs among things between running the test on its own and the whole class
    resultMap("username") should be(username)
    resultMap("currentQuery") should be(query)
    // Default values:
    resultMap("status") should be("Running")
    resultMap("connectionId") should be("")
    resultMap("clientAddress") should be("")
    // Don't check exact values:
    resultMap("startTime").isInstanceOf[String] should be(true) // This is a timestamp
    resultMap("elapsedTime").isInstanceOf[DurationValue] should be(true)
  }

  protected def assertCorrectFullMap(
    resultMap: Map[String, AnyRef],
    transactionId: String,
    username: String,
    query: String,
    runtime: String,
    database: String = DEFAULT_DATABASE_NAME,
    planner: String = "idp",
    queryAllocatedBytesIsNull: Boolean = false
  ): Unit = {
    assertCorrectDefaultMap(resultMap, transactionId, username, query, database, numColumns = 39)
    resultMap("planner") should be(planner)
    resultMap("runtime") should be(runtime)
    // Default values:
    resultMap("outerTransactionId") should be("")
    resultMap("parameters") should be(Map())
    resultMap("indexes") should be(List())
    resultMap("protocol") should be("embedded")
    resultMap("metaData") should be(Map())
    resultMap("requestUri") should be(null)
    resultMap("currentQueryStatus") should be("running")
    resultMap("statusDetails") should be("")
    resultMap("resourceInformation") should be(Map())
    val currentQueryAllocatedBytes = resultMap("currentQueryAllocatedBytes")
    if (queryAllocatedBytesIsNull) currentQueryAllocatedBytes should be(null)
    else {
      currentQueryAllocatedBytes.isInstanceOf[Long] should be(true)
      (currentQueryAllocatedBytes.asInstanceOf[Long] > 0L) should be(true)
    }
    resultMap("allocatedDirectBytes") should be(0L)
    resultMap("initializationStackTrace") should be("")
    // Don't check exact values:
    resultMap("currentQueryStartTime").isInstanceOf[String] should be(true) // This is a timestamp
    resultMap("currentQueryElapsedTime").isInstanceOf[DurationValue] should be(true)
    resultMap("estimatedUsedHeapMemory").isInstanceOf[Long] should be(true)
    resultMap("activeLockCount").isInstanceOf[Long] should be(true)
    resultMap("currentQueryActiveLockCount").isInstanceOf[Long] should be(true)
    resultMap("pageHits").isInstanceOf[Long] should be(true)
    resultMap("pageFaults").isInstanceOf[Long] should be(true)
    resultMap("currentQueryPageHits").isInstanceOf[Long] should be(true)
    resultMap("currentQueryPageFaults").isInstanceOf[Long] should be(true)
    val cpuTime = resultMap("cpuTime")
    (cpuTime == null || cpuTime.isInstanceOf[DurationValue]) should be(true)
    resultMap("waitTime").isInstanceOf[DurationValue] should be(true)
    val idleTime = resultMap("idleTime")
    (idleTime == null || idleTime.isInstanceOf[DurationValue]) should be(true)
    val currentQueryCpuTime = resultMap("currentQueryCpuTime")
    (currentQueryCpuTime == null || currentQueryCpuTime.isInstanceOf[DurationValue]) should be(true)
    resultMap("currentQueryWaitTime").isInstanceOf[DurationValue] should be(true)
    val currentQueryIdleTime = resultMap("currentQueryIdleTime")
    (currentQueryIdleTime == null || currentQueryIdleTime.isInstanceOf[DurationValue]) should be(true)
  }

  /* Sets up one query for _username_ on neo4j.
   * Sets up one query for _username2_ on neo4j.
   *
   * Returns the two queries.
   */
  protected def setupTwoUsersAndOneTransactionEach(latch: DoubleLatch): (String, String) = {
    createUser()
    createUser(username2)

    val user1Query = "UNWIND [1,2,3] AS x RETURN x"
    val tx1 = ThreadedTransaction(latch)
    tx1.execute(username, password, threading, user1Query)

    val user2Query = "MATCH (n) RETURN n"
    val tx2 = ThreadedTransaction(latch)
    tx2.execute(username2, password, threading, user2Query)

    (user1Query, user2Query)
  }

  /* Creates a latch and sets up one query for _username_ on neo4j.
   * Starts the latch.
   *
   * Returns the query and the latch.
   */
  protected def setupUserWithOneTransaction(params: Map[String, AnyRef] = Map.empty): (String, DoubleLatch) = {
    val latch = new DoubleLatch(2, true)
    createUser()
    val userQuery = "UNWIND [1,2,3] AS x RETURN x"
    val tx = ThreadedTransaction(latch)
    tx.execute(username, password, threading, userQuery, params)
    latch.startAndWaitForAllToStart()
    (userQuery, latch)
  }

  protected def getTransactionIdExecutingQuery(query: String): String = {
    val result = execute(
      s"""SHOW TRANSACTIONS
         |WHERE currentQuery = '$query'
         |""".stripMargin
    )

    if (result.isEmpty) throw new RuntimeException(s"Expected query not found: $query")

    result.columnAs[String]("transactionId").next // There should be exactly one match for the given query
  }

  protected def login(username: String, password: String): LoginContext = {
    val authManager = graph.getDependencyResolver.resolveDependency(classOf[AuthManager])
    authManager.login(SecurityTestUtils.authToken(username, password), EMBEDDED_CONNECTION)
  }

  protected def createUser(username: String = username, password: String = password): Unit = {
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute(s"CREATE USER $username SET PASSWORD '$password' CHANGE NOT REQUIRED")
    selectDatabase(DEFAULT_DATABASE_NAME)
  }

  protected def executeAs(username: String, password: String, queryText: String): RewindableExecutionResult = {
    val tx = graph.beginTransaction(Type.EXPLICIT, login(username, password))
    try {
      val result = execute(queryText, Map.empty[String, Any], tx, QueryExecutionConfiguration.DEFAULT_CONFIG)
      tx.commit()
      result
    } finally {
      tx.close()
    }
  }

  protected case class ThreadedTransaction(latch: DoubleLatch, database: String = DEFAULT_DATABASE_NAME) {
    private val graphService = new GraphDatabaseCypherService(managementService.database(database))

    def execute(
      username: String,
      password: String,
      threading: Threading,
      query: String,
      params: Map[String, AnyRef] = Map.empty
    ): Unit = {
      val startTransaction =
        new NamedFunction[LoginContext, Throwable]("threaded-transaction-" + util.Arrays.hashCode(query.toCharArray)) {
          override def apply(subject: LoginContext): Throwable = {
            val tx = graphService.beginTransaction(Type.EXPLICIT, subject)

            try {
              var result: Result = null
              try {
                result = tx.execute(query, params.asJava)
                latch.startAndWaitForAllToStart()
              } finally {
                latch.start()
                latch.finishAndWaitForAllToFinish()
              }
              if (result != null) {
                result.accept((_: Result.ResultRow) => true)
                result.close()
              }
              tx.commit()
              null
            } catch {
              case t: Throwable => t
            } finally {
              if (tx != null) tx.close()
            }
          }
        }
      val subject = login(username, password)
      threading.execute(startTransaction, subject)
    }
  }
}
