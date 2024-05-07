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

import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME
import org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME
import org.neo4j.configuration.GraphDatabaseSettings.auth_enabled
import org.neo4j.cypher.CommunityShowFuncProcAcceptanceTest.readAll
import org.neo4j.cypher.internal.RewindableExecutionResult
import org.neo4j.graphdb.config.Setting
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo
import org.neo4j.kernel.api.KernelTransaction.Type
import org.neo4j.kernel.api.procedure.GlobalProcedures
import org.neo4j.kernel.api.security.AuthManager
import org.neo4j.kernel.impl.query.QueryExecutionConfiguration
import org.neo4j.procedure.Description
import org.neo4j.procedure.Name
import org.neo4j.procedure.UserAggregationFunction
import org.neo4j.procedure.UserAggregationResult
import org.neo4j.procedure.UserAggregationUpdate
import org.neo4j.procedure.UserFunction
import org.neo4j.server.security.auth.SecurityTestUtils
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.ListValue
import org.neo4j.values.virtual.VirtualValues

import java.net.URL
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files
import java.nio.file.NoSuchFileException
import java.nio.file.Paths

import scala.jdk.CollectionConverters.SeqHasAsJava

class CommunityShowFuncProcAcceptanceTest extends ExecutionEngineFunSuite with GraphDatabaseTestSupport {
  private val username = "foo"
  private val password = "secretpassword"

  override def databaseConfig(): Map[Setting[_], Object] =
    super.databaseConfig() ++ Map(auth_enabled -> java.lang.Boolean.TRUE)

  override protected def onNewGraphDatabase(): Unit = {
    super.onNewGraphDatabase()
    val globalProcedures: GlobalProcedures = graph.getDependencyResolver.resolveDependency(classOf[GlobalProcedures])
    globalProcedures.registerFunction(classOf[TestShowFunction])
    globalProcedures.registerAggregationFunction(classOf[TestShowFunction])
  }

  // SHOW FUNCTIONS

  private val funcResourceUrl = getClass.getResource("/builtInFunctions.json")
  if (funcResourceUrl == null) throw new NoSuchFileException(s"File not found: builtInFunctions.json")

  // Verbose output

  private val builtInFunctionsVerbose =
    readAll(funcResourceUrl)
      .filterNot(m => m.getOrElse("enterpriseOnly", false).asInstanceOf[Boolean])
      .map(m => m.view.filterKeys(k => !k.equals("enterpriseOnly")).toMap)
      .map(m =>
        m.map {
          case ("rolesExecution", _)        => ("rolesExecution", null)
          case ("rolesBoostedExecution", _) => ("rolesBoostedExecution", null)
          case m                            => m
        }
      )

  private val userDefinedFunctionsVerbose = List(
    Map[String, Any](
      "name" -> "test.function",
      "category" -> "",
      "description" -> "",
      "signature" -> "test.function() :: STRING",
      "isBuiltIn" -> false,
      "argumentDescription" -> List(),
      "returnDescription" -> "STRING",
      "aggregating" -> false,
      "rolesExecution" -> null,
      "rolesBoostedExecution" -> null,
      "isDeprecated" -> false,
      "deprecatedBy" -> null
    ),
    Map[String, Any](
      "name" -> "test.functionWithInput",
      "category" -> "",
      "description" -> "",
      "signature" -> "test.functionWithInput(input :: STRING) :: LIST<ANY>",
      "isBuiltIn" -> false,
      "argumentDescription" -> List(Map[String, Any](
        "name" -> "input",
        "description" -> "input :: STRING",
        "type" -> "STRING",
        "isDeprecated" -> false
      )),
      "returnDescription" -> "LIST<ANY>",
      "aggregating" -> false,
      "rolesExecution" -> null,
      "rolesBoostedExecution" -> null,
      "isDeprecated" -> false,
      "deprecatedBy" -> null
    ),
    Map[String, Any](
      "name" -> "test.return.latest",
      "category" -> "",
      "description" -> "Return the latest number, continuously updating the value.",
      "signature" -> "test.return.latest(value :: INTEGER) :: INTEGER",
      "isBuiltIn" -> false,
      "argumentDescription" -> List(Map[String, Any](
        "name" -> "value",
        "description" -> "value :: INTEGER",
        "type" -> "INTEGER",
        "isDeprecated" -> false
      )),
      "returnDescription" -> "INTEGER",
      "aggregating" -> true,
      "rolesExecution" -> null,
      "rolesBoostedExecution" -> null,
      "isDeprecated" -> false,
      "deprecatedBy" -> null
    )
  )

  private val allFunctionsVerbose =
    (builtInFunctionsVerbose ++ userDefinedFunctionsVerbose).sortBy(m => m("name").asInstanceOf[String])

  // Brief output

  private val builtInFunctionsBrief =
    builtInFunctionsVerbose.map(m =>
      m.view.filterKeys(k => Seq("name", "category", "description").contains(k)).toMap
        .map { case (key, value) => (key, value.asInstanceOf[String]) }
    ) // All brief columns are String columns

  private val userDefinedFunctionsBrief = List(
    Map("name" -> "test.function", "category" -> "", "description" -> ""),
    Map("name" -> "test.functionWithInput", "category" -> "", "description" -> ""),
    Map(
      "name" -> "test.return.latest",
      "category" -> "",
      "description" -> "Return the latest number, continuously updating the value."
    )
  )

  private val allFunctionsBrief = (builtInFunctionsBrief ++ userDefinedFunctionsBrief).sortBy(m => m("name"))

  // Tests

  test("should show functions") {
    // GIVEN
    selectDatabase(DEFAULT_DATABASE_NAME)

    // WHEN
    val result = execute("SHOW FUNCTIONS")

    // THEN
    result.toList should be(allFunctionsBrief)
  }

  test("should show built-in functions") {
    // GIVEN
    selectDatabase(DEFAULT_DATABASE_NAME)

    // WHEN
    val result = execute("SHOW BUILT IN FUNCTIONS")

    // THEN
    result.toList should be(builtInFunctionsBrief)
  }

  test("should show user-defined functions") {
    // GIVEN
    selectDatabase(DEFAULT_DATABASE_NAME)

    // WHEN
    val result = execute("SHOW USER DEFINED FUNCTIONS")

    // THEN
    result.toList should be(userDefinedFunctionsBrief)
  }

  test("should show functions with yield") {
    // GIVEN
    selectDatabase(DEFAULT_DATABASE_NAME)

    // WHEN
    val result = execute("SHOW FUNCTIONS YIELD *")

    // THEN
    result.toList should be(allFunctionsVerbose)
  }

  test("should show functions executable by current user") {
    // GIVEN
    createUser()

    // WHEN
    val result = executeAs(username, password, "SHOW FUNCTIONS EXECUTABLE")

    // THEN
    result.toList should be(allFunctionsBrief)
  }

  test("should show functions executable by current user with yield") {
    // GIVEN
    createUser()

    // WHEN
    val result = executeAs(username, password, "SHOW FUNCTIONS EXECUTABLE YIELD name, description, isBuiltIn")

    // THEN
    result.toList should be(allFunctionsVerbose.map(m =>
      m.view.filterKeys(k => Seq("name", "description", "isBuiltIn").contains(k)).toMap
    ))
  }

  test("should show functions executable by specified user") {
    // GIVEN
    createUser()

    // WHEN
    val result = execute(s"SHOW FUNCTIONS EXECUTABLE BY $username")

    // THEN
    result.toList should be(allFunctionsBrief)
  }

  test("should show functions executable by specified user with yield") {
    // GIVEN
    createUser()

    // WHEN
    val result = execute(s"SHOW FUNCTIONS EXECUTABLE BY $username YIELD *")

    // THEN
    result.toList should be(allFunctionsVerbose)
  }

  test("should show functions on system") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // WHEN
    val result = execute("SHOW FUNCTIONS")

    // THEN
    result.toList should be(allFunctionsBrief)
  }

  // SHOW PROCEDURES

  private val procResourceUrl = getClass.getResource("/procedures.json")
  if (procResourceUrl == null) throw new NoSuchFileException(s"File not found: procedures.json")

  private val allProceduresVerbose: List[Map[String, Any]] = readAll(procResourceUrl)
    .filterNot(m => m("enterpriseOnly").asInstanceOf[Boolean])
    .map(m => m.view.filterKeys(k => !k.equals("enterpriseOnly")).toMap)
    .map(m =>
      m.map {
        case ("rolesExecution", _)        => ("rolesExecution", null)
        case ("rolesBoostedExecution", _) => ("rolesBoostedExecution", null)
        case m                            => m
      }
    )

  private val allProceduresBrief = allProceduresVerbose.map(m =>
    m.view.filterKeys(k => Seq("name", "description", "mode", "worksOnSystem").contains(k)).toMap
  )

  test("should show procedures") {
    // GIVEN
    selectDatabase(DEFAULT_DATABASE_NAME)

    // WHEN
    val result = execute("SHOW PROCEDURES")

    // THEN
    result.toList should be(allProceduresBrief)
  }

  test("should show procedures with yield") {
    // GIVEN
    selectDatabase(DEFAULT_DATABASE_NAME)

    // WHEN
    val result = execute("SHOW PROCEDURES YIELD *")

    // THEN
    result.toList should be(allProceduresVerbose)
  }

  test("should show procedures executable by current user") {
    // GIVEN
    createUser()

    // WHEN
    val result = executeAs(username, password, "SHOW PROCEDURES EXECUTABLE")

    // THEN
    result.toList should be(allProceduresBrief)
  }

  test("should show procedures executable by current user with yield") {
    // GIVEN
    createUser()

    // WHEN
    val result = executeAs(username, password, "SHOW PROCEDURES EXECUTABLE YIELD name, description, signature")

    // THEN
    result.toList should be(allProceduresVerbose.map(m =>
      m.view.filterKeys(k => Seq("name", "description", "signature").contains(k)).toMap
    ))
  }

  test("should show procedures executable by specified user") {
    // GIVEN
    createUser()

    // WHEN
    val result = execute(s"SHOW PROCEDURES EXECUTABLE BY $username")

    // THEN
    result.toList should be(allProceduresBrief)
  }

  test("should show procedures executable by specified user with yield") {
    // GIVEN
    createUser()

    // WHEN
    val result = execute(s"SHOW PROCEDURES EXECUTABLE BY $username YIELD *")

    // THEN
    result.toList should be(allProceduresVerbose)
  }

  test("should show procedures on system") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // WHEN
    val result = execute("SHOW PROCEDURES")

    // THEN
    result.toList should be(allProceduresBrief)
  }

  // Help methods

  private def createUser(): Unit = {
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute(s"CREATE USER $username SET PASSWORD '$password' CHANGE NOT REQUIRED")
    selectDatabase(DEFAULT_DATABASE_NAME)
  }

  private def executeAs(
    username: String,
    password: String,
    queryText: String,
    params: Map[String, Any] = Map.empty
  ): RewindableExecutionResult = {
    val authManager = graph.getDependencyResolver.resolveDependency(classOf[AuthManager])
    val login =
      authManager.login(SecurityTestUtils.authToken(username, password), ClientConnectionInfo.EMBEDDED_CONNECTION)
    val tx = graph.beginTransaction(Type.EXPLICIT, login)
    try {
      val result = execute(queryText, params, tx, QueryExecutionConfiguration.DEFAULT_CONFIG)
      tx.commit()
      result
    } finally {
      tx.close()
    }
  }
}

object CommunityShowFuncProcAcceptanceTest {

  def readAll(resourceUrl: URL): List[Map[String, Any]] = {
    val jsonMapper = JsonMapper.builder().addModule(DefaultScalaModule).build()
    val reader = Files.newBufferedReader(Paths.get(resourceUrl.toURI), UTF_8)
    jsonMapper.readValue(reader, new TypeReference[List[Map[String, Any]]] {})
  }
}

class TestShowFunction {

  @UserFunction("test.function")
  def function(): String = "OK"

  @UserFunction("test.functionWithInput")
  def functionWithInput(@Name("input") input: String): ListValue = {
    val inputVal = Values.stringValue(input)
    val values: List[AnyValue] = List(inputVal, inputVal, inputVal)
    VirtualValues.fromList(values.asJava)
  }

  @UserAggregationFunction("test.return.latest")
  @Description("Return the latest number, continuously updating the value.")
  def myAggFunc: ReturnLatest = new ReturnLatest
}

object TestShowFunction {
  def apply(): TestShowFunction = new TestShowFunction()
}

class ReturnLatest {
  var latest: Long = 0

  @UserAggregationUpdate
  def update(@Name("value") value: Long): Unit = latest = value

  @UserAggregationResult
  def result: Long = latest
}
