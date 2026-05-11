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
import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.RewindableExecutionResult
import org.neo4j.cypher.util.DontRunOnSpdBuild
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

class CommunityShowFuncProcAcceptanceTest extends ExecutionEngineFunSuite with GraphDatabaseTestSupport
    with DontRunOnSpdBuild {
  private val username = "foo"
  private val password = "secretpassword"

  override def databaseConfig(): Map[Setting[?], Object] =
    super.databaseConfig() ++ Map(auth_enabled -> java.lang.Boolean.TRUE)

  override protected def onNewGraphDatabase(): Unit = {
    super.onNewGraphDatabase()
    val globalProcedures: GlobalProcedures = graph.getDependencyResolver.resolveDependency(classOf[GlobalProcedures])
    globalProcedures.registerFunction(classOf[TestShowFunction])
    globalProcedures.registerAggregationFunction(classOf[TestShowFunction])
  }

  private val defaultsToCypher5: Boolean = dbmsDefaultQueryLanguage.equals(CypherVersion.Cypher5)

  private val cypherVersions =
    (CypherVersion.values().map(cv => (s"CYPHER ${cv.versionName} ", cv.equals(CypherVersion.Cypher5)))
      :+ ("", defaultsToCypher5))

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

  protected val builtInFunctionsVerboseCypher5: List[Map[String, Any]] =
    builtInFunctionsVerbose.filter(m => m("cypherVersionScope").asInstanceOf[List[Int]].contains(5))
      .map(m => m.view.filterKeys(k => !k.equals("cypherVersionScope")).toMap)

  protected val builtInFunctionsVerboseCypher25: List[Map[String, Any]] =
    builtInFunctionsVerbose.filter(m => m("cypherVersionScope").asInstanceOf[List[Int]].contains(25))
      .map(m => m.view.filterKeys(k => !k.equals("cypherVersionScope")).toMap)

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
      "signature" -> "test.functionWithInput(input1 :: STRING, input2 :: FLOAT) :: LIST<ANY>",
      "isBuiltIn" -> false,
      "argumentDescription" -> List(
        Map[String, Any](
          "name" -> "input1",
          "description" -> "Input to this test function.",
          "type" -> "STRING",
          "isDeprecated" -> false
        ),
        Map[String, Any](
          "name" -> "input2",
          "description" -> "input2 :: FLOAT",
          "type" -> "FLOAT",
          "isDeprecated" -> false
        )
      ),
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
        "description" -> "A somewhat useful description of this argument.",
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

  private val allFunctionsVerboseCypher5 =
    (builtInFunctionsVerboseCypher5 ++ userDefinedFunctionsVerbose).sortBy(m => m("name").asInstanceOf[String])

  private val allFunctionsVerboseCypher25 =
    (builtInFunctionsVerboseCypher25 ++ userDefinedFunctionsVerbose).sortBy(m => m("name").asInstanceOf[String])

  private val allFunctionsVerbose = if (defaultsToCypher5) allFunctionsVerboseCypher5 else allFunctionsVerboseCypher25
  // Brief output

  private val builtInFunctionsBriefCypher5 =
    builtInFunctionsVerboseCypher5.map(m =>
      m.view.filterKeys(k => Seq("name", "category", "description").contains(k)).toMap
        .map { case (key, value) => (key, value.asInstanceOf[String]) }
    ) // All brief columns are String columns

  private val builtInFunctionsBriefCypher25 =
    builtInFunctionsVerboseCypher25.map(m =>
      m.view.filterKeys(k => Seq("name", "category", "description").contains(k)).toMap
        .map { case (key, value) => (key, value.asInstanceOf[String]) }
    ) // All brief columns are String columns

  private val builtInFunctionsBrief =
    if (defaultsToCypher5) builtInFunctionsBriefCypher5 else builtInFunctionsBriefCypher25

  private val userDefinedFunctionsBrief = List(
    Map("name" -> "test.function", "category" -> "", "description" -> ""),
    Map("name" -> "test.functionWithInput", "category" -> "", "description" -> ""),
    Map(
      "name" -> "test.return.latest",
      "category" -> "",
      "description" -> "Return the latest number, continuously updating the value."
    )
  )

  private val allFunctionsBriefCypher5 =
    (builtInFunctionsBriefCypher5 ++ userDefinedFunctionsBrief).sortBy(m => m("name"))

  private val allFunctionsBriefCypher25 =
    (builtInFunctionsBriefCypher25 ++ userDefinedFunctionsBrief).sortBy(m => m("name"))

  private val allFunctionsBrief = if (defaultsToCypher5) allFunctionsBriefCypher5 else allFunctionsBriefCypher25

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

  test("show functions with Cypher versions") {
    cypherVersions.foreach { case (cypherVersionString, usesCypher5) =>
      selectDatabase(DEFAULT_DATABASE_NAME)
      withClue(cypherVersionString + "user database") {
        // WHEN
        val result = execute(cypherVersionString + "SHOW FUNCTIONS")

        // THEN
        val allFunctions = if (usesCypher5) allFunctionsBriefCypher5 else allFunctionsBriefCypher25
        result.toList should be(allFunctions)
      }

      selectDatabase(SYSTEM_DATABASE_NAME)
      withClue(cypherVersionString + "system database") {
        // WHEN
        val result = execute(cypherVersionString + "SHOW FUNCTIONS")

        // THEN
        val allFunctions = if (usesCypher5) allFunctionsBriefCypher5 else allFunctionsBriefCypher25
        result.toList should be(allFunctions)
      }
    }
  }

  // SHOW PROCEDURES

  private val procResourceUrl = getClass.getResource("/procedures.json")
  if (procResourceUrl == null) throw new NoSuchFileException(s"File not found: procedures.json")

  private val allProceduresVerboseCypher5: List[Map[String, Any]] = readAll(procResourceUrl)
    .filterNot(m => m("enterpriseOnly").asInstanceOf[Boolean])
    .filter(m => m("cypherVersionScope").asInstanceOf[List[Int]].contains(5))
    .map(m => m.view.filterKeys(k => !Seq("enterpriseOnly", "cypherVersionScope").contains(k)).toMap)
    .map(m =>
      m.map {
        case ("rolesExecution", _)        => ("rolesExecution", null)
        case ("rolesBoostedExecution", _) => ("rolesBoostedExecution", null)
        case m                            => m
      }
    )

  private val allProceduresVerboseCypher25: List[Map[String, Any]] = readAll(procResourceUrl)
    .filterNot(m => m("enterpriseOnly").asInstanceOf[Boolean])
    .filter(m => m("cypherVersionScope").asInstanceOf[List[Int]].contains(25))
    .map(m => m.view.filterKeys(k => !Seq("enterpriseOnly", "cypherVersionScope").contains(k)).toMap)
    .map(m =>
      m.map {
        case ("rolesExecution", _)        => ("rolesExecution", null)
        case ("rolesBoostedExecution", _) => ("rolesBoostedExecution", null)
        case m                            => m
      }
    )

  private val allProceduresVerboseDefault: List[Map[String, Any]] =
    if (defaultsToCypher5) allProceduresVerboseCypher5 else allProceduresVerboseCypher25

  private val allProceduresBriefCypher5 = allProceduresVerboseCypher5.map(m =>
    m.view.filterKeys(k => Seq("name", "description", "mode", "worksOnSystem").contains(k)).toMap
  )

  private val allProceduresBriefCypher25 = allProceduresVerboseCypher25.map(m =>
    m.view.filterKeys(k => Seq("name", "description", "mode", "worksOnSystem").contains(k)).toMap
  )

  private val allProceduresBriefDefault: List[Map[String, Any]] =
    if (defaultsToCypher5) allProceduresBriefCypher5 else allProceduresBriefCypher25

  test("should show procedures") {
    // GIVEN
    selectDatabase(DEFAULT_DATABASE_NAME)

    // WHEN
    val result = execute("SHOW PROCEDURES")

    // THEN
    result.toList should be(allProceduresBriefDefault)
  }

  test("should show procedures with yield") {
    // GIVEN
    selectDatabase(DEFAULT_DATABASE_NAME)

    // WHEN
    val result = execute("SHOW PROCEDURES YIELD *")

    // THEN
    result.toList should be(allProceduresVerboseDefault)
  }

  test("should show procedures executable by current user") {
    // GIVEN
    createUser()

    // WHEN
    val result = executeAs(username, password, "SHOW PROCEDURES EXECUTABLE")

    // THEN
    result.toList should be(allProceduresBriefDefault)
  }

  test("should show procedures executable by current user with yield") {
    // GIVEN
    createUser()

    // WHEN
    val result = executeAs(username, password, "SHOW PROCEDURES EXECUTABLE YIELD name, description, signature")

    // THEN
    result.toList should be(allProceduresVerboseDefault.map(m =>
      m.view.filterKeys(k => Seq("name", "description", "signature").contains(k)).toMap
    ))
  }

  test("should show procedures executable by specified user") {
    // GIVEN
    createUser()

    // WHEN
    val result = execute(s"SHOW PROCEDURES EXECUTABLE BY $username")

    // THEN
    result.toList should be(allProceduresBriefDefault)
  }

  test("should show procedures executable by specified user with yield") {
    // GIVEN
    createUser()

    // WHEN
    val result = execute(s"SHOW PROCEDURES EXECUTABLE BY $username YIELD *")

    // THEN
    result.toList should be(allProceduresVerboseDefault)
  }

  test("should show procedures on system") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // WHEN
    val result = execute("SHOW PROCEDURES")

    // THEN
    result.toList should be(allProceduresBriefDefault)
  }

  test("show procedures with Cypher versions") {
    cypherVersions.foreach { case (cypherVersionString, usesCypher5) =>
      val expected = if (usesCypher5) allProceduresBriefCypher5 else allProceduresBriefCypher25

      selectDatabase(DEFAULT_DATABASE_NAME)
      withClue(cypherVersionString + "user database") {
        // WHEN
        val result = execute(cypherVersionString + "SHOW PROCEDURES")

        // THEN
        result.toList should be(expected)
      }

      selectDatabase(SYSTEM_DATABASE_NAME)
      withClue(cypherVersionString + "system database") {
        // WHEN
        val result = execute(cypherVersionString + "SHOW PROCEDURES")

        // THEN
        result.toList should be(expected)
      }
    }
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
  def functionWithInput(
    @Name(value = "input1", description = "Input to this test function.") input1: String,
    @Name(value = "input2") input2: Double
  ): ListValue = {
    val inputVal = Values.stringValue(input1)
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
  def update(@Name(value = "value", description = "A somewhat useful description of this argument.") value: Long)
    : Unit =
    latest = value

  @UserAggregationResult
  def result: Long = latest
}
