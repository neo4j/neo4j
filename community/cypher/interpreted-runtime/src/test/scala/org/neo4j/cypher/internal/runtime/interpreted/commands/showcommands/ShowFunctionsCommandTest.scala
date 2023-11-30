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
package org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import org.neo4j.cypher.internal.ast.AllFunctions
import org.neo4j.cypher.internal.ast.BuiltInFunctions
import org.neo4j.cypher.internal.ast.CommandResultItem
import org.neo4j.cypher.internal.ast.CurrentUser
import org.neo4j.cypher.internal.ast.ShowFunctionsClause
import org.neo4j.cypher.internal.ast.User
import org.neo4j.cypher.internal.ast.UserDefinedFunctions
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.expressions.functions.Category
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.graphdb.Result
import org.neo4j.internal.kernel.api.procs.FieldSignature
import org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTAny
import org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTBoolean
import org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTDuration
import org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTInteger
import org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTMap
import org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTString
import org.neo4j.internal.kernel.api.procs.QualifiedName
import org.neo4j.internal.kernel.api.procs.UserFunctionSignature
import org.neo4j.internal.kernel.api.security.AdminActionOnResource
import org.neo4j.internal.kernel.api.security.AuthSubject
import org.neo4j.internal.kernel.api.security.PermissionState
import org.neo4j.internal.kernel.api.security.PrivilegeAction.SHOW_ROLE
import org.neo4j.internal.kernel.api.security.Segment
import org.neo4j.kernel.impl.query.FunctionInformation
import org.neo4j.kernel.impl.query.FunctionInformation.InputInformation
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.VirtualValues

import scala.jdk.CollectionConverters.SeqHasAsJava
import scala.jdk.CollectionConverters.SetHasAsJava

class ShowFunctionsCommandTest extends ShowCommandTestBase {

  private val defaultColumns =
    ShowFunctionsClause(AllFunctions, None, None, List.empty, yieldAll = false)(InputPosition.NONE)
      .unfilteredColumns
      .columns

  private val allColumns =
    ShowFunctionsClause(AllFunctions, None, None, List.empty, yieldAll = true)(InputPosition.NONE)
      .unfilteredColumns
      .columns

  private val func1 = new UserFunctionSignature(
    new QualifiedName(List.empty[String].asJava, "func1"),
    List.empty[FieldSignature].asJava,
    NTString,
    null,
    "Built-in non-aggregating function",
    Category.STRING,
    true,
    true,
    false,
    false
  )

  private val func2 = new UserFunctionSignature(
    new QualifiedName(List.empty[String].asJava, "func2"),
    List(FieldSignature.inputField("input", NTAny)).asJava,
    NTBoolean,
    null,
    "Built-in aggregating function",
    Category.AGGREGATING,
    true,
    true,
    false,
    false
  )

  private val func3 = new UserFunctionSignature(
    new QualifiedName(List("zzz").asJava, "func3"),
    List(
      FieldSignature.inputField("intInput", NTInteger),
      FieldSignature.inputField("booleanInput", NTBoolean)
    ).asJava,
    NTMap,
    null,
    "User-defined non-aggregating function",
    Category.NUMERIC,
    true,
    false,
    false,
    false
  )

  private val func4 = new UserFunctionSignature(
    new QualifiedName(List("zzz", "zz").asJava, "func4"),
    List(
      FieldSignature.inputField("input", NTDuration)
    ).asJava,
    NTInteger,
    null,
    "User-defined aggregating function",
    Category.AGGREGATING,
    true,
    false,
    false,
    false
  )

  private val func5 = TestLanguageFunction(
    name = "language.aggregating.func",
    description = "Aggregating language function",
    signature = "language.aggregating.func() :: INTEGER",
    category = Category.AGGREGATING,
    aggregating = true,
    output = NTInteger.toString,
    arguments = List.empty
  )

  private val func6 = TestLanguageFunction(
    name = "language.func",
    description = "Language function",
    signature = "language.func(input :: FLOAT) :: STRING",
    category = Category.STRING,
    aggregating = false,
    output = NTString.toString,
    arguments = List(new InputInformation(
      "input",
      "FLOAT",
      "input :: FLOAT",
      false,
      java.util.Optional.empty[String]()
    ))
  )

  // Cannot reach the default role variables (and are in either case mocking the privileges & roles...)
  private val readerRole = "reader"
  private val editorRole = "editor"
  private val publisherRole = "publisher"
  private val architectRole = "architect"

  override def beforeEach(): Unit = {
    super.beforeEach()

    // Defaults:
    mockSetupProcFunc()
    when(systemTx.execute(any())).thenAnswer(invocation => handleSystemQueries(invocation.getArgument(0)))
    when(systemTx.execute(any(), any())).thenAnswer(invocation => handleSystemQueries(invocation.getArgument(0)))
  }

  private def handleSystemQueries(query: String): Result = query match {
    case "SHOW ALL ROLES YIELD role" =>
      MockResult(List(
        Map("role" -> publicRole),
        Map("role" -> readerRole),
        Map("role" -> editorRole),
        Map("role" -> publisherRole),
        Map("role" -> architectRole),
        Map("role" -> adminRole)
      ).sortBy(m => m("role")))
    case _ => handleSystemQueries(query, "FUNCTION")
  }

  /* func1:
   *  roles: PUBLIC, reader, editor, publisher, architect, admin
   *  boostedRoles:
   *
   * func2:
   *  roles: PUBLIC, reader, editor, publisher, architect, admin
   *  boostedRoles:
   *
   * func3:
   *  roles: PUBLIC, reader, admin
   *  boostedRoles: admin
   *
   * func4:
   *  roles: editor, admin
   *  boostedRoles: reader, architect, admin
   */
  def morePrivileges(query: String): Result = {
    query match {
      case "SHOW ALL PRIVILEGES YIELD * WHERE action='execute' AND segment STARTS WITH $seg RETURN access, segment, collect(role) as roles" =>
        MockResult(List(
          Map("access" -> "GRANTED", "segment" -> "FUNCTION(*)", "roles" -> List(publicRole).asJava),
          Map("access" -> "GRANTED", "segment" -> "FUNCTION(*3)", "roles" -> List(readerRole).asJava),
          Map("access" -> "GRANTED", "segment" -> "FUNCTION(*4)", "roles" -> List(editorRole).asJava),
          // deny on *2 won't affect as it's a built-in function
          Map("access" -> "DENIED", "segment" -> "FUNCTION(*2)", "roles" -> List(publicRole).asJava),
          Map("access" -> "DENIED", "segment" -> "FUNCTION(*4)", "roles" -> List(publicRole).asJava)
        ))
      case "SHOW ALL PRIVILEGES YIELD * WHERE action STARTS WITH 'execute_boosted' AND segment STARTS WITH $seg RETURN access, segment, collect(role) as roles" =>
        MockResult(List(
          Map("access" -> "GRANTED", "segment" -> "FUNCTION(*)", "roles" -> List(architectRole).asJava),
          Map("access" -> "GRANTED", "segment" -> "FUNCTION(*4)", "roles" -> List(readerRole).asJava),
          Map("access" -> "DENIED", "segment" -> "FUNCTION(*3)", "roles" -> List(architectRole).asJava)
        ))
      case "SHOW ALL PRIVILEGES YIELD * WHERE action IN ['admin', 'dbms_actions'] RETURN access, collect(role) as roles" =>
        MockResult(List(Map("access" -> "GRANTED", "roles" -> List(adminRole).asJava)))
      case _ => handleSystemQueries(query)
    }
  }

  private def returnDefaultFunctions(): Unit = {
    when(procedures.functionGetAll()).thenReturn(List(func1, func3).asJava.stream())
    when(procedures.aggregationFunctionGetAll()).thenReturn(List(func2, func4).asJava.stream())
    when(ctx.providedLanguageFunctions).thenReturn(List(func5, func6))
  }

  // Only checks the given parameters
  private def checkResult(
    resultMap: Map[String, AnyValue],
    name: Option[String] = None,
    category: Option[String] = None,
    description: Option[String] = None,
    signature: Option[String] = None,
    isBuiltIn: Option[Boolean] = None,
    argumentDescription: Option[List[AnyValue]] = None,
    returnDescription: Option[String] = None,
    aggregating: Option[Boolean] = None,
    roles: Option[List[String]] = None,
    rolesBoosted: Option[List[String]] = None,
    isDeprecated: Option[Boolean] = None
  ): Unit = {
    name.foreach(expected => resultMap(ShowFunctionsClause.nameColumn) should be(Values.stringValue(expected)))
    category.foreach(expected => resultMap(ShowFunctionsClause.categoryColumn) should be(Values.stringValue(expected)))
    description.foreach(expected =>
      resultMap(ShowFunctionsClause.descriptionColumn) should be(Values.stringValue(expected))
    )
    signature.foreach(expected =>
      resultMap(ShowFunctionsClause.signatureColumn) should be(Values.stringValue(expected))
    )
    isBuiltIn.foreach(expected =>
      resultMap(ShowFunctionsClause.isBuiltInColumn) should be(Values.booleanValue(expected))
    )
    argumentDescription.foreach(expected =>
      resultMap(ShowFunctionsClause.argumentDescriptionColumn) should be(VirtualValues.list(expected: _*))
    )
    returnDescription.foreach(expected =>
      resultMap(ShowFunctionsClause.returnDescriptionColumn) should be(Values.stringValue(expected))
    )
    aggregating.foreach(expected =>
      resultMap(ShowFunctionsClause.aggregatingColumn) should be(Values.booleanValue(expected))
    )
    roles.foreach(expected =>
      if (expected == null) resultMap(ShowFunctionsClause.rolesExecutionColumn) should be(Values.NO_VALUE)
      else resultMap(ShowFunctionsClause.rolesExecutionColumn) should be(
        VirtualValues.list(expected.map(Values.stringValue): _*)
      )
    )
    rolesBoosted.foreach(expected =>
      if (expected == null) resultMap(ShowFunctionsClause.rolesBoostedExecutionColumn) should be(Values.NO_VALUE)
      else resultMap(ShowFunctionsClause.rolesBoostedExecutionColumn) should be(
        VirtualValues.list(expected.map(Values.stringValue): _*)
      )
    )
    isDeprecated.foreach(expected =>
      resultMap(ShowFunctionsClause.isDeprecatedColumn) should be(Values.booleanValue(expected))
    )
  }

  // Tests

  test("show functions should give back correct community default values") {
    // Set-up which functions to return:
    returnDefaultFunctions()

    // When
    val showFunctions =
      ShowFunctionsCommand(AllFunctions, None, defaultColumns, List.empty, isCommunity = true)
    val result = showFunctions.originalNameRows(queryState, initialCypherRow).toList

    // Then
    result should have size 6
    checkResult(
      result.head,
      name = "func1",
      category = Category.STRING,
      description = "Built-in non-aggregating function"
    )
    checkResult(
      result(1),
      name = "func2",
      category = Category.AGGREGATING,
      description = "Built-in aggregating function"
    )
    checkResult(
      result(2),
      name = "language.aggregating.func",
      category = Category.AGGREGATING,
      description = "Aggregating language function"
    )
    checkResult(
      result(3),
      name = "language.func",
      category = Category.STRING,
      description = "Language function"
    )
    checkResult(
      result(4),
      name = "zzz.func3",
      category = Category.NUMERIC,
      description = "User-defined non-aggregating function"
    )
    checkResult(
      result(5),
      name = "zzz.zz.func4",
      category = Category.AGGREGATING,
      description = "User-defined aggregating function"
    )
    // confirm no verbose columns:
    result.foreach(res => {
      res.keys.toList should contain noElementsOf List(
        ShowFunctionsClause.signatureColumn,
        ShowFunctionsClause.isBuiltInColumn,
        ShowFunctionsClause.argumentDescriptionColumn,
        ShowFunctionsClause.returnDescriptionColumn,
        ShowFunctionsClause.aggregatingColumn,
        ShowFunctionsClause.rolesExecutionColumn,
        ShowFunctionsClause.rolesBoostedExecutionColumn,
        ShowFunctionsClause.isDeprecatedColumn
      )
    })
  }

  test("show functions should give back correct enterprise default values") {
    // Set-up which functions to return:
    returnDefaultFunctions()

    // When
    val showFunctions =
      ShowFunctionsCommand(AllFunctions, None, defaultColumns, List.empty, isCommunity = false)
    val result = showFunctions.originalNameRows(queryState, initialCypherRow).toList

    // Then
    result should have size 6
    checkResult(
      result.head,
      name = "func1",
      category = Category.STRING,
      description = "Built-in non-aggregating function"
    )
    checkResult(
      result(1),
      name = "func2",
      category = Category.AGGREGATING,
      description = "Built-in aggregating function"
    )
    checkResult(
      result(2),
      name = "language.aggregating.func",
      category = Category.AGGREGATING,
      description = "Aggregating language function"
    )
    checkResult(
      result(3),
      name = "language.func",
      category = Category.STRING,
      description = "Language function"
    )
    checkResult(
      result(4),
      name = "zzz.func3",
      category = Category.NUMERIC,
      description = "User-defined non-aggregating function"
    )
    checkResult(
      result(5),
      name = "zzz.zz.func4",
      category = Category.AGGREGATING,
      description = "User-defined aggregating function"
    )
    // confirm no verbose columns:
    result.foreach(res => {
      res.keys.toList should contain noElementsOf List(
        ShowFunctionsClause.signatureColumn,
        ShowFunctionsClause.isBuiltInColumn,
        ShowFunctionsClause.argumentDescriptionColumn,
        ShowFunctionsClause.returnDescriptionColumn,
        ShowFunctionsClause.aggregatingColumn,
        ShowFunctionsClause.rolesExecutionColumn,
        ShowFunctionsClause.rolesBoostedExecutionColumn,
        ShowFunctionsClause.isDeprecatedColumn
      )
    })
  }

  test("show functions should give back correct community full values") {
    // Set-up which functions to return:
    returnDefaultFunctions()

    // When
    val showFunctions =
      ShowFunctionsCommand(AllFunctions, None, allColumns, List.empty, isCommunity = true)
    val result = showFunctions.originalNameRows(queryState, initialCypherRow).toList

    // Then
    result should have size 6
    checkResult(
      result.head,
      name = "func1",
      category = Category.STRING,
      description = "Built-in non-aggregating function",
      signature = "func1() :: STRING",
      isBuiltIn = true,
      argumentDescription = List.empty[AnyValue],
      returnDescription = "STRING",
      aggregating = false,
      roles = Some(null),
      rolesBoosted = Some(null),
      isDeprecated = false
    )
    checkResult(
      result(1),
      name = "func2",
      category = Category.AGGREGATING,
      description = "Built-in aggregating function",
      signature = "func2(input :: ANY) :: BOOLEAN",
      isBuiltIn = true,
      argumentDescription = List(argumentAndReturnDescriptionMaps("input", "input :: ANY", "ANY")),
      returnDescription = "BOOLEAN",
      aggregating = true,
      roles = Some(null),
      rolesBoosted = Some(null),
      isDeprecated = false
    )
    checkResult(
      result(2),
      name = "language.aggregating.func",
      category = Category.AGGREGATING,
      description = "Aggregating language function",
      signature = "language.aggregating.func() :: INTEGER",
      isBuiltIn = true,
      argumentDescription = List.empty[AnyValue],
      returnDescription = "INTEGER",
      aggregating = true,
      roles = Some(null),
      rolesBoosted = Some(null),
      isDeprecated = false
    )
    checkResult(
      result(3),
      name = "language.func",
      category = Category.STRING,
      description = "Language function",
      signature = "language.func(input :: FLOAT) :: STRING",
      isBuiltIn = true,
      argumentDescription = List(argumentAndReturnDescriptionMaps("input", "input :: FLOAT", "FLOAT")),
      returnDescription = "STRING",
      aggregating = false,
      roles = Some(null),
      rolesBoosted = Some(null),
      isDeprecated = false
    )
    checkResult(
      result(4),
      name = "zzz.func3",
      category = Category.NUMERIC,
      description = "User-defined non-aggregating function",
      signature = "zzz.func3(intInput :: INTEGER, booleanInput :: BOOLEAN) :: MAP",
      isBuiltIn = false,
      argumentDescription = List(
        argumentAndReturnDescriptionMaps("intInput", "intInput :: INTEGER", "INTEGER"),
        argumentAndReturnDescriptionMaps("booleanInput", "booleanInput :: BOOLEAN", "BOOLEAN")
      ),
      returnDescription = "MAP",
      aggregating = false,
      roles = Some(null),
      rolesBoosted = Some(null),
      isDeprecated = false
    )
    checkResult(
      result(5),
      name = "zzz.zz.func4",
      category = Category.AGGREGATING,
      description = "User-defined aggregating function",
      signature = "zzz.zz.func4(input :: DURATION) :: INTEGER",
      isBuiltIn = false,
      argumentDescription = List(argumentAndReturnDescriptionMaps("input", "input :: DURATION", "DURATION")),
      returnDescription = "INTEGER",
      aggregating = true,
      roles = Some(null),
      rolesBoosted = Some(null),
      isDeprecated = false
    )
  }

  test("show functions should give back correct enterprise full values") {
    // Set-up which functions to return:
    returnDefaultFunctions()

    // When
    val showFunctions =
      ShowFunctionsCommand(AllFunctions, None, allColumns, List.empty, isCommunity = false)
    val result = showFunctions.originalNameRows(queryState, initialCypherRow).toList

    // Then
    result should have size 6
    checkResult(
      result.head,
      name = "func1",
      category = Category.STRING,
      description = "Built-in non-aggregating function",
      signature = "func1() :: STRING",
      isBuiltIn = true,
      argumentDescription = List.empty[AnyValue],
      returnDescription = "STRING",
      aggregating = false,
      roles = List(publicRole, readerRole, editorRole, publisherRole, architectRole, adminRole).sorted,
      rolesBoosted = List.empty[String],
      isDeprecated = false
    )
    checkResult(
      result(1),
      name = "func2",
      category = Category.AGGREGATING,
      description = "Built-in aggregating function",
      signature = "func2(input :: ANY) :: BOOLEAN",
      isBuiltIn = true,
      argumentDescription = List(argumentAndReturnDescriptionMaps("input", "input :: ANY", "ANY")),
      returnDescription = "BOOLEAN",
      aggregating = true,
      roles = List(publicRole, readerRole, editorRole, publisherRole, architectRole, adminRole).sorted,
      rolesBoosted = List.empty[String],
      isDeprecated = false
    )
    checkResult(
      result(2),
      name = "language.aggregating.func",
      category = Category.AGGREGATING,
      description = "Aggregating language function",
      signature = "language.aggregating.func() :: INTEGER",
      isBuiltIn = true,
      argumentDescription = List.empty[AnyValue],
      returnDescription = "INTEGER",
      aggregating = true,
      roles = List(publicRole, readerRole, editorRole, publisherRole, architectRole, adminRole).sorted,
      rolesBoosted = List.empty[String],
      isDeprecated = false
    )
    checkResult(
      result(3),
      name = "language.func",
      category = Category.STRING,
      description = "Language function",
      signature = "language.func(input :: FLOAT) :: STRING",
      isBuiltIn = true,
      argumentDescription = List(argumentAndReturnDescriptionMaps("input", "input :: FLOAT", "FLOAT")),
      returnDescription = "STRING",
      aggregating = false,
      roles = List(publicRole, readerRole, editorRole, publisherRole, architectRole, adminRole).sorted,
      rolesBoosted = List.empty[String],
      isDeprecated = false
    )
    checkResult(
      result(4),
      name = "zzz.func3",
      category = Category.NUMERIC,
      description = "User-defined non-aggregating function",
      signature = "zzz.func3(intInput :: INTEGER, booleanInput :: BOOLEAN) :: MAP",
      isBuiltIn = false,
      argumentDescription = List(
        argumentAndReturnDescriptionMaps("intInput", "intInput :: INTEGER", "INTEGER"),
        argumentAndReturnDescriptionMaps("booleanInput", "booleanInput :: BOOLEAN", "BOOLEAN")
      ),
      returnDescription = "MAP",
      aggregating = false,
      roles = List(publicRole, adminRole),
      rolesBoosted = List(adminRole),
      isDeprecated = false
    )
    checkResult(
      result(5),
      name = "zzz.zz.func4",
      category = Category.AGGREGATING,
      description = "User-defined aggregating function",
      signature = "zzz.zz.func4(input :: DURATION) :: INTEGER",
      isBuiltIn = false,
      argumentDescription = List(argumentAndReturnDescriptionMaps("input", "input :: DURATION", "DURATION")),
      returnDescription = "INTEGER",
      aggregating = true,
      roles = List(publicRole, adminRole),
      rolesBoosted = List(adminRole),
      isDeprecated = false
    )
  }

  test("show functions should return the functions sorted on name") {
    // Set-up which functions to return, not ordered by name:
    when(procedures.functionGetAll()).thenReturn(List(func3, func1).asJava.stream())
    when(procedures.aggregationFunctionGetAll()).thenReturn(List(func4, func2).asJava.stream())
    when(ctx.providedLanguageFunctions).thenReturn(List(func6, func5))

    // When
    val showFunctions =
      ShowFunctionsCommand(AllFunctions, None, defaultColumns, List.empty, isCommunity = true)
    val result = showFunctions.originalNameRows(queryState, initialCypherRow).toList

    // Then
    result should have size 6
    checkResult(result.head, name = "func1")
    checkResult(result(1), name = "func2")
    checkResult(result(2), name = "language.aggregating.func")
    checkResult(result(3), name = "language.func")
    checkResult(result(4), name = "zzz.func3")
    checkResult(result(5), name = "zzz.zz.func4")
  }

  test("show functions should not return internal functions") {
    // Set-up which functions to return:
    val internalFunc = new UserFunctionSignature(
      new QualifiedName(List("internal").asJava, "func"),
      List.empty[FieldSignature].asJava,
      NTString,
      null,
      "Internal function",
      Category.STRING,
      true,
      true,
      true,
      false
    )
    val internalAggregatingFunc = new UserFunctionSignature(
      new QualifiedName(List("internal.aggregating").asJava, "func"),
      List.empty[FieldSignature].asJava,
      NTString,
      null,
      "Internal aggregating function",
      Category.STRING,
      true,
      true,
      true,
      false
    )
    when(procedures.functionGetAll()).thenReturn(List(func1, internalFunc).asJava.stream())
    when(procedures.aggregationFunctionGetAll()).thenReturn(List(func2, internalAggregatingFunc).asJava.stream())
    when(ctx.providedLanguageFunctions).thenReturn(List.empty)

    // When
    val showFunctions =
      ShowFunctionsCommand(AllFunctions, None, defaultColumns, List.empty, isCommunity = true)
    val result = showFunctions.originalNameRows(queryState, initialCypherRow).toList

    // Then
    result should have size 2
    checkResult(result.head, name = "func1")
    checkResult(result.last, name = "func2")
  }

  test("show functions should return deprecated functions") {
    // Set-up which functions to return:
    val deprecatedFunc = new UserFunctionSignature(
      new QualifiedName(List("deprecated").asJava, "func"),
      List.empty[FieldSignature].asJava,
      NTString,
      "I'm deprecated",
      "Deprecated function",
      Category.STRING,
      true,
      true,
      false,
      false
    )
    val deprecatedAggregatingFunc = new UserFunctionSignature(
      new QualifiedName(List("deprecated.aggregating").asJava, "func"),
      List.empty[FieldSignature].asJava,
      NTString,
      "I'm deprecated",
      "Deprecated aggregating function",
      Category.STRING,
      true,
      false,
      false,
      false
    )
    val deprecatedLanguageFunction = TestLanguageFunction(
      name = "deprecated.language",
      description = "Deprecated language function",
      signature = "deprecated.language() :: INTEGER",
      category = Category.SCALAR,
      aggregating = false,
      output = NTInteger.toString,
      arguments = List.empty,
      deprecated = true
    )
    when(procedures.functionGetAll()).thenReturn(List(deprecatedFunc).asJava.stream())
    when(procedures.aggregationFunctionGetAll()).thenReturn(List(deprecatedAggregatingFunc).asJava.stream())
    when(ctx.providedLanguageFunctions).thenReturn(List(deprecatedLanguageFunction))

    // When
    val showFunctions =
      ShowFunctionsCommand(AllFunctions, None, allColumns, List.empty, isCommunity = true)
    val result = showFunctions.originalNameRows(queryState, initialCypherRow).toList

    // Then
    result should have size 3
    checkResult(
      result.head,
      name = "deprecated.aggregating.func",
      signature = "deprecated.aggregating.func() :: STRING",
      isDeprecated = true
    )
    checkResult(
      result(1),
      name = "deprecated.func",
      signature = "deprecated.func() :: STRING",
      isDeprecated = true
    )
    checkResult(
      result(2),
      name = "deprecated.language",
      signature = "deprecated.language() :: INTEGER",
      isDeprecated = true
    )
  }

  test("show functions should not give back roles without SHOW ROLE privilege") {
    // Set-up which functions to return:
    returnDefaultFunctions()

    // Block SHOW ROLE
    when(securityContext.allowsAdminAction(any())).thenAnswer(_.getArgument[AdminActionOnResource](0) match {
      case a: AdminActionOnResource
        if a.matches(new AdminActionOnResource(SHOW_ROLE, AdminActionOnResource.DatabaseScope.ALL, Segment.ALL)) =>
        PermissionState.NOT_GRANTED
      case _ => PermissionState.EXPLICIT_GRANT
    })

    // When
    val showFunctions =
      ShowFunctionsCommand(AllFunctions, None, allColumns, List.empty, isCommunity = false)
    val result = showFunctions.originalNameRows(queryState, initialCypherRow).toList

    // Then
    result should have size 6
    result.foreach(res => checkResult(res, roles = Some(null), rolesBoosted = Some(null)))
  }

  test("show functions should not give back roles when denied SHOW ROLE privilege") {
    // Set-up which functions to return:
    returnDefaultFunctions()

    // Block SHOW ROLE
    when(securityContext.allowsAdminAction(any())).thenAnswer(_.getArgument[AdminActionOnResource](0) match {
      case a: AdminActionOnResource
        if a.matches(new AdminActionOnResource(SHOW_ROLE, AdminActionOnResource.DatabaseScope.ALL, Segment.ALL)) =>
        PermissionState.EXPLICIT_DENY
      case _ => PermissionState.EXPLICIT_GRANT
    })

    // When
    val showFunctions =
      ShowFunctionsCommand(AllFunctions, None, allColumns, List.empty, isCommunity = false)
    val result = showFunctions.originalNameRows(queryState, initialCypherRow).toList

    // Then
    result should have size 6
    result.foreach(res => checkResult(res, roles = Some(null), rolesBoosted = Some(null)))
  }

  test("show functions should return correct roles") {
    // Set-up which functions to return:
    returnDefaultFunctions()

    // Set-up role and privileges
    when(systemTx.execute(any())).thenAnswer(invocation => morePrivileges(invocation.getArgument(0)))
    when(systemTx.execute(any(), any()))
      .thenAnswer(invocation => morePrivileges(invocation.getArgument(0)))

    // When
    val showFunctions =
      ShowFunctionsCommand(AllFunctions, None, allColumns, List.empty, isCommunity = false)
    val result = showFunctions.originalNameRows(queryState, initialCypherRow).toList

    // Then
    result should have size 6
    checkResult(
      result.head,
      name = "func1",
      roles = List(publicRole, readerRole, editorRole, publisherRole, architectRole, adminRole).sorted,
      rolesBoosted = List.empty[String]
    )
    checkResult(
      result(1),
      name = "func2",
      roles = List(publicRole, readerRole, editorRole, publisherRole, architectRole, adminRole).sorted,
      rolesBoosted = List.empty[String]
    )
    checkResult(
      result(2),
      name = "language.aggregating.func",
      roles = List(publicRole, readerRole, editorRole, publisherRole, architectRole, adminRole).sorted,
      rolesBoosted = List.empty[String]
    )
    checkResult(
      result(3),
      name = "language.func",
      roles = List(publicRole, readerRole, editorRole, publisherRole, architectRole, adminRole).sorted,
      rolesBoosted = List.empty[String]
    )
    checkResult(
      result(4),
      name = "zzz.func3",
      roles = List(publicRole, readerRole, adminRole).sorted,
      rolesBoosted = List(adminRole)
    )
    checkResult(
      result(5),
      name = "zzz.zz.func4",
      roles = List(editorRole, adminRole).sorted,
      rolesBoosted = List(readerRole, architectRole, adminRole).sorted
    )
  }

  test("show functions executable by current user should return everything with AUTH_DISABLED") {
    def defaultRolesOnly(query: String): Result = query match {
      case "SHOW ALL ROLES YIELD role" =>
        MockResult(List(
          Map("role" -> publicRole),
          Map("role" -> readerRole),
          Map("role" -> editorRole),
          Map("role" -> publisherRole),
          Map("role" -> architectRole),
          Map("role" -> adminRole)
        ).sortBy(m => m("role")))
      case _ => MockResult()
    }

    // Set-up which functions to return:
    returnDefaultFunctions()

    // Set user and privileges
    when(securityContext.subject()).thenReturn(AuthSubject.AUTH_DISABLED)

    when(securityContext.allowsAdminAction(any())).thenReturn(PermissionState.EXPLICIT_DENY)
    when(systemTx.execute(any())).thenAnswer(invocation => defaultRolesOnly(invocation.getArgument(0)))
    when(systemTx.execute(any(), any()))
      .thenAnswer(invocation => defaultRolesOnly(invocation.getArgument(0)))

    // When
    val showFunctions =
      ShowFunctionsCommand(
        AllFunctions,
        Some(CurrentUser),
        defaultColumns,
        List.empty,
        isCommunity = false
      )
    val result = showFunctions.originalNameRows(queryState, initialCypherRow).toList

    // Then
    result should have size 6
    checkResult(result.head, name = "func1")
    checkResult(result(1), name = "func2")
    checkResult(result(2), name = "language.aggregating.func")
    checkResult(result(3), name = "language.func")
    checkResult(result(4), name = "zzz.func3")
    checkResult(result(5), name = "zzz.zz.func4")
  }

  test("show functions executable by current user should only return executable functions") {
    // Set-up which functions to return:
    returnDefaultFunctions()

    // Set user and privileges
    val username = "my_user"
    val user = mock[AuthSubject]
    when(user.hasUsername(username)).thenReturn(true)
    when(securityContext.subject()).thenReturn(user)
    when(securityContext.roles()).thenReturn(Set(publicRole).asJava)

    when(systemTx.execute(any())).thenAnswer(invocation => morePrivileges(invocation.getArgument(0)))
    when(systemTx.execute(any(), any()))
      .thenAnswer(invocation => morePrivileges(invocation.getArgument(0)))

    // When: EXECUTABLE BY CURRENT USER
    val showFunctionsCurrent =
      ShowFunctionsCommand(
        AllFunctions,
        Some(CurrentUser),
        defaultColumns,
        List.empty,
        isCommunity = false
      )
    val resultCurrent = showFunctionsCurrent.originalNameRows(queryState, initialCypherRow).toList

    // Then
    resultCurrent should have size 5
    checkResult(resultCurrent.head, name = "func1")
    checkResult(resultCurrent(1), name = "func2")
    checkResult(resultCurrent(2), name = "language.aggregating.func")
    checkResult(resultCurrent(3), name = "language.func")
    checkResult(resultCurrent(4), name = "zzz.func3")

    // Need to return new function streams
    returnDefaultFunctions()

    // When: EXECUTABLE BY <current user>
    val showFunctionsSame =
      ShowFunctionsCommand(
        AllFunctions,
        Some(User(username)),
        defaultColumns,
        List.empty,
        isCommunity = false
      )
    val resultSame = showFunctionsSame.originalNameRows(queryState, initialCypherRow).toList

    // Then
    resultSame should be(resultCurrent)
  }

  test("show functions executable by given user should only return executable functions") {
    // Set-up which functions to return:
    returnDefaultFunctions()

    // Set user and privileges
    val user = mock[AuthSubject]
    val otherUser = "other_user"
    when(user.hasUsername(otherUser)).thenReturn(false)
    when(securityContext.subject()).thenReturn(user)

    when(systemTx.execute(any())).thenAnswer(invocation => morePrivileges(invocation.getArgument(0)))
    when(systemTx.execute(any(), any()))
      .thenAnswer(invocation => morePrivileges(invocation.getArgument(0)))

    // When
    val showFunctionsCurrent =
      ShowFunctionsCommand(
        AllFunctions,
        Some(User(otherUser)),
        defaultColumns,
        List.empty,
        isCommunity = false
      )
    val resultCurrent = showFunctionsCurrent.originalNameRows(queryState, initialCypherRow).toList

    // Then
    resultCurrent should have size 5
    checkResult(resultCurrent.head, name = "func1")
    checkResult(resultCurrent(1), name = "func2")
    checkResult(resultCurrent(2), name = "language.aggregating.func")
    checkResult(resultCurrent(3), name = "language.func")
    checkResult(resultCurrent(4), name = "zzz.func3")
  }

  test("show functions executable by given user should return nothing for non-existing users") {
    // Return no result for missing user
    def specialHandlingOfUserRoles(query: String) = query match {
      case "SHOW USERS YIELD user, roles WHERE user = $name RETURN roles" =>
        MockResult()
      case _ => handleSystemQueries(query)
    }

    // Set-up which functions exists:
    returnDefaultFunctions()

    // Set user and privileges
    val user = mock[AuthSubject]
    val missingUser = "missing_user"
    when(user.hasUsername(missingUser)).thenReturn(false)
    when(securityContext.subject()).thenReturn(user)

    when(systemTx.execute(any())).thenAnswer(invocation => specialHandlingOfUserRoles(invocation.getArgument(0)))
    when(systemTx.execute(any(), any()))
      .thenAnswer(invocation => specialHandlingOfUserRoles(invocation.getArgument(0)))

    // When
    val showFunctionsCurrent =
      ShowFunctionsCommand(
        AllFunctions,
        Some(User(missingUser)),
        defaultColumns,
        List.empty,
        isCommunity = false
      )
    val resultCurrent = showFunctionsCurrent.originalNameRows(queryState, initialCypherRow).toList

    // Then
    resultCurrent should have size 0
  }

  test("show functions should show all function types") {
    // Set-up which functions the context returns:
    returnDefaultFunctions()

    // When
    val showFunctions =
      ShowFunctionsCommand(AllFunctions, None, allColumns, List.empty, isCommunity = false)
    val result = showFunctions.originalNameRows(queryState, initialCypherRow).toList

    // Then
    result should have size 6
    checkResult(
      result.head,
      name = "func1",
      isBuiltIn = true
    )
    checkResult(
      result(1),
      name = "func2",
      isBuiltIn = true
    )
    checkResult(
      result(2),
      name = "language.aggregating.func",
      isBuiltIn = true
    )
    checkResult(
      result(3),
      name = "language.func",
      isBuiltIn = true
    )
    checkResult(
      result(4),
      name = "zzz.func3",
      isBuiltIn = false
    )
    checkResult(
      result(5),
      name = "zzz.zz.func4",
      isBuiltIn = false
    )
  }

  test("show built in functions should only show built-in functions") {
    // Set-up which functions the context returns:
    returnDefaultFunctions()

    // When
    val showFunctions =
      ShowFunctionsCommand(BuiltInFunctions, None, allColumns, List.empty, isCommunity = false)
    val result = showFunctions.originalNameRows(queryState, initialCypherRow).toList

    // Then
    result should have size 4
    checkResult(
      result.head,
      name = "func1",
      isBuiltIn = true
    )
    checkResult(
      result(1),
      name = "func2",
      isBuiltIn = true
    )
    checkResult(
      result(2),
      name = "language.aggregating.func",
      isBuiltIn = true
    )
    checkResult(
      result(3),
      name = "language.func",
      isBuiltIn = true
    )
  }

  test("show user defined functions should only show user-defined functions") {
    // Set-up which functions the context returns:
    returnDefaultFunctions()

    // When
    val showFunctions =
      ShowFunctionsCommand(UserDefinedFunctions, None, allColumns, List.empty, isCommunity = false)
    val result = showFunctions.originalNameRows(queryState, initialCypherRow).toList

    // Then
    result should have size 2
    checkResult(
      result.head,
      name = "zzz.func3",
      isBuiltIn = false
    )
    checkResult(
      result.last,
      name = "zzz.zz.func4",
      isBuiltIn = false
    )
  }

  test("show functions should rename columns renamed in YIELD") {
    // Given: YIELD name AS function, aggregating AS aggr, isBuiltIn, description
    val yieldColumns: List[CommandResultItem] = List(
      CommandResultItem(ShowFunctionsClause.nameColumn, Variable("function")(InputPosition.NONE))(InputPosition.NONE),
      CommandResultItem(
        ShowFunctionsClause.aggregatingColumn,
        Variable("aggr")(InputPosition.NONE)
      )(InputPosition.NONE),
      CommandResultItem(
        ShowFunctionsClause.isBuiltInColumn,
        Variable(ShowFunctionsClause.isBuiltInColumn)(InputPosition.NONE)
      )(InputPosition.NONE),
      CommandResultItem(
        ShowFunctionsClause.descriptionColumn,
        Variable(ShowFunctionsClause.descriptionColumn)(InputPosition.NONE)
      )(InputPosition.NONE)
    )

    // Set-up which functions to return:
    returnDefaultFunctions()

    // When
    val showFunctions =
      ShowFunctionsCommand(AllFunctions, None, allColumns, yieldColumns, isCommunity = false)
    val result = showFunctions.originalNameRows(queryState, initialCypherRow).toList

    // Then
    result should have size 6
    result should be(List(
      Map(
        "function" -> Values.stringValue("func1"),
        "aggr" -> Values.FALSE,
        ShowFunctionsClause.isBuiltInColumn -> Values.TRUE,
        ShowFunctionsClause.descriptionColumn -> Values.stringValue("Built-in non-aggregating function")
      ),
      Map(
        "function" -> Values.stringValue("func2"),
        "aggr" -> Values.TRUE,
        ShowFunctionsClause.isBuiltInColumn -> Values.TRUE,
        ShowFunctionsClause.descriptionColumn -> Values.stringValue("Built-in aggregating function")
      ),
      Map(
        "function" -> Values.stringValue("language.aggregating.func"),
        "aggr" -> Values.TRUE,
        ShowFunctionsClause.isBuiltInColumn -> Values.TRUE,
        ShowFunctionsClause.descriptionColumn -> Values.stringValue("Aggregating language function")
      ),
      Map(
        "function" -> Values.stringValue("language.func"),
        "aggr" -> Values.FALSE,
        ShowFunctionsClause.isBuiltInColumn -> Values.TRUE,
        ShowFunctionsClause.descriptionColumn -> Values.stringValue("Language function")
      ),
      Map(
        "function" -> Values.stringValue("zzz.func3"),
        "aggr" -> Values.FALSE,
        ShowFunctionsClause.isBuiltInColumn -> Values.FALSE,
        ShowFunctionsClause.descriptionColumn -> Values.stringValue("User-defined non-aggregating function")
      ),
      Map(
        "function" -> Values.stringValue("zzz.zz.func4"),
        "aggr" -> Values.TRUE,
        ShowFunctionsClause.isBuiltInColumn -> Values.FALSE,
        ShowFunctionsClause.descriptionColumn -> Values.stringValue("User-defined aggregating function")
      )
    ))
  }

  // Need a FunctionInformation for the language functions and can't reach the actual implementation
  private case class TestLanguageFunction(
    name: String,
    description: String,
    signature: String,
    category: String,
    aggregating: Boolean,
    output: String,
    arguments: List[InputInformation],
    deprecated: Boolean = false
  ) extends FunctionInformation {
    override def getFunctionName: String = name

    override def getDescription: String = description

    override def getSignature: String = signature

    override def getCategory: String = category

    override def isAggregationFunction: java.lang.Boolean = aggregating

    override def isDeprecated: java.lang.Boolean = deprecated

    override def returnType(): String = output

    override def inputSignature(): java.util.List[InputInformation] = arguments.asJava
  }
}
