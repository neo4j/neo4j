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
import org.neo4j.cypher.internal.ast.CurrentUser
import org.neo4j.cypher.internal.ast.User
import org.neo4j.graphdb.Result
import org.neo4j.graphdb.security.AuthorizationViolationException
import org.neo4j.internal.kernel.api.procs.DefaultParameterValue
import org.neo4j.internal.kernel.api.procs.FieldSignature
import org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTAny
import org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTDate
import org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTInteger
import org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTMap
import org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTString
import org.neo4j.internal.kernel.api.security.AdminActionOnResource
import org.neo4j.internal.kernel.api.security.AuthSubject
import org.neo4j.internal.kernel.api.security.PermissionState
import org.neo4j.internal.kernel.api.security.PrivilegeAction.SHOW_USER
import org.neo4j.internal.kernel.api.security.Segment
import org.neo4j.kernel.impl.query.FunctionInformation.InputInformation
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.VirtualValues

import scala.jdk.CollectionConverters.SeqHasAsJava
import scala.jdk.CollectionConverters.SetHasAsJava

class ShowProcFuncCommandHelperTest extends ShowCommandTestBase {

  /* Sets up privileges:
   * GRANT EXECUTE * TO role1, role5, role8
   * GRANT EXECUTE *1 TO role10
   * GRANT EXECUTE *3 TO role11
   * DENY EXECUTE *2 TO role5
   * DENY EXECUTE *1 TO role12
   *
   * GRANT BOOSTED * TO role9
   * GRANT BOOSTED *3 TO role2
   * DENY BOOSTED *1 TO role3
   * DENY BOOSTED *2 TO role8
   *
   * GRANT ALL DBMS TO role3, role5
   * DENY ALL DBMS TO role6
   *
   * GRANT EXECUTE ADMIN TO role4
   * DENY EXECUTE ADMIN TO role7
   */
  private val privileges = ShowProcFuncCommandHelper.Privileges(
    executePrivileges = List(
      Map("access" -> "GRANTED", "segment" -> "(*)", "roles" -> List("role1", "role5", "role8").asJava),
      Map("access" -> "GRANTED", "segment" -> "(*1)", "roles" -> List("role10").asJava),
      Map("access" -> "GRANTED", "segment" -> "(*3)", "roles" -> List("role11").asJava),
      Map("access" -> "DENIED", "segment" -> "(*2)", "roles" -> List("role5").asJava),
      Map("access" -> "DENIED", "segment" -> "(*1)", "roles" -> List("role12").asJava)
    ),
    boostedExecutePrivileges = List(
      Map("access" -> "GRANTED", "segment" -> "(*)", "roles" -> List("role9").asJava),
      Map("access" -> "GRANTED", "segment" -> "(*3)", "roles" -> List("role2").asJava),
      Map("access" -> "DENIED", "segment" -> "(*1)", "roles" -> List("role3").asJava),
      Map("access" -> "DENIED", "segment" -> "(*2)", "roles" -> List("role8").asJava)
    ),
    allDbmsPrivileges = List(
      Map("access" -> "GRANTED", "roles" -> List("role3", "role5").asJava),
      Map("access" -> "DENIED", "roles" -> List("role6").asJava)
    ),
    adminExecutePrivileges = List(
      Map("access" -> "GRANTED", "roles" -> List("role4").asJava),
      Map("access" -> "DENIED", "roles" -> List("role7").asJava)
    )
  )

  private def handlePrivilegeQueries(query: String): Result = query match {
    case "SHOW ALL PRIVILEGES YIELD * WHERE action='execute' AND segment STARTS WITH $seg RETURN access, segment, collect(role) as roles" =>
      MockResult(List(
        Map("access" -> "GRANTED", "segment" -> "(*)", "roles" -> List("role1", "role5", "role8").asJava),
        Map("access" -> "GRANTED", "segment" -> "(*1)", "roles" -> List("role10").asJava),
        Map("access" -> "GRANTED", "segment" -> "(*3)", "roles" -> List("role11").asJava),
        Map("access" -> "DENIED", "segment" -> "(*2)", "roles" -> List("role5").asJava),
        Map("access" -> "DENIED", "segment" -> "(*1)", "roles" -> List("role12").asJava)
      ))
    case "SHOW ALL PRIVILEGES YIELD * WHERE action STARTS WITH 'execute_boosted' AND segment STARTS WITH $seg RETURN access, segment, collect(role) as roles" =>
      MockResult(List(
        Map("access" -> "GRANTED", "segment" -> "(*)", "roles" -> List("role9").asJava),
        Map("access" -> "GRANTED", "segment" -> "(*3)", "roles" -> List("role2").asJava),
        Map("access" -> "DENIED", "segment" -> "(*1)", "roles" -> List("role3").asJava),
        Map("access" -> "DENIED", "segment" -> "(*2)", "roles" -> List("role8").asJava)
      ))
    case "SHOW ALL PRIVILEGES YIELD * WHERE action='execute_admin' RETURN access, collect(role) as roles" =>
      MockResult(List(
        Map("access" -> "GRANTED", "roles" -> List("role4").asJava),
        Map("access" -> "DENIED", "roles" -> List("role7").asJava)
      ))
    case "SHOW ALL PRIVILEGES YIELD * WHERE action IN ['admin', 'dbms_actions'] RETURN access, collect(role) as roles" =>
      MockResult(List(
        Map("access" -> "GRANTED", "roles" -> List("role3", "role5").asJava),
        Map("access" -> "DENIED", "roles" -> List("role6").asJava)
      ))
    case _ => MockResult()
  }

  test("`getSignatureValues` and `fieldDescriptions` should return empty lists for empty list") {
    // Given
    val fields = List.empty[FieldSignature].asJava

    // When
    val signatureResult = ShowProcFuncCommandHelper.getSignatureValues(fields)

    // Then
    signatureResult should be(List.empty)

    // When
    val descriptionResult = ShowProcFuncCommandHelper.fieldDescriptions(signatureResult)

    // Then
    descriptionResult should be(VirtualValues.EMPTY_LIST)
  }

  test("`getSignatureValues` and `fieldDescriptions` should return correct result for single input field") {
    // Given
    val field = FieldSignature.inputField("input", NTDate)

    // When
    val signatureResult = ShowProcFuncCommandHelper.getSignatureValues(List(field).asJava)

    // Then
    signatureResult should be(List(
      new InputInformation(
        "input",
        NTDate.toString,
        field.toString,
        false,
        java.util.Optional.empty[String]()
      )
    ))

    // When
    val descriptionResult = ShowProcFuncCommandHelper.fieldDescriptions(signatureResult)

    // Then
    descriptionResult should be(VirtualValues.fromList(
      List(argumentAndReturnDescriptionMaps("input", field.toString, NTDate.toString)).asJava
    ))
  }

  test("`getSignatureValues` and `fieldDescriptions` should return correct result with default values") {
    // Given
    val defaultValue = new DefaultParameterValue("defaultValue", NTString)
    val field = FieldSignature.inputField("input", NTString, defaultValue)

    // When
    val signatureResult = ShowProcFuncCommandHelper.getSignatureValues(List(field).asJava)

    // Then
    signatureResult should be(List(new InputInformation(
      "input",
      NTString.toString,
      field.toString,
      false,
      java.util.Optional.of[String](defaultValue.toString)
    )))

    // When
    val descriptionResult = ShowProcFuncCommandHelper.fieldDescriptions(signatureResult)

    // Then
    descriptionResult should be(VirtualValues.fromList(List(
      argumentAndReturnDescriptionMaps("input", field.toString, NTString.toString, default = defaultValue.toString)
    ).asJava))
  }

  test("`getSignatureValues` and `fieldDescriptions` should return correct result for multiple output fields") {
    // Given
    val field1 = FieldSignature.outputField("mapOutput", NTMap)
    val field2 = FieldSignature.outputField("anyOutput", NTAny)

    // When
    val signatureResult = ShowProcFuncCommandHelper.getSignatureValues(List(field1, field2).asJava)

    // Then
    signatureResult should be(List(
      new InputInformation(
        "mapOutput",
        NTMap.toString,
        field1.toString,
        false,
        java.util.Optional.empty[String]()
      ),
      new InputInformation(
        "anyOutput",
        NTAny.toString,
        field2.toString,
        false,
        java.util.Optional.empty[String]()
      )
    ))

    // When
    val descriptionResult = ShowProcFuncCommandHelper.fieldDescriptions(signatureResult)

    // Then
    descriptionResult should be(VirtualValues.fromList(List(
      argumentAndReturnDescriptionMaps("mapOutput", field1.toString, NTMap.toString),
      argumentAndReturnDescriptionMaps("anyOutput", field2.toString, NTAny.toString)
    ).asJava))
  }

  test(
    "`getSignatureValues` and `fieldDescriptions` should return correct result for input fields with or without descriptions"
  ) {
    // Given
    val inputField1 =
      FieldSignature.inputField("dateInput", NTDate, false, false, "This is a description about dateInput.")
    val inputField2 = FieldSignature.inputField("integerInput", NTInteger)

    // When
    val signatureResult = ShowProcFuncCommandHelper.getSignatureValues(List(inputField1, inputField2).asJava)

    // Then
    signatureResult should be(List(
      new InputInformation(
        "dateInput",
        NTDate.toString,
        "This is a description about dateInput.",
        false,
        java.util.Optional.empty[String]()
      ),
      new InputInformation(
        "integerInput",
        NTInteger.toString,
        inputField2.toString,
        false,
        java.util.Optional.empty[String]()
      )
    ))

    // When
    val descriptionResult = ShowProcFuncCommandHelper.fieldDescriptions(signatureResult)

    // Then
    descriptionResult should be(VirtualValues.fromList(List(
      argumentAndReturnDescriptionMaps("dateInput", "This is a description about dateInput.", NTDate.toString),
      argumentAndReturnDescriptionMaps("integerInput", inputField2.toString, NTInteger.toString)
    ).asJava))
  }

  test(
    "`getSignatureValues` and `fieldDescriptions` should return correct result for output fields with or without descriptions"
  ) {
    // Given
    val outputField1 = FieldSignature.outputField("mapOutput", NTMap)
    val outputField2 = FieldSignature.outputField("anyOutput", NTAny, false, "This is a description about anyOutput.")

    // When
    val signatureResult = ShowProcFuncCommandHelper.getSignatureValues(List(outputField1, outputField2).asJava)

    // Then
    signatureResult should be(List(
      new InputInformation(
        "mapOutput",
        NTMap.toString,
        outputField1.toString,
        false,
        java.util.Optional.empty[String]()
      ),
      new InputInformation(
        "anyOutput",
        NTAny.toString,
        "This is a description about anyOutput.",
        false,
        java.util.Optional.empty[String]()
      )
    ))

    // When
    val descriptionResult = ShowProcFuncCommandHelper.fieldDescriptions(signatureResult)

    // Then
    descriptionResult should be(VirtualValues.fromList(List(
      argumentAndReturnDescriptionMaps("mapOutput", outputField1.toString, NTMap.toString),
      argumentAndReturnDescriptionMaps("anyOutput", "This is a description about anyOutput.", NTAny.toString)
    ).asJava))
  }

  test("`getSignatureValues` and `fieldDescriptions` should return correct result with deprecated fields") {
    // Given
    val field = FieldSignature.outputField("output", NTString, true)

    // When
    val signatureResult = ShowProcFuncCommandHelper.getSignatureValues(List(field).asJava)

    // Then
    signatureResult should be(List(new InputInformation(
      "output",
      NTString.toString,
      field.toString,
      true,
      java.util.Optional.empty[String]()
    )))

    // When
    val descriptionResult = ShowProcFuncCommandHelper.fieldDescriptions(signatureResult)

    // Then
    descriptionResult should be(VirtualValues.fromList(List(
      argumentAndReturnDescriptionMaps("output", field.toString, NTString.toString, deprecated = true)
    ).asJava))
  }

  test("`roleValues` should return empty lists for empty sets") {
    // When
    val (executeRoles, boostedRoles) = ShowProcFuncCommandHelper.roleValues(Set.empty, Set.empty)

    // Then
    executeRoles should be(VirtualValues.EMPTY_LIST)
    boostedRoles should be(VirtualValues.EMPTY_LIST)
  }

  test("`roleValues` should return correct and ordered result") {
    // Given
    val executeRoles = Set("role1", "role3", "role2")
    val boostedRoles = Set("role2", "role4")

    // When
    val (executeRolesResult, boostedRolesResult) = ShowProcFuncCommandHelper.roleValues(executeRoles, boostedRoles)

    // Then
    executeRolesResult should be(VirtualValues.fromList(List(
      Values.stringValue("role1").asInstanceOf[AnyValue],
      Values.stringValue("role2").asInstanceOf[AnyValue],
      Values.stringValue("role3").asInstanceOf[AnyValue]
    ).asJava))
    boostedRolesResult should be(VirtualValues.fromList(List(
      Values.stringValue("role2").asInstanceOf[AnyValue],
      Values.stringValue("role4").asInstanceOf[AnyValue]
    ).asJava))
  }

  test("`roles` should return correct result for the given admin procedure") {
    // Given:
    // proc2 is admin
    // the privileges above
    val userRoles = List(
      // roles, allowedExecute
      (Set("role3"), true),
      (Set("role4"), true),
      (Set("role1"), false), // missing grant EXECUTE BOOSTED
      (Set("role2"), false), // missing grants
      (Set("role10"), false), // missing grants
      (Set("role11"), false), // missing grants
      (Set("role12"), false), // missing grants
      (Set("role9"), false), // missing grant EXECUTE
      (Set("role5"), false), // denied EXECUTE
      (Set("role8"), false), // denied EXECUTE BOOSTED
      (Set("role6"), false), // denied ALL ON DBMS
      (Set("role7"), false), // denied EXECUTE ADMIN
      (Set("role1", "role9"), true),
      (Set("role3", "role5"), false), // deny EXECUTE from role5 takes precedence
      (Set("role4", "role8"), false), // deny EXECUTE BOOSTED from role8 takes precedence
      (Set("role4", "role6"), false), // deny ALL ON DBMS from role6 takes precedence
      (Set("role1", "role9", "role7"), false) // deny EXECUTE ADMIN from role7 takes precedence
    )

    userRoles.foreach { case (givenRoles, expectedExecute) =>
      withClue(s"Given roles $givenRoles:") {
        // When
        val (executeRoles, boostedRoles, allowedExecute) = ShowProcFuncCommandHelper.roles(
          name = "proc2",
          isAdmin = true,
          privileges = privileges,
          userRoles = givenRoles
        )

        // Then
        executeRoles should be(Set("role3", "role4"))
        boostedRoles should be(Set("role3", "role4", "role5", "role9"))
        allowedExecute should be(expectedExecute)
      }
    }
  }

  test("`roles` should return correct result for the given non-admin procedure/function") {
    // Given:
    // non-admin procedures/functions: proc1, proc2, proc3
    // the privileges above
    val userRoles = List(
      // roles, allowedExecute - proc1, allowedExecute - proc2, allowedExecute - proc3
      (Set("role1"), true, true, true),
      (Set("role3"), true, true, true), // denied EXECUTE BOOSTED does not affect execution of non-admin
      (Set("role8"), true, true, true), // denied EXECUTE BOOSTED does not affect execution of non-admin
      (Set("role5"), true, false, true), // denied EXECUTE proc2
      (Set("role2"), false, false, false), // missing EXECUTE
      (Set("role4"), false, false, false), // only handles admin
      (Set("role6"), false, false, false), // denied ALL ON DBMS
      (Set("role7"), false, false, false), // only handles admin
      (Set("role9"), false, false, false), // missing EXECUTE
      (Set("role10"), true, false, false), // only EXECUTE proc1
      (Set("role11"), false, false, true), // only EXECUTE proc3
      (Set("role10", "role11"), true, false, true), // missing EXECUTE proc2
      (Set("role3", "role5"), true, false, true), // deny EXECUTE from role5 takes precedence
      (Set("role1", "role6"), false, false, false), // deny ALL ON DBMS from role6 takes precedence
      (Set("role10", "role11", "role12"), false, false, true) // denied EXECUTE proc1, missing EXECUTE proc2
    )

    userRoles.foreach { case (givenRoles, expectedExecuteProc1, expectedExecuteProc2, expectedExecuteProc3) =>
      List(
        (
          "proc1",
          Set("role1", "role3", "role5", "role8", "role10"),
          Set("role5", "role9"),
          expectedExecuteProc1
        ),
        (
          "proc2",
          Set("role1", "role3", "role8"),
          Set("role3", "role5", "role9"),
          expectedExecuteProc2
        ),
        (
          "proc3",
          Set("role1", "role3", "role5", "role8", "role11"),
          Set("role2", "role3", "role5", "role9"),
          expectedExecuteProc3
        )
      ).foreach { case (proc, expectedRoles, expectedBoostedRoles, expectedExecute) =>
        withClue(s"Given roles $givenRoles and procedure $proc:") {
          // When
          val (executeRoles, boostedRoles, allowedExecute) = ShowProcFuncCommandHelper.roles(
            name = proc,
            isAdmin = false,
            privileges = privileges,
            userRoles = givenRoles
          )

          // Then
          executeRoles should be(expectedRoles)
          boostedRoles should be(expectedBoostedRoles)
          allowedExecute should be(expectedExecute)
        }
      }
    }
  }

  test("`getPrivileges` should return empty privileges when no privileges exists") {
    // Given
    mockSetupProcFunc()
    when(systemTx.execute(any())).thenReturn(MockResult())
    when(systemTx.execute(any(), any())).thenReturn(MockResult())

    val emptyPrivileges = ShowProcFuncCommandHelper.Privileges(List.empty, List.empty, List.empty, List.empty)

    // When
    val resultFunction = ShowProcFuncCommandHelper.getPrivileges(systemGraph, "FUNCTION")

    // Then
    resultFunction should be(emptyPrivileges)

    // When
    val resultProcedure = ShowProcFuncCommandHelper.getPrivileges(systemGraph, "PROCEDURE")

    // Then
    resultProcedure should be(emptyPrivileges)
  }

  test("`getPrivileges` should return correct privileges for functions") {
    // Given
    mockSetupProcFunc()
    when(systemTx.execute(any())).thenAnswer(invocation => handlePrivilegeQueries(invocation.getArgument(0)))
    when(systemTx.execute(any(), any())).thenAnswer(invocation => handlePrivilegeQueries(invocation.getArgument(0)))

    // When
    val result = ShowProcFuncCommandHelper.getPrivileges(systemGraph, "FUNCTION")

    // Then
    val expectedPrivileges = privileges.copy(adminExecutePrivileges = List.empty)
    result should be(expectedPrivileges)
  }

  test("`getPrivileges` should return correct privileges for procedures") {
    // Given
    mockSetupProcFunc()
    when(systemTx.execute(any())).thenAnswer(invocation => handlePrivilegeQueries(invocation.getArgument(0)))
    when(systemTx.execute(any(), any())).thenAnswer(invocation => handlePrivilegeQueries(invocation.getArgument(0)))

    // When
    val result = ShowProcFuncCommandHelper.getPrivileges(systemGraph, "PROCEDURE")

    // Then
    result should be(privileges)
  }

  test("`getRolesForExecutableByUser` should get exception when missing `SHOW USER` privilege") {
    // Given: Missing SHOW USER
    mockSetupProcFunc()
    when(securityContext.allowsAdminAction(any())).thenAnswer(_.getArgument[AdminActionOnResource](0) match {
      case a: AdminActionOnResource
        if a.matches(new AdminActionOnResource(SHOW_USER, AdminActionOnResource.DatabaseScope.ALL, Segment.ALL)) =>
        PermissionState.NOT_GRANTED
      case _ => PermissionState.EXPLICIT_GRANT
    })

    // Then
    the[AuthorizationViolationException] thrownBy {
      ShowProcFuncCommandHelper.getRolesForExecutableByUser(
        securityContext,
        securityHandler,
        systemGraph,
        Some(User("otherUser")),
        "SHOW FUNCTIONS"
      )
    } should have message "Permission not granted for SHOW FUNCTIONS, requires SHOW USER privilege. " +
      "Try executing SHOW USER PRIVILEGES to determine the missing privileges. " +
      "In case of missing privileges, they need to be granted (See GRANT)."
  }

  test("`getRolesForExecutableByUser` should get exception when denied `SHOW USER` privilege") {
    // Given: Deny SHOW USER
    mockSetupProcFunc()
    when(securityContext.allowsAdminAction(any())).thenAnswer(_.getArgument[AdminActionOnResource](0) match {
      case a: AdminActionOnResource
        if a.matches(new AdminActionOnResource(SHOW_USER, AdminActionOnResource.DatabaseScope.ALL, Segment.ALL)) =>
        PermissionState.EXPLICIT_DENY
      case _ => PermissionState.EXPLICIT_GRANT
    })

    // Then
    the[AuthorizationViolationException] thrownBy {
      ShowProcFuncCommandHelper.getRolesForExecutableByUser(
        securityContext,
        securityHandler,
        systemGraph,
        Some(User("otherUser")),
        "SHOW PROCEDURES"
      )
    } should have message "Permission denied for SHOW PROCEDURES, requires SHOW USER privilege. " +
      "Try executing SHOW USER PRIVILEGES to determine the denied privileges. " +
      "In case of denied privileges, they need to be revoked (See REVOKE) and granted."
  }

  test("`getRolesForExecutableByUser` should return correct result for currentUser and AUTH_DISABLED") {
    // Given
    mockSetupProcFunc()
    when(securityContext.subject()).thenReturn(AuthSubject.AUTH_DISABLED)

    // When
    val (roles, execute) = ShowProcFuncCommandHelper.getRolesForExecutableByUser(
      securityContext,
      securityHandler,
      systemGraph,
      Some(CurrentUser),
      "command"
    )

    // Then
    roles should be(Set.empty)
    execute should be(true)
  }

  test("`getRolesForExecutableByUser` should return correct result for no user filtering") {
    // Given
    mockSetupProcFunc()

    // When
    val (roles, execute) = ShowProcFuncCommandHelper.getRolesForExecutableByUser(
      securityContext,
      securityHandler,
      systemGraph,
      None,
      "command"
    )

    // Then
    roles should be(Set.empty)
    execute should be(false)
  }

  test("`getRolesForExecutableByUser` should return correct result for current user filtering") {
    // Given
    mockSetupProcFunc()
    val username = "my_user"
    val user = mock[AuthSubject]
    when(user.hasUsername(username)).thenReturn(true)
    when(securityContext.subject()).thenReturn(user)
    when(securityContext.roles()).thenReturn(Set("role1", "role9").asJava)

    // When
    val (roles, execute) = ShowProcFuncCommandHelper.getRolesForExecutableByUser(
      securityContext,
      securityHandler,
      systemGraph,
      Some(CurrentUser),
      "command"
    )

    // Then
    roles should be(Set("role1", "role9"))
    execute should be(false)
  }

  test("`getRolesForExecutableByUser` should return correct result for other user filtering on missing user") {
    // Given
    mockSetupProcFunc()
    val user = mock[AuthSubject]
    val missingUser = "missing_user"
    when(user.hasUsername(missingUser)).thenReturn(false)
    when(securityContext.subject()).thenReturn(user)

    when(systemTx.execute(any())).thenReturn(MockResult())
    when(systemTx.execute(any(), any())).thenReturn(MockResult())

    // When
    val (roles, execute) = ShowProcFuncCommandHelper.getRolesForExecutableByUser(
      securityContext,
      securityHandler,
      systemGraph,
      Some(User(missingUser)),
      "command"
    )

    // Then
    roles should be(Set.empty)
    execute should be(false)
  }

  test("`getRolesForExecutableByUser` should return correct result for other user filtering") {
    def handleUserQuery(query: String): Result = query match {
      case "SHOW USERS YIELD user, roles WHERE user = $name RETURN roles" =>
        MockResult(List(Map("roles" -> List("role1", "role9").asJava)))
      case _ => MockResult()
    }

    // Given
    mockSetupProcFunc()
    val user = mock[AuthSubject]
    val otherUser = "other_user"
    when(user.hasUsername(otherUser)).thenReturn(false)
    when(securityContext.subject()).thenReturn(user)

    when(systemTx.execute(any())).thenAnswer(invocation => handleUserQuery(invocation.getArgument(0)))
    when(systemTx.execute(any(), any())).thenAnswer(invocation => handleUserQuery(invocation.getArgument(0)))

    // When
    val (roles, execute) = ShowProcFuncCommandHelper.getRolesForExecutableByUser(
      securityContext,
      securityHandler,
      systemGraph,
      Some(User(otherUser)),
      "command"
    )

    // Then
    roles should be(Set("role1", "role9"))
    execute should be(false)
  }

  // Test inner class Privileges

  test("`grantedExecuteRoles` should return the roles that have GRANT EXECUTE the given procedure/function") {
    // When
    val resultProc1 = privileges.grantedExecuteRoles("proc1")
    val resultProc2 = privileges.grantedExecuteRoles("proc2")
    val resultProc3 = privileges.grantedExecuteRoles("proc3")

    // Then
    resultProc1 should be(Set("role1", "role3", "role5", "role8", "role10"))
    resultProc2 should be(Set("role1", "role3", "role5", "role8"))
    resultProc3 should be(Set("role1", "role3", "role5", "role8", "role11"))
  }

  test("`deniedExecuteRoles` should return the roles that have DENY EXECUTE the given procedure/function") {
    // When
    val resultProc1 = privileges.deniedExecuteRoles("proc1")
    val resultProc2 = privileges.deniedExecuteRoles("proc2")
    val resultProc3 = privileges.deniedExecuteRoles("proc3")

    // Then
    resultProc1 should be(Set("role6", "role12"))
    resultProc2 should be(Set("role5", "role6"))
    resultProc3 should be(Set("role6"))
  }

  test(
    "`grantedBoostedExecuteRoles` should return the roles that have GRANT EXECUTE BOOSTED the given procedure/function"
  ) {
    // When
    val resultProc1 = privileges.grantedBoostedExecuteRoles("proc1")
    val resultProc2 = privileges.grantedBoostedExecuteRoles("proc2")
    val resultProc3 = privileges.grantedBoostedExecuteRoles("proc3")

    // Then
    resultProc1 should be(Set("role3", "role5", "role9"))
    resultProc2 should be(Set("role3", "role5", "role9"))
    resultProc3 should be(Set("role2", "role3", "role5", "role9"))
  }

  test(
    "`deniedBoostedExecuteRoles` should return the roles that have DENY EXECUTE BOOSTED the given procedure/function"
  ) {
    // When
    val resultProc1 = privileges.deniedBoostedExecuteRoles("proc1")
    val resultProc2 = privileges.deniedBoostedExecuteRoles("proc2")
    val resultProc3 = privileges.deniedBoostedExecuteRoles("proc3")

    // Then
    resultProc1 should be(Set("role3", "role6"))
    resultProc2 should be(Set("role6", "role8"))
    resultProc3 should be(Set("role6"))
  }

  test("`grantedAdminExecuteRoles` should return the roles that have GRANT EXECUTE ADMIN") {
    // When
    val resultProc = privileges.grantedAdminExecuteRoles

    // Then
    resultProc should be(Set("role4"))
  }

  test("`deniedAdminExecuteRoles` should return the roles that have DENY EXECUTE ADMIN") {
    // When
    val resultProc = privileges.deniedAdminExecuteRoles

    // Then
    resultProc should be(Set("role7"))
  }
}
