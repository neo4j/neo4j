/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.ast

import org.neo4j.cypher.internal.ast.semantics.SemanticCheckContext
import org.neo4j.cypher.internal.ast.semantics.SemanticCheckResult
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.scalatest.prop.TableDrivenPropertyChecks._

class AdministrationCommandTest extends CypherFunSuite {

  // Privilege command tests
  private val p = InputPosition(0, 0, 0)
  private val initialState = SemanticState.clean

  test("it should not be possible to administer privileges pertaining to an unassignable action") {

    val privilegeManagementActions =
      Table("PrivilegeManagementActions", AssignImmutablePrivilegeAction, RemoveImmutablePrivilegeAction)

    val grant = (pma: PrivilegeManagementAction) =>
      new GrantPrivilege(
        DbmsPrivilege(pma)(p),
        false,
        Some(DatabaseResource()(p)),
        List(AllQualifier()(p)),
        Seq(Left("role1"))
      )(p)

    val deny = (pma: PrivilegeManagementAction) =>
      new DenyPrivilege(
        DbmsPrivilege(pma)(p),
        false,
        Some(DatabaseResource()(p)),
        List(AllQualifier()(p)),
        Seq(Left("role1"))
      )(p)

    val revoke = (pma: PrivilegeManagementAction, rt: RevokeType) =>
      new RevokePrivilege(
        DbmsPrivilege(pma)(p),
        false,
        Some(DatabaseResource()(p)),
        List(AllQualifier()(p)),
        Seq(Left("role1")),
        rt
      )(p)

    val revokeBoth = revoke(_, RevokeBothType()(p))
    val revokeGrant = revoke(_, RevokeGrantType()(p))
    val revokeDeny = revoke(_, RevokeDenyType()(p))
    val privilegeCommands = Table("PrivilegeCommand", grant, deny, revokeBoth, revokeGrant, revokeDeny)

    forAll(privilegeManagementActions) { pma =>
      forAll(privilegeCommands) { privilegeCommand =>
        val privilege = privilegeCommand(pma)
        privilege.semanticCheck.run(initialState, SemanticCheckContext.default) shouldBe SemanticCheckResult
          .error(initialState, s"`GRANT`, `DENY` and `REVOKE` are not supported for `${pma.name}`", p)
      }
    }
  }

  test("it should not be possible to administer privileges on the default graph") {
    val privilege = new GrantPrivilege(
      GraphPrivilege(AllGraphAction, DefaultGraphScope()(p))(p),
      false,
      Some(DatabaseResource()(p)),
      List(AllQualifier()(p)),
      Seq(Left("role1"))
    )(p)

    privilege.semanticCheck.run(initialState, SemanticCheckContext.default) shouldBe SemanticCheckResult
      .error(initialState, "`ON DEFAULT GRAPH` is not supported. Use `ON HOME GRAPH` instead.", p)
  }

  test("it should not be possible to administer privileges on the default database") {
    val privilege = new GrantPrivilege(
      DatabasePrivilege(AllConstraintActions, DefaultDatabaseScope()(p))(p),
      false,
      Some(DatabaseResource()(p)),
      List(AllQualifier()(p)),
      Seq(Left("role1"))
    )(p)

    privilege.semanticCheck.run(initialState, SemanticCheckContext.default) shouldBe SemanticCheckResult
      .error(initialState, "`ON DEFAULT DATABASE` is not supported. Use `ON HOME DATABASE` instead.", p)
  }

}
