/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.parser.privilege

import org.neo4j.cypher.internal.ast
import org.neo4j.cypher.internal.parser.AdministrationCommandParserTestBase

class DbmsPrivilegeAdministrationCommandParserTest extends AdministrationCommandParserTestBase {

  def privilegeTests(command: String, preposition: String, privilegeFunc: dbmsPrivilegeFunc): Unit = {
    Seq(
      ("CREATE ROLE", ast.CreateRoleAction),
      ("DROP ROLE", ast.DropRoleAction),
      ("SHOW ROLE", ast.ShowRoleAction),
      ("ASSIGN ROLE", ast.AssignRoleAction),
      ("REMOVE ROLE", ast.RemoveRoleAction),
      ("ROLE MANAGEMENT", ast.AllRoleActions),
      ("CREATE USER", ast.CreateUserAction),
      ("DROP USER", ast.DropUserAction),
      ("SHOW USER", ast.ShowUserAction),
      ("SET PASSWORD", ast.SetPasswordsAction),
      ("SET PASSWORDS", ast.SetPasswordsAction),
      ("SET USER STATUS", ast.SetUserStatusAction),
      ("ALTER USER", ast.AlterUserAction),
      ("USER MANAGEMENT", ast.AllUserActions),
      ("CREATE DATABASE", ast.CreateDatabaseAction),
      ("DROP DATABASE", ast.DropDatabaseAction),
      ("DATABASE MANAGEMENT", ast.AllDatabaseManagementActions),
      ("SHOW PRIVILEGE", ast.ShowPrivilegeAction),
      ("ASSIGN PRIVILEGE", ast.AssignPrivilegeAction),
      ("REMOVE PRIVILEGE", ast.RemovePrivilegeAction),
      ("PRIVILEGE MANAGEMENT", ast.AllPrivilegeActions),
      ("ALL DBMS PRIVILEGES", ast.AllDbmsAction)
    ).foreach {
      case (privilege: String, action: ast.AdminAction) =>

        test(s"$command $privilege ON DBMS $preposition role") {
          yields(privilegeFunc(action, Seq(literal("role"))))
        }

        test(s"$command $privilege ON DBMS $preposition role1, $$role2") {
          yields(privilegeFunc(action, Seq(literal("role1"), param("role2"))))
        }

        test(s"$command $privilege ON DBMS $preposition `r:ole`") {
          yields(privilegeFunc(action, Seq(literal("r:ole"))))
        }

        test(s"dbmsPrivilegeParsingErrors$command $privilege $preposition") {
          assertFails(s"$command $privilege ON DATABASE $preposition role")
          assertFails(s"$command $privilege ON DEFAULT DATABASE $preposition role")
          assertFails(s"$command $privilege DBMS $preposition role")
          assertFails(s"$command $privilege ON $preposition role")
          assertFails(s"$command $privilege ON DBMS $preposition r:ole")
          assertFails(s"$command $privilege ON DBMS $preposition")
          assertFails(s"$command $privilege ON DBMS")
        }
    }

    // The tests below needs to be outside the loop since ALL [PRIVILEGES] ON DATABASE is a valid (but different) command

    test(s"$command ALL ON DBMS $preposition $$role") {
      yields(privilegeFunc(ast.AllDbmsAction, Seq(param("role"))))
    }

    test(s"$command ALL ON DBMS $preposition role1, role2") {
      yields(privilegeFunc(ast.AllDbmsAction, Seq(literal("role1"), literal("role2"))))
    }

    test(s"$command ALL PRIVILEGES ON DBMS $preposition role") {
      yields(privilegeFunc(ast.AllDbmsAction, Seq(literal("role"))))
    }

    test(s"$command ALL PRIVILEGES ON DBMS $preposition $$role1, role2") {
      yields(privilegeFunc(ast.AllDbmsAction, Seq(param("role1"), literal("role2"))))
    }
  }
}
