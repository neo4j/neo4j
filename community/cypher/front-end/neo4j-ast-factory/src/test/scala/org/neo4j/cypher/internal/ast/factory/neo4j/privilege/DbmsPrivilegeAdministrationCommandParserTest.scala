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
package org.neo4j.cypher.internal.ast.factory.neo4j.privilege

import org.neo4j.cypher.internal.ast
import org.neo4j.cypher.internal.ast.factory.neo4j.AdministrationCommandParserTestBase

class DbmsPrivilegeAdministrationCommandParserTest extends AdministrationCommandParserTestBase {

  def privilegeTests(command: String, preposition: String, privilegeFunc: dbmsPrivilegeFunc): Unit = {
    Seq(
      ("CREATE ROLE", ast.CreateRoleAction),
      ("RENAME ROLE", ast.RenameRoleAction),
      ("DROP ROLE", ast.DropRoleAction),
      ("SHOW ROLE", ast.ShowRoleAction),
      ("ASSIGN ROLE", ast.AssignRoleAction),
      ("REMOVE ROLE", ast.RemoveRoleAction),
      ("ROLE MANAGEMENT", ast.AllRoleActions),
      ("CREATE USER", ast.CreateUserAction),
      ("RENAME USER", ast.RenameUserAction),
      ("DROP USER", ast.DropUserAction),
      ("SHOW USER", ast.ShowUserAction),
      ("SET PASSWORD", ast.SetPasswordsAction),
      ("SET PASSWORDS", ast.SetPasswordsAction),
      ("SET USER STATUS", ast.SetUserStatusAction),
      ("SET USER HOME DATABASE", ast.SetUserHomeDatabaseAction),
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
      case (privilege: String, action: ast.DbmsAction) =>

        test(s"$command $privilege ON DBMS $preposition role") {
          yields(privilegeFunc(action, Seq(literalRole)))
        }

        test(s"$command $privilege ON DBMS $preposition role1, $$role2") {
          yields(privilegeFunc(action, Seq(literalRole1, paramRole2)))
        }

        test(s"$command $privilege ON DBMS $preposition `r:ole`") {
          yields(privilegeFunc(action, Seq(literalRColonOle)))
        }

        test(s"dbmsPrivilegeParsingErrors$command $privilege $preposition") {
          assertFails(s"$command $privilege ON DATABASE $preposition role")
          assertFails(s"$command $privilege ON HOME DATABASE $preposition role")
          assertFails(s"$command $privilege DBMS $preposition role")
          assertFails(s"$command $privilege ON $preposition role")
          assertFails(s"$command $privilege ON DBMS $preposition r:ole")
          assertFails(s"$command $privilege ON DBMS $preposition")
          assertFails(s"$command $privilege ON DBMS")
        }
    }

    // The tests below needs to be outside the loop since ALL [PRIVILEGES] ON DATABASE is a valid (but different) command

    test(s"$command ALL ON DBMS $preposition $$role") {
      yields(privilegeFunc(ast.AllDbmsAction, Seq(paramRole)))
    }

    test(s"$command ALL ON DBMS $preposition role1, role2") {
      yields(privilegeFunc(ast.AllDbmsAction, Seq(literalRole1, literalRole2)))
    }

    test(s"$command ALL PRIVILEGES ON DBMS $preposition role") {
      yields(privilegeFunc(ast.AllDbmsAction, Seq(literalRole)))
    }

    test(s"$command ALL PRIVILEGES ON DBMS $preposition $$role1, role2") {
      yields(privilegeFunc(ast.AllDbmsAction, Seq(paramRole1, literalRole2)))
    }
  }
}
