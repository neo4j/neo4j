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
import org.neo4j.cypher.internal.ast.factory.neo4j.AdministrationAndSchemaCommandParserTestBase

class DbmsPrivilegeAdministrationCommandParserTest extends AdministrationAndSchemaCommandParserTestBase {

  def privilegeTests(command: String, preposition: String, privilegeFunc: dbmsPrivilegeFunc): Unit = {
    val offset = command.length + 1

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
      ("ALTER DATABASE", ast.AlterDatabaseAction),
      ("SET DATABASE ACCESS", ast.SetDatabaseAccessAction),
      ("DATABASE MANAGEMENT", ast.AllDatabaseManagementActions),
      ("SHOW PRIVILEGE", ast.ShowPrivilegeAction),
      ("ASSIGN PRIVILEGE", ast.AssignPrivilegeAction),
      ("REMOVE PRIVILEGE", ast.RemovePrivilegeAction),
      ("PRIVILEGE MANAGEMENT", ast.AllPrivilegeActions)
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

        test(s"$command $privilege ON DATABASE $preposition role") {
          val offset = command.length + 5 + privilege.length
          assertFailsWithMessage(testName, s"""Invalid input 'DATABASE': expected "DBMS" (line 1, column ${offset + 1} (offset: $offset))""")
        }

        test(s"$command $privilege ON HOME DATABASE $preposition role") {
          val offset = command.length + 5 + privilege.length
          assertFailsWithMessage(testName, s"""Invalid input 'HOME': expected "DBMS" (line 1, column ${offset + 1} (offset: $offset))""")
        }

        test(s"$command $privilege DBMS $preposition role") {
          val offset = command.length + 2 + privilege.length
          val expected = (command, privilege) match {
            // this case looks like granting/revoking a role named MANAGEMENT to/from a user
            case ("GRANT", "ROLE MANAGEMENT") => s"""Invalid input 'DBMS': expected "," or "TO" (line 1, column ${offset + 1} (offset: $offset))"""
            case ("REVOKE", "ROLE MANAGEMENT") => s"""Invalid input 'DBMS': expected "," or "FROM" (line 1, column ${offset + 1} (offset: $offset))"""
            case _ => s"""Invalid input 'DBMS': expected "ON" (line 1, column ${offset + 1} (offset: $offset))"""
          }
          assertFailsWithMessage(testName, expected)
        }

        test(s"$command $privilege ON $preposition role") {
          val offset = command.length + 5 + privilege.length
          assertFailsWithMessage(testName, s"""Invalid input '$preposition': expected "DBMS" (line 1, column ${offset + 1} (offset: $offset))""")
        }

        test(s"$command $privilege ON DBMS $preposition r:ole") {
          val offset = command.length + 12 + privilege.length + preposition.length
          assertFailsWithMessage(testName, s"""Invalid input ':': expected "," or <EOF> (line 1, column ${offset + 1} (offset: $offset))""")
        }

        test(s"$command $privilege ON DBMS $preposition") {
          val offset = command.length + 10 + privilege.length + preposition.length
          assertFailsWithMessage(testName, s"""Invalid input '': expected a parameter or an identifier (line 1, column ${offset + 1} (offset: $offset))""")
        }

        test(s"$command $privilege ON DBMS") {
          val offset = command.length + 9 + privilege.length
          assertFailsWithMessage(testName, s"""Invalid input '': expected "$preposition" (line 1, column ${offset + 1} (offset: $offset))""")
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

    test(s"$command ALL DBMS PRIVILEGES ON DBMS $preposition role") {
      yields(privilegeFunc(ast.AllDbmsAction, Seq(literalRole)))
    }

    test(s"$command ALL DBMS PRIVILEGES ON DBMS $preposition `r:ole`, $$role2") {
      yields(privilegeFunc(ast.AllDbmsAction, Seq(literalRColonOle, paramRole2)))
    }

    test(s"$command ALL DBMS PRIVILEGES ON DATABASE $preposition role") {
      assertFailsWithMessage(testName,
        s"""Invalid input 'DATABASE': expected "DBMS" (line 1, column ${offset+24} (offset: ${offset+23}))""")
    }

    test(s"$command ALL DBMS PRIVILEGES ON HOME DATABASE $preposition role") {
      assertFailsWithMessage(testName,
        s"""Invalid input 'HOME': expected "DBMS" (line 1, column ${offset+24} (offset: ${offset+23}))""")
    }

    test(s"$command ALL DBMS PRIVILEGES DBMS $preposition role") {
      assertFailsWithMessage(testName,
        s"""Invalid input 'DBMS': expected "ON" (line 1, column ${offset+21} (offset: ${offset+20}))""")
    }

    test(s"$command ALL DBMS PRIVILEGES $preposition") {
      assertFailsWithMessage(testName,
        s"""Invalid input '$preposition': expected "ON" (line 1, column ${offset+21} (offset: ${offset+20}))""")
    }

    test(s"$command ALL DBMS PRIVILEGES ON $preposition") {
      assertFailsWithMessage(testName,
        s"""Invalid input '$preposition': expected
           |  "DATABASE"
           |  "DATABASES"
           |  "DBMS"
           |  "DEFAULT"
           |  "GRAPH"
           |  "GRAPHS"
           |  "HOME" (line 1, column ${offset+24} (offset: ${offset+23}))""".stripMargin)
    }

    test(s"$command ALL DBMS PRIVILEGES ON DBMS $preposition r:ole") {
      val finalOffset = offset+30+preposition.length
      assertFailsWithMessage(testName,
        s"""Invalid input ':': expected "," or <EOF> (line 1, column ${finalOffset+1} (offset: $finalOffset))""")
    }

    test(s"$command ALL DBMS PRIVILEGES ON DBMS $preposition") {
      val finalOffset = offset+28+preposition.length
      assertFailsWithMessage(testName,
        s"""Invalid input '': expected a parameter or an identifier (line 1, column ${finalOffset+1} (offset: $finalOffset))""")
    }

    test(s"$command ALL DBMS PRIVILEGES ON DBMS") {
      assertFailsWithMessage(testName,
        s"""Invalid input '': expected "$preposition" (line 1, column ${offset+28} (offset: ${offset+27}))""")
    }
  }
}
