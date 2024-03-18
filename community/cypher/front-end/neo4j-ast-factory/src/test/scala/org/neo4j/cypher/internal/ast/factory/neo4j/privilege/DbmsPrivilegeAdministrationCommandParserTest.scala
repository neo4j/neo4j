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
package org.neo4j.cypher.internal.ast.factory.neo4j.privilege

import org.neo4j.cypher.internal.ast
import org.neo4j.cypher.internal.ast.factory.neo4j.AdministrationAndSchemaCommandParserTestBase

class DbmsPrivilegeAdministrationCommandParserTest extends AdministrationAndSchemaCommandParserTestBase {

  // Impersonate and execute privileges have their own files and are not in this list

  def privilegeTests(command: String, preposition: String, privilegeFunc: dbmsPrivilegeFunc): Unit = {
    Seq[Immutable](true, false).foreach {
      immutable =>
        val immutableString = immutableOrEmpty(immutable)
        val offset = command.length + immutableString.length + 1
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
          ("PRIVILEGE MANAGEMENT", ast.AllPrivilegeActions),
          ("SHOW SERVER", ast.ShowServerAction),
          ("SHOW SERVERS", ast.ShowServerAction),
          ("SERVER MANAGEMENT", ast.ServerManagementAction),
          ("COMPOSITE DATABASE MANAGEMENT", ast.CompositeDatabaseManagementActions),
          ("CREATE COMPOSITE DATABASE", ast.CreateCompositeDatabaseAction),
          ("DROP COMPOSITE DATABASE", ast.DropCompositeDatabaseAction),
          ("ALIAS MANAGEMENT", ast.AllAliasManagementActions),
          ("CREATE ALIAS", ast.CreateAliasAction),
          ("DROP ALIAS", ast.DropAliasAction),
          ("ALTER ALIAS", ast.AlterAliasAction),
          ("SHOW ALIAS", ast.ShowAliasAction)
        ).foreach {
          case (privilege: String, action: ast.DbmsAction) =>
            test(s"$command$immutableString $privilege ON DBMS $preposition role") {
              yields[ast.Statements](privilegeFunc(action, Seq(literalRole), immutable))
            }

            test(s"$command$immutableString $privilege ON DBMS $preposition role1, $$role2") {
              yields[ast.Statements](privilegeFunc(action, Seq(literalRole1, paramRole2), immutable))
            }

            test(s"$command$immutableString $privilege ON DBMS $preposition `r:ole`") {
              yields[ast.Statements](privilegeFunc(action, Seq(literalRColonOle), immutable))
            }

            test(s"$command$immutableString $privilege ON DATABASE $preposition role") {
              val offset = command.length + immutableString.length + 5 + privilege.length
              assertFailsWithMessage[ast.Statements](
                testName,
                s"""Invalid input 'DATABASE': expected "DBMS" (line 1, column ${offset + 1} (offset: $offset))"""
              )
            }

            test(s"$command$immutableString $privilege ON HOME DATABASE $preposition role") {
              val offset = command.length + immutableString.length + 5 + privilege.length
              assertFailsWithMessage[ast.Statements](
                testName,
                s"""Invalid input 'HOME': expected "DBMS" (line 1, column ${offset + 1} (offset: $offset))"""
              )
            }

            test(s"$command$immutableString $privilege DBMS $preposition role") {
              val offset = command.length + immutableString.length + 2 + privilege.length
              val expected = (command, immutable, privilege) match {
                // this case looks like granting/revoking a role named MANAGEMENT to/from a user
                case ("GRANT", false, "ROLE MANAGEMENT") =>
                  s"""Invalid input 'DBMS': expected "," or "TO" (line 1, column ${offset + 1} (offset: $offset))"""
                case ("REVOKE", false, "ROLE MANAGEMENT") =>
                  s"""Invalid input 'DBMS': expected "," or "FROM" (line 1, column ${offset + 1} (offset: $offset))"""
                case _ => s"""Invalid input 'DBMS': expected "ON" (line 1, column ${offset + 1} (offset: $offset))"""
              }
              assertFailsWithMessage[ast.Statements](testName, expected)
            }

            test(s"$command$immutableString $privilege ON $preposition role") {
              val offset = command.length + immutableString.length + 5 + privilege.length
              assertFailsWithMessage[ast.Statements](
                testName,
                s"""Invalid input '$preposition': expected "DBMS" (line 1, column ${offset + 1} (offset: $offset))"""
              )
            }

            test(s"$command$immutableString $privilege ON DBMS $preposition r:ole") {
              val offset = command.length + immutableString.length + 12 + privilege.length + preposition.length
              assertFailsWithMessage[ast.Statements](
                testName,
                s"""Invalid input ':': expected "," or <EOF> (line 1, column ${offset + 1} (offset: $offset))"""
              )
            }

            test(s"$command$immutableString $privilege ON DBMS $preposition") {
              val offset = command.length + immutableString.length + 10 + privilege.length + preposition.length
              assertFailsWithMessage[ast.Statements](
                testName,
                s"""Invalid input '': expected a parameter or an identifier (line 1, column ${offset + 1} (offset: $offset))"""
              )
            }

            test(s"$command$immutableString $privilege ON DBMS") {
              val offset = command.length + immutableString.length + 9 + privilege.length
              assertFailsWithMessage[ast.Statements](
                testName,
                s"""Invalid input '': expected "$preposition" (line 1, column ${offset + 1} (offset: $offset))"""
              )
            }
        }

        // The tests below needs to be outside the loop since ALL [PRIVILEGES] ON DATABASE is a valid (but different) command

        test(s"$command$immutableString ALL ON DBMS $preposition $$role") {
          yields[ast.Statements](privilegeFunc(ast.AllDbmsAction, Seq(paramRole), immutable))
        }

        test(s"$command$immutableString ALL ON DBMS $preposition role1, role2") {
          yields[ast.Statements](privilegeFunc(ast.AllDbmsAction, Seq(literalRole1, literalRole2), immutable))
        }

        test(s"$command$immutableString ALL PRIVILEGES ON DBMS $preposition role") {
          yields[ast.Statements](privilegeFunc(ast.AllDbmsAction, Seq(literalRole), immutable))
        }

        test(s"$command$immutableString ALL PRIVILEGES ON DBMS $preposition $$role1, role2") {
          yields[ast.Statements](privilegeFunc(ast.AllDbmsAction, Seq(paramRole1, literalRole2), immutable))
        }

        test(s"$command$immutableString ALL DBMS PRIVILEGES ON DBMS $preposition role") {
          yields[ast.Statements](privilegeFunc(ast.AllDbmsAction, Seq(literalRole), immutable))
        }

        test(s"$command$immutableString ALL DBMS PRIVILEGES ON DBMS $preposition `r:ole`, $$role2") {
          yields[ast.Statements](privilegeFunc(ast.AllDbmsAction, Seq(literalRColonOle, paramRole2), immutable))
        }

        test(s"$command$immutableString ALL DBMS PRIVILEGES ON DATABASE $preposition role") {
          assertFailsWithMessage[ast.Statements](
            testName,
            s"""Invalid input 'DATABASE': expected "DBMS" (line 1, column ${offset + 24} (offset: ${offset + 23}))"""
          )
        }

        test(s"$command$immutableString ALL DBMS PRIVILEGES ON HOME DATABASE $preposition role") {
          assertFailsWithMessage[ast.Statements](
            testName,
            s"""Invalid input 'HOME': expected "DBMS" (line 1, column ${offset + 24} (offset: ${offset + 23}))"""
          )
        }

        test(s"$command$immutableString ALL DBMS PRIVILEGES DBMS $preposition role") {
          assertFailsWithMessage[ast.Statements](
            testName,
            s"""Invalid input 'DBMS': expected "ON" (line 1, column ${offset + 21} (offset: ${offset + 20}))"""
          )
        }

        test(s"$command$immutableString ALL DBMS PRIVILEGES $preposition") {
          assertFailsWithMessage[ast.Statements](
            testName,
            s"""Invalid input '$preposition': expected "ON" (line 1, column ${offset + 21} (offset: ${offset + 20}))"""
          )
        }

        test(s"$command$immutableString ALL DBMS PRIVILEGES ON $preposition") {
          assertFailsWithMessage[ast.Statements](
            testName,
            s"""Invalid input '$preposition': expected
               |  "DATABASE"
               |  "DATABASES"
               |  "DBMS"
               |  "DEFAULT"
               |  "GRAPH"
               |  "GRAPHS"
               |  "HOME" (line 1, column ${offset + 24} (offset: ${offset + 23}))""".stripMargin
          )
        }

        test(s"$command$immutableString ALL DBMS PRIVILEGES ON DBMS $preposition r:ole") {
          val finalOffset = offset + 30 + preposition.length
          assertFailsWithMessage[ast.Statements](
            testName,
            s"""Invalid input ':': expected "," or <EOF> (line 1, column ${finalOffset + 1} (offset: $finalOffset))"""
          )
        }

        test(s"$command$immutableString ALL DBMS PRIVILEGES ON DBMS $preposition") {
          val finalOffset = offset + 28 + preposition.length
          assertFailsWithMessage[ast.Statements](
            testName,
            s"""Invalid input '': expected a parameter or an identifier (line 1, column ${finalOffset + 1} (offset: $finalOffset))"""
          )
        }

        test(s"$command$immutableString ALL DBMS PRIVILEGES ON DBMS") {
          assertFailsWithMessage[ast.Statements](
            testName,
            s"""Invalid input '': expected "$preposition" (line 1, column ${offset + 28} (offset: ${offset + 27}))"""
          )
        }

        // Tests for invalid alias management privileges (database keyword in wrong place)

        test(s"$command$immutableString DATABASE ALIAS MANAGEMENT ON DBMS $preposition role") {
          assertFailsWithMessage[ast.Statements](
            testName,
            s"""Invalid input 'ALIAS': expected "MANAGEMENT" (line 1, column ${offset + 10} (offset: ${offset + 9}))"""
          )
        }

        test(s"$command$immutableString CREATE DATABASE ALIAS ON DBMS $preposition role") {
          assertFailsWithMessage[ast.Statements](
            testName,
            s"""Invalid input 'ALIAS': expected "ON" (line 1, column ${offset + 17} (offset: ${offset + 16}))"""
          )
        }

        test(s"$command$immutableString DROP DATABASE ALIAS ON DBMS $preposition role") {
          assertFailsWithMessage[ast.Statements](
            testName,
            s"""Invalid input 'ALIAS': expected "ON" (line 1, column ${offset + 15} (offset: ${offset + 14}))"""
          )
        }

        test(s"$command$immutableString ALTER DATABASE ALIAS ON DBMS $preposition role") {
          assertFailsWithMessage[ast.Statements](
            testName,
            s"""Invalid input 'ALIAS': expected "ON" (line 1, column ${offset + 16} (offset: ${offset + 15}))"""
          )
        }

        test(s"$command$immutableString SHOW DATABASE ALIAS ON DBMS $preposition role") {
          assertFailsWithMessage[ast.Statements](
            testName,
            s"""Invalid input 'DATABASE': expected
               |  "ALIAS"
               |  "CONSTRAINT"
               |  "CONSTRAINTS"
               |  "INDEX"
               |  "INDEXES"
               |  "PRIVILEGE"
               |  "ROLE"
               |  "SERVER"
               |  "SERVERS"
               |  "SETTING"
               |  "SETTINGS"
               |  "TRANSACTION"
               |  "TRANSACTIONS"
               |  "USER" (line 1, column ${offset + 6} (offset: ${offset + 5}))""".stripMargin
          )
        }
    }
  }
}
