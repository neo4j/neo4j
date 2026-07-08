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
package org.neo4j.cypher.internal.ast.factory.ddl.privilege

import org.neo4j.cypher.internal.ast.AdministrationAction
import org.neo4j.cypher.internal.ast.AllAliasManagementActions
import org.neo4j.cypher.internal.ast.AllAuthRuleActions
import org.neo4j.cypher.internal.ast.AllDatabaseManagementActions
import org.neo4j.cypher.internal.ast.AllDbmsAction
import org.neo4j.cypher.internal.ast.AllPrivilegeActions
import org.neo4j.cypher.internal.ast.AllRoleActions
import org.neo4j.cypher.internal.ast.AllSecretManagementActions
import org.neo4j.cypher.internal.ast.AllUserActions
import org.neo4j.cypher.internal.ast.AllUserMetadataActions
import org.neo4j.cypher.internal.ast.AlterAliasAction
import org.neo4j.cypher.internal.ast.AlterAuthRuleAction
import org.neo4j.cypher.internal.ast.AlterCompositeDatabaseAction
import org.neo4j.cypher.internal.ast.AlterDatabaseAction
import org.neo4j.cypher.internal.ast.AlterUserAction
import org.neo4j.cypher.internal.ast.AssignPrivilegeAction
import org.neo4j.cypher.internal.ast.AssignRoleAction
import org.neo4j.cypher.internal.ast.AuthRuleManagementAction
import org.neo4j.cypher.internal.ast.CompositeDatabaseManagementActions
import org.neo4j.cypher.internal.ast.CreateAliasAction
import org.neo4j.cypher.internal.ast.CreateAuthRuleAction
import org.neo4j.cypher.internal.ast.CreateCompositeDatabaseAction
import org.neo4j.cypher.internal.ast.CreateDatabaseAction
import org.neo4j.cypher.internal.ast.CreateRoleAction
import org.neo4j.cypher.internal.ast.CreateUserAction
import org.neo4j.cypher.internal.ast.DropAliasAction
import org.neo4j.cypher.internal.ast.DropAuthRuleAction
import org.neo4j.cypher.internal.ast.DropCompositeDatabaseAction
import org.neo4j.cypher.internal.ast.DropDatabaseAction
import org.neo4j.cypher.internal.ast.DropRoleAction
import org.neo4j.cypher.internal.ast.DropUserAction
import org.neo4j.cypher.internal.ast.RemovePrivilegeAction
import org.neo4j.cypher.internal.ast.RemoveRoleAction
import org.neo4j.cypher.internal.ast.RenameAuthRuleAction
import org.neo4j.cypher.internal.ast.RenameRoleAction
import org.neo4j.cypher.internal.ast.RenameUserAction
import org.neo4j.cypher.internal.ast.SecretManagementAction
import org.neo4j.cypher.internal.ast.ServerManagementAction
import org.neo4j.cypher.internal.ast.SetAuthAction
import org.neo4j.cypher.internal.ast.SetDatabaseAccessAction
import org.neo4j.cypher.internal.ast.SetDatabaseDefaultLanguageAction
import org.neo4j.cypher.internal.ast.SetPasswordsAction
import org.neo4j.cypher.internal.ast.SetUserHomeDatabaseAction
import org.neo4j.cypher.internal.ast.SetUserMetadataAction
import org.neo4j.cypher.internal.ast.SetUserStatusAction
import org.neo4j.cypher.internal.ast.ShowAliasAction
import org.neo4j.cypher.internal.ast.ShowAuthRuleAction
import org.neo4j.cypher.internal.ast.ShowPrivilegeAction
import org.neo4j.cypher.internal.ast.ShowRoleAction
import org.neo4j.cypher.internal.ast.ShowSecretsAction
import org.neo4j.cypher.internal.ast.ShowServerAction
import org.neo4j.cypher.internal.ast.ShowUserAction
import org.neo4j.cypher.internal.ast.ShowUserMetadataAction
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.UserMetadataManagementAction
import org.neo4j.cypher.internal.ast.WriteSecretsAction
import org.neo4j.cypher.internal.ast.factory.ddl.AdministrationAndSchemaCommandParserTestBase
import org.neo4j.cypher.internal.ast.prettifier.Prettifier.maybeImmutable
import org.neo4j.cypher.internal.ast.test.util.AstParsing.Cypher25
import org.neo4j.cypher.internal.ast.test.util.AstParsing.Cypher5

class DbmsPrivilegeAdministrationCommandParserTest extends AdministrationAndSchemaCommandParserTestBase {

  // Impersonate, execute and read secrets privileges have their own files and are not in this list

  private val dbmsActionPrivileges: Seq[(String, Boolean => AdministrationAction, Boolean)] =
    Seq(
      ("CREATE ROLE", CreateRoleAction),
      ("RENAME ROLE", RenameRoleAction),
      ("DROP ROLE", DropRoleAction),
      ("SHOW ROLE", ShowRoleAction),
      ("ASSIGN ROLE", AssignRoleAction),
      ("REMOVE ROLE", RemoveRoleAction),
      ("ROLE MANAGEMENT", AllRoleActions),
      ("CREATE USER", CreateUserAction),
      ("RENAME USER", RenameUserAction),
      ("DROP USER", DropUserAction),
      ("SHOW USER", ShowUserAction),
      ("SHOW USER METADATA", ShowUserMetadataAction),
      ("SET PASSWORD", SetPasswordsAction),
      ("SET PASSWORDS", SetPasswordsAction),
      ("SET AUTH", SetAuthAction),
      ("SET USER STATUS", SetUserStatusAction),
      ("SET USER HOME DATABASE", SetUserHomeDatabaseAction),
      ("SET USER METADATA", SetUserMetadataAction),
      ("ALTER USER", AlterUserAction),
      ("USER MANAGEMENT", AllUserActions),
      ("USER METADATA MANAGEMENT", AllUserMetadataActions),
      ("SHOW AUTH RULE", ShowAuthRuleAction),
      ("CREATE AUTH RULE", CreateAuthRuleAction),
      ("DROP AUTH RULE", DropAuthRuleAction),
      ("ALTER AUTH RULE", AlterAuthRuleAction),
      ("RENAME AUTH RULE", RenameAuthRuleAction),
      ("AUTH RULE MANAGEMENT", AllAuthRuleActions),
      ("CREATE DATABASE", CreateDatabaseAction),
      ("DROP DATABASE", DropDatabaseAction),
      ("DATABASE MANAGEMENT", AllDatabaseManagementActions),
      ("SHOW PRIVILEGE", ShowPrivilegeAction),
      ("ASSIGN PRIVILEGE", AssignPrivilegeAction),
      ("REMOVE PRIVILEGE", RemovePrivilegeAction),
      ("PRIVILEGE MANAGEMENT", AllPrivilegeActions),
      ("SECRETS MANAGEMENT", AllSecretManagementActions),
      ("WRITE SECRETS", WriteSecretsAction),
      ("SHOW SECRETS", ShowSecretsAction),
      ("SHOW SERVER", ShowServerAction),
      ("SHOW SERVERS", ShowServerAction),
      ("SERVER MANAGEMENT", ServerManagementAction),
      ("COMPOSITE DATABASE MANAGEMENT", CompositeDatabaseManagementActions),
      ("CREATE COMPOSITE DATABASE", CreateCompositeDatabaseAction),
      ("DROP COMPOSITE DATABASE", DropCompositeDatabaseAction),
      ("ALIAS MANAGEMENT", AllAliasManagementActions),
      ("CREATE ALIAS", CreateAliasAction),
      ("DROP ALIAS", DropAliasAction),
      ("ALTER ALIAS", AlterAliasAction),
      ("SHOW ALIAS", ShowAliasAction)
    ).map { case (privilege: String, action: AdministrationAction) =>
      (privilege, (_: Boolean) => action, supportedInCypher5(action))
    }

  private val databaseActionPrivileges: Seq[(String, Boolean => AdministrationAction, Boolean)] =
    Seq[(String, Boolean => AdministrationAction)](
      ("ALTER DATABASE", AlterDatabaseAction.apply),
      ("SET DATABASE ACCESS", SetDatabaseAccessAction.apply),
      ("SET DATABASE DEFAULT LANGUAGE", SetDatabaseDefaultLanguageAction.apply),
      ("ALTER COMPOSITE DATABASE", AlterCompositeDatabaseAction.apply)
    ).map { case (privilege, action) => (privilege, action, true) }

  private def supportedInCypher5(action: AdministrationAction): Boolean =
    action match {
      case _: AuthRuleManagementAction     => false
      case _: UserMetadataManagementAction => false
      case _: SecretManagementAction       => false
      case _                               => true
    }

  def privilegeTests(command: String, preposition: String, privilegeFunc: adminPrivilegeFunc): Unit = {
    Seq[Immutable](true, false).foreach {
      immutable =>
        val immutableString = maybeImmutable(immutable)
        val offset = command.length + immutableString.length + 1
        // not unnecessary as `action: (Boolean => AdministrationAction)` fails to compile without the ()
        // noinspection ScalaUnnecessaryParentheses
        (dbmsActionPrivileges ++ databaseActionPrivileges).foreach {
          case (privilege: String, action: (Boolean => AdministrationAction), supportedInCypher5) =>
            test(s"$command$immutableString $privilege ON DBMS $preposition role") {
              parsesIn[Statements] {
                case Cypher5 if supportedInCypher5 =>
                  _.toAst(Statements(Seq(privilegeFunc(action(true), Seq(literalRole), immutable)(pos))))
                case Cypher5 =>
                  _.withSyntaxErrorContaining(command)
                case _ =>
                  _.toAst(Statements(Seq(privilegeFunc(action(false), Seq(literalRole), immutable)(pos))))
              }
            }

            test(s"$command$immutableString $privilege ON DBMS $preposition role1, $$role2") {
              parsesIn[Statements] {
                case Cypher5 if supportedInCypher5 =>
                  _.toAst(Statements(Seq(privilegeFunc(action(true), Seq(literalRole1, paramRole2), immutable)(pos))))
                case Cypher5 =>
                  _.withSyntaxErrorContaining(command)
                case _ =>
                  _.toAst(Statements(Seq(privilegeFunc(action(false), Seq(literalRole1, paramRole2), immutable)(pos))))
              }
            }

            test(s"$command$immutableString $privilege ON DBMS $preposition `r:ole`") {
              parsesIn[Statements] {
                case Cypher5 if supportedInCypher5 =>
                  _.toAst(Statements(Seq(privilegeFunc(action(true), Seq(literalRColonOle), immutable)(pos))))
                case Cypher5 =>
                  _.withSyntaxErrorContaining(command)
                case _ =>
                  _.toAst(Statements(Seq(privilegeFunc(action(false), Seq(literalRColonOle), immutable)(pos))))
              }
            }

            test(s"$command$immutableString $privilege ON DBMS $preposition r:ole") {
              val offset = command.length + immutableString.length + 12 + privilege.length + preposition.length
              failsParsing[Statements].in {
                case Cypher5 if !supportedInCypher5 =>
                  _.withSyntaxErrorContaining(command)
                case _ => _.withSyntaxErrorContaining(
                    s"""Invalid input ':': expected ',' or <EOF> (line 1, column ${offset + 1} (offset: $offset))"""
                  )
              }
            }

            test(s"$command$immutableString $privilege ON DBMS $preposition") {
              val offset = command.length + immutableString.length + 10 + privilege.length + preposition.length
              failsParsing[Statements].in {
                case Cypher5 if !supportedInCypher5 =>
                  _.withSyntaxErrorContaining(command)
                case _ => _.withSyntaxErrorContaining(
                    s"""Invalid input '': expected a parameter or an identifier (line 1, column ${offset + 1} (offset: $offset))"""
                  )
              }
            }

            test(s"$command$immutableString $privilege ON DBMS") {
              val offset = command.length + immutableString.length + 9 + privilege.length
              failsParsing[Statements].in {
                case Cypher5 if !supportedInCypher5 =>
                  _.withSyntaxErrorContaining(command)
                case _ => _.withSyntaxErrorContaining(
                    s"""Invalid input '': expected '$preposition' (line 1, column ${offset + 1} (offset: $offset))"""
                  )
              }
            }
        }

        // These tests are only reasonable for dbms privileges which do not have an ON DATABASE variant
        dbmsActionPrivileges.foreach {
          case (privilege: String, _, supportedInCypher5) =>
            test(s"$command$immutableString $privilege ON DATABASE $preposition role") {
              failsParsing[Statements].in {
                case Cypher5 if !supportedInCypher5 =>
                  _.withSyntaxErrorContaining(command)
                case _ => _.withSyntaxErrorContaining("""Invalid input 'DATABASE': expected 'DBMS'""")
              }
            }

            test(s"$command$immutableString $privilege ON HOME DATABASE $preposition role") {
              failsParsing[Statements].in {
                case Cypher5 if !supportedInCypher5 =>
                  _.withSyntaxErrorContaining(command)
                case _ => _.withSyntaxErrorContaining("""Invalid input 'HOME': expected 'DBMS'""")
              }
            }

            test(s"$command$immutableString $privilege DBMS $preposition role") {
              val offset = command.length + immutableString.length + 2 + privilege.length
              failsParsing[Statements].in {
                case Cypher5 if !supportedInCypher5 =>
                  _.withSyntaxErrorContaining(command)
                case parserVersion => _.withSyntaxErrorContaining((parserVersion, command, immutable, privilege) match {
                    // this case looks like granting/revoking a role named MANAGEMENT to/from a user
                    case (_, "GRANT", false, "ROLE MANAGEMENT") | (_, "REVOKE", false, "ROLE MANAGEMENT") =>
                      s"Invalid input 'DBMS': expected ',', 'ON DBMS' or '$preposition'"
                    // Cypher 25 adds the SHOW USER METADATA privilege, which could be a valid alternative
                    case (Cypher25, _, _, "SHOW USER") =>
                      s"Invalid input 'DBMS': expected 'ON DBMS' or 'METADATA' (line 1, column ${offset + 1} (offset: $offset))"
                    case _ =>
                      s"Invalid input 'DBMS': expected 'ON DBMS' (line 1, column ${offset + 1} (offset: $offset))"
                  })
              }
            }

            test(s"$command$immutableString $privilege ON $preposition role") {
              val offset = command.length + immutableString.length + 5 + privilege.length
              failsParsing[Statements].in {
                case Cypher5 if !supportedInCypher5 =>
                  _.withSyntaxErrorContaining(command)
                case _ => _.withSyntaxErrorContaining(
                    s"""Invalid input '$preposition': expected 'DBMS' (line 1, column ${offset + 1} (offset: $offset))"""
                  )
              }
            }
        }

        // These tests are only reasonable for dbms privileges which do have an ON DATABASE variant
        databaseActionPrivileges.foreach {
          case (privilege: String, _, _) =>

            test(s"$command$immutableString $privilege ON $preposition role") {
              val offset = command.length + immutableString.length + 5 + privilege.length
              failsParsing[Statements].in {
                case Cypher5 => _.withSyntaxErrorContaining(
                    s"""Invalid input '$preposition': expected 'DBMS' (line 1, column ${offset + 1} (offset: $offset))"""
                  )
                case _ => _.withSyntaxErrorContaining(
                    s"""Invalid input '$preposition': expected 'DATABASE', 'HOME DATABASE', 'DATABASES' or 'DBMS' (line 1, column ${offset + 1} (offset: $offset))"""
                  )
              }
            }
        }

        // The tests below needs to be outside the loop since ALL [PRIVILEGES] ON DATABASE is a valid (but different) command

        test(s"$command$immutableString ALL ON DBMS $preposition $$role") {
          parsesTo[Statements](privilegeFunc(AllDbmsAction, Seq(paramRole), immutable)(pos))
        }

        test(s"$command$immutableString ALL ON DBMS $preposition role1, role2") {
          parsesTo[Statements](privilegeFunc(AllDbmsAction, Seq(literalRole1, literalRole2), immutable)(pos))
        }

        test(s"$command$immutableString ALL PRIVILEGES ON DBMS $preposition role") {
          parsesTo[Statements](privilegeFunc(AllDbmsAction, Seq(literalRole), immutable)(pos))
        }

        test(s"$command$immutableString ALL PRIVILEGES ON DBMS $preposition $$role1, role2") {
          parsesTo[Statements](privilegeFunc(AllDbmsAction, Seq(paramRole1, literalRole2), immutable)(pos))
        }

        test(s"$command$immutableString ALL DBMS PRIVILEGES ON DBMS $preposition role") {
          parsesTo[Statements](privilegeFunc(AllDbmsAction, Seq(literalRole), immutable)(pos))
        }

        test(s"$command$immutableString ALL DBMS PRIVILEGES ON DBMS $preposition `r:ole`, $$role2") {
          parsesTo[Statements](privilegeFunc(AllDbmsAction, Seq(literalRColonOle, paramRole2), immutable)(pos))
        }

        test(s"$command$immutableString ALL DBMS PRIVILEGES ON DATABASE $preposition role") {
          failsParsing[Statements].withSyntaxErrorContaining(
            s"""Invalid input 'DATABASE': expected "DBMS" (line 1, column ${offset + 24} (offset: ${offset + 23}))"""
          )
        }

        test(s"$command$immutableString ALL DBMS PRIVILEGES ON HOME DATABASE $preposition role") {
          failsParsing[Statements].withSyntaxErrorContaining(
            s"""Invalid input 'HOME': expected "DBMS" (line 1, column ${offset + 24} (offset: ${offset + 23}))"""
          )
        }

        test(s"$command$immutableString ALL DBMS PRIVILEGES DBMS $preposition role") {
          failsParsing[Statements].withSyntaxErrorContaining(
            s"""Invalid input 'DBMS': expected 'ON' (line 1, column ${offset + 21} (offset: ${offset + 20}))"""
          )
        }

        test(s"$command$immutableString ALL DBMS PRIVILEGES $preposition") {
          failsParsing[Statements].withSyntaxErrorContaining(
            s"""Invalid input '$preposition': expected 'ON' (line 1, column ${offset + 21} (offset: ${offset + 20}))"""
          )
        }

        test(s"$command$immutableString ALL DBMS PRIVILEGES ON $preposition") {
          failsParsing[Statements].in {
            case Cypher5 => _.withSyntaxErrorContaining(
                s"""Invalid input '$preposition': expected 'DATABASE', 'DATABASES', 'DBMS', 'DEFAULT', 'GRAPH', 'GRAPHS' or 'HOME' (line 1, column ${offset + 24} (offset: ${offset + 23}))"""
              )
            case _ => _.withSyntaxErrorContaining(
                s"""Invalid input '$preposition': expected 'DATABASE', 'DATABASES', 'DBMS', 'GRAPH', 'GRAPHS' or 'HOME' (line 1, column ${offset + 24} (offset: ${offset + 23}))"""
              )
          }
        }

        test(s"$command$immutableString ALL DBMS PRIVILEGES ON DBMS $preposition r:ole") {
          val finalOffset = offset + 30 + preposition.length
          failsParsing[Statements].withSyntaxErrorContaining(
            s"""Invalid input ':': expected ',' or <EOF> (line 1, column ${finalOffset + 1} (offset: $finalOffset))"""
          )
        }

        test(s"$command$immutableString ALL DBMS PRIVILEGES ON DBMS $preposition") {
          val finalOffset = offset + 28 + preposition.length
          failsParsing[Statements].withSyntaxErrorContaining(
            s"""Invalid input '': expected a parameter or an identifier (line 1, column ${finalOffset + 1} (offset: $finalOffset))"""
          )
        }

        test(s"$command$immutableString ALL DBMS PRIVILEGES ON DBMS") {
          failsParsing[Statements].withSyntaxErrorContaining(
            s"""Invalid input '': expected '$preposition' (line 1, column ${offset + 28} (offset: ${offset + 27}))"""
          )
        }

        // Tests for invalid alias management privileges (database keyword in wrong place)

        test(s"$command$immutableString DATABASE ALIAS MANAGEMENT ON DBMS $preposition role") {
          failsParsing[Statements].withSyntaxErrorContaining(
            s"""Invalid input 'ALIAS': expected 'MANAGEMENT' (line 1, column ${offset + 10} (offset: ${offset + 9}))"""
          )
        }

        test(s"$command$immutableString CREATE DATABASE ALIAS ON DBMS $preposition role") {
          failsParsing[Statements].withSyntaxErrorContaining(
            s"""Invalid input 'ALIAS': expected 'ON DBMS' (line 1, column ${offset + 17} (offset: ${offset + 16}))"""
          )
        }

        test(s"$command$immutableString DROP DATABASE ALIAS ON DBMS $preposition role") {
          failsParsing[Statements].withSyntaxErrorContaining(
            s"""Invalid input 'ALIAS': expected 'ON DBMS' (line 1, column ${offset + 15} (offset: ${offset + 14}))"""
          )
        }

        test(s"$command$immutableString ALTER DATABASE ALIAS ON DBMS $preposition role") {
          failsParsing[Statements].in {
            case Cypher5 => _.withSyntaxErrorContaining(
                s"""Invalid input 'ALIAS': expected 'ON DBMS' (line 1, column ${offset + 16} (offset: ${offset + 15}))"""
              )
            case _ => _.withSyntaxErrorContaining(
                s"""Invalid input 'ALIAS': expected 'ON' (line 1, column ${offset + 16} (offset: ${offset + 15}))"""
              )
          }
        }

        test(s"$command$immutableString SHOW DATABASE ALIAS ON DBMS $preposition role") {
          failsParsing[Statements].in {
            case Cypher5 => _.withSyntaxErrorContaining(
                s"""Invalid input 'DATABASE': expected 'ALIAS', 'CONSTRAINT', 'CONSTRAINTS', 'INDEX', 'INDEXES', 'PRIVILEGE', 'ROLE', 'SERVER', 'SERVERS', 'SETTING', 'SETTINGS', 'TRANSACTION', 'TRANSACTIONS' or 'USER' (line 1, column ${offset + 6} (offset: ${offset + 5}))"""
              )
            case _ => _.withSyntaxErrorContaining(
                s"""Invalid input 'DATABASE': expected 'ALIAS', 'CONSTRAINT', 'CONSTRAINTS', 'INDEX', 'INDEXES', 'PRIVILEGE', 'ROLE', 'AUTH RULE', 'SECRETS', 'SERVER', 'SERVERS', 'SETTING', 'SETTINGS', 'TRANSACTION', 'TRANSACTIONS' or 'USER' (line 1, column ${offset + 6} (offset: ${offset + 5}))"""
              )
          }
        }

        // Other invalid tests

        test(s"$command$immutableString ALTER DATABASE DBMS $preposition role") {
          failsParsing[Statements].in {
            case Cypher5 => _.withSyntaxErrorContaining(
                s"Invalid input 'DBMS': expected 'ON DBMS' (line 1, column ${offset + 16} (offset: ${offset + 15}))"
              )
            case _ => _.withSyntaxErrorContaining(
                s"Invalid input 'DBMS': expected 'ON' (line 1, column ${offset + 16} (offset: ${offset + 15}))"
              )
          }
        }

        test(s"$command$immutableString SET DATABASE ACCESS DBMS $preposition role") {
          failsParsing[Statements].withSyntaxErrorContaining(
            s"Invalid input 'DBMS': expected 'ON DBMS' (line 1, column ${offset + 21} (offset: ${offset + 20}))"
          )
        }

        test(s"$command$immutableString SET DATABASE DEFAULT LANGUAGE DBMS $preposition role") {
          failsParsing[Statements].withSyntaxErrorContaining(
            s"Invalid input 'DBMS': expected 'ON DBMS' (line 1, column ${offset + 31} (offset: ${offset + 30}))"
          )
        }

        test(s"$command$immutableString ALTER COMPOSITE DATABASE DBMS $preposition role") {
          failsParsing[Statements].in {
            case Cypher5 => _.withSyntaxErrorContaining(
                s"Invalid input 'DBMS': expected 'ON DBMS' (line 1, column ${offset + 26} (offset: ${offset + 25}))"
              )
            case _ => _.withSyntaxErrorContaining(
                s"Invalid input 'DBMS': expected 'ON' (line 1, column ${offset + 26} (offset: ${offset + 25}))"
              )
          }
        }

        test(s"$command$immutableString SET AUTHS ON DBMS $preposition role") {
          testName should notParse[Statements].withSyntaxErrorContaining(
            s"""Invalid input 'AUTHS': expected 'AUTH', 'DATABASE', 'LABEL', 'PASSWORD', 'PASSWORDS', 'PROPERTY' or 'USER' (line 1, column ${offset + 5} (offset: ${offset + 4}))"""
          )
        }

        test(s"$command$immutableString SET DEFAULT LANGUAGE ON DBMS $preposition role") {
          testName should notParse[Statements].withSyntaxErrorContaining(
            s"""Invalid input 'DEFAULT': expected 'AUTH', 'DATABASE', 'LABEL', 'PASSWORD', 'PASSWORDS', 'PROPERTY' or 'USER' (line 1, column ${offset + 5} (offset: ${offset + 4}))"""
          )
        }
    }
  }
}
