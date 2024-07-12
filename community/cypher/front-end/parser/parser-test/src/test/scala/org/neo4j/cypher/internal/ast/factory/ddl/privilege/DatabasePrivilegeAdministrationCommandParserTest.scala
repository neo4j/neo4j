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

import org.neo4j.cypher.internal.ast.AccessDatabaseAction
import org.neo4j.cypher.internal.ast.AllConstraintActions
import org.neo4j.cypher.internal.ast.AllDatabaseAction
import org.neo4j.cypher.internal.ast.AllDatabasesScope
import org.neo4j.cypher.internal.ast.AllIndexActions
import org.neo4j.cypher.internal.ast.AllTokenActions
import org.neo4j.cypher.internal.ast.AllTransactionActions
import org.neo4j.cypher.internal.ast.CreateConstraintAction
import org.neo4j.cypher.internal.ast.CreateIndexAction
import org.neo4j.cypher.internal.ast.CreateNodeLabelAction
import org.neo4j.cypher.internal.ast.CreatePropertyKeyAction
import org.neo4j.cypher.internal.ast.CreateRelationshipTypeAction
import org.neo4j.cypher.internal.ast.DatabaseAction
import org.neo4j.cypher.internal.ast.DefaultDatabaseScope
import org.neo4j.cypher.internal.ast.DropConstraintAction
import org.neo4j.cypher.internal.ast.DropIndexAction
import org.neo4j.cypher.internal.ast.HomeDatabaseScope
import org.neo4j.cypher.internal.ast.NamedDatabasesScope
import org.neo4j.cypher.internal.ast.ShowConstraintAction
import org.neo4j.cypher.internal.ast.ShowIndexAction
import org.neo4j.cypher.internal.ast.ShowTransactionAction
import org.neo4j.cypher.internal.ast.StartDatabaseAction
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.StopDatabaseAction
import org.neo4j.cypher.internal.ast.TerminateTransactionAction
import org.neo4j.cypher.internal.ast.UserAllQualifier
import org.neo4j.cypher.internal.ast.UserQualifier
import org.neo4j.cypher.internal.ast.factory.ddl.AdministrationAndSchemaCommandParserTestBase
import org.neo4j.cypher.internal.ast.test.util.AstParsing.Cypher5JavaCc

class DatabasePrivilegeAdministrationCommandParserTest extends AdministrationAndSchemaCommandParserTestBase {
  private val databaseScopeFoo = NamedDatabasesScope(Seq(literalFoo))(_)
  private val databaseScopeParamFoo = NamedDatabasesScope(Seq(namespacedParamFoo))(_)
  private val databaseScopeFooBar = NamedDatabasesScope(Seq(literalFoo, namespacedName("bar")))(_)
  private val databaseScopeFooParamBar = NamedDatabasesScope(Seq(literalFoo, stringParamName("bar")))(_)

  Seq(
    ("GRANT", "TO", grantDatabasePrivilege: databasePrivilegeFunc),
    ("DENY", "TO", denyDatabasePrivilege: databasePrivilegeFunc),
    ("REVOKE GRANT", "FROM", revokeGrantDatabasePrivilege: databasePrivilegeFunc),
    ("REVOKE DENY", "FROM", revokeDenyDatabasePrivilege: databasePrivilegeFunc),
    ("REVOKE", "FROM", revokeDatabasePrivilege: databasePrivilegeFunc)
  ).foreach {
    case (verb: String, preposition: String, privilegeFunc: databasePrivilegeFunc) =>
      Seq[Immutable](true, false).foreach {
        immutable =>
          val immutableString = immutableOrEmpty(immutable)
          Seq(
            ("ACCESS", AccessDatabaseAction),
            ("START", StartDatabaseAction),
            ("STOP", StopDatabaseAction),
            ("CREATE INDEX", CreateIndexAction),
            ("CREATE INDEXES", CreateIndexAction),
            ("DROP INDEX", DropIndexAction),
            ("DROP INDEXES", DropIndexAction),
            ("SHOW INDEX", ShowIndexAction),
            ("SHOW INDEXES", ShowIndexAction),
            ("INDEX", AllIndexActions),
            ("INDEXES", AllIndexActions),
            ("INDEX MANAGEMENT", AllIndexActions),
            ("INDEXES MANAGEMENT", AllIndexActions),
            ("CREATE CONSTRAINT", CreateConstraintAction),
            ("CREATE CONSTRAINTS", CreateConstraintAction),
            ("DROP CONSTRAINT", DropConstraintAction),
            ("DROP CONSTRAINTS", DropConstraintAction),
            ("SHOW CONSTRAINT", ShowConstraintAction),
            ("SHOW CONSTRAINTS", ShowConstraintAction),
            ("CONSTRAINT", AllConstraintActions),
            ("CONSTRAINTS", AllConstraintActions),
            ("CONSTRAINT MANAGEMENT", AllConstraintActions),
            ("CONSTRAINTS MANAGEMENT", AllConstraintActions),
            ("CREATE NEW LABEL", CreateNodeLabelAction),
            ("CREATE NEW LABELS", CreateNodeLabelAction),
            ("CREATE NEW NODE LABEL", CreateNodeLabelAction),
            ("CREATE NEW NODE LABELS", CreateNodeLabelAction),
            ("CREATE NEW TYPE", CreateRelationshipTypeAction),
            ("CREATE NEW TYPES", CreateRelationshipTypeAction),
            ("CREATE NEW RELATIONSHIP TYPE", CreateRelationshipTypeAction),
            ("CREATE NEW RELATIONSHIP TYPES", CreateRelationshipTypeAction),
            ("CREATE NEW NAME", CreatePropertyKeyAction),
            ("CREATE NEW NAMES", CreatePropertyKeyAction),
            ("CREATE NEW PROPERTY NAME", CreatePropertyKeyAction),
            ("CREATE NEW PROPERTY NAMES", CreatePropertyKeyAction),
            ("NAME", AllTokenActions),
            ("NAME MANAGEMENT", AllTokenActions),
            ("ALL", AllDatabaseAction),
            ("ALL PRIVILEGES", AllDatabaseAction),
            ("ALL DATABASE PRIVILEGES", AllDatabaseAction)
          ).foreach {
            case (privilege: String, action: DatabaseAction) =>
              test(s"$verb$immutableString $privilege ON DATABASE * $preposition $$role") {
                parsesTo[Statements](privilegeFunc(action, AllDatabasesScope() _, Seq(paramRole), immutable)(pos))
              }

              test(s"$verb$immutableString $privilege ON DATABASES * $preposition role") {
                parsesTo[Statements](privilegeFunc(action, AllDatabasesScope() _, Seq(literalRole), immutable)(pos))
              }

              test(s"$verb$immutableString $privilege ON DATABASE * $preposition role1, role2") {
                parsesTo[Statements](privilegeFunc(
                  action,
                  AllDatabasesScope() _,
                  Seq(literalRole1, literalRole2),
                  immutable
                )(pos))
              }

              test(s"$verb$immutableString $privilege ON DATABASE foo $preposition role") {
                parsesTo[Statements](privilegeFunc(action, databaseScopeFoo, Seq(literalRole), immutable)(pos))
              }

              test(s"$verb$immutableString $privilege ON DATABASE `fo:o` $preposition role") {
                parsesTo[Statements](privilegeFunc(
                  action,
                  NamedDatabasesScope(Seq(literal("fo:o"))) _,
                  Seq(literalRole),
                  immutable
                )(pos))
              }

              test(s"$verb$immutableString $privilege ON DATABASE more.Dots.more.Dots $preposition role") {
                parsesTo[Statements](privilegeFunc(
                  action,
                  NamedDatabasesScope(Seq(namespacedName("more", "Dots", "more", "Dots"))) _,
                  Seq(literalRole),
                  immutable
                )(pos))
              }

              test(s"$verb$immutableString $privilege ON DATABASE foo $preposition `r:ole`") {
                parsesTo[Statements](privilegeFunc(action, databaseScopeFoo, Seq(literalRColonOle), immutable)(pos))
              }

              test(s"$verb$immutableString $privilege ON DATABASE foo $preposition role1, $$role2") {
                parsesTo[Statements](
                  privilegeFunc(action, databaseScopeFoo, Seq(literalRole1, paramRole2), immutable)(pos)
                )
              }

              test(s"$verb$immutableString $privilege ON DATABASE $$foo $preposition role") {
                parsesTo[Statements](privilegeFunc(action, databaseScopeParamFoo, Seq(literalRole), immutable)(pos))
              }

              test(s"$verb$immutableString $privilege ON DATABASE foo, bar $preposition role") {
                parsesTo[Statements](privilegeFunc(action, databaseScopeFooBar, Seq(literalRole), immutable)(pos))
              }

              test(s"$verb$immutableString $privilege ON DATABASES foo, $$bar $preposition role") {
                parsesTo[Statements](privilegeFunc(
                  action,
                  databaseScopeFooParamBar,
                  Seq(literalRole),
                  immutable
                )(pos))
              }

              test(s"$verb$immutableString $privilege ON HOME DATABASE $preposition role") {
                parsesTo[Statements](privilegeFunc(action, HomeDatabaseScope() _, Seq(literalRole), immutable)(pos))
              }

              test(s"$verb$immutableString $privilege ON HOME DATABASE $preposition $$role1, role2") {
                parsesTo[Statements](privilegeFunc(
                  action,
                  HomeDatabaseScope() _,
                  Seq(paramRole1, literalRole2),
                  immutable
                )(pos))
              }

              test(s"$verb$immutableString $privilege ON DEFAULT DATABASE $preposition role") {
                parsesTo[Statements](
                  privilegeFunc(action, DefaultDatabaseScope() _, Seq(literalRole), immutable)(pos)
                )
              }

              test(s"$verb$immutableString $privilege ON DEFAULT DATABASE $preposition $$role1, role2") {
                parsesTo[Statements](privilegeFunc(
                  action,
                  DefaultDatabaseScope() _,
                  Seq(paramRole1, literalRole2),
                  immutable
                )(pos))
              }

              test(s"$verb$immutableString $privilege ON GRAPH * $preposition role") {
                // GRAPH instead of DATABASE
                if (!(privilege.equals("ALL") || privilege.equals("ALL PRIVILEGES"))) {
                  failsParsing[Statements]
                }
              }

              test(s"$verb$immutableString $privilege ON DATABASE fo:o $preposition role") {
                // invalid database name
                failsParsing[Statements]
              }

              test(s"$verb$immutableString $privilege ON DATABASE foo, * $preposition role") {
                // specific database followed by *
                failsParsing[Statements]
              }

              test(s"$verb$immutableString $privilege ON DATABASE *, foo $preposition role") {
                // * followed by specific database
                failsParsing[Statements]
              }

              test(s"$verb$immutableString $privilege ON DATABASE `a`.`b`.`c` $preposition role") {
                // more than two components
                failsParsing[Statements]
                  .withMessageContaining(
                    "Invalid input ``a`.`b`.`c`` for name. Expected name to contain at most two components separated by `.`."
                  )
              }

              test(s"$verb$immutableString $privilege ON DATABASE foo $preposition r:ole") {
                // invalid role name
                failsParsing[Statements]
              }

              test(s"$verb$immutableString $privilege ON DATABASES * $preposition") {
                // Missing role
                failsParsing[Statements]
              }

              test(s"$verb$immutableString $privilege ON DATABASES *") {
                // Missing role and preposition
                failsParsing[Statements]
              }

              test(s"$verb$immutableString $privilege ON DATABASES $preposition role") {
                // Missing dbName
                failsParsing[Statements]
              }

              test(s"$verb$immutableString $privilege ON * $preposition role") {
                // Missing DATABASE keyword
                failsParsing[Statements]
              }

              test(s"$verb$immutableString $privilege DATABASE foo $preposition role") {
                // Missing ON keyword
                failsParsing[Statements]
              }

              test(s"$verb$immutableString $privilege ON HOME DATABASES $preposition role") {
                // 'databases' instead of 'database'
                failsParsing[Statements].in {
                  case Cypher5JavaCc => _.withMessageStart("""Invalid input 'DATABASES': expected "DATABASE"""")
                  case _ => _.withSyntaxErrorContaining(
                      """Invalid input 'DATABASES': expected 'DATABASE'"""
                    )
                }

              }

              test(s"$verb$immutableString $privilege ON HOME DATABASE foo $preposition role") {
                // both home and database name
                failsParsing[Statements].in {
                  case Cypher5JavaCc => _.withMessageStart(s"""Invalid input 'foo': expected "$preposition"""")
                  case _ => _.withSyntaxErrorContaining(
                      s"""Invalid input 'foo': expected '$preposition'"""
                    )
                }
              }

              test(s"$verb$immutableString $privilege ON HOME DATABASE * $preposition role") {
                // both home and *
                failsParsing[Statements].in {
                  case Cypher5JavaCc => _.withMessageStart(s"""Invalid input '*': expected "$preposition"""")
                  case _ => _.withSyntaxErrorContaining(
                      s"""Invalid input '*': expected '$preposition'"""
                    )
                }
              }

              test(s"$verb$immutableString $privilege ON DEFAULT DATABASES $preposition role") {
                // 'databases' instead of 'database'
                failsParsing[Statements].in {
                  case Cypher5JavaCc => _.withMessageStart("""Invalid input 'DATABASES': expected "DATABASE"""")
                  case _ => _.withSyntaxErrorContaining(
                      """Invalid input 'DATABASES': expected 'DATABASE'""".stripMargin
                    )
                }
              }

              test(s"$verb$immutableString $privilege ON DEFAULT DATABASE foo $preposition role") {
                // both default and database name
                failsParsing[Statements].in {
                  case Cypher5JavaCc => _.withMessageStart(s"""Invalid input 'foo': expected "$preposition"""")
                  case _ => _.withSyntaxErrorContaining(
                      s"""Invalid input 'foo': expected '$preposition'""".stripMargin
                    )
                }
              }

              test(s"$verb$immutableString $privilege ON DEFAULT DATABASE * $preposition role") {
                // both default and *
                failsParsing[Statements].in {
                  case Cypher5JavaCc => _.withMessageStart(s"""Invalid input '*': expected "$preposition"""")
                  case _ => _.withSyntaxErrorContaining(
                      s"""Invalid input '*': expected '$preposition'""".stripMargin
                    )
                }
              }
          }

          // Dropping instead of creating name management privileges

          test(s"$verb$immutableString DROP NEW LABEL ON DATABASE * $preposition role") {
            failsParsing[Statements]
          }

          test(s"$verb$immutableString DROP NEW TYPE ON DATABASE * $preposition role") {
            failsParsing[Statements]
          }

          test(s"$verb$immutableString DROP NEW NAME ON DATABASE * $preposition role") {
            failsParsing[Statements]
          }

          test(s"$verb$immutableString DROP LABEL ON DATABASE * $preposition role") {
            failsParsing[Statements]
          }

          test(s"$verb$immutableString DROP TYPE ON DATABASE * $preposition role") {
            failsParsing[Statements]
          }

          test(s"$verb$immutableString DROP NAME ON DATABASE * $preposition role") {
            failsParsing[Statements]
          }
      }
  }

  // transaction management

  Seq(
    ("GRANT", "TO", grantTransactionPrivilege: transactionPrivilegeFunc),
    ("DENY", "TO", denyTransactionPrivilege: transactionPrivilegeFunc),
    ("REVOKE GRANT", "FROM", revokeGrantTransactionPrivilege: transactionPrivilegeFunc),
    ("REVOKE DENY", "FROM", revokeDenyTransactionPrivilege: transactionPrivilegeFunc),
    ("REVOKE", "FROM", revokeTransactionPrivilege: transactionPrivilegeFunc)
  ).foreach {
    case (verb: String, preposition: String, privilegeFunc: transactionPrivilegeFunc) =>
      Seq[Immutable](true, false).foreach {
        immutable =>
          val immutableString = immutableOrEmpty(immutable)
          test(s"$verb$immutableString SHOW TRANSACTION (*) ON DATABASE * $preposition role") {
            parsesTo[Statements](privilegeFunc(
              ShowTransactionAction,
              AllDatabasesScope() _,
              List(UserAllQualifier() _),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString SHOW TRANSACTIONS (*) ON DATABASES foo $preposition role1, role2") {
            parsesTo[Statements](privilegeFunc(
              ShowTransactionAction,
              databaseScopeFoo,
              List(UserAllQualifier() _),
              Seq(literalRole1, literalRole2),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString SHOW TRANSACTIONS (*) ON DATABASES $$foo $preposition $$role1, $$role2") {
            parsesTo[Statements](privilegeFunc(
              ShowTransactionAction,
              databaseScopeParamFoo,
              List(UserAllQualifier() _),
              Seq(paramRole1, paramRole2),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString SHOW TRANSACTION (user) ON HOME DATABASE $preposition role") {
            parsesTo[Statements](privilegeFunc(
              ShowTransactionAction,
              HomeDatabaseScope() _,
              List(UserQualifier(literalUser) _),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString SHOW TRANSACTION ($$user) ON HOME DATABASE $preposition role") {
            parsesTo[Statements](privilegeFunc(
              ShowTransactionAction,
              HomeDatabaseScope() _,
              List(UserQualifier(paramUser) _),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString SHOW TRANSACTION (user) ON DEFAULT DATABASE $preposition role") {
            parsesTo[Statements](privilegeFunc(
              ShowTransactionAction,
              DefaultDatabaseScope() _,
              List(UserQualifier(literalUser) _),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString SHOW TRANSACTION ($$user) ON DEFAULT DATABASE $preposition role") {
            parsesTo[Statements](privilegeFunc(
              ShowTransactionAction,
              DefaultDatabaseScope() _,
              List(UserQualifier(paramUser) _),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString SHOW TRANSACTIONS (user1,user2) ON DATABASES * $preposition role1, role2") {
            parsesTo[Statements](privilegeFunc(
              ShowTransactionAction,
              AllDatabasesScope() _,
              List(UserQualifier(literalUser1) _, UserQualifier(literal("user2")) _),
              Seq(literalRole1, literalRole2),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString SHOW TRANSACTIONS ON DATABASES * $preposition role") {
            parsesTo[Statements](privilegeFunc(
              ShowTransactionAction,
              AllDatabasesScope() _,
              List(UserAllQualifier() _),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString SHOW TRANSACTION ON DATABASE foo, bar $preposition role") {
            parsesTo[Statements](privilegeFunc(
              ShowTransactionAction,
              databaseScopeFooBar,
              List(UserAllQualifier() _),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString SHOW TRANSACTION (user) ON DATABASES foo, $$bar $preposition role") {
            parsesTo[Statements](privilegeFunc(
              ShowTransactionAction,
              databaseScopeFooParamBar,
              List(UserQualifier(literalUser) _),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString TERMINATE TRANSACTION (*) ON DATABASE * $preposition $$role") {
            parsesTo[Statements](privilegeFunc(
              TerminateTransactionAction,
              AllDatabasesScope() _,
              List(UserAllQualifier() _),
              Seq(paramRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString TERMINATE TRANSACTIONS (*) ON DATABASES foo $preposition role1, role2") {
            parsesTo[Statements](privilegeFunc(
              TerminateTransactionAction,
              databaseScopeFoo,
              List(UserAllQualifier() _),
              Seq(literalRole1, literalRole2),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString TERMINATE TRANSACTIONS (*) ON DATABASES $$foo $preposition role") {
            parsesTo[Statements](privilegeFunc(
              TerminateTransactionAction,
              databaseScopeParamFoo,
              List(UserAllQualifier() _),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString TERMINATE TRANSACTION (user) ON HOME DATABASE $preposition role") {
            parsesTo[Statements](privilegeFunc(
              TerminateTransactionAction,
              HomeDatabaseScope() _,
              List(UserQualifier(literalUser) _),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString TERMINATE TRANSACTION (user) ON DEFAULT DATABASE $preposition role") {
            parsesTo[Statements](privilegeFunc(
              TerminateTransactionAction,
              DefaultDatabaseScope() _,
              List(UserQualifier(literalUser) _),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString TERMINATE TRANSACTIONS (user1,user2) ON DATABASES * $preposition role1, role2") {
            parsesTo[Statements](privilegeFunc(
              TerminateTransactionAction,
              AllDatabasesScope() _,
              List(UserQualifier(literalUser1) _, UserQualifier(literal("user2")) _),
              Seq(literalRole1, literalRole2),
              immutable
            )(pos))
          }

          test(
            s"$verb$immutableString TERMINATE TRANSACTIONS ($$user1,$$user2) ON DATABASES * $preposition role1, role2"
          ) {
            parsesTo[Statements](privilegeFunc(
              TerminateTransactionAction,
              AllDatabasesScope() _,
              List(UserQualifier(stringParam("user1")) _, UserQualifier(stringParam("user2")) _),
              Seq(literalRole1, literalRole2),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString TERMINATE TRANSACTIONS ON DATABASES * $preposition role") {
            parsesTo[Statements](privilegeFunc(
              TerminateTransactionAction,
              AllDatabasesScope() _,
              List(UserAllQualifier() _),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString TERMINATE TRANSACTION ON DATABASE foo, bar $preposition role") {
            parsesTo[Statements](privilegeFunc(
              TerminateTransactionAction,
              databaseScopeFooBar,
              List(UserAllQualifier() _),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString TERMINATE TRANSACTION (user) ON DATABASES foo, $$bar $preposition role") {
            parsesTo[Statements](privilegeFunc(
              TerminateTransactionAction,
              databaseScopeFooParamBar,
              List(UserQualifier(literalUser) _),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString TRANSACTION ON DATABASES * $preposition role1, role2") {
            parsesTo[Statements](privilegeFunc(
              AllTransactionActions,
              AllDatabasesScope() _,
              List(UserAllQualifier() _),
              Seq(literalRole1, literalRole2),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString TRANSACTION (*) ON DATABASES foo $preposition role1, $$role2") {
            parsesTo[Statements](privilegeFunc(
              AllTransactionActions,
              databaseScopeFoo,
              List(UserAllQualifier() _),
              Seq(literalRole1, paramRole2),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString TRANSACTION (*) ON DATABASES $$foo $preposition role1, role2") {
            parsesTo[Statements](privilegeFunc(
              AllTransactionActions,
              databaseScopeParamFoo,
              List(UserAllQualifier() _),
              Seq(literalRole1, literalRole2),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString TRANSACTION (user) ON DATABASES * $preposition role") {
            parsesTo[Statements](privilegeFunc(
              AllTransactionActions,
              AllDatabasesScope() _,
              List(UserQualifier(literalUser) _),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString TRANSACTION ON DATABASE foo, bar $preposition role") {
            parsesTo[Statements](privilegeFunc(
              AllTransactionActions,
              databaseScopeFooBar,
              List(UserAllQualifier() _),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString TRANSACTION (user) ON DATABASES foo, $$bar $preposition role") {
            parsesTo[Statements](privilegeFunc(
              AllTransactionActions,
              databaseScopeFooParamBar,
              List(UserQualifier(literalUser) _),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString TRANSACTION MANAGEMENT ON HOME DATABASE $preposition role") {
            parsesTo[Statements](privilegeFunc(
              AllTransactionActions,
              HomeDatabaseScope() _,
              List(UserAllQualifier() _),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString TRANSACTION MANAGEMENT ON DEFAULT DATABASE $preposition role") {
            parsesTo[Statements](privilegeFunc(
              AllTransactionActions,
              DefaultDatabaseScope() _,
              List(UserAllQualifier() _),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString TRANSACTION MANAGEMENT (*) ON DATABASE * $preposition role") {
            parsesTo[Statements](privilegeFunc(
              AllTransactionActions,
              AllDatabasesScope() _,
              List(UserAllQualifier() _),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString TRANSACTION MANAGEMENT (user) ON DATABASES * $preposition role") {
            parsesTo[Statements](privilegeFunc(
              AllTransactionActions,
              AllDatabasesScope() _,
              List(UserQualifier(literalUser) _),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString TRANSACTION MANAGEMENT (user1, $$user2) ON DATABASES * $preposition role") {
            parsesTo[Statements](privilegeFunc(
              AllTransactionActions,
              AllDatabasesScope() _,
              List(UserQualifier(literalUser1) _, UserQualifier(stringParam("user2")) _),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString TRANSACTION MANAGEMENT ON DATABASE foo, bar $preposition role") {
            parsesTo[Statements](privilegeFunc(
              AllTransactionActions,
              databaseScopeFooBar,
              List(UserAllQualifier() _),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString TRANSACTION MANAGEMENT (user) ON DATABASES foo, $$bar $preposition role") {
            parsesTo[Statements](privilegeFunc(
              AllTransactionActions,
              databaseScopeFooParamBar,
              List(UserQualifier(literalUser) _),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString TRANSACTION ON DATABASE foo, * $preposition role") {
            failsParsing[Statements]
          }

          test(s"$verb$immutableString TRANSACTION ON DATABASE `a`.`b`.`c` $preposition role") {
            failsParsing[Statements]
              .withMessageContaining(
                "Invalid input ``a`.`b`.`c`` for name. Expected name to contain at most two components separated by `.`."
              )
          }

          test(s"$verb$immutableString TRANSACTION ON DATABASE *, foo $preposition role") {
            failsParsing[Statements]
          }

          test(s"$verb$immutableString TRANSACTIONS ON DATABASES * $preposition role") {
            // can't have the complete error message, since grant and revoke have more accepted keywords than deny
            val expected =
              """Invalid input 'TRANSACTIONS': expected
                |  "ACCESS"""".stripMargin
            failsParsing[Statements].in {
              case Cypher5JavaCc => _.withMessageStart(expected)
              case _ => _.withSyntaxErrorContaining(
                  """Invalid input 'TRANSACTIONS': expected"""
                )
            }
          }

          test(s"$verb$immutableString TRANSACTIONS (*) ON DATABASES * $preposition role") {
            // can't have the complete error message, since grant and revoke have more accepted keywords than deny
            val expected =
              """Invalid input 'TRANSACTIONS': expected
                |  "ACCESS"""".stripMargin
            failsParsing[Statements].in {
              case Cypher5JavaCc => _.withMessageStart(expected)
              case _ => _.withSyntaxErrorContaining(
                  """Invalid input 'TRANSACTIONS': expected"""
                )
            }
          }

          test(s"$verb$immutableString TRANSACTIONS MANAGEMENT ON DATABASES * $preposition role") {
            // can't have the complete error message, since grant and revoke have more accepted keywords than deny
            val expected =
              """Invalid input 'TRANSACTIONS': expected
                |  "ACCESS"""".stripMargin
            failsParsing[Statements].in {
              case Cypher5JavaCc => _.withMessageStart(expected)
              case _ => _.withSyntaxErrorContaining(
                  """Invalid input 'TRANSACTIONS': expected"""
                )
            }
          }

          test(s"$verb$immutableString TRANSACTIONS MANAGEMENT (*) ON DATABASES * $preposition role") {
            // can't have the complete error message, since grant and revoke have more accepted keywords than deny
            val expected =
              """Invalid input 'TRANSACTIONS': expected
                |  "ACCESS"""".stripMargin
            failsParsing[Statements].in {
              case Cypher5JavaCc => _.withMessageStart(expected)
              case _ => _.withSyntaxErrorContaining(
                  """Invalid input 'TRANSACTIONS': expected"""
                )
            }
          }
      }
  }
}
