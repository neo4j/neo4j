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
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.factory.neo4j.AdministrationAndSchemaCommandParserTestBase
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsing.Antlr
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsing.JavaCc
import org.neo4j.exceptions.SyntaxException

class DatabasePrivilegeAdministrationCommandParserTest extends AdministrationAndSchemaCommandParserTestBase {
  private val databaseScopeFoo = ast.NamedDatabasesScope(Seq(literalFoo))(_)
  private val databaseScopeParamFoo = ast.NamedDatabasesScope(Seq(namespacedParamFoo))(_)
  private val databaseScopeFooBar = ast.NamedDatabasesScope(Seq(literalFoo, namespacedName("bar")))(_)
  private val databaseScopeFooParamBar = ast.NamedDatabasesScope(Seq(literalFoo, stringParamName("bar")))(_)

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
            ("ACCESS", ast.AccessDatabaseAction),
            ("START", ast.StartDatabaseAction),
            ("STOP", ast.StopDatabaseAction),
            ("CREATE INDEX", ast.CreateIndexAction),
            ("CREATE INDEXES", ast.CreateIndexAction),
            ("DROP INDEX", ast.DropIndexAction),
            ("DROP INDEXES", ast.DropIndexAction),
            ("SHOW INDEX", ast.ShowIndexAction),
            ("SHOW INDEXES", ast.ShowIndexAction),
            ("INDEX", ast.AllIndexActions),
            ("INDEXES", ast.AllIndexActions),
            ("INDEX MANAGEMENT", ast.AllIndexActions),
            ("INDEXES MANAGEMENT", ast.AllIndexActions),
            ("CREATE CONSTRAINT", ast.CreateConstraintAction),
            ("CREATE CONSTRAINTS", ast.CreateConstraintAction),
            ("DROP CONSTRAINT", ast.DropConstraintAction),
            ("DROP CONSTRAINTS", ast.DropConstraintAction),
            ("SHOW CONSTRAINT", ast.ShowConstraintAction),
            ("SHOW CONSTRAINTS", ast.ShowConstraintAction),
            ("CONSTRAINT", ast.AllConstraintActions),
            ("CONSTRAINTS", ast.AllConstraintActions),
            ("CONSTRAINT MANAGEMENT", ast.AllConstraintActions),
            ("CONSTRAINTS MANAGEMENT", ast.AllConstraintActions),
            ("CREATE NEW LABEL", ast.CreateNodeLabelAction),
            ("CREATE NEW LABELS", ast.CreateNodeLabelAction),
            ("CREATE NEW NODE LABEL", ast.CreateNodeLabelAction),
            ("CREATE NEW NODE LABELS", ast.CreateNodeLabelAction),
            ("CREATE NEW TYPE", ast.CreateRelationshipTypeAction),
            ("CREATE NEW TYPES", ast.CreateRelationshipTypeAction),
            ("CREATE NEW RELATIONSHIP TYPE", ast.CreateRelationshipTypeAction),
            ("CREATE NEW RELATIONSHIP TYPES", ast.CreateRelationshipTypeAction),
            ("CREATE NEW NAME", ast.CreatePropertyKeyAction),
            ("CREATE NEW NAMES", ast.CreatePropertyKeyAction),
            ("CREATE NEW PROPERTY NAME", ast.CreatePropertyKeyAction),
            ("CREATE NEW PROPERTY NAMES", ast.CreatePropertyKeyAction),
            ("NAME", ast.AllTokenActions),
            ("NAME MANAGEMENT", ast.AllTokenActions),
            ("ALL", ast.AllDatabaseAction),
            ("ALL PRIVILEGES", ast.AllDatabaseAction),
            ("ALL DATABASE PRIVILEGES", ast.AllDatabaseAction)
          ).foreach {
            case (privilege: String, action: ast.DatabaseAction) =>
              test(s"$verb$immutableString $privilege ON DATABASE * $preposition $$role") {
                parsesTo[Statements](privilegeFunc(action, ast.AllDatabasesScope() _, Seq(paramRole), immutable)(pos))
              }

              test(s"$verb$immutableString $privilege ON DATABASES * $preposition role") {
                parsesTo[Statements](privilegeFunc(action, ast.AllDatabasesScope() _, Seq(literalRole), immutable)(pos))
              }

              test(s"$verb$immutableString $privilege ON DATABASE * $preposition role1, role2") {
                parsesTo[Statements](privilegeFunc(
                  action,
                  ast.AllDatabasesScope() _,
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
                  ast.NamedDatabasesScope(Seq(literal("fo:o"))) _,
                  Seq(literalRole),
                  immutable
                )(pos))
              }

              test(s"$verb$immutableString $privilege ON DATABASE more.Dots.more.Dots $preposition role") {
                parsesTo[Statements](privilegeFunc(
                  action,
                  ast.NamedDatabasesScope(Seq(namespacedName("more", "Dots", "more", "Dots"))) _,
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
                parsesTo[Statements](privilegeFunc(action, ast.HomeDatabaseScope() _, Seq(literalRole), immutable)(pos))
              }

              test(s"$verb$immutableString $privilege ON HOME DATABASE $preposition $$role1, role2") {
                parsesTo[Statements](privilegeFunc(
                  action,
                  ast.HomeDatabaseScope() _,
                  Seq(paramRole1, literalRole2),
                  immutable
                )(pos))
              }

              test(s"$verb$immutableString $privilege ON DEFAULT DATABASE $preposition role") {
                parsesTo[Statements](
                  privilegeFunc(action, ast.DefaultDatabaseScope() _, Seq(literalRole), immutable)(pos)
                )
              }

              test(s"$verb$immutableString $privilege ON DEFAULT DATABASE $preposition $$role1, role2") {
                parsesTo[Statements](privilegeFunc(
                  action,
                  ast.DefaultDatabaseScope() _,
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
                failsParsing[Statements]
                  .parseIn(JavaCc)(_.withMessageStart("""Invalid input 'DATABASES': expected "DATABASE""""))
                  .parseIn(Antlr)(_.throws[SyntaxException].withMessageStart(
                    """Invalid input 'DATABASES': expected 'DATABASE'"""
                  ))

              }

              test(s"$verb$immutableString $privilege ON HOME DATABASE foo $preposition role") {
                // both home and database name
                failsParsing[Statements]
                  .parseIn(JavaCc)(_.withMessageStart(s"""Invalid input 'foo': expected "$preposition""""))
                  .parseIn(Antlr)(_.throws[SyntaxException].withMessageStart(
                    s"""Invalid input 'foo': expected '$preposition'"""
                  ))
              }

              test(s"$verb$immutableString $privilege ON HOME DATABASE * $preposition role") {
                // both home and *
                failsParsing[Statements]
                  .parseIn(JavaCc)(_.withMessageStart(s"""Invalid input '*': expected "$preposition""""))
                  .parseIn(Antlr)(_.throws[SyntaxException].withMessageStart(
                    s"""Invalid input '*': expected '$preposition'"""
                  ))
              }

              test(s"$verb$immutableString $privilege ON DEFAULT DATABASES $preposition role") {
                // 'databases' instead of 'database'
                failsParsing[Statements]
                  .parseIn(JavaCc)(_.withMessageStart("""Invalid input 'DATABASES': expected "DATABASE""""))
                  .parseIn(Antlr)(_.throws[SyntaxException].withMessageStart(
                    """Invalid input 'DATABASES': expected 'DATABASE'""".stripMargin
                  ))
              }

              test(s"$verb$immutableString $privilege ON DEFAULT DATABASE foo $preposition role") {
                // both default and database name
                failsParsing[Statements]
                  .parseIn(JavaCc)(_.withMessageStart(s"""Invalid input 'foo': expected "$preposition""""))
                  .parseIn(Antlr)(_.throws[SyntaxException].withMessageStart(
                    s"""Invalid input 'foo': expected '$preposition'""".stripMargin
                  ))
              }

              test(s"$verb$immutableString $privilege ON DEFAULT DATABASE * $preposition role") {
                // both default and *
                failsParsing[Statements]
                  .parseIn(JavaCc)(_.withMessageStart(s"""Invalid input '*': expected "$preposition""""))
                  .parseIn(Antlr)(_.throws[SyntaxException].withMessageStart(
                    s"""Invalid input '*': expected '$preposition'""".stripMargin
                  ))
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
              ast.ShowTransactionAction,
              ast.AllDatabasesScope() _,
              List(ast.UserAllQualifier() _),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString SHOW TRANSACTIONS (*) ON DATABASES foo $preposition role1, role2") {
            parsesTo[Statements](privilegeFunc(
              ast.ShowTransactionAction,
              databaseScopeFoo,
              List(ast.UserAllQualifier() _),
              Seq(literalRole1, literalRole2),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString SHOW TRANSACTIONS (*) ON DATABASES $$foo $preposition $$role1, $$role2") {
            parsesTo[Statements](privilegeFunc(
              ast.ShowTransactionAction,
              databaseScopeParamFoo,
              List(ast.UserAllQualifier() _),
              Seq(paramRole1, paramRole2),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString SHOW TRANSACTION (user) ON HOME DATABASE $preposition role") {
            parsesTo[Statements](privilegeFunc(
              ast.ShowTransactionAction,
              ast.HomeDatabaseScope() _,
              List(ast.UserQualifier(literalUser) _),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString SHOW TRANSACTION ($$user) ON HOME DATABASE $preposition role") {
            parsesTo[Statements](privilegeFunc(
              ast.ShowTransactionAction,
              ast.HomeDatabaseScope() _,
              List(ast.UserQualifier(paramUser) _),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString SHOW TRANSACTION (user) ON DEFAULT DATABASE $preposition role") {
            parsesTo[Statements](privilegeFunc(
              ast.ShowTransactionAction,
              ast.DefaultDatabaseScope() _,
              List(ast.UserQualifier(literalUser) _),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString SHOW TRANSACTION ($$user) ON DEFAULT DATABASE $preposition role") {
            parsesTo[Statements](privilegeFunc(
              ast.ShowTransactionAction,
              ast.DefaultDatabaseScope() _,
              List(ast.UserQualifier(paramUser) _),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString SHOW TRANSACTIONS (user1,user2) ON DATABASES * $preposition role1, role2") {
            parsesTo[Statements](privilegeFunc(
              ast.ShowTransactionAction,
              ast.AllDatabasesScope() _,
              List(ast.UserQualifier(literalUser1) _, ast.UserQualifier(literal("user2")) _),
              Seq(literalRole1, literalRole2),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString SHOW TRANSACTIONS ON DATABASES * $preposition role") {
            parsesTo[Statements](privilegeFunc(
              ast.ShowTransactionAction,
              ast.AllDatabasesScope() _,
              List(ast.UserAllQualifier() _),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString SHOW TRANSACTION ON DATABASE foo, bar $preposition role") {
            parsesTo[Statements](privilegeFunc(
              ast.ShowTransactionAction,
              databaseScopeFooBar,
              List(ast.UserAllQualifier() _),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString SHOW TRANSACTION (user) ON DATABASES foo, $$bar $preposition role") {
            parsesTo[Statements](privilegeFunc(
              ast.ShowTransactionAction,
              databaseScopeFooParamBar,
              List(ast.UserQualifier(literalUser) _),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString TERMINATE TRANSACTION (*) ON DATABASE * $preposition $$role") {
            parsesTo[Statements](privilegeFunc(
              ast.TerminateTransactionAction,
              ast.AllDatabasesScope() _,
              List(ast.UserAllQualifier() _),
              Seq(paramRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString TERMINATE TRANSACTIONS (*) ON DATABASES foo $preposition role1, role2") {
            parsesTo[Statements](privilegeFunc(
              ast.TerminateTransactionAction,
              databaseScopeFoo,
              List(ast.UserAllQualifier() _),
              Seq(literalRole1, literalRole2),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString TERMINATE TRANSACTIONS (*) ON DATABASES $$foo $preposition role") {
            parsesTo[Statements](privilegeFunc(
              ast.TerminateTransactionAction,
              databaseScopeParamFoo,
              List(ast.UserAllQualifier() _),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString TERMINATE TRANSACTION (user) ON HOME DATABASE $preposition role") {
            parsesTo[Statements](privilegeFunc(
              ast.TerminateTransactionAction,
              ast.HomeDatabaseScope() _,
              List(ast.UserQualifier(literalUser) _),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString TERMINATE TRANSACTION (user) ON DEFAULT DATABASE $preposition role") {
            parsesTo[Statements](privilegeFunc(
              ast.TerminateTransactionAction,
              ast.DefaultDatabaseScope() _,
              List(ast.UserQualifier(literalUser) _),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString TERMINATE TRANSACTIONS (user1,user2) ON DATABASES * $preposition role1, role2") {
            parsesTo[Statements](privilegeFunc(
              ast.TerminateTransactionAction,
              ast.AllDatabasesScope() _,
              List(ast.UserQualifier(literalUser1) _, ast.UserQualifier(literal("user2")) _),
              Seq(literalRole1, literalRole2),
              immutable
            )(pos))
          }

          test(
            s"$verb$immutableString TERMINATE TRANSACTIONS ($$user1,$$user2) ON DATABASES * $preposition role1, role2"
          ) {
            parsesTo[Statements](privilegeFunc(
              ast.TerminateTransactionAction,
              ast.AllDatabasesScope() _,
              List(ast.UserQualifier(stringParam("user1")) _, ast.UserQualifier(stringParam("user2")) _),
              Seq(literalRole1, literalRole2),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString TERMINATE TRANSACTIONS ON DATABASES * $preposition role") {
            parsesTo[Statements](privilegeFunc(
              ast.TerminateTransactionAction,
              ast.AllDatabasesScope() _,
              List(ast.UserAllQualifier() _),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString TERMINATE TRANSACTION ON DATABASE foo, bar $preposition role") {
            parsesTo[Statements](privilegeFunc(
              ast.TerminateTransactionAction,
              databaseScopeFooBar,
              List(ast.UserAllQualifier() _),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString TERMINATE TRANSACTION (user) ON DATABASES foo, $$bar $preposition role") {
            parsesTo[Statements](privilegeFunc(
              ast.TerminateTransactionAction,
              databaseScopeFooParamBar,
              List(ast.UserQualifier(literalUser) _),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString TRANSACTION ON DATABASES * $preposition role1, role2") {
            parsesTo[Statements](privilegeFunc(
              ast.AllTransactionActions,
              ast.AllDatabasesScope() _,
              List(ast.UserAllQualifier() _),
              Seq(literalRole1, literalRole2),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString TRANSACTION (*) ON DATABASES foo $preposition role1, $$role2") {
            parsesTo[Statements](privilegeFunc(
              ast.AllTransactionActions,
              databaseScopeFoo,
              List(ast.UserAllQualifier() _),
              Seq(literalRole1, paramRole2),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString TRANSACTION (*) ON DATABASES $$foo $preposition role1, role2") {
            parsesTo[Statements](privilegeFunc(
              ast.AllTransactionActions,
              databaseScopeParamFoo,
              List(ast.UserAllQualifier() _),
              Seq(literalRole1, literalRole2),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString TRANSACTION (user) ON DATABASES * $preposition role") {
            parsesTo[Statements](privilegeFunc(
              ast.AllTransactionActions,
              ast.AllDatabasesScope() _,
              List(ast.UserQualifier(literalUser) _),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString TRANSACTION ON DATABASE foo, bar $preposition role") {
            parsesTo[Statements](privilegeFunc(
              ast.AllTransactionActions,
              databaseScopeFooBar,
              List(ast.UserAllQualifier() _),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString TRANSACTION (user) ON DATABASES foo, $$bar $preposition role") {
            parsesTo[Statements](privilegeFunc(
              ast.AllTransactionActions,
              databaseScopeFooParamBar,
              List(ast.UserQualifier(literalUser) _),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString TRANSACTION MANAGEMENT ON HOME DATABASE $preposition role") {
            parsesTo[Statements](privilegeFunc(
              ast.AllTransactionActions,
              ast.HomeDatabaseScope() _,
              List(ast.UserAllQualifier() _),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString TRANSACTION MANAGEMENT ON DEFAULT DATABASE $preposition role") {
            parsesTo[Statements](privilegeFunc(
              ast.AllTransactionActions,
              ast.DefaultDatabaseScope() _,
              List(ast.UserAllQualifier() _),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString TRANSACTION MANAGEMENT (*) ON DATABASE * $preposition role") {
            parsesTo[Statements](privilegeFunc(
              ast.AllTransactionActions,
              ast.AllDatabasesScope() _,
              List(ast.UserAllQualifier() _),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString TRANSACTION MANAGEMENT (user) ON DATABASES * $preposition role") {
            parsesTo[Statements](privilegeFunc(
              ast.AllTransactionActions,
              ast.AllDatabasesScope() _,
              List(ast.UserQualifier(literalUser) _),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString TRANSACTION MANAGEMENT (user1, $$user2) ON DATABASES * $preposition role") {
            parsesTo[Statements](privilegeFunc(
              ast.AllTransactionActions,
              ast.AllDatabasesScope() _,
              List(ast.UserQualifier(literalUser1) _, ast.UserQualifier(stringParam("user2")) _),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString TRANSACTION MANAGEMENT ON DATABASE foo, bar $preposition role") {
            parsesTo[Statements](privilegeFunc(
              ast.AllTransactionActions,
              databaseScopeFooBar,
              List(ast.UserAllQualifier() _),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString TRANSACTION MANAGEMENT (user) ON DATABASES foo, $$bar $preposition role") {
            parsesTo[Statements](privilegeFunc(
              ast.AllTransactionActions,
              databaseScopeFooParamBar,
              List(ast.UserQualifier(literalUser) _),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString TRANSACTION ON DATABASE foo, * $preposition role") {
            failsParsing[Statements]
          }

          test(s"$verb$immutableString TRANSACTION ON DATABASE *, foo $preposition role") {
            failsParsing[Statements]
          }

          test(s"$verb$immutableString TRANSACTIONS ON DATABASES * $preposition role") {
            // can't have the complete error message, since grant and revoke have more accepted keywords than deny
            val expected =
              """Invalid input 'TRANSACTIONS': expected
                |  "ACCESS"""".stripMargin
            failsParsing[Statements]
              .parseIn(JavaCc)(_.withMessageStart(expected))
              .parseIn(Antlr)(_.throws[SyntaxException].withMessageStart(
                """Invalid input 'TRANSACTIONS': expected"""
              ))
          }

          test(s"$verb$immutableString TRANSACTIONS (*) ON DATABASES * $preposition role") {
            // can't have the complete error message, since grant and revoke have more accepted keywords than deny
            val expected =
              """Invalid input 'TRANSACTIONS': expected
                |  "ACCESS"""".stripMargin
            failsParsing[Statements]
              .parseIn(JavaCc)(_.withMessageStart(expected))
              .parseIn(Antlr)(_.throws[SyntaxException].withMessageStart(
                """Invalid input 'TRANSACTIONS': expected"""
              ))
          }

          test(s"$verb$immutableString TRANSACTIONS MANAGEMENT ON DATABASES * $preposition role") {
            // can't have the complete error message, since grant and revoke have more accepted keywords than deny
            val expected =
              """Invalid input 'TRANSACTIONS': expected
                |  "ACCESS"""".stripMargin
            failsParsing[Statements]
              .parseIn(JavaCc)(_.withMessageStart(expected))
              .parseIn(Antlr)(_.throws[SyntaxException].withMessageStart(
                """Invalid input 'TRANSACTIONS': expected"""
              ))
          }

          test(s"$verb$immutableString TRANSACTIONS MANAGEMENT (*) ON DATABASES * $preposition role") {
            // can't have the complete error message, since grant and revoke have more accepted keywords than deny
            val expected =
              """Invalid input 'TRANSACTIONS': expected
                |  "ACCESS"""".stripMargin
            failsParsing[Statements]
              .parseIn(JavaCc)(_.withMessageStart(expected))
              .parseIn(Antlr)(_.throws[SyntaxException].withMessageStart(
                """Invalid input 'TRANSACTIONS': expected"""
              ))
          }
      }
  }
}
