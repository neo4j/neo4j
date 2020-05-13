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

class DatabasePrivilegeAdministrationCommandParserTest extends AdministrationCommandParserTestBase {

  Seq(
    ("GRANT", "TO", grantDatabasePrivilege: databasePrivilegeFunc),
    ("DENY", "TO", denyDatabasePrivilege: databasePrivilegeFunc),
    ("REVOKE GRANT", "FROM", revokeGrantDatabasePrivilege: databasePrivilegeFunc),
    ("REVOKE DENY", "FROM", revokeDenyDatabasePrivilege: databasePrivilegeFunc),
    ("REVOKE", "FROM", revokeDatabasePrivilege: databasePrivilegeFunc)
  ).foreach {
    case (command: String, preposition: String, privilegeFunc: databasePrivilegeFunc) =>

      Seq(
        ("ACCESS", ast.AccessDatabaseAction),
        ("START", ast.StartDatabaseAction),
        ("STOP", ast.StopDatabaseAction),
        ("CREATE INDEX", ast.CreateIndexAction),
        ("CREATE INDEXES", ast.CreateIndexAction),
        ("DROP INDEX", ast.DropIndexAction),
        ("DROP INDEXES", ast.DropIndexAction),
        ("INDEX", ast.AllIndexActions),
        ("INDEXES", ast.AllIndexActions),
        ("INDEX MANAGEMENT", ast.AllIndexActions),
        ("INDEXES MANAGEMENT", ast.AllIndexActions),
        ("CREATE CONSTRAINT", ast.CreateConstraintAction),
        ("CREATE CONSTRAINTS", ast.CreateConstraintAction),
        ("DROP CONSTRAINT", ast.DropConstraintAction),
        ("DROP CONSTRAINTS", ast.DropConstraintAction),
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

          test(s"$command $privilege ON DATABASE * $preposition $$role") {
            yields(privilegeFunc(action, List(ast.AllGraphsScope() _), Seq(param("role"))))
          }

          test(s"$command $privilege ON DATABASES * $preposition role") {
            yields(privilegeFunc(action, List(ast.AllGraphsScope() _), Seq(literal("role"))))
          }

          test(s"$command $privilege ON DATABASE * $preposition role1, role2") {
            yields(privilegeFunc(action, List(ast.AllGraphsScope() _), Seq(literal("role1"), literal("role2"))))
          }

          test(s"$command $privilege ON DATABASE foo $preposition role") {
            yields(privilegeFunc(action, List(ast.NamedGraphScope(literal("foo")) _), Seq(literal("role"))))
          }

          test(s"$command $privilege ON DATABASE `fo:o` $preposition role") {
            yields(privilegeFunc(action, List(ast.NamedGraphScope(literal("fo:o")) _), Seq(literal("role"))))
          }

          test(s"$command $privilege ON DATABASE foo $preposition `r:ole`") {
            yields(privilegeFunc(action, List(ast.NamedGraphScope(literal("foo")) _), Seq(literal("r:ole"))))
          }

          test(s"$command $privilege ON DATABASE foo $preposition role1, $$role2") {
            yields(privilegeFunc(action, List(ast.NamedGraphScope(literal("foo")) _), Seq(literal("role1"), param("role2"))))
          }

          test(s"$command $privilege ON DATABASE $$foo $preposition role") {
            yields(privilegeFunc(action, List(ast.NamedGraphScope(param("foo")) _), Seq(literal("role"))))
          }

          test(s"$command $privilege ON DATABASE foo, bar $preposition role") {
            yields(privilegeFunc(action, List(ast.NamedGraphScope(literal("foo")) _, ast.NamedGraphScope(literal("bar")) _), Seq(literal("role"))))
          }

          test(s"$command $privilege ON DATABASES foo, $$bar $preposition role") {
            yields(privilegeFunc(action, List(ast.NamedGraphScope(literal("foo")) _, ast.NamedGraphScope(param("bar")) _), Seq(literal("role"))))
          }

          test(s"$command $privilege ON DEFAULT DATABASE $preposition role") {
            yields(privilegeFunc(action, List(ast.DefaultDatabaseScope() _), Seq(literal("role"))))
          }

          test(s"$command $privilege ON DEFAULT DATABASE $preposition $$role1, role2") {
            yields(privilegeFunc(action, List(ast.DefaultDatabaseScope() _), Seq(param("role1"), literal("role2"))))
          }

          test(s"$command $privilege ON GRAPH * $preposition role") {
            // GRAPH instead of DATABASE
            if (!(privilege.equals("ALL") || privilege.equals("ALL PRIVILEGES"))) {
              failsToParse
            }
          }

          test(s"$command $privilege ON DATABASE fo:o $preposition role") {
            // invalid database name
            failsToParse
          }

          test(s"$command $privilege ON DATABASE foo, * $preposition role") {
            // specific database followed by *
            failsToParse
          }

          test(s"$command $privilege ON DATABASE *, foo $preposition role") {
            // * followed by specific database
            failsToParse
          }

          test(s"$command $privilege ON DATABASE foo $preposition r:ole") {
            // invalid role name
            failsToParse
          }

          test(s"$command $privilege ON DATABASES * $preposition") {
            // Missing role
            failsToParse
          }

          test(s"$command $privilege ON DATABASES *") {
            // Missing role and preposition
            failsToParse
          }

          test(s"$command $privilege ON DATABASES $preposition role") {
            // Missing dbName
            failsToParse
          }

          test(s"$command $privilege ON * $preposition role") {
            // Missing DATABASE keyword
            failsToParse
          }

          test(s"$command $privilege DATABASE foo $preposition role") {
            // Missing ON keyword
            failsToParse
          }

          test(s"$command $privilege ON DEFAULT DATABASES $preposition role") {
            // 'databases' instead of 'database'
            failsToParse
          }

          test(s"$command $privilege ON DEFAULT DATABASE foo $preposition role") {
            // both default and database name
            failsToParse
          }

          test(s"$command $privilege ON DEFAULT DATABASE * $preposition role") {
            // both default and *
            failsToParse
          }
      }

      // Dropping instead of creating name management privileges

      test(s"$command DROP NEW LABEL ON DATABASE * $preposition role") {
        failsToParse
      }

      test(s"$command DROP NEW TYPE ON DATABASE * $preposition role") {
        failsToParse
      }

      test(s"$command DROP NEW NAME ON DATABASE * $preposition role") {
        failsToParse
      }

      test(s"$command DROP LABEL ON DATABASE * $preposition role") {
        failsToParse
      }

      test(s"$command DROP TYPE ON DATABASE * $preposition role") {
        failsToParse
      }

      test(s"$command DROP NAME ON DATABASE * $preposition role") {
        failsToParse
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
    case (command: String, preposition: String, privilegeFunc: transactionPrivilegeFunc) =>

      test(s"$command SHOW TRANSACTION (*) ON DATABASE * $preposition role") {
        yields(privilegeFunc(ast.ShowTransactionAction, List(ast.AllGraphsScope() _), ast.UserAllQualifier() _, Seq(literal("role"))))
      }

      test(s"$command SHOW TRANSACTIONS (*) ON DATABASES foo $preposition role1, role2") {
        yields(privilegeFunc(ast.ShowTransactionAction, List(ast.NamedGraphScope(literal("foo")) _), ast.UserAllQualifier() _, Seq(literal("role1"), literal("role2"))))
      }

      test(s"$command SHOW TRANSACTIONS (*) ON DATABASES $$foo $preposition $$role1, $$role2") {
        yields(privilegeFunc(ast.ShowTransactionAction, List(ast.NamedGraphScope(param("foo")) _), ast.UserAllQualifier() _, Seq(param("role1"), param("role2"))))
      }

      test(s"$command SHOW TRANSACTION (user) ON DEFAULT DATABASE $preposition role") {
        yields(privilegeFunc(ast.ShowTransactionAction, List(ast.DefaultDatabaseScope() _), ast.UsersQualifier(Seq(literal("user"))) _, Seq(literal("role"))))
      }

      test(s"$command SHOW TRANSACTION ($$user) ON DEFAULT DATABASE $preposition role") {
        yields(privilegeFunc(ast.ShowTransactionAction, List(ast.DefaultDatabaseScope() _), ast.UsersQualifier(Seq(param("user"))) _, Seq(literal("role"))))
      }

      test(s"$command SHOW TRANSACTIONS (user1,user2) ON DATABASES * $preposition role1, role2") {
        yields(privilegeFunc(ast.ShowTransactionAction, List(ast.AllGraphsScope() _), ast.UsersQualifier(Seq(literal("user1"), literal("user2"))) _, Seq(literal("role1"), literal("role2"))))
      }

      test(s"$command SHOW TRANSACTIONS ON DATABASES * $preposition role") {
        yields(privilegeFunc(ast.ShowTransactionAction, List(ast.AllGraphsScope() _), ast.UserAllQualifier() _, Seq(literal("role"))))
      }

      test(s"$command SHOW TRANSACTION ON DATABASE foo, bar $preposition role") {
        yields(privilegeFunc(ast.ShowTransactionAction, List(ast.NamedGraphScope(literal("foo")) _, ast.NamedGraphScope(literal("bar")) _), ast.UserAllQualifier() _, Seq(literal("role"))))
      }

      test(s"$command SHOW TRANSACTION (user) ON DATABASES foo, $$bar $preposition role") {
        yields(privilegeFunc(ast.ShowTransactionAction, List(ast.NamedGraphScope(literal("foo")) _, ast.NamedGraphScope(param("bar")) _), ast.UsersQualifier(Seq(literal("user"))) _, Seq(literal("role"))))
      }

      test(s"$command TERMINATE TRANSACTION (*) ON DATABASE * $preposition $$role") {
        yields(privilegeFunc(ast.TerminateTransactionAction, List(ast.AllGraphsScope() _), ast.UserAllQualifier() _, Seq(param("role"))))
      }

      test(s"$command TERMINATE TRANSACTIONS (*) ON DATABASES foo $preposition role1, role2") {
        yields(privilegeFunc(ast.TerminateTransactionAction, List(ast.NamedGraphScope(literal("foo")) _), ast.UserAllQualifier() _, Seq(literal("role1"), literal("role2"))))
      }

      test(s"$command TERMINATE TRANSACTIONS (*) ON DATABASES $$foo $preposition role") {
        yields(privilegeFunc(ast.TerminateTransactionAction, List(ast.NamedGraphScope(param("foo")) _), ast.UserAllQualifier() _, Seq(literal("role"))))
      }

      test(s"$command TERMINATE TRANSACTION (user) ON DEFAULT DATABASE $preposition role") {
        yields(privilegeFunc(ast.TerminateTransactionAction, List(ast.DefaultDatabaseScope() _), ast.UsersQualifier(Seq(literal("user"))) _, Seq(literal("role"))))
      }

      test(s"$command TERMINATE TRANSACTIONS (user1,user2) ON DATABASES * $preposition role1, role2") {
        yields(privilegeFunc(ast.TerminateTransactionAction, List(ast.AllGraphsScope() _), ast.UsersQualifier(Seq(literal("user1"), literal("user2"))) _, Seq(literal("role1"), literal("role2"))))
      }

      test(s"$command TERMINATE TRANSACTIONS ($$user1,$$user2) ON DATABASES * $preposition role1, role2") {
        yields(privilegeFunc(ast.TerminateTransactionAction, List(ast.AllGraphsScope() _), ast.UsersQualifier(Seq(param("user1"), param("user2"))) _, Seq(literal("role1"), literal("role2"))))
      }

      test(s"$command TERMINATE TRANSACTIONS ON DATABASES * $preposition role") {
        yields(privilegeFunc(ast.TerminateTransactionAction, List(ast.AllGraphsScope() _), ast.UserAllQualifier() _, Seq(literal("role"))))
      }

      test(s"$command TERMINATE TRANSACTION ON DATABASE foo, bar $preposition role") {
        yields(privilegeFunc(ast.TerminateTransactionAction, List(ast.NamedGraphScope(literal("foo"))_, ast.NamedGraphScope(literal("bar")) _), ast.UserAllQualifier() _, Seq(literal("role"))))
      }

      test(s"$command TERMINATE TRANSACTION (user) ON DATABASES foo, $$bar $preposition role") {
        yields(privilegeFunc(ast.TerminateTransactionAction, List(ast.NamedGraphScope(literal("foo"))_, ast.NamedGraphScope(param("bar")) _), ast.UsersQualifier(Seq(literal("user"))) _, Seq(literal("role"))))
      }

      test(s"$command TRANSACTION ON DATABASES * $preposition role1, role2") {
        yields(privilegeFunc(ast.AllTransactionActions, List(ast.AllGraphsScope() _), ast.UserAllQualifier() _, Seq(literal("role1"), literal("role2"))))
      }

      test(s"$command TRANSACTION (*) ON DATABASES foo $preposition role1, $$role2") {
        yields(privilegeFunc(ast.AllTransactionActions, List(ast.NamedGraphScope(literal("foo")) _), ast.UserAllQualifier() _, Seq(literal("role1"), param("role2"))))
      }

      test(s"$command TRANSACTION (*) ON DATABASES $$foo $preposition role1, role2") {
        yields(privilegeFunc(ast.AllTransactionActions, List(ast.NamedGraphScope(param("foo")) _), ast.UserAllQualifier() _, Seq(literal("role1"), literal("role2"))))
      }

      test(s"$command TRANSACTION (user) ON DATABASES * $preposition role") {
        yields(privilegeFunc(ast.AllTransactionActions, List(ast.AllGraphsScope() _), ast.UsersQualifier(Seq(literal("user"))) _, Seq(literal("role"))))
      }

      test(s"$command TRANSACTION ON DATABASE foo, bar $preposition role") {
        yields(privilegeFunc(ast.AllTransactionActions, List(ast.NamedGraphScope(literal("foo")) _, ast.NamedGraphScope(literal("bar")) _), ast.UserAllQualifier() _, Seq(literal("role"))))
      }

      test(s"$command TRANSACTION (user) ON DATABASES foo, $$bar $preposition role") {
        yields(privilegeFunc(ast.AllTransactionActions, List(ast.NamedGraphScope(literal("foo"))_, ast.NamedGraphScope(param("bar")) _), ast.UsersQualifier(Seq(literal("user"))) _, Seq(literal("role"))))
      }

      test(s"$command TRANSACTION ON DATABASE foo, * $preposition role") {
        failsToParse
      }

      test(s"$command TRANSACTION ON DATABASE *, foo $preposition role") {
        failsToParse
      }

      test(s"$command TRANSACTIONS ON DATABASES * $preposition role") {
        failsToParse
      }

      test(s"$command TRANSACTIONS (*) ON DATABASES * $preposition role") {
        failsToParse
      }

      test(s"$command TRANSACTION MANAGEMENT ON DEFAULT DATABASE $preposition role") {
        yields(privilegeFunc(ast.AllTransactionActions, List(ast.DefaultDatabaseScope() _), ast.UserAllQualifier() _, Seq(literal("role"))))
      }

      test(s"$command TRANSACTION MANAGEMENT (*) ON DATABASE * $preposition role") {
        yields(privilegeFunc(ast.AllTransactionActions, List(ast.AllGraphsScope() _), ast.UserAllQualifier() _, Seq(literal("role"))))
      }

      test(s"$command TRANSACTION MANAGEMENT (user) ON DATABASES * $preposition role") {
        yields(privilegeFunc(ast.AllTransactionActions, List(ast.AllGraphsScope() _), ast.UsersQualifier(Seq(literal("user"))) _, Seq(literal("role"))))
      }

      test(s"$command TRANSACTION MANAGEMENT (user1, $$user2) ON DATABASES * $preposition role") {
        yields(privilegeFunc(ast.AllTransactionActions, List(ast.AllGraphsScope() _), ast.UsersQualifier(Seq(literal("user1"), param("user2"))) _, Seq(literal("role"))))
      }

      test(s"$command TRANSACTION MANAGEMENT ON DATABASE foo, bar $preposition role") {
        yields(privilegeFunc(ast.AllTransactionActions, List(ast.NamedGraphScope(literal("foo"))_, ast.NamedGraphScope(literal("bar")) _), ast.UserAllQualifier() _, Seq(literal("role"))))
      }

      test(s"$command TRANSACTION MANAGEMENT (user) ON DATABASES foo, $$bar $preposition role") {
        yields(privilegeFunc(ast.AllTransactionActions, List(ast.NamedGraphScope(literal("foo"))_, ast.NamedGraphScope(param("bar")) _), ast.UsersQualifier(Seq(literal("user"))) _, Seq(literal("role"))))
      }

      test(s"$command TRANSACTIONS MANAGEMENT ON DATABASES * $preposition role") {
        failsToParse
      }

      test(s"$command TRANSACTIONS MANAGEMENT (*) ON DATABASES * $preposition role") {
        failsToParse
      }
  }
}
