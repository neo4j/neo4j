/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.v4_0.parser.privilege

import org.neo4j.cypher.internal.v4_0.ast
import org.neo4j.cypher.internal.v4_0.parser.AdministrationCommandParserTestBase

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
        ("DROP INDEX", ast.DropIndexAction),
        ("INDEX MANAGEMENT", ast.IndexManagementAction),
        ("CREATE CONSTRAINT", ast.CreateConstraintAction),
        ("DROP CONSTRAINT", ast.DropConstraintAction),
        ("CONSTRAINT MANAGEMENT", ast.ConstraintManagementAction),
        ("CREATE NEW LABEL", ast.CreateNodeLabelAction),
        ("CREATE NEW NODE LABEL", ast.CreateNodeLabelAction),
        ("CREATE NEW TYPE", ast.CreateRelationshipTypeAction),
        ("CREATE NEW RELATIONSHIP TYPE", ast.CreateRelationshipTypeAction),
        ("CREATE NEW NAME", ast.CreatePropertyKeyAction),
        ("CREATE NEW PROPERTY NAME", ast.CreatePropertyKeyAction),
        ("NAME MANAGEMENT", ast.TokenManagementAction),
        ("ALL", ast.AllDatabaseAction),
        ("ALL PRIVILEGES", ast.AllDatabaseAction),
        ("ALL DATABASE PRIVILEGES", ast.AllDatabaseAction)
      ).foreach {
        case (privilege: String, action: ast.DatabaseAction) =>

          test(s"$command $privilege ON DATABASE * $preposition role") {
            yields(privilegeFunc(action, ast.AllGraphsScope() _, Seq("role")))
          }

          test(s"$command $privilege ON DATABASES * $preposition role") {
            yields(privilegeFunc(action, ast.AllGraphsScope() _, Seq("role")))
          }

          test(s"$command $privilege ON DATABASE * $preposition role1, role2") {
            yields(privilegeFunc(action, ast.AllGraphsScope() _, Seq("role1", "role2")))
          }

          test(s"$command $privilege ON DATABASE foo $preposition role") {
            yields(privilegeFunc(action, ast.NamedGraphScope("foo") _, Seq("role")))
          }

          test(s"$command $privilege ON DATABASE foo $preposition role1, role2") {
            yields(privilegeFunc(action, ast.NamedGraphScope("foo") _, Seq("role1", "role2")))
          }

          test(s"$command $privilege ON GRAPH * $preposition role") {
            failsToParse
          }

          test(s"$command $privilege ON DATABASES foo, bar $preposition role") {
            failsToParse
          }
      }
  }
}
