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
package org.neo4j.cypher.internal.v4_0.parser.privilege

import org.neo4j.cypher.internal.v4_0.ast
import org.neo4j.cypher.internal.v4_0.parser.AdministrationCommandParserTestBase

class DbmsPrivilegeAdministrationCommandParserTest extends AdministrationCommandParserTestBase {

  def privilegeTests(command: String, preposition: String, privilegeFunc: dbmsPrivilegeFunc): Unit = {
    Seq(
      ("ROLE MANAGEMENT", ast.AllRoleActions),
      ("CREATE ROLE", ast.CreateRoleAction),
      ("DROP ROLE", ast.DropRoleAction),
      ("SHOW ROLE", ast.ShowRoleAction),
      ("ASSIGN ROLE", ast.AssignRoleAction),
      ("REMOVE ROLE", ast.RemoveRoleAction)
    ).foreach {
      case (privilege: String, action: ast.AdminAction) =>

        test(s"$command $privilege ON DBMS $preposition role") {
          yields(privilegeFunc(action, Seq("role")))
        }

        test(s"$command $privilege ON DBMS $preposition role1, role2") {
          yields(privilegeFunc(action, Seq("role1", "role2")))
        }

        test(s"$command $privilege ON DBMS $preposition `r:ole`") {
          yields(privilegeFunc(action, Seq("r:ole")))
        }

        test( s"dbmsPrivilegeParsingErrors$command $privilege $preposition") {
          assertFails(s"$command $privilege ON DATABASE $preposition role")
          assertFails(s"$command $privilege DBMS $preposition role")
          assertFails(s"$command $privilege ON $preposition role")
          assertFails(s"$command $privilege ON DBMS $preposition r:ole")
          assertFails(s"$command $privilege ON DBMS $preposition")
          assertFails(s"$command $privilege ON DBMS")
        }
    }
  }
}
