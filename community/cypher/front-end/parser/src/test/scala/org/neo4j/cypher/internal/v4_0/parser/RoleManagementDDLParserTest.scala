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
package org.neo4j.cypher.internal.v4_0.parser

import org.neo4j.cypher.internal.v4_0.ast

class RoleManagementDDLParserTest extends DDLParserTestBase {

  //  Showing roles

  test("SHOW ROLES") {
    yields(ast.ShowRoles(withUsers = false, showAll = true))
  }

  test("CATALOG SHOW ALL ROLES") {
    yields(ast.ShowRoles(withUsers = false, showAll = true))
  }

  test("CATALOG SHOW POPULATED ROLES") {
    yields(ast.ShowRoles(withUsers = false, showAll = false))
  }

  test("SHOW ROLES WITH USERS") {
    yields(ast.ShowRoles(withUsers = true, showAll = true))
  }

  test("CATALOG SHOW ALL ROLES WITH USERS") {
    yields(ast.ShowRoles(withUsers = true, showAll = true))
  }

  test("SHOW POPULATED ROLES WITH USERS") {
    yields(ast.ShowRoles(withUsers = true, showAll = false))
  }

  test("CATALOG SHOW ROLE") {
    failsToParse
  }

  test("SHOW ALL ROLE") {
    failsToParse
  }

  test("SHOW POPULATED ROLE") {
    failsToParse
  }

  test("SHOW ROLE WITH USERS") {
    failsToParse
  }

  test("CATALOG SHOW ROLES WITH USER") {
    failsToParse
  }

  test("SHOW ROLE WITH USER") {
    failsToParse
  }

  test("SHOW ALL ROLE WITH USERS") {
    failsToParse
  }

  test("SHOW ALL ROLES WITH USER") {
    failsToParse
  }

  test("SHOW ALL ROLE WITH USER") {
    failsToParse
  }

  test("CATALOG SHOW POPULATED ROLE WITH USERS") {
    failsToParse
  }

  test("CATALOG SHOW POPULATED ROLES WITH USER") {
    failsToParse
  }

  test("CATALOG SHOW POPULATED ROLE WITH USER") {
    failsToParse
  }

  //  Creating role

  test("CREATE ROLE foo") {
    yields(ast.CreateRole("foo", None))
  }

  test("CATALOG CREATE ROLE `foo`") {
    yields(ast.CreateRole("foo", None))
  }

  test("CREATE ROLE ``") {
    yields(ast.CreateRole("", None))
  }

  test("CREATE ROLE foo AS COPY OF bar") {
    yields(ast.CreateRole("foo", Some("bar")))
  }

  test("CREATE ROLE foo AS COPY OF ``") {
    yields(ast.CreateRole("foo", Some("")))
  }

  test("CREATE ROLE `` AS COPY OF bar") {
    yields(ast.CreateRole("", Some("bar")))
  }

  test("CATALOG CREATE ROLE \"foo\"") {
    failsToParse
  }

  test("CREATE ROLE f%o") {
    failsToParse
  }

  test("CREATE ROLE foo AS COPY OF") {
    failsToParse
  }

  //  Dropping role

  test("DROP ROLE foo") {
    yields(ast.DropRole("foo"))
  }

  test("DROP ROLE ``") {
    yields(ast.DropRole(""))
  }

  test("DROP ROLE ") {
    failsToParse
  }
}
