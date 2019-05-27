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
import org.neo4j.cypher.internal.v4_0.ast._
import org.neo4j.cypher.internal.v4_0.expressions.{Parameter => Param}
import org.neo4j.cypher.internal.v4_0.util.InputPosition
import org.neo4j.cypher.internal.v4_0.util.symbols._
import org.parboiled.scala.Rule1

class SecurityDDLParserTest
  extends ParserAstTest[ast.Statement] with Statement with AstConstructionTestSupport {

  implicit val parser: Rule1[ast.Statement] = Statement

  // User Management Commands
  //  Showing user

  test("SHOW USERS") {
    yields(ast.ShowUsers())
  }

  test("CATALOG SHOW USERS") {
    yields(ast.ShowUsers())
  }

  test("CATALOG SHOW USER") {
    failsToParse
  }

  //  Creating user

  test("CATALOG CREATE USER foo SET PASSWORD 'password'") {
    yields(ast.CreateUser("foo", Some("password"), None, requirePasswordChange = true, suspended = false))
  }

  test("CREATE USER foo SET PASSwORD ''") {
    yields(ast.CreateUser("foo", Some(""), None, requirePasswordChange = true, suspended = false))
  }

  test("CREATE uSER foo SET PASSWORD $password") {
    yields(ast.CreateUser("foo", None, Some(Param("password", CTAny)(_)), requirePasswordChange = true, suspended = false))
  }

  test("CREATE USER \"foo\" SET PASSwORD 'password'") {
    failsToParse
  }

  test("CREATE USER foo SET PASSwORD 'passwordString'+$passwordParam") {
    failsToParse
  }

  test("CREATE USER `` SET PASSwORD 'password'") {
    failsToParse
  }

  test("CREATE USER `foo` SET PASSwORD 'password'") {
    yields(ast.CreateUser("foo", Some("password"), None, requirePasswordChange = true, suspended = false))
  }

  test("CREATE USER !#\"~ SeT PASSWORD 'password'") {
    failsToParse
  }

  test("CREATE USER `!#\"~` SeT PASSWORD 'password'") {
    yields(ast.CreateUser("!#\"~", Some("password"), None, requirePasswordChange = true, suspended = false))
  }

  test("CREATE USER foo SeT PASSWORD 'pasS5Wor%d'") {
    yields(ast.CreateUser("foo", Some("pasS5Wor%d"), None, requirePasswordChange = true, suspended = false))
  }

  test("CREaTE USER foo SET PASSWORD 'password' CHANGE REQUIRED") {
    yields(ast.CreateUser("foo", Some("password"), None, requirePasswordChange = true, suspended = false))
  }

  test("CATALOG CREATE USER foo SET PASSWORD $password CHANGE REQUIRED") {
    yields(ast.CreateUser("foo", None, Some(Param("password", CTAny)(_)), requirePasswordChange = true, suspended = false))
  }

  test("CREATE USER foo SET PASSWORD 'password' SET PASSWORD CHANGE required") {
    yields(ast.CreateUser("foo", Some("password"), None, requirePasswordChange = true, suspended = false))
  }

  test("CREATE USER foo SET PASSWORD 'password' CHAngE NOT REQUIRED") {
    yields(ast.CreateUser("foo", Some("password"), None, requirePasswordChange = false, suspended = false))
  }

  test("CREATE USER foo SET PASSWORD 'password' SET PASSWORD CHANGE NOT REQUIRED") {
    yields(ast.CreateUser("foo", Some("password"), None, requirePasswordChange = false, suspended = false))
  }

  test("CREATE USER foo SET PASSWORD $password SET  PASSWORD CHANGE NOT REQUIRED") {
    yields(ast.CreateUser("foo", None, Some(Param("password", CTAny)(_)), requirePasswordChange = false, suspended = false))
  }

  test("CATALOG CREATE USER foo SET PASSWORD 'password' SET STATUS SUSPENDed") {
    yields(ast.CreateUser("foo", Some("password"), None, requirePasswordChange = true, suspended = true))
  }

  test("CREATE USER foo SET PASSWORD 'password' SET STATUS ACtiVE") {
    yields(ast.CreateUser("foo", Some("password"), None, requirePasswordChange = true, suspended = false))
  }

  test("CREATE USER foo SET PASSWORD 'password' SET PASSWORD CHANGE NOT REQUIRED SET   STATuS SUSPENDED") {
    yields(ast.CreateUser("foo", Some("password"), None, requirePasswordChange = false, suspended = true))
  }

  test("CREATE USER foo SET PASSWORD $password CHANGE REQUIRED SET STATUS SUSPENDED") {
    yields(ast.CreateUser("foo", None, Some(Param("password", CTAny)(_)), requirePasswordChange = true, suspended = true))
  }

  test("CREATE USER foo") {
    failsToParse
  }

  test("CREATE USER foo SET PASSWORD") {
    failsToParse
  }

  test("CREATE USER foo SET PASSWORD null CHANGE REQUIRED") {
    failsToParse
  }

  test("CATALOG CREATE USER fo,o SET PASSWORD 'password'") {
    failsToParse
  }

  test("CREATE USER f:oo SET PASSWORD 'password'") {
    failsToParse
  }

  test("CREATE USER `f:oo` SET PASSWORD 'password'") {
    failsToParse
  }

  test("CREATE USER foo PASSWORD 'password'") {
    failsToParse
  }

  test("CREATE USER foo SET PASSWORD 'password' SET STAUS ACTIVE") {
    failsToParse
  }

  test("CREATE USER foo SET PASSWORD 'password' SET STATUS IMAGINARY") {
    failsToParse
  }

  test("CREATE USER foo SET PASSWORD CHANGE REQUIRED") {
    failsToParse
  }

  test("CREATE USER foo SET STATUS SUSPENDED") {
    failsToParse
  }

  test("CREATE USER foo SET PASSWORD CHANGE REQUIRED SET STATUS ACTIVE") {
    failsToParse
  }

  //  Dropping user

  test("DROP USER foo") {
    yields(ast.DropUser("foo"))
  }

  test("DROP USER ``") {
    yields(ast.DropUser(""))
  }

  test("DROP USER `f:oo`") {
    yields(ast.DropUser("f:oo"))
  }

  test("DROP USER ") {
    failsToParse
  }

  //  Altering user

  test("CATALOG ALTER USER foo SET PASSWORD 'password'") {
    yields(ast.AlterUser("foo", Some("password"), None, None, None))
  }

  test("ALTER USER `` SET PASSWORD 'password'") {
    yields(ast.AlterUser("", Some("password"), None, None, None))
  }

  test("ALTER USER `f:oo` SET PASSWORD 'password'") {
    yields(ast.AlterUser("f:oo", Some("password"), None, None, None))
  }

  test("ALTER USER foo SET PASSWORD ''") {
    yields(ast.AlterUser("foo", Some(""), None, None, None))
  }

  test("ALTER USER foo SET PASSWORD $password") {
    yields(ast.AlterUser("foo", None, Some(Param("password", CTAny)(_)), None, None))
  }

  test("CATALOG ALTER USER foo SET PASSWORD CHANGE REQUIRED") {
    yields(ast.AlterUser("foo", None, None, requirePasswordChange = Some(true), None))
  }

  test("CATALOG ALTER USER foo SET PASSWORD CHANGE NOT REQUIRED") {
    yields(ast.AlterUser("foo", None, None, requirePasswordChange = Some(false), None))
  }

  test("ALTER USER foo SET STATUS SUSPENDED") {
    yields(ast.AlterUser("foo", None, None, None, suspended = Some(true)))
  }

  test("ALTER USER foo SET STATUS ACTIVE") {
    yields(ast.AlterUser("foo", None, None, None, suspended = Some(false)))
  }

  test("CATALOG ALTER USER foo SET PASSWORD 'password' CHANGE REQUIRED") {
    yields(ast.AlterUser("foo", Some("password"), None, requirePasswordChange = Some(true), None))
  }

  test("ALTER USER foo SET PASSWORD $password SET PASSWORD CHANGE NOT REQUIRED") {
    yields(ast.AlterUser("foo", None, Some(Param("password", CTAny)(_)), requirePasswordChange = Some(false), None))
  }

  test("CATALOG ALTER USER foo SET PASSWORD 'password' SET STATUS ACTIVE") {
    yields(ast.AlterUser("foo", Some("password"), None, None, suspended = Some(false)))
  }

  test("CATALOG ALTER USER foo SET PASSWORD CHANGE NOT REQUIRED SET STATUS ACTIVE") {
    yields(ast.AlterUser("foo", None, None, requirePasswordChange = Some(false), suspended = Some(false)))
  }

  test("ALTER USER foo SET PASSWORD $password SET PASSWORD CHANGE NOT REQUIRED SET STATUS SUSPENDED") {
    yields(ast.AlterUser("foo", None, Some(Param("password", CTAny)(_)), requirePasswordChange = Some(false), suspended = Some(true)))
  }

  test("ALTER USER foo SET PASSWORD") {
    failsToParse
  }

  test("ALTER USER foo SET STATUS") {
    failsToParse
  }

  test("ALTER USER foo SET PASSWORD null") {
    failsToParse
  }

  test("ALTER USER foo SET PASSWORD 123") {
    failsToParse
  }

  test("ALTER USER foo SET PASSWORD 'password' SET PASSWORD SET STATUS ACTIVE") {
    failsToParse
  }

  test("ALTER USER foo SET PASSWORD STATUS ACTIVE") {
    failsToParse
  }

  test("CATALOG ALTER USER foo SET PASSWORD 'password' SET STATUS IMAGINARY") {
    failsToParse
  }

  // Role Management Commands
  //  Showing roles

  test("SHOW ROLES") {
    yields(ast.ShowRoles(withUsers = false, showAll = true))
  }

  test("CATALOG SHOW ROLE") {
    failsToParse
  }

  test("CATALOG SHOW ALL ROLES") {
    yields(ast.ShowRoles(withUsers = false, showAll = true))
  }

  test("SHOW ALL ROLE") {
    failsToParse
  }

  test("CATALOG SHOW POPULATED ROLES") {
    yields(ast.ShowRoles(withUsers = false, showAll = false))
  }

  test("SHOW POPULATED ROLE") {
    failsToParse
  }

  test("SHOW ROLES WITH USERS") {
    yields(ast.ShowRoles(withUsers = true, showAll = true))
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

  test("CATALOG SHOW ALL ROLES WITH USERS") {
    yields(ast.ShowRoles(withUsers = true, showAll = true))
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

  test("SHOW POPULATED ROLES WITH USERS") {
    yields(ast.ShowRoles(withUsers = true, showAll = false))
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

  test("CATALOG CREATE ROLE \"foo\"") {
    failsToParse
  }

  test("CATALOG CREATE ROLE `foo`") {
    yields(ast.CreateRole("foo", None))
  }

  test("CREATE ROLE f%o") {
    failsToParse
  }

  test("CREATE ROLE ``") {
    failsToParse
  }

  test("CREATE ROLE foo AS COPY OF bar") {
    yields(ast.CreateRole("foo", Some("bar")))
  }

  test("CREATE ROLE foo AS COPY OF ``") {
    yields(ast.CreateRole("foo", Some("")))
  }

  test("CREATE ROLE foo AS COPY OF") {
    failsToParse
  }

  test("CREATE ROLE `` AS COPY OF bar") {
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

  // Privilege Management Commands
  //  Granting roles to users

  test("GRANT ROLE foo TO abc") {
    yields(ast.GrantRolesToUsers(Seq("foo"), Seq("abc")))
  }

  test("CATALOG GRANT ROLE foo TO abc") {
    yields(ast.GrantRolesToUsers(Seq("foo"), Seq("abc")))
  }

  test("GRANT ROLE foo, baz TO bar") {
    yields(ast.GrantRolesToUsers(Seq("foo", "baz"), Seq("bar")))
  }

  test("GRANT ROLE foo TO bar, baz") {
    yields(ast.GrantRolesToUsers(Seq("foo"), Seq("bar", "baz")))
  }

  test("GRANT ROLE foo,bla,roo TO bar, baz,abc,  def") {
    yields(ast.GrantRolesToUsers(Seq("foo", "bla", "roo"), Seq("bar", "baz", "abc", "def")))
  }

  test("GRANT ROLE `fo:o` TO bar") {
    yields(ast.GrantRolesToUsers(Seq("fo:o"), Seq("bar")))
  }

  test("GRANT ROLE fo:o TO bar") {
    failsToParse
  }

  test("GRANT ROLE foo TO `b:ar`") {
    yields(ast.GrantRolesToUsers(Seq("foo"), Seq("b:ar")))
  }

  test("GRANT ROLE foo TO b:ar") {
    failsToParse
  }

  test("GRANT ROLE foo") {
    failsToParse
  }

  test("GRANT ROLE TO bar") {
    failsToParse
  }

  test("GRANT ROLE") {
    failsToParse
  }

  test("GRANT ROLES foo TO abc") {
    yields(ast.GrantRolesToUsers(Seq("foo"), Seq("abc")))
  }

  test("GRANT ROLES foo, baz TO bar") {
    yields(ast.GrantRolesToUsers(Seq("foo", "baz"), Seq("bar")))
  }

  test("GRANT ROLES foo TO bar, baz") {
    yields(ast.GrantRolesToUsers(Seq("foo"), Seq("bar", "baz")))
  }

  test("GRANT ROLES foo,bla,roo TO bar, baz,abc,  def") {
    yields(ast.GrantRolesToUsers(Seq("foo", "bla", "roo"), Seq("bar", "baz", "abc", "def")))
  }

  test("GRANT ROLES `$f00`,bar TO abc,`$a&c`") {
    yields(ast.GrantRolesToUsers(Seq("$f00", "bar"), Seq("abc", "$a&c")))
  }

  test("GRANT ROLES $f00 TO abc") {
    failsToParse
  }

  test("GRANT ROLES `fo:o` TO bar") {
    yields(ast.GrantRolesToUsers(Seq("fo:o"), Seq("bar")))
  }

  test("GRANT ROLES fo:o TO bar") {
    failsToParse
  }

  test("GRANT ROLES foo TO `b:ar`") {
    yields(ast.GrantRolesToUsers(Seq("foo"), Seq("b:ar")))
  }

  test("GRANT ROLES foo TO b:ar") {
    failsToParse
  }

  test("GRANT ROLES foo") {
    failsToParse
  }

  test("GRANT ROLES foo TO") {
    failsToParse
  }

  test("GRANT ROLES foo FROM abc") {
    failsToParse
  }

  test("GRANT ROLES TO bar") {
    failsToParse
  }

  test("GRANT ROLES") {
    failsToParse
  }

  //  Revoking roles from users

  test("CATALOG REVOKE ROLE foo FROM abc") {
    yields(ast.RevokeRolesFromUsers(Seq("foo"), Seq("abc")))
  }

  test("REVOKE ROLE foo FROM abc") {
    yields(ast.RevokeRolesFromUsers(Seq("foo"), Seq("abc")))
  }

  test("REVOKE ROLES foo FROM abc") {
    yields(ast.RevokeRolesFromUsers(Seq("foo"), Seq("abc")))
  }

  test("REVOKE ROLES foo, bar FROM abc") {
    yields(ast.RevokeRolesFromUsers(Seq("foo", "bar"), Seq("abc")))
  }

  test("REVOKE ROLES foo FROM abc, def") {
    yields(ast.RevokeRolesFromUsers(Seq("foo"), Seq("abc", "def")))
  }

  test("REVOKE ROLES `$f00`,bar FROM abc,`$a&c`") {
    yields(ast.RevokeRolesFromUsers(Seq("$f00", "bar"), Seq("abc", "$a&c")))
  }

  test("REVOKE ROLES $f00 FROM abc") {
    failsToParse
  }

  test("REVOKE ROLE $f00 FROM abc") {
    failsToParse
  }

  test("REVOKE ROLES foo") {
    failsToParse
  }

  test("REVOKE ROLE foo") {
    failsToParse
  }

  test("REVOKE ROLES foo FROM") {
    failsToParse
  }

  test("REVOKE ROLE foo FROM") {
    failsToParse
  }

  test("REVOKE ROLES foo TO abc") {
    failsToParse
  }

  test("REVOKE ROLES FROM abc") {
    failsToParse
  }

  test("REVOKE ROLE FROM abc") {
    failsToParse
  }

  test("REVOKE ROLES") {
    failsToParse
  }

  test("REVOKE ROLE") {
    failsToParse
  }

  //  Showing privileges

  test("SHOW PRIVILEGES") {
    yields(ast.ShowPrivileges(ast.ShowAllPrivileges() _))
  }

  test("catalog show privileges") {
    yields(ast.ShowPrivileges(ast.ShowAllPrivileges() _))
  }

  test("SHOW PRIVILEGE") {
    failsToParse
  }

  test("SHOW PRIVILAGES") {
    failsToParse
  }

  test("SHOW PRIVELAGES") {
    failsToParse
  }

  test("SHOW privalages") {
    failsToParse
  }

  test("SHOW ALL PRIVILEGES") {
    yields(ast.ShowPrivileges(ast.ShowAllPrivileges() _))
  }

  test("SHOW USER PRIVILEGES") {
    failsToParse
  }

  test("SHOW ALL USER user PRIVILEGES") {
    failsToParse
  }

  test("SHOW USER user PRIVILEGES") {
    yields(ast.ShowPrivileges(ast.ShowUserPrivileges("user") _))
  }

  test("SHOW USER us%er PRIVILEGES") {
    failsToParse
  }

  test("SHOW USER `us%er` PRIVILEGES") {
    yields(ast.ShowPrivileges(ast.ShowUserPrivileges("us%er") _))
  }

  test("SHOW ROLE PRIVILEGES") {
    failsToParse
  }

  test("SHOW ALL ROLE role PRIVILEGES") {
    failsToParse
  }

  test("SHOW ROLE role PRIVILEGES") {
    yields(ast.ShowPrivileges(ast.ShowRolePrivileges("role") _))
  }

  test("SHOW ROLE ro%le PRIVILEGES") {
    failsToParse
  }

  test("SHOW ROLE `ro%le` PRIVILEGES") {
    yields(ast.ShowPrivileges(ast.ShowRolePrivileges("ro%le") _))
  }

  //  Granting traverse to role

  test("GRANT TRAVERSE GRAPH * NODES * (*) TO role") {
    failsToParse
  }

  test("GRANT TRAVERSE ON GRAPH * NODES * (*)") {
    failsToParse
  }

  test("GRANT TRAVERSE ON GRAPH * NODES * TO role") {
    yields(ast.GrantPrivilege.traverse(ast.AllGraphsScope() _, ast.AllQualifier() _, Seq("role")))
  }

  test("GRANT TRAVERSE ON GRAPH * TO role") {
    yields(ast.GrantPrivilege.traverse(ast.AllGraphsScope() _, ast.AllQualifier() _, Seq("role")))
  }

  test("GRANT TRAVERSE ON GRAPH foo TO role") {
    yields(ast.GrantPrivilege.traverse(ast.NamedGraphScope("foo") _, ast.AllQualifier() _, Seq("role")))
  }

  test("GRANT TRAVERSE ON GRAPHS foo TO role") {
    failsToParse
  }

  test("GRANT TRAVERSE ON GRAPH foo NODES * TO role") {
    yields(ast.GrantPrivilege.traverse(ast.NamedGraphScope("foo") _, ast.AllQualifier() _, Seq("role")))
  }

  test("GRANT TRAVERSE ON GRAPH foo NODES A TO role") {
    yields(ast.GrantPrivilege.traverse(ast.NamedGraphScope("foo") _, ast.LabelsQualifier(Seq("A")) _, Seq("role")))
  }

  test("GRANT TRAVERSE ON GRAPH `*` NODES A TO role") {
    yields(ast.GrantPrivilege.traverse(ast.NamedGraphScope("*") _, ast.LabelsQualifier(Seq("A")) _, Seq("role")))
  }

  test("GRANT TRAVERSE ON GRAPH * NODES * (*) TO role") {
    yields(ast.GrantPrivilege.traverse(ast.AllGraphsScope() _, ast.AllQualifier() _, Seq("role")))
  }

  test("GRANT TRAVERSE ON GRAPH foo NODES * (*) TO role") {
    yields(ast.GrantPrivilege.traverse(ast.NamedGraphScope("foo") _, ast.AllQualifier() _, Seq("role")))
  }

  test("GRANT TRAVERSE ON GRAPH * NODES A (*) TO role") {
    yields(ast.GrantPrivilege.traverse(ast.AllGraphsScope() _, ast.LabelsQualifier(Seq("A")) _, Seq("role")))
  }

  test("GRANT TRAVERSE ON GRAPH foo NODES A (*) TO role") {
    yields(ast.GrantPrivilege.traverse(ast.NamedGraphScope("foo") _, ast.LabelsQualifier(Seq("A")) _, Seq("role")))
  }

  test("GRANT TRAVERSE ON GRAPH foo NODES A (*) TO role1, role2") {
    yields(ast.GrantPrivilege.traverse(ast.NamedGraphScope("foo") _, ast.LabelsQualifier(Seq("A")) _, Seq("role1", "role2")))
  }

  test("GRANT TRAVERSE ON GRAPH foo NODES A, B (*) TO role1, role2") {
    yields(ast.GrantPrivilege.traverse(ast.NamedGraphScope("foo") _, ast.LabelsQualifier(Seq("A", "B")) _, Seq("role1", "role2")))
  }

  test("GRANT TRAVERSE ON GRAPH foo NODES A, B (*) TO role") {
    yields(ast.GrantPrivilege.traverse(ast.NamedGraphScope("foo") _, ast.LabelsQualifier(Seq("A", "B")) _, Seq("role")))
  }

  test("GRANT TRAVERSE ON GRAPH foo NODES `A B` (*) TO role") {
    yields(ast.GrantPrivilege.traverse(ast.NamedGraphScope("foo") _, ast.LabelsQualifier(Seq("A B")) _, Seq("role")))
  }

  test("GRANT TRAVERSE ON GRAPH foo NODES A B (*) TO role") {
    failsToParse
  }

  test("GRANT TRAVERSE ON GRAPH foo NODES A (foo) TO role") {
    failsToParse
  }

  test("GRANT TRAVERSE ON GRAPH NODES * TO role") {
    failsToParse
  }

  test("GRANT TRAVERSE ON GRAPH NODES A TO role") {
    failsToParse
  }

  test("GRANT TRAVERSE ON GRAPH NODES * (*) TO role") {
    failsToParse
  }

  test("GRANT TRAVERSE ON GRAPH NODES A (*) TO role") {
    failsToParse
  }

  test("GRANT TRAVERSE ON GRAPH foo NODES A (*) TO `r:ole`") {
    yields(ast.GrantPrivilege.traverse(ast.NamedGraphScope("foo") _, ast.LabelsQualifier(Seq("A")) _, Seq("r:ole")))
  }

  test("GRANT TRAVERSE ON GRAPH foo NODES A (*) TO r:ole") {
    failsToParse
  }

  test("GRANT TRAVERSE ON GRAPH `2foo` NODES A (*) TO role") {
    yields(ast.GrantPrivilege.traverse(ast.NamedGraphScope("2foo") _, ast.LabelsQualifier(Seq("A")) _, Seq("role")))
  }

  test("GRANT TRAVERSE ON GRAPH 2foo NODES A (*) TO role") {
    failsToParse
  }

  test("GRANT TRAVERSE ON GRAPH foo, baz NODES A (*) TO role") {
    failsToParse
  }

  //  Revoking traverse from role

  test("REVOKE TRAVERSE GRAPH * NODES * (*) FROM role") {
    failsToParse
  }

  test("REVOKE TRAVERSE ON GRAPH * NODES * (*)") {
    failsToParse
  }

  test("REVOKE TRAVERSE ON GRAPH * NODES * FROM role") {
    yields(ast.RevokePrivilege.traverse(ast.AllGraphsScope() _, ast.AllQualifier() _, Seq("role")))
  }

  test("REVOKE TRAVERSE ON GRAPH * FROM role") {
    yields(ast.RevokePrivilege.traverse(ast.AllGraphsScope() _, ast.AllQualifier() _, Seq("role")))
  }

  test("REVOKE TRAVERSE ON GRAPH foo FROM role") {
    yields(ast.RevokePrivilege.traverse(ast.NamedGraphScope("foo") _, ast.AllQualifier() _, Seq("role")))
  }

  test("REVOKE TRAVERSE ON GRAPH foo NODES * FROM role") {
    yields(ast.RevokePrivilege.traverse(ast.NamedGraphScope("foo") _, ast.AllQualifier() _, Seq("role")))
  }

  test("REVOKE TRAVERSE ON GRAPH foo NODES A FROM role") {
    yields(ast.RevokePrivilege.traverse(ast.NamedGraphScope("foo") _, ast.LabelsQualifier(Seq("A")) _, Seq("role")))
  }

  test("REVOKE TRAVERSE ON GRAPH * NODES * (*) FROM role") {
    yields(ast.RevokePrivilege.traverse(ast.AllGraphsScope() _, ast.AllQualifier() _, Seq("role")))
  }

  test("REVOKE TRAVERSE ON GRAPH foo NODES * (*) FROM role") {
    yields(ast.RevokePrivilege.traverse(ast.NamedGraphScope("foo") _, ast.AllQualifier() _, Seq("role")))
  }

  test("REVOKE TRAVERSE ON GRAPH * NODES A (*) FROM role") {
    yields(ast.RevokePrivilege.traverse(ast.AllGraphsScope() _, ast.LabelsQualifier(Seq("A")) _, Seq("role")))
  }

  test("REVOKE TRAVERSE ON GRAPH * NODES A FROM role") {
    yields(ast.RevokePrivilege.traverse(ast.AllGraphsScope() _, ast.LabelsQualifier(Seq("A")) _, Seq("role")))
  }

  test("REVOKE TRAVERSE ON GRAPH foo NODES A (*) FROM role") {
    yields(ast.RevokePrivilege.traverse(ast.NamedGraphScope("foo") _, ast.LabelsQualifier(Seq("A")) _, Seq("role")))
  }

  test("REVOKE TRAVERSE ON GRAPH foo NODES A, B (*) FROM role1, role2") {
    yields(ast.RevokePrivilege.traverse(ast.NamedGraphScope("foo") _, ast.LabelsQualifier(Seq("A", "B")) _, Seq("role1", "role2")))
  }

  test("REVOKE TRAVERSE ON GRAPH foo NODES A (foo) FROM role") {
    failsToParse
  }

  type grantOrRevokeFunc = (PrivilegeType, ActionResource, GraphScope, PrivilegeQualifier, Seq[String]) => InputPosition => ast.Statement

  def grant(p: PrivilegeType, a: ActionResource, s: GraphScope, q: PrivilegeQualifier, r: Seq[String]): InputPosition => ast.Statement = GrantPrivilege(p, a, s, q, r)

  def revoke(p: PrivilegeType, a: ActionResource, s: GraphScope, q: PrivilegeQualifier, r: Seq[String]): InputPosition => ast.Statement = RevokePrivilege(p, a, s, q, r)

  Seq(
    (ReadPrivilege()(pos), "GRANT", "TO", grant: grantOrRevokeFunc),
    (ReadPrivilege()(pos), "REVOKE", "FROM", revoke: grantOrRevokeFunc),
    (MatchPrivilege()(pos), "GRANT", "TO", grant: grantOrRevokeFunc),
    (MatchPrivilege()(pos), "REVOKE", "FROM", revoke: grantOrRevokeFunc)
  ).foreach {
    case (privilege: PrivilegeType, command: String, preposition: String, func: grantOrRevokeFunc) =>
      Seq(
        ("*", ast.AllResource()(pos), "*", ast.AllGraphsScope()(pos)),
        ("*", ast.AllResource()(pos), "foo", ast.NamedGraphScope("foo")(pos)),
        ("bar", ast.PropertiesResource(Seq("bar"))(pos), "*", ast.AllGraphsScope()(pos)),
        ("bar", ast.PropertiesResource(Seq("bar"))(pos), "foo", ast.NamedGraphScope("foo")(pos)),
        ("foo, bar", ast.PropertiesResource(Seq("foo", "bar"))(pos), "*", ast.AllGraphsScope()(pos)),
        ("foo, bar", ast.PropertiesResource(Seq("foo", "bar"))(pos), "foo", ast.NamedGraphScope("foo")(pos))
      ).foreach {
        case (properties: String, resource: ActionResource, dbName: String, graphScope: GraphScope) =>

          test(s"$command ${privilege.name} ($properties) ON GRAPH $dbName $preposition role") {
            yields(func(privilege, resource, graphScope, ast.AllQualifier() _, Seq("role")))
          }

          test(s"$command ${privilege.name} ($properties) ON GRAPH $dbName NODES * $preposition role") {
            yields(func(privilege, resource, graphScope, ast.AllQualifier() _, Seq("role")))
          }

          test(s"$command ${privilege.name} ($properties) ON GRAPH $dbName NODES * (*) $preposition role") {
            yields(func(privilege, resource, graphScope, ast.AllQualifier() _, Seq("role")))
          }

          test(s"$command ${privilege.name} ($properties) ON GRAPH $dbName NODES A $preposition role") {
            yields(func(privilege, resource, graphScope, ast.LabelsQualifier(Seq("A")) _, Seq("role")))
          }

          test(s"$command ${privilege.name} ($properties) ON GRAPH $dbName NODES A (*) $preposition role") {
            yields(func(privilege, resource, graphScope, ast.LabelsQualifier(Seq("A")) _, Seq("role")))
          }

          test(s"$command ${privilege.name} ($properties) ON GRAPH $dbName NODES `A B` (*) $preposition role") {
            yields(func(privilege, resource, graphScope, ast.LabelsQualifier(Seq("A B")) _, Seq("role")))
          }

          test(s"$command ${privilege.name} ($properties) ON GRAPH $dbName NODES A, B (*) $preposition role1, role2") {
            yields(func(privilege, resource, graphScope, ast.LabelsQualifier(Seq("A", "B")) _, Seq("role1", "role2")))
          }

          test(s"$command ${privilege.name} ($properties) ON GRAPH $dbName NODES * $preposition `r:ole`") {
            yields(func(privilege, resource, graphScope, ast.AllQualifier() _, Seq("r:ole")))
          }

          test(s"$command ${privilege.name} ($properties) ON GRAPH $dbName NODES `:A` (*) $preposition role") {
            yields(func(privilege, resource, graphScope, ast.LabelsQualifier(Seq(":A")) _, Seq("role")))
          }

          test(s"$command ${privilege.name} ($properties) GRAPH $dbName NODES * (*) $preposition role") {
            failsToParse
          }

          test(s"$command ${privilege.name} ($properties) GRAPH $dbName NODES A $preposition role") {
            failsToParse
          }

          test(s"$command ${privilege.name} ($properties) ON GRAPH $dbName NODES * (*)") {
            failsToParse
          }

          test(s"$command ${privilege.name} ($properties) ON GRAPH $dbName NODES A B (*) $preposition role") {
            failsToParse
          }

          test(s"$command ${privilege.name} ($properties) ON GRAPH $dbName NODES A (foo) $preposition role") {
            failsToParse
          }

          test(s"$command ${privilege.name} ($properties) ON GRAPH $dbName NODES * $preposition r:ole") {
            failsToParse
          }
      }

      test(s"$command ${privilege.name} (*) ON GRAPH `f:oo` NODES * $preposition role") {
        yields(func(privilege, ast.AllResource() _, ast.NamedGraphScope("f:oo") _, ast.AllQualifier() _, Seq("role")))
      }

      test(s"$command ${privilege.name} (bar) ON GRAPH `f:oo` NODES * $preposition role") {
        yields(func(privilege, ast.PropertiesResource(Seq("bar")) _, ast.NamedGraphScope("f:oo") _, ast.AllQualifier() _, Seq("role")))
      }

      test(s"$command ${privilege.name} (`b:ar`) ON GRAPH foo NODES * $preposition role") {
        yields(func(privilege, ast.PropertiesResource(Seq("b:ar")) _, ast.NamedGraphScope("foo") _, ast.AllQualifier() _, Seq("role")))
      }

      // Invalid graph name

      test(s"$command ${privilege.name} (*) ON GRAPH f:oo NODES * $preposition role") {
        failsToParse
      }

      test(s"$command ${privilege.name} (bar) ON GRAPH f:oo NODES * $preposition role") {
        failsToParse
      }

      // multiple graphs not allowed

      test(s"$command ${privilege.name} (*) ON GRAPH foo, baz NODES A (*) $preposition role") {
        failsToParse
      }

      test(s"$command ${privilege.name} (bar) ON GRAPH foo, baz NODES A (*) $preposition role") {
        failsToParse
      }

      // invalid property definition

      test(s"$command ${privilege.name} (b:ar) ON GRAPH foo NODES * $preposition role") {failsToParse}

      // missing graph name

      test(s"$command ${privilege.name} (*) ON GRAPH NODES * $preposition role") {
        failsToParse
      }

      test(s"$command ${privilege.name} (*) ON GRAPH NODES * (*) $preposition role") {
        failsToParse
      }

      test(s"$command ${privilege.name} (*) ON GRAPH NODES A $preposition role") {
        failsToParse
      }

      test(s"$command ${privilege.name} (*) ON GRAPH NODES A (*) $preposition role") {
        failsToParse
      }

      test(s"$command ${privilege.name} (bar) ON GRAPH NODES * $preposition role") {
        failsToParse
      }

      test(s"$command ${privilege.name} (bar) ON GRAPH NODES * (*) $preposition role") {
        failsToParse
      }

      test(s"$command ${privilege.name} (bar) ON GRAPH NODES A $preposition role") {
        failsToParse
      }

      test(s"$command ${privilege.name} (bar) ON GRAPH NODES A (*) $preposition role") {
        failsToParse
      }

      // missing property definition

      test(s"$command ${privilege.name} ON GRAPH * NODES * $preposition role") {
        failsToParse
      }

      test(s"$command ${privilege.name} ON GRAPH * NODES * (*) $preposition role") {
        failsToParse
      }

      test(s"$command ${privilege.name} ON GRAPH * NODES A $preposition role") {
        failsToParse
      }

      test(s"$command ${privilege.name} ON GRAPH * NODES A (*) $preposition role") {
        failsToParse
      }

      test(s"$command ${privilege.name} ON GRAPH foo NODES * $preposition role") {
        failsToParse
      }

      test(s"$command ${privilege.name} ON GRAPH foo NODES * (*) $preposition role") {
        failsToParse
      }

      test(s"$command ${privilege.name} ON GRAPH foo NODES A $preposition role") {
        failsToParse
      }

      test(s"$command ${privilege.name} ON GRAPH foo NODES A (*) $preposition role") {
        failsToParse
      }
  }
}
