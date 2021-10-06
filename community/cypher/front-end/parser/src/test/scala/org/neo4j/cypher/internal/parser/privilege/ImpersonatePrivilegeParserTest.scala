/*
 * Copyright (c) "Neo4j"
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
import org.neo4j.cypher.internal.ast.PrivilegeQualifier
import org.neo4j.cypher.internal.ast.RevokeBothType
import org.neo4j.cypher.internal.ast.RevokeDenyType
import org.neo4j.cypher.internal.ast.RevokeGrantType
import org.neo4j.cypher.internal.expressions.ExplicitParameter
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.parser.AdministrationCommandParserTestBase
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.symbols.CTString

class ImpersonatePrivilegeParserTest extends AdministrationCommandParserTestBase {
  type impersonatePrivilegeFunc = (List[PrivilegeQualifier], Seq[Either[String, Parameter]]) => InputPosition => ast.Statement

  def grantImpersonatePrivilege(q: List[PrivilegeQualifier], r: Seq[Either[String, Parameter]]): InputPosition => ast.Statement =
    ast.GrantPrivilege.dbmsAction(ast.ImpersonateUserAction, r, q)

  def denyImpersonatePrivilege(q: List[PrivilegeQualifier], r: Seq[Either[String, Parameter]]): InputPosition => ast.Statement =
    ast.DenyPrivilege.dbmsAction(ast.ImpersonateUserAction, r, q)

  def revokeGrantImpersonatePrivilege(q: List[PrivilegeQualifier], r: Seq[Either[String, Parameter]]): InputPosition => ast.Statement =
    ast.RevokePrivilege.dbmsAction(ast.ImpersonateUserAction, r, RevokeGrantType()(pos), q)

  def revokeDenyImpersonatePrivilege(q: List[PrivilegeQualifier], r: Seq[Either[String, Parameter]]): InputPosition => ast.Statement =
    ast.RevokePrivilege.dbmsAction(ast.ImpersonateUserAction, r, RevokeDenyType()(pos), q)

  def revokeImpersonatePrivilege(q: List[PrivilegeQualifier], r: Seq[Either[String, Parameter]]): InputPosition => ast.Statement =
    ast.RevokePrivilege.dbmsAction(ast.ImpersonateUserAction, r, RevokeBothType()(pos), q)

  Seq(
    ("GRANT", "TO", grantImpersonatePrivilege: impersonatePrivilegeFunc),
    ("DENY", "TO", denyImpersonatePrivilege: impersonatePrivilegeFunc),
    ("REVOKE GRANT", "FROM", revokeGrantImpersonatePrivilege: impersonatePrivilegeFunc),
    ("REVOKE DENY", "FROM", revokeDenyImpersonatePrivilege: impersonatePrivilegeFunc),
    ("REVOKE", "FROM", revokeImpersonatePrivilege: impersonatePrivilegeFunc)
  ).foreach {
    case (verb: String, preposition: String, func: impersonatePrivilegeFunc) =>
      test(s"$verb IMPERSONATE ON DBMS $preposition role") {
        yields(func(List(ast.UserAllQualifier()(pos)), List(Left("role"))))
      }

      test(s"$verb IMPERSONATE (*) ON DBMS $preposition role") {
        yields(func(List(ast.UserAllQualifier()(pos)), List(Left("role"))))
      }

      test(s"$verb IMPERSONATE (foo) ON DBMS $preposition role") {
        yields(func(List(ast.UserQualifier(Left("foo"))(pos)), List(Left("role"))))
      }

      test(s"$verb IMPERSONATE (foo, $$userParam) ON DBMS $preposition role") {
        yields(func(List(ast.UserQualifier(Left("foo"))(pos), ast.UserQualifier(Right(ExplicitParameter("userParam", CTString)(pos)))(pos)), List(Left("role"))))
      }
  }
}
