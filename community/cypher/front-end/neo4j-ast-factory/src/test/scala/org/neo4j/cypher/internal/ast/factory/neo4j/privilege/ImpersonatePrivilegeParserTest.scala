/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.ast.factory.neo4j.privilege

import org.neo4j.cypher.internal.ast
import org.neo4j.cypher.internal.ast.factory.neo4j.AdministrationAndSchemaCommandParserTestBase
import org.neo4j.cypher.internal.expressions.ExplicitParameter
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.symbols.CTString

class ImpersonatePrivilegeParserTest extends AdministrationAndSchemaCommandParserTestBase {

  type impersonatePrivilegeFunc =
    (List[ast.PrivilegeQualifier], Seq[Either[String, Parameter]], Immutable) => InputPosition => ast.Statement

  def grantImpersonatePrivilege(
    q: List[ast.PrivilegeQualifier],
    r: Seq[Either[String, Parameter]],
    i: Immutable
  ): InputPosition => ast.Statement =
    ast.GrantPrivilege.dbmsAction(ast.ImpersonateUserAction, i, r, q)

  def denyImpersonatePrivilege(
    q: List[ast.PrivilegeQualifier],
    r: Seq[Either[String, Parameter]],
    i: Immutable
  ): InputPosition => ast.Statement =
    ast.DenyPrivilege.dbmsAction(ast.ImpersonateUserAction, i, r, q)

  def revokeGrantImpersonatePrivilege(
    q: List[ast.PrivilegeQualifier],
    r: Seq[Either[String, Parameter]],
    i: Immutable
  ): InputPosition => ast.Statement =
    ast.RevokePrivilege.dbmsAction(ast.ImpersonateUserAction, i, r, ast.RevokeGrantType()(pos), q)

  def revokeDenyImpersonatePrivilege(
    q: List[ast.PrivilegeQualifier],
    r: Seq[Either[String, Parameter]],
    i: Immutable
  ): InputPosition => ast.Statement =
    ast.RevokePrivilege.dbmsAction(ast.ImpersonateUserAction, i, r, ast.RevokeDenyType()(pos), q)

  def revokeImpersonatePrivilege(
    q: List[ast.PrivilegeQualifier],
    r: Seq[Either[String, Parameter]],
    i: Immutable
  ): InputPosition => ast.Statement =
    ast.RevokePrivilege.dbmsAction(ast.ImpersonateUserAction, i, r, ast.RevokeBothType()(pos), q)

  Seq(
    ("GRANT", "TO", grantImpersonatePrivilege: impersonatePrivilegeFunc),
    ("DENY", "TO", denyImpersonatePrivilege: impersonatePrivilegeFunc),
    ("REVOKE GRANT", "FROM", revokeGrantImpersonatePrivilege: impersonatePrivilegeFunc),
    ("REVOKE DENY", "FROM", revokeDenyImpersonatePrivilege: impersonatePrivilegeFunc),
    ("REVOKE", "FROM", revokeImpersonatePrivilege: impersonatePrivilegeFunc)
  ).foreach {
    case (verb: String, preposition: String, func: impersonatePrivilegeFunc) =>
      Seq[Immutable](true, false).foreach {
        immutable =>
          val immutableString = immutableOrEmpty(immutable)
          test(s"$verb$immutableString IMPERSONATE ON DBMS $preposition role") {
            assertAst(
              func(List(ast.UserAllQualifier()(pos)), List(Left("role")), immutable)(defaultPos),
              comparePosition = false
            )
          }

          test(s"$verb$immutableString IMPERSONATE (*) ON DBMS $preposition role") {
            assertAst(
              func(List(ast.UserAllQualifier()(pos)), List(Left("role")), immutable)(defaultPos),
              comparePosition = false
            )
          }

          test(s"$verb$immutableString IMPERSONATE (foo) ON DBMS $preposition role") {
            assertAst(
              func(List(ast.UserQualifier(Left("foo"))(pos)), List(Left("role")), immutable)(defaultPos),
              comparePosition = false
            )
          }

          test(s"$verb$immutableString IMPERSONATE (foo, $$userParam) ON DBMS $preposition role") {
            val fooColumn: Int = verb.length + immutableString.length + " IMPERSONATE (".length
            val useParamColumn: Int = fooColumn + "foo $".length
            assertAst(func(
              List(
                ast.UserQualifier(Left("foo"))((1, fooColumn + 1, fooColumn)),
                ast.UserQualifier(Right(ExplicitParameter("userParam", CTString)((
                  1,
                  useParamColumn + 1,
                  useParamColumn
                ))))(
                  defaultPos
                )
              ),
              List(Left("role")),
              immutable
            )(defaultPos))
          }
      }
  }
}
