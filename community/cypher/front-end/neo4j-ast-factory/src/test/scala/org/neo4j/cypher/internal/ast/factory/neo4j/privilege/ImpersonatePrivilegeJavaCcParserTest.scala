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
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.PrivilegeQualifier
import org.neo4j.cypher.internal.ast.RevokeBothType
import org.neo4j.cypher.internal.ast.RevokeDenyType
import org.neo4j.cypher.internal.ast.RevokeGrantType
import org.neo4j.cypher.internal.ast.factory.neo4j.ParserComparisonTestBase
import org.neo4j.cypher.internal.expressions.ExplicitParameter
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.test_helpers.TestName
import org.scalatest.FunSuiteLike

class ImpersonatePrivilegeJavaCcParserTest extends ParserComparisonTestBase with FunSuiteLike with TestName with AstConstructionTestSupport {

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
        assertJavaCCAST(testName, func(List(ast.UserAllQualifier()(defaultPos)), List(Left("role")))(defaultPos), comparePosition = false)
      }

      test(s"$verb IMPERSONATE (*) ON DBMS $preposition role") {
        assertJavaCCAST(testName, func(List(ast.UserAllQualifier()(defaultPos)), List(Left("role")))(defaultPos), comparePosition = false)
      }

      test(s"$verb IMPERSONATE (foo) ON DBMS $preposition role") {
        assertJavaCCAST(testName, func(List(ast.UserQualifier(Left("foo"))(defaultPos)), List(Left("role")))(defaultPos), comparePosition = false)
      }

      test(s"$verb IMPERSONATE (foo, $$userParam) ON DBMS $preposition role") {
        val fooColumn:Int = verb.length + " IMPERSONATE (".length
        val useParamColumn: Int = fooColumn + "foo $".length
        assertJavaCCAST(testName, func(List(
          ast.UserQualifier(Left("foo"))(1, fooColumn + 1, fooColumn),
          ast.UserQualifier(Right(ExplicitParameter("userParam", CTString)(1, useParamColumn + 1, useParamColumn)))
          (defaultPos)),
          List(Left("role")))
        (defaultPos))
      }
  }
}
