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
import org.neo4j.cypher.internal.ast.DbmsAction
import org.neo4j.cypher.internal.ast.ExecuteBoostedFunctionAction
import org.neo4j.cypher.internal.ast.ExecuteFunctionAction
import org.neo4j.cypher.internal.ast.FunctionPrivilegeQualifier
import org.neo4j.cypher.internal.ast.FunctionQualifier
import org.neo4j.cypher.internal.ast.RevokeBothType
import org.neo4j.cypher.internal.ast.RevokeDenyType
import org.neo4j.cypher.internal.ast.RevokeGrantType
import org.neo4j.cypher.internal.ast.factory.neo4j.ParserComparisonTestBase
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.util.DummyPosition
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.test_helpers.TestName
import org.scalatest.FunSuiteLike

class ExecuteFunctionPrivilegeAdministrationCommandJavaccParserTest extends ParserComparisonTestBase with FunSuiteLike with TestName {

  Seq(
    ("GRANT", "TO", grantExecuteFunctionPrivilege: executeFunctionPrivilegeFunc),
    ("DENY", "TO", denyExecuteFunctionPrivilege: executeFunctionPrivilegeFunc),
    ("REVOKE GRANT", "FROM", revokeGrantExecuteFunctionPrivilege: executeFunctionPrivilegeFunc),
    ("REVOKE DENY", "FROM", revokeDenyExecuteFunctionPrivilege: executeFunctionPrivilegeFunc),
    ("REVOKE", "FROM", revokeExecuteFunctionPrivilege: executeFunctionPrivilegeFunc)
  ).foreach {
    case (verb: String, preposition: String, func: executeFunctionPrivilegeFunc) =>

      Seq(
        ("EXECUTE FUNCTION", ExecuteFunctionAction),
        ("EXECUTE USER FUNCTION", ExecuteFunctionAction),
        ("EXECUTE USER DEFINED FUNCTION", ExecuteFunctionAction),
        ("EXECUTE BOOSTED FUNCTION", ExecuteBoostedFunctionAction),
        ("EXECUTE BOOSTED USER FUNCTION", ExecuteBoostedFunctionAction),
        ("EXECUTE BOOSTED USER DEFINED FUNCTION", ExecuteBoostedFunctionAction)
      ).foreach {
        case (execute, action) =>

          test(s"$verb $execute * ON DBMS $preposition role") {
            assertSameAST(testName)
          }

          // The following two tests check that the plural form EXECUTE ... FUNCTIONS is valid

          test(s"$verb ${execute}S * ON DBMS $preposition role") {
            assertSameAST(testName)
          }

          test(s"$verb ${execute}S `*` ON DBMS $preposition role") {
            assertSameAST(testName)
          }

          test(s"$verb $execute apoc.function ON DBMS $preposition role") {
            assertSameAST(testName)
          }

          test(s"$verb ${execute}S apoc.function ON DBMS $preposition role") {
            assertSameAST(testName)
          }

          test(s"$verb $execute apoc.math.sin ON DBMS $preposition role") {
            assertSameAST(testName)
          }

          test(s"$verb $execute apoc* ON DBMS $preposition role") {
            assertSameAST(testName)
          }

          test(s"$verb $execute *apoc ON DBMS $preposition role") {
            assertSameAST(testName)
          }

          test(s"$verb $execute *apoc, *.sin ON DBMS $preposition role") {
            assertSameAST(testName)
          }

          test(s"$verb $execute *.sin, apoc* ON DBMS $preposition role") {
            assertSameAST(testName)
          }

          test(s"$verb $execute *.sin ON DBMS $preposition role") {
            assertSameAST(testName)
          }

          test(s"$verb $execute apoc.*.math.* ON DBMS $preposition role") {
            assertSameAST(testName)
          }

          test(s"$verb $execute math.*n ON DBMS $preposition role") {
            assertSameAST(testName)
          }

          test(s"$verb $execute math.si? ON DBMS $preposition role") {
            assertSameAST(testName)
          }

          test(s"$verb $execute mat*.sin ON DBMS $preposition role") {
            assertSameAST(testName)
          }

          test(s"$verb $execute mat?.sin ON DBMS $preposition role") {
            assertSameAST(testName)
          }

          test(s"$verb $execute ?ath.sin ON DBMS $preposition role") {
            assertSameAST(testName)
          }

          test(s"$verb $execute mat?.`a.\n`.*n ON DBMS $preposition role") {
            assertSameAST(testName)
          }

          test(s"$verb $execute `a b` ON DBMS $preposition role") {
            assertSameAST(testName)
          }

          test(s"$verb $execute a b ON DBMS $preposition role") {
            assertJavaCCAST(testName, func(action, List(FunctionQualifier("ab")(defaultPos)), Seq(Left("role")))(defaultPos))
          }

          test(s"$verb $execute math.sin, math.cos ON DBMS $preposition role") {
            assertSameAST(testName)
          }

          test(s"$verb $execute apoc.math.sin, math.* ON DBMS $preposition role") {
            assertSameAST(testName)
          }

          test(s"$verb $execute apoc.math.sin, math.*, apoc* ON DBMS $preposition role") {
            assertSameAST(testName)
          }

          test(s"$verb $execute * $preposition role") {
            val offset = testName.length
            assertJavaCCException(testName,
              s"""Invalid input '': expected
                 |  "*"
                 |  "."
                 |  "?"
                 |  "ON"
                 |  an identifier (line 1, column ${offset + 1} (offset: $offset))""".stripMargin)
          }

          test(s"$verb $execute * ON DATABASE * $preposition role") {
            val offset = testName.length
            assertJavaCCException(testName,
              s"""Invalid input '': expected
                 |  "*"
                 |  "."
                 |  "?"
                 |  "ON"
                 |  an identifier (line 1, column ${offset + 1} (offset: $offset))""".stripMargin)
          }
      }

      test(s"$verb EXECUTE DEFINED FUNCTION * ON DATABASE * $preposition role") {
        val offset = s"$verb EXECUTE ".length
        assertJavaCCException(testName,
          s"""Invalid input 'DEFINED': expected
             |  "ADMIN"
             |  "ADMINISTRATOR"
             |  "BOOSTED"
             |  "FUNCTION"
             |  "FUNCTIONS"
             |  "PROCEDURE"
             |  "PROCEDURES"
             |  "USER" (line 1, column ${offset + 1} (offset: $offset))""".stripMargin)
      }
  }

  private val defaultPos: InputPosition = InputPosition(0, 1, 1)

  private type executeFunctionPrivilegeFunc = (DbmsAction, List[FunctionPrivilegeQualifier], Seq[Either[String, Parameter]]) => InputPosition => ast.Statement

  private def grantExecuteFunctionPrivilege(a: DbmsAction, q: List[FunctionPrivilegeQualifier], r: Seq[Either[String, Parameter]]): InputPosition => ast.Statement =
    ast.GrantPrivilege.dbmsAction(a, r, q)

  private def denyExecuteFunctionPrivilege(a: DbmsAction, q: List[FunctionPrivilegeQualifier], r: Seq[Either[String, Parameter]]): InputPosition => ast.Statement =
    ast.DenyPrivilege.dbmsAction(a, r, q)

  private def revokeGrantExecuteFunctionPrivilege(a: DbmsAction, q: List[FunctionPrivilegeQualifier], r: Seq[Either[String, Parameter]]): InputPosition => ast.Statement =
    ast.RevokePrivilege.dbmsAction(a, r, RevokeGrantType()(DummyPosition(0)), q)

  private def revokeDenyExecuteFunctionPrivilege(a: DbmsAction, q: List[FunctionPrivilegeQualifier], r: Seq[Either[String, Parameter]]): InputPosition => ast.Statement =
    ast.RevokePrivilege.dbmsAction(a, r, RevokeDenyType()(DummyPosition(0)), q)

  private def revokeExecuteFunctionPrivilege(a: DbmsAction, q: List[FunctionPrivilegeQualifier], r: Seq[Either[String, Parameter]]): InputPosition => ast.Statement =
    ast.RevokePrivilege.dbmsAction(a, r, RevokeBothType()(DummyPosition(0)), q)
}
