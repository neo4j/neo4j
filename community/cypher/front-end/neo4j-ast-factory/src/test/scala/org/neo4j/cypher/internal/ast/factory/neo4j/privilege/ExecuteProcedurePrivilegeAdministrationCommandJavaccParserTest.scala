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
import org.neo4j.cypher.internal.ast.ExecuteBoostedProcedureAction
import org.neo4j.cypher.internal.ast.ExecuteProcedureAction
import org.neo4j.cypher.internal.ast.ProcedurePrivilegeQualifier
import org.neo4j.cypher.internal.ast.ProcedureQualifier
import org.neo4j.cypher.internal.ast.RevokeBothType
import org.neo4j.cypher.internal.ast.RevokeDenyType
import org.neo4j.cypher.internal.ast.RevokeGrantType
import org.neo4j.cypher.internal.ast.factory.neo4j.ParserComparisonTestBase
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.util.DummyPosition
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.test_helpers.TestName
import org.scalatest.FunSuiteLike

class ExecuteProcedurePrivilegeAdministrationCommandJavaccParserTest extends ParserComparisonTestBase with FunSuiteLike with TestName {

  Seq(
    ("GRANT", "TO", grantExecuteProcedurePrivilege: executeProcedurePrivilegeFunc),
    ("DENY", "TO", denyExecuteProcedurePrivilege: executeProcedurePrivilegeFunc),
    ("REVOKE GRANT", "FROM", revokeGrantExecuteProcedurePrivilege: executeProcedurePrivilegeFunc),
    ("REVOKE DENY", "FROM", revokeDenyExecuteProcedurePrivilege: executeProcedurePrivilegeFunc),
    ("REVOKE", "FROM", revokeExecuteProcedurePrivilege: executeProcedurePrivilegeFunc)
  ).foreach {
    case (verb: String, preposition: String, func: executeProcedurePrivilegeFunc) =>

      Seq(
        ("EXECUTE PROCEDURE", ExecuteProcedureAction),
        ("EXECUTE BOOSTED PROCEDURE", ExecuteBoostedProcedureAction)
      ).foreach {
        case (execute, action) =>

          test(s"$verb $execute * ON DBMS $preposition role") {
            assertSameAST(testName)
          }

          // The following two tests check that the plural form EXECUTE [BOOSTED] PROCEDURES is valid

          test(s"$verb ${execute}S * ON DBMS $preposition role") {
            assertSameAST(testName)
          }

          test(s"$verb ${execute}S `*` ON DBMS $preposition role") {
            assertSameAST(testName)
          }

          test(s"$verb $execute apoc.procedure ON DBMS $preposition role") {
            assertSameAST(testName)
          }

          test(s"$verb ${execute}S apoc.procedure ON DBMS $preposition role") {
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

          test(s"$verb $execute `mat?`.`a.\n`.`*n` ON DBMS $preposition role") {
            assertSameAST(testName)
          }

          test(s"$verb $execute `a b` ON DBMS $preposition role") {
            assertSameAST(testName)
          }

          test(s"$verb $execute a b ON DBMS $preposition role") {
            assertJavaCCAST(testName, func(action, List(ProcedureQualifier("ab")(defaultPos)), Seq(Left("role")))(defaultPos))
          }

          test(s"$verb $execute apoc.math.* ON DBMS $preposition role") {
            assertSameAST(testName)
          }

          test(s"$verb $execute math.sin, math.cos ON DBMS $preposition role") {
            assertSameAST(testName)
          }

          test(s"$verb $execute apoc.math.sin, math.* ON DBMS $preposition role") {
            assertSameAST(testName)
          }

          test(s"$verb $execute * $preposition role") {
            val offset = testName.length
            assertJavaCCException(testName, s"""Invalid input '': expected
                                               |  "*"
                                               |  "."
                                               |  "?"
                                               |  "ON"
                                               |  an identifier (line 1, column ${offset + 1} (offset: $offset))""".stripMargin)
          }

          test(s"$verb $execute * ON DATABASE * $preposition role") {
            val offset = testName.length
            assertJavaCCException(testName, s"""Invalid input '': expected
                                               |  "*"
                                               |  "."
                                               |  "?"
                                               |  "ON"
                                               |  an identifier (line 1, column ${offset + 1} (offset: $offset))""".stripMargin)
          }

          // Tests for invalid escaping

          test(s"$verb $execute `ab?`* ON DBMS $preposition role") {
            val offset = s"$verb $execute ".length
            assertJavaCCException(testName,
              s"""Invalid input 'ab?': expected "*", ".", "?" or an identifier (line 1, column ${offset + 1} (offset: $offset))""".stripMargin)
          }

          test(s"$verb $execute a`ab?` ON DBMS $preposition role") {
            val offset = s"$verb $execute a".length
            assertJavaCCException(testName,
              s"""Invalid input 'ab?': expected
                 |  "*"
                 |  "."
                 |  "?"
                 |  "ON"
                 |  an identifier (line 1, column ${offset + 1} (offset: $offset))""".stripMargin)
          }

          test(s"$verb $execute ab?`%ab`* ON DBMS $preposition role") {
            val offset = s"$verb $execute ab?".length
            assertJavaCCException(testName,
              s"""Invalid input '%ab': expected
                 |  "*"
                 |  "."
                 |  "?"
                 |  "ON"
                 |  "YIELD"
                 |  an identifier (line 1, column ${offset + 1} (offset: $offset))""".stripMargin)
          }

          test(s"$verb $execute apoc.`*`ab? ON DBMS $preposition role") {
            val offset = s"$verb $execute apoc.".length
            assertJavaCCException(testName,
              s"""Invalid input '*': expected
                 |  "*"
                 |  "."
                 |  "?"
                 |  "YIELD"
                 |  an identifier (line 1, column ${offset + 1} (offset: $offset))""".stripMargin)
          }

          test(s"$verb $execute apoc.*`ab?` ON DBMS $preposition role") {
            val offset = s"$verb $execute apoc.*".length
            assertJavaCCException(testName,
              s"""Invalid input 'ab?': expected
                 |  "*"
                 |  "."
                 |  "?"
                 |  "ON"
                 |  an identifier (line 1, column ${offset + 1} (offset: $offset))""".stripMargin)
          }

          test(s"$verb $execute `ap`oc.ab? ON DBMS $preposition role") {
            val offset = s"$verb $execute ".length
            assertJavaCCException(testName,
              s"""Invalid input 'ap': expected "*", ".", "?" or an identifier (line 1, column ${offset + 1} (offset: $offset))""".stripMargin)
          }

          test(s"$verb $execute ap`oc`.ab? ON DBMS $preposition role") {
            val offset = s"$verb $execute ap".length
            assertJavaCCException(testName,
              s"""Invalid input 'oc': expected
                 |  "*"
                 |  "."
                 |  "?"
                 |  "ON"
                 |  an identifier (line 1, column ${offset + 1} (offset: $offset))""".stripMargin)
          }
      }
  }

  Seq(
    ("GRANT", "TO"),
    ("DENY", "TO"),
    ("REVOKE GRANT", "FROM"),
    ("REVOKE DENY", "FROM"),
    ("REVOKE", "FROM")
  ).foreach {
    case (verb: String, preposition: String) =>
      Seq(
        "EXECUTE ADMIN PROCEDURES",
        "EXECUTE ADMINISTRATOR PROCEDURES"
      ).foreach {
        command =>

          test(s"$verb $command ON DBMS $preposition role") {
            assertSameAST(testName)
          }

          test(s"$verb $command * ON DBMS $preposition role") {
            val offset = s"$verb $command ".length
            assertJavaCCException(testName, s"""Invalid input '*': expected "ON" (line 1, column ${offset + 1} (offset: $offset))""".stripMargin)
          }

          test(s"$verb $command ON DATABASE * $preposition role") {
            val offset = s"$verb $command ON ".length
            assertJavaCCException(testName, s"""Invalid input 'DATABASE': expected "DBMS" (line 1, column ${offset + 1} (offset: $offset))""".stripMargin)
          }
      }

      test(s"$verb EXECUTE ADMIN PROCEDURE ON DBMS $preposition role") {
        val offset = s"$verb EXECUTE ADMIN ".length
        assertJavaCCException(testName, s"""Invalid input 'PROCEDURE': expected "PROCEDURES" (line 1, column ${offset + 1} (offset: $offset))""".stripMargin)
      }
  }

  private val defaultPos: InputPosition = InputPosition(0, 1, 1)

  private type executeProcedurePrivilegeFunc = (DbmsAction, List[ProcedurePrivilegeQualifier], Seq[Either[String, Parameter]]) => InputPosition => ast.Statement

  private def grantExecuteProcedurePrivilege(a: DbmsAction, q: List[ProcedurePrivilegeQualifier], r: Seq[Either[String, Parameter]]): InputPosition => ast.Statement =
    ast.GrantPrivilege.dbmsAction(a, r, q)

  private def denyExecuteProcedurePrivilege(a: DbmsAction, q: List[ProcedurePrivilegeQualifier], r: Seq[Either[String, Parameter]]): InputPosition => ast.Statement =
    ast.DenyPrivilege.dbmsAction(a, r, q)

  private def revokeGrantExecuteProcedurePrivilege(a: DbmsAction, q: List[ProcedurePrivilegeQualifier], r: Seq[Either[String, Parameter]]): InputPosition => ast.Statement =
    ast.RevokePrivilege.dbmsAction(a, r, RevokeGrantType()(DummyPosition(0)), q)

  private def revokeDenyExecuteProcedurePrivilege(a: DbmsAction, q: List[ProcedurePrivilegeQualifier], r: Seq[Either[String, Parameter]]): InputPosition => ast.Statement =
    ast.RevokePrivilege.dbmsAction(a, r, RevokeDenyType()(DummyPosition(0)), q)

  private def revokeExecuteProcedurePrivilege(a: DbmsAction, q: List[ProcedurePrivilegeQualifier], r: Seq[Either[String, Parameter]]): InputPosition => ast.Statement =
    ast.RevokePrivilege.dbmsAction(a, r, RevokeBothType()(DummyPosition(0)), q)
}
