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

import org.neo4j.cypher.internal.ast.ExecuteBoostedFunctionAction
import org.neo4j.cypher.internal.ast.ExecuteFunctionAction
import org.neo4j.cypher.internal.ast.FunctionQualifier
import org.neo4j.cypher.internal.ast.ProcedureQualifier
import org.neo4j.cypher.internal.ast.factory.neo4j.AdministrationAndSchemaCommandParserTestBase
import org.neo4j.cypher.internal.util.InputPosition

class ExecuteFunctionPrivilegeAdministrationCommandParserTest extends AdministrationAndSchemaCommandParserTestBase {

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
            yields(func(action, List(functionQualifier("*")), Seq(literalRole)))
          }

          // The following two tests check that the plural form EXECUTE ... FUNCTIONS is valid

          test(s"$verb ${execute}S * ON DBMS $preposition role") {
            yields(func(action, List(functionQualifier("*")), Seq(literalRole)))
          }

          test(s"$verb ${execute}S `*` ON DBMS $preposition role") {
            yields(func(action, List(functionQualifier("*")), Seq(literalRole)))
          }

          test(s"$verb $execute apoc.function ON DBMS $preposition role") {
            yields(func(action, List(functionQualifier("apoc.function")), Seq(literalRole)))
          }

          test(s"$verb ${execute}S apoc.function ON DBMS $preposition role") {
            yields(func(action, List(functionQualifier("apoc.function")), Seq(literalRole)))
          }

          test(s"$verb $execute apoc.math.sin ON DBMS $preposition role") {
            yields(func(action, List(functionQualifier("apoc.math.sin")), Seq(literalRole)))
          }

          test(s"$verb $execute apoc* ON DBMS $preposition role") {
            yields(func(action, List(functionQualifier("apoc*")), Seq(literalRole)))
          }

          test(s"$verb $execute *apoc ON DBMS $preposition role") {
            yields(func(action, List(functionQualifier("*apoc")), Seq(literalRole)))
          }

          test(s"$verb $execute *apoc, *.sin ON DBMS $preposition role") {
            yields(func(action, List(functionQualifier("*apoc"), functionQualifier("*.sin")), Seq(literalRole)))
          }

          test(s"$verb $execute *.sin, apoc* ON DBMS $preposition role") {
            yields(func(action, List(functionQualifier("*.sin"), functionQualifier("apoc*")), Seq(literalRole)))
          }

          test(s"$verb $execute *.sin ON DBMS $preposition role") {
            yields(func(action, List(functionQualifier("*.sin")), Seq(literalRole)))
          }

          test(s"$verb $execute apoc.*.math.* ON DBMS $preposition role") {
            yields(func(action, List(functionQualifier("apoc.*.math.*")), Seq(literalRole)))
          }

          test(s"$verb $execute math.*n ON DBMS $preposition role") {
            yields(func(action, List(functionQualifier("math.*n")), Seq(literalRole)))
          }

          test(s"$verb $execute math.si? ON DBMS $preposition role") {
            yields(func(action, List(functionQualifier("math.si?")), Seq(literalRole)))
          }

          test(s"$verb $execute mat*.sin ON DBMS $preposition role") {
            yields(func(action, List(functionQualifier("mat*.sin")), Seq(literalRole)))
          }

          test(s"$verb $execute mat?.sin ON DBMS $preposition role") {
            yields(func(action, List(functionQualifier("mat?.sin")), Seq(literalRole)))
          }

          test(s"$verb $execute ?ath.sin ON DBMS $preposition role") {
            yields(func(action, List(functionQualifier("?ath.sin")), Seq(literalRole)))
          }

          test(s"$verb $execute mat?.`a.\n`.*n ON DBMS $preposition role") {
            yields(func(action, List(functionQualifier("mat?.a.\n.*n")), Seq(literalRole)))
          }

          test(s"$verb $execute `mat?`.`a.\n`.`*n` ON DBMS $preposition role") {
            yields(func(action, List(functionQualifier("mat?.a.\n.*n")), Seq(literalRole)))
          }

          test(s"$verb $execute `a b` ON DBMS $preposition role") {
            yields(func(action, List(functionQualifier("a b")), Seq(literalRole)))
          }

          test(s"$verb $execute a b ON DBMS $preposition role") {
            assertAst(func(action, List(FunctionQualifier("ab")(defaultPos)), Seq(Left("role")))(defaultPos))
          }

          test(s"$verb $execute math.sin, math.cos ON DBMS $preposition role") {
            yields(func(action, List(functionQualifier("math.sin"), functionQualifier("math.cos")), Seq(literalRole)))
          }

          test(s"$verb $execute apoc.math.sin, math.* ON DBMS $preposition role") {
            yields(func(
              action,
              List(functionQualifier("apoc.math.sin"), functionQualifier("math.*")),
              Seq(literalRole)
            ))
          }

          test(s"$verb $execute apoc.math.sin, math.*, apoc* ON DBMS $preposition role") {
            yields(func(
              action,
              List(functionQualifier("apoc.math.sin"), functionQualifier("math.*"), functionQualifier("apoc*")),
              Seq(literalRole)
            ))
          }

          test(s"$verb $execute * $preposition role") {
            val offset = testName.length
            assertFailsWithMessage(
              testName,
              s"""Invalid input '': expected
                 |  "*"
                 |  "."
                 |  "?"
                 |  "ON"
                 |  an identifier (line 1, column ${offset + 1} (offset: $offset))""".stripMargin
            )
          }

          test(s"$verb $execute * ON DATABASE * $preposition role") {
            val offset = testName.length
            assertFailsWithMessage(
              testName,
              s"""Invalid input '': expected
                 |  "*"
                 |  "."
                 |  "?"
                 |  "ON"
                 |  an identifier (line 1, column ${offset + 1} (offset: $offset))""".stripMargin
            )
          }

          // Tests for invalid escaping

          test(s"$verb $execute `ab?`* ON DBMS $preposition role") {
            val offset = s"$verb $execute ".length
            assertFailsWithMessage(
              testName,
              s"""Invalid input 'ab?': expected "*", ".", "?" or an identifier (line 1, column ${offset + 1} (offset: $offset))""".stripMargin
            )
          }

          test(s"$verb $execute a`ab?` ON DBMS $preposition role") {
            val offset = s"$verb $execute a".length
            assertFailsWithMessage(
              testName,
              s"""Invalid input 'ab?': expected
                 |  "*"
                 |  "."
                 |  "?"
                 |  "ON"
                 |  an identifier (line 1, column ${offset + 1} (offset: $offset))""".stripMargin
            )
          }

          test(s"$verb $execute ab?`%ab`* ON DBMS $preposition role") {
            val offset = s"$verb $execute ab?".length
            assertFailsWithMessage(
              testName,
              s"""Invalid input '%ab': expected
                 |  "*"
                 |  "."
                 |  "?"
                 |  "ON"
                 |  "YIELD"
                 |  an identifier (line 1, column ${offset + 1} (offset: $offset))""".stripMargin
            )
          }

          test(s"$verb $execute apoc.`*`ab? ON DBMS $preposition role") {
            val offset = s"$verb $execute apoc.".length
            assertFailsWithMessage(
              testName,
              s"""Invalid input '*': expected
                 |  "*"
                 |  "."
                 |  "?"
                 |  "YIELD"
                 |  an identifier (line 1, column ${offset + 1} (offset: $offset))""".stripMargin
            )
          }

          test(s"$verb $execute apoc.*`ab?` ON DBMS $preposition role") {
            val offset = s"$verb $execute apoc.*".length
            assertFailsWithMessage(
              testName,
              s"""Invalid input 'ab?': expected
                 |  "*"
                 |  "."
                 |  "?"
                 |  "ON"
                 |  an identifier (line 1, column ${offset + 1} (offset: $offset))""".stripMargin
            )
          }

          test(s"$verb $execute `ap`oc.ab? ON DBMS $preposition role") {
            val offset = s"$verb $execute ".length
            assertFailsWithMessage(
              testName,
              s"""Invalid input 'ap': expected "*", ".", "?" or an identifier (line 1, column ${offset + 1} (offset: $offset))""".stripMargin
            )
          }

          test(s"$verb $execute ap`oc`.ab? ON DBMS $preposition role") {
            val offset = s"$verb $execute ap".length
            assertFailsWithMessage(
              testName,
              s"""Invalid input 'oc': expected
                 |  "*"
                 |  "."
                 |  "?"
                 |  "ON"
                 |  an identifier (line 1, column ${offset + 1} (offset: $offset))""".stripMargin
            )
          }
      }

      test(s"$verb EXECUTE DEFINED FUNCTION * ON DATABASE * $preposition role") {
        val offset = s"$verb EXECUTE ".length
        assertFailsWithMessage(
          testName,
          s"""Invalid input 'DEFINED': expected
             |  "ADMIN"
             |  "ADMINISTRATOR"
             |  "BOOSTED"
             |  "FUNCTION"
             |  "FUNCTIONS"
             |  "PROCEDURE"
             |  "PROCEDURES"
             |  "USER" (line 1, column ${offset + 1} (offset: $offset))""".stripMargin
        )
      }
  }

  private def functionQualifier(glob: String): InputPosition => FunctionQualifier = FunctionQualifier(glob)(_)
}
