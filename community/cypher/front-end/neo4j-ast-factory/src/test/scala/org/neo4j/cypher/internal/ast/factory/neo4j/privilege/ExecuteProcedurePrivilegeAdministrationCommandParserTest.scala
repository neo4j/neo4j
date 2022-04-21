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

import org.neo4j.cypher.internal.ast.ExecuteAdminProcedureAction
import org.neo4j.cypher.internal.ast.ExecuteBoostedProcedureAction
import org.neo4j.cypher.internal.ast.ExecuteProcedureAction
import org.neo4j.cypher.internal.ast.ProcedureQualifier
import org.neo4j.cypher.internal.ast.factory.neo4j.AdministrationAndSchemaCommandParserTestBase
import org.neo4j.cypher.internal.util.InputPosition

class ExecuteProcedurePrivilegeAdministrationCommandParserTest extends AdministrationAndSchemaCommandParserTestBase {

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
            yields(func(action, List(procedureQualifier("*")), Seq(literalRole)))
          }

          // The following two tests check that the plural form EXECUTE [BOOSTED] PROCEDURES is valid

          test(s"$verb ${execute}S * ON DBMS $preposition role") {
            yields(func(action, List(procedureQualifier("*")), Seq(literalRole)))
          }

          test(s"$verb ${execute}S `*` ON DBMS $preposition role") {
            yields(func(action, List(procedureQualifier("*")), Seq(literalRole)))
          }

          test(s"$verb $execute apoc.procedure ON DBMS $preposition role") {
            yields(func(action, List(procedureQualifier("apoc.procedure")), Seq(literalRole)))
          }

          test(s"$verb ${execute}S apoc.procedure ON DBMS $preposition role") {
            yields(func(action, List(procedureQualifier("apoc.procedure")), Seq(literalRole)))
          }

          test(s"$verb $execute apoc.math.sin ON DBMS $preposition role") {
            yields(func(action, List(procedureQualifier("apoc.math.sin")), Seq(literalRole)))
          }

          test(s"$verb $execute apoc* ON DBMS $preposition role") {
            yields(func(action, List(procedureQualifier("apoc*")), Seq(literalRole)))
          }

          test(s"$verb $execute *apoc ON DBMS $preposition role") {
            yields(func(action, List(procedureQualifier("*apoc")), Seq(literalRole)))
          }

          test(s"$verb $execute *apoc, *.sin ON DBMS $preposition role") {
            yields(func(action, List(procedureQualifier("*apoc"), procedureQualifier("*.sin")), Seq(literalRole)))
          }

          test(s"$verb $execute *.sin, apoc* ON DBMS $preposition role") {
            yields(func(action, List(procedureQualifier("*.sin"), procedureQualifier("apoc*")), Seq(literalRole)))
          }

          test(s"$verb $execute *.sin ON DBMS $preposition role") {
            yields(func(action, List(procedureQualifier("*.sin")), Seq(literalRole)))
          }

          test(s"$verb $execute apoc.*.math.* ON DBMS $preposition role") {
            yields(func(action, List(procedureQualifier("apoc.*.math.*")), Seq(literalRole)))
          }

          test(s"$verb $execute math.*n ON DBMS $preposition role") {
            yields(func(action, List(procedureQualifier("math.*n")), Seq(literalRole)))
          }

          test(s"$verb $execute math.si? ON DBMS $preposition role") {
            yields(func(action, List(procedureQualifier("math.si?")), Seq(literalRole)))
          }

          test(s"$verb $execute mat*.sin ON DBMS $preposition role") {
            yields(func(action, List(procedureQualifier("mat*.sin")), Seq(literalRole)))
          }

          test(s"$verb $execute mat?.sin ON DBMS $preposition role") {
            yields(func(action, List(procedureQualifier("mat?.sin")), Seq(literalRole)))
          }

          test(s"$verb $execute ?ath.sin ON DBMS $preposition role") {
            yields(func(action, List(procedureQualifier("?ath.sin")), Seq(literalRole)))
          }

          test(s"$verb $execute mat?.`a.\n`.*n ON DBMS $preposition role") {
            yields(func(action, List(procedureQualifier("mat?.a.\n.*n")), Seq(literalRole)))
          }

          test(s"$verb $execute `mat?`.`a.\n`.`*n` ON DBMS $preposition role") {
            yields(func(action, List(procedureQualifier("mat?.a.\n.*n")), Seq(literalRole)))
          }

          test(s"$verb $execute `a b` ON DBMS $preposition role") {
            yields(func(action, List(procedureQualifier("a b")), Seq(literalRole)))
          }

          test(s"$verb $execute a b ON DBMS $preposition role") {
            assertAst(func(action, List(ProcedureQualifier("ab")(defaultPos)), Seq(Left("role")))(defaultPos))
          }

          test(s"$verb $execute apoc.math.* ON DBMS $preposition role") {
            yields(func(action, List(procedureQualifier("apoc.math.*")), Seq(literalRole)))
          }

          test(s"$verb $execute math.sin, math.cos ON DBMS $preposition role") {
            yields(func(action, List(procedureQualifier("math.sin"), procedureQualifier("math.cos")), Seq(literalRole)))
          }

          test(s"$verb $execute apoc.math.sin, math.* ON DBMS $preposition role") {
            yields(func(
              action,
              List(procedureQualifier("apoc.math.sin"), procedureQualifier("math.*")),
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
  }

  Seq(
    ("GRANT", "TO", grantDbmsPrivilege: dbmsPrivilegeFunc),
    ("DENY", "TO", denyDbmsPrivilege: dbmsPrivilegeFunc),
    ("REVOKE GRANT", "FROM", revokeGrantDbmsPrivilege: dbmsPrivilegeFunc),
    ("REVOKE DENY", "FROM", revokeDenyDbmsPrivilege: dbmsPrivilegeFunc),
    ("REVOKE", "FROM", revokeDbmsPrivilege: dbmsPrivilegeFunc)
  ).foreach {
    case (verb: String, preposition: String, func: dbmsPrivilegeFunc) =>
      Seq(
        "EXECUTE ADMIN PROCEDURES",
        "EXECUTE ADMINISTRATOR PROCEDURES"
      ).foreach {
        command =>
          test(s"$verb $command ON DBMS $preposition role") {
            yields(func(ExecuteAdminProcedureAction, Seq(literalRole)))
          }

          test(s"$verb $command * ON DBMS $preposition role") {
            val offset = s"$verb $command ".length
            assertFailsWithMessage(
              testName,
              s"""Invalid input '*': expected "ON" (line 1, column ${offset + 1} (offset: $offset))""".stripMargin
            )
          }

          test(s"$verb $command ON DATABASE * $preposition role") {
            val offset = s"$verb $command ON ".length
            assertFailsWithMessage(
              testName,
              s"""Invalid input 'DATABASE': expected "DBMS" (line 1, column ${offset + 1} (offset: $offset))""".stripMargin
            )
          }

      }

      test(s"$verb EXECUTE ADMIN PROCEDURE ON DBMS $preposition role") {
        val offset = s"$verb EXECUTE ADMIN ".length
        assertFailsWithMessage(
          testName,
          s"""Invalid input 'PROCEDURE': expected "PROCEDURES" (line 1, column ${offset + 1} (offset: $offset))""".stripMargin
        )
      }
  }

  private def procedureQualifier(procName: String): InputPosition => ProcedureQualifier =
    ProcedureQualifier(procName)(_)
}
