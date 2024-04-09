/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
package org.neo4j.cypher.internal.ast.factory.neo4j.privilege

import org.neo4j.cypher.internal.ast.ExecuteAdminProcedureAction
import org.neo4j.cypher.internal.ast.ExecuteBoostedProcedureAction
import org.neo4j.cypher.internal.ast.ExecuteProcedureAction
import org.neo4j.cypher.internal.ast.ProcedureQualifier
import org.neo4j.cypher.internal.ast.Statements
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
          Seq[Immutable](true, false).foreach {
            immutable =>
              val immutableString = immutableOrEmpty(immutable)
              test(s"$verb$immutableString $execute * ON DBMS $preposition role") {
                parsesTo[Statements](func(action, List(procedureQualifier("*")), Seq(literalRole), immutable)(pos))
              }

              // The following two tests check that the plural form EXECUTE [BOOSTED] PROCEDURES is valid

              test(s"$verb$immutableString ${execute}S * ON DBMS $preposition role") {
                parsesTo[Statements](func(action, List(procedureQualifier("*")), Seq(literalRole), immutable)(pos))
              }

              test(s"$verb$immutableString ${execute}S `*` ON DBMS $preposition role") {
                parsesTo[Statements](func(action, List(procedureQualifier("*")), Seq(literalRole), immutable)(pos))
              }

              test(s"$verb$immutableString $execute apoc.procedure ON DBMS $preposition role") {
                parsesTo[Statements](func(
                  action,
                  List(procedureQualifier("apoc.procedure")),
                  Seq(literalRole),
                  immutable
                )(pos))
              }

              test(s"$verb$immutableString ${execute}S apoc.procedure ON DBMS $preposition role") {
                parsesTo[Statements](func(
                  action,
                  List(procedureQualifier("apoc.procedure")),
                  Seq(literalRole),
                  immutable
                )(pos))
              }

              test(s"$verb$immutableString $execute apoc.math.sin ON DBMS $preposition role") {
                parsesTo[Statements](func(
                  action,
                  List(procedureQualifier("apoc.math.sin")),
                  Seq(literalRole),
                  immutable
                )(pos))
              }

              test(s"$verb$immutableString $execute apoc* ON DBMS $preposition role") {
                parsesTo[Statements](func(action, List(procedureQualifier("apoc*")), Seq(literalRole), immutable)(pos))
              }

              test(s"$verb$immutableString $execute *apoc ON DBMS $preposition role") {
                parsesTo[Statements](func(action, List(procedureQualifier("*apoc")), Seq(literalRole), immutable)(pos))
              }

              test(s"$verb$immutableString $execute *apoc, *.sin ON DBMS $preposition role") {
                parsesTo[Statements](func(
                  action,
                  List(procedureQualifier("*apoc"), procedureQualifier("*.sin")),
                  Seq(literalRole),
                  immutable
                )(pos))
              }

              test(s"$verb$immutableString $execute *.sin, apoc* ON DBMS $preposition role") {
                parsesTo[Statements](func(
                  action,
                  List(procedureQualifier("*.sin"), procedureQualifier("apoc*")),
                  Seq(literalRole),
                  immutable
                )(pos))
              }

              test(s"$verb$immutableString $execute *.sin ON DBMS $preposition role") {
                parsesTo[Statements](func(action, List(procedureQualifier("*.sin")), Seq(literalRole), immutable)(pos))
              }

              test(s"$verb$immutableString $execute apoc.*.math.* ON DBMS $preposition role") {
                parsesTo[Statements](func(
                  action,
                  List(procedureQualifier("apoc.*.math.*")),
                  Seq(literalRole),
                  immutable
                )(pos))
              }

              test(s"$verb$immutableString $execute math.*n ON DBMS $preposition role") {
                parsesTo[Statements](
                  func(action, List(procedureQualifier("math.*n")), Seq(literalRole), immutable)(pos)
                )
              }

              test(s"$verb$immutableString $execute math.si? ON DBMS $preposition role") {
                parsesTo[Statements](
                  func(action, List(procedureQualifier("math.si?")), Seq(literalRole), immutable)(pos)
                )
              }

              test(s"$verb$immutableString $execute mat*.sin ON DBMS $preposition role") {
                parsesTo[Statements](
                  func(action, List(procedureQualifier("mat*.sin")), Seq(literalRole), immutable)(pos)
                )
              }

              test(s"$verb$immutableString $execute mat?.sin ON DBMS $preposition role") {
                parsesTo[Statements](
                  func(action, List(procedureQualifier("mat?.sin")), Seq(literalRole), immutable)(pos)
                )
              }

              test(s"$verb$immutableString $execute ?ath.sin ON DBMS $preposition role") {
                parsesTo[Statements](
                  func(action, List(procedureQualifier("?ath.sin")), Seq(literalRole), immutable)(pos)
                )
              }

              test(s"$verb$immutableString $execute mat?.`a.\n`.*n ON DBMS $preposition role") {
                parsesTo[Statements](func(
                  action,
                  List(procedureQualifier("mat?.a.\n.*n")),
                  Seq(literalRole),
                  immutable
                )(pos))
              }

              test(s"$verb$immutableString $execute `mat?`.`a.\n`.`*n` ON DBMS $preposition role") {
                parsesTo[Statements](func(
                  action,
                  List(procedureQualifier("mat?.a.\n.*n")),
                  Seq(literalRole),
                  immutable
                )(pos))
              }

              test(s"$verb$immutableString $execute `a b` ON DBMS $preposition role") {
                parsesTo[Statements](func(action, List(procedureQualifier("a b")), Seq(literalRole), immutable)(pos))
              }

              test(s"$verb$immutableString $execute a b ON DBMS $preposition role") {
                assertAstNotAntlr(
                  func(action, List(ProcedureQualifier("ab")(defaultPos)), Seq(literalRole), immutable)(defaultPos)
                )
              }

              test(s"$verb$immutableString $execute math. ON DBMS $preposition role") {
                parsesTo[Statements](func(action, List(procedureQualifier("math.")), Seq(literalRole), immutable)(pos))
              }

              test(s"$verb$immutableString $execute `math.` ON DBMS $preposition role") {
                parsesTo[Statements](func(action, List(procedureQualifier("math.")), Seq(literalRole), immutable)(pos))
              }

              test(s"$verb$immutableString $execute math.`sin.`. ON DBMS $preposition role") {
                parsesTo[Statements](
                  func(action, List(procedureQualifier("math.sin..")), Seq(literalRole), immutable)(pos)
                )
              }

              test(s"$verb$immutableString $execute apoc.math.* ON DBMS $preposition role") {
                parsesTo[Statements](
                  func(action, List(procedureQualifier("apoc.math.*")), Seq(literalRole), immutable)(pos)
                )
              }

              test(s"$verb$immutableString $execute math.sin, math.cos ON DBMS $preposition role") {
                parsesTo[Statements](func(
                  action,
                  List(procedureQualifier("math.sin"), procedureQualifier("math.cos")),
                  Seq(literalRole),
                  immutable
                )(pos))
              }

              test(s"$verb$immutableString $execute apoc.math.sin, math.* ON DBMS $preposition role") {
                parsesTo[Statements](func(
                  action,
                  List(procedureQualifier("apoc.math.sin"), procedureQualifier("math.*")),
                  Seq(literalRole),
                  immutable
                )(pos))
              }

              test(s"$verb$immutableString $execute * $preposition role") {
                val offset = testName.length
                assertFailsWithMessage[Statements](
                  testName,
                  s"""Invalid input '': expected
                     |  "*"
                     |  "."
                     |  "?"
                     |  "ON"
                     |  an identifier (line 1, column ${offset + 1} (offset: $offset))""".stripMargin
                )
              }

              test(s"$verb$immutableString $execute * ON DATABASE * $preposition role") {
                val offset = testName.length
                assertFailsWithMessage[Statements](
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

              test(s"$verb$immutableString $execute `ab?`* ON DBMS $preposition role") {
                val offset = s"$verb$immutableString $execute ".length
                assertFailsWithMessage[Statements](
                  testName,
                  s"""Invalid input 'ab?': expected "*", ".", "?" or an identifier (line 1, column ${offset + 1} (offset: $offset))""".stripMargin
                )
              }

              test(s"$verb$immutableString $execute a`ab?` ON DBMS $preposition role") {
                val offset = s"$verb$immutableString $execute a".length
                assertFailsWithMessage[Statements](
                  testName,
                  s"""Invalid input 'ab?': expected
                     |  "*"
                     |  "."
                     |  "?"
                     |  "ON"
                     |  an identifier (line 1, column ${offset + 1} (offset: $offset))""".stripMargin
                )
              }

              test(s"$verb$immutableString $execute ab?`%ab`* ON DBMS $preposition role") {
                val offset = s"$verb$immutableString $execute ab?".length
                assertFailsWithMessage[Statements](
                  testName,
                  s"""Invalid input '%ab': expected
                     |  "*"
                     |  "."
                     |  "?"
                     |  "NFKD"
                     |  "ON"
                     |  an identifier (line 1, column ${offset + 1} (offset: $offset))""".stripMargin
                )
              }

              test(s"$verb$immutableString $execute apoc.`*`ab? ON DBMS $preposition role") {
                val offset = s"$verb$immutableString $execute apoc.".length
                assertFailsWithMessage[Statements](
                  testName,
                  s"""Invalid input '*': expected
                     |  "*"
                     |  "."
                     |  "?"
                     |  "NFKD"
                     |  an identifier (line 1, column ${offset + 1} (offset: $offset))""".stripMargin
                )
              }

              test(s"$verb$immutableString $execute apoc.*`ab?` ON DBMS $preposition role") {
                val offset = s"$verb$immutableString $execute apoc.*".length
                assertFailsWithMessage[Statements](
                  testName,
                  s"""Invalid input 'ab?': expected
                     |  "*"
                     |  "."
                     |  "?"
                     |  "ON"
                     |  an identifier (line 1, column ${offset + 1} (offset: $offset))""".stripMargin
                )
              }

              test(s"$verb$immutableString $execute `ap`oc.ab? ON DBMS $preposition role") {
                val offset = s"$verb$immutableString $execute ".length
                assertFailsWithMessage[Statements](
                  testName,
                  s"""Invalid input 'ap': expected "*", ".", "?" or an identifier (line 1, column ${offset + 1} (offset: $offset))""".stripMargin
                )
              }

              test(s"$verb$immutableString $execute ap`oc`.ab? ON DBMS $preposition role") {
                val offset = s"$verb$immutableString $execute ap".length
                assertFailsWithMessage[Statements](
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
  }

  Seq(
    ("GRANT", "TO", grantDbmsPrivilege: dbmsPrivilegeFunc),
    ("DENY", "TO", denyDbmsPrivilege: dbmsPrivilegeFunc),
    ("REVOKE GRANT", "FROM", revokeGrantDbmsPrivilege: dbmsPrivilegeFunc),
    ("REVOKE DENY", "FROM", revokeDenyDbmsPrivilege: dbmsPrivilegeFunc),
    ("REVOKE", "FROM", revokeDbmsPrivilege: dbmsPrivilegeFunc)
  ).foreach {
    case (verb: String, preposition: String, func: dbmsPrivilegeFunc) =>
      Seq[Immutable](true, false).foreach {
        immutable =>
          val immutableString = immutableOrEmpty(immutable)
          Seq(
            "EXECUTE ADMIN PROCEDURES",
            "EXECUTE ADMINISTRATOR PROCEDURES"
          ).foreach {
            command =>
              test(s"$verb$immutableString $command ON DBMS $preposition role") {
                parsesTo[Statements](func(ExecuteAdminProcedureAction, Seq(literalRole), immutable)(pos))
              }

              test(s"$verb$immutableString $command * ON DBMS $preposition role") {
                val offset = s"$verb$immutableString $command ".length
                assertFailsWithMessage[Statements](
                  testName,
                  s"""Invalid input '*': expected "ON" (line 1, column ${offset + 1} (offset: $offset))""".stripMargin
                )
              }

              test(s"$verb$immutableString $command ON DATABASE * $preposition role") {
                val offset = s"$verb$immutableString $command ON ".length
                assertFailsWithMessage[Statements](
                  testName,
                  s"""Invalid input 'DATABASE': expected "DBMS" (line 1, column ${offset + 1} (offset: $offset))""".stripMargin
                )
              }

          }

          test(s"$verb$immutableString EXECUTE ADMIN PROCEDURE ON DBMS $preposition role") {
            val offset = s"$verb$immutableString EXECUTE ADMIN ".length
            assertFailsWithMessage[Statements](
              testName,
              s"""Invalid input 'PROCEDURE': expected "PROCEDURES" (line 1, column ${offset + 1} (offset: $offset))""".stripMargin
            )
          }
      }
  }

  private def procedureQualifier(procName: String): InputPosition => ProcedureQualifier =
    ProcedureQualifier(procName)(_)
}
