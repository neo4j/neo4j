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

import org.neo4j.cypher.internal.ast.SettingQualifier
import org.neo4j.cypher.internal.ast.ShowSettingAction
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.factory.neo4j.AdministrationAndSchemaCommandParserTestBase
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsing.Antlr
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsing.JavaCc
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.exceptions.SyntaxException

class ShowSettingPrivilegeAdministrationCommandParserTest extends AdministrationAndSchemaCommandParserTestBase {

  Seq(
    ("GRANT", "TO", grantShowSettingPrivilege: settingPrivilegeFunc),
    ("DENY", "TO", denyShowSettingPrivilege: settingPrivilegeFunc),
    ("REVOKE GRANT", "FROM", revokeGrantShowSettingPrivilege: settingPrivilegeFunc),
    ("REVOKE DENY", "FROM", revokeDenyShowSettingPrivilege: settingPrivilegeFunc),
    ("REVOKE", "FROM", revokeShowSettingPrivilege: settingPrivilegeFunc)
  ).foreach {
    case (verb: String, preposition: String, func: settingPrivilegeFunc) =>
      Seq[Immutable](true, false).foreach {
        immutable =>
          val immutableString = immutableOrEmpty(immutable)
          Seq(
            ("SHOW SETTING", ShowSettingAction)
          ).foreach {
            case (command, action) =>
              test(s"$verb$immutableString $command * ON DBMS $preposition role") {
                parsesTo[Statements](func(action, List(settingQualifier("*")), Seq(literalRole), immutable)(pos))
              }

              test(s"$verb$immutableString ${command}S * ON DBMS $preposition role") {
                parsesTo[Statements](func(action, List(settingQualifier("*")), Seq(literalRole), immutable)(pos))
              }

              test(s"$verb$immutableString ${command}S `*` ON DBMS $preposition role") {
                parsesTo[Statements](func(action, List(settingQualifier("*")), Seq(literalRole), immutable)(pos))
              }

              test(s"$verb$immutableString $command dbms.cypher.planner ON DBMS $preposition role") {
                parsesTo[Statements](func(
                  action,
                  List(settingQualifier("dbms.cypher.planner")),
                  Seq(literalRole),
                  immutable
                )(pos))
              }

              test(s"$verb$immutableString ${command}S dbms.cypher.planner ON DBMS $preposition role") {
                parsesTo[Statements](func(
                  action,
                  List(settingQualifier("dbms.cypher.planner")),
                  Seq(literalRole),
                  immutable
                )(pos))
              }

              test(s"$verb$immutableString $command dbms.* ON DBMS $preposition role") {
                parsesTo[Statements](func(action, List(settingQualifier("dbms.*")), Seq(literalRole), immutable)(pos))
              }

              test(s"$verb$immutableString ${command}S dbms.* ON DBMS $preposition role") {
                parsesTo[Statements](func(action, List(settingQualifier("dbms.*")), Seq(literalRole), immutable)(pos))
              }

              test(s"$verb$immutableString $command *cypher* ON DBMS $preposition role") {
                parsesTo[Statements](func(action, List(settingQualifier("*cypher*")), Seq(literalRole), immutable)(pos))
              }

              test(s"$verb$immutableString $command *cypher*, *metrics* ON DBMS $preposition role") {
                parsesTo[Statements](func(
                  action,
                  List(settingQualifier("*cypher*"), settingQualifier("*metrics*")),
                  Seq(literalRole),
                  immutable
                )(pos))
              }

              test(s"$verb$immutableString $command *memory, dbms* ON DBMS $preposition role") {
                parsesTo[Statements](func(
                  action,
                  List(settingQualifier("*memory"), settingQualifier("dbms*")),
                  Seq(literalRole),
                  immutable
                )(pos))
              }

              test(s"$verb$immutableString $command dbms.*.memory.* ON DBMS $preposition role") {
                parsesTo[Statements](func(
                  action,
                  List(settingQualifier("dbms.*.memory.*")),
                  Seq(literalRole),
                  immutable
                )(pos))
              }

              test(s"$verb$immutableString $command d?.transaction.timeout ON DBMS $preposition role") {
                parsesTo[Statements](func(
                  action,
                  List(settingQualifier("d?.transaction.timeout")),
                  Seq(literalRole),
                  immutable
                )(pos))
              }

              test(s"$verb$immutableString $command ?b.transaction.timeout ON DBMS $preposition role") {
                parsesTo[Statements](func(
                  action,
                  List(settingQualifier("?b.transaction.timeout")),
                  Seq(literalRole),
                  immutable
                )(pos))
              }

              test(s"$verb$immutableString $command mat?.`a.\n`.*n ON DBMS $preposition role") {
                parsesTo[Statements](
                  func(action, List(settingQualifier("mat?.a.\n.*n")), Seq(literalRole), immutable)(pos)
                )
              }

              test(s"$verb$immutableString $command `mat?`.`a.\n`.`*n` ON DBMS $preposition role") {
                parsesTo[Statements](
                  func(action, List(settingQualifier("mat?.a.\n.*n")), Seq(literalRole), immutable)(pos)
                )
              }

              test(s"$verb$immutableString $command `a b` ON DBMS $preposition role") {
                parsesTo[Statements](func(action, List(settingQualifier("a b")), Seq(literalRole), immutable)(pos))
              }

              test(s"$verb$immutableString $command a b ON DBMS $preposition role") {
                assertAstNotAntlr(
                  func(action, List(settingQualifier("ab")(defaultPos)), Seq(literalRole), immutable)(defaultPos)
                )
              }

              test(s"$verb$immutableString ${command}S dbms. ON DBMS $preposition role") {
                parsesTo[Statements](func(action, List(settingQualifier("dbms.")), Seq(literalRole), immutable)(pos))
              }

              test(s"$verb$immutableString ${command}S `dbms.` ON DBMS $preposition role") {
                parsesTo[Statements](func(action, List(settingQualifier("dbms.")), Seq(literalRole), immutable)(pos))
              }

              test(s"$verb$immutableString ${command}S db.transaction.concurrent. ON DBMS $preposition role") {
                parsesTo[Statements](func(
                  action,
                  List(settingQualifier("db.transaction.concurrent.")),
                  Seq(literalRole),
                  immutable
                )(pos))
              }

              test(s"$verb$immutableString ${command}S `db.transaction.concurrent..` ON DBMS $preposition role") {
                parsesTo[Statements](func(
                  action,
                  List(settingQualifier("db.transaction.concurrent..")),
                  Seq(literalRole),
                  immutable
                )(pos))
              }

              test(
                s"$verb$immutableString $command db.transaction.timeout, db.transaction.concurrent.maximum ON DBMS $preposition role"
              ) {
                parsesTo[Statements](func(
                  action,
                  List(
                    settingQualifier("db.transaction.timeout"),
                    settingQualifier("db.transaction.concurrent.maximum")
                  ),
                  Seq(literalRole),
                  immutable
                )(pos))
              }

              test(s"$verb$immutableString $command * $preposition role") {
                val offset = testName.length
                failsParsing[Statements]
                  .parseIn(JavaCc)(_.withMessage(
                    s"""Invalid input '': expected
                       |  "*"
                       |  "."
                       |  "?"
                       |  "ON"
                       |  an identifier (line 1, column ${offset + 1} (offset: $offset))""".stripMargin
                  ))
                  .parseIn(Antlr)(_.throws[SyntaxException].withMessageStart(
                    s"""Invalid input '': expected an identifier, '*', ',', '.', '?' or 'ON DBMS' (line 1, column ${offset + 1} (offset: $offset))""".stripMargin
                  ))
              }

              test(s"$verb$immutableString $command * ON DATABASE * $preposition role") {
                val offset = testName.length
                failsParsing[Statements]
                  .parseIn(JavaCc)(_.withMessage(
                    s"""Invalid input '': expected
                       |  "*"
                       |  "."
                       |  "?"
                       |  "ON"
                       |  an identifier (line 1, column ${offset + 1} (offset: $offset))""".stripMargin
                  ))
                  .parseIn(Antlr)(_.throws[SyntaxException].withMessageStart(
                    s"""Invalid input '': expected an identifier, '*', ',', '.', '?' or 'ON DBMS' (line 1, column ${offset + 1} (offset: $offset))"""
                  ))
              }

              // Tests for invalid escaping

              test(s"$verb$immutableString $command `ab?`* ON DBMS $preposition role") {
                val offset = s"$verb$immutableString $command ".length
                failsParsing[Statements]
                  .parseIn(JavaCc)(_.withMessage(
                    s"""Invalid input 'ab?': expected "*", ".", "?" or an identifier (line 1, column ${offset + 1} (offset: $offset))""".stripMargin
                  ))
                  .parseIn(Antlr)(_.throws[SyntaxException].withMessageStart(
                    """Each part of the glob (a block of text up until a dot) must either be fully escaped or not escaped at all."""
                  ))
              }

              test(s"$verb$immutableString $command a`ab?` ON DBMS $preposition role") {
                val offset = s"$verb$immutableString $command a".length
                failsParsing[Statements]
                  .parseIn(JavaCc)(_.withMessage(
                    s"""Invalid input 'ab?': expected
                       |  "*"
                       |  "."
                       |  "?"
                       |  "ON"
                       |  an identifier (line 1, column ${offset + 1} (offset: $offset))""".stripMargin
                  ))
                  .parseIn(Antlr)(_.throws[SyntaxException].withMessageStart(
                    s"""Invalid input '`ab?`': expected an identifier, '*', ',', '.', '?' or 'ON DBMS' (line 1, column ${offset + 1} (offset: $offset))"""
                  ))
              }

              test(s"$verb$immutableString $command ab?`%ab`* ON DBMS $preposition role") {
                val offset = s"$verb$immutableString $command ab?".length
                failsParsing[Statements]
                  .parseIn(JavaCc)(_.withMessage(
                    s"""Invalid input '%ab': expected
                       |  "*"
                       |  "."
                       |  "?"
                       |  "NFKD"
                       |  "ON"
                       |  an identifier (line 1, column ${offset + 1} (offset: $offset))""".stripMargin
                  ))
                  .parseIn(Antlr)(_.throws[SyntaxException].withMessageStart(
                    s"""Invalid input '`%ab`': expected an identifier, '*', ',', '.', '?' or 'ON DBMS' (line 1, column ${offset + 1} (offset: $offset))"""
                  ))
              }

              test(s"$verb$immutableString $command dbms.`*`ab? ON DBMS $preposition role") {
                val offset = s"$verb$immutableString $command dbms.".length
                failsParsing[Statements]
                  .parseIn(JavaCc)(_.withMessage(
                    s"""Invalid input '*': expected
                       |  "*"
                       |  "."
                       |  "?"
                       |  "NFKD"
                       |  an identifier (line 1, column ${offset + 1} (offset: $offset))""".stripMargin
                  ))
                  .parseIn(Antlr)(_.throws[SyntaxException].withMessageStart(
                    """Each part of the glob (a block of text up until a dot) must either be fully escaped or not escaped at all."""
                  ))
              }

              test(s"$verb$immutableString $command dbms.*`ab?` ON DBMS $preposition role") {
                val offset = s"$verb$immutableString $command dbms.*".length
                failsParsing[Statements]
                  .parseIn(JavaCc)(_.withMessage(
                    s"""Invalid input 'ab?': expected
                       |  "*"
                       |  "."
                       |  "?"
                       |  "ON"
                       |  an identifier (line 1, column ${offset + 1} (offset: $offset))""".stripMargin
                  ))
                  .parseIn(Antlr)(_.throws[SyntaxException].withMessageStart(
                    s"""Invalid input '`ab?`': expected an identifier, '*', ',', '.', '?' or 'ON DBMS' (line 1, column ${offset + 1} (offset: $offset))"""
                  ))
              }

              test(s"$verb$immutableString $command `db`ms.ab? ON DBMS $preposition role") {
                val offset = s"$verb$immutableString $command ".length
                failsParsing[Statements]
                  .parseIn(JavaCc)(_.withMessage(
                    s"""Invalid input 'db': expected "*", ".", "?" or an identifier (line 1, column ${offset + 1} (offset: $offset))""".stripMargin
                  ))
                  .parseIn(Antlr)(_.throws[SyntaxException].withMessageStart(
                    """Each part of the glob (a block of text up until a dot) must either be fully escaped or not escaped at all."""
                  ))
              }

              test(s"$verb$immutableString $command db`ms`.ab? ON DBMS $preposition role") {
                val offset = s"$verb$immutableString $command db".length
                failsParsing[Statements]
                  .parseIn(JavaCc)(_.withMessage(
                    s"""Invalid input 'ms': expected
                       |  "*"
                       |  "."
                       |  "?"
                       |  "ON"
                       |  an identifier (line 1, column ${offset + 1} (offset: $offset))""".stripMargin
                  ))
                  .parseIn(Antlr)(_.throws[SyntaxException].withMessageStart(
                    s"""Invalid input '`ms`': expected an identifier, '*', ',', '.', '?' or 'ON DBMS' (line 1, column ${offset + 1} (offset: $offset))"""
                  ))
              }

              test(s"$verb$immutableString $command $$param ON DBMS $preposition role") {
                val offset = s"$verb$immutableString $command ".length
                failsParsing[Statements]
                  .parseIn(JavaCc)(_.withMessage(
                    s"""Invalid input '$$': expected "*", ".", "?" or an identifier (line 1, column ${offset + 1} (offset: $offset))""".stripMargin
                  ))
                  .parseIn(Antlr)(_.throws[SyntaxException].withMessageStart(
                    s"""Invalid input '$$': expected an identifier, '*', '.' or '?' (line 1, column ${offset + 1} (offset: $offset))"""
                  ))
              }
          }

          test(s"$verb$immutableString SHOW SOME SETTING * ON DATABASE * $preposition role") {
            val offset = s"$verb$immutableString SHOW ".length
            failsParsing[Statements]
              .parseIn(JavaCc)(_.withMessage(
                s"""Invalid input 'SOME': expected
                   |  "ALIAS"
                   |  "CONSTRAINT"
                   |  "CONSTRAINTS"
                   |  "INDEX"
                   |  "INDEXES"
                   |  "PRIVILEGE"
                   |  "ROLE"
                   |  "SERVER"
                   |  "SERVERS"
                   |  "SETTING"
                   |  "SETTINGS"
                   |  "TRANSACTION"
                   |  "TRANSACTIONS"
                   |  "USER" (line 1, column ${offset + 1} (offset: $offset))""".stripMargin
              ))
              .parseIn(Antlr)(_.throws[SyntaxException].withMessageStart(
                s"""Invalid input 'SOME': expected 'ALIAS', 'CONSTRAINT', 'CONSTRAINTS', 'INDEX', 'INDEXES', 'PRIVILEGE', 'ROLE', 'SERVER', 'SERVERS', 'SETTING', 'TRANSACTION', 'TRANSACTIONS' or 'USER' (line 1, column ${offset + 1} (offset: $offset))""".stripMargin
              ))
          }
      }
  }

  private def settingQualifier(glob: String): InputPosition => SettingQualifier =
    SettingQualifier(glob)(_)
}
