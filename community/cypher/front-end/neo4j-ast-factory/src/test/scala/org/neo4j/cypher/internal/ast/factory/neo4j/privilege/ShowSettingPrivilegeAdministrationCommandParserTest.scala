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

import org.neo4j.cypher.internal.ast.SettingQualifier
import org.neo4j.cypher.internal.ast.ShowSettingAction
import org.neo4j.cypher.internal.ast.factory.neo4j.AdministrationAndSchemaCommandParserTestBase
import org.neo4j.cypher.internal.util.InputPosition

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
                yields(func(action, List(settingQualifier("*")), Seq(literalRole), immutable))
              }

              test(s"$verb$immutableString ${command}S * ON DBMS $preposition role") {
                yields(func(action, List(settingQualifier("*")), Seq(literalRole), immutable))
              }

              test(s"$verb$immutableString ${command}S `*` ON DBMS $preposition role") {
                yields(func(action, List(settingQualifier("*")), Seq(literalRole), immutable))
              }

              test(s"$verb$immutableString $command dbms.cypher.planner ON DBMS $preposition role") {
                yields(func(action, List(settingQualifier("dbms.cypher.planner")), Seq(literalRole), immutable))
              }

              test(s"$verb$immutableString ${command}S dbms.cypher.planner ON DBMS $preposition role") {
                yields(func(action, List(settingQualifier("dbms.cypher.planner")), Seq(literalRole), immutable))
              }

              test(s"$verb$immutableString $command dbms.* ON DBMS $preposition role") {
                yields(func(action, List(settingQualifier("dbms.*")), Seq(literalRole), immutable))
              }

              test(s"$verb$immutableString ${command}S dbms.* ON DBMS $preposition role") {
                yields(func(action, List(settingQualifier("dbms.*")), Seq(literalRole), immutable))
              }

              test(s"$verb$immutableString $command *cypher* ON DBMS $preposition role") {
                yields(func(action, List(settingQualifier("*cypher*")), Seq(literalRole), immutable))
              }

              test(s"$verb$immutableString $command *cypher*, *metrics* ON DBMS $preposition role") {
                yields(func(
                  action,
                  List(settingQualifier("*cypher*"), settingQualifier("*metrics*")),
                  Seq(literalRole),
                  immutable
                ))
              }

              test(s"$verb$immutableString $command *memory, dbms* ON DBMS $preposition role") {
                yields(func(
                  action,
                  List(settingQualifier("*memory"), settingQualifier("dbms*")),
                  Seq(literalRole),
                  immutable
                ))
              }

              test(s"$verb$immutableString $command dbms.*.memory.* ON DBMS $preposition role") {
                yields(func(action, List(settingQualifier("dbms.*.memory.*")), Seq(literalRole), immutable))
              }

              test(s"$verb$immutableString $command d?.transaction.timeout ON DBMS $preposition role") {
                yields(func(
                  action,
                  List(settingQualifier("d?.transaction.timeout")),
                  Seq(literalRole),
                  immutable
                ))
              }

              test(s"$verb$immutableString $command ?b.transaction.timeout ON DBMS $preposition role") {
                yields(func(
                  action,
                  List(settingQualifier("?b.transaction.timeout")),
                  Seq(literalRole),
                  immutable
                ))
              }

              test(s"$verb$immutableString $command mat?.`a.\n`.*n ON DBMS $preposition role") {
                yields(func(action, List(settingQualifier("mat?.a.\n.*n")), Seq(literalRole), immutable))
              }

              test(s"$verb$immutableString $command `mat?`.`a.\n`.`*n` ON DBMS $preposition role") {
                yields(func(action, List(settingQualifier("mat?.a.\n.*n")), Seq(literalRole), immutable))
              }

              test(s"$verb$immutableString $command `a b` ON DBMS $preposition role") {
                yields(func(action, List(settingQualifier("a b")), Seq(literalRole), immutable))
              }

              test(s"$verb$immutableString $command a b ON DBMS $preposition role") {
                assertAst(
                  func(action, List(settingQualifier("ab")(defaultPos)), Seq(Left("role")), immutable)(defaultPos)
                )
              }

              test(
                s"$verb$immutableString $command db.transaction.timeout, db.transaction.concurrent.maximum ON DBMS $preposition role"
              ) {
                yields(func(
                  action,
                  List(
                    settingQualifier("db.transaction.timeout"),
                    settingQualifier("db.transaction.concurrent.maximum")
                  ),
                  Seq(literalRole),
                  immutable
                ))
              }

              test(s"$verb$immutableString $command * $preposition role") {
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

              test(s"$verb$immutableString $command * ON DATABASE * $preposition role") {
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

              test(s"$verb$immutableString $command `ab?`* ON DBMS $preposition role") {
                val offset = s"$verb$immutableString $command ".length
                assertFailsWithMessage(
                  testName,
                  s"""Invalid input 'ab?': expected "*", ".", "?" or an identifier (line 1, column ${offset + 1} (offset: $offset))""".stripMargin
                )
              }

              test(s"$verb$immutableString $command a`ab?` ON DBMS $preposition role") {
                val offset = s"$verb$immutableString $command a".length
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

              test(s"$verb$immutableString $command ab?`%ab`* ON DBMS $preposition role") {
                val offset = s"$verb$immutableString $command ab?".length
                assertFailsWithMessage(
                  testName,
                  s"""Invalid input '%ab': expected
                     |  "*"
                     |  "."
                     |  "?"
                     |  "ON"
                     |  "TYPED"
                     |  an identifier (line 1, column ${offset + 1} (offset: $offset))""".stripMargin
                )
              }

              test(s"$verb$immutableString $command dbms.`*`ab? ON DBMS $preposition role") {
                val offset = s"$verb$immutableString $command dbms.".length
                assertFailsWithMessage(
                  testName,
                  s"""Invalid input '*': expected
                     |  "*"
                     |  "."
                     |  "?"
                     |  "TYPED"
                     |  an identifier (line 1, column ${offset + 1} (offset: $offset))""".stripMargin
                )
              }

              test(s"$verb$immutableString $command dbms.*`ab?` ON DBMS $preposition role") {
                val offset = s"$verb$immutableString $command dbms.*".length
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

              test(s"$verb$immutableString $command `db`ms.ab? ON DBMS $preposition role") {
                val offset = s"$verb$immutableString $command ".length
                assertFailsWithMessage(
                  testName,
                  s"""Invalid input 'db': expected "*", ".", "?" or an identifier (line 1, column ${offset + 1} (offset: $offset))""".stripMargin
                )
              }

              test(s"$verb$immutableString $command db`ms`.ab? ON DBMS $preposition role") {
                val offset = s"$verb$immutableString $command db".length
                assertFailsWithMessage(
                  testName,
                  s"""Invalid input 'ms': expected
                     |  "*"
                     |  "."
                     |  "?"
                     |  "ON"
                     |  an identifier (line 1, column ${offset + 1} (offset: $offset))""".stripMargin
                )
              }

              test(s"$verb$immutableString $command $$param ON DBMS $preposition role") {
                val offset = s"$verb$immutableString $command ".length
                assertFailsWithMessage(
                  testName,
                  s"""Invalid input '$$': expected "*", ".", "?" or an identifier (line 1, column ${offset + 1} (offset: $offset))""".stripMargin
                )
              }
          }

          test(s"$verb$immutableString SHOW SOME SETTING * ON DATABASE * $preposition role") {
            val offset = s"$verb$immutableString SHOW ".length
            assertFailsWithMessage(
              testName,
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
            )
          }
      }
  }

  private def settingQualifier(glob: String): InputPosition => SettingQualifier =
    SettingQualifier(glob)(_)
}
