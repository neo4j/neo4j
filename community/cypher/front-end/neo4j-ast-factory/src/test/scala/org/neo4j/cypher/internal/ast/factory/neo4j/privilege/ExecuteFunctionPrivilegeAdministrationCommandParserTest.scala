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

import org.neo4j.cypher.internal.ast.ExecuteBoostedFunctionAction
import org.neo4j.cypher.internal.ast.ExecuteFunctionAction
import org.neo4j.cypher.internal.ast.FunctionQualifier
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.factory.neo4j.AdministrationAndSchemaCommandParserTestBase
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsing.Antlr
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsing.JavaCc
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.exceptions.SyntaxException

class ExecuteFunctionPrivilegeAdministrationCommandParserTest extends AdministrationAndSchemaCommandParserTestBase {

  Seq(
    ("GRANT", "TO", grantExecuteFunctionPrivilege: executeFunctionPrivilegeFunc),
    ("DENY", "TO", denyExecuteFunctionPrivilege: executeFunctionPrivilegeFunc),
    ("REVOKE GRANT", "FROM", revokeGrantExecuteFunctionPrivilege: executeFunctionPrivilegeFunc),
    ("REVOKE DENY", "FROM", revokeDenyExecuteFunctionPrivilege: executeFunctionPrivilegeFunc),
    ("REVOKE", "FROM", revokeExecuteFunctionPrivilege: executeFunctionPrivilegeFunc)
  ).foreach {
    case (verb: String, preposition: String, func: executeFunctionPrivilegeFunc) =>
      Seq[Immutable](true, false).foreach {
        immutable =>
          val immutableString = immutableOrEmpty(immutable)
          Seq(
            ("EXECUTE FUNCTION", ExecuteFunctionAction),
            ("EXECUTE USER FUNCTION", ExecuteFunctionAction),
            ("EXECUTE USER DEFINED FUNCTION", ExecuteFunctionAction),
            ("EXECUTE BOOSTED FUNCTION", ExecuteBoostedFunctionAction),
            ("EXECUTE BOOSTED USER FUNCTION", ExecuteBoostedFunctionAction),
            ("EXECUTE BOOSTED USER DEFINED FUNCTION", ExecuteBoostedFunctionAction)
          ).foreach {
            case (execute, action) =>
              test(s"$verb$immutableString $execute * ON DBMS $preposition role") {
                parsesTo[Statements](func(action, List(functionQualifier("*")), Seq(literalRole), immutable)(pos))
              }

              // The following two tests check that the plural form EXECUTE ... FUNCTIONS is valid

              test(s"$verb$immutableString ${execute}S * ON DBMS $preposition role") {
                parsesTo[Statements](func(action, List(functionQualifier("*")), Seq(literalRole), immutable)(pos))
              }

              test(s"$verb$immutableString ${execute}S `*` ON DBMS $preposition role") {
                parsesTo[Statements](func(action, List(functionQualifier("*")), Seq(literalRole), immutable)(pos))
              }

              test(s"$verb$immutableString $execute apoc.function ON DBMS $preposition role") {
                parsesTo[Statements](func(
                  action,
                  List(functionQualifier("apoc.function")),
                  Seq(literalRole),
                  immutable
                )(pos))
              }

              test(s"$verb$immutableString ${execute}S apoc.function ON DBMS $preposition role") {
                parsesTo[Statements](func(
                  action,
                  List(functionQualifier("apoc.function")),
                  Seq(literalRole),
                  immutable
                )(pos))
              }

              test(s"$verb$immutableString $execute apoc.math.sin ON DBMS $preposition role") {
                parsesTo[Statements](func(
                  action,
                  List(functionQualifier("apoc.math.sin")),
                  Seq(literalRole),
                  immutable
                )(pos))
              }

              test(s"$verb$immutableString $execute apoc* ON DBMS $preposition role") {
                parsesTo[Statements](func(action, List(functionQualifier("apoc*")), Seq(literalRole), immutable)(pos))
              }

              test(s"$verb$immutableString $execute *apoc ON DBMS $preposition role") {
                parsesTo[Statements](func(action, List(functionQualifier("*apoc")), Seq(literalRole), immutable)(pos))
              }

              test(s"$verb$immutableString $execute *apoc, *.sin ON DBMS $preposition role") {
                parsesTo[Statements](func(
                  action,
                  List(functionQualifier("*apoc"), functionQualifier("*.sin")),
                  Seq(literalRole),
                  immutable
                )(pos))
              }

              test(s"$verb$immutableString $execute *.sin, apoc* ON DBMS $preposition role") {
                parsesTo[Statements](func(
                  action,
                  List(functionQualifier("*.sin"), functionQualifier("apoc*")),
                  Seq(literalRole),
                  immutable
                )(pos))
              }

              test(s"$verb$immutableString $execute *.sin ON DBMS $preposition role") {
                parsesTo[Statements](func(action, List(functionQualifier("*.sin")), Seq(literalRole), immutable)(pos))
              }

              test(s"$verb$immutableString $execute apoc.*.math.* ON DBMS $preposition role") {
                parsesTo[Statements](func(
                  action,
                  List(functionQualifier("apoc.*.math.*")),
                  Seq(literalRole),
                  immutable
                )(pos))
              }

              test(s"$verb$immutableString $execute math.*n ON DBMS $preposition role") {
                parsesTo[Statements](func(action, List(functionQualifier("math.*n")), Seq(literalRole), immutable)(pos))
              }

              test(s"$verb$immutableString $execute math.si? ON DBMS $preposition role") {
                parsesTo[Statements](
                  func(action, List(functionQualifier("math.si?")), Seq(literalRole), immutable)(pos)
                )
              }

              test(s"$verb$immutableString $execute mat*.sin ON DBMS $preposition role") {
                parsesTo[Statements](
                  func(action, List(functionQualifier("mat*.sin")), Seq(literalRole), immutable)(pos)
                )
              }

              test(s"$verb$immutableString $execute mat?.sin ON DBMS $preposition role") {
                parsesTo[Statements](
                  func(action, List(functionQualifier("mat?.sin")), Seq(literalRole), immutable)(pos)
                )
              }

              test(s"$verb$immutableString $execute ?ath.sin ON DBMS $preposition role") {
                parsesTo[Statements](
                  func(action, List(functionQualifier("?ath.sin")), Seq(literalRole), immutable)(pos)
                )
              }

              test(s"$verb$immutableString $execute mat?.`a.\n`.*n ON DBMS $preposition role") {
                parsesTo[Statements](
                  func(action, List(functionQualifier("mat?.a.\n.*n")), Seq(literalRole), immutable)(pos)
                )
              }

              test(s"$verb$immutableString $execute `mat?`.`a.\n`.`*n` ON DBMS $preposition role") {
                parsesTo[Statements](
                  func(action, List(functionQualifier("mat?.a.\n.*n")), Seq(literalRole), immutable)(pos)
                )
              }

              test(s"$verb$immutableString $execute `a b` ON DBMS $preposition role") {
                parsesTo[Statements](func(action, List(functionQualifier("a b")), Seq(literalRole), immutable)(pos))
              }

              test(s"$verb$immutableString $execute a b ON DBMS $preposition role") {
                assertAst(
                  func(action, List(FunctionQualifier("ab")(defaultPos)), Seq(literalRole), immutable)(defaultPos)
                )
              }

              test(s"$verb$immutableString $execute math.sin. ON DBMS $preposition role") {
                parsesTo[Statements](
                  func(action, List(functionQualifier("math.sin.")), Seq(literalRole), immutable)(pos)
                )
              }

              test(s"$verb$immutableString $execute `math.sin.` ON DBMS $preposition role") {
                parsesTo[Statements](
                  func(action, List(functionQualifier("math.sin.")), Seq(literalRole), immutable)(pos)
                )
              }

              test(s"$verb$immutableString $execute math.sin, math.cos ON DBMS $preposition role") {
                parsesTo[Statements](func(
                  action,
                  List(functionQualifier("math.sin"), functionQualifier("math.cos")),
                  Seq(literalRole),
                  immutable
                )(pos))
              }

              test(s"$verb$immutableString $execute apoc.math.sin, math.* ON DBMS $preposition role") {
                parsesTo[Statements](func(
                  action,
                  List(functionQualifier("apoc.math.sin"), functionQualifier("math.*")),
                  Seq(literalRole),
                  immutable
                )(pos))
              }

              test(s"$verb$immutableString $execute apoc.math.sin, math.*, apoc* ON DBMS $preposition role") {
                parsesTo[Statements](func(
                  action,
                  List(functionQualifier("apoc.math.sin"), functionQualifier("math.*"), functionQualifier("apoc*")),
                  Seq(literalRole),
                  immutable
                )(pos))
              }

              test(s"$verb$immutableString $execute * $preposition role") {
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

              test(s"$verb$immutableString $execute * ON DATABASE * $preposition role") {
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

              test(s"$verb$immutableString $execute `ab?`* ON DBMS $preposition role") {
                val offset = s"$verb$immutableString $execute ".length
                failsParsing[Statements]
                  .parseIn(JavaCc)(_.withMessage(
                    s"""Invalid input 'ab?': expected "*", ".", "?" or an identifier (line 1, column ${offset + 1} (offset: $offset))""".stripMargin
                  ))
                  .parseIn(Antlr)(_.throws[SyntaxException].withMessageStart(
                    """Each part of the glob (a block of text up until a dot) must either be fully escaped or not escaped at all."""
                  ))
              }

              test(s"$verb$immutableString $execute a`ab?` ON DBMS $preposition role") {
                val offset = s"$verb$immutableString $execute a".length
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

              test(s"$verb$immutableString $execute ab?`%ab`* ON DBMS $preposition role") {
                val offset = s"$verb$immutableString $execute ab?".length
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
                    s"Invalid input '`%ab`': expected an identifier, '*', ',', '.', '?' or 'ON DBMS' (line 1, column ${offset + 1} (offset: $offset))"
                  ))
              }

              test(s"$verb$immutableString $execute apoc.`*`ab? ON DBMS $preposition role") {
                val offset = s"$verb$immutableString $execute apoc.".length
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

              test(s"$verb$immutableString $execute apoc.*`ab?` ON DBMS $preposition role") {
                val offset = s"$verb$immutableString $execute apoc.*".length
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
                    s"Invalid input '`ab?`': expected an identifier, '*', ',', '.', '?' or 'ON DBMS' (line 1, column ${offset + 1} (offset: $offset))"
                  ))
              }

              test(s"$verb$immutableString $execute `ap`oc.ab? ON DBMS $preposition role") {
                val offset = s"$verb$immutableString $execute ".length
                failsParsing[Statements]
                  .parseIn(JavaCc)(_.withMessage(
                    s"""Invalid input 'ap': expected "*", ".", "?" or an identifier (line 1, column ${offset + 1} (offset: $offset))""".stripMargin
                  ))
                  .parseIn(Antlr)(_.throws[SyntaxException].withMessageStart(
                    """Each part of the glob (a block of text up until a dot) must either be fully escaped or not escaped at all."""
                  ))
              }

              test(s"$verb$immutableString $execute ap`oc`.ab? ON DBMS $preposition role") {
                val offset = s"$verb$immutableString $execute ap".length
                failsParsing[Statements]
                  .parseIn(JavaCc)(_.withMessage(
                    s"""Invalid input 'oc': expected
                       |  "*"
                       |  "."
                       |  "?"
                       |  "ON"
                       |  an identifier (line 1, column ${offset + 1} (offset: $offset))""".stripMargin
                  ))
                  .parseIn(Antlr)(_.throws[SyntaxException].withMessageStart(
                    s"""Invalid input '`oc`': expected an identifier, '*', ',', '.', '?' or 'ON DBMS' (line 1, column ${offset + 1} (offset: $offset))"""
                  ))
              }
          }
          test(s"$verb$immutableString EXECUTE DEFINED FUNCTION * ON DATABASE * $preposition role") {
            val offset = s"$verb$immutableString EXECUTE ".length
            failsParsing[Statements]
              .parseIn(JavaCc)(_.withMessage(
                s"""Invalid input 'DEFINED': expected
                   |  "ADMIN"
                   |  "ADMINISTRATOR"
                   |  "BOOSTED"
                   |  "FUNCTION"
                   |  "FUNCTIONS"
                   |  "PROCEDURE"
                   |  "PROCEDURES"
                   |  "USER" (line 1, column ${offset + 1} (offset: $offset))""".stripMargin
              ))
              .parseIn(Antlr)(_.throws[SyntaxException].withMessageStart(
                s"""Invalid input 'DEFINED': expected 'ADMIN', 'ADMINISTRATOR', 'BOOSTED', 'FUNCTION', 'FUNCTIONS', 'PROCEDURE', 'PROCEDURES' or 'USER' (line 1, column ${offset + 1} (offset: $offset))"""
              ))
          }
      }
  }

  private def functionQualifier(glob: String): InputPosition => FunctionQualifier = FunctionQualifier(glob)(_)
}
