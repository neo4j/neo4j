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
package org.neo4j.cypher.internal.ast.factory.ddl

import org.neo4j.cypher.internal.ast.AlterDatabase
import org.neo4j.cypher.internal.ast.IndefiniteWait
import org.neo4j.cypher.internal.ast.NoOptions
import org.neo4j.cypher.internal.ast.NoWait
import org.neo4j.cypher.internal.ast.OptionsMap
import org.neo4j.cypher.internal.ast.ReadOnlyAccess
import org.neo4j.cypher.internal.ast.ReadWriteAccess
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.TimeoutAfter
import org.neo4j.cypher.internal.ast.Topology
import org.neo4j.cypher.internal.ast.test.util.AstParsing.Cypher5JavaCc
import org.neo4j.cypher.internal.expressions.Null
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.StringLiteral

class AlterDatabaseAdministrationCommandParserTest extends AdministrationAndSchemaCommandParserTestBase {

  Seq(
    ("READ ONLY", ReadOnlyAccess),
    ("READ WRITE", ReadWriteAccess)
  ).foreach {
    case (accessKeyword, accessType) =>
      test(s"ALTER DATABASE foo SET ACCESS $accessKeyword") {
        assertAst(
          AlterDatabase(literalFoo, ifExists = false, Some(accessType), None, NoOptions, Set.empty, NoWait)(defaultPos)
        )
      }

      test(s"ALTER DATABASE $$foo SET ACCESS $accessKeyword") {
        assertAst(AlterDatabase(
          stringParamName("foo"),
          ifExists = false,
          Some(accessType),
          None,
          NoOptions,
          Set.empty,
          NoWait
        )(defaultPos))
      }

      test(s"ALTER DATABASE `foo.bar` SET ACCESS $accessKeyword") {
        assertAst(
          AlterDatabase(
            literal("foo.bar"),
            ifExists = false,
            Some(accessType),
            None,
            NoOptions,
            Set.empty,
            NoWait
          )(defaultPos)
        )
      }

      test(s"USE system ALTER DATABASE foo SET ACCESS $accessKeyword") {
        // can parse USE clause, but is not included in AST
        assertAst(AlterDatabase(
          literalFoo,
          ifExists = false,
          Some(accessType),
          None,
          NoOptions,
          Set.empty,
          NoWait
        )((1, 12, 11)).withGraph(Some(use(List("system")))))
      }

      test(s"ALTER DATABASE foo IF EXISTS SET ACCESS $accessKeyword") {
        assertAst(
          AlterDatabase(literalFoo, ifExists = true, Some(accessType), None, NoOptions, Set.empty, NoWait)(defaultPos)
        )
      }
  }

  test("ALTER DATABASE") {
    failsParsing[Statements].in {
      case Cypher5JavaCc =>
        _.withMessageStart("Invalid input '': expected a parameter or an identifier (line 1, column 15 (offset: 14))")
      case _ => _.withSyntaxError(
          """Invalid input '': expected a database name or a parameter (line 1, column 15 (offset: 14))
            |"ALTER DATABASE"
            |               ^""".stripMargin
        )
    }
  }

  test("ALTER DATABASE foo") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(
          "Invalid input '': expected \".\", \"IF\", \"REMOVE\" or \"SET\" (line 1, column 19 (offset: 18))"
        )
      case _ => _.withSyntaxError(
          """Invalid input '': expected a database name, 'IF EXISTS', 'REMOVE OPTION' or 'SET' (line 1, column 19 (offset: 18))
            |"ALTER DATABASE foo"
            |                   ^""".stripMargin
        )
    }
  }

  test("ALTER DATABASE foo SET READ ONLY") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(
          "Invalid input 'READ': expected \"ACCESS\", \"OPTION\" or \"TOPOLOGY\" (line 1, column 24 (offset: 23))"
        )
      case _ => _.withSyntaxError(
          """Invalid input 'READ': expected 'OPTION', 'ACCESS READ' or 'TOPOLOGY' (line 1, column 24 (offset: 23))
            |"ALTER DATABASE foo SET READ ONLY"
            |                        ^""".stripMargin
        )
    }
  }

  test("ALTER DATABASE foo ACCESS READ WRITE") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(
          "Invalid input 'ACCESS': expected \".\", \"IF\", \"REMOVE\" or \"SET\" (line 1, column 20 (offset: 19))"
        )
      case _ => _.withSyntaxError(
          """Invalid input 'ACCESS': expected a database name, 'IF EXISTS', 'REMOVE OPTION' or 'SET' (line 1, column 20 (offset: 19))
            |"ALTER DATABASE foo ACCESS READ WRITE"
            |                    ^""".stripMargin
        )
    }
  }

  test("ALTER DATABASE foo SET ACCESS READ") {
    failsParsing[Statements].in {
      case Cypher5JavaCc =>
        _.withMessageStart("Invalid input '': expected \"ONLY\" or \"WRITE\" (line 1, column 35 (offset: 34))")
      case _ => _.withSyntaxError(
          """Invalid input '': expected 'ONLY' or 'WRITE' (line 1, column 35 (offset: 34))
            |"ALTER DATABASE foo SET ACCESS READ"
            |                                   ^""".stripMargin
        )
    }
  }

  test("ALTER DATABASE foo SET ACCESS READWRITE'") {
    failsParsing[Statements].in {
      case Cypher5JavaCc =>
        _.withMessageStart("Invalid input 'READWRITE': expected \"READ\" (line 1, column 31 (offset: 30))")
      case _ => _.withSyntaxError(
          """Invalid input 'READWRITE': expected 'READ' (line 1, column 31 (offset: 30))
            |"ALTER DATABASE foo SET ACCESS READWRITE'"
            |                               ^""".stripMargin
        )
    }
  }

  test("ALTER DATABASE foo SET ACCESS READ_ONLY") {
    failsParsing[Statements].in {
      case Cypher5JavaCc =>
        _.withMessageStart("Invalid input 'READ_ONLY': expected \"READ\" (line 1, column 31 (offset: 30))")
      case _ => _.withSyntaxError(
          """Invalid input 'READ_ONLY': expected 'READ' (line 1, column 31 (offset: 30))
            |"ALTER DATABASE foo SET ACCESS READ_ONLY"
            |                               ^""".stripMargin
        )
    }
  }

  test("ALTER DATABASE foo SET ACCESS WRITE") {
    failsParsing[Statements].in {
      case Cypher5JavaCc =>
        _.withMessageStart("Invalid input 'WRITE': expected \"READ\" (line 1, column 31 (offset: 30))")
      case _ => _.withSyntaxError(
          """Invalid input 'WRITE': expected 'READ' (line 1, column 31 (offset: 30))
            |"ALTER DATABASE foo SET ACCESS WRITE"
            |                               ^""".stripMargin
        )
    }
  }

  test("ALTER DATABASE `foo`.`bar`.`baz` SET ACCESS READ WRITE") {
    failsParsing[Statements].withMessageStart(
      "Invalid input ``foo`.`bar`.`baz`` for name. Expected name to contain at most two components separated by `.`."
    )
  }

  // Set ACCESS multiple times in the same command
  test("ALTER DATABASE foo SET ACCESS READ ONLY SET ACCESS READ WRITE") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Duplicate SET ACCESS clause (line 1, column 41 (offset: 40))")
      case _ => _.withSyntaxErrorContaining(
          """Duplicate ACCESS clause (line 1, column 45 (offset: 44))"""
        )
    }
  }

  // Wrong order between IF EXISTS and SET
  test("ALTER DATABASE foo SET ACCESS READ ONLY IF EXISTS") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(
          "Invalid input 'IF': expected \"NOWAIT\", \"SET\", \"WAIT\" or <EOF> (line 1, column 41 (offset: 40))"
        )
      case _ => _.withSyntaxError(
          """Invalid input 'IF': expected 'NOWAIT', 'SET', 'WAIT' or <EOF> (line 1, column 41 (offset: 40))
            |"ALTER DATABASE foo SET ACCESS READ ONLY IF EXISTS"
            |                                         ^""".stripMargin
        )
    }
  }

  // IF NOT EXISTS instead of IF EXISTS
  test("ALTER DATABASE foo IF NOT EXISTS SET ACCESS READ ONLY") {
    failsParsing[Statements].in {
      case Cypher5JavaCc =>
        _.withMessageStart("Invalid input 'NOT': expected \"EXISTS\" (line 1, column 23 (offset: 22))")
      case _ => _.withSyntaxError(
          """Invalid input 'NOT': expected 'EXISTS' (line 1, column 23 (offset: 22))
            |"ALTER DATABASE foo IF NOT EXISTS SET ACCESS READ ONLY"
            |                       ^""".stripMargin
        )
    }
  }

  // ALTER with OPTIONS

  test("ALTER DATABASE foo SET OPTION txLogEnrichment 'FULL'") {
    assertAst(
      AlterDatabase(
        literalFoo,
        ifExists = false,
        None,
        None,
        OptionsMap(Map("txLogEnrichment" -> StringLiteral("FULL")(pos.withInputLength(0)))),
        Set.empty,
        NoWait
      )(pos)
    )
  }

  test("ALTER DATABASE foo SET OPTION key 1") {
    assertAst(
      AlterDatabase(
        literalFoo,
        ifExists = false,
        None,
        None,
        OptionsMap(Map("key" -> SignedDecimalIntegerLiteral("1")(pos))),
        Set.empty,
        NoWait
      )(pos)
    )
  }

  test("ALTER DATABASE foo SET OPTION key -1") {
    assertAst(
      AlterDatabase(
        literalFoo,
        ifExists = false,
        None,
        None,
        OptionsMap(Map("key" -> SignedDecimalIntegerLiteral("-1")(pos))),
        Set.empty,
        NoWait
      )(pos)
    )
  }

  test("ALTER DATABASE foo SET OPTION key null") {
    assertAst(
      AlterDatabase(
        literalFoo,
        ifExists = false,
        None,
        None,
        OptionsMap(Map("key" -> Null()(pos))),
        Set.empty,
        NoWait
      )(pos)
    )
  }

  test("ALTER DATABASE foo SET OPTION key1 1 SET OPTION key2 'two'") {
    parsesTo[Statements](
      AlterDatabase(
        literalFoo,
        ifExists = false,
        None,
        None,
        OptionsMap(Map(
          "key1" -> SignedDecimalIntegerLiteral("1")(pos),
          "key2" -> StringLiteral("two")(pos.withInputLength(0))
        )),
        Set.empty,
        NoWait
      )(pos)
    )
  }

  test("ALTER DATABASE foo SET ACCESS READ ONLY SET TOPOLOGY 1 PRIMARY SET OPTION txLogEnrichment 'FULL'") {
    assertAst(
      AlterDatabase(
        literalFoo,
        ifExists = false,
        Some(ReadOnlyAccess),
        Some(Topology(Some(1), None)),
        OptionsMap(Map("txLogEnrichment" -> StringLiteral("FULL")(pos.withInputLength(0)))),
        Set.empty,
        NoWait
      )(pos)
    )
  }

  test("ALTER DATABASE foo REMOVE OPTION key REMOVE OPTION key2") {
    parsesTo[Statements](
      AlterDatabase(
        literalFoo,
        ifExists = false,
        None,
        None,
        NoOptions,
        Set("key", "key2"),
        NoWait
      )(pos)
    )
  }

  test("ALTER DATABASE foo SET TOPOLOGY 1 PRIMARY") {
    assertAst(
      AlterDatabase(
        literalFoo,
        ifExists = false,
        None,
        Some(Topology(Some(1), None)),
        NoOptions,
        Set.empty,
        NoWait
      )(pos)
    )
  }

  test("ALTER DATABASE foo SET TOPOLOGY 1 PRIMARY 1 SECONDARY") {
    assertAst(
      AlterDatabase(
        literalFoo,
        ifExists = false,
        None,
        Some(Topology(Some(1), Some(1))),
        NoOptions,
        Set.empty,
        NoWait
      )(pos)
    )
  }

  test("ALTER DATABASE foo SET TOPOLOGY 1 SECONDARY 1 PRIMARY") {
    assertAst(
      AlterDatabase(
        literalFoo,
        ifExists = false,
        None,
        Some(Topology(Some(1), Some(1))),
        NoOptions,
        Set.empty,
        NoWait
      )(pos)
    )
  }

  test("ALTER DATABASE foo SET ACCESS READ WRITE SET TOPOLOGY 1 PRIMARY 1 SECONDARY") {
    assertAst(
      AlterDatabase(
        literalFoo,
        ifExists = false,
        Some(ReadWriteAccess),
        Some(Topology(Some(1), Some(1))),
        NoOptions,
        Set.empty,
        NoWait
      )(pos)
    )
  }

  test("ALTER DATABASE foo SET TOPOLOGY 1 PRIMARY 1 SECONDARY SET ACCESS READ WRITE") {
    assertAst(
      AlterDatabase(
        literalFoo,
        ifExists = false,
        Some(ReadWriteAccess),
        Some(Topology(Some(1), Some(1))),
        NoOptions,
        Set.empty,
        NoWait
      )(pos)
    )
  }

  test("ALTER DATABASE foo SET TOPOLOGY 1 PRIMARY 1 SECONDARY SET ACCESS READ WRITE WAIT") {
    assertAst(
      AlterDatabase(
        literalFoo,
        ifExists = false,
        Some(ReadWriteAccess),
        Some(Topology(Some(1), Some(1))),
        NoOptions,
        Set.empty,
        IndefiniteWait
      )(pos)
    )
  }

  test("ALTER DATABASE foo SET TOPOLOGY 1 PRIMARY 1 SECONDARY SET ACCESS READ WRITE WAIT 5") {
    assertAst(
      AlterDatabase(
        literalFoo,
        ifExists = false,
        Some(ReadWriteAccess),
        Some(Topology(Some(1), Some(1))),
        NoOptions,
        Set.empty,
        TimeoutAfter(5)
      )(pos)
    )
  }

  test("ALTER DATABASE foo SET TOPOLOGY 1 PRIMARY 1 SECONDARY SET ACCESS READ WRITE WAIT 5 SEC") {
    assertAst(
      AlterDatabase(
        literalFoo,
        ifExists = false,
        Some(ReadWriteAccess),
        Some(Topology(Some(1), Some(1))),
        NoOptions,
        Set.empty,
        TimeoutAfter(5)
      )(pos)
    )
  }

  test("ALTER DATABASE foo SET TOPOLOGY 1 PRIMARY 1 SECONDARY SET ACCESS READ WRITE WAIT 5 SECOND") {
    assertAst(
      AlterDatabase(
        literalFoo,
        ifExists = false,
        Some(ReadWriteAccess),
        Some(Topology(Some(1), Some(1))),
        NoOptions,
        Set.empty,
        TimeoutAfter(5)
      )(pos)
    )
  }

  test("ALTER DATABASE foo SET TOPOLOGY 1 PRIMARY 1 SECONDARY SET ACCESS READ WRITE WAIT 5 SECONDS") {
    assertAst(
      AlterDatabase(
        literalFoo,
        ifExists = false,
        Some(ReadWriteAccess),
        Some(Topology(Some(1), Some(1))),
        NoOptions,
        Set.empty,
        TimeoutAfter(5)
      )(pos)
    )
  }

  test("ALTER DATABASE foo SET TOPOLOGY 1 PRIMARY 1 SECONDARY SET ACCESS READ WRITE NOWAIT") {
    assertAst(
      AlterDatabase(
        literalFoo,
        ifExists = false,
        Some(ReadWriteAccess),
        Some(Topology(Some(1), Some(1))),
        NoOptions,
        Set.empty,
        NoWait
      )(pos)
    )
  }

  test("ALTER DATABASE foo SET TOPOLOGY 1 SECONDARY") {
    assertAst(
      AlterDatabase(
        literalFoo,
        ifExists = false,
        None,
        Some(Topology(None, Some(1))),
        NoOptions,
        Set.empty,
        NoWait
      )(pos)
    )
  }

  test("ALTER DATABASE foo SET ACCESS READ WRITE OPTIONS {existingData: 'use'}") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(
          "Invalid input 'OPTIONS': expected \"NOWAIT\", \"SET\", \"WAIT\" or <EOF> (line 1, column 42 (offset: 41))"
        )
      case _ => _.withSyntaxError(
          """Invalid input 'OPTIONS': expected 'NOWAIT', 'SET', 'WAIT' or <EOF> (line 1, column 42 (offset: 41))
            |"ALTER DATABASE foo SET ACCESS READ WRITE OPTIONS {existingData: 'use'}"
            |                                          ^""".stripMargin
        )
    }
  }

  test("ALTER DATABASE foo REMOVE OPTION key REMOVE OPTION key") {
    failsParsing[Statements].in {
      case Cypher5JavaCc =>
        _.withMessageStart("""Duplicate 'REMOVE OPTION key' clause (line 1, column 38 (offset: 37))""")
      case _ => _.withSyntaxErrorContaining(
          """Duplicate 'REMOVE OPTION key' clause (line 1, column 52 (offset: 51))"""
        )
    }
  }

  test("ALTER DATABASE foo SET ACCESS READ ONLY REMOVE OPTION key") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(
          """Invalid input 'REMOVE': expected "NOWAIT", "SET", "WAIT" or <EOF> (line 1, column 41 (offset: 40))"""
        )
      case _ => _.withSyntaxError(
          """Invalid input 'REMOVE': expected 'NOWAIT', 'SET', 'WAIT' or <EOF> (line 1, column 41 (offset: 40))
            |"ALTER DATABASE foo SET ACCESS READ ONLY REMOVE OPTION key"
            |                                         ^""".stripMargin
        )
    }
  }

  test("ALTER DATABASE foo SET OPTIONS {key: value}") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(
          """Invalid input 'OPTIONS': expected "ACCESS", "OPTION" or "TOPOLOGY" (line 1, column 24 (offset: 23))"""
        )
      case _ => _.withSyntaxError(
          """Invalid input 'OPTIONS': expected 'OPTION', 'ACCESS READ' or 'TOPOLOGY' (line 1, column 24 (offset: 23))
            |"ALTER DATABASE foo SET OPTIONS {key: value}"
            |                        ^""".stripMargin
        )
    }
  }

  test("ALTER DATABASE foo SET OPTION {key: value}") {
    failsParsing[Statements].in {
      case Cypher5JavaCc =>
        _.withMessageStart("""Invalid input '{': expected an identifier (line 1, column 31 (offset: 30))""")
      case _ => _.withSyntaxError(
          """Invalid input '{': expected an identifier (line 1, column 31 (offset: 30))
            |"ALTER DATABASE foo SET OPTION {key: value}"
            |                               ^""".stripMargin
        )
    }
  }

  test("ALTER DATABASE foo SET OPTIONS key value") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(
          """Invalid input 'OPTIONS': expected "ACCESS", "OPTION" or "TOPOLOGY" (line 1, column 24 (offset: 23))"""
        )
      case _ => _.withSyntaxError(
          """Invalid input 'OPTIONS': expected 'OPTION', 'ACCESS READ' or 'TOPOLOGY' (line 1, column 24 (offset: 23))
            |"ALTER DATABASE foo SET OPTIONS key value"
            |                        ^""".stripMargin
        )
    }
  }

  test("ALTER DATABASE foo SET OPTION key value key2 value") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(
          """Invalid input 'key2': expected
            |  "!="
            |  "%"
            |  "*"
            |  "+"
            |  "-"
            |  "/"
            |  "::"
            |  "<"
            |  "<="
            |  "<>"
            |  "="
            |  "=~"
            |  ">"
            |  ">="
            |  "AND"
            |  "CONTAINS"
            |  "ENDS"
            |  "IN"
            |  "IS"
            |  "NOWAIT"
            |  "OR"
            |  "SET"
            |  "STARTS"
            |  "WAIT"
            |  "XOR"
            |  "^"
            |  "||"
            |  <EOF> (line 1, column 41 (offset: 40))""".stripMargin
        )
      case _ => _.withSyntaxError(
          """Invalid input 'key2': expected an expression, 'NOWAIT', 'SET', 'WAIT' or <EOF> (line 1, column 41 (offset: 40))
            |"ALTER DATABASE foo SET OPTION key value key2 value"
            |                                         ^""".stripMargin
        )
    }
  }

  test("ALTER DATABASE foo SET OPTION key value, key2 value") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(
          """Invalid input ',': expected
            |  "!="
            |  "%"
            |  "*"
            |  "+"
            |  "-"
            |  "/"
            |  "::"
            |  "<"
            |  "<="
            |  "<>"
            |  "="
            |  "=~"
            |  ">"
            |  ">="
            |  "AND"
            |  "CONTAINS"
            |  "ENDS"
            |  "IN"
            |  "IS"
            |  "NOWAIT"
            |  "OR"
            |  "SET"
            |  "STARTS"
            |  "WAIT"
            |  "XOR"
            |  "^"
            |  "||"
            |  <EOF> (line 1, column 40 (offset: 39))""".stripMargin
        )
      case _ => _.withSyntaxError(
          """Invalid input ',': expected an expression, 'NOWAIT', 'SET', 'WAIT' or <EOF> (line 1, column 40 (offset: 39))
            |"ALTER DATABASE foo SET OPTION key value, key2 value"
            |                                        ^""".stripMargin
        )
    }
  }

  test("ALTER DATABASE foo REMOVE OPTION key key2") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(
          """Invalid input 'key2': expected "NOWAIT", "REMOVE", "WAIT" or <EOF> (line 1, column 38 (offset: 37))"""
        )
      case _ => _.withSyntaxError(
          """Invalid input 'key2': expected 'NOWAIT', 'REMOVE OPTION', 'WAIT' or <EOF> (line 1, column 38 (offset: 37))
            |"ALTER DATABASE foo REMOVE OPTION key key2"
            |                                      ^""".stripMargin
        )
    }
  }

  test("ALTER DATABASE foo REMOVE OPTION key, key2") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(
          """Invalid input ',': expected "NOWAIT", "REMOVE", "WAIT" or <EOF> (line 1, column 37 (offset: 36))"""
        )
      case _ => _.withSyntaxError(
          """Invalid input ',': expected 'NOWAIT', 'REMOVE OPTION', 'WAIT' or <EOF> (line 1, column 37 (offset: 36))
            |"ALTER DATABASE foo REMOVE OPTION key, key2"
            |                                     ^""".stripMargin
        )
    }
  }

  test("ALTER DATABASE foo REMOVE OPTIONS key") {
    failsParsing[Statements].in {
      case Cypher5JavaCc =>
        _.withMessageStart("""Invalid input 'OPTIONS': expected "OPTION" (line 1, column 27 (offset: 26))""")
      case _ => _.withSyntaxError(
          """Invalid input 'OPTIONS': expected 'OPTION' (line 1, column 27 (offset: 26))
            |"ALTER DATABASE foo REMOVE OPTIONS key"
            |                           ^""".stripMargin
        )
    }
  }

  test("ALTER DATABASE foo SET OPTION txLogEnrichment 'FULL' SET OPTION txLogEnrichment 'FULL'") {
    failsParsing[Statements].in {
      case Cypher5JavaCc =>
        _.withMessageStart("Duplicate 'SET OPTION txLogEnrichment' clause (line 1, column 54 (offset: 53))")
      case _ => _.withSyntaxErrorContaining(
          """Duplicate 'SET OPTION txLogEnrichment' clause (line 1, column 58 (offset: 57))"""
        )
    }
  }

  test("ALTER DATABASE foo SET OPTION txLogEnrichment 'FULL' REMOVE OPTION txLogEnrichment") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(
          """Invalid input 'REMOVE': expected
            |  "!="
            |  "%"
            |  "*"
            |  "+"
            |  "-"
            |  "/"
            |  "::"
            |  "<"
            |  "<="
            |  "<>"
            |  "="
            |  "=~"
            |  ">"
            |  ">="
            |  "AND"
            |  "CONTAINS"
            |  "ENDS"
            |  "IN"
            |  "IS"
            |  "NOWAIT"
            |  "OR"
            |  "SET"
            |  "STARTS"
            |  "WAIT"
            |  "XOR"
            |  "^"
            |  "||"
            |  <EOF> (line 1, column 54 (offset: 53))""".stripMargin
        )
      case _ => _.withSyntaxError(
          """Invalid input 'REMOVE': expected an expression, 'NOWAIT', 'SET', 'WAIT' or <EOF> (line 1, column 54 (offset: 53))
            |"ALTER DATABASE foo SET OPTION txLogEnrichment 'FULL' REMOVE OPTION txLogEnrichment"
            |                                                      ^""".stripMargin
        )
    }
  }

  test("ALTER DATABASE foo REMOVE OPTION txLogEnrichment SET OPTION txLogEnrichment 'FULL'") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(
          "Invalid input 'SET': expected \"NOWAIT\", \"REMOVE\", \"WAIT\" or <EOF> (line 1, column 50 (offset: 49))"
        )
      case _ => _.withSyntaxError(
          """Invalid input 'SET': expected 'NOWAIT', 'REMOVE OPTION', 'WAIT' or <EOF> (line 1, column 50 (offset: 49))
            |"ALTER DATABASE foo REMOVE OPTION txLogEnrichment SET OPTION txLogEnrichment 'FULL'"
            |                                                  ^""".stripMargin
        )
    }
  }

  // ALTER OR REPLACE
  test("ALTER OR REPLACE DATABASE foo SET ACCESS READ WRITE") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(
          """Invalid input 'OR': expected
            |  "ALIAS"
            |  "CURRENT"
            |  "DATABASE"
            |  "SERVER"
            |  "USER" (line 1, column 7 (offset: 6))""".stripMargin
        )
      case _ => _.withSyntaxError(
          """Invalid input 'OR': expected 'ALIAS', 'DATABASE', 'CURRENT USER SET PASSWORD FROM', 'SERVER' or 'USER' (line 1, column 7 (offset: 6))
            |"ALTER OR REPLACE DATABASE foo SET ACCESS READ WRITE"
            |       ^""".stripMargin
        )
    }
  }

  test("ALTER DATABASE foo SET TOPOLOGY $param PRIMARY") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(
          """Invalid input '$': expected <UNSIGNED_DECIMAL_INTEGER> (line 1, column 33 (offset: 32))"""
        )
      case _ => _.withSyntaxError(
          """Invalid input '$': expected an integer value (line 1, column 33 (offset: 32))
            |"ALTER DATABASE foo SET TOPOLOGY $param PRIMARY"
            |                                 ^""".stripMargin
        )
    }
  }

  test("ALTER DATABASE foo SET TOPOLOGY 1 PRIMARY $param SECONDARY") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(
          """Invalid input '$': expected
            |  "NOWAIT"
            |  "SET"
            |  "WAIT"
            |  <EOF>
            |  <UNSIGNED_DECIMAL_INTEGER> (line 1, column 43 (offset: 42))""".stripMargin
        )
      case _ => _.withSyntaxError(
          """Invalid input '$': expected 'NOWAIT', 'SET', 'WAIT', <EOF> or an integer value (line 1, column 43 (offset: 42))
            |"ALTER DATABASE foo SET TOPOLOGY 1 PRIMARY $param SECONDARY"
            |                                           ^""".stripMargin
        )
    }
  }

  test("ALTER DATABASE foo SET TOPOLOGY 2 PRIMARIES 1 PRIMARY") {
    failsParsing[Statements].in {
      case Cypher5JavaCc =>
        _.withMessageStart("""Duplicate PRIMARY clause (line 1, column 47 (offset: 46))""".stripMargin)
      case _ => _.withSyntaxErrorContaining(
          """Duplicate PRIMARY clause (line 1, column 45 (offset: 44))""".stripMargin
        )
    }
  }

  test("ALTER DATABASE foo SET TOPOLOGY 2 SECONDARIES 1 SECONDARY") {
    failsParsing[Statements].in {
      case Cypher5JavaCc =>
        _.withMessageStart("""Duplicate SECONDARY clause (line 1, column 49 (offset: 48))""".stripMargin)
      case _ => _.withSyntaxErrorContaining(
          """Duplicate SECONDARY clause (line 1, column 47 (offset: 46))""".stripMargin
        )
    }
  }

  test("ALTER DATABASE foo SET TOPOLOGY 5 PRIMARIES 10 PRIMARIES 1 PRIMARY 2 SECONDARIES") {
    failsParsing[Statements].in {
      case Cypher5JavaCc =>
        _.withMessageStart("""Duplicate PRIMARY clause (line 1, column 48 (offset: 47))""")
      case _ => _.withSyntaxErrorContaining(
          """Duplicate PRIMARY clause (line 1, column 45 (offset: 44))"""
        )
    }
  }

  test("ALTER DATABASE foo SET TOPOLOGY 1 PRIMARY 2 SECONDARIES 1 SECONDARIES") {
    failsParsing[Statements].in {
      case Cypher5JavaCc =>
        _.withMessageStart("""Duplicate SECONDARY clause (line 1, column 59 (offset: 58))""")
      case _ => _.withSyntaxErrorContaining(
          """Duplicate SECONDARY clause (line 1, column 57 (offset: 56))"""
        )
    }
  }

  test("ALTER DATABASE foo SET TOPOLOGY 1 PRIMARY SET TOPOLOGY 1 SECONDARY") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Duplicate SET TOPOLOGY clause (line 1, column 43 (offset: 42))")
      case _ => _.withSyntaxErrorContaining(
          """Duplicate TOPOLOGY clause (line 1, column 47 (offset: 46))"""
        )
    }
  }

  test("ALTER DATABASE foo SET TOPOLOGY 1 PRIMARY 1 PRIMARY") {
    failsParsing[Statements].in {
      case Cypher5JavaCc =>
        _.withMessageStart("""Duplicate PRIMARY clause (line 1, column 45 (offset: 44))""".stripMargin)
      case _ => _.withSyntaxErrorContaining(
          """Duplicate PRIMARY clause (line 1, column 43 (offset: 42))"""
        )
    }
  }

  test("ALTER DATABASE foo SET TOPOLOGY 1 PRIMARY 1 SECONDARY 2 SECONDARY") {
    failsParsing[Statements].in {
      case Cypher5JavaCc =>
        _.withMessageStart("""Duplicate SECONDARY clause (line 1, column 57 (offset: 56))""".stripMargin)
      case _ => _.withSyntaxErrorContaining(
          """Duplicate SECONDARY clause (line 1, column 55 (offset: 54))"""
        )
    }
  }

  test("ALTER DATABASE foo SET TOPOLOGY -1 PRIMARY") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(
          """Invalid input '-': expected <UNSIGNED_DECIMAL_INTEGER> (line 1, column 33 (offset: 32))""".stripMargin
        )
      case _ => _.withSyntaxError(
          """Invalid input '-': expected an integer value (line 1, column 33 (offset: 32))
            |"ALTER DATABASE foo SET TOPOLOGY -1 PRIMARY"
            |                                 ^""".stripMargin
        )
    }
  }

  test("ALTER DATABASE foo SET TOPOLOGY 1 PRIMARY -1 SECONDARY") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(
          """Invalid input '-': expected
            |  "NOWAIT"
            |  "SET"
            |  "WAIT"
            |  <EOF>
            |  <UNSIGNED_DECIMAL_INTEGER> (line 1, column 43 (offset: 42))""".stripMargin
        )
      case _ => _.withSyntaxError(
          """Invalid input '-': expected 'NOWAIT', 'SET', 'WAIT', <EOF> or an integer value (line 1, column 43 (offset: 42))
            |"ALTER DATABASE foo SET TOPOLOGY 1 PRIMARY -1 SECONDARY"
            |                                           ^""".stripMargin
        )
    }
  }

  test("ALTER DATABASE foo SET TOPOLOGY -1 SECONDARY 1 PRIMARY") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(
          """Invalid input '-': expected <UNSIGNED_DECIMAL_INTEGER> (line 1, column 33 (offset: 32))""".stripMargin
        )
      case _ => _.withSyntaxError(
          """Invalid input '-': expected an integer value (line 1, column 33 (offset: 32))
            |"ALTER DATABASE foo SET TOPOLOGY -1 SECONDARY 1 PRIMARY"
            |                                 ^""".stripMargin
        )
    }
  }

  test("ALTER DATABASE foo SET TOPOLOGY 1 SECONDARY 1 SECONDARY") {
    failsParsing[Statements].in {
      case Cypher5JavaCc =>
        _.withMessageStart("""Duplicate SECONDARY clause (line 1, column 47 (offset: 46))""".stripMargin)
      case _ => _.withSyntaxErrorContaining(
          """Duplicate SECONDARY clause (line 1, column 45 (offset: 44))""".stripMargin
        )
    }
  }

  test("ALTER DATABASE foo SET TOPOLOGY") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(
          """Invalid input '': expected <UNSIGNED_DECIMAL_INTEGER> (line 1, column 32 (offset: 31))"""
        )
      case _ => _.withSyntaxError(
          """Invalid input '': expected an integer value (line 1, column 32 (offset: 31))
            |"ALTER DATABASE foo SET TOPOLOGY"
            |                                ^""".stripMargin
        )
    }
  }
}
