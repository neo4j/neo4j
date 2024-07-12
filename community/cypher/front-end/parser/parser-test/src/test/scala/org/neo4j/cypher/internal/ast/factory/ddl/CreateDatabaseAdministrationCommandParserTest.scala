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

import org.neo4j.cypher.internal.ast.CreateDatabase
import org.neo4j.cypher.internal.ast.IfExistsDoNothing
import org.neo4j.cypher.internal.ast.IfExistsInvalidSyntax
import org.neo4j.cypher.internal.ast.IfExistsReplace
import org.neo4j.cypher.internal.ast.IfExistsThrowError
import org.neo4j.cypher.internal.ast.IndefiniteWait
import org.neo4j.cypher.internal.ast.NamespacedName
import org.neo4j.cypher.internal.ast.NoOptions
import org.neo4j.cypher.internal.ast.NoWait
import org.neo4j.cypher.internal.ast.OptionsMap
import org.neo4j.cypher.internal.ast.OptionsParam
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.TimeoutAfter
import org.neo4j.cypher.internal.ast.Topology
import org.neo4j.cypher.internal.ast.test.util.AstParsing.Cypher5JavaCc
import org.neo4j.cypher.internal.expressions.ExplicitParameter
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.util.symbols.CTMap

class CreateDatabaseAdministrationCommandParserTest extends AdministrationAndSchemaCommandParserTestBase {

  test("CREATE DATABASE foo") {
    parsesTo[Statements](CreateDatabase(literalFoo, IfExistsThrowError, NoOptions, NoWait, None)(pos))
  }

  test("USE system CREATE DATABASE foo") {
    // can parse USE clause, but is not included in AST
    parsesTo[Statements] {
      CreateDatabase(literalFoo, IfExistsThrowError, NoOptions, NoWait, None)(pos)
        .withGraph(Some(use(List("system"))))
    }
  }

  test("CREATE DATABASE $foo") {
    parsesTo[Statements](CreateDatabase(
      stringParamName("foo"),
      IfExistsThrowError,
      NoOptions,
      NoWait,
      None
    )(pos))
  }

  test("CREATE DATABASE $wait") {
    parsesTo[Statements](CreateDatabase(
      stringParamName("wait"),
      IfExistsThrowError,
      NoOptions,
      NoWait,
      None
    )(pos))
  }

  test("CREATE DATABASE `nowait.sec`") {
    parsesTo[Statements](CreateDatabase(
      literal("nowait.sec"),
      IfExistsThrowError,
      NoOptions,
      NoWait,
      None
    )(pos))
  }

  test("CREATE DATABASE second WAIT") {
    parsesTo[Statements](CreateDatabase(
      literal("second"),
      IfExistsThrowError,
      NoOptions,
      IndefiniteWait,
      None
    )(pos))
  }

  test("CREATE DATABASE seconds WAIT 12") {
    parsesTo[Statements](CreateDatabase(
      literal("seconds"),
      IfExistsThrowError,
      NoOptions,
      TimeoutAfter(12),
      None
    )(pos))
  }

  test("CREATE DATABASE dump WAIT 12 SEC") {
    parsesTo[Statements](CreateDatabase(
      literal("dump"),
      IfExistsThrowError,
      NoOptions,
      TimeoutAfter(12),
      None
    )(pos))
  }

  test("CREATE DATABASE destroy WAIT 12 SECOND") {
    parsesTo[Statements](CreateDatabase(
      literal("destroy"),
      IfExistsThrowError,
      NoOptions,
      TimeoutAfter(12),
      None
    )(pos))
  }

  test("CREATE DATABASE data WAIT 12 SECONDS") {
    parsesTo[Statements](CreateDatabase(
      literal("data"),
      IfExistsThrowError,
      NoOptions,
      TimeoutAfter(12),
      None
    )(pos))
  }

  test("CREATE DATABASE foo NOWAIT") {
    parsesTo[Statements](
      CreateDatabase(literal("foo"), IfExistsThrowError, NoOptions, NoWait, None)(pos)
    )
  }

  test("CREATE DATABASE `foo.bar`") {
    parsesTo[Statements](CreateDatabase(
      literal("foo.bar"),
      IfExistsThrowError,
      NoOptions,
      NoWait,
      None
    )(pos))
  }

  test("CREATE DATABASE foo.bar") {
    parsesTo[Statements](CreateDatabase(
      namespacedName("foo", "bar"),
      IfExistsThrowError,
      NoOptions,
      NoWait,
      None
    )(pos))
  }

  test("CREATE DATABASE `foo-bar42`") {
    parsesTo[Statements](
      CreateDatabase(literal("foo-bar42"), IfExistsThrowError, NoOptions, NoWait, None)(pos)
    )
  }

  test("CREATE DATABASE `_foo-bar42`") {
    parsesTo[Statements](
      CreateDatabase(literal("_foo-bar42"), IfExistsThrowError, NoOptions, NoWait, None)(pos)
    )
  }

  test("CREATE DATABASE ``") {
    parsesTo[Statements](
      CreateDatabase(literal(""), IfExistsThrowError, NoOptions, NoWait, None)(pos)
    )
  }

  test("CREATE DATABASE foo IF NOT EXISTS") {
    parsesTo[Statements](CreateDatabase(literalFoo, IfExistsDoNothing, NoOptions, NoWait, None)(pos))
  }

  test("CREATE DATABASE foo IF NOT EXISTS WAIT 10 SECONDS") {
    parsesTo[Statements](CreateDatabase(
      literalFoo,
      IfExistsDoNothing,
      NoOptions,
      TimeoutAfter(10),
      None
    )(pos))
  }

  test("CREATE DATABASE foo IF NOT EXISTS WAIT") {
    parsesTo[Statements](
      CreateDatabase(literalFoo, IfExistsDoNothing, NoOptions, IndefiniteWait, None)(pos)
    )
  }

  test("CREATE  DATABASE foo IF NOT EXISTS NOWAIT") {
    parsesTo[Statements](CreateDatabase(literalFoo, IfExistsDoNothing, NoOptions, NoWait, None)(pos))
  }

  test("CREATE DATABASE `_foo-bar42` IF NOT EXISTS") {
    parsesTo[Statements](
      CreateDatabase(literal("_foo-bar42"), IfExistsDoNothing, NoOptions, NoWait, None)(pos)
    )
  }

  test("CREATE OR REPLACE DATABASE foo") {
    parsesTo[Statements](CreateDatabase(literalFoo, IfExistsReplace, NoOptions, NoWait, None)(pos))
  }

  test("CREATE OR REPLACE DATABASE foo WAIT 10 SECONDS") {
    parsesTo[Statements](
      CreateDatabase(literalFoo, IfExistsReplace, NoOptions, TimeoutAfter(10), None)(pos)
    )
  }

  test("CREATE OR REPLACE DATABASE foo WAIT") {
    parsesTo[Statements](
      CreateDatabase(literalFoo, IfExistsReplace, NoOptions, IndefiniteWait, None)(pos)
    )
  }

  test("CREATE OR REPLACE DATABASE foo NOWAIT") {
    parsesTo[Statements](CreateDatabase(literalFoo, IfExistsReplace, NoOptions, NoWait, None)(pos))
  }

  test("CREATE OR REPLACE DATABASE `_foo-bar42`") {
    parsesTo[Statements](
      CreateDatabase(literal("_foo-bar42"), IfExistsReplace, NoOptions, NoWait, None)(pos)
    )
  }

  test("CREATE OR REPLACE DATABASE foo IF NOT EXISTS") {
    parsesTo[Statements](
      CreateDatabase(literalFoo, IfExistsInvalidSyntax, NoOptions, NoWait, None)(pos)
    )
  }

  test(
    "CREATE DATABASE foo OPTIONS {existingData: 'use', existingDataSeedInstance: '84c3ee6f-260e-47db-a4b6-589c807f2c2e'}"
  ) {
    assertAst(
      CreateDatabase(
        NamespacedName("foo")((1, 17, 16)),
        IfExistsThrowError,
        OptionsMap(Map(
          "existingData" -> StringLiteral("use")((1, 44, 43).withInputLength(5)),
          "existingDataSeedInstance" -> StringLiteral("84c3ee6f-260e-47db-a4b6-589c807f2c2e")(
            (1, 77, 76).withInputLength(38)
          )
        )),
        NoWait,
        None
      )(defaultPos)
    )
  }

  test(
    "CREATE DATABASE foo OPTIONS {existingData: 'use', existingDataSeedInstance: '84c3ee6f-260e-47db-a4b6-589c807f2c2e'} WAIT"
  ) {
    assertAst(
      CreateDatabase(
        NamespacedName("foo")((1, 17, 16)),
        IfExistsThrowError,
        OptionsMap(Map(
          "existingData" -> StringLiteral("use")((1, 44, 43).withInputLength(5)),
          "existingDataSeedInstance" -> StringLiteral("84c3ee6f-260e-47db-a4b6-589c807f2c2e")(
            (1, 77, 76).withInputLength(38)
          )
        )),
        IndefiniteWait,
        None
      )(defaultPos)
    )
  }

  test("CREATE DATABASE foo OPTIONS $param") {
    assertAst(
      CreateDatabase(
        NamespacedName("foo")((1, 17, 16)),
        IfExistsThrowError,
        OptionsParam(ExplicitParameter("param", CTMap)((1, 29, 28))),
        NoWait,
        None
      )(defaultPos)
    )
  }

  test("CREATE DATABASE foo TOPOLOGY 1 PRIMARY") {
    assertAst(
      CreateDatabase(
        literalFoo,
        IfExistsThrowError,
        NoOptions,
        NoWait,
        Some(Topology(Some(1), None))
      )(pos)
    )
  }

  test("CREATE DATABASE foo TOPOLOGY 1 PRIMARIES") {
    assertAst(
      CreateDatabase(
        literalFoo,
        IfExistsThrowError,
        NoOptions,
        NoWait,
        Some(Topology(Some(1), None))
      )(pos)
    )
  }

  test("CREATE DATABASE foo TOPOLOGY 1 PRIMARY 1 SECONDARY") {
    assertAst(
      CreateDatabase(
        literalFoo,
        IfExistsThrowError,
        NoOptions,
        NoWait,
        Some(Topology(Some(1), Some(1)))
      )(pos)
    )
  }

  test("CREATE DATABASE foo TOPOLOGY 1 PRIMARY 2 SECONDARIES") {
    assertAst(
      CreateDatabase(
        literalFoo,
        IfExistsThrowError,
        NoOptions,
        NoWait,
        Some(Topology(Some(1), Some(2)))
      )(pos)
    )
  }

  test("CREATE DATABASE foo TOPOLOGY 1 SECONDARY 1 PRIMARY") {
    assertAst(
      CreateDatabase(
        literalFoo,
        IfExistsThrowError,
        NoOptions,
        NoWait,
        Some(Topology(Some(1), Some(1)))
      )(pos)
    )
  }

  test("CREATE DATABASE foo TOPOLOGY 1 SECONDARY") {
    assertAst(
      CreateDatabase(
        literalFoo,
        IfExistsThrowError,
        NoOptions,
        NoWait,
        Some(Topology(None, Some(1)))
      )(pos)
    )
  }

  test("CREATE DATABASE alias") {
    parsesTo[Statements](
      CreateDatabase(literal("alias"), IfExistsThrowError, NoOptions, NoWait, None)(pos)
    )
  }

  test("CREATE DATABASE alias IF NOT EXISTS") {
    parsesTo[Statements](
      CreateDatabase(literal("alias"), IfExistsDoNothing, NoOptions, NoWait, None)(pos)
    )
  }

  test("CREATE DATABASE") {
    // missing db name but parses as 'normal' cypher CREATE...
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(
          s"""Invalid input '': expected a parameter or an identifier (line 1, column 16 (offset: 15))"""
        )
      case _ => _.withSyntaxError(
          """Invalid input '': expected a database name, a graph pattern or a parameter (line 1, column 16 (offset: 15))
            |"CREATE DATABASE"
            |                ^""".stripMargin
        )
    }
  }

  test("CREATE DATABASE `graph.db`.`db.db`") {
    failsParsing[Statements].withMessageStart(
      "Invalid input ``graph.db`.`db.db`` for database name. Expected name to contain at most one component"
    )
  }

  test("CREATE DATABASE \"foo.bar\"") {
    failsParsing[Statements]
  }

  test("CREATE DATABASE foo-bar42") {
    failsParsing[Statements]
  }

  test("CREATE DATABASE _foo-bar42") {
    failsParsing[Statements]
  }

  test("CREATE DATABASE 42foo-bar") {
    failsParsing[Statements]
  }

  test("CREATE DATABASE _foo-bar42 IF NOT EXISTS") {
    failsParsing[Statements]
  }

  test("CREATE DATABASE `foo`.`bar`.`baz`") {
    failsParsing[Statements]
      .withMessageStart(
        "Invalid input ``foo`.`bar`.`baz`` for database name. Expected name to contain at most one component"
      )
  }

  test("CREATE DATABASE  IF NOT EXISTS") {
    val exceptionMessage =
      s"""Invalid input 'NOT': expected
         |  "."
         |  "IF"
         |  "NOWAIT"
         |  "OPTIONS"
         |  "TOPOLOGY"
         |  "WAIT"
         |  <EOF> (line 1, column 21 (offset: 20))""".stripMargin

    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(exceptionMessage)
      case _ => _.withSyntaxError(
          """Invalid input 'NOT': expected a database name, 'IF NOT EXISTS', 'NOWAIT', 'OPTIONS', 'TOPOLOGY', 'WAIT' or <EOF> (line 1, column 21 (offset: 20))
            |"CREATE DATABASE  IF NOT EXISTS"
            |                     ^""".stripMargin
        )
    }
  }

  test("CREATE DATABASE foo IF EXISTS") {
    failsParsing[Statements]
  }

  test("CREATE DATABASE foo WAIT -12") {
    failsParsing[Statements]
  }

  test("CREATE DATABASE foo WAIT 3.14") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(
          "Invalid input '3.14': expected <EOF> or <UNSIGNED_DECIMAL_INTEGER> (line 1, column 26 (offset: 25))"
        )
      case _ => _.withSyntaxError(
          """Invalid input '3.14': expected <EOF> or an integer value (line 1, column 26 (offset: 25))
            |"CREATE DATABASE foo WAIT 3.14"
            |                          ^""".stripMargin
        )
    }
  }

  test("CREATE DATABASE foo WAIT bar") {
    failsParsing[Statements]
  }

  test("CREATE OR REPLACE DATABASE _foo-bar42") {
    failsParsing[Statements]
  }

  test("CREATE OR REPLACE DATABASE") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(
          s"""Invalid input '': expected a parameter or an identifier (line 1, column 27 (offset: 26))"""
        )
      case _ => _.withSyntaxError(
          """Invalid input '': expected a database name or a parameter (line 1, column 27 (offset: 26))
            |"CREATE OR REPLACE DATABASE"
            |                           ^""".stripMargin
        )
    }
  }

  test("CREATE DATABASE foo SET OPTION key value") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(
          """Invalid input 'SET': expected
            |  "."
            |  "IF"
            |  "NOWAIT"
            |  "OPTIONS"
            |  "TOPOLOGY"
            |  "WAIT"
            |  <EOF> (line 1, column 21 (offset: 20))""".stripMargin
        )
      case _ => _.withSyntaxError(
          """Invalid input 'SET': expected a database name, 'IF NOT EXISTS', 'NOWAIT', 'OPTIONS', 'TOPOLOGY', 'WAIT' or <EOF> (line 1, column 21 (offset: 20))
            |"CREATE DATABASE foo SET OPTION key value"
            |                     ^""".stripMargin
        )
    }
  }

  test("CREATE DATABASE foo OPTION {key: value}") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(
          """Invalid input 'OPTION': expected
            |  "."
            |  "IF"
            |  "NOWAIT"
            |  "OPTIONS"
            |  "TOPOLOGY"
            |  "WAIT"
            |  <EOF> (line 1, column 21 (offset: 20))""".stripMargin
        )
      case _ => _.withSyntaxError(
          """Invalid input 'OPTION': expected a database name, 'IF NOT EXISTS', 'NOWAIT', 'OPTIONS', 'TOPOLOGY', 'WAIT' or <EOF> (line 1, column 21 (offset: 20))
            |"CREATE DATABASE foo OPTION {key: value}"
            |                     ^""".stripMargin
        )
    }
  }

  test("CREATE DATABASE foo SET OPTIONS key value") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(
          """Invalid input 'SET': expected
            |  "."
            |  "IF"
            |  "NOWAIT"
            |  "OPTIONS"
            |  "TOPOLOGY"
            |  "WAIT"
            |  <EOF> (line 1, column 21 (offset: 20))""".stripMargin
        )
      case _ => _.withSyntaxError(
          """Invalid input 'SET': expected a database name, 'IF NOT EXISTS', 'NOWAIT', 'OPTIONS', 'TOPOLOGY', 'WAIT' or <EOF> (line 1, column 21 (offset: 20))
            |"CREATE DATABASE foo SET OPTIONS key value"
            |                     ^""".stripMargin
        )
    }
  }

  test("CREATE DATABASE foo OPTIONS key value") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(
          """Invalid input 'key': expected "{" or a parameter (line 1, column 29 (offset: 28))""".stripMargin
        )
      case _ => _.withSyntaxError(
          """Invalid input 'key': expected a parameter or '{' (line 1, column 29 (offset: 28))
            |"CREATE DATABASE foo OPTIONS key value"
            |                             ^""".stripMargin
        )
    }
  }

  test("CREATE DATABASE foo TOPOLOGY 1 PRIMARY TOPOLOGY 1 SECONDARY") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(
          """Invalid input 'TOPOLOGY': expected
            |  "NOWAIT"
            |  "OPTIONS"
            |  "WAIT"
            |  <EOF>
            |  <UNSIGNED_DECIMAL_INTEGER> (line 1, column 40 (offset: 39))""".stripMargin
        )
      case _ => _.withSyntaxError(
          """Invalid input 'TOPOLOGY': expected 'NOWAIT', 'OPTIONS', 'WAIT', <EOF> or an integer value (line 1, column 40 (offset: 39))
            |"CREATE DATABASE foo TOPOLOGY 1 PRIMARY TOPOLOGY 1 SECONDARY"
            |                                        ^""".stripMargin
        )
    }
  }

  test("CREATE DATABASE foo TOPOLOGY 1 PRIMARY 1 PRIMARY") {
    failsParsing[Statements].in {
      case Cypher5JavaCc =>
        _.withMessageStart("""Duplicate PRIMARY clause (line 1, column 42 (offset: 41))""".stripMargin)
      case _ => _.withSyntaxErrorContaining(
          """Duplicate PRIMARY clause (line 1, column 40 (offset: 39))""".stripMargin
        )
    }
  }

  test("CREATE DATABASE foo TOPOLOGY 2 PRIMARIES 1 PRIMARY") {
    failsParsing[Statements].in {
      case Cypher5JavaCc =>
        _.withMessageStart("""Duplicate PRIMARY clause (line 1, column 44 (offset: 43))""".stripMargin)
      case _ => _.withSyntaxErrorContaining(
          """Duplicate PRIMARY clause (line 1, column 42 (offset: 41))"""
        )
    }
  }

  test("CREATE DATABASE foo TOPOLOGY 2 SECONDARIES 1 SECONDARY") {
    failsParsing[Statements].in {
      case Cypher5JavaCc =>
        _.withMessageStart("""Duplicate SECONDARY clause (line 1, column 46 (offset: 45))""".stripMargin)
      case _ => _.withSyntaxErrorContaining(
          """Duplicate SECONDARY clause (line 1, column 44 (offset: 43))""".stripMargin
        )
    }
  }

  test("CREATE DATABASE foo TOPOLOGY 1 PRIMARY 1 SECONDARY 2 SECONDARY") {
    failsParsing[Statements].in {
      case Cypher5JavaCc =>
        _.withMessageStart("""Duplicate SECONDARY clause (line 1, column 54 (offset: 53))""".stripMargin)
      case _ => _.withSyntaxErrorContaining(
          """Duplicate SECONDARY clause (line 1, column 52 (offset: 51))""".stripMargin
        )
    }
  }

  test("CREATE DATABASE foo TOPOLOGY -1 PRIMARY") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(
          """Invalid input '-': expected <UNSIGNED_DECIMAL_INTEGER> (line 1, column 30 (offset: 29))""".stripMargin
        )
      case _ => _.withSyntaxError(
          """Invalid input '-': expected an integer value (line 1, column 30 (offset: 29))
            |"CREATE DATABASE foo TOPOLOGY -1 PRIMARY"
            |                              ^""".stripMargin
        )
    }
  }

  test("CREATE DATABASE foo TOPOLOGY 1 PRIMARY -1 SECONDARY") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(
          """Invalid input '-': expected
            |  "NOWAIT"
            |  "OPTIONS"
            |  "WAIT"
            |  <EOF>
            |  <UNSIGNED_DECIMAL_INTEGER> (line 1, column 40 (offset: 39))""".stripMargin
        )
      case _ => _.withSyntaxError(
          """Invalid input '-': expected 'NOWAIT', 'OPTIONS', 'WAIT', <EOF> or an integer value (line 1, column 40 (offset: 39))
            |"CREATE DATABASE foo TOPOLOGY 1 PRIMARY -1 SECONDARY"
            |                                        ^""".stripMargin
        )
    }
  }

  test("CREATE DATABASE foo TOPOLOGY -1 SECONDARY 1 PRIMARY") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(
          """Invalid input '-': expected <UNSIGNED_DECIMAL_INTEGER> (line 1, column 30 (offset: 29))"""
        )
      // Modify update error message. -1 is an integer...
      case _ => _.withSyntaxError(
          """Invalid input '-': expected an integer value (line 1, column 30 (offset: 29))
            |"CREATE DATABASE foo TOPOLOGY -1 SECONDARY 1 PRIMARY"
            |                              ^""".stripMargin
        )
    }
  }

  test("CREATE DATABASE foo TOPOLOGY 1 SECONDARY 1 SECONDARY") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("""Duplicate SECONDARY clause (line 1, column 44 (offset: 43))""")
      case _ => _.withSyntaxErrorContaining("""Duplicate SECONDARY clause (line 1, column 42 (offset: 41))""")
    }
  }

  test("CREATE DATABASE foo TOPOLOGY $param PRIMARY") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(
          """Invalid input '$': expected <UNSIGNED_DECIMAL_INTEGER> (line 1, column 30 (offset: 29))"""
        )
      case _ => _.withSyntaxError(
          """Invalid input '$': expected an integer value (line 1, column 30 (offset: 29))
            |"CREATE DATABASE foo TOPOLOGY $param PRIMARY"
            |                              ^""".stripMargin
        )
    }
  }

  test("CREATE DATABASE foo TOPOLOGY 1 PRIMARY $param SECONDARY") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(
          """Invalid input '$': expected
            |  "NOWAIT"
            |  "OPTIONS"
            |  "WAIT"
            |  <EOF>
            |  <UNSIGNED_DECIMAL_INTEGER> (line 1, column 40 (offset: 39))""".stripMargin
        )
      case _ => _.withSyntaxError(
          """Invalid input '$': expected 'NOWAIT', 'OPTIONS', 'WAIT', <EOF> or an integer value (line 1, column 40 (offset: 39))
            |"CREATE DATABASE foo TOPOLOGY 1 PRIMARY $param SECONDARY"
            |                                        ^""".stripMargin
        )
    }
  }

  test("CREATE DATABASE foo TOPOLOGY") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(
          """Invalid input '': expected <UNSIGNED_DECIMAL_INTEGER> (line 1, column 29 (offset: 28))"""
        )
      case _ => _.withSyntaxError(
          """Invalid input '': expected an integer value (line 1, column 29 (offset: 28))
            |"CREATE DATABASE foo TOPOLOGY"
            |                             ^""".stripMargin
        )
    }
  }
}
