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

import org.neo4j.cypher.internal.ast.CascadeAliases
import org.neo4j.cypher.internal.ast.DestroyData
import org.neo4j.cypher.internal.ast.DropDatabase
import org.neo4j.cypher.internal.ast.DumpData
import org.neo4j.cypher.internal.ast.IndefiniteWait
import org.neo4j.cypher.internal.ast.NamespacedName
import org.neo4j.cypher.internal.ast.NoWait
import org.neo4j.cypher.internal.ast.Restrict
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.TimeoutAfter
import org.neo4j.cypher.internal.ast.test.util.AstParsing.Cypher5JavaCc

class DropDatabaseAdministrationCommandParserTest extends AdministrationAndSchemaCommandParserTestBase {

  test("DROP DATABASE foo") {
    parsesTo[Statements](DropDatabase(
      literal("foo"),
      ifExists = false,
      composite = false,
      Restrict,
      DestroyData,
      NoWait
    )(pos))
  }

  test("DROP DATABASE alias") {
    parsesTo[Statements](DropDatabase(
      literal("alias"),
      ifExists = false,
      composite = false,
      Restrict,
      DestroyData,
      NoWait
    )(pos))
  }

  test("DROP DATABASE alias WAIT") {
    parsesTo[Statements](DropDatabase(
      literal("alias"),
      ifExists = false,
      composite = false,
      Restrict,
      DestroyData,
      IndefiniteWait
    )(pos))
  }

  test("DROP DATABASE alias NOWAIT") {
    parsesTo[Statements](DropDatabase(
      literal("alias"),
      ifExists = false,
      composite = false,
      Restrict,
      DestroyData,
      NoWait
    )(pos))
  }

  test("DROP DATABASE $foo") {
    parsesTo[Statements](DropDatabase(
      stringParamName("foo"),
      ifExists = false,
      composite = false,
      Restrict,
      DestroyData,
      NoWait
    )(pos))
  }

  test("DROP DATABASE foo WAIT") {
    parsesTo[Statements](DropDatabase(
      literal("foo"),
      ifExists = false,
      composite = false,
      Restrict,
      DestroyData,
      IndefiniteWait
    )(pos))
  }

  test("DROP DATABASE foo WAIT 10") {
    parsesTo[Statements](DropDatabase(
      literal("foo"),
      ifExists = false,
      composite = false,
      Restrict,
      DestroyData,
      TimeoutAfter(10)
    )(pos))
  }

  test("DROP DATABASE foo WAIT 10 SEC") {
    parsesTo[Statements](DropDatabase(
      literal("foo"),
      ifExists = false,
      composite = false,
      Restrict,
      DestroyData,
      TimeoutAfter(10)
    )(pos))
  }

  test("DROP DATABASE foo WAIT 10 SECOND") {
    parsesTo[Statements](DropDatabase(
      literal("foo"),
      ifExists = false,
      composite = false,
      Restrict,
      DestroyData,
      TimeoutAfter(10)
    )(pos))
  }

  test("DROP DATABASE foo WAIT 10 SECONDS") {
    parsesTo[Statements](DropDatabase(
      literal("foo"),
      ifExists = false,
      composite = false,
      Restrict,
      DestroyData,
      TimeoutAfter(10)
    )(pos))
  }

  test("DROP DATABASE foo NOWAIT") {
    parsesTo[Statements](
      DropDatabase(literalFoo, ifExists = false, composite = false, Restrict, DestroyData, NoWait)(pos)
    )
  }

  test("DROP DATABASE `foo.bar`") {
    parsesTo[Statements](
      DropDatabase(literal("foo.bar"), ifExists = false, composite = false, Restrict, DestroyData, NoWait)(pos)
    )
  }

  test("DROP DATABASE foo.bar") {
    parsesTo[Statements](
      DropDatabase(
        NamespacedName(List("bar"), Some("foo"))((1, 14, 13)),
        ifExists = false,
        composite = false,
        Restrict,
        DestroyData,
        NoWait
      )(pos)
    )
  }

  test("DROP DATABASE foo IF EXISTS") {
    parsesTo[Statements](
      DropDatabase(literalFoo, ifExists = true, composite = false, Restrict, DestroyData, NoWait)(pos)
    )
  }

  test("DROP DATABASE foo IF EXISTS WAIT") {
    parsesTo[Statements](DropDatabase(
      literalFoo,
      ifExists = true,
      composite = false,
      Restrict,
      DestroyData,
      IndefiniteWait
    )(pos))
  }

  test("DROP DATABASE foo IF EXISTS NOWAIT") {
    parsesTo[Statements](
      DropDatabase(literalFoo, ifExists = true, composite = false, Restrict, DestroyData, NoWait)(pos)
    )
  }

  test("DROP DATABASE foo DUMP DATA") {
    parsesTo[Statements](
      DropDatabase(literalFoo, ifExists = false, composite = false, Restrict, DumpData, NoWait)(pos)
    )
  }

  test("DROP DATABASE foo DESTROY DATA") {
    parsesTo[Statements](
      DropDatabase(literalFoo, ifExists = false, composite = false, Restrict, DestroyData, NoWait)(pos)
    )
  }

  test("DROP DATABASE foo IF EXISTS DUMP DATA") {
    parsesTo[Statements](
      DropDatabase(literalFoo, ifExists = true, composite = false, Restrict, DumpData, NoWait)(pos)
    )
  }

  test("DROP DATABASE foo IF EXISTS DESTROY DATA") {
    parsesTo[Statements](
      DropDatabase(literalFoo, ifExists = true, composite = false, Restrict, DestroyData, NoWait)(pos)
    )
  }

  test("DROP DATABASE foo IF EXISTS DESTROY DATA WAIT") {
    parsesTo[Statements](DropDatabase(
      literal("foo"),
      ifExists = true,
      composite = false,
      Restrict,
      DestroyData,
      IndefiniteWait
    )(pos))
  }

  test("DROP DATABASE foo RESTRICT") {
    parsesTo[Statements](DropDatabase(
      literal("foo"),
      ifExists = false,
      composite = false,
      Restrict,
      DestroyData,
      NoWait
    )(pos))
  }

  test("DROP DATABASE foo CASCADE ALIAS") {
    parsesTo[Statements](DropDatabase(
      literal("foo"),
      ifExists = false,
      composite = false,
      CascadeAliases,
      DestroyData,
      NoWait
    )(pos))
  }

  test("DROP DATABASE foo IF EXISTS RESTRICT") {
    parsesTo[Statements](DropDatabase(
      literal("foo"),
      ifExists = true,
      composite = false,
      Restrict,
      DestroyData,
      NoWait
    )(pos))
  }

  test("DROP DATABASE foo IF EXISTS CASCADE ALIASES") {
    parsesTo[Statements](DropDatabase(
      literal("foo"),
      ifExists = true,
      composite = false,
      CascadeAliases,
      DestroyData,
      NoWait
    )(pos))
  }

  test("DROP DATABASE foo RESTRICT DUMP DATA") {
    parsesTo[Statements](DropDatabase(
      literal("foo"),
      ifExists = false,
      composite = false,
      Restrict,
      DumpData,
      NoWait
    )(pos))
  }

  test("DROP DATABASE foo CASCADE ALIAS DESTROY DATA") {
    parsesTo[Statements](DropDatabase(
      literal("foo"),
      ifExists = false,
      composite = false,
      CascadeAliases,
      DestroyData,
      NoWait
    )(pos))
  }

  test("DROP DATABASE foo RESTRICT NOWAIT") {
    parsesTo[Statements](DropDatabase(
      literal("foo"),
      ifExists = false,
      composite = false,
      Restrict,
      DestroyData,
      NoWait
    )(pos))
  }

  test("DROP DATABASE foo CASCADE ALIASES WAIT") {
    parsesTo[Statements](DropDatabase(
      literal("foo"),
      ifExists = false,
      composite = false,
      CascadeAliases,
      DestroyData,
      IndefiniteWait
    )(pos))
  }

  test("DROP DATABASE") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(
          """Invalid input '': expected a parameter or an identifier (line 1, column 14 (offset: 13))"""
        )
      case _ => _.withSyntaxError(
          """Invalid input '': expected a database name or a parameter (line 1, column 14 (offset: 13))
            |"DROP DATABASE"
            |              ^""".stripMargin
        )
    }
  }

  test("DROP DATABASE  IF EXISTS") {
    failsParsing[Statements]
  }

  test("DROP DATABASE foo IF NOT EXISTS") {
    failsParsing[Statements]
  }

  test("DROP DATABASE `foo`.`bar`.`baz`") {
    failsParsing[Statements].withMessageStart(
      "Invalid input ``foo`.`bar`.`baz`` for name. Expected name to contain at most two components separated by `.`."
    )
  }

  test("DROP DATABASE KEEP DATA") {
    val exceptionMessage =
      """Invalid input 'DATA': expected
        |  "."
        |  "CASCADE"
        |  "DESTROY"
        |  "DUMP"
        |  "IF"
        |  "NOWAIT"
        |  "RESTRICT"
        |  "WAIT"
        |  <EOF> (line 1, column 20 (offset: 19))""".stripMargin

    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(exceptionMessage)
      case _ => _.withSyntaxError(
          """Invalid input 'DATA': expected a database name, 'CASCADE', 'DESTROY', 'DUMP', 'IF EXISTS', 'NOWAIT', 'RESTRICT', 'WAIT' or <EOF> (line 1, column 20 (offset: 19))
            |"DROP DATABASE KEEP DATA"
            |                    ^""".stripMargin
        )
    }
  }

  test("DROP DATABASE db KEEP DATA") {
    val exceptionMessage =
      """Invalid input 'KEEP': expected
        |  "."
        |  "CASCADE"
        |  "DESTROY"
        |  "DUMP"
        |  "IF"
        |  "NOWAIT"
        |  "RESTRICT"
        |  "WAIT"
        |  <EOF> (line 1, column 18 (offset: 17))""".stripMargin

    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(exceptionMessage)
      case _ => _.withSyntaxError(
          """Invalid input 'KEEP': expected a database name, 'CASCADE', 'DESTROY', 'DUMP', 'IF EXISTS', 'NOWAIT', 'RESTRICT', 'WAIT' or <EOF> (line 1, column 18 (offset: 17))
            |"DROP DATABASE db KEEP DATA"
            |                  ^""".stripMargin
        )
    }
  }

  test("DROP DATABASE foo CASCADE") {
    val exceptionMessage =
      """Invalid input '': expected "ALIAS" or "ALIASES" (line 1, column 26 (offset: 25))"""

    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(exceptionMessage)
      case _ => _.withSyntaxError(
          """Invalid input '': expected 'ALIAS' or 'ALIASES' (line 1, column 26 (offset: 25))
            |"DROP DATABASE foo CASCADE"
            |                          ^""".stripMargin
        )
    }
  }

  test("DROP DATABASE foo DUMP DATA CASCADE ALIASES") {
    val exceptionMessage =
      """Invalid input 'CASCADE': expected "NOWAIT", "WAIT" or <EOF> (line 1, column 29 (offset: 28))"""

    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(exceptionMessage)
      case _ => _.withSyntaxError(
          """Invalid input 'CASCADE': expected 'NOWAIT', 'WAIT' or <EOF> (line 1, column 29 (offset: 28))
            |"DROP DATABASE foo DUMP DATA CASCADE ALIASES"
            |                             ^""".stripMargin
        )
    }
  }

  test("DROP DATABASE foo DESTROY DATA RESTRICT") {
    val exceptionMessage =
      """Invalid input 'RESTRICT': expected "NOWAIT", "WAIT" or <EOF> (line 1, column 32 (offset: 31))"""

    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(exceptionMessage)
      case _ => _.withSyntaxError(
          """Invalid input 'RESTRICT': expected 'NOWAIT', 'WAIT' or <EOF> (line 1, column 32 (offset: 31))
            |"DROP DATABASE foo DESTROY DATA RESTRICT"
            |                                ^""".stripMargin
        )
    }
  }
}
