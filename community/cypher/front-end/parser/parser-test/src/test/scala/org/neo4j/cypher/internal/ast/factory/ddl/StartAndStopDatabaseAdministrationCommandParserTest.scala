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

import org.neo4j.cypher.internal.ast.IndefiniteWait
import org.neo4j.cypher.internal.ast.NamespacedName
import org.neo4j.cypher.internal.ast.NoWait
import org.neo4j.cypher.internal.ast.StartDatabase
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.StopDatabase
import org.neo4j.cypher.internal.ast.TimeoutAfter
import org.neo4j.cypher.internal.ast.test.util.AstParsing.Cypher5JavaCc

class StartAndStopDatabaseAdministrationCommandParserTest extends AdministrationAndSchemaCommandParserTestBase {

  // START DATABASE

  test("START DATABASE foo") {
    parsesTo[Statements](StartDatabase(literalFoo, NoWait)(pos))
  }

  test("START DATABASE $foo") {
    parsesTo[Statements](StartDatabase(stringParamName("foo"), NoWait)(pos))
  }

  test("START DATABASE foo WAIT") {
    parsesTo[Statements](StartDatabase(literalFoo, IndefiniteWait)(pos))
  }

  test("START DATABASE foo WAIT 5") {
    parsesTo[Statements](StartDatabase(literal("foo"), TimeoutAfter(5))(pos))
  }

  test("START DATABASE foo WAIT 5 SEC") {
    parsesTo[Statements](StartDatabase(literal("foo"), TimeoutAfter(5))(pos))
  }

  test("START DATABASE foo WAIT 5 SECOND") {
    parsesTo[Statements](StartDatabase(literal("foo"), TimeoutAfter(5))(pos))
  }

  test("START DATABASE foo WAIT 5 SECONDS") {
    parsesTo[Statements](StartDatabase(literal("foo"), TimeoutAfter(5))(pos))
  }

  test("START DATABASE foo NOWAIT") {
    parsesTo[Statements](StartDatabase(literalFoo, NoWait)(pos))
  }

  test("START DATABASE `foo.bar`") {
    parsesTo[Statements](StartDatabase(literal("foo.bar"), NoWait)(pos))
  }

  test("START DATABASE foo.bar") {
    parsesTo[Statements](StartDatabase(NamespacedName(List("bar"), Some("foo"))((1, 16, 15)), NoWait)(pos))
  }

  test("START DATABASE") {
    failsParsing[Statements].in {
      case Cypher5JavaCc =>
        _.withMessageStart("Invalid input '': expected a parameter or an identifier (line 1, column 15 (offset: 14))")
      case _ => _.withSyntaxError(
          """Invalid input '': expected a database name or a parameter (line 1, column 15 (offset: 14))
            |"START DATABASE"
            |               ^""".stripMargin
        )
    }
  }

  test("START DATABASE `foo`.bar.`baz`") {
    failsParsing[Statements].withMessageStart(
      "Invalid input ``foo`.bar.`baz`` for name. Expected name to contain at most two components separated by `.`."
    )
  }

  // STOP DATABASE

  test("STOP DATABASE foo") {
    parsesTo[Statements](StopDatabase(literalFoo, NoWait)(pos))
  }

  test("STOP DATABASE $foo") {
    parsesTo[Statements](StopDatabase(stringParamName("foo"), NoWait)(pos))
  }

  test("STOP DATABASE foo WAIT") {
    parsesTo[Statements](StopDatabase(literalFoo, IndefiniteWait)(pos))
  }

  test("STOP DATABASE foo WAIT 99") {
    parsesTo[Statements](StopDatabase(literal("foo"), TimeoutAfter(99))(pos))
  }

  test("STOP DATABASE foo WAIT 99 SEC") {
    parsesTo[Statements](StopDatabase(literal("foo"), TimeoutAfter(99))(pos))
  }

  test("STOP DATABASE foo WAIT 99 SECOND") {
    parsesTo[Statements](StopDatabase(literal("foo"), TimeoutAfter(99))(pos))
  }

  test("STOP DATABASE foo WAIT 99 SECONDS") {
    parsesTo[Statements](StopDatabase(literal("foo"), TimeoutAfter(99))(pos))
  }

  test("STOP DATABASE foo NOWAIT") {
    parsesTo[Statements](StopDatabase(literalFoo, NoWait)(pos))
  }

  test("STOP DATABASE `foo.bar`") {
    parsesTo[Statements](StopDatabase(literal("foo.bar"), NoWait)(pos))
  }

  test("STOP DATABASE foo.bar") {
    parsesTo[Statements](StopDatabase(NamespacedName(List("bar"), Some("foo"))((1, 16, 15)), NoWait)(pos))
  }

  test("STOP DATABASE `foo`.bar.`baz`") {
    failsParsing[Statements].withMessageStart(
      "Invalid input ``foo`.bar.`baz`` for name. Expected name to contain at most two components separated by `.`."
    )
  }

  test("STOP DATABASE") {
    failsParsing[Statements].in {
      case Cypher5JavaCc =>
        _.withMessageStart("Invalid input '': expected a parameter or an identifier (line 1, column 14 (offset: 13))")
      case _ => _.withSyntaxError(
          """Invalid input '': expected a database name or a parameter (line 1, column 14 (offset: 13))
            |"STOP DATABASE"
            |              ^""".stripMargin
        )
    }
  }
}
