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
package org.neo4j.cypher.internal.ast.factory.neo4j

import org.neo4j.cypher.internal.ast
import org.neo4j.cypher.internal.ast.Statements

class RenameUserAdministrationCommandParserTest extends UserAdministrationCommandParserTestBase {

  test("RENAME USER foo TO bar") {
    parsesTo[Statements](ast.RenameUser(literalFoo, literalBar, ifExists = false)(pos))
  }

  test("RENAME USER foo TO $bar") {
    parsesTo[Statements](ast.RenameUser(literalFoo, stringParam("bar"), ifExists = false)(pos))
  }

  test("RENAME USER $foo TO bar") {
    parsesTo[Statements](ast.RenameUser(stringParam("foo"), literalBar, ifExists = false)(pos))
  }

  test("RENAME USER $foo TO $bar") {
    parsesTo[Statements](ast.RenameUser(stringParam("foo"), stringParam("bar"), ifExists = false)(pos))
  }

  test("RENAME USER foo IF EXISTS TO bar") {
    parsesTo[Statements](ast.RenameUser(literalFoo, literalBar, ifExists = true)(pos))
  }

  test("RENAME USER foo IF EXISTS TO $bar") {
    parsesTo[Statements](ast.RenameUser(literalFoo, stringParam("bar"), ifExists = true)(pos))
  }

  test("RENAME USER $foo IF EXISTS TO bar") {
    parsesTo[Statements](ast.RenameUser(stringParam("foo"), literalBar, ifExists = true)(pos))
  }

  test("RENAME USER $foo IF EXISTS TO $bar") {
    parsesTo[Statements](ast.RenameUser(stringParam("foo"), stringParam("bar"), ifExists = true)(pos))
  }

  test("RENAME USER foo TO ``") {
    parsesTo[Statements](ast.RenameUser(literalFoo, literalEmpty, ifExists = false)(pos))
  }

  test("RENAME USER `` TO bar") {
    parsesTo[Statements](ast.RenameUser(literalEmpty, literalBar, ifExists = false)(pos))
  }

  // fails parsing

  test("RENAME USER foo TO") {
    assertFailsWithMessage[Statements](
      testName,
      "Invalid input '': expected a parameter or an identifier (line 1, column 19 (offset: 18))"
    )
  }

  test("RENAME USER TO bar") {
    assertFailsWithMessage[Statements](
      testName,
      "Invalid input 'bar': expected \"IF\" or \"TO\" (line 1, column 16 (offset: 15))"
    )
  }

  test("RENAME USER TO") {
    assertFailsWithMessage[Statements](
      testName,
      "Invalid input '': expected \"IF\" or \"TO\" (line 1, column 15 (offset: 14))"
    )
  }

  test("RENAME USER foo SET NAME TO bar") {
    assertFailsWithMessage[Statements](
      testName,
      "Invalid input 'SET': expected \"IF\" or \"TO\" (line 1, column 17 (offset: 16))"
    )
  }

  test("RENAME USER foo SET NAME bar") {
    assertFailsWithMessage[Statements](
      testName,
      "Invalid input 'SET': expected \"IF\" or \"TO\" (line 1, column 17 (offset: 16))"
    )
  }

  test("RENAME USER foo IF EXIST TO bar") {
    assertFailsWithMessage[Statements](
      testName,
      "Invalid input 'EXIST': expected \"EXISTS\" (line 1, column 20 (offset: 19))"
    )
  }

  test("RENAME USER foo IF NOT EXISTS TO bar") {
    assertFailsWithMessage[Statements](
      testName,
      "Invalid input 'NOT': expected \"EXISTS\" (line 1, column 20 (offset: 19))"
    )
  }

  test("RENAME USER foo TO bar IF EXISTS") {
    assertFailsWithMessage[Statements](testName, "Invalid input 'IF': expected <EOF> (line 1, column 24 (offset: 23))")
  }

  test("RENAME IF EXISTS USER foo TO bar") {
    assertFailsWithMessage[Statements](
      testName,
      "Invalid input 'IF': expected \"ROLE\", \"SERVER\" or \"USER\" (line 1, column 8 (offset: 7))"
    )
  }

  test("RENAME OR REPLACE USER foo TO bar") {
    assertFailsWithMessage[Statements](
      testName,
      "Invalid input 'OR': expected \"ROLE\", \"SERVER\" or \"USER\" (line 1, column 8 (offset: 7))"
    )
  }

  test("RENAME USER foo TO bar SET PASSWORD 'secret'") {
    failsToParse[Statements]
  }
}
