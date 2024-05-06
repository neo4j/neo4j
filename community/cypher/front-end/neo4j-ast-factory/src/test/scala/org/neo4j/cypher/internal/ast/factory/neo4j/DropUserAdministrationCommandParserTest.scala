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
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsing.Antlr
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsing.JavaCc
import org.neo4j.exceptions.SyntaxException

class DropUserAdministrationCommandParserTest extends UserAdministrationCommandParserTestBase {

  test("DROP USER foo") {
    parsesTo[Statements](ast.DropUser(literalFoo, ifExists = false)(pos))
  }

  test("DROP USER $foo") {
    parsesTo[Statements](ast.DropUser(paramFoo, ifExists = false)(pos))
  }

  test("DROP USER ``") {
    parsesTo[Statements](ast.DropUser(literalEmpty, ifExists = false)(pos))
  }

  test("DROP USER `f:oo`") {
    parsesTo[Statements](ast.DropUser(literalFColonOo, ifExists = false)(pos))
  }

  test("DROP USER foo IF EXISTS") {
    parsesTo[Statements](ast.DropUser(literalFoo, ifExists = true)(pos))
  }

  test("DROP USER `` IF EXISTS") {
    parsesTo[Statements](ast.DropUser(literalEmpty, ifExists = true)(pos))
  }

  test("DROP USER `f:oo` IF EXISTS") {
    parsesTo[Statements](ast.DropUser(literalFColonOo, ifExists = true)(pos))
  }

  // fails parsing

  test("DROP USER ") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        "Invalid input '': expected a parameter or an identifier (line 1, column 10 (offset: 9))"
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Invalid input '': expected a parameter or an identifier (line 1, column 10 (offset: 9))
          |"DROP USER"
          |          ^""".stripMargin
      ))
  }

  test("DROP USER  IF EXISTS") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        "Invalid input 'EXISTS': expected \"IF\" or <EOF> (line 1, column 15 (offset: 14))"
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Invalid input 'EXISTS': expected 'IF EXISTS' or <EOF> (line 1, column 15 (offset: 14))
          |"DROP USER  IF EXISTS"
          |               ^""".stripMargin
      ))
  }

  test("DROP USER foo IF NOT EXISTS") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart("Invalid input 'NOT': expected \"EXISTS\" (line 1, column 18 (offset: 17))"))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Invalid input 'NOT': expected 'EXISTS' (line 1, column 18 (offset: 17))
          |"DROP USER foo IF NOT EXISTS"
          |                  ^""".stripMargin
      ))
  }
}
