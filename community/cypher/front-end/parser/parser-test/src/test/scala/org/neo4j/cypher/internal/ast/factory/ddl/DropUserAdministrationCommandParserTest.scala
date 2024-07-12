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

import org.neo4j.cypher.internal.ast.DropUser
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.test.util.AstParsing.Cypher5JavaCc

class DropUserAdministrationCommandParserTest extends UserAdministrationCommandParserTestBase {

  test("DROP USER foo") {
    parsesTo[Statements](DropUser(literalFoo, ifExists = false)(pos))
  }

  test("DROP USER $foo") {
    parsesTo[Statements](DropUser(paramFoo, ifExists = false)(pos))
  }

  test("DROP USER ``") {
    parsesTo[Statements](DropUser(literalEmpty, ifExists = false)(pos))
  }

  test("DROP USER `f:oo`") {
    parsesTo[Statements](DropUser(literalFColonOo, ifExists = false)(pos))
  }

  test("DROP USER foo IF EXISTS") {
    parsesTo[Statements](DropUser(literalFoo, ifExists = true)(pos))
  }

  test("DROP USER `` IF EXISTS") {
    parsesTo[Statements](DropUser(literalEmpty, ifExists = true)(pos))
  }

  test("DROP USER `f:oo` IF EXISTS") {
    parsesTo[Statements](DropUser(literalFColonOo, ifExists = true)(pos))
  }

  // fails parsing

  test("DROP USER ") {
    failsParsing[Statements].in {
      case Cypher5JavaCc =>
        _.withMessageStart("Invalid input '': expected a parameter or an identifier (line 1, column 10 (offset: 9))")
      case _ => _.withSyntaxError(
          """Invalid input '': expected a parameter or an identifier (line 1, column 10 (offset: 9))
            |"DROP USER"
            |          ^""".stripMargin
        )
    }
  }

  test("DROP USER  IF EXISTS") {
    failsParsing[Statements].in {
      case Cypher5JavaCc =>
        _.withMessageStart("Invalid input 'EXISTS': expected \"IF\" or <EOF> (line 1, column 15 (offset: 14))")
      case _ => _.withSyntaxError(
          """Invalid input 'EXISTS': expected 'IF EXISTS' or <EOF> (line 1, column 15 (offset: 14))
            |"DROP USER  IF EXISTS"
            |               ^""".stripMargin
        )
    }
  }

  test("DROP USER foo IF NOT EXISTS") {
    failsParsing[Statements].in {
      case Cypher5JavaCc =>
        _.withMessageStart("Invalid input 'NOT': expected \"EXISTS\" (line 1, column 18 (offset: 17))")
      case _ => _.withSyntaxError(
          """Invalid input 'NOT': expected 'EXISTS' (line 1, column 18 (offset: 17))
            |"DROP USER foo IF NOT EXISTS"
            |                  ^""".stripMargin
        )
    }
  }
}
