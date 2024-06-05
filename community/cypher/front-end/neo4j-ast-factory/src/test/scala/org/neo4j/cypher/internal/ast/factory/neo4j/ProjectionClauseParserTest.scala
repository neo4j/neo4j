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
import org.neo4j.cypher.internal.ast.Clause
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsing.Cypher5JavaCc
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsingTestBase

class ProjectionClauseParserTest extends AstParsingTestBase {

  test("WITH *") {
    parsesTo[Clause](ast.With(ast.ReturnItems(includeExisting = true, Seq.empty)(pos))(pos))
  }

  test("WITH 1 AS a") {
    parsesTo[Clause](ast.With(ast.ReturnItems(
      includeExisting = false,
      Seq(ast.AliasedReturnItem(literalInt(1), varFor("a"))(pos))
    )(pos))(pos))
  }

  test("WITH *, 1 AS a") {
    parsesTo[Clause](ast.With(ast.ReturnItems(
      includeExisting = true,
      Seq(ast.AliasedReturnItem(literalInt(1), varFor("a"))(pos))
    )(pos))(pos))
  }

  test("WITH ") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input '': expected \"*\", \"DISTINCT\" or an expression")
      case _ => _.withMessage(
          """Invalid input '': expected an expression, '*' or 'DISTINCT' (line 1, column 5 (offset: 4))
            |"WITH"
            |     ^""".stripMargin
        )
    }
  }

  test("RETURN *") {
    parsesTo[Clause](ast.Return(ast.ReturnItems(includeExisting = true, Seq.empty)(pos))(pos))
  }

  test("RETURN 1 AS a") {
    parsesTo[Clause](ast.Return(ast.ReturnItems(
      includeExisting = false,
      Seq(ast.AliasedReturnItem(literalInt(1), varFor("a"))(pos))
    )(pos))(pos))
  }

  test("RETURN *, 1 AS a") {
    parsesTo[Clause](ast.Return(ast.ReturnItems(
      includeExisting = true,
      Seq(ast.AliasedReturnItem(literalInt(1), varFor("a"))(pos))
    )(pos))(pos))
  }

  test("RETURN ") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input '': expected \"*\", \"DISTINCT\" or an expression")
      case _ => _.withMessage(
          """Invalid input '': expected an expression, '*' or 'DISTINCT' (line 1, column 7 (offset: 6))
            |"RETURN"
            |       ^""".stripMargin
        )
    }
  }

  test("RETURN GRAPH *") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input '': expected \"+\" or \"-\"")
      case _ => _.withMessage(
          """Invalid input '': expected an expression (line 1, column 15 (offset: 14))
            |"RETURN GRAPH *"
            |               ^""".stripMargin
        )
    }
  }
}
