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
package org.neo4j.cypher.internal.ast.factory.query

import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.test.util.AstParsing.Cypher5JavaCc
import org.neo4j.cypher.internal.ast.test.util.AstParsingTestBase
import org.neo4j.cypher.internal.ast.test.util.LegacyAstParsingTestSupport

class UseParserTest extends AstParsingTestBase with LegacyAstParsingTestSupport {

  test("USING PERIODIC COMMIT USE db LOAD CSV FROM 'url' AS line RETURN line") {
    failsParsing[Statements]
  }

  test("USE GRAPH db USING PERIODIC COMMIT LOAD CSV FROM 'url' AS line RETURN line") {
    failsParsing[Statements]
  }

  test("USE 1 RETURN 1") {
    failsParsing[Statements]
  }

  test("USE 'a' RETURN 1") {
    failsParsing[Statements]
  }

  test("USE [x] RETURN 1") {
    failsParsing[Statements]
  }

  test("USE 1 + 2 RETURN 1") {
    failsParsing[Statements]
  }

  test("CALL { USE neo4j RETURN 1 AS y } RETURN y") {
    parsesTo[Statements] {
      singleQuery(
        subqueryCall(
          use(List("neo4j")),
          returnLit(1 -> "y")
        ),
        return_(variableReturnItem("y"))
      )
    }
  }

  test("WITH 1 AS x CALL { WITH x USE neo4j RETURN x AS y } RETURN x, y") {
    parsesTo[Statements] {
      singleQuery(
        with_(literal(1) as "x"),
        subqueryCall(
          with_(variableReturnItem("x")),
          use(List("neo4j")),
          return_(varFor("x") as "y")
        ),
        return_(variableReturnItem("x"), variableReturnItem("y"))
      )
    }
  }

  test("USE foo UNION ALL RETURN 1") {
    parsesTo[Statements] {
      union(
        singleQuery(use(List("foo"))),
        singleQuery(return_(returnItem(literal(1), "1")))
      ).all
    }
  }

  test("USE GRAPH neo4j RETURN 1") {
    parsesTo[Statements] {
      singleQuery(
        use(List("neo4j")),
        return_(returnItem(literal(1), "1"))
      )
    }
  }

  // Should be able to have database name "graph" (only works in Antlr).
  test("USE GRAPH RETURN 1") {
    parsesIn[Statements] {
      case Cypher5JavaCc => _.withAnyFailure
      case _ => _.toAst(Statements(Seq(singleQuery(use(List("GRAPH")), return_(returnItem(literal(1), "1"))))))
    }
  }
}
