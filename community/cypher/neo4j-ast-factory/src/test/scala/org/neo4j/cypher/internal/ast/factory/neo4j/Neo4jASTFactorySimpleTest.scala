/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.ast.factory.neo4j

import org.neo4j.cypher.internal.util.test_helpers.TestName
import org.scalatest.FunSuiteLike

class Neo4jASTFactorySimpleTest extends ParsingTestBase with FunSuiteLike with TestName {

  test("RETURN 1 AS x") {
    assertSameAST(testName)
  }

  test("RETURN 'apoks\\tf\\np' AS x") {
    assertSameAST(testName)
  }

  test("RETURN \"apoks\\tf\\np\" AS x") {
    assertSameAST(testName)
  }

  test("RETURN '\r\n\t\b\f'") {
    assertSameAST(testName)
  }
  test("RETURN '\\\\\\''") {
    assertSameAST(testName)
  }

  test("RETURN 'hi' AS `call`") {
    assertSameAST(testName)
  }

  test("RETURN '\\u01FF' AS a2114") {
    assertSameAST(testName)
  }

  test("RETURN '\uD83D\uDCA9' AS `turd`") {
    assertSameAST(testName)
  }

  test("RETURN NOT true") {
    assertSameAST(testName)
  }

  test("RETURN filter (x IN [1,2,3] WHERE x = 2) AS k") {
    assertSameAST(testName)
  }

  test("RETURN 1 AS x //l33t comment") {
    assertSameAST(testName)
  }

  test("MATCH (a),(b) RETURN shortestPath((a)-[*]->(b)) as path") {
    assertSameAST(testName)
  }

  test("keywords are allowed names") {
    val keywords =
      Seq("RETURN", "CREATE", "DELETE", "SET", "REMOVE", "DETACH", "MATCH", "WITH",
          "UNWIND", "USE", "GRAPH", "CALL", "YIELD", "LOAD", "CSV", "PERIODIC", "COMMIT",
          "HEADERS", "FROM", "FIELDTERMINATOR", "FOREACH", "WHERE", "DISTINCT", "MERGE",
          "OPTIONAL", "USING", "ORDER", "BY", "ASC", "ASCENDING", "DESC", "DESCENDING",
          "SKIP", "LIMIT", "UNION", "DROP", "INDEX", "SEEK", "SCAN", "JOIN", "CONSTRAINT",
          "ASSERT", "IS", "NODE", "KEY", "UNIQUE", "ON", "AS", "OR", "XOR", "AND", "NOT",
          "STARTS", "ENDS", "CONTAINS", "IN", "count", "FILTER", "EXTRACT", "REDUCE",
          "EXISTS", "ALL", "ANY", "NONE", "SINGLE", "CASE", "ELSE", "WHEN", "THEN", "END",
          "shortestPath", "allShortestPaths")

    for (keyword <- keywords) {
      assertSameAST(s"WITH $$$keyword AS x RETURN x AS $keyword")
    }
  }

  test("allow backslash in escaped symbolic name") {
    assertSameAST(
    """MATCH (`This isn\'t a common variable`)
      |WHERE `This isn\'t a common variable`.name = 'A'
      |RETURN `This isn\'t a common variable`.happy""".stripMargin)
  }

  // extra spaces tests

  private def assertSameASTWithExtraSpaces(query: String) = {
    assertSameAST(query.replaceAll(" ", " "*2))
    assertSameAST(query.replaceAll(" ", "\n"))
  }

  test("MERGE (n) ON CREATE SET n.x = 123") {
    assertSameASTWithExtraSpaces(testName)
  }

  test("MERGE (n) ON MATCH SET n.x = 123") {
    assertSameASTWithExtraSpaces(testName)
  }

  test("LOAD CSV FROM 'url' AS line") {
    assertSameASTWithExtraSpaces(testName)
  }

  test("USING PERIODIC COMMIT LOAD CSV FROM 'url' AS line RETURN line") {
    assertSameASTWithExtraSpaces(testName)
  }

  test("USING PERIODIC COMMIT LOAD CSV FROM 'url' AS line") {
    assertSameASTWithExtraSpaces(testName)
  }

  test("RETURN 1 AS x ORDER BY x") {
    assertSameASTWithExtraSpaces(testName)
  }

  test("RETURN 1 AS x UNION ALL RETURN 2 AS x") {
    assertSameASTWithExtraSpaces(testName)
  }

  test("MATCH (a)--(b)--(c) USING JOIN ON b RETURN a,b,c") {
    assertSameASTWithExtraSpaces(testName)
  }

  // schema commands are not supported
  ignore("CREATE CONSTRAINT ON (node:Label) ASSERT (node.prop) IS NODE KEY") {
    assertSameASTWithExtraSpaces(testName)
  }

  // schema commands are not supported
  ignore("CREATE CONSTRAINT ON (node:Label) ASSERT (node.prop) IS UNIQUE") {
    assertSameASTWithExtraSpaces(testName)
  }

  test("MATCH (n) WHERE n.name STARTS WITH 'hello'") {
    assertSameASTWithExtraSpaces(testName)
  }

  test("MATCH (n) WHERE n.name ENDS WITH 'hello'") {
    assertSameASTWithExtraSpaces(testName)
  }

  test("MATCH (n) WHERE n.name IS NULL") {
    assertSameASTWithExtraSpaces(testName)
  }

  test("MATCH (n) WHERE n.name IS NOT NULL") {
    assertSameASTWithExtraSpaces(testName)
  }
}
