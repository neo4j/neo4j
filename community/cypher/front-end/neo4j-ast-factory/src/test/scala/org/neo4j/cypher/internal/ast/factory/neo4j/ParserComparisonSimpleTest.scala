/*
 * Copyright (c) "Neo4j"
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

class ParserComparisonSimpleTest extends ParserComparisonTestBase with FunSuiteLike with TestName {

  test("RETURN 1 AS x") {
    assertSameAST(testName)
  }

  test("RETURN 'apoks\\tf\\np' AS x") {
    assertSameAST(testName)
  }

  test("RETURN \"apoks\\tf\\np\" AS x") {
    assertSameAST(testName)
  }

  test("RETURN special characters") {
    assertSameAST("RETURN '\r\n\t\b\f'")
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
      Seq("TRUE", "FALSE", "NULL", "RETURN", "CREATE", "DELETE", "SET", "REMOVE", "DETACH", "MATCH", "WITH",
          "UNWIND", "USE", "GRAPH", "CALL", "YIELD", "LOAD", "CSV", "PERIODIC", "COMMIT",
          "HEADERS", "FROM", "FIELDTERMINATOR", "FOREACH", "WHERE", "DISTINCT", "MERGE",
          "OPTIONAL", "USING", "ORDER", "BY", "ASC", "ASCENDING", "DESC", "DESCENDING",
          "SKIP", "LIMIT", "UNION", "DROP", "INDEX", "SEEK", "SCAN", "JOIN", "CONSTRAINT",
          "ASSERT", "IS", "NODE", "KEY", "UNIQUE", "ON", "AS", "OR", "XOR", "AND", "NOT",
          "STARTS", "ENDS", "CONTAINS", "IN", "count", "FILTER", "EXTRACT", "REDUCE", "ROW", "ROWS",
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

  test("allow escaped symbolic name") {
    // The parsed label should be Label
    assertSameAST("CREATE (n:`Label`)")
  }

  test("allow escaped backticks inside escaped simple symbolic name") {
    assertSameAST("RETURN 1 AS `a``b`")
  }

  test("allow escaped backticks inside escaped token symbolic name") {
    // The parsed label should be Label`123
    assertSameAST("CREATE (n:`Label``123`)")
  }

  test("allow escaped backticks first and last in escaped symbolic name") {
    // The parsed label should be ``Label`
    assertSameAST("CREATE (n:`````Label```)")
  }

  test("disallow unescaped backticks in escaped symbolic name") {
    // Should throw error on same position in both parsers
    assertSameAST("CREATE (n:`L`abel`)")
  }

  test("MATCH (n) WITH {node:n} AS map SET map.node.property = 123") {
    assertSameAST(testName)
  }

  test("MATCH (n) WITH {node:n} AS map REMOVE map.node.property") {
    assertSameAST(testName)
  }

  test("RETURN [x = '1']") {
    assertSameAST(testName)
  }

  test("RETURN [x = ()--()]") {
    assertSameAST(testName)
  }

  test("RETURN [x = ()--()--()--()--()--()--()--()--()--()--()]") {
    assertSameAST(testName)
  }

  test("RETURN [p = (x) | p]") {
    assertSameAST(testName)
  }

  test("CREATE (:True)") {
    assertSameAST(testName)
  }

  test("CREATE (:False)") {
    assertSameAST(testName)
  }

  test("CREATE (t:True)") {
    assertSameAST(testName)
  }

  test("CREATE (f:False)") {
    assertSameAST(testName)
  }

  test("MATCH (:True) RETURN 1 AS one") {
    assertSameAST(testName)
  }

  test("MATCH (:False) RETURN 1 AS one") {
    assertSameAST(testName)
  }

  test("MATCH (t:True) RETURN t") {
    assertSameAST(testName)
  }

  test("MATCH (f:False) RETURN f") {
    assertSameAST(testName)
  }

  test("MATCH ()-[:Person*1..2]-(f) RETURN f") {
    assertSameAST(testName)
  }

  // extra spaces tests

  private def assertSameASTWithExtraSpaces(query: String): Unit = {
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

  test("USING PERIODIC COMMIT USE db LOAD CSV FROM 'url' AS line RETURN line") {
    assertSameAST(testName)
  }

  test("USE GRAPH db USING PERIODIC COMMIT LOAD CSV FROM 'url' AS line RETURN line") {
    assertJavaCCExceptionStart(testName, "Invalid input 'USING'")
  }

  test("MATCH (n:NODE) WITH true as n.prop RETURN true OR FALSE") {
    assertSameAST(testName)
  }

  test("MATCH (n)") {
    assertSameAST(testName)
  }

  test("CALL { USE neo4j RETURN 1 AS y } RETURN y") {
    assertSameAST(testName)
  }

  test("WITH 1 AS x CALL { WITH x USE neo4j RETURN x AS y } RETURN x, y") {
    assertSameAST(testName)
  }

  test("RETURN 0.0d as double") {
    assertSameAST(testName)
  }

  test("RETURN .0d as double") {
    assertSameAST(testName)
  }

  test("RETURN 1e0d as double") {
    assertSameAST(testName)
  }

  test("RETURN 0.0f as float") {
    assertSameAST(testName)
  }

  test("RETURN 0.0somegibberish as double") {
    assertSameAST(testName)
  }

  test("RETURN 0.0 as double") {
    assertSameAST(testName)
  }

  test("RETURN NaN as double") {
    assertSameAST(testName)
  }

  test("RETURN Infinity as double") {
    assertSameAST(testName)
  }

  test("RETURN - 1.4 as double") {
    //java cc is whitespace ignorant
    assertSameASTForQueries(testName, "RETURN -1.4 as double", comparePosition = false)
  }

  test("RETURN Ox as literal") {
    assertSameAST(testName)
  }

  test("RETURN 01 AS literal") {
    assertSameAST(testName)
  }

  test("RETURN 0o1 AS literal") {
    assertSameAST(testName)
  }

  test("MATCH (n) WITH CASE WHEN (e) THEN e ELSE null END as e RETURN e") {
    assertSameAST(testName)
  }

  test("MATCH (n) WITH CASE when(e) WHEN (e) THEN e ELSE null END as e RETURN e") {
    assertSameAST(testName)
  }

  test("MATCH (n) WITH CASE when(v1) + 1 WHEN THEN v2 ELSE null END as e RETURN e") {
    assertSameAST(testName)
  }

  //parameter number boundaries

  test("RETURN $1_2") {
    assertSameAST(testName)
  }

  test("RETURN $0_2") {
    //this fails on both parsers because it starts with 0 -> which is not a UnsignedDecimalInteger (but an Octal?)
    assertJavaCCException(testName, "Invalid input '$': expected \"+\" or \"-\" (line 1, column 8 (offset: 7))")
  }

  test("RETURN $0") {
    //0, on the other hand, is an UnsignedDecimalInteger
    assertSameAST(testName)
  }

  test("RETURN 0_2 as Literal") {
    assertSameAST(testName)
  }

  test("RETURN $1") {
    assertSameAST(testName)
  }

  test("RETURN $1.0f") {
    assertJavaCCException(testName, "Invalid input '$': expected \"+\" or \"-\" (line 1, column 8 (offset: 7))" )
  }

  test("RETURN $1gibberish") {
    assertSameAST(testName)
  }

  test("RETURN 2*(2.0-1.5)") {
    assertSameAST(testName)
  }

  test("RETURN +1.5") {
    assertSameAST(testName)
  }

  test("RETURN +1") {
    assertSameAST(testName)
  }

  test("RETURN 2*(2.0 - +1.5)") {
    assertSameAST(testName)
  }

  test("RETURN 0-1") {
    assertSameAST(testName)
  }

  test("RETURN 0-0.1") {
    assertSameAST(testName)
  }

  test("USE foo UNION ALL RETURN 1") {
    assertSameAST(testName)
  }

  test("MATCH (n WHERE n.prop > 123)") {
    assertSameAST(testName)
  }

  test("MATCH (n wHeRe n.prop > 123)") {
    assertSameAST(testName)
  }

  test("MATCH (n:A:B:C {prop: 42} WHERE n.otherProp < 123)") {
    assertSameAST(testName)
  }

  test("MATCH (WHERE WHERE WHERE.prop > 123)") {
    assertSameAST(testName)
  }

  test("RETURN [(n:A WHERE n.prop >= 123)-->(end WHERE end.prop < 42) | n]") {
    assertSameAST(testName)
  }

  test("RETURN exists((n {prop: 'test'} WHERE n.otherProp = 123)-->(end WHERE end.prop = 42)) AS result") {
    assertSameAST(testName)
  }

  test("MATCH (WHERE {prop: 123})") {
    assertSameAST(testName)
  }

  test("MATCH (:Label {prop: 123} WHERE 2 > 1)") {
    assertJavaCCExceptionStart(testName, "Invalid input 'WHERE'")
    assertSameAST(testName)
  }
}
