/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler

import org.neo4j.cypher.internal.PreParserOption
import org.neo4j.cypher.internal.ast.factory.neo4j.Neo4jASTExceptionFactory
import org.neo4j.cypher.internal.options.CypherExecutionMode
import org.neo4j.cypher.internal.preparser.javacc.CypherPreParser
import org.neo4j.cypher.internal.preparser.javacc.PreParserCharStream
import org.neo4j.cypher.internal.preparser.javacc.PreParserResult
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Neo4jCypherExceptionFactory
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.prop.TableFor2

import java.util

import scala.language.implicitConversions

class CypherPreParserTest extends CypherFunSuite with TableDrivenPropertyChecks {

  private def mode(mode: String, position: InputPosition) = PreParserOption(CypherExecutionMode.name, mode, position)
  private def opt(key: String, value: String, position: InputPosition) = PreParserOption(key, value, position)

  val queries: TableFor2[String, PreParserResult] = Table(
    ("query", "expected"),
    ("RETURN 1 / 0.5 as number", new PreParserResult(util.List.of[PreParserOption](), (1, 1, 0))),
    ("RETURN .1e9 AS literal", new PreParserResult(util.List.of[PreParserOption](), (1, 1, 0))),
    ("RETURN '\\uH'", new PreParserResult(util.List.of[PreParserOption](), (1, 1, 0))),
    ("PROFILE MATCH", new PreParserResult(util.List.of[PreParserOption](mode("PROFILE", (1, 1, 0))), (1, 9, 8))),
    ("EXPLAIN MATCH", new PreParserResult(util.List.of[PreParserOption](mode("EXPLAIN", (1, 1, 0))), (1, 9, 8))),
    ("CYPHER WITH YALL", new PreParserResult(util.List.of[PreParserOption](), (1, 8, 7))),
    (
      "CYPHER planner=cost RETURN",
      new PreParserResult(util.List.of[PreParserOption](opt("planner", "cost", (1, 8, 7))), (1, 21, 20))
    ),
    (
      "CYPHER planner = idp RETURN",
      new PreParserResult(util.List.of[PreParserOption](opt("planner", "idp", (1, 8, 7))), (1, 22, 21))
    ),
    (
      "CYPHER planner =dp RETURN",
      new PreParserResult(util.List.of[PreParserOption](opt("planner", "dp", (1, 8, 7))), (1, 20, 19))
    ),
    (
      "CYPHER runtime=interpreted RETURN",
      new PreParserResult(util.List.of[PreParserOption](opt("runtime", "interpreted", (1, 8, 7))), (1, 28, 27))
    ),
    (
      "CYPHER planner=cost runtime=interpreted RETURN",
      new PreParserResult(
        util.List.of[PreParserOption](opt("planner", "cost", (1, 8, 7)), opt("runtime", "interpreted", (1, 21, 20))),
        (1, 41, 40)
      )
    ),
    (
      "CYPHER planner=dp runtime=interpreted RETURN",
      new PreParserResult(
        util.List.of[PreParserOption](opt("planner", "dp", (1, 8, 7)), opt("runtime", "interpreted", (1, 19, 18))),
        (1, 39, 38)
      )
    ),
    (
      "CYPHER planner=idp runtime=interpreted RETURN",
      new PreParserResult(
        util.List.of[PreParserOption](opt("planner", "idp", (1, 8, 7)), opt("runtime", "interpreted", (1, 20, 19))),
        (1, 40, 39)
      )
    ),
    (
      "CYPHER planner=idp planner=dp runtime=interpreted RETURN",
      new PreParserResult(
        util.List.of[PreParserOption](
          opt("planner", "idp", (1, 8, 7)),
          opt("planner", "dp", (1, 20, 19)),
          opt("runtime", "interpreted", (1, 31, 30))
        ),
        (1, 51, 50)
      )
    ),
    (
      "CYPHER updateStrategy=eager RETURN",
      new PreParserResult(util.List.of[PreParserOption](opt("updateStrategy", "eager", (1, 8, 7))), (1, 29, 28))
    ),
    (
      "CYPHER runtime=slotted RETURN",
      new PreParserResult(util.List.of[PreParserOption](opt("runtime", "slotted", (1, 8, 7))), (1, 24, 23))
    ),
    (
      "CYPHER expressionEngine=interpreted RETURN",
      new PreParserResult(util.List.of[PreParserOption](opt("expressionEngine", "interpreted", (1, 8, 7))), (1, 37, 36))
    ),
    (
      "CYPHER expressionEngine=compiled RETURN",
      new PreParserResult(util.List.of[PreParserOption](opt("expressionEngine", "compiled", (1, 8, 7))), (1, 34, 33))
    ),
    (
      "CYPHER replan=force RETURN",
      new PreParserResult(util.List.of[PreParserOption](opt("replan", "force", (1, 8, 7))), (1, 21, 20))
    ),
    (
      "CYPHER replan=skip RETURN",
      new PreParserResult(util.List.of[PreParserOption](opt("replan", "skip", (1, 8, 7))), (1, 20, 19))
    ),
    (
      "CYPHER planner=cost MATCH(n:Node) WHERE n.prop = 3 RETURN n",
      new PreParserResult(util.List.of[PreParserOption](opt("planner", "cost", (1, 8, 7))), (1, 21, 20))
    ),
    ("CREATE ({name: 'USING PERIODIC COMMIT'})", new PreParserResult(util.List.of[PreParserOption](), (1, 1, 0))),
    (
      "match (c:CYPHER) WITH c as debug, 'profile' as explain, RETURN debug, explain",
      new PreParserResult(util.List.of[PreParserOption](), (1, 1, 0))
    ),
    (
      "match (runtime:C) WITH 'string' as slotted WHERE runtime=slotted RETURN runtime",
      new PreParserResult(util.List.of[PreParserOption](), (1, 1, 0))
    ),
    (
      "EXPLAIN LOAD CSV WITH HEADERS FROM 'file:///fake.csv' AS row CREATE ()-[:T]->()",
      new PreParserResult(util.List.of[PreParserOption](mode("EXPLAIN", (1, 1, 0))), (1, 9, 8))
    ),
    (
      "//TESTING \n //TESTING \n EXPLAIN MATCH (n) //TESTING\n MATCH (b:X) return n,b Limit 1",
      new PreParserResult(util.List.of[PreParserOption](mode("EXPLAIN", (3, 2, 24))), (3, 10, 32))
    ),
    (
      " EXPLAIN MATCH (n) RETURN",
      new PreParserResult(util.List.of[PreParserOption](mode("EXPLAIN", (1, 2, 1))), (1, 10, 9))
    ),
    (
      " /* Some \n comment */ EXPLAIN MATCH (n) RETURN /* Some \n comment */ n",
      new PreParserResult(util.List.of[PreParserOption](mode("EXPLAIN", (2, 13, 22))), (2, 21, 30))
    ),
    (
      "EXPLAIN /* Some \n comment */ MATCH (n) RETURN /* Some \n comment */ n",
      new PreParserResult(util.List.of[PreParserOption](mode("EXPLAIN", (1, 1, 0))), (2, 13, 29))
    ),
    (
      "//TESTING \n //TESTING \n EXPLAIN MATCH (n)\n MATCH (b:X) return n,b Limit 1",
      new PreParserResult(util.List.of[PreParserOption](mode("EXPLAIN", (3, 2, 24))), (3, 10, 32))
    ),
    (
      " EXPLAIN/* 2 */ // \n  query",
      new PreParserResult(util.List.of[PreParserOption](mode("EXPLAIN", (1, 2, 1))), (2, 3, 22))
    ),
    (
      "CYPHER // \n a // \n = b query",
      new PreParserResult(util.List.of[PreParserOption](opt("a", "b", (2, 2, 12))), (3, 6, 23))
    ),
    (
      " /* 1 */ EXPLAIN CYPHER\n planner /* 2 */ // \n =  /** 3 */ // \n cost MATCH /* 4 */ s ",
      new PreParserResult(
        util.List.of[PreParserOption](mode("EXPLAIN", (1, 10, 9)), opt("planner", "cost", (2, 2, 25))),
        (4, 7, 68)
      )
    )
  )

  test("run the tests") {
    forAll(queries) {
      case (query, expected) =>
        withClue(s"Failed on query: $query\n") {
          parse(query) should equal(expected)
        }
    }
  }

  private def parse(queryText: String): PreParserResult = {
    new CypherPreParser(
      new Neo4jASTExceptionFactory(Neo4jCypherExceptionFactory(queryText, None)),
      new PreParserCharStream(queryText)
    ).parse()
  }

  implicit private def lift(pos: (Int, Int, Int)): InputPosition = InputPosition(pos._3, pos._1, pos._2)
}
