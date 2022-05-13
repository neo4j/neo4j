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
package org.neo4j.cypher.internal.compiler

import org.neo4j.cypher.internal.PreParserOption
import org.neo4j.cypher.internal.ast.factory.neo4j.Neo4jASTExceptionFactory
import org.neo4j.cypher.internal.options.CypherExecutionMode
import org.neo4j.cypher.internal.options.CypherVersion
import org.neo4j.cypher.internal.preparser.javacc.CypherPreParser
import org.neo4j.cypher.internal.preparser.javacc.PreParserCharStream
import org.neo4j.cypher.internal.preparser.javacc.PreParserResult
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.prop.TableFor2

import java.util.List

import scala.language.implicitConversions

class CypherPreParserTest extends CypherFunSuite with TableDrivenPropertyChecks {

  private def version(ver: String) = PreParserOption(CypherVersion.name, ver)
  private def mode(mode: String) = PreParserOption(CypherExecutionMode.name, mode)
  private def opt(key: String, value: String) = PreParserOption(key, value)

  val queries: TableFor2[String, PreParserResult] = Table(
    ("query", "expected"),
    ("RETURN 1 / 0.5 as number", new PreParserResult(List.of[PreParserOption](), (1, 1, 0))),
    ("RETURN .1e9 AS literal", new PreParserResult(List.of[PreParserOption](), (1, 1, 0))),
    ("RETURN '\\uH'", new PreParserResult(List.of[PreParserOption](), (1, 1, 0))),
    ("CYPHER 4.1 MATCH something", new PreParserResult(List.of[PreParserOption](version("4.1")), (1, 12, 11))),
    ("PROFILE MATCH", new PreParserResult(List.of[PreParserOption](mode("PROFILE")), (1, 9, 8))),
    ("EXPLAIN MATCH", new PreParserResult(List.of[PreParserOption](mode("EXPLAIN")), (1, 9, 8))),
    ("CYPHER WITH YALL", new PreParserResult(List.of[PreParserOption](), (1, 8, 7))),
    (
      "EXPLAIN CYPHER 4.1 WITH YALL",
      new PreParserResult(List.of[PreParserOption](mode("EXPLAIN"), version("4.1")), (1, 20, 19))
    ),
    (
      "EXPLAIN CYPHER 4.1 $WITH YALL",
      new PreParserResult(List.of[PreParserOption](mode("EXPLAIN"), version("4.1")), (1, 20, 19))
    ),
    ("CYPHER planner=cost RETURN", new PreParserResult(List.of[PreParserOption](opt("planner", "cost")), (1, 21, 20))),
    (
      "CYPHER 4.1 planner=cost RETURN",
      new PreParserResult(List.of[PreParserOption](version("4.1"), opt("planner", "cost")), (1, 25, 24))
    ),
    (
      "CYPHER 4.1 planner = idp RETURN",
      new PreParserResult(List.of[PreParserOption](version("4.1"), opt("planner", "idp")), (1, 26, 25))
    ),
    ("CYPHER planner =dp RETURN", new PreParserResult(List.of[PreParserOption](opt("planner", "dp")), (1, 20, 19))),
    (
      "CYPHER runtime=interpreted RETURN",
      new PreParserResult(List.of[PreParserOption](opt("runtime", "interpreted")), (1, 28, 27))
    ),
    (
      "CYPHER 4.1 planner=cost runtime=interpreted RETURN",
      new PreParserResult(
        List.of[PreParserOption](version("4.1"), opt("planner", "cost"), opt("runtime", "interpreted")),
        (1, 45, 44)
      )
    ),
    (
      "CYPHER 4.1 planner=dp runtime=interpreted RETURN",
      new PreParserResult(
        List.of[PreParserOption](version("4.1"), opt("planner", "dp"), opt("runtime", "interpreted")),
        (1, 43, 42)
      )
    ),
    (
      "CYPHER 4.1 planner=idp runtime=interpreted RETURN",
      new PreParserResult(
        List.of[PreParserOption](version("4.1"), opt("planner", "idp"), opt("runtime", "interpreted")),
        (1, 44, 43)
      )
    ),
    (
      "CYPHER 4.1 planner=idp planner=dp runtime=interpreted RETURN",
      new PreParserResult(
        List.of[PreParserOption](
          version("4.1"),
          opt("planner", "idp"),
          opt("planner", "dp"),
          opt("runtime", "interpreted")
        ),
        (1, 55, 54)
      )
    ),
    (
      "CYPHER updateStrategy=eager RETURN",
      new PreParserResult(List.of[PreParserOption](opt("updateStrategy", "eager")), (1, 29, 28))
    ),
    (
      "CYPHER runtime=slotted RETURN",
      new PreParserResult(List.of[PreParserOption](opt("runtime", "slotted")), (1, 24, 23))
    ),
    (
      "CYPHER expressionEngine=interpreted RETURN",
      new PreParserResult(List.of[PreParserOption](opt("expressionEngine", "interpreted")), (1, 37, 36))
    ),
    (
      "CYPHER expressionEngine=compiled RETURN",
      new PreParserResult(List.of[PreParserOption](opt("expressionEngine", "compiled")), (1, 34, 33))
    ),
    ("CYPHER replan=force RETURN", new PreParserResult(List.of[PreParserOption](opt("replan", "force")), (1, 21, 20))),
    ("CYPHER replan=skip RETURN", new PreParserResult(List.of[PreParserOption](opt("replan", "skip")), (1, 20, 19))),
    (
      "CYPHER 4.1 planner=cost MATCH(n:Node) WHERE n.prop = 3 RETURN n",
      new PreParserResult(List.of[PreParserOption](version("4.1"), opt("planner", "cost")), (1, 25, 24))
    ),
    ("CREATE ({name: 'USING PERIODIC COMMIT'})", new PreParserResult(List.of[PreParserOption](), (1, 1, 0))),
    (
      "match (c:CYPHER) WITH c as debug, 'profile' as explain, RETURN debug, explain",
      new PreParserResult(List.of[PreParserOption](), (1, 1, 0))
    ),
    (
      "match (runtime:C) WITH 'string' as slotted WHERE runtime=slotted RETURN runtime",
      new PreParserResult(List.of[PreParserOption](), (1, 1, 0))
    ),
    (
      "EXPLAIN LOAD CSV WITH HEADERS FROM 'file:///fake.csv' AS row CREATE ()-[:T]->()",
      new PreParserResult(List.of[PreParserOption](mode("EXPLAIN")), (1, 9, 8))
    ),
    (
      "//TESTING \n //TESTING \n EXPLAIN MATCH (n) //TESTING\n MATCH (b:X) return n,b Limit 1",
      new PreParserResult(List.of[PreParserOption](mode("EXPLAIN")), (3, 10, 32))
    ),
    (
      " EXPLAIN CYPHER 4.1 MATCH (n) RETURN",
      new PreParserResult(List.of[PreParserOption](mode("EXPLAIN"), version("4.1")), (1, 21, 20))
    ),
    (
      " /* Some \n comment */ EXPLAIN CYPHER 4.1 MATCH (n) RETURN /* Some \n comment */ n",
      new PreParserResult(List.of[PreParserOption](mode("EXPLAIN"), version("4.1")), (2, 32, 41))
    ),
    (
      "EXPLAIN /* Some \n comment */ CYPHER 4.1 MATCH (n) RETURN /* Some \n comment */ n",
      new PreParserResult(List.of[PreParserOption](mode("EXPLAIN"), version("4.1")), (2, 24, 40))
    ),
    (
      "CYPHER 3.5 //TESTING \n //TESTING \n EXPLAIN MATCH (n)\n MATCH (b:X) return n,b Limit 1",
      new PreParserResult(List.of[PreParserOption](version("3.5"), mode("EXPLAIN")), (3, 10, 43))
    ),
    (" EXPLAIN/* 2 */ // \n  query", new PreParserResult(List.of[PreParserOption](mode("EXPLAIN")), (2, 3, 22))),
    ("CYPHER // \n a // \n = b query", new PreParserResult(List.of[PreParserOption](opt("a", "b")), (3, 6, 23))),
    (
      " /* 1 */ EXPLAIN CYPHER\n planner /* 2 */ // \n =  /** 3 */ // \n cost MATCH /* 4 */ s ",
      new PreParserResult(List.of[PreParserOption](mode("EXPLAIN"), opt("planner", "cost")), (4, 7, 68))
    )
  )

  test("run the tests") {
    forAll(queries) {
      case (query, expected) =>
        parse(query) should equal(expected)
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
