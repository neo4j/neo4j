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

import org.neo4j.cypher.internal.PreParsedStatement
import org.neo4j.cypher.internal.PreParserOption
import org.neo4j.cypher.internal.ast.factory.neo4j.Neo4jASTExceptionFactory
import org.neo4j.cypher.internal.options.CypherExecutionMode
import org.neo4j.cypher.internal.options.CypherVersion
import org.neo4j.cypher.internal.preparser.javacc.CypherPreParser
import org.neo4j.cypher.internal.preparser.javacc.PreParserCharStream
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.prop.TableFor2

import scala.collection.JavaConverters.asScalaBufferConverter

class CypherPreParserTest extends CypherFunSuite with TableDrivenPropertyChecks {

  private def version(ver: String) = PreParserOption(CypherVersion.name, ver)
  private def mode(mode: String) = PreParserOption(CypherExecutionMode.name, mode)
  private def opt(key: String, value: String) = PreParserOption(key, value)

  val queries: TableFor2[String, PreParsedStatement] = Table(
    ("query", "expected"),
    ("RETURN 1 / 0.5 as number", PreParsedStatement("RETURN 1 / 0.5 as number", List(), (1, 1, 0))),
    ("RETURN .1e9 AS literal", PreParsedStatement("RETURN .1e9 AS literal", List(), (1, 1, 0))),
    ("RETURN '\\uH'", PreParsedStatement("RETURN '\\uH'", List(), (1, 1, 0))),
    ("CYPHER 4.1 MATCH something", PreParsedStatement("MATCH something", List(version("4.1")), (1, 12, 11))),
    ("PROFILE MATCH", PreParsedStatement("MATCH", List(mode("PROFILE")), (1, 9, 8))),
    ("EXPLAIN MATCH", PreParsedStatement("MATCH", List(mode("EXPLAIN")), (1, 9, 8))),
    ("CYPHER WITH YALL", PreParsedStatement("WITH YALL", List(), (1, 8, 7))),
    ("EXPLAIN CYPHER 4.1 WITH YALL", PreParsedStatement("WITH YALL", List(mode("EXPLAIN"), version("4.1")), (1, 20, 19))),
    ("EXPLAIN CYPHER 4.1 $WITH YALL", PreParsedStatement("$WITH YALL", List(mode("EXPLAIN"), version("4.1")), (1, 20, 19))),
    ("CYPHER planner=cost RETURN", PreParsedStatement("RETURN", List(opt("planner", "cost")), (1, 21, 20))),
    ("CYPHER 4.1 planner=cost RETURN", PreParsedStatement("RETURN", List(version("4.1"), opt("planner", "cost")), (1, 25, 24))),
    ("CYPHER 4.1 planner = idp RETURN", PreParsedStatement("RETURN", List(version("4.1"), opt("planner", "idp")), (1, 26, 25))),
    ("CYPHER planner =dp RETURN", PreParsedStatement("RETURN", List(opt("planner", "dp")), (1, 20, 19))),
    ("CYPHER runtime=interpreted RETURN", PreParsedStatement("RETURN", List(opt("runtime", "interpreted")), (1, 28, 27))),
    ("CYPHER 4.1 planner=cost runtime=interpreted RETURN", PreParsedStatement("RETURN", List(version("4.1"), opt("planner", "cost"), opt("runtime", "interpreted")), (1, 45, 44))),
    ("CYPHER 4.1 planner=dp runtime=interpreted RETURN", PreParsedStatement("RETURN", List(version("4.1"), opt("planner", "dp"), opt("runtime", "interpreted")), (1, 43, 42))),
    ("CYPHER 4.1 planner=idp runtime=interpreted RETURN", PreParsedStatement("RETURN", List(version("4.1"), opt("planner", "idp"), opt("runtime", "interpreted")), (1, 44, 43))),
    ("CYPHER 4.1 planner=idp planner=dp runtime=interpreted RETURN", PreParsedStatement("RETURN", List(version("4.1"), opt("planner", "idp"), opt("planner", "dp"), opt("runtime", "interpreted")), (1, 55, 54))),
    ("CYPHER updateStrategy=eager RETURN", PreParsedStatement("RETURN", List(opt("updateStrategy", "eager")), (1, 29, 28))),
    ("CYPHER runtime=slotted RETURN", PreParsedStatement("RETURN", List(opt("runtime", "slotted")), (1, 24, 23))),
    ("CYPHER expressionEngine=interpreted RETURN", PreParsedStatement("RETURN", List(opt("expressionEngine", "interpreted")), (1, 37, 36))),
    ("CYPHER expressionEngine=compiled RETURN", PreParsedStatement("RETURN", List(opt("expressionEngine", "compiled")), (1, 34, 33))),
    ("CYPHER replan=force RETURN", PreParsedStatement("RETURN", List(opt("replan", "force")), (1, 21, 20))),
    ("CYPHER replan=skip RETURN", PreParsedStatement("RETURN", List(opt("replan", "skip")), (1, 20, 19))),
    ("CYPHER 4.1 planner=cost MATCH(n:Node) WHERE n.prop = 3 RETURN n",
      PreParsedStatement("MATCH(n:Node) WHERE n.prop = 3 RETURN n", List(version("4.1"), opt("planner", "cost")), (1, 25, 24))),
    ("CREATE ({name: 'USING PERIODIC COMMIT'})", PreParsedStatement("CREATE ({name: 'USING PERIODIC COMMIT'})", List(), (1, 1, 0))),
    ("match (c:CYPHER) WITH c as debug, 'profile' as explain, RETURN debug, explain",
      PreParsedStatement("match (c:CYPHER) WITH c as debug, 'profile' as explain, RETURN debug, explain", List(), (1, 1, 0))),
    ("match (runtime:C) WITH 'string' as slotted WHERE runtime=slotted RETURN runtime",
        PreParsedStatement("match (runtime:C) WITH 'string' as slotted WHERE runtime=slotted RETURN runtime", List(), (1, 1, 0))),
    ("EXPLAIN LOAD CSV WITH HEADERS FROM 'file:///fake.csv' AS row CREATE ()-[:T]->()",
      PreParsedStatement("LOAD CSV WITH HEADERS FROM 'file:///fake.csv' AS row CREATE ()-[:T]->()", List(mode("EXPLAIN")), (1, 9, 8))),
    ("//TESTING \n //TESTING \n EXPLAIN MATCH (n) //TESTING\n MATCH (b:X) return n,b Limit 1",
      PreParsedStatement("MATCH (n) //TESTING\n MATCH (b:X) return n,b Limit 1", List(mode("EXPLAIN")), (3, 10, 32))),
    (" EXPLAIN CYPHER 4.1 MATCH (n) RETURN", PreParsedStatement("MATCH (n) RETURN", List(mode("EXPLAIN"), version("4.1")), (1, 21, 20))),
    (" /* Some \n comment */ EXPLAIN CYPHER 4.1 MATCH (n) RETURN /* Some \n comment */ n",
      PreParsedStatement("MATCH (n) RETURN /* Some \n comment */ n", List(mode("EXPLAIN"), version("4.1")), (2, 32, 41))),
    ("EXPLAIN /* Some \n comment */ CYPHER 4.1 MATCH (n) RETURN /* Some \n comment */ n",
      PreParsedStatement("MATCH (n) RETURN /* Some \n comment */ n", List(mode("EXPLAIN"), version("4.1")), (2, 24, 40))),
    ("CYPHER 3.5 //TESTING \n //TESTING \n EXPLAIN MATCH (n)\n MATCH (b:X) return n,b Limit 1",
      PreParsedStatement("MATCH (n)\n MATCH (b:X) return n,b Limit 1", List(version("3.5"), mode("EXPLAIN")), (3, 10, 43))),
    (" EXPLAIN/* 2 */ // \n  query",
      PreParsedStatement("query", List(mode("EXPLAIN")), (2, 3, 22))),
    ("CYPHER // \n a // \n = b query",
      PreParsedStatement("query", List(opt("a", "b")), (3, 6, 23))),
    (" /* 1 */ EXPLAIN CYPHER\n planner /* 2 */ // \n =  /** 3 */ // \n cost MATCH /* 4 */ s ",
      PreParsedStatement("MATCH /* 4 */ s ", List(mode("EXPLAIN"), opt("planner", "cost")), (4, 7, 68))),
  )

  test("run the tests") {
    forAll(queries) {
      case (query, expected) =>
        parse(query) should equal(expected)
    }
  }

  private def parse(queryText: String): PreParsedStatement = {
    val preParserResult = new CypherPreParser(new Neo4jASTExceptionFactory(Neo4jCypherExceptionFactory(queryText, None)), new PreParserCharStream(queryText)).
      parse()
    PreParsedStatement(queryText.substring(preParserResult.position().offset), preParserResult.options.asScala.toList, preParserResult.position())
  }

  private implicit def lift(pos: (Int, Int, Int)): InputPosition = InputPosition(pos._3, pos._1, pos._2)
}
