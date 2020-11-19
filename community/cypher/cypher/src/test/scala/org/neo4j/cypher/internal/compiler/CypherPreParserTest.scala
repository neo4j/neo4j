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
package org.neo4j.cypher.internal.compiler

import org.neo4j.cypher.internal.CypherPreParser
import org.neo4j.cypher.internal.PreParsedStatement
import org.neo4j.cypher.internal.PreParserOption
import org.neo4j.cypher.internal.options.CypherExecutionMode
import org.neo4j.cypher.internal.options.CypherVersion
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.prop.TableFor2

class CypherPreParserTest extends CypherFunSuite with TableDrivenPropertyChecks {

  private def version(ver: String) = PreParserOption(CypherVersion.name, ver)
  private def mode(mode: String) = PreParserOption(CypherExecutionMode.name, mode)
  private def opt(key: String, value: String) = PreParserOption(key, value)

  val queries: TableFor2[String, PreParsedStatement] = Table(
    ("query", "expected"),
    ("CYPHER 4.1 PRO", PreParsedStatement("PRO", List(version("4.1")), (1, 12, 11))),
    ("PROFILE THINGS", PreParsedStatement("THINGS", List(mode("PROFILE")), (1, 9, 8))),
    ("EXPLAIN THIS", PreParsedStatement("THIS", List(mode("EXPLAIN")), (1, 9, 8))),
    ("EXPLAIN CYPHER 4.1 YALL", PreParsedStatement("YALL", List(mode("EXPLAIN"), version("4.1")), (1, 20, 19))),
    ("CYPHER planner=cost RETURN", PreParsedStatement("RETURN", List(opt("planner", "cost")), (1, 21, 20))),
    ("CYPHER 4.1 planner=cost RETURN", PreParsedStatement("RETURN", List(version("4.1"), opt("planner", "cost")), (1, 25, 24))),
    ("CYPHER 4.1 planner = idp RETURN", PreParsedStatement("RETURN", List(version("4.1"), opt("planner", "idp")), (1, 26, 25))),
    ("CYPHER planner =dp RETURN", PreParsedStatement("RETURN", List(opt("planner", "dp")), (1, 20, 19))),
    ("CYPHER runtime=interpreted RETURN", PreParsedStatement("RETURN", List(opt("runtime", "interpreted")), (1, 28, 27))),
    ("CYPHER 4.1 planner=cost runtime=interpreted RETURN", PreParsedStatement("RETURN", List(version("4.1"), opt("planner", "cost"), opt("runtime", "interpreted")), (1, 45, 44))),
    ("CYPHER 4.1 planner=dp runtime=interpreted RETURN", PreParsedStatement("RETURN", List(version("4.1"), opt("planner", "dp"), opt("runtime", "interpreted")), (1, 43, 42))),
    ("CYPHER 4.1 planner=idp runtime=interpreted RETURN", PreParsedStatement("RETURN", List(version("4.1"), opt("planner", "idp"), opt("runtime", "interpreted")), (1, 44, 43))),
    ("CYPHER 4.1 planner=idp planner=dp runtime=interpreted RETURN", PreParsedStatement("RETURN", List(version("4.1"), opt("planner", "idp"), opt("planner", "dp"), opt("runtime", "interpreted")), (1, 55, 54))),
    ("explainmatch", PreParsedStatement("explainmatch", List.empty, (1, 1, 0))),
    ("CYPHER updateStrategy=eager RETURN", PreParsedStatement("RETURN", List(opt("updateStrategy", "eager")), (1, 29, 28))),
    ("CYPHER debug=tostring debug=reportCostComparisonsAsRows RETURN", PreParsedStatement("RETURN", List(opt("debug", "tostring"), opt("debug", "reportCostComparisonsAsRows")), (1, 57, 56))),
    ("CYPHER runtime=slotted RETURN", PreParsedStatement("RETURN", List(opt("runtime", "slotted")), (1, 24, 23))),
    ("CYPHER expressionEngine=interpreted RETURN", PreParsedStatement("RETURN", List(opt("expressionEngine", "interpreted")), (1, 37, 36))),
    ("CYPHER expressionEngine=compiled RETURN", PreParsedStatement("RETURN", List(opt("expressionEngine", "compiled")), (1, 34, 33))),
    ("CYPHER replan=force RETURN", PreParsedStatement("RETURN", List(opt("replan", "force")), (1, 21, 20))),
    ("CYPHER replan=skip RETURN", PreParsedStatement("RETURN", List(opt("replan", "skip")), (1, 20, 19))),
  )

  test("run the tests") {
    forAll(queries) {
      case (query, expected) => parse(query) should equal(expected)
    }
  }

  private def parse(arg:String): PreParsedStatement = {
    CypherPreParser(arg)
  }

  private implicit def lift(pos: (Int, Int, Int)): InputPosition = InputPosition(pos._3, pos._1, pos._2)
}
