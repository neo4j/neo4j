/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.frontend.v3_4.ast

import org.neo4j.cypher.internal.frontend.v3_4.parser.{CypherParser, ParserTest}
import org.neo4j.cypher.internal.aux.v3_4.test_helpers.CypherFunSuite

class StatementReturnColumnsTest extends CypherFunSuite with ParserTest[Statement, List[String]] {

  override def convert(statement: Statement): List[String] = statement.returnColumns

  implicit val parserToTest = CypherParser.Statement

  test("MATCH ... RETURN ...") {
    parsing("MATCH (n) RETURN n, n.prop AS m") shouldGive List("n", "m")
    parsing("MATCH (n) WITH 1 AS x RETURN x") shouldGive List("x")
  }

  test("UNION") {
    parsing("MATCH (n) RETURN n UNION MATCH (n) RETURN n") shouldGive List("n")
    parsing("MATCH (n) RETURN n UNION ALL MATCH (n) RETURN n") shouldGive List("n")
  }

  test("CALL ... YIELD ...") {
    parsing("CALL foo YIELD x, y") shouldGive List("x", "y")
    parsing("CALL foo YIELD x, y AS z") shouldGive List("x", "z")
  }

  test("Updates") {
    parsing("MATCH (n) CREATE ()") shouldGive List.empty
    parsing("MATCH (n) CREATE UNIQUE ()") shouldGive List.empty
    parsing("MATCH (n) SET n.prop=12") shouldGive List.empty
    parsing("MATCH (n) REMOVE n.prop") shouldGive List.empty
    parsing("MATCH (n) DELETE (m)") shouldGive List.empty
    parsing("MATCH (n) MERGE (m:Person {name: 'Stefan'}) ON MATCH SET n.happy = 100") shouldGive List.empty
    parsing("MATCH (n) FOREACH (m IN [1,2,3] | CREATE())") shouldGive List.empty
  }
}
