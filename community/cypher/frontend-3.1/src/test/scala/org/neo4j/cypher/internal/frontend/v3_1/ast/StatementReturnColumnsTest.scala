/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.frontend.v3_1.ast

import org.neo4j.cypher.internal.frontend.v3_1.parser.{CypherParser, ParserTest}
import org.neo4j.cypher.internal.frontend.v3_1.test_helpers.CypherFunSuite

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
