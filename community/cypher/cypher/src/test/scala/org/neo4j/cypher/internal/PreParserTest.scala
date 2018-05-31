/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.cypher.internal

import org.neo4j.cypher.{CypherPlannerOption, CypherRuntimeOption, CypherVersion, InvalidArgumentException}
import org.opencypher.v9_0.util.test_helpers.CypherFunSuite

class PreParserTest extends CypherFunSuite {

  val preParser = new PreParser(CypherVersion.default, CypherPlannerOption.default, CypherRuntimeOption.default, 0)

  test("should not allow inconsistent planner options") {
    intercept[InvalidArgumentException](preParser.preParseQuery("CYPHER planner=cost planner=rule RETURN 42"))
  }

  test("should not allow inconsistent runtime options") {
    intercept[InvalidArgumentException](preParser.preParseQuery("CYPHER runtime=compiled runtime=interpreted RETURN 42"))
  }

  test("should not allow multiple versions") {
    intercept[InvalidArgumentException](preParser.preParseQuery("CYPHER 2.3 CYPHER 3.1 RETURN 42"))
  }

  test("should not allow both EXPLAIN and PROFILE") {
    intercept[InvalidArgumentException](preParser.preParseQuery("EXPLAIN PROFILE RETURN 42"))
    intercept[InvalidArgumentException](preParser.preParseQuery("PROFILE EXPLAIN RETURN 42"))
  }
}
