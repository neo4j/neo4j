/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal

import org.neo4j.cypher.InvalidArgumentException
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class CypherStatementWithOptionsTest extends CypherFunSuite {

  test("should not allow inconsistent planner options with new notation, old notation and mixed notation") {
    intercept[InvalidArgumentException](CypherStatementWithOptions("CYPHER planner=cost PLANNER RULE RETURN 42"))
    intercept[InvalidArgumentException](CypherStatementWithOptions("CYPHER planner=cost planner=rule RETURN 42"))
    intercept[InvalidArgumentException](CypherStatementWithOptions("CYPHER PLANNER COST PLANNER RULE RETURN 42"))
  }

  test("should not allow multiple versions") {
    intercept[InvalidArgumentException](CypherStatementWithOptions("CYPHER 2.2 CYPHER 2.3 RETURN 42"))
  }

  test("should not allow both EXPLAIN and PROFILE") {
    intercept[InvalidArgumentException](CypherStatementWithOptions("EXPLAIN PROFILE RETURN 42"))
    intercept[InvalidArgumentException](CypherStatementWithOptions("PROFILE EXPLAIN RETURN 42"))
  }

  private implicit def parse(arg: String): PreParsedStatement = {
    CypherPreParser(arg)
  }
}
