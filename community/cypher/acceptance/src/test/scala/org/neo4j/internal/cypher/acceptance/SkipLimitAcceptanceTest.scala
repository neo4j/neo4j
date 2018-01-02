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
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.{SyntaxException, NewPlannerTestSupport, ExecutionEngineFunSuite}

class SkipLimitAcceptanceTest extends ExecutionEngineFunSuite {
  test("SKIP should not allow identifiers") {
    intercept[SyntaxException](execute("MATCH (n) RETURN n SKIP n.count"))
  }

  test("LIMIT should not allow identifiers") {
    intercept[SyntaxException](execute("MATCH (n) RETURN n LIMIT n.count"))
  }

  test("SKIP with an expression that does not depend on identifiers should work") {
    1 to 10 foreach { _ => createNode() }

    val query = "MATCH (n) RETURN n SKIP toInt(rand()*9)"
    val result = execute(query)

    result.toList should not be empty
  }

  test("LIMIT with an expression that does not depend on identifiers should work") {
    1 to 3 foreach { _ => createNode() }

    val query = "MATCH (n) RETURN n LIMIT toInt(ceil(1.7))"
    val result = execute(query)

    result.toList should have size 2
  }
}
