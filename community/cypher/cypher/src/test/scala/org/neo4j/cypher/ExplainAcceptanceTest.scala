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
package org.neo4j.cypher

import org.neo4j.cypher.internal.compiler.v2_3.planDescription.InternalPlanDescription.Arguments.MergePattern

class ExplainAcceptanceTest extends ExecutionEngineFunSuite {
  test("normal query is marked as such") {
    createNode()
    val result = execute("match n return n")

    result.planDescriptionRequested should equal(false)
    result shouldNot be(empty)
  }

  test("explain query is marked as such") {
    createNode()
    val result = execute("explain match n return n")

    result.planDescriptionRequested should equal(true)
    result should be(empty)
  }

  test("EXPLAIN for Cypher 2.3") {
    val result = eengine.execute("explain match n return n")
    result.toList
    assert(result.planDescriptionRequested, "result not marked with planDescriptionRequested")
    result.executionPlanDescription().toString should include("Estimated Rows")
    result.executionPlanDescription().asJava.toString should include("Estimated Rows")
  }

  test("should report which node the merge starts from") {
    val query = "EXPLAIN MERGE (first)-[:PIZZA]->(second)"

    val result = execute(query)
    val plan = result.executionPlanDescription()
    result.close()

    plan.toString should include(MergePattern("second").toString)
  }
}
