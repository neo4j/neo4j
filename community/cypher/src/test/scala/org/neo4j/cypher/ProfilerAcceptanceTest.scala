/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import org.scalatest.Assertions
import org.junit.Test
import org.junit.Assert._

class ProfilerAcceptanceTest extends ExecutionEngineHelper with Assertions {
  @Test
  def unfinished_profiler_complains() {
    //GIVEN
    val result: ExecutionResult = engine.profile("START n=node(0) RETURN n")

    //WHEN THEN
    intercept[ProfilerStatisticsNotReadyException](result.executionPlanDescription())
  }

  @Test
  def tracks_number_of_rows() {
    //GIVEN
    createNode("foo" -> "bar")
    val result: ExecutionResult = engine.profile("START n=node(1) RETURN n")
    materialise(result)

    //WHEN THEN
    assertContains(result, "rows=1")
  }

  @Test
  def tracks_number_of_graph_accesses() {
    //GIVEN
    createNode("foo" -> "bar")
    val result: ExecutionResult = engine.profile("START n=node(1) RETURN n.foo")
    materialise(result)

    //WHEN THEN
    assertContains(result, "dbhits=1")
  }

  @Test
  def tracks_graph_global_queries() {
    //GIVEN
    val result: ExecutionResult = engine.profile("START n=node(*) RETURN n")
    materialise(result)

    //WHEN THEN
    assertContains(result, "dbhits=1")
  }


  private def assertContains(result:ExecutionResult, expected:String) {
    assertTrue(s"Expected ´$expected´, but got: \n${result.executionPlanDescription()}", result.executionPlanDescription().contains(expected))
  }

  private def materialise(result: ExecutionResult) {
    result.size
  }
}
