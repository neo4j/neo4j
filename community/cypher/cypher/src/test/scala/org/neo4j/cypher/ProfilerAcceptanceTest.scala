/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import javacompat.ProfilerStatistics
import org.scalatest.Assertions
import org.junit.Test
import scala.collection.JavaConverters._
import java.lang.{Iterable => JIterable}
import org.neo4j.cypher.internal.compiler.v2_0
import org.neo4j.cypher.internal.compiler.v2_0.data.{SimpleVal, MapVal, SeqVal}

class ProfilerAcceptanceTest extends ExecutionEngineJUnitSuite {
  @Test
  def unfinished_profiler_complains() {
    //GIVEN
    createNode("foo" -> "bar")
    val result: ExecutionResult = engine.profile("START n=node(0) RETURN n")

    //WHEN THEN
    intercept[ProfilerStatisticsNotReadyException](result.executionPlanDescription())
    result.toList // need to exhaust the results to ensure that the transaction is closed
  }

  @Test
  def tracks_number_of_rows() {
    //GIVEN
    createNode("foo" -> "bar")
    val result: ExecutionResult = engine.profile("START n = node(0) RETURN n")

    //WHEN THEN
    assertRows(1)(result)("NodeById")
  }

  @Test
  def tracks_number_of_graph_accesses() {
    //GIVEN
    createNode("foo" -> "bar")
    val result: ExecutionResult = engine.profile("START n = node(0) RETURN n.foo")

    //WHEN THEN
    assertDbHits(1)(result)("ColumnFilter", "Extract", "NodeById")
  }

  @Test
  def no_problem_measuring_creation() {
    //GIVEN
    val result: ExecutionResult = engine.profile("CREATE n")

    //WHEN THEN
    assertDbHits(0)(result)("EmptyResult")
  }

  @Test
  def tracks_graph_global_queries() {
    createNode()

    //GIVEN
    val result: ExecutionResult = engine.profile("START n=node(*) RETURN n.foo")

    //WHEN THEN
    assertDbHits(1)(result)("ColumnFilter", "Extract", "AllNodes")
  }

  @Test
  def tracks_optional_matches() {
    //GIVEN
    createNode()
    val result: ExecutionResult = engine.profile("start n=node(*) optional match (n)-->(x) return x")

    //WHEN THEN
    assertDbHits(0)(result)("ColumnFilter", "NullableMatch")
    assertDbHits(0)(result)("ColumnFilter", "NullableMatch", "SimplePatternMatcher")
  }

  @Test
  def tracks_pattern_matcher_start_items() {
    //GIVEN
    createNode()
    val result: ExecutionResult = engine.profile("match (n:Person)-->(x) return x")

    //WHEN THEN
    assertDbHits(0)(result)("ColumnFilter")
    assertDbHits(0)(result)("ColumnFilter", "TraversalMatcher")

    val start = result.executionPlanDescription()
      .asJava
      .cd("TraversalMatcher")
      .getArguments
      .get("start")
      .asInstanceOf[java.util.Map[String, Any]]
      .asScala

    assert( "Person" === start("label") )
    assert( "NodeByLabel" === start("producer") )
    assert( Seq("n") === start("identifiers").asInstanceOf[java.lang.Iterable[String]].asScala.toSeq )
  }

  @Test
  def tracks_merge_node_producers() {
    //GIVEN
    val result: ExecutionResult = engine.profile("merge (n:Person {id: 1})")

    //WHEN THEN
    val planDescription = result.executionPlanDescription().asInstanceOf[v2_0.PlanDescription]

    val commands = planDescription.cd("UpdateGraph").arguments("commands").asInstanceOf[SeqVal]
    assert( 1 === commands.v.size )
    val command = commands.v.seq.head.asInstanceOf[MapVal]

    val producers = command.v("producers").asInstanceOf[SeqVal]
    assert( 1 === producers.v.size )
    val producer = producers.v.head.asInstanceOf[MapVal]

    assert( Map(
        "label" -> SimpleVal.fromStr("Person"),
        "producer" -> SimpleVal.fromStr("NodeByLabel"),
        "identifiers" -> SimpleVal.fromSeq("n")
      ) === producer.v )
  }

  @Test
  def allows_optional_match_to_start_a_query() {
    //GIVEN
    val result: ExecutionResult = engine.profile("optional match (n) return n")

    //WHEN THEN
    assertRows(1)(result)("NullableMatch")
  }

  private def assertRows(expectedRows: Int)(result: ExecutionResult)(names: String*) {
    assert(expectedRows === parentCd(result, names).getProfilerStatistics.getRows)
  }

  private def assertDbHits(expectedHits: Int)(result: ExecutionResult)(names: String*) {
    val statistics: ProfilerStatistics = parentCd(result, names).getProfilerStatistics
    assert(expectedHits === statistics.getDbHits)
  }

  private def parentCd(result: ExecutionResult, names: Seq[String]) = {
    result.toList
    val descr = result.executionPlanDescription().asJava
    if (names.isEmpty)
      descr
    else {
      assert(names.head === descr.getName)
      descr.cd(names.tail: _*)
    }
  }
}
