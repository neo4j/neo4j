/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
import java.lang.{Iterable => JIterable}
import org.neo4j.cypher.internal.compiler.v2_1
import org.neo4j.cypher.internal.compiler.v2_1.data.{SimpleVal, MapVal, SeqVal}
import org.neo4j.cypher.internal.helpers.TxCounts
import org.neo4j.cypher.internal.commons.CreateTempFileTestSupport
import org.neo4j.cypher.internal.compiler.v2_1.commands.expressions.StringHelper.RichString

class ProfilerAcceptanceTest extends ExecutionEngineFunSuite with CreateTempFileTestSupport {

  test("unfinished profiler complains") {
    //GIVEN
    createNode("foo" -> "bar")
    val result: ExecutionResult = engine.profile("START n=node(0) RETURN n")

    //WHEN THEN
    intercept[ProfilerStatisticsNotReadyException](result.executionPlanDescription())
    result.toList // need to exhaust the results to ensure that the transaction is closed
  }

  test("tracks number of rows") {
    //GIVEN
    createNode("foo" -> "bar")
    val result: ExecutionResult = engine.profile("START n = node(0) RETURN n")

    //WHEN THEN
    assertRows(1)(result)("NodeById")
  }


  test("tracks number of graph accesses") {
    //GIVEN
    createNode("foo" -> "bar")
    val result: ExecutionResult = engine.profile("START n = node(0) RETURN n.foo")

    //WHEN THEN
    assertDbHits(1)(result)("ColumnFilter", "Extract", "NodeById")
  }


  test("no problem measuring creation") {
    //GIVEN
    val result: ExecutionResult = engine.profile("CREATE n")

    //WHEN THEN
    assertDbHits(0)(result)("EmptyResult")
  }


  test("tracks graph global queries") {
    createNode()

    //GIVEN
    val result: ExecutionResult = engine.profile("START n=node(*) RETURN n.foo")

    //WHEN THEN
    assertDbHits(1)(result)("ColumnFilter", "Extract", "AllNodes")
  }


  test("tracks optional matches") {
    //GIVEN
    createNode()
    val result: ExecutionResult = engine.profile("start n=node(*) optional match (n)-->(x) return x")

    //WHEN THEN
    assertDbHits(0)(result)("ColumnFilter", "NullableMatch")
    assertDbHits(0)(result)("ColumnFilter", "NullableMatch", "SimplePatternMatcher")
  }

  test("tracks merge node producers") {
    //GIVEN
    val result: ExecutionResult = engine.profile("merge (n:Person {id: 1})")

    //WHEN THEN
    val planDescription = result.executionPlanDescription().asInstanceOf[v2_1.PlanDescription]

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

  test("allows optional match to start a query") {
    //GIVEN
    val result: ExecutionResult = engine.profile("cypher experimental optional match (n) return n")

    //WHEN THEN
    assertRows(1)(result)("Optional")
  }

  test("should produce profile when using limit") {
    // GIVEN
    createNode()
    createNode()
    createNode()
    val result = profile("""START n=node(*) RETURN n LIMIT 1""")

    // WHEN
    result.toList

    // THEN PASS
    result.executionPlanDescription()
  }

  test ("should support profiling union queries") {
    val result = profile("return 1 as A union return 2 as A")
    result.toList should equal(List(Map("A" -> 1), Map("A" -> 2)))
  }

  test("should support profiling merge_queries") {
    val result = profile("merge (a {x: 1}) return a.x as A")
    result.toList.head("A") should equal(1)
  }

  test("should support profiling optional match queries") {
    createLabeledNode(Map("x" -> 1), "Label")
    val result = profile("match (a:Label {x: 1}) optional match (a)-[:REL]->(b) return a.x as A, b.x as B").toList.head
    result("A") should equal(1)
    result("B") should equal(null.asInstanceOf[Int])
  }

  test("should support profiling optional match and with") {
    createLabeledNode(Map("x" -> 1), "Label")
    val result = profile("match (n) optional match (n)--(m) with n, m where m is null return n.x as A").toList.head
    result("A") should equal(1)
  }

  test("should handle PERIODIC COMMIT when profiling") {
    val url = createTempFileURL("cypher", ".csv")(writer => {
      1.to(100).foreach { i =>
        writer.println(i.toString)
      }
    }).cypherEscape

    val query = s"USING PERIODIC COMMIT 10 LOAD CSV FROM '$url' AS line CREATE()"

    // given
    execute(query).toList
    deleteAllEntities()
    val initialTxCounts = graph.txCounts

    // when
    val result = profile(query)

    // then
    graph.txCounts-initialTxCounts should equal(TxCounts(commits = 11))
    result.executionPlanDescription().asJava should not equal(null)
    result.queryStatistics().containsUpdates should equal(true)
    result.queryStatistics().nodesCreated should equal(100)
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
    val description = result.executionPlanDescription().asJava
    if (names.isEmpty)
      description
    else {
      assert(names.head === description.getName)
      description.cd(names.tail: _*)
    }
  }
}
