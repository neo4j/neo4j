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
package org.neo4j.cypher

import org.neo4j.cypher.internal.commons.CreateTempFileTestSupport
import org.neo4j.cypher.internal.compiler.v2_2
import org.neo4j.cypher.internal.compiler.v2_2.commands.expressions.StringHelper.RichString
import org.neo4j.cypher.internal.compiler.v2_2.executionplan.InternalExecutionResult
import org.neo4j.cypher.internal.compiler.v2_2.planDescription.Argument
import org.neo4j.cypher.internal.compiler.v2_2.planDescription.InternalPlanDescription.Arguments.{EstimatedRows, DbHits, Rows}
import org.neo4j.cypher.internal.compiler.v2_2.spi.GraphStatistics
import org.neo4j.cypher.internal.helpers.TxCounts

class ProfilerAcceptanceTest extends ExecutionEngineFunSuite with CreateTempFileTestSupport with NewPlannerTestSupport {

  test("match n where n-[:FOO]->() return *") {
    //GIVEN
    relate( createNode(), createNode(), "FOO")

    //WHEN
    val result = profile("CYPHER 2.2 planner=cost match n where n-[:FOO]->() return *")

    //THEN
    assertRows(1)(result)("SemiApply")
    assertDbHits(0)(result)("SemiApply")

    assertRows(2)(result)("AllNodesScan")
    assertDbHits(3)(result)("AllNodesScan")

    assertRows(0)(result)("Expand")
    assertDbHits(2)(result)("Expand")
  }

  test("PROFILE for Cypher 2.0") {
    val result = eengine.profile("cypher 2.0 match n where n-[:FOO]->() return *")

    assert(result.planDescriptionRequested, "result not marked with planDescriptionRequested")
    result.executionPlanDescription().toString should include("_db_hits")
  }

  test("EXPLAIN for Cypher 2.2") {
    val result = eengine.execute("explain match n return n")
    result.toList
    assert(result.planDescriptionRequested, "result not marked with planDescriptionRequested")
    result.executionPlanDescription().toString should include("EstimatedRows")
    result.executionPlanDescription().asJava.toString should include("EstimatedRows")
  }

  test("match n where not n-[:FOO]->() return *") {
    //GIVEN
    relate( createNode(), createNode(), "FOO")

    //WHEN
    val result = profile("CYPHER 2.2 planner=cost match n where not n-[:FOO]->() return *")

    //THEN
    assertRows(1)(result)("AntiSemiApply")
    assertDbHits(0)(result)("AntiSemiApply")

    assertRows(2)(result)("AllNodesScan")
    assertDbHits(3)(result)("AllNodesScan")

    assertRows(0)(result)("Expand")
    assertDbHits(2)(result)("Expand")
  }

  test("unfinished profiler complains") {
    //GIVEN
    createNode("foo" -> "bar")
    val result: ExecutionResult = eengine.profile("match (n) where id(n) = 0 RETURN n")

    //WHEN THEN
    intercept[ProfilerStatisticsNotReadyException](result.executionPlanDescription())
    result.toList // need to exhaust the results to ensure that the transaction is closed
  }

  test("tracks number of rows") {
    //GIVEN
    createNode("foo" -> "bar")
    val result = profile("match (n) where id(n) = 0 RETURN n")

    //WHEN THEN
    assertRows(1)(result)("NodeById")
  }

  test("tracks number of graph accesses") {
    //GIVEN
    createNode("foo" -> "bar")
    val result = profile("match (n) where id(n) = 0 RETURN n.foo")

    //WHEN THEN
    assertRows(1)(result)("ColumnFilter", "Extract", "NodeById")
    assertDbHits(0)(result)("ColumnFilter")
    assertDbHits(2)(result)("Extract")
    assertDbHits(1)(result)("NodeById")
  }


  test("no problem measuring creation") {
    //GIVEN
    val result = legacyProfile("CREATE n")

    //WHEN THEN
    assertDbHits(0)(result)("EmptyResult")
  }

  test("tracks graph global queries") {
    createNode()

    //GIVEN
    val result = profile("MATCH n RETURN n.foo")

    //WHEN THEN
    assertRows(1)(result)("ColumnFilter")
    assertDbHits(0)(result)("ColumnFilter")

    assertRows(1)(result)("Extract")
    assertDbHits(1)(result)("Extract")

    assertRows(1)(result)("AllNodes")
    assertDbHits(2)(result)("AllNodes")
  }


  test("tracks optional matches") {
    //GIVEN
    createNode()
    val result = profile("MATCH n optional match (n)-->(x) return x")

    //WHEN THEN
    assertDbHits(0)(result)("ColumnFilter", "NullableMatch")
    assertDbHits(0)(result)("ColumnFilter", "NullableMatch", "SimplePatternMatcher")
  }

  test("allows optional match to start a query") {
    //GIVEN
    val result = profile("CYPHER 2.2 planner=cost optional match (n) return n")

    //WHEN THEN
    assertRows(1)(result)("Optional")
  }

  test("should produce profile when using limit") {
    // GIVEN
    createNode()
    createNode()
    createNode()
    val result = profile("""MATCH n RETURN n LIMIT 1""")

    // WHEN
    result.toList

    // THEN PASS
    result.executionPlanDescription()
  }

  test("LIMIT should influence cardinality estimation even when parameterized") {
    (0 until 100).map(i => createLabeledNode("Person"))
    val result = executeWithAllPlanners(s"PROFILE MATCH (p:Person) RETURN p LIMIT {limit}", "limit" -> 10)
    assertEstimatedRows(GraphStatistics.DEFAULT_LIMIT_CARDINALITY.amount.toInt)(result)("Limit")
  }

  test("LIMIT should influence cardinality estimation with literal") {
    (0 until 100).map(i => createLabeledNode("Person"))
    val result = executeWithAllPlanners(s"PROFILE MATCH (p:Person) RETURN p LIMIT 10")
    assertEstimatedRows(10)(result)("Limit")
  }

  test("LIMIT should influence cardinality estimation with literal and parameters") {
    (0 until 100).map(i => createLabeledNode("Person"))
    val result = executeWithAllPlanners(s"PROFILE MATCH (p:Person) WHERE 50 = {fifty} RETURN p LIMIT 10", "fifty" -> 50)
    assertEstimatedRows(10)(result)("Limit")
  }

  test ("should support profiling union queries") {
    val result = profile("return 1 as A union return 2 as A")
    result.toSet should equal(Set(Map("A" -> 1), Map("A" -> 2)))
  }

  test("should support profiling merge_queries") {
    val result = legacyProfile("merge (a {x: 1}) return a.x as A")
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
    val executionResult: InternalExecutionResult = profile("match (n) optional match (n)--(m) with n, m where m is null return n.x as A")
    val result = executionResult.toList.head
    result("A") should equal(1)
  }

  test("should handle PERIODIC COMMIT when profiling") {
    val url = createTempFileURL("cypher", ".csv")(writer => {
      (1 to 100).foreach(writer.println)
    }).cypherEscape

    val query = s"USING PERIODIC COMMIT 10 LOAD CSV FROM '$url' AS line CREATE()"

    // given
    executeWithRulePlannerOnly(query).toList
    deleteAllEntities()
    val initialTxCounts = graph.txCounts

    // when
    val result = legacyProfile(query)

    // then
    val expectedTxCount = 1 + // First tx used to compile the query
                          10  // One per 10 rows of CSV file

    graph.txCounts-initialTxCounts should equal(TxCounts(commits = expectedTxCount))
    result.queryStatistics().containsUpdates should equal(true)
    result.queryStatistics().nodesCreated should equal(100)
  }

  test("should not have a problem profiling empty results") {
    val result = super.profile("CYPHER 2.2 planner=cost MATCH n WHERE (n)-->() RETURN n")

    result shouldBe empty
    result.executionPlanDescription().toString should include("AllNodes")
  }

  test("reports COST planner when showing plan description") {
    val executionPlanDescription = eengine.execute("CYPHER 2.2 planner=cost match n return n").executionPlanDescription()
    executionPlanDescription.toString should include("Planner COST" + System.lineSeparator())
  }

  test("reports RULE planner when showing plan description") {
    val executionPlanDescription = eengine.execute("CYPHER 2.2 create ()").executionPlanDescription()

    executionPlanDescription.toString should not include "Planner COST"
    executionPlanDescription.toString should include("Planner RULE" + System.lineSeparator())
  }

  test("does not use Apply for aggregation and order by") {
    val a = profile("match n return n, count(*) as c order by c")

    a.executionPlanDescription().toString should not include "Apply"
  }

  test("should not use eager plans for distinct") {
    val a = profile("match n return distinct n.name")
    a.executionPlanDescription().toString should not include "Eager"
  }

  test("should not show  EstimatedRows in legacy profiling") {
    val result =legacyProfile("create()")
    result.executionPlanDescription().toString should not include("EstimatedRows")
  }

  test("CYPHER planner=cost match (p:Person {name:'Seymour'}) return (p)-[:RELATED_TO]->()") {
    //GIVEN
    val seymour = createLabeledNode(Map("name" -> "Seymour"), "Person")
    relate(seymour, createLabeledNode(Map("name" -> "Buddy"), "Person"), "RELATED_TO")
    relate(seymour, createLabeledNode(Map("name" -> "Boo Boo"), "Person"), "RELATED_TO")
    relate(seymour, createLabeledNode(Map("name" -> "Walt"), "Person"), "RELATED_TO")
    relate(seymour, createLabeledNode(Map("name" -> "Waker"), "Person"), "RELATED_TO")
    relate(seymour, createLabeledNode(Map("name" -> "Zooey"), "Person"), "RELATED_TO")
    relate(seymour, createLabeledNode(Map("name" -> "Franny"), "Person"), "RELATED_TO")

    graph.createConstraint("Person", "name")

    //WHEN
    val result = profile("CYPHER planner=cost match (p:Person {name:'Seymour'}) return (p)-[:RELATED_TO]->()")

    //THEN
    assertDbHits(7)(result)("Projection")
    assertDbHits(1)(result)("NodeUniqueIndexSeek")
   }

  test("should show expand without types in a simple form") {
    val a = profile("match n-->() return *")

    a.executionPlanDescription().toString should include("()<--(n)")
  }

  test("should show expand with types in a simple form") {
    val a = profile("CYPHER planner=cost match n-[r:T]->() return *")

    a.executionPlanDescription().toString should include("()<-[r:T]-(n)")
  }

  test("profile projections") {
    // given
    val n = createLabeledNode(Map("name" -> "Seymour"), "Glass")
    val o = createNode()
    relate(n, o, "R1")
    relate(o, createLabeledNode(Map("name" -> "Zoey"), "Glass"), "R2")
    relate(o, createLabeledNode(Map("name" -> "Franny"), "Glass"), "R2")
    relate(o, createNode(), "R2")
    relate(o, createNode(), "R2")
    graph.createIndex("Glass", "name")

    // when
    val result = innerExecute(
      "profile match (n:Glass {name: 'Seymour'})-[:R1]->(o)-[:R2]->(p:Glass) USING INDEX n:Glass(name) return p.name")

    // then
    assertDbHits(2)(result)("Projection")
  }

  test("profile filter") {
    // given
    val n = createLabeledNode(Map("name" -> "Seymour"), "Glass")
    val o = createNode()
    relate(n, o, "R1")
    relate(o, createLabeledNode(Map("name" -> "Zoey"), "Glass"), "R2")
    relate(o, createLabeledNode(Map("name" -> "Franny"), "Glass"), "R2")
    relate(o, createNode(), "R2")
    relate(o, createNode(), "R2")
    graph.createIndex("Glass", "name")

    // when
    val result = innerExecute(
      "profile match (n:Glass {name: 'Seymour'})-[:R1]->(o)-[:R2]->(p) USING INDEX n:Glass(name) WHERE p.name = 'Franny' return p.name")

    // then
    assertDbHits(1)(result)("Projection")
    assertDbHits(4)(result)("Filter")
  }

  private def assertRows(expectedRows: Int)(result: InternalExecutionResult)(names: String*) {
    getPlanDescriptions(result, names).foreach {
      plan => assert(expectedRows === getArgument[Rows](plan).value, s" wrong row count for plan: ${plan.name}")
    }
  }

  private def assertEstimatedRows(expectedRows: Int)(result: InternalExecutionResult)(names: String*) {
    getPlanDescriptions(result, names).foreach {
      plan => assert(expectedRows === getArgument[EstimatedRows](plan).value, s" wrong estiamted row count for plan: ${plan.name}")
    }
  }

  private def assertDbHits(expectedRows: Int)(result: InternalExecutionResult)(names: String*) {
    getPlanDescriptions(result, names).foreach {
      plan => assert(expectedRows === getArgument[DbHits](plan).value, s" wrong db hits for plan: ${plan.name}")
    }
  }

  type Planner = (String, Seq[(String, Any)]) => InternalExecutionResult

  def profileWithPlanner(planner: Planner, q: String, params: (String, Any)*): InternalExecutionResult = {
    val result = planner("profile " + q, params)
    assert(result.planDescriptionRequested, "result not marked with planDescriptionRequested")
    val planDescription: v2_2.planDescription.InternalPlanDescription = result.executionPlanDescription()
    planDescription.flatten.foreach {
      p =>
        if (!p.arguments.exists(_.isInstanceOf[DbHits])) {
          fail("Found plan that was not profiled with DbHits: " + p.name)
        }
        if (!p.arguments.exists(_.isInstanceOf[Rows])) fail("Found plan that was not profiled with Rows: " + p.name)
    }
    result
  }

  override def profile(q: String, params: (String, Any)*): InternalExecutionResult = profileWithPlanner(executeWithAllPlanners(_,_:_*), q, params:_*)

  def legacyProfile(q: String, params: (String, Any)*): InternalExecutionResult = profileWithPlanner(innerExecute(_,_:_*), q, params:_*)



  private def getArgument[A <: Argument](plan: v2_2.planDescription.InternalPlanDescription)(implicit manifest: Manifest[A]): A = plan.arguments.collectFirst {
    case x: A => x
  }.getOrElse(fail(s"Failed to find plan description argument where expected. Wanted ${manifest.toString()} but only found ${plan.arguments}"))

  private def getPlanDescriptions(result: InternalExecutionResult, names: Seq[String]): Seq[v2_2.planDescription.InternalPlanDescription] = {
    result.toList
    val description = result.executionPlanDescription()
    if (names.isEmpty)
      description.flatten
    else {
      names.flatMap {
        name => description.find(name)
      }
    }
  }
}
