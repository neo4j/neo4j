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
package org.neo4j.cypher.internal.compiler

import org.neo4j.cypher.GraphDatabaseFunSuite
import org.neo4j.cypher.internal.CypherCompiler.{CLOCK, DEFAULT_QUERY_PLAN_TTL, DEFAULT_STATISTICS_DIVERGENCE_THRESHOLD}
import org.neo4j.cypher.internal.compatibility.{EntityAccessorWrapper2_3, WrappedMonitors2_3}
import org.neo4j.cypher.internal.compiler.v2_3.tracing.rewriters.RewriterStepSequencer
import org.neo4j.cypher.internal.compiler.v2_3.{CypherCompilerFactory, GreedyPlannerName, InfoLogger, _}
import org.neo4j.kernel.GraphDatabaseAPI
import org.neo4j.kernel.impl.core.NodeManager

class CypherCompilerPerformanceTest extends GraphDatabaseFunSuite {

  import org.scalatest.prop.TableDrivenPropertyChecks._

  val VERBOSE = false
  val NO_RUNS = 5

  val warmup = "warmup" ->
    """MATCH (n:Person)-[:KNOWS]->(c:City)
      |WITH DISTINCT n, count(*) AS count
      |MATCH (n)-[:KNOWS*..5]->(f:Person)-[:LIVES_IN]->(:City {name: "Berlin"})
      |WHERE (f)-[:LOVES]->(:Girl)
      |RETURN n, count, collect(f) AS friends""".stripMargin

  val foo1 = "foo1" ->
    """match
      |  (toFrom:Duck{Id:{duckId}}),
      |  brook-[:precedes|is]->startbrook,
      |  toFrom-[toFrom_rel:wiggle|quack]->startbrook
      |with brook
      |match brook-[:regarding_summary_phrase]->Duck
      |with brook, Duck
      |match checkUnpopularDuck-[:quack]->brook
      |where ((checkUnpopularDuck.Rank > 0 or checkUnpopularDuck.Rank is null)) and ((Duck.Rank > 0 or Duck.Rank is null)) and (NOT(Duck.Id = {duckId}))
      |RETURN Duck.Id as Id, Duck.Name as Name, Duck.Type as Type, Duck.MentionNames as Mentions, count(distinct brook) as brookCount
      |order by brookCount desc skip 0 limit 51""".stripMargin

  val foo2 = "foo2" ->
    """match
      |  (toFrom:Duck{Id:{duckId}}),
      |  toFrom-[toFrom_rel:wiggle|quack]->(brook:brook),
      |  copy-[:of]->bridge-[bridgebrook:containing]->brook,
      |  brook-[:in_thicket]->(thicket:Thicket),
      |  (checkUnpopularDuck:Duck)-[:quack]->brook
      |where (checkUnpopularDuck.Rank > 0 or checkUnpopularDuck.Rank is null)
      |return count(distinct thicket) as ThicketCount, count(distinct brook) as brookCount
      |skip 0""".stripMargin

  val socnet1 = "socnet1" ->
    """MATCH (subject:User {name:{name}})
      |MATCH p=(subject)-[:WORKED_ON]->()<-[:WORKED_ON]-(person)-[:INTERESTED_IN]->(interest)
      |WHERE person<>subject AND interest.name={topic}
      |WITH DISTINCT person.name AS name, min(length(p)) as pathLength
      |ORDER BY pathLength ASC LIMIT 10 RETURN name, pathLength
      |UNION
      |MATCH (subject:User {name:{name}})
      |MATCH p=(subject)-[:WORKED_ON]->()-[:WORKED_ON]-()<-[:WORKED_ON]-(person)-[:INTERESTED_IN]->(interest)
      |WHERE person<>subject AND interest.name={topic}
      |WITH DISTINCT person.name AS name, min(length(p)) as pathLength
      |ORDER BY pathLength ASC LIMIT 10 RETURN name, pathLength
      |UNION
      |MATCH (subject:User {name:{name}})
      |MATCH p=(subject)-[:WORKED_ON]->()-[:WORKED_ON]-()-[:WORKED_ON]-()<-[:WORKED_ON]-(person)-[:INTERESTED_IN]->(interest)
      |WHERE person<>subject AND interest.name={topic}
      |WITH DISTINCT person.name AS name, min(length(p)) as pathLength
      |ORDER BY pathLength ASC LIMIT 10
      |RETURN name, pathLength""".stripMargin

  val socnet9 = "socnet9" ->
    """MATCH (subject:User {name:{name}})
      |MATCH p=(subject)-[:WORKED_WITH*0..1]-()-[:WORKED_WITH]-(person)-[:INTERESTED_IN]->(interest)
      |WHERE person<>subject AND interest.name IN {interests}
      |WITH person, interest, min(length(p)) as pathLength
      |RETURN person.name AS name, count(interest) AS score, collect(interest.name) AS interests, (pathLength - 1) AS distance
      |ORDER BY score DESC LIMIT 20""".stripMargin

  val qmul1 = "qmul1" ->
    """MATCH (a:Person)-->(m)-[r]->(n)-->(a)
      |WHERE a.uid IN ['1195630902','1457065010'] AND HAS(m.location_lat) AND HAS(n.location_lat)
      |RETURN count(r)""".stripMargin

  test("plans are built fast enough") {
    val queries = Table("query", foo1, foo2, socnet1, socnet9, qmul1)
    plan(10)(warmup)

    forAll(queries) {
      (query: (String, String)) =>
        assertIsPlannedTimely(query)
    }
  }

  def assertIsPlannedTimely(query: (String, String)) = {
    val ((prepareFirst, planFirst), (prepareAvg, planAvg)) = plan(NO_RUNS)(query)

    (prepareAvg + planAvg) shouldBe < (fromSeconds(1.0))
    (prepareFirst + planFirst) shouldBe < (fromSeconds(1.0))
  }

  def fromSeconds(seconds: Double) = seconds * 1000000000.0

  def toSeconds(nanoSeconds: Double) = nanoSeconds / 1000000000.0

  def plan(times: Int)(queryWithTag: (String, String)): ((Double, Double), (Double, Double)) = {
    val (tag, query) = queryWithTag
    var (prepareTotal, planTotal) = (0.0d, 0.0d)
    val (prepareFirst, planFirst) = plan(query)

    var count = 2
    while (count <= times) {
      val (prepareTime, planTime) = plan(query)
      prepareTotal = prepareTotal + prepareTime
      planTotal = planTotal + planTime
      count += 1
    }
    val prepareAvg = (prepareTotal + prepareFirst) / times
    val planAvg = (planTotal + planFirst) / times

    if (VERBOSE) {
      println(s"* Planned query '$tag':\n\n${indent(query)}\n")
      println(s"  * First prepare time: ${toSeconds(prepareFirst)}")
      println(s"  * First plan time: ${toSeconds(planFirst)}")
      println(s"  * First total time: ${toSeconds(prepareFirst + planFirst)}")
      println(s"  * Number of runs: $times")
      println(s"  * Average prepare time: ${toSeconds(prepareAvg)} (excluding first ${toSeconds(prepareTotal / (times-1))})")
      println(s"  * Average planning time: ${toSeconds(planAvg)} (excluding first ${toSeconds(planTotal / (times-1))})")
      println(s"  * Average total time: ${toSeconds(prepareAvg + planAvg)} (excluding first ${toSeconds((prepareTotal + planTotal) / (times-1))})")
      println()
    }

    ((prepareFirst, planFirst), (prepareAvg, planAvg))
  }

  def indent(text: String, indent: String = "    ") =
    indent + text.replace("\n", s"\n$indent")

  def plan(query: String): (Double, Double) = {
    val compiler = createCurrentCompiler
    val (prepareTime, preparedQuery) = measure(compiler.prepareQuery(query, query, devNullLogger))
    val (planTime, _) = graph.inTx {
      measure(compiler.executionPlanBuilder.build(planContext, preparedQuery))
    }
    (prepareTime, planTime)
  }

  def measure[T](f: => T): (Long, T) = {
    val start = System.nanoTime()
    val result = f
    val stop = System.nanoTime()
    (stop - start, result)
  }

  def createCurrentCompiler = {
    val nodeManager = graph.asInstanceOf[GraphDatabaseAPI].getDependencyResolver.resolveDependency(classOf[NodeManager])

    CypherCompilerFactory.costBasedCompiler(
      graph = graph,
      new EntityAccessorWrapper2_3(nodeManager),
      CypherCompilerConfiguration(
        queryCacheSize = 1,
        statsDivergenceThreshold = DEFAULT_STATISTICS_DIVERGENCE_THRESHOLD,
        queryPlanTTL = DEFAULT_QUERY_PLAN_TTL,
        useErrorsOverWarnings = false,
        idpMaxTableSize = 128,
        idpIterationDuration = 1000,
        nonIndexedLabelWarningThreshold = 10000L
      ),
      clock = CLOCK,
      monitors = new WrappedMonitors2_3(kernelMonitors),
      logger = DEV_NULL,
      rewriterSequencer = RewriterStepSequencer.newPlain,
      plannerName = Some(GreedyPlannerName),
      runtimeName = Some(InterpretedRuntimeName)
    )
  }

  object DEV_NULL extends InfoLogger {
    def info(message: String){}
  }
}
