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
package org.neo4j.cypher.internal.compiler

import org.neo4j.cypher.GraphDatabaseFunSuite
import org.neo4j.cypher.internal.CypherCompiler.{CLOCK, DEFAULT_QUERY_PLAN_TTL, DEFAULT_STATISTICS_DIVERGENCE_THRESHOLD}
import org.neo4j.cypher.internal.compatibility.WrappedMonitors3_1
import org.neo4j.cypher.internal.compiler.v3_1.helpers.IdentityTypeConverter
import org.neo4j.cypher.internal.compiler.v3_1.tracing.rewriters.RewriterStepSequencer
import org.neo4j.cypher.internal.compiler.v3_1.{CypherCompilerFactory, InfoLogger, _}
import org.neo4j.cypher.internal.spi.v3_1.codegen.GeneratedQueryStructure

import scala.concurrent.duration._

class CypherCompilerPerformanceTest extends GraphDatabaseFunSuite {

  val NUMBER_OF_RUNS = 10

  val warmup =
    """MATCH (n:Person)-[:KNOWS]->(c:City)
      |WITH DISTINCT n, count(*) AS count
      |MATCH (n)-[:KNOWS*..5]->(f:Person)-[:LIVES_IN]->(:City {name: "Berlin"})
      |WHERE (f)-[:LOVES]->(:Girl)
      |RETURN n, count, collect(f) AS friends""".stripMargin

  val foo1 =
    """match
      |  (toFrom:Duck{Id:{duckId}}),
      |  (brook)-[:precedes|is]->(startbrook),
      |  (toFrom)-[toFrom_rel:wiggle|quack]->(startbrook)
      |with brook
      |match (brook)-[:regarding_summary_phrase]->(Duck)
      |with brook, Duck
      |match (checkUnpopularDuck)-[:quack]->(brook)
      |where ((checkUnpopularDuck.Rank > 0 or checkUnpopularDuck.Rank is null)) and ((Duck.Rank > 0 or Duck.Rank is null)) and (NOT(Duck.Id = {duckId}))
      |RETURN Duck.Id as Id, Duck.Name as Name, Duck.Type as Type, Duck.MentionNames as Mentions, count(distinct brook) as brookCount
      |order by brookCount desc skip 0 limit 51""".stripMargin

  val foo2 =
    """match
      |  (toFrom:Duck{Id:{duckId}}),
      |  (toFrom)-[toFrom_rel:wiggle|quack]->(brook:brook),
      |  (copy)-[:of]->(bridge)-[bridgebrook:containing]->(brook),
      |  (brook)-[:in_thicket]->(thicket:Thicket),
      |  (checkUnpopularDuck:Duck)-[:quack]->(brook)
      |where (checkUnpopularDuck.Rank > 0 or checkUnpopularDuck.Rank is null)
      |return count(distinct thicket) as ThicketCount, count(distinct brook) as brookCount
      |skip 0""".stripMargin

  val socnet1 =
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

  val socnet9 =
    """MATCH (subject:User {name:{name}})
      |MATCH p=(subject)-[:WORKED_WITH*0..1]-()-[:WORKED_WITH]-(person)-[:INTERESTED_IN]->(interest)
      |WHERE person<>subject AND interest.name IN {interests}
      |WITH person, interest, min(length(p)) as pathLength
      |RETURN person.name AS name, count(interest) AS score, collect(interest.name) AS interests, (pathLength - 1) AS distance
      |ORDER BY score DESC LIMIT 20""".stripMargin

  val qmul1 =
    """MATCH (a:Person)-->(m)-[r]->(n)-->(a)
      |WHERE a.uid IN ['1195630902','1457065010'] AND exists(m.location_lat) AND exists(n.location_lat)
      |RETURN count(r)""".stripMargin

  test("plans are built fast enough") {
    val queries = List(foo1, foo2, socnet1, socnet9, qmul1)
    plan(NUMBER_OF_RUNS)(warmup)

    queries.foreach(assertIsPlannedTimely)
  }

  def assertIsPlannedTimely(query: String) = {
    val result = plan(NUMBER_OF_RUNS)(query)

    withClue("Planning of the first query took too long:\n" + result.description) {
      result.prepareAndPlanFirstNanos shouldBe <(3.second.toNanos)
    }

    withClue("Median planning took too long:\n" + result.description) {
      result.prepareAndPlanMedianNanos shouldBe <(1.5.second.toNanos)
    }
  }

  def plan(times: Int)(query: String): PlanningResult = {
    val prepareAndPlanTimes = (1 to times).map(i => plan(query)).toArray

    val totalTimes = prepareAndPlanTimes.map(t => t._1 + t._2)
    val prepareTimes = prepareAndPlanTimes.map(_._1)
    val planTimes = prepareAndPlanTimes.map(_._2)

    val (prepareFirst, planFirst) = prepareAndPlanTimes(0)

    val totalMedian = median(totalTimes)
    val prepareMedian = median(prepareTimes)
    val planMedian = median(planTimes)

    val description =
      s"""
         |* Planned query:\n\n${indent(query)}\n
         |  * First prepare time: ${ms(prepareFirst)}
         |  * First plan time: ${ms(planFirst)}
         |  * First total time: ${ms(prepareFirst + planFirst)}
         |  * Number of runs: $times
         |  * Median prepare time: ${ms(prepareMedian)}
         |  * Median planning time: ${ms(planMedian)}
         |  * Median total time: ${ms(totalMedian)}
         |
         |  * Prepare times: ${ms(prepareTimes)}
         |  * Plan times: ${ms(planTimes)}
         |  * Total times: ${ms(totalTimes)}
         """.stripMargin

    PlanningResult(description, totalTimes(0), totalMedian)
  }

  def ms(valueNanos: Long): String = valueNanos.nanos.toMillis + "ms"

  def ms(valuesNanos: Seq[Long]): String = valuesNanos.map(ms).mkString(", ")

  def median(values: Seq[Long]): Long = {
    val sorted = values.sorted
    val mid = sorted.size / 2
    if (sorted.size % 2 != 0)
      sorted(mid)
    else
      (sorted(mid - 1) + sorted(mid)) / 2
  }

  def indent(text: String, indent: String = "    ") =
    indent + text.replace("\n", s"\n$indent")

  def plan(query: String): (Long, Long) = {
    val compiler = createCurrentCompiler
    val (preparedSyntacticQueryTime, preparedSyntacticQuery) = measure(compiler.prepareSyntacticQuery(query, query, devNullLogger))
    val planTime = graph.inTx {
      val (semanticTime, semanticQuery) = measure(compiler.prepareSemanticQuery(preparedSyntacticQuery, devNullLogger, planContext, None, CompilationPhaseTracer.NO_TRACING))
      val (planTime, _) = measure(compiler.executionPlanBuilder.build(planContext, semanticQuery))
      planTime + semanticTime
    }
    (preparedSyntacticQueryTime, planTime)
  }

  def measure[T](f: => T): (Long, T) = {
    val start = System.nanoTime()
    val result = f
    val stop = System.nanoTime()
    (stop - start, result)
  }

  def createCurrentCompiler = {
    CypherCompilerFactory.costBasedCompiler(
      graph = graph,
      CypherCompilerConfiguration(
        queryCacheSize = 1,
        statsDivergenceThreshold = DEFAULT_STATISTICS_DIVERGENCE_THRESHOLD,
        queryPlanTTL = DEFAULT_QUERY_PLAN_TTL,
        useErrorsOverWarnings = false,
        idpMaxTableSize = 128,
        idpIterationDuration = 1000,
        errorIfShortestPathFallbackUsedAtRuntime = false,
        nonIndexedLabelWarningThreshold = 10000L
      ),
      clock = CLOCK,
      structure = GeneratedQueryStructure,
      monitors = new WrappedMonitors3_1(kernelMonitors),
      logger = DEV_NULL,
      rewriterSequencer = RewriterStepSequencer.newPlain,
      plannerName = Some(IDPPlannerName),
      runtimeName = Some(CompiledRuntimeName),
      updateStrategy = None,
      typeConverter = IdentityTypeConverter
    )
  }

  object DEV_NULL extends InfoLogger {
    def info(message: String){}
  }

  case class PlanningResult(description: String, prepareAndPlanFirstNanos: Long, prepareAndPlanMedianNanos: Long)

}
