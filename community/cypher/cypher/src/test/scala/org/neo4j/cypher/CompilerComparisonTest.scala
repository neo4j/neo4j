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

import java.io.FileWriter
import java.text.NumberFormat
import java.util.{Locale, Date}

import org.neo4j.cypher.internal.compiler.v2_2.executionplan._
import org.neo4j.cypher.internal.compiler.v2_2.parser.{CypherParser, ParserMonitor}
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.Metrics.{CardinalityModel, PredicateSelectivityCombiner}
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.cardinality.combinePredicates
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.{CachedMetricsFactory, MetricsFactory, SimpleMetricsFactory}
import org.neo4j.cypher.internal.compiler.v2_2.planner.{Planner, PlanningMonitor, SemanticTable}
import org.neo4j.cypher.internal.compiler.v2_2.spi.{PlanContext, QueriedGraphStatistics, GraphStatistics}
import org.neo4j.cypher.internal.compiler.v2_2.{Monitors, _}
import org.neo4j.cypher.internal.spi.v2_2.{TransactionBoundPlanContext, TransactionBoundQueryContext}
import org.neo4j.cypher.internal.{LRUCache, Profiled}
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.factory.GraphDatabaseFactory
import org.neo4j.kernel.GraphDatabaseAPI
import org.neo4j.kernel.monitoring.{Monitors => KernelMonitors}

import scala.xml.Elem

class CompilerComparisonTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport with NewPlannerTestSupport {

  val monitorTag = "APA"

  def ronjaCompiler(metricsFactoryInput: MetricsFactory = SimpleMetricsFactory)(graph: GraphDatabaseService): CypherCompiler = {
    val kernelMonitors = new KernelMonitors()
    val monitors = new Monitors(kernelMonitors)
    val parser = new CypherParser(monitors.newMonitor[ParserMonitor[ast.Statement]](monitorTag))
    val checker = new SemanticChecker(monitors.newMonitor[SemanticCheckMonitor](monitorTag))
    val rewriter = new ASTRewriter(monitors.newMonitor[AstRewritingMonitor](monitorTag))
    val planBuilderMonitor = monitors.newMonitor[NewLogicalPlanSuccessRateMonitor](monitorTag)
    val planningMonitor = monitors.newMonitor[PlanningMonitor](monitorTag)
    val metricsFactory = CachedMetricsFactory(metricsFactoryInput)
    val planner = new Planner(monitors, metricsFactory, planningMonitor)
    val pipeBuilder = new LegacyVsNewPipeBuilder(new LegacyPipeBuilder(monitors), planner, planBuilderMonitor)
    val execPlanBuilder = new ExecutionPlanBuilder(graph, pipeBuilder)
    val planCacheFactory = () => new LRUCache[PreparedQuery, ExecutionPlan](100)
    val cacheMonitor = monitors.newMonitor[AstCacheMonitor](monitorTag)
    val cache = new MonitoringCacheAccessor[PreparedQuery, ExecutionPlan](cacheMonitor)

    new CypherCompiler(parser, checker, execPlanBuilder, rewriter, cache, planCacheFactory, cacheMonitor, monitors)
  }

  val queriesByDataSet = Map[(String, String), Map[String, String]](
    "ldbc" -> "/Users/ata/dev/neo/ronja-benchmarks/target/ldbc_data/" ->
    Map(
      "LDBC #5" ->
      """MATCH (person:Person {id: 3298534944679 })-[:KNOWS*1..2]-(friend:Person)<-[membership:HAS_MEMBER]-(forum:Forum)
        |  WHERE membership.joinDate > "2010-09-04T05:01:10Z"
        |MATCH (friend)<-[:HAS_CREATOR]-(post:Post)<-[:CONTAINER_OF]-(forum)
        |RETURN forum.title AS forumx, count(post) AS postCount
        |ORDER BY postCount DESC;""".stripMargin,

      "LDBC #2" ->
      """MATCH (:Person {id: 48215})-[:KNOWS]-(friend:Person)<-[:HAS_CREATOR]-(post:Post)
        |  WHERE post.creationDate <= "2010-04-26T08:18:49Z"
        |RETURN friend.id AS personId,
        |       friend.firstName AS personFirstName,
        |       friend.lastName AS personLastName,
        |       post.id AS postId,
        |       post.content AS postContent,
        |       post.creationDate AS postDate
        |ORDER BY postDate DESC
        |LIMIT 50""".stripMargin,

      "LDBC #4" ->
      """MATCH (person:Person {id: 4398046519427})-[:KNOWS]-(friend:Person)<-[:HAS_CREATOR]-(post:Post)-[HAS_TAG]->(tag:Tag)
        |  WHERE post.creationDate >= "2010-11-20T13:18:43Z" AND post.creationDate <= "2010-12-07T07:15:53Z"
        |WITH DISTINCT tag, collect(tag) AS tags
        |RETURN tag.name AS tagName, length(tags) AS tagCount
        |ORDER BY tagCount DESC
        |LIMIT 50""".stripMargin,

      "LDBC #6" ->
      """MATCH (person:Person {id: 5497558159594})-[:KNOWS*1..2]-(:Person)<-[:HAS_CREATOR]-(post:Post)-[:HAS_TAG]->(:Tag {name: "Mundian_To_Bach_Ke"})
        |WITH DISTINCT post
        |MATCH (post)-[:HAS_TAG]->(tag:Tag)
        |  WHERE NOT(tag.name="Mundian_To_Bach_Ke")
        |RETURN tag.name AS tagName, count(tag) AS tagCount
        |ORDER BY tagCount DESC
        |LIMIT 50""".stripMargin,

      "LDBC #13" ->
      """MATCH path = shortestPath((person1:Person {id: 2199023260872})-[:KNOWS]-(person2:Person {id: 3298534890133}))
        |RETURN length(path) AS pathLength""".stripMargin,

      "LDBC #3" ->
      """MATCH (person:Person {id: 1099511630017})-[:KNOWS*1..2]-(friend:Person)<-[:HAS_CREATOR]-(postX:Post)-[:IS_LOCATED_IN]->(countryX:Country)
        |  WHERE countryX.name="Tunisia" AND postX.creationDate>="" AND postX.creationDate<="2010-09-24T16:20:15Z"
        |WITH friend, count(DISTINCT postX) AS xCount
        |MATCH (friend)<-[:HAS_CREATOR]-(postY:Post)-[:IS_LOCATED_IN]->(countryY:Country {name: "Republic_of_Macedonia"})
        |  WHERE postY.creationDate>={min_date} AND postY.creationDate<={max_date}
        |WITH friend.firstName + ' ' + friend.lastName AS friendName , xCount, count(DISTINCT postY) AS yCount
        |RETURN friendName, xCount, yCount, xCount + yCount AS xyCount
        |ORDER BY xyCount DESC LIMIT 50""".stripMargin
    ),
    "music" -> "/Users/ata/dev/neo/ronja-benchmarks/target/benchmarkdb/" ->
    Map(
      "music #1" ->
      """MATCH (t:Track)-[:APPEARS_ON]->(a:Album)
        |WHERE t.duration IN [60, 61, 62, 63, 64]
        |RETURN *""".stripMargin
    )
  )

  val compilers = Seq[(String, GraphDatabaseService => CypherCompiler)](
    "assume Independence" -> ronjaCompiler(customMetrics(combinePredicates.assumeIndependence)),
    "assume Dependence" -> ronjaCompiler(customMetrics(combinePredicates.assumeDependence))
  )

  private def compilerNames = compilers.map {
    case (compilerName, _) => compilerName
  }

  private def compilerList = {
    <h3>Compilers</h3>
      <ul>
        { compilerNames.map( compiler => <li>{compiler}</li>) }
      </ul>
  }

  private def queryList = {
    <h3>Queries</h3>
      <table border="1">
        <tr>
          <th>Data Set</th>
          <th>Query Name</th>
          <th>Query String</th>
        </tr>{queriesByDataSet.map {
        case ((dataset, _), queries) =>
          queries.toSeq.map {
            case (name, query) =>
              <tr>
                <td>{dataset}</td>
                <td>{name}</td>
                <td><pre><code>{query}</code></pre></td>
              </tr>
          }
      }}
      </table>
  }

  private def results = {
    <h3>Db</h3>
      <table border="1">
        <tr>
          <th>Query Name</th>{compilerNames.map(name => <th>
          {name}
        </th>)}
        </tr>{executionResults.map(_.toXml)}
      </table>
  }

  private def format(in: Option[Long]) = in.map(l => NumberFormat.getNumberInstance(Locale.US).format(l)).getOrElse("???")

  case class QueryExecutionResult(compiler: String, dbHits: Option[Long], plan: PlanDescription) {
    def toXml(q: String, min: Long): Elem = {
      val id = s"${q}_$compiler"
      val execute = s"javascript:setPlanTo('$id');"
      val color = if(dbHits == Some(min)) "#00ff00" else "#ffffff"
      <td bgcolor={color}><a href={execute}>{format(dbHits)}</a><div id={id} style="display: none;">{plan.toString}</div></td>
    }
  }

  case class QueryResult(queryName: String, executionResults: Seq[QueryExecutionResult]) {
    def toXml: Elem = {
      val min = executionResults.map(_.dbHits).flatten.min

      <tr><td>{queryName}</td>{executionResults.map(_.toXml(queryName, min))}</tr>
    }
  }

  case class DataSetResults(name: String, results: Seq[QueryResult]) {
    def toXml: Seq[Elem] = results.map(_.toXml)
  }

  private def executionResults: Seq[DataSetResults] = (for ((dataSet, queries) <- queriesByDataSet) yield {
    val (dataSetName, dataSetDir) = dataSet
    val db = new GraphDatabaseFactory().newEmbeddedDatabase(dataSetDir).asInstanceOf[GraphDatabaseAPI]
    try {
      val queryResults = for ((queryName, queryText) <- queries) yield {
        val results = for ((compilerName, compilerCreator) <- compilers) yield {
          val compiler = compilerCreator(db)
          val (_, result) = runQueryWith(queryText, compiler, db)
          QueryExecutionResult(compilerName, result.executionPlanDescription().totalDbHits, result.executionPlanDescription())
        }
        QueryResult(queryName, results)
      }
      DataSetResults(dataSetName, queryResults.toSeq)
    }
    finally
      db.shutdown()
  }).toSeq


  ignore("Compworking test - should not be running on build servers") {
    val script = scala.xml.Unparsed("""function setPlanTo(planId) {
                                      |  var plan = document.getElementById(planId).innerHTML;
                                      |  document.getElementById("planDescription").innerHTML = plan;
                                      |}""".stripMargin)
    val output =
      <html>
        <head>
          <title>Compiler report {new Date().toGMTString}
          </title>
          <script>{script}</script>
        </head>
        <body>
          {compilerList}<br/>
          {queryList}<br/>
          <table>
            <tr><td valign="top">{results}</td><td><pre><code><div id="planDescription">PLAN</div></code></pre></td></tr>
          </table>
        </body>
      </html>

    val writer = new FileWriter("/Users/ata/Desktop/compiler-report2.html")
    writer.write(output.toString())
    writer.close()

  }

  trait RealStatistics extends PlanContext {
    def gdb: GraphDatabaseService

    lazy val _statistics: GraphStatistics = {
      val db = gdb.asInstanceOf[GraphDatabaseAPI]
      val queryCtx = new TransactionBoundQueryContext(db, null, true, db.statement)
      new QueriedGraphStatistics(gdb, queryCtx)
    }

    override def statistics: GraphStatistics = _statistics
  }

  private def runQueryWith(query: String, compiler: CypherCompiler, db: GraphDatabaseAPI): (List[Map[String, Any]], InternalExecutionResult) = {
    val (plan, parameters) = db.withTx {
      tx =>
        val kernelAPI = db.getDependencyResolver.resolveDependency(classOf[org.neo4j.kernel.api.KernelAPI])
        val planContext = new TransactionBoundPlanContext(db.statement, kernelAPI, db) with RealStatistics
        compiler.planQuery(query, planContext, Profiled)
    }

    db.withTx {
      tx =>
        val queryContext = new TransactionBoundQueryContext(db, tx, true, db.statement)
        val result = plan.execute(queryContext, parameters)
        (result.toList, result)
    }
  }

  private def customMetrics(selectivity: PredicateSelectivityCombiner) = new MetricsFactory {

    def newSelectivity() = selectivity

    def newCardinalityEstimator(statistics: GraphStatistics, selectivity: PredicateSelectivityCombiner, semanticTable: SemanticTable) =
      SimpleMetricsFactory.newCardinalityEstimator(statistics, selectivity, semanticTable)

    def newCostModel(cardinality: CardinalityModel) = SimpleMetricsFactory.newCostModel(cardinality)

  }
}
