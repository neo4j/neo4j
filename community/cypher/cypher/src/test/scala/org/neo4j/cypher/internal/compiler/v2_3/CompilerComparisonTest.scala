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
package org.neo4j.cypher.internal.compiler.v2_3

import java.io.{File, FileWriter}
import java.text.NumberFormat
import java.util.{Date, Locale}

import org.neo4j.cypher.internal.compatibility.{EntityAccessorWrapper2_3, WrappedMonitors2_3}
import org.neo4j.cypher.internal.compiler.v2_3.executionplan._
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.InternalPlanDescription
import org.neo4j.cypher.internal.compiler.v2_3.planner._
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical._
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans.rewriter.LogicalPlanRewriter
import org.neo4j.cypher.internal.compiler.v2_3.tracing.rewriters.RewriterStepSequencer
import org.neo4j.cypher.internal.frontend.v2_3.ast.Statement
import org.neo4j.cypher.internal.frontend.v2_3.parser.CypherParser
import org.neo4j.cypher.internal.spi.v2_3.{TransactionBoundPlanContext, TransactionBoundQueryContext}
import org.neo4j.cypher.{ExecutionEngineFunSuite, NewPlannerTestSupport, QueryStatisticsTestSupport}
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.factory.GraphDatabaseFactory
import org.neo4j.helpers.Clock
import org.neo4j.kernel.GraphDatabaseAPI
import org.neo4j.kernel.impl.core.NodeManager
import org.neo4j.kernel.monitoring.{Monitors => KernelMonitors}

import scala.xml.Elem

class CompilerComparisonTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport with NewPlannerTestSupport {

  val monitorTag = "CompilerComparison"
  val clock = Clock.SYSTEM_CLOCK
  val config = CypherCompilerConfiguration(
    queryCacheSize = 100,
    statsDivergenceThreshold = 0.5,
    queryPlanTTL = 1000,
    useErrorsOverWarnings = false,
    idpMaxTableSize = 128,
    idpIterationDuration = 1000,
    nonIndexedLabelWarningThreshold = 10000
  )

  val compilers = Seq[(String, GraphDatabaseService => CypherCompiler)](
    "legacy (rule)" -> legacyCompiler,
    "ronja (greedy)" -> ronjaCompiler(GreedyPlannerName),
    "ronja (idp)" -> ronjaCompiler(IDPPlannerName),
    "ronja (dp)" -> ronjaCompiler(DPPlannerName)
  )

  val queriesByDataSet: Map[(String, String), Map[String, String]] = Map.empty

  val qmul = "qmul" -> "qmul-2.2" ->
    Map(
      "QMUL1" ->
        "MATCH (a:Person)-->(m)-[r]->(n)-->(a) WHERE a.uid IN ['1195630902', '1457065010'] AND HAS(m.location_lat) AND HAS(n.location_lat) RETURN count(r)"
    )

  val access_control = "access_control" -> "access-control-2.2" ->
    Map(
      "Q1 - More complex should get accessible companies for admin" ->
        "MATCH (admin:Administrator {name:'Administrator-512'}) MATCH (admin)-[:MEMBER_OF]->()-[:ALLOWED_INHERIT]->(company) WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->(company)) RETURN company.name AS company UNION MATCH (admin:Administrator {name:'Administrator-512'}) MATCH (admin)-[:MEMBER_OF]->()-[:ALLOWED_INHERIT]->()<-[:CHILD_OF]-(company) WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->()<-[:CHILD_OF]-(company)) RETURN company.name AS company UNION MATCH (admin:Administrator {name:'Administrator-512'}) MATCH (admin)-[:MEMBER_OF]->()-[:ALLOWED_DO_NOT_INHERIT]->(company) RETURN company.name AS company",

      "Q3 - More complex should find accessible accounts for admin and company" ->
        "MATCH (admin:Administrator {name:'Administrator-632'}),(company:Company {name:'Company-12846'}) MATCH (admin)-[:MEMBER_OF]->(group)-[:ALLOWED_INHERIT]->(company)<-[:WORKS_FOR]-(employee)-[:HAS_ACCOUNT]->(account) WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->(company)) RETURN account.name AS account UNION MATCH (admin:Administrator {name:'Administrator-632'}),(company:Company {name:'Company-12846'}) MATCH (admin)-[:MEMBER_OF]->(group)-[:ALLOWED_INHERIT]->(company)<-[:CHILD_OF]-(subcompany)<-[:WORKS_FOR]-(employee)-[:HAS_ACCOUNT]->(account) WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->()<-[:CHILD_OF]-(subcompany)) RETURN account.name AS account UNION MATCH (admin:Administrator {name:'Administrator-632'}),(company:Company {name:'Company-12846'}) MATCH (admin)-[:MEMBER_OF]->(group)-[:ALLOWED_DO_NOT_INHERIT]->(company)<-[:WORKS_FOR]-(employee)-[:HAS_ACCOUNT]->(account) RETURN account.name AS account",

      "Q4 - More complex should find accessible accounts for admin and any matching company" ->
        "MATCH (admin:Administrator {name:'Administrator-332'}) MATCH (admin)-[:MEMBER_OF]->(group)-[:ALLOWED_INHERIT]->(company:Company)<-[:WORKS_FOR]-(employee)-[:HAS_ACCOUNT]->(account) WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->(company)) RETURN account.name AS account UNION MATCH (admin:Administrator {name:'Administrator-332'}) MATCH (admin)-[:MEMBER_OF]->(group)-[:ALLOWED_INHERIT]->(company:Company) <-[:CHILD_OF]-(subcompany)<-[:WORKS_FOR]-(employee)-[:HAS_ACCOUNT]->(account) WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->()<-[:CHILD_OF]-(subcompany)) RETURN account.name AS account UNION MATCH (admin:Administrator {name:'Administrator-332'}) MATCH (admin)-[:MEMBER_OF]->(group)-[:ALLOWED_DO_NOT_INHERIT]->(company:Company)<-[:WORKS_FOR]-(employee)-[:HAS_ACCOUNT]->(account) RETURN account.name AS account"
    )

  val geneated_music = "generated-music" -> "generated-music" ->
    Map(
      "The Dreaded Query No 31" ->
        "match (a1:Artist {name: 'Artist-544'})-[:PERFORMED_AT]->(:Concert)<-[:PERFORMED_AT]-(a2:Artist) match (a1)-[:SIGNED_WITH]->(corp:Company)<-[:SIGNED_WITH]-(a2) return a2, count(*) order by count(*) DESC"
    )

  val ldbc = "ldbc" -> "target/ldbc_data/" ->
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
    )

  val music = "music" -> "target/benchmarkdb/" ->
    Map(
      "music #1" ->
        """MATCH (t:Track)-[:APPEARS_ON]->(a:Album)
          |RETURN *""".stripMargin,

      "music #2" ->
        """MATCH (t:Track)-[:APPEARS_ON]->(a:Album)
          |  WHERE id(a) = 8564
          |RETURN *""".stripMargin,

      "music #3" ->
        """MATCH (a:Artist)-[:CREATED]->(al:Album)
          | WHERE al.releasedIn = 1979
          |RETURN *""".stripMargin,

      "music #4" ->
        """MATCH (t1:Track)--(al:Album)--(t2:Track)
          | WHERE t1.duration = 61 AND t2.duration = 68
          |RETURN *""".stripMargin,

      "music #5" ->
        """MATCH (t:Track)--(al:Album)--(a:Artist)
          | WHERE t.duration = 61 AND a.gender = 'male'
          |RETURN *""".stripMargin,

      "music #6" ->
        """MATCH (al:Album), (a:Artist)
          | WHERE al.title = a.name
          |RETURN *""".stripMargin,

      "music #7" ->
        """MATCH (a)-[r]->(b)
          | WHERE id(r) = 1029
          |RETURN *""".stripMargin,

      "music #8" ->
        """MATCH (t:Track)
          |RETURN count(t)""".stripMargin,

      "music #9" ->
        """MATCH (a:Album)
          |  WHERE (a.releasedIn = 1989 OR a.title = 'Album-5')
          |RETURN *""".stripMargin,

      "music #10" ->
        """MATCH (a:Album)
          |  WHERE (a.releasedIn = 1989 AND a.title = 'Album-5') OR (a.releasedIn = 2000)
          |RETURN *""".stripMargin,

      "music #11" ->
        """MATCH (a)-[:APPEARS_ON]->(b)<-[:CREATED]-(c)
          |RETURN *""".stripMargin,

      "music #12" ->
        """MATCH (a:Artist)-[:WORKED_WITH]->(b:Artist)-[:WORKED_WITH]->(c:Artist)-[:WORKED_WITH]->(a)
          |RETURN *""".stripMargin,

      "music #13" ->
        """MATCH (al:Album)
          |RETURN (:Artist)-[:CREATED]->(al)<-[:APPEARS_ON]-(:Track)""".stripMargin,

      "music #14" ->
        """MATCH (a:Artist)
          |OPTIONAL MATCH (b:Artist)-[:WORKED_WITH]->(a)
          |RETURN *""".stripMargin,

      "music #16" ->
        """MATCH (al:Album)
          |  WHERE (:Artist)-[:CREATED]->(al)<-[:APPEARS_ON]-(:Track)
          |RETURN *""".stripMargin,

      "music #18" ->
        """MATCH (al:Album)
          |WHERE (:Artist)-[:CREATED]->(al) AND (al)<-[:APPEARS_ON]-(:Track)
          |RETURN *""".stripMargin,

      "music #19" ->
        """MATCH (artist:Artist)
          |WHERE NOT (artist)-[:CREATED]->(:Album {relasedIn: 1975})
          |RETURN *""".stripMargin,

      "music #20" ->
        """MATCH (t:Track)
          |WHERE t.duration IN [60, 61, 62, 63, 64]
          |RETURN *""".stripMargin,

      "music #21" ->
        """MATCH (a:Artist:Person)
          |RETURN *""".stripMargin,

      "music #22" ->
        """MATCH (t:Track)-[:APPEARS_ON]->(a:Album)
          |WHERE t.duration IN [60, 61, 62, 63, 64]
          |RETURN *""".stripMargin,

      "music #24" ->
        """MATCH (a1:Artist)-[:WORKED_WITH * 4]->(a2:Artist)
          |WHERE id(a1) = 462
          |RETURN DISTINCT a2""".stripMargin,

      "music #25" ->
        """MATCH p = shortestPath( (a1:Artist)-[:WORKED_WITH]->(a2:Artist) )
          |WHERE id(a1) = 349 AND id(a2) = 156
          |RETURN p""".stripMargin,

      "music #26" ->
        """MATCH (corp:Company)<-[:SIGNED_WITH]-(a1:Artist)-[:PERFORMED_AT]->(c:Concert)-[:IN]->(v:Venue)
          |MATCH (corp)<-[:SIGNED_WITH]-(a2:Artist)-[:PERFORMED_AT]->(c)
          |RETURN a1, a2, v""".stripMargin,

      "music #27" ->
        """MATCH (a:Album)
          |WHERE a.title = 'Album-6651' OR a.releasedIn = 1960
          |RETURN *""".stripMargin,

      "music #28" ->
        """MATCH (a:Artist)-[:WORKED_WITH*..5 { year: 1991 }]->(b:Artist)
          |RETURN *""".stripMargin,

      "music #29" ->
        """MATCH (a:Artist)-[:CREATED]->(al:Album)
          |WHERE a.gender = 'male'
          |RETURN *""".stripMargin
    )

  private def compilerNames = compilers.map {
    case (compilerName, _) => compilerName
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

  private val rewriterSequencer = RewriterStepSequencer.newPlain _

  private def ronjaCompiler(plannerName: CostBasedPlannerName, metricsFactoryInput: MetricsFactory = SimpleMetricsFactory)(graph: GraphDatabaseService): CypherCompiler = {
    val kernelMonitors = new KernelMonitors()
    val monitors = new WrappedMonitors2_3(kernelMonitors)
    val parser = new CypherParser
    val checker = new SemanticChecker
    val rewriter = new ASTRewriter(rewriterSequencer)
    val planBuilderMonitor = monitors.newMonitor[NewLogicalPlanSuccessRateMonitor](monitorTag)
    val metricsFactory = CachedMetricsFactory(metricsFactoryInput)
    val queryPlanner = new DefaultQueryPlanner(LogicalPlanRewriter(rewriterSequencer))
    val planner = CostBasedPipeBuilderFactory.create(
      monitors = monitors,
      metricsFactory = metricsFactory,
      plannerName = Some(plannerName),
      rewriterSequencer = rewriterSequencer,
      queryPlanner = queryPlanner,
      runtimeBuilder = InterpretedRuntimeBuilder(InterpretedPlanBuilder(clock, monitors)),
      semanticChecker = checker,
      useErrorsOverWarnings = false,
      idpMaxTableSize = 128,
      idpIterationDuration = 1000
    )
    val pipeBuilder = new SilentFallbackPlanBuilder(new LegacyExecutablePlanBuilder(monitors, rewriterSequencer), planner,
                                                    planBuilderMonitor)
    val nodeManager = graph.asInstanceOf[GraphDatabaseAPI].getDependencyResolver.resolveDependency(classOf[NodeManager])
    val execPlanBuilder =
      new ExecutionPlanBuilder(graph, new EntityAccessorWrapper2_3(nodeManager), config, clock, pipeBuilder)
    val planCacheFactory = () => new LRUCache[Statement, ExecutionPlan](100)
    val cacheHitMonitor = monitors.newMonitor[CypherCacheHitMonitor[Statement]](monitorTag)
    val cacheFlushMonitor =
      monitors.newMonitor[CypherCacheFlushingMonitor[CacheAccessor[Statement, ExecutionPlan]]](monitorTag)
    val cache = new MonitoringCacheAccessor[Statement, ExecutionPlan](cacheHitMonitor)

    new CypherCompiler(parser, checker, execPlanBuilder, rewriter, cache, planCacheFactory, cacheFlushMonitor, monitors)
  }

  private def legacyCompiler(graph: GraphDatabaseService): CypherCompiler = {
    val kernelMonitors = new KernelMonitors()
    val monitors = new WrappedMonitors2_3(kernelMonitors)
    val parser = new CypherParser
    val checker = new SemanticChecker
    val rewriter = new ASTRewriter(rewriterSequencer)
    val pipeBuilder = new LegacyExecutablePlanBuilder(monitors, rewriterSequencer)
    val nodeManager = graph.asInstanceOf[GraphDatabaseAPI].getDependencyResolver.resolveDependency(classOf[NodeManager])
    val execPlanBuilder =
      new ExecutionPlanBuilder(graph, new EntityAccessorWrapper2_3(nodeManager), config, clock, pipeBuilder)
    val planCacheFactory = () => new LRUCache[Statement, ExecutionPlan](100)
    val cacheHitMonitor = monitors.newMonitor[CypherCacheHitMonitor[Statement]](monitorTag)
    val cacheFlushMonitor =
      monitors.newMonitor[CypherCacheFlushingMonitor[CacheAccessor[Statement, ExecutionPlan]]](monitorTag)
    val cache = new MonitoringCacheAccessor[Statement, ExecutionPlan](cacheHitMonitor)

    new CypherCompiler(parser, checker, execPlanBuilder, rewriter, cache, planCacheFactory, cacheFlushMonitor, monitors)
  }

  case class QueryExecutionResult(compiler: String, dbHits: Option[Long], plan: InternalPlanDescription) {
    def toXml(q: String, min: Long): Elem = {
      val id = s"${q}_$compiler"
      val cell_id = s"${id}_cell"
      val execute = s"javascript:setPlanTo('$q', '$compiler');"
      val resultClass =
        if (dbHits == Some(min)) "good"
        else if (dbHits.forall(s => s > min * 2)) "bad"
        else "normal"
      <td id={cell_id} class={resultClass}>
        <a href={execute}>
          {format(dbHits)}
        </a> <div id={id} style="display: none;">
        {plan.toString}
      </div>
      </td>
    }
  }

  case class QueryResult(queryName: String, executionResults: Seq[QueryExecutionResult], queryText: String) {
    def toXml: Elem = {
      val min = executionResults.map(_.dbHits).flatten.min

      <tr>
        <td>
          {queryName}<div id={queryName} style="display: none;">
          {queryText}
        </div>
        </td>{executionResults.map(_.toXml(queryName, min))}
      </tr>
    }
  }

  case class DataSetResults(name: String, results: Seq[QueryResult]) {
    def toXml: Seq[Elem] = results.sortBy(_.queryName).map(_.toXml)
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
        QueryResult(queryName, results, queryText)
      }
      DataSetResults(dataSetName, queryResults.toSeq)
    }
    finally
      db.shutdown()
  }).toSeq


  ignore("This test is a utility runner.  It should be ignored.") {
    val script = scala.xml.Unparsed( """var previousCell = null;
                                       |function setPlanTo(query, compiler) {
                                       |  var queryText = document.getElementById(query).innerHTML.
                                       |    replace(/^["\s]+|["\s]+$/g,'').
                                       |    replace(/\s+(optional match|match|where|retur)/gi,"</p><p>$1").
                                       |    replace(/\,\s+([\w:()]+)\-/g,",<br/>$1-");
                                       |  document.getElementById("query").innerHTML = queryText;

                                       |  var planId = query.concat("_", compiler);
                                       |  var plan = document.getElementById(planId).innerHTML.replace(/^["\s]+|["\s]+$/g,'');
                                       |  document.getElementById("planDescription").innerHTML = plan;
                                       |
                                       |  if (previousCell != null) {
                                       |    previousCell.className = previousCell.className.replace( /(?:^|\s)selected(?!\S)/g , '' )
                                       |  }
                                       |  var cell = document.getElementById(planId+"_cell");
                                       |  previousCell = cell;
                                       |  if(cell != null) {
                                       |    cell.className += " selected";
                                       |  }
                                       |}""".stripMargin)
    val styles =
      """
        |body {
        |  border: none;
        |  width: 100%;
        |  margin: 0;
        |  padding: 10px;
        |}
        |table {
        |  border: none;
        |  border-spacing: 1;
        |  width: 200pt;
        |}
        |th {
        |  padding: 5pt;
        |  margin: 0px;
        |  border: none;
        |  background: #888;
        |  color: #fff;
        |  width: 50pt;
        |  height: 30pt;
        |}
        |td {
        |  padding: 5pt;
        |  margin: 0;
        |  border: solid #fff 2px;
        |  height: 20pt;
        |}
        |tr {
        |  margin: 0;
        |  padding: 0;
        |}
        |.good, .normal, .bad, .neutral {
        |  text-align: right;
        |}
        |.good {
        |  background: #4f4;
        |  border: solid #4f4 2px;
        |}
        |.normal {
        |  background: #fff;
        |}
        |.bad {
        |  background: #fff;
        |  border: solid #f44 2px;
        |}
        |.neutral {
        |  background: #eee;
        |  border: solid #eee 2px;
        |}
        |.selected {
        |  background: #eef;
        |  border: solid #00f 2px;
        |}
        |/* Source: http://snipplr.com/view/10979/css-cross-browser-word-wrap */
        |.wordwrap {
        |  white-space: pre-wrap;      /* CSS3 */
        |  white-space: -moz-pre-wrap; /* Firefox */
        |  white-space: -pre-wrap;     /* Opera <7 */
        |  white-space: -o-pre-wrap;   /* Opera 7 */
        |  word-wrap: break-word;      /* IE */
        |  padding-left: 2em;
        |  text-indent: -1em;
        |}
        |div#query, div#planDescription {
        |  display: block;
        |  font-family: monospace;
        |  border: none;
        |  padding: 5pt;
        |  min-width: 800px;
        |}
        |div#planDescription {
        |  white-space: pre;
        |  overflow:scroll;
        |}
        |div#query {
        |  color: #fff;
        |  background: #888;
        |  font-weight: bold;
        |  padding: 10pt;
        |  width: 800px;
        |  padding-left: 2em;
        |}
        |
      """.stripMargin
    val output =
      <html>
        <head>
          <title>Compiler report
            {new Date().toGMTString}
          </title>
          <script>
            {script}
          </script>
          <style>
            {styles}
          </style>
        </head>
        <body>
          <table>
            <tr>
              <td valign="top">
                {results}
              </td> <td valign="top">
              <div id="query" class="wordwrap"></div>
              <div id="planDescription"></div>
            </td>
            </tr>
          </table>
        </body>
      </html>

    val report = new File("compiler-report.html")
    val writer = new FileWriter(report)
    writer.write(output.toString())
    writer.close()
    println(s"report written to ${report.getAbsolutePath}")
  }

  private def runQueryWith(query: String, compiler: CypherCompiler, db: GraphDatabaseAPI): (List[Map[String, Any]], InternalExecutionResult) = {
    val (plan: ExecutionPlan, parameters) = db.withTx {
      tx =>
        val planContext = new TransactionBoundPlanContext(db.statement, db)
        compiler.planQuery(query, planContext, devNullLogger)
    }

    db.withTx {
      tx =>
        val queryContext = new TransactionBoundQueryContext(db, tx, true, db.statement)(indexSearchMonitor)
        val result = plan.run(queryContext, statement, ProfileMode, parameters)
        (result.toList, result)
    }
  }
}
