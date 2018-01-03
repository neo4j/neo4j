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
package org.neo4j.cypher.docgen

import java.io.{ByteArrayOutputStream, File, PrintWriter, StringWriter}
import java.util.concurrent.TimeUnit

import org.junit.{After, Before}
import org.neo4j.cypher.CypherException
import org.neo4j.cypher.example.JavaExecutionEngineDocTest
import org.neo4j.cypher.export.{DatabaseSubGraph, SubGraphExporter}
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.InternalExecutionResult
import org.neo4j.cypher.internal.compiler.v2_3.prettifier.Prettifier
import org.neo4j.cypher.internal.helpers.GraphIcing
import org.neo4j.cypher.internal.frontend.v2_3.helpers.Eagerly
import org.neo4j.cypher.internal.{RewindableExecutionResult, ServerExecutionEngine}
import org.neo4j.cypher.javacompat.GraphImpl
import org.neo4j.graphdb._
import org.neo4j.graphdb.factory.{GraphDatabaseFactory, GraphDatabaseSettings}
import org.neo4j.graphdb.index.Index
import org.neo4j.helpers.Settings
import org.neo4j.kernel.GraphDatabaseAPI
import org.neo4j.kernel.impl.api.KernelStatement
import org.neo4j.kernel.impl.api.index.IndexingService
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingMode
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge
import org.neo4j.test.GraphDatabaseServiceCleaner.cleanDatabaseContent
import org.neo4j.test.{AsciiDocGenerator, GraphDescription, TestGraphDatabaseFactory}
import org.neo4j.tooling.GlobalGraphOperations
import org.neo4j.visualization.asciidoc.AsciidocHelper
import org.neo4j.visualization.graphviz.{AsciiDocStyle, GraphStyle, GraphvizWriter}
import org.neo4j.walk.Walker
import org.scalatest.junit.JUnitSuite

import scala.collection.JavaConverters._
import scala.reflect.ClassTag


trait DocumentationHelper extends GraphIcing {
  def generateConsole: Boolean
  def db: GraphDatabaseAPI

  def niceify(in: String): String = in.toLowerCase.replace(" ", "-")

  def simpleName: String = this.getClass.getSimpleName.replaceAll("Test", "").toLowerCase

  def createDir(folder: String): File = createDir(path, folder)

  def createDir(where: File, folder: String): File = {
    val dir = new File(where, niceify(folder))
    if (!dir.exists()) {
      dir.mkdirs()
    }
    dir
  }

  def createWriter(title: String, dir: File): PrintWriter = {
    new PrintWriter(new File(dir, niceify(title) + ".asciidoc"), "UTF-8")
  }

  def createCypherSnippet(query: String) = {
    val escapedQuery = query.trim().replace("\\", "\\\\")
    val prettifiedQuery = Prettifier(escapedQuery)
    AsciidocHelper.createCypherSnippetFromPreformattedQuery(prettifiedQuery, true)
  }

  def prepareFormatting(query: String): String = {
    val str = Prettifier(query.trim())
    if ((str takeRight 1) == ";") {
      str
    } else {
      str + ";"
    }
  }

  def dumpSetupQueries(queries: List[String], dir: File) {
    dumpQueries(queries, dir, simpleName + "-setup")
  }

  def dumpSetupConstraintsQueries(queries: List[String], dir: File) {
    dumpQueries(queries, dir, simpleName + "-setup-constraints")
  }

  def dumpPreparationQueries(queries: List[String], dir: File, testid: String) {
    dumpQueries(queries, dir, simpleName + "-" + niceify(testid) + ".preparation")
  }

  private def dumpQueries(queries: List[String], dir: File, testid: String): String = {
    if (queries.isEmpty) {
      ""
    } else {
      val queryStrings = queries.map(prepareFormatting)
      val output = AsciidocHelper.createCypherSnippetFromPreformattedQuery(queryStrings.mkString("\n"), true)
      AsciiDocGenerator.dumpToSeparateFile(dir, testid, output)
    }
  }

  val path: File = new File("target/docs/dev/ql/")

  val graphvizFileName = "cypher-" + simpleName + "-graph"

  def dumpGraphViz(dir: File, graphVizOptions: String): String = {
    emitGraphviz(dir, graphvizFileName, graphVizOptions)
  }

  def dumpPreparationGraphviz(dir: File, testid: String, graphVizOptions: String): String = {
    emitGraphviz(dir, simpleName + "-" + niceify(testid) + ".preparation-graph", graphVizOptions)
  }

  private def emitGraphviz(dir: File, testid: String, graphVizOptions: String): String = {
    val out = new ByteArrayOutputStream()
    val writer = new GraphvizWriter(getGraphvizStyle)

    db.inTx {
      writer.emit(out, Walker.fullGraph(db))
    }

    val graphOutput = """["dot", "%s.svg", "neoviz", "%s"]
----
%s
----

""".format(testid, graphVizOptions, out)
    ".Graph\n" + AsciiDocGenerator.dumpToSeparateFile(dir, testid, graphOutput)
  }

  protected def getGraphvizStyle: GraphStyle = AsciiDocStyle.withAutomaticRelationshipTypeColors()

}

abstract class DocumentingTestBase extends JUnitSuite with DocumentationHelper with GraphIcing with ResetStrategy {
  def testQuery(title: String, text: String, queryText: String, optionalResultExplanation: String = null,
                parameters: Map[String, Any] = Map.empty, assertions: InternalExecutionResult => Unit) {
    internalTestQuery(title, text, queryText, optionalResultExplanation, None, None, parameters, assertions)
  }

  def testFailingQuery[T <: CypherException: ClassTag](title: String, text: String, queryText: String, optionalResultExplanation: String = null) {
    val classTag = implicitly[ClassTag[T]]
    internalTestQuery(title, text, queryText, optionalResultExplanation, Some(classTag), None, Map.empty, _ => {})
  }

  def prepareAndTestQuery(title: String, text: String, queryText: String, optionalResultExplanation: String = "",
                          prepare: GraphDatabaseAPI => Unit, assertions: InternalExecutionResult => Unit) {
    internalTestQuery(title, text, queryText, optionalResultExplanation, None, Some(prepare), Map.empty, assertions)
  }

  def profileQuery(title: String, text: String, queryText: String, realQuery: Option[String] = None, assertions: InternalExecutionResult => Unit) {
    internalProfileQuery(title, text, queryText, realQuery, None, None, assertions)
  }

  private def internalProfileQuery(title: String,
                                   text: String,
                                   queryText: String,
                                   realQuery: Option[String],
                                   expectedException: Option[ClassTag[_ <: CypherException]],
                                   prepare: Option[GraphDatabaseAPI => Unit],
                                   assertions: InternalExecutionResult => Unit) {
    preparationQueries = List()

    dumpSetupConstraintsQueries(setupConstraintQueries, dir)
    dumpSetupQueries(setupQueries, dir)

    val consoleData: String = "none"

    val keySet = nodeMap.keySet
    val writer: PrintWriter = createWriter(title, dir)
    prepareForTest(title, prepare)

    val query = db.inTx {
      keySet.foldLeft(realQuery.getOrElse(queryText)) {
        (acc, key) => acc.replace("%" + key + "%", node(key).getId.toString)
      }
    }

    try {
      val results = engine.profile(query)
      val result = RewindableExecutionResult(results)

      if (expectedException.isDefined) {
        fail(s"Expected the test to throw an exception: $expectedException")
      }

      val testId = niceify(section + " " + title)
      writer.println("[[" + testId + "]]")
      if (!noTitle) writer.println("== " + title + " ==")
      writer.println(text)
      writer.println()

      val output = new StringBuilder(2048)
      output.append(".Query\n")
      output.append(createCypherSnippet(query))
      writer.println(AsciiDocGenerator.dumpToSeparateFile(dir, testId + ".query", output.toString()))
      writer.println()
      writer.println()

      writer.append(".Query Plan\n")
      writer.append(AsciidocHelper.createOutputSnippet(result.executionPlanDescription().toString))

      writer.flush()
      writer.close()


      db.inTx {
        assertions(result)
      }

    } catch {
      case e: CypherException if expectedException.nonEmpty =>
        val expectedExceptionType = expectedException.get
        e match {
          case expectedExceptionType(typedE) =>
            dumpToFileWithException(dir, writer, title, query, "", text, typedE, consoleData, Map.empty)
          case _ => fail(s"Expected an exception of type $expectedException but got ${e.getClass}", e)
        }
    }
  }


  private def internalTestQuery(title: String,
                                text: String,
                                queryText: String,
                                optionalResultExplanation: String,
                                expectedException: Option[ClassTag[_ <: CypherException]],
                                prepare: Option[GraphDatabaseAPI => Unit],
                                parameters: Map[String, Any],
                                assertions: InternalExecutionResult => Unit)
  {
    preparationQueries = List()
    //dumpGraphViz(dir, graphvizOptions.trim)
    if (!graphvizExecutedAfter) {
      dumpGraphViz(dir, graphvizOptions.trim)
    }
    dumpSetupConstraintsQueries(setupConstraintQueries, dir)
    dumpSetupQueries(setupQueries, dir)

    var consoleData: String = ""
    if (generateConsole) {
      if (generateInitialGraphForConsole) {
        val out = new StringWriter()
        db.inTx {
          new SubGraphExporter(DatabaseSubGraph.from(db)).export(new PrintWriter(out))
          consoleData = out.toString
        }
      }
      if (consoleData.isEmpty) {
        consoleData = "none"
      }
    }

    val keySet = nodeMap.keySet
    val writer: PrintWriter = createWriter(title, dir)
    prepareForTest(title, prepare)

    val query = db.inTx {
      keySet.foldLeft(queryText)((acc, key) => acc.replace("%" + key + "%", node(key).getId.toString))
    }

    assert(filePaths.size == urls.size)

    val testQuery = filePaths.foldLeft(query)( (acc, entry) => acc.replace(entry._1, entry._2))
    val docQuery = urls.foldLeft(query)( (acc, entry) => acc.replace(entry._1, entry._2))

    executeWithAllPlannersAndAssert(testQuery, assertions, expectedException,
      dumpToFileWithException(dir, writer, title, docQuery, optionalResultExplanation, text, _, consoleData, parameters), parameters,
      prepareForTest(title, prepare))
    match {
      case Some(result) => dumpToFileWithResult(dir, writer, title, docQuery, optionalResultExplanation, text, result, consoleData, parameters)
      case  None =>
    }
  }

  def prepareForTest(title: String, prepare: Option[GraphDatabaseAPI => Unit]) {
    prepare.foreach {
      (prepareStep: GraphDatabaseAPI => Any) => prepareStep(db)
    }
    if (preparationQueries.nonEmpty) {
      dumpPreparationQueries(preparationQueries, dir, title)
      dumpPreparationGraphviz(dir, title, graphvizOptions)
    }
    db.inTx { db.schema().awaitIndexesOnline(2, TimeUnit.SECONDS) }
  }

  private def executeWithAllPlannersAndAssert(query: String, assertions: InternalExecutionResult => Unit,
                                              expectedException: Option[ClassTag[_ <: CypherException]],
                                              expectedCaught: CypherException => Unit,
                                              parameters: Map[String, Any],
                                              prepareFunction: => Unit) = {
    // COST planner is default. Can't specify it without getting exception thrown if it's unavailable.
    val planners = Seq("", "CYPHER PLANNER=rule ")

    val results = planners.flatMap {
      case s if expectedException.isEmpty =>
        val rewindable = RewindableExecutionResult(engine.execute(s"$s $query", parameters))
        db.inTx(assertions(rewindable))
        val dump = rewindable.dumpToString()
        if (graphvizExecutedAfter && s == planners.head) {
          dumpGraphViz(dir, graphvizOptions.trim)
        }
        reset()
        prepareFunction
        Some(dump)

      case s =>
        val e = intercept[CypherException](engine.execute(s"$s $query", parameters))
        val expectedExceptionType = expectedException.get
        e match {
          case expectedExceptionType(typedE) => expectedCaught(typedE)
          case _ => fail(s"Expected an exception of type $expectedException but got ${e.getClass}", e)
        }
        None
    }

    results.headOption
  }

  var db: GraphDatabaseAPI = null
  var engine: ServerExecutionEngine = null
  var nodeMap: Map[String, Long] = null
  var nodeIndex: Index[Node] = null
  var relIndex: Index[Relationship] = null
  val properties: Map[String, Map[String, Any]] = Map()
  var generateConsole: Boolean = true
  var generateInitialGraphForConsole: Boolean = true
  val graphvizOptions: String = ""
  val noTitle: Boolean = false
  val graphvizExecutedAfter: Boolean = false
  var preparationQueries: List[String] = List()

  protected val baseUrl = System.getProperty("remote-csv-upload")
  var filePaths: Map[String, String] = Map.empty
  var urls: Map[String, String] = Map.empty

  def section: String
  val dir: File = createDir(section)

  def graphDescription: List[String] = List()

  val setupQueries: List[String] = List()
  val setupConstraintQueries: List[String] = List()

  def indexProps: List[String] = List()

  def dumpToFileWithResult(dir: File, writer: PrintWriter, title: String, query: String, returns: String, text: String,
                 result: String, consoleData: String, parameters: Map[String, Any]) {
    dumpToFile(dir, writer, title, query, returns, text, Right(result), consoleData, parameters)
  }

  def dumpToFileWithException(dir: File, writer: PrintWriter, title: String, query: String, returns: String, text: String,
                 failure: CypherException, consoleData: String, parameters: Map[String, Any]) {
    dumpToFile(dir, writer, title, query, returns, text, Left(failure), consoleData, parameters)
  }

  private def dumpToFile(dir: File, writer: PrintWriter, title: String, query: String, returns: String, text: String,
                         result: Either[CypherException, String], consoleData: String, parameters: Map[String, Any]) {
    val testId = niceify(section + " " + title)
    writer.println("[[" + testId + "]]")
    if (!noTitle) writer.println("== " + title + " ==")
    writer.println(text)
    writer.println()
    dumpQuery(dir, writer, testId, query, returns, result, consoleData, parameters)
    writer.flush()
    writer.close()
  }

  def executePreparationQueries(queries: List[String]) {
    preparationQueries = queries
    preparationQueries.foreach(engine.execute)
  }

  protected def sampleAllIndicesAndWait(mode: IndexSamplingMode = IndexSamplingMode.TRIGGER_REBUILD_ALL, time: Long = 10, unit: TimeUnit = TimeUnit.SECONDS) = {
    samplingController.sampleIndexes(mode)
    samplingController.awaitSamplingCompleted(time, unit)
  }

  protected def samplingController = indexingService.samplingController
  protected def indexingService = db.getDependencyResolver.resolveDependency(classOf[IndexingService])

  protected def assertIsDeleted(pc: PropertyContainer) {
    val nodeManager: ThreadToStatementContextBridge = db.getDependencyResolver.resolveDependency(classOf[ThreadToStatementContextBridge])

    val statement : KernelStatement = nodeManager.getKernelTransactionBoundToThisThread( true ).acquireStatement().asInstanceOf[KernelStatement]

    pc match {
      case node: Node =>
        if (statement.txState().nodeIsDeletedInThisTx(node.getId)) {
        fail("Expected " + pc + " to be deleted, but it isn't.")
        }
      case rel: Relationship =>
        if (statement.txState().relationshipIsDeletedInThisTx(rel.getId)) {
          fail("Expected " + pc + " to be deleted, but it isn't.")
        }
      case _ => throw new ClassCastException
    }


  }

  protected def getLabelsFromNode(p: InternalExecutionResult): Iterable[String] = p.columnAs[Node]("n").next().labels

  def indexProperties[T <: PropertyContainer](n: T, index: Index[T]) {
    indexProps.foreach((property) => {
      if (n.hasProperty(property)) {
        val value = n.getProperty(property)
        index.add(n, property, value)
      }
    })
  }

  def node(name: String): Node = db.getNodeById(nodeMap.getOrElse(name, throw new NotFoundException(name)))
  def nodes(names: String*): List[Node] = names.map(node).toList
  def rel(id: Long): Relationship = db.getRelationshipById(id)

  @After
  def tearDown() {
    if (db != null) db.shutdown()
  }

  @Before
  def init() {
    hardReset()
  }

  protected def newTestGraphDatabaseFactory(): TestGraphDatabaseFactory = new TestGraphDatabaseFactory()

  override def hardReset() {
    tearDown()
    db = newTestGraphDatabaseFactory().newImpermanentDatabaseBuilder().
      setConfig(GraphDatabaseSettings.node_keys_indexable, "name").
      setConfig(GraphDatabaseSettings.node_auto_indexing, Settings.TRUE).
      newGraphDatabase().asInstanceOf[GraphDatabaseAPI]
    engine = new ServerExecutionEngine(db)

    softReset()
  }

  override def softReset() {
    cleanDatabaseContent(db)

    db.inTx {
      db.schema().awaitIndexesOnline(10, TimeUnit.SECONDS)

      nodeIndex = db.index().forNodes("nodes")
      relIndex = db.index().forRelationships("rels")
      val g = new GraphImpl(graphDescription.toArray[String])
      val description = GraphDescription.create(g)


      nodeMap = description.create(db).asScala.map {
        case (name, node) => name -> node.getId
      }.toMap

      setupQueries.foreach(engine.execute)

      GlobalGraphOperations.at(db).getAllNodes.asScala.foreach((n) => {
        indexProperties(n, nodeIndex)
        n.getRelationships(Direction.OUTGOING).asScala.foreach(indexProperties(_, relIndex))
      })

      asNodeMap(properties) foreach {
        case (n: Node, seq: Map[String, Any]) =>
          seq foreach { case (k, v) => n.setProperty(k, v) }
      }
    }

    setupConstraintQueries.foreach(engine.execute)
  }

  private def asNodeMap[T: Manifest](m: Map[String, T]): Map[Node, T] =
    m map { case (k: String, v: T) => (node(k), v) }

  private def mapMapValue(v: Any): Any = v match {
    case v: Map[_, _] => Eagerly.immutableMapValues(v, mapMapValue).asJava
    case seq: Seq[_]  => seq.map(mapMapValue).asJava
    case v: Any       => v
  }

  private def dumpQuery(dir: File,
                        writer: PrintWriter,
                        testId: String,
                        query: String,
                        returns: String,
                        result: Either[CypherException, String],
                        consoleData: String,
                        parameters: Map[String, Any]) {
    if (parameters != null && !parameters.isEmpty) {
      writer.append(JavaExecutionEngineDocTest.parametersToAsciidoc(mapMapValue(parameters)))
    }
    val output = new StringBuilder(2048)
    output.append(".Query\n")
    output.append(createCypherSnippet(query))
    writer.println(AsciiDocGenerator.dumpToSeparateFile(dir, testId + ".query", output.toString()))
    writer.println()
    if (returns != null && !returns.isEmpty) {
      writer.println(returns)
      writer.println()
    }

    output.clear()
    result match {
      case Left(failure) =>
        output.append(".Error message\n")
        output.append(AsciidocHelper.createQueryFailureSnippet(failure.getMessage))
      case Right(rightResult) =>
        output.append(".Result\n")
        output.append(AsciidocHelper.createQueryResultSnippet(rightResult))
    }
    output.append('\n')
    writer.println(AsciiDocGenerator.dumpToSeparateFile(dir, testId + ".result", output.toString()))

    if (generateConsole && (parameters == null || parameters.isEmpty)) {
      output.clear()
      writer.println(".Try this query live")
      output.append("[console]\n")
      output.append("----\n")
      output.append(consoleData)
      output.append("\n\n")
      output.append(query)
      output.append("\n----")
      writer.println(AsciiDocGenerator.dumpToSeparateFile(dir, testId + ".console", output.toString()))
    }
  }
}

trait ResetStrategy {
  def reset() {}
  def hardReset() {}
  def softReset() {}
}

trait HardReset extends ResetStrategy {
  override def reset() {
    hardReset()
  }
}

trait SoftReset extends ResetStrategy {
  override def reset() {
    softReset()
  }
}
