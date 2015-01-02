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
package org.neo4j.cypher.docgen

import org.neo4j.graphdb.index.Index
import org.junit.{ Before, After }
import scala.collection.JavaConverters._
import java.io.{ StringWriter, PrintWriter, File, ByteArrayOutputStream }
import org.neo4j.graphdb._
import factory.GraphDatabaseSettings
import org.neo4j.visualization.graphviz.{ AsciiDocStyle, GraphvizWriter, GraphStyle }
import org.neo4j.walk.Walker
import org.neo4j.visualization.asciidoc.AsciidocHelper
import org.neo4j.cypher.javacompat.GraphImpl
import org.neo4j.cypher.{ CypherException, ExecutionResult, ExecutionEngine }
import org.neo4j.test.{ TestGraphDatabaseFactory, GraphDescription }
import org.scalatest.Assertions
import org.neo4j.test.AsciiDocGenerator
import org.neo4j.test.GraphDatabaseServiceCleaner.cleanDatabaseContent
import org.neo4j.kernel.{ GraphDatabaseAPI, AbstractGraphDatabase }
import org.neo4j.cypher.internal.helpers.{ Materialized, GraphIcing }
import org.neo4j.cypher.export.{ SubGraphExporter, DatabaseSubGraph }
import org.neo4j.helpers.Settings
import org.neo4j.cypher.example.JavaExecutionEngineDocTest
import org.neo4j.tooling.GlobalGraphOperations
import scala.reflect.ClassTag
import org.neo4j.cypher.internal.compiler.v2_0.prettifier.Prettifier
import org.neo4j.kernel.impl.core.NodeManager
import org.scalatest.junit.JUnitSuite

trait DocumentationHelper extends GraphIcing {
  def generateConsole: Boolean
  def db: GraphDatabaseAPI

  def nicefy(in: String): String = in.toLowerCase.replace(" ", "-")

  def simpleName: String = this.getClass.getSimpleName.replaceAll("Test", "").toLowerCase

  def createDir(folder: String): File = createDir(path, folder)

  def createDir(where: File, folder: String): File = {
    val dir = new File(where, nicefy(folder))
    if (!dir.exists()) {
      dir.mkdirs()
    }
    dir
  }

  def createWriter(title: String, dir: File): PrintWriter = {
    new PrintWriter(new File(dir, nicefy(title) + ".asciidoc"), "UTF-8")
  }

  def createCypherSnippet(query: String) = {
    val prettifiedQuery = Prettifier(query.trim())
    val result = AsciidocHelper.createCypherSnippetFromPreformattedQuery(prettifiedQuery)
    result
  }

  def prepareFormatting(query: String): String = {
    val str = Prettifier(query.trim())
    if ((str takeRight 1) == ";") {
      str
    } else {
      str + ";";
    }
  }

  def dumpSetupQueries(queries: List[String], dir: File) {
    dumpQueries(queries, dir, simpleName + "-setup");
  }

  def dumpSetupConstraintsQueries(queries: List[String], dir: File) {
    dumpQueries(queries, dir, simpleName + "-setup-constraints");
  }
  
  def dumpPreparationQueries(queries: List[String], dir: File, testid: String) {
    dumpQueries(queries, dir, simpleName + "-" + nicefy(testid) + ".preparation");
  }
  
  private def dumpQueries(queries: List[String], dir: File, testid: String): String = {
    if (queries.isEmpty) {
      ""
    } else {
      val queryStrings = queries.map(prepareFormatting)
      val output = AsciidocHelper.createCypherSnippetFromPreformattedQuery(queryStrings.mkString("\n"))
      AsciiDocGenerator.dumpToSeparateFile(dir, testid, output);
    }
  }
  
  val path: File = new File("target/docs/dev/ql/")

  val graphvizFileName = "cypher-" + simpleName + "-graph"

  def dumpGraphViz(dir: File, graphVizOptions: String): String = {
    emitGraphviz(dir, graphvizFileName, graphVizOptions)
  }

  def dumpPreparationGraphviz(dir: File, testid: String, graphVizOptions: String): String = {
    emitGraphviz(dir, simpleName + "-" + nicefy(testid) + ".preparation-graph", graphVizOptions)
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

abstract class DocumentingTestBase extends JUnitSuite with Assertions with DocumentationHelper with GraphIcing {

  def testQuery(title: String, text: String, queryText: String, optionalResultExplanation: String, assertions: (ExecutionResult => Unit)*) {
    internalTestQuery(title, text, queryText, optionalResultExplanation, None, None, assertions: _*)
  }

  def testFailingQuery[T <: CypherException: ClassTag](title: String, text: String, queryText: String, optionalResultExplanation: String) {
    val classTag = implicitly[ClassTag[T]]
    internalTestQuery(title, text, queryText, optionalResultExplanation, Some(classTag), None)
  }

  def prepareAndTestQuery(title: String, text: String, queryText: String, optionalResultExplanation: String, prepare: => Any, assertions: (ExecutionResult => Unit)*) {
    internalTestQuery(title, text, queryText, optionalResultExplanation, None, Some(() => prepare), assertions: _*)
  }

  def internalTestQuery(title: String, text: String, queryText: String, optionalResultExplanation: String, expectedException: Option[ClassTag[_ <: CypherException]], prepare: Option[() => Any], assertions: (ExecutionResult => Unit)*) {
    parameters = null
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

    // val r = testWithoutDocs(queryText, prepare, assertions:_*)
    var query = queryText
    val keySet = nodes.keySet
    val tx1 = db.beginTx()
    try {
      prepare.foreach { (prepareStep: () => Any) => prepareStep() }
      if (preparationQueries.size > 0) {
        dumpPreparationQueries(preparationQueries, dir, title)
        dumpPreparationGraphviz(dir, title, graphvizOptions)
      }
      keySet.foreach((key) => query = query.replace("%" + key + "%", node(key).getId.toString))
      val result = if (parameters == null) engine.execute(query) else engine.execute(query, parameters)
      if (expectedException.isDefined) {
        fail(s"Expected the test to throw an exception: $expectedException")
      }
      assertions.foreach(_.apply(result))
      tx1.failure()
    } catch {
      case e: CypherException =>
        expectedException match {
          case Some(expectedExceptionType) => e match {
            case expectedExceptionType(typedE) =>
            case _ => fail(s"Expected an exception of type $expectedException but got ${e.getClass}", e)
          }
          case None => throw e
        }
    } finally {
      tx1.close()
    }

    val tx2 = db.beginTx()
    val writer: PrintWriter = createWriter(title, dir)
    try {
      prepare.foreach { (prepareStep: () => Any) => prepareStep() }
      val result = if (parameters == null) engine.execute(query) else engine.execute(query, parameters)
      if (expectedException.isDefined) {
        fail(s"Expected the test to throw an exception: $expectedException")
      }
      dumpToFile(dir, writer, title, query, optionalResultExplanation, text, result, consoleData)
      if (graphvizExecutedAfter) {
        dumpGraphViz(dir, graphvizOptions.trim)
      }
    } catch {
      case e: CypherException if expectedException.nonEmpty =>
        val expectedExceptionType = expectedException.get
        e match {
          case expectedExceptionType(typedE) =>
            dumpToFile(dir, writer, title, query, optionalResultExplanation, text, typedE, consoleData)
          case _ => fail(s"Expected an exception of type $expectedException but got ${e.getClass}", e)
        }
    } finally {
      tx2.close()
    }
  }

  var db: GraphDatabaseAPI = null
  var engine: ExecutionEngine = null
  var nodes: Map[String, Long] = null
  var nodeIndex: Index[Node] = null
  var relIndex: Index[Relationship] = null
  val properties: Map[String, Map[String, Any]] = Map()
  var generateConsole: Boolean = true
  var generateInitialGraphForConsole: Boolean = true
  val graphvizOptions: String = ""
  val noTitle: Boolean = false
  val graphvizExecutedAfter: Boolean = false
  var parameters: Map[String, Any] = null
  var preparationQueries: List[String] = List()

  def section: String
  val dir: File = createDir(section)

  def graphDescription: List[String] = List()

  val setupQueries: List[String] = List()
  val setupConstraintQueries: List[String] = List()

  def indexProps: List[String] = List()

  def dumpToFile(dir: File, writer: PrintWriter, title: String, query: String, returns: String, text: String, result: ExecutionResult, consoleData: String) {
    dumpToFile(dir, writer, title, query, returns, text, Right(result), consoleData)
  }

  def dumpToFile(dir: File, writer: PrintWriter, title: String, query: String, returns: String, text: String, failure: CypherException, consoleData: String) {
    dumpToFile(dir, writer, title, query, returns, text, Left(failure), consoleData)
  }

  def dumpToFile(dir: File, writer: PrintWriter, title: String, query: String, returns: String, text: String, result: Either[CypherException, ExecutionResult], consoleData: String) {
    val testId = nicefy(section + " " + title)
    writer.println("[[" + testId + "]]")
    if (!noTitle) writer.println("== " + title + " ==")
    writer.println(text)
    writer.println()
    runQuery(dir, writer, testId, query, returns, result, consoleData)
    writer.flush()
    writer.close()
  }

  def executeQuery(queryText: String): ExecutionResult = {
    var query = queryText
    nodes.keySet.foreach((key) => query = query.replace("%" + key + "%", node(key).getId.toString))
    engine.execute(query)
  }

  def setParameters(params: Map[String, Any]) {
    parameters = params
  }
  
  def executePreparationQueries(queries: List[String]) {
    preparationQueries = queries
    preparationQueries.foreach(engine.execute)
  }

  protected def assertIsDeleted(pc: PropertyContainer) {
    val internalDb: AbstractGraphDatabase = db.asInstanceOf[AbstractGraphDatabase]

    val nodeManager: NodeManager = internalDb.asInstanceOf[GraphDatabaseAPI].getDependencyResolver.resolveDependency(classOf[NodeManager])

    if (!nodeManager.isDeleted(pc)) {
      fail("Expected " + pc + " to be deleted, but it isn't.")
    }
  }

  def testWithoutDocs(queryText: String, prepare: Option[() => Any], assertions: (ExecutionResult => Unit)*): (ExecutionResult, String) = {
    prepare.foreach { (prepareStep: () => Any) => prepareStep() }

    var query = queryText
    val keySet = nodes.keySet
    keySet.foreach((key) => query = query.replace("%" + key + "%", node(key).getId.toString))
    val result = engine.execute(query)
    assertions.foreach(_.apply(result))

    (result, query)
  }

  protected def getLabelsFromNode(p: ExecutionResult): Iterable[String] = p.columnAs[Node]("n").next().labels

  def indexProperties[T <: PropertyContainer](n: T, index: Index[T]) {
    indexProps.foreach((property) => {
      if (n.hasProperty(property)) {
        val value = n.getProperty(property)
        index.add(n, property, value)
      }
    })
  }

  def node(name: String): Node = db.getNodeById(nodes.getOrElse(name, throw new NotFoundException(name)))
  def nodes(names: String*): List[Node] = names.map(node).toList

  def rel(id: Long): Relationship = db.getRelationshipById(id)

  @After
  def teardown() {
    if (db != null) db.shutdown()
  }

  @Before
  def init() {
    db = new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder().
      setConfig(GraphDatabaseSettings.node_keys_indexable, "name").
      setConfig(GraphDatabaseSettings.node_auto_indexing, Settings.TRUE).
      newGraphDatabase().asInstanceOf[GraphDatabaseAPI]
    engine = new ExecutionEngine(db)

    cleanDatabaseContent( db )

    setupConstraintQueries.foreach(engine.execute)

    db.inTx {
      nodeIndex = db.index().forNodes("nodes")
      relIndex = db.index().forRelationships("rels")
      val g = new GraphImpl(graphDescription.toArray[String])
      val description = GraphDescription.create(g)

      setupQueries.foreach(engine.execute)

      nodes = description.create(db).asScala.map {
        case (name, node) => name -> node.getId
      }.toMap

      GlobalGraphOperations.at(db).getAllNodes.asScala.foreach((n) => {
        indexProperties(n, nodeIndex)
        n.getRelationships(Direction.OUTGOING).asScala.foreach(indexProperties(_, relIndex))
      })

      asNodeMap(properties) foreach {
        case (n: Node, seq: Map[String, Any]) =>
          seq foreach { case (k, v) => n.setProperty(k, v) }
      }
    }
  }

  private def asNodeMap[T: Manifest](m: Map[String, T]): Map[Node, T] =
    m map { case (k: String, v: T) => (node(k), v) }

  def mapMapValue(v: Any): Any = v match {
    case v: Map[_, _] => Materialized.mapValues(v, mapMapValue).asJava
    case seq: Seq[_] => seq.map(mapMapValue).asJava
    case v: Any => v
  }

  def runQuery(dir: File, writer: PrintWriter, testId: String, query: String, returns: String, result: Either[CypherException, ExecutionResult], consoleData: String) {
    if (parameters != null) {
      writer.append(JavaExecutionEngineDocTest.parametersToAsciidoc(mapMapValue(parameters)))
    }
    val output = new StringBuilder(2048)
    output.append(".Query\n")
    output.append(createCypherSnippet(query))
    writer.println(AsciiDocGenerator.dumpToSeparateFile(dir, testId + ".query", output.toString()))
    writer.println()
    writer.println(returns)
    writer.println()

    output.clear()
    result match {
      case Left(failure) =>
        output.append(".Error message\n")
        output.append(AsciidocHelper.createQueryFailureSnippet(failure.getMessage))
      case Right(rightResult) =>
        output.append(".Result\n")
        output.append(AsciidocHelper.createQueryResultSnippet(rightResult.dumpToString()))
    }
    output.append('\n')
    writer.println(AsciiDocGenerator.dumpToSeparateFile(dir, testId + ".result", output.toString()))

    if (generateConsole && parameters == null) {
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

