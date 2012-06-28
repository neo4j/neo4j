/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
import org.junit.{Before, After}
import scala.collection.JavaConverters._
import java.io.{PrintWriter, File}
import org.neo4j.graphdb._
import factory.{GraphDatabaseSetting, GraphDatabaseSettings}
import java.io.ByteArrayOutputStream
import org.neo4j.visualization.graphviz.{AsciiDocStyle, GraphvizWriter, GraphStyle}
import org.neo4j.walk.Walker
import org.neo4j.visualization.asciidoc.AsciidocHelper
import org.neo4j.cypher.CuteGraphDatabaseService.gds2cuteGds
import org.neo4j.cypher.javacompat.GraphImpl
import org.neo4j.cypher.{CypherParser, ExecutionResult, ExecutionEngine}
import org.neo4j.test.{ImpermanentGraphDatabase, TestGraphDatabaseFactory, GraphDescription}
import org.neo4j.test.GeoffService
import org.scalatest.Assertions

trait DocumentationHelper {
  def generateConsole:Boolean
  def db: GraphDatabaseService

  def nicefy(in: String): String = in.toLowerCase.replace(" ", "-")

  def createWriter(title: String, folder: String): (File, PrintWriter) = {
    val dir = new File(path + nicefy(folder))
    if (!dir.exists()) {
      dir.mkdirs()
    }

    val writer = new PrintWriter(new File(dir, nicefy(title) + ".txt"), "UTF-8")
    (dir, writer)
  }

  val path: String = "target/docs/dev/ql/"

  val graphvizFileName = "cypher-" + this.getClass.getSimpleName.replaceAll("Test", "").toLowerCase + "-graph.txt"

  def dumpGraphViz(dir: File, graphVizOptions:String) {
    val graphViz = new PrintWriter(new File(dir, graphvizFileName), "UTF-8")
    val foo = emitGraphviz(graphvizFileName, graphVizOptions)
    graphViz.write(foo)
    graphViz.flush()
    graphViz.close()
  }

  private def emitGraphviz(fileName:String, graphVizOptions:String): String = {

    val out = new ByteArrayOutputStream()
    val writer = new GraphvizWriter(getGraphvizStyle)
    writer.emit(out, Walker.fullGraph(db))

    return """
_Graph_

["dot", "%s.svg", "neoviz", "%s"]
----
%s
----

""".format(fileName, graphVizOptions, out)
  }

  protected def getGraphvizStyle: GraphStyle = AsciiDocStyle.withAutomaticRelationshipTypeColors()

}

abstract class DocumentingTestBase extends Assertions with DocumentationHelper {
  def testQuery(title: String, text: String, queryText: String, returns: String, assertions: (ExecutionResult => Unit)*) {
    val r = testWithoutDocs(queryText, assertions:_*)
    val result: ExecutionResult = r._1
    var query: String = r._2

    val (dir: File, writer: PrintWriter) = createWriter(title, section)
    dumpToFile(writer, title, query, returns, text, result)

    dumpGraphViz(dir, graphvizOptions)
  }

  var db: GraphDatabaseService = null
  val parser: CypherParser = new CypherParser
  var engine: ExecutionEngine = null
  var nodes: Map[String, Long] = null
  var nodeIndex: Index[Node] = null
  var relIndex: Index[Relationship] = null
  val properties: Map[String, Map[String, Any]] = Map()
  var generateConsole: Boolean = true
  var generateInitialGraphForConsole: Boolean = true
  val graphvizOptions: String = ""

  def section: String

  def graphDescription: List[String]

  def indexProps: List[String] = List()

  def dumpToFile(writer: PrintWriter, title: String, query: String, returns: String, text: String, result: ExecutionResult) {
    writer.println("[[" + nicefy(section + " " + title) + "]]")
    writer.println("== " + title + " ==")
    writer.println(text)
    writer.println()
    runQuery(writer, query, returns, result)
    writer.flush()
    writer.close()
  }

  def executeQuery(queryText: String): ExecutionResult = {
    var query = queryText
    nodes.keySet.foreach((key) => query = query.replace("%" + key + "%", node(key).getId.toString))
    engine.execute(query)
  }



  def testWithoutDocs(queryText: String, assertions: (ExecutionResult => Unit)*): (ExecutionResult, String) = {
    var query = queryText
    nodes.keySet.foreach((key) => query = query.replace("%" + key + "%", node(key).getId.toString))
    val result = engine.execute(query)
    assertions.foreach(_.apply(result))
    (result, query)
  }

  def indexProperties[T <: PropertyContainer](n: T, index: Index[T]) {
    indexProps.foreach((property) => {
      if (n.hasProperty(property)) {
        val value = n.getProperty(property)
        index.add(n, property, value)
      }
    })
  }

  def node(name: String): Node = db.getNodeById(nodes.getOrElse(name, throw new NotFoundException(name)))

  def rel(id: Long): Relationship = db.getRelationshipById(id)

  @After
  def teardown() {
    if (db != null) db.shutdown()
  }

  @Before
  def init() {
    db = new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder().
      setConfig( GraphDatabaseSettings.node_keys_indexable, "name" ).
      setConfig( GraphDatabaseSettings.node_auto_indexing, GraphDatabaseSetting.TRUE ).
      newGraphDatabase()
    engine = new ExecutionEngine(db)

    db.asInstanceOf[ImpermanentGraphDatabase].cleanContent(false)

    db.inTx(() => {
      nodeIndex = db.index().forNodes("nodes")
      relIndex = db.index().forRelationships("rels")
      val g = new GraphImpl(graphDescription.toArray[String])
      val description = GraphDescription.create(g)

      nodes = description.create(db).asScala.map {
        case (name, node) => name -> node.getId
      }.toMap

      db.getAllNodes.asScala.foreach((n) => {
        indexProperties(n, nodeIndex)
        n.getRelationships(Direction.OUTGOING).asScala.foreach(indexProperties(_, relIndex))
      })

      properties.foreach((n) => {
        val nod = node(n._1)
        n._2.foreach((kv) => nod.setProperty(kv._1, kv._2))
      })
    })
  }

  def runQuery(writer: PrintWriter, query: String, returns: String, result: ExecutionResult) {
    writer.println("_Query_")
    writer.println()
    writer.println(AsciidocHelper.createCypherSnippet(query))
    writer.println()
    writer.println(returns)
    writer.println()

    val resultText = result.dumpToString()
    writer.println(".Result")
    writer.println(AsciidocHelper.createQueryResultSnippet(resultText))
    writer.println()
    writer.println()
    if (generateConsole) {
      writer.println(".Try this query live")
      writer.println("[console]")
      writer.println("----\n" + (if (generateInitialGraphForConsole) new GeoffService(db).toGeoff else "start n=node(*) match n-[r?]->() delete n, r;") + "\n\n" + query + "\n----")
      writer.println()
    }
  }

}




