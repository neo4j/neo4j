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
import org.junit.Test
import scala.collection.JavaConverters._
import java.io.{File, PrintWriter}
import org.neo4j.graphdb._
import org.neo4j.visualization.asciidoc.AsciidocHelper
import org.neo4j.cypher.CuteGraphDatabaseService.gds2cuteGds
import org.neo4j.cypher.javacompat.GraphImpl
import org.neo4j.cypher._
import org.neo4j.test.{GeoffService, ImpermanentGraphDatabase, TestGraphDatabaseFactory, GraphDescription}
import org.scalatest.Assertions

/*
Use this base class for tests that are more flowing text with queries intersected in the middle of the text.
 */
abstract class ArticleTest extends Assertions with DocumentationHelper {

  var db: GraphDatabaseService = null
  val parser: CypherParser = new CypherParser
  implicit var engine: ExecutionEngine = null
  var nodes: Map[String, Long] = null
  var nodeIndex: Index[Node] = null
  var relIndex: Index[Relationship] = null
  val properties: Map[String, Map[String, Any]] = Map()
  var generateConsole: Boolean = true

  def title: String
  def section: String
  def assert(name: String, result: ExecutionResult)
  def graphDescription: List[String]
  def indexProps: List[String] = List()

  def executeQuery(queryText: String)(implicit engine: ExecutionEngine): ExecutionResult = try {
    val result = engine.execute(replaceNodeIds(queryText))
    result.toList //Let's materialize the result
    result.dumpToString()
    result
  } catch {
    case e: CypherException => throw new InternalException(queryText, e)
  }

  def replaceNodeIds(_query: String): String = {
    var query = _query
    nodes.keySet.foreach((key) => query = query.replace("%" + key + "%", node(key).getId.toString))
    query
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


  def text: String

  def expandQuery(query: String, includeResults: Boolean, emptyGraph: Boolean, possibleAssertion: Seq[String]) = {
    val querySnippet = AsciidocHelper.createCypherSnippet(replaceNodeIds(query))
    val consoleText = consoleSnippet(replaceNodeIds(query), emptyGraph)
    val queryOutput = runQuery(emptyGraph, query, possibleAssertion)
    val resultSnippet = AsciidocHelper.createQueryResultSnippet(queryOutput)

    val queryText = """_Query_

%s

""".format(querySnippet)

    val resultText = """.Result
%s
""".format(resultSnippet)

    if (includeResults)
      queryText + resultText + consoleText
    else
      queryText + consoleText
  }


  def runQuery(emptyGraph: Boolean, query: String, possibleAssertion: Seq[String]): String = {
    val result = if (emptyGraph) {
      val db = new ImpermanentGraphDatabase()
      val engine = new ExecutionEngine(db)
      val result = executeQuery(query)(engine)
      db.shutdown()
      result
    }
    else
      executeQuery(query)

    possibleAssertion.foreach(assert(_, result))

    result.dumpToString()
  }

  private def consoleSnippet(query: String, empty: Boolean): String = {
    if (generateConsole) {
      val create = if (!empty) new GeoffService(db).toGeoff.trim else "start n=node(*) match n-[r?]->() delete n, r;"
      """.Try this query live
[console]
----
%s

%s
----
""".format(create, query)
    } else ""
  }

  def header = "[[%s-%s]]".format(section.toLowerCase, title.toLowerCase.replace(" ", "-"))

  @Test
  def produceDocumentation() {
    val db = init()
    try {
      val (dir: File, writer: PrintWriter) = createWriter(title, section)

      val queryText = includeQueries(text, dir)

      writer.println(header)
      writer.println(queryText)
      writer.close()
    } finally {
      db.shutdown()
    }
  }

  val assertiongRegEx = "assertion=([^\\s]*)".r

  private def includeGraphviz(startText: String, dir: File):String = {
    val graphVizLine = "include::" + graphvizFileName + "[]"

    val regex = "###graph-image(.*?)###".r
    regex.findFirstMatchIn(startText) match {
      case None => startText
      case Some(options) =>
        val optionString = options.group(1)
        val txt = startText.replaceAllLiterally("###graph-image" + optionString + "###", graphVizLine)
        if (txt != startText) {
          dumpGraphViz(dir, optionString.trim)
        }
        txt
    }
  }

  private def includeQueries(query: String, dir: File) = {
    val startText = includeGraphviz(query, dir)
    val regex = ("(?s)###(.*?)###").r
    val queries = (regex findAllIn startText).toList

    var producedText = startText
    queries.foreach {
      query => {
        val firstLine = query.split("\n").head

        val includeResults = !firstLine.contains("no-results")
        val emptyGraph = firstLine.contains("empty-graph")
        val asserts: Seq[String] = assertiongRegEx.findFirstMatchIn(firstLine).toSeq.flatMap(_.subgroups)

        val rest = query.split("\n").tail.mkString("\n")
        val q = rest.replaceAll("#", "")
        producedText = producedText.replace(query, expandQuery(q, includeResults, emptyGraph, asserts))
      }
    }

    producedText
  }

  private def init() = {
    db = new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder().newGraphDatabase()

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
    engine = new ExecutionEngine(db)
    db
  }
}




