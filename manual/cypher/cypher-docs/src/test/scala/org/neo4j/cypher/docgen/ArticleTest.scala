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

import org.neo4j.cypher.internal.RewindableExecutionResult
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.InternalExecutionResult
import org.neo4j.graphdb.index.Index
import org.junit.Test
import scala.collection.JavaConverters._
import java.io.{StringWriter, File, PrintWriter}
import org.neo4j.graphdb._
import org.neo4j.visualization.asciidoc.AsciidocHelper
import org.neo4j.cypher.javacompat.GraphImpl
import org.neo4j.cypher._
import export.{DatabaseSubGraph, SubGraphExporter}
import org.neo4j.test.{ImpermanentGraphDatabase, TestGraphDatabaseFactory, GraphDescription}
import org.scalatest.Assertions
import org.neo4j.test.AsciiDocGenerator
import org.neo4j.test.GraphDatabaseServiceCleaner.cleanDatabaseContent
import org.neo4j.kernel.GraphDatabaseAPI
import org.neo4j.tooling.GlobalGraphOperations
import org.neo4j.cypher.internal.compiler.v2_3.prettifier.Prettifier

/*
Use this base class for tests that are more flowing text with queries intersected in the middle of the text.
 */
abstract class ArticleTest extends Assertions with DocumentationHelper {

  var db: GraphDatabaseAPI = null
  implicit var engine: ExecutionEngine = null
  var nodes: Map[String, Long] = null
  var nodeIndex: Index[Node] = null
  var relIndex: Index[Relationship] = null
  val properties: Map[String, Map[String, Any]] = Map()
  var generateConsole: Boolean = true
  var dir: File = null

  def title: String
  def section: String
  def assert(name: String, result: InternalExecutionResult)
  def graphDescription: List[String]
  def indexProps: List[String] = List()

  private def executeQuery(queryText: String)(implicit engine: ExecutionEngine): InternalExecutionResult = try {
    val result = RewindableExecutionResult(engine.execute(replaceNodeIds(queryText)))
    result.dumpToString()
    result
  } catch {
    case e: CypherException => throw new InternalException(queryText, e)
  }

  private def replaceNodeIds(_query: String): String = {
    var query = _query
    nodes.keySet.foreach((key) => query = query.replace("%" + key + "%", node(key).getId.toString))
    query
  }

  private def indexProperties[T <: PropertyContainer](n: T, index: Index[T]) {
    indexProps.foreach((property) => {
      if (n.hasProperty(property)) {
        val value = n.getProperty(property)
        index.add(n, property, value)
      }
    })
  }

  private def node(name: String): Node = db.inTx(db.getNodeById(nodes.getOrElse(name, throw new NotFoundException(name))))

  def text: String

  private def expandQuery(query: String, includeResults: Boolean, emptyGraph: Boolean, dir: File, possibleAssertion: Seq[String]) = {
    val name = title.toLowerCase.replace(" ", "-")
    val queryAsciidoc = createCypherSnippet(replaceNodeIds(query))
    val querySnippet = AsciiDocGenerator.dumpToSeparateFileWithType(dir,  name + "-query", queryAsciidoc)
    val consoleAsciidoc = consoleSnippet(replaceNodeIds(query), emptyGraph)
    val consoleText = if (!consoleAsciidoc.isEmpty)
        ".Try this query live\n" +
        AsciiDocGenerator.dumpToSeparateFileWithType(dir, name + "-console", consoleAsciidoc)
      else ""
    val queryOutput = runQuery(emptyGraph, query, possibleAssertion)
    val resultSnippetAsciiDoc = AsciidocHelper.createQueryResultSnippet(queryOutput)
    val resultSnippet = AsciiDocGenerator.dumpToSeparateFileWithType(dir, name + "-result", resultSnippetAsciiDoc)

    val queryText = """.Query
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


  private def runQuery(emptyGraph: Boolean, query: String, possibleAssertion: Seq[String]): String = {

    def testAssertions(result: InternalExecutionResult) {
      possibleAssertion.foreach(name => assert(name, result) )
    }

    if (emptyGraph) {
      val db = new TestGraphDatabaseFactory().
        newImpermanentDatabaseBuilder().
        newGraphDatabase().asInstanceOf[GraphDatabaseAPI]
      try {
        val engine = new ExecutionEngine(db)
        val result = executeQuery(query)(engine)
        testAssertions(result)
        result.dumpToString()
      } finally {
        db.shutdown()
      }
    }
    else {
      val result = executeQuery(query)
      testAssertions(result)
      result.dumpToString()
    }
  }

  private def consoleSnippet(query: String, empty: Boolean): String = {
    val prettifiedQuery = Prettifier(query.trim())
    if (generateConsole) {
      val create = if (!empty) {
        db.inTx {
          val out = new StringWriter()
          new SubGraphExporter(DatabaseSubGraph.from(db)).export(new PrintWriter(out))
          out.toString
        }
      } else "match n-[r?]->() delete n, r;"
      """[console]
----
%s

%s
----
""".format(create, prettifiedQuery)
    } else ""
  }

  private def header = "[[%s-%s]]".format(section.toLowerCase, title.toLowerCase.replace(" ", "-"))

  def doThisBefore() {}

  @Test
  def produceDocumentation() {
    val db = init()
    doThisBefore()
    try {
      val writer: PrintWriter = createWriter(title, dir)

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
    val regex = "###graph-image(.*?)###".r
    regex.findFirstMatchIn(startText) match {
      case None => startText
      case Some(options) =>
        val optionString = options.group(1)
        val txt = startText.replaceAllLiterally("###graph-image" + optionString + "###",
            dumpGraphViz(dir, optionString.trim))
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
        producedText = producedText.replace(query, expandQuery(q, includeResults, emptyGraph, dir, asserts))
      }
    }

    producedText
  }

  private def init() = {
    dir = createDir(section)
    db = new TestGraphDatabaseFactory().
      newImpermanentDatabaseBuilder().
      newGraphDatabase().asInstanceOf[GraphDatabaseAPI]

    cleanDatabaseContent( db.asInstanceOf[GraphDatabaseService] )

    db.inTx {
      nodeIndex = db.index().forNodes("nodes")
      relIndex = db.index().forRelationships("rels")
      val g = new GraphImpl(graphDescription.toArray[String])
      val description = GraphDescription.create(g)

      nodes = description.create(db).asScala.map {
        case (name, node) => name -> node.getId
      }.toMap

      GlobalGraphOperations.at(db).getAllNodes.asScala.foreach((n) => {
        indexProperties(n, nodeIndex)
        n.getRelationships(Direction.OUTGOING).asScala.foreach(indexProperties(_, relIndex))
      })

      properties.foreach((n) => {
        val nod = node(n._1)
        n._2.foreach((kv) => nod.setProperty(kv._1, kv._2))
      })
    }
    engine = new ExecutionEngine(db)
    db
  }
}




