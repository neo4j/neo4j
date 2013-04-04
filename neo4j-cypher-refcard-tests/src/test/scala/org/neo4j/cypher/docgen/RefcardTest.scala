/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
import java.io.{ File, PrintWriter }
import org.neo4j.graphdb._
import org.neo4j.visualization.asciidoc.AsciidocHelper
import org.neo4j.cypher.CuteGraphDatabaseService.gds2cuteGds
import org.neo4j.cypher.javacompat.GraphImpl
import org.neo4j.cypher._
import org.neo4j.test.{ GeoffService, ImpermanentGraphDatabase, TestGraphDatabaseFactory, GraphDescription }
import org.scalatest.Assertions
import org.neo4j.test.AsciiDocGenerator

/*
Use this base class for refcard tests
 */
abstract class RefcardTest extends Assertions with DocumentationHelper {

  var db: GraphDatabaseService = null
  val parser: CypherParser = new CypherParser
  implicit var engine: ExecutionEngine = null
  var nodes: Map[String, Long] = null
  var nodeIndex: Index[Node] = null
  var relIndex: Index[Relationship] = null
  val properties: Map[String, Map[String, Any]] = Map()
  var generateConsole: Boolean = true
  var dir: File = null

  def title: String
  def section: String
  def assert(name: String, result: ExecutionResult)
  def parameters(name: String): Map[String, Any] = Map()
  def graphDescription: List[String]
  def indexProps: List[String] = List()

  def executeQuery(queryText: String, params: Map[String, Any])(implicit engine: ExecutionEngine): ExecutionResult = try {
    val result = engine.execute(replaceNodeIds(queryText), params)
    result
  } catch {
    case e: CypherException => throw new InternalException(queryText, e)
  }

  def replaceNodeIds(_query: String): String = {
    var query = _query
    nodes.keySet.foreach((key) => query = query.replace("%" + key + "%", node(key).getId.toString))
    query
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

  def expandQuery(query: String, queryPart: String, emptyGraph: Boolean, dir: File, possibleAssertion: Seq[String], parametersChoice: String) = {
    val name = title.toLowerCase.replace(" ", "-")
    runQuery(emptyGraph, query, possibleAssertion, parametersChoice)

    queryPart
  }

  def runQuery(emptyGraph: Boolean, query: String, possibleAssertion: Seq[String], parametersChoice: String): ExecutionResult = {
    val result = if (emptyGraph) {
      val db = new ImpermanentGraphDatabase()
      val engine = new ExecutionEngine(db)
      val result = executeQuery(query, parameters(parametersChoice))(engine)
      db.shutdown()
      result
    } else
      executeQuery(query, parameters(parametersChoice))

    possibleAssertion.foreach(name => {
      try {
        assert(name, result)
      } catch {
        case e: Exception => throw new RuntimeException("Test: %s\n%s".format(name, e.getMessage), e)
      }
    })

    result
  }

  def header = "[[%s-%s]]".format(section.toLowerCase, title.toLowerCase.replace(" ", "-"))

  @Test
  def produceDocumentation() {
    val db = init()
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

  private def includeGraphviz(startText: String, dir: File): String = {
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

  val assertiongRegEx = "assertion=([^\\s]*)".r
  val parametersRegEx = "parameters=([^\\s]*)".r

  private def includeQueries(query: String, dir: File) = {
    val startText = includeGraphviz(query, dir)
    val regex = ("(?s)###(.*?)###").r
    val queries = (regex findAllIn startText).toList

    var producedText = startText
    queries.foreach {
      query =>
        {
          val firstLine = query.split("\n").head

          val emptyGraph = firstLine.contains("empty-graph")
          val asserts: Seq[String] = assertiongRegEx.findFirstMatchIn(firstLine).toSeq.flatMap(_.subgroups)
          val parameterChoice: String = parametersRegEx.findFirstMatchIn(firstLine).mkString("")

          val rest = query.split("\n").tail.mkString("\n")
          val q = rest.replaceAll("#", "")
          val parts = q.split("\n\n")
          val publishPart = parts(1)
          producedText = producedText.replace(query, expandQuery(q, publishPart, emptyGraph, dir, asserts, parameterChoice))
        }
    }

    producedText
  }

  private def init() = {
    dir = createDir(section)
    db = new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder().newGraphDatabase()

    db.asInstanceOf[ImpermanentGraphDatabase].cleanContent(false)

    db.inTx(() => {
      nodeIndex = db.index().forNodes("nodeIndexName")
      relIndex = db.index().forRelationships("relationshipIndexName")
      val g = new GraphImpl(graphDescription.toArray[String])
      val description = GraphDescription.create(g)

      nodes = description.create(db).asScala.map {
        case (name, node) => name -> node.getId
      }.toMap

      properties.foreach((n) => {
        val nod = node(n._1)
        n._2.foreach((kv) => nod.setProperty(kv._1, kv._2))
      })

      db.getAllNodes.asScala.foreach((n) => {
        indexProperties(n, nodeIndex)
        n.getRelationships(Direction.OUTGOING).asScala.foreach(indexProperties(_, relIndex))
      })
    })
    engine = new ExecutionEngine(db)
    db
  }
}

