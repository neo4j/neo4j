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
package org.neo4j.cypher.internal.compatibility.v3_4.runtime

import java.io.{PrintWriter, StringWriter}

import org.neo4j.cypher.internal.compiler.v3_4.helpers.IsList
import org.neo4j.cypher.internal.runtime.interpreted.commands.values.KeyToken
import org.neo4j.cypher.internal.runtime.{QueryContext, QueryStatistics}
import org.neo4j.graphdb.{Node, PropertyContainer, Relationship}

import scala.collection.Map

case class ExecutionResultDumper(result: Seq[Map[String, Any]], columns: List[String], queryStatistics: QueryStatistics) {

  def dumpToString(implicit query: QueryContext): String = {
    val stringWriter = new StringWriter()
    val writer = new PrintWriter(stringWriter)
    dumpToString(writer)
    writer.close()
    stringWriter.getBuffer.toString
  }

  def dumpToString(writer: PrintWriter)(implicit query: QueryContext) {
    if (columns.nonEmpty) {
      val headers = columns.map((c) => Map[String, Any](c -> Some(c))).reduceLeft(_ ++ _)
      val columnSizes = calculateColumnSizes
      val headerLine = createString(columnSizes, headers)
      val lineWidth = headerLine.length - 2
      val --- = "+" + repeat("-", lineWidth) + "+"

      val row = if (result.size > 1) "rows" else "row"
      val footer = "%d %s".format(result.size, row)

      writer.println(---)
      writer.println(headerLine)
      writer.println(---)

      result.foreach(resultLine => writer.println(createString(columnSizes, resultLine)))

      writer.println(---)
      writer.println(footer)

      if (queryStatistics.containsUpdates) {
        writer.print(queryStatistics.toString)
      }
    } else {
      if (queryStatistics.containsUpdates) {
        writer.println("+-------------------+")
        writer.println("| No data returned. |")
        writer.println("+-------------------+")
        writer.print(queryStatistics.toString)
      } else {
        writer.println("+--------------------------------------------+")
        writer.println("| No data returned, and nothing was changed. |")
        writer.println("+--------------------------------------------+")
      }
    }
  }

  def createString(columnSizes: Map[String, Int], m: Map[String, Any])(implicit query: QueryContext): String = {
    columns.map(c => {
      val length = columnSizes.get(c).get
      val txt = serialize(m.get(c).get, query)
      val value = makeSize(txt, length)
      value
    }).mkString("| ", " | ", " |")
  }

  def calculateColumnSizes(implicit query: QueryContext): Map[String, Int] = {
    val columnSizes = new scala.collection.mutable.OpenHashMap[String, Int] ++ columns.map(name => name -> name.size)

    result.foreach((m) => {
      m.foreach((kv) => {
        val length = serialize(kv._2, query).size
        if (!columnSizes.contains(kv._1) || columnSizes.get(kv._1).get < length) {
          columnSizes.put(kv._1, length)
        }
      })
    })
    columnSizes.toMap
  }

  private def makeSize(txt: String, wantedSize: Int): String = {
    val actualSize = txt.length()
    if (actualSize > wantedSize) {
      txt.slice(0, wantedSize)
    } else if (actualSize < wantedSize) {
      txt + repeat(" ", wantedSize - actualSize)
    } else txt
  }

  private def repeat(x: String, size: Int): String = (1 to size).map((i) => x).mkString

  private def serializeProperties(x: PropertyContainer, qtx: QueryContext): String = {
    val (ops, id, deleted) = x match {
      case n: Node => (qtx.nodeOps, n.getId, qtx.nodeOps.isDeletedInThisTx(n.getId))
      case r: Relationship => (qtx.relationshipOps, r.getId, qtx.relationshipOps.isDeletedInThisTx(r.getId))
    }

    val keyValStrings = if (deleted) Iterator("deleted")
    else ops.propertyKeyIds(id).
      map(pkId => qtx.getPropertyKeyName(pkId) + ":" + serialize(ops.getProperty(id, pkId).asObject(), qtx))

    keyValStrings.mkString("{", ",", "}")
  }

  import scala.collection.JavaConverters._
  private def serialize(a: Any, qtx: QueryContext): String = a match {
    case x: Node         => x.toString + serializeProperties(x, qtx)
    case x: Relationship => ":" + x.getType.name() + "[" + x.getId + "]" + serializeProperties(x, qtx)
    case x: Any if x.isInstanceOf[Map[_, _]] => makeString(_ => x.asInstanceOf[Map[String, Any]], qtx)
    case x: Any if x.isInstanceOf[java.util.Map[_, _]] => makeString(_ => x.asInstanceOf[java.util.Map[String, Any]].asScala, qtx)
    case IsList(coll)    => coll.map(elem => serialize(elem, qtx)).mkString("[", ",", "]")
    case x: String       => "\"" + x + "\""
    case v: KeyToken     => v.name
    case Some(x)         => x.toString
    case null            => "<null>"
    case x               => x.toString
  }

  private def makeString(m: QueryContext => Map[String, Any], qtx: QueryContext) = m(qtx).map {
    case (k, v) => k + " -> " + serialize(v, qtx)
  }.mkString("{", ", ", "}")
}

