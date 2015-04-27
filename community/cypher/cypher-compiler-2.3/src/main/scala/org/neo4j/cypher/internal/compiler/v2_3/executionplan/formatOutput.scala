/*
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
package org.neo4j.cypher.internal.compiler.v2_3.executionplan

import java.io.PrintWriter

import org.neo4j.cypher.internal.compiler.v2_3.InternalQueryStatistics
import org.neo4j.cypher.internal.compiler.v2_3.commands.values.KeyToken
import org.neo4j.cypher.internal.compiler.v2_3.helpers.IsCollection
import org.neo4j.graphdb.{Node, PropertyContainer, Relationship}
import org.neo4j.kernel.api.ReadOperations

import scala.collection.Map

/**
 * Creates formatted tabular output.
 */
object formatOutput extends ((ReadOperations, PrintWriter, List[String], Seq[Map[String, Any]], InternalQueryStatistics) => Unit) {
  import scala.collection.JavaConverters._

  def apply(readOperations: ReadOperations, writer: PrintWriter, columns: List[String],
            result: Seq[Map[String, Any]], queryStatistics: InternalQueryStatistics) {

    def props(x: PropertyContainer): String = {
      val (properties, propFcn, id) = x match {
        case n: Node => (readOperations.nodeGetAllProperties(n.getId).asScala.map(_.propertyKeyId()), readOperations.nodeGetProperty _, n.getId )
        case r: Relationship => (readOperations.relationshipGetAllProperties(r.getId).asScala.map(_.propertyKeyId()), readOperations.relationshipGetProperty _, r.getId)
      }

      val keyValStrings = properties.
        map(pkId => readOperations.propertyKeyGetName(pkId) + ":" + text(propFcn(id, pkId).value(null)))

      keyValStrings.mkString("{", ",", "}")
    }

      def text(a: Any): String = a match {
      case x: Node            => x.toString + props(x)
      case x: Relationship    => ":" + x.getType.name() + "[" + x.getId + "]" + props(x)
      case x if x.isInstanceOf[Map[_, _]] => makeString(x.asInstanceOf[Map[String, Any]])
      case x if x.isInstanceOf[java.util.Map[_, _]] => makeString(x.asInstanceOf[java.util.Map[String, Any]].asScala)
      case IsCollection(coll) => coll.map(elem => text(elem)).mkString("[", ",", "]")
      case x: String          => "\"" + x + "\""
      case v: KeyToken        => v.name
      case Some(x)            => x.toString
      case null               => "<null>"
      case x                  => x.toString
    }

    def textWithType(x: Any) = s"${text(x)} (${x.getClass.getSimpleName})"

    def makeString(m: Map[String, Any]) = m.map {
      case (k, v) => k + " -> " + text(v)
    }.mkString("{", ", ", "}")

    def makeSize(txt: String, wantedSize: Int): String = {
      val actualSize = txt.length()
      if (actualSize > wantedSize) {
        txt.slice(0, wantedSize)
      } else if (actualSize < wantedSize) {
        txt + repeat(" ", wantedSize - actualSize)
      } else txt
    }

    def repeat(x: String, size: Int): String = (1 to size).map((i) => x).mkString

    def createString(columnSizes: Map[String, Int], m: Map[String, Any]): String = {
      columns.map(c => {
        val length = columnSizes.get(c).get
        val txt = text(m.get(c).get)
        val value = makeSize(txt, length)
        value
      }).mkString("| ", " | ", " |")
    }

    def calculateColumnSizes(result: Seq[Map[String, Any]]): Map[String, Int] = {
      val columnSizes = new scala.collection.mutable.OpenHashMap[String, Int] ++ columns.map(name => name -> name.length)

      result.foreach((m) => {
        m.foreach((kv) => {
          val length = text(kv._2).length
          if (!columnSizes.contains(kv._1) || columnSizes.get(kv._1).get < length) {
            columnSizes.put(kv._1, length)
          }
        })
      })
      columnSizes.toMap
    }

    if (columns.nonEmpty) {
      val headers = columns.map((c) => Map[String, Any](c -> Some(c))).reduceLeft(_ ++ _)
      val columnSizes = calculateColumnSizes(result)
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


}
