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
package org.neo4j.cypher

import internal.helpers.CollectionSupport
import internal.pipes.QueryState
import internal.{ResultValueMapper, StringExtras}
import internal.commands.expressions.StringHelper
import scala.collection.JavaConverters._
import java.io.{StringWriter, PrintWriter}
import collection.immutable.{Map => ImmutableMap}
import collection.Map

class
PipeExecutionResult(result: Iterator[Map[String, Any]], val columns: List[String], state:QueryState)
  extends ExecutionResult
  with StringExtras
  with CollectionSupport
  with StringHelper {

  def javaColumns: java.util.List[String] = columns.asJava

  def javaColumnAs[T](column: String): java.util.Iterator[T] = columnAs[T](column).map(x => makeValueJavaCompatible(x).asInstanceOf[T]).asJava

  def columnAs[T](column: String): Iterator[T] = map {
    case m => {
      val item: Any = m.getOrElse(column, throw new EntityNotFoundException("No column named '" + column + "' was found. Found: " + m.keys.mkString("(\"", "\", \"", "\")")))
      item.asInstanceOf[T]
    }
  }

  private def makeValueJavaCompatible(value: Any): Any = value match {
    case iter: Seq[_] => iter.map(makeValueJavaCompatible).asJava
    case x => x
  }

  def javaIterator: java.util.Iterator[java.util.Map[String, Any]] = this.map(m => {
    m.map(kv => kv._1 -> makeValueJavaCompatible(kv._2)).asJava
  }).toIterator.asJava

  private def calculateColumnSizes(result: Seq[Map[String, Any]]): Map[String, Int] = {
    val columnSizes = new scala.collection.mutable.HashMap[String, Int] ++ columns.map(name => name -> name.size)

    result.foreach((m) => {
      m.foreach((kv) => {
        val length = text(kv._2, state.queryContext).size
        if (!columnSizes.contains(kv._1) || columnSizes.get(kv._1).get < length) {
          columnSizes.put(kv._1, length)
        }
      })
    })
    columnSizes.toMap
  }

  protected def createTimedResults: (List[Map[String, Any]], String) = {
    val start = System.currentTimeMillis()
    val eagerResult = result.toList
    val ms = System.currentTimeMillis() - start

    (eagerResult, ms.toString)
  }

  def dumpToString(writer: PrintWriter) {
    val (eagerResult, timeTaken) = createTimedResults

    val columnSizes = calculateColumnSizes(eagerResult)

    if (columns.nonEmpty) {
      val headers = columns.map((c) => Map[String, Any](c -> Some(c))).reduceLeft(_ ++ _)
      val headerLine: String = createString(columns, columnSizes, headers)
      val lineWidth: Int = headerLine.length - 2
      val --- = "+" + repeat("-", lineWidth) + "+"

      val row = if (eagerResult.size > 1) "rows" else "row"
      val footer = "%d %s".format(eagerResult.size, row)

      writer.println(---)
      writer.println(headerLine)
      writer.println(---)

      eagerResult.foreach(resultLine => writer.println(createString(columns, columnSizes, resultLine)))

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



    writer.println("%s ms".format(timeTaken))
  }

  lazy val dumpToString: String = {
    val stringWriter = new StringWriter()
    val writer = new PrintWriter(stringWriter)
    dumpToString(writer)
    writer.close()
    stringWriter.getBuffer.toString
  }

  private def createString(columns: List[String], columnSizes: Map[String, Int], m: Map[String, Any]): String = {
    columns.map(c => {
      val length = columnSizes.get(c).get
      val txt = text(m.get(c).get, state.queryContext)
      val value = makeSize(txt, length)
      value
    }).mkString("| ", " | ", " |")
  }

  def hasNext: Boolean = result.hasNext

  def next(): ImmutableMap[String, Any] = result.next().toMap

  def queryStatistics = QueryStatistics.empty
}

