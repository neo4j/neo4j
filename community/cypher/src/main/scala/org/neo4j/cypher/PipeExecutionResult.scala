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
import scala.collection.JavaConverters._
import java.io.{StringWriter, PrintWriter}
import collection.immutable.{Map => ImmutableMap}
import collection.Map
import javacompat.{PlanDescription => JPlanDescription}

class PipeExecutionResult(result: Iterator[Map[String, Any]],
                          val columns: List[String], state: QueryState,
                          executionPlanBuilder: () => PlanDescription)
  extends ExecutionResult
  with CollectionSupport
  with ResultPrinter {

  def executionPlanDescription(): PlanDescription = executionPlanBuilder()

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

  def dumpToString(writer: PrintWriter) {
    dumpToString(writer,columns,data,state.query.timeTaken,queryStatistics)
  }

  def data = result.toList

  lazy val dumpToString: String = {
    val stringWriter = new StringWriter()
    val writer = new PrintWriter(stringWriter)
    dumpToString(writer)
    writer.close()
    stringWriter.getBuffer.toString
  }


  def hasNext: Boolean = result.hasNext

  def next(): ImmutableMap[String, Any] = result.next().toMap

  def queryStatistics = state.getStatistics

  def text(x : Any) : String = text(x, state.query)
}

