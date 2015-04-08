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
package org.neo4j.cypher.internal.compiler.v2_3.executionplan

import java.io.{PrintWriter, StringWriter}
import java.util
import java.util.Collections

import org.neo4j.cypher.internal.compiler.v2_3._
import org.neo4j.cypher.internal.compiler.v2_3.helpers.Eagerly
import org.neo4j.cypher.internal.compiler.v2_3.notification.InternalNotification
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.InternalPlanDescription
import org.neo4j.graphdb.Result.{ResultRow, ResultVisitor}
import org.neo4j.graphdb.{QueryExecutionType, ResourceIterator}

import scala.collection.{Map, mutable}

/**
 * Base class for compiled execution results, implements everything in InternalExecutionResult
 * except `javaColumns` and `accept` which should be implemented by the generated classes.
 */
abstract class CompiledExecutionResult extends InternalExecutionResult {
  self =>

  import scala.collection.JavaConverters._

  protected var innerIterator: ResultIterator = null

  override def columns: List[String] = javaColumns.asScala.toList

  def javaColumnAs[T](column: String): ResourceIterator[T] = new WrappingResourceIterator[T] {
    def hasNext = self.hasNext
    def next() = makeValueJavaCompatible(getAnyColumn(column, self.next())).asInstanceOf[T]
  }

  override def columnAs[T](column: String): Iterator[T] = javaColumnAs(column).asScala

  override def javaIterator: ResourceIterator[util.Map[String, Any]] = new WrappingResourceIterator[util.Map[String, Any]] {
    def hasNext = self.hasNext
    def next() = Eagerly.immutableMapValues(self.next(), makeValueJavaCompatible).asJava
  }

  override def dumpToString(writer: PrintWriter) = {
    ensureIterator()
    //todo make pretty
    writer.println(innerIterator.toList)
  }

  override def dumpToString(): String = {
    val stringWriter = new StringWriter()
    val writer = new PrintWriter(stringWriter)
    dumpToString(writer)
    writer.close()
    stringWriter.getBuffer.toString
  }

  override def queryStatistics(): InternalQueryStatistics = InternalQueryStatistics()

  override def close(): Unit = {
    ensureIterator()
    innerIterator.close()
  }

  override def planDescriptionRequested: Boolean = ???

  override def executionType: QueryExecutionType = ???

  override def notifications = Iterable.empty[InternalNotification]

  override def hasNext = {
    ensureIterator()
    innerIterator.hasNext
  }

  override def next() = {
    ensureIterator()
    innerIterator.next()
  }

  private def ensureIterator() = {
    if (innerIterator == null) {
      val res = Seq.newBuilder[Map[String, Any]]
      accept(new ResultVisitor[RuntimeException] {
        private val cols = columns
        override def visit(row: ResultRow): Boolean = {
          val map = new mutable.HashMap[String, Any]()
          cols.foreach(c => map.put(c, row.get(c)))
          res += map
          true
        }
      })
      innerIterator = new ClosingIterator(res.result().toIterator, new TaskCloser, identity)
    }
  }

  private def makeValueJavaCompatible(value: Any): Any = value match {
    case iter: Seq[_]    => iter.map(makeValueJavaCompatible).asJava
    case iter: Map[_, _] => Eagerly.immutableMapValues(iter, makeValueJavaCompatible).asJava
    case x               => x
  }

  private def getAnyColumn[T](column: String, m: Map[String, Any]): Any = {
    m.getOrElse(column, {
      throw new EntityNotFoundException("No column named '" + column + "' was found. Found: " + m.keys.mkString("(\"", "\", \"", "\")"))
    })
  }

  private trait WrappingResourceIterator[T] extends ResourceIterator[T] {
    def remove() { Collections.emptyIterator[T]().remove() }
    def close() { self.close() }
  }
}
