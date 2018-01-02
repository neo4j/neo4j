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
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.executionplan

import java.io.{PrintWriter, StringWriter}
import java.util

import org.neo4j.cypher.internal.compatibility.v3_4.runtime._
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.helpers.{MapBasedRow, RuntimeTextValueConverter}
import org.neo4j.cypher.internal.runtime._
import org.neo4j.cypher.internal.util.v3_4.{Eagerly, TaskCloser}
import org.neo4j.cypher.internal.runtime.planDescription.InternalPlanDescription
import org.neo4j.cypher.internal.runtime.planDescription.InternalPlanDescription.Arguments.{Runtime, RuntimeImpl}
import org.neo4j.cypher.result.QueryResult
import org.neo4j.cypher.result.QueryResult.QueryResultVisitor
import org.neo4j.graphdb.{NotFoundException, Notification, ResourceIterator}
import org.neo4j.graphdb.Result.{ResultRow, ResultVisitor}

import scala.collection.{Map, mutable}

abstract class StandardInternalExecutionResult(context: QueryContext, runtime: RuntimeName,
                                               taskCloser: Option[TaskCloser] = None)
  extends InternalExecutionResult
    with Completable {

  self =>

  import scala.collection.JavaConverters._

  protected val isGraphKernelResultValue = context.isGraphKernelResultValue _
  private val scalaValues = new RuntimeScalaValueConverter(isGraphKernelResultValue)

  protected def isOpen: Boolean = !isClosed

  protected def isClosed: Boolean = taskCloser.exists(_.isClosed)

  override def hasNext: Boolean = inner.hasNext

  override def next(): Predef.Map[String, Any] = scalaValues.asDeepScalaMap(inner.next())

  // Override one of them in subclasses
  override def columnAs[T](column: String): Iterator[T] =
    if (this.columns.contains(column)) map(m => extractScalaColumn(column, m).asInstanceOf[T])
    else throw columnNotFound(column, columns)

  override def javaIterator: ResourceIterator[util.Map[String, Any]] = new ClosingJavaIterator[util.Map[String, Any]] {
    override def next(): util.Map[String, Any] = inner.next()
  }

  override def javaColumnAs[T](column: String): ResourceIterator[T] = new ClosingJavaIterator[T] {
    override def next(): T = extractJavaColumn(column, inner.next()).asInstanceOf[T]
  }

  override def dumpToString(): String = {
    val stringWriter = new StringWriter()
    val writer = new PrintWriter(stringWriter)
    dumpToString(writer)
    writer.close()
    stringWriter.getBuffer.toString
  }

  override def dumpToString(writer: PrintWriter): Unit = {
    val builder = Seq.newBuilder[Map[String, String]]
    doInAccept(populateDumpToStringResults(builder))
    formatOutput(writer, columns, builder.result(), queryStatistics())
  }

  override def planDescriptionRequested: Boolean = executionMode == ExplainMode || executionMode == ProfileMode
  override def notifications = Iterable.empty[Notification]

  override def close(): Unit = {
    completed(success = true)
  }

  override def completed(success: Boolean): Unit = {
    taskCloser.foreach(_.close(success = success))
  }

  override def accept[E <: Exception](visitor: ResultVisitor[E]): Unit = {
    accept(new QueryResultVisitor[E] {
      val names = fieldNames()
      override def visit(record: QueryResult.Record): Boolean = {
        val fields = record.fields()
        val mapData = new mutable.AnyRefMap[String, Any](names.length)
        for (i <- 0 until names.length) {
          mapData.put(names(i), context.asObject(fields(i)))
        }
        visitor.visit(new MapBasedRow(mapData))
      }
    })
  }

  /*
     * NOTE: This should ony be used for testing, it creates an InternalExecutionResult
     * where you can call both toList and dumpToString
     */
  def toEagerResultForTestingOnly(): InternalExecutionResult = {
    val dumpToStringBuilder = Seq.newBuilder[Map[String, String]]
    val result = new util.ArrayList[util.Map[String, Any]]()
    if (isOpen)
      doInAccept { (row) =>
        populateResults(result)(row)
        populateDumpToStringResults(dumpToStringBuilder)(row)
      }

    new StandardInternalExecutionResult(context, runtime, taskCloser) {

      override protected def createInner: util.Iterator[util.Map[String, Any]] = result.iterator()

      override def executionPlanDescription(): InternalPlanDescription = {
        val description = self.executionPlanDescription()
        if (!description.arguments.exists(_.isInstanceOf[Runtime])) {
          description.addArgument(Runtime(runtime.toTextOutput))
        }
        if (!description.arguments.exists(_.isInstanceOf[RuntimeImpl])) {
          description.addArgument(RuntimeImpl(runtime.name))
        }
        description
      }

      override def toList: List[Predef.Map[String, Any]] = result.asScala
        .map(m => Eagerly.immutableMapValues(m.asScala, scalaValues.asDeepScalaValue)).toList

      override def dumpToString(writer: PrintWriter): Unit =
        formatOutput(writer, columns, dumpToStringBuilder.result(), queryStatistics())

      override def executionMode: ExecutionMode = self.executionMode

      override def queryStatistics(): QueryStatistics = self.queryStatistics()

      override def queryType: InternalQueryType = self.queryType

      override def withNotifications(notification: Notification*): InternalExecutionResult = self

      override def fieldNames(): Array[String] = self.fieldNames()

      override def accept[E <: Exception](visitor: QueryResultVisitor[E]): Unit = self.accept(visitor)
    }
  }

  protected final lazy val inner: util.Iterator[util.Map[String, Any]] = createInner

  protected def createInner: util.Iterator[util.Map[String, Any]]

  protected def doInAccept[T](body: ResultRow => T): Unit = {
    if (isOpen) {
      accept(new ResultVisitor[RuntimeException] {
        override def visit(row: ResultRow): Boolean = {
          body(row)
          true
        }
      })
    } else {
      throw new IllegalStateException("Unable to accept visitors after resources have been closed.")
    }
  }

  protected def populateDumpToStringResults(builder: mutable.Builder[Map[String, String], Seq[Map[String, String]]])
                                           (row: ResultRow): builder.type = {
    val textValues = new RuntimeTextValueConverter(scalaValues)(context)
    val map = new mutable.HashMap[String, String]()
    columns.foreach(c => map.put(c, textValues.asTextValue(row.get(c))))

    builder += map
  }

  protected def populateResults(results: util.List[util.Map[String, Any]])(row: ResultRow): Boolean = {
    val map = new util.HashMap[String, Any]()
    columns.foreach(c => map.put(c, row.get(c)))
    results.add(map)
  }

  private def extractScalaColumn(column: String, data: Map[String, Any]): Any = {
    data.getOrElse(column, {
      throw columnNotFound(column, data.keys)
    })
  }

  private def extractJavaColumn(column: String, data: util.Map[String, Any]): Any = {
    val value = data.get(column)
    if (value == null) {
      throw columnNotFound(column, columns)
    }
    value
  }

  private def columnNotFound(column: String, expected: Iterable[String]) =
    new NotFoundException(s"No column named '$column' was found. Found: ${expected.mkString("(\"", "\", \"", "\")")}")

  private abstract class ClosingJavaIterator[A] extends ResourceIterator[A] {

    def hasNext: Boolean = self.hasNext

    def remove() {
      throw new UnsupportedOperationException("remove")
    }

    def close() {
      self.close()
    }
  }
}

object StandardInternalExecutionResult {

  // Accept and pull into memory when iterating
  trait IterateByAccepting {

    self: StandardInternalExecutionResult =>

    override def createInner = {
      val list = new util.ArrayList[util.Map[String, Any]]()
      if (isOpen) doInAccept(populateResults(list))
      list.iterator()
    }
  }
}




