/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_0.executionplan

import java.io.{PrintWriter, StringWriter}
import java.util

import org.neo4j.cypher.internal.compiler.v3_0.helpers.{JavaResultValueConverter, ScalaResultValueConverter, TextResultValueConverter}
import org.neo4j.cypher.internal.compiler.v3_0.planDescription.InternalPlanDescription
import org.neo4j.cypher.internal.compiler.v3_0.planDescription.InternalPlanDescription.Arguments.{Planner, Runtime}
import org.neo4j.cypher.internal.compiler.v3_0.spi.{InternalResultRow, InternalResultVisitor, QueryContext}
import org.neo4j.cypher.internal.compiler.v3_0.{TaskCloser, _}
import org.neo4j.cypher.internal.frontend.v3_0.EntityNotFoundException
import org.neo4j.cypher.internal.frontend.v3_0.helpers.Eagerly
import org.neo4j.cypher.internal.frontend.v3_0.notification.InternalNotification
import org.neo4j.graphdb.ResourceIterator

import scala.collection.{Map, mutable}

abstract class StandardInternalExecutionResult(context: QueryContext,
                                               taskCloser: Option[TaskCloser] = None)
  extends InternalExecutionResult
    with SuccessfulCloseable {

  self =>

  import scala.collection.JavaConverters._

  protected val isGraphKernelResultValue = context.isGraphKernelResultValue _
  private val scalaValues = new ScalaResultValueConverter(isGraphKernelResultValue)

  private var successful = false

  protected def isOpen = !isClosed
  protected def isClosed = taskCloser.exists(_.isClosed)

  override def hasNext = inner.hasNext
  override def next() = scalaValues.asDeepScalaResultMap(inner.next())

  // Override one of them in subclasses
  override def javaColumns: util.List[String] = columns.asJava
  override def columns: List[String] = javaColumns.asScala.toList

  override def columnAs[T](column: String): Iterator[T] = map { case m => extractScalaColumn(column, m).asInstanceOf[T] }

  override def javaIterator: ResourceIterator[util.Map[String, Any]] = new ClosingJavaIterator[util.Map[String, Any]] {
    override def next(): util.Map[String, Any] = inner.next()
  }
  override def javaColumnAs[T](column: String): ResourceIterator[T] =  new ClosingJavaIterator[T] {
    override def next(): T = extractJavaColumn(column, inner.next()).asInstanceOf[T]
  }

  override def dumpToString(): String = {
    val stringWriter = new StringWriter()
    val writer = new PrintWriter(stringWriter)
    dumpToString(writer)
    writer.close()
    stringWriter.getBuffer.toString
  }

  override def dumpToString(writer: PrintWriter) = {
    val builder = Seq.newBuilder[Map[String, String]]
    doInAccept(populateDumpToStringResults(builder))
    formatOutput(writer, columns, builder.result(), queryStatistics())
  }

  def success() = {
    successful = true
  }

  override def planDescriptionRequested: Boolean = executionMode == ExplainMode || executionMode == ProfileMode
  override def notifications = Iterable.empty[InternalNotification]

  override def close() = {
    taskCloser.foreach(_.close(success = successful))
  }

  /*
   * NOTE: This should ony be used for testing, it creates an InternalExecutionResult
   * where you can call both toList and dumpToString
   */
  def toEagerResultForTestingOnly(planner: PlannerName, runtime: RuntimeName): InternalExecutionResult = {
    val dumpToStringBuilder = Seq.newBuilder[Map[String, String]]
    val result = new util.ArrayList[util.Map[String, Any]]()
    doInAccept { (row) =>
      populateResults(result)(row)
      populateDumpToStringResults(dumpToStringBuilder)(row)
    }
    new StandardInternalExecutionResult(context, taskCloser)
      with StandardInternalExecutionResult.AcceptByIterating {

      override protected def createInner = result.iterator()

      override def javaColumns: util.List[String] = self.javaColumns

      override def executionPlanDescription(): InternalPlanDescription =
        self.executionPlanDescription().addArgument(Planner(planner.name)).addArgument(Runtime(runtime.name))

      override def toList = result.asScala.map(m => Eagerly.immutableMapValues(m.asScala, scalaValues.asDeepScalaResultValue)).toList

      override def dumpToString(writer: PrintWriter) =
        formatOutput(writer, columns, dumpToStringBuilder.result(), queryStatistics())

      override def executionMode: ExecutionMode = self.executionMode
      override def queryStatistics(): InternalQueryStatistics = self.queryStatistics()
      override def executionType: InternalQueryType = self.executionType
    }
  }

  protected final lazy val inner = createInner

  protected def createInner: util.Iterator[util.Map[String, Any]]

  protected def doInAccept[T](body: InternalResultRow => T) = {
    if (isOpen) {
      accept(new InternalResultVisitor[RuntimeException] {
        override def visit(row: InternalResultRow): Boolean = {
          body(row)
          true
        }
      })
    } else {
      throw new IllegalStateException("Unable to accept visitors after resources have been closed.")
    }
  }

  protected def populateDumpToStringResults(builder: mutable.Builder[Map[String, String], Seq[Map[String, String]]])
                                           (row: InternalResultRow) = {
    val textValues = new TextResultValueConverter(scalaValues)(context)
    val map = new mutable.HashMap[String, String]()
    columns.foreach(c => map.put(c, textValues.asTextResultValue(row.get(c))))

    builder += map
  }

  protected def populateResults(results: util.List[util.Map[String, Any]])(row: InternalResultRow) = {
    val map = new util.HashMap[String, Any]()
    columns.foreach(c => map.put(c, row.get(c)))
    results.add(map)
  }

  private def extractScalaColumn(column: String, data: Map[String, Any]): Any = {
    data.getOrElse(column, {
      throw columnNotFound(column, data)
    })
  }

  private def extractJavaColumn(column: String, data: util.Map[String, Any]): Any = {
    val value = data.get(column)
    if (value == null) {
      throw columnNotFound(column, data.asScala)
    }
    value
  }

  private def columnNotFound(column: String, data: Map[_, _]) =
    new EntityNotFoundException(s"No column named '$column' was found. Found: ${data.keys.mkString("(\"", "\", \"", "\")")}")

  private abstract class ClosingJavaIterator[A] extends ResourceIterator[A] {
      def hasNext = self.hasNext

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

  trait AcceptByIterating {

    self: StandardInternalExecutionResult =>

    val javaValues = new JavaResultValueConverter(isGraphKernelResultValue)

    @throws(classOf[Exception])
    def accept[EX <: Exception](visitor: InternalResultVisitor[EX]) = {
      javaValues.iteratorToVisitable(self).accept(visitor)
    }
  }
}


