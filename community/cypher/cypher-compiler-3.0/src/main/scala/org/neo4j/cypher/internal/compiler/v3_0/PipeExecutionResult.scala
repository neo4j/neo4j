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
package org.neo4j.cypher.internal.compiler.v3_0

import java.io.PrintWriter
import java.util

import org.neo4j.cypher.internal.compiler.v3_0.executionplan.{InternalExecutionResult, InternalQueryType}
import org.neo4j.cypher.internal.compiler.v3_0.helpers.{CollectionSupport, RuntimeJavaValueConverter}
import org.neo4j.cypher.internal.compiler.v3_0.pipes.QueryState
import org.neo4j.cypher.internal.compiler.v3_0.planDescription.InternalPlanDescription
import org.neo4j.cypher.internal.compiler.v3_0.spi.{InternalResultVisitor, QueryContext}
import org.neo4j.cypher.internal.frontend.v3_0.helpers.Eagerly
import org.neo4j.cypher.internal.frontend.v3_0.notification.InternalNotification
import org.neo4j.graphdb.{NotFoundException, ResourceIterator}

import scala.collection.JavaConverters._
import scala.collection.Map

class PipeExecutionResult(val result: ResultIterator,
                          val columns: List[String],
                          val state: QueryState,
                          val executionPlanBuilder: () => InternalPlanDescription,
                          val executionMode: ExecutionMode,
                          val executionType: InternalQueryType)
  extends InternalExecutionResult
  with CollectionSupport {

  self =>

  val javaValues = new RuntimeJavaValueConverter(state.query.isGraphKernelResultValue)
  lazy val dumpToString = withDumper(dumper => dumper.dumpToString(_))

  def dumpToString(writer: PrintWriter) { withDumper(dumper => dumper.dumpToString(writer)(_)) }

  def executionPlanDescription(): InternalPlanDescription = executionPlanBuilder()

  def javaColumns: java.util.List[String] = columns.asJava

  def javaColumnAs[T](column: String): ResourceIterator[T] = new WrappingResourceIterator[T] {
    def hasNext = self.hasNext
    def next() = javaValues.asDeepJavaValue(getAnyColumn(column, self.next())).asInstanceOf[T]
  }

  def columnAs[T](column: String): Iterator[T] =
    if (this.columns.contains(column)) map { case m => getAnyColumn(column, m).asInstanceOf[T] }
    else columnNotFoundException(column, columns)

  def javaIterator: ResourceIterator[java.util.Map[String, Any]] = new WrappingResourceIterator[util.Map[String, Any]] {
    def hasNext = self.hasNext
    def next() = Eagerly.immutableMapValues(self.next(), javaValues.asDeepJavaValue).asJava
  }

  override def toList: List[Predef.Map[String, Any]] = result.toList

  def hasNext = result.hasNext

  def next() = result.next()

  def queryStatistics() = state.getStatistics

  def close() { result.close() }

  def planDescriptionRequested = executionMode == ExplainMode || executionMode == ProfileMode

  private def columnNotFoundException(column: String, expected: Iterable[String]) =
    throw new NotFoundException("No column named '" + column + "' was found. Found: " + expected.mkString("(\"", "\", \"", "\")"))

  private def getAnyColumn[T](column: String, m: Map[String, Any]): Any =
    m.getOrElse(column, columnNotFoundException(column, m.keys))


  private def withDumper[T](f: (ExecutionResultDumper) => (QueryContext => T)): T = {
    val result = toList
    state.query.withAnyOpenQueryContext( qtx => f(ExecutionResultDumper(result, columns, queryStatistics()))(qtx) )
  }

  private trait WrappingResourceIterator[T] extends ResourceIterator[T] {
    def remove() { throw new UnsupportedOperationException("remove") }
    def close() { self.close() }
  }

  //notifications only present for EXPLAIN
  override val notifications = Iterable.empty[InternalNotification]

  def accept[EX <: Exception](visitor: InternalResultVisitor[EX]) = {
    try {
      javaValues.feedIteratorToVisitable(self).accept(visitor)
    } finally {
      self.close()
    }
  }
}
