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
package org.neo4j.cypher.internal.compatibility.v3_4.runtime

import java.io.PrintWriter
import java.util

import org.neo4j.cypher.CypherVersion
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.helpers.MapBasedRow
import org.neo4j.cypher.internal.util.v3_4.Eagerly.immutableMapValues
import org.neo4j.cypher.internal.runtime._
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.planDescription.InternalPlanDescription
import org.neo4j.cypher.internal.runtime.planDescription.InternalPlanDescription.Arguments.{RuntimeVersion, Version}
import org.neo4j.cypher.result.QueryResult
import org.neo4j.cypher.result.QueryResult.QueryResultVisitor
import org.neo4j.graphdb.Result.ResultVisitor
import org.neo4j.graphdb.{NotFoundException, Notification, ResourceIterator}

import scala.collection.JavaConverters._
import scala.collection.{Map, mutable}

class PipeExecutionResult(val result: ResultIterator,
                          val fieldNames: Array[String],
                          val state: QueryState,
                          val executionPlanBuilder: () => InternalPlanDescription,
                          val executionMode: ExecutionMode,
                          val queryType: InternalQueryType)
  extends InternalExecutionResult {

  self =>

  private val query = state.query
  val javaValues = new RuntimeJavaValueConverter(query.isGraphKernelResultValue)
  val scalaValues = new RuntimeScalaValueConverter(query.isGraphKernelResultValue)
  lazy val dumpToString = withDumper(dumper => dumper.dumpToString(_))

  def dumpToString(writer: PrintWriter) { withDumper(dumper => dumper.dumpToString(writer)(_)) }

  def executionPlanDescription(): InternalPlanDescription =
    executionPlanBuilder()
      .addArgument(Version(s"CYPHER ${CypherVersion.default.name}"))
      .addArgument(RuntimeVersion(CypherVersion.default.name))

  def javaColumnAs[T](column: String): ResourceIterator[T] = new WrappingResourceIterator[T] {
    def hasNext = self.hasNext
    def next() = query.asObject(result.next().getOrElse(column, columnNotFoundException(column, columns))).asInstanceOf[T]
  }

  def columnAs[T](column: String): Iterator[T] =
    if (this.columns.contains(column)) map { case m => getAnyColumn(column, m).asInstanceOf[T] }
    else columnNotFoundException(column, columns)

  def javaIterator: ResourceIterator[java.util.Map[String, Any]] = new WrappingResourceIterator[util.Map[String, Any]] {
    def hasNext = self.hasNext
    def next() = immutableMapValues(result.next(), query.asObject).asJava
  }

  override def toList: List[Predef.Map[String, Any]] = result.toList.map(immutableMapValues(_, query.asObject))
    .map(immutableMapValues(_, scalaValues.asDeepScalaValue))

  def hasNext = result.hasNext

  def next() = immutableMapValues(immutableMapValues(result.next(), query.asObject), scalaValues.asDeepScalaValue)

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
  override val notifications = Iterable.empty[Notification]
  override def withNotifications(notification: Notification*): InternalExecutionResult = this

  def accept[EX <: Exception](visitor: QueryResultVisitor[EX]): Unit = {
    try {
      javaValues.feedIteratorToVisitable(result.map(r => fieldNames.map(r))).accept(visitor)
    } finally {
      self.close()
    }
  }

  override def accept[E <: Exception](visitor: ResultVisitor[E]): Unit = {
    accept(new QueryResultVisitor[E] {
      override def visit(record: QueryResult.Record): Boolean = {
        val fields = record.fields()
        val mapData = new mutable.AnyRefMap[String, Any](fieldNames.length)
        for (i <- 0 until fieldNames.length) {
          mapData.put(fieldNames(i), state.query.asObject(fields(i)))
        }
        visitor.visit(new MapBasedRow(mapData))
      }
    })
  }
}
