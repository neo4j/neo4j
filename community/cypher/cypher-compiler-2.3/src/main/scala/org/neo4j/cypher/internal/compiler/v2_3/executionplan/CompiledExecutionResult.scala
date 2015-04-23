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

import java.io.{PrintWriter, StringWriter}
import java.util
import java.util.Collections

import org.neo4j.cypher.internal.compiler.v2_3.commands.values.KeyToken
import org.neo4j.cypher.internal.compiler.v2_3.helpers.{Eagerly, IsCollection}
import org.neo4j.cypher.internal.compiler.v2_3.notification.InternalNotification
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.InternalPlanDescription
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.InternalPlanDescription.Arguments.{Runtime, Planner}
import org.neo4j.cypher.internal.compiler.v2_3.{ExecutionMode, ExplainMode, ProfileMode, _}
import org.neo4j.graphdb.QueryExecutionType._
import org.neo4j.graphdb.Result.{ResultRow, ResultVisitor}
import org.neo4j.graphdb._
import org.neo4j.kernel.api.Statement

import scala.collection.{Map, mutable}

/**
 * Base class for compiled execution results, implements everything in InternalExecutionResult
 * except `javaColumns` and `accept` which should be implemented by the generated classes.
 */
abstract class CompiledExecutionResult(taskCloser: TaskCloser, statement:Statement, executionMode:ExecutionMode, description:InternalPlanDescription) extends InternalExecutionResult {
  self =>

  import scala.collection.JavaConverters._

  protected var innerIterator: Iterator[Map[String, Any]] = null

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

  /*
   * NOTE: This should ony be used for testing, it creates an InternalExecutionResult
   * where you can call both toList and dumpToString
   */
  def toEagerIterableResult(planner: PlannerName, runtime: RuntimeName): InternalExecutionResult = {
    val dumpToStringBuilder = Seq.newBuilder[Map[String, String]]
    val resultBuilder = Seq.newBuilder[Map[String, Any]]
    doInAccept{ (row) =>
      populateResults(resultBuilder)(row)
      populateDumpToStringResults(dumpToStringBuilder)(row)
    }
    val result = resultBuilder.result()
    val iterator = result.toIterator
    new CompiledExecutionResult(taskCloser, statement, executionMode, description) {

      override def javaColumns: util.List[String] = self.javaColumns

      override def accept[EX <: Exception](visitor: ResultVisitor[EX]): Unit = throw new UnsupportedOperationException

      override def executionPlanDescription(): InternalPlanDescription = self.executionPlanDescription().addArgument(Planner(planner.name)).addArgument(Runtime(runtime.name))

      override def toList = result.map(Eagerly.immutableMapValues(_, materialize)).toList

      override def dumpToString(writer: PrintWriter) = formatOutput(writer, columns, dumpToStringBuilder.result(), queryStatistics())

      override def next() = Eagerly.immutableMapValues(iterator.next(), materialize)

      override def hasNext = iterator.hasNext
    }

  }

  override def queryStatistics() = InternalQueryStatistics()

  private var successful = false
  protected def success() = {
    successful = true
  }

  override def close() = {
    taskCloser.close(success = successful)
  }

  override def executionPlanDescription() = description

  def mode = executionMode

  override def planDescriptionRequested: Boolean =  executionMode == ExplainMode || executionMode == ProfileMode

  override def executionType = if (executionMode == ProfileMode) {
    profiled(queryType)
  } else {
    query(queryType)
  }

  override def notifications = Iterable.empty[InternalNotification]

  override def hasNext = {
    ensureIterator()
    innerIterator.hasNext
  }

  override def next() = {
    ensureIterator()
    Eagerly.immutableMapValues(innerIterator.next(), materialize)
  }

  //TODO when allowing writes this should be moved to the generated class
  protected def queryType: QueryType = QueryType.READ_ONLY

  private def ensureIterator() = {
    if (innerIterator == null) {
      val res = Seq.newBuilder[Map[String, Any]]
      doInAccept(populateResults(res))
      innerIterator = res.result().toIterator
    }
  }

  private def populateResults(builder: mutable.Builder[Map[String, Any], Seq[Map[String, Any]]])(row: ResultRow) = {
    val map = new mutable.HashMap[String, Any]()
    columns.foreach(c => map.put(c, row.get(c)))
    builder += map
  }

  private def populateDumpToStringResults(builder: mutable.Builder[Map[String, String], Seq[Map[String, String]]])(row: ResultRow) = {
    val map = new mutable.HashMap[String, String]()
    columns.foreach(c => map.put(c, text(row.get(c))))

    builder += map
  }

  private def props(x: PropertyContainer): String = {
    val readOperations = statement.readOperations()
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

  def makeString(m: Map[String, Any]) = m.map {
    case (k, v) => k + " -> " + text(v)
  }.mkString("{", ", ", "}")

  private def doInAccept[T](body: ResultRow => T) = {
    accept(new ResultVisitor[RuntimeException] {
      override def visit(row: ResultRow): Boolean = {
        body(row)
        true
      }
    })
  }

  private def materialize(v: Any): Any = v match {
    case (x: Stream[_])   => x.map(materialize).toList
    case (x: Map[_, _])   => Eagerly.immutableMapValues(x, materialize)
    case (x: Iterable[_]) => x.map(materialize)
    case x => x
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

