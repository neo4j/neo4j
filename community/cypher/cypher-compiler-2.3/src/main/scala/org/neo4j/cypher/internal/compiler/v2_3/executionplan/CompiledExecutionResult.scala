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

import org.neo4j.cypher.internal.compiler.v2_3._
import org.neo4j.cypher.internal.compiler.v2_3.commands.values.KeyToken
import org.neo4j.cypher.internal.compiler.v2_3.helpers.{Eagerly, IsCollection}
import org.neo4j.cypher.internal.compiler.v2_3.notification.InternalNotification
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.InternalPlanDescription
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.InternalPlanDescription.Arguments.{Planner, Runtime}
import org.neo4j.function.Supplier
import org.neo4j.graphdb.QueryExecutionType._
import org.neo4j.graphdb.Result.{ResultRow, ResultVisitor}
import org.neo4j.graphdb._
import org.neo4j.kernel.api.Statement

import scala.collection.{Map, mutable}

trait SuccessfulCloseable {
  def success(): Unit
  def close(): Unit
}

/**
 * Main class for compiled execution results, implements everything in InternalExecutionResult
 * except `javaColumns` and `accept` which delegates to the injected compiled code.
 */
class CompiledExecutionResult(taskCloser: TaskCloser,
                              statement: Statement,
                              compiledCode: GeneratedQueryExecution,
                              description: Supplier[InternalPlanDescription])
  extends InternalExecutionResult with SuccessfulCloseable  {
  self =>

  import scala.collection.JavaConverters._

  private lazy val innerIterator: util.Iterator[util.Map[String, Any]] = {
    val list = new util.ArrayList[util.Map[String, Any]]()
    doInAccept(populateResults(list))
    list.iterator()
  }

  compiledCode.setSuccessfulCloseable(self)

  override def columns: List[String] = javaColumns.asScala.toList

  override final def javaColumnAs[T](column: String): ResourceIterator[T] = new WrappingResourceIterator[T] {
    def hasNext = self.hasNext
    def next() = extractJavaColumn(column, innerIterator.next()).asInstanceOf[T]
  }

  override def columnAs[T](column: String): Iterator[T] = map { case m => extractColumn(column, m).asInstanceOf[T] }

  override def javaIterator: ResourceIterator[util.Map[String, Any]] = new WrappingResourceIterator[util.Map[String, Any]] {
    def hasNext = self.hasNext
    def next() = innerIterator.next()
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
    val result = new util.ArrayList[util.Map[String, Any]]()
    doInAccept{ (row) =>
      populateResults(result)(row)
      populateDumpToStringResults(dumpToStringBuilder)(row)
    }
    val iterator = result.iterator()
    new CompiledExecutionResult(taskCloser, statement, compiledCode, description) {

      override def javaColumns: util.List[String] = self.javaColumns

      override def accept[EX <: Exception](visitor: ResultVisitor[EX]): Unit = throw new UnsupportedOperationException

      override def executionPlanDescription(): InternalPlanDescription = self.executionPlanDescription().addArgument(Planner(planner.name)).addArgument(Runtime(runtime.name))

      override def toList = result.asScala.map(m => Eagerly.immutableMapValues(m.asScala, materializeAsScala)).toList

      override def dumpToString(writer: PrintWriter) = formatOutput(writer, columns, dumpToStringBuilder.result(), queryStatistics())

      override def next() = Eagerly.immutableMapValues(iterator.next().asScala, materializeAsScala)

      override def hasNext = iterator.hasNext

      override def javaIterator: ResourceIterator[util.Map[String, Any]] = new WrappingResourceIterator[util.Map[String, Any]] {
        def hasNext = iterator.hasNext
        def next() = iterator.next()
      }
    }

  }

  override def queryStatistics() = InternalQueryStatistics()

  private var successful = false

  def success() = {
    successful = true
  }

  override def close() = {
    taskCloser.close(success = successful)
  }

  val mode = executionMode

  override def planDescriptionRequested: Boolean =  executionMode == ExplainMode || executionMode == ProfileMode

  override def executionType = if (executionMode == ProfileMode) {
    profiled(queryType)
  } else {
    query(queryType)
  }

  override def notifications = Iterable.empty[InternalNotification]

  override def hasNext = innerIterator.hasNext

  override def next() = Eagerly.immutableMapValues(innerIterator.next().asScala, materializeAsScala)

  //TODO when allowing writes this should be moved to the generated class
  protected def queryType: QueryType = QueryType.READ_ONLY

  private def populateResults(results: util.List[util.Map[String, Any]])(row: ResultRow) = {
    val map = new util.HashMap[String, Any]()
    columns.foreach(c => map.put(c, row.get(c)))
    results.add(map)
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
    if (!taskCloser.isClosed) {
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

  private def materializeAsScala(v: Any): Any = {
    val scalaValue = makeValueScalaCompatible(v)

    scalaValue match {
      case (x: Stream[_]) => x.map(materializeAsScala).toList
      case (x: Map[_, _]) => Eagerly.immutableMapValues(x, materializeAsScala)
      case (x: Iterable[_]) => x.map(materializeAsScala)
      case x => x
    }
  }

  private def makeValueScalaCompatible(value: Any): Any = value match {
    case list: java.util.List[_] => list.asScala.map(makeValueScalaCompatible).toList
    case map: java.util.Map[_, _] => Eagerly.immutableMapValues(map.asScala, makeValueScalaCompatible)
    case x => x
  }

  private def extractColumn[T](column: String, data: Map[String, Any]): Any = {
    data.getOrElse(column, {
      throw columnNotFound(column, data)
    })
  }

  private def extractJavaColumn[T](column: String, data: util.Map[String, Any]): Any = {
    val value = data.get(column)
    if (value == null) {
      throw columnNotFound(column, data.asScala)
    }
    value
  }

  private def columnNotFound(column: String, data: Map[String, Any]) =
    new EntityNotFoundException("No column named '" + column + "' was found. Found: " + data.keys.mkString("(\"", "\", \"", "\")"))

  private trait WrappingResourceIterator[T] extends ResourceIterator[T] {
    def remove() { Collections.emptyIterator[T]().remove() }
    def close() { self.close() }
  }

  // *** Delegate to compiled code

  def executionMode: ExecutionMode = compiledCode.executionMode()

  override def javaColumns: util.List[String] = compiledCode.javaColumns()

  //todo this should not depend on external visitor
  override def accept[EX <: Exception](visitor: ResultVisitor[EX]): Unit =
    compiledCode.accept(visitor)

  override def executionPlanDescription(): InternalPlanDescription = {
    if (!taskCloser.isClosed) throw new ProfilerStatisticsNotReadyException

    compiledCode.executionPlanDescription()
  }
}

