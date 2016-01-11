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

import org.neo4j.cypher.internal.compiler.v3_0.commands.values.KeyToken
import org.neo4j.cypher.internal.compiler.v3_0.helpers.IsCollection
import org.neo4j.cypher.internal.compiler.v3_0.planDescription.InternalPlanDescription
import org.neo4j.cypher.internal.compiler.v3_0.planDescription.InternalPlanDescription.Arguments.{Planner, Runtime}
import org.neo4j.cypher.internal.compiler.v3_0.spi.{InternalResultRow, InternalResultVisitor, QueryContext}
import org.neo4j.cypher.internal.compiler.v3_0.{TaskCloser, _}
import org.neo4j.cypher.internal.frontend.v3_0.EntityNotFoundException
import org.neo4j.cypher.internal.frontend.v3_0.helpers.Eagerly
import org.neo4j.cypher.internal.frontend.v3_0.notification.InternalNotification
import org.neo4j.graphdb.QueryExecutionType._
import org.neo4j.graphdb.{ResourceIterator, _}

import scala.collection.{Map, mutable}

/*
* An AcceptingExecutionResult delegates all methods to the accept method which is the responsibility
* extending classes to provide.
*/
abstract class AcceptingExecutionResult(taskCloser: TaskCloser,
                                        context: QueryContext)
  extends InternalExecutionResult with SuccessfulCloseable {
  self =>

  import scala.collection.JavaConverters._

  private lazy val innerIterator: util.Iterator[util.Map[String, Any]] = {
    val list = new util.ArrayList[util.Map[String, Any]]()
    doInAccept(populateResults(list))
    list.iterator()
  }

  override def columns: List[String] = javaColumns.asScala.toList

  override final def javaColumnAs[T](column: String): ResourceIterator[T] = new WrappingResourceIterator[T] {
    def hasNext = self.hasNext

    def next() = extractJavaColumn(column, innerIterator.next()).asInstanceOf[T]
  }

  override def columnAs[T](column: String): Iterator[T] = map { case m => extractColumn(column, m).asInstanceOf[T] }

  override def javaIterator: ResourceIterator[util.Map[String, Any]] = new
      WrappingResourceIterator[util.Map[String, Any]] {
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
    doInAccept { (row) =>
      populateResults(result)(row)
      populateDumpToStringResults(dumpToStringBuilder)(row)
    }
    val iterator = result.iterator()
    new AcceptingExecutionResult(taskCloser, context) {

      override def javaColumns: util.List[String] = self.javaColumns

      override def accept[EX <: Exception](visitor: InternalResultVisitor[EX]): Unit = throw new UnsupportedOperationException

      override def executionPlanDescription(): InternalPlanDescription = self.executionPlanDescription()
        .addArgument(Planner(planner.name)).addArgument(Runtime(runtime.name))

      override def toList = result.asScala.map(m => Eagerly.immutableMapValues(m.asScala, materializeAsScala)).toList

      override def dumpToString(writer: PrintWriter) = formatOutput(writer, columns, dumpToStringBuilder.result(),
        queryStatistics())

      override def next() = Eagerly.immutableMapValues(iterator.next().asScala, materializeAsScala)

      override def hasNext = iterator.hasNext

      override def javaIterator: ResourceIterator[util.Map[String, Any]] = new
          WrappingResourceIterator[util.Map[String, Any]] {
        def hasNext = iterator.hasNext

        def next() = iterator.next()
      }

      override def executionMode: ExecutionMode = self.executionMode

      override def queryStatistics(): InternalQueryStatistics = self.queryStatistics()
    }
  }

  private var successful = false

  def success() = {
    successful = true
  }

  override def close() = {
    taskCloser.close(success = successful)
  }

  val mode = executionMode

  override def planDescriptionRequested: Boolean = executionMode == ExplainMode || executionMode == ProfileMode

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

  private def populateResults(results: util.List[util.Map[String, Any]])(row: InternalResultRow) = {
    val map = new util.HashMap[String, Any]()
    columns.foreach(c => map.put(c, row.get(c)))
    results.add(map)
  }

  private def populateDumpToStringResults(builder: mutable.Builder[Map[String, String], Seq[Map[String, String]]])
                                         (row: InternalResultRow) = {
    val map = new mutable.HashMap[String, String]()
    columns.foreach(c => map.put(c, text(row.get(c))))

    builder += map
  }

  private def props(n: Node): String = {
    val ops = context.nodeOps
    val properties = ops.propertyKeyIds(n.getId)
    val keyValStrings = properties.
      map(pkId => context.getPropertyKeyName(pkId) + ":" + text(ops.getProperty(n.getId, pkId)))
    keyValStrings.mkString("{", ",", "}")
  }

  private def props(r: Relationship): String = {
    val ops = context.relationshipOps
    val properties = ops.propertyKeyIds(r.getId)
    val keyValStrings = properties.
      map(pkId => context.getPropertyKeyName(pkId) + ":" + text(ops.getProperty(r.getId, pkId)))
    keyValStrings.mkString("{", ",", "}")
  }

  def text(a: Any): String = a match {
    case x: Node => x.toString + props(x)
    case x: Relationship => ":" + x.getType.name() + "[" + x.getId + "]" + props(x)
    case x if x.isInstanceOf[Map[_, _]] => makeString(x.asInstanceOf[Map[String, Any]])
    case x if x.isInstanceOf[java.util.Map[_, _]] => makeString(x.asInstanceOf[java.util.Map[String, Any]].asScala)
    case IsCollection(coll) => coll.map(elem => text(elem)).mkString("[", ",", "]")
    case x: String => "\"" + x + "\""
    case v: KeyToken => v.name
    case Some(x) => x.toString
    case null => "<null>"
    case x => x.toString
  }

  def makeString(m: Map[String, Any]) = m.map {
    case (k, v) => k + " -> " + text(v)
  }.mkString("{", ", ", "}")

  private def doInAccept[T](body: InternalResultRow => T) = {
    if (!taskCloser.isClosed) {
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
    new EntityNotFoundException(
      "No column named '" + column + "' was found. Found: " + data.keys.mkString("(\"", "\", \"", "\")"))

  private trait WrappingResourceIterator[T] extends ResourceIterator[T] {

    def remove() {
      throw new UnsupportedOperationException("remove")
    }

    def close() {
      self.close()
    }
  }

  // *** Delegate to compiled code

  def executionMode: ExecutionMode

  def javaColumns: util.List[String]

  //todo this should not depend on external visitor
  def accept[EX <: Exception](visitor: InternalResultVisitor[EX]): Unit

  override def executionPlanDescription(): InternalPlanDescription
}
