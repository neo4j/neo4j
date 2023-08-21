/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.result.string

import org.neo4j.cypher.internal.runtime.RuntimeScalaValueConverter
import org.neo4j.cypher.internal.runtime.interpreted.commands.values.KeyToken
import org.neo4j.cypher.internal.runtime.isGraphKernelResultValue
import org.neo4j.graphdb.Entity
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.NotFoundException
import org.neo4j.graphdb.Path
import org.neo4j.graphdb.QueryStatistics
import org.neo4j.graphdb.Relationship
import org.neo4j.graphdb.Result.ResultRow
import org.neo4j.graphdb.Result.ResultVisitor
import org.neo4j.kernel.impl.query.TransactionalContext

import java.io.PrintWriter

import scala.collection.mutable.ArrayBuffer

/**
 * Assembles result rows into a nicely formatted string table.
 *
 * Note: this class might be used by the docs build. Consider not changing it's
 * signature if that's what you are doing.
 */
object ResultStringBuilder {

  /**
   * Create [[ResultStringBuilder]] without the capability to determine if
   * entities have been deleted in this transaction.
   *
   * @param columns the result columns
   */
  def apply(columns: Array[String]): ResultStringBuilder =
    new ResultStringBuilder(columns, NoTransactionSupport)

  /**
   * Create [[ResultStringBuilder]] which uses a [[org.neo4j.cypher.internal.runtime.QueryTransactionalContext]] to annotate if
   * entities have been deleted in this transaction.
   *
   * @param columns the result columns
   * @param context the transactional context
   */
  def apply(columns: Array[String], context: TransactionalContext): ResultStringBuilder =
    new ResultStringBuilder(columns, InternalTransactionSupport(context))

  // INTERNALS

  trait DeletedInTx {
    def node(id: Long): Boolean
    def relationship(id: Long): Boolean
  }

  object NoTransactionSupport extends DeletedInTx {
    override def node(id: Long): Boolean = false
    override def relationship(id: Long): Boolean = false
  }

  case class InternalTransactionSupport(context: TransactionalContext) extends DeletedInTx {

    override def node(id: Long): Boolean =
      context.kernelTransaction().dataRead.nodeDeletedInTransaction(id)

    override def relationship(id: Long): Boolean =
      context.kernelTransaction().dataRead.relationshipDeletedInTransaction(id)
  }
}

/**
 * The actual builder.
 */
class ResultStringBuilder private (columns: Array[String], deletedInTx: ResultStringBuilder.DeletedInTx)
    extends ResultVisitor[Exception] {

  private val scalaValues = new RuntimeScalaValueConverter(isGraphKernelResultValue)
  private val rows = new ArrayBuffer[Array[String]]

  // ADD ROWS

  /**
   * Add a row to this result string.
   */
  def addRow(row: ResultRow): Unit = {
    val stringRow = new Array[String](columns.length)
    for (i <- columns.indices) {
      stringRow(i) = asTextValue(row.get(columns(i)))
    }
    rows += stringRow
  }

  /**
   * addRow variant that implements [[ResultVisitor]]
   */
  override def visit(row: ResultRow): Boolean = {
    addRow(row)
    true
  }

  // PRODUCE RESULT

  /**
   * Produce result by constructing a String which is returned.
   */
  def result(queryStatistics: QueryStatistics): String = {
    val sb = new StringBuilder
    result(sb, queryStatistics)
    sb.result()
  }

  /**
   * Produce result by printing all lines to a provided [[PrintWriter]].
   */
  def result(writer: PrintWriter, queryStatistics: QueryStatistics): Unit =
    FormatOutput.format(PrintWriterWrapper(writer), columns, rows, queryStatistics)

  /**
   * Produce result by printing all lines to a provided [[StringBuilder]].
   */
  def result(sb: StringBuilder, queryStatistics: QueryStatistics): Unit =
    FormatOutput.format(StringBuilderWrapper(sb), columns, rows, queryStatistics)

  // HELPERS

  private def asTextValue(a: Any): String = {
    val scalaValue = scalaValues.asShallowScalaValue(a)
    scalaValue match {
      case node: Node => s"Node[${node.getId}]${props(node)}"
      case relationship: Relationship =>
        s"${relationshipType(relationship)}[${relationship.getId}]${props(relationship)}"
      case path: Path            => pathAsTextValue(path)
      case map: Map[_, _]        => makeString(map)
      case opt: Option[_]        => opt.map(asTextValue).getOrElse("None")
      case array: Array[_]       => array.map(elem => asTextValue(elem)).mkString("[", ",", "]")
      case iterable: Iterable[_] => iterable.map(elem => asTextValue(elem)).mkString("[", ",", "]")
      case str: String           => "\"" + str + "\""
      case token: KeyToken       => token.name
      case null                  => "<null>"
      case value                 => value.toString
    }
  }

  private def makeString[K, V](m: Map[K, V]) =
    m.map { case (k, v) => s"$k -> ${asTextValue(v)}" }.mkString("{", ", ", "}")

  private def pathAsTextValue(path: Path): String = {
    val nodes = path.nodes().iterator()
    val relationships = path.relationships().iterator()
    val sb = new StringBuilder

    def formatNode(n: Node) = {
      val isDeleted = deletedInTx.node(n.getId)
      val deletedString = if (isDeleted) ",deleted" else ""
      sb ++= s"(${n.getId}$deletedString)"
    }

    def formatRelationship(leftNode: Node, r: Relationship) = {
      val isDeleted = deletedInTx.relationship(r.getId)
      val deletedString = if (isDeleted) ",deleted" else ""
      if (r.getStartNode != leftNode)
        sb += '<'
      sb ++= s"-[${r.getId}:${r.getType}$deletedString]-"
      if (r.getEndNode != leftNode)
        sb += '>'
    }

    var n = nodes.next()
    formatNode(n)
    while (relationships.hasNext) {
      val r = relationships.next()
      formatRelationship(n, r)
      n = nodes.next()
      formatNode(n)
    }

    sb.result()
  }

  private def props(n: Node): String = {
    if (deletedInTx.node(n.getId))
      "{deleted}"
    else entityProps(n)
  }

  private def props(r: Relationship): String = {
    if (deletedInTx.relationship(r.getId))
      "{deleted}"
    else entityProps(r)
  }

  private def relationshipType(r: Relationship): String = {
    if (deletedInTx.relationship(r.getId)) {
      // deleted in this tx
      ""
    } else {
      try {
        ":" + r.getType.name()
      } catch {
        // deleted in another tx
        case _: NotFoundException => ""
      }
    }
  }

  private def entityProps(e: Entity): String = {
    if (isVirtualEntityHack(e))
      "{}"
    else {
      try {
        propertiesAsTextValue(e.getAllProperties)
      } catch {
        case _: NotFoundException => "{}"
      }
    }
  }

  private def propertiesAsTextValue(properties: java.util.Map[String, Object]): String = {
    val keyValues = new ArrayBuffer[String]
    val propertyIterator = properties.entrySet().iterator()
    while (propertyIterator.hasNext) {
      val entry = propertyIterator.next()
      val key = entry.getKey
      val value = asTextValue(entry.getValue)
      keyValues += s"$key:$value"
    }
    keyValues.mkString("{", ",", "}")
  }

  private def isVirtualEntityHack(entity: Entity): Boolean = entity.getId < 0
}
