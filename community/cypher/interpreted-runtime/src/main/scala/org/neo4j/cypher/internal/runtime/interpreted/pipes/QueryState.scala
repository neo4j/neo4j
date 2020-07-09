/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.ExpressionCursors
import org.neo4j.cypher.internal.runtime.InputDataStream
import org.neo4j.cypher.internal.runtime.MapCypherRow
import org.neo4j.cypher.internal.runtime.NoInput
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.QueryMemoryTracker
import org.neo4j.cypher.internal.runtime.QueryStatistics
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.PathValueBuilder
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.InCheckContainer
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.SingleThreadedLRUCache
import org.neo4j.internal.kernel.api.IndexReadSession
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.values.AnyValue

class QueryState(val query: QueryContext,
                 val resources: ExternalCSVResource,
                 val params: Array[AnyValue],
                 val cursors: ExpressionCursors,
                 val queryIndexes: Array[IndexReadSession],
                 val expressionVariables: Array[AnyValue],
                 val subscriber: QuerySubscriber,
                 val memoryTracker: QueryMemoryTracker,
                 val decorator: PipeDecorator = NullPipeDecorator,
                 val initialContext: Option[CypherRow] = None,
                 val cachedIn: SingleThreadedLRUCache[Any, InCheckContainer] = new SingleThreadedLRUCache(maxSize = 16),
                 val lenientCreateRelationship: Boolean = false,
                 val prePopulateResults: Boolean = false,
                 val input: InputDataStream = NoInput) extends AutoCloseable {

  private var _pathValueBuilder: PathValueBuilder = _
  private var _rowFactory: CypherRowFactory = _

  def newRow(rowFactory: CypherRowFactory): CypherRow = {
    initialContext match {
      case Some(init) => rowFactory.copyWith(init)
      case None => rowFactory.newRow()
    }
  }

  /**
   * When running on the RHS of an Apply, this method will fill the new row with argument data
   */
  def newRowWithArgument(rowFactory: CypherRowFactory): CypherRow = {
    initialContext match {
      case Some(init) => rowFactory.copyArgumentOf(init)
      case None => rowFactory.newRow()
    }
  }

  def clearPathValueBuilder: PathValueBuilder = {
    if (_pathValueBuilder == null) {
      _pathValueBuilder = new PathValueBuilder()
    }
    _pathValueBuilder.clear()
  }

  def getStatistics: QueryStatistics = query.getOptStatistics.getOrElse(QueryState.defaultStatistics)

  def withDecorator(decorator: PipeDecorator) =
    new QueryState(query, resources, params, cursors, queryIndexes, expressionVariables, subscriber, memoryTracker, decorator, initialContext,
      cachedIn, lenientCreateRelationship, prePopulateResults, input)

  def withInitialContext(initialContext: CypherRow) =
    new QueryState(query, resources, params, cursors, queryIndexes, expressionVariables, subscriber, memoryTracker, decorator, Some(initialContext),
      cachedIn, lenientCreateRelationship, prePopulateResults, input)

  def withQueryContext(query: QueryContext) =
    new QueryState(query, resources, params, cursors, queryIndexes, expressionVariables, subscriber, memoryTracker, decorator, initialContext,
      cachedIn, lenientCreateRelationship, prePopulateResults, input)

  def setExecutionContextFactory(rowFactory: CypherRowFactory): Unit = {
    _rowFactory = rowFactory
  }

  def rowFactory: CypherRowFactory = _rowFactory

  override def close(): Unit = {
    cursors.close()
  }
}

object QueryState {

  val defaultStatistics = QueryStatistics()
}

trait CypherRowFactory {

  def newRow(): CypherRow

  def copyArgumentOf(row: ReadableRow): CypherRow

  def copyWith(row: ReadableRow): CypherRow

  def copyWith(row: ReadableRow, newEntries: Seq[(String, AnyValue)]): CypherRow

  def copyWith(row: ReadableRow, key: String, value: AnyValue): CypherRow

  def copyWith(row: ReadableRow, key1: String, value1: AnyValue, key2: String, value2: AnyValue): CypherRow

  def copyWith(row: ReadableRow,
               key1: String, value1: AnyValue,
               key2: String, value2: AnyValue,
               key3: String, value3: AnyValue): CypherRow
}

case class CommunityCypherRowFactory() extends CypherRowFactory {

  override def newRow(): CypherRow = CypherRow.empty

  override def copyArgumentOf(row: ReadableRow): CypherRow = copyWith(row)

  // Not using polymorphism here, instead cast since the cost of being megamorhpic is too high
  override def copyWith(row: ReadableRow): CypherRow = row match {
    case context: MapCypherRow =>
      context.createClone()
  }

  // Not using polymorphism here, instead cast since the cost of being megamorhpic is too high
  override def copyWith(row: ReadableRow, newEntries: Seq[(String, AnyValue)]): CypherRow = row match {
    case context: MapCypherRow =>
      context.copyWith(newEntries)
  }

  // Not using polymorphism here, instead cast since the cost of being megamorhpic is too high
  override def copyWith(row: ReadableRow, key: String, value: AnyValue): CypherRow = row match {
    case context: MapCypherRow =>
      context.copyWith(key, value)
  }

  // Not using polymorphism here, instead cast since the cost of being megamorhpic is too high
  override def copyWith(row: ReadableRow, key1: String, value1: AnyValue, key2: String, value2: AnyValue): CypherRow = row match {
    case context: MapCypherRow =>
      context.copyWith(key1, value1, key2, value2)
    }

  // Not using polymorphism here, instead cast since the cost of being megamorhpic is too high
  override def copyWith(row: ReadableRow, key1: String, value1: AnyValue, key2: String, value2: AnyValue, key3: String, value3: AnyValue): CypherRow = row match {
    case context: MapCypherRow =>
      context.copyWith(key1, value1, key2, value2, key3, value3)
  }
}
