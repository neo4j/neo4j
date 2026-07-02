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
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.NodeFulltextIndexSearchPipe.fulltextSearchCursor
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.operations.CypherFunctions
import org.neo4j.internal.kernel.api.IndexQueryConstraints
import org.neo4j.internal.kernel.api.IndexReadSession
import org.neo4j.internal.kernel.api.NodeValueIndexCursor
import org.neo4j.internal.kernel.api.PropertyIndexQuery
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values.NO_VALUE
import org.neo4j.values.storable.Values.floatValue
import org.neo4j.values.virtual.VirtualValues

abstract class BaseNodeFulltextIndexSearchPipe(
  queryStringExpression: Expression,
  analyzerExpression: Option[Expression],
  skipExpression: Option[Expression],
  limitExpression: Expression,
  queryIndexId: Int
) extends Pipe {

  protected def newRow(row: CypherRow, cursor: NodeValueIndexCursor): CypherRow

  protected def internalCreateResults(
    state: QueryState
  ): ClosingIterator[CypherRow] = {
    val query = state.query
    val incomingRow = state.newRowWithArgument(rowFactory)
    val index = state.queryIndexes(queryIndexId)
    val queryString = queryStringExpression(incomingRow, state)
    if (queryString eq NO_VALUE) {
      ClosingIterator.empty
    } else {
      new ClosingIterator[CypherRow]() {
        private[this] val cursor = fulltextSearchCursor(
          query,
          index,
          queryString,
          analyzerExpression,
          skipExpression,
          limitExpression,
          incomingRow,
          state
        )

        private[this] var _hasNext: java.lang.Boolean = _
        override protected[this] def closeMore(): Unit = cursor.close()

        override def next(): CypherRow = {
          if (hasNext) {
            val r = newRow(state.newRowWithArgument(rowFactory), cursor)
            _hasNext = null
            r
          } else {
            throw new NoSuchElementException
          }
        }

        override protected[this] def innerHasNext: Boolean = {
          if (_hasNext == null) {
            _hasNext = cursor.next()
          }
          _hasNext
        }
      }
    }
  }
}

case class NodeFulltextIndexSearchPipe(
  node: String,
  score: Option[String],
  queryStringExpression: Expression,
  analyzerExpression: Option[Expression],
  skipExpression: Option[Expression],
  limitExpression: Expression,
  queryIndexId: Int
)(val id: Id = Id.INVALID_ID)
    extends BaseNodeFulltextIndexSearchPipe(
      queryStringExpression,
      analyzerExpression,
      skipExpression,
      limitExpression,
      queryIndexId
    ) {

  private[this] val _newRow: (CypherRow, NodeValueIndexCursor) => CypherRow = score match {
    case Some(value) => (incomingRow: CypherRow, cursor: NodeValueIndexCursor) =>
        rowFactory.copyWith(
          incomingRow,
          node,
          VirtualValues.node(cursor.nodeReference()),
          value,
          floatValue(cursor.score())
        )
    case None => (incomingRow: CypherRow, cursor: NodeValueIndexCursor) =>
        rowFactory.copyWith(incomingRow, node, VirtualValues.node(cursor.nodeReference()))
  }

  override protected def newRow(row: CypherRow, cursor: NodeValueIndexCursor): CypherRow = _newRow(row, cursor)
}

object NodeFulltextIndexSearchPipe {

  def fulltextSearchCursor(
    query: QueryContext,
    index: IndexReadSession,
    queryString: AnyValue,
    analyzer: Option[Expression],
    skip: Option[Expression],
    limit: Expression,
    row: CypherRow,
    state: QueryState
  ): NodeValueIndexCursor = {
    val l = CypherFunctions.asNonNegativeIntExact(limit(row, state))
    if (l == 0) {
      NodeValueIndexCursor.EMPTY
    } else {
      val (constraints, predicate) = fulltextSearchQuery(queryString, l, analyzer, skip, row, state)
      query.nodeFulltextIndexSeek(index, constraints, predicate)
    }
  }

  /**
   * The index seek is performed separately, because the node and relationship read APIs differ.
   *
   * A NO_VALUE analyzer means "no override": it resolves to null and the index's default analyzer is used.
   */
  def fulltextSearchQuery(
    queryString: AnyValue,
    limit: Int,
    analyzer: Option[Expression],
    skip: Option[Expression],
    row: ReadableRow,
    state: QueryState
  ): (IndexQueryConstraints, PropertyIndexQuery.FulltextSearchPredicate) = {
    val analyzerOrNull = analyzer.map(_.apply(row, state)) match {
      case Some(value) if value ne NO_VALUE => CypherFunctions.asTextValue(value).stringValue()
      case _                                => null
    }
    var constraints = IndexQueryConstraints.unconstrained().limit(limit)
    skip.foreach(s => constraints = constraints.skip(CypherFunctions.asNonNegativeIntExact(s(row, state))))
    (
      constraints,
      PropertyIndexQuery.fulltextSearch(CypherFunctions.asTextValue(queryString).stringValue(), analyzerOrNull)
    )
  }
}
