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
import org.neo4j.cypher.internal.runtime.ClosingLongIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.PrimitiveLongHelper
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.RelationshipIterator
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.RelationshipFulltextIndexSearchPipe.fulltextSearchCursor
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.operations.CypherFunctions
import org.neo4j.internal.kernel.api.IndexReadSession
import org.neo4j.internal.kernel.api.RelationshipValueIndexCursor
import org.neo4j.storageengine.api.RelationshipVisitor
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.storable.Values.NO_VALUE

abstract class RelationshipFulltextIndexSearchPipe(
  queryStringExpression: Expression,
  analyzerExpression: Option[Expression],
  skipExpression: Option[Expression],
  limitExpression: Expression,
  queryIndexId: Int
) extends Pipe {

  protected def newRow(
    row: CypherRow,
    iterator: RelationshipFulltextSearchIterator
  ): CypherRow

  protected def iteratorFrom(cursor: RelationshipValueIndexCursor): RelationshipFulltextSearchIterator

  protected def internalCreateResults(
    state: QueryState
  ): ClosingIterator[CypherRow] = {
    val incomingRow = state.newRowWithArgument(rowFactory)
    val index = state.queryIndexes(queryIndexId)
    val queryString = queryStringExpression(incomingRow, state)
    if (queryString eq NO_VALUE) {
      ClosingIterator.empty
    } else {
      val cursor = fulltextSearchCursor(
        state.query,
        index,
        queryString,
        analyzerExpression,
        skipExpression,
        limitExpression,
        incomingRow,
        state
      )

      val iterator = iteratorFrom(cursor)
      PrimitiveLongHelper.map(
        iterator,
        _ => newRow(state.newRowWithArgument(rowFactory), iterator)
      )
    }
  }

  class RelationshipFulltextSearchIterator(cursor: RelationshipValueIndexCursor) extends ClosingLongIterator
      with RelationshipIterator {
    private[this] var hasFetchedNext = false
    private[this] var exhausted = false

    override def typeId: Int = cursor.`type`()

    def score: Float = cursor.score()

    def reference: Long = cursor.reference()

    override def startNodeId(): Long = cursor.sourceNodeReference()

    override def endNodeId(): Long = cursor.targetNodeReference()

    override protected[this] def innerHasNext: Boolean = {
      if (!hasFetchedNext) {
        hasFetchedNext = true
        if (!fetchNext()) {
          exhausted = true
        }
      }
      !exhausted
    }

    override def next(): Long = {
      if (!hasNext) {
        close()
        throw new NoSuchElementException
      }
      hasFetchedNext = false
      cursor.reference()
    }

    override def close(): Unit = {
      cursor.close()
    }

    protected[this] def fetchNext(): Boolean = {
      while (cursor.next() && cursor.readFromStore()) {
        return true
      }
      false
    }

    override def relationshipVisit[EXCEPTION <: Exception](
      relationshipId: Long,
      visitor: RelationshipVisitor[EXCEPTION]
    ): Boolean = {
      visitor.visit(reference, typeId, startNodeId(), endNodeId())
      true
    }
  }

  class NonStoreAccessingFulltextSearchIterator(cursor: RelationshipValueIndexCursor)
      extends RelationshipFulltextSearchIterator(cursor) {
    final override protected[this] def fetchNext(): Boolean = cursor.next()
  }

  class UndirectedRelationshipFulltextSearchIterator(cursor: RelationshipValueIndexCursor)
      extends RelationshipFulltextSearchIterator(cursor) {
    private[this] var emitSibling: Boolean = false

    final override protected[this] def fetchNext(): Boolean = {
      if (emitSibling) {
        emitSibling = false
        true
      } else {
        val next = super.fetchNext()
        if (next) {
          emitSibling = cursor.sourceNodeReference() != cursor.targetNodeReference()
        }
        next
      }
    }

    override def startNodeId(): Long = {
      if (emitSibling) {
        cursor.sourceNodeReference()
      } else {
        cursor.targetNodeReference()
      }
    }

    override def endNodeId(): Long = {
      if (emitSibling) {
        cursor.targetNodeReference()
      } else {
        cursor.sourceNodeReference()
      }
    }
  }
}

abstract class BaseRelationshipFulltextIndexSearchPipe(
  ident: Option[String],
  fromNode: Option[String],
  toNode: Option[String],
  score: Option[String],
  queryStringExpression: Expression,
  analyzerExpression: Option[Expression],
  skipExpression: Option[Expression],
  limitExpression: Expression,
  queryIndexId: Int
) extends RelationshipFulltextIndexSearchPipe(
      queryStringExpression,
      analyzerExpression,
      skipExpression,
      limitExpression,
      queryIndexId
    ) {

  private val relationshipWriter: Relationships.RelationshipWriter =
    Relationships.compileRelationshipWriter(ident, fromNode, toNode)

  private[this] val _newRow: (CypherRow, RelationshipFulltextSearchIterator) => CypherRow = {
    score match {
      case Some(value) =>
        (incomingRow: CypherRow, iterator: RelationshipFulltextSearchIterator) =>
          val row = relationshipWriter.writeRow(
            rowFactory,
            incomingRow,
            iterator.reference,
            iterator
          )
          row.set(value, Values.floatValue(iterator.score))
          row
      case None => (incomingRow: CypherRow, iterator: RelationshipFulltextSearchIterator) =>
          relationshipWriter.writeRow(
            rowFactory,
            incomingRow,
            iterator.reference,
            iterator
          )
    }
  }

  override protected def newRow(
    row: CypherRow,
    iterator: RelationshipFulltextSearchIterator
  ): CypherRow = _newRow(row, iterator)
}

case class DirectedRelationshipFulltextIndexSearchPipe(
  ident: Option[String],
  fromNode: Option[String],
  toNode: Option[String],
  score: Option[String],
  queryStringExpression: Expression,
  analyzerExpression: Option[Expression],
  skipExpression: Option[Expression],
  limitExpression: Expression,
  queryIndexId: Int
)(val id: Id = Id.INVALID_ID)
    extends BaseRelationshipFulltextIndexSearchPipe(
      ident,
      fromNode,
      toNode,
      score,
      queryStringExpression,
      analyzerExpression,
      skipExpression,
      limitExpression,
      queryIndexId
    ) {

  override protected def iteratorFrom(cursor: RelationshipValueIndexCursor): RelationshipFulltextSearchIterator = {
    if (fromNode.nonEmpty || toNode.nonEmpty) {
      new RelationshipFulltextSearchIterator(cursor)
    } else {
      new NonStoreAccessingFulltextSearchIterator(cursor)
    }
  }
}

case class UndirectedRelationshipFulltextIndexSearchPipe(
  ident: Option[String],
  fromNode: Option[String],
  toNode: Option[String],
  score: Option[String],
  queryStringExpression: Expression,
  analyzerExpression: Option[Expression],
  skipExpression: Option[Expression],
  limitExpression: Expression,
  queryIndexId: Int
)(val id: Id = Id.INVALID_ID)
    extends BaseRelationshipFulltextIndexSearchPipe(
      ident,
      fromNode,
      toNode,
      score,
      queryStringExpression,
      analyzerExpression,
      skipExpression,
      limitExpression,
      queryIndexId
    ) {

  override protected def iteratorFrom(cursor: RelationshipValueIndexCursor): RelationshipFulltextSearchIterator =
    new UndirectedRelationshipFulltextSearchIterator(cursor)
}

object RelationshipFulltextIndexSearchPipe {

  def fulltextSearchCursor(
    query: QueryContext,
    index: IndexReadSession,
    queryString: AnyValue,
    analyzer: Option[Expression],
    skip: Option[Expression],
    limit: Expression,
    row: CypherRow,
    state: QueryState
  ): RelationshipValueIndexCursor = {
    val l = CypherFunctions.asNonNegativeIntExact(limit(row, state))
    if (l == 0) {
      RelationshipValueIndexCursor.EMPTY
    } else {
      val (constraints, predicate) =
        NodeFulltextIndexSearchPipe.fulltextSearchQuery(queryString, l, analyzer, skip, row, state)
      query.relationshipFulltextIndexSeek(index, constraints, predicate)
    }
  }
}
