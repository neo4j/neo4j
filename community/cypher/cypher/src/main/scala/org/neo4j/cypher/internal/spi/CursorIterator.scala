package org.neo4j.cypher.internal.spi

import org.neo4j.collection.primitive.PrimitiveLongResourceIterator
import org.neo4j.internal.kernel.api.helpers.RelationshipSelectionCursor
import org.neo4j.kernel.impl.api.RelationshipVisitor
import org.neo4j.kernel.impl.api.store.RelationshipIterator

import scala.collection.Iterator

abstract class CursorIterator[T] extends Iterator[T] {
  private var _next: T = fetchNext()

  protected def fetchNext(): T
  protected def close(): Unit

  override def hasNext: Boolean = _next != null

  override def next(): T = {
    if (!hasNext) {
      close()
      Iterator.empty.next()
    }

    val current = _next
    _next = fetchNext()
    if (!hasNext) {
      close()
    }
    current
  }
}

class RelationshipCursorIterator(selectionCursor: RelationshipSelectionCursor) extends RelationshipIterator {
  import RelationshipCursorIterator.{NOT_INITIALIZED, NO_ID}

  private var _next = NOT_INITIALIZED
  private var typeId: Int = NO_ID
  private var source: Long = NO_ID
  private var target: Long = NO_ID

  override def relationshipVisit[EXCEPTION <: Exception](relationshipId: Long,
                                                         visitor: RelationshipVisitor[EXCEPTION]): Boolean = {
    visitor.visit(relationshipId, typeId, source, target)
    true
  }

  private def fetchNext(): Long = if (selectionCursor.next()) selectionCursor.relationshipReference() else -1L

  override def hasNext: Boolean = {
    if (_next == NOT_INITIALIZED) {
      _next = fetchNext()
    }

    _next >= 0
  }

  //We store the current state in case the underlying cursor is
  //closed when calling next.
  private def storeState(): Unit = {
    typeId = selectionCursor.`type`()
    source = selectionCursor.sourceNodeReference()
    target = selectionCursor.targetNodeReference()
  }

  override def next(): Long = {
    if (!hasNext) {
      selectionCursor.close()
      Iterator.empty.next()
    }

    val current = _next
    storeState()
    //Note that if no more elements are found the selection cursor
    //will be closed so no need to do a extra check after fetching.
    _next = fetchNext()

    current
  }
}

object RelationshipCursorIterator {
  private val NOT_INITIALIZED = -2L
  private val NO_ID = -1
}

abstract class PrimitiveCursorIterator extends PrimitiveLongResourceIterator {
  private var _next: Long = fetchNext()

  protected def fetchNext(): Long

  override def hasNext: Boolean = _next >= 0

  override def next(): Long = {
    if (!hasNext) {
      close()
      Iterator.empty.next()
    }

    val current = _next
    _next = fetchNext()
    if (!hasNext) close()

    current
  }
}
