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
import org.neo4j.cypher.internal.runtime.ResourceLinenumber
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.exceptions.Neo4jException
import org.neo4j.exceptions.StatusWrapCypherException
import org.neo4j.exceptions.StatusWrapCypherException.ExtraInformation.LINE_NUMBER

class LinenumberPipeDecorator(private var inner: PipeDecorator = NullPipeDecorator) extends PipeDecorator {

  def setInnerDecorator(newDecorator: PipeDecorator): Unit = inner = newDecorator

  def getInnerDecorator: PipeDecorator = inner

  override def decorate(planId: Id, state: QueryState): QueryState = inner.decorate(planId, state)

  override def decorate(planId: Id, state: QueryState, iter: ClosingIterator[CypherRow]): ClosingIterator[CypherRow] =
    throw new UnsupportedOperationException("This method should never be called on LinenumberPipeDecorator")

  override def decorate(
    planId: Id,
    queryState: QueryState,
    iter: ClosingIterator[CypherRow],
    sourceIter: ClosingIterator[CypherRow]
  ): ClosingIterator[CypherRow] = {
    val previousContextSupplier = sourceIter match {
      case p: LinenumberIterator => () => p.previousRecord
      case _                     => () => None
    }
    decorate(planId, queryState, iter, previousContextSupplier)
  }

  override def decorate(
    planId: Id,
    queryState: QueryState,
    iter: ClosingIterator[CypherRow],
    previousContextSupplier: () => Option[CypherRow]
  ): ClosingIterator[CypherRow] = {
    new LinenumberIterator(inner.decorate(planId, queryState, iter), previousContextSupplier)
  }

  override def innerDecorator(owningPipe: Id): PipeDecorator = this

  override def afterCreateResults(planId: Id, state: QueryState): Unit = {
    inner.afterCreateResults(planId, state)
  }

  private class LinenumberIterator(inner: ClosingIterator[CypherRow], previousContextSupplier: () => Option[CypherRow])
      extends ClosingIterator[CypherRow] {

    var previousRecord: Option[CypherRow] = None

    override protected[this] def closeMore(): Unit = inner.close()

    def innerHasNext: Boolean = executeWithHandling {
      inner.hasNext
    }

    def next(): CypherRow = executeWithHandling {
      val record = inner.next()
      previousRecord = Some(record)
      record
    }

    private def executeWithHandling[T](f: => T): T = {
      try {
        f
      } catch {
        case e: StatusWrapCypherException =>
          throw addInformation(e, previousContextSupplier())
        case e: Neo4jException =>
          throw addInformation(new StatusWrapCypherException(e), previousContextSupplier())
        case e: Throwable =>
          throw e
      }
    }

    def addInformation(e: StatusWrapCypherException, maybeRow: Option[CypherRow]): StatusWrapCypherException = {
      if (!e.containsInfoFor(LINE_NUMBER)) {
        addLineNumberInfoToException(e, maybeRow)
      } else {
        e
      }
    }

    private def addLineNumberInfoToException(
      e: StatusWrapCypherException,
      maybeContext: Option[CypherRow]
    ): StatusWrapCypherException = maybeContext match {
      case Some(record: CypherRow) if record.getLinenumber.nonEmpty => e.addExtraInfo(LINE_NUMBER, errorMessage(record))
      case _                                                        => e
    }

    private def errorMessage(record: CypherRow): String = {
      record.getLinenumber match {
        case Some(ResourceLinenumber(file, line, last)) =>
          s"Failure when processing file '$file' on line $line" +
            (if (last) " (which is the last row in the file)." else ".")
        case _ => "" // should not get here TODO: throw an exception?
      }
    }
  }
}
