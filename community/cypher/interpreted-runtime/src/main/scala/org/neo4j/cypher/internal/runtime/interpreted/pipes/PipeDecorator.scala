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
import org.neo4j.cypher.internal.runtime.ResourceLinenumber
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.exceptions.LoadCsvStatusWrapCypherException
import org.neo4j.exceptions.Neo4jException

/*
A PipeDecorator is used to instrument calls between Pipes, and between a Pipe and the graph
 */
trait PipeDecorator {
  def decorate(planId: Id, state: QueryState): QueryState

  /**
   * This method should be called after createResults, with the decorated QueryState
   */
  def afterCreateResults(planId: Id, state: QueryState): Unit

  def decorate(planId: Id, iter: Iterator[CypherRow]): Iterator[CypherRow]

  // These two are used for linenumber only
  def decorate(planId: Id, iter: Iterator[CypherRow], sourceIter: Iterator[CypherRow]): Iterator[CypherRow] = decorate(planId, iter)

  def decorate(planId: Id, iter: Iterator[CypherRow], previousContextSupplier: () => Option[CypherRow]): Iterator[CypherRow] = decorate(planId, iter)

  /*
   * Returns the inner decorator of this decorator. The inner decorator is used for nested expressions
   * where the `decorate` should refer to the parent pipe instead of the calling pipe.
   */
  def innerDecorator(planId: Id): PipeDecorator
}

object NullPipeDecorator extends PipeDecorator {
  def decorate(planId: Id, iter: Iterator[CypherRow]): Iterator[CypherRow] = iter

  def decorate(planId: Id, state: QueryState): QueryState = state

  def innerDecorator(planId: Id): PipeDecorator = NullPipeDecorator

  override def afterCreateResults(planId: Id, state: QueryState): Unit = {}
}

class LinenumberPipeDecorator() extends PipeDecorator {
  private var inner: PipeDecorator = NullPipeDecorator

  def setInnerDecorator(newDecorator: PipeDecorator): Unit = inner = newDecorator

  override def decorate(planId: Id, state: QueryState): QueryState = inner.decorate(planId, state)

  def decorate(planId: Id, iter: Iterator[CypherRow]): Iterator[CypherRow] = throw new UnsupportedOperationException("This method should never be called on LinenumberPipeDecorator")

  override def decorate(planId: Id, iter: Iterator[CypherRow], sourceIter: Iterator[CypherRow]): Iterator[CypherRow] = {
    val previousContextSupplier = sourceIter match {
      case p: LinenumberIterator => () => p.previousRecord
      case _ => () => None
    }
    decorate(planId, iter, previousContextSupplier)
  }

  override def decorate(planId: Id, iter: Iterator[CypherRow], previousContextSupplier: () => Option[CypherRow]): Iterator[CypherRow] = {
    new LinenumberIterator(inner.decorate(planId, iter), previousContextSupplier)
  }

  override def innerDecorator(owningPipe: Id): PipeDecorator = this

  class LinenumberIterator(inner: Iterator[CypherRow], previousContextSupplier: () => Option[CypherRow]) extends Iterator[CypherRow] {

    var previousRecord: Option[CypherRow] = None

    def hasNext: Boolean = {
      try {
        inner.hasNext
      } catch {
        case e: LoadCsvStatusWrapCypherException =>
          throw e
        case e: Neo4jException =>
          throw wrapException(e, previousContextSupplier())
        case e: Throwable =>
          throw e
      }
    }

    def next(): CypherRow = {
      try {
        val record = inner.next()
        previousRecord = Some(record)
        record
      } catch {
        case e: LoadCsvStatusWrapCypherException =>
          throw e
        case e: Neo4jException =>
            throw wrapException(e, previousContextSupplier())
        case e: Throwable =>
          throw e
      }
    }

    private def wrapException(e: Neo4jException, maybeContext: Option[CypherRow]): Exception = maybeContext match {
      case Some(record: CypherRow) if record.getLinenumber.nonEmpty =>
        new LoadCsvStatusWrapCypherException(errorMessage(record), e)
      case _ => e
    }

    private def errorMessage(record: CypherRow): String = {
      record.getLinenumber match {
        case Some(ResourceLinenumber(file, line, last)) =>
          s"Failure when processing file '$file' on line $line" +
            (if (last) " (which is the last row in the file)." else ".")
        case _ => "" //should not get here
      }
    }
  }

  override def afterCreateResults(planId: Id, state: QueryState): Unit = {
    inner.afterCreateResults(planId, state)
  }
}
