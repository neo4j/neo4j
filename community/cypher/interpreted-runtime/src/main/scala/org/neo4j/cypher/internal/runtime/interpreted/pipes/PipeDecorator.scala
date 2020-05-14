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

import org.neo4j.cypher.internal.runtime.ExecutionContext
import org.neo4j.cypher.internal.runtime.ResourceLinenumber
import org.neo4j.exceptions.LoadCsvStatusWrapCypherException
import org.neo4j.exceptions.Neo4jException

/*
A PipeDecorator is used to instrument calls between Pipes, and between a Pipe and the graph
 */
trait PipeDecorator {
  def decorate(pipe: Pipe, state: QueryState): QueryState

  /**
   * This method should be called after createResults, with the decorated QueryState
   */
  def afterCreateResults(pipe: Pipe, state: QueryState): Unit

  def decorate(pipe: Pipe, iter: Iterator[ExecutionContext]): Iterator[ExecutionContext]

  // These two are used for linenumber only
  def decorate(pipe: Pipe, iter: Iterator[ExecutionContext], sourceIter: Iterator[ExecutionContext]): Iterator[ExecutionContext] = decorate(pipe, iter)

  def decorate(pipe: Pipe, iter: Iterator[ExecutionContext], previousContextSupplier: () => Option[ExecutionContext]): Iterator[ExecutionContext] = decorate(pipe, iter)

  /*
   * Returns the inner decorator of this decorator. The inner decorator is used for nested expressions
   * where the `decorate` should refer to the parent pipe instead of the calling pipe.
   */
  def innerDecorator(pipe: Pipe): PipeDecorator
}

object NullPipeDecorator extends PipeDecorator {
  def decorate(pipe: Pipe, iter: Iterator[ExecutionContext]): Iterator[ExecutionContext] = iter

  def decorate(pipe: Pipe, state: QueryState): QueryState = state

  def innerDecorator(pipe: Pipe): PipeDecorator = NullPipeDecorator

  override def afterCreateResults(pipe: Pipe, state: QueryState): Unit = {}
}

class LinenumberPipeDecorator() extends PipeDecorator {
  private var inner: PipeDecorator = NullPipeDecorator

  def setInnerDecorator(newDecorator: PipeDecorator): Unit = inner = newDecorator

  override def decorate(pipe: Pipe, state: QueryState): QueryState = inner.decorate(pipe, state)

  def decorate(pipe: Pipe, iter: Iterator[ExecutionContext]): Iterator[ExecutionContext] = throw new UnsupportedOperationException("This method should never be called on LinenumberPipeDecorator")

  override def decorate(pipe: Pipe, iter: Iterator[ExecutionContext], sourceIter: Iterator[ExecutionContext]): Iterator[ExecutionContext] = {
    val previousContextSupplier = sourceIter match {
      case p: LinenumberIterator => () => p.previousRecord
      case _ => () => None
    }
    decorate(pipe, iter, previousContextSupplier)
  }

  override def decorate(pipe: Pipe, iter: Iterator[ExecutionContext], previousContextSupplier: () => Option[ExecutionContext]): Iterator[ExecutionContext] = {
    new LinenumberIterator(inner.decorate(pipe, iter), previousContextSupplier)
  }

  override def innerDecorator(owningPipe: Pipe): PipeDecorator = this

  class LinenumberIterator(inner: Iterator[ExecutionContext], previousContextSupplier: () => Option[ExecutionContext]) extends Iterator[ExecutionContext] {

    var previousRecord: Option[ExecutionContext] = None

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

    def next(): ExecutionContext = {
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

    private def wrapException(e: Neo4jException, maybeContext: Option[ExecutionContext]): Exception = maybeContext match {
      case Some(record: ExecutionContext) if record.getLinenumber.nonEmpty =>
        new LoadCsvStatusWrapCypherException(errorMessage(record), e)
      case _ => e
    }

    private def errorMessage(record: ExecutionContext): String = {
      record.getLinenumber match {
        case Some(ResourceLinenumber(file, line, last)) =>
          s"Failure when processing file '$file' on line $line" +
            (if (last) " (which is the last row in the file)." else ".")
        case _ => "" //should not get here
      }
    }
  }

  override def afterCreateResults(pipe: Pipe, state: QueryState): Unit = {
    inner.afterCreateResults(pipe, state)
  }
}
