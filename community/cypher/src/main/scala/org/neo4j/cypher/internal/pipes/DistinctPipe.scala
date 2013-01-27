/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher.internal.pipes

import org.neo4j.cypher.internal.symbols.{AnyType, SymbolTable}
import org.neo4j.cypher.internal.ExecutionContext
import collection.mutable
import org.neo4j.cypher.internal.commands.expressions.Expression

class DistinctPipe(source: Pipe, expressions: Map[String, Expression]) extends PipeWithSource(source) {

  val keyNames: Seq[String] = expressions.keys.toSeq

  def createResults(state: QueryState): Iterator[ExecutionContext] = {
    val inner = source.createResults(state).map(ctx => {
      val newMap = expressions.mapValues(expression => expression(ctx))
      ctx.newFrom(newMap)
    })

    new DistinctIterator(inner)
  }

  def executionPlan() = source.executionPlan() + "\nDistinct()"

  def symbols: SymbolTable = {
    val identifiers = expressions.mapValues(e => e.evaluateType(AnyType(), source.symbols))
    new SymbolTable(identifiers)
  }


  def throwIfSymbolsMissing(symbols: SymbolTable) {
    expressions.values.foreach(e => e.throwIfSymbolsMissing(symbols))
  }

  private class DistinctIterator(inner: Iterator[ExecutionContext]) extends PrefetchingIterator[ExecutionContext] {

    var seen = mutable.Set[NiceHasher]()

    def getNext = if ( !inner.hasNext ) {
      None
    } else {
      var value: ExecutionContext = null
      var hash: NiceHasher = null

      def fetchNext() {
        value = inner.next()
        hash = new NiceHasher(keyNames.map(value).toSeq)
      }

      fetchNext()
      while ( seen.contains(hash) && inner.hasNext ) {
        fetchNext()
      }

      if ( seen.contains(hash) ) {
        None
      }
      else {
        seen += hash
        Some(value)
      }
    }
  }

  private abstract class PrefetchingIterator[T] extends Iterator[T] {

    def getNext: Option[T]

    private var nextObject: Option[T] = null

    private def spoolToNext() {
      if ( nextObject == null ) {
        nextObject = getNext()
      }
    }


    def hasNext = {
      spoolToNext()
      nextObject.nonEmpty
    }

    def next() = {
      spoolToNext()

      val result = nextObject.getOrElse(Iterator.empty.next())
      nextObject = null
      result
    }
  }

}