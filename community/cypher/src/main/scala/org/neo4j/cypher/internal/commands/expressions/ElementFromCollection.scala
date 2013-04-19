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
package org.neo4j.cypher.internal.commands.expressions

import org.neo4j.cypher.internal.symbols._
import org.neo4j.cypher.internal.ExecutionContext
import org.neo4j.cypher.internal.symbols.SymbolTable
import org.neo4j.cypher.internal.pipes.QueryState
import org.neo4j.cypher.internal.helpers.CastSupport.castOrFail
import org.neo4j.cypher.internal.helpers.CollectionSupport
import org.neo4j.cypher.OutOfBoundsException


case class ElementFromCollection(collection: Expression, index: Expression) extends Expression with CollectionSupport {
  def children = Seq(collection, index)

  def apply(ctx: ExecutionContext)(implicit state: QueryState): Any = {
    var idx = castOrFail[Number](index(ctx)).intValue()
    val iter = makeTraversable(collection(ctx)).toIterator

    if (idx < 0)
      throw new OutOfBoundsException("Can't have negative values for collection indexes")

    try {
      while (idx > 0) {
        iter.next()
        idx -= 1
      }

      iter.next()
    } catch {
      case _: NoSuchElementException =>
        throw new OutOfBoundsException(s"Passed the end of the collection ${collection.toString()}")
    }
  }

  protected def calculateType(symbols: SymbolTable): CypherType = {
    val myType = collection.evaluateType(AnyCollectionType(), symbols).asInstanceOf[CollectionType].iteratedType
    index.evaluateType(NumberType(), symbols)

    myType
  }

  def rewrite(f: (Expression) => Expression): Expression = f(ElementFromCollection(collection.rewrite(f), index.rewrite(f)))

  def symbolTableDependencies: Set[String] = collection.symbolTableDependencies ++ index.symbolTableDependencies
}