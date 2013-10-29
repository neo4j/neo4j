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
package org.neo4j.cypher.internal.compiler.v2_0.commands.expressions

import org.neo4j.cypher.internal.compiler.v2_0._
import pipes.QueryState
import symbols._
import org.neo4j.cypher.internal.helpers._
import org.neo4j.cypher.OutOfBoundsException

case class CollectionIndex(collection: Expression, index: Expression) extends NullInNullOutExpression(collection)
with CollectionSupport {
  def arguments = Seq(collection, index)

  def compute(value:Any, ctx: ExecutionContext)(implicit state: QueryState): Any = {
    var idx = CastSupport.castOrFail[Number](index(ctx)).intValue()
    val collectionValue = makeTraversable(value).toList

    if (idx < 0) {
      idx = collectionValue.size + idx
    }

    if(idx>=collectionValue.size)
      throw new OutOfBoundsException(s"Passed the end of the collection ${collection.toString()}")

    collectionValue.apply(idx)
  }

  protected def calculateType(symbols: SymbolTable): CypherType = {
    val myType = collection.evaluateType(CollectionType(AnyType()), symbols).asInstanceOf[CollectionType].iteratedType
    index.evaluateType(NumberType(), symbols)

    myType
  }

  def rewrite(f: (Expression) => Expression): Expression = f(CollectionIndex(collection.rewrite(f), index.rewrite(f)))

  def symbolTableDependencies: Set[String] = collection.symbolTableDependencies ++ index.symbolTableDependencies
}
