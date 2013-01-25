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
import org.neo4j.cypher.internal.helpers.CollectionSupport
import org.neo4j.cypher.internal.ExecutionContext
import org.neo4j.cypher.CollectionIndexOutOfRange

case class NthFunction(n: Expression, collection: Expression)
  extends NullInNullOutExpression(collection) with CollectionSupport with NumericHelper {
  def compute(value: Any, m: ExecutionContext) = {
    val nval = asInt(n(m))
    val coll = makeTraversable(value)
    if(nval >= coll.size) throw new CollectionIndexOutOfRange("n out of range for nth")
    coll.drop(nval).head
  }
                    
  def rewrite(f: (Expression) => Expression) =
    f(NthFunction(n.rewrite(f), collection.rewrite(f)))

  def children = Seq(n, collection)

  def identifierDependencies(expectedType: CypherType) = AnyType

  def calculateType(symbols: SymbolTable) = collection.evaluateType(AnyCollectionType(), symbols).iteratedType

  def symbolTableDependencies = (collection.symbolTableDependencies ++ n.symbolTableDependencies)
}
