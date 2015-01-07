/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v1_9.commands.expressions

import org.neo4j.cypher.internal.compiler.v1_9.symbols._
import org.neo4j.cypher.CypherTypeException
import org.neo4j.cypher.internal.compiler.v1_9.ExecutionContext
import org.neo4j.cypher.internal.compiler.v1_9.pipes.QueryState
import org.neo4j.cypher.internal.helpers.{IsCollection, TypeSafeMathSupport}

case class Add(a: Expression, b: Expression) extends Expression with TypeSafeMathSupport {
  def apply(ctx: ExecutionContext)(implicit state: QueryState) = {
    val aVal = a(ctx)
    val bVal = b(ctx)

    (aVal, bVal) match {
      case (null, _)                          => null
      case (_, null)                          => null
      case (x: Number, y: Number)             => plus(x,y)
      case (x: String, y: String)             => x + y
      case (IsCollection(x), IsCollection(y)) => x ++ y
      case (IsCollection(x), y)               => x ++ Seq(y)
      case (x, IsCollection(y))               => Seq(x) ++ y
      case (x: String, y: Number)             => x + y.toString
      case (x: Number, y: String)             => x.toString + y
      case _                                  => throw new CypherTypeException("Don't know how to add `" + aVal.toString + "` and `" + bVal.toString + "`")
    }
  }

  def rewrite(f: (Expression) => Expression) = f(Add(a.rewrite(f), b.rewrite(f)))


  def children = Seq(a, b)

  def calculateType(symbols: SymbolTable): CypherType = {
    val aT = a.getType(symbols)
    val bT = b.getType(symbols)

    (aT.isCollection, bT.isCollection) match {
      case (true, false) => mergeWithCollection(collection = aT, singleElement = bT)
      case (false, true) => mergeWithCollection(collection = bT, singleElement = aT)
      case _ => (aT, bT) match {
        case (x:StringType, y:NumberType) => StringType()
        case (x:NumberType, y:StringType) => StringType()
        case _ => aT.mergeWith(bT)
      }
    }
  }

  private def mergeWithCollection(collection: CypherType, singleElement: CypherType):CypherType= {
    val collectionType = collection.asInstanceOf[CollectionType]
    val mergedInnerType = collectionType.iteratedType.mergeWith(singleElement)
    new CollectionType(mergedInnerType)
  }

  def symbolTableDependencies = a.symbolTableDependencies ++ b.symbolTableDependencies
}
