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


import org.neo4j.cypher.internal.symbols.{NumberType, AnyCollectionType, CypherType, SymbolTable}
import org.neo4j.cypher.internal.ExecutionContext
import org.neo4j.cypher.internal.pipes.QueryState
import org.neo4j.cypher.internal.helpers.CollectionSupport
import org.neo4j.cypher.internal.helpers.CastSupport.castOrFail


case class SliceExpression(collection: Expression, from: Option[Expression], to: Option[Expression])
  extends Expression with CollectionSupport {
  def children: Seq[Expression] = from.toSeq ++ to.toSeq :+ collection

  private val function: (Iterable[Any], ExecutionContext, QueryState) => Any = {
    (from, to) match {
      case (Some(f), Some(n)) => fullSlice(f, n)
      case (Some(f), None)    => fromSlice(f)
      case (None, Some(f))    => toSlice(f)
    }
  }

  private def fullSlice(from: Expression, to: Expression)(collectionValue: Iterable[Any], ctx: ExecutionContext, state: QueryState) = {
    val fromValue = asInt(from, ctx, state)
    val toValue = asInt(to, ctx, state)

    if (fromValue >= 0 && toValue >= 0)
      collectionValue.slice(fromValue, toValue)
    else if (fromValue >= 0) {
      val end = collectionValue.size + toValue
      collectionValue.slice(fromValue, end)
    } else if (toValue >= 0) {
      val start = collectionValue.size + fromValue
      collectionValue.slice(start, toValue)
    } else {
      val size = collectionValue.size
      val start = size + fromValue
      val end = size + toValue
      collectionValue.slice(start, end)
    }
  }

  private def fromSlice(from: Expression)(collectionValue: Iterable[Any], ctx: ExecutionContext, state: QueryState) = {
    val fromValue = asInt(from, ctx, state)

    if (fromValue >= 0)
      collectionValue.drop(fromValue)
    else {
      val end = collectionValue.size + fromValue
      collectionValue.drop(end)
    }
  }

  private def toSlice(from: Expression)(collectionValue: Iterable[Any], ctx: ExecutionContext, state: QueryState) = {
    val toValue = asInt(from, ctx, state)

    if (toValue >= 0)
      collectionValue.take(toValue)
    else {
      val end = collectionValue.size + toValue
      collectionValue.take(end)
    }
  }


  def asInt(e: Expression, ctx: ExecutionContext, state: QueryState): Int =
    castOrFail[Number](e(ctx)(state)).intValue()

  def apply(ctx: ExecutionContext)(implicit state: QueryState): Any = {
    val collectionValue: Iterable[Any] = makeTraversable(collection(ctx))
    function(collectionValue, ctx, state)
  }

  protected def calculateType(symbols: SymbolTable): CypherType = {
    from.foreach(_.evaluateType(NumberType(), symbols))
    to.foreach(_.evaluateType(NumberType(), symbols))
    collection.evaluateType(AnyCollectionType(), symbols)
  }

  def rewrite(f: (Expression) => Expression): Expression =
    f(SliceExpression(collection.rewrite(f), from.map(_.rewrite(f)), to.map(_.rewrite(f))))

  def symbolTableDependencies: Set[String] = children.flatMap(_.symbolTableDependencies).toSet
}