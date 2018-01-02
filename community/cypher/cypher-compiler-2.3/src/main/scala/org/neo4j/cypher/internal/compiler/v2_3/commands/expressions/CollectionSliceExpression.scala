/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.commands.expressions

import org.neo4j.cypher.internal.compiler.v2_3._
import org.neo4j.cypher.internal.compiler.v2_3.helpers.{CastSupport, CollectionSupport}
import org.neo4j.cypher.internal.compiler.v2_3.pipes.QueryState
import org.neo4j.cypher.internal.compiler.v2_3.symbols.SymbolTable
import org.neo4j.cypher.internal.frontend.v2_3.symbols._

case class CollectionSliceExpression(collection: Expression, from: Option[Expression], to: Option[Expression])
  extends NullInNullOutExpression(collection) with CollectionSupport {
  def arguments: Seq[Expression] = from.toSeq ++ to.toSeq :+ collection

  private val function: (Iterable[Any], ExecutionContext, QueryState) => Any =
    (from, to) match {
      case (Some(f), Some(n)) => fullSlice(f, n)
      case (Some(f), None)    => fromSlice(f)
      case (None, Some(f))    => toSlice(f)
      case (None, None)       => (coll, _, _) => coll
    }

  private def fullSlice(from: Expression, to: Expression)(collectionValue: Iterable[Any], ctx: ExecutionContext, state: QueryState) = {
    val maybeFromValue = asInt(from, ctx, state)
    val maybeToValue = asInt(to, ctx, state)
    (maybeFromValue, maybeToValue) match {
      case (None, _) => null
      case (_, None) => null
      case (Some(fromValue), Some(toValue)) =>
        val size = collectionValue.size
        if (fromValue >= 0 && toValue >= 0)
          collectionValue.slice(fromValue, toValue)
        else if (fromValue >= 0) {
          val end = size + toValue
          collectionValue.slice(fromValue, end)
        } else if (toValue >= 0) {
          val start = size + fromValue
          collectionValue.slice(start, toValue)
        } else {
          val start = size + fromValue
          val end = size + toValue
          collectionValue.slice(start, end)
        }
    }
  }

  private def fromSlice(from: Expression)(collectionValue: Iterable[Any], ctx: ExecutionContext, state: QueryState) = {
    val fromValue = asInt(from, ctx, state)
    fromValue match {
      case None => null
      case Some(value) if value >= 0 => collectionValue.drop(value)
      case Some(value) =>
        val end = collectionValue.size + value
        collectionValue.drop(end)
    }
  }

  private def toSlice(from: Expression)(collectionValue: Iterable[Any], ctx: ExecutionContext, state: QueryState) = {
    val toValue = asInt(from, ctx, state)
    toValue match {
      case None => null
      case Some(value) if value >= 0 => collectionValue.take(value)
      case Some(value) =>
        val end = collectionValue.size + value
        collectionValue.take(end)
    }
  }


  def asInt(e: Expression, ctx: ExecutionContext, state: QueryState): Option[Int] = {
    val index = e(ctx)(state)
    if (index == null) None
    else Some(CastSupport.castOrFail[Number](index).intValue())
  }

  def compute(value: Any, ctx: ExecutionContext)(implicit state: QueryState): Any = {
    val collectionValue: Iterable[Any] = makeTraversable(value)
    function(collectionValue, ctx, state)
  }

  protected def calculateType(symbols: SymbolTable): CypherType = {
    from.foreach(_.evaluateType(CTNumber, symbols))
    to.foreach(_.evaluateType(CTNumber, symbols))
    collection.evaluateType(CTCollection(CTAny), symbols)
  }

  def rewrite(f: (Expression) => Expression): Expression =
    f(CollectionSliceExpression(collection.rewrite(f), from.map(_.rewrite(f)), to.map(_.rewrite(f))))

  def symbolTableDependencies: Set[String] = arguments.flatMap(_.symbolTableDependencies).toSet
}
