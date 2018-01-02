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
package org.neo4j.cypher.internal.runtime.interpreted.commands.expressions

import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.{CastSupport, ListSupport}
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.{NumberValue, Values}
import org.neo4j.values.virtual.{ListValue, VirtualValues}

case class ListSlice(collection: Expression, from: Option[Expression], to: Option[Expression])
  extends NullInNullOutExpression(collection) with ListSupport {
  def arguments: Seq[Expression] = from.toIndexedSeq ++ to.toIndexedSeq :+ collection

  private val function: (ListValue, ExecutionContext, QueryState) => AnyValue =
    (from, to) match {
      case (Some(f), Some(n)) => fullSlice(f, n)
      case (Some(f), None)    => fromSlice(f)
      case (None, Some(f))    => toSlice(f)
      case (None, None)       => (coll, _, _) => coll
    }

  private def fullSlice(from: Expression, to: Expression)(collectionValue: ListValue, ctx: ExecutionContext, state: QueryState) = {
    val maybeFromValue = asInt(from, ctx, state)
    val maybeToValue = asInt(to, ctx, state)
    (maybeFromValue, maybeToValue) match {
      case (None, _) => Values.NO_VALUE
      case (_, None) => Values.NO_VALUE
      case (Some(fromValue), Some(toValue)) =>
        val size = collectionValue.size
        if (fromValue >= 0 && toValue >= 0)
          VirtualValues.slice(collectionValue, fromValue, toValue)
        else if (fromValue >= 0) {
          val end = size + toValue
          VirtualValues.slice(collectionValue, fromValue, end)
        } else if (toValue >= 0) {
          val start = size + fromValue
          VirtualValues.slice(collectionValue, start, toValue)
        } else {
          val start = size + fromValue
          val end = size + toValue
          VirtualValues.slice(collectionValue, start, end)
        }
    }
  }

  private def fromSlice(from: Expression)(collectionValue: ListValue, ctx: ExecutionContext, state: QueryState) = {
    val fromValue = asInt(from, ctx, state)
    fromValue match {
      case None => Values.NO_VALUE
      case Some(value) if value >= 0 =>
        VirtualValues.drop(collectionValue, value)
      case Some(value) =>
        val end = collectionValue.size + value
        VirtualValues.drop(collectionValue, end)
    }
  }

  private def toSlice(from: Expression)(collectionValue: ListValue, ctx: ExecutionContext, state: QueryState) = {
    val toValue = asInt(from, ctx, state)
    toValue match {
      case None => Values.NO_VALUE
      case Some(value) if value >= 0 =>
       VirtualValues.take(collectionValue, value)
      case Some(value) =>
        val end = collectionValue.size + value
        VirtualValues.take(collectionValue, end)
    }
  }


  def asInt(e: Expression, ctx: ExecutionContext, state: QueryState): Option[Int] = {
    val index = e(ctx, state)
    if (index == Values.NO_VALUE) None
    else Some(CastSupport.castOrFail[NumberValue](index).longValue().toInt)
  }

  override def compute(value: AnyValue, ctx: ExecutionContext, state: QueryState): AnyValue = {
    val collectionValue = makeTraversable(value)
    function(collectionValue, ctx, state)
  }

  def rewrite(f: (Expression) => Expression): Expression =
    f(ListSlice(collection.rewrite(f), from.map(_.rewrite(f)), to.map(_.rewrite(f))))

  def symbolTableDependencies: Set[String] = arguments.flatMap(_.symbolTableDependencies).toSet
}
