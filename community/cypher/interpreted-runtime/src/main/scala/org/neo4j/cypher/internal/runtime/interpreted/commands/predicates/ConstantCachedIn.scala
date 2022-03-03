/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.runtime.interpreted.commands.predicates

import org.neo4j.cypher.internal.runtime.IsNoValue
import org.neo4j.cypher.internal.runtime.ListSupport
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.AstNode
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.operations.CypherFunctions
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.BooleanValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.ListValue

import scala.collection.mutable

/*
This class is used for making the common <exp> IN <constant-expression> fast
 */
case class ConstantCachedIn(value: Expression, list: Expression, id: Id) extends Predicate with ListSupport {

  // These two are here to make the fields accessible without conflicting with the case classes
  override def isMatch(ctx: ReadableRow, state: QueryState): Option[Boolean] = {
    val listValue = list(ctx, state)
    if (listValue eq Values.NO_VALUE) {
      None
    } else {
      val input = makeTraversable(listValue)
      if (input.isEmpty) {
        Some(false)
      } else {
        state.cachedIn.check(
          value(ctx, state),
          input,
          state.memoryTrackerForOperatorProvider.memoryTrackerForOperator(id.x)
        ) match {
          case IsNoValue()     => None
          case b: BooleanValue => Some(b.booleanValue())
          case v               => throw new IllegalStateException(s"$v is not a supported value for a predicate")
        }
      }
    }
  }

  override def containsIsNull = false

  override def children: Seq[AstNode[_]] = Seq(value, list)

  override def arguments: Seq[Expression] = Seq(list)

  override def rewrite(f: Expression => Expression): Expression =
    f(ConstantCachedIn(value.rewrite(f), list.rewrite(f), id))
}

object EvaluateIn {
  // Boolean boxing was showing up in micro benchmarks
  private[this] val someFalse = Some(false)
  private[this] val someTrue = Some(true)

  def apply(value: AnyValue, list: ListValue): Option[Boolean] = {
    CypherFunctions.in(value, list) match {
      case BooleanValue.FALSE => someFalse
      case BooleanValue.TRUE  => someTrue
      case Values.NO_VALUE    => None
    }
  }
}

class ReferenceCacheKey(private val reference: AnyRef) {

  override def equals(other: Any): Boolean = other match {
    case otherCacheKey: ReferenceCacheKey => reference eq otherCacheKey.reference
    case _                                => false
  }

  override def hashCode(): Int = System.identityHashCode(reference)
}

class ConcurrentLRUCache[K, V](maxSizePerThread: Int) extends InLRUCache[K, V] {

  private val threadLocalCache =
    ThreadLocal.withInitial[mutable.ArrayDeque[(K, V)]](() => new mutable.ArrayDeque[(K, V)](maxSizePerThread))
  override val maxSize: Int = maxSizePerThread

  override def cache: mutable.ArrayDeque[(K, V)] = threadLocalCache.get()
}

class SingleThreadedLRUCache[K, V](override val maxSize: Int) extends InLRUCache[K, V] {
  override val cache: mutable.ArrayDeque[(K, V)] = new mutable.ArrayDeque[(K, V)](maxSize)
}

abstract class InLRUCache[K, V] {
  def maxSize: Int
  def cache: mutable.ArrayDeque[(K, V)]

  def getOrElseUpdate(key: K, f: => V): V = {
    val idx = findIndex(key)
    val entry =
      if (idx == -1) {
        if (cache.size == maxSize) cache.remove(maxSize - 1)
        (key, f)
      } else {
        cache.remove(idx)
      }
    cache.prepend(entry)
    entry._2
  }

  // Boxing of booleans showed up in micro benchmarks when using the scala method
  private def findIndex(key: K): Int = {
    var i = 0
    while (i < cache.size) {
      if (cache(i)._1 == key) {
        return i
      }
      i += 1
    }
    -1
  }
}
