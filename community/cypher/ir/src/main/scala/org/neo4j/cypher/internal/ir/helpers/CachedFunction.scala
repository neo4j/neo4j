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
package org.neo4j.cypher.internal.ir.helpers

import scala.collection.mutable

trait CachedFunction {
  def cacheSize: Int
}

object CachedFunction {

  def apply[A, B](f: A => B): (A => B) with CachedFunction = new (A => B) with CachedFunction {
    private val cache = mutable.HashMap[A, B]()

    override def cacheSize: Int = cache.size

    def apply(input: A): B =
      cache.getOrElseUpdate(input, f(input))
  }

  def apply[A, B, C](f: (A, B) => C): ((A, B) => C ) with CachedFunction = {
    val tupledCachedFunction = apply(f.tupled)
    val untupledCachedFunction = Function.untupled(tupledCachedFunction)
    new ((A, B) => C) with CachedFunction {
      override def apply(v1: A, v2: B): C = untupledCachedFunction(v1, v2)
      override def cacheSize: Int = tupledCachedFunction.cacheSize
    }
  }

  def apply[A, B, C, D](f: (A, B, C) => D): ((A, B, C) => D ) with CachedFunction = {
    val tupledCachedFunction = apply(f.tupled)
    val untupledCachedFunction = Function.untupled(tupledCachedFunction)
    new ((A, B, C) => D) with CachedFunction {
      override def apply(a: A, b: B, c: C): D = untupledCachedFunction(a, b, c)
      override def cacheSize: Int = tupledCachedFunction.cacheSize
    }
  }

  def apply[A, B, C, D, E](f: (A, B, C, D) => E): ((A, B, C, D) => E ) with CachedFunction = {
    val tupledCachedFunction = apply(f.tupled)
    val untupledCachedFunction = Function.untupled(tupledCachedFunction)
    new ((A, B, C, D) => E) with CachedFunction {
      override def apply(v1: A, v2: B, v3: C, v4: D): E = untupledCachedFunction(v1, v2, v3, v4)
      override def cacheSize: Int = tupledCachedFunction.cacheSize
    }
  }

  def apply[A, B, C, D, E, F](f: (A, B, C, D, E) => F): ((A, B, C, D, E) => F ) with CachedFunction = {
    val tupledCachedFunction = apply(f.tupled)
    val untupledCachedFunction = Function.untupled(tupledCachedFunction)
    new ((A, B, C, D, E) => F) with CachedFunction {
      override def apply(v1: A, v2: B, v3: C, v4: D, v5: E): F = untupledCachedFunction(v1, v2, v3, v4, v5)
      override def cacheSize: Int = tupledCachedFunction.cacheSize
    }
  }

  def apply[A, B, C, D, E, F, G](f: (A, B, C, D, E, F) => G): ((A, B, C, D, E, F) => G ) with CachedFunction = {
    {
      val tupledCachedFunction = apply(f.tupled)
      val untupledCachedFunction = untupled(tupledCachedFunction)
      new ((A, B, C, D, E, F) => G) with CachedFunction {
        override def apply(v1: A, v2: B, v3: C, v4: D, v5: E, v6: F): G = untupledCachedFunction(v1, v2, v3, v4, v5, v6)
        override def cacheSize: Int = tupledCachedFunction.cacheSize
      }
    }
  }

  def apply[A, B, C, D, E, F, G, H](f: (A, B, C, D, E, F, G) => H): (A, B, C, D, E, F, G) => H = {
    untupled(apply(f.tupled))
  }

  /** Un-tupling for functions of arity 6. This transforms a function taking
   * a 6-tuple of arguments into a function of arity 6 which takes each argument separately.
   */
  def untupled[a1, a2, a3, a4, a5, a6, b](f: ((a1, a2, a3, a4, a5, a6)) => b): (a1, a2, a3, a4, a5, a6) => b = {
    (x1, x2, x3, x4, x5, x6) => f(Tuple6(x1, x2, x3, x4, x5, x6))
  }

  /** Un-tupling for functions of arity 7. This transforms a function taking
   * a 7-tuple of arguments into a function of arity 7 which takes each argument separately.
   */
  def untupled[a1, a2, a3, a4, a5, a6, a7, b](f: ((a1, a2, a3, a4, a5, a6, a7)) => b)
    : (a1, a2, a3, a4, a5, a6, a7) => b = {
    (x1, x2, x3, x4, x5, x6, x7) => f(Tuple7(x1, x2, x3, x4, x5, x6, x7))
  }

  /** Allows passing [[value]] into [[CachedFunction]] while only using [[cacheKey]] for cache lookup.
   *
   * Because [[value]] is passed via the second argument list, it is excluded from generated equals() and hashCode() methods.
   */
  final case class CacheKey[Key, Value](cacheKey: Key)(val value: Value)

  object CacheKey {

    def computeFrom[Key, Value](value: Value)(f: Value => Key): CacheKey[Key, Value] = {
      CacheKey(f(value))(value)
    }
  }
}
