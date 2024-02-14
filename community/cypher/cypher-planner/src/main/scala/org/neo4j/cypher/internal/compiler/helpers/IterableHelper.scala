/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.helpers

import scala.collection.Factory
import scala.collection.immutable
import scala.collection.mutable

object IterableHelper {

  /** Groups values according to some discriminator function, preserving the order in which the values are first encountered.
   *
   *  This is highly generic, and so type inference will probably get in your way, you probably should use a specialised version of it such as the one defined
   *  in `RichSeq` inside [[SeqSupport]].
   *
   *  Also note that you probably only want to use this function if your values are in a specific order, like in a `Vector` or in a `ListSet`. It doesn't make
   *  much sense if you have an unordered collection such as a `Set`. You probably want to reach for the standard `groupBy` in that case.
   *
   *  @param f     the discriminator function.
   *  @tparam A    the type of elements in the input, and in the resulting groups.
   *  @tparam K    the type of keys returned by the discriminator function.
   *  @tparam C    the type of the outer collection being returned.
   *  @tparam D    the type used to group elements with the same key.
   *  @return      a sequence of tuples each containing a key `k = f(x)` and all the elements `x` of the sequence where `f(x)` is equal to `k`.
   */
  def sequentiallyGroupBy[A, K, C[_], D[_]](values: immutable.Iterable[A])(f: A => K)(
    implicit groupFactory: Factory[A, D[A]],
    allGroupsFactory: Factory[(K, D[A]), C[(K, D[A])]]
  ): C[(K, D[A])] = {
    val hashMap = mutable.LinkedHashMap.empty[K, mutable.Builder[A, D[A]]]
    values.foreach { value =>
      val key = f(value)
      val groupBuilder = hashMap.getOrElseUpdate(key, groupFactory.newBuilder)
      groupBuilder += value
    }
    val allGroups = allGroupsFactory.newBuilder
    hashMap.foreach { case (key, groupBuilder) =>
      allGroups.addOne((key, groupBuilder.result()))
    }
    allGroups.result()
  }

  implicit class RichIterableOnce[T, C[_] <: IterableOnce[_]](inner: C[T]) {

    /**
     * Traverses the IterableOnce and collects the results of applying `f` to every element,
     * returning a collection identical to `this.flatMap(f)`
     * but only if `f` returns `Some` for every element. Returns `None` collection otherwise.
     *
     * @param f the function
     * @tparam CC the type of the collection ebeing returned.
     * @tparam B the type of elements returned by `f`
     */
    def traverseInto[CC[_], B](f: T => Option[B])(implicit factory: Factory[B, CC[B]]): Option[CC[B]] = {
      val builder = factory.newBuilder
      val iterator = inner.iterator.asInstanceOf[Iterator[T]]
      var isDefined = true
      while (isDefined && iterator.hasNext) {
        f(iterator.next()) match {
          case Some(value) => builder.addOne(value)
          case None        => isDefined = false
        }
      }
      Option.when(isDefined)(builder.result())
    }

    /**
     * Traverses the IterableOnce and collects the results of applying `f` to every element,
     * returning a collection identical to `this.flatMap(f)`
     * but only if `f` returns `Some` for every element. Returns `None` otherwise.
     *
     * @param f the function
     * @tparam B the type of elements returned by `f`
     */
    def traverse[B](f: T => Option[B])(implicit factory: Factory[B, C[B]]): Option[C[B]] = traverseInto[C, B](f)
  }
}
