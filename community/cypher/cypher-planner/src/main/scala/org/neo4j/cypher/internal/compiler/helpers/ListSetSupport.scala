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

import org.neo4j.cypher.internal.util.collection.immutable.ListSet

object ListSetSupport {

  implicit class RichListSet[A](listSet: ListSet[A]) {

    /** Partitions this `ListSet`, grouping values into `ListSet`s according to some discriminator function,
     *  preserving the order in which the values are first encountered.
     *
     *  For example:
     *  {{{
     *   scala> val groups = ListSet("foo", "", "bar").sequentiallyGroupBy(_.length)
     *   groups: ListSet[(Int, ListSet[String])] = ListSet((3,ListSet(foo, bar)), (0,ListSet("")))
     *  }}}
     *
     *  Compare with the default `groupBy` implementation, note how the resulting `ListSet` starts with the second value, there is no guaranteed order:
     *  {{{
     *   scala> val groups = ListSet("foo", "", "bar").groupBy(_.length).to(ListSet)
     *   groups: ListSet[(Int, ListSet[String])] = ListSet((0,ListSet("")), (3,ListSet(foo, bar)))
     *  }}}
     *
     *  @param f     the discriminator function.
     *  @tparam K    the type of keys returned by the discriminator function.
     *  @return      A `ListSet` of tuples each containing a key `k = f(x)` and all the elements `x` of the sequence where `f(x)` is equal to `k`.
     */
    def sequentiallyGroupBy[K](f: A => K): ListSet[(K, ListSet[A])] = IterableHelper.sequentiallyGroupBy(listSet)(f)
  }
}
