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
package scala.collection // a workaround to access package-private members

import scala.collection.BitSetOps.LogWL
import scala.collection.BitSetOps.WordLength
import scala.collection.immutable.BitSet

/** Contains methods that re-implement standard library methods to improve performance. */
object CypherPlannerBitSetOptimizations {

  def explodedBitSet(ids: BitSet, compactionMap: mutable.HashMap[Int, BitSet]): BitSet = {
    val builder = mutable.BitSet.empty

    /** Adapted from [[scala.collection.BitSetOps.foreach]]. */
    var i = 0
    val len = ids.nwords
    while (i < len) {
      var w = ids.word(i)
      var j = i * WordLength
      while (w != 0L) {
        if ((w & 1L) == 1L) {
          val compacted = compactionMap.getOrElse(j, null)
          if (compacted ne null) {
            builder |= compacted
          } else {
            builder.addOne(j)
          }
        }
        w = w >>> 1
        j += 1
      }
      i += 1
    }

    BitSet.fromBitMaskNoCopy(builder.elems)
  }

  /** Adapted from [[scala.collection.BitSetOps.rangeImpl]].
   * This version does not wrap arguments in `Option` to avoid boxing.
   */
  def range(coll: BitSet, from: Int, until: Int): BitSet = {
    val a = coll.toBitMask
    val len = a.length
    locally {
      val f = from
      val w = f >> LogWL
      val b = f & (WordLength - 1)
      if (w >= 0) {
        java.util.Arrays.fill(a, 0, math.min(w, len), 0)
        if (b > 0 && w < len) a(w) &= ~((1L << b) - 1)
      }
    }
    locally {
      val u = until
      val w = u >> LogWL
      val b = u & (WordLength - 1)
      if (w < len) {
        java.util.Arrays.fill(a, math.max(w + 1, 0), len, 0)
        if (w >= 0) a(w) &= (1L << b) - 1
      }
    }
    coll.fromBitMaskNoCopy(a)
  }

  /** Adapted from Scala 2.12 `scala.collection.BitSetLike.subsetOf`. */
  def subsetOf(subset: BitSet, superset: BitSet): Boolean = {
    var idx = 0
    val len = subset.nwords
    while (idx < len) {
      if ((subset.word(idx) & ~superset.word(idx)) != 0L)
        return false
      idx += 1
    }
    true
  }
}
