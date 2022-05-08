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
package scala.collection // a workaround to access package-private members

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
}
