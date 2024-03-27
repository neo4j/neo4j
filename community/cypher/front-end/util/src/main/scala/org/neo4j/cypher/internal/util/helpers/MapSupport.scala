/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.util.helpers

import scala.collection.immutable

object MapSupport {

  implicit class PowerMap[A, B](m: immutable.Map[A, B]) {

    def fuse(other: immutable.Map[A, B])(f: (B, B) => B): immutable.Map[A, B] = {
      other.foldLeft(m) {
        case (acc, (k, v)) if acc.contains(k) => acc + (k -> f(acc(k), v))
        case (acc, entry)                     => acc + entry
      }
    }

    /**
     * For each (RHSKey, RHSValue)-pair on the RHS (RHS is called `other`),
     *   if the LFS (called `m`) also contains the key RHSKey, then apply function `f` (f(LHSValue, RHSValue))
     *   the value of the LHS is given as the first parameter to `f`
     *   the value of the RHS is given as the second parameter to `f`
     * Note that this function is asymmetric since `a.fuseLeft(b)` is not equivalent to `b.fuseLeft(a)`
     *
     * {{{
     * Map(1 -> Set("a", "b"), 2 -> Set("c")).fuseLeft(Map(1 -> Set("b", "c"), 3 -> Set("d")))(_ -- _) gives Map(1 -> Set("a"), 2 -> Set("c"))
     * }}}
     * @param other RHS to be applied
     * @param f     function over the map values (f(LHSValue, RHSValue)) to be applied when the LHS contains the key
     * @return      the result of applying f for all RHSValues where the LHS has the RHSKey
     */
    def fuseLeft(other: immutable.Map[A, B])(f: (B, B) => B): immutable.Map[A, B] = {
      other.foldLeft(m) {
        case (acc, (k, v)) if acc.contains(k) => acc + (k -> f(acc(k), v))
        case (acc, _)                         => acc
      }
    }
  }

}
