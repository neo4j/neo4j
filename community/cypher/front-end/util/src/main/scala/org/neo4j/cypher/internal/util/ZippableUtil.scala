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
package org.neo4j.cypher.internal.util

import scala.collection.BuildFrom

object ZippableUtil {

  implicit class Zippable[F[x] <: Iterable[x], A](left: F[A]) {

    /**
     * zip the two given sequences but fill right if it is too small.
     */
    def zipLeft[B](right: Iterable[B], fill: B)(implicit bf: BuildFrom[F[A], (A, B), F[(A, B)]]): F[(A, B)] = {
      val right2 = right ++ Seq.fill(left.size - right.size)(fill)
      val zipped = left.zip(right2)

      bf.fromSpecific(left)(zipped)
    }

    /**
     * zip the two given sequences but fill left if it is too small.
     */
    def zipRight[B](right: Iterable[B], fill: A)(implicit bf: BuildFrom[F[A], (A, B), F[(A, B)]]): F[(A, B)] = {
      val left2 = left ++ Seq.fill(right.size - left.size)(fill)
      val zipped = left2.zip(right)

      bf.fromSpecific(left)(zipped)
    }
  }
}
