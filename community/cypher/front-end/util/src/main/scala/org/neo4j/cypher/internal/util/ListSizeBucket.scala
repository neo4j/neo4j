/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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

object ListSizeBucket {

  /**
   * Compute the next closest power of 10. This number is used when planning queries where a list has been auto-parameterized, by keeping this "bucket" as a
   * size hint in the auto-parameter. By keeping a rough estimate of the list size, we can make a more informed decision at plan time when solving predicates
   * like `IN [...]`.
   * @param size The size of the list that is being auto-parametrized.
   * @return Next closest power of 10 for size.
   *         Examples: computeBucket(1) = 1, computeBucket(7) = 10, computeBucket(17) = 100, computeBucket(42) = 100
   */
  def computeBucket(listSize: Int): Int = {
    Math.pow(10, Math.ceil(Math.log10(listSize))).toInt
  }
}
