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

object SizeBucket {

  /**
   * Compute the next closest power of 10. This number is used when planning queries where the literal being auto-parameterized has a size, 
   * e.g. a list or a string, by keeping this "bucket" as a size hint in the auto-parameter. By keeping a rough estimate of the size, we can make a more 
   * informed decision at plan time when solving predicates like `IN [...]` or `a.prop STARTS WITH 'foo'`.
   * 
   * Note estimation is exact for size 0 and 1 which is an important property of the computation since we leverage that fact
   * in other parts of the code base.
   *
   * @param size The size of the literal that is being auto-parametrized.
   * @return Next closest power of 10 for size.
   *         Examples: computeBucket(1) = 1, computeBucket(7) = 10, computeBucket(17) = 100, computeBucket(42) = 100
   */
  def computeBucket(size: Int): Int = {
    Math.pow(10, Math.ceil(Math.log10(size))).toInt
  }
}
