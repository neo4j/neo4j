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

/**
 * This can be used to determine in which order to align plans with Cartesian Products.
 * The math is coincidentally the same as in [[PredicateOrdering]].
 */
object CartesianOrdering extends Ordering[(Cost, Cardinality)] {
  override def compare(side0: (Cost, Cardinality), side1: (Cost, Cardinality)): Int = {
    costFor(side0, side1).compare(costFor(side1, side0))
  }

  /**
   * The cost for CartesianProduct(side0, side1)
   */
  private def costFor(side0: (Cost, Cardinality), side1: (Cost, Cardinality)): Cost = (side0, side1) match {
    case ((cost0, Cardinality(card0)), (cost1, _)) =>
      // side0 needs to be executed once. side1 is executed card0 times.
      cost0 + cost1 * card0
  }
}
