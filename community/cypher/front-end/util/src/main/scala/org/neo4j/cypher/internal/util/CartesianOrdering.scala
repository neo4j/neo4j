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

/**
 * This can be used to determine in which order to align plans with Cartesian Products.
 * The math is coincidentally the same as in [[PredicateOrdering]].
 */
object VolcanoCartesianOrdering extends CartesianOrdering {

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

/**
 * Components that should be combined with Cartesian Products should be ordered such that the overall cost is minimized.
 * A component here is represented by its cost and its cardinality.
 *
 * Given c0 as the cost for component0 and s0 as the cardinality of component0
 * (and analogous for other components), component0 should be ordered before
 * component1 iff c0 + c1 * ⌈s0 / B⌉ > c1 + c0 * ⌈s1 / B⌉.
 *
 * B is a constant that stands for batch size.
 * An expression ⌈x⌉ means that the expression x is evaluated with ceiling rounding.
 *
 * This is a well defined ordering. Given component0, component1 and component2,
 * where
 *
 * I:  c0 + c1 * ⌈s0 / B⌉ > c1 + c0 * ⌈s1 / B⌉ (component0 comes before component1)
 * II: c1 + c2 * ⌈s1 / B⌉ > c2 + c1 * ⌈s2 / B⌉ (component1 comes before component2)
 * and all c_i and s_i and B are positive.
 *
 * we can show that component0 comes before component2:
 *
 * I:    c0 + c1 * ⌈s0 / B⌉             > c1 + c0 * ⌈s1 / B⌉                  | -c1
 *       c0 + c1 * ⌈s0 / B⌉ - c1        > c0 * ⌈s1 / B⌉                       | /c0
 *      (c0 + c1 * ⌈s0 / B⌉ - c1) / c0  >      ⌈s1 / B⌉
 *
 * II: c1 + c2 * ⌈s1 / B⌉ > c2 + c1 * ⌈s2 / B⌉                                | -c1
 *          c2 * ⌈s1 / B⌉ >  c2 + c1 * ⌈s2 / B⌉ - c1                          | /c2
 *               ⌈s1 / B⌉      > (c2 + c1 * ⌈s2 / B⌉ - c1) / c2
 *
 * Substitute ⌈s1 / B⌉ in I:
 * (c0 + c1 * ⌈s0 / B⌉ - c1) / c0 > (c2 + c1 * ⌈s2 / B⌉ - c1) / c2
 *  1 + (c1 * ⌈s0 / B⌉ - c1) / c0 >  1 + (c1 * ⌈s2 / B⌉ - c1) / c2          | -1
 *      (c1 * ⌈s0 / B⌉ - c1) / c0 >      (c1 * ⌈s2 / B⌉ - c1) / c2          | /c1
 *      (⌈s0 / B⌉ - 1)       / c0 >      (⌈s2 / B⌉ - 1)       / c2          | *c0
 *      (⌈s0 / B⌉ - 1)            >      (⌈s2 / B⌉ - 1) * c0  / c2          | *c2
 *      (⌈s0 / B⌉ - 1) * c2       >      (⌈s2 / B⌉ - 1) * c0
 *      ⌈s0 / B⌉ * c2 - c2        >      ⌈s2 / B⌉ * c0 - c0                 | +c0
 * c0 + ⌈s0 / B⌉ * c2 - c2        >      ⌈s2 / B⌉ * c0                      | +c2
 * c0 + ⌈s0 / B⌉ * c2             > c2 + ⌈s2 / B⌉ * c0
 */
class BatchedCartesianOrdering(batchSize: Int) extends CartesianOrdering {

  override def compare(side0: (Cost, Cardinality), side1: (Cost, Cardinality)): Int = {
    costFor(side0, side1).compare(costFor(side1, side0))
  }

  /**
   * The cost for CartesianProduct(side0, side1)
   */
  private def costFor(side0: (Cost, Cardinality), side1: (Cost, Cardinality)): Cost = (side0, side1) match {
    case ((cost0, Cardinality(card0)), (cost1, _)) =>
      cost0 + cost1 * Math.ceil(card0 / batchSize)
  }
}
