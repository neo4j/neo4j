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
 * Predicates should be ordered such that the overall cost per row is minimized.
 * A predicate here is represented by the cost per row to evaluate the predicate
 * and by its selectivity.
 *
 * Given c0 as the cost for predicate0 and s0 as the selectivity of predicate0
 * (and analogous for other predicates), predicate0 should be evaluated before
 * predicate1 iff c0 + s0 * c1 > c1 + s1 * c0.
 *
 * This is a well defined ordering. Given predicate0, predicate1 and predicate2,
 * where
 *
 * I:  c0 + s0 * c1 > c1 + s1 * c0 (predicate0 comes before predicate1)
 * II: c1 + s1 * c2 > c2 + s2 * c1 (predicate1 comes before predicate2)
 * and all c_i and s_i are positive.
 *
 * we can show that predicate0 comes before predicate2:
 *
 * I:    c0 + s0 * c1            > c1 + s1 * c0                   | -c1
 *       c0 + s0 * c1 - c1       >      s1 * c0                   | /c0
 *      (c0 + s0 * c1 - c1) / c0 >      s1
 *
 * II: c1 + s1 * c2 >  c2 + s2 * c1                               | -c1
 *          s1 * c2 >  c2 + s2 * c1 - c1                          | /c2
 *          s1      > (c2 + s2 * c1 - c1) / c2
 *
 * Substitute s1 in I:
 *   (c0 + s0 * c1 - c1) / c0 > (c2 + s2 * c1 - c1) / c2
 *    1 + (s0 * c1 - c1) / c0 >  1 + (s2 * c1 - c1) / c2          | -1
 *        (s0 * c1 - c1) / c0 >      (s2 * c1 - c1) / c2          | /c1
 *        (s0 - 1)       / c0 >      (s2 - 1)       / c2          | *c0
 *        (s0 - 1)            >      (s2 - 1) * c0  / c2          | *c2
 *        (s0 - 1) * c2       >      (s2 - 1) * c0
 *        s0 * c2 - c2        >      s2 * c0 - c0                 | +c0
 *   c0 + s0 * c2 - c2        >      s2 * c0                      | +c2
 *   c0 + s0 * c2             > c2 + s2 * c0
 */
object PredicateOrdering extends Ordering[(CostPerRow, Selectivity)] {

  override def compare(predicate0: (CostPerRow, Selectivity), predicate1: (CostPerRow, Selectivity)): Int = {
    val cost01 = costFor(predicate0, predicate1)
    val cost10 = costFor(predicate1, predicate0)
    compareWithTolerance(cost01, cost10)
  }

  private def compareWithTolerance(a: Double, b: Double): Int = {
    if (math.abs(a - b) <= 0.000001) 0
    else if (a < b) -1
    else 1
  }

  /**
   * The cost per row for evaluating first predicate0 and then predicate1.
   */
  private def costFor(predicate0: (CostPerRow, Selectivity), predicate1: (CostPerRow, Selectivity)): Double =
    (predicate0, predicate1) match {
      case ((CostPerRow(c0), Selectivity(s0)), (CostPerRow(c1), _)) =>
        // predicate0 needs to be evaluated on all rows.
        // predicate1 only on those where predicate0==true
        c0 + c1 * s0
    }
}
