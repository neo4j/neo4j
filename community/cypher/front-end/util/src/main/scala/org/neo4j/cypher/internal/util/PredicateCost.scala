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
 *
 * As it happens, cost per row can be 0, and selectivity can be 1 (when a predicate returns all incoming rows).
 * This breaks the transitivity property of equality.
 * Particularly, the cost of a predicate with a cost per row of 0 and a selectivity of 1 will be equal to the cost of
 * any other predicate:
 *   0 + 1 * c = c + s * 0
 * So two predicates with different costs will both have the same cost as the no-op predicate, yikes!
 * We can break down the various cases when comparing the cost of two arbitrary predicates p0 and p1:
 *   c0 = 0
 *     c1 = 0
 *       0 + s0 * 0 = 0 + s1 * 0      (EQ)
 *     c1 > 0
 *       s0 = 1
 *         0 + 1 * c1 = c1 + s1 * 0   (EQ)
 *       s0 < 1
 *         0 + s0 * c1 < c1 + s1 * 0  (LT)
 *   c0 > 0
 *     c1 = 0
 *       s1 = 1
 *         c0 + s0 * 0 = 0 + 1 * c0   (EQ)
 *       s1 < 1
 *         c0 + s0 * 0 > 0 + s1 * c0  (GT)
 *     c1 > 0
 *       both c0 and c1 > 0, we can safely compare c0 + s0 * c1 with c1 + s1 * c0 like described higher up.
 *
 * To define a total order for predicates, handling the case where cost per row is 0, we need some extra constructs:
 * If cost per row is 0 and selectivity is 1, we call it a no-op predicate, and that can be applied first.
 * If the cost per row is 0, but the selectivity is less than 1, we call it a free predicate, and it comes after no-op.
 * After that, we order the remaining predicates using the compare c0 + s0 * c1 vs c1 + s1 * c0 comparison.
 *
 * Finally we can rejig our comparison formula (when both c0 and c1 > 0):
 *   c0 + s0 * c1 <= c1 + s1 * c0    | -c0
 *   s0 * c1 <= c1 + s1 * c0 - c0    | -c1
 *   s0 * c1 - c1 <= s1 * c0 - c0    | factorise c1
 *   c1 * (s0 - 1) <= s1 * c0 - c0   | factorise c0
 *   c1 * (s0 - 1) <= c0 * (s1 - 1)  | /c1
 *   (s0 - 1) <= c0 * (s1 - 1) / c1  | /c0
 *   (s0 - 1) / c0 <= (s1 - 1) / c1
 * For a predicate p, we have a unique factor f = (s - 1) / c that we can use to base our ordering on.
 */
sealed trait PredicateCost extends Ordered[PredicateCost]

object PredicateCost {

  private case object NoOp extends PredicateCost {

    override def compare(other: PredicateCost): Int =
      other match {
        case NoOp                   => 0
        case Free                   => -1
        case SelectivityAdjusted(_) => -1
      }
  }

  private case object Free extends PredicateCost {

    override def compare(other: PredicateCost): Int =
      other match {
        case NoOp                   => 1
        case Free                   => 0
        case SelectivityAdjusted(_) => -1
      }
  }

  private case class SelectivityAdjusted(factor: Double) extends PredicateCost {

    override def compare(other: PredicateCost): Int =
      other match {
        case NoOp                             => 1
        case Free                             => 1
        case SelectivityAdjusted(otherFactor) => factor.compare(otherFactor)
      }
  }

  def apply(costPerRow: CostPerRow, selectivity: Selectivity): PredicateCost =
    if (costPerRow.cost == 0.0) {
      if (selectivity.factor == 1.0)
        NoOp
      else
        Free
    } else {
      SelectivityAdjusted((selectivity.factor - 1.0) / costPerRow.cost)
    }
}
