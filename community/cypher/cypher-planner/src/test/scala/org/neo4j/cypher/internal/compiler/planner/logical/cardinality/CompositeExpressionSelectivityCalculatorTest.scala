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
package org.neo4j.cypher.internal.compiler.planner.logical.cardinality

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.compiler.planner.logical.CardinalitySupport.SelectivityEquality
import org.neo4j.cypher.internal.compiler.planner.logical.PlannerDefaults.DEFAULT_RANGE_SEEK_FACTOR
import org.neo4j.cypher.internal.compiler.planner.logical.PlannerDefaults.DEFAULT_STRING_LENGTH
import org.neo4j.cypher.internal.compiler.planner.logical.cardinality.CompositeExpressionSelectivityCalculator.SelectivitiesForPredicates
import org.neo4j.cypher.internal.compiler.planner.logical.cardinality.CompositeExpressionSelectivityCalculator.selectivityForCompositeIndexPredicates
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.functions.Distance
import org.neo4j.cypher.internal.util.Selectivity
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class CompositeExpressionSelectivityCalculatorTest extends CypherFunSuite with AstConstructionTestSupport {

  private val combiner: SelectivityCombiner = IndependenceCombiner

  /**
   * @param predicate compute the predicate given a property
   * @param calculateSelectivity calculate the selectivity of the predicate, assuming the existence of the property.
   *                             The arguments to the function are
   *                             1) the existence selectivity
   *                             2) the uniqueness selectivity of the index
   *                             3) the number of properties in the index
   */
  case class TestedPredicate(
    description: String,
    predicate: Property => Expression,
    calculateSelectivity: (Double, Double, Int) => Double
  )

  val testedPredicates = Seq(
    TestedPredicate(
      "equals",
      equals(_, literalInt(42)),
      (_, uniqueSelectivity, numProps) => Math.pow(uniqueSelectivity, 1.0 / numProps)
    ),
    TestedPredicate(
      "in",
      in(_, listOfInt(1, 2, 3, 4)),
      (_, uniqueSelectivity, numProps) =>
        combiner.orTogetherSelectivities(
          Seq.fill(4)(Selectivity(Math.pow(uniqueSelectivity, 1.0 / numProps)))
        ).get.factor
    ),
    TestedPredicate("is not null", isNotNull(_), (_, _, _) => 1.0),
    TestedPredicate(
      "starts with literal",
      startsWith(_, literalString("foo")),
      (_, _, _) => DEFAULT_RANGE_SEEK_FACTOR / "foo".length
    ),
    TestedPredicate(
      "starts with expression",
      startsWith(_, v"x"),
      (_, _, _) => DEFAULT_RANGE_SEEK_FACTOR / DEFAULT_STRING_LENGTH
    ),
    TestedPredicate(
      "ends with literal",
      endsWith(_, literalString("foo")),
      (_, _, _) => DEFAULT_RANGE_SEEK_FACTOR / "foo".length
    ),
    TestedPredicate(
      "ends with expression",
      endsWith(_, v"x"),
      (_, _, _) => DEFAULT_RANGE_SEEK_FACTOR / DEFAULT_STRING_LENGTH
    ),
    TestedPredicate(
      "contains literal",
      contains(_, literalString("foo")),
      (_, _, _) => DEFAULT_RANGE_SEEK_FACTOR / "foo".length
    ),
    TestedPredicate(
      "contains expression",
      contains(_, v"x"),
      (_, _, _) => DEFAULT_RANGE_SEEK_FACTOR / DEFAULT_STRING_LENGTH
    ),
    TestedPredicate(
      "distance",
      property => lessThan(Distance.asInvocation(property, v"point")(pos), literalFloat(3.5)),
      (_, _, _) => DEFAULT_RANGE_SEEK_FACTOR
    ),
    TestedPredicate(
      "greaterThan",
      greaterThan(_, literalInt(42)),
      // This calculation is only correct for very low unique selectivities (a lot of distinct elements in the index).
      (_, uniqueSelectivity, numProps) => (1 - Math.pow(uniqueSelectivity, 1.0 / numProps)) * DEFAULT_RANGE_SEEK_FACTOR
    ),
    TestedPredicate(
      "lessThan",
      lessThan(_, literalInt(42)),
      // This calculation is only correct for very low unique selectivities (a lot of distinct elements in the index).
      (_, uniqueSelectivity, numProps) => (1 - Math.pow(uniqueSelectivity, 1.0 / numProps)) * DEFAULT_RANGE_SEEK_FACTOR
    ),
    TestedPredicate(
      "greaterThanOrEqual",
      greaterThanOrEqual(_, literalInt(42)),
      // This calculation is only correct for very low unique selectivities (a lot of distinct elements in the index).
      (_, uniqueSelectivity, numProps) => {
        val eqSel = Math.pow(uniqueSelectivity, 1.0 / numProps)
        (1 - eqSel) * DEFAULT_RANGE_SEEK_FACTOR + eqSel
      }
    ),
    TestedPredicate(
      "lessThanOrEqual",
      lessThanOrEqual(_, literalInt(42)),
      // This calculation is only correct for very low unique selectivities (a lot of distinct elements in the index).
      (_, uniqueSelectivity, numProps) => {
        val eqSel = Math.pow(uniqueSelectivity, 1.0 / numProps)
        (1 - eqSel) * DEFAULT_RANGE_SEEK_FACTOR + eqSel
      }
    ),
    TestedPredicate(
      "range: x < prop < y",
      prop => andedPropertyInequalities(greaterThan(prop, literalInt(10)), lessThan(prop, literalInt(100))),
      // This calculation is only correct for very low unique selectivities (a lot of distinct elements in the index).
      (_, uniqueSelectivity, numProps) =>
        (1 - Math.pow(uniqueSelectivity, 1.0 / numProps)) * DEFAULT_RANGE_SEEK_FACTOR / 2
    ),
    TestedPredicate(
      "range: x <= prop < y",
      prop => andedPropertyInequalities(greaterThanOrEqual(prop, literalInt(10)), lessThan(prop, literalInt(100))),
      // This calculation is only correct for very low unique selectivities (a lot of distinct elements in the index).
      (_, uniqueSelectivity, numProps) => {
        val eqSel = Math.pow(uniqueSelectivity, 1.0 / numProps)
        (1 - eqSel) * DEFAULT_RANGE_SEEK_FACTOR / 2 + eqSel
      }
    ),
    TestedPredicate(
      "range: x < prop <= y",
      prop => andedPropertyInequalities(greaterThan(prop, literalInt(10)), lessThanOrEqual(prop, literalInt(100))),
      // This calculation is only correct for very low unique selectivities (a lot of distinct elements in the index).
      (_, uniqueSelectivity, numProps) => {
        val eqSel = Math.pow(uniqueSelectivity, 1.0 / numProps)
        (1 - eqSel) * DEFAULT_RANGE_SEEK_FACTOR / 2 + eqSel
      }
    ),
    TestedPredicate(
      "range: x <= prop <= y",
      prop =>
        andedPropertyInequalities(greaterThanOrEqual(prop, literalInt(10)), lessThanOrEqual(prop, literalInt(100))),
      // This calculation is only correct for very low unique selectivities (a lot of distinct elements in the index).
      (_, uniqueSelectivity, numProps) => {
        val eqSel = Math.pow(uniqueSelectivity, 1.0 / numProps)
        (1 - eqSel) * DEFAULT_RANGE_SEEK_FACTOR / 2 + eqSel
      }
    )
  )

  // Single predicates
  for (testedPredicate <- testedPredicates) {
    test(s"selectivityForCompositeIndexPredicates, single predicate: ${testedPredicate.description}") {
      val predicate = testedPredicate.predicate(prop("n", "prop"))
      val existsSelectivity = Selectivity(0.1)
      val uniqueSelectivity = Selectivity(0.005)
      val selectivities = SelectivitiesForPredicates(Set(predicate), existsSelectivity, uniqueSelectivity, 1)
      val actual = selectivityForCompositeIndexPredicates(selectivities, combiner)
      val expected = existsSelectivity * Selectivity(testedPredicate.calculateSelectivity(
        existsSelectivity.factor,
        uniqueSelectivity.factor,
        1
      ))
      actual should equal(expected)(SelectivityEquality)
    }
  }

  // Two predicates
  for (testedPredicate1 <- testedPredicates; testedPredicate2 <- testedPredicates) {
    test(
      s"selectivityForCompositeIndexPredicates, two predicates: ${testedPredicate1.description}, ${testedPredicate2.description}"
    ) {
      val predicates = Set(
        testedPredicate1.predicate(prop("n", "prop")),
        testedPredicate2.predicate(prop("n", "prop2"))
      )
      val existsSelectivity = Selectivity(0.1)
      val uniqueSelectivity = Selectivity(0.00005)
      val selectivities = SelectivitiesForPredicates(predicates, existsSelectivity, uniqueSelectivity, 2)
      val actual = selectivityForCompositeIndexPredicates(selectivities, combiner)
      val expected = existsSelectivity *
        Selectivity(testedPredicate1.calculateSelectivity(existsSelectivity.factor, uniqueSelectivity.factor, 2)) *
        Selectivity(testedPredicate2.calculateSelectivity(existsSelectivity.factor, uniqueSelectivity.factor, 2))
      actual should equal(expected)(SelectivityEquality)
    }
  }

  // Two indexed properties, only one predicate
  for (testedPredicate <- testedPredicates) {
    test(
      s"selectivityForCompositeIndexPredicates, two indexed properties, one predicate: ${testedPredicate.description}"
    ) {
      val predicate = testedPredicate.predicate(prop("n", "prop"))
      val existsSelectivity = Selectivity(0.1)
      val uniqueSelectivity = Selectivity(0.00005)
      val selectivities = SelectivitiesForPredicates(Set(predicate), existsSelectivity, uniqueSelectivity, 2)
      val actual = selectivityForCompositeIndexPredicates(selectivities, combiner)
      val expected = existsSelectivity * Selectivity(testedPredicate.calculateSelectivity(
        existsSelectivity.factor,
        uniqueSelectivity.factor,
        2
      ))
      actual should equal(expected)(SelectivityEquality)
    }
  }
}
