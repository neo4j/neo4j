/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.v3_5.planner.logical.cardinality

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.neo4j.cypher.internal.ir.v3_5._
import org.neo4j.cypher.internal.planner.v3_5.spi.GraphStatistics
import org.neo4j.cypher.internal.planner.v3_5.spi.GraphStatistics.DEFAULT_EQUALITY_SELECTIVITY
import org.neo4j.cypher.internal.planner.v3_5.spi.GraphStatistics.DEFAULT_LIST_CARDINALITY
import org.neo4j.cypher.internal.planner.v3_5.spi.GraphStatistics.DEFAULT_PROPERTY_SELECTIVITY
import org.neo4j.cypher.internal.planner.v3_5.spi.GraphStatistics.DEFAULT_RANGE_SEEK_FACTOR
import org.neo4j.cypher.internal.planner.v3_5.spi.GraphStatistics.DEFAULT_RANGE_SELECTIVITY
import org.neo4j.cypher.internal.planner.v3_5.spi.GraphStatistics.DEFAULT_STRING_LENGTH
import org.neo4j.cypher.internal.planner.v3_5.spi.GraphStatistics.DEFAULT_TYPE_SELECTIVITY
import org.neo4j.cypher.internal.planner.v3_5.spi.IndexDescriptor
import org.neo4j.cypher.internal.v3_5.ast._
import org.neo4j.cypher.internal.v3_5.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.v3_5.expressions.LessThan
import org.neo4j.cypher.internal.v3_5.expressions._
import org.neo4j.cypher.internal.v3_5.expressions.functions.Distance
import org.neo4j.cypher.internal.v3_5.expressions.functions.Exists
import org.neo4j.cypher.internal.v3_5.util._
import org.neo4j.cypher.internal.v3_5.util.test_helpers.CypherFunSuite

class ExpressionSelectivityCalculatorTest extends CypherFunSuite with AstConstructionTestSupport {

  private val indexPerson = IndexDescriptor(LabelId(0), Seq(PropertyKeyId(0)))
  private val indexAnimal = IndexDescriptor(LabelId(1), Seq(PropertyKeyId(0)))

  private val nProp: Property = Property(varFor("n"), PropertyKeyName("prop") _) _

  private val nIsPerson = Predicate(Set("n"), HasLabels(varFor("n"), Seq(LabelName("Person") _)) _)
  private val nIsAnimal = Predicate(Set("n"), HasLabels(varFor("n"), Seq(LabelName("Animal") _)) _)

  private val personPropSel = 0.2
  private val indexPersonUniqueSel = 1.0 / 180.0

  private val PERSON_COUNT = 1000.0

  // RANGE SEEK

  test("half-open (>) range with no label") {
    val inequality = Predicate(Set("n"), AndedPropertyInequalities(varFor("n"), nProp, NonEmptyList(
      GreaterThan(nProp, SignedDecimalIntegerLiteral("3") _) _
    )))

    val calculator = setUpCalculator(inequality, Seq.empty)
    val inequalityResult = calculator(inequality.expr)
    inequalityResult should equal(DEFAULT_RANGE_SELECTIVITY)
  }

  test("half-open (>=) range with no label") {
    val inequality = Predicate(Set("n"), AndedPropertyInequalities(varFor("n"), nProp, NonEmptyList(
      GreaterThanOrEqual(nProp, SignedDecimalIntegerLiteral("3") _) _
    )))

    val calculator = setUpCalculator(inequality, Seq.empty)
    val inequalityResult = calculator(inequality.expr)
    inequalityResult.factor should equal(DEFAULT_RANGE_SELECTIVITY.factor + DEFAULT_EQUALITY_SELECTIVITY.factor)
  }

  test("closed (> && <) range with no label") {
    val inequality = Predicate(Set("n"), AndedPropertyInequalities(varFor("n"), nProp, NonEmptyList(
      GreaterThan(nProp, SignedDecimalIntegerLiteral("3") _) _,
      LessThan(nProp, SignedDecimalIntegerLiteral("4") _) _
    )))

    val calculator = setUpCalculator(inequality, Seq.empty)
    val inequalityResult = calculator(inequality.expr)
    inequalityResult.factor should equal(DEFAULT_RANGE_SELECTIVITY.factor / 2)
  }

  test("closed (>= && <) range with no label") {
    val inequality = Predicate(Set("n"), AndedPropertyInequalities(varFor("n"), nProp, NonEmptyList(
      GreaterThanOrEqual(nProp, SignedDecimalIntegerLiteral("3") _) _,
      LessThan(nProp, SignedDecimalIntegerLiteral("4") _) _
    )))

    val calculator = setUpCalculator(inequality, Seq.empty)
    val inequalityResult = calculator(inequality.expr)
    inequalityResult.factor should equal(DEFAULT_RANGE_SELECTIVITY.factor / 2 + DEFAULT_EQUALITY_SELECTIVITY.factor)
  }

  test("three inequalityualities should be equal to two inequalityualities, no labels") {
    val inequality = Predicate(Set("n"), AndedPropertyInequalities(varFor("n"), nProp, NonEmptyList(
      GreaterThan(nProp, SignedDecimalIntegerLiteral("3") _) _,
      LessThan(nProp, SignedDecimalIntegerLiteral("4") _) _,
      LessThan(nProp, SignedDecimalIntegerLiteral("7") _) _
    )))

    val calculator = setUpCalculator(inequality, Seq.empty)
    val inequalityResult = calculator(inequality.expr)
    inequalityResult.factor should equal(DEFAULT_RANGE_SELECTIVITY.factor / 2)
  }

  test("half-open (>) range with one label") {
    val inequality = Predicate(Set("n"), AndedPropertyInequalities(varFor("n"), nProp, NonEmptyList(
      GreaterThan(nProp, SignedDecimalIntegerLiteral("3") _) _
    )))

    val calculator = setUpCalculator(inequality, Seq(nIsPerson))

    val labelResult = calculator(nIsPerson.expr)
    val inequalityResult = calculator(inequality.expr)

    labelResult.factor should equal(0.1)
    inequalityResult.factor should equal(
      personPropSel
        * (1-indexPersonUniqueSel) // Selectivity for != x
        * DEFAULT_RANGE_SEEK_FACTOR // Selectivity for range
        +- 0.00000001
    )
  }

  test("half-open (>=) range with one label") {
    val inequality = Predicate(Set("n"), AndedPropertyInequalities(varFor("n"), nProp, NonEmptyList(
      GreaterThanOrEqual(nProp, SignedDecimalIntegerLiteral("3") _) _
    )))

    val calculator = setUpCalculator(inequality, Seq(nIsPerson))

    val labelResult = calculator(nIsPerson.expr)
    val inequalityResult = calculator(inequality.expr)

    labelResult.factor should equal(0.1)
    inequalityResult.factor should equal(
      personPropSel
        * (1 - indexPersonUniqueSel) // Selectivity for != x
        * DEFAULT_RANGE_SEEK_FACTOR // Selectivity for range
        + personPropSel
        * indexPersonUniqueSel
        +- 0.00000001
    )
  }

  test("closed (> && <) range with one label") {
    val inequality = Predicate(Set("n"), AndedPropertyInequalities(varFor("n"), nProp, NonEmptyList(
      GreaterThan(nProp, SignedDecimalIntegerLiteral("3") _) _,
      LessThan(nProp, SignedDecimalIntegerLiteral("4") _) _
    )))

    val calculator = setUpCalculator(inequality, Seq(nIsPerson))

    val labelResult = calculator(nIsPerson.expr)
    val inequalityResult = calculator(inequality.expr)

    labelResult.factor should equal(0.1)
    inequalityResult.factor should equal(
      personPropSel
        * (1-indexPersonUniqueSel) // Selectivity for != x
        * DEFAULT_RANGE_SEEK_FACTOR / 2 // Selectivity for range
        +- 0.00000001
    )
  }

  test("closed (>= && <) range with one label") {
    val inequality = Predicate(Set("n"), AndedPropertyInequalities(varFor("n"), nProp, NonEmptyList(
      GreaterThanOrEqual(nProp, SignedDecimalIntegerLiteral("3") _) _,
      LessThan(nProp, SignedDecimalIntegerLiteral("4") _) _
    )))

    val calculator = setUpCalculator(inequality, Seq(nIsPerson))

    val labelResult = calculator(nIsPerson.expr)
    val inequalityResult = calculator(inequality.expr)

    labelResult.factor should equal(0.1)
    inequalityResult.factor should equal(
      personPropSel
        * (1 - indexPersonUniqueSel) // Selectivity for != x
        * DEFAULT_RANGE_SEEK_FACTOR / 2 // Selectivity for range
        + personPropSel
        * indexPersonUniqueSel
        +- 0.00000001
    )
  }

  test("three inequalityualities should be equal to two inequalityualities, one label") {
    val inequality = Predicate(Set("n"), AndedPropertyInequalities(varFor("n"), nProp, NonEmptyList(
      GreaterThan(nProp, SignedDecimalIntegerLiteral("3") _) _,
      LessThan(nProp, SignedDecimalIntegerLiteral("4") _) _,
      LessThan(nProp, SignedDecimalIntegerLiteral("7") _) _
    )))

    val calculator = setUpCalculator(inequality, Seq(nIsPerson))

    val labelResult = calculator(nIsPerson.expr)
    val inequalityResult = calculator(inequality.expr)

    labelResult.factor should equal(0.1)
    inequalityResult.factor should equal(
      personPropSel
        * (1-indexPersonUniqueSel) // Selectivity for != x
        * DEFAULT_RANGE_SEEK_FACTOR / 2 // Selectivity for range
        +- 0.00000001
    )
  }

  test("half-open (>) range with one label, no index") {
    val inequality = Predicate(Set("n"), AndedPropertyInequalities(varFor("n"), nProp, NonEmptyList(
      GreaterThan(nProp, SignedDecimalIntegerLiteral("3") _) _
    )))

    val calculator = setUpCalculator(inequality, Seq(nIsPerson), mockStats(indexCardinalities = Map.empty))

    val labelResult = calculator(nIsPerson.expr)
    val inequalityResult = calculator(inequality.expr)

    labelResult.factor should equal(0.1)
    inequalityResult should equal(DEFAULT_RANGE_SELECTIVITY)
  }

  test("closed (> && <) range with one label, no index") {
    val inequality = Predicate(Set("n"), AndedPropertyInequalities(varFor("n"), nProp, NonEmptyList(
      GreaterThan(nProp, SignedDecimalIntegerLiteral("3") _) _,
      LessThan(nProp, SignedDecimalIntegerLiteral("4") _) _
    )))

    val calculator = setUpCalculator(inequality, Seq(nIsPerson), mockStats(indexCardinalities = Map.empty))

    val labelResult = calculator(nIsPerson.expr)
    val inequalityResult = calculator(inequality.expr)

    labelResult.factor should equal(0.1)
    inequalityResult.factor should equal(DEFAULT_RANGE_SELECTIVITY.factor / 2)
  }

  test("half-open (>) range with two labels, one index") {
    val inequality = Predicate(Set("n"), AndedPropertyInequalities(varFor("n"), nProp, NonEmptyList(
      GreaterThan(nProp, SignedDecimalIntegerLiteral("3") _) _
    )))

    val calculator = setUpCalculator(inequality, Seq(nIsPerson, nIsAnimal),
      mockStats(labelCardinalities = Map(indexPerson.label -> 1000.0, indexAnimal.label -> 10.0)))

    val labelResult1 = calculator(nIsPerson.expr)
    val labelResult2 = calculator(nIsAnimal.expr)
    val inequalityResult = calculator(inequality.expr)

    labelResult1.factor should equal(0.1)
    labelResult2.factor should equal(0.001)
    inequalityResult.factor should equal(
      personPropSel
        * (1-indexPersonUniqueSel) // Selectivity for != x
        * DEFAULT_RANGE_SEEK_FACTOR // Selectivity for range
        +- 0.00000001
    )
  }

  test("closed (> && <) range with two labels, one index") {
    val inequality = Predicate(Set("n"), AndedPropertyInequalities(varFor("n"), nProp, NonEmptyList(
      GreaterThan(nProp, SignedDecimalIntegerLiteral("3") _) _,
      LessThan(nProp, SignedDecimalIntegerLiteral("4") _) _
    )))

    val calculator = setUpCalculator(inequality, Seq(nIsPerson),
      mockStats(labelCardinalities = Map(indexPerson.label -> 1000.0, indexAnimal.label -> 10.0)))

    val labelResult = calculator(nIsPerson.expr)
    val labelResult2 = calculator(nIsAnimal.expr)
    val inequalityResult = calculator(inequality.expr)

    labelResult.factor should equal(0.1)
    labelResult2.factor should equal(0.001)
    inequalityResult.factor should equal(
      personPropSel
        * (1 - indexPersonUniqueSel) // Selectivity for != x
        * DEFAULT_RANGE_SEEK_FACTOR / 2 // Selectivity for range
        +- 0.00000001
    )
  }

  test("half-open (>) range with two labels, two indexes") {
    val inequality = Predicate(Set("n"), AndedPropertyInequalities(varFor("n"), nProp, NonEmptyList(
      GreaterThan(nProp, SignedDecimalIntegerLiteral("3") _) _
    )))

    val calculator = setUpCalculator(inequality, Seq(nIsPerson, nIsAnimal), mockStats(
      labelCardinalities = Map(indexPerson.label -> 1000, indexAnimal.label -> 800.0),
      indexCardinalities = Map(indexPerson -> 200.0, indexAnimal -> 400.0),
      indexUniqueCardinalities = Map(indexPerson -> 180.0, indexAnimal -> 380.0)))

    val labelResult1 = calculator(nIsPerson.expr)
    val labelResult2 = calculator(nIsAnimal.expr)
    val inequalityResult = calculator(inequality.expr)

    labelResult1.factor should equal(0.1)
    labelResult2.factor should equal(0.08)

    val personIndexSelectivity = (
      personPropSel
        * (1-indexPersonUniqueSel) // Selectivity for != x
        * DEFAULT_RANGE_SEEK_FACTOR // Selectivity for range
      )
    val animalIndexSelectivity = (
      0.5 // Selectivity for .prop
        * (1.0 - 1.0 / 380.0) // Selectivity for != x
        * DEFAULT_RANGE_SEEK_FACTOR // Selectivity for range
      )

    inequalityResult.factor should equal(personIndexSelectivity + animalIndexSelectivity - personIndexSelectivity * animalIndexSelectivity
      +- 0.00000001)
  }

  test("half-open (>=) range with two labels, two indexes") {
    val inequality = Predicate(Set("n"), AndedPropertyInequalities(varFor("n"), nProp, NonEmptyList(
      GreaterThanOrEqual(nProp, SignedDecimalIntegerLiteral("3") _) _
    )))

    val calculator = setUpCalculator(inequality, Seq(nIsPerson, nIsAnimal), mockStats(
      labelCardinalities = Map(indexPerson.label -> 1000.0, indexAnimal.label -> 800.0),
      indexCardinalities = Map(indexPerson -> 200.0, indexAnimal -> 400.0),
      indexUniqueCardinalities = Map(indexPerson -> 180.0, indexAnimal -> 380.0)))

    val labelResult1 = calculator(nIsPerson.expr)
    val labelResult2 = calculator(nIsAnimal.expr)
    val inequalityResult = calculator(inequality.expr)

    labelResult1.factor should equal(0.1)
    labelResult2.factor should equal(0.08)

    val personIndexSelectivity = (
      personPropSel
        * (1 - indexPersonUniqueSel) // Selectivity for != x
        * DEFAULT_RANGE_SEEK_FACTOR // Selectivity for range
        + personPropSel
        * indexPersonUniqueSel
      )
    val animalIndexSelectivity = (
      0.5 // Selectivity for .prop
        * (1.0 - 1.0 / 380.0) // Selectivity for != x
        * DEFAULT_RANGE_SEEK_FACTOR // Selectivity for range
        + 0.5 // Selectivity for .prop
        * (1.0 / 380.0) // Selectivity for == 3
      )

    inequalityResult.factor should equal(personIndexSelectivity + animalIndexSelectivity - personIndexSelectivity * animalIndexSelectivity
      +- 0.00000001)
  }

  test("closed (> && <) range with two labels, two indexes") {
    val inequality = Predicate(Set("n"), AndedPropertyInequalities(varFor("n"), nProp, NonEmptyList(
      GreaterThan(nProp, SignedDecimalIntegerLiteral("3") _) _,
      LessThan(nProp, SignedDecimalIntegerLiteral("4") _) _
    )))

    val calculator = setUpCalculator(inequality, Seq(nIsPerson, nIsAnimal), mockStats(
      labelCardinalities = Map(indexPerson.label -> 1000.0, indexAnimal.label -> 800.0),
      indexCardinalities = Map(indexPerson -> 200.0, indexAnimal -> 400.0),
      indexUniqueCardinalities = Map(indexPerson -> 180.0, indexAnimal -> 380.0)))

    val labelResult1 = calculator(nIsPerson.expr)
    val labelResult2 = calculator(nIsAnimal.expr)
    val inequalityResult = calculator(inequality.expr)

    labelResult1.factor should equal(0.1)
    labelResult2.factor should equal(0.08)

    val personIndexSelectivity = (
      personPropSel
        * (1 - indexPersonUniqueSel) // Selectivity for != x
        * DEFAULT_RANGE_SEEK_FACTOR / 2 // Selectivity for range
      )
    val animalIndexSelectivity = (
      0.5 // Selectivity for .prop
        * (1.0 - 1.0 / 380.0) // Selectivity for != x
        * DEFAULT_RANGE_SEEK_FACTOR / 2 // Selectivity for range
      )

    inequalityResult.factor should equal(personIndexSelectivity + animalIndexSelectivity - personIndexSelectivity * animalIndexSelectivity
      +- 0.00000001)
  }

  test("closed (>= && <) range with two labels, two indexes") {
    val inequality = Predicate(Set("n"), AndedPropertyInequalities(varFor("n"), nProp, NonEmptyList(
      GreaterThanOrEqual(nProp, SignedDecimalIntegerLiteral("3") _) _,
      LessThan(nProp, SignedDecimalIntegerLiteral("4") _) _
    )))

    val calculator = setUpCalculator(inequality, Seq(nIsPerson, nIsAnimal), mockStats(
      labelCardinalities = Map(indexPerson.label -> 1000.0, indexAnimal.label -> 800.0),
      indexCardinalities = Map(indexPerson -> 200.0, indexAnimal -> 400.0),
      indexUniqueCardinalities = Map(indexPerson -> 180.0, indexAnimal -> 380.0)))

    val labelResult1 = calculator(nIsPerson.expr)
    val labelResult2 = calculator(nIsAnimal.expr)
    val inequalityResult = calculator(inequality.expr)

    labelResult1.factor should equal(0.1)
    labelResult2.factor should equal(0.08)

    val personIndexSelectivity = (
      personPropSel
        * (1 - indexPersonUniqueSel) // Selectivity for != x
        * DEFAULT_RANGE_SEEK_FACTOR / 2 // Selectivity for range
        + personPropSel
        * indexPersonUniqueSel
      )
    val animalIndexSelectivity = (
      0.5 // Selectivity for .prop
        * (1.0 - 1.0 / 380.0) // Selectivity for != x
        * DEFAULT_RANGE_SEEK_FACTOR / 2 // Selectivity for range
        + 0.5 // Selectivity for .prop
        * (1.0 / 380.0) // Selectivity for != x
      )

    inequalityResult.factor should equal(personIndexSelectivity + animalIndexSelectivity - personIndexSelectivity * animalIndexSelectivity
      +- 0.00000001)
  }

  // POINT DISTANCE

  private val fakePoint = True()(pos)
  private val distance = Predicate(Set("n"), LessThan(FunctionInvocation(nProp, FunctionName(Distance.name)(pos), fakePoint), SignedDecimalIntegerLiteral("3")(pos))(pos))

  test("distance with no label") {
    val calculator = setUpCalculator(distance, Seq.empty)
    val distanceResult = calculator(distance.expr)
    distanceResult should equal(DEFAULT_RANGE_SELECTIVITY)
  }

  test("distance with one label") {
    val calculator = setUpCalculator(distance, Seq(nIsPerson))

    val labelResult = calculator(nIsPerson.expr)
    val distanceResult = calculator(distance.expr)

    labelResult.factor should equal(0.1)
    distanceResult.factor should equal(
      0.2 // exists n.prop
        * DEFAULT_RANGE_SEEK_FACTOR // point distance
    )
  }

  test("distance with two labels, two indexes") {
    val calculator = setUpCalculator(distance, Seq(nIsPerson, nIsAnimal), mockStats(
      labelCardinalities = Map(indexPerson.label -> 1000.0, indexAnimal.label -> 800.0),
      indexCardinalities = Map(indexPerson -> 200.0, indexAnimal -> 400.0)))

    val labelResult1 = calculator(nIsPerson.expr)
    val labelResult2 = calculator(nIsAnimal.expr)
    val distanceResult = calculator(distance.expr)

    labelResult1.factor should equal(0.1)
    labelResult2.factor should equal(0.08)

    val personIndexSelectivity = (
      personPropSel
        * DEFAULT_RANGE_SEEK_FACTOR // point distance
      )
    val animalIndexSelectivity = (
      0.5 // Selectivity for .prop
        * DEFAULT_RANGE_SEEK_FACTOR // point distance
      )

    distanceResult.factor should equal(personIndexSelectivity + animalIndexSelectivity - personIndexSelectivity * animalIndexSelectivity
      +- 0.00000001)
  }

  // STARTS WITH

  test("starts with length 0, no label") {
    val stringPredicate = Predicate(Set("n"), StartsWith(nProp, StringLiteral("")(pos))(pos))

    val calculator = setUpCalculator(stringPredicate, Seq.empty)
    val stringPredicateResult = calculator(stringPredicate.expr)
    stringPredicateResult should equal(DEFAULT_PROPERTY_SELECTIVITY * DEFAULT_TYPE_SELECTIVITY)
  }

  test("starts with length 1, no label") {
    val stringPredicate = Predicate(Set("n"), StartsWith(nProp, StringLiteral("1")(pos))(pos))

    val calculator = setUpCalculator(stringPredicate, Seq.empty)
    val stringPredicateResult = calculator(stringPredicate.expr)
    stringPredicateResult should equal(DEFAULT_RANGE_SELECTIVITY)
  }

  test("starts with length 2, no label") {
    val stringPredicate = Predicate(Set("n"), StartsWith(nProp, StringLiteral("12")(pos))(pos))

    val calculator = setUpCalculator(stringPredicate, Seq.empty)
    val stringPredicateResult = calculator(stringPredicate.expr)
    stringPredicateResult.factor should equal(DEFAULT_RANGE_SELECTIVITY.factor / 2)
  }

  test("starts with length unknown, no label") {
    val stringPredicate = Predicate(Set("n"), StartsWith(nProp, varFor("string"))(pos))

    val calculator = setUpCalculator(stringPredicate, Seq.empty)
    val stringPredicateResult = calculator(stringPredicate.expr)
    stringPredicateResult.factor should equal(DEFAULT_RANGE_SELECTIVITY.factor / DEFAULT_STRING_LENGTH
    +- 0.00000001)
  }

  test("starts with length 0, one label") {
    val stringPredicate = Predicate(Set("n"), StartsWith(nProp, StringLiteral("")(pos))(pos))

    val calculator = setUpCalculator(stringPredicate, Seq(nIsPerson))

    val labelResult = calculator(nIsPerson.expr)
    val stringPredicateResult = calculator(stringPredicate.expr)

    labelResult.factor should equal(0.1)
    stringPredicateResult.factor should equal(
      0.2 // exists
      * DEFAULT_TYPE_SELECTIVITY.factor // is string
    )
  }

  test("starts with length 1, one label") {
    val stringPredicate = Predicate(Set("n"), StartsWith(nProp, StringLiteral("1")(pos))(pos))

    val calculator = setUpCalculator(stringPredicate, Seq(nIsPerson))

    val labelResult = calculator(nIsPerson.expr)
    val stringPredicateResult = calculator(stringPredicate.expr)

    labelResult.factor should equal(0.1)
    stringPredicateResult.factor should equal(
      0.2 // exists
      * DEFAULT_RANGE_SEEK_FACTOR // starts with
    )
  }

  test("starts with length 2, one label") {
    val stringPredicate = Predicate(Set("n"), StartsWith(nProp, StringLiteral("12")(pos))(pos))

    val calculator = setUpCalculator(stringPredicate, Seq(nIsPerson))

    val labelResult = calculator(nIsPerson.expr)
    val stringPredicateResult = calculator(stringPredicate.expr)

    labelResult.factor should equal(0.1)
    stringPredicateResult.factor should equal(
      0.2 // exists
      * DEFAULT_RANGE_SEEK_FACTOR / 2 // starts with
    )
  }

  test("starts with length unknown, one label") {
    val stringPredicate = Predicate(Set("n"), StartsWith(nProp, varFor("string"))(pos))

    val calculator = setUpCalculator(stringPredicate, Seq(nIsPerson))

    val labelResult = calculator(nIsPerson.expr)
    val stringPredicateResult = calculator(stringPredicate.expr)

    labelResult.factor should equal(0.1)
    stringPredicateResult.factor should equal(
      0.2 // exists
      * DEFAULT_RANGE_SEEK_FACTOR / DEFAULT_STRING_LENGTH // starts with
    )
  }

  test("starts with length 0, two labels") {
    val stringPredicate = Predicate(Set("n"), StartsWith(nProp, StringLiteral("")(pos))(pos))

    val calculator = setUpCalculator(stringPredicate, Seq(nIsPerson, nIsAnimal), mockStats(
      labelCardinalities = Map(indexPerson.label -> 1000.0, indexAnimal.label -> 800.0),
      indexCardinalities = Map(indexPerson -> 200.0, indexAnimal -> 400.0)))

    val labelResult1 = calculator(nIsPerson.expr)
    val labelResult2 = calculator(nIsAnimal.expr)
    val stringPredicateResult = calculator(stringPredicate.expr)

    labelResult1.factor should equal(0.1)
    labelResult2.factor should equal(0.08)

    val personIndexSelectivity = (
      0.2 // exists
        * DEFAULT_TYPE_SELECTIVITY.factor // is string
      )
    val animalIndexSelectivity = (
      0.5 // exists
        * DEFAULT_TYPE_SELECTIVITY.factor // is string
      )

    stringPredicateResult.factor should equal(personIndexSelectivity + animalIndexSelectivity - personIndexSelectivity * animalIndexSelectivity
      +- 0.00000001)
  }

  test("starts with length 1, two labels") {
    val stringPredicate = Predicate(Set("n"), StartsWith(nProp, StringLiteral("1")(pos))(pos))

    val calculator = setUpCalculator(stringPredicate, Seq(nIsPerson, nIsAnimal), mockStats(
      labelCardinalities = Map(indexPerson.label -> 1000.0, indexAnimal.label -> 800.0),
      indexCardinalities = Map(indexPerson -> 200.0, indexAnimal -> 400.0)))

    val labelResult1 = calculator(nIsPerson.expr)
    val labelResult2 = calculator(nIsAnimal.expr)
    val stringPredicateResult = calculator(stringPredicate.expr)

    labelResult1.factor should equal(0.1)
    labelResult2.factor should equal(0.08)

    val personIndexSelectivity = (
      0.2 // exists
        * DEFAULT_RANGE_SEEK_FACTOR // starts with
      )
    val animalIndexSelectivity = (
      0.5 // exists
        * DEFAULT_RANGE_SEEK_FACTOR // starts with
      )

    stringPredicateResult.factor should equal(personIndexSelectivity + animalIndexSelectivity - personIndexSelectivity * animalIndexSelectivity
      +- 0.00000001)
  }

  test("starts with length 2, two labels") {
    val stringPredicate = Predicate(Set("n"), StartsWith(nProp, StringLiteral("12")(pos))(pos))

    val calculator = setUpCalculator(stringPredicate, Seq(nIsPerson, nIsAnimal), mockStats(
      labelCardinalities = Map(indexPerson.label -> 1000.0, indexAnimal.label -> 800.0),
      indexCardinalities = Map(indexPerson -> 200.0, indexAnimal -> 400.0)))

    val labelResult1 = calculator(nIsPerson.expr)
    val labelResult2 = calculator(nIsAnimal.expr)
    val stringPredicateResult = calculator(stringPredicate.expr)

    labelResult1.factor should equal(0.1)
    labelResult2.factor should equal(0.08)

    val personIndexSelectivity = (
      0.2 // exists
        * DEFAULT_RANGE_SEEK_FACTOR / 2 // starts with
      )
    val animalIndexSelectivity = (
      0.5 // exists
        * DEFAULT_RANGE_SEEK_FACTOR / 2 // starts with
      )

    stringPredicateResult.factor should equal(personIndexSelectivity + animalIndexSelectivity - personIndexSelectivity * animalIndexSelectivity
      +- 0.00000001)
  }

  test("starts with length unknown, two labels") {
    val stringPredicate = Predicate(Set("n"), StartsWith(nProp, varFor("string"))(pos))

    val calculator = setUpCalculator(stringPredicate, Seq(nIsPerson, nIsAnimal), mockStats(
      labelCardinalities = Map(indexPerson.label -> 1000.0, indexAnimal.label -> 800.0),
      indexCardinalities = Map(indexPerson -> 200.0, indexAnimal -> 400.0)))

    val labelResult1 = calculator(nIsPerson.expr)
    val labelResult2 = calculator(nIsAnimal.expr)
    val stringPredicateResult = calculator(stringPredicate.expr)

    labelResult1.factor should equal(0.1)
    labelResult2.factor should equal(0.08)

    val personIndexSelectivity = (
      0.2 // exists
        * DEFAULT_RANGE_SEEK_FACTOR / DEFAULT_STRING_LENGTH // starts with
      )
    val animalIndexSelectivity = (
      0.5 // exists
        * DEFAULT_RANGE_SEEK_FACTOR / DEFAULT_STRING_LENGTH // starts with
      )

    stringPredicateResult.factor should equal(personIndexSelectivity + animalIndexSelectivity - personIndexSelectivity * animalIndexSelectivity
      +- 0.00000001)
  }

  // EXISTS

  private val exists = Predicate(Set("n"), FunctionInvocation(FunctionName(Exists.name)(pos), nProp)(pos))

  test("exists with no label") {
    val calculator = setUpCalculator(exists, Seq.empty)
    val existsResult = calculator(exists.expr)
    existsResult should equal(DEFAULT_PROPERTY_SELECTIVITY)
  }

  test("exists with one label") {
    val calculator = setUpCalculator(exists, Seq(nIsPerson))

    val labelResult = calculator(nIsPerson.expr)
    val existsResult = calculator(exists.expr)

    labelResult.factor should equal(0.1)
    existsResult.factor should equal(0.2)
  }

  test("exists with one label, no index") {
    val calculator = setUpCalculator(exists, Seq(nIsPerson), mockStats(indexCardinalities = Map.empty))

    val labelResult = calculator(nIsPerson.expr)
    val existsResult = calculator(exists.expr)

    labelResult.factor should equal(0.1)
    existsResult should equal(DEFAULT_PROPERTY_SELECTIVITY)
  }

  test("exists with two labels, one index") {
    val calculator = setUpCalculator(exists, Seq(nIsPerson, nIsAnimal),
      mockStats(labelCardinalities = Map(indexPerson.label -> 1000.0, indexAnimal.label -> 10.0)))

    val labelResult1 = calculator(nIsPerson.expr)
    val labelResult2 = calculator(nIsAnimal.expr)
    val existsResult = calculator(exists.expr)

    labelResult1.factor should equal(0.1)
    labelResult2.factor should equal(0.001)
    existsResult.factor should equal(0.2)
  }

  test("exists with two labels, two indexes") {
    val calculator = setUpCalculator(exists, Seq(nIsPerson, nIsAnimal), mockStats(
      labelCardinalities = Map(indexPerson.label -> 1000.0, indexAnimal.label -> 800.0),
      indexCardinalities = Map(indexPerson -> 200.0, indexAnimal -> 400.0)))

    val labelResult1 = calculator(nIsPerson.expr)
    val labelResult2 = calculator(nIsAnimal.expr)
    val existsResult = calculator(exists.expr)

    labelResult1.factor should equal(0.1)
    labelResult2.factor should equal(0.08)
    existsResult.factor should equal(0.2 + 0.5 - 0.2 * 0.5
      +- 0.00000001)
  }

  // EQUALITY / IN

  test("equality with no label, size 0") {
    val equals = Predicate(Set("n"), In(nProp, listOf()) _)

    val calculator = setUpCalculator(equals, Seq.empty)
    val eqResult = calculator(equals.expr)
    eqResult.factor should equal(0.0)
  }

  test("equality with no label, size 1") {
    val equals = Predicate(Set("n"), Equals(nProp, SignedDecimalIntegerLiteral("3") _) _)

    val calculator = setUpCalculator(equals, Seq.empty)
    val eqResult = calculator(equals.expr)
    eqResult should equal(DEFAULT_EQUALITY_SELECTIVITY)
  }

  test("equality with no label, size 2") {
    val equals = Predicate(Set("n"), In(nProp, listOf(SignedDecimalIntegerLiteral("3") _, SignedDecimalIntegerLiteral("4") _)) _)

    val calculator = setUpCalculator(equals, Seq.empty)
    val eqResult = calculator(equals.expr)
    val resFor1 = DEFAULT_EQUALITY_SELECTIVITY.factor
    eqResult.factor should equal(resFor1 + resFor1 - resFor1 * resFor1)
  }

  test("equality with no label, size unknown") {
    val equals = Predicate(Set("n"), In(nProp, varFor("someList")) _)

    val calculator = setUpCalculator(equals, Seq.empty)
    val eqResult = calculator(equals.expr)
    val resFor1 = DEFAULT_EQUALITY_SELECTIVITY
    eqResult should equal(IndependenceCombiner.orTogetherSelectivities(for (_ <- 1 to DEFAULT_LIST_CARDINALITY.amount.toInt) yield resFor1).get)
  }

  test("equality with one label, size 0") {
    val equals = Predicate(Set("n"), In(nProp, listOf()) _)

    val calculator = setUpCalculator(equals, Seq(nIsPerson))
    val labelResult = calculator(nIsPerson.expr)
    val eqResult = calculator(equals.expr)

    labelResult.factor should equal(0.1)
    eqResult.factor should equal(0.0)
  }

  test("equality with one label, size 1") {
    val equals = Predicate(Set("n"), Equals(nProp, SignedDecimalIntegerLiteral("3") _) _)

    val calculator = setUpCalculator(equals, Seq(nIsPerson))

    val labelResult = calculator(nIsPerson.expr)
    val eqResult = calculator(equals.expr)

    labelResult.factor should equal(0.1)
    eqResult.factor should equal(0.2 * (1.0 / 180.0))
  }

  test("equality with one label, size 2") {
    val equals = Predicate(Set("n"), In(nProp, listOf(SignedDecimalIntegerLiteral("3") _, SignedDecimalIntegerLiteral("4") _)) _)

    val calculator = setUpCalculator(equals, Seq(nIsPerson))

    val labelResult = calculator(nIsPerson.expr)
    val eqResult = calculator(equals.expr)

    labelResult.factor should equal(0.1)
    val resFor1 = 0.2 * (1.0 / 180.0)
    eqResult.factor should equal(resFor1 + resFor1 - resFor1 * resFor1)
  }

  test("equality with one label, size unknown") {
    val equals = Predicate(Set("n"), In(nProp, varFor("someList")) _)

    val calculator = setUpCalculator(equals, Seq(nIsPerson))

    val labelResult = calculator(nIsPerson.expr)
    val eqResult = calculator(equals.expr)

    labelResult.factor should equal(0.1)
    val resFor1 = Selectivity(0.2 * (1.0 / 180.0))
    eqResult should equal(IndependenceCombiner.orTogetherSelectivities(for (_ <- 1 to DEFAULT_LIST_CARDINALITY.amount.toInt) yield resFor1).get)
  }

  test("equality with two labels, size 0") {
    val equals = Predicate(Set("n"), In(nProp, listOf()) _)

    val calculator = setUpCalculator(equals, Seq(nIsPerson, nIsAnimal), mockStats(
      labelCardinalities = Map(indexPerson.label -> 1000.0, indexAnimal.label -> 800.0),
      indexUniqueCardinalities = Map(indexPerson -> 200.0, indexAnimal -> 400.0)))

    val labelResult1 = calculator(nIsPerson.expr)
    val labelResult2 = calculator(nIsAnimal.expr)
    val eqResult = calculator(equals.expr)

    labelResult1.factor should equal(0.1)
    labelResult2.factor should equal(0.08)
    eqResult.factor should equal(0.0)
  }

  test("equality with two labels, size 1") {
    val equals = Predicate(Set("n"), Equals(nProp, SignedDecimalIntegerLiteral("3") _) _)

    val calculator = setUpCalculator(equals, Seq(nIsPerson, nIsAnimal), mockStats(
      labelCardinalities = Map(indexPerson.label -> 1000.0, indexAnimal.label -> 800.0),
      indexCardinalities = Map(indexPerson -> 300.0, indexAnimal -> 500.0),
      indexUniqueCardinalities = Map(indexPerson -> 200.0, indexAnimal -> 400.0)))

    val labelResult1 = calculator(nIsPerson.expr)
    val labelResult2 = calculator(nIsAnimal.expr)
    val eqResult = calculator(equals.expr)

    labelResult1.factor should equal(0.1)
    labelResult2.factor should equal(0.08)

    val personSel = (300.0 / 1000.0) * (1.0 / 200.0)
    val animalSel = (500.0 / 800.0) * (1.0 / 400.0)
    eqResult.factor should equal(personSel + animalSel - personSel * animalSel)
  }

  test("equality with two labels, size 2") {
    val equals = Predicate(Set("n"), In(nProp, listOf(SignedDecimalIntegerLiteral("3") _, SignedDecimalIntegerLiteral("4") _)) _)

    val calculator = setUpCalculator(equals, Seq(nIsPerson, nIsAnimal), mockStats(
      labelCardinalities = Map(indexPerson.label -> 1000.0, indexAnimal.label -> 800.0),
      indexCardinalities = Map(indexPerson -> 300.0, indexAnimal -> 500.0),
      indexUniqueCardinalities = Map(indexPerson -> 200.0, indexAnimal -> 400.0)))

    val labelResult1 = calculator(nIsPerson.expr)
    val labelResult2 = calculator(nIsAnimal.expr)
    val eqResult = calculator(equals.expr)

    labelResult1.factor should equal(0.1)
    labelResult2.factor should equal(0.08)

    val personSel = (300.0 / 1000.0) * (1.0 / 200.0)
    val animalSel = (500.0 / 800.0) * (1.0 / 400.0)
    val resFor1 = personSel + animalSel - personSel * animalSel
    eqResult.factor should equal(resFor1 + resFor1 - resFor1 * resFor1)
  }

  test("equality with two labels, size unknown") {
    val equals = Predicate(Set("n"), In(nProp, varFor("someList")) _)

    val calculator = setUpCalculator(equals, Seq(nIsPerson, nIsAnimal), mockStats(
      labelCardinalities = Map(indexPerson.label -> 1000.0, indexAnimal.label -> 800.0),
      indexCardinalities = Map(indexPerson -> 300.0, indexAnimal -> 500.0),
      indexUniqueCardinalities = Map(indexPerson -> 200.0, indexAnimal -> 400.0)))

    val labelResult1 = calculator(nIsPerson.expr)
    val labelResult2 = calculator(nIsAnimal.expr)
    val eqResult = calculator(equals.expr)

    labelResult1.factor should equal(0.1)
    labelResult2.factor should equal(0.08)

    val personSel = (300.0 / 1000.0) * (1.0 / 200.0)
    val animalSel = (500.0 / 800.0) * (1.0 / 400.0)
    val resFor1 = Selectivity(personSel + animalSel - personSel * animalSel)
    eqResult should equal(IndependenceCombiner.orTogetherSelectivities(for (_ <- 1 to DEFAULT_LIST_CARDINALITY.amount.toInt) yield resFor1).get)
  }

  // OTHER

  test("Should peek inside sub predicates") {
    implicit val semanticTable = SemanticTable()
    semanticTable.resolvedLabelNames.put("Page", LabelId(0))

    implicit val selections = Selections(Set(Predicate(Set("n"), HasLabels(varFor("n"), Seq(LabelName("Page") _)) _)))

    val stats = mock[GraphStatistics]
    when(stats.nodesAllCardinality()).thenReturn(2000.0)
    when(stats.nodesWithLabelCardinality(Some(indexPerson.label))).thenReturn(1000.0)
    val calculator = ExpressionSelectivityCalculator(stats, IndependenceCombiner)

    val result = calculator(PartialPredicate[HasLabels](HasLabels(varFor("n"), Seq(LabelName("Page") _)) _, mock[HasLabels]))

    result.factor should equal(0.5)
  }

  test("should default to single cardinality for HasLabels with previously unknown label") {
    val stats = mock[GraphStatistics]
    when(stats.nodesAllCardinality()).thenReturn(Cardinality(10))
    val calculator = ExpressionSelectivityCalculator(stats, IndependenceCombiner)
    implicit val semanticTable = SemanticTable()
    implicit val selections = mock[Selections]

    val expr = HasLabels(null, Seq(LabelName("Foo")(pos)))(pos)
    calculator(expr) should equal(Selectivity.of(1.0 / 10.0).get)
  }

  // HELPER METHODS

  private def setUpCalculator(predicate: Predicate, hasLabels: Seq[Predicate], stats: GraphStatistics = mockStats()): Expression => Selectivity = {
    implicit val semanticTable = SemanticTable()
    semanticTable.resolvedLabelNames.put("Person", indexPerson.label)
    semanticTable.resolvedLabelNames.put("Animal", indexAnimal.label)
    semanticTable.resolvedPropertyKeyNames.put("prop", indexPerson.property)

    implicit val selections = Selections(Set(predicate) ++ hasLabels)
    val combiner = IndependenceCombiner
    val calculator = ExpressionSelectivityCalculator(stats, combiner)
    exp: Expression => calculator(exp)
  }

  /**
    * @param allNodesCardinality      total number of nodes
    * @param labelCardinalities       for each label, the number of nodes that have that label
    * @param indexCardinalities       for each index, the number of values in that index
    * @param indexUniqueCardinalities for each index, the number of unique values in that index
    */
  private def mockStats(allNodesCardinality: Double = 10000.0,
                        labelCardinalities: Map[LabelId, Double] = Map(indexPerson.label -> 1000.0),
                        indexCardinalities: Map[IndexDescriptor, Double] = Map(indexPerson -> 200.0),
                        indexUniqueCardinalities: Map[IndexDescriptor, Double] = Map(indexPerson -> 180.0)): GraphStatistics = {

    // sanity check:
    for {
      (id, indexCardinality) <- indexCardinalities
      labelCardinality <- labelCardinalities.get(id.label)
    } {
      if (indexCardinality > labelCardinality) {
        throw new IllegalArgumentException("Wrong test setup: Index cardinality cannot be larger than label cardinality")
      }
    }
    for {
      (id, indexUniqueCardinality) <- indexUniqueCardinalities
      otherCardinality <- indexCardinalities.get(id) ++ labelCardinalities.get(id.label)
    } {
      if (indexUniqueCardinality > otherCardinality) {
        throw new IllegalArgumentException("Wrong test setup: Index unique cardinality cannot be larger than index cardinality or label cardinality")
      }
    }

    val stats = mock[GraphStatistics]
    when(stats.nodesAllCardinality()).thenReturn(allNodesCardinality)
    labelCardinalities.foreach { case (label, number) =>
      when(stats.nodesWithLabelCardinality(Some(label))).thenReturn(number)
    }

    when(stats.indexPropertyExistsSelectivity(any())).thenAnswer(new Answer[Option[Selectivity]] {
      override def answer(invocationOnMock: InvocationOnMock): Option[Selectivity] = {
        val theIndex = invocationOnMock.getArgument[IndexDescriptor](0)
        for {
          indexCardinality <- indexCardinalities.get(theIndex)
          labelCardinality <- labelCardinalities.get(theIndex.label)
        } yield Selectivity(indexCardinality / labelCardinality)
      }
    })

    when(stats.uniqueValueSelectivity(any())).thenAnswer(new Answer[Option[Selectivity]] {
      override def answer(invocationOnMock: InvocationOnMock): Option[Selectivity] = {
        val theIndex = invocationOnMock.getArgument[IndexDescriptor](0)
        for {
          indexUniqueCardinality <- indexUniqueCardinalities.get(theIndex)
        } yield Selectivity(1 / indexUniqueCardinality)
      }
    })

    stats
  }
}
