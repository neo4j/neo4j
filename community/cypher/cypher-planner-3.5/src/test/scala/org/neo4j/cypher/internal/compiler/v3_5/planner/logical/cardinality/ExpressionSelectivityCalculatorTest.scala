/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
import org.neo4j.cypher.internal.planner.v3_5.spi.IndexDescriptor
import org.opencypher.v9_0.ast._
import org.opencypher.v9_0.ast.semantics.SemanticTable
import org.opencypher.v9_0.expressions.LessThan
import org.opencypher.v9_0.expressions._
import org.opencypher.v9_0.expressions.functions.Distance
import org.opencypher.v9_0.expressions.functions.Exists
import org.opencypher.v9_0.util._
import org.opencypher.v9_0.util.symbols._
import org.opencypher.v9_0.util.test_helpers.CypherFunSuite

class ExpressionSelectivityCalculatorTest extends CypherFunSuite with AstConstructionTestSupport {

  val indexPerson = IndexDescriptor(LabelId(0), Seq(PropertyKeyId(0)))
  val indexAnimal = IndexDescriptor(LabelId(1), Seq(PropertyKeyId(0)))

  // RANGE SEEK

  test("half-open (>) range with no label") {
    val n_prop: Property = Property(varFor("n"), PropertyKeyName("prop") _) _
    val ineqality = Predicate(Set("n"), AndedPropertyInequalities(varFor("n"), n_prop, NonEmptyList(
      GreaterThan(n_prop, SignedDecimalIntegerLiteral("3") _) _
    )))

    val calculator = setUpCalculator(ineqality, Seq.empty)
    val ineqResult = calculator(ineqality.expr)
    ineqResult should equal(GraphStatistics.DEFAULT_RANGE_SELECTIVITY)
  }

  test("half-open (>=) range with no label") {
    val n_prop: Property = Property(varFor("n"), PropertyKeyName("prop") _) _
    val ineqality = Predicate(Set("n"), AndedPropertyInequalities(varFor("n"), n_prop, NonEmptyList(
      GreaterThanOrEqual(n_prop, SignedDecimalIntegerLiteral("3") _) _
    )))

    val calculator = setUpCalculator(ineqality, Seq.empty)
    val ineqResult = calculator(ineqality.expr)
    ineqResult.factor should equal(GraphStatistics.DEFAULT_RANGE_SELECTIVITY.factor + GraphStatistics.DEFAULT_EQUALITY_SELECTIVITY.factor)
  }

  test("closed (> && <) range with no label") {
    val n_prop: Property = Property(varFor("n"), PropertyKeyName("prop") _) _
    val ineqality = Predicate(Set("n"), AndedPropertyInequalities(varFor("n"), n_prop, NonEmptyList(
      GreaterThan(n_prop, SignedDecimalIntegerLiteral("3") _) _,
      LessThan(n_prop, SignedDecimalIntegerLiteral("4") _) _
    )))

    val calculator = setUpCalculator(ineqality, Seq.empty)
    val ineqResult = calculator(ineqality.expr)
    ineqResult.factor should equal(GraphStatistics.DEFAULT_RANGE_SELECTIVITY.factor / 2)
  }

  test("closed (>= && <) range with no label") {
    val n_prop: Property = Property(varFor("n"), PropertyKeyName("prop") _) _
    val ineqality = Predicate(Set("n"), AndedPropertyInequalities(varFor("n"), n_prop, NonEmptyList(
      GreaterThanOrEqual(n_prop, SignedDecimalIntegerLiteral("3") _) _,
      LessThan(n_prop, SignedDecimalIntegerLiteral("4") _) _
    )))

    val calculator = setUpCalculator(ineqality, Seq.empty)
    val ineqResult = calculator(ineqality.expr)
    ineqResult.factor should equal(GraphStatistics.DEFAULT_RANGE_SELECTIVITY.factor / 2 + GraphStatistics.DEFAULT_EQUALITY_SELECTIVITY.factor)
  }

  test("three inequalities should be equal to two inequalities, no labels") {
    val n_prop: Property = Property(varFor("n"), PropertyKeyName("prop") _) _
    val ineqality = Predicate(Set("n"), AndedPropertyInequalities(varFor("n"), n_prop, NonEmptyList(
      GreaterThan(n_prop, SignedDecimalIntegerLiteral("3") _) _,
      LessThan(n_prop, SignedDecimalIntegerLiteral("4") _) _,
      LessThan(n_prop, SignedDecimalIntegerLiteral("7") _) _
    )))

    val calculator = setUpCalculator(ineqality, Seq.empty)
    val ineqResult = calculator(ineqality.expr)
    ineqResult.factor should equal(GraphStatistics.DEFAULT_RANGE_SELECTIVITY.factor / 2)
  }

  test("half-open (>) range with one label") {
    val n_is_Person = Predicate(Set("n"), HasLabels(varFor("n"), Seq(LabelName("Person") _)) _)
    val n_prop: Property = Property(varFor("n"), PropertyKeyName("prop") _) _
    val ineqality = Predicate(Set("n"), AndedPropertyInequalities(varFor("n"), n_prop, NonEmptyList(
      GreaterThan(n_prop, SignedDecimalIntegerLiteral("3") _) _
    )))

    val calculator = setUpCalculator(ineqality, Seq(n_is_Person))

    val labelResult = calculator(n_is_Person.expr)
    val ineqResult = calculator(ineqality.expr)

    labelResult.factor should equal(0.1)
    ineqResult.factor should equal(
      0.2 // Selectivity for .prop
        * 0.75 // Selectivity for != x
        * GraphStatistics.DEFAULT_RANGE_SEEK_FACTOR // Selectivity for range
        +- 0.000001
    )
  }

  test("half-open (>=) range with one label") {
    val n_is_Person = Predicate(Set("n"), HasLabels(varFor("n"), Seq(LabelName("Person") _)) _)
    val n_prop: Property = Property(varFor("n"), PropertyKeyName("prop") _) _
    val ineqality = Predicate(Set("n"), AndedPropertyInequalities(varFor("n"), n_prop, NonEmptyList(
      GreaterThanOrEqual(n_prop, SignedDecimalIntegerLiteral("3") _) _
    )))

    val calculator = setUpCalculator(ineqality, Seq(n_is_Person))

    val labelResult = calculator(n_is_Person.expr)
    val ineqResult = calculator(ineqality.expr)

    labelResult.factor should equal(0.1)
    ineqResult.factor should equal(
      0.2 // Selectivity for .prop
        * 0.75 // Selectivity for != 3
        * GraphStatistics.DEFAULT_RANGE_SEEK_FACTOR // Selectivity for range
        + 0.2 // Selectivity for .prop
        * 0.25 // Selectivity for == 3
        +- 0.000001
    )
  }

  test("closed (> && <) range with one label") {
    val n_is_Person = Predicate(Set("n"), HasLabels(varFor("n"), Seq(LabelName("Person") _)) _)
    val n_prop: Property = Property(varFor("n"), PropertyKeyName("prop") _) _
    val ineqality = Predicate(Set("n"), AndedPropertyInequalities(varFor("n"), n_prop, NonEmptyList(
      GreaterThan(n_prop, SignedDecimalIntegerLiteral("3") _) _,
      LessThan(n_prop, SignedDecimalIntegerLiteral("4") _) _
    )))

    val calculator = setUpCalculator(ineqality, Seq(n_is_Person))

    val labelResult = calculator(n_is_Person.expr)
    val ineqResult = calculator(ineqality.expr)

    labelResult.factor should equal(0.1)
    ineqResult.factor should equal(
      0.2 // Selectivity for .prop
        * 0.75 // Selectivity for != x
        * GraphStatistics.DEFAULT_RANGE_SEEK_FACTOR / 2 // Selectivity for range
        +- 0.000001
    )
  }

  test("closed (>= && <) range with one label") {
    val n_is_Person = Predicate(Set("n"), HasLabels(varFor("n"), Seq(LabelName("Person") _)) _)
    val n_prop: Property = Property(varFor("n"), PropertyKeyName("prop") _) _
    val ineqality = Predicate(Set("n"), AndedPropertyInequalities(varFor("n"), n_prop, NonEmptyList(
      GreaterThanOrEqual(n_prop, SignedDecimalIntegerLiteral("3") _) _,
      LessThan(n_prop, SignedDecimalIntegerLiteral("4") _) _
    )))

    val calculator = setUpCalculator(ineqality, Seq(n_is_Person))

    val labelResult = calculator(n_is_Person.expr)
    val ineqResult = calculator(ineqality.expr)

    labelResult.factor should equal(0.1)
    ineqResult.factor should equal(
      0.2 // Selectivity for .prop
        * 0.75 // Selectivity for != 3
        * GraphStatistics.DEFAULT_RANGE_SEEK_FACTOR / 2 // Selectivity for range
        + 0.2 // Selectivity for .prop
        * 0.25 // Selectivity for == 3
        +- 0.000001
    )
  }

  test("three inequalities should be equal to two inequalities, one label") {
    val n_is_Person = Predicate(Set("n"), HasLabels(varFor("n"), Seq(LabelName("Person") _)) _)
    val n_prop: Property = Property(varFor("n"), PropertyKeyName("prop") _) _
    val ineqality = Predicate(Set("n"), AndedPropertyInequalities(varFor("n"), n_prop, NonEmptyList(
      GreaterThan(n_prop, SignedDecimalIntegerLiteral("3") _) _,
      LessThan(n_prop, SignedDecimalIntegerLiteral("4") _) _,
      LessThan(n_prop, SignedDecimalIntegerLiteral("7") _) _
    )))

    val calculator = setUpCalculator(ineqality, Seq(n_is_Person))

    val labelResult = calculator(n_is_Person.expr)
    val ineqResult = calculator(ineqality.expr)

    labelResult.factor should equal(0.1)
    ineqResult.factor should equal(
      0.2 // Selectivity for .prop
        * 0.75 // Selectivity for != x
        * GraphStatistics.DEFAULT_RANGE_SEEK_FACTOR / 2 // Selectivity for range
        +- 0.000001
    )
  }

  test("half-open (>) range with one label, no index") {
    val n_is_Person = Predicate(Set("n"), HasLabels(varFor("n"), Seq(LabelName("Person") _)) _)
    val n_prop: Property = Property(varFor("n"), PropertyKeyName("prop") _) _
    val ineqality = Predicate(Set("n"), AndedPropertyInequalities(varFor("n"), n_prop, NonEmptyList(
      GreaterThan(n_prop, SignedDecimalIntegerLiteral("3") _) _
    )))

    val calculator = setUpCalculator(ineqality, Seq(n_is_Person), mockStats(indexCardinalities = Map.empty))

    val labelResult = calculator(n_is_Person.expr)
    val ineqResult = calculator(ineqality.expr)

    labelResult.factor should equal(0.1)
    ineqResult should equal(GraphStatistics.DEFAULT_RANGE_SELECTIVITY)
  }

  test("closed (> && <) range with one label, no index") {
    val n_is_Person = Predicate(Set("n"), HasLabels(varFor("n"), Seq(LabelName("Person") _)) _)
    val n_prop: Property = Property(varFor("n"), PropertyKeyName("prop") _) _
    val ineqality = Predicate(Set("n"), AndedPropertyInequalities(varFor("n"), n_prop, NonEmptyList(
      GreaterThan(n_prop, SignedDecimalIntegerLiteral("3") _) _,
      LessThan(n_prop, SignedDecimalIntegerLiteral("4") _) _
    )))

    val calculator = setUpCalculator(ineqality, Seq(n_is_Person), mockStats(indexCardinalities = Map.empty))

    val labelResult = calculator(n_is_Person.expr)
    val ineqResult = calculator(ineqality.expr)

    labelResult.factor should equal(0.1)
    ineqResult.factor should equal(GraphStatistics.DEFAULT_RANGE_SELECTIVITY.factor / 2)
  }

  test("half-open (>) range with two labels, one index") {
    val n_is_Person = Predicate(Set("n"), HasLabels(varFor("n"), Seq(LabelName("Person") _)) _)
    val n_is_Animal = Predicate(Set("n"), HasLabels(varFor("n"), Seq(LabelName("Animal") _)) _)
    val n_prop: Property = Property(varFor("n"), PropertyKeyName("prop") _) _
    val ineqality = Predicate(Set("n"), AndedPropertyInequalities(varFor("n"), n_prop, NonEmptyList(
      GreaterThan(n_prop, SignedDecimalIntegerLiteral("3") _) _
    )))

    val calculator = setUpCalculator(ineqality, Seq(n_is_Person, n_is_Animal),
      mockStats(labelCardinalities = Map(indexPerson.label -> 1000.0, indexAnimal.label -> 10.0)))

    val labelResult1 = calculator(n_is_Person.expr)
    val labelResult2 = calculator(n_is_Animal.expr)
    val ineqResult = calculator(ineqality.expr)

    labelResult1.factor should equal(0.1)
    labelResult2.factor should equal(0.001)
    ineqResult.factor should equal(
      0.2 // Selectivity for .prop
        * 0.75 // Selectivity for != x
        * GraphStatistics.DEFAULT_RANGE_SEEK_FACTOR // Selectivity for range
        +- 0.000001
    )
  }

  test("closed (> && <) range with two labels, one index") {
    val n_is_Person = Predicate(Set("n"), HasLabels(varFor("n"), Seq(LabelName("Person") _)) _)
    val n_is_Animal = Predicate(Set("n"), HasLabels(varFor("n"), Seq(LabelName("Animal") _)) _)
    val n_prop: Property = Property(varFor("n"), PropertyKeyName("prop") _) _
    val ineqality = Predicate(Set("n"), AndedPropertyInequalities(varFor("n"), n_prop, NonEmptyList(
      GreaterThan(n_prop, SignedDecimalIntegerLiteral("3") _) _,
      LessThan(n_prop, SignedDecimalIntegerLiteral("4") _) _
    )))

    val calculator = setUpCalculator(ineqality, Seq(n_is_Person),
      mockStats(labelCardinalities = Map(indexPerson.label -> 1000.0, indexAnimal.label -> 10.0)))

    val labelResult = calculator(n_is_Person.expr)
    val labelResult2 = calculator(n_is_Animal.expr)
    val ineqResult = calculator(ineqality.expr)

    labelResult.factor should equal(0.1)
    labelResult2.factor should equal(0.001)
    ineqResult.factor should equal(
      0.2 // Selectivity for .prop
        * 0.75 // Selectivity for != 3
        * GraphStatistics.DEFAULT_RANGE_SEEK_FACTOR / 2 // Selectivity for range
        +- 0.000001
    )
  }

  test("half-open (>) range with two labels, two indexes") {
    val n_is_Person = Predicate(Set("n"), HasLabels(varFor("n"), Seq(LabelName("Person") _)) _)
    val n_is_Animal = Predicate(Set("n"), HasLabels(varFor("n"), Seq(LabelName("Animal") _)) _)
    val n_prop: Property = Property(varFor("n"), PropertyKeyName("prop") _) _
    val ineqality = Predicate(Set("n"), AndedPropertyInequalities(varFor("n"), n_prop, NonEmptyList(
      GreaterThan(n_prop, SignedDecimalIntegerLiteral("3") _) _
    )))

    val calculator = setUpCalculator(ineqality, Seq(n_is_Person, n_is_Animal), mockStats(
      labelCardinalities = Map(indexPerson.label -> 1000, indexAnimal.label -> 800.0),
      indexCardinalities = Map(indexPerson -> 200.0, indexAnimal -> 400.0),
      indexUniqueCardinalities = Map(indexPerson -> 50.0, indexAnimal -> 40.0)))

    val labelResult1 = calculator(n_is_Person.expr)
    val labelResult2 = calculator(n_is_Animal.expr)
    val ineqResult = calculator(ineqality.expr)

    labelResult1.factor should equal(0.1)
    labelResult2.factor should equal(0.08)

    val personIndexSelectivity = (
      0.2 // Selectivity for .prop
        * 0.75 // Selectivity for != x
        * GraphStatistics.DEFAULT_RANGE_SEEK_FACTOR // Selectivity for range
      )
    val animalIndexSelectivity = (
      0.5 // Selectivity for .prop
        * 0.9 // Selectivity for != x
        * GraphStatistics.DEFAULT_RANGE_SEEK_FACTOR // Selectivity for range
      )

    ineqResult.factor should equal(personIndexSelectivity + animalIndexSelectivity - personIndexSelectivity * animalIndexSelectivity +- 0.000001)
  }

  test("half-open (>=) range with two labels, two indexes") {
    val n_is_Person = Predicate(Set("n"), HasLabels(varFor("n"), Seq(LabelName("Person") _)) _)
    val n_is_Animal = Predicate(Set("n"), HasLabels(varFor("n"), Seq(LabelName("Animal") _)) _)
    val n_prop: Property = Property(varFor("n"), PropertyKeyName("prop") _) _
    val ineqality = Predicate(Set("n"), AndedPropertyInequalities(varFor("n"), n_prop, NonEmptyList(
      GreaterThanOrEqual(n_prop, SignedDecimalIntegerLiteral("3") _) _
    )))

    val calculator = setUpCalculator(ineqality, Seq(n_is_Person, n_is_Animal), mockStats(
      labelCardinalities = Map(indexPerson.label -> 1000, indexAnimal.label -> 800.0),
      indexCardinalities = Map(indexPerson -> 200.0, indexAnimal -> 400.0),
      indexUniqueCardinalities = Map(indexPerson -> 50.0, indexAnimal -> 40.0)))

    val labelResult1 = calculator(n_is_Person.expr)
    val labelResult2 = calculator(n_is_Animal.expr)
    val ineqResult = calculator(ineqality.expr)

    labelResult1.factor should equal(0.1)
    labelResult2.factor should equal(0.08)

    val personIndexSelectivity = (
      0.2 // Selectivity for .prop
        * 0.75 // Selectivity for != 3
        * GraphStatistics.DEFAULT_RANGE_SEEK_FACTOR // Selectivity for range
        + 0.2 // Selectivity for .prop
        * 0.25 // Selectivity for == 3
      )
    val animalIndexSelectivity = (
      0.5 // Selectivity for .prop
        * 0.9 // Selectivity for != x
        * GraphStatistics.DEFAULT_RANGE_SEEK_FACTOR // Selectivity for range
        + 0.5 // Selectivity for .prop
        * 0.1 // Selectivity for == 3
      )

    ineqResult.factor should equal(personIndexSelectivity + animalIndexSelectivity - personIndexSelectivity * animalIndexSelectivity +- 0.000001)
  }

  test("closed (> && <) range with two labels, two indexes") {
    val n_is_Person = Predicate(Set("n"), HasLabels(varFor("n"), Seq(LabelName("Person") _)) _)
    val n_is_Animal = Predicate(Set("n"), HasLabels(varFor("n"), Seq(LabelName("Animal") _)) _)
    val n_prop: Property = Property(varFor("n"), PropertyKeyName("prop") _) _
    val ineqality = Predicate(Set("n"), AndedPropertyInequalities(varFor("n"), n_prop, NonEmptyList(
      GreaterThan(n_prop, SignedDecimalIntegerLiteral("3") _) _,
      LessThan(n_prop, SignedDecimalIntegerLiteral("4") _) _
    )))

    val calculator = setUpCalculator(ineqality, Seq(n_is_Person, n_is_Animal), mockStats(
      labelCardinalities = Map(indexPerson.label -> 1000, indexAnimal.label -> 800.0),
      indexCardinalities = Map(indexPerson -> 200.0, indexAnimal -> 400.0),
      indexUniqueCardinalities = Map(indexPerson -> 50.0, indexAnimal -> 40.0)))

    val labelResult1 = calculator(n_is_Person.expr)
    val labelResult2 = calculator(n_is_Animal.expr)
    val ineqResult = calculator(ineqality.expr)

    labelResult1.factor should equal(0.1)
    labelResult2.factor should equal(0.08)

    val personIndexSelectivity = (
      0.2 // Selectivity for .prop
        * 0.75 // Selectivity for != x
        * GraphStatistics.DEFAULT_RANGE_SEEK_FACTOR / 2 // Selectivity for range
      )
    val animalIndexSelectivity = (
      0.5 // Selectivity for .prop
        * 0.9 // Selectivity for != x
        * GraphStatistics.DEFAULT_RANGE_SEEK_FACTOR / 2 // Selectivity for range
      )

    ineqResult.factor should equal(personIndexSelectivity + animalIndexSelectivity - personIndexSelectivity * animalIndexSelectivity +- 0.000001)
  }

  test("closed (>= && <) range with two labels, two indexes") {
    val n_is_Person = Predicate(Set("n"), HasLabels(varFor("n"), Seq(LabelName("Person") _)) _)
    val n_is_Animal = Predicate(Set("n"), HasLabels(varFor("n"), Seq(LabelName("Animal") _)) _)
    val n_prop: Property = Property(varFor("n"), PropertyKeyName("prop") _) _
    val ineqality = Predicate(Set("n"), AndedPropertyInequalities(varFor("n"), n_prop, NonEmptyList(
      GreaterThanOrEqual(n_prop, SignedDecimalIntegerLiteral("3") _) _,
      LessThan(n_prop, SignedDecimalIntegerLiteral("4") _) _
    )))

    val calculator = setUpCalculator(ineqality, Seq(n_is_Person, n_is_Animal), mockStats(
      labelCardinalities = Map(indexPerson.label -> 1000, indexAnimal.label -> 800.0),
      indexCardinalities = Map(indexPerson -> 200.0, indexAnimal -> 400.0),
      indexUniqueCardinalities = Map(indexPerson -> 50.0, indexAnimal -> 40.0)))

    val labelResult1 = calculator(n_is_Person.expr)
    val labelResult2 = calculator(n_is_Animal.expr)
    val ineqResult = calculator(ineqality.expr)

    labelResult1.factor should equal(0.1)
    labelResult2.factor should equal(0.08)

    val personIndexSelectivity = (
      0.2 // Selectivity for .prop
        * 0.75 // Selectivity for != 3
        * GraphStatistics.DEFAULT_RANGE_SEEK_FACTOR / 2 // Selectivity for range
        + 0.2 // Selectivity for .prop
        * 0.25 // Selectivity for == 3
      )
    val animalIndexSelectivity = (
      0.5 // Selectivity for .prop
        * 0.9 // Selectivity for != 3
        * GraphStatistics.DEFAULT_RANGE_SEEK_FACTOR / 2 // Selectivity for range
        + 0.5 // Selectivity for .prop
        * 0.1 // Selectivity for == 3
      )

    ineqResult.factor should equal(personIndexSelectivity + animalIndexSelectivity - personIndexSelectivity * animalIndexSelectivity +- 0.000001)
  }

  // POINT DISTANCE

  private val fakePoint = True()(pos)

  test("distance with no label") {
    val n_prop: Property = Property(varFor("n"), PropertyKeyName("prop") _) _
    val distance = Predicate(Set("n"), LessThan(FunctionInvocation(n_prop, FunctionName(Distance.name)(pos), fakePoint), SignedDecimalIntegerLiteral("3")(pos))(pos))

    val calculator = setUpCalculator(distance, Seq.empty)
    val distanceResult = calculator(distance.expr)
    distanceResult should equal(GraphStatistics.DEFAULT_RANGE_SELECTIVITY)
  }

  test("distance with one label") {
    val n_is_Person = Predicate(Set("n"), HasLabels(varFor("n"), Seq(LabelName("Person") _)) _)
    val n_prop: Property = Property(varFor("n"), PropertyKeyName("prop") _) _
    val distance = Predicate(Set("n"), LessThan(FunctionInvocation(n_prop, FunctionName(Distance.name)(pos), fakePoint), SignedDecimalIntegerLiteral("3")(pos))(pos))

    val calculator = setUpCalculator(distance, Seq(n_is_Person))

    val labelResult = calculator(n_is_Person.expr)
    val distanceResult = calculator(distance.expr)

    labelResult.factor should equal(0.1)
    distanceResult.factor should equal(
      0.2 // exists n.prop
        * GraphStatistics.DEFAULT_RANGE_SEEK_FACTOR // point distance
    )
  }

  test("distance with two labels, two indexes") {
    val n_is_Person = Predicate(Set("n"), HasLabels(varFor("n"), Seq(LabelName("Person") _)) _)
    val n_is_Animal = Predicate(Set("n"), HasLabels(varFor("n"), Seq(LabelName("Animal") _)) _)
    val n_prop: Property = Property(varFor("n"), PropertyKeyName("prop") _) _
    val distance = Predicate(Set("n"), LessThan(FunctionInvocation(n_prop, FunctionName(Distance.name)(pos), fakePoint), SignedDecimalIntegerLiteral("3")(pos))(pos))

    val calculator = setUpCalculator(distance, Seq(n_is_Person, n_is_Animal), mockStats(
      labelCardinalities = Map(indexPerson.label -> 1000, indexAnimal.label -> 800.0),
      indexCardinalities = Map(indexPerson -> 200.0, indexAnimal -> 400.0)))

    val labelResult1 = calculator(n_is_Person.expr)
    val labelResult2 = calculator(n_is_Animal.expr)
    val distanceResult = calculator(distance.expr)

    labelResult1.factor should equal(0.1)
    labelResult2.factor should equal(0.08)

    val personIndexSelectivity = (
      0.2 // Selectivity for .prop
        * GraphStatistics.DEFAULT_RANGE_SEEK_FACTOR // point distance
      )
    val animalIndexSelectivity = (
      0.5 // Selectivity for .prop
        * GraphStatistics.DEFAULT_RANGE_SEEK_FACTOR // point distance
      )

    distanceResult.factor should equal(personIndexSelectivity + animalIndexSelectivity - personIndexSelectivity * animalIndexSelectivity +- 0.001)
  }

  // EXISTS

  test("exists with no label") {
    val n_prop: Property = Property(varFor("n"), PropertyKeyName("prop") _) _
    val exists = Predicate(Set("n"), FunctionInvocation(FunctionName(Exists.name)(pos), n_prop)(pos))

    val calculator = setUpCalculator(exists, Seq.empty)
    val existsResult = calculator(exists.expr)
    existsResult should equal(GraphStatistics.DEFAULT_PROPERTY_SELECTIVITY)
  }

  test("exists with one label") {
    val n_is_Person = Predicate(Set("n"), HasLabels(varFor("n"), Seq(LabelName("Person") _)) _)
    val n_prop: Property = Property(varFor("n"), PropertyKeyName("prop") _) _
    val exists = Predicate(Set("n"), FunctionInvocation(FunctionName(Exists.name)(pos), n_prop)(pos))

    val calculator = setUpCalculator(exists, Seq(n_is_Person))

    val labelResult = calculator(n_is_Person.expr)
    val existsResult = calculator(exists.expr)

    labelResult.factor should equal(0.1)
    existsResult.factor should equal(0.2)
  }

  test("exists with one label, no index") {
    val n_is_Person = Predicate(Set("n"), HasLabels(varFor("n"), Seq(LabelName("Person") _)) _)
    val n_prop: Property = Property(varFor("n"), PropertyKeyName("prop") _) _
    val exists = Predicate(Set("n"), FunctionInvocation(FunctionName(Exists.name)(pos), n_prop)(pos))

    val calculator = setUpCalculator(exists, Seq(n_is_Person), mockStats(indexCardinalities = Map.empty))

    val labelResult = calculator(n_is_Person.expr)
    val existsResult = calculator(exists.expr)

    labelResult.factor should equal(0.1)
    existsResult should equal(GraphStatistics.DEFAULT_PROPERTY_SELECTIVITY)
  }

  test("exists with two labels, one index") {
    val n_is_Person = Predicate(Set("n"), HasLabels(varFor("n"), Seq(LabelName("Person") _)) _)
    val n_is_Animal = Predicate(Set("n"), HasLabels(varFor("n"), Seq(LabelName("Animal") _)) _)
    val n_prop: Property = Property(varFor("n"), PropertyKeyName("prop") _) _
    val exists = Predicate(Set("n"), FunctionInvocation(FunctionName(Exists.name)(pos), n_prop)(pos))

    val calculator = setUpCalculator(exists, Seq(n_is_Person, n_is_Animal),
      mockStats(labelCardinalities = Map(indexPerson.label -> 1000.0, indexAnimal.label -> 10.0)))

    val labelResult1 = calculator(n_is_Person.expr)
    val labelResult2 = calculator(n_is_Animal.expr)
    val existsResult = calculator(exists.expr)

    labelResult1.factor should equal(0.1)
    labelResult2.factor should equal(0.001)
    existsResult.factor should equal(0.2)
  }

  test("exists with two labels, two indexes") {
    val n_is_Person = Predicate(Set("n"), HasLabels(varFor("n"), Seq(LabelName("Person") _)) _)
    val n_is_Animal = Predicate(Set("n"), HasLabels(varFor("n"), Seq(LabelName("Animal") _)) _)
    val n_prop: Property = Property(varFor("n"), PropertyKeyName("prop") _) _
    val exists = Predicate(Set("n"), FunctionInvocation(FunctionName(Exists.name)(pos), n_prop)(pos))

    val calculator = setUpCalculator(exists, Seq(n_is_Person, n_is_Animal), mockStats(
      labelCardinalities = Map(indexPerson.label -> 1000, indexAnimal.label -> 800.0),
      indexCardinalities = Map(indexPerson -> 200.0, indexAnimal -> 400.0)))

    val labelResult1 = calculator(n_is_Person.expr)
    val labelResult2 = calculator(n_is_Animal.expr)
    val existsResult = calculator(exists.expr)

    labelResult1.factor should equal(0.1)
    labelResult2.factor should equal(0.08)
    existsResult.factor should equal(0.2 + 0.5 - 0.2 * 0.5 +- 0.001)
  }

  // EQUALITY / IN

  test("equality with no label, size 0") {
    val n_prop: Property = Property(varFor("n"), PropertyKeyName("prop") _) _
    val equals = Predicate(Set("n"), In(n_prop, listOf()) _)

    val calculator = setUpCalculator(equals, Seq.empty)
    val eqResult = calculator(equals.expr)
    eqResult.factor should equal(0.0)
  }

  test("equality with no label, size 1") {
    val n_prop: Property = Property(varFor("n"), PropertyKeyName("prop") _) _
    val equals = Predicate(Set("n"), Equals(n_prop, SignedDecimalIntegerLiteral("3") _) _)

    val calculator = setUpCalculator(equals, Seq.empty)
    val eqResult = calculator(equals.expr)
    eqResult should equal(GraphStatistics.DEFAULT_EQUALITY_SELECTIVITY)
  }

  test("equality with no label, size 2") {
    val n_prop: Property = Property(varFor("n"), PropertyKeyName("prop") _) _
    val equals = Predicate(Set("n"), In(n_prop, listOf(SignedDecimalIntegerLiteral("3") _, SignedDecimalIntegerLiteral("4") _)) _)

    val calculator = setUpCalculator(equals, Seq.empty)
    val eqResult = calculator(equals.expr)
    val resFor1 = GraphStatistics.DEFAULT_EQUALITY_SELECTIVITY.factor
    eqResult.factor should equal(resFor1 + resFor1 - resFor1 * resFor1)
  }

  test("equality with no label, size unknown") {
    val n_prop: Property = Property(varFor("n"), PropertyKeyName("prop") _) _
    val equals = Predicate(Set("n"), In(n_prop, varFor("someList")) _)

    val calculator = setUpCalculator(equals, Seq.empty)
    val eqResult = calculator(equals.expr)
    val resFor1 = GraphStatistics.DEFAULT_EQUALITY_SELECTIVITY
    eqResult should equal(IndependenceCombiner.orTogetherSelectivities(for (_ <- 1 to GraphStatistics.DEFAULT_LIST_CARDINALITY.amount.toInt) yield resFor1).get)
  }

  test("equality with one label, size 0") {
    val n_is_Person = Predicate(Set("n"), HasLabels(varFor("n"), Seq(LabelName("Person") _)) _)
    val n_prop: Property = Property(varFor("n"), PropertyKeyName("prop") _) _
    val equals = Predicate(Set("n"), In(n_prop, listOf()) _)

    val calculator = setUpCalculator(equals, Seq(n_is_Person))
    val labelResult = calculator(n_is_Person.expr)
    val eqResult = calculator(equals.expr)

    labelResult.factor should equal(0.1)
    eqResult.factor should equal(0.0)
  }

  test("equality with one label, size 1") {
    val n_is_Person = Predicate(Set("n"), HasLabels(varFor("n"), Seq(LabelName("Person") _)) _)
    val n_prop: Property = Property(varFor("n"), PropertyKeyName("prop") _) _
    val equals = Predicate(Set("n"), Equals(n_prop, SignedDecimalIntegerLiteral("3") _) _)

    val calculator = setUpCalculator(equals, Seq(n_is_Person))

    val labelResult = calculator(n_is_Person.expr)
    val eqResult = calculator(equals.expr)

    labelResult.factor should equal(0.1)
    eqResult.factor should equal(0.05)
  }

  test("equality with one label, size 2") {
    val n_is_Person = Predicate(Set("n"), HasLabels(varFor("n"), Seq(LabelName("Person") _)) _)
    val n_prop: Property = Property(varFor("n"), PropertyKeyName("prop") _) _
    val equals = Predicate(Set("n"), In(n_prop, listOf(SignedDecimalIntegerLiteral("3") _, SignedDecimalIntegerLiteral("4") _)) _)

    val calculator = setUpCalculator(equals, Seq(n_is_Person))

    val labelResult = calculator(n_is_Person.expr)
    val eqResult = calculator(equals.expr)

    labelResult.factor should equal(0.1)
    val resFor1 = 0.05
    eqResult.factor should equal(resFor1 + resFor1 - resFor1 * resFor1)
  }

  test("equality with one label, size unknown") {
    val n_is_Person = Predicate(Set("n"), HasLabels(varFor("n"), Seq(LabelName("Person") _)) _)
    val n_prop: Property = Property(varFor("n"), PropertyKeyName("prop") _) _
    val equals = Predicate(Set("n"), In(n_prop, varFor("someList")) _)

    val calculator = setUpCalculator(equals, Seq(n_is_Person))

    val labelResult = calculator(n_is_Person.expr)
    val eqResult = calculator(equals.expr)

    labelResult.factor should equal(0.1)
    val resFor1 = Selectivity(0.05)
    eqResult should equal(IndependenceCombiner.orTogetherSelectivities(for (_ <- 1 to GraphStatistics.DEFAULT_LIST_CARDINALITY.amount.toInt) yield resFor1).get)
  }

  test("equality with two labels, size 0") {
    val n_is_Person = Predicate(Set("n"), HasLabels(varFor("n"), Seq(LabelName("Person") _)) _)
    val n_is_Animal = Predicate(Set("n"), HasLabels(varFor("n"), Seq(LabelName("Animal") _)) _)
    val n_prop: Property = Property(varFor("n"), PropertyKeyName("prop") _) _
    val equals = Predicate(Set("n"), In(n_prop, listOf()) _)

    val calculator = setUpCalculator(equals, Seq(n_is_Person, n_is_Animal), mockStats(
      labelCardinalities = Map(indexPerson.label -> 1000, indexAnimal.label -> 800.0),
      indexUniqueCardinalities = Map(indexPerson -> 200.0, indexAnimal -> 400.0)))

    val labelResult1 = calculator(n_is_Person.expr)
    val labelResult2 = calculator(n_is_Animal.expr)
    val eqResult = calculator(equals.expr)

    labelResult1.factor should equal(0.1)
    labelResult2.factor should equal(0.08)
    eqResult.factor should equal(0.0)
  }

  test("equality with two labels, size 1") {
    val n_is_Person = Predicate(Set("n"), HasLabels(varFor("n"), Seq(LabelName("Person") _)) _)
    val n_is_Animal = Predicate(Set("n"), HasLabels(varFor("n"), Seq(LabelName("Animal") _)) _)
    val n_prop: Property = Property(varFor("n"), PropertyKeyName("prop") _) _
    val equals = Predicate(Set("n"), Equals(n_prop, SignedDecimalIntegerLiteral("3") _) _)

    val calculator = setUpCalculator(equals, Seq(n_is_Person, n_is_Animal), mockStats(
      labelCardinalities = Map(indexPerson.label -> 1000, indexAnimal.label -> 800.0),
      indexCardinalities = Map(indexPerson -> 300.0, indexAnimal -> 500.0),
      indexUniqueCardinalities = Map(indexPerson -> 200.0, indexAnimal -> 400.0)))

    val labelResult1 = calculator(n_is_Person.expr)
    val labelResult2 = calculator(n_is_Animal.expr)
    val eqResult = calculator(equals.expr)

    labelResult1.factor should equal(0.1)
    labelResult2.factor should equal(0.08)
    eqResult.factor should equal(0.2 + 0.5 - 0.2 * 0.5)
  }

  test("equality with two labels, size 2") {
    val n_is_Person = Predicate(Set("n"), HasLabels(varFor("n"), Seq(LabelName("Person") _)) _)
    val n_is_Animal = Predicate(Set("n"), HasLabels(varFor("n"), Seq(LabelName("Animal") _)) _)
    val n_prop: Property = Property(varFor("n"), PropertyKeyName("prop") _) _
    val equals = Predicate(Set("n"), In(n_prop, listOf(SignedDecimalIntegerLiteral("3") _, SignedDecimalIntegerLiteral("4") _)) _)

    val calculator = setUpCalculator(equals, Seq(n_is_Person, n_is_Animal), mockStats(
      labelCardinalities = Map(indexPerson.label -> 1000, indexAnimal.label -> 800.0),
      indexCardinalities = Map(indexPerson -> 300.0, indexAnimal -> 500.0),
      indexUniqueCardinalities = Map(indexPerson -> 200.0, indexAnimal -> 400.0)))

    val labelResult1 = calculator(n_is_Person.expr)
    val labelResult2 = calculator(n_is_Animal.expr)
    val eqResult = calculator(equals.expr)

    labelResult1.factor should equal(0.1)
    labelResult2.factor should equal(0.08)
    val resFor1 = 0.2 + 0.5 - 0.2 * 0.5
    eqResult.factor should equal(resFor1 + resFor1 - resFor1 * resFor1)
  }

  test("equality with two labels, size unknown") {
    val n_is_Person = Predicate(Set("n"), HasLabels(varFor("n"), Seq(LabelName("Person") _)) _)
    val n_is_Animal = Predicate(Set("n"), HasLabels(varFor("n"), Seq(LabelName("Animal") _)) _)
    val n_prop: Property = Property(varFor("n"), PropertyKeyName("prop") _) _
    val equals = Predicate(Set("n"), In(n_prop, varFor("someList")) _)

    val calculator = setUpCalculator(equals, Seq(n_is_Person, n_is_Animal), mockStats(
      labelCardinalities = Map(indexPerson.label -> 1000, indexAnimal.label -> 800.0),
      indexCardinalities = Map(indexPerson -> 300.0, indexAnimal -> 500.0),
      indexUniqueCardinalities = Map(indexPerson -> 200.0, indexAnimal -> 400.0)))

    val labelResult1 = calculator(n_is_Person.expr)
    val labelResult2 = calculator(n_is_Animal.expr)
    val eqResult = calculator(equals.expr)

    labelResult1.factor should equal(0.1)
    labelResult2.factor should equal(0.08)
    val resFor1 = Selectivity(0.2 + 0.5 - 0.2 * 0.5)
    eqResult should equal(IndependenceCombiner.orTogetherSelectivities(for (_ <- 1 to GraphStatistics.DEFAULT_LIST_CARDINALITY.amount.toInt) yield resFor1).get)
  }

  // OTHER

  test("Should consider parameter expressions when calculating index selectivity") {
    implicit val semanticTable = SemanticTable()
    semanticTable.resolvedLabelNames.put("Page", indexPerson.label)
    semanticTable.resolvedPropertyKeyNames.put("title", indexPerson.property)

    implicit val selections = Selections(Set(Predicate(Set("n"), HasLabels(varFor("n"), Seq(LabelName("Page") _)) _)))

    val stats = mock[GraphStatistics]
    when(stats.nodesAllCardinality()).thenReturn(1000.0)
    when(stats.indexPropertyExistsSelectivity(indexPerson)).thenReturn(Some(Selectivity.of(0.1).get))
    when(stats.uniqueValueSelectivity(indexPerson)).thenReturn(Some(Selectivity.of(1.0).get))

    val calculator = ExpressionSelectivityCalculator(stats, IndependenceCombiner)

    val result = calculator(In(Property(varFor("n"), PropertyKeyName("title") _) _, Parameter("titles", CTAny) _) _)

    result.factor should equal(0.92 +- 0.01)
  }

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

  test("Should optimize selectivity with respect to prefix length for STARTS WITH predicates") {
    implicit val semanticTable = SemanticTable()
    semanticTable.resolvedLabelNames.put("A", indexPerson.label)
    semanticTable.resolvedPropertyKeyNames.put("prop", indexPerson.property)

    implicit val selections = mock[Selections]
    val label = LabelName("A")(InputPosition.NONE)
    val propKey = PropertyKeyName("prop")(InputPosition.NONE)
    when(selections.labelsOnNode("a")).thenReturn(Set(label))

    val stats = mock[GraphStatistics]
    when(stats.uniqueValueSelectivity(indexPerson)).thenReturn(Some(Selectivity.of(0.01).get))
    when(stats.indexPropertyExistsSelectivity(indexPerson)).thenReturn(Some(Selectivity.ONE))
    val calculator = ExpressionSelectivityCalculator(stats, IndependenceCombiner)

    val prefixes = Map("p" -> 0.23384596099184043,
      "p2" -> 0.2299568541948447,
      "p33" -> 0.22801230079634685,
      "p5555" -> 0.22606774739784896,
      "reallylong" -> 0.22429997158103274)

    prefixes.foreach { case (prefix, selectivity) =>
      val actual = calculator(StartsWith(Property(Variable("a") _, propKey) _, StringLiteral(prefix)(InputPosition.NONE)) _)
      actual.factor should equal(selectivity +- selectivity * 0.000000000000001)
    }
  }

  //  test("Selectivity should never be worse than corresponding existence selectivity") {
  //    implicit val semanticTable = SemanticTable()
  //    semanticTable.resolvedLabelNames.put("A", indexPerson.label)
  //    semanticTable.resolvedPropertyKeyNames.put("prop", indexPerson.property)
  //
  //    implicit val selections = mock[Selections]
  //    val label = LabelName("A")(InputPosition.NONE)
  //    val propKey = PropertyKeyName("prop")(InputPosition.NONE)
  //    when(selections.labelsOnNode("a")).thenReturn(Set(label))
  //
  //    val stats = mock[GraphStatistics]
  //    //when(stats.uniqueValueSelectivity(indexPerson)).thenReturn(Some(Selectivity.of(0.01).get))
  //    val existenceSelectivity = .2285
  //    when(stats.indexPropertyExistsSelectivity(indexPerson)).thenReturn(Some(Selectivity.of(existenceSelectivity).get))
  //    val calculator = ExpressionSelectivityCalculator(stats, IndependenceCombiner)
  //
  //    val prefixes = Map("p" -> existenceSelectivity,
  //      "p2" -> existenceSelectivity,
  //      "p33" -> 0.22801230079634685,
  //      "p5555" -> 0.22606774739784896,
  //      "reallylong" -> 0.22429997158103274)
  //
  //    prefixes.foreach { case (prefix, selectivity) =>
  //      val actual = calculator(StartsWith(Property(Variable("a") _, propKey) _, StringLiteral(prefix)(InputPosition.NONE)) _)
  //      actual.factor should equal(selectivity +- selectivity * 0.000000000000001)
  //    }
  //  }

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

  private def setUpCalculator(inequality: Predicate, hasLabels: Seq[Predicate], stats: GraphStatistics = mockStats()): Expression => Selectivity = {
    implicit val semanticTable = SemanticTable()
    semanticTable.resolvedLabelNames.put("Person", indexPerson.label)
    semanticTable.resolvedLabelNames.put("Animal", indexAnimal.label)
    semanticTable.resolvedPropertyKeyNames.put("prop", indexPerson.property)

    implicit val selections = Selections(Set(inequality) ++ hasLabels)
    val combiner = IndependenceCombiner
    val calculator = ExpressionSelectivityCalculator(stats, combiner)
    exp: Expression => calculator(exp)
  }

  /**
    * @param allNodesCardinality      total number of ndes
    * @param labelCardinalities       for each label, the number of nodes that have that label
    * @param indexCardinalities       for each index, the number of values in that index
    * @param indexUniqueCardinalities for each index, the number of unique values in that index
    */
  private def mockStats(allNodesCardinality: Double = 10000.0,
                        labelCardinalities: Map[LabelId, Double] = Map(indexPerson.label -> 1000.0),
                        indexCardinalities: Map[IndexDescriptor, Double] = Map(indexPerson -> 200.0),
                        indexUniqueCardinalities: Map[IndexDescriptor, Double] = Map(indexPerson -> 50.0)): GraphStatistics = {
    val stats = mock[GraphStatistics]
    when(stats.nodesAllCardinality()).thenReturn(10000.0)
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
          indexCardinality <- indexCardinalities.get(theIndex)
        } yield Selectivity(indexUniqueCardinality / indexCardinality)
      }
    })

    stats
  }
}
