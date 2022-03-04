/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.compiler.planner.logical.cardinality

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.LabelInfo
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.RelTypeInfo
import org.neo4j.cypher.internal.compiler.planner.logical.PlannerDefaults.DEFAULT_EQUALITY_SELECTIVITY
import org.neo4j.cypher.internal.compiler.planner.logical.PlannerDefaults.DEFAULT_LIST_CARDINALITY
import org.neo4j.cypher.internal.compiler.planner.logical.PlannerDefaults.DEFAULT_PROPERTY_SELECTIVITY
import org.neo4j.cypher.internal.compiler.planner.logical.PlannerDefaults.DEFAULT_RANGE_SEEK_FACTOR
import org.neo4j.cypher.internal.compiler.planner.logical.PlannerDefaults.DEFAULT_RANGE_SELECTIVITY
import org.neo4j.cypher.internal.compiler.planner.logical.PlannerDefaults.DEFAULT_STRING_LENGTH
import org.neo4j.cypher.internal.compiler.planner.logical.PlannerDefaults.DEFAULT_TYPE_SELECTIVITY
import org.neo4j.cypher.internal.compiler.planner.logical.cardinality.ExpressionSelectivityCalculatorTest.IndexDescriptorHelper
import org.neo4j.cypher.internal.expressions.AndedPropertyInequalities
import org.neo4j.cypher.internal.expressions.AutoExtractedParameter
import org.neo4j.cypher.internal.expressions.BooleanExpression
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.HasLabels
import org.neo4j.cypher.internal.expressions.InequalityExpression
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.ListOfLiteralWriter
import org.neo4j.cypher.internal.expressions.PartialPredicate
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.ir.Predicate
import org.neo4j.cypher.internal.ir.Selections
import org.neo4j.cypher.internal.planner.spi.GraphStatistics
import org.neo4j.cypher.internal.planner.spi.IndexDescriptor
import org.neo4j.cypher.internal.planner.spi.IndexDescriptor.IndexType
import org.neo4j.cypher.internal.planner.spi.MinimumGraphStatistics.MIN_NODES_ALL_CARDINALITY
import org.neo4j.cypher.internal.planner.spi.MinimumGraphStatistics.MIN_NODES_WITH_LABEL_CARDINALITY
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.LabelId
import org.neo4j.cypher.internal.util.ListSizeBucket
import org.neo4j.cypher.internal.util.NameId
import org.neo4j.cypher.internal.util.NonEmptyList
import org.neo4j.cypher.internal.util.PropertyKeyId
import org.neo4j.cypher.internal.util.RelTypeId
import org.neo4j.cypher.internal.util.Selectivity
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.CTList
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

abstract class ExpressionSelectivityCalculatorTest extends CypherFunSuite with AstConstructionTestSupport {

  def getIndexType: IndexDescriptor.IndexType

  // NODES

  protected val indexPersonRange: IndexDescriptor = IndexDescriptor.forLabel(getIndexType, LabelId(0), Seq(PropertyKeyId(0)))
  protected val indexPersonText: IndexDescriptor = indexPersonRange.copy(indexType = IndexDescriptor.IndexType.Text)
  protected val indexPersonPoint: IndexDescriptor = indexPersonRange.copy(indexType = IndexDescriptor.IndexType.Point)

  protected val indexAnimal: IndexDescriptor = IndexDescriptor.forLabel(getIndexType, LabelId(1), Seq(PropertyKeyId(0)))

  protected val nProp: Property = prop("n", "nodeProp")

  protected val nIsPerson: Predicate = nPredicate(HasLabels(varFor("n"), Seq(labelName("Person"))) _)
  protected val nIsAnimal: Predicate = nPredicate(HasLabels(varFor("n"), Seq(labelName("Animal"))) _)

  protected val nIsPersonLabelInfo: Map[String, Set[LabelName]] = Map("n" -> Set(labelName("Person")))
  protected val nIsPersonAndAnimalLabelInfo: Map[String, Set[LabelName]] = Map("n" -> Set(labelName("Person"), labelName("Animal")))

  protected val personPropIsNotNullSel: Double = 0.2
  protected val personTextPropIsNotNullSel: Double = 0.1
  protected val personPointPropIsNotNullSel: Double = 0.1
  protected val indexPersonUniqueSel: Double= 1.0 / 180.0
  protected val animalPropIsNotNullSel: Double = 0.5

  // RELATIONSHIPS

  protected val indexFriends: IndexDescriptor = IndexDescriptor.forRelType(getIndexType, RelTypeId(0), Seq(PropertyKeyId(0)))

  protected val rProp: Property = prop("r", "relProp")

  protected val rFriendsRelTypeInfo: Map[String, RelTypeName] = Map("r" -> relTypeName("Friends"))

  protected val friendsPropIsNotNullSel: Double = 0.2
  protected val indexFriendsUniqueSel: Double = 1.0 / 180.0

  // RANGE SEEK

  test("half-open (>) range with no label") {
    val inequality = nPredicate(nAnded(NonEmptyList(
      greaterThan(nProp, literalInt(3))
    )))

    val calculator = setUpCalculator()
    val inequalityResult = calculator(inequality.expr)
    inequalityResult should equal(DEFAULT_RANGE_SELECTIVITY)
  }

  test("half-open (>=) range with no label") {
    val inequality = nPredicate(nAnded(NonEmptyList(
      greaterThanOrEqual(nProp, literalInt(3))
    )))

    val calculator = setUpCalculator()
    val inequalityResult = calculator(inequality.expr)
    inequalityResult.factor should equal(DEFAULT_RANGE_SELECTIVITY.factor + DEFAULT_EQUALITY_SELECTIVITY.factor)
  }

  test("closed (> && <) range with no label") {
    val inequality = nPredicate(nAnded(NonEmptyList(
      greaterThan(nProp, literalInt(3)),
      lessThan(nProp, literalInt(4))
    )))

    val calculator = setUpCalculator()
    val inequalityResult = calculator(inequality.expr)
    inequalityResult.factor should equal(DEFAULT_RANGE_SELECTIVITY.factor / 2)
  }

  test("closed (>= && <) range with no label") {
    val inequality = nPredicate(nAnded(NonEmptyList(
      greaterThanOrEqual(nProp, literalInt(3)),
      lessThan(nProp, literalInt(4))
    )))

    val calculator = setUpCalculator()
    val inequalityResult = calculator(inequality.expr)
    inequalityResult.factor should equal(DEFAULT_RANGE_SELECTIVITY.factor / 2 + DEFAULT_EQUALITY_SELECTIVITY.factor)
  }

  test("three inequalities should be equal to two inequalities, no labels") {
    val inequality = nPredicate(nAnded(NonEmptyList(
      greaterThan(nProp, literalInt(3)),
      lessThan(nProp, literalInt(4)),
      lessThan(nProp, literalInt(7))
    )))

    val calculator = setUpCalculator()
    val inequalityResult = calculator(inequality.expr)
    inequalityResult.factor should equal(DEFAULT_RANGE_SELECTIVITY.factor / 2)
  }

  test("half-open (>) range with one label") {
    val inequality = nPredicate(nAnded(NonEmptyList(
      greaterThan(nProp, literalInt(3))
    )))

    val calculator = setUpCalculator(labelInfo = nIsPersonLabelInfo)

    val labelResult = calculator(nIsPerson.expr)
    val inequalityResult = calculator(inequality.expr)

    labelResult.factor should equal(0.1)
    inequalityResult.factor should equal(
      personPropIsNotNullSel
        * (1-indexPersonUniqueSel) // Selectivity for != x
        * DEFAULT_RANGE_SEEK_FACTOR // Selectivity for range
        +- 0.00000001
    )
  }

  test("half-open (>) range with one relType") {
    val inequality = rPredicate(rAnded(NonEmptyList(
      greaterThan(rProp, literalInt(3))
    )))

    val calculator = setUpCalculator(relTypeInfo = rFriendsRelTypeInfo)

    val inequalityResult = calculator(inequality.expr)

    inequalityResult.factor should equal(
      friendsPropIsNotNullSel
        * (1-indexFriendsUniqueSel) // Selectivity for != x
        * DEFAULT_RANGE_SEEK_FACTOR // Selectivity for range
        +- 0.00000001
    )
  }

  test("half-open (>=) range with one label") {
    val inequality = nPredicate(nAnded(NonEmptyList(
      greaterThanOrEqual(nProp, literalInt(3))
    )))

    val calculator = setUpCalculator(labelInfo = nIsPersonLabelInfo)

    val labelResult = calculator(nIsPerson.expr)
    val inequalityResult = calculator(inequality.expr)

    labelResult.factor should equal(0.1)
    inequalityResult.factor should equal(
      personPropIsNotNullSel
        * (1 - indexPersonUniqueSel) // Selectivity for != x
        * DEFAULT_RANGE_SEEK_FACTOR // Selectivity for range
        + personPropIsNotNullSel
        * indexPersonUniqueSel
        +- 0.00000001
    )
  }

  test("closed (> && <) range with one label") {
    val inequality = nPredicate(nAnded(NonEmptyList(
      greaterThan(nProp, literalInt(3)),
      lessThan(nProp, literalInt(4))
    )))

    val calculator = setUpCalculator(labelInfo = nIsPersonLabelInfo)

    val labelResult = calculator(nIsPerson.expr)
    val inequalityResult = calculator(inequality.expr)

    labelResult.factor should equal(0.1)
    inequalityResult.factor should equal(
      personPropIsNotNullSel
        * (1-indexPersonUniqueSel) // Selectivity for != x
        * DEFAULT_RANGE_SEEK_FACTOR / 2 // Selectivity for range
        +- 0.00000001
    )
  }

  test("closed (>= && <) range with one label") {
    val inequality = nPredicate(nAnded(NonEmptyList(
      greaterThanOrEqual(nProp, literalInt(3)),
      lessThan(nProp, literalInt(4))
    )))

    val calculator = setUpCalculator(labelInfo = nIsPersonLabelInfo)

    val labelResult = calculator(nIsPerson.expr)
    val inequalityResult = calculator(inequality.expr)

    labelResult.factor should equal(0.1)
    inequalityResult.factor should equal(
      personPropIsNotNullSel
        * (1 - indexPersonUniqueSel) // Selectivity for != x
        * DEFAULT_RANGE_SEEK_FACTOR / 2 // Selectivity for range
        + personPropIsNotNullSel
        * indexPersonUniqueSel
        +- 0.00000001
    )
  }

  test("three inequalities should be equal to two inequalities, one label") {
    val inequality = nPredicate(nAnded(NonEmptyList(
      greaterThan(nProp, literalInt(3)),
      lessThan(nProp, literalInt(4)),
      lessThan(nProp, literalInt(7))
    )))

    val calculator = setUpCalculator(labelInfo = nIsPersonLabelInfo)

    val labelResult = calculator(nIsPerson.expr)
    val inequalityResult = calculator(inequality.expr)

    labelResult.factor should equal(0.1)
    inequalityResult.factor should equal(
      personPropIsNotNullSel
        * (1-indexPersonUniqueSel) // Selectivity for != x
        * DEFAULT_RANGE_SEEK_FACTOR / 2 // Selectivity for range
        +- 0.00000001
    )
  }

  test("half-open (>) range with one label, no index") {
    val inequality = nPredicate(nAnded(NonEmptyList(
      greaterThan(nProp, literalInt(3))
    )))

    val calculator = setUpCalculator(labelInfo = nIsPersonLabelInfo, stats = mockStats(indexCardinalities = Map.empty))

    val labelResult = calculator(nIsPerson.expr)
    val inequalityResult = calculator(inequality.expr)

    labelResult.factor should equal(0.1)
    inequalityResult should equal(DEFAULT_RANGE_SELECTIVITY)
  }

  test("closed (> && <) range with one label, no index") {
    val inequality = nPredicate(nAnded(NonEmptyList(
      greaterThan(nProp, literalInt(3)),
      lessThan(nProp, literalInt(4))
    )))

    val calculator = setUpCalculator(labelInfo = nIsPersonLabelInfo, stats = mockStats(indexCardinalities = Map.empty))

    val labelResult = calculator(nIsPerson.expr)
    val inequalityResult = calculator(inequality.expr)

    labelResult.factor should equal(0.1)
    inequalityResult.factor should equal(DEFAULT_RANGE_SELECTIVITY.factor / 2)
  }

  test("half-open (>) range with two labels, one index") {
    val inequality = nPredicate(nAnded(NonEmptyList(
      greaterThan(nProp, literalInt(3))
    )))

    val calculator = setUpCalculator(labelInfo = nIsPersonAndAnimalLabelInfo,
      stats = mockStats(labelOrRelCardinalities = Map(indexPersonRange.label -> 1000.0, indexAnimal.label -> 10.0)))

    val labelResult1 = calculator(nIsPerson.expr)
    val labelResult2 = calculator(nIsAnimal.expr)
    val inequalityResult = calculator(inequality.expr)

    labelResult1.factor should equal(0.1)
    labelResult2.factor should equal(0.001)
    inequalityResult.factor should equal(
      personPropIsNotNullSel
        * (1-indexPersonUniqueSel) // Selectivity for != x
        * DEFAULT_RANGE_SEEK_FACTOR // Selectivity for range
        +- 0.00000001
    )
  }

  test("closed (> && <) range with two labels, one index") {
    val inequality = nPredicate(nAnded(NonEmptyList(
      greaterThan(nProp, literalInt(3)),
      lessThan(nProp, literalInt(4))
    )))

    val calculator = setUpCalculator(labelInfo = nIsPersonLabelInfo,
      stats = mockStats(labelOrRelCardinalities = Map(indexPersonRange.label -> 1000.0, indexAnimal.label -> 10.0)))

    val labelResult = calculator(nIsPerson.expr)
    val labelResult2 = calculator(nIsAnimal.expr)
    val inequalityResult = calculator(inequality.expr)

    labelResult.factor should equal(0.1)
    labelResult2.factor should equal(0.001)
    inequalityResult.factor should equal(
      personPropIsNotNullSel
        * (1 - indexPersonUniqueSel) // Selectivity for != x
        * DEFAULT_RANGE_SEEK_FACTOR / 2 // Selectivity for range
        +- 0.00000001
    )
  }

  test("half-open (>) range with two labels, two indexes") {
    val inequality = nPredicate(nAnded(NonEmptyList(
      greaterThan(nProp, literalInt(3))
    )))

    val calculator = setUpCalculator(labelInfo = nIsPersonAndAnimalLabelInfo, stats = mockStats(
      labelOrRelCardinalities = Map(indexPersonRange.label -> 1000, indexAnimal.label -> 800.0),
      indexCardinalities = Map(indexPersonRange -> 200.0, indexAnimal -> 400.0),
      indexUniqueCardinalities = Map(indexPersonRange -> 180.0, indexAnimal -> 380.0)))

    val labelResult1 = calculator(nIsPerson.expr)
    val labelResult2 = calculator(nIsAnimal.expr)
    val inequalityResult = calculator(inequality.expr)

    labelResult1.factor should equal(0.1)
    labelResult2.factor should equal(0.08)

    val personIndexSelectivity = (
      personPropIsNotNullSel
        * (1-indexPersonUniqueSel) // Selectivity for != x
        * DEFAULT_RANGE_SEEK_FACTOR // Selectivity for range
      )
    val animalIndexSelectivity = (
      animalPropIsNotNullSel
        * (1.0 - 1.0 / 380.0) // Selectivity for != x
        * DEFAULT_RANGE_SEEK_FACTOR // Selectivity for range
      )

    inequalityResult.factor should equal(personIndexSelectivity + animalIndexSelectivity - personIndexSelectivity * animalIndexSelectivity
      +- 0.00000001)
  }

  test("half-open (>=) range with two labels, two indexes") {
    val inequality = nPredicate(nAnded(NonEmptyList(
      greaterThanOrEqual(nProp, literalInt(3))
    )))

    val calculator = setUpCalculator(labelInfo = nIsPersonAndAnimalLabelInfo, stats = mockStats(
      labelOrRelCardinalities = Map(indexPersonRange.label -> 1000.0, indexAnimal.label -> 800.0),
      indexCardinalities = Map(indexPersonRange -> 200.0, indexAnimal -> 400.0),
      indexUniqueCardinalities = Map(indexPersonRange -> 180.0, indexAnimal -> 380.0)))

    val labelResult1 = calculator(nIsPerson.expr)
    val labelResult2 = calculator(nIsAnimal.expr)
    val inequalityResult = calculator(inequality.expr)

    labelResult1.factor should equal(0.1)
    labelResult2.factor should equal(0.08)

    val personIndexSelectivity = (
      personPropIsNotNullSel
        * (1 - indexPersonUniqueSel) // Selectivity for != x
        * DEFAULT_RANGE_SEEK_FACTOR // Selectivity for range
        + personPropIsNotNullSel
        * indexPersonUniqueSel
      )
    val animalIndexSelectivity = (
      animalPropIsNotNullSel
        * (1.0 - 1.0 / 380.0) // Selectivity for != x
        * DEFAULT_RANGE_SEEK_FACTOR // Selectivity for range
        + animalPropIsNotNullSel
        * (1.0 / 380.0) // Selectivity for == 3
      )

    inequalityResult.factor should equal(personIndexSelectivity + animalIndexSelectivity - personIndexSelectivity * animalIndexSelectivity
      +- 0.00000001)
  }

  test("closed (> && <) range with two labels, two indexes") {
    val inequality = nPredicate(nAnded(NonEmptyList(
      greaterThan(nProp, literalInt(3)),
      lessThan(nProp, literalInt(4))
    )))

    val calculator = setUpCalculator(labelInfo = nIsPersonAndAnimalLabelInfo, stats = mockStats(
      labelOrRelCardinalities = Map(indexPersonRange.label -> 1000.0, indexAnimal.label -> 800.0),
      indexCardinalities = Map(indexPersonRange -> 200.0, indexAnimal -> 400.0),
      indexUniqueCardinalities = Map(indexPersonRange -> 180.0, indexAnimal -> 380.0)))

    val labelResult1 = calculator(nIsPerson.expr)
    val labelResult2 = calculator(nIsAnimal.expr)
    val inequalityResult = calculator(inequality.expr)

    labelResult1.factor should equal(0.1)
    labelResult2.factor should equal(0.08)

    val personIndexSelectivity = (
      personPropIsNotNullSel
        * (1 - indexPersonUniqueSel) // Selectivity for != x
        * DEFAULT_RANGE_SEEK_FACTOR / 2 // Selectivity for range
      )
    val animalIndexSelectivity = (
      animalPropIsNotNullSel
        * (1.0 - 1.0 / 380.0) // Selectivity for != x
        * DEFAULT_RANGE_SEEK_FACTOR / 2 // Selectivity for range
      )

    inequalityResult.factor should equal(personIndexSelectivity + animalIndexSelectivity - personIndexSelectivity * animalIndexSelectivity
      +- 0.00000001)
  }

  test("closed (>= && <) range with two labels, two indexes") {
    val inequality = nPredicate(nAnded(NonEmptyList(
      greaterThanOrEqual(nProp, literalInt(3)),
      lessThan(nProp, literalInt(4))
    )))

    val calculator = setUpCalculator(labelInfo = nIsPersonAndAnimalLabelInfo, stats = mockStats(
      labelOrRelCardinalities = Map(indexPersonRange.label -> 1000.0, indexAnimal.label -> 800.0),
      indexCardinalities = Map(indexPersonRange -> 200.0, indexAnimal -> 400.0),
      indexUniqueCardinalities = Map(indexPersonRange -> 180.0, indexAnimal -> 380.0)))

    val labelResult1 = calculator(nIsPerson.expr)
    val labelResult2 = calculator(nIsAnimal.expr)
    val inequalityResult = calculator(inequality.expr)

    labelResult1.factor should equal(0.1)
    labelResult2.factor should equal(0.08)

    val personIndexSelectivity = (
      personPropIsNotNullSel
        * (1 - indexPersonUniqueSel) // Selectivity for != x
        * DEFAULT_RANGE_SEEK_FACTOR / 2 // Selectivity for range
        + personPropIsNotNullSel
        * indexPersonUniqueSel
      )
    val animalIndexSelectivity = (
      animalPropIsNotNullSel
        * (1.0 - 1.0 / 380.0) // Selectivity for != x
        * DEFAULT_RANGE_SEEK_FACTOR / 2 // Selectivity for range
        + animalPropIsNotNullSel
        * (1.0 / 380.0) // Selectivity for != x
      )

    inequalityResult.factor should equal(personIndexSelectivity + animalIndexSelectivity - personIndexSelectivity * animalIndexSelectivity
      +- 0.00000001)
  }

  // STARTS WITH, ENDS WITH, CONTAINS

  protected val substringPredicatesWithClues: Seq[((Expression, Expression) => BooleanExpression, String)]

  test("starts with/ends with/contains length 0, no label") {
    for ((mkExpr, clue) <- substringPredicatesWithClues) withClue(clue) {
      val stringPredicate = nPredicate(mkExpr(nProp, literalString("")))

      val calculator = setUpCalculator()
      val stringPredicateResult = calculator(stringPredicate.expr)
      stringPredicateResult should equal(DEFAULT_PROPERTY_SELECTIVITY * DEFAULT_TYPE_SELECTIVITY)
    }
  }

  test("starts with/ends with/contains length 1, no label") {
    for ((mkExpr, clue) <- substringPredicatesWithClues) withClue(clue) {
      val stringPredicate = nPredicate(mkExpr(nProp, literalString("1")))

      val calculator = setUpCalculator()
      val stringPredicateResult = calculator(stringPredicate.expr)
      stringPredicateResult should equal(DEFAULT_RANGE_SELECTIVITY)
    }
  }

  test("starts with/ends with/contains length 2, no label") {
    for ((mkExpr, clue) <- substringPredicatesWithClues) withClue(clue) {
      val stringPredicate = nPredicate(mkExpr(nProp, literalString("12")))

      val calculator = setUpCalculator()
      val stringPredicateResult = calculator(stringPredicate.expr)
      stringPredicateResult.factor should equal(DEFAULT_RANGE_SELECTIVITY.factor / 2)
    }
  }

  test("starts with/ends with/contains length unknown, no label") {
    for ((mkExpr, clue) <- substringPredicatesWithClues) withClue(clue) {
      val stringPredicate = nPredicate(mkExpr(nProp, varFor("string")))

      val calculator = setUpCalculator()
      val stringPredicateResult = calculator(stringPredicate.expr)
      stringPredicateResult.factor should equal(DEFAULT_RANGE_SELECTIVITY.factor / DEFAULT_STRING_LENGTH
        +- 0.00000001)
    }
  }

  test("starts with/ends with/contains length 0, one label, text index planning disabled") {
    for ((mkExpr, clue) <- substringPredicatesWithClues) withClue(clue) {
      val stringPredicate = nPredicate(mkExpr(nProp, literalString("")))

      val calculator = setUpCalculator(labelInfo = nIsPersonLabelInfo, planningTextIndexesEnabled = false)

      val labelResult = calculator(nIsPerson.expr)
      val stringPredicateResult = calculator(stringPredicate.expr)

      labelResult.factor should equal(0.1)
      stringPredicateResult.factor should equal(
        personPropIsNotNullSel
          * DEFAULT_TYPE_SELECTIVITY.factor // is string
      )
    }
  }

  test("starts with/ends with/contains length 0, one label") {
    for ((mkExpr, clue) <- substringPredicatesWithClues) withClue(clue) {
      val stringPredicate = nPredicate(mkExpr(nProp, literalString("")))

      val calculator = setUpCalculator(labelInfo = nIsPersonLabelInfo)

      val labelResult = calculator(nIsPerson.expr)
      val stringPredicateResult = calculator(stringPredicate.expr)

      labelResult.factor should equal(0.1)
      stringPredicateResult.factor shouldEqual personTextPropIsNotNullSel
    }
  }

  test("starts with/ends with/contains length 1, one label, text index planning disabled") {
    for ((mkExpr, clue) <- substringPredicatesWithClues) withClue(clue) {
      val stringPredicate = nPredicate(mkExpr(nProp, literalString("1")))

      val calculator = setUpCalculator(labelInfo = nIsPersonLabelInfo, planningTextIndexesEnabled = false)

      val labelResult = calculator(nIsPerson.expr)
      val stringPredicateResult = calculator(stringPredicate.expr)

      labelResult.factor should equal(0.1)
      stringPredicateResult.factor should equal(
        personPropIsNotNullSel
          * DEFAULT_RANGE_SEEK_FACTOR
      )
    }
  }

  test("starts with/ends with/contains length 1, one label") {
    for ((mkExpr, clue) <- substringPredicatesWithClues) withClue(clue) {
      val stringPredicate = nPredicate(mkExpr(nProp, literalString("1")))

      val calculator = setUpCalculator(labelInfo = nIsPersonLabelInfo)

      val labelResult = calculator(nIsPerson.expr)
      val stringPredicateResult = calculator(stringPredicate.expr)

      labelResult.factor should equal(0.1)
      stringPredicateResult.factor should equal(
        personTextPropIsNotNullSel
          * DEFAULT_RANGE_SEEK_FACTOR
      )
    }
  }

  test("starts with/ends with/contains length 1, one relType") {
    for ((mkExpr, clue) <- substringPredicatesWithClues) withClue(clue) {
      val stringPredicate = rPredicate(mkExpr(rProp, literalString("1")))

      val calculator = setUpCalculator(relTypeInfo = rFriendsRelTypeInfo)

      val stringPredicateResult = calculator(stringPredicate.expr)

      stringPredicateResult.factor should equal(
        friendsPropIsNotNullSel
          * DEFAULT_RANGE_SEEK_FACTOR
      )
    }
  }

  test("starts with/ends with/contains length 2, one label, text index planning disabled") {
    for ((mkExpr, clue) <- substringPredicatesWithClues) withClue(clue) {
      val stringPredicate = nPredicate(mkExpr(nProp, literalString("12")))

      val calculator = setUpCalculator(labelInfo = nIsPersonLabelInfo, planningTextIndexesEnabled = false)

      val labelResult = calculator(nIsPerson.expr)
      val stringPredicateResult = calculator(stringPredicate.expr)

      labelResult.factor should equal(0.1)
      stringPredicateResult.factor should equal(
        personPropIsNotNullSel
          * DEFAULT_RANGE_SEEK_FACTOR / 2
      )
    }
  }

  test("starts with/ends with/contains length 2, one label") {
    for ((mkExpr, clue) <- substringPredicatesWithClues) withClue(clue) {
      val stringPredicate = nPredicate(mkExpr(nProp, literalString("12")))

      val calculator = setUpCalculator(labelInfo = nIsPersonLabelInfo)

      val labelResult = calculator(nIsPerson.expr)
      val stringPredicateResult = calculator(stringPredicate.expr)

      labelResult.factor should equal(0.1)
      stringPredicateResult.factor should equal(
        personTextPropIsNotNullSel
          * DEFAULT_RANGE_SEEK_FACTOR / 2
      )
    }
  }

  test("starts with/ends with/contains length unknown, one label, text index planning disabled") {
    for ((mkExpr, clue) <- substringPredicatesWithClues) withClue(clue) {
      val stringPredicate = nPredicate(mkExpr(nProp, varFor("string")))

      val calculator = setUpCalculator(labelInfo = nIsPersonLabelInfo, planningTextIndexesEnabled = false)

      val labelResult = calculator(nIsPerson.expr)
      val stringPredicateResult = calculator(stringPredicate.expr)

      labelResult.factor should equal(0.1)
      stringPredicateResult.factor should equal(
        personPropIsNotNullSel
          * DEFAULT_RANGE_SEEK_FACTOR / DEFAULT_STRING_LENGTH
      )
    }
  }

  test("starts with/ends with/contains length unknown, one label") {
    for ((mkExpr, clue) <- substringPredicatesWithClues) withClue(clue) {
      val stringPredicate = nPredicate(mkExpr(nProp, varFor("string")))

      val calculator = setUpCalculator(labelInfo = nIsPersonLabelInfo)

      val labelResult = calculator(nIsPerson.expr)
      val stringPredicateResult = calculator(stringPredicate.expr)

      labelResult.factor should equal(0.1)
      stringPredicateResult.factor should equal(
        personTextPropIsNotNullSel
          * DEFAULT_RANGE_SEEK_FACTOR / DEFAULT_STRING_LENGTH
      )
    }
  }

  test("starts with/ends with/contains length 0, two labels") {
    for ((mkExpr, clue) <- substringPredicatesWithClues) withClue(clue) {
      val stringPredicate = nPredicate(mkExpr(nProp, literalString("")))

      val calculator = setUpCalculator(labelInfo = nIsPersonAndAnimalLabelInfo, stats = mockStats(
        labelOrRelCardinalities = Map(indexPersonRange.label -> 1000.0, indexAnimal.label -> 800.0),
        indexCardinalities = Map(indexPersonRange -> 200.0, indexAnimal -> 400.0)))

      val labelResult1 = calculator(nIsPerson.expr)
      val labelResult2 = calculator(nIsAnimal.expr)
      val stringPredicateResult = calculator(stringPredicate.expr)

      labelResult1.factor should equal(0.1)
      labelResult2.factor should equal(0.08)

      val personIndexSelectivity = (
        personPropIsNotNullSel
          * DEFAULT_TYPE_SELECTIVITY.factor // is string
        )
      val animalIndexSelectivity = (
        animalPropIsNotNullSel
          * DEFAULT_TYPE_SELECTIVITY.factor // is string
        )

      stringPredicateResult.factor should equal(personIndexSelectivity + animalIndexSelectivity - personIndexSelectivity * animalIndexSelectivity
        +- 0.00000001)
    }
  }

  test("starts with/ends with/contains length 1, two labels") {
    for ((mkExpr, clue) <- substringPredicatesWithClues) withClue(clue) {
      val stringPredicate = nPredicate(mkExpr(nProp, literalString("1")))

      val calculator = setUpCalculator(labelInfo = nIsPersonAndAnimalLabelInfo, stats = mockStats(
        labelOrRelCardinalities = Map(indexPersonRange.label -> 1000.0, indexAnimal.label -> 800.0),
        indexCardinalities = Map(indexPersonRange -> 200.0, indexAnimal -> 400.0)))

      val labelResult1 = calculator(nIsPerson.expr)
      val labelResult2 = calculator(nIsAnimal.expr)
      val stringPredicateResult = calculator(stringPredicate.expr)

      labelResult1.factor should equal(0.1)
      labelResult2.factor should equal(0.08)

      val personIndexSelectivity = (
        personPropIsNotNullSel
          * DEFAULT_RANGE_SEEK_FACTOR
        )
      val animalIndexSelectivity = (
        animalPropIsNotNullSel
          * DEFAULT_RANGE_SEEK_FACTOR
        )

      stringPredicateResult.factor should equal(personIndexSelectivity + animalIndexSelectivity - personIndexSelectivity * animalIndexSelectivity
        +- 0.00000001)
    }
  }

  test("starts with/ends with/contains length 0, two labels, multiple index types") {
    for ((mkExpr, clue) <- substringPredicatesWithClues) withClue(clue) {
      val stringPredicate = nPredicate(mkExpr(nProp, literalString("")))

      val calculator = setUpCalculator(labelInfo = nIsPersonAndAnimalLabelInfo, stats = mockStats(
        labelOrRelCardinalities = Map(indexPersonRange.label -> 1000.0, indexAnimal.label -> 800.0),
        indexCardinalities = Map(indexPersonRange -> 200.0, indexPersonText -> 100.0, indexAnimal -> 400.0)))

      val labelResult1 = calculator(nIsPerson.expr)
      val labelResult2 = calculator(nIsAnimal.expr)
      val stringPredicateResult = calculator(stringPredicate.expr)

      labelResult1.factor should equal(0.1)
      labelResult2.factor should equal(0.08)

      val personIndexSelectivity = personTextPropIsNotNullSel
      val animalIndexSelectivity = (
        animalPropIsNotNullSel
          * DEFAULT_TYPE_SELECTIVITY.factor // is string
        )

      stringPredicateResult.factor should equal(personIndexSelectivity + animalIndexSelectivity - personIndexSelectivity * animalIndexSelectivity
        +- 0.00000001)
    }
  }

  test("starts with/ends with/contains length 2, two labels") {
    for ((mkExpr, clue) <- substringPredicatesWithClues) withClue(clue) {
      val stringPredicate = nPredicate(mkExpr(nProp, literalString("12")))

      val calculator = setUpCalculator(labelInfo = nIsPersonAndAnimalLabelInfo, stats = mockStats(
        labelOrRelCardinalities = Map(indexPersonRange.label -> 1000.0, indexAnimal.label -> 800.0),
        indexCardinalities = Map(indexPersonRange -> 200.0, indexAnimal -> 400.0)))

      val labelResult1 = calculator(nIsPerson.expr)
      val labelResult2 = calculator(nIsAnimal.expr)
      val stringPredicateResult = calculator(stringPredicate.expr)

      labelResult1.factor should equal(0.1)
      labelResult2.factor should equal(0.08)

      val personIndexSelectivity = (
        personPropIsNotNullSel
          * DEFAULT_RANGE_SEEK_FACTOR / 2
        )
      val animalIndexSelectivity = (
        animalPropIsNotNullSel
          * DEFAULT_RANGE_SEEK_FACTOR / 2
        )

      stringPredicateResult.factor should equal(personIndexSelectivity + animalIndexSelectivity - personIndexSelectivity * animalIndexSelectivity
        +- 0.00000001)
    }
  }

  test("starts with/ends with/contains length 2, two labels, multiple index types") {
    for ((mkExpr, clue) <- substringPredicatesWithClues) withClue(clue) {
      val stringPredicate = nPredicate(mkExpr(nProp, literalString("12")))

      val calculator = setUpCalculator(labelInfo = nIsPersonAndAnimalLabelInfo, stats = mockStats(
        labelOrRelCardinalities = Map(indexPersonRange.label -> 1000.0, indexAnimal.label -> 800.0),
        indexCardinalities = Map(indexPersonRange -> 200.0, indexPersonText -> 100.0, indexAnimal -> 400.0)))

      val labelResult1 = calculator(nIsPerson.expr)
      val labelResult2 = calculator(nIsAnimal.expr)
      val stringPredicateResult = calculator(stringPredicate.expr)

      labelResult1.factor should equal(0.1)
      labelResult2.factor should equal(0.08)

      val personIndexSelectivity = (
        personTextPropIsNotNullSel
          * DEFAULT_RANGE_SEEK_FACTOR / 2
        )
      val animalIndexSelectivity = (
        animalPropIsNotNullSel
          * DEFAULT_RANGE_SEEK_FACTOR / 2
        )

      stringPredicateResult.factor should equal(personIndexSelectivity + animalIndexSelectivity - personIndexSelectivity * animalIndexSelectivity
        +- 0.00000001)
    }
  }

  test("starts with/ends with/contains length unknown, two labels") {
    for ((mkExpr, clue) <- substringPredicatesWithClues) withClue(clue) {
      val stringPredicate = nPredicate(mkExpr(nProp, varFor("string")))

      val calculator = setUpCalculator(labelInfo = nIsPersonAndAnimalLabelInfo, stats = mockStats(
        labelOrRelCardinalities = Map(indexPersonRange.label -> 1000.0, indexAnimal.label -> 800.0),
        indexCardinalities = Map(indexPersonRange -> 200.0, indexAnimal -> 400.0)))

      val labelResult1 = calculator(nIsPerson.expr)
      val labelResult2 = calculator(nIsAnimal.expr)
      val stringPredicateResult = calculator(stringPredicate.expr)

      labelResult1.factor should equal(0.1)
      labelResult2.factor should equal(0.08)

      val personIndexSelectivity = (
        personPropIsNotNullSel
          * DEFAULT_RANGE_SEEK_FACTOR / DEFAULT_STRING_LENGTH
        )
      val animalIndexSelectivity = (
        animalPropIsNotNullSel
          * DEFAULT_RANGE_SEEK_FACTOR / DEFAULT_STRING_LENGTH
        )

      stringPredicateResult.factor should equal(personIndexSelectivity + animalIndexSelectivity - personIndexSelectivity * animalIndexSelectivity
        +- 0.00000001)
    }
  }

  test("starts with/ends with/contains length unknown, two labels, multiple index types") {
    for ((mkExpr, clue) <- substringPredicatesWithClues) withClue(clue) {
      val stringPredicate = nPredicate(mkExpr(nProp, varFor("string")))

      val calculator = setUpCalculator(labelInfo = nIsPersonAndAnimalLabelInfo, stats = mockStats(
        labelOrRelCardinalities = Map(indexPersonRange.label -> 1000.0, indexAnimal.label -> 800.0),
        indexCardinalities = Map(indexPersonRange -> 200.0, indexPersonText -> 100.0, indexAnimal -> 400.0)))

      val labelResult1 = calculator(nIsPerson.expr)
      val labelResult2 = calculator(nIsAnimal.expr)
      val stringPredicateResult = calculator(stringPredicate.expr)

      labelResult1.factor should equal(0.1)
      labelResult2.factor should equal(0.08)

      val personIndexSelectivity = (
        personTextPropIsNotNullSel
          * DEFAULT_RANGE_SEEK_FACTOR / DEFAULT_STRING_LENGTH
        )
      val animalIndexSelectivity = (
        animalPropIsNotNullSel
          * DEFAULT_RANGE_SEEK_FACTOR / DEFAULT_STRING_LENGTH
        )

      stringPredicateResult.factor should equal(personIndexSelectivity + animalIndexSelectivity - personIndexSelectivity * animalIndexSelectivity
        +- 0.00000001)
    }
  }

  // IS NOT NULL

  private val nIsNotNull = nPredicate(isNotNull(nProp))
  private val rIsNotNull = rPredicate(isNotNull(rProp))

  test("isNotNull with no label") {
    val calculator = setUpCalculator()
    val isNotNullResult = calculator(nIsNotNull.expr)
    isNotNullResult should equal(DEFAULT_PROPERTY_SELECTIVITY)
  }

  test("isNotNull with no relType") {
    val calculator = setUpCalculator()
    val isNotNullResult = calculator(rIsNotNull.expr)
    isNotNullResult should equal(DEFAULT_PROPERTY_SELECTIVITY)
  }

  test("isNotNull with one label") {
    val calculator = setUpCalculator(labelInfo = nIsPersonLabelInfo)

    val labelResult = calculator(nIsPerson.expr)
    val isNotNullResult = calculator(nIsNotNull.expr)

    labelResult.factor should equal(0.1)
    isNotNullResult.factor should equal(personPropIsNotNullSel)
  }

  test("isNotNull with one label, text index only, greater than the default value") {
    val calculator = setUpCalculator(labelInfo = nIsPersonLabelInfo,
      stats = mockStats(indexCardinalities = Map(indexPersonText -> 900.0)))

    val labelResult = calculator(nIsPerson.expr)
    val isNotNullResult = calculator(nIsNotNull.expr)

    labelResult.factor shouldEqual 0.1
    isNotNullResult.factor shouldEqual 0.95
  }

  test("isNotNull with one label, text index only, less than the default value") {
    val calculator = setUpCalculator(labelInfo = nIsPersonLabelInfo,
      stats = mockStats(indexCardinalities = Map(indexPersonText -> 100.0)))

    val labelResult = calculator(nIsPerson.expr)
    val isNotNullResult = calculator(nIsNotNull.expr)

    labelResult.factor shouldEqual 0.1
    isNotNullResult.factor shouldEqual 0.55
  }

  test("isNotNull with one label, empty text index only") {
    val calculator = setUpCalculator(labelInfo = nIsPersonLabelInfo,
      stats = mockStats(indexCardinalities = Map(indexPersonText -> 0.0)))

    val labelResult = calculator(nIsPerson.expr)
    val isNotNullResult = calculator(nIsNotNull.expr)

    labelResult.factor shouldEqual 0.1
    isNotNullResult shouldEqual DEFAULT_PROPERTY_SELECTIVITY
  }

  test("isNotNull with one label, text index only, all properties are text") {
    val calculator = setUpCalculator(labelInfo = nIsPersonLabelInfo,
      stats = mockStats(indexCardinalities = Map(indexPersonText -> 1000.0)))

    val labelResult = calculator(nIsPerson.expr)
    val isNotNullResult = calculator(nIsNotNull.expr)

    labelResult.factor shouldEqual 0.1
    isNotNullResult shouldEqual Selectivity.ONE
  }

  test("isNotNull with one relType") {
    val calculator = setUpCalculator(relTypeInfo = rFriendsRelTypeInfo)

    val isNotNullResult = calculator(rIsNotNull.expr)

    isNotNullResult.factor should equal(friendsPropIsNotNullSel)
  }

  test("isNotNull with one label, no index") {
    val calculator = setUpCalculator(labelInfo = nIsPersonLabelInfo, stats = mockStats(indexCardinalities = Map.empty))

    val labelResult = calculator(nIsPerson.expr)
    val isNotNullResult = calculator(nIsNotNull.expr)

    labelResult.factor should equal(0.1)
    isNotNullResult should equal(DEFAULT_PROPERTY_SELECTIVITY)
  }

  test("isNotNull with one relType, no index") {
    val calculator = setUpCalculator(relTypeInfo = rFriendsRelTypeInfo, stats = mockStats(indexCardinalities = Map.empty))

    val isNotNullResult = calculator(rIsNotNull.expr)

    isNotNullResult should equal(DEFAULT_PROPERTY_SELECTIVITY)
  }

  test("isNotNull with one relType, one index") {
    val calculator = setUpCalculator(relTypeInfo = rFriendsRelTypeInfo,
      stats = mockStats(labelOrRelCardinalities = Map(indexFriends.relType -> 1000.0)))

    val isNotNullResult = calculator(rIsNotNull.expr)

    isNotNullResult.factor should equal(friendsPropIsNotNullSel)
  }

  test("isNotNull with two labels, one index") {
    val calculator = setUpCalculator(labelInfo = nIsPersonAndAnimalLabelInfo,
      stats = mockStats(labelOrRelCardinalities = Map(indexPersonRange.label -> 1000.0, indexAnimal.label -> 10.0)))

    val labelResult1 = calculator(nIsPerson.expr)
    val labelResult2 = calculator(nIsAnimal.expr)
    val isNotNullResult = calculator(nIsNotNull.expr)

    labelResult1.factor should equal(0.1)
    labelResult2.factor should equal(0.001)
    isNotNullResult.factor should equal(personPropIsNotNullSel)
  }

  test("isNotNull with two labels, two indexes") {
    val calculator = setUpCalculator(labelInfo = nIsPersonAndAnimalLabelInfo, stats = mockStats(
      labelOrRelCardinalities = Map(indexPersonRange.label -> 1000.0, indexAnimal.label -> 800.0),
      indexCardinalities = Map(indexPersonRange -> 200.0, indexAnimal -> 400.0)))

    val labelResult1 = calculator(nIsPerson.expr)
    val labelResult2 = calculator(nIsAnimal.expr)
    val isNotNullResult = calculator(nIsNotNull.expr)

    labelResult1.factor should equal(0.1)
    labelResult2.factor should equal(0.08)
    isNotNullResult.factor should equal(personPropIsNotNullSel + animalPropIsNotNullSel - personPropIsNotNullSel * animalPropIsNotNullSel
      +- 0.00000001)
  }

  // EQUALITY / IN

  test("equality with no label, size 0") {
    val equals = nPredicate(in(nProp, listOf()))

    val calculator = setUpCalculator()
    val eqResult = calculator(equals.expr)
    eqResult.factor should equal(0.0)
  }

  test("equality with no label, size 1") {
    val equals = nPredicate(super.equals(nProp, literalInt(3)))

    val calculator = setUpCalculator()
    val eqResult = calculator(equals.expr)
    eqResult should equal(DEFAULT_PROPERTY_SELECTIVITY * DEFAULT_EQUALITY_SELECTIVITY)
  }

  test("equality with no label, size 2") {
    val equals = nPredicate(in(nProp, listOfInt(3, 4)))

    val calculator = setUpCalculator()
    val eqResult = calculator(equals.expr)
    val resFor1 = DEFAULT_EQUALITY_SELECTIVITY.factor
    val resForAny = resFor1 + resFor1 - resFor1 * resFor1
    eqResult.factor should equal(DEFAULT_PROPERTY_SELECTIVITY.factor * resForAny)
  }

  test("equality with no label, size unknown") {
    val equals = nPredicate(in(nProp, varFor("someList")))

    val calculator = setUpCalculator()
    val eqResult = calculator(equals.expr)
    val resFor1 =  DEFAULT_EQUALITY_SELECTIVITY
    val resForAny = IndependenceCombiner.orTogetherSelectivities(Seq.fill(DEFAULT_LIST_CARDINALITY.amount.toInt)(resFor1)).get
    eqResult should equal(DEFAULT_PROPERTY_SELECTIVITY * resForAny)
  }

  test("equality with one label, size 0") {
    val equals = nPredicate(in(nProp, listOf()))

    val calculator = setUpCalculator()
    val labelResult = calculator(nIsPerson.expr)
    val eqResult = calculator(equals.expr)

    labelResult.factor should equal(0.1)
    eqResult.factor should equal(0.0)
  }

  test("equality with one relType, size 0") {
    val equals = rPredicate(in(rProp, listOf()))

    val calculator = setUpCalculator()
    val eqResult = calculator(equals.expr)

    eqResult.factor should equal(0.0)
  }

  test("equality with one label, size 1") {
    val equals = nPredicate(super.equals(nProp, literalInt(3)))

    val calculator = setUpCalculator(labelInfo = nIsPersonLabelInfo)

    val labelResult = calculator(nIsPerson.expr)
    val eqResult = calculator(equals.expr)

    labelResult.factor should equal(0.1)
    eqResult.factor should equal(0.2 * (1.0 / 180.0))
  }

  test("equality with one relType, size 1") {
    val equals = rPredicate(super.equals(rProp, literalInt(3)))

    val calculator = setUpCalculator(relTypeInfo = rFriendsRelTypeInfo)

    val eqResult = calculator(equals.expr)

    eqResult.factor should equal(friendsPropIsNotNullSel * indexFriendsUniqueSel)
  }

  test("equality with one label, size 2") {
    val equals = nPredicate(in(nProp, listOfInt(3, 4)))

    val calculator = setUpCalculator(labelInfo = nIsPersonLabelInfo)

    val labelResult = calculator(nIsPerson.expr)
    val eqResult = calculator(equals.expr)

    labelResult.factor should equal(0.1)
    val isNotNullSel = 200.0 / 1000.0
    val equal1Sel = 1.0 / 180.0
    val inSel = isNotNullSel * (equal1Sel + equal1Sel - equal1Sel * equal1Sel)
    eqResult.factor should equal(inSel)
  }

  test("equality with one label, auto-extracted parameter of size 2") {
    val param = AutoExtractedParameter("PARAM", CTList(CTAny), ListOfLiteralWriter(Seq(literalString("a"), literalString("b"))), Some(2))(pos)
    val equals = nPredicate(in(nProp, param))

    val calculator = setUpCalculator(labelInfo = nIsPersonLabelInfo)

    val eqResult = calculator(equals.expr)

    val existsSel = 200.0 / 1000.0
    val equal1Sel = 1.0 / 180.0
    val inSel = existsSel * (equal1Sel + equal1Sel - equal1Sel * equal1Sel)
    eqResult.factor should equal(inSel)
  }

  test("equality with one label, auto-extracted parameter of size 42") {
    val bucketSize = ListSizeBucket.computeBucket(42)
    val literalWriters = (1 to bucketSize).map(literalInt(_))
    val param = AutoExtractedParameter("PARAM", CTList(CTAny), ListOfLiteralWriter(literalWriters), Some(bucketSize))(pos)
    val equals = nPredicate(in(nProp, param))

    val calculator = setUpCalculator(labelInfo = nIsPersonLabelInfo)

    val eqResult = calculator(equals.expr)

    val existsSel = Selectivity(200.0 / 1000.0)
    val equal1Sel = Selectivity(1.0 / 180.0)
    val inSel = IndependenceCombiner.orTogetherSelectivities(Seq.fill(bucketSize)(equal1Sel)).get
    eqResult should equal(existsSel * inSel)
  }

  test("equality with one label, size unknown") {
    val equals = nPredicate(in(nProp, varFor("someList")))

    val calculator = setUpCalculator(labelInfo = nIsPersonLabelInfo)

    val labelResult = calculator(nIsPerson.expr)
    val eqResult = calculator(equals.expr)

    labelResult.factor should equal(0.1)
    val isNotNullSel = Selectivity(200.0 / 1000.0)
    val equal1Sel = Selectivity(1.0 / 180.0)
    val equalAnySel = IndependenceCombiner.orTogetherSelectivities(Seq.fill(DEFAULT_LIST_CARDINALITY.amount.toInt)(equal1Sel)).get
    eqResult should equal(isNotNullSel * equalAnySel)
  }

  test("equality with two labels, size 0") {
    val equals = nPredicate(in(nProp, listOf()))

    val calculator = setUpCalculator(labelInfo = nIsPersonAndAnimalLabelInfo, stats = mockStats(
      labelOrRelCardinalities = Map(indexPersonRange.label -> 1000.0, indexAnimal.label -> 800.0),
      indexUniqueCardinalities = Map(indexPersonRange -> 200.0, indexAnimal -> 400.0)))

    val labelResult1 = calculator(nIsPerson.expr)
    val labelResult2 = calculator(nIsAnimal.expr)
    val eqResult = calculator(equals.expr)

    labelResult1.factor should equal(0.1)
    labelResult2.factor should equal(0.08)
    eqResult.factor should equal(0.0)
  }

  test("equality with two labels, size 1") {
    val equals = nPredicate(super.equals(nProp, literalInt(3)))

    val calculator = setUpCalculator(labelInfo = nIsPersonAndAnimalLabelInfo, stats = mockStats(
      labelOrRelCardinalities = Map(indexPersonRange.label -> 1000.0, indexAnimal.label -> 800.0),
      indexCardinalities = Map(indexPersonRange -> 300.0, indexAnimal -> 500.0),
      indexUniqueCardinalities = Map(indexPersonRange -> 200.0, indexAnimal -> 400.0)))

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
    val equals = nPredicate(in(nProp, listOfInt(3, 4)))

    val calculator = setUpCalculator(labelInfo = nIsPersonAndAnimalLabelInfo, stats = mockStats(
      labelOrRelCardinalities = Map(indexPersonRange.label -> 1000.0, indexAnimal.label -> 800.0),
      indexCardinalities = Map(indexPersonRange -> 300.0, indexAnimal -> 500.0),
      indexUniqueCardinalities = Map(indexPersonRange -> 200.0, indexAnimal -> 400.0)))

    val labelResult1 = calculator(nIsPerson.expr)
    val labelResult2 = calculator(nIsAnimal.expr)
    val eqResult = calculator(equals.expr)

    labelResult1.factor should equal(0.1)
    labelResult2.factor should equal(0.08)

    val personIsNotNullSel = 300.0 / 1000.0
    val personEquals1Sel = 1.0 / 200.0
    val animalIsNotNullSel = 500.0 / 800.0
    val animalEquals1Sel = 1.0 / 400.0
    val personInSel = personIsNotNullSel * (personEquals1Sel + personEquals1Sel - personEquals1Sel * personEquals1Sel)
    val animalInSel = animalIsNotNullSel * (animalEquals1Sel + animalEquals1Sel - animalEquals1Sel * animalEquals1Sel)
    eqResult.factor should equal(personInSel + animalInSel - personInSel * animalInSel)
  }

  test("equality with two labels, size unknown") {
    val equals = nPredicate(in(nProp, varFor("someList")))

    val calculator = setUpCalculator(labelInfo = nIsPersonAndAnimalLabelInfo, stats = mockStats(
      labelOrRelCardinalities = Map(indexPersonRange.label -> 1000.0, indexAnimal.label -> 800.0),
      indexCardinalities = Map(indexPersonRange -> 300.0, indexAnimal -> 500.0),
      indexUniqueCardinalities = Map(indexPersonRange -> 200.0, indexAnimal -> 400.0)))

    val labelResult1 = calculator(nIsPerson.expr)
    val labelResult2 = calculator(nIsAnimal.expr)
    val eqResult = calculator(equals.expr)

    labelResult1.factor should equal(0.1)
    labelResult2.factor should equal(0.08)

    val personIsNotNullSel = Selectivity(300.0 / 1000.0)
    val personEquals1Sel = Selectivity(1.0 / 200.0)
    val animalIsNotNullSel = Selectivity(500.0 / 800.0)
    val animalEquals1Sel = Selectivity(1.0 / 400.0)
    val personInSel = personIsNotNullSel * IndependenceCombiner.orTogetherSelectivities(Seq.fill(DEFAULT_LIST_CARDINALITY.amount.toInt)(personEquals1Sel)).get
    val animalInSel = animalIsNotNullSel * IndependenceCombiner.orTogetherSelectivities(Seq.fill(DEFAULT_LIST_CARDINALITY.amount.toInt)(animalEquals1Sel)).get
    eqResult should equal(IndependenceCombiner.orTogetherSelectivities(Seq(personInSel, animalInSel)).get)
  }

  // OTHER

  test("Label index: Should peek inside sub predicates") {
    implicit val semanticTable: SemanticTable = SemanticTable()
    semanticTable.resolvedLabelNames.put("Page", LabelId(0))

    val hasLabels = HasLabels(varFor("n"), Seq(labelName("Page"))) _
    val labelInfo: LabelInfo = Selections(Set(nPredicate(hasLabels))).labelInfo

    val stats = mock[GraphStatistics]
    when(stats.nodesAllCardinality()).thenReturn(2000.0)
    when(stats.nodesWithLabelCardinality(Some(indexPersonRange.label))).thenReturn(1000.0)
    val calculator = ExpressionSelectivityCalculator(stats, IndependenceCombiner,
      planningTextIndexesEnabled = false, planningRangeIndexesEnabled = false, planningPointIndexesEnabled = false)

    val result = calculator(PartialPredicate[HasLabels](hasLabels, mock[HasLabels]), labelInfo, Map.empty)

    result.factor should equal(0.5)
  }

  test("should default to min graph cardinality for HasLabels with previously unknown label") {
    val stats = mock[GraphStatistics]
    when(stats.nodesAllCardinality()).thenReturn(MIN_NODES_ALL_CARDINALITY)
    when(stats.nodesWithLabelCardinality(any())).thenReturn(MIN_NODES_WITH_LABEL_CARDINALITY)
    val calculator = ExpressionSelectivityCalculator(stats, IndependenceCombiner,
      planningTextIndexesEnabled = false, planningRangeIndexesEnabled = false, planningPointIndexesEnabled = false)

    implicit val semanticTable: SemanticTable = SemanticTable()

    val expr = HasLabels(null, Seq(labelName("Foo")))(pos)
    calculator(expr, Map.empty, Map.empty) should equal(Selectivity.of(10.0 / 10.0).get)
  }

  test("selectivity of IN should never exceed the IS NOT NULL selectivity") {
    val in1 = nPredicate(in(nProp, listOfInt(1L)))
    val in100 = nPredicate(in(nProp, listOfInt(0L to 100L:_*)))
    val in10000 = nPredicate(in(nProp, listOfInt(0L to 10000L:_*)))

    val calculator = setUpCalculator(
      labelInfo = nIsPersonLabelInfo,
      stats = mockStats(
        labelOrRelCardinalities = Map(indexPersonRange.label -> 1000.0),
        indexCardinalities = Map(),
        indexUniqueCardinalities = Map()
      )
    )

    val in1Result = calculator(in1.expr)
    val in100Result = calculator(in100.expr)
    val in10000Result = calculator(in10000.expr)

    val isNotNullSel = calculator(isNotNull(nProp))

    in1Result should be <= isNotNullSel
    in100Result should be <= isNotNullSel
    in10000Result should be <= isNotNullSel
  }

  test("selectivity of IN should never exceed the IS NOT NULL selectivity, with indexes") {
    val in1 = nPredicate(in(nProp, listOfInt(1L)))
    val in100 = nPredicate(in(nProp, listOfInt(0L to 100L:_*)))
    val in10000 = nPredicate(in(nProp, listOfInt(0L to 10000L:_*)))

    val calculator = setUpCalculator(
      labelInfo = nIsPersonLabelInfo,
      stats = mockStats(
        labelOrRelCardinalities = Map(indexPersonRange.label -> 1000.0),
        indexCardinalities = Map(indexPersonRange -> 300.0),
        indexUniqueCardinalities = Map(indexPersonRange -> 2.0)
      )
    )

    val in1Result = calculator(in1.expr)
    val in100Result = calculator(in100.expr)
    val in10000Result = calculator(in10000.expr)

    val isNotNullSel = Selectivity(300.0 / 1000.0)

    in1Result should be <= isNotNullSel
    in100Result should be <= isNotNullSel
    in10000Result should be <= isNotNullSel
  }

  // POINT DISTANCE

  private val fakePoint = trueLiteral
  private val nPropDistance = nPredicate(lessThan(function(Seq("point"), "distance", nProp, fakePoint), literalInt(3)))
  private val rPropDistance = nPredicate(lessThan(function(Seq("point"), "distance", rProp, fakePoint), literalInt(3)))
  test("distance with no label") {
    val calculator = setUpCalculator()
    calculator(nPropDistance.expr) should equal(DEFAULT_RANGE_SELECTIVITY)
  }

  test("distance with one label, point index disabled") {
    val calculator = setUpCalculator(labelInfo = nIsPersonLabelInfo, planningPointIndexesEnabled = false)

    val labelResult = calculator(nIsPerson.expr)
    labelResult.factor should equal(0.1)
    calculator(nPropDistance.expr).factor should equal(
      personPropIsNotNullSel // n.prop IS NOT NULL
        * DEFAULT_RANGE_SEEK_FACTOR // point distance
    )
  }

  test("distance with one label") {
    val calculator = setUpCalculator(labelInfo = nIsPersonLabelInfo)

    val labelResult = calculator(nIsPerson.expr)
    labelResult.factor should equal(0.1)
    calculator(nPropDistance.expr).factor should equal(
      personPointPropIsNotNullSel // n.prop IS NOT NULL
        * DEFAULT_RANGE_SEEK_FACTOR // point distance
    )
  }

  test("distance with one relType, oneIndex") {
    val calculator = setUpCalculator(relTypeInfo = rFriendsRelTypeInfo, stats = mockStats(
      labelOrRelCardinalities = Map(indexFriends.relType -> 1000.0),
      indexCardinalities = Map(indexFriends -> 200.0)))

    val friendsIndexSelectivity = (
      friendsPropIsNotNullSel
        * DEFAULT_RANGE_SEEK_FACTOR // point distance
      )

    calculator(rPropDistance.expr).factor should equal(friendsIndexSelectivity)
  }

  test("distance with two labels, two indexes") {
    val calculator = setUpCalculator(labelInfo = nIsPersonAndAnimalLabelInfo, stats = mockStats(
      labelOrRelCardinalities = Map(indexPersonRange.label -> 1000.0, indexAnimal.label -> 800.0),
      indexCardinalities = Map(indexPersonRange -> 200.0, indexAnimal -> 400.0)))

    val labelResult1 = calculator(nIsPerson.expr)
    val labelResult2 = calculator(nIsAnimal.expr)
    labelResult1.factor should equal(0.1)
    labelResult2.factor should equal(0.08)

    val personIndexSelectivity = (
      personPropIsNotNullSel
        * DEFAULT_RANGE_SEEK_FACTOR // point distance
      )
    val animalIndexSelectivity = (
      animalPropIsNotNullSel
        * DEFAULT_RANGE_SEEK_FACTOR // point distance
      )

    calculator(nPropDistance.expr).factor should equal(personIndexSelectivity + animalIndexSelectivity - personIndexSelectivity * animalIndexSelectivity
      +- 0.00000001)
  }

  test("distance in AndedPropertyInequalities, point index disabled") {
    val inequality = lessThan(function(Seq("point"), "distance", nProp, fakePoint), rProp)
    val predicate = AndedPropertyInequalities(varFor("r"), rProp, NonEmptyList(inequality))

    val calculator = setUpCalculator(labelInfo = nIsPersonLabelInfo, planningPointIndexesEnabled = false)

    val distanceResult = calculator(predicate)

    distanceResult.factor should equal(
      personPropIsNotNullSel // exists n.prop
        * DEFAULT_RANGE_SEEK_FACTOR // point distance
    )
  }

  test("distance in AndedPropertyInequalities") {
    val inequality = lessThan(function(Seq("point"), "distance", nProp, fakePoint), rProp)
    val predicate = AndedPropertyInequalities(varFor("r"), rProp, NonEmptyList(inequality))

    val calculator = setUpCalculator(labelInfo = nIsPersonLabelInfo)

    val distanceResult = calculator(predicate)

    distanceResult.factor should equal(
      personPointPropIsNotNullSel // exists n.prop
        * DEFAULT_RANGE_SEEK_FACTOR // point distance
    )
  }

  // POINT BOUNDING BOX

  test("point.withinBBox(p, p1, p2) should have same selectivity as p1 <= p <= p2 when all properties are indexed points") {
    val calculator = setUpCalculator(labelInfo = nIsPersonLabelInfo, stats = mockStats(
      indexCardinalities =
        Map(indexPersonRange -> 200.0, indexPersonPoint -> 200.0),
      indexUniqueCardinalities =
        Map(indexPersonRange -> 200.0, indexPersonPoint -> 200.0),
    ))

    val lowerLeft = function("point", mapOf("x" -> literalInt(0), "y" -> literalInt(0)))
    val upperRight = function("point", mapOf("x" -> literalInt(10), "y" -> literalInt(10)))
    val inequality = nPredicate(nAnded(NonEmptyList(
      greaterThanOrEqual(nProp, lowerLeft),
      lessThanOrEqual(nProp, upperRight)
    )))
    val bbox = nPredicate(function(List("point"), "withinBBox", nProp, lowerLeft, upperRight))

    calculator(inequality.expr) should equal(calculator(bbox.expr))
  }

  test("point.withinBBox(p, p1, p2) should have lower selectivity than p1 <= p <= p2 when not all properties are points") {
    val calculator = setUpCalculator(labelInfo = nIsPersonLabelInfo, stats = mockStats(
      indexCardinalities =
        Map(indexPersonRange -> 200.0, indexPersonPoint -> 100.0),
      indexUniqueCardinalities =
        Map(indexPersonRange -> 200.0, indexPersonPoint -> 100.0),
    ))

    val lowerLeft = function("point", mapOf("x" -> literalInt(0), "y" -> literalInt(0)))
    val upperRight = function("point", mapOf("x" -> literalInt(10), "y" -> literalInt(10)))
    val inequality = nPredicate(nAnded(NonEmptyList(
      greaterThanOrEqual(nProp, lowerLeft),
      lessThanOrEqual(nProp, upperRight)
    )))
    val bbox = nPredicate(function(List("point"), "withinBBox", nProp, lowerLeft, upperRight))

    calculator(inequality.expr) should be > calculator(bbox.expr)
  }

  // HELPER METHODS

  protected def setupSemanticTable(): SemanticTable = {
    val semanticTable: SemanticTable = SemanticTable()
    semanticTable.resolvedLabelNames.put("Person", indexPersonRange.label)
    semanticTable.resolvedLabelNames.put("Animal", indexAnimal.label)
    semanticTable.resolvedPropertyKeyNames.put("nodeProp", indexPersonRange.property)

    semanticTable.resolvedRelTypeNames.put("Friends", indexFriends.relType)
    semanticTable.resolvedRelTypeNames.put("Friends", indexFriends.relType)
    semanticTable.resolvedPropertyKeyNames.put("relProp", indexFriends.property)

    semanticTable
      .addTypeInfo(literalInt(3), CTInteger)
      .addTypeInfo(literalInt(4), CTInteger)
      .addTypeInfo(literalInt(7), CTInteger)
  }

  protected def setUpCalculator(labelInfo: LabelInfo = Map.empty,
                                relTypeInfo: RelTypeInfo = Map.empty,
                                stats: GraphStatistics = mockStats(),
                                planningTextIndexesEnabled: Boolean = true,
                                planningRangeIndexesEnabled: Boolean = true,
                                planningPointIndexesEnabled: Boolean = true,
                               ): Expression => Selectivity = {
    implicit val semanticTable: SemanticTable = setupSemanticTable()

    val combiner = IndependenceCombiner
    val calculator = ExpressionSelectivityCalculator(stats, combiner, planningTextIndexesEnabled, planningRangeIndexesEnabled, planningPointIndexesEnabled)
    exp: Expression => calculator(exp, labelInfo, relTypeInfo)
  }

  /**
   * @param allNodesCardinality      total number of nodes
   * @param labelOrRelCardinalities  for each label, the number of nodes that have that label
   * @param indexCardinalities       for each index, the number of values in that index
   * @param indexUniqueCardinalities for each index, the number of unique values in that index
   */
  protected case class mockStats(
                                  allNodesCardinality: Double = 10000.0,
                                  allRelCardinality: Double = 10000.0,
                                  labelOrRelCardinalities: Map[NameId, Double] = Map(indexPersonRange.label -> 1000.0, indexFriends.relType -> 1000.0),
                                  indexCardinalities: Map[IndexDescriptor, Double] = Map(
                                    indexPersonRange -> 200.0,
                                    indexPersonText -> 100.0,
                                    indexPersonPoint -> 100.0,
                                    indexFriends -> 200.0,
                                  ),
                                  indexUniqueCardinalities: Map[IndexDescriptor, Double] = Map(indexPersonRange -> 180.0, indexFriends -> 180.0)
                                ) extends GraphStatistics {

    // sanity check:
    for {
      (id, indexCardinality) <- indexCardinalities
      labelOrRelCardinality <- labelOrRelCardinalities.get(id.id)
    } {
      if (indexCardinality > labelOrRelCardinality) {
        throw new IllegalArgumentException("Wrong test setup: Index cardinality cannot be larger than label cardinality")
      }
    }
    for {
      (id, indexUniqueCardinality) <- indexUniqueCardinalities
      otherCardinality <- indexCardinalities.get(id) ++ labelOrRelCardinalities.get(id.id)
    } {
      if (indexUniqueCardinality > otherCardinality) {
        throw new IllegalArgumentException("Wrong test setup: Index unique cardinality cannot be larger than index cardinality or label cardinality")
      }
    }

    override def nodesAllCardinality(): Cardinality = allNodesCardinality

    override def patternStepCardinality(fromLabel: Option[LabelId], relTypeId: Option[RelTypeId], toLabel: Option[LabelId]): Cardinality = (fromLabel, relTypeId, toLabel) match {
      case (None, None, None) => allRelCardinality
      case (None, relTypeId: Option[RelTypeId], None) => labelOrRelCardinalities(relTypeId.get)
    }

    override def nodesWithLabelCardinality(labelId: Option[LabelId]): Cardinality = labelOrRelCardinalities(labelId.get)

    override def indexPropertyIsNotNullSelectivity(index: IndexDescriptor): Option[Selectivity] = {
      for {
        indexCardinality <- indexCardinalities.get(index)
        labelOrRelCardinality <- labelOrRelCardinalities.get(index.id)
      } yield Selectivity(indexCardinality / labelOrRelCardinality)
    }

    override def uniqueValueSelectivity(index: IndexDescriptor): Option[Selectivity] = {
      for {
        indexUniqueCardinality <- indexUniqueCardinalities.get(index)
      } yield Selectivity(1 / indexUniqueCardinality)
    }
  }

  protected def nPredicate(expr: Expression): Predicate = Predicate(Set("n"), expr)
  protected def rPredicate(expr: Expression): Predicate = Predicate(Set("r"), expr)

  protected def nAnded(exprs: NonEmptyList[InequalityExpression]): Expression = AndedPropertyInequalities(varFor("n"), nProp, exprs)
  protected def rAnded(exprs: NonEmptyList[InequalityExpression]): Expression = AndedPropertyInequalities(varFor("r"), rProp, exprs)
}

object ExpressionSelectivityCalculatorTest {

  implicit class IndexDescriptorHelper(index: IndexDescriptor) {
    def label: LabelId = index.entityType match {
      case IndexDescriptor.EntityType.Node(label) => label
      case IndexDescriptor.EntityType.Relationship(_) => throw new IllegalStateException("Should not have been called in this test.")
    }

    def relType: RelTypeId = index.entityType match {
      case IndexDescriptor.EntityType.Node(_) => throw new IllegalStateException("Should not have been called in this test.")
      case IndexDescriptor.EntityType.Relationship(relType) => relType
    }

    def id: NameId = index.entityType match {
      case IndexDescriptor.EntityType.Node(label) => label
      case IndexDescriptor.EntityType.Relationship(relType) => relType
    }
  }
}

class RangeExpressionSelectivityCalculatorTest extends ExpressionSelectivityCalculatorTest {
  override def getIndexType: IndexDescriptor.IndexType = IndexType.Range

  override val substringPredicatesWithClues: Seq[((Expression, Expression) => BooleanExpression, String)] =
    Seq(startsWith _, endsWith _, contains _)
      .map(mkExpr => (mkExpr, mkExpr(null, null).getClass.getSimpleName))
}
