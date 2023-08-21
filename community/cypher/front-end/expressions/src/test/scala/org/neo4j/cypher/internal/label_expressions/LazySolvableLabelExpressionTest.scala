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
package org.neo4j.cypher.internal.label_expressions

import org.neo4j.cypher.internal.label_expressions.NodeLabels.KnownLabels
import org.neo4j.cypher.internal.label_expressions.NodeLabels.SomeUnknownLabels
import org.neo4j.cypher.internal.util.test_helpers.CypherScalaCheckDrivenPropertyChecks
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class LazySolvableLabelExpressionTest extends AnyFunSuite with Matchers with CypherScalaCheckDrivenPropertyChecks
    with SolvableLabelExpressionGenerators {

  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSuccessful = 100)

  test("solutions and rejected candidates are distinct") {
    forAll(genLabelExpressions) { labelExpressions =>
      labelExpressions.solutions.toSet.intersect(labelExpressions.rejectedCandidates.toSet) shouldBe empty
    }
  }

  test("no duplicated solutions or candidates") {
    forAll(genLabelExpressions) { labelExpressions =>
      val solutionsAndCandidates = labelExpressions.solutions.toVector ++ labelExpressions.rejectedCandidates.toVector
      solutionsAndCandidates.distinct shouldEqual solutionsAndCandidates
    }
  }

  test("only evaluates the first two predicates in [:A|B, :!(A|B), :C, :%, :D|E]") {
    val aOrB = SolvableLabelExpression.label("A").or(SolvableLabelExpression.label("B"))
    val labelExpression = LazySolvableLabelExpression.fold(List(
      aOrB,
      aOrB.not,
      SolvableLabelExpression.label("C"),
      SolvableLabelExpression.wildcard,
      SolvableLabelExpression.label("D").or(SolvableLabelExpression.label("E"))
    ))

    labelExpression.allKnownLabels shouldEqual Set("A", "B", "C", "D", "E")
    labelExpression.solutions.toList shouldBe empty
    labelExpression.rejectedCandidates.toList shouldEqual List(
      KnownLabels(Set.empty),
      SomeUnknownLabels,
      KnownLabels(Set("A")),
      KnownLabels(Set("B")),
      KnownLabels(Set("A", "B"))
    )
  }

  test("evaluation stops as soon as it runs out of solutions") {
    forAll(genNonEmptyListOfLabelExpressions) { expressions =>
      val candidates =
        LazySolvableLabelExpression.fold(
          expressions.head :: expressions.head.not :: expressions.tail
        ).rejectedCandidates.toList
      val expected =
        LazySolvableLabelExpression.fold(List(expressions.head, expressions.head.not)).rejectedCandidates.toList

      candidates shouldEqual expected
    }
  }

  test("nonEmptySubsets should return all subsets bar the empty set") {
    val expected =
      List(
        Set("A"),
        Set("B"),
        Set("C"),
        Set("A", "B"),
        Set("A", "C"),
        Set("B", "C"),
        Set("A", "B", "C")
      )

    LazySolvableLabelExpression.nonEmptySubsets(Set("A", "B", "C")).toList shouldEqual expected
  }

  test("nonEmptySubsets of the empty set is empty") {
    LazySolvableLabelExpression.nonEmptySubsets(Set.empty).toList shouldBe empty
  }

  test("nonEmptySubsets of a singleton set is the singleton set itself") {
    LazySolvableLabelExpression.nonEmptySubsets(Set("A")).toList shouldEqual List(Set("A"))
  }
}
