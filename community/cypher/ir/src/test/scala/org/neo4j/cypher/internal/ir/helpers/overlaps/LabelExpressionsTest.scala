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
package org.neo4j.cypher.internal.ir.helpers.overlaps

import org.neo4j.cypher.internal.ir.helpers.overlaps.NodeLabels.KnownLabels
import org.neo4j.cypher.internal.ir.helpers.overlaps.NodeLabels.SomeUnknownLabels
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class LabelExpressionsTest extends AnyFunSuite with Matchers with ScalaCheckPropertyChecks
    with LabelExpressionGenerators {

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
    val aOrB = LabelExpression.label("A").or(LabelExpression.label("B"))
    val labelExpressions = LabelExpressions.fold(List(
      aOrB,
      aOrB.not,
      LabelExpression.label("C"),
      LabelExpression.wildcard,
      LabelExpression.label("D").or(LabelExpression.label("E"))
    ))

    labelExpressions.allKnownLabels shouldEqual Set("A", "B", "C", "D", "E")
    labelExpressions.solutions.toList shouldBe empty
    labelExpressions.rejectedCandidates.toList shouldEqual List(
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
        LabelExpressions.fold(expressions.head :: expressions.head.not :: expressions.tail).rejectedCandidates.toList
      val expected =
        LabelExpressions.fold(List(expressions.head, expressions.head.not)).rejectedCandidates.toList

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

    LabelExpressions.nonEmptySubsets(Set("A", "B", "C")).toList shouldEqual expected
  }

  test("nonEmptySubsets of the empty set is empty") {
    LabelExpressions.nonEmptySubsets(Set.empty).toList shouldBe empty
  }

  test("nonEmptySubsets of a singleton set is the singleton set itself") {
    LabelExpressions.nonEmptySubsets(Set("A")).toList shouldEqual List(Set("A"))
  }
}
