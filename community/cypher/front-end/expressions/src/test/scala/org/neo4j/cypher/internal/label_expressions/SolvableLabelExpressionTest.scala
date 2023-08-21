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
import org.scalatest.Inspectors
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SolvableLabelExpressionTest extends AnyFunSuite with Matchers with CypherScalaCheckDrivenPropertyChecks
    with SolvableLabelExpressionGenerators {

  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSuccessful = 100)

  // Label expression forms a boolean algebra, it comes with a set of laws
  // Monotone laws:

  test("Associativity of OR") {
    forAll(gen3LabelExpressions) { case (x, y, z) =>
      x.or(y.or(z)).solutions should ===((x.or(y)).or(z).solutions)
    }
  }

  test("Associativity of AND") {
    forAll(gen3LabelExpressions) { case (x, y, z) =>
      x.and(y.and(z)).solutions should ===((x.and(y)).and(z).solutions)
    }
  }

  test("Commutativity of OR") {
    forAll(gen2LabelExpressions) { case (x, y) =>
      x.or(y).solutions should ===(y.or(x).solutions)
    }
  }

  test("Commutativity of AND") {
    forAll(gen2LabelExpressions) { case (x, y) =>
      x.and(y).solutions should ===(y.and(x).solutions)
    }
  }

  test("Distributivity of OR over AND") {
    forAll(gen3LabelExpressions) { case (x, y, z) =>
      x.or(y.and(z)).solutions should ===((x.or(y)).and(x.or(z)).solutions)
    }
  }

  test("Distributivity of AND over OR") {
    forAll(gen3LabelExpressions) { case (x, y, z) =>
      x.and(y.or(z)).solutions should ===((x.and(y)).or(x.and(z)).solutions)
    }
  }

  test("Zero evaluates to no solutions") {
    zero.solutions should ===(Set.empty)
  }

  test("One evaluates to all solutions") {
    one.solutions should ===(allSolutionsForLabels(Set.empty))
  }

  test("Identity for OR") {
    forAll(genLabelExpression) { x =>
      x.or(zero).solutions should ===(x.solutions)
    }
  }

  test("Identity for AND") {
    forAll(genLabelExpression) { x =>
      x.and(one).solutions should ===(x.solutions)
    }
  }

  test("Annihilator for OR") {
    forAll(genLabelExpression) { x =>
      x.or(one).solutions should ===(allSolutionsForLabels(x.allLabels))
    }
  }

  test("Annihilator for AND") {
    forAll(genLabelExpression) { x =>
      x.and(zero).solutions shouldBe empty
    }
  }

  test("Idempotence for OR") {
    forAll(genLabelExpression) { x =>
      x.or(x).solutions should ===(x.solutions)
    }
  }

  test("Idempotence for And") {
    forAll(genLabelExpression) { x =>
      x.and(x).solutions should ===(x.solutions)
    }
  }

  test("Absorption of OR over AND") {
    forAll(gen2LabelExpressions) { case (x, y) =>
      x.or(x.and(y)).solutions should contain allElementsOf x.solutions
    }
  }

  test("Absorption of AND over OR") {
    forAll(gen2LabelExpressions) { case (x, y) =>
      x.and(x.or(y)).solutions should contain allElementsOf x.solutions
    }
  }

  // Nonmonotone laws:

  test("Complementation over OR") {
    forAll(genLabelExpression) { x =>
      x.or(x.not).solutions should ===(allSolutionsForLabels(x.allLabels))
    }
  }

  test("Complementation over AND") {
    forAll(genLabelExpression) { x =>
      x.and(x.not).solutions shouldBe empty
    }
  }

  test("Double negation") {
    forAll(genLabelExpression) { x =>
      x.not.not.solutions should ===(x.solutions)
    }
  }

  test("De Morgan over OR") {
    forAll(gen2LabelExpressions) { case (x, y) =>
      x.not.or(y.not).solutions should ===(x.and(y).not.solutions)
    }
  }

  test("De Morgan over AND") {
    forAll(gen2LabelExpressions) { case (x, y) =>
      x.not.and(y.not).solutions should ===(x.or(y).not.solutions)
    }
  }

  // Additional XOR equivalences:

  test("x XOR y = (x OR y) AND NOT (x AND y)") {
    forAll(gen2LabelExpressions) { case (x, y) =>
      x.xor(y).solutions should ===((x.or(y)).and((x.and(y)).not).solutions)
    }
  }

  test("x XOR y = (x AND NOT y) OR (NOT x AND y)") {
    forAll(gen2LabelExpressions) { case (x, y) =>
      x.xor(y).solutions should ===((x.and(y.not)).or(x.not.and(y)).solutions)
    }
  }

  test("x XOR y = (x OR y) AND (NOT x OR NOT y)") {
    forAll(gen2LabelExpressions) { case (x, y) =>
      x.xor(y).solutions should ===((x.or(y)).and(x.not.or(y.not)).solutions)
    }
  }

  // Finally, we want to make sure that we combine label expressions together correctly:

  test("Evaluating multiple label expressions is like evaluating the conjunction of all the expressions") {
    forAll(genListOfLabelExpressions) { expressions =>
      SolvableLabelExpression.allSolutions(expressions).toSet should ===(
        expressions.foldLeft(one)(_.and(_)).solutions
      )
    }
  }

  test("A set of labels matches an expression if it is one of its solutions") {
    forAll(genLabelExpression) { labelExpression =>
      val (solutions, other) = labelExpression.allLabels.subsets().toList.partition { labels =>
        labelExpression.solutions.contains(KnownLabels(labels))
      }

      Inspectors.forAll(solutions) { labels =>
        labelExpression.matchesLabels(labels) shouldBe true
      }

      Inspectors.forAll(other) { labels =>
        labelExpression.matchesLabels(labels) shouldBe false
      }
    }
  }

  def allSolutionsForLabels(allLabels: Set[String]): Set[NodeLabels] =
    allLabels
      .subsets
      .map(KnownLabels)
      .toSet[NodeLabels]
      .incl(SomeUnknownLabels)
}
