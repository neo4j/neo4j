/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.rewriting

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.expressions.LabelExpression
import org.neo4j.cypher.internal.expressions.LabelExpression.ColonConjunction
import org.neo4j.cypher.internal.expressions.LabelExpression.ColonDisjunction
import org.neo4j.cypher.internal.expressions.LabelExpression.Conjunction
import org.neo4j.cypher.internal.expressions.LabelExpression.Disjunction
import org.neo4j.cypher.internal.expressions.LabelExpression.Leaf
import org.neo4j.cypher.internal.expressions.LabelExpression.Negation
import org.neo4j.cypher.internal.expressions.LabelExpression.Wildcard
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.rewriting.rewriters.AddUniquenessPredicates
import org.neo4j.cypher.internal.rewriting.rewriters.AddUniquenessPredicates.evaluate
import org.neo4j.cypher.internal.rewriting.rewriters.AddUniquenessPredicates.getRelTypesToConsider
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.scalacheck.Arbitrary
import org.scalacheck.Gen
import org.scalactic.anyvals.PosZInt
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.annotation.tailrec

class AddUniquenessPredicatesTest extends CypherFunSuite with RewriteTest with AstConstructionTestSupport {

  test("does not introduce predicate not needed") {
    assertIsNotRewritten("RETURN 42")
    assertIsNotRewritten("MATCH (n) RETURN n")
    assertIsNotRewritten("MATCH (n)-[r1]->(m) RETURN n")
    assertIsNotRewritten("MATCH (n)-[r1]->(m) MATCH (m)-[r2]->(x) RETURN x")
  }

  test("uniqueness check is done between relationships of simple and variable pattern lengths") {
    assertRewrite(
      "MATCH (a)-[r1]->(b)-[r2*0..1]->(c) RETURN *",
      "MATCH (a)-[r1]->(b)-[r2*0..1]->(c) WHERE NONE(`  UNNAMED0` IN r2 WHERE r1 = `  UNNAMED0`) RETURN *"
    )

    assertRewrite(
      "MATCH (a)-[r1*0..1]->(b)-[r2]->(c) RETURN *",
      "MATCH (a)-[r1*0..1]->(b)-[r2]->(c) WHERE NONE(`  UNNAMED0` IN r1 WHERE `  UNNAMED0` = r2) RETURN *"
    )

    assertRewrite(
      "MATCH (a)-[r1*0..1]->(b)-[r2*0..1]->(c) RETURN *",
      "MATCH (a)-[r1*0..1]->(b)-[r2*0..1]->(c) WHERE NONE(`  UNNAMED0` IN r1 WHERE ANY(`  UNNAMED1` IN r2 WHERE `  UNNAMED0` = `  UNNAMED1`)) RETURN *"
    )
  }

  test("uniqueness check is done between relationships") {
    assertRewrite(
      "MATCH (a)-[r1]->(b)-[r2]->(c) RETURN *",
      "MATCH (a)-[r1]->(b)-[r2]->(c) WHERE not(r1 = r2) RETURN *"
    )

    assertRewrite(
      "MATCH (a)-[r1]->(b)-[r2]->(c)-[r3]->(d) RETURN *",
      "MATCH (a)-[r1]->(b)-[r2]->(c)-[r3]->(d) WHERE not(r2 = r3) AND not(r1 = r3) AND not(r1 = r2) RETURN *"
    )

    assertRewrite(
      "MATCH (a)-[r1]->(b), (b)-[r2]->(c), (c)-[r3]->(d) RETURN *",
      "MATCH (a)-[r1]->(b), (b)-[r2]->(c), (c)-[r3]->(d) WHERE not(r1 = r2) AND not(r1 = r3) AND not(r2 = r3) RETURN *"
    )
  }

  test("no uniqueness check between relationships of different type") {
    assertRewrite(
      "MATCH (a)-[r1:X]->(b)-[r2:Y]->(c) RETURN *",
      "MATCH (a)-[r1:X]->(b)-[r2:Y]->(c) RETURN *"
    )

    assertRewrite(
      "MATCH (a)-[r1:X]->(b)-[r2:X|Y]->(c) RETURN *",
      "MATCH (a)-[r1:X]->(b)-[r2:X|Y]->(c) WHERE not(r1 = r2) RETURN *"
    )

    assertRewrite(
      "MATCH (a)-[r1]->(b)-[r2:X]->(c) RETURN *",
      "MATCH (a)-[r1]->(b)-[r2:X]->(c) WHERE not(r1 = r2) RETURN *"
    )

    assertRewrite(
      "MATCH (a)-[r1]->(b)-[r2]->(c) RETURN *",
      "MATCH (a)-[r1]->(b)-[r2]->(c) WHERE not(r1 = r2) RETURN *"
    )

    assertRewrite(
      "MATCH (a)-[r1]->(b)-[r2:%]->(c) RETURN *",
      "MATCH (a)-[r1]->(b)-[r2:%]->(c) WHERE not(r1 = r2) RETURN *"
    )

    assertRewrite(
      "MATCH (a)-[r1:%]->(b)-[r2:%]->(c) RETURN *",
      "MATCH (a)-[r1:%]->(b)-[r2:%]->(c) WHERE not(r1 = r2) RETURN *"
    )

    assertIsNotRewritten("MATCH (a)-[r1]->(b)-[r2:!%]->(c) RETURN *")

    assertIsNotRewritten("MATCH (a)-[r1:X]->(b)-[r2:!X]->(c) RETURN *")

    assertRewrite(
      "MATCH (a)-[r1]->(b)-[r2:!X]->(c) RETURN *",
      "MATCH (a)-[r1]->(b)-[r2:!X]->(c) WHERE not(r1 = r2) RETURN *"
    )

    assertIsNotRewritten("MATCH (a)-[r1:A&B]->(b)-[r2:B&C]->(c) RETURN *")
  }

  test("getRelTypesToConsider should return all relevant relationship types") {
    getRelTypesToConsider(None) shouldEqual Seq(relTypeName(""))

    getRelTypesToConsider(Some(labelRelTypeLeaf("A"))) should contain theSameElementsAs Seq(
      relTypeName(""),
      relTypeName("A")
    )

    getRelTypesToConsider(Some(
      labelConjunction(
        labelRelTypeLeaf("A"),
        labelDisjunction(labelNegation(labelRelTypeLeaf("B")), labelRelTypeLeaf("C"))
      )
    )) should contain theSameElementsAs Seq(relTypeName(""), relTypeName("A"), relTypeName("B"), relTypeName("C"))
  }

  test("overlaps") {
    evaluate(labelRelTypeLeaf("A"), relTypeName("A")).result shouldBe true

    evaluate(labelDisjunction(labelRelTypeLeaf("A"), labelRelTypeLeaf("B")), relTypeName("B")).result shouldBe true
    evaluate(labelConjunction(labelRelTypeLeaf("A"), labelRelTypeLeaf("B")), relTypeName("B")).result shouldBe false
    evaluate(labelConjunction(labelWildcard(), labelRelTypeLeaf("B")), relTypeName("B")).result shouldBe true
    evaluate(
      labelConjunction(labelNegation(labelRelTypeLeaf("A")), labelRelTypeLeaf("B")),
      relTypeName("B")
    ).result shouldBe true
  }

  test("ignores shortestPath relationships for uniqueness") {
    assertRewrite(
      "MATCH (a)-[r1]->(b), shortestPath((a)-[r]->(b)) RETURN *",
      "MATCH (a)-[r1]->(b), shortestPath((a)-[r]->(b)) RETURN *"
    )

    assertRewrite(
      "MATCH (a)-[r1]->(b)-[r2]->(c), shortestPath((a)-[r]->(b)) RETURN *",
      "MATCH (a)-[r1]->(b)-[r2]->(c), shortestPath((a)-[r]->(b)) WHERE not(r1 = r2) RETURN *"
    )
  }

  test("ignores allShortestPaths relationships for uniqueness") {
    assertRewrite(
      "MATCH (a)-[r1]->(b), allShortestPaths((a)-[r]->(b)) RETURN *",
      "MATCH (a)-[r1]->(b), allShortestPaths((a)-[r]->(b)) RETURN *"
    )

    assertRewrite(
      "MATCH (a)-[r1]->(b)-[r2]->(c), allShortestPaths((a)-[r]->(b)) RETURN *",
      "MATCH (a)-[r1]->(b)-[r2]->(c), allShortestPaths((a)-[r]->(b)) WHERE not(r1 = r2) RETURN *"
    )
  }

  def rewriterUnderTest: Rewriter = AddUniquenessPredicates(new AnonymousVariableNameGenerator)
}

class AddUniquenessPredicatesPropertyTest extends CypherFunSuite with ScalaCheckPropertyChecks
    with RelationshipTypeExpressionGenerators {

  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(
      minSuccessful = 100,
      minSize = PosZInt(0),
      sizeRange = PosZInt(20)
    )

  test("overlaps is commutative") {
    forAll { (expression1: RelationshipTypeExpression, expression2: RelationshipTypeExpression) =>
      expression1.overlaps(expression2) shouldEqual expression2.overlaps(expression1)
    }
  }

  test("never overlap with nothing") {
    forAll { (expression: RelationshipTypeExpression) =>
      expression.overlaps(!wildcard) shouldBe false
    }
  }

  test("overlaps boolean logic") {
    forAll {
      (
        expression1: RelationshipTypeExpression,
        expression2: RelationshipTypeExpression,
        expression3: RelationshipTypeExpression
      ) =>
        val doesOverlap = expression1.overlaps(expression2)
        if (doesOverlap)
          withClue("expression1.overlaps(expression2) ==> expression1.overlaps(expression2.or(expression3))") {
            expression1.overlaps(expression2.or(expression3)) shouldBe true
          }
        else
          withClue("!expression1.overlaps(expression2) ==> !expression1.overlaps(expression2.and(expression3))") {
            expression1.overlaps(expression2.and(expression3)) shouldBe false
          }
    }
  }

  test("overlaps is stack-safe") {
    @tailrec
    def buildExpression(i: Int, expression: RelationshipTypeExpression): RelationshipTypeExpression =
      if (i <= 0) expression else buildExpression(i - 1, !expression)

    buildExpression(10_000, wildcard).overlaps(wildcard) shouldBe true
  }
}

trait RelationshipTypeExpressionGenerators {

  /**
   * Finite (small) set of names used to build arbitrary relationship type expressions.
   * It's all Greek to me.
   * Keeping it small and hard-coded ensures that the expressions will contain overlaps
   */
  val names = Set("ALPHA", "BETA", "GAMMA", "DELTA", "EPSILON")

  val position: InputPosition = InputPosition.NONE

  /**
   * Wrapper type around a [[LabelExpression]] that can be found in a relationship pattern
   * @param value Underlying [[LabelExpression]] that doesn't contain any [[Label]] or [[LabelOrRelType]]
   */
  case class RelationshipTypeExpression(value: LabelExpression) {

    def overlaps(other: RelationshipTypeExpression): Boolean = {
      val allTypes = AddUniquenessPredicates.getRelTypesToConsider(Some(value)).concat(
        AddUniquenessPredicates.getRelTypesToConsider(Some(other.value))
      )
      (AddUniquenessPredicates.overlaps(allTypes, Some(value)) intersect AddUniquenessPredicates.overlaps(
        allTypes,
        Some(other.value)
      )).nonEmpty
    }

    def unary_! : RelationshipTypeExpression =
      RelationshipTypeExpression(Negation(value)(position))

    def and(other: RelationshipTypeExpression): RelationshipTypeExpression =
      RelationshipTypeExpression(Conjunction(value, other.value)(position))

    def or(other: RelationshipTypeExpression): RelationshipTypeExpression =
      RelationshipTypeExpression(Disjunction(value, other.value)(position))
  }

  val wildcard: RelationshipTypeExpression = RelationshipTypeExpression(Wildcard()(position))

  val genWildCard: Gen[Wildcard] = Gen.const(Wildcard()(position))

  val genRelType: Gen[Leaf] =
    Gen.oneOf(names.toSeq).map(name => Leaf(RelTypeName(name)(position)))

  def genBinary[A](f: (LabelExpression, LabelExpression) => A): Gen[A] =
    Gen.sized(size =>
      for {
        lhs <- Gen.resize(size / 2, genLabelExpression)
        rhs <- Gen.resize(size / 2, genLabelExpression)
      } yield f(lhs, rhs)
    )

  val genConjunction: Gen[Conjunction] =
    genBinary((lhs, rhs) => Conjunction(lhs, rhs)(position))

  val genColonConjunction: Gen[ColonConjunction] =
    genBinary((lhs, rhs) => ColonConjunction(lhs, rhs)(position))

  val genDisjunction: Gen[Disjunction] =
    genBinary((lhs, rhs) => Disjunction(lhs, rhs)(position))

  val genColonDisjunction: Gen[ColonDisjunction] =
    genBinary((lhs, rhs) => ColonDisjunction(lhs, rhs)(position))

  val genNegation: Gen[Negation] =
    Gen.sized(size => Gen.resize(size - 1, genLabelExpression)).map(Negation(_)(position))

  val genLabelExpression: Gen[LabelExpression] =
    Gen.sized(size =>
      if (size <= 0)
        Gen.oneOf(
          genWildCard,
          genRelType
        )
      else
        Gen.oneOf(
          genConjunction,
          genColonConjunction,
          genDisjunction,
          genColonDisjunction,
          genNegation,
          genWildCard,
          genRelType
        )
    )

  implicit val arbitraryRelationshipTypeExpression: Arbitrary[RelationshipTypeExpression] =
    Arbitrary(genLabelExpression.map(RelationshipTypeExpression))
}
