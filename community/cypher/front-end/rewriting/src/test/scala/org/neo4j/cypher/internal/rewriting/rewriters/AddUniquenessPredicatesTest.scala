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
package org.neo4j.cypher.internal.rewriting.rewriters

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.label_expressions.BinaryLabelExpression
import org.neo4j.cypher.internal.label_expressions.LabelExpression
import org.neo4j.cypher.internal.label_expressions.LabelExpression.ColonConjunction
import org.neo4j.cypher.internal.label_expressions.LabelExpression.ColonDisjunction
import org.neo4j.cypher.internal.label_expressions.LabelExpression.Conjunctions
import org.neo4j.cypher.internal.label_expressions.LabelExpression.Disjunctions
import org.neo4j.cypher.internal.label_expressions.LabelExpression.Leaf
import org.neo4j.cypher.internal.label_expressions.LabelExpression.Negation
import org.neo4j.cypher.internal.label_expressions.LabelExpression.Wildcard
import org.neo4j.cypher.internal.label_expressions.MultiOperatorLabelExpression
import org.neo4j.cypher.internal.rewriting.RewriteTest
import org.neo4j.cypher.internal.rewriting.rewriters.AddUniquenessPredicates.evaluate
import org.neo4j.cypher.internal.rewriting.rewriters.AddUniquenessPredicates.getRelTypesToConsider
import org.neo4j.cypher.internal.rewriting.rewriters.RelationshipUniqueness.SingleRelationship
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.inSequence
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.test_helpers.CypherScalaCheckDrivenPropertyChecks
import org.scalacheck.Arbitrary
import org.scalacheck.Gen
import org.scalactic.anyvals.PosZInt

import scala.annotation.tailrec

class AddUniquenessPredicatesTest extends CypherFunSuite with RewriteTest with AstConstructionTestSupport {

  private def disjoint(lhs: String, rhs: String): String =
    s"NONE(`  UNNAMED0` IN $lhs WHERE `  UNNAMED0` IN $rhs)"

  private def unique(rhs: String, unnamedOffset: Int = 0): String =
    s"ALL(`  UNNAMED$unnamedOffset` IN $rhs WHERE SINGLE(`  UNNAMED${unnamedOffset + 1}` IN $rhs WHERE `  UNNAMED$unnamedOffset` = `  UNNAMED${unnamedOffset + 1}`))"

  test("does not introduce predicate not needed") {
    assertIsNotRewritten("RETURN 42")
    assertIsNotRewritten("MATCH (n) RETURN n")
    assertIsNotRewritten("MATCH (n)-[r1]->(m) RETURN n")
    assertIsNotRewritten("MATCH (n)-[r1]->(m) MATCH (m)-[r2]->(x) RETURN x")
  }

  test("MATCH with REPEATABLE ELEMENTS should not introduce uniqueness predicates") {
    Seq(
      "(b)-[r*0..1]->(c)",
      "(a)-[r1]->(b)-[r2*0..1]->(c)",
      "(a)-[r1:R1]->(b)-[r2:R2*0..1]->(c)-[r3:R1|R2*0..1]->(d)",
      "(a)-[r1]->(b)-[r2]->(c)",
      "(a)-[r1]->(b)-[r2]->(c), shortestPath((a)-[r*]->(b))",
      "(a)-[r]->(b)-[r]->(c)",
      "(a)-[r]->+(b)-[r]->(c)",
      "(a)(()-[r]->(b)-[r]->())+(c)",
      "SHORTEST 1 (a)-[r]->+(b)-[r]->(c)"
    ).foreach { pattern =>
      assertIsNotRewritten(s"MATCH REPEATABLE ELEMENTS $pattern RETURN *")
    }
  }

  test("uniqueness check is done for one variable length relationship") {
    assertRewrite(
      "MATCH (b)-[r*0..1]->(c) RETURN *",
      s"MATCH (b)-[r*0..1]->(c) WHERE ${unique("r")} RETURN *"
    )
  }

  test("uniqueness check is done for one variable length relationship inside an EXISTS Expression") {
    assertRewrite(
      "MATCH (a) WHERE EXISTS { MATCH (b)-[r*0..1]->(c) RETURN 1 } RETURN *",
      s"MATCH (a) WHERE EXISTS { MATCH (b)-[r*0..1]->(c) WHERE ${unique("r")} RETURN 1 } RETURN *"
    )
  }

  test(
    "uniqueness check should still be done when outer MATCH has REPEATABLE ELEMENTS for one variable length relationship inside an EXISTS Expression"
  ) {
    assertRewrite(
      "MATCH REPEATABLE ELEMENTS (a) WHERE EXISTS { MATCH (b)-[r*0..1]->(c) RETURN 1 } RETURN *",
      s"MATCH REPEATABLE ELEMENTS (a) WHERE EXISTS { MATCH (b)-[r*0..1]->(c) WHERE ${unique("r")} RETURN 1 } RETURN *"
    )
  }

  test("uniqueness check is done for one variable length relationship inside a COUNT Expression") {
    assertRewrite(
      "MATCH (a) WHERE COUNT { MATCH (b)-[r*0..1]->(c) RETURN 1 } > 2 RETURN *",
      s"MATCH (a) WHERE COUNT { MATCH (b)-[r*0..1]->(c) WHERE ${unique("r")} RETURN 1 } > 2 RETURN *"
    )
  }

  test("uniqueness check is done between relationships of simple and variable pattern lengths") {
    assertRewrite(
      "MATCH (a)-[r1]->(b)-[r2*0..1]->(c) RETURN *",
      s"MATCH (a)-[r1]->(b)-[r2*0..1]->(c) WHERE NOT r1 IN r2 AND ${unique("r2")} RETURN *"
    )

    assertRewrite(
      "MATCH (a)-[r1*0..1]->(b)-[r2]->(c) RETURN *",
      s"MATCH (a)-[r1*0..1]->(b)-[r2]->(c) WHERE NOT r2 IN r1 AND ${unique("r1")} RETURN *"
    )
  }

  test("no uniqueness check between relationships of simple and variable pattern lengths of different type") {
    assertRewrite(
      "MATCH (a)-[r1:R1]->(b)-[r2:R2*0..1]->(c) RETURN *",
      s"MATCH (a)-[r1:R1]->(b)-[r2:R2*0..1]->(c) WHERE ${unique("r2")} RETURN *"
    )

    assertRewrite(
      "MATCH (a)-[r1:R1*0..1]->(b)-[r2:R2]->(c) RETURN *",
      s"MATCH (a)-[r1:R1*0..1]->(b)-[r2:R2]->(c) WHERE ${unique("r1")} RETURN *"
    )

    assertRewrite(
      "MATCH (a)-[r1:R1]->(b)-[r2:R2*0..1]->(c)-[r3:R1|R2*0..1]->(d) RETURN *",
      s"""MATCH (a)-[r1:R1]->(b)-[r2:R2*0..1]->(c)-[r3:R1|R2*0..1]->(d)
         |WHERE ${disjoint("r3", "r2")} AND NOT r1 IN r3 AND ${unique("r3", 1)} AND ${unique("r2", 3)}
         |RETURN *""".stripMargin
    )
  }

  test("uniqueness check is done between relationships of variable and variable pattern lengths") {
    assertRewrite(
      "MATCH (a)-[r1*0..1]->(b)-[r2*0..1]->(c) RETURN *",
      s"""MATCH (a)-[r1*0..1]->(b)-[r2*0..1]->(c)
         |WHERE ${disjoint("r2", "r1")} AND ${unique("r2", 1)} AND ${unique("r1", 3)}
         |RETURN *""".stripMargin
    )
  }

  test("uniqueness check is done for the same repeated variable length relationship") {
    assertRewrite(
      "MATCH (a)-[r1*0..1]->(b)-[r1*0..1]->(c) RETURN *",
      s"""MATCH (a)-[r1*0..1]->(b)-[r1*0..1]->(c)
         |WHERE ${disjoint("r1", "r1")} AND ${unique("r1", 1)} AND ${unique("r1", 3)}
         |RETURN *""".stripMargin
    )
  }

  test("no uniqueness check between relationships of variable and variable pattern lengths of different type") {
    assertRewrite(
      "MATCH (a)-[r1:R1*0..1]->(b)-[r2:R2*0..1]->(c) RETURN *",
      s"""MATCH (a)-[r1:R1*0..1]->(b)-[r2:R2*0..1]->(c)
         |WHERE ${unique("r2")} AND ${unique("r1", 2)}
         |RETURN *""".stripMargin
    )
  }

  test("uniqueness check is done between relationships") {
    assertRewrite(
      "MATCH (a)-[r1]->(b)-[r2]->(c) RETURN *",
      "MATCH (a)-[r1]->(b)-[r2]->(c) WHERE NOT(r2 = r1) RETURN *"
    )

    assertRewrite(
      "MATCH (a)-[r1]->(b)-[r2]->(c)-[r3]->(d) RETURN *",
      "MATCH (a)-[r1]->(b)-[r2]->(c)-[r3]->(d) WHERE NOT(r3 = r2) AND NOT(r3 = r1) AND NOT(r2 = r1) RETURN *"
    )

    assertRewrite(
      "MATCH (a)-[r1]->(b), (b)-[r2]->(c), (c)-[r3]->(d) RETURN *",
      "MATCH (a)-[r1]->(b), (b)-[r2]->(c), (c)-[r3]->(d) WHERE NOT(r1 = r2) AND NOT(r1 = r3) AND NOT(r2 = r3) RETURN *"
    )

    assertRewrite(
      "MATCH (a)-[r1:A]->(b), (b)-[r2:B]->(c), (c)-[r3:A|B]->(d) RETURN *",
      "MATCH (a)-[r1:A]->(b), (b)-[r2:B]->(c), (c)-[r3:A|B]->(d) WHERE NOT(r1 = r3) AND NOT(r2 = r3) RETURN *"
    )
  }

  test("uniqueness check is done between relationships, also if they have the same name") {
    assertRewrite(
      "MATCH (a)-[r]->(b)-[r]->(c) RETURN *",
      "MATCH (a)-[r]->(b)-[r]->(c) WHERE false RETURN *"
    )
  }

  test("no uniqueness check between relationships of different type") {
    assertIsNotRewritten("MATCH (a)-[r1:X]->(b)-[r2:Y]->(c) RETURN *")

    assertRewrite(
      "MATCH (a)-[r1:X]->(b)-[r2:X|Y]->(c) RETURN *",
      "MATCH (a)-[r1:X]->(b)-[r2:X|Y]->(c) WHERE NOT(r2 = r1) RETURN *"
    )

    assertRewrite(
      "MATCH (a)-[r1]->(b)-[r2:X]->(c) RETURN *",
      "MATCH (a)-[r1]->(b)-[r2:X]->(c) WHERE NOT(r2 = r1) RETURN *"
    )

    assertRewrite(
      "MATCH (a)-[r1]->(b)-[r2]->(c) RETURN *",
      "MATCH (a)-[r1]->(b)-[r2]->(c) WHERE NOT(r2 = r1) RETURN *"
    )

    assertRewrite(
      "MATCH (a)-[r1]->(b)-[r2:%]->(c) RETURN *",
      "MATCH (a)-[r1]->(b)-[r2:%]->(c) WHERE NOT(r2 = r1) RETURN *"
    )

    assertRewrite(
      "MATCH (a)-[r1:%]->(b)-[r2:%]->(c) RETURN *",
      "MATCH (a)-[r1:%]->(b)-[r2:%]->(c) WHERE NOT(r2 = r1) RETURN *"
    )

    assertIsNotRewritten("MATCH (a)-[r1]->(b)-[r2:!%]->(c) RETURN *")

    assertIsNotRewritten("MATCH (a)-[r1:X]->(b)-[r2:!X]->(c) RETURN *")

    assertRewrite(
      "MATCH (a)-[r1]->(b)-[r2:!X]->(c) RETURN *",
      "MATCH (a)-[r1]->(b)-[r2:!X]->(c) WHERE NOT(r2 = r1) RETURN *"
    )

    assertIsNotRewritten("MATCH (a)-[r1:A&B]->(b)-[r2:B&C]->(c) RETURN *")
  }

  test("uniqueness check is done for a single length-one QPP") {
    assertRewrite(
      "MATCH (b) (()-[r]->()){0,1} (c) RETURN *",
      s"MATCH (b) (()-[r]->()){0,1} (c) WHERE ${unique("r")} RETURN *"
    )
  }

  test("uniqueness check is done for a single length-two QPP") {
    assertRewrite(
      "MATCH (b) (()-[r1]->()-[r2]->()){0,1} (c) RETURN *",
      s"MATCH (b) (()-[r1]->()-[r2]->() WHERE NOT (r2 = r1)){0,1} (c) WHERE ${unique("r1 + r2")} RETURN *"
    )
  }

  test("uniqueness check is done for a single length-three QPP") {
    assertRewrite(
      "MATCH (b) (()-[r1]->()-[r2]->()-[r3]->()){0,1} (c) RETURN *",
      s"""MATCH (b) (()-[r1]->()-[r2]->()-[r3]->() WHERE NOT(r3 = r2) AND NOT(r3 = r1) AND NOT(r2 = r1)){0,1} (c)
         |WHERE ${unique("r1 + r2 + r3")}
         |RETURN *""".stripMargin
    )
  }

  test("uniqueness check is done between relationships of simple lengths and QPPs") {
    assertRewrite(
      "MATCH (a)-[r1]->(b) (()-[r2]->())* RETURN *",
      s"MATCH (a)-[r1]->(b) (()-[r2]->())* WHERE NOT r1 IN r2 AND ${unique("r2")} RETURN *"
    )

    assertRewrite(
      "MATCH (c) WHERE EXISTS { MATCH (a)-[r1]->(b) (()-[r2]->())* RETURN 1 } RETURN *",
      s"MATCH (c) WHERE EXISTS { MATCH (a)-[r1]->(b) (()-[r2]->())* WHERE NOT r1 IN r2 AND ${unique("r2")} RETURN 1 } RETURN *"
    )

    assertRewrite(
      "MATCH (c) WHERE COUNT { MATCH (a)-[r1]->(b) (()-[r2]->())* RETURN 1 } > 2 RETURN *",
      s"MATCH (c) WHERE COUNT { MATCH (a)-[r1]->(b) (()-[r2]->())* WHERE NOT r1 IN r2 AND ${unique("r2")} RETURN 1 } > 2 RETURN *"
    )

    assertRewrite(
      "MATCH (a)-[r1]->(b) (()-[r2]->()-[r3]->())* RETURN *",
      s"""MATCH (a)-[r1]->(b) (()-[r2]->()-[r3]->() WHERE NOT (r3 = r2))*
         |WHERE NOT r1 IN (r2 + r3) AND ${unique("r2 + r3")}
         |RETURN *""".stripMargin
    )

    assertRewrite(
      "MATCH ((a)-[r1]->())* (b)-[r2]->(c) RETURN *",
      s"MATCH ((a)-[r1]->())* (b)-[r2]->(c) WHERE NOT r2 IN r1 AND ${unique("r1")} RETURN *"
    )

    assertRewrite(
      "MATCH (a)-[r1]->(b) (()-[r2]->()-[r3]->())* (c)-[r4]->(d) RETURN *",
      s"""MATCH (a)-[r1]->(b) (()-[r2]->()-[r3]->() WHERE NOT (r3 = r2))* (c)-[r4]->(d)
         |WHERE NOT r1 IN (r2 + r3) AND NOT r1 = r4 AND NOT r4 IN (r2 + r3) AND ${unique("r2 + r3")}
         |RETURN *""".stripMargin
    )
  }

  test("no uniqueness check between relationships of simple lengths and QPPs of different type") {
    assertRewrite(
      "MATCH (a)-[r1:R1]->(b) (()-[r2:R2]->())* RETURN *",
      s"MATCH (a)-[r1:R1]->(b) (()-[r2:R2]->())* WHERE ${unique("r2")} RETURN *"
    )

    assertRewrite(
      "MATCH (a)-[r1:R1]->(b) (()-[r2:R2]->()-[r3:R3]->())* RETURN *",
      s"""MATCH (a)-[r1:R1]->(b) (()-[r2:R2]->()-[r3:R3]->())*
         |WHERE ${unique("r2 + r3")}
         |RETURN *""".stripMargin
    )

    assertRewrite(
      "MATCH (a)-[r1:R1]->(b) (()-[r2:R2]->()-[r3:R1]->())* RETURN *",
      s"""MATCH (a)-[r1:R1]->(b) (()-[r2:R2]->()-[r3:R1]->())*
         |WHERE NOT r1 IN r3 AND ${unique("r2 + r3")}
         |RETURN *""".stripMargin
    )

    assertRewrite(
      "MATCH (a)-[r1:R1]->(b) (()-[r2:R2]->())* (()-[r3:R1]->())* RETURN *",
      s"""MATCH (a)-[r1:R1]->(b) (()-[r2:R2]->())* (()-[r3:R1]->())*
         |WHERE NOT r1 IN r3 AND ${unique("r2")} AND ${unique("r3", 2)}
         |RETURN *""".stripMargin
    )
  }

  test("uniqueness check is done between QPPs and QPPs") {
    assertRewrite(
      "MATCH (()-[r1]->())+ (()-[r2]->())+ RETURN *",
      s"""MATCH (()-[r1]->())+ (()-[r2]->())+
         |WHERE ${disjoint("r1", "r2")} AND ${unique("r1", 1)} AND ${unique("r2", 3)}
         |RETURN *""".stripMargin
    )

    assertRewrite(
      "MATCH (()-[r1]->()-[r2]->())+ (()-[r3]->()-[r4]->())+ RETURN *",
      s"""MATCH (()-[r1]->()-[r2]->() WHERE NOT (r2 = r1))+ (()-[r3]->()-[r4]->() WHERE NOT (r4 = r3))+
         |WHERE ${disjoint("r1 + r2", "r3 + r4")} AND ${unique("r1 + r2", 1)} AND ${unique("r3 + r4", 3)}
         |RETURN *""".stripMargin
    )
  }

  test("no uniqueness check between QPPs and QPPs of different type") {
    assertRewrite(
      "MATCH (()-[r1:R1]->())+ (()-[r2:R2]->())+ RETURN *",
      s"""MATCH (()-[r1:R1]->())+ (()-[r2:R2]->())+
         |WHERE ${unique("r1")} AND ${unique("r2", 2)}
         |RETURN *""".stripMargin
    )

    // Here there is no overlap between the first and the second QPP, so no need for a disjoint.
    assertRewrite(
      "MATCH (()-[r1:R1]->()-[r2:R2]->())+ (()-[r3:R3]->()-[r4:R4]->())+ RETURN *",
      s"""MATCH (()-[r1:R1]->()-[r2:R2]->())+ (()-[r3:R3]->()-[r4:R4]->())+
         |WHERE ${unique("r1 + r2")} AND ${unique("r3 + r4", 2)}
         |RETURN *""".stripMargin
    )

    // Here relationships overlap pairwise.
    // But since the trail operator puts everything into one big set anyway, we put all relationships in disjoint.
    assertRewrite(
      "MATCH (()-[r1:R1]->()-[r2:R2]->())+ (()-[r3:R1]->()-[r4:R2]->())+ RETURN *",
      s"""MATCH (()-[r1:R1]->()-[r2:R2]->())+ (()-[r3:R1]->()-[r4:R2]->())+
         |WHERE ${disjoint("r1 + r2", "r3 + r4")} AND ${unique("r1 + r2", 1)} AND ${unique("r3 + r4", 3)}
         |RETURN *""".stripMargin
    )

    // Here some relationships overlap.
    assertRewrite(
      "MATCH (()-[r1:R1]->()-[r2:R2]->())+ (()-[r3:R1]->()-[r4:R4]->())+ RETURN *",
      s"""MATCH (()-[r1:R1]->()-[r2:R2]->())+ (()-[r3:R1]->()-[r4:R4]->())+
         |WHERE ${disjoint("r1", "r3")} AND ${unique("r1 + r2", 1)} AND ${unique("r3 + r4", 3)}
         |RETURN *""".stripMargin
    )
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
      "MATCH (a)-[r1]->(b), shortestPath((a)-[r*]->(b)) RETURN *",
      "MATCH (a)-[r1]->(b), shortestPath((a)-[r*]->(b)) RETURN *"
    )

    assertRewrite(
      "MATCH (a)-[r1]->(b)-[r2]->(c), shortestPath((a)-[r*]->(b)) RETURN *",
      "MATCH (a)-[r1]->(b)-[r2]->(c), shortestPath((a)-[r*]->(b)) WHERE not(r2 = r1) RETURN *"
    )
  }

  test("ignores allShortestPaths relationships for uniqueness") {
    assertRewrite(
      "MATCH (a)-[r1]->(b), allShortestPaths((a)-[r*]->(b)) RETURN *",
      "MATCH (a)-[r1]->(b), allShortestPaths((a)-[r*]->(b)) RETURN *"
    )

    assertRewrite(
      "MATCH (a)-[r1]->(b)-[r2]->(c), allShortestPaths((a)-[r*]->(b)) RETURN *",
      "MATCH (a)-[r1]->(b)-[r2]->(c), allShortestPaths((a)-[r*]->(b)) WHERE not(r2 = r1) RETURN *"
    )
  }

  test("add uniqueness predicates inside a parenthesized path pattern used under a path selector") {
    assertRewrite(
      originalQuery = "MATCH ANY SHORTEST ((a:A)-[r:R]->+(b:B) WHERE b.prop IS NOT NULL) RETURN *",
      expectedQuery = s"MATCH ANY SHORTEST ((a:A)-[r:R]->+(b:B) WHERE b.prop IS NOT NULL AND ${unique("r")}) RETURN *"
    )
  }

  test("introduce parentheses to add uniqueness predicates under a path selector") {
    assertRewrite(
      originalQuery = "MATCH ANY SHORTEST (a:A)-[r:R]->+(b:B) WHERE b.prop IS NOT NULL RETURN *",
      expectedQuery = s"MATCH ANY SHORTEST ((a:A)-[r:R]->+(b:B) WHERE ${unique("r")}) WHERE b.prop IS NOT NULL RETURN *"
    )
  }

  test("should not introduce unnecessary parentheses if there's no predicate to add under a selector") {
    assertIsNotRewritten("MATCH ANY SHORTEST (a) RETURN *")
  }

  def rewriterUnderTest: Rewriter = inSequence(
    AddUniquenessPredicates.rewriter,
    UniquenessRewriter(new AnonymousVariableNameGenerator)
  )
}

class AddUniquenessPredicatesPropertyTest extends CypherFunSuite with CypherScalaCheckDrivenPropertyChecks
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

  // This test was used for a quick and unreliable benchmark of the performance of isAlwaysDifferentFrom,
  // to answer the question if it is worth it to shortcut the calculation above certain sizes and always return
  // false. It is left here for reference
  ignore("isAlwaysDifferentFrom performance") {
    // This is more accurate than le.flatten.size, which does not count wildcards or non-leaves.
    def size(le: LabelExpression): Long = le match {
      case le: BinaryLabelExpression        => size(le.lhs) + size(le.rhs) + 1
      case le: MultiOperatorLabelExpression => le.children.map(size).sum + 1
      case Negation(e, _)                   => 1 + size(e)
      case Wildcard(_)                      => 1
      case Leaf(_, _)                       => 1
    }

    var seed = org.scalacheck.rng.Seed.random()
    val exps = for (i <- Seq(1, 10, 100, 1000, 10_000, 100_000, 1_000_000, 10_000_000)) yield {
      val exp1 = genLabelExpression(Gen.Parameters.default.withSize(i), seed).getOrElse(
        Wildcard()(position)
      )
      seed = seed.next
      val exp2 = genLabelExpression(Gen.Parameters.default.withSize(i), seed).getOrElse(
        Wildcard()(position)
      )
      seed = seed.next

      (exp1, exp2)
    }

    for {
      (exp1, exp2) <- exps
    } {
      val actualSizes = s"{${size(exp1)},${size(exp2)}} / {${exp1.flatten.size},${exp2.flatten.size}}"
      val sr1 = SingleRelationship(Variable("v1")(position), Some(exp1))
      val sr2 = SingleRelationship(Variable("v2")(position), Some(exp2))

      {
        val t0 = System.nanoTime()
        sr1.isAlwaysDifferentFrom(sr2)
        val t1 = System.nanoTime()
        val elapsed = t1 - t0
        println(s"$actualSizes isAlwaysDifferentFrom Elapsed time: " + elapsed + " ns")
      }

      {
        val t0 = System.nanoTime()
        exp1.flatten.size
        val t1 = System.nanoTime()
        val elapsed = t1 - t0
        println(s"$actualSizes flatten.size Elapsed time: " + elapsed + " ns")
      }
      println()
    }
  }
}

trait RelationshipTypeExpressionGenerators {

  /**
   * Finite (small) set of names used to build arbitrary relationship type expressions.
   * It's all Greek to me.
   * Keeping it small and hard-coded ensures that the expressions will contain overlaps
   */
  private val names = Set("ALPHA", "BETA", "GAMMA", "DELTA", "EPSILON")

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
      AddUniquenessPredicates.overlaps(allTypes, Some(value), Some(other.value))
    }

    def unary_! : RelationshipTypeExpression =
      RelationshipTypeExpression(Negation(value)(position))

    def and(other: RelationshipTypeExpression): RelationshipTypeExpression =
      RelationshipTypeExpression(Conjunctions(Seq(value, other.value))(position))

    def or(other: RelationshipTypeExpression): RelationshipTypeExpression =
      RelationshipTypeExpression(Disjunctions(Seq(value, other.value))(position))
  }

  val wildcard: RelationshipTypeExpression = RelationshipTypeExpression(Wildcard()(position))

  val genWildCard: Gen[Wildcard] = Gen.const(Wildcard()(position))

  val genRelType: Gen[Leaf] =
    Gen.oneOf(names.toSeq).map(name => Leaf(RelTypeName(name)(position)))

  def genBinary[A](f: (LabelExpression, LabelExpression) => A): Gen[A] =
    Gen.sized(size =>
      for {
        lhs <- Gen.resize((size - 1) / 2, genLabelExpression)
        rhs <- Gen.resize((size - 1) / 2, genLabelExpression)
      } yield f(lhs, rhs)
    )

  val genConjunction: Gen[Conjunctions] =
    genBinary((lhs, rhs) => Conjunctions(Seq(lhs, rhs))(position))

  val genColonConjunction: Gen[ColonConjunction] =
    genBinary((lhs, rhs) => ColonConjunction(lhs, rhs)(position))

  val genDisjunction: Gen[Disjunctions] =
    genBinary((lhs, rhs) => Disjunctions(Seq(lhs, rhs))(position))

  val genColonDisjunction: Gen[ColonDisjunction] =
    genBinary((lhs, rhs) => ColonDisjunction(lhs, rhs)(position))

  val genNegation: Gen[Negation] =
    Gen.sized(size => Gen.resize(size - 1, genLabelExpression)).map(Negation(_)(position))

  val genLabelExpression: Gen[LabelExpression] =
    Gen.sized(size =>
      if (size <= 1)
        Gen.frequency(
          1 -> genWildCard,
          names.size -> genRelType
        )
      else
        Gen.oneOf(
          genConjunction,
          genColonConjunction,
          genDisjunction,
          genColonDisjunction,
          genNegation
        )
    )

  implicit val arbitraryRelationshipTypeExpression: Arbitrary[RelationshipTypeExpression] =
    Arbitrary(genLabelExpression.map(RelationshipTypeExpression))
}
