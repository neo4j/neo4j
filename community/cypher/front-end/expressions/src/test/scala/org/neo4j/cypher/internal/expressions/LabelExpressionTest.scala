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
package org.neo4j.cypher.internal.expressions

import org.neo4j.cypher.internal.label_expressions.LabelExpression
import org.neo4j.cypher.internal.label_expressions.LabelExpression.ColonConjunction
import org.neo4j.cypher.internal.label_expressions.LabelExpression.ColonDisjunction
import org.neo4j.cypher.internal.label_expressions.LabelExpression.Conjunctions
import org.neo4j.cypher.internal.label_expressions.LabelExpression.Disjunctions
import org.neo4j.cypher.internal.label_expressions.LabelExpression.Leaf
import org.neo4j.cypher.internal.label_expressions.LabelExpression.Negation
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class LabelExpressionTest extends CypherFunSuite {

  private val pos = InputPosition.NONE

  private def longDisjunction: LabelExpression = {
    val leaf = Leaf(LabelName("A")(pos))
    Disjunctions(Vector.fill(10000)(leaf))(pos)
  }

  private def longConjunction: LabelExpression = {
    val leaf = Leaf(LabelName("A")(pos))
    Conjunctions(Vector.fill(10000)(leaf))(pos)
  }

  test("disjunction hashCode should not stackoverflow") {
    noException should be thrownBy longDisjunction.hashCode()
  }

  test("disjunction flatten should not stackoverflow") {
    noException should be thrownBy {
      longDisjunction.flatten shouldBe Seq.fill(10000)(LabelName("A")(pos))
    }
  }

  test("disjunction containsGpmSpecificLabelExpression should not stackoverflow") {
    noException should be thrownBy {
      longDisjunction.containsGpmSpecificLabelExpression shouldBe true
    }
  }

  test("disjunction containsGpmSpecificRelTypeExpression should not stackoverflow") {
    noException should be thrownBy {
      longDisjunction.containsGpmSpecificRelTypeExpression shouldBe false
    }
  }

  test("conjunction hashCode should not stackoverflow") {
    noException should be thrownBy longConjunction.hashCode()
  }

  test("conjunction flatten should not stackoverflow") {
    noException should be thrownBy {
      longConjunction.flatten shouldBe Seq.fill(10000)(LabelName("A")(pos))
    }
  }

  test("conjunction containsGpmSpecificLabelExpression should not stackoverflow") {
    noException should be thrownBy {
      longConjunction.containsGpmSpecificLabelExpression shouldBe true
    }
  }

  test("conjunction containsGpmSpecificRelTypeExpression should not stackoverflow") {
    noException should be thrownBy {
      longConjunction.containsGpmSpecificRelTypeExpression shouldBe true
    }
  }

  test("should replace A:A&A with a flat conjunction") {
    val a = Leaf(LabelName("A")(pos))
    val b = Leaf(LabelName("B")(pos))
    val c = Leaf(LabelName("C")(pos))
    val expr = Conjunctions.flat(a, ColonConjunction(b, c)(pos), pos, containsIs = false)
    expr.replaceColonSyntax shouldBe Conjunctions(Seq(a, b, c))(pos)
  }

  test("should replace A|A|:A with a flat disjunction") {
    val a = Leaf(LabelName("A")(pos))
    val b = Leaf(LabelName("B")(pos))
    val c = Leaf(LabelName("C")(pos))
    val expr = Disjunctions.flat(a, ColonDisjunction(b, c)(pos), pos, containsIs = false)
    expr.replaceColonSyntax shouldBe Disjunctions(Seq(a, b, c))(pos)
  }

  test("flatten on many consecutive negations should not stackoverflow") {
    val expr = (1 to 20000).foldLeft[LabelExpression](Leaf(LabelName("A")(pos))) {
      case (expr, _) => Negation(expr)(pos)
    }
    noException should be thrownBy {
      expr.flatten shouldBe Seq(LabelName("A")(pos))
    }
  }
}
