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
package org.neo4j.cypher.internal.compiler.ast.convert.plannerQuery

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.compiler.CypherPlannerTestSuite
import org.neo4j.cypher.internal.compiler.ast.convert.plannerQuery.LabelExpressionConversion.IllegalLabelExpressionException
import org.neo4j.cypher.internal.compiler.ast.convert.plannerQuery.LabelExpressionConversion.NodeLabelsToCreate
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LabelOrRelTypeName
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.label_expressions.LabelExpression.Leaf
import org.neo4j.cypher.internal.util.symbols.CTAny

class LabelExpressionConversionTest
    extends CypherPlannerTestSuite
    with LabelExpressionConversion
    with AstConstructionTestSupport {

  test("The absence of label expression should be converted to, well, no labels") {
    extractNodeLabelsToCreate(None) shouldEqual NodeLabelsToCreate.empty
  }

  test("(:A) should be converted to the static label { A }") {
    val labelExpression = labelLeaf("A")
    val labelsToCreate = extractNodeLabelsToCreate(Some(labelExpression))
    labelsToCreate shouldEqual buildStaticLabelNames("A")
  }

  test("(IS A) should be converted to the static label { A }") {
    val labelExpression = labelLeaf("A", containsIs = true)
    val labelsToCreate = extractNodeLabelsToCreate(Some(labelExpression))
    labelsToCreate shouldEqual buildStaticLabelNames("A")
  }

  test("LabelOrRelTypeName should throw an exception") {
    val labelExpression = Leaf(LabelOrRelTypeName("A")(pos))
    an[IllegalLabelExpressionException] should be thrownBy extractNodeLabelsToCreate(Some(labelExpression))
  }

  test("RelTypeName should throw an exception") {
    val labelExpression = Leaf(RelTypeName("A")(pos))
    an[IllegalLabelExpressionException] should be thrownBy extractNodeLabelsToCreate(Some(labelExpression))
  }

  test("(:$(['A', 'B', 'C'])) should be converted to the dynamic labels { ['A', 'B', 'C'] }") {
    val abc = listOfString("A", "B", "C")
    val labelExpression = dynamicLabelLeaf(abc)
    val labelsToCreate = extractNodeLabelsToCreate(Some(labelExpression))
    labelsToCreate shouldEqual buildDynamicLabelNames(abc)
  }

  test("(IS $(['A', 'B', 'C'])) should be converted to the dynamic labels { ['A', 'B', 'C'] }") {
    val abc = listOfString("A", "B", "C")
    val labelExpression = dynamicLabelLeaf(abc, containsIs = true)
    val labelsToCreate = extractNodeLabelsToCreate(Some(labelExpression))
    labelsToCreate shouldEqual buildDynamicLabelNames(abc)
  }

  test("(:$any(['A', 'B', 'C'])) should throw an exception – any not allowed") {
    val abc = listOfString("A", "B", "C")
    val labelExpression = dynamicLabelLeaf(abc, all = false)
    an[IllegalLabelExpressionException] should be thrownBy extractNodeLabelsToCreate(Some(labelExpression))
  }

  test("DynamicLabelOrRelTypeExpression should throw an exception") {
    val abc = listOfString("A", "B", "C")
    val labelExpression = dynamicLabelOrRelTypeLeaf(abc)
    an[IllegalLabelExpressionException] should be thrownBy extractNodeLabelsToCreate(Some(labelExpression))
  }

  test("DynamicRelTypeExpression should throw an exception") {
    val abc = listOfString("A", "B", "C")
    val labelExpression = dynamicRelTypeLeaf(abc)
    an[IllegalLabelExpressionException] should be thrownBy extractNodeLabelsToCreate(Some(labelExpression))
  }

  test("(:A:B) should be converted to the static labels { A, B }") {
    val labelExpression = labelColonConjunction(labelLeaf("A"), labelLeaf("B"))
    val labelsToCreate = extractNodeLabelsToCreate(Some(labelExpression))
    labelsToCreate shouldEqual buildStaticLabelNames("A", "B")
  }

  test("(:A:A) should be converted to the static label { A }") {
    val labelExpression = labelColonConjunction(labelLeaf("A"), labelLeaf("A"))
    val labelsToCreate = extractNodeLabelsToCreate(Some(labelExpression))
    labelsToCreate shouldEqual buildStaticLabelNames("A")
  }

  test("(:A:%) should throw an exception – wildcard not allowed") {
    val labelExpression = labelColonConjunction(labelLeaf("A"), labelWildcard())
    an[IllegalLabelExpressionException] should be thrownBy extractNodeLabelsToCreate(Some(labelExpression))
  }

  test(
    "(:A:$(['A', 'B', 'C']):B) should be converted to the static labels { A, B } and dynamic labels { ['A', 'B', 'C'] }"
  ) {
    val abc = listOfString("A", "B", "C")
    val labelExpression =
      labelColonConjunction(
        labelLeaf("A"),
        labelColonConjunction(dynamicLabelLeaf(abc), labelLeaf("B"))
      )

    val labelsToCreate = extractNodeLabelsToCreate(Some(labelExpression))

    labelsToCreate shouldEqual
      buildNodeLabelsToCreate(
        staticLabelNames = Set("A", "B"),
        dynamicLabelNames = Set(abc)
      )
  }

  test("(:A&B) should be converted to the static labels { A, B }") {
    val labelExpression = labelConjunctions(List(labelLeaf("A"), labelLeaf("B")))
    val labelsToCreate = extractNodeLabelsToCreate(Some(labelExpression))
    labelsToCreate shouldEqual buildStaticLabelNames("A", "B")
  }

  test("(IS A&B) should be converted to the static labels { A, B }") {
    val labelExpression = labelConjunctions(List(labelLeaf("A"), labelLeaf("B")), containsIs = true)
    val labelsToCreate = extractNodeLabelsToCreate(Some(labelExpression))
    labelsToCreate shouldEqual buildStaticLabelNames("A", "B")
  }

  test(
    "(:$($labels) & A & $($labels) & B) should be converted to the static labels { A, B } and dynamic labels { $labels }"
  ) {
    val labels = parameter("labels", CTAny)
    val labelExpression =
      labelConjunctions(List(
        dynamicLabelLeaf(labels),
        labelLeaf("A"),
        dynamicLabelLeaf(labels),
        labelLeaf("B")
      ))

    val labelsToCreate = extractNodeLabelsToCreate(Some(labelExpression))

    labelsToCreate shouldEqual
      buildNodeLabelsToCreate(
        staticLabelNames = Set("A", "B"),
        dynamicLabelNames = Set(labels)
      )
  }

  test("(:!$($labels) & A) should throw an exception – negation not allowed") {
    val labelExpression =
      labelConjunctions(List(
        labelNegation(dynamicLabelLeaf(parameter("labels", CTAny))),
        labelLeaf("A")
      ))

    an[IllegalLabelExpressionException] should be thrownBy extractNodeLabelsToCreate(Some(labelExpression))
  }

  test("ColonDisjunction should throw an exception") {
    val labelExpression = labelColonDisjunction(labelLeaf("A"), labelLeaf("B"))
    an[IllegalLabelExpressionException] should be thrownBy extractNodeLabelsToCreate(Some(labelExpression))
  }

  test("(:A | $(['A', 'B', 'C']) | B) should throw an exception – disjunction not allowed") {
    val abc = listOfString("A", "B", "C")
    val labelExpression = labelDisjunctions(List(
      labelLeaf("A"),
      dynamicLabelLeaf(abc),
      labelLeaf("B")
    ))
    an[IllegalLabelExpressionException] should be thrownBy extractNodeLabelsToCreate(Some(labelExpression))
  }

  test("(:!$(['A', 'B', 'C'])) should throw an exception – negation not allowed") {
    val abc = listOfString("A", "B", "C")
    val labelExpression = labelNegation(dynamicLabelLeaf(abc))
    an[IllegalLabelExpressionException] should be thrownBy extractNodeLabelsToCreate(Some(labelExpression))
  }

  test("(:%) should throw an exception – wildcard not allowed") {
    val labelExpression = labelWildcard()
    an[IllegalLabelExpressionException] should be thrownBy extractNodeLabelsToCreate(Some(labelExpression))
  }

  private def buildNodeLabelsToCreate(
    staticLabelNames: Set[String] = Set.empty,
    dynamicLabelNames: Set[Expression] = Set.empty
  ): NodeLabelsToCreate =
    NodeLabelsToCreate(
      staticLabelNames = staticLabelNames.map(name => labelName(name)),
      dynamicLabelNames = dynamicLabelNames
    )

  private def buildStaticLabelNames(labelNames: String*): NodeLabelsToCreate =
    buildNodeLabelsToCreate(staticLabelNames = labelNames.toSet)

  private def buildDynamicLabelNames(expressions: Expression*): NodeLabelsToCreate =
    buildNodeLabelsToCreate(dynamicLabelNames = expressions.toSet)
}
