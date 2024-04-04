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
package org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanBuilder
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.TrailToVarExpandRewriterTest.StubCardinalities
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.TrailToVarExpandRewriterTest.StubProvidedOrders
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.TrailToVarExpandRewriterTest.StubSolveds
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.ir.NodeBinding
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.QuantifiedPathPattern
import org.neo4j.cypher.internal.ir.Selections
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.ir.ast.ForAllRepetitions
import org.neo4j.cypher.internal.logical.plans.NestedPlanExistsExpression
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.NonEmptyList
import org.neo4j.cypher.internal.util.Repetition
import org.neo4j.cypher.internal.util.UpperBound
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.attribution.SameId
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class ForAllRepetitionsPredicateRewriterTest extends CypherFunSuite with AstConstructionTestSupport {

  // (start) ((x)-[r]->(y)-[q]->(z))* (end)
  private val qpp = QuantifiedPathPattern(
    leftBinding = NodeBinding(v"x", v"start"),
    rightBinding = NodeBinding(v"z", v"end"),
    patternRelationships = NonEmptyList(
      PatternRelationship(
        v"r",
        (v"x", v"y"),
        SemanticDirection.OUTGOING,
        Seq.empty,
        SimplePatternLength
      ),
      PatternRelationship(
        v"q",
        (v"y", v"z"),
        SemanticDirection.OUTGOING,
        Seq.empty,
        SimplePatternLength
      )
    ),
    argumentIds = Set(v"argument"),
    selections = Selections.empty,
    repetition = Repetition(0, UpperBound.Unlimited),
    nodeVariableGroupings = Set(
      variableGrouping(v"x", v"xGroup"),
      variableGrouping(v"y", v"yGroup"),
      variableGrouping(v"z", v"zGroup")
    ),
    relationshipVariableGroupings = Set(
      variableGrouping(v"r", v"rGroup"),
      variableGrouping(v"q", v"qGroup")
    )
  )

  private def rangeForGroupVariable(groupVar: String): FunctionInvocation = {
    function(
      "range",
      literalInt(0),
      subtract(
        size(varFor(groupVar)),
        literalInt(1)
      )
    )
  }

  test("predicate with singleton variable") {
    val predicate = greaterThan(prop("z", "prop"), prop(v"argument", "prop"))
    val far = ForAllRepetitions(qpp, predicate)

    far.dependencies shouldBe Set(v"zGroup", v"argument")
    val iterVar = v"  UNNAMED0"

    getRewriter.rewriteToAllIterablePredicate(far) shouldBe {
      allInList(
        iterVar,
        rangeForGroupVariable("zGroup"),
        greaterThan(
          prop(
            containerIndex(v"zGroup", iterVar),
            "prop"
          ),
          prop(v"argument", "prop")
        )
      )
    }
  }

  test("predicate with multiple singleton variables") {
    val predicate = greaterThan(prop("z", "prop"), prop("y", "prop"))
    val far = ForAllRepetitions(qpp, predicate)

    far.dependencies shouldBe Set(v"zGroup", v"yGroup")
    val iterVar = v"  UNNAMED0"

    getRewriter.rewriteToAllIterablePredicate(far) shouldBe {
      allInList(
        iterVar,
        rangeForGroupVariable("yGroup"),
        greaterThan(
          prop(
            containerIndex(v"zGroup", iterVar),
            "prop"
          ),
          prop(
            containerIndex(v"yGroup", iterVar),
            "prop"
          )
        )
      )
    }
  }

  test("predicate without dependencies") {
    val predicate = greaterThan(literal(123), parameter("param", CTAny))
    val far = ForAllRepetitions(qpp, predicate)

    far.dependencies shouldBe Set(v"qGroup")
    val iterVar = v"  UNNAMED0"

    getRewriter.rewriteToAllIterablePredicate(far) shouldBe {
      allInList(iterVar, rangeForGroupVariable("qGroup"), predicate)
    }
  }

  test("anded inequalities predicate") {
    val predicate = andedPropertyInequalities(
      propGreaterThan("z", "prop", 0),
      propLessThan("y", "prop", 10)
    )

    val far = ForAllRepetitions(qpp, predicate)
    val iterVar = v"  UNNAMED0"

    val actual = getRewriter.rewriteToAllIterablePredicate(far)
    actual shouldBe {
      allInList(
        iterVar,
        rangeForGroupVariable("yGroup"),
        ands(
          greaterThan(
            prop(
              containerIndex(v"zGroup", iterVar),
              "prop"
            ),
            literalInt(0)
          ),
          lessThan(
            prop(
              containerIndex(v"yGroup", iterVar),
              "prop"
            ),
            literalInt(10)
          )
        )
      )
    }
  }

  test("nested plan expression") {

    val previousNestedPlan = new LogicalPlanBuilder(wholePlan = false)
      .expandInto("(x)-[s]->(z)")
      .argument("x", "z")
      .build()

    val nestedPlanExpression = NestedPlanExistsExpression(
      plan = previousNestedPlan,
      solvedExpressionAsString = ""
    )(pos)
    val far = ForAllRepetitions(qpp, nestedPlanExpression)

    far.dependencies shouldBe Set(v"xGroup", v"zGroup", v"s")
    val iterVar = v"  UNNAMED0"

    val expectedNestedPlan = new LogicalPlanBuilder(wholePlan = false)
      .apply()
      .|.expandInto("(`  x@1`)-[s]->(`  z@2`)")
      .|.argument("  x@1", "  z@2")
      .projection(s"xGroup[`${iterVar.name}`] AS `  x@1`", s"zGroup[`${iterVar.name}`] AS `  z@2`")
      .argument("xGroup", "zGroup", iterVar.name)
      .build()

    getRewriter.rewriteToAllIterablePredicate(far) shouldBe {
      allInList(iterVar, rangeForGroupVariable("xGroup"), NestedPlanExistsExpression(expectedNestedPlan, "")(pos))
    }
  }

  private def getRewriter =
    ForAllRepetitionsPredicateRewriter(
      new AnonymousVariableNameGenerator,
      new StubSolveds,
      new StubCardinalities,
      new StubProvidedOrders,
      SameId(Id(0))
    )
}
