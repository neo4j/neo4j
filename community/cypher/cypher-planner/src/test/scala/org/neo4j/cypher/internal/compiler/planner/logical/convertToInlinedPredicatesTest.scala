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
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.compiler.CypherPlannerTestSuite
import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanBuilder
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.SemanticDirection.BOTH
import org.neo4j.cypher.internal.expressions.SemanticDirection.INCOMING
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.logical.plans.Expand.VariablePredicate
import org.neo4j.cypher.internal.logical.plans.NestedPlanExpression
import org.neo4j.cypher.internal.runtime.ast.TraversalEndpoint
import org.neo4j.cypher.internal.runtime.ast.TraversalEndpoint.Endpoint.From
import org.neo4j.cypher.internal.runtime.ast.TraversalEndpoint.Endpoint.To
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.Repetition
import org.neo4j.cypher.internal.util.UpperBound.Unlimited
import org.scalatest.OptionValues

class convertToInlinedPredicatesTest extends CypherPlannerTestSuite with AstConstructionTestSupport with OptionValues {

  private val outerStart = v"outerStart"
  private val innerStart = v"innerStart"
  private val innerEnd = v"innerEnd"
  private val outerEnd = v"outerEnd"
  private val rel = v"rel"

  private object TO {

    object prop {
      def equalsOne(tempVar: Variable): Expression = traversalEndpointPropEqualsOne(tempVar, To)
    }
  }

  private object FROM {

    object prop {
      def equalsOne(tempVar: Variable): Expression = traversalEndpointPropEqualsOne(tempVar, From)
    }
  }

  private def traversalEndpointPropEqualsOne(tempVar: Variable, endpoint: TraversalEndpoint.Endpoint): Expression = {
    equals(
      propExpression(TraversalEndpoint(tempVar, endpoint), "prop"),
      literalInt(1)
    )
  }

  private def rewrite(
    innerPredicates: Seq[Expression] = Seq.empty,
    minRep: Int = 0,
    direction: SemanticDirection = BOTH,
    mode: convertToInlinedPredicates.Mode = convertToInlinedPredicates.Mode.Repeat
  ): Option[InlinedPredicates] = {
    convertToInlinedPredicates(
      outerStart,
      innerStart,
      innerEnd,
      outerEnd,
      rel,
      predicatesToInline = innerPredicates,
      pathRepetition = Repetition(min = minRep, max = Unlimited),
      pathDirection = direction,
      mode = mode,
      anonymousVariableNameGenerator = new AnonymousVariableNameGenerator()
    )
  }

  test("Rewrites (outerStart{prop:1})((innerStart)--(innerEnd))+(outerEnd{prop:1})") {
    rewrite(minRep = 1).value shouldBe InlinedPredicates()
  }

  test("Rewrites (outerStart)((innerStart{prop:1})--(innerEnd{prop:1}))+(outerEnd)") {
    rewrite(
      innerPredicates = Seq(propEquality("innerStart", "prop", 1), propEquality("innerEnd", "prop", 1)),
      minRep = 1
    ).value shouldEqual InlinedPredicates(nodePredicates =
      Seq(VariablePredicate(v"  UNNAMED0", propEquality("  UNNAMED0", "prop", 1)))
    )
  }

  test("Rewrites (outerStart)((innerStart{prop:1})--(innerEnd))+(outerEnd{prop:1})") {
    rewrite(
      innerPredicates = Seq(propEquality("innerStart", "prop", 1)),
      minRep = 1
    ).value shouldEqual InlinedPredicates(relationshipPredicates =
      Seq(VariablePredicate(v"  UNNAMED1", FROM.prop.equalsOne(v"  UNNAMED2")))
    )
  }

  test("Rewrites (outerStart)((innerStart{prop:1})--(innerEnd))*(outerEnd{prop:1})") {
    rewrite(
      innerPredicates = Seq(propEquality("innerStart", "prop", 1))
    ).value shouldEqual InlinedPredicates(relationshipPredicates =
      Seq(VariablePredicate(v"  UNNAMED1", FROM.prop.equalsOne(v"  UNNAMED2")))
    )
  }

  test("Rewrites (outerStart{prop:1})((innerStart)--(innerEnd{prop:1}))*(outerEnd)") {
    rewrite(
      innerPredicates = Seq(propEquality("innerEnd", "prop", 1))
    ).value shouldEqual InlinedPredicates(relationshipPredicates =
      Seq(VariablePredicate(v"  UNNAMED1", TO.prop.equalsOne(v"  UNNAMED2")))
    )
  }

  test("Rewrites (outerStart)((innerStart)-[rel{prop:1}]-(innerEnd))*(outerEnd)") {
    rewrite(
      innerPredicates = Seq(propEquality("rel", "prop", 1))
    ).value shouldEqual InlinedPredicates(relationshipPredicates =
      Seq(VariablePredicate(v"  UNNAMED1", propEquality("  UNNAMED1", "prop", 1)))
    )
  }

  test(
    "Rewrite (outerStart)((innerStart)-[rel{prop:1}]->(innerEnd) WHERE innerStart.prop <> innerEnd.prop)*(outerEnd)"
  ) {
    rewrite(
      innerPredicates =
        Seq(propEquality("rel", "prop", 1), notEquals(prop("innerStart", "prop"), prop("innerEnd", "prop"))),
      direction = OUTGOING
    ).value shouldEqual InlinedPredicates(relationshipPredicates =
      Seq(
        VariablePredicate(v"  UNNAMED1", propEquality("  UNNAMED1", "prop", 1)),
        VariablePredicate(
          v"  UNNAMED1",
          notEquals(prop(startNode("  UNNAMED1"), "prop"), prop(endNode("  UNNAMED1"), "prop"))
        )
      )
    )
  }

  test(
    "Rewrite (outerStart)((innerStart)-[rel{prop:1}]->(innerEnd) WHERE innerStart.prop <> innerEnd.prop)+(outerEnd)"
  ) {
    rewrite(
      innerPredicates =
        Seq(propEquality("rel", "prop", 1), notEquals(prop("innerStart", "prop"), prop("innerEnd", "prop"))),
      direction = OUTGOING,
      minRep = 1,
      mode = convertToInlinedPredicates.Mode.Shortest(Seq.empty)
    ).value shouldEqual InlinedPredicates(relationshipPredicates =
      Seq(
        VariablePredicate(v"  UNNAMED1", propEquality("  UNNAMED1", "prop", 1)),
        VariablePredicate(
          v"  UNNAMED1",
          notEquals(prop(startNode("  UNNAMED1"), "prop"), prop(endNode("  UNNAMED1"), "prop"))
        )
      )
    )
  }

  test(
    "Rewrite (outerStart)((innerStart)-[rel{prop:1}]->(innerEnd) WHERE innerStart.prop = 2 AND innerEnd.prop = 3)*(outerEnd)"
  ) {
    rewrite(
      innerPredicates =
        Seq(
          propEquality("rel", "prop", 1),
          propEquality("innerStart", "prop", 2),
          propEquality("innerEnd", "prop", 3)
        ),
      direction = OUTGOING
    ).value shouldEqual InlinedPredicates(relationshipPredicates =
      Seq(
        VariablePredicate(v"  UNNAMED1", propEquality("  UNNAMED1", "prop", 1)),
        VariablePredicate(v"  UNNAMED1", equals(prop(startNode("  UNNAMED1"), "prop"), literalInt(2))),
        VariablePredicate(v"  UNNAMED1", equals(prop(endNode("  UNNAMED1"), "prop"), literalInt(3)))
      )
    )
  }

  test(
    "Rewrites (outerStart)((innerStart)<-[rel{prop:1}]-(innerEnd) WHERE innerStart.prop = 2 AND innerEnd.prop = 3)*(outerEnd)"
  ) {
    rewrite(
      innerPredicates =
        Seq(
          propEquality("rel", "prop", 1),
          propEquality("innerStart", "prop", 2),
          propEquality("innerEnd", "prop", 3)
        ),
      direction = INCOMING
    ).value shouldEqual InlinedPredicates(relationshipPredicates =
      Seq(
        VariablePredicate(v"  UNNAMED1", propEquality("  UNNAMED1", "prop", 1)),
        VariablePredicate(v"  UNNAMED1", equals(prop(endNode("  UNNAMED1"), "prop"), literalInt(2))),
        VariablePredicate(v"  UNNAMED1", equals(prop(startNode("  UNNAMED1"), "prop"), literalInt(3)))
      )
    )
  }

  test(
    "Rewrites (outerStart)((innerStart)-[rel]->(innerEnd) WHERE innerEnd.prop = outerStart.prop)*(outerEnd)"
  ) {
    rewrite(
      innerPredicates =
        Seq(
          equals(prop("innerEnd", "prop"), prop("outerStart", "prop"))
        ),
      direction = OUTGOING
    ).value shouldEqual
      InlinedPredicates(relationshipPredicates =
        Seq(
          VariablePredicate(v"  UNNAMED1", equals(prop(endNode("  UNNAMED1"), "prop"), prop("outerStart", "prop")))
        )
      )
  }

  test(
    "Rewrites (outerStart)((innerStart)-[rel]->(innerEnd) WHERE innerStart.prop = outerEnd.prop)*(outerEnd)"
  ) {
    rewrite(
      innerPredicates =
        Seq(
          equals(prop("innerStart", "prop"), prop("outerEnd", "prop"))
        ),
      direction = OUTGOING
    ).value shouldEqual
      InlinedPredicates(relationshipPredicates =
        Seq(
          VariablePredicate(v"  UNNAMED1", equals(prop(startNode("  UNNAMED1"), "prop"), prop("outerEnd", "prop")))
        )
      )
  }

  test(
    "Rewrites (outerStart)((innerStart)-[rel{prop:1}]->(innerEnd) WHERE innerStart.prop <> innerEnd.prop)+(outerEnd)"
  ) {
    rewrite(
      innerPredicates =
        Seq(propEquality("rel", "prop", 1), notEquals(prop("innerStart", "prop"), prop("innerEnd", "prop"))),
      direction = OUTGOING,
      minRep = 1,
      mode = convertToInlinedPredicates.Mode.Shortest(Seq.empty)
    ).value shouldEqual
      InlinedPredicates(relationshipPredicates =
        Seq(
          VariablePredicate(v"  UNNAMED1", propEquality("  UNNAMED1", "prop", 1)),
          VariablePredicate(
            v"  UNNAMED1",
            notEquals(prop(startNode("  UNNAMED1"), "prop"), prop(endNode("  UNNAMED1"), "prop"))
          )
        )
      )
  }

  test("Rewrite (outerStart{prop:1})((innerStart{prop:1})--(innerEnd{prop:1}))*(outerEnd)") {
    rewrite(
      innerPredicates =
        Seq(propEquality("innerStart", "prop", 1), propEquality("innerEnd", "prop", 1))
    ).value shouldEqual
      InlinedPredicates(relationshipPredicates =
        Seq(
          VariablePredicate(v"  UNNAMED1", FROM.prop.equalsOne(v"  UNNAMED2")),
          VariablePredicate(v"  UNNAMED1", TO.prop.equalsOne(v"  UNNAMED3"))
        )
      )
  }

  test(
    "Rewrite (outerStart)((innerStart{prop:1})--(innerEnd{prop:1}))*(outerEnd{prop:1})"
  ) {
    rewrite(
      innerPredicates =
        Seq(propEquality("innerStart", "prop", 1), propEquality("innerEnd", "prop", 1))
    ).value shouldEqual
      InlinedPredicates(relationshipPredicates =
        Seq(
          VariablePredicate(v"  UNNAMED1", FROM.prop.equalsOne(v"  UNNAMED2")),
          VariablePredicate(v"  UNNAMED1", TO.prop.equalsOne(v"  UNNAMED3"))
        )
      )
  }

  test(
    "Rewrite (outerStart)((innerStart{prop:1})--(innerEnd{prop:1}))*(outerEnd)"
  ) {
    rewrite(
      innerPredicates = Seq(propEquality("innerStart", "prop", 1), propEquality("innerEnd", "prop", 1))
    ).value shouldBe InlinedPredicates(relationshipPredicates =
      Seq(
        VariablePredicate(v"  UNNAMED1", FROM.prop.equalsOne(v"  UNNAMED2")),
        VariablePredicate(v"  UNNAMED1", TO.prop.equalsOne(v"  UNNAMED3"))
      )
    )
  }

  test(
    "Rewrite (outerStart)((innerStart)--(innerEnd{prop:1}))+(outerEnd{prop:1})"
  ) {
    rewrite(
      innerPredicates = Seq(propEquality("innerEnd", "prop", 1)),
      minRep = 1
    ).value shouldBe InlinedPredicates(relationshipPredicates =
      Seq(VariablePredicate(v"  UNNAMED1", TO.prop.equalsOne(v"  UNNAMED2")))
    )
  }

  test(
    "Rewrite (outerStart{prop: 1})((innerStart{prop:1})--(innerEnd))*(outerEnd)"
  ) {
    rewrite(
      innerPredicates = Seq(propEquality("innerStart", "prop", 1))
    ).value shouldBe InlinedPredicates(relationshipPredicates =
      Seq(VariablePredicate(v"  UNNAMED1", FROM.prop.equalsOne(v"  UNNAMED2")))
    )
  }

  test(
    "Rewrite (outerStart)((innerStart)-[rel{prop:1}]-(innerEnd) WHERE innerStart.prop <> innerEnd.prop)*(outerEnd)"
  ) {
    rewrite(
      innerPredicates =
        Seq(propEquality("rel", "prop", 1), notEquals(prop("innerStart", "prop"), prop("innerEnd", "prop")))
    ).value shouldBe InlinedPredicates(relationshipPredicates =
      Seq(
        VariablePredicate(v"  UNNAMED1", propEquality("  UNNAMED1", "prop", 1)),
        VariablePredicate(
          v"  UNNAMED1",
          notEquals(
            propExpression(TraversalEndpoint(v"  UNNAMED2", From), "prop"),
            propExpression(TraversalEndpoint(v"  UNNAMED3", To), "prop")
          )
        )
      )
    )
  }

  test(
    "Should rewrite (outerStart)((innerStart)-[rel{prop:1}]->(innerEnd) WHERE NOT exists((innerEnd)--(innerEnd)))+(outerEnd)"
  ) {
    rewrite(
      innerPredicates =
        Seq(
          propEquality("rel", "prop", 1),
          not(exists(patternExpression(innerEnd, innerEnd).withComputedIntroducedVariables(
            Set()
          ).withComputedScopeDependencies(Set(innerEnd))))
        ),
      direction = OUTGOING,
      minRep = 1,
      mode = convertToInlinedPredicates.Mode.Shortest(Seq.empty)
    ) shouldBe None
  }

  test(
    "(outerStart)((innerStart)-[rel{prop:1}]->(innerEnd) WHERE unknownVar.prop= 3)*(outerEnd)"
  ) {
    rewrite(
      innerPredicates =
        Seq(
          propEquality("rel", "prop", 1),
          propEquality("unknownVar", "prop", 3)
        ),
      direction = OUTGOING
    ) shouldBe None
  }

  test(
    "Rewrite (outerStart)((innerStart{prop:1})-[r]-(innerEnd))+(outerEnd) or (outerEnd)((innerEnd)-[r]-(innerStart{prop:1}))+(outerStart)"
  ) {
    rewrite(
      innerPredicates = Seq(propEquality("innerStart", "prop", 1)),
      minRep = 1
    ).value shouldEqual InlinedPredicates(relationshipPredicates =
      Seq(VariablePredicate(v"  UNNAMED1", FROM.prop.equalsOne(v"  UNNAMED2")))
    )
  }

  test(
    "Rewrite (outerStart)((innerStart)-[r]-(innerEnd{prop:1}))+(outerEnd) or (outerEnd)((innerEnd{prop:1})-[r]-(innerStart))+(outerStart)"
  ) {
    rewrite(
      innerPredicates = Seq(propEquality("innerEnd", "prop", 1)),
      minRep = 1
    ).value shouldEqual InlinedPredicates(relationshipPredicates =
      Seq(VariablePredicate(v"  UNNAMED1", TO.prop.equalsOne(v"  UNNAMED2")))
    )
  }

  test("should not rewrite nested plan expressions") {
    val expr = NestedPlanExpression.exists(
      new LogicalPlanBuilder(wholePlan = false).argument(innerStart.name).build(),
      v"x"
    )(pos)

    rewrite(
      innerPredicates = Seq(expr),
      minRep = 1
    ) shouldBe None
  }
}
