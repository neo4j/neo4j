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
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.SemanticDirection.BOTH
import org.neo4j.cypher.internal.expressions.SemanticDirection.INCOMING
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.logical.plans.Expand.VariablePredicate
import org.neo4j.cypher.internal.runtime.ast.TraversalEndpoint
import org.neo4j.cypher.internal.runtime.ast.TraversalEndpoint.Endpoint.From
import org.neo4j.cypher.internal.runtime.ast.TraversalEndpoint.Endpoint.To
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.Repetition
import org.neo4j.cypher.internal.util.UpperBound.Unlimited
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class convertToInlinedPredicatesTest extends CypherFunSuite with AstConstructionTestSupport {

  private case class Input(
    innerPredicates: Seq[Expression] = Seq.empty,
    outerPredicates: Seq[Expression] = Seq.empty,
    minRep: Int = 0,
    direction: SemanticDirection = BOTH,
    nodeToRelationshipRewriteOption: NodeToRelationshipRewriteOption = ApplyNodeToRelationshipRewriter
  )

  private val outerStart = v"outerStart"
  private val innerStart = v"innerStart"
  private val innerEnd = v"innerEnd"
  private val outerEnd = v"outerEnd"
  private val rel = v"rel"

  private def rewrite(input: Input): Option[InlinedPredicates] = convertToInlinedPredicates(
    outerStart,
    innerStart,
    innerEnd,
    outerEnd,
    rel,
    predicatesToInline = input.innerPredicates,
    pathRepetition = Repetition(min = input.minRep, max = Unlimited),
    pathDirection = input.direction,
    predicatesOutsideRepetition = input.outerPredicates,
    anonymousVariableNameGenerator = new AnonymousVariableNameGenerator(),
    nodeToRelationshipRewriteOption = input.nodeToRelationshipRewriteOption
  )

  test("Rewrites (outerStart{prop:1})((innerStart)--(innerEnd))+(outerEnd{prop:1})") {
    rewrite(Input(
      outerPredicates = Seq(propEquality("outerStart", "prop", 1), propEquality("outerEnd", "prop", 1)),
      minRep = 1
    )) shouldBe Some(InlinedPredicates())
  }

  test("Rewrites (outerStart)((innerStart{prop:1})--(innerEnd{prop:1}))+(outerEnd)") {
    rewrite(Input(
      innerPredicates = Seq(propEquality("innerStart", "prop", 1), propEquality("innerEnd", "prop", 1)),
      minRep = 1
    )) shouldEqual Some(InlinedPredicates(nodePredicates =
      Seq(VariablePredicate(v"  UNNAMED0", propEquality("  UNNAMED0", "prop", 1)))
    ))
  }

  test("Rewrites (outerStart)((innerStart{prop:1})--(innerEnd))+(outerEnd{prop:1})") {
    rewrite(Input(
      innerPredicates = Seq(propEquality("innerStart", "prop", 1)),
      outerPredicates = Seq(propEquality("outerEnd", "prop", 1)),
      minRep = 1
    )) shouldEqual Some(InlinedPredicates(nodePredicates =
      Seq(VariablePredicate(v"  UNNAMED0", propEquality("  UNNAMED0", "prop", 1)))
    ))
  }

  test("Rewrites (outerStart)((innerStart{prop:1})--(innerEnd))*(outerEnd{prop:1})") {
    rewrite(Input(
      innerPredicates = Seq(propEquality("innerStart", "prop", 1)),
      outerPredicates = Seq(propEquality("outerEnd", "prop", 1))
    )) shouldEqual Some(InlinedPredicates(nodePredicates =
      Seq(VariablePredicate(v"  UNNAMED0", propEquality("  UNNAMED0", "prop", 1)))
    ))
  }

  test("Rewrites (outerStart{prop:1})((innerStart)--(innerEnd{prop:1}))*(outerEnd)") {
    rewrite(Input(
      innerPredicates = Seq(propEquality("innerEnd", "prop", 1)),
      outerPredicates = Seq(propEquality("outerStart", "prop", 1))
    )) shouldEqual Some(InlinedPredicates(nodePredicates =
      Seq(VariablePredicate(v"  UNNAMED0", propEquality("  UNNAMED0", "prop", 1)))
    ))
  }

  test("Rewrites (outerStart)((innerStart)-[rel{prop:1}]-(innerEnd))*(outerEnd)") {
    rewrite(Input(
      innerPredicates = Seq(propEquality("rel", "prop", 1))
    )) shouldEqual
      Some(InlinedPredicates(relationshipPredicates =
        Seq(VariablePredicate(v"  UNNAMED1", propEquality("  UNNAMED1", "prop", 1)))
      ))
  }

  test(
    "Rewrite (outerStart)((innerStart)-[rel{prop:1}]->(innerEnd) WHERE innerStart.prop <> innerEnd.prop)*(outerEnd)"
  ) {
    rewrite(Input(
      innerPredicates =
        Seq(propEquality("rel", "prop", 1), notEquals(prop("innerStart", "prop"), prop("innerEnd", "prop"))),
      direction = OUTGOING
    )) shouldEqual
      Some(InlinedPredicates(relationshipPredicates =
        Seq(
          VariablePredicate(v"  UNNAMED1", propEquality("  UNNAMED1", "prop", 1)),
          VariablePredicate(
            v"  UNNAMED1",
            notEquals(prop(startNode("  UNNAMED1"), "prop"), prop(endNode("  UNNAMED1"), "prop"))
          )
        )
      ))
  }

  test(
    "Rewrite (outerStart)((innerStart)-[rel{prop:1}]->(innerEnd) WHERE innerStart.prop <> innerEnd.prop)+(outerEnd)"
  ) {
    rewrite(Input(
      innerPredicates =
        Seq(propEquality("rel", "prop", 1), notEquals(prop("innerStart", "prop"), prop("innerEnd", "prop"))),
      direction = OUTGOING,
      minRep = 1,
      nodeToRelationshipRewriteOption = SkipRewriteOnZeroRepetitions
    )) shouldEqual
      Some(InlinedPredicates(relationshipPredicates =
        Seq(
          VariablePredicate(v"  UNNAMED1", propEquality("  UNNAMED1", "prop", 1)),
          VariablePredicate(
            v"  UNNAMED1",
            notEquals(prop(startNode("  UNNAMED1"), "prop"), prop(endNode("  UNNAMED1"), "prop"))
          )
        )
      ))
  }

  test(
    "Rewrite (outerStart)((innerStart)-[rel{prop:1}]->(innerEnd) WHERE innerStart.prop = 2 AND innerEnd.prop = 3)*(outerEnd)"
  ) {
    rewrite(Input(
      innerPredicates =
        Seq(
          propEquality("rel", "prop", 1),
          propEquality("innerStart", "prop", 2),
          propEquality("innerEnd", "prop", 3)
        ),
      direction = OUTGOING
    )) shouldEqual
      Some(InlinedPredicates(relationshipPredicates =
        Seq(
          VariablePredicate(v"  UNNAMED1", propEquality("  UNNAMED1", "prop", 1)),
          VariablePredicate(v"  UNNAMED1", equals(prop(startNode("  UNNAMED1"), "prop"), literalInt(2))),
          VariablePredicate(v"  UNNAMED1", equals(prop(endNode("  UNNAMED1"), "prop"), literalInt(3)))
        )
      ))
  }

  test(
    "Rewrites (outerStart)((innerStart)<-[rel{prop:1}]-(innerEnd) WHERE innerStart.prop = 2 AND innerEnd.prop = 3)*(outerEnd)"
  ) {
    rewrite(Input(
      innerPredicates =
        Seq(
          propEquality("rel", "prop", 1),
          propEquality("innerStart", "prop", 2),
          propEquality("innerEnd", "prop", 3)
        ),
      direction = INCOMING
    )) shouldEqual
      Some(InlinedPredicates(relationshipPredicates =
        Seq(
          VariablePredicate(v"  UNNAMED1", propEquality("  UNNAMED1", "prop", 1)),
          VariablePredicate(v"  UNNAMED1", equals(prop(endNode("  UNNAMED1"), "prop"), literalInt(2))),
          VariablePredicate(v"  UNNAMED1", equals(prop(startNode("  UNNAMED1"), "prop"), literalInt(3)))
        )
      ))
  }

  test(
    "Rewrites (outerStart)((innerStart)-[rel]->(innerEnd) WHERE innerEnd.prop = outerStart.prop)*(outerEnd)"
  ) {
    rewrite(Input(
      innerPredicates =
        Seq(
          equals(prop("innerEnd", "prop"), prop("outerStart", "prop"))
        ),
      direction = OUTGOING
    )) shouldEqual
      Some(InlinedPredicates(relationshipPredicates =
        Seq(
          VariablePredicate(v"  UNNAMED1", equals(prop(endNode("  UNNAMED1"), "prop"), prop("outerStart", "prop")))
        )
      ))
  }

  test(
    "Rewrites (outerStart)((innerStart)-[rel]->(innerEnd) WHERE innerStart.prop = outerEnd.prop)*(outerEnd)"
  ) {
    rewrite(Input(
      innerPredicates =
        Seq(
          equals(prop("innerStart", "prop"), prop("outerEnd", "prop"))
        ),
      direction = OUTGOING
    )) shouldEqual
      Some(InlinedPredicates(relationshipPredicates =
        Seq(
          VariablePredicate(v"  UNNAMED1", equals(prop(startNode("  UNNAMED1"), "prop"), prop("outerEnd", "prop")))
        )
      ))
  }

  test(
    "Rewrites (outerStart)((innerStart)-[rel{prop:1}]->(innerEnd) WHERE innerStart.prop <> innerEnd.prop)+(outerEnd)"
  ) {
    rewrite(Input(
      innerPredicates =
        Seq(propEquality("rel", "prop", 1), notEquals(prop("innerStart", "prop"), prop("innerEnd", "prop"))),
      direction = OUTGOING,
      minRep = 1,
      nodeToRelationshipRewriteOption = SkipRewriteOnZeroRepetitions
    )) shouldEqual
      Some(InlinedPredicates(relationshipPredicates =
        Seq(
          VariablePredicate(v"  UNNAMED1", propEquality("  UNNAMED1", "prop", 1)),
          VariablePredicate(
            v"  UNNAMED1",
            notEquals(prop(startNode("  UNNAMED1"), "prop"), prop(endNode("  UNNAMED1"), "prop"))
          )
        )
      ))
  }

  test(
    "Rewrite (outerStart{prop:1})((innerStart{prop:1})--(innerEnd{prop:1}))*(outerEnd)"
  ) {
    rewrite(Input(
      innerPredicates =
        Seq(propEquality("innerStart", "prop", 1), propEquality("innerEnd", "prop", 1)),
      outerPredicates = Seq(propEquality("outerStart", "prop", 1))
    )) shouldEqual
      Some(InlinedPredicates(nodePredicates =
        Seq(
          VariablePredicate(v"  UNNAMED0", propEquality("  UNNAMED0", "prop", 1))
        )
      ))
  }

  test(
    "Rewrite (outerStart)((innerStart{prop:1})--(innerEnd{prop:1}))*(outerEnd{prop:1})"
  ) {
    rewrite(Input(
      innerPredicates =
        Seq(propEquality("innerStart", "prop", 1), propEquality("innerEnd", "prop", 1)),
      outerPredicates = Seq(propEquality("outerEnd", "prop", 1))
    )) shouldEqual
      Some(InlinedPredicates(nodePredicates =
        Seq(
          VariablePredicate(v"  UNNAMED0", propEquality("  UNNAMED0", "prop", 1))
        )
      ))
  }

  test(
    "Rewrite (outerStart)((innerStart{prop:1})--(innerEnd{prop:1}))*(outerEnd)"
  ) {
    rewrite(Input(
      innerPredicates = Seq(propEquality("innerStart", "prop", 1), propEquality("innerEnd", "prop", 1))
    )) shouldBe Some(InlinedPredicates(relationshipPredicates =
      Seq(
        VariablePredicate(
          v"  UNNAMED1",
          equals(
            propExpression(TraversalEndpoint(v"  UNNAMED2", From), "prop"),
            literalInt(1)
          )
        ),
        VariablePredicate(
          v"  UNNAMED1",
          equals(
            propExpression(TraversalEndpoint(v"  UNNAMED3", To), "prop"),
            literalInt(1)
          )
        )
      )
    ))
  }

  test(
    "Rewrite (outerStart)((innerStart)--(innerEnd{prop:1}))+(outerEnd{prop:1})"
  ) {
    rewrite(Input(
      innerPredicates = Seq(propEquality("innerEnd", "prop", 1)),
      outerPredicates = Seq(propEquality("outerEnd", "prop", 1)),
      minRep = 1
    )) shouldBe Some(InlinedPredicates(relationshipPredicates =
      Seq(VariablePredicate(
        v"  UNNAMED1",
        equals(
          propExpression(TraversalEndpoint(v"  UNNAMED2", To), "prop"),
          literalInt(1)
        )
      ))
    ))
  }

  test(
    "Rewrite (outerStart{prop: 1})((innerStart{prop:1})--(innerEnd))*(outerEnd)"
  ) {
    rewrite(Input(
      innerPredicates = Seq(propEquality("innerStart", "prop", 1)),
      outerPredicates = Seq(propEquality("outerStart", "prop", 1))
    )) shouldBe Some(InlinedPredicates(relationshipPredicates =
      Seq(
        VariablePredicate(
          v"  UNNAMED1",
          equals(
            propExpression(TraversalEndpoint(v"  UNNAMED2", From), "prop"),
            literalInt(1)
          )
        )
      )
    ))
  }

  test(
    "Should not rewrite (outerStart)((innerStart)-[rel{prop:1}]->(innerEnd) WHERE innerStart.prop <> innerEnd.prop)*(outerEnd) if nodeToRelationshipRewriteOption is SkipRewriteOnZeroRepetitions"
  ) {
    rewrite(Input(
      innerPredicates =
        Seq(propEquality("rel", "prop", 1), notEquals(prop("innerStart", "prop"), prop("innerEnd", "prop"))),
      direction = OUTGOING,
      nodeToRelationshipRewriteOption = SkipRewriteOnZeroRepetitions
    )) shouldBe None
  }

  test(
    "Rewrite (outerStart)((innerStart)-[rel{prop:1}]-(innerEnd) WHERE innerStart.prop <> innerEnd.prop)*(outerEnd)"
  ) {
    rewrite(Input(
      innerPredicates =
        Seq(propEquality("rel", "prop", 1), notEquals(prop("innerStart", "prop"), prop("innerEnd", "prop")))
    )) shouldBe Some(InlinedPredicates(relationshipPredicates =
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
    ))
  }

  test(
    "Should not rewrite (outerStart)((innerStart)-[rel{prop:1}]->(innerEnd) WHERE NOT exists((innerEnd)--(innerEnd)))+(outerEnd)"
  ) {
    rewrite(Input(
      innerPredicates =
        Seq(
          propEquality("rel", "prop", 1),
          not(exists(patternExpression(innerEnd, innerEnd).withComputedIntroducedVariables(
            Set()
          ).withComputedScopeDependencies(Set(innerEnd))))
        ),
      direction = OUTGOING,
      minRep = 1,
      nodeToRelationshipRewriteOption = SkipRewriteOnZeroRepetitions
    )) shouldBe None
  }

  test(
    "(outerStart)((innerStart)-[rel{prop:1}]->(innerEnd) WHERE unknownVar.prop= 3)*(outerEnd)"
  ) {
    rewrite(Input(
      innerPredicates =
        Seq(
          propEquality("rel", "prop", 1),
          propEquality("unknownVar", "prop", 3)
        ),
      direction = OUTGOING
    )) shouldBe None
  }

  test(
    "Rewrite (outerStart)((innerStart{prop:1})-[r]-(innerEnd))+(outerEnd) or (outerEnd)((innerEnd)-[r]-(innerStart{prop:1}))+(outerStart)"
  ) {
    rewrite(Input(
      innerPredicates = Seq(propEquality("innerStart", "prop", 1)),
      minRep = 1
    )) shouldEqual Some(InlinedPredicates(relationshipPredicates =
      Seq(VariablePredicate(
        v"  UNNAMED1",
        equals(
          propExpression(TraversalEndpoint(v"  UNNAMED2", From), "prop"),
          literalInt(1)
        )
      ))
    ))
  }

  test(
    "Rewrite (outerStart)((innerStart)-[r]-(innerEnd{prop:1}))+(outerEnd) or (outerEnd)((innerEnd{prop:1})-[r]-(innerStart))+(outerStart)"
  ) {
    rewrite(Input(
      innerPredicates = Seq(propEquality("innerEnd", "prop", 1)),
      minRep = 1
    )) shouldEqual Some(InlinedPredicates(relationshipPredicates =
      Seq(VariablePredicate(
        v"  UNNAMED1",
        equals(
          propExpression(TraversalEndpoint(v"  UNNAMED2", To), "prop"),
          literalInt(1)
        )
      ))
    ))
  }
}
