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

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.compiler.CypherPlannerTestSuite
import org.neo4j.cypher.internal.compiler.ExecutionModel
import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanBuilder
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanTestOps
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.RepeatToVarExpandRewriter.RewritableRepeatExtractor
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.RepeatToVarExpandRewriter.RewritableRepeatExtractor.FilterAfterExpand
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.RepeatToVarExpandRewriterTest.DbFormat
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.RepeatToVarExpandRewriterTest.TrailParametersOps
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.RepeatToVarExpandRewriterTest.WalkParametersOps
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.RepeatToVarExpandRewriterTest.`(a) ((n)-[r]-(m))+ (b)`
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.RepeatToVarExpandRewriterTest.`(b) ((x)-[rr]-(y))+ (c)`
import org.neo4j.cypher.internal.expressions.SemanticDirection.INCOMING
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.Predicate
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.TrailParameters
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.WalkParameters
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandAll
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandInto
import org.neo4j.cypher.internal.logical.plans.Expand.VariablePredicate
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.TraversalPathMode.Walk
import org.neo4j.cypher.internal.runtime.ast.TraversalEndpoint
import org.neo4j.cypher.internal.runtime.ast.TraversalEndpoint.Endpoint.From
import org.neo4j.cypher.internal.runtime.ast.TraversalEndpoint.Endpoint.To
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.UpperBound
import org.neo4j.cypher.internal.util.UpperBound.Limited
import org.neo4j.cypher.internal.util.UpperBound.Unlimited
import org.neo4j.cypher.internal.util.attribution.Attributes

// Additional tests can be found in QuantifiedPathPatternPlanningIntegrationTest
class RepeatToVarExpandRewriterTest extends CypherPlannerTestSuite with LogicalPlanningTestSupport
    with LogicalPlanTestOps {

  // happy case
  test("Rewrites MATCH (a) ((n)-[r]->(m))+ (b) RETURN 1 AS s") {
    val trail = subPlanBuilder
      .projection("1 AS s")
      .repeatTrail(`(a) ((n)-[r]-(m))+ (b)`.emptyTrail)
      .|.filter(isRepeatTrailUnique("r_i"))
      .|.expand("(n_i)-[r_i]->(m_i)")
      .|.argument("n_i")
      .allNodeScan("a")
      .build()

    val expand = subPlanBuilder
      .projection("1 AS s")
      .expand("(a)-[r_i*1..]->(b)")
      .allNodeScan("a")
      .build()

    rewrites(trail, expand)
  }

  // relationship group variable r is used
  test("Rewrites MATCH (a) ((n)-[r]->(m))+ (b) RETURN r AS r") {
    val trail = new LogicalPlanBuilder()
      .produceResults("r")
      .repeatTrail(`(a) ((n)-[r]-(m))+ (b)`.nmlessTrail)
      .|.filter(isRepeatTrailUnique("r_i"))
      .|.expand("(n_i)-[r_i]->(m_i)")
      .|.argument("n_i")
      .allNodeScan("a")
      .build()
    val expand = new LogicalPlanBuilder()
      .produceResults("r")
      .expand("(a)-[r*]->(b)")
      .allNodeScan("a")
      .build()
    rewrites(trail, expand)
  }

  // node variable n is used
  test("Preserves MATCH (a) ((n)-[r]->(m))+ (b) RETURN n") {
    val trail = subPlanBuilder
      .repeatTrail(`(a) ((n)-[r]-(m))+ (b)`.rmlessTrail)
      .|.filter(isRepeatTrailUnique("r_i"))
      .|.expand("(n_i)-[r_i]->(m_i)")
      .|.argument("n_i")
      .allNodeScan("a")
      .build()
    preserves(trail)
  }

  // node variable m is used
  test("Preserves MATCH (a) ((n)-[r]->(m))+ (b) RETURN m") {
    val trail = subPlanBuilder
      .repeatTrail(`(a) ((n)-[r]-(m))+ (b)`.rnlessTrail)
      .|.filter(isRepeatTrailUnique("r_i"))
      .|.expand("(n_i)-[r_i]->(m_i)")
      .|.argument("n_i")
      .allNodeScan("a")
      .build()
    preserves(trail)
  }

  // node variables n and m are used
  test("Preserves MATCH (a) ((n)-[r]->(m))+ (b) RETURN n,m") {
    val trail = subPlanBuilder
      .repeatTrail(`(a) ((n)-[r]-(m))+ (b)`.rlessTrail)
      .|.filter(isRepeatTrailUnique("r_i"))
      .|.expand("(n_i)-[r_i]->(m_i)")
      .|.argument("n_i")
      .allNodeScan("a")
      .build()
    preserves(trail)
  }

  // node variable n has a predicate
  test("Rewrites MATCH (a) ((n WHERE n.prop > 123)-[r]->(m))+ (b) RETURN 1 AS s, depending on extractor") {
    val trail = subPlanBuilder
      .projection("1 AS s")
      .repeatTrail(`(a) ((n)-[r]-(m))+ (b)`.emptyTrail)
      .|.filter(isRepeatTrailUnique("r_i"))
      .|.expand("(n_i)-[r_i]->(m_i)")
      .|.filter("n_i.prop > 123")
      .|.argument("n_i")
      .nodeByLabelScan("a", "N")
      .build()

    val expand = subPlanBuilder
      .projection("1 AS s")
      .expand(
        "(a)-[r_i*1..]->(b)",
        relationshipPredicates = Seq(Predicate("  UNNAMED1", "startNode(`  UNNAMED1`).prop > 123"))
      )
      .nodeByLabelScan("a", "N")
      .build()

    rewrites(trail, expand, extractor = RewritableRepeatExtractor.FilterBeforeAndAfterExpand)
    preserves(trail, extractor = RewritableRepeatExtractor.FilterAfterExpand)
  }

  // the qpp relationship chain contains multiple relationships
  test("Preserves MATCH (a) ((n)-[r]->(m)->[rr]->(o))+ (b) RETURN 1 AS s") {
    object `(a) ((n)-[r]->(m)->[rr]->(o))+ (b)` {

      val empty: TrailParameters = TrailParameters(
        min = 1,
        max = Unlimited,
        start = "a",
        end = "b",
        innerStart = "n_i",
        innerEnd = "o_i",
        groupNodes = Set.empty,
        groupRelationships = Set.empty,
        innerRelationships = Set("r_i", "rr_i"),
        previouslyBoundRelationships = Set.empty,
        previouslyBoundRelationshipGroups = Set.empty,
        reverseGroupVariableProjections = false,
        expansionMode = ExpandAll,
        accumulators = Set.empty
      )
    }

    val trail = subPlanBuilder
      .projection("1 AS s")
      .repeatTrail(`(a) ((n)-[r]->(m)->[rr]->(o))+ (b)`.empty)
      .|.filter(isRepeatTrailUnique("rr_i"), differentRelationships("rr_i", "r_i"))
      .|.expand("(m)-[rr_i]->(o)")
      .|.filter(isRepeatTrailUnique("r_i"))
      .|.expand("(n_i)-[r_i]->(m_i)")
      .|.argument("n_i")
      .allNodeScan("a")
      .build()
    preserves(trail)
  }

  // pre-filter predicate with no dependency
  test("Preserves MATCH (a) ((n)-[r]->(m) WHERE 1 = true)+ (b) RETURN 1 AS s") {
    val trail = subPlanBuilder
      .projection("1 AS s")
      .repeatTrail(`(a) ((n)-[r]-(m))+ (b)`.emptyTrail)
      .|.filter(isRepeatTrailUnique("r_i"))
      .|.expand("(n_i)-[r_i]->(m_i)")
      .|.filter("1 = true")
      .|.argument("n_i")
      .filter("1 = true")
      .allNodeScan("a")
      .build()
    preserves(trail)
  }

  // pre-filter predicate with inner relationship dependency
  test("Rewrites MATCH (a) ((n)-[r]->(m) WHERE r.p = true)+ (b) RETURN 1 AS s") {
    val trail = subPlanBuilder
      .projection("1 AS s")
      .repeatTrail(`(a) ((n)-[r]-(m))+ (b)`.emptyTrail)
      .|.filter("r_i.p = true", isRepeatTrailUnique("r_i"))
      .|.expand("(n_i)-[r_i]->(m_i)")
      .|.argument("n_i")
      .allNodeScan("a")
      .build()
    val expand = subPlanBuilder
      .projection("1 AS s")
      .expand("(a)-[r_i*1..]->(b)", relationshipPredicates = Seq(Predicate("  UNNAMED1", "`  UNNAMED1`.p = true")))
      .allNodeScan("a")
      .build()

    rewrites(trail, expand, dbFormat = DbFormat.Aligned)
    rewrites(trail, expand, executionModels = Seq(ExecutionModel.Volcano))
    preserves(trail, dbFormat = DbFormat.Block, executionModels = Seq(ExecutionModel.Batched.default))
  }

  // pre-filter predicate with inner node dependency
  test("Preserves MATCH (a) ((n)-[r]->(m) WHERE n.p = true)+ (b) RETURN 1 AS s") {
    val trail = subPlanBuilder
      .projection("1 AS s")
      .repeatTrail(`(a) ((n)-[r]-(m))+ (b)`.emptyTrail)
      .|.filter(isRepeatTrailUnique("r_i"))
      .|.expand("(n_i)-[r_i]->(m_i)")
      .|.filter("n_i.p = true")
      .|.argument("n_i")
      .filter("a.p = true")
      .allNodeScan("a")
      .build()
    preserves(trail)
  }

  test("Rewrites MATCH (a) ((n)<-[r]-(m) WHERE n.p = true)+ (b) RETURN 1 AS s in the INCOMING direction") {
    val trail = subPlanBuilder
      .projection("1 AS s")
      .repeatTrail(`(a) ((n)-[r]-(m))+ (b)`.emptyTrail)
      .|.filter("n_i.p = true", isRepeatTrailUnique("r_i"))
      .|.expand("(n_i)<-[r_i]-(m_i)")
      .|.argument("n_i")
      .allNodeScan("a")
      .build()
    val expand = subPlanBuilder
      .projection("1 AS s")
      .expand(
        "(a)<-[r_i*1..]-(b)",
        projectedDir = INCOMING,
        relationshipPredicates = Seq(Predicate("  UNNAMED1", "endNode(`  UNNAMED1`).p = true"))
      )
      .allNodeScan("a")
      .build()
    rewrites(trail, expand)
  }

  test("Rewrites MATCH (a) ((n)-[r]->(m) WHERE n.p <> m.p)+ (b) RETURN 1 AS s") {
    val trail = subPlanBuilder
      .projection("1 AS s")
      .repeatTrail(`(a) ((n)-[r]-(m))+ (b)`.emptyTrail)
      .|.filter("n_i.p <> m_i.p", isRepeatTrailUnique("r_i"))
      .|.expand("(n_i)-[r_i]->(m_i)")
      .|.argument("n_i")
      .allNodeScan("a")
      .build()
    val expand = subPlanBuilder
      .projection("1 AS s")
      .expand(
        "(a)-[r_i*1..]->(b)",
        relationshipPredicates = Seq(Predicate("  UNNAMED1", "startNode(`  UNNAMED1`).p <> endNode(`  UNNAMED1`).p"))
      )
      .allNodeScan("a")
      .build()
    rewrites(trail, expand)
  }

  test("Rewrites bidirectional MATCH (a) ((n)-[r]-(m) WHERE n.p <> m.p)+ (b) RETURN 1 AS s") {
    val trail = subPlanBuilder
      .projection("1 AS s")
      .repeatTrail(`(a) ((n)-[r]-(m))+ (b)`.emptyTrail)
      .|.filter("n_i.p <> m_i.p", isRepeatTrailUnique("r_i"))
      .|.expand("(n_i)-[r_i]-(m_i)")
      .|.argument("n_i")
      .allNodeScan("a")
      .build()

    val expand = subPlanBuilder
      .projection("1 AS s")
      .expandExpr(
        "(a)-[r_i*1..]-(b)",
        relationshipPredicates = Seq(VariablePredicate(
          v"  UNNAMED1",
          notEquals(
            propExpression(TraversalEndpoint(v"  UNNAMED2", From), "p"),
            propExpression(TraversalEndpoint(v"  UNNAMED3", To), "p")
          )
        ))
      )
      .allNodeScan("a")
      .build()

    rewrites(trail, expand)
  }

  // pre-filter predicate with dependency on variable from previous clause
  test("Rewrites MATCH (z) MATCH (a) ((n)-[r]->(m) WHERE r.p = z.p)+ (b) RETURN 1 AS s") {
    val trail = subPlanBuilder
      .projection("1 AS s")
      .apply()
      .|.repeatTrail(`(a) ((n)-[r]-(m))+ (b)`.emptyTrail)
      .|.|.filter("r_i.p = cacheN[z.p]", isRepeatTrailUnique("r_i"))
      .|.|.expandAll("(n_i)-[r_i]->(m_i)")
      .|.|.argument("n_i", "z_i")
      .|.allNodeScan("a", "z")
      .cacheProperties("cacheNFromStore[z.p]")
      .allNodeScan("z")
      .build()
    val expand = subPlanBuilder
      .projection("1 AS s")
      .apply()
      .|.expand(
        "(a)-[r_i*1..]->(b)",
        relationshipPredicates = Seq(Predicate("  UNNAMED1", "`  UNNAMED1`.p = cacheN[z.p]"))
      )
      .|.allNodeScan("a", "z")
      .cacheProperties("cacheNFromStore[z.p]")
      .allNodeScan("z")
      .build()

    rewrites(trail, expand, dbFormat = DbFormat.Aligned)
    rewrites(trail, expand, executionModels = Seq(ExecutionModel.Volcano))
    preserves(trail, dbFormat = DbFormat.Block, executionModels = Seq(ExecutionModel.Batched.default))
  }

  // pre-filter relationship type predicate
  test("Rewrites MATCH (a) ((n)-[r:T]->(m))+ (b) RETURN 1 AS s") {
    val trail = subPlanBuilder
      .repeatTrail(`(a) ((n)-[r]-(m))+ (b)`.emptyTrail)
      .|.filter(isRepeatTrailUnique("r_i"))
      .|.expand("(n_i)-[r_i:T]->(m_i)")
      .|.argument("n_i")
      .allNodeScan("a")
      .build()
    val expand = subPlanBuilder
      .expand("(a)-[r_i:T*1..]->(b)")
      .allNodeScan("a")
      .build()
    rewrites(trail, expand)
  }

  // pre-filter relationship property predicate
  test(s"Rewrites MATCH (a) ((n)-[r WHERE r.p = 0]->(m))+ (b) RETURN 1 AS s") {
    val trail = subPlanBuilder
      .repeatTrail(`(a) ((n)-[r]-(m))+ (b)`.emptyTrail)
      .|.filter("r_i.p = 0", isRepeatTrailUnique("r_i"))
      .|.expand("(n_i)-[r_i]->(m_i)")
      .|.argument("n_i")
      .allNodeScan("a")
      .build()
    val expand = subPlanBuilder
      .expand("(a)-[r_i*1..]->(b)", relationshipPredicates = Seq(Predicate("  UNNAMED1", "`  UNNAMED1`.p = 0")))
      .allNodeScan("a")
      .build()

    rewrites(trail, expand, dbFormat = DbFormat.Aligned)
    rewrites(trail, expand, executionModels = Seq(ExecutionModel.Volcano))
    preserves(trail, dbFormat = DbFormat.Block, executionModels = Seq(ExecutionModel.Batched.default))
  }

  // post-filter predicate
  test("Rewrites MATCH (a) ((n)-[r]->(m))+ (b) WHERE all(x IN r WHERE x:T) RETURN 1 AS s") {
    val trail = subPlanBuilder
      .projection("1 AS s")
      .filter("all(x IN r WHERE x:T)")
      .repeatTrail(`(a) ((n)-[r]-(m))+ (b)`.nmlessTrail)
      .|.filter(isRepeatTrailUnique("r_i"))
      .|.expand("(n_i)-[r_i]->(m_i)")
      .|.argument("n_i")
      .allNodeScan("a")
      .build()
    val expand = subPlanBuilder
      .projection("1 AS s")
      .filter("all(x IN r WHERE x:T)")
      .expand("(a)-[r*1..]->(b)")
      .allNodeScan("a")
      .build()

    rewrites(trail, expand)
  }

  // quantifier with kleene star
  test("Rewrites MATCH (a) ((n)-[r]->(m))* (b) RETURN 1 AS s") {
    val trail = subPlanBuilder
      .projection("1 AS s")
      .repeatTrail(`(a) ((n)-[r]-(m))+ (b)`.emptyTrail.withQuantifier(0, Unlimited))
      .|.filter(isRepeatTrailUnique("r_i"))
      .|.expand("(n_i)-[r_i]->(m_i)")
      .|.argument("n_i")
      .allNodeScan("a")
      .build()

    val expand = subPlanBuilder
      .projection("1 AS s")
      .expand("(a)-[r_i*0..]->(b)")
      .allNodeScan("a")
      .build()

    rewrites(trail, expand)
  }

  // quantifier with limited ub
  test("Rewrites MATCH (a) ((n)-[r]->(m)){,2} (b) RETURN 1 AS s") {
    val trail = subPlanBuilder
      .projection("1 AS s")
      .repeatTrail(`(a) ((n)-[r]-(m))+ (b)`.emptyTrail.withQuantifier(0, Limited(2)))
      .|.filter(isRepeatTrailUnique("r_i"))
      .|.expand("(n_i)-[r_i]->(m_i)")
      .|.argument("n_i")
      .allNodeScan("a")
      .build()

    val expand = subPlanBuilder
      .projection("1 AS s")
      .expand("(a)-[r_i*0..2]->(b)")
      .allNodeScan("a")
      .build()

    rewrites(trail, expand)
  }

  // quantifier with unlimited ub
  test("Rewrites MATCH (a) ((n)-[r]->(m)){2,} (b) RETURN 1 AS s") {
    val trail = subPlanBuilder
      .projection("1 AS s")
      .repeatTrail(`(a) ((n)-[r]-(m))+ (b)`.emptyTrail.withQuantifier(2, Unlimited))
      .|.filter(isRepeatTrailUnique("r_i"))
      .|.expand("(n_i)-[r_i]->(m_i)")
      .|.argument("n_i")
      .allNodeScan("a")
      .build()

    val expand = subPlanBuilder
      .projection("1 AS s")
      .expand("(a)-[r_i*2..]->(b)")
      .allNodeScan("a")
      .build()

    rewrites(trail, expand)
  }

  // quantifier with equal lb and ub
  test("Rewrites MATCH (a) ((n)-[r]->(m)){2,2} (b) RETURN 1 AS s") {
    val trail = subPlanBuilder
      .projection("1 AS s")
      .repeatTrail(`(a) ((n)-[r]-(m))+ (b)`.emptyTrail.withQuantifier(2, Limited(2)))
      .|.filter(isRepeatTrailUnique("r_i"))
      .|.expand("(n_i)-[r_i]->(m_i)")
      .|.argument("n_i")
      .allNodeScan("a")
      .build()

    val expand = subPlanBuilder
      .projection("1 AS s")
      .expand("(a)-[r_i*2..2]->(b)")
      .allNodeScan("a")
      .build()

    rewrites(trail, expand)
  }

  // quantifier with different lb and ub
  test("Rewrites MATCH (a) ((n)-[r]->(m)){2,5} (b) RETURN 1 AS s") {
    val trail = subPlanBuilder
      .projection("1 AS s")
      .repeatTrail(`(a) ((n)-[r]-(m))+ (b)`.emptyTrail.withQuantifier(2, Limited(5)))
      .|.filter(isRepeatTrailUnique("r_i"))
      .|.expand("(n_i)-[r_i]->(m_i)")
      .|.argument("n_i")
      .allNodeScan("a")
      .build()

    val expand = subPlanBuilder
      .projection("1 AS s")
      .expand("(a)-[r_i*2..5]->(b)")
      .allNodeScan("a")
      .build()

    rewrites(trail, expand)
  }

  // cannot convert quantifier from long to int
  test("Preserves MATCH (a) ((n)-[r]->(m){,3000000000} (b) RETURN 1 AS s") {
    val trail = subPlanBuilder
      .projection("1 AS s")
      .repeatTrail(`(a) ((n)-[r]-(m))+ (b)`.emptyTrail.withQuantifier(0, Limited(3000000000L)))
      .|.filter(isRepeatTrailUnique("r_i"))
      .|.expand("(n_i)-[r_i]->(m_i)")
      .|.argument("n_i")
      .allNodeScan("a")
      .build()
    preserves(trail)
  }

  // dir=outgoing, reverseGroupVariableProjections
  test("Rewrites MATCH (a) ((n)-[r]->(m))+ (b) RETURN 1 AS s, reverseGroupVariableProjections=true") {
    val trail = subPlanBuilder
      .projection("1 AS s")
      .repeatTrail(`(a) ((n)-[r]-(m))+ (b)`.emptyTrail.reverse)
      .|.filter(isRepeatTrailUnique("r_i"))
      .|.expand("(m_i)<-[r_i]-(n_i)")
      .|.argument("m_i")
      .allNodeScan("b")
      .build()

    val expand = subPlanBuilder
      .projection("1 AS s")
      .expand("(b)<-[r_i*1..]-(a)")
      .allNodeScan("b")
      .build()

    rewrites(trail, expand)
  }

  // dir=incoming
  test("Rewrites MATCH (a) ((n)<-[r]-(m))+ (b) RETURN 1 AS s") {
    val trail = subPlanBuilder
      .projection("1 AS s")
      .repeatTrail(`(a) ((n)-[r]-(m))+ (b)`.emptyTrail)
      .|.filter(isRepeatTrailUnique("r_i"))
      .|.expand("(n_i)<-[r_i]-(m_i)")
      .|.argument("n_i")
      .allNodeScan("a")
      .build()

    val expand = subPlanBuilder
      .projection("1 AS s")
      .expand("(a)<-[r_i*1..]-(b)", projectedDir = INCOMING)
      .allNodeScan("a")
      .build()

    rewrites(trail, expand)
  }

  // dir=incoming, reverseGroupVariableProjections
  test("Rewrites MATCH (a) ((n)<-[r]-(m))+ (b) RETURN 1 AS s, reverseGroupVariableProjections=true") {
    val trail = subPlanBuilder
      .projection("1 AS s")
      .repeatTrail(`(a) ((n)-[r]-(m))+ (b)`.emptyTrail.reverse)
      .|.filter(isRepeatTrailUnique("r_i"))
      .|.expand("(m_i)-[r_i]->(n_i)")
      .|.argument("m_i")
      .allNodeScan("b")
      .build()

    val expand = subPlanBuilder
      .projection("1 AS s")
      .expand("(b)-[r_i*1..]->(a)", projectedDir = INCOMING)
      .allNodeScan("b")
      .build()

    rewrites(trail, expand)
  }

  // dir=both
  test("Rewrites MATCH (a) ((n)-[r]-(m))+ (b) RETURN 1 AS s") {
    val trail = subPlanBuilder
      .projection("1 AS s")
      .repeatTrail(`(a) ((n)-[r]-(m))+ (b)`.emptyTrail)
      .|.filter(isRepeatTrailUnique("r_i"))
      .|.expand("(n)-[r]-(m)")
      .|.argument("n_i")
      .allNodeScan("a")
      .build()

    val expand = subPlanBuilder
      .projection("1 AS s")
      .expand("(a)-[r_i*1..]-(b)")
      .allNodeScan("a")
      .build()

    rewrites(trail, expand)
  }

  // dir=both, reverseGroupVariableProjections
  test("Rewrites MATCH (a) ((n)-[r]-(m))+ (b) RETURN 1 AS s, reverseGroupVariableProjections=true") {
    val trail = subPlanBuilder
      .projection("1 AS s")
      .repeatTrail(`(a) ((n)-[r]-(m))+ (b)`.emptyTrail.reverse)
      .|.filter(isRepeatTrailUnique("r_i"))
      .|.expand("(m_i)-[r_i]-(n_i)")
      .|.argument("m_i")
      .allNodeScan("b")
      .build()

    val expand = subPlanBuilder
      .projection("1 AS s")
      .expand("(b)-[r_i*1..]-(a)", projectedDir = INCOMING)
      .allNodeScan("b")
      .build()

    rewrites(trail, expand)
  }

  // qpp + relationship pattern, inserts relationship uniqueness predicate because Trail has previously bound
  // relationships
  test("Rewrites MATCH (a) ((n)-[r]->(m))+ (b)-[rr]-(c) RETURN 1 AS s") {
    val trail = subPlanBuilder
      .projection("1 AS s")
      .repeatTrail(`(a) ((n)-[r]-(m))+ (b)`.emptyTrail.reverse.withPreviouslyBoundRel("rr"))
      .|.filter(isRepeatTrailUnique("r_i"))
      .|.expand("(m_i)<-[r_i]-(n_i)")
      .|.argument("m_i")
      .allRelationshipsScan("(b)-[rr]-(c)")
      .build()

    val expand = subPlanBuilder
      .projection("1 AS s")
      .filter(noneOfRels(v"rr", v"r_i"))
      .expand("(b)<-[r_i*1..]-(a)")
      .allRelationshipsScan("(b)-[rr]-(c)")
      .build()

    rewrites(trail, expand)
  }

  // qpp + relationship pattern, does not insert relationship uniqueness predicate because Trail has no previously bound
  // relationships
  test("Rewrites MATCH (b)-[rr]-(a) ((n)-[r]->(m))+ RETURN 1 AS s") {
    val trail = subPlanBuilder
      .projection("1 AS s")
      .filter("not rr IN r")
      .expand("(b)-[rr]-(c)")
      .repeatTrail(`(a) ((n)-[r]-(m))+ (b)`.nmlessTrail)
      .|.filter(isRepeatTrailUnique("r_i"))
      .|.expand("(n_i)-[r_i]->(m_i)")
      .|.argument("n_i")
      .allNodeScan("a")
      .build()

    val expand = subPlanBuilder
      .projection("1 AS s")
      .filter("not rr IN r")
      .expand("(b)-[rr]-(c)")
      .expand("(a)-[r*1..]->(b)")
      .allNodeScan("a")
      .build()

    rewrites(trail, expand)
  }

  // qpp + relationship pattern, does not insert relationship uniqueness predicate because Trail has no previously bound
  // relationships (the relationships are provably disjoint)
  test("Rewrites MATCH (a) ((n)-[r:R]->(m))+ (b)-[rr:RR]-(c) RETURN 1 AS s") {
    val trail = subPlanBuilder
      .projection("1 AS s")
      .repeatTrail(`(a) ((n)-[r]-(m))+ (b)`.emptyTrail.reverse)
      .|.filter(isRepeatTrailUnique("r_i"))
      .|.expand("(m_i)<-[r_i:R]-(n_i)")
      .|.argument("m_i")
      .allRelationshipsScan("(b)-[rr:RR]-(c)")
      .build()

    val expand = subPlanBuilder
      .projection("1 AS s")
      .expand("(b)<-[r_i:R*1..]-(a)")
      .allRelationshipsScan("(b)-[rr:RR]-(c)")
      .build()

    rewrites(trail, expand)
  }

  // two rewritable qpps. inserts relationship uniqueness predicate after the Trail which has previously bound
  // relationship group variables
  test("Rewrites MATCH (a) ((n)-[r]->(m))+ (b) ((x)-[rr]->(y))+ (c) RETURN 1 AS s") {
    val trail = subPlanBuilder
      .projection("1 AS s")
      .repeatTrail(`(a) ((n)-[r]-(m))+ (b)`.emptyTrail.reverse.withPreviouslyBoundRelGroup("rr"))
      .|.filter(isRepeatTrailUnique("r_i"))
      .|.expand("(m_i)<-[r_i]-(n_i)")
      .|.argument("m_i")
      .repeatTrail(`(b) ((x)-[rr]-(y))+ (c)`.xylessTrail)
      .|.filter(isRepeatTrailUnique("rr"))
      .|.expand("(x_i)-[rr_i]->(y_i)")
      .|.argument("x_i")
      .allNodeScan("b")
      .build()

    val expand = subPlanBuilder
      .projection("1 AS s")
      .filter(disjoint(v"r_i", v"rr"))
      .expand("(b)<-[r_i*1..]-(a)")
      .expand("(b)-[rr*1..]->(c)")
      .allNodeScan("b")
      .build()

    rewrites(trail, expand)
  }

  // two qpps, only the earliest tail is rewritable. do not insert relationship uniqueness predicate because the
  // earliest trail has no previously bound relationship group variables (latest Trail will take care of filtering out)
  test("Rewrites MATCH (a) ((n)-[r]->(m))+ (b) ((x {p: 0})-[rr]->(y))+ (c) RETURN 1 AS s") {
    val trail = subPlanBuilder
      .projection("1 AS s")
      .repeatTrail(`(b) ((x)-[rr]-(y))+ (c)`.emptyTrail.withPreviouslyBoundRelGroup("r"))
      .|.filter(isRepeatTrailUnique("rr"))
      .|.expand("(x_i)-[rr_i]->(y_i)")
      .|.filter("x_i.p = 0")
      .|.argument("x_i")
      .filter("b.p = 0")
      .repeatTrail(`(a) ((n)-[r]-(m))+ (b)`.nmlessTrail)
      .|.filter(isRepeatTrailUnique("r_i"))
      .|.expand("(n_i)-[r_i]->(m_i)")
      .|.argument("n_i")
      .allNodeScan("a")
      .build()

    val expand = subPlanBuilder
      .projection("1 AS s")
      .repeatTrail(`(b) ((x)-[rr]-(y))+ (c)`.emptyTrail.withPreviouslyBoundRelGroup("r"))
      .|.filter(isRepeatTrailUnique("rr"))
      .|.expand("(x_i)-[rr_i]->(y_i)")
      .|.filter("x_i.p = 0")
      .|.argument("x_i")
      .filter("b.p = 0")
      .expand("(a)-[r*1..]->(b)")
      .allNodeScan("a")
      .build()

    rewrites(trail, expand)
  }

  // two qpps, only the latest tail is rewritable. we insert relationship uniqueness predicate because the trail
  // has previously bound relationship group variables
  test("Rewrites MATCH (a) ((n)-[r]->(m))+ (b) ((x)-[rr]->(y)-[rrr]->(z))+ (c) RETURN 1 AS s") {
    object `(b) ((x)-[rr]-(y)-[rrr]-(z))+ (c)` {

      val xyzless: TrailParameters = TrailParameters(
        min = 1,
        max = Unlimited,
        start = "b",
        end = "c",
        innerStart = "x_i",
        innerEnd = "z_i",
        groupNodes = Set.empty,
        groupRelationships = Set(("rr_i", "rr"), ("rrr_i", "rrr")),
        innerRelationships = Set("rr_i", "rrr_i"),
        previouslyBoundRelationships = Set(),
        previouslyBoundRelationshipGroups = Set(),
        reverseGroupVariableProjections = false,
        expansionMode = ExpandAll,
        accumulators = Set.empty
      )
    }

    val trail = subPlanBuilder
      .projection("1 AS s")
      .repeatTrail(`(a) ((n)-[r]-(m))+ (b)`.emptyTrail.reverse.withPreviouslyBoundRelGroup("rr", "rrr"))
      .|.filter(isRepeatTrailUnique("r_i"))
      .|.expand("(m_i)<-[r_i]-(n_i)")
      .|.argument("m_i")
      .repeatTrail(`(b) ((x)-[rr]-(y)-[rrr]-(z))+ (c)`.xyzless)
      .|.filter(differentRelationships("rrr_i", "rr_i"), isRepeatTrailUnique("rrr_i"))
      .|.expand("(y_i)-[rrr_i]->(z_i)")
      .|.filter(isRepeatTrailUnique("rr_i"))
      .|.expand("(x_i)-[rr_i]->(y_i)")
      .|.argument("x_i")
      .allNodeScan("b")
      .build()

    val expand = subPlanBuilder
      .projection("1 AS s")
      .filter(
        disjoint(v"r_i", v"rr"),
        disjoint(v"r_i", v"rrr")
      )
      .expand("(b)<-[r_i*1..]-(a)")
      .repeatTrail(`(b) ((x)-[rr]-(y)-[rrr]-(z))+ (c)`.xyzless)
      .|.filter(differentRelationships("rrr_i", "rr_i"), isRepeatTrailUnique("rrr_i"))
      .|.expand("(y_i)-[rrr_i]->(z_i)")
      .|.filter(isRepeatTrailUnique("rr_i"))
      .|.expand("(x_i)-[rr_i]->(y_i)")
      .|.argument("x_i")
      .allNodeScan("b")
      .build()

    rewrites(trail, expand)
  }

  // two qpps with provably different relationship types. do not insert any relationship uniqueness predicates because
  // there are no previously bound relationship group variables (planner knows they are provably disjoint)
  test("Rewrites MATCH (a) ((n)-[r:R]->(m))+ (b) ((x)-[rr:RR]->(y))+ (c) RETURN 1 AS s") {

    val trail = subPlanBuilder
      .projection("1 AS s")
      .repeatTrail(`(a) ((n)-[r]-(m))+ (b)`.emptyTrail.reverse)
      .|.filter(isRepeatTrailUnique("r_i"))
      .|.expand("(m_i)<-[r_i:R]-(n_i)")
      .|.argument("m_i")
      .repeatTrail(`(b) ((x)-[rr]-(y))+ (c)`.emptyTrail)
      .|.filter(isRepeatTrailUnique("rr_i"))
      .|.expand("(x_i)-[rr_i:RR]->(y_i)")
      .|.argument("x_i")
      .allNodeScan("b")
      .build()

    val expand = subPlanBuilder
      .projection("1 AS s")
      .expand("(b)<-[r_i:R*1..]-(a)")
      .expand("(b)-[rr_i:RR*1..]->(c)")
      .allNodeScan("b")
      .build()

    rewrites(trail, expand)
  }

  test(
    "Rewrites MATCH (a) ((n)-[r:R]->(m) WHERE n.prop <> m.prop)+ (b) ((x)-[rr:RR]->(y {name: 'foo'))+ (c) RETURN 1 AS s"
  ) {

    val trail = subPlanBuilder
      .projection("1 AS s")
      .repeatTrail(`(a) ((n)-[r]-(m))+ (b)`.emptyTrail.reverse)
      .|.filter("n_i.prop <> m_i.prop", isRepeatTrailUnique("r_i"))
      .|.expand("(m_i)<-[r_i:R]-(n_i)")
      .|.argument("m_i")
      .repeatTrail(`(b) ((x)-[rr]-(y))+ (c)`.emptyTrail)
      .|.filter("y_i.name = 'foo'", isRepeatTrailUnique("rr_i"))
      .|.expand("(x_i)-[rr_i:RR]->(y_i)")
      .|.argument("x_i")
      .allNodeScan("b")
      .build()

    val expand = subPlanBuilder
      .projection("1 AS s")
      .expand(
        "(b)<-[r_i:R*1..]-(a)",
        relationshipPredicates =
          Seq(Predicate("  UNNAMED3", "startNode(`  UNNAMED3`).prop <> endNode(`  UNNAMED3`).prop"))
      )
      .expand(
        "(b)-[rr_i:RR*1..]->(c)",
        relationshipPredicates = Seq(Predicate("  UNNAMED1", "endNode(`  UNNAMED1`).name = 'foo'"))
      )
      .allNodeScan("b")
      .build()

    rewrites(trail, expand)
  }

  // mix qpps and relationship pattern, inserts relationship uniqueness predicates when needed
  test("Rewrites MATCH (a) ((n)-[r]->(m))+ (b)-[rr]->(c) ((x)-[rrr]->(y))+ (d) RETURN 1 AS s") {
    object `(c) ((x)-[rrr]-(y))+ (d)` {
      val empty: TrailParameters = TrailParameters(
        min = 1,
        max = Unlimited,
        start = "c",
        end = "d",
        innerStart = "x_i",
        innerEnd = "y_i",
        groupNodes = Set.empty,
        groupRelationships = Set.empty,
        innerRelationships = Set("rrr_i"),
        previouslyBoundRelationships = Set("rr"),
        previouslyBoundRelationshipGroups = Set("r"),
        reverseGroupVariableProjections = false,
        expansionMode = ExpandAll,
        accumulators = Set.empty
      )
    }
    val trail = subPlanBuilder
      .projection("1 AS s")
      .repeatTrail(`(c) ((x)-[rrr]-(y))+ (d)`.empty)
      .|.filter(isRepeatTrailUnique("rrr_i"))
      .|.expand("(x_i)-[rrr_i]->(y_i)")
      .|.argument("x_i")
      .filter(noneOfRels(v"rr", v"r"))
      .expand("(b)-[rr]->(c)")
      .repeatTrail(`(a) ((n)-[r]-(m))+ (b)`.nmlessTrail)
      .|.filter(isRepeatTrailUnique("r_i"))
      .|.expand("(n_i)-[r_i]->(m_i)")
      .|.argument("n_i")
      .allNodeScan("a")
      .build()
    val expand = subPlanBuilder
      .projection("1 AS s")
      .filter(disjoint(v"rrr_i", v"r"))
      .filter(noneOfRels(v"rr", v"rrr_i"))
      .expand("(c)-[rrr_i*1..]->(d)")
      .filter(noneOfRels(v"rr", v"r"))
      .expand("(b)-[rr]->(c)")
      .expand("(a)-[r*1..]->(b)")
      .allNodeScan("a")
      .build()

    rewrites(trail, expand)
  }

  // does not rewrite directly to PruningVarExpand (responsibility of pruningVarExpander)
  test("Rewrites MATCH (a) ((n)-[r]->(m))+ (b) RETURN DISTINCT b") {
    val trail = subPlanBuilder
      .distinct("b AS b")
      .repeatTrail(`(a) ((n)-[r]-(m))+ (b)`.emptyTrail)
      .|.filter(isRepeatTrailUnique("r_i"))
      .|.expand("(n_i)-[r_i]->(m_i)")
      .|.argument("n_i")
      .allNodeScan("a")
      .build()
    val expand = subPlanBuilder
      .distinct("b AS b")
      .expand("(a)-[r_i*1..]->(b)")
      .allNodeScan("a")
      .build()

    rewrites(trail, expand)
  }

  // named path uses group node variables
  test("Preserves MATCH p = (a) ((n)-[r]->(m))+ (b) RETURN p") {
    val trail = subPlanBuilder
      .projection(Map("p" -> qppPath(v"a", Seq(v"n", v"r"), v"b")))
      .repeatTrail(`(a) ((n)-[r]-(m))+ (b)`.mlessTrail)
      .|.filter(isRepeatTrailUnique("r_i"))
      .|.expand("(n_i)-[r_i]->(m_i)")
      .|.argument("n_i")
      .allNodeScan("a")
      .build()
    preserves(trail)
  }

  val `(a) ((x_i)-[r_i]-(y_i))+ (  UNNAMED4)`: TrailParameters = TrailParameters(
    min = 1,
    max = Unlimited,
    start = "a",
    end = "  UNNAMED4",
    innerStart = "x_i",
    innerEnd = "y_i",
    groupNodes = Set.empty,
    groupRelationships = Set.empty,
    innerRelationships = Set("r_i"),
    previouslyBoundRelationships = Set.empty,
    previouslyBoundRelationshipGroups = Set.empty,
    reverseGroupVariableProjections = false,
    expansionMode = ExpandAll,
    accumulators = Set.empty
  )

  val `(a) ((x_i)-[r_i]-(y_i))+ (b)`: TrailParameters = TrailParameters(
    min = 1,
    max = Unlimited,
    start = "a",
    end = "b",
    innerStart = "x_i",
    innerEnd = "y_i",
    groupNodes = Set.empty,
    groupRelationships = Set.empty,
    innerRelationships = Set("r_i"),
    previouslyBoundRelationships = Set.empty,
    previouslyBoundRelationshipGroups = Set.empty,
    reverseGroupVariableProjections = false,
    expansionMode = ExpandInto,
    accumulators = Set.empty
  )

  test("Rewrite selection and trail to VarLengthExpand(Into)") {
    val selectionAndTrail = subPlanBuilder
      .projection("1 AS s")
      .repeatTrail(`(a) ((x_i)-[r_i]-(y_i))+ (b)`)
      .|.filter(isRepeatTrailUnique("r_i"))
      .|.expandAll("(`  UNNAMED2`)-[`r_i`]->(`  UNNAMED3`)")
      .|.argument("  UNNAMED1")
      .allRelationshipsScan("(a)-[r_j]->(b)")
      .build()

    val expand = subPlanBuilder
      .projection("1 AS s")
      .expand("(a)-[r_i*1..]->(b)", ExpandInto)
      .allRelationshipsScan("(a)-[r_j]->(b)")
      .build()

    rewrites(selectionAndTrail, expand)
  }

  test(
    "Rewrite selection and trail to VarLengthExpand(Into) - cartesian product and expandInto for the first relationship"
  ) {
    val selectionAndTrail = subPlanBuilder
      .projection("1 AS s")
      .repeatTrail(`(a) ((x_i)-[r_i]-(y_i))+ (b)`)
      .|.filter(isRepeatTrailUnique("r_i"))
      .|.expandAll("(`  UNNAMED2`)-[`r_i`]->(`  UNNAMED3`)")
      .|.argument("  UNNAMED1")
      .expandInto("(a)-[r_j]->(b)")
      .cartesianProduct()
      .|.allNodeScan("b")
      .allNodeScan("a")
      .build()

    val expand = subPlanBuilder
      .projection("1 AS s")
      .expand("(a)-[r_i*1..]->(b)", ExpandInto)
      .expandInto("(a)-[r_j]->(b)")
      .cartesianProduct()
      .|.allNodeScan("b")
      .allNodeScan("a")
      .build()

    rewrites(selectionAndTrail, expand)
  }

  test(
    "Do not rewrite selection and trail to VarLengthExpand(Into) when the selection does not refer to the end node of the trail - it refers to the innerEnd"
  ) {
    // This plan cannot be generated because the filter cannot refer to the innerEnd node of the QPP.
    // It tests that having a `  UNNAMED` node is not enough. It should be the end point of the QPP.
    val selectionAndTrail = subPlanBuilder
      .projection("1 AS s")
      .filter("`  UNNAMED3` = b")
      .repeatTrail(`(a) ((x_i)-[r_i]-(y_i))+ (  UNNAMED4)`)
      .|.filter(isRepeatTrailUnique("r_i"))
      .|.expandAll("(`  UNNAMED2`)-[`r_i`]->(`  UNNAMED3`)")
      .|.argument("  UNNAMED1")
      .allRelationshipsScan("(a)-[r_j]->(b)")
      .build()

    val expand = subPlanBuilder
      .projection("1 AS s")
      .filter("`  UNNAMED3` = b")
      .expand("(a)-[r_i*1..]->(`  UNNAMED4`)", ExpandAll)
      .allRelationshipsScan("(a)-[r_j]->(b)")
      .build()

    rewrites(selectionAndTrail, expand)
  }

  test(
    "Do not rewrite selection and trail to VarLengthExpand(Into) when the selection does not refer to the end node of the trail - it refers to a previously bound node"
  ) {
    val selectionAndTrail = subPlanBuilder
      .projection("1 AS s")
      .filter("a = b")
      .repeatTrail(`(a) ((x_i)-[r_i]-(y_i))+ (  UNNAMED4)`)
      .|.filter(isRepeatTrailUnique("r_i"))
      .|.expandAll("(`  UNNAMED2`)-[`r_i`]->(`  UNNAMED3`)")
      .|.argument("  UNNAMED1")
      .allRelationshipsScan("(a)-[r_j]->(b)")
      .build()

    val expand = subPlanBuilder
      .projection("1 AS s")
      .filter("a = b")
      .expand("(a)-[r_i*1..]->(`  UNNAMED4`)", ExpandAll)
      .allRelationshipsScan("(a)-[r_j]->(b)")
      .build()

    rewrites(selectionAndTrail, expand)
  }

  test(
    "Do not rewrite selection and trail to VarLengthExpand(Into) when the selection does not refer a previously bound node"
  ) {
    // This plan cannot be generated because 'c' in the filter does not refer to anything
    val selectionAndTrail = subPlanBuilder
      .projection("1 AS s")
      .expandAll("(b)-[r_k]->(c)")
      .filter("`  UNNAMED4` = c")
      .repeatTrail(`(a) ((x_i)-[r_i]-(y_i))+ (  UNNAMED4)`)
      .|.filter(isRepeatTrailUnique("r_i"))
      .|.expandAll("(`  UNNAMED2`)-[`r_i`]->(`  UNNAMED3`)")
      .|.argument("  UNNAMED1")
      .allRelationshipsScan("(a)-[r_j]->(b)")
      .build()

    val expand = subPlanBuilder
      .projection("1 AS s")
      .expandAll("(b)-[r_k]->(c)")
      .filter("`  UNNAMED4` = c")
      .expand("(a)-[r_i*1..]->(`  UNNAMED4`)", ExpandAll)
      .allRelationshipsScan("(a)-[r_j]->(b)")
      .build()

    rewrites(selectionAndTrail, expand)
  }

  test(
    "Do not rewrite selection and trail to VarLengthExpand(Into) when the selection does not refer a previously bound node - it refers to the innerEnd"
  ) {
    val selectionAndTrail = subPlanBuilder
      .projection("1 AS s")
      .filter("`  UNNAMED4` = `  UNNAMED3`")
      .repeatTrail(`(a) ((x_i)-[r_i]-(y_i))+ (  UNNAMED4)`)
      .|.filter(isRepeatTrailUnique("r_i"))
      .|.expandAll("(`  UNNAMED2`)-[`r_i`]->(`  UNNAMED3`)")
      .|.argument("  UNNAMED1")
      .allRelationshipsScan("(a)-[r_j]->(b)")
      .build()

    val expand = subPlanBuilder
      .projection("1 AS s")
      .filter("`  UNNAMED4` = `  UNNAMED3`")
      .expand("(a)-[r_i*1..]->(`  UNNAMED4`)", ExpandAll)
      .allRelationshipsScan("(a)-[r_j]->(b)")
      .build()

    rewrites(selectionAndTrail, expand)
  }

  test(
    "Do not rewrite selection and trail to VarLengthExpand(Into) when the endpoint of the trail is not an UNNAMED node"
  ) {
    // Node 'b' might be used in other places of the query, therefore we cannot override it with 'c' to create a VarLengthExpand(Into)
    val selectionAndTrail = subPlanBuilder
      .projection("1 AS s")
      .filter("b = c")
      .repeatTrail(`(a) ((n)-[r]-(m))+ (b)`.emptyTrail)
      .|.filter(isRepeatTrailUnique("r_i"))
      .|.expandAll("(`  UNNAMED2`)-[`r_i`]->(`  UNNAMED3`)")
      .|.argument("  UNNAMED1")
      .allRelationshipsScan("(a)-[r_j]->(c)")
      .build()

    val expand = subPlanBuilder
      .projection("1 AS s")
      .filter("b = c")
      .expand("(a)-[r_i*1..]->(b)", ExpandAll)
      .allRelationshipsScan("(a)-[r_j]->(c)")
      .build()

    rewrites(selectionAndTrail, expand)
  }

  test("Do not rewrite selection and trail to VarLengthExpand(Into) when the selection has more than 1 predicate") {
    val selectionAndTrail = subPlanBuilder
      .projection("1 AS s")
      .filter("`  UNNAMED4` = b", "not a = b")
      .repeatTrail(`(a) ((x_i)-[r_i]-(y_i))+ (  UNNAMED4)`)
      .|.filter(isRepeatTrailUnique("r_i"))
      .|.expandAll("(`  UNNAMED2`)-[`r_i`]->(`  UNNAMED3`)")
      .|.argument("  UNNAMED1")
      .allRelationshipsScan("(a)-[r_j]->(b)")
      .build()

    val expand = subPlanBuilder
      .projection("1 AS s")
      .filter("`  UNNAMED4` = b", "not a = b")
      .expand("(a)-[r_i*1..]->(`  UNNAMED4`)", ExpandAll)
      .allRelationshipsScan("(a)-[r_j]->(b)")
      .build()

    rewrites(selectionAndTrail, expand)
  }

  test(
    "Rewrites MATCH (a) ((n WHERE n.prop > 123)-[r]->(m WHERE m.prop < 321))+ (b) RETURN 1 AS s (directed relationship)"
  ) {
    val trail = subPlanBuilder
      .projection("1 AS s")
      .repeatTrail(`(a) ((n)-[r]-(m))+ (b)`.emptyTrail)
      .|.filter("m_i.prop < 321", isRepeatTrailUnique("r_i"))
      .|.expand("(n_i)-[r_i]->(m_i)")
      .|.filter("n_i.prop > 123")
      .|.argument("n_i")
      .allNodeScan("a")
      .build()
    val expand = subPlanBuilder
      .projection("1 AS s")
      .expand(
        "(a)-[r_i*1..]->(b)",
        relationshipPredicates = Seq(
          Predicate("  UNNAMED1", "endNode(`  UNNAMED1`).prop < 321"),
          Predicate("  UNNAMED1", "startNode(`  UNNAMED1`).prop > 123")
        )
      )
      .allNodeScan("a")
      .build()

    rewrites(trail, expand, extractor = RewritableRepeatExtractor.FilterBeforeAndAfterExpand)
    preserves(trail, extractor = RewritableRepeatExtractor.FilterAfterExpand)
  }

  test(
    "Rewrites MATCH (a) ((n WHERE n.prop > 123)-[r]-(m WHERE m.prop < 321))+ (b) RETURN 1 AS s (undirected relationship)"
  ) {
    val trail = subPlanBuilder
      .projection("1 AS s")
      .repeatTrail(`(a) ((n)-[r]-(m))+ (b)`.emptyTrail)
      .|.filter("m_i.prop < 321", isRepeatTrailUnique("r_i"))
      .|.expand("(n_i)-[r_i]-(m_i)")
      .|.filter("n_i.prop > 123")
      .|.argument("n_i")
      .allNodeScan("a")
      .build()

    val TO = TraversalEndpoint(tempVar = v"  UNNAMED2", endpoint = To)
    val FROM = TraversalEndpoint(tempVar = v"  UNNAMED3", endpoint = From)

    val expand = subPlanBuilder
      .projection("1 AS s")
      .expandExpr(
        "(a)-[r_i*1..]-(b)",
        relationshipPredicates = Seq(
          VariablePredicate(v"  UNNAMED1", lessThan(prop(TO, "prop"), literalInt(321))),
          VariablePredicate(v"  UNNAMED1", greaterThan(prop(FROM, "prop"), literalInt(123)))
        )
      )
      .allNodeScan("a")
      .build()

    rewrites(trail, expand, extractor = RewritableRepeatExtractor.FilterBeforeAndAfterExpand)
    preserves(trail, extractor = RewritableRepeatExtractor.FilterAfterExpand)
  }

  // happy case
  test("Rewrites MATCH REPEATABLE ELEMENTS (a) ((n)-[r]->(m))+ (b) RETURN 1 AS s") {
    val walk = subPlanBuilder
      .projection("1 AS s")
      .repeatWalk(`(a) ((n)-[r]-(m))+ (b)`.emptyWalk)
      .|.expand("(n_i)-[r_i]->(m_i)")
      .|.argument("n_i")
      .allNodeScan("a")
      .build()

    val expand = subPlanBuilder
      .projection("1 AS s")
      .expand("(a)-[r_i*1..]->(b)", pathMode = Walk)
      .allNodeScan("a")
      .build()

    rewrites(walk, expand)
  }

  // relationship group variable r is used
  test("Rewrites MATCH REPEATABLE ELEMENTS (a) ((n)-[r]->(m))+ (b) RETURN r AS r") {
    val walk = new LogicalPlanBuilder()
      .produceResults("r")
      .repeatWalk(`(a) ((n)-[r]-(m))+ (b)`.nmlessWalk)
      .|.expand("(n_i)-[r_i]->(m_i)")
      .|.argument("n_i")
      .allNodeScan("a")
      .build()
    val expand = new LogicalPlanBuilder()
      .produceResults("r")
      .expand("(a)-[r*]->(b)", pathMode = Walk)
      .allNodeScan("a")
      .build()
    rewrites(walk, expand)
  }

  // node variable n is used
  test("Preserves MATCH REPEATABLE ELEMENTS (a) ((n)-[r]->(m))+ (b) RETURN n") {
    val walk = subPlanBuilder
      .repeatWalk(`(a) ((n)-[r]-(m))+ (b)`.rmlessWalk)
      .|.expand("(n_i)-[r_i]->(m_i)")
      .|.argument("n_i")
      .allNodeScan("a")
      .build()
    preserves(walk)
  }

  // node variable m is used
  test("Preserves MATCH REPEATABLE ELEMENTS (a) ((n)-[r]->(m))+ (b) RETURN m") {
    val walk = subPlanBuilder
      .repeatWalk(`(a) ((n)-[r]-(m))+ (b)`.rnlessWalk)
      .|.expand("(n_i)-[r_i]->(m_i)")
      .|.argument("n_i")
      .allNodeScan("a")
      .build()
    preserves(walk)
  }

  // node variables n and m are used
  test("Preserves MATCH REPEATABLE ELEMENTS (a) ((n)-[r]->(m))+ (b) RETURN n,m") {
    val walk = subPlanBuilder
      .repeatWalk(`(a) ((n)-[r]-(m))+ (b)`.rlessWalk)
      .|.expand("(n_i)-[r_i]->(m_i)")
      .|.argument("n_i")
      .allNodeScan("a")
      .build()
    preserves(walk)
  }

  // node variable n has a predicate
  test(
    "Rewrites MATCH REPEATABLE ELEMENTS (a) ((n WHERE n.prop > 123)-[r]->(m))+ (b) RETURN 1 AS s, depending on extractor"
  ) {
    val walk = subPlanBuilder
      .projection("1 AS s")
      .repeatWalk(`(a) ((n)-[r]-(m))+ (b)`.emptyWalk)
      .|.expand("(n_i)-[r_i]->(m_i)")
      .|.filter("n_i.prop > 123")
      .|.argument("n_i")
      .nodeByLabelScan("a", "N")
      .build()

    val expand = subPlanBuilder
      .projection("1 AS s")
      .expand(
        "(a)-[r_i*1..]->(b)",
        relationshipPredicates = Seq(Predicate("  UNNAMED1", "startNode(`  UNNAMED1`).prop > 123")),
        pathMode = Walk
      )
      .nodeByLabelScan("a", "N")
      .build()

    rewrites(walk, expand, extractor = RewritableRepeatExtractor.FilterBeforeAndAfterExpand)
    preserves(walk, extractor = RewritableRepeatExtractor.FilterAfterExpand)
  }

  // the qpp relationship chain contains multiple relationships
  test("Preserves MATCH REPEATABLE ELEMENTS (a) ((n)-[r]->(m)->[rr]->(o))+ (b) RETURN 1 AS s") {
    object `(a) ((n)-[r]->(m)->[rr]->(o))+ (b)` {

      val empty: WalkParameters = WalkParameters(
        min = 1,
        max = Unlimited,
        start = "a",
        end = "b",
        innerStart = "n_i",
        innerEnd = "o_i",
        groupNodes = Set.empty,
        groupRelationships = Set.empty,
        innerRelationships = Set("r_i", "rr_i"),
        reverseGroupVariableProjections = false,
        expansionMode = ExpandAll,
        accumulators = Set.empty
      )
    }

    val walk = subPlanBuilder
      .projection("1 AS s")
      .repeatWalk(`(a) ((n)-[r]->(m)->[rr]->(o))+ (b)`.empty)
      .|.expand("(m)-[rr_i]->(o)")
      .|.expand("(n_i)-[r_i]->(m_i)")
      .|.argument("n_i")
      .allNodeScan("a")
      .build()
    preserves(walk)
  }

  // pre-filter predicate with no dependency
  test("Preserves MATCH REPEATABLE ELEMENTS (a) ((n)-[r]->(m) WHERE 1 = true)+ (b) RETURN 1 AS s") {
    val walk = subPlanBuilder
      .projection("1 AS s")
      .repeatWalk(`(a) ((n)-[r]-(m))+ (b)`.emptyWalk)
      .|.expand("(n_i)-[r_i]->(m_i)")
      .|.filter("1 = true")
      .|.argument("n_i")
      .filter("1 = true")
      .allNodeScan("a")
      .build()
    preserves(walk)
  }

  // pre-filter predicate with inner relationship dependency
  test("Rewrites MATCH REPEATABLE ELEMENTS (a) ((n)-[r]->(m) WHERE r.p = true)+ (b) RETURN 1 AS s") {
    val walk = subPlanBuilder
      .projection("1 AS s")
      .repeatWalk(`(a) ((n)-[r]-(m))+ (b)`.emptyWalk)
      .|.filter("r_i.p = true")
      .|.expand("(n_i)-[r_i]->(m_i)")
      .|.argument("n_i")
      .allNodeScan("a")
      .build()
    val expand = subPlanBuilder
      .projection("1 AS s")
      .expand(
        "(a)-[r_i*1..]->(b)",
        relationshipPredicates = Seq(Predicate("  UNNAMED1", "`  UNNAMED1`.p = true")),
        pathMode = Walk
      )
      .allNodeScan("a")
      .build()

    rewrites(walk, expand, dbFormat = DbFormat.Aligned)
    rewrites(walk, expand, executionModels = Seq(ExecutionModel.Volcano))
    preserves(walk, dbFormat = DbFormat.Block, executionModels = Seq(ExecutionModel.Batched.default))
  }

  // pre-filter predicate with inner node dependency
  test("Preserves MATCH REPEATABLE ELEMENTS (a) ((n)-[r]->(m) WHERE n.p = true)+ (b) RETURN 1 AS s") {
    val walk = subPlanBuilder
      .projection("1 AS s")
      .repeatWalk(`(a) ((n)-[r]-(m))+ (b)`.emptyWalk)
      .|.expand("(n_i)-[r_i]->(m_i)")
      .|.filter("n_i.p = true")
      .|.argument("n_i")
      .filter("a.p = true")
      .allNodeScan("a")
      .build()
    preserves(walk)
  }

  test(
    "Rewrites MATCH REPEATABLE ELEMENTS (a) ((n)<-[r]-(m) WHERE n.p = true)+ (b) RETURN 1 AS s in the INCOMING direction"
  ) {
    val walk = subPlanBuilder
      .projection("1 AS s")
      .repeatWalk(`(a) ((n)-[r]-(m))+ (b)`.emptyWalk)
      .|.filter("n_i.p = true")
      .|.expand("(n_i)<-[r_i]-(m_i)")
      .|.argument("n_i")
      .allNodeScan("a")
      .build()
    val expand = subPlanBuilder
      .projection("1 AS s")
      .expand(
        "(a)<-[r_i*1..]-(b)",
        projectedDir = INCOMING,
        relationshipPredicates = Seq(Predicate("  UNNAMED1", "endNode(`  UNNAMED1`).p = true")),
        pathMode = Walk
      )
      .allNodeScan("a")
      .build()
    rewrites(walk, expand)
  }

  test("Rewrites MATCH REPEATABLE ELEMENTS (a) ((n)-[r]->(m) WHERE n.p <> m.p)+ (b) RETURN 1 AS s") {
    val walk = subPlanBuilder
      .projection("1 AS s")
      .repeatWalk(`(a) ((n)-[r]-(m))+ (b)`.emptyWalk)
      .|.filter("n_i.p <> m_i.p")
      .|.expand("(n_i)-[r_i]->(m_i)")
      .|.argument("n_i")
      .allNodeScan("a")
      .build()
    val expand = subPlanBuilder
      .projection("1 AS s")
      .expand(
        "(a)-[r_i*1..]->(b)",
        relationshipPredicates = Seq(Predicate("  UNNAMED1", "startNode(`  UNNAMED1`).p <> endNode(`  UNNAMED1`).p")),
        pathMode = Walk
      )
      .allNodeScan("a")
      .build()
    rewrites(walk, expand)
  }

  test("Rewrites bidirectional MATCH REPEATABLE ELEMENTS (a) ((n)-[r]-(m) WHERE n.p <> m.p)+ (b) RETURN 1 AS s") {
    val walk = subPlanBuilder
      .projection("1 AS s")
      .repeatWalk(`(a) ((n)-[r]-(m))+ (b)`.emptyWalk)
      .|.filter("n_i.p <> m_i.p")
      .|.expand("(n_i)-[r_i]-(m_i)")
      .|.argument("n_i")
      .allNodeScan("a")
      .build()

    val expand = subPlanBuilder
      .projection("1 AS s")
      .expandExpr(
        "(a)-[r_i*1..]-(b)",
        relationshipPredicates = Seq(VariablePredicate(
          v"  UNNAMED1",
          notEquals(
            propExpression(TraversalEndpoint(v"  UNNAMED2", From), "p"),
            propExpression(TraversalEndpoint(v"  UNNAMED3", To), "p")
          )
        )),
        pathMode = Walk
      )
      .allNodeScan("a")
      .build()

    rewrites(walk, expand)
  }

  // pre-filter predicate with dependency on variable from previous clause
  test(
    "Rewrites MATCH REPEATABLE ELEMENTS (z) MATCH REPEATABLE ELEMENTS (a) ((n)-[r]->(m) WHERE r.p = z.p)+ (b) RETURN 1 AS s"
  ) {
    val walk = subPlanBuilder
      .projection("1 AS s")
      .apply()
      .|.repeatWalk(`(a) ((n)-[r]-(m))+ (b)`.emptyWalk)
      .|.|.filter("r_i.p = cacheN[z.p]")
      .|.|.expandAll("(n_i)-[r_i]->(m_i)")
      .|.|.argument("n_i", "z_i")
      .|.allNodeScan("a", "z")
      .cacheProperties("cacheNFromStore[z.p]")
      .allNodeScan("z")
      .build()
    val expand = subPlanBuilder
      .projection("1 AS s")
      .apply()
      .|.expand(
        "(a)-[r_i*1..]->(b)",
        relationshipPredicates = Seq(Predicate("  UNNAMED1", "`  UNNAMED1`.p = cacheN[z.p]")),
        pathMode = Walk
      )
      .|.allNodeScan("a", "z")
      .cacheProperties("cacheNFromStore[z.p]")
      .allNodeScan("z")
      .build()

    rewrites(walk, expand, dbFormat = DbFormat.Aligned)
    rewrites(walk, expand, executionModels = Seq(ExecutionModel.Volcano))
    preserves(walk, dbFormat = DbFormat.Block, executionModels = Seq(ExecutionModel.Batched.default))
  }

  // pre-filter relationship type predicate
  test("Rewrites MATCH REPEATABLE ELEMENTS (a) ((n)-[r:T]->(m))+ (b) RETURN 1 AS s") {
    val walk = subPlanBuilder
      .repeatWalk(`(a) ((n)-[r]-(m))+ (b)`.emptyWalk)
      .|.expand("(n_i)-[r_i:T]->(m_i)")
      .|.argument("n_i")
      .allNodeScan("a")
      .build()
    val expand = subPlanBuilder
      .expand("(a)-[r_i:T*1..]->(b)", pathMode = Walk)
      .allNodeScan("a")
      .build()
    rewrites(walk, expand)
  }

  // pre-filter relationship property predicate
  test(s"Rewrites MATCH REPEATABLE ELEMENTS (a) ((n)-[r WHERE r.p = 0]->(m))+ (b) RETURN 1 AS s") {
    val walk = subPlanBuilder
      .repeatWalk(`(a) ((n)-[r]-(m))+ (b)`.emptyWalk)
      .|.filter("r_i.p = 0")
      .|.expand("(n_i)-[r_i]->(m_i)")
      .|.argument("n_i")
      .allNodeScan("a")
      .build()
    val expand = subPlanBuilder
      .expand(
        "(a)-[r_i*1..]->(b)",
        relationshipPredicates = Seq(Predicate("  UNNAMED1", "`  UNNAMED1`.p = 0")),
        pathMode = Walk
      )
      .allNodeScan("a")
      .build()

    rewrites(walk, expand, dbFormat = DbFormat.Aligned)
    rewrites(walk, expand, executionModels = Seq(ExecutionModel.Volcano))
    preserves(walk, dbFormat = DbFormat.Block, executionModels = Seq(ExecutionModel.Batched.default))
  }

  // post-filter predicate
  test("Rewrites MATCH REPEATABLE ELEMENTS (a) ((n)-[r]->(m))+ (b) WHERE all(x IN r WHERE x:T) RETURN 1 AS s") {
    val walk = subPlanBuilder
      .projection("1 AS s")
      .filter("all(x IN r WHERE x:T)")
      .repeatWalk(`(a) ((n)-[r]-(m))+ (b)`.nmlessWalk)
      .|.expand("(n_i)-[r_i]->(m_i)")
      .|.argument("n_i")
      .allNodeScan("a")
      .build()
    val expand = subPlanBuilder
      .projection("1 AS s")
      .filter("all(x IN r WHERE x:T)")
      .expand("(a)-[r*1..]->(b)", pathMode = Walk)
      .allNodeScan("a")
      .build()

    rewrites(walk, expand)
  }

  // quantifier with kleene star
  test("Rewrites MATCH REPEATABLE ELEMENTS (a) ((n)-[r]->(m))* (b) RETURN 1 AS s") {
    val walk = subPlanBuilder
      .projection("1 AS s")
      .repeatWalk(`(a) ((n)-[r]-(m))+ (b)`.emptyWalk.withWalkQuantifier(0, Unlimited))
      .|.expand("(n_i)-[r_i]->(m_i)")
      .|.argument("n_i")
      .allNodeScan("a")
      .build()

    val expand = subPlanBuilder
      .projection("1 AS s")
      .expand("(a)-[r_i*0..]->(b)", pathMode = Walk)
      .allNodeScan("a")
      .build()

    rewrites(walk, expand)
  }

  // quantifier with limited ub
  test("Rewrites MATCH REPEATABLE ELEMENTS (a) ((n)-[r]->(m)){,2} (b) RETURN 1 AS s") {
    val walk = subPlanBuilder
      .projection("1 AS s")
      .repeatWalk(`(a) ((n)-[r]-(m))+ (b)`.emptyWalk.withWalkQuantifier(0, Limited(2)))
      .|.expand("(n_i)-[r_i]->(m_i)")
      .|.argument("n_i")
      .allNodeScan("a")
      .build()

    val expand = subPlanBuilder
      .projection("1 AS s")
      .expand("(a)-[r_i*0..2]->(b)", pathMode = Walk)
      .allNodeScan("a")
      .build()

    rewrites(walk, expand)
  }

  // quantifier with unlimited ub
  test("Rewrites MATCH REPEATABLE ELEMENTS (a) ((n)-[r]->(m)){2,} (b) RETURN 1 AS s") {
    val walk = subPlanBuilder
      .projection("1 AS s")
      .repeatWalk(`(a) ((n)-[r]-(m))+ (b)`.emptyWalk.withWalkQuantifier(2, Unlimited))
      .|.expand("(n_i)-[r_i]->(m_i)")
      .|.argument("n_i")
      .allNodeScan("a")
      .build()

    val expand = subPlanBuilder
      .projection("1 AS s")
      .expand("(a)-[r_i*2..]->(b)", pathMode = Walk)
      .allNodeScan("a")
      .build()

    rewrites(walk, expand)
  }

  // quantifier with equal lb and ub
  test("Rewrites MATCH REPEATABLE ELEMENTS (a) ((n)-[r]->(m)){2,2} (b) RETURN 1 AS s") {
    val walk = subPlanBuilder
      .projection("1 AS s")
      .repeatWalk(`(a) ((n)-[r]-(m))+ (b)`.emptyWalk.withWalkQuantifier(2, Limited(2)))
      .|.expand("(n_i)-[r_i]->(m_i)")
      .|.argument("n_i")
      .allNodeScan("a")
      .build()

    val expand = subPlanBuilder
      .projection("1 AS s")
      .expand("(a)-[r_i*2..2]->(b)", pathMode = Walk)
      .allNodeScan("a")
      .build()

    rewrites(walk, expand)
  }

  // quantifier with different lb and ub
  test("Rewrites MATCH REPEATABLE ELEMENTS (a) ((n)-[r]->(m)){2,5} (b) RETURN 1 AS s") {
    val walk = subPlanBuilder
      .projection("1 AS s")
      .repeatWalk(`(a) ((n)-[r]-(m))+ (b)`.emptyWalk.withWalkQuantifier(2, Limited(5)))
      .|.expand("(n_i)-[r_i]->(m_i)")
      .|.argument("n_i")
      .allNodeScan("a")
      .build()

    val expand = subPlanBuilder
      .projection("1 AS s")
      .expand("(a)-[r_i*2..5]->(b)", pathMode = Walk)
      .allNodeScan("a")
      .build()

    rewrites(walk, expand)
  }

  // cannot convert quantifier from long to int
  test("Preserves MATCH REPEATABLE ELEMENTS (a) ((n)-[r]->(m){,3000000000} (b) RETURN 1 AS s") {
    val walk = subPlanBuilder
      .projection("1 AS s")
      .repeatWalk(`(a) ((n)-[r]-(m))+ (b)`.emptyWalk.withWalkQuantifier(0, Limited(3000000000L)))
      .|.expand("(n_i)-[r_i]->(m_i)")
      .|.argument("n_i")
      .allNodeScan("a")
      .build()
    preserves(walk)
  }

  // dir=outgoing, reverseGroupVariableProjections
  test(
    "Rewrites MATCH REPEATABLE ELEMENTS (a) ((n)-[r]->(m))+ (b) RETURN 1 AS s, reverseGroupVariableProjections=true"
  ) {
    val walk = subPlanBuilder
      .projection("1 AS s")
      .repeatWalk(`(a) ((n)-[r]-(m))+ (b)`.emptyWalk.reverse)
      .|.expand("(m_i)<-[r_i]-(n_i)")
      .|.argument("m_i")
      .allNodeScan("b")
      .build()

    val expand = subPlanBuilder
      .projection("1 AS s")
      .expand("(b)<-[r_i*1..]-(a)", pathMode = Walk)
      .allNodeScan("b")
      .build()

    rewrites(walk, expand)
  }

  // dir=incoming
  test("Rewrites MATCH REPEATABLE ELEMENTS (a) ((n)<-[r]-(m))+ (b) RETURN 1 AS s") {
    val walk = subPlanBuilder
      .projection("1 AS s")
      .repeatWalk(`(a) ((n)-[r]-(m))+ (b)`.emptyWalk)
      .|.expand("(n_i)<-[r_i]-(m_i)")
      .|.argument("n_i")
      .allNodeScan("a")
      .build()

    val expand = subPlanBuilder
      .projection("1 AS s")
      .expand("(a)<-[r_i*1..]-(b)", projectedDir = INCOMING, pathMode = Walk)
      .allNodeScan("a")
      .build()

    rewrites(walk, expand)
  }

  // dir=incoming, reverseGroupVariableProjections
  test(
    "Rewrites MATCH REPEATABLE ELEMENTS (a) ((n)<-[r]-(m))+ (b) RETURN 1 AS s, reverseGroupVariableProjections=true"
  ) {
    val walk = subPlanBuilder
      .projection("1 AS s")
      .repeatWalk(`(a) ((n)-[r]-(m))+ (b)`.emptyWalk.reverse)
      .|.expand("(m_i)-[r_i]->(n_i)")
      .|.argument("m_i")
      .allNodeScan("b")
      .build()

    val expand = subPlanBuilder
      .projection("1 AS s")
      .expand("(b)-[r_i*1..]->(a)", projectedDir = INCOMING, pathMode = Walk)
      .allNodeScan("b")
      .build()

    rewrites(walk, expand)
  }

  // dir=both
  test("Rewrites MATCH REPEATABLE ELEMENTS (a) ((n)-[r]-(m))+ (b) RETURN 1 AS s") {
    val walk = subPlanBuilder
      .projection("1 AS s")
      .repeatWalk(`(a) ((n)-[r]-(m))+ (b)`.emptyWalk)
      .|.expand("(n)-[r]-(m)")
      .|.argument("n_i")
      .allNodeScan("a")
      .build()

    val expand = subPlanBuilder
      .projection("1 AS s")
      .expand("(a)-[r_i*1..]-(b)", pathMode = Walk)
      .allNodeScan("a")
      .build()

    rewrites(walk, expand)
  }

  // dir=both, reverseGroupVariableProjections
  test(
    "Rewrites MATCH REPEATABLE ELEMENTS (a) ((n)-[r]-(m))+ (b) RETURN 1 AS s, reverseGroupVariableProjections=true"
  ) {
    val walk = subPlanBuilder
      .projection("1 AS s")
      .repeatWalk(`(a) ((n)-[r]-(m))+ (b)`.emptyWalk.reverse)
      .|.expand("(m_i)-[r_i]-(n_i)")
      .|.argument("m_i")
      .allNodeScan("b")
      .build()

    val expand = subPlanBuilder
      .projection("1 AS s")
      .expand("(b)-[r_i*1..]-(a)", projectedDir = INCOMING, pathMode = Walk)
      .allNodeScan("b")
      .build()

    rewrites(walk, expand)
  }

  // qpp + relationship pattern, does not insert relationship uniqueness predicate because Walk has no previously bound
  // relationships
  test("Rewrites MATCH REPEATABLE ELEMENTS (b)-[rr]-(a) ((n)-[r]->(m))+ RETURN 1 AS s") {
    val walk = subPlanBuilder
      .projection("1 AS s")
      .filter("not rr IN r")
      .expand("(b)-[rr]-(c)", pathMode = Walk)
      .repeatWalk(`(a) ((n)-[r]-(m))+ (b)`.nmlessWalk)
      .|.expand("(n_i)-[r_i]->(m_i)")
      .|.argument("n_i")
      .allNodeScan("a")
      .build()

    val expand = subPlanBuilder
      .projection("1 AS s")
      .filter("not rr IN r")
      .expand("(b)-[rr]-(c)", pathMode = Walk)
      .expand("(a)-[r*1..]->(b)", pathMode = Walk)
      .allNodeScan("a")
      .build()

    rewrites(walk, expand)
  }

  // qpp + relationship pattern, does not insert relationship uniqueness predicate because Walk has no previously bound
  // relationships
  test("Rewrites MATCH REPEATABLE ELEMENTS (a) ((n)-[r:R]->(m))+ (b)-[rr:RR]-(c) RETURN 1 AS s") {
    val walk = subPlanBuilder
      .projection("1 AS s")
      .repeatWalk(`(a) ((n)-[r]-(m))+ (b)`.emptyWalk.reverse)
      .|.expand("(m_i)<-[r_i:R]-(n_i)")
      .|.argument("m_i")
      .allRelationshipsScan("(b)-[rr:RR]-(c)")
      .build()

    val expand = subPlanBuilder
      .projection("1 AS s")
      .expand("(b)<-[r_i:R*1..]-(a)", pathMode = Walk)
      .allRelationshipsScan("(b)-[rr:RR]-(c)")
      .build()

    rewrites(walk, expand)
  }

  // two qpps with provably different relationship types. do not insert any relationship uniqueness predicates because
  // there are no previously bound relationship group variables
  test("Rewrites MATCH REPEATABLE ELEMENTS (a) ((n)-[r:R]->(m))+ (b) ((x)-[rr:RR]->(y))+ (c) RETURN 1 AS s") {

    val walk = subPlanBuilder
      .projection("1 AS s")
      .repeatWalk(`(a) ((n)-[r]-(m))+ (b)`.emptyWalk.reverse)
      .|.expand("(m_i)<-[r_i:R]-(n_i)")
      .|.argument("m_i")
      .repeatWalk(`(b) ((x)-[rr]-(y))+ (c)`.emptyWalk)
      .|.expand("(x_i)-[rr_i:RR]->(y_i)")
      .|.argument("x_i")
      .allNodeScan("b")
      .build()

    val expand = subPlanBuilder
      .projection("1 AS s")
      .expand("(b)<-[r_i:R*1..]-(a)", pathMode = Walk)
      .expand("(b)-[rr_i:RR*1..]->(c)", pathMode = Walk)
      .allNodeScan("b")
      .build()

    rewrites(walk, expand)
  }

  test(
    "Rewrites MATCH REPEATABLE ELEMENTS (a) ((n)-[r:R]->(m) WHERE n.prop <> m.prop)+ (b) ((x)-[rr:RR]->(y {name: 'foo'))+ (c) RETURN 1 AS s"
  ) {

    val walk = subPlanBuilder
      .projection("1 AS s")
      .repeatWalk(`(a) ((n)-[r]-(m))+ (b)`.emptyWalk.reverse)
      .|.filter("n_i.prop <> m_i.prop")
      .|.expand("(m_i)<-[r_i:R]-(n_i)")
      .|.argument("m_i")
      .repeatWalk(`(b) ((x)-[rr]-(y))+ (c)`.emptyWalk)
      .|.filter("y_i.name = 'foo'")
      .|.expand("(x_i)-[rr_i:RR]->(y_i)")
      .|.argument("x_i")
      .allNodeScan("b")
      .build()

    val expand = subPlanBuilder
      .projection("1 AS s")
      .expand(
        "(b)<-[r_i:R*1..]-(a)",
        relationshipPredicates =
          Seq(Predicate("  UNNAMED3", "startNode(`  UNNAMED3`).prop <> endNode(`  UNNAMED3`).prop")),
        pathMode = Walk
      )
      .expand(
        "(b)-[rr_i:RR*1..]->(c)",
        relationshipPredicates = Seq(Predicate("  UNNAMED1", "endNode(`  UNNAMED1`).name = 'foo'")),
        pathMode = Walk
      )
      .allNodeScan("b")
      .build()

    rewrites(walk, expand)
  }

  // mix qpps and relationship pattern, does not insert relationship uniqueness predicates for walk
  test("Rewrites MATCH REPEATABLE ELEMENTS (a) ((n)-[r]->(m))+ (b)-[rr]->(c) ((x)-[rrr]->(y))+ (d) RETURN 1 AS s") {
    object `(c) ((x)-[rrr]-(y))+ (d)` {
      val empty: WalkParameters = WalkParameters(
        min = 1,
        max = Unlimited,
        start = "c",
        end = "d",
        innerStart = "x_i",
        innerEnd = "y_i",
        groupNodes = Set.empty,
        groupRelationships = Set.empty,
        innerRelationships = Set("rrr_i"),
        reverseGroupVariableProjections = false,
        expansionMode = ExpandAll,
        accumulators = Set.empty
      )
    }
    val walk = subPlanBuilder
      .projection("1 AS s")
      .repeatWalk(`(c) ((x)-[rrr]-(y))+ (d)`.empty)
      .|.expand("(x_i)-[rrr_i]->(y_i)")
      .|.argument("x_i")
      .filter(noneOfRels(v"rr", v"r"))
      .expand("(b)-[rr]->(c)", pathMode = Walk)
      .repeatWalk(`(a) ((n)-[r]-(m))+ (b)`.nmlessWalk)
      .|.expand("(n_i)-[r_i]->(m_i)")
      .|.argument("n_i")
      .allNodeScan("a")
      .build()
    val expand = subPlanBuilder
      .projection("1 AS s")
      .expand("(c)-[rrr_i*1..]->(d)", pathMode = Walk)
      .filter(noneOfRels(v"rr", v"r"))
      .expand("(b)-[rr]->(c)", pathMode = Walk)
      .expand("(a)-[r*1..]->(b)", pathMode = Walk)
      .allNodeScan("a")
      .build()

    rewrites(walk, expand)
  }

  // does not rewrite directly to PruningVarExpand (responsibility of pruningVarExpander)
  test("Rewrites MATCH REPEATABLE ELEMENTS (a) ((n)-[r]->(m))+ (b) RETURN DISTINCT b") {
    val walk = subPlanBuilder
      .distinct("b AS b")
      .repeatWalk(`(a) ((n)-[r]-(m))+ (b)`.emptyWalk)
      .|.expand("(n_i)-[r_i]->(m_i)")
      .|.argument("n_i")
      .allNodeScan("a")
      .build()
    val expand = subPlanBuilder
      .distinct("b AS b")
      .expand("(a)-[r_i*1..]->(b)", pathMode = Walk)
      .allNodeScan("a")
      .build()

    rewrites(walk, expand)
  }

  // named path uses group node variables
  test("Preserves MATCH REPEATABLE ELEMENTS p = (a) ((n)-[r]->(m))+ (b) RETURN p") {
    val walk = subPlanBuilder
      .projection(Map("p" -> qppPath(v"a", Seq(v"n", v"r"), v"b")))
      .repeatWalk(`(a) ((n)-[r]-(m))+ (b)`.mlessWalk)
      .|.expand("(n_i)-[r_i]->(m_i)")
      .|.argument("n_i")
      .allNodeScan("a")
      .build()
    preserves(walk)
  }

  val `Walk (a) ((x_i)-[r_i]-(y_i))+ (  UNNAMED4)`: WalkParameters = WalkParameters(
    min = 1,
    max = Unlimited,
    start = "a",
    end = "  UNNAMED4",
    innerStart = "x_i",
    innerEnd = "y_i",
    groupNodes = Set.empty,
    groupRelationships = Set.empty,
    innerRelationships = Set("r_i"),
    reverseGroupVariableProjections = false,
    expansionMode = ExpandAll,
    accumulators = Set.empty
  )

  val `Walk (a) ((x_i)-[r_i]-(y_i))+ (b)`: WalkParameters = WalkParameters(
    min = 1,
    max = Unlimited,
    start = "a",
    end = "b",
    innerStart = "x_i",
    innerEnd = "y_i",
    groupNodes = Set.empty,
    groupRelationships = Set.empty,
    innerRelationships = Set("r_i"),
    reverseGroupVariableProjections = false,
    expansionMode = ExpandInto,
    accumulators = Set.empty
  )

  test("Rewrite selection and walk to VarLengthExpand(Into)") {
    val selectionAndWalk = subPlanBuilder
      .projection("1 AS s")
      .repeatWalk(`Walk (a) ((x_i)-[r_i]-(y_i))+ (b)`)
      .|.expandAll("(`  UNNAMED2`)-[`r_i`]->(`  UNNAMED3`)")
      .|.argument("  UNNAMED1")
      .allRelationshipsScan("(a)-[r_j]->(b)")
      .build()

    val expand = subPlanBuilder
      .projection("1 AS s")
      .expand("(a)-[r_i*1..]->(b)", ExpandInto, pathMode = Walk)
      .allRelationshipsScan("(a)-[r_j]->(b)")
      .build()

    rewrites(selectionAndWalk, expand)
  }

  test(
    "Rewrite selection and walk to VarLengthExpand(Into) - cartesian product and expandInto for the first relationship"
  ) {
    val selectionAndWalk = subPlanBuilder
      .projection("1 AS s")
      .repeatWalk(`Walk (a) ((x_i)-[r_i]-(y_i))+ (b)`)
      .|.expandAll("(`  UNNAMED2`)-[`r_i`]->(`  UNNAMED3`)")
      .|.argument("  UNNAMED1")
      .expandInto("(a)-[r_j]->(b)")
      .cartesianProduct()
      .|.allNodeScan("b")
      .allNodeScan("a")
      .build()

    val expand = subPlanBuilder
      .projection("1 AS s")
      .expand("(a)-[r_i*1..]->(b)", ExpandInto, pathMode = Walk)
      .expandInto("(a)-[r_j]->(b)")
      .cartesianProduct()
      .|.allNodeScan("b")
      .allNodeScan("a")
      .build()

    rewrites(selectionAndWalk, expand)
  }

  test(
    "Do not rewrite selection and walk to VarLengthExpand(Into) when the selection does not refer to the end node of the walk - it refers to the innerEnd"
  ) {
    // This plan cannot be generated because the filter cannot refer to the innerEnd node of the QPP.
    // It tests that having a `  UNNAMED` node is not enough. It should be the end point of the QPP.
    val selectionAndWalk = subPlanBuilder
      .projection("1 AS s")
      .filter("`  UNNAMED3` = b")
      .repeatWalk(`Walk (a) ((x_i)-[r_i]-(y_i))+ (  UNNAMED4)`)
      .|.expandAll("(`  UNNAMED2`)-[`r_i`]->(`  UNNAMED3`)")
      .|.argument("  UNNAMED1")
      .allRelationshipsScan("(a)-[r_j]->(b)")
      .build()

    val expand = subPlanBuilder
      .projection("1 AS s")
      .filter("`  UNNAMED3` = b")
      .expand("(a)-[r_i*1..]->(`  UNNAMED4`)", ExpandAll, pathMode = Walk)
      .allRelationshipsScan("(a)-[r_j]->(b)")
      .build()

    rewrites(selectionAndWalk, expand)
  }

  test(
    "Do not rewrite selection and walk to VarLengthExpand(Into) when the selection does not refer to the end node of the walk - it refers to a previously bound node"
  ) {
    val selectionAndWalk = subPlanBuilder
      .projection("1 AS s")
      .filter("a = b")
      .repeatWalk(`Walk (a) ((x_i)-[r_i]-(y_i))+ (  UNNAMED4)`)
      .|.expandAll("(`  UNNAMED2`)-[`r_i`]->(`  UNNAMED3`)")
      .|.argument("  UNNAMED1")
      .allRelationshipsScan("(a)-[r_j]->(b)")
      .build()

    val expand = subPlanBuilder
      .projection("1 AS s")
      .filter("a = b")
      .expand("(a)-[r_i*1..]->(`  UNNAMED4`)", ExpandAll, pathMode = Walk)
      .allRelationshipsScan("(a)-[r_j]->(b)")
      .build()

    rewrites(selectionAndWalk, expand)
  }

  test(
    "Do not rewrite selection and walk to VarLengthExpand(Into) when the selection does not refer a previously bound node"
  ) {
    // This plan cannot be generated because 'c' in the filter does not refer to anything
    val selectionAndWalk = subPlanBuilder
      .projection("1 AS s")
      .expandAll("(b)-[r_k]->(c)")
      .filter("`  UNNAMED4` = c")
      .repeatWalk(`Walk (a) ((x_i)-[r_i]-(y_i))+ (  UNNAMED4)`)
      .|.expandAll("(`  UNNAMED2`)-[`r_i`]->(`  UNNAMED3`)")
      .|.argument("  UNNAMED1")
      .allRelationshipsScan("(a)-[r_j]->(b)")
      .build()

    val expand = subPlanBuilder
      .projection("1 AS s")
      .expandAll("(b)-[r_k]->(c)")
      .filter("`  UNNAMED4` = c")
      .expand("(a)-[r_i*1..]->(`  UNNAMED4`)", ExpandAll, pathMode = Walk)
      .allRelationshipsScan("(a)-[r_j]->(b)")
      .build()

    rewrites(selectionAndWalk, expand)
  }

  test(
    "Do not rewrite selection and walk to VarLengthExpand(Into) when the selection does not refer a previously bound node - it refers to the innerEnd"
  ) {
    val selectionAndWalk = subPlanBuilder
      .projection("1 AS s")
      .filter("`  UNNAMED4` = `  UNNAMED3`")
      .repeatWalk(`Walk (a) ((x_i)-[r_i]-(y_i))+ (  UNNAMED4)`)
      .|.expandAll("(`  UNNAMED2`)-[`r_i`]->(`  UNNAMED3`)")
      .|.argument("  UNNAMED1")
      .allRelationshipsScan("(a)-[r_j]->(b)")
      .build()

    val expand = subPlanBuilder
      .projection("1 AS s")
      .filter("`  UNNAMED4` = `  UNNAMED3`")
      .expand("(a)-[r_i*1..]->(`  UNNAMED4`)", ExpandAll, pathMode = Walk)
      .allRelationshipsScan("(a)-[r_j]->(b)")
      .build()

    rewrites(selectionAndWalk, expand)
  }

  test(
    "Do not rewrite selection and walk to VarLengthExpand(Into) when the endpoint of the walk is not an UNNAMED node"
  ) {
    // Node 'b' might be used in other places of the query, therefore we cannot override it with 'c' to create a VarLengthExpand(Into)
    val selectionAndWalk = subPlanBuilder
      .projection("1 AS s")
      .filter("b = c")
      .repeatWalk(`(a) ((n)-[r]-(m))+ (b)`.emptyWalk)
      .|.expandAll("(`  UNNAMED2`)-[`r_i`]->(`  UNNAMED3`)")
      .|.argument("  UNNAMED1")
      .allRelationshipsScan("(a)-[r_j]->(c)")
      .build()

    val expand = subPlanBuilder
      .projection("1 AS s")
      .filter("b = c")
      .expand("(a)-[r_i*1..]->(b)", ExpandAll, pathMode = Walk)
      .allRelationshipsScan("(a)-[r_j]->(c)")
      .build()

    rewrites(selectionAndWalk, expand)
  }

  test("Do not rewrite selection and walk to VarLengthExpand(Into) when the selection has more than 1 predicate") {
    val selectionAndWalk = subPlanBuilder
      .projection("1 AS s")
      .filter("`  UNNAMED4` = b", "not a = b")
      .repeatWalk(`Walk (a) ((x_i)-[r_i]-(y_i))+ (  UNNAMED4)`)
      .|.expandAll("(`  UNNAMED2`)-[`r_i`]->(`  UNNAMED3`)")
      .|.argument("  UNNAMED1")
      .allRelationshipsScan("(a)-[r_j]->(b)")
      .build()

    val expand = subPlanBuilder
      .projection("1 AS s")
      .filter("`  UNNAMED4` = b", "not a = b")
      .expand("(a)-[r_i*1..]->(`  UNNAMED4`)", ExpandAll, pathMode = Walk)
      .allRelationshipsScan("(a)-[r_j]->(b)")
      .build()

    rewrites(selectionAndWalk, expand)
  }

  test(
    "Rewrites MATCH REPEATABLE ELEMENTS (a) ((n WHERE n.prop > 123)-[r]->(m WHERE m.prop < 321))+ (b) RETURN 1 AS s (directed relationship)"
  ) {
    val walk = subPlanBuilder
      .projection("1 AS s")
      .repeatWalk(`(a) ((n)-[r]-(m))+ (b)`.emptyWalk)
      .|.filter("m_i.prop < 321")
      .|.expand("(n_i)-[r_i]->(m_i)")
      .|.filter("n_i.prop > 123")
      .|.argument("n_i")
      .allNodeScan("a")
      .build()
    val expand = subPlanBuilder
      .projection("1 AS s")
      .expand(
        "(a)-[r_i*1..]->(b)",
        relationshipPredicates = Seq(
          Predicate("  UNNAMED1", "endNode(`  UNNAMED1`).prop < 321"),
          Predicate("  UNNAMED1", "startNode(`  UNNAMED1`).prop > 123")
        ),
        pathMode = Walk
      )
      .allNodeScan("a")
      .build()

    rewrites(walk, expand, extractor = RewritableRepeatExtractor.FilterBeforeAndAfterExpand)
    preserves(walk, extractor = RewritableRepeatExtractor.FilterAfterExpand)
  }

  test(
    "Rewrites MATCH REPEATABLE ELEMENTS (a) ((n WHERE n.prop > 123)-[r]-(m WHERE m.prop < 321))+ (b) RETURN 1 AS s (undirected relationship)"
  ) {
    val walk = subPlanBuilder
      .projection("1 AS s")
      .repeatWalk(`(a) ((n)-[r]-(m))+ (b)`.emptyWalk)
      .|.filter("m_i.prop < 321")
      .|.expand("(n_i)-[r_i]-(m_i)")
      .|.filter("n_i.prop > 123")
      .|.argument("n_i")
      .allNodeScan("a")
      .build()

    val TO = TraversalEndpoint(tempVar = v"  UNNAMED2", endpoint = To)
    val FROM = TraversalEndpoint(tempVar = v"  UNNAMED3", endpoint = From)

    val expand = subPlanBuilder
      .projection("1 AS s")
      .expandExpr(
        "(a)-[r_i*1..]-(b)",
        relationshipPredicates = Seq(
          VariablePredicate(v"  UNNAMED1", lessThan(prop(TO, "prop"), literalInt(321))),
          VariablePredicate(v"  UNNAMED1", greaterThan(prop(FROM, "prop"), literalInt(123)))
        ),
        pathMode = Walk
      )
      .allNodeScan("a")
      .build()

    rewrites(walk, expand, extractor = RewritableRepeatExtractor.FilterBeforeAndAfterExpand)
    preserves(walk, extractor = RewritableRepeatExtractor.FilterAfterExpand)
  }

  test("does not rewrite Repeat with unique index seek on LHS in Parallel runtime") {
    val trail = subPlanBuilder
      .projection("1 AS s")
      .repeatTrail(`(a) ((n)-[r]-(m))+ (b)`.emptyTrail)
      .|.filter(isRepeatTrailUnique("r_i"))
      .|.expand("(n_i)-[r_i]->(m_i)")
      .|.argument("n_i")
      .nodeIndexOperator("a:A(prop = 123)", unique = true)
      .build()

    val expand = subPlanBuilder
      .projection("1 AS s")
      .expand("(a)-[r_i*1..]->(b)")
      .nodeIndexOperator("a:A(prop = 123)", unique = true)
      .build()

    rewrites(trail, expand, cypherParallelRepeatHeuristicOptionEnabled = true)
    preserves(
      trail,
      executionModels = Seq(ExecutionModel.Batched.default.asParallel),
      cypherParallelRepeatHeuristicOptionEnabled = true
    )
  }

  test("rewrites Repeat with unique index seek on LHS, nested under Apply, including in Parallel runtime") {
    val trail = subPlanBuilder
      .apply()
      .|.projection("1 AS s")
      .|.repeatTrail(`(a) ((n)-[r]-(m))+ (b)`.emptyTrail)
      .|.|.filter(isRepeatTrailUnique("r_i"))
      .|.|.expand("(n_i)-[r_i]->(m_i)")
      .|.|.argument("n_i")
      .|.nodeIndexOperator("a:A(prop = x.prop)", unique = true, argumentIds = Set("x"))
      .allNodeScan("x")
      .build()

    val expand = subPlanBuilder
      .apply()
      .|.projection("1 AS s")
      .|.expand("(a)-[r_i*1..]->(b)")
      .|.nodeIndexOperator("a:A(prop = x.prop)", unique = true, argumentIds = Set("x"))
      .allNodeScan("x")
      .build()

    rewrites(trail, expand)
    rewrites(trail, expand, executionModels = Seq(ExecutionModel.Batched.default.asParallel))
  }

  test("only accepts logical plan as an input") {
    val plan = subPlanBuilder.argument().build()
    val instance = rewriter(
      isBlockFormat = false,
      executionModelSupportsCursorReuseInBlockFormat = false,
      extractor = FilterAfterExpand,
      isParallel = false,
      cypherParallelRepeatHeuristicOptionEnabled = false
    )
    an[IllegalArgumentException] should be thrownBy instance.apply(Vector(plan, plan))
  }

  private def rewrites(
    repeat: LogicalPlan,
    expand: LogicalPlan,
    dbFormat: DbFormat = DbFormat.All,
    executionModels: Seq[ExecutionModel] = Seq(ExecutionModel.Volcano, ExecutionModel.Batched.default),
    extractor: RewritableRepeatExtractor = RewritableRepeatExtractor.FilterAfterExpand,
    cypherParallelRepeatHeuristicOptionEnabled: Boolean = false
  ): Unit =
    for {
      isBlockFormat <- dbFormat.isBlockFormat
      em <- executionModels
      supportsCursorReuse = em.supportsCursorReuseInBlockFormat
      isParallel = em.isParallel
    } withClue(
      s"isBlockFormat = $isBlockFormat, supportsCursorReuse = $supportsCursorReuse, extractor = $extractor, isParallel = $isParallel\n"
    ) {
      rewrite(
        repeat,
        isBlockFormat,
        supportsCursorReuse,
        extractor,
        isParallel,
        cypherParallelRepeatHeuristicOptionEnabled
      ).stripProduceResults shouldEqual expand.stripProduceResults
    }

  private def preserves(
    repeat: LogicalPlan,
    dbFormat: DbFormat = DbFormat.All,
    executionModels: Seq[ExecutionModel] = Seq(ExecutionModel.Volcano, ExecutionModel.Batched.default),
    extractor: RewritableRepeatExtractor = RewritableRepeatExtractor.FilterAfterExpand,
    cypherParallelRepeatHeuristicOptionEnabled: Boolean = false
  ): Unit = {
    for {
      isBlockFormat <- dbFormat.isBlockFormat
      em <- executionModels
      supportsCursorReuse = em.supportsCursorReuseInBlockFormat
      isParallel = em.isParallel
    } withClue(
      s"isBlockFormat = $isBlockFormat, supportsCursorReuse = $supportsCursorReuse, extractor = $extractor, isParallel = $isParallel\n"
    ) {
      rewrite(
        repeat,
        isBlockFormat,
        supportsCursorReuse,
        extractor,
        isParallel,
        cypherParallelRepeatHeuristicOptionEnabled
      ).stripProduceResults shouldEqual repeat.stripProduceResults
    }
  }

  private def rewrite(
    p: LogicalPlan,
    isBlockFormat: Boolean,
    executionModelSupportsCursorReuseInBlockFormat: Boolean,
    extractor: RepeatToVarExpandRewriter.RewritableRepeatExtractor,
    isParallel: Boolean,
    cypherParallelRepeatHeuristicOptionEnabled: Boolean
  ): LogicalPlan =
    p.endoRewrite(rewriter(
      isBlockFormat = isBlockFormat,
      executionModelSupportsCursorReuseInBlockFormat = executionModelSupportsCursorReuseInBlockFormat,
      extractor = extractor,
      isParallel = isParallel,
      cypherParallelRepeatHeuristicOptionEnabled = cypherParallelRepeatHeuristicOptionEnabled
    ))

  private def rewriter(
    isBlockFormat: Boolean,
    executionModelSupportsCursorReuseInBlockFormat: Boolean,
    extractor: RepeatToVarExpandRewriter.RewritableRepeatExtractor,
    isParallel: Boolean,
    cypherParallelRepeatHeuristicOptionEnabled: Boolean
  ): RepeatToVarExpandRewriter = {
    RepeatToVarExpandRewriter(
      new StubLabelAndRelTypeInfos,
      Attributes(idGen, new StubSolveds),
      new AnonymousVariableNameGenerator,
      rewritableRepeatExtractor = extractor,
      isShardedDatabase = false,
      heuristics = Set(
        Some(RepeatToVarExpandRewriter.Heuristic.PreferRepeatForRelationshipPredicatesWithCursorReuseOnBlock(
          isBlockFormat,
          executionModelSupportsCursorReuseInBlockFormat
        )),
        Option.when(cypherParallelRepeatHeuristicOptionEnabled)(
          RepeatToVarExpandRewriter.Heuristic.PreferRepeatForBetterParallelizationWhenInputCardinalityIsExactlyOne(
            isParallelRuntime = isParallel
          )
        )
      ).flatten
    )
  }

  private def subPlanBuilder = new LogicalPlanBuilder(wholePlan = false)
}

object RepeatToVarExpandRewriterTest {

  implicit class TrailParametersOps(params: TrailParameters) {

    def reverse: TrailParameters = params.copy(
      end = params.start,
      start = params.end,
      innerEnd = params.innerStart,
      innerStart = params.innerEnd,
      reverseGroupVariableProjections = !params.reverseGroupVariableProjections
    )

    def withPreviouslyBoundRel(r: String*): TrailParameters = params.copy(previouslyBoundRelationships = Set(r: _*))

    def withPreviouslyBoundRelGroup(r: String*): TrailParameters =
      params.copy(previouslyBoundRelationshipGroups = Set(r: _*))

    def withQuantifier(min: Int, max: UpperBound): TrailParameters =
      params.copy(min = min, max = max)
  }

  implicit class WalkParametersOps(params: WalkParameters) {

    def reverse: WalkParameters = params.copy(
      end = params.start,
      start = params.end,
      innerEnd = params.innerStart,
      innerStart = params.innerEnd,
      reverseGroupVariableProjections = !params.reverseGroupVariableProjections
    )

    def withWalkQuantifier(min: Int, max: UpperBound): WalkParameters =
      params.copy(min = min, max = max)
  }

  private object `(a) ((n)-[r]-(m))+ (b)` {

    val trail: TrailParameters = TrailParameters(
      min = 1,
      max = Unlimited,
      start = "a",
      end = "b",
      innerStart = "n_i",
      innerEnd = "m_i",
      groupNodes = Set(("n_i", "n"), ("m_i", "m")),
      groupRelationships = Set(("r_i", "r")),
      innerRelationships = Set("r_i"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = false,
      expansionMode = ExpandAll,
      accumulators = Set.empty
    )

    val walk: WalkParameters = WalkParameters(
      min = 1,
      max = Unlimited,
      start = "a",
      end = "b",
      innerStart = "n_i",
      innerEnd = "m_i",
      groupNodes = Set(("n_i", "n"), ("m_i", "m")),
      groupRelationships = Set(("r_i", "r")),
      reverseGroupVariableProjections = false,
      innerRelationships = Set("r_i"),
      expansionMode = ExpandAll,
      accumulators = Set.empty
    )

    val nlessTrail: TrailParameters = trail.copy(groupNodes = Set(("m_i", "m")))

    val rlessTrail: TrailParameters = trail.copy(groupRelationships = Set.empty)

    val mlessTrail: TrailParameters = trail.copy(groupNodes = Set(("n_i", "n")))

    val nmlessTrail: TrailParameters = trail.copy(groupNodes = Set.empty)

    val rnlessTrail: TrailParameters = trail.copy(groupNodes = Set(("m_i", "m")), groupRelationships = Set.empty)

    val rmlessTrail: TrailParameters = trail.copy(groupNodes = Set(("n_i", "n")), groupRelationships = Set.empty)

    val emptyTrail: TrailParameters = trail.copy(groupNodes = Set.empty, groupRelationships = Set.empty)

    val nlessWalk: WalkParameters = walk.copy(groupNodes = Set(("m_i", "m")))

    val rlessWalk: WalkParameters = walk.copy(groupRelationships = Set.empty)

    val mlessWalk: WalkParameters = walk.copy(groupNodes = Set(("n_i", "n")))

    val nmlessWalk: WalkParameters = walk.copy(groupNodes = Set.empty)

    val rnlessWalk: WalkParameters = walk.copy(groupNodes = Set(("m_i", "m")), groupRelationships = Set.empty)

    val rmlessWalk: WalkParameters = walk.copy(groupNodes = Set(("n_i", "n")), groupRelationships = Set.empty)

    val emptyWalk: WalkParameters = walk.copy(groupNodes = Set.empty, groupRelationships = Set.empty)

  }

  private object `(b) ((x)-[rr]-(y))+ (c)` {

    val emptyTrail: TrailParameters = TrailParameters(
      min = 1,
      max = Unlimited,
      start = "b",
      end = "c",
      innerStart = "x_i",
      innerEnd = "y_i",
      groupNodes = Set.empty,
      groupRelationships = Set.empty,
      innerRelationships = Set("rr_i"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = false,
      expansionMode = ExpandAll,
      accumulators = Set.empty
    )

    val xylessTrail: TrailParameters = emptyTrail.copy(groupRelationships = Set(("rr_i", "rr")))

    val emptyWalk: WalkParameters = WalkParameters(
      min = 1,
      max = Unlimited,
      start = "b",
      end = "c",
      innerStart = "x_i",
      innerEnd = "y_i",
      groupNodes = Set.empty,
      groupRelationships = Set.empty,
      innerRelationships = Set("rr_i"),
      reverseGroupVariableProjections = false,
      expansionMode = ExpandAll,
      accumulators = Set.empty
    )

    val xylessWalk: WalkParameters = emptyWalk.copy(groupRelationships = Set(("rr_i", "rr")))
  }

  sealed private trait DbFormat {

    def isBlockFormat: Seq[Boolean] = {
      this match {
        case DbFormat.Block   => Seq(true)
        case DbFormat.Aligned => Seq(false)
        case DbFormat.All     => Seq(true, false)
      }
    }
  }

  private object DbFormat {
    case object Block extends DbFormat
    case object Aligned extends DbFormat
    case object All extends DbFormat
  }
}
