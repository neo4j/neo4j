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

import TrailToVarExpandRewriterTest.TrailParametersOps
import TrailToVarExpandRewriterTest.`(a) ((n)-[r]-(m))+ (b)`
import TrailToVarExpandRewriterTest.`(b) ((x)-[rr]-(y))+ (c)`
import TrailToVarExpandRewriterTest.preserves
import TrailToVarExpandRewriterTest.rewrite
import TrailToVarExpandRewriterTest.rewrites
import TrailToVarExpandRewriterTest.subPlanBuilder
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanBuilder
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanConstructionTestSupport
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanTestOps
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.expressions.SemanticDirection.INCOMING
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.Predicate
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.TrailParameters
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandAll
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandInto
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.util.UpperBound
import org.neo4j.cypher.internal.util.UpperBound.Limited
import org.neo4j.cypher.internal.util.UpperBound.Unlimited
import org.neo4j.cypher.internal.util.attribution.Attributes
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.scalatest.Assertion

// Additional tests can be found in QuantifiedPathPatternPlanningIntegrationTest
class TrailToVarExpandRewriterTest extends CypherFunSuite with LogicalPlanningTestSupport {

  // happy case
  test("Rewrites MATCH (a) ((n)-[r]->(m))+ (b) RETURN 1 AS s") {
    val trail = subPlanBuilder
      .projection("1 AS s")
      .trail(`(a) ((n)-[r]-(m))+ (b)`.empty)
      .|.filterExpression(isRepeatTrailUnique("r_i"))
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
      .trail(`(a) ((n)-[r]-(m))+ (b)`.nmless)
      .|.filterExpression(isRepeatTrailUnique("r_i"))
      .|.expand("(n_i)-[r_i]->(m_i)")
      .|.argument("n_i")
      .allNodeScan("a")
      .build()
    val expand = new LogicalPlanBuilder()
      .produceResults("r")
      .expand("(a)-[r*]->(b)")
      .allNodeScan("a")
      .build()
    rewrite(trail) should equal(expand)
  }

  // node variable n is used
  test("Preserves MATCH (a) ((n)-[r]->(m))+ (b) RETURN n") {
    val trail = subPlanBuilder
      .trail(`(a) ((n)-[r]-(m))+ (b)`.rmless)
      .|.filterExpression(isRepeatTrailUnique("r_i"))
      .|.expand("(n_i)-[r_i]->(m_i)")
      .|.argument("n_i")
      .allNodeScan("a")
      .build()
    preserves(trail)
  }

  // node variable m is used
  test("Preserves MATCH (a) ((n)-[r]->(m))+ (b) RETURN m") {
    val trail = subPlanBuilder
      .trail(`(a) ((n)-[r]-(m))+ (b)`.rnless)
      .|.filterExpression(isRepeatTrailUnique("r_i"))
      .|.expand("(n_i)-[r_i]->(m_i)")
      .|.argument("n_i")
      .allNodeScan("a")
      .build()
    preserves(trail)
  }

  // node variables n and m are used
  test("Preserves MATCH (a) ((n)-[r]->(m))+ (b) RETURN n,m") {
    val trail = subPlanBuilder
      .trail(`(a) ((n)-[r]-(m))+ (b)`.rless)
      .|.filterExpression(isRepeatTrailUnique("r_i"))
      .|.expand("(n_i)-[r_i]->(m_i)")
      .|.argument("n_i")
      .allNodeScan("a")
      .build()
    preserves(trail)
  }

  // node variable n has a predicate
  test("Preserves MATCH (a) ((n:N)-[r]->(m))+ (b) RETURN 1 AS s") {
    val trail = subPlanBuilder
      .projection("1 AS s")
      .trail(`(a) ((n)-[r]-(m))+ (b)`.empty)
      .|.filterExpression(isRepeatTrailUnique("r_i"))
      .|.expand("(n_i)-[r_i]->(m_i)")
      .|.filter("n_i:N")
      .|.argument("n_i")
      .nodeByLabelScan("a", "N")
      .build()
    preserves(trail)
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
        reverseGroupVariableProjections = false
      )
    }

    val trail = subPlanBuilder
      .projection("1 AS s")
      .trail(`(a) ((n)-[r]->(m)->[rr]->(o))+ (b)`.empty)
      .|.filterExpression(isRepeatTrailUnique("rr_i"), differentRelationships("rr_i", "r_i"))
      .|.expand("(m)-[rr_i]->(o)")
      .|.filterExpression(isRepeatTrailUnique("r_i"))
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
      .trail(`(a) ((n)-[r]-(m))+ (b)`.empty)
      .|.filterExpression(isRepeatTrailUnique("r_i"))
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
      .trail(`(a) ((n)-[r]-(m))+ (b)`.empty)
      .|.filterExpressionOrString("r_i.p = true", isRepeatTrailUnique("r_i"))
      .|.expand("(n_i)-[r_i]->(m_i)")
      .|.argument("n_i")
      .allNodeScan("a")
      .build()
    val expand = subPlanBuilder
      .projection("1 AS s")
      .expand("(a)-[r_i*1..]->(b)", relationshipPredicates = Seq(Predicate("r_i", "r_i.p = true")))
      .allNodeScan("a")
      .build()
    rewrites(trail, expand)
  }

  // pre-filter predicate with inner node dependency
  test("Preserves MATCH (a) ((n)-[r]->(m) WHERE n.p = true)+ (b) RETURN 1 AS s") {
    val trail = subPlanBuilder
      .projection("1 AS s")
      .trail(`(a) ((n)-[r]-(m))+ (b)`.empty)
      .|.filterExpression(isRepeatTrailUnique("r_i"))
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
      .trail(`(a) ((n)-[r]-(m))+ (b)`.empty)
      .|.filterExpressionOrString("n_i.p = true", isRepeatTrailUnique("r_i"))
      .|.expand("(n_i)<-[r_i]-(m_i)")
      .|.argument("n_i")
      .allNodeScan("a")
      .build()
    val expand = subPlanBuilder
      .projection("1 AS s")
      .expand(
        "(a)<-[r_i*1..]-(b)",
        projectedDir = INCOMING,
        relationshipPredicates = Seq(Predicate("r_i", "endNode(r_i).p = true"))
      )
      .allNodeScan("a")
      .build()
    rewrites(trail, expand)
  }

  test("Rewrites MATCH (a) ((n)-[r]->(m) WHERE n.p <> m.p)+ (b) RETURN 1 AS s") {
    val trail = subPlanBuilder
      .projection("1 AS s")
      .trail(`(a) ((n)-[r]-(m))+ (b)`.empty)
      .|.filterExpressionOrString("n_i.p <> m_i.p", isRepeatTrailUnique("r_i"))
      .|.expand("(n_i)-[r_i]->(m_i)")
      .|.argument("n_i")
      .allNodeScan("a")
      .build()
    val expand = subPlanBuilder
      .projection("1 AS s")
      .expand(
        "(a)-[r_i*1..]->(b)",
        relationshipPredicates = Seq(Predicate("r_i", "startNode(r_i).p <> endNode(r_i).p"))
      )
      .allNodeScan("a")
      .build()
    rewrites(trail, expand)
  }

  test("Preserves bidirectional MATCH (a) ((n)-[r]-(m) WHERE n.p <> m.p)+ (b) RETURN 1 AS s") {
    val trail = subPlanBuilder
      .projection("1 AS s")
      .trail(`(a) ((n)-[r]-(m))+ (b)`.empty)
      .|.filterExpressionOrString("n_i.p <> m_i.p", isRepeatTrailUnique("r_i"))
      .|.expand("(n_i)-[r_i]-(m_i)")
      .|.argument("n_i")
      .allNodeScan("a")
      .build()
    preserves(trail)
  }

  // pre-filter predicate with dependency on variable from previous clause
  test("Rewrites MATCH (z) MATCH (a) ((n)-[r]->(m) WHERE r.p = z.p)+ (b) RETURN 1 AS s") {
    val trail = subPlanBuilder
      .projection("1 AS s")
      .apply()
      .|.trail(`(a) ((n)-[r]-(m))+ (b)`.empty)
      .|.|.filterExpressionOrString("r_i.p = cacheN[z.p]", isRepeatTrailUnique("r_i"))
      .|.|.expandAll("(n_i)-[r_i]->(m_i)")
      .|.|.argument("n_i", "z_i")
      .|.allNodeScan("a", "z")
      .cacheProperties("cacheNFromStore[z.p]")
      .allNodeScan("z")
      .build()
    val expand = subPlanBuilder
      .projection("1 AS s")
      .apply()
      .|.expand("(a)-[r_i*1..]->(b)", relationshipPredicates = Seq(Predicate("r_i", "r_i.p = cacheN[z.p]")))
      .|.allNodeScan("a", "z")
      .cacheProperties("cacheNFromStore[z.p]")
      .allNodeScan("z")
      .build()
    rewrites(trail, expand)
  }

  // pre-filter relationship type predicate
  test("Rewrites MATCH (a) ((n)-[r:T]->(m))+ (b) RETURN 1 AS s") {
    val trail = subPlanBuilder
      .trail(`(a) ((n)-[r]-(m))+ (b)`.empty)
      .|.filterExpression(isRepeatTrailUnique("r_i"))
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
      .trail(`(a) ((n)-[r]-(m))+ (b)`.empty)
      .|.filterExpressionOrString("r_i.p = 0", isRepeatTrailUnique("r_i"))
      .|.expand("(n_i)-[r_i]->(m_i)")
      .|.argument("n_i")
      .allNodeScan("a")
      .build()
    val expand = subPlanBuilder
      .expand("(a)-[r_i*1..]->(b)", relationshipPredicates = Seq(Predicate("r_i", "r_i.p = 0")))
      .allNodeScan("a")
      .build()
    rewrites(trail, expand)
  }

  // post-filter predicate
  test("Rewrites MATCH (a) ((n)-[r]->(m))+ (b) WHERE all(x IN r WHERE x:T) RETURN 1 AS s") {
    val trail = subPlanBuilder
      .projection("1 AS s")
      .filter("all(x IN r WHERE x:T)")
      .trail(`(a) ((n)-[r]-(m))+ (b)`.nmless)
      .|.filterExpression(isRepeatTrailUnique("r_i"))
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
      .trail(`(a) ((n)-[r]-(m))+ (b)`.empty.withQuantifier(0, Unlimited))
      .|.filterExpression(isRepeatTrailUnique("r_i"))
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
      .trail(`(a) ((n)-[r]-(m))+ (b)`.empty.withQuantifier(0, Limited(2)))
      .|.filterExpression(isRepeatTrailUnique("r_i"))
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
      .trail(`(a) ((n)-[r]-(m))+ (b)`.empty.withQuantifier(2, Unlimited))
      .|.filterExpression(isRepeatTrailUnique("r_i"))
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
      .trail(`(a) ((n)-[r]-(m))+ (b)`.empty.withQuantifier(2, Limited(2)))
      .|.filterExpression(isRepeatTrailUnique("r_i"))
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
      .trail(`(a) ((n)-[r]-(m))+ (b)`.empty.withQuantifier(2, Limited(5)))
      .|.filterExpression(isRepeatTrailUnique("r_i"))
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
      .trail(`(a) ((n)-[r]-(m))+ (b)`.empty.withQuantifier(0, Limited(3000000000L)))
      .|.filterExpression(isRepeatTrailUnique("r_i"))
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
      .trail(`(a) ((n)-[r]-(m))+ (b)`.empty.reverse)
      .|.filterExpression(isRepeatTrailUnique("r_i"))
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
      .trail(`(a) ((n)-[r]-(m))+ (b)`.empty)
      .|.filterExpression(isRepeatTrailUnique("r_i"))
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
      .trail(`(a) ((n)-[r]-(m))+ (b)`.empty.reverse)
      .|.filterExpression(isRepeatTrailUnique("r_i"))
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
      .trail(`(a) ((n)-[r]-(m))+ (b)`.empty)
      .|.filterExpression(isRepeatTrailUnique("r_i"))
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
      .trail(`(a) ((n)-[r]-(m))+ (b)`.empty.reverse)
      .|.filterExpression(isRepeatTrailUnique("r_i"))
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
      .trail(`(a) ((n)-[r]-(m))+ (b)`.empty.reverse.withPreviouslyBoundRel("rr"))
      .|.filterExpression(isRepeatTrailUnique("r_i"))
      .|.expand("(m_i)<-[r_i]-(n_i)")
      .|.argument("m_i")
      .allRelationshipsScan("(b)-[rr]-(c)")
      .build()

    val expand = subPlanBuilder
      .projection("1 AS s")
      .filterExpression(noneOfRels(v"rr", v"r_i"))
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
      .trail(`(a) ((n)-[r]-(m))+ (b)`.nmless)
      .|.filterExpression(isRepeatTrailUnique("r_i"))
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
      .trail(`(a) ((n)-[r]-(m))+ (b)`.empty.reverse)
      .|.filterExpression(isRepeatTrailUnique("r_i"))
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
      .trail(`(a) ((n)-[r]-(m))+ (b)`.empty.reverse.withPreviouslyBoundRelGroup("rr"))
      .|.filterExpression(isRepeatTrailUnique("r_i"))
      .|.expand("(m_i)<-[r_i]-(n_i)")
      .|.argument("m_i")
      .trail(`(b) ((x)-[rr]-(y))+ (c)`.xyless)
      .|.filterExpression(isRepeatTrailUnique("rr"))
      .|.expand("(x_i)-[rr_i]->(y_i)")
      .|.argument("x_i")
      .allNodeScan("b")
      .build()

    val expand = subPlanBuilder
      .projection("1 AS s")
      .filterExpression(disjoint(v"r_i", v"rr"))
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
      .trail(`(b) ((x)-[rr]-(y))+ (c)`.empty.withPreviouslyBoundRelGroup("r"))
      .|.filterExpression(isRepeatTrailUnique("rr"))
      .|.expand("(x_i)-[rr_i]->(y_i)")
      .|.filter("x_i.p = 0")
      .|.argument("x_i")
      .filter("b.p = 0")
      .trail(`(a) ((n)-[r]-(m))+ (b)`.nmless)
      .|.filterExpression(isRepeatTrailUnique("r_i"))
      .|.expand("(n_i)-[r_i]->(m_i)")
      .|.argument("n_i")
      .allNodeScan("a")
      .build()

    val expand = subPlanBuilder
      .projection("1 AS s")
      .trail(`(b) ((x)-[rr]-(y))+ (c)`.empty.withPreviouslyBoundRelGroup("r"))
      .|.filterExpression(isRepeatTrailUnique("rr"))
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
        reverseGroupVariableProjections = false
      )
    }

    val trail = subPlanBuilder
      .projection("1 AS s")
      .trail(`(a) ((n)-[r]-(m))+ (b)`.empty.reverse.withPreviouslyBoundRelGroup("rr", "rrr"))
      .|.filterExpression(isRepeatTrailUnique("r_i"))
      .|.expand("(m_i)<-[r_i]-(n_i)")
      .|.argument("m_i")
      .trail(`(b) ((x)-[rr]-(y)-[rrr]-(z))+ (c)`.xyzless)
      .|.filterExpression(differentRelationships("rrr_i", "rr_i"), isRepeatTrailUnique("rrr_i"))
      .|.expand("(y_i)-[rrr_i]->(z_i)")
      .|.filterExpression(isRepeatTrailUnique("rr_i"))
      .|.expand("(x_i)-[rr_i]->(y_i)")
      .|.argument("x_i")
      .allNodeScan("b")
      .build()

    val expand = subPlanBuilder
      .projection("1 AS s")
      .filterExpression(
        disjoint(v"r_i", v"rr"),
        disjoint(v"r_i", v"rrr")
      )
      .expand("(b)<-[r_i*1..]-(a)")
      .trail(`(b) ((x)-[rr]-(y)-[rrr]-(z))+ (c)`.xyzless)
      .|.filterExpression(differentRelationships("rrr_i", "rr_i"), isRepeatTrailUnique("rrr_i"))
      .|.expand("(y_i)-[rrr_i]->(z_i)")
      .|.filterExpression(isRepeatTrailUnique("rr_i"))
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
      .trail(`(a) ((n)-[r]-(m))+ (b)`.empty.reverse)
      .|.filterExpression(isRepeatTrailUnique("r_i"))
      .|.expand("(m_i)<-[r_i:R]-(n_i)")
      .|.argument("m_i")
      .trail(`(b) ((x)-[rr]-(y))+ (c)`.empty)
      .|.filterExpression(isRepeatTrailUnique("rr_i"))
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
      .trail(`(a) ((n)-[r]-(m))+ (b)`.empty.reverse)
      .|.filterExpressionOrString("n_i.prop <> m_i.prop", isRepeatTrailUnique("r_i"))
      .|.expand("(m_i)<-[r_i:R]-(n_i)")
      .|.argument("m_i")
      .trail(`(b) ((x)-[rr]-(y))+ (c)`.empty)
      .|.filterExpressionOrString("y_i.name = 'foo'", isRepeatTrailUnique("rr_i"))
      .|.expand("(x_i)-[rr_i:RR]->(y_i)")
      .|.argument("x_i")
      .allNodeScan("b")
      .build()

    val expand = subPlanBuilder
      .projection("1 AS s")
      .expand(
        "(b)<-[r_i:R*1..]-(a)",
        relationshipPredicates = Seq(Predicate("r_i", "startNode(r_i).prop <> endNode(r_i).prop"))
      )
      .expand("(b)-[rr_i:RR*1..]->(c)", relationshipPredicates = Seq(Predicate("rr_i", "endNode(rr_i).name = 'foo'")))
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
        reverseGroupVariableProjections = false
      )
    }
    val trail = subPlanBuilder
      .projection("1 AS s")
      .trail(`(c) ((x)-[rrr]-(y))+ (d)`.empty)
      .|.filterExpression(isRepeatTrailUnique("rrr_i"))
      .|.expand("(x_i)-[rrr_i]->(y_i)")
      .|.argument("x_i")
      .filterExpression(noneOfRels(v"rr", v"r"))
      .expand("(b)-[rr]->(c)")
      .trail(`(a) ((n)-[r]-(m))+ (b)`.nmless)
      .|.filterExpression(isRepeatTrailUnique("r_i"))
      .|.expand("(n_i)-[r_i]->(m_i)")
      .|.argument("n_i")
      .allNodeScan("a")
      .build()
    val expand = subPlanBuilder
      .projection("1 AS s")
      .filterExpression(disjoint(v"rrr_i", v"r"))
      .filterExpression(noneOfRels(v"rr", v"rrr_i"))
      .expand("(c)-[rrr_i*1..]->(d)")
      .filterExpression(noneOfRels(v"rr", v"r"))
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
      .trail(`(a) ((n)-[r]-(m))+ (b)`.empty)
      .|.filterExpression(isRepeatTrailUnique("r_i"))
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
      .trail(`(a) ((n)-[r]-(m))+ (b)`.mless)
      .|.filterExpression(isRepeatTrailUnique("r_i"))
      .|.expand("(n_i)-[r_i]->(m_i)")
      .|.argument("n_i")
      .allNodeScan("a")
      .build()
    preserves(trail)
  }

  val `(a) ((x_i)-[r_i]-(y_i))+ (  UNNAMED4)` : TrailParameters = TrailParameters(
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
    reverseGroupVariableProjections = false
  )

  test("Rewrite selection and trail to VarLengthExpand(Into)") {
    val selectionAndTrail = subPlanBuilder
      .projection("1 AS s")
      .filter("`  UNNAMED4` = b")
      .trail(`(a) ((x_i)-[r_i]-(y_i))+ (  UNNAMED4)`)
      .|.filterExpression(isRepeatTrailUnique("r_i"))
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
      .filter("`  UNNAMED4` = b")
      .trail(`(a) ((x_i)-[r_i]-(y_i))+ (  UNNAMED4)`)
      .|.filterExpression(isRepeatTrailUnique("r_i"))
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
      .trail(`(a) ((x_i)-[r_i]-(y_i))+ (  UNNAMED4)`)
      .|.filterExpression(isRepeatTrailUnique("r_i"))
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
      .trail(`(a) ((x_i)-[r_i]-(y_i))+ (  UNNAMED4)`)
      .|.filterExpression(isRepeatTrailUnique("r_i"))
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
      .trail(`(a) ((x_i)-[r_i]-(y_i))+ (  UNNAMED4)`)
      .|.filterExpression(isRepeatTrailUnique("r_i"))
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
      .trail(`(a) ((x_i)-[r_i]-(y_i))+ (  UNNAMED4)`)
      .|.filterExpression(isRepeatTrailUnique("r_i"))
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
      .trail(`(a) ((n)-[r]-(m))+ (b)`.empty)
      .|.filterExpression(isRepeatTrailUnique("r_i"))
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

  test("De not rewrite selection and trail to VarLengthExpand(Into) when the selection has more than 1 predicate") {
    val selectionAndTrail = subPlanBuilder
      .projection("1 AS s")
      .filter("`  UNNAMED4` = b", "not a = b")
      .trail(`(a) ((x_i)-[r_i]-(y_i))+ (  UNNAMED4)`)
      .|.filterExpression(isRepeatTrailUnique("r_i"))
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
}

object TrailToVarExpandRewriterTest
    extends CypherFunSuite
    with LogicalPlanTestOps
    with LogicalPlanConstructionTestSupport
    with AstConstructionTestSupport {

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

  private def rewrites(trail: LogicalPlan, expand: LogicalPlan): Assertion =
    rewrite(trail).stripProduceResults should equal(expand)

  private def preserves(trail: LogicalPlan): Assertion =
    rewrite(trail).stripProduceResults should equal(trail.stripProduceResults)

  object `(a) ((n)-[r]-(m))+ (b)` {

    val full: TrailParameters = TrailParameters(
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
      reverseGroupVariableProjections = false
    )

    val nless: TrailParameters = full.copy(groupNodes = Set(("m_i", "m")))

    val rless: TrailParameters = full.copy(groupRelationships = Set.empty)

    val mless: TrailParameters = full.copy(groupNodes = Set(("n_i", "n")))

    val nmless: TrailParameters = full.copy(groupNodes = Set.empty)

    val rnless: TrailParameters = full.copy(groupNodes = Set(("m_i", "m")), groupRelationships = Set.empty)

    val rmless: TrailParameters = full.copy(groupNodes = Set(("n_i", "n")), groupRelationships = Set.empty)

    val empty: TrailParameters = full.copy(groupNodes = Set.empty, groupRelationships = Set.empty)
  }

  object `(b) ((x)-[rr]-(y))+ (c)` {

    val empty: TrailParameters = TrailParameters(
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
      reverseGroupVariableProjections = false
    )

    val xyless: TrailParameters = empty.copy(groupRelationships = Set(("rr_i", "rr")))
  }

  private def rewrite(p: LogicalPlan): LogicalPlan =
    p.endoRewrite(TrailToVarExpandRewriter(new StubLabelAndRelTypeInfos, Attributes(idGen)))

  private def subPlanBuilder = new LogicalPlanBuilder(wholePlan = false)
}
