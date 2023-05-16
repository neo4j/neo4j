/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter

import TrailToVarExpandRewriterTest.`(a) ((n)-[r]-(m))+ (b)`
import TrailToVarExpandRewriterTest.`(b) ((x)-[rr]-(y))+ (c)`
import TrailToVarExpandRewriterTest.`reversed (a) ((n)-[r]-(m))+ (b)`
import TrailToVarExpandRewriterTest.preserves
import TrailToVarExpandRewriterTest.rewrite
import TrailToVarExpandRewriterTest.rewrites
import TrailToVarExpandRewriterTest.subPlanBuilder
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanBuilder
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanConstructionTestSupport
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanTestOps
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.expressions.SemanticDirection.INCOMING
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.TrailParameters
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.util.UpperBound.Limited
import org.neo4j.cypher.internal.util.UpperBound.Unlimited
import org.neo4j.cypher.internal.util.attribution.Attributes
import org.neo4j.cypher.internal.util.helpers.fixedPoint
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.scalatest.Assertion

// Additional tests can be found in QuantifiedPathPatternPlanningIntegrationTest
class TrailToVarExpandRewriterTest extends CypherFunSuite with LogicalPlanningTestSupport {

  // happy case
  test("Rewrites MATCH (a) ((n)-[r]->(m))+ (b) RETURN 1 AS s") {
    val trail = subPlanBuilder
      .projection(project = Seq("1 AS s"), discard = Set("a", "b", "r"))
      .trail(`(a) ((n)-[r]-(m))+ (b)`)
      .|.filterExpression(isRepeatTrailUnique("r"))
      .|.expand("(n)-[r]->(m)")
      .|.argument("n")
      .allNodeScan("a")
      .build()

    val expand = subPlanBuilder
      .projection(project = Seq("1 AS s"), discard = Set("a", "r", "b"))
      .expand("(a)-[r*1..]->(b)")
      .allNodeScan("a")
      .build()

    rewrites(trail, expand)
  }

  // relationship group variable r is used
  test("Rewrites MATCH (a) ((n)-[r]->(m))+ (b) RETURN r AS r") {
    val trail = new LogicalPlanBuilder()
      .produceResults("r")
      .trail(`(a) ((n)-[r]-(m))+ (b)`)
      .|.filterExpression(isRepeatTrailUnique("r"))
      .|.expand("(n)-[r]->(m)")
      .|.argument("n")
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
    val `(a) ((n)-[r]-(m))+ (b) with groupNode("n")` = `(a) ((n)-[r]-(m))+ (b)`.copy(
      groupNodes = Set(("n", "n"))
    )
    val trail = subPlanBuilder
      .trail(`(a) ((n)-[r]-(m))+ (b) with groupNode("n")`)
      .|.filterExpression(isRepeatTrailUnique("r"))
      .|.expand("(n)-[r]->(m)")
      .|.argument("n")
      .allNodeScan("a")
      .build()
    preserves(trail)
  }

  // node variables n and m are used
  test("Preserves MATCH (a) ((n)-[r]->(m))+ (b) RETURN *") {
    val `(a) ((n)-[r]-(m))+ (b) with groupNodes("n", "m")` = `(a) ((n)-[r]-(m))+ (b)`.copy(
      groupNodes = Set(("n", "n"), ("m", "m"))
    )
    val trail = subPlanBuilder
      .trail(`(a) ((n)-[r]-(m))+ (b) with groupNodes("n", "m")`)
      .|.filterExpression(isRepeatTrailUnique("r"))
      .|.expand("(n)-[r]->(m)")
      .|.argument("n")
      .allNodeScan("a")
      .build()
    preserves(trail)
  }

  // node variable n has a predicate
  test("Preserves MATCH (a) ((n:N)-[r]->(m))+ (b) RETURN 1 AS s") {
    val trail = subPlanBuilder
      .projection(project = Seq("1 AS s"), discard = Set("a", "b", "r"))
      .trail(`(a) ((n)-[r]-(m))+ (b)`)
      .|.filterExpression(isRepeatTrailUnique("r"))
      .|.expand("(n)-[r]->(m)")
      .|.filter("n:N")
      .|.argument("n")
      .nodeByLabelScan("a", "N")
      .build()
    preserves(trail)
  }

  // the qpp relationship chain contains multiple relationships
  test("Preserves MATCH (a) ((n)-[r]->(m)->[rr]->(o))+ (b) RETURN 1 AS s") {
    val `(a) ((n)-[r]->(m)->[rr]->(o))+ (b)` = TrailParameters(
      min = 1,
      max = Unlimited,
      start = "a",
      end = "b",
      innerStart = "n",
      innerEnd = "o",
      groupNodes = Set.empty,
      groupRelationships = Set(("r", "r"), ("rr", "rr")),
      innerRelationships = Set("r", "rr"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = false
    )
    val trail = subPlanBuilder
      .projection(project = Seq("1 AS s"), discard = Set("a", "b", "rr", "r"))
      .trail(`(a) ((n)-[r]->(m)->[rr]->(o))+ (b)`)
      .|.filterExpression(isRepeatTrailUnique("rr"), differentRelationships("rr", "r"))
      .|.expand("(m)-[rr]->(o)")
      .|.filterExpression(isRepeatTrailUnique("r"))
      .|.expand("(n)-[r]->(m)")
      .|.argument("n")
      .allNodeScan("a")
      .build()
    preserves(trail)
  }

  // although the relationship has a predicate, it is a predicate which is also expressible with VarExpand
  test("Rewrites MATCH (a) ((n)-[r:R]->(m))+ (b) RETURN 1 AS s") {
    val trail = subPlanBuilder
      .projection(project = Seq("1 AS s"), discard = Set("a", "b", "r"))
      .trail(`(a) ((n)-[r]-(m))+ (b)`)
      .|.filterExpression(isRepeatTrailUnique("r"))
      .|.expand("(n)-[r:R]->(m)")
      .|.argument("n")
      .allNodeScan("a")
      .build()
    val expand = subPlanBuilder
      .projection(project = Seq("1 AS s"), discard = Set("a", "b", "r"))
      .expand("(a)-[r:R*1..]->(b)")
      .allNodeScan("a")
      .build()
    rewrites(trail, expand)
  }

  // TODO: should be possible, follow-up with this in next PR
  // relationship variable r has a predicate
  test("Preserves MATCH (a) ((n)-[r {p: 1}]->(m))+ (b) RETURN 1 AS s") {
    val trail = subPlanBuilder
      .projection(project = Seq("1 AS s"), discard = Set("a", "b", "r"))
      .trail(`(a) ((n)-[r]-(m))+ (b)`)
      .|.filterExpression(isRepeatTrailUnique("r"))
      .|.filter("r.p = 1")
      .|.expandAll("(n)-[r]->(m)")
      .|.argument("n")
      .allNodeScan("a")
      .build()
    preserves(trail)
  }

  // relationship type predicate in post-filter position
  test("Rewrites MATCH (a) ((n)-[r]->(m))+ (b) WHERE all(x IN r WHERE x:T) RETURN 1 AS s") {
    val trail = subPlanBuilder
      .projection(project = Seq("1 AS s"), discard = Set("a", "b", "r"))
      .filter("all(x IN r WHERE x:T)")
      .trail(`(a) ((n)-[r]-(m))+ (b)`)
      .|.filterExpression(isRepeatTrailUnique("r"))
      .|.expand("(n)-[r]->(m)")
      .|.argument("n")
      .allNodeScan("a")
      .build()
    val expand = subPlanBuilder
      .projection(project = Seq("1 AS s"), discard = Set("a", "b", "r"))
      .filter("all(x IN r WHERE x:T)")
      .expand("(a)-[r*1..]->(b)")
      .allNodeScan("a")
      .build()

    rewrites(trail, expand)
  }

  // quantifier with kleene star
  test("Rewrites MATCH (a) ((n)-[r]->(m))* (b) RETURN 1 AS s") {
    val `(a) ((n)-[r]-(m))* (b)` = `(a) ((n)-[r]-(m))+ (b)`.copy(min = 0)
    val trail = subPlanBuilder
      .projection(project = Seq("1 AS s"), discard = Set("a", "b", "r"))
      .trail(`(a) ((n)-[r]-(m))* (b)`)
      .|.filterExpression(isRepeatTrailUnique("r"))
      .|.expand("(n)-[r]->(m)")
      .|.argument("n")
      .allNodeScan("a")
      .build()

    val expand = subPlanBuilder
      .projection(project = Seq("1 AS s"), discard = Set("a", "r", "b"))
      .expand("(a)-[r*0..]->(b)")
      .allNodeScan("a")
      .build()

    rewrites(trail, expand)
  }

  // quantifier with limited ub
  test("Rewrites MATCH (a) ((n)-[r]->(m)){,2} (b) RETURN 1 AS s") {
    val `(a) ((n)-[r]-(m)){0,2} (b)` = `(a) ((n)-[r]-(m))+ (b)`.copy(min = 0, max = Limited(2))
    val trail = subPlanBuilder
      .projection(project = Seq("1 AS s"), discard = Set("a", "b", "r"))
      .trail(`(a) ((n)-[r]-(m)){0,2} (b)`)
      .|.filterExpression(isRepeatTrailUnique("r"))
      .|.expand("(n)-[r]->(m)")
      .|.argument("n")
      .allNodeScan("a")
      .build()

    val expand = subPlanBuilder
      .projection(project = Seq("1 AS s"), discard = Set("a", "r", "b"))
      .expand("(a)-[r*0..2]->(b)")
      .allNodeScan("a")
      .build()

    rewrites(trail, expand)
  }

  // quantifier with unlimited ub
  test("Rewrites MATCH (a) ((n)-[r]->(m)){2,} (b) RETURN 1 AS s") {
    val `(a) ((n)-[r]-(m)){2,} (b)` = `(a) ((n)-[r]-(m))+ (b)`.copy(min = 2, max = Unlimited)
    val trail = subPlanBuilder
      .projection(project = Seq("1 AS s"), discard = Set("a", "b", "r"))
      .trail(`(a) ((n)-[r]-(m)){2,} (b)`)
      .|.filterExpression(isRepeatTrailUnique("r"))
      .|.expand("(n)-[r]->(m)")
      .|.argument("n")
      .allNodeScan("a")
      .build()

    val expand = subPlanBuilder
      .projection(project = Seq("1 AS s"), discard = Set("a", "r", "b"))
      .expand("(a)-[r*2..]->(b)")
      .allNodeScan("a")
      .build()

    rewrites(trail, expand)
  }

  // quantifier with equal lb and ub
  test("Rewrites MATCH (a) ((n)-[r]->(m)){2,2} (b) RETURN 1 AS s") {
    val `(a) ((n)-[r]-(m)){2,2} (b)` = `(a) ((n)-[r]-(m))+ (b)`.copy(min = 2, max = Limited(2))
    val trail = subPlanBuilder
      .projection(project = Seq("1 AS s"), discard = Set("a", "b", "r"))
      .trail(`(a) ((n)-[r]-(m)){2,2} (b)`)
      .|.filterExpression(isRepeatTrailUnique("r"))
      .|.expand("(n)-[r]->(m)")
      .|.argument("n")
      .allNodeScan("a")
      .build()

    val expand = subPlanBuilder
      .projection(project = Seq("1 AS s"), discard = Set("a", "r", "b"))
      .expand("(a)-[r*2..2]->(b)")
      .allNodeScan("a")
      .build()

    rewrites(trail, expand)
  }

  // quantifier with different lb and ub
  test("Rewrites MATCH (a) ((n)-[r]->(m)){2,5} (b) RETURN 1 AS s") {
    val `(a) ((n)-[r]-(m)){2,5} (b)` = `(a) ((n)-[r]-(m))+ (b)`.copy(min = 2, max = Limited(5))
    val trail = subPlanBuilder
      .projection(project = Seq("1 AS s"), discard = Set("a", "b", "r"))
      .trail(`(a) ((n)-[r]-(m)){2,5} (b)`)
      .|.filterExpression(isRepeatTrailUnique("r"))
      .|.expand("(n)-[r]->(m)")
      .|.argument("n")
      .allNodeScan("a")
      .build()

    val expand = subPlanBuilder
      .projection(project = Seq("1 AS s"), discard = Set("a", "r", "b"))
      .expand("(a)-[r*2..5]->(b)")
      .allNodeScan("a")
      .build()

    rewrites(trail, expand)
  }

  // cannot convert quantifier from long to int
  test("Preserves MATCH (a) ((n)-[r]->(m){,3000000000} (b) RETURN p") {
    val `(a) ((n)-[r]->(m){,3000000000} (b)` = `(a) ((n)-[r]-(m))+ (b)`.copy(
      min = 0,
      max = Limited(3000000000L)
    )
    val trail = subPlanBuilder
      .projection(project = Seq("1 AS s"), discard = Set("a", "b", "r"))
      .trail(`(a) ((n)-[r]->(m){,3000000000} (b)`)
      .|.filterExpression(isRepeatTrailUnique("r"))
      .|.expand("(n)-[r]->(m)")
      .|.argument("n")
      .allNodeScan("a")
      .build()
    preserves(trail)
  }

  // dir=outgoing, reverseGroupVariableProjections
  test("Rewrites MATCH (a) ((n)-[r]->(m))+ (b) RETURN 1 AS s, reverseGroupVariableProjections=true") {
    val trail = subPlanBuilder
      .projection(project = Seq("1 AS s"), discard = Set("b", "a", "r"))
      .trail(`reversed (a) ((n)-[r]-(m))+ (b)`)
      .|.filterExpression(isRepeatTrailUnique("r"))
      .|.expand("(m)<-[r]-(n)")
      .|.argument("m")
      .allNodeScan("b")
      .build()

    val expand = subPlanBuilder
      .projection(project = Seq("1 AS s"), discard = Set("b", "a", "r"))
      .expand("(b)<-[r*1..]-(a)")
      .allNodeScan("b")
      .build()

    rewrites(trail, expand)
  }

  // dir=incoming
  test("Rewrites MATCH (a) ((n)<-[r]-(m))+ (b) RETURN 1 AS s") {
    val trail = subPlanBuilder
      .projection(project = Seq("1 AS s"), discard = Set("a", "b", "r"))
      .trail(`(a) ((n)-[r]-(m))+ (b)`)
      .|.filterExpression(isRepeatTrailUnique("r"))
      .|.expand("(n)<-[r]-(m)")
      .|.argument("n")
      .allNodeScan("a")
      .build()

    val expand = subPlanBuilder
      .projection(project = Seq("1 AS s"), discard = Set("a", "r", "b"))
      .expand("(a)<-[r*1..]-(b)", projectedDir = INCOMING)
      .allNodeScan("a")
      .build()

    rewrites(trail, expand)
  }

  // dir=incoming, reverseGroupVariableProjections
  test("Rewrites MATCH (a) ((n)<-[r]-(m))+ (b) RETURN 1 AS s, reverseGroupVariableProjections=true") {
    val trail = subPlanBuilder
      .projection(project = Seq("1 AS s"), discard = Set("b", "a", "r"))
      .trail(`reversed (a) ((n)-[r]-(m))+ (b)`)
      .|.filterExpression(isRepeatTrailUnique("r"))
      .|.expand("(m)-[r]->(n)")
      .|.argument("m")
      .allNodeScan("b")
      .build()

    val expand = subPlanBuilder
      .projection(project = Seq("1 AS s"), discard = Set("b", "a", "r"))
      .expand("(b)-[r*1..]->(a)", projectedDir = INCOMING)
      .allNodeScan("b")
      .build()

    rewrites(trail, expand)
  }

  // dir=both
  test("Rewrites MATCH (a) ((n)-[r]-(m))+ (b) RETURN 1 AS s") {
    val trail = subPlanBuilder
      .projection(project = Seq("1 AS s"), discard = Set("a", "r", "b"))
      .trail(`(a) ((n)-[r]-(m))+ (b)`)
      .|.filterExpression(isRepeatTrailUnique("r"))
      .|.expand("(n)-[r]-(m)")
      .|.argument("n")
      .allNodeScan("a")
      .build()

    val expand = subPlanBuilder
      .projection(project = Seq("1 AS s"), discard = Set("a", "r", "b"))
      .expand("(a)-[r*1..]-(b)")
      .allNodeScan("a")
      .build()

    rewrites(trail, expand)
  }

  // dir=both, reverseGroupVariableProjections
  test("Rewrites MATCH (a) ((n)-[r]-(m))+ (b) RETURN 1 AS s, reverseGroupVariableProjections=true") {
    val trail = subPlanBuilder
      .projection(project = Seq("1 AS s"), discard = Set("a", "r", "b"))
      .trail(`reversed (a) ((n)-[r]-(m))+ (b)`)
      .|.filterExpression(isRepeatTrailUnique("r"))
      .|.expand("(m)-[r]-(n)")
      .|.argument("m")
      .allNodeScan("b")
      .build()

    val expand = subPlanBuilder
      .projection(project = Seq("1 AS s"), discard = Set("a", "r", "b"))
      .expand("(b)-[r*1..]-(a)", projectedDir = INCOMING)
      .allNodeScan("b")
      .build()

    rewrites(trail, expand)
  }

  // qpp + relationship pattern, inserts relationship uniqueness predicate because Trail has previously bound
  // relationships
  test("Rewrites MATCH (a) ((n)-[r]->(m))+ (b)-[rr]-(c) RETURN 1 AS s") {
    val `reversed (a) ((n)-[r]-(m))+ (b) with previouslyBoundRel` = `reversed (a) ((n)-[r]-(m))+ (b)`.copy(
      previouslyBoundRelationships = Set("rr")
    )
    val trail = subPlanBuilder
      .projection(project = Seq("1 AS s"), discard = Set("rr", "r", "b", "a", "c"))
      .trail(`reversed (a) ((n)-[r]-(m))+ (b) with previouslyBoundRel`)
      .|.filterExpression(isRepeatTrailUnique("r"))
      .|.expand("(m)<-[r]-(n)")
      .|.argument("m")
      .allRelationshipsScan("(b)-[rr]-(c)")
      .build()

    val expand = subPlanBuilder
      .projection(project = Seq("1 AS s"), discard = Set("c", "rr", "r", "b", "a"))
      .filter("not rr IN r")
      .expand("(b)<-[r*1..]-(a)")
      .allRelationshipsScan("(b)-[rr]-(c)")
      .build()

    rewrites(trail, expand)
  }

  // qpp + relationship pattern, does not insert relationship uniqueness predicate because Trail has no previously bound
  // relationships
  test("Rewrites MATCH (b)-[rr]-(a) ((n)-[r]->(m))+ RETURN 1 AS s") {
    val trail = subPlanBuilder
      .projection(project = Seq("1 AS s"), discard = Set("rr", "r", "b", "a", "c"))
      .filter("not rr IN r")
      .expand("(b)-[rr]-(c)")
      .trail(`(a) ((n)-[r]-(m))+ (b)`)
      .|.filterExpression(isRepeatTrailUnique("r"))
      .|.expand("(n)-[r]->(m)")
      .|.argument("n")
      .allNodeScan("a")
      .build()

    val expand = subPlanBuilder
      .projection(project = Seq("1 AS s"), discard = Set("rr", "r", "b", "a", "c"))
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
      .projection(project = Seq("1 AS s"), discard = Set("rr", "r", "b", "a", "c"))
      .trail(`reversed (a) ((n)-[r]-(m))+ (b)`)
      .|.filterExpression(isRepeatTrailUnique("r"))
      .|.expand("(m)<-[r:R]-(n)")
      .|.argument("m")
      .allRelationshipsScan("(b)-[rr:RR]-(c)")
      .build()

    val expand = subPlanBuilder
      .projection(project = Seq("1 AS s"), discard = Set("rr", "r", "b", "a", "c"))
      .expand("(b)<-[r:R*1..]-(a)")
      .allRelationshipsScan("(b)-[rr:RR]-(c)")
      .build()

    rewrites(trail, expand)
  }

  // two rewritable qpps. inserts relationship uniqueness predicate after the Trail which has previously bound
  // relationship group variables
  test("Rewrites MATCH (a) ((n)-[r]->(m))+ (b) ((x)-[rr]->(y))+ (c) RETURN 1 AS s") {
    val `reversed (a) ((n)-[r]-(m))+ (b) with previouslyBoundGroupRel` = `reversed (a) ((n)-[r]-(m))+ (b)`.copy(
      previouslyBoundRelationshipGroups = Set("rr")
    )
    val trail = subPlanBuilder
      .projection(project = Seq("1 AS s"), discard = Set("c", "rr", "r", "b", "a"))
      .trail(`reversed (a) ((n)-[r]-(m))+ (b) with previouslyBoundGroupRel`)
      .|.filterExpression(isRepeatTrailUnique("r"))
      .|.expand("(m)<-[r]-(n)")
      .|.argument("m")
      .trail(`(b) ((x)-[rr]-(y))+ (c)`)
      .|.filterExpression(isRepeatTrailUnique("rr"))
      .|.expand("(x)-[rr]->(y)")
      .|.argument("x")
      .allNodeScan("b")
      .build()

    val expand = subPlanBuilder
      .projection(project = Seq("1 AS s"), discard = Set("c", "rr", "r", "b", "a"))
      .filterExpression(disjoint(varFor("r"), varFor("rr")))
      .expand("(b)<-[r*1..]-(a)")
      .expand("(b)-[rr*1..]->(c)")
      .allNodeScan("b")
      .build()

    rewrites(trail, expand)
  }

  // two qpps, only the earliest tail is rewritable. do not insert relationship uniqueness predicate because the
  // earliest trail has no previously bound relationship group variables (latest Trail will take care of filtering out)
  test("Rewrites MATCH (a) ((n)-[r]->(m))+ (b) ((x {p: 0})-[rr]->(y))+ (c) RETURN 1 AS s") {
    val `(b) ((x)-[rr]-(y))+ (c) with previouslyBoundGroupRel("r")` = `(b) ((x)-[rr]-(y))+ (c)`.copy(
      previouslyBoundRelationshipGroups = Set("r")
    )

    val trail = subPlanBuilder
      .projection(project = Seq("1 AS s"), discard = Set("c", "rr", "r", "b", "a"))
      .trail(`(b) ((x)-[rr]-(y))+ (c) with previouslyBoundGroupRel("r")`)
      .|.filterExpression(isRepeatTrailUnique("rr"))
      .|.expand("(x)-[rr]->(y)")
      .|.filter("x.p = 0")
      .|.argument("x")
      .filter("b.p = 0")
      .trail(`(a) ((n)-[r]-(m))+ (b)`)
      .|.filterExpression(isRepeatTrailUnique("r"))
      .|.expand("(n)-[r]->(m)")
      .|.argument("n")
      .allNodeScan("a")
      .build()

    val expand = subPlanBuilder
      .projection(project = Seq("1 AS s"), discard = Set("c", "rr", "r", "b", "a"))
      .trail(`(b) ((x)-[rr]-(y))+ (c) with previouslyBoundGroupRel("r")`)
      .|.filterExpression(isRepeatTrailUnique("rr"))
      .|.expand("(x)-[rr]->(y)")
      .|.filter("x.p = 0")
      .|.argument("x")
      .filter("b.p = 0")
      .expand("(a)-[r*1..]->(b)")
      .allNodeScan("a")
      .build()

    rewrites(trail, expand)
  }

  // two qpps, only the latest tail is rewritable. we insert relationship uniqueness predicate because the trail
  // has previously bound relationship group variables
  test("Rewrites MATCH (a) ((n)-[r]->(m))+ (b) ((x)-[rr]->(y)-[rrr]->(z))+ (c) RETURN 1 AS s") {
    val `reversed (a) ((n)-[r]-(m))+ (b) with previouslyBoundGroupRel` = `reversed (a) ((n)-[r]-(m))+ (b)`.copy(
      previouslyBoundRelationshipGroups = Set("rr", "rrr")
    )
    val `(b) ((x)-[rr]-(y)-[rrr]-(z))+ (c)` = TrailParameters(
      min = 1,
      max = Unlimited,
      start = "b",
      end = "c",
      innerStart = "x",
      innerEnd = "z",
      groupNodes = Set(),
      groupRelationships = Set(("rr", "rr"), ("rrr", "rrr")),
      innerRelationships = Set("rr", "rrr"),
      previouslyBoundRelationships = Set(),
      previouslyBoundRelationshipGroups = Set(),
      reverseGroupVariableProjections = false
    )
    val trail = subPlanBuilder
      .projection(project = Seq("1 AS s"), discard = Set("c", "rr", "r", "rrr", "b", "a"))
      .trail(`reversed (a) ((n)-[r]-(m))+ (b) with previouslyBoundGroupRel`)
      .|.filterExpression(isRepeatTrailUnique("r"))
      .|.expand("(m)<-[r]-(n)")
      .|.argument("m")
      .trail(`(b) ((x)-[rr]-(y)-[rrr]-(z))+ (c)`)
      .|.filterExpression(differentRelationships("rrr", "rr"), isRepeatTrailUnique("rrr"))
      .|.expand("(y)-[rrr]->(z)")
      .|.filterExpression(isRepeatTrailUnique("rr"))
      .|.expand("(x)-[rr]->(y)")
      .|.argument("x")
      .allNodeScan("b")
      .build()

    val expand = subPlanBuilder
      .projection(project = Seq("1 AS s"), discard = Set("c", "rr", "r", "rrr", "b", "a"))
      .filterExpression(disjoint(varFor("r"), varFor("rr")), disjoint(varFor("r"), varFor("rrr")))
      .expand("(b)<-[r*1..]-(a)")
      .trail(`(b) ((x)-[rr]-(y)-[rrr]-(z))+ (c)`)
      .|.filterExpression(differentRelationships("rrr", "rr"), isRepeatTrailUnique("rrr"))
      .|.expand("(y)-[rrr]->(z)")
      .|.filterExpression(isRepeatTrailUnique("rr"))
      .|.expand("(x)-[rr]->(y)")
      .|.argument("x")
      .allNodeScan("b")
      .build()

    rewrites(trail, expand)
  }

  // two qpps with provably different relationship types. do not insert any relationship uniqueness predicates because
  // there are no previously bound relationship group variables (planner knows they are provably disjoint)
  test("Rewrites MATCH (a) ((n)-[r:R]->(m))+ (b) ((x)-[rr:RR]->(y))+ (c) RETURN 1 AS s") {

    val trail = subPlanBuilder
      .projection(project = Seq("1 AS s"), discard = Set("c", "rr", "r", "b", "a"))
      .trail(`reversed (a) ((n)-[r]-(m))+ (b)`)
      .|.filterExpression(isRepeatTrailUnique("r"))
      .|.expand("(m)<-[r:R]-(n)")
      .|.argument("m")
      .trail(`(b) ((x)-[rr]-(y))+ (c)`)
      .|.filterExpression(isRepeatTrailUnique("rr"))
      .|.expand("(x)-[rr:RR]->(y)")
      .|.argument("x")
      .allNodeScan("b")
      .build()

    val expand = subPlanBuilder
      .projection(project = Seq("1 AS s"), discard = Set("c", "rr", "r", "b", "a"))
      .expand("(b)<-[r:R*1..]-(a)")
      .expand("(b)-[rr:RR*1..]->(c)")
      .allNodeScan("b")
      .build()

    rewrites(trail, expand)
  }

  // mix qpps and relationship pattern, inserts relationship uniqueness predicates when needed
  test("Rewrites MATCH (a) ((n)-[r]->(m))+ (b)-[rr]->(c) ((x)-[rrr]->(y))+ (d) RETURN 1 AS s") {
    val `(c) ((x)-[rrr]-(y))+ (d)` = TrailParameters(
      min = 1,
      max = Unlimited,
      start = "c",
      end = "d",
      innerStart = "x",
      innerEnd = "y",
      groupNodes = Set.empty,
      groupRelationships = Set(("rrr", "rrr")),
      innerRelationships = Set("rrr"),
      previouslyBoundRelationships = Set("rr"),
      previouslyBoundRelationshipGroups = Set("r"),
      reverseGroupVariableProjections = false
    )
    val trail = subPlanBuilder
      .projection(project = Seq("1 AS s"), discard = Set("c", "rr", "r", "rrr", "d", "b", "a"))
      .trail(`(c) ((x)-[rrr]-(y))+ (d)`)
      .|.filterExpression(isRepeatTrailUnique("rrr"))
      .|.expand("(x)-[rrr]->(y)")
      .|.argument("x")
      .filter("not rr IN r")
      .expand("(b)-[rr]->(c)")
      .trail(`(a) ((n)-[r]-(m))+ (b)`)
      .|.filterExpression(isRepeatTrailUnique("r"))
      .|.expand("(n)-[r]->(m)")
      .|.argument("n")
      .allNodeScan("a")
      .build()
    val expand = subPlanBuilder
      .projection(project = Seq("1 AS s"), discard = Set("c", "rr", "r", "rrr", "d", "b", "a"))
      .filterExpression(disjoint(varFor("rrr"), varFor("r")))
      .filter("not rr IN rrr")
      .expand("(c)-[rrr*1..]->(d)")
      .filter("not rr IN r")
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
      .trail(`(a) ((n)-[r]-(m))+ (b)`)
      .|.filterExpression(isRepeatTrailUnique("r"))
      .|.expand("(n)-[r]->(m)")
      .|.argument("n")
      .allNodeScan("a")
      .build()
    val expand = subPlanBuilder
      .distinct("b AS b")
      .expand("(a)-[r*1..]->(b)")
      .allNodeScan("a")
      .build()

    rewrites(trail, expand)
  }

  // named path uses group node variables
  test("Preserves MATCH p = (a) ((n)-[r]->(m))+ (b) RETURN p") {
    val `(a) ((n)-[r]-(m))+ with groupNodes("n")` = `(a) ((n)-[r]-(m))+ (b)`.copy(
      groupNodes = Set(("n", "n"))
    )
    val trail = subPlanBuilder
      .projection(
        project = Map("p" -> qppPath(varFor("a"), Seq(varFor("n"), varFor("r")), varFor("b"))),
        discard = Set("a", "b", "n", "r")
      )
      .trail(`(a) ((n)-[r]-(m))+ with groupNodes("n")`)
      .|.filterExpression(isRepeatTrailUnique("r"))
      .|.expandAll("(n)-[r]->(m)")
      .|.argument("n")
      .allNodeScan("a")
      .build()
    preserves(trail)
  }
}

object TrailToVarExpandRewriterTest
    extends CypherFunSuite
    with LogicalPlanTestOps
    with LogicalPlanConstructionTestSupport
    with AstConstructionTestSupport {

  private def rewrites(trail: LogicalPlan, expand: LogicalPlan): Assertion =
    rewrite(trail).stripProduceResults should equal(expand)

  private def preserves(trail: LogicalPlan): Assertion =
    rewrite(trail).stripProduceResults should equal(trail.stripProduceResults)

  private val `(a) ((n)-[r]-(m))+ (b)` = TrailParameters(
    min = 1,
    max = Unlimited,
    start = "a",
    end = "b",
    innerStart = "n",
    innerEnd = "m",
    groupNodes = Set.empty,
    groupRelationships = Set(("r", "r")),
    innerRelationships = Set("r"),
    previouslyBoundRelationships = Set.empty,
    previouslyBoundRelationshipGroups = Set.empty,
    reverseGroupVariableProjections = false
  )

  private val `reversed (a) ((n)-[r]-(m))+ (b)` = `(a) ((n)-[r]-(m))+ (b)`.copy(
    end = "a",
    start = "b",
    innerEnd = "n",
    innerStart = "m",
    reverseGroupVariableProjections = true
  )

  private val `(b) ((x)-[rr]-(y))+ (c)` = TrailParameters(
    min = 1,
    max = Unlimited,
    start = "b",
    end = "c",
    innerStart = "x",
    innerEnd = "y",
    groupNodes = Set.empty,
    groupRelationships = Set(("rr", "rr")),
    innerRelationships = Set("rr"),
    previouslyBoundRelationships = Set.empty,
    previouslyBoundRelationshipGroups = Set.empty,
    reverseGroupVariableProjections = false
  )

  private def rewrite(p: LogicalPlan): LogicalPlan = {
    fixedPoint((p: LogicalPlan) =>
      p.endoRewrite(TrailToVarExpandRewriter(new StubLabelAndRelTypeInfos, Attributes(idGen)))
    )(p)
  }

  private def subPlanBuilder = new LogicalPlanBuilder(wholePlan = false)
}
