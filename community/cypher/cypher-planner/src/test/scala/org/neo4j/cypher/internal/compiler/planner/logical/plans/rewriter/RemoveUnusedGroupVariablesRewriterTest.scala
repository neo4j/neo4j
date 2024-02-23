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
import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanBuilder
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.RemoveUnusedGroupVariablesRewriterTest.`(a) ((n)-[r]-(m))+ (b)`
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.RemoveUnusedGroupVariablesRewriterTest.preserves
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.RemoveUnusedGroupVariablesRewriterTest.rewrites
import org.neo4j.cypher.internal.ir.EagernessReason
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.TrailParameters
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNodeWithProperties
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.setNodeProperty
import org.neo4j.cypher.internal.logical.builder.TestNFABuilder
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandAll
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.NFA
import org.neo4j.cypher.internal.logical.plans.StatefulShortestPath
import org.neo4j.cypher.internal.util.UpperBound.Unlimited
import org.neo4j.cypher.internal.util.collection.immutable.ListSet
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.scalatest.Assertion

class RemoveUnusedGroupVariablesRewriterTest extends CypherFunSuite with LogicalPlanningTestSupport {

  // all node group variables unused
  test("MATCH (a) ((n)-[r]->(m))+ (b) RETURN r") {
    def plan(params: TrailParameters) =
      new LogicalPlanBuilder()
        .produceResults("r")
        .trail(params)
        .|.filterExpression(isRepeatTrailUnique("r_i"))
        .|.expandAll("(n_i)-[r_i]->(m_i)")
        .|.argument("n_i")
        .allNodeScan("a")
        .build()

    val origin = plan(`(a) ((n)-[r]-(m))+ (b)`.full)
    val target = plan(`(a) ((n)-[r]-(m))+ (b)`.nmless)
    rewrites(origin, target)
  }

  // all node group variables unused
  test("MATCH ANY SHORTEST (a) ((n)-[r]->(m))+ (b) RETURN r") {
    def plan(params: TrailParameters) =
      new LogicalPlanBuilder()
        .produceResults("r")
        .statefulShortestPath(
          sourceNode = "a",
          targetNode = "b",
          solvedExpressionString = "",
          nonInlinedPreFilters = None,
          groupNodes = params.groupNodes,
          groupRelationships = params.groupRelationships,
          singletonNodeVariables = Set("b" -> "b"),
          singletonRelationshipVariables = Set.empty,
          selector = StatefulShortestPath.Selector.Shortest(1),
          `(a) ((n)-[r]-(m))+ (b)`.nfa,
          ExpandAll,
          false
        )
        .allNodeScan("a")
        .build()

    val origin = plan(`(a) ((n)-[r]-(m))+ (b)`.full)
    val target = plan(`(a) ((n)-[r]-(m))+ (b)`.nmless)
    rewrites(origin, target)
  }

  // relationship group variable unused
  test("MATCH (a) ((n)-[r]->(m))+ (b) RETURN n, m") {
    def plan(params: TrailParameters) =
      new LogicalPlanBuilder()
        .produceResults("n", "m")
        .trail(params)
        .|.filterExpression(isRepeatTrailUnique("r_i"))
        .|.expandAll("(n_i)-[r_i]->(m_i)")
        .|.argument("n_i")
        .allNodeScan("a")
        .build()

    val origin = plan(`(a) ((n)-[r]-(m))+ (b)`.full)
    val target = plan(`(a) ((n)-[r]-(m))+ (b)`.rless)
    rewrites(origin, target)
  }

  // relationship group variable unused
  test("MATCH ANY SHORTEST (a) ((n)-[r]->(m))+ (b) RETURN n, m") {
    def plan(params: TrailParameters) =
      new LogicalPlanBuilder()
        .produceResults("n", "m")
        .statefulShortestPath(
          sourceNode = "a",
          targetNode = "b",
          solvedExpressionString = "",
          nonInlinedPreFilters = None,
          groupNodes = params.groupNodes,
          groupRelationships = params.groupRelationships,
          singletonNodeVariables = Set("b" -> "b"),
          singletonRelationshipVariables = Set.empty,
          selector = StatefulShortestPath.Selector.Shortest(1),
          `(a) ((n)-[r]-(m))+ (b)`.nfa,
          ExpandAll,
          false
        )
        .allNodeScan("a")
        .build()

    val origin = plan(`(a) ((n)-[r]-(m))+ (b)`.full)
    val target = plan(`(a) ((n)-[r]-(m))+ (b)`.rless)
    rewrites(origin, target)
  }

  // all group variables used
  test("MATCH (a) ((n)-[r]->(m))+ (b) RETURN n, r, m") {
    def plan(params: TrailParameters) =
      new LogicalPlanBuilder()
        .produceResults("n", "r", "m")
        .trail(params)
        .|.filterExpression(isRepeatTrailUnique("r_i"))
        .|.expandAll("(n_i)-[r_i]->(m_i)")
        .|.argument("n_i")
        .allNodeScan("a")
        .build()

    val origin = plan(`(a) ((n)-[r]-(m))+ (b)`.full)
    preserves(origin)
  }

  // all group variables used
  test("MATCH ANY SHORTEST (a) ((n)-[r]->(m))+ (b) RETURN n, r, m") {
    def plan(params: TrailParameters) =
      new LogicalPlanBuilder()
        .produceResults("n", "r", "m")
        .statefulShortestPath(
          sourceNode = "a",
          targetNode = "b",
          solvedExpressionString = "",
          nonInlinedPreFilters = None,
          groupNodes = params.groupNodes,
          groupRelationships = params.groupRelationships,
          singletonNodeVariables = Set("b" -> "b"),
          singletonRelationshipVariables = Set.empty,
          selector = StatefulShortestPath.Selector.Shortest(1),
          `(a) ((n)-[r]-(m))+ (b)`.nfa,
          ExpandAll,
          false
        )
        .allNodeScan("a")
        .build()

    val origin = plan(`(a) ((n)-[r]-(m))+ (b)`.full)
    preserves(origin)
  }

  // multiple qpp
  test("MATCH (a) ((n)-[r]->(m))+ (b) ((x)-[rr]->(y))+ (c) RETURN n, x") {
    def plan(firstParams: TrailParameters, secondParams: TrailParameters) =
      new LogicalPlanBuilder()
        .produceResults("n", "x")
        .trail(secondParams)
        .|.filterExpression(isRepeatTrailUnique("rr_i"))
        .|.expandAll("(x_i)-[rr_i]->(y_i)")
        .|.argument("x_i")
        .trail(firstParams)
        .|.filterExpression(isRepeatTrailUnique("r_i"))
        .|.expandAll("(n_i)-[r_i]->(m_i)")
        .|.argument("n_i")
        .allNodeScan("a")
        .build()

    object `(b) ((x)-[rr]-(y)) (c)` {
      val full: TrailParameters = TrailParameters(
        min = 1,
        max = Unlimited,
        start = "b",
        end = "c",
        innerStart = "x_i",
        innerEnd = "y_i",
        groupNodes = Set(("x_i", "x"), ("y_i", "y")),
        groupRelationships = Set(("rr_i", "rr")),
        innerRelationships = Set("rr_i"),
        previouslyBoundRelationships = Set.empty,
        previouslyBoundRelationshipGroups = Set("r"),
        reverseGroupVariableProjections = false
      )

      val rryless: TrailParameters = full.copy(groupNodes = Set(("x_i", "x")), groupRelationships = Set.empty)
    }

    val origin = plan(`(a) ((n)-[r]-(m))+ (b)`.full, `(b) ((x)-[rr]-(y)) (c)`.full)
    val target = plan(`(a) ((n)-[r]-(m))+ (b)`.mless, `(b) ((x)-[rr]-(y)) (c)`.rryless)
    rewrites(origin, target)
  }

  // group variable used in post-filter predicate (list index lookup)
  test("MATCH (a) ((n)-[r]->(m))+ (b) WHERE n[0].p = 0 RETURN 1 AS s") {
    def plan(params: TrailParameters) =
      new LogicalPlanBuilder()
        .produceResults("s")
        .projection("1 AS s")
        .filter("(n[0]).p = 0")
        .trail(params)
        .|.filterExpression(isRepeatTrailUnique("r_i"))
        .|.expandAll("(n_i)-[r_i]->(m_i)")
        .|.argument("n_i")
        .allNodeScan("a")
        .build()

    val origin = plan(`(a) ((n)-[r]-(m))+ (b)`.full)
    val target = plan(`(a) ((n)-[r]-(m))+ (b)`.rmless)
    rewrites(origin, target)
  }

  // group variable used in post-filter predicate (list traversal)
  test("MATCH (a) ((n)-[r]->(m))+ (b) WHERE all(x IN n WHERE x.p = 0) RETURN 1 AS s") {
    def plan(params: TrailParameters) =
      new LogicalPlanBuilder()
        .produceResults("s")
        .projection("1 AS s")
        .filter("all(x IN n WHERE x.p = 0)")
        .trail(params)
        .|.filterExpression(isRepeatTrailUnique("r_i"))
        .|.expandAll("(n_i)-[r_i]->(m_i)")
        .|.argument("n_i")
        .allNodeScan("a")
        .build()

    val origin = plan(`(a) ((n)-[r]-(m))+ (b)`.full)
    val target = plan(`(a) ((n)-[r]-(m))+ (b)`.rmless)
    rewrites(origin, target)
  }

  // group variable used in pre-filter predicate
  test("MATCH (a) ((n)-[r]->(m) WHERE n.p = 0)+ (b) RETURN 1 AS s") {
    def plan(params: TrailParameters, projectionDiscard: Set[String]) =
      new LogicalPlanBuilder()
        .produceResults("s")
        .projection("1 AS s")
        .trail(params)
        .|.filterExpression(isRepeatTrailUnique("r_i"))
        .|.expandAll("(n_i)-[r_i]->(m_i)")
        .|.filter("n_i.p = 0")
        .|.argument("n_i")
        .filter("a.p = 0")
        .allNodeScan("a")
        .build()

    val origin = plan(`(a) ((n)-[r]-(m))+ (b)`.full, Set("r", "a", "n", "b", "m"))
    val target = plan(`(a) ((n)-[r]-(m))+ (b)`.empty, Set("a", "b"))
    rewrites(origin, target)
  }

  // relationship group variable used in implicit join
  test("MATCH (a) ((n)-[r]->(m))+ (b) MATCH (x)-[r*]->(y) RETURN 1 AS s") {
    def plan(params: TrailParameters, projectionDiscard: Set[String]) =
      new LogicalPlanBuilder()
        .produceResults("s")
        .projection("1 AS s")
        .valueHashJoin("r = anon_6")
        .|.expand("(x)-[anon_6*1..]->(y)")
        .|.allNodeScan("x")
        .filter("size(r) >= 1")
        .trail(params)
        .|.filterExpression(isRepeatTrailUnique("r_i"))
        .|.expandAll("(n_i)-[r_i]->(m_i)")
        .|.argument("n_i")
        .allNodeScan("a")
        .build()

    val origin = plan(`(a) ((n)-[r]-(m))+ (b)`.full, Set("r", "a", "n", "b", "m", "x", "y", "anon_6"))
    val target = plan(`(a) ((n)-[r]-(m))+ (b)`.nmless, Set("r", "a", "b", "x", "y", "anon_6"))
    rewrites(origin, target)
  }

  // node group variable used in explicit join
  test("MATCH (a) ((n)-[r]->(m))+ (b) MATCH (x WHERE x = n[0]) RETURN 1 AS s") {
    def plan(params: TrailParameters, projectionDiscard: Set[String]) =
      new LogicalPlanBuilder()
        .produceResults("s")
        .projection("1 AS s")
        .valueHashJoin("n[0] = x")
        .|.allNodeScan("x")
        .trail(params)
        .|.filterExpression(isRepeatTrailUnique("r_i"))
        .|.expandAll("(n_i)-[r_i]->(m_i)")
        .|.argument("n_i")
        .allNodeScan("a")
        .build()

    val origin = plan(`(a) ((n)-[r]-(m))+ (b)`.full, Set("r", "a", "n", "b", "m", "x"))
    val target = plan(`(a) ((n)-[r]-(m))+ (b)`.rmless, Set("a", "n", "b", "x"))
    rewrites(origin, target)
  }

  // group variable used in projection
  test("MATCH (a) ((n)-[r]->(m))+ (b) WITH n[0] AS x RETURN x") {
    def plan(params: TrailParameters, projectionDiscard: Set[String]) =
      new LogicalPlanBuilder()
        .produceResults("x")
        .projection("n[0] AS x")
        .trail(params)
        .|.filterExpression(isRepeatTrailUnique("r_i"))
        .|.expandAll("(n_i)-[r_i]->(m_i)")
        .|.argument("n_i")
        .allNodeScan("a")
        .build()

    val origin = plan(`(a) ((n)-[r]-(m))+ (b)`.full, Set("a", "b", "r", "n", "m"))
    val target = plan(`(a) ((n)-[r]-(m))+ (b)`.rmless, Set("a", "b", "n"))
    rewrites(origin, target)
  }

  // group variables used by named path
  test("MATCH p = (a) ((n)-[r]->(m))+ (b) RETURN p") {
    def plan(params: TrailParameters, projectionDiscard: Set[String]) = {
      val pathExpression = qppPath(v"a", Seq(v"n", v"r"), v"b")
      new LogicalPlanBuilder()
        .produceResults("p")
        .projection(Map("p" -> pathExpression))
        .trail(params)
        .|.filterExpression(isRepeatTrailUnique("r_i"))
        .|.expandAll("(n_i)-[r_i]->(m_i)")
        .|.argument("n_i")
        .allNodeScan("a")
        .build()
    }

    val origin = plan(`(a) ((n)-[r]-(m))+ (b)`.full, Set("a", "b", "r", "n", "m"))
    val target = plan(`(a) ((n)-[r]-(m))+ (b)`.mless, Set("r", "n", "a", "b"))
    rewrites(origin, target)
  }

  // group variables used by named path
  test("MATCH p = ANY SHORTEST (a) ((n)-[r]->(m))+ (b) RETURN p") {
    def plan(params: TrailParameters) = {
      val pathExpression = qppPath(v"a", Seq(v"n", v"r"), v"b")
      new LogicalPlanBuilder()
        .produceResults("p")
        .projection(Map("p" -> pathExpression))
        .statefulShortestPath(
          sourceNode = "a",
          targetNode = "b",
          solvedExpressionString = "",
          nonInlinedPreFilters = None,
          groupNodes = params.groupNodes,
          groupRelationships = params.groupRelationships,
          singletonNodeVariables = Set("b" -> "b"),
          singletonRelationshipVariables = Set.empty,
          selector = StatefulShortestPath.Selector.Shortest(1),
          `(a) ((n)-[r]-(m))+ (b)`.nfa,
          ExpandAll,
          false
        )
        .allNodeScan("a")
        .build()
    }

    val origin = plan(`(a) ((n)-[r]-(m))+ (b)`.full)
    val target = plan(`(a) ((n)-[r]-(m))+ (b)`.mless)
    rewrites(origin, target)
  }

  // group variables used by named path - named path is pruned, so the group variables are unused
  // note that in this case, the named path would be itself be removed before planning
  test("MATCH p = (a) ((n)-[r]->(m))+ (b) RETURN 1 AS s") {
    def plan(params: TrailParameters, projectionDiscard: Set[String]) =
      new LogicalPlanBuilder()
        .produceResults("s")
        .projection("1 AS s")
        .trail(params)
        .|.filterExpression(isRepeatTrailUnique("r_i"))
        .|.expandAll("(n_i)-[r_i]->(m_i)")
        .|.argument("n_i")
        .allNodeScan("a")
        .build()

    val origin = plan(`(a) ((n)-[r]-(m))+ (b)`.full, Set("r", "a", "n", "b", "m"))
    val target = plan(`(a) ((n)-[r]-(m))+ (b)`.empty, Set("a", "b"))
    rewrites(origin, target)
  }

  // unused group variables in subquery
  test("MATCH (x) CALL { MATCH (a) ((n)-[r]->(m))+ (b) RETURN 1 AS s } RETURN x") {
    def plan(params: TrailParameters) =
      new LogicalPlanBuilder()
        .produceResults("x")
        .cartesianProduct()
        .|.projection("1 AS s")
        .|.trail(params)
        .|.|.filterExpression(isRepeatTrailUnique("r_i"))
        .|.|.expandAll("(n_i)-[r_i]->(m_i)")
        .|.|.argument("n_i")
        .|.allNodeScan("a")
        .allNodeScan("x")
        .build()

    val origin = plan(`(a) ((n)-[r]-(m))+ (b)`.full)
    val target = plan(`(a) ((n)-[r]-(m))+ (b)`.empty)
    rewrites(origin, target)
  }

  // group variable used in ORDER BY
  test("MATCH (a) ((n)-[r]->(m))+ (b) RETURN 1 AS s ORDER BY n") {
    def plan(params: TrailParameters, projectionDiscard: Set[String]) =
      new LogicalPlanBuilder()
        .produceResults("s")
        .projection("1 AS s")
        .sort("n ASC")
        .trail(params)
        .|.filterExpression(isRepeatTrailUnique("r_i"))
        .|.expandAll("(n_i)-[r_i]->(m_i)")
        .|.argument("n_i")
        .allNodeScan("a")
        .build()

    val origin = plan(`(a) ((n)-[r]-(m))+ (b)`.full, Set("m", "r", "b", "n", "a"))
    val target = plan(`(a) ((n)-[r]-(m))+ (b)`.rmless, Set("a", "n", "b"))
    rewrites(origin, target)
  }

  // group variable used in UNWIND
  test("MATCH (a) ((n)-[r]->(m))+ (b) UNWIND n AS x RETURN 1 AS s") {
    def plan(params: TrailParameters, projectionDiscard: Set[String]) =
      new LogicalPlanBuilder()
        .produceResults("s")
        .projection("1 AS s")
        .unwind("n AS x")
        .trail(params)
        .|.filterExpression(isRepeatTrailUnique("r_i"))
        .|.expandAll("(n_i)-[r_i]->(m_i)")
        .|.argument("n_i")
        .allNodeScan("a")
        .build()

    val origin = plan(`(a) ((n)-[r]-(m))+ (b)`.full, Set("m", "r", "b", "x", "n", "a"))
    val target = plan(`(a) ((n)-[r]-(m))+ (b)`.rmless, Set("b", "x", "n", "a"))
    rewrites(origin, target)
  }

  // group variable used as Procedure argument
  test("MATCH (a) ((n)-[r]->(m))+ (b) RETURN abs(n[0].p)") {
    def plan(params: TrailParameters, projectionDiscard: Set[String]) =
      new LogicalPlanBuilder()
        .produceResults("`abs(n[0].p)`")
        .projection("abs((n[0]).p) AS `abs(n[0].p)`")
        .trail(params)
        .|.filterExpression(isRepeatTrailUnique("r_i"))
        .|.expandAll("(n_i)-[r_i]->(m_i)")
        .|.argument("n_i")
        .allNodeScan("a")
        .build()

    val origin = plan(`(a) ((n)-[r]-(m))+ (b)`.full, Set("m", "r", "b", "n", "a"))
    val target = plan(`(a) ((n)-[r]-(m))+ (b)`.rmless, Set("b", "n", "a"))
    rewrites(origin, target)
  }

  // group variable used in write
  test("MATCH (a) ((n)-[r]->(m))+ (b) CREATE ({p: n[0].p})") {
    def plan(params: TrailParameters) =
      new LogicalPlanBuilder()
        .produceResults()
        .emptyResult()
        .create(createNodeWithProperties("anon_0", Seq(), "{p: (n[0]).p}"))
        .eager(ListSet(EagernessReason.Unknown))
        .trail(params)
        .|.filterExpression(isRepeatTrailUnique("r_i"))
        .|.expandAll("(n_i)-[r_i]->(m_i)")
        .|.argument("n_i")
        .allNodeScan("a")
        .build()

    val origin = plan(`(a) ((n)-[r]-(m))+ (b)`.full)
    val target = plan(`(a) ((n)-[r]-(m))+ (b)`.rmless)
    rewrites(origin, target)
  }

  // group variable used in FOREACH
  test("MATCH (a) ((n)-[r]->(m))+ (b) FOREACH (x IN n | SET x.p = 0)") {
    def plan(params: TrailParameters) =
      new LogicalPlanBuilder()
        .produceResults()
        .emptyResult()
        .foreach("x", "n", Seq(setNodeProperty("x", "p", "0")))
        .trail(params)
        .|.filterExpression(isRepeatTrailUnique("r_i"))
        .|.expandAll("(n_i)-[r_i]->(m_i)")
        .|.argument("n_i")
        .allNodeScan("a")
        .build()

    val origin = plan(`(a) ((n)-[r]-(m))+ (b)`.full)
    val target = plan(`(a) ((n)-[r]-(m))+ (b)`.rmless)
    rewrites(origin, target)
  }

  // group variable used in for comprehension
  test("MATCH (a) ((n)-[r]->(m))+ (b) RETURN [i IN n WHERE i.p = 0] AS s") {
    def plan(params: TrailParameters, projectionDiscard: Set[String]) =
      new LogicalPlanBuilder()
        .produceResults("s")
        .projection("[i IN n WHERE i.p = 0] AS s")
        .trail(params)
        .|.filterExpression(isRepeatTrailUnique("r_i"))
        .|.expandAll("(n_i)-[r_i]->(m_i)")
        .|.argument("n_i")
        .allNodeScan("a")
        .build()

    val origin = plan(`(a) ((n)-[r]-(m))+ (b)`.full, Set("m", "r", "b", "n", "a"))
    val target = plan(`(a) ((n)-[r]-(m))+ (b)`.rmless, Set("b", "n", "a"))
    rewrites(origin, target)
  }
}

object RemoveUnusedGroupVariablesRewriterTest extends CypherFunSuite {

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

    val rmless: TrailParameters = full.copy(groupNodes = Set(("n_i", "n")), groupRelationships = Set.empty)

    val empty: TrailParameters = full.copy(groupNodes = Set.empty, groupRelationships = Set.empty)

    val nfa: NFA = new TestNFABuilder(0, "a")
      .addTransition(0, 1, "(a) (n_i)")
      .addTransition(1, 2, "(n_i)-[r_i]-(m_i)")
      .addTransition(2, 1, "(m_i) (n_i)")
      .addTransition(2, 3, "(m_i) (b)")
      .setFinalState(3)
      .build()
  }

  def preserves(origin: LogicalPlan): Assertion =
    rewrite(origin) should equal(origin)

  def rewrites(origin: LogicalPlan, target: LogicalPlan): Assertion =
    rewrite(origin) should equal(target)

  private def rewrite(p: LogicalPlan): LogicalPlan =
    p.endoRewrite(RemoveUnusedGroupVariablesRewriter)

}
