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

import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanBuilder
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.RemoveUnusedGroupVariablesRewriterTest.`(a) ((n)-[r]-(m))+ (b)`
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.RemoveUnusedGroupVariablesRewriterTest.preserves
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.RemoveUnusedGroupVariablesRewriterTest.rewrites
import org.neo4j.cypher.internal.ir.EagernessReason
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.TrailParameters
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNodeWithProperties
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.setNodeProperty
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.util.UpperBound.Unlimited
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.scalatest.Assertion

import scala.collection.immutable.ListSet

class RemoveUnusedGroupVariablesRewriterTest extends CypherFunSuite with LogicalPlanningTestSupport {

  // all node group variables unused
  test("MATCH (a) ((n)-[r]->(m))+ (b) RETURN r") {
    def plan(params: TrailParameters) =
      new LogicalPlanBuilder()
        .produceResults("r")
        .trail(params)
        .|.filterExpression(isRepeatTrailUnique("r"))
        .|.expandAll("(n_i)-[r]->(m_i)")
        .|.argument("n_i")
        .allNodeScan("a")
        .build()

    val origin = plan(`(a) ((n)-[r]-(m))+ (b)`.full)
    val target = plan(`(a) ((n)-[r]-(m))+ (b)`.nodeless)
    rewrites(origin, target)
  }

  // relationship group variable unused
  test("MATCH (a) ((n)-[r]->(m))+ (b) RETURN n, m") {
    def plan(params: TrailParameters) =
      new LogicalPlanBuilder()
        .produceResults("n", "m")
        .trail(params)
        .|.filterExpression(isRepeatTrailUnique("r"))
        .|.expandAll("(n)-[r]->(m)")
        .|.argument("n")
        .allNodeScan("a")
        .build()

    val origin = plan(`(a) ((n)-[r]-(m))+ (b)`.full)
    preserves(origin)
  }

  // all group variables used
  test("MATCH (a) ((n)-[r]->(m))+ (b) RETURN n, r, m") {
    def plan(params: TrailParameters) =
      new LogicalPlanBuilder()
        .produceResults("n", "r", "m")
        .trail(params)
        .|.filterExpression(isRepeatTrailUnique("r"))
        .|.expandAll("(n)-[r]->(m)")
        .|.argument("n")
        .allNodeScan("a")
        .build()

    val origin = plan(`(a) ((n)-[r]-(m))+ (b)`.full)
    preserves(origin)
  }

  // multiple qpp
  test("MATCH (a) ((n)-[r]->(m))+ (b) ((x)-[rr]->(y))+ (c) RETURN n, x") {
    def plan(atocParams: TrailParameters, xtoyParams: TrailParameters) =
      new LogicalPlanBuilder()
        .produceResults("n", "x")
        .trail(xtoyParams)
        .|.filterExpression(isRepeatTrailUnique("rr"))
        .|.expandAll("(x_i)-[rr]->(y_i)")
        .|.argument("x_i")
        .trail(atocParams)
        .|.filterExpression(isRepeatTrailUnique("r"))
        .|.expandAll("(n_i)-[r]->(m_i)")
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
        groupRelationships = Set(("rr", "rr")),
        innerRelationships = Set("rr"),
        previouslyBoundRelationships = Set.empty,
        previouslyBoundRelationshipGroups = Set("r"),
        reverseGroupVariableProjections = false
      )

      val yless = full.copy(groupNodes = Set(("x_i", "x")))
    }

    val origin = plan(`(a) ((n)-[r]-(m))+ (b)`.full, `(b) ((x)-[rr]-(y)) (c)`.full)
    val target = plan(`(a) ((n)-[r]-(m))+ (b)`.mless, `(b) ((x)-[rr]-(y)) (c)`.yless)
    rewrites(origin, target)
  }

  // group variable used in post-filter predicate (list index lookup)
  test("MATCH (a) ((n)-[r]->(m))+ (b) WHERE n[0].p = 0 RETURN 1 AS s") {
    def plan(params: TrailParameters, projectionDiscard: Set[String]) =
      new LogicalPlanBuilder()
        .produceResults("s")
        .projection(project = Seq("1 AS s"), discard = projectionDiscard)
        .filter("(n[0]).p = 0")
        .trail(params)
        .|.filterExpression(isRepeatTrailUnique("r"))
        .|.expandAll("(n_i)-[r]->(m_i)")
        .|.argument("n_i")
        .allNodeScan("a")
        .build()

    val origin = plan(`(a) ((n)-[r]-(m))+ (b)`.full, Set("r", "a", "n", "m", "b"))
    val target = plan(`(a) ((n)-[r]-(m))+ (b)`.mless, Set("r", "a", "n", "b"))
    rewrites(origin, target)
  }

  // group variable used in post-filter predicate (list traversal)
  test("MATCH (a) ((n)-[r]->(m))+ (b) WHERE all(x IN n WHERE x.p = 0) RETURN 1 AS s") {
    def plan(params: TrailParameters, projectionDiscard: Set[String]) =
      new LogicalPlanBuilder()
        .produceResults("s")
        .projection(project = Seq("1 AS s"), discard = projectionDiscard)
        .filter("all(x IN n WHERE x.p = 0)")
        .trail(params)
        .|.filterExpression(isRepeatTrailUnique("r"))
        .|.expandAll("(n_i)-[r]->(m_i)")
        .|.argument("n_i")
        .allNodeScan("a")
        .build()

    val origin = plan(`(a) ((n)-[r]-(m))+ (b)`.full, Set("r", "a", "n", "b", "m"))
    val target = plan(`(a) ((n)-[r]-(m))+ (b)`.mless, Set("r", "a", "n", "b"))
    rewrites(origin, target)
  }

  // group variable used in pre-filter predicate
  test("MATCH (a) ((n)-[r]->(m) WHERE n.p = 0)+ (b) RETURN 1 AS s") {
    def plan(params: TrailParameters, projectionDiscard: Set[String]) =
      new LogicalPlanBuilder()
        .produceResults("s")
        .projection(project = Seq("1 AS s"), discard = projectionDiscard)
        .trail(params)
        .|.filterExpression(isRepeatTrailUnique("r"))
        .|.expandAll("(n_i)-[r]->(m_i)")
        .|.filter("n.p = 0")
        .|.argument("n_i")
        .filter("a.p = 0")
        .allNodeScan("a")
        .build()

    val origin = plan(`(a) ((n)-[r]-(m))+ (b)`.full, Set("r", "a", "n", "b", "m"))
    val target = plan(`(a) ((n)-[r]-(m))+ (b)`.mless, Set("r", "a", "n", "b"))
    rewrites(origin, target)
  }

  // relationship group variable used in implicit join
  test("MATCH (a) ((n)-[r]->(m))+ (b) MATCH (x)-[r*]->(y) RETURN 1 AS s") {
    def plan(params: TrailParameters, projectionDiscard: Set[String]) =
      new LogicalPlanBuilder()
        .produceResults("s")
        .projection(project = Seq("1 AS s"), discard = projectionDiscard)
        .cartesianProduct()
        .|.expand("(x)-[r*1..]->(y)")
        .|.allNodeScan("x")
        .filter("size(r) >= 1")
        .trail(params)
        .|.filterExpression(isRepeatTrailUnique("r"))
        .|.expandAll("(n_i)-[r]->(m_i)")
        .|.argument("n_i")
        .allNodeScan("a")
        .build()

    val origin = plan(`(a) ((n)-[r]-(m))+ (b)`.full, Set("r", "a", "n", "b", "m", "x", "y"))
    val target = plan(`(a) ((n)-[r]-(m))+ (b)`.nodeless, Set("r", "a", "b", "x", "y"))
    rewrites(origin, target)
  }

  // node group variable used in explicit join
  test("MATCH (a) ((n)-[r]->(m))+ (b) MATCH (x WHERE x = n[0]) RETURN 1 AS s") {
    def plan(params: TrailParameters, projectionDiscard: Set[String]) =
      new LogicalPlanBuilder()
        .produceResults("s")
        .projection(project = Seq("1 AS s"), discard = projectionDiscard)
        .valueHashJoin("n[0] = x")
        .|.allNodeScan("x")
        .trail(params)
        .|.filterExpression(isRepeatTrailUnique("r"))
        .|.expandAll("(n_i)-[r]->(m_i)")
        .|.argument("n")
        .allNodeScan("a")
        .build()

    val origin = plan(`(a) ((n)-[r]-(m))+ (b)`.full, Set("r", "a", "n", "b", "m", "x"))
    val target = plan(`(a) ((n)-[r]-(m))+ (b)`.mless, Set("r", "a", "n", "b", "x"))
    rewrites(origin, target)
  }

  // group variable used in projection
  test("MATCH (a) ((n)-[r]->(m))+ (b) WITH n[0] AS x RETURN x") {
    def plan(params: TrailParameters, projectionDiscard: Set[String]) =
      new LogicalPlanBuilder()
        .produceResults("x")
        .projection(project = Seq("n[0] AS x"), discard = projectionDiscard)
        .trail(params)
        .|.filterExpression(isRepeatTrailUnique("r"))
        .|.expandAll("(n_i)-[r]->(m_i)")
        .|.argument("n_i")
        .allNodeScan("a")
        .build()

    val origin = plan(`(a) ((n)-[r]-(m))+ (b)`.full, Set("a", "b", "r", "n", "m"))
    val target = plan(`(a) ((n)-[r]-(m))+ (b)`.mless, Set("a", "b", "r", "n"))
    rewrites(origin, target)
  }

  // group variables used by named path
  test("MATCH p = (a) ((n)-[r]->(m))+ (b) RETURN p") {
    def plan(params: TrailParameters, projectionDiscard: Set[String]) = {
      val pathExpression = qppPath(varFor("a"), Seq(varFor("n"), varFor("r")), varFor("b"))
      new LogicalPlanBuilder()
        .produceResults("p")
        .projection(project = Map("p" -> pathExpression), discard = projectionDiscard)
        .trail(params)
        .|.filterExpression(isRepeatTrailUnique("r"))
        .|.expandAll("(n_i)-[r]->(m_i)")
        .|.argument("n_i")
        .allNodeScan("a")
        .build()
    }

    val origin = plan(`(a) ((n)-[r]-(m))+ (b)`.full, Set("a", "b", "r", "n", "m"))
    val target = plan(`(a) ((n)-[r]-(m))+ (b)`.mless, Set("a", "b", "r", "n"))
    rewrites(origin, target)
  }

  // group variables used by named path - named path is pruned, so the group variables are unused
  // note that in this case, the named path would be itself be removed before planning
  test("MATCH p = (a) ((n)-[r]->(m))+ (b) RETURN 1 AS s") {
    def plan(params: TrailParameters, projectionDiscard: Set[String]) =
      new LogicalPlanBuilder()
        .produceResults("s")
        .projection(project = Seq("1 AS s"), discard = projectionDiscard)
        .trail(params)
        .|.filterExpression(isRepeatTrailUnique("r"))
        .|.expandAll("(n_i)-[r]->(m_i)")
        .|.argument("n_i")
        .allNodeScan("a")
        .build()

    val origin = plan(`(a) ((n)-[r]-(m))+ (b)`.full, Set("r", "a", "n", "b", "m"))
    val target = plan(`(a) ((n)-[r]-(m))+ (b)`.nodeless, Set("r", "a", "b"))
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
        .|.|.filterExpression(isRepeatTrailUnique("r"))
        .|.|.expandAll("(n_i)-[r]->(m_i)")
        .|.|.argument("n_i")
        .|.allNodeScan("a")
        .allNodeScan("x")
        .build()

    val origin = plan(`(a) ((n)-[r]-(m))+ (b)`.full)
    val target = plan(`(a) ((n)-[r]-(m))+ (b)`.nodeless)
    rewrites(origin, target)
  }

  // group variable used in ORDER BY
  test("MATCH (a) ((n)-[r]->(m))+ (b) RETURN 1 AS s ORDER BY n") {
    def plan(params: TrailParameters, projectionDiscard: Set[String]) =
      new LogicalPlanBuilder()
        .produceResults("s")
        .projection(project = Seq("1 AS s"), discard = projectionDiscard)
        .sort("n ASC")
        .trail(params)
        .|.filterExpression(isRepeatTrailUnique("r"))
        .|.expandAll("(n_i)-[r]->(m_i)")
        .|.argument("n_i")
        .allNodeScan("a")
        .build()

    val origin = plan(`(a) ((n)-[r]-(m))+ (b)`.full, Set("m", "r", "b", "n", "a"))
    val target = plan(`(a) ((n)-[r]-(m))+ (b)`.mless, Set("a", "r", "n", "b"))
    rewrites(origin, target)
  }

  // group variable used in UNWIND
  test("MATCH (a) ((n)-[r]->(m))+ (b) UNWIND n AS x RETURN 1 AS s") {
    def plan(params: TrailParameters, projectionDiscard: Set[String]) =
      new LogicalPlanBuilder()
        .produceResults("s")
        .projection(project = Seq("1 AS s"), discard = projectionDiscard)
        .unwind("n AS x")
        .trail(params)
        .|.filterExpression(isRepeatTrailUnique("r"))
        .|.expandAll("(n_i)-[r]->(m_i)")
        .|.argument("n_i")
        .allNodeScan("a")
        .build()

    val origin = plan(`(a) ((n)-[r]-(m))+ (b)`.full, Set("m", "r", "b", "x", "n", "a"))
    val target = plan(`(a) ((n)-[r]-(m))+ (b)`.mless, Set("r", "b", "x", "n", "a"))
    rewrites(origin, target)
  }

  // group variable used as Procedure argument
  test("MATCH (a) ((n)-[r]->(m))+ (b) RETURN abs(n[0].p)") {
    def plan(params: TrailParameters, projectionDiscard: Set[String]) =
      new LogicalPlanBuilder()
        .produceResults("`abs(n[0].p)`")
        .projection(project = Seq("abs((n[0]).p) AS `abs(n[0].p)`"), discard = projectionDiscard)
        .trail(params)
        .|.filterExpression(isRepeatTrailUnique("r"))
        .|.expandAll("(n_i)-[r]->(m_i)")
        .|.argument("n_i")
        .allNodeScan("a")
        .build()

    val origin = plan(`(a) ((n)-[r]-(m))+ (b)`.full, Set("m", "r", "b", "n", "a"))
    val target = plan(`(a) ((n)-[r]-(m))+ (b)`.mless, Set("r", "b", "n", "a"))
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
        .|.filterExpression(isRepeatTrailUnique("r"))
        .|.expandAll("(n_i)-[r]->(m_i)")
        .|.argument("n_i")
        .allNodeScan("a")
        .build()

    val origin = plan(`(a) ((n)-[r]-(m))+ (b)`.full)
    val target = plan(`(a) ((n)-[r]-(m))+ (b)`.mless)
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
        .|.filterExpression(isRepeatTrailUnique("r"))
        .|.expandAll("(n_i)-[r]->(m_i)")
        .|.argument("n")
        .allNodeScan("a")
        .build()

    val origin = plan(`(a) ((n)-[r]-(m))+ (b)`.full)
    val target = plan(`(a) ((n)-[r]-(m))+ (b)`.mless)
    rewrites(origin, target)
  }

  // group variable used in for comprehension
  test("MATCH (a) ((n)-[r]->(m))+ (b) RETURN [i IN n WHERE i.p = 0] AS s") {
    def plan(params: TrailParameters, projectionDiscard: Set[String]) =
      new LogicalPlanBuilder()
        .produceResults("s")
        .projection(project = Seq("[i IN n WHERE i.p = 0] AS s"), projectionDiscard)
        .trail(params)
        .|.filterExpression(isRepeatTrailUnique("r"))
        .|.expandAll("(n_i)-[r]->(m_i)")
        .|.argument("n")
        .allNodeScan("a")
        .build()

    val origin = plan(`(a) ((n)-[r]-(m))+ (b)`.full, Set("m", "r", "b", "n", "a"))
    val target = plan(`(a) ((n)-[r]-(m))+ (b)`.mless, Set("r", "b", "n", "a"))
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
      groupRelationships = Set(("r", "r")),
      innerRelationships = Set("r"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = false
    )

    val nless: TrailParameters = full.copy(groupNodes = Set(("m_i", "m")))

    val rless: TrailParameters = full.copy(groupRelationships = Set.empty)

    val mless: TrailParameters = full.copy(groupNodes = Set(("n_i", "n")))

    val nodeless: TrailParameters = full.copy(groupNodes = Set.empty)

    val empty: TrailParameters = full.copy(groupNodes = Set.empty, groupRelationships = Set.empty)
  }

  def preserves(origin: LogicalPlan): Assertion =
    rewrite(origin) should equal(origin)

  def rewrites(origin: LogicalPlan, target: LogicalPlan): Assertion =
    rewrite(origin) should equal(target)

  private def rewrite(p: LogicalPlan): LogicalPlan =
    p.endoRewrite(RemoveUnusedGroupVariablesRewriter)

}
