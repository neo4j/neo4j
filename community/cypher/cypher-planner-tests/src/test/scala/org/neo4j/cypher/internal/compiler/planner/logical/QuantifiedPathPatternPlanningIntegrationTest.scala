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

import org.neo4j.cypher.internal.CypherVersionTestSupport
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.compiler.CypherPlannerTestSuite
import org.neo4j.cypher.internal.compiler.ExecutionModel
import org.neo4j.cypher.internal.compiler.ExecutionModel.BatchedParallel
import org.neo4j.cypher.internal.compiler.ExecutionModel.Volcano
import org.neo4j.cypher.internal.compiler.planner.AttributeComparisonStrategy
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningAttributesTestSupport
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningIntegrationTestSupport
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfiguration
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfigurationBuilder
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfigurationBuilder.DatabaseFormat
import org.neo4j.cypher.internal.compiler.planner.logical.QuantifiedPathPatternPlanningIntegrationTestBase.RelationshipPredicate
import org.neo4j.cypher.internal.compiler.planner.logical.QuantifiedPathPatternPlanningIntegrationTestBase.relationshipPredicatesWithPropertyAccess
import org.neo4j.cypher.internal.compiler.planner.logical.QuantifiedPathPatternPlanningIntegrationTestBase.relationshipPredicatesWithoutPropertyAccess
import org.neo4j.cypher.internal.expressions.AssertIsNode
import org.neo4j.cypher.internal.expressions.HasLabels
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.MultiRelationshipPathStep
import org.neo4j.cypher.internal.expressions.NilPathStep
import org.neo4j.cypher.internal.expressions.NodePathStep
import org.neo4j.cypher.internal.expressions.PathExpression
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.SemanticDirection.INCOMING
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.expressions.functions.EndNode
import org.neo4j.cypher.internal.ir.EagernessReason.Conflict
import org.neo4j.cypher.internal.ir.EagernessReason.ReadCreateConflict
import org.neo4j.cypher.internal.ir.EagernessReason.ReadDeleteConflict
import org.neo4j.cypher.internal.ir.EagernessReason.TypeReadSetConflict
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.Predicate
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.ToExpression
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.TrailParameters
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.WalkParameters
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.andsReorderable
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.column
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNode
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNodeWithProperties
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createRelationship
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandAll
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandInto
import org.neo4j.cypher.internal.logical.plans.Expand.VariablePredicate
import org.neo4j.cypher.internal.logical.plans.GetValue
import org.neo4j.cypher.internal.logical.plans.IndexOrderAscending
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.logical.plans.TraversalPathMode.Trail
import org.neo4j.cypher.internal.logical.plans.VarExpand
import org.neo4j.cypher.internal.options.CypherParallelRepeatHeuristicOption
import org.neo4j.cypher.internal.planner.spi.DatabaseMode
import org.neo4j.cypher.internal.runtime.ast.TraversalEndpoint
import org.neo4j.cypher.internal.runtime.ast.TraversalEndpoint.Endpoint.From
import org.neo4j.cypher.internal.runtime.ast.TraversalEndpoint.Endpoint.To
import org.neo4j.cypher.internal.util.UpperBound
import org.neo4j.cypher.internal.util.UpperBound.Limited
import org.neo4j.cypher.internal.util.UpperBound.Unlimited
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.collection.immutable.ListSet

class QuantifiedPathPatternBlockPlanningIntegrationTest extends QuantifiedPathPatternPlanningIntegrationTestBase
    with QuantifiedPathPatternBlockSpecificPlanningIntegrationTestBase {
  override protected def databaseFormat: DatabaseFormat = DatabaseFormat.Block
}

class QuantifiedPathPatternAlignedPlanningIntegrationTest extends QuantifiedPathPatternPlanningIntegrationTestBase
    with QuantifiedPathPatternAlignedSpecificPlanningIntegrationTestBase {
  override protected def databaseFormat: DatabaseFormat = DatabaseFormat.Aligned
}

trait QuantifiedPathPatternPlanningIntegrationTestBase extends CypherPlannerTestSuite
    with LogicalPlanningIntegrationTestSupport
    with AstConstructionTestSupport with LogicalPlanningAttributesTestSupport with CypherVersionTestSupport {

  protected def databaseFormat: DatabaseFormat

  override protected def plannerBuilder(): StatisticsBackedLogicalPlanningConfigurationBuilder =
    super.plannerBuilder().setDatabaseFormat(databaseFormat)

  private def disjoint(lhs: String, rhs: String, unnamedOffset: Int = 0): String =
    s"NONE(anon_$unnamedOffset IN $lhs WHERE anon_$unnamedOffset IN $rhs)"

  protected val plannerBase: StatisticsBackedLogicalPlanningConfigurationBuilder = plannerBuilder()
    .enablePlanningIntersectionScans()
    .setAllNodesCardinality(100)
    .setAllRelationshipsCardinality(40)
    .setLabelCardinality("User", 4)
    .setLabelCardinality("N", 6)
    .setLabelCardinality("NN", 5)
    .setLabelCardinality("NNN", 100)
    .setRelationshipCardinality("()-[:R]->()", 11)
    .setRelationshipCardinality("()-[:S]->()", 11)
    .setRelationshipCardinality("()-[:T]->()", 11)
    .setRelationshipCardinality("(:N)-[]->()", 10)
    .setRelationshipCardinality("(:N)-[]->(:N)", 10)
    .setRelationshipCardinality("(:N)-[]->(:NN)", 10)
    .setRelationshipCardinality("()-[]->(:N)", 10)
    .setRelationshipCardinality("(:NN)-[]->()", 10)
    .setRelationshipCardinality("()-[]->(:NN)", 10)
    .setRelationshipCardinality("(:NN)-[]->(:N)", 10)
    .setRelationshipCardinality("(:NN)-[]->(:NN)", 10)
    .setRelationshipCardinality("(:User)-[]->()", 10)
    .setRelationshipCardinality("(:User)-[]->(:N)", 10)
    .setRelationshipCardinality("(:User)-[]->(:NN)", 10)
    .setRelationshipCardinality("()-[]->(:User)", 10)
    .setRelationshipCardinality("(:User)-[:R]->()", 10)
    .setRelationshipCardinality("()-[:R]->(:User)", 10)
    .setRelationshipCardinality("(:User)-[]->(:User)", 10)
    .setRelationshipCardinality("(:User)-[:R]->(:User)", 10)
    .setRelationshipCardinality("()-[:R]->(:User)", 0)
    .setRelationshipCardinality("()-[:S]->(:User)", 0)
    .setRelationshipCardinality("(:User)-[:S]->()", 0)
    .setRelationshipCardinality("(:N)-[:R]->()", 10)
    .setRelationshipCardinality("()-[:R]->(:N)", 10)
    .setRelationshipCardinality("()-[]->(:NNN)", 10)
    .setRelationshipCardinality("(:N)-[]->(:NNN)", 10)
    .setRelationshipCardinality("()-[:R]->(:N)", 0)

  protected def planner: StatisticsBackedLogicalPlanningConfiguration = plannerBase
    .build()

  private val plannerForShardedDatabases = plannerBase
    .setDatabaseMode(DatabaseMode.SHARDED)
    .build()

  test("Should use correctly namespaced variables") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(10)
      .setAllRelationshipsCardinality(10)
      .enableDeduplicateNames(enable = false)
      .build()

    val query = "MATCH (a)((n)-[r]->())*(b) RETURN n, r"
    val plan = planner.plan(query).stripProduceResults
    val `(a)((n)-[r]-())*(b)` = TrailParameters(
      min = 0,
      max = Unlimited,
      start = "a",
      end = "b",
      innerStart = "  n@0",
      innerEnd = "  UNNAMED0",
      groupNodes = Set(("  n@0", "  n@2")),
      groupRelationships = Set(("  r@1", "  r@3")),
      innerRelationships = Set("  r@1"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = false,
      expansionMode = ExpandAll,
      accumulators = Set.empty
    )

    plan should equal(
      planner.subPlanBuilder()
        .repeatTrail(`(a)((n)-[r]-())*(b)`)
        .|.filter(isRepeatTrailUnique("  r@1"))
        .|.expand("(`  n@0`)-[`  r@1`]->(`  UNNAMED0`)")
        .|.argument("  n@0")
        .allNodeScan("a")
        .build()
    )
  }

  test("Should use correctly namespaced copied predicates") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(10)
      .setAllRelationshipsCardinality(10)
      .enableDeduplicateNames(enable = false)
      .build()
    val query = "MATCH (a) ((n)-[r]->(m) WHERE any(a IN n.list WHERE n.p > a))+ (b) RETURN *"
    val plan = planner.plan(query).stripProduceResults

    val `(a) ((n)-[r]->(m))+ (b)` = TrailParameters(
      min = 1,
      max = Unlimited,
      start = "  a@0",
      end = "b",
      innerStart = "  n@1",
      innerEnd = "  m@3",
      groupNodes = Set(("  n@1", "  n@5"), ("  m@3", "  m@7")),
      groupRelationships = Set(("  r@2", "  r@6")),
      innerRelationships = Set("  r@2"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = false,
      expansionMode = ExpandAll,
      accumulators = Set.empty
    )

    plan shouldEqual
      planner.subPlanBuilder()
        .repeatTrail(`(a) ((n)-[r]->(m))+ (b)`)
        .|.filter(isRepeatTrailUnique("  r@2"))
        .|.expandAll("(`  n@1`)-[`  r@2`]->(`  m@3`)")
        .|.filter("any(`  a@4` IN `  n@1`.list WHERE `  n@1`.p > `  a@4`)")
        .|.argument("  n@1")
        .filter("any(`  a@4` IN `  a@0`.list WHERE `  a@0`.p > `  a@4`)")
        .allNodeScan("`  a@0`")
        .build()
  }

  test("Should plan quantifier * with start node") {
    val query = "MATCH (u:User)((n)-[]->(m))* RETURN n, m"
    val plan = planner.plan(query).stripProduceResults
    val `(u)((n)-[]-(m))*` = TrailParameters(
      min = 0,
      max = Unlimited,
      start = "u",
      end = "anon_0",
      innerStart = "n",
      innerEnd = "m",
      groupNodes = Set(("n", "n"), ("m", "m")),
      groupRelationships = Set.empty,
      innerRelationships = Set("anon_1"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = false,
      expansionMode = ExpandAll,
      accumulators = Set.empty
    )

    plan should equal(
      planner.subPlanBuilder()
        .repeatTrail(`(u)((n)-[]-(m))*`)
        .|.filter(isRepeatTrailUnique("anon_1"))
        .|.expand("(n)-[anon_1]->(m)")
        .|.argument("n")
        .nodeByLabelScan("u", "User")
        .build()
    )
  }

  test("Should plan quantifier * with start node without label") {
    val query = "MATCH (u)((n:User)-[]->(m))* RETURN n, m"
    val plan = planner.plan(query).stripProduceResults
    val `(u)((n)-[]-(m))*` = TrailParameters(
      min = 0,
      max = Unlimited,
      start = "u",
      end = "anon_0",
      innerStart = "n",
      innerEnd = "m",
      groupNodes = Set(("n", "n"), ("m", "m")),
      groupRelationships = Set.empty,
      innerRelationships = Set("anon_1"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = false,
      expansionMode = ExpandAll,
      accumulators = Set.empty
    )

    plan should equal(
      planner.subPlanBuilder()
        .repeatTrail(`(u)((n)-[]-(m))*`)
        .|.filter(isRepeatTrailUnique("anon_1"))
        .|.expand("(n)-[anon_1]->(m)")
        .|.filter("n:User")
        .|.argument("n")
        .allNodeScan("u")
        .build()
    )
  }

  test("Should plan quantifier * with end node") {
    val query = "MATCH ((n)-[]->(m))*(u:User) RETURN n, m"

    val plan = planner.plan(query).stripProduceResults
    val `((n)-[]->(m))*(u)` = TrailParameters(
      min = 0,
      max = Unlimited,
      start = "u",
      end = "anon_0",
      innerStart = "m",
      innerEnd = "n",
      groupNodes = Set(("n", "n"), ("m", "m")),
      groupRelationships = Set.empty,
      innerRelationships = Set("anon_1"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = true,
      expansionMode = ExpandAll,
      accumulators = Set.empty
    )

    plan should equal(
      planner.subPlanBuilder()
        .repeatTrail(`((n)-[]->(m))*(u)`)
        .|.filter(isRepeatTrailUnique("anon_1"))
        .|.expand("(m)<-[anon_1]-(n)")
        .|.argument("m")
        .nodeByLabelScan("u", "User")
        .build()
    )
  }

  test("Should plan quantifier +") {
    val query = "MATCH ((n)-[]->(m))+ RETURN n, m"

    val plan = planner.plan(query).stripProduceResults
    val `((n)-[]->(m))+` = TrailParameters(
      min = 1,
      max = Unlimited,
      start = "anon_0",
      end = "anon_1",
      innerStart = "n",
      innerEnd = "m",
      groupNodes = Set(("n", "n"), ("m", "m")),
      groupRelationships = Set.empty,
      innerRelationships = Set("anon_2"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = false,
      expansionMode = ExpandAll,
      accumulators = Set.empty
    )

    plan should equal(
      planner.subPlanBuilder()
        .repeatTrail(`((n)-[]->(m))+`)
        .|.filter(isRepeatTrailUnique("anon_2"))
        .|.expand("(n)-[anon_2]->(m)")
        .|.argument("n")
        .allNodeScan("anon_0")
        .build()
    )
  }

  test("Should plan quantifier {1,}") {
    val query = "MATCH ((n)-[]->(m)){1,} RETURN n, m"

    val plan = planner.plan(query).stripProduceResults

    val `((n)-[]->(m)){1,}` = TrailParameters(
      min = 1,
      max = Unlimited,
      start = "anon_0",
      end = "anon_1",
      innerStart = "n",
      innerEnd = "m",
      groupNodes = Set(("n", "n"), ("m", "m")),
      groupRelationships = Set.empty,
      innerRelationships = Set("anon_2"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = false,
      expansionMode = ExpandAll,
      accumulators = Set.empty
    )
    plan should equal(
      planner.subPlanBuilder()
        .repeatTrail(`((n)-[]->(m)){1,}`)
        .|.filter(isRepeatTrailUnique("anon_2"))
        .|.expand("(n)-[anon_2]->(m)")
        .|.argument("n")
        .allNodeScan("anon_0")
        .build()
    )
  }

  test("Should plan quantifier {,5} with start node") {
    val query = "MATCH ()((n)-[]->(m)){,5} RETURN n, m"

    val plan = planner.plan(query).stripProduceResults
    val `()((n)-[]->(m)){,5}` = TrailParameters(
      min = 0,
      max = UpperBound.Limited(5),
      start = "anon_0",
      end = "anon_1",
      innerStart = "n",
      innerEnd = "m",
      groupNodes = Set(("n", "n"), ("m", "m")),
      groupRelationships = Set.empty,
      innerRelationships = Set("anon_2"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = false,
      expansionMode = ExpandAll,
      accumulators = Set.empty
    )

    plan should equal(
      planner.subPlanBuilder()
        .repeatTrail(`()((n)-[]->(m)){,5}`)
        .|.filter(isRepeatTrailUnique("anon_2"))
        .|.expand("(n)-[anon_2]->(m)")
        .|.argument("n")
        .allNodeScan("anon_0")
        .build()
    )
  }

  test("Should plan quantifier {1,5} with end node") {
    val query = "MATCH ((n)-[]->(m)){1,5} RETURN n, m"

    val plan = planner.plan(query).stripProduceResults
    val `((n)-[]->(m)){1,5}` = TrailParameters(
      min = 1,
      max = UpperBound.Limited(5),
      start = "anon_0",
      end = "anon_1",
      innerStart = "n",
      innerEnd = "m",
      groupNodes = Set(("n", "n"), ("m", "m")),
      groupRelationships = Set.empty,
      innerRelationships = Set("anon_2"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = false,
      expansionMode = ExpandAll,
      accumulators = Set.empty
    )

    plan should equal(
      planner.subPlanBuilder()
        .repeatTrail(`((n)-[]->(m)){1,5}`)
        .|.filter(isRepeatTrailUnique("anon_2"))
        .|.expand("(n)-[anon_2]->(m)")
        .|.argument("n")
        .allNodeScan("anon_0")
        .build()
    )
  }

  test(
    "Should plan mix of quantified and non-quantified path patterns, expanding the non-quantified relationship first"
  ) {
    val query = "MATCH ((n)-[]->(m))+ (a)--(b) RETURN n, m"

    val plan = planner.plan(query).stripProduceResults
    val `((n)-[]->(m))+(a)` = TrailParameters(
      min = 1,
      max = Unlimited,
      start = "a",
      end = "anon_0",
      innerStart = "m",
      innerEnd = "n",
      groupNodes = Set(("n", "n"), ("m", "m")),
      groupRelationships = Set.empty,
      innerRelationships = Set("anon_2"),
      previouslyBoundRelationships = Set("anon_1"),
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = true,
      expansionMode = ExpandAll,
      accumulators = Set.empty
    )

    plan should equal(
      planner.subPlanBuilder()
        .repeatTrail(`((n)-[]->(m))+(a)`)
        .|.filter(isRepeatTrailUnique("anon_2"))
        .|.expandAll("(m)<-[anon_2]-(n)")
        .|.argument("m")
        .allRelationshipsScan("(a)-[anon_1]-()")
        .build()
    )
  }

  test("Should plan mix of quantified and non-quantified path patterns, expanding the quantified one first") {
    val query = "MATCH (:User) ((n)-[]->(m))+ (a)--(b) RETURN n, m"

    val plan = planner.plan(query).stripProduceResults
    val `((n)-[]->(m))+(a)` = TrailParameters(
      min = 1,
      max = Unlimited,
      start = "anon_0",
      end = "a",
      innerStart = "n",
      innerEnd = "m",
      groupNodes = Set(("n", "n"), ("m", "m")),
      groupRelationships = Set(("anon_2", "anon_3")),
      innerRelationships = Set("anon_2"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = false,
      expansionMode = ExpandAll,
      accumulators = Set.empty
    )

    plan should equal(
      planner.subPlanBuilder()
        .filter("not anon_1 in anon_3")
        .expandAll("(a)-[anon_1]-()")
        .repeatTrail(`((n)-[]->(m))+(a)`)
        .|.filter(isRepeatTrailUnique("anon_2"))
        .|.expandAll("(n)-[anon_2]->(m)")
        .|.argument("n")
        .nodeByLabelScan("anon_0", "User")
        .build()
    )
  }

  test("Should plan mix of quantified path pattern and two non-quantified relationships") {
    val query = "MATCH ((n)-[r1]->(m)-[r2]->(o))+ (a)-[r3]-(b)<-[r4]-(c:N) RETURN n, m"

    val plan = planner.plan(query).stripProduceResults
    val `((n)-[r1]->(m)-[r2]->(o))+(a)` = TrailParameters(
      min = 1,
      max = Unlimited,
      start = "a",
      end = "anon_0",
      innerStart = "o",
      innerEnd = "n",
      groupNodes = Set(("n", "n"), ("m", "m")),
      groupRelationships = Set.empty,
      innerRelationships = Set("r1", "r2"),
      previouslyBoundRelationships = Set("r3", "r4"),
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = true,
      expansionMode = ExpandAll,
      accumulators = Set.empty
    )

    plan should equal(
      planner.subPlanBuilder()
        .repeatTrail(`((n)-[r1]->(m)-[r2]->(o))+(a)`)
        .|.filter("not r2 = r1", isRepeatTrailUnique("r1"))
        .|.expandAll("(m)<-[r1]-(n)")
        .|.filter(isRepeatTrailUnique("r2"))
        .|.expandAll("(o)<-[r2]-(m)")
        .|.argument("o")
        .filter("not r4 = r3")
        .expandAll("(b)-[r3]-(a)")
        .expandAll("(c)-[r4]->(b)")
        .nodeByLabelScan("c", "N")
        .build()
    )
  }

  test(
    "Should plan mix of quantified path pattern and two non-quantified relationships of which some are provably disjoint"
  ) {
    val query = "MATCH ((n)-[r1:R]->(m)-[r2]->(o))+ (a)-[r3:T]-(b)<-[r4]-(c:N) RETURN n, m"

    val plan = planner.plan(query).stripProduceResults
    val `((n)-[r1:R]->(m)-[r2]->(o))+(a)` = TrailParameters(
      min = 1,
      max = Unlimited,
      start = "a",
      end = "anon_0",
      innerStart = "o",
      innerEnd = "n",
      groupNodes = Set(("n", "n"), ("m", "m")),
      groupRelationships = Set.empty,
      innerRelationships = Set("r1", "r2"),
      previouslyBoundRelationships = Set("r3", "r4"),
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = true,
      expansionMode = ExpandAll,
      accumulators = Set.empty
    )

    plan should equal(
      planner.subPlanBuilder()
        .repeatTrail(`((n)-[r1:R]->(m)-[r2]->(o))+(a)`)
        .|.filter("not r2 = r1", isRepeatTrailUnique("r1"))
        .|.expandAll("(m)<-[r1:R]-(n)")
        .|.filter(isRepeatTrailUnique("r2"))
        .|.expandAll("(o)<-[r2]-(m)")
        .|.argument("o")
        .filter("not r4 = r3")
        .expandAll("(b)-[r3:T]-(a)")
        .expandAll("(c)-[r4]->(b)")
        .nodeByLabelScan("c", "N")
        .build()
    )
  }

  test("Should plan two consecutive quantified path patterns") {
    val query = "MATCH (u:User) ((n)-[]->(m))+ ((a)--(b))+ RETURN n, m"

    val plan = planner.plan(query).stripProduceResults
    val `(u) ((n)-[]->(m))+` = TrailParameters(
      min = 1,
      max = Unlimited,
      start = "u",
      end = "anon_0",
      innerStart = "n",
      innerEnd = "m",
      groupNodes = Set(("n", "n"), ("m", "m")),
      groupRelationships = Set(("anon_1", "anon_2")),
      innerRelationships = Set("anon_1"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = false,
      expansionMode = ExpandAll,
      accumulators = Set.empty
    )

    plan should equal(
      planner.subPlanBuilder()
        .filter(disjoint("anon_3", "anon_2", 4))
        .expand("(anon_0)-[anon_3*1..]-()")
        .repeatTrail(`(u) ((n)-[]->(m))+`)
        .|.filter(isRepeatTrailUnique("anon_1"))
        .|.expandAll("(n)-[anon_1]->(m)")
        .|.argument("n")
        .nodeByLabelScan("u", "User")
        .build()
    )
  }

  test("Should plan two consecutive quantified path patterns with a join") {
    val query = "MATCH (u:User&N) ((n)-[r]->(m))+ (x) ((a)-[r2]-(b))+ (v:User) USING JOIN ON x RETURN n, m"

    val plan = planner.plan(query).stripProduceResults
    val `(u) ((n)-[r]->(m))+ (x)` = TrailParameters(
      min = 1,
      max = Unlimited,
      start = "u",
      end = "x",
      innerStart = "n",
      innerEnd = "m",
      groupNodes = Set(("n", "n"), ("m", "m")),
      groupRelationships = Set(("r", "r")),
      innerRelationships = Set("r"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = false,
      expansionMode = ExpandAll,
      accumulators = Set.empty
    )

    plan should equal(
      planner.subPlanBuilder()
        .filter(disjoint("r", "r2"))
        .nodeHashJoin("x")
        .|.expand("(v)-[r2*1..]-(x)", projectedDir = INCOMING)
        .|.nodeByLabelScan("v", "User")
        .repeatTrail(`(u) ((n)-[r]->(m))+ (x)`)
        .|.filter(isRepeatTrailUnique("r"))
        .|.expandAll("(n)-[r]->(m)")
        .|.argument("n")
        .intersectionNodeByLabelsScan("u", Seq("N", "User"))
        .build()
    )
  }

  test("Should plan two consecutive quantified path patterns with different relationship types") {
    val query = "MATCH (u:User) ((n)-[:R]->(m))+ ((a)-[:T]-(b))+ RETURN n, m"

    val plan = planner.plan(query).stripProduceResults
    val `(u) ((n)-[]->(m))+` = TrailParameters(
      min = 1,
      max = Unlimited,
      start = "u",
      end = "anon_0",
      innerStart = "n",
      innerEnd = "m",
      groupNodes = Set(("n", "n"), ("m", "m")),
      groupRelationships = Set.empty,
      innerRelationships = Set("anon_1"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = false,
      expansionMode = ExpandAll,
      accumulators = Set.empty
    )

    plan should equal(
      planner.subPlanBuilder()
        .expand("(anon_0)-[:T*1..]-()")
        .repeatTrail(`(u) ((n)-[]->(m))+`)
        .|.filter(isRepeatTrailUnique("anon_1"))
        .|.expandAll("(n)-[anon_1:R]->(m)")
        .|.argument("n")
        .nodeByLabelScan("u", "User")
        .build()
    )
  }

  test("Should plan two quantified path pattern with some provably disjoint relationships") {
    val query = "MATCH ((n)-[r1:R]->(m)-[r2]->(o))+ ((a)-[r3:T]-(b)<-[r4]-(c))+ (:N) RETURN n, m"

    val `((n)-[r1:R]->(m)-[r2]->(o))+` = TrailParameters(
      min = 1,
      max = Unlimited,
      start = "anon_1",
      end = "anon_0",
      innerStart = "o",
      innerEnd = "n",
      groupNodes = Set(("n", "n"), ("m", "m")),
      groupRelationships = Set.empty,
      innerRelationships = Set("r1", "r2"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set("r3", "r4"),
      reverseGroupVariableProjections = true,
      expansionMode = ExpandAll,
      accumulators = Set.empty
    )
    val `((a)-[r3:T]-(b)<-[r4]-(c))+` = TrailParameters(
      min = 1,
      max = Unlimited,
      start = "anon_2",
      end = "anon_1",
      innerStart = "c",
      innerEnd = "a",
      groupNodes = Set.empty,
      groupRelationships = Set(("r3", "r3"), ("r4", "r4")),
      innerRelationships = Set("r4", "r3"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = true,
      expansionMode = ExpandAll,
      accumulators = Set.empty
    )

    val plan = planner.plan(query).stripProduceResults
    plan should equal(
      planner.subPlanBuilder()
        .repeatTrail(`((n)-[r1:R]->(m)-[r2]->(o))+`)
        .|.filter("not r2 = r1", isRepeatTrailUnique("r1"))
        .|.expandAll("(m)<-[r1:R]-(n)")
        .|.filter(isRepeatTrailUnique("r2"))
        .|.expandAll("(o)<-[r2]-(m)")
        .|.argument("o")
        .repeatTrail(`((a)-[r3:T]-(b)<-[r4]-(c))+`)
        .|.filter("not r4 = r3", isRepeatTrailUnique("r3"))
        .|.expandAll("(b)-[r3:T]-(a)")
        .|.filter(isRepeatTrailUnique("r4"))
        .|.expandAll("(c)-[r4]->(b)")
        .|.argument("c")
        .nodeByLabelScan("anon_2", "N")
        .build()
    )
  }

  test("Should plan two quantified path pattern with some provably disjoint relationships in planner query tail") {
    val query =
      "MATCH (x) WITH * SKIP 0 MATCH ((n)-[r1:R]->(m)-[r2]->(o))+ ((a)-[r3:T]-(b)<-[r4]-(c))+ (:N) RETURN n, m"

    val `((n)-[r1:R]->(m)-[r2]->(o))+` = TrailParameters(
      min = 1,
      max = Unlimited,
      start = "anon_1",
      end = "anon_0",
      innerStart = "o",
      innerEnd = "n",
      groupNodes = Set(("n", "n"), ("m", "m")),
      groupRelationships = Set.empty,
      innerRelationships = Set("r1", "r2"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set("r3", "r4"),
      reverseGroupVariableProjections = true,
      expansionMode = ExpandAll,
      accumulators = Set.empty
    )
    val `((a)-[r3:T]-(b)<-[r4]-(c))+` = TrailParameters(
      min = 1,
      max = Unlimited,
      start = "anon_2",
      end = "anon_1",
      innerStart = "c",
      innerEnd = "a",
      groupNodes = Set.empty,
      groupRelationships = Set(("r3", "r3"), ("r4", "r4")),
      innerRelationships = Set("r3", "r4"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = true,
      expansionMode = ExpandAll,
      accumulators = Set.empty
    )

    val plan = planner.plan(query).stripProduceResults
    plan should equal(
      planner.subPlanBuilder()
        .apply()
        .|.repeatTrail(`((n)-[r1:R]->(m)-[r2]->(o))+`)
        .|.|.filter("not r2 = r1", isRepeatTrailUnique("r1"))
        .|.|.expandAll("(m)<-[r1:R]-(n)")
        .|.|.filter(isRepeatTrailUnique("r2"))
        .|.|.expandAll("(o)<-[r2]-(m)")
        .|.|.argument("o")
        .|.repeatTrail(`((a)-[r3:T]-(b)<-[r4]-(c))+`)
        .|.|.filter("not r4 = r3", isRepeatTrailUnique("r3"))
        .|.|.expandAll("(b)-[r3:T]-(a)")
        .|.|.filter(isRepeatTrailUnique("r4"))
        .|.|.expandAll("(c)-[r4]->(b)")
        .|.|.argument("c")
        .|.nodeByLabelScan("anon_2", "N", "x")
        .skip(0)
        .allNodeScan("x")
        .build()
    )
  }

  test("Should plan two quantified path pattern with partial overlap in relationship types") {
    val query = "MATCH ((n)-[r1:R]->(m)-[r2:S]->(o))+ ((a)-[r3:T]-(b)<-[r4:R]-(c))+ (:N) RETURN n, m"

    val `((n)-[r1:R]->(m)-[r2]->(o))+` = TrailParameters(
      min = 1,
      max = Unlimited,
      start = "anon_1",
      end = "anon_0",
      innerStart = "o",
      innerEnd = "n",
      groupNodes = Set(("n", "n"), ("m", "m")),
      groupRelationships = Set.empty,
      innerRelationships = Set("r1", "r2"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set("r4"),
      reverseGroupVariableProjections = true,
      expansionMode = ExpandAll,
      accumulators = Set.empty
    )
    val `((a)-[r3:T]-(b)<-[r4]-(c))+` = TrailParameters(
      min = 1,
      max = Unlimited,
      start = "anon_2",
      end = "anon_1",
      innerStart = "c",
      innerEnd = "a",
      groupNodes = Set.empty,
      groupRelationships = Set(("r4", "r4")),
      innerRelationships = Set("r4", "r3"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = true,
      expansionMode = ExpandAll,
      accumulators = Set.empty
    )

    val plan = planner.plan(query).stripProduceResults
    plan should equal(
      planner.subPlanBuilder()
        .repeatTrail(`((n)-[r1:R]->(m)-[r2]->(o))+`)
        .|.filter(isRepeatTrailUnique("r1"))
        .|.expandAll("(m)<-[r1:R]-(n)")
        .|.filter(isRepeatTrailUnique("r2"))
        .|.expandAll("(o)<-[r2:S]-(m)")
        .|.argument("o")
        .repeatTrail(`((a)-[r3:T]-(b)<-[r4]-(c))+`)
        .|.filter(isRepeatTrailUnique("r3"))
        .|.expandAll("(b)-[r3:T]-(a)")
        .|.filter(isRepeatTrailUnique("r4"))
        .|.expandAll("(c)-[r4:R]->(b)")
        .|.argument("c")
        .nodeByLabelScan("anon_2", "N")
        .build()
    )
  }

  test("Should plan quantified path pattern with a WHERE clause") {
    val query = "MATCH ((n)-[]->(m) WHERE n.prop > m.prop)+ RETURN n, m"

    val plan = planner.plan(query).stripProduceResults
    val `((n)-[]->(m))+` = TrailParameters(
      min = 1,
      max = Unlimited,
      start = "anon_0",
      end = "anon_1",
      innerStart = "n",
      innerEnd = "m",
      groupNodes = Set(("n", "n"), ("m", "m")),
      groupRelationships = Set.empty,
      innerRelationships = Set("anon_2"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = false,
      expansionMode = ExpandAll,
      accumulators = Set.empty
    )

    plan should equal(
      planner.subPlanBuilder()
        .repeatTrail(`((n)-[]->(m))+`)
        .|.filter("n.prop > m.prop", isRepeatTrailUnique("anon_2"))
        .|.expandAll("(n)-[anon_2]->(m)")
        .|.argument("n")
        .allNodeScan("anon_0")
        .build()
    )
  }

  test("Should plan quantified path pattern with a WHERE clause with references to previous MATCH") {
    val query =
      "MATCH (a) WITH a.prop AS prop MATCH ((n)-[]->(m) WHERE n.prop > prop)+ RETURN n, m"

    val plan = planner.plan(query).stripProduceResults
    val `((n)-[]->(m))+` = TrailParameters(
      min = 1,
      max = Unlimited,
      start = "anon_0",
      end = "anon_1",
      innerStart = "n",
      innerEnd = "m",
      groupNodes = Set(("n", "n"), ("m", "m")),
      groupRelationships = Set.empty,
      innerRelationships = Set("anon_2"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = false,
      expansionMode = ExpandAll,
      accumulators = Set.empty
    )

    plan should equal(
      planner.subPlanBuilder()
        .apply()
        .|.repeatTrail(`((n)-[]->(m))+`)
        .|.|.filter(isRepeatTrailUnique("anon_2"))
        .|.|.expandAll("(n)-[anon_2]->(m)")
        .|.|.filter("n.prop > prop")
        .|.|.argument("n", "prop")
        .|.filter("anon_0.prop > prop")
        .|.allNodeScan("anon_0", "prop")
        .projection("a.prop AS prop")
        .allNodeScan("a")
        .build()
    )
  }

  test("Should plan quantified path pattern with a WHERE clause with multiple references to previous MATCH") {
    val query =
      """MATCH (a), (b)
        |CALL {
        |  WITH a, b
        |  MATCH (a) ((n)-[r]->(m) WHERE n.prop > a.prop AND n.prop > b.prop)+ (b)
        |  RETURN n, m
        |}
        |RETURN n, m""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    val `((n)-[r]->(m))+` = TrailParameters(
      min = 1,
      max = Unlimited,
      start = "b",
      end = "a",
      innerStart = "m",
      innerEnd = "n",
      groupNodes = Set(("n", "n"), ("m", "m")),
      groupRelationships = Set.empty,
      innerRelationships = Set("r"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = true,
      expansionMode = ExpandInto,
      accumulators = Set.empty
    )

    plan should equal(
      planner.subPlanBuilder()
        .repeatTrail(`((n)-[r]->(m))+`)
        .|.filter(
          andsReorderable("cacheNFromStore[n.prop] > cacheN[a.prop]", " cacheNFromStore[n.prop] > cacheN[b.prop]"),
          isRepeatTrailUnique("r")
        )
        .|.expandAll("(m)<-[r]-(n)")
        .|.argument("m", "a", "b")
        .filter("cacheN[a.prop] > cacheN[a.prop]", "cacheN[a.prop] > cacheN[b.prop]")
        .cartesianProduct()
        .|.cacheProperties("cacheNFromStore[b.prop]")
        .|.allNodeScan("b")
        .cacheProperties("cacheNFromStore[a.prop]")
        .allNodeScan("a")
        .build()
    )
  }

  test("Should plan quantified path pattern with a WHERE clause with anded properties") {
    val query =
      """MATCH (a:User) ((n)-[r]->(m) WHERE n.prop > m.prop AND n.prop < m.prop)+ (b)
        |RETURN n, m""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    val `((n)-[r]->(m))+` = TrailParameters(
      min = 1,
      max = Unlimited,
      start = "a",
      end = "b",
      innerStart = "n",
      innerEnd = "m",
      groupNodes = Set(("n", "n"), ("m", "m")),
      groupRelationships = Set.empty,
      innerRelationships = Set("r"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = false,
      expansionMode = ExpandAll,
      accumulators = Set.empty
    )

    plan should equal(
      planner.subPlanBuilder()
        .repeatTrail(`((n)-[r]->(m))+`)
        .|.filter(
          andsReorderable(
            "cacheNFromStore[n.prop] > cacheNFromStore[m.prop]",
            "cacheNFromStore[n.prop] < cacheNFromStore[m.prop]"
          ),
          isRepeatTrailUnique("r")
        )
        .|.expandAll("(n)-[r]->(m)")
        .|.argument("n")
        .nodeByLabelScan("a", "User")
        .build()
    )
  }

  test("Should plan quantified path pattern with inlined predicates") {
    val query = "MATCH (:User) ((n:N&NN {prop: 5} WHERE n.foo > 0)-[r:!REL WHERE r.prop > 0]->(m))+ RETURN n, m"

    val plan = planner.plan(query).stripProduceResults
    val `((n)-[r]->(m))+` = TrailParameters(
      min = 1,
      max = Unlimited,
      start = "anon_0",
      end = "anon_1",
      innerStart = "n",
      innerEnd = "m",
      groupNodes = Set(("n", "n"), ("m", "m")),
      groupRelationships = Set.empty,
      innerRelationships = Set("r"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = false,
      expansionMode = ExpandAll,
      accumulators = Set.empty
    )

    plan should equal(
      planner.subPlanBuilder()
        .repeatTrail(`((n)-[r]->(m))+`)
        .|.filter("not r:REL", "r.prop > 0", isRepeatTrailUnique("r"))
        .|.expandAll("(n)-[r]->(m)")
        .|.filter("n:N", "n:NN", "n.prop = 5", "n.foo > 0")
        .|.argument("n")
        .filter("anon_0.prop = 5", "anon_0.foo > 0", "anon_0:NN", "anon_0:N")
        .nodeByLabelScan("anon_0", "User")
        .build()
    )
  }

  test("Should plan quantified path pattern with extracted predicate with local dependencies") {
    val query = "MATCH (a) ((n)-[r]->(m))+ (b) WHERE all(i IN r WHERE i.p = 0) RETURN *"
    val plan = planner.plan(query).stripProduceResults
    val `(a) ((n)-[r]-(m))+ (b)` = TrailParameters(
      min = 1,
      max = Unlimited,
      start = "a",
      end = "b",
      innerStart = "n",
      innerEnd = "m",
      groupNodes = Set(("n", "n"), ("m", "m")),
      groupRelationships = Set(("r", "r")),
      innerRelationships = Set("r"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = false,
      expansionMode = ExpandAll,
      accumulators = Set.empty
    )

    plan shouldEqual planner.subPlanBuilder()
      .repeatTrail(`(a) ((n)-[r]-(m))+ (b)`)
      .|.filter("r.p = 0", isRepeatTrailUnique("r"))
      .|.expandAll("(n)-[r]->(m)")
      .|.argument("n")
      .allNodeScan("a")
      .build()
  }

  test("Should plan quantified path pattern with extracted predicate with bound non-local dependency") {
    val query = "MATCH (z)-[rr]->(a) ((n)-[r]->(m))+ (b) WHERE all(i IN r WHERE i.p = z.p) RETURN *"
    val plan = planner.plan(query).stripProduceResults
    val `(a) ((n)-[r]-(m))+ (b)` = TrailParameters(
      min = 1,
      max = Unlimited,
      start = "a",
      end = "b",
      innerStart = "n",
      innerEnd = "m",
      groupNodes = Set(("n", "n"), ("m", "m")),
      groupRelationships = Set(("r", "r")),
      innerRelationships = Set("r"),
      previouslyBoundRelationships = Set("rr"),
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = false,
      expansionMode = ExpandAll,
      accumulators = Set.empty
    )

    plan shouldEqual planner.subPlanBuilder()
      .repeatTrail(`(a) ((n)-[r]-(m))+ (b)`)
      .|.filter("r.p = z.p", isRepeatTrailUnique("r"))
      .|.expandAll("(n)-[r]->(m)")
      .|.argument("n", "z")
      .allRelationshipsScan("(z)-[rr]->(a)")
      .build()
  }

  test(
    "Should plan quantified path pattern with extracted predicate with bound non-local dependency from previous horizon"
  ) {
    val query = """
                  |WITH 1 AS x, 1 AS y
                  |SKIP 0
                  |MATCH (a) ((n)-[r]->(m))+ (b)
                  |WHERE all(i IN r WHERE i.p = x)
                  |RETURN *""".stripMargin
    val plan = planner.plan(query).stripProduceResults
    val `(a) ((n)-[r]-(m))+ (b)` = TrailParameters(
      min = 1,
      max = Unlimited,
      start = "a",
      end = "b",
      innerStart = "n",
      innerEnd = "m",
      groupNodes = Set(("n", "n"), ("m", "m")),
      groupRelationships = Set(("r", "r")),
      innerRelationships = Set("r"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = false,
      expansionMode = ExpandAll,
      accumulators = Set.empty
    )

    plan shouldEqual planner.subPlanBuilder()
      .apply()
      .|.repeatTrail(`(a) ((n)-[r]-(m))+ (b)`)
      .|.|.filter("r.p = x", isRepeatTrailUnique("r"))
      .|.|.expandAll("(n)-[r]->(m)")
      .|.|.argument("n", "x")
      .|.allNodeScan("a", "x", "y")
      .projection("1 AS x", "1 AS y")
      .skip(0)
      .argument()
      .build()
  }

  test("Should plan quantified path pattern without extracted predicate with unbound non-local dependency") {
    val query = "MATCH (a) ((n)-[r]->(m))+ (b: N) WHERE all(i IN r WHERE i.p = a.p) RETURN *"
    val plan = planner.plan(query).stripProduceResults
    val `(a) ((n)-[r]-(m))+ (b)` = TrailParameters(
      min = 1,
      max = Unlimited,
      start = "b",
      end = "a",
      innerStart = "m",
      innerEnd = "n",
      groupNodes = Set(("n", "n"), ("m", "m")),
      groupRelationships = Set(("r", "r")),
      innerRelationships = Set("r"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = true,
      expansionMode = ExpandAll,
      accumulators = Set.empty
    )
    val predicate = allInList(
      v"i",
      v"r",
      equals(prop(v"i", "p"), prop(v"a", "p"))
    )

    plan shouldEqual planner.subPlanBuilder()
      .filter(predicate)
      .repeatTrail(`(a) ((n)-[r]-(m))+ (b)`)
      .|.filter(isRepeatTrailUnique("r"))
      .|.expandAll("(m)<-[r]-(n)")
      .|.argument("m")
      .nodeByLabelScan("b", "N")
      .build()
  }

  test("Should plan quantified path pattern if both start and end are already bound") {
    val query =
      s"""
         |MATCH (n), (m)
         |WITH * SKIP 1
         |MATCH (n)((n_inner)-[r_inner]->(m_inner))+ (m)
         |RETURN *
         |""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    val `(n)((n_inner)-[r_inner]->(m_inner))+ (m)` = TrailParameters(
      min = 1,
      max = Unlimited,
      start = "n",
      end = "m",
      innerStart = "n_inner",
      innerEnd = "m_inner",
      groupNodes = Set(("n_inner", "n_inner"), ("m_inner", "m_inner")),
      groupRelationships = Set(("r_inner", "r_inner")),
      innerRelationships = Set("r_inner"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = false,
      expansionMode = ExpandInto,
      accumulators = Set.empty
    )

    plan should equal(
      planner.subPlanBuilder()
        .repeatTrail(`(n)((n_inner)-[r_inner]->(m_inner))+ (m)`)
        .|.filter(isRepeatTrailUnique("r_inner"))
        .|.expandAll("(n_inner)-[r_inner]->(m_inner)")
        .|.argument("n_inner")
        .skip(1)
        .cartesianProduct()
        .|.allNodeScan("m")
        .allNodeScan("n")
        .build()
    )
  }

  test("Should not plan assertNode with quantified path pattern in OPTIONAL MATCH") {
    val array = (1 to 10).mkString("[", ",", "]")

    val query =
      s"""
         |MATCH (n1)
         |UNWIND $array AS a0
         |OPTIONAL MATCH (n1) ((inner1)-[:R]->(inner2))+ (n2)
         |WITH a0
         |RETURN a0 ORDER BY a0
         |""".stripMargin

    // Plan for the OPTIONAL MATCH should not contain a .filter(assertIsNode("n1")),
    // since n1 is not a single node pattern, but a concatenated path.
    val plan = planner.plan(query).stripProduceResults
    plan.folder.treeFindByClass[AssertIsNode] should be(None)
  }

  test("Should plan OUTGOING QPP in requested direction") {
    val query =
      s"""
         |MATCH (n)-[r]->+(m)
         |RETURN n, m
         |""".stripMargin

    val plan = planner.plan(query).stripProduceResults

    plan shouldBe planner.subPlanBuilder()
      .expandAll("(n)-[*1..]->(m)")
      .allNodeScan("n")
      .build()
  }

  test("Should plan OUTGOING QPP in reverse direction") {
    val query =
      s"""
         |MATCH (n:N)-[r]->+(nn:NN)
         |RETURN *
         |""".stripMargin

    val plan = planner.plan(query).stripProduceResults

    plan shouldBe planner.subPlanBuilder()
      .filter("n:N")
      .expandAll("(nn)<-[r*1..]-(n)")
      .nodeByLabelScan("nn", "NN")
      .build()
  }

  test("Should plan INCOMING QPP in requested direction") {
    val query =
      s"""
         |MATCH (n)<-[r]-+(m)
         |RETURN n, m
         |""".stripMargin

    val plan = planner.plan(query).stripProduceResults

    plan shouldBe planner.subPlanBuilder()
      .expand("(n)<-[*1..]-(m)", expandMode = ExpandAll, projectedDir = SemanticDirection.INCOMING)
      .allNodeScan("n")
      .build()
  }

  test("Should plan INCOMING QPP in reverse direction") {
    val query =
      s"""
         |MATCH (n:N)<-[r]-+(nn:NN)
         |RETURN *
         |""".stripMargin

    val plan = planner.plan(query).stripProduceResults

    plan shouldBe planner.subPlanBuilder()
      .filter("n:N")
      .expand("(nn)-[r*1..]->(n)", expandMode = ExpandAll, projectedDir = SemanticDirection.INCOMING)
      .nodeByLabelScan("nn", "NN")
      .build()
  }

  test("Should plan BOTH QPP in requested direction") {
    val query =
      s"""
         |MATCH (n)-[r]-+(m)
         |RETURN n, m
         |""".stripMargin

    val plan = planner.plan(query).stripProduceResults

    plan shouldBe planner.subPlanBuilder()
      .expandAll("(n)-[*1..]-(m)")
      .allNodeScan("n")
      .build()
  }

  test("Should plan BOTH QPP in reverse direction") {
    val query =
      s"""
         |MATCH (n)-[r]-+(nn:NN)
         |RETURN n, nn
         |""".stripMargin

    val plan = planner.plan(query).stripProduceResults

    plan shouldBe planner.subPlanBuilder()
      .expand("(nn)-[*1..]-(n)", expandMode = ExpandAll, projectedDir = SemanticDirection.INCOMING)
      .nodeByLabelScan("nn", "NN")
      .build()
  }

  test("Should plan quantified relationship with predicates") {
    val query =
      s"""
         |MATCH (n)-[r:R WHERE properties(r).prop > 123]->{2,5}(m)
         |RETURN n, m
         |""".stripMargin
    val plan = planner.plan(query).stripProduceResults

    plan shouldBe planner.subPlanBuilder()
      .expand("(n)-[:R*2..5]->(m)", relationshipPredicates = Seq(Predicate("anon_0", "properties(anon_0).prop > 123")))
      .allNodeScan("n")
      .build()
  }

  test("Should plan an index seek with multiple predicates on RHS of Trail") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setAllRelationshipsCardinality(5000)
      .setLabelCardinality("A", 10)
      .setRelationshipCardinality("()-[:REL]->()", 5000)
      .setRelationshipCardinality("(:A)-[:REL]->()", 10)
      .setRelationshipCardinality("()-[:REL]->(:A)", 10)
      .addNodeIndex("A", Seq("prop"), 0.5, 0.5)
      .build()

    val q = "MATCH (n) ((x)<-[r1:REL]-(a:A WHERE a.prop > 100 AND a.prop < 123)-[r2:REL]->(y))+ (m) RETURN *"

    val plan = planner.plan(q).stripProduceResults
    val params = TrailParameters(
      min = 1,
      max = Unlimited,
      start = "n",
      end = "m",
      innerStart = "x",
      innerEnd = "y",
      groupNodes = Set(("y", "y"), ("x", "x"), ("a", "a")),
      groupRelationships = Set(("r2", "r2"), ("r1", "r1")),
      innerRelationships = Set("r1", "r2"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = false,
      expansionMode = ExpandAll,
      accumulators = Set.empty
    )

    plan shouldBe planner.subPlanBuilder()
      .repeatTrail(params)
      .|.filter("not r2 = r1", isRepeatTrailUnique("r2"))
      .|.expandAll("(a)-[r2:REL]->(y)")
      .|.filter(isRepeatTrailUnique("r1"))
      .|.expandInto("(x)<-[r1:REL]-(a)")
      .|.nodeIndexOperator("a:A(100 < prop < 123)", argumentIds = Set("x"))
      .allNodeScan("n")
      .build()
  }

  test("Should plan an index seek for predicates on QPP start node") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setAllRelationshipsCardinality(5000)
      .setLabelCardinality("A", 100)
      .setRelationshipCardinality("()-[]->(:A)", 500)
      .setRelationshipCardinality("(:A)-[]->(:A)", 500)
      .addNodeIndex("A", Seq("prop"), 0.5, 0.5)
      .build()

    val q = "MATCH ((a:A WHERE a.prop > 0)<--())+ RETURN *"

    val plan = planner.plan(q).stripProduceResults
    val params = TrailParameters(
      min = 1,
      max = Unlimited,
      start = "anon_0",
      end = "anon_1",
      innerStart = "a",
      innerEnd = "anon_3",
      groupNodes = Set(("a", "a")),
      groupRelationships = Set.empty,
      innerRelationships = Set("anon_2"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = false,
      expansionMode = ExpandAll,
      accumulators = Set.empty
    )

    plan shouldBe planner.subPlanBuilder()
      .repeatTrail(params)
      .|.filter(isRepeatTrailUnique("anon_2"))
      .|.expandAll("(a)<-[anon_2]-(anon_3)")
      .|.filter("a:A", "a.prop > 0")
      .|.argument("a")
      .nodeIndexOperator("anon_0:A(prop > 0)")
      .build()
  }

  test("Should plan an index seek for predicates on QPP end node") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setAllRelationshipsCardinality(5000)
      .setLabelCardinality("A", 100)
      .setRelationshipCardinality("(:A)-[]->()", 500)
      .setRelationshipCardinality("(:A)-[]->(:A)", 500)
      .addNodeIndex("A", Seq("prop"), 0.5, 0.5)
      .build()

    val q = "MATCH (()<--(a:A WHERE a.prop > 0))+ RETURN *"

    val plan = planner.plan(q).stripProduceResults
    val params = TrailParameters(
      min = 1,
      max = Unlimited,
      start = "anon_1",
      end = "anon_0",
      innerStart = "a",
      innerEnd = "anon_2",
      groupNodes = Set(("a", "a")),
      groupRelationships = Set.empty,
      innerRelationships = Set("anon_3"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = true,
      expansionMode = ExpandAll,
      accumulators = Set.empty
    )

    plan shouldBe planner.subPlanBuilder()
      .repeatTrail(params)
      .|.filter(isRepeatTrailUnique("anon_3"))
      .|.expandAll("(a)-[anon_3]->(anon_2)")
      .|.filter("a:A", "a.prop > 0")
      .|.argument("a")
      .nodeIndexOperator("anon_1:A(prop > 0)")
      .build()
  }

  test("Should start with higher cardinality label if first expansion is cheaper") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(160)
      .setAllRelationshipsCardinality(1000)
      .setLabelCardinality("B", 100)
      .setLabelCardinality("C", 10)
      .setRelationshipCardinality("(:B)-[:R]->(:C)", 2)
      .setRelationshipCardinality("(:B)-[:R]->()", 2)
      .setRelationshipCardinality("()-[:R]->(:C)", 500)
      .setRelationshipCardinality("(:C)-[:R]->()", 500)
      .setRelationshipCardinality("()-[:R]->()", 501)
      .setRelationshipCardinality("()-[:R]->(:B)", 0)
      .setRelationshipCardinality("(:C)-[:R]->(:C)", 498)
      .build()

    val query = "MATCH (b:B) ( (x)-[r:R]->(y) ){3} (c:C) RETURN *"

    val plan = planner.plan(query).stripProduceResults

    val trailParameters = TrailParameters(
      min = 3,
      max = UpperBound.Limited(3),
      start = "b",
      end = "c",
      innerStart = "x",
      innerEnd = "y",
      groupNodes = Set(("x", "x"), ("y", "y")),
      groupRelationships = Set(("r", "r")),
      innerRelationships = Set("r"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = false,
      expansionMode = ExpandAll,
      accumulators = Set.empty
    )

    plan shouldBe planner.subPlanBuilder()
      .filter("c:C")
      .repeatTrail(trailParameters)
      .|.filter(isRepeatTrailUnique("r"))
      .|.expandAll("(x)-[r:R]->(y)")
      .|.argument("x")
      .nodeByLabelScan("b", "B")
      .build()
  }

  test("Should go into trail with lower cardinality first") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setAllRelationshipsCardinality(10000)
      .setLabelCardinality("X", 40)
      .setLabelCardinality("A", 500)
      .setLabelCardinality("B", 800)
      .setLabelCardinality("N", 500)
      .setLabelCardinality("M", 500)
      .setLabelCardinality("P", 20)
      .setLabelCardinality("Q", 30)
      .setRelationshipCardinality("()-[:REL]->()", 10000)
      .setRelationshipCardinality("(:N)-[:REL]->()", 500)
      .setRelationshipCardinality("()-[:REL]->(:M)", 500)
      .setRelationshipCardinality("(:M)-[:REL]->()", 500)
      .setRelationshipCardinality("()-[:REL]->(:N)", 500)
      .setRelationshipCardinality("(:A)-[:REL]->(:M)", 500)
      .setRelationshipCardinality("(:A)-[:REL]->(:N)", 500)
      .setRelationshipCardinality("(:N)-[:REL]->(:M)", 500)
      .setRelationshipCardinality("(:N)-[:REL]->(:N)", 500)
      .setRelationshipCardinality("(:M)-[:REL]->(:M)", 500)
      .setRelationshipCardinality("(:M)-[:REL]->(:N)", 500)
      .setRelationshipCardinality("(:N)-[:REL]->(:X)", 500)
      .setRelationshipCardinality("(:M)-[:REL]->(:X)", 500)
      .setRelationshipCardinality("(:N)-[:REL]->(:P)", 500)
      .setRelationshipCardinality("(:M)-[:REL]->(:P)", 500)
      .setRelationshipCardinality("(:P)-[:REL]->()", 10)
      .setRelationshipCardinality("()-[:REL]->(:Q)", 10)
      .setRelationshipCardinality("(:P)-[:REL]->(:Q)", 10)
      .setRelationshipCardinality("(:P)-[:REL]->(:P)", 10)
      .setRelationshipCardinality("(:P)-[:REL]->(:B)", 10)
      .setRelationshipCardinality("(:Q)-[:REL]->(:Q)", 10)
      .setRelationshipCardinality("(:Q)-[:REL]->(:P)", 10)
      .setRelationshipCardinality("(:Q)-[:REL]->(:B)", 10)
      .setRelationshipCardinality("(:X)-[:REL]->(:Q)", 10)
      .setRelationshipCardinality("(:X)-[:REL]->(:P)", 10)
      .setRelationshipCardinality("(:M)-[:REL]->(:Q)", 10)
      .setRelationshipCardinality("(:M)-[:REL]->()", 2010)
      .build()

    val query =
      """
        |MATCH (a:A) ((n:N)-[r1:REL]->(m:M)){3} (x:X) ((p:P)-[r2:REL]->(q:Q)){3} (b:B)
        |RETURN *""".stripMargin

    val plan = planner.plan(query).stripProduceResults

    val n_r1_m_trail = TrailParameters(
      min = 3,
      max = UpperBound.Limited(3),
      start = "x",
      end = "a",
      innerStart = "m",
      innerEnd = "n",
      groupNodes = Set(("n", "n"), ("m", "m")),
      groupRelationships = Set(("r1", "r1")),
      innerRelationships = Set("r1"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set("r2"),
      reverseGroupVariableProjections = true,
      expansionMode = ExpandAll,
      accumulators = Set.empty
    )

    val p_r2_q_trail = TrailParameters(
      min = 3,
      max = UpperBound.Limited(3),
      start = "x",
      end = "b",
      innerStart = "p",
      innerEnd = "q",
      groupNodes = Set(("p", "p"), ("q", "q")),
      groupRelationships = Set(("r2", "r2")),
      innerRelationships = Set("r2"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = false,
      expansionMode = ExpandAll,
      accumulators = Set.empty
    )

    plan shouldBe planner.subPlanBuilder()
      .filter("a:A", "a:N")
      .repeatTrail(n_r1_m_trail)
      .|.filter("n:N", isRepeatTrailUnique("r1"))
      .|.expandAll("(m)<-[r1:REL]-(n)")
      .|.filter("m:M")
      .|.argument("m")
      .filter("b:B", "b:Q")
      .repeatTrail(p_r2_q_trail)
      .|.filter("q:Q", isRepeatTrailUnique("r2"))
      .|.expandAll("(p)-[r2:REL]->(q)")
      .|.filter("p:P")
      .|.argument("p")
      .intersectionNodeByLabelsScan("x", Seq("M", "P", "X"))
      .build()
  }

  test("Should insert eager between quantified relationship and the creation of an overlapping relationship") {
    val query = "MATCH (a)(()-[r]->()){1,5}(b) CREATE (a)-[r2:T]->(b) RETURN *"
    val plan = planner.plan(query).stripProduceResults

    plan shouldEqual planner.subPlanBuilder()
      .create(createRelationship("r2", "a", "T", "b", SemanticDirection.OUTGOING))
      .eager(ListSet(TypeReadSetConflict(relTypeName("T")).withConflict(Conflict(Id(1), Id(3)))))
      .expandAll("(a)-[r*1..5]->(b)")
      .allNodeScan("a")
      .build()
  }

  test(
    "Should not insert eager between QPP and CREATE of single node: QPPs internal nodes are connected and can therefore not overlap."
  ) {
    val query = "MATCH (a:N)(()-[r]->())*(b:NNN {prop: 42}) MERGE (c:N {prop: 123})"
    val plan = planner.plan(query).stripProduceResults

    plan shouldEqual planner.subPlanBuilder()
      .emptyResult()
      .apply()
      .|.merge(List(createNodeWithProperties("c", List("N"), "{prop: 123}")), Nil, Nil, Nil, Set.empty)
      .|.filter("c.prop = 123")
      .|.nodeByLabelScan("c", "N")
      .filter("b.prop = 42", "b:NNN")
      .expandAll("(a)-[*0..]->(b)")
      .nodeByLabelScan("a", "N")
      .build()
  }

  test(
    "Should not, but does insert an unnecessary eager based solely on the predicates contained within the quantified path pattern"
  ) {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(40)
      .setLabelCardinality("A", 5)
      .setLabelCardinality("B", 50)
      .setRelationshipCardinality("()-[]->(:B)", 10)
      .setRelationshipCardinality("(:A)-[]->(:B)", 10)
      .setRelationshipCardinality("(:B)-[]->(:B)", 10)
      .build()

    // This first MATCH expands to: (:A) | (:A)-->(:B) | (:A)-->(:B)-->(:B) | etc – all nodes have at least one label
    // This ideally should not produce an eager since only a node with no labels is created.
    val query = "MATCH (start:A)((a)-[r]->(b:B))*(end) CREATE (x)"
    val plan = planner.plan(query).stripProduceResults
    val expectedRelPredicate =
      VariablePredicate(v"anon_0", HasLabels(EndNode(v"anon_0")(pos), Seq(LabelName("B")(pos)))(pos))

    val expected = planner.subPlanBuilder()
      .emptyResult()
      .create(createNode("x"))
      .eager(ListSet(ReadCreateConflict.withConflict(Conflict(Id(2), Id(4))))) // Unnecessary eager
      .expandExpr(
        "(start)-[*0..]->()",
        expandMode = ExpandAll,
        projectedDir = OUTGOING,
        nodePredicates = Seq(),
        relationshipPredicates = Seq(expectedRelPredicate)
      )
      .nodeByLabelScan("start", "A", IndexOrderNone)
      .build()

    plan shouldEqual expected
  }

  test(
    "shouldn't insert eager when a quantified path pattern and a created node don't overlap"
  ) {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(40)
      .setLabelCardinality("A", 5)
      .setLabelCardinality("B", 5)
      .setRelationshipCardinality("(:A)-[]->()", 10)
      .setRelationshipCardinality("()-[]->(:B)", 10)
      .setRelationshipCardinality("(:A)-[]->(:A)", 10)
      .setRelationshipCardinality("(:A)-[]->(:B)", 10)
      .setRelationshipCardinality("(:B)-[]->(:A)", 10)
      .setRelationshipCardinality("(:B)-[]->(:B)", 10)
      .build()

    val query = "MATCH (start:A)((a:A)-[r]->(b:B))*(end:B) CREATE (c:C) RETURN *"
    val plan = planner.plan(query).stripProduceResults

    val `(start)((a)-[r]->(b))*(end)` =
      TrailParameters(
        min = 0,
        max = UpperBound.Unlimited,
        start = "start",
        end = "end",
        innerStart = "a",
        innerEnd = "b",
        groupNodes = Set(("a", "a"), ("b", "b")),
        groupRelationships = Set(("r", "r")),
        innerRelationships = Set("r"),
        previouslyBoundRelationships = Set.empty,
        previouslyBoundRelationshipGroups = Set.empty,
        reverseGroupVariableProjections = false,
        expansionMode = ExpandAll,
        accumulators = Set.empty
      )

    plan shouldEqual planner.subPlanBuilder()
      .create(createNode("c", "C"))
      .filter("end:B")
      .repeatTrail(`(start)((a)-[r]->(b))*(end)`)
      .|.filter("b:B", isRepeatTrailUnique("r"))
      .|.expandAll("(a)-[r]->(b)")
      .|.filter("a:A")
      .|.argument("a")
      .nodeByLabelScan("start", "A")
      .build()
  }

  test(
    "Should insert eager when a quantified path pattern and a deleted node overlap"
  ) {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(40)
      .setLabelCardinality("A", 5)
      .setLabelCardinality("B", 5)
      .setLabelCardinality("C", 5)
      .setRelationshipCardinality("(:A)-[]->()", 10)
      .setRelationshipCardinality("(:B)-[]->()", 10)
      .setRelationshipCardinality("()-[]->(:B)", 10)
      .setRelationshipCardinality("(:A)-[]->(:A)", 10)
      .setRelationshipCardinality("(:A)-[]->(:B)", 10)
      .setRelationshipCardinality("(:B)-[]->(:A)", 10)
      .setRelationshipCardinality("(:B)-[]->(:B)", 10)
      .build()

    val query = "MATCH (x:C) OPTIONAL MATCH (start:A)((a:A)-[r1]->(b:B)-[r2]->(c:B))*(end:B) DELETE x"
    val plan = planner.plan(query).stripProduceResults

    val `(start)((a)-[r]->(b))*(end)` =
      TrailParameters(
        min = 0,
        max = UpperBound.Unlimited,
        start = "start",
        end = "end",
        innerStart = "a",
        innerEnd = "c",
        groupNodes = Set.empty,
        groupRelationships = Set.empty,
        innerRelationships = Set("r1", "r2"),
        previouslyBoundRelationships = Set.empty,
        previouslyBoundRelationshipGroups = Set.empty,
        reverseGroupVariableProjections = false,
        expansionMode = ExpandAll,
        accumulators = Set.empty
      )

    plan shouldEqual planner.subPlanBuilder()
      .emptyResult()
      .deleteNode("x")
      .eager(ListSet(
        ReadDeleteConflict("c").withConflict(Conflict(Id(2), Id(7))),
        ReadDeleteConflict("a").withConflict(Conflict(Id(2), Id(12))),
        ReadDeleteConflict("start").withConflict(Conflict(Id(2), Id(7))),
        ReadDeleteConflict("a").withConflict(Conflict(Id(2), Id(7))),
        ReadDeleteConflict("end").withConflict(Conflict(Id(2), Id(7))),
        ReadDeleteConflict("c").withConflict(Conflict(Id(2), Id(8))),
        ReadDeleteConflict("c").withConflict(Conflict(Id(2), Id(9))),
        ReadDeleteConflict("b").withConflict(Conflict(Id(2), Id(10))),
        ReadDeleteConflict("end").withConflict(Conflict(Id(2), Id(6))),
        ReadDeleteConflict("start").withConflict(Conflict(Id(2), Id(14))),
        ReadDeleteConflict("a").withConflict(Conflict(Id(2), Id(11))),
        ReadDeleteConflict("b").withConflict(Conflict(Id(2), Id(9))),
        ReadDeleteConflict("b").withConflict(Conflict(Id(2), Id(11)))
      ))
      .apply()
      .|.optional("x")
      .|.filter("end:B")
      .|.repeatTrail(`(start)((a)-[r]->(b))*(end)`)
      .|.|.filter("NOT r2 = r1", isRepeatTrailUnique("r2"), "c:B")
      .|.|.expandAll("(b)-[r2]->(c)")
      .|.|.filter(isRepeatTrailUnique("r1"), "b:B")
      .|.|.expandAll("(a)-[r1]->(b)")
      .|.|.filter("a:A")
      .|.|.argument("a", "x")
      .|.nodeByLabelScan("start", "A", "x")
      .nodeByLabelScan("x", "C")
      .build()
  }

  test(
    "Should plan VarExpand instead of Trail on quantified path pattern with single relationship and no inner node variables - kleene star"
  ) {
    val query = "MATCH (a)(()-[r]->())*(b) RETURN *"
    val plan = planner.plan(query).stripProduceResults

    plan shouldEqual planner.subPlanBuilder()
      .expandAll("(a)-[r*0..]->(b)")
      .allNodeScan("a")
      .build()
  }

  test(
    "Should plan VarExpand instead of Trail on quantified path pattern with relationship and no inner node variables - kleene plus"
  ) {
    val query = "MATCH (a)(()-[r]->())+(b) RETURN *"
    val plan = planner.plan(query).stripProduceResults

    plan shouldEqual planner.subPlanBuilder()
      .expandAll("(a)-[r*1..]->(b)")
      .allNodeScan("a")
      .build()
  }

  test(
    "Should plan VarExpand instead of Trail on quantified path pattern with relationship and no inner node variables - lower bound"
  ) {
    val query = "MATCH (a)(()-[r]->()){2,}(b) RETURN *"
    val plan = planner.plan(query).stripProduceResults

    plan shouldEqual planner.subPlanBuilder()
      .expandAll("(a)-[r*2..]->(b)")
      .allNodeScan("a")
      .build()
  }

  test(
    "Should plan VarExpand instead of Trail on quantified path pattern with relationship and no inner node variables - upper bound"
  ) {
    val query = "MATCH (a)(()-[r]->()){,2}(b) RETURN *"
    val plan = planner.plan(query).stripProduceResults

    plan shouldEqual planner.subPlanBuilder()
      .expandAll("(a)-[r*0..2]->(b)")
      .allNodeScan("a")
      .build()
  }

  test(
    "Should plan VarExpand instead of Trail on quantified path pattern with relationship and no inner node variables - lower and upper bound"
  ) {
    val query = "MATCH (a)(()-[r]->()){2,3}(b) RETURN *"
    val plan = planner.plan(query).stripProduceResults

    plan shouldEqual planner.subPlanBuilder()
      .expandAll("(a)-[r*2..3]->(b)")
      .allNodeScan("a")
      .build()
  }

  test(
    "Should not plan VarExpand instead of Trail on quantified path patterns with relationship and a inner node variable - left inner variable"
  ) {
    val query = "MATCH (a)((b)-[r]->()){2,3}(c) RETURN *"
    val plan = planner.plan(query).stripProduceResults
    val `(a)((b)-[r]->()){2,3}(c)` =
      TrailParameters(
        min = 2,
        max = UpperBound.Limited(3),
        start = "a",
        end = "c",
        innerStart = "b",
        innerEnd = "anon_0",
        groupNodes = Set(("b", "b")),
        groupRelationships = Set(("r", "r")),
        innerRelationships = Set("r"),
        previouslyBoundRelationships = Set.empty,
        previouslyBoundRelationshipGroups = Set.empty,
        reverseGroupVariableProjections = false,
        expansionMode = ExpandAll,
        accumulators = Set.empty
      )

    plan shouldEqual planner.subPlanBuilder()
      .repeatTrail(`(a)((b)-[r]->()){2,3}(c)`)
      .|.filter(isRepeatTrailUnique("r"))
      .|.expandAll("(b)-[r]->(anon_0)")
      .|.argument("b")
      .allNodeScan("a")
      .build()
  }

  test(
    "Should not plan VarExpand instead of Trail on quantified path patterns with relationship and a inner node variable - right inner variable"
  ) {
    val query = "MATCH (a)(()-[r]->(b)){2,3}(c) RETURN *"
    val plan = planner.plan(query).stripProduceResults
    val `(a)(()-[r]->(b)){2,3}(c)` =
      TrailParameters(
        min = 2,
        max = UpperBound.Limited(3),
        start = "a",
        end = "c",
        innerStart = "anon_0",
        innerEnd = "b",
        groupNodes = Set(("b", "b")),
        groupRelationships = Set(("r", "r")),
        innerRelationships = Set("r"),
        previouslyBoundRelationships = Set.empty,
        previouslyBoundRelationshipGroups = Set.empty,
        reverseGroupVariableProjections = false,
        expansionMode = ExpandAll,
        accumulators = Set.empty
      )

    plan shouldEqual planner.subPlanBuilder()
      .repeatTrail(`(a)(()-[r]->(b)){2,3}(c)`)
      .|.filter(isRepeatTrailUnique("r"))
      .|.expandAll("(anon_0)-[r]->(b)")
      .|.argument("anon_0")
      .allNodeScan("a")
      .build()
  }

  test(
    "Should not plan VarExpand on quantified path pattern with multiple relationships"
  ) {
    val query = "MATCH (a)(()-[r]->()-[]->(b)){2,3}(c) RETURN *"
    val plan = planner.plan(query).stripProduceResults
    val `(a)(()-[r]->(b)){2,3}(c)` =
      TrailParameters(
        min = 2,
        max = UpperBound.Limited(3),
        start = "a",
        end = "c",
        innerStart = "anon_0",
        innerEnd = "b",
        groupNodes = Set(("b", "b")),
        groupRelationships = Set(("r", "r")),
        innerRelationships = Set("r", "anon_2"),
        previouslyBoundRelationships = Set.empty,
        previouslyBoundRelationshipGroups = Set.empty,
        reverseGroupVariableProjections = false,
        expansionMode = ExpandAll,
        accumulators = Set.empty
      )

    plan shouldEqual planner.subPlanBuilder()
      .repeatTrail(`(a)(()-[r]->(b)){2,3}(c)`)
      .|.filter("not anon_2 = r", isRepeatTrailUnique("anon_2"))
      .|.expandAll("(anon_1)-[anon_2]->(b)")
      .|.filter(isRepeatTrailUnique("r"))
      .|.expandAll("(anon_0)-[r]->(anon_1)")
      .|.argument("anon_0")
      .allNodeScan("a")
      .build()
  }

  test(
    "Should plan VarExpand instead of Trail on quantified path pattern with relationship and no inner node variable - juxtaposed relationship"
  ) {
    val query = "MATCH ()--(a)(()-[r]->()){2,3} RETURN *"
    val plan = planner.plan(query).stripProduceResults

    plan shouldEqual planner.subPlanBuilder()
      .filter("not anon_0 IN r")
      .expandAll("(a)-[r*2..3]->()")
      .allRelationshipsScan("()-[anon_0]-(a)")
      .build()
  }

  test(
    "Should not plan VarExpand instead of Trail on quantified path pattern with pre-filter predicate that has no dependencies"
  ) {
    val query = "MATCH (a) ((n)-[r]->(m) WHERE 1 = true)+ (b) RETURN r"
    val plan = planner.plan(query).stripProduceResults
    val `(a) ((n)-[r]-(m))+ (b)` =
      TrailParameters(
        min = 1,
        max = UpperBound.Unlimited,
        start = "a",
        end = "b",
        innerStart = "n",
        innerEnd = "m",
        groupNodes = Set.empty,
        groupRelationships = Set(("r", "r")),
        innerRelationships = Set("r"),
        previouslyBoundRelationships = Set.empty,
        previouslyBoundRelationshipGroups = Set.empty,
        reverseGroupVariableProjections = false,
        expansionMode = ExpandAll,
        accumulators = Set.empty
      )

    plan shouldEqual planner.subPlanBuilder()
      .repeatTrail(`(a) ((n)-[r]-(m))+ (b)`)
      .|.filter(isRepeatTrailUnique("r"))
      .|.expandAll("(n)-[r]->(m)")
      .|.filter("1 = true")
      .|.argument("n")
      .allNodeScan("a")
      .build()
  }

  test(
    "Should not plan VarExpand instead of Trail on quantified path pattern with pre-filter predicate that has only previously matched dependencies"
  ) {
    val query = "MATCH (z) WITH * SKIP 0 MATCH (a) ((n)-[r]->(m) WHERE z.prop = true)+ (b) RETURN r"
    val plan = planner.plan(query).stripProduceResults
    val `(a) ((n)-[r]-(m))+ (b)` =
      TrailParameters(
        min = 1,
        max = UpperBound.Unlimited,
        start = "a",
        end = "b",
        innerStart = "n",
        innerEnd = "m",
        groupNodes = Set.empty,
        groupRelationships = Set(("r", "r")),
        innerRelationships = Set("r"),
        previouslyBoundRelationships = Set(),
        previouslyBoundRelationshipGroups = Set(),
        reverseGroupVariableProjections = false,
        expansionMode = ExpandAll,
        accumulators = Set.empty
      )

    plan shouldEqual planner.subPlanBuilder()
      .apply()
      .|.repeatTrail(`(a) ((n)-[r]-(m))+ (b)`)
      .|.|.filter(isRepeatTrailUnique("r"))
      .|.|.expandAll("(n)-[r]->(m)")
      .|.|.filter("cacheN[z.prop] = true")
      .|.|.argument("n", "z")
      .|.allNodeScan("a", "z")
      .cacheProperties("cacheNFromStore[z.prop]")
      .skip(0)
      .allNodeScan("z")
      .build()
  }

  test("Should not plan VarExpand instead of Trail on quantified path pattern with filter on start node") {
    val query = "MATCH (a) ((n)-[r]->(m) WHERE n.p = 1)+ (b) RETURN r"
    val plan = planner.plan(query).stripProduceResults
    val `(a) ((n)-[r]-(m))+ (b)` =
      TrailParameters(
        min = 1,
        max = UpperBound.Unlimited,
        start = "a",
        end = "b",
        innerStart = "n",
        innerEnd = "m",
        groupNodes = Set.empty,
        groupRelationships = Set(("r", "r")),
        innerRelationships = Set("r"),
        previouslyBoundRelationships = Set.empty,
        previouslyBoundRelationshipGroups = Set.empty,
        reverseGroupVariableProjections = false,
        expansionMode = ExpandAll,
        accumulators = Set.empty
      )

    plan shouldEqual planner.subPlanBuilder()
      .repeatTrail(`(a) ((n)-[r]-(m))+ (b)`)
      .|.filter(isRepeatTrailUnique("r"))
      .|.expandAll("(n)-[r]->(m)")
      .|.filter("n.p = 1")
      .|.argument("n")
      .filter("a.p = 1")
      .allNodeScan("a")
      .build()
  }

  test("Should plan pruning VarExpand instead of Trail on quantified path pattern with filter on start node") {
    val query = "MATCH (a) ((n)-[r]->(m) WHERE n.p = 1)+ (b) RETURN DISTINCT b"
    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .distinct("b AS b")
      .bfsPruningVarExpand(
        "(a)-[r*1..]->(b)",
        relationshipPredicates = Seq(Predicate("anon_0", "startNode(anon_0).p = 1"))
      )
      .filter("a.p = 1")
      .allNodeScan("a")
      .build()
  }

  test(
    "Should plan VarExpand instead of Trail on quantified path pattern with pre-filter predicate that has inner relationship dependency"
  ) {
    val query = "MATCH (a) ((n)-[r]->(m) WHERE properties(r).p = 1)+ (b) RETURN r"
    val plan = planner.plan(query).stripProduceResults

    plan shouldEqual planner.subPlanBuilder()
      .expand("(a)-[r*1..]->()", relationshipPredicates = Seq(Predicate("anon_0", "properties(anon_0).p = 1")))
      .allNodeScan("a")
      .build()
  }

  test(
    "Should plan VarExpand instead of Trail on quantified path pattern with predicates that has non-local variable dependency"
  ) {
    val query = s"MATCH (a) ((n)-[r]->(m) WHERE m.p = a.p)+ (b) RETURN r"
    planner.plan(query) should equal(
      planner.planBuilder()
        .produceResults("r")
        .filter("b.p = cacheN[a.p]")
        .expand(
          "(a)-[r*1..]->(b)",
          relationshipPredicates = Seq(Predicate("anon_0", "endNode(anon_0).p = cacheNFromStore[a.p]"))
        )
        .allNodeScan("a")
        .build()
    )
  }

  test(
    "Should plan VarExpand instead of Trail on quantified path pattern with predicates that has dependency on variables from previous clauses"
  ) {
    val query = s"MATCH (z) WITH * SKIP 0 MATCH (a) ((n)-[r]->(m) WHERE properties(r).p = z.p)+ (b) RETURN r"
    val plan = planner.plan(query).stripProduceResults

    plan shouldEqual planner.subPlanBuilder()
      .expand(
        "(a)-[r*1..]->()",
        relationshipPredicates = Seq(Predicate("anon_0", "properties(anon_0).p = cacheN[z.p]"))
      )
      .apply()
      .|.allNodeScan("a", "z")
      .cacheProperties("cacheNFromStore[z.p]")
      .skip(0)
      .allNodeScan("z")
      .build()
  }

  relationshipPredicatesWithoutPropertyAccess.foreach {
    case RelationshipPredicate(queryPredicate, plannedType, plannedPredicates @ _*) =>
      test(
        s"Should plan VarExpand instead of Trail on quantified path pattern with relationship predicate $queryPredicate"
      ) {
        val query = s"MATCH (a) ((n)-[r $queryPredicate]->(m))+ () RETURN r"
        val plan = planner.plan(query).stripProduceResults
        plan shouldEqual planner.subPlanBuilder()
          .expand(s"(a)-[r$plannedType*1..]->()", relationshipPredicates = plannedPredicates)
          .allNodeScan("a")
          .build()
      }

      test(
        s"Should plan VarExpand instead of Trail on quantified relationship with relationship predicate $queryPredicate"
      ) {
        val query = s"MATCH (a)-[r $queryPredicate]->+(b) RETURN r"
        val plan = planner.plan(query).stripProduceResults

        plan shouldEqual planner.subPlanBuilder()
          .expand(s"(a)-[r$plannedType*1..]->()", relationshipPredicates = plannedPredicates.toList)
          .allNodeScan("a")
          .build()
      }
  }

  test("Should plan VarExpand instead of Trail on quantified path pattern with post-filter relationship predicate") {
    val query = "MATCH (a) ((n)-[r]->(m))+ (b) WHERE all(x IN r WHERE properties(x).p = 0 AND x IS NOT NULL) RETURN r"
    val plan = planner.plan(query).stripProduceResults
    val predicates = Seq(Predicate("anon_0", "properties(anon_0).p = 0"), Predicate("anon_0", "anon_0 IS NOT NULL"))

    plan shouldEqual planner.subPlanBuilder()
      .expand("(a)-[r*1..]->()", relationshipPredicates = predicates)
      .allNodeScan("a")
      .build()
  }

  test(
    "Should plan VarExpand instead of Trail on Quantified path patterns with Relationship and no inner node variables - Relationship form Kleene plus"
  ) {
    val query = "MATCH (a)-[r]->+(b) RETURN *"
    val plan = planner.plan(query).stripProduceResults

    plan shouldEqual planner.subPlanBuilder()
      .expandAll("(a)-[r*1..]->(b)")
      .allNodeScan("a")
      .build()
  }

  test("Should plan VarExpand with relationship uniqueness predicate for QPP juxtaposed to relationship pattern") {
    val query = "MATCH (a) ((b)-[r1]->(c))+ (d)-[r2]->(e) RETURN r1, r2"
    val plan = planner.plan(query).stripProduceResults

    plan shouldEqual planner.subPlanBuilder()
      .filter("not r2 IN r1")
      .expandAll("(d)<-[r1*1..]-()")
      .allRelationshipsScan("(d)-[r2]->()")
      .build()
  }

  test("Should plan VarExpand with relationship uniqueness predicate for QPP juxtaposed to QPP") {
    val query = "MATCH (a) ((b)-[r1]->(c))+ (d) ((e)-[r2]->(f))+ RETURN r1, r2"
    val plan = planner.plan(query).stripProduceResults

    plan shouldEqual planner.subPlanBuilder()
      .filter(disjoint("r1", "r2"))
      .expandAll("(d)<-[r1*1..]-()")
      .expandAll("(d)-[r2*1..]->()")
      .allNodeScan("d")
      .build()
  }

  test("Should plan VarExpand with relationship uniqueness predicates, for a mix of QPPs and relationship patterns") {
    val query = "MATCH (a) ((b)-[r1]->(c))+ (d) ((e)-[r2]->(f))+ (g)-[r3]-(h) RETURN r1, r2, r3"
    val plan = planner.plan(query).stripProduceResults

    plan shouldEqual planner.subPlanBuilder()
      .filter("not r3 IN r1", disjoint("r1", "r2"))
      .expandAll("(d)<-[r1*1..]-()")
      .filter("not r3 IN r2")
      .expandAll("(g)<-[r2*1..]-(d)")
      .allRelationshipsScan("(g)-[r3]-()")
      .build()
  }

  test(
    "Should plan VarExpand without relationship uniqueness predicates if provably disjoint, for a mix of QPPs and relationship patterns"
  ) {
    val query = "MATCH (a) ((b)-[r1:R]->(c))+ (d) ((e)-[r2:T]->(f))+ (g)-[r3:S]-(h) RETURN r1, r2, r3"
    val plan = planner.plan(query).stripProduceResults

    plan shouldEqual planner.subPlanBuilder()
      .expandAll("(d)<-[r1:R*1..]-()")
      .expandAll("(g)<-[r2:T*1..]-(d)")
      .relationshipTypeScan("(g)-[r3:S]-()")
      .build()
  }

  test("Should plan VarExpand for multiple qpp's") {
    val query = "MATCH (a) (()-[r1]->())+ (b) (()<-[r2]-())+ (c) RETURN *"
    val plan = planner.plan(query).stripProduceResults

    plan shouldEqual planner.subPlanBuilder()
      .filter(disjoint("r1", "r2"))
      .expand("(b)<-[r1*1..]-(a)")
      .expand("(b)<-[r2*1..]-(c)", projectedDir = INCOMING)
      .allNodeScan("b")
      .build()
  }

  test("Should plan VarExpand if there are argumentIds") {
    val query = "MATCH (a) WITH a SKIP 1 MATCH (a) (()-[r1]->())+ (b) RETURN *"
    val plan = planner.plan(query).stripProduceResults

    plan shouldEqual planner.subPlanBuilder()
      .expand("(a)-[r1*1..]->(b)")
      .skip(1)
      .allNodeScan("a")
      .build()
  }

  test(
    "Should plan VarExpand instead of Trail on Quantified path patterns where unnecessary group variables have been removed"
  ) {
    val query = "MATCH ((a)-[r]->(b))+ RETURN 1"
    val plan = planner.plan(query).stripProduceResults

    plan shouldEqual planner.subPlanBuilder()
      .projection("1 AS 1")
      .expandAll("(anon_0)-[*1..]->()")
      .allNodeScan("anon_0")
      .build()
  }

  test("Should plan VarExpand for named path if named path variable is not used") {
    val query = "MATCH p=(a) ((b)-[r]->(c))+ (d) RETURN r"
    val plan = planner.plan(query).stripProduceResults

    plan shouldEqual planner.subPlanBuilder()
      .expandAll("(a)-[r*1..]->()")
      .allNodeScan("a")
      .build()
  }

  test("Should plan VarExpand for named path if named path variable is used") {
    val query = "MATCH p=(a) ((b)-[r]->(c))+ (d) RETURN p"
    val plan = planner.plan(query).stripProduceResults

    val path = PathExpression(NodePathStep(
      v"a",
      MultiRelationshipPathStep(varFor("r"), OUTGOING, Some(varFor("d")), NilPathStep()(pos))(pos)
    )(pos))(pos)

    plan shouldEqual planner.subPlanBuilder()
      .projection(Map("p" -> path))
      .expand("(a)-[r*1..]->(d)")
      .allNodeScan("a")
      .build()
  }

  test("Should plan PruningVarExpand for VarExpand QPP with DISTINCT endNode") {
    val query = "MATCH (a) ((b)-[r]->(c))+ (d) RETURN DISTINCT d"
    val plan = planner.plan(query).stripProduceResults

    plan shouldEqual planner.subPlanBuilder()
      .distinct("d AS d")
      .bfsPruningVarExpand("(a)-[*1..2147483647]->(d)")
      .allNodeScan("a")
      .build()
  }

  test(
    "Should plan Expand with unique group variable names after group variable optimisation"
  ) {
    val planner = plannerBuilder()
      .setAllNodesCardinality(10)
      .setAllRelationshipsCardinality(10)
      .setLabelCardinality("A", 10)
      .setRelationshipCardinality("(:A)-[]->()", 10)
      .setRelationshipCardinality("(:A)-[]->(:A)", 5)
      .enableDeduplicateNames(enable = false)
      .build()
    val query = "MATCH p=(()-[y]->(z))+ RETURN p"
    val plan = planner.plan(query).stripProduceResults
    val path = PathExpression(NodePathStep(
      v"  UNNAMED0",
      MultiRelationshipPathStep(v"  y@0", OUTGOING, Some(v"  UNNAMED1"), NilPathStep()(pos))(pos)
    )(pos))(pos)

    plan shouldEqual planner.subPlanBuilder()
      .projection(Map("p" -> path))
      .expand("(`  UNNAMED0`)-[`  y@0`*1..]->(`  UNNAMED1`)")
      .allNodeScan("`  UNNAMED0`")
      .build()
  }

  test(
    "Should plan Expand with unique group variable names after group variable optimisation with anonymous relationship"
  ) {
    val planner = plannerBuilder()
      .setAllNodesCardinality(10)
      .setAllRelationshipsCardinality(10)
      .setLabelCardinality("A", 10)
      .setRelationshipCardinality("(:A)-[]->()", 10)
      .setRelationshipCardinality("(:A)-[]->(:A)", 5)
      .enableDeduplicateNames(enable = false)
      .build()
    val query = "MATCH p=(()-[]->(z))+ RETURN p"
    val plan = planner.plan(query).stripProduceResults
    val path = PathExpression(NodePathStep(
      v"  UNNAMED0",
      MultiRelationshipPathStep(v"  UNNAMED2", OUTGOING, Some(v"  UNNAMED1"), NilPathStep()(pos))(pos)
    )(pos))(pos)

    plan shouldEqual planner.subPlanBuilder()
      .projection(Map("p" -> path))
      .expand("(`  UNNAMED0`)-[`  UNNAMED2`*1..]->(`  UNNAMED1`)")
      .allNodeScan("`  UNNAMED0`")
      .build()
  }

  test("Should plan implicit join with group variable / legacy var-length relationship correctly") {
    val query = "MATCH (a) ((n)-[r]->(m))+ (b) MATCH (c)-[r*]->(d) RETURN *"
    val plan = planner.plan(query).stripProduceResults
    val `(a) ((n)-[r]->(m))+ (b)` = TrailParameters(
      min = 1,
      max = Unlimited,
      start = "a",
      end = "b",
      innerStart = "n",
      innerEnd = "m",
      groupNodes = Set(("n", "n"), ("m", "m")),
      groupRelationships = Set(("r", "r")),
      innerRelationships = Set("r"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = false,
      expansionMode = ExpandAll,
      accumulators = Set.empty
    )
    // A better plan than Expand + ValueHashJoin would be this one:
    // .projectEndpoints("(c)-[r*]->(d)", startInScope = false, endInScope = false)
    // We do not plan it because the other approach is simpler and avoids edge cases with IDP compaction.
    plan shouldEqual planner.subPlanBuilder()
      .valueHashJoin("r = anon_0")
      .|.expand("(c)-[anon_0*1..]->(d)")
      .|.allNodeScan("c")
      .filter("size(r) >= 1")
      .repeatTrail(`(a) ((n)-[r]->(m))+ (b)`)
      .|.filter(isRepeatTrailUnique("r"))
      .|.expandAll("(n)-[r]->(m)")
      .|.argument("n")
      .allNodeScan("a")
      .build()
  }

  test(
    "Should plan implicit join with group variable / legacy var-length relationship correctly (start in scope)"
  ) {
    val query = "MATCH (a) ((n)-[r]-(m))* (b) MATCH (b)-[r*0..]-(d) RETURN *"
    val plan = planner.plan(query).stripProduceResults
    val `(a) ((n)-[r]-(m))+ (b)` = TrailParameters(
      min = 0,
      max = Unlimited,
      start = "a",
      end = "b",
      innerStart = "n",
      innerEnd = "m",
      groupNodes = Set(("n", "n"), ("m", "m")),
      groupRelationships = Set(("r", "r")),
      innerRelationships = Set("r"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = false,
      expansionMode = ExpandAll,
      accumulators = Set.empty
    )
    // A better plan than Expand + Filter would be this one:
    // .projectEndpoints("(b)-[r*0..]-(d)", startInScope = true, endInScope = false)
    // We do not plan it because the other approach is simpler and avoids edge cases with IDP compaction.
    plan shouldEqual planner.subPlanBuilder()
      .filter("r = anon_0")
      .expand("(b)-[anon_0*0..]-(d)")
      .repeatTrail(`(a) ((n)-[r]-(m))+ (b)`)
      .|.filter(isRepeatTrailUnique("r"))
      .|.expandAll("(n)-[r]-(m)")
      .|.argument("n")
      .allNodeScan("a")
      .build()
  }

  test(
    "Should plan implicit join with group variable / legacy var-length relationship correctly (start and end in scope)"
  ) {
    val query = "MATCH (a) ((n)-[r]-(m))* (b) MATCH (a)-[r*0..]-(b) RETURN *"
    val plan = planner.plan(query).stripProduceResults
    val `(a) ((n)-[r]-(m))+ (b)` = TrailParameters(
      min = 0,
      max = Unlimited,
      start = "a",
      end = "b",
      innerStart = "n",
      innerEnd = "m",
      groupNodes = Set(("n", "n"), ("m", "m")),
      groupRelationships = Set(("r", "r")),
      innerRelationships = Set("r"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = false,
      expansionMode = ExpandAll,
      accumulators = Set.empty
    )
    // A better plan than Expand + NodeHashJoin + Filter would be this one:
    // .projectEndpoints("(a)-[r*0..]-(b)", startInScope = true, endInScope = true)
    // We do not plan it because the other approach is simpler and avoids edge cases with IDP compaction.
    plan shouldEqual planner.subPlanBuilder()
      .filter("r = anon_0")
      .nodeHashJoin("a", "b")
      .|.repeatTrail(`(a) ((n)-[r]-(m))+ (b)`)
      .|.|.filter(isRepeatTrailUnique("r"))
      .|.|.expandAll("(n)-[r]-(m)")
      .|.|.argument("n")
      .|.allNodeScan("a")
      .expand("(a)-[anon_0*0..]-(b)", pathMode = Trail)
      .allNodeScan("a")
      .build()
  }

  test("Should plan pattern predicate with dependency on a variable from previous MATCH inside Trail") {
    val query = s"MATCH (z) WITH * SKIP 0 MATCH (a) ((n)-[r]->(m) WHERE EXISTS { (m)-[mzRel]-(z) })+ (b) RETURN r"
    val plan = planner.plan(query).stripProduceResults

    val trailParameters = TrailParameters(
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
      reverseGroupVariableProjections = false,
      expansionMode = ExpandAll,
      accumulators = Set.empty
    )
    plan shouldEqual planner.subPlanBuilder()
      .apply()
      .|.repeatTrail(trailParameters)
      .|.|.filter(isRepeatTrailUnique("r"))
      .|.|.semiApply()
      .|.|.|.expandInto("(m)-[]-(z)")
      .|.|.|.argument("m", "z")
      .|.|.expandAll("(n)-[r]->(m)")
      .|.|.argument("n", "z")
      .|.allNodeScan("a", "z")
      .skip(0)
      .allNodeScan("z")
      .build()
  }

  test("Should plan QPPs in two path patterns with predicates from one to the other") {
    val query =
      """MATCH
        |  (a) ((b)-[r]-(c) WHERE i.prop = properties(r).prop)+ (d)-[t]-(e),
        |  (f)-[u:R]-(d) ((g)-[s:S]-(h) WHERE properties(s).prop = a.prop)+ (i)
        |RETURN count(*)""".stripMargin

    planner.plan(query) should equal(
      planner.planBuilder()
        .produceResults("`count(*)`")
        .aggregation(Seq(), Seq("count(*) AS `count(*)`"))
        .filter(
          "NOT t IN r",
          "NOT u IN r",
          "none(anon_2 IN r WHERE anon_2 IN s)",
          "all(anon_0 IN range(0, size(s) - 1) WHERE (properties(s[anon_0])).prop = a.prop)"
        )
        .expand(
          "(d)-[r*1..]-(a)",
          projectedDir = INCOMING,
          relationshipPredicates = Seq(Predicate("anon_1", "i.prop = properties(anon_1).prop"))
        )
        .filter("NOT t IN s")
        .expand("(d)-[s:S*1..]-(i)", projectedDir = OUTGOING)
        .filter("NOT t = u")
        .expandAll("(d)-[t]-()")
        .relationshipTypeScan("()-[u:R]-(d)")
        .build()
    )
  }

  test("Should not inline ForAllRepetitions predicate into the wrong QPP") {
    val query =
      """MATCH
        |  (a:N {start: "Yes"}) ((b)-[r]-(c) WHERE c in h)+ (d) ((g)-[s]-(h))+ (i)
        |RETURN count(*)""".stripMargin

    planner.plan(query) should equal(
      planner.planBuilder()
        .produceResults("`count(*)`")
        .aggregation(Seq(), Seq("count(*) AS `count(*)`"))
        .filter("d IN h", "all(anon_0 IN range(0, size(c) - 1) WHERE c[anon_0] IN h)")
        .repeatTrail(TrailParameters(
          1,
          Unlimited,
          "d",
          "i",
          "g",
          "h",
          Set(("h", "h")),
          Set(),
          Set("s"),
          Set(),
          Set("r"),
          false,
          expansionMode = ExpandAll,
          Set.empty
        ))
        .|.filter(isRepeatTrailUnique("s"))
        .|.expandAll("(g)-[s]-(h)")
        .|.argument("g")
        .repeatTrail(TrailParameters(
          1,
          Unlimited,
          "a",
          "d",
          "b",
          "c",
          Set(("c", "c")),
          Set(("r", "r")),
          Set("r"),
          Set(),
          Set(),
          false,
          expansionMode = ExpandAll,
          Set.empty
        ))
        .|.filter(isRepeatTrailUnique("r"))
        .|.expandAll("(b)-[r]-(c)")
        .|.argument("b")
        .filter("a.start = 'Yes'")
        .nodeByLabelScan("a", "N")
        .build()
    )
  }

  test("Should inline ForAllRepetitions predicate if possible and plan afterwards if not") {
    val query =
      """MATCH
        |  (z)-[r WHERE r.prop = z.prop]-+(a)((b)--(c) WHERE c.prop = a.prop AND b.prop = d.prop)+(d)-[s WHERE elementId(s) = e.prop]-+(e)
        |RETURN count(*)""".stripMargin

    planner.plan(query) should equal(
      planner.planBuilder()
        .produceResults("`count(*)`")
        .aggregation(Seq(), Seq("count(*) AS `count(*)`"))
        .filter(
          "none(anon_5 IN r WHERE anon_5 IN anon_1)",
          "none(anon_6 IN r WHERE anon_6 IN s)",
          "all(anon_2 IN range(0, size(r) - 1) WHERE (r[anon_2]).prop = z.prop)"
        )
        .expand(
          "(a)-[r*1..]-(z)",
          projectedDir = INCOMING
        )
        .filter(
          andsReorderable(
            "cacheNFromStore[a.prop] = cacheN[d.prop]",
            "cacheN[d.prop] = cacheNFromStore[a.prop]"
          ),
          "all(anon_3 IN range(0, size(c) - 1) WHERE (c[anon_3]).prop = cacheNFromStore[a.prop])"
        )
        .repeatTrail(TrailParameters(
          1,
          Unlimited,
          "d",
          "a",
          "c",
          "b",
          Set(("c", "c")),
          Set(("anon_0", "anon_1")),
          Set("anon_0"),
          Set(),
          Set("s"),
          true,
          expansionMode = ExpandAll,
          Set.empty
        ))
        .|.filter("b.prop = cacheNFromStore[d.prop]", isRepeatTrailUnique("anon_0"))
        .|.expandAll("(c)-[anon_0]-(b)")
        .|.argument("c", "d")
        .expand(
          "(e)-[s*1..]-(d)",
          projectedDir = INCOMING,
          relationshipPredicates = Seq(Predicate("anon_4", "elementId(anon_4) = e.prop"))
        )
        .allNodeScan("e")
        .build()
    )
  }

  test("Should plan cartesian product for components connected when the cardinality of LHS is low") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("Car", 50)
      .setLabelCardinality("Geo", 50)
      .setAllRelationshipsCardinality(40)
      .setRelationshipCardinality("()-[:ROAD]->()", 40)
      .setRelationshipCardinality("(:Geo)-[:ROAD]->(:Geo)", 40)
      .setRelationshipCardinality("()-[:ROAD]->(:Geo)", 40)
      .setRelationshipCardinality("(:Geo)-[:ROAD]->()", 40)
      .build()

    val query =
      """
        |MATCH (c:Car {id: $car_id})
        |MATCH (a:Geo {name: $source_geo_name})
        |MATCH (b:Geo {name: $target_geo_name})
        |MATCH p =  (a)
        |  (() -[rels:ROAD]->(x:Geo) WHERE rels.distance_km <> c.battery_capacity_kwh)+
        |  (b)
        |RETURN p
        |""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .projection(Map("p" -> varLengthPathExpression(
        start = v"a",
        end = v"b",
        relationships = v"rels",
        direction = OUTGOING
      )))
      .filter("all(anon_0 IN range(0, size(rels) - 1) WHERE NOT (rels[anon_0]).distance_km = c.battery_capacity_kwh)")
      .cartesianProduct()
      .|.filter("b.name = $target_geo_name", "b:Geo")
      .|.expandExpr(
        "(a)-[rels:ROAD*1..]->(b)",
        expandMode = ExpandAll,
        projectedDir = OUTGOING,
        nodePredicates = Seq(),
        relationshipPredicates = Seq(VariablePredicate(v"anon_1", hasLabels(endNode("anon_1"), "Geo"))),
        pathMode = Trail
      )
      .|.filter("a.name = $source_geo_name")
      .|.nodeByLabelScan("a", "Geo", IndexOrderNone)
      .filter("c.id = $car_id")
      .nodeByLabelScan("c", "Car", IndexOrderNone)
      .build()
  }

  test("should plan Apply for components connected by predicates in QPP if RHS doesn't use AllNodesScan") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("Car", 50)
      .setLabelCardinality("Geo", 50)
      .setAllRelationshipsCardinality(1000)
      .setRelationshipCardinality("()-[:ROAD]->()", 500)
      .setRelationshipCardinality("(:Geo)-[:ROAD]->(:Geo)", 500)
      .setRelationshipCardinality("()-[:ROAD]->(:Geo)", 500)
      .setRelationshipCardinality("(:Geo)-[:ROAD]->()", 500)
      .build()

    val query =
      """
        |MATCH (c:Car {id: $car_id})
        |MATCH (a:Geo {name: $source_geo_name})
        |MATCH (b:Geo {name: $target_geo_name})
        |MATCH p =  (a)
        |  (() -[rels:ROAD]->(x:Geo) WHERE rels.distance_km <> c.battery_capacity_kwh)+
        |  (b)
        |RETURN p
        |""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .projection(Map("p" -> varLengthPathExpression(
        start = v"a",
        end = v"b",
        relationships = v"rels",
        direction = OUTGOING
      )))
      .filter("a.name = $source_geo_name", hasLabels("a", "Geo"))
      .apply()
      .|.repeatTrail(TrailParameters(
        1,
        Unlimited,
        "b",
        "a",
        "x",
        "anon_0",
        Set(),
        Set(("rels", "rels")),
        Set("rels"),
        Set(),
        Set(),
        true,
        ExpandAll,
        Set()
      ))
      .|.|.filter("NOT rels.distance_km = cacheN[c.battery_capacity_kwh]", isRepeatTrailUnique("rels"))
      .|.|.expandAll("(x)<-[rels:ROAD]-(anon_0)")
      .|.|.filter("x:Geo")
      .|.|.argument("x", "c")
      .|.filter("b.name = $target_geo_name")
      .|.nodeByLabelScan("b", "Geo", IndexOrderNone, "c")
      .cacheProperties("cacheNFromStore[c.battery_capacity_kwh]")
      .filter("c.id = $car_id")
      .nodeByLabelScan("c", "Car", IndexOrderNone)
      .build()
  }

  test("Should plan apply for components connected only by a predicate inside QPP, subquery expression") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("User", 50)
      .setRelationshipCardinality("()-[]->()", 1500)
      .setRelationshipCardinality("(:User)-[]->()", 900)
      .enableDeduplicateNames(enable = false)
      .build()

    val query =
      """
        |MATCH (a:User)
        |MATCH (b) ((c)-[r]->(d) WHERE EXISTS { (a)-[rr]->(d) } )+ (e)
        |RETURN a, b, c, d, e
        |""".stripMargin

    val plan = planner.plan(query).stripProduceResults

    val trailParameters = TrailParameters(
      min = 1,
      max = Unlimited,
      start = "e",
      end = "b",
      innerStart = "  d@2",
      innerEnd = "  c@0",
      groupNodes = Set(("  c@0", "  c@3"), ("  d@2", "  d@4")),
      groupRelationships = Set(),
      innerRelationships = Set("  r@1"),
      previouslyBoundRelationships = Set(),
      previouslyBoundRelationshipGroups = Set(),
      reverseGroupVariableProjections = true,
      expansionMode = ExpandAll,
      accumulators = Set.empty
    )

    val expected = planner.subPlanBuilder()
      .apply()
      .|.repeatTrail(trailParameters)
      .|.|.filter(isRepeatTrailUnique("  r@1"))
      .|.|.expandAll("(`  d@2`)<-[`  r@1`]-(`  c@0`)")
      .|.|.semiApply()
      .|.|.|.expandInto("(a)-[]->(`  d@2`)")
      .|.|.|.argument("a", "  d@2")
      .|.|.argument("  d@2", "a")
      .|.allNodeScan("e", "a")
      .nodeByLabelScan("a", "User", IndexOrderNone)
      .build()

    plan shouldEqual expected
  }

  test("Should pass label info to QPP planner to estimate cardinality correctly") {

    val placeCount = 10
    val toPlaceCount = 9500
    val relsPerPlace = toPlaceCount / placeCount

    val planner = plannerBuilder()
      .setAllNodesCardinality(10000)
      .setLabelCardinality("Place", placeCount)
      .setLabelCardinality("Start", 10)
      .setRelationshipCardinality("(:Start)-[:TO_PLACE]->(:Place)", 10)
      .setRelationshipCardinality("(:Start)-[:TO_PLACE]->()", 10)
      .setRelationshipCardinality("(:Start)-[]->()", 5000)
      .setRelationshipCardinality("()-[]->()", 10000)
      .setRelationshipCardinality("()-[:TO_PLACE]->()", toPlaceCount)
      .setRelationshipCardinality("()-[:TO_PLACE]->(:Place)", toPlaceCount)
      .setRelationshipCardinality("(:Place)-[:TO_PLACE]->()", toPlaceCount)
      .setRelationshipCardinality("(:Place)-[:TO_PLACE]->(:Place)", toPlaceCount)
      .build()

    val query =
      """
        |MATCH (start:Start)-[r:TO_PLACE]->(place:Place)
        |MATCH
        |  (start)
        |  ((b)-[p]->(c)
        |    WHERE EXISTS { (x)-[q:TO_PLACE]->(place) WHERE x.prop > p.prop }
        |  )+
        |  (end)
        |RETURN start, end, b, c
        |""".stripMargin

    val plan = planner.planState(query)

    val trailParameters = TrailParameters(
      min = 1,
      max = Unlimited,
      start = "start",
      end = "end",
      innerStart = "b",
      innerEnd = "c",
      groupNodes = Set(("b", "b"), ("c", "c")),
      groupRelationships = Set(),
      innerRelationships = Set("p"),
      previouslyBoundRelationships = Set(),
      previouslyBoundRelationshipGroups = Set(),
      reverseGroupVariableProjections = false,
      expansionMode = ExpandAll,
      accumulators = Set.empty
    )

    val expected = planner.subPlanBuilder()
      .produceResults("start", "end", "b", "c")
      .repeatTrail(trailParameters)
      .|.filter(isRepeatTrailUnique("p"))
      .|.semiApply()
      .|.|.filter("x.prop > cacheR[p.prop]")
      .|.|.expandAll("(place)<-[:TO_PLACE]-(x)").withCardinality(relsPerPlace) // <-- testing this
      .|.|.cacheProperties("cacheRFromStore[p.prop]")
      .|.|.argument("place", "p")
      .|.expandAll("(b)-[p]->(c)")
      .|.argument("b", "place")
      .filter("place:Place")
      .expandAll("(start)-[:TO_PLACE]->(place)")
      .nodeByLabelScan("start", "Start")

    plan should haveSamePlanAndCardinalitiesAsBuilder(
      expected,
      AttributeComparisonStrategy.ComparingProvidedAttributesOnly
    )
  }

  test("Should rewrite single directed relationships to varexpand") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("A", 10)
      .setLabelCardinality("B", 10)
      .setRelationshipCardinality("(:A)-[:R]->(:A)", 10)
      .setRelationshipCardinality("()-[:R]->()", 10)
      .setRelationshipCardinality("(:A)-[:R]->()", 10)
      .setRelationshipCardinality("()-[:R]->(:A)", 10)
      .build()

    val query =
      """
        |MATCH (a:A {name: "Foo"}) (()-[:R]->(next) WHERE next.name <> "Foo")* (e)
        |RETURN DISTINCT e
        |""".stripMargin

    val plan = planner.plan(query).stripProduceResults

    val expected = planner.subPlanBuilder()
      .distinct("e AS e")
      .bfsPruningVarExpand(
        "(a)-[:R*0..]->(e)",
        nodePredicates = Seq(),
        relationshipPredicates = Seq(Predicate("anon_0", "NOT endNode(anon_0).name = 'Foo'")),
        mode = ExpandAll
      )
      .filter("a.name = 'Foo'")
      .nodeByLabelScan("a", "A", IndexOrderNone)
      .build()

    plan shouldEqual expected
  }

  test("Should rewrite single directed relationships that depend on the outer node to varexpand") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("A", 10)
      .setLabelCardinality("B", 10)
      .setRelationshipCardinality("(:A)-[:R]->(:A)", 10)
      .setRelationshipCardinality("()-[:R]->()", 10)
      .setRelationshipCardinality("(:A)-[:R]->()", 10)
      .setRelationshipCardinality("()-[:R]->(:A)", 10)
      .build()

    val query =
      """
        |MATCH (a:A {name: "Foo"}) (()-[:R]->(next) WHERE next.name <> a.name)* (e)
        |RETURN DISTINCT e
        |""".stripMargin

    val plan = planner.plan(query).stripProduceResults

    val expected = planner.subPlanBuilder()
      .distinct("e AS e")
      .bfsPruningVarExpand(
        "(a)-[:R*0..]->(e)",
        nodePredicates = Seq(),
        relationshipPredicates = Seq(Predicate("anon_0", "NOT endNode(anon_0).name = cacheN[a.name]")),
        mode = ExpandAll
      )
      .filter("cacheNFromStore[a.name] = 'Foo'")
      .nodeByLabelScan("a", "A", IndexOrderNone)
      .build()

    plan shouldEqual expected
  }

  test("Should rewrite single directed relationships with predicates on both inner nodes to varExpand") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("A", 10)
      .setLabelCardinality("B", 10)
      .setRelationshipCardinality("(:A)-[:R]->(:A)", 10)
      .setRelationshipCardinality("()-[:R]->()", 10)
      .setRelationshipCardinality("(:A)-[:R]->()", 10)
      .setRelationshipCardinality("()-[:R]->(:A)", 10)
      .build()

    val query =
      """
        |MATCH (a:A {name: "Foo"}) ((prev)<-[:R]-(next) WHERE prev.prop > next.prop AND next.name <> "Foo")* (e)
        |RETURN DISTINCT e
        |""".stripMargin

    val plan = planner.plan(query).stripProduceResults

    val expected = planner.subPlanBuilder()
      .distinct("e AS e")
      .bfsPruningVarExpand(
        "(a)<-[:R*0..]-(e)",
        nodePredicates = Seq(),
        relationshipPredicates = Seq(
          Predicate("anon_0", "endNode(anon_0).prop > startNode(anon_0).prop"),
          Predicate("anon_0", "NOT startNode(anon_0).name = 'Foo'")
        ),
        mode = ExpandAll
      )
      .filter("a.name = 'Foo'")
      .nodeByLabelScan("a", "A", IndexOrderNone)
      .build()

    plan shouldEqual expected
  }

  test(
    "Should rewrite single undirected relationships to varExpand - start expanding from left, with predicate on the right inner node"
  ) {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("A", 10)
      .setLabelCardinality("B", 10)
      .setRelationshipCardinality("(:A)-[:R]->(:A)", 10)
      .setRelationshipCardinality("()-[:R]->()", 10)
      .setRelationshipCardinality("(:A)-[:R]->()", 10)
      .setRelationshipCardinality("()-[:R]->(:A)", 10)
      .build()

    val query =
      """
        |MATCH (a:A {name: "Foo"}) (()-[:R]-(n) WHERE n.name <> "Foo")* (e)
        |RETURN DISTINCT e
        |""".stripMargin

    val plan = planner.plan(query).stripProduceResults

    val expected = planner.subPlanBuilder()
      .distinct("e AS e")
      .bfsPruningVarExpandExpr(
        "(a)-[:R*0..]-(e)",
        relationshipPredicates = Seq(VariablePredicate(
          v"anon_0",
          not(equals(
            propExpression(TraversalEndpoint(v"anon_1", To), "name"),
            literalString("Foo")
          ))
        ))
      )
      .filter("a.name = 'Foo'")
      .nodeByLabelScan("a", "A", IndexOrderNone)
      .build()

    plan shouldEqual expected
  }

  test(
    "Should rewrite single undirected relationships to varExpand - start expanding from right, with predicate on the right inner node"
  ) {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("A", 10)
      .setLabelCardinality("B", 10)
      .setRelationshipCardinality("(:A)-[:R]->(:A)", 10)
      .setRelationshipCardinality("()-[:R]->()", 10)
      .setRelationshipCardinality("(:A)-[:R]->()", 10)
      .setRelationshipCardinality("()-[:R]->(:A)", 10)
      .build()

    val query =
      """
        |MATCH (a) (()-[:R]-(n) WHERE n.name <> "Foo")* (e:A {name: "Foo"})
        |RETURN DISTINCT e
        |""".stripMargin

    val plan = planner.plan(query).stripProduceResults

    val expected = planner.subPlanBuilder()
      .orderedDistinct(Seq("e"), "e AS e")
      .bfsPruningVarExpandExpr(
        "(e)-[:R*0..2147483647]-()",
        relationshipPredicates = Seq(VariablePredicate(
          v"anon_0",
          not(equals(
            prop(TraversalEndpoint(v"anon_1", From), "name"),
            literalString("Foo")
          ))
        ))
      )
      .filter("e.name = 'Foo'")
      .nodeByLabelScan("e", "A", IndexOrderAscending)
      .build()

    plan shouldEqual expected
  }

  test(
    "Should rewrite single undirected relationships to varExpand - start expanding from left, with predicate on the left inner node"
  ) {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("A", 10)
      .setLabelCardinality("B", 10)
      .setRelationshipCardinality("(:A)-[:R]->(:A)", 10)
      .setRelationshipCardinality("()-[:R]->()", 10)
      .setRelationshipCardinality("(:A)-[:R]->()", 10)
      .setRelationshipCardinality("()-[:R]->(:A)", 10)
      .build()

    val query =
      """
        |MATCH (a:A {name: "Foo"}) ((n)-[:R]-() WHERE n.name <> "Foo")* (e)
        |RETURN DISTINCT e
        |""".stripMargin

    val plan = planner.plan(query).stripProduceResults

    val expected = planner.subPlanBuilder()
      .distinct("e AS e")
      .bfsPruningVarExpandExpr(
        "(a)-[:R*0..2147483647]-(e)",
        relationshipPredicates = Seq(VariablePredicate(
          v"anon_0",
          not(equals(
            prop(TraversalEndpoint(v"anon_1", From), "name"),
            literalString("Foo")
          ))
        ))
      )
      .filter("a.name = 'Foo'")
      .nodeByLabelScan("a", "A", IndexOrderNone)
      .build()

    plan shouldEqual expected
  }

  test(
    "Should rewrite single undirected relationships to varExpand - start expanding from right, with predicate on the left inner node"
  ) {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("A", 10)
      .setLabelCardinality("B", 10)
      .setRelationshipCardinality("(:A)-[:R]->(:A)", 10)
      .setRelationshipCardinality("()-[:R]->()", 10)
      .setRelationshipCardinality("(:A)-[:R]->()", 10)
      .setRelationshipCardinality("()-[:R]->(:A)", 10)
      .build()

    val query =
      """
        |MATCH (a) ((n)-[:R]-() WHERE n.name <> "Foo")* (e:A {name: "Foo"})
        |RETURN DISTINCT e
        |""".stripMargin

    val plan = planner.plan(query).stripProduceResults

    val expected = planner.subPlanBuilder()
      .orderedDistinct(Seq("e"), "e AS e")
      .bfsPruningVarExpandExpr(
        "(e)-[:R*0..]-()",
        relationshipPredicates = Seq(VariablePredicate(
          v"anon_0",
          not(equals(
            propExpression(TraversalEndpoint(v"anon_1", To), "name"),
            literalString("Foo")
          ))
        ))
      )
      .filter("e.name = 'Foo'")
      .nodeByLabelScan("e", "A", IndexOrderAscending)
      .build()

    plan shouldEqual expected
  }

  test("Should rewrite simple queries to bidirectional queries to inlinable TraversalEndpoint predicates") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("A", 10)
      .setLabelCardinality("B", 10)
      .setRelationshipCardinality("(:A)-[:R]->(:A)", 10)
      .setRelationshipCardinality("()-[:R]->()", 10)
      .setRelationshipCardinality("(:A)-[:R]->()", 10)
      .setRelationshipCardinality("()-[:R]->(:A)", 10)
      .build()

    val query =
      """
        |MATCH p = (:A {prop: "bar"})
        |          (()-[:R]-({prop: "bar"}))* (e)
        |RETURN DISTINCT e
        """.stripMargin

    val plan = planner.plan(query).stripProduceResults

    val expected = planner.subPlanBuilder()
      .distinct("e AS e")
      .bfsPruningVarExpandExpr(
        "(anon_0)-[:R*0..]-(e)",
        relationshipPredicates = Seq(VariablePredicate(
          v"anon_1",
          equals(
            propExpression(TraversalEndpoint(v"anon_2", To), "prop"),
            literalString("bar")
          )
        )),
        mode = ExpandAll
      )
      .filter("anon_0.prop = 'bar'")
      .nodeByLabelScan("anon_0", "A", IndexOrderNone)
      .build()

    plan shouldEqual expected
  }

  test(
    "Should rewrite simple queries to bidirectional queries to inlinable NODE predicates if there is at least 1 repetition and all inner nodes have the same predicate"
  ) {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("A", 10)
      .setLabelCardinality("B", 10)
      .setRelationshipCardinality("(:A)-[:R]->(:A)", 10)
      .setRelationshipCardinality("()-[:R]->()", 10)
      .setRelationshipCardinality("(:A)-[:R]->()", 10)
      .setRelationshipCardinality("()-[:R]->(:A)", 10)
      .build()

    val query =
      """
        |MATCH p = (:A)
        |          (({prop: "bar"})-[:R]-({prop: "bar"}))+ (e)
        |RETURN DISTINCT e
        """.stripMargin

    val plan = planner.plan(query).stripProduceResults

    val expected = planner.subPlanBuilder()
      .distinct("e AS e")
      .filter("e.prop = 'bar'")
      .bfsPruningVarExpand(
        "(anon_0)-[:R*1..]-(e)",
        nodePredicates = Seq(Predicate("anon_1", "anon_1.prop = 'bar'")),
        relationshipPredicates = Seq(),
        mode = ExpandAll
      )
      .filter("anon_0.prop = 'bar'")
      .nodeByLabelScan("anon_0", "A", IndexOrderNone)
      .build()

    plan shouldEqual expected
  }

  test(
    "Should not plan to solve both endpoints using a Cartesian Product before planning the QPP when source-plan has cardinality larger than 1"
  ) {
    val nodes = 200
    val relationships = 500
    val a = 2
    val r = 100
    val builder = plannerBuilder()
      .setAllNodesCardinality(nodes)
      .setLabelCardinality("A", a)
      .setAllRelationshipsCardinality(relationships)
      .setRelationshipCardinality("()-[R]->()", r)
      .setRelationshipCardinality("(:A)-[R]->()", r)
      .setRelationshipCardinality("()-[R]->(:A)", r)
      .setRelationshipCardinality("(:A)-[R]->(:A)", r)
      .build()

    val query = "MATCH (n1:A)((inner1:A)-[r:R]->(inner2)){50,55}(n2) RETURN n1, n2, r, inner1"

    val plan = builder.plan(query)

    val trailParameters = TrailParameters(
      min = 50,
      max = Limited(55),
      start = "n1",
      end = "n2",
      innerStart = "inner1",
      innerEnd = "inner2",
      groupNodes = Set(("inner1", "inner1")),
      groupRelationships = Set(("r", "r")),
      innerRelationships = Set("r"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = false,
      expansionMode = ExpandAll,
      accumulators = Set.empty
    )

    plan should equal(
      builder.planBuilder()
        .produceResults("n1", "n2", "r", "inner1")
        .repeatTrail(trailParameters)
        .|.filter(isRepeatTrailUnique("r"))
        .|.expandAll("(inner1)-[r:R]->(inner2)")
        .|.filter("inner1:A")
        .|.argument("inner1")
        .nodeByLabelScan("n1", "A")
        .build()
    )
  }

  test(
    "Should plan to solve both endpoints using a Cartesian Product before planning the QPP when source-plan has cardinality 1 and total cost is less than the QPPAll plans"
  ) {
    val nodes = 200
    val relationships = 500
    val a = 1
    val r = 100
    val builder = plannerBuilder()
      .setAllNodesCardinality(nodes)
      .setLabelCardinality("A", a)
      .setAllRelationshipsCardinality(relationships)
      .setRelationshipCardinality("()-[R]->()", r)
      .setRelationshipCardinality("(:A)-[R]->()", r)
      .setRelationshipCardinality("()-[R]->(:A)", r)
      .setRelationshipCardinality("(:A)-[R]->(:A)", r)
      .build()

    val query = "MATCH (n1:A)((inner1:A)-[r:R]->(inner2:A)){50,55}(n2:A) RETURN n1, n2, r, inner1"

    val plan = builder.plan(query)

    val trailParameters = TrailParameters(
      min = 50,
      max = Limited(55),
      start = "n1",
      end = "n2",
      innerStart = "inner1",
      innerEnd = "inner2",
      groupNodes = Set(("inner1", "inner1")),
      groupRelationships = Set(("r", "r")),
      innerRelationships = Set("r"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = false,
      expansionMode = ExpandInto,
      accumulators = Set.empty
    )

    plan should equal(
      builder.planBuilder()
        .produceResults("n1", "n2", "r", "inner1")
        .repeatTrail(trailParameters)
        .|.filter(isRepeatTrailUnique("r"), "inner2:A")
        .|.expandAll("(inner1)-[r:R]->(inner2)")
        .|.filter("inner1:A")
        .|.argument("inner1")
        .cartesianProduct()
        .|.nodeByLabelScan("n2", "A")
        .nodeByLabelScan("n1", "A")
        .build()
    )
  }

  test(
    "Should not plan to solve both endpoints using a Cartesian Product before planning the QPP when source-plan has cardinality 1 but total cost is larger than a QPPAll plans"
  ) {
    val nodes = 200
    val relationships = 500
    val a = 1
    val r = 1
    val builder = plannerBuilder()
      .setAllNodesCardinality(nodes)
      .setLabelCardinality("A", a)
      .setAllRelationshipsCardinality(relationships)
      .setRelationshipCardinality("()-[R]->()", r)
      .setRelationshipCardinality("(:A)-[R]->()", r)
      .setRelationshipCardinality("()-[R]->(:A)", r)
      .setRelationshipCardinality("(:A)-[R]->(:A)", r)
      .build()

    val query = "MATCH (n1:A)((inner1:A)-[r:R]->(inner2:A)){50,55}(n2:A) RETURN n1, n2, r, inner1"

    val plan = builder.plan(query)

    val n1n2trailParameters = TrailParameters(
      min = 50,
      max = Limited(55),
      start = "n1",
      end = "n2",
      innerStart = "inner1",
      innerEnd = "inner2",
      groupNodes = Set(("inner1", "inner1")),
      groupRelationships = Set(("r", "r")),
      innerRelationships = Set("r"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = false,
      expansionMode = ExpandAll,
      accumulators = Set.empty
    )

    val n2n1trailParameters = n1n2trailParameters.copy(
      start = "n2",
      end = "n1",
      innerStart = "inner2",
      innerEnd = "inner1",
      reverseGroupVariableProjections = true
    )

    plan should {
      equal {
        builder.planBuilder()
          .produceResults("n1", "n2", "r", "inner1")
          .filter("n2:A")
          .repeatTrail(n1n2trailParameters)
          .|.filter(isRepeatTrailUnique("r"), "inner2:A")
          .|.expandAll("(inner1)-[r:R]->(inner2)")
          .|.filter("inner1:A")
          .|.argument("inner1")
          .nodeByLabelScan("n1", "A")
          .build()
      } or {
        equal {
          builder.planBuilder()
            .produceResults("n1", "n2", "r", "inner1")
            .filter("n1:A")
            .repeatTrail(n2n1trailParameters)
            .|.filter(isRepeatTrailUnique("r"), "inner1:A")
            .|.expandAll("(inner2)<-[r:R]-(inner1)")
            .|.filter("inner2:A")
            .|.argument("inner2")
            .nodeByLabelScan("n2", "A")
            .build()
        }
      }
    }
  }

  test("Should rewrite QPP to VarLengthExpand(Into)") {

    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setRelationshipCardinality("()-[]->()", 500)
      .build()

    val query =
      """
        |MATCH (a)-[r1]->(b), (a)-[r2]->*(b)
        |RETURN a,b,r1,r2
        |""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual planner.subPlanBuilder()
      .produceResults("a", "b", "r1", "r2")
      .filter("NOT r1 IN r2")
      .expand("(a)-[r2*0..]->(b)", expandMode = ExpandInto, projectedDir = OUTGOING)
      .allRelationshipsScan("(a)-[r1]->(b)")
      .build()
  }

  test("Should rewrite QPP to VarLengthExpand(All)") {

    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setRelationshipCardinality("()-[]->()", 500)
      .build()

    val query =
      """
        |MATCH (a)-[r1]->(b), (a)-[r2]->*(c)
        |WHERE b=c
        |RETURN a,b,c
        |""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual planner.subPlanBuilder()
      .produceResults("a", "b", "c")
      .filter("b = c", "NOT r1 IN r2")
      .expand("(a)-[r2*0..]->(c)", expandMode = ExpandAll, projectedDir = OUTGOING)
      .allRelationshipsScan("(a)-[r1]->(b)")
      .build()
  }

  test("Should rewrite QPP with predicates on start/end nodes to pruning VarExpand, directed relationship, 1..") {
    val query = "MATCH (a:User) ((n WHERE n.prop > 123)-[r]->(m WHERE m.prop < 321))+ (b) RETURN DISTINCT b"
    val plan = planner.plan(query).stripProduceResults

    plan shouldEqual planner.subPlanBuilder()
      .distinct("b AS b")
      .filter("b.prop < 321")
      .bfsPruningVarExpand(
        "(a)-[r*1..]->(b)",
        relationshipPredicates = Seq(
          Predicate("anon_0", "endNode(anon_0).prop < 321"),
          Predicate("anon_0", "startNode(anon_0).prop > 123")
        )
      )
      .filter("a.prop > 123")
      .nodeByLabelScan("a", "User")
      .build()
  }

  test("Should rewrite QPP with predicate on start/end nodes to pruning VarExpand, directed relationship, 0..") {
    val query = "MATCH (a:User) ((n WHERE n.prop > 123)-[r]->(m WHERE m.prop < 321))* (b) RETURN DISTINCT b"
    val plan = planner.plan(query).stripProduceResults

    plan shouldEqual planner.subPlanBuilder()
      .distinct("b AS b")
      .bfsPruningVarExpand(
        "(a)-[r*0..]->(b)",
        relationshipPredicates = Seq(
          Predicate("anon_0", "endNode(anon_0).prop < 321"),
          Predicate("anon_0", "startNode(anon_0).prop > 123")
        )
      )
      .nodeByLabelScan("a", "User")
      .build()
  }

  test("Should rewrite QPP with predicate on start/end nodes to pruning VarExpand, undirected relationship, 1..") {
    val query = "MATCH (a:User) ((n WHERE n.prop > 123)-[r]-(m WHERE m.prop < 321))+ (b) RETURN DISTINCT b"
    val plan = planner.plan(query).stripProduceResults

    val TO = TraversalEndpoint(tempVar = v"anon_1", endpoint = To)
    val FROM = TraversalEndpoint(tempVar = v"anon_2", endpoint = From)

    plan shouldEqual planner.subPlanBuilder()
      .distinct("b AS b")
      .filter("b.prop < 321")
      .bfsPruningVarExpandExpr(
        "(a)-[r*1..]-(b)",
        relationshipPredicates = Seq(
          VariablePredicate(v"anon_0", lessThan(prop(TO, "prop"), literalInt(321))),
          VariablePredicate(v"anon_0", greaterThan(prop(FROM, "prop"), literalInt(123)))
        )
      ).filter("a.prop > 123")
      .nodeByLabelScan("a", "User")
      .build()
  }

  test("Should rewrite QPP with predicate on start/end nodes to pruning VarExpand, undirected relationship, 0..") {
    val query = "MATCH (a:User) ((n WHERE n.prop > 123)-[r]-(m WHERE m.prop < 321))* (b) RETURN DISTINCT b"
    val plan = planner.plan(query).stripProduceResults

    val TO = TraversalEndpoint(tempVar = v"anon_1", endpoint = To)
    val FROM = TraversalEndpoint(tempVar = v"anon_2", endpoint = From)

    plan shouldEqual planner.subPlanBuilder()
      .distinct("b AS b")
      .bfsPruningVarExpandExpr(
        "(a)-[r*0..]-(b)",
        relationshipPredicates = Seq(
          VariablePredicate(v"anon_0", lessThan(prop(TO, "prop"), literalInt(321))),
          VariablePredicate(v"anon_0", greaterThan(prop(FROM, "prop"), literalInt(123)))
        )
      )
      .nodeByLabelScan("a", "User")
      .build()
  }

  /**
   * SHARDED DATABASE
   */

  test(
    "Should not plan VarExpand instead of Trail on quantified path pattern for property predicates in the sharded mode"
  ) {
    val query =
      """
        |MATCH (a:User {name: "Foo"}) (()-[:R]->(next) WHERE next.name <> "Foo")* (e)
        |RETURN DISTINCT e
        |""".stripMargin
    val plan = plannerForShardedDatabases.plan(query).stripProduceResults

    plan shouldEqual plannerForShardedDatabases.subPlanBuilder()
      .distinct("e AS e")
      .repeatTrail(TrailParameters(
        0,
        Unlimited,
        "a",
        "e",
        "anon_0",
        "next",
        Set(),
        Set(),
        Set("anon_1"),
        Set(),
        Set(),
        reverseGroupVariableProjections = false,
        expansionMode = ExpandAll,
        accumulators = Set.empty
      ))
      .|.remoteBatchPropertiesWithFilter("cacheNFromStore[next.name]")("NOT next.name = 'Foo'")
      .|.filter(isRepeatTrailUnique("anon_1"))
      .|.expandAll("(anon_0)-[anon_1:R]->(next)")
      .|.argument("anon_0")
      .remoteBatchPropertiesWithFilter("cacheNFromStore[a.name]")("a.name = 'Foo'")
      .nodeByLabelScan("a", "User")
      .build()
  }

  test(
    "Should plan VarExpand instead of Trail on quantified path pattern for non-property predicates in the sharded mode"
  ) {
    val query =
      """
        |MATCH (a:User {name: "Foo"}) (()-->(next) WHERE next:User)* (e)
        |RETURN DISTINCT e
        |""".stripMargin
    val plan = plannerForShardedDatabases.plan(query).stripProduceResults

    plan shouldEqual plannerForShardedDatabases.subPlanBuilder()
      .distinct("e AS e")
      .bfsPruningVarExpandExpr(
        "(a)-[*0..]->(e)",
        nodePredicates = Seq(),
        relationshipPredicates = Seq(VariablePredicate(v"anon_0", hasLabels(endNode("anon_0"), "User"))),
        mode = ExpandAll
      )
      .remoteBatchPropertiesWithFilter("cacheNFromStore[a.name]")("a.name = 'Foo'")
      .nodeByLabelScan("a", "User", IndexOrderNone)
      .build()
  }

  test("Should plan quantified relationship with VarExpand when runtime=Parallel") {
    val query = "MATCH (a:User)-->+(b) RETURN a, b"
    val planner = plannerBase.setExecutionModel(BatchedParallel(1, 100)).build()
    val plan = planner.plan(query)

    plan.folder.findAllByClass[VarExpand].size shouldBe 1
  }

  test("Should plan quantified path pattern with VarExpand when runtime=Parallel") {
    val query = "MATCH p = ((a)-[:R]->(b) WHERE a.p < b.p){1,10} RETURN p"
    val planner = plannerBase.setExecutionModel(BatchedParallel(1, 100)).build()
    val plan = planner.plan(query)

    plan.folder.findAllByClass[VarExpand].size shouldBe 1
  }

  test("Should plan quantified relationship with VarExpand when runtime is single-threaded") {
    val query = "MATCH (a:User)-->+(b) RETURN a, b"
    val planner = plannerBase.setExecutionModel(Volcano).build()
    val plan = planner.plan(query)

    plan.folder.findAllByClass[VarExpand].size shouldBe 1
  }

  test("Should plan quantified path pattern with VarExpand when runtime is single-threaded") {
    val query = "MATCH p = ((a)-[:R]->(b) WHERE a.p < b.p){1,10} RETURN p"
    val planner = plannerBase.setExecutionModel(Volcano).build()
    val plan = planner.plan(query)

    plan.folder.findAllByClass[VarExpand].size shouldBe 1
  }

  test("Should plan simple bound quantifier under REPEATABLE ELEMENTS") {
    versionsExcept5 { version =>
      val query =
        """MATCH REPEATABLE ELEMENTS
          |  (u:User)((n)-[]->(m)) {, 100}
          |RETURN n, m""".stripMargin
      val `(u)((n)-[]-(m)) {, 100}` = WalkParameters(
        min = 0,
        max = Limited(100),
        start = "u",
        end = "anon_0",
        innerStart = "n",
        innerEnd = "m",
        groupNodes = Set(("n", "n"), ("m", "m")),
        groupRelationships = Set.empty,
        reverseGroupVariableProjections = false,
        innerRelationships = Set("anon_1"),
        expansionMode = ExpandAll,
        accumulators = Set.empty
      )

      planner.plan(version, query) should equal(
        planner.planBuilder()
          .produceResults("n", "m")
          .repeatWalk(`(u)((n)-[]-(m)) {, 100}`)
          .|.expand("(n)-[anon_1]->(m)")
          .|.argument("n")
          .nodeByLabelScan("u", "User")
          .build()
      )
    }
  }

  test("Should plan bound quantifiers on 2 qpps with 2 relationships each under REPEATABLE ELEMENTS") {
    versionsExcept5 { version =>
      val query =
        """MATCH REPEATABLE ELEMENTS
          |  (u:User)
          |    ((n)-[]->(m)-[]->(o)) {, 100}
          |  (p)
          |    ((r)-[]->(s)-[]->(t)) {, 100}
          |RETURN u, t""".stripMargin
      val `(u)((n)-...-(o)) {, 100}(p)` = WalkParameters(
        min = 0,
        max = Limited(100),
        start = "u",
        end = "p",
        innerStart = "n",
        innerEnd = "o",
        groupNodes = Set.empty,
        groupRelationships = Set.empty,
        reverseGroupVariableProjections = false,
        innerRelationships = Set("anon_1", "anon_2"),
        expansionMode = ExpandAll,
        accumulators = Set.empty
      )
      val `(p)((r)-...-(t)) {, 100}` = WalkParameters(
        min = 0,
        max = Limited(100),
        start = "p",
        end = "anon_0",
        innerStart = "r",
        innerEnd = "t",
        groupNodes = Set(("t", "t")),
        groupRelationships = Set.empty,
        reverseGroupVariableProjections = false,
        innerRelationships = Set("anon_3", "anon_4"),
        expansionMode = ExpandAll,
        accumulators = Set.empty
      )

      planner.plan(version, query) should equal(
        planner.planBuilder()
          .produceResults("u", "t")
          .repeatWalk(`(p)((r)-...-(t)) {, 100}`)
          .|.expand("(s)-[anon_4]->(t)")
          .|.expand("(r)-[anon_3]->(s)")
          .|.argument("r")
          .repeatWalk(`(u)((n)-...-(o)) {, 100}(p)`)
          .|.expand("(m)-[anon_2]->(o)")
          .|.expand("(n)-[anon_1]->(m)")
          .|.argument("n")
          .nodeByLabelScan("u", "User")
          .build()
      )
    }
  }
}

trait QuantifiedPathPatternBlockSpecificPlanningIntegrationTestBase {
  this: QuantifiedPathPatternPlanningIntegrationTestBase =>

  relationshipPredicatesWithPropertyAccess.foreach {
    case RelationshipPredicate(queryPredicate, plannedType, plannedPredicates @ _*) =>
      val filterExpressions: Seq[ToExpression] =
        plannedPredicates.map[ToExpression](p => p.predicate.replace(p.entity, "r")) :+
          isRepeatTrailUnique("r")

      test(
        s"Should not plan VarExpand instead of Trail on quantified path pattern with relationship predicate $queryPredicate"
      ) {
        val query = s"MATCH (a) ((n)-[r $queryPredicate]->(m))+ (b) RETURN r"

        val trailParameters = TrailParameters(
          min = 1,
          max = Unlimited,
          start = "a",
          end = "b",
          innerStart = "n",
          innerEnd = "m",
          groupNodes = Set(),
          groupRelationships = Set(("r", "r")),
          innerRelationships = Set("r"),
          previouslyBoundRelationships = Set(),
          previouslyBoundRelationshipGroups = Set(),
          reverseGroupVariableProjections = false,
          expansionMode = ExpandAll,
          accumulators = Set.empty
        )
        val plan = planner.plan(query).stripProduceResults

        plan shouldEqual planner.subPlanBuilder()
          .repeatTrail(trailParameters)
          .|.filter(filterExpressions: _*)
          .|.expandAll("(n)-[r]->(m)")
          .|.argument("n")
          .allNodeScan("a")
          .build()
      }

      test(
        s"Should not plan VarExpand instead of Trail on quantified relationship with relationship predicate $queryPredicate"
      ) {
        val query = s"MATCH (a)-[r $queryPredicate]->+(b) RETURN r"
        val plan = planner.plan(query).stripProduceResults

        val trailParameters = TrailParameters(
          min = 1,
          max = Unlimited,
          start = "a",
          end = "b",
          innerStart = "anon_0",
          innerEnd = "anon_1",
          groupNodes = Set(),
          groupRelationships = Set(("r", "r")),
          innerRelationships = Set("r"),
          previouslyBoundRelationships = Set(),
          previouslyBoundRelationshipGroups = Set(),
          reverseGroupVariableProjections = false,
          expansionMode = ExpandAll,
          accumulators = Set.empty
        )
        plan shouldEqual planner.subPlanBuilder()
          .repeatTrail(trailParameters)
          .|.filter(filterExpressions: _*)
          .|.expandAll("(anon_0)-[r]->(anon_1)")
          .|.argument("anon_0")
          .allNodeScan("a")
          .build()
      }
  }

  relationshipPredicatesWithPropertyAccess.foreach {
    case RelationshipPredicate(queryPredicate, plannedType, plannedPredicates @ _*) =>
      test(
        s"Should plan VarExpand instead of Trail on quantified path pattern with relationship predicate $queryPredicate, Volcano execution model"
      ) {
        val planner = plannerBase
          .setExecutionModel(ExecutionModel.Volcano)
          .build()

        val query = s"MATCH (a) ((n)-[r $queryPredicate]->(m))+ (b) RETURN r"
        val plan = planner.plan(query).stripProduceResults
        plan shouldEqual planner.subPlanBuilder()
          .expand(s"(a)-[r$plannedType*1..]->()", relationshipPredicates = plannedPredicates)
          .allNodeScan("a")
          .build()
      }

      test(
        s"Should plan VarExpand instead of Trail on quantified relationship with relationship predicate $queryPredicate, Volcano execution model"
      ) {
        val planner = plannerBase
          .setExecutionModel(ExecutionModel.Volcano)
          .build()

        val query = s"MATCH (a)-[r $queryPredicate]->+(b) RETURN r"
        val plan = planner.plan(query).stripProduceResults
        plan shouldEqual planner.subPlanBuilder()
          .expand(s"(a)-[r$plannedType*1..]->()", relationshipPredicates = plannedPredicates.toList)
          .allNodeScan("a")
          .build()
      }
  }
}

trait QuantifiedPathPatternAlignedSpecificPlanningIntegrationTestBase {
  this: QuantifiedPathPatternPlanningIntegrationTestBase =>

  test("Should rewrite QPP with predicate r.prop=ri.prop on directed relationship to VarLengthExpand(All)") {

    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setRelationshipCardinality("()-[]->()", 500)
      .build()

    val query =
      """
        |MATCH (lo {id:1})((li)-[r]->(ri) WHERE r.prop=ri.prop)*(ro)
        |RETURN lo, ro
        |""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual planner.subPlanBuilder()
      .produceResults("lo", "ro")
      .expand(
        "(lo)-[*0..]->(ro)",
        relationshipPredicates = Seq(Predicate("anon_0", "anon_0.prop = endNode(anon_0).prop"))
      )
      .filter("lo.id = 1")
      .allNodeScan("lo")
      .build()
  }

  test("Should rewrite QPP with predicate r.prop=ri.prop on undirected relationship to VarLengthExpand(All)") {

    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setRelationshipCardinality("()-[]->()", 500)
      .build()

    val query =
      """
        |MATCH (lo {id:1})((li)-[r]-(ri) WHERE r.prop=ri.prop)*(ro)
        |RETURN lo, ro
        |""".stripMargin

    val plan = planner.plan(query)
    plan shouldEqual planner.subPlanBuilder()
      .produceResults("lo", "ro")
      .expandExpr(
        "(lo)-[*0..]-(ro)",
        relationshipPredicates = Seq(
          VariablePredicate(
            v"anon_0",
            equals(
              prop("anon_0", "prop"),
              propExpression(TraversalEndpoint(v"anon_1", To), "prop")
            )
          )
        )
      )
      .filter("lo.id = 1")
      .allNodeScan("lo")
      .build()
  }

  test("Should rewrite QPP with predicate li.prop=ri.prop on undirected relationship to VarLengthExpand(All)") {

    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setRelationshipCardinality("()-[]->()", 500)
      .build()

    val query =
      """
        |MATCH (lo {id:1})((li)-[r]-(ri) WHERE li.prop=ri.prop)*(ro)
        |RETURN lo, ro
        |""".stripMargin

    val plan = planner.plan(query)
    plan shouldEqual planner.subPlanBuilder()
      .produceResults("lo", "ro")
      .expandExpr(
        "(lo)-[*0..]-(ro)",
        relationshipPredicates = Seq(
          VariablePredicate(
            v"anon_0",
            equals(
              propExpression(TraversalEndpoint(v"anon_1", From), "prop"),
              propExpression(TraversalEndpoint(v"anon_2", To), "prop")
            )
          )
        )
      )
      .filter("lo.id = 1")
      .allNodeScan("lo")
      .build()
  }

  test("Should rewrite QPP with predicate li.prop=r.prop on undirected relationship to VarLengthExpand(All)") {

    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setRelationshipCardinality("()-[]->()", 500)
      .build()

    val query =
      """
        |MATCH (lo {id:1})((li)-[r]-(ri) WHERE li.prop=r.prop)*(ro)
        |RETURN lo, ro
        |""".stripMargin

    val plan = planner.plan(query)
    plan shouldEqual planner.subPlanBuilder()
      .produceResults("lo", "ro")
      .expandExpr(
        "(lo)-[*0..]-(ro)",
        relationshipPredicates = Seq(
          VariablePredicate(
            v"anon_0",
            equals(
              propExpression(TraversalEndpoint(v"anon_1", From), "prop"),
              prop("anon_0", "prop")
            )
          )
        )
      )
      .filter("lo.id = 1")
      .allNodeScan("lo")
      .build()
  }

  test("parallelRepeatHeuristic=enabled preserves Repeat in parallel runtime with AtMostOneRow LHS") {
    val builder = plannerBuilder()
      .setAllNodesCardinality(200)
      .setLabelCardinality("A", 1)
      .setAllRelationshipsCardinality(100)
      .setRelationshipCardinality("()-[R]->()", 100)
      .setRelationshipCardinality("(:A)-[:R]->()", 100)
      .addNodeIndex("A", Seq("prop"), existsSelectivity = 1.0, uniqueSelectivity = 1.0, isUnique = true)
      .setExecutionModel(BatchedParallel(1, 2))
      .setParallelRepeatHeuristic(CypherParallelRepeatHeuristicOption.enabled)
      .build()

    val query = "MATCH (a:A {prop: 123}) ((n)-[r:R]->(m))+ (b) RETURN a, b"

    val `(a) ((n)-[r:R]->(m))+ (b)`: TrailParameters = TrailParameters(
      min = 1,
      max = Unlimited,
      start = "a",
      end = "b",
      innerStart = "n",
      innerEnd = "m",
      groupNodes = Set.empty,
      groupRelationships = Set.empty,
      innerRelationships = Set("r"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = false,
      expansionMode = ExpandAll,
      accumulators = Set.empty
    )

    val plan = builder.plan(query)

    plan should equal(
      builder.planBuilder()
        .produceResults(column("a", "cacheN[a.prop]"), column("b"))
        .repeatTrail(`(a) ((n)-[r:R]->(m))+ (b)`)
        .|.filter(isRepeatTrailUnique("r"))
        .|.expandAll("(n)-[r:R]->(m)")
        .|.argument("n")
        .nodeIndexOperator("a:A(prop = 123)", _ => GetValue, unique = true)
        .build()
    )
  }

  test("parallelRepeatHeuristic=disabled (default) rewrites Repeat to VarExpand in parallel runtime") {
    val builder = plannerBuilder()
      .setAllNodesCardinality(200)
      .setLabelCardinality("A", 1)
      .setAllRelationshipsCardinality(100)
      .setRelationshipCardinality("(:A)-[:R]->()", 100)
      .setRelationshipCardinality("()-[R]->()", 100)
      .addNodeIndex("A", Seq("prop"), existsSelectivity = 1.0, uniqueSelectivity = 1.0, isUnique = true)
      .setExecutionModel(BatchedParallel(1, 2))
      .build()

    val plan = builder.plan("MATCH (a:A {prop: 123}) ((n)-[r:R]->(m))+ (b) RETURN a, b")

    plan should equal(
      builder.planBuilder()
        .produceResults(column("a", "cacheN[a.prop]"), column("b"))
        .expand("(a)-[:R*1..]->(b)")
        .nodeIndexOperator("a:A(prop = 123)", _ => GetValue, unique = true)
        .build()
    )
  }

  relationshipPredicatesWithPropertyAccess.foreach {
    case RelationshipPredicate(queryPredicate, plannedType, plannedPredicates @ _*) =>
      test(
        s"Should plan VarExpand instead of Trail on quantified path pattern with relationship predicate $queryPredicate"
      ) {
        val query = s"MATCH (a) ((n)-[r $queryPredicate]->(m))+ (b) RETURN r"
        val plan = planner.plan(query).stripProduceResults
        plan shouldEqual planner.subPlanBuilder()
          .expand(s"(a)-[r$plannedType*1..]->()", relationshipPredicates = plannedPredicates)
          .allNodeScan("a")
          .build()
      }

      test(
        s"Should plan VarExpand instead of Trail on quantified relationship with relationship predicate $queryPredicate"
      ) {
        val query = s"MATCH (a)-[r $queryPredicate]->+(b) RETURN r"
        val plan = planner.plan(query).stripProduceResults
        plan shouldEqual planner.subPlanBuilder()
          .expand(s"(a)-[r$plannedType*1..]->()", relationshipPredicates = plannedPredicates.toList)
          .allNodeScan("a")
          .build()
      }
  }
}

object QuantifiedPathPatternPlanningIntegrationTestBase {

  case class RelationshipPredicate(
    queryPredicate: String,
    plannedType: String,
    plannedPredicates: Predicate*
  )

  val relationshipPredicatesWithoutPropertyAccess: Seq[RelationshipPredicate] = Seq(
    RelationshipPredicate(":R", ":R"),
    RelationshipPredicate(":%", ""),
    RelationshipPredicate(":%|R", ""),
    RelationshipPredicate(":R|T", ":R|T"),
    RelationshipPredicate("WHERE r = 0", "", Predicate("anon_0", "anon_0 = 0")),
    RelationshipPredicate("WHERE r IS NOT NULL", "", Predicate("anon_0", "anon_0 IS NOT NULL"))
  )

  val relationshipPredicatesWithPropertyAccess: Seq[RelationshipPredicate] = Seq(
    RelationshipPredicate("{p: 0}", "", Predicate("anon_0", "anon_0.p = 0")),
    RelationshipPredicate("WHERE r.p = 0", "", Predicate("anon_0", "anon_0.p = 0")),
    RelationshipPredicate("WHERE r.p <> 0", "", Predicate("anon_0", "NOT anon_0.p = 0")),
    RelationshipPredicate("WHERE r.p <= 0", "", Predicate("anon_0", "anon_0.p <= 0")),
    RelationshipPredicate("WHERE NOT r.p = 0", "", Predicate("anon_0", "NOT anon_0.p = 0")),
    RelationshipPredicate("WHERE r.p IN [0, 1, 2]", "", Predicate("anon_0", "anon_0.p IN [0, 1, 2]")),
    RelationshipPredicate("WHERE r.p IS NULL", "", Predicate("anon_0", "anon_0.p IS NULL")),
    RelationshipPredicate("WHERE r.p IS NOT NULL", "", Predicate("anon_0", "anon_0.p IS NOT NULL")),
    RelationshipPredicate(
      "WHERE r.p = 0 OR r.pp IS NOT NULL",
      "",
      Predicate("anon_0", "anon_0.p = 0 OR anon_0.pp IS NOT NULL")
    ),
    RelationshipPredicate(
      "WHERE r.p = 0 OR NOT r.pp = 0",
      "",
      Predicate("anon_0", "anon_0.p = 0 OR NOT anon_0.pp = 0")
    ),
    RelationshipPredicate(
      "WHERE r.p = 0 AND r.pp > 0",
      "",
      Predicate("anon_0", "anon_0.p = 0"),
      Predicate("anon_0", "anon_0.pp > 0")
    ),
    RelationshipPredicate(
      "WHERE r.p = 0 AND r IS NULL",
      "",
      Predicate("anon_0", "anon_0.p = 0"),
      Predicate("anon_0", "anon_0 IS NULL")
    )
  )
}
