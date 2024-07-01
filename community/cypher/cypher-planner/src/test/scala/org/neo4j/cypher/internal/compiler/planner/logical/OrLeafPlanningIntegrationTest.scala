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

import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanConstructionTestSupport
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningIntegrationTestSupport
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfigurationBuilder
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.column
import org.neo4j.cypher.internal.logical.plans.Distinct
import org.neo4j.cypher.internal.logical.plans.GetValue
import org.neo4j.cypher.internal.logical.plans.IndexOrderAscending
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.logical.plans.IndexSeek.nodeIndexSeek
import org.neo4j.cypher.internal.logical.plans.NodeIndexSeek
import org.neo4j.cypher.internal.logical.plans.Selection
import org.neo4j.cypher.internal.logical.plans.Union
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.test_helpers.CypherScalaCheckDrivenPropertyChecks
import org.neo4j.graphdb.schema.IndexType
import org.scalacheck.Gen

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class OrLeafPlanningIntegrationTest
    extends CypherFunSuite
    with LogicalPlanningIntegrationTestSupport
    with AstConstructionTestSupport
    with LogicalPlanConstructionTestSupport
    with CypherScalaCheckDrivenPropertyChecks {

  private def plannerConfig(): StatisticsBackedLogicalPlanningConfigurationBuilder =
    plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("L", 50)
      .setLabelCardinality("P", 50)
      .setAllRelationshipsCardinality(10)
      .setRelationshipCardinality("()-[:REL1]->()", 5)
      .setRelationshipCardinality("()-[:REL2]->()", 5)

  // This config favors linear plans without union/distinct in most cases.
  private def hintPlannerConfig(): StatisticsBackedLogicalPlanningConfigurationBuilder =
    plannerBuilder()
      .setAllNodesCardinality(1)
      .setLabelCardinality("L", 1)
      .setLabelCardinality("P", 1)
      .setAllRelationshipsCardinality(2)
      .setRelationshipCardinality("()-[:REL1]->()", 1)
      .setRelationshipCardinality("()-[:REL2]->()", 1)

  test("should work with index seeks of property disjunctions") {
    val cfg = plannerConfig()
      .addNodeIndex("L", Seq("p1"), 0.5, 0.5)
      .addNodeIndex("L", Seq("p2"), 0.5, 0.5)
      .build()

    val plan = cfg.plan(
      """MATCH (n)
        |WHERE n:L AND (n.p1 = 1 OR n.p2 = 2)
        |RETURN n""".stripMargin
    )

    plan should equal(
      cfg.planBuilder()
        .produceResults(column("n", "cacheN[n.p1]", "cacheN[n.p2]"))
        .distinct("n AS n")
        .union()
        .|.nodeIndexOperator("n:L(p2 = 2)", _ => GetValue, indexType = IndexType.RANGE)
        .nodeIndexOperator("n:L(p1 = 1)", _ => GetValue, indexType = IndexType.RANGE)
        .build()
    )(SymmetricalLogicalPlanEquality)
  }

  test("should work with index range seeks of property disjunctions") {
    val cfg = plannerConfig()
      .addNodeIndex("L", Seq("p1"), 0.5, 0.5)
      .addNodeIndex("L", Seq("p2"), 0.5, 0.5)
      .build()

    val plan = cfg.plan(
      """MATCH (n)
        |WHERE n:L AND (n.p1 > 3 OR n.p2 < 7)
        |RETURN n""".stripMargin
    )

    plan should equal(
      cfg.planBuilder()
        .produceResults(column("n", "cacheN[n.p1]", "cacheN[n.p2]"))
        .distinct("n AS n")
        .union()
        .|.nodeIndexOperator("n:L(p2 < 7)", _ => GetValue, indexType = IndexType.RANGE)
        .nodeIndexOperator("n:L(p1 > 3)", _ => GetValue, indexType = IndexType.RANGE)
        .build()
    )(SymmetricalLogicalPlanEquality)
  }

  test("should work with index range-between seeks of property disjunctions") {
    val cfg = plannerConfig()
      .addNodeIndex("L", Seq("p1"), 0.5, 0.5)
      .addNodeIndex("L", Seq("p2"), 0.5, 0.5)
      .build()

    val plan = cfg.plan(
      """MATCH (n)
        |WHERE n:L AND (10 > n.p1 > 3 OR 3 < n.p2 < 7)
        |RETURN n""".stripMargin
    )

    plan should equal(
      cfg.planBuilder()
        .produceResults(column("n", "cacheN[n.p1]", "cacheN[n.p2]"))
        .distinct("n AS n")
        .union()
        .|.nodeIndexOperator("n:L(3 < p2 < 7)", _ => GetValue, indexType = IndexType.RANGE)
        .nodeIndexOperator("n:L(3 < p1 < 10)", _ => GetValue, indexType = IndexType.RANGE)
        .build()
    )(SymmetricalLogicalPlanEquality)
  }

  test("should work with index seeks of property disjunctions with label conjunction") {
    val cfg = plannerConfig()
      .addNodeIndex("L", Seq("p1"), 0.5, 0.5)
      .addNodeIndex("L", Seq("p2"), 0.5, 0.5)
      .build()

    val plan = cfg.plan(
      """MATCH (n:L:P)
        |WHERE n.p1 = 1 OR n.p2 = 2
        |RETURN n""".stripMargin
    )

    plan should equal(
      cfg.planBuilder()
        .produceResults(column("n", "cacheN[n.p1]", "cacheN[n.p2]"))
        .distinct("n AS n")
        .union()
        .|.filterExpression(hasLabels("n", "P"))
        .|.nodeIndexOperator("n:L(p2 = 2)", _ => GetValue, indexType = IndexType.RANGE)
        .filterExpression(hasLabels("n", "P"))
        .nodeIndexOperator("n:L(p1 = 1)", _ => GetValue, indexType = IndexType.RANGE)
        .build()
    )(SymmetricalLogicalPlanEquality)
  }

  test("should work with index seeks of property disjunctions with label conjunction and solve single index hint") {
    val cfg = hintPlannerConfig()
      .addNodeIndex("L", Seq("p1"), 1.0, 1.0)
      .addNodeIndex("L", Seq("p2"), 1.0, 1.0)
      .build()

    def plan(withHint: Boolean = true) = cfg.plan(
      s"""MATCH (n:L:P)
         |${if (withHint) "" else "//"} USING INDEX n:L(p1)
         |WHERE n.p1 = 1 OR n.p2 = 2
         |RETURN n""".stripMargin
    )

    plan() should equal(
      cfg.planBuilder()
        .produceResults(column("n", "cacheN[n.p1]", "cacheN[n.p2]"))
        .distinct("n AS n")
        .union()
        .|.filterExpression(hasLabels("n", "P"))
        .|.nodeIndexOperator("n:L(p2 = 2)", _ => GetValue, indexType = IndexType.RANGE)
        .filterExpression(hasLabels("n", "P"))
        .nodeIndexOperator("n:L(p1 = 1)", _ => GetValue, indexType = IndexType.RANGE)
        .build()
    )(SymmetricalLogicalPlanEquality)
    plan() should not equal plan(withHint = false)
  }

  test("should work with index seeks of property disjunctions with label conjunction and solve two index hints") {
    val cfg = hintPlannerConfig()
      .addNodeIndex("L", Seq("p1"), 1.0, 1.0)
      .addNodeIndex("L", Seq("p2"), 1.0, 1.0)
      .build()

    def plan(withHint: Boolean = true) = cfg.plan(
      s"""MATCH (n:L:P)
         |${if (withHint) "" else "//"} USING INDEX n:L(p1)
         |${if (withHint) "" else "//"} USING INDEX n:L(p2)
         |WHERE n.p1 = 1 OR n.p2 = 2
         |RETURN n""".stripMargin
    )

    plan() should equal(
      cfg.planBuilder()
        .produceResults(column("n", "cacheN[n.p1]", "cacheN[n.p2]"))
        .distinct("n AS n")
        .union()
        .|.filterExpression(hasLabels("n", "P"))
        .|.nodeIndexOperator("n:L(p2 = 2)", _ => GetValue, indexType = IndexType.RANGE)
        .filterExpression(hasLabels("n", "P"))
        .nodeIndexOperator("n:L(p1 = 1)", _ => GetValue, indexType = IndexType.RANGE)
        .build()
    )(SymmetricalLogicalPlanEquality)
    plan() should not equal plan(withHint = false)
  }

  test(
    "should work with index seeks of property disjunctions with label conjunction, where every combination is indexed"
  ) {
    val cfg = plannerConfig()
      .addNodeIndex("L", Seq("p1"), 0.5, 0.5)
      .addNodeIndex("L", Seq("p2"), 0.5, 0.5)
      .addNodeIndex("P", Seq("p1"), 0.5, 0.5)
      .addNodeIndex("P", Seq("p2"), 0.5, 0.5)
      .build()

    val plan = cfg.plan(
      """MATCH (n:L:P)
        |WHERE n.p1 = 1 OR n.p2 = 2
        |RETURN n""".stripMargin
    ).stripProduceResults

    val hasL = hasLabels("n", "L")
    val hasP = hasLabels("n", "P")

    val seekLProp1 = nodeIndexSeek("n:L(p1 = 1)", _ => GetValue)
    val seekLProp2 = nodeIndexSeek("n:L(p2 = 2)", _ => GetValue, propIds = Some(Map("p2" -> 1)))
    val seekPProp1 = nodeIndexSeek("n:P(p1 = 1)", _ => GetValue, labelId = 1)
    val seekPProp2 = nodeIndexSeek("n:P(p2 = 2)", _ => GetValue, labelId = 1, propIds = Some(Map("p2" -> 1)))

    val coveringCombinations = Seq(
      (seekLProp1, hasP),
      (seekLProp2, hasP),
      (seekPProp1, hasL),
      (seekPProp2, hasL)
    )

    val planAlternatives = for {
      Seq((seek1, filter1), (seek2, filter2)) <- coveringCombinations.permutations.map(_.take(2)).toSeq
    } yield Distinct(Union(Selection(Seq(filter1), seek1), Selection(Seq(filter2), seek2)), Map(v"n" -> v"n"))

    planAlternatives should contain(plan)
  }

  test("should work with relationship index seeks of property disjunctions") {
    val cfg = plannerConfig()
      .addRelationshipIndex("REL1", Seq("p1"), 0.5, 0.5)
      .addRelationshipIndex("REL1", Seq("p2"), 0.5, 0.5)
      .build()

    val plan = cfg.plan(
      """MATCH (a)-[r:REL1]-(b)
        |WHERE r.p1 = 1 OR r.p2 = 2
        |RETURN r""".stripMargin
    )

    plan should equal(
      cfg.planBuilder()
        .produceResults(column("r", "cacheR[r.p2]", "cacheR[r.p1]"))
        .distinct("r AS r", "a AS a", "b AS b")
        .union()
        .|.relationshipIndexOperator("(a)-[r:REL1(p2 = 2)]-(b)", _ => GetValue, indexType = IndexType.RANGE)
        .relationshipIndexOperator("(a)-[r:REL1(p1 = 1)]-(b)", _ => GetValue, indexType = IndexType.RANGE)
        .build()
    )(SymmetricalLogicalPlanEquality)
  }

  test("should work with relationship index seeks of property disjunctions and solve single index hint") {
    val cfg = hintPlannerConfig()
      .addRelationshipIndex("REL1", Seq("p1"), 1.0, 1.0)
      .addRelationshipIndex("REL1", Seq("p2"), 1.0, 1.0)
      .build()

    def plan(withHint: Boolean = true) = cfg.plan(
      s"""MATCH (a)-[r:REL1]-(b)
         |${if (withHint) "" else "//"} USING INDEX r:REL1(p1)
         |WHERE r.p1 = 1 OR r.p2 = 2
         |RETURN r""".stripMargin
    )

    plan() should equal(
      cfg.planBuilder()
        .produceResults(column("r", "cacheR[r.p2]", "cacheR[r.p1]"))
        .distinct("r AS r", "a AS a", "b AS b")
        .union()
        .|.relationshipIndexOperator("(a)-[r:REL1(p2 = 2)]-(b)", _ => GetValue, indexType = IndexType.RANGE)
        .relationshipIndexOperator("(a)-[r:REL1(p1 = 1)]-(b)", _ => GetValue, indexType = IndexType.RANGE)
        .build()
    )(SymmetricalLogicalPlanEquality)
    plan() should not equal plan(withHint = false)
  }

  test("should work with relationship index seeks of property disjunctions and solve two index hints") {
    val cfg = hintPlannerConfig()
      .addRelationshipIndex("REL1", Seq("p1"), 1.0, 1.0)
      .addRelationshipIndex("REL1", Seq("p2"), 1.0, 1.0)
      .build()

    def plan(withHint: Boolean = true) = cfg.plan(
      s"""MATCH (a)-[r:REL1]-(b)
         |${if (withHint) "" else "//"} USING INDEX r:REL1(p1)
         |${if (withHint) "" else "//"} USING INDEX r:REL1(p2)
         |WHERE r.p1 = 1 OR r.p2 = 2
         |RETURN r""".stripMargin
    )

    plan() should equal(
      cfg.planBuilder()
        .produceResults(column("r", "cacheR[r.p2]", "cacheR[r.p1]"))
        .distinct("r AS r", "a AS a", "b AS b")
        .union()
        .|.relationshipIndexOperator("(a)-[r:REL1(p2 = 2)]-(b)", _ => GetValue, indexType = IndexType.RANGE)
        .relationshipIndexOperator("(a)-[r:REL1(p1 = 1)]-(b)", _ => GetValue, indexType = IndexType.RANGE)
        .build()
    )(SymmetricalLogicalPlanEquality)
    plan() should not equal plan(withHint = false)
  }

  test("should work with index seeks of label disjunctions") {
    val cfg = plannerConfig()
      .addNodeIndex("L", Seq("p1"), 0.5, 0.5)
      .addNodeIndex("P", Seq("p1"), 0.5, 0.5)
      .build()

    val plan = cfg.plan(
      """MATCH (n)
        |WHERE (n:L OR n:P) AND (n.p1 = 1)
        |RETURN n""".stripMargin
    )

    // Possible improvement: We could have planned this as OrderedDistinct
    plan should equal(
      cfg.planBuilder()
        .produceResults(column("n", "cacheN[n.p1]"))
        .distinct("n AS n")
        .union()
        .|.nodeIndexOperator("n:P(p1 = 1)", _ => GetValue, indexType = IndexType.RANGE)
        .nodeIndexOperator("n:L(p1 = 1)", _ => GetValue, indexType = IndexType.RANGE)
        .build()
    )(SymmetricalLogicalPlanEquality)
  }

  test("should work with index seeks of label disjunctions and solve single index hint") {
    val cfg = hintPlannerConfig()
      .addNodeIndex("L", Seq("p1"), 1.0, 1.0)
      .addNodeIndex("P", Seq("p1"), 1.0, 1.0)
      .build()

    val plan = cfg.plan(
      """MATCH (n)
        |USING INDEX n:L(p1)
        |WHERE (n:L OR n:P) AND (n.p1 = 1)
        |RETURN n""".stripMargin
    )

    // Possible improvement: We could have planned this as OrderedDistinct
    // It is impossible to make up statistics where an AllNodeScan would be better, so we will get the same plan even without the hint
    plan should equal(
      cfg.planBuilder()
        .produceResults(column("n", "cacheN[n.p1]"))
        .distinct("n AS n")
        .union()
        .|.nodeIndexOperator("n:P(p1 = 1)", _ => GetValue, indexType = IndexType.RANGE)
        .nodeIndexOperator("n:L(p1 = 1)", _ => GetValue, indexType = IndexType.RANGE)
        .build()
    )(SymmetricalLogicalPlanEquality)
  }

  test("should work with index seeks of label disjunctions and solve two index hints") {
    val cfg = hintPlannerConfig()
      .addNodeIndex("L", Seq("p1"), 1.0, 1.0)
      .addNodeIndex("P", Seq("p1"), 1.0, 1.0)
      .build()

    val plan = cfg.plan(
      """MATCH (n)
        |USING INDEX n:L(p1)
        |USING INDEX n:P(p1)
        |WHERE (n:L OR n:P) AND (n.p1 = 1)
        |RETURN n""".stripMargin
    )

    // Possible improvement: We could have planned this as OrderedDistinct
    // It is impossible to make up statistics where an AllNodeScan would be better, so we will get the same plan even without the hint
    plan should equal(
      cfg.planBuilder()
        .produceResults(column("n", "cacheN[n.p1]"))
        .distinct("n AS n")
        .union()
        .|.nodeIndexOperator("n:P(p1 = 1)", _ => GetValue, indexType = IndexType.RANGE)
        .nodeIndexOperator("n:L(p1 = 1)", _ => GetValue, indexType = IndexType.RANGE)
        .build()
    )(SymmetricalLogicalPlanEquality)
  }

  test("should work with label scan + filter on one side of label disjunctions if there is only one index") {
    val cfg = plannerConfig()
      .addNodeIndex("L", Seq("p1"), 0.5, 0.5)
      .build()

    val plan = cfg.plan(
      """MATCH (n)
        |WHERE (n:L OR n:P) AND (n.p1 < 1)
        |RETURN n""".stripMargin
    )

    plan should equal(
      cfg.planBuilder()
        .produceResults(column("n", "cacheN[n.p1]"))
        .distinct("n AS n")
        .union()
        .|.filter("cacheNFromStore[n.p1] < 1")
        .|.nodeByLabelScan("n", "P", IndexOrderAscending)
        .nodeIndexOperator("n:L(p1 < 1)", _ => GetValue, indexType = IndexType.RANGE)
        .build()
    )(SymmetricalLogicalPlanEquality)
  }

  test("should work with relationship index seeks of relationship type disjunctions") {
    val cfg = plannerConfig()
      .addRelationshipIndex("REL1", Seq("p1"), 0.5, 0.5)
      .addRelationshipIndex("REL2", Seq("p1"), 0.5, 0.5)
      .build()

    val plan = cfg.plan(
      """MATCH (a)-[r]-(b)
        |WHERE (r:REL1 OR r:REL2) AND (r.p1 = 1)
        |RETURN r""".stripMargin
    )

    // Possible improvement: We could have planned this as OrderedDistinct
    plan should equal(
      cfg.planBuilder()
        .produceResults(column("r", "cacheR[r.p1]"))
        .distinct("r AS r", "a AS a", "b AS b")
        .union()
        .|.relationshipIndexOperator("(a)-[r:REL2(p1 = 1)]-(b)", _ => GetValue, indexType = IndexType.RANGE)
        .relationshipIndexOperator("(a)-[r:REL1(p1 = 1)]-(b)", _ => GetValue, indexType = IndexType.RANGE)
        .build()
    )(SymmetricalLogicalPlanEquality)
  }

  test("should work with relationship index seeks of relationship type disjunctions and solve single index hint") {
    val cfg = hintPlannerConfig()
      .addRelationshipIndex("REL1", Seq("p1"), 1.0, 1.0)
      .addRelationshipIndex("REL2", Seq("p1"), 1.0, 1.0)
      .build()

    def plan(withHint: Boolean = true) = cfg.plan(
      s"""MATCH (a)-[r]-(b)
         |${if (withHint) "" else "//"} USING INDEX r:REL2(p1)
         |WHERE (r:REL1 OR r:REL2) AND (r.p1 = 1)
         |RETURN r""".stripMargin
    )

    // Possible improvement: We could have planned this as OrderedDistinct
    plan() should equal(
      cfg.planBuilder()
        .produceResults(column("r", "cacheR[r.p1]"))
        .distinct("r AS r", "a AS a", "b AS b")
        .union()
        .|.relationshipIndexOperator("(a)-[r:REL2(p1 = 1)]-(b)", _ => GetValue, indexType = IndexType.RANGE)
        .relationshipIndexOperator("(a)-[r:REL1(p1 = 1)]-(b)", _ => GetValue, indexType = IndexType.RANGE)
        .build()
    )(SymmetricalLogicalPlanEquality)
    plan() should not equal plan(withHint = false)
  }

  test("should work with relationship index seeks of relationship type disjunctions and solve two index hints") {
    val cfg = hintPlannerConfig()
      .addRelationshipIndex("REL1", Seq("p1"), 1.0, 1.0)
      .addRelationshipIndex("REL2", Seq("p1"), 1.0, 1.0)
      .build()

    def plan(withHint: Boolean = true) = cfg.plan(
      s"""MATCH (a)-[r]-(b)
         |${if (withHint) "" else "//"} USING INDEX r:REL1(p1)
         |${if (withHint) "" else "//"} USING INDEX r:REL2(p1)
         |WHERE (r:REL1 OR r:REL2) AND (r.p1 = 1)
         |RETURN r""".stripMargin
    )

    // Possible improvement: We could have planned this as OrderedDistinct
    plan() should equal(
      cfg.planBuilder()
        .produceResults(column("r", "cacheR[r.p1]"))
        .distinct("r AS r", "a AS a", "b AS b")
        .union()
        .|.relationshipIndexOperator("(a)-[r:REL2(p1 = 1)]-(b)", _ => GetValue, indexType = IndexType.RANGE)
        .relationshipIndexOperator("(a)-[r:REL1(p1 = 1)]-(b)", _ => GetValue, indexType = IndexType.RANGE)
        .build()
    )(SymmetricalLogicalPlanEquality)
    plan() should not equal plan(withHint = false)
  }

  test("should work with label scans of label disjunctions only") {
    val cfg = plannerConfig().build()

    val plan = cfg.plan(
      """MATCH (n)
        |WHERE (n:L OR n:P)
        |RETURN n""".stripMargin
    )

    plan should equal(
      cfg.planBuilder()
        .produceResults("n")
        .unionNodeByLabelsScan("n", Seq("P", "L"))
        .build()
    )(SymmetricalLogicalPlanEquality)
  }

  test("should work with label scans of label disjunctions only and solve single scan hint") {
    val cfg = hintPlannerConfig().build()

    val plan = cfg.plan(
      """MATCH (n)
        |USING SCAN n:L
        |WHERE n:L OR n:P
        |RETURN n""".stripMargin
    )

    // It is impossible to make up statistics where an AllNodeScan would be better, so we will get the same plan even without the hint
    plan should equal(
      cfg.planBuilder()
        .produceResults("n")
        .unionNodeByLabelsScan("n", Seq("P", "L"))
        .build()
    )(SymmetricalLogicalPlanEquality)
  }

  test("should work with label scans of label disjunctions only and solve two scan hints") {
    val cfg = hintPlannerConfig().build()

    val plan = cfg.plan(
      """MATCH (n)
        |USING SCAN n:L
        |USING SCAN n:P
        |WHERE n:L OR n:P
        |RETURN n""".stripMargin
    )

    // It is impossible to make up statistics where an AllNodeScan would be better, so we will get the same plan even without the hint
    plan should equal(
      cfg.planBuilder()
        .produceResults("n")
        .unionNodeByLabelsScan("n", Seq("P", "L"))
        .build()
    )(SymmetricalLogicalPlanEquality)
  }

  test("should work with relationship type scans of relationship type disjunctions only") {
    val cfg = plannerConfig().build()

    val plan = cfg.plan(
      """MATCH (a)-[r]-(b)
        |WHERE (r:REL1 OR r:REL2)
        |RETURN r""".stripMargin
    )

    plan should (equal(
      cfg.planBuilder()
        .produceResults("r")
        .unionRelationshipTypesScan("(a)-[r:REL1|REL2]-(b)")
        .build()
    ) or equal(
      cfg.planBuilder()
        .produceResults("r")
        .unionRelationshipTypesScan("(a)-[r:REL2|REL1]-(b)")
        .build()
    ))
  }

  test("should work with relationship type scans of relationship type disjunctions only and solve single scan hint") {
    val cfg = hintPlannerConfig().build()

    def plan(withHint: Boolean = true) = cfg.plan(
      s"""MATCH (a)-[r]-(b)
         |${if (withHint) "" else "//"} USING SCAN r:REL2
         |WHERE (r:REL1 OR r:REL2)
         |RETURN r""".stripMargin
    )

    val thePlan = plan()
    thePlan should (equal(
      cfg.planBuilder()
        .produceResults("r")
        .unionRelationshipTypesScan("(a)-[r:REL1|REL2]-(b)")
        .build()
    ) or equal(
      cfg.planBuilder()
        .produceResults("r")
        .unionRelationshipTypesScan("(a)-[r:REL2|REL1]-(b)")
        .build()
    ))
    thePlan should not equal plan(withHint = false)
  }

  test("should work with relationship type scans of relationship type disjunctions only and solve two scan hint") {
    val cfg = hintPlannerConfig().build()

    def plan(withHint: Boolean = true) = cfg.plan(
      s"""MATCH (a)-[r]-(b)
         |${if (withHint) "" else "//"} USING SCAN r:REL1
         |${if (withHint) "" else "//"} USING SCAN r:REL2
         |WHERE (r:REL1 OR r:REL2)
         |RETURN r""".stripMargin
    )

    val thePlan = plan()
    thePlan should (equal(
      cfg.planBuilder()
        .produceResults("r")
        .unionRelationshipTypesScan("(a)-[r:REL1|REL2]-(b)")
        .build()
    ) or equal(
      cfg.planBuilder()
        .produceResults("r")
        .unionRelationshipTypesScan("(a)-[r:REL2|REL1]-(b)")
        .build()
    ))
    thePlan should not equal plan(withHint = false)
  }

  test("should work with relationship type scans of inlined relationship type disjunctions only") {
    val cfg = plannerConfig().build()

    val plan = cfg.plan(
      """MATCH (a)-[r:REL1|REL2]-(b)
        |RETURN r""".stripMargin
    )

    plan should (equal(
      cfg.planBuilder()
        .produceResults("r")
        .unionRelationshipTypesScan("(a)-[r:REL1|REL2]-(b)")
        .build()
    ) or equal(
      cfg.planBuilder()
        .produceResults("r")
        .unionRelationshipTypesScan("(a)-[r:REL2|REL1]-(b)")
        .build()
    ))
  }

  test("should handle same relationship type disjunction") {
    val planner = plannerConfig()
      .setRelationshipCardinality("(:L)-[:REL1]->()", 4)
      .build()

    val plan = planner.plan(
      """MATCH (a:L)-[:REL1|REL1]->(b)
        |RETURN b""".stripMargin
    )

    // we should not plan a union distinct
    plan should equal(
      planner.planBuilder()
        .produceResults("b")
        .expandAll("(a)-[anon_0:REL1|REL1]->(b)")
        .nodeByLabelScan("a", "L")
        .build()
    )
  }

  test("should handle query with complex but essentially false selection") {
    val planner = plannerConfig()
      .build()

    // this should not throw
    val plan = planner.plan(
      """MATCH (n:L)
        |WHERE (n.a AND (NOT ((n.a OR n.a) AND false))) XOR n.a
        |RETURN n""".stripMargin
    )

    plan should equal(
      planner.planBuilder()
        .produceResults(column("n", "cacheNFromStore[n.a]"))
        .filter(
          "not cacheNFromStore[n.a]",
          "CoerceToPredicate(cacheNFromStore[n.a])"
        )
        .nodeByLabelScan("n", "L")
        .build()
    )
  }

  test("should handle multiple disjunctions by picking the first one") {
    // this tests a published github issue.
    val cfg = plannerConfig()
      .setLabelCardinality("L1", 10).build()
    val plan = cfg.plan(
      """
        |MATCH (n:L1)
        |WHERE ((((((((("<")+("j4"))ENDS WITH(("")+("Ta")))OR(reverse("VU,")=~toUpper("%")))XOR(false))AND(n.k1))XOR(n.k1)))OR(n.k1))XOR(n.k1)
        |RETURN n.k1
        |""".stripMargin
    )

    plan should equal(
      cfg.planBuilder()
        .produceResults("`n.k1`")
        .projection("cacheN[n.k1] AS `n.k1`")
        .filter(
          "NOT ('<' + 'j4') ENDS WITH ('' + 'Ta') OR NOT cacheN[n.k1] OR cacheN[n.k1]",
          "NOT reverse('VU,') =~ toUpper('%') OR NOT cacheN[n.k1] OR cacheN[n.k1]",
          "('<' + 'j4') ENDS WITH ('' + 'Ta') OR reverse('VU,') =~ toUpper('%') OR cacheN[n.k1]",
          "NOT ('<' + 'j4') ENDS WITH ('' + 'Ta') OR ('<' + 'j4') ENDS WITH ('' + 'Ta') OR reverse('VU,') =~ toUpper('%') OR NOT cacheN[n.k1] OR cacheN[n.k1]",
          "NOT reverse('VU,') =~ toUpper('%') OR ('<' + 'j4') ENDS WITH ('' + 'Ta') OR reverse('VU,') =~ toUpper('%') OR NOT cacheN[n.k1] OR cacheN[n.k1]"
        )
        .distinct("n AS n")
        .cacheProperties("cacheN[n.k1]")
        .union()
        .|.filter("NOT cacheN[n.k1]", "CoerceToPredicate(cacheNFromStore[n.k1])")
        .|.nodeByLabelScan("n", "L1", IndexOrderNone)
        .filter(
          "NOT cacheNFromStore[n.k1]",
          "CoerceToPredicate(cacheNFromStore[n.k1])",
          "(NOT ('<' + 'j4') ENDS WITH ('' + 'Ta') AND NOT reverse('VU,') =~ toUpper('%') OR NOT cacheNFromStore[n.k1]) AND NOT cacheNFromStore[n.k1] OR (('<' + 'j4') ENDS WITH ('' + 'Ta') OR reverse('VU,') =~ toUpper('%')) AND cacheNFromStore[n.k1]"
        )
        .nodeByLabelScan("n", "L1", IndexOrderNone)
        .build()
    )
  }

  test(
    "should not redistribute on a complex nested disjunction with negations"
  ) {
    // this tests a published github issue.
    // a normalized version of the predicate is (:((!n1 & !n2 & n3 & n4 & !n5 & !n6 & !n7 & n8 & n9 | n10 & !x)&!x))
    // the next test is a simplified expression from here without negations.
    val q =
      """
        |MATCH (:!(!(!(n1|(!(!(n2|(!(!(!(n3&n5)|n5))|n6)|!(!(n7|!(n8&n9)))))))|!(x|!n10))|x))
        |RETURN 0
        |""".stripMargin
    val cfg = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("n1", 10)
      .setLabelCardinality("n2", 10)
      .setLabelCardinality("n3", 10)
      .setLabelCardinality("n4", 10)
      .setLabelCardinality("n5", 10)
      .setLabelCardinality("n6", 10)
      .setLabelCardinality("n7", 10)
      .setLabelCardinality("n8", 10)
      .setLabelCardinality("n9", 10)
      .setLabelCardinality("n10", 10)
      .setLabelCardinality("x", 10)
      .build()

    val plan = cfg.plan(q)

    plan should equal(
      cfg.planBuilder()
        .produceResults("0")
        .projection("0 AS 0")
        .distinct("anon_0 AS anon_0")
        .union()
        .|.subtractionNodeByLabelsScan("anon_0", Seq("n10"), Seq("x"), IndexOrderNone)
        .subtractionNodeByLabelsScan(
          "anon_0",
          Seq("n8", "n5", "n9", "n3"),
          Seq("n6", "n5", "n1", "n2", "n7", "x"),
          IndexOrderNone
        )
        .build()
    )
  }

  test(
    "should not redistribute on a disjunction with overlapping predicates if all the predicates are solved by the disjunction already"
  ) {
    val q =
      """
        |MATCH (:((n1 & n2 & n3 & n4 & n5 & n6 & n7 & n8 & n9 | n10 & x)&x))
        |RETURN 0
        |""".stripMargin
    val cfg = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("n1", 10)
      .setLabelCardinality("n2", 10)
      .setLabelCardinality("n3", 10)
      .setLabelCardinality("n4", 10)
      .setLabelCardinality("n5", 10)
      .setLabelCardinality("n6", 10)
      .setLabelCardinality("n7", 10)
      .setLabelCardinality("n8", 10)
      .setLabelCardinality("n9", 10)
      .setLabelCardinality("n10", 10)
      .setLabelCardinality("x", 10)
      .build()

    val plan = cfg.plan(q)

    plan should equal(
      cfg.planBuilder()
        .produceResults("0")
        .projection("0 AS 0")
        .distinct("anon_0 AS anon_0")
        .union()
        .|.intersectionNodeByLabelsScan("anon_0", Seq("x", "n10"), IndexOrderNone)
        .intersectionNodeByLabelsScan(
          "anon_0",
          Seq("n8", "n9", "n6", "n4", "n5", "n3", "n1", "n2", "n7", "x"),
          IndexOrderNone
        )
        .build()
    )
  }

  test(
    "Should compute the solved expression properly if either side of the disjunction is completely contained by the other side."
  ) {
    val planner =
      plannerConfig()
        .setAllNodesCardinality(10_000)
        .setLabelCardinality("A", 1_000)
        .setLabelCardinality("B", 1_000)
        .setLabelCardinality("C", 1_000)
        .setLabelCardinality("D", 1_000)
        .setLabelCardinality("E", 1_000)
        .setLabelCardinality("F", 1_000)
        .setLabelCardinality("G", 1_000)
        .setLabelCardinality("H", 1_000)
        .setLabelCardinality("I", 1_000)
        .setLabelCardinality("X", 1_000)
        .setLabelCardinality("Y", 1_000)
        .build()

    val query =
      """MATCH (n:(A & B & C & D & E & F & G & H & I | A & X) & (X & B & C & D & E & F & G & H & I))
        |RETURN n""".stripMargin

    val plan = planner.plan(query)

    plan should equal(
      planner.planBuilder()
        .produceResults("n")
        .intersectionNodeByLabelsScan("n", Seq("A", "I", "H", "C", "D", "G", "E", "B", "F", "X"), IndexOrderNone)
        .build()
    )
  }

  test("should work with index disjunction of conjunctions") {
    val cfg = plannerConfig()
      .addNodeIndex("L", Seq("p1"), 0.5, 0.5)
      .addNodeIndex("P", Seq("p1"), 0.5, 0.5)
      .build()

    val plan = cfg.plan(
      """MATCH (n)
        |WHERE (n:L) OR (n:P AND n.p1 = 1)
        |RETURN n""".stripMargin
    )

    // Possible improvement: This could be planned with a nodeByLabelScan and a nodeIndexSeek
    plan should equal(
      cfg.planBuilder()
        .produceResults("n")
        .filterExpression(ors(hasLabels("n", "L"), propEquality("n", "p1", 1)))
        .unionNodeByLabelsScan("n", Seq("P", "L"))
        .build()
    )(SymmetricalLogicalPlanEquality)
  }

  test("should work with relationship index disjunction of conjunctions") {
    val cfg = plannerConfig()
      .addRelationshipIndex("REL1", Seq("p1"), 0.5, 0.5)
      .addRelationshipIndex("REL1", Seq("p2"), 0.5, 0.5)
      .build()

    val plan = cfg.plan(
      """MATCH (a)-[r]-(b)
        |WHERE (r:REL1) OR (r:REL2 AND r.p1 = 1)
        |RETURN r""".stripMargin
    )

    // Possible improvement: This could be planned with a relationshipTypeScan and a relationshipIndexSeek
    plan should (equal(
      cfg.planBuilder()
        .produceResults("r")
        .filterExpression(ors(hasTypes("r", "REL1"), propEquality("r", "p1", 1)))
        .unionRelationshipTypesScan("(a)-[r:REL1|REL2]-(b)")
        .build()
    ) or equal(
      cfg.planBuilder()
        .produceResults("r")
        .filterExpression(ors(hasTypes("r", "REL1"), propEquality("r", "p1", 1)))
        .unionRelationshipTypesScan("(a)-[r:REL2|REL1]-(b)")
        .build()
    ))
  }

  test("should prefer label scan to node index scan from existence constraint with same cardinality") {
    val cfg = plannerConfig()
      .addNodeIndex("L", Seq("p1"), 1.0, 0.1)
      .addNodeExistenceConstraint("L", "p1")
      .addNodeIndex("P", Seq("p1"), 1.0, 0.1)
      .addNodeExistenceConstraint("P", "p1")
      .build()

    val plan = cfg.plan(s"MATCH (n) WHERE n:L or n:P RETURN n")

    plan should equal(
      cfg.planBuilder()
        .produceResults("n")
        .unionNodeByLabelsScan("n", Seq("P", "L"))
        .build()
    )(SymmetricalLogicalPlanEquality)
  }

  test("should prefer type scan to relationship index scan from existence constraint with same cardinality") {
    val cfg = plannerConfig()
      .addRelationshipIndex("REL1", Seq("p1"), 1.0, 0.1)
      .addRelationshipExistenceConstraint("REL1", "p1")
      .addRelationshipIndex("REL2", Seq("p2"), 1.0, 0.1)
      .addRelationshipExistenceConstraint("REL2", "p1")
      .build()

    val plan = cfg.plan(s"MATCH (a)-[r:REL1|REL2]->(b) RETURN r")

    plan should (equal(
      cfg.planBuilder()
        .produceResults("r")
        .unionRelationshipTypesScan("(a)-[r:REL1|REL2]->(b)")
        .build()
    ) or equal(
      cfg.planBuilder()
        .produceResults("r")
        .unionRelationshipTypesScan("(a)-[r:REL2|REL1]->(b)")
        .build()
    ))
  }

  test(
    "should prefer union label scan to node index scan from existence constraint with same cardinality, if indexed property is used"
  ) {
    val cfg = plannerConfig()
      .addNodeIndex("L", Seq("p1"), 1.0, 0.1)
      .addNodeExistenceConstraint("L", "p1")
      .addNodeIndex("P", Seq("p1"), 1.0, 0.1)
      .addNodeExistenceConstraint("P", "p1")
      .build()

    val plan = cfg.plan(s"MATCH (n) WHERE n:L or n:P RETURN n.p1 AS p")

    plan should equal(
      cfg.planBuilder()
        .produceResults("p")
        .projection("n.p1 AS p")
        .unionNodeByLabelsScan("n", Seq("L", "P"))
        .build()
    )(SymmetricalLogicalPlanEquality)
  }

  test(
    "should prefer union relationship type scan " +
      "to relationship index scan from existence constraint with same cardinality, if indexed property is used"
  ) {
    val cfg = plannerConfig()
      .addRelationshipIndex("REL1", Seq("p1"), 1.0, 0.1)
      .addRelationshipExistenceConstraint("REL1", "p1")
      .addRelationshipIndex("REL2", Seq("p1"), 1.0, 0.1)
      .addRelationshipExistenceConstraint("REL2", "p1")
      .build()

    val plan = cfg.plan(s"MATCH (a)-[r:REL1|REL2]->(b) RETURN r.p1 AS p")

    plan should (equal(
      cfg.planBuilder()
        .produceResults("p")
        .projection("r.p1 AS p")
        .unionRelationshipTypesScan("(a)-[r:REL1|REL2]->(b)")
        .build()
    ) or equal(
      cfg.planBuilder()
        .produceResults("p")
        .projection("r.p1 AS p")
        .unionRelationshipTypesScan("(a)-[r:REL2|REL1]->(b)")
        .build()
    ))
  }

  test(
    "should prefer union label scan " +
      "to node index scan from aggregation and " +
      "to node index scan from existence constraint with same cardinality"
  ) {
    val cfg = plannerConfig()
      .addNodeIndex("L", Seq("p1"), 1.0, 0.1)
      .addNodeExistenceConstraint("L", "p1")
      .addNodeIndex("L", Seq("p2"), 1.0, 0.1)
      .addNodeIndex("P", Seq("p1"), 1.0, 0.1)
      .addNodeExistenceConstraint("P", "p1")
      .addNodeIndex("P", Seq("p2"), 1.0, 0.1)
      .build()

    val plan = cfg.plan(s"MATCH (n) WHERE n:L or n:P RETURN count(n.p2) AS c")

    plan should equal(
      cfg.planBuilder()
        .produceResults("c")
        .aggregation(Seq(), Seq("count(n.p2) AS c"))
        .unionNodeByLabelsScan("n", Seq("L", "P"))
        .build()
    )(SymmetricalLogicalPlanEquality)
  }

  test(
    "should prefer union relationship type scan " +
      "to relationship index scan from aggregation and " +
      "to relationship index scan from existence constraint with same cardinality"
  ) {
    val cfg = plannerConfig()
      .addRelationshipIndex("REL1", Seq("p1"), 1.0, 0.1)
      .addRelationshipExistenceConstraint("REL1", "p1")
      .addRelationshipIndex("REL1", Seq("p2"), 1.0, 0.1)
      .addRelationshipIndex("REL2", Seq("p1"), 1.0, 0.1)
      .addRelationshipExistenceConstraint("REL2", "p1")
      .addRelationshipIndex("REL2", Seq("p2"), 1.0, 0.1)
      .build()

    val plan = cfg.plan(s"MATCH (a)-[r:REL1|REL2]->(b) RETURN count(r.p2) AS c")

    plan should (equal(
      cfg.planBuilder()
        .produceResults("c")
        .aggregation(Seq(), Seq("count(r.p2) AS c"))
        .unionRelationshipTypesScan("(a)-[r:REL1|REL2]->(b)")
        .build()
    ) or equal(
      cfg.planBuilder()
        .produceResults("c")
        .aggregation(Seq(), Seq("count(r.p2) AS c"))
        .unionRelationshipTypesScan("(a)-[r:REL2|REL1]->(b)")
        .build()
    ))
  }

  test("should prefer union label scan for aggregated property, even if other property is referenced") {
    val cfg = plannerConfig()
      .addNodeIndex("L", Seq("p1"), 1.0, 0.1)
      .addNodeExistenceConstraint("L", "p1")
      .addNodeIndex("L", Seq("p2"), 1.0, 0.1)
      .addNodeIndex("P", Seq("p1"), 1.0, 0.1)
      .addNodeExistenceConstraint("P", "p1")
      .addNodeIndex("P", Seq("p2"), 1.0, 0.1)
      .build()

    val plan = cfg.plan(s"MATCH (n) WHERE (n:L or n:P) AND n.p1 <> 1 RETURN count(n.p2) AS c")

    plan should equal(
      cfg.planBuilder()
        .produceResults("c")
        .aggregation(Seq(), Seq("count(n.p2) AS c"))
        .filter("not n.p1 = 1")
        .unionNodeByLabelsScan("n", Seq("L", "P"))
        .build()
    )(SymmetricalLogicalPlanEquality)
  }

  test("should prefer union relationship type scan for aggregated property, even if other property is referenced") {
    val cfg = plannerConfig()
      .addRelationshipIndex("REL1", Seq("p1"), 1.0, 0.01)
      .addRelationshipExistenceConstraint("REL1", "p1")
      .addRelationshipIndex("REL1", Seq("p2"), 1.0, 0.01)
      .addRelationshipIndex("REL2", Seq("p1"), 1.0, 0.01)
      .addRelationshipExistenceConstraint("REL2", "p1")
      .addRelationshipIndex("REL2", Seq("p2"), 1.0, 0.01)
      .build()

    val plan = cfg.plan(s"MATCH (a)-[r:REL1|REL2]->(b) WHERE r.p1 <> 1 RETURN count(r.p2) AS c")

    plan should (equal(
      cfg.planBuilder()
        .produceResults("c")
        .aggregation(Seq(), Seq("count(r.p2) AS c"))
        .filter("not r.p1 = 1")
        .unionRelationshipTypesScan("(a)-[r:REL1|REL2]->(b)")
        .build()
    ) or equal(
      cfg.planBuilder()
        .produceResults("c")
        .aggregation(Seq(), Seq("count(r.p2) AS c"))
        .filter("not r.p1 = 1")
        .unionRelationshipTypesScan("(a)-[r:REL2|REL1]->(b)")
        .build()
    ))
  }

  test("should not explode for many STARTS WITH") {
    val query =
      """MATCH (tn:X) USING INDEX tn:X(prop)
        |WHERE (tn:Y) AND ((
        |   tn.prop STARTS WITH $p3 OR
        |   tn.prop STARTS WITH $p4 OR
        |   tn.prop STARTS WITH $p5 OR
        |   tn.prop STARTS WITH $p6 OR
        |   tn.prop STARTS WITH $p7 OR
        |   tn.prop STARTS WITH $p8 OR
        |   tn.prop STARTS WITH $p9 OR
        |   tn.prop STARTS WITH $p10 OR
        |   tn.prop STARTS WITH $p11 OR
        |   tn.prop STARTS WITH $p12 OR
        |   tn.prop STARTS WITH $p13 OR
        |   tn.prop STARTS WITH $p14 OR
        |   tn.prop STARTS WITH $p15 OR
        |   tn.prop STARTS WITH $p16 OR
        |   tn.prop STARTS WITH $p17 OR
        |   tn.prop STARTS WITH $p18 OR
        |   tn.prop STARTS WITH $p19 OR
        |   tn.prop STARTS WITH $p20 OR
        |   tn.prop STARTS WITH $p21 OR
        |   tn.prop STARTS WITH $p22 OR
        |   tn.prop STARTS WITH $p23 OR
        |   tn.prop STARTS WITH $p24 OR
        |   tn.prop STARTS WITH $p25 OR
        |   tn.prop STARTS WITH $p26 OR
        |   tn.prop STARTS WITH $p27 OR
        |   tn.prop STARTS WITH $p28 OR
        |   tn.prop STARTS WITH $p29 OR
        |   tn.prop STARTS WITH $p30 OR
        |   tn.prop STARTS WITH $p31 OR
        |   tn.prop STARTS WITH $p32 OR
        |   tn.prop STARTS WITH $p33 OR
        |   tn.prop STARTS WITH $p34 OR
        |   tn.prop STARTS WITH $p35 OR
        |   tn.prop STARTS WITH $p36 OR
        |   tn.prop STARTS WITH $p37 OR
        |   tn.prop STARTS WITH $p38 OR
        |   tn.prop STARTS WITH $p39 OR
        |   tn.prop STARTS WITH $p40 OR
        |   tn.prop STARTS WITH $p41 OR
        |   tn.prop STARTS WITH $p42 OR
        |   tn.prop STARTS WITH $p43 OR
        |   tn.prop STARTS WITH $p44 OR
        |   tn.prop STARTS WITH $p45 OR
        |   tn.prop STARTS WITH $p46 OR
        |   tn.prop STARTS WITH $p47 OR
        |   tn.prop STARTS WITH $p48 OR
        |   tn.prop STARTS WITH $p49 OR
        |   tn.prop STARTS WITH $p50 OR
        |   tn.prop STARTS WITH $p51 OR
        |   tn.prop STARTS WITH $p52 OR
        |   tn.prop STARTS WITH $p53 OR
        |   tn.prop STARTS WITH $p54 OR
        |   tn.prop STARTS WITH $p55 OR
        |   tn.prop STARTS WITH $p56 OR
        |   tn.prop STARTS WITH $p57 OR
        |   tn.prop STARTS WITH $p58 OR
        |   tn.prop STARTS WITH $p59 OR
        |   tn.prop STARTS WITH $p60 OR
        |   tn.prop STARTS WITH $p61 OR
        |   tn.prop STARTS WITH $p62 OR
        |   tn.prop STARTS WITH $p63 OR
        |   tn.prop STARTS WITH $p64 OR
        |   tn.prop STARTS WITH $p65 OR
        |   tn.prop STARTS WITH $p66 OR
        |   tn.prop STARTS WITH $p67 OR
        |   tn.prop STARTS WITH $p68 OR
        |   tn.prop STARTS WITH $p69 OR
        |   tn.prop STARTS WITH $p70 OR
        |   tn.prop STARTS WITH $p71 OR
        |   tn.prop STARTS WITH $p72 OR
        |   tn.prop STARTS WITH $p73 OR
        |   tn.prop STARTS WITH $p74 OR
        |   tn.prop STARTS WITH $p75 OR
        |   tn.prop STARTS WITH $p76 OR
        |   tn.prop STARTS WITH $p77 OR
        |   tn.prop STARTS WITH $p78 OR
        |   tn.prop STARTS WITH $p79 OR
        |   tn.prop STARTS WITH $p80 OR
        |   tn.prop STARTS WITH $p81 OR
        |   tn.prop STARTS WITH $p82 OR
        |   tn.prop STARTS WITH $p83 OR
        |   tn.prop STARTS WITH $p84 OR
        |   tn.prop STARTS WITH $p85 OR
        |   tn.prop STARTS WITH $p86 OR
        |   tn.prop STARTS WITH $p87 OR
        |   tn.prop STARTS WITH $p88 OR
        |   tn.prop STARTS WITH $p89 OR
        |   tn.prop STARTS WITH $p90 OR
        |   tn.prop STARTS WITH $p91 OR
        |   tn.prop STARTS WITH $p92
        | ) AND
        |   tn.processType IN $p0
        | AND
        |   tn.status IN $p1
        | AND
        |   tn.fileCollectionEnabled = $p2
        |)
        |RETURN tn""".stripMargin

    val cfg = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("X", 50)
      .setLabelCardinality("Y", 50)
      .addNodeIndex("X", Seq("prop"), 0.5, 0.5)
      .build()

    val plan = runWithTimeout(1000)(cfg.plan(query))

    plan.folder.treeCount {
      case _: NodeIndexSeek => ()
    } should be(90)
  }

  test("should solve id seekable predicates in OR expression") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("L", 50)
      .addNodeIndex("L", Seq("prop"), 0.5, 0.5)
      .build()

    val plan = cfg.plan(
      """MATCH (n:L) WHERE id(n) = 1 OR n.prop > 123
        |RETURN n""".stripMargin
    )

    plan should equal(
      cfg.planBuilder()
        .produceResults(column("n", "cacheN[n.prop]"))
        .distinct("n AS n")
        .union()
        .|.filter("n:L")
        .|.nodeByIdSeek("n", Set(), 1)
        .nodeIndexOperator(
          "n:L(prop > 123)",
          argumentIds = Set(),
          getValue = Map("prop" -> GetValue),
          indexType = IndexType.RANGE
        )
        .build()
    )(SymmetricalLogicalPlanEquality)
  }

  test("should solve element id seekable predicates in OR expression") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("L", 50)
      .addNodeIndex("L", Seq("prop"), 0.5, 0.5)
      .build()

    val plan = cfg.plan(
      """MATCH (n:L) WHERE elementId(n) = 'some-id' OR n.prop > 123
        |RETURN n""".stripMargin
    )

    plan should equal(
      cfg.planBuilder()
        .produceResults(column("n", "cacheN[n.prop]"))
        .distinct("n AS n")
        .union()
        .|.filter("n:L")
        .|.nodeByElementIdSeek("n", Set(), "'some-id'")
        .nodeIndexOperator(
          "n:L(prop > 123)",
          argumentIds = Set(),
          getValue = Map("prop" -> GetValue),
          indexType = IndexType.RANGE
        )
        .build()
    )(SymmetricalLogicalPlanEquality)
  }

  test("should be able to cope with disjunction of overlapping predicates") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(0)
      .setLabelCardinality("A", 0)
      .enableMinimumGraphStatistics()
      .addNodeIndex("A", Seq("prop2"), 0.1, 0.001)
      .build()

    // Note that prop1 is present on all parts of the conjunction.
    // After the OrLeafPlanner has distributed the disjunction on prop1 and prop2, it calculates what the union of the two resulting plans solves.
    // When doing this, the predicates solved are separated in those predicates which are solved by all (two) plans and those which are solved by just one plan.
    // In this case, `a.prop1 != NULL` and `a.prop1 != NULL OR a.prop3 != NULL` are solved by both plans, whereas the plan given `a.prop2 != NULL` will also solve that.
    // The OrLeafPlanner then tries to create a disjunction over the predicates that only one plan solves.
    // This might go wrong because the prop1-plan will not contribute any predicate and therefore saying that the overall plan solves
    // the disjunction of all individual plan's predicates (= Ors(`a.prop2 != NULL`)) is wrong.
    val plan = planner.plan(
      """MATCH (a:A)
        |WHERE (
        |  a.prop1 IS NOT NULL OR
        |  a.prop2 IS NOT NULL
        |)
        |AND (
        |  a.prop1 IS NOT NULL OR
        |  a.prop3 IS NOT NULL
        |)
        |AND a.prop1 IS NOT NULL
        |RETURN a""".stripMargin
    )

    plan should equal(
      planner.planBuilder()
        .produceResults(column("a", "cacheNHasProperty[a.prop1]", "cacheN[a.prop2]"))
        .filter(
          "a.prop3 IS NOT NULL OR cacheNHasProperty[a.prop1] IS NOT NULL"
        )
        .distinct("a AS a")
        .union()
        .|.filter("cacheNHasPropertyFromStore[a.prop1] IS NOT NULL")
        .|.nodeByLabelScan("a", "A")
        .filter("cacheNHasPropertyFromStore[a.prop1] IS NOT NULL")
        .nodeIndexOperator("a:A(prop2)", _ => GetValue)
        .build()
    )(SymmetricalLogicalPlanEquality)
  }

  test("should be able to cope with any combination of disjunction of predicates") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(0)
      .setLabelCardinality("A", 0)
      .enableMinimumGraphStatistics()
      .build()

    forAll(for {
      size <- Gen.choose(2, 3)
      disjunctions <-
        Gen.sequence[Seq[Set[Int]], Set[Int]](Seq.fill(size)(Gen.nonEmptyBuildableOf[Set[Int], Int](Gen.choose(1, 3))))
    } yield disjunctions) { (disjunctions: Seq[Set[Int]]) =>
      val selections =
        disjunctions.map(
          _.map(operand => s"a.prop$operand IS NOT NULL").mkString("(", " AND ", ")")
        ).mkString("WHERE ", " OR ", "")
      val query =
        s"""MATCH (a:A)
           |$selections
           |RETURN a""".stripMargin
      withClue(query) {
        planner.plan(query)
      }
    }
  }

  test("should plan index with fuzzy expression in ORs") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("L", 50)
      .addNodeIndex("L", Seq("a"), 0.5, 0.5)
      .build()

    val plan = cfg.plan(
      """MATCH (n:L) WHERE n.a OR (((""=~"") AND NOT(true)) >= n.a) 
        |RETURN n
        |""".stripMargin
    )
    atLeast(1, plan.leaves) should matchPattern {
      case _: NodeIndexSeek =>
    }
  }

  test("should plan index with nested property in ORs") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("L", 50)
      .addNodeIndex("L", Seq("a"), 0.5, 0.5)
      .build()

    val plan = cfg.plan(
      """MATCH (x) WITH x SKIP 0 
        |MATCH (n:L) WHERE n.a OR (((""=~"") AND x.b > 5) >= n.a) 
        |RETURN n
        |""".stripMargin
    )
    atLeast(1, plan.leaves) should matchPattern {
      case _: NodeIndexSeek =>
    }
  }

  test(
    "should not plan a distinct union if the number of predicates on a single variable in a WHERE sub-clause is greater than `predicates_as_union_max_size`"
  ) {
    val cfg = plannerConfig()
      .addNodeIndex("L", Seq("prop"), 0.1, 0.1)
      .addNodeIndex("L", Seq("foo"), 0.1, 0.1)
      .withSetting(GraphDatabaseInternalSettings.predicates_as_union_max_size, java.lang.Integer.valueOf(1))
      .build()

    val plan = cfg.plan(
      """MATCH (n:L)
        |WHERE (n.prop > 3 OR n.foo > 3)
        |RETURN n""".stripMargin
    )

    val expectedPlan =
      cfg.planBuilder()
        .produceResults("n")
        .filter("n.prop > 3 OR n.foo > 3")
        .nodeByLabelScan("n", "L")
        .build()

    plan shouldEqual expectedPlan
  }

  test("should plan unionNodeByLabelsScan if there are more predicates") {
    val cfg = plannerConfig().build()

    val plan = cfg.plan(
      """MATCH (n)
        |WHERE (n:L OR n:P) AND (n.p1 < 1)
        |RETURN n""".stripMargin
    )

    plan should equal(
      cfg.planBuilder()
        .produceResults("n")
        .filter("n.p1 < 1")
        .unionNodeByLabelsScan("n", Seq("P", "L"))
        .build()
    )(SymmetricalLogicalPlanEquality)
  }

  test("should be able to use composite index that partially solves disjunctions") {
    val cfg = plannerConfig()
      .addNodeIndex("L", Seq("p1", "p2"), 0.5, 0.5)
      .addNodeIndex("L", Seq("p3"), 0.5, 0.5)
      .build()

    val plan = cfg.plan(
      """MATCH (n:L)
        |  WHERE (n.p1 <= 10 <= n.p2) OR n.p3 > 20
        |RETURN n""".stripMargin
    )

    plan should equal(
      cfg.planBuilder()
        .produceResults(column("n", "cacheN[n.p2]", "cacheN[n.p1]", "cacheN[n.p3]"))
        .distinct("n AS n")
        .union()
        .|.nodeIndexOperator("n:L(p3 > 20)", _ => GetValue)
        .filter("cacheN[n.p2] >= 10")
        .nodeIndexOperator(
          "n:L(p1 <= 10, p2)",
          Map("p1" -> GetValue, "p2" -> GetValue),
          supportPartitionedScan = false
        )
        .build()
    )(SymmetricalLogicalPlanEquality)
  }

  test("should not crash on XOR predicates that might get rewritten to Ors with a single predicate") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("L", 50)
      .build()

    noException should be thrownBy {
      cfg.plan(
        """MATCH (n:L) WHERE NOT(((NOT(toBoolean(true))) AND NOT(isEmpty(n.p))) XOR (isEmpty(n.p)))
          |RETURN *
          |""".stripMargin
      )
    }
  }

  private def runWithTimeout[T](timeout: Long)(f: => T): T = {
    Await.result(scala.concurrent.Future(f)(scala.concurrent.ExecutionContext.global), Duration.apply(timeout, "s"))
  }
}
