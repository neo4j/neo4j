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
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanConstructionTestSupport
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningIntegrationTestSupport
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfigurationBuilder
import org.neo4j.cypher.internal.logical.plans.Ascending
import org.neo4j.cypher.internal.logical.plans.Distinct
import org.neo4j.cypher.internal.logical.plans.DoNotGetValue
import org.neo4j.cypher.internal.logical.plans.GetValue
import org.neo4j.cypher.internal.logical.plans.IndexOrderAscending
import org.neo4j.cypher.internal.logical.plans.IndexSeek.nodeIndexSeek
import org.neo4j.cypher.internal.logical.plans.NodeIndexSeek
import org.neo4j.cypher.internal.logical.plans.Selection
import org.neo4j.cypher.internal.logical.plans.Union
import org.neo4j.cypher.internal.planner.spi.IndexOrderCapability
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class OrLeafPlanningIntegrationTest
  extends CypherFunSuite
  with LogicalPlanningIntegrationTestSupport
  with AstConstructionTestSupport
  with LogicalPlanConstructionTestSupport {

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

    plan should (equal(
      cfg.planBuilder()
        .produceResults("n")
        .distinct("n AS n")
        .union()
        .|.nodeIndexOperator("n:L(p2 = 2)")
        .nodeIndexOperator("n:L(p1 = 1)")
        .build()
    ) or equal(
      cfg.planBuilder()
        .produceResults("n")
        .distinct("n AS n")
        .union()
        .|.nodeIndexOperator("n:L(p1 = 1)")
        .nodeIndexOperator("n:L(p2 = 2)")
        .build()
    ))
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

    plan should (equal(
      cfg.planBuilder()
        .produceResults("n")
        .distinct("n AS n")
        .union()
        .|.nodeIndexOperator("n:L(p2 < 7)")
        .nodeIndexOperator("n:L(p1 > 3)")
        .build()
    ) or equal(
      cfg.planBuilder()
        .produceResults("n")
        .distinct("n AS n")
        .union()
        .|.nodeIndexOperator("n:L(p1 > 3)")
        .nodeIndexOperator("n:L(p2 < 7)")
        .build()
    ))
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

    plan should (equal(
      cfg.planBuilder()
        .produceResults("n")
        .distinct("n AS n")
        .union()
        .|.nodeIndexOperator("n:L(3 < p2 < 7)")
        .nodeIndexOperator("n:L(3 < p1 < 10)")
        .build()
    ) or equal(
      cfg.planBuilder()
        .produceResults("n")
        .distinct("n AS n")
        .union()
        .|.nodeIndexOperator("n:L(3 < p1 < 10)")
        .nodeIndexOperator("n:L(3 < p2 < 7)")
        .build()
    ))
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

    plan should (equal(
      cfg.planBuilder()
        .produceResults("n")
        .distinct("n AS n")
        .union()
        .|.filterExpression(hasLabels("n", "P"))
        .|.nodeIndexOperator("n:L(p2 = 2)")
        .filterExpression(hasLabels("n", "P"))
        .nodeIndexOperator("n:L(p1 = 1)")
        .build()
    ) or equal(
      cfg.planBuilder()
        .produceResults("n")
        .filterExpression(hasLabels("n", "P"))
        .distinct("n AS n")
        .union()
        .|.nodeIndexOperator("n:L(p1 = 1)")
        .nodeIndexOperator("n:L(p2 = 2)")
        .build()
    ))
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

    plan() should (equal(
      cfg.planBuilder()
        .produceResults("n")
        .distinct("n AS n")
        .union()
        .|.filterExpression(hasLabels("n", "P"))
        .|.nodeIndexOperator("n:L(p2 = 2)")
        .filterExpression(hasLabels("n", "P"))
        .nodeIndexOperator("n:L(p1 = 1)")
        .build()
    ) or equal(
      cfg.planBuilder()
        .produceResults("n")
        .filterExpression(hasLabels("n", "P"))
        .distinct("n AS n")
        .union()
        .|.nodeIndexOperator("n:L(p1 = 1)")
        .nodeIndexOperator("n:L(p2 = 2)")
        .build()
    ))
    plan() should not equal plan(withHint = false)
  }

  test("should work with index seeks of property disjunctions with label conjunction and solve two index hints") {
    val cfg = hintPlannerConfig()
      .addNodeIndex("L", Seq("p1"), 1.0, 1.0)
      .addNodeIndex("L", Seq("p2"), 1.0, 1.0)
      .build()

    def plan(withHint: Boolean = true)= cfg.plan(
      s"""MATCH (n:L:P)
        |${if (withHint) "" else "//"} USING INDEX n:L(p1)
        |${if (withHint) "" else "//"} USING INDEX n:L(p2)
        |WHERE n.p1 = 1 OR n.p2 = 2
        |RETURN n""".stripMargin
    )

    plan() should (equal(
      cfg.planBuilder()
        .produceResults("n")
        .distinct("n AS n")
        .union()
        .|.filterExpression(hasLabels("n", "P"))
        .|.nodeIndexOperator("n:L(p2 = 2)")
        .filterExpression(hasLabels("n", "P"))
        .nodeIndexOperator("n:L(p1 = 1)")
        .build()
    ) or equal(
      cfg.planBuilder()
        .produceResults("n")
        .filterExpression(hasLabels("n", "P"))
        .distinct("n AS n")
        .union()
        .|.nodeIndexOperator("n:L(p1 = 1)")
        .nodeIndexOperator("n:L(p2 = 2)")
        .build()
    ))
    plan() should not equal plan(withHint = false)
  }

  test("should work with index seeks of property disjunctions with label conjunction, where every combination is indexed") {
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

    val seekLProp1 = nodeIndexSeek("n:L(p1 = 1)", _ => DoNotGetValue)
    val seekLProp2 = nodeIndexSeek("n:L(p2 = 2)", _ => DoNotGetValue, propIds = Some(Map("p2" -> 1)))
    val seekPProp1 = nodeIndexSeek("n:P(p1 = 1)", _ => DoNotGetValue, labelId = 1)
    val seekPProp2 = nodeIndexSeek("n:P(p2 = 2)", _ => DoNotGetValue, labelId = 1, propIds = Some(Map("p2" -> 1)))

    val coveringCombinations = Seq(
      (seekLProp1, hasP),
      (seekLProp2, hasP),
      (seekPProp1, hasL),
      (seekPProp2, hasL),
    )

    val planAlternatives = for {
      Seq((seek1, filter1), (seek2, filter2)) <- coveringCombinations.permutations.map(_.take(2)).toSeq
    } yield Distinct(Union(Selection(Seq(filter1), seek1), Selection(Seq(filter2), seek2)), Map("n" -> varFor("n")))

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

    plan should (equal(
      cfg.planBuilder()
        .produceResults("r")
        .distinct("r AS r", "a AS a", "b AS b")
        .union()
        .|.relationshipIndexOperator("(a)-[r:REL1(p2 = 2)]-(b)")
        .relationshipIndexOperator("(a)-[r:REL1(p1 = 1)]-(b)")
        .build()
    ) or equal(
      cfg.planBuilder()
        .produceResults("r")
        .distinct("r AS r", "a AS a", "b AS b")
        .union()
        .|.relationshipIndexOperator("(a)-[r:REL1(p1 = 1)]-(b)")
        .relationshipIndexOperator("(a)-[r:REL1(p2 = 2)]-(b)")
        .build()
    ))
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

    plan() should (equal(
      cfg.planBuilder()
        .produceResults("r")
        .distinct("r AS r", "a AS a", "b AS b")
        .union()
        .|.relationshipIndexOperator("(a)-[r:REL1(p2 = 2)]-(b)")
        .relationshipIndexOperator("(a)-[r:REL1(p1 = 1)]-(b)")
        .build()
    ) or equal(
      cfg.planBuilder()
        .produceResults("r")
        .distinct("r AS r", "a AS a", "b AS b")
        .union()
        .|.relationshipIndexOperator("(a)-[r:REL1(p1 = 1)]-(b)")
        .relationshipIndexOperator("(a)-[r:REL1(p2 = 2)]-(b)")
        .build()
    ))
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

    plan() should (equal(
      cfg.planBuilder()
        .produceResults("r")
        .distinct("r AS r", "a AS a", "b AS b")
        .union()
        .|.relationshipIndexOperator("(a)-[r:REL1(p2 = 2)]-(b)")
        .relationshipIndexOperator("(a)-[r:REL1(p1 = 1)]-(b)")
        .build()
    ) or equal(
      cfg.planBuilder()
        .produceResults("r")
        .distinct("r AS r", "a AS a", "b AS b")
        .union()
        .|.relationshipIndexOperator("(a)-[r:REL1(p1 = 1)]-(b)")
        .relationshipIndexOperator("(a)-[r:REL1(p2 = 2)]-(b)")
        .build()
    ))
    plan() should not equal plan(withHint = false)
  }

  test("should work with index seeks of label disjunctions") {
    val cfg = plannerConfig()
      .addNodeIndex("L", Seq("p1"), 0.5, 0.5, providesOrder = IndexOrderCapability.BOTH)
      .addNodeIndex("P", Seq("p1"), 0.5, 0.5, providesOrder = IndexOrderCapability.BOTH)
      .build()

    val plan = cfg.plan(
      """MATCH (n)
        |WHERE (n:L OR n:P) AND (n.p1 = 1)
        |RETURN n""".stripMargin
    )

    // Possible improvement: We could have planned this as OrderedDistinct
    plan should (equal(
      cfg.planBuilder()
        .produceResults("n")
        .distinct("n AS n")
        .union()
        .|.nodeIndexOperator("n:P(p1 = 1)")
        .nodeIndexOperator("n:L(p1 = 1)")
        .build()
    ) or equal(
      cfg.planBuilder()
        .produceResults("n")
        .distinct("n AS n")
        .union()
        .|.nodeIndexOperator("n:L(p1 = 1)")
        .nodeIndexOperator("n:P(p1 = 1)")
        .build()
    ))
  }

  test("should work with index seeks of label disjunctions and solve single index hint") {
    val cfg = hintPlannerConfig()
      .addNodeIndex("L", Seq("p1"), 1.0, 1.0, providesOrder = IndexOrderCapability.BOTH)
      .addNodeIndex("P", Seq("p1"), 1.0, 1.0, providesOrder = IndexOrderCapability.BOTH)
      .build()

    val plan = cfg.plan(
      """MATCH (n)
        |USING INDEX n:L(p1)
        |WHERE (n:L OR n:P) AND (n.p1 = 1)
        |RETURN n""".stripMargin
    )

    // Possible improvement: We could have planned this as OrderedDistinct
    // It is impossible to make up statistics where an AllNodeScan would be better, so we will get the same plan even without the hint
    plan should (equal(
      cfg.planBuilder()
        .produceResults("n")
        .distinct("n AS n")
        .union()
        .|.nodeIndexOperator("n:P(p1 = 1)")
        .nodeIndexOperator("n:L(p1 = 1)")
        .build()
    ) or equal(
      cfg.planBuilder()
        .produceResults("n")
        .distinct("n AS n")
        .union()
        .|.nodeIndexOperator("n:L(p1 = 1)")
        .nodeIndexOperator("n:P(p1 = 1)")
        .build()
    ))
  }

  test("should work with index seeks of label disjunctions and solve two index hints") {
    val cfg = hintPlannerConfig()
      .addNodeIndex("L", Seq("p1"), 1.0, 1.0, providesOrder = IndexOrderCapability.BOTH)
      .addNodeIndex("P", Seq("p1"), 1.0, 1.0, providesOrder = IndexOrderCapability.BOTH)
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
    plan should (equal(
      cfg.planBuilder()
        .produceResults("n")
        .distinct("n AS n")
        .union()
        .|.nodeIndexOperator("n:P(p1 = 1)")
        .nodeIndexOperator("n:L(p1 = 1)")
        .build()
    ) or equal(
      cfg.planBuilder()
        .produceResults("n")
        .distinct("n AS n")
        .union()
        .|.nodeIndexOperator("n:L(p1 = 1)")
        .nodeIndexOperator("n:P(p1 = 1)")
        .build()
    ))
  }

  test("should work with label scan + filter on one side of label disjunctions if there is only one index") {
    val cfg = plannerConfig()
      .addNodeIndex("L", Seq("p1"), 0.5, 0.5, providesOrder = IndexOrderCapability.BOTH)
      .build()

    val plan = cfg.plan(
      """MATCH (n)
        |WHERE (n:L OR n:P) AND (n.p1 = 1)
        |RETURN n""".stripMargin
    )

    plan should (equal(
      cfg.planBuilder()
        .produceResults("n")
        .distinct("n AS n")
        .union()
        .|.filter("n.p1 = 1")
        .|.nodeByLabelScan("n", "P", IndexOrderAscending)
        .nodeIndexOperator("n:L(p1 = 1)")
        .build()
    ) or equal(
      cfg.planBuilder()
        .produceResults("n")
        .distinct("n AS n")
        .union()
        .|.nodeIndexOperator("n:L(p1 = 1)")
        .filter("n.p1 = 1")
        .nodeByLabelScan("n", "P", IndexOrderAscending)
        .build()
    ))
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
    plan should (equal(
      cfg.planBuilder()
        .produceResults("r")
        .distinct("r AS r", "a AS a", "b AS b")
        .union()
        .|.relationshipIndexOperator("(a)-[r:REL2(p1 = 1)]-(b)")
        .relationshipIndexOperator("(a)-[r:REL1(p1 = 1)]-(b)")
        .build()
    ) or equal(
      cfg.planBuilder()
        .produceResults("r")
        .distinct("r AS r", "a AS a", "b AS b")
        .union()
        .|.relationshipIndexOperator("(a)-[r:REL1(p1 = 1)]-(b)")
        .relationshipIndexOperator("(a)-[r:REL2(p1 = 1)]-(b)")
        .build()
    ))
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
    plan() should (equal(
      cfg.planBuilder()
        .produceResults("r")
        .distinct("r AS r", "a AS a", "b AS b")
        .union()
        .|.relationshipIndexOperator("(a)-[r:REL2(p1 = 1)]-(b)")
        .relationshipIndexOperator("(a)-[r:REL1(p1 = 1)]-(b)")
        .build()
    ) or equal(
      cfg.planBuilder()
        .produceResults("r")
        .distinct("r AS r", "a AS a", "b AS b")
        .union()
        .|.relationshipIndexOperator("(a)-[r:REL1(p1 = 1)]-(b)")
        .relationshipIndexOperator("(a)-[r:REL2(p1 = 1)]-(b)")
        .build()
    ))
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
    plan() should (equal(
      cfg.planBuilder()
        .produceResults("r")
        .distinct("r AS r", "a AS a", "b AS b")
        .union()
        .|.relationshipIndexOperator("(a)-[r:REL2(p1 = 1)]-(b)")
        .relationshipIndexOperator("(a)-[r:REL1(p1 = 1)]-(b)")
        .build()
    ) or equal(
      cfg.planBuilder()
        .produceResults("r")
        .distinct("r AS r", "a AS a", "b AS b")
        .union()
        .|.relationshipIndexOperator("(a)-[r:REL1(p1 = 1)]-(b)")
        .relationshipIndexOperator("(a)-[r:REL2(p1 = 1)]-(b)")
        .build()
    ))
    plan() should not equal plan(withHint = false)
  }

  test("should work with label scans of label disjunctions only") {
    val cfg = plannerConfig().build()

    val plan = cfg.plan(
      """MATCH (n)
        |WHERE (n:L OR n:P)
        |RETURN n""".stripMargin
    )

    plan should (equal(
      cfg.planBuilder()
        .produceResults("n")
        .orderedDistinct(Seq("n"), "n AS n")
        .orderedUnion(Seq(Ascending("n")))
        .|.nodeByLabelScan("n", "P", indexOrder = IndexOrderAscending)
        .nodeByLabelScan("n", "L", indexOrder = IndexOrderAscending)
        .build()
    ) or equal(
      cfg.planBuilder()
        .produceResults("n")
        .orderedDistinct(Seq("n"), "n AS n")
        .orderedUnion(Seq(Ascending("n")))
        .|.nodeByLabelScan("n", "L", indexOrder = IndexOrderAscending)
        .nodeByLabelScan("n", "P", indexOrder = IndexOrderAscending)
        .build()
    ))
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
    plan should (equal(
      cfg.planBuilder()
        .produceResults("n")
        .orderedDistinct(Seq("n"), "n AS n")
        .orderedUnion(Seq(Ascending("n")))
        .|.nodeByLabelScan("n", "P", IndexOrderAscending)
        .nodeByLabelScan("n", "L", IndexOrderAscending)
        .build()
    ) or equal(
      cfg.planBuilder()
        .produceResults("n")
        .orderedDistinct(Seq("n"), "n AS n")
        .orderedUnion(Seq(Ascending("n")))
        .|.nodeByLabelScan("n", "L", IndexOrderAscending)
        .nodeByLabelScan("n", "P", IndexOrderAscending)
        .build()
    ))
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
    plan should (equal(
      cfg.planBuilder()
        .produceResults("n")
        .orderedDistinct(Seq("n"), "n AS n")
        .orderedUnion(Seq(Ascending("n")))
        .|.nodeByLabelScan("n", "P", IndexOrderAscending)
        .nodeByLabelScan("n", "L", IndexOrderAscending)
        .build()
    ) or equal(
      cfg.planBuilder()
        .produceResults("n")
        .orderedDistinct(Seq("n"), "n AS n")
        .orderedUnion(Seq(Ascending("n")))
        .|.nodeByLabelScan("n", "L", IndexOrderAscending)
        .nodeByLabelScan("n", "P", IndexOrderAscending)
        .build()
    ))
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
        .orderedDistinct(Seq("r"), "r AS r", "a AS a", "b AS b")
        .orderedUnion(Seq(Ascending("r")))
        .|.relationshipTypeScan("(a)-[r:REL2]-(b)", indexOrder = IndexOrderAscending)
        .relationshipTypeScan("(a)-[r:REL1]-(b)", indexOrder = IndexOrderAscending)
        .build()
    ) or equal(
      cfg.planBuilder()
        .produceResults("r")
        .orderedDistinct(Seq("r"), "r AS r", "a AS a", "b AS b")
        .orderedUnion(Seq(Ascending("r")))
        .|.relationshipTypeScan("(a)-[r:REL1]-(b)", indexOrder = IndexOrderAscending)
        .relationshipTypeScan("(a)-[r:REL2]-(b)", indexOrder = IndexOrderAscending)
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

    plan() should (equal(
      cfg.planBuilder()
        .produceResults("r")
        .orderedDistinct(Seq("r"), "r AS r", "a AS a", "b AS b")
        .orderedUnion(Seq(Ascending("r")))
        .|.relationshipTypeScan("(a)-[r:REL2]-(b)", indexOrder = IndexOrderAscending)
        .relationshipTypeScan("(a)-[r:REL1]-(b)", indexOrder = IndexOrderAscending)
        .build()
    ) or equal(
      cfg.planBuilder()
        .produceResults("r")
        .orderedDistinct(Seq("r"), "r AS r", "a AS a", "b AS b")
        .orderedUnion(Seq(Ascending("r")))
        .|.relationshipTypeScan("(a)-[r:REL1]-(b)", indexOrder = IndexOrderAscending)
        .relationshipTypeScan("(a)-[r:REL2]-(b)", indexOrder = IndexOrderAscending)
        .build()
    ))
    plan() should not equal plan(withHint = false)
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

    plan() should (equal(
      cfg.planBuilder()
        .produceResults("r")
        .orderedDistinct(Seq("r"), "r AS r", "a AS a", "b AS b")
        .orderedUnion(Seq(Ascending("r")))
        .|.relationshipTypeScan("(a)-[r:REL2]-(b)", indexOrder = IndexOrderAscending)
        .relationshipTypeScan("(a)-[r:REL1]-(b)", indexOrder = IndexOrderAscending)
        .build()
    ) or equal(
      cfg.planBuilder()
        .produceResults("r")
        .orderedDistinct(Seq("r"), "r AS r", "a AS a", "b AS b")
        .orderedUnion(Seq(Ascending("r")))
        .|.relationshipTypeScan("(a)-[r:REL1]-(b)", indexOrder = IndexOrderAscending)
        .relationshipTypeScan("(a)-[r:REL2]-(b)", indexOrder = IndexOrderAscending)
        .build()
    ))
    plan() should not equal plan(withHint = false)
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
        .orderedDistinct(Seq("r"), "r AS r", "a AS a", "b AS b")
        .orderedUnion(Seq(Ascending("r")))
        .|.relationshipTypeScan("(a)-[r:REL2]-(b)", indexOrder = IndexOrderAscending)
        .relationshipTypeScan("(a)-[r:REL1]-(b)", indexOrder = IndexOrderAscending)
        .build()
    ) or equal(
      cfg.planBuilder()
        .produceResults("r")
        .orderedDistinct(Seq("r"), "r AS r", "a AS a", "b AS b")
        .orderedUnion(Seq(Ascending("r")))
        .|.relationshipTypeScan("(a)-[r:REL1]-(b)", indexOrder = IndexOrderAscending)
        .relationshipTypeScan("(a)-[r:REL2]-(b)", indexOrder = IndexOrderAscending)
        .build()
    ))
  }

  test("should work with index disjunction of conjunctions") {
    val cfg = plannerConfig()
      .addNodeIndex("L", Seq("p1"), 0.5, 0.5)
      .addNodeIndex("P", Seq("p1"), 0.5, 0.5)
      .build()

    val plan = cfg.plan(
      """MATCH (n)
        |WHERE (n:L) OR (n:P AND n.p1 = 1)
        |RETURN n""".stripMargin)

    // Possible improvement: This could be planned with a nodeByLabelScan and a nodeIndexSeek
    plan should (equal(
      cfg.planBuilder()
        .produceResults("n")
        .filterExpression(ors(hasLabels("n", "L"), propEquality("n", "p1", 1)))
        .orderedDistinct(Seq("n"), "n AS n")
        .orderedUnion(Seq(Ascending("n")))
        .|.nodeByLabelScan("n", "P", indexOrder = IndexOrderAscending)
        .nodeByLabelScan("n", "L", indexOrder = IndexOrderAscending)
        .build()
    ) or equal(
      cfg.planBuilder()
        .produceResults("n")
        .filterExpression(ors(hasLabels("n", "L"), propEquality("n", "p1", 1)))
        .orderedDistinct(Seq("n"), "n AS n")
        .orderedUnion(Seq(Ascending("n")))
        .|.nodeByLabelScan("n", "L", indexOrder = IndexOrderAscending)
        .nodeByLabelScan("n", "P", indexOrder = IndexOrderAscending)
        .build()
    ))
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
        .orderedDistinct(Seq("r"), "r AS r", "a AS a", "b AS b")
        .orderedUnion(Seq(Ascending("r")))
        .|.relationshipTypeScan("(a)-[r:REL2]-(b)", indexOrder = IndexOrderAscending)
        .relationshipTypeScan("(a)-[r:REL1]-(b)", indexOrder = IndexOrderAscending)
        .build()
    ) or equal(
      cfg.planBuilder()
        .produceResults("r")
        .filterExpression(ors(hasTypes("r", "REL1"), propEquality("r", "p1", 1)))
        .orderedDistinct(Seq("r"), "r AS r", "a AS a", "b AS b")
        .orderedUnion(Seq(Ascending("r")))
        .|.relationshipTypeScan("(a)-[r:REL1]-(b)", indexOrder = IndexOrderAscending)
        .relationshipTypeScan("(a)-[r:REL2]-(b)", indexOrder = IndexOrderAscending)
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

    plan should (equal(
      cfg.planBuilder()
        .produceResults("n")
        .orderedDistinct(Seq("n"), "n AS n")
        .orderedUnion(Seq(Ascending("n")))
        .|.nodeByLabelScan("n", "P", IndexOrderAscending)
        .nodeByLabelScan("n", "L", IndexOrderAscending)
        .build()
    ) or equal(
      cfg.planBuilder()
        .produceResults("n")
        .orderedDistinct(Seq("n"), "n AS n")
        .orderedUnion(Seq(Ascending("n")))
        .|.nodeByLabelScan("n", "L", IndexOrderAscending)
        .nodeByLabelScan("n", "P", IndexOrderAscending)
        .build()
    ))
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
        .orderedDistinct(Seq("r"), "r AS r", "a AS a", "b AS b")
        .orderedUnion(Seq(Ascending("r")))
        .|.relationshipTypeScan("(a)-[r:REL2]->(b)", indexOrder = IndexOrderAscending)
        .relationshipTypeScan("(a)-[r:REL1]->(b)", indexOrder = IndexOrderAscending)
        .build()
    ) or equal(
      cfg.planBuilder()
        .produceResults("r")
        .orderedDistinct(Seq("r"), "r AS r", "a AS a", "b AS b")
        .orderedUnion(Seq(Ascending("r")))
        .|.relationshipTypeScan("(a)-[r:REL1]->(b)", indexOrder = IndexOrderAscending)
        .relationshipTypeScan("(a)-[r:REL2]->(b)", indexOrder = IndexOrderAscending)
        .build()
    ))
  }

  test("should prefer node index scan from existence constraint to label scan with same cardinality, if indexed property is used") {
    val cfg = plannerConfig()
      .addNodeIndex("L", Seq("p1"), 1.0, 0.1, withValues = true)
      .addNodeExistenceConstraint("L", "p1")
      .addNodeIndex("P", Seq("p1"), 1.0, 0.1, withValues = true)
      .addNodeExistenceConstraint("P", "p1")
      .build()

    val plan = cfg.plan(s"MATCH (n) WHERE n:L or n:P RETURN n.p1 AS p")

    plan should (equal(
      cfg.planBuilder()
        .produceResults("p")
        .projection("cacheN[n.p1] AS p")
        .distinct("n AS n")
        .union()
        .|.nodeIndexOperator("n:L(p1)", getValue = _ => GetValue)
        .nodeIndexOperator("n:P(p1)", getValue = _ => GetValue)
        .build()
    ) or equal(
      cfg.planBuilder()
        .produceResults("p")
        .projection("cacheN[n.p1] AS p")
        .distinct("n AS n")
        .union()
        .|.nodeIndexOperator("n:P(p1)", getValue = _ => GetValue)
        .nodeIndexOperator("n:L(p1)", getValue = _ => GetValue)
        .build()
    ))
  }

  test("should prefer relationship index scan from existence constraint to type scan with same cardinality, if indexed property is used") {
    val cfg = plannerConfig()
      .addRelationshipIndex("REL1", Seq("p1"), 1.0, 0.1, withValues = true)
      .addRelationshipExistenceConstraint("REL1", "p1")
      .addRelationshipIndex("REL2", Seq("p1"), 1.0, 0.1, withValues = true)
      .addRelationshipExistenceConstraint("REL2", "p1")
      .build()

    val plan = cfg.plan(s"MATCH (a)-[r:REL1|REL2]->(b) RETURN r.p1 AS p")

    plan should (equal(
      cfg.planBuilder()
        .produceResults("p")
        .projection("cacheR[r.p1] AS p")
        .distinct("r AS r", "a AS a", "b AS b")
        .union()
        .|.relationshipIndexOperator("(a)-[r:REL1(p1)]->(b)", getValue = _ => GetValue)
        .relationshipIndexOperator("(a)-[r:REL2(p1)]->(b)", getValue = _ => GetValue)
        .build()
    ) or equal(
      cfg.planBuilder()
        .produceResults("p")
        .projection("cacheR[r.p1] AS p")
        .distinct("r AS r", "a AS a", "b AS b")
        .union()
        .|.relationshipIndexOperator("(a)-[r:REL2(p1)]->(b)", getValue = _ => GetValue)
        .relationshipIndexOperator("(a)-[r:REL1(p1)]->(b)", getValue = _ => GetValue)
        .build()
    ))
  }

  test("should prefer node index scan from aggregation to node index scan from existence constraint with same cardinality") {
    val cfg = plannerConfig()
      .addNodeIndex("L", Seq("p1"), 1.0, 0.1, withValues = true)
      .addNodeExistenceConstraint("L", "p1")
      .addNodeIndex("L", Seq("p2"), 1.0, 0.1, withValues = true)
      .addNodeIndex("P", Seq("p1"), 1.0, 0.1, withValues = true)
      .addNodeExistenceConstraint("P", "p1")
      .addNodeIndex("P", Seq("p2"), 1.0, 0.1, withValues = true)
      .build()

    val plan = cfg.plan(s"MATCH (n) WHERE n:L or n:P RETURN count(n.p2) AS c")

    plan should (equal(
      cfg.planBuilder()
        .produceResults("c")
        .aggregation(Seq(), Seq("count(cacheN[n.p2]) AS c"))
        .distinct("n AS n")
        .union()
        .|.nodeIndexOperator("n:L(p2)", getValue = _ => GetValue)
        .nodeIndexOperator("n:P(p2)", getValue = _ => GetValue)
        .build()
    ) or equal(
      cfg.planBuilder()
        .produceResults("c")
        .aggregation(Seq(), Seq("count(cacheN[n.p2]) AS c"))
        .distinct("n AS n")
        .union()
        .|.nodeIndexOperator("n:P(p2)", getValue = _ => GetValue)
        .nodeIndexOperator("n:L(p2)", getValue = _ => GetValue)
        .build()
    ))
  }

  test("should prefer relationship index scan from aggregation to relationship index scan from existence constraint with same cardinality") {
    val cfg = plannerConfig()
      .addRelationshipIndex("REL1", Seq("p1"), 1.0, 0.1, withValues = true)
      .addRelationshipExistenceConstraint("REL1", "p1")
      .addRelationshipIndex("REL1", Seq("p2"), 1.0, 0.1, withValues = true)
      .addRelationshipIndex("REL2", Seq("p1"), 1.0, 0.1, withValues = true)
      .addRelationshipExistenceConstraint("REL2", "p1")
      .addRelationshipIndex("REL2", Seq("p2"), 1.0, 0.1, withValues = true)
      .build()

    val plan = cfg.plan(s"MATCH (a)-[r:REL1|REL2]->(b) RETURN count(r.p2) AS c")

    plan should (equal(
      cfg.planBuilder()
        .produceResults("c")
        .aggregation(Seq(), Seq("count(cacheR[r.p2]) AS c"))
        .distinct("r AS r", "a AS a", "b AS b")
        .union()
        .|.relationshipIndexOperator("(a)-[r:REL1(p2)]->(b)", getValue = _ => GetValue)
        .relationshipIndexOperator("(a)-[r:REL2(p2)]->(b)", getValue = _ => GetValue)
        .build()
    ) or equal(
      cfg.planBuilder()
        .produceResults("c")
        .aggregation(Seq(), Seq("count(cacheR[r.p2]) AS c"))
        .distinct("r AS r", "a AS a", "b AS b")
        .union()
        .|.relationshipIndexOperator("(a)-[r:REL2(p2)]->(b)", getValue = _ => GetValue)
        .relationshipIndexOperator("(a)-[r:REL1(p2)]->(b)", getValue = _ => GetValue)
        .build()
    ))
  }

  test("should prefer node index scan for aggregated property, even if other property is referenced") {
    val cfg = plannerConfig()
      .addNodeIndex("L", Seq("p1"), 1.0, 0.1, withValues = true)
      .addNodeExistenceConstraint("L", "p1")
      .addNodeIndex("L", Seq("p2"), 1.0, 0.1, withValues = true)
      .addNodeIndex("P", Seq("p1"), 1.0, 0.1, withValues = true)
      .addNodeExistenceConstraint("P", "p1")
      .addNodeIndex("P", Seq("p2"), 1.0, 0.1, withValues = true)
      .build()

    val plan = cfg.plan(s"MATCH (n) WHERE (n:L or n:P) AND n.p1 <> 1 RETURN count(n.p2) AS c")

    plan should (equal(
      cfg.planBuilder()
        .produceResults("c")
        .aggregation(Seq(), Seq("count(cacheN[n.p2]) AS c"))
        .distinct("n AS n")
        .union()
        .|.filter("not n.p1 = 1")
        .|.nodeIndexOperator("n:L(p2)", getValue = _ => GetValue)
        .filter("not n.p1 = 1")
        .nodeIndexOperator("n:P(p2)", getValue = _ => GetValue)
        .build()
    ) or equal(
      cfg.planBuilder()
        .produceResults("c")
        .aggregation(Seq(), Seq("count(cacheN[n.p2]) AS c"))
        .distinct("n AS n")
        .union()
        .|.filter("not n.p1 = 1")
        .|.nodeIndexOperator("n:P(p2)", getValue = _ => GetValue)
        .filter("not n.p1 = 1")
        .nodeIndexOperator("n:L(p2)", getValue = _ => GetValue)
        .build()
    ))
  }

  test("should prefer relationship index scan for aggregated property, even if other property is referenced") {
    val cfg = plannerConfig()
      .addRelationshipIndex("REL1", Seq("p1"), 1.0, 0.01, withValues = true)
      .addRelationshipExistenceConstraint("REL1", "p1")
      .addRelationshipIndex("REL1", Seq("p2"), 1.0, 0.01, withValues = true)
      .addRelationshipIndex("REL2", Seq("p1"), 1.0, 0.01, withValues = true)
      .addRelationshipExistenceConstraint("REL2", "p1")
      .addRelationshipIndex("REL2", Seq("p2"), 1.0, 0.01, withValues = true)
      .build()

    val plan = cfg.plan(s"MATCH (a)-[r:REL1|REL2]->(b) WHERE r.p1 <> 1 RETURN count(r.p2) AS c")

    plan should (equal(
      cfg.planBuilder()
        .produceResults("c")
        .aggregation(Seq(), Seq("count(cacheR[r.p2]) AS c"))
        .distinct("r AS r", "a AS a", "b AS b")
        .union()
        .|.filter("not r.p1 = 1")
        .|.relationshipIndexOperator("(a)-[r:REL1(p2)]->(b)", getValue = _ => GetValue)
        .filter("not r.p1 = 1")
        .relationshipIndexOperator("(a)-[r:REL2(p2)]->(b)", getValue = _ => GetValue)
        .build()
    ) or equal(
      cfg.planBuilder()
        .produceResults("c")
        .aggregation(Seq(), Seq("count(cacheR[r.p2]) AS c"))
        .distinct("r AS r", "a AS a", "b AS b")
        .union()
        .|.filter("not r.p1 = 1")
        .|.relationshipIndexOperator("(a)-[r:REL2(p2)]->(b)", getValue = _ => GetValue)
        .filter("not r.p1 = 1")
        .relationshipIndexOperator("(a)-[r:REL1(p2)]->(b)", getValue = _ => GetValue)
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

    plan.treeCount {
      case _: NodeIndexSeek => true
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

    plan should (equal(
      cfg.planBuilder()
        .produceResults("n")
        .distinct("n AS n")
        .union()
        .|.filter("n:L")
        .|.nodeByIdSeek("n", Set(), 1)
        .nodeIndexOperator("n:L(prop > 123)", argumentIds = Set(), getValue = Map("prop" -> DoNotGetValue))
        .build()
    ) or equal(
      cfg.planBuilder()
        .produceResults("n")
        .distinct("n AS n")
        .union()
        .|.nodeIndexOperator("n:L(prop > 123)", argumentIds = Set(), getValue = Map("prop" -> DoNotGetValue))
        .filter("n:L")
        .nodeByIdSeek("n", Set(), 1)
        .build()
    ))
  }

  private def runWithTimeout[T](timeout: Long)(f: => T): T = {
    Await.result(scala.concurrent.Future(f)(scala.concurrent.ExecutionContext.global), Duration.apply(timeout, "s"))
  }
}
