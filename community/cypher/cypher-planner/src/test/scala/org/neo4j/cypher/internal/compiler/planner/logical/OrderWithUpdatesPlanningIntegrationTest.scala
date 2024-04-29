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

import org.neo4j.cypher.internal.compiler.planner.BeLikeMatcher.beLike
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningIntegrationTestSupport
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfigurationBuilder
import org.neo4j.cypher.internal.frontend.phases.ProcedureReadOnlyAccess
import org.neo4j.cypher.internal.frontend.phases.ProcedureReadWriteAccess
import org.neo4j.cypher.internal.frontend.phases.ProcedureSignature
import org.neo4j.cypher.internal.frontend.phases.QualifiedName
import org.neo4j.cypher.internal.logical.plans.Apply
import org.neo4j.cypher.internal.logical.plans.Create
import org.neo4j.cypher.internal.logical.plans.DeleteExpression
import org.neo4j.cypher.internal.logical.plans.DeleteNode
import org.neo4j.cypher.internal.logical.plans.DeletePath
import org.neo4j.cypher.internal.logical.plans.DeleteRelationship
import org.neo4j.cypher.internal.logical.plans.Foreach
import org.neo4j.cypher.internal.logical.plans.ForeachApply
import org.neo4j.cypher.internal.logical.plans.IndexOrderAscending
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.Merge
import org.neo4j.cypher.internal.logical.plans.NodeIndexScan
import org.neo4j.cypher.internal.logical.plans.ProcedureCall
import org.neo4j.cypher.internal.logical.plans.ProduceResult
import org.neo4j.cypher.internal.logical.plans.RemoveLabels
import org.neo4j.cypher.internal.logical.plans.SetLabels
import org.neo4j.cypher.internal.logical.plans.SetNodePropertiesFromMap
import org.neo4j.cypher.internal.logical.plans.SetNodeProperty
import org.neo4j.cypher.internal.logical.plans.SetPropertiesFromMap
import org.neo4j.cypher.internal.logical.plans.SetProperty
import org.neo4j.cypher.internal.logical.plans.SetRelationshipPropertiesFromMap
import org.neo4j.cypher.internal.logical.plans.SetRelationshipProperty
import org.neo4j.cypher.internal.logical.plans.Sort
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class OrderWithUpdatesIDPPlanningIntegrationTest extends OrderWithUpdatesPlanningIntegrationTestBase(true)
class OrderWithUpdatesGreedyPlanningIntegrationTest extends OrderWithUpdatesPlanningIntegrationTestBase(false)

class OrderWithUpdatesPlanningIntegrationTestBase(useIDPConnectComponents: Boolean)
    extends CypherFunSuite
    with LogicalPlanningIntegrationTestSupport {

  override def plannerBuilder(): StatisticsBackedLogicalPlanningConfigurationBuilder =
    super.plannerBuilder()
      .enableConnectComponentsPlanner(useIDPConnectComponents)

  test("SetProperty should eliminate provided order and cause planning Sort") {
    shouldEliminateProvidedSortOrder(
      "UNWIND [n, r] AS x SET x.prop = 1",
      { case _: SetProperty => true }
    )
  }

  test("SetPropertiesFromMap should eliminate provided order and cause planning Sort") {
    shouldEliminateProvidedSortOrder(
      "UNWIND [n, r] AS x SET x = {prop: 1}",
      { case _: SetPropertiesFromMap => true }
    )
  }

  test("SetNodeProperty should eliminate provided order and cause planning Sort") {
    shouldEliminateProvidedSortOrder(
      "SET n.prop = n.foo",
      { case _: SetNodeProperty => true }
    )
  }

  test("SetNodePropertiesFromMap should eliminate provided order and cause planning Sort") {
    shouldEliminateProvidedSortOrder(
      "SET n = {prop: 'something', foo: 'something'}",
      { case _: SetNodePropertiesFromMap => true }
    )
  }

  test("SetRelationshipProperty should eliminate provided order and cause planning Sort") {
    shouldEliminateProvidedSortOrder(
      "SET r.prop = 1",
      { case _: SetRelationshipProperty => true }
    )
  }

  test("SetRelationshipPropertiesFromMap should eliminate provided order and cause planning Sort") {
    shouldEliminateProvidedSortOrder(
      "SET r = {prop: 1}",
      { case _: SetRelationshipPropertiesFromMap => true }
    )
  }

  test("ProcedureCall of readwrite procedure should eliminate provided order and cause planning Sort") {
    shouldEliminateProvidedSortOrder(
      "CALL writer()",
      { case _: ProcedureCall => true }
    )
  }

  test("ProcedureCall of readonly procedure should not eliminate provided order and cause planning Sort") {
    shouldRetainProvidedSortOrder(
      "CALL reader()",
      { case _: ProcedureCall => true }
    )
  }

  test("Create should eliminate provided order and cause planning Sort") {
    shouldEliminateProvidedSortOrder(
      "CREATE (x)",
      { case _: Create => true }
    )
  }

  test("Merge node should not eliminate provided order and cause planning Sort") {
    shouldRetainProvidedSortOrder(
      "MERGE (x:N)",
      { case _: Merge => true }
    )
  }

  test("Merge relationship should not eliminate provided order and cause planning Sort") {
    shouldRetainProvidedSortOrder(
      "MERGE ()-[x:R]-()",
      { case _: Merge => true }
    )
  }

  test("DeleteNode should eliminate provided order and cause planning Sort") {
    shouldEliminateProvidedSortOrder(
      "DELETE n",
      { case _: DeleteNode => true }
    )
  }

  test("DeleteRelationship should eliminate provided order and cause planning Sort") {
    shouldEliminateProvidedSortOrder(
      "DELETE r",
      { case _: DeleteRelationship => true }
    )
  }

  test("DeletePath should eliminate provided order and cause planning Sort") {
    shouldEliminateProvidedSortOrder(
      "MATCH p = ()-[]-() DELETE p",
      { case _: DeletePath => true }
    )
  }

  test("DeleteExpression should eliminate provided order and cause planning Sort") {
    shouldEliminateProvidedSortOrder(
      "WITH *, {prop: n} AS m DELETE m.prop",
      { case _: DeleteExpression => true }
    )
  }

  test("Setlabel should eliminate provided order and cause planning Sort") {
    shouldEliminateProvidedSortOrder(
      "SET n:X",
      { case _: SetLabels => true }
    )
  }

  test("RemoveLabel should eliminate provided order and cause planning Sort") {
    shouldEliminateProvidedSortOrder(
      "REMOVE n:N:X",
      { case _: RemoveLabels => true }
    )
  }

  test("TailApply containing update should eliminate provided order and cause planning Sort") {
    shouldEliminateProvidedSortOrder(
      "WITH *, 1 as x MATCH (y) SET n.prop = x",
      { case _: Apply => true }
    )
  }

  test("Foreach containing update should eliminate provided order and cause planning Sort") {
    shouldEliminateProvidedSortOrder(
      "FOREACH (x in [1,2,3] | SET n.prop = x)",
      { case _: Foreach => true }
    )
  }

  test("ForeachApply containing update should eliminate provided order and cause planning Sort") {
    shouldEliminateProvidedSortOrder(
      "FOREACH (x in [1,2,3] | MERGE (m)-[:R]-(n) ON MATCH SET m:L)",
      { case _: ForeachApply => true }
    )
  }

  test("Subquery (uncorrelated) with update should eliminate provided order and cause planning Sort") {
    shouldEliminateProvidedSortOrder(
      "CALL {MATCH (x) SET x.prop = 1 RETURN 'foo' AS foo}",
      { case _: Apply => true }
    )
  }

  test("Subquery (correlated) with update should eliminate provided order and cause planning Sort") {
    shouldEliminateProvidedSortOrder(
      "CALL {WITH n, r MATCH (x) SET x.prop = 1 RETURN 'foo' AS foo}",
      { case _: Apply => true }
    )
  }

  test("MERGE node + ON MATCH with update should eliminate provided order and cause planning Sort") {
    shouldEliminateProvidedSortOrder(
      "MERGE (x) ON MATCH SET x.prop = 1",
      { case _: Merge => true }
    )
  }

  test("MERGE node + ON CREATE with update should not eliminate provided order and cause planning Sort") {
    shouldRetainProvidedSortOrder(
      "MERGE (x) ON CREATE SET x.prop = 1",
      { case _: Merge => true }
    )
  }

  test("Merge relationship + ON MATCH should eliminate provided order and cause planning Sort") {
    shouldEliminateProvidedSortOrder(
      "MERGE ()-[x:R]-() ON MATCH set x.prop = 1",
      { case _: Merge => true }
    )
  }

  test("Merge relationship + ON CREATE should not eliminate provided order and cause planning Sort") {
    shouldRetainProvidedSortOrder(
      "MERGE ()-[x:R]-() ON CREATE set x.prop = 1",
      { case _: Merge => true }
    )
  }

  private def shouldRetainProvidedSortOrder(clause: String, containsTestedPlan: PartialFunction[Any, Boolean]): Unit = {
    verifyProvidedSortOrder(
      clause,
      containsTestedPlan,
      sortCheck = plan =>
        withClue(s"Should not contain sort: $plan")(
          plan.folder.treeExists { case _: Sort => true } shouldBe false
        )
    )
  }

  private def shouldEliminateProvidedSortOrder(
    clause: String,
    containsTestedPlan: PartialFunction[Any, Boolean]
  ): Unit = {
    verifyProvidedSortOrder(
      clause,
      containsTestedPlan,
      sortCheck = plan =>
        withClue(s"Did not plan sort: $plan")(
          plan should beLike { case ProduceResult(Sort(_, _), _, _) => }
        )
    )
  }

  private def verifyProvidedSortOrder(
    clause: String,
    containsTestedPlan: PartialFunction[Any, Boolean],
    sortCheck: LogicalPlan => Unit
  ): Unit = {
    val writer = ProcedureSignature(
      QualifiedName(Seq(), "writer"),
      IndexedSeq(),
      None,
      None,
      ProcedureReadWriteAccess,
      id = 0
    )

    val reader = ProcedureSignature(
      QualifiedName(Seq(), "reader"),
      IndexedSeq(),
      None,
      None,
      ProcedureReadOnlyAccess,
      id = 1
    )

    val cfg = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("N", 100)
      .setAllRelationshipsCardinality(100)
      .setRelationshipCardinality("(:N)-[]-()", 100)
      .setRelationshipCardinality("(:N)-[:R]-()", 100)
      .setRelationshipCardinality("()-[:R]-()", 100)
      .addNodeIndex("N", Seq("prop"), 1.0, 0.3)
      .addProcedure(writer)
      .addProcedure(reader)
      .build()

    val plan = cfg.plan(
      s"""MATCH (n:N)-[r:R]-() WHERE n.prop IS NOT NULL
         |$clause
         |RETURN n
         |ORDER BY n.prop""".stripMargin
    )

    withClue(s"Did not contain indexSeek with provided order: $plan")(
      providesOrder(plan) shouldBe true
    )

    withClue(s"Did not contain the expected plan: $plan")(
      containsPlan(plan, containsTestedPlan) shouldBe true
    )

    sortCheck(plan)
  }

  private def containsPlan(plan: LogicalPlan, f: PartialFunction[Any, Boolean]) = plan.folder.treeExists(f)

  private def providesOrder(plan: LogicalPlan) =
    plan.folder.treeExists { case NodeIndexScan(_, _, _, _, IndexOrderAscending, _, _) => true }
}
