package org.neo4j.cypher.internal.compiler.planner.logical.plans

import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.compiler.planner.logical.steps.labelScanLeafPlanner
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.planner.logical.steps.relationshipTypeScanLeafPlanner
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.SemanticDirection.BOTH
import org.neo4j.cypher.internal.expressions.SemanticDirection.INCOMING
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.ir.VarPatternLength
import org.neo4j.cypher.internal.ir.ordering.InterestingOrder
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipTypeScan
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipTypeScan
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class RelationshipTypeScanLeafPlannerTest extends CypherFunSuite with LogicalPlanningTestSupport {
  test("simple outgoing directed type scan") {
    // given
    val semanticTable = new SemanticTable()
    val context = newMockedLogicalPlanningContext(planContext = newMockedPlanContext(), semanticTable = semanticTable)
    //(a)-[:R]->(b)
    val qg = pattern("r", "a", "b", OUTGOING, "R")

    // when
    val resultPlans = relationshipTypeScanLeafPlanner(Set.empty)(qg, InterestingOrder.empty, context)

    // then
    resultPlans should equal(Seq(
      DirectedRelationshipTypeScan("r", "a", relTypeName("R"), "b", Set.empty)
    ))
  }

  test("simple incoming directed type scan") {
    // given
    val semanticTable = new SemanticTable()
    val context = newMockedLogicalPlanningContext(planContext = newMockedPlanContext(), semanticTable = semanticTable)
    //(a)<-[:R]-(b)
    val qg = pattern("r", "a", "b", INCOMING, "R")

    // when
    val resultPlans = relationshipTypeScanLeafPlanner(Set.empty)(qg, InterestingOrder.empty, context)

    // then
    resultPlans should equal(Seq(
      DirectedRelationshipTypeScan("r", "b", relTypeName("R"), "a", Set.empty)
    ))
  }

  test("simple undirected type scan") {
    // given
    val semanticTable = new SemanticTable()
    val context = newMockedLogicalPlanningContext(planContext = newMockedPlanContext(), semanticTable = semanticTable)
    //(a)-[:R]-(b)
    val qg = pattern("r", "a", "b", BOTH, "R")

    // when
    val resultPlans = relationshipTypeScanLeafPlanner(Set.empty)(qg, InterestingOrder.empty, context)

    // then
    resultPlans should equal(Seq(
      UndirectedRelationshipTypeScan("r", "a", relTypeName("R"), "b", Set.empty)
    ))
  }

  test("should not scan if multiple types") {
    // given
    val semanticTable = new SemanticTable()
    val context = newMockedLogicalPlanningContext(planContext = newMockedPlanContext(), semanticTable = semanticTable)
    //(a)-[:R1|R2]->(b)
    val qg = pattern("r", "a", "b", OUTGOING, "R1", "R2")

    // when
    val resultPlans = relationshipTypeScanLeafPlanner(Set.empty)(qg, InterestingOrder.empty, context)

    // then
    resultPlans shouldBe empty
  }

  test("should not scan if variable length pattern") {
    // given
    val semanticTable = new SemanticTable()
    val context = newMockedLogicalPlanningContext(planContext = newMockedPlanContext(), semanticTable = semanticTable)
    //(a)-[:R*]->(b)
    val qg =varPattern("r", "a", "b", OUTGOING, "R")

    // when
    val resultPlans = relationshipTypeScanLeafPlanner(Set.empty)(qg, InterestingOrder.empty, context)

    // then
    resultPlans shouldBe empty
  }

  test("should not plan type scan for skipped ids") {
    // given
    val context = planningContext()
    //(a)-[:R]->(b)
    val qg = pattern("r", "a", "b", OUTGOING, "R")

    // then
    relationshipTypeScanLeafPlanner(Set("r"))(qg, InterestingOrder.empty, context) should be(empty)
  }

  test("should not plan type scan if no type index") {
    // given
    val context = planningContext(typeScanEnabled = false)

      // then
      labelScanLeafPlanner(Set("n"))(qg, InterestingOrder.empty, context) should be(empty)
      labelScanLeafPlanner(Set("a"))(qg, InterestingOrder.empty, context) should be(empty)
      labelScanLeafPlanner(Set("b"))(qg, InterestingOrder.empty, context) should be(empty)
    }

  private def pattern(name: String, from: String, to: String, direction: SemanticDirection, types: String*) =
    QueryGraph(
      patternNodes = Set(name,  from, to),
      patternRelationships = Set(PatternRelationship(name, (from, to), direction, types.map(relTypeName), SimplePatternLength)))

  private def varPattern(name: String, from: String, to: String, direction: SemanticDirection, types: String*) =
    QueryGraph(
      patternNodes = Set(name,  from, to),
      patternRelationships = Set(PatternRelationship(name, (from, to), direction, types.map(relTypeName), VarPatternLength(1, None))))

}
