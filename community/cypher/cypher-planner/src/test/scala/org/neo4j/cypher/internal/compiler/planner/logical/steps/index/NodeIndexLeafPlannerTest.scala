package org.neo4j.cypher.internal.compiler.planner.logical.steps.index

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.compiler.planner.logical.LeafPlanRestrictions
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class NodeIndexLeafPlannerTest extends CypherFunSuite with LogicalPlanningTestSupport2 with AstConstructionTestSupport {

  private val planner = new NodeIndexPlanner(Seq.empty, LeafPlanRestrictions.NoRestrictions)

  test("testFindIndexCompatiblePredicates on hasLabel with label with constraint") {

    new given {
      qg = QueryGraph()
      nodeConstraints = Set(("A", Set("prop1")))
    } withLogicalPlanningContext { (_, context) =>
      val compatiblePredicates = planner.findIndexCompatiblePredicates(Set(hasLabels("n", "A")), Set.empty, context)
      compatiblePredicates.size shouldBe 1
      val predicate = exists(prop("n", "prop1"))
      compatiblePredicates.foreach { compatiblePredicate =>
        compatiblePredicate.predicate should be(predicate)
      }
    }
  }

  test("testFindIndexCompatiblePredicates on hasLabel with label without constraint") {

    new given {
      qg = QueryGraph()
    } withLogicalPlanningContext { (_, context) =>
      val compatiblePredicates = planner.findIndexCompatiblePredicates(Set(hasLabels("n", "A")), Set.empty, context)
      compatiblePredicates shouldBe empty
    }
  }
}
