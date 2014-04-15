package org.neo4j.cypher.internal.compiler.v2_1.planner.logical

import org.neo4j.graphdb.Direction
import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_1.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_1.ast._
import org.mockito.Mockito._
import org.mockito.Matchers._

class OptionalMatchIntegrationTest extends CypherFunSuite with LogicalPlanningTestSupport   {

  test("should build plans containing joins") {
    implicit val planContext = newMockedPlanContext
    val factory = newMockedMetricsFactory
    when(factory.newCardinalityEstimator(any(), any())).thenReturn((plan: LogicalPlan) => plan match {
      case _: AllNodesScan => 2000000
      case _: NodeByLabelScan => 20
      case _: Expand => 10
      case _: OuterHashJoin => 20
      case _ => Double.MaxValue
    })
    implicit val planner = newPlanner(factory)
    when(planContext.getOptLabelId("X")).thenReturn(None)
    when(planContext.getOptLabelId("Y")).thenReturn(None)

    produceLogicalPlan("MATCH (a:X)-[r1]->(b) OPTIONAL MATCH (b)-[r2]->(c:Y) RETURN b") should equal(
      Projection(
        OuterHashJoin("b",
          Expand(NodeByLabelScan("a", Left("X"))(), "a", Direction.OUTGOING, Seq(), "b", "r1", SimplePatternLength)(null),
          Expand(NodeByLabelScan("c", Left("Y"))(), "c", Direction.INCOMING, Seq(), "b", "r2", SimplePatternLength)(null)
        ),
        expressions = Map("b" -> Identifier("b") _)
      )
    )
  }
}
