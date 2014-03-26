package org.neo4j.cypher.internal.compiler.v2_1.planner.logical

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_1.planner.{Planner, LogicalPlanningTestSupport}
import org.neo4j.cypher.internal.compiler.v2_1.parser.{ParserMonitor, CypherParser}
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.{SingleRow, Projection, LogicalPlan}
import org.neo4j.cypher.internal.compiler.v2_1.spi.PlanContext
import org.neo4j.cypher.internal.compiler.v2_1.ast.{SignedIntegerLiteral, Query}

class LogicalPlanningAcceptanceTest extends CypherFunSuite with LogicalPlanningTestSupport {

  val parser = new CypherParser(mock[ParserMonitor])
  val planner = new Planner(monitors)

  test("should build plans containing single row") {
    val plan = produceLogicalPlan("return 42") {
      case _ => 100
    }
    implicit val context = plan.context

    plan should equal(
      Projection(
        SingleRow(), expressions = Map("42" -> SignedIntegerLiteral("42")_)
      ))
  }

  def produceLogicalPlan(queryText: String)(estimator: PartialFunction[LogicalPlan, Int], planContext: PlanContext = newMockedPlanContext) =
    parser.parse(queryText) match {
      case ast: Query =>
        planner.produceLogicalPlan(ast, null, CardinalityEstimator.lift(estimator))(planContext)
      case _ =>
        throw new IllegalArgumentException("produceLogicalPlan only supports ast.Query input")
    }
}
