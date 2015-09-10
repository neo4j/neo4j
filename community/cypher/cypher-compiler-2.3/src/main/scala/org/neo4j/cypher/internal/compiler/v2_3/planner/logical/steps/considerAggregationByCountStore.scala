package org.neo4j.cypher.internal.compiler.v2_3.planner.logical.steps

import org.neo4j.cypher.internal.compiler.v2_3.planner.{AggregatingQueryProjection, PlannerQuery}
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.{LogicalPlanningContext, PlanTransformer}
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans._
import org.neo4j.cypher.internal.frontend.v2_3.ast.{FunctionName, FunctionInvocation, Identifier}

object considerAggregationByCountStore extends PlanTransformer[PlannerQuery] {

  def apply(plan: LogicalPlan, query: PlannerQuery)(implicit context: LogicalPlanningContext) = query.horizon match {
    case AggregatingQueryProjection(groupingKeys, aggregatingExpressions, shuffle)
      if (groupingKeys.size == 0 && aggregatingExpressions.size == 1) => aggregatingExpressions.head._2 match {
      case FunctionInvocation(FunctionName("count"), false, Vector(Identifier(countName))) => plan match {
        case a: Aggregation => a.left match {
          case AllNodesScan(nodeId, argumentIds) if (countName == nodeId.name) =>
            context.logicalPlanProducer.planCountStoreNodeAggregation(plan, nodeId, None, argumentIds)
          // MATCH ()-[r]->(), MATCH ()-[r:X]->(), MATCH ()-[r:X|Y]->()
          case Expand(AllNodesScan(nodeId, argumentIds), from, direction, types, to, relId, ExpandAll) if (countName == relId.name) =>
            context.logicalPlanProducer.planCountStoreRelationshipAggregation(plan, relId, None, types, None, argumentIds)
          case NodeByLabelScan(nodeId, label, argumentIds) if (countName == nodeId.name) =>
            context.logicalPlanProducer.planCountStoreNodeAggregation(plan, nodeId, Some(label.name), argumentIds)
          case _ => plan
        }
        case _ => plan
      }
      case _ => plan
    }
    case _ => plan
  }
}
