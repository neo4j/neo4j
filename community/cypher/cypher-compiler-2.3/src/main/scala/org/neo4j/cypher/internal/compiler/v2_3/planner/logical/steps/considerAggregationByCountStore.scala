package org.neo4j.cypher.internal.compiler.v2_3.planner.logical.steps

import org.neo4j.cypher.internal.compiler.v2_3.planner.{AggregatingQueryProjection, PlannerQuery}
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.{LogicalPlanningContext, PlanTransformer}
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans._
import org.neo4j.cypher.internal.frontend.v2_3.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.frontend.v2_3.ast.{HasLabels, FunctionName, FunctionInvocation, Identifier}

object considerAggregationByCountStore extends PlanTransformer[PlannerQuery] {

  def apply(plan: LogicalPlan, query: PlannerQuery)(implicit context: LogicalPlanningContext) = query.horizon match {
    case AggregatingQueryProjection(groupingKeys, aggregatingExpressions, shuffle)
      if (groupingKeys.size == 0 && aggregatingExpressions.size == 1) => aggregatingExpressions.head._2 match {
      case FunctionInvocation(FunctionName("count"), false, Vector(Identifier(countName))) => plan match {
        case a: Aggregation => a.left match {
          // MATCH (n) RETURN count(n)
          case AllNodesScan(nodeId, argumentIds) if (countName == nodeId.name) =>
            context.logicalPlanProducer.planCountStoreNodeAggregation(plan, nodeId, None, argumentIds)
          // MATCH ()-[r]->(), MATCH ()-[r:X]->(), MATCH ()-[r:X|Y]->()
          case Expand(AllNodesScan(nodeId, argumentIds), from, direction, types, to, relId, ExpandAll) if (countName == relId.name) =>
            context.logicalPlanProducer.planCountStoreRelationshipAggregation(plan, relId, None, types, None, argumentIds)
          // MATCH (:A)-[r]->(), MATCH (:A)<-[r]-()
          case Expand(NodeByLabelScan(nodeId, label, argumentIds), from, direction, types, to, relId, ExpandAll) if (countName == relId.name) => direction match {
            // MATCH (:A)-[r]->()
            case OUTGOING =>
              context.logicalPlanProducer.planCountStoreRelationshipAggregation(plan, relId, Some(label.name), types, None, argumentIds)
            // MATCH (:A)<-[r]-()
            case _ =>
              context.logicalPlanProducer.planCountStoreRelationshipAggregation(plan, relId, None, types, Some(label.name), argumentIds)
          }
          // MATCH (:A)-[r]->(), MATCH (:A)<-[r]-()
          case Selection(predicates, Expand(AllNodesScan(nodeId, argumentIds), from, direction, types, to, relId, ExpandAll))
            // TODO: consider predicates with more than just HasLabel, by maintaining a Selection() around the plan
            if (countName == relId.name && predicates.length == 1) => predicates.head match {
              case HasLabels(Identifier(nodeIdentifier), labels) if(labels.length == 1) => direction match {
                // MATCH (:A)<-[r]-()
                case OUTGOING =>
                  context.logicalPlanProducer.planCountStoreRelationshipAggregation(plan, relId, None, types, Some(labels.head.name), argumentIds)
                // MATCH (:A)-[r]->()
                case _ =>
                  context.logicalPlanProducer.planCountStoreRelationshipAggregation(plan, relId, Some(labels.head.name), types, None, argumentIds)
              }
              case _ => plan
          }
          // MATCH (n:A) RETURN count(n)
            // TODO: This query could be planned with Selection(HasLabels,AllNodeScan), so we should consider this too!
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
