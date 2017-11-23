package org.neo4j.cypher.internal.compatibility.v3_3

import org.neo4j.cypher.internal.v3_3.logical.plans.{LogicalPlan => LogicalPlanV3_3}
import org.neo4j.cypher.internal.v3_4.logical.plans.{LogicalPlan => LogicalPlanV3_4}
import org.neo4j.cypher.internal.v3_3.logical.{plans => plansV3_3}
import org.neo4j.cypher.internal.v3_4.logical.{plans => plansV3_4}

object LogicalPlanConverter {


  def convertLogicalPlan(logicalPlan: LogicalPlanV3_3) : LogicalPlanV3_4 = logicalPlan match {
    case plansV3_3.Aggregation(left, groupingExpressions, aggregationExpression) =>
      plansV3_4.Aggregation(convertLogicalPlan(left),
        groupingExpressions.mapValues(ASTConverter.convertExpression _),
        aggregationExpression.mapValues(ASTConverter.convertExpression _))(null)
  }
}
