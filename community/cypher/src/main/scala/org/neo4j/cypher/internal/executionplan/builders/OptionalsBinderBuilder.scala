package org.neo4j.cypher.internal.executionplan.builders

import org.neo4j.cypher.internal.executionplan.{ExecutionPlanInProgress, PlanBuilder}
import org.neo4j.cypher.internal.spi.PlanContext
import org.neo4j.cypher.internal.pipes.OptionalsBindingPipe

class OptionalsBinderBuilder extends PlanBuilder {

  def canWorkWith(plan: ExecutionPlanInProgress, ctx: PlanContext): Boolean = ! plan.query.bound

  def apply(plan: ExecutionPlanInProgress, ctx: PlanContext): ExecutionPlanInProgress = {
    plan.copy( query = plan.query.copy( bound = true ), pipe = new OptionalsBindingPipe( plan.pipe ) )
  }

  def priority: Int = PlanBuilder.Lowest
}