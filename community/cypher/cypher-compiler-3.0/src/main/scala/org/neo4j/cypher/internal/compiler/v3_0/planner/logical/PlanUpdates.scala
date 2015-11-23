/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.compiler.v3_0.planner.logical

import org.neo4j.cypher.internal.compiler.v3_0.planner._
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.frontend.v3_0.CypherTypeException
import org.neo4j.cypher.internal.frontend.v3_0.ast.{Variable, PathExpression}

/*
 * This coordinates PlannerQuery planning of updates.
 */
case object PlanUpdates
  extends LogicalPlanningFunction2[PlannerQuery, LogicalPlan, LogicalPlan] {

  override def apply(query: PlannerQuery, plan: LogicalPlan)(implicit context: LogicalPlanningContext): LogicalPlan =
    query.updateGraph.mutatingPatterns.foldLeft(plan)((plan, pattern) => planUpdate(plan, pattern))

  private def planUpdate(inner: LogicalPlan, pattern: MutatingPattern)(implicit context: LogicalPlanningContext) = pattern match {
    //CREATE ()
    case p: CreateNodePattern => context.logicalPlanProducer.planCreateNode(inner, p)
    //CREATE (a)-[:R]->(b)
    case p: CreateRelationshipPattern => context.logicalPlanProducer.planCreateRelationship(inner, p)
    //SET n:Foo:Bar
    case pattern: SetLabelPattern => context.logicalPlanProducer.planSetLabel(inner, pattern)
    //SET n.prop = 42
    case pattern: SetNodePropertyPattern =>
      context.logicalPlanProducer.planSetNodeProperty(inner, pattern)
    //SET r.prop = 42
    case pattern: SetRelationshipPropertyPattern =>
      context.logicalPlanProducer.planSetRelationshipProperty(inner, pattern)
    //SET n.prop += {}
    case pattern: SetNodePropertiesFromMapPattern =>
      context.logicalPlanProducer.planSetNodePropertiesFromMap(inner, pattern)
    //SET r.prop = 42
    case pattern: SetRelationshipPropertiesFromMapPattern =>
      context.logicalPlanProducer.planSetRelationshipPropertiesFromMap(inner, pattern)
    //REMOVE n:Foo:Bar
    case pattern: RemoveLabelPattern => context.logicalPlanProducer.planRemoveLabel(inner, pattern)
    //DELETE a
    case p: DeleteExpression =>
      p.expression match {
        case Variable(n) if context.semanticTable.isNode(n) =>
          context.logicalPlanProducer.planDeleteNode(inner, p)
        case Variable(r) if context.semanticTable.isRelationship(r) =>
          context.logicalPlanProducer.planDeleteRelationship(inner, p)
        case PathExpression(e)  =>
          context.logicalPlanProducer.planDeletePath(inner, p)

        case e => throw new CypherTypeException(s"Don't know how to delete a $e")
      }
  }
}
