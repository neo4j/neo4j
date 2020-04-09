/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.compiler.planner.logical.steps

import org.neo4j.cypher.internal.compiler.planner.logical.LeafPlanFromExpression
import org.neo4j.cypher.internal.compiler.planner.logical.LeafPlanner
import org.neo4j.cypher.internal.compiler.planner.logical.LeafPlansForVariable
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.planner.logical.plans.AsIdSeekable
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.FunctionName
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.Ors
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.SemanticDirection.BOTH
import org.neo4j.cypher.internal.expressions.SemanticDirection.INCOMING
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.ordering.InterestingOrder
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.SeekableArgs

object idSeekLeafPlanner extends LeafPlanner with LeafPlanFromExpression {

  override def producePlanFor(e: Expression, qg: QueryGraph, interestingOrder: InterestingOrder, context: LogicalPlanningContext): Option[LeafPlansForVariable] = {
    val arguments: Set[LogicalVariable] = qg.argumentIds.map(n => Variable(n)(null))
    val idSeekPredicates: Option[(Expression, LogicalVariable, SeekableArgs)] = e match {
      // MATCH (a)-[r]-(b) WHERE id(r) IN expr
      // MATCH a WHERE id(a) IN {param}
      case predicate@AsIdSeekable(seekable) if seekable.args.dependencies.forall(arguments) && !arguments(seekable.ident) =>
        Some((predicate, seekable.ident, seekable.args))
      case _ => None
    }

    idSeekPredicates map {
      case (predicate, variable@Variable(id), idValues) if !qg.argumentIds.contains(id) =>

        qg.patternRelationships.find(_.name == id) match {
          case Some(relationship) =>
            val types = relationship.types.toList
            val seekPlan = planRelationshipByIdSeek(relationship, idValues, Seq(predicate), qg.argumentIds, context)
            LeafPlansForVariable(id, Set(planRelTypeFilter(seekPlan, variable, types, context)))
          case None =>
            val plan = context.logicalPlanProducer.planNodeByIdSeek(variable, idValues, Seq(predicate), qg.argumentIds, context)
            LeafPlansForVariable(id, Set(plan))
        }
    }
  }

  override def apply(queryGraph: QueryGraph, interestingOrder: InterestingOrder, context: LogicalPlanningContext): Seq[LogicalPlan] =
    queryGraph.selections.flatPredicates.flatMap(e => producePlanFor(e, queryGraph, interestingOrder, context).toSeq.flatMap(_.plans))

  private def planRelationshipByIdSeek(relationship: PatternRelationship,
                                       idValues: SeekableArgs,
                                       predicates: Seq[Expression],
                                       argumentIds: Set[String],
                                       context: LogicalPlanningContext): LogicalPlan = {
    val (left, right) = relationship.nodes
    val name = relationship.name
    relationship.dir match {
      case BOTH     => context.logicalPlanProducer.planUndirectedRelationshipByIdSeek(name, idValues, left, right, relationship, argumentIds, predicates, context)
      case INCOMING => context.logicalPlanProducer.planDirectedRelationshipByIdSeek(name, idValues, right, left, relationship, argumentIds, predicates, context)
      case OUTGOING => context.logicalPlanProducer.planDirectedRelationshipByIdSeek(name, idValues, left, right, relationship, argumentIds, predicates, context)
    }
  }

  private def planRelTypeFilter(plan: LogicalPlan, idExpr: Variable, relTypes: List[RelTypeName], context: LogicalPlanningContext): LogicalPlan = {
    relTypes match {
      case Seq(tpe) =>
        val relTypeExpr = relTypeAsStringLiteral(tpe)
        val predicate = Equals(typeOfRelExpr(idExpr), relTypeExpr)(idExpr.position)
        context.logicalPlanProducer.planHiddenSelection(Seq(predicate), plan, context)

      case tpe :: _ =>
        val relTypeExprs = relTypes.map(relTypeAsStringLiteral).toSet
        val invocation = typeOfRelExpr(idExpr)
        val idPos = idExpr.position
        val predicate = Ors(relTypeExprs.map { expr => Equals(invocation, expr)(idPos) } )(idPos)
        context.logicalPlanProducer.planHiddenSelection(Seq(predicate), plan, context)

      case _ =>
        plan
    }
  }

  private def relTypeAsStringLiteral(relType: RelTypeName) = StringLiteral(relType.name)(relType.position)

  private def typeOfRelExpr(idExpr: Variable) =
    FunctionInvocation(FunctionName("type")(idExpr.position), idExpr)(idExpr.position)
}
