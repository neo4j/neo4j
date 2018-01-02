/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical.steps

import org.neo4j.cypher.internal.compiler.v2_3.planner.QueryGraph
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical._
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans._
import org.neo4j.cypher.internal.frontend.v2_3.SemanticDirection.{BOTH, INCOMING, OUTGOING}
import org.neo4j.cypher.internal.frontend.v2_3.ast._

object idSeekLeafPlanner extends LeafPlanner {
  def apply(queryGraph: QueryGraph)(implicit context: LogicalPlanningContext) = {
    val predicates: Seq[Expression] = queryGraph.selections.flatPredicates
    val arguments = queryGraph.argumentIds.map(n => Identifier(n.name)(null))

    val idSeekPredicates: Seq[(Expression, Identifier, SeekableArgs)] = predicates.collect {
      // MATCH (a)-[r]->b WHERE id(r) IN expr
      // MATCH a WHERE id(a) IN {param}
      case predicate@AsIdSeekable(seekable) if seekable.args.dependencies.forall(arguments) && !arguments(seekable.ident) =>
        (predicate, seekable.ident, seekable.args)
    }

    val candidatePlans = idSeekPredicates.collect {
      case (predicate, idExpr @ Identifier(idName), idValues) if !queryGraph.argumentIds.contains(IdName(idName)) =>
        queryGraph.patternRelationships.find(_.name.name == idName) match {
          case Some(relationship) =>
            val types = relationship.types.toList
            val seekPlan = planRelationshipByIdSeek(relationship, idValues, Seq(predicate), queryGraph.argumentIds)
            planRelTypeFilter(seekPlan, idExpr, types)
          case None =>
            context.logicalPlanProducer.planNodeByIdSeek(IdName(idName), idValues, Seq(predicate), queryGraph.argumentIds)
        }
    }

    candidatePlans
  }

  private def planRelationshipByIdSeek(relationship: PatternRelationship, idValues: SeekableArgs, predicates: Seq[Expression], argumentIds: Set[IdName])
                                      (implicit context: LogicalPlanningContext): LogicalPlan = {
    val (left, right) = relationship.nodes
    val name = relationship.name
    relationship.dir match {
      case BOTH     => context.logicalPlanProducer.planUndirectedRelationshipByIdSeek(name, idValues, left, right, relationship, argumentIds, predicates)
      case INCOMING => context.logicalPlanProducer.planDirectedRelationshipByIdSeek(name, idValues, right, left, relationship, argumentIds, predicates)
      case OUTGOING => context.logicalPlanProducer.planDirectedRelationshipByIdSeek(name, idValues, left, right, relationship, argumentIds, predicates)
    }
  }

  private def planRelTypeFilter(plan: LogicalPlan, idExpr: Identifier, relTypes: List[RelTypeName])
                               (implicit context: LogicalPlanningContext): LogicalPlan = {
    relTypes match {
      case Seq(tpe) =>
        val relTypeExpr = relTypeAsStringLiteral(tpe)
        val predicate = Equals(typeOfRelExpr(idExpr), relTypeExpr)(idExpr.position)
        context.logicalPlanProducer.planHiddenSelection(Seq(predicate), plan)

      case tpe :: _ =>
        val relTypeExprs = relTypes.map(relTypeAsStringLiteral).toSet
        val invocation = typeOfRelExpr(idExpr)
        val idPos = idExpr.position
        val predicate = Ors(relTypeExprs.map { expr => Equals(invocation, expr)(idPos) } )(idPos)
        context.logicalPlanProducer.planHiddenSelection(Seq(predicate), plan)

      case _ =>
        plan
    }
  }

  private def relTypeAsStringLiteral(relType: RelTypeName) = StringLiteral(relType.name)(relType.position)

  private def typeOfRelExpr(idExpr: Identifier) =
    FunctionInvocation(FunctionName("type")(idExpr.position), idExpr)(idExpr.position)
}
