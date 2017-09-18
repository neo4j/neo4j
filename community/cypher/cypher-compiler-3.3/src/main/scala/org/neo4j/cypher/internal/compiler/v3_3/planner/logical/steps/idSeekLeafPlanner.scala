/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_3.planner.logical.steps

import org.neo4j.cypher.internal.compiler.v3_3.planner.logical._
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.plans._
import org.neo4j.cypher.internal.frontend.v3_4.SemanticDirection.{BOTH, INCOMING, OUTGOING}
import org.neo4j.cypher.internal.frontend.v3_4.ast._
import org.neo4j.cypher.internal.ir.v3_4.{IdName, PatternRelationship, QueryGraph}
import org.neo4j.cypher.internal.v3_4.logical.plans.{LogicalPlan, SeekableArgs}

object idSeekLeafPlanner extends LeafPlanner with LeafPlanFromExpression {

  override def producePlanFor(e: Expression, qg: QueryGraph)(implicit context: LogicalPlanningContext): Option[LeafPlansForVariable] = {
    val arguments = qg.argumentIds.map(n => Variable(n.name)(null))
    val idSeekPredicates: Option[(Expression, Variable, SeekableArgs)] = e match {
      // MATCH (a)-[r]-(b) WHERE id(r) IN expr
      // MATCH a WHERE id(a) IN {param}
      case predicate@AsIdSeekable(seekable) if seekable.args.dependencies.forall(arguments) && !arguments(seekable.ident) =>
        Some((predicate, seekable.ident, seekable.args))
      case _ => None
    }

    idSeekPredicates map {
      case (predicate, idExpr@Variable(id), idValues) if !qg.argumentIds.contains(IdName(id)) =>

        qg.patternRelationships.find(_.name.name == id) match {
          case Some(relationship) =>
            val types = relationship.types.toList
            val seekPlan = planRelationshipByIdSeek(relationship, idValues, Seq(predicate), qg.argumentIds)
            LeafPlansForVariable(IdName(id), Set(planRelTypeFilter(seekPlan, idExpr, types)))
          case None =>
            val plan = context.logicalPlanProducer.planNodeByIdSeek(IdName(id), idValues, Seq(predicate), qg.argumentIds)
            LeafPlansForVariable(IdName(id), Set(plan))
        }
    }
  }

  override def apply(queryGraph: QueryGraph)(implicit context: LogicalPlanningContext) =
    queryGraph.selections.flatPredicates.flatMap(e => producePlanFor(e, queryGraph).toSeq.flatMap(_.plans))

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

  private def planRelTypeFilter(plan: LogicalPlan, idExpr: Variable, relTypes: List[RelTypeName])
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

  private def typeOfRelExpr(idExpr: Variable) =
    FunctionInvocation(FunctionName("type")(idExpr.position), idExpr)(idExpr.position)
}
