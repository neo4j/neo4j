/**
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
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical.steps

import org.neo4j.cypher.internal.compiler.v2_2.ast._
import org.neo4j.cypher.internal.compiler.v2_2.functions
import org.neo4j.cypher.internal.compiler.v2_2.planner.QueryGraph
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.steps.LogicalPlanProducer._
import org.neo4j.graphdb.Direction.{BOTH, INCOMING, OUTGOING}

object idSeekLeafPlanner extends LeafPlanner {
  def apply(queryGraph: QueryGraph)(implicit context: LogicalPlanningContext) = {
    val predicates: Seq[Expression] = queryGraph.selections.flatPredicates

    val idSeekPredicates: Seq[(In, Identifier, EntityByIdRhs)] = predicates.collect {
      // MATCH (a)-[r]->b WHERE id(r) IN value
      // MATCH a WHERE id(a) IN value
      case predicate@In(func@FunctionInvocation(_, _, IndexedSeq(idExpr: Identifier)), idsExpr@Collection(idValueExprs))
        if func.function == Some(functions.Id) &&
           idValueExprs.forall(ConstantExpression.unapply(_).isDefined) =>
        (predicate, idExpr, EntityByIdExprs(idValueExprs))

      // MATCH (a)-[r]->b WHERE id(r) IN {param}
      // MATCH a WHERE id(a) IN {param}
      case predicate@In(func@FunctionInvocation(_, _, IndexedSeq(idExpr: Identifier)), param@Parameter(_))
        if func.function == Some(functions.Id) =>
        (predicate, idExpr, EntityByIdParameter(param))
    }

    val candidatePlans = idSeekPredicates.collect {
      case (predicate, idExpr @ Identifier(idName), idValues) if !queryGraph.argumentIds.contains(IdName(idName)) =>
        queryGraph.patternRelationships.find(_.name.name == idName) match {
          case Some(relationship) =>
            val types = relationship.types.toList
            val seekPlan = planRelationshipByIdSeek(relationship, idValues, Seq(predicate), queryGraph.argumentIds)
            planRelTypeFilter(seekPlan, idExpr, types)
          case None =>
            planNodeByIdSeek(IdName(idName), idValues, Seq(predicate), queryGraph.argumentIds)
        }
    }

    candidatePlans
  }

  private def planRelationshipByIdSeek(relationship: PatternRelationship, idValues: EntityByIdRhs, predicates: Seq[Expression], argumentIds: Set[IdName]): LogicalPlan = {
    val (left, right) = relationship.nodes
    val name = relationship.name
    relationship.dir match {
      case BOTH     => planUndirectedRelationshipByIdSeek(name, idValues, left, right, relationship, argumentIds, predicates)
      case INCOMING => planDirectedRelationshipByIdSeek(name, idValues, right, left, relationship, argumentIds, predicates)
      case OUTGOING => planDirectedRelationshipByIdSeek(name, idValues, left, right, relationship, argumentIds, predicates)
    }
  }

  private def planRelTypeFilter(plan: LogicalPlan, idExpr: Identifier, relTypes: List[RelTypeName]): LogicalPlan = {
    relTypes match {
      case Seq(tpe) =>
        val relTypeExpr = relTypeAsStringLiteral(tpe)
        val predicate = Equals(typeOfRelExpr(idExpr), relTypeExpr)(idExpr.position)
        planHiddenSelection(Seq(predicate), plan)

      case tpe :: _ =>
        val relTypeExprs = relTypes.map(relTypeAsStringLiteral).toSet
        val invocation = typeOfRelExpr(idExpr)
        val idPos = idExpr.position
        val predicate = Ors(relTypeExprs.map { expr => Equals(invocation, expr)(idPos) } )(idPos)
        planHiddenSelection(Seq(predicate), plan)

      case _ =>
        plan
    }
  }

  private def relTypeAsStringLiteral(relType: RelTypeName) = StringLiteral(relType.name)(relType.position)

  private def typeOfRelExpr(idExpr: Identifier) =
    FunctionInvocation(FunctionName("type")(idExpr.position), idExpr)(idExpr.position)
}
