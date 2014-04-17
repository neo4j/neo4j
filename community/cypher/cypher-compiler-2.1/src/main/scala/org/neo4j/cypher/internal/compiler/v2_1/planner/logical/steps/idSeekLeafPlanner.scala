/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.planner.logical.steps

import org.neo4j.cypher.internal.compiler.v2_1.ast._
import org.neo4j.graphdb.Direction
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.{LeafPlanner, CandidateList, LogicalPlanContext}
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.{LogicalPlan, DirectedRelationshipByIdSeek, UndirectedRelationshipByIdSeek}
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.IdName
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.PatternRelationship
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.NodeByIdSeek
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.Selection
import org.neo4j.cypher.internal.compiler.v2_1.planner.QueryGraph

object idSeekLeafPlanner extends LeafPlanner {
  def apply(qg: QueryGraph)(implicit context: LogicalPlanContext) = {
    val predicates: Seq[Expression] = qg.selections.flatPredicates

    CandidateList(
      predicates.collect {
        // MATCH (a)-[r]->b WHERE id(r) = value
        // MATCH a WHERE id(a) = value
        case predicate@Equals(FunctionInvocation(FunctionName("id"), _, IndexedSeq(idExpr)), ConstantExpression(idValueExpr)) =>
          (predicate, idExpr, Seq(idValueExpr))

        // MATCH (a)-[r]->b WHERE id(r) IN value
        // MATCH a WHERE id(a) IN value
        case predicate@In(FunctionInvocation(FunctionName("id"), _, IndexedSeq(idExpr)), idsExpr@Collection(idValueExprs))
          if idValueExprs.forall(ConstantExpression.unapply(_).isDefined) =>
          (predicate, idExpr, idValueExprs)
      }.collect {
        case (predicate, Identifier(idName), idValues) =>
          context.queryGraph.patternRelationships.find(_.name.name == idName) match {
            case Some(relationship) =>
              createRelationshipByIdSeek(relationship, idValues, predicate)
            case None =>
              NodeByIdSeek(IdName(idName), idValues)(Seq(predicate))
          }
      }
    )
  }

  def createRelationshipByIdSeek(relationship: PatternRelationship, idValues: Seq[Expression], predicate: Expression): LogicalPlan = {
    val (left, right) = relationship.nodes
    val name = relationship.name
    val plan = relationship.dir match {
      case Direction.BOTH =>
        UndirectedRelationshipByIdSeek(name, idValues, left, right)(relationship, Seq(predicate))
      case Direction.INCOMING =>
        DirectedRelationshipByIdSeek(name, idValues, right, left)(relationship, Seq(predicate))
      case Direction.OUTGOING =>
        DirectedRelationshipByIdSeek(name, idValues, left, right)(relationship, Seq(predicate))
    }
    filterIfNeeded(plan, name.name, relationship.types)
  }

  private def filterIfNeeded(plan: LogicalPlan, relName: String, types: Seq[RelTypeName]): LogicalPlan =
    if (types.isEmpty)
      plan
    else {
      val id = Identifier(relName)(null)
      val name = FunctionName("type")(null)
      val invocation = FunctionInvocation(name, id)(null)

      val predicates: Seq[Expression] = types.map {
        relType => Equals(invocation, StringLiteral(relType.name)(null))(null)
      }

      val predicate = predicates.reduce(Or(_, _)(null))
      Selection(Seq(predicate), plan, hideSelections = true)
    }
}
