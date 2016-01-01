/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import org.neo4j.graphdb.Direction
import org.neo4j.cypher.internal.compiler.v2_1.ast._
import org.neo4j.cypher.internal.compiler.v2_1.functions
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical._
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_1.planner.QueryGraph
import org.neo4j.cypher.internal.compiler.v2_1.InputPosition.NONE
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.steps.QueryPlanProducer._

object idSeekLeafPlanner extends LeafPlanner {
  def apply(queryGraph: QueryGraph)(implicit context: LogicalPlanningContext, subQueriesLookupTable: Map[PatternExpression, QueryGraph]) = {
    val predicates: Seq[Expression] = queryGraph.selections.flatPredicates

    val candidatePlans = predicates.collect {
      // MATCH (a)-[r]->b WHERE id(r) IN value
      // MATCH a WHERE id(a) IN value
      case predicate@In(func@FunctionInvocation(_, _, IndexedSeq(idExpr)), idsExpr@Collection(idValueExprs))
        if func.function == Some(functions.Id) &&
           idValueExprs.forall(ConstantExpression.unapply(_).isDefined) =>
        (predicate, idExpr, idValueExprs)
    }.collect {
      case (predicate, Identifier(idName), idValues) =>
        queryGraph.patternRelationships.find(_.name.name == idName) match {
          case Some(relationship) =>
            createRelationshipByIdSeek(relationship, idValues, predicate)
          case None =>
            planNodeByIdSeek(IdName(idName), idValues, Seq(predicate))
        }
    }

    CandidateList(candidatePlans)
  }

  def createRelationshipByIdSeek(relationship: PatternRelationship, idValues: Seq[Expression], predicate: Expression): QueryPlan = {
    val (left, right) = relationship.nodes
    val name = relationship.name
    val plan = relationship.dir match {
      case Direction.BOTH =>
        planUndirectedRelationshipByIdSeek(name, idValues, left, right, relationship, Seq(predicate))

      case Direction.INCOMING =>
        planDirectedRelationshipByIdSeek(name, idValues, right, left, relationship, Seq(predicate))

      case Direction.OUTGOING =>
        planDirectedRelationshipByIdSeek(name, idValues, left, right, relationship, Seq(predicate))
    }
    filterIfNeeded(plan, name.name, relationship.types)
  }

  private def filterIfNeeded(plan: QueryPlan, relName: String, types: Seq[RelTypeName]): QueryPlan =
    if (types.isEmpty)
      plan
    else {
      val id = Identifier(relName)(NONE)
      val name = FunctionName("type")(NONE)
      val invocation = FunctionInvocation(name, id)(NONE)

      val predicates = types.map {
        relType => Equals(invocation, StringLiteral(relType.name)(NONE))(NONE)
      }.toList

      val predicate = predicates match {
        case exp :: Nil => exp
        case _ => Ors(predicates.toSet)(predicates.head.position)
      }

      planHiddenSelection(Seq(predicate), plan)
    }
}
