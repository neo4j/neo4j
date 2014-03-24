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
package org.neo4j.cypher.internal.compiler.v2_1.planner.logical

import org.neo4j.cypher.internal.compiler.v2_1.ast._
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans._
import org.neo4j.cypher.InternalException
import org.neo4j.graphdb.Direction

case class idSeekLeafPlanner(predicates: Seq[Expression]) extends LeafPlanner {
  def apply()(implicit context: LogicalPlanContext): Seq[LogicalPlan] =
    predicates.collect {
      // MATCH (a)-[r]->b WHERE id(r) = value
      case predicate@Equals(FunctionInvocation(FunctionName("id"), _, IndexedSeq(RelationshipIdName(idName))), ConstantExpression(idExpr)) =>
        val cardinality = context.estimator.estimateRelationshipByIdSeek()
        context.queryGraph.patternRelationships.collectFirst {

          case PatternRelationship(relName, (l, r), dir, types) if relName == idName && dir != Direction.BOTH =>
            val (from, to) = if (dir == Direction.OUTGOING) (l, r) else (r, l)
            val relById = DirectedRelationshipByIdSeek(idName, idExpr, cardinality, from, to)(Seq(predicate))
            filterIfNeeded(relById, relName.name, types)

          case PatternRelationship(relName, (l, r), _, types) if relName == idName =>
            val relById = UndirectedRelationshipByIdSeek(idName, idExpr, cardinality, l, r)(Seq(predicate))
            filterIfNeeded(relById, relName.name, types)
        }.getOrElse(throw new InternalException(s"Identifier ${idName} typed as a relationship, but no relationship found by that name in the query graph "))

      // MATCH (a)-[r]->b WHERE id(r) IN value
      case predicate@In(FunctionInvocation(FunctionName("id"), _, IndexedSeq(RelationshipIdName(idName))), idsExpr@Collection(expressions)) if !expressions.exists(x => ConstantExpression.unapply(x).isEmpty) =>
        val cardinality = expressions.size * context.estimator.estimateRelationshipByIdSeek()
        context.queryGraph.patternRelationships.collectFirst {

          case PatternRelationship(relName, (l, r), dir, types) if relName == idName && dir != Direction.BOTH =>
            val (from, to) = if (dir == Direction.OUTGOING) (l, r) else (r, l)
            val relById = DirectedRelationshipByIdSeek(idName, idsExpr, cardinality, from, to)(Seq(predicate))
            filterIfNeeded(relById, relName.name, types)

          case PatternRelationship(relName, (l, r), _, types) if relName == idName =>
            val relById = UndirectedRelationshipByIdSeek(idName, idsExpr, cardinality, l, r)(Seq(predicate))
            filterIfNeeded(relById, relName.name, types)
        }.getOrElse(throw new InternalException(s"Identifier ${idName} typed as a relationship, but no relationship found by that name in the query graph "))


      case predicate@Equals(FunctionInvocation(FunctionName("id"), _, IndexedSeq(NodeIdName(idName))), ConstantExpression(idExpr)) =>
        val cardinality = context.estimator.estimateNodeByIdSeek()
        NodeByIdSeek(idName, idExpr, cardinality)(Seq(predicate))
      case predicate@In(FunctionInvocation(FunctionName("id"), _, IndexedSeq(NodeIdName(idName))), idsExpr@Collection(expressions)) if !expressions.exists(x => ConstantExpression.unapply(x).isEmpty) =>
        val cardinality = expressions.size * context.estimator.estimateNodeByIdSeek()
        NodeByIdSeek(idName, idsExpr, cardinality)(Seq(predicate))
    }

  private def filterIfNeeded(plan: LogicalPlan, relName: String, types: Seq[RelTypeName])
                            (implicit context: LogicalPlanContext): LogicalPlan = if (types.isEmpty) plan
  else {
    val id = Identifier(relName)(null)
    val name = FunctionName("type")(null)
    val invocation = FunctionInvocation(name, id)(null)

    val predicates: Seq[Expression] = types.map {
      t => Equals(invocation, StringLiteral(relName)(null))(null)
    }

    val predicate = predicates.reduce(And(_, _)(null))
    Selection(Seq(predicate), plan)
  }
}
