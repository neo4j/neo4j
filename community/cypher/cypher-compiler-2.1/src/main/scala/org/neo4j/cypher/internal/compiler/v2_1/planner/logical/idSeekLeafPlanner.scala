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

case class idSeekLeafPlanner(predicates: Seq[Expression]) extends LeafPlanner {
  def apply()(implicit context: LogicalPlanContext): Seq[LogicalPlan] =
    predicates.collect {
      // MATCH (a)-[r]->b WHERE id(r) = value
//      case predicate@Equals(FunctionInvocation(Identifier("id"), _, IndexedSeq(RelationshipIdName(idName))), ConstantExpression(idExpr)) =>
//        val cardinality = context.estimator.estimateRelationshipByIdSeek()
//        RelationshipByIdSeek(idName, idExpr, cardinality)(Seq(predicate))
//      case predicate@In(FunctionInvocation(Identifier("id"), _, IndexedSeq(RelationshipIdName(idName))), idsExpr@Collection(expressions)) if !expressions.exists(x => ConstantExpression.unapply(x).isEmpty) =>
//        val cardinality = expressions.size * context.estimator.estimateRelationshipByIdSeek()
//        RelationshipByIdSeek(idName, idsExpr, cardinality)(Seq(predicate))
      case predicate@Equals(FunctionInvocation(Identifier("id"), _, IndexedSeq(NodeIdName(idName))), ConstantExpression(idExpr)) =>
        val cardinality = context.estimator.estimateNodeByIdSeek()
        NodeByIdSeek(idName, idExpr, cardinality)(Seq(predicate))
      case predicate@In(FunctionInvocation(Identifier("id"), _, IndexedSeq(NodeIdName(idName))), idsExpr@Collection(expressions)) if !expressions.exists(x => ConstantExpression.unapply(x).isEmpty) =>
        val cardinality = expressions.size * context.estimator.estimateNodeByIdSeek()
        NodeByIdSeek(idName, idsExpr, cardinality)(Seq(predicate))
    }
}
