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
package org.neo4j.cypher.internal.compiler.v2_1.planner.execution

import org.neo4j.cypher.internal.compiler.v2_1.ast.convert.ExpressionConverters._
import org.neo4j.cypher.internal.compiler.v2_1.pipes._
import org.neo4j.cypher.internal.compiler.v2_1.ast.Expression
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_1.Monitors
import org.neo4j.cypher.internal.compiler.v2_1.executionplan.PipeInfo
import org.neo4j.cypher.internal.compiler.v2_1.planner.CantHandleQueryException
import org.neo4j.cypher.internal.compiler.v2_1.commands.True
import org.neo4j.cypher.internal.compiler.v2_1.ast.convert.OtherConverters._
import org.neo4j.cypher.internal.compiler.v2_1.symbols._
import org.neo4j.cypher.internal.compiler.v2_1.commands.expressions.AggregationExpression


class PipeExecutionPlanBuilder(monitors: Monitors) {

  def build(plan: LogicalPlan): PipeInfo = {
    val updating = false

    def buildPipe(plan: LogicalPlan): Pipe = {
      implicit val monitor = monitors.newMonitor[PipeMonitor]()
      plan match {
        case Projection(left, expressions) =>
          ProjectionNewPipe(buildPipe(left), toLegacyExpressions(expressions))

        case sr @ SingleRow(ids) =>
          NullPipe(new SymbolTable(sr.typeInfo))

        case AllNodesScan(IdName(id)) =>
          AllNodesScanPipe(id)

        case NodeByLabelScan(IdName(id), label) =>
          NodeByLabelScanPipe(id, label)

        case NodeByIdSeek(IdName(id), nodeIdExpr) =>
          NodeByIdSeekPipe(id, nodeIdExpr.map(_.asCommandExpression))

        case DirectedRelationshipByIdSeek(IdName(id), relIdExpr, IdName(fromNode), IdName(toNode)) =>
          DirectedRelationshipByIdSeekPipe(id, relIdExpr.map(_.asCommandExpression), toNode, fromNode)

        case UndirectedRelationshipByIdSeek(IdName(id), relIdExpr, IdName(fromNode), IdName(toNode)) =>
          UndirectedRelationshipByIdSeekPipe(id, relIdExpr.map(_.asCommandExpression), toNode, fromNode)

        case NodeIndexSeek(IdName(id), labelId, propertyKeyId, valueExpr) =>
          NodeIndexSeekPipe(id, Right(labelId), Right(propertyKeyId), valueExpr.asCommandExpression)

        case NodeIndexUniqueSeek(IdName(id), labelId, propertyKeyId, valueExpr) =>
          NodeUniqueIndexSeekPipe(id, Right(labelId), Right(propertyKeyId), valueExpr.asCommandExpression)

        case Selection(predicates, left) =>
          FilterPipe(buildPipe(left), predicates.map(_.asCommandPredicate).reduce(_ ++ _))

        case CartesianProduct(left, right) =>
          CartesianProductPipe(buildPipe(left), buildPipe(right))

        case Expand(left, IdName(fromName), dir, types, IdName(toName), IdName(relName), SimplePatternLength) =>
          ExpandPipe(buildPipe(left), fromName, relName, toName, dir, types.map(_.name))

        case Expand(left, IdName(fromName), dir, types, IdName(toName), IdName(relName), VarPatternLength(min, max)) =>
          VarLengthExpandPipe(buildPipe(left), fromName, relName, toName, dir, types.map(_.name), min, max)

        case OptionalExpand(left, IdName(fromName), dir, types, IdName(toName), IdName(relName), SimplePatternLength, predicates) =>
          val predicate = predicates.map(_.asCommandPredicate).reduceOption(_ ++ _).getOrElse(True())
          OptionalExpandPipe(buildPipe(left), fromName, relName, toName, dir, types.map(_.name), predicate)

        case NodeHashJoin(node, left, right) =>
          NodeHashJoinPipe(node.name, buildPipe(left), buildPipe(right))

        case OuterHashJoin(node, left, right) =>
          NodeOuterHashJoinPipe(node.name, buildPipe(left), buildPipe(right), (right.availableSymbols -- left.availableSymbols).map(_.name))

        case Optional(inner) =>
          OptionalPipe(inner.availableSymbols.map(_.name), buildPipe(inner))

        case Apply(outer, inner) =>
          ApplyPipe(buildPipe(outer), buildPipe(inner))

        case SemiApply(outer, inner) =>
          SemiApplyPipe(buildPipe(outer), buildPipe(inner), negated = false)

        case AntiSemiApply(outer, inner) =>
          SemiApplyPipe(buildPipe(outer), buildPipe(inner), negated = true)

        case LetSemiApply(outer, inner, idName) =>
          LetSemiApplyPipe(buildPipe(outer), buildPipe(inner), idName.name, negated = false)

        case LetAntiSemiApply(outer, inner, idName) =>
          LetSemiApplyPipe(buildPipe(outer), buildPipe(inner), idName.name, negated = true)

        case apply@SelectOrSemiApply(outer, inner, predicate) =>
          SelectOrSemiApplyPipe(buildPipe(outer), buildPipe(inner), predicate.asCommandPredicate, negated = false)

        case apply@SelectOrAntiSemiApply(outer, inner, predicate) =>
          SelectOrSemiApplyPipe(buildPipe(outer), buildPipe(inner), predicate.asCommandPredicate, negated = true)

        case apply@LetSelectOrSemiApply(outer, inner, idName, predicate) =>
          LetSelectOrSemiApplyPipe(buildPipe(outer), buildPipe(inner), idName.name, predicate.asCommandPredicate, negated = false)

        case apply@LetSelectOrAntiSemiApply(outer, inner, idName, predicate) =>
          LetSelectOrSemiApplyPipe(buildPipe(outer), buildPipe(inner), idName.name, predicate.asCommandPredicate, negated = true)

        case Sort(left, sortItems) =>
          SortPipe(buildPipe(left), sortItems)

        case Skip(input, count) =>
          SkipPipe(buildPipe(input), count.asCommandExpression)

        case Limit(input, count) =>
          LimitPipe(buildPipe(input), count.asCommandExpression)

        case SortedLimit(input, exp, sortItems) =>
          TopPipe(buildPipe(input), sortItems.map(_.asCommandSortItem).toList, exp.asCommandExpression)

        case Aggregation(input, groupingExpressions, aggregatingExpressions) =>
          EagerAggregationPipe(
            buildPipe(input),
            groupingExpressions.mapValues(_.asCommandExpression),
            aggregatingExpressions.mapValues(_.asCommandExpression.asInstanceOf[AggregationExpression]))

        case _ =>
          throw new CantHandleQueryException
      }
    }

    val topLevelPipe = buildPipe(plan)

    PipeInfo(topLevelPipe, updating, None)
  }

  def toLegacyExpressions(expressions: Map[String, Expression]) = expressions.mapValues(_.asCommandExpression)
}
