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

import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.SimpleLogicalPlanner._
import org.neo4j.cypher.internal.compiler.v2_1.ast._
import org.neo4j.cypher.internal.compiler.v2_1.{PropertyKeyId, LabelId}
import org.neo4j.kernel.api.index.IndexDescriptor

abstract class IndexLeafPlanner extends LeafPlanner {
  def apply()(implicit context: LogicalPlanContext): CandidateList =
    CandidateList(predicates.collect {
      // n.prop = value
      case expression@Equals(Property(identifier@Identifier(name), propertyKey), ConstantExpression(valueExpr)) if propertyKey.id.isDefined =>
        val idName = IdName(name)
        val propertyKeyId = propertyKey.id.get
        val labelPredicates = labelPredicateMap.getOrElse(idName, Set.empty)
        labelPredicates.flatMap { predicate =>
          // For some reason, a sugared partial function with a condition (case x if foo) throws a MatchError here. :(
          predicate.labels.flatMap(_.id).collect ( new PartialFunction[LabelId, PlanTableEntry] {
            def isDefinedAt(labelId: LabelId) =
              findIndexesForLabel(labelId.id).exists(_.getPropertyKeyId == propertyKeyId.id)
            def apply(labelId: LabelId) = {
              val plan = constructPlan(idName, labelId, propertyKeyId, valueExpr)
              PlanTableEntry(plan, Seq(expression, predicate))
            }
          } )
        }
    }.flatten)

  def predicates: Seq[Expression]

  def labelPredicateMap: Map[IdName, Set[HasLabels]]

  protected def constructPlan(idName: IdName, labelId: LabelId, propertyKeyId: PropertyKeyId, valueExpr: Expression)(implicit context: LogicalPlanContext): LogicalPlan

  protected def findIndexesForLabel(labelId: Int)(implicit context: LogicalPlanContext): Iterator[IndexDescriptor]
}

case class indexSeekLeafPlanner(predicates: Seq[Expression], labelPredicateMap: Map[IdName, Set[HasLabels]]) extends IndexLeafPlanner {
  protected def constructPlan(idName: IdName, labelId: LabelId, propertyKeyId: PropertyKeyId, valueExpr: Expression)(implicit context: LogicalPlanContext) =
    NodeIndexSeek(idName, labelId, propertyKeyId, valueExpr, context.estimator.estimateNodeIndexSeek(labelId, propertyKeyId))

  protected def findIndexesForLabel(labelId: Int)(implicit context: LogicalPlanContext): Iterator[IndexDescriptor] =
    context.planContext.uniqueIndexesGetForLabel(labelId)
}

case class indexScanLeafPlanner(predicates: Seq[Expression], labelPredicateMap: Map[IdName, Set[HasLabels]]) extends IndexLeafPlanner {
  protected def constructPlan(idName: IdName, labelId: LabelId, propertyKeyId: PropertyKeyId, valueExpr: Expression)(implicit context: LogicalPlanContext) =
    NodeIndexScan(idName, labelId, propertyKeyId, valueExpr, context.estimator.estimateNodeIndexScan(labelId, propertyKeyId))

  protected def findIndexesForLabel(labelId: Int)(implicit context: LogicalPlanContext): Iterator[IndexDescriptor] =
    context.planContext.indexesGetForLabel(labelId)
}
