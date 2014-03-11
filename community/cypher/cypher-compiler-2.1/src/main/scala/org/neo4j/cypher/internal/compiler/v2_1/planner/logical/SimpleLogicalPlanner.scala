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

import org.neo4j.cypher.internal.compiler.v2_1.planner.{SemanticTable, CantHandleQueryException, CardinalityEstimator, QueryGraph}
import org.neo4j.cypher.internal.compiler.v2_1.ast._
import org.neo4j.cypher.internal.compiler.v2_1.ast.Identifier
import org.neo4j.cypher.internal.compiler.v2_1.ast.HasLabels
import org.neo4j.cypher.internal.compiler.v2_1.spi.PlanContext
import org.neo4j.cypher.internal.compiler.v2_1.ast.Literal

case class SimpleLogicalPlanner(estimator: CardinalityEstimator) extends LogicalPlanner {

  val projectionPlanner = new ProjectionPlanner

  override def plan(qg: QueryGraph, semanticQuery: SemanticTable)(implicit planContext: PlanContext): LogicalPlan = {
    val planTableBuilder = Map.newBuilder[Set[IdName], Seq[LogicalPlan]]
    qg.identifiers.foreach { id =>
      planTableBuilder += (Set(id) -> identifierSources(id, qg, semanticQuery))
    }

    val planTable = planTableBuilder.result()
    while (planTable.size > 1) {
      throw new CantHandleQueryException
    }

    val logicalPlan = planTable.values.headOption.map(_.sortBy(_.cardinality).head).getOrElse(SingleRow())
    projectionPlanner.amendPlan(qg, logicalPlan)
  }

  /*

    // add unique index seek pipe

    for each identifier: find cheapest logical plan

    split single predicate into seq[predicate]

     map<identifier, (cost, plan)>, seq<predicate>

     map<identifier, seq<labels>>

     for each predicate:
         check if we can seek using id
         // check if we can seek using unique index
         check if we can can using index

     for each identifier:
        update to use label scans
        update to use all nodes scans


      for each plan that we produce:
        generate predicates that should be removed

      remove predicates from selections

      fail if selections not empty

  */
  private def identifierSources(id: IdName, qg: QueryGraph, semanticQuery: SemanticTable)(implicit planContext: PlanContext): Seq[LogicalPlan] = {

    val predicates = qg.selections.apply(Set(id))

    val labelScanPlans = predicates.collect {
      // n:Label
      case HasLabels(Identifier(id.name), label :: Nil) =>
        val labelId = label.id
        NodeByLabelScan(id, labelId.toRight(label.name), estimator.estimateNodeByLabelScan(labelId))
    }

    val labelIds = labelScanPlans.flatMap(_.label.right.toOption)
    val indexSeekPlans = labelIds.flatMap { labelId =>

      val indexedPropertyKeyIds = planContext.indexesGetForLabel(labelId.id).map(_.getPropertyKeyId).toSet
      predicates.collect {
        // n.prop = value
        case Equals(Property(Identifier(id.name), propertyKey), valueExpr) if valueExpr.isInstanceOf[Literal] =>
          propertyKey.id.filter(x => indexedPropertyKeyIds(x.id)).map { propertyKeyId =>
            NodeIndexScan(id, labelId, propertyKeyId, valueExpr, estimator.estimateNodeByIndexSeek(labelId, propertyKeyId))
          }
      }.flatten
    }

    val idLookupPlans = predicates.collect {
      // id(n) = value
      case Equals(FunctionInvocation(Identifier("id"), _, IndexedSeq( ident @ Identifier(identName))), idExpr)
        if idExpr.isInstanceOf[Literal] || idExpr.isInstanceOf[Parameter] =>
        val idName = IdName(identName)
        if (semanticQuery.isRelationship(ident))
          RelationshipByIdSeek(idName, idExpr, estimator.estimateRelationshipByIdSeek())
        else
          NodeByIdSeek(idName, idExpr, estimator.estimateNodeByIdSeek())
    }

    /*
     * FIXME: since we do not have FilterPipe in the plan
     * we need to allow start points only when we have the exact number of predicates to activate a pipe
     * this code should go away for good as soon as we can add filtering so we can actually avoid failing
     */
    val plans = if (predicates.size == 0) {
      val allNodesScan = AllNodesScan(id, estimator.estimateAllNodes())
      Seq(allNodesScan)
    } else if (predicates.size == 1)
      labelScanPlans ++ idLookupPlans
    else if (predicates.size == 2)
      idLookupPlans ++ indexSeekPlans
    else
      idLookupPlans

    if (plans.isEmpty)
      throw new CantHandleQueryException

    plans
  }
}

