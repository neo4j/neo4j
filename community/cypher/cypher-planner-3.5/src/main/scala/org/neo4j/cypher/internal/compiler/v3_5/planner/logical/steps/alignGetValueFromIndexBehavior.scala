/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.compiler.v3_5.planner.logical.steps

import org.neo4j.cypher.internal.compiler.v3_5.planner.logical.LeafPlanUpdater
import org.neo4j.cypher.internal.ir.v3_5.PlannerQuery
import org.neo4j.cypher.internal.ir.v3_5.Predicate
import org.neo4j.cypher.internal.ir.v3_5.QueryProjection
import org.neo4j.cypher.internal.planner.v3_5.spi.PlanningAttributes.Solveds
import org.neo4j.cypher.internal.v3_5.logical.plans._
import org.neo4j.cypher.internal.v3_5.expressions._
import org.neo4j.cypher.internal.v3_5.util.Foldable.FoldableAny
import org.neo4j.cypher.internal.v3_5.util.InputPosition
import org.neo4j.cypher.internal.v3_5.util.Rewriter
import org.neo4j.cypher.internal.v3_5.util.attribution.Attributes
import org.neo4j.cypher.internal.v3_5.util.topDown

/**
  * This updates index leaf plans such that they have the right GetValueFromIndexBehavior.
  * The index leaf planners will always set `CanGetValue`, if the index has the capability to provide values.
  * Here, we set this to `GetValue` or `DoNotGetValue`, depending on if the property is used in the rest of the PlannerQuery, aside from the predicate.
  */
object alignGetValueFromIndexBehavior {
  // How many QGs to look into the future for variable accesses
  private val recursionLimit = 5

  def apply(query: PlannerQuery, plan: LogicalPlan, lpp: LogicalPlanProducer, solveds: Solveds, attributes: Attributes): LogicalPlan = {
    val usedExps = usedExpressionsRecursive(query, firstPart = true, recursionLimit)
    rewriter(usedExps, query, solveds, attributes)(plan).asInstanceOf[LogicalPlan]
  }

  private def usedExpressionsInQueryPart(queryPart: PlannerQuery, withPredicates: Boolean): Set[Expression] = {
    val horizonDependingExpressions = queryPart.horizon.dependingExpressions.toSet
    val usedExpressionsInHorizon = collectPropertiesAndVariables(horizonDependingExpressions)
    if (withPredicates) {
      val usedExpressionsInPredicates = collectPropertiesAndVariables(queryPart.queryGraph.selections.predicates.map(_.expr))
      usedExpressionsInHorizon ++ usedExpressionsInPredicates
    } else {
      usedExpressionsInHorizon
    }
  }

  private def usedExpressionsRecursive(queryPart: PlannerQuery, firstPart: Boolean, recursionLimit: Int): Set[Expression] = {
    if (recursionLimit == 0) {
      Set.empty
    } else {
      val maybeHorizonProjections = queryPart.horizon match {
        case projection: QueryProjection => Some(projection.projections)
        case _ => None
      }

      val usedExpressionsInThisPart =
        if (firstPart) {
          // The predicates used in the first will be added during the rewriting. We must filter the predicate out that is solved by the leaf plan
          usedExpressionsInQueryPart(queryPart, withPredicates = false)
        } else {
          // Pass true since the leaf plan can only solve predicates of the first query part
          usedExpressionsInQueryPart(queryPart, withPredicates = true)
        }

      val nextUsedExpressions = for {
        nextPart <- queryPart.tail.toSet[PlannerQuery]
        expressions <- usedExpressionsRecursive(nextPart, firstPart = false, recursionLimit - 1)
        // If the horizon does not rename, keep the expressions as they are. Otherwise rename them for this query part
        renamedExpressions <- maybeHorizonProjections.fold(Option(expressions))(projections => renameExpressionsFromNextQueryPart(expressions, projections))
      } yield {
        renamedExpressions
      }
      usedExpressionsInThisPart ++ nextUsedExpressions
    }
  }

  private def collectPropertiesAndVariables(expression: FoldableAny): Set[Expression] =
    expression.treeFold(Set.empty[Expression]) {
      case prop@Property(v: Variable, _) => acc => (acc + prop, None)
      case v: Variable => acc => (acc + v, None)
    }

  private def leafPlanSolvedPredicates(plan: LogicalPlan, solveds: Solveds): Set[Predicate] = {
    val leaf = plan match {
      case _: IndexLeafPlan => Some(plan)
      case Selection(_, l: IndexLeafPlan) => Some(l)
      case _ => None
    }
    leaf.fold(Set.empty[Predicate])(l => solveds(l.id).queryGraph.selections.predicates)
  }

  /**
    * Given
    * - projection var("n") -> "m"
    * - usedExp    prop("m", "prop")
    * -> prop("n", "prop")
    *
    * Given
    * - projection var("n") -> "m"
    * - usedExp    var("m")
    * -> var("n")
    *
    * Given
    * - projection prop("n", "prop") -> "nprop"
    * - usedExp    var("n.prop")
    * -> prop("n", "prop")
    */
  private def renameExpressionsFromNextQueryPart(usedExpression: Expression, projectExpressions: Map[String, Expression]): Option[Expression] = {
    usedExpression match {
      case Property(Variable(newVarName), PropertyKeyName(propName)) =>
        projectExpressions.collectFirst {
          case (`newVarName`, Variable(oldVarName)) => Property(Variable(oldVarName)(InputPosition.NONE), PropertyKeyName(propName)(InputPosition.NONE))(InputPosition.NONE)
        }

      case Variable(newVarName) =>
        projectExpressions.collectFirst {
          case (`newVarName`, oldVar:Variable) => oldVar
          case (`newVarName`, oldProp@Property(v: Variable, _)) => oldProp
        }
    }
  }

  private def rewriter(usedExpressions: Set[Expression], query: PlannerQuery, solveds: Solveds, attributes: Attributes): Rewriter = topDown(Rewriter.lift {

    case x: NodeIndexSeek =>
      val aligned = alignedProperties(x, usedExpressions, query, solveds)
      NodeIndexSeek(x.idName, x.label, aligned, x.valueExpr, x.argumentIds, x.indexOrder)(attributes.copy(x.id))

    case x: NodeUniqueIndexSeek =>
      val aligned = alignedProperties(x, usedExpressions, query, solveds)
      NodeUniqueIndexSeek(x.idName, x.label, aligned, x.valueExpr, x.argumentIds, x.indexOrder)(attributes.copy(x.id))

    case x: NodeIndexContainsScan =>
      val aligned = alignedProperties(x, usedExpressions, query, solveds).head
      NodeIndexContainsScan(x.idName, x.label, aligned, x.valueExpr, x.argumentIds, x.indexOrder)(attributes.copy(x.id))

    case x: NodeIndexEndsWithScan =>
      val aligned = alignedProperties(x, usedExpressions, query, solveds).head
      NodeIndexEndsWithScan(x.idName, x.label, aligned, x.valueExpr, x.argumentIds, x.indexOrder)(attributes.copy(x.id))

    case x: NodeIndexScan =>
      val aligned = alignedProperties(x, usedExpressions, query, solveds).head
      NodeIndexScan(x.idName, x.label, aligned, x.argumentIds, x.indexOrder)(attributes.copy(x.id))
  },
    // We don't want to traverse down into union trees, even if that means we will leave the setting at CanGetValue, instead of DoNotGetValue
    stopper = {
      case _: Union => true
      case _ => false
    })

  private def alignedProperties(plan: IndexLeafPlan,
                                usedExpressions: Set[Expression],
                                query: PlannerQuery,
                                solveds: Solveds): Seq[IndexedProperty] = {
    val solvedPredicates = leafPlanSolvedPredicates(plan, solveds)
    val moreUsedExpressions = collectPropertiesAndVariables((query.queryGraph.selections.predicates -- solvedPredicates).map(_.expr))
    val allUsedExpressions = usedExpressions ++ moreUsedExpressions
    plan.properties.map(withAlignedGetValueBehavior(plan.idName, allUsedExpressions, _))
  }

  /**
    * Returns a copy of the provided indexedProperty with the correct GetValueBehavior set.
    */
  private def withAlignedGetValueBehavior(idName: String,
                                          usedExpressions: Set[Expression],
                                          indexedProperty: IndexedProperty): IndexedProperty = indexedProperty match {
    case ip@IndexedProperty(PropertyKeyToken(_, _), DoNotGetValue) => ip
    case ip@IndexedProperty(PropertyKeyToken(_, _), GetValue) => throw new IllegalStateException("Whether to get values from an index is not decided yet")
    case ip@IndexedProperty(PropertyKeyToken(propName, _), CanGetValue) =>
      val propExpression = Property(Variable(idName)(InputPosition.NONE), PropertyKeyName(propName)(InputPosition.NONE))(InputPosition.NONE)
      if (usedExpressions.contains(propExpression)) {
        // Get the value since we use it later
        ip.copy(getValueFromIndex = GetValue)
      } else {
        // We could get the value but we don't need it later
        ip.copy(getValueFromIndex = DoNotGetValue)
      }
  }
}
