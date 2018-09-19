/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
import org.opencypher.v9_0.expressions._
import org.opencypher.v9_0.util.Foldable.FoldableAny
import org.opencypher.v9_0.util.InputPosition
import org.opencypher.v9_0.util.Rewriter
import org.opencypher.v9_0.util.attribution.Attributes
import org.opencypher.v9_0.util.topDown

/**
  * This updates index leaf plans such that they have the right GetValueFromIndexBehavior.
  * The index leaf planners will always set `CanGetValue`, if the index has the capability to provide values.
  * Here, we set this to `GetValue` or `DoNotGetValue`, depending on if the property is used in the rest of the PlannerQuery, aside from the predicate.
  */
case class alignGetValueFromIndexBehavior(query: PlannerQuery, lpp: LogicalPlanProducer, solveds: Solveds, attributes: Attributes) extends LeafPlanUpdater {

  def apply(leafPlan: LogicalPlan): LogicalPlan = {
    // We want to find property usages only in those predicates that are not already solved by the leaf-plan we are rewriting
    val solvedPredicates = leafPlanSolvedPredicates(leafPlan, solveds)
    val usedExps = usedExpressionsInFirstPartWithIgnoredPredicates(query, solvedPredicates) ++ usedExpressionsInLaterParts
    rewriter(usedExps)(leafPlan).asInstanceOf[LogicalPlan]
  }

  private def usedExpressionsInFirstPartWithIgnoredPredicates(queryPart: PlannerQuery, predicatesToIgnore: Set[Predicate]): Set[Expression] = {
    val horizonDependingExpressions = queryPart.horizon.dependingExpressions.toSet
    val usedExpressionsInHorizon = collectPropertiesAndVariables(horizonDependingExpressions)
    val usedExpressionsInPredicates = collectPropertiesAndVariables((queryPart.queryGraph.selections.predicates -- predicatesToIgnore).map(_.expr))
    usedExpressionsInHorizon ++ usedExpressionsInPredicates
  }

  // We can cache the expressions used in later parts, they don't have any dependency on the leafPlan we're aligning
  private val usedExpressionsInLaterParts = usedExpressionsInLaterPartsRecursive(query, firstPart = true)

  private def usedExpressionsInLaterPartsRecursive(queryPart: PlannerQuery, firstPart: Boolean): Set[Expression] = {
    val maybeHorizonProjections = queryPart.horizon match {
      case projection: QueryProjection => Some(projection.projections)
      case _ => None
    }

    val usedExpressionsInThisPart =
      if (firstPart) {
        // The expressions used in the first part are not relevant here
        Set.empty
      } else {
        // Pass Set.empty since the leaf plan can only solve predicates of the first query part
        usedExpressionsInFirstPartWithIgnoredPredicates(queryPart, Set.empty)
      }

    val nextUsedExpressions = for {
      nextPart <- queryPart.tail.toSet[PlannerQuery]
      expressions <- usedExpressionsInLaterPartsRecursive(nextPart, firstPart = false)
      // If the horizon does not rename, keep the expressions as they are. Otherwise rename them for this query part
      renamedExpressions <- maybeHorizonProjections.fold(Option(expressions))(projections => renameExpressionsFromNextQueryPart(expressions, projections))
    } yield {
      renamedExpressions
    }
    usedExpressionsInThisPart ++ nextUsedExpressions
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

  private def rewriter(usedExpressions: Set[Expression]): Rewriter = topDown(Rewriter.lift {

    case x: NodeIndexSeek =>
      val alignedProperties = x.properties.map(withAlignedGetValueBehavior(x.idName, usedExpressions, _))
      NodeIndexSeek(x.idName, x.label, alignedProperties, x.valueExpr, x.argumentIds, x.indexOrder)(attributes.copy(x.id))

    case x: NodeUniqueIndexSeek =>
      val alignedProperties = x.properties.map(withAlignedGetValueBehavior(x.idName, usedExpressions, _))
      NodeUniqueIndexSeek(x.idName, x.label, alignedProperties, x.valueExpr, x.argumentIds, x.indexOrder)(attributes.copy(x.id))

    case x: NodeIndexContainsScan =>
      val alignedProperty = withAlignedGetValueBehavior(x.idName, usedExpressions, x.property)
      NodeIndexContainsScan(x.idName, x.label, alignedProperty, x.valueExpr, x.argumentIds, x.indexOrder)(attributes.copy(x.id))

    case x: NodeIndexEndsWithScan =>
      val alignedProperty = withAlignedGetValueBehavior(x.idName, usedExpressions, x.property)
      NodeIndexEndsWithScan(x.idName, x.label, alignedProperty, x.valueExpr, x.argumentIds, x.indexOrder)(attributes.copy(x.id))

    case x: NodeIndexScan =>
      val alignedProperty = withAlignedGetValueBehavior(x.idName, usedExpressions, x.property)
      NodeIndexScan(x.idName, x.label, alignedProperty, x.argumentIds, x.indexOrder)(attributes.copy(x.id))
  },
    // We don't want to traverse down into union trees, even if that means we will leave the setting at CanGetValue, instead of DoNotGetValue
    stopper = {
      case _: Union => true
      case _ => false
    })

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
