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
import org.neo4j.cypher.internal.v3_5.logical.plans._
import org.opencypher.v9_0.expressions._
import org.opencypher.v9_0.util.attribution.Attributes
import org.opencypher.v9_0.util.{InputPosition, Rewriter, topDown}

/**
  * This updates index leaf plans such that they have the right GetValueFromIndexBehavior.
  * The index leaf planners will always set `GetValue`, if the index has the capability to provide values.
  * Here, we set this to `DoNotGetValue`, if the property is not used in the rest of the PlannerQuery, aside from the predicate.
  */
case class alignGetValueFromIndexBehavior(query: PlannerQuery, lpp: LogicalPlanProducer, attributes: Attributes) extends LeafPlanUpdater {

  // TODO we can be cleverer here and also find depending expressions in other predicates or other places in the query
  // TODO if we consider passthrough (n AS n) in projections, we can also include depending expressions in later horizons/query graphs
  val horizonDependingExpressions: Set[Expression] = query.horizon.dependingExpressions.toSet

  val horizonDependingProperties: Set[Property] = horizonDependingExpressions.treeFold(Set.empty[Property]) {
    case prop: Property =>
      acc => (acc + prop, None)
  }

  def apply(leafPlan: LogicalPlan): LogicalPlan = {
    rewriter(leafPlan).asInstanceOf[LogicalPlan]
  }

  /**
    * Returns a copy of the provided indexedProperty with the correct GetValueBehavior set.
    */
  def setGetValueBehavior(idName: String, indexedProperty: IndexedProperty): IndexedProperty = indexedProperty match {
    case ip@IndexedProperty(PropertyKeyToken(propName, _), DoNotGetValue) => ip
    case ip@IndexedProperty(PropertyKeyToken(propName, _), GetValue) =>
      val propExpression = Property(Variable(idName)(InputPosition.NONE), PropertyKeyName(propName)(InputPosition.NONE))(InputPosition.NONE)
      if (horizonDependingProperties.contains(propExpression)) {
        // Get the value since we use it later
        ip
      } else {
        // We could get the value but we don't need it later
        ip.copy(getValueFromIndex = DoNotGetValue)
      }
  }

  private val rewriter: Rewriter = topDown(Rewriter.lift {

    // We can't get values in a union that is planned by OrLeafPlanner
    case union@Union(l1:IndexLeafPlan, l2:IndexLeafPlan) =>
      Union(l1.copyWithoutGettingValues, l2.copyWithoutGettingValues)(attributes.copy(union.id))

    case union@Union(s1@Selection(pred1, l1:IndexLeafPlan), s2@Selection(pred2, l2:IndexLeafPlan)) =>
      Union(
        Selection(pred1, l1.copyWithoutGettingValues)(attributes.copy(s1.id)),
        Selection(pred2, l2.copyWithoutGettingValues)(attributes.copy(s2.id))
      )(attributes.copy(union.id))

    case seek@NodeIndexSeek(idName, _, properties, _, _) =>
      val newProperties = properties.map(setGetValueBehavior(idName, _))
      NodeIndexSeek(idName, seek.label, newProperties, seek.valueExpr, seek.argumentIds)(attributes.copy(seek.id))

    case seek@NodeUniqueIndexSeek(idName, _, properties, _, _) =>
      val newProperties = properties.map(setGetValueBehavior(idName, _))
      NodeUniqueIndexSeek(idName, seek.label, newProperties, seek.valueExpr, seek.argumentIds)(attributes.copy(seek.id))

    case scan@NodeIndexContainsScan(idName, _, property, _, _) =>
      val newProperty = setGetValueBehavior(idName, property)
      NodeIndexContainsScan(idName, scan.label, newProperty, scan.valueExpr, scan.argumentIds)(attributes.copy(scan.id))

    case scan@NodeIndexEndsWithScan(idName, _, property, _, _) =>
      val newProperty = setGetValueBehavior(idName, property)
      NodeIndexEndsWithScan(idName, scan.label, newProperty, scan.valueExpr, scan.argumentIds)(attributes.copy(scan.id))

    case scan@NodeIndexScan(idName, _, property, _) =>
      val newProperty = setGetValueBehavior(idName, property)
      NodeIndexScan(idName, scan.label, newProperty, scan.argumentIds)(attributes.copy(scan.id))

    // Don't update other plans
    case x => x
  })
}
