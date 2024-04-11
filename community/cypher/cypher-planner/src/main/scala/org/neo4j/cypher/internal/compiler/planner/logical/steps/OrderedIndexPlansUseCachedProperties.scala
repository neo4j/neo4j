/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.planner.logical.steps

import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.logical.plans.GetValue
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.logical.plans.IndexedProperty
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.NodeIndexLeafPlan
import org.neo4j.cypher.internal.logical.plans.ProduceResult
import org.neo4j.cypher.internal.logical.plans.RelationshipIndexLeafPlan
import org.neo4j.cypher.internal.logical.plans.UpdatingPlan
import org.neo4j.cypher.internal.rewriting.ValidatingCondition
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.Foldable.FoldableAny
import org.neo4j.cypher.internal.util.Foldable.SkipChildren
import org.neo4j.cypher.internal.util.Foldable.TraverseChildren
import org.neo4j.cypher.internal.util.InputPosition

/**
 * Checks to ensure that plans will return correct results
 * under concurrent updates and read-committed anomalies.
 *
 * There are two checks, both variants of making sure that a property that can be obtained from an ordered index
 * is obtained from there and not from the property store.
 *
 * 1) For all entities of which all properties are read (e.g. in ValuePopulation in ProduceResults),
 *    any ordered index plan on that entity must cache all properties.
 * 2) For all ordered indexes, for all their properties, they must not appear non-cached anywhere in the plan.
 *
 * This analysis won't detect all edge cases, as it would otherwise reimplement large parts of the
 * InsertCachedProperties logic. E.g. it does not recognize the `properties` function, or renamings of variables.
 */
case object OrderedIndexPlansUseCachedProperties extends ValidatingCondition {

  private val expressionStringifier = ExpressionStringifier()

  override def name: String = productPrefix

  private def indexedPropertyToProperty(idName: LogicalVariable)(indexedProperty: IndexedProperty) = {
    Property(idName, PropertyKeyName(indexedProperty.propertyKeyToken.name)(InputPosition.NONE))(InputPosition.NONE)
  }

  override def apply(a: Any)(cancellationChecker: CancellationChecker): Seq[String] = {
    val returnedEntities = a match {
      case produceResult: ProduceResult => produceResult.columns.toSet
      case _: LogicalPlan               =>
        // In unit tests, we run with incomplete plans
        Set.empty[LogicalVariable]
      case _ => throw new IllegalStateException()
    }

    // If an entity is returned, then all properties from an index plan of that entity must be cached.
    val indexPropertiesThatMustBeCachedButAreNot = a.folder(cancellationChecker).treeCollect {
      case x: NodeIndexLeafPlan if x.indexOrder != IndexOrderNone && returnedEntities.contains(x.idName) =>
        x.properties.view
          .filter(_.getValueFromIndex != GetValue)
          .map(x -> indexedPropertyToProperty(x.idName)(_))
      case x: RelationshipIndexLeafPlan if x.indexOrder != IndexOrderNone && returnedEntities.contains(x.idName) =>
        x.properties.view
          .filter(_.getValueFromIndex != GetValue)
          .map(x -> indexedPropertyToProperty(x.idName)(_))
    }.flatten.map {
      case (indexPlan, property) =>
        s"$indexPlan does not cache ${expressionStringifier(property)}, but the entity is returned in ProduceResult."
    }

    val propertiesThatMustBeCached = a.folder(cancellationChecker).treeCollect {
      case x: NodeIndexLeafPlan if x.indexOrder != IndexOrderNone =>
        x.properties.map(indexedPropertyToProperty(x.idName))
      case x: RelationshipIndexLeafPlan if x.indexOrder != IndexOrderNone =>
        x.properties.map(indexedPropertyToProperty(x.idName))
    }.flatten.toSet

    // If an index is ordered, then the properties of the index must not appear non-cached anywhere in the plan.
    val propertiesThatMustBeCachedButAreNot = a.folder(cancellationChecker).treeFold(List.empty[String]) {
      case _: UpdatingPlan =>
        // SET, potentially nested in MERGE or FOREACH, is allowed to skip using a caching property.
        // In these cases we anyway will plan a Sort, so we don't run the risk of returning out-of-order results.
        SkipChildren(_)
      case prop: Property if propertiesThatMustBeCached.contains(prop) =>
        acc => TraverseChildren(s"${expressionStringifier(prop)} should not appear non-cached." :: acc)
    }

    indexPropertiesThatMustBeCachedButAreNot ++ propertiesThatMustBeCachedButAreNot
  }
}
