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
package org.neo4j.cypher.internal.compiler.planner.logical.steps

import org.neo4j.cypher.internal.compiler.phases.LogicalPlanState
import org.neo4j.cypher.internal.compiler.phases.PlannerContext
import org.neo4j.cypher.internal.expressions.CachedProperty
import org.neo4j.cypher.internal.expressions.EntityType
import org.neo4j.cypher.internal.expressions.NODE_TYPE
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.RELATIONSHIP_TYPE
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.frontend.phases.Transformer
import org.neo4j.cypher.internal.logical.plans.CanGetValue
import org.neo4j.cypher.internal.logical.plans.CursorProperty
import org.neo4j.cypher.internal.logical.plans.DoNotGetValue
import org.neo4j.cypher.internal.logical.plans.Expand
import org.neo4j.cypher.internal.logical.plans.GetValue
import org.neo4j.cypher.internal.logical.plans.IndexLeafPlan
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.MultiNodeIndexSeek
import org.neo4j.cypher.internal.logical.plans.OptionalExpand
import org.neo4j.cypher.internal.logical.plans.ProjectingPlan
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.bottomUp
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTRelationship

import scala.collection.mutable

/**
 * A logical plan rewriter that also changes the semantic table (thus a Transformer).
 *
 * It traverses the plan and swaps property lookups for cached properties where possible.
 */
case class InsertCachedProperties(pushdownPropertyReads: Boolean, readPropertiesFromCursor: Boolean = false) extends Transformer[PlannerContext, LogicalPlanState, LogicalPlanState] {

  override def transform(from: LogicalPlanState, context: PlannerContext): LogicalPlanState = {

    val logicalPlan =
      if (pushdownPropertyReads) {
        val cardinalities = from.planningAttributes.cardinalities
        val attributes = from.planningAttributes.asAttributes(context.logicalPlanIdGen)
        PushdownPropertyReads.pushdown(from.logicalPlan, cardinalities, attributes, from.semanticTable())
      } else {
        from.logicalPlan
      }
    def isNode(variable: Variable) = from.semanticTable().types.get(variable).exists(t => t.actual == CTNode.invariant)
    def isRel(variable: Variable) = from.semanticTable().types.get(variable).exists(t => t.actual == CTRelationship.invariant)

    case class PropertyUsages(canGetFromIndex: Boolean, canReadFromCursor: Boolean, usages: Int, entityType: EntityType) {
      //always prefer reading from index
      def registerIndexUsage: PropertyUsages = copy(canGetFromIndex = true, canReadFromCursor = false)
      //always prefer reading from index
      def registerCanReadFromCursor: PropertyUsages = if (canReadFromCursor) this else copy(canReadFromCursor = true)
      def addUsage: PropertyUsages = copy(usages = usages + 1)
    }

    val NODE_NO_PROP_USAGE = PropertyUsages(canGetFromIndex = false, canReadFromCursor = false, 0, NODE_TYPE)
    val REL_NO_PROP_USAGE = PropertyUsages(canGetFromIndex = false, canReadFromCursor = false, 0, RELATIONSHIP_TYPE)

    case class Acc(properties: Map[Property, PropertyUsages] = Map.empty,
                   previousNames: Map[String, String] = Map.empty) {

      def addIndexNodeProperty(prop: Property): Acc = {
        val previousUsages = properties.getOrElse(prop, NODE_NO_PROP_USAGE)
        val newProperties = properties.updated(prop, previousUsages.registerIndexUsage)
        copy(properties = newProperties)
      }

      def addNodeProperty(prop: Property): Acc = {
        val originalProp = originalProperty(prop)
        val previousUsages = properties.getOrElse(originalProp, NODE_NO_PROP_USAGE)
        val newProperties = properties.updated(originalProp, previousUsages.addUsage)
        copy(properties = newProperties)
      }

      def addRelProperty(prop: Property): Acc = {
        val originalProp = originalProperty(prop)
        val previousUsages = properties.getOrElse(originalProp, REL_NO_PROP_USAGE)
        val newProperties = properties.updated(originalProp, previousUsages.addUsage)
        copy(properties = newProperties)
      }

      def readsNode(name: String): Acc = {
        val newProperties = properties.map {
          case (p@Property(Variable(n), _), v@PropertyUsages(_, _, _, NODE_TYPE)) if n == name =>
            p -> v.registerCanReadFromCursor
          case p => p
        }
        copy(properties = newProperties)
      }

      def readsRelationship(name: String): Acc = {
        val newProperties = properties.map {
          case (p@Property(Variable(r), _), v@PropertyUsages(_, _, _, RELATIONSHIP_TYPE)) if r == name =>
            p -> v.registerCanReadFromCursor
          case p => p
        }
        copy(properties = newProperties)
      }

      def addPreviousNames(mappings: Map[String, String]): Acc = {
        val newRenamings = previousNames ++ mappings

        // Find the original name of all variables so far, and fail if we have a cycle
        val normalizedRenamings = newRenamings.map {
          case (currentName, prevName) =>
            var name = prevName
            val seenNames = mutable.Set(currentName, prevName)
            while (newRenamings.contains(name)) {
              name = newRenamings(name)
              if (!seenNames.add(name)) {
                // We have a cycle
                throw new IllegalStateException(s"There was a cycle in names: $seenNames. This is likely a namespacing bug.")
              }
            }
            (currentName, name)
        }

        // Rename all properties that we found so far and that are affected by this
        val withPreviousNames = copy(previousNames = normalizedRenamings)
        val renamedProperties = properties.map { case (prop, use) => (withPreviousNames.originalProperty(prop), use) }
        withPreviousNames.copy(properties = renamedProperties)
      }

      def variableWithOriginalName(variable: Variable): Variable = {
        var name = variable.name
        while (previousNames.contains(name)) {
          name = previousNames(name)
        }
        variable.copy(name)(variable.position)
      }

      def originalProperty(prop: Property): Property = {
        val Property(v: Variable, _) = prop
        prop.copy(variableWithOriginalName(v))(prop.position)
      }
    }

    // In the first step we collect all property usages and renaming while going over the tree
    val acc = logicalPlan.treeFold(Acc()) {
      // Make sure to register any renaming of variables
      case plan: ProjectingPlan => acc =>
        val newRenamings = plan.projectExpressions.collect {
          case (key, v: Variable) if key != v.name => (key, v.name)
        }
        (acc.addPreviousNames(newRenamings), Some(identity))

      // Find properties
      case prop@Property(v: Variable, _) if isNode(v) => acc =>
        (acc.addNodeProperty(prop), Some(identity))
      case prop@Property(v: Variable, _) if isRel(v) => acc =>
        (acc.addRelProperty(prop), Some(identity))

      // Find index plans that can provide cached properties
      case _: MultiNodeIndexSeek => acc =>
        (acc, Some(identity))

      case indexPlan: IndexLeafPlan => acc =>
        val newAcc = indexPlan.properties.filter(_.getValueFromIndex == CanGetValue).foldLeft(acc) { (acc, indexedProp) =>
          acc.addIndexNodeProperty(property(indexPlan.idName, indexedProp.propertyKeyToken.name))
        }
        (newAcc, Some(identity))

      case expand: Expand if readPropertiesFromCursor => acc =>
          val newAcc = acc.readsNode(expand.from).readsRelationship(expand.relName)
          (newAcc, Some(identity))

      case expand: OptionalExpand if readPropertiesFromCursor => acc =>
        val newAcc = acc.readsNode(expand.from).readsRelationship(expand.relName)
        (newAcc, Some(identity))
    }

    var currentTypes = from.semanticTable().types

    // In the second step we rewrite both properties as well as plans that will cache properties, i.e. index plans and expands
    val alreadyCachedFromCursor = mutable.Set.empty[CursorProperty]
    val propertyRewriter = bottomUp(Rewriter.lift {
      // Rewrite properties to be cached if they are used more than once, or can be fetched from an index
      case prop@Property(v: Variable, propertyKeyName) =>
        val originalVar = acc.variableWithOriginalName(v)
        val originalProp = acc.originalProperty(prop)
        acc.properties.get(originalProp) match {
          case Some(PropertyUsages(canGetFromIndex, canReadFromCursor, usages, entityType)) if usages > 1 || (canGetFromIndex || canReadFromCursor) =>
            // Use the original variable name for the cached property
            val newProperty = CachedProperty(originalVar.name, v, propertyKeyName, entityType)(prop.position)
            // Register the new variables in the semantic table
            currentTypes.get(prop) match {
              case None => // I don't like this. We have to make sure we retain the type from semantic analysis
              case Some(currentType) =>
                currentTypes = currentTypes.updated(newProperty, currentType)
            }

            newProperty
          case _ =>
            prop
        }

      // Rewrite index plans to either GetValue or DoNotGetValue
      case indexPlan: IndexLeafPlan =>
        indexPlan.withMappedProperties { indexedProp =>
          acc.properties.get(property(indexPlan.idName, indexedProp.propertyKeyToken.name)) match {
            // Get the value since we use it later
            case Some(PropertyUsages(true, _, usages, _)) if usages >= 1 =>
              indexedProp.copy(getValueFromIndex = GetValue)
            // We could get the value but we don't need it later
            case _ =>
              indexedProp.copy(getValueFromIndex = DoNotGetValue)
          }
        }

      case e:Expand if readPropertiesFromCursor =>
        val nodePropsToCache = acc.properties.collect {
          case (Property(Variable(n), prop), PropertyUsages(_, true, usages, NODE_TYPE)) if n == e.from & usages >= 1 =>
            CursorProperty(n, NODE_TYPE, prop)
        }
        val relPropsToCache = acc.properties.collect {
          case (Property(Variable(r), prop), PropertyUsages(_, true, usages, RELATIONSHIP_TYPE)) if r == e.relName & usages >= 1 =>
            CursorProperty(r, RELATIONSHIP_TYPE, prop)
        }

        e.withNodeProperties(nodePropsToCache.filter(alreadyCachedFromCursor.add).toList:_*)
          .withRelationshipProperties(relPropsToCache.filter(alreadyCachedFromCursor.add).toList:_*)

      case e:OptionalExpand if readPropertiesFromCursor =>
        val nodePropsToCache = acc.properties.collect {
          case (Property(Variable(n), prop), PropertyUsages(_, true, usages, NODE_TYPE)) if n == e.from & usages >= 1 =>
            CursorProperty(n, NODE_TYPE, prop)
        }
        val relPropsToCache = acc.properties.collect {
          case (Property(Variable(r), prop), PropertyUsages(_, true, usages, RELATIONSHIP_TYPE)) if r == e.relName & usages >= 1 =>
            CursorProperty(r, RELATIONSHIP_TYPE, prop)
        }

        e.withNodeProperties(nodePropsToCache.filter(alreadyCachedFromCursor.add).toList:_*)
          .withRelationshipProperties(relPropsToCache.filter(alreadyCachedFromCursor.add).toList:_*)
    })

    val plan = propertyRewriter(logicalPlan).asInstanceOf[LogicalPlan]
    val newSemanticTable = if (currentTypes == from.semanticTable().types) from.semanticTable() else from.semanticTable().copy(types = currentTypes)
    from.withMaybeLogicalPlan(Some(plan)).withSemanticTable(newSemanticTable)
  }

  override def name: String = "insertCachedProperties"

  def property(entity: String, propName: String): Property =
    Property(Variable(entity)(InputPosition.NONE), PropertyKeyName(propName)(InputPosition.NONE))(InputPosition.NONE)


}
