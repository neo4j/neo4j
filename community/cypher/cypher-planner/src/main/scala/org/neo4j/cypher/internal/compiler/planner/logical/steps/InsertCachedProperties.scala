/*
 * Copyright (c) "Neo4j"
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

import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.compiler.helpers.MapSupport.PowerMap
import org.neo4j.cypher.internal.compiler.phases.CompilationContains
import org.neo4j.cypher.internal.compiler.phases.LogicalPlanState
import org.neo4j.cypher.internal.compiler.phases.PlannerContext
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.QueryGraphSolverInput
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.LogicalPlanUsesEffectiveOutputCardinality
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.SingleAndedPropertyInequalitiesRemoved
import org.neo4j.cypher.internal.expressions.Ands
import org.neo4j.cypher.internal.expressions.CachedProperty
import org.neo4j.cypher.internal.expressions.EntityType
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.NODE_TYPE
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.RELATIONSHIP_TYPE
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.frontend.phases.Transformer
import org.neo4j.cypher.internal.frontend.phases.factories.PlanPipelineTransformerFactory
import org.neo4j.cypher.internal.logical.plans.ApplyPlan
import org.neo4j.cypher.internal.logical.plans.CanGetValue
import org.neo4j.cypher.internal.logical.plans.DoNotGetValue
import org.neo4j.cypher.internal.logical.plans.GetValue
import org.neo4j.cypher.internal.logical.plans.IndexLeafPlan
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.LogicalPlans
import org.neo4j.cypher.internal.logical.plans.NestedPlanExpression
import org.neo4j.cypher.internal.logical.plans.ProjectingPlan
import org.neo4j.cypher.internal.logical.plans.Selection
import org.neo4j.cypher.internal.util.Foldable.SkipChildren
import org.neo4j.cypher.internal.util.Foldable.TraverseChildren
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.attribution.SameId
import org.neo4j.cypher.internal.util.bottomUp

import scala.collection.mutable

case object PropertiesAreCached extends StepSequencer.Condition

/**
 * A logical plan rewriter that also changes the semantic table (thus a Transformer).
 *
 * It traverses the plan and swaps property lookups for cached properties where possible.
 */
case class InsertCachedProperties(pushdownPropertyReads: Boolean) extends Transformer[PlannerContext, LogicalPlanState, LogicalPlanState] {

  override def transform(from: LogicalPlanState, context: PlannerContext): LogicalPlanState = {

    val logicalPlan =
      if (pushdownPropertyReads) {
        val cardinalities = from.planningAttributes.cardinalities
        val attributes = from.planningAttributes.asAttributes(context.logicalPlanIdGen)
        PushdownPropertyReads.pushdown(from.logicalPlan, cardinalities, attributes, from.semanticTable())
      } else {
        from.logicalPlan
      }

    def isNode(variable: Variable) = from.semanticTable().isNodeNoFail(variable.name)
    def isRel(variable: Variable) = from.semanticTable().isRelationshipNoFail(variable.name)

    case class PropertyUsages(canGetFromIndex: Boolean,
                              usages: Int,
                              entityType: EntityType,
                              firstWritingAccess: Option[Property]) {
      //always prefer reading from index
      def registerIndexUsage: PropertyUsages =
        copy(canGetFromIndex = true, firstWritingAccess = None)
      def addUsage(prop: Property): PropertyUsages = {
        val fWA = if (canGetFromIndex) None else firstWritingAccess.orElse(Some(prop))
        copy(usages = usages + 1, firstWritingAccess = fWA)
      }

      def ++(other: PropertyUsages): PropertyUsages = PropertyUsages(
        this.canGetFromIndex || other.canGetFromIndex,
        this.usages + other.usages,
        this.entityType,
        this.firstWritingAccess.orElse(other.firstWritingAccess)
      )
    }

    val NODE_NO_PROP_USAGE = PropertyUsages(canGetFromIndex = false, 0, NODE_TYPE, None)
    val REL_NO_PROP_USAGE = PropertyUsages(canGetFromIndex = false, 0, RELATIONSHIP_TYPE, None)

    case class Acc(properties: Map[Property, PropertyUsages] = Map.empty,
                   previousNames: Map[String, String] = Map.empty) {

      def ++(other: Acc): Acc = Acc(
        this.properties.fuse(other.properties)(_ ++ _),
        this.previousNames ++ other.previousNames
      )

      def addIndexNodeProperty(prop: Property): Acc = {
        val previousUsages = properties.getOrElse(prop, NODE_NO_PROP_USAGE)
        val newProperties = properties.updated(prop, previousUsages.registerIndexUsage)
        copy(properties = newProperties)
      }

      def addNodeProperty(prop: Property): Acc = {
        val originalProp = originalProperty(prop)
        val previousUsages = properties.getOrElse(originalProp, NODE_NO_PROP_USAGE)
        val newProperties = properties.updated(originalProp, previousUsages.addUsage(prop))
        copy(properties = newProperties)
      }

      def addRelProperty(prop: Property): Acc = {
        val originalProp = originalProperty(prop)
        val previousUsages = properties.getOrElse(originalProp, REL_NO_PROP_USAGE)
        val newProperties = properties.updated(originalProp, previousUsages.addUsage(prop))
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

    def findPropertiesInPlan(acc: Acc, logicalPlan: LogicalPlan): Acc = logicalPlan.treeFold(acc) {
      // Find properties
      case prop@Property(v: Variable, _) if isNode(v) => acc =>
        TraverseChildren(acc.addNodeProperty(prop))
      case prop@Property(v: Variable, _) if isRel(v) => acc =>
        TraverseChildren(acc.addRelProperty(prop))

      // New fold for nested plan expression
      case nested:NestedPlanExpression => acc =>
        val accWithNested = findPropertiesInTree(acc, nested.plan)
        TraverseChildren(accWithNested)

      // Don't traverse into other logical plans
      case lp: LogicalPlan if !(lp eq logicalPlan) => acc =>
        SkipChildren(acc)
    }

    def findPropertiesInTree(initialAcc: Acc, logicalPlan: LogicalPlan): Acc = {
      // Traverses the plan tree in plan execution order
      LogicalPlans.foldPlan(initialAcc)(
        logicalPlan,
        (acc, _, p) => {
          val accWithProps = findPropertiesInPlan(acc, p)
          p match {
            // Make sure to register any renaming of variables
            case plan: ProjectingPlan =>
              val newRenamings = plan.projectExpressions.collect {
                case (key, v: Variable) if key != v.name => (key, v.name)
              }
              accWithProps.addPreviousNames(newRenamings)

            // Find index plans that can provide cached properties
            case indexPlan: IndexLeafPlan =>
              indexPlan.properties.filter(_.getValueFromIndex == CanGetValue).foldLeft(accWithProps) { (innerAcc, indexedProp) =>
                innerAcc.addIndexNodeProperty(property(indexPlan.idName, indexedProp.propertyKeyToken.name))
              }

            case _ => accWithProps
          }
        },
        (lhsAcc, rhsAcc, plan) =>
          plan match {
            case _:ApplyPlan =>
              // The rhsAcc was initialized with lhsAcc as argument
              rhsAcc
            case _ =>
              // Both accs are independent and need to be combined.
              lhsAcc ++ rhsAcc
          }
      )
    }

    // In the first step we collect all property usages and renaming while going over the tree
    val acc = findPropertiesInTree(Acc(), logicalPlan)

    var currentTypes = from.semanticTable().types

    // In the second step we rewrite both properties and index plans
    val propertyRewriter = bottomUp(Rewriter.lift {
      // Rewrite properties to be cached if they are used more than once, or can be fetched from an index
      case prop@Property(v: Variable, propertyKeyName) =>
        val originalVar = acc.variableWithOriginalName(v)
        val originalProp = acc.originalProperty(prop)
        acc.properties.get(originalProp) match {
          case Some(PropertyUsages(canGetFromIndex, usages, entityType, firstWritingAccess)) if usages > 1 || canGetFromIndex =>
            // Use the original variable name for the cached property
            val knownToAccessStore = firstWritingAccess.exists(_ eq prop)
            val newProperty = CachedProperty(originalVar.name, v, propertyKeyName, entityType, knownToAccessStore)(prop.position)
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
            case Some(PropertyUsages(true, usages, _, _)) if usages >= 1 =>
              indexedProp.copy(getValueFromIndex = GetValue)
            // We could get the value but we don't need it later
            case _ =>
              indexedProp.copy(getValueFromIndex = DoNotGetValue)
          }
        }

      case s:Selection =>
        // Since CachedProperties are cheaper than Properties, the previously best predicate evaluation order in a Selection
        // might not be the best order any more.
        // We re-order the predicates to find the new best order.
        val newPredicates = resortSelectionPredicates(from, context, s)
        s.copy(predicate = Ands(newPredicates)(s.predicate.position))(SameId(s.id))
    })

    val plan = propertyRewriter(logicalPlan).asInstanceOf[LogicalPlan]
    val newSemanticTable = if (currentTypes == from.semanticTable().types) from.semanticTable() else from.semanticTable().copy(types = currentTypes)
    from.withMaybeLogicalPlan(Some(plan)).withSemanticTable(newSemanticTable)
  }

  protected[steps] def resortSelectionPredicates(from: LogicalPlanState,
                                                 context: PlannerContext,
                                                 s: Selection): Seq[Expression] = {
    LogicalPlanProducer.sortPredicatesBySelectivity(
      s.source,
      s.predicate.exprs,
      QueryGraphSolverInput.empty,
      from.semanticTable(),
      from.planningAttributes.solveds,
      from.planningAttributes.cardinalities,
      context.metrics.cardinality
    )
  }

  override def name: String = "insertCachedProperties"

  def property(entity: String, propName: String): Property =
    Property(Variable(entity)(InputPosition.NONE), PropertyKeyName(propName)(InputPosition.NONE))(InputPosition.NONE)
}

object InsertCachedProperties extends StepSequencer.Step with PlanPipelineTransformerFactory {
  override def preConditions: Set[StepSequencer.Condition] = Set(
    // This rewriter operates on the LogicalPlan
    CompilationContains[LogicalPlan],
    // AndedPropertyInequalities contain the same property twice, which would mess up our counts.
    SingleAndedPropertyInequalitiesRemoved,
    // If effective output cardinality has already been calculated, the calculations of LogicalPlanProducer$sortPredicatesBySelectivity will be incorrect.
    !LogicalPlanUsesEffectiveOutputCardinality
  )

  override def postConditions: Set[StepSequencer.Condition] = Set(
    PropertiesAreCached
  )

  override def invalidatedConditions: Set[StepSequencer.Condition] = Set(
    // Rewriting logical plans introduces new IDs
    PlanIDsAreCompressed
  )

  override def getTransformer(pushdownPropertyReads: Boolean,
                              semanticFeatures: Seq[SemanticFeature]): Transformer[PlannerContext, LogicalPlanState, LogicalPlanState] = InsertCachedProperties(pushdownPropertyReads)
}