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
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.AndedPropertyInequalitiesRemoved
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.LogicalPlanUsesEffectiveOutputCardinality
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.IndexCompatiblePredicatesProviderContext
import org.neo4j.cypher.internal.expressions.Ands
import org.neo4j.cypher.internal.expressions.CachedHasProperty
import org.neo4j.cypher.internal.expressions.CachedProperty
import org.neo4j.cypher.internal.expressions.EntityType
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.IsNotNull
import org.neo4j.cypher.internal.expressions.NODE_TYPE
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.RELATIONSHIP_TYPE
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.CompilationPhase.LOGICAL_PLANNING
import org.neo4j.cypher.internal.frontend.phases.Phase
import org.neo4j.cypher.internal.frontend.phases.Transformer
import org.neo4j.cypher.internal.frontend.phases.factories.PlanPipelineTransformerFactory
import org.neo4j.cypher.internal.logical.plans.CanGetValue
import org.neo4j.cypher.internal.logical.plans.DoNotGetValue
import org.neo4j.cypher.internal.logical.plans.GetValue
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.LogicalPlans
import org.neo4j.cypher.internal.logical.plans.NestedPlanExpression
import org.neo4j.cypher.internal.logical.plans.NodeIndexLeafPlan
import org.neo4j.cypher.internal.logical.plans.ProjectingPlan
import org.neo4j.cypher.internal.logical.plans.RelationshipIndexLeafPlan
import org.neo4j.cypher.internal.logical.plans.Selection
import org.neo4j.cypher.internal.logical.plans.Union
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
case class InsertCachedProperties(pushdownPropertyReads: Boolean)
    extends Phase[PlannerContext, LogicalPlanState, LogicalPlanState] {

  override def phase: CompilationPhaseTracer.CompilationPhase = LOGICAL_PLANNING

  override def postConditions: Set[StepSequencer.Condition] = InsertCachedProperties.postConditions

  override def process(from: LogicalPlanState, context: PlannerContext): LogicalPlanState = {

    val logicalPlan =
      if (pushdownPropertyReads) {
        val effectiveCardinalities = from.planningAttributes.effectiveCardinalities
        val attributes = from.planningAttributes.asAttributes(context.logicalPlanIdGen)
        PushdownPropertyReads.pushdown(from.logicalPlan, effectiveCardinalities, attributes, from.semanticTable())
      } else {
        from.logicalPlan
      }

    def isNode(variable: Variable) = from.semanticTable().isNodeNoFail(variable.name)
    def isRel(variable: Variable) = from.semanticTable().isRelationshipNoFail(variable.name)

    /**
     * A summary of all usages of a property
     *
     * @param canGetFromIndex      if the property can be read from an index.
     * @param usages               how many accesses to the property happen
     * @param entityType           the type of property
     * @param firstWritingAccesses if there can be determined a first logical plan that writes the property,
     *                             this contains that plan and all accesses that happen to this property from that plan.
     */
    case class PropertyUsages(
      canGetFromIndex: Boolean,
      usages: Int,
      entityType: EntityType,
      firstWritingAccesses: Option[(LogicalPlan, Seq[Property])],
      needsValue: Boolean
    ) {
      // always prefer reading from index
      def registerIndexUsage: PropertyUsages =
        copy(canGetFromIndex = true, firstWritingAccesses = None, needsValue = true)

      def addUsage(prop: Property, accessingPlan: LogicalPlan, newNeedsValue: Boolean): PropertyUsages = {
        val fWA =
          if (canGetFromIndex) None
          else firstWritingAccesses match {
            case None                                 => Some((accessingPlan, Seq(prop)))
            case Some((`accessingPlan`, otherUsages)) => Some((accessingPlan, otherUsages :+ prop))
            case x                                    => x
          }

        copy(usages = usages + 1, firstWritingAccesses = fWA, needsValue = needsValue || newNeedsValue)
      }

      def ++(other: PropertyUsages): PropertyUsages = PropertyUsages(
        this.canGetFromIndex || other.canGetFromIndex,
        this.usages + other.usages,
        this.entityType,
        this.firstWritingAccesses.orElse(other.firstWritingAccesses),
        this.needsValue || other.needsValue
      )
    }

    val NODE_NO_PROP_USAGE = PropertyUsages(canGetFromIndex = false, 0, NODE_TYPE, None, needsValue = false)
    val REL_NO_PROP_USAGE = PropertyUsages(canGetFromIndex = false, 0, RELATIONSHIP_TYPE, None, needsValue = false)

    case class Acc(
      properties: Map[Property, PropertyUsages] = Map.empty,
      previousNames: Map[String, String] = Map.empty
    ) {

      def ++(other: Acc): Acc = Acc(
        this.properties.fuse(other.properties)(_ ++ _),
        this.previousNames ++ other.previousNames
      )

      def addIndexNodeProperty(prop: Property): Acc = {
        val previousUsages = properties.getOrElse(prop, NODE_NO_PROP_USAGE)
        val newProperties = properties.updated(prop, previousUsages.registerIndexUsage)
        copy(properties = newProperties)
      }

      def addIndexRelationshipProperty(prop: Property): Acc = {
        val previousUsages = properties.getOrElse(prop, REL_NO_PROP_USAGE)
        val newProperties = properties.updated(prop, previousUsages.registerIndexUsage)
        copy(properties = newProperties)
      }

      def addNodeProperty(prop: Property, accessingPlan: LogicalPlan, needsValue: Boolean): Acc = {
        val originalProp = originalProperty(prop)
        val previousUsages: PropertyUsages = properties.getOrElse(originalProp, NODE_NO_PROP_USAGE)
        val newProperties = properties.updated(originalProp, previousUsages.addUsage(prop, accessingPlan, needsValue))
        copy(properties = newProperties)
      }

      def addRelProperty(prop: Property, accessingPlan: LogicalPlan, needsValue: Boolean): Acc = {
        val originalProp = originalProperty(prop)
        val previousUsages = properties.getOrElse(originalProp, REL_NO_PROP_USAGE)
        val newProperties = properties.updated(originalProp, previousUsages.addUsage(prop, accessingPlan, needsValue))
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
                throw new IllegalStateException(
                  s"There was a cycle in names: $seenNames. This is likely a namespacing bug."
                )
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

      def resetUsagesCount(): Acc =
        copy(properties = properties.view.mapValues(_.copy(usages = 0)).toMap)
    }

    def findPropertiesInPlan(acc: Acc, logicalPlan: LogicalPlan): Acc = logicalPlan.folder.treeFold(acc) {
      // Find properties
      case IsNotNull(prop @ Property(v: Variable, _)) if isNode(v) =>
        acc =>
          SkipChildren(acc.addNodeProperty(prop, logicalPlan, needsValue = false))
      case IsNotNull(prop @ Property(v: Variable, _)) if isRel(v) =>
        acc =>
          SkipChildren(acc.addRelProperty(prop, logicalPlan, needsValue = false))
      case prop @ Property(v: Variable, _) if isNode(v) =>
        acc =>
          TraverseChildren(acc.addNodeProperty(prop, logicalPlan, needsValue = true))
      case prop @ Property(v: Variable, _) if isRel(v) =>
        acc =>
          TraverseChildren(acc.addRelProperty(prop, logicalPlan, needsValue = true))

      // New fold for nested plan expression
      case nested: NestedPlanExpression => acc =>
          val accWithNested = findPropertiesInTree(acc, nested.plan)
          TraverseChildren(accWithNested)

      // Don't traverse into other logical plans
      case lp: LogicalPlan if !(lp eq logicalPlan) =>
        acc =>
          SkipChildren(acc)
    }

    def findPropertiesInTree(initialAcc: Acc, logicalPlan: LogicalPlan): Acc = {
      // Traverses the plan tree in plan execution order
      LogicalPlans.foldPlan(initialAcc)(
        logicalPlan,
        (acc, p) => {
          val accWithProps = {
            val initialAcc = if (!p.isLeaf) acc else acc.resetUsagesCount()
            findPropertiesInPlan(initialAcc, p)
          }

          p match {
            // Make sure to register any renaming of variables
            case plan: ProjectingPlan =>
              val newRenamings = plan.projectExpressions.collect {
                case (key, v: Variable) if key != v.name => (key, v.name)
              }
              accWithProps.addPreviousNames(newRenamings)

            // Find index plans that can provide cached properties
            case indexPlan: NodeIndexLeafPlan =>
              indexPlan.properties.filter(_.getValueFromIndex == CanGetValue).foldLeft(accWithProps) {
                (innerAcc, indexedProp) =>
                  innerAcc.addIndexNodeProperty(property(indexPlan.idName, indexedProp.propertyKeyToken.name))
              }
            case indexPlan: RelationshipIndexLeafPlan =>
              indexPlan.properties.filter(_.getValueFromIndex == CanGetValue).foldLeft(accWithProps) {
                (innerAcc, indexedProp) =>
                  innerAcc.addIndexRelationshipProperty(property(indexPlan.idName, indexedProp.propertyKeyToken.name))
              }

            case _ => accWithProps
          }
        },
        (lhsAcc, rhsAcc, plan) =>
          plan match {
            case _: Union =>
              // Take on only consistent renaming across both unions and remember properties from both subtrees
              val mergedNames = lhsAcc.previousNames.filter(entry =>
                rhsAcc.previousNames.get(entry._1) match {
                  case None        => false
                  case Some(value) => value.equals(entry._2)
                }
              )
              val mergedProperties = lhsAcc.properties.fuse(rhsAcc.properties) {
                (lhs, rhs) => (lhs ++ rhs).copy(usages = math.max(lhs.usages, rhs.usages))
              }
              Acc(mergedProperties, mergedNames)

            case plan =>
              val combinedChildAcc = lhsAcc ++ rhsAcc
              findPropertiesInPlan(combinedChildAcc, plan)
          }
      )
    }

    // In the first step we collect all property usages and renaming while going over the tree
    val acc = findPropertiesInTree(Acc(), logicalPlan)

    var currentTypes = from.semanticTable().types

    // In the second step we rewrite both properties and index plans
    val propertyRewriter = bottomUp(Rewriter.lift {
      // Rewrite properties to be cached if they are used more than once, or can be fetched from an index
      case prop @ Property(v: Variable, propertyKeyName) =>
        val originalVar = acc.variableWithOriginalName(v)
        val originalProp = acc.originalProperty(prop)
        acc.properties.get(originalProp) match {
          case Some(PropertyUsages(canGetFromIndex, usages, entityType, firstWritingAccesses, needsValue))
            if usages > 1 || canGetFromIndex =>
            // Use the original variable name for the cached property
            val knownToAccessStore = firstWritingAccesses.exists {
              case (_, properties) => properties.exists(_ eq prop)
            }
            val newProperty = {
              if (needsValue) {
                CachedProperty(originalVar.name, v, propertyKeyName, entityType, knownToAccessStore)(prop.position)
              } else {
                CachedHasProperty(originalVar.name, v, propertyKeyName, entityType, knownToAccessStore)(prop.position)
              }
            }
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
      case indexPlan: NodeIndexLeafPlan =>
        indexPlan.withMappedProperties { indexedProp =>
          acc.properties.get(property(indexPlan.idName, indexedProp.propertyKeyToken.name)) match {
            // Get the value since we use it later
            case Some(PropertyUsages(true, usages, _, _, _)) if usages >= 1 =>
              indexedProp.copy(getValueFromIndex = GetValue)
            // We could get the value but we don't need it later
            case _ =>
              indexedProp.copy(getValueFromIndex = DoNotGetValue)
          }
        }
      case indexPlan: RelationshipIndexLeafPlan =>
        indexPlan.withMappedProperties { indexedProp =>
          acc.properties.get(property(indexPlan.idName, indexedProp.propertyKeyToken.name)) match {
            // Get the value since we use it later
            case Some(PropertyUsages(true, usages, _, _, _)) if usages >= 1 =>
              indexedProp.copy(getValueFromIndex = GetValue)
            // We could get the value but we don't need it later
            case _ =>
              indexedProp.copy(getValueFromIndex = DoNotGetValue)
          }
        }

      case s: Selection =>
        // Since CachedProperties are cheaper than Properties, the previously best predicate evaluation order in a Selection
        // might not be the best order any more.
        // We re-order the predicates to find the new best order.
        val newPredicates = resortSelectionPredicates(from, context, s)
        s.copy(predicate = Ands(newPredicates)(s.predicate.position))(SameId(s.id))
    })

    val plan = propertyRewriter(logicalPlan).asInstanceOf[LogicalPlan]
    val newSemanticTable =
      if (currentTypes == from.semanticTable().types) from.semanticTable()
      else from.semanticTable().copy(types = currentTypes)
    from.withMaybeLogicalPlan(Some(plan)).withSemanticTable(newSemanticTable)
  }

  protected[steps] def resortSelectionPredicates(
    from: LogicalPlanState,
    context: PlannerContext,
    s: Selection
  ): Seq[Expression] = {
    LogicalPlanProducer.sortPredicatesBySelectivity(
      s.source,
      s.predicate.exprs.toSeq,
      QueryGraphSolverInput.empty,
      from.semanticTable(),
      IndexCompatiblePredicatesProviderContext.default,
      from.planningAttributes.solveds,
      // These do not include effective cardinalities, which is important since this methods assumes "original" cardinalities.
      from.planningAttributes.cardinalities,
      context.metrics.cardinality
    )
  }

  override def name: String = "insertCachedProperties"

  def property(entity: String, propName: String): Property =
    Property(Variable(entity)(InputPosition.NONE), PropertyKeyName(propName)(InputPosition.NONE))(InputPosition.NONE)
}

case object InsertCachedProperties extends StepSequencer.Step with PlanPipelineTransformerFactory {

  override def preConditions: Set[StepSequencer.Condition] = Set(
    // This rewriter operates on the LogicalPlan
    CompilationContains[LogicalPlan],
    // AndedPropertyInequalities contain the same property twice, which would mess up our counts.
    AndedPropertyInequalitiesRemoved,
    // PushdownPropertyReads needs effectiveCardinalities
    LogicalPlanUsesEffectiveOutputCardinality
  )

  override def postConditions: Set[StepSequencer.Condition] = Set(
    PropertiesAreCached
  )

  override def invalidatedConditions: Set[StepSequencer.Condition] = Set(
    // Rewriting logical plans introduces new IDs
    PlanIDsAreCompressed
  )

  override def getTransformer(
    pushdownPropertyReads: Boolean,
    semanticFeatures: Seq[SemanticFeature]
  ): Transformer[PlannerContext, LogicalPlanState, LogicalPlanState] = InsertCachedProperties(pushdownPropertyReads)
}
