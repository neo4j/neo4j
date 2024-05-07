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

import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.phases.CompilationContains
import org.neo4j.cypher.internal.compiler.phases.LogicalPlanCondition
import org.neo4j.cypher.internal.compiler.phases.LogicalPlanState
import org.neo4j.cypher.internal.compiler.phases.PlannerContext
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.AndedPropertyInequalitiesRemoved
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.CardinalityRewriter
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.eager.LogicalPlanContainsEagerIfNeeded
import org.neo4j.cypher.internal.compiler.planner.logical.steps.InsertCachedProperties.PropertyUsages
import org.neo4j.cypher.internal.compiler.planner.logical.steps.InsertCachedProperties.PropertyUsagesAndRenamings
import org.neo4j.cypher.internal.compiler.planner.logical.steps.RestrictedCaching.CacheAll
import org.neo4j.cypher.internal.compiler.planner.logical.steps.RestrictedCaching.CachedPropertiesTracker
import org.neo4j.cypher.internal.compiler.planner.logical.steps.RestrictedCaching.ProtectedProperties
import org.neo4j.cypher.internal.expressions.ASTCachedProperty
import org.neo4j.cypher.internal.expressions.CachedHasProperty
import org.neo4j.cypher.internal.expressions.CachedProperty
import org.neo4j.cypher.internal.expressions.CaseExpression
import org.neo4j.cypher.internal.expressions.EntityType
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.IsNotNull
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.MapExpression
import org.neo4j.cypher.internal.expressions.NODE_TYPE
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.RELATIONSHIP_TYPE
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.expressions.functions.Properties
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.CompilationPhase.LOGICAL_PLANNING
import org.neo4j.cypher.internal.frontend.phases.Phase
import org.neo4j.cypher.internal.frontend.phases.Transformer
import org.neo4j.cypher.internal.frontend.phases.factories.PlanPipelineTransformerFactory
import org.neo4j.cypher.internal.ir.RemoveLabelPattern
import org.neo4j.cypher.internal.ir.SetLabelPattern
import org.neo4j.cypher.internal.ir.SetNodePropertiesFromMapPattern
import org.neo4j.cypher.internal.ir.SetNodePropertiesPattern
import org.neo4j.cypher.internal.ir.SetNodePropertyPattern
import org.neo4j.cypher.internal.ir.SetPropertiesFromMapPattern
import org.neo4j.cypher.internal.ir.SetPropertiesPattern
import org.neo4j.cypher.internal.ir.SetPropertyPattern
import org.neo4j.cypher.internal.ir.SetRelationshipPropertiesFromMapPattern
import org.neo4j.cypher.internal.ir.SetRelationshipPropertiesPattern
import org.neo4j.cypher.internal.ir.SetRelationshipPropertyPattern
import org.neo4j.cypher.internal.logical.plans.CanGetValue
import org.neo4j.cypher.internal.logical.plans.Create
import org.neo4j.cypher.internal.logical.plans.DeleteExpression
import org.neo4j.cypher.internal.logical.plans.DeleteNode
import org.neo4j.cypher.internal.logical.plans.DeletePath
import org.neo4j.cypher.internal.logical.plans.DeleteRelationship
import org.neo4j.cypher.internal.logical.plans.DetachDeleteExpression
import org.neo4j.cypher.internal.logical.plans.DetachDeleteNode
import org.neo4j.cypher.internal.logical.plans.DetachDeletePath
import org.neo4j.cypher.internal.logical.plans.DoNotGetValue
import org.neo4j.cypher.internal.logical.plans.Foreach
import org.neo4j.cypher.internal.logical.plans.GetValue
import org.neo4j.cypher.internal.logical.plans.IndexedPropertyProvidingPlan
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.LogicalPlans
import org.neo4j.cypher.internal.logical.plans.Merge
import org.neo4j.cypher.internal.logical.plans.NestedPlanExpression
import org.neo4j.cypher.internal.logical.plans.NodeIndexLeafPlan
import org.neo4j.cypher.internal.logical.plans.ProduceResult
import org.neo4j.cypher.internal.logical.plans.ProjectingPlan
import org.neo4j.cypher.internal.logical.plans.RelationshipIndexLeafPlan
import org.neo4j.cypher.internal.logical.plans.RemoveLabels
import org.neo4j.cypher.internal.logical.plans.SetLabels
import org.neo4j.cypher.internal.logical.plans.SetNodeProperties
import org.neo4j.cypher.internal.logical.plans.SetNodePropertiesFromMap
import org.neo4j.cypher.internal.logical.plans.SetNodeProperty
import org.neo4j.cypher.internal.logical.plans.SetProperties
import org.neo4j.cypher.internal.logical.plans.SetPropertiesFromMap
import org.neo4j.cypher.internal.logical.plans.SetProperty
import org.neo4j.cypher.internal.logical.plans.SetRelationshipProperties
import org.neo4j.cypher.internal.logical.plans.SetRelationshipPropertiesFromMap
import org.neo4j.cypher.internal.logical.plans.SetRelationshipProperty
import org.neo4j.cypher.internal.logical.plans.Union
import org.neo4j.cypher.internal.logical.plans.UpdatingPlan
import org.neo4j.cypher.internal.runtime.ast.PropertiesUsingCachedProperties
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.Foldable
import org.neo4j.cypher.internal.util.Foldable.SkipChildren
import org.neo4j.cypher.internal.util.Foldable.TraverseChildren
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Ref
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.StepSequencer.DefaultPostCondition
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.bottomUp
import org.neo4j.cypher.internal.util.helpers.MapSupport.PowerMap
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTRelationship

import scala.collection.mutable

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

    if (context.materializedEntitiesMode) {
      // When working with materialized entities only, caching properties is not useful.
      // Moreover, the runtime implementation of CachedProperty does not work with virtual entities.
      return from
    }

    val logicalPlan =
      // always push down property reads in a sharded properties database
      if (pushdownPropertyReads || context.config.propertyCachingMode.isRemoteBatchProperties) {
        val effectiveCardinalities = from.planningAttributes.effectiveCardinalities
        val attributes = from.planningAttributes.asAttributes(context.logicalPlanIdGen)
        PushdownPropertyReads.pushdown(
          from.logicalPlan,
          effectiveCardinalities,
          attributes,
          from.semanticTable(),
          context.config.propertyCachingMode,
          context.cancellationChecker
        )
      } else {
        from.logicalPlan
      }

    // In the first step we collect all property usages and renaming while going over the tree
    val propertyUsagesAndRenamings =
      findPropertiesInTree(
        PropertyUsagesAndRenamings.empty,
        logicalPlan,
        from.semanticTable(),
        context.cancellationChecker
      )

    // In the second step we rewrite both properties and index plans
    val (rewrittenPlan, newSemanticTable) = rewrite(logicalPlan, propertyUsagesAndRenamings, from.semanticTable())

    from
      .withMaybeLogicalPlan(Some(rewrittenPlan))
      .withSemanticTable(newSemanticTable)
  }

  override def name: String = "insertCachedProperties"

  private def findPropertiesInTree(
    initialAcc: PropertyUsagesAndRenamings,
    logicalPlan: LogicalPlan,
    semanticTable: SemanticTable,
    cancellationChecker: CancellationChecker
  ): PropertyUsagesAndRenamings = {
    // Traverses the plan tree in plan execution order
    LogicalPlans.foldPlan(initialAcc)(
      logicalPlan,
      (acc, p) => {
        val accWithProps = {
          val initialAcc = if (!p.isLeaf) acc else acc.resetUsagesCount()
          findPropertiesInPlan(initialAcc, p, semanticTable, cancellationChecker)
        }

        p match {
          // Make sure to register any renaming of variables
          case plan: ProjectingPlan =>
            val newRenamings = plan.projectExpressions.collect {
              case (key, v: Variable) if key.name != v.name => key.name -> v.name
            }
            accWithProps.addPreviousNames(newRenamings)

          // Find index plans that can provide cached properties
          case indexPlan: NodeIndexLeafPlan =>
            indexPlan.properties.filter(_.getValueFromIndex == CanGetValue).foldLeft(
              accWithProps.registerIndexedEntity(indexPlan.idName.name)
            ) {
              (innerAcc, indexedProp) =>
                innerAcc.addIndexNodeProperty(property(indexPlan.idName, indexedProp.propertyKeyToken.name))
            }
          case indexPlan: RelationshipIndexLeafPlan =>
            indexPlan.properties.filter(_.getValueFromIndex == CanGetValue).foldLeft(
              accWithProps.registerIndexedEntity(indexPlan.idName.name)
            ) {
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
              (lhs, rhs) =>
                (lhs ++ rhs).copy(
                  usages = lhs.usages ++ rhs.usages,
                  usageCount = math.max(lhs.usageCount, rhs.usageCount)
                )
            }
            val mergedEntitiesWithAllPropertiesRead =
              lhsAcc.entitiesWithAllPropertiesRead ++ rhsAcc.entitiesWithAllPropertiesRead

            PropertyUsagesAndRenamings(
              mergedProperties,
              mergedEntitiesWithAllPropertiesRead,
              mergedNames,
              Map.empty,
              indexedEntityAliases = lhsAcc.indexedEntityAliases.fuse(rhsAcc.indexedEntityAliases)(_ ++ _)
            )

          case plan =>
            val combinedChildAcc = lhsAcc ++ rhsAcc
            findPropertiesInPlan(combinedChildAcc, plan, semanticTable, cancellationChecker)
        }
    )(cancellationChecker)
  }

  private def findPropertiesInPlan(
    acc: PropertyUsagesAndRenamings,
    logicalPlan: LogicalPlan,
    semanticTable: SemanticTable,
    cancellationChecker: CancellationChecker,
    lookIn: Option[Foldable] = None
  ): PropertyUsagesAndRenamings = {
    lookIn.getOrElse(logicalPlan).folder.treeFold(acc) {
      // Don't traverse into other logical plans
      case lp: LogicalPlan if !(lp eq logicalPlan) => acc => SkipChildren(acc)

      case RestrictedCaching(plan, restrictedCaching) =>
        acc => TraverseChildren(acc.withProtectedProperties(plan, restrictedCaching))

      // Find properties
      case IsNotNull(prop @ Property(v: Variable, _)) if isNode(semanticTable, v) =>
        acc =>
          SkipChildren(acc.addNodeProperty(prop, logicalPlan, needsValue = false))
      case IsNotNull(prop @ Property(v: Variable, _)) if isRel(semanticTable, v) =>
        acc =>
          SkipChildren(acc.addRelProperty(prop, logicalPlan, needsValue = false))
      case prop @ Property(v: Variable, _) if isNode(semanticTable, v) =>
        acc =>
          TraverseChildren(acc.addNodeProperty(prop, logicalPlan, needsValue = true))
      case prop @ Property(v: Variable, _) if isRel(semanticTable, v) =>
        acc =>
          TraverseChildren(acc.addRelProperty(prop, logicalPlan, needsValue = true))
      case pr: ProduceResult =>
        acc => TraverseChildren(acc.addEntitiesWithAllPropertiesRead(pr.columns))
      case Properties(variable: LogicalVariable) =>
        acc => TraverseChildren(acc.addEntityWithAllPropertiesRead(variable))

      // New fold for nested plan expression
      case nested: NestedPlanExpression => acc =>
          val accWithNested = findPropertiesInTree(acc, nested.plan, semanticTable, cancellationChecker)
          TraverseChildren(accWithNested)

      // We don't cache all properties in case expressions to avoid the risk of reading properties that are not used.
      // Potential optimisation: Figure out properties that are shared between all case branches and cache them.
      case caseExp: CaseExpression => acc =>
          val whenExprs = caseExp.alternatives.map { case (when, _) => when }
          val accWithCase = whenExprs.foldLeft(acc) {
            case (acc, expr) => findPropertiesInPlan(acc, logicalPlan, semanticTable, cancellationChecker, Some(expr))
          }
          SkipChildren(accWithCase)
    }
  }

  private def asVariable(lv: LogicalVariable): Variable = lv match {
    case v: Variable => v
    case _           => Variable(lv.name)(lv.position)
  }

  private def cacheProperty(
    property: Property,
    prop: PropertyUsages,
    acc: PropertyUsagesAndRenamings
  ): ASTCachedProperty = {
    val v = property.map.asInstanceOf[Variable]
    val propertyKeyName = property.propertyKey
    val originalVar = acc.variableWithOriginalName(v)
    val PropertyUsages(_, _, _, entityType, firstWritingAccesses, needsValue) = prop
    // Use the original variable name for the cached property
    val knownToAccessStore = firstWritingAccesses.exists {
      case (_, properties) => properties.contains(Ref(property))
    }
    val newProperty = {
      if (needsValue) {
        CachedProperty(originalVar, v, propertyKeyName, entityType, knownToAccessStore)(property.position)
      } else {
        CachedHasProperty(originalVar, v, propertyKeyName, entityType, knownToAccessStore)(property.position)
      }
    }

    newProperty
  }

  private def rewrite(
    logicalPlan: LogicalPlan,
    acc: PropertyUsagesAndRenamings,
    semanticTable: SemanticTable
  ): (LogicalPlan, SemanticTable) = {
    var currentTypes = semanticTable.types
    val cachedPropertiesTracker = new CachedPropertiesTracker
    val rewriter = bottomUp(Rewriter.lift {

      case produceResult: ProduceResult =>
        val newColumns =
          produceResult
            .returnColumns
            .map(column =>
              cachedPropertiesTracker.get(acc.variableWithOriginalName(asVariable(column.variable))).map(cached =>
                column.copy(cachedProperties = cached)
              ).getOrElse(column)
            )

        produceResult.withNewReturnColumns(newColumns)

      case properties @ Properties(variable: LogicalVariable) =>
        cachedPropertiesTracker.get(acc.variableWithOriginalName(asVariable(variable))) match {
          case Some(cached) => PropertiesUsingCachedProperties(variable, cached)
          case None         => properties
        }

      // Rewrite properties to be cached if they are used more than once, or can be fetched from an index
      case prop @ Property(v: Variable, propertyKeyName) =>
        val originalVar = acc.variableWithOriginalName(v)
        val originalProp = acc.originalProperty(prop)
        acc.properties.get(originalProp) match {
          case Some(pu @ PropertyUsages(
              canGetFromIndex,
              usages,
              usageCount,
              entityType,
              firstWritingAccesses,
              needsValue
            ))
            if usages.contains(Ref(prop)) &&
              (usageCount > 1 || canGetFromIndex) =>
            val newProperty: ASTCachedProperty = cacheProperty(prop, pu, acc)
            cachedPropertiesTracker.addOne(originalVar, newProperty)
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
        val rewrittenIndexPlan: IndexedPropertyProvidingPlan = rewriteIndexPlan(
          acc,
          indexPlan,
          indexPlan.idName
        )
        cachedPropertiesTracker.addMany(indexPlan.idName, rewrittenIndexPlan.cachedProperties)
        rewrittenIndexPlan
      case indexPlan: RelationshipIndexLeafPlan =>
        val rewrittenIndexPlan = rewriteIndexPlan(
          acc,
          indexPlan,
          indexPlan.idName
        )
        cachedPropertiesTracker.addMany(indexPlan.idName, rewrittenIndexPlan.cachedProperties)
        rewrittenIndexPlan
    })

    val newPlan = logicalPlan.endoRewrite(rewriter)
    val newSemanticTable =
      if (currentTypes == semanticTable.types) semanticTable
      else semanticTable.copy(types = currentTypes)

    (newPlan, newSemanticTable)
  }

  private def rewriteIndexPlan(
    acc: PropertyUsagesAndRenamings,
    indexPlan: IndexedPropertyProvidingPlan,
    idName: LogicalVariable
  ) = {
    val shouldForceCache =
      acc.indexedEntityAliases.getOrElse(idName.name, Set.empty).intersect(acc.entitiesWithAllPropertiesRead).nonEmpty

    indexPlan.withMappedProperties { indexedProp =>
      val prop = property(idName, indexedProp.propertyKeyToken.name)
      acc.properties.get(prop) match {
        // Get the value since we use it later
        case Some(PropertyUsages(true, _, usageCount, _, _, _))
          // If you can't get the property from the index, `canGetFromIndex` should be false inside `PropertyUsages`.
          // However the first phase isn't entirely sound, in some cases, when there are two indexes on the same property, hence the extra check.
          if (shouldForceCache || usageCount > 0) && indexedProp.getValueFromIndex != DoNotGetValue =>
          indexedProp.copy(getValueFromIndex = GetValue)
        // We could get the value but we don't need it later
        case _ =>
          indexedProp.copy(getValueFromIndex = DoNotGetValue)
      }
    }
  }

  private def property(entity: LogicalVariable, propName: String): Property =
    Property(entity, PropertyKeyName(propName)(InputPosition.NONE))(InputPosition.NONE)

  private def isNode(semanticTable: SemanticTable, variable: Variable) =
    semanticTable.typeFor(variable.name).is(CTNode)

  private def isRel(semanticTable: SemanticTable, variable: Variable) =
    semanticTable.typeFor(variable.name).is(CTRelationship)
}

case object InsertCachedProperties extends StepSequencer.Step with DefaultPostCondition
    with PlanPipelineTransformerFactory {

  override def preConditions: Set[StepSequencer.Condition] = Set(
    // This rewriter operates on the LogicalPlan
    CompilationContains[LogicalPlan](),
    // AndedPropertyInequalities contain the same property twice, which would mess up our counts.
    AndedPropertyInequalitiesRemoved,
    // PushdownPropertyReads needs effectiveCardinalities
    CardinalityRewriter.completed,
    // There are rules to make sure not to push reads over Eager boundaries, so we need those to be
    // present before this phase.
    LogicalPlanContainsEagerIfNeeded
  )

  override def invalidatedConditions: Set[StepSequencer.Condition] = Set(
    // Rewriting logical plans introduces new IDs
    CompressPlanIDs.completed,
    // Since CachedProperties are cheaper than Properties, the previously best predicate evaluation order in a Selection
    // might not be the best order any more.
    SortPredicatesBySelectivity.completed
  )

  override def postConditions: Set[StepSequencer.Condition] =
    super.postConditions + LogicalPlanCondition(OrderedIndexPlansUseCachedProperties)

  override def getTransformer(
    pushdownPropertyReads: Boolean,
    semanticFeatures: Seq[SemanticFeature]
  ): Transformer[PlannerContext, LogicalPlanState, LogicalPlanState] = InsertCachedProperties(pushdownPropertyReads)

  /**
   * A summary of all usages of a property
   *
   * @param canGetFromIndex      if the property can be read from an index.
   * @param usages               references to all usages of this property
   * @param usageCount           effective count of usages of property. Note this is not usages.size() since
   *                             we don't want to count LHS and RHS of unions simultaneously.
   * @param entityType           the type of property
   * @param firstWritingAccesses if there can be determined a first logical plan that writes the property,
   *                             this contains that plan and all accesses that happen to this property from that plan.
   */
  private case class PropertyUsages(
    canGetFromIndex: Boolean,
    usages: Set[Ref[Property]],
    usageCount: Int,
    entityType: EntityType,
    firstWritingAccesses: Option[(LogicalPlan, Set[Ref[Property]])],
    needsValue: Boolean
  ) {

    // always prefer reading from index
    def registerIndexUsage: PropertyUsages =
      copy(canGetFromIndex = true, firstWritingAccesses = None, needsValue = true)

    def addUsage(prop: Property, accessingPlan: LogicalPlan, newNeedsValue: Boolean): PropertyUsages = {
      val fWA =
        if (canGetFromIndex) None
        else firstWritingAccesses match {
          case None                                 => Some((accessingPlan, Set(Ref(prop))))
          case Some((`accessingPlan`, otherUsages)) => Some((accessingPlan, otherUsages + Ref(prop)))
          case x                                    => x
        }

      copy(
        usages = usages + Ref(prop),
        usageCount = usageCount + 1,
        firstWritingAccesses = fWA,
        needsValue = needsValue || newNeedsValue
      )
    }

    def ++(other: PropertyUsages): PropertyUsages = PropertyUsages(
      this.canGetFromIndex || other.canGetFromIndex,
      this.usages ++ other.usages,
      this.usageCount + other.usageCount,
      this.entityType,
      this.firstWritingAccesses.orElse(other.firstWritingAccesses),
      this.needsValue || other.needsValue
    )
  }

  private object PropertyUsages {

    val NODE_NO_PROP_USAGE: PropertyUsages =
      PropertyUsages(canGetFromIndex = false, Set.empty, 0, NODE_TYPE, None, needsValue = false)

    val REL_NO_PROP_USAGE: PropertyUsages =
      PropertyUsages(canGetFromIndex = false, Set.empty, 0, RELATIONSHIP_TYPE, None, needsValue = false)
  }

  /**
   * Accumulator used when traversing the plan
   * @param properties for each encountered property, accumulated info as [[PropertyUsages]]
   * @param entitiesWithAllPropertiesRead all entities for which all properties are read (either through ValuePopulation in ProduceResults or through the properties function)
   * @param previousNames for each variable that gets projected, its previous name
   * @param protectedPropertiesByPlanId per plan id, all properties that are protected, i.e. should not be cached
   * @param indexedEntityAliases for each indexed entity, a list of later names (aliases).
   */
  private case class PropertyUsagesAndRenamings(
    properties: Map[Property, PropertyUsages],
    entitiesWithAllPropertiesRead: Set[String],
    previousNames: Map[String, String],
    protectedPropertiesByPlanId: Map[Id, ProtectedProperties],
    indexedEntityAliases: Map[String, Set[String]]
  ) {

    def ++(other: PropertyUsagesAndRenamings): PropertyUsagesAndRenamings = PropertyUsagesAndRenamings(
      this.properties.fuse(other.properties)(_ ++ _),
      this.entitiesWithAllPropertiesRead ++ other.entitiesWithAllPropertiesRead,
      this.previousNames ++ other.previousNames,
      this.protectedPropertiesByPlanId ++ other.protectedPropertiesByPlanId,
      this.indexedEntityAliases.fuse(other.indexedEntityAliases)(_ ++ _)
    )

    def withProtectedProperties(plan: LogicalPlan, protectedProps: ProtectedProperties): PropertyUsagesAndRenamings = {
      copy(protectedPropertiesByPlanId = protectedPropertiesByPlanId.updated(plan.id, protectedProps))
    }

    def addIndexNodeProperty(prop: Property): PropertyUsagesAndRenamings = {
      val previousUsages = properties.getOrElse(prop, PropertyUsages.NODE_NO_PROP_USAGE)
      val newProperties = properties.updated(prop, previousUsages.registerIndexUsage)
      copy(properties = newProperties)
    }

    def registerIndexedEntity(name: String): PropertyUsagesAndRenamings = {
      copy(indexedEntityAliases = indexedEntityAliases.updated(name, Set(name)))
    }

    def addIndexRelationshipProperty(prop: Property): PropertyUsagesAndRenamings = {
      val previousUsages = properties.getOrElse(prop, PropertyUsages.REL_NO_PROP_USAGE)
      val newProperties = properties.updated(prop, previousUsages.registerIndexUsage)
      copy(properties = newProperties)
    }

    def addNodeProperty(prop: Property, accessingPlan: LogicalPlan, needsValue: Boolean): PropertyUsagesAndRenamings = {
      ifShouldCache(accessingPlan, prop) {
        val originalProp = originalProperty(prop)
        val previousUsages: PropertyUsages = properties.getOrElse(originalProp, PropertyUsages.NODE_NO_PROP_USAGE)
        val newProperties = properties.updated(originalProp, previousUsages.addUsage(prop, accessingPlan, needsValue))
        copy(properties = newProperties)
      }
    }

    def addRelProperty(prop: Property, accessingPlan: LogicalPlan, needsValue: Boolean): PropertyUsagesAndRenamings = {
      ifShouldCache(accessingPlan, prop) {
        val originalProp = originalProperty(prop)
        val previousUsages = properties.getOrElse(originalProp, PropertyUsages.REL_NO_PROP_USAGE)
        val newProperties = properties.updated(originalProp, previousUsages.addUsage(prop, accessingPlan, needsValue))
        copy(properties = newProperties)
      }
    }

    def addEntityWithAllPropertiesRead(variable: LogicalVariable): PropertyUsagesAndRenamings = {
      copy(entitiesWithAllPropertiesRead = entitiesWithAllPropertiesRead + variable.name)
    }

    def addEntitiesWithAllPropertiesRead(variables: Iterable[LogicalVariable]): PropertyUsagesAndRenamings = {
      copy(entitiesWithAllPropertiesRead = entitiesWithAllPropertiesRead ++ variables.map(_.name))
    }

    def addPreviousNames(mappings: Map[String, String]): PropertyUsagesAndRenamings = {
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

      val newEntityAliases = mappings.foldLeft(indexedEntityAliases) {
        case (aliases, (newName, oldName)) =>
          aliases.map {
            case (entityName, aliases) if aliases.contains(oldName) =>
              (entityName, aliases + newName)
            case a => a
          }
      }

      withPreviousNames.copy(properties = renamedProperties, indexedEntityAliases = newEntityAliases)
    }

    def variableWithOriginalName(variable: Variable): Variable = {
      var name = variable.name
      while (previousNames.contains(name)) {
        name = previousNames(name)
      }
      variable.copy(name)(variable.position)
    }

    private def ifShouldCache(
      plan: LogicalPlan,
      prop: Property
    )(f: => PropertyUsagesAndRenamings): PropertyUsagesAndRenamings = {
      if (protectedProperties(plan).shouldCache(prop)) f else this
    }

    def originalProperty(prop: Property): Property = {
      val v = prop.map.asInstanceOf[Variable]
      prop.copy(variableWithOriginalName(v))(prop.position)
    }

    def resetUsagesCount(): PropertyUsagesAndRenamings =
      copy(properties = properties.view.mapValues(_.copy(usages = Set.empty, usageCount = 0)).toMap)

    def protectedProperties(plan: LogicalPlan): ProtectedProperties = {
      protectedPropertiesByPlanId.getOrElse(plan.id, CacheAll)
    }
  }

  private object PropertyUsagesAndRenamings {

    def empty: PropertyUsagesAndRenamings = PropertyUsagesAndRenamings(
      properties = Map.empty,
      entitiesWithAllPropertiesRead = Set.empty,
      previousNames = Map.empty,
      protectedPropertiesByPlanId = Map.empty,
      indexedEntityAliases = Map.empty
    )
  }
}

object RestrictedCaching {

  sealed trait ProtectedProperties {
    def shouldCache(property: Property): Boolean
  }

  private case class DontCacheProperties(protectedProperties: Set[Ref[Property]]) extends ProtectedProperties {
    override def shouldCache(property: Property): Boolean = !protectedProperties.contains(Ref(property))
  }

  private case class CombinedProtectedProperties(inner: Seq[ProtectedProperties]) extends ProtectedProperties {
    override def shouldCache(property: Property): Boolean = inner.forall(_.shouldCache(property))
  }

  case object CacheAll extends ProtectedProperties {
    def shouldCache(property: Property): Boolean = true
  }

  private def findPropertyReads(foldable: Foldable, entityName: String, propertyName: String): Seq[Property] = {
    foldable.folder.treeCollect {
      case p @ Property(LogicalVariable(`entityName`), PropertyKeyName(`propertyName`)) => p
    }
  }

  private def protectedProperties(entity: Expression, values: Seq[(String, Expression)]): ProtectedProperties = {
    entity match {
      case LogicalVariable(entityName) => protectedProperties(entityName, values)
      case _                           => CacheAll
    }
  }

  private def protectedProperties(entityName: String, values: Seq[(String, Expression)]): ProtectedProperties = {
    DontCacheProperties(values.flatMap {
      case (propertyKeyName, expr) =>
        findPropertyReads(expr, entityName, propertyKeyName).map(Ref(_))
    }.toSet)
  }

  private def byName(property: (PropertyKeyName, Expression)): (String, Expression) = property match {
    case (key, exp) => key.name -> exp
  }

  def unapply(plan: UpdatingPlan): Option[(UpdatingPlan, ProtectedProperties)] = {
    val protectedProps = plan match {
      case SetProperty(_, entity, key, value) =>
        Some(protectedProperties(entity, Seq(key.name -> value)))
      case SetProperties(_, entity, items) =>
        Some(protectedProperties(entity, items.map(byName)))
      case SetPropertiesFromMap(_, entity, value: MapExpression, _) =>
        Some(protectedProperties(entity, value.items.map(byName)))
      case _: SetPropertiesFromMap =>
        // Only protect properties when there is a direct dependency, which only happens when value is a MapExpression
        Some(CacheAll)
      case SetNodeProperty(_, entity, key, value) =>
        Some(protectedProperties(entity, Seq(key.name -> value)))
      case SetNodeProperties(_, entity, items) =>
        Some(protectedProperties(entity, items.map(byName)))
      case SetNodePropertiesFromMap(_, entity, value: MapExpression, _) =>
        Some(protectedProperties(entity, value.items.map(byName)))
      case _: SetNodePropertiesFromMap =>
        // Only protect properties when there is a direct dependency, which only happens when value is a MapExpression
        Some(CacheAll)
      case SetRelationshipProperty(_, entity, key, value) =>
        Some(protectedProperties(entity, Seq(key.name -> value)))
      case SetRelationshipProperties(_, entity, items) =>
        Some(protectedProperties(entity, items.map(byName)))
      case SetRelationshipPropertiesFromMap(_, entity, value: MapExpression, _) =>
        Some(protectedProperties(entity, value.items.map(byName)))
      case _: SetRelationshipPropertiesFromMap =>
        // Only protect properties when there is a direct dependency, which only happens when value is a MapExpression
        Some(CacheAll)
      case merge: Merge =>
        val protectedMergeProps = merge.onMatch.map {
          case SetPropertyPattern(e, key, value)             => protectedProperties(e, Seq(key.name -> value))
          case SetPropertiesPattern(e, ps)                   => protectedProperties(e, ps.map(byName))
          case SetRelationshipPropertyPattern(e, key, value) => protectedProperties(e, Seq(key.name -> value))
          case SetRelationshipPropertiesPattern(e, ps)       => protectedProperties(e, ps.map(byName))
          case SetNodePropertiesFromMapPattern(e, map: MapExpression, _) =>
            protectedProperties(e, map.items.map(byName))
          case _: SetNodePropertiesFromMapPattern => CacheAll
          case SetRelationshipPropertiesFromMapPattern(e, map: MapExpression, _) =>
            protectedProperties(e, map.items.map(byName))
          case _: SetRelationshipPropertiesFromMapPattern            => CacheAll
          case SetPropertiesFromMapPattern(e, map: MapExpression, _) => protectedProperties(e, map.items.map(byName))
          case _: SetPropertiesFromMapPattern                        => CacheAll
          case SetNodePropertyPattern(e, key, value)                 => protectedProperties(e, Seq(key.name -> value))
          case SetNodePropertiesPattern(e, ps)                       => protectedProperties(e, ps.map(byName))
          case _: SetLabelPattern                                    => CacheAll
          case _: RemoveLabelPattern                                 => CacheAll
        }
        if (protectedMergeProps.nonEmpty) Some(CombinedProtectedProperties(protectedMergeProps))
        else None
      case _: Create                 => None
      case _: DeleteExpression       => None
      case _: DeleteNode             => None
      case _: DeletePath             => None
      case _: DeleteRelationship     => None
      case _: DetachDeleteExpression => None
      case _: DetachDeleteNode       => None
      case _: DetachDeletePath       => None
      case _: Foreach                => None
      case _: RemoveLabels           => None
      case _: SetLabels              => None
    }

    protectedProps.map(plan -> _)
  }

  class CachedPropertiesTracker {
    private val cachedProperties = mutable.Map.empty[LogicalVariable, Set[ASTCachedProperty]]

    def addOne(variable: LogicalVariable, newProperty: ASTCachedProperty): Unit = {
      // If we have one property with knownToAccessStore=true and one with knownToAccessStore=false we merge into a
      // single one with knownToAccessStore=false
      val (flippedStoreAccess, newPropertyKnownToAccessStore) = newProperty match {
        case c: CachedProperty => (c.copy(knownToAccessStore = !c.knownToAccessStore)(c.position), c.knownToAccessStore)
        case c: CachedHasProperty =>
          (c.copy(knownToAccessStore = !c.knownToAccessStore)(c.position), c.knownToAccessStore)
        case _ =>
          throw new IllegalStateException(s"$newProperty should either be a CachedProperty or a CachedHasProperty")
      }
      cachedProperties.updateWith(variable)({
        case Some(existing) if existing(flippedStoreAccess) =>
          if (newPropertyKnownToAccessStore) Some(existing)
          else {
            Some((existing - flippedStoreAccess) + newProperty)
          }
        case Some(existing) => Some(existing + newProperty)
        case None           => Some(Set(newProperty))
      })
    }

    def addMany(variable: LogicalVariable, newProperties: Seq[ASTCachedProperty]): Unit = {
      newProperties.foreach(np => addOne(variable, np))
    }

    def get(variable: LogicalVariable): Option[Set[ASTCachedProperty]] = cachedProperties.get(variable)
  }
}
