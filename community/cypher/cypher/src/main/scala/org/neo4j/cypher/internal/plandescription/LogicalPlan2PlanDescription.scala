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
package org.neo4j.cypher.internal.plandescription

import org.neo4j.common.EntityType
import org.neo4j.cypher.internal.ast.ExecutableBy
import org.neo4j.cypher.internal.ast.NoOptions
import org.neo4j.cypher.internal.ast.Options
import org.neo4j.cypher.internal.ast.OptionsMap
import org.neo4j.cypher.internal.ast.OptionsParam
import org.neo4j.cypher.internal.expressions
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.FunctionName
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.LabelToken
import org.neo4j.cypher.internal.expressions.ListLiteral
import org.neo4j.cypher.internal.expressions.MapExpression
import org.neo4j.cypher.internal.expressions.NameToken
import org.neo4j.cypher.internal.expressions.Namespace
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.PropertyKeyToken
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.RelationshipTypeToken
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.functions.Labels
import org.neo4j.cypher.internal.expressions.functions.Point
import org.neo4j.cypher.internal.expressions.functions.Type
import org.neo4j.cypher.internal.frontend.PlannerName
import org.neo4j.cypher.internal.ir
import org.neo4j.cypher.internal.ir.CreateNode
import org.neo4j.cypher.internal.ir.CreatePattern
import org.neo4j.cypher.internal.ir.CreateRelationship
import org.neo4j.cypher.internal.ir.EagernessReason
import org.neo4j.cypher.internal.ir.PatternLength
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.RemoveLabelPattern
import org.neo4j.cypher.internal.ir.SetLabelPattern
import org.neo4j.cypher.internal.ir.SetNodePropertiesFromMapPattern
import org.neo4j.cypher.internal.ir.SetNodePropertyPattern
import org.neo4j.cypher.internal.ir.SetPropertiesFromMapPattern
import org.neo4j.cypher.internal.ir.SetPropertyPattern
import org.neo4j.cypher.internal.ir.SetRelationshipPropertiesFromMapPattern
import org.neo4j.cypher.internal.ir.SetRelationshipPropertyPattern
import org.neo4j.cypher.internal.ir.ShortestPathPattern
import org.neo4j.cypher.internal.ir.SimpleMutatingPattern
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.ir.VarPatternLength
import org.neo4j.cypher.internal.logical.plans
import org.neo4j.cypher.internal.logical.plans.AdministrationCommandLogicalPlan
import org.neo4j.cypher.internal.logical.plans.Aggregation
import org.neo4j.cypher.internal.logical.plans.AllNodesScan
import org.neo4j.cypher.internal.logical.plans.Anti
import org.neo4j.cypher.internal.logical.plans.AntiConditionalApply
import org.neo4j.cypher.internal.logical.plans.AntiSemiApply
import org.neo4j.cypher.internal.logical.plans.Apply
import org.neo4j.cypher.internal.logical.plans.Ascending
import org.neo4j.cypher.internal.logical.plans.AssertSameNode
import org.neo4j.cypher.internal.logical.plans.AssertingMultiNodeIndexSeek
import org.neo4j.cypher.internal.logical.plans.Bound
import org.neo4j.cypher.internal.logical.plans.CacheProperties
import org.neo4j.cypher.internal.logical.plans.CartesianProduct
import org.neo4j.cypher.internal.logical.plans.ColumnOrder
import org.neo4j.cypher.internal.logical.plans.CompositeQueryExpression
import org.neo4j.cypher.internal.logical.plans.ConditionalApply
import org.neo4j.cypher.internal.logical.plans.Create
import org.neo4j.cypher.internal.logical.plans.CreateBtreeIndex
import org.neo4j.cypher.internal.logical.plans.CreateFulltextIndex
import org.neo4j.cypher.internal.logical.plans.CreateLookupIndex
import org.neo4j.cypher.internal.logical.plans.CreateNodeKeyConstraint
import org.neo4j.cypher.internal.logical.plans.CreateNodePropertyExistenceConstraint
import org.neo4j.cypher.internal.logical.plans.CreateRangeIndex
import org.neo4j.cypher.internal.logical.plans.CreateRelationshipPropertyExistenceConstraint
import org.neo4j.cypher.internal.logical.plans.CreateTextIndex
import org.neo4j.cypher.internal.logical.plans.CreateUniquePropertyConstraint
import org.neo4j.cypher.internal.logical.plans.DeleteExpression
import org.neo4j.cypher.internal.logical.plans.DeleteNode
import org.neo4j.cypher.internal.logical.plans.DeletePath
import org.neo4j.cypher.internal.logical.plans.DeleteRelationship
import org.neo4j.cypher.internal.logical.plans.Descending
import org.neo4j.cypher.internal.logical.plans.DetachDeleteExpression
import org.neo4j.cypher.internal.logical.plans.DetachDeleteNode
import org.neo4j.cypher.internal.logical.plans.DetachDeletePath
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipByIdSeek
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipIndexContainsScan
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipIndexEndsWithScan
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipIndexScan
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipIndexSeek
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipTypeScan
import org.neo4j.cypher.internal.logical.plans.Distinct
import org.neo4j.cypher.internal.logical.plans.DoNothingIfExistsForBtreeIndex
import org.neo4j.cypher.internal.logical.plans.DoNothingIfExistsForConstraint
import org.neo4j.cypher.internal.logical.plans.DoNothingIfExistsForFulltextIndex
import org.neo4j.cypher.internal.logical.plans.DoNothingIfExistsForLookupIndex
import org.neo4j.cypher.internal.logical.plans.DoNothingIfExistsForRangeIndex
import org.neo4j.cypher.internal.logical.plans.DoNothingIfExistsForTextIndex
import org.neo4j.cypher.internal.logical.plans.DropConstraintOnName
import org.neo4j.cypher.internal.logical.plans.DropIndex
import org.neo4j.cypher.internal.logical.plans.DropIndexOnName
import org.neo4j.cypher.internal.logical.plans.DropNodeKeyConstraint
import org.neo4j.cypher.internal.logical.plans.DropNodePropertyExistenceConstraint
import org.neo4j.cypher.internal.logical.plans.DropRelationshipPropertyExistenceConstraint
import org.neo4j.cypher.internal.logical.plans.DropUniquePropertyConstraint
import org.neo4j.cypher.internal.logical.plans.Eager
import org.neo4j.cypher.internal.logical.plans.EmptyResult
import org.neo4j.cypher.internal.logical.plans.ErrorPlan
import org.neo4j.cypher.internal.logical.plans.ExhaustiveLimit
import org.neo4j.cypher.internal.logical.plans.ExistenceQueryExpression
import org.neo4j.cypher.internal.logical.plans.Expand
import org.neo4j.cypher.internal.logical.plans.ExpandAll
import org.neo4j.cypher.internal.logical.plans.ExpandInto
import org.neo4j.cypher.internal.logical.plans.FindShortestPaths
import org.neo4j.cypher.internal.logical.plans.Foreach
import org.neo4j.cypher.internal.logical.plans.ForeachApply
import org.neo4j.cypher.internal.logical.plans.InequalitySeekRangeWrapper
import org.neo4j.cypher.internal.logical.plans.Input
import org.neo4j.cypher.internal.logical.plans.LeftOuterHashJoin
import org.neo4j.cypher.internal.logical.plans.LetAntiSemiApply
import org.neo4j.cypher.internal.logical.plans.LetSelectOrAntiSemiApply
import org.neo4j.cypher.internal.logical.plans.LetSelectOrSemiApply
import org.neo4j.cypher.internal.logical.plans.LetSemiApply
import org.neo4j.cypher.internal.logical.plans.Limit
import org.neo4j.cypher.internal.logical.plans.LoadCSV
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.LogicalPlans
import org.neo4j.cypher.internal.logical.plans.ManyQueryExpression
import org.neo4j.cypher.internal.logical.plans.ManySeekableArgs
import org.neo4j.cypher.internal.logical.plans.Merge
import org.neo4j.cypher.internal.logical.plans.MultiNodeIndexSeek
import org.neo4j.cypher.internal.logical.plans.NodeByIdSeek
import org.neo4j.cypher.internal.logical.plans.NodeByLabelScan
import org.neo4j.cypher.internal.logical.plans.NodeCountFromCountStore
import org.neo4j.cypher.internal.logical.plans.NodeHashJoin
import org.neo4j.cypher.internal.logical.plans.NodeIndexContainsScan
import org.neo4j.cypher.internal.logical.plans.NodeIndexEndsWithScan
import org.neo4j.cypher.internal.logical.plans.NodeIndexScan
import org.neo4j.cypher.internal.logical.plans.NodeIndexSeek
import org.neo4j.cypher.internal.logical.plans.NodeKey
import org.neo4j.cypher.internal.logical.plans.NodeUniqueIndexSeek
import org.neo4j.cypher.internal.logical.plans.Optional
import org.neo4j.cypher.internal.logical.plans.OptionalExpand
import org.neo4j.cypher.internal.logical.plans.OrderedAggregation
import org.neo4j.cypher.internal.logical.plans.OrderedDistinct
import org.neo4j.cypher.internal.logical.plans.OrderedUnion
import org.neo4j.cypher.internal.logical.plans.PartialSort
import org.neo4j.cypher.internal.logical.plans.PartialTop
import org.neo4j.cypher.internal.logical.plans.PointDistanceRange
import org.neo4j.cypher.internal.logical.plans.PointDistanceSeekRangeWrapper
import org.neo4j.cypher.internal.logical.plans.PrefixSeekRangeWrapper
import org.neo4j.cypher.internal.logical.plans.PreserveOrder
import org.neo4j.cypher.internal.logical.plans.ProcedureCall
import org.neo4j.cypher.internal.logical.plans.ProduceResult
import org.neo4j.cypher.internal.logical.plans.ProjectEndpoints
import org.neo4j.cypher.internal.logical.plans.Projection
import org.neo4j.cypher.internal.logical.plans.PruningVarExpand
import org.neo4j.cypher.internal.logical.plans.QueryExpression
import org.neo4j.cypher.internal.logical.plans.RangeBetween
import org.neo4j.cypher.internal.logical.plans.RangeGreaterThan
import org.neo4j.cypher.internal.logical.plans.RangeLessThan
import org.neo4j.cypher.internal.logical.plans.RangeQueryExpression
import org.neo4j.cypher.internal.logical.plans.RelationshipCountFromCountStore
import org.neo4j.cypher.internal.logical.plans.RemoveLabels
import org.neo4j.cypher.internal.logical.plans.ResolvedCall
import org.neo4j.cypher.internal.logical.plans.RightOuterHashJoin
import org.neo4j.cypher.internal.logical.plans.RollUpApply
import org.neo4j.cypher.internal.logical.plans.SeekableArgs
import org.neo4j.cypher.internal.logical.plans.SelectOrAntiSemiApply
import org.neo4j.cypher.internal.logical.plans.SelectOrSemiApply
import org.neo4j.cypher.internal.logical.plans.Selection
import org.neo4j.cypher.internal.logical.plans.SemiApply
import org.neo4j.cypher.internal.logical.plans.SetLabels
import org.neo4j.cypher.internal.logical.plans.SetNodePropertiesFromMap
import org.neo4j.cypher.internal.logical.plans.SetNodeProperty
import org.neo4j.cypher.internal.logical.plans.SetPropertiesFromMap
import org.neo4j.cypher.internal.logical.plans.SetProperty
import org.neo4j.cypher.internal.logical.plans.SetRelationshipPropertiesFromMap
import org.neo4j.cypher.internal.logical.plans.SetRelationshipProperty
import org.neo4j.cypher.internal.logical.plans.ShowConstraints
import org.neo4j.cypher.internal.logical.plans.ShowFunctions
import org.neo4j.cypher.internal.logical.plans.ShowIndexes
import org.neo4j.cypher.internal.logical.plans.ShowProcedures
import org.neo4j.cypher.internal.logical.plans.SingleQueryExpression
import org.neo4j.cypher.internal.logical.plans.Skip
import org.neo4j.cypher.internal.logical.plans.Sort
import org.neo4j.cypher.internal.logical.plans.SubqueryForeach
import org.neo4j.cypher.internal.logical.plans.SystemProcedureCall
import org.neo4j.cypher.internal.logical.plans.Top
import org.neo4j.cypher.internal.logical.plans.Top1WithTies
import org.neo4j.cypher.internal.logical.plans.TriadicBuild
import org.neo4j.cypher.internal.logical.plans.TriadicFilter
import org.neo4j.cypher.internal.logical.plans.TriadicSelection
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipByIdSeek
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipIndexContainsScan
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipIndexEndsWithScan
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipIndexScan
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipIndexSeek
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipTypeScan
import org.neo4j.cypher.internal.logical.plans.Union
import org.neo4j.cypher.internal.logical.plans.Uniqueness
import org.neo4j.cypher.internal.logical.plans.UnwindCollection
import org.neo4j.cypher.internal.logical.plans.ValueHashJoin
import org.neo4j.cypher.internal.logical.plans.VarExpand
import org.neo4j.cypher.internal.logical.plans.VariablePredicate
import org.neo4j.cypher.internal.macros.AssertMacros.checkOnlyWhenAssertionsAreEnabled
import org.neo4j.cypher.internal.options.CypherVersion
import org.neo4j.cypher.internal.plandescription.Arguments.Details
import org.neo4j.cypher.internal.plandescription.Arguments.EstimatedRows
import org.neo4j.cypher.internal.plandescription.Arguments.Planner
import org.neo4j.cypher.internal.plandescription.Arguments.PlannerImpl
import org.neo4j.cypher.internal.plandescription.Arguments.PlannerVersion
import org.neo4j.cypher.internal.plandescription.Arguments.RuntimeVersion
import org.neo4j.cypher.internal.plandescription.Arguments.Version
import org.neo4j.cypher.internal.plandescription.asPrettyString.PrettyStringInterpolator
import org.neo4j.cypher.internal.plandescription.asPrettyString.PrettyStringMaker
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.EffectiveCardinalities
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.ProvidedOrders
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.exceptions.InternalException

object LogicalPlan2PlanDescription {

  def create(input: LogicalPlan,
             plannerName: PlannerName,
             cypherVersion: CypherVersion,
             readOnly: Boolean,
             effectiveCardinalities: EffectiveCardinalities,
             withRawCardinalities: Boolean,
             providedOrders: ProvidedOrders,
             runtimeOperatorMetadata: Id => Seq[Argument]): InternalPlanDescription = {
    new LogicalPlan2PlanDescription(readOnly, effectiveCardinalities, withRawCardinalities, providedOrders, runtimeOperatorMetadata)
      .create(input)
      .addArgument(Version("CYPHER " + cypherVersion.name))
      .addArgument(RuntimeVersion("4.4"))
      .addArgument(Planner(plannerName.toTextOutput))
      .addArgument(PlannerImpl(plannerName.name))
      .addArgument(PlannerVersion(plannerName.version))
  }
}

case class LogicalPlan2PlanDescription(readOnly: Boolean,
                                       effectiveCardinalities: EffectiveCardinalities,
                                       withRawCardinalities: Boolean,
                                       providedOrders: ProvidedOrders,
                                       runtimeOperatorMetadata: Id => Seq[Argument])
  extends LogicalPlans.Mapper[InternalPlanDescription] {
  private val SEPARATOR = ", "

  def create(plan: LogicalPlan): InternalPlanDescription =
    LogicalPlans.map(plan, this)

  override def onLeaf(plan: LogicalPlan): InternalPlanDescription = {
    checkOnlyWhenAssertionsAreEnabled(plan.isLeaf)

    val id = plan.id
    val variables = plan.availableSymbols.map(asPrettyString(_))

    val result: InternalPlanDescription = plan match {
      case _:AdministrationCommandLogicalPlan =>
      PlanDescriptionImpl(id, "AdministrationCommand", NoChildren, Seq.empty, Set.empty, withRawCardinalities)

      case AllNodesScan(idName, _) =>
        PlanDescriptionImpl(id, "AllNodesScan", NoChildren, Seq(Details(asPrettyString(idName))), variables, withRawCardinalities)

      case NodeByLabelScan(idName, label, _, _) =>
        val prettyDetails = pretty"${asPrettyString(idName)}:${asPrettyString(label.name)}"
        PlanDescriptionImpl(id, "NodeByLabelScan", NoChildren, Seq(Details(prettyDetails)), variables, withRawCardinalities)

      case NodeByIdSeek(idName, nodeIds: SeekableArgs, _) =>
        val prettyDetails = pretty"${asPrettyString(idName)} WHERE id(${asPrettyString(idName)}) ${seekableArgsInfo(nodeIds)}"
        PlanDescriptionImpl(id, "NodeByIdSeek", NoChildren, Seq(Details(prettyDetails)), variables, withRawCardinalities)

      case p@NodeIndexSeek(idName, label, properties, valueExpr, _, _) =>
        val (indexMode, indexDesc) = getNodeIndexDescriptions(idName, label, properties.map(_.propertyKeyToken), valueExpr, unique = false, readOnly, p.cachedProperties)
        PlanDescriptionImpl(id, indexMode, NoChildren, Seq(Details(indexDesc)), variables, withRawCardinalities)

      case p@NodeUniqueIndexSeek(idName, label, properties, valueExpr, _, _) =>
        val (indexMode, indexDesc) = getNodeIndexDescriptions(idName, label, properties.map(_.propertyKeyToken), valueExpr, unique = true, readOnly, p.cachedProperties)
        PlanDescriptionImpl(id, indexMode, NoChildren, Seq(Details(indexDesc)), variables, withRawCardinalities)

      case p@MultiNodeIndexSeek(indexLeafPlans) =>
        val (_, indexDescs) = indexLeafPlans.map(l => getNodeIndexDescriptions(l.idName, l.label, l.properties.map(_.propertyKeyToken), l.valueExpr, unique = l.isInstanceOf[NodeUniqueIndexSeek], readOnly, p.cachedProperties)).unzip
        PlanDescriptionImpl(id = plan.id, "MultiNodeIndexSeek", NoChildren, Seq(Details(indexDescs)), variables, withRawCardinalities)

      case p@AssertingMultiNodeIndexSeek(_, indexLeafPlans) =>
        val (_, indexDescs) = indexLeafPlans.map(l => getNodeIndexDescriptions(l.idName, l.label, l.properties.map(_.propertyKeyToken), l.valueExpr, unique = l.isInstanceOf[NodeUniqueIndexSeek], readOnly, p.cachedProperties)).unzip
        PlanDescriptionImpl(id = plan.id, "AssertingMultiNodeIndexSeek", NoChildren, Seq(Details(indexDescs)), variables, withRawCardinalities)
      case p@DirectedRelationshipIndexSeek(idName, start, end, typ, properties, valueExpr, _, _) =>
        val (indexMode, indexDesc) = getRelIndexDescriptions(idName, start, typ, end, isDirected = true, properties.map(_.propertyKeyToken), valueExpr, directed = true, p.cachedProperties)
        PlanDescriptionImpl(id, indexMode, NoChildren, Seq(Details(indexDesc)), variables)
      case p@UndirectedRelationshipIndexSeek(idName, start, end, typ, properties, valueExpr, _, _) =>
        val (indexMode, indexDesc) = getRelIndexDescriptions(idName, start, typ, end, isDirected = false, properties.map(_.propertyKeyToken), valueExpr, directed = false, p.cachedProperties)
        PlanDescriptionImpl(id, indexMode, NoChildren, Seq(Details(indexDesc)), variables,  withRawCardinalities)
      case p@DirectedRelationshipIndexScan(idName, start, end, typ, properties, _, _) =>
        val tokens = properties.map(_.propertyKeyToken)
        val props = tokens.map(x => asPrettyString(x.name))
        val predicates = props.map(p => pretty"$p IS NOT NULL").mkPrettyString(" AND ")
        val info = relIndexInfoString(idName, start, typ, end, isDirected = true, tokens, predicates, p.cachedProperties)
        PlanDescriptionImpl(id, "DirectedRelationshipIndexScan", NoChildren, Seq(Details(info)), variables,  withRawCardinalities)
      case p@UndirectedRelationshipIndexScan(idName, start, end, typ, properties, _, _) =>
        val tokens = properties.map(_.propertyKeyToken)
        val props = tokens.map(x => asPrettyString(x.name))
        val predicates = props.map(p => pretty"$p IS NOT NULL").mkPrettyString(" AND ")
        val info = relIndexInfoString(idName, start, typ, end, isDirected = false,  tokens, predicates, p.cachedProperties)
        PlanDescriptionImpl(id, "UndirectedRelationshipIndexScan", NoChildren, Seq(Details(info)), variables,  withRawCardinalities)
      case p@DirectedRelationshipIndexContainsScan(idName, start, end, typ, property, valueExpr, _, _) =>
        val predicate = pretty"${asPrettyString(property.propertyKeyToken.name)} CONTAINS ${asPrettyString(valueExpr)}"
        val info = relIndexInfoString(idName, start, typ, end, isDirected = true, Seq(property.propertyKeyToken), predicate, p.cachedProperties)
        PlanDescriptionImpl(id, "DirectedRelationshipIndexContainsScan", NoChildren, Seq(Details(info)), variables)
      case p@UndirectedRelationshipIndexContainsScan(idName, start, end, typ, property, valueExpr, _, _) =>
        val predicate = pretty"${asPrettyString(property.propertyKeyToken.name)} CONTAINS ${asPrettyString(valueExpr)}"
        val info = relIndexInfoString(idName, start, typ, end, isDirected = false, Seq(property.propertyKeyToken), predicate, p.cachedProperties)
        PlanDescriptionImpl(id, "UndirectedRelationshipIndexContainsScan", NoChildren, Seq(Details(info)), variables,  withRawCardinalities)
      case p@DirectedRelationshipIndexEndsWithScan(idName, start, end, typ, property, valueExpr, _, _) =>
        val predicate = pretty"${asPrettyString(property.propertyKeyToken.name)} ENDS WITH ${asPrettyString(valueExpr)}"
        val info = relIndexInfoString(idName, start, typ, end, isDirected = true, Seq(property.propertyKeyToken), predicate, p.cachedProperties)
        PlanDescriptionImpl(id, "DirectedRelationshipIndexEndsWithScan", NoChildren, Seq(Details(info)), variables,  withRawCardinalities)
      case p@UndirectedRelationshipIndexEndsWithScan(idName, start, end, typ, property, valueExpr, _, _) =>
        val predicate = pretty"${asPrettyString(property.propertyKeyToken.name)} ENDS WITH ${asPrettyString(valueExpr)}"
        val info = relIndexInfoString(idName, start, typ, end, isDirected = false, Seq(property.propertyKeyToken), predicate, p.cachedProperties)
        PlanDescriptionImpl(id, "UndirectedRelationshipIndexEndsWithScan", NoChildren, Seq(Details(info)), variables,  withRawCardinalities)

      case plans.Argument(argumentIds) if argumentIds.nonEmpty =>
        val details = if (argumentIds.nonEmpty) Seq(Details(argumentIds.map(asPrettyString(_)).mkPrettyString(SEPARATOR))) else Seq.empty
        PlanDescriptionImpl(id, "Argument", NoChildren, details, variables, withRawCardinalities)

      case _: plans.Argument =>
        ArgumentPlanDescription(id, Seq.empty, variables)

      case DirectedRelationshipByIdSeek(idName, relIds, startNode, endNode, _) =>
        val details = Details(relationshipByIdSeekInfo(idName, relIds, startNode, endNode, true))
        PlanDescriptionImpl(id, "DirectedRelationshipByIdSeek", NoChildren, Seq(details), variables, withRawCardinalities)

      case UndirectedRelationshipByIdSeek(idName, relIds, startNode, endNode, _) =>
        val details = Details(relationshipByIdSeekInfo(idName, relIds, startNode, endNode, false))
        PlanDescriptionImpl(id, "UndirectedRelationshipByIdSeek", NoChildren, Seq(details), variables, withRawCardinalities)

      case DirectedRelationshipTypeScan(idName, start, typeName, end, _, _) =>
        val prettyDetails = pretty"(${asPrettyString(start)})-[${asPrettyString(idName)}:${asPrettyString(typeName.name)}]->(${asPrettyString(end)})"
        PlanDescriptionImpl(id, "DirectedRelationshipTypeScan", NoChildren, Seq(Details(prettyDetails)), variables, withRawCardinalities)

      case UndirectedRelationshipTypeScan(idName, start, typeName, end, _, _) =>
        val prettyDetails = pretty"(${asPrettyString(start)})-[${asPrettyString(idName)}:${asPrettyString(typeName.name)}]-(${asPrettyString(end)})"
        PlanDescriptionImpl(id, "UndirectedRelationshipTypeScan", NoChildren, Seq(Details(prettyDetails)), variables, withRawCardinalities)

      case Input(nodes, rels, inputVars, _) =>
        PlanDescriptionImpl(id, "Input", NoChildren, Seq(Details((nodes ++ rels ++ inputVars).map(asPrettyString(_)))), variables, withRawCardinalities)

      case NodeCountFromCountStore(ident, labelNames, _) =>
        val info = nodeCountFromCountStoreInfo(ident, labelNames)
        PlanDescriptionImpl(id, "NodeCountFromCountStore", NoChildren, Seq(Details(info)), variables, withRawCardinalities)

      case p@NodeIndexContainsScan(idName, label, property, valueExpr, _, _) =>
        val predicate = pretty"${asPrettyString(property.propertyKeyToken.name)} CONTAINS ${asPrettyString(valueExpr)}"
        val info = nodeIndexInfoString(idName, unique = false, label, Seq(property.propertyKeyToken), predicate, p.cachedProperties)
        PlanDescriptionImpl(id, "NodeIndexContainsScan", NoChildren, Seq(Details(info)), variables, withRawCardinalities)

      case p@NodeIndexEndsWithScan(idName, label, property, valueExpr, _, _) =>
        val predicate = pretty"${asPrettyString(property.propertyKeyToken.name)} ENDS WITH ${asPrettyString(valueExpr)}"
        val info = nodeIndexInfoString(idName, unique = false, label, Seq(property.propertyKeyToken), predicate, p.cachedProperties)
        PlanDescriptionImpl(id, "NodeIndexEndsWithScan", NoChildren, Seq(Details(info)), variables, withRawCardinalities)

      case p@NodeIndexScan(idName, label, properties, _, _) =>
        val tokens = properties.map(_.propertyKeyToken)
        val props = tokens.map(x => asPrettyString(x.name))
        val predicates = props.map(p => pretty"$p IS NOT NULL").mkPrettyString(" AND ")
        val info = nodeIndexInfoString(idName, unique = false, label, tokens, predicates, p.cachedProperties)
        PlanDescriptionImpl(id, "NodeIndexScan", NoChildren, Seq(Details(info)), variables, withRawCardinalities)

      case ProcedureCall(_, call) =>
        PlanDescriptionImpl(id, "ProcedureCall", NoChildren, Seq(Details(signatureInfo(call))), variables, withRawCardinalities)

      case RelationshipCountFromCountStore(ident, startLabel, typeNames, endLabel, _) =>
        val info = relationshipCountFromCountStoreInfo(ident, startLabel, typeNames, endLabel)
        PlanDescriptionImpl(id, "RelationshipCountFromCountStore", NoChildren, Seq(Details(info)), variables, withRawCardinalities)

      case DoNothingIfExistsForBtreeIndex(entityName, propertyKeyNames, nameOption) =>
        PlanDescriptionImpl(id, s"DoNothingIfExists(INDEX)", NoChildren, Seq(Details(btreeIndexInfo(nameOption, entityName, propertyKeyNames, NoOptions))), variables, withRawCardinalities)

      case DoNothingIfExistsForRangeIndex(entityName, propertyKeyNames, nameOption) =>
        PlanDescriptionImpl(id, s"DoNothingIfExists(INDEX)", NoChildren, Seq(Details(rangeIndexInfo(nameOption, entityName, propertyKeyNames, NoOptions))), variables, withRawCardinalities)

      case DoNothingIfExistsForLookupIndex(entityType, nameOption) =>
        PlanDescriptionImpl(id, s"DoNothingIfExists(INDEX)", NoChildren, Seq(Details(lookupIndexInfo(nameOption, entityType, NoOptions))), variables, withRawCardinalities)

      case DoNothingIfExistsForFulltextIndex(entityNames, propertyKeyNames, nameOption) =>
        PlanDescriptionImpl(id, s"DoNothingIfExists(INDEX)", NoChildren, Seq(Details(fulltextIndexInfo(nameOption, entityNames, propertyKeyNames, NoOptions))), variables, withRawCardinalities)

      case DoNothingIfExistsForTextIndex(entityName, propertyKeyNames, nameOption) =>
        PlanDescriptionImpl(id, s"DoNothingIfExists(INDEX)", NoChildren, Seq(Details(textIndexInfo(nameOption, entityName, propertyKeyNames, NoOptions))), variables, withRawCardinalities)

      case CreateBtreeIndex(_, entityName, propertyKeyNames, nameOption, options) => // Can be both a leaf plan and a middle plan so need to be in both places
        PlanDescriptionImpl(id, "CreateIndex", NoChildren, Seq(Details(btreeIndexInfo(nameOption, entityName, propertyKeyNames, options))), variables, withRawCardinalities)

      case CreateRangeIndex(_, entityName, propertyKeyNames, nameOption, options) => // Can be both a leaf plan and a middle plan so need to be in both places
        PlanDescriptionImpl(id, "CreateIndex", NoChildren, Seq(Details(rangeIndexInfo(nameOption, entityName, propertyKeyNames, options))), variables, withRawCardinalities)

      case CreateLookupIndex(_, entityType, nameOption, options) => // Can be both a leaf plan and a middle plan so need to be in both places
        PlanDescriptionImpl(id, "CreateIndex", NoChildren, Seq(Details(lookupIndexInfo(nameOption, entityType, options))), variables, withRawCardinalities)

      case CreateFulltextIndex(_, entityNames, propertyKeyNames, nameOption, options) => // Can be both a leaf plan and a middle plan so need to be in both places
        PlanDescriptionImpl(id, "CreateIndex", NoChildren, Seq(Details(fulltextIndexInfo(nameOption, entityNames, propertyKeyNames, options))), variables, withRawCardinalities)

      case CreateTextIndex(_, entityName, propertyKeyNames, nameOption, options) => // Can be both a leaf plan and a middle plan so need to be in both places
        PlanDescriptionImpl(id, "CreateIndex", NoChildren, Seq(Details(textIndexInfo(nameOption, entityName, propertyKeyNames, options))), variables, withRawCardinalities)

      case DropIndex(labelName, propertyKeyNames) =>
        PlanDescriptionImpl(id, "DropIndex", NoChildren, Seq(Details(btreeIndexInfo(None, Left(labelName), propertyKeyNames, NoOptions))), variables, withRawCardinalities)

      case DropIndexOnName(name, _) =>
        PlanDescriptionImpl(id, "DropIndex", NoChildren, Seq(Details(pretty"INDEX ${asPrettyString(name)}")), variables, withRawCardinalities)

      case ShowIndexes(indexType, verbose, _) =>
        val typeDescription = asPrettyString.raw(indexType.description)
        val colsDescription = if (verbose) pretty"allColumns" else pretty"defaultColumns"
        PlanDescriptionImpl(id, "ShowIndexes", NoChildren, Seq(Details(pretty"$typeDescription, $colsDescription")), variables, withRawCardinalities)

      case DoNothingIfExistsForConstraint(entity, entityName, props, assertion, name) =>
        val a = assertion match {
          case NodeKey    => scala.util.Right("IS NODE KEY")
          case Uniqueness => scala.util.Right("IS UNIQUE")
          case _          => scala.util.Right("IS NOT NULL")
        }
        PlanDescriptionImpl(id, s"DoNothingIfExists(CONSTRAINT)", NoChildren, Seq(Details(constraintInfo(name, entity, entityName, props, a))), variables, withRawCardinalities)

      case CreateUniquePropertyConstraint(_, node, label, properties: Seq[Property], nameOption, options) => // Can be both a leaf plan and a middle plan so need to be in both places
        val details = Details(constraintInfo(nameOption, node, scala.util.Left(label), properties, scala.util.Right("IS UNIQUE"), options))
        PlanDescriptionImpl(id, "CreateConstraint", NoChildren, Seq(details), variables, withRawCardinalities)

      case CreateNodeKeyConstraint(_, node, label, properties: Seq[Property], nameOption, options) => // Can be both a leaf plan and a middle plan so need to be in both places
        val details = Details(constraintInfo(nameOption, node, scala.util.Left(label), properties, scala.util.Right("IS NODE KEY"), options))
        PlanDescriptionImpl(id, "CreateConstraint", NoChildren, Seq(details), variables, withRawCardinalities)

      case CreateNodePropertyExistenceConstraint(_, label, prop, nameOption, options) => // Can be both a leaf plan and a middle plan so need to be in both places
        val node = prop.map.asCanonicalStringVal
        val details = Details(constraintInfo(nameOption, node, scala.util.Left(label), Seq(prop), scala.util.Right("IS NOT NULL"), options))
        PlanDescriptionImpl(id, "CreateConstraint", NoChildren, Seq(details), variables, withRawCardinalities)

      case CreateRelationshipPropertyExistenceConstraint(_, relTypeName, prop, nameOption, options) => // Can be both a leaf plan and a middle plan so need to be in both places
        val relationship = prop.map.asCanonicalStringVal
        val details = Details(constraintInfo(nameOption, relationship, scala.util.Right(relTypeName), Seq(prop), scala.util.Right("IS NOT NULL"), options))
        PlanDescriptionImpl(id, "CreateConstraint", NoChildren, Seq(details), variables, withRawCardinalities)

      case DropUniquePropertyConstraint(label, props) =>
        val node = props.head.map.asCanonicalStringVal
        val details = Details(constraintInfo(None, node, scala.util.Left(label), props, scala.util.Right("IS UNIQUE"), useForAndRequire = false))
        PlanDescriptionImpl(id, "DropConstraint", NoChildren, Seq(details), variables, withRawCardinalities)

      case DropNodeKeyConstraint(label, props) =>
        val node = props.head.map.asCanonicalStringVal
        val details = Details(constraintInfo(None, node, scala.util.Left(label), props, scala.util.Right("IS NODE KEY"), useForAndRequire = false))
        PlanDescriptionImpl(id, "DropConstraint", NoChildren, Seq(details), variables, withRawCardinalities)

      case DropNodePropertyExistenceConstraint(label, prop) =>
        val node = prop.map.asCanonicalStringVal
        val details = Details(constraintInfo(None, node, scala.util.Left(label), Seq(prop), scala.util.Left("exists"), useForAndRequire = false))
        PlanDescriptionImpl(id, "DropConstraint", NoChildren, Seq(details), variables, withRawCardinalities)

      case DropRelationshipPropertyExistenceConstraint(relTypeName, prop) =>
        val relationship = prop.map.asCanonicalStringVal
        val details = Details(constraintInfo(None, relationship,scala.util.Right(relTypeName), Seq(prop), scala.util.Left("exists"), useForAndRequire = false))
        PlanDescriptionImpl(id, "DropConstraint", NoChildren, Seq(details), variables, withRawCardinalities)

      case DropConstraintOnName(name, _) =>
        val constraintName = Details(pretty"CONSTRAINT ${asPrettyString(name)}")
        PlanDescriptionImpl(id, "DropConstraint", NoChildren, Seq(constraintName), variables, withRawCardinalities)

      case ShowConstraints(constraintType, verbose, _) =>
        val typeDescription = asPrettyString.raw(constraintType.description)
        val colsDescription = if (verbose) pretty"allColumns" else pretty"defaultColumns"
        PlanDescriptionImpl(id, "ShowConstraints", NoChildren, Seq(Details(pretty"$typeDescription, $colsDescription")), variables, withRawCardinalities)

      case s: ShowProcedures =>
        val executableDescription = s.executableBy.map(e => asPrettyString.raw(e.description("procedures"))).getOrElse(asPrettyString.raw(ExecutableBy.defaultDescription("procedures")))
        val colsDescription = if (s.verbose) pretty"allColumns" else pretty"defaultColumns"
        PlanDescriptionImpl(id, "ShowProcedures", NoChildren, Seq(Details(pretty"$executableDescription, $colsDescription")), variables, withRawCardinalities)

      case s: ShowFunctions =>
        val typeDescription = asPrettyString.raw(s.functionType.description)
        val executableDescription = s.executableBy.map(e => asPrettyString.raw(e.description("functions"))).getOrElse(pretty"functionsForUser(all)")
        val colsDescription = if (s.verbose) pretty"allColumns" else pretty"defaultColumns"
        PlanDescriptionImpl(id, "ShowFunctions", NoChildren, Seq(Details(pretty"$typeDescription, $executableDescription, $colsDescription")), variables, withRawCardinalities)

      case SystemProcedureCall(procedureName, _, _, _, _) =>
        PlanDescriptionImpl(id, procedureName, NoChildren, Seq.empty, variables, withRawCardinalities)

      case x => throw new InternalException(s"Unknown plan type: ${x.getClass.getSimpleName}. Missing a case?")
    }

    addRuntimeAttributes(addPlanningAttributes(result, plan), plan)
  }

  override def onOneChildPlan(plan: LogicalPlan, source: InternalPlanDescription): InternalPlanDescription = {
    checkOnlyWhenAssertionsAreEnabled(plan.lhs.nonEmpty)
    checkOnlyWhenAssertionsAreEnabled(plan.rhs.isEmpty)

    val id = plan.id
    val variables = plan.availableSymbols.map(asPrettyString(_))
    val children = if (source.isInstanceOf[ArgumentPlanDescription]) NoChildren else SingleChild(source)

    val result: InternalPlanDescription = plan match {
      case _:AdministrationCommandLogicalPlan =>
        PlanDescriptionImpl(id, "AdministrationCommand", NoChildren, Seq.empty, Set.empty, withRawCardinalities)

      case Distinct(_, groupingExpressions) =>
        PlanDescriptionImpl(id, "Distinct", children, Seq(Details(aggregationInfo(groupingExpressions, Map.empty))), variables, withRawCardinalities)

      case OrderedDistinct(_, groupingExpressions, orderToLeverage) =>
        val details = aggregationInfo(groupingExpressions, Map.empty, orderToLeverage)
        PlanDescriptionImpl(id, "OrderedDistinct", children, Seq(Details(details)), variables, withRawCardinalities)

      case Aggregation(_, groupingExpressions, aggregationExpressions) =>
        val details = aggregationInfo(groupingExpressions, aggregationExpressions)
        PlanDescriptionImpl(id, "EagerAggregation", children, Seq(Details(details)), variables, withRawCardinalities)

      case OrderedAggregation(_, groupingExpressions, aggregationExpressions, orderToLeverage) =>
        val details = aggregationInfo(groupingExpressions, aggregationExpressions, orderToLeverage)
        PlanDescriptionImpl(id, "OrderedAggregation", children, Seq(Details(details)), variables, withRawCardinalities)

      case Create(_, nodes, relationships) =>
        val relationshipDetails = relationships.map {
            case CreateRelationship(idName, leftNode, relType, rightNode, direction, properties) =>
              expandExpressionDescription(leftNode, Some(idName), Seq(relType.name), rightNode, direction, 1, Some(1), properties)
          }
        val nodeDetails = nodes.map(createNodeDescription)
        PlanDescriptionImpl(id, "Create", children, Seq(Details(nodeDetails ++ relationshipDetails)), variables, withRawCardinalities)

      case DeleteExpression(_, expression) =>
        PlanDescriptionImpl(id, "Delete", children, Seq(Details(asPrettyString(expression))), variables, withRawCardinalities)

      case DeleteNode(_, expression) =>
        PlanDescriptionImpl(id, "Delete", children, Seq(Details(asPrettyString(expression))), variables, withRawCardinalities)

      case DeletePath(_, expression) =>
        PlanDescriptionImpl(id, "Delete", children, Seq(Details(asPrettyString(expression))), variables, withRawCardinalities)

      case DeleteRelationship(_, expression) =>
        PlanDescriptionImpl(id, "Delete", children, Seq(Details(asPrettyString(expression))), variables, withRawCardinalities)

      case DetachDeleteExpression(_, expression) =>
        PlanDescriptionImpl(id, "DetachDelete", children, Seq(Details(asPrettyString(expression))), variables, withRawCardinalities)

      case DetachDeleteNode(_, expression) =>
        PlanDescriptionImpl(id, "DetachDelete", children, Seq(Details(asPrettyString(expression))), variables, withRawCardinalities)

      case DetachDeletePath(_, expression) =>
        PlanDescriptionImpl(id, "DetachDelete", children, Seq(Details(asPrettyString(expression))), variables, withRawCardinalities)

      case Eager(_, reasons) =>
        val info = eagernessReasonInfo(reasons)
        val details = if (info.nonEmpty) Seq(Details(info)) else Seq.empty
        PlanDescriptionImpl(id, "Eager", children, details, variables, withRawCardinalities)

      case _: EmptyResult =>
        PlanDescriptionImpl(id, "EmptyResult", children, Seq.empty, variables, withRawCardinalities)

      case NodeCountFromCountStore(idName, labelName, _) =>
        val info = nodeCountFromCountStoreInfo(idName, labelName)
        PlanDescriptionImpl(id, "NodeCountFromCountStore", NoChildren, Seq(Details(info)), variables, withRawCardinalities)

      case RelationshipCountFromCountStore(idName, start, types, end, _) =>
        val info = relationshipCountFromCountStoreInfo(idName, start, types, end)
        PlanDescriptionImpl(id, "RelationshipCountFromCountStore", NoChildren, Seq(Details(info)), variables, withRawCardinalities)

      case _: ErrorPlan =>
        PlanDescriptionImpl(id, "Error", children, Seq.empty, variables, withRawCardinalities)

      case Expand(_, fromName, dir, typeNames, toName, relName, mode) =>
        val expression = Details(expandExpressionDescription(fromName, Some(relName), typeNames.map(_.name), toName, dir, 1, Some(1), None))
        val modeText = mode match {
          case ExpandAll => "Expand(All)"
          case ExpandInto => "Expand(Into)"
        }
        PlanDescriptionImpl(id, modeText, children, Seq(expression), variables, withRawCardinalities)

      case Limit(_, count) =>
        PlanDescriptionImpl(id, "Limit", children, Seq(Details(asPrettyString(count))), variables, withRawCardinalities)

      case ExhaustiveLimit(_, count) =>
        PlanDescriptionImpl(id, "ExhaustiveLimit", children, Seq(Details(asPrettyString(count))), variables, withRawCardinalities)

      case CacheProperties(_, properties) =>
        PlanDescriptionImpl(id, "CacheProperties", children, Seq(Details(properties.toSeq.map(asPrettyString(_)))), variables, withRawCardinalities)

      case OptionalExpand(_, fromName, dir, typeNames, toName, relName, mode, predicates) =>
        val predicate = predicates.map(p => pretty" WHERE ${asPrettyString(p)}").getOrElse(pretty"")
        val expandExpressionDesc = expandExpressionDescription(fromName, Some(relName), typeNames.map(_.name), toName, dir, 1, Some(1), None)
        val details = Details(pretty"$expandExpressionDesc$predicate")
        val modeText = mode match {
          case ExpandAll => "OptionalExpand(All)"
          case ExpandInto => "OptionalExpand(Into)"
        }
        PlanDescriptionImpl(id, modeText, children, Seq(details), variables, withRawCardinalities)

      case ProduceResult(_, columns) =>
        PlanDescriptionImpl(id, "ProduceResults", children, Seq(Details(columns.map(asPrettyString(_)))), variables, withRawCardinalities)

      case Projection(_, expr) =>
        val expressions = Details(projectedExpressionInfo(expr))
        PlanDescriptionImpl(id, "Projection", children, Seq(expressions), variables, withRawCardinalities)

      case Selection(predicate, _) =>
        val details = Details(asPrettyString(predicate))
        PlanDescriptionImpl(id, "Filter", children, Seq(details), variables, withRawCardinalities)

      case Skip(_, count) =>
        PlanDescriptionImpl(id, name = "Skip", children, Seq(Details(asPrettyString(count))), variables, withRawCardinalities)

      case FindShortestPaths(_, ShortestPathPattern(maybePathName, PatternRelationship(relName, (fromName, toName), dir, relTypes, patternLength: PatternLength), isSingle), predicates, _, _) =>
        val patternRelationshipInfo = expandExpressionDescription(fromName, Some(relName), relTypes.map(_.name), toName, dir, patternLength)

        val predicatesInfo = if (predicates.isEmpty) {
          pretty""
        } else {
          pretty" WHERE ${predicates.map(asPrettyString(_)).mkPrettyString(" AND ")}"
        }

        val pathName = maybePathName match {
          case Some(p) => pretty"${asPrettyString(p)} = "
          case _ => pretty""
        }

        PlanDescriptionImpl(id, "ShortestPath", children, Seq(Details(pretty"$pathName$patternRelationshipInfo$predicatesInfo")), variables, withRawCardinalities)

      case LoadCSV(_, _, variableName, _, _, _, _) =>
        PlanDescriptionImpl(id, "LoadCSV", children, Seq(Details(asPrettyString(variableName))), variables, withRawCardinalities)

      case Merge(_, createNodes, createRelationships, onMatch, onCreate, nodesToLock) =>
        val createNodesPretty = createNodes.map(createNodeDescription)
        val createRelsPretty = createRelationships.map {
          case CreateRelationship(relationship, startNode, typ, endNode, direction, properties) =>
            expandExpressionDescription(startNode, Some(relationship), Seq(typ.name), endNode, direction, 1, Some(1), properties)
        }
        val details: Seq[PrettyString] = Seq(pretty"CREATE ${(createNodesPretty ++ createRelsPretty).mkPrettyString(", ")}") ++
          (if (onMatch.nonEmpty) Seq(pretty"ON MATCH ${onMatch.map(mutatingPatternString).mkPrettyString(", ")}") else Seq.empty) ++
          (if (onCreate.nonEmpty) Seq(pretty"ON CREATE ${onCreate.map(mutatingPatternString).mkPrettyString(", ")}") else Seq.empty) ++
          (if (nodesToLock.nonEmpty) Seq(pretty"LOCK(${keyNamesInfo(nodesToLock.toSeq)})") else Seq.empty)

        val name = if(nodesToLock.isEmpty) "Merge" else "LockingMerge"
        PlanDescriptionImpl(id, name, children, Seq(Details(details)), variables, withRawCardinalities)

      case Optional(_, protectedSymbols) =>
        PlanDescriptionImpl(id, "Optional", children, Seq(Details(keyNamesInfo(protectedSymbols.toSeq))), variables, withRawCardinalities)

      case _: Anti =>
        PlanDescriptionImpl(id, "Anti", children, Seq.empty, variables, withRawCardinalities)

      case ProcedureCall(_, call) =>
        PlanDescriptionImpl(id, "ProcedureCall", children, Seq(Details(signatureInfo(call))), variables, withRawCardinalities)

      case ProjectEndpoints(_, relName, start, _, end, _, relTypes, directed, patternLength) =>
        val name = if (directed) "ProjectEndpoints" else "ProjectEndpoints(BOTH)"
        val direction = if (directed) SemanticDirection.OUTGOING else SemanticDirection.BOTH
        val relTypeNames = relTypes.toSeq.flatten.map(_.name)
        val details = expandExpressionDescription(start, Some(relName), relTypeNames, end, direction, patternLength)
        PlanDescriptionImpl(id, name, children, Seq(Details(details)), variables, withRawCardinalities)

      case PruningVarExpand(_, fromName, dir, types, toName, min, max, maybeNodePredicate, maybeRelationshipPredicate) =>
        val maybeRelName = maybeRelationshipPredicate.map(_.variable.name)
        val expandInfo = expandExpressionDescription(fromName, maybeRelName, types.map(_.name), toName, dir, minLength = min, maxLength = Some(max), maybeProperties = None)
        val predicatesDescription = buildPredicatesDescription(maybeNodePredicate, maybeRelationshipPredicate) match {
          case Some(predicateInfo) => pretty" WHERE $predicateInfo"
          case _ => pretty""
        }
        PlanDescriptionImpl(id, s"VarLengthExpand(Pruning)", children, Seq(Details(pretty"$expandInfo$predicatesDescription")), variables, withRawCardinalities)

      case RemoveLabels(_, idName, labelNames) =>
        val prettyId = asPrettyString(idName)
        val prettyLabels = labelNames.map(labelName => asPrettyString(labelName.name)).mkPrettyString(":", ":", "")
        val details = Details(pretty"$prettyId$prettyLabels")
        PlanDescriptionImpl(id, "RemoveLabels", children, Seq(details), variables, withRawCardinalities)

      case SetLabels(_, idName, labelNames) =>

        val prettyId = asPrettyString(idName)
        val prettyLabels = labelNames.map(labelName => asPrettyString(labelName.name)).mkPrettyString(":", ":", "")
        val details = Details(pretty"$prettyId$prettyLabels")
        PlanDescriptionImpl(id, "SetLabels", children, Seq(details), variables, withRawCardinalities)

      case SetNodePropertiesFromMap(_, idName, expression, removeOtherProps) =>
        val details = Details(setPropertyInfo(asPrettyString(idName), expression, removeOtherProps))
        PlanDescriptionImpl(id, "SetNodePropertiesFromMap", children, Seq(details), variables, withRawCardinalities)

      case SetPropertiesFromMap(_, entity, expression, removeOtherProps) =>
        val details = Details(setPropertyInfo(asPrettyString(entity), expression, removeOtherProps))
        PlanDescriptionImpl(id, "SetPropertiesFromMap", children, Seq(details), variables, withRawCardinalities)

      case SetProperty(_, entity, propertyKey, expression) =>
        val entityString = pretty"${asPrettyString(entity)}.${asPrettyString(propertyKey.name)}"
        val details = Details(setPropertyInfo(entityString, expression, true))
        PlanDescriptionImpl(id, "SetProperty", children, Seq(details), variables, withRawCardinalities)

      case SetNodeProperty(_, idName, propertyKey, expression) =>
        val details = Details(setPropertyInfo(pretty"${asPrettyString(idName)}.${asPrettyString(propertyKey.name)}", expression, true))
        PlanDescriptionImpl(id, "SetProperty", children, Seq(details), variables, withRawCardinalities)

      case SetRelationshipProperty(_, idName, propertyKey, expression) =>
        val details = Details(setPropertyInfo(pretty"${asPrettyString(idName)}.${asPrettyString(propertyKey.name)}", expression, true))
        PlanDescriptionImpl(id, "SetProperty", children, Seq(details), variables, withRawCardinalities)

      case SetRelationshipPropertiesFromMap(_, idName, expression, removeOtherProps) =>
        val details = Details(setPropertyInfo(asPrettyString(idName), expression, removeOtherProps))
        PlanDescriptionImpl(id, "SetRelationshipPropertiesFromMap", children, Seq(details), variables, withRawCardinalities)

      case Sort(_, orderBy) =>
        PlanDescriptionImpl(id, "Sort", children, Seq(Details(orderInfo(orderBy))), variables, withRawCardinalities)

      case PartialSort(_, alreadySortedPrefix, stillToSortSuffix, _) =>
        PlanDescriptionImpl(id, "PartialSort", children, Seq(Details(orderInfo(alreadySortedPrefix ++ stillToSortSuffix))), variables, withRawCardinalities)

      case Top(_, orderBy, limit) =>
        val details = pretty"${orderInfo(orderBy)} LIMIT ${asPrettyString(limit)}"
        PlanDescriptionImpl(id, "Top", children, Seq(Details(details)), variables, withRawCardinalities)

      case Top1WithTies(_, orderBy) =>
        val details = pretty"${orderInfo(orderBy)}"
        PlanDescriptionImpl(id, "Top1WithTies", children, Seq(Details(details)), variables, withRawCardinalities)

      case PartialTop(_, alreadySortedPrefix, stillToSortSuffix, limit, _) =>
        val details = pretty"${orderInfo(alreadySortedPrefix ++ stillToSortSuffix)} LIMIT ${asPrettyString(limit)}"
        PlanDescriptionImpl(id, "PartialTop", children, Seq(Details(details)), variables, withRawCardinalities)

      case UnwindCollection(_, variable, expression) =>
        val details = Details(projectedExpressionInfo(Map(variable -> expression)).mkPrettyString(SEPARATOR))
        PlanDescriptionImpl(id, "Unwind", children, Seq(details), variables, withRawCardinalities)

      case VarExpand(_, fromName, dir, _, types, toName, relName, length, mode, maybeNodePredicate, maybeRelationshipPredicate) =>
        val expandDescription = expandExpressionDescription(fromName, Some(relName), types.map(_.name), toName, dir, minLength = length.min, maxLength = length.max, maybeProperties = None)
        val predicatesDescription = buildPredicatesDescription(maybeNodePredicate, maybeRelationshipPredicate) match {
          case Some(predicateInfo) => pretty" WHERE $predicateInfo"
          case _ => pretty""
        }
        val modeDescr = mode match {
          case ExpandAll => "All"
          case ExpandInto => "Into"
        }
        PlanDescriptionImpl(id, s"VarLengthExpand($modeDescr)", children, Seq(Details(pretty"$expandDescription$predicatesDescription")), variables, withRawCardinalities)

      case CreateBtreeIndex(_, entityName, propertyKeyNames, nameOption, options) => // Can be both a leaf plan and a middle plan so need to be in both places
        PlanDescriptionImpl(id, "CreateIndex", children, Seq(Details(btreeIndexInfo(nameOption, entityName, propertyKeyNames, options))), variables, withRawCardinalities)

      case CreateRangeIndex(_, entityName, propertyKeyNames, nameOption, options) => // Can be both a leaf plan and a middle plan so need to be in both places
        PlanDescriptionImpl(id, "CreateIndex", children, Seq(Details(rangeIndexInfo(nameOption, entityName, propertyKeyNames, options))), variables, withRawCardinalities)

      case CreateLookupIndex(_, isNodeIndex, nameOption, options) => // Can be both a leaf plan and a middle plan so need to be in both places
        PlanDescriptionImpl(id, "CreateIndex", children, Seq(Details(lookupIndexInfo(nameOption, isNodeIndex, options))), variables, withRawCardinalities)

      case CreateFulltextIndex(_, entityNames, propertyKeyNames, nameOption, options) => // Can be both a leaf plan and a middle plan so need to be in both places
        PlanDescriptionImpl(id, "CreateIndex", children, Seq(Details(fulltextIndexInfo(nameOption, entityNames, propertyKeyNames, options))), variables, withRawCardinalities)

      case CreateTextIndex(_, entityName, propertyKeyNames, nameOption, options) => // Can be both a leaf plan and a middle plan so need to be in both places
        PlanDescriptionImpl(id, "CreateIndex", children, Seq(Details(textIndexInfo(nameOption, entityName, propertyKeyNames, options))), variables, withRawCardinalities)

      case CreateUniquePropertyConstraint(_, node, label, properties: Seq[Property], nameOption, options) => // Can be both a leaf plan and a middle plan so need to be in both places
        val details = Details(constraintInfo(nameOption, node, scala.util.Left(label), properties, scala.util.Right("IS UNIQUE"), options))
        PlanDescriptionImpl(id, "CreateConstraint", children, Seq(details), variables, withRawCardinalities)

      case CreateNodeKeyConstraint(_, node, label, properties: Seq[Property], nameOption, options) => // Can be both a leaf plan and a middle plan so need to be in both places
        val details = Details(constraintInfo(nameOption, node, scala.util.Left(label), properties, scala.util.Right("IS NODE KEY"), options))
        PlanDescriptionImpl(id, "CreateConstraint", children, Seq(details), variables, withRawCardinalities)

      case CreateNodePropertyExistenceConstraint(_, label, prop, nameOption, options) => // Can be both a leaf plan and a middle plan so need to be in both places
        val node = prop.map.asCanonicalStringVal
        val details = Details(constraintInfo(nameOption, node, scala.util.Left(label), Seq(prop), scala.util.Right("IS NOT NULL"), options))
        PlanDescriptionImpl(id, "CreateConstraint", children, Seq(details), variables, withRawCardinalities)

      case CreateRelationshipPropertyExistenceConstraint(_, relTypeName, prop, nameOption, options) => // Can be both a leaf plan and a middle plan so need to be in both places
        val relationship = prop.map.asCanonicalStringVal
        val details = Details(constraintInfo(nameOption, relationship, scala.util.Right(relTypeName), Seq(prop), scala.util.Right("IS NOT NULL"), options))
        PlanDescriptionImpl(id, "CreateConstraint", children, Seq(details), variables, withRawCardinalities)

      case TriadicBuild(_, sourceId, seenId, _) =>
        val details = Details(pretty"(${asPrettyString(sourceId)})--(${asPrettyString(seenId)})")
        PlanDescriptionImpl(id, "TriadicBuild", children, Seq(details), variables, withRawCardinalities)

      case TriadicFilter(_, positivePredicate, sourceId, targetId, _) =>
        val positivePredicateString = if (positivePredicate) pretty"" else pretty"NOT "
        val details = Details(pretty"WHERE $positivePredicateString(${asPrettyString(sourceId)})--(${asPrettyString(targetId)})")
        PlanDescriptionImpl(id, "TriadicFilter", children, Seq(details), variables, withRawCardinalities)

      case PreserveOrder(_) =>
        PlanDescriptionImpl(id, "PreserveOrder", children, Seq.empty[Argument], variables, withRawCardinalities)

      case Foreach(_, variable, expression, mutations) =>
        val details = pretty"${asPrettyString(variable)} IN ${asPrettyString(expression)}" +: mutations.map(mutatingPatternString)
        PlanDescriptionImpl(id, "Foreach", children, Seq(Details(details)), variables, withRawCardinalities)
      case x => throw new InternalException(s"Unknown plan type: ${x.getClass.getSimpleName}. Missing a case?")
    }

    addRuntimeAttributes(addPlanningAttributes(result, plan), plan)
  }

  override def onTwoChildPlan(plan: LogicalPlan, lhs: InternalPlanDescription,
                              rhs: InternalPlanDescription): InternalPlanDescription = {
    checkOnlyWhenAssertionsAreEnabled(plan.lhs.nonEmpty)
    checkOnlyWhenAssertionsAreEnabled(plan.rhs.nonEmpty)

    val id = plan.id
    val variables = plan.availableSymbols.map(asPrettyString(_))
    val children = TwoChildren(lhs, rhs)

    val result: InternalPlanDescription = plan match {
      case _: AntiConditionalApply =>
        PlanDescriptionImpl(id, "AntiConditionalApply", children, Seq.empty, variables, withRawCardinalities)

      case _: AntiSemiApply =>
        PlanDescriptionImpl(id, "AntiSemiApply", children, Seq.empty, variables, withRawCardinalities)

      case _: ConditionalApply =>
        PlanDescriptionImpl(id, "ConditionalApply", children, Seq.empty, variables, withRawCardinalities)

      case _: Apply =>
        PlanDescriptionImpl(id, "Apply", children, Seq.empty, variables, withRawCardinalities)

      case AssertSameNode(node, _, _) =>
        PlanDescriptionImpl(id, "AssertSameNode", children, Seq(Details(asPrettyString(node))), variables, withRawCardinalities)

      case CartesianProduct(_, _, _) =>
        PlanDescriptionImpl(id, "CartesianProduct", children, Seq.empty, variables, withRawCardinalities)

      case NodeHashJoin(nodes, _, _) =>
        PlanDescriptionImpl(id, "NodeHashJoin", children, Seq(Details(keyNamesInfo(nodes.toSeq))), variables, withRawCardinalities)

      case ForeachApply(_, _, variable, expression) =>
        val details = pretty"${asPrettyString(variable)} IN ${asPrettyString(expression)}"
        PlanDescriptionImpl(id, "Foreach", children, Seq(Details(details)), variables, withRawCardinalities)

      case LetSelectOrSemiApply(_, _, _, predicate) =>
        PlanDescriptionImpl(id, "LetSelectOrSemiApply", children, Seq(Details(asPrettyString(predicate))), variables, withRawCardinalities)

      case row: plans.Argument =>
        ArgumentPlanDescription(id = plan.id, Seq.empty, variables)

      case LetSelectOrAntiSemiApply(_, _, _, predicate) =>
        PlanDescriptionImpl(id, "LetSelectOrAntiSemiApply", children, Seq(Details(asPrettyString(predicate))), variables, withRawCardinalities)

      case _: LetSemiApply =>
        PlanDescriptionImpl(id, "LetSemiApply", children, Seq.empty, variables, withRawCardinalities)

      case _: LetAntiSemiApply =>
        PlanDescriptionImpl(id, "LetAntiSemiApply", children, Seq.empty, variables, withRawCardinalities)

      case LeftOuterHashJoin(nodes, _, _) =>
        PlanDescriptionImpl(id, "NodeLeftOuterHashJoin", children, Seq(Details(keyNamesInfo(nodes.toSeq))), variables, withRawCardinalities)

      case RightOuterHashJoin(nodes, _, _) =>
        PlanDescriptionImpl(id, "NodeRightOuterHashJoin", children, Seq(Details(keyNamesInfo(nodes.toSeq))), variables, withRawCardinalities)

      case RollUpApply(_, _, collectionName, variableToCollect) =>
        val detailsList = Seq(collectionName, variableToCollect).map(e => keyNamesInfo(Seq(e)))
        PlanDescriptionImpl(id, "RollUpApply", children, Seq(Details(detailsList)), variables, withRawCardinalities)

      case SelectOrAntiSemiApply(_, _, predicate) =>
        PlanDescriptionImpl(id, "SelectOrAntiSemiApply", children, Seq(Details(asPrettyString(predicate))), variables, withRawCardinalities)

      case SelectOrSemiApply(_, _, predicate) =>
        PlanDescriptionImpl(id, "SelectOrSemiApply", children, Seq(Details(asPrettyString(predicate))), variables, withRawCardinalities)

      case _: SemiApply =>
        PlanDescriptionImpl(id, "SemiApply", children, Seq.empty, variables, withRawCardinalities)

      case TriadicSelection(_, _, positivePredicate, source, seen, target) =>
        val positivePredicateString = if (positivePredicate) pretty"" else pretty"NOT "
        val details = Details(pretty"WHERE $positivePredicateString(${asPrettyString(source)})--(${asPrettyString(target)})")
        PlanDescriptionImpl(id, "TriadicSelection", children, Seq(details), variables, withRawCardinalities)

      case _: Union =>
        PlanDescriptionImpl(id, "Union", children, Seq.empty, variables, withRawCardinalities)

      case _: OrderedUnion =>
        PlanDescriptionImpl(id, "OrderedUnion", children, Seq.empty, variables)

      case ValueHashJoin(_, _, predicate) =>
        PlanDescriptionImpl(
          id = id,
          name = "ValueHashJoin",
          children = children,
          arguments = Seq(Details(asPrettyString(predicate))),
          variables,
          withRawCardinalities)

      case _: MultiNodeIndexSeek | _: AssertingMultiNodeIndexSeek | _: SubqueryForeach =>
        PlanDescriptionImpl(id = plan.id, plan.productPrefix, children, Seq.empty, variables, withRawCardinalities)

      case x => throw new InternalException(s"Unknown plan type: ${x.getClass.getSimpleName}. Missing a case?")
    }

    addRuntimeAttributes(addPlanningAttributes(result, plan), plan)
  }

  private def addPlanningAttributes(description: InternalPlanDescription, plan: LogicalPlan): InternalPlanDescription = {
    val withEstRows = if (effectiveCardinalities.isDefinedAt(plan.id)) {
      val effectiveCardinality = effectiveCardinalities.get(plan.id)
      description.addArgument(EstimatedRows(effectiveCardinality.amount, effectiveCardinality.originalCardinality.map(_.amount)))
    } else {
      description
    }
    if (providedOrders.isDefinedAt(plan.id) && !providedOrders(plan.id).isEmpty) {
      withEstRows.addArgument(asPrettyString.order(providedOrders(plan.id)))
    } else {
      withEstRows
    }
  }

  private def addRuntimeAttributes(description: InternalPlanDescription, plan: LogicalPlan): InternalPlanDescription = {
    runtimeOperatorMetadata(plan.id).foldLeft(description)((acc, x) => acc.addArgument(x))
  }

  private def buildPredicatesDescription(maybeNodePredicate: Option[VariablePredicate],
                                         maybeRelationshipPredicate: Option[VariablePredicate]): Option[PrettyString] = {
    val nodePredicateInfo = maybeNodePredicate.map(_.predicate).map(asPrettyString(_))
    val relationshipPredicateInfo = maybeRelationshipPredicate.map(_.predicate).map(asPrettyString(_))

    (nodePredicateInfo ++ relationshipPredicateInfo) match {
      case predicates if predicates.nonEmpty => Some(predicates.mkPrettyString(" AND "))
      case _ => None
    }
  }

  private def getNodeIndexDescriptions(idName: String,
                                       label: LabelToken,
                                       propertyKeys: Seq[PropertyKeyToken],
                                       valueExpr: QueryExpression[expressions.Expression],
                                       unique: Boolean,
                                       readOnly: Boolean,
                                       caches: Seq[expressions.Expression]): (String, PrettyString) = {

    val name = nodeIndexOperatorName(valueExpr, unique, readOnly)
    val predicate = indexPredicateString(propertyKeys, valueExpr)
    val info = nodeIndexInfoString(idName, unique, label, propertyKeys, predicate, caches)

    (name, info)
  }

  private def getRelIndexDescriptions(idName: String,
                                      start: String,
                                      typeToken: RelationshipTypeToken,
                                      end: String,
                                      isDirected: Boolean,
                                      propertyKeys: Seq[PropertyKeyToken],
                                      valueExpr: QueryExpression[expressions.Expression],
                                      directed: Boolean,
                                      caches: Seq[expressions.Expression]): (String, PrettyString) = {

    val name = relationshipIndexOperatorName(valueExpr, directed)
    val predicate = indexPredicateString(propertyKeys, valueExpr)
    val info = relIndexInfoString(idName, start, typeToken, end, isDirected, propertyKeys, predicate, caches)

    (name, info)
  }

  private def nodeIndexOperatorName(valueExpr: QueryExpression[expressions.Expression],
                                    unique: Boolean,
                                    readOnly: Boolean): String = {
    def findName(exactOnly: Boolean = true) =
      if (unique && !readOnly && exactOnly) {
        NodeIndexSeek.PLAN_DESCRIPTION_UNIQUE_LOCKING_INDEX_SEEK_NAME
      } else if (unique) {
        NodeIndexSeek.PLAN_DESCRIPTION_UNIQUE_INDEX_SEEK_NAME
      } else {
        NodeIndexSeek.PLAN_DESCRIPTION_INDEX_SEEK_NAME
      }
    valueExpr match {
      case _: ExistenceQueryExpression[expressions.Expression] => NodeIndexSeek.PLAN_DESCRIPTION_INDEX_SCAN_NAME
      case _: RangeQueryExpression[expressions.Expression] =>
        if (unique) NodeIndexSeek.PLAN_DESCRIPTION_UNIQUE_INDEX_SEEK_RANGE_NAME else NodeIndexSeek.PLAN_DESCRIPTION_INDEX_SEEK_RANGE_NAME
      case e: CompositeQueryExpression[expressions.Expression] =>
        findName(e.exactOnly)
      case _: SingleQueryExpression[org.neo4j.cypher.internal.expressions.Expression] =>
        findName()
      case _: ManyQueryExpression[org.neo4j.cypher.internal.expressions.Expression] =>
        findName()
    }
  }

  private def relationshipIndexOperatorName(valueExpr: QueryExpression[expressions.Expression], directed: Boolean): String = {
    val indexSeekNames = if (directed) DirectedRelationshipIndexSeek else UndirectedRelationshipIndexSeek
    valueExpr match {
      case _: ExistenceQueryExpression[expressions.Expression] => indexSeekNames.PLAN_DESCRIPTION_INDEX_SCAN_NAME
      case _: RangeQueryExpression[expressions.Expression] => indexSeekNames.PLAN_DESCRIPTION_INDEX_SEEK_RANGE_NAME
      case _ => indexSeekNames.PLAN_DESCRIPTION_INDEX_SEEK_NAME
    }
  }

  private def indexPredicateString(propertyKeys: Seq[PropertyKeyToken],
                                   valueExpr: QueryExpression[expressions.Expression]): PrettyString = valueExpr match {
    case _: ExistenceQueryExpression[expressions.Expression] =>
      pretty"${asPrettyString(propertyKeys.head.name)} IS NOT NULL"

    case e: RangeQueryExpression[expressions.Expression] =>
      checkOnlyWhenAssertionsAreEnabled(propertyKeys.size == 1)
      e.expression match {
        case PrefixSeekRangeWrapper(range) =>
          val propertyKeyName = asPrettyString(propertyKeys.head.name)
          pretty"$propertyKeyName STARTS WITH ${asPrettyString(range.prefix)}"

        case InequalitySeekRangeWrapper(RangeLessThan(bounds)) =>
          bounds.map(rangeBoundString(propertyKeys.head, _, '<')).toIndexedSeq.mkPrettyString(" AND ")

        case InequalitySeekRangeWrapper(RangeGreaterThan(bounds)) =>
          bounds.map(rangeBoundString(propertyKeys.head, _, '>')).toIndexedSeq.mkPrettyString(" AND ")

        case InequalitySeekRangeWrapper(RangeBetween(greaterThanBounds, lessThanBounds)) =>
          val gtBoundString = greaterThanBounds.bounds.map(rangeBoundString(propertyKeys.head, _, '>'))
          val ltBoundStrings = lessThanBounds.bounds.map(rangeBoundString(propertyKeys.head, _, '<'))
          (gtBoundString ++ ltBoundStrings).toIndexedSeq.mkPrettyString(" AND ")

        case PointDistanceSeekRangeWrapper(PointDistanceRange(point, distance, inclusive)) =>
          val funcName = Point.name
          val poi = point match {
            case FunctionInvocation(Namespace(List()), FunctionName(`funcName`), _, Seq(MapExpression(args))) =>
              pretty"point(${args.map(_._2).map(asPrettyString(_)).mkPrettyString(", ")})"
            case _ => asPrettyString(point)
          }
          val propertyKeyName = asPrettyString(propertyKeys.head.name)
          val distanceStr = asPrettyString(distance)
          pretty"distance($propertyKeyName, $poi) <${if (inclusive) pretty"=" else pretty""} $distanceStr"
      }

    case e: SingleQueryExpression[expressions.Expression] =>
      val propertyKeyName = asPrettyString(propertyKeys.head.name)
      pretty"$propertyKeyName = ${asPrettyString(e.expression)}"

    case e: ManyQueryExpression[expressions.Expression] =>
      val (eqOp, innerExp) = e.expression match {
        case ll@ListLiteral(es) =>
          if (es.size == 1) (pretty"=", es.head) else (pretty"IN", ll)
        // This case is used for example when the expression in a parameter
        case x => (pretty"IN", x)
      }
      val propertyKeyName = asPrettyString(propertyKeys.head.name)
      pretty"$propertyKeyName $eqOp ${asPrettyString(innerExp)}"

    case e: CompositeQueryExpression[expressions.Expression] =>
      val predicates = e.inner.zipWithIndex.map {
        case (exp, i) => indexPredicateString(Seq(propertyKeys(i)), exp)
      }
      predicates.mkPrettyString(" AND ")
  }

  private def rangeBoundString(propertyKey: PropertyKeyToken, bound: Bound[expressions.Expression], sign: Char): PrettyString = {
    pretty"${asPrettyString(propertyKey.name)} ${asPrettyString.raw(sign + bound.inequalitySignSuffix)} ${asPrettyString(bound.endPoint)}"
  }

  private def nodeCountFromCountStoreInfo(ident: String, labelNames: List[Option[LabelName]]): PrettyString = {
    val nodes = labelNames.map{
      case Some(label) => pretty"(:${asPrettyString(label.name)})"
      case None        => pretty"()"
    }.mkPrettyString(", ")
    pretty"count( $nodes ) AS ${asPrettyString(ident)}"
  }

  private def relationshipCountFromCountStoreInfo(ident: String,
                                                  startLabel: Option[LabelName],
                                                  typeNames: Seq[RelTypeName],
                                                  endLabel: Option[LabelName]): PrettyString = {
    val start = startLabel
      .map(_.name)
      .map(asPrettyString(_))
      .map(l => pretty":$l")
      .getOrElse(pretty"")
    val end = endLabel
      .map(_.name)
      .map(asPrettyString(_))
      .map(l => pretty":$l")
      .getOrElse(pretty"")
    val types = if (typeNames.nonEmpty) {
      typeNames
      .map(_.name)
      .map(asPrettyString(_))
      .mkPrettyString(":", "|", "")
    } else {
      pretty""
    }

    pretty"count( ($start)-[$types]->($end) ) AS ${asPrettyString(ident)}"
  }

  private def relationshipByIdSeekInfo(idName: String, relIds: SeekableArgs, startNode: String, endNode: String, isDirectional: Boolean): PrettyString = {
    val predicate = seekableArgsInfo(relIds)
    val directionString = if (isDirectional) pretty">" else pretty""
    val prettyStartNode = asPrettyString(startNode)
    val prettyIdName = asPrettyString(idName)
    val prettyEndNode = asPrettyString(endNode)
    pretty"(${prettyStartNode})-[$prettyIdName]-$directionString($prettyEndNode) WHERE id($prettyIdName) $predicate"
  }

  private def seekableArgsInfo(seekableArgs: SeekableArgs): PrettyString = seekableArgs match {
    case ManySeekableArgs(ListLiteral(exprs)) if exprs.size > 1 =>
      pretty"IN ${exprs.map(asPrettyString(_)).mkPrettyString("[", ",", "]")}"
    case ManySeekableArgs(ListLiteral(exprs)) =>
      pretty"= ${asPrettyString(exprs.head)}"
    case _ =>
      pretty"= ${asPrettyString(seekableArgs.expr)}"
  }

  private def signatureInfo(call: ResolvedCall): PrettyString = {
    val argString = call.callArguments.map(asPrettyString(_)).mkPrettyString(SEPARATOR)
    val resultString = call.callResultTypes
      .map { case (name, typ) => pretty"${asPrettyString(name)} :: ${asPrettyString.raw(typ.toNeoTypeString)}" }
      .mkPrettyString(SEPARATOR)
    pretty"${asPrettyString.raw(call.qualifiedName.toString)}($argString) :: ($resultString)"
  }

  private def orderInfo(orderBy: Seq[ColumnOrder]): PrettyString = {
    orderBy.map {
      case Ascending(id) => pretty"${asPrettyString(id)} ASC"
      case Descending(id) => pretty"${asPrettyString(id)} DESC"
    }.mkPrettyString(SEPARATOR)
  }

  private def eagernessReasonInfo(reasons: Seq[EagernessReason.Reason]): Seq[PrettyString] = {
    reasons.collect {
      case _@EagernessReason.UpdateStrategyEager => pretty"updateStrategy=eager"
      case _@EagernessReason.OverlappingSetLabels(labels) => pretty"overlapping set labels: ${labels.map(asPrettyString(_)).mkPrettyString(", ")}"
      case _@EagernessReason.OverlappingDeletedLabels(labels) => pretty"overlapping remove labels: ${labels.map(asPrettyString(_)).mkPrettyString(", ")}"
      case _@EagernessReason.DeleteOverlap(identifiers) => pretty"delete overlap: ${identifiers.map(asPrettyString(_)).mkPrettyString(", ")}"
    }
  }

  private def expandExpressionDescription(from: String,
                                          maybeRelName: Option[String],
                                          relTypes: Seq[String],
                                          to: String,
                                          direction: SemanticDirection,
                                          patternLength: PatternLength): PrettyString = {
    val (min, maybeMax) = patternLength match {
      case SimplePatternLength => (1, None)
      case VarPatternLength(min, maybeMax) => (min, maybeMax)
    }

    expandExpressionDescription(from, maybeRelName, relTypes, to, direction, min, maybeMax, None)
  }

  private def createNodeDescription(cn: CreateNode) = {
    val CreateNode(node, labels, properties) = cn
    val separator = if (labels.isEmpty) pretty": " else pretty" "
    val labelsString = if (labels.nonEmpty) labels.map(x => asPrettyString(x.name)).mkPrettyString(":", ":", "") else pretty""
    val propsString = properties.map(p => pretty"$separator${asPrettyString(p)}").getOrElse(pretty"")
    pretty"(${asPrettyString(node)}$labelsString$propsString)"
  }

  private def expandExpressionDescription(from: String,
                                          maybeRelName: Option[String],
                                          relTypes: Seq[String],
                                          to: String,
                                          direction: SemanticDirection,
                                          minLength: Int,
                                          maxLength: Option[Int],
                                          maybeProperties: Option[Expression]): PrettyString = {
    val left = if (direction == SemanticDirection.INCOMING) pretty"<-" else pretty"-"
    val right = if (direction == SemanticDirection.OUTGOING) pretty"->" else pretty"-"
    val types = if (relTypes.isEmpty) pretty"" else relTypes.map(asPrettyString(_)).mkPrettyString(":", "|", "")
    val separator = if (relTypes.isEmpty) pretty": " else pretty" "
    val propsString = maybeProperties.map(p => pretty"$separator${asPrettyString(p)}").getOrElse(pretty"")
    val lengthDescr: PrettyString = (minLength, maxLength) match {
      case (1, Some(1)) => pretty""
      case (1, None) => pretty"*"
      case (1, Some(m)) => pretty"*..${asPrettyString.raw(m.toString)}"
      case _ => pretty"*${asPrettyString.raw(minLength.toString)}..${asPrettyString.raw(maxLength.map(_.toString).getOrElse(""))}"
    }
    val relName = asPrettyString(maybeRelName.getOrElse(""))
    val relInfo = if (lengthDescr == pretty"" && relTypes.isEmpty && relName.prettifiedString.isEmpty) pretty"" else pretty"[$relName$types$lengthDescr$propsString]"
    pretty"(${asPrettyString(from)})$left$relInfo$right(${asPrettyString(to)})"
  }

  private def nodeIndexInfoString(idName: String,
                                  unique: Boolean,
                                  label: NameToken[_],
                                  propertyKeys: Seq[PropertyKeyToken],
                                  predicate: PrettyString,
                                  caches: Seq[expressions.Expression]): PrettyString = {
    val uniqueStr = if (unique) pretty"UNIQUE " else pretty""
    val propertyKeyString = propertyKeys.map(x => asPrettyString(x.name)).mkPrettyString(SEPARATOR)
    pretty"$uniqueStr${asPrettyString(idName)}:${asPrettyString(label.name)}($propertyKeyString) WHERE $predicate${cachesSuffix(caches)}"
  }

  private def relIndexInfoString(idName: String,
                                 start: String,
                                 relType: NameToken[_],
                                 end: String,
                                 isDirected: Boolean,
                                 propertyKeys: Seq[PropertyKeyToken],
                                 predicate: PrettyString,
                                 caches: Seq[expressions.Expression]): PrettyString = {
    val propertyKeyString = propertyKeys.map(x => asPrettyString(x.name)).mkPrettyString(SEPARATOR)
    val left =  pretty"-"
    val right = if (isDirected) pretty"->" else pretty"-"
    val relInfo = pretty"[${asPrettyString(idName)}:${asPrettyString(relType.name)}($propertyKeyString)]"
    val pattern =  pretty"(${asPrettyString(start)})$left$relInfo$right(${asPrettyString(end)})"
    pretty"$pattern WHERE $predicate${cachesSuffix(caches)}"
  }

  private def aggregationInfo(groupingExpressions: Map[String, Expression],
                              aggregationExpressions: Map[String, Expression],
                              ordered: Seq[Expression] = Seq.empty): PrettyString = {
    val sanitizedOrdered = ordered.map(asPrettyString(_)).toIndexedSeq
    val groupingInfo = projectedExpressionInfo(groupingExpressions, sanitizedOrdered)
    val aggregatingInfo = projectedExpressionInfo(aggregationExpressions)
    (groupingInfo ++ aggregatingInfo).mkPrettyString(SEPARATOR)
  }

  private def projectedExpressionInfo(expressions: Map[String, Expression], ordered: IndexedSeq[PrettyString] = IndexedSeq.empty): Seq[PrettyString] = {
    expressions.toList.map { case (k, v) =>
      val key = asPrettyString(k)
      val value = asPrettyString(v)
      (key, value)
    }.sortBy {
      case (key, _) if ordered.contains(key) => ordered.indexOf(key)
      case (_, value) if ordered.contains(value) => ordered.indexOf(value)
      case _ => Int.MaxValue
    }.map { case (key, value) => if (key == value) key else pretty"$value AS $key" }
  }

  private def keyNamesInfo(keys: Seq[String]): PrettyString = {
    keys
      .map(asPrettyString(_))
      .mkPrettyString(SEPARATOR)
  }

  private def cachesSuffix(caches: Seq[expressions.Expression]): PrettyString = {
    if (caches.isEmpty) pretty"" else caches.map(asPrettyString(_)).mkPrettyString(", ", ", ", "")
  }

  private def indexInfo(indexType: String, nameOption: Option[String], entityName: Either[LabelName, RelTypeName], properties: Seq[PropertyKeyName], options: Options): PrettyString = {
    val name = nameOption.map(n => pretty" ${asPrettyString(n)}").getOrElse(pretty"")
    val propertyString = properties.map(asPrettyString(_)).mkPrettyString("(", SEPARATOR, ")")
    val pattern = entityName match {
      case Left(label) =>
        val prettyLabel = asPrettyString(label.name)
        pretty"(:$prettyLabel)"
      case Right(relType) =>
        val prettyType = asPrettyString(relType.name)
        pretty"()-[:$prettyType]-()"
    }
    pretty"${asPrettyString.raw(indexType)} INDEX$name FOR $pattern ON $propertyString${prettyOptions(options)}"
  }

  private def btreeIndexInfo(nameOption: Option[String], entityName: Either[LabelName, RelTypeName], properties: Seq[PropertyKeyName], options: Options): PrettyString =
    indexInfo("BTREE", nameOption, entityName, properties, options)

  private def rangeIndexInfo(nameOption: Option[String], entityName: Either[LabelName, RelTypeName], properties: Seq[PropertyKeyName], options: Options): PrettyString =
    indexInfo("RANGE", nameOption, entityName, properties, options)

  private def textIndexInfo(nameOption: Option[String], entityName: Either[LabelName, RelTypeName], properties: Seq[PropertyKeyName], options: Options): PrettyString =
    indexInfo("TEXT", nameOption, entityName, properties, options)

  private def fulltextIndexInfo(nameOption: Option[String], entityNames: Either[List[LabelName], List[RelTypeName]], properties: Seq[PropertyKeyName], options: Options): PrettyString = {
    val name = nameOption.map(n => pretty" ${asPrettyString(n)}").getOrElse(pretty"")
    val propertyString = properties.map(asPrettyString(_)).mkPrettyString("[", SEPARATOR, "]")
    val pattern = entityNames match {
      case Left(labels) =>
        val innerPattern = labels.map(l => asPrettyString(l.name)).mkPrettyString(":", "|", "")
        pretty"($innerPattern)"
      case Right(relTypes) =>
        val innerPattern = relTypes.map(r => asPrettyString(r.name)).mkPrettyString(":", "|", "")
        pretty"()-[$innerPattern]-()"
    }
    pretty"FULLTEXT INDEX$name FOR $pattern ON EACH $propertyString${prettyOptions(options)}"
  }

  private def lookupIndexInfo(nameOption: Option[String], entityType: EntityType, options: Options): PrettyString = {
    val name = nameOption.map(n => pretty" ${asPrettyString(n)}").getOrElse(pretty"")
    val (pattern, function) = entityType match {
      case EntityType.NODE         => (pretty"(n)", pretty"${asPrettyString.raw(Labels.name)}(n)")
      case EntityType.RELATIONSHIP => (pretty"()-[r]-()", pretty"${asPrettyString.raw(Type.name)}(r)")
    }
    pretty"LOOKUP INDEX$name FOR $pattern ON EACH $function${prettyOptions(options)}"
  }

  private def constraintInfo(nameOption: Option[String],
                             entity: String,
                             entityName: Either[LabelName, RelTypeName],
                             properties: Seq[Property],
                             assertion: Either[String, String],
                             options: Options = NoOptions,
                             useForAndRequire: Boolean = true): PrettyString = {
    val name = nameOption.map(n => pretty" ${asPrettyString(n)}").getOrElse(pretty"")
    val (leftAssertion, rightAssertion) = assertion match {
      case scala.util.Left(a) => (asPrettyString.raw(a), pretty"")
      case scala.util.Right(a) => (pretty"",asPrettyString.raw(s" $a"))
    }
    val propertyString = properties.map(asPrettyString(_)).mkPrettyString("(", SEPARATOR, ")")
    val prettyEntity = asPrettyString(entity)

    val entityInfo = entityName match {
      case scala.util.Left(label) => pretty"($prettyEntity:${asPrettyString(label)})"
      case scala.util.Right(relType) => pretty"()-[$prettyEntity:${asPrettyString(relType)}]-()"
    }
    val onOrFor = if (useForAndRequire) pretty"FOR" else pretty"ON"
    val assertOrRequire = if (useForAndRequire) pretty"REQUIRE" else pretty"ASSERT"

    pretty"CONSTRAINT$name $onOrFor $entityInfo $assertOrRequire $leftAssertion$propertyString$rightAssertion${prettyOptions(options)}"
  }

  private def prettyOptions(options: Options): PrettyString = options match {
    case NoOptions => pretty""
    case OptionsParam(parameter) => pretty" OPTIONS ${asPrettyString(parameter)}"
    case OptionsMap(options)  => pretty" OPTIONS ${options.map({ case (s, e) => pretty"${asPrettyString(s)}: ${asPrettyString(e)}" }).mkPrettyString("{", SEPARATOR, "}")}"
  }

  private def setPropertyInfo(idName: PrettyString,
                              expression: Expression,
                              removeOtherProps: Boolean): PrettyString = {
    val setString = if (removeOtherProps) pretty"=" else pretty"+="

    pretty"$idName $setString ${asPrettyString(expression)}"
  }

  def mutatingPatternString(setOp: SimpleMutatingPattern): PrettyString = setOp match {
    case CreatePattern(nodes, relationships) =>
      val createNodesPretty = nodes.map(createNodeDescription)
      val createRelsPretty = relationships.map {
        case CreateRelationship(relationship, startNode, typ, endNode, direction, properties) =>
          expandExpressionDescription(startNode, Some(relationship), Seq(typ.name), endNode, direction, 1, Some(1), properties)
      }
      pretty"CREATE ${(createNodesPretty ++ createRelsPretty).mkPrettyString(", ")}"
    case ir.DeleteExpression(toDelete, forced) =>
      if (forced) pretty"DETACH DELETE ${asPrettyString(toDelete)}" else  pretty"DELETE ${asPrettyString(toDelete)}"
    case SetLabelPattern(node, labelNames) =>
      val prettyId = asPrettyString(node)
      val prettyLabels = labelNames.map(labelName => asPrettyString(labelName.name)).mkPrettyString(":", ":", "")
      pretty"SET $prettyId$prettyLabels"
    case RemoveLabelPattern(node, labelNames) =>
      val prettyId = asPrettyString(node)
      val prettyLabels = labelNames.map(labelName => asPrettyString(labelName.name)).mkPrettyString(":", ":", "")
      pretty"REMOVE $prettyId$prettyLabels"
    case SetNodePropertyPattern(node, propertyKey, value) =>
      pretty"SET ${setPropertyInfo(pretty"${asPrettyString(node)}.${asPrettyString(propertyKey.name)}", value, removeOtherProps = true)}"
    case SetNodePropertiesFromMapPattern(node, value, removeOtherProps) =>
      pretty"SET ${setPropertyInfo(asPrettyString(node),  value, removeOtherProps)}"
    case SetRelationshipPropertyPattern(relationship, propertyKey, value) =>
      pretty" SET ${setPropertyInfo(pretty"${asPrettyString(relationship)}.${asPrettyString(propertyKey.name)}", value, removeOtherProps = true)}"
    case SetRelationshipPropertiesFromMapPattern(relationship, value, removeOtherProps) =>
      pretty" SET ${setPropertyInfo(asPrettyString(relationship),  value, removeOtherProps)}"
    case SetPropertyPattern(entity, propertyKey, expression) =>
      val entityString = pretty"${asPrettyString(entity)}.${asPrettyString(propertyKey.name)}"
      pretty" SET ${setPropertyInfo(entityString, expression, true)}"
    case SetPropertiesFromMapPattern(entity, expression, removeOtherProps) =>
      pretty" SET ${setPropertyInfo(asPrettyString(entity), expression, removeOtherProps)}"
  }
}
