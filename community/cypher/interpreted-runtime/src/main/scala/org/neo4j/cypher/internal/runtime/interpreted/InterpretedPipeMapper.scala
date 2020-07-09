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
package org.neo4j.cypher.internal.runtime.interpreted

import org.neo4j.cypher.internal
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.expressions.IterablePredicateExpression
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.ir.VarPatternLength
import org.neo4j.cypher.internal.logical.plans
import org.neo4j.cypher.internal.logical.plans.Aggregation
import org.neo4j.cypher.internal.logical.plans.AllNodesScan
import org.neo4j.cypher.internal.logical.plans.AntiConditionalApply
import org.neo4j.cypher.internal.logical.plans.AntiSemiApply
import org.neo4j.cypher.internal.logical.plans.Apply
import org.neo4j.cypher.internal.logical.plans.Argument
import org.neo4j.cypher.internal.logical.plans.AssertSameNode
import org.neo4j.cypher.internal.logical.plans.CacheProperties
import org.neo4j.cypher.internal.logical.plans.CartesianProduct
import org.neo4j.cypher.internal.logical.plans.ConditionalApply
import org.neo4j.cypher.internal.logical.plans.Create
import org.neo4j.cypher.internal.logical.plans.DeleteExpression
import org.neo4j.cypher.internal.logical.plans.DeleteNode
import org.neo4j.cypher.internal.logical.plans.DeletePath
import org.neo4j.cypher.internal.logical.plans.DeleteRelationship
import org.neo4j.cypher.internal.logical.plans.DetachDeleteExpression
import org.neo4j.cypher.internal.logical.plans.DetachDeleteNode
import org.neo4j.cypher.internal.logical.plans.DetachDeletePath
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipByIdSeek
import org.neo4j.cypher.internal.logical.plans.Distinct
import org.neo4j.cypher.internal.logical.plans.DoNotIncludeTies
import org.neo4j.cypher.internal.logical.plans.DropResult
import org.neo4j.cypher.internal.logical.plans.Eager
import org.neo4j.cypher.internal.logical.plans.EmptyResult
import org.neo4j.cypher.internal.logical.plans.ErrorPlan
import org.neo4j.cypher.internal.logical.plans.Expand
import org.neo4j.cypher.internal.logical.plans.ExpandAll
import org.neo4j.cypher.internal.logical.plans.ExpandInto
import org.neo4j.cypher.internal.logical.plans.FindShortestPaths
import org.neo4j.cypher.internal.logical.plans.ForeachApply
import org.neo4j.cypher.internal.logical.plans.IncludeTies
import org.neo4j.cypher.internal.logical.plans.Input
import org.neo4j.cypher.internal.logical.plans.LeftOuterHashJoin
import org.neo4j.cypher.internal.logical.plans.LetAntiSemiApply
import org.neo4j.cypher.internal.logical.plans.LetSelectOrAntiSemiApply
import org.neo4j.cypher.internal.logical.plans.LetSelectOrSemiApply
import org.neo4j.cypher.internal.logical.plans.LetSemiApply
import org.neo4j.cypher.internal.logical.plans.Limit
import org.neo4j.cypher.internal.logical.plans.LoadCSV
import org.neo4j.cypher.internal.logical.plans.LockNodes
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.MergeCreateNode
import org.neo4j.cypher.internal.logical.plans.MergeCreateRelationship
import org.neo4j.cypher.internal.logical.plans.MultiNodeIndexSeek
import org.neo4j.cypher.internal.logical.plans.NodeByIdSeek
import org.neo4j.cypher.internal.logical.plans.NodeByLabelScan
import org.neo4j.cypher.internal.logical.plans.NodeCountFromCountStore
import org.neo4j.cypher.internal.logical.plans.NodeHashJoin
import org.neo4j.cypher.internal.logical.plans.NodeIndexContainsScan
import org.neo4j.cypher.internal.logical.plans.NodeIndexEndsWithScan
import org.neo4j.cypher.internal.logical.plans.NodeIndexScan
import org.neo4j.cypher.internal.logical.plans.NodeIndexSeek
import org.neo4j.cypher.internal.logical.plans.NodeUniqueIndexSeek
import org.neo4j.cypher.internal.logical.plans.NonFuseable
import org.neo4j.cypher.internal.logical.plans.Optional
import org.neo4j.cypher.internal.logical.plans.OptionalExpand
import org.neo4j.cypher.internal.logical.plans.OrderedAggregation
import org.neo4j.cypher.internal.logical.plans.OrderedDistinct
import org.neo4j.cypher.internal.logical.plans.PartialSort
import org.neo4j.cypher.internal.logical.plans.PartialTop
import org.neo4j.cypher.internal.logical.plans.ProcedureCall
import org.neo4j.cypher.internal.logical.plans.ProduceResult
import org.neo4j.cypher.internal.logical.plans.ProjectEndpoints
import org.neo4j.cypher.internal.logical.plans.Projection
import org.neo4j.cypher.internal.logical.plans.PruningVarExpand
import org.neo4j.cypher.internal.logical.plans.RelationshipCountFromCountStore
import org.neo4j.cypher.internal.logical.plans.RemoveLabels
import org.neo4j.cypher.internal.logical.plans.ResolvedCall
import org.neo4j.cypher.internal.logical.plans.RightOuterHashJoin
import org.neo4j.cypher.internal.logical.plans.RollUpApply
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
import org.neo4j.cypher.internal.logical.plans.Skip
import org.neo4j.cypher.internal.logical.plans.Sort
import org.neo4j.cypher.internal.logical.plans.Top
import org.neo4j.cypher.internal.logical.plans.TriadicSelection
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipByIdSeek
import org.neo4j.cypher.internal.logical.plans.Union
import org.neo4j.cypher.internal.logical.plans.UnwindCollection
import org.neo4j.cypher.internal.logical.plans.ValueHashJoin
import org.neo4j.cypher.internal.logical.plans.VarExpand
import org.neo4j.cypher.internal.logical.plans.VariablePredicate
import org.neo4j.cypher.internal.planner.spi.TokenContext
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.ProcedureCallMode
import org.neo4j.cypher.internal.runtime.QueryIndexRegistrator
import org.neo4j.cypher.internal.runtime.ast.ExpressionVariable
import org.neo4j.cypher.internal.runtime.interpreted.commands.KeyTokenResolver
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.ExpressionConverters
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.InterpretedCommandProjection
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.PatternConverters.ShortestPathsConverter
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.AggregationExpression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Literal
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.ShortestPathExpression
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.Predicate
import org.neo4j.cypher.internal.runtime.interpreted.pipes.AggregationPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.AllNodesScanPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.AllOrderedDistinctPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.AntiSemiApplyPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.ApplyPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.ArgumentPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.AssertSameNodePipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.CachePropertiesPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.CartesianProductPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.ConditionalApplyPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.CreateNodeCommand
import org.neo4j.cypher.internal.runtime.interpreted.pipes.CreatePipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.CreateRelationshipCommand
import org.neo4j.cypher.internal.runtime.interpreted.pipes.DeletePipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.DirectedRelationshipByIdSeekPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.DistinctPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.DropResultPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.EagerAggregationPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.EagerPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.EmptyResultPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.ErrorPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.CypherRowFactory
import org.neo4j.cypher.internal.runtime.interpreted.pipes.ExpandAllPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.ExpandIntoPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.FilterPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.ForeachPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.IndexSeekModeFactory
import org.neo4j.cypher.internal.runtime.interpreted.pipes.InputPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.LazyLabel
import org.neo4j.cypher.internal.runtime.interpreted.pipes.LazyPropertyKey
import org.neo4j.cypher.internal.runtime.interpreted.pipes.LazyType
import org.neo4j.cypher.internal.runtime.interpreted.pipes.LetSelectOrSemiApplyPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.LetSemiApplyPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.LimitPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.LoadCSVPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.LockNodesPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.MergeCreateNodePipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.MergeCreateRelationshipPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.NodeByIdSeekPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.NodeByLabelScanPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.NodeCountFromCountStorePipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.NodeHashJoinPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.NodeIndexContainsScanPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.NodeIndexEndsWithScanPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.NodeIndexScanPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.NodeIndexSeekPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.NodeLeftOuterHashJoinPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.NodeRightOuterHashJoinPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.NonFuseablePipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.OptionalExpandAllPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.OptionalExpandIntoPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.OptionalPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.OrderedAggregationPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.OrderedDistinctPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.PartialSortPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.PartialTop1Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.PartialTop1WithTiesPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.PartialTopNPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.PipeMapper
import org.neo4j.cypher.internal.runtime.interpreted.pipes.ProcedureCallPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.ProcedureCallRowProcessing
import org.neo4j.cypher.internal.runtime.interpreted.pipes.ProduceResultsPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.ProjectEndpointsPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.ProjectionPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.PruningVarLengthExpandPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.interpreted.pipes.RelationshipCountFromCountStorePipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.RelationshipTypes
import org.neo4j.cypher.internal.runtime.interpreted.pipes.RemoveLabelsPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.RollUpApplyPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.SelectOrSemiApplyPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.SemiApplyPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.SetLabelsOperation
import org.neo4j.cypher.internal.runtime.interpreted.pipes.SetNodePropertyFromMapOperation
import org.neo4j.cypher.internal.runtime.interpreted.pipes.SetNodePropertyOperation
import org.neo4j.cypher.internal.runtime.interpreted.pipes.SetPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.SetPropertyFromMapOperation
import org.neo4j.cypher.internal.runtime.interpreted.pipes.SetPropertyOperation
import org.neo4j.cypher.internal.runtime.interpreted.pipes.SetRelationshipPropertyFromMapOperation
import org.neo4j.cypher.internal.runtime.interpreted.pipes.SetRelationshipPropertyOperation
import org.neo4j.cypher.internal.runtime.interpreted.pipes.ShortestPathPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.SkipPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.SortPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Top1Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Top1WithTiesPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.TopNPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.TriadicSelectionPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.UndirectedRelationshipByIdSeekPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.UnionPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.UnwindPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.ValueHashJoinPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.VarLengthExpandPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.VarLengthPredicate
import org.neo4j.cypher.internal.runtime.interpreted.pipes.aggregation.GroupingAggTable
import org.neo4j.cypher.internal.runtime.interpreted.pipes.aggregation.NonGroupingAggTable
import org.neo4j.cypher.internal.runtime.interpreted.pipes.aggregation.OrderedGroupingAggTable
import org.neo4j.cypher.internal.runtime.interpreted.pipes.aggregation.OrderedNonGroupingAggTable
import org.neo4j.cypher.internal.util.Eagerly
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.exceptions.InternalException
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.NodeValue
import org.neo4j.values.virtual.RelationshipValue

/**
 * Responsible for turning a logical plan with argument pipes into a new pipe.
 * When adding new Pipes and LogicalPlans, this is where you should be looking.
 */
case class InterpretedPipeMapper(readOnly: Boolean,
                                 expressionConverters: ExpressionConverters,
                                 tokenContext: TokenContext,
                                 indexRegistrator: QueryIndexRegistrator)
                                (implicit semanticTable: SemanticTable) extends PipeMapper {

  private def getBuildExpression(id: Id): internal.expressions.Expression => Expression =
    ((e: internal.expressions.Expression) => expressionConverters.toCommandExpression(id, e)) andThen
      (expression => expression.rewrite(KeyTokenResolver.resolveExpressions(_, tokenContext)))

  def onLeaf(plan: LogicalPlan): Pipe = {
    val id = plan.id
    val buildExpression = getBuildExpression(id)
    plan match {
      case Argument(_) =>
        ArgumentPipe()(id)

      case AllNodesScan(ident, _) =>
        AllNodesScanPipe(ident)(id = id)

      case NodeCountFromCountStore(ident, labels, _) =>
        NodeCountFromCountStorePipe(ident, labels.map(l => l.map(LazyLabel.apply)))(id = id)

      case RelationshipCountFromCountStore(ident, startLabel, typeNames, endLabel, _) =>
        RelationshipCountFromCountStorePipe(ident, startLabel.map(LazyLabel.apply(_)),
          RelationshipTypes(typeNames.map(_.name).toArray, tokenContext), endLabel.map(LazyLabel.apply(_)))(id = id)

      case NodeByLabelScan(ident, label, _, indexOrder) =>
        indexRegistrator.registerLabelScan()
        NodeByLabelScanPipe(ident, LazyLabel(label), indexOrder)(id = id)

      case NodeByIdSeek(ident, nodeIdExpr, _) =>
        NodeByIdSeekPipe(ident, expressionConverters.toCommandSeekArgs(id, nodeIdExpr))(id = id)

      case DirectedRelationshipByIdSeek(ident, relIdExpr, fromNode, toNode, _) =>
        DirectedRelationshipByIdSeekPipe(ident, expressionConverters.toCommandSeekArgs(id, relIdExpr), toNode, fromNode)(id = id)

      case UndirectedRelationshipByIdSeek(ident, relIdExpr, fromNode, toNode, _) =>
        UndirectedRelationshipByIdSeekPipe(ident, expressionConverters.toCommandSeekArgs(id, relIdExpr), toNode, fromNode)(id = id)

      case NodeIndexSeek(ident, label, properties, valueExpr, _, indexOrder) =>
        val indexSeekMode = IndexSeekModeFactory(unique = false, readOnly = readOnly).fromQueryExpression(valueExpr)
        NodeIndexSeekPipe(ident, label, properties.toArray, indexRegistrator.registerQueryIndex(label, properties),
          valueExpr.map(buildExpression), indexSeekMode, indexOrder)(id = id)

      case NodeUniqueIndexSeek(ident, label, properties, valueExpr, _, indexOrder) =>
        val indexSeekMode = IndexSeekModeFactory(unique = true, readOnly = readOnly).fromQueryExpression(valueExpr)
        NodeIndexSeekPipe(ident, label, properties.toArray, indexRegistrator.registerQueryIndex(label, properties),
          valueExpr.map(buildExpression), indexSeekMode, indexOrder)(id = id)

      case NodeIndexScan(ident, label, properties, _, indexOrder) =>
        NodeIndexScanPipe(ident, label, properties, indexRegistrator.registerQueryIndex(label, properties), indexOrder)(id = id)

      case NodeIndexContainsScan(ident, label, property, valueExpr, _, indexOrder) =>
        NodeIndexContainsScanPipe(ident, label, property, indexRegistrator.registerQueryIndex(label, property),
          buildExpression(valueExpr), indexOrder)(id = id)

      case NodeIndexEndsWithScan(ident, label, property, valueExpr, _, indexOrder) =>
        NodeIndexEndsWithScanPipe(ident, label, property, indexRegistrator.registerQueryIndex(label, property),
          buildExpression(valueExpr), indexOrder)(id = id)

      // Currently used for testing only
      case MultiNodeIndexSeek(indexLeafPlans) =>
        indexLeafPlans.foldLeft(None: Option[Pipe]) {
          case (None, plan) =>
            Some(onLeaf(plan))
          case (Some(pipe), plan) => {
            Some(CartesianProductPipe(pipe, onLeaf(plan))(id = id))
          }
        }.get

      case Input(nodes, relationships, variables, _) =>
        InputPipe(nodes.toArray ++ relationships ++ variables)(id = id)
    }
  }

  def onOneChildPlan(plan: LogicalPlan, source: Pipe): Pipe = {
    val id = plan.id
    val buildExpression = getBuildExpression(id)
    plan match {
      case Projection(_, expressions) =>
        ProjectionPipe(source,  InterpretedCommandProjection(Eagerly.immutableMapValues(expressions, buildExpression)))(id = id)

      case ProjectEndpoints(_, rel, start, startInScope, end, endInScope, types, directed, length) =>
        ProjectEndpointsPipe(source, rel,
                             start, startInScope,
                             end, endInScope,
                             types.map(_.toArray).map(RelationshipTypes.apply).getOrElse(RelationshipTypes.empty), directed, length.isSimple)(id = id)

      case EmptyResult(_) =>
        EmptyResultPipe(source)(id = id)

      case DropResult(_) =>
        DropResultPipe(source)(id = id)

      case NonFuseable(_) =>
        NonFuseablePipe(source)(id = id)

      case Selection(predicate, _) =>
        val predicateExpression =
          if (predicate.exprs.size == 1) buildExpression(predicate.exprs.head) else buildExpression(predicate)
        FilterPipe(source, predicateExpression)(id = id)

      case CacheProperties(_, properties) =>
        val runtimeProperties = properties.toArray.map(buildExpression(_))
        CachePropertiesPipe(source, runtimeProperties)(id = id)

      case Expand(_, fromName, dir, types: Seq[RelTypeName], toName, relName, ExpandAll, _) =>
        ExpandAllPipe(source, fromName, relName, toName, dir, RelationshipTypes(types.toArray))(id = id)

      case Expand(_, fromName, dir, types: Seq[RelTypeName], toName, relName, ExpandInto, _) =>
        ExpandIntoPipe(source, fromName, relName, toName, dir, RelationshipTypes(types.toArray))(id = id)

      case LockNodes(_, nodesToLock) =>
        LockNodesPipe(source, nodesToLock)(id = id)

      case OptionalExpand(_, fromName, dir, types, toName, relName, ExpandAll, predicate, _) =>
        OptionalExpandAllPipe(source, fromName, relName, toName, dir, RelationshipTypes(types.toArray), predicate.map(buildExpression))(id = id)

      case OptionalExpand(_, fromName, dir, types, toName, relName, ExpandInto, predicate, _) =>
        OptionalExpandIntoPipe(source, fromName, relName, toName, dir, RelationshipTypes(types.toArray), predicate.map(buildExpression))(id = id)

      case VarExpand(_,
                     fromName,
                     dir,
                     projectedDir,
                     types,
                     toName,
                     relName,
                     VarPatternLength(min, max),
                     expansionMode,
                     nodePredicate,
                     relationshipPredicate) =>
        val predicate = varLengthPredicate(id, nodePredicate, relationshipPredicate)

        val nodeInScope = expansionMode match {
          case ExpandAll => false
          case ExpandInto => true
        }

        VarLengthExpandPipe(source, fromName, relName, toName, dir, projectedDir,
          RelationshipTypes(types.toArray), min, max, nodeInScope, predicate)(id = id)

      case Optional(inner, protectedSymbols) =>
        OptionalPipe(inner.availableSymbols -- protectedSymbols, source)(id = id)

      case PruningVarExpand(_,
                            from,
                            dir,
                            types,
                            toName,
                            minLength,
                            maxLength,
                            nodePredicate,
                            relationshipPredicate) =>
        val predicate = varLengthPredicate(id, nodePredicate, relationshipPredicate)
        PruningVarLengthExpandPipe(source, from, toName, RelationshipTypes(types.toArray), dir, minLength, maxLength, predicate)(id = id)

      case Sort(_, sortItems) =>
        SortPipe(source, InterpretedExecutionContextOrdering.asComparator(sortItems.map(translateColumnOrder)))(id = id)

      case PartialSort(_, alreadySortedPrefix, stillToSortSuffix) =>
        PartialSortPipe(source, InterpretedExecutionContextOrdering.asComparator(alreadySortedPrefix.map(translateColumnOrder)), InterpretedExecutionContextOrdering.asComparator(stillToSortSuffix.map(translateColumnOrder)))(id = id)

      case Skip(_, count) =>
        SkipPipe(source, buildExpression(count))(id = id)

      case Top(_, sortItems, _) if sortItems.isEmpty => source

      case Top(_, sortItems, SignedDecimalIntegerLiteral("1")) =>
        Top1Pipe(source, InterpretedExecutionContextOrdering.asComparator(sortItems.map(translateColumnOrder).toList))(id = id)

      case Top(_, sortItems, limit) =>
        TopNPipe(source, buildExpression(limit),
          InterpretedExecutionContextOrdering.asComparator(sortItems.map(translateColumnOrder).toList))(id = id)

      case PartialTop(_, _, stillToSortSuffix, _) if stillToSortSuffix.isEmpty => source

      case PartialTop(_, alreadySortedPrefix, stillToSortSuffix, SignedDecimalIntegerLiteral("1")) =>
        PartialTop1Pipe(source, InterpretedExecutionContextOrdering.asComparator(alreadySortedPrefix.map(translateColumnOrder).toList),
          InterpretedExecutionContextOrdering.asComparator(stillToSortSuffix.map(translateColumnOrder).toList))(id = id)

      case PartialTop(_, alreadySortedPrefix, stillToSortSuffix, limit) =>
        PartialTopNPipe(source, buildExpression(limit), InterpretedExecutionContextOrdering.asComparator(alreadySortedPrefix.map(translateColumnOrder).toList),
          InterpretedExecutionContextOrdering.asComparator(stillToSortSuffix.map(translateColumnOrder).toList))(id = id)

      case Limit(_, count, DoNotIncludeTies) =>
        LimitPipe(source, buildExpression(count))(id = id)

      case Limit(_, count, IncludeTies) =>
        (source, count) match {
          case (SortPipe(inner, comparator), SignedDecimalIntegerLiteral("1")) =>
            Top1WithTiesPipe(inner, comparator)(id = id)
          case (PartialSortPipe(inner, prefixComparator, suffixComparator), SignedDecimalIntegerLiteral("1")) =>
            PartialTop1WithTiesPipe(inner, prefixComparator, suffixComparator)(id = id)

          case _ => throw new InternalException("Including ties is only supported for very specific plans")
        }

      case Aggregation(_, groupingExpressions, aggregatingExpressions) if aggregatingExpressions.isEmpty =>
        val projection = groupingExpressions.map {
          case (key, value) => DistinctPipe.GroupingCol(key, buildExpression(value))
        }.toArray
        DistinctPipe(source, projection)(id = id)

      case Distinct(_, groupingExpressions) =>
        val projection = groupingExpressions.map {
          case (key, value) => DistinctPipe.GroupingCol(key, buildExpression(value))
        }.toArray
        DistinctPipe(source, projection)(id = id)

      case OrderedDistinct(_, groupingExpressions, orderToLeverage) =>
        val projection = groupingExpressions.map {
          case (key, value) => DistinctPipe.GroupingCol(key, buildExpression(value), orderToLeverage.contains(value))
        }.toArray
        if (projection.forall(_.ordered)) {
          AllOrderedDistinctPipe(source, projection)(id = id)
        } else {
          OrderedDistinctPipe(source, projection)(id = id)
        }

      case OrderedAggregation(_, groupingExpressions, aggregatingExpressions, orderToLeverage) if aggregatingExpressions.isEmpty =>
        val projection = groupingExpressions.map {
          case (key, value) => DistinctPipe.GroupingCol(key, buildExpression(value), orderToLeverage.contains(value))
        }.toArray
        OrderedDistinctPipe(source, projection)(id = id)

      case Aggregation(_, groupingExpressions, aggregatingExpressions) =>
        val aggregationColumns = aggregatingExpressions.map {
          case (key, value) => AggregationPipe.AggregatingCol(key, buildExpression(value).asInstanceOf[AggregationExpression])
        }.toArray

        val tableFactory =
          if (groupingExpressions.isEmpty) {
            NonGroupingAggTable.Factory(aggregationColumns)
          } else {
            val groupingColumns = groupingExpressions.map {
              case (key, value) => DistinctPipe.GroupingCol(key, buildExpression(value))
            }.toArray
            val groupingFunction: (CypherRow, QueryState) => AnyValue = AggregationPipe.computeGroupingFunction(groupingColumns)
            GroupingAggTable.Factory(groupingColumns, groupingFunction, aggregationColumns)
          }
        EagerAggregationPipe(source, tableFactory)(id = id)

      case OrderedAggregation(_, groupingExpressions, aggregatingExpressions, orderToLeverage) =>
        val aggregationColumns = aggregatingExpressions.map {
          case (key, value) => AggregationPipe.AggregatingCol(key, buildExpression(value).asInstanceOf[AggregationExpression])
        }.toArray
        val groupingColumns = groupingExpressions.map {
          case (key, value) => DistinctPipe.GroupingCol(key, buildExpression(value), orderToLeverage.contains(value))
        }.toArray

        val (orderedGroupingColumns, unorderedGroupingColumns) = groupingColumns.partition(_.ordered)
        val unorderedGroupingFunction: (CypherRow, QueryState) => AnyValue = AggregationPipe.computeGroupingFunction(unorderedGroupingColumns)
        val orderedGroupingFunction: (CypherRow, QueryState) => AnyValue = AggregationPipe.computeGroupingFunction(orderedGroupingColumns)

        val tableFactory =
          if (groupingColumns.forall(_.ordered)) {
            OrderedNonGroupingAggTable.Factory(orderedGroupingFunction, orderedGroupingColumns, aggregationColumns)
          } else {
            OrderedGroupingAggTable.Factory(orderedGroupingFunction, orderedGroupingColumns, unorderedGroupingFunction, unorderedGroupingColumns, aggregationColumns)
          }
        OrderedAggregationPipe(source, tableFactory)(id = id)

      case FindShortestPaths(_, shortestPathPattern, predicates, withFallBack, disallowSameNode) =>
        val legacyShortestPath = shortestPathPattern.expr.asLegacyPatterns(id, shortestPathPattern.name, expressionConverters).head
        val pathVariables = Set(legacyShortestPath.pathName, legacyShortestPath.relIterator.getOrElse(""))

        def noDependency(expression: internal.expressions.Expression) =
          (expression.dependencies.map(_.name) intersect pathVariables).isEmpty

        val (perStepPredicates, fullPathPredicates) = predicates.partition {
          case p: IterablePredicateExpression =>
            noDependency(
              p.innerPredicate.getOrElse(throw new InternalException("This should have been handled in planning")))
          case e => noDependency(e)
        }
        val commandPerStepPredicates = perStepPredicates.map(p => buildPredicate(id, p))
        val commandFullPathPredicates = fullPathPredicates.map(p => buildPredicate(id, p))

        val commandExpression = ShortestPathExpression(legacyShortestPath, commandPerStepPredicates,
          commandFullPathPredicates, withFallBack, disallowSameNode, id)
        ShortestPathPipe(source, commandExpression)(id = id)

      case UnwindCollection(_, variable, collection) =>
        UnwindPipe(source, buildExpression(collection), variable)(id = id)

      case ProcedureCall(_, call@ResolvedCall(signature, callArguments, _, _, _)) =>
        val callMode = ProcedureCallMode.fromAccessMode(signature.accessMode)
        val callArgumentCommands = callArguments.map(Some(_)).zipAll(signature.inputSignature.map(_.default), None, None).map {
          case (given, default) => given.map(buildExpression).getOrElse(Literal(default.get))
        }
        val rowProcessing = ProcedureCallRowProcessing(signature)
        ProcedureCallPipe(source, signature, callMode, callArgumentCommands, rowProcessing, call.callResultTypes, call.callResultIndices)(id = id)

      case LoadCSV(_, url, variableName, format, fieldTerminator, legacyCsvQuoteEscaping, bufferSize) =>
        LoadCSVPipe(source, format, buildExpression(url), variableName, fieldTerminator, legacyCsvQuoteEscaping, bufferSize)(id = id)

      case ProduceResult(_, columns) =>
        ProduceResultsPipe(source, columns.toArray)(id = id)

      case Create(_, nodes, relationships) =>
        CreatePipe(
          source,
          nodes.map(n =>
            CreateNodeCommand(n.idName, n.labels.map(LazyLabel.apply), n.properties.map(buildExpression))
          ).toArray,
          relationships.map(r =>
            CreateRelationshipCommand(r.idName, r.startNode, LazyType(r.relType.name), r.endNode, r.properties.map(buildExpression))
          ).toArray
        )(id = id)

      case MergeCreateNode(_, idName, labels, props) =>
        MergeCreateNodePipe(source,
          CreateNodeCommand(idName, labels.map(LazyLabel.apply), props.map(buildExpression))
        )(id = id)

      case MergeCreateRelationship(_, idName, startNode, typ, endNode, props) =>
        MergeCreateRelationshipPipe(source,
          CreateRelationshipCommand(idName, startNode, LazyType(typ)(semanticTable), endNode, props.map(buildExpression))
        )(id = id)

      case SetLabels(_, name, labels) =>
        SetPipe(source, SetLabelsOperation(name, labels.map(LazyLabel.apply)))(id = id)

      case SetNodeProperty(_, name, propertyKey, expression) =>
        val needsExclusiveLock = internal.expressions.Expression.hasPropertyReadDependency(name, expression, propertyKey)
        SetPipe(source, SetNodePropertyOperation(name, LazyPropertyKey(propertyKey),
          buildExpression(expression), needsExclusiveLock))(id = id)

      case SetNodePropertiesFromMap(_, name, expression, removeOtherProps) =>
        val needsExclusiveLock = internal.expressions.Expression.mapExpressionHasPropertyReadDependency(name, expression)
        SetPipe(source,
          SetNodePropertyFromMapOperation(name, buildExpression(expression), removeOtherProps, needsExclusiveLock))(id = id)

      case SetPropertiesFromMap(_, entityExpr, expression, removeOtherProps) =>
        SetPipe(source,
          SetPropertyFromMapOperation(buildExpression(entityExpr), buildExpression(expression), removeOtherProps))(id = id)

      case SetRelationshipProperty(_, name, propertyKey, expression) =>
        val needsExclusiveLock = internal.expressions.Expression.hasPropertyReadDependency(name, expression, propertyKey)
        SetPipe(source,
          SetRelationshipPropertyOperation(name, LazyPropertyKey(propertyKey), buildExpression(expression), needsExclusiveLock))(id = id)

      case SetRelationshipPropertiesFromMap(_, name, expression, removeOtherProps) =>
        val needsExclusiveLock = internal.expressions.Expression.mapExpressionHasPropertyReadDependency(name, expression)
        SetPipe(source,
          SetRelationshipPropertyFromMapOperation(name, buildExpression(expression), removeOtherProps, needsExclusiveLock))(id = id)

      case SetProperty(_, entityExpr, propertyKey, expression) =>
        SetPipe(source, SetPropertyOperation(
          buildExpression(entityExpr), LazyPropertyKey(propertyKey), buildExpression(expression)))(id = id)

      case RemoveLabels(_, name, labels) =>
        RemoveLabelsPipe(source, name, labels.map(LazyLabel.apply))(id = id)

      case DeleteNode(_, expression) =>
        DeletePipe(source, buildExpression(expression), forced = false)(id = id)

      case DetachDeleteNode(_, expression) =>
        DeletePipe(source, buildExpression(expression), forced = true)(id = id)

      case DeleteRelationship(_, expression) =>
        DeletePipe(source, buildExpression(expression), forced = false)(id = id)

      case DeletePath(_, expression) =>
        DeletePipe(source, buildExpression(expression), forced = false)(id = id)

      case DetachDeletePath(_, expression) =>
        DeletePipe(source, buildExpression(expression), forced = true)(id = id)

      case DeleteExpression(_, expression) =>
        DeletePipe(source, buildExpression(expression), forced = false)(id = id)

      case DetachDeleteExpression(_, expression) =>
        DeletePipe(source, buildExpression(expression), forced = true)(id = id)

      case Eager(_) =>
        EagerPipe(source)(id = id)

      case ErrorPlan(_, ex) =>
        ErrorPipe(source, ex)(id = id)

      case x =>
        throw new InternalException(s"Received a logical plan that has no physical operator $x")
    }
  }

  private def varLengthPredicate(id: Id,
                                 nodePredicate: Option[VariablePredicate],
                                 relationshipPredicate: Option[VariablePredicate]): VarLengthPredicate  = {

    //Creates commands out of the predicates
    def asCommand(maybeVariablePredicate: Option[VariablePredicate]): ((CypherRow, QueryState, AnyValue) => Boolean, Option[Predicate]) =
      maybeVariablePredicate match {
        case None => ((_: CypherRow, _: QueryState, _: AnyValue) => true, None)
        case Some(VariablePredicate(variable, astPredicate)) =>
          val command = buildPredicate(id, astPredicate)
          val ev = ExpressionVariable.cast(variable)
          ((context: CypherRow, state: QueryState, entity: AnyValue) => {
            state.expressionVariables(ev.offset) = entity
            command.isTrue(context, state)
          }, Some(command))
      }

    val (nodeCommand, maybeNodeCommandPred) = asCommand(nodePredicate)
    val (relCommand, maybeRelCommandPred) = asCommand(relationshipPredicate)

    new VarLengthPredicate {
      override def filterNode(row: CypherRow, state: QueryState)(node: NodeValue): Boolean = nodeCommand(row, state, node)
      override def filterRelationship(row: CypherRow, state: QueryState)(rel: RelationshipValue): Boolean = relCommand(row, state, rel)

      override def predicateExpressions: Seq[Predicate] = maybeNodeCommandPred.toSeq ++ maybeRelCommandPred
    }
  }

  def onTwoChildPlan(plan: LogicalPlan, lhs: Pipe, rhs: Pipe): Pipe = {
    val id = plan.id
    val buildExpression = getBuildExpression(id)
    plan match {
      case CartesianProduct(_, _) =>
        CartesianProductPipe(lhs, rhs)(id = id)

      case NodeHashJoin(nodes, _, _) =>
        NodeHashJoinPipe(nodes, lhs, rhs)(id = id)

      case LeftOuterHashJoin(nodes, l, r) =>
        val nullableVariables = r.availableSymbols -- l.availableSymbols
        NodeLeftOuterHashJoinPipe(nodes, lhs, rhs, nullableVariables)(id = id)

      case RightOuterHashJoin(nodes, l, r) =>
        val nullableVariables = l.availableSymbols -- r.availableSymbols
        NodeRightOuterHashJoinPipe(nodes, lhs, rhs, nullableVariables)(id = id)

      case Apply(_, _) => ApplyPipe(lhs, rhs)(id = id)

      case AssertSameNode(node, _, _) =>
        AssertSameNodePipe(lhs, rhs, node)(id = id)

      case SemiApply(_, _) =>
        SemiApplyPipe(lhs, rhs)(id = id)

      case AntiSemiApply(_, _) =>
        AntiSemiApplyPipe(lhs, rhs)(id = id)

      case LetSemiApply(_, _, idName) =>
        LetSemiApplyPipe(lhs, rhs, idName, negated = false)(id = id)

      case LetAntiSemiApply(_, _, idName) =>
        LetSemiApplyPipe(lhs, rhs, idName, negated = true)(id = id)

      case SelectOrSemiApply(_, _, predicate) =>
        SelectOrSemiApplyPipe(lhs, rhs, buildPredicate(id, predicate), negated = false)(id = id)

      case SelectOrAntiSemiApply(_, _, predicate) =>
        SelectOrSemiApplyPipe(lhs, rhs, buildPredicate(id, predicate), negated = true)(id = id)

      case LetSelectOrSemiApply(_, _, idName, predicate) =>
        LetSelectOrSemiApplyPipe(lhs, rhs, idName, buildPredicate(id, predicate), negated = false)(id = id)

      case LetSelectOrAntiSemiApply(_, _, idName, predicate) =>
        LetSelectOrSemiApplyPipe(lhs, rhs, idName, buildPredicate(id, predicate), negated = true)(id = id)

      case ConditionalApply(lhsPlan, rhsPlan, ids) =>
        ConditionalApplyPipe(lhs, rhs, ids, negated = false, rhsPlan.availableSymbols -- lhsPlan.availableSymbols)(id = id)

      case AntiConditionalApply(lhsPlan, rhsPlan, ids) =>
        ConditionalApplyPipe(lhs, rhs, ids, negated = true, rhsPlan.availableSymbols -- lhsPlan.availableSymbols)(id = id)

      case Union(_, _) =>
        UnionPipe(lhs, rhs)(id = id)

      case TriadicSelection(_, _, positivePredicate, sourceId, seenId, targetId) =>
        TriadicSelectionPipe(positivePredicate, lhs, sourceId, seenId, targetId, rhs)(id = id)

      case ValueHashJoin(_, _, internal.expressions.Equals(lhsExpression, rhsExpression)) =>
        ValueHashJoinPipe(buildExpression(lhsExpression), buildExpression(rhsExpression), lhs, rhs)(id = id)

      case ForeachApply(_, _, variable, expression) =>
        ForeachPipe(lhs, rhs, variable, buildExpression(expression))(id = id)

      case RollUpApply(_, _, collectionName, identifierToCollection) =>
        RollUpApplyPipe(lhs, rhs, collectionName, identifierToCollection)(id = id)

      case x =>
        throw new InternalException(s"Received a logical plan that has no physical operator $x")
    }
  }

  private def buildPredicate(id: Id, expr: internal.expressions.Expression): Predicate =
    expressionConverters.toCommandPredicate(id, expr)
      .rewrite(KeyTokenResolver.resolveExpressions(_, tokenContext))
      .asInstanceOf[Predicate]

  private def translateColumnOrder(s: plans.ColumnOrder): org.neo4j.cypher.internal.runtime.interpreted.ColumnOrder = s match {
    case plans.Ascending(name) => org.neo4j.cypher.internal.runtime.interpreted.Ascending(name)
    case plans.Descending(name) => org.neo4j.cypher.internal.runtime.interpreted.Descending(name)
  }
}
