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
package org.neo4j.cypher.internal.runtime.interpreted

import org.neo4j.cypher.internal
import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.expressions
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.NODE_TYPE
import org.neo4j.cypher.internal.expressions.RELATIONSHIP_TYPE
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.frontend.phases.CypherScope.toKernelScope
import org.neo4j.cypher.internal.frontend.phases.ResolvedCall
import org.neo4j.cypher.internal.ir.CreatePattern
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
import org.neo4j.cypher.internal.ir.SimpleMutatingPattern
import org.neo4j.cypher.internal.ir.VarPatternLength
import org.neo4j.cypher.internal.logical.plans
import org.neo4j.cypher.internal.logical.plans.Aggregation
import org.neo4j.cypher.internal.logical.plans.AllNodesScan
import org.neo4j.cypher.internal.logical.plans.AntiConditionalApply
import org.neo4j.cypher.internal.logical.plans.AntiSemiApply
import org.neo4j.cypher.internal.logical.plans.Apply
import org.neo4j.cypher.internal.logical.plans.Argument
import org.neo4j.cypher.internal.logical.plans.AssertSameNode
import org.neo4j.cypher.internal.logical.plans.AssertSameRelationship
import org.neo4j.cypher.internal.logical.plans.BFSPruningVarExpand
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
import org.neo4j.cypher.internal.logical.plans.DirectedAllRelationshipsScan
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipByElementIdSeek
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipByIdSeek
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipIndexContainsScan
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipIndexEndsWithScan
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipIndexScan
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipIndexSeek
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipTypeScan
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipUniqueIndexSeek
import org.neo4j.cypher.internal.logical.plans.DirectedUnionRelationshipTypesScan
import org.neo4j.cypher.internal.logical.plans.Distinct
import org.neo4j.cypher.internal.logical.plans.Eager
import org.neo4j.cypher.internal.logical.plans.EmptyResult
import org.neo4j.cypher.internal.logical.plans.ErrorPlan
import org.neo4j.cypher.internal.logical.plans.ExhaustiveLimit
import org.neo4j.cypher.internal.logical.plans.Expand
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandAll
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandInto
import org.neo4j.cypher.internal.logical.plans.Expand.VariablePredicate
import org.neo4j.cypher.internal.logical.plans.FindShortestPaths
import org.neo4j.cypher.internal.logical.plans.FindShortestPaths.AllowSameNode
import org.neo4j.cypher.internal.logical.plans.Foreach
import org.neo4j.cypher.internal.logical.plans.ForeachApply
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.logical.plans.InjectCompilationError
import org.neo4j.cypher.internal.logical.plans.Input
import org.neo4j.cypher.internal.logical.plans.IntersectionNodeByLabelsScan
import org.neo4j.cypher.internal.logical.plans.LeftOuterHashJoin
import org.neo4j.cypher.internal.logical.plans.LetAntiSemiApply
import org.neo4j.cypher.internal.logical.plans.LetSelectOrAntiSemiApply
import org.neo4j.cypher.internal.logical.plans.LetSelectOrSemiApply
import org.neo4j.cypher.internal.logical.plans.LetSemiApply
import org.neo4j.cypher.internal.logical.plans.Limit
import org.neo4j.cypher.internal.logical.plans.LoadCSV
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.Merge
import org.neo4j.cypher.internal.logical.plans.MultiNodeIndexSeek
import org.neo4j.cypher.internal.logical.plans.NodeByElementIdSeek
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
import org.neo4j.cypher.internal.logical.plans.NonPipelined
import org.neo4j.cypher.internal.logical.plans.NonPipelinedStreaming
import org.neo4j.cypher.internal.logical.plans.Optional
import org.neo4j.cypher.internal.logical.plans.OptionalExpand
import org.neo4j.cypher.internal.logical.plans.OrderedAggregation
import org.neo4j.cypher.internal.logical.plans.OrderedDistinct
import org.neo4j.cypher.internal.logical.plans.OrderedUnion
import org.neo4j.cypher.internal.logical.plans.PartialSort
import org.neo4j.cypher.internal.logical.plans.PartialTop
import org.neo4j.cypher.internal.logical.plans.PartitionedAllNodesScan
import org.neo4j.cypher.internal.logical.plans.PartitionedDirectedAllRelationshipsScan
import org.neo4j.cypher.internal.logical.plans.PartitionedDirectedRelationshipIndexScan
import org.neo4j.cypher.internal.logical.plans.PartitionedDirectedRelationshipIndexSeek
import org.neo4j.cypher.internal.logical.plans.PartitionedDirectedRelationshipTypeScan
import org.neo4j.cypher.internal.logical.plans.PartitionedDirectedUnionRelationshipTypesScan
import org.neo4j.cypher.internal.logical.plans.PartitionedIntersectionNodeByLabelsScan
import org.neo4j.cypher.internal.logical.plans.PartitionedNodeByLabelScan
import org.neo4j.cypher.internal.logical.plans.PartitionedNodeIndexScan
import org.neo4j.cypher.internal.logical.plans.PartitionedNodeIndexSeek
import org.neo4j.cypher.internal.logical.plans.PartitionedSubtractionNodeByLabelsScan
import org.neo4j.cypher.internal.logical.plans.PartitionedUndirectedAllRelationshipsScan
import org.neo4j.cypher.internal.logical.plans.PartitionedUndirectedRelationshipIndexScan
import org.neo4j.cypher.internal.logical.plans.PartitionedUndirectedRelationshipIndexSeek
import org.neo4j.cypher.internal.logical.plans.PartitionedUndirectedRelationshipTypeScan
import org.neo4j.cypher.internal.logical.plans.PartitionedUndirectedUnionRelationshipTypesScan
import org.neo4j.cypher.internal.logical.plans.PartitionedUnionNodeByLabelsScan
import org.neo4j.cypher.internal.logical.plans.PartitionedUnwindCollection
import org.neo4j.cypher.internal.logical.plans.Prober
import org.neo4j.cypher.internal.logical.plans.ProcedureCall
import org.neo4j.cypher.internal.logical.plans.ProduceResult
import org.neo4j.cypher.internal.logical.plans.ProjectEndpoints
import org.neo4j.cypher.internal.logical.plans.Projection
import org.neo4j.cypher.internal.logical.plans.PruningVarExpand
import org.neo4j.cypher.internal.logical.plans.RelationshipCountFromCountStore
import org.neo4j.cypher.internal.logical.plans.RemoteBatchProperties
import org.neo4j.cypher.internal.logical.plans.RemoveLabels
import org.neo4j.cypher.internal.logical.plans.RightOuterHashJoin
import org.neo4j.cypher.internal.logical.plans.RollUpApply
import org.neo4j.cypher.internal.logical.plans.RunQueryAt
import org.neo4j.cypher.internal.logical.plans.SelectOrAntiSemiApply
import org.neo4j.cypher.internal.logical.plans.SelectOrSemiApply
import org.neo4j.cypher.internal.logical.plans.Selection
import org.neo4j.cypher.internal.logical.plans.SemiApply
import org.neo4j.cypher.internal.logical.plans.SetDynamicProperty
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
import org.neo4j.cypher.internal.logical.plans.ShowConstraints
import org.neo4j.cypher.internal.logical.plans.ShowFunctions
import org.neo4j.cypher.internal.logical.plans.ShowIndexes
import org.neo4j.cypher.internal.logical.plans.ShowProcedures
import org.neo4j.cypher.internal.logical.plans.ShowSettings
import org.neo4j.cypher.internal.logical.plans.ShowTransactions
import org.neo4j.cypher.internal.logical.plans.Skip
import org.neo4j.cypher.internal.logical.plans.Sort
import org.neo4j.cypher.internal.logical.plans.StatefulShortestPath
import org.neo4j.cypher.internal.logical.plans.SubqueryForeach
import org.neo4j.cypher.internal.logical.plans.SubtractionNodeByLabelsScan
import org.neo4j.cypher.internal.logical.plans.TerminateTransactions
import org.neo4j.cypher.internal.logical.plans.Top
import org.neo4j.cypher.internal.logical.plans.Top1WithTies
import org.neo4j.cypher.internal.logical.plans.Trail
import org.neo4j.cypher.internal.logical.plans.TransactionApply
import org.neo4j.cypher.internal.logical.plans.TransactionConcurrency
import org.neo4j.cypher.internal.logical.plans.TransactionForeach
import org.neo4j.cypher.internal.logical.plans.TriadicSelection
import org.neo4j.cypher.internal.logical.plans.UndirectedAllRelationshipsScan
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipByElementIdSeek
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipByIdSeek
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipIndexContainsScan
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipIndexEndsWithScan
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipIndexScan
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipIndexSeek
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipTypeScan
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipUniqueIndexSeek
import org.neo4j.cypher.internal.logical.plans.UndirectedUnionRelationshipTypesScan
import org.neo4j.cypher.internal.logical.plans.Union
import org.neo4j.cypher.internal.logical.plans.UnionNodeByLabelsScan
import org.neo4j.cypher.internal.logical.plans.UnwindCollection
import org.neo4j.cypher.internal.logical.plans.ValueHashJoin
import org.neo4j.cypher.internal.logical.plans.VarExpand
import org.neo4j.cypher.internal.planner.spi.ReadTokenContext
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.ParameterMapping
import org.neo4j.cypher.internal.runtime.ProcedureCallMode
import org.neo4j.cypher.internal.runtime.QueryIndexRegistrator
import org.neo4j.cypher.internal.runtime.ast.ExpressionVariable
import org.neo4j.cypher.internal.runtime.interpreted.commands.CommandNFA
import org.neo4j.cypher.internal.runtime.interpreted.commands.KeyTokenResolver
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.ExpressionConverters
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.InterpretedCommandProjection
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.AggregationExpression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.CreateNode
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.CreateRelationship
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.DeleteOperation
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Literal
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.ReferenceByName
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.RemoveLabelsOperation
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.SideEffect
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.ValuePopulatingReferenceByName
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.Predicate
import org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands.ShowConstraintsCommand
import org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands.ShowFunctionsCommand
import org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands.ShowIndexesCommand
import org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands.ShowProceduresCommand
import org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands.ShowSettingsCommand
import org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands.ShowTransactionsCommand
import org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands.TerminateTransactionsCommand
import org.neo4j.cypher.internal.runtime.interpreted.pipes.AggregationPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.AllNodesScanPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.AllOrderedDistinctPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.AntiSemiApplyPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.ApplyPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.ArgumentPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.AssertSameNodePipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.AssertSameRelationshipPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.BFSPruningVarLengthExpandPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.CachePropertiesPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.CartesianProductPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.CommandPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.ConcurrentTransactionApplyLegacyPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.ConcurrentTransactionForeachLegacyPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.ConditionalApplyPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.CreateNodeCommand
import org.neo4j.cypher.internal.runtime.interpreted.pipes.CreatePipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.CreateRelationshipCommand
import org.neo4j.cypher.internal.runtime.interpreted.pipes.DeletePipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.DirectedAllRelationshipsScanPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.DirectedRelationshipByIdSeekPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.DirectedRelationshipIndexContainsScanPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.DirectedRelationshipIndexEndsWithScanPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.DirectedRelationshipIndexScanPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.DirectedRelationshipIndexSeekPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.DirectedRelationshipTypeScanPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.DirectedUnionRelationshipTypesScanPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.DistinctPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.EagerAggregationPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.EagerPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.EmptyResultPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.ErrorPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.ExhaustiveLimitPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.ExpandAllPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.ExpandIntoPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.FilterPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.ForeachApplyPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.ForeachPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.IndexSeekModeFactory
import org.neo4j.cypher.internal.runtime.interpreted.pipes.InputPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.IntersectionNodeByLabelsScanPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.LazyLabel
import org.neo4j.cypher.internal.runtime.interpreted.pipes.LazyPropertyKey
import org.neo4j.cypher.internal.runtime.interpreted.pipes.LazyType
import org.neo4j.cypher.internal.runtime.interpreted.pipes.LetSelectOrSemiApplyPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.LetSemiApplyPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.LimitPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.LoadCSVPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.LockingMergePipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.MergePipe
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
import org.neo4j.cypher.internal.runtime.interpreted.pipes.NonPipelinedStreamingTestPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.NonPipelinedTestPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.OptionalExpandAllPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.OptionalExpandIntoPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.OptionalPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.OrderedAggregationPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.OrderedDistinctPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.OrderedUnionPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.PartialSortPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.PartialTop1Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.PartialTopNPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.PipeMapper
import org.neo4j.cypher.internal.runtime.interpreted.pipes.ProberPipe
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
import org.neo4j.cypher.internal.runtime.interpreted.pipes.RunQueryAtPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.SelectOrSemiApplyPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.SemiApplyPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.SetDynamicPropertyOperation
import org.neo4j.cypher.internal.runtime.interpreted.pipes.SetLabelsOperation
import org.neo4j.cypher.internal.runtime.interpreted.pipes.SetNodePropertiesOperation
import org.neo4j.cypher.internal.runtime.interpreted.pipes.SetNodePropertyFromMapOperation
import org.neo4j.cypher.internal.runtime.interpreted.pipes.SetNodePropertyOperation
import org.neo4j.cypher.internal.runtime.interpreted.pipes.SetPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.SetPropertiesOperation
import org.neo4j.cypher.internal.runtime.interpreted.pipes.SetPropertyFromMapOperation
import org.neo4j.cypher.internal.runtime.interpreted.pipes.SetPropertyOperation
import org.neo4j.cypher.internal.runtime.interpreted.pipes.SetRelationshipPropertiesOperation
import org.neo4j.cypher.internal.runtime.interpreted.pipes.SetRelationshipPropertyFromMapOperation
import org.neo4j.cypher.internal.runtime.interpreted.pipes.SetRelationshipPropertyOperation
import org.neo4j.cypher.internal.runtime.interpreted.pipes.ShortestPathPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.SkipPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.SortPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.StatefulShortestPathPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.SubqueryForeachPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.SubtractionNodeByLabelsScanPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.TestPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Top1Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Top1WithTiesPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.TopNPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.TrailPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.TransactionApplyPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.TransactionForeachPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.TriadicSelectionPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.UndirectedAllRelationshipsScanPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.UndirectedRelationshipByIdSeekPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.UndirectedRelationshipIndexContainsScanPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.UndirectedRelationshipIndexEndsWithScanPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.UndirectedRelationshipIndexScanPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.UndirectedRelationshipIndexSeekPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.UndirectedRelationshipTypeScanPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.UndirectedUnionRelationshipTypesScanPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.UnionNodeByLabelsScanPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.UnionPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.UnwindPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.ValueHashJoinPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.VarLengthExpandPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.VarLengthPredicate
import org.neo4j.cypher.internal.runtime.interpreted.pipes.aggregation.GroupingAggTable
import org.neo4j.cypher.internal.runtime.interpreted.pipes.aggregation.NonGroupingAggTable
import org.neo4j.cypher.internal.runtime.interpreted.pipes.aggregation.OrderedGroupingAggTable
import org.neo4j.cypher.internal.runtime.interpreted.pipes.aggregation.OrderedNonGroupingAggTable
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.Eagerly
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.exceptions.InternalException
import org.neo4j.internal.kernel.api.helpers.traversal.SlotOrName
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.VirtualNodeValue
import org.neo4j.values.virtual.VirtualRelationshipValue

/**
 * Responsible for turning a logical plan with argument pipes into a new pipe.
 * When adding new Pipes and LogicalPlans, this is where you should be looking.
 */
case class InterpretedPipeMapper(
  cypherVersion: CypherVersion,
  readOnly: Boolean,
  expressionConverters: ExpressionConverters,
  tokenContext: ReadTokenContext,
  indexRegistrator: QueryIndexRegistrator,
  anonymousVariableNameGenerator: AnonymousVariableNameGenerator,
  isCommunity: Boolean,
  parameterMapping: ParameterMapping
)(implicit semanticTable: SemanticTable) extends PipeMapper {

  private def getBuildExpression(id: Id): internal.expressions.Expression => Expression =
    ((e: internal.expressions.Expression) => expressionConverters.toCommandExpression(id, e)) andThen
      (expression => expression.rewrite(KeyTokenResolver.resolveExpressions(_, tokenContext)))

  override def onLeaf(plan: LogicalPlan): Pipe = {
    val id = plan.id
    val buildExpression = getBuildExpression(id)
    plan match {
      case Argument(_) =>
        ArgumentPipe()(id)

      case AllNodesScan(ident, _) =>
        AllNodesScanPipe(ident.name)(id = id)

      // Note: this plan shouldn't really be used here, but having it mapped here helps
      //      fallback and makes testing easier
      case PartitionedAllNodesScan(ident, _) =>
        AllNodesScanPipe(ident.name)(id = id)

      case NodeCountFromCountStore(ident, labels, _) =>
        NodeCountFromCountStorePipe(ident.name, labels.map(l => l.map(LazyLabel.apply)))(id = id)

      case RelationshipCountFromCountStore(ident, startLabel, typeNames, endLabel, _) =>
        RelationshipCountFromCountStorePipe(
          ident.name,
          startLabel.map(LazyLabel.apply(_)),
          RelationshipTypes(typeNames.map(_.name).toArray, tokenContext),
          endLabel.map(LazyLabel.apply(_))
        )(id = id)

      case NodeByLabelScan(ident, label, _, indexOrder) =>
        indexRegistrator.registerLabelScan()
        NodeByLabelScanPipe(ident.name, LazyLabel(label), indexOrder)(id = id)

      // Note: this plan shouldn't really be used here, but having it mapped here helps
      //      fallback and makes testing easier
      case PartitionedNodeByLabelScan(ident, label, _) =>
        indexRegistrator.registerLabelScan()
        NodeByLabelScanPipe(ident.name, LazyLabel(label), IndexOrderNone)(id = id)

      case UnionNodeByLabelsScan(ident, labels, _, indexOrder) =>
        indexRegistrator.registerLabelScan()
        UnionNodeByLabelsScanPipe(ident.name, labels.map(l => LazyLabel(l)), indexOrder)(id = id)

      case PartitionedUnionNodeByLabelsScan(ident, labels, _) =>
        indexRegistrator.registerLabelScan()
        UnionNodeByLabelsScanPipe(ident.name, labels.map(l => LazyLabel(l)), IndexOrderNone)(id = id)

      case IntersectionNodeByLabelsScan(ident, labels, _, indexOrder) =>
        indexRegistrator.registerLabelScan()
        IntersectionNodeByLabelsScanPipe(ident.name, labels.map(l => LazyLabel(l)), indexOrder)(id = id)

      case PartitionedIntersectionNodeByLabelsScan(ident, labels, _) =>
        indexRegistrator.registerLabelScan()
        IntersectionNodeByLabelsScanPipe(ident.name, labels.map(l => LazyLabel(l)), IndexOrderNone)(id = id)

      case SubtractionNodeByLabelsScan(ident, positiveLabels, negativeLabels, _, indexOrder) =>
        indexRegistrator.registerLabelScan()
        SubtractionNodeByLabelsScanPipe(
          ident.name,
          positiveLabels.map(l => LazyLabel(l)),
          negativeLabels.map(l => LazyLabel(l)),
          indexOrder
        )(id =
          id
        )

      case PartitionedSubtractionNodeByLabelsScan(ident, positiveLabels, negativeLabels, _) =>
        indexRegistrator.registerLabelScan()
        SubtractionNodeByLabelsScanPipe(
          ident.name,
          positiveLabels.map(l => LazyLabel(l)),
          negativeLabels.map(l => LazyLabel(l)),
          IndexOrderNone
        )(id =
          id
        )

      case NodeByIdSeek(ident, nodeIdExpr, _) =>
        NodeByIdSeekPipe(ident.name, expressionConverters.toCommandSeekArgs(id, nodeIdExpr))(id = id)

      case NodeByElementIdSeek(ident, nodeIdExpr, _) =>
        NodeByIdSeekPipe(ident.name, expressionConverters.toCommandElementIdSeekArgs(id, nodeIdExpr, NODE_TYPE))(id =
          id
        )

      case DirectedRelationshipByIdSeek(ident, relIdExpr, fromNode, toNode, _) =>
        DirectedRelationshipByIdSeekPipe(
          ident.name,
          expressionConverters.toCommandSeekArgs(id, relIdExpr),
          toNode.name,
          fromNode.name
        )(id = id)

      case DirectedRelationshipByElementIdSeek(ident, relIdExpr, fromNode, toNode, _) =>
        DirectedRelationshipByIdSeekPipe(
          ident.name,
          expressionConverters.toCommandElementIdSeekArgs(id, relIdExpr, RELATIONSHIP_TYPE),
          toNode.name,
          fromNode.name
        )(id = id)

      case UndirectedRelationshipByIdSeek(ident, relIdExpr, fromNode, toNode, _) =>
        UndirectedRelationshipByIdSeekPipe(
          ident.name,
          expressionConverters.toCommandSeekArgs(id, relIdExpr),
          toNode.name,
          fromNode.name
        )(id = id)

      case UndirectedRelationshipByElementIdSeek(ident, relIdExpr, fromNode, toNode, _) =>
        UndirectedRelationshipByIdSeekPipe(
          ident.name,
          expressionConverters.toCommandElementIdSeekArgs(id, relIdExpr, RELATIONSHIP_TYPE),
          toNode.name,
          fromNode.name
        )(id = id)

      case DirectedAllRelationshipsScan(ident, fromNode, toNode, _) =>
        DirectedAllRelationshipsScanPipe(ident.name, fromNode.name, toNode.name)(id = id)

      case UndirectedAllRelationshipsScan(ident, fromNode, toNode, _) =>
        UndirectedAllRelationshipsScanPipe(ident.name, fromNode.name, toNode.name)(id = id)

      case PartitionedDirectedAllRelationshipsScan(ident, fromNode, toNode, _) =>
        DirectedAllRelationshipsScanPipe(ident.name, fromNode.name, toNode.name)(id = id)

      case PartitionedUndirectedAllRelationshipsScan(ident, fromNode, toNode, _) =>
        UndirectedAllRelationshipsScanPipe(ident.name, fromNode.name, toNode.name)(id = id)

      case DirectedRelationshipTypeScan(ident, fromNode, typ, toNode, _, indexOrder) =>
        indexRegistrator.registerTypeScan()
        DirectedRelationshipTypeScanPipe(
          ident.name,
          fromNode.name,
          LazyType(typ)(semanticTable),
          toNode.name,
          indexOrder
        )(id = id)

      case UndirectedRelationshipTypeScan(ident, fromNode, typ, toNode, _, indexOrder) =>
        indexRegistrator.registerTypeScan()
        UndirectedRelationshipTypeScanPipe(
          ident.name,
          fromNode.name,
          LazyType(typ)(semanticTable),
          toNode.name,
          indexOrder
        )(id = id)

      case PartitionedDirectedRelationshipTypeScan(ident, fromNode, typ, toNode, _) =>
        indexRegistrator.registerTypeScan()
        DirectedRelationshipTypeScanPipe(
          ident.name,
          fromNode.name,
          LazyType(typ)(semanticTable),
          toNode.name,
          IndexOrderNone
        )(id = id)

      case PartitionedUndirectedRelationshipTypeScan(ident, fromNode, typ, toNode, _) =>
        indexRegistrator.registerTypeScan()
        UndirectedRelationshipTypeScanPipe(
          ident.name,
          fromNode.name,
          LazyType(typ)(semanticTable),
          toNode.name,
          IndexOrderNone
        )(id = id)

      case DirectedUnionRelationshipTypesScan(ident, fromNode, types, endNode, _, indexOrder) =>
        indexRegistrator.registerTypeScan()
        DirectedUnionRelationshipTypesScanPipe(
          ident.name,
          fromNode.name,
          types.map(l => LazyType(l)(semanticTable)),
          endNode.name,
          indexOrder
        )(id = id)

      case UndirectedUnionRelationshipTypesScan(ident, fromNode, types, endNode, _, indexOrder) =>
        indexRegistrator.registerTypeScan()
        UndirectedUnionRelationshipTypesScanPipe(
          ident.name,
          fromNode.name,
          types.map(l => LazyType(l)(semanticTable)),
          endNode.name,
          indexOrder
        )(id = id)

      case PartitionedDirectedUnionRelationshipTypesScan(ident, fromNode, types, endNode, _) =>
        indexRegistrator.registerTypeScan()
        DirectedUnionRelationshipTypesScanPipe(
          ident.name,
          fromNode.name,
          types.map(l => LazyType(l)(semanticTable)),
          endNode.name,
          IndexOrderNone
        )(id = id)

      case PartitionedUndirectedUnionRelationshipTypesScan(ident, fromNode, types, endNode, _) =>
        indexRegistrator.registerTypeScan()
        UndirectedUnionRelationshipTypesScanPipe(
          ident.name,
          fromNode.name,
          types.map(l => LazyType(l)(semanticTable)),
          endNode.name,
          IndexOrderNone
        )(id = id)

      case DirectedRelationshipUniqueIndexSeek(
          idName,
          startNode,
          endNode,
          typeToken,
          properties,
          valueExpr,
          _,
          indexOrder,
          indexType
        ) =>
        val indexSeekMode = IndexSeekModeFactory(unique = true, readOnly = readOnly).fromQueryExpression(valueExpr)
        DirectedRelationshipIndexSeekPipe(
          idName.name,
          startNode.name,
          endNode.name,
          typeToken,
          properties.toArray,
          indexRegistrator.registerQueryIndex(indexType, typeToken, properties),
          valueExpr.map(buildExpression),
          indexSeekMode,
          indexOrder
        )(id = id)

      case DirectedRelationshipIndexSeek(
          idName,
          startNode,
          endNode,
          typeToken,
          properties,
          valueExpr,
          _,
          indexOrder,
          indexType,
          _
        ) =>
        val indexSeekMode = IndexSeekModeFactory(unique = false, readOnly = readOnly).fromQueryExpression(valueExpr)
        DirectedRelationshipIndexSeekPipe(
          idName.name,
          startNode.name,
          endNode.name,
          typeToken,
          properties.toArray,
          indexRegistrator.registerQueryIndex(indexType, typeToken, properties),
          valueExpr.map(buildExpression),
          indexSeekMode,
          indexOrder
        )(id = id)

      case PartitionedDirectedRelationshipIndexSeek(
          idName,
          startNode,
          endNode,
          typeToken,
          properties,
          valueExpr,
          _,
          indexType
        ) =>
        val indexSeekMode = IndexSeekModeFactory(unique = false, readOnly = readOnly).fromQueryExpression(valueExpr)
        DirectedRelationshipIndexSeekPipe(
          idName.name,
          startNode.name,
          endNode.name,
          typeToken,
          properties.toArray,
          indexRegistrator.registerQueryIndex(indexType, typeToken, properties),
          valueExpr.map(buildExpression),
          indexSeekMode,
          IndexOrderNone
        )(id = id)

      case UndirectedRelationshipUniqueIndexSeek(
          idName,
          startNode,
          endNode,
          typeToken,
          properties,
          valueExpr,
          _,
          indexOrder,
          indexType
        ) =>
        val indexSeekMode = IndexSeekModeFactory(unique = true, readOnly = readOnly).fromQueryExpression(valueExpr)
        UndirectedRelationshipIndexSeekPipe(
          idName.name,
          startNode.name,
          endNode.name,
          typeToken,
          properties.toArray,
          indexRegistrator.registerQueryIndex(indexType, typeToken, properties),
          valueExpr.map(buildExpression),
          indexSeekMode,
          indexOrder
        )(id = id)

      case UndirectedRelationshipIndexSeek(
          idName,
          startNode,
          endNode,
          typeToken,
          properties,
          valueExpr,
          _,
          indexOrder,
          indexType,
          _
        ) =>
        val indexSeekMode = IndexSeekModeFactory(unique = false, readOnly = readOnly).fromQueryExpression(valueExpr)
        UndirectedRelationshipIndexSeekPipe(
          idName.name,
          startNode.name,
          endNode.name,
          typeToken,
          properties.toArray,
          indexRegistrator.registerQueryIndex(indexType, typeToken, properties),
          valueExpr.map(buildExpression),
          indexSeekMode,
          indexOrder
        )(id = id)

      case PartitionedUndirectedRelationshipIndexSeek(
          idName,
          startNode,
          endNode,
          typeToken,
          properties,
          valueExpr,
          _,
          indexType
        ) =>
        val indexSeekMode = IndexSeekModeFactory(unique = false, readOnly = readOnly).fromQueryExpression(valueExpr)
        UndirectedRelationshipIndexSeekPipe(
          idName.name,
          startNode.name,
          endNode.name,
          typeToken,
          properties.toArray,
          indexRegistrator.registerQueryIndex(indexType, typeToken, properties),
          valueExpr.map(buildExpression),
          indexSeekMode,
          IndexOrderNone
        )(id = id)

      case DirectedRelationshipIndexScan(
          idName,
          startNode,
          endNode,
          typeToken,
          properties,
          _,
          indexOrder,
          indexType,
          _
        ) =>
        DirectedRelationshipIndexScanPipe(
          idName.name,
          startNode.name,
          endNode.name,
          typeToken,
          properties.toArray,
          indexRegistrator.registerQueryIndex(indexType, typeToken, properties),
          indexOrder
        )(id = id)

      case UndirectedRelationshipIndexScan(
          idName,
          startNode,
          endNode,
          typeToken,
          properties,
          _,
          indexOrder,
          indexType,
          _
        ) =>
        UndirectedRelationshipIndexScanPipe(
          idName.name,
          startNode.name,
          endNode.name,
          typeToken,
          properties.toArray,
          indexRegistrator.registerQueryIndex(indexType, typeToken, properties),
          indexOrder
        )(id = id)
      case PartitionedDirectedRelationshipIndexScan(idName, startNode, endNode, typeToken, properties, _, indexType) =>
        DirectedRelationshipIndexScanPipe(
          idName.name,
          startNode.name,
          endNode.name,
          typeToken,
          properties.toArray,
          indexRegistrator.registerQueryIndex(indexType, typeToken, properties),
          IndexOrderNone
        )(id = id)

      case PartitionedUndirectedRelationshipIndexScan(
          idName,
          startNode,
          endNode,
          typeToken,
          properties,
          _,
          indexType
        ) =>
        UndirectedRelationshipIndexScanPipe(
          idName.name,
          startNode.name,
          endNode.name,
          typeToken,
          properties.toArray,
          indexRegistrator.registerQueryIndex(indexType, typeToken, properties),
          IndexOrderNone
        )(id = id)

      case DirectedRelationshipIndexContainsScan(
          idName,
          startNode,
          endNode,
          typeToken,
          property,
          valueExpr,
          _,
          indexOrder,
          indexType
        ) =>
        DirectedRelationshipIndexContainsScanPipe(
          idName.name,
          startNode.name,
          endNode.name,
          typeToken,
          property,
          indexRegistrator.registerQueryIndex(indexType, typeToken, property),
          buildExpression(valueExpr),
          indexOrder
        )(id = id)

      case UndirectedRelationshipIndexContainsScan(
          idName,
          startNode,
          endNode,
          typeToken,
          property,
          valueExpr,
          _,
          indexOrder,
          indexType
        ) =>
        UndirectedRelationshipIndexContainsScanPipe(
          idName.name,
          startNode.name,
          endNode.name,
          typeToken,
          property,
          indexRegistrator.registerQueryIndex(indexType, typeToken, property),
          buildExpression(valueExpr),
          indexOrder
        )(id = id)

      case DirectedRelationshipIndexEndsWithScan(
          idName,
          startNode,
          endNode,
          typeToken,
          property,
          valueExpr,
          _,
          indexOrder,
          indexType
        ) =>
        DirectedRelationshipIndexEndsWithScanPipe(
          idName.name,
          startNode.name,
          endNode.name,
          typeToken,
          property,
          indexRegistrator.registerQueryIndex(indexType, typeToken, property),
          buildExpression(valueExpr),
          indexOrder
        )(id = id)

      case UndirectedRelationshipIndexEndsWithScan(
          idName,
          startNode,
          endNode,
          typeToken,
          property,
          valueExpr,
          _,
          indexOrder,
          indexType
        ) =>
        UndirectedRelationshipIndexEndsWithScanPipe(
          idName.name,
          startNode.name,
          endNode.name,
          typeToken,
          property,
          indexRegistrator.registerQueryIndex(indexType, typeToken, property),
          buildExpression(valueExpr),
          indexOrder
        )(id = id)

      case NodeIndexSeek(ident, label, properties, valueExpr, _, indexOrder, indexType, _) =>
        val indexSeekMode = IndexSeekModeFactory(unique = false, readOnly = readOnly).fromQueryExpression(valueExpr)
        NodeIndexSeekPipe(
          ident.name,
          label,
          properties.toArray,
          indexRegistrator.registerQueryIndex(indexType, label, properties),
          valueExpr.map(buildExpression),
          indexSeekMode,
          indexOrder
        )(id = id)

      case PartitionedNodeIndexSeek(ident, label, properties, valueExpr, _, indexType) =>
        val indexSeekMode = IndexSeekModeFactory(unique = false, readOnly = readOnly).fromQueryExpression(valueExpr)
        NodeIndexSeekPipe(
          ident.name,
          label,
          properties.toArray,
          indexRegistrator.registerQueryIndex(indexType, label, properties),
          valueExpr.map(buildExpression),
          indexSeekMode,
          IndexOrderNone
        )(id = id)

      case NodeUniqueIndexSeek(ident, label, properties, valueExpr, _, indexOrder, indexType, _) =>
        val indexSeekMode = IndexSeekModeFactory(unique = true, readOnly = readOnly).fromQueryExpression(valueExpr)
        NodeIndexSeekPipe(
          ident.name,
          label,
          properties.toArray,
          indexRegistrator.registerQueryIndex(indexType, label, properties),
          valueExpr.map(buildExpression),
          indexSeekMode,
          indexOrder
        )(id = id)

      case NodeIndexScan(ident, label, properties, _, indexOrder, indexType, _) =>
        NodeIndexScanPipe(
          ident.name,
          label,
          properties,
          indexRegistrator.registerQueryIndex(indexType, label, properties),
          indexOrder
        )(id = id)

      case PartitionedNodeIndexScan(ident, label, properties, _, indexType) =>
        NodeIndexScanPipe(
          ident.name,
          label,
          properties,
          indexRegistrator.registerQueryIndex(indexType, label, properties),
          IndexOrderNone
        )(id = id)

      case NodeIndexContainsScan(ident, label, property, valueExpr, _, indexOrder, indexType) =>
        NodeIndexContainsScanPipe(
          ident.name,
          label,
          property,
          indexRegistrator.registerQueryIndex(indexType, label, property),
          buildExpression(valueExpr),
          indexOrder
        )(id = id)

      case NodeIndexEndsWithScan(ident, label, property, valueExpr, _, indexOrder, indexType) =>
        NodeIndexEndsWithScanPipe(
          ident.name,
          label,
          property,
          indexRegistrator.registerQueryIndex(indexType, label, property),
          buildExpression(valueExpr),
          indexOrder
        )(id = id)

      case ShowIndexes(indexType, columns, yields, _) =>
        CommandPipe(ShowIndexesCommand(indexType, columns, yields))(id)

      case ShowConstraints(constraintType, columns, yields, _) =>
        CommandPipe(ShowConstraintsCommand(constraintType, columns, yields))(id)

      case ShowProcedures(executableBy, columns, yields, _) =>
        CommandPipe(ShowProceduresCommand(executableBy, columns, yields, isCommunity, toKernelScope(cypherVersion)))(id)

      case ShowFunctions(functionType, executableBy, columns, yields, _) =>
        CommandPipe(ShowFunctionsCommand(
          functionType,
          executableBy,
          columns,
          yields,
          isCommunity,
          toKernelScope(cypherVersion)
        ))(id)

      case ShowTransactions(ids, columns, yields, _) =>
        val newIds = ids match {
          case Right(e) => Right(buildExpression(e))
          case Left(l)  => Left(l)
        }
        CommandPipe(ShowTransactionsCommand(newIds, columns, yields))(id)

      case TerminateTransactions(ids, columns, yields, _) =>
        val newIds = ids match {
          case Right(e) => Right(buildExpression(e))
          case Left(l)  => Left(l)
        }
        CommandPipe(TerminateTransactionsCommand(newIds, columns, yields))(id)

      case ShowSettings(names, columns, yields, _) =>
        val newNames = names match {
          case Right(e) => Right(buildExpression(e))
          case Left(l)  => Left(l)
        }
        CommandPipe(ShowSettingsCommand(newNames, columns, yields))(id)

      // Currently used for testing only
      case MultiNodeIndexSeek(indexLeafPlans) =>
        indexLeafPlans.foldLeft(None: Option[Pipe]) {
          case (None, plan) =>
            Some(onLeaf(plan))
          case (Some(pipe), plan) =>
            Some(CartesianProductPipe(pipe, onLeaf(plan))(id = id))
        }.get

      case Input(nodes, relationships, variables, _) =>
        InputPipe((nodes ++ relationships ++ variables).map(_.name).toArray)(id = id)

      case x =>
        throw new InternalException(s"Received a logical plan that has no physical operator $x")
    }
  }

  override def onOneChildPlan(plan: LogicalPlan, source: Pipe): Pipe = {
    val id = plan.id
    val buildExpression = getBuildExpression(id)

    def compileEffects(sideEffect: SimpleMutatingPattern): Seq[SideEffect] = {
      sideEffect match {
        case CreatePattern(commands) =>
          commands.map {
            case org.neo4j.cypher.internal.ir.CreateNode(node, labels, properties) =>
              CreateNode(
                CreateNodeCommand(node.name, labels.toSeq.map(LazyLabel.apply), properties.map(buildExpression)),
                allowNullOrNaNProperty = true
              )
            case r: org.neo4j.cypher.internal.ir.CreateRelationship =>
              CreateRelationship(
                CreateRelationshipCommand(
                  r.variable.name,
                  r.startNode.name,
                  LazyType(r.relType)(semanticTable),
                  r.endNode.name,
                  r.properties.map(buildExpression)
                ),
                allowNullOrNaNProperty = true
              )
          }

        case org.neo4j.cypher.internal.ir.DeleteExpression(expression, forced) =>
          Seq(DeleteOperation(buildExpression(expression), forced))
        case SetLabelPattern(node, labelNames) => Seq(SetLabelsOperation(node.name, labelNames.map(LazyLabel.apply)))
        case RemoveLabelPattern(node, labelNames) =>
          Seq(RemoveLabelsOperation(node.name, labelNames.map(LazyLabel.apply)))
        case SetNodePropertyPattern(node, propertyKey, value) =>
          val needsExclusiveLock =
            internal.expressions.Expression.hasPropertyReadDependency(node, value, propertyKey)
          Seq(SetNodePropertyOperation(
            node.name,
            LazyPropertyKey(propertyKey),
            buildExpression(value),
            needsExclusiveLock
          ))
        case SetNodePropertiesPattern(node, items) =>
          val needsExclusiveLock = items.exists {
            case (p, e) => internal.expressions.Expression.hasPropertyReadDependency(node, e, p)
          }
          val size = items.size
          val keys = new Array[LazyPropertyKey](size)
          val values = new Array[Expression](size)
          items.zipWithIndex.foreach {
            case ((k, e), i) =>
              keys(i) = LazyPropertyKey(k)
              values(i) = buildExpression(e)
          }

          Seq(SetNodePropertiesOperation(node.name, keys, values, needsExclusiveLock))
        case SetNodePropertiesFromMapPattern(node, map, removeOtherProps) =>
          val needsExclusiveLock =
            internal.expressions.Expression.mapExpressionHasPropertyReadDependency(node, map)
          Seq(SetNodePropertyFromMapOperation(node.name, buildExpression(map), removeOtherProps, needsExclusiveLock))
        case SetRelationshipPropertyPattern(relationship, propertyKey, value) =>
          val needsExclusiveLock =
            internal.expressions.Expression.hasPropertyReadDependency(relationship, value, propertyKey)
          Seq(SetRelationshipPropertyOperation(
            relationship.name,
            LazyPropertyKey(propertyKey),
            buildExpression(value),
            needsExclusiveLock
          ))
        case SetRelationshipPropertiesPattern(rel, items) =>
          val needsExclusiveLock = items.exists {
            case (p, e) => internal.expressions.Expression.hasPropertyReadDependency(rel, e, p)
          }
          val size = items.size
          val keys = new Array[LazyPropertyKey](size)
          val values = new Array[Expression](size)
          items.zipWithIndex.foreach {
            case ((k, e), i) =>
              keys(i) = LazyPropertyKey(k)
              values(i) = buildExpression(e)
          }

          Seq(SetRelationshipPropertiesOperation(rel.name, keys, values, needsExclusiveLock))
        case SetRelationshipPropertiesFromMapPattern(relationship, map, removeOtherProps) =>
          val needsExclusiveLock =
            internal.expressions.Expression.mapExpressionHasPropertyReadDependency(relationship, map)
          Seq(SetRelationshipPropertyFromMapOperation(
            relationship.name,
            buildExpression(map),
            removeOtherProps,
            needsExclusiveLock
          ))
        case SetPropertyPattern(entityExpression, propertyKeyName, expression) =>
          Seq(SetPropertyOperation(
            buildExpression(entityExpression),
            LazyPropertyKey(propertyKeyName),
            buildExpression(expression)
          ))
        case SetPropertiesPattern(entityExpression, items) =>
          val size = items.size
          val keys = new Array[LazyPropertyKey](size)
          val values = new Array[Expression](size)
          items.zipWithIndex.foreach {
            case ((k, e), i) =>
              keys(i) = LazyPropertyKey(k)
              values(i) = buildExpression(e)
          }
          Seq(SetPropertiesOperation(buildExpression(entityExpression), keys, values))
        case SetPropertiesFromMapPattern(entityExpression, expression, removeOtherProps) =>
          Seq(SetPropertyFromMapOperation(
            buildExpression(entityExpression),
            buildExpression(expression),
            removeOtherProps
          ))

        case other => throw new IllegalStateException(s"Cannot compile $other")
      }
    }

    plan match {
      case Projection(_, expressions) =>
        ProjectionPipe(
          source,
          InterpretedCommandProjection(Eagerly.immutableMapValues(
            expressions.view.map { case (key, value) => key.name -> value },
            buildExpression
          ))
        )(
          id = id
        )

      case ProjectEndpoints(_, rel, start, startInScope, end, endInScope, types, direction, length) =>
        ProjectEndpointsPipe(
          source,
          rel.name,
          start.name,
          startInScope,
          end.name,
          endInScope,
          RelationshipTypes(types.toArray),
          direction,
          length.isSimple
        )(id = id)

      case EmptyResult(_) =>
        EmptyResultPipe(source)(id = id)

      case NonFuseable(_) =>
        TestPipe(source)(id = id)

      case InjectCompilationError(_) =>
        TestPipe(source)(id = id)

      case NonPipelined(_) =>
        NonPipelinedTestPipe(source)(id = id)

      case NonPipelinedStreaming(_, expandFactor) =>
        NonPipelinedStreamingTestPipe(source, expandFactor)(id = id)

      case Prober(_, probe) =>
        ProberPipe(source, probe)(id = id)

      case Selection(predicate, _) =>
        val predicateExpression =
          if (predicate.exprs.size == 1) buildExpression(predicate.exprs.head) else buildExpression(predicate)
        FilterPipe(source, predicateExpression)(id = id)

      case CacheProperties(_, properties) =>
        val runtimeProperties = properties.toArray.map(buildExpression(_))
        CachePropertiesPipe(source, runtimeProperties)(id = id)

      case RemoteBatchProperties(_, _) =>
        source // TODO: implement

      case Expand(_, fromName, dir, types: Seq[internal.expressions.RelTypeName], toName, relName, ExpandAll) =>
        ExpandAllPipe(source, fromName.name, relName.name, toName.name, dir, RelationshipTypes(types.toArray))(id = id)

      case Expand(_, fromName, dir, types: Seq[internal.expressions.RelTypeName], toName, relName, ExpandInto) =>
        ExpandIntoPipe(source, fromName.name, relName.name, toName.name, dir, RelationshipTypes(types.toArray))(id = id)

      case OptionalExpand(_, fromName, dir, types, toName, relName, ExpandAll, predicate) =>
        OptionalExpandAllPipe(
          source,
          fromName.name,
          relName.name,
          toName.name,
          dir,
          RelationshipTypes(types.toArray),
          predicate.map(buildExpression)
        )(id = id)

      case OptionalExpand(_, fromName, dir, types, toName, relName, ExpandInto, predicate) =>
        OptionalExpandIntoPipe(
          source,
          fromName.name,
          relName.name,
          toName.name,
          dir,
          RelationshipTypes(types.toArray),
          predicate.map(buildExpression)
        )(id = id)

      case VarExpand(
          _,
          fromName,
          dir,
          projectedDir,
          types,
          toName,
          relName,
          VarPatternLength(min, max),
          expansionMode,
          nodePredicates,
          relationshipPredicates
        ) =>
        val predicate = varLengthPredicates(id, nodePredicates, relationshipPredicates)

        val nodeInScope = expansionMode match {
          case ExpandAll  => false
          case ExpandInto => true
        }

        VarLengthExpandPipe(
          source,
          fromName.name,
          relName.name,
          toName.name,
          dir,
          projectedDir,
          RelationshipTypes(types.toArray),
          min,
          max,
          nodeInScope,
          predicate
        )(id = id)

      case Optional(inner, protectedSymbols) =>
        OptionalPipe(inner.availableSymbols.map(_.name) -- protectedSymbols.map(_.name), source)(id = id)

      case PruningVarExpand(_, from, dir, types, toName, minLength, maxLength, nodePredicate, relationshipPredicate) =>
        val predicate = varLengthPredicates(id, nodePredicate, relationshipPredicate)
        PruningVarLengthExpandPipe(
          source,
          from.name,
          toName.name,
          RelationshipTypes(types.toArray),
          dir,
          minLength,
          maxLength,
          predicate
        )(id = id)

      case BFSPruningVarExpand(
          _,
          from,
          dir,
          types,
          to,
          includeStartNode,
          max,
          depthName,
          mode,
          nodePredicate,
          relationshipPredicate
        ) =>
        val predicate = varLengthPredicates(id, nodePredicate, relationshipPredicate)
        BFSPruningVarLengthExpandPipe(
          source,
          from.name,
          to.name,
          depthName.map(_.name),
          RelationshipTypes(types.toArray),
          dir,
          includeStartNode,
          max,
          mode,
          predicate
        )(id = id)

      case Sort(_, sortItems) =>
        SortPipe(source, InterpretedExecutionContextOrdering.asComparator(sortItems.map(translateColumnOrder)))(id = id)

      case PartialSort(_, alreadySortedPrefix, stillToSortSuffix, skipSortingPrefixLength) =>
        PartialSortPipe(
          source,
          InterpretedExecutionContextOrdering.asComparator(alreadySortedPrefix.map(translateColumnOrder)),
          InterpretedExecutionContextOrdering.asComparator(stillToSortSuffix.map(translateColumnOrder)),
          skipSortingPrefixLength.map(buildExpression)
        )(id = id)

      case Skip(_, count) =>
        SkipPipe(source, buildExpression(count))(id = id)

      case Top(_, sortItems, _) if sortItems.isEmpty => source

      case Top(_, sortItems, internal.expressions.SignedDecimalIntegerLiteral("1")) =>
        Top1Pipe(source, InterpretedExecutionContextOrdering.asComparator(sortItems.map(translateColumnOrder).toList))(
          id = id
        )

      case Top1WithTies(_, sortItems) =>
        Top1WithTiesPipe(
          source,
          InterpretedExecutionContextOrdering.asComparator(sortItems.map(translateColumnOrder).toList)
        )(id = id)

      case Top(_, sortItems, limit) =>
        TopNPipe(
          source,
          buildExpression(limit),
          InterpretedExecutionContextOrdering.asComparator(sortItems.map(translateColumnOrder).toList)
        )(id = id)

      case PartialTop(_, _, stillToSortSuffix, _, _) if stillToSortSuffix.isEmpty => source

      case PartialTop(
          _,
          alreadySortedPrefix,
          stillToSortSuffix,
          internal.expressions.SignedDecimalIntegerLiteral("1"),
          _
        ) =>
        PartialTop1Pipe(
          source,
          InterpretedExecutionContextOrdering.asComparator(alreadySortedPrefix.map(translateColumnOrder).toList),
          InterpretedExecutionContextOrdering.asComparator(stillToSortSuffix.map(translateColumnOrder).toList)
        )(id = id)

      case PartialTop(_, alreadySortedPrefix, stillToSortSuffix, limit, skipSortingPrefixLength) =>
        PartialTopNPipe(
          source,
          buildExpression(limit),
          skipSortingPrefixLength.map(buildExpression),
          InterpretedExecutionContextOrdering.asComparator(alreadySortedPrefix.map(translateColumnOrder).toList),
          InterpretedExecutionContextOrdering.asComparator(stillToSortSuffix.map(translateColumnOrder).toList)
        )(id = id)

      case Limit(_, count) =>
        LimitPipe(source, buildExpression(count))(id = id)

      case ExhaustiveLimit(_, count) =>
        ExhaustiveLimitPipe(source, buildExpression(count))(id = id)

      case Aggregation(_, groupingExpressions, aggregatingExpressions) if aggregatingExpressions.isEmpty =>
        val projection = groupingExpressions.map {
          case (key, value) => DistinctPipe.GroupingCol(key.name, buildExpression(value))
        }.toArray
        DistinctPipe(source, projection)(id = id)

      case Distinct(_, groupingExpressions) =>
        val projection = groupingExpressions.map {
          case (key, value) => DistinctPipe.GroupingCol(key.name, buildExpression(value))
        }.toArray
        DistinctPipe(source, projection)(id = id)

      case OrderedDistinct(_, groupingExpressions, orderToLeverage) =>
        val projection = groupingExpressions.map {
          case (key, value) =>
            DistinctPipe.GroupingCol(key.name, buildExpression(value), orderToLeverage.contains(value))
        }.toArray
        if (projection.forall(_.ordered)) {
          AllOrderedDistinctPipe(source, projection)(id = id)
        } else {
          OrderedDistinctPipe(source, projection)(id = id)
        }

      case OrderedAggregation(_, groupingExpressions, aggregatingExpressions, orderToLeverage)
        if aggregatingExpressions.isEmpty =>
        val projection = groupingExpressions.map {
          case (key, value) =>
            DistinctPipe.GroupingCol(key.name, buildExpression(value), orderToLeverage.contains(value))
        }.toArray
        OrderedDistinctPipe(source, projection)(id = id)

      case Aggregation(_, groupingExpressions, aggregatingExpressions) =>
        val aggregationColumns = aggregatingExpressions.map {
          case (key, value) =>
            AggregationPipe.AggregatingCol(key.name, buildExpression(value).asInstanceOf[AggregationExpression])
        }.toArray

        val tableFactory =
          if (groupingExpressions.isEmpty) {
            NonGroupingAggTable.Factory(aggregationColumns)
          } else {
            val groupingColumns = groupingExpressions.map {
              case (key, value) => DistinctPipe.GroupingCol(key.name, buildExpression(value))
            }.toArray
            val groupingFunction: (CypherRow, QueryState) => AnyValue =
              AggregationPipe.computeGroupingFunction(groupingColumns)
            GroupingAggTable.Factory(groupingColumns, groupingFunction, aggregationColumns)
          }
        EagerAggregationPipe(source, tableFactory)(id = id)

      case OrderedAggregation(_, groupingExpressions, aggregatingExpressions, orderToLeverage) =>
        val aggregationColumns = aggregatingExpressions.map {
          case (key, value) =>
            AggregationPipe.AggregatingCol(key.name, buildExpression(value).asInstanceOf[AggregationExpression])
        }.toArray
        val groupingColumns = groupingExpressions.map {
          case (key, value) =>
            DistinctPipe.GroupingCol(key.name, buildExpression(value), orderToLeverage.contains(value))
        }.toArray

        val (orderedGroupingColumns, unorderedGroupingColumns) = groupingColumns.partition(_.ordered)
        val unorderedGroupingFunction: (CypherRow, QueryState) => AnyValue =
          AggregationPipe.computeGroupingFunction(unorderedGroupingColumns)
        val orderedGroupingFunction: (CypherRow, QueryState) => AnyValue =
          AggregationPipe.computeGroupingFunction(orderedGroupingColumns)

        val tableFactory =
          if (groupingColumns.forall(_.ordered)) {
            OrderedNonGroupingAggTable.Factory(orderedGroupingFunction, orderedGroupingColumns, aggregationColumns)
          } else {
            OrderedGroupingAggTable.Factory(
              orderedGroupingFunction,
              orderedGroupingColumns,
              unorderedGroupingFunction,
              unorderedGroupingColumns,
              aggregationColumns
            )
          }
        OrderedAggregationPipe(source, tableFactory)(id = id)

      case FindShortestPaths(
          _,
          shortestPathPattern,
          perStepNodePredicates,
          perStepRelPredicates,
          pathPredicates,
          withFallBack,
          sameNodeMode
        ) =>
        val single = shortestPathPattern.expr.single

        val filteringStep = varLengthPredicates(id, perStepNodePredicates, perStepRelPredicates)
        val commandPathPredicates = pathPredicates.map(buildPredicate(id, _))

        val patternRelationship = shortestPathPattern.rel

        val (sourceNodeName, targetNodeName) = patternRelationship.boundaryNodes

        val rel = shortestPathPattern.expr.element match {
          case internal.expressions.RelationshipChain(_, relationshipPattern, _) =>
            relationshipPattern
          case _ =>
            throw new IllegalStateException("This should be caught during semantic checking")
        }

        val (allowZeroLength, maxDepth) = rel.length match {
          case Some(Some(internal.expressions.Range(lower, max))) =>
            (lower.exists(_.value == 0L), max.map(_.value.toInt))
          case None => (false, Some(1)) // non-varlength case
          case _    => (false, None)
        }

        if (!allowZeroLength && sameNodeMode == AllowSameNode && rel.direction == SemanticDirection.BOTH) {
          throw new IllegalArgumentException("We don't allow -[*1..]- for AllowSameNode")
        }

        val pathName = shortestPathPattern.maybePathVar.map(_.name).getOrElse(anonymousVariableNameGenerator.nextName)
        ShortestPathPipe(
          source,
          sourceNodeName.name,
          targetNodeName.name,
          pathName,
          rel.variable.get.name,
          RelationshipTypes(patternRelationship.types.toArray),
          patternRelationship.dir,
          filteringStep,
          commandPathPredicates,
          single,
          sameNodeMode,
          allowZeroLength,
          maxDepth,
          single && !withFallBack
        )(id)

      case StatefulShortestPath(
          _,
          sourceNode,
          targetNode,
          nfa,
          mode,
          nonInlinedPreFilters,
          nodeVariableGroupings,
          relationshipVariableGroupings,
          singletonNodeVariables,
          singletonRelationshipVariables,
          selector,
          _,
          reverseGroupVariableProjections,
          bounds
        ) =>
        def convertPredicate(varPred: VariablePredicate) =
          expressionConverters.toCommandPredicate(id, varPred.predicate)
            .rewriteAsPredicate(KeyTokenResolver.resolveExpressions(_, tokenContext))

        val groupMap = (nodeVariableGroupings ++ relationshipVariableGroupings)
          .map(grouping => grouping.singleton.name -> grouping.group.name)
          .toMap

        val singletonMap = (singletonNodeVariables ++ singletonRelationshipVariables)
          .map(mapping => mapping.nfaExprVar.name -> mapping.rowVar.name)
          .toMap

        def getName(variable: LogicalVariable): SlotOrName = {
          groupMap.get(variable.name) match {
            case Some(name) =>
              SlotOrName.VarName(name, isGroup = true)
            case None =>
              SlotOrName.VarName(singletonMap.getOrElse(variable.name, variable.name), isGroup = false)
          }
        }

        val commandNFA = CommandNFA.fromLogicalNFA(nfa, convertPredicate, getName)

        val commandPreFilters: Option[Predicate] = nonInlinedPreFilters.map(buildPredicate(id, _))

        val intoTargetName = mode match {
          case ExpandInto => Some(targetNode.name)
          case ExpandAll  => None
        }

        StatefulShortestPathPipe(
          source,
          sourceNode.name,
          intoTargetName,
          commandNFA,
          bounds,
          commandPreFilters,
          selector,
          groupMap.values.toSet,
          reverseGroupVariableProjections
        )(id = id)

      case UnwindCollection(_, variable, collection) =>
        UnwindPipe(source, buildExpression(collection), variable.name)(id = id)

      // Note: this plan shouldn't really be used here, but having it mapped here helps
      //      fallback and makes testing easier
      case PartitionedUnwindCollection(_, variable, collection) =>
        UnwindPipe(source, buildExpression(collection), variable.name)(id = id)

      case ProcedureCall(_, call @ ResolvedCall(signature, callArguments, _, _, _, _)) =>
        val callMode = ProcedureCallMode.fromAccessMode(signature.accessMode)
        val callArgumentCommands =
          callArguments.map(Some(_)).zipAll(signature.inputSignature.map(_.default), None, None).map {
            case (givenArg, default) => givenArg.map(buildExpression).getOrElse(Literal(default.get))
          }
        val rowProcessing = ProcedureCallRowProcessing(signature)
        ProcedureCallPipe(
          source,
          signature,
          callMode,
          callArgumentCommands,
          rowProcessing,
          call.callResultTypes,
          call.callResultIndices
        )(id = id)

      // `parameterMapping` already contains the parameters defined in `RunQueryAt`, we can ignore them here.
      // `importsAsParameters` need to be created here, to forward value imported in sub-queries to the component DB.
      case RunQueryAt(_, query, graph, _, importsAsParameters, columns) =>
        RunQueryAtPipe(
          source,
          query,
          expressionConverters.toCommandExpression(id, graph),
          importsAsParameters.view.mapValues(expressionConverters.toCommandExpression(id, _)).toMap,
          columns,
          parameterMapping
        )(id = id)

      case LoadCSV(_, url, variableName, format, fieldTerminator, legacyCsvQuoteEscaping, bufferSize) =>
        LoadCSVPipe(
          source,
          format,
          buildExpression(url),
          variableName.name,
          fieldTerminator,
          legacyCsvQuoteEscaping,
          bufferSize
        )(id = id)

      case ProduceResult(_, columns) =>
        val columnExpressions = columns.map(c =>
          c.variable -> c.cachedProperties.map(e => LazyPropertyKey(e.propertyKey)(semanticTable) -> buildExpression(e))
        )
        ProduceResultsPipe(source, createProjectionsForResult(columnExpressions).toArray)(id = id)

      case Create(_, commands) =>
        CreatePipe(
          source,
          commands.map {
            case n: org.neo4j.cypher.internal.ir.CreateNode =>
              CreateNodeCommand(n.variable.name, n.labels.toSeq.map(LazyLabel.apply), n.properties.map(buildExpression))
            case r: org.neo4j.cypher.internal.ir.CreateRelationship =>
              CreateRelationshipCommand(
                r.variable.name,
                r.startNode.name,
                LazyType(r.relType.name),
                r.endNode.name,
                r.properties.map(buildExpression)
              )
          }.toArray
        )(id = id)

      case Merge(_, createNodes, createRelationships, onMatch, onCreate, nodesToLock) =>
        val creates = createNodes.map {
          case org.neo4j.cypher.internal.ir.CreateNode(node, labels, properties) =>
            CreateNode(
              CreateNodeCommand(node.name, labels.toSeq.map(LazyLabel.apply), properties.map(buildExpression)),
              allowNullOrNaNProperty = false
            )
        } ++ createRelationships.map {
          (r: org.neo4j.cypher.internal.ir.CreateRelationship) =>
            CreateRelationship(
              CreateRelationshipCommand(
                r.variable.name,
                r.startNode.name,
                LazyType(r.relType)(semanticTable),
                r.endNode.name,
                r.properties.map(buildExpression)
              ),
              allowNullOrNaNProperty = false
            )
        }
        if (nodesToLock.isEmpty) new MergePipe(
          source,
          (creates ++ onCreate.flatMap(compileEffects)).toArray,
          onMatch.flatMap(compileEffects).toArray
        )(id = id)
        else new LockingMergePipe(
          source,
          (creates ++ onCreate.flatMap(compileEffects)).toArray,
          onMatch.flatMap(compileEffects).toArray,
          nodesToLock.map(_.name).toArray
        )(id = id)

      case SetLabels(_, name, labels) =>
        SetPipe(source, SetLabelsOperation(name.name, labels.toSeq.map(LazyLabel.apply)))(id = id)

      case SetNodeProperty(_, name, propertyKey, expression) =>
        val needsExclusiveLock =
          internal.expressions.Expression.hasPropertyReadDependency(name, expression, propertyKey)
        SetPipe(
          source,
          SetNodePropertyOperation(
            name.name,
            LazyPropertyKey(propertyKey),
            buildExpression(expression),
            needsExclusiveLock
          )
        )(id = id)

      case SetNodeProperties(_, name, items) =>
        val needsExclusiveLock = items.exists {
          case (p, e) => internal.expressions.Expression.hasPropertyReadDependency(name, e, p)
        }
        val size = items.size
        val keys = new Array[LazyPropertyKey](size)
        val values = new Array[Expression](size)
        items.zipWithIndex.foreach {
          case ((k, e), i) =>
            keys(i) = LazyPropertyKey(k)
            values(i) = buildExpression(e)
        }
        SetPipe(source, SetNodePropertiesOperation(name.name, keys, values, needsExclusiveLock))(id = id)

      case SetNodePropertiesFromMap(_, name, expression, removeOtherProps) =>
        val needsExclusiveLock =
          internal.expressions.Expression.mapExpressionHasPropertyReadDependency(name, expression)
        SetPipe(
          source,
          SetNodePropertyFromMapOperation(name.name, buildExpression(expression), removeOtherProps, needsExclusiveLock)
        )(id = id)

      case SetPropertiesFromMap(_, entityExpr, expression, removeOtherProps) =>
        SetPipe(
          source,
          SetPropertyFromMapOperation(buildExpression(entityExpr), buildExpression(expression), removeOtherProps)
        )(id = id)

      case SetRelationshipProperty(_, name, propertyKey, expression) =>
        val needsExclusiveLock =
          internal.expressions.Expression.hasPropertyReadDependency(name, expression, propertyKey)
        SetPipe(
          source,
          SetRelationshipPropertyOperation(
            name.name,
            LazyPropertyKey(propertyKey),
            buildExpression(expression),
            needsExclusiveLock
          )
        )(id = id)

      case SetRelationshipProperties(_, name, items) =>
        val needsExclusiveLock = items.exists {
          case (p, e) => internal.expressions.Expression.hasPropertyReadDependency(name, e, p)
        }
        val size = items.size
        val keys = new Array[LazyPropertyKey](size)
        val values = new Array[Expression](size)
        items.zipWithIndex.foreach {
          case ((k, e), i) =>
            keys(i) = LazyPropertyKey(k)
            values(i) = buildExpression(e)
        }
        SetPipe(source, SetRelationshipPropertiesOperation(name.name, keys, values, needsExclusiveLock))(id = id)

      case SetRelationshipPropertiesFromMap(_, name, expression, removeOtherProps) =>
        val needsExclusiveLock =
          internal.expressions.Expression.mapExpressionHasPropertyReadDependency(name, expression)
        SetPipe(
          source,
          SetRelationshipPropertyFromMapOperation(
            name.name,
            buildExpression(expression),
            removeOtherProps,
            needsExclusiveLock
          )
        )(id = id)

      case SetProperty(_, entityExpr, propertyKey, expression) =>
        SetPipe(
          source,
          SetPropertyOperation(
            buildExpression(entityExpr),
            LazyPropertyKey(propertyKey),
            buildExpression(expression)
          )
        )(id = id)

      case SetDynamicProperty(_, entity, property, value) =>
        SetPipe(
          source,
          SetDynamicPropertyOperation(
            buildExpression(entity),
            buildExpression(property),
            buildExpression(value)
          )
        )(id = id)

      case SetProperties(_, entityExpr, items) =>
        val size = items.size
        val keys = new Array[LazyPropertyKey](size)
        val values = new Array[Expression](size)
        items.zipWithIndex.foreach {
          case ((k, e), i) =>
            keys(i) = LazyPropertyKey(k)
            values(i) = buildExpression(e)
        }
        SetPipe(source, SetPropertiesOperation(buildExpression(entityExpr), keys, values))(id = id)

      case RemoveLabels(_, name, labels) =>
        RemoveLabelsPipe(source, name.name, labels.toSeq.map(LazyLabel.apply))(id = id)

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

      case Eager(_, _) =>
        EagerPipe(source)(id = id)

      case ErrorPlan(_, ex) =>
        ErrorPipe(source, ex)(id = id)

      case Foreach(_, variable, expression, mutations) =>
        ForeachPipe(source, variable.name, buildExpression(expression), mutations.flatMap(compileEffects).toArray)(id =
          id
        )

      case x =>
        throw new InternalException(s"Received a logical plan that has no physical operator $x")
    }
  }

  def asCommand(id: Id)(variablePredicate: VariablePredicate): (CypherRow, QueryState, AnyValue) => Boolean = {
    val command = buildPredicate(id, variablePredicate.predicate)
    val ev = ExpressionVariable.cast(variablePredicate.variable)

    (context: CypherRow, state: QueryState, entity: AnyValue) => {
      state.expressionVariables(ev.offset) = entity
      command.isTrue(context, state)
    }
  }

  def varLengthPredicates(
    id: Id,
    nodePredicates: Seq[VariablePredicate],
    relationshipPredicates: Seq[VariablePredicate]
  ): VarLengthPredicate = {

    // Creates commands out of the predicates

    (nodePredicates, relationshipPredicates) match {
      case (Seq(), Seq()) => VarLengthPredicate.NONE
      case _ =>
        val nodeCommands = nodePredicates.map(asCommand(id))
        val relCommands = relationshipPredicates.map(asCommand(id))

        new VarLengthPredicate {
          override def filterNode(row: CypherRow, state: QueryState)(node: VirtualNodeValue): Boolean =
            nodeCommands.forall(nodeCommand => nodeCommand(row, state, node))

          override def filterRelationship(row: CypherRow, state: QueryState)(rel: VirtualRelationshipValue): Boolean =
            relCommands.forall(relCommand => relCommand(row, state, rel))
        }
    }
  }

  override def onTwoChildPlan(plan: LogicalPlan, lhs: Pipe, rhs: Pipe): Pipe = {
    val id = plan.id
    val buildExpression = getBuildExpression(id)
    plan match {
      case CartesianProduct(_, _) =>
        CartesianProductPipe(lhs, rhs)(id = id)

      case NodeHashJoin(nodes, _, _) =>
        NodeHashJoinPipe(nodes.map(_.name), lhs, rhs)(id = id)

      case LeftOuterHashJoin(nodes, l, r) =>
        val nullableVariables = r.availableSymbols.map(_.name) -- l.availableSymbols.map(_.name)
        NodeLeftOuterHashJoinPipe(nodes.map(_.name), lhs, rhs, nullableVariables)(id = id)

      case RightOuterHashJoin(nodes, l, r) =>
        val nullableVariables = l.availableSymbols.map(_.name) -- r.availableSymbols.map(_.name)
        NodeRightOuterHashJoinPipe(nodes.map(_.name), lhs, rhs, nullableVariables)(id = id)

      case Apply(_, _) => ApplyPipe(lhs, rhs)(id = id)

      case AssertSameNode(node, _, _) =>
        AssertSameNodePipe(lhs, rhs, node.name)(id = id)

      case AssertSameRelationship(relationship, _, _) =>
        AssertSameRelationshipPipe(lhs, rhs, relationship.name)(id = id)

      case SemiApply(_, _) =>
        SemiApplyPipe(lhs, rhs)(id = id)

      case AntiSemiApply(_, _) =>
        AntiSemiApplyPipe(lhs, rhs)(id = id)

      case LetSemiApply(_, _, idName) =>
        LetSemiApplyPipe(lhs, rhs, idName.name, negated = false)(id = id)

      case LetAntiSemiApply(_, _, idName) =>
        LetSemiApplyPipe(lhs, rhs, idName.name, negated = true)(id = id)

      case SelectOrSemiApply(_, _, predicate) =>
        SelectOrSemiApplyPipe(lhs, rhs, buildPredicate(id, predicate), negated = false)(id = id)

      case SelectOrAntiSemiApply(_, _, predicate) =>
        SelectOrSemiApplyPipe(lhs, rhs, buildPredicate(id, predicate), negated = true)(id = id)

      case LetSelectOrSemiApply(_, _, idName, predicate) =>
        LetSelectOrSemiApplyPipe(lhs, rhs, idName.name, buildPredicate(id, predicate), negated = false)(id = id)

      case LetSelectOrAntiSemiApply(_, _, idName, predicate) =>
        LetSelectOrSemiApplyPipe(lhs, rhs, idName.name, buildPredicate(id, predicate), negated = true)(id = id)

      case ConditionalApply(lhsPlan, rhsPlan, ids) =>
        ConditionalApplyPipe(
          lhs,
          rhs,
          ids.map(_.name),
          negated = false,
          rhsPlan.availableSymbols.map(_.name) -- lhsPlan.availableSymbols.map(_.name)
        )(id =
          id
        )

      case AntiConditionalApply(lhsPlan, rhsPlan, ids) =>
        ConditionalApplyPipe(
          lhs,
          rhs,
          ids.map(_.name),
          negated = true,
          rhsPlan.availableSymbols.map(_.name) -- lhsPlan.availableSymbols.map(_.name)
        )(id =
          id
        )

      case Union(_, _) =>
        UnionPipe(lhs, rhs)(id = id)

      case OrderedUnion(_, _, sortedColumns) =>
        OrderedUnionPipe(
          lhs,
          rhs,
          InterpretedExecutionContextOrdering.asComparator(sortedColumns.map(translateColumnOrder))
        )(id = id)

      case TriadicSelection(_, _, positivePredicate, sourceId, seenId, targetId) =>
        TriadicSelectionPipe(positivePredicate, lhs, sourceId.name, seenId.name, targetId.name, rhs)(id = id)

      case ValueHashJoin(_, _, internal.expressions.Equals(lhsExpression, rhsExpression)) =>
        ValueHashJoinPipe(buildExpression(lhsExpression), buildExpression(rhsExpression), lhs, rhs)(id = id)

      case ForeachApply(_, _, variable, expression) =>
        ForeachApplyPipe(lhs, rhs, variable.name, buildExpression(expression))(id = id)

      case RollUpApply(_, _, collectionName, identifierToCollection) =>
        RollUpApplyPipe(lhs, rhs, collectionName.name, identifierToCollection.name)(id = id)

      case SubqueryForeach(_, _) =>
        SubqueryForeachPipe(lhs, rhs)(id = id)

      case TransactionForeach(_, _, batchSize, TransactionConcurrency.Serial, onErrorBehaviour, maybeReportAs) =>
        TransactionForeachPipe(
          lhs,
          rhs,
          buildExpression(batchSize),
          onErrorBehaviour,
          maybeReportAs.map(_.name)
        )(id = id)

      case TransactionApply(
          lhsPlan,
          rhsPlan,
          batchSize,
          TransactionConcurrency.Serial,
          onErrorBehaviour,
          maybeReportAs
        ) =>
        TransactionApplyPipe(
          lhs,
          rhs,
          buildExpression(batchSize),
          onErrorBehaviour,
          rhsPlan.availableSymbols.map(_.name) -- lhsPlan.availableSymbols.map(_.name),
          maybeReportAs.map(_.name)
        )(id = id)

      case TransactionForeach(
          _,
          _,
          batchSize,
          TransactionConcurrency.Concurrent(maybeConcurrency),
          onErrorBehaviour,
          maybeReportAs
        ) =>
        ConcurrentTransactionForeachLegacyPipe(
          lhs,
          rhs,
          buildExpression(batchSize),
          maybeConcurrency.map(expressionConverters.toCommandExpression(id, _)),
          onErrorBehaviour,
          maybeReportAs.map(_.name)
        )(id = id)

      case TransactionApply(
          lhsPlan,
          rhsPlan,
          batchSize,
          TransactionConcurrency.Concurrent(maybeConcurrency),
          onErrorBehaviour,
          maybeReportAs
        ) =>
        ConcurrentTransactionApplyLegacyPipe(
          lhs,
          rhs,
          buildExpression(batchSize),
          maybeConcurrency.map(expressionConverters.toCommandExpression(id, _)),
          onErrorBehaviour,
          rhsPlan.availableSymbols.map(_.name) -- lhsPlan.availableSymbols.map(_.name),
          maybeReportAs.map(_.name)
        )(id = id)

      case Trail(
          _,
          _,
          repetition,
          start,
          end,
          innerStart,
          innerEnd,
          groupNodes,
          groupRelationships,
          innerRelationships,
          previouslyBoundRelationships,
          previouslyBoundRelationshipGroups,
          reverseGroupVariableProjections
        ) =>
        TrailPipe(
          lhs,
          rhs,
          repetition,
          start.name,
          end.name,
          innerStart.name,
          innerEnd.name,
          groupNodes,
          groupRelationships,
          innerRelationships.map(_.name),
          previouslyBoundRelationships.map(_.name),
          previouslyBoundRelationshipGroups.map(_.name),
          reverseGroupVariableProjections
        )(id = id)

      case x =>
        throw new InternalException(s"Received a logical plan that has no physical operator $x")
    }
  }

  private def createProjectionsForResult(
    columns: Seq[(LogicalVariable, Set[(LazyPropertyKey, Expression)])]
  ): Seq[Expression] = {
    val runtimeColumns =
      columns.map {
        case (v, cached) if cached.isEmpty => ReferenceByName(v.name)
        case (v, cached)                   => ValuePopulatingReferenceByName(v.name, cached.toArray)
      }
    runtimeColumns
  }

  private def buildPredicate(id: Id, expr: expressions.Expression): Predicate =
    expressionConverters.toCommandPredicate(id, expr)
      .rewrite(KeyTokenResolver.resolveExpressions(_, tokenContext))
      .asInstanceOf[Predicate]

  private def translateColumnOrder(s: plans.ColumnOrder): org.neo4j.cypher.internal.runtime.interpreted.ColumnOrder =
    s match {
      case plans.Ascending(name)  => org.neo4j.cypher.internal.runtime.interpreted.Ascending(name.name)
      case plans.Descending(name) => org.neo4j.cypher.internal.runtime.interpreted.Descending(name.name)
    }
}
