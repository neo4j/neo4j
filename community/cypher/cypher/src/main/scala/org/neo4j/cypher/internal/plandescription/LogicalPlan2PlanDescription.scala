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
package org.neo4j.cypher.internal.plandescription

import org.neo4j.common.EntityType
import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.AllDatabasesScope
import org.neo4j.cypher.internal.ast.CommaSeparatedNames
import org.neo4j.cypher.internal.ast.CommandClauseNames
import org.neo4j.cypher.internal.ast.CreateConstraintType
import org.neo4j.cypher.internal.ast.DatabaseScope
import org.neo4j.cypher.internal.ast.DefaultDatabaseScope
import org.neo4j.cypher.internal.ast.ExecutableBy
import org.neo4j.cypher.internal.ast.ExpressionNames
import org.neo4j.cypher.internal.ast.HomeDatabaseScope
import org.neo4j.cypher.internal.ast.NoNames
import org.neo4j.cypher.internal.ast.NoOptions
import org.neo4j.cypher.internal.ast.Options
import org.neo4j.cypher.internal.ast.OptionsMap
import org.neo4j.cypher.internal.ast.OptionsParam
import org.neo4j.cypher.internal.ast.SingleNamedDatabaseScope
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorBreak
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorContinue
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorFail
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorRetryThenBreak
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorRetryThenContinue
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorRetryThenFail
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsRetryParameters
import org.neo4j.cypher.internal.ast.prettifier.Prettifier
import org.neo4j.cypher.internal.expressions
import org.neo4j.cypher.internal.expressions.AllReduceAccumulator
import org.neo4j.cypher.internal.expressions.Ands
import org.neo4j.cypher.internal.expressions.AutoExtractedParameter
import org.neo4j.cypher.internal.expressions.DecimalDoubleLiteral
import org.neo4j.cypher.internal.expressions.DynamicLabelExpression
import org.neo4j.cypher.internal.expressions.DynamicRelTypeExpression
import org.neo4j.cypher.internal.expressions.ElementTypeName
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.LabelToken
import org.neo4j.cypher.internal.expressions.ListLiteral
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.MapExpression
import org.neo4j.cypher.internal.expressions.NameToken
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.PropertyKeyToken
import org.neo4j.cypher.internal.expressions.RelTypeExpression
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.RelationshipTypeToken
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.SemanticDirection.BOTH
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.UnPositionedVariable.varFor
import org.neo4j.cypher.internal.expressions.functions.Labels
import org.neo4j.cypher.internal.expressions.functions.Point
import org.neo4j.cypher.internal.expressions.functions.Type
import org.neo4j.cypher.internal.frontend.PlannerName
import org.neo4j.cypher.internal.frontend.phases.ResolvedNonLocalCall
import org.neo4j.cypher.internal.ir.CreateNode
import org.neo4j.cypher.internal.ir.CreatePattern
import org.neo4j.cypher.internal.ir.CreateRelationship
import org.neo4j.cypher.internal.ir.EagernessReason
import org.neo4j.cypher.internal.ir.PatternLength
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.RemoveLabelPattern
import org.neo4j.cypher.internal.ir.SetDynamicPropertyPattern
import org.neo4j.cypher.internal.ir.SetLabelPattern
import org.neo4j.cypher.internal.ir.SetMutatingPattern
import org.neo4j.cypher.internal.ir.SetNodePropertiesFromMapPattern
import org.neo4j.cypher.internal.ir.SetNodePropertiesPattern
import org.neo4j.cypher.internal.ir.SetNodePropertyPattern
import org.neo4j.cypher.internal.ir.SetPropertiesFromMapPattern
import org.neo4j.cypher.internal.ir.SetPropertiesPattern
import org.neo4j.cypher.internal.ir.SetPropertyPattern
import org.neo4j.cypher.internal.ir.SetRelationshipPropertiesFromMapPattern
import org.neo4j.cypher.internal.ir.SetRelationshipPropertiesPattern
import org.neo4j.cypher.internal.ir.SetRelationshipPropertyPattern
import org.neo4j.cypher.internal.ir.ShortestRelationshipPattern
import org.neo4j.cypher.internal.ir.SimpleMutatingPattern
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.ir.VarPatternLength
import org.neo4j.cypher.internal.logical.plans
import org.neo4j.cypher.internal.logical.plans.AdministrationCommandLogicalPlan
import org.neo4j.cypher.internal.logical.plans.Aggregation
import org.neo4j.cypher.internal.logical.plans.AllNodesScan
import org.neo4j.cypher.internal.logical.plans.AllQueryExpression
import org.neo4j.cypher.internal.logical.plans.AllowedNonAdministrationCommands
import org.neo4j.cypher.internal.logical.plans.AlterCurrentGraphType
import org.neo4j.cypher.internal.logical.plans.Anti
import org.neo4j.cypher.internal.logical.plans.AntiConditionalApply
import org.neo4j.cypher.internal.logical.plans.AntiSemiApply
import org.neo4j.cypher.internal.logical.plans.Apply
import org.neo4j.cypher.internal.logical.plans.ArgumentTracker
import org.neo4j.cypher.internal.logical.plans.Ascending
import org.neo4j.cypher.internal.logical.plans.AssertSameNode
import org.neo4j.cypher.internal.logical.plans.AssertSameRelationship
import org.neo4j.cypher.internal.logical.plans.AssertingMultiNodeIndexSeek
import org.neo4j.cypher.internal.logical.plans.AssertingMultiRelationshipIndexSeek
import org.neo4j.cypher.internal.logical.plans.BFSPruningVarExpand
import org.neo4j.cypher.internal.logical.plans.BidirectionalRepeatTrail
import org.neo4j.cypher.internal.logical.plans.Bound
import org.neo4j.cypher.internal.logical.plans.CacheProperties
import org.neo4j.cypher.internal.logical.plans.CartesianProduct
import org.neo4j.cypher.internal.logical.plans.ColumnOrder
import org.neo4j.cypher.internal.logical.plans.CommandYieldColumn
import org.neo4j.cypher.internal.logical.plans.CompositeQueryExpression
import org.neo4j.cypher.internal.logical.plans.ConditionalApply
import org.neo4j.cypher.internal.logical.plans.Create
import org.neo4j.cypher.internal.logical.plans.CreateConstraint
import org.neo4j.cypher.internal.logical.plans.CreateFulltextIndex
import org.neo4j.cypher.internal.logical.plans.CreateIndex
import org.neo4j.cypher.internal.logical.plans.CreateLookupIndex
import org.neo4j.cypher.internal.logical.plans.CreateVectorIndex
import org.neo4j.cypher.internal.logical.plans.DeleteExpression
import org.neo4j.cypher.internal.logical.plans.DeleteNode
import org.neo4j.cypher.internal.logical.plans.DeletePath
import org.neo4j.cypher.internal.logical.plans.DeleteRelationship
import org.neo4j.cypher.internal.logical.plans.Descending
import org.neo4j.cypher.internal.logical.plans.DetachDeleteExpression
import org.neo4j.cypher.internal.logical.plans.DetachDeleteNode
import org.neo4j.cypher.internal.logical.plans.DetachDeletePath
import org.neo4j.cypher.internal.logical.plans.DirectedAllRelationshipsScan
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipByElementIdSeek
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipByIdSeek
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipFulltextIndexSearch
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipIndexContainsScan
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipIndexEndsWithScan
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipIndexScan
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipIndexSeek
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipTypeScan
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipUniqueIndexSeek
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipVectorIndexSearch
import org.neo4j.cypher.internal.logical.plans.DirectedUnionRelationshipTypesScan
import org.neo4j.cypher.internal.logical.plans.Distinct
import org.neo4j.cypher.internal.logical.plans.DoNothingIfExistsForConstraint
import org.neo4j.cypher.internal.logical.plans.DoNothingIfExistsForFulltextIndex
import org.neo4j.cypher.internal.logical.plans.DoNothingIfExistsForIndex
import org.neo4j.cypher.internal.logical.plans.DoNothingIfExistsForLookupIndex
import org.neo4j.cypher.internal.logical.plans.DoNothingIfExistsForVectorIndex
import org.neo4j.cypher.internal.logical.plans.DropConstraintOnName
import org.neo4j.cypher.internal.logical.plans.DropIndexOnName
import org.neo4j.cypher.internal.logical.plans.DynamicDirectedRelationshipTypeLookup
import org.neo4j.cypher.internal.logical.plans.DynamicElement
import org.neo4j.cypher.internal.logical.plans.DynamicLabelNodeLookup
import org.neo4j.cypher.internal.logical.plans.DynamicUndirectedRelationshipTypeLookup
import org.neo4j.cypher.internal.logical.plans.Eager
import org.neo4j.cypher.internal.logical.plans.EmptyResult
import org.neo4j.cypher.internal.logical.plans.ErrorPlan
import org.neo4j.cypher.internal.logical.plans.ExhaustiveLimit
import org.neo4j.cypher.internal.logical.plans.ExistenceQueryExpression
import org.neo4j.cypher.internal.logical.plans.Expand
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandAll
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandInto
import org.neo4j.cypher.internal.logical.plans.Expand.VariablePredicate
import org.neo4j.cypher.internal.logical.plans.FindShortestPaths
import org.neo4j.cypher.internal.logical.plans.Foreach
import org.neo4j.cypher.internal.logical.plans.ForeachApply
import org.neo4j.cypher.internal.logical.plans.ForeignLeafPlan
import org.neo4j.cypher.internal.logical.plans.FusedMerge
import org.neo4j.cypher.internal.logical.plans.GraphType.graphTypeDropInfo
import org.neo4j.cypher.internal.logical.plans.GraphType.graphTypeInfoForPlan
import org.neo4j.cypher.internal.logical.plans.GraphTypeForAdd
import org.neo4j.cypher.internal.logical.plans.GraphTypeForAlter
import org.neo4j.cypher.internal.logical.plans.GraphTypeForDrop
import org.neo4j.cypher.internal.logical.plans.GraphTypeForSet
import org.neo4j.cypher.internal.logical.plans.IndexSeekNames
import org.neo4j.cypher.internal.logical.plans.InequalitySeekRangeWrapper
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
import org.neo4j.cypher.internal.logical.plans.LogicalPlans
import org.neo4j.cypher.internal.logical.plans.ManyQueryExpression
import org.neo4j.cypher.internal.logical.plans.ManySeekableArgs
import org.neo4j.cypher.internal.logical.plans.MatchAllQueryExpression
import org.neo4j.cypher.internal.logical.plans.MatchEntitySetQueryExpression
import org.neo4j.cypher.internal.logical.plans.Merge
import org.neo4j.cypher.internal.logical.plans.MergeInto
import org.neo4j.cypher.internal.logical.plans.MergeUniqueNode
import org.neo4j.cypher.internal.logical.plans.MultiNodeIndexSeek
import org.neo4j.cypher.internal.logical.plans.NestedPlanExpression
import org.neo4j.cypher.internal.logical.plans.NodeByElementIdSeek
import org.neo4j.cypher.internal.logical.plans.NodeByIdSeek
import org.neo4j.cypher.internal.logical.plans.NodeByLabelScan
import org.neo4j.cypher.internal.logical.plans.NodeCountFromCountStore
import org.neo4j.cypher.internal.logical.plans.NodeFulltextIndexSearch
import org.neo4j.cypher.internal.logical.plans.NodeHashJoin
import org.neo4j.cypher.internal.logical.plans.NodeIndexContainsScan
import org.neo4j.cypher.internal.logical.plans.NodeIndexEndsWithScan
import org.neo4j.cypher.internal.logical.plans.NodeIndexScan
import org.neo4j.cypher.internal.logical.plans.NodeIndexSeek
import org.neo4j.cypher.internal.logical.plans.NodeUniqueIndexSeek
import org.neo4j.cypher.internal.logical.plans.NodeVectorIndexSearch
import org.neo4j.cypher.internal.logical.plans.NonExistenceQueryExpression
import org.neo4j.cypher.internal.logical.plans.NullifyMetadata
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
import org.neo4j.cypher.internal.logical.plans.PathPropagatingBFS
import org.neo4j.cypher.internal.logical.plans.PointBoundingBoxRange
import org.neo4j.cypher.internal.logical.plans.PointBoundingBoxSeekRangeWrapper
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
import org.neo4j.cypher.internal.logical.plans.RemoteBatchProperties
import org.neo4j.cypher.internal.logical.plans.RemoteBatchPropertiesWithFilter
import org.neo4j.cypher.internal.logical.plans.RemoteNodeIndexSeek
import org.neo4j.cypher.internal.logical.plans.RemoteNodeUniqueIndexSeek
import org.neo4j.cypher.internal.logical.plans.RemoveLabels
import org.neo4j.cypher.internal.logical.plans.RepeatAcyclic
import org.neo4j.cypher.internal.logical.plans.RepeatOptions
import org.neo4j.cypher.internal.logical.plans.RepeatTrail
import org.neo4j.cypher.internal.logical.plans.RepeatWalk
import org.neo4j.cypher.internal.logical.plans.RightOuterHashJoin
import org.neo4j.cypher.internal.logical.plans.RollUpApply
import org.neo4j.cypher.internal.logical.plans.RunQueryAt
import org.neo4j.cypher.internal.logical.plans.SeekableArgs
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
import org.neo4j.cypher.internal.logical.plans.ShowCurrentGraphType
import org.neo4j.cypher.internal.logical.plans.ShowDatabases
import org.neo4j.cypher.internal.logical.plans.ShowFunctions
import org.neo4j.cypher.internal.logical.plans.ShowIndexes
import org.neo4j.cypher.internal.logical.plans.ShowProcedures
import org.neo4j.cypher.internal.logical.plans.ShowSettings
import org.neo4j.cypher.internal.logical.plans.ShowTransactions
import org.neo4j.cypher.internal.logical.plans.SimulatedExpand
import org.neo4j.cypher.internal.logical.plans.SimulatedNodeScan
import org.neo4j.cypher.internal.logical.plans.SimulatedSelection
import org.neo4j.cypher.internal.logical.plans.SingleQueryExpression
import org.neo4j.cypher.internal.logical.plans.SingleSeekableArg
import org.neo4j.cypher.internal.logical.plans.Skip
import org.neo4j.cypher.internal.logical.plans.Sort
import org.neo4j.cypher.internal.logical.plans.StatefulShortestPath
import org.neo4j.cypher.internal.logical.plans.SubqueryForeach
import org.neo4j.cypher.internal.logical.plans.SubtractionNodeByLabelsScan
import org.neo4j.cypher.internal.logical.plans.SystemProcedureCall
import org.neo4j.cypher.internal.logical.plans.TerminateTransactions
import org.neo4j.cypher.internal.logical.plans.Top
import org.neo4j.cypher.internal.logical.plans.Top1WithTies
import org.neo4j.cypher.internal.logical.plans.TransactionApply
import org.neo4j.cypher.internal.logical.plans.TransactionConcurrency
import org.neo4j.cypher.internal.logical.plans.TransactionForeach
import org.neo4j.cypher.internal.logical.plans.TriadicBuild
import org.neo4j.cypher.internal.logical.plans.TriadicFilter
import org.neo4j.cypher.internal.logical.plans.TriadicSelection
import org.neo4j.cypher.internal.logical.plans.UndirectedAllRelationshipsScan
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipByElementIdSeek
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipByIdSeek
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipFulltextIndexSearch
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipIndexContainsScan
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipIndexEndsWithScan
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipIndexScan
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipIndexSeek
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipTypeScan
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipUniqueIndexSeek
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipVectorIndexSearch
import org.neo4j.cypher.internal.logical.plans.UndirectedUnionRelationshipTypesScan
import org.neo4j.cypher.internal.logical.plans.Union
import org.neo4j.cypher.internal.logical.plans.UnionNodeByLabelsScan
import org.neo4j.cypher.internal.logical.plans.UnwindCollection
import org.neo4j.cypher.internal.logical.plans.ValueHashJoin
import org.neo4j.cypher.internal.logical.plans.ValueMergeJoin
import org.neo4j.cypher.internal.logical.plans.VarExpand
import org.neo4j.cypher.internal.macros.AssertMacros3.checkOnlyWhenAssertionsAreEnabled
import org.neo4j.cypher.internal.plandescription.Arguments.Details
import org.neo4j.cypher.internal.plandescription.Arguments.EstimatedRows
import org.neo4j.cypher.internal.plandescription.Arguments.Planner
import org.neo4j.cypher.internal.plandescription.Arguments.PlannerImpl
import org.neo4j.cypher.internal.plandescription.Arguments.PlannerVersionArgument
import org.neo4j.cypher.internal.plandescription.Arguments.RuntimeVersion
import org.neo4j.cypher.internal.plandescription.Arguments.Version
import org.neo4j.cypher.internal.plandescription.LogicalPlan2PlanDescription.getPrettyDynamicElement
import org.neo4j.cypher.internal.plandescription.LogicalPlan2PlanDescription.getPrettyStringName
import org.neo4j.cypher.internal.plandescription.LogicalPlan2PlanDescription.prettyOptions
import org.neo4j.cypher.internal.plandescription.LogicalPlan2PlanDescription.prettyUpdateLabelString
import org.neo4j.cypher.internal.plandescription.asPrettyString.PrettyStringInterpolator
import org.neo4j.cypher.internal.plandescription.asPrettyString.PrettyStringMaker
import org.neo4j.cypher.internal.plandescription.asPrettyString.raw
import org.neo4j.cypher.internal.planner.spi.ImmutablePlanningAttributes
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.Foldable.SkipChildren
import org.neo4j.cypher.internal.util.FunctionName
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Namespace
import org.neo4j.cypher.internal.util.Repetition
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.UpperBound.Limited
import org.neo4j.cypher.internal.util.UpperBound.Unlimited
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.collection.immutable.ListSet
import org.neo4j.cypher.internal.util.topDown
import org.neo4j.exceptions.InternalException
import org.neo4j.graphdb.schema.IndexType

object LogicalPlan2PlanDescription {

  def create(
    input: LogicalPlan,
    plannerName: PlannerName,
    readOnly: Boolean,
    effectiveCardinalities: ImmutablePlanningAttributes.EffectiveCardinalities,
    withRawCardinalities: Boolean,
    withDistinctness: Boolean,
    renderNestedPlanExpressions: Boolean,
    providedOrders: ImmutablePlanningAttributes.ProvidedOrders,
    runtimeOperatorMetadata: Id => Seq[Argument],
    cypherVersion: CypherVersion,
    cypherPlannerVersion: Option[String] = None
  ): InternalPlanDescription = {
    new LogicalPlan2PlanDescription(
      readOnly,
      effectiveCardinalities,
      withRawCardinalities,
      withDistinctness,
      renderNestedPlanExpressions,
      providedOrders,
      runtimeOperatorMetadata
    )
      .create(input)
      .addArgument(Version(cypherVersion.versionName))
      .addArgument(RuntimeVersion.currentVersion)
      .addArgument(Planner(plannerName.toTextOutput))
      .addArgument(PlannerImpl(plannerName.name))
      .addArgument(PlannerVersionArgument.forDisplay(cypherPlannerVersion))
  }

  def prettyOptions(options: Options): PrettyString = options match {
    case NoOptions               => pretty""
    case OptionsParam(parameter) => pretty" OPTIONS ${asPrettyString(parameter)}"
    case OptionsMap(options) =>
      pretty" OPTIONS ${options.map({
          case (s, e) => pretty"${asPrettyString(s)}: ${asPrettyString(e)}"
        }).mkPrettyString("{", ", ", "}")}"
  }

  def prettyUpdateLabelString(labelNames: Iterable[LabelName], dynamicLabels: Iterable[Expression]): PrettyString = {
    val prettyStaticLabels = if (labelNames.isEmpty) pretty""
    else labelNames.map(labelName => asPrettyString(labelName.name)).mkPrettyString(":", ":", "")
    val prettyDynamicLabels = if (dynamicLabels.isEmpty) pretty""
    else
      dynamicLabels.map(labelExpression => pretty"$$(${asPrettyString(labelExpression)})").mkPrettyString(":", ":", "")
    pretty"$prettyStaticLabels$prettyDynamicLabels"
  }

  def getPrettyStringName(nameOption: Option[Expression]): PrettyString =
    nameOption.map(n => pretty" ${getPrettyStringName(n)}").getOrElse(pretty"")

  def getPrettyStringName(name: Expression): PrettyString =
    name match {
      case _: AutoExtractedParameter => asPrettyString(name)
      case _                         => PrettyString(Prettifier.escapeName(name))
    }

  def getPrettyDynamicElement(expr: DynamicElement) = {
    expr match {
      case DynamicElement.Simple(expr, DynamicElement.All) => pretty"$$all(${asPrettyString(expr)})"
      case DynamicElement.Simple(expr, DynamicElement.Any) => pretty"$$any(${asPrettyString(expr)})"
    }
  }
}

case class LogicalPlan2PlanDescription(
  readOnly: Boolean,
  effectiveCardinalities: ImmutablePlanningAttributes.EffectiveCardinalities,
  withRawCardinalities: Boolean,
  withDistinctness: Boolean = false,
  renderNestedPlanExpressions: Boolean = false,
  providedOrders: ImmutablePlanningAttributes.ProvidedOrders,
  runtimeOperatorMetadata: Id => Seq[Argument]
) extends LogicalPlans.Mapper[InternalPlanDescription] {
  private val SEPARATOR = ", "

  def create(plan: LogicalPlan): InternalPlanDescription = {
    // We don't use an actual CancellationChecker here,
    // because it is possible to get here even after the transaction has been closed.
    LogicalPlans.map(plan, this)(CancellationChecker.neverCancelled())
  }

  override def onLeaf(plan: LogicalPlan): InternalPlanDescription = {
    checkOnlyWhenAssertionsAreEnabled(plan.isLeaf)

    val id = plan.id
    val variables = plan.availableSymbols.map(asPrettyString(_))
    val children = describeNestedPlans(plan)

    val result: InternalPlanDescription = replaceNestedPlansWithPlaceholder(plan) match {
      case AllowedNonAdministrationCommands(_, Some(commandPlan)) => create(commandPlan)

      case _: AdministrationCommandLogicalPlan =>
        PlanDescriptionImpl(
          id,
          "AdministrationCommand",
          children,
          Seq.empty,
          Set.empty,
          withRawCardinalities,
          withDistinctness
        )

      case AllNodesScan(idName, _) =>
        PlanDescriptionImpl(
          id,
          "AllNodesScan",
          children,
          Seq(Details(asPrettyString(idName))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case PartitionedAllNodesScan(idName, _) =>
        PlanDescriptionImpl(
          id,
          "PartitionedAllNodesScan",
          children,
          Seq(Details(asPrettyString(idName))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case NodeByLabelScan(idName, label, _, _) =>
        val prettyDetails = pretty"${asPrettyString(idName)}:${asPrettyString(label.name)}"
        PlanDescriptionImpl(
          id,
          "NodeByLabelScan",
          children,
          Seq(Details(prettyDetails)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case DynamicLabelNodeLookup(idName, labelExpr, _, propertyPredicates) =>
        val label = getPrettyDynamicElement(labelExpr)
        val prettyDetails = pretty"${asPrettyString(idName)}:$label"
          .append(formatPropertyPredicatesForDynamicIndexSeek(asPrettyString(idName), propertyPredicates))

        PlanDescriptionImpl(
          id,
          "DynamicLabelNodeLookup",
          children,
          Seq(Details(prettyDetails)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case PartitionedNodeByLabelScan(idName, label, _) =>
        val prettyDetails = pretty"${asPrettyString(idName)}:${asPrettyString(label.name)}"
        PlanDescriptionImpl(
          id,
          "PartitionedNodeByLabelScan",
          children,
          Seq(Details(prettyDetails)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case UnionNodeByLabelsScan(idName, labels, _, _) =>
        val prettyDetails =
          pretty"${asPrettyString(idName)}:${labels.map(l => asPrettyString(l.name)).mkPrettyString("|")}"
        PlanDescriptionImpl(
          id,
          "UnionNodeByLabelsScan",
          children,
          Seq(Details(prettyDetails)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case PartitionedUnionNodeByLabelsScan(idName, labels, _) =>
        val prettyDetails =
          pretty"${asPrettyString(idName)}:${labels.map(l => asPrettyString(l.name)).mkPrettyString("|")}"
        PlanDescriptionImpl(
          id,
          "PartitionedUnionNodeByLabelsScan",
          children,
          Seq(Details(prettyDetails)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case IntersectionNodeByLabelsScan(idName, labels, _, _) =>
        val prettyDetails =
          pretty"${asPrettyString(idName)}:${labels.map(l => asPrettyString(l.name)).mkPrettyString("&")}"
        PlanDescriptionImpl(
          id,
          "IntersectionNodeByLabelsScan",
          children,
          Seq(Details(prettyDetails)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case PartitionedIntersectionNodeByLabelsScan(idName, labels, _) =>
        val prettyDetails =
          pretty"${asPrettyString(idName)}:${labels.map(l => asPrettyString(l.name)).mkPrettyString("&")}"
        PlanDescriptionImpl(
          id,
          "PartitionedIntersectionNodeByLabelsScan",
          children,
          Seq(Details(prettyDetails)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case SubtractionNodeByLabelsScan(idName, positiveLabels, negativeLabels, _, _) =>
        val prettyDetails =
          pretty"${asPrettyString(idName)}:${positiveLabels.map(l => asPrettyString(l.name)).mkPrettyString(
              "",
              "&",
              "&"
            )}${negativeLabels.map(l => pretty"!${asPrettyString(l.name)}").mkPrettyString("&")}"
        PlanDescriptionImpl(
          id,
          "SubtractionNodeByLabelsScan",
          children,
          Seq(Details(prettyDetails)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case PartitionedSubtractionNodeByLabelsScan(idName, positiveLabels, negativeLabels, _) =>
        val prettyDetails =
          pretty"${asPrettyString(idName)}:${positiveLabels.map(l => asPrettyString(l.name)).mkPrettyString(
              "",
              "&",
              "&"
            )}${negativeLabels.map(l => pretty"!${asPrettyString(l.name)}").mkPrettyString("&")}"
        PlanDescriptionImpl(
          id,
          "PartitionedSubtractionNodeByLabelsScan",
          children,
          Seq(Details(prettyDetails)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case DirectedUnionRelationshipTypesScan(idName, start, types, end, _, _) =>
        val prettyTypes = types
          .map(_.name)
          .map(asPrettyString(_))
          .mkPrettyString("|")
        val prettyDetails =
          pretty"(${asPrettyString(start)})-[${asPrettyString(idName)}:$prettyTypes]->(${asPrettyString(end)})"
        PlanDescriptionImpl(
          id,
          "DirectedUnionRelationshipTypesScan",
          children,
          Seq(Details(prettyDetails)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case UndirectedUnionRelationshipTypesScan(idName, start, types, end, _, _) =>
        val prettyTypes = types
          .map(_.name)
          .map(asPrettyString(_))
          .mkPrettyString("|")
        val prettyDetails =
          pretty"(${asPrettyString(start)})-[${asPrettyString(idName)}:$prettyTypes]-(${asPrettyString(end)})"
        PlanDescriptionImpl(
          id,
          "UndirectedUnionRelationshipTypesScan",
          children,
          Seq(Details(prettyDetails)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case PartitionedDirectedUnionRelationshipTypesScan(idName, start, types, end, _) =>
        val prettyTypes = types
          .map(_.name)
          .map(asPrettyString(_))
          .mkPrettyString("|")
        val prettyDetails =
          pretty"(${asPrettyString(start)})-[${asPrettyString(idName)}:$prettyTypes]->(${asPrettyString(end)})"
        PlanDescriptionImpl(
          id,
          "PartitionedDirectedUnionRelationshipTypesScan",
          children,
          Seq(Details(prettyDetails)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case PartitionedUndirectedUnionRelationshipTypesScan(idName, start, types, end, _) =>
        val prettyTypes = types
          .map(_.name)
          .map(asPrettyString(_))
          .mkPrettyString("|")
        val prettyDetails =
          pretty"(${asPrettyString(start)})-[${asPrettyString(idName)}:$prettyTypes]-(${asPrettyString(end)})"
        PlanDescriptionImpl(
          id,
          "PartitionedUndirectedUnionRelationshipTypesScan",
          children,
          Seq(Details(prettyDetails)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case NodeByIdSeek(idName, nodeIds: SeekableArgs, _) =>
        val prettyDetails =
          pretty"${asPrettyString(idName)} WHERE id(${asPrettyString(idName)}) ${seekableArgsInfo(nodeIds)}"
        PlanDescriptionImpl(
          id,
          "NodeByIdSeek",
          children,
          Seq(Details(prettyDetails)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case NodeByElementIdSeek(idName, nodeIds: SeekableArgs, _) =>
        val prettyDetails =
          pretty"${asPrettyString(idName)} WHERE elementId(${asPrettyString(idName)}) ${seekableArgsInfo(nodeIds)}"
        PlanDescriptionImpl(
          id,
          "NodeByElementIdSeek",
          children,
          Seq(Details(prettyDetails)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case NodeVectorIndexSearch(
          idName,
          _,
          properties,
          maybeScore,
          indexName,
          vector,
          limit,
          _,
          maybePropertyFilter,
          _
        ) =>
        val predicate = maybePropertyFilter match {
          case Some(valueExpr) =>
            pretty" WHERE ${indexPredicateString(Some(idName), properties.drop(1).map(_.propertyKeyToken), valueExpr)}"
          case None => pretty""
        }
        val score = maybeScore match {
          case Some(scoreVariable) => pretty" SCORE AS ${asPrettyString(scoreVariable.name)}"
          case None                => pretty""
        }
        val prettyDetails =
          pretty"SEARCH ${asPrettyString(idName)} IN (VECTOR INDEX ${asPrettyString(indexName)} FOR ${asPrettyString(vector)}$predicate LIMIT ${asPrettyString(limit)})$score"

        PlanDescriptionImpl(
          id,
          "NodeVectorIndexSearch",
          Seq.empty,
          Seq(Details(prettyDetails)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case DirectedRelationshipVectorIndexSearch(
          idName,
          start,
          end,
          typeTokens,
          properties,
          maybeScore,
          indexName,
          vector,
          limit,
          _,
          maybePropertyFilter,
          _
        ) =>

        val predicate = maybePropertyFilter match {
          case Some(valueExpr) =>
            pretty" WHERE ${indexPredicateString(idName, properties.drop(1).map(_.propertyKeyToken), valueExpr)}"
          case None => pretty""
        }
        val score = maybeScore match {
          case Some(scoreVariable) => pretty" SCORE AS ${asPrettyString(scoreVariable.name)}"
          case None                => pretty""
        }
        val prettyDetails =
          pretty"SEARCH ${relationshipPattern(start, idName, typeTokens.map(t => RelTypeName(t.name)(InputPosition.NONE)), end, OUTGOING)} IN (VECTOR INDEX ${asPrettyString(indexName)} FOR ${asPrettyString(vector)}$predicate LIMIT ${asPrettyString(limit)})$score"

        PlanDescriptionImpl(
          id,
          "DirectedRelationshipVectorIndexSearch",
          Seq.empty,
          Seq(Details(prettyDetails)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case UndirectedRelationshipVectorIndexSearch(
          idName,
          start,
          end,
          typeTokens,
          properties,
          maybeScore,
          indexName,
          vector,
          limit,
          _,
          maybePropertyFilter,
          _
        ) =>

        val predicate = maybePropertyFilter match {
          case Some(valueExpr) =>
            pretty" WHERE ${indexPredicateString(idName, properties.drop(1).map(_.propertyKeyToken), valueExpr)}"
          case None => pretty""
        }
        val score = maybeScore match {
          case Some(scoreVariable) => pretty" SCORE AS ${asPrettyString(scoreVariable.name)}"
          case None                => pretty""
        }
        val prettyDetails =
          pretty"SEARCH ${relationshipPattern(start, idName, typeTokens.map(t => RelTypeName(t.name)(InputPosition.NONE)), end, BOTH)} IN (VECTOR INDEX ${asPrettyString(indexName)} FOR ${asPrettyString(vector)}$predicate LIMIT ${asPrettyString(limit)})$score"

        PlanDescriptionImpl(
          id,
          "UndirectedRelationshipVectorIndexSearch",
          Seq.empty,
          Seq(Details(prettyDetails)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case s: NodeFulltextIndexSearch =>
        val analyzerPart = s.analyzer match {
          case Some(a) => pretty" WITH ANALYZER ${asPrettyString(a)}"
          case None    => pretty""
        }
        val skipPart = s.skip match {
          case Some(sk) => pretty" SKIP ${asPrettyString(sk)}"
          case None     => pretty""
        }
        val score = s.score match {
          case Some(scoreVariable) => pretty" SCORE AS ${asPrettyString(scoreVariable.name)}"
          case None                => pretty""
        }
        val prettyDetails =
          pretty"SEARCH ${asPrettyString(s.idName)} IN (FULLTEXT INDEX ${asPrettyString(s.indexName)} FOR ${asPrettyString(s.queryString)}$analyzerPart$skipPart LIMIT ${asPrettyString(s.limit)})$score"

        PlanDescriptionImpl(
          id,
          "NodeFulltextIndexSearch",
          Seq.empty,
          Seq(Details(prettyDetails)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case s: DirectedRelationshipFulltextIndexSearch =>
        val analyzerPart = s.analyzer match {
          case Some(a) => pretty" WITH ANALYZER ${asPrettyString(a)}"
          case None    => pretty""
        }
        val skipPart = s.skip match {
          case Some(sk) => pretty" SKIP ${asPrettyString(sk)}"
          case None     => pretty""
        }
        val score = s.score match {
          case Some(scoreVariable) => pretty" SCORE AS ${asPrettyString(scoreVariable.name)}"
          case None                => pretty""
        }
        val prettyDetails =
          pretty"SEARCH ${relationshipPattern(s.startNode, s.idName, s.typeTokens.map(t => RelTypeName(t.name)(InputPosition.NONE)), s.endNode, OUTGOING)} IN (FULLTEXT INDEX ${asPrettyString(s.indexName)} FOR ${asPrettyString(s.queryString)}$analyzerPart$skipPart LIMIT ${asPrettyString(s.limit)})$score"

        PlanDescriptionImpl(
          id,
          "DirectedRelationshipFulltextIndexSearch",
          Seq.empty,
          Seq(Details(prettyDetails)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case s: UndirectedRelationshipFulltextIndexSearch =>
        val analyzerPart = s.analyzer match {
          case Some(a) => pretty" WITH ANALYZER ${asPrettyString(a)}"
          case None    => pretty""
        }
        val skipPart = s.skip match {
          case Some(sk) => pretty" SKIP ${asPrettyString(sk)}"
          case None     => pretty""
        }
        val score = s.score match {
          case Some(scoreVariable) => pretty" SCORE AS ${asPrettyString(scoreVariable.name)}"
          case None                => pretty""
        }
        val prettyDetails =
          pretty"SEARCH ${relationshipPattern(s.startNode, s.idName, s.typeTokens.map(t => RelTypeName(t.name)(InputPosition.NONE)), s.endNode, BOTH)} IN (FULLTEXT INDEX ${asPrettyString(s.indexName)} FOR ${asPrettyString(s.queryString)}$analyzerPart$skipPart LIMIT ${asPrettyString(s.limit)})$score"

        PlanDescriptionImpl(
          id,
          "UndirectedRelationshipFulltextIndexSearch",
          Seq.empty,
          Seq(Details(prettyDetails)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case p @ NodeIndexSeek(idName, label, properties, valueExpr, _, _, indexType, _) =>
        val (indexMode, indexDesc) = getNodeIndexDescriptions(
          idName,
          label,
          properties.map(_.propertyKeyToken),
          indexType,
          valueExpr,
          unique = false,
          readOnly,
          p.cachedProperties
        )
        PlanDescriptionImpl(
          id,
          indexMode,
          Seq.empty,
          Seq(Details(indexDesc)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case p @ RemoteNodeIndexSeek(idName, label, properties, valueExpr, _, _, indexType, _) =>
        val (indexMode, indexDesc) = getNodeIndexDescriptions(
          idName,
          label,
          properties.map(_.propertyKeyToken),
          indexType,
          valueExpr,
          unique = false,
          readOnly,
          p.cachedProperties
        )
        PlanDescriptionImpl(
          id,
          "Remote" + indexMode,
          Seq.empty,
          Seq(Details(indexDesc)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case p @ PartitionedNodeIndexSeek(idName, label, properties, valueExpr, _, indexType) =>
        val (indexMode, indexDesc) = getNodeIndexDescriptions(
          idName,
          label,
          properties.map(_.propertyKeyToken),
          indexType,
          valueExpr,
          unique = false,
          readOnly,
          p.cachedProperties
        )
        PlanDescriptionImpl(
          id,
          "Partitioned" + indexMode,
          Seq.empty,
          Seq(Details(indexDesc)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case p @ RemoteNodeUniqueIndexSeek(idName, label, properties, valueExpr, _, _, indexType, _) =>
        val (indexMode, indexDesc) = getNodeIndexDescriptions(
          idName,
          label,
          properties.map(_.propertyKeyToken),
          indexType,
          valueExpr,
          unique = true,
          readOnly,
          p.cachedProperties
        )
        PlanDescriptionImpl(
          id,
          "Remote" + indexMode,
          Seq.empty,
          Seq(Details(indexDesc)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case p @ NodeUniqueIndexSeek(idName, label, properties, valueExpr, _, _, indexType, _) =>
        val (indexMode, indexDesc) = getNodeIndexDescriptions(
          idName,
          label,
          properties.map(_.propertyKeyToken),
          indexType,
          valueExpr,
          unique = true,
          readOnly,
          p.cachedProperties
        )
        PlanDescriptionImpl(
          id,
          indexMode,
          Seq.empty,
          Seq(Details(indexDesc)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case MergeUniqueNode(idName, label, properties, seekExpressions, _, _, indexType, onMatch, onCreate) =>
        val queryExpression = if (seekExpressions.length == 1) {
          SingleQueryExpression(seekExpressions.head)
        } else {
          CompositeQueryExpression(seekExpressions.map(SingleQueryExpression(_)))
        }

        def asPrettyProps(kv: (PropertyKeyName, Expression)): PrettyString = {
          pretty"${asPrettyString(idName)}.${asPrettyString(kv._1)} = ${asPrettyString(kv._2)}"
        }
        def asPretty(op: String, props: Seq[(PropertyKeyName, Expression)]): PrettyString = {
          if (props.isEmpty) pretty""
          else {
            pretty"ON ${asPrettyString(op)} SET ${props.map(asPrettyProps).mkPrettyString(", ")}"
          }
        }

        val onDetails = pretty"${asPretty("MATCH", onMatch)} ${asPretty("CREATE", onCreate)}"

        val (_, indexDesc) = getNodeIndexDescriptions(
          idName,
          label,
          properties.map(_.propertyKeyToken),
          indexType,
          queryExpression,
          unique = true,
          readOnly,
          Seq.empty
        )
        PlanDescriptionImpl(
          id,
          "MergeUniqueNode",
          Seq.empty,
          Seq(Details(
            Seq(
              indexDesc,
              onDetails
            )
          )),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case p @ MultiNodeIndexSeek(indexLeafPlans) =>
        val (_, indexDescs) = indexLeafPlans.map(l =>
          getNodeIndexDescriptions(
            l.idName,
            l.label,
            l.properties.map(_.propertyKeyToken),
            l.indexType,
            l.valueExpr,
            unique = l.isInstanceOf[NodeUniqueIndexSeek],
            readOnly,
            p.cachedProperties
          )
        ).unzip
        PlanDescriptionImpl(
          id = plan.id,
          "MultiNodeIndexSeek",
          children,
          Seq(Details(indexDescs)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case p @ AssertingMultiNodeIndexSeek(_, indexLeafPlans) =>
        val (_, indexDescs) = indexLeafPlans.map(l =>
          getNodeIndexDescriptions(
            l.idName,
            l.label,
            l.properties.map(_.propertyKeyToken),
            l.indexType,
            l.valueExpr,
            unique = l.isInstanceOf[NodeUniqueIndexSeek],
            readOnly,
            p.cachedProperties
          )
        ).unzip
        PlanDescriptionImpl(
          id = plan.id,
          "AssertingMultiNodeIndexSeek",
          children,
          Seq(Details(indexDescs)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case p @ AssertingMultiRelationshipIndexSeek(_, _, _, _, indexLeafPlans) =>
        val (_, indexDescs) = indexLeafPlans.map(l =>
          getRelIndexDescriptions(
            l.idName,
            l.leftNode,
            l.typeToken,
            l.rightNode,
            l.directed,
            l.properties.map(_.propertyKeyToken),
            l.indexType,
            l.valueExpr,
            unique = l.unique,
            readOnly,
            p.cachedProperties
          )
        ).unzip
        PlanDescriptionImpl(
          id = plan.id,
          "AssertingMultiRelationshipIndexSeek",
          children,
          Seq(Details(indexDescs)),
          variables,
          withRawCardinalities,
          withDistinctness
        )
      case p @ DirectedRelationshipIndexSeek(idName, start, end, typ, properties, valueExpr, _, _, indexType, _) =>
        val (indexMode, indexDesc) = getRelIndexDescriptions(
          idName,
          start,
          typ,
          end,
          isDirected = true,
          properties.map(_.propertyKeyToken),
          indexType,
          valueExpr,
          unique = false,
          readOnly = readOnly,
          p.cachedProperties
        )
        PlanDescriptionImpl(id, indexMode, Seq.empty, Seq(Details(indexDesc)), variables)
      case p @ UndirectedRelationshipIndexSeek(idName, start, end, typ, properties, valueExpr, _, _, indexType, _) =>
        val (indexMode, indexDesc) = getRelIndexDescriptions(
          idName,
          start,
          typ,
          end,
          isDirected = false,
          properties.map(_.propertyKeyToken),
          indexType,
          valueExpr,
          unique = false,
          readOnly = readOnly,
          p.cachedProperties
        )
        PlanDescriptionImpl(
          id,
          indexMode,
          Seq.empty,
          Seq(Details(indexDesc)),
          variables,
          withRawCardinalities,
          withDistinctness
        )
      case p @ PartitionedDirectedRelationshipIndexSeek(idName, start, end, typ, properties, valueExpr, _, indexType) =>
        val (indexMode, indexDesc) = getRelIndexDescriptions(
          idName,
          start,
          typ,
          end,
          isDirected = true,
          properties.map(_.propertyKeyToken),
          indexType,
          valueExpr,
          unique = false,
          readOnly = readOnly,
          p.cachedProperties
        )
        PlanDescriptionImpl(id, "Partitioned" + indexMode, Seq.empty, Seq(Details(indexDesc)), variables)
      case p @ PartitionedUndirectedRelationshipIndexSeek(
          idName,
          start,
          end,
          typ,
          properties,
          valueExpr,
          _,
          indexType
        ) =>
        val (indexMode, indexDesc) = getRelIndexDescriptions(
          idName,
          start,
          typ,
          end,
          isDirected = false,
          properties.map(_.propertyKeyToken),
          indexType,
          valueExpr,
          unique = false,
          readOnly = readOnly,
          p.cachedProperties
        )
        PlanDescriptionImpl(
          id,
          "Partitioned" + indexMode,
          Seq.empty,
          Seq(Details(indexDesc)),
          variables,
          withRawCardinalities,
          withDistinctness
        )
      case p @ DirectedRelationshipUniqueIndexSeek(idName, start, end, typ, properties, valueExpr, _, _, indexType) =>
        val (indexMode, indexDesc) = getRelIndexDescriptions(
          idName,
          start,
          typ,
          end,
          isDirected = true,
          properties.map(_.propertyKeyToken),
          indexType,
          valueExpr,
          unique = true,
          readOnly = readOnly,
          p.cachedProperties
        )
        PlanDescriptionImpl(id, indexMode, Seq.empty, Seq(Details(indexDesc)), variables)
      case p @ UndirectedRelationshipUniqueIndexSeek(idName, start, end, typ, properties, valueExpr, _, _, indexType) =>
        val (indexMode, indexDesc) = getRelIndexDescriptions(
          idName,
          start,
          typ,
          end,
          isDirected = false,
          properties.map(_.propertyKeyToken),
          indexType,
          valueExpr,
          unique = true,
          readOnly = readOnly,
          p.cachedProperties
        )
        PlanDescriptionImpl(
          id,
          indexMode,
          Seq.empty,
          Seq(Details(indexDesc)),
          variables,
          withRawCardinalities,
          withDistinctness
        )
      case p @ DirectedRelationshipIndexScan(idName, start, end, typ, properties, _, _, indexType, _) =>
        val tokens = properties.map(_.propertyKeyToken)
        val props = tokens.map(x => asPrettyString(x.name))
        val predicates = props.map(p => pretty"$p IS NOT NULL").mkPrettyString(" AND ")
        val info = relIndexInfoString(
          idName,
          start,
          typ,
          end,
          isDirected = true,
          tokens,
          indexType,
          predicates,
          p.cachedProperties
        )
        PlanDescriptionImpl(
          id,
          "DirectedRelationshipIndexScan",
          children,
          Seq(Details(info)),
          variables,
          withRawCardinalities,
          withDistinctness
        )
      case p @ UndirectedRelationshipIndexScan(idName, start, end, typ, properties, _, _, indexType, _) =>
        val tokens = properties.map(_.propertyKeyToken)
        val props = tokens.map(x => asPrettyString(x.name))
        val predicates = props.map(p => pretty"$p IS NOT NULL").mkPrettyString(" AND ")
        val info = relIndexInfoString(
          idName,
          start,
          typ,
          end,
          isDirected = false,
          tokens,
          indexType,
          predicates,
          p.cachedProperties
        )
        PlanDescriptionImpl(
          id,
          "UndirectedRelationshipIndexScan",
          children,
          Seq(Details(info)),
          variables,
          withRawCardinalities,
          withDistinctness
        )
      case p @ PartitionedDirectedRelationshipIndexScan(idName, start, end, typ, properties, _, indexType) =>
        val tokens = properties.map(_.propertyKeyToken)
        val props = tokens.map(x => asPrettyString(x.name))
        val predicates = props.map(p => pretty"$p IS NOT NULL").mkPrettyString(" AND ")
        val info = relIndexInfoString(
          idName,
          start,
          typ,
          end,
          isDirected = true,
          tokens,
          indexType,
          predicates,
          p.cachedProperties
        )
        PlanDescriptionImpl(
          id,
          "PartitionedDirectedRelationshipIndexScan",
          children,
          Seq(Details(info)),
          variables,
          withRawCardinalities,
          withDistinctness
        )
      case p @ PartitionedUndirectedRelationshipIndexScan(idName, start, end, typ, properties, _, indexType) =>
        val tokens = properties.map(_.propertyKeyToken)
        val props = tokens.map(x => asPrettyString(x.name))
        val predicates = props.map(p => pretty"$p IS NOT NULL").mkPrettyString(" AND ")
        val info = relIndexInfoString(
          idName,
          start,
          typ,
          end,
          isDirected = false,
          tokens,
          indexType,
          predicates,
          p.cachedProperties
        )
        PlanDescriptionImpl(
          id,
          "PartitionedUndirectedRelationshipIndexScan",
          children,
          Seq(Details(info)),
          variables,
          withRawCardinalities,
          withDistinctness
        )
      case p @ DirectedRelationshipIndexContainsScan(idName, start, end, typ, property, valueExpr, _, _, indexType) =>
        val predicate = pretty"${asPrettyString(property.propertyKeyToken.name)} CONTAINS ${asPrettyString(valueExpr)}"
        val info = relIndexInfoString(
          idName,
          start,
          typ,
          end,
          isDirected = true,
          Seq(property.propertyKeyToken),
          indexType,
          predicate,
          p.cachedProperties
        )
        PlanDescriptionImpl(id, "DirectedRelationshipIndexContainsScan", Seq.empty, Seq(Details(info)), variables)
      case p @ UndirectedRelationshipIndexContainsScan(idName, start, end, typ, property, valueExpr, _, _, indexType) =>
        val predicate = pretty"${asPrettyString(property.propertyKeyToken.name)} CONTAINS ${asPrettyString(valueExpr)}"
        val info = relIndexInfoString(
          idName,
          start,
          typ,
          end,
          isDirected = false,
          Seq(property.propertyKeyToken),
          indexType,
          predicate,
          p.cachedProperties
        )
        PlanDescriptionImpl(
          id,
          "UndirectedRelationshipIndexContainsScan",
          children,
          Seq(Details(info)),
          variables,
          withRawCardinalities,
          withDistinctness
        )
      case p @ DirectedRelationshipIndexEndsWithScan(idName, start, end, typ, property, valueExpr, _, _, indexType) =>
        val predicate = pretty"${asPrettyString(property.propertyKeyToken.name)} ENDS WITH ${asPrettyString(valueExpr)}"
        val info = relIndexInfoString(
          idName,
          start,
          typ,
          end,
          isDirected = true,
          Seq(property.propertyKeyToken),
          indexType,
          predicate,
          p.cachedProperties
        )
        PlanDescriptionImpl(
          id,
          "DirectedRelationshipIndexEndsWithScan",
          children,
          Seq(Details(info)),
          variables,
          withRawCardinalities,
          withDistinctness
        )
      case p @ UndirectedRelationshipIndexEndsWithScan(idName, start, end, typ, property, valueExpr, _, _, indexType) =>
        val predicate = pretty"${asPrettyString(property.propertyKeyToken.name)} ENDS WITH ${asPrettyString(valueExpr)}"
        val info = relIndexInfoString(
          idName,
          start,
          typ,
          end,
          isDirected = false,
          Seq(property.propertyKeyToken),
          indexType,
          predicate,
          p.cachedProperties
        )
        PlanDescriptionImpl(
          id,
          "UndirectedRelationshipIndexEndsWithScan",
          children,
          Seq(Details(info)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case plans.Argument(argumentIds) if argumentIds.nonEmpty =>
        val details =
          if (argumentIds.nonEmpty) Seq(Details(argumentIds.map(asPrettyString(_)).mkPrettyString(SEPARATOR)))
          else Seq.empty
        PlanDescriptionImpl(id, "Argument", Seq.empty, details, variables, withRawCardinalities, withDistinctness)

      case _: plans.Argument =>
        ArgumentPlanDescription(id, Seq.empty, variables)

      case DirectedRelationshipByIdSeek(idName, relIds, startNode, endNode, _) =>
        val details = Details(relationshipByIdSeekInfo(idName, relIds, startNode, endNode, true, "id"))
        PlanDescriptionImpl(
          id,
          "DirectedRelationshipByIdSeek",
          children,
          Seq(details),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case DirectedRelationshipByElementIdSeek(idName, relIds, startNode, endNode, _) =>
        val details =
          Details(relationshipByIdSeekInfo(idName, relIds, startNode, endNode, true, "elementId"))
        PlanDescriptionImpl(
          id,
          "DirectedRelationshipByElementIdSeek",
          children,
          Seq(details),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case UndirectedRelationshipByIdSeek(idName, relIds, startNode, endNode, _) =>
        val details = Details(relationshipByIdSeekInfo(idName, relIds, startNode, endNode, false, "id"))
        PlanDescriptionImpl(
          id,
          "UndirectedRelationshipByIdSeek",
          children,
          Seq(details),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case UndirectedRelationshipByElementIdSeek(idName, relIds, startNode, endNode, _) =>
        val details =
          Details(relationshipByIdSeekInfo(idName, relIds, startNode, endNode, false, "elementId"))
        PlanDescriptionImpl(
          id,
          "UndirectedRelationshipByElementIdSeek",
          children,
          Seq(details),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case DirectedAllRelationshipsScan(idName, start, end, _) =>
        PlanDescriptionImpl(
          id,
          "DirectedAllRelationshipsScan",
          children,
          Seq(Details(relationshipPattern(start, idName, Seq.empty, end, OUTGOING))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case PartitionedDirectedAllRelationshipsScan(idName, start, end, _) =>
        PlanDescriptionImpl(
          id,
          "PartitionedDirectedAllRelationshipsScan",
          children,
          Seq(Details(relationshipPattern(start, idName, Seq.empty, end, OUTGOING))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case UndirectedAllRelationshipsScan(idName, start, end, _) =>
        PlanDescriptionImpl(
          id,
          "UndirectedAllRelationshipsScan",
          children,
          Seq(Details(relationshipPattern(start, idName, Seq.empty, end, BOTH))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case PartitionedUndirectedAllRelationshipsScan(idName, start, end, _) =>
        PlanDescriptionImpl(
          id,
          "PartitionedUndirectedAllRelationshipsScan",
          children,
          Seq(Details(relationshipPattern(start, idName, Seq.empty, end, BOTH))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case DirectedRelationshipTypeScan(idName, start, typeName, end, _, _) =>
        PlanDescriptionImpl(
          id,
          "DirectedRelationshipTypeScan",
          children,
          Seq(Details(relationshipPattern(start, idName, Seq(typeName), end, OUTGOING))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case DynamicDirectedRelationshipTypeLookup(idName, start, typeExpr, end, _, _, propertyPredicates) =>
        val prettyType =
          pretty"(${asPrettyString(start)})-[${asPrettyString(idName)}:${getPrettyDynamicElement(typeExpr)}]->(${asPrettyString(end)})"
            .append(formatPropertyPredicatesForDynamicIndexSeek(asPrettyString(idName), propertyPredicates))
        PlanDescriptionImpl(
          id,
          "DynamicDirectedRelationshipTypeLookup",
          children,
          Seq(Details(prettyType)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case UndirectedRelationshipTypeScan(idName, start, typeName, end, _, _) =>
        PlanDescriptionImpl(
          id,
          "UndirectedRelationshipTypeScan",
          children,
          Seq(Details(relationshipPattern(start, idName, Seq(typeName), end, BOTH))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case DynamicUndirectedRelationshipTypeLookup(idName, start, typeExpr, end, _, _, propertyPredicates) =>
        val prettyDetails =
          pretty"(${asPrettyString(start)})-[${asPrettyString(idName)}:${getPrettyDynamicElement(typeExpr)}]-(${asPrettyString(end)})"
            .append(formatPropertyPredicatesForDynamicIndexSeek(asPrettyString(idName), propertyPredicates))
        PlanDescriptionImpl(
          id,
          "DynamicUndirectedRelationshipTypeLookup",
          children,
          Seq(Details(prettyDetails)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case PartitionedDirectedRelationshipTypeScan(idName, start, typeName, end, _) =>
        PlanDescriptionImpl(
          id,
          "PartitionedDirectedRelationshipTypeScan",
          children,
          Seq(Details(relationshipPattern(start, idName, Seq(typeName), end, OUTGOING))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case PartitionedUndirectedRelationshipTypeScan(idName, start, typeName, end, _) =>
        PlanDescriptionImpl(
          id,
          "PartitionedUndirectedRelationshipTypeScan",
          children,
          Seq(Details(relationshipPattern(start, idName, Seq(typeName), end, BOTH))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case Input(nodes, rels, inputVars, _) =>
        PlanDescriptionImpl(
          id,
          "Input",
          children,
          Seq(Details((nodes ++ rels ++ inputVars).map(asPrettyString(_)))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case NodeCountFromCountStore(ident, labelNames, _) =>
        val info = nodeCountFromCountStoreInfo(ident, labelNames)
        PlanDescriptionImpl(
          id,
          "NodeCountFromCountStore",
          children,
          Seq(Details(info)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case p @ NodeIndexContainsScan(idName, label, property, valueExpr, _, _, indexType) =>
        val predicate = pretty"${asPrettyString(property.propertyKeyToken.name)} CONTAINS ${asPrettyString(valueExpr)}"
        val info = nodeIndexInfoString(
          idName.name,
          unique = false,
          label,
          Seq(property.propertyKeyToken),
          indexType,
          predicate,
          p.cachedProperties
        )
        PlanDescriptionImpl(
          id,
          "NodeIndexContainsScan",
          children,
          Seq(Details(info)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case p @ NodeIndexEndsWithScan(idName, label, property, valueExpr, _, _, indexType) =>
        val predicate = pretty"${asPrettyString(property.propertyKeyToken.name)} ENDS WITH ${asPrettyString(valueExpr)}"
        val info = nodeIndexInfoString(
          idName.name,
          unique = false,
          label,
          Seq(property.propertyKeyToken),
          indexType,
          predicate,
          p.cachedProperties
        )
        PlanDescriptionImpl(
          id,
          "NodeIndexEndsWithScan",
          children,
          Seq(Details(info)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case p @ NodeIndexScan(idName, label, properties, _, _, indexType, _) =>
        val tokens = properties.map(_.propertyKeyToken)
        val props = tokens.map(x => asPrettyString(x.name))
        val predicates = props.map(p => pretty"$p IS NOT NULL").mkPrettyString(" AND ")
        val info =
          nodeIndexInfoString(idName.name, unique = false, label, tokens, indexType, predicates, p.cachedProperties)
        PlanDescriptionImpl(
          id,
          "NodeIndexScan",
          children,
          Seq(Details(info)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case p @ PartitionedNodeIndexScan(idName, label, properties, _, indexType) =>
        val tokens = properties.map(_.propertyKeyToken)
        val props = tokens.map(x => asPrettyString(x.name))
        val predicates = props.map(p => pretty"$p IS NOT NULL").mkPrettyString(" AND ")
        val info =
          nodeIndexInfoString(idName.name, unique = false, label, tokens, indexType, predicates, p.cachedProperties)
        PlanDescriptionImpl(
          id,
          "PartitionedNodeIndexScan",
          children,
          Seq(Details(info)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case SimulatedNodeScan(idName, numberOfRows) =>
        val details = Details(Seq(
          asPrettyString(idName),
          asPrettyString(SignedDecimalIntegerLiteral(numberOfRows.toString)(InputPosition.NONE))
        ))
        PlanDescriptionImpl(
          id,
          "SimulatedNodeScan",
          children,
          Seq(details),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case ProcedureCall(_, call) =>
        PlanDescriptionImpl(
          id,
          "ProcedureCall",
          children,
          Seq(Details(signatureInfo(call))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case RelationshipCountFromCountStore(ident, startLabel, typeNames, endLabel, _) =>
        val info = relationshipCountFromCountStoreInfo(ident, startLabel, typeNames, endLabel)
        PlanDescriptionImpl(
          id,
          "RelationshipCountFromCountStore",
          children,
          Seq(Details(info)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case DoNothingIfExistsForIndex(entityName, propertyKeyNames, indexType, nameOption, _) =>
        PlanDescriptionImpl(
          id,
          s"DoNothingIfExists(INDEX)",
          children,
          Seq(Details(indexInfo(indexType.name(), nameOption, entityName, propertyKeyNames, NoOptions))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case DoNothingIfExistsForLookupIndex(entityType, nameOption, _) =>
        PlanDescriptionImpl(
          id,
          s"DoNothingIfExists(INDEX)",
          children,
          Seq(Details(lookupIndexInfo(nameOption, entityType, NoOptions))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case DoNothingIfExistsForFulltextIndex(entityNames, propertyKeyNames, nameOption, _) =>
        PlanDescriptionImpl(
          id,
          s"DoNothingIfExists(INDEX)",
          children,
          Seq(Details(fulltextIndexInfo(nameOption, entityNames, propertyKeyNames, NoOptions))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case DoNothingIfExistsForVectorIndex(entityNames, propertyKeyNames, additionalPropertyKeyNames, nameOption, _) =>
        PlanDescriptionImpl(
          id,
          s"DoNothingIfExists(INDEX)",
          children,
          Seq(Details(vectorIndexInfo(
            nameOption,
            entityNames,
            propertyKeyNames,
            additionalPropertyKeyNames,
            NoOptions
          ))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case CreateIndex(
          _,
          indexType,
          entityName,
          propertyKeyNames,
          nameOption,
          options
        ) => // Can be both a leaf plan and a middle plan so need to be in both places
        PlanDescriptionImpl(
          id,
          "CreateIndex",
          children,
          Seq(Details(indexInfo(indexType.name(), nameOption, entityName, propertyKeyNames, options))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case CreateLookupIndex(
          _,
          entityType,
          nameOption,
          options
        ) => // Can be both a leaf plan and a middle plan so need to be in both places
        PlanDescriptionImpl(
          id,
          "CreateIndex",
          children,
          Seq(Details(lookupIndexInfo(nameOption, entityType, options))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case CreateFulltextIndex(
          _,
          entityNames,
          propertyKeyNames,
          nameOption,
          options
        ) => // Can be both a leaf plan and a middle plan so need to be in both places
        PlanDescriptionImpl(
          id,
          "CreateIndex",
          children,
          Seq(Details(fulltextIndexInfo(nameOption, entityNames, propertyKeyNames, options))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case CreateVectorIndex(
          _,
          entityNames,
          propertyKeyNames,
          additionalPropertyKeyNames,
          nameOption,
          options
        ) => // Can be both a leaf plan and a middle plan so need to be in both places
        PlanDescriptionImpl(
          id,
          "CreateIndex",
          children,
          Seq(Details(vectorIndexInfo(nameOption, entityNames, propertyKeyNames, additionalPropertyKeyNames, options))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case DropIndexOnName(name, ifExists) =>
        val ifExistsString = if (ifExists) pretty" IF EXISTS" else pretty""
        PlanDescriptionImpl(
          id,
          "DropIndex",
          children,
          Seq(Details(pretty"INDEX ${getPrettyStringName(name)}$ifExistsString")),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case s: ShowIndexes =>
        val typeDescription = asPrettyString.raw(s.indexType.description)
        val colsDescription = commandColumnInfo(s.yieldColumns, s.yieldAll)
        PlanDescriptionImpl(
          id,
          "ShowIndexes",
          children,
          Seq(Details(pretty"$typeDescription, $colsDescription")),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case DoNothingIfExistsForConstraint(entityName, props, assertion, name, _) =>
        val entity = props.head.map.asCanonicalStringVal
        PlanDescriptionImpl(
          id,
          s"DoNothingIfExists(CONSTRAINT)",
          children,
          Seq(Details(constraintInfo(name, entity, entityName, props, assertion))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case CreateConstraint(
          _,
          constraintType,
          label,
          properties: Seq[Property],
          nameOption,
          options
        ) => // Can be both a leaf plan and a middle plan so need to be in both places
        val entity = properties.head.map.asCanonicalStringVal
        val details = Details(constraintInfo(
          nameOption,
          entity,
          label,
          properties,
          constraintType,
          options
        ))
        PlanDescriptionImpl(
          id,
          "CreateConstraint",
          children,
          Seq(details),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case DropConstraintOnName(name, ifExists) =>
        val ifExistsString = if (ifExists) pretty" IF EXISTS" else pretty""
        val constraintDetails = Details(pretty"CONSTRAINT ${getPrettyStringName(name)}$ifExistsString")
        PlanDescriptionImpl(
          id,
          "DropConstraint",
          children,
          Seq(constraintDetails),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case s: ShowConstraints =>
        val typeDescription = asPrettyString.raw(s.constraintType.description)
        val colsDescription = commandColumnInfo(s.yieldColumns, s.yieldAll)
        PlanDescriptionImpl(
          id,
          "ShowConstraints",
          children,
          Seq(Details(pretty"$typeDescription, $colsDescription")),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case AlterCurrentGraphType(gt: GraphTypeForSet) =>
        val details = Details(asPrettyString.raw(graphTypeInfoForPlan(gt.elementTypes, gt.constraints)))
        PlanDescriptionImpl(
          id,
          "AlterCurrentGraphTypeSet",
          children,
          Seq(details),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case AlterCurrentGraphType(gt: GraphTypeForAdd) =>
        val details = Details(asPrettyString.raw(graphTypeInfoForPlan(gt.elementTypes, gt.constraints)))
        PlanDescriptionImpl(
          id,
          "AlterCurrentGraphTypeAdd",
          children,
          Seq(details),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case AlterCurrentGraphType(gt: GraphTypeForDrop) =>
        val details = Details(asPrettyString.raw(graphTypeDropInfo(gt.elementTypes, gt.constraints)))
        PlanDescriptionImpl(
          id,
          "AlterCurrentGraphTypeDrop",
          children,
          Seq(details),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case AlterCurrentGraphType(gt: GraphTypeForAlter) =>
        val details = Details(asPrettyString.raw(graphTypeInfoForPlan(gt.elementTypes, Set.empty)))
        PlanDescriptionImpl(
          id,
          "AlterCurrentGraphTypeAlter",
          children,
          Seq(details),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case s: ShowCurrentGraphType =>
        val colsDescription = commandColumnInfo(s.yieldColumns, s.yieldAll)
        val commandDescription = if (s.asGraph) pretty"graphTypeAsGraph" else pretty"graphTypeAsString"

        PlanDescriptionImpl(
          id,
          "ShowCurrentGraphType",
          children,
          Seq(Details(pretty"$commandDescription, $colsDescription")),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case s: ShowProcedures =>
        val executableDescription = s.executableBy.map(e => asPrettyString.raw(e.description("procedures"))).getOrElse(
          asPrettyString.raw(ExecutableBy.defaultDescription("procedures"))
        )
        val colsDescription = commandColumnInfo(s.yieldColumns, s.yieldAll)
        PlanDescriptionImpl(
          id,
          "ShowProcedures",
          children,
          Seq(Details(pretty"$executableDescription, $colsDescription")),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case s: ShowFunctions =>
        val typeDescription = asPrettyString.raw(s.functionType.description)
        val executableDescription = s.executableBy.map(e => asPrettyString.raw(e.description("functions"))).getOrElse(
          pretty"functionsForUser(all)"
        )
        val colsDescription = commandColumnInfo(s.yieldColumns, s.yieldAll)
        PlanDescriptionImpl(
          id,
          "ShowFunctions",
          children,
          Seq(Details(pretty"$typeDescription, $executableDescription, $colsDescription")),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case s: ShowTransactions =>
        val idsDescription =
          getInnerNameDescriptions(s.ids).map(i => pretty"transactions($i)").getOrElse(pretty"allTransactions")
        val colsDescription = commandColumnInfo(s.yieldColumns, s.yieldAll)
        PlanDescriptionImpl(
          id,
          "ShowTransactions",
          children,
          Seq(Details(pretty"$colsDescription, $idsDescription")),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case t: TerminateTransactions =>
        val idsDescription = getInnerNameDescriptions(t.ids).getOrElse(
          // We always parse ids so it shouldn't be able to be empty
          throw InternalException.internalError(
            this.getClass.getSimpleName,
            "Terminate transactions had no id's."
          )
        )
        val colsDescription = commandColumnInfo(t.yieldColumns, t.yieldAll)
        PlanDescriptionImpl(
          id,
          "TerminateTransactions",
          children,
          Seq(Details(pretty"$colsDescription, transactions($idsDescription)")),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case s: ShowSettings =>
        val namesDescription =
          getInnerNameDescriptions(s.names).map(i => pretty"settings($i)").getOrElse(pretty"allSettings")
        val colsDescription = commandColumnInfo(s.yieldColumns, s.yieldAll)
        PlanDescriptionImpl(
          id,
          "ShowSettings",
          children,
          Seq(Details(pretty"$namesDescription, $colsDescription")),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      // System database only commands
      case s: ShowDatabases =>
        val colsDescription = commandColumnInfo(s.yieldColumns, s.yieldAll)
        val scopeDescription: PartialFunction[DatabaseScope, PrettyString] = {
          case AllDatabasesScope()          => pretty"allDatabases"
          case DefaultDatabaseScope()       => pretty"defaultDatabase"
          case HomeDatabaseScope()          => pretty"homeDatabase"
          case SingleNamedDatabaseScope(db) => pretty"database(${asPrettyString(db.asCanonicalStringVal)})"
        }
        PlanDescriptionImpl(
          id,
          "ShowDatabases",
          children,
          Seq(Details(pretty"${scopeDescription(s.dbScope)}, $colsDescription")),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case SystemProcedureCall(call, _, _, _) =>
        PlanDescriptionImpl(
          id,
          "ProcedureCall",
          children,
          Seq(Details(signatureInfo(call))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case ForeignLeafPlan(_, externalPlan, _) =>
        PlanDescriptionImpl(
          id,
          "External",
          Seq.empty,
          Seq(Details(asPrettyString.raw(externalPlan.planDescriptionDetails))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case x => throw InternalException.internalError(
          this.getClass.getSimpleName,
          s"Unknown plan type: ${x.getClass.getSimpleName}. Missing a case?"
        )
    }

    addRuntimeAttributes(addPlanningAttributes(result, plan), plan)
  }

  override def onOneChildPlan(plan: LogicalPlan, source: InternalPlanDescription): InternalPlanDescription = {
    checkOnlyWhenAssertionsAreEnabled(plan.lhs.nonEmpty)
    checkOnlyWhenAssertionsAreEnabled(plan.rhs.isEmpty)

    val id = plan.id
    val variables = plan.availableSymbols.map(asPrettyString(_))
    val nestedPlanDescriptions = describeNestedPlans(plan)
    val children =
      Seq(source).filterNot(_.isInstanceOf[ArgumentPlanDescription]) ++ nestedPlanDescriptions

    val result: InternalPlanDescription = replaceNestedPlansWithPlaceholder(plan) match {
      case _: AdministrationCommandLogicalPlan =>
        PlanDescriptionImpl(
          id,
          "AdministrationCommand",
          nestedPlanDescriptions,
          Seq.empty,
          Set.empty,
          withRawCardinalities,
          withDistinctness
        )

      case Distinct(_, groupingExpressions) =>
        PlanDescriptionImpl(
          id,
          "Distinct",
          children,
          Seq(Details(aggregationInfo(groupingExpressions, Map.empty))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case OrderedDistinct(_, groupingExpressions, orderToLeverage) =>
        val details = aggregationInfo(groupingExpressions, Map.empty, orderToLeverage)
        PlanDescriptionImpl(
          id,
          "OrderedDistinct",
          children,
          Seq(Details(details)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case Aggregation(_, groupingExpressions, aggregationExpressions) =>
        val details =
          aggregationInfo(groupingExpressions, aggregationExpressions, ordered = Seq.empty)
        PlanDescriptionImpl(
          id,
          "EagerAggregation",
          children,
          Seq(Details(details)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case OrderedAggregation(_, groupingExpressions, aggregationExpressions, orderToLeverage) =>
        val details = aggregationInfo(groupingExpressions, aggregationExpressions, orderToLeverage)
        PlanDescriptionImpl(
          id,
          "OrderedAggregation",
          children,
          Seq(Details(details)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case Create(_, commands) =>
        val details = commands.map {
          case c: CreateNode => createNodeDescription(c)
          case CreateRelationship(idName, leftNode, relType, rightNode, direction, properties) =>
            expandExpressionDescription(
              Some(leftNode),
              Some(idName),
              Seq(relType),
              Some(rightNode),
              direction,
              1,
              Some(1),
              properties
            )
        }
        PlanDescriptionImpl(
          id,
          "Create",
          children,
          Seq(Details(details)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case DeleteExpression(_, expression) =>
        PlanDescriptionImpl(
          id,
          "Delete",
          children,
          Seq(Details(asPrettyString(expression))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case DeleteNode(_, expression) =>
        PlanDescriptionImpl(
          id,
          "Delete",
          children,
          Seq(Details(asPrettyString(expression))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case DeletePath(_, expression) =>
        PlanDescriptionImpl(
          id,
          "Delete",
          children,
          Seq(Details(asPrettyString(expression))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case DeleteRelationship(_, expression) =>
        PlanDescriptionImpl(
          id,
          "Delete",
          children,
          Seq(Details(asPrettyString(expression))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case DetachDeleteExpression(_, expression) =>
        PlanDescriptionImpl(
          id,
          "DetachDelete",
          children,
          Seq(Details(asPrettyString(expression))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case DetachDeleteNode(_, expression) =>
        PlanDescriptionImpl(
          id,
          "DetachDelete",
          children,
          Seq(Details(asPrettyString(expression))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case DetachDeletePath(_, expression) =>
        PlanDescriptionImpl(
          id,
          "DetachDelete",
          children,
          Seq(Details(asPrettyString(expression))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case Eager(_, reasons) =>
        val info = eagernessReasonInfo(reasons)
        val details = if (info.nonEmpty) Seq(Details(info)) else Seq.empty
        PlanDescriptionImpl(id, "Eager", children, details, variables, withRawCardinalities, withDistinctness)

      case _: EmptyResult =>
        PlanDescriptionImpl(id, "EmptyResult", children, Seq.empty, variables, withRawCardinalities, withDistinctness)

      case NodeCountFromCountStore(idName, labelName, _) =>
        val info = nodeCountFromCountStoreInfo(idName, labelName)
        PlanDescriptionImpl(
          id,
          "NodeCountFromCountStore",
          nestedPlanDescriptions,
          Seq(Details(info)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case RelationshipCountFromCountStore(idName, start, types, end, _) =>
        val info = relationshipCountFromCountStoreInfo(idName, start, types, end)
        PlanDescriptionImpl(
          id,
          "RelationshipCountFromCountStore",
          nestedPlanDescriptions,
          Seq(Details(info)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case _: ErrorPlan =>
        PlanDescriptionImpl(id, "Error", children, Seq.empty, variables, withRawCardinalities, withDistinctness)

      case Expand(_, fromName, dir, typeNames, toName, relName, mode) =>
        val expression = Details(expandExpressionDescription(
          Some(fromName),
          relName,
          typeNames,
          toName,
          dir,
          1,
          Some(1),
          None
        ))
        val modeDescr = expandModeDescription(mode)
        PlanDescriptionImpl(
          id,
          s"Expand($modeDescr)",
          children,
          Seq(expression),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case SimulatedExpand(_, fromName, relName, toName, factor) =>
        val prettyFactor = asPrettyString(DecimalDoubleLiteral(factor.toString)(InputPosition.NONE))
        val details = Details(Seq(
          expandExpressionDescription(
            Some(fromName),
            Some(relName),
            Seq.empty,
            Some(toName),
            SemanticDirection.OUTGOING,
            1,
            Some(1),
            None
          ),
          prettyFactor
        ))
        PlanDescriptionImpl(
          id,
          "SimulatedExpand",
          children,
          Seq(details),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case Limit(_, count) =>
        PlanDescriptionImpl(
          id,
          "Limit",
          children,
          Seq(Details(asPrettyString(count))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case ExhaustiveLimit(_, count) =>
        PlanDescriptionImpl(
          id,
          "ExhaustiveLimit",
          children,
          Seq(Details(asPrettyString(count))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case CacheProperties(_, properties) =>
        PlanDescriptionImpl(
          id,
          "CacheProperties",
          children,
          Seq(Details(properties.toSeq.map(asPrettyString(_)))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case RemoteBatchProperties(_, properties) =>
        PlanDescriptionImpl(
          id,
          "RemoteBatchProperties",
          children,
          Seq(Details(properties.toSeq.map(asPrettyString(_)))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case RemoteBatchPropertiesWithFilter(_, predicates, properties) =>
        PlanDescriptionImpl(
          id,
          "RemoteBatchPropertiesWithFilter",
          children,
          Seq(Details(properties.toSeq.map(asPrettyString(_)) ++ predicates.toSeq.map(asPrettyString(_)))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case OptionalExpand(_, fromName, dir, typeNames, toName, relName, mode, predicates) =>
        val predicate = predicates.map(p => pretty" WHERE ${asPrettyString(p)}").getOrElse(pretty"")
        val expandExpressionDesc =
          expandExpressionDescription(Some(fromName), relName, typeNames, toName, dir, 1, Some(1), None)
        val details = Details(pretty"$expandExpressionDesc$predicate")
        val modeText = mode match {
          case ExpandAll  => "OptionalExpand(All)"
          case ExpandInto => "OptionalExpand(Into)"
        }
        PlanDescriptionImpl(id, modeText, children, Seq(details), variables, withRawCardinalities, withDistinctness)

      case ProduceResult(_, columns) =>
        PlanDescriptionImpl(
          id,
          "ProduceResults",
          children,
          Seq(Details(columns.map(c => asPrettyString(c.variable)))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case Projection(_, expr) =>
        val expressions = Details(projectedExpressionInfo(expr))
        PlanDescriptionImpl(
          id,
          "Projection",
          children,
          Seq(expressions),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case Selection(predicate, _) =>
        val details = Details(asPrettyString(predicate))
        PlanDescriptionImpl(id, "Filter", children, Seq(details), variables, withRawCardinalities, withDistinctness)

      case SimulatedSelection(_, selectivity) =>
        val details = Details(asPrettyString(DecimalDoubleLiteral(selectivity.toString)(InputPosition.NONE)))
        PlanDescriptionImpl(
          id,
          "SimulatedFilter",
          children,
          Seq(details),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case Skip(_, count) =>
        PlanDescriptionImpl(
          id,
          name = "Skip",
          children,
          Seq(Details(asPrettyString(count))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case StatefulShortestPath(
          _,
          sourceNode,
          _,
          nfa,
          expansionMode,
          nonInlinedPreFilters,
          _,
          _,
          _,
          _,
          _,
          solvedExpressionString,
          _,
          _,
          pathMode
        ) =>
        def predicateLines(preds: Seq[PrettyString], prefix: PrettyString): PrettyString = {
          preds.tail
            .map(e => pretty"${pretty" ".repeat(prefix.prettifiedString.length)}$e")
            .mkPrettyString(
              pretty"\n$prefix${preds.head}\n".prettifiedString,
              "\n",
              ""
            )
        }

        val modeDescr = expandModeDescription(expansionMode)
        val patternStr = asPrettyString.solvedExpressionString(solvedExpressionString)
        val expandDirStr = expansionMode match {
          case ExpandAll  => pretty"\n        expanding from: ${asPrettyString(sourceNode)}"
          case ExpandInto => pretty"" // We expand from both ends in ExpandInto
        }
        val inlinedPredicatesStr = {
          val prefix = pretty"    inlined predicates: "
          val inlinedPredicates = nfa.predicates.flatMap {
            case Ands(exprs) => exprs
            case x           => Set(x)
          }.toSeq
          inlinedPredicates match {
            case Seq() => pretty""
            case preds => predicateLines(preds.map(asPrettyString(_)).sorted, prefix)
          }
        }
        val nonInlinedPredicatesStr = {
          val prefix = pretty"non-inlined predicates: "
          nonInlinedPreFilters match {
            case Some(Ands(preds)) => predicateLines(preds.toSeq.map(asPrettyString(_)).sorted, prefix)
            case Some(singleExpr)  => predicateLines(Seq(asPrettyString(singleExpr)), prefix)
            case None              => pretty""
          }
        }

        val detailsStr = pretty"$patternStr$expandDirStr$inlinedPredicatesStr$nonInlinedPredicatesStr"
        PlanDescriptionImpl(
          id = id,
          name = s"StatefulShortestPath($modeDescr, $pathMode)",
          children = children,
          arguments = Seq(Details(detailsStr)),
          variables = variables,
          withRawCardinalities = withRawCardinalities,
          withDistinctness = withDistinctness
        )

      case FindShortestPaths(
          _,
          ShortestRelationshipPattern(
            maybePathName,
            PatternRelationship(relName, (fromName, toName), dir, relTypes, patternLength: PatternLength),
            isSingle
          ),
          nodePredicates,
          relPredicates,
          pathPredicates,
          _,
          _,
          _
        ) =>
        val patternRelationshipInfo =
          expandExpressionDescription(Some(fromName), Some(relName), relTypes, Some(toName), dir, patternLength)

        val pathName = asPrettyString(maybePathName.map(_.name).getOrElse("p"))

        val (_, nodeAndRelPredicatesDescription) =
          varExpandPredicateDescriptions(nodePredicates, relPredicates, pathName)

        val standalonePathPredicatesDescription = pathPredicates.map(asPrettyString(_)).mkPrettyString(" AND ")

        val predicatesDescription = (pathPredicates.isEmpty, (nodePredicates ++ relPredicates).isEmpty) match {
          case (true, true)  => pretty""
          case (false, true) => pretty" WHERE ${standalonePathPredicatesDescription}"
          case (true, false) => nodeAndRelPredicatesDescription
          case (false, false) =>
            Seq(nodeAndRelPredicatesDescription, standalonePathPredicatesDescription).mkPrettyString(" AND  ")
        }

        val pathPrefix =
          if ((pathPredicates ++ nodePredicates ++ relPredicates).isEmpty && maybePathName.isEmpty) {
            pretty""
          } else {
            pretty"$pathName = "
          }

        PlanDescriptionImpl(
          id,
          "ShortestPath",
          children,
          Seq(Details(pretty"$pathPrefix$patternRelationshipInfo$predicatesDescription")),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case LoadCSV(_, _, variableName, _, _, _, _) =>
        PlanDescriptionImpl(
          id,
          "LoadCSV",
          children,
          Seq(Details(asPrettyString(variableName))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case MergeInto(_, r, leftNode, dir, relType, rightNode, onMatch, onCreate) =>
        val createRelationship =
          expandExpressionDescription(Some(leftNode), Some(r), Seq(relType), Some(rightNode), dir, 1, Some(1), None)
        def asPrettyProps(kv: (PropertyKeyName, Expression)): PrettyString = {
          pretty"${asPrettyString(r)}.${asPrettyString(kv._1)} = ${asPrettyString(kv._2)}"
        }
        def asPretty(op: String, props: Seq[(PropertyKeyName, Expression)]): PrettyString = {
          if (props.isEmpty) pretty""
          else {
            pretty" ON ${asPrettyString(op)} SET ${props.map(asPrettyProps).mkPrettyString(", ")}"
          }
        }

        val detail = pretty"MERGE $createRelationship${asPretty("MATCH", onMatch)}${asPretty("CREATE", onCreate)}"
        PlanDescriptionImpl(
          id,
          "MergeInto",
          children,
          Seq(Details(Seq(detail))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case Merge(_, createNodes, createRelationships, onMatch, onCreate, nodesToLock) =>
        mergePlanDescription(id, variables, children, createNodes, createRelationships, onMatch, onCreate, nodesToLock)

      case FusedMerge(_, createNodes, createRelationships, onMatch, onCreate, nodesToLock) =>
        mergePlanDescription(id, variables, children, createNodes, createRelationships, onMatch, onCreate, nodesToLock)

      case Optional(_, protectedSymbols) =>
        PlanDescriptionImpl(
          id,
          "Optional",
          children,
          Seq(Details(keyNamesInfo(protectedSymbols.toSeq))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case _: Anti =>
        PlanDescriptionImpl(id, "Anti", children, Seq.empty, variables, withRawCardinalities, withDistinctness)

      case ProcedureCall(_, call) =>
        PlanDescriptionImpl(
          id,
          "ProcedureCall",
          children,
          Seq(Details(signatureInfo(call))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case ProjectEndpoints(_, relName, start, _, end, _, relTypes, direction, patternLength) =>
        val name = s"ProjectEndpoints"

        val details =
          expandExpressionDescription(Some(start), Some(relName), relTypes, Some(end), direction, patternLength)
        PlanDescriptionImpl(
          id,
          name,
          children,
          Seq(Details(details)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case PruningVarExpand(
          _,
          fromName,
          dir,
          types,
          toName,
          min,
          max,
          nodePredicates,
          relationshipPredicates,
          _
        ) =>
        val expandInfo = expandExpressionDescription(
          Some(fromName),
          None,
          types,
          toName,
          dir,
          minLength = min,
          maxLength = Some(max),
          maybeProperties = None
        )
        val (expandDescriptionPrefix, predicatesDescription) =
          varExpandPredicateDescriptions(nodePredicates, relationshipPredicates)
        PlanDescriptionImpl(
          id,
          s"VarLengthExpand(Pruning)",
          children,
          Seq(Details(pretty"$expandDescriptionPrefix$expandInfo$predicatesDescription")),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case BFSPruningVarExpand(
          _,
          fromName,
          dir,
          types,
          toName,
          includeStartNode,
          max,
          depthName,
          mode,
          nodePredicates,
          relationshipPredicates,
          _
        ) =>
        val expandInfo = expandExpressionDescription(
          Some(fromName),
          None,
          types,
          toName,
          dir,
          minLength = if (includeStartNode) 0 else 1,
          maxLength = Some(max),
          maybeProperties = None
        )
        val modeDescr = expandModeDescription(mode)

        val (expandDescriptionPrefix, predicatesDescription) =
          varExpandPredicateDescriptions(nodePredicates, relationshipPredicates)
        val depthString = depthName.map(d => asPrettyString(s"${d.name}")).getOrElse(asPrettyString(""))
        PlanDescriptionImpl(
          id,
          s"VarLengthExpand(Pruning,BFS,$modeDescr)",
          children,
          Seq(Details(pretty"$expandDescriptionPrefix$expandInfo$predicatesDescription $depthString")),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case PathPropagatingBFS(
          _,
          _,
          from,
          dir,
          _,
          types,
          to,
          relName,
          length,
          nodePredicates,
          relationshipPredicates
        ) =>
        val expandDescription = expandExpressionDescription(
          Some(from),
          Some(relName),
          types,
          Some(to),
          dir,
          minLength = length.min,
          maxLength = length.max,
          maybeProperties = None
        )

        val (expandDescriptionPrefix, predicatesDescription) =
          varExpandPredicateDescriptions(nodePredicates, relationshipPredicates)

        PlanDescriptionImpl(
          id,
          "PathPropagatingBFS",
          children,
          Seq(Details(pretty"$expandDescriptionPrefix$expandDescription$predicatesDescription")),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case RemoveLabels(_, idName, labelNames, dynamicLabels) =>
        val prettyId = asPrettyString(idName)
        val prettyLabels = prettyUpdateLabelString(labelNames, dynamicLabels)
        val details = Details(pretty"$prettyId$prettyLabels")
        PlanDescriptionImpl(
          id,
          "RemoveLabels",
          children,
          Seq(details),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case SetLabels(_, idName, labelNames, dynamicLabels) =>
        val prettyId = asPrettyString(idName)
        val prettyLabels = prettyUpdateLabelString(labelNames, dynamicLabels)
        val details = Details(pretty"$prettyId$prettyLabels")
        PlanDescriptionImpl(id, "SetLabels", children, Seq(details), variables, withRawCardinalities, withDistinctness)

      case SetNodePropertiesFromMap(_, idName, expression, removeOtherProps) =>
        val details = Details(setPropertyInfo(asPrettyString(idName), expression, removeOtherProps))
        PlanDescriptionImpl(
          id,
          "SetNodePropertiesFromMap",
          children,
          Seq(details),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case SetPropertiesFromMap(_, entity, expression, removeOtherProps) =>
        val details = Details(setPropertyInfo(asPrettyString(entity), expression, removeOtherProps))
        PlanDescriptionImpl(
          id,
          "SetPropertiesFromMap",
          children,
          Seq(details),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case SetProperty(_, entity, propertyKey, expression) =>
        val entityString = pretty"${asPrettyString(entity)}.${asPrettyString(propertyKey.name)}"
        val details = Details(setPropertyInfo(entityString, expression, true))
        PlanDescriptionImpl(
          id,
          "SetProperty",
          children,
          Seq(details),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case SetDynamicProperty(_, entityExpression, propertyExpression, valueExpression) =>
        val entityString = pretty"${asPrettyString(entityExpression)}[${asPrettyString(propertyExpression)}]"
        val details = Details(setPropertyInfo(entityString, valueExpression, true))
        PlanDescriptionImpl(
          id,
          "SetDynamicProperty",
          children,
          Seq(details),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case SetNodeProperty(_, idName, propertyKey, expression) =>
        val details = Details(setPropertyInfo(
          pretty"${asPrettyString(idName)}.${asPrettyString(propertyKey.name)}",
          expression,
          true
        ))
        PlanDescriptionImpl(
          id,
          "SetProperty",
          children,
          Seq(details),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case SetRelationshipProperty(_, idName, propertyKey, expression) =>
        val details = Details(setPropertyInfo(
          pretty"${asPrettyString(idName)}.${asPrettyString(propertyKey.name)}",
          expression,
          true
        ))
        PlanDescriptionImpl(
          id,
          "SetProperty",
          children,
          Seq(details),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case SetProperties(_, entity, items) =>
        val setOps = items.map {
          case (p, v) =>
            val entityString = pretty"${asPrettyString(entity)}.${asPrettyString(p.name)}"
            setPropertyInfo(entityString, v, removeOtherProps = true)
        }.mkPrettyString(", ")
        val details = Details(setOps)
        PlanDescriptionImpl(
          id,
          "SetProperties",
          children,
          Seq(details),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case SetNodeProperties(_, idName, items) =>
        val setOps = items.map {
          case (p, v) =>
            val entityString = pretty"${asPrettyString(idName)}.${asPrettyString(p.name)}"
            setPropertyInfo(entityString, v, removeOtherProps = true)
        }.mkPrettyString(", ")
        val details = Details(setOps)
        PlanDescriptionImpl(
          id,
          "SetProperties",
          children,
          Seq(details),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case SetRelationshipProperties(_, idName, items) =>
        val setOps = items.map {
          case (p, v) =>
            val entityString = pretty"${asPrettyString(idName)}.${asPrettyString(p.name)}"
            setPropertyInfo(entityString, v, removeOtherProps = true)
        }.mkPrettyString(", ")
        val details = Details(setOps)
        PlanDescriptionImpl(
          id,
          "SetProperties",
          children,
          Seq(details),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case SetRelationshipPropertiesFromMap(_, idName, expression, removeOtherProps) =>
        val details = Details(setPropertyInfo(asPrettyString(idName), expression, removeOtherProps))
        PlanDescriptionImpl(
          id,
          "SetRelationshipPropertiesFromMap",
          children,
          Seq(details),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case Sort(_, orderBy) =>
        PlanDescriptionImpl(
          id,
          "Sort",
          children,
          Seq(Details(orderInfo(orderBy))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case PartialSort(_, alreadySortedPrefix, stillToSortSuffix, _) =>
        PlanDescriptionImpl(
          id,
          "PartialSort",
          children,
          Seq(Details(orderInfo(alreadySortedPrefix ++ stillToSortSuffix))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case Top(_, orderBy, limit) =>
        val details = pretty"${orderInfo(orderBy)} LIMIT ${asPrettyString(limit)}"
        PlanDescriptionImpl(
          id,
          "Top",
          children,
          Seq(Details(details)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case Top1WithTies(_, orderBy) =>
        val details = pretty"${orderInfo(orderBy)}"
        PlanDescriptionImpl(
          id,
          "Top1WithTies",
          children,
          Seq(Details(details)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case PartialTop(_, alreadySortedPrefix, stillToSortSuffix, limit, _) =>
        val details = pretty"${orderInfo(alreadySortedPrefix ++ stillToSortSuffix)} LIMIT ${asPrettyString(limit)}"
        PlanDescriptionImpl(
          id,
          "PartialTop",
          children,
          Seq(Details(details)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case UnwindCollection(_, maybeVariable, expression) =>
        val details = maybeVariable.map(variable =>
          Details(projectedExpressionInfo(Map(variable -> expression)).mkPrettyString(SEPARATOR))
        ).toSeq
        PlanDescriptionImpl(id, "Unwind", children, details, variables, withRawCardinalities, withDistinctness)

      case PartitionedUnwindCollection(_, maybeVariable, expression) =>
        val details = maybeVariable.map(variable =>
          Details(projectedExpressionInfo(Map(variable -> expression)).mkPrettyString(SEPARATOR))
        ).toSeq
        PlanDescriptionImpl(
          id,
          "PartitionedUnwind",
          children,
          details,
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case VarExpand(
          _,
          fromName,
          dir,
          _,
          types,
          toName,
          relName,
          length,
          mode,
          nodePredicates,
          relationshipPredicates,
          _
        ) =>
        val expandDescription = expandExpressionDescription(
          Some(fromName),
          relName,
          types,
          toName,
          dir,
          minLength = length.min,
          maxLength = length.max,
          maybeProperties = None
        )
        val (expandDescriptionPrefix, predicatesDescription) =
          varExpandPredicateDescriptions(nodePredicates, relationshipPredicates)
        val modeDescr = expandModeDescription(mode)
        PlanDescriptionImpl(
          id,
          s"VarLengthExpand($modeDescr)",
          children,
          Seq(Details(pretty"$expandDescriptionPrefix$expandDescription$predicatesDescription")),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case CreateIndex(
          _,
          indexType,
          entityName,
          propertyKeyNames,
          nameOption,
          options
        ) => // Can be both a leaf plan and a middle plan so need to be in both places
        PlanDescriptionImpl(
          id,
          "CreateIndex",
          children,
          Seq(Details(indexInfo(indexType.name(), nameOption, entityName, propertyKeyNames, options))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case CreateLookupIndex(
          _,
          isNodeIndex,
          nameOption,
          options
        ) => // Can be both a leaf plan and a middle plan so need to be in both places
        PlanDescriptionImpl(
          id,
          "CreateIndex",
          children,
          Seq(Details(lookupIndexInfo(nameOption, isNodeIndex, options))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case CreateFulltextIndex(
          _,
          entityNames,
          propertyKeyNames,
          nameOption,
          options
        ) => // Can be both a leaf plan and a middle plan so need to be in both places
        PlanDescriptionImpl(
          id,
          "CreateIndex",
          children,
          Seq(Details(fulltextIndexInfo(nameOption, entityNames, propertyKeyNames, options))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case CreateVectorIndex(
          _,
          entityNames,
          propertyKeyNames,
          additionalPropertyKeyNames,
          nameOption,
          options
        ) => // Can be both a leaf plan and a middle plan so need to be in both places
        PlanDescriptionImpl(
          id,
          "CreateIndex",
          children,
          Seq(Details(vectorIndexInfo(nameOption, entityNames, propertyKeyNames, additionalPropertyKeyNames, options))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case CreateConstraint(
          _,
          constraintType,
          label,
          properties: Seq[Property],
          nameOption,
          options
        ) => // Can be both a leaf plan and a middle plan so need to be in both places
        val entity = properties.head.map.asCanonicalStringVal
        val details = Details(constraintInfo(
          nameOption,
          entity,
          label,
          properties,
          constraintType,
          options
        ))
        PlanDescriptionImpl(
          id,
          "CreateConstraint",
          children,
          Seq(details),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case TriadicBuild(_, sourceId, seenId, _) =>
        val details = Details(pretty"(${asPrettyString(sourceId)})--(${asPrettyString(seenId)})")
        PlanDescriptionImpl(
          id,
          "TriadicBuild",
          children,
          Seq(details),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case TriadicFilter(_, positivePredicate, sourceId, targetId, _) =>
        val positivePredicateString = if (positivePredicate) pretty"" else pretty"NOT "
        val details =
          Details(pretty"WHERE $positivePredicateString(${asPrettyString(sourceId)})--(${asPrettyString(targetId)})")
        PlanDescriptionImpl(
          id,
          "TriadicFilter",
          children,
          Seq(details),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case PreserveOrder(_) =>
        PlanDescriptionImpl(
          id,
          "PreserveOrder",
          children,
          Seq.empty[Argument],
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case Foreach(_, variable, expression, mutations) =>
        val details =
          pretty"${asPrettyString(variable)} IN ${asPrettyString(expression)}" +: mutations.map(mutatingPatternString)
        PlanDescriptionImpl(
          id,
          "Foreach",
          children,
          Seq(Details(details)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case ArgumentTracker(_) =>
        PlanDescriptionImpl(id, "ArgumentTracker", children, Seq(), variables, withRawCardinalities, withDistinctness)

      case NullifyMetadata(_, _, _) =>
        PlanDescriptionImpl(
          id,
          "NullifyMetadata",
          children,
          arguments = Seq.empty,
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case p: RunQueryAt =>
        PlanDescriptionImpl(
          id,
          "RunQueryAt",
          children,
          arguments = Seq(Details(asPrettyString.raw(p.query))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case plans.NonFuseable(_) =>
        PlanDescriptionImpl(
          id,
          "NonFuseable",
          children,
          arguments = Seq.empty,
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case plans.LockNodes(_, nodesToLock) =>
        PlanDescriptionImpl(
          id,
          "LockNodes",
          children,
          Seq(Details(keyNamesInfo(nodesToLock.toSeq))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case x => throw InternalException.internalError(
          this.getClass.getSimpleName,
          s"Unknown plan type: ${x.getClass.getSimpleName}. Missing a case?"
        )
    }

    addRuntimeAttributes(addPlanningAttributes(result, plan), plan)
  }

  private def mergePlanDescription(
    id: Id,
    variables: Set[PrettyString],
    children: Seq[InternalPlanDescription],
    createNodes: Seq[CreateNode],
    createRelationships: Seq[CreateRelationship],
    onMatch: Seq[SetMutatingPattern],
    onCreate: Seq[SetMutatingPattern],
    nodesToLock: Set[LogicalVariable]
  ) = {
    val createNodesPretty = createNodes.map(createNodeDescription)
    val createRelsPretty = createRelationships.map {
      case CreateRelationship(relationship, startNode, typ, endNode, direction, properties) =>
        expandExpressionDescription(
          Some(startNode),
          Some(relationship),
          Seq(typ),
          Some(endNode),
          direction,
          1,
          Some(1),
          properties
        )
    }
    val details: Seq[PrettyString] =
      Seq(pretty"CREATE ${(createNodesPretty ++ createRelsPretty).mkPrettyString(", ")}") ++
        (if (onMatch.nonEmpty) Seq(pretty"ON MATCH ${onMatch.map(mutatingPatternString).mkPrettyString(", ")}")
         else Seq.empty) ++
        (if (onCreate.nonEmpty) Seq(pretty"ON CREATE ${onCreate.map(mutatingPatternString).mkPrettyString(", ")}")
         else Seq.empty) ++
        (if (nodesToLock.nonEmpty) Seq(pretty"LOCK(${keyNamesInfo(nodesToLock.toSeq)})") else Seq.empty)

    val name = if (nodesToLock.isEmpty) "Merge" else "LockingMerge"
    PlanDescriptionImpl(
      id,
      name,
      children,
      Seq(Details(details)),
      variables,
      withRawCardinalities,
      withDistinctness
    )
  }

  private def replaceNestedPlansWithPlaceholder(plan: LogicalPlan) = {
    if (renderNestedPlanExpressions) {
      val nestedPlansToVariables =
        nestedPlanExpressionsMap(plan)
          .view
          .mapValues(varFor)
      plan.endoRewrite(topDown(
        Rewriter.lift({
          case expression: NestedPlanExpression if nestedPlansToVariables.contains(expression) =>
            nestedPlansToVariables(expression)
        }),
        stopper = elem => elem != plan && elem.isInstanceOf[LogicalPlan]
      ))
    } else {
      plan
    }
  }

  /**
   * Nested plan expressions mapped to a Cypher-esque identifier
   */
  def nestedPlanExpressionsMap(plan: LogicalPlan): Map[NestedPlanExpression, String] =
    plan.folder.treeFold(Seq.empty[NestedPlanExpression]) {
      case e: NestedPlanExpression =>
        acc => SkipChildren(acc :+ e)
      case childPlan: LogicalPlan if childPlan != plan =>
        acc => SkipChildren(acc)
    }
      .zipWithIndex
      .map { case (nestedPlanExpression, index) =>
        nestedPlanExpression -> s"nested_plan_$index"
      }
      .toMap

  private def describeNestedPlans(plan: LogicalPlan): Seq[InternalPlanDescription] = {
    if (renderNestedPlanExpressions) {
      nestedPlanExpressionsMap(plan)
        .map { case (expression, expressionIdentifier) =>
          // We don't use an actual CancellationChecker here,
          // because it is possible to get here even after the transaction has been closed.
          val nestedPlanDescription: InternalPlanDescription =
            LogicalPlans.map(expression.plan, this)(CancellationChecker.neverCancelled())
          val details: PrettyString = asPrettyString(expression) append pretty" AS ${raw(expressionIdentifier)}"
          val description = PlanDescriptionImpl(
            Id.INVALID_ID,
            expression.getClass.getSimpleName,
            Seq(nestedPlanDescription),
            Seq(Details(details)),
            Set.empty,
            withRawCardinalities,
            withDistinctness
          )
          addPlanningAttributes(description, expression.plan)
        }
        .toSeq
    } else {
      Seq.empty
    }
  }

  private def expandModeDescription(mode: Expand.ExpansionMode) = {
    mode match {
      case ExpandAll  => "All"
      case ExpandInto => "Into"
    }
  }

  private def callInTxsDetails(
    batchSize: Expression,
    concurrency: TransactionConcurrency,
    onErrorBehaviour: InTransactionsOnErrorBehaviour,
    maybeReportAs: Option[LogicalVariable],
    maybeRetryParameters: Option[InTransactionsRetryParameters],
    disjointBy: Seq[Expression]
  ) = {
    val concurrencyParams = concurrency match {
      case TransactionConcurrency.Concurrent(None)              => "CONCURRENT "
      case TransactionConcurrency.Concurrent(Some(concurrency)) => s"${asPrettyString(concurrency)} CONCURRENT "
      case _                                                    => ""
    }
    val errorParams = onErrorBehaviour match {
      case OnErrorContinue          => (" ON ERROR CONTINUE", "")
      case OnErrorBreak             => (" ON ERROR BREAK", "")
      case OnErrorFail              => (" ON ERROR FAIL", "")
      case OnErrorRetryThenContinue => (" ON ERROR RETRY ", "THEN CONTINUE")
      case OnErrorRetryThenBreak    => (" ON ERROR RETRY ", "THEN BREAK")
      case OnErrorRetryThenFail     => (" ON ERROR RETRY ", "THEN FAIL")
    }
    val reportParams = maybeReportAs.fold("")(status => s" REPORT STATUS AS ${status.name}")
    val retryParams = maybeRetryParameters.fold("")(_.timeout match {
      case Some(timeout) => s"FOR ${asPrettyString(timeout)} SECONDS "
      case _             => ""
    })
    val prettyDisjointBy = if (disjointBy.isEmpty) pretty""
    else {
      disjointBy.map(asPrettyString(_)).mkPrettyString(" DISJOINT BY (", ", ", ")")
    }

    Details(
      pretty"IN ${asPrettyString.raw(concurrencyParams)}TRANSACTIONS OF ${asPrettyString(
          batchSize
        )} ROWS${prettyDisjointBy}${asPrettyString.raw(errorParams._1)}${asPrettyString.raw(retryParams)}${asPrettyString.raw(errorParams._2)}${asPrettyString.raw(reportParams)}"
    )
  }

  override def onTwoChildPlan(
    plan: LogicalPlan,
    lhs: InternalPlanDescription,
    rhs: InternalPlanDescription
  ): InternalPlanDescription = {
    checkOnlyWhenAssertionsAreEnabled(plan.lhs.nonEmpty)
    checkOnlyWhenAssertionsAreEnabled(plan.rhs.nonEmpty)

    val id = plan.id
    val variables = plan.availableSymbols.map(asPrettyString(_))
    val children = Seq(lhs, rhs) ++ describeNestedPlans(plan)

    val result: InternalPlanDescription = replaceNestedPlansWithPlaceholder(plan) match {
      case _: AntiConditionalApply =>
        PlanDescriptionImpl(
          id,
          "AntiConditionalApply",
          children,
          Seq.empty,
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case _: AntiSemiApply =>
        PlanDescriptionImpl(id, "AntiSemiApply", children, Seq.empty, variables, withRawCardinalities, withDistinctness)

      case _: ConditionalApply =>
        PlanDescriptionImpl(
          id,
          "ConditionalApply",
          children,
          Seq.empty,
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case _: Apply =>
        PlanDescriptionImpl(id, "Apply", children, Seq.empty, variables, withRawCardinalities, withDistinctness)

      case AssertSameNode(node, _, _) =>
        PlanDescriptionImpl(
          id,
          "AssertSameNode",
          children,
          Seq(Details(asPrettyString(node))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case AssertSameRelationship(idName, _, _) =>
        PlanDescriptionImpl(
          id,
          "AssertSameRelationship",
          children,
          Seq(Details(asPrettyString(idName))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case CartesianProduct(_, _) =>
        PlanDescriptionImpl(
          id,
          "CartesianProduct",
          children,
          Seq.empty,
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case NodeHashJoin(nodes, _, _) =>
        PlanDescriptionImpl(
          id,
          "NodeHashJoin",
          children,
          Seq(Details(keyNamesInfo(nodes.toSeq))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case ForeachApply(_, _, variable, expression) =>
        val details = pretty"${asPrettyString(variable)} IN ${asPrettyString(expression)}"
        PlanDescriptionImpl(
          id,
          "Foreach",
          children,
          Seq(Details(details)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case LetSelectOrSemiApply(_, _, _, predicate) =>
        PlanDescriptionImpl(
          id,
          "LetSelectOrSemiApply",
          children,
          Seq(Details(asPrettyString(predicate))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case row: plans.Argument =>
        ArgumentPlanDescription(id = plan.id, Seq.empty, variables)

      case LetSelectOrAntiSemiApply(_, _, _, predicate) =>
        PlanDescriptionImpl(
          id,
          "LetSelectOrAntiSemiApply",
          children,
          Seq(Details(asPrettyString(predicate))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case LetSemiApply(_, _, idName) =>
        PlanDescriptionImpl(
          id,
          "LetSemiApply",
          children,
          Seq(Details(asPrettyString(idName))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case _: LetAntiSemiApply =>
        PlanDescriptionImpl(
          id,
          "LetAntiSemiApply",
          children,
          Seq.empty,
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case LeftOuterHashJoin(nodes, _, _) =>
        PlanDescriptionImpl(
          id,
          "NodeLeftOuterHashJoin",
          children,
          Seq(Details(keyNamesInfo(nodes.toSeq))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case RightOuterHashJoin(nodes, _, _) =>
        PlanDescriptionImpl(
          id,
          "NodeRightOuterHashJoin",
          children,
          Seq(Details(keyNamesInfo(nodes.toSeq))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case RollUpApply(_, _, collectionName, variableToCollect) =>
        val detailsList = Seq(collectionName, variableToCollect).map(e => keyNamesInfo(Seq(e)))
        PlanDescriptionImpl(
          id,
          "RollUpApply",
          children,
          Seq(Details(detailsList)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case SelectOrAntiSemiApply(_, _, predicate) =>
        PlanDescriptionImpl(
          id,
          "SelectOrAntiSemiApply",
          children,
          Seq(Details(asPrettyString(predicate))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case SelectOrSemiApply(_, _, predicate) =>
        PlanDescriptionImpl(
          id,
          "SelectOrSemiApply",
          children,
          Seq(Details(asPrettyString(predicate))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case _: SemiApply =>
        PlanDescriptionImpl(id, "SemiApply", children, Seq.empty, variables, withRawCardinalities, withDistinctness)

      case TransactionForeach(
          _,
          _,
          batchSize,
          concurrency,
          onErrorBehaviour,
          maybeReportAs,
          maybeRetryParameters,
          _,
          effectiveDisjointBy
        ) =>
        val details =
          callInTxsDetails(
            batchSize,
            concurrency,
            onErrorBehaviour,
            maybeReportAs,
            maybeRetryParameters,
            effectiveDisjointBy
          )
        PlanDescriptionImpl(
          id,
          "TransactionForeach",
          children,
          Seq(details),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case TransactionApply(
          _,
          _,
          batchSize,
          concurrency,
          onErrorBehaviour,
          maybeReportAs,
          maybeRetryParameters,
          _,
          effectiveDisjointBy
        ) =>
        val details =
          callInTxsDetails(
            batchSize,
            concurrency,
            onErrorBehaviour,
            maybeReportAs,
            maybeRetryParameters,
            effectiveDisjointBy
          )
        PlanDescriptionImpl(
          id,
          "TransactionApply",
          children,
          Seq(details),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case TriadicSelection(_, _, positivePredicate, source, seen, target) =>
        val positivePredicateString = if (positivePredicate) pretty"" else pretty"NOT "
        val details =
          Details(pretty"WHERE $positivePredicateString(${asPrettyString(source)})--(${asPrettyString(target)})")
        PlanDescriptionImpl(
          id,
          "TriadicSelection",
          children,
          Seq(details),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case _: Union =>
        PlanDescriptionImpl(id, "Union", children, Seq.empty, variables, withRawCardinalities, withDistinctness)

      case _: OrderedUnion =>
        PlanDescriptionImpl(id, "OrderedUnion", children, Seq.empty, variables)

      case ValueHashJoin(_, _, predicate) =>
        PlanDescriptionImpl(
          id = id,
          name = "ValueHashJoin",
          children = children,
          arguments = Seq(Details(asPrettyString(predicate))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case ValueMergeJoin(_, _, predicate) =>
        PlanDescriptionImpl(
          id = id,
          name = "ValueMergeJoin",
          children = children,
          arguments = Seq(Details(asPrettyString(predicate))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case _: MultiNodeIndexSeek | _: AssertingMultiNodeIndexSeek | _: SubqueryForeach =>
        PlanDescriptionImpl(
          id = plan.id,
          plan.productPrefix,
          children,
          Seq.empty,
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case RepeatTrail(_, _, repetition, start, end, _, _, _, _, _, _, _, _, mode, allReduceMappings) =>
        PlanDescriptionImpl(
          id = plan.id,
          s"Repeat(${expandModeDescription(mode)}, Trail)",
          children,
          Seq(Details(repeatDetails(repetition, start, end, allReduceMappings))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case RepeatAcyclic(_, _, repetition, start, end, _, _, _, _, _, _, _, _, _, _, _, mode, allReduceMappings) =>
        PlanDescriptionImpl(
          id = plan.id,
          s"Repeat(${expandModeDescription(mode)}, Acyclic)",
          children,
          Seq(Details(repeatDetails(repetition, start, end, allReduceMappings))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case BidirectionalRepeatTrail(_, _, repetition, start, end, _, _, _, _, _, _, _, _) =>
        PlanDescriptionImpl(
          id = plan.id,
          "BidirectionalRepeat(Trail)",
          children,
          Seq(Details(repeatDetails(repetition, start, end, Set.empty))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case RepeatOptions(_, _) =>
        PlanDescriptionImpl(
          id = plan.id,
          "RepeatOptions",
          children,
          Seq.empty,
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case RepeatWalk(_, _, repetition, start, end, _, _, _, _, _, _, mode, allReduceMappings) =>
        PlanDescriptionImpl(
          id = plan.id,
          s"Repeat(${expandModeDescription(mode)}, Walk)",
          children,
          Seq(Details(repeatDetails(repetition, start, end, allReduceMappings))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case x => throw InternalException.internalError(
          this.getClass.getSimpleName,
          s"Unknown plan type: ${x.getClass.getSimpleName}. Missing a case?"
        )
    }

    addRuntimeAttributes(addPlanningAttributes(result, plan), plan)
  }

  private def repeatDetails(
    repetition: Repetition,
    start: LogicalVariable,
    end: LogicalVariable,
    inlinedAllReduceAccumulators: Set[AllReduceAccumulator]
  ): PrettyString = {
    val allReduceInitString = {
      inlinedAllReduceAccumulators.toSeq match {
        case Seq() => pretty""
        case accs =>
          accs
            .map(acc => pretty"  ${asPrettyString(acc.previous)} = ${asPrettyString(acc.initial)}")
            .sorted
            .mkPrettyString(
              "\n \ninlined allReduce() initializers:\n",
              "\n",
              ""
            )
      }
    }

    val repString = repetition match {
      case Repetition(min, Limited(n)) =>
        pretty"{${asPrettyString.raw(min.toString)}, ${asPrettyString.raw(n.toString)}}"
      case Repetition(min, Unlimited) =>
        pretty"{${asPrettyString.raw(min.toString)}, }"
    }
    pretty"(${asPrettyString(start.name)}) (...)$repString (${asPrettyString(end.name)})$allReduceInitString"
  }

  private def addPlanningAttributes(
    description: InternalPlanDescription,
    plan: LogicalPlan
  ): InternalPlanDescription = {
    Function.chain[InternalPlanDescription](Seq(
      if (effectiveCardinalities.isDefinedAt(plan.id)) {
        val effectiveCardinality = effectiveCardinalities.get(plan.id)
        _.addArgument(EstimatedRows(
          effectiveCardinality.amount,
          effectiveCardinality.originalCardinality.map(_.amount)
        ))
      } else {
        identity
      },
      if (providedOrders.isDefinedAt(plan.id) && !providedOrders.get(plan.id).isEmpty) {
        _.addArgument(asPrettyString.order(providedOrders.get(plan.id)))
      } else {
        identity
      },
      if (withDistinctness) {
        _.addArgument(asPrettyString.distinctness(plan.distinctness))
      } else {
        identity
      }
    ))(description)
  }

  private def addRuntimeAttributes(description: InternalPlanDescription, plan: LogicalPlan): InternalPlanDescription = {
    runtimeOperatorMetadata(plan.id).foldLeft(description)((acc, x) => acc.addArgument(x))
  }

  /**
   * @return (a prefix to put before the expand description, a description of the predicates to put after the expand description)
   */
  private def varExpandPredicateDescriptions(
    nodePredicates: Seq[VariablePredicate],
    relationshipPredicates: Seq[VariablePredicate],
    pathName: PrettyString = pretty"p"
  ): (PrettyString, PrettyString) = {
    val predicateStrings = nodePredicates.map(buildNodePredicatesDescription(_, pathName)) ++
      relationshipPredicates.map(buildRelationshipPredicatesDescription(_, pathName))

    val (expandDescriptionPrefix, predicatesDescription) =
      if (predicateStrings.isEmpty) {
        (pretty"", pretty"")
      } else {
        (pretty"$pathName = ", predicateStrings.mkPrettyString(" WHERE ", " AND ", ""))
      }
    (expandDescriptionPrefix, predicatesDescription)
  }

  private def buildNodePredicatesDescription(nodePredicate: VariablePredicate, pathName: PrettyString): PrettyString = {
    val nodePredicateInfo = asPrettyString(nodePredicate.predicate)
    val predVar = asPrettyString(nodePredicate.variable)
    pretty"all($predVar IN nodes($pathName) WHERE $nodePredicateInfo)"
  }

  private def buildRelationshipPredicatesDescription(
    relPredicate: VariablePredicate,
    pathName: PrettyString
  ): PrettyString = {
    val relPredicateInfo = asPrettyString(relPredicate.predicate)
    val predVar = asPrettyString(relPredicate.variable)
    pretty"all($predVar IN relationships($pathName) WHERE $relPredicateInfo)"
  }

  private def getNodeIndexDescriptions(
    idName: LogicalVariable,
    label: LabelToken,
    propertyKeys: Seq[PropertyKeyToken],
    indexType: IndexType,
    valueExpr: QueryExpression[expressions.Expression],
    unique: Boolean,
    readOnly: Boolean,
    caches: Seq[expressions.Expression]
  ): (String, PrettyString) = {

    val name = nodeIndexOperatorName(valueExpr, unique, readOnly)
    val predicate = indexPredicateString(Some(idName), propertyKeys, valueExpr)
    val info = nodeIndexInfoString(idName.name, unique, label, propertyKeys, indexType, predicate, caches)

    (name, info)
  }

  private def getRelIndexDescriptions(
    idName: Option[LogicalVariable],
    start: Option[LogicalVariable],
    typeToken: RelationshipTypeToken,
    end: Option[LogicalVariable],
    isDirected: Boolean,
    propertyKeys: Seq[PropertyKeyToken],
    indexType: IndexType,
    valueExpr: QueryExpression[expressions.Expression],
    unique: Boolean,
    readOnly: Boolean,
    caches: Seq[expressions.Expression]
  ): (String, PrettyString) = {

    val name = relationshipIndexOperatorName(valueExpr, unique, readOnly, isDirected)
    val predicate = indexPredicateString(idName, propertyKeys, valueExpr)
    val info = relIndexInfoString(idName, start, typeToken, end, isDirected, propertyKeys, indexType, predicate, caches)

    (name, info)
  }

  private def nodeIndexOperatorName(
    valueExpr: QueryExpression[expressions.Expression],
    unique: Boolean,
    readOnly: Boolean
  ): String = {
    indexOperatorName(NodeIndexSeek, valueExpr, unique, readOnly)
  }

  private def relationshipIndexOperatorName(
    valueExpr: QueryExpression[expressions.Expression],
    unique: Boolean,
    readOnly: Boolean,
    directed: Boolean
  ): String = {
    val indexSeekNames = if (directed) DirectedRelationshipIndexSeek else UndirectedRelationshipIndexSeek
    indexOperatorName(indexSeekNames, valueExpr, unique, readOnly)
  }

  private def indexOperatorName(
    indexSeekNames: IndexSeekNames,
    valueExpr: QueryExpression[expressions.Expression],
    unique: Boolean,
    readOnly: Boolean
  ): String = {
    def findName(exactOnly: Boolean = true) =
      if (unique && !readOnly && exactOnly) {
        indexSeekNames.PLAN_DESCRIPTION_UNIQUE_LOCKING_INDEX_SEEK_NAME
      } else if (unique) {
        indexSeekNames.PLAN_DESCRIPTION_UNIQUE_INDEX_SEEK_NAME
      } else {
        indexSeekNames.PLAN_DESCRIPTION_INDEX_SEEK_NAME
      }
    valueExpr match {
      case ExistenceQueryExpression =>
        indexSeekNames.PLAN_DESCRIPTION_INDEX_SCAN_NAME
      case _: RangeQueryExpression[?] =>
        if (unique) indexSeekNames.PLAN_DESCRIPTION_UNIQUE_INDEX_SEEK_RANGE_NAME
        else indexSeekNames.PLAN_DESCRIPTION_INDEX_SEEK_RANGE_NAME
      case e: CompositeQueryExpression[?] =>
        findName(e.exact)
      case _ =>
        findName()
    }
  }

  private def indexPredicateString(
    idName: Option[LogicalVariable],
    propertyKeys: Seq[PropertyKeyToken],
    valueExpr: QueryExpression[expressions.Expression]
  ): PrettyString = valueExpr match {
    case AllQueryExpression =>
      pretty"all(${asPrettyString(propertyKeys.head.name)})"

    case ExistenceQueryExpression =>
      pretty"${asPrettyString(propertyKeys.head.name)} IS NOT NULL"

    case NonExistenceQueryExpression =>
      pretty"${asPrettyString(propertyKeys.head.name)} IS NULL"

    case MatchEntitySetQueryExpression(expression) =>
      pretty"${asPrettyString(idName)} IN ${asPrettyString(expression)}"

    case MatchAllQueryExpression => pretty""

    case e: RangeQueryExpression[?] =>
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
          val poi = prettyPoint(point)
          val propertyKeyName = asPrettyString(propertyKeys.head.name)
          val distanceStr = asPrettyString(distance)
          pretty"point.distance($propertyKeyName, $poi) <${if (inclusive) pretty"=" else pretty""} $distanceStr"

        case PointBoundingBoxSeekRangeWrapper(PointBoundingBoxRange(ll, ur)) =>
          val pll = prettyPoint(ll)
          val pur = prettyPoint(ur)
          val propertyKeyName = asPrettyString(propertyKeys.head.name)
          pretty"point.withinBBox($propertyKeyName, $pll, $pur)"
        case _ =>
          throw new IllegalStateException("The expression did not conform to the expected type RangeQueryExpression")
      }

    case e: SingleQueryExpression[?] =>
      val propertyKeyName = asPrettyString(propertyKeys.head.name)
      pretty"$propertyKeyName = ${asPrettyString(e.expression)}"

    case e: ManyQueryExpression[?] =>
      val (eqOp, innerExp) = e.expression match {
        case ll @ ListLiteral(es) =>
          if (es.size == 1) (pretty"=", es.head) else (pretty"IN", ll)
        // This case is used for example when the expression in a parameter
        case x => (pretty"IN", x)
      }
      val propertyKeyName = asPrettyString(propertyKeys.head.name)
      pretty"$propertyKeyName $eqOp ${asPrettyString(innerExp)}"

    case e: CompositeQueryExpression[?] =>
      val predicates = e.inner.zipWithIndex.map {
        case (exp, i) => indexPredicateString(idName, Seq(propertyKeys(i)), exp)
      }
      predicates.mkPrettyString(" AND ")
  }

  private def rangeBoundString(
    propertyKey: PropertyKeyToken,
    bound: Bound[expressions.Expression],
    sign: Char
  ): PrettyString = {
    pretty"${asPrettyString(propertyKey.name)} ${asPrettyString.raw(s"$sign${bound.inequalitySignSuffix}")} ${asPrettyString(bound.endPoint)}"
  }

  private def prettyPoint(point: Expression) = {
    val funcName = Point.name
    point match {
      case FunctionInvocation(FunctionName(Namespace(List()), `funcName`), _, Seq(MapExpression(args)), _, _, _, _) =>
        pretty"point(${args.map(_._2).map(asPrettyString(_)).mkPrettyString(", ")})"
      case _ => asPrettyString(point)
    }
  }

  private def nodeCountFromCountStoreInfo(ident: LogicalVariable, labelNames: List[Option[LabelName]]): PrettyString = {
    val nodes = labelNames.map {
      case Some(label) => pretty"(:${asPrettyString(label.name)})"
      case None        => pretty"()"
    }.mkPrettyString(", ")
    pretty"count( $nodes ) AS ${asPrettyString(ident.name)}"
  }

  private def relationshipCountFromCountStoreInfo(
    ident: LogicalVariable,
    startLabel: Option[LabelName],
    typeNames: Seq[RelTypeName],
    endLabel: Option[LabelName]
  ): PrettyString = {
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
    val types =
      if (typeNames.nonEmpty) {
        typeNames
          .map(_.name)
          .map(asPrettyString(_))
          .mkPrettyString(":", "|", "")
      } else {
        pretty""
      }

    pretty"count( ($start)-[$types]->($end) ) AS ${asPrettyString(ident.name)}"
  }

  private def relationshipByIdSeekInfo(
    idName: Option[LogicalVariable],
    relIds: SeekableArgs,
    startNode: Option[LogicalVariable],
    endNode: Option[LogicalVariable],
    isDirectional: Boolean,
    functionName: String
  ): PrettyString = {
    val predicate = seekableArgsInfo(relIds)
    val directionString = if (isDirectional) pretty">" else pretty""
    val prettyStartNode = asPrettyString(startNode)
    val prettyIdName = idName.map(n => asPrettyString(n)).getOrElse(pretty"_")
    val prettyRelationship = idName.map(n => pretty"[${asPrettyString(n)}]").getOrElse(pretty"")
    val prettyEndNode = asPrettyString(endNode)
    pretty"($prettyStartNode)-$prettyRelationship-$directionString($prettyEndNode) WHERE ${asPrettyString(functionName)}($prettyIdName) $predicate"
  }

  private def seekableArgsInfo(seekableArgs: SeekableArgs): PrettyString = seekableArgs match {
    case ManySeekableArgs(ListLiteral(exprs)) if exprs.size == 1 =>
      pretty"= ${asPrettyString(exprs.head)}"
    case ManySeekableArgs(expr) =>
      pretty"IN ${asPrettyString(expr)}"
    case SingleSeekableArg(expr) =>
      pretty"= ${asPrettyString(expr)}"
  }

  private def signatureInfo(call: ResolvedNonLocalCall): PrettyString = {
    val argString = call.callArguments.map(asPrettyString(_)).mkPrettyString(SEPARATOR)
    val resultString = call.callResultTypes
      .map { case (name, typ) =>
        pretty"${asPrettyString(name)} :: ${asPrettyString.raw(typ.normalizedCypherTypeString())}"
      }
      .mkPrettyString(SEPARATOR)
    pretty"${asPrettyString.raw(call.procedureName.fullName)}($argString) :: ($resultString)"
  }

  private def orderInfo(orderBy: Seq[ColumnOrder]): PrettyString = {
    orderBy.map {
      case Ascending(id)  => pretty"${asPrettyString(id)} ASC"
      case Descending(id) => pretty"${asPrettyString(id)} DESC"
    }.mkPrettyString(SEPARATOR)
  }

  private def eagernessReasonInfo(reasons: ListSet[EagernessReason]): Seq[PrettyString] = {
    reasons.toSeq.flatMap(eagernessReasonDetails).sorted
  }

  private def eagernessReasonDetails(reason: EagernessReason): Seq[PrettyString] = reason match {
    case r: EagernessReason.NonUnique =>
      Seq(formatEagernessReason(nonUniqueEagernessReasonDetails(r)))
    case r: EagernessReason.ReasonWithConflict =>
      Seq(formatEagernessReason(nonUniqueEagernessReasonDetails(r.reason), Some(r.conflict)))
    case EagernessReason.UpdateStrategyEager =>
      Seq(formatEagernessReason(pretty"updateStrategy=eager"))
    case EagernessReason.WriteAfterCallInTransactions =>
      Seq(formatEagernessReason(pretty"write after CALL { ... } IN TRANSACTIONS"))
    case EagernessReason.Summarized(summary) =>
      summary.map { case (reason, EagernessReason.SummaryEntry(conflict, count)) =>
        formatEagernessReason(
          nonUniqueEagernessReasonDetails(reason),
          Some(conflict),
          Option.when(count > 1)(count - 1)
        )
      }.toSeq
    case EagernessReason.ProcedureCallEager =>
      Seq(formatEagernessReason(pretty"Eager ProcedureCall"))
    case EagernessReason.Unknown =>
      Seq.empty
  }

  private def formatEagernessReason(
    detail: PrettyString,
    conflict: Option[EagernessReason.Conflict] = None,
    extraCount: Option[Int] = None
  ): PrettyString = {
    val extraCountStr = extraCount.fold("")(c => s", and $c more conflicting operators")
    val conflictStr = conflict.fold("")(c => s" (${conflictInfo(c)}$extraCountStr)")
    pretty"$detail${asPrettyString.raw(conflictStr)}"
  }

  private def nonUniqueEagernessReasonDetails(reason: EagernessReason.NonUnique): PrettyString = {
    reason match {
      case EagernessReason.LabelReadSetConflict(label) =>
        pretty"read/set conflict for label: ${asPrettyString(label)}"
      case EagernessReason.TypeReadSetConflict(label) =>
        (pretty"read/set conflict for relationship type: ${asPrettyString(label)}")
      case EagernessReason.LabelReadRemoveConflict(label) =>
        (pretty"read/remove conflict for label: ${asPrettyString(label)}")
      case EagernessReason.ReadDeleteConflict(identifier) =>
        (pretty"read/delete conflict for variable: ${asPrettyString(identifier)}")
      case EagernessReason.ReadCreateConflict =>
        (pretty"read/create conflict")
      case EagernessReason.PropertyReadSetConflict(property) =>
        (pretty"read/set conflict for property: ${asPrettyString(property)}")
      case EagernessReason.UnknownPropertyReadSetConflict =>
        (pretty"read/set conflict for some property")
      case EagernessReason.UnknownLabelReadRemoveConflict =>
        (pretty"read/remove conflict for some label")
      case EagernessReason.UnknownLabelReadSetConflict =>
        (pretty"read/set conflict for some label")
    }
  }

  private def conflictInfo(conflict: EagernessReason.Conflict): String =
    s"Operator: ${conflict.first.x} vs ${conflict.second.x}"

  private def relationshipPattern(
    from: Option[LogicalVariable],
    maybeRelName: Option[LogicalVariable],
    relTypes: Seq[RelTypeExpression],
    to: Option[LogicalVariable],
    direction: SemanticDirection
  ): PrettyString =
    expandExpressionDescription(from, maybeRelName, relTypes, to, direction, SimplePatternLength)

  private def expandExpressionDescription(
    from: Option[LogicalVariable],
    maybeRelName: Option[LogicalVariable],
    relTypes: Seq[RelTypeExpression],
    to: Option[LogicalVariable],
    direction: SemanticDirection,
    patternLength: PatternLength
  ): PrettyString = {
    val (min, maybeMax) = patternLength match {
      case SimplePatternLength             => (1, Some(1))
      case VarPatternLength(min, maybeMax) => (min, maybeMax)
    }

    expandExpressionDescription(from, maybeRelName, relTypes, to, direction, min, maybeMax, None)
  }

  private def createNodeDescription(cn: CreateNode) = {
    val CreateNode(node, labels, labelExpressions, properties) = cn
    val separator = if (labels.isEmpty && labelExpressions.isEmpty) pretty": " else pretty" "
    val labelsString =
      if (labels.nonEmpty) labels.map(x => asPrettyString(x.name)).mkPrettyString(":", ":", "") else pretty""
    val labelExpressionsString =
      if (labelExpressions.nonEmpty) labelExpressions.map(asPrettyString(_)).mkPrettyString(":$(", "):$(", ")")
      else pretty""
    val propsString = properties.map(p => pretty"$separator${asPrettyString(p)}").getOrElse(pretty"")
    pretty"(${asPrettyString(node)}$labelsString$labelExpressionsString$propsString)"
  }

  private def expandExpressionDescription(
    maybeFromName: Option[LogicalVariable],
    maybeRelName: Option[LogicalVariable],
    relTypes: Seq[RelTypeExpression],
    maybeToName: Option[LogicalVariable],
    direction: SemanticDirection,
    minLength: Int,
    maxLength: Option[Int],
    maybeProperties: Option[Expression]
  ): PrettyString = {
    val left = if (direction == SemanticDirection.INCOMING) pretty"<-" else pretty"-"
    val right = if (direction == SemanticDirection.OUTGOING) pretty"->" else pretty"-"
    val types = if (relTypes.isEmpty) pretty""
    else relTypes.map {
      case e: RelTypeName => asPrettyString(e)
      case DynamicRelTypeExpression(expr, all) =>
        val inner = asPrettyString(expr)
        val selector = if (all) pretty"all" else pretty"any"
        pretty"$$$selector($inner)"
    }.mkPrettyString(":", "|", "")
    val separator = if (relTypes.isEmpty) pretty": " else pretty" "
    val propsString = maybeProperties.map(p => pretty"$separator${asPrettyString(p)}").getOrElse(pretty"")
    val lengthDescr: PrettyString = (minLength, maxLength) match {
      case (1, Some(1)) => pretty""
      case (1, None)    => pretty"*"
      case (1, Some(m)) => pretty"*..${asPrettyString.raw(m.toString)}"
      case (`minLength`, Some(`minLength`)) =>
        pretty"*${asPrettyString.raw(minLength.toString)}"
      case _ =>
        pretty"*${asPrettyString.raw(minLength.toString)}..${asPrettyString.raw(maxLength.map(_.toString).getOrElse(""))}"
    }
    val relName = asPrettyString(maybeRelName.map(_.name).getOrElse(""))
    val relInfo =
      if (lengthDescr == pretty"" && relTypes.isEmpty && relName.prettifiedString.isEmpty) pretty""
      else pretty"[$relName$types$lengthDescr$propsString]"
    pretty"(${asPrettyString(maybeFromName.map(_.name).getOrElse(""))})$left$relInfo$right(${asPrettyString(maybeToName.map(_.name).getOrElse(""))})"
  }

  private def nodeIndexInfoString(
    idName: String,
    unique: Boolean,
    label: NameToken[_],
    propertyKeys: Seq[PropertyKeyToken],
    indexType: IndexType,
    predicate: PrettyString,
    caches: Seq[expressions.Expression]
  ): PrettyString = {
    val indexStr = if (unique) pretty"UNIQUE " else pretty"${asPrettyString(indexType.name())} INDEX "
    val propertyKeyString = propertyKeys.map(x => asPrettyString(x.name)).mkPrettyString(SEPARATOR)
    pretty"$indexStr${asPrettyString(idName)}:${asPrettyString(label.name)}($propertyKeyString) WHERE $predicate${cachesSuffix(caches)}"
  }

  private def relIndexInfoString(
    idName: Option[LogicalVariable],
    start: Option[LogicalVariable],
    relType: NameToken[_],
    end: Option[LogicalVariable],
    isDirected: Boolean,
    propertyKeys: Seq[PropertyKeyToken],
    indexType: IndexType,
    predicate: PrettyString,
    caches: Seq[expressions.Expression]
  ): PrettyString = {
    val propertyKeyString = propertyKeys.map(x => asPrettyString(x.name)).mkPrettyString(SEPARATOR)
    val left = pretty"-"
    val right = if (isDirected) pretty"->" else pretty"-"
    val relInfo = pretty"[${asPrettyString(idName)}:${asPrettyString(relType.name)}($propertyKeyString)]"
    val pattern = pretty"(${asPrettyString(start)})$left$relInfo$right(${asPrettyString(end)})"
    pretty"${asPrettyString(indexType.name())} INDEX $pattern WHERE $predicate${cachesSuffix(caches)}"
  }

  private def aggregationInfo(
    groupingExpressions: Map[LogicalVariable, Expression],
    aggregationExpressions: Map[LogicalVariable, Expression],
    ordered: Seq[Expression] = Seq.empty
  ): PrettyString = {
    val sanitizedOrdered = ordered.map(asPrettyString(_)).toIndexedSeq
    val groupingInfo = projectedExpressionInfo(groupingExpressions, sanitizedOrdered)
    val aggregatingInfo = projectedExpressionInfo(aggregationExpressions)
    (groupingInfo ++ aggregatingInfo).mkPrettyString(SEPARATOR)
  }

  private def projectedExpressionInfo(
    expressions: Map[LogicalVariable, Expression],
    ordered: IndexedSeq[PrettyString] = IndexedSeq.empty
  ): Seq[PrettyString] = {
    expressions.toList.map { case (k, v) =>
      val key = asPrettyString(k)
      val value = asPrettyString(v)
      (key, value)
    }.sortBy {
      case (key, _) if ordered.contains(key)     => ordered.indexOf(key)
      case (_, value) if ordered.contains(value) => ordered.indexOf(value)
      case _                                     => Int.MaxValue
    }.map { case (key, value) => if (key == value) key else pretty"$value AS $key" }
  }

  private def keyNamesInfo(keys: Iterable[LogicalVariable]): PrettyString = {
    keys
      .map(asPrettyString(_))
      .mkPrettyString(SEPARATOR)
  }

  private def cachesSuffix(caches: Seq[expressions.Expression]): PrettyString = {
    if (caches.isEmpty) pretty"" else caches.map(asPrettyString(_)).mkPrettyString(", ", ", ", "")
  }

  private def indexInfo(
    indexType: String,
    nameOption: Option[Expression],
    entityName: ElementTypeName,
    properties: Seq[PropertyKeyName],
    options: Options
  ): PrettyString = {
    val propertyString = properties.map(asPrettyString(_)).mkPrettyString("(", SEPARATOR, ")")
    val pattern = entityName match {
      case label: LabelName =>
        val prettyLabel = asPrettyString(label.name)
        pretty"(:$prettyLabel)"
      case relType: RelTypeName =>
        val prettyType = asPrettyString(relType.name)
        pretty"()-[:$prettyType]-()"
      case dynamicLabel @ DynamicLabelExpression(_, all) if all =>
        val prettyLabel = asPrettyString(dynamicLabel.expression)
        pretty"(:all$$($prettyLabel))"
      case dynamicLabel: DynamicLabelExpression =>
        val prettyLabel = asPrettyString(dynamicLabel.expression)
        pretty"(:any$$($prettyLabel))"
      case dynamicType @ DynamicRelTypeExpression(_, all) if all =>
        val prettyType = asPrettyString(dynamicType.expression)
        pretty"()-[:all$$($prettyType)]-()"
      case dynamicType: DynamicRelTypeExpression =>
        val prettyType = asPrettyString(dynamicType.expression)
        pretty"()-[:any$$($prettyType)]-()"
    }
    indexInfoString(indexType, nameOption, pattern, propertyString, options)
  }

  private def fulltextIndexInfo(
    nameOption: Option[Expression],
    entityNames: Either[List[LabelName], List[RelTypeName]],
    properties: Seq[PropertyKeyName],
    options: Options
  ): PrettyString = {
    val propertyString = properties.map(asPrettyString(_)).mkPrettyString("[", SEPARATOR, "]")
    val pattern = entityNames match {
      case Left(labels) =>
        val innerPattern = labels.map(l => asPrettyString(l.name)).mkPrettyString(":", "|", "")
        pretty"($innerPattern)"
      case Right(relTypes) =>
        val innerPattern = relTypes.map(r => asPrettyString(r.name)).mkPrettyString(":", "|", "")
        pretty"()-[$innerPattern]-()"
    }
    indexInfoString("FULLTEXT", nameOption, pattern, pretty"EACH $propertyString", options)
  }

  private def vectorIndexInfo(
    nameOption: Option[Expression],
    entityNames: Either[List[LabelName], List[RelTypeName]],
    properties: Seq[PropertyKeyName],
    additionalProperties: Seq[PropertyKeyName],
    options: Options
  ): PrettyString = {
    val propertyString = properties.map(asPrettyString(_)).mkPrettyString("(", SEPARATOR, ")")
    val additionalPropertiesString = if (additionalProperties.nonEmpty)
      additionalProperties.map(asPrettyString(_)).mkPrettyString(" WITH [", SEPARATOR, "]")
    else pretty""
    val pattern = entityNames match {
      case Left(labels) =>
        val innerPattern = labels.map(l => asPrettyString(l.name)).mkPrettyString(":", "|", "")
        pretty"($innerPattern)"
      case Right(relTypes) =>
        val innerPattern = relTypes.map(r => asPrettyString(r.name)).mkPrettyString(":", "|", "")
        pretty"()-[$innerPattern]-()"
    }
    indexInfoString("VECTOR", nameOption, pattern, pretty"$propertyString$additionalPropertiesString", options)
  }

  private def lookupIndexInfo(
    nameOption: Option[Expression],
    entityType: EntityType,
    options: Options
  ): PrettyString = {
    val (pattern, function) = entityType match {
      case EntityType.NODE         => (pretty"(n)", pretty"${asPrettyString.raw(Labels.name)}(n)")
      case EntityType.RELATIONSHIP => (pretty"()-[r]-()", pretty"${asPrettyString.raw(Type.name)}(r)")
    }
    indexInfoString("LOOKUP", nameOption, pattern, pretty"EACH $function", options)
  }

  private def indexInfoString(
    indexType: String,
    nameOption: Option[Expression],
    nodeOrRelPattern: PrettyString,
    onDefinition: PrettyString,
    options: Options
  ) = {
    val name = getPrettyStringName(nameOption)
    pretty"${asPrettyString.raw(indexType)} INDEX$name FOR $nodeOrRelPattern ON $onDefinition${prettyOptions(options)}"
  }

  private def constraintInfo(
    nameOption: Option[Expression],
    entity: String,
    entityName: ElementTypeName,
    properties: Seq[Property],
    constraintType: CreateConstraintType,
    options: Options = NoOptions,
    useForAndRequire: Boolean = true
  ): PrettyString = {
    val name = getPrettyStringName(nameOption)
    val prettyAssertion = asPrettyString.raw(constraintType.predicate)

    val propertyString = properties.map(asPrettyString(_)).mkPrettyString("(", SEPARATOR, ")")
    val prettyEntity = asPrettyString(entity)

    val entityInfo = entityName match {
      case label: LabelName     => pretty"($prettyEntity:${asPrettyString(label)})"
      case relType: RelTypeName => pretty"()-[$prettyEntity:${asPrettyString(relType)}]-()"
      case dynamicLabel @ DynamicLabelExpression(_, all) if all =>
        val prettyLabel = asPrettyString(dynamicLabel.expression)
        pretty"($prettyEntity:all$$($prettyLabel))"
      case dynamicLabel: DynamicLabelExpression =>
        val prettyLabel = asPrettyString(dynamicLabel.expression)
        pretty"($prettyEntity:any$$($prettyLabel))"
      case dynamicType @ DynamicRelTypeExpression(_, all) if all =>
        val prettyType = asPrettyString(dynamicType.expression)
        pretty"()-[$prettyEntity:all$$($prettyType)]-()"
      case dynamicType: DynamicRelTypeExpression =>
        val prettyType = asPrettyString(dynamicType.expression)
        pretty"()-[$prettyEntity:any$$($prettyType)]-()"
    }
    val onOrFor = if (useForAndRequire) pretty"FOR" else pretty"ON"
    val assertOrRequire = if (useForAndRequire) pretty"REQUIRE" else pretty"ASSERT"

    pretty"CONSTRAINT$name $onOrFor $entityInfo $assertOrRequire $propertyString $prettyAssertion${prettyOptions(options)}"
  }

  private def setPropertyInfo(idName: PrettyString, expression: Expression, removeOtherProps: Boolean): PrettyString = {
    val setString = if (removeOtherProps) pretty"=" else pretty"+="

    pretty"$idName $setString ${asPrettyString(expression)}"
  }

  def mutatingPatternString(setOp: SimpleMutatingPattern): PrettyString = setOp match {
    case CreatePattern(commands) =>
      val details = commands.map {
        case c: CreateNode => createNodeDescription(c)
        case CreateRelationship(relationship, startNode, typ, endNode, direction, properties) =>
          expandExpressionDescription(
            Some(startNode),
            Some(relationship),
            Seq(typ),
            Some(endNode),
            direction,
            1,
            Some(1),
            properties
          )
      }
      pretty"CREATE ${details.mkPrettyString(", ")}"
    case org.neo4j.cypher.internal.ir.DeleteExpression(toDelete, forced) =>
      if (forced) pretty"DETACH DELETE ${asPrettyString(toDelete)}" else pretty"DELETE ${asPrettyString(toDelete)}"
    case SetLabelPattern(node, labelNames, dynamicLabels) =>
      val prettyId = asPrettyString(node)
      val prettyLabels = prettyUpdateLabelString(labelNames, dynamicLabels)
      pretty"SET $prettyId$prettyLabels"
    case RemoveLabelPattern(node, labelNames, dynamicLabels) =>
      val prettyId = asPrettyString(node)
      val prettyLabels = prettyUpdateLabelString(labelNames, dynamicLabels)
      pretty"REMOVE $prettyId$prettyLabels"
    case SetNodePropertyPattern(node, propertyKey, value) =>
      pretty"SET ${setPropertyInfo(pretty"${asPrettyString(node)}.${asPrettyString(propertyKey.name)}", value, removeOtherProps = true)}"
    case SetNodePropertiesFromMapPattern(node, value, removeOtherProps) =>
      pretty"SET ${setPropertyInfo(asPrettyString(node), value, removeOtherProps)}"
    case SetRelationshipPropertyPattern(relationship, propertyKey, value) =>
      pretty"SET ${setPropertyInfo(pretty"${asPrettyString(relationship)}.${asPrettyString(propertyKey.name)}", value, removeOtherProps = true)}"
    case SetRelationshipPropertiesFromMapPattern(relationship, value, removeOtherProps) =>
      pretty"SET ${setPropertyInfo(asPrettyString(relationship), value, removeOtherProps)}"
    case SetPropertyPattern(entity, propertyKey, expression) =>
      val entityString = pretty"${asPrettyString(entity)}.${asPrettyString(propertyKey.name)}"
      pretty"SET ${setPropertyInfo(entityString, expression, true)}"
    case SetDynamicPropertyPattern(entity, propertyKey, expression) =>
      val entityString = pretty"${asPrettyString(entity)}.${asPrettyString(propertyKey)}"
      pretty"SET ${setPropertyInfo(entityString, expression, true)}"
    case SetPropertiesFromMapPattern(entity, expression, removeOtherProps) =>
      pretty"SET ${setPropertyInfo(asPrettyString(entity), expression, removeOtherProps)}"
    case SetPropertiesPattern(entity, items) =>
      val setOps = items.map {
        case (p, e) =>
          val entityString = pretty"${asPrettyString(entity)}.${asPrettyString(p.name)}"
          setPropertyInfo(entityString, e, removeOtherProps = true)
      }.mkPrettyString(", ")
      pretty"SET $setOps"
    case SetNodePropertiesPattern(entity, items) =>
      val setOps = items.map {
        case (p, e) =>
          val entityString = pretty"${asPrettyString(entity)}.${asPrettyString(p.name)}"
          setPropertyInfo(entityString, e, removeOtherProps = true)
      }.mkPrettyString(", ")
      pretty"SET $setOps"
    case SetRelationshipPropertiesPattern(entity, items) =>
      val setOps = items.map {
        case (p, e) =>
          val entityString = pretty"${asPrettyString(entity)}.${asPrettyString(p.name)}"
          setPropertyInfo(entityString, e, removeOtherProps = true)
      }.mkPrettyString(", ")
      pretty"SET $setOps"
  }

  private def commandColumnInfo(yieldColumns: List[CommandYieldColumn], yieldAll: Boolean): PrettyString =
    if (yieldColumns.nonEmpty)
      asPrettyString.raw(yieldColumns.map(y => {
        val variableName = y.originalName
        val aliasName = y.aliasedName

        if (!variableName.equals(aliasName)) s"$variableName AS $aliasName"
        else variableName
      }).mkString("columns(", ", ", ")"))
    else if (yieldAll) pretty"allColumns"
    else pretty"defaultColumns"

  private def getInnerNameDescriptions(names: CommandClauseNames): Option[PrettyString] = names match {
    case NoNames => None
    case CommaSeparatedNames(ls) =>
      val es = ls.expressions.map(asPrettyString(_))
      Some(asPrettyString.raw(es.mkString(", ")))
    case ExpressionNames(e) => Some(asPrettyString(e))
  }

  private def formatPropertyPredicatesForDynamicIndexSeek(
    idName: PrettyString,
    propertyPredicates: Map[PropertyKeyToken, Expression]
  ): PrettyString = {
    if (propertyPredicates.isEmpty) {
      pretty""
    } else {
      pretty"\n"
        .append(propertyPredicates
          .map { case (prop, expr) =>
            pretty"$idName.${asPrettyString.raw(prop.name)} = ${asPrettyString(expr)}"
          }
          .mkPrettyString("\n"))
    }
  }
}
