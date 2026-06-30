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
package org.neo4j.cypher.internal.logical.builder

import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.AscSortItem
import org.neo4j.cypher.internal.ast.DescSortItem
import org.neo4j.cypher.internal.ast.NonOptional
import org.neo4j.cypher.internal.ast.OptionalState
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsDisjointByParameters
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorFail
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorRetryThenFail
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsRetryParameters
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.expressions.AllReduceAccumulator
import org.neo4j.cypher.internal.expressions.Ands
import org.neo4j.cypher.internal.expressions.AndsReorderable
import org.neo4j.cypher.internal.expressions.CachedProperty
import org.neo4j.cypher.internal.expressions.DecimalDoubleLiteral
import org.neo4j.cypher.internal.expressions.DynamicRelTypeExpression
import org.neo4j.cypher.internal.expressions.EntityType
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.ExplicitParameter
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.False
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.HasLabels
import org.neo4j.cypher.internal.expressions.HasLabelsOrTypes
import org.neo4j.cypher.internal.expressions.HasTypes
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.LabelToken
import org.neo4j.cypher.internal.expressions.ListComprehension
import org.neo4j.cypher.internal.expressions.ListLiteral
import org.neo4j.cypher.internal.expressions.LogicalProperty
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.MapExpression
import org.neo4j.cypher.internal.expressions.NODE_TYPE
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.expressions.PathLengthQuantifier
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.PropertyKeyToken
import org.neo4j.cypher.internal.expressions.RELATIONSHIP_TYPE
import org.neo4j.cypher.internal.expressions.Range
import org.neo4j.cypher.internal.expressions.RelTypeExpression
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.RelationshipChain
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.RelationshipTypeToken
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.expressions.ShortestPathsPatternPart
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.expressions.True
import org.neo4j.cypher.internal.expressions.UnPositionedVariable.varFor
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.expressions.VariableGrouping
import org.neo4j.cypher.internal.frontend.phases.ProcedureSignature
import org.neo4j.cypher.internal.frontend.phases.ResolvedFunctionInvocation
import org.neo4j.cypher.internal.frontend.phases.ResolvedNonLocalCall
import org.neo4j.cypher.internal.frontend.phases.UserFunctionSignature
import org.neo4j.cypher.internal.ir.CSVFormat
import org.neo4j.cypher.internal.ir.CreateCommand
import org.neo4j.cypher.internal.ir.CreateNode
import org.neo4j.cypher.internal.ir.CreatePattern
import org.neo4j.cypher.internal.ir.CreateRelationship
import org.neo4j.cypher.internal.ir.EagernessReason
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
import org.neo4j.cypher.internal.label_expressions.LabelExpression.disjoinRelTypesToLabelExpression
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.AcyclicParameters
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.Predicate
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.PushdownOperators
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.ToExpression
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.TrailParameters
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.WalkParameters
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.pos
import org.neo4j.cypher.internal.logical.plans.Aggregation
import org.neo4j.cypher.internal.logical.plans.AllNodesScan
import org.neo4j.cypher.internal.logical.plans.Anti
import org.neo4j.cypher.internal.logical.plans.AntiConditionalApply
import org.neo4j.cypher.internal.logical.plans.AntiSemiApply
import org.neo4j.cypher.internal.logical.plans.Apply
import org.neo4j.cypher.internal.logical.plans.Argument
import org.neo4j.cypher.internal.logical.plans.ArgumentTracker
import org.neo4j.cypher.internal.logical.plans.AssertSameNode
import org.neo4j.cypher.internal.logical.plans.AssertSameRelationship
import org.neo4j.cypher.internal.logical.plans.BFSPruningVarExpand
import org.neo4j.cypher.internal.logical.plans.BidirectionalRepeatTrail
import org.neo4j.cypher.internal.logical.plans.CacheProperties
import org.neo4j.cypher.internal.logical.plans.CartesianProduct
import org.neo4j.cypher.internal.logical.plans.CoerceToPredicate
import org.neo4j.cypher.internal.logical.plans.Column
import org.neo4j.cypher.internal.logical.plans.ColumnOrder
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
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipFulltextIndexSearch
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipTypeScan
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipVectorIndexSearch
import org.neo4j.cypher.internal.logical.plans.DirectedUnionRelationshipTypesScan
import org.neo4j.cypher.internal.logical.plans.Distinct
import org.neo4j.cypher.internal.logical.plans.DoNotGetValue
import org.neo4j.cypher.internal.logical.plans.DynamicDirectedRelationshipTypeLookup
import org.neo4j.cypher.internal.logical.plans.DynamicElement
import org.neo4j.cypher.internal.logical.plans.DynamicElement.SetOperator
import org.neo4j.cypher.internal.logical.plans.DynamicLabelNodeLookup
import org.neo4j.cypher.internal.logical.plans.DynamicUndirectedRelationshipTypeLookup
import org.neo4j.cypher.internal.logical.plans.Eager
import org.neo4j.cypher.internal.logical.plans.EmptyResult
import org.neo4j.cypher.internal.logical.plans.EntityFilterQueryExpression
import org.neo4j.cypher.internal.logical.plans.ErrorPlan
import org.neo4j.cypher.internal.logical.plans.ExhaustiveLimit
import org.neo4j.cypher.internal.logical.plans.Expand
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandAll
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandInto
import org.neo4j.cypher.internal.logical.plans.Expand.ExpansionMode
import org.neo4j.cypher.internal.logical.plans.Expand.VariablePredicate
import org.neo4j.cypher.internal.logical.plans.FindShortestPaths
import org.neo4j.cypher.internal.logical.plans.FindShortestPaths.DisallowSameNode
import org.neo4j.cypher.internal.logical.plans.FindShortestPaths.SameNodeMode
import org.neo4j.cypher.internal.logical.plans.Foreach
import org.neo4j.cypher.internal.logical.plans.ForeachApply
import org.neo4j.cypher.internal.logical.plans.FusedMerge
import org.neo4j.cypher.internal.logical.plans.GetValue
import org.neo4j.cypher.internal.logical.plans.GetValueFromIndexBehavior
import org.neo4j.cypher.internal.logical.plans.IndexOrder
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.logical.plans.IndexedProperty
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
import org.neo4j.cypher.internal.logical.plans.LockNodes
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.ManySeekableArgs
import org.neo4j.cypher.internal.logical.plans.MatchAllQueryExpression
import org.neo4j.cypher.internal.logical.plans.Merge
import org.neo4j.cypher.internal.logical.plans.MergeInto
import org.neo4j.cypher.internal.logical.plans.MergeUniqueNode
import org.neo4j.cypher.internal.logical.plans.MultiNodeIndexSeek
import org.neo4j.cypher.internal.logical.plans.NFA
import org.neo4j.cypher.internal.logical.plans.NestedPlanCollectExpression
import org.neo4j.cypher.internal.logical.plans.NestedPlanExistsExpression
import org.neo4j.cypher.internal.logical.plans.NestedPlanGetByNameExpression
import org.neo4j.cypher.internal.logical.plans.NodeByElementIdSeek
import org.neo4j.cypher.internal.logical.plans.NodeByIdSeek
import org.neo4j.cypher.internal.logical.plans.NodeByLabelScan
import org.neo4j.cypher.internal.logical.plans.NodeCountFromCountStore
import org.neo4j.cypher.internal.logical.plans.NodeFulltextIndexSearch
import org.neo4j.cypher.internal.logical.plans.NodeHashJoin
import org.neo4j.cypher.internal.logical.plans.NodeIndexLeafPlan
import org.neo4j.cypher.internal.logical.plans.NodeIndexSeek
import org.neo4j.cypher.internal.logical.plans.NodeIndexSeekSingleLabelLeafPlan
import org.neo4j.cypher.internal.logical.plans.NodeVectorIndexSearch
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
import org.neo4j.cypher.internal.logical.plans.PartitionedDirectedRelationshipTypeScan
import org.neo4j.cypher.internal.logical.plans.PartitionedDirectedUnionRelationshipTypesScan
import org.neo4j.cypher.internal.logical.plans.PartitionedIntersectionNodeByLabelsScan
import org.neo4j.cypher.internal.logical.plans.PartitionedNodeByLabelScan
import org.neo4j.cypher.internal.logical.plans.PartitionedSubtractionNodeByLabelsScan
import org.neo4j.cypher.internal.logical.plans.PartitionedUndirectedAllRelationshipsScan
import org.neo4j.cypher.internal.logical.plans.PartitionedUndirectedRelationshipTypeScan
import org.neo4j.cypher.internal.logical.plans.PartitionedUndirectedUnionRelationshipTypesScan
import org.neo4j.cypher.internal.logical.plans.PartitionedUnionNodeByLabelsScan
import org.neo4j.cypher.internal.logical.plans.PartitionedUnwindCollection
import org.neo4j.cypher.internal.logical.plans.PathPropagatingBFS
import org.neo4j.cypher.internal.logical.plans.PipelineBreaker
import org.neo4j.cypher.internal.logical.plans.PointBoundingBoxRange
import org.neo4j.cypher.internal.logical.plans.PointBoundingBoxSeekRangeWrapper
import org.neo4j.cypher.internal.logical.plans.PointDistanceRange
import org.neo4j.cypher.internal.logical.plans.PointDistanceSeekRangeWrapper
import org.neo4j.cypher.internal.logical.plans.Prober
import org.neo4j.cypher.internal.logical.plans.Prober.FlowProbe
import org.neo4j.cypher.internal.logical.plans.Prober.NoopFlowProbe
import org.neo4j.cypher.internal.logical.plans.ProcedureCall
import org.neo4j.cypher.internal.logical.plans.ProduceResult
import org.neo4j.cypher.internal.logical.plans.ProjectEndpoints
import org.neo4j.cypher.internal.logical.plans.Projection
import org.neo4j.cypher.internal.logical.plans.PropertyKeyNameOrder
import org.neo4j.cypher.internal.logical.plans.PruningVarExpand
import org.neo4j.cypher.internal.logical.plans.QueryExpression
import org.neo4j.cypher.internal.logical.plans.RangeQueryExpression
import org.neo4j.cypher.internal.logical.plans.RelationshipCountFromCountStore
import org.neo4j.cypher.internal.logical.plans.RelationshipIndexLeafPlan
import org.neo4j.cypher.internal.logical.plans.RemoteBatchProperties
import org.neo4j.cypher.internal.logical.plans.RemoteBatchPropertiesWithFilter
import org.neo4j.cypher.internal.logical.plans.RemoteBatchPropertiesWithPushdownOperators
import org.neo4j.cypher.internal.logical.plans.RemoveLabels
import org.neo4j.cypher.internal.logical.plans.RepeatAcyclic
import org.neo4j.cypher.internal.logical.plans.RepeatOptions
import org.neo4j.cypher.internal.logical.plans.RepeatTrail
import org.neo4j.cypher.internal.logical.plans.RepeatWalk
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
import org.neo4j.cypher.internal.logical.plans.SimulatedExpand
import org.neo4j.cypher.internal.logical.plans.SimulatedNodeScan
import org.neo4j.cypher.internal.logical.plans.SimulatedSelection
import org.neo4j.cypher.internal.logical.plans.Skip
import org.neo4j.cypher.internal.logical.plans.Sort
import org.neo4j.cypher.internal.logical.plans.StatefulShortestPath
import org.neo4j.cypher.internal.logical.plans.StatefulShortestPath.LengthBounds
import org.neo4j.cypher.internal.logical.plans.StatefulShortestPath.Mapping
import org.neo4j.cypher.internal.logical.plans.SubqueryForeach
import org.neo4j.cypher.internal.logical.plans.SubtractionNodeByLabelsScan
import org.neo4j.cypher.internal.logical.plans.Top
import org.neo4j.cypher.internal.logical.plans.Top1WithTies
import org.neo4j.cypher.internal.logical.plans.TransactionApply
import org.neo4j.cypher.internal.logical.plans.TransactionConcurrency
import org.neo4j.cypher.internal.logical.plans.TransactionForeach
import org.neo4j.cypher.internal.logical.plans.TraversalPathMode
import org.neo4j.cypher.internal.logical.plans.TraversalPathMode.Trail
import org.neo4j.cypher.internal.logical.plans.TriadicBuild
import org.neo4j.cypher.internal.logical.plans.TriadicFilter
import org.neo4j.cypher.internal.logical.plans.TriadicSelection
import org.neo4j.cypher.internal.logical.plans.UndirectedAllRelationshipsScan
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipByElementIdSeek
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipByIdSeek
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipFulltextIndexSearch
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipTypeScan
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipVectorIndexSearch
import org.neo4j.cypher.internal.logical.plans.UndirectedUnionRelationshipTypesScan
import org.neo4j.cypher.internal.logical.plans.Union
import org.neo4j.cypher.internal.logical.plans.UnionNodeByLabelsScan
import org.neo4j.cypher.internal.logical.plans.UnwindCollection
import org.neo4j.cypher.internal.logical.plans.ValueHashJoin
import org.neo4j.cypher.internal.logical.plans.ValueMergeJoin
import org.neo4j.cypher.internal.logical.plans.VarExpand
import org.neo4j.cypher.internal.rewriting.rewriters.astRewriters.DesugarMapProjection
import org.neo4j.cypher.internal.rewriting.rewriters.astRewriters.HasLabelsAndHasTypeNormalizer
import org.neo4j.cypher.internal.rewriting.rewriters.combineHasLabels
import org.neo4j.cypher.internal.rewriting.rewriters.preparatoryRewriters.RemoveSyntaxTracking
import org.neo4j.cypher.internal.util.FunctionName
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.InputPosition.NONE
import org.neo4j.cypher.internal.util.LabelId
import org.neo4j.cypher.internal.util.ProcedureName
import org.neo4j.cypher.internal.util.PropertyKeyId
import org.neo4j.cypher.internal.util.RelTypeId
import org.neo4j.cypher.internal.util.Repetition
import org.neo4j.cypher.internal.util.Rewritable.RewritableAny
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.UpperBound
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.attribution.IdGen
import org.neo4j.cypher.internal.util.attribution.SameId
import org.neo4j.cypher.internal.util.attribution.SequentialIdGen
import org.neo4j.cypher.internal.util.collection.immutable.ListSet
import org.neo4j.cypher.internal.util.inSequence
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTList
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTRelationship
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.cypher.internal.util.symbols.TypeSpec
import org.neo4j.cypher.internal.util.topDown
import org.neo4j.graphdb.schema.IndexType

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration.SECONDS
import scala.language.implicitConversions

/**
 * Used by [[AbstractLogicalPlanBuilder]] to resolve tokens and procedures
 */
trait Resolver {

  /**
   * Obtain the token of a label by name.
   */
  def getLabelId(label: String): Int

  def getRelTypeId(label: String): Int

  def getPropertyKeyId(prop: String): Int

  def procedureSignature(name: ProcedureName): ProcedureSignature

  def functionSignature(name: FunctionName): Option[UserFunctionSignature]
}

/**
 * Test help utility for hand-writing objects needing logical plans.
 * @param wholePlan Validate that we are creating a whole plan
 */
abstract class AbstractLogicalPlanBuilder[T, IMPL <: AbstractLogicalPlanBuilder[T, IMPL]](
  protected val resolver: Resolver,
  wholePlan: Boolean = true,
  initialId: Int = 0,
  language: CypherVersion = CypherVersion.Legacy.legacyVersion()
) {

  self: IMPL =>

  val patternParser = new PatternParser
  val parser = Parser(language)
  protected var semanticTable = new SemanticTable()

  sealed protected trait OperatorBuilder

  protected class LeafOperator(planToIdConstructor: IdGen => LogicalPlan) extends OperatorBuilder {
    private val id = idGen.id()
    _idOfLastPlan = id

    def planConstructor(): LogicalPlan = planToIdConstructor(SameId(id))
  }

  protected class UnaryOperator(planToIdConstructor: LogicalPlan => IdGen => LogicalPlan) extends OperatorBuilder {
    private val id = idGen.id()
    _idOfLastPlan = id

    def planConstructor: LogicalPlan => LogicalPlan = planToIdConstructor(_)(SameId(id))
  }

  protected class BinaryOperator(planToIdConstructor: (LogicalPlan, LogicalPlan) => IdGen => LogicalPlan)
      extends OperatorBuilder {
    private val id = idGen.id()
    _idOfLastPlan = id

    def planConstructor: (LogicalPlan, LogicalPlan) => LogicalPlan = planToIdConstructor(_, _)(SameId(id))
  }

  protected class Tree(operator: OperatorBuilder) {
    private var _left: Option[Tree] = None
    private var _right: Option[Tree] = None

    def left: Option[Tree] = _left

    def left_=(newVal: Option[Tree]): Unit = {
      operator match {
        case _: LeafOperator =>
          throw new IllegalArgumentException(s"Cannot attach a LHS to a leaf plan.")
        case _ =>
      }
      _left = newVal
    }

    def right: Option[Tree] = _right

    def right_=(newVal: Option[Tree]): Unit = {
      operator match {
        case _: LeafOperator =>
          throw new IllegalArgumentException(s"Cannot attach a RHS to a leaf plan.")
        case _: UnaryOperator =>
          throw new IllegalArgumentException(s"Cannot attach a RHS to a unary plan.")
        case _ =>
      }
      _right = newVal
    }

    def build(): LogicalPlan = {
      operator match {
        case o: LeafOperator =>
          o.planConstructor()
        case o: UnaryOperator =>
          o.planConstructor(left.get.build())
        case o: BinaryOperator =>
          (left, right) match {
            case (Some(leftVal), Some(rightVal)) => o.planConstructor(leftVal.build(), rightVal.build())
            case (None, _) =>
              val fakePlan = o.planConstructor(Argument()(idGen), Argument()(idGen)).productPrefix
              throw new IllegalStateException(s"Tried building plan '$fakePlan' but left operator is missing.")
            case (_, None) =>
              val fakePlan = o.planConstructor(Argument()(idGen), Argument()(idGen)).productPrefix
              throw new IllegalStateException(s"Tried building plan '$fakePlan' but right operator is missing.")
          }
      }
    }
  }

  val idGen: IdGen = new SequentialIdGen(initialId)

  private var tree: Tree = _
  private val looseEnds = new ArrayBuffer[Tree]
  private var indent = 0
  protected var resultColumns: Array[String] = _
  protected var enabled = true

  private var _idOfLastPlan = Id.INVALID_ID

  protected def idOfLastPlan: Id = _idOfLastPlan

  /**
   * Increase indent. The indent determines where the next
   * logical plan will be appended to the tree.
   */
  def | : IMPL = {
    indent += 1
    self
  }

  def resetIndent(): IMPL = {
    indent = 0
    self
  }

  def planIf(condition: Boolean)(builder: IMPL => IMPL): IMPL = {
    if (condition) {
      builder(self)
    } else {
      self
    }
  }

  def planAny(builder: IMPL => IMPL): IMPL = {
    builder(self)
  }

  // OPERATORS

  def produceResults(inputs: AnyRef*): IMPL = {
    if (inputs.forall(_.isInstanceOf[String])) {
      val vars = inputs.map(_.asInstanceOf[String])
      val resultColumnsSeq = vars.map(VariableParser.unescaped)
      resultColumns = resultColumnsSeq.toArray
      tree =
        new Tree(UnaryOperator(lp => ProduceResult(lp, resultColumnsSeq.map(c => Column(varFor(c), Set.empty)))(_)))
      looseEnds += tree
      self
    } else if (inputs.forall(_.isInstanceOf[Column])) {
      val cols = inputs.map(_.asInstanceOf[Column])
      resultColumns = cols.map(_.variable.name).toArray
      tree = new Tree(UnaryOperator(lp => ProduceResult(lp, cols)(_)))
      looseEnds += tree
      self
    } else {
      throw new IllegalArgumentException(
        s"${inputs.mkString(", ")} must either all be of type String or all of type Column."
      )
    }
  }

  def procedureCall(
    call: String,
    withFakedFullDeclarations: Boolean = false,
    optionalState: OptionalState = NonOptional
  ): IMPL = {
    val unresolvedCall = parser.parseProcedureCall(call)
    appendAtCurrentIndent(UnaryOperator(lp => {
      val resolvedCall =
        ResolvedNonLocalCall(resolver.procedureSignature)(unresolvedCall)
          .coerceArguments
          .copy(optionalState = optionalState)(unresolvedCall.position)
      val rewrittenResolvedCall =
        if (withFakedFullDeclarations) resolvedCall.withFakedFullDeclarations else resolvedCall
      ProcedureCall(lp, rewrittenResolvedCall)(_)
    }))
    self
  }

  def procedureCall(call: ResolvedNonLocalCall): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp => {
      ProcedureCall(lp, call)(_)
    }))
    self
  }

  def optional(protectedSymbols: String*): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp => Optional(lp, protectedSymbols.map(varFor).toSet)(_)))
    self
  }

  def anti(): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp => Anti(lp)(_)))
    self
  }

  def limit(count: ToExpression): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp => Limit(lp, toExpression(count))(_)))
    self
  }

  def exhaustiveLimit(count: ToExpression): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp => ExhaustiveLimit(lp, toExpression(count))(_)))
    self
  }

  def skip(count: ToExpression): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp => Skip(lp, toExpression(count))(_)))
    self
  }

  def argumentTracker(): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp => ArgumentTracker(lp)(_)))
    self
  }

  def expand(
    pattern: String,
    expandMode: ExpansionMode = ExpandAll,
    projectedDir: SemanticDirection = OUTGOING,
    nodePredicates: Seq[Predicate] = Seq.empty,
    relationshipPredicates: Seq[Predicate] = Seq.empty,
    pathMode: TraversalPathMode = TraversalPathMode.Trail
  ): IMPL = {
    expandExpr(
      pattern,
      expandMode,
      projectedDir,
      nodePredicates.map(_.asVariablePredicate),
      relationshipPredicates.map(_.asVariablePredicate),
      pathMode
    )
  }

  def expandExpr(
    pattern: String,
    expandMode: ExpansionMode = ExpandAll,
    projectedDir: SemanticDirection = OUTGOING,
    nodePredicates: Seq[VariablePredicate] = Seq.empty,
    relationshipPredicates: Seq[VariablePredicate] = Seq.empty,
    pathMode: TraversalPathMode = TraversalPathMode.Trail
  ): IMPL = {
    val p = patternParser.parse(pattern)
    newRelationship(varFor(p.maybeRelName))
    if (expandMode == ExpandAll) {
      newNode(varFor(p.maybeTo))
    }

    p.length match {
      case SimplePatternLength =>
        appendAtCurrentIndent(UnaryOperator(lp =>
          Expand(lp, varFor(p.from), p.dir, p.relTypes, varFor(p.maybeTo), varFor(p.maybeRelName), expandMode)(_)
        ))
      case varPatternLength: VarPatternLength =>
        appendAtCurrentIndent(UnaryOperator(lp =>
          VarExpand(
            lp,
            varFor(p.from),
            p.dir,
            projectedDir,
            p.relTypes,
            varFor(p.maybeTo),
            varFor(p.maybeRelName),
            varPatternLength,
            expandMode,
            nodePredicates,
            relationshipPredicates,
            pathMode
          )(_)
        ))
    }
    self
  }

  def simulatedExpand(fromNode: String, relName: String, toNode: String, factor: Double): IMPL = {
    val from = varFor(fromNode)
    val relVar = varFor(relName)
    val to = varFor(toNode)
    newNode(from)
    newRelationship(relVar)
    newNode(to)
    appendAtCurrentIndent(UnaryOperator(lp =>
      SimulatedExpand(lp, from, relVar, to, factor)(_)
    ))
    self
  }

  def shortestPath(
    pattern: String,
    pathName: Option[String] = None,
    all: Boolean = false,
    nodePredicates: Seq[Predicate] = Seq.empty,
    relationshipPredicates: Seq[Predicate] = Seq.empty,
    pathPredicates: Seq[String] = Seq.empty,
    withFallback: Boolean = false,
    sameNodeMode: SameNodeMode = DisallowSameNode,
    traversalPathMode: TraversalPathMode = Trail
  ): IMPL =
    shortestPathSolver(
      pattern,
      pathName,
      all,
      nodePredicates.map(_.asVariablePredicate),
      relationshipPredicates.map(_.asVariablePredicate),
      pathPredicates.map(parseExpression),
      withFallback,
      sameNodeMode,
      traversalPathMode
    )

  def shortestPathExpr(
    pattern: String,
    pathName: Option[String] = None,
    all: Boolean = false,
    nodePredicates: Seq[VariablePredicate] = Seq.empty,
    relationshipPredicates: Seq[VariablePredicate] = Seq.empty,
    pathPredicates: Seq[Expression] = Seq.empty,
    withFallback: Boolean = false,
    sameNodeMode: SameNodeMode = DisallowSameNode,
    traversalPathMode: TraversalPathMode = Trail
  ): IMPL =
    shortestPathSolver(
      pattern,
      pathName,
      all,
      nodePredicates,
      relationshipPredicates,
      pathPredicates,
      withFallback,
      sameNodeMode,
      traversalPathMode
    )

  def statefulShortestPathExpr(
    sourceNode: String,
    targetNode: String,
    solvedExpressionString: String,
    nonInlinedPreFilters: Option[Expression],
    groupNodes: Set[(String, String)],
    groupRelationships: Set[(String, String)],
    singletonNodeVariables: Set[(String, String)],
    singletonRelationshipVariables: Set[(String, String)],
    selector: StatefulShortestPath.Selector,
    nfa: NFA,
    mode: ExpansionMode,
    reverseGroupVariableProjections: Boolean = false,
    minLength: Int = 0,
    maxLength: Option[Int] = None,
    pathMode: TraversalPathMode = TraversalPathMode.Trail
  ): IMPL = {
    val nodeVariableGroupings = groupNodes.map { case (x, y) => VariableGrouping(varFor(x), varFor(y))(pos) }
    val relationshipVariableGroupings = groupRelationships.map { case (x, y) =>
      VariableGrouping(varFor(x), varFor(y))(pos)
    }
    val singletonNodeMappings = singletonNodeVariables.map { case (x, y) => Mapping(varFor(x), varFor(y)) }
    val singletonRelMappings = singletonRelationshipVariables.map { case (x, y) => Mapping(varFor(x), varFor(y)) }

    // Assign types to group variables
    nodeVariableGroupings.map(_.group.asInstanceOf[Variable])
      .foreach(newVariable(_, CTList(CTNode)))
    relationshipVariableGroupings.map(_.group.asInstanceOf[Variable])
      .foreach(newVariable(_, CTList(CTRelationship)))

    // Assign types to singleton variables.
    // Be aware that this will override the types from the group variables if they share the same name with
    // the singleton variables. This will for instance happen if you use .enableDeduplicateNames(true),
    // which is the default setting in any LogicalPlanning integration test.
    // Overriding the type of the group variable is usually fine, except for Eagerness analysis on logical plans.
    // For these tests, you might want to consider setting .enableDeduplicateNames(false).
    newNodes(nfa.nodes.map(_.name) + targetNode)
    newRelationships(nfa.relationships.map(_.name))
    singletonNodeMappings.map(_.rowVar.asInstanceOf[Variable])
      .foreach(newVariable(_, CTNode))
    singletonRelMappings.map(_.rowVar.asInstanceOf[Variable])
      .foreach(newVariable(_, CTRelationship))

    appendAtCurrentIndent(UnaryOperator(lp =>
      StatefulShortestPath(
        lp,
        varFor(sourceNode),
        varFor(targetNode),
        nfa.endoRewrite(expressionRewriter),
        mode,
        nonInlinedPreFilters.endoRewrite(expressionRewriter),
        nodeVariableGroupings,
        relationshipVariableGroupings,
        singletonNodeMappings,
        singletonRelMappings,
        selector,
        solvedExpressionString,
        reverseGroupVariableProjections,
        LengthBounds(minLength, maxLength),
        pathMode
      )(_)
    ))
    self
  }

  def statefulShortestPath(
    sourceNode: String,
    targetNode: String,
    solvedExpressionString: String,
    nonInlinedPreFilters: Option[String],
    groupNodes: Set[(String, String)],
    groupRelationships: Set[(String, String)],
    singletonNodeVariables: Set[(String, String)],
    singletonRelationshipVariables: Set[(String, String)],
    selector: StatefulShortestPath.Selector,
    nfa: NFA,
    mode: ExpansionMode,
    reverseGroupVariableProjections: Boolean = false,
    minLength: Int = 0,
    maxLength: Option[Int] = None,
    pathMode: TraversalPathMode = TraversalPathMode.Trail
  ): IMPL = {
    val predicates = nonInlinedPreFilters.map(parseExpression)
    statefulShortestPathExpr(
      sourceNode,
      targetNode,
      solvedExpressionString,
      predicates,
      groupNodes,
      groupRelationships,
      singletonNodeVariables,
      singletonRelationshipVariables,
      selector,
      nfa,
      mode,
      reverseGroupVariableProjections,
      minLength,
      maxLength,
      pathMode
    )
  }

  private def shortestPathSolver(
    pattern: String,
    pathName: Option[String],
    all: Boolean,
    nodePredicates: Seq[VariablePredicate],
    relationshipPredicates: Seq[VariablePredicate],
    pathPredicates: Seq[Expression],
    withFallback: Boolean,
    sameNodeMode: SameNodeMode,
    pathMode: TraversalPathMode
  ): IMPL = {
    val p = patternParser.parse(pattern)
    newRelationship(varFor(p.relName))

    val length = p.length match {
      case SimplePatternLength => None
      case VarPatternLength(min, max) => Some(Some(Range(
          Some(PathLengthQuantifier(min.toString)(pos)),
          max.map(i => PathLengthQuantifier(i.toString)(pos))
        )(pos)))
    }

    appendAtCurrentIndent(UnaryOperator(lp =>
      FindShortestPaths(
        lp,
        ShortestRelationshipPattern(
          pathName.map(varFor),
          PatternRelationship(varFor(p.relName), (varFor(p.from), varFor(p.to)), p.dir, p.relTypes, p.length),
          !all
        )(ShortestPathsPatternPart(
          RelationshipChain(
            NodePattern(Some(varFor(p.from)), None, None, None)(
              pos
            ), // labels, properties and predicates are not used at runtime
            RelationshipPattern(
              Some(varFor(p.relName)),
              disjoinRelTypesToLabelExpression(p.relTypes),
              length,
              None, // properties are not used at runtime
              None,
              p.dir
            )(pos),
            NodePattern(Some(varFor(p.to)), None, None, None)(
              pos
            ) // labels, properties and predicates are not used at runtime
          )(pos),
          !all
        )(pos)),
        nodePredicates,
        relationshipPredicates,
        pathPredicates,
        withFallback,
        sameNodeMode,
        pathMode
      )(_)
    ))
  }

  def pruningVarExpand(
    pattern: String,
    nodePredicates: Seq[Predicate] = Seq.empty,
    relationshipPredicates: Seq[Predicate] = Seq.empty,
    pathMode: TraversalPathMode = TraversalPathMode.Trail
  ): IMPL = {
    val p = patternParser.parse(pattern)
    newRelationship(varFor(p.relName))
    newNode(varFor(p.to))
    p.length match {
      case VarPatternLength(min, Some(max)) =>
        appendAtCurrentIndent(UnaryOperator(lp =>
          PruningVarExpand(
            lp,
            varFor(p.from),
            p.dir,
            p.relTypes,
            varFor(p.maybeTo),
            min,
            max,
            nodePredicates.map(_.asVariablePredicate),
            relationshipPredicates.map(_.asVariablePredicate),
            pathMode
          )(_)
        ))
      case _ =>
        throw new IllegalArgumentException("This pattern is not compatible with pruning var expand")
    }
    self
  }

  def bfsPruningVarExpand(
    pattern: String,
    depthName: Option[String] = None,
    nodePredicates: Seq[Predicate] = Seq.empty,
    relationshipPredicates: Seq[Predicate] = Seq.empty,
    mode: ExpansionMode = ExpandAll,
    pathMode: TraversalPathMode = TraversalPathMode.Trail
  ): IMPL = {
    bfsPruningVarExpandExpr(
      pattern,
      depthName,
      nodePredicates.map(_.asVariablePredicate),
      relationshipPredicates.map(_.asVariablePredicate),
      mode,
      pathMode
    )
  }

  def bfsPruningVarExpandExpr(
    pattern: String,
    depthName: Option[String] = None,
    nodePredicates: Seq[VariablePredicate] = Seq.empty,
    relationshipPredicates: Seq[VariablePredicate] = Seq.empty,
    mode: ExpansionMode = ExpandAll,
    pathMode: TraversalPathMode = TraversalPathMode.Trail
  ): IMPL = {
    val p = patternParser.parse(pattern)
    newRelationship(varFor(p.relName))
    if (mode == ExpandAll) {
      newNode(varFor(p.maybeTo))
    }
    p.length match {
      case VarPatternLength(min, maybeMax) if min <= 1 =>
        appendAtCurrentIndent(UnaryOperator(lp =>
          BFSPruningVarExpand(
            lp,
            varFor(p.from),
            p.dir,
            p.relTypes,
            varFor(p.maybeTo),
            min == 0,
            maxLength = maybeMax.getOrElse(Int.MaxValue),
            depthName.map(varFor),
            mode,
            nodePredicates,
            relationshipPredicates,
            pathMode
          )(_)
        ))
      case _ =>
        throw new IllegalArgumentException("This pattern is not compatible with a bfs pruning var expand")
    }
    self
  }

  def pathPropagatingBFS(
    pattern: String,
    projectedDir: SemanticDirection = OUTGOING,
    nodePredicates: Seq[Predicate] = Seq.empty,
    relationshipPredicates: Seq[Predicate] = Seq.empty
  ): IMPL = {
    val p = patternParser.parse(pattern)
    val varPatternLength = p.length.asInstanceOf[VarPatternLength]

    newRelationship(varFor(p.relName))
    newNode(varFor(p.to))

    appendAtCurrentIndent(BinaryOperator((lhs, rhs) =>
      PathPropagatingBFS(
        lhs,
        rhs,
        varFor(p.from),
        p.dir,
        projectedDir,
        p.relTypes,
        varFor(p.to),
        varFor(p.relName),
        varPatternLength,
        nodePredicates.map(_.asVariablePredicate),
        relationshipPredicates.map(_.asVariablePredicate)
      )(_)
    ))
  }

  def expandInto(pattern: String): IMPL = expand(pattern, ExpandInto)

  def optionalExpandAll(pattern: String, predicate: Option[String] = None): IMPL =
    optionalExpandAll(
      patternParser.parse(pattern),
      predicate
        .map(parseExpression)
        .map {
          case ands: Ands => ands
          case p          => Ands(ListSet(p))(p.position)
        }
    )

  private def optionalExpandAll(pattern: PatternParser.Pattern, predicate: Option[Expression]): IMPL = {
    val nodes = Set(pattern.from, pattern.to)
    val rewrittenPredicate = predicate.map(_.endoRewrite(topDown(Rewriter.lift {
      case p @ HasLabelsOrTypes(v: Variable, labelsOrTypes) if nodes.contains(v.name) =>
        HasLabels(v, labelsOrTypes.map(l => LabelName(l.name)(l.position)))(p.position)
      case p @ HasLabelsOrTypes(v: Variable, labelsOrTypes) if pattern.relName == v.name =>
        HasTypes(v, labelsOrTypes.map(l => RelTypeName(l.name)(l.position)))(p.position)
    })))
    pattern.length match {
      case SimplePatternLength =>
        appendAtCurrentIndent(UnaryOperator(lp =>
          OptionalExpand(
            lp,
            varFor(pattern.from),
            pattern.dir,
            pattern.relTypes,
            varFor(pattern.maybeTo),
            varFor(pattern.maybeRelName),
            ExpandAll,
            rewrittenPredicate
          )(_)
        ))
      case _ =>
        throw new IllegalArgumentException("Cannot have optional expand with variable length pattern")
    }
    self
  }

  def optionalExpandInto(pattern: String, predicate: Option[String] = None): IMPL = {
    val p = patternParser.parse(pattern)
    p.length match {
      case SimplePatternLength =>
        val pred = predicate.map(parseExpression).map(p => Ands(ListSet(p))(p.position))
        appendAtCurrentIndent(UnaryOperator(lp =>
          OptionalExpand(
            lp,
            varFor(p.from),
            p.dir,
            p.relTypes,
            varFor(p.maybeTo),
            varFor(p.maybeRelName),
            ExpandInto,
            pred
          )(_)
        ))
      case _ =>
        throw new IllegalArgumentException("Cannot have optional expand with variable length pattern")
    }
    self
  }

  def projectEndpoints(pattern: String, startInScope: Boolean, endInScope: Boolean): IMPL = {
    val p = patternParser.parse(pattern)
    newNode(varFor(p.from))
    newNode(varFor(p.to))
    appendAtCurrentIndent(UnaryOperator(lp =>
      ProjectEndpoints(
        lp,
        varFor(p.relName),
        varFor(p.from),
        startInScope,
        varFor(p.to),
        endInScope,
        p.relTypes,
        p.dir,
        p.length
      )(_)
    ))
  }

  def partialSort(alreadySortedPrefix: Seq[String], stillToSortSuffix: Seq[String]): IMPL =
    partialSortColumns(parser.parseSort(alreadySortedPrefix), parser.parseSort(stillToSortSuffix))

  def partialSortColumns(alreadySortedPrefix: Seq[ColumnOrder], stillToSortSuffix: Seq[ColumnOrder]): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp => PartialSort(lp, alreadySortedPrefix, stillToSortSuffix, None)(_)))
    self
  }

  def partialSortColumns(
    alreadySortedPrefix: Seq[ColumnOrder],
    stillToSortSuffix: Seq[ColumnOrder],
    skipSortingPrefixLength: Long
  ): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp =>
      PartialSort(lp, alreadySortedPrefix, stillToSortSuffix, Some(literalInt(skipSortingPrefixLength)))(_)
    ))
    self
  }

  def partialSort(
    alreadySortedPrefix: Seq[String],
    stillToSortSuffix: Seq[String],
    skipSortingPrefixLength: Long = 0
  ): IMPL = {
    val skipSort = if (skipSortingPrefixLength == 0) None else Some(literalInt(skipSortingPrefixLength))
    appendAtCurrentIndent(UnaryOperator(lp =>
      PartialSort(lp, parser.parseSort(alreadySortedPrefix), parser.parseSort(stillToSortSuffix), skipSort)(_)
    ))
    self
  }

  def sortColumns(sortItems: Seq[ColumnOrder]): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp => Sort(lp, sortItems)(_)))
    self
  }

  def sort(sortItems: String*): IMPL = sortColumns(parser.parseSort(sortItems))

  def top(limit: Long, sortItems: String*): IMPL = top(parser.parseSort(sortItems), limit)

  def top(sortItems: Seq[ColumnOrder], limitExpr: ToExpression): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp => Top(lp, sortItems, toExpression(limitExpr))(_)))
    self
  }

  def top1WithTies(sortItems: String*): IMPL = top1WithTiesColumns(parser.parseSort(sortItems))

  def top1WithTiesColumns(sortItems: Seq[ColumnOrder]): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp => Top1WithTies(lp, sortItems)(_)))
    self
  }

  def partialTop(alreadySortedPrefix: Seq[ColumnOrder], stillToSortSuffix: Seq[ColumnOrder], limit: Long): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp =>
      PartialTop(lp, alreadySortedPrefix, stillToSortSuffix, literalInt(limit), None)(_)
    ))
    self
  }

  def partialTop(limit: Long, alreadySortedPrefix: Seq[String], stillToSortSuffix: Seq[String]): IMPL = {
    partialTop(parser.parseSort(alreadySortedPrefix), parser.parseSort(stillToSortSuffix), limit)
  }

  def partialTop(
    alreadySortedPrefix: Seq[ColumnOrder],
    stillToSortSuffix: Seq[ColumnOrder],
    limit: Long,
    skipSortingPrefixLength: Long
  ): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp =>
      PartialTop(
        lp,
        alreadySortedPrefix,
        stillToSortSuffix,
        literalInt(limit),
        Some(literalInt(skipSortingPrefixLength))
      )(_)
    ))
    self
  }

  def partialTop(
    alreadySortedPrefix: Seq[ColumnOrder],
    stillToSortSuffix: Seq[ColumnOrder],
    limitExpr: ToExpression,
    skipExpr: Option[ToExpression] = None
  ): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp =>
      PartialTop(
        lp,
        alreadySortedPrefix,
        stillToSortSuffix,
        toExpression(limitExpr),
        skipExpr.map(toExpression(_))
      )(_)
    ))
  }

  def partialTop(
    limit: Long,
    skipSortingPrefixLength: Long,
    alreadySortedPrefix: Seq[String],
    stillToSortSuffix: Seq[String]
  ): IMPL = {
    partialTop(
      parser.parseSort(alreadySortedPrefix),
      parser.parseSort(stillToSortSuffix),
      limit,
      Some(skipSortingPrefixLength)
    )
  }

  def eager(reasons: ListSet[EagernessReason] = ListSet(EagernessReason.Unknown)): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp => Eager(lp, reasons)(_)))
    self
  }

  def emptyResult(): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp => EmptyResult(lp)(_)))
    self
  }

  def deleteNode(node: ToExpression): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp => DeleteNode(lp, toExpression(node))(_)))
    self
  }

  def detachDeleteNode(node: ToExpression): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp => DetachDeleteNode(lp, toExpression(node))(_)))
    self
  }

  def deleteRelationship(rel: ToExpression): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp => DeleteRelationship(lp, toExpression(rel))(_)))
    self
  }

  def deletePath(path: ToExpression): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp => DeletePath(lp, toExpression(path))(_)))
    self
  }

  def detachDeletePath(path: ToExpression): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp => DetachDeletePath(lp, toExpression(path))(_)))
    self
  }

  def deleteExpression(expression: ToExpression): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp => DeleteExpression(lp, toExpression(expression))(_)))
    self
  }

  def detachDeleteExpression(expression: ToExpression): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp => DetachDeleteExpression(lp, toExpression(expression))(_)))
    self
  }

  def setLabels(nodeVariable: String, labels: String*): IMPL = {
    val labelNames = labels.map(l => LabelName(l)(InputPosition.NONE)).toSet
    appendAtCurrentIndent(UnaryOperator(lp => SetLabels(lp, varFor(nodeVariable), labelNames, Set.empty)(_)))
  }

  def setLabels(nodeVariable: String, labelNames: Seq[String], labelExpressions: Seq[String]): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp =>
      SetLabels(
        lp,
        varFor(nodeVariable),
        labelNames.map(l => LabelName(l)(InputPosition.NONE)).toSet,
        labelExpressions.map(l => parser.parseExpression(l)).toSet
      )(_)
    ))
  }

  def setDynamicLabels(nodeVariable: String, labels: ToExpression*): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp =>
      SetLabels(lp, varFor(nodeVariable), Set.empty, labels.map(toExpression(_)).toSet)(_)
    ))
  }

  def removeLabels(nodeVariable: String, labels: String*): IMPL = {
    val labelNames = labels.map(l => LabelName(l)(InputPosition.NONE)).toSet
    appendAtCurrentIndent(UnaryOperator(lp => RemoveLabels(lp, varFor(nodeVariable), labelNames, Set.empty)(_)))
  }

  def removeLabels(nodeVariable: String, labelNames: Seq[String], labelExpressions: Seq[String]): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp =>
      RemoveLabels(
        lp,
        varFor(nodeVariable),
        labelNames.map(l => LabelName(l)(InputPosition.NONE)).toSet,
        labelExpressions.map(l => parser.parseExpression(l)).toSet
      )(_)
    ))
  }

  def removeDynamicLabels(nodeVariable: String, labels: String*): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp =>
      RemoveLabels(lp, varFor(nodeVariable), Set.empty, labels.map(l => parser.parseExpression(l)).toSet)(_)
    ))
  }

  def removeDynamicLabelsWithExpressions(nodeVariable: String, labelsExpressions: Set[Expression]): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp =>
      RemoveLabels(lp, varFor(nodeVariable), Set.empty, labelsExpressions)(_)
    ))
  }

  def unwind(projectionString: String): IMPL = {
    val (name, expression) = toVarMap(parser.parseProjections(projectionString)).head
    val maybeVariable = name.name match {
      case "_" => None
      case _   => Some(name)
    }
    appendAtCurrentIndent(UnaryOperator(lp => UnwindCollection(lp, maybeVariable, expression)(_)))
    self
  }

  def partitionedUnwind(projectionString: String): IMPL = {
    val (name, expression) = toVarMap(parser.parseProjections(projectionString)).head
    val maybeVariable = name.name match {
      case "_" => None
      case _   => Some(name)
    }
    appendAtCurrentIndent(UnaryOperator(lp => PartitionedUnwindCollection(lp, maybeVariable, expression)(_)))
    self
  }

  def runQueryAt(
    query: String,
    graphReference: String,
    parameters: Set[String] = Set.empty,
    importsAsParameters: Map[String, String] = Map.empty,
    columns: Set[String] = Set.empty
  ): IMPL = {
    val properParameters = parameters.map(asParameter)
    val properImports = importsAsParameters.map[Parameter, LogicalVariable] {
      case (parameterName, variableName) => asParameter(parameterName) -> varFor(variableName)
    }
    appendAtCurrentIndent(UnaryOperator(source =>
      RunQueryAt(
        source,
        query,
        parser.parseGraphReference(graphReference),
        properParameters,
        properImports,
        columns.map(varFor)
      )(_)
    ))
  }

  private def asParameter(parameterName: String): Parameter =
    ExplicitParameter(parameterName.stripPrefix("$"), CTAny)(pos)

  def projection(projectionStrings: String*): IMPL = {
    doProjection(parseProjections(projectionStrings: _*))
  }

  def projection(projectExpressions: Map[String, Expression]): IMPL = {
    doProjection(toVarMap(projectExpressions))
  }

  private def doProjection(projectExpressions: Map[LogicalVariable, Expression]): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp => {
      val rewrittenProjections = projectExpressions.map { case (name, expr) =>
        name -> expr.endoRewrite(expressionRewriter)
      }
      projectExpressions.foreach { case (name, expr) => newAlias(name, expr) }
      Projection(lp, rewrittenProjections)(_)
    }))
    self
  }

  private def toVarMap(map: Map[String, Expression]): Map[LogicalVariable, Expression] = map.map {
    case (key, value) => varFor(key) -> value
  }

  def distinct(projectionStrings: String*): IMPL = {
    val projections = parser.parseProjections(projectionStrings: _*)
    appendAtCurrentIndent(UnaryOperator(lp => Distinct(lp, toVarMap(projections))(_)))
    self
  }

  def orderedDistinct(orderToLeverage: Seq[String], projectionStrings: String*): IMPL = {
    val order = orderToLeverage.map(parseExpression)
    val projections = parser.parseProjections(projectionStrings: _*)
    appendAtCurrentIndent(UnaryOperator(lp => OrderedDistinct(lp, toVarMap(projections), order)(_)))
    self
  }

  def allNodeScan(node: String, args: String*): IMPL = {
    val n = VariableParser.unescaped(node)
    newNode(varFor(n))
    appendAtCurrentIndent(LeafOperator(AllNodesScan(
      varFor(n),
      args.map(a => VariableParser.unescapedVar(a)).toSet
    )(_)))
  }

  def partitionedAllNodeScan(node: String, args: String*): IMPL = {
    val n = VariableParser.unescaped(node)
    newNode(varFor(n))
    appendAtCurrentIndent(LeafOperator(PartitionedAllNodesScan(
      varFor(n),
      args.map(a => VariableParser.unescapedVar(a)).toSet
    )(_)))
  }

  def simulatedNodeScan(node: String, numberOfRows: Long): IMPL = {
    val n = VariableParser.unescaped(node)
    newNode(varFor(n))
    appendAtCurrentIndent(LeafOperator(SimulatedNodeScan(varFor(n), numberOfRows)(_)))
  }

  def argument(args: String*): IMPL = {
    appendAtCurrentIndent(LeafOperator(Argument(args.map(varFor).toSet)(_)))
  }

  def nodeByLabelScan(node: String, label: String, args: String*): IMPL =
    nodeByLabelScan(node, label, IndexOrderNone, args: _*)

  def nodeByLabelScan(node: String, label: String, indexOrder: IndexOrder, args: String*): IMPL = {
    val n = VariableParser.unescaped(node)
    newNode(varFor(n))
    appendAtCurrentIndent(LeafOperator(NodeByLabelScan(
      varFor(n),
      labelName(label),
      args.map(a => VariableParser.unescapedVar(a)).toSet,
      indexOrder
    )(_)))
  }

  def dynamicLabelNodeLookup(
    node: String,
    labelExpr: String,
    operator: SetOperator,
    args: String*
  ): IMPL = {
    dynamicLabelNodeLookup(node, parseExpression(labelExpr), operator, args: _*)
  }

  def dynamicLabelNodeLookup(
    node: String,
    labelExpr: String,
    operator: SetOperator,
    propConstraints: Map[String, String],
    args: String*
  ): IMPL = {
    val props = propConstraints.view.mapValues(parseExpression).toMap
    dynamicLabelNodeLookup(node, parseExpression(labelExpr), operator, props, args: _*)
  }

  def dynamicLabelNodeLookup(
    node: String,
    labelExpr: Expression,
    operator: SetOperator,
    args: String*
  ): IMPL = {
    val n = VariableParser.unescaped(node)
    newNode(varFor(n))
    appendAtCurrentIndent(LeafOperator(DynamicLabelNodeLookup(
      varFor(n),
      DynamicElement.Simple(labelExpr, operator),
      args.map(a => VariableParser.unescapedVar(a)).toSet,
      Map.empty
    )(_)))
  }

  def dynamicLabelNodeLookup(
    node: String,
    labelExpr: Expression,
    operator: SetOperator,
    propConstraints: Map[String, Expression],
    args: String*
  ): IMPL = {
    val n = VariableParser.unescaped(node)
    newNode(varFor(n))
    appendAtCurrentIndent(LeafOperator(DynamicLabelNodeLookup(
      varFor(n),
      DynamicElement.Simple(labelExpr, operator),
      args.map(a => varFor(VariableParser.unescaped(a))).toSet,
      resolvePropertyKeyIdsForDynamicIndexUse(propConstraints)
    )(_)))
  }

  def partitionedNodeByLabelScan(node: String, label: String, args: String*): IMPL = {
    val n = VariableParser.unescaped(node)
    newNode(varFor(n))
    appendAtCurrentIndent(LeafOperator(PartitionedNodeByLabelScan(
      varFor(n),
      labelName(label),
      args.map(a => VariableParser.unescapedVar(a)).toSet
    )(_)))
  }

  def unionNodeByLabelsScan(node: String, labels: Seq[String], args: String*): IMPL = {
    unionNodeByLabelsScan(node, labels, IndexOrderNone, args: _*)
  }

  def unionNodeByLabelsScan(node: String, labels: Seq[String], indexOrder: IndexOrder, args: String*): IMPL = {
    val n = VariableParser.unescaped(node)
    newNode(varFor(n))
    appendAtCurrentIndent(LeafOperator(UnionNodeByLabelsScan(
      varFor(n),
      labels.map(labelName),
      args.map(a => VariableParser.unescapedVar(a)).toSet,
      indexOrder
    )(_)))
  }

  def partitionedUnionNodeByLabelsScan(node: String, labels: Seq[String], args: String*): IMPL = {
    val n = VariableParser.unescaped(node)
    newNode(varFor(n))
    appendAtCurrentIndent(LeafOperator(PartitionedUnionNodeByLabelsScan(
      varFor(n),
      labels.map(labelName),
      args.map(a => VariableParser.unescapedVar(a)).toSet
    )(_)))
  }

  def intersectionNodeByLabelsScan(node: String, labels: Seq[String], args: String*): IMPL = {
    intersectionNodeByLabelsScan(node, labels, IndexOrderNone, args: _*)
  }

  def intersectionNodeByLabelsScan(node: String, labels: Seq[String], indexOrder: IndexOrder, args: String*): IMPL = {
    val n = VariableParser.unescaped(node)
    newNode(varFor(n))
    appendAtCurrentIndent(LeafOperator(IntersectionNodeByLabelsScan(
      varFor(n),
      labels.map(labelName),
      args.map(a => VariableParser.unescapedVar(a)).toSet,
      indexOrder
    )(_)))
  }

  def partitionedIntersectionNodeByLabelsScan(node: String, labels: Seq[String], args: String*): IMPL = {
    val n = VariableParser.unescaped(node)
    newNode(varFor(n))
    appendAtCurrentIndent(LeafOperator(PartitionedIntersectionNodeByLabelsScan(
      varFor(n),
      labels.map(labelName),
      args.map(a => VariableParser.unescapedVar(a)).toSet
    )(_)))
  }

  def subtractionNodeByLabelsScan(
    node: String,
    positiveLabel: String,
    negativeLabel: String,
    args: String*
  ): IMPL = {
    subtractionNodeByLabelsScan(node, Seq(positiveLabel), Seq(negativeLabel), args: _*)
  }

  def subtractionNodeByLabelsScan(
    node: String,
    positiveLabel: String,
    negativeLabel: String,
    indexOrder: IndexOrder,
    args: String*
  ): IMPL = {
    subtractionNodeByLabelsScan(node, Seq(positiveLabel), Seq(negativeLabel), indexOrder, args: _*)
  }

  def subtractionNodeByLabelsScan(
    node: String,
    positiveLabels: Seq[String],
    negativeLabels: Seq[String],
    args: String*
  ): IMPL = {
    subtractionNodeByLabelsScan(node, positiveLabels, negativeLabels, IndexOrderNone, args: _*)
  }

  def subtractionNodeByLabelsScan(
    node: String,
    positiveLabels: Seq[String],
    negativeLabels: Seq[String],
    indexOrder: IndexOrder,
    args: String*
  ): IMPL = {
    val n = VariableParser.unescaped(node)
    newNode(varFor(n))
    appendAtCurrentIndent(LeafOperator(SubtractionNodeByLabelsScan(
      varFor(n),
      positiveLabels.map(labelName),
      negativeLabels.map(labelName),
      args.map(a => VariableParser.unescapedVar(a)).toSet,
      indexOrder
    )(_)))
  }

  def partitionedSubtractionNodeByLabelsScan(
    node: String,
    positiveLabels: Seq[String],
    negativeLabels: Seq[String],
    args: String*
  ): IMPL = {
    val n = VariableParser.unescaped(node)
    newNode(varFor(n))
    appendAtCurrentIndent(LeafOperator(PartitionedSubtractionNodeByLabelsScan(
      varFor(n),
      positiveLabels.map(labelName),
      negativeLabels.map(labelName),
      args.map(a => VariableParser.unescapedVar(a)).toSet
    )(_)))
  }

  def unionRelationshipTypesScan(pattern: String, args: String*): IMPL = {
    unionRelationshipTypesScan(pattern, IndexOrderNone, args: _*)
  }

  def unionRelationshipTypesScan(pattern: String, indexOrder: IndexOrder, args: String*): IMPL = {
    val p = patternParser.parse(pattern)
    newRelationship(varFor(p.relName))
    newNode(varFor(p.maybeFrom))
    newNode(varFor(p.maybeTo))
    if (!p.length.isSimple) throw new UnsupportedOperationException("Cannot do a scan from a variable pattern")

    p.dir match {
      case SemanticDirection.OUTGOING =>
        appendAtCurrentIndent(LeafOperator(DirectedUnionRelationshipTypesScan(
          varFor(p.maybeRelName),
          varFor(p.maybeFrom),
          p.relTypes,
          varFor(p.maybeTo),
          args.map(varFor).toSet,
          indexOrder
        )(_)))
      case SemanticDirection.INCOMING =>
        appendAtCurrentIndent(LeafOperator(DirectedUnionRelationshipTypesScan(
          varFor(p.maybeRelName),
          varFor(p.maybeTo),
          p.relTypes,
          varFor(p.maybeFrom),
          args.map(varFor).toSet,
          indexOrder
        )(_)))
      case SemanticDirection.BOTH =>
        appendAtCurrentIndent(LeafOperator(UndirectedUnionRelationshipTypesScan(
          varFor(p.maybeRelName),
          varFor(p.maybeFrom),
          p.relTypes,
          varFor(p.maybeTo),
          args.map(varFor).toSet,
          indexOrder
        )(_)))
    }
  }

  def partitionedUnionRelationshipTypesScan(pattern: String, args: String*): IMPL = {
    val p = patternParser.parse(pattern)
    newRelationship(varFor(p.relName))
    newNode(varFor(p.maybeFrom))
    newNode(varFor(p.maybeTo))
    if (!p.length.isSimple) throw new UnsupportedOperationException("Cannot do a scan from a variable pattern")

    p.dir match {
      case SemanticDirection.OUTGOING =>
        appendAtCurrentIndent(LeafOperator(PartitionedDirectedUnionRelationshipTypesScan(
          varFor(p.maybeRelName),
          varFor(p.maybeFrom),
          p.relTypes,
          varFor(p.maybeTo),
          args.map(varFor).toSet
        )(_)))
      case SemanticDirection.INCOMING =>
        appendAtCurrentIndent(LeafOperator(PartitionedDirectedUnionRelationshipTypesScan(
          varFor(p.maybeRelName),
          varFor(p.maybeTo),
          p.relTypes,
          varFor(p.maybeFrom),
          args.map(varFor).toSet
        )(_)))
      case SemanticDirection.BOTH =>
        appendAtCurrentIndent(LeafOperator(PartitionedUndirectedUnionRelationshipTypesScan(
          varFor(p.maybeRelName),
          varFor(p.maybeFrom),
          p.relTypes,
          varFor(p.maybeTo),
          args.map(varFor).toSet
        )(_)))
    }
  }

  def nodeByIdSeek(node: String, args: Set[String], ids: Any*): IMPL = {
    val n = VariableParser.unescaped(node)
    newNode(varFor(n))

    val input = idSeekInput(ids)
    appendAtCurrentIndent(LeafOperator(NodeByIdSeek(varFor(n), input, args.map(varFor))(_)))
  }

  private def directedRelationshipByIdSeekSolver(
    relationship: Option[String],
    from: Option[String],
    to: Option[String],
    args: Set[String],
    expr: Seq[Expression]
  ): IMPL = {
    newRelationship(varFor(relationship))
    newNode(varFor(from))
    newNode(varFor(to))
    val input = ManySeekableArgs(ListLiteral(expr)(pos))

    appendAtCurrentIndent(LeafOperator(DirectedRelationshipByIdSeek(
      varFor(relationship),
      input,
      varFor(from),
      varFor(to),
      args.map(varFor)
    )(_)))
  }

  def relationshipByIdSeek(
    pattern: String,
    args: Set[String],
    ids: ToExpression*
  ): IMPL = {
    val idExpressions: Seq[Expression] = ids.map(toExpression(_))
    val p = patternParser.parse(pattern)
    if (!p.length.isSimple) throw new UnsupportedOperationException("Cannot do a scan from a variable pattern")

    p.dir match {
      case SemanticDirection.OUTGOING =>
        directedRelationshipByIdSeekSolver(p.maybeRelName, p.maybeFrom, p.maybeTo, args, idExpressions)
      case SemanticDirection.INCOMING =>
        directedRelationshipByIdSeekSolver(p.maybeRelName, p.maybeTo, p.maybeFrom, args, idExpressions)
      case SemanticDirection.BOTH =>
        undirectedRelationshipByIdSeekSolver(p.maybeRelName, p.maybeFrom, p.maybeTo, args, idExpressions)
    }
  }

  def directedRelationshipByIdSeekExpr(
    relationship: String,
    from: String,
    to: String,
    args: Set[String],
    expr: Expression*
  ): IMPL = {
    directedRelationshipByIdSeekExpr(Some(relationship), Some(from), Some(to), args, expr: _*)
  }

  def directedRelationshipByIdSeekExpr(
    relationship: Option[String],
    from: Option[String],
    to: Option[String],
    args: Set[String],
    expr: Expression*
  ): IMPL = {
    directedRelationshipByIdSeekSolver(relationship, from, to, args, expr)
  }

  private def undirectedRelationshipByIdSeekSolver(
    relationship: Option[String],
    from: Option[String],
    to: Option[String],
    args: Set[String],
    expr: Seq[Expression]
  ): IMPL = {
    newRelationship(varFor(relationship))
    newNode(varFor(from))
    newNode(varFor(to))
    val input = ManySeekableArgs(ListLiteral(expr)(pos))

    appendAtCurrentIndent(LeafOperator(UndirectedRelationshipByIdSeek(
      varFor(relationship),
      input,
      varFor(from),
      varFor(to),
      args.map(varFor)
    )(_)))
  }

  def undirectedRelationshipByIdSeekExpr(
    relationship: String,
    from: String,
    to: String,
    args: Set[String],
    expr: Expression*
  ): IMPL = {
    undirectedRelationshipByIdSeekExpr(Some(relationship), Some(from), Some(to), args, expr: _*)
  }

  def undirectedRelationshipByIdSeekExpr(
    relationship: Option[String],
    from: Option[String],
    to: Option[String],
    args: Set[String],
    expr: Expression*
  ): IMPL = {
    undirectedRelationshipByIdSeekSolver(relationship, from, to, args, expr)
  }

  def nodeByElementIdSeek(node: String, args: Set[String], ids: Any*): IMPL = {
    val n = VariableParser.unescaped(node)
    newNode(varFor(n))

    val input = idSeekInput(ids)
    appendAtCurrentIndent(LeafOperator(NodeByElementIdSeek(varFor(n), input, args.map(varFor))(_)))
  }

  def relationshipByElementIdSeek(pattern: String, args: Set[String], ids: Any*): IMPL = {
    val p = patternParser.parse(pattern)
    if (!p.length.isSimple) throw new UnsupportedOperationException("Cannot do a scan from a variable pattern")
    val (relationship, from, to) = (p.maybeRelName, p.maybeFrom, p.maybeTo)
    newRelationship(varFor(relationship))
    newNode(varFor(from))
    newNode(varFor(to))

    val input = idSeekInput(ids)
    p.dir match {
      case SemanticDirection.OUTGOING =>
        appendAtCurrentIndent(LeafOperator(DirectedRelationshipByElementIdSeek(
          varFor(relationship),
          input,
          varFor(from),
          varFor(to),
          args.map(varFor)
        )(_)))
      case SemanticDirection.INCOMING =>
        appendAtCurrentIndent(LeafOperator(DirectedRelationshipByElementIdSeek(
          varFor(relationship),
          input,
          varFor(to),
          varFor(from),
          args.map(varFor)
        )(_)))
      case SemanticDirection.BOTH =>
        appendAtCurrentIndent(LeafOperator(UndirectedRelationshipByElementIdSeek(
          varFor(relationship),
          input,
          varFor(from),
          varFor(to),
          args.map(varFor)
        )(_)))
    }
  }

  private def idSeekInput(ids: Seq[Any]): ManySeekableArgs = {
    ids match {
      case Seq(expression: Expression) =>
        ManySeekableArgs(expression)
      case _ =>
        val idExpressions = ids.map {
          case x: Expression => x
          // This is a bit hacky but we cannot separate passing in an expression string
          // like "$param" and an actual id string "thisisanelementid". If we don't do this hack
          // the caller would have to quote elementIds all the time.
          case x: String =>
            try {
              parser.parseExpression(x)
            } catch {
              case _: Exception => StringLiteral(x)(pos.withInputLength(0))
            }
          case x @ (_: Long | _: Int)     => SignedDecimalIntegerLiteral(x.toString)(pos.zeroLength)
          case x @ (_: Float | _: Double) => DecimalDoubleLiteral(x.toString)(pos.zeroLength)
          case x                          => throw new IllegalArgumentException(s"$x is not a supported value for ID")
        }
        ManySeekableArgs(ListLiteral(idExpressions)(pos))
    }
  }

  def allRelationshipsScan(pattern: String, args: String*): IMPL = {
    val p = patternParser.parse(pattern)
    newRelationship(varFor(p.relName))
    newNode(varFor(p.maybeFrom))
    newNode(varFor(p.maybeTo))
    if (!p.length.isSimple) throw new UnsupportedOperationException("Cannot do a scan from a variable pattern")

    p.dir match {
      case SemanticDirection.OUTGOING =>
        appendAtCurrentIndent(LeafOperator(DirectedAllRelationshipsScan(
          varFor(p.maybeRelName),
          varFor(p.maybeFrom),
          varFor(p.maybeTo),
          args.map(varFor).toSet
        )(_)))
      case SemanticDirection.INCOMING =>
        appendAtCurrentIndent(LeafOperator(DirectedAllRelationshipsScan(
          varFor(p.maybeRelName),
          varFor(p.maybeTo),
          varFor(p.maybeFrom),
          args.map(varFor).toSet
        )(_)))
      case SemanticDirection.BOTH =>
        appendAtCurrentIndent(LeafOperator(UndirectedAllRelationshipsScan(
          varFor(p.maybeRelName),
          varFor(p.maybeFrom),
          varFor(p.maybeTo),
          args.map(varFor).toSet
        )(_)))
    }
  }

  def partitionedAllRelationshipsScan(pattern: String, args: String*): IMPL = {
    val p = patternParser.parse(pattern)
    newRelationship(varFor(p.relName))
    newNode(varFor(p.maybeFrom))
    newNode(varFor(p.maybeTo))
    if (!p.length.isSimple) throw new UnsupportedOperationException("Cannot do a scan from a variable pattern")

    p.dir match {
      case SemanticDirection.OUTGOING =>
        appendAtCurrentIndent(LeafOperator(PartitionedDirectedAllRelationshipsScan(
          varFor(p.maybeRelName),
          varFor(p.maybeFrom),
          varFor(p.maybeTo),
          args.map(varFor).toSet
        )(_)))
      case SemanticDirection.INCOMING =>
        appendAtCurrentIndent(LeafOperator(PartitionedDirectedAllRelationshipsScan(
          varFor(p.maybeRelName),
          varFor(p.maybeTo),
          varFor(p.maybeFrom),
          args.map(varFor).toSet
        )(_)))
      case SemanticDirection.BOTH =>
        appendAtCurrentIndent(LeafOperator(PartitionedUndirectedAllRelationshipsScan(
          varFor(p.maybeRelName),
          varFor(p.maybeFrom),
          varFor(p.maybeTo),
          args.map(varFor).toSet
        )(_)))
    }
  }

  def relationshipTypeScan(pattern: String, args: String*): IMPL = {
    relationshipTypeScan(pattern, IndexOrderNone, args: _*)
  }

  def relationshipTypeScan(pattern: String, indexOrder: IndexOrder, args: String*): IMPL = {
    val p = patternParser.parse(pattern)
    newRelationship(varFor(p.relName))
    newNode(varFor(p.maybeFrom))
    newNode(varFor(p.maybeTo))
    if (!p.length.isSimple) throw new UnsupportedOperationException("Cannot do a scan from a variable pattern")
    val typ =
      if (p.relTypes.size == 1) p.relTypes.head
      else throw new UnsupportedOperationException("Cannot do a scan with multiple types")

    p.dir match {
      case SemanticDirection.OUTGOING =>
        appendAtCurrentIndent(LeafOperator(DirectedRelationshipTypeScan(
          varFor(p.maybeRelName),
          varFor(p.maybeFrom),
          typ,
          varFor(p.maybeTo),
          args.map(varFor).toSet,
          indexOrder
        )(_)))
      case SemanticDirection.INCOMING =>
        appendAtCurrentIndent(LeafOperator(DirectedRelationshipTypeScan(
          varFor(p.maybeRelName),
          varFor(p.maybeTo),
          typ,
          varFor(p.maybeFrom),
          args.map(varFor).toSet,
          indexOrder
        )(_)))
      case SemanticDirection.BOTH =>
        appendAtCurrentIndent(LeafOperator(UndirectedRelationshipTypeScan(
          varFor(p.maybeRelName),
          varFor(p.maybeFrom),
          typ,
          varFor(p.maybeTo),
          args.map(varFor).toSet,
          indexOrder
        )(_)))
    }
  }

  def dynamicRelationshipTypeLookup(
    pattern: String,
    relTypeExpr: String,
    indexOrder: IndexOrder = IndexOrderNone,
    propertyPredicates: Map[String, String] = Map.empty,
    argumentIds: Set[String] = Set.empty
  ): IMPL = {
    val p = patternParser.parse(pattern)
    val regex = "^(\\$|\\$any|\\$all)\\((.*)\\)$".r
    relTypeExpr match {
      case regex(operatorMatch: String, expression) =>
        val op = operatorMatch match {
          case "$" | "$all" =>
            DynamicElement.All
          case "$any" =>
            DynamicElement.Any
        }

        dynamicRelationshipTypeLookup(
          p.maybeFrom,
          p.maybeRelName,
          parseExpression(expression),
          p.maybeTo,
          p.dir,
          op,
          indexOrder,
          propertyPredicates.view.mapValues(parseExpression).toMap,
          argumentIds
        )
      case _ =>
        throw new IllegalArgumentException(s"'$pattern' cannot be parsed as a dynamic relationship type expression")
    }
  }

  def dynamicRelationshipTypeLookup(
    leftNode: Option[String],
    relName: Option[String],
    relTypeExpr: Expression,
    rightNode: Option[String],
    direction: SemanticDirection,
    operator: SetOperator,
    indexOrder: IndexOrder,
    propertyPredicates: Map[String, Expression],
    argumentIds: Set[String]
  ): IMPL = {
    newRelationship(varFor(relName))
    newNode(varFor(leftNode))
    newNode(varFor(rightNode))

    direction match {
      case SemanticDirection.OUTGOING =>
        appendAtCurrentIndent(LeafOperator(DynamicDirectedRelationshipTypeLookup(
          varFor(relName),
          varFor(leftNode),
          DynamicElement.Simple(relTypeExpr, operator),
          varFor(rightNode),
          argumentIds.map(varFor),
          indexOrder,
          resolvePropertyKeyIdsForDynamicIndexUse(propertyPredicates)
        )(_)))
      case SemanticDirection.INCOMING =>
        appendAtCurrentIndent(LeafOperator(DynamicDirectedRelationshipTypeLookup(
          varFor(relName),
          varFor(rightNode),
          DynamicElement.Simple(relTypeExpr, operator),
          varFor(leftNode),
          argumentIds.map(varFor),
          indexOrder,
          resolvePropertyKeyIdsForDynamicIndexUse(propertyPredicates)
        )(_)))
      case SemanticDirection.BOTH =>
        appendAtCurrentIndent(LeafOperator(DynamicUndirectedRelationshipTypeLookup(
          varFor(relName),
          varFor(leftNode),
          DynamicElement.Simple(relTypeExpr, operator),
          varFor(rightNode),
          argumentIds.map(varFor),
          indexOrder,
          resolvePropertyKeyIdsForDynamicIndexUse(propertyPredicates)
        )(_)))
    }
  }

  def partitionedRelationshipTypeScan(pattern: String, args: String*): IMPL = {
    val p = patternParser.parse(pattern)
    newRelationship(varFor(p.relName))
    newNode(varFor(p.maybeFrom))
    newNode(varFor(p.maybeTo))
    if (!p.length.isSimple) throw new UnsupportedOperationException("Cannot do a scan from a variable pattern")
    val typ =
      if (p.relTypes.size == 1) p.relTypes.head
      else throw new UnsupportedOperationException("Cannot do a scan with multiple types")

    p.dir match {
      case SemanticDirection.OUTGOING =>
        appendAtCurrentIndent(LeafOperator(PartitionedDirectedRelationshipTypeScan(
          varFor(p.maybeRelName),
          varFor(p.maybeFrom),
          typ,
          varFor(p.maybeTo),
          args.map(varFor).toSet
        )(_)))
      case SemanticDirection.INCOMING =>
        appendAtCurrentIndent(LeafOperator(PartitionedDirectedRelationshipTypeScan(
          varFor(p.maybeRelName),
          varFor(p.maybeTo),
          typ,
          varFor(p.maybeFrom),
          args.map(varFor).toSet
        )(_)))
      case SemanticDirection.BOTH =>
        appendAtCurrentIndent(LeafOperator(PartitionedUndirectedRelationshipTypeScan(
          varFor(p.maybeRelName),
          varFor(p.maybeFrom),
          typ,
          varFor(p.maybeTo),
          args.map(varFor).toSet
        )(_)))
    }
  }

  def nodeCountFromCountStore(name: String, labels: Seq[Option[String]], args: String*): IMPL = {
    val labelNames = labels.map(maybeLabel => maybeLabel.map(labelName)).toList
    appendAtCurrentIndent(LeafOperator(NodeCountFromCountStore(
      varFor(name),
      labelNames,
      args.map(varFor).toSet
    )(_)))
  }

  def relationshipCountFromCountStore(
    name: String,
    maybeStartLabel: Option[String],
    relTypes: Seq[String],
    maybeEndLabel: Option[String],
    args: String*
  ): IMPL = {
    val startLabel = maybeStartLabel.map(labelName)
    val relTypeNames = relTypes.map(relTypeName)
    val endLabel = maybeEndLabel.map(labelName)
    appendAtCurrentIndent(LeafOperator(RelationshipCountFromCountStore(
      varFor(name),
      startLabel,
      relTypeNames,
      endLabel,
      args.map(varFor).toSet
    )(_)))
  }

  def nodeIndexOperator(
    indexSeekString: String,
    getValue: String => GetValueFromIndexBehavior = _ => DoNotGetValue,
    indexOrder: IndexOrder = IndexOrderNone,
    paramExpr: IterableOnce[Expression] = None,
    argumentIds: Set[String] = Set.empty,
    unique: Boolean = false,
    customQueryExpression: Option[QueryExpression[Expression]] = None,
    indexType: IndexType = IndexType.RANGE,
    supportPartitionedScan: Boolean = true
  ): IMPL = {
    val planBuilder = (idGen: IdGen) => {
      val plan = nodeIndexSeek(
        indexSeekString,
        getValue,
        indexOrder,
        paramExpr.iterator.toSeq,
        argumentIds,
        unique,
        customQueryExpression,
        indexType,
        supportPartitionedScan
      )(idGen)
      plan
    }
    appendAtCurrentIndent(LeafOperator(planBuilder))
  }

  def remoteNodeIndexSeek(
    indexSeekString: String,
    getValue: String => GetValueFromIndexBehavior = _ => DoNotGetValue,
    indexOrder: IndexOrder = IndexOrderNone,
    paramExpr: Iterable[Expression] = Seq.empty,
    argumentIds: Set[String] = Set.empty,
    unique: Boolean = false,
    indexType: IndexType = IndexType.RANGE,
    supportPartitionedScan: Boolean = true
  ): IdGen => NodeIndexLeafPlan = {
    val label = resolver.getLabelId(IndexSeek.labelFromIndexSeekString(indexSeekString))
    val propIds: PartialFunction[String, Int] = {
      case x => resolver.getPropertyKeyId(x)
    }
    val planBuilder = (idGen: IdGen) => {
      val plan = IndexSeek.remoteNodeIndexSeek(
        indexSeekString,
        getValue,
        indexOrder,
        paramExpr,
        argumentIds,
        Some(propIds),
        label,
        unique,
        indexType,
        supportPartitionedScan
      )(idGen)
      newNode(varFor(plan.idName.name))
      plan
    }
    planBuilder
  }

  def remoteNodeIndexOperator(
    indexSeekString: String,
    getValue: String => GetValueFromIndexBehavior = _ => DoNotGetValue,
    indexOrder: IndexOrder = IndexOrderNone,
    paramExpr: IterableOnce[Expression] = None,
    argumentIds: Set[String] = Set.empty,
    unique: Boolean = false,
    indexType: IndexType = IndexType.RANGE,
    supportPartitionedScan: Boolean = true
  ): IMPL = {
    val planBuilder = (idGen: IdGen) =>
      remoteNodeIndexSeek(
        indexSeekString,
        getValue,
        indexOrder,
        paramExpr.iterator.toSeq,
        argumentIds,
        unique,
        indexType,
        supportPartitionedScan
      )(idGen)
    appendAtCurrentIndent(LeafOperator(planBuilder))
  }

  def partitionedNodeIndexOperator(
    indexSeekString: String,
    getValue: String => GetValueFromIndexBehavior = _ => DoNotGetValue,
    paramExpr: IterableOnce[Expression] = None,
    argumentIds: Set[String] = Set.empty,
    customQueryExpression: Option[QueryExpression[Expression]] = None,
    indexType: IndexType = IndexType.RANGE
  ): IMPL = {
    val planBuilder = (idGen: IdGen) => {
      val plan = partitionedNodeIndexSeek(
        indexSeekString,
        getValue,
        paramExpr.iterator.toSeq,
        argumentIds,
        customQueryExpression,
        indexType
      )(idGen)
      plan
    }
    appendAtCurrentIndent(LeafOperator(planBuilder))
  }

  def relationshipIndexOperator(
    indexSeekString: String,
    getValue: String => GetValueFromIndexBehavior = _ => DoNotGetValue,
    indexOrder: IndexOrder = IndexOrderNone,
    paramExpr: Iterable[Expression] = Seq.empty,
    argumentIds: Set[String] = Set.empty,
    unique: Boolean = false,
    customQueryExpression: Option[QueryExpression[Expression]] = None,
    indexType: IndexType = IndexType.RANGE,
    supportPartitionedScan: Boolean = true
  ): IMPL = {
    val planBuilder = (idGen: IdGen) => {
      val plan = relationshipIndexSeek(
        indexSeekString,
        getValue,
        indexOrder,
        paramExpr,
        argumentIds,
        unique,
        customQueryExpression,
        indexType,
        supportPartitionedScan
      )(idGen)
      plan
    }
    appendAtCurrentIndent(LeafOperator(planBuilder))
  }

  def partitionedRelationshipIndexOperator(
    indexSeekString: String,
    getValue: String => GetValueFromIndexBehavior = _ => DoNotGetValue,
    paramExpr: Iterable[Expression] = Seq.empty,
    argumentIds: Set[String] = Set.empty,
    customQueryExpression: Option[QueryExpression[Expression]] = None,
    indexType: IndexType = IndexType.RANGE
  ): IMPL = {
    val planBuilder = (idGen: IdGen) => {
      val plan = partitionedRelationshipIndexSeek(
        indexSeekString,
        getValue,
        paramExpr,
        argumentIds,
        customQueryExpression,
        indexType
      )(idGen)
      plan
    }
    appendAtCurrentIndent(LeafOperator(planBuilder))
  }

  def nodeIndexSeek(
    indexSeekString: String,
    getValue: String => GetValueFromIndexBehavior = _ => DoNotGetValue,
    indexOrder: IndexOrder = IndexOrderNone,
    paramExpr: Iterable[Expression] = Seq.empty,
    argumentIds: Set[String] = Set.empty,
    unique: Boolean = false,
    customQueryExpression: Option[QueryExpression[Expression]] = None,
    indexType: IndexType = IndexType.RANGE,
    supportPartitionedScan: Boolean = true
  ): IdGen => NodeIndexLeafPlan = {
    val label = resolver.getLabelId(IndexSeek.labelFromIndexSeekString(indexSeekString))
    val propIds: PartialFunction[String, Int] = {
      case x => resolver.getPropertyKeyId(x)
    }
    val planBuilder = (idGen: IdGen) => {
      val plan = IndexSeek.nodeIndexSeek(
        indexSeekString,
        getValue,
        indexOrder,
        paramExpr,
        argumentIds,
        Some(propIds),
        label,
        unique,
        customQueryExpression,
        indexType,
        supportPartitionedScan
      )(idGen)
      newNode(varFor(plan.idName.name))
      plan
    }
    planBuilder
  }

  def partitionedNodeIndexSeek(
    indexSeekString: String,
    getValue: String => GetValueFromIndexBehavior = _ => DoNotGetValue,
    paramExpr: Iterable[Expression] = Seq.empty,
    argumentIds: Set[String] = Set.empty,
    customQueryExpression: Option[QueryExpression[Expression]] = None,
    indexType: IndexType = IndexType.RANGE
  ): IdGen => NodeIndexLeafPlan = {
    val label = resolver.getLabelId(IndexSeek.labelFromIndexSeekString(indexSeekString))
    val propIds: PartialFunction[String, Int] = {
      case x => resolver.getPropertyKeyId(x)
    }
    val planBuilder = (idGen: IdGen) => {
      val plan = IndexSeek.partitionedNodeIndexSeek(
        indexSeekString,
        getValue,
        paramExpr,
        argumentIds,
        Some(propIds),
        label,
        customQueryExpression,
        indexType
      )(idGen)
      newNode(varFor(plan.idName.name))
      plan
    }
    planBuilder
  }

  def relationshipIndexSeek(
    indexSeekString: String,
    getValue: String => GetValueFromIndexBehavior = _ => DoNotGetValue,
    indexOrder: IndexOrder = IndexOrderNone,
    paramExpr: Iterable[Expression] = Seq.empty,
    argumentIds: Set[String] = Set.empty,
    unique: Boolean = false,
    customQueryExpression: Option[QueryExpression[Expression]] = None,
    indexType: IndexType = IndexType.RANGE,
    supportPartitionedScan: Boolean = true
  ): IdGen => RelationshipIndexLeafPlan = {
    val relType = resolver.getRelTypeId(IndexSeek.relTypeFromIndexSeekString(indexSeekString))
    val propIds: PartialFunction[String, Int] = {
      case x => resolver.getPropertyKeyId(x)
    }
    val planBuilder = (idGen: IdGen) => {
      val plan = IndexSeek.relationshipIndexSeek(
        indexSeekString,
        getValue,
        indexOrder,
        paramExpr,
        argumentIds,
        Some(propIds),
        relType,
        unique,
        customQueryExpression,
        indexType,
        supportPartitionedScan
      )(idGen)
      plan.idName.foreach(r => newRelationship(varFor(r.name)))
      plan.leftNode.foreach(l => newNode(varFor(l.name)))
      plan.rightNode.foreach(r => newNode(varFor(r.name)))
      plan
    }
    planBuilder
  }

  def partitionedRelationshipIndexSeek(
    indexSeekString: String,
    getValue: String => GetValueFromIndexBehavior = _ => DoNotGetValue,
    paramExpr: Iterable[Expression] = Seq.empty,
    argumentIds: Set[String] = Set.empty,
    customQueryExpression: Option[QueryExpression[Expression]] = None,
    indexType: IndexType = IndexType.RANGE
  ): IdGen => RelationshipIndexLeafPlan = {
    val relType = resolver.getRelTypeId(IndexSeek.relTypeFromIndexSeekString(indexSeekString))
    val propIds: PartialFunction[String, Int] = {
      case x => resolver.getPropertyKeyId(x)
    }
    val planBuilder = (idGen: IdGen) => {
      val plan = IndexSeek.partitionedRelationshipIndexSeek(
        indexSeekString,
        getValue,
        paramExpr,
        argumentIds,
        Some(propIds),
        relType,
        customQueryExpression,
        indexType
      )(idGen)
      plan.rightNode.foreach(r => newRelationship(varFor(r.name)))
      plan.leftNode.foreach(n => newNode(varFor(n.name)))
      plan.rightNode.foreach(n => newNode(varFor(n.name)))
      plan
    }
    planBuilder
  }

  def multiNodeIndexSeekOperator(seeks: (IMPL => IdGen => NodeIndexLeafPlan)*): IMPL = {
    val planBuilder = (idGen: IdGen) => {
      MultiNodeIndexSeek(seeks.map(_(this)(idGen).asInstanceOf[NodeIndexSeekSingleLabelLeafPlan]))(idGen)
    }
    appendAtCurrentIndent(LeafOperator(planBuilder))
  }

  def nodeVectorIndexSearch(
    node: String,
    labelNames: Seq[String],
    properties: Seq[String],
    indexName: String,
    vector: ToExpression,
    limit: ToExpression,
    score: String = "",
    argumentIds: Set[String] = Set.empty,
    getValueFromIndex: String => GetValueFromIndexBehavior = _ => DoNotGetValue,
    entityFilter: EntityFilterQueryExpression[Expression] = MatchAllQueryExpression,
    propertyFilter: Option[QueryExpression[Expression]] = None
  ): IMPL = {
    val labels = labelNames.map(labelName => LabelToken(labelName, LabelId(resolver.getLabelId(labelName))))
    val propIDs = properties
      .map(p =>
        IndexedProperty(
          PropertyKeyToken(PropertyKeyName(p)(NONE), PropertyKeyId(resolver.getPropertyKeyId(p))),
          getValueFromIndex(p),
          NODE_TYPE
        )
      )

    val nodeVariable = varFor(node)
    newNode(nodeVariable)

    val planBuilder = (idGen: IdGen) => {
      NodeVectorIndexSearch(
        nodeVariable,
        labels,
        propIDs,
        if (score.isEmpty) None else Some(varFor(score)),
        indexName,
        toExpression(vector),
        toExpression(limit),
        entityFilter,
        propertyFilter,
        argumentIds.map(varFor)
      )(idGen)
    }
    appendAtCurrentIndent(LeafOperator(planBuilder))
  }

  def relationshipVectorIndexSearch(
    pattern: String,
    typeNames: Seq[String],
    properties: Seq[String],
    indexName: String,
    vector: ToExpression,
    limit: ToExpression,
    score: String = "",
    argumentIds: Set[String] = Set.empty,
    getValueFromIndex: String => GetValueFromIndexBehavior = _ => DoNotGetValue,
    entityFilter: EntityFilterQueryExpression[Expression] = MatchAllQueryExpression,
    propertyFilter: Option[QueryExpression[Expression]] = None
  ): IMPL = {

    val p = patternParser.parse(pattern)
    newRelationship(varFor(p.maybeRelName))
    newNode(varFor(p.maybeFrom))
    newNode(varFor(p.maybeTo))
    if (!p.length.isSimple) throw new UnsupportedOperationException("Cannot do a search from a variable pattern")
    val types = typeNames.map(typeName => RelationshipTypeToken(typeName, RelTypeId(resolver.getRelTypeId(typeName))))
    val propIDs = properties
      .map(p =>
        IndexedProperty(
          PropertyKeyToken(PropertyKeyName(p)(NONE), PropertyKeyId(resolver.getPropertyKeyId(p))),
          getValueFromIndex(p),
          RELATIONSHIP_TYPE
        )
      )

    p.dir match {
      case SemanticDirection.OUTGOING =>
        appendAtCurrentIndent(LeafOperator(
          DirectedRelationshipVectorIndexSearch(
            varFor(p.maybeRelName),
            varFor(p.maybeFrom),
            varFor(p.maybeTo),
            types,
            propIDs,
            if (score.isEmpty) None else Some(varFor(score)),
            indexName,
            toExpression(vector),
            toExpression(limit),
            entityFilter,
            propertyFilter,
            argumentIds.map(varFor)
          )(_)
        ))
      case SemanticDirection.INCOMING =>
        appendAtCurrentIndent(LeafOperator(
          DirectedRelationshipVectorIndexSearch(
            varFor(p.maybeRelName),
            varFor(p.maybeTo),
            varFor(p.maybeFrom),
            types,
            propIDs,
            if (score.isEmpty) None else Some(varFor(score)),
            indexName,
            toExpression(vector),
            toExpression(limit),
            entityFilter,
            propertyFilter,
            argumentIds.map(varFor)
          )(_)
        ))
      case SemanticDirection.BOTH =>
        appendAtCurrentIndent(LeafOperator(
          UndirectedRelationshipVectorIndexSearch(
            varFor(p.maybeRelName),
            varFor(p.maybeFrom),
            varFor(p.maybeTo),
            types,
            propIDs,
            if (score.isEmpty) None else Some(varFor(score)),
            indexName,
            toExpression(vector),
            toExpression(limit),
            entityFilter,
            propertyFilter,
            argumentIds.map(varFor)
          )(_)
        ))
    }
  }

  def nodeFulltextIndexSearch(
    node: String,
    labelNames: Seq[String],
    properties: Seq[String],
    indexName: String,
    queryString: ToExpression,
    limit: ToExpression = literalInt(Int.MaxValue),
    analyzer: Option[ToExpression] = None,
    skip: Option[ToExpression] = None,
    score: String = "",
    argumentIds: Set[String] = Set.empty,
    getValueFromIndex: String => GetValueFromIndexBehavior = _ => DoNotGetValue
  ): IMPL = {
    val labels = labelNames.map(labelName => LabelToken(labelName, LabelId(resolver.getLabelId(labelName))))
    val propIDs = properties
      .map(p =>
        IndexedProperty(
          PropertyKeyToken(PropertyKeyName(p)(NONE), PropertyKeyId(resolver.getPropertyKeyId(p))),
          getValueFromIndex(p),
          NODE_TYPE
        )
      )

    val nodeVariable = varFor(node)
    newNode(nodeVariable)

    val planBuilder = (idGen: IdGen) => {
      NodeFulltextIndexSearch(
        nodeVariable,
        labels,
        propIDs,
        if (score.isEmpty) None else Some(varFor(score)),
        indexName,
        toExpression(queryString),
        analyzer.map(a => toExpression(a)),
        skip.map(s => toExpression(s)),
        toExpression(limit),
        argumentIds.map(varFor)
      )(idGen)
    }
    appendAtCurrentIndent(LeafOperator(planBuilder))
  }

  def relationshipFulltextIndexSearch(
    pattern: String,
    typeNames: Seq[String],
    properties: Seq[String],
    indexName: String,
    queryString: ToExpression,
    limit: ToExpression = literalInt(Int.MaxValue),
    analyzer: Option[ToExpression] = None,
    skip: Option[ToExpression] = None,
    score: String = "",
    argumentIds: Set[String] = Set.empty,
    getValueFromIndex: String => GetValueFromIndexBehavior = _ => DoNotGetValue
  ): IMPL = {

    val p = patternParser.parse(pattern)
    newRelationship(varFor(p.maybeRelName))
    newNode(varFor(p.maybeFrom))
    newNode(varFor(p.maybeTo))
    if (!p.length.isSimple) throw new UnsupportedOperationException("Cannot do a search from a variable pattern")
    val types = typeNames.map(typeName => RelationshipTypeToken(typeName, RelTypeId(resolver.getRelTypeId(typeName))))
    val propIDs = properties
      .map(p =>
        IndexedProperty(
          PropertyKeyToken(PropertyKeyName(p)(NONE), PropertyKeyId(resolver.getPropertyKeyId(p))),
          getValueFromIndex(p),
          RELATIONSHIP_TYPE
        )
      )

    p.dir match {
      case SemanticDirection.OUTGOING =>
        appendAtCurrentIndent(LeafOperator(
          DirectedRelationshipFulltextIndexSearch(
            varFor(p.maybeRelName),
            varFor(p.maybeFrom),
            varFor(p.maybeTo),
            types,
            propIDs,
            if (score.isEmpty) None else Some(varFor(score)),
            indexName,
            toExpression(queryString),
            toExpression(limit),
            analyzer.map(a => toExpression(a)),
            skip.map(s => toExpression(s)),
            argumentIds.map(varFor)
          )(_)
        ))
      case SemanticDirection.INCOMING =>
        appendAtCurrentIndent(LeafOperator(
          DirectedRelationshipFulltextIndexSearch(
            varFor(p.maybeRelName),
            varFor(p.maybeTo),
            varFor(p.maybeFrom),
            types,
            propIDs,
            if (score.isEmpty) None else Some(varFor(score)),
            indexName,
            toExpression(queryString),
            toExpression(limit),
            analyzer.map(a => toExpression(a)),
            skip.map(s => toExpression(s)),
            argumentIds.map(varFor)
          )(_)
        ))
      case SemanticDirection.BOTH =>
        appendAtCurrentIndent(LeafOperator(
          UndirectedRelationshipFulltextIndexSearch(
            varFor(p.maybeRelName),
            varFor(p.maybeFrom),
            varFor(p.maybeTo),
            types,
            propIDs,
            if (score.isEmpty) None else Some(varFor(score)),
            indexName,
            toExpression(queryString),
            toExpression(limit),
            analyzer.map(a => toExpression(a)),
            skip.map(s => toExpression(s)),
            argumentIds.map(varFor)
          )(_)
        ))
    }
  }

  def pointDistanceNodeIndexSeek(
    node: String,
    labelName: String,
    property: String,
    point: String,
    distance: ToExpression,
    getValue: GetValueFromIndexBehavior = DoNotGetValue,
    indexOrder: IndexOrder = IndexOrderNone,
    inclusive: Boolean = false,
    argumentIds: Set[String] = Set.empty,
    indexType: IndexType = IndexType.POINT
  ): IMPL = {
    exprPointDistanceNodeIndexSeek(
      node,
      labelName,
      property,
      function("point", parseExpression(point)),
      toExpression(distance),
      getValue,
      indexOrder,
      inclusive,
      argumentIds,
      indexType
    )
  }

  def pointBoundingBoxNodeIndexSeek(
    node: String,
    labelName: String,
    property: String,
    lowerLeft: String,
    upperRight: String,
    getValue: GetValueFromIndexBehavior = DoNotGetValue,
    indexOrder: IndexOrder = IndexOrderNone,
    argumentIds: Set[String] = Set.empty,
    indexType: IndexType = IndexType.POINT
  ): IMPL = {
    pointBoundingBoxNodeIndexSeekExpr(
      node,
      labelName,
      property,
      lowerLeft,
      upperRight,
      getValue,
      indexOrder,
      argumentIds,
      indexType
    )
  }

  def pointDistanceNodeIndexSeekParam(
    node: String,
    labelName: String,
    property: String,
    paramName: String,
    distance: Double,
    getValue: GetValueFromIndexBehavior = DoNotGetValue,
    indexOrder: IndexOrder = IndexOrderNone,
    argumentIds: Set[String] = Set.empty,
    indexType: IndexType = IndexType.POINT,
    inclusive: Boolean = true,
    supportPartitionedScan: Boolean = false
  ): IMPL = {
    val label = resolver.getLabelId(labelName)

    val propId = resolver.getPropertyKeyId(property)
    val planBuilder = (idGen: IdGen) => {
      val labelToken = LabelToken(labelName, LabelId(label))
      val propToken = PropertyKeyToken(PropertyKeyName(property)(NONE), PropertyKeyId(propId))
      val indexedProperty = IndexedProperty(propToken, getValue, NODE_TYPE)

      val e: QueryExpression[PointDistanceSeekRangeWrapper] =
        RangeQueryExpression(PointDistanceSeekRangeWrapper(PointDistanceRange[Expression](
          asParameter(paramName),
          literalFloat(distance),
          inclusive
        ))(NONE))

      val plan = NodeIndexSeek(
        varFor(node),
        labelToken,
        Seq(indexedProperty),
        e,
        argumentIds.map(varFor),
        indexOrder,
        indexType,
        supportPartitionedScan
      )(idGen)
      newNode(plan.idName)
      plan
    }
    appendAtCurrentIndent(LeafOperator(planBuilder))
  }

  def cachedPropertyPointDistanceNodeIndexSeek(
    node: String,
    labelName: String,
    property: String,
    point: CachedProperty,
    distanceExpr: Expression,
    getValue: GetValueFromIndexBehavior = DoNotGetValue,
    indexOrder: IndexOrder = IndexOrderNone,
    inclusive: Boolean = false,
    argumentIds: Set[String] = Set.empty,
    indexType: IndexType = IndexType.POINT
  ): IMPL = {
    exprPointDistanceNodeIndexSeek(
      node,
      labelName,
      property,
      point,
      distanceExpr,
      getValue,
      indexOrder,
      inclusive,
      argumentIds,
      indexType
    )
  }

  private def exprPointDistanceNodeIndexSeek(
    node: String,
    labelName: String,
    property: String,
    point: Expression,
    distanceExpr: Expression,
    getValue: GetValueFromIndexBehavior,
    indexOrder: IndexOrder,
    inclusive: Boolean,
    argumentIds: Set[String],
    indexType: IndexType
  ): IMPL = {
    val label = resolver.getLabelId(labelName)

    val propId = resolver.getPropertyKeyId(property)
    val planBuilder = (idGen: IdGen) => {
      val labelToken = LabelToken(labelName, LabelId(label))
      val propToken = PropertyKeyToken(PropertyKeyName(property)(NONE), PropertyKeyId(propId))
      val indexedProperty = IndexedProperty(propToken, getValue, NODE_TYPE)
      val otherPointExpr =
        RangeQueryExpression(PointDistanceSeekRangeWrapper(
          PointDistanceRange(point, distanceExpr, inclusive)
        )(NONE))
      val plan = NodeIndexSeek(
        varFor(node),
        labelToken,
        Seq(indexedProperty),
        otherPointExpr,
        argumentIds.map(varFor),
        indexOrder,
        indexType,
        supportPartitionedScan = false
      )(idGen)
      newNode(plan.idName)
      plan
    }
    appendAtCurrentIndent(LeafOperator(planBuilder))
  }

  def pointBoundingBoxNodeIndexSeekExpr(
    node: String,
    labelName: String,
    property: String,
    lowerLeft: String,
    upperRight: String,
    getValue: GetValueFromIndexBehavior = DoNotGetValue,
    indexOrder: IndexOrder = IndexOrderNone,
    argumentIds: Set[String] = Set.empty,
    indexType: IndexType = IndexType.POINT
  ): IMPL = {
    val label = resolver.getLabelId(labelName)

    val propId = resolver.getPropertyKeyId(property)
    val planBuilder = (idGen: IdGen) => {
      val labelToken = LabelToken(labelName, LabelId(label))
      val propToken = PropertyKeyToken(PropertyKeyName(property)(NONE), PropertyKeyId(propId))
      val indexedProperty = IndexedProperty(propToken, getValue, NODE_TYPE)
      val e =
        RangeQueryExpression(PointBoundingBoxSeekRangeWrapper(
          PointBoundingBoxRange(
            function("point", parseExpression(lowerLeft)),
            function("point", parseExpression(upperRight))
          )
        )(NONE))
      val plan = NodeIndexSeek(
        varFor(node),
        labelToken,
        Seq(indexedProperty),
        e,
        argumentIds.map(varFor),
        indexOrder,
        indexType,
        supportPartitionedScan = false
      )(idGen)
      newNode(plan.idName)
      plan
    }
    appendAtCurrentIndent(LeafOperator(planBuilder))
  }

  def pointDistanceRelationshipIndexSeek(
    pattern: String,
    point: String,
    distance: Double,
    inclusive: Boolean = false,
    getValue: GetValueFromIndexBehavior = DoNotGetValue,
    indexOrder: IndexOrder = IndexOrderNone,
    argumentIds: Set[String] = Set.empty,
    indexType: IndexType = IndexType.POINT
  ): IMPL = {
    pointDistanceRelationshipIndexSeekExpr(
      pattern,
      point,
      literalFloat(distance),
      inclusive,
      getValue,
      indexOrder,
      argumentIds,
      indexType
    )
  }

  def pointDistanceRelationshipIndexSeekExpr(
    pattern: String,
    point: String,
    distanceExpr: Expression,
    inclusive: Boolean = false,
    getValue: GetValueFromIndexBehavior = DoNotGetValue,
    indexOrder: IndexOrder = IndexOrderNone,
    argumentIds: Set[String] = Set.empty,
    indexType: IndexType = IndexType.POINT
  ): IMPL = {
    val relType = resolver.getRelTypeId(IndexSeek.relTypeFromIndexSeekString(pattern))
    val propIds: PartialFunction[String, Int] = {
      case x => resolver.getPropertyKeyId(x)
    }
    val planBuilder = (idGen: IdGen) => {
      val customQueryExpression =
        RangeQueryExpression(PointDistanceSeekRangeWrapper(
          PointDistanceRange(function("point", parseExpression(point)), distanceExpr, inclusive)
        )(NONE))

      val plan = IndexSeek.relationshipIndexSeek(
        pattern,
        getValue = _ => getValue,
        indexOrder = indexOrder,
        paramExpr = Seq.empty,
        argumentIds = argumentIds,
        propIds = Some(propIds),
        typeId = relType,
        customQueryExpression = Some(customQueryExpression),
        indexType = indexType,
        supportPartitionedScan = false
      )(idGen)
      plan.idName.foreach(r => newRelationship(varFor(r.name)))
      plan.leftNode.foreach(l => newNode(varFor(l.name)))
      plan.rightNode.foreach(r => newNode(varFor(r.name)))
      plan
    }
    appendAtCurrentIndent(LeafOperator(planBuilder))
  }

  def pointBoundingBoxRelationshipIndexSeek(
    pattern: String,
    lowerLeft: String,
    upperRight: String,
    getValue: GetValueFromIndexBehavior = DoNotGetValue,
    indexOrder: IndexOrder = IndexOrderNone,
    argumentIds: Set[String] = Set.empty,
    indexType: IndexType = IndexType.POINT
  ): IMPL = {
    pointBoundingBoxRelationshipIndexSeekExpr(
      pattern,
      lowerLeft,
      upperRight,
      getValue,
      indexOrder,
      argumentIds,
      indexType
    )
  }

  def pointBoundingBoxRelationshipIndexSeekExpr(
    pattern: String,
    lowerLeft: String,
    upperRight: String,
    getValue: GetValueFromIndexBehavior = DoNotGetValue,
    indexOrder: IndexOrder = IndexOrderNone,
    argumentIds: Set[String] = Set.empty,
    indexType: IndexType = IndexType.POINT
  ): IMPL = {
    val relType = resolver.getRelTypeId(IndexSeek.relTypeFromIndexSeekString(pattern))
    val propIds: PartialFunction[String, Int] = {
      case x => resolver.getPropertyKeyId(x)
    }
    val planBuilder = (idGen: IdGen) => {
      val customQueryExpression =
        RangeQueryExpression(PointBoundingBoxSeekRangeWrapper(
          PointBoundingBoxRange(
            function("point", parseExpression(lowerLeft)),
            function("point", parseExpression(upperRight))
          )
        )(NONE))

      val plan = IndexSeek.relationshipIndexSeek(
        pattern,
        getValue = _ => getValue,
        indexOrder = indexOrder,
        paramExpr = Seq.empty,
        argumentIds = argumentIds,
        propIds = Some(propIds),
        typeId = relType,
        customQueryExpression = Some(customQueryExpression),
        indexType = indexType,
        supportPartitionedScan = false
      )(idGen)
      plan.rightNode.foreach(r => newRelationship(varFor(r.name)))
      plan.leftNode.foreach(l => newNode(varFor(l.name)))
      plan.rightNode.foreach(r => newNode(varFor(r.name)))
      plan
    }
    appendAtCurrentIndent(LeafOperator(planBuilder))
  }

  def aggregation(groupingExpressions: Seq[String], aggregationExpression: Seq[String]): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp => {
      Aggregation(
        lp,
        toVarMap(parser.parseProjections(groupingExpressions: _*)),
        parseAggregationProjections(aggregationExpression: _*)
      )(_)
    }))
  }

  def aggregation(
    groupingExpressions: Map[String, Expression],
    aggregationExpressions: Map[String, Expression]
  ): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp => {
      Aggregation(lp, toVarMap(groupingExpressions), toVarMap(aggregationExpressions))(_)
    }))
  }

  def orderedAggregation(
    groupingExpressions: Seq[String],
    aggregationExpression: Seq[String],
    orderToLeverage: Seq[String]
  ): IMPL = {
    val order = orderToLeverage.map(parseExpression)
    appendAtCurrentIndent(UnaryOperator(lp =>
      OrderedAggregation(
        lp,
        toVarMap(parser.parseProjections(groupingExpressions: _*)),
        parseAggregationProjections(aggregationExpression: _*),
        order
      )(_)
    ))
  }

  def orderedAggregation(
    groupingExpressions: Map[String, Expression],
    aggregationExpressions: Map[String, Expression],
    orderToLeverage: Seq[String]
  ): IMPL = {
    val order = orderToLeverage.map(parseExpression)
    appendAtCurrentIndent(UnaryOperator(lp => {
      OrderedAggregation(lp, toVarMap(groupingExpressions), toVarMap(aggregationExpressions), order)(_)
    }))
  }

  def apply(): IMPL =
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) => Apply(lhs, rhs)(_)))

  def antiSemiApply(): IMPL =
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) => AntiSemiApply(lhs, rhs)(_)))

  def semiApply(): IMPL =
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) => SemiApply(lhs, rhs)(_)))

  def letAntiSemiApply(item: String): IMPL =
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) => LetAntiSemiApply(lhs, rhs, varFor(item))(_)))

  def letSemiApply(item: String): IMPL =
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) => LetSemiApply(lhs, rhs, varFor(item))(_)))

  def conditionalApply(items: String*): IMPL =
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) => ConditionalApply(lhs, rhs, items.map(varFor))(_)))

  def antiConditionalApply(items: String*): IMPL =
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) => AntiConditionalApply(lhs, rhs, items.map(varFor))(_)))

  def selectOrSemiApply(predicate: ToExpression): IMPL = {
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) => {
      SelectOrSemiApply(lhs, rhs, toExpression(predicate))(_)
    }))
  }

  def selectOrAntiSemiApply(predicate: ToExpression): IMPL = {
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) => {
      SelectOrAntiSemiApply(lhs, rhs, toExpression(predicate))(_)
    }))
  }

  def letSelectOrSemiApply(idName: String, predicate: ToExpression): IMPL = {
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) => {
      LetSelectOrSemiApply(lhs, rhs, varFor(idName), toExpression(predicate))(_)
    }))
  }

  def letSelectOrAntiSemiApply(idName: String, predicate: ToExpression): IMPL = {
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) => {
      LetSelectOrAntiSemiApply(lhs, rhs, varFor(idName), toExpression(predicate))(_)
    }))
  }

  def rollUpApply(collectionName: String, variableToCollect: String): IMPL =
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) =>
      RollUpApply(lhs, rhs, varFor(collectionName), varFor(variableToCollect))(_)
    ))

  def foreachApply(variable: String, expression: ToExpression): IMPL =
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) =>
      ForeachApply(lhs, rhs, varFor(variable), toExpression(expression))(_)
    ))

  def foreach(variable: String, expression: ToExpression, mutations: Seq[SimpleMutatingPattern]): IMPL =
    appendAtCurrentIndent(UnaryOperator(lp => Foreach(lp, varFor(variable), toExpression(expression), mutations)(_)))

  def subqueryForeach(): IMPL =
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) => SubqueryForeach(lhs, rhs)(_)))

  def cartesianProduct(): IMPL =
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) => CartesianProduct(lhs, rhs)(_)))

  def union(): IMPL =
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) => Union(lhs, rhs)(_)))

  def assertSameNode(node: String): IMPL =
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) => AssertSameNode(varFor(node), lhs, rhs)(_)))

  def assertSameRelationship(idName: String): IMPL =
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) => AssertSameRelationship(varFor(idName), lhs, rhs)(_)))

  def orderedUnion(sortedOn: String*): IMPL =
    orderedUnionColumns(parser.parseSort(sortedOn))

  def orderedUnionColumns(sortedOn: Seq[ColumnOrder]): IMPL =
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) => OrderedUnion(lhs, rhs, sortedOn)(_)))

  def expandAll(pattern: String): IMPL = expand(pattern, ExpandAll)

  def nonFuseable(): IMPL =
    appendAtCurrentIndent(UnaryOperator(lp => NonFuseable(lp)(_)))

  def nonPipelined(): IMPL =
    appendAtCurrentIndent(UnaryOperator(lp => NonPipelined(lp)(_)))

  def nonPipelinedStreaming(expandFactor: Long = 1L): IMPL =
    appendAtCurrentIndent(UnaryOperator(lp => NonPipelinedStreaming(lp, expandFactor)(_)))

  def pipelineBreaker(flowProbe: FlowProbe = NoopFlowProbe): IMPL =
    appendAtCurrentIndent(UnaryOperator(lp => PipelineBreaker(lp, flowProbe)(_)))

  def prober(probe: Prober.Probe): IMPL =
    appendAtCurrentIndent(UnaryOperator(lp => Prober(lp, probe)(_)))

  def cacheProperties(properties: String*): IMPL = {
    cacheProperties(properties.map(parseExpression(_).asInstanceOf[LogicalProperty]).toSet)
  }

  def cacheProperties(properties: Set[LogicalProperty]): IMPL = {
    appendAtCurrentIndent(UnaryOperator(source => CacheProperties(source, properties)(_)))
  }

  def remoteBatchProperties(properties: ToExpression*): IMPL =
    appendAtCurrentIndent(UnaryOperator(source =>
      RemoteBatchProperties(
        source,
        properties.map(toExpression(_).asInstanceOf[LogicalProperty]).toSet
      )(_)
    ))

  def remoteBatchPropertiesWithFilter(properties: ToExpression*)(expressions: ToExpression*): IMPL =
    appendAtCurrentIndent(UnaryOperator(source =>
      RemoteBatchPropertiesWithFilter(
        source,
        expressions.map(toExpression(_)).toSet,
        properties.map(toExpression(_).asInstanceOf[LogicalProperty]).toSet
      )(_)
    ))

  def remoteBatchPropertiesWithPushdownOperatorsOnNode(
    variable: String,
    properties: String*
  )(pushdownOperators: PushdownOperators): IMPL = {
    remoteBatchPropertiesWithPushdownOperators(variable, NODE_TYPE, properties, pushdownOperators)
  }

  def remoteBatchPropertiesWithPushdownOperatorsOnRelationship(
    variable: String,
    properties: String*
  )(pushdownOperators: PushdownOperators): IMPL = {
    remoteBatchPropertiesWithPushdownOperators(variable, RELATIONSHIP_TYPE, properties, pushdownOperators)
  }

  private def remoteBatchPropertiesWithPushdownOperators(
    variable: String,
    entityType: EntityType,
    properties: Seq[String],
    pushdownOperators: PushdownOperators
  ): IMPL = {
    appendAtCurrentIndent(UnaryOperator { source =>
      val (unexpected, orderBy) = pushdownOperators.orderBy.partitionMap {
        case (LogicalVariable(`variable`), order) => Right(order)
        case (expression, _)                      => Left(expression)
      }
      assert(unexpected.isEmpty, s"Unexpected sorting keys, expected $variable, got $unexpected")
      RemoteBatchPropertiesWithPushdownOperators(
        source,
        variable = varFor(variable),
        entityType = entityType,
        properties = properties.map(PropertyKeyName(_)(pos)).toSet,
        predicates = pushdownOperators.filter.map(_.endoRewrite(expressionRewriter)),
        distinctBy = pushdownOperators.distinct,
        orderBy = orderBy,
        limit = pushdownOperators.limit,
        importedConstantValues = pushdownOperators.importedConstantValues,
        importedPerRowValues = pushdownOperators.importedPerRowValues
      )(_)
    })
  }

  def setProperty(entity: ToExpression, propertyKey: String, value: ToExpression): IMPL = {
    appendAtCurrentIndent(UnaryOperator(source =>
      SetProperty(source, toExpression(entity), PropertyKeyName(propertyKey)(pos), toExpression(value))(_)
    ))
  }

  def setDynamicProperty(
    entityExpression: ToExpression,
    propertyString: ToExpression,
    valueExpression: ToExpression
  ): IMPL = {
    appendAtCurrentIndent(UnaryOperator(source =>
      SetDynamicProperty(
        source,
        toExpression(entityExpression),
        toExpression(propertyString),
        toExpression(valueExpression)
      )(_)
    ))
  }

  def setProperty(entity: Expression, propertyKey: String, value: Expression): IMPL = {
    appendAtCurrentIndent(UnaryOperator(source =>
      SetProperty(source, entity, PropertyKeyName(propertyKey)(pos), value)(_)
    ))
  }

  def setNodeProperty(node: String, propertyKey: String, value: ToExpression): IMPL = {
    appendAtCurrentIndent(UnaryOperator(source =>
      SetNodeProperty(source, varFor(node), PropertyKeyName(propertyKey)(pos), toExpression(value))(_)
    ))
  }

  def setRelationshipProperty(relationship: String, propertyKey: String, value: ToExpression): IMPL = {
    appendAtCurrentIndent(
      UnaryOperator(source =>
        SetRelationshipProperty(
          source,
          varFor(relationship),
          PropertyKeyName(propertyKey)(pos),
          toExpression(value)
        )(_)
      )
    )
  }

  def setProperties(entity: ToExpression, items: (String, ToExpression)*): IMPL = {
    appendAtCurrentIndent(UnaryOperator(source =>
      SetProperties(
        source,
        toExpression(entity),
        items.map(item => (PropertyKeyName(item._1)(pos), toExpression(item._2)))
      )(_)
    ))
  }

  def setNodeProperties(node: String, items: (String, ToExpression)*): IMPL = {
    appendAtCurrentIndent(UnaryOperator(source =>
      SetNodeProperties(
        source,
        varFor(node),
        items.map(item => (PropertyKeyName(item._1)(pos), toExpression(item._2)))
      )(_)
    ))
  }

  def setRelationshipProperties(relationship: String, items: (String, ToExpression)*): IMPL = {
    appendAtCurrentIndent(UnaryOperator(source =>
      SetRelationshipProperties(
        source,
        varFor(relationship),
        items.map(item => (PropertyKeyName(item._1)(pos), toExpression(item._2)))
      )(_)
    ))
  }

  def setPropertiesFromMap(entity: ToExpression, map: ToExpression, removeOtherProps: Boolean): IMPL = {
    appendAtCurrentIndent(UnaryOperator(source =>
      SetPropertiesFromMap(source, toExpression(entity), toExpression(map), removeOtherProps)(_)
    ))
  }

  def setNodePropertiesFromMap(node: String, map: ToExpression, removeOtherProps: Boolean): IMPL = {
    appendAtCurrentIndent(UnaryOperator(source =>
      SetNodePropertiesFromMap(source, varFor(node), toExpression(map), removeOtherProps)(_)
    ))
  }

  def setRelationshipPropertiesFromMap(relationship: String, map: ToExpression, removeOtherProps: Boolean): IMPL = {
    appendAtCurrentIndent(UnaryOperator(source =>
      SetRelationshipPropertiesFromMap(source, varFor(relationship), toExpression(map), removeOtherProps)(_)
    ))
  }

  def create(commands: CreateCommand*): IMPL = {
    commands.foreach {
      case node: CreateNode => newNode(node.variable)
      case relationship: CreateRelationship =>
        newRelationship(relationship.variable)
        newNode(relationship.startNode)
        newNode(relationship.endNode)
    }

    appendAtCurrentIndent(UnaryOperator(source => Create(source, commands)(_)))
  }

  def merge(
    nodes: Seq[CreateNode] = Seq.empty,
    relationships: Seq[CreateRelationship] = Seq.empty,
    onMatch: Seq[SetMutatingPattern] = Seq.empty,
    onCreate: Seq[SetMutatingPattern] = Seq.empty,
    lockNodes: Set[String] = Set.empty
  ): IMPL = {
    appendAtCurrentIndent(UnaryOperator(source =>
      Merge(source, nodes, relationships, onMatch, onCreate, lockNodes.map(varFor))(_)
    ))
  }

  def fusedMerge(
    nodes: Seq[CreateNode] = Seq.empty,
    relationships: Seq[CreateRelationship] = Seq.empty,
    onMatch: Seq[SetMutatingPattern] = Seq.empty,
    onCreate: Seq[SetMutatingPattern] = Seq.empty,
    lockNodes: Set[String] = Set.empty
  ): IMPL = {
    appendAtCurrentIndent(UnaryOperator(source =>
      FusedMerge(source, nodes, relationships, onMatch, onCreate, lockNodes.map(varFor))(_)
    ))
  }

  def mergeUniqueNode(
    node: String,
    labelName: String,
    predicates: Seq[(String, String)],
    onMatch: Seq[(String, String)] = Seq.empty,
    onCreate: Seq[(String, String)] = Seq.empty,
    args: Set[String] = Set.empty,
    indexType: IndexType = IndexType.RANGE,
    cacheValues: Boolean = false
  ): IMPL = {
    mergeUniqueNodeExpression(
      node,
      labelName,
      predicates,
      onMatch.map(e => e._1 -> parseExpression(e._2)),
      onCreate.map(e => e._1 -> parseExpression(e._2)),
      args,
      indexType,
      cacheValues
    )
  }

  def mergeUniqueNodeExpression(
    node: String,
    labelName: String,
    predicates: Seq[(String, String)],
    onMatch: Seq[(String, Expression)] = Seq.empty,
    onCreate: Seq[(String, Expression)] = Seq.empty,
    args: Set[String] = Set.empty,
    indexType: IndexType = IndexType.RANGE,
    cacheValues: Boolean = false
  ): IMPL = {

    val n = varFor(VariableParser.unescaped(node))
    newNode(n)
    val label = resolver.getLabelId(labelName)

    val (properties, seekExpressions) = predicates.foldLeft((Seq.empty[IndexedProperty], Seq.empty[Expression]))(
      (acc, current) =>
        (
          acc._1 :+
            IndexedProperty(
              PropertyKeyToken(current._1, PropertyKeyId(resolver.getPropertyKeyId(current._1))),
              if (cacheValues) GetValue else DoNotGetValue,
              NODE_TYPE
            ),
          acc._2 :+ parseExpression(current._2)
        )
    )
    appendAtCurrentIndent(LeafOperator(MergeUniqueNode(
      n,
      LabelToken(labelName, LabelId(label)),
      properties,
      seekExpressions,
      args.map(a => VariableParser.unescapedVar(a)),
      IndexOrderNone,
      indexType,
      onMatch.map {
        case (k, v) => PropertyKeyName(k)(pos) -> v
      },
      onCreate.map {
        case (k, v) => PropertyKeyName(k)(pos) -> v
      }
    )(_)))
  }

  def mergeInto(
    pattern: String,
    onMatch: Seq[(String, String)] = Seq.empty,
    onCreate: Seq[(String, String)] = Seq.empty
  ): IMPL = {
    mergeIntoExpression(
      pattern,
      onMatch = onMatch.map(e => e._1 -> parseExpression(e._2)),
      onCreate = onCreate.map(e => e._1 -> parseExpression(e._2))
    )
  }

  def mergeIntoExpression(
    pattern: String,
    onMatch: Seq[(String, Expression)] = Seq.empty,
    onCreate: Seq[(String, Expression)] = Seq.empty
  ): IMPL = {
    val p = patternParser.parse(pattern)
    newRelationship(varFor(p.relName))
    newNode(varFor(p.from))
    newNode(varFor(p.to))
    if (!p.length.isSimple) throw new UnsupportedOperationException("Cannot mergeInto from a variable pattern")
    if (p.relTypes.length != 1)
      throw new UnsupportedOperationException("MergeInto pattern must contain single relationship type")

    appendAtCurrentIndent(UnaryOperator(source =>
      MergeInto(
        source,
        varFor(p.relName),
        varFor(p.from),
        p.dir,
        p.relTypes.head,
        varFor(p.to),
        onMatch.map(e => PropertyKeyName(e._1)(pos) -> e._2),
        onCreate.map(e => PropertyKeyName(e._1)(pos) -> e._2)
      )(_)
    ))
  }

  def nodeHashJoin(nodes: String*): IMPL = {
    appendAtCurrentIndent(BinaryOperator((left, right) => NodeHashJoin(nodes.map(varFor).toSet, left, right)(_)))
  }

  def rightOuterHashJoin(nodes: String*): IMPL = {
    appendAtCurrentIndent(BinaryOperator((left, right) => RightOuterHashJoin(nodes.map(varFor).toSet, left, right)(_)))
  }

  def leftOuterHashJoin(nodes: String*): IMPL = {
    appendAtCurrentIndent(BinaryOperator((left, right) => LeftOuterHashJoin(nodes.map(varFor).toSet, left, right)(_)))
  }

  def valueHashJoin(predicate: ToExpression): IMPL = {
    val expression = toExpression(predicate)
    expression match {
      case e: Equals =>
        appendAtCurrentIndent(BinaryOperator((left, right) => ValueHashJoin(left, right, e)(_)))
      case _ => throw new IllegalArgumentException(s"can't join on $expression")
    }
  }

  def valueMergeJoin(predicate: String): IMPL = {
    val expression = parseExpression(predicate)
    expression match {
      case e: Equals =>
        appendAtCurrentIndent(BinaryOperator((left, right) => ValueMergeJoin(left, right, e)(_)))
      case _ => throw new IllegalArgumentException(s"can't join on $expression")
    }
  }

  def input(
    nodes: Seq[String] = Seq.empty,
    relationships: Seq[String] = Seq.empty,
    variables: Seq[String] = Seq.empty,
    nullable: Boolean = true
  ): IMPL = {
    if (indent != 0) {
      throw new IllegalStateException("The input operator has to be the left-most leaf of the plan")
    }
    if (
      nodes.toSet.size < nodes.size || relationships.toSet.size < relationships.size || variables.toSet.size < variables.size
    ) {
      throw new IllegalArgumentException("Input must create unique variables")
    }
    newNodes(nodes)
    newRelationships(relationships)
    newVariables(variables)
    appendAtCurrentIndent(LeafOperator(Input(
      nodes.map(varFor),
      relationships.map(varFor),
      variables.map(varFor),
      nullable
    )(_)))
  }

  def injectCompilationError(): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp => {
      InjectCompilationError(lp)(_)
    }))
  }

  def lockNodes(nodes: String*): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp => {
      LockNodes(lp, nodes.map(varFor).toSet)(_)
    }))
  }

  def filter(expressions: ToExpression*): IMPL =
    appendAtCurrentIndent(UnaryOperator(lp =>
      Selection(expressions.map(toExpression(_)), lp)(_)
    ))

  def simulatedFilter(selectivity: Double): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp => {
      SimulatedSelection(lp, selectivity)(_)
    }))
  }

  def errorPlan(e: Exception): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp => ErrorPlan(lp, e)(_)))
  }

  def nestedPlanCollectExpressionProjection(resultList: String, resultPart: String): IMPL = {
    val inner = parseExpression(resultPart)
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) =>
      Projection(
        lhs,
        Map(varFor(resultList) -> NestedPlanCollectExpression(rhs, inner, "collect(...)")(NONE))
      )(_)
    ))
  }

  def nestedPlanExistsExpressionProjection(resultList: String): IMPL = {
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) =>
      Projection(lhs, Map(varFor(resultList) -> NestedPlanExistsExpression(rhs, "exists(...)")(NONE)))(_)
    ))
  }

  def nestedPlanGetByNameExpressionProjection(columnNameToGet: String, resultName: String): IMPL = {
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) =>
      Projection(
        lhs,
        Map(varFor(resultName) -> NestedPlanGetByNameExpression(rhs, varFor(columnNameToGet), "getByName(...)")(NONE))
      )(_)
    ))
  }

  /**
   * The right-hand side is a nested plan of a NestedPlanGetByNameExpression that is placed inside of
   * the extract expression of a list comprehension on a literal list of strings.
   */
  def nestedPlanGetByNameExpressionInListComprehensionProjection(
    listElementVariable: String,
    list: Seq[String],
    columnNameToGet: String,
    resultName: String
  ): IMPL = {
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) => {
      val nestedPlanExpression = NestedPlanGetByNameExpression(rhs, varFor(columnNameToGet), "getByName(...)")(NONE)
      val literalList = ListLiteral(list.map(StringLiteral(_)(NONE.withInputLength(0))))(NONE)
      val listComprehension =
        ListComprehension(varFor(listElementVariable), literalList, None, Some(nestedPlanExpression))(NONE)
      Projection(
        lhs,
        Map(varFor(resultName) -> listComprehension)
      )(_)
    }))
  }

  def triadicSelection(positivePredicate: Boolean, sourceId: String, seenId: String, targetId: String): IMPL =
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) =>
      TriadicSelection(lhs, rhs, positivePredicate, varFor(sourceId), varFor(seenId), varFor(targetId))(_)
    ))

  def triadicBuild(triadicSelectionId: Int, sourceId: String, seenId: String): IMPL =
    appendAtCurrentIndent(UnaryOperator(lp =>
      TriadicBuild(lp, varFor(sourceId), varFor(seenId), Some(Id(triadicSelectionId)))(_)
    ))

  def triadicFilter(triadicSelectionId: Int, positivePredicate: Boolean, sourceId: String, targetId: String): IMPL =
    appendAtCurrentIndent(UnaryOperator(lp =>
      TriadicFilter(lp, positivePredicate, varFor(sourceId), varFor(targetId), Some(Id(triadicSelectionId)))(_)
    ))

  def loadCSV(url: String, variableName: String, format: CSVFormat, fieldTerminator: Option[String] = None): IMPL = {
    val urlExpr = parseExpression(url)
    appendAtCurrentIndent(UnaryOperator(lp =>
      LoadCSV(
        lp,
        urlExpr,
        varFor(variableName),
        format,
        fieldTerminator,
        legacyCsvQuoteEscaping = GraphDatabaseSettings.csv_legacy_quote_escaping.defaultValue(),
        csvBufferSize = GraphDatabaseSettings.csv_buffer_size.defaultValue().toInt
      )(_)
    ))
  }

  def injectValue(variable: String, value: String): IMPL = {
    val collection = s"${variable}Collection"
    val level = indent
    unwind(s"$collection AS $variable")
    indent = level
    projection(s"$collection + [$value] AS $collection")
    indent = level
    aggregation(Seq(), Seq(s"collect($variable) AS $collection"))
  }

  def transactionForeach(
    batchSize: Long = TransactionForeach.defaultBatchSize,
    concurrency: TransactionConcurrency = TransactionConcurrency.Serial,
    onErrorBehaviour: InTransactionsOnErrorBehaviour = OnErrorFail,
    maybeReportAs: Option[String] = None,
    maybeRetryParameters: Option[InTransactionsRetryParameters] = None,
    maybeDisjointByParameters: Option[String] = None,
    effectiveDisjointBy: Seq[String] = Seq.empty
  ): IMPL =
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) =>
      TransactionForeach(
        lhs,
        rhs,
        literalInt(batchSize),
        concurrency,
        onErrorBehaviour,
        maybeReportAs.map(varFor),
        maybeRetryParameters,
        maybeDisjointByParameters.map(parseDisjointByParameters),
        effectiveDisjointBy.map(parseExpression)
      )(_)
    ))

  def transactionForeachWithRetry(
    batchSize: Long = TransactionForeach.defaultBatchSize,
    concurrency: TransactionConcurrency = TransactionConcurrency.Serial,
    onErrorBehaviour: InTransactionsOnErrorBehaviour = OnErrorRetryThenFail,
    maybeReportAs: Option[String] = None,
    maybeRetryTimeout: Option[FiniteDuration] = None,
    effectiveDisjointBy: Seq[String] = Seq.empty
  ): IMPL = {
    val maybeDurationExpr = maybeRetryTimeout.map(t => DecimalDoubleLiteral(t.toUnit(SECONDS).toString)(pos.zeroLength))
    transactionForeach(
      batchSize,
      concurrency,
      onErrorBehaviour,
      maybeReportAs,
      Some(InTransactionsRetryParameters(maybeDurationExpr)(pos)),
      effectiveDisjointBy = effectiveDisjointBy
    )
  }

  def transactionApply(
    batchSize: Long,
    concurrency: Int
  ): IMPL = {
    transactionApply(batchSize = batchSize, concurrency = TransactionConcurrency.Concurrent(concurrency))
  }

  def transactionApply(
    batchSize: Long = TransactionForeach.defaultBatchSize,
    concurrency: TransactionConcurrency = TransactionConcurrency.Serial,
    onErrorBehaviour: InTransactionsOnErrorBehaviour = OnErrorFail,
    maybeReportAs: Option[String] = None,
    maybeRetryParameters: Option[InTransactionsRetryParameters] = None,
    maybeDisjointByParameters: Option[String] = None,
    effectiveDisjointBy: Seq[String] = Seq.empty
  ): IMPL =
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) =>
      TransactionApply(
        lhs,
        rhs,
        literalInt(batchSize),
        concurrency,
        onErrorBehaviour,
        maybeReportAs.map(varFor),
        maybeRetryParameters,
        maybeDisjointByParameters.map(parseDisjointByParameters),
        effectiveDisjointBy.map(parseExpression)
      )(_)
    ))

  def transactionApplyWithRetry(
    batchSize: Long = TransactionForeach.defaultBatchSize,
    concurrency: TransactionConcurrency = TransactionConcurrency.Serial,
    onErrorBehaviour: InTransactionsOnErrorBehaviour = OnErrorRetryThenFail,
    maybeReportAs: Option[String] = None,
    maybeRetryTimeout: Option[FiniteDuration] = None,
    effectiveDisjointBy: Seq[String] = Seq.empty
  ): IMPL = {
    val maybeDurationExpr = maybeRetryTimeout.map(t => DecimalDoubleLiteral(t.toUnit(SECONDS).toString)(pos.zeroLength))
    transactionApply(
      batchSize,
      concurrency,
      onErrorBehaviour,
      maybeReportAs,
      Some(InTransactionsRetryParameters(maybeDurationExpr)(pos)),
      effectiveDisjointBy = effectiveDisjointBy
    )
  }

  def repeatTrail(
    trailParameters: TrailParameters
  ): IMPL = {
    // This one comes in as an argument, so we need to declare it as a node here
    newNode(varFor(trailParameters.innerStart))
    // This is the node we "expand-to" , so we need to declare it as a node here
    newNode(varFor(trailParameters.end))

    appendAtCurrentIndent(BinaryOperator((lhs, rhs) =>
      RepeatTrail(
        lhs,
        rhs,
        Repetition(trailParameters.min, trailParameters.max),
        varFor(trailParameters.start),
        varFor(trailParameters.end),
        varFor(trailParameters.innerStart),
        varFor(trailParameters.innerEnd),
        trailParameters.groupNodes.map { case (inner, outer) => VariableGrouping(varFor(inner), varFor(outer))(pos) },
        trailParameters.groupRelationships.map { case (inner, outer) =>
          VariableGrouping(varFor(inner), varFor(outer))(pos)
        },
        trailParameters.innerRelationships.map(varFor),
        trailParameters.previouslyBoundRelationships.map(varFor),
        trailParameters.previouslyBoundRelationshipGroups.map(varFor),
        trailParameters.reverseGroupVariableProjections,
        trailParameters.expansionMode,
        trailParameters.accumulators.map { case (initial, previous, current) =>
          AllReduceAccumulator(toExpression(initial), varFor(previous), varFor(current))(pos)
        }
      )(_)
    ))
  }

  def bidirectionalRepeatTrail(
    trailParameters: TrailParameters
  ): IMPL = {
    // These come in as arguments, so we need to declare them as nodes here
    newNode(varFor(trailParameters.innerStart))
    newNode(varFor(trailParameters.innerEnd))

    appendAtCurrentIndent(BinaryOperator((lhs, rhs) =>
      rhs match {
        case repeatOptions: RepeatOptions =>
          BidirectionalRepeatTrail(
            lhs,
            repeatOptions,
            Repetition(trailParameters.min, trailParameters.max),
            varFor(trailParameters.start),
            varFor(trailParameters.end),
            varFor(trailParameters.innerStart),
            varFor(trailParameters.innerEnd),
            trailParameters.groupNodes.map { case (inner, outer) =>
              VariableGrouping(varFor(inner), varFor(outer))(pos)
            },
            trailParameters.groupRelationships.map { case (inner, outer) =>
              VariableGrouping(varFor(inner), varFor(outer))(pos)
            },
            trailParameters.innerRelationships.map(varFor),
            trailParameters.previouslyBoundRelationships.map(varFor),
            trailParameters.previouslyBoundRelationshipGroups.map(varFor),
            trailParameters.reverseGroupVariableProjections
          )(_)
        case _ => throw new IllegalArgumentException("BidirectionalRepeatTrail must have RepeatOptions as its RHS.")
      }
    ))
  }

  def repeatWalk(
    walkParameters: WalkParameters
  ): IMPL = {
    // This one comes in as an argument, so we need to declare it as a node here
    newNode(varFor(walkParameters.innerStart))
    // This is the node we "expand-to" , so we need to declare it as a node here
    newNode(varFor(walkParameters.end))

    appendAtCurrentIndent(BinaryOperator((lhs, rhs) =>
      RepeatWalk(
        lhs,
        rhs,
        Repetition(walkParameters.min, walkParameters.max),
        varFor(walkParameters.start),
        varFor(walkParameters.end),
        varFor(walkParameters.innerStart),
        varFor(walkParameters.innerEnd),
        walkParameters.groupNodes.map { case (inner, outer) => VariableGrouping(varFor(inner), varFor(outer))(pos) },
        walkParameters.groupRelationships.map { case (inner, outer) =>
          VariableGrouping(varFor(inner), varFor(outer))(pos)
        },
        walkParameters.reverseGroupVariableProjections,
        walkParameters.innerRelationships.map(varFor),
        walkParameters.expansionMode,
        walkParameters.accumulators.map { case (initial, previous, current) =>
          AllReduceAccumulator(parser.parseExpression(initial), varFor(previous), varFor(current))(pos)
        }
      )(_)
    ))
  }

  def repeatAcyclic(
    acyclicParameters: AcyclicParameters
  ): IMPL = {
    // This one comes in as an argument, so we need to declare it as a node here
    newNode(varFor(acyclicParameters.innerStart))
    // This is the node we "expand-to" , so we need to declare it as a node here
    newNode(varFor(acyclicParameters.end))

    appendAtCurrentIndent(BinaryOperator((lhs, rhs) =>
      RepeatAcyclic(
        left = lhs,
        right = rhs,
        repetition = Repetition(acyclicParameters.min, acyclicParameters.max),
        start = varFor(acyclicParameters.start),
        end = varFor(acyclicParameters.end),
        innerStart = varFor(acyclicParameters.innerStart),
        innerEnd = varFor(acyclicParameters.innerEnd),
        nodeVariableGroupings = acyclicParameters.groupNodes.map { case (inner, outer) =>
          VariableGrouping(varFor(inner), varFor(outer))(pos)
        },
        previouslyBoundNodes = acyclicParameters.previouslyBoundNodes.map(varFor),
        previouslyBoundNodeGroups = acyclicParameters.previouslyBoundNodeGroups.map(varFor),
        innerNodes = acyclicParameters.innerNodes.map(varFor),
        relationshipVariableGroupings = acyclicParameters.groupRelationships.map { case (inner, outer) =>
          VariableGrouping(varFor(inner), varFor(outer))(pos)
        },
        previouslyBoundRelationships = acyclicParameters.previouslyBoundRelationships.map(varFor),
        previouslyBoundRelationshipGroups = acyclicParameters.previouslyBoundRelationshipGroups.map(varFor),
        innerRelationships = acyclicParameters.innerRelationships.map(varFor),
        reverseGroupVariableProjections = acyclicParameters.reverseGroupVariableProjections,
        expansionMode = acyclicParameters.expansionMode,
        accumulatorMappings = acyclicParameters.accumulators.map { case (initial, previous, current) =>
          AllReduceAccumulator(parser.parseExpression(initial), varFor(previous), varFor(current))(pos)
        }
      )(_)
    ))
  }

  def repeatOptions(): IMPL = {
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) =>
      RepeatOptions(lhs, rhs)(_)
    ))
  }

  // SHIP IT

  protected def buildLogicalPlan(): LogicalPlan = tree.build()

  def getSemanticTable: SemanticTable = semanticTable

  /**
   * Called every time a new node is introduced by some logical operator.
   */
  def newNode(node: LogicalVariable): Unit = {
    semanticTable = semanticTable.addNode(node.asInstanceOf[Variable])
  }

  def newNode(maybeNode: Option[LogicalVariable]): Unit = {
    maybeNode.foreach(newNode)
  }

  /**
   * Called every time a new relationship is introduced by some logical operator.
   */
  def newRelationship(relationship: LogicalVariable): Unit = {
    semanticTable = semanticTable.addRelationship(relationship.asInstanceOf[Variable])
  }

  def newRelationship(maybeRel: Option[LogicalVariable]): Unit = {
    maybeRel.foreach(newRelationship)
  }

  /**
   * Called every time a new variable is introduced by some logical operator.
   */
  def newVariable(variable: Variable): Unit = {
    semanticTable = semanticTable.addTypeInfoCTAny(variable)
  }

  /**
   * Called if a variable needs to have a specific type
   */
  def newVariable(variable: Variable, typ: CypherType): Unit = {
    semanticTable = semanticTable.addTypeInfo(variable, typ.invariant)
  }

  /**
   * Called if a variable needs to have a specific type
   */
  def newVariable(variable: Variable, typ: TypeSpec): Unit = {
    semanticTable = semanticTable.addTypeInfo(variable, typ)
  }

  protected def newAlias(variable: LogicalVariable, expression: Expression): Unit = {
    if (!semanticTable.types.contains(variable)) {
      val typeInfo = semanticTable.types.get(expression).orElse(findTypeIgnoringPosition(expression))
      val spec = typeInfo.map(_.actual).getOrElse(CTAny.invariant)
      semanticTable = semanticTable.addTypeInfo(variable, spec)
    }
  }

  private def findTypeIgnoringPosition(expr: Expression) =
    semanticTable.types.iterator
      .map { case (k, v) => (k.node, v) } // unwrap nodes to discard position
      .collectFirst { case (`expr`, t) => t }

  protected def newNodes(nodes: Iterable[String]): Seq[LogicalVariable] = {
    val result = nodes.map(varFor).toSeq
    result.foreach(newNode)
    result
  }

  protected def newRelationships(relationships: Iterable[String]): Seq[LogicalVariable] = {
    val result = relationships.map(varFor).toSeq
    result.foreach(newRelationship)
    result
  }

  protected def newVariables(variables: Iterable[String]): Seq[LogicalVariable] = {
    val result = variables.map(varFor).toSeq
    result.foreach(newVariable)
    result
  }

  private val hasLabelsAndHasTypeNormalizer = new HasLabelsAndHasTypeNormalizer {
    override def isNode(expr: Expression): Boolean = semanticTable.typeFor(expr).is(CTNode)

    override def isRelationship(expr: Expression): Boolean = semanticTable.typeFor(expr).is(CTRelationship)
  }

  private val resolvedFunction = topDown(
    Rewriter.lift {
      case f: FunctionInvocation if f.needsToBeResolved =>
        ResolvedFunctionInvocation.fromUnresolved(resolver.functionSignature)(f).coerceArguments
    }
  )

  protected def expressionRewriter: Rewriter =
    inSequence(
      RemoveSyntaxTracking.instance,
      hasLabelsAndHasTypeNormalizer,
      combineHasLabels,
      DesugarMapProjection.instance,
      resolvedFunction
    )

  /**
   * Returns the finalized output of the builder.
   */
  protected def build(readOnly: Boolean = true): T

  // HELPERS
  private def parseExpression(expression: String): Expression = {
    parser.parseExpression(expression).endoRewrite(expressionRewriter)
  }

  /**
   * Parses the DISJOINT BY mode written as it would appear in Cypher: "auto", "none", or a
   * parenthesised expression list such as "(a.id, b.id)".
   */
  private def parseDisjointByParameters(disjointByString: String): InTransactionsDisjointByParameters =
    parser.parseDisjointByParameters(disjointByString).endoRewrite(expressionRewriter)

  private def parseProjections(projections: String*): Map[LogicalVariable, Expression] = {
    toVarMap(parser.parseProjections(projections: _*)).view.mapValues {
      case f: FunctionInvocation if f.needsToBeResolved =>
        ResolvedFunctionInvocation.fromUnresolved(resolver.functionSignature)(f).coerceArguments
      case e => e
    }.toMap
  }

  private def parseAggregationProjections(projections: String*): Map[LogicalVariable, Expression] = {
    toVarMap(parser.parseAggregationProjections(projections: _*)).view.mapValues {
      case f: FunctionInvocation if f.needsToBeResolved =>
        ResolvedFunctionInvocation.fromUnresolved(resolver.functionSignature)(f).coerceArguments
      case e => e
    }.toMap
  }

  private def toExpression(expr: ToExpression, parser: Parser = parser): Expression =
    expr match {
      case ToExpression.FromString(str)      => parser.parseExpression(str).endoRewrite(expressionRewriter)
      case ToExpression.FromExpression(expr) => expr.endoRewrite(expressionRewriter)
    }

  /**
   * Enables/disables the builder for the next statements until ___CONDITION_END___ is called.
   */
  def ___CONDITION_BEGIN___(enabled: Boolean): IMPL = {
    this.enabled = enabled
    self
  }

  /**
   * Re-enables the builder after a call to ___CONDITION_BEGIN___.
   */
  def ___CONDITION_END___(): IMPL = {
    this.enabled = true
    self
  }

  private def resolvePropertyKeyIdsForDynamicIndexUse(propertyPredicates: Map[String, Expression])
    : Map[PropertyKeyToken, Expression] = {
    propertyPredicates.map { case (prop, expr) =>
      PropertyKeyToken(prop, PropertyKeyId(resolver.getPropertyKeyId(prop))) -> expr
    }
  }

  protected def appendAtCurrentIndent(operatorBuilder: OperatorBuilder): IMPL = {
    if (enabled) {
      if (tree == null) {
        if (wholePlan) {
          throw new IllegalStateException("Must call produceResults before adding other operators.")
        } else {
          tree = new Tree(operatorBuilder)
          looseEnds += tree
        }
      } else {
        val newTree = new Tree(operatorBuilder)

        def appendAtIndent(): Unit = {
          val parent = looseEnds(indent)
          parent.left = Some(newTree)
          looseEnds(indent) = newTree
        }

        indent - (looseEnds.size - 1) match {
          case 1 => // new rhs
            val parent = looseEnds.last
            parent.right = Some(newTree)
            looseEnds += newTree

          case 0 => // append to lhs
            appendAtIndent()

          case -1 => // end of rhs
            appendAtIndent()
            looseEnds.remove(looseEnds.size - 1)

          case _ =>
            throw new IllegalStateException("out of bounds")
        }
        indent = 0
      }
    }
    self
  }

  // AST construction
  protected def varFor(name: String): Variable = Variable(name)(pos, Variable.isIsolatedDefault)
  protected def varFor(maybeName: Option[String]): Option[Variable] = maybeName.map(varFor)
  private def labelName(s: String): LabelName = LabelName(s)(pos)
  private def relTypeName(s: String): RelTypeName = RelTypeName(s)(pos)

  private def literalInt(value: Long): SignedDecimalIntegerLiteral =
    SignedDecimalIntegerLiteral(value.toString)(pos.zeroLength)

  private def literalFloat(value: Double): DecimalDoubleLiteral =
    DecimalDoubleLiteral(value.toString)(pos.zeroLength)
  def literalString(str: String): StringLiteral = StringLiteral(str)(pos.withInputLength(0))

  def function(name: String, args: Expression*): FunctionInvocation =
    FunctionInvocation(FunctionName(name)(pos), distinct = false, args.toIndexedSeq)(pos)
}

object AbstractLogicalPlanBuilder {
  val pos: InputPosition.Range = InputPosition.NONE

  case class Predicate(entity: String, predicate: String) {

    // Note! Parses with default language.
    def asVariablePredicate: VariablePredicate =
      VariablePredicate(Variable(entity)(pos, Variable.isIsolatedDefault), Parser.Latest.parseExpression(predicate))
  }

  case class TrailParameters(
    min: Int,
    max: UpperBound,
    start: String,
    end: String,
    innerStart: String,
    innerEnd: String,
    groupNodes: Set[(String, String)],
    groupRelationships: Set[(String, String)],
    innerRelationships: Set[String],
    previouslyBoundRelationships: Set[String],
    previouslyBoundRelationshipGroups: Set[String],
    reverseGroupVariableProjections: Boolean,
    expansionMode: ExpansionMode,
    accumulators: Set[(ToExpression, String, String)]
  )

  object TrailParameters {

    def accumulator(initial: String, previous: String, next: String): (ToExpression, String, String) =
      (initial, previous, next)
  }

  case class WalkParameters(
    min: Int,
    max: UpperBound,
    start: String,
    end: String,
    innerStart: String,
    innerEnd: String,
    groupNodes: Set[(String, String)],
    groupRelationships: Set[(String, String)],
    reverseGroupVariableProjections: Boolean,
    innerRelationships: Set[String],
    expansionMode: ExpansionMode,
    accumulators: Set[(String, String, String)]
  )

  case class AcyclicParameters(
    min: Int,
    max: UpperBound,
    start: String,
    end: String,
    innerStart: String,
    innerEnd: String,
    groupNodes: Set[(String, String)],
    innerNodes: Set[String],
    previouslyBoundNodes: Set[String], // in the current PATH pattern
    previouslyBoundNodeGroups: Set[String], // in the current PATH pattern
    groupRelationships: Set[(String, String)],
    innerRelationships: Set[String],
    previouslyBoundRelationships: Set[String], // in the current GRAPH pattern
    previouslyBoundRelationshipGroups: Set[String], // // in the current GRAPH pattern
    reverseGroupVariableProjections: Boolean,
    expansionMode: ExpansionMode,
    accumulators: Set[(String, String, String)]
  )

  def createPattern(
    nodes: Seq[CreateNode] = Seq.empty,
    relationships: Seq[CreateRelationship] = Seq.empty
  ): CreatePattern = {
    CreatePattern(nodes ++ relationships)
  }

  def createNode(node: String, labels: String*): CreateNode =
    createNodeFull(node, labels = labels)

  def createNodeWithDynamicLabels(node: String, dynamicLabels: Expression*): CreateNode =
    createNodeFullExpression(node, dynamicLabels = dynamicLabels)

  // Note! Parses with default language.
  def createNodeWithProperties(node: String, labels: Seq[String], properties: String): CreateNode =
    createNodeFullExpression(node, labels, properties = Some(Parser.Latest.parseExpression(properties)))

  def createNodeWithProperties(node: String, labels: Seq[String], properties: MapExpression): CreateNode =
    createNodeFullExpression(node, labels, properties = Some(properties))

  // Note! Parses with default language.
  def createNodeFull(
    node: String,
    labels: Seq[String] = Seq.empty,
    dynamicLabels: Seq[String] = Seq.empty,
    properties: Option[String] = None
  ): CreateNode =
    createNodeFullExpression(
      node,
      labels = labels,
      dynamicLabels = dynamicLabels.map(Parser.Latest.parseExpression),
      properties = properties.map(Parser.Latest.parseExpression)
    )

  def createNodeFullExpression(
    node: String,
    labels: Seq[String] = Seq.empty,
    dynamicLabels: Seq[Expression] = Seq.empty,
    properties: Option[Expression] = None
  ): CreateNode =
    CreateNode(
      varFor(node),
      labels.map(LabelName(_)(pos)).toSet,
      dynamicLabels.toSet,
      properties
    )

  // Note! Parses with default language.
  def createRelationship(
    relationship: String,
    left: String,
    typ: String,
    right: String,
    direction: SemanticDirection = OUTGOING,
    properties: Option[String] = None
  ): CreateRelationship = {
    val props = properties.map(Parser.Latest.parseExpression)
    if (props.exists(!_.isInstanceOf[MapExpression]))
      throw new IllegalArgumentException("Property must be a Map Expression")
    createRelationshipFull(
      relationship,
      left,
      RelTypeName(typ)(pos),
      right,
      direction,
      props
    )
  }

  // Note! Parses with default language.
  def createRelationshipWithDynamicType(
    relationship: String,
    left: String,
    typeExpr: String,
    right: String,
    direction: SemanticDirection = OUTGOING,
    properties: Option[String] = None
  ): CreateRelationship = {
    val dynamicType = Parser.Latest.parseExpression(typeExpr)
    val props = properties.map(Parser.Latest.parseExpression)
    if (props.exists(!_.isInstanceOf[MapExpression]))
      throw new IllegalArgumentException("Property must be a Map Expression")
    createRelationshipFull(
      relationship,
      left,
      DynamicRelTypeExpression(dynamicType)(pos),
      right,
      direction,
      props
    )
  }

  def createRelationshipExpression(
    relationship: String,
    left: String,
    typ: String,
    right: String,
    direction: SemanticDirection = OUTGOING,
    properties: Option[MapExpression] = None
  ): CreateRelationship = {
    createRelationshipFull(
      relationship,
      left,
      RelTypeName(typ)(pos),
      right,
      direction,
      properties
    )
  }

  def createRelationshipFull(
    relationship: String,
    left: String,
    typ: RelTypeExpression,
    right: String,
    direction: SemanticDirection = OUTGOING,
    properties: Option[Expression] = None
  ): CreateRelationship = {
    CreateRelationship(
      varFor(relationship),
      varFor(left),
      typ,
      varFor(right),
      direction,
      properties
    )
  }

  // Note! Parses with default language.
  def setNodeProperty(node: String, key: String, value: String): SetMutatingPattern =
    SetNodePropertyPattern(varFor(node), PropertyKeyName(key)(InputPosition.NONE), Parser.Latest.parseExpression(value))

  // Note! Parses with default language.
  def setNodeProperties(node: String, items: (String, String)*): SetMutatingPattern =
    SetNodePropertiesPattern(
      varFor(node),
      items.map(i => (PropertyKeyName(i._1)(InputPosition.NONE), Parser.Latest.parseExpression(i._2)))
    )

  // Note! Parses with default language.
  def setNodePropertiesFromMap(node: String, map: String, removeOtherProps: Boolean = true): SetMutatingPattern =
    SetNodePropertiesFromMapPattern(varFor(node), Parser.Latest.parseExpression(map), removeOtherProps)

  // Note! Parses with default language.
  def setRelationshipProperty(relationship: String, key: String, value: String): SetMutatingPattern =
    SetRelationshipPropertyPattern(
      varFor(relationship),
      PropertyKeyName(key)(InputPosition.NONE),
      Parser.Latest.parseExpression(value)
    )

  // Note! Parses with default language.
  def setRelationshipProperties(rel: String, items: (String, String)*): SetMutatingPattern =
    SetRelationshipPropertiesPattern(
      varFor(rel),
      items.map(i => (PropertyKeyName(i._1)(InputPosition.NONE), Parser.Latest.parseExpression(i._2)))
    )

  // Note! Parses with default language.
  def setRelationshipPropertiesFromMap(
    node: String,
    map: String,
    removeOtherProps: Boolean = true
  ): SetMutatingPattern =
    SetRelationshipPropertiesFromMapPattern(varFor(node), Parser.Latest.parseExpression(map), removeOtherProps)

  // Note! Parses with default language.
  def setProperty(entity: String, key: String, value: String): SetMutatingPattern =
    SetPropertyPattern(
      Parser.Latest.parseExpression(entity),
      PropertyKeyName(key)(InputPosition.NONE),
      Parser.Latest.parseExpression(value)
    )

  // Note! Parses with default language.
  def setDynamicProperty(entity: String, key: String, value: String): SetMutatingPattern =
    SetDynamicPropertyPattern(
      Parser.Latest.parseExpression(entity),
      Parser.Latest.parseExpression(key),
      Parser.Latest.parseExpression(value)
    )

  // Note! Parses with default language.
  def setProperties(entity: String, items: (String, String)*): SetMutatingPattern =
    SetPropertiesPattern(
      Parser.Latest.parseExpression(entity),
      items.map(i => (PropertyKeyName(i._1)(InputPosition.NONE), Parser.Latest.parseExpression(i._2)))
    )

  // Note! Parses with default language.
  def setPropertyFromMap(entity: String, map: String, removeOtherProps: Boolean = true): SetMutatingPattern =
    SetPropertiesFromMapPattern(
      Parser.Latest.parseExpression(entity),
      Parser.Latest.parseExpression(map),
      removeOtherProps
    )

  def setLabel(node: String, labels: String*): SetMutatingPattern =
    SetLabelPattern(varFor(node), labels.map(l => LabelName(l)(InputPosition.NONE)), Seq.empty)

  // Note! Parses with default language.
  def setLabel(node: String, staticLabels: Seq[String], dynamicLabelExpressions: Seq[String]): SetMutatingPattern =
    SetLabelPattern(
      varFor(node),
      staticLabels.map(l => LabelName(l)(InputPosition.NONE)),
      dynamicLabelExpressions.map(e => Parser.Latest.parseExpression(e))
    )

  // Note! Parses with default language.
  def setDynamicLabel(node: String, labels: String*): SetMutatingPattern =
    SetLabelPattern(varFor(node), Seq.empty, labels.map(l => Parser.Latest.parseExpression(l)))

  def removeLabel(node: String, labels: String*): RemoveLabelPattern =
    RemoveLabelPattern(varFor(node), labels.map(l => LabelName(l)(InputPosition.NONE)), Seq.empty)

  // Note! Parses with default language.
  def removeLabel(node: String, staticLabels: Seq[String], dynamicLabelExpressions: Seq[String]): SetMutatingPattern =
    RemoveLabelPattern(
      varFor(node),
      staticLabels.map(l => LabelName(l)(InputPosition.NONE)),
      dynamicLabelExpressions.map(e => Parser.Latest.parseExpression(e))
    )

  // Note! Parses with default language.
  def removeDynamicLabel(node: String, labels: String*): SetMutatingPattern =
    RemoveLabelPattern(varFor(node), Seq.empty, labels.map(l => Parser.Latest.parseExpression(l)))

  // Note! Parses with default language.
  def delete(entity: String, forced: Boolean = false): org.neo4j.cypher.internal.ir.DeleteExpression =
    org.neo4j.cypher.internal.ir.DeleteExpression(Parser.Latest.parseExpression(entity), forced)

  // Note! Parses with default language.
  def andsReorderable(predicateExpressionsOrStrings: AnyRef*): AndsReorderable = {
    val predicates = predicateExpressionsOrStrings.map {
      case s: String     => Parser.Latest.parseExpression(s)
      case e: Expression => e
      case other => throw new IllegalArgumentException(
          s"Expected Expression or String, got [${other.getClass.getSimpleName}] $other}"
        )
    }
    AndsReorderable(ListSet.from(predicates))(pos)
  }

  // Note! Parses with default language.
  def column(name: String, cachedProperties: String*): Column = {
    Column(
      varFor(name),
      cachedProperties.map(cp =>
        Parser.Latest.parseExpression(cp).asInstanceOf[CachedProperty].copy(failOnMissingEntity = false)(pos)
      ).toSet
    )
  }

  // Note! Parses with default language.
  def coerceToPredicate(expression: String) = CoerceToPredicate(Parser.Latest.parseExpression(expression))

  case class PushdownOperators(
    filter: Seq[Expression] = Seq.empty,
    distinct: Option[Expression] = None,
    orderBy: Seq[(Expression, PropertyKeyNameOrder)] = Seq.empty,
    limit: Option[Expression] = None,
    importedConstantValues: Set[Expression] = Set.empty,
    importedPerRowValues: Map[LogicalVariable, Expression] = Map.empty
  ) {

    def filter(exprs: String*): PushdownOperators = {
      val newPredicates = exprs.map(Parser.Latest.parseExpression)
      copy(filter = filter ++ newPredicates)
    }

    def distinct(expr: String): PushdownOperators = {
      copy(distinct = Some(Parser.Latest.parseExpression(expr)))
    }

    def orderBy(property: String, otherProperties: String*): PushdownOperators =
      copy(orderBy = parsePropertyKeyNameOrder(property) +: otherProperties.map(parsePropertyKeyNameOrder))

    private def parsePropertyKeyNameOrder(property: String): (Expression, PropertyKeyNameOrder) =
      Parser.Latest.parseSortItem(property) match {
        case AscSortItem(LogicalProperty(expression, propertyKeyName)) =>
          expression -> PropertyKeyNameOrder(propertyKeyName, PropertyKeyNameOrder.Ascending)
        case DescSortItem(LogicalProperty(expression, propertyKeyName)) =>
          expression -> PropertyKeyNameOrder(propertyKeyName, PropertyKeyNameOrder.Descending)
        case other => throw new IllegalArgumentException(s"Unexpected sort item: $other")
      }

    def limit(expr: String): PushdownOperators = {
      copy(limit = Some(Parser.Latest.parseExpression(expr)))
    }

    def importedConstantValues(exprs: String*): PushdownOperators = {
      copy(importedConstantValues = importedConstantValues ++ exprs.map(Parser.Latest.parseExpression).toSet)
    }

    def importedPerRowValues(exprs: Map[String, String]): PushdownOperators = {
      copy(importedPerRowValues =
        importedPerRowValues ++ exprs.map {
          case (assignedVariable, importedValue) =>
            varFor(assignedVariable) -> Parser.Latest.parseExpression(importedValue)
        }
      )
    }
  }

  // magnet pattern lets us provide parseable strings where expressions are required without requiring overloads
  sealed trait ToExpression

  object ToExpression {
    case class FromString(str: String) extends ToExpression

    case class FromExpression(expr: Expression) extends ToExpression

    // typeclass lets us provide auto conversions for vararg seqs
    trait Converter[A] {
      def toExpression(value: A): ToExpression
      def contramap[B](f: B => A): Converter[B] = (value: B) => toExpression(f(value))
    }

    object Converter {
      def apply[A](implicit C: Converter[A]): Converter[A] = C
      implicit val fromStringConverter: Converter[String] = (value: String) => FromString(value)

      implicit def fromExpressionConverter[E <: Expression]: Converter[E] =
        (value: E) => FromExpression(value)

      private val pos = InputPosition.NONE.zeroLength

      implicit val fromIntConverter: Converter[Int] =
        Converter[Expression].contramap(x => SignedDecimalIntegerLiteral(x.toString)(pos))

      implicit val fromLongConverter: Converter[Long] =
        Converter[Expression].contramap(x => SignedDecimalIntegerLiteral(x.toString)(pos))

      implicit val fromFloatConverter: Converter[Float] =
        Converter[Expression].contramap(x => DecimalDoubleLiteral(x.toString)(pos))

      implicit val fromDoubleConverter: Converter[Double] =
        Converter[Expression].contramap(x => DecimalDoubleLiteral(x.toString)(pos))

      implicit val fromBooleanConverter: Converter[Boolean] =
        Converter[Expression].contramap {
          case true  => True()(pos)
          case false => False()(pos)
        }
    }

    implicit def implicitConversion[A: Converter](value: A): ToExpression =
      Converter[A].toExpression(value)

    implicit def seqConverter[A: Converter](seq: Seq[A]): Seq[ToExpression] =
      seq.map(Converter[A].toExpression)

    implicit def optionConverter[A: Converter](opt: Option[A]): Option[ToExpression] =
      opt.map(Converter[A].toExpression)
  }

}
