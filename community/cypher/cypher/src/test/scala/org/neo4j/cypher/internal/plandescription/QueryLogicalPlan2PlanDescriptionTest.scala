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

import org.neo4j.cypher.QueryPlanTestSupport.StubExecutionPlan
import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.ast.CatalogName
import org.neo4j.cypher.internal.ast.GraphDirectReference
import org.neo4j.cypher.internal.ast.ProcedureResultItem
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsDisjointByMode.DisjointByAuto
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsDisjointByMode.DisjointByExpressions
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsDisjointByMode.DisjointByNone
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsDisjointByParameters
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorBreak
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorContinue
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorFail
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorRetryThenBreak
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorRetryThenContinue
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorRetryThenFail
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsRetryParameters
import org.neo4j.cypher.internal.expressions.AllReduceAccumulator
import org.neo4j.cypher.internal.expressions.And
import org.neo4j.cypher.internal.expressions.AndedPropertyInequalities
import org.neo4j.cypher.internal.expressions.AutoExtractedParameter
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.ExplicitParameter
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.FunctionInvocation.ArgumentAsc
import org.neo4j.cypher.internal.expressions.GreaterThanOrEqual
import org.neo4j.cypher.internal.expressions.In
import org.neo4j.cypher.internal.expressions.LabelToken
import org.neo4j.cypher.internal.expressions.LessThan
import org.neo4j.cypher.internal.expressions.ListLiteral
import org.neo4j.cypher.internal.expressions.MapExpression
import org.neo4j.cypher.internal.expressions.NODE_TYPE
import org.neo4j.cypher.internal.expressions.NilPathStep
import org.neo4j.cypher.internal.expressions.NodePathStep
import org.neo4j.cypher.internal.expressions.PathExpression
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.PropertyKeyToken
import org.neo4j.cypher.internal.expressions.RELATIONSHIP_TYPE
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.RelationshipTypeToken
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.SemanticDirection.BOTH
import org.neo4j.cypher.internal.expressions.SemanticDirection.INCOMING
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.expressions.SingleRelationshipPathStep
import org.neo4j.cypher.internal.expressions.functions.Collect
import org.neo4j.cypher.internal.expressions.functions.Count
import org.neo4j.cypher.internal.expressions.functions.IsEmpty
import org.neo4j.cypher.internal.expressions.functions.Point
import org.neo4j.cypher.internal.frontend.phases.FieldSignature
import org.neo4j.cypher.internal.frontend.phases.ProcedureReadOnlyAccess
import org.neo4j.cypher.internal.frontend.phases.ProcedureSignature
import org.neo4j.cypher.internal.frontend.phases.ResolvedFunctionInvocation
import org.neo4j.cypher.internal.frontend.phases.ResolvedNonLocalCall
import org.neo4j.cypher.internal.frontend.phases.UserFunctionSignature
import org.neo4j.cypher.internal.ir.CreateNode
import org.neo4j.cypher.internal.ir.CreateRelationship
import org.neo4j.cypher.internal.ir.EagernessReason
import org.neo4j.cypher.internal.ir.EagernessReason.Conflict
import org.neo4j.cypher.internal.ir.NoHeaders
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.RemoveLabelPattern
import org.neo4j.cypher.internal.ir.SelectivePathPattern.CountInteger
import org.neo4j.cypher.internal.ir.SetLabelPattern
import org.neo4j.cypher.internal.ir.SetNodePropertiesPattern
import org.neo4j.cypher.internal.ir.SetNodePropertyPattern
import org.neo4j.cypher.internal.ir.ShortestRelationshipPattern
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.ir.VarPatternLength
import org.neo4j.cypher.internal.ir.ordering.ColumnOrder
import org.neo4j.cypher.internal.logical.builder.IndexSeek.nodeIndexSeek
import org.neo4j.cypher.internal.logical.builder.IndexSeek.partitionedNodeIndexSeek
import org.neo4j.cypher.internal.logical.builder.IndexSeek.partitionedRelationshipIndexSeek
import org.neo4j.cypher.internal.logical.builder.IndexSeek.relationshipIndexSeek
import org.neo4j.cypher.internal.logical.builder.IndexSeek.remoteNodeIndexSeek
import org.neo4j.cypher.internal.logical.plans
import org.neo4j.cypher.internal.logical.plans.Aggregation
import org.neo4j.cypher.internal.logical.plans.AllNodesScan
import org.neo4j.cypher.internal.logical.plans.Anti
import org.neo4j.cypher.internal.logical.plans.AntiConditionalApply
import org.neo4j.cypher.internal.logical.plans.AntiSemiApply
import org.neo4j.cypher.internal.logical.plans.Apply
import org.neo4j.cypher.internal.logical.plans.ArgumentTracker
import org.neo4j.cypher.internal.logical.plans.Ascending
import org.neo4j.cypher.internal.logical.plans.AssertSameNode
import org.neo4j.cypher.internal.logical.plans.AssertSameRelationship
import org.neo4j.cypher.internal.logical.plans.BFSPruningVarExpand
import org.neo4j.cypher.internal.logical.plans.BidirectionalRepeatTrail
import org.neo4j.cypher.internal.logical.plans.CacheProperties
import org.neo4j.cypher.internal.logical.plans.CartesianProduct
import org.neo4j.cypher.internal.logical.plans.CompositeQueryExpression
import org.neo4j.cypher.internal.logical.plans.ConditionalApply
import org.neo4j.cypher.internal.logical.plans.Create
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
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipTypeScan
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipVectorIndexSearch
import org.neo4j.cypher.internal.logical.plans.DirectedUnionRelationshipTypesScan
import org.neo4j.cypher.internal.logical.plans.Distinct
import org.neo4j.cypher.internal.logical.plans.DoNotGetValue
import org.neo4j.cypher.internal.logical.plans.DynamicDirectedRelationshipTypeLookup
import org.neo4j.cypher.internal.logical.plans.DynamicElement
import org.neo4j.cypher.internal.logical.plans.DynamicElement.Any
import org.neo4j.cypher.internal.logical.plans.DynamicLabelNodeLookup
import org.neo4j.cypher.internal.logical.plans.DynamicUndirectedRelationshipTypeLookup
import org.neo4j.cypher.internal.logical.plans.Eager
import org.neo4j.cypher.internal.logical.plans.EmptyResult
import org.neo4j.cypher.internal.logical.plans.ErrorPlan
import org.neo4j.cypher.internal.logical.plans.ExclusiveBound
import org.neo4j.cypher.internal.logical.plans.ExistenceQueryExpression
import org.neo4j.cypher.internal.logical.plans.Expand
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandAll
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandInto
import org.neo4j.cypher.internal.logical.plans.Expand.VariablePredicate
import org.neo4j.cypher.internal.logical.plans.FindShortestPaths
import org.neo4j.cypher.internal.logical.plans.Foreach
import org.neo4j.cypher.internal.logical.plans.ForeachApply
import org.neo4j.cypher.internal.logical.plans.GetValue
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.logical.plans.IndexedProperty
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
import org.neo4j.cypher.internal.logical.plans.ManyQueryExpression
import org.neo4j.cypher.internal.logical.plans.ManySeekableArgs
import org.neo4j.cypher.internal.logical.plans.MatchAllQueryExpression
import org.neo4j.cypher.internal.logical.plans.Merge
import org.neo4j.cypher.internal.logical.plans.MergeInto
import org.neo4j.cypher.internal.logical.plans.MergeUniqueNode
import org.neo4j.cypher.internal.logical.plans.MultiNodeIndexSeek
import org.neo4j.cypher.internal.logical.plans.NFA
import org.neo4j.cypher.internal.logical.plans.NFABuilder
import org.neo4j.cypher.internal.logical.plans.NodeByElementIdSeek
import org.neo4j.cypher.internal.logical.plans.NodeByIdSeek
import org.neo4j.cypher.internal.logical.plans.NodeByLabelScan
import org.neo4j.cypher.internal.logical.plans.NodeCountFromCountStore
import org.neo4j.cypher.internal.logical.plans.NodeFulltextIndexSearch
import org.neo4j.cypher.internal.logical.plans.NodeHashJoin
import org.neo4j.cypher.internal.logical.plans.NodeIndexSeek
import org.neo4j.cypher.internal.logical.plans.NodeIndexSeekSingleLabelLeafPlan
import org.neo4j.cypher.internal.logical.plans.NodeUniqueIndexSeek
import org.neo4j.cypher.internal.logical.plans.NodeVectorIndexSearch
import org.neo4j.cypher.internal.logical.plans.NonExistenceQueryExpression
import org.neo4j.cypher.internal.logical.plans.NonFuseable
import org.neo4j.cypher.internal.logical.plans.Optional
import org.neo4j.cypher.internal.logical.plans.OptionalExpand
import org.neo4j.cypher.internal.logical.plans.OrderedAggregation
import org.neo4j.cypher.internal.logical.plans.OrderedDistinct
import org.neo4j.cypher.internal.logical.plans.PartialSort
import org.neo4j.cypher.internal.logical.plans.PartialTop
import org.neo4j.cypher.internal.logical.plans.PartitionedAllNodesScan
import org.neo4j.cypher.internal.logical.plans.PartitionedDirectedRelationshipTypeScan
import org.neo4j.cypher.internal.logical.plans.PartitionedDirectedUnionRelationshipTypesScan
import org.neo4j.cypher.internal.logical.plans.PartitionedIntersectionNodeByLabelsScan
import org.neo4j.cypher.internal.logical.plans.PartitionedNodeByLabelScan
import org.neo4j.cypher.internal.logical.plans.PartitionedSubtractionNodeByLabelsScan
import org.neo4j.cypher.internal.logical.plans.PartitionedUndirectedRelationshipTypeScan
import org.neo4j.cypher.internal.logical.plans.PartitionedUndirectedUnionRelationshipTypesScan
import org.neo4j.cypher.internal.logical.plans.PartitionedUnionNodeByLabelsScan
import org.neo4j.cypher.internal.logical.plans.PartitionedUnwindCollection
import org.neo4j.cypher.internal.logical.plans.PointBoundingBoxRange
import org.neo4j.cypher.internal.logical.plans.PointBoundingBoxSeekRangeWrapper
import org.neo4j.cypher.internal.logical.plans.PointDistanceRange
import org.neo4j.cypher.internal.logical.plans.PointDistanceSeekRangeWrapper
import org.neo4j.cypher.internal.logical.plans.PreserveOrder
import org.neo4j.cypher.internal.logical.plans.ProcedureCall
import org.neo4j.cypher.internal.logical.plans.ProduceResult
import org.neo4j.cypher.internal.logical.plans.ProjectEndpoints
import org.neo4j.cypher.internal.logical.plans.Projection
import org.neo4j.cypher.internal.logical.plans.PruningVarExpand
import org.neo4j.cypher.internal.logical.plans.RangeGreaterThan
import org.neo4j.cypher.internal.logical.plans.RangeQueryExpression
import org.neo4j.cypher.internal.logical.plans.RelationshipCountFromCountStore
import org.neo4j.cypher.internal.logical.plans.RemoveLabels
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
import org.neo4j.cypher.internal.logical.plans.SetRelationshipPropertiesFromMap
import org.neo4j.cypher.internal.logical.plans.SetRelationshipProperty
import org.neo4j.cypher.internal.logical.plans.SimulatedExpand
import org.neo4j.cypher.internal.logical.plans.SimulatedNodeScan
import org.neo4j.cypher.internal.logical.plans.SimulatedSelection
import org.neo4j.cypher.internal.logical.plans.SingleQueryExpression
import org.neo4j.cypher.internal.logical.plans.SingleSeekableArg
import org.neo4j.cypher.internal.logical.plans.Skip
import org.neo4j.cypher.internal.logical.plans.Sort
import org.neo4j.cypher.internal.logical.plans.StatefulShortestPath
import org.neo4j.cypher.internal.logical.plans.StatefulShortestPath.LengthBounds
import org.neo4j.cypher.internal.logical.plans.StatefulShortestPath.Selector
import org.neo4j.cypher.internal.logical.plans.SubtractionNodeByLabelsScan
import org.neo4j.cypher.internal.logical.plans.Top
import org.neo4j.cypher.internal.logical.plans.TransactionApply
import org.neo4j.cypher.internal.logical.plans.TransactionConcurrency
import org.neo4j.cypher.internal.logical.plans.TransactionForeach
import org.neo4j.cypher.internal.logical.plans.TraversalPathMode
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
import org.neo4j.cypher.internal.logical.plans.VarExpand
import org.neo4j.cypher.internal.logical.plans.ordering.DefaultProvidedOrderFactory
import org.neo4j.cypher.internal.logical.plans.ordering.ProvidedOrder
import org.neo4j.cypher.internal.plandescription.Arguments.CypherPlannerVersion
import org.neo4j.cypher.internal.plandescription.Arguments.EstimatedRows
import org.neo4j.cypher.internal.plandescription.Arguments.Order
import org.neo4j.cypher.internal.plandescription.Arguments.Planner
import org.neo4j.cypher.internal.plandescription.Arguments.PlannerImpl
import org.neo4j.cypher.internal.plandescription.Arguments.PlannerVersion
import org.neo4j.cypher.internal.plandescription.Arguments.PlannerVersionArgument
import org.neo4j.cypher.internal.plandescription.Arguments.Version
import org.neo4j.cypher.internal.plandescription.LogicalPlan2PlanDescriptionTestBase.anonVar
import org.neo4j.cypher.internal.plandescription.LogicalPlan2PlanDescriptionTestBase.details
import org.neo4j.cypher.internal.plandescription.LogicalPlan2PlanDescriptionTestBase.planDescription
import org.neo4j.cypher.internal.plandescription.asPrettyString.PrettyStringInterpolator
import org.neo4j.cypher.internal.planner.spi.IDPPlannerName
import org.neo4j.cypher.internal.planner.spi.ImmutablePlanningAttributes
import org.neo4j.cypher.internal.runtime.ast.MakeTraversable
import org.neo4j.cypher.internal.runtime.ast.RuntimeConstant
import org.neo4j.cypher.internal.util.EffectiveCardinality
import org.neo4j.cypher.internal.util.ExactSize
import org.neo4j.cypher.internal.util.LabelId
import org.neo4j.cypher.internal.util.NonEmptyList
import org.neo4j.cypher.internal.util.PropertyKeyId
import org.neo4j.cypher.internal.util.RelTypeId
import org.neo4j.cypher.internal.util.Repetition
import org.neo4j.cypher.internal.util.UpperBound.Unlimited
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.collection.immutable.ListSet
import org.neo4j.cypher.internal.util.symbols.BooleanType
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTFloat
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.CTList
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.graphdb.schema.IndexType
import org.neo4j.values.storable.Values.stringValue

import scala.collection.immutable

class QueryLogicalPlan2PlanDescriptionTest extends LogicalPlan2PlanDescriptionTestBase {

  private val lhsLP =
    attach(AllNodesScan(varFor("a"), Set.empty), EffectiveCardinality(2.0, Some(10.0)), ProvidedOrder.empty)

  private val lhsRelLP = attach(
    UndirectedAllRelationshipsScan(varFor("r"), varFor("a"), varFor("b"), Set.empty),
    EffectiveCardinality(2.0, Some(10.0)),
    ProvidedOrder.empty
  )

  private val lhsPD =
    PlanDescriptionImpl(id, "AllNodesScan", Seq.empty, Seq(details("a"), EstimatedRows(2, Some(10))), Set(pretty"a"))

  private val lhsRelPD = PlanDescriptionImpl(
    id,
    "UndirectedAllRelationshipsScan",
    Seq.empty,
    Seq(details("(a)-[r]-(b)"), EstimatedRows(2, Some(10))),
    Set(pretty"r", pretty"a", pretty"b")
  )

  private val rhsLP = attach(AllNodesScan(varFor("b"), Set.empty), 2.0, providedOrder = ProvidedOrder.empty)

  private val rhsPD =
    PlanDescriptionImpl(id, "AllNodesScan", Seq.empty, Seq(details("b"), EstimatedRows(2, Some(10))), Set(pretty"b"))

  test("Validate all arguments") {
    assertGood(
      attach(
        AllNodesScan(varFor("a"), Set.empty),
        EffectiveCardinality(1.0, Some(15.0)),
        DefaultProvidedOrderFactory.asc(varFor("a"))
      ),
      planDescription(
        id,
        "AllNodesScan",
        Seq.empty,
        Seq(
          details("a"),
          EstimatedRows(1, Some(15)),
          Order(asPrettyString.raw("a ASC")),
          Version("5"),
          RUNTIME_VERSION,
          Planner("COST"),
          PlannerImpl("IDP"),
          PLANNER_VERSION
        ),
        Set("a")
      ),
      validateAllArgs = true,
      cypherVersion = CypherVersion.Cypher5
    )

    assertGood(
      attach(
        AllNodesScan(varFor("  UNNAMED111"), Set.empty),
        EffectiveCardinality(1.0, Some(10.0)),
        DefaultProvidedOrderFactory.asc(varFor("  UNNAMED111"))
      ),
      planDescription(
        id,
        "AllNodesScan",
        Seq.empty,
        Seq(
          details(anonVar("111")),
          EstimatedRows(1, Some(10)),
          Order(asPrettyString.raw(s"${anonVar("111")} ASC")),
          Version("5"),
          RUNTIME_VERSION,
          Planner("COST"),
          PlannerImpl("IDP"),
          PLANNER_VERSION
        ),
        Set(anonVar("111"))
      ),
      validateAllArgs = true,
      cypherVersion = CypherVersion.Cypher5
    )

    assertGood(
      attach(
        Input(Seq(varFor("n1"), varFor("n2")), Seq(varFor("r")), Seq(varFor("v1"), varFor("v2")), nullable = false),
        EffectiveCardinality(42.3, Some(132))
      ),
      planDescription(
        id,
        "Input",
        Seq.empty,
        Seq(
          details(Seq("n1", "n2", "r", "v1", "v2")),
          EstimatedRows(42.3, Some(132)),
          Version("5"),
          RUNTIME_VERSION,
          Planner("COST"),
          PlannerImpl("IDP"),
          PLANNER_VERSION
        ),
        Set("n1", "n2", "r", "v1", "v2")
      ),
      validateAllArgs = true,
      cypherVersion = CypherVersion.Cypher5
    )
  }

  // Leaf Plans
  test("AllNodesScan") {
    assertGood(
      attach(AllNodesScan(varFor("a"), Set.empty), 1.0, providedOrder = DefaultProvidedOrderFactory.asc(varFor("a"))),
      planDescription(id, "AllNodesScan", Seq.empty, Seq(details("a"), Order(asPrettyString.raw("a ASC"))), Set("a"))
    )

    assertGood(
      attach(
        AllNodesScan(varFor("  UNNAMED111"), Set.empty),
        1.0,
        providedOrder = DefaultProvidedOrderFactory.asc(varFor("  UNNAMED111"))
      ),
      planDescription(
        id,
        "AllNodesScan",
        Seq.empty,
        Seq(details(anonVar("111")), Order(asPrettyString.raw(s"${anonVar("111")} ASC"))),
        Set(anonVar("111"))
      )
    )

    assertGood(
      attach(
        AllNodesScan(varFor("b"), Set.empty),
        42.0,
        providedOrder = DefaultProvidedOrderFactory.providedOrder(
          Seq(ColumnOrder.Asc(varFor("b")), ColumnOrder.Desc(prop("b", "foo"))),
          ProvidedOrder.Self,
          None
        )
      ),
      planDescription(
        id,
        "AllNodesScan",
        Seq.empty,
        Seq(details("b"), Order(asPrettyString.raw("b ASC, b.foo DESC"))),
        Set("b")
      )
    )
  }

  test("PartitionedAllNodesScan") {
    assertGood(
      attach(
        PartitionedAllNodesScan(varFor("a"), Set.empty),
        1.0,
        providedOrder = DefaultProvidedOrderFactory.asc(varFor("a"))
      ),
      planDescription(
        id,
        "PartitionedAllNodesScan",
        Seq.empty,
        Seq(details("a"), Order(asPrettyString.raw("a ASC"))),
        Set("a")
      )
    )

    assertGood(
      attach(
        PartitionedAllNodesScan(varFor("  UNNAMED111"), Set.empty),
        1.0,
        providedOrder = DefaultProvidedOrderFactory.asc(varFor("  UNNAMED111"))
      ),
      planDescription(
        id,
        "PartitionedAllNodesScan",
        Seq.empty,
        Seq(details(anonVar("111")), Order(asPrettyString.raw(s"${anonVar("111")} ASC"))),
        Set(anonVar("111"))
      )
    )

    assertGood(
      attach(
        PartitionedAllNodesScan(varFor("b"), Set.empty),
        42.0,
        providedOrder = DefaultProvidedOrderFactory.providedOrder(
          Seq(ColumnOrder.Asc(varFor("b")), ColumnOrder.Desc(prop("b", "foo"))),
          ProvidedOrder.Self,
          None
        )
      ),
      planDescription(
        id,
        "PartitionedAllNodesScan",
        Seq.empty,
        Seq(details("b"), Order(asPrettyString.raw("b ASC, b.foo DESC"))),
        Set("b")
      )
    )
  }

  test("NodeByLabelScan") {
    assertGood(
      attach(NodeByLabelScan(varFor("node"), label("X"), Set.empty, IndexOrderNone), 33.0),
      planDescription(id, "NodeByLabelScan", Seq.empty, Seq(details("node:X")), Set("node"))
    )

    assertGood(
      attach(NodeByLabelScan(varFor("  UNNAMED123"), label("X"), Set.empty, IndexOrderNone), 33.0),
      planDescription(id, "NodeByLabelScan", Seq.empty, Seq(details(s"${anonVar("123")}:X")), Set(anonVar("123")))
    )
  }

  test("DynamicLabelNodeLookup") {
    assertGood(
      attach(
        DynamicLabelNodeLookup(
          varFor("node"),
          DynamicElement.Simple(literal(List("A", "B")), DynamicElement.All),
          Set.empty,
          Map(
            PropertyKeyToken("prop", PropertyKeyId(0)) -> literal(1),
            PropertyKeyToken("foo", PropertyKeyId(0)) -> literal("bar")
          )
        ),
        33.0
      ),
      planDescription(
        id,
        "DynamicLabelNodeLookup",
        Seq.empty,
        Seq(details(
          """node:$all(["A", "B"])
            |node.prop = 1
            |node.foo = "bar"""".stripMargin
        )),
        Set("node")
      )
    )

    assertGood(
      attach(
        DynamicLabelNodeLookup(
          varFor("  UNNAMED123"),
          DynamicElement.Simple(varFor("y"), DynamicElement.Any),
          Set.empty,
          Map.empty
        ),
        33.0
      ),
      planDescription(
        id,
        "DynamicLabelNodeLookup",
        Seq.empty,
        Seq(details(s"${anonVar("123")}:$$any(y)")),
        Set(anonVar("123"))
      )
    )
  }

  test("PartitionedNodeByLabelScan") {
    assertGood(
      attach(PartitionedNodeByLabelScan(varFor("node"), label("X"), Set.empty), 33.0),
      planDescription(id, "PartitionedNodeByLabelScan", Seq.empty, Seq(details("node:X")), Set("node"))
    )

    assertGood(
      attach(PartitionedNodeByLabelScan(varFor("  UNNAMED123"), label("X"), Set.empty), 33.0),
      planDescription(
        id,
        "PartitionedNodeByLabelScan",
        Seq.empty,
        Seq(details(s"${anonVar("123")}:X")),
        Set(anonVar("123"))
      )
    )
  }

  test("UnionNodeByLabelScan") {
    assertGood(
      attach(
        UnionNodeByLabelsScan(varFor("node"), Seq(label("X"), label("Y"), label("Z")), Set.empty, IndexOrderNone),
        33.0
      ),
      planDescription(id, "UnionNodeByLabelsScan", Seq.empty, Seq(details("node:X|Y|Z")), Set("node"))
    )

    assertGood(
      attach(
        UnionNodeByLabelsScan(
          varFor("  UNNAMED123"),
          Seq(label("X"), label("Y"), label("Z")),
          Set.empty,
          IndexOrderNone
        ),
        33.0
      ),
      planDescription(
        id,
        "UnionNodeByLabelsScan",
        Seq.empty,
        Seq(details(s"${anonVar("123")}:X|Y|Z")),
        Set(anonVar("123"))
      )
    )
  }

  test("PartitionedUnionNodeByLabelScan") {
    assertGood(
      attach(
        PartitionedUnionNodeByLabelsScan(varFor("node"), Seq(label("X"), label("Y"), label("Z")), Set.empty),
        33.0
      ),
      planDescription(id, "PartitionedUnionNodeByLabelsScan", Seq.empty, Seq(details("node:X|Y|Z")), Set("node"))
    )

    assertGood(
      attach(
        PartitionedUnionNodeByLabelsScan(
          varFor("  UNNAMED123"),
          Seq(label("X"), label("Y"), label("Z")),
          Set.empty
        ),
        33.0
      ),
      planDescription(
        id,
        "PartitionedUnionNodeByLabelsScan",
        Seq.empty,
        Seq(details(s"${anonVar("123")}:X|Y|Z")),
        Set(anonVar("123"))
      )
    )
  }

  test("UnionRelationshipTypesScan") {
    assertGood(
      attach(
        DirectedUnionRelationshipTypesScan(
          varFor("r"),
          varFor("x"),
          Seq(relType("A"), relType("B"), relType("C")),
          varFor("y"),
          Set.empty,
          IndexOrderNone
        ),
        23.0
      ),
      planDescription(
        id,
        "DirectedUnionRelationshipTypesScan",
        Seq.empty,
        Seq(details("(x)-[r:A|B|C]->(y)")),
        Set("r", "x", "y")
      )
    )

    assertGood(
      attach(
        UndirectedUnionRelationshipTypesScan(
          varFor("r"),
          varFor("x"),
          Seq(relType("A"), relType("B"), relType("C")),
          varFor("y"),
          Set.empty,
          IndexOrderNone
        ),
        23.0
      ),
      planDescription(
        id,
        "UndirectedUnionRelationshipTypesScan",
        Seq.empty,
        Seq(details("(x)-[r:A|B|C]-(y)")),
        Set("r", "x", "y")
      )
    )
  }

  test("PartitionedUnionRelationshipTypesScan") {
    assertGood(
      attach(
        PartitionedDirectedUnionRelationshipTypesScan(
          Some(varFor("r")),
          Some(varFor("x")),
          Seq(relType("A"), relType("B"), relType("C")),
          Some(varFor("y")),
          Set.empty
        ),
        23.0
      ),
      planDescription(
        id,
        "PartitionedDirectedUnionRelationshipTypesScan",
        Seq.empty,
        Seq(details("(x)-[r:A|B|C]->(y)")),
        Set("r", "x", "y")
      )
    )

    assertGood(
      attach(
        PartitionedUndirectedUnionRelationshipTypesScan(
          Some(varFor("r")),
          Some(varFor("x")),
          Seq(relType("A"), relType("B"), relType("C")),
          Some(varFor("y")),
          Set.empty
        ),
        23.0
      ),
      planDescription(
        id,
        "PartitionedUndirectedUnionRelationshipTypesScan",
        Seq.empty,
        Seq(details("(x)-[r:A|B|C]-(y)")),
        Set("r", "x", "y")
      )
    )
  }

  test("IntersectionNodeByLabelScan") {
    assertGood(
      attach(
        IntersectionNodeByLabelsScan(
          varFor("node"),
          Seq(label("X"), label("Y"), label("Z")),
          Set.empty,
          IndexOrderNone
        ),
        33.0
      ),
      planDescription(id, "IntersectionNodeByLabelsScan", Seq.empty, Seq(details("node:X&Y&Z")), Set("node"))
    )

    assertGood(
      attach(
        IntersectionNodeByLabelsScan(
          varFor("  UNNAMED123"),
          Seq(label("X"), label("Y"), label("Z")),
          Set.empty,
          IndexOrderNone
        ),
        33.0
      ),
      planDescription(
        id,
        "IntersectionNodeByLabelsScan",
        Seq.empty,
        Seq(details(s"${anonVar("123")}:X&Y&Z")),
        Set(anonVar("123"))
      )
    )
  }

  test("PartitionedIntersectionNodeByLabelScan") {
    assertGood(
      attach(
        PartitionedIntersectionNodeByLabelsScan(
          varFor("node"),
          Seq(label("X"), label("Y"), label("Z")),
          Set.empty
        ),
        33.0
      ),
      planDescription(
        id,
        "PartitionedIntersectionNodeByLabelsScan",
        Seq.empty,
        Seq(details("node:X&Y&Z")),
        Set("node")
      )
    )

    assertGood(
      attach(
        PartitionedIntersectionNodeByLabelsScan(
          varFor("  UNNAMED123"),
          Seq(label("X"), label("Y"), label("Z")),
          Set.empty
        ),
        33.0
      ),
      planDescription(
        id,
        "PartitionedIntersectionNodeByLabelsScan",
        Seq.empty,
        Seq(details(s"${anonVar("123")}:X&Y&Z")),
        Set(anonVar("123"))
      )
    )
  }

  test("SubtractionNodeByLabelScan") {
    assertGood(
      attach(
        SubtractionNodeByLabelsScan(
          varFor("node"),
          Seq(label("A"), label("B"), label("C")),
          Seq(label("X"), label("Y"), label("Z")),
          Set.empty,
          IndexOrderNone
        ),
        33.0
      ),
      planDescription(id, "SubtractionNodeByLabelsScan", Seq.empty, Seq(details("node:A&B&C&!X&!Y&!Z")), Set("node"))
    )

    assertGood(
      attach(
        SubtractionNodeByLabelsScan(
          varFor("  UNNAMED123"),
          Seq(label("A"), label("B"), label("C")),
          Seq(label("X"), label("Y"), label("Z")),
          Set.empty,
          IndexOrderNone
        ),
        33.0
      ),
      planDescription(
        id,
        "SubtractionNodeByLabelsScan",
        Seq.empty,
        Seq(details(s"${anonVar("123")}:A&B&C&!X&!Y&!Z")),
        Set(anonVar("123"))
      )
    )
  }

  test("PartitionedSubtractionNodeByLabelScan") {
    assertGood(
      attach(
        PartitionedSubtractionNodeByLabelsScan(
          varFor("node"),
          Seq(label("A"), label("B"), label("C")),
          Seq(label("X"), label("Y"), label("Z")),
          Set.empty
        ),
        33.0
      ),
      planDescription(
        id,
        "PartitionedSubtractionNodeByLabelsScan",
        Seq.empty,
        Seq(details("node:A&B&C&!X&!Y&!Z")),
        Set("node")
      )
    )
  }

  test("NodeByIdSeek") {
    assertGood(
      attach(
        NodeByIdSeek(varFor("node"), ManySeekableArgs(ListLiteral(Seq(number("1"), number("32")))(pos)), Set.empty),
        333.0
      ),
      planDescription(id, "NodeByIdSeek", Seq.empty, Seq(details("node WHERE id(node) IN [1, 32]")), Set("node"))
    )

    assertGood(
      attach(
        NodeByIdSeek(
          varFor("node"),
          ManySeekableArgs(AutoExtractedParameter("autolist_0", CTList(CTAny))(pos)),
          Set.empty
        ),
        333.0
      ),
      planDescription(id, "NodeByIdSeek", Seq.empty, Seq(details("node WHERE id(node) IN $autolist_0")), Set("node"))
    )

    assertGood(
      attach(
        NodeByIdSeek(
          varFor("node"),
          ManySeekableArgs(ExplicitParameter("listParam", CTList(CTAny), ExactSize(5))(pos)),
          Set.empty
        ),
        333.0
      ),
      planDescription(id, "NodeByIdSeek", Seq.empty, Seq(details("node WHERE id(node) IN $listParam")), Set("node"))
    )

    assertGood(
      attach(
        NodeByIdSeek(
          varFor("node"),
          SingleSeekableArg(AutoExtractedParameter("autoint_0", CTInteger)(pos)),
          Set.empty
        ),
        333.0
      ),
      planDescription(id, "NodeByIdSeek", Seq.empty, Seq(details("node WHERE id(node) = $autoint_0")), Set("node"))
    )

    assertGood(
      attach(
        NodeByIdSeek(
          varFor("  UNNAMED11"),
          ManySeekableArgs(ListLiteral(Seq(number("1"), number("32")))(pos)),
          Set.empty
        ),
        333.0
      ),
      planDescription(
        id,
        "NodeByIdSeek",
        Seq.empty,
        Seq(details(s"${anonVar("11")} WHERE id(${anonVar("11")}) IN [1, 32]")),
        Set(anonVar("11"))
      )
    )
  }

  test("NodeByElementIdSeek") {
    assertGood(
      attach(
        NodeByElementIdSeek(varFor("node"), ManySeekableArgs(listOfString("some-id", "other-id")), Set.empty),
        333.0
      ),
      planDescription(
        id,
        "NodeByElementIdSeek",
        Seq.empty,
        Seq(details("node WHERE elementId(node) IN [\"some-id\", \"other-id\"]")),
        Set("node")
      )
    )

    assertGood(
      attach(
        NodeByElementIdSeek(
          varFor("node"),
          ManySeekableArgs(autoParameter("autolist_0", CTList(CTAny))),
          Set.empty
        ),
        333.0
      ),
      planDescription(
        id,
        "NodeByElementIdSeek",
        Seq.empty,
        Seq(details("node WHERE elementId(node) IN $autolist_0")),
        Set("node")
      )
    )

    assertGood(
      attach(
        NodeByElementIdSeek(varFor("node"), SingleSeekableArg(stringLiteral("some-id")), Set.empty),
        333.0
      ),
      planDescription(
        id,
        "NodeByElementIdSeek",
        Seq.empty,
        Seq(details("node WHERE elementId(node) = \"some-id\"")),
        Set("node")
      )
    )
  }

  test("NodeVectorIndexSearch") {
    assertGood(
      attach(
        NodeVectorIndexSearch(
          varFor("n"),
          Seq(LabelToken("Label", LabelId(0))),
          Seq(IndexedProperty(PropertyKeyToken("prop", PropertyKeyId(0)), DoNotGetValue, NODE_TYPE)),
          None,
          "vectorIndex",
          listOf(literalInt(1), literalInt(2)),
          literalInt(5),
          MatchAllQueryExpression,
          None,
          Set.empty
        ),
        23.0
      ),
      planDescription(
        id,
        "NodeVectorIndexSearch",
        Seq.empty,
        Seq(details("SEARCH n IN (VECTOR INDEX vectorIndex FOR [1, 2] LIMIT 5)")),
        Set("n")
      )
    )

    assertGood(
      attach(
        NodeVectorIndexSearch(
          varFor("n"),
          Seq(
            LabelToken("Label1", LabelId(0)),
            LabelToken("Label2", LabelId(1))
          ),
          properties = Seq(
            // the vector property
            IndexedProperty(PropertyKeyToken("prop0", PropertyKeyId(0)), DoNotGetValue, NODE_TYPE),
            // additional properties
            IndexedProperty(PropertyKeyToken("prop1", PropertyKeyId(1)), DoNotGetValue, NODE_TYPE),
            IndexedProperty(PropertyKeyToken("prop2", PropertyKeyId(2)), DoNotGetValue, NODE_TYPE)
          ),
          score = Some(v"sc"),
          indexName = "vectorIndex",
          vector = listOf(literalInt(1), literalInt(2)),
          limit = literalInt(5),
          MatchAllQueryExpression,
          maybePropertyFilter = Some(CompositeQueryExpression(
            Seq(
              SingleQueryExpression(literalInt(42)),
              RangeQueryExpression(
                InequalitySeekRangeWrapper(RangeGreaterThan(NonEmptyList(ExclusiveBound(literalInt(42)))))(pos)
              )
            )
          )),
          argumentIds = Set.empty
        ),
        23.0
      ),
      planDescription(
        id,
        "NodeVectorIndexSearch",
        Seq.empty,
        Seq(details(
          "SEARCH n IN (VECTOR INDEX vectorIndex FOR [1, 2] WHERE prop1 = 42 AND prop2 > 42 LIMIT 5) SCORE AS sc"
        )),
        Set("n", "sc")
      )
    )

    assertGood(
      attach(
        NodeVectorIndexSearch(
          varFor("n"),
          Seq(
            LabelToken("Label1", LabelId(0)),
            LabelToken("Label2", LabelId(1))
          ),
          properties = Seq(
            // the vector property
            IndexedProperty(PropertyKeyToken("prop0", PropertyKeyId(0)), DoNotGetValue, NODE_TYPE),
            // additional properties
            IndexedProperty(PropertyKeyToken("prop1", PropertyKeyId(1)), DoNotGetValue, NODE_TYPE),
            IndexedProperty(PropertyKeyToken("prop2", PropertyKeyId(2)), DoNotGetValue, NODE_TYPE)
          ),
          score = Some(v"sc"),
          indexName = "vectorIndex",
          vector = listOf(literalInt(1), literalInt(2)),
          limit = literalInt(5),
          MatchAllQueryExpression,
          maybePropertyFilter = Some(CompositeQueryExpression(
            Seq(
              ExistenceQueryExpression,
              NonExistenceQueryExpression
            )
          )),
          argumentIds = Set.empty
        ),
        23.0
      ),
      planDescription(
        id,
        "NodeVectorIndexSearch",
        Seq.empty,
        Seq(details(
          "SEARCH n IN (VECTOR INDEX vectorIndex FOR [1, 2] WHERE prop1 IS NOT NULL AND prop2 IS NULL LIMIT 5) SCORE AS sc"
        )),
        Set("n", "sc")
      )
    )
  }

  test("DirectedRelationshipVectorIndexSearch") {
    assertGood(
      attach(
        DirectedRelationshipVectorIndexSearch(
          Some(varFor("r")),
          Some(varFor("a")),
          Some(varFor("b")),
          Seq(RelationshipTypeToken("R1", RelTypeId(0)), RelationshipTypeToken("R2", RelTypeId(1))),
          Seq(
            IndexedProperty(PropertyKeyToken("p1", PropertyKeyId(0)), DoNotGetValue, RELATIONSHIP_TYPE),
            IndexedProperty(PropertyKeyToken("p2", PropertyKeyId(0)), DoNotGetValue, RELATIONSHIP_TYPE)
          ),
          None,
          "vectorIndex",
          listOf(literalInt(1), literalInt(2)),
          literalInt(5),
          MatchAllQueryExpression,
          None,
          Set.empty
        ),
        23.0
      ),
      planDescription(
        id,
        "DirectedRelationshipVectorIndexSearch",
        Seq.empty,
        Seq(details("SEARCH (a)-[r:R1|R2]->(b) IN (VECTOR INDEX vectorIndex FOR [1, 2] LIMIT 5)")),
        Set("r", "a", "b")
      )
    )

    assertGood(
      attach(
        DirectedRelationshipVectorIndexSearch(
          Some(varFor("r")),
          Some(varFor("a")),
          Some(varFor("b")),
          Seq(RelationshipTypeToken("R1", RelTypeId(0)), RelationshipTypeToken("R2", RelTypeId(1))),
          Seq(
            IndexedProperty(PropertyKeyToken("p0", PropertyKeyId(0)), DoNotGetValue, RELATIONSHIP_TYPE),
            IndexedProperty(PropertyKeyToken("p1", PropertyKeyId(1)), DoNotGetValue, RELATIONSHIP_TYPE),
            IndexedProperty(PropertyKeyToken("p2", PropertyKeyId(2)), DoNotGetValue, RELATIONSHIP_TYPE)
          ),
          Some(v"sc"),
          "vectorIndex",
          listOf(literalInt(1), literalInt(2)),
          literalInt(5),
          MatchAllQueryExpression,
          Some(CompositeQueryExpression(
            Seq(
              SingleQueryExpression(literalInt(42)),
              RangeQueryExpression(
                InequalitySeekRangeWrapper(RangeGreaterThan(NonEmptyList(ExclusiveBound(literalInt(42)))))(pos)
              )
            )
          )),
          Set.empty
        ),
        23.0
      ),
      planDescription(
        id,
        "DirectedRelationshipVectorIndexSearch",
        Seq.empty,
        Seq(
          details(
            "SEARCH (a)-[r:R1|R2]->(b) IN (VECTOR INDEX vectorIndex FOR [1, 2] WHERE p1 = 42 AND p2 > 42 LIMIT 5) SCORE AS sc"
          )
        ),
        Set("r", "a", "b", "sc")
      )
    )

    assertGood(
      attach(
        DirectedRelationshipVectorIndexSearch(
          Some(varFor("r")),
          Some(varFor("a")),
          Some(varFor("b")),
          Seq(RelationshipTypeToken("R1", RelTypeId(0)), RelationshipTypeToken("R2", RelTypeId(1))),
          Seq(
            IndexedProperty(PropertyKeyToken("p0", PropertyKeyId(0)), DoNotGetValue, RELATIONSHIP_TYPE),
            IndexedProperty(PropertyKeyToken("p1", PropertyKeyId(1)), DoNotGetValue, RELATIONSHIP_TYPE),
            IndexedProperty(PropertyKeyToken("p2", PropertyKeyId(2)), DoNotGetValue, RELATIONSHIP_TYPE)
          ),
          Some(v"sc"),
          "vectorIndex",
          listOf(literalInt(1), literalInt(2)),
          literalInt(5),
          MatchAllQueryExpression,
          Some(CompositeQueryExpression(
            Seq(
              NonExistenceQueryExpression,
              ExistenceQueryExpression
            )
          )),
          Set.empty
        ),
        23.0
      ),
      planDescription(
        id,
        "DirectedRelationshipVectorIndexSearch",
        Seq.empty,
        Seq(
          details(
            "SEARCH (a)-[r:R1|R2]->(b) IN (VECTOR INDEX vectorIndex FOR [1, 2] WHERE p1 IS NULL AND p2 IS NOT NULL LIMIT 5) SCORE AS sc"
          )
        ),
        Set("r", "a", "b", "sc")
      )
    )
  }

  test("UndirectedRelationshipVectorIndexSearch") {
    assertGood(
      attach(
        UndirectedRelationshipVectorIndexSearch(
          Some(varFor("r")),
          Some(varFor("a")),
          Some(varFor("b")),
          Seq(RelationshipTypeToken("R1", RelTypeId(0)), RelationshipTypeToken("R2", RelTypeId(1))),
          Seq(
            IndexedProperty(PropertyKeyToken("p1", PropertyKeyId(0)), DoNotGetValue, RELATIONSHIP_TYPE),
            IndexedProperty(PropertyKeyToken("p2", PropertyKeyId(0)), DoNotGetValue, RELATIONSHIP_TYPE)
          ),
          None,
          "vectorIndex",
          listOf(literalInt(1), literalInt(2)),
          literalInt(5),
          MatchAllQueryExpression,
          None,
          Set.empty
        ),
        23.0
      ),
      planDescription(
        id,
        "UndirectedRelationshipVectorIndexSearch",
        Seq.empty,
        Seq(details("SEARCH (a)-[r:R1|R2]-(b) IN (VECTOR INDEX vectorIndex FOR [1, 2] LIMIT 5)")),
        Set("r", "a", "b")
      )
    )

    assertGood(
      attach(
        UndirectedRelationshipVectorIndexSearch(
          Some(varFor("r")),
          Some(varFor("a")),
          Some(varFor("b")),
          Seq(RelationshipTypeToken("R1", RelTypeId(0)), RelationshipTypeToken("R2", RelTypeId(1))),
          Seq(
            IndexedProperty(PropertyKeyToken("p0", PropertyKeyId(0)), DoNotGetValue, RELATIONSHIP_TYPE),
            IndexedProperty(PropertyKeyToken("p1", PropertyKeyId(1)), DoNotGetValue, RELATIONSHIP_TYPE),
            IndexedProperty(PropertyKeyToken("p2", PropertyKeyId(2)), DoNotGetValue, RELATIONSHIP_TYPE)
          ),
          Some(v"sc"),
          "vectorIndex",
          listOf(literalInt(1), literalInt(2)),
          literalInt(5),
          MatchAllQueryExpression,
          Some(CompositeQueryExpression(
            Seq(
              SingleQueryExpression(literalInt(42)),
              RangeQueryExpression(
                InequalitySeekRangeWrapper(RangeGreaterThan(NonEmptyList(ExclusiveBound(literalInt(42)))))(pos)
              )
            )
          )),
          Set.empty
        ),
        23.0
      ),
      planDescription(
        id,
        "UndirectedRelationshipVectorIndexSearch",
        Seq.empty,
        Seq(
          details(
            "SEARCH (a)-[r:R1|R2]-(b) IN (VECTOR INDEX vectorIndex FOR [1, 2] WHERE p1 = 42 AND p2 > 42 LIMIT 5) SCORE AS sc"
          )
        ),
        Set("r", "a", "b", "sc")
      )
    )
    assertGood(
      attach(
        UndirectedRelationshipVectorIndexSearch(
          Some(varFor("r")),
          Some(varFor("a")),
          Some(varFor("b")),
          Seq(RelationshipTypeToken("R1", RelTypeId(0)), RelationshipTypeToken("R2", RelTypeId(1))),
          Seq(
            IndexedProperty(PropertyKeyToken("p0", PropertyKeyId(0)), DoNotGetValue, RELATIONSHIP_TYPE),
            IndexedProperty(PropertyKeyToken("p1", PropertyKeyId(1)), DoNotGetValue, RELATIONSHIP_TYPE),
            IndexedProperty(PropertyKeyToken("p2", PropertyKeyId(2)), DoNotGetValue, RELATIONSHIP_TYPE)
          ),
          Some(v"sc"),
          "vectorIndex",
          listOf(literalInt(1), literalInt(2)),
          literalInt(5),
          MatchAllQueryExpression,
          Some(CompositeQueryExpression(
            Seq(
              ExistenceQueryExpression,
              NonExistenceQueryExpression
            )
          )),
          Set.empty
        ),
        23.0
      ),
      planDescription(
        id,
        "UndirectedRelationshipVectorIndexSearch",
        Seq.empty,
        Seq(
          details(
            "SEARCH (a)-[r:R1|R2]-(b) IN (VECTOR INDEX vectorIndex FOR [1, 2] WHERE p1 IS NOT NULL AND p2 IS NULL LIMIT 5) SCORE AS sc"
          )
        ),
        Set("r", "a", "b", "sc")
      )
    )
  }

  test("NodeFulltextIndexSearch") {
    assertGood(
      attach(
        NodeFulltextIndexSearch(
          idName = varFor("n"),
          labels = Seq(LabelToken("Label", LabelId(0))),
          properties = Seq(IndexedProperty(PropertyKeyToken("prop", PropertyKeyId(0)), DoNotGetValue, NODE_TYPE)),
          score = None,
          indexName = "fulltextIndex",
          queryString = literalString("hello"),
          analyzer = None,
          skip = None,
          limit = literalInt(5),
          argumentIds = Set.empty
        ),
        23.0
      ),
      planDescription(
        id,
        "NodeFulltextIndexSearch",
        Seq.empty,
        Seq(details("SEARCH n IN (FULLTEXT INDEX fulltextIndex FOR \"hello\" LIMIT 5)")),
        Set("n")
      )
    )

    assertGood(
      attach(
        NodeFulltextIndexSearch(
          idName = varFor("n"),
          labels = Seq(LabelToken("Label", LabelId(0))),
          properties = Seq(IndexedProperty(PropertyKeyToken("prop", PropertyKeyId(0)), DoNotGetValue, NODE_TYPE)),
          score = Some(v"sc"),
          indexName = "fulltextIndex",
          queryString = literalString("hello"),
          analyzer = Some(literalString("english")),
          skip = Some(literalInt(2)),
          limit = literalInt(5),
          argumentIds = Set.empty
        ),
        23.0
      ),
      planDescription(
        id,
        "NodeFulltextIndexSearch",
        Seq.empty,
        Seq(details(
          "SEARCH n IN (FULLTEXT INDEX fulltextIndex FOR \"hello\" WITH ANALYZER \"english\" SKIP 2 LIMIT 5) SCORE AS sc"
        )),
        Set("n", "sc")
      )
    )
  }

  test("DirectedRelationshipFulltextIndexSearch") {
    assertGood(
      attach(
        DirectedRelationshipFulltextIndexSearch(
          idName = Some(varFor("r")),
          startNode = Some(varFor("a")),
          endNode = Some(varFor("b")),
          typeTokens = Seq(RelationshipTypeToken("R1", RelTypeId(0)), RelationshipTypeToken("R2", RelTypeId(1))),
          properties = Seq(IndexedProperty(PropertyKeyToken("p1", PropertyKeyId(0)), DoNotGetValue, RELATIONSHIP_TYPE)),
          score = None,
          indexName = "fulltextIndex",
          queryString = literalString("hello"),
          limit = literalInt(5),
          analyzer = None,
          skip = None,
          argumentIds = Set.empty
        ),
        23.0
      ),
      planDescription(
        id,
        "DirectedRelationshipFulltextIndexSearch",
        Seq.empty,
        Seq(details("SEARCH (a)-[r:R1|R2]->(b) IN (FULLTEXT INDEX fulltextIndex FOR \"hello\" LIMIT 5)")),
        Set("r", "a", "b")
      )
    )

    assertGood(
      attach(
        DirectedRelationshipFulltextIndexSearch(
          idName = Some(varFor("r")),
          startNode = Some(varFor("a")),
          endNode = Some(varFor("b")),
          typeTokens = Seq(RelationshipTypeToken("R1", RelTypeId(0)), RelationshipTypeToken("R2", RelTypeId(1))),
          properties = Seq(IndexedProperty(PropertyKeyToken("p1", PropertyKeyId(0)), DoNotGetValue, RELATIONSHIP_TYPE)),
          score = Some(v"sc"),
          indexName = "fulltextIndex",
          queryString = literalString("hello"),
          limit = literalInt(5),
          analyzer = Some(literalString("english")),
          skip = Some(literalInt(2)),
          argumentIds = Set.empty
        ),
        23.0
      ),
      planDescription(
        id,
        "DirectedRelationshipFulltextIndexSearch",
        Seq.empty,
        Seq(details(
          "SEARCH (a)-[r:R1|R2]->(b) IN (FULLTEXT INDEX fulltextIndex FOR \"hello\" WITH ANALYZER \"english\" SKIP 2 LIMIT 5) SCORE AS sc"
        )),
        Set("r", "a", "b", "sc")
      )
    )
  }

  test("UndirectedRelationshipFulltextIndexSearch") {
    assertGood(
      attach(
        UndirectedRelationshipFulltextIndexSearch(
          idName = Some(varFor("r")),
          startNode = Some(varFor("a")),
          endNode = Some(varFor("b")),
          typeTokens = Seq(RelationshipTypeToken("R1", RelTypeId(0)), RelationshipTypeToken("R2", RelTypeId(1))),
          properties = Seq(IndexedProperty(PropertyKeyToken("p1", PropertyKeyId(0)), DoNotGetValue, RELATIONSHIP_TYPE)),
          score = None,
          indexName = "fulltextIndex",
          queryString = literalString("hello"),
          limit = literalInt(5),
          analyzer = None,
          skip = None,
          argumentIds = Set.empty
        ),
        23.0
      ),
      planDescription(
        id,
        "UndirectedRelationshipFulltextIndexSearch",
        Seq.empty,
        Seq(details("SEARCH (a)-[r:R1|R2]-(b) IN (FULLTEXT INDEX fulltextIndex FOR \"hello\" LIMIT 5)")),
        Set("r", "a", "b")
      )
    )
  }

  test("NodeIndexSeek") {
    assertGood(
      attach(nodeIndexSeek("x:Label(Prop)"), 23.0),
      planDescription(
        id,
        "NodeIndexScan",
        Seq.empty,
        Seq(details("RANGE INDEX x:Label(Prop) WHERE Prop IS NOT NULL")),
        Set("x")
      )
    )

    assertGood(
      attach(nodeIndexSeek("x:Label(Prop)"), 23.0),
      planDescription(
        id,
        "NodeIndexScan",
        Seq.empty,
        Seq(details("RANGE INDEX x:Label(Prop) WHERE Prop IS NOT NULL")),
        Set("x")
      )
    )

    assertGood(
      attach(nodeIndexSeek("x:Label(Prop)", getValue = _ => GetValue), 23.0),
      planDescription(
        id,
        "NodeIndexScan",
        Seq.empty,
        Seq(details("RANGE INDEX x:Label(Prop) WHERE Prop IS NOT NULL, cache[x.Prop]")),
        Set("x")
      )
    )

    assertGood(
      attach(nodeIndexSeek("x:Label(Prop,Foo)"), 23.0),
      planDescription(
        id,
        "NodeIndexScan",
        Seq.empty,
        Seq(details("RANGE INDEX x:Label(Prop, Foo) WHERE Prop IS NOT NULL AND Foo IS NOT NULL")),
        Set("x")
      )
    )

    assertGood(
      attach(nodeIndexSeek("x:Label(Prop,Foo)", getValue = _ => GetValue), 23.0),
      planDescription(
        id,
        "NodeIndexScan",
        Seq.empty,
        Seq(details(
          "RANGE INDEX x:Label(Prop, Foo) WHERE Prop IS NOT NULL AND Foo IS NOT NULL, cache[x.Prop], cache[x.Foo]"
        )),
        Set("x")
      )
    )

    assertGood(
      attach(nodeIndexSeek("x:Label(Prop = 'Andres')"), 23.0),
      planDescription(
        id,
        "NodeIndexSeek",
        Seq.empty,
        Seq(details("RANGE INDEX x:Label(Prop) WHERE Prop = \"Andres\"")),
        Set("x")
      )
    )

    assertGood(
      attach(nodeIndexSeek("x:Label(Prop = 'Andres')", getValue = _ => GetValue), 23.0),
      planDescription(
        id,
        "NodeIndexSeek",
        Seq.empty,
        Seq(details("RANGE INDEX x:Label(Prop) WHERE Prop = \"Andres\", cache[x.Prop]")),
        Set("x")
      )
    )

    assertGood(
      attach(nodeIndexSeek("x:Label(Prop = 'Andres')", unique = true), 23.0),
      planDescription(
        id,
        "NodeUniqueIndexSeek",
        Seq.empty,
        Seq(details("UNIQUE x:Label(Prop) WHERE Prop = \"Andres\"")),
        Set("x")
      )
    )

    assertGood(
      attach(nodeIndexSeek("x:Label(Prop = 'Andres' OR 'Pontus')"), 23.0),
      planDescription(
        id,
        "NodeIndexSeek",
        Seq.empty,
        Seq(details("RANGE INDEX x:Label(Prop) WHERE Prop IN [\"Andres\", \"Pontus\"]")),
        Set("x")
      )
    )

    assertGood(
      attach(nodeIndexSeek("x:Label(Prop = 'Andres' OR 'Pontus')", unique = true), 23.0),
      planDescription(
        id,
        "NodeUniqueIndexSeek",
        Seq.empty,
        Seq(details("UNIQUE x:Label(Prop) WHERE Prop IN [\"Andres\", \"Pontus\"]")),
        Set("x")
      )
    )

    assertGood(
      attach(nodeIndexSeek("x:Label(Prop > 9)"), 23.0),
      planDescription(
        id,
        "NodeIndexSeekByRange",
        Seq.empty,
        Seq(details("RANGE INDEX x:Label(Prop) WHERE Prop > 9")),
        Set("x")
      )
    )

    assertGood(
      attach(nodeIndexSeek("x:Label(Prop < 9)"), 23.0),
      planDescription(
        id,
        "NodeIndexSeekByRange",
        Seq.empty,
        Seq(details("RANGE INDEX x:Label(Prop) WHERE Prop < 9")),
        Set("x")
      )
    )

    assertGood(
      attach(nodeIndexSeek("x:Label(9 <= Prop <= 11)"), 23.0),
      planDescription(
        id,
        "NodeIndexSeekByRange",
        Seq.empty,
        Seq(details("RANGE INDEX x:Label(Prop) WHERE Prop >= 9 AND Prop <= 11")),
        Set("x")
      )
    )

    assertGood(
      attach(nodeIndexSeek("x:Label(Prop STARTS WITH 'Foo')"), 23.0),
      planDescription(
        id,
        "NodeIndexSeekByRange",
        Seq.empty,
        Seq(details("RANGE INDEX x:Label(Prop) WHERE Prop STARTS WITH \"Foo\"")),
        Set("x")
      )
    )

    assertGood(
      attach(nodeIndexSeek("x:Label(Prop STARTS WITH 'Foo')", indexType = IndexType.TEXT), 23.0),
      planDescription(
        id,
        "NodeIndexSeekByRange",
        Seq.empty,
        Seq(details("TEXT INDEX x:Label(Prop) WHERE Prop STARTS WITH \"Foo\"")),
        Set("x")
      )
    )

    assertGood(
      attach(nodeIndexSeek("x:Label(Prop STARTS WITH 'Foo')", indexType = IndexType.FULLTEXT), 23.0),
      planDescription(
        id,
        "NodeIndexSeekByRange",
        Seq.empty,
        Seq(details("FULLTEXT INDEX x:Label(Prop) WHERE Prop STARTS WITH \"Foo\"")),
        Set("x")
      )
    )

    assertGood(
      attach(nodeIndexSeek("x:Label(Prop ENDS WITH 'Foo')"), 23.0),
      planDescription(
        id,
        "NodeIndexEndsWithScan",
        Seq.empty,
        Seq(details("RANGE INDEX x:Label(Prop) WHERE Prop ENDS WITH \"Foo\"")),
        Set("x")
      )
    )

    assertGood(
      attach(nodeIndexSeek("x:Label(Prop CONTAINS 'Foo')"), 23.0),
      planDescription(
        id,
        "NodeIndexContainsScan",
        Seq.empty,
        Seq(details("RANGE INDEX x:Label(Prop) WHERE Prop CONTAINS \"Foo\"")),
        Set("x")
      )
    )

    assertGood(
      attach(nodeIndexSeek("x:Label(Prop = 10,Foo = 12)"), 23.0),
      planDescription(
        id,
        "NodeIndexSeek",
        Seq.empty,
        Seq(details("RANGE INDEX x:Label(Prop, Foo) WHERE Prop = 10 AND Foo = 12")),
        Set("x")
      )
    )

    assertGood(
      attach(nodeIndexSeek("x:Label(Prop = 10,Foo = 12)", unique = true), 23.0),
      planDescription(
        id,
        "NodeUniqueIndexSeek",
        Seq.empty,
        Seq(details("UNIQUE x:Label(Prop, Foo) WHERE Prop = 10 AND Foo = 12")),
        Set("x")
      )
    )

    assertGood(
      attach(nodeIndexSeek("x:Label(Prop > 10,Foo)", indexType = IndexType.RANGE), 23.0),
      planDescription(
        id,
        "NodeIndexSeek",
        Seq.empty,
        Seq(details("RANGE INDEX x:Label(Prop, Foo) WHERE Prop > 10 AND Foo IS NOT NULL")),
        Set("x")
      )
    )

    // This is ManyQueryExpression with only a single expression. That is possible to get, but the test utility IndexSeek cannot create those.
    assertGood(
      attach(
        NodeUniqueIndexSeek(
          varFor("x"),
          LabelToken("Label", LabelId(0)),
          Seq(IndexedProperty(PropertyKeyToken("Prop", PropertyKeyId(0)), DoNotGetValue, NODE_TYPE)),
          ManyQueryExpression(ListLiteral(Seq(stringLiteral("Andres")))(pos)),
          Set.empty,
          IndexOrderNone,
          IndexType.RANGE,
          supportPartitionedScan = false
        ),
        95.0
      ),
      planDescription(
        id,
        "NodeUniqueIndexSeek",
        Seq.empty,
        Seq(details("UNIQUE x:Label(Prop) WHERE Prop = \"Andres\"")),
        Set("x")
      )
    )

    assertGood(
      attach(
        NodeIndexSeek(
          varFor("x"),
          LabelToken("Label", LabelId(0)),
          Seq(IndexedProperty(PropertyKeyToken("Prop", PropertyKeyId(0)), DoNotGetValue, NODE_TYPE)),
          RangeQueryExpression(PointDistanceSeekRangeWrapper(PointDistanceRange(
            FunctionInvocation(
              MapExpression(Seq(
                (key("x"), number("1")),
                (key("y"), number("2")),
                (key("crs"), stringLiteral("cartesian"))
              ))(pos),
              functionName(Point.name)
            ),
            number("10"),
            inclusive = true
          ))(pos)),
          Set.empty,
          IndexOrderNone,
          IndexType.POINT,
          supportPartitionedScan = false
        ),
        95.0
      ),
      planDescription(
        id,
        "NodeIndexSeekByRange",
        Seq.empty,
        Seq(details("POINT INDEX x:Label(Prop) WHERE point.distance(Prop, point(1, 2, \"cartesian\")) <= 10")),
        Set("x")
      )
    )

    assertGood(
      attach(
        NodeIndexSeek(
          varFor("x"),
          LabelToken("Label", LabelId(0)),
          Seq(IndexedProperty(PropertyKeyToken("Prop", PropertyKeyId(0)), DoNotGetValue, NODE_TYPE)),
          RangeQueryExpression(PointBoundingBoxSeekRangeWrapper(PointBoundingBoxRange(
            FunctionInvocation(
              MapExpression(Seq(
                (key("x"), number("0")),
                (key("y"), number("0")),
                (key("crs"), stringLiteral("cartesian"))
              ))(pos),
              functionName(Point.name)
            ),
            FunctionInvocation(
              MapExpression(Seq(
                (key("x"), number("10")),
                (key("y"), number("10")),
                (key("crs"), stringLiteral("cartesian"))
              ))(pos),
              functionName(Point.name)
            )
          ))(pos)),
          Set.empty,
          IndexOrderNone,
          IndexType.RANGE,
          supportPartitionedScan = true
        ),
        95.0
      ),
      planDescription(
        id,
        "NodeIndexSeekByRange",
        Seq.empty,
        Seq(details(
          "RANGE INDEX x:Label(Prop) WHERE point.withinBBox(Prop, point(0, 0, \"cartesian\"), point(10, 10, \"cartesian\"))"
        )),
        Set("x")
      )
    )
  }

  test("RemoteNodeIndexSeek") {
    assertGood(
      attach(remoteNodeIndexSeek("x:Label(Prop = 'Andres')"), 23.0),
      planDescription(
        id,
        "RemoteNodeIndexSeek",
        Seq.empty,
        Seq(details("RANGE INDEX x:Label(Prop) WHERE Prop = \"Andres\"")),
        Set("x")
      )
    )

    assertGood(
      attach(remoteNodeIndexSeek("x:Label(Prop = 'Andres')", getValue = _ => GetValue), 23.0),
      planDescription(
        id,
        "RemoteNodeIndexSeek",
        Seq.empty,
        Seq(details("RANGE INDEX x:Label(Prop) WHERE Prop = \"Andres\", cache[x.Prop]")),
        Set("x")
      )
    )

    assertGood(
      attach(remoteNodeIndexSeek("x:Label(Prop = 'Andres' OR 'Pontus')"), 23.0),
      planDescription(
        id,
        "RemoteNodeIndexSeek",
        Seq.empty,
        Seq(details("RANGE INDEX x:Label(Prop) WHERE Prop IN [\"Andres\", \"Pontus\"]")),
        Set("x")
      )
    )

    assertGood(
      attach(remoteNodeIndexSeek("x:Label(Prop > 9)"), 23.0),
      planDescription(
        id,
        "RemoteNodeIndexSeekByRange",
        Seq.empty,
        Seq(details("RANGE INDEX x:Label(Prop) WHERE Prop > 9")),
        Set("x")
      )
    )

    assertGood(
      attach(remoteNodeIndexSeek("x:Label(Prop < 9)"), 23.0),
      planDescription(
        id,
        "RemoteNodeIndexSeekByRange",
        Seq.empty,
        Seq(details("RANGE INDEX x:Label(Prop) WHERE Prop < 9")),
        Set("x")
      )
    )

    assertGood(
      attach(remoteNodeIndexSeek("x:Label(9 <= Prop <= 11)"), 23.0),
      planDescription(
        id,
        "RemoteNodeIndexSeekByRange",
        Seq.empty,
        Seq(details("RANGE INDEX x:Label(Prop) WHERE Prop >= 9 AND Prop <= 11")),
        Set("x")
      )
    )

    assertGood(
      attach(remoteNodeIndexSeek("x:Label(Prop = 'Andres')", unique = true), 23.0),
      planDescription(
        id,
        "RemoteNodeUniqueIndexSeek",
        Seq.empty,
        Seq(details("UNIQUE x:Label(Prop) WHERE Prop = \"Andres\"")),
        Set("x")
      )
    )

    assertGood(
      attach(remoteNodeIndexSeek("x:Label(Prop = 'Andres')", getValue = _ => GetValue, unique = true), 23.0),
      planDescription(
        id,
        "RemoteNodeUniqueIndexSeek",
        Seq.empty,
        Seq(details("UNIQUE x:Label(Prop) WHERE Prop = \"Andres\", cache[x.Prop]")),
        Set("x")
      )
    )

    assertGood(
      attach(remoteNodeIndexSeek("x:Label(Prop = 'Andres' OR 'Pontus')", unique = true), 23.0),
      planDescription(
        id,
        "RemoteNodeUniqueIndexSeek",
        Seq.empty,
        Seq(details("UNIQUE x:Label(Prop) WHERE Prop IN [\"Andres\", \"Pontus\"]")),
        Set("x")
      )
    )
  }

  test("PartitionedNodeIndexSeek") {
    assertGood(
      attach(partitionedNodeIndexSeek("x:Label(Prop)"), 23.0),
      planDescription(
        id,
        "PartitionedNodeIndexScan",
        Seq.empty,
        Seq(details("RANGE INDEX x:Label(Prop) WHERE Prop IS NOT NULL")),
        Set("x")
      )
    )

    assertGood(
      attach(partitionedNodeIndexSeek("x:Label(Prop)"), 23.0),
      planDescription(
        id,
        "PartitionedNodeIndexScan",
        Seq.empty,
        Seq(details("RANGE INDEX x:Label(Prop) WHERE Prop IS NOT NULL")),
        Set("x")
      )
    )

    assertGood(
      attach(partitionedNodeIndexSeek("x:Label(Prop)", getValue = _ => GetValue), 23.0),
      planDescription(
        id,
        "PartitionedNodeIndexScan",
        Seq.empty,
        Seq(details("RANGE INDEX x:Label(Prop) WHERE Prop IS NOT NULL, cache[x.Prop]")),
        Set("x")
      )
    )

    assertGood(
      attach(partitionedNodeIndexSeek("x:Label(Prop,Foo)"), 23.0),
      planDescription(
        id,
        "PartitionedNodeIndexScan",
        Seq.empty,
        Seq(details("RANGE INDEX x:Label(Prop, Foo) WHERE Prop IS NOT NULL AND Foo IS NOT NULL")),
        Set("x")
      )
    )

    assertGood(
      attach(partitionedNodeIndexSeek("x:Label(Prop,Foo)", getValue = _ => GetValue), 23.0),
      planDescription(
        id,
        "PartitionedNodeIndexScan",
        Seq.empty,
        Seq(details(
          "RANGE INDEX x:Label(Prop, Foo) WHERE Prop IS NOT NULL AND Foo IS NOT NULL, cache[x.Prop], cache[x.Foo]"
        )),
        Set("x")
      )
    )

    assertGood(
      attach(partitionedNodeIndexSeek("x:Label(Prop = 'Andres')"), 23.0),
      planDescription(
        id,
        "PartitionedNodeIndexSeek",
        Seq.empty,
        Seq(details("RANGE INDEX x:Label(Prop) WHERE Prop = \"Andres\"")),
        Set("x")
      )
    )

    assertGood(
      attach(partitionedNodeIndexSeek("x:Label(Prop = 'Andres')", getValue = _ => GetValue), 23.0),
      planDescription(
        id,
        "PartitionedNodeIndexSeek",
        Seq.empty,
        Seq(details("RANGE INDEX x:Label(Prop) WHERE Prop = \"Andres\", cache[x.Prop]")),
        Set("x")
      )
    )

    assertGood(
      attach(partitionedNodeIndexSeek("x:Label(Prop = 'Andres' OR 'Pontus')"), 23.0),
      planDescription(
        id,
        "PartitionedNodeIndexSeek",
        Seq.empty,
        Seq(details("RANGE INDEX x:Label(Prop) WHERE Prop IN [\"Andres\", \"Pontus\"]")),
        Set("x")
      )
    )

    assertGood(
      attach(partitionedNodeIndexSeek("x:Label(Prop > 9)"), 23.0),
      planDescription(
        id,
        "PartitionedNodeIndexSeekByRange",
        Seq.empty,
        Seq(details("RANGE INDEX x:Label(Prop) WHERE Prop > 9")),
        Set("x")
      )
    )

    assertGood(
      attach(partitionedNodeIndexSeek("x:Label(Prop < 9)"), 23.0),
      planDescription(
        id,
        "PartitionedNodeIndexSeekByRange",
        Seq.empty,
        Seq(details("RANGE INDEX x:Label(Prop) WHERE Prop < 9")),
        Set("x")
      )
    )

    assertGood(
      attach(partitionedNodeIndexSeek("x:Label(9 <= Prop <= 11)"), 23.0),
      planDescription(
        id,
        "PartitionedNodeIndexSeekByRange",
        Seq.empty,
        Seq(details("RANGE INDEX x:Label(Prop) WHERE Prop >= 9 AND Prop <= 11")),
        Set("x")
      )
    )
  }

  test("RelationshipIndexSeek") {
    assertGood(
      attach(relationshipIndexSeek("(x)-[r:R(Prop)]->(y)"), 23.0),
      planDescription(
        id,
        "DirectedRelationshipIndexScan",
        Seq.empty,
        Seq(details("RANGE INDEX (x)-[r:R(Prop)]->(y) WHERE Prop IS NOT NULL")),
        Set("r", "x", "y")
      )
    )
    assertGood(
      attach(relationshipIndexSeek("(x)-[r:R(Prop)]-(y)"), 23.0),
      planDescription(
        id,
        "UndirectedRelationshipIndexScan",
        Seq.empty,
        Seq(details("RANGE INDEX (x)-[r:R(Prop)]-(y) WHERE Prop IS NOT NULL")),
        Set("r", "x", "y")
      )
    )

    assertGood(
      attach(relationshipIndexSeek("(x)-[r:R(Prop = 42)]->(y)", getValue = _ => GetValue), 23.0),
      planDescription(
        id,
        "DirectedRelationshipIndexSeek",
        Seq.empty,
        Seq(details("RANGE INDEX (x)-[r:R(Prop)]->(y) WHERE Prop = 42, cache[r.Prop]")),
        Set("r", "x", "y")
      )
    )
    assertGood(
      attach(relationshipIndexSeek("(x)-[r:R(Prop = 42)]-(y)", getValue = _ => GetValue), 23.0),
      planDescription(
        id,
        "UndirectedRelationshipIndexSeek",
        Seq.empty,
        Seq(details("RANGE INDEX (x)-[r:R(Prop)]-(y) WHERE Prop = 42, cache[r.Prop]")),
        Set("r", "x", "y")
      )
    )

    assertGood(
      attach(relationshipIndexSeek("(x)-[r:R(Prop CONTAINS 'Foo')]->(y)"), 23.0),
      planDescription(
        id,
        "DirectedRelationshipIndexContainsScan",
        Seq.empty,
        Seq(details("RANGE INDEX (x)-[r:R(Prop)]->(y) WHERE Prop CONTAINS \"Foo\"")),
        Set("r", "x", "y")
      )
    )
    assertGood(
      attach(relationshipIndexSeek("(x)-[r:R(Prop CONTAINS 'Foo')]-(y)"), 23.0),
      planDescription(
        id,
        "UndirectedRelationshipIndexContainsScan",
        Seq.empty,
        Seq(details("RANGE INDEX (x)-[r:R(Prop)]-(y) WHERE Prop CONTAINS \"Foo\"")),
        Set("r", "x", "y")
      )
    )

    assertGood(
      attach(relationshipIndexSeek("(x)-[r:R(Prop ENDS WITH 'Foo')]->(y)", indexType = IndexType.TEXT), 23.0),
      planDescription(
        id,
        "DirectedRelationshipIndexEndsWithScan",
        Seq.empty,
        Seq(details("TEXT INDEX (x)-[r:R(Prop)]->(y) WHERE Prop ENDS WITH \"Foo\"")),
        Set("r", "x", "y")
      )
    )
    assertGood(
      attach(relationshipIndexSeek("(x)-[r:R(Prop ENDS WITH 'Foo')]-(y)"), 23.0),
      planDescription(
        id,
        "UndirectedRelationshipIndexEndsWithScan",
        Seq.empty,
        Seq(details("RANGE INDEX (x)-[r:R(Prop)]-(y) WHERE Prop ENDS WITH \"Foo\"")),
        Set("r", "x", "y")
      )
    )
  }

  test("PartitionedRelationshipIndexSeek") {
    assertGood(
      attach(partitionedRelationshipIndexSeek("(x)-[r:R(Prop)]->(y)"), 23.0),
      planDescription(
        id,
        "PartitionedDirectedRelationshipIndexScan",
        Seq.empty,
        Seq(details("RANGE INDEX (x)-[r:R(Prop)]->(y) WHERE Prop IS NOT NULL")),
        Set("r", "x", "y")
      )
    )
    assertGood(
      attach(partitionedRelationshipIndexSeek("(x)-[r:R(Prop)]-(y)"), 23.0),
      planDescription(
        id,
        "PartitionedUndirectedRelationshipIndexScan",
        Seq.empty,
        Seq(details("RANGE INDEX (x)-[r:R(Prop)]-(y) WHERE Prop IS NOT NULL")),
        Set("r", "x", "y")
      )
    )

    assertGood(
      attach(partitionedRelationshipIndexSeek("(x)-[r:R(Prop = 42)]->(y)", getValue = _ => GetValue), 23.0),
      planDescription(
        id,
        "PartitionedDirectedRelationshipIndexSeek",
        Seq.empty,
        Seq(details("RANGE INDEX (x)-[r:R(Prop)]->(y) WHERE Prop = 42, cache[r.Prop]")),
        Set("r", "x", "y")
      )
    )
    assertGood(
      attach(partitionedRelationshipIndexSeek("(x)-[r:R(Prop = 42)]-(y)", getValue = _ => GetValue), 23.0),
      planDescription(
        id,
        "PartitionedUndirectedRelationshipIndexSeek",
        Seq.empty,
        Seq(details("RANGE INDEX (x)-[r:R(Prop)]-(y) WHERE Prop = 42, cache[r.Prop]")),
        Set("r", "x", "y")
      )
    )
  }

  test("RelationshipUniqueIndexSeek") {
    assertGood(
      attach(relationshipIndexSeek("(x)-[r:R(Prop = 42)]->(y)", getValue = _ => GetValue, unique = true), 23.0),
      planDescription(
        id,
        "DirectedRelationshipUniqueIndexSeek",
        Seq.empty,
        Seq(details("RANGE INDEX (x)-[r:R(Prop)]->(y) WHERE Prop = 42, cache[r.Prop]")),
        Set("r", "x", "y")
      )
    )

    assertGood(
      attach(relationshipIndexSeek("(x)-[r:R(1 < Prop < 123)]->(y)", getValue = _ => GetValue, unique = true), 23.0),
      planDescription(
        id,
        "DirectedRelationshipUniqueIndexSeekByRange",
        Seq.empty,
        Seq(details("RANGE INDEX (x)-[r:R(Prop)]->(y) WHERE Prop > 1 AND Prop < 123, cache[r.Prop]")),
        Set("r", "x", "y")
      )
    )

    assertGood(
      attach(relationshipIndexSeek("(x)-[r:R(Prop = 42)]->(y)", getValue = _ => GetValue, unique = true), 23.0),
      planDescription(
        id,
        "DirectedRelationshipUniqueIndexSeek(Locking)",
        Seq.empty,
        Seq(details("RANGE INDEX (x)-[r:R(Prop)]->(y) WHERE Prop = 42, cache[r.Prop]")),
        Set("r", "x", "y")
      ),
      readOnly = false
    )

    assertGood(
      attach(relationshipIndexSeek("(x)-[r:R(Prop = 42)]-(y)", getValue = _ => GetValue, unique = true), 23.0),
      planDescription(
        id,
        "UndirectedRelationshipUniqueIndexSeek",
        Seq.empty,
        Seq(details("RANGE INDEX (x)-[r:R(Prop)]-(y) WHERE Prop = 42, cache[r.Prop]")),
        Set("r", "x", "y")
      )
    )

    assertGood(
      attach(relationshipIndexSeek("(x)-[r:R(1 < Prop < 123)]-(y)", getValue = _ => GetValue, unique = true), 23.0),
      planDescription(
        id,
        "UndirectedRelationshipUniqueIndexSeekByRange",
        Seq.empty,
        Seq(details("RANGE INDEX (x)-[r:R(Prop)]-(y) WHERE Prop > 1 AND Prop < 123, cache[r.Prop]")),
        Set("r", "x", "y")
      )
    )

    assertGood(
      attach(relationshipIndexSeek("(x)-[r:R(Prop = 42)]-(y)", getValue = _ => GetValue, unique = true), 23.0),
      planDescription(
        id,
        "UndirectedRelationshipUniqueIndexSeek(Locking)",
        Seq.empty,
        Seq(details("RANGE INDEX (x)-[r:R(Prop)]-(y) WHERE Prop = 42, cache[r.Prop]")),
        Set("r", "x", "y")
      ),
      readOnly = false
    )
  }

  test("AllRelationshipsScan") {
    assertGood(
      attach(
        DirectedAllRelationshipsScan(varFor("r"), varFor("x"), varFor("y"), Set.empty),
        23.0
      ),
      planDescription(
        id,
        "DirectedAllRelationshipsScan",
        Seq.empty,
        Seq(details("(x)-[r]->(y)")),
        Set("r", "x", "y")
      )
    )

    assertGood(
      attach(
        UndirectedAllRelationshipsScan(varFor("r"), varFor("x"), varFor("y"), Set.empty),
        23.0
      ),
      planDescription(
        id,
        "UndirectedAllRelationshipsScan",
        Seq.empty,
        Seq(details("(x)-[r]-(y)")),
        Set("r", "x", "y")
      )
    )

    assertGood(
      attach(
        DirectedAllRelationshipsScan(Some(varFor("r")), None, Some(varFor("y")), Set.empty),
        23.0
      ),
      planDescription(
        id,
        "DirectedAllRelationshipsScan",
        Seq.empty,
        Seq(details("()-[r]->(y)")),
        Set("r", "y")
      )
    )

    assertGood(
      attach(
        DirectedAllRelationshipsScan(None, None, Some(varFor("y")), Set.empty),
        23.0
      ),
      planDescription(
        id,
        "DirectedAllRelationshipsScan",
        Seq.empty,
        Seq(details("()-->(y)")),
        Set("y")
      )
    )

    assertGood(
      attach(
        UndirectedAllRelationshipsScan(Some(varFor("r")), None, None, Set.empty),
        23.0
      ),
      planDescription(
        id,
        "UndirectedAllRelationshipsScan",
        Seq.empty,
        Seq(details("()-[r]-()")),
        Set("r")
      )
    )

    assertGood(
      attach(
        UndirectedAllRelationshipsScan(Some(varFor("r")), None, None, Set.empty),
        23.0
      ),
      planDescription(
        id,
        "UndirectedAllRelationshipsScan",
        Seq.empty,
        Seq(details("()-[r]-()")),
        Set("r")
      )
    )
  }

  test("RelationshipTypeScan") {
    assertGood(
      attach(
        DirectedRelationshipTypeScan(
          varFor("r"),
          varFor("x"),
          RelTypeName("R")(pos),
          varFor("y"),
          Set.empty,
          IndexOrderNone
        ),
        23.0
      ),
      planDescription(
        id,
        "DirectedRelationshipTypeScan",
        Seq.empty,
        Seq(details("(x)-[r:R]->(y)")),
        Set("r", "x", "y")
      )
    )

    assertGood(
      attach(
        UndirectedRelationshipTypeScan(
          varFor("r"),
          varFor("x"),
          RelTypeName("R")(pos),
          varFor("y"),
          Set.empty,
          IndexOrderNone
        ),
        23.0
      ),
      planDescription(
        id,
        "UndirectedRelationshipTypeScan",
        Seq.empty,
        Seq(details("(x)-[r:R]-(y)")),
        Set("r", "x", "y")
      )
    )

    assertGood(
      attach(
        DirectedRelationshipTypeScan(
          Some(varFor("r")),
          None,
          RelTypeName("R")(pos),
          None,
          Set.empty,
          IndexOrderNone
        ),
        23.0
      ),
      planDescription(
        id,
        "DirectedRelationshipTypeScan",
        Seq.empty,
        Seq(details("()-[r:R]->()")),
        Set("r")
      )
    )

    assertGood(
      attach(
        DirectedRelationshipTypeScan(
          None,
          None,
          RelTypeName("R")(pos),
          None,
          Set.empty,
          IndexOrderNone
        ),
        23.0
      ),
      planDescription(
        id,
        "DirectedRelationshipTypeScan",
        Seq.empty,
        Seq(details("()-[:R]->()")),
        Set.empty
      )
    )

    assertGood(
      attach(
        UndirectedRelationshipTypeScan(
          Some(varFor("r")),
          Some(varFor("x")),
          RelTypeName("R")(pos),
          None,
          Set.empty,
          IndexOrderNone
        ),
        23.0
      ),
      planDescription(
        id,
        "UndirectedRelationshipTypeScan",
        Seq.empty,
        Seq(details("(x)-[r:R]-()")),
        Set("r", "x")
      )
    )

    assertGood(
      attach(
        UndirectedRelationshipTypeScan(
          None,
          Some(varFor("x")),
          RelTypeName("R")(pos),
          None,
          Set.empty,
          IndexOrderNone
        ),
        23.0
      ),
      planDescription(
        id,
        "UndirectedRelationshipTypeScan",
        Seq.empty,
        Seq(details("(x)-[:R]-()")),
        Set("x")
      )
    )
  }

  test("DynamicRelationshipTypeLookup") {
    assertGood(
      attach(
        DynamicDirectedRelationshipTypeLookup(
          Some(varFor("r")),
          Some(varFor("x")),
          DynamicElement.Simple(literal(List("R", "S")), Any),
          Some(varFor("y")),
          Set.empty,
          IndexOrderNone,
          propertyPredicates = Map.empty
        ),
        23.0
      ),
      planDescription(
        id,
        "DynamicDirectedRelationshipTypeLookup",
        Seq.empty,
        Seq(details("(x)-[r:$any([\"R\", \"S\"])]->(y)")),
        Set("r", "x", "y")
      )
    )
    assertGood(
      attach(
        DynamicDirectedRelationshipTypeLookup(
          None,
          None,
          DynamicElement.Simple(literal(List("R", "S")), Any),
          Some(varFor("y")),
          Set.empty,
          IndexOrderNone,
          propertyPredicates = Map.empty
        ),
        23.0
      ),
      planDescription(
        id,
        "DynamicDirectedRelationshipTypeLookup",
        Seq.empty,
        Seq(details("()-[:$any([\"R\", \"S\"])]->(y)")),
        Set("y")
      )
    )

    assertGood(
      attach(
        DynamicUndirectedRelationshipTypeLookup(
          Some(varFor("r")),
          Some(varFor("x")),
          DynamicElement.Simple(literal(List("R", "S")), Any),
          Some(varFor("y")),
          Set.empty,
          IndexOrderNone,
          propertyPredicates = Map.empty
        ),
        23.0
      ),
      planDescription(
        id,
        "DynamicUndirectedRelationshipTypeLookup",
        Seq.empty,
        Seq(details("(x)-[r:$any([\"R\", \"S\"])]-(y)")),
        Set("r", "x", "y")
      )
    )

    assertGood(
      attach(
        DynamicUndirectedRelationshipTypeLookup(
          None,
          Some(varFor("x")),
          DynamicElement.Simple(literal(List("R", "S")), Any),
          Some(varFor("y")),
          Set.empty,
          IndexOrderNone,
          propertyPredicates = Map.empty
        ),
        23.0
      ),
      planDescription(
        id,
        "DynamicUndirectedRelationshipTypeLookup",
        Seq.empty,
        Seq(details("(x)-[:$any([\"R\", \"S\"])]-(y)")),
        Set("x", "y")
      )
    )

    assertGood(
      attach(
        DynamicDirectedRelationshipTypeLookup(
          Some(varFor("r")),
          None,
          DynamicElement.Simple(literal(List("R", "S")), Any),
          None,
          Set.empty,
          IndexOrderNone,
          propertyPredicates = Map.empty
        ),
        23.0
      ),
      planDescription(
        id,
        "DynamicDirectedRelationshipTypeLookup",
        Seq.empty,
        Seq(details("()-[r:$any([\"R\", \"S\"])]->()")),
        Set("r")
      )
    )

    assertGood(
      attach(
        DynamicDirectedRelationshipTypeLookup(
          None,
          None,
          DynamicElement.Simple(literal(List("R", "S")), Any),
          None,
          Set.empty,
          IndexOrderNone,
          propertyPredicates = Map.empty
        ),
        23.0
      ),
      planDescription(
        id,
        "DynamicDirectedRelationshipTypeLookup",
        Seq.empty,
        Seq(details("()-[:$any([\"R\", \"S\"])]->()")),
        Set.empty
      )
    )

    assertGood(
      attach(
        DynamicUndirectedRelationshipTypeLookup(
          Some(varFor("r")),
          Some(varFor("x")),
          DynamicElement.Simple(literal(List("R", "S")), Any),
          None,
          Set.empty,
          IndexOrderNone,
          propertyPredicates = Map.empty
        ),
        23.0
      ),
      planDescription(
        id,
        "DynamicUndirectedRelationshipTypeLookup",
        Seq.empty,
        Seq(details("(x)-[r:$any([\"R\", \"S\"])]-()")),
        Set("r", "x")
      )
    )

    assertGood(
      attach(
        DynamicDirectedRelationshipTypeLookup(
          Some(varFor("  UNNAMED123")),
          Some(varFor("x")),
          DynamicElement.Simple(literal(List("R")), Any),
          Some(varFor("y")),
          Set.empty,
          IndexOrderNone,
          propertyPredicates = Map.empty
        ),
        33.0
      ),
      planDescription(
        id,
        "DynamicDirectedRelationshipTypeLookup",
        Seq.empty,
        Seq(details(s"(x)-[${anonVar("123")}:$$any([\"R\"])]->(y)")),
        Set(anonVar("123"), "x", "y")
      )
    )

    assertGood(
      attach(
        DynamicDirectedRelationshipTypeLookup(
          Some(varFor("r")),
          Some(varFor("x")),
          DynamicElement.Simple(literal(List("R", "S")), Any),
          Some(varFor("y")),
          Set.empty,
          IndexOrderNone,
          Map(
            PropertyKeyToken("prop", PropertyKeyId(0)) -> literal(1),
            PropertyKeyToken("foo", PropertyKeyId(0)) -> literal("bar")
          )
        ),
        23.0
      ),
      planDescription(
        id,
        "DynamicDirectedRelationshipTypeLookup",
        Seq.empty,
        Seq(details(
          """(x)-[r:$any(["R", "S"])]->(y)
            |r.prop = 1
            |r.foo = "bar"""".stripMargin
        )),
        Set("r", "x", "y")
      )
    )
    assertGood(
      attach(
        DynamicUndirectedRelationshipTypeLookup(
          Some(varFor("r")),
          Some(varFor("x")),
          DynamicElement.Simple(literal(List("R", "S")), Any),
          Some(varFor("y")),
          Set.empty,
          IndexOrderNone,
          Map(
            PropertyKeyToken("prop", PropertyKeyId(0)) -> literal(1),
            PropertyKeyToken("foo", PropertyKeyId(0)) -> literal("bar")
          )
        ),
        23.0
      ),
      planDescription(
        id,
        "DynamicUndirectedRelationshipTypeLookup",
        Seq.empty,
        Seq(details(
          """(x)-[r:$any(["R", "S"])]-(y)
            |r.prop = 1
            |r.foo = "bar"""".stripMargin
        )),
        Set("r", "x", "y")
      )
    )
  }

  test("PartitionedRelationshipTypeScan") {
    assertGood(
      attach(
        PartitionedDirectedRelationshipTypeScan(
          Some(varFor("r")),
          Some(varFor("x")),
          RelTypeName("R")(pos),
          Some(varFor("y")),
          Set.empty
        ),
        23.0
      ),
      planDescription(
        id,
        "PartitionedDirectedRelationshipTypeScan",
        Seq.empty,
        Seq(details("(x)-[r:R]->(y)")),
        Set("r", "x", "y")
      )
    )

    assertGood(
      attach(
        PartitionedUndirectedRelationshipTypeScan(
          Some(varFor("r")),
          Some(varFor("x")),
          RelTypeName("R")(pos),
          Some(varFor("y")),
          Set.empty
        ),
        23.0
      ),
      planDescription(
        id,
        "PartitionedUndirectedRelationshipTypeScan",
        Seq.empty,
        Seq(details("(x)-[r:R]-(y)")),
        Set("r", "x", "y")
      )
    )

    assertGood(
      attach(
        PartitionedUndirectedRelationshipTypeScan(
          None,
          Some(varFor("x")),
          RelTypeName("R")(pos),
          Some(varFor("y")),
          Set.empty
        ),
        23.0
      ),
      planDescription(
        id,
        "PartitionedUndirectedRelationshipTypeScan",
        Seq.empty,
        Seq(details("(x)-[:R]-(y)")),
        Set("x", "y")
      )
    )

    assertGood(
      attach(
        PartitionedDirectedRelationshipTypeScan(
          Some(varFor("r")),
          None,
          RelTypeName("R")(pos),
          None,
          Set.empty
        ),
        23.0
      ),
      planDescription(
        id,
        "PartitionedDirectedRelationshipTypeScan",
        Seq.empty,
        Seq(details("()-[r:R]->()")),
        Set("r")
      )
    )

    assertGood(
      attach(
        PartitionedUndirectedRelationshipTypeScan(
          Some(varFor("r")),
          None,
          RelTypeName("R")(pos),
          None,
          Set.empty
        ),
        23.0
      ),
      planDescription(
        id,
        "PartitionedUndirectedRelationshipTypeScan",
        Seq.empty,
        Seq(details("()-[r:R]-()")),
        Set("r")
      )
    )

    assertGood(
      attach(
        PartitionedUndirectedRelationshipTypeScan(
          None,
          None,
          RelTypeName("R")(pos),
          None,
          Set.empty
        ),
        23.0
      ),
      planDescription(
        id,
        "PartitionedUndirectedRelationshipTypeScan",
        Seq.empty,
        Seq(details("()-[:R]-()")),
        Set.empty
      )
    )
  }

  test("MultiNodeIndexSeek") {
    assertGood(
      attach(
        MultiNodeIndexSeek(Seq(
          nodeIndexSeek("x:Label(Prop = 10,Foo = 12)", unique = true).asInstanceOf[NodeIndexSeekSingleLabelLeafPlan],
          nodeIndexSeek("y:Label(Prop = 12)", unique = false).asInstanceOf[NodeIndexSeekSingleLabelLeafPlan]
        )),
        230.0
      ),
      planDescription(
        id,
        "MultiNodeIndexSeek",
        Seq.empty,
        Seq(details(Seq(
          "UNIQUE x:Label(Prop, Foo) WHERE Prop = 10 AND Foo = 12",
          "RANGE INDEX y:Label(Prop) WHERE Prop = 12"
        ))),
        Set("x", "y")
      )
    )
  }

  test("ProduceResult") {
    assertGood(
      attach(ProduceResult.withNoCachedProperties(lhsLP, Seq(varFor("a"), varFor("b"), varFor("c\nd"))), 12.0),
      planDescription(id, "ProduceResults", Seq(lhsPD), Seq(details(Seq("a", "b", "`c d`"))), Set("a"))
    )
  }

  test("Argument") {
    assertGood(
      attach(plans.Argument(Set.empty), 95.0),
      planDescription(id, "EmptyRow", Seq.empty, Seq.empty, Set.empty)
    )

    assertGood(
      attach(plans.Argument(Set(varFor("a"), varFor("b"))), 95.0),
      planDescription(id, "Argument", Seq.empty, Seq(details("a, b")), Set("a", "b"))
    )
  }

  test("RelationshipByIdSeek") {
    assertGood(
      attach(
        DirectedRelationshipByIdSeek(varFor("r"), SingleSeekableArg(number("1")), varFor("a"), varFor("b"), Set.empty),
        70.0
      ),
      planDescription(
        id,
        "DirectedRelationshipByIdSeek",
        Seq.empty,
        Seq(details("(a)-[r]->(b) WHERE id(r) = 1")),
        Set("r", "a", "b")
      )
    )

    assertGood(
      attach(
        DirectedRelationshipByIdSeek(
          Some(varFor("r")),
          SingleSeekableArg(number("1")),
          Some(varFor("a")),
          Some(varFor("b")),
          Set(varFor("x"))
        ),
        70.0
      ),
      planDescription(
        id,
        "DirectedRelationshipByIdSeek",
        Seq.empty,
        Seq(details("(a)-[r]->(b) WHERE id(r) = 1")),
        Set("r", "a", "b", "x")
      )
    )

    assertGood(
      attach(
        DirectedRelationshipByIdSeek(
          varFor("r"),
          ManySeekableArgs(ListLiteral(Seq(number("1")))(pos)),
          varFor("a"),
          varFor("b"),
          Set(varFor("x"))
        ),
        70.0
      ),
      planDescription(
        id,
        "DirectedRelationshipByIdSeek",
        Seq.empty,
        Seq(details("(a)-[r]->(b) WHERE id(r) = 1")),
        Set("r", "a", "b", "x")
      )
    )

    assertGood(
      attach(
        DirectedRelationshipByIdSeek(
          varFor("r"),
          ManySeekableArgs(ListLiteral(Seq(number("1"), number("2")))(pos)),
          varFor("a"),
          varFor("b"),
          Set(varFor("x"))
        ),
        70.0
      ),
      planDescription(
        id,
        "DirectedRelationshipByIdSeek",
        Seq.empty,
        Seq(details("(a)-[r]->(b) WHERE id(r) IN [1, 2]")),
        Set("r", "a", "b", "x")
      )
    )

    assertGood(
      attach(
        DirectedRelationshipByIdSeek(Some(varFor("r")), SingleSeekableArg(number("1")), None, None, Set.empty),
        70.0
      ),
      planDescription(
        id,
        "DirectedRelationshipByIdSeek",
        Seq.empty,
        Seq(details("()-[r]->() WHERE id(r) = 1")),
        Set("r")
      )
    )
    assertGood(
      attach(
        DirectedRelationshipByIdSeek(None, SingleSeekableArg(number("1")), None, None, Set.empty),
        70.0
      ),
      planDescription(
        id,
        "DirectedRelationshipByIdSeek",
        Seq.empty,
        Seq(details("()-->() WHERE id(_) = 1")),
        Set.empty
      )
    )

    assertGood(
      attach(
        UndirectedRelationshipByIdSeek(
          varFor("r"),
          SingleSeekableArg(number("1")),
          varFor("a"),
          varFor("b"),
          Set(varFor("x"))
        ),
        70.0
      ),
      planDescription(
        id,
        "UndirectedRelationshipByIdSeek",
        Seq.empty,
        Seq(details("(a)-[r]-(b) WHERE id(r) = 1")),
        Set("r", "a", "b", "x")
      )
    )

    assertGood(
      attach(
        UndirectedRelationshipByIdSeek(
          varFor("  UNNAMED2"),
          SingleSeekableArg(number("1")),
          varFor("a"),
          varFor("  UNNAMED32"),
          Set(varFor("x"))
        ),
        70.0
      ),
      planDescription(
        id,
        "UndirectedRelationshipByIdSeek",
        Seq.empty,
        Seq(details(s"(a)-[${anonVar("2")}]-(${anonVar("32")}) WHERE id(${anonVar("2")}) = 1")),
        Set(anonVar("2"), "a", anonVar("32"), "x")
      )
    )
  }

  test("RelationshipByElementIdSeek") {
    assertGood(
      attach(
        DirectedRelationshipByElementIdSeek(
          varFor("r"),
          SingleSeekableArg(stringLiteral("some-id")),
          varFor("a"),
          varFor("b"),
          Set.empty
        ),
        70.0
      ),
      planDescription(
        id,
        "DirectedRelationshipByElementIdSeek",
        Seq.empty,
        Seq(details("(a)-[r]->(b) WHERE elementId(r) = \"some-id\"")),
        Set("r", "a", "b")
      )
    )

    assertGood(
      attach(
        DirectedRelationshipByElementIdSeek(
          varFor("r"),
          SingleSeekableArg(stringLiteral("some-id")),
          varFor("a"),
          varFor("b"),
          Set(varFor("x"))
        ),
        70.0
      ),
      planDescription(
        id,
        "DirectedRelationshipByElementIdSeek",
        Seq.empty,
        Seq(details("(a)-[r]->(b) WHERE elementId(r) = \"some-id\"")),
        Set("r", "a", "b", "x")
      )
    )

    assertGood(
      attach(
        DirectedRelationshipByElementIdSeek(
          varFor("r"),
          ManySeekableArgs(listOfString("some-id")),
          varFor("a"),
          varFor("b"),
          Set(varFor("x"))
        ),
        70.0
      ),
      planDescription(
        id,
        "DirectedRelationshipByElementIdSeek",
        Seq.empty,
        Seq(details("(a)-[r]->(b) WHERE elementId(r) = \"some-id\"")),
        Set("r", "a", "b", "x")
      )
    )

    assertGood(
      attach(
        DirectedRelationshipByElementIdSeek(
          varFor("r"),
          ManySeekableArgs(listOfString("some-id", "other-id")),
          varFor("a"),
          varFor("b"),
          Set(varFor("x"))
        ),
        70.0
      ),
      planDescription(
        id,
        "DirectedRelationshipByElementIdSeek",
        Seq.empty,
        Seq(details("(a)-[r]->(b) WHERE elementId(r) IN [\"some-id\", \"other-id\"]")),
        Set("r", "a", "b", "x")
      )
    )

    assertGood(
      attach(
        UndirectedRelationshipByElementIdSeek(
          varFor("r"),
          SingleSeekableArg(stringLiteral("some-id")),
          varFor("a"),
          varFor("b"),
          Set(varFor("x"))
        ),
        70.0
      ),
      planDescription(
        id,
        "UndirectedRelationshipByElementIdSeek",
        Seq.empty,
        Seq(details("(a)-[r]-(b) WHERE elementId(r) = \"some-id\"")),
        Set("r", "a", "b", "x")
      )
    )

    assertGood(
      attach(
        UndirectedRelationshipByElementIdSeek(
          varFor("  UNNAMED2"),
          SingleSeekableArg(stringLiteral("some-id")),
          varFor("a"),
          varFor("  UNNAMED32"),
          Set(varFor("x"))
        ),
        70.0
      ),
      planDescription(
        id,
        "UndirectedRelationshipByElementIdSeek",
        Seq.empty,
        Seq(details(s"(a)-[${anonVar("2")}]-(${anonVar("32")}) WHERE elementId(${anonVar("2")}) = \"some-id\"")),
        Set(anonVar("2"), "a", anonVar("32"), "x")
      )
    )
    assertGood(
      attach(
        UndirectedRelationshipByIdSeek(
          Some(varFor("r")),
          SingleSeekableArg(number("1")),
          None,
          None,
          Set(varFor("x"))
        ),
        70.0
      ),
      planDescription(
        id,
        "UndirectedRelationshipByIdSeek",
        Seq.empty,
        Seq(details("()-[r]-() WHERE id(r) = 1")),
        Set("r", "x")
      )
    )

    assertGood(
      attach(
        UndirectedRelationshipByIdSeek(
          None,
          SingleSeekableArg(number("1")),
          None,
          None,
          Set(varFor("x"))
        ),
        70.0
      ),
      planDescription(
        id,
        "UndirectedRelationshipByIdSeek",
        Seq.empty,
        Seq(details("()--() WHERE id(_) = 1")),
        Set("x")
      )
    )
  }

  test("LoadCSV") {
    assertGood(
      attach(
        LoadCSV(
          lhsLP,
          stringLiteral("file:///tmp/foo.csv"),
          varFor("u"),
          NoHeaders,
          None,
          legacyCsvQuoteEscaping = false,
          csvBufferSize = 2
        ),
        27.6
      ),
      planDescription(id, "LoadCSV", Seq(lhsPD), Seq(details("u")), Set("u", "a"))
    )

    assertGood(
      attach(
        LoadCSV(
          lhsLP,
          stringLiteral("file:///tmp/foo.csv"),
          varFor("  UNNAMED2"),
          NoHeaders,
          None,
          legacyCsvQuoteEscaping = false,
          csvBufferSize = 2
        ),
        27.6
      ),
      planDescription(id, "LoadCSV", Seq(lhsPD), Seq(details(anonVar("2"))), Set(anonVar("2"), "a"))
    )
  }

  test("Input") {
    assertGood(
      attach(
        Input(Seq(varFor("n1"), varFor("n2")), Seq(varFor("r")), Seq(varFor("v1"), varFor("v2")), nullable = false),
        4.0
      ),
      planDescription(
        id,
        "Input",
        Seq.empty,
        Seq(details(Seq("n1", "n2", "r", "v1", "v2"))),
        Set("n1", "n2", "r", "v1", "v2")
      )
    )
  }

  test("NodeCountFromCountStore") {
    assertGood(
      attach(NodeCountFromCountStore(varFor("x"), List(Some(label("LabelName"))), Set.empty), 54.2),
      planDescription(id, "NodeCountFromCountStore", Seq.empty, Seq(details("count( (:LabelName) ) AS x")), Set("x"))
    )

    assertGood(
      attach(
        NodeCountFromCountStore(varFor("x"), List(Some(label("LabelName")), Some(label("LabelName2"))), Set.empty),
        54.2
      ),
      planDescription(
        id,
        "NodeCountFromCountStore",
        Seq.empty,
        Seq(details("count( (:LabelName), (:LabelName2) ) AS x")),
        Set("x")
      )
    )

    assertGood(
      attach(NodeCountFromCountStore(varFor("  UNNAMED123"), List(Some(label("LabelName"))), Set.empty), 54.2),
      planDescription(
        id,
        "NodeCountFromCountStore",
        Seq.empty,
        Seq(details(s"count( (:LabelName) ) AS ${anonVar("123")}")),
        Set(anonVar("123"))
      )
    )

    assertGood(
      attach(NodeCountFromCountStore(varFor("x"), List(None, None), Set.empty), 54.2),
      planDescription(id, "NodeCountFromCountStore", Seq.empty, Seq(details("count( (), () ) AS x")), Set("x"))
    )
  }

  test("ProcedureCall") {
    val name = procedureName("my", "proc", "foo")
    val signatureInputs = IndexedSeq(FieldSignature("a", CTInteger))
    val signatureOutputs = Some(IndexedSeq(FieldSignature("x", CTInteger), FieldSignature("y", CTList(CTNode))))
    val signature =
      ProcedureSignature(name, signatureInputs, signatureOutputs, None, ProcedureReadOnlyAccess, id = 42)
    val callResults = IndexedSeq(ProcedureResultItem(varFor("x"))(pos), ProcedureResultItem(varFor("y"))(pos))
    val call = ResolvedNonLocalCall(signature, Seq(varFor("a1")), callResults)(pos)

    assertGood(
      attach(ProcedureCall(lhsLP, call), 33.2),
      planDescription(
        id,
        "ProcedureCall",
        Seq(lhsPD),
        Seq(details("my.proc.foo(a1) :: (x :: INTEGER, y :: LIST<NODE>)")),
        Set("a", "x", "y")
      )
    )
  }

  test("RelationshipCountFromCountStore") {
    assertGood(
      attach(
        RelationshipCountFromCountStore(varFor("x"), None, Seq(relType("LIKES"), relType("LOVES")), None, Set.empty),
        54.2
      ),
      planDescription(
        id,
        "RelationshipCountFromCountStore",
        Seq.empty,
        Seq(details("count( ()-[:LIKES|LOVES]->() ) AS x")),
        Set("x")
      )
    )

    assertGood(
      attach(
        RelationshipCountFromCountStore(varFor("  UNNAMED122"), None, Seq(relType("LIKES")), None, Set.empty),
        54.2
      ),
      planDescription(
        id,
        "RelationshipCountFromCountStore",
        Seq.empty,
        Seq(details(s"count( ()-[:LIKES]->() ) AS ${anonVar("122")}")),
        Set(anonVar("122"))
      )
    )

    assertGood(
      attach(
        RelationshipCountFromCountStore(
          varFor("x"),
          Some(label("StartLabel")),
          Seq(relType("LIKES"), relType("LOVES")),
          None,
          Set.empty
        ),
        54.2
      ),
      planDescription(
        id,
        "RelationshipCountFromCountStore",
        Seq.empty,
        Seq(details("count( (:StartLabel)-[:LIKES|LOVES]->() ) AS x")),
        Set("x")
      )
    )

    assertGood(
      attach(
        RelationshipCountFromCountStore(
          varFor("x"),
          Some(label("StartLabel")),
          Seq(relType("LIKES"), relType("LOVES")),
          Some(label("EndLabel")),
          Set.empty
        ),
        54.2
      ),
      planDescription(
        id,
        "RelationshipCountFromCountStore",
        Seq.empty,
        Seq(details("count( (:StartLabel)-[:LIKES|LOVES]->(:EndLabel) ) AS x")),
        Set("x")
      )
    )

    assertGood(
      attach(
        RelationshipCountFromCountStore(
          varFor("x"),
          Some(label("StartLabel")),
          Seq.empty,
          Some(label("EndLabel")),
          Set.empty
        ),
        54.2
      ),
      planDescription(
        id,
        "RelationshipCountFromCountStore",
        Seq.empty,
        Seq(details("count( (:StartLabel)-[]->(:EndLabel) ) AS x")),
        Set("x")
      )
    )
  }

  test("Aggregation") {
    // Aggregation 1 grouping, 0 aggregating
    assertGood(
      attach(Aggregation(lhsLP, Map(varFor("a") -> varFor("a")), Map.empty), 17.5),
      planDescription(id, "EagerAggregation", Seq(lhsPD), Seq(details("a")), Set("a"))
    )

    // Aggregation 2 grouping, 0 aggregating
    assertGood(
      attach(Aggregation(lhsLP, Map(varFor("a") -> varFor("a"), varFor("b") -> varFor("c")), Map.empty), 17.5),
      planDescription(id, "EagerAggregation", Seq(lhsPD), Seq(details("a, c AS b")), Set("a", "b"))
    )

    val countFunction =
      FunctionInvocation(functionName(Count.name), distinct = false, IndexedSeq(varFor("c")))(pos)
    val collectDistinctFunction =
      FunctionInvocation(functionName(Collect.name), distinct = true, IndexedSeq(varFor("c")))(pos)
    val orderedCollectDistinctFunction =
      FunctionInvocation(functionName(Collect.name), distinct = true, IndexedSeq(varFor("c")), ArgumentAsc)(pos)

    // Aggregation 1 grouping, 1 aggregating
    assertGood(
      attach(Aggregation(lhsLP, Map(varFor("a") -> varFor("a")), Map(varFor("count") -> countFunction)), 1.3),
      planDescription(
        id,
        "EagerAggregation",
        Seq(lhsPD),
        Seq(details("a, count(c) AS count")),
        Set("a", "count")
      )
    )

    // Aggregation 2 grouping, 2 aggregating
    assertGood(
      attach(
        Aggregation(
          lhsLP,
          Map(varFor("a") -> varFor("a"), varFor("b") -> varFor("c")),
          Map(varFor("count(c)") -> countFunction, varFor("collect") -> collectDistinctFunction)
        ),
        1.3
      ),
      planDescription(
        id,
        "EagerAggregation",
        Seq(lhsPD),
        Seq(details("a, c AS b, count(c) AS `count(c)`, collect(DISTINCT c) AS collect")),
        Set("a", "b", "`count(c)`", "collect")
      )
    )

    assertGood(
      attach(
        Aggregation(
          lhsLP,
          Map(varFor("a") -> varFor("a"), varFor("b") -> varFor("c")),
          Map(varFor("count(c)") -> countFunction, varFor("collect") -> orderedCollectDistinctFunction)
        ),
        1.3
      ),
      planDescription(
        id,
        "EagerAggregation",
        Seq(lhsPD),
        Seq(details("a, c AS b, count(c) AS `count(c)`, collect(DISTINCT c) ASC AS collect")),
        Set("a", "b", "`count(c)`", "collect")
      )
    )

    // Distinct 1 grouping
    assertGood(
      attach(Distinct(lhsLP, Map(varFor("  a@23") -> varFor("  a@23"))), 45.9),
      planDescription(id, "Distinct", Seq(lhsPD), Seq(details("a")), Set("a"))
    )

    // Distinct 2 grouping
    assertGood(
      attach(Distinct(lhsLP, Map(varFor("a") -> varFor("a"), varFor("b") -> varFor("c"))), 45.9),
      planDescription(id, "Distinct", Seq(lhsPD), Seq(details("a, c AS b")), Set("a", "b"))
    )

    // OrderedDistinct 1 column, 1 sorted
    assertGood(
      attach(OrderedDistinct(lhsLP, Map(varFor("a") -> varFor("a")), Seq(varFor("a"))), 45.9),
      planDescription(id, "OrderedDistinct", Seq(lhsPD), Seq(details("a")), Set("a"))
    )

    // OrderedDistinct 3 column, 2 sorted
    assertGood(
      attach(
        OrderedDistinct(
          lhsLP,
          Map(varFor("a") -> varFor("a"), varFor("b") -> varFor("c"), varFor("d") -> varFor("d")),
          Seq(varFor("d"), varFor("a"))
        ),
        45.9
      ),
      planDescription(id, "OrderedDistinct", Seq(lhsPD), Seq(details("d, a, c AS b")), Set("a", "b", "d"))
    )

    // OrderedAggregation 1 grouping, 0 aggregating, 1 sorted
    assertGood(
      attach(OrderedAggregation(lhsLP, Map(varFor("a") -> varFor("a")), Map.empty, Seq(varFor("a"))), 17.5),
      planDescription(id, "OrderedAggregation", Seq(lhsPD), Seq(details("a")), Set("a"))
    )

    // OrderedAggregation 3 grouping, 0 aggregating, 2 sorted
    assertGood(
      attach(
        OrderedAggregation(
          lhsLP,
          Map(varFor("a") -> varFor("a"), varFor("b") -> varFor("c"), varFor("d") -> varFor("d")),
          Map.empty,
          Seq(varFor("d"), varFor("a"))
        ),
        17.5
      ),
      planDescription(id, "OrderedAggregation", Seq(lhsPD), Seq(details("d, a, c AS b")), Set("a", "b", "d"))
    )

    // OrderedAggregation 3 grouping, 1 aggregating with alias, 2 sorted
    assertGood(
      attach(
        OrderedAggregation(
          lhsLP,
          Map(varFor("a") -> varFor("a"), varFor("b") -> varFor("c"), varFor("d") -> varFor("d")),
          Map(varFor("count") -> countFunction),
          Seq(varFor("d"), varFor("a"))
        ),
        1.3
      ),
      planDescription(
        id,
        "OrderedAggregation",
        Seq(lhsPD),
        Seq(details("d, a, c AS b, count(c) AS count")),
        Set("a", "b", "d", "count")
      )
    )

    // OrderedAggregation 3 grouping, 1 aggregating without alias, 2 sorted
    assertGood(
      attach(
        OrderedAggregation(
          lhsLP,
          Map(varFor("a") -> varFor("a"), varFor("b") -> varFor("c"), varFor("d") -> varFor("d")),
          Map(varFor("collect(DISTINCT c)") -> collectDistinctFunction),
          Seq(varFor("d"), varFor("a"))
        ),
        1.3
      ),
      planDescription(
        id,
        "OrderedAggregation",
        Seq(lhsPD),
        Seq(details("d, a, c AS b, collect(DISTINCT c) AS `collect(DISTINCT c)`")),
        Set("a", "b", "d", "`collect(DISTINCT c)`")
      )
    )

    assertGood(
      attach(
        OrderedAggregation(
          lhsLP,
          Map(varFor("a") -> varFor("a"), varFor("b") -> varFor("c"), varFor("d") -> varFor("d")),
          Map(varFor("collect(DISTINCT c)") -> orderedCollectDistinctFunction),
          Seq(varFor("d"), varFor("a"))
        ),
        1.3
      ),
      planDescription(
        id,
        "OrderedAggregation",
        Seq(lhsPD),
        Seq(details("d, a, c AS b, collect(DISTINCT c) ASC AS `collect(DISTINCT c)`")),
        Set("a", "b", "d", "`collect(DISTINCT c)`")
      )
    )

    // OrderedAggregation 3 grouping, 1 aggregating with alias, sorted on aliased column
    assertGood(
      attach(
        OrderedAggregation(
          lhsLP,
          Map(varFor("a") -> varFor("a"), varFor("b") -> varFor("c"), varFor("d") -> varFor("d")),
          Map(varFor("collect(DISTINCT c)") -> collectDistinctFunction),
          Seq(varFor("d"), varFor("c"))
        ),
        1.3
      ),
      planDescription(
        id,
        "OrderedAggregation",
        Seq(lhsPD),
        Seq(details("d, c AS b, a, collect(DISTINCT c) AS `collect(DISTINCT c)`")),
        Set("a", "b", "d", "`collect(DISTINCT c)`")
      )
    )
  }

  test("FunctionInvocation") {
    val namespace = List("ns")
    val name = "datetime"
    val args = IndexedSeq(number("23391882379"))
    val functionSignature = UserFunctionSignature(
      functionName("datetime"),
      IndexedSeq(FieldSignature("Input", CTAny, Some(stringValue("DEFAULT_TEMPORAL_ARGUMENT")))),
      BooleanType(isNullable = true)(pos),
      None,
      None,
      isAggregate = false,
      1,
      builtIn = true
    )

    val functionInvocation = FunctionInvocation(
      functionName = functionName(namespace, name),
      distinct = false,
      args = args
    )(pos)
    assertGood(
      attach(SetRelationshipProperty(lhsLP, varFor("x"), key("prop"), functionInvocation), 1.0),
      planDescription(
        id,
        "SetProperty",
        Seq(lhsPD),
        Seq(details("x.prop = ns.datetime(23391882379)")),
        Set("a", "x")
      )
    )

    val resolvedFunctionInvocation = ResolvedFunctionInvocation(
      functionName = functionName(namespace, name),
      fcnSignature = Some(functionSignature),
      callArguments = args
    )(pos)
    assertGood(
      attach(SetRelationshipProperty(lhsLP, varFor("x"), key("prop"), resolvedFunctionInvocation), 1.0),
      planDescription(
        id,
        "SetProperty",
        Seq(lhsPD),
        Seq(details("x.prop = ns.datetime(23391882379)")),
        Set("a", "x")
      )
    )
  }

  test("Create") {
    val properties = MapExpression(Seq(
      (key("y"), number("1")),
      (key("crs"), stringLiteral("cartesian"))
    ))(pos)

    assertGood(
      attach(
        Create(
          lhsLP,
          Seq(
            CreateNode(varFor("x"), Set.empty, Set.empty, None),
            CreateRelationship(varFor("r"), varFor("x"), relType("R"), varFor("y"), SemanticDirection.INCOMING, None)
          )
        ),
        32.2
      ),
      planDescription(id, "Create", Seq(lhsPD), Seq(details(Seq("(x)", "(x)<-[r:R]-(y)"))), Set("a", "x", "r"))
    )

    assertGood(
      attach(
        Create(
          lhsLP,
          Seq(
            CreateNode(varFor("x"), Set(label("Label")), Set.empty, None),
            CreateRelationship(
              varFor("  UNNAMED67"),
              varFor("x"),
              relType("R"),
              varFor("y"),
              SemanticDirection.INCOMING,
              None
            )
          )
        ),
        32.2
      ),
      planDescription(
        id,
        "Create",
        Seq(lhsPD),
        Seq(details(Seq("(x:Label)", s"(x)<-[${anonVar("67")}:R]-(y)"))),
        Set("a", "x", anonVar("67"))
      )
    )

    assertGood(
      attach(
        Create(
          lhsLP,
          Seq(
            CreateNode(varFor("x"), Set(label("Label1"), label("Label2")), Set.empty, None),
            CreateRelationship(varFor("r"), varFor("x"), relType("R"), varFor("y"), SemanticDirection.INCOMING, None)
          )
        ),
        32.2
      ),
      planDescription(
        id,
        "Create",
        Seq(lhsPD),
        Seq(details(Seq("(x:Label1:Label2)", "(x)<-[r:R]-(y)"))),
        Set("a", "x", "r")
      )
    )

    assertGood(
      attach(
        Create(
          lhsLP,
          Seq(
            CreateNode(varFor("x"), Set(label("Label")), Set.empty, Some(properties)),
            CreateRelationship(
              varFor("r"),
              varFor("x"),
              relType("R"),
              varFor("y"),
              SemanticDirection.INCOMING,
              Some(properties)
            )
          )
        ),
        32.2
      ),
      planDescription(
        id,
        "Create",
        Seq(lhsPD),
        Seq(details(Seq("(x:Label {y: 1, crs: \"cartesian\"})", "(x)<-[r:R {y: 1, crs: \"cartesian\"}]-(y)"))),
        Set("a", "x", "r")
      )
    )

    assertGood(
      attach(
        Create(
          lhsLP,
          Seq(
            CreateNode(
              varFor("x"),
              Set(label("Label")),
              Set(prop(varFor("n"), "label"), literal("LBL")),
              Some(properties)
            )
          )
        ),
        32.2
      ),
      planDescription(
        id,
        "Create",
        Seq(lhsPD),
        Seq(details(Seq("(x:Label:$(n.label):$(\"LBL\") {y: 1, crs: \"cartesian\"})"))),
        Set("a", "x")
      )
    )

    assertGood(
      attach(
        Create(
          lhsLP,
          Seq(
            CreateRelationship(
              varFor("r"),
              varFor("x"),
              relType(prop(varFor("n"), "prop")),
              varFor("y"),
              SemanticDirection.INCOMING,
              Some(properties)
            )
          )
        ),
        32.2
      ),
      planDescription(
        id,
        "Create",
        Seq(lhsPD),
        Seq(details(Seq("(x)<-[r:$all(n.prop) {y: 1, crs: \"cartesian\"}]-(y)"))),
        Set("a", "r")
      )
    )
  }

  test("MergeUniqueNode") {
    assertGood(
      attach(
        MergeUniqueNode(
          varFor("n"),
          LabelToken("Label", LabelId(0)),
          Seq(IndexedProperty(PropertyKeyToken("prop", PropertyKeyId(0)), DoNotGetValue, NODE_TYPE)),
          Seq(stringLiteral("A value")),
          Set.empty,
          IndexOrderNone,
          IndexType.RANGE,
          Seq(PropertyKeyName("p1")(pos) -> stringLiteral("A"), PropertyKeyName("p2")(pos) -> stringLiteral("B")),
          Seq(PropertyKeyName("p3")(pos) -> stringLiteral("C"), PropertyKeyName("p4")(pos) -> stringLiteral("D"))
        ),
        32.2
      ),
      planDescription(
        id,
        "MergeUniqueNode",
        Seq.empty,
        Seq(details(
          Seq(
            "UNIQUE n:Label(prop) WHERE prop = \"A value\"",
            "ON MATCH SET n.p1 = \"A\", n.p2 = \"B\" ON CREATE SET n.p3 = \"C\", n.p4 = \"D\""
          )
        )),
        Set("n")
      )
    )
  }

  test("MergeInto") {
    assertGood(
      attach(
        MergeInto(
          lhsLP,
          varFor("r"),
          varFor("x"),
          OUTGOING,
          relType("R"),
          varFor("y"),
          Seq.empty,
          Seq.empty
        ),
        32.2
      ),
      planDescription(
        id,
        "MergeInto",
        Seq(lhsPD),
        Seq(details(Seq("MERGE (x)-[r:R]->(y)"))),
        Set("r", "x", "y")
      )
    )
    assertGood(
      attach(
        MergeInto(
          lhsLP,
          varFor("r"),
          varFor("x"),
          INCOMING,
          relType("R"),
          varFor("y"),
          Seq.empty,
          Seq.empty
        ),
        32.2
      ),
      planDescription(
        id,
        "MergeInto",
        Seq(lhsPD),
        Seq(details(Seq("MERGE (x)<-[r:R]-(y)"))),
        Set("r", "x", "y")
      )
    )
    assertGood(
      attach(
        MergeInto(
          lhsLP,
          varFor("r"),
          varFor("x"),
          BOTH,
          relType("R"),
          varFor("y"),
          Seq.empty,
          Seq.empty
        ),
        32.2
      ),
      planDescription(
        id,
        "MergeInto",
        Seq(lhsPD),
        Seq(details(Seq("MERGE (x)-[r:R]-(y)"))),
        Set("r", "x", "y")
      )
    )
    assertGood(
      attach(
        MergeInto(
          lhsLP,
          varFor("r"),
          varFor("x"),
          OUTGOING,
          relType("R"),
          varFor("y"),
          Seq(PropertyKeyName("p1")(pos) -> stringLiteral("A"), PropertyKeyName("p2")(pos) -> stringLiteral("B")),
          Seq.empty
        ),
        32.2
      ),
      planDescription(
        id,
        "MergeInto",
        Seq(lhsPD),
        Seq(details(Seq("MERGE (x)-[r:R]->(y) ON MATCH SET r.p1 = \"A\", r.p2 = \"B\""))),
        Set("r", "x", "y")
      )
    )
    assertGood(
      attach(
        MergeInto(
          lhsLP,
          varFor("r"),
          varFor("x"),
          OUTGOING,
          relType("R"),
          varFor("y"),
          Seq.empty,
          Seq(PropertyKeyName("p1")(pos) -> stringLiteral("A"), PropertyKeyName("p2")(pos) -> stringLiteral("B"))
        ),
        32.2
      ),
      planDescription(
        id,
        "MergeInto",
        Seq(lhsPD),
        Seq(details(Seq("MERGE (x)-[r:R]->(y) ON CREATE SET r.p1 = \"A\", r.p2 = \"B\""))),
        Set("r", "x", "y")
      )
    )
    assertGood(
      attach(
        MergeInto(
          lhsLP,
          varFor("r"),
          varFor("x"),
          OUTGOING,
          relType("R"),
          varFor("y"),
          Seq(PropertyKeyName("p1")(pos) -> stringLiteral("A"), PropertyKeyName("p2")(pos) -> stringLiteral("B")),
          Seq(PropertyKeyName("p3")(pos) -> stringLiteral("C"), PropertyKeyName("p4")(pos) -> stringLiteral("D"))
        ),
        32.2
      ),
      planDescription(
        id,
        "MergeInto",
        Seq(lhsPD),
        Seq(details(
          Seq("MERGE (x)-[r:R]->(y) ON MATCH SET r.p1 = \"A\", r.p2 = \"B\" ON CREATE SET r.p3 = \"C\", r.p4 = \"D\"")
        )),
        Set("r", "x", "y")
      )
    )
  }

  test("Merge") {
    val properties = MapExpression(Seq(
      (key("x"), number("1")),
      (key("y"), stringLiteral("two"))
    ))(pos)

    assertGood(
      attach(
        Merge(
          lhsLP,
          Seq(CreateNode(varFor("x"), Set.empty, Set.empty, None)),
          Seq(CreateRelationship(
            varFor("r"),
            varFor("x"),
            relType("R"),
            varFor("y"),
            SemanticDirection.INCOMING,
            None
          )),
          Seq.empty,
          Seq.empty,
          Set.empty
        ),
        32.2
      ),
      planDescription(id, "Merge", Seq(lhsPD), Seq(details(Seq("CREATE (x), (x)<-[r:R]-(y)"))), Set("a"))
    )

    assertGood(
      attach(
        Merge(
          lhsLP,
          Seq(CreateNode(varFor("x"), Set(label("L")), Set.empty, None)),
          Seq.empty,
          Seq(SetLabelPattern(varFor("x"), Seq(label("NEW")), Seq.empty)),
          Seq.empty,
          Set.empty
        ),
        32.2
      ),
      planDescription(
        id,
        "Merge",
        Seq(lhsPD),
        Seq(details(Seq("CREATE (x:L)", "ON MATCH SET x:NEW"))),
        Set("a")
      )
    )

    assertGood(
      attach(
        Merge(
          lhsLP,
          Seq(CreateNode(varFor("x"), Set(label("L")), Set.empty, None)),
          Seq.empty,
          Seq.empty,
          Seq(SetLabelPattern(varFor("x"), Seq(label("NEW")), Seq.empty)),
          Set.empty
        ),
        32.2
      ),
      planDescription(
        id,
        "Merge",
        Seq(lhsPD),
        Seq(details(Seq("CREATE (x:L)", "ON CREATE SET x:NEW"))),
        Set("a")
      )
    )

    assertGood(
      attach(
        Merge(
          lhsLP,
          Seq(CreateNode(varFor("x"), Set(label("L")), Set.empty, None)),
          Seq.empty,
          Seq(SetLabelPattern(varFor("x"), Seq(label("ON_MATCH")), Seq.empty)),
          Seq(SetLabelPattern(varFor("x"), Seq(label("ON_CREATE")), Seq.empty)),
          Set.empty
        ),
        32.2
      ),
      planDescription(
        id,
        "Merge",
        Seq(lhsPD),
        Seq(details(Seq("CREATE (x:L)", "ON MATCH SET x:ON_MATCH", "ON CREATE SET x:ON_CREATE"))),
        Set("a")
      )
    )

    assertGood(
      attach(
        Merge(
          lhsLP,
          Seq(CreateNode(varFor("x"), Set.empty, Set.empty, Some(properties))),
          Seq(CreateRelationship(
            varFor("r"),
            varFor("x"),
            relType("R"),
            varFor("y"),
            SemanticDirection.INCOMING,
            Some(properties)
          )),
          Seq.empty,
          Seq.empty,
          Set.empty
        ),
        32.2
      ),
      planDescription(
        id,
        "Merge",
        Seq(lhsPD),
        Seq(details(Seq("CREATE (x: {x: 1, y: \"two\"}), (x)<-[r:R {x: 1, y: \"two\"}]-(y)"))),
        Set("a")
      )
    )

    assertGood(
      attach(
        Merge(
          lhsLP,
          Seq(CreateNode(varFor("x"), Set(label("L")), Set.empty, Some(properties))),
          Seq(CreateRelationship(
            varFor("r"),
            varFor("x"),
            relType("R"),
            varFor("y"),
            SemanticDirection.INCOMING,
            Some(properties)
          )),
          Seq.empty,
          Seq.empty,
          Set.empty
        ),
        32.2
      ),
      planDescription(
        id,
        "Merge",
        Seq(lhsPD),
        Seq(details(Seq("CREATE (x:L {x: 1, y: \"two\"}), (x)<-[r:R {x: 1, y: \"two\"}]-(y)"))),
        Set("a")
      )
    )

    assertGood(
      attach(
        Merge(
          lhsLP,
          Seq.empty,
          Seq(CreateRelationship(
            varFor("r"),
            varFor("x"),
            relType("R"),
            varFor("y"),
            SemanticDirection.INCOMING,
            None
          )),
          Seq.empty,
          Seq.empty,
          Set(varFor("x"), varFor("y"))
        ),
        32.2
      ),
      planDescription(
        id,
        "LockingMerge",
        Seq(lhsPD),
        Seq(details(Seq("CREATE (x)<-[r:R]-(y)", "LOCK(x, y)"))),
        Set("a")
      )
    )

    assertGood(
      attach(
        Merge(
          lhsLP,
          Seq(CreateNode(varFor("x"), Set(label("L")), Set.empty, None)),
          Seq.empty,
          Seq(SetNodePropertiesPattern(varFor("x"), Seq((key("p1"), number("1")), (key("p2"), number("2"))))),
          Seq.empty,
          Set.empty
        ),
        32.2
      ),
      planDescription(
        id,
        "Merge",
        Seq(lhsPD),
        Seq(details(Seq("CREATE (x:L)", "ON MATCH SET x.p1 = 1, x.p2 = 2"))),
        Set("a")
      )
    )

  }

  test("foreach") {
    assertGood(
      attach(
        Foreach(
          lhsLP,
          varFor("i"),
          parameter("p", CTList(CTInteger)),
          Seq(SetNodePropertyPattern(varFor("x"), PropertyKeyName("prop")(pos), stringLiteral("foo")))
        ),
        32.2
      ),
      planDescription(id, "Foreach", Seq(lhsPD), Seq(details(Seq("i IN $p", "SET x.prop = \"foo\""))), Set("a"))
    )

    assertGood(
      attach(
        Foreach(
          lhsLP,
          varFor("i"),
          parameter("p", CTList(CTInteger)),
          Seq(RemoveLabelPattern(varFor("x"), Seq(label("L"), label("M")), Seq.empty))
        ),
        32.2
      ),
      planDescription(id, "Foreach", Seq(lhsPD), Seq(details(Seq("i IN $p", "REMOVE x:L:M"))), Set("a"))
    )

    assertGood(
      attach(
        Foreach(
          lhsLP,
          varFor("i"),
          parameter("p", CTList(CTInteger)),
          Seq(org.neo4j.cypher.internal.ir.DeleteExpression(varFor("x"), detachDelete = true))
        ),
        32.2
      ),
      planDescription(id, "Foreach", Seq(lhsPD), Seq(details(Seq("i IN $p", "DETACH DELETE x"))), Set("a"))
    )
    assertGood(
      attach(
        Foreach(
          lhsLP,
          varFor("i"),
          parameter("p", CTList(CTInteger)),
          Seq(org.neo4j.cypher.internal.ir.DeleteExpression(varFor("x"), detachDelete = false))
        ),
        32.2
      ),
      planDescription(id, "Foreach", Seq(lhsPD), Seq(details(Seq("i IN $p", "DELETE x"))), Set("a"))
    )
  }

  test("Delete") {
    assertGood(
      attach(DeleteExpression(lhsLP, prop("x", "prop")), 34.5),
      planDescription(id, "Delete", Seq(lhsPD), Seq(details("x.prop")), Set("a"))
    )

    assertGood(
      attach(DeleteNode(lhsLP, varFor("x")), 34.5),
      planDescription(id, "Delete", Seq(lhsPD), Seq(details("x")), Set("a"))
    )

    assertGood(
      attach(DeletePath(lhsLP, varFor("x")), 34.5),
      planDescription(id, "Delete", Seq(lhsPD), Seq(details("x")), Set("a"))
    )

    assertGood(
      attach(DeleteRelationship(lhsLP, varFor("x")), 34.5),
      planDescription(id, "Delete", Seq(lhsPD), Seq(details("x")), Set("a"))
    )

    assertGood(
      attach(DetachDeleteExpression(lhsLP, varFor("x")), 34.5),
      planDescription(id, "DetachDelete", Seq(lhsPD), Seq(details("x")), Set("a"))
    )

    assertGood(
      attach(DetachDeleteNode(lhsLP, varFor("x")), 34.5),
      planDescription(id, "DetachDelete", Seq(lhsPD), Seq(details("x")), Set("a"))
    )

    assertGood(
      attach(DetachDeletePath(lhsLP, varFor("x")), 34.5),
      planDescription(id, "DetachDelete", Seq(lhsPD), Seq(details("x")), Set("a"))
    )
  }

  test("Eager") {
    assertGood(
      attach(Eager(lhsLP, ListSet.empty), 34.5),
      planDescription(id, "Eager", Seq(lhsPD), Seq.empty, Set("a"))
    )

    assertGood(
      attach(Eager(lhsLP, ListSet(EagernessReason.Unknown)), 34.5),
      planDescription(id, "Eager", Seq(lhsPD), Seq.empty, Set("a"))
    )

    assertGood(
      attach(Eager(lhsLP, ListSet(EagernessReason.UpdateStrategyEager)), 34.5),
      planDescription(id, "Eager", Seq(lhsPD), Seq(details(Seq("updateStrategy=eager"))), Set("a"))
    )

    assertGood(
      attach(Eager(lhsLP, ListSet(EagernessReason.LabelReadRemoveConflict(label("Foo")))), 34.5),
      planDescription(
        id,
        "Eager",
        Seq(lhsPD),
        Seq(details(Seq("read/remove conflict for label: Foo"))),
        Set("a")
      )
    )

    {
      val reason = EagernessReason.LabelReadRemoveConflict(label("Foo")).withConflict(Conflict(Id(1), Id(2)))
      assertGood(
        attach(Eager(lhsLP, ListSet(reason)), 34.5),
        planDescription(
          id,
          "Eager",
          Seq(lhsPD),
          Seq(details(Seq("read/remove conflict for label: Foo (Operator: 1 vs 2)"))),
          Set("a")
        )
      )
    }

    assertGood(
      attach(Eager(lhsLP, ListSet(EagernessReason.LabelReadSetConflict(label("Foo")))), 34.5),
      planDescription(id, "Eager", Seq(lhsPD), Seq(details(Seq("read/set conflict for label: Foo"))), Set("a"))
    )

    assertGood(
      attach(Eager(lhsRelLP, ListSet(EagernessReason.TypeReadSetConflict(relType("Foo")))), 34.5),
      planDescription(
        id,
        "Eager",
        Seq(lhsRelPD),
        Seq(details(Seq("read/set conflict for relationship type: Foo"))),
        Set("r", "a", "b")
      )
    )

    assertGood(
      attach(Eager(lhsLP, ListSet(EagernessReason.ReadDeleteConflict("b"))), 34.5),
      planDescription(
        id,
        "Eager",
        Seq(lhsPD),
        Seq(details(Seq("read/delete conflict for variable: b"))),
        Set("a")
      )
    )

    assertGood(
      attach(
        Eager(
          lhsLP,
          ListSet(EagernessReason.ReadDeleteConflict("b"), EagernessReason.LabelReadSetConflict(label("Foo")))
        ),
        34.5
      ),
      planDescription(
        id,
        "Eager",
        Seq(lhsPD),
        Seq(details(Seq("read/delete conflict for variable: b", "read/set conflict for label: Foo"))),
        Set("a")
      )
    )

    assertGood(
      attach(
        Eager(
          lhsRelLP,
          ListSet(EagernessReason.ReadDeleteConflict("r"), EagernessReason.TypeReadSetConflict(relType("Foo")))
        ),
        34.5
      ),
      planDescription(
        id,
        "Eager",
        Seq(lhsRelPD),
        Seq(details(Seq("read/delete conflict for variable: r", "read/set conflict for relationship type: Foo"))),
        Set("r", "a", "b")
      )
    )

    {
      val reason1 = EagernessReason.ReadDeleteConflict("b").withConflict(Conflict(Id(1), Id(2)))
      val reason2 = EagernessReason.LabelReadSetConflict(label("Foo")).withConflict(Conflict(Id(3), Id(4)))
      assertGood(
        attach(
          Eager(
            lhsLP,
            ListSet(reason1, reason2)
          ),
          34.5
        ),
        planDescription(
          id,
          "Eager",
          Seq(lhsPD),
          Seq(details(Seq(
            "read/delete conflict for variable: b (Operator: 1 vs 2)",
            "read/set conflict for label: Foo (Operator: 3 vs 4)"
          ))),
          Set("a")
        )
      )
    }

    {
      val reason1 = EagernessReason.ReadDeleteConflict("r").withConflict(Conflict(Id(1), Id(2)))
      val reason2 = EagernessReason.TypeReadSetConflict(relType("Foo")).withConflict(Conflict(Id(3), Id(4)))
      assertGood(
        attach(
          Eager(
            lhsRelLP,
            ListSet(reason1, reason2)
          ),
          34.5
        ),
        planDescription(
          id,
          "Eager",
          Seq(lhsRelPD),
          Seq(details(Seq(
            "read/delete conflict for variable: r (Operator: 1 vs 2)",
            "read/set conflict for relationship type: Foo (Operator: 3 vs 4)"
          ))),
          Set("r", "a", "b")
        )
      )
    }
    {
      val reason = {
        EagernessReason.Summarized(Map(
          EagernessReason.ReadDeleteConflict("r") -> EagernessReason.SummaryEntry(Conflict(Id(1), Id(2)), 123),
          EagernessReason.TypeReadSetConflict(relType("REL")) -> EagernessReason.SummaryEntry(
            Conflict(Id(3), Id(4)),
            321
          )
        ))
      }
      assertGood(
        attach(
          Eager(
            lhsRelLP,
            ListSet(reason)
          ),
          34.5
        ),
        planDescription(
          id,
          "Eager",
          Seq(lhsRelPD),
          Seq(details(Seq(
            "read/delete conflict for variable: r (Operator: 1 vs 2, and 122 more conflicting operators)",
            "read/set conflict for relationship type: REL (Operator: 3 vs 4, and 320 more conflicting operators)"
          ))),
          Set("r", "a", "b")
        )
      )
    }
  }

  test("EmptyResult") {
    assertGood(
      attach(EmptyResult(lhsLP), 34.5),
      planDescription(id, "EmptyResult", Seq(lhsPD), Seq.empty, Set("a"))
    )
  }

  test("ErrorPlan") {
    assertGood(
      attach(ErrorPlan(lhsLP, new Exception("Exception")), 12.5),
      planDescription(id, "Error", Seq(lhsPD), Seq.empty, Set("a"))
    )
  }

  test("Expand") {
    assertGood(
      attach(Expand(lhsLP, varFor("a"), OUTGOING, Seq.empty, varFor("  UNNAMED4"), varFor("r1"), ExpandAll), 95.0),
      planDescription(
        id,
        "Expand(All)",
        Seq(lhsPD),
        Seq(details(s"(a)-[r1]->(${anonVar("4")})")),
        Set("a", anonVar("4"), "r1")
      )
    )

    assertGood(
      attach(Expand(lhsLP, varFor("a"), INCOMING, Seq(relType("R")), varFor("y"), varFor("r1"), ExpandAll), 95.0),
      planDescription(id, "Expand(All)", Seq(lhsPD), Seq(details("(a)<-[r1:R]-(y)")), Set("a", "y", "r1"))
    )

    assertGood(
      attach(
        Expand(lhsLP, varFor("a"), BOTH, Seq(relType("R1"), relType("R2")), varFor("y"), varFor("r1"), ExpandAll),
        95.0
      ),
      planDescription(id, "Expand(All)", Seq(lhsPD), Seq(details("(a)-[r1:R1|R2]-(y)")), Set("a", "y", "r1"))
    )

    assertGood(
      attach(Expand(lhsLP, varFor("a"), OUTGOING, Seq.empty, varFor("y"), varFor("r1"), ExpandInto), 113.0),
      planDescription(id, "Expand(Into)", Seq(lhsPD), Seq(details("(a)-[r1]->(y)")), Set("a", "y", "r1"))
    )

    assertGood(
      attach(Expand(lhsLP, varFor("a"), INCOMING, Seq(relType("R")), varFor("y"), varFor("r1"), ExpandInto), 113.0),
      planDescription(id, "Expand(Into)", Seq(lhsPD), Seq(details("(a)<-[r1:R]-(y)")), Set("a", "y", "r1"))
    )

    assertGood(
      attach(
        Expand(lhsLP, varFor("a"), BOTH, Seq(relType("R1"), relType("R2")), varFor("y"), varFor("r1"), ExpandInto),
        113.0
      ),
      planDescription(id, "Expand(Into)", Seq(lhsPD), Seq(details("(a)-[r1:R1|R2]-(y)")), Set("a", "y", "r1"))
    )

    assertGood(
      attach(
        Expand(
          lhsLP,
          varFor("a"),
          BOTH,
          Seq(relType("R1"), relType("R2")),
          varFor("y"),
          varFor("  UNNAMED1"),
          ExpandInto
        ),
        113.0
      ),
      planDescription(
        id,
        "Expand(Into)",
        Seq(lhsPD),
        Seq(details(s"(a)-[${anonVar("1")}:R1|R2]-(y)")),
        Set("a", "y", anonVar("1"))
      )
    )

    assertGood(
      attach(Expand(lhsLP, varFor("a"), OUTGOING, Seq.empty, None, Some(varFor("r1")), ExpandAll), 95.0),
      planDescription(
        id,
        "Expand(All)",
        Seq(lhsPD),
        Seq(details(s"(a)-[r1]->()")),
        Set("a", "r1")
      )
    )

    assertGood(
      attach(Expand(lhsLP, varFor("a"), OUTGOING, Seq.empty, None, None, ExpandAll), 95.0),
      planDescription(
        id,
        "Expand(All)",
        Seq(lhsPD),
        Seq(details(s"(a)-->()")),
        Set("a")
      )
    )
  }

  test("Limit") {
    assertGood(
      attach(Limit(lhsLP, number("1")), 113.0),
      planDescription(id, "Limit", Seq(lhsPD), Seq(details("1")), Set("a"))
    )
  }

  test("CachedProperties") {
    assertGood(
      attach(CacheProperties(lhsLP, Set(cachedProp("n", "prop"))), 113.0),
      planDescription(id, "CacheProperties", Seq(lhsPD), Seq(details("cache[n.prop]")), Set("a"))
    )

    assertGood(
      attach(CacheProperties(lhsLP, Set(prop("n", "prop1"), prop("n", "prop2"))), 113.0),
      planDescription(id, "CacheProperties", Seq(lhsPD), Seq(details(Seq("n.prop1", "n.prop2"))), Set("a"))
    )
  }

  test("OptionalExpand") {
    val predicate1 = Equals(varFor("to"), parameter("  AUTOINT2", CTInteger))(pos)
    val predicate2 = LessThan(varFor("a"), number("2002"))(pos)

    // Without predicate
    assertGood(
      attach(
        OptionalExpand(
          lhsLP,
          varFor("a"),
          INCOMING,
          Seq(relType("R")),
          Some(varFor("  UNNAMED5")),
          Some(varFor("r")),
          ExpandAll,
          None
        ),
        12.0
      ),
      planDescription(
        id,
        "OptionalExpand(All)",
        Seq(lhsPD),
        Seq(details(s"(a)<-[r:R]-(${anonVar("5")})")),
        Set("a", anonVar("5"), "r")
      )
    )

    // With predicate and no relationship types
    assertGood(
      attach(
        OptionalExpand(
          lhsLP,
          varFor("a"),
          OUTGOING,
          Seq(),
          Some(varFor("to")),
          Some(varFor("r")),
          ExpandAll,
          Some(predicate1)
        ),
        12.0
      ),
      planDescription(
        id,
        "OptionalExpand(All)",
        Seq(lhsPD),
        Seq(details("(a)-[r]->(to) WHERE to = $autoint_2")),
        Set("a", "to", "r")
      )
    )

    // With predicate and relationship types
    assertGood(
      attach(
        OptionalExpand(
          lhsLP,
          varFor("a"),
          BOTH,
          Seq(relType("R")),
          Some(varFor("to")),
          Some(varFor("r")),
          ExpandAll,
          Some(And(predicate1, predicate2)(pos))
        ),
        12.0
      ),
      planDescription(
        id,
        "OptionalExpand(All)",
        Seq(lhsPD),
        Seq(details("(a)-[r:R]-(to) WHERE to = $autoint_2 AND a < 2002")),
        Set("a", "to", "r")
      )
    )

    // With multiple relationship types
    assertGood(
      attach(
        OptionalExpand(
          lhsLP,
          varFor("a"),
          INCOMING,
          Seq(relType("R1"), relType("R2")),
          Some(varFor("to")),
          Some(varFor("r")),
          ExpandAll,
          None
        ),
        12.0
      ),
      planDescription(
        id,
        "OptionalExpand(All)",
        Seq(lhsPD),
        Seq(details("(a)<-[r:R1|R2]-(to)")),
        Set("a", "to", "r")
      )
    )

    // with removed end node
    assertGood(
      attach(
        OptionalExpand(
          lhsLP,
          varFor("a"),
          INCOMING,
          Seq(relType("R")),
          None,
          Some(varFor("r")),
          ExpandAll,
          None
        ),
        12.0
      ),
      planDescription(
        id,
        "OptionalExpand(All)",
        Seq(lhsPD),
        Seq(details(s"(a)<-[r:R]-()")),
        Set("a", "r")
      )
    )

    // with removed relationship
    assertGood(
      attach(
        OptionalExpand(
          lhsLP,
          varFor("a"),
          INCOMING,
          Seq(relType("R")),
          Some(varFor("  UNNAMED5")),
          None,
          ExpandAll,
          None
        ),
        12.0
      ),
      planDescription(
        id,
        "OptionalExpand(All)",
        Seq(lhsPD),
        Seq(details(s"(a)<-[:R]-(${anonVar("5")})")),
        Set("a", anonVar("5"))
      )
    )

    // with removed end node and relationship
    assertGood(
      attach(
        OptionalExpand(
          lhsLP,
          varFor("a"),
          INCOMING,
          Seq(relType("R")),
          None,
          None,
          ExpandAll,
          None
        ),
        12.0
      ),
      planDescription(
        id,
        "OptionalExpand(All)",
        Seq(lhsPD),
        Seq(details(s"(a)<-[:R]-()")),
        Set("a")
      )
    )
  }

  test("Projection") {
    val pathExpression = PathExpression(NodePathStep(
      varFor("c"),
      SingleRelationshipPathStep(varFor("  UNNAMED42"), OUTGOING, Some(varFor("  UNNAMED32")), NilPathStep()(pos))(pos)
    )(pos))(pos)

    assertGood(
      attach(Projection(lhsLP, Map(varFor("x") -> varFor("y"))), 2345.0),
      planDescription(id, "Projection", Seq(lhsPD), Seq(details("y AS x")), Set("a", "x"))
    )

    assertGood(
      attach(Projection(lhsLP, Map(varFor("x") -> pathExpression)), 2345.0),
      planDescription(
        id,
        "Projection",
        Seq(lhsPD),
        Seq(details(s"(c)-[${anonVar("42")}]->(${anonVar("32")}) AS x")),
        Set("a", "x")
      )
    )

    assertGood(
      attach(
        Projection(lhsLP, Map(varFor("x") -> varFor("  UNNAMED42"), varFor("n.prop") -> prop("n", "prop"))),
        2345.0
      ),
      planDescription(
        id,
        "Projection",
        Seq(lhsPD),
        Seq(details(Seq(s"${anonVar("42")} AS x", "n.prop AS `n.prop`"))),
        Set("a", "x", "`n.prop`")
      )
    )

    // Projection should show up in the order they were specified in
    assertGood(
      attach(
        Projection(lhsLP, Map(varFor("n.prop") -> prop("n", "prop"), varFor("x") -> varFor("y"))),
        2345.0
      ),
      planDescription(
        id,
        "Projection",
        Seq(lhsPD),
        Seq(details(Seq("n.prop AS `n.prop`", "y AS x"))),
        Set("a", "x", "`n.prop`")
      )
    )
  }

  test("Selection") {
    val predicate1 = In(varFor("x"), parameter("  AUTOLIST1", CTList(CTInteger)))(pos)
    val predicate2 = LessThan(prop("a", "prop1"), number("2002"))(pos)
    val predicate3 = GreaterThanOrEqual(cachedProp("a", "prop1"), number("1001"))(pos)
    val predicate4 = AndedPropertyInequalities(varFor("a"), prop("a", "prop"), NonEmptyList(predicate2, predicate3))

    assertGood(
      attach(Selection(Seq(predicate1, predicate4), lhsLP), 2345.0),
      planDescription(
        id,
        "Filter",
        Seq(lhsPD),
        Seq(details("x IN $autolist_1 AND a.prop1 < 2002 AND cache[a.prop1] >= 1001")),
        Set("a")
      )
    )
  }

  test("Skip") {
    assertGood(
      attach(Skip(lhsLP, number("78")), 2345.0),
      planDescription(id, "Skip", Seq(lhsPD), Seq(details("78")), Set("a"))
    )
  }

  test("FindShortestPaths") {
    val relPredicate =
      VariablePredicate(varFor("r"), Equals(prop("r", "prop"), parameter("  AUTOSTRING1", CTString))(pos))

    // with(out) relationship types
    // length variations

    // simple length
    assertGood(
      attach(
        FindShortestPaths(
          lhsLP,
          ShortestRelationshipPattern(
            None,
            PatternRelationship(
              varFor("r"),
              (varFor("  UNNAMED23"), varFor("y")),
              SemanticDirection.BOTH,
              Seq.empty,
              SimplePatternLength
            ),
            single = true
          )(null),
          Seq.empty,
          Seq.empty,
          Seq.empty
        ),
        2345.0
      ),
      planDescription(
        id,
        "ShortestPath",
        Seq(lhsPD),
        Seq(details(s"(${anonVar("23")})-[r]-(y)")),
        Set("r", "a", anonVar("23"), "y")
      )
    )

    // fixed length of 1
    assertGood(
      attach(
        FindShortestPaths(
          lhsLP,
          ShortestRelationshipPattern(
            None,
            PatternRelationship(
              varFor("r"),
              (varFor("  UNNAMED23"), varFor("y")),
              SemanticDirection.BOTH,
              Seq.empty,
              VarPatternLength(1, Some(1))
            ),
            single = true
          )(null),
          Seq.empty,
          Seq.empty,
          Seq.empty
        ),
        2345.0
      ),
      planDescription(
        id,
        "ShortestPath",
        Seq(lhsPD),
        Seq(details(s"(${anonVar("23")})-[r]-(y)")),
        Set("r", "a", anonVar("23"), "y")
      )
    )

    // without: predicates, path name, relationship type
    assertGood(
      attach(
        FindShortestPaths(
          lhsLP,
          ShortestRelationshipPattern(
            None,
            PatternRelationship(
              varFor("r"),
              (varFor("  UNNAMED23"), varFor("y")),
              SemanticDirection.BOTH,
              Seq.empty,
              VarPatternLength(2, Some(4))
            ),
            single = true
          )(null),
          Seq.empty,
          Seq.empty,
          Seq.empty
        ),
        2345.0
      ),
      planDescription(
        id,
        "ShortestPath",
        Seq(lhsPD),
        Seq(details(s"(${anonVar("23")})-[r*2..4]-(y)")),
        Set("r", "a", anonVar("23"), "y")
      )
    )

    // with: predicates, path name, relationship type
    assertGood(
      attach(
        FindShortestPaths(
          lhsLP,
          ShortestRelationshipPattern(
            Some(varFor("  UNNAMED12")),
            PatternRelationship(
              varFor("r"),
              (varFor("a"), varFor("y")),
              SemanticDirection.BOTH,
              Seq(relType("R")),
              VarPatternLength(2, Some(4))
            ),
            single = true
          )(null),
          Seq.empty,
          Seq(relPredicate),
          Seq.empty
        ),
        2345.0
      ),
      planDescription(
        id,
        "ShortestPath",
        Seq(lhsPD),
        Seq(details(
          s"${anonVar("12")} = (a)-[r:R*2..4]-(y) WHERE all(r IN relationships(${anonVar("12")}) WHERE r.prop = $$autostring_1)"
        )),
        Set("r", "a", "y", anonVar("12"))
      )
    )

    // with: predicates, UNNAMED variables, relationship type, unbounded max length
    assertGood(
      attach(
        FindShortestPaths(
          lhsLP,
          ShortestRelationshipPattern(
            Some(varFor("  UNNAMED12")),
            PatternRelationship(
              varFor("r"),
              (varFor("a"), varFor("  UNNAMED2")),
              SemanticDirection.BOTH,
              Seq(relType("R")),
              VarPatternLength(2, None)
            ),
            single = true
          )(null),
          Seq.empty,
          Seq(relPredicate),
          Seq.empty
        ),
        2345.0
      ),
      planDescription(
        id,
        "ShortestPath",
        Seq(lhsPD),
        Seq(details(
          s"${anonVar("12")} = (a)-[r:R*2..]-(${anonVar("2")}) WHERE all(r IN relationships(${anonVar("12")}) WHERE r.prop = $$autostring_1)"
        )),
        Set("r", "a", anonVar("2"), anonVar("12"))
      )
    )

    // with: predicates, UNNAMED variables, relationship type, unbounded max length
    assertGood(
      attach(
        FindShortestPaths(
          lhsLP,
          ShortestRelationshipPattern(
            Some(varFor("  UNNAMED12")),
            PatternRelationship(
              varFor("r"),
              (varFor("a"), varFor("  UNNAMED2")),
              SemanticDirection.BOTH,
              Seq(relType("R")),
              VarPatternLength(1, None)
            ),
            single = true
          )(null),
          Seq.empty,
          Seq(relPredicate),
          Seq.empty
        ),
        2345.0
      ),
      planDescription(
        id,
        "ShortestPath",
        Seq(lhsPD),
        Seq(details(
          s"${anonVar("12")} = (a)-[r:R*]-(${anonVar("2")}) WHERE all(r IN relationships(${anonVar("12")}) WHERE r.prop = $$autostring_1)"
        )),
        Set("r", "a", anonVar("2"), anonVar("12"))
      )
    )
  }

  test("StatefulShortestPath") {
    val solvedExpressionStr = "SHORTEST 5 PATHS (a)-[`  UNNAMED0`]->*(`  b@45`)"
    val nfa = {
      val builder = new NFABuilder(varFor("a"))
      builder
        .setFinalState(builder.getLastState)
        .build()
    }
    assertGood(
      attach(
        StatefulShortestPath(
          lhsLP,
          varFor("a"),
          varFor("b"),
          nfa,
          ExpandAll,
          None,
          Set.empty,
          Set.empty,
          Set.empty,
          Set.empty,
          Selector.Shortest(CountInteger(5)),
          solvedExpressionStr,
          reverseGroupVariableProjections = false,
          LengthBounds.none,
          TraversalPathMode.Trail
        ),
        2345.0
      ),
      planDescription(
        id,
        "StatefulShortestPath(All, Trail)",
        Seq(lhsPD),
        Seq(details(
          """SHORTEST 5 PATHS (a)-[`anon_0`]->*(`b`)
            |        expanding from: a""".stripMargin
        )),
        Set("a")
      )
    )
    assertGood(
      attach(
        StatefulShortestPath(
          lhsLP,
          varFor("a"),
          varFor("b"),
          nfa,
          ExpandInto,
          None,
          Set.empty,
          Set.empty,
          Set.empty,
          Set.empty,
          Selector.Shortest(CountInteger(5)),
          solvedExpressionStr,
          reverseGroupVariableProjections = false,
          LengthBounds.none,
          TraversalPathMode.Trail
        ),
        2345.0
      ),
      planDescription(
        id,
        "StatefulShortestPath(Into, Trail)",
        Seq(lhsPD),
        Seq(details(
          """SHORTEST 5 PATHS (a)-[`anon_0`]->*(`b`)""".stripMargin
        )),
        Set("a")
      )
    )
    assertGood(
      attach(
        StatefulShortestPath(
          lhsLP,
          varFor("a"),
          varFor("b"),
          nfa,
          ExpandInto,
          None,
          Set.empty,
          Set.empty,
          Set.empty,
          Set.empty,
          Selector.Shortest(CountInteger(5)),
          solvedExpressionStr,
          reverseGroupVariableProjections = false,
          LengthBounds.none,
          TraversalPathMode.Walk
        ),
        2345.0
      ),
      planDescription(
        id,
        "StatefulShortestPath(Into, Walk)",
        Seq(lhsPD),
        Seq(details(
          """SHORTEST 5 PATHS (a)-[`anon_0`]->*(`b`)""".stripMargin
        )),
        Set("a")
      )
    )
  }

  test("StatefulShortestPath with predicates") {
    val solvedExpressionStr = "SHORTEST 5 PATHS (a)-[r1]->(b)-[r2]->(c)"
    val nfa = {
      val builder = new NFABuilder(v"a")
      builder
        .addTransition(
          builder.getLastState,
          NFA.NodeJuxtapositionTransition(builder.addAndGetState(
            v"b",
            Some(VariablePredicate(v"b", propEquality("b", "prop", 5)))
          ).id)
        )
        .addTransition(
          builder.getLastState,
          NFA.RelationshipExpansionTransition(
            NFA.RelationshipExpansionPredicate(
              v"r",
              Some(VariablePredicate(v"r", propEquality("r", "prop", 5))),
              Seq.empty,
              SemanticDirection.OUTGOING
            ),
            builder.addAndGetState(
              v"c",
              Some(VariablePredicate(v"c", propEquality("c", "prop", 5)))
            ).id
          )
        )
        .setFinalState(builder.getLastState)
        .build()
    }
    assertGood(
      attach(
        StatefulShortestPath(
          lhsLP,
          varFor("a"),
          varFor("d"),
          nfa,
          ExpandAll,
          Some(ands(
            propGreaterThan("b", "prop", 10),
            propGreaterThan("r", "prop", 10),
            propGreaterThan("c", "prop", 10)
          )),
          Set.empty,
          Set.empty,
          Set.empty,
          Set.empty,
          Selector.Shortest(CountInteger(5)),
          solvedExpressionStr,
          reverseGroupVariableProjections = false,
          LengthBounds.none,
          TraversalPathMode.Trail
        ),
        2345.0
      ),
      planDescription(
        id,
        "StatefulShortestPath(All, Trail)",
        Seq(lhsPD),
        Seq(details(
          s"""$solvedExpressionStr
             |        expanding from: a
             |    inlined predicates: b.prop = 5
             |                        c.prop = 5
             |                        r.prop = 5
             |non-inlined predicates: b.prop > 10
             |                        c.prop > 10
             |                        r.prop > 10""".stripMargin
        )),
        Set("a")
      )
    )
  }

  test("Optional") {
    assertGood(
      attach(Optional(lhsLP, Set(varFor("a"))), 113.0),
      planDescription(id, "Optional", Seq(lhsPD), Seq(details("a")), Set("a"))
    )
  }

  test("Anti") {
    assertGood(attach(Anti(lhsLP), 113.0), planDescription(id, "Anti", Seq(lhsPD), Seq.empty, Set("a")))
  }

  test("ProjectEndpoints") {
    assertGood(
      attach(
        ProjectEndpoints(
          lhsLP,
          varFor("r"),
          varFor("start"),
          startInScope = true,
          varFor("end"),
          endInScope = true,
          Seq.empty,
          direction = SemanticDirection.OUTGOING,
          VarPatternLength(1, Some(1))
        ),
        234.2
      ),
      planDescription(
        id,
        "ProjectEndpoints",
        Seq(lhsPD),
        Seq(details("(start)-[r]->(end)")),
        Set("a", "start", "r", "end")
      )
    )

    assertGood(
      attach(
        ProjectEndpoints(
          lhsLP,
          varFor("r"),
          varFor("start"),
          startInScope = true,
          varFor("end"),
          endInScope = true,
          Seq.empty,
          direction = SemanticDirection.OUTGOING,
          SimplePatternLength
        ),
        234.2
      ),
      planDescription(
        id,
        "ProjectEndpoints",
        Seq(lhsPD),
        Seq(details("(start)-[r]->(end)")),
        Set("a", "start", "r", "end")
      )
    )

    assertGood(
      attach(
        ProjectEndpoints(
          lhsLP,
          varFor("r"),
          varFor("start"),
          startInScope = true,
          varFor("end"),
          endInScope = true,
          Seq.empty,
          direction = SemanticDirection.INCOMING,
          VarPatternLength(1, None)
        ),
        234.2
      ),
      planDescription(
        id,
        "ProjectEndpoints",
        Seq(lhsPD),
        Seq(details("(start)<-[r*]-(end)")),
        Set("a", "start", "r", "end")
      )
    )

    assertGood(
      attach(
        ProjectEndpoints(
          lhsLP,
          varFor("r"),
          varFor("start"),
          startInScope = true,
          varFor("end"),
          endInScope = true,
          Seq(relType("R")),
          direction = SemanticDirection.BOTH,
          VarPatternLength(1, Some(3))
        ),
        234.2
      ),
      planDescription(
        id,
        "ProjectEndpoints",
        Seq(lhsPD),
        Seq(details("(start)-[r:R*..3]-(end)")),
        Set("a", "start", "r", "end")
      )
    )
  }

  test("VarExpand") {
    val predicate = (varName: String) => Equals(prop(varName, "prop"), parameter("  AUTODOUBLE1", CTFloat))(pos)
    val nodePredicate = VariablePredicate(varFor("x"), predicate("x"))
    val nodePredicate2 = VariablePredicate(varFor("x2"), predicate("x2"))
    val relationshipPredicate = VariablePredicate(varFor("r"), predicate("r"))

    // -- PruningVarExpand --

    // With nodePredicate and relationshipPredicate
    assertGood(
      attach(
        PruningVarExpand(
          lhsLP,
          varFor("a"),
          SemanticDirection.OUTGOING,
          Seq(relType("R")),
          Some(varFor("y")),
          1,
          4,
          Seq(nodePredicate),
          Seq(relationshipPredicate)
        ),
        1.0
      ),
      planDescription(
        id,
        "VarLengthExpand(Pruning)",
        Seq(lhsPD),
        Seq(details(
          "p = (a)-[:R*..4]->(y) WHERE all(x IN nodes(p) WHERE x.prop = $autodouble_1) AND all(r IN relationships(p) WHERE r.prop = $autodouble_1)"
        )),
        Set("a", "y")
      )
    )

    // With nodePredicate, without relationshipPredicate
    assertGood(
      attach(
        PruningVarExpand(
          lhsLP,
          varFor("a"),
          SemanticDirection.OUTGOING,
          Seq(relType("R")),
          Some(varFor("y")),
          2,
          4,
          Seq(nodePredicate),
          Seq()
        ),
        1.0
      ),
      planDescription(
        id,
        "VarLengthExpand(Pruning)",
        Seq(lhsPD),
        Seq(details("p = (a)-[:R*2..4]->(y) WHERE all(x IN nodes(p) WHERE x.prop = $autodouble_1)")),
        Set("a", "y")
      )
    )

    // With 2 nodePredicates, without relationshipPredicate
    assertGood(
      attach(
        PruningVarExpand(
          lhsLP,
          varFor("a"),
          SemanticDirection.OUTGOING,
          Seq(relType("R")),
          Some(varFor("y")),
          2,
          4,
          Seq(nodePredicate, nodePredicate2),
          Seq()
        ),
        1.0
      ),
      planDescription(
        id,
        "VarLengthExpand(Pruning)",
        Seq(lhsPD),
        Seq(details(
          "p = (a)-[:R*2..4]->(y) WHERE all(x IN nodes(p) WHERE x.prop = $autodouble_1) AND all(x2 IN nodes(p) WHERE x2.prop = $autodouble_1)"
        )),
        Set("a", "y")
      )
    )

    // Without predicates, without relationship type
    assertGood(
      attach(
        PruningVarExpand(lhsLP, varFor("a"), SemanticDirection.OUTGOING, Seq(), Some(varFor("y")), 2, 4, Seq(), Seq()),
        1.0
      ),
      planDescription(
        id,
        "VarLengthExpand(Pruning)",
        Seq(lhsPD),
        Seq(details("(a)-[*2..4]->(y)")),
        Set("a", "y")
      )
    )
    // with to node removed
    assertGood(
      attach(
        PruningVarExpand(lhsLP, varFor("a"), SemanticDirection.OUTGOING, Seq(), None, 2, 4, Seq(), Seq()),
        1.0
      ),
      planDescription(
        id,
        "VarLengthExpand(Pruning)",
        Seq(lhsPD),
        Seq(details("(a)-[*2..4]->()")),
        Set("a")
      )
    )

    // -- BFSPruningVarExpand --

    // With nodePredicate and relationshipPredicate
    assertGood(
      attach(
        BFSPruningVarExpand(
          lhsLP,
          varFor("a"),
          SemanticDirection.OUTGOING,
          Seq(relType("R")),
          Some(varFor("y")),
          includeStartNode = false,
          maxLength = 4,
          depthName = Some(varFor("depth")),
          expansionMode = ExpandAll,
          nodePredicates = Seq(nodePredicate),
          relationshipPredicates = Seq(relationshipPredicate)
        ),
        1.0
      ),
      planDescription(
        id,
        "VarLengthExpand(Pruning,BFS,All)",
        Seq(lhsPD),
        Seq(details(
          "p = (a)-[:R*..4]->(y) WHERE all(x IN nodes(p) WHERE x.prop = $autodouble_1) AND all(r IN relationships(p) WHERE r.prop = $autodouble_1) depth"
        )),
        Set("a", "y", "depth")
      )
    )

    // With nodePredicate, without relationshipPredicate
    assertGood(
      attach(
        BFSPruningVarExpand(
          lhsLP,
          varFor("a"),
          SemanticDirection.OUTGOING,
          Seq(relType("R")),
          Some(varFor("y")),
          includeStartNode = true,
          4,
          depthName = Some(varFor("depth")),
          expansionMode = ExpandAll,
          Seq(nodePredicate),
          Seq()
        ),
        1.0
      ),
      planDescription(
        id,
        "VarLengthExpand(Pruning,BFS,All)",
        Seq(lhsPD),
        Seq(details("p = (a)-[:R*0..4]->(y) WHERE all(x IN nodes(p) WHERE x.prop = $autodouble_1) depth")),
        Set("a", "y", "depth")
      )
    )

    // Without predicates, without relationship type
    assertGood(
      attach(
        BFSPruningVarExpand(
          lhsLP,
          varFor("a"),
          SemanticDirection.OUTGOING,
          Seq(),
          Some(varFor("y")),
          includeStartNode = false,
          4,
          depthName = Some(varFor("depth")),
          expansionMode = ExpandInto,
          Seq(),
          Seq()
        ),
        1.0
      ),
      planDescription(
        id,
        "VarLengthExpand(Pruning,BFS,Into)",
        Seq(lhsPD),
        Seq(details("(a)-[*..4]->(y) depth")),
        Set("a", "y", "depth")
      )
    )

    // with to node removed
    assertGood(
      attach(
        BFSPruningVarExpand(
          lhsLP,
          varFor("a"),
          SemanticDirection.OUTGOING,
          Seq(),
          None,
          includeStartNode = false,
          4,
          depthName = Some(varFor("depth")),
          expansionMode = ExpandInto,
          Seq(),
          Seq()
        ),
        1.0
      ),
      planDescription(
        id,
        "VarLengthExpand(Pruning,BFS,Into)",
        Seq(lhsPD),
        Seq(details("(a)-[*..4]->() depth")),
        Set("a", "depth")
      )
    )

    // -- VarExpand --

    // With unnamed variables, without predicates
    assertGood(
      attach(
        VarExpand(
          lhsLP,
          varFor("a"),
          INCOMING,
          INCOMING,
          Seq(relType("LIKES"), relType("LOVES")),
          Some(varFor("  UNNAMED123")),
          Some(varFor("  UNNAMED99")),
          VarPatternLength(1, Some(1)),
          ExpandAll
        ),
        1.0
      ),
      planDescription(
        id,
        "VarLengthExpand(All)",
        Seq(lhsPD),
        Seq(details(s"(a)<-[${anonVar("99")}:LIKES|LOVES]-(${anonVar("123")})")),
        Set("a", anonVar("99"), anonVar("123"))
      )
    )

    // With nodePredicate and relationshipPredicate
    assertGood(
      attach(
        VarExpand(
          lhsLP,
          varFor("a"),
          INCOMING,
          INCOMING,
          Seq(relType("LIKES"), relType("LOVES")),
          Some(varFor("to")),
          Some(varFor("rel")),
          VarPatternLength(1, Some(1)),
          ExpandAll,
          Seq(nodePredicate),
          Seq(relationshipPredicate),
          TraversalPathMode.Trail
        ),
        1.0
      ),
      planDescription(
        id,
        "VarLengthExpand(All)",
        Seq(lhsPD),
        Seq(details(
          "p = (a)<-[rel:LIKES|LOVES]-(to) WHERE all(x IN nodes(p) WHERE x.prop = $autodouble_1) AND all(r IN relationships(p) WHERE r.prop = $autodouble_1)"
        )),
        Set("a", "to", "rel")
      )
    )

    // With nodePredicate, without relationshipPredicate, with length
    assertGood(
      attach(
        VarExpand(
          lhsLP,
          varFor("a"),
          INCOMING,
          INCOMING,
          Seq(relType("LIKES"), relType("LOVES")),
          Some(varFor("to")),
          Some(varFor("rel")),
          VarPatternLength(2, Some(3)),
          ExpandAll,
          Seq(nodePredicate)
        ),
        1.0
      ),
      planDescription(
        id,
        "VarLengthExpand(All)",
        Seq(lhsPD),
        Seq(details("p = (a)<-[rel:LIKES|LOVES*2..3]-(to) WHERE all(x IN nodes(p) WHERE x.prop = $autodouble_1)")),
        Set("a", "to", "rel")
      )
    )

    // With unbounded length
    assertGood(
      attach(
        VarExpand(
          lhsLP,
          varFor("a"),
          OUTGOING,
          OUTGOING,
          Seq(relType("LIKES"), relType("LOVES")),
          Some(varFor("to")),
          Some(varFor("rel")),
          VarPatternLength(2, None),
          ExpandAll
        ),
        1.0
      ),
      planDescription(
        id,
        "VarLengthExpand(All)",
        Seq(lhsPD),
        Seq(details("(a)-[rel:LIKES|LOVES*2..]->(to)")),
        Set("a", "to", "rel")
      )
    )

    // With fixed length
    assertGood(
      attach(
        VarExpand(
          lhsLP,
          varFor("a"),
          OUTGOING,
          OUTGOING,
          Seq(relType("LIKES"), relType("LOVES")),
          Some(varFor("to")),
          Some(varFor("rel")),
          VarPatternLength(2, Some(2)),
          ExpandAll
        ),
        1.0
      ),
      planDescription(
        id,
        "VarLengthExpand(All)",
        Seq(lhsPD),
        Seq(details("(a)-[rel:LIKES|LOVES*2]->(to)")),
        Set("a", "to", "rel")
      )
    )

    // with to and rel removed
    assertGood(
      attach(
        VarExpand(
          lhsLP,
          varFor("a"),
          OUTGOING,
          OUTGOING,
          Seq(relType("LIKES"), relType("LOVES")),
          None,
          None,
          VarPatternLength(2, Some(2)),
          ExpandAll
        ),
        1.0
      ),
      planDescription(
        id,
        "VarLengthExpand(All)",
        Seq(lhsPD),
        Seq(details("(a)-[:LIKES|LOVES*2]->()")),
        Set("a")
      )
    )
  }

  test("Updates") {
    // RemoveLabels
    assertGood(
      attach(RemoveLabels(lhsLP, varFor("x"), Set(label("L1")), Set.empty), 1.0),
      planDescription(id, "RemoveLabels", Seq(lhsPD), Seq(details("x:L1")), Set("a", "x"))
    )

    assertGood(
      attach(RemoveLabels(lhsLP, varFor("x"), Set(label("L1"), label("L2")), Set.empty), 1.0),
      planDescription(id, "RemoveLabels", Seq(lhsPD), Seq(details("x:L1:L2")), Set("a", "x"))
    )

    // SetLabels
    assertGood(
      attach(SetLabels(lhsLP, varFor("x"), Set(label("L1")), Set.empty), 1.0),
      planDescription(id, "SetLabels", Seq(lhsPD), Seq(details("x:L1")), Set("a", "x"))
    )

    assertGood(
      attach(SetLabels(lhsLP, varFor("x"), Set(label("L1"), label("L2")), Set.empty), 1.0),
      planDescription(id, "SetLabels", Seq(lhsPD), Seq(details("x:L1:L2")), Set("a", "x"))
    )

    assertGood(
      attach(SetLabels(lhsLP, varFor("x"), Set(label("L1"), label("L2")), Set(stringLiteral("L3"))), 1.0),
      planDescription(id, "SetLabels", Seq(lhsPD), Seq(details("x:L1:L2:$(\"L3\")")), Set("a", "x"))
    )

    val map = MapExpression(Seq(
      (key("foo"), number("1")),
      (key("bar"), number("2"))
    ))(pos)
    val prettifiedMapExpr = "{foo: 1, bar: 2}"

    // Set From Map
    assertGood(
      attach(SetNodePropertiesFromMap(lhsLP, varFor("x"), map, removeOtherProps = true), 1.0),
      planDescription(
        id,
        "SetNodePropertiesFromMap",
        Seq(lhsPD),
        Seq(details(s"x = $prettifiedMapExpr")),
        Set("a", "x")
      )
    )

    assertGood(
      attach(SetNodePropertiesFromMap(lhsLP, varFor("x"), map, removeOtherProps = false), 1.0),
      planDescription(
        id,
        "SetNodePropertiesFromMap",
        Seq(lhsPD),
        Seq(details(s"x += $prettifiedMapExpr")),
        Set("a", "x")
      )
    )

    assertGood(
      attach(SetRelationshipPropertiesFromMap(lhsLP, varFor("x"), map, removeOtherProps = true), 1.0),
      planDescription(
        id,
        "SetRelationshipPropertiesFromMap",
        Seq(lhsPD),
        Seq(details(s"x = $prettifiedMapExpr")),
        Set("a", "x")
      )
    )

    assertGood(
      attach(SetRelationshipPropertiesFromMap(lhsLP, varFor("x"), map, removeOtherProps = false), 1.0),
      planDescription(
        id,
        "SetRelationshipPropertiesFromMap",
        Seq(lhsPD),
        Seq(details(s"x += $prettifiedMapExpr")),
        Set("a", "x")
      )
    )

    assertGood(
      attach(SetPropertiesFromMap(lhsLP, varFor("x"), map, removeOtherProps = true), 1.0),
      planDescription(id, "SetPropertiesFromMap", Seq(lhsPD), Seq(details(s"x = $prettifiedMapExpr")), Set("a"))
    )

    assertGood(
      attach(SetDynamicProperty(lhsLP, varFor("x"), stringLiteral("prop"), number("42")), 1.0),
      planDescription(id, "SetDynamicProperty", Seq(lhsPD), Seq(details(s"x[\"prop\"] = 42")), Set("a"))
    )

    assertGood(
      attach(SetPropertiesFromMap(lhsLP, varFor("x"), map, removeOtherProps = false), 1.0),
      planDescription(
        id,
        "SetPropertiesFromMap",
        Seq(lhsPD),
        Seq(details(s"x += $prettifiedMapExpr")),
        Set("a")
      )
    )

    // Set
    assertGood(
      attach(SetProperty(lhsLP, varFor("x"), key("prop"), number("1")), 1.0),
      planDescription(id, "SetProperty", Seq(lhsPD), Seq(details("x.prop = 1")), Set("a"))
    )

    assertGood(
      attach(SetNodeProperty(lhsLP, varFor("x"), key("prop"), number("1")), 1.0),
      planDescription(id, "SetProperty", Seq(lhsPD), Seq(details("x.prop = 1")), Set("a", "x"))
    )

    assertGood(
      attach(SetRelationshipProperty(lhsLP, varFor("x"), key("prop"), number("1")), 1.0),
      planDescription(id, "SetProperty", Seq(lhsPD), Seq(details("x.prop = 1")), Set("a", "x"))
    )
    // Set multiple properties
    assertGood(
      attach(SetProperties(lhsLP, varFor("x"), Seq((key("p1"), number("1")), (key("p2"), number("2")))), 1.0),
      planDescription(id, "SetProperties", Seq(lhsPD), Seq(details("x.p1 = 1, x.p2 = 2")), Set("a"))
    )

    assertGood(
      attach(SetNodeProperties(lhsLP, varFor("x"), Seq((key("p1"), number("1")), (key("p2"), number("2")))), 1.0),
      planDescription(id, "SetProperties", Seq(lhsPD), Seq(details("x.p1 = 1, x.p2 = 2")), Set("a", "x"))
    )

    assertGood(
      attach(SetNodeProperties(lhsLP, varFor("x"), Seq((key("p1"), number("1")), (key("p2"), number("2")))), 1.0),
      planDescription(id, "SetProperties", Seq(lhsPD), Seq(details("x.p1 = 1, x.p2 = 2")), Set("a", "x"))
    )

  }

  test("Sort") {
    // Sort
    assertGood(
      attach(Sort(lhsLP, Seq(Ascending(varFor("a")))), 1.0),
      planDescription(id, "Sort", Seq(lhsPD), Seq(details("a ASC")), Set("a"))
    )

    assertGood(
      attach(Sort(lhsLP, Seq(Descending(varFor("a")), Ascending(varFor("y")))), 1.0),
      planDescription(id, "Sort", Seq(lhsPD), Seq(details("a DESC, y ASC")), Set("a"))
    )

    // Top
    assertGood(
      attach(Top(lhsLP, Seq(Ascending(varFor("a"))), number("3")), 1.0),
      planDescription(id, "Top", Seq(lhsPD), Seq(details("a ASC LIMIT 3")), Set("a"))
    )

    assertGood(
      attach(Top(lhsLP, Seq(Descending(varFor("a")), Ascending(varFor("y"))), number("3")), 1.0),
      planDescription(id, "Top", Seq(lhsPD), Seq(details("a DESC, y ASC LIMIT 3")), Set("a"))
    )

    // Partial Sort
    assertGood(
      attach(PartialSort(lhsLP, Seq(Ascending(varFor("a"))), Seq(Descending(varFor("y")))), 1.0),
      planDescription(id, "PartialSort", Seq(lhsPD), Seq(details("a ASC, y DESC")), Set("a"))
    )

    // Partial Top
    assertGood(
      attach(PartialTop(lhsLP, Seq(Ascending(varFor("a"))), Seq(Descending(varFor("y"))), number("3")), 1.0),
      planDescription(id, "PartialTop", Seq(lhsPD), Seq(details("a ASC, y DESC LIMIT 3")), Set("a"))
    )
  }

  test("Unwind") {
    assertGood(
      attach(UnwindCollection(lhsLP, varFor("x"), varFor("list")), 1.0),
      planDescription(id, "Unwind", Seq(lhsPD), Seq(details("list AS x")), Set("a", "x"))
    )
  }

  test("PartitionedUnwind") {
    assertGood(
      attach(PartitionedUnwindCollection(lhsLP, Some(varFor("x")), varFor("list")), 1.0),
      planDescription(id, "PartitionedUnwind", Seq(lhsPD), Seq(details("list AS x")), Set("a", "x"))
    )
  }

  test("PreserveOrder") {
    assertGood(
      attach(PreserveOrder(lhsLP), 1.0),
      planDescription(id, "PreserveOrder", Seq(lhsPD), Seq.empty, Set("a"))
    )
  }

  test("AntiConditionalApply") {
    assertGood(
      attach(AntiConditionalApply(lhsLP, rhsLP, Seq(varFor("c"))), 2345.0),
      planDescription(id, "AntiConditionalApply", Seq(lhsPD, rhsPD), Seq.empty, Set("a", "b", "c"))
    )
  }

  test("AntiSemiApply") {
    assertGood(
      attach(AntiSemiApply(lhsLP, rhsLP), 2345.0),
      planDescription(id, "AntiSemiApply", Seq(lhsPD, rhsPD), Seq.empty, Set("a"))
    )
  }

  test("ConditionalApply") {
    assertGood(
      attach(ConditionalApply(lhsLP, rhsLP, Seq(varFor("c"))), 2345.0),
      planDescription(id, "ConditionalApply", Seq(lhsPD, rhsPD), Seq.empty, Set("a", "b", "c"))
    )
  }

  test("Apply") {
    assertGood(
      attach(Apply(lhsLP, rhsLP), 2345.0),
      planDescription(id, "Apply", Seq(lhsPD, rhsPD), Seq.empty, Set("a", "b"))
    )
  }

  test("AssertSameNode") {
    assertGood(
      attach(AssertSameNode(varFor("n"), lhsLP, rhsLP), 2345.0),
      planDescription(id, "AssertSameNode", Seq(lhsPD, rhsPD), Seq(details(Seq("n"))), Set("a", "b", "n"))
    )
  }

  test("AssertSameRelationship") {
    assertGood(
      attach(AssertSameRelationship(varFor("r"), lhsLP, rhsLP), 2345.0),
      planDescription(
        id,
        "AssertSameRelationship",
        Seq(lhsPD, rhsPD),
        Seq(details(Seq("r"))),
        Set("a", "b", "r")
      )
    )
  }

  test("CartesianProduct") {
    assertGood(
      attach(CartesianProduct(lhsLP, rhsLP), 2345.0),
      planDescription(id, "CartesianProduct", Seq(lhsPD, rhsPD), Seq.empty, Set("a", "b"))
    )
  }

  test("NodeHashJoin") {
    assertGood(
      attach(NodeHashJoin(Set(varFor("a")), lhsLP, rhsLP), 2345.0),
      planDescription(id, "NodeHashJoin", Seq(lhsPD, rhsPD), Seq(details("a")), Set("a", "b"))
    )
  }

  test("ForeachApply") {
    val testCases = Seq(
      ("a", ListLiteral(Seq(number("1"), number("2")))(pos)) ->
        "a IN [1, 2]",
      ("b", parameter("param", CTList(CTInteger))) ->
        "b IN $param",
      ("c", ListLiteral(Seq.empty)(pos)) ->
        "c IN []",
      ("d", FunctionInvocation(number("1"), functionName("range"), number("100"))) ->
        "d IN range(1, 100)"
    )

    for (((variable, expr), expectedDetails) <- testCases) {
      assertGood(
        attach(ForeachApply(lhsLP, rhsLP, varFor(variable), expr), 2345.0),
        planDescription(id, "Foreach", Seq(lhsPD, rhsPD), Seq(details(expectedDetails)), Set("a"))
      )
    }
  }

  test("LetSelectOrSemiApply") {
    assertGood(
      attach(LetSelectOrSemiApply(lhsLP, rhsLP, varFor("x"), Equals(prop("a", "foo"), number("42"))(pos)), 2345.0),
      planDescription(id, "LetSelectOrSemiApply", Seq(lhsPD, rhsPD), Seq(details("a.foo = 42")), Set("a", "x"))
    )
  }

  test("LetSelectOrAntiSemiApply") {
    assertGood(
      attach(LetSelectOrAntiSemiApply(lhsLP, rhsLP, varFor("x"), Equals(prop("a", "foo"), number("42"))(pos)), 2345.0),
      planDescription(
        id,
        "LetSelectOrAntiSemiApply",
        Seq(lhsPD, rhsPD),
        Seq(details("a.foo = 42")),
        Set("a", "x")
      )
    )
  }

  test("LetSemiApply") {
    assertGood(
      attach(LetSemiApply(lhsLP, rhsLP, varFor("x")), 2345.0),
      planDescription(id, "LetSemiApply", Seq(lhsPD, rhsPD), Seq(details("x")), Set("a", "x"))
    )
  }

  test("LetAntiSemiApply") {
    assertGood(
      attach(LetAntiSemiApply(lhsLP, rhsLP, varFor("x")), 2345.0),
      planDescription(id, "LetAntiSemiApply", Seq(lhsPD, rhsPD), Seq.empty, Set("a", "x"))
    )
  }

  test("LeftOuterHashJoin") {
    assertGood(
      attach(LeftOuterHashJoin(Set(varFor("a")), lhsLP, rhsLP), 2345.0),
      planDescription(id, "NodeLeftOuterHashJoin", Seq(lhsPD, rhsPD), Seq(details("a")), Set("a", "b"))
    )
  }

  test("RightOuterHashJoin") {
    assertGood(
      attach(RightOuterHashJoin(Set(varFor("a")), lhsLP, rhsLP), 2345.0),
      planDescription(id, "NodeRightOuterHashJoin", Seq(lhsPD, rhsPD), Seq(details("a")), Set("a", "b"))
    )
  }

  test("RollUpApply") {
    assertGood(
      attach(RollUpApply(lhsLP, rhsLP, varFor("collection"), varFor("x")), 2345.0),
      planDescription(
        id,
        "RollUpApply",
        Seq(lhsPD, rhsPD),
        Seq(details(Seq("collection", "x"))),
        Set("a", "collection")
      )
    )
  }

  test("SelectOrAntiSemiApply") {
    assertGood(
      attach(SelectOrAntiSemiApply(lhsLP, rhsLP, Equals(prop("a", "foo"), number("42"))(pos)), 2345.0),
      planDescription(id, "SelectOrAntiSemiApply", Seq(lhsPD, rhsPD), Seq(details("a.foo = 42")), Set("a"))
    )
  }

  test("SelectOrSemiApply") {
    assertGood(
      attach(SelectOrSemiApply(lhsLP, rhsLP, Equals(prop("a", "foo"), number("42"))(pos)), 2345.0),
      planDescription(id, "SelectOrSemiApply", Seq(lhsPD, rhsPD), Seq(details("a.foo = 42")), Set("a"))
    )
  }

  test("SemiApply") {
    assertGood(
      attach(SemiApply(lhsLP, rhsLP), 2345.0),
      planDescription(id, "SemiApply", Seq(lhsPD, rhsPD), Seq.empty, Set("a"))
    )
  }

  test("TransactionForeach") {
    assertGood(
      attach(
        TransactionForeach(
          lhsLP,
          rhsLP,
          batchSize = number("100"),
          concurrency = TransactionConcurrency.Serial,
          onErrorBehaviour = OnErrorContinue,
          maybeReportAs = None,
          maybeRetryParameters = None,
          maybeDisjointByParameters = None
        ),
        2345.0
      ),
      planDescription(
        id,
        "TransactionForeach",
        Seq(lhsPD, rhsPD),
        Seq(details("IN TRANSACTIONS OF 100 ROWS ON ERROR CONTINUE")),
        Set("a")
      )
    )
  }

  test("TransactionForeach with status") {
    assertGood(
      attach(
        TransactionForeach(
          lhsLP,
          rhsLP,
          batchSize = number("100"),
          concurrency = TransactionConcurrency.Serial,
          onErrorBehaviour = OnErrorBreak,
          maybeReportAs = Some(varFor("status")),
          maybeRetryParameters = None,
          maybeDisjointByParameters = None
        ),
        2345.0
      ),
      planDescription(
        id,
        "TransactionForeach",
        Seq(lhsPD, rhsPD),
        Seq(details("IN TRANSACTIONS OF 100 ROWS ON ERROR BREAK REPORT STATUS AS status")),
        Set("a", "status")
      )
    )
  }

  test("TransactionForeach with explicit concurrency") {
    assertGood(
      attach(
        TransactionForeach(
          lhsLP,
          rhsLP,
          batchSize = number("100"),
          concurrency = TransactionConcurrency.Concurrent(Some(number("5"))),
          onErrorBehaviour = OnErrorContinue,
          maybeReportAs = None,
          maybeRetryParameters = None,
          maybeDisjointByParameters = None
        ),
        2345.0
      ),
      planDescription(
        id,
        "TransactionForeach",
        Seq(lhsPD, rhsPD),
        Seq(details("IN 5 CONCURRENT TRANSACTIONS OF 100 ROWS ON ERROR CONTINUE")),
        Set("a")
      )
    )
  }

  test("TransactionForeach with default concurrency") {
    assertGood(
      attach(
        TransactionForeach(
          lhsLP,
          rhsLP,
          batchSize = number("100"),
          concurrency = TransactionConcurrency.Concurrent(None),
          onErrorBehaviour = OnErrorContinue,
          maybeReportAs = None,
          maybeRetryParameters = None,
          maybeDisjointByParameters = None
        ),
        2345.0
      ),
      planDescription(
        id,
        "TransactionForeach",
        Seq(lhsPD, rhsPD),
        Seq(details("IN CONCURRENT TRANSACTIONS OF 100 ROWS ON ERROR CONTINUE")),
        Set("a")
      )
    )
  }

  test("TransactionApply") {
    assertGood(
      attach(
        TransactionApply(
          lhsLP,
          rhsLP,
          batchSize = number("100"),
          concurrency = TransactionConcurrency.Serial,
          onErrorBehaviour = OnErrorFail,
          maybeReportAs = None,
          maybeRetryParameters = None,
          maybeDisjointByParameters = None
        ),
        2345.0
      ),
      planDescription(
        id,
        "TransactionApply",
        Seq(lhsPD, rhsPD),
        Seq(details("IN TRANSACTIONS OF 100 ROWS ON ERROR FAIL")),
        Set("a", "b")
      )
    )
  }

  test("TransactionApply with status") {
    assertGood(
      attach(
        TransactionApply(
          lhsLP,
          rhsLP,
          batchSize = number("100"),
          concurrency = TransactionConcurrency.Concurrent(Some(number("5"))),
          onErrorBehaviour = OnErrorFail,
          maybeReportAs = Some(varFor("status")),
          maybeRetryParameters = None,
          maybeDisjointByParameters = None
        ),
        2345.0
      ),
      planDescription(
        id,
        "TransactionApply",
        Seq(lhsPD, rhsPD),
        Seq(details("IN 5 CONCURRENT TRANSACTIONS OF 100 ROWS ON ERROR FAIL REPORT STATUS AS status")),
        Set("a", "b", "status")
      )
    )
  }

  test("TransactionApply with default concurrency") {
    assertGood(
      attach(
        TransactionApply(
          lhsLP,
          rhsLP,
          batchSize = number("100"),
          concurrency = TransactionConcurrency.Concurrent(None),
          onErrorBehaviour = OnErrorFail,
          maybeReportAs = None,
          maybeRetryParameters = None,
          maybeDisjointByParameters = None
        ),
        2345.0
      ),
      planDescription(
        id,
        "TransactionApply",
        Seq(lhsPD, rhsPD),
        Seq(details("IN CONCURRENT TRANSACTIONS OF 100 ROWS ON ERROR FAIL")),
        Set("a", "b")
      )
    )
  }

  test("TransactionApply with specific concurrency") {
    assertGood(
      attach(
        TransactionApply(
          lhsLP,
          rhsLP,
          batchSize = number("100"),
          concurrency = TransactionConcurrency.Concurrent(Some(number("5"))),
          onErrorBehaviour = OnErrorFail,
          maybeReportAs = None,
          maybeRetryParameters = None,
          maybeDisjointByParameters = None
        ),
        2345.0
      ),
      planDescription(
        id,
        "TransactionApply",
        Seq(lhsPD, rhsPD),
        Seq(details("IN 5 CONCURRENT TRANSACTIONS OF 100 ROWS ON ERROR FAIL")),
        Set("a", "b")
      )
    )
  }

  test("TransactionApply with default retry") {
    assertGood(
      attach(
        TransactionApply(
          lhsLP,
          rhsLP,
          batchSize = number("100"),
          concurrency = TransactionConcurrency.Serial,
          onErrorBehaviour = OnErrorRetryThenFail,
          maybeReportAs = None,
          maybeRetryParameters = None,
          maybeDisjointByParameters = None
        ),
        2345.0
      ),
      planDescription(
        id,
        "TransactionApply",
        Seq(lhsPD, rhsPD),
        Seq(details("IN TRANSACTIONS OF 100 ROWS ON ERROR RETRY THEN FAIL")),
        Set("a", "b")
      )
    )
  }

  test("TransactionApply with specific retry") {
    assertGood(
      attach(
        TransactionApply(
          lhsLP,
          rhsLP,
          batchSize = number("100"),
          concurrency = TransactionConcurrency.Serial,
          onErrorBehaviour = OnErrorRetryThenBreak,
          maybeReportAs = None,
          maybeRetryParameters = Some(InTransactionsRetryParameters(Some(float("1.5")))(pos)),
          maybeDisjointByParameters = None
        ),
        2345.0
      ),
      planDescription(
        id,
        "TransactionApply",
        Seq(lhsPD, rhsPD),
        Seq(details("IN TRANSACTIONS OF 100 ROWS ON ERROR RETRY FOR 1.5 SECONDS THEN BREAK")),
        Set("a", "b")
      )
    )
  }

  test("TransactionForeach with default retry") {
    assertGood(
      attach(
        TransactionForeach(
          lhsLP,
          rhsLP,
          batchSize = number("100"),
          concurrency = TransactionConcurrency.Serial,
          onErrorBehaviour = OnErrorRetryThenFail,
          maybeReportAs = None,
          maybeRetryParameters = None,
          maybeDisjointByParameters = None
        ),
        2345.0
      ),
      planDescription(
        id,
        "TransactionForeach",
        Seq(lhsPD, rhsPD),
        Seq(details("IN TRANSACTIONS OF 100 ROWS ON ERROR RETRY THEN FAIL")),
        Set("a")
      )
    )
  }

  test("TransactionForeach with specific retry") {
    assertGood(
      attach(
        TransactionForeach(
          lhsLP,
          rhsLP,
          batchSize = number("100"),
          concurrency = TransactionConcurrency.Serial,
          onErrorBehaviour = OnErrorRetryThenContinue,
          maybeReportAs = Some(varFor("status")),
          maybeRetryParameters = Some(InTransactionsRetryParameters(Some(float("1.5")))(pos)),
          maybeDisjointByParameters = None
        ),
        2345.0
      ),
      planDescription(
        id,
        "TransactionForeach",
        Seq(lhsPD, rhsPD),
        Seq(
          details("IN TRANSACTIONS OF 100 ROWS ON ERROR RETRY FOR 1.5 SECONDS THEN CONTINUE REPORT STATUS AS status")
        ),
        Set("a", "status")
      )
    )
  }

  test("TransactionApply with specific disjoint by expression group") {
    assertGood(
      attach(
        TransactionApply(
          lhsLP,
          rhsLP,
          batchSize = number("100"),
          concurrency = TransactionConcurrency.Concurrent(None),
          onErrorBehaviour = OnErrorFail,
          maybeReportAs = None,
          maybeRetryParameters = None,
          maybeDisjointByParameters =
            Some(InTransactionsDisjointByParameters(DisjointByExpressions(Seq(prop("a", "id"), prop("b", "id"))))(pos)),
          effectiveDisjointBy = Seq(prop("a", "id"), prop("b", "id"))
        ),
        2345.0
      ),
      planDescription(
        id,
        "TransactionApply",
        Seq(lhsPD, rhsPD),
        Seq(details("IN CONCURRENT TRANSACTIONS OF 100 ROWS DISJOINT BY (a.id, b.id) ON ERROR FAIL")),
        Set("a", "b")
      )
    )
  }

  test("TransactionApply with specific disjoint by auto shows effective disjoint by") {
    assertGood(
      attach(
        TransactionApply(
          lhsLP,
          rhsLP,
          batchSize = number("100"),
          concurrency = TransactionConcurrency.Concurrent(None),
          onErrorBehaviour = OnErrorFail,
          maybeReportAs = None,
          maybeRetryParameters = None,
          maybeDisjointByParameters = Some(InTransactionsDisjointByParameters(DisjointByAuto)(pos)),
          effectiveDisjointBy = Seq(prop("a", "id"), prop("b", "id"))
        ),
        2345.0
      ),
      planDescription(
        id,
        "TransactionApply",
        Seq(lhsPD, rhsPD),
        Seq(details("IN CONCURRENT TRANSACTIONS OF 100 ROWS DISJOINT BY (a.id, b.id) ON ERROR FAIL")),
        Set("a", "b")
      )
    )
  }

  test("TransactionApply with disjoint by none shows effective disjoint by (nothing)") {
    assertGood(
      attach(
        TransactionApply(
          lhsLP,
          rhsLP,
          batchSize = number("100"),
          concurrency = TransactionConcurrency.Concurrent(None),
          onErrorBehaviour = OnErrorFail,
          maybeReportAs = None,
          maybeRetryParameters = None,
          maybeDisjointByParameters = Some(InTransactionsDisjointByParameters(DisjointByNone)(pos)),
          effectiveDisjointBy = Seq.empty
        ),
        2345.0
      ),
      planDescription(
        id,
        "TransactionApply",
        Seq(lhsPD, rhsPD),
        Seq(details("IN CONCURRENT TRANSACTIONS OF 100 ROWS ON ERROR FAIL")),
        Set("a", "b")
      )
    )
  }

  test("TransactionForeach with specific disjoint by expression group") {
    assertGood(
      attach(
        TransactionForeach(
          lhsLP,
          rhsLP,
          batchSize = number("100"),
          concurrency = TransactionConcurrency.Concurrent(None),
          onErrorBehaviour = OnErrorContinue,
          maybeReportAs = None,
          maybeRetryParameters = None,
          maybeDisjointByParameters =
            Some(InTransactionsDisjointByParameters(DisjointByExpressions(Seq(prop("a", "id"))))(pos)),
          effectiveDisjointBy = Seq(prop("a", "id"))
        ),
        2345.0
      ),
      planDescription(
        id,
        "TransactionForeach",
        Seq(lhsPD, rhsPD),
        Seq(details("IN CONCURRENT TRANSACTIONS OF 100 ROWS DISJOINT BY (a.id) ON ERROR CONTINUE")),
        Set("a")
      )
    )
  }

  test("TransactionForeach with specific disjoint by auto shows effective disjoint by") {
    assertGood(
      attach(
        TransactionForeach(
          lhsLP,
          rhsLP,
          batchSize = number("100"),
          concurrency = TransactionConcurrency.Concurrent(None),
          onErrorBehaviour = OnErrorContinue,
          maybeReportAs = None,
          maybeRetryParameters = None,
          maybeDisjointByParameters = Some(InTransactionsDisjointByParameters(DisjointByAuto)(pos)),
          effectiveDisjointBy = Seq(prop("a", "id"))
        ),
        2345.0
      ),
      planDescription(
        id,
        "TransactionForeach",
        Seq(lhsPD, rhsPD),
        Seq(details("IN CONCURRENT TRANSACTIONS OF 100 ROWS DISJOINT BY (a.id) ON ERROR CONTINUE")),
        Set("a")
      )
    )
  }

  test("TransactionForeach with disjoint by none shows effective disjoint by (nothing)") {
    assertGood(
      attach(
        TransactionForeach(
          lhsLP,
          rhsLP,
          batchSize = number("100"),
          concurrency = TransactionConcurrency.Concurrent(None),
          onErrorBehaviour = OnErrorFail,
          maybeReportAs = None,
          maybeRetryParameters = None,
          maybeDisjointByParameters = Some(InTransactionsDisjointByParameters(DisjointByNone)(pos)),
          effectiveDisjointBy = Seq.empty
        ),
        2345.0
      ),
      planDescription(
        id,
        "TransactionForeach",
        Seq(lhsPD, rhsPD),
        Seq(details("IN CONCURRENT TRANSACTIONS OF 100 ROWS ON ERROR FAIL")),
        Set("a")
      )
    )
  }

  test("TriadicBuild") {
    assertGood(
      attach(TriadicBuild(lhsLP, varFor("a"), varFor("b"), Some(Id(1))), 113.0),
      planDescription(id, "TriadicBuild", Seq(lhsPD), Seq(details("(a)--(b)")), Set("a"))
    )
  }

  test("TriadicFilter") {
    assertGood(
      attach(TriadicFilter(lhsLP, positivePredicate = true, varFor("a"), varFor("b"), Some(Id(1))), 113.0),
      planDescription(id, "TriadicFilter", Seq(lhsPD), Seq(details("WHERE (a)--(b)")), Set("a"))
    )

    assertGood(
      attach(TriadicFilter(lhsLP, positivePredicate = false, varFor("a"), varFor("b"), Some(Id(1))), 113.0),
      planDescription(id, "TriadicFilter", Seq(lhsPD), Seq(details("WHERE NOT (a)--(b)")), Set("a"))
    )
  }

  test("TriadicSelection") {
    assertGood(
      attach(TriadicSelection(lhsLP, rhsLP, positivePredicate = true, varFor("a"), varFor("b"), varFor("c")), 2345.0),
      planDescription(id, "TriadicSelection", Seq(lhsPD, rhsPD), Seq(details("WHERE (a)--(c)")), Set("a", "b"))
    )

    assertGood(
      attach(TriadicSelection(lhsLP, rhsLP, positivePredicate = false, varFor("a"), varFor("b"), varFor("c")), 2345.0),
      planDescription(
        id,
        "TriadicSelection",
        Seq(lhsPD, rhsPD),
        Seq(details("WHERE NOT (a)--(c)")),
        Set("a", "b")
      )
    )
  }

  test("Union") {
    // leafs no overlapping variables
    assertGood(
      attach(Union(lhsLP, rhsLP), 2345.0),
      planDescription(id, "Union", Seq(lhsPD, rhsPD), Seq.empty, Set.empty)
    )

    // leafs with overlapping variables
    val lp = attach(AllNodesScan(varFor("a"), Set.empty), 2.0, providedOrder = ProvidedOrder.empty)
    val pd = planDescription(id, "AllNodesScan", Seq.empty, Seq(details("a")), Set("a"))
    assertGood(
      attach(Union(lhsLP, lp), 2345.0),
      planDescription(id, "Union", Seq(lhsPD, pd), Seq.empty, Set("a"))
    )
  }

  test("ValueHashJoin") {
    assertGood(
      attach(ValueHashJoin(lhsLP, rhsLP, Equals(prop("a", "foo"), prop("b", "foo"))(pos)), 2345.0),
      planDescription(id, "ValueHashJoin", Seq(lhsPD, rhsPD), Seq(details("a.foo = b.foo")), Set("a", "b"))
    )
  }

  test("ArgumentTracker") {
    assertGood(
      attach(ArgumentTracker(lhsLP), 113.0),
      planDescription(id, "ArgumentTracker", Seq(lhsPD), Seq(), Set("a"))
    )
  }

  test("Repeat(Trail)") {
    assertGood(
      attach(
        RepeatTrail(
          lhsLP,
          rhsLP,
          Repetition(0, Unlimited),
          varFor("start"),
          varFor("end"),
          varFor("  a@1"),
          varFor("  UNNAMED1"),
          Set(
            variableGrouping(varFor("  a@1"), varFor("  a@2")),
            variableGrouping(varFor("  UNNAMED1"), varFor("  UNNAMED2"))
          ),
          Set(variableGrouping(varFor("  r@1"), varFor("  r@2"))),
          Set(varFor("  r@1")),
          Set.empty,
          Set.empty,
          reverseGroupVariableProjections = false,
          ExpandInto
        ),
        2345.0
      ),
      planDescription(
        id,
        "Repeat(Into, Trail)",
        Seq(lhsPD, rhsPD),
        List(details("(start) (...){0, } (end)")),
        Set("r", "a", "anon_2", "start", "end")
      )
    )

    assertGood(
      attach(
        RepeatTrail(
          lhsLP,
          rhsLP,
          Repetition(0, Unlimited),
          varFor("  UNNAMED0"),
          varFor("  end@1"),
          varFor("  a@1"),
          varFor("  UNNAMED1"),
          Set(
            variableGrouping(varFor("  a@1"), varFor("  a@2")),
            variableGrouping(varFor("  UNNAMED1"), varFor("  UNNAMED2"))
          ),
          Set(variableGrouping(varFor("  r@1"), varFor("  r@2"))),
          Set(varFor("  r@1")),
          Set.empty,
          Set.empty,
          reverseGroupVariableProjections = false,
          ExpandAll
        ),
        2345.0
      ),
      planDescription(
        id,
        "Repeat(All, Trail)",
        Seq(lhsPD, rhsPD),
        List(details("(anon_0) (...){0, } (end)")),
        Set("r", "a", "anon_2", "anon_0", "end")
      )
    )

    assertGood(
      attach(
        RepeatTrail(
          lhsLP,
          rhsLP,
          Repetition(0, Unlimited),
          varFor("  UNNAMED0"),
          varFor("  end@1"),
          varFor("  a@1"),
          varFor("  UNNAMED1"),
          Set(
            variableGrouping(varFor("  a@1"), varFor("  a@2")),
            variableGrouping(varFor("  UNNAMED1"), varFor("  UNNAMED2"))
          ),
          Set(variableGrouping(varFor("  r@1"), varFor("  r@2"))),
          Set(varFor("  r@1")),
          Set.empty,
          Set.empty,
          reverseGroupVariableProjections = false,
          ExpandAll,
          accumulatorMappings = Set(
            AllReduceAccumulator(literal(123), varFor("  acc@512"), varFor("  acc@1024"))(pos),
            AllReduceAccumulator(literal("hello"), varFor("  acc@111"), varFor("  acc@222"))(pos),
            AllReduceAccumulator(
              listOf(literal(1), varFor("x"), literal(3)),
              varFor("  result@512"),
              varFor("  result@1024")
            )(pos)
          )
        ),
        2345.0
      ),
      planDescription(
        id,
        "Repeat(All, Trail)",
        Seq(lhsPD, rhsPD),
        List(details(Seq(
          "(anon_0) (...){0, } (end)",
          " ",
          "inlined allReduce() initializers:",
          "  acc = \"hello\"",
          "  acc = 123",
          "  result = [1, x, 3]"
        ).mkString("\n"))),
        Set("r", "a", "anon_2", "anon_0", "end")
      )
    )
  }

  test("BidirectionalRepeat && RepeatOptions") {
    assertGood(
      attach(
        BidirectionalRepeatTrail(
          lhsLP,
          RepeatOptions(lhsLP, rhsLP),
          Repetition(0, Unlimited),
          varFor("  UNNAMED0"),
          varFor("  end@1"),
          varFor("  a@1"),
          varFor("  UNNAMED1"),
          Set(
            variableGrouping(varFor("  a@1"), varFor("  a@2")),
            variableGrouping(varFor("  UNNAMED1"), varFor("  UNNAMED2"))
          ),
          Set(variableGrouping(varFor("  r@1"), varFor("  r@2"))),
          Set(varFor("  r@1")),
          Set.empty,
          Set.empty,
          reverseGroupVariableProjections = false
        ),
        2345.0
      ),
      planDescription(
        id,
        "BidirectionalRepeat(Trail)",
        Seq(
          lhsPD,
          planDescription(
            id,
            "RepeatOptions",
            Seq(lhsPD, rhsPD),
            List(),
            Set("a")
          )
        ),
        List(details("(anon_0) (...){0, } (end)")),
        Set("r", "a", "anon_2", "anon_0", "end")
      )
    )
  }

  test("Repeat(Walk)") {
    assertGood(
      attach(
        RepeatWalk(
          lhsLP,
          rhsLP,
          Repetition(0, Unlimited),
          varFor("start"),
          varFor("end"),
          varFor("  a@1"),
          varFor("  UNNAMED1"),
          Set(
            variableGrouping(varFor("  a@1"), varFor("  a@2")),
            variableGrouping(varFor("  UNNAMED1"), varFor("  UNNAMED2"))
          ),
          Set(variableGrouping(varFor("  r@1"), varFor("  r@2"))),
          reverseGroupVariableProjections = false,
          Set(varFor("  r@1")),
          ExpandInto
        ),
        2345.0
      ),
      planDescription(
        id,
        "Repeat(Into, Walk)",
        Seq(lhsPD, rhsPD),
        List(details("(start) (...){0, } (end)")),
        Set("r", "a", "anon_2", "start", "end")
      )
    )

    assertGood(
      attach(
        RepeatWalk(
          lhsLP,
          rhsLP,
          Repetition(0, Unlimited),
          varFor("  UNNAMED0"),
          varFor("  end@1"),
          varFor("  a@1"),
          varFor("  UNNAMED1"),
          Set(
            variableGrouping(varFor("  a@1"), varFor("  a@2")),
            variableGrouping(varFor("  UNNAMED1"), varFor("  UNNAMED2"))
          ),
          Set(variableGrouping(varFor("  r@1"), varFor("  r@2"))),
          reverseGroupVariableProjections = false,
          Set(varFor("  r@1")),
          ExpandAll
        ),
        2345.0
      ),
      planDescription(
        id,
        "Repeat(All, Walk)",
        Seq(lhsPD, rhsPD),
        List(details("(anon_0) (...){0, } (end)")),
        Set("r", "a", "anon_2", "anon_0", "end")
      )
    )

    assertGood(
      attach(
        RepeatWalk(
          lhsLP,
          rhsLP,
          Repetition(0, Unlimited),
          varFor("  UNNAMED0"),
          varFor("  end@1"),
          varFor("  a@1"),
          varFor("  UNNAMED1"),
          Set(
            variableGrouping(varFor("  a@1"), varFor("  a@2")),
            variableGrouping(varFor("  UNNAMED1"), varFor("  UNNAMED2"))
          ),
          Set(variableGrouping(varFor("  r@1"), varFor("  r@2"))),
          reverseGroupVariableProjections = false,
          Set(varFor("  r@1")),
          ExpandAll,
          accumulatorMappings = Set(
            AllReduceAccumulator(literal(123), varFor("  acc@512"), varFor("  acc@1024"))(pos),
            AllReduceAccumulator(literal("hello"), varFor("  acc@111"), varFor("  acc@222"))(pos),
            AllReduceAccumulator(
              listOf(literal(1), varFor("x"), literal(3)),
              varFor("  result@512"),
              varFor("  result@1024")
            )(pos)
          )
        ),
        2345.0
      ),
      planDescription(
        id,
        "Repeat(All, Walk)",
        Seq(lhsPD, rhsPD),
        List(details(Seq(
          "(anon_0) (...){0, } (end)",
          " ",
          "inlined allReduce() initializers:",
          "  acc = \"hello\"",
          "  acc = 123",
          "  result = [1, x, 3]"
        ).mkString("\n"))),
        Set("r", "a", "anon_2", "anon_0", "end")
      )
    )
  }

  test("SimulatedNodeScan") {
    assertGood(
      attach(SimulatedNodeScan(varFor("a"), 1000), 1.0, providedOrder = DefaultProvidedOrderFactory.asc(varFor("a"))),
      planDescription(
        id,
        "SimulatedNodeScan",
        Seq.empty,
        Seq(details(Seq("a", "1000")), Order(asPrettyString.raw("a ASC"))),
        Set("a")
      )
    )
  }

  test("SimulatedExpand") {
    assertGood(
      attach(SimulatedExpand(lhsLP, varFor("a"), varFor("r1"), varFor("b"), 1.0), 95.0),
      planDescription(
        id,
        "SimulatedExpand",
        Seq(lhsPD),
        Seq(details(Seq("(a)-[r1]->(b)", "1.0"))),
        Set("a", "r1", "b")
      )
    )
  }

  test("SimulatedSelection") {
    assertGood(
      attach(SimulatedSelection(lhsLP, 1.0), 2345.0),
      planDescription(
        id,
        "SimulatedFilter",
        Seq(lhsPD),
        Seq(details("1.0")),
        Set("a")
      )
    )
  }

  test("RuntimeConstant") {
    val namespace = List("ns")
    val name = "datetime"
    val args = IndexedSeq(number("23391882379"))

    val functionInvocation = FunctionInvocation(
      functionName = functionName(namespace, name),
      distinct = false,
      args = args
    )(pos)

    assertGood(
      attach(
        Projection(
          lhsLP,
          immutable.Map(varFor("function") -> RuntimeConstant(varFor("x"), functionInvocation))
        ),
        12345.0
      ),
      planDescription(
        id,
        "Projection",
        Seq(lhsPD),
        Seq(details("RuntimeConstant(ns.datetime(23391882379)) AS function")),
        Set("a", "function")
      )
    )

    assertGood(
      attach(
        Projection(
          lhsLP,
          immutable.Map(varFor("function") -> RuntimeConstant(
            varFor("y"),
            RuntimeConstant(varFor("x"), functionInvocation)
          ))
        ),
        12345.0
      ),
      planDescription(
        id,
        "Projection",
        Seq(lhsPD),
        Seq(details("RuntimeConstant(ns.datetime(23391882379)) AS function")),
        Set("a", "function")
      )
    )
  }

  test("RunQueryAt") {
    assertGood(
      attach(
        RunQueryAt(
          lhsLP,
          "inner query",
          GraphDirectReference(CatalogName(true, "composite", "remote"))(pos),
          Set.empty,
          Map.empty,
          Set(varFor("col"))
        ),
        666.0
      ),
      planDescription(
        id,
        "RunQueryAt",
        Seq(lhsPD),
        Seq(details("inner query")),
        Set("a", "col")
      )
    )
    assertGood(
      attach(
        RunQueryAt(
          lhsLP,
          """multi-line
            |  inner query""".stripMargin,
          GraphDirectReference(CatalogName(true, "composite", "remote"))(pos),
          Set.empty,
          Map.empty,
          Set(varFor("col"))
        ),
        666.0
      ),
      planDescription(
        id,
        "RunQueryAt",
        Seq(lhsPD),
        Seq(details(
          """multi-line
            |  inner query""".stripMargin
        )),
        Set("a", "col")
      )
    )
  }

  test("NonFuseable") {
    assertGood(
      attach(NonFuseable(lhsLP), 2345.0),
      planDescription(
        id,
        "NonFuseable",
        Seq(lhsPD),
        Seq.empty,
        Set("a")
      )
    )
  }

  test("MakeTraversable + Variable") {
    assertGood(
      attach(
        SelectOrSemiApply(
          lhsLP,
          rhsLP,
          IsEmpty.asInvocation(MakeTraversable(varFor("n")))(pos)
        ),
        2345.0
      ),
      planDescription(
        id,
        "SelectOrSemiApply",
        Seq(lhsPD, rhsPD),
        Seq(details("isEmpty(n)")),
        Set("a")
      )
    )
  }

  // cypherPlannerVersion propagation
  test(
    "LogicalPlan2PlanDescription.create with cypherPlannerVersion=Some displays the set cypherPlannerVersion option"
  ) {
    val planDesc = LogicalPlan2PlanDescription.create(
      lhsLP,
      IDPPlannerName,
      readOnly = true,
      ImmutablePlanningAttributes.EffectiveCardinalities(effectiveCardinalities),
      withRawCardinalities = false,
      withDistinctness = false,
      renderNestedPlanExpressions = false,
      providedOrders = ImmutablePlanningAttributes.ProvidedOrders(providedOrders),
      StubExecutionPlan().operatorMetadata,
      CypherVersion.Legacy.legacyVersion(),
      cypherPlannerVersion = Some("v2026_04")
    )
    planDesc.arguments should contain(CypherPlannerVersion("v2026_04"))
    planDesc.arguments should not contain PlannerVersionArgument.currentVersion
  }

  test("LogicalPlan2PlanDescription.create with cypherPlannerVersion=None uses currentVersion") {
    val planDesc = LogicalPlan2PlanDescription.create(
      lhsLP,
      IDPPlannerName,
      readOnly = true,
      ImmutablePlanningAttributes.EffectiveCardinalities(effectiveCardinalities),
      withRawCardinalities = false,
      withDistinctness = false,
      renderNestedPlanExpressions = false,
      providedOrders = ImmutablePlanningAttributes.ProvidedOrders(providedOrders),
      StubExecutionPlan().operatorMetadata,
      CypherVersion.Legacy.legacyVersion()
    )
    planDesc.arguments should contain(PlannerVersionArgument.currentVersion)
  }

  // plannerversion forDisplay
  test("PlannerVersionArgument.forDisplay(None) returns CURRENT DB VERSION") {
    PlannerVersionArgument.forDisplay(None) shouldEqual PlannerVersion(Arguments.CURRENT_DB_VERSION)
  }

  test("PlannerVersionArgument.forDisplay(Some(version)) returns CypherPlannerVersion with that version") {
    PlannerVersionArgument.forDisplay(Some("v2026_04")) should equal(CypherPlannerVersion("v2026_04"))
  }
}
