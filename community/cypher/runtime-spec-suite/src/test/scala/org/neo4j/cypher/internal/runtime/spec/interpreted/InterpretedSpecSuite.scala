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
package org.neo4j.cypher.internal.runtime.spec.interpreted

import org.neo4j.cypher.internal.CommunityInterpretedRuntime
import org.neo4j.cypher.internal.CommunityRuntimeContext
import org.neo4j.cypher.internal.logical.plans.TransactionConcurrency
import org.neo4j.cypher.internal.logical.plans.TraversalPathMode
import org.neo4j.cypher.internal.runtime.spec.COMMUNITY
import org.neo4j.cypher.internal.runtime.spec.interpreted.InterpretedSpecSuite.SIZE_HINT
import org.neo4j.cypher.internal.runtime.spec.tests.AggregationTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.AllNodeScanTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.AllNodeScanWithOtherOperatorsTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.AllRelationshipsScanTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.AntiConditionalApplyTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.AntiSemiApplyTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ApplyTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ArgumentTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.AssertSameNodeTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.AssertSameRelationshipTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.BFSPruningVarLengthExpandTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.CachePropertiesTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.CachePropertiesTxStateTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.CartesianProductProvidedOrderTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.CartesianProductTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ConcurrentTransactionApplyTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ConcurrentTransactionForeachTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ConditionalApplyTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.CreateTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.DeleteDetachExpressionTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.DeleteDetachNodeTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.DeleteDetachPathTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.DeleteExpressionTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.DeleteNodeTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.DeletePathTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.DeleteRelationshipTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.DirectedRelationshipByElementIdSeekTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.DirectedRelationshipByIdSeekTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.DistinctTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.DynamicLabelNodeLookupTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.DynamicRelationshipTypeLookupTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.EagerLimitProfileRowsTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.EagerTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.EmptyResultTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.EsotericAssertSameNodeTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ExhaustiveLimitTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ExpandAllTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ExpandAllWithOtherOperatorsTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ExpandIntoTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ExpandIntoWithOtherOperatorsTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ExpressionTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ExpressionWithTxStateChangesTests
import org.neo4j.cypher.internal.runtime.spec.tests.FilterTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ForeachApplyTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ForeachTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.FullSupportMemoryManagementTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.FullSupportProfileMemoryTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.InputTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.IntersectionLabelScanTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.LabelScanTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.LeftOuterHashJoinTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.LenientCreateRelationshipTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.LetAntiSemiApplyTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.LetSelectOrAntiSemiApplyTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.LetSelectOrSemiApplyTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.LetSemiApplyTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.LimitTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.LoadCsvTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.LoadCsvWithCallInTransactionsAndMerge
import org.neo4j.cypher.internal.runtime.spec.tests.LockNodesTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.MemoryDeallocationTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.MemoryLeakTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.MemoryManagementDisabledTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.MemoryManagementTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.MergeIntoTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.MergeProfileRowsTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.MergeStressTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.MergeTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.MergeUniqueNodeTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.MiscTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.MultiNodeIndexSeekTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.NestedPlanDbHitsTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.NestedPlanExpressionTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.NodeByElementIdSeekTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.NodeByIdSeekTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.NodeCountFromCountStoreTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.NodeFulltextIndexSearchTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.NodeHashJoinTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.NodeIndexContainsScanTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.NodeIndexEndsWithScanTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.NodeIndexPointBoundingBoxSeekTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.NodeIndexPointDistanceSeekTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.NodeIndexScanTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.NodeIndexSeekTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.NodeIndexStartsWithSeekTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.NodeLockingUniqueIndexSeekTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.NonFusedWriteOperatorsDbHitsTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.NonParallelProfileRowsTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.NonParallelProvidedOrderTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.OptionalExpandAllTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.OptionalExpandIntoTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.OptionalFailureTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.OptionalTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.OrderedAcyclicTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.OrderedAggregationTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.OrderedConditionalApplyTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.OrderedDistinctTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.OrderedSelectOrSemiApplyTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.OrderedTrailTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.OrderedUnionTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.OrderedWalkTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.PartialSortTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.PartialTop1TestBase
import org.neo4j.cypher.internal.runtime.spec.tests.PartialTopNTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ProcedureCallDbHitsTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ProcedureCallTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ProfileMemoryTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ProfileRowsTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ProjectEndpointsTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ProjectionTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ProvidedOrderTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.PruningVarLengthExpandFuzzTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.PruningVarLengthExpandTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ReactiveResultTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.RelationshipCountFromCountStoreTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.RelationshipFulltextIndexSearchTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.RelationshipIndexContainsScanTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.RelationshipIndexEndsWithScanTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.RelationshipIndexPointBoundingBoxSeekTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.RelationshipIndexScanTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.RelationshipIndexSeekTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.RelationshipIndexStartsWithSeekTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.RelationshipLockingUniqueIndexSeekTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.RelationshipTypeScanTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.RemoteNodeIndexSeekCompatibilityTestRewriter
import org.neo4j.cypher.internal.runtime.spec.tests.RemoveDynamicLabelsTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.RemoveLabelsTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.RepeatAcyclicTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.RepeatTrailTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.RepeatWalkTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.RightOuterHashJoinTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.RollupApplyTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.RunQueryAtTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.RuntimeDebugLoggingTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.RuntimeNotificationsTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.SelectOrAntiSemiApplyTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.SelectOrSemiApplyTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.SemiApplyTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.SetDynamicLabelsTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.SetDynamicPropertyTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.SetLabelsTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.SetNodePropertiesFromMapTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.SetNodePropertiesTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.SetNodePropertyTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.SetPropertiesFromMapNodeTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.SetPropertiesFromMapRelationshipTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.SetPropertiesTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.SetPropertyTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.SetRelationshipPropertiesFromMapTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.SetRelationshipPropertiesTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.SetRelationshipPropertyTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ShortestPathTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.SkipTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.SortTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.StatefulShortestPathAcyclicModeTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.StatefulShortestPathPropagationTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.StatefulShortestPathTrailModeTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.StatefulShortestPathWalkModeTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.SubqueryForeachTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.SubscriberErrorTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.SubtractionLabelScanTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.Top1WithTiesTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.TopTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.TrailProfileRowsTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.TransactionApplyTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.TransactionForeachDbHitsTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.TransactionForeachMemoryManagementTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.TransactionForeachProfileRowsTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.TransactionForeachTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.TransactionRetryTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.TransactionTerminationTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.TriadicSelectionTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.UndirectedRelationshipByElementIdSeekTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.UndirectedRelationshipByIdSeekTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.UnionLabelScanTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.UnionRelationshipTypeTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.UnionTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.UnwindTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.UserDefinedAggregationSupport
import org.neo4j.cypher.internal.runtime.spec.tests.ValueHashJoinTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.VarLengthExpandTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.WritingSubqueryApplyTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.stress.RelationshipIndexContainsScanConcurrencyStressTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.stress.RelationshipIndexEndsWithScanConcurrencyStressTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.stress.RelationshipIndexLockingUniqueSeekConcurrencyStressTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.stress.RelationshipIndexScanConcurrencyStressTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.stress.RelationshipIndexSeekConcurrencyStressTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.stress.RelationshipTypeReadConcurrencyStressTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.stress.RelationshipTypeScanConcurrencyStressTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.stress.UnionRelationshipTypesScanConcurrencyStressTestBase
import org.neo4j.cypher.internal.util.test_helpers.TimeLimitedCypherTest

object InterpretedSpecSuite {
  val SIZE_HINT = 200
}

class InterpretedAggregationTest extends AggregationTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)
    with UserDefinedAggregationSupport[CommunityRuntimeContext]

class InterpretedOrderedAggregationTest
    extends OrderedAggregationTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)

class InterpretedAllNodeScanTest extends AllNodeScanTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)
    with AllNodeScanWithOtherOperatorsTestBase[CommunityRuntimeContext]

class InterpretedCartesianProductTest
    extends CartesianProductTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)
class InterpretedApplyTest extends ApplyTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)

class InterpretedWritingSubqueryApplyTest
    extends WritingSubqueryApplyTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime)

class InterpretedNodeByIdSeekTest
    extends NodeByIdSeekTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)

class InterpretedNodeByElementIdSeekTest
    extends NodeByElementIdSeekTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)

class InterpretedDirectedRelationshipByIdSeekTest
    extends DirectedRelationshipByIdSeekTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)

class InterpretedDirectedRelationshipByElementIdSeekTest
    extends DirectedRelationshipByElementIdSeekTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)

class InterpretedUndirectedRelationshipByIdSeekTest
    extends UndirectedRelationshipByIdSeekTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)

class InterpretedUndirectedRelationshipByElementIdSeekTest
    extends UndirectedRelationshipByElementIdSeekTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)

class InterpretedNodeCountFromCountStoreTest
    extends NodeCountFromCountStoreTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime)

class InterpretedRelationshipCountFromCountStoreTest
    extends RelationshipCountFromCountStoreTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime)

class InterpretedExpandAllTest extends ExpandAllTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)
    with ExpandAllWithOtherOperatorsTestBase[CommunityRuntimeContext]

class InterpretedExpandIntoTest extends ExpandIntoTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)
    with ExpandIntoWithOtherOperatorsTestBase[CommunityRuntimeContext]

class InterpretedOptionalExpandAllTest
    extends OptionalExpandAllTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)

class InterpretedOptionalExpandIntoTest
    extends OptionalExpandIntoTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)

class InterpretedVarExpandTrailTest
    extends VarLengthExpandTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT, TraversalPathMode.Trail)

class InterpretedVarExpandWalkTest
    extends VarLengthExpandTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT, TraversalPathMode.Walk)

class InterpretedVarExpandAcyclicTest
    extends VarLengthExpandTestBase(
      COMMUNITY.EDITION,
      CommunityInterpretedRuntime,
      SIZE_HINT,
      TraversalPathMode.Acyclic
    )

class InterpretedPruningVarExpandAcyclicTest
    extends PruningVarLengthExpandTestBase(
      COMMUNITY.EDITION,
      CommunityInterpretedRuntime,
      SIZE_HINT,
      TraversalPathMode.Acyclic
    )

class InterpretedPruningVarExpandTrailTest
    extends PruningVarLengthExpandTestBase(
      COMMUNITY.EDITION,
      CommunityInterpretedRuntime,
      SIZE_HINT,
      TraversalPathMode.Trail
    )

class InterpretedPruningVarExpandWalkTest
    extends PruningVarLengthExpandTestBase(
      COMMUNITY.EDITION,
      CommunityInterpretedRuntime,
      SIZE_HINT,
      TraversalPathMode.Walk
    )

class InterpretedPruningVarExpandFuzzTest
    extends PruningVarLengthExpandFuzzTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime)

class InterpretedBFSPruningVarExpandAcyclicTest
    extends BFSPruningVarLengthExpandTestBase(
      COMMUNITY.EDITION,
      CommunityInterpretedRuntime,
      SIZE_HINT,
      TraversalPathMode.Acyclic
    )

class InterpretedBFSPruningVarExpandTrailTest
    extends BFSPruningVarLengthExpandTestBase(
      COMMUNITY.EDITION,
      CommunityInterpretedRuntime,
      SIZE_HINT,
      TraversalPathMode.Trail
    )

class InterpretedBFSPruningVarExpandWalkTest
    extends BFSPruningVarLengthExpandTestBase(
      COMMUNITY.EDITION,
      CommunityInterpretedRuntime,
      SIZE_HINT,
      TraversalPathMode.Walk
    )

class InterpretedProjectEndpointsTest
    extends ProjectEndpointsTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)
class InterpretedLabelScanTest extends LabelScanTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)

class InterpretedDynamicLabelNodeLookupTest
    extends DynamicLabelNodeLookupTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)

class InterpretedUnionLabelScanTest
    extends UnionLabelScanTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)

class InterpretedNodeIndexScanTest
    extends NodeIndexScanTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)

class InterpretedIntersectionLabelScanTest
    extends IntersectionLabelScanTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)

class InterpretedSubtractionLabelScanTest
    extends SubtractionLabelScanTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)

class InterpretedNodeIndexContainsScanTest
    extends NodeIndexContainsScanTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)

class InterpretedNodeIndexStartsWithSeekTest
    extends NodeIndexStartsWithSeekTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)

class InterpretedNodeIndexEndsWithScanTest
    extends NodeIndexEndsWithScanTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)

class InterpretedNodeIndexSeekTest
    extends NodeIndexSeekTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)
    with NodeLockingUniqueIndexSeekTestBase[CommunityRuntimeContext]

class InterpretedRemoteNodeIndexSeekCompatibilityTest
    extends NodeIndexSeekTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)
    with RemoteNodeIndexSeekCompatibilityTestRewriter[CommunityRuntimeContext]

class InterpretedNodeFulltextIndexSearchTest
    extends NodeFulltextIndexSearchTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)

class InterpretedRelationshipFulltextIndexSearchTest
    extends RelationshipFulltextIndexSearchTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)

class InterpretedRelationshipIndexSeekTest
    extends RelationshipIndexSeekTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)
    with RelationshipLockingUniqueIndexSeekTestBase[CommunityRuntimeContext]

class InterpretedRelationshipIndexScanTest
    extends RelationshipIndexScanTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)

class InterpretedRelationshipIndexStartsWithSeekTest
    extends RelationshipIndexStartsWithSeekTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)

class InterpretedRelationshipIndexContainsScanTest
    extends RelationshipIndexContainsScanTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)

class InterpretedRelationshipIndexEndsWithScanTest
    extends RelationshipIndexEndsWithScanTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)

class InterpretedNodeIndexPointDistanceSeekTest
    extends NodeIndexPointDistanceSeekTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)

class InterpretedNodeIndexPointBoundingBoxSeekTest
    extends NodeIndexPointBoundingBoxSeekTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)

class InterpretedRelationshipIndexPointBoundingBoxSeekTest
    extends RelationshipIndexPointBoundingBoxSeekTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)

class InterpretedMultiNodeIndexSeekTest
    extends MultiNodeIndexSeekTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)
class InterpretedInputTest extends InputTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)

class InterpretedLoadCsvTest extends LoadCsvTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)
    with LoadCsvWithCallInTransactionsAndMerge[CommunityRuntimeContext]
class InterpretedPartialSortTest extends PartialSortTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)
class InterpretedTopTest extends TopTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)

class InterpretedTop1WithTiesTest
    extends Top1WithTiesTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)
class InterpretedSortTest extends SortTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)
class InterpretedPartialTopNTest extends PartialTopNTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)
class InterpretedPartialTop1Test extends PartialTop1TestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)
class InterpretedFilterTest extends FilterTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)
class InterpretedArgumentTest extends ArgumentTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)
class InterpretedProjectionTest extends ProjectionTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)

class InterpretedCachePropertiesTest
    extends CachePropertiesTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT, 1)
    with CachePropertiesTxStateTestBase[CommunityRuntimeContext]
class InterpretedUnwindTest extends UnwindTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)
class InterpretedDistinctTest extends DistinctTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)

class InterpretedOrderedDistinctTest
    extends OrderedDistinctTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)
class InterpretedLimitTest extends LimitTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)

class InterpretedExhaustiveLimitTest
    extends ExhaustiveLimitTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)
class InterpretedSkipTest extends SkipTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)

class InterpretedNodeHashJoinTest
    extends NodeHashJoinTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)

class InterpretedValueHashJoinTest
    extends ValueHashJoinTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)

class InterpretedRightOuterHashJoinTest
    extends RightOuterHashJoinTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)

class InterpretedLeftOuterHashJoinTest
    extends LeftOuterHashJoinTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)
class InterpretedReactiveResultsTest extends ReactiveResultTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime)
class InterpretedMiscTest extends MiscTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime)

class InterpretedOptionalTest extends OptionalTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)
    with OptionalFailureTestBase[CommunityRuntimeContext]

class InterpretedProvidedOrderTest
    extends ProvidedOrderTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)
    with NonParallelProvidedOrderTestBase[CommunityRuntimeContext]
    with CartesianProductProvidedOrderTestBase[CommunityRuntimeContext]

class InterpretedProfileDbHitsTest
    extends LegacyDbHitsTestBase(
      COMMUNITY.EDITION,
      CommunityInterpretedRuntime,
      SIZE_HINT,
      createsRelValueInExpand = true
    )
    with ProcedureCallDbHitsTestBase[CommunityRuntimeContext]
    with NestedPlanDbHitsTestBase[CommunityRuntimeContext]
    with NonFusedWriteOperatorsDbHitsTestBase[CommunityRuntimeContext]
    with TransactionForeachDbHitsTestBase[CommunityRuntimeContext]

class InterpretedProfileRowsTest
    extends ProfileRowsTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT, 1)
    with EagerLimitProfileRowsTestBase[CommunityRuntimeContext]
    with MergeProfileRowsTestBase[CommunityRuntimeContext]
    with NonParallelProfileRowsTestBase[CommunityRuntimeContext]
    with TransactionForeachProfileRowsTestBase[CommunityRuntimeContext]
    with TrailProfileRowsTestBase[CommunityRuntimeContext]

class InterpretedMemoryManagementTest extends MemoryManagementTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime)
    with TimeLimitedCypherTest
    with FullSupportMemoryManagementTestBase[CommunityRuntimeContext]
    with TransactionForeachMemoryManagementTestBase[CommunityRuntimeContext]

class InterpretedMemoryManagementDisabledTest
    extends MemoryManagementDisabledTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime)

class InterpretedMemoryDeallocationTest
    extends MemoryDeallocationTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)

class InterpretedProfileMemoryTest extends ProfileMemoryTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime)
    with FullSupportProfileMemoryTestBase[CommunityRuntimeContext]

class InterpretedMemoryLeakTest
    extends MemoryLeakTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime)

class InterpretedSubscriberErrorTest extends SubscriberErrorTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime)

class InterpretedExpressionTest extends ExpressionTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime)
    with ExpressionWithTxStateChangesTests[CommunityRuntimeContext]

class InterpretedProcedureCallTest
    extends ProcedureCallTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)

class InterpretedShortestPathTest
    extends ShortestPathTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)

class InterpretedStatefulShortestPathTrailModeTest
    extends StatefulShortestPathTrailModeTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)

class InterpretedStatefulShortestPathWalkModeTest
    extends StatefulShortestPathWalkModeTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)

class InterpretedStatefulShortestPathAcyclicModeTest
    extends StatefulShortestPathAcyclicModeTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)

class InterpretedStatefulShortestPathPropagationTrailModeTest
    extends StatefulShortestPathPropagationTestBase(
      COMMUNITY.EDITION,
      CommunityInterpretedRuntime,
      SIZE_HINT,
      TraversalPathMode.Trail
    )

class InterpretedStatefulShortestPathPropagationWalkModeTest
    extends StatefulShortestPathPropagationTestBase(
      COMMUNITY.EDITION,
      CommunityInterpretedRuntime,
      SIZE_HINT,
      TraversalPathMode.Walk
    )

class InterpretedUnionTest extends UnionTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)

class InterpretedOrderedUnionTest
    extends OrderedUnionTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)
class InterpretedSemiApplyTest extends SemiApplyTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)

class InterpretedAntiSemiApplyTest
    extends AntiSemiApplyTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)

class InterpretedLetAntiSemiApplyTest
    extends LetAntiSemiApplyTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)

class InterpretedLetSemiApplyTest
    extends LetSemiApplyTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)

class InterpretedConditionalApplyTest
    extends ConditionalApplyTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)
    with OrderedConditionalApplyTestBase[CommunityRuntimeContext]

class InterpretedAntiConditionalApplyTest
    extends AntiConditionalApplyTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)

class InterpretedSelectOrSemiApplyTest
    extends SelectOrSemiApplyTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)
    with OrderedSelectOrSemiApplyTestBase[CommunityRuntimeContext]

class InterpretedSelectOrAntiSemiApplyTest
    extends SelectOrAntiSemiApplyTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)

class InterpretedLetSelectOrSemiApplyTest
    extends LetSelectOrSemiApplyTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)

class InterpretedLetSelectOrAntiSemiApplyTest
    extends LetSelectOrAntiSemiApplyTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)

class InterpretedNestedPlanExpressionTest
    extends NestedPlanExpressionTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)
class InterpretedRollupApplyTest extends RollupApplyTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)
class InterpretedRunQueryAtTest extends RunQueryAtTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)

class InterpretedAllRelationshipsScanTest
    extends AllRelationshipsScanTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)

class InterpretedRelationshipTypeScanTest
    extends RelationshipTypeScanTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)

class InterpretedDynamicRelationshipTypeLookupTest
    extends DynamicRelationshipTypeLookupTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)

class InterpretedUnionRelationshipTypeTest
    extends UnionRelationshipTypeTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)

class InterpretedRepeatAcyclicTest
    extends RepeatAcyclicTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)
    with OrderedAcyclicTestBase[CommunityRuntimeContext]

class InterpretedRepeatTrailTest
    extends RepeatTrailTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)
    with OrderedTrailTestBase[CommunityRuntimeContext]

class InterpretedRepeatWalkTest
    extends RepeatWalkTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)
    with OrderedWalkTestBase[CommunityRuntimeContext]

//UPDATING
class InterpretedEmptyResultTest extends EmptyResultTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)
class InterpretedEagerTest extends EagerTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)

class InterpretedTriadicSelectionTest
    extends TriadicSelectionTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)

class InterpretedAssertSameNodeTest
    extends AssertSameNodeTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)
    with EsotericAssertSameNodeTestBase[CommunityRuntimeContext]

class InterpretedAssertSameRelationshipTest
    extends AssertSameRelationshipTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)
class InterpretedCreateTest extends CreateTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)

class InterpretedLenientCreateRelationshipTest
    extends LenientCreateRelationshipTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime)
class InterpretedSetPropertyTest extends SetPropertyTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)

class InterpretedSetDynamicPropertyTest
    extends SetDynamicPropertyTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)

class InterpretedSetPropertiesTest
    extends SetPropertiesTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)

class InterpretedSetPropertiesFromMapNodeTest
    extends SetPropertiesFromMapNodeTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)

class InterpretedSetPropertiesFromMapRelationshipTest
    extends SetPropertiesFromMapRelationshipTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)

class InterpretedSetNodePropertyTest
    extends SetNodePropertyTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)

class InterpretedSetNodePropertiesTest
    extends SetNodePropertiesTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)

class InterpretedSetNodePropertiesFromMapTest
    extends SetNodePropertiesFromMapTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)

class InterpretedSetRelationshipPropertiesFromMapTest
    extends SetRelationshipPropertiesFromMapTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)
class InterpretedLockNodesTest extends LockNodesTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)

class InterpretedMergeIntoTest
    extends MergeIntoTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)

class InterpretedMergeUniqueNodeTest
    extends MergeUniqueNodeTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)

class InterpretedMergeTest extends MergeTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)
class InterpretedMergeStressTest extends MergeStressTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime)
class InterpretedSetLabelsTest extends SetLabelsTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)

class InterpretedSetDynamicLabelsTest
    extends SetDynamicLabelsTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)

class InterpretedForEachTest extends ForeachTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)

class InterpretedForEachApplyTest
    extends ForeachApplyTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)

class InterpretedSubqueryForeachTest
    extends SubqueryForeachTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)

class InterpretedTransactionRetryTest
    extends TransactionRetryTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)

class InterpretedTransactionForeachTest
    extends TransactionForeachTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)

class InterpretedTransactionApplyTest
    extends TransactionApplyTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)

class InterpretedConcurrentTransactionForeachTest
    extends ConcurrentTransactionForeachTestBase(
      COMMUNITY.EDITION,
      CommunityInterpretedRuntime,
      SIZE_HINT,
      concurrency = TransactionConcurrency.Concurrent(None)
    )

class InterpretedConcurrentTransactionApplyTest
    extends ConcurrentTransactionApplyTestBase(
      COMMUNITY.EDITION,
      CommunityInterpretedRuntime,
      SIZE_HINT,
      concurrency = TransactionConcurrency.Concurrent(None)
    )

class InterpretedSetRelationshipPropertyTest
    extends SetRelationshipPropertyTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)

class InterpretedSetRelationshipPropertiesTest
    extends SetRelationshipPropertiesTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)
class InterpretedDeleteNodeTest extends DeleteNodeTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)

class InterpretedDetachDeleteNodeTest
    extends DeleteDetachNodeTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)

class InterpretedDeleteRelationshipTest
    extends DeleteRelationshipTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)
class InterpretedDeletePathTest extends DeletePathTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)

class InterpretedDeleteDetachPathTest
    extends DeleteDetachPathTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)

class InterpretedDeleteExpressionTest
    extends DeleteExpressionTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)

class InterpretedDeleteDetachExpressionTest
    extends DeleteDetachExpressionTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)

class InterpretedRemoveLabelsTest
    extends RemoveLabelsTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)

class InterpretedRemoveDynamicLabelsTest
    extends RemoveDynamicLabelsTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)

// CONCURRENT UPDATE STRESS TESTS
class InterpretedRelationshipTypeScanConcurrencyStressTest
    extends RelationshipTypeScanConcurrencyStressTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime)

class InterpretedUnionRelationshipTypesScanConcurrencyStressTest
    extends UnionRelationshipTypesScanConcurrencyStressTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime)

class InterpretedRelationshipIndexScanConcurrencyStressTest
    extends RelationshipIndexScanConcurrencyStressTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime)

class InterpretedRelationshipTypeReadConcurrencyStressTestBase
    extends RelationshipTypeReadConcurrencyStressTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime)

class InterpretedRelationshipIndexContainsScanConcurrencyStressTest
    extends RelationshipIndexContainsScanConcurrencyStressTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime)

class InterpretedRelationshipIndexEndsWithScanConcurrencyStressTest
    extends RelationshipIndexEndsWithScanConcurrencyStressTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime)

class InterpretedRelationshipIndexSeekConcurrencyStressTest
    extends RelationshipIndexSeekConcurrencyStressTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime)

class InterpretedRelationshipIndexLockingUniqueSeekConcurrencyStressTest
    extends RelationshipIndexLockingUniqueSeekConcurrencyStressTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime)

class CommunityInterpretedRuntimeDebugLoggingTest
    extends RuntimeDebugLoggingTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime, SIZE_HINT)

class InterpretedTransactionTerminationTest
    extends TransactionTerminationTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime)

class CommunityInterpretedRuntimeNotificationsTest
    extends RuntimeNotificationsTestBase(COMMUNITY.EDITION, CommunityInterpretedRuntime)
