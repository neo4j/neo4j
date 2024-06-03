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

import org.neo4j.cypher.internal.CommunityRuntimeContext
import org.neo4j.cypher.internal.InterpretedRuntime
import org.neo4j.cypher.internal.logical.plans.TransactionConcurrency
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
import org.neo4j.cypher.internal.runtime.spec.tests.MemoryDeallocationTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.MemoryLeakTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.MemoryManagementDisabledTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.MemoryManagementTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.MergeProfileRowsTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.MergeStressTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.MergeTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.MiscTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.MultiNodeIndexSeekTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.NestedPlanDbHitsTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.NestedPlanExpressionTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.NodeByElementIdSeekTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.NodeByIdSeekTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.NodeCountFromCountStoreTestBase
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
import org.neo4j.cypher.internal.runtime.spec.tests.OrderedAggregationTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.OrderedConditionalApplyTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.OrderedDistinctTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.OrderedTrailTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.OrderedUnionTestBase
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
import org.neo4j.cypher.internal.runtime.spec.tests.RelationshipIndexContainsScanTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.RelationshipIndexEndsWithScanTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.RelationshipIndexPointBoundingBoxSeekTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.RelationshipIndexScanTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.RelationshipIndexSeekTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.RelationshipIndexStartsWithSeekTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.RelationshipLockingUniqueIndexSeekTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.RelationshipTypeScanTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.RemoveLabelsTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.RightOuterHashJoinTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.RollupApplyTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.RunQueryAtTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.RuntimeDebugLoggingTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.SelectOrAntiSemiApplyTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.SelectOrSemiApplyTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.SemiApplyTestBase
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
import org.neo4j.cypher.internal.runtime.spec.tests.StatefulShortestPathPropagationTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.StatefulShortestPathTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.SubqueryForeachTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.SubscriberErrorTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.SubtractionLabelScanTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.Top1WithTiesTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.TopTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.TrailProfileRowsTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.TrailTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.TransactionApplyTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.TransactionForeachDbHitsTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.TransactionForeachMemoryManagementTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.TransactionForeachProfileRowsTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.TransactionForeachTestBase
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

class InterpretedAggregationTest extends AggregationTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
    with UserDefinedAggregationSupport[CommunityRuntimeContext]

class InterpretedOrderedAggregationTest
    extends OrderedAggregationTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)

class InterpretedAllNodeScanTest extends AllNodeScanTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
    with AllNodeScanWithOtherOperatorsTestBase[CommunityRuntimeContext]
class InterpretedCartesianProductTest extends CartesianProductTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedApplyTest extends ApplyTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedWritingSubqueryApplyTest extends WritingSubqueryApplyTestBase(COMMUNITY.EDITION, InterpretedRuntime)
class InterpretedNodeByIdSeekTest extends NodeByIdSeekTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)

class InterpretedNodeByElementIdSeekTest
    extends NodeByElementIdSeekTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)

class InterpretedDirectedRelationshipByIdSeekTest
    extends DirectedRelationshipByIdSeekTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)

class InterpretedDirectedRelationshipByElementIdSeekTest
    extends DirectedRelationshipByElementIdSeekTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)

class InterpretedUndirectedRelationshipByIdSeekTest
    extends UndirectedRelationshipByIdSeekTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)

class InterpretedUndirectedRelationshipByElementIdSeekTest
    extends UndirectedRelationshipByElementIdSeekTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)

class InterpretedNodeCountFromCountStoreTest
    extends NodeCountFromCountStoreTestBase(COMMUNITY.EDITION, InterpretedRuntime)

class InterpretedRelationshipCountFromCountStoreTest
    extends RelationshipCountFromCountStoreTestBase(COMMUNITY.EDITION, InterpretedRuntime)

class InterpretedExpandAllTest extends ExpandAllTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
    with ExpandAllWithOtherOperatorsTestBase[CommunityRuntimeContext]

class InterpretedExpandIntoTest extends ExpandIntoTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
    with ExpandIntoWithOtherOperatorsTestBase[CommunityRuntimeContext]

class InterpretedOptionalExpandAllTest
    extends OptionalExpandAllTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)

class InterpretedOptionalExpandIntoTest
    extends OptionalExpandIntoTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedVarExpandAllTest extends VarLengthExpandTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)

class InterpretedPruningVarExpandTest
    extends PruningVarLengthExpandTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)

class InterpretedPruningVarExpandFuzzTest
    extends PruningVarLengthExpandFuzzTestBase(COMMUNITY.EDITION, InterpretedRuntime)

class InterpretedBFSPruningVarExpandTest
    extends BFSPruningVarLengthExpandTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedProjectEndpointsTest extends ProjectEndpointsTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedLabelScanTest extends LabelScanTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedUnionLabelScanTest extends UnionLabelScanTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedNodeIndexScanTest extends NodeIndexScanTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)

class InterpretedIntersectionLabelScanTest
    extends IntersectionLabelScanTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)

class InterpretedSubtractionLabelScanTest
    extends SubtractionLabelScanTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)

class InterpretedNodeIndexContainsScanTest
    extends NodeIndexContainsScanTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)

class InterpretedNodeIndexStartsWithSeekTest
    extends NodeIndexStartsWithSeekTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)

class InterpretedNodeIndexEndsWithScanTest
    extends NodeIndexEndsWithScanTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)

class InterpretedNodeIndexSeekTest extends NodeIndexSeekTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
    with NodeLockingUniqueIndexSeekTestBase[CommunityRuntimeContext]

class InterpretedRelationshipIndexSeekTest
    extends RelationshipIndexSeekTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
    with RelationshipLockingUniqueIndexSeekTestBase[CommunityRuntimeContext]

class InterpretedRelationshipIndexScanTest
    extends RelationshipIndexScanTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)

class InterpretedRelationshipIndexStartsWithSeekTest
    extends RelationshipIndexStartsWithSeekTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)

class InterpretedRelationshipIndexContainsScanTest
    extends RelationshipIndexContainsScanTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)

class InterpretedRelationshipIndexEndsWithScanTest
    extends RelationshipIndexEndsWithScanTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)

class InterpretedNodeIndexPointDistanceSeekTest
    extends NodeIndexPointDistanceSeekTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)

class InterpretedNodeIndexPointBoundingBoxSeekTest
    extends NodeIndexPointBoundingBoxSeekTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)

class InterpretedRelationshipIndexPointBoundingBoxSeekTest
    extends RelationshipIndexPointBoundingBoxSeekTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)

class InterpretedMultiNodeIndexSeekTest
    extends MultiNodeIndexSeekTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedInputTest extends InputTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)

class InterpretedLoadCsvTest extends LoadCsvTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
    with LoadCsvWithCallInTransactionsAndMerge[CommunityRuntimeContext]
class InterpretedPartialSortTest extends PartialSortTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedTopTest extends TopTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedTop1WithTiesTest extends Top1WithTiesTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedSortTest extends SortTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedPartialTopNTest extends PartialTopNTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedPartialTop1Test extends PartialTop1TestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedFilterTest extends FilterTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedArgumentTest extends ArgumentTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedProjectionTest extends ProjectionTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)

class InterpretedCachePropertiesTest
    extends CachePropertiesTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT, 1)
    with CachePropertiesTxStateTestBase[CommunityRuntimeContext]
class InterpretedUnwindTest extends UnwindTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedDistinctTest extends DistinctTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedOrderedDistinctTest extends OrderedDistinctTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedLimitTest extends LimitTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedExhaustiveLimitTest extends ExhaustiveLimitTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedSkipTest extends SkipTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedNodeHashJoinTest extends NodeHashJoinTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedValueHashJoinTest extends ValueHashJoinTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)

class InterpretedRightOuterHashJoinTest
    extends RightOuterHashJoinTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)

class InterpretedLeftOuterHashJoinTest
    extends LeftOuterHashJoinTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedReactiveResultsTest extends ReactiveResultTestBase(COMMUNITY.EDITION, InterpretedRuntime)
class InterpretedMiscTest extends MiscTestBase(COMMUNITY.EDITION, InterpretedRuntime)

class InterpretedOptionalTest extends OptionalTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
    with OptionalFailureTestBase[CommunityRuntimeContext]

class InterpretedProvidedOrderTest extends ProvidedOrderTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
    with NonParallelProvidedOrderTestBase[CommunityRuntimeContext]
    with CartesianProductProvidedOrderTestBase[CommunityRuntimeContext]

class InterpretedProfileDbHitsTest
    extends LegacyDbHitsTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT, createsRelValueInExpand = true)
    with ProcedureCallDbHitsTestBase[CommunityRuntimeContext]
    with NestedPlanDbHitsTestBase[CommunityRuntimeContext]
    with NonFusedWriteOperatorsDbHitsTestBase[CommunityRuntimeContext]
    with TransactionForeachDbHitsTestBase[CommunityRuntimeContext]

class InterpretedProfileRowsTest extends ProfileRowsTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT, 1)
    with EagerLimitProfileRowsTestBase[CommunityRuntimeContext]
    with MergeProfileRowsTestBase[CommunityRuntimeContext]
    with NonParallelProfileRowsTestBase[CommunityRuntimeContext]
    with TransactionForeachProfileRowsTestBase[CommunityRuntimeContext]
    with TrailProfileRowsTestBase[CommunityRuntimeContext]

class InterpretedMemoryManagementTest extends MemoryManagementTestBase(COMMUNITY.EDITION, InterpretedRuntime)
    with TimeLimitedCypherTest
    with FullSupportMemoryManagementTestBase[CommunityRuntimeContext]
    with TransactionForeachMemoryManagementTestBase[CommunityRuntimeContext]

class InterpretedMemoryManagementDisabledTest
    extends MemoryManagementDisabledTestBase(COMMUNITY.EDITION, InterpretedRuntime)

class InterpretedMemoryDeallocationTest
    extends MemoryDeallocationTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)

class InterpretedProfileMemoryTest extends ProfileMemoryTestBase(COMMUNITY.EDITION, InterpretedRuntime)
    with FullSupportProfileMemoryTestBase[CommunityRuntimeContext]

class InterpretedMemoryLeakTest
    extends MemoryLeakTestBase(COMMUNITY.EDITION, InterpretedRuntime)

class InterpretedSubscriberErrorTest extends SubscriberErrorTestBase(COMMUNITY.EDITION, InterpretedRuntime)

class InterpretedExpressionTest extends ExpressionTestBase(COMMUNITY.EDITION, InterpretedRuntime)
    with ExpressionWithTxStateChangesTests[CommunityRuntimeContext]
class InterpretedProcedureCallTest extends ProcedureCallTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedShortestPathTest extends ShortestPathTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)

class InterpretedStatefulShortestPathTest
    extends StatefulShortestPathTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)

class InterpretedStatefulShortestPathPropagationTest
    extends StatefulShortestPathPropagationTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedUnionTest extends UnionTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedOrderedUnionTest extends OrderedUnionTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedSemiApplyTest extends SemiApplyTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedAntiSemiApplyTest extends AntiSemiApplyTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedLetAntiSemiApplyTest extends LetAntiSemiApplyTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedLetSemiApplyTest extends LetSemiApplyTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)

class InterpretedConditionalApplyTest
    extends ConditionalApplyTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
    with OrderedConditionalApplyTestBase[CommunityRuntimeContext]

class InterpretedAntiConditionalApplyTest
    extends AntiConditionalApplyTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)

class InterpretedSelectOrSemiApplyTest
    extends SelectOrSemiApplyTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)

class InterpretedSelectOrAntiSemiApplyTest
    extends SelectOrAntiSemiApplyTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)

class InterpretedLetSelectOrSemiApplyTest
    extends LetSelectOrSemiApplyTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)

class InterpretedLetSelectOrAntiSemiApplyTest
    extends LetSelectOrAntiSemiApplyTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)

class InterpretedNestedPlanExpressionTest
    extends NestedPlanExpressionTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedRollupApplyTest extends RollupApplyTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedRunQueryAtTest extends RunQueryAtTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)

class InterpretedAllRelationshipsScanTest
    extends AllRelationshipsScanTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)

class InterpretedRelationshipTypeScanTest
    extends RelationshipTypeScanTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)

class InterpretedUnionRelationshipTypeTest
    extends UnionRelationshipTypeTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)

class InterpretedTrailTest
    extends TrailTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
    with OrderedTrailTestBase[CommunityRuntimeContext]

//UPDATING
class InterpretedEmptyResultTest extends EmptyResultTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedEagerTest extends EagerTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedTriadicSelectionTest extends TriadicSelectionTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)

class InterpretedAssertSameNodeTest extends AssertSameNodeTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
    with EsotericAssertSameNodeTestBase[CommunityRuntimeContext]

class InterpretedAssertSameRelationshipTest
    extends AssertSameRelationshipTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedCreateTest extends CreateTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)

class InterpretedLenientCreateRelationshipTest
    extends LenientCreateRelationshipTestBase(COMMUNITY.EDITION, InterpretedRuntime)
class InterpretedSetPropertyTest extends SetPropertyTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)

class InterpretedSetDynamicPropertyTest
    extends SetDynamicPropertyTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedSetPropertiesTest extends SetPropertiesTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)

class InterpretedSetPropertiesFromMapNodeTest
    extends SetPropertiesFromMapNodeTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)

class InterpretedSetPropertiesFromMapRelationshipTest
    extends SetPropertiesFromMapRelationshipTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedSetNodePropertyTest extends SetNodePropertyTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)

class InterpretedSetNodePropertiesTest
    extends SetNodePropertiesTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)

class InterpretedSetNodePropertiesFromMapTest
    extends SetNodePropertiesFromMapTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)

class InterpretedSetRelationshipPropertiesFromMapTest
    extends SetRelationshipPropertiesFromMapTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedMergeTest extends MergeTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedMergeStressTest extends MergeStressTestBase(COMMUNITY.EDITION, InterpretedRuntime)
class InterpretedSetLabelsTest extends SetLabelsTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)

class InterpretedForEachTest extends ForeachTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedForEachApplyTest extends ForeachApplyTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedSubqueryForeachTest extends SubqueryForeachTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)

class InterpretedTransactionForeachTest
    extends TransactionForeachTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)

class InterpretedTransactionApplyTest
    extends TransactionApplyTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)

class InterpretedConcurrentTransactionForeachTest
    extends ConcurrentTransactionForeachTestBase(
      COMMUNITY.EDITION,
      InterpretedRuntime,
      SIZE_HINT,
      concurrency = TransactionConcurrency.Concurrent(None)
    )

class InterpretedConcurrentTransactionApplyTest
    extends ConcurrentTransactionApplyTestBase(
      COMMUNITY.EDITION,
      InterpretedRuntime,
      SIZE_HINT,
      concurrency = TransactionConcurrency.Concurrent(None)
    )

class InterpretedSetRelationshipPropertyTest
    extends SetRelationshipPropertyTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)

class InterpretedSetRelationshipPropertiesTest
    extends SetRelationshipPropertiesTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedDeleteNodeTest extends DeleteNodeTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedDetachDeleteNodeTest extends DeleteDetachNodeTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)

class InterpretedDeleteRelationshipTest
    extends DeleteRelationshipTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedDeletePathTest extends DeletePathTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedDeleteDetachPathTest extends DeleteDetachPathTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedDeleteExpressionTest extends DeleteExpressionTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)

class InterpretedDeleteDetachExpressionTest
    extends DeleteDetachExpressionTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedRemoveLabelsTest extends RemoveLabelsTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)

// CONCURRENT UPDATE STRESS TESTS
class InterpretedRelationshipTypeScanConcurrencyStressTest
    extends RelationshipTypeScanConcurrencyStressTestBase(COMMUNITY.EDITION, InterpretedRuntime)

class InterpretedUnionRelationshipTypesScanConcurrencyStressTest
    extends UnionRelationshipTypesScanConcurrencyStressTestBase(COMMUNITY.EDITION, InterpretedRuntime)

class InterpretedRelationshipIndexScanConcurrencyStressTest
    extends RelationshipIndexScanConcurrencyStressTestBase(COMMUNITY.EDITION, InterpretedRuntime)

class InterpretedRelationshipTypeReadConcurrencyStressTestBase
    extends RelationshipTypeReadConcurrencyStressTestBase(COMMUNITY.EDITION, InterpretedRuntime)

class InterpretedRelationshipIndexContainsScanConcurrencyStressTest
    extends RelationshipIndexContainsScanConcurrencyStressTestBase(COMMUNITY.EDITION, InterpretedRuntime)

class InterpretedRelationshipIndexEndsWithScanConcurrencyStressTest
    extends RelationshipIndexEndsWithScanConcurrencyStressTestBase(COMMUNITY.EDITION, InterpretedRuntime)

class InterpretedRelationshipIndexSeekConcurrencyStressTest
    extends RelationshipIndexSeekConcurrencyStressTestBase(COMMUNITY.EDITION, InterpretedRuntime)

class InterpretedRelationshipIndexLockingUniqueSeekConcurrencyStressTest
    extends RelationshipIndexLockingUniqueSeekConcurrencyStressTestBase(COMMUNITY.EDITION, InterpretedRuntime)

class InterpretedRuntimeDebugLoggingTest
    extends RuntimeDebugLoggingTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
