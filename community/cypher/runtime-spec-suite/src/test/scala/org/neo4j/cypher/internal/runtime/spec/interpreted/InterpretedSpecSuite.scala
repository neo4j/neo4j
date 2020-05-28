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
package org.neo4j.cypher.internal.runtime.spec.interpreted

import org.neo4j.cypher.internal.CommunityRuntimeContext
import org.neo4j.cypher.internal.InterpretedRuntime
import org.neo4j.cypher.internal.runtime.spec.COMMUNITY
import org.neo4j.cypher.internal.runtime.spec.interpreted.InterpretedSpecSuite.SIZE_HINT
import org.neo4j.cypher.internal.runtime.spec.tests.AggregationTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.AllNodeScanTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.AllNodeScanWithOtherOperatorsTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.AntiSemiApplyTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ApplyTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ArgumentTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ArrayIndexSupport
import org.neo4j.cypher.internal.runtime.spec.tests.CachePropertiesTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.CartesianProductProvidedOrderTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.CartesianProductTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ConditionalApplyTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.DirectedRelationshipByIdSeekTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.DistinctTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ExpandAllTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ExpandAllWithOtherOperatorsTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ExpandIntoTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ExpandIntoWithOtherOperatorsTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ExpressionTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ExpressionWithTxStateChangesTests
import org.neo4j.cypher.internal.runtime.spec.tests.FilterTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.FullSupportMemoryManagementTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.FullSupportProfileMemoryTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.InputTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.LabelScanTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.LeftOuterHashJoinTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.LimitTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.MemoryDeallocationTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.MemoryManagementDisabledTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.MemoryManagementTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.MiscTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.MultiNodeIndexSeekTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.NestedPlanDbHitsTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.NestedPlanExpressionTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.NodeByIdSeekTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.NodeCountFromCountStoreTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.NodeHashJoinTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.NodeIndexContainsScanTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.NodeIndexEndsWithScanTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.NodeIndexPointDistanceSeekTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.NodeIndexScanTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.NodeIndexSeekRangeAndCompositeTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.NodeIndexSeekTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.NodeIndexStartsWithSeekTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.NodeLockingUniqueIndexSeekTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.NonParallelProfileRowsTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.OptionalExpandAllTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.OptionalExpandIntoTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.OptionalFailureTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.OptionalTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.OrderedAggregationTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.OrderedDistinctTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.PartialSortTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.PartialTop1TestBase
import org.neo4j.cypher.internal.runtime.spec.tests.PartialTopNTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ProcedureCallDbHitsTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ProcedureCallTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ProfileMemoryTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ProfileMemoryTrackingDisabledTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ProfileRowsTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ProjectEndpointsTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ProjectionTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ProvidedOrderTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.PruningVarLengthExpandTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ReactiveResultTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.RelationshipCountFromCountStoreTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.RightOuterHashJoinTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.RollupApplyTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.SelectOrSemiApplyTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.SemiApplyTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ShortestPathTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.SkipTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.SortTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.SubscriberErrorTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ThreadUnsafeExpressionTests
import org.neo4j.cypher.internal.runtime.spec.tests.TopTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.UndirectedRelationshipByIdSeekTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.UnionTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.UnwindTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ValueHashJoinTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.VarLengthExpandTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.WriteProcedureCallTestBase

object InterpretedSpecSuite {
  val SIZE_HINT = 200
}

class InterpretedAggregationTest extends AggregationTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedOrderedAggregationTest extends OrderedAggregationTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedAllNodeScanTest extends AllNodeScanTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
                                 with AllNodeScanWithOtherOperatorsTestBase[CommunityRuntimeContext]
class InterpretedCartesianProductTest extends CartesianProductTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedApplyTest extends ApplyTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedNodeByIdSeekTest extends NodeByIdSeekTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedDirectedRelationshipByIdSeekTest extends DirectedRelationshipByIdSeekTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedUndirectedRelationshipByIdSeekTest extends UndirectedRelationshipByIdSeekTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedNodeCountFromCountStoreTest extends NodeCountFromCountStoreTestBase(COMMUNITY.EDITION, InterpretedRuntime)
class InterpretedRelationshipCountFromCountStoreTest extends RelationshipCountFromCountStoreTestBase(COMMUNITY.EDITION, InterpretedRuntime)
class InterpretedExpandAllTest extends ExpandAllTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
                               with ExpandAllWithOtherOperatorsTestBase[CommunityRuntimeContext]
class InterpretedExpandIntoTest extends ExpandIntoTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
                                with ExpandIntoWithOtherOperatorsTestBase[CommunityRuntimeContext]
class InterpretedOptionalExpandAllTest extends OptionalExpandAllTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedOptionalExpandIntoTest extends OptionalExpandIntoTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedVarExpandAllTest extends VarLengthExpandTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedPruningVarExpandTest extends PruningVarLengthExpandTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedProjectEndpointsTest extends ProjectEndpointsTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedLabelScanTest extends LabelScanTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedNodeIndexScanTest extends NodeIndexScanTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedNodeIndexContainsScanTest extends NodeIndexContainsScanTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedNodeIndexStartsWithSeekTest extends NodeIndexStartsWithSeekTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedNodeIndexEndsWithScanTest extends NodeIndexEndsWithScanTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedNodeIndexSeekTest extends NodeIndexSeekTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
                                   with NodeIndexSeekRangeAndCompositeTestBase[CommunityRuntimeContext]
                                   with NodeLockingUniqueIndexSeekTestBase[CommunityRuntimeContext]
                                   with ArrayIndexSupport[CommunityRuntimeContext]
class InterpretedNodeIndexPointDistanceSeekTest extends NodeIndexPointDistanceSeekTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedMultiNodeIndexSeekTest extends MultiNodeIndexSeekTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedInputTest extends InputTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedPartialSortTest extends PartialSortTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedTopTest extends TopTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedSortTest extends SortTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedPartialTopNTest extends PartialTopNTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedPartialTop1Test extends PartialTop1TestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedFilterTest extends FilterTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedArgumentTest extends ArgumentTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedProjectionTest extends ProjectionTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedCachePropertiesTest extends CachePropertiesTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedUnwindTest extends UnwindTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedDistinctTest extends DistinctTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedOrderedDistinctTest extends OrderedDistinctTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedLimitTest extends LimitTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedSkipTest extends SkipTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedNodeHashJoinTest extends NodeHashJoinTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedValueHashJoinTest extends ValueHashJoinTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedRightOuterHashJoinTest extends RightOuterHashJoinTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedLeftOuterHashJoinTest extends LeftOuterHashJoinTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedReactiveResultsTest extends ReactiveResultTestBase(COMMUNITY.EDITION, InterpretedRuntime)
class InterpretedMiscTest extends MiscTestBase(COMMUNITY.EDITION, InterpretedRuntime)
class InterpretedOptionalTest extends OptionalTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
                              with OptionalFailureTestBase[CommunityRuntimeContext]
class InterpretedProvidedOrderTest extends ProvidedOrderTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
                                   with CartesianProductProvidedOrderTestBase[CommunityRuntimeContext]
class InterpretedProfileDbHitsTest extends LegacyDbHitsTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
                                   with ProcedureCallDbHitsTestBase[CommunityRuntimeContext]
                                   with NestedPlanDbHitsTestBase[CommunityRuntimeContext]
class InterpretedProfileRowsTest extends ProfileRowsTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT, 1)
                                 with NonParallelProfileRowsTestBase[CommunityRuntimeContext]
class InterpretedMemoryManagementTest extends MemoryManagementTestBase(COMMUNITY.EDITION, InterpretedRuntime)
                                      with FullSupportMemoryManagementTestBase[CommunityRuntimeContext]
class InterpretedMemoryManagementDisabledTest extends MemoryManagementDisabledTestBase(COMMUNITY.EDITION, InterpretedRuntime)
class InterpretedMemoryDeallocationTest extends MemoryDeallocationTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedProfileMemoryTest extends ProfileMemoryTestBase(COMMUNITY.EDITION, InterpretedRuntime)
                                   with FullSupportProfileMemoryTestBase[CommunityRuntimeContext]
class InterpretedProfileMemoryTrackingDisabledTest extends ProfileMemoryTrackingDisabledTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedSubscriberErrorTest extends SubscriberErrorTestBase(COMMUNITY.EDITION, InterpretedRuntime)

class InterpretedExpressionTest extends ExpressionTestBase(COMMUNITY.EDITION, InterpretedRuntime)
                                with ThreadUnsafeExpressionTests[CommunityRuntimeContext]
                                with ExpressionWithTxStateChangesTests[CommunityRuntimeContext]
class InterpretedProcedureCallTest extends ProcedureCallTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
                                   with WriteProcedureCallTestBase[CommunityRuntimeContext]
class InterpretedShortestPathTest extends ShortestPathTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedUnionTest extends UnionTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedSemiApplyTest extends SemiApplyTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedAntiSemiApplyTest extends AntiSemiApplyTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedConditionalApplyTest extends ConditionalApplyTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)

class InterpretedSelectOrSemiApplyTest extends SelectOrSemiApplyTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedNestedPlanExpressionTest extends NestedPlanExpressionTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedRollupApplyTest extends RollupApplyTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
