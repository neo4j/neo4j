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

import org.neo4j.cypher.internal.runtime.spec.COMMUNITY
import org.neo4j.cypher.internal.runtime.spec.interpreted.InterpretedSpecSuite.SIZE_HINT
import org.neo4j.cypher.internal.runtime.spec.tests._
import org.neo4j.cypher.internal.{CommunityRuntimeContext, InterpretedRuntime}

object InterpretedSpecSuite {
  val SIZE_HINT = 200
}

class InterpretedAggregationTest extends AggregationTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedOrderedAggregationTest extends OrderedAggregationTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedAllNodeScanTest extends AllNodeScanTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
                                 with AllNodeScanWithOtherOperatorsTestBase[CommunityRuntimeContext]
class InterpretedCartesianProductTest extends CartesianProductTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
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
class InterpretedNodeIndexEndsWithScanTest extends NodeIndexEndsWithScanTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedNodeIndexSeekTest extends NodeIndexSeekTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
                                   with NodeIndexSeekRangeAndCompositeTestBase[CommunityRuntimeContext]
                                   with NodeLockingUniqueIndexSeekTestBase[CommunityRuntimeContext]
                                   with ArrayIndexSupport[CommunityRuntimeContext]
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
class InterpretedNodeHashJoinTest extends NodeHashJoinTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedValueHashJoinTest extends ValueHashJoinTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedReactiveResultsTest extends ReactiveResultTestBase(COMMUNITY.EDITION, InterpretedRuntime)
class InterpretedMiscTest extends MiscTestBase(COMMUNITY.EDITION, InterpretedRuntime)
class InterpretedOptionalTest extends OptionalTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedProvidedOrderTest extends ProvidedOrderTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
                                   with CartesianProductProvidedOrderTestBase[CommunityRuntimeContext]
class InterpretedProfileDbHitsTest extends LegacyDbHitsTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
                                   with ProcedureCallDbHitsTestBase[CommunityRuntimeContext]
class InterpretedProfileRowsTest extends ProfileRowsTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT, 1)
class InterpretedMemoryManagementTest extends MemoryManagementTestBase(COMMUNITY.EDITION, InterpretedRuntime)
                                      with FullSupportMemoryManagementTestBase[CommunityRuntimeContext]
class InterpretedMemoryManagementDisabledTest extends MemoryManagementDisabledTestBase(COMMUNITY.EDITION, InterpretedRuntime)
class InterpretedSubscriberErrorTest extends SubscriberErrorTestBase(COMMUNITY.EDITION, InterpretedRuntime)
class InterpretedExpressionTest extends ExpressionTestBase(COMMUNITY.EDITION, InterpretedRuntime)
                                with ExpressionWithTxStateChangesTests[CommunityRuntimeContext]
