/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
class InterpretedNodeByIdSeekTest extends NodeByIdSeekTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedNodeCountFromCountStoreTest extends NodeCountFromCountStoreTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedExpandAllTest extends ExpandAllTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
                               with ExpandAllWithOptionalTestBase[CommunityRuntimeContext]
class InterpretedLabelScanTest extends LabelScanTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedNodeIndexScanTest extends NodeIndexScanTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedNodeIndexContainsScanTest extends NodeIndexContainsScanTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedNodeIndexSeekTest extends NodeIndexSeekTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
                                   with NodeIndexSeekRangeAndCompositeTestBase[CommunityRuntimeContext]
                                   with NodeLockingUniqueIndexSeekTestBase[CommunityRuntimeContext]
class InterpretedInputTest extends InputTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedPartialSortTest extends PartialSortTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedSortTest extends SortTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedPartialTopNTest extends PartialTopNTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedPartialTop1Test extends PartialTop1TestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedFilterTest extends FilterTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedArgumentTest extends ArgumentTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedProjectionTest extends ProjectionTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedUnwindTest extends UnwindTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedDistinctTest extends DistinctTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedOrderedDistinctTest extends OrderedDistinctTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedLimitTest extends LimitTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedNodeHashJoinTest extends NodeHashJoinTestBase(COMMUNITY.EDITION, InterpretedRuntime, SIZE_HINT)
class InterpretedReactiveResultsTest extends ReactiveResultTestBase(COMMUNITY.EDITION, InterpretedRuntime)
class InterpretedMiscTest extends MiscTestBase(COMMUNITY.EDITION, InterpretedRuntime)

