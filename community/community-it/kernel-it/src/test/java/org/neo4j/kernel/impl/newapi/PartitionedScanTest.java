/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.kernel.impl.newapi;

import org.junit.jupiter.api.Nested;

class PartitionedScanTest
{
    // Token Indexes

    @Nested
    class NodeLabelIndexScan extends NodeLabelIndexScanPartitionedScanTestSuite
    {
    }

    @Nested
    class RelationshipTypeIndexScan extends RelationshipTypeIndexScanPartitionedScanTestSuite
    {
    }

    // Property Indexes

    @Nested
    class NodePropertyIndexSeek
    {
        @Nested
        class BTree extends NodePropertyIndexSeekPartitionedScanTestSuite
        {
            BTree()
            {
                super( TestIndexType.BTREE );
            }
        }

        @Nested
        class Fusion extends NodePropertyIndexSeekPartitionedScanTestSuite
        {
            Fusion()
            {
                super( TestIndexType.FUSION );
            }
        }

        @Nested
        class Range extends NodePropertyIndexSeekPartitionedScanTestSuite
        {
            Range()
            {
                super( TestIndexType.RANGE );
            }
        }
    }

    @Nested
    class NodePropertyIndexScan
    {
        @Nested
        class BTree extends NodePropertyIndexScanPartitionedScanTestSuite
        {
            BTree()
            {
                super( TestIndexType.BTREE );
            }
        }

        @Nested
        class Range extends NodePropertyIndexScanPartitionedScanTestSuite
        {
            Range()
            {
                super( TestIndexType.RANGE );
            }
        }
    }

    @Nested
    class RelationshipPropertyIndexSeek
    {
        @Nested
        class BTree extends RelationshipPropertyIndexSeekPartitionedScanTestSuite
        {
            BTree()
            {
                super( TestIndexType.BTREE );
            }
        }

        @Nested
        class Fusion extends RelationshipPropertyIndexSeekPartitionedScanTestSuite
        {
            Fusion()
            {
                super( TestIndexType.FUSION );
            }
        }

        @Nested
        class Range extends RelationshipPropertyIndexSeekPartitionedScanTestSuite
        {
            Range()
            {
                super( TestIndexType.RANGE );
            }
        }
    }

    @Nested
    class RelationshipPropertyIndexScan
    {
        @Nested
        class BTree extends RelationshipPropertyIndexScanPartitionedScanTestSuite
        {
            BTree()
            {
                super( TestIndexType.BTREE );
            }
        }

        @Nested
        class Range extends RelationshipPropertyIndexScanPartitionedScanTestSuite
        {
            Range()
            {
                super( TestIndexType.RANGE );
            }
        }
    }
}
