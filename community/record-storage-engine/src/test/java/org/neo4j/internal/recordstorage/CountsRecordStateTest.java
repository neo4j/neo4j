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
package org.neo4j.internal.recordstorage;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import org.neo4j.test.LatestVersions;

class CountsRecordStateTest {
    @Test
    void trackCounts() {
        // given
        CountsRecordState counts = new CountsRecordState(
                RecordStorageCommandReaderFactory.INSTANCE.get(LatestVersions.LATEST_KERNEL_VERSION));
        counts.incrementNodeCount(17, 5);
        counts.incrementNodeCount(12, 9);
        counts.incrementRelationshipCount(1, 2, 3, 19);
        counts.incrementRelationshipCount(1, 4, 3, 25);

        assertEquals(0, counts.nodeCount(1));
        assertEquals(5, counts.nodeCount(17));
        assertEquals(9, counts.nodeCount(12));
        assertEquals(19, counts.relationshipCount(1, 2, 3));
        assertEquals(25, counts.relationshipCount(1, 4, 3));

        counts.incrementNodeCount(17, 0);
        counts.incrementNodeCount(12, -2);
        counts.incrementRelationshipCount(1, 2, 3, 1);
        counts.incrementRelationshipCount(1, 4, 3, -25);

        assertEquals(5, counts.nodeCount(17));
        assertEquals(7, counts.nodeCount(12));
        assertEquals(20, counts.relationshipCount(1, 2, 3));
        assertEquals(0, counts.relationshipCount(1, 4, 3));
    }
}
