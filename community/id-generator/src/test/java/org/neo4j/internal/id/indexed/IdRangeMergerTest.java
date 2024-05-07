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
package org.neo4j.internal.id.indexed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.neo4j.internal.id.indexed.IndexedIdGenerator.NO_MONITOR;

import org.junit.jupiter.api.Test;

class IdRangeMergerTest {
    private static final IdRangeMerger DEFAULT = new IdRangeMerger(false, NO_MONITOR, null);
    private static final IdRangeMerger RECOVERY = new IdRangeMerger(true, NO_MONITOR, null);
    private static final IdRangeKey K = new IdRangeKey(1);

    @Test
    void normalizeInNormalMode() {
        final var v1 = spy(new IdRange(1));
        final var v2 = spy(new IdRange(1));
        v1.setGeneration(1);
        v2.setGeneration(1);

        DEFAULT.merge(K, K, v1, v2);
        verify(v1, never()).normalize();

        v2.setGeneration(2);
        DEFAULT.merge(K, K, v1, v2);
        verify(v1).normalize();
        assertEquals(2, v1.getGeneration());
    }

    @Test
    void doNotNormalizeInRecoveryMode() {
        final var v1 = spy(new IdRange(1));
        final var v2 = spy(new IdRange(1));
        v1.setGeneration(1);
        v2.setGeneration(1);

        RECOVERY.merge(K, K, v1, v2);

        v2.setGeneration(2);
        RECOVERY.merge(K, K, v1, v2);

        verify(v1, never()).normalize();
    }
}
