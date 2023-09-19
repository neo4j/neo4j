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
package org.neo4j.index.internal.gbptree;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import org.neo4j.io.pagecache.context.CursorContext;

class TripCountingRootCatchupTest {
    @Test
    void mustThrowOnConsecutiveCatchupsFromSamePage() {
        // Given
        TripCountingRootCatchup tripCountingRootCatchup = getTripCounter();

        assertThrows(TreeInconsistencyException.class, () -> {
            for (int i = 0; i < TripCountingRootCatchup.MAX_TRIP_COUNT; i++) {
                tripCountingRootCatchup.catchupFrom(10, CursorContext.NULL_CONTEXT);
            }
        });
    }

    @Test
    void mustNotThrowOnInterleavedCatchups() {
        // given
        TripCountingRootCatchup tripCountingRootCatchup = getTripCounter();

        // When
        for (int i = 0; i < TripCountingRootCatchup.MAX_TRIP_COUNT * 4; i++) {
            tripCountingRootCatchup.catchupFrom(i % 2, CursorContext.NULL_CONTEXT);
        }

        // then this should be fine
    }

    @Test
    void mustReturnRootUsingProvidedSupplier() {
        // given
        Root expectedRoot = new Root(1, 2);
        RootSupplier rootSupplier = context -> expectedRoot;
        TripCountingRootCatchup tripCountingRootCatchup = new TripCountingRootCatchup(rootSupplier);

        // when
        Root actualRoot = tripCountingRootCatchup.catchupFrom(10, CursorContext.NULL_CONTEXT);

        // then
        assertSame(expectedRoot, actualRoot);
    }

    private static TripCountingRootCatchup getTripCounter() {
        Root root = new Root(1, 2);
        return new TripCountingRootCatchup(c -> root);
    }
}
