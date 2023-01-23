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
package org.neo4j.kernel.impl.transaction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;
import org.neo4j.kernel.impl.transaction.log.LogHeaderCache;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.storageengine.api.StoreId;

class LogHeaderCacheTest {
    @Test
    void shouldReturnNullWhenThereIsNoHeaderInTheCache() {
        // given
        final LogHeaderCache cache = new LogHeaderCache(2);

        // when
        final LogHeader logHeader = cache.getLogHeader(5);

        // then
        assertNull(logHeader);
    }

    @Test
    void shouldReturnTheHeaderIfInTheCache() {
        // given
        final LogHeaderCache cache = new LogHeaderCache(2);

        // when
        cache.putHeader(5, new LogHeader(1, 3, new StoreId(1, 2, "engine-1", "format-1", 3, 4)));
        final LogHeader logHeader = cache.getLogHeader(5);

        // then
        assertEquals(3, logHeader.getLastCommittedTxId());
    }

    @Test
    void shouldClearTheCache() {
        // given
        final LogHeaderCache cache = new LogHeaderCache(2);

        // when
        cache.putHeader(5, new LogHeader(1, 3, new StoreId(1, 2, "engine-1", "format-1", 3, 4)));
        cache.clear();
        final LogHeader logHeader = cache.getLogHeader(5);

        // then
        assertNull(logHeader);
    }
}
