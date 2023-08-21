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
package org.neo4j.kernel.impl.index.schema;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.test.Race.throwing;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.test.Race;

class WorkSyncedIndexPopulatorTest {

    @Test
    void callsToDelegatingPopulatorAddShouldNotBeConcurrent() throws Throwable {
        var populator = new WorkSyncedIndexPopulator(new NotThreadSafePopulator());

        var race = new Race();
        race.addContestants(10, throwing(() -> {
            populator.add(
                    Collections.<IndexEntryUpdate<?>>singleton(Mockito.mock(IndexEntryUpdate.class)),
                    CursorContext.NULL_CONTEXT);
        }));
        race.go();
    }

    static class NotThreadSafePopulator extends IndexPopulator.Adapter {
        private final AtomicBoolean flag = new AtomicBoolean();

        @Override
        public void add(Collection<? extends IndexEntryUpdate<?>> updates, CursorContext cursorContext) {
            assertTrue(flag.compareAndSet(false, true), "Only one instance can flip flag at a time");
            assertTrue(flag.compareAndSet(true, false), "Only one instance can flip flag back at a time");
        }
    }
}
