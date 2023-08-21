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
package org.neo4j.storageengine.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;
import org.neo4j.internal.id.IdGenerator;
import org.neo4j.io.pagecache.context.CursorContext;

class IdGeneratorUpdatesWorkSyncTest {
    @Test
    void shouldGatherChangedIds() throws ExecutionException {
        // given
        IdGeneratorUpdatesWorkSync workSync = new IdGeneratorUpdatesWorkSync();
        IdGenerator idGenerator = mock(IdGenerator.class);
        RecordingMarker marker = new RecordingMarker();
        when(idGenerator.transactionalMarker(any())).thenReturn(marker);
        workSync.add(idGenerator);

        // when
        IdGeneratorUpdatesWorkSync.Batch batch = workSync.newBatch(CursorContext.NULL_CONTEXT);
        batch.markIdAsUsed(idGenerator, 10, 1, CursorContext.NULL_CONTEXT);
        batch.markIdAsUnused(idGenerator, 11, 1, CursorContext.NULL_CONTEXT);
        batch.markIdAsUsed(idGenerator, 270, 4, CursorContext.NULL_CONTEXT);
        batch.markIdAsUnused(idGenerator, 513, 7, CursorContext.NULL_CONTEXT);
        batch.apply();

        // then
        assertThat(marker.used(10, 1)).isTrue();
        assertThat(marker.deleted(11, 1)).isTrue();
        assertThat(marker.used(270, 4)).isTrue();
        assertThat(marker.deleted(513, 7)).isTrue();
    }

    private static class RecordingMarker implements IdGenerator.TransactionalMarker {
        private final Set<Pair<Long, Integer>> used = new HashSet<>();
        private final Set<Pair<Long, Integer>> deleted = new HashSet<>();

        @Override
        public void markUsed(long id, int numberOfIds) {
            used.add(Pair.of(id, numberOfIds));
        }

        @Override
        public void markDeleted(long id, int numberOfIds) {
            deleted.add(Pair.of(id, numberOfIds));
        }

        @Override
        public void markDeletedAndFree(long id, int numberOfIds) {}

        @Override
        public void markUnallocated(long id, int numberOfIds) {}

        @Override
        public void close() {}

        boolean used(long id, int numberOfIds) {
            return used.contains(Pair.of(id, numberOfIds));
        }

        boolean deleted(long id, int numberOfIds) {
            return deleted.contains(Pair.of(id, numberOfIds));
        }
    }
}
