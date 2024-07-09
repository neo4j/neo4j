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
package org.neo4j.internal.batchimport;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;
import static org.neo4j.kernel.impl.store.record.Record.NULL_REFERENCE;

import org.junit.jupiter.api.Test;
import org.neo4j.batchimport.api.Configuration;
import org.neo4j.internal.batchimport.cache.NodeRelationshipCache;
import org.neo4j.internal.batchimport.staging.SimpleStageControl;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;

class CalculateDenseNodesStepTest {
    @Test
    void shouldNotProcessLoopsTwice() throws Exception {
        // GIVEN
        NodeRelationshipCache cache = mock(NodeRelationshipCache.class);
        try (CalculateDenseNodesStep step = new CalculateDenseNodesStep(
                new SimpleStageControl(),
                Configuration.DEFAULT,
                cache,
                new CursorContextFactory(PageCacheTracer.NULL, EMPTY_CONTEXT_SUPPLIER))) {
            step.start(0);
            step.processors(4);

            // WHEN
            long id = 0;
            RelationshipRecord[] batch = batch(
                    relationship(id++, 1, 5),
                    relationship(id++, 3, 10),
                    relationship(id++, 2, 2), // <-- the loop
                    relationship(id, 4, 1));
            step.receive(0, batch);
            step.endOfUpstream();
            step.awaitCompleted();

            // THEN
            verify(cache, times(2)).incrementCount(eq(1L));
            verify(cache).incrementCount(eq(2L));
            verify(cache).incrementCount(eq(3L));
            verify(cache).incrementCount(eq(4L));
            verify(cache).incrementCount(eq(5L));
            verify(cache).incrementCount(eq(10L));
        }
    }

    private static RelationshipRecord[] batch(RelationshipRecord... relationships) {
        return relationships;
    }

    private static RelationshipRecord relationship(long id, long startNodeId, long endNodeId) {
        return new RelationshipRecord(id)
                .initialize(
                        true,
                        Record.NO_NEXT_PROPERTY.longValue(),
                        startNodeId,
                        endNodeId,
                        0,
                        NULL_REFERENCE.longValue(),
                        NULL_REFERENCE.longValue(),
                        NULL_REFERENCE.longValue(),
                        NULL_REFERENCE.longValue(),
                        false,
                        false);
    }
}
