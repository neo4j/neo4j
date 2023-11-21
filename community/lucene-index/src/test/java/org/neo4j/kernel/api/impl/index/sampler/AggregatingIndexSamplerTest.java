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
package org.neo4j.kernel.api.impl.index.sampler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.index.IndexSample;
import org.neo4j.kernel.api.index.IndexSampler;

class AggregatingIndexSamplerTest {
    @Test
    void samplePartitionedIndex() {
        List<IndexSampler> samplers = Arrays.asList(createSampler(1), createSampler(2));
        AggregatingIndexSampler partitionedSampler = new AggregatingIndexSampler(samplers);

        IndexSample sample = partitionedSampler.sampleIndex(NULL_CONTEXT, new AtomicBoolean());

        assertEquals(new IndexSample(3, 3, 6), sample);
    }

    private static IndexSampler createSampler(long value) {
        return new TestIndexSampler(value);
    }

    private static class TestIndexSampler implements IndexSampler {
        private final long value;

        TestIndexSampler(long value) {
            this.value = value;
        }

        @Override
        public IndexSample sampleIndex(CursorContext cursorContext, AtomicBoolean stopped) {
            return new IndexSample(value, value, value * 2);
        }
    }
}
