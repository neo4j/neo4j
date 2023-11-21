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

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.io.IOUtils;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.index.IndexSample;
import org.neo4j.kernel.api.index.IndexSampler;

/**
 * Index sampler implementation that provide total sampling result of multiple provided samples, by aggregating their
 * internal independent samples.
 */
public class AggregatingIndexSampler implements IndexSampler {
    private final List<IndexSampler> indexSamplers;

    public AggregatingIndexSampler(List<IndexSampler> indexSamplers) {
        this.indexSamplers = indexSamplers;
    }

    @Override
    public IndexSample sampleIndex(CursorContext cursorContext, AtomicBoolean stopped) {
        return indexSamplers.parallelStream()
                .map(sampler -> sampleIndex(sampler, cursorContext, stopped))
                .reduce(AggregatingIndexSampler::combine)
                .get();
    }

    private static IndexSample sampleIndex(IndexSampler sampler, CursorContext cursorContext, AtomicBoolean stopped) {
        try {
            return sampler.sampleIndex(cursorContext, stopped);
        } catch (IndexNotFoundKernelException e) {
            throw new RuntimeException(e);
        }
    }

    public static IndexSample combine(IndexSample sample1, IndexSample sample2) {
        long indexSize = Math.addExact(sample1.indexSize(), sample2.indexSize());
        long uniqueValues = Math.addExact(sample1.uniqueValues(), sample2.uniqueValues());
        long sampleSize = Math.addExact(sample1.sampleSize(), sample2.sampleSize());
        return new IndexSample(indexSize, uniqueValues, sampleSize);
    }

    @Override
    public void close() {
        IOUtils.closeAllSilently(indexSamplers);
    }
}
