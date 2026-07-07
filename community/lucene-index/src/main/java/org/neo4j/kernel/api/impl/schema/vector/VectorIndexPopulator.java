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
package org.neo4j.kernel.api.impl.schema.vector;

import java.io.IOException;
import java.io.UncheckedIOException;
import org.neo4j.configuration.Config;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.impl.index.DatabaseIndex;
import org.neo4j.kernel.api.impl.index.IndexWriterConfigMode;
import org.neo4j.kernel.api.impl.index.lucene.LuceneDocument;
import org.neo4j.kernel.api.impl.index.lucene.LuceneIndexWriter;
import org.neo4j.kernel.api.impl.index.lucene.LuceneSettings;
import org.neo4j.kernel.api.impl.index.lucene.LuceneSettings.PostPopulationCompaction;
import org.neo4j.kernel.api.impl.index.partition.AbstractIndexPartition;
import org.neo4j.kernel.api.impl.schema.populator.LuceneIndexPopulator;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.impl.index.schema.IndexUpdateIgnoreStrategy;
import org.neo4j.storageengine.api.ValueIndexEntryUpdate;

class VectorIndexPopulator extends LuceneIndexPopulator<DatabaseIndex<VectorIndexReader>> {
    private final VectorDocumentStructure documentStructure;
    private final Neo4jVectorSimilarityFunction similarityFunction;
    private final Config config;

    VectorIndexPopulator(
            DatabaseIndex<VectorIndexReader> luceneIndex,
            IndexUpdateIgnoreStrategy ignoreStrategy,
            VectorDocumentStructure documentStructure,
            Neo4jVectorSimilarityFunction similarityFunction,
            Config config) {
        super(luceneIndex, ignoreStrategy);
        this.documentStructure = documentStructure;
        this.similarityFunction = similarityFunction;
        this.config = config;
    }

    @Override
    public IndexUpdater newPopulatingUpdater(CursorContext cursorContext) {
        return new VectorIndexPopulatingUpdater(writer, ignoreStrategy, documentStructure, similarityFunction);
    }

    /**
     * Compact the freshly populated segments before the index is marked online. Running the merge here, rather
     * than from {@link VectorIndexProvider#getOnlineAccessor}, means it executes on the still-open populating
     * writer which is configured with the parallel intra-merge codec — the online accessor's codec forces the
     * merge to be single-threaded. {@code super.close} subsequently marks the index online and commits the
     * merged segments.
     */
    @Override
    public void close(boolean populationCompletedSuccessfully, CursorContext cursorContext) {
        if (populationCompletedSuccessfully) {
            try {
                compactSegments();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        super.close(populationCompletedSuccessfully, cursorContext);
    }

    @Override
    protected LuceneDocument updateAsDocument(ValueIndexEntryUpdate update) {
        return documentsFactory.createVectorDocument(
                documentStructure, update.getEntityId(), similarityFunction, update.values());
    }

    /**
     * Perform post-population compaction of the index according to
     * {@link LuceneSettings#vector_post_population_compaction}.
     * <ul>
     *   <li>{@link PostPopulationCompaction#NONE} — skip merging entirely.</li>
     *   <li>{@link PostPopulationCompaction#AUTO} — invoke Lucene's natural merge policy via
     *       {@code maybeMerge()}. The configured merge policy decides which segments (if any) to merge.</li>
     *   <li>{@link PostPopulationCompaction#PARTIAL} — force-merge down to
     *       {@link LuceneSettings#vector_standard_merge_factor} segments per partition.</li>
     *   <li>{@link PostPopulationCompaction#FULL} — force-merge each partition to a single segment.</li>
     * </ul>
     */
    private void compactSegments() throws IOException {
        PostPopulationCompaction mode = config.get(LuceneSettings.vector_post_population_compaction);
        if (mode == PostPopulationCompaction.NONE) {
            return;
        }
        // The populating writer is configured with the population merge tuning (e.g. a large mergeFactor) which
        // suppresses merging during ingest. Swap in the standard tuning so AUTO's maybeMerge() actually
        // consolidates rather than no-opping. Harmless for PARTIAL/FULL, which forceMerge past the policy anyway.
        IndexWriterConfigMode standard = IndexWriterConfigMode.VECTOR;
        int forceMergeTarget = mode == PostPopulationCompaction.PARTIAL ? standard.getMergeFactor(config) : 1;

        IOException exception = null;
        for (AbstractIndexPartition partition : luceneIndex.getPartitions()) {
            try {
                LuceneIndexWriter writer = partition.getIndexWriter();
                writer.updateMergePolicy(
                        standard.getMergeFactor(config),
                        standard.segmentsPerTier(config),
                        standard.maxMergeAtOnce(config));
                switch (mode) {
                    case NONE -> {
                        // handled above
                    }
                    case AUTO -> writer.maybeMerge();
                    case PARTIAL, FULL -> writer.forceMerge(forceMergeTarget);
                }
            } catch (IOException e) {
                if (exception != null) {
                    exception.addSuppressed(e);
                } else {
                    exception = e;
                }
            }
        }
        if (exception != null) {
            throw exception;
        }
    }
}
