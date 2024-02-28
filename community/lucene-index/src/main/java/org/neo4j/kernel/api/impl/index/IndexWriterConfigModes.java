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
package org.neo4j.kernel.api.impl.index;

import static org.neo4j.kernel.api.impl.index.LuceneSettings.lucene_merge_factor;
import static org.neo4j.kernel.api.impl.index.LuceneSettings.lucene_population_max_buffered_docs;
import static org.neo4j.kernel.api.impl.index.LuceneSettings.lucene_population_ram_buffer_size;
import static org.neo4j.kernel.api.impl.index.LuceneSettings.lucene_population_serial_merge_scheduler;
import static org.neo4j.kernel.api.impl.index.LuceneSettings.lucene_standard_ram_buffer_size;
import static org.neo4j.kernel.api.impl.index.LuceneSettings.lucene_writer_max_buffered_docs;
import static org.neo4j.kernel.api.impl.index.LuceneSettings.vector_population_merge_factor;
import static org.neo4j.kernel.api.impl.index.LuceneSettings.vector_population_ram_buffer_size;
import static org.neo4j.kernel.api.impl.index.LuceneSettings.vector_standard_merge_factor;

import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LogMergePolicy;
import org.neo4j.configuration.Config;

public class IndexWriterConfigModes {
    public static final class TextModes extends TextBasedModes {}

    public static final class FulltextModes extends TextBasedModes {
        public static final Mode TRANSACTION_STATE = new TransactionState();

        private static class TransactionState extends TextBasedModes.Standard {
            private TransactionState() {}

            @Override
            public IndexWriterConfig visitWithConfig(IndexWriterConfig writerConfig, Config config) {
                // Index transaction state is never directly persisted, so never commit it on close.
                return super.visitWithConfig(writerConfig, config).setCommitOnClose(false);
            }
        }
    }

    public static final class VectorModes {
        public static final Mode STANDARD = new Standard();
        public static final Mode POPULATION = new Population();

        private static class Standard extends DefaultModes.Standard {
            private Standard() {}

            @Override
            public LogMergePolicy visitWithConfig(LogMergePolicy mergePolicy, Config config) {
                mergePolicy.setMergeFactor(config.get(vector_standard_merge_factor));
                return mergePolicy;
            }
        }

        private static class Population extends DefaultModes.Population {
            private Population() {}

            @Override
            public LogMergePolicy visitWithConfig(LogMergePolicy mergePolicy, Config config) {
                mergePolicy.setMergeFactor(config.get(vector_population_merge_factor));
                return mergePolicy;
            }

            @Override
            public IndexWriterConfig visitWithConfig(IndexWriterConfig writerConfig, Config config) {
                super.visitWithConfig(writerConfig, config)
                        .setRAMBufferSizeMB(config.get(vector_population_ram_buffer_size));
                return writerConfig;
            }
        }
    }

    private abstract static class TextBasedModes {
        public static final Mode STANDARD = new Standard();
        public static final Mode POPULATION = new Population();

        private static class Standard extends DefaultModes.Standard {
            private Standard() {}

            @Override
            public LogMergePolicy visitWithConfig(LogMergePolicy mergePolicy, Config config) {
                mergePolicy.setMergeFactor(config.get(lucene_merge_factor));
                return mergePolicy;
            }
        }

        private static class Population extends DefaultModes.Population {
            private Population() {}

            @Override
            public LogMergePolicy visitWithConfig(LogMergePolicy mergePolicy, Config config) {
                mergePolicy.setMergeFactor(config.get(lucene_merge_factor));
                return mergePolicy;
            }

            @Override
            public IndexWriterConfig visitWithConfig(IndexWriterConfig writerConfig, Config config) {
                super.visitWithConfig(writerConfig, config)
                        .setRAMBufferSizeMB(config.get(lucene_population_ram_buffer_size));
                return writerConfig;
            }
        }
    }

    static class DefaultModes {
        abstract static class Standard implements Mode {
            @Override
            public IndexWriterConfig visitWithConfig(IndexWriterConfig writerConfig, Config config) {
                return writerConfig
                        .setMaxBufferedDocs(config.get(lucene_writer_max_buffered_docs))
                        .setRAMBufferSizeMB(config.get(lucene_standard_ram_buffer_size));
            }
        }

        abstract static class Population implements Mode {
            @Override
            public IndexWriterConfig visitWithConfig(IndexWriterConfig writerConfig, Config config) {
                writerConfig.setMaxBufferedDocs(config.get(lucene_population_max_buffered_docs));

                if (config.get(lucene_population_serial_merge_scheduler)) {
                    // With this setting 'true' we respect the GraphDatabaseInternalSettings.index_population_workers
                    // setting and don't use separate lucene threads for merging during population.
                    // Population is a background task, and it is probably more important to limit CPU usage than be
                    // as fast as possible here.
                    writerConfig.setMergeScheduler(new OnThreadConcurrentMergeScheduler());
                }
                return writerConfig;
            }
        }
    }

    public interface Mode {
        LogMergePolicy visitWithConfig(LogMergePolicy mergePolicy, Config config);

        IndexWriterConfig visitWithConfig(IndexWriterConfig writerConfig, Config config);
    }
}
