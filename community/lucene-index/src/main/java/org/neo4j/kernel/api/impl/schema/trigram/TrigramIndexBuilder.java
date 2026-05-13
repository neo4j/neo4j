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
package org.neo4j.kernel.api.impl.schema.trigram;

import java.util.function.Supplier;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker;
import org.neo4j.function.Factory;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.api.impl.index.DatabaseIndex;
import org.neo4j.kernel.api.impl.index.IndexWriterConfigBuilder;
import org.neo4j.kernel.api.impl.index.IndexWriterConfigMode;
import org.neo4j.kernel.api.impl.index.WritableDatabaseIndex;
import org.neo4j.kernel.api.impl.index.builder.AbstractLuceneIndexBuilder;
import org.neo4j.kernel.api.impl.index.lucene.LuceneIndexWriter;
import org.neo4j.kernel.api.impl.index.lucene.LuceneIndexWriterConfig;
import org.neo4j.kernel.api.impl.index.partition.WritableIndexPartitionFactory;
import org.neo4j.kernel.api.impl.index.storage.PartitionedIndexStorage;
import org.neo4j.kernel.api.index.ValueIndexReader;
import org.neo4j.logging.LogProvider;

public class TrigramIndexBuilder extends AbstractLuceneIndexBuilder<TrigramIndexBuilder> {
    private final IndexDescriptor descriptor;
    private final Config config;
    private Supplier<LuceneIndexWriterConfig> writerConfigFactory;

    private TrigramIndexBuilder(
            IndexDescriptor descriptor,
            DatabaseReadOnlyChecker readOnlyChecker,
            Config config,
            LogProvider logProvider) {
        super(readOnlyChecker, logProvider);
        this.descriptor = descriptor;
        this.config = config;

        IndexWriterConfigBuilder writerConfigBuilder =
                new IndexWriterConfigBuilder(IndexWriterConfigMode.TEXT, config).withLogProvider(logProvider);
        this.writerConfigFactory = writerConfigBuilder::build;
    }

    /**
     * Create new lucene schema index builder.
     *
     * @return {@link TrigramIndexBuilder} that can be used to build trigram based Text index built on Lucene
     * @param descriptor The descriptor for this index
     */
    public static TrigramIndexBuilder create(
            IndexDescriptor descriptor,
            DatabaseReadOnlyChecker readOnlyChecker,
            Config config,
            LogProvider logProvider) {
        return new TrigramIndexBuilder(descriptor, readOnlyChecker, config, logProvider);
    }

    /**
     * Specify {@link Factory} of lucene {@link LuceneIndexWriterConfig} to create {@link LuceneIndexWriter}s.
     *
     * @param writerConfigFactory the supplier of writer configs
     * @return index builder
     */
    TrigramIndexBuilder withWriterConfig(Supplier<LuceneIndexWriterConfig> writerConfigFactory) {
        this.writerConfigFactory = writerConfigFactory;
        return this;
    }

    /**
     * Build lucene schema index with specified configuration
     *
     * @return lucene schema index
     */
    public DatabaseIndex<ValueIndexReader> build() {
        PartitionedIndexStorage storage = storageBuilder.build();
        TrigramIndex index = new TrigramIndex(
                storage, descriptor, new WritableIndexPartitionFactory(writerConfigFactory), config, logProvider);
        return new WritableDatabaseIndex<>(index, readOnlyChecker, permanentlyReadOnly);
    }
}
