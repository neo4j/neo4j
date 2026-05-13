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
import org.neo4j.kernel.api.impl.index.lucene.codec.LuceneCodec;
import org.neo4j.kernel.api.impl.index.partition.WritableIndexPartitionFactory;
import org.neo4j.kernel.api.impl.index.storage.PartitionedIndexStorage;
import org.neo4j.logging.LogProvider;

class VectorIndexBuilder extends AbstractLuceneIndexBuilder<VectorIndexBuilder> {
    private final IndexDescriptor descriptor;
    private final VectorIndexConfig vectorIndexConfig;
    private final VectorDocumentStructure documentStructure;
    private final Config config;
    private Supplier<LuceneIndexWriterConfig> writerConfigFactory;

    private VectorIndexBuilder(
            IndexDescriptor descriptor,
            VectorIndexConfig vectorIndexConfig,
            VectorDocumentStructure documentStructure,
            LuceneCodec codec,
            DatabaseReadOnlyChecker readOnlyChecker,
            Config config,
            LogProvider logProvider) {
        super(readOnlyChecker, logProvider);
        this.descriptor = descriptor;
        this.vectorIndexConfig = vectorIndexConfig;
        this.documentStructure = documentStructure;
        this.config = config;

        IndexWriterConfigBuilder writerConfigBuilder = new IndexWriterConfigBuilder(
                        IndexWriterConfigMode.VECTOR, config)
                .withLogProvider(logProvider)
                .withCodec(codec);
        this.writerConfigFactory = writerConfigBuilder::build;
    }

    /**
     * Create new lucene schema index builder.
     * @param descriptor The descriptor for this index
     * @param vectorIndexConfig The vector index config for the index
     * @param documentStructure The lucene document structure for this index
     * @return {@link VectorIndexBuilder} that can be used to build vector index built on Lucene
     */
    static VectorIndexBuilder create(
            IndexDescriptor descriptor,
            VectorIndexConfig vectorIndexConfig,
            VectorDocumentStructure documentStructure,
            LuceneCodec codec,
            DatabaseReadOnlyChecker readOnlyChecker,
            Config config,
            LogProvider logProvider) {
        return new VectorIndexBuilder(
                descriptor, vectorIndexConfig, documentStructure, codec, readOnlyChecker, config, logProvider);
    }

    /**
     * Specify {@link Factory} of lucene {@link LuceneIndexWriterConfig} to create {@link LuceneIndexWriter}s.
     *
     * @param writerConfigFactory the supplier of writer configs
     * @return index builder
     */
    VectorIndexBuilder withWriterConfig(Supplier<LuceneIndexWriterConfig> writerConfigFactory) {
        this.writerConfigFactory = writerConfigFactory;
        return this;
    }

    /**
     * Build lucene schema index with specified configuration
     *
     * @return lucene schema index
     */
    DatabaseIndex<VectorIndexReader> build() {
        PartitionedIndexStorage storage = storageBuilder.build();
        VectorIndex index = new VectorIndex(
                storage,
                new WritableIndexPartitionFactory(writerConfigFactory),
                documentStructure,
                descriptor,
                vectorIndexConfig,
                config,
                logProvider);
        return new WritableDatabaseIndex<>(index, readOnlyChecker, permanentlyReadOnly);
    }
}
