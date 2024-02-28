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
package org.neo4j.kernel.api.impl.schema;

import java.util.function.Supplier;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker;
import org.neo4j.function.Factory;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.api.impl.index.DatabaseIndex;
import org.neo4j.kernel.api.impl.index.IndexWriterConfigBuilder;
import org.neo4j.kernel.api.impl.index.IndexWriterConfigModes.TextModes;
import org.neo4j.kernel.api.impl.index.WritableDatabaseIndex;
import org.neo4j.kernel.api.impl.index.builder.AbstractLuceneIndexBuilder;
import org.neo4j.kernel.api.impl.index.partition.WritableIndexPartitionFactory;
import org.neo4j.kernel.api.impl.index.storage.PartitionedIndexStorage;
import org.neo4j.kernel.api.index.ValueIndexReader;
import org.neo4j.kernel.impl.api.index.IndexSamplingConfig;

/**
 * Helper builder class to simplify construction and instantiation of lucene text indexes.
 * Most of the values already have most useful default value, that still can be overridden by corresponding
 * builder methods.
 *
 * @see TextIndex
 * @see AbstractLuceneIndexBuilder
 */
public class TextIndexBuilder extends AbstractLuceneIndexBuilder<TextIndexBuilder> {
    private final IndexDescriptor descriptor;
    private final Config config;
    private IndexSamplingConfig samplingConfig;
    private Supplier<IndexWriterConfig> writerConfigFactory;

    private TextIndexBuilder(IndexDescriptor descriptor, DatabaseReadOnlyChecker readOnlyChecker, Config config) {
        super(readOnlyChecker);
        this.descriptor = descriptor;
        this.config = config;
        this.samplingConfig = new IndexSamplingConfig(config);

        final var writerConfigBuilder = new IndexWriterConfigBuilder(TextModes.STANDARD, config);
        this.writerConfigFactory = writerConfigBuilder::build;
    }

    /**
     * Create new text index builder.
     *
     * @return {@link TextIndexBuilder} that can be used to build simple Text index built on Lucene
     * @param descriptor The descriptor for this index
     */
    public static TextIndexBuilder create(
            IndexDescriptor descriptor, DatabaseReadOnlyChecker readOnlyChecker, Config config) {
        return new TextIndexBuilder(descriptor, readOnlyChecker, config);
    }

    /**
     * Specify text index sampling config
     *
     * @param samplingConfig sampling config
     * @return index builder
     */
    public TextIndexBuilder withSamplingConfig(IndexSamplingConfig samplingConfig) {
        this.samplingConfig = samplingConfig;
        return this;
    }

    /**
     * Specify {@link Factory} of lucene {@link IndexWriterConfig} to create {@link IndexWriter}s.
     *
     * @param writerConfigFactory the supplier of writer configs
     * @return index builder
     */
    public TextIndexBuilder withWriterConfig(Supplier<IndexWriterConfig> writerConfigFactory) {
        this.writerConfigFactory = writerConfigFactory;
        return this;
    }

    /**
     * Build text index with specified configuration
     *
     * @return text index
     */
    public DatabaseIndex<ValueIndexReader> build() {
        PartitionedIndexStorage storage = storageBuilder.build();
        var index = new TextIndex(
                storage, descriptor, samplingConfig, new WritableIndexPartitionFactory(writerConfigFactory), config);
        return new WritableDatabaseIndex<>(index, readOnlyChecker, permanentlyReadOnly);
    }
}
