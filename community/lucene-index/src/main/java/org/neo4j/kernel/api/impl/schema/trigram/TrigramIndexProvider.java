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

import static org.neo4j.internal.schema.IndexCapability.NO_CAPABILITY;

import java.io.IOException;
import java.nio.file.OpenOption;
import org.eclipse.collections.api.set.ImmutableSet;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker;
import org.neo4j.internal.schema.IndexCapability;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.StorageEngineIndexingBehaviour;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.memory.ByteBufferFactory;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.api.impl.index.IndexWriterConfigBuilder;
import org.neo4j.kernel.api.impl.index.IndexWriterConfigModes.TextModes;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.kernel.api.impl.schema.AbstractTextIndexProvider;
import org.neo4j.kernel.api.impl.schema.TextIndexCapability;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.impl.api.LuceneIndexValueValidator;
import org.neo4j.kernel.impl.api.index.IndexSamplingConfig;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.monitoring.Monitors;

public class TrigramIndexProvider extends AbstractTextIndexProvider {
    public static final IndexProviderDescriptor DESCRIPTOR = new IndexProviderDescriptor("text", "2.0");
    public static final IndexCapability CAPABILITY = TextIndexCapability.trigram();

    private final FileSystemAbstraction fileSystem;

    public TrigramIndexProvider(
            FileSystemAbstraction fileSystem,
            DirectoryFactory directoryFactory,
            IndexDirectoryStructure.Factory directoryStructureFactory,
            Monitors monitors,
            Config config,
            DatabaseReadOnlyChecker readOnlyChecker) {
        super(
                KernelVersion.VERSION_TRIGRAM_INDEX_INTRODUCED,
                IndexType.TEXT,
                DESCRIPTOR,
                fileSystem,
                directoryFactory,
                directoryStructureFactory,
                monitors,
                config,
                readOnlyChecker);
        this.fileSystem = fileSystem;
    }

    @Override
    public IndexDescriptor completeConfiguration(
            IndexDescriptor index, StorageEngineIndexingBehaviour indexingBehaviour) {
        return index.getCapability().equals(NO_CAPABILITY) ? index.withIndexCapability(CAPABILITY) : index;
    }

    @Override
    public IndexPopulator getPopulator(
            IndexDescriptor descriptor,
            IndexSamplingConfig samplingConfig,
            ByteBufferFactory bufferFactory,
            MemoryTracker memoryTracker,
            TokenNameLookup tokenNameLookup,
            ImmutableSet<OpenOption> openOptions,
            StorageEngineIndexingBehaviour indexingBehaviour) {
        final var writerConfigBuilder = new IndexWriterConfigBuilder(TextModes.POPULATION, config);
        final var luceneIndex = TrigramIndexBuilder.create(descriptor, readOnlyChecker, config)
                .withFileSystem(fileSystem)
                .withIndexStorage(getIndexStorage(descriptor.getId()))
                .withWriterConfig(writerConfigBuilder::build)
                .build();

        if (luceneIndex.isReadOnly()) {
            throw new UnsupportedOperationException("Can't create populator for read only index");
        }
        final var validator = valueValidator(descriptor, tokenNameLookup);
        return new TrigramIndexPopulator(luceneIndex, UPDATE_IGNORE_STRATEGY, validator);
    }

    @Override
    public IndexAccessor getOnlineAccessor(
            IndexDescriptor descriptor,
            IndexSamplingConfig samplingConfig,
            TokenNameLookup tokenNameLookup,
            ImmutableSet<OpenOption> openOptions,
            boolean readOnly,
            StorageEngineIndexingBehaviour indexingBehaviour)
            throws IOException {
        var builder = builder(descriptor);
        if (readOnly) {
            builder = builder.permanentlyReadOnly();
        }
        final var luceneIndex = builder.build();
        luceneIndex.open();
        final var validator = valueValidator(descriptor, tokenNameLookup);
        return new TrigramIndexAccessor(luceneIndex, descriptor, UPDATE_IGNORE_STRATEGY, validator);
    }

    private TrigramIndexBuilder builder(IndexDescriptor descriptor) {
        return TrigramIndexBuilder.create(descriptor, readOnlyChecker, config)
                .withIndexStorage(getIndexStorage(descriptor.getId()));
    }

    private LuceneIndexValueValidator valueValidator(IndexDescriptor descriptor, TokenNameLookup tokenNameLookup) {
        return new LuceneIndexValueValidator(descriptor, tokenNameLookup);
    }
}
