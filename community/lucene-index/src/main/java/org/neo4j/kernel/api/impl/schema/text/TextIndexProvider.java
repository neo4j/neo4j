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
package org.neo4j.kernel.api.impl.schema.text;

import static org.neo4j.internal.schema.IndexCapability.NO_CAPABILITY;

import java.io.IOException;
import java.nio.file.OpenOption;
import org.eclipse.collections.api.set.ImmutableSet;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker;
import org.neo4j.internal.schema.AllIndexProviderDescriptors;
import org.neo4j.internal.schema.IndexCapability;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.StorageEngineIndexingBehaviour;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.memory.ByteBufferFactory;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.api.impl.index.DatabaseIndex;
import org.neo4j.kernel.api.impl.index.IndexWriterConfigBuilder;
import org.neo4j.kernel.api.impl.index.IndexWriterConfigMode;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.kernel.api.impl.schema.AbstractTextIndexProvider;
import org.neo4j.kernel.api.impl.schema.TextIndexCapability;
import org.neo4j.kernel.api.impl.schema.populator.TextIndexPopulator;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.ValueIndexReader;
import org.neo4j.kernel.impl.api.index.IndexSamplingConfig;
import org.neo4j.logging.LogProvider;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.monitoring.Monitors;
import org.neo4j.values.ElementIdMapper;

public class TextIndexProvider extends AbstractTextIndexProvider {
    public static final IndexCapability CAPABILITY = TextIndexCapability.text();

    private final FileSystemAbstraction fileSystem;

    public TextIndexProvider(
            FileSystemAbstraction fileSystem,
            DirectoryFactory directoryFactory,
            IndexDirectoryStructure.Factory directoryStructureFactory,
            Monitors monitors,
            Config config,
            DatabaseReadOnlyChecker readOnlyChecker,
            LogProvider logProvider) {
        super(
                KernelVersion.VERSION_RANGE_POINT_TEXT_INDEXES_ARE_INTRODUCED,
                IndexType.TEXT,
                AllIndexProviderDescriptors.TEXT_V1_DESCRIPTOR,
                fileSystem,
                directoryFactory,
                directoryStructureFactory,
                monitors,
                config,
                readOnlyChecker,
                logProvider);
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
            ElementIdMapper elementIdMapper,
            ImmutableSet<OpenOption> openOptions,
            StorageEngineIndexingBehaviour indexingBehaviour,
            IndexPopulator.Configuration configuration) {
        IndexWriterConfigBuilder writerConfigBuilder = new IndexWriterConfigBuilder(
                        IndexWriterConfigMode.TEXT_POPULATION, config)
                .withLogProvider(logProvider);
        DatabaseIndex<ValueIndexReader> index = TextIndexBuilder.create(
                        descriptor, readOnlyChecker, config, logProvider)
                .withFileSystem(fileSystem)
                .withSamplingConfig(samplingConfig)
                .withIndexStorage(getIndexStorage(descriptor.getId()))
                .withWriterConfig(writerConfigBuilder::build)
                .build();

        if (index.isReadOnly()) {
            throw new UnsupportedOperationException("Can't create populator for read only index");
        }
        return new TextIndexPopulator(index, UPDATE_IGNORE_STRATEGY);
    }

    @Override
    public IndexAccessor getOnlineAccessor(
            IndexDescriptor descriptor,
            IndexSamplingConfig samplingConfig,
            TokenNameLookup tokenNameLookup,
            ElementIdMapper elementIdMapper,
            ImmutableSet<OpenOption> openOptions,
            boolean readOnly,
            StorageEngineIndexingBehaviour indexingBehaviour)
            throws IOException {
        TextIndexBuilder builder = builder(descriptor, samplingConfig);
        if (readOnly) {
            builder = builder.permanentlyReadOnly();
        }
        DatabaseIndex<ValueIndexReader> index = builder.build();
        index.open();
        return new TextIndexAccessor(index, descriptor, tokenNameLookup, elementIdMapper, UPDATE_IGNORE_STRATEGY);
    }

    private TextIndexBuilder builder(IndexDescriptor descriptor, IndexSamplingConfig samplingConfig) {
        return TextIndexBuilder.create(descriptor, readOnlyChecker, config, logProvider)
                .withSamplingConfig(samplingConfig)
                .withIndexStorage(getIndexStorage(descriptor.getId()));
    }
}
