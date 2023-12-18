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

import static org.neo4j.internal.schema.IndexCapability.NO_CAPABILITY;
import static org.neo4j.kernel.api.impl.schema.LuceneIndexType.VECTOR;
import static org.neo4j.kernel.api.impl.schema.vector.VectorUtils.vectorDimensionsFrom;
import static org.neo4j.kernel.api.impl.schema.vector.VectorUtils.vectorSimilarityFunctionFrom;

import java.io.IOException;
import java.nio.file.OpenOption;
import org.eclipse.collections.api.set.ImmutableSet;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker;
import org.neo4j.graphdb.schema.IndexSetting;
import org.neo4j.internal.schema.IndexCapability;
import org.neo4j.internal.schema.IndexConfig;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.StorageEngineIndexingBehaviour;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.memory.ByteBufferFactory;
import org.neo4j.kernel.api.impl.index.DatabaseIndex;
import org.neo4j.kernel.api.impl.index.IndexWriterConfigs;
import org.neo4j.kernel.api.impl.index.LuceneSettings;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.kernel.api.impl.schema.AbstractLuceneIndexProvider;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.impl.api.index.IndexSamplingConfig;
import org.neo4j.kernel.impl.index.schema.IndexUpdateIgnoreStrategy;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.values.storable.FloatingPointArray;
import org.neo4j.values.storable.Value;

public class VectorIndexProvider extends AbstractLuceneIndexProvider {
    public static final IndexProviderDescriptor DESCRIPTOR = new IndexProviderDescriptor("vector", "1.0");

    private final FileSystemAbstraction fileSystem;
    private final JobScheduler scheduler;

    public VectorIndexProvider(
            FileSystemAbstraction fileSystem,
            DirectoryFactory directoryFactory,
            IndexDirectoryStructure.Factory directoryStructureFactory,
            Monitors monitors,
            Config config,
            DatabaseReadOnlyChecker readOnlyChecker,
            JobScheduler scheduler) {
        super(
                IndexType.VECTOR,
                DESCRIPTOR,
                fileSystem,
                directoryFactory,
                directoryStructureFactory,
                monitors,
                config,
                readOnlyChecker);
        this.fileSystem = fileSystem;
        this.scheduler = scheduler;
    }

    @Override
    public void validatePrototype(IndexPrototype prototype) {
        super.validatePrototype(prototype);
        final var config = prototype.getIndexConfig();

        final var dimensions = VectorUtils.vectorDimensionsFrom(config);
        if (dimensions > VectorUtils.MAX_DIMENSIONS) {
            throw new UnsupportedOperationException("'%s' set greater than %d is unsupported"
                    .formatted(IndexSetting.vector_Dimensions().getSettingName(), VectorUtils.MAX_DIMENSIONS));
        }
        VectorUtils.vectorSimilarityFunctionFrom(config);
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
        final var indexConfig = descriptor.getIndexConfig();
        var luceneIndex = VectorIndexBuilder.create(descriptor, readOnlyChecker, config)
                .withFileSystem(fileSystem)
                .withIndexStorage(getIndexStorage(descriptor.getId()))
                .withWriterConfig(() -> IndexWriterConfigs.population(VECTOR, config, indexConfig))
                .build();

        if (luceneIndex.isReadOnly()) {
            throw new UnsupportedOperationException("Can't create populator for read only index");
        }

        final var ignoreStrategy = new IgnoreStrategy(vectorDimensionsFrom(indexConfig));
        final var similarityFunction = vectorSimilarityFunctionFrom(indexConfig);
        return new VectorIndexPopulator(luceneIndex, ignoreStrategy, similarityFunction);
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
        var builder = VectorIndexBuilder.create(descriptor, readOnlyChecker, config)
                .withIndexStorage(getIndexStorage(descriptor.getId()));
        if (readOnly) {
            builder = builder.permanentlyReadOnly();
        }
        final var luceneIndex = builder.build();
        luceneIndex.open();
        forceMergeSegments(luceneIndex);

        final var indexConfig = descriptor.getIndexConfig();
        final var ignoreStrategy = new IgnoreStrategy(vectorDimensionsFrom(indexConfig));
        final var similarityFunction = vectorSimilarityFunctionFrom(indexConfig);
        return new VectorIndexAccessor(luceneIndex, descriptor, ignoreStrategy, similarityFunction);
    }

    @Override
    public IndexDescriptor completeConfiguration(
            IndexDescriptor index, StorageEngineIndexingBehaviour indexingBehaviour) {
        return index.getCapability().equals(NO_CAPABILITY)
                ? index.withIndexCapability(capability(index.getIndexConfig()))
                : index;
    }

    public static IndexCapability capability(IndexConfig config) {
        return new VectorIndexCapability(config);
    }

    private record IgnoreStrategy(int dimensions) implements IndexUpdateIgnoreStrategy {
        @Override
        public boolean ignore(Value[] values) {
            return !(values[0] instanceof final FloatingPointArray vector && vector.length() == dimensions);
        }
    }

    /**
     * {@link LuceneSettings#vector_population_merge_factor} should be larger than {@link LuceneSettings#vector_standard_merge_factor}
     * to enable faster population, but at the cost of more segment files.
     * This coerces the index to merge the segments to the {@link LuceneSettings#vector_standard_merge_factor}
     */
    private static void forceMergeSegments(DatabaseIndex<?> luceneIndex) throws IOException {
        IOException exception = null;
        for (final var partition : luceneIndex.getPartitions()) {
            try {
                partition.getIndexWriter().forceMerge(Integer.MAX_VALUE);
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
