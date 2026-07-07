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
import java.nio.file.OpenOption;
import java.util.OptionalInt;
import java.util.concurrent.ExecutorService;
import org.eclipse.collections.api.set.ImmutableSet;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker;
import org.neo4j.graphdb.WriteOperationsNotAllowedException;
import org.neo4j.internal.schema.IndexCapability;
import org.neo4j.internal.schema.IndexConfig;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.SettingsAccessor.IndexConfigAccessor;
import org.neo4j.internal.schema.StorageEngineIndexingBehaviour;
import org.neo4j.internal.schema.TypedIndexSettingsValidator;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.memory.ByteBufferFactory;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.KernelVersionProvider;
import org.neo4j.kernel.api.impl.index.DatabaseIndex;
import org.neo4j.kernel.api.impl.index.IndexWriterConfigBuilder;
import org.neo4j.kernel.api.impl.index.IndexWriterConfigMode;
import org.neo4j.kernel.api.impl.index.JobSchedulerExecutorService;
import org.neo4j.kernel.api.impl.index.lucene.LuceneContext;
import org.neo4j.kernel.api.impl.index.lucene.LuceneSettings;
import org.neo4j.kernel.api.impl.index.lucene.codec.LuceneCodec;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.kernel.api.impl.schema.AbstractLuceneIndexProvider;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexDirectoryStructure.Factory;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.vector.VectorSimilarityFunction;
import org.neo4j.kernel.impl.api.index.IndexSamplingConfig;
import org.neo4j.kernel.impl.index.schema.IndexUpdateIgnoreStrategy;
import org.neo4j.logging.LogProvider;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.util.VisibleForTesting;
import org.neo4j.values.ElementIdMapper;
import org.neo4j.values.VectorCandidate;
import org.neo4j.values.storable.Value;

public class VectorIndexProvider extends AbstractLuceneIndexProvider {
    private final VectorIndexVersion version;
    private final LuceneContext luceneContext;
    private final KernelVersionProvider kernelVersionProvider;
    private final VectorDocumentStructure documentStructure;
    private final FileSystemAbstraction fileSystem;
    private final JobScheduler scheduler;

    public VectorIndexProvider(
            VectorIndexVersion version,
            LuceneContext luceneContext,
            FileSystemAbstraction fileSystem,
            DirectoryFactory directoryFactory,
            Factory directoryStructureFactory,
            Monitors monitors,
            Config config,
            KernelVersionProvider kernelVersionProvider,
            DatabaseReadOnlyChecker readOnlyChecker,
            JobScheduler scheduler,
            LogProvider logProvider) {
        super(
                version.minimumRequiredKernelVersion(),
                IndexType.VECTOR,
                version.descriptor(),
                fileSystem,
                directoryFactory,
                directoryStructureFactory,
                monitors,
                config,
                readOnlyChecker,
                logProvider);
        this.version = version;
        this.luceneContext = luceneContext;
        this.kernelVersionProvider = kernelVersionProvider;
        this.documentStructure = VectorDocumentStructures.documentStructureFor(version);
        this.fileSystem = fileSystem;
        this.scheduler = scheduler;
    }

    @Override
    public IndexPrototype validatePrototype(IndexPrototype prototype) {
        prototype = super.validatePrototype(prototype);
        // construction handles validation
        VectorIndexConfig vectorIndexConfig =
                settingsValidator().validateToTypedConfig(new IndexConfigAccessor(prototype.getIndexConfig()));
        // replaces provided config with validated config with set defaults
        return prototype.withIndexConfig(vectorIndexConfig.config());
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
        VectorIndexConfig vectorIndexConfig = settingsValidator()
                .interpretAuthoritativeToTypedConfig(new IndexConfigAccessor(descriptor.getIndexConfig()));
        OptionalInt dimensions = vectorIndexConfig.dimensions();

        LuceneCodec codec = codecForVectorIndex(vectorIndexConfig, true);
        IndexWriterConfigBuilder writerConfigBuilder = new IndexWriterConfigBuilder(
                        IndexWriterConfigMode.VECTOR_POPULATION, config)
                .withLogProvider(logProvider)
                .withCodec(codec);
        DatabaseIndex<VectorIndexReader> luceneIndex = VectorIndexBuilder.create(
                        descriptor, vectorIndexConfig, documentStructure, codec, readOnlyChecker, config, logProvider)
                .withFileSystem(fileSystem)
                .withIndexStorage(getIndexStorage(descriptor.getId()))
                .withWriterConfig(writerConfigBuilder::build)
                .build();

        if (luceneIndex.isReadOnly()) {
            throw WriteOperationsNotAllowedException.noWriteOperationAllowed();
        }

        IgnoreStrategy ignoreStrategy = new IgnoreStrategy(version, dimensions);
        Neo4jVectorSimilarityFunction similarityFunction = vectorSimilarityFunctionFrom(vectorIndexConfig);
        return new VectorIndexPopulator(luceneIndex, ignoreStrategy, documentStructure, similarityFunction, config);
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
        VectorIndexConfig vectorIndexConfig = settingsValidator()
                .interpretAuthoritativeToTypedConfig(new IndexConfigAccessor(descriptor.getIndexConfig()));
        LuceneCodec codec = codecForVectorIndex(vectorIndexConfig, false);
        VectorIndexBuilder builder = VectorIndexBuilder.create(
                        descriptor, vectorIndexConfig, documentStructure, codec, readOnlyChecker, config, logProvider)
                .withIndexStorage(getIndexStorage(descriptor.getId()));
        if (readOnly) {
            builder = builder.permanentlyReadOnly();
        }
        DatabaseIndex<VectorIndexReader> luceneIndex = builder.build();
        luceneIndex.open();

        IgnoreStrategy ignoreStrategy = new IgnoreStrategy(version, vectorIndexConfig.dimensions());
        Neo4jVectorSimilarityFunction similarityFunction = vectorSimilarityFunctionFrom(vectorIndexConfig);
        return new VectorIndexAccessor(luceneIndex, descriptor, ignoreStrategy, documentStructure, similarityFunction);
    }

    @Override
    public IndexDescriptor completeConfiguration(
            IndexDescriptor index, StorageEngineIndexingBehaviour indexingBehaviour) {
        return index.getCapability().equals(IndexCapability.NO_CAPABILITY)
                ? index.withIndexCapability(capability(version, index.getIndexConfig(), settingsValidator()))
                : index;
    }

    @VisibleForTesting
    public static IndexCapability capability(
            VectorIndexVersion version, IndexConfig config, KernelVersion kernelVersion) {
        return capability(version, config, version.indexSettingValidator(kernelVersion));
    }

    private static IndexCapability capability(
            VectorIndexVersion version,
            IndexConfig config,
            TypedIndexSettingsValidator<VectorIndexConfig> indexSettingsValidator) {
        VectorIndexConfig vectorIndexConfig =
                indexSettingsValidator.interpretAuthoritativeToTypedConfig(new IndexConfigAccessor(config));
        return new VectorIndexCapability(
                new IgnoreStrategy(version, vectorIndexConfig.dimensions()), vectorIndexConfig.similarityFunction());
    }

    private TypedIndexSettingsValidator<VectorIndexConfig> settingsValidator() {
        return this.version.indexSettingValidator(this.kernelVersionProvider.kernelVersion());
    }

    record IgnoreStrategy(VectorIndexVersion version, OptionalInt dimensions) implements IndexUpdateIgnoreStrategy {
        @Override
        public boolean ignore(Value... values) {
            if (values.length < 1) {
                return true;
            }

            // Vector value
            Value value = values[0];
            return !version.acceptsValueInstanceType(value) || hasInvalidDimensions(VectorCandidate.maybeFrom(value));
        }

        private boolean hasInvalidDimensions(VectorCandidate candidate) {
            return candidate == null
                    || dimensions.isPresent() && candidate.dimensions() != dimensions.getAsInt()
                    || dimensions.isEmpty()
                            && (candidate.dimensions() < 1 || candidate.dimensions() > version.maxDimensions());
        }
    }

    private static Neo4jVectorSimilarityFunction vectorSimilarityFunctionFrom(VectorIndexConfig vectorIndexConfig) {
        VectorSimilarityFunction vectorSimilarityFunction = vectorIndexConfig.similarityFunction();
        if (vectorSimilarityFunction instanceof Neo4jVectorSimilarityFunction sf) {
            return sf;
        }
        throw new IllegalArgumentException(
                "'%s' vector similarity function is expected to be compatible with Lucene. Provided: %s"
                        .formatted(vectorSimilarityFunction.functionName(), vectorSimilarityFunction));
    }

    /**
     * Build a codec for the given vector index config, wiring an intra-merge executor when
     * {@link LuceneSettings#vector_intra_merge_workers} > 1. The executor is backed by
     * {@link Group#VECTOR_INDEX_MERGE} and shares lifecycle with the {@link JobScheduler}.
     */
    private LuceneCodec codecForVectorIndex(VectorIndexConfig vectorIndexConfig, boolean allowIntraParallelMerge) {
        int numMergeWorkers = allowIntraParallelMerge ? config.get(LuceneSettings.vector_intra_merge_workers) : 1;
        ExecutorService mergeExec = numMergeWorkers > 1
                ? JobSchedulerExecutorService.nonShutdownable(scheduler, Group.VECTOR_INDEX_MERGE)
                : null;
        return luceneContext.codecsFactory().codecFor(vectorIndexConfig, numMergeWorkers, mergeExec);
    }
}
