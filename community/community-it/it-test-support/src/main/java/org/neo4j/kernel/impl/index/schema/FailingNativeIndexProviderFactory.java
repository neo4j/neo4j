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
package org.neo4j.kernel.impl.index.schema;

import static java.util.Arrays.copyOfRange;

import java.io.IOException;
import java.nio.file.OpenOption;
import java.util.Collection;
import java.util.EnumSet;
import org.eclipse.collections.api.set.ImmutableSet;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.StorageEngineIndexingBehaviour;
import org.neo4j.io.memory.ByteBufferFactory;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.impl.api.index.IndexSamplingConfig;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.kernel.impl.api.index.updater.DelegatingIndexUpdater;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.IndexEntryUpdate;

/**
 * Testing utility which takes a fully functional {@link RangeIndexProviderFactory} and turns it into a provider which
 * is guaranteed to fail for various reasons, e.g. failing index population with the goal of creating an index which is in a
 * {@link InternalIndexState#FAILED} state. To get to this state in high-level testing is surprisingly hard,
 * so this test utility helps a lot to accomplish this.
 *
 * To be sure to use this provider in your test please do something like:
 * - add as extension
 * - create the index with specified indexProvider (possible on kernelLevel)
 */
public class FailingNativeIndexProviderFactory extends BuiltInDelegatingIndexProviderFactory {
    public static final String INITIAL_STATE_FAILURE_MESSAGE = "Override initial state as failed";
    public static final String POPULATION_FAILURE_MESSAGE = "Fail on update during population";

    public static final IndexProviderDescriptor DESCRIPTOR = new IndexProviderDescriptor("failing-provider", "0.1");

    public enum FailureType {
        POPULATION,
        INITIAL_STATE,
        SKIP_ONLINE_UPDATES
    }

    private final EnumSet<FailureType> failureTypes;

    public FailingNativeIndexProviderFactory(FailureType... failureTypes) {
        this(new RangeIndexProviderFactory(), failureTypes);
    }

    public FailingNativeIndexProviderFactory(
            AbstractIndexProviderFactory<?> indexProviderFactory, FailureType... failureTypes) {
        super(indexProviderFactory, DESCRIPTOR);
        if (failureTypes.length == 0) {
            throw new IllegalArgumentException(
                    "At least one failure type, otherwise there's no point in this provider");
        }
        this.failureTypes = EnumSet.of(failureTypes[0], copyOfRange(failureTypes, 1, failureTypes.length));
    }

    @Override
    public IndexProvider newInstance(ExtensionContext context, Dependencies dependencies) {
        var actualProvider = super.newInstance(context, dependencies);
        return new IndexProvider.Delegating(actualProvider) {
            @Override
            public IndexPopulator getPopulator(
                    IndexDescriptor descriptor,
                    IndexSamplingConfig samplingConfig,
                    ByteBufferFactory bufferFactory,
                    MemoryTracker memoryTracker,
                    TokenNameLookup tokenNameLookup,
                    ImmutableSet<OpenOption> openOptions,
                    StorageEngineIndexingBehaviour indexingBehaviour) {
                IndexPopulator actualPopulator = actualProvider.getPopulator(
                        descriptor,
                        samplingConfig,
                        bufferFactory,
                        memoryTracker,
                        tokenNameLookup,
                        openOptions,
                        indexingBehaviour);
                if (failureTypes.contains(FailureType.POPULATION)) {
                    return new IndexPopulator.Delegating(actualPopulator) {
                        @Override
                        public void add(
                                Collection<? extends IndexEntryUpdate<?>> updates, CursorContext cursorContext) {
                            throw new RuntimeException(POPULATION_FAILURE_MESSAGE);
                        }
                    };
                }
                return actualPopulator;
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
                IndexAccessor actualAccessor = actualProvider.getOnlineAccessor(
                        descriptor, samplingConfig, tokenNameLookup, openOptions, readOnly, indexingBehaviour);
                return new IndexAccessor.Delegating(actualAccessor) {
                    @Override
                    public IndexUpdater newUpdater(
                            IndexUpdateMode mode, CursorContext cursorContext, boolean parallel) {
                        IndexUpdater actualUpdater = actualAccessor.newUpdater(mode, cursorContext, parallel);
                        return new DelegatingIndexUpdater(actualUpdater) {
                            @Override
                            public void process(IndexEntryUpdate<?> update) throws IndexEntryConflictException {
                                if (!failureTypes.contains(FailureType.SKIP_ONLINE_UPDATES)) {
                                    super.process(update);
                                }
                            }
                        };
                    }
                };
            }

            @Override
            public String getPopulationFailure(
                    IndexDescriptor descriptor, CursorContext cursorContext, ImmutableSet<OpenOption> openOptions) {
                return failureTypes.contains(FailureType.INITIAL_STATE)
                        ? INITIAL_STATE_FAILURE_MESSAGE
                        : actualProvider.getPopulationFailure(descriptor, cursorContext, openOptions);
            }

            @Override
            public InternalIndexState getInitialState(
                    IndexDescriptor descriptor, CursorContext cursorContext, ImmutableSet<OpenOption> openOptions) {
                return failureTypes.contains(FailureType.INITIAL_STATE)
                        ? InternalIndexState.FAILED
                        : actualProvider.getInitialState(descriptor, cursorContext, openOptions);
            }
        };
    }
}
