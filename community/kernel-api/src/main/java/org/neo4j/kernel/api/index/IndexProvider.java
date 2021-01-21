/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.api.index;

import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.nio.file.Path;

import org.neo4j.common.TokenNameLookup;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.schema.IndexConfigCompleter;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.memory.ByteBufferFactory;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.api.index.IndexSamplingConfig;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.migration.StoreMigrationParticipant;

/**
 * Contract for implementing an index in Neo4j.
 *
 * This is a sensitive thing to implement, because it manages data that is controlled by
 * Neo4js logical log. As such, the implementation needs to behave under some rather strict rules.
 *
 * <h3>Populating the index</h3>
 *
 * When an index rule is added, the IndexingService is notified. It will, in turn, ask
 * your {@link IndexProvider} for a
 * {@link #getPopulator(IndexDescriptor, IndexSamplingConfig, ByteBufferFactory, MemoryTracker, TokenNameLookup)} batch index writer}.
 *
 * A background index job is triggered, and all existing data that applies to the new rule, as well as new data
 * from the "outside", will be inserted using the writer. You are guaranteed that usage of this writer,
 * during population, will be single threaded.
 *
 * These are the rules you must adhere to here:
 *
 * <ul>
 * <li>You CANNOT say that the state of the index is {@link InternalIndexState#ONLINE}</li>
 * <li>You MUST store all updates given to you</li>
 * <li>You MAY persistently store the updates</li>
 * </ul>
 *
 *
 * <h3>The Flip</h3>
 *
 * Once population is done, the index needs to be "flipped" to an online mode of operation.
 *
 * The index will be notified, through the {@link org.neo4j.kernel.api.index.IndexPopulator#close(boolean, PageCursorTracer)}
 * method, that population is done, and that the index should turn it's state to {@link InternalIndexState#ONLINE} or
 * {@link InternalIndexState#FAILED} depending on the value given to the
 * {@link org.neo4j.kernel.api.index.IndexPopulator#close(boolean, PageCursorTracer)}  close method}.
 *
 * If the index is persisted to disk, this is a <i>vital</i> part of the index lifecycle.
 * For a persisted index, the index MUST NOT store the state as online unless it first guarantees that the entire index
 * is flushed to disk. Failure to do so could produce a situation where, after a crash,
 * an index is believed to be online when it in fact was not yet fully populated. This would break the database
 * recovery process.
 *
 * If you are implementing this interface, you can choose to not store index state. In that case,
 * you should report index state as {@link InternalIndexState#POPULATING} upon startup.
 * This will cause the database to re-create the index from scratch again.
 *
 * These are the rules you must adhere to here:
 *
 * <ul>
 * <li>You MUST have flushed the index to durable storage if you are to persist index state as {@link InternalIndexState#ONLINE}</li>
 * <li>You MAY decide not to store index state</li>
 * <li>If you don't store index state, you MUST default to {@link InternalIndexState#POPULATING}</li>
 * </ul>
 *
 * <h3>Online operation</h3>
 *
 * Once the index is online, the database will move to using the
 * {@link #getOnlineAccessor(IndexDescriptor, IndexSamplingConfig, TokenNameLookup) online accessor} to
 * write to the index.
 */
public abstract class IndexProvider extends LifecycleAdapter implements IndexConfigCompleter
{
    public interface Monitor
    {
        void failedToOpenIndex( IndexDescriptor index, String action, Exception cause );

        void recoveryCleanupRegistered( Path indexFile, IndexDescriptor index );

        void recoveryCleanupStarted( Path indexFile, IndexDescriptor index );

        void recoveryCleanupFinished( Path indexFile, IndexDescriptor index,
                long numberOfPagesVisited, long numberOfTreeNodes, long numberOfCleanedCrashPointers, long durationMillis );

        void recoveryCleanupClosed( Path indexFile, IndexDescriptor index );

        void recoveryCleanupFailed( Path indexFile, IndexDescriptor index, Throwable throwable );
    }

    public static final IndexProvider EMPTY =
            new IndexProvider( new IndexProviderDescriptor( "no-index-provider", "1.0" ), IndexDirectoryStructure.NONE )
            {
                @Override
                public IndexDescriptor completeConfiguration( IndexDescriptor index )
                {
                    return index;
                }

                private final IndexAccessor singleWriter = IndexAccessor.EMPTY;
                private final IndexPopulator singlePopulator = IndexPopulator.EMPTY;
                private final MinimalIndexAccessor singleMinimalAccessor = MinimalIndexAccessor.EMPTY;

                @Override
                public IndexAccessor getOnlineAccessor( IndexDescriptor descriptor, IndexSamplingConfig samplingConfig, TokenNameLookup tokenNameLookup )
                {
                    return singleWriter;
                }

                @Override
                public MinimalIndexAccessor getMinimalIndexAccessor( IndexDescriptor descriptor )
                {
                    return singleMinimalAccessor;
                }

                @Override
                public IndexPopulator getPopulator( IndexDescriptor descriptor, IndexSamplingConfig samplingConfig, ByteBufferFactory bufferFactory,
                        MemoryTracker memoryTracker, TokenNameLookup tokenNameLookup )
                {
                    return singlePopulator;
                }

                @Override
                public InternalIndexState getInitialState( IndexDescriptor descriptor, PageCursorTracer cursorTracer )
                {
                    return InternalIndexState.ONLINE;
                }

                @Override
                public StoreMigrationParticipant storeMigrationParticipant( FileSystemAbstraction fs,
                        PageCache pageCache, StorageEngineFactory storageEngineFactory )
                {
                    return StoreMigrationParticipant.NOT_PARTICIPATING;
                }

                @Override
                public String getPopulationFailure( IndexDescriptor descriptor, PageCursorTracer cursorTracer )
                {
                    return StringUtils.EMPTY;
                }
            };

    private final IndexProviderDescriptor providerDescriptor;
    private final IndexDirectoryStructure.Factory directoryStructureFactory;
    private final IndexDirectoryStructure directoryStructure;

    protected IndexProvider( IndexProvider copySource )
    {
        this( copySource.providerDescriptor, copySource.directoryStructureFactory );
    }

    protected IndexProvider( IndexProviderDescriptor descriptor, IndexDirectoryStructure.Factory directoryStructureFactory )
    {
        this.directoryStructureFactory = directoryStructureFactory;
        assert descriptor != null;
        this.providerDescriptor = descriptor;
        this.directoryStructure = directoryStructureFactory.forProvider( descriptor );
    }

    public abstract MinimalIndexAccessor getMinimalIndexAccessor( IndexDescriptor descriptor );

    /**
     * Used for initially populating a created index, using batch insertion.
     */
    public abstract IndexPopulator getPopulator( IndexDescriptor descriptor, IndexSamplingConfig samplingConfig, ByteBufferFactory bufferFactory,
            MemoryTracker memoryTracker, TokenNameLookup tokenNameLookup );

    /**
     * Used for updating an index once initial population has completed.
     */
    public abstract IndexAccessor getOnlineAccessor( IndexDescriptor descriptor, IndexSamplingConfig samplingConfig, TokenNameLookup tokenNameLookup )
            throws IOException;

    /**
     * Returns a failure previously gotten from {@link IndexPopulator#markAsFailed(String)}
     *
     * Implementations are expected to persist this failure
     * @param descriptor {@link IndexDescriptor} of the index.
     * @param cursorTracer underlying page cursor tracer
     * @return failure, in the form of a stack trace, that happened during population or empty string if there is no failure
     */
    public abstract String getPopulationFailure( IndexDescriptor descriptor, PageCursorTracer cursorTracer );

    /**
     * Called during startup to find out which state an index is in. If {@link InternalIndexState#FAILED}
     * is returned then a further call to {@link #getPopulationFailure(IndexDescriptor, PageCursorTracer)} is expected and should return
     * the failure accepted by any call to {@link IndexPopulator#markAsFailed(String)} call at the time
     * of failure.
     * @param descriptor to get initial state for.
     * @param cursorTracer underlying page cursor tracer.
     */
    public abstract InternalIndexState getInitialState( IndexDescriptor descriptor, PageCursorTracer cursorTracer );

    /**
     * @return a description of this index provider
     */
    public IndexProviderDescriptor getProviderDescriptor()
    {
        return providerDescriptor;
    }

    /**
     * Validate that the given index prototype can be used to create an index with the given index provider, or throw an {@link IllegalArgumentException} if
     * that is not the case.
     * @param prototype The prototype to be validated.
     */
    public void validatePrototype( IndexPrototype prototype )
    {
        // By default we only accept BTREE indexes.
        IndexType indexType = prototype.getIndexType();
        if ( indexType != IndexType.BTREE )
        {
            String providerName = getProviderDescriptor().name();
            throw new IllegalArgumentException( "The '" + providerName + "' index provider does not support " + indexType + " indexes: " + prototype );
        }
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        IndexProvider other = (IndexProvider) o;

        return providerDescriptor.equals( other.providerDescriptor );
    }

    @Override
    public int hashCode()
    {
        return providerDescriptor.hashCode();
    }

    /**
     * @return {@link IndexDirectoryStructure} for this schema index provider. From it can be retrieved directories
     * for individual indexes.
     */
    public IndexDirectoryStructure directoryStructure()
    {
        return directoryStructure;
    }

    public abstract StoreMigrationParticipant storeMigrationParticipant( FileSystemAbstraction fs, PageCache pageCache,
            StorageEngineFactory storageEngineFactory );

    public static class Adaptor extends IndexProvider
    {
        protected Adaptor( IndexProviderDescriptor descriptor, IndexDirectoryStructure.Factory directoryStructureFactory )
        {
            super( descriptor, directoryStructureFactory );
        }

        @Override
        public MinimalIndexAccessor getMinimalIndexAccessor( IndexDescriptor descriptor )
        {
            return null;
        }

        @Override
        public IndexPopulator getPopulator( IndexDescriptor descriptor, IndexSamplingConfig samplingConfig, ByteBufferFactory bufferFactory,
                MemoryTracker memoryTracker, TokenNameLookup tokenNameLookup )
        {
            return null;
        }

        @Override
        public IndexAccessor getOnlineAccessor( IndexDescriptor descriptor, IndexSamplingConfig samplingConfig, TokenNameLookup tokenNameLookup )
        {
            return null;
        }

        @Override
        public String getPopulationFailure( IndexDescriptor descriptor, PageCursorTracer cursorTracer )
        {
            return StringUtils.EMPTY;
        }

        @Override
        public InternalIndexState getInitialState( IndexDescriptor descriptor, PageCursorTracer cursorTracer )
        {
            return null;
        }

        @Override
        public StoreMigrationParticipant storeMigrationParticipant( FileSystemAbstraction fs, PageCache pageCache, StorageEngineFactory storageEngineFactory )
        {
            return StoreMigrationParticipant.NOT_PARTICIPATING;
        }

        @Override
        public IndexDescriptor completeConfiguration( IndexDescriptor index )
        {
            return index;
        }
    }

    public static class Delegating extends IndexProvider
    {
        private final IndexProvider provider;

        public Delegating( IndexProvider provider )
        {
            super( provider );
            this.provider = provider;
        }

        @Override
        public MinimalIndexAccessor getMinimalIndexAccessor( IndexDescriptor descriptor )
        {
            return provider.getMinimalIndexAccessor( descriptor );
        }

        @Override
        public IndexPopulator getPopulator( IndexDescriptor descriptor, IndexSamplingConfig samplingConfig, ByteBufferFactory bufferFactory,
                MemoryTracker memoryTracker, TokenNameLookup tokenNameLookup )
        {
            return provider.getPopulator( descriptor, samplingConfig, bufferFactory, memoryTracker, tokenNameLookup );
        }

        @Override
        public IndexAccessor getOnlineAccessor( IndexDescriptor descriptor, IndexSamplingConfig samplingConfig, TokenNameLookup tokenNameLookup )
                throws IOException
        {
            return provider.getOnlineAccessor( descriptor, samplingConfig, tokenNameLookup );
        }

        @Override
        public String getPopulationFailure( IndexDescriptor descriptor, PageCursorTracer cursorTracer )
        {
            return provider.getPopulationFailure( descriptor, cursorTracer );
        }

        @Override
        public InternalIndexState getInitialState( IndexDescriptor descriptor, PageCursorTracer cursorTracer )
        {
            return provider.getInitialState( descriptor, cursorTracer );
        }

        @Override
        public IndexProviderDescriptor getProviderDescriptor()
        {
            return provider.getProviderDescriptor();
        }

        @Override
        public boolean equals( Object o )
        {
            return o instanceof IndexProvider && provider.equals( o );
        }

        @Override
        public int hashCode()
        {
            return provider.hashCode();
        }

        @Override
        public IndexDirectoryStructure directoryStructure()
        {
            return provider.directoryStructure();
        }

        @Override
        public StoreMigrationParticipant storeMigrationParticipant( FileSystemAbstraction fs, PageCache pageCache, StorageEngineFactory storageEngineFactory )
        {
            return provider.storeMigrationParticipant( fs, pageCache, storageEngineFactory );
        }

        @Override
        public void init() throws Exception
        {
            provider.init();
        }

        @Override
        public void start() throws Exception
        {
            provider.start();
        }

        @Override
        public void stop() throws Exception
        {
            provider.stop();
        }

        @Override
        public void shutdown() throws Exception
        {
            provider.shutdown();
        }

        @Override
        public IndexDescriptor completeConfiguration( IndexDescriptor index )
        {
            return provider.completeConfiguration( index );
        }
    }
}
