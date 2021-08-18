/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.kernel.impl.index.schema;

import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.nio.file.Path;

import org.neo4j.common.EntityType;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.configuration.Config;
import org.neo4j.index.internal.gbptree.MetadataMismatchException;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.TokenPredicate;
import org.neo4j.internal.schema.IndexBehaviour;
import org.neo4j.internal.schema.IndexCapability;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexOrderCapability;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.IndexQuery;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.IndexValueCapability;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.memory.ByteBufferFactory;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.index.MinimalIndexAccessor;
import org.neo4j.kernel.impl.api.index.IndexSamplingConfig;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.migration.StoreMigrationParticipant;
import org.neo4j.storageengine.migration.TokenIndexMigrator;
import org.neo4j.util.Preconditions;
import org.neo4j.values.storable.ValueCategory;

import static org.apache.commons.lang3.StringUtils.defaultIfEmpty;

public class TokenIndexProvider extends IndexProvider
{
    public static final IndexProviderDescriptor DESCRIPTOR = new IndexProviderDescriptor( "token-lookup", "1.0" );
    public static final IndexCapability CAPABILITY = new TokenIndexCapability();

    private final DatabaseIndexContext databaseIndexContext;
    private final RecoveryCleanupWorkCollector recoveryCleanupWorkCollector;
    private final Monitor monitor;
    private final Config config;
    private final DatabaseLayout databaseLayout;

    protected TokenIndexProvider( DatabaseIndexContext databaseIndexContext, IndexDirectoryStructure.Factory directoryStructureFactory,
            RecoveryCleanupWorkCollector recoveryCleanupWorkCollector,
            Config config, DatabaseLayout databaseLayout )
    {
        super( DESCRIPTOR, directoryStructureFactory );
        this.databaseIndexContext = databaseIndexContext;
        this.recoveryCleanupWorkCollector = recoveryCleanupWorkCollector;
        this.monitor = databaseIndexContext.monitors.newMonitor( IndexProvider.Monitor.class, databaseIndexContext.monitorTag );
        this.config = config;
        this.databaseLayout = databaseLayout;
    }

    @Override
    public MinimalIndexAccessor getMinimalIndexAccessor( IndexDescriptor descriptor )
    {
        return new NativeMinimalIndexAccessor( descriptor, indexFiles( descriptor ), databaseIndexContext.readOnlyChecker );
    }

    @Override
    public IndexPopulator getPopulator( IndexDescriptor descriptor, IndexSamplingConfig samplingConfig, ByteBufferFactory bufferFactory,
            MemoryTracker memoryTracker, TokenNameLookup tokenNameLookup )
    {
        if ( databaseIndexContext.readOnlyChecker.isReadOnly() )
        {
            throw new UnsupportedOperationException( "Can't create populator for read only index" );
        }

        return new WorkSyncedIndexPopulator( new TokenIndexPopulator( databaseIndexContext, databaseLayout, indexFiles( descriptor ), config, descriptor ) );
    }

    @Override
    public IndexAccessor getOnlineAccessor( IndexDescriptor descriptor, IndexSamplingConfig samplingConfig, TokenNameLookup tokenNameLookup ) throws IOException
    {
        return new TokenIndexAccessor( databaseIndexContext, databaseLayout, indexFiles( descriptor ), config, descriptor, recoveryCleanupWorkCollector );
    }

    @Override
    public String getPopulationFailure( IndexDescriptor descriptor, CursorContext cursorContext )
    {
        try
        {
            String failureMessage = TokenIndexes.readFailureMessage( databaseIndexContext.pageCache, storeFile( descriptor ), databaseIndexContext.databaseName,
                    cursorContext );
            return defaultIfEmpty( failureMessage, StringUtils.EMPTY );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    @Override
    public InternalIndexState getInitialState( IndexDescriptor descriptor, CursorContext cursorContext )
    {
        try
        {
            return TokenIndexes.readState( databaseIndexContext.pageCache, storeFile( descriptor ), databaseIndexContext.databaseName, cursorContext );
        }
        catch ( MetadataMismatchException | IOException e )
        {
            monitor.failedToOpenIndex( descriptor, "Requesting re-population.", e );
            return InternalIndexState.POPULATING;
        }
    }

    @Override
    public StoreMigrationParticipant storeMigrationParticipant( FileSystemAbstraction fs, PageCache pageCache, StorageEngineFactory storageEngineFactory )
    {
        return new TokenIndexMigrator( "Token indexes", fs, storageEngineFactory, databaseLayout );
    }

    @Override
    public IndexDescriptor completeConfiguration( IndexDescriptor index )
    {
        if ( index.getCapability().equals( IndexCapability.NO_CAPABILITY ) )
        {
            index = index.withIndexCapability( CAPABILITY );
        }
        return index;
    }

    @Override
    public void validatePrototype( IndexPrototype prototype )
    {
        IndexType indexType = prototype.getIndexType();
        if ( indexType != IndexType.LOOKUP )
        {
            throw new IllegalArgumentException(
                    "The '" + getProviderDescriptor().name() + "' index provider does not support " + indexType + " indexes: " + prototype );
        }
        if ( !prototype.schema().isAnyTokenSchemaDescriptor() )
        {
            throw new IllegalArgumentException(
                    "The " + prototype.schema() + " index schema is not an any-token index schema, which it is required to be for the '" +
                    getProviderDescriptor().name() + "' index provider to be able to create an index." );
        }
        if ( !prototype.getIndexProvider().equals( DESCRIPTOR ) )
        {
            throw new IllegalArgumentException(
                    "The '" + getProviderDescriptor().name() + "' index provider does not support " + prototype.getIndexProvider() + " indexes: " + prototype );
        }
        if ( prototype.isUnique() )
        {
            throw new IllegalArgumentException(
                    "The '" + getProviderDescriptor().name() + "' index provider does not support uniqueness indexes: " + prototype );
        }
    }

    private Path storeFile( IndexDescriptor descriptor )
    {
        IndexFiles indexFiles = indexFiles( descriptor );
        return indexFiles.getStoreFile();
    }

    private IndexFiles indexFiles( IndexDescriptor descriptor )
    {
        EntityType entityType = descriptor.schema().entityType();
        boolean labelIndex = entityType == EntityType.NODE;
        Path filePath = labelIndex ? databaseLayout.labelScanStore() : databaseLayout.relationshipTypeScanStore();
        return new IndexFiles.SingleFile( databaseIndexContext.fileSystem, filePath );
    }

    private static class TokenIndexCapability implements IndexCapability
    {
        @Override
        public IndexOrderCapability orderCapability( ValueCategory... valueCategories )
        {
            return IndexOrderCapability.BOTH_FULLY_SORTED;
        }

        @Override
        public IndexValueCapability valueCapability( ValueCategory... valueCategories )
        {
            return IndexValueCapability.YES;
        }

        @Override
        public boolean isQuerySupported( IndexQuery.IndexQueryType queryType, ValueCategory valueCategory )
        {
            return false;
        }

        @Override
        public double getCostMultiplier( IndexQuery.IndexQueryType... queryTypes )
        {
            return 1.0;
        }

        @Override
        public boolean supportPartitionedScan( IndexQuery... queries )
        {
            Preconditions.requireNoNullElements( queries );
            return queries.length == 1 && queries[0] instanceof TokenPredicate;
        }
    }
}
