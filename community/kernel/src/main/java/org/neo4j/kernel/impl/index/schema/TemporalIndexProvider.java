/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import java.io.IOException;

import org.neo4j.index.internal.gbptree.MetadataMismatchException;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.kernel.api.IndexCapability;
import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexValueCapability;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.storemigration.StoreMigrationParticipant;
import org.neo4j.values.storable.ValueCategory;

public class TemporalIndexProvider extends IndexProvider
{
    public static final String KEY = "temporal";
    static final IndexCapability CAPABILITY = new TemporalIndexCapability();
    private static final Descriptor TEMPORAL_PROVIDER_DESCRIPTOR = new Descriptor( KEY, "1.0" );

    private final PageCache pageCache;
    private final FileSystemAbstraction fs;
    private final Monitor monitor;
    private final RecoveryCleanupWorkCollector recoveryCleanupWorkCollector;
    private final boolean readOnly;

    public TemporalIndexProvider( PageCache pageCache, FileSystemAbstraction fs,
                                  IndexDirectoryStructure.Factory directoryStructure, Monitor monitor,
                                  RecoveryCleanupWorkCollector recoveryCleanupWorkCollector, boolean readOnly )
    {
        super( TEMPORAL_PROVIDER_DESCRIPTOR, 0, directoryStructure );
        this.pageCache = pageCache;
        this.fs = fs;
        this.monitor = monitor;
        this.recoveryCleanupWorkCollector = recoveryCleanupWorkCollector;
        this.readOnly = readOnly;
    }

    @Override
    public IndexPopulator getPopulator( long indexId, SchemaIndexDescriptor descriptor, IndexSamplingConfig samplingConfig )
    {
        if ( readOnly )
        {
            throw new UnsupportedOperationException( "Can't create populator for read only index" );
        }
        TemporalIndexFiles files = new TemporalIndexFiles( directoryStructure(), indexId, descriptor, fs );
        return new TemporalIndexPopulator( indexId, descriptor, samplingConfig, files, pageCache, fs, monitor );
    }

    @Override
    public IndexAccessor getOnlineAccessor( long indexId, SchemaIndexDescriptor descriptor, IndexSamplingConfig samplingConfig ) throws IOException
    {
        TemporalIndexFiles files = new TemporalIndexFiles( directoryStructure(), indexId, descriptor, fs );
        return new TemporalIndexAccessor( indexId, descriptor, samplingConfig, pageCache, fs, recoveryCleanupWorkCollector, monitor, files );
    }

    @Override
    public String getPopulationFailure( long indexId, SchemaIndexDescriptor descriptor ) throws IllegalStateException
    {
        TemporalIndexFiles temporalIndexFiles = new TemporalIndexFiles( directoryStructure(), indexId, descriptor, fs );

        try
        {
            for ( TemporalIndexFiles.FileLayout subIndex : temporalIndexFiles.existing() )
            {
                String indexFailure = NativeSchemaIndexes.readFailureMessage( pageCache, subIndex.indexFile );
                if ( indexFailure != null )
                {
                    return indexFailure;
                }
            }
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
        throw new IllegalStateException( "Index " + indexId + " isn't failed" );
    }

    @Override
    public InternalIndexState getInitialState( long indexId, SchemaIndexDescriptor descriptor )
    {
        TemporalIndexFiles temporalIndexFiles = new TemporalIndexFiles( directoryStructure(), indexId, descriptor, fs );

        final Iterable<TemporalIndexFiles.FileLayout> existing = temporalIndexFiles.existing();
        InternalIndexState state = InternalIndexState.ONLINE;
        for ( TemporalIndexFiles.FileLayout subIndex : existing )
        {
            try
            {
                switch ( NativeSchemaIndexes.readState( pageCache, subIndex.indexFile ) )
                {
                case FAILED:
                    return InternalIndexState.FAILED;
                case POPULATING:
                    state = InternalIndexState.POPULATING;
                default: // continue
                }
            }
            catch ( MetadataMismatchException | IOException e )
            {
                monitor.failedToOpenIndex( indexId, descriptor, "Requesting re-population.", e );
                return InternalIndexState.POPULATING;
            }
        }
        return state;
    }

    @Override
    public IndexCapability getCapability( SchemaIndexDescriptor schemaIndexDescriptor )
    {
        return CAPABILITY;
    }

    @Override
    public StoreMigrationParticipant storeMigrationParticipant( FileSystemAbstraction fs, PageCache pageCache )
    {
        // Since this native provider is a new one, there's no need for migration on this level.
        // Migration should happen in the combined layer for the time being.
        return StoreMigrationParticipant.NOT_PARTICIPATING;
    }

    /**
     * For single property temporal queries capabilities are
     * Order: ASCENDING
     * Value: YES (can provide exact value)
     *
     * For other queries there is no support
     */
    private static class TemporalIndexCapability implements IndexCapability
    {
        @Override
        public IndexOrder[] orderCapability( ValueCategory... valueCategories )
        {
            if ( support( valueCategories ) )
            {
                return ORDER_ASC;
            }
            return ORDER_NONE;
        }

        @Override
        public IndexValueCapability valueCapability( ValueCategory... valueCategories )
        {
            if ( support( valueCategories ) )
            {
                return IndexValueCapability.YES;
            }
            if ( singleWildcard( valueCategories ) )
            {
                return IndexValueCapability.PARTIAL;
            }
            return IndexValueCapability.NO;
        }

        private boolean support( ValueCategory[] valueCategories )
        {
            return valueCategories.length == 1 && valueCategories[0] == ValueCategory.TEMPORAL;
        }
    }
}
