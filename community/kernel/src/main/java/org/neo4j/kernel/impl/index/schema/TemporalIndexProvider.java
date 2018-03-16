/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
import org.neo4j.values.storable.ValueGroup;
import org.neo4j.values.storable.Values;

public class TemporalIndexProvider extends IndexProvider
{
    public static final String KEY = "temporal";
    public static final Descriptor TEMPORAL_PROVIDER_DESCRIPTOR = new Descriptor( KEY, "1.0" );
    private static final IndexCapability CAPABILITY = new TemporalIndexCapability();

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
                String indexFailure = NativeSchemaIndexes.readFailureMessage( pageCache, subIndex.indexFile, subIndex.layout );
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

        InternalIndexState state = InternalIndexState.ONLINE;
        for ( TemporalIndexFiles.FileLayout subIndex : temporalIndexFiles.existing() )
        {
            try
            {
                switch ( NativeSchemaIndexes.readState( pageCache, subIndex.indexFile, subIndex.layout ) )
                {
                case FAILED:
                    return InternalIndexState.FAILED;
                case POPULATING:
                    state = InternalIndexState.POPULATING;
                default: // continue
                }
            }
            catch ( IOException e )
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

    private static class TemporalIndexCapability implements IndexCapability
    {
        private static final IndexOrder[] SUPPORTED_ORDER = {IndexOrder.ASCENDING};

        @Override
        public IndexOrder[] orderCapability( ValueGroup... valueGroups )
        {
            return support( valueGroups ) ? SUPPORTED_ORDER : EMPTY_ORDER;
        }

        @Override
        public IndexValueCapability valueCapability( ValueGroup... valueGroups )
        {
            if ( support( valueGroups ) )
            {
                return IndexValueCapability.YES;
            }
            if ( singleWildcard( valueGroups ) )
            {
                return IndexValueCapability.PARTIAL;
            }
            return IndexValueCapability.NO;
        }

        private boolean singleWildcard( ValueGroup[] valueGroups )
        {
            return valueGroups.length == 1 && valueGroups[0] == ValueGroup.UNKNOWN;
        }

        private boolean support( ValueGroup[] valueGroups )
        {
            return valueGroups.length == 1 && Values.isTemporalValue( valueGroups[0] );
        }
    }
}
