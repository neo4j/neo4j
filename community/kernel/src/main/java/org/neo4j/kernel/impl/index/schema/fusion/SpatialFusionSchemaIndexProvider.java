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
package org.neo4j.kernel.impl.index.schema.fusion;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.kernel.api.IndexCapability;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.index.schema.SpatialKnownIndex;
import org.neo4j.kernel.impl.storemigration.StoreMigrationParticipant;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Value;

/**
 * Schema index provider for native indexes backed by e.g. {@link GBPTree}.
 */
public class SpatialFusionSchemaIndexProvider extends SchemaIndexProvider implements SpatialKnownIndex.Factory
{
    public static final String KEY = "spatial";
    public static final Descriptor SPATIAL_PROVIDER_DESCRIPTOR = new Descriptor( KEY, "1.0" );

    private final PageCache pageCache;
    private final FileSystemAbstraction fs;
    private final Monitor monitor;
    private final RecoveryCleanupWorkCollector recoveryCleanupWorkCollector;
    private final boolean readOnly;

    private Map<Long,Map<CoordinateReferenceSystem,SpatialKnownIndex>> indexes = new HashMap<>();

    public SpatialFusionSchemaIndexProvider( PageCache pageCache, FileSystemAbstraction fs,
            IndexDirectoryStructure.Factory directoryStructure, Monitor monitor,
            RecoveryCleanupWorkCollector recoveryCleanupWorkCollector, boolean readOnly )
    {
        super( SPATIAL_PROVIDER_DESCRIPTOR, 0, directoryStructure );
        this.pageCache = pageCache;
        this.fs = fs;
        this.monitor = monitor;
        this.recoveryCleanupWorkCollector = recoveryCleanupWorkCollector;
        this.readOnly = readOnly;
        findAndCreateKnownSpatialIndexes();
    }

    @Override
    public IndexPopulator getPopulator( long indexId, IndexDescriptor descriptor, IndexSamplingConfig samplingConfig )
    {
        if ( readOnly )
        {
            throw new UnsupportedOperationException( "Can't create populator for read only index" );
        }
        return new SpatialFusionIndexPopulator( indexesFor( indexId ), indexId, descriptor, samplingConfig, this );
    }

    @Override
    public IndexAccessor getOnlineAccessor( long indexId, IndexDescriptor descriptor, IndexSamplingConfig samplingConfig ) throws IOException
    {
        return new SpatialFusionIndexAccessor( indexesFor( indexId ), indexId, descriptor, samplingConfig, this );
    }

    Map<CoordinateReferenceSystem,SpatialKnownIndex> indexesFor( long indexId )
    {
        return indexes.computeIfAbsent( indexId, k -> new HashMap<>() );
    }

    @Override
    public String getPopulationFailure( long indexId, IndexDescriptor descriptor ) throws IllegalStateException
    {
        try
        {
            // This assumes a previous call to getInitialState returned that at least one index was failed
            // We find the first failed index failure message
            for ( SpatialKnownIndex index : indexesFor( indexId ).values() )
            {
                String indexFailure = index.readPopulationFailure( descriptor );
                if ( indexFailure != null )
                {
                    return indexFailure;
                }
            }
            throw new IllegalStateException( "Index " + indexId + " isn't failed" );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    @Override
    public InternalIndexState getInitialState( long indexId, IndexDescriptor descriptor )
    {
        // loop through all files, check if file exists, then check state
        // if any have failed, return failed, else if any are populating return populating, else online
        InternalIndexState state = InternalIndexState.ONLINE;
        if ( !indexes.containsKey( indexId ) )
        {
            return InternalIndexState.ONLINE;
        }
        for ( SpatialKnownIndex index : indexes.get( indexId ).values() )
        {
            if ( index.indexExists() )
            {
                try
                {
                    switch ( index.readState( descriptor ) )
                    {
                    case FAILED:
                        return InternalIndexState.FAILED;
                    case POPULATING:
                        state = InternalIndexState.POPULATING;
                    default:
                    }
                }
                catch ( IOException e )
                {
                    monitor.failedToOpenIndex( indexId, descriptor, "Requesting re-population.", e );
                    state = InternalIndexState.POPULATING;
                }
            }
        }
        return state;
    }

    @Override
    public IndexCapability getCapability( IndexDescriptor indexDescriptor )
    {
        // Spatial indexes are not ordered, nor do they return complete values
        return IndexCapability.NO_CAPABILITY;
    }

    @Override
    public StoreMigrationParticipant storeMigrationParticipant( FileSystemAbstraction fs, PageCache pageCache )
    {
        // Since this native provider is a new one, there's no need for migration on this level.
        // Migration should happen in the combined layer for the time being.
        return StoreMigrationParticipant.NOT_PARTICIPATING;
    }

    @Override
    public SpatialKnownIndex selectAndCreate( Map<CoordinateReferenceSystem,SpatialKnownIndex> indexMap, long indexId, Value value )
    {
        PointValue pointValue = (PointValue) value;
        CoordinateReferenceSystem crs = pointValue.getCoordinateReferenceSystem();
        return selectAndCreate( indexMap, indexId, crs );
    }

    @Override
    public SpatialKnownIndex selectAndCreate( Map<CoordinateReferenceSystem,SpatialKnownIndex> indexMap, long indexId, CoordinateReferenceSystem crs )
    {
        return indexMap.computeIfAbsent( crs,
                k -> new SpatialKnownIndex( directoryStructure(), crs, indexId, pageCache, fs, monitor, recoveryCleanupWorkCollector ) );
    }

    /**
     * The expected directory structure for spatial indexes are:
     * provider/{indexId}/spatial/{crs-tableId}-{crs-code}/index-{indexId}
     * If a directory is found for a crs and it is missing the index file, the index will be marked as failed.
     */
    private void findAndCreateKnownSpatialIndexes()
    {
        Pattern pattern = Pattern.compile( "(\\d+)-(\\d+)" );
        File rootDirectory = this.directoryStructure().rootDirectory();
        if ( rootDirectory == null )
        {
            return;
        }
        File[] files = rootDirectory.listFiles();
        if ( files != null )
        {
            for ( File file : files )
            {
                if ( file.isDirectory() )
                {
                    Integer indexId = Integer.parseInt( file.getName() );
                    File[] subdirs = this.directoryStructure().directoryForIndex( indexId ).listFiles();
                    if ( subdirs != null )
                    {
                        for ( File subdir : subdirs )
                        {
                            Matcher m = pattern.matcher( subdir.getName() );
                            if ( m.matches() )
                            {
                                int tableId = Integer.parseInt( m.group( 1 ) );
                                int code = Integer.parseInt( m.group( 2 ) );
                                CoordinateReferenceSystem crs = CoordinateReferenceSystem.get( tableId, code );
                                SpatialKnownIndex index = selectAndCreate( indexesFor( indexId ), indexId, crs );
                                if ( !index.indexExists() )
                                {
                                    index.markAsFailed( "Index file was not found" );
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
