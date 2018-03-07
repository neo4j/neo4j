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

import org.neo4j.gis.spatial.index.curves.PartialOverlapConfiguration;
import org.neo4j.gis.spatial.index.curves.SpaceFillingCurveConfiguration;
import org.neo4j.gis.spatial.index.curves.StandardConfiguration;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
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
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.index.schema.SpatialCRSSchemaIndex;
import org.neo4j.kernel.impl.storemigration.StoreMigrationParticipant;
import org.neo4j.values.storable.CoordinateReferenceSystem;

/**
 * Schema index provider for native indexes backed by e.g. {@link GBPTree}.
 */
public class SpatialFusionSchemaIndexProvider extends SchemaIndexProvider implements SpatialCRSSchemaIndex.Supplier
{
    public static final String KEY = "spatial";
    public static final Descriptor SPATIAL_PROVIDER_DESCRIPTOR = new Descriptor( KEY, "1.0" );
    private static final Pattern CRS_DIR_PATTERN = Pattern.compile( "(\\d+)-(\\d+)" );

    private final PageCache pageCache;
    private final FileSystemAbstraction fs;
    private final Monitor monitor;
    private final RecoveryCleanupWorkCollector recoveryCleanupWorkCollector;
    private final boolean readOnly;
    private final SpaceFillingCurveConfiguration configuration;
    private final int maxLevels;

    private Map<Long,Map<CoordinateReferenceSystem,SpatialCRSSchemaIndex>> indexes = new HashMap<>();

    public SpatialFusionSchemaIndexProvider( PageCache pageCache, FileSystemAbstraction fs,
            IndexDirectoryStructure.Factory directoryStructure, Monitor monitor, RecoveryCleanupWorkCollector recoveryCleanupWorkCollector, boolean readOnly,
            Config config )
    {
        super( SPATIAL_PROVIDER_DESCRIPTOR, 0, directoryStructure );
        this.pageCache = pageCache;
        this.fs = fs;
        this.monitor = monitor;
        this.recoveryCleanupWorkCollector = recoveryCleanupWorkCollector;
        this.readOnly = readOnly;
        this.configuration = getConfiguredSpaceFillingCurveConfiguration( config );
        this.maxLevels = config.get( GraphDatabaseSettings.space_filling_curve_max_levels );
    }

    private static SpaceFillingCurveConfiguration getConfiguredSpaceFillingCurveConfiguration( Config config )
    {
        int extraLevels = config.get( GraphDatabaseSettings.space_filling_curve_extra_levels );
        double topThreshold = config.get( GraphDatabaseSettings.space_filling_curve_top_threshold );
        double bottomThreshold = config.get( GraphDatabaseSettings.space_filling_curve_bottom_threshold );

        if ( topThreshold == 0.0 || bottomThreshold == 0.0 )
        {
            return new StandardConfiguration( extraLevels );
        }
        else
        {
            return new PartialOverlapConfiguration( extraLevels, topThreshold, bottomThreshold );
        }
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

    Map<CoordinateReferenceSystem,SpatialCRSSchemaIndex> indexesFor( long indexId )
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
            for ( SpatialCRSSchemaIndex index : indexesFor( indexId ).values() )
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

        Map<CoordinateReferenceSystem,SpatialCRSSchemaIndex> indexMap = indexesFor( indexId );
        findAndCreateSpatialIndex( indexMap, indexId, descriptor );

        InternalIndexState state = InternalIndexState.ONLINE;
        for ( SpatialCRSSchemaIndex index : indexMap.values() )
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
    public SpatialCRSSchemaIndex get( IndexDescriptor descriptor,
            Map<CoordinateReferenceSystem,SpatialCRSSchemaIndex> indexMap, long indexId, CoordinateReferenceSystem crs )
    {
        return indexMap.computeIfAbsent( crs,
                crsKey -> new SpatialCRSSchemaIndex( descriptor, directoryStructure(), crsKey, indexId, pageCache, fs, monitor,
                        recoveryCleanupWorkCollector, configuration, maxLevels ) );
    }

    /**
     * The expected directory structure for spatial indexes are:
     * provider/{indexId}/spatial/{crs-tableId}-{crs-code}/index-{indexId}
     * If a directory is found for a crs and it is missing the index file, the index will be marked as failed.
     */
    private void findAndCreateSpatialIndex( Map<CoordinateReferenceSystem,SpatialCRSSchemaIndex> indexMap, long indexId, IndexDescriptor descriptor )
    {
        File directoryForIndex = this.directoryStructure().directoryForIndex( indexId );
        if ( directoryForIndex != null )
        {
            File[] crsDirs = directoryForIndex.listFiles();
            if ( crsDirs != null )
            {
                for ( File crsDir : crsDirs )
                {
                    Matcher m = CRS_DIR_PATTERN.matcher( crsDir.getName() );
                    if ( m.matches() )
                    {
                        int tableId = Integer.parseInt( m.group( 1 ) );
                        int code = Integer.parseInt( m.group( 2 ) );
                        CoordinateReferenceSystem crs = CoordinateReferenceSystem.get( tableId, code );
                        SpatialCRSSchemaIndex index = get( descriptor, indexMap, indexId, crs );
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
