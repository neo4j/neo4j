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

import org.neo4j.gis.spatial.index.curves.PartialOverlapConfiguration;
import org.neo4j.gis.spatial.index.curves.SpaceFillingCurveConfiguration;
import org.neo4j.gis.spatial.index.curves.StandardConfiguration;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.index.internal.gbptree.MetadataMismatchException;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.kernel.api.IndexCapability;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.storemigration.StoreMigrationParticipant;

public class SpatialIndexProvider extends IndexProvider<SchemaIndexDescriptor>
{
    public static final String KEY = "spatial";
    private static final Descriptor SPATIAL_PROVIDER_DESCRIPTOR = new Descriptor( KEY, "1.0" );

    private final PageCache pageCache;
    private final FileSystemAbstraction fs;
    private final Monitor monitor;
    private final RecoveryCleanupWorkCollector recoveryCleanupWorkCollector;
    private final boolean readOnly;
    private final SpaceFillingCurveConfiguration configuration;
    private final Integer maxBits;

    public SpatialIndexProvider( PageCache pageCache, FileSystemAbstraction fs,
            IndexDirectoryStructure.Factory directoryStructure, Monitor monitor,
            RecoveryCleanupWorkCollector recoveryCleanupWorkCollector, boolean readOnly, Config config )
    {
        super( SPATIAL_PROVIDER_DESCRIPTOR, 0, directoryStructure );
        this.pageCache = pageCache;
        this.fs = fs;
        this.monitor = monitor;
        this.recoveryCleanupWorkCollector = recoveryCleanupWorkCollector;
        this.readOnly = readOnly;
        this.configuration = getConfiguredSpaceFillingCurveConfiguration( config );
        this.maxBits = config.get( GraphDatabaseSettings.space_filling_curve_max_bits );
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
    public IndexDescriptor indexDescriptorFor( SchemaDescriptor schema, String name, String metadata )
    {
        return SchemaIndexDescriptorFactory.forLabelBySchema( schema );
    }

    @Override
    public IndexPopulator getPopulator( long indexId, SchemaIndexDescriptor descriptor,
                                        IndexSamplingConfig samplingConfig )
    {
        if ( readOnly )
        {
            throw new UnsupportedOperationException( "Can't create populator for read only index" );
        }
        SpatialIndexFiles files = new SpatialIndexFiles( directoryStructure(), indexId, fs, maxBits );
        return new SpatialIndexPopulator( indexId, descriptor, samplingConfig, files, pageCache, fs, monitor, configuration );
    }

    @Override
    public IndexAccessor getOnlineAccessor( long indexId, SchemaIndexDescriptor descriptor,
                                            IndexSamplingConfig samplingConfig ) throws IOException
    {
        SpatialIndexFiles files = new SpatialIndexFiles( directoryStructure(), indexId, fs, maxBits );
        return new SpatialIndexAccessor( indexId, descriptor, samplingConfig, pageCache, fs, recoveryCleanupWorkCollector, monitor, files, configuration );
    }

    @Override
    public String getPopulationFailure( long indexId, IndexDescriptor descriptor ) throws IllegalStateException
    {
        SpatialIndexFiles spatialIndexFiles = new SpatialIndexFiles( directoryStructure(), indexId, fs, maxBits );

        try
        {
            for ( SpatialIndexFiles.SpatialFileLayout subIndex : spatialIndexFiles.existing() )
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
        SpatialIndexFiles spatialIndexFiles = new SpatialIndexFiles( directoryStructure(), indexId, fs, maxBits );

        InternalIndexState state = InternalIndexState.ONLINE;
        for ( SpatialIndexFiles.SpatialFileLayout subIndex : spatialIndexFiles.existing() )
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
    public IndexCapability getCapability( IndexDescriptor indexDescriptor )
    {
        return IndexCapability.NO_CAPABILITY;
    }

    @Override
    public boolean compatible( IndexDescriptor indexDescriptor )
    {
        return indexDescriptor instanceof SchemaIndexDescriptor;
    }

    @Override
    public StoreMigrationParticipant storeMigrationParticipant( FileSystemAbstraction fs, PageCache pageCache )
    {
        // Since this native provider is a new one, there's no need for migration on this level.
        // Migration should happen in the combined layer for the time being.
        return StoreMigrationParticipant.NOT_PARTICIPATING;
    }
}
