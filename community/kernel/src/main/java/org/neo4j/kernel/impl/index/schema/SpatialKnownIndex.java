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

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.neo4j.gis.spatial.index.Envelope;
import org.neo4j.gis.spatial.index.curves.HilbertSpaceFillingCurve2D;
import org.neo4j.gis.spatial.index.curves.SpaceFillingCurve;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.index.schema.fusion.SpatialFusionSchemaIndexProvider;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.Value;

import static org.neo4j.kernel.impl.index.schema.NativeSchemaIndexPopulator.BYTE_FAILED;
import static org.neo4j.kernel.impl.index.schema.NativeSchemaIndexPopulator.BYTE_ONLINE;
import static org.neo4j.kernel.impl.index.schema.NativeSchemaIndexPopulator.BYTE_POPULATING;
import static org.neo4j.values.storable.CoordinateReferenceSystem.WGS84;

/**
 * An instance of this class represents a dynamically created sub-index specific to a particular coordinate reference system.
 * This allows the fusion index design to be extended to an unknown number of sub-indexes, one for each CRS.
 */
public class SpatialKnownIndex
{

    public interface Factory
    {
        SpatialKnownIndex selectAndCreate( Map<CoordinateReferenceSystem,SpatialKnownIndex> indexMap, long indexId, Value... values );

        SpatialKnownIndex selectAndCreate( Map<CoordinateReferenceSystem,SpatialKnownIndex> indexMap, long indexId, CoordinateReferenceSystem crs );
    }

    private final File indexFile;
    private final PageCache pageCache;
    private final CoordinateReferenceSystem crs;
    private final long indexId;
    private final FileSystemAbstraction fs;
    private final SchemaIndexProvider.Monitor monitor;
    private final RecoveryCleanupWorkCollector recoveryCleanupWorkCollector;
    private SpatialSchemaIndexAccessor indexAccessor;
    private NativeSchemaIndexPopulator indexPopulator;
    private SpaceFillingCurve curve;

    /**
     * Create a representation of a spatial index for a specific coordinate reference system. This constructure should be used for first time creation.
     */
    public SpatialKnownIndex( IndexDirectoryStructure directoryStructure, CoordinateReferenceSystem crs, long indexId, PageCache pageCache,
            FileSystemAbstraction fs, SchemaIndexProvider.Monitor monitor, RecoveryCleanupWorkCollector recoveryCleanupWorkCollector )
    {
        this.crs = crs;
        this.indexId = indexId;
        this.pageCache = pageCache;
        this.fs = fs;
        this.monitor = monitor;
        this.recoveryCleanupWorkCollector = recoveryCleanupWorkCollector;

        // Depends on crs
        SchemaIndexProvider.Descriptor crsDescriptor =
                new SchemaIndexProvider.Descriptor( Integer.toString( crs.getTable().getTableId() ), Integer.toString( crs.getCode() ) );
        IndexDirectoryStructure indexDir = IndexDirectoryStructure.directoriesBySubProvider( directoryStructure ).forProvider( crsDescriptor );
        indexFile = new File( indexDir.directoryForIndex( indexId ), indexFileName( indexId ) );
        curve = new HilbertSpaceFillingCurve2D( envelopeFromCRS( crs ), 5 );
    }

    static Envelope envelopeFromCRS( CoordinateReferenceSystem crs )
    {
        Envelope curveEnvelope;
        //TODO: Support more CRS
        if ( crs.equals( WGS84 ) )
        {
            curveEnvelope = new Envelope( -180, 180, -90, 90 );
        }
        else
        {
            // TODO: support user configurable default envelope ranges (neo4j.conf or other)
            curveEnvelope = new Envelope( -1000000, 1000000, -1000000, 1000000 );
        }
        //TODO: support 3D
        return curveEnvelope;
    }

    private static String indexFileName( long indexId )
    {
        return "index-" + indexId;
    }

    public boolean indexExists()
    {
        return fs.fileExists( indexFile );
    }

    public String readPopupationFailure() throws IOException
    {
        NativeSchemaIndexHeaderReader headerReader = new NativeSchemaIndexHeaderReader();
        GBPTree.readHeader( pageCache, indexFile, new SpatialFusionSchemaIndexProvider.ReadOnlyMetaNumberLayout(), headerReader );
        return headerReader.failureMessage;
    }

    public InternalIndexState readState() throws IOException
    {
        NativeSchemaIndexHeaderReader headerReader = new NativeSchemaIndexHeaderReader();
        GBPTree.readHeader( pageCache, indexFile, new SpatialFusionSchemaIndexProvider.ReadOnlyMetaNumberLayout(), headerReader );
        switch ( headerReader.state )
        {
        case BYTE_FAILED:
            return InternalIndexState.FAILED;
        case BYTE_ONLINE:
            return InternalIndexState.ONLINE;
        case BYTE_POPULATING:
            return InternalIndexState.POPULATING;
        default:
            throw new IllegalStateException( "Unexpected initial state byte value " + headerReader.state );
        }
    }

    private NativeSchemaIndexPopulator makeIndexPopulator( IndexDescriptor descriptor, IndexSamplingConfig samplingConfig )
    {
        switch ( descriptor.type() )
        {
        case GENERAL:
            return new NativeNonUniqueSchemaIndexPopulator<>( pageCache, fs, indexFile, new SpatialLayoutNonUnique( crs, curve ), samplingConfig, monitor,
                    descriptor, indexId );
        case UNIQUE:
            return new NativeUniqueSchemaIndexPopulator<>( pageCache, fs, indexFile, new SpatialLayoutUnique( crs, curve ), monitor, descriptor, indexId );
        default:
            throw new UnsupportedOperationException( "Can not create index populator of type " + descriptor.type() );
        }
    }

    public IndexPopulator getPopulator( IndexDescriptor descriptor, IndexSamplingConfig samplingConfig )
    {
        if ( indexPopulator == null )
        {
            indexPopulator = makeIndexPopulator( descriptor, samplingConfig );
            try
            {
                // The index populator is always created on demand. The demand occurs when populating is desired, so we can also create it at this point
                // Note that this deletes the GBPTree in advance of re-populating it
                indexPopulator.create();
            }
            catch ( IOException e )
            {
                e.printStackTrace();
            }
        }
        return indexPopulator;
    }

    public void closePopulator( IndexDescriptor descriptor, IndexSamplingConfig samplingConfig, boolean populationCompletedSuccessfully ) throws IOException
    {
        getPopulator( descriptor, samplingConfig ).close( populationCompletedSuccessfully );
        this.indexPopulator = null;
    }

    private SpatialSchemaIndexAccessor makeOnlineAccessor( IndexDescriptor descriptor, IndexSamplingConfig samplingConfig ) throws IOException
    {
        SpatialLayout layout;
        switch ( descriptor.type() )
        {
        case GENERAL:
            layout = new SpatialLayoutNonUnique( crs, curve );
            break;
        case UNIQUE:
            layout = new SpatialLayoutUnique( crs, curve );
            break;
        default:
            throw new UnsupportedOperationException( "Can not create index accessor of type " + descriptor.type() );
        }
        return new SpatialSchemaIndexAccessor<>( pageCache, fs, indexFile, layout, recoveryCleanupWorkCollector, monitor, descriptor, indexId, samplingConfig );
    }

    public IndexAccessor getOnlineAccessor( IndexDescriptor descriptor, IndexSamplingConfig samplingConfig ) throws IOException
    {
        if ( indexAccessor == null )
        {
            if ( !indexExists() )
            {
                // No CRS specific index was created during normal population (due to no data of that CRS being in the database)
                // We need to create the index as if it was created by normal index populating, but empty
                getPopulator( descriptor, samplingConfig ).close( true );
            }
            indexAccessor = makeOnlineAccessor( descriptor, samplingConfig );
        }
        return indexAccessor;
    }
}
