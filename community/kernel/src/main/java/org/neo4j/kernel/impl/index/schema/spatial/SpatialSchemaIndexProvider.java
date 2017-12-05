/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.impl.index.schema.spatial;

import java.io.File;
import java.io.IOException;

import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.kernel.api.IndexCapability;
import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexValueCapability;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.storemigration.StoreMigrationParticipant;
import org.neo4j.values.storable.ValueGroup;

import static org.neo4j.kernel.impl.index.schema.spatial.SpatialSchemaIndexPopulator.BYTE_FAILED;
import static org.neo4j.kernel.impl.index.schema.spatial.SpatialSchemaIndexPopulator.BYTE_ONLINE;
import static org.neo4j.kernel.impl.index.schema.spatial.SpatialSchemaIndexPopulator.BYTE_POPULATING;

/**
 * Schema index provider for native indexes backed by e.g. {@link GBPTree}.
 */
public class SpatialSchemaIndexProvider extends SchemaIndexProvider
{
    public static final String KEY = "spatial";
    public static final Descriptor SPATIAL_PROVIDER_DESCRIPTOR = new Descriptor( KEY, "1.0" );
    static final IndexCapability CAPABILITY = new SpatialIndexCapability();

    private final PageCache pageCache;
    private final FileSystemAbstraction fs;
    private final Monitor monitor;
    private final RecoveryCleanupWorkCollector recoveryCleanupWorkCollector;
    private final boolean readOnly;

    public SpatialSchemaIndexProvider( PageCache pageCache, FileSystemAbstraction fs,
            IndexDirectoryStructure.Factory directoryStructure, Monitor monitor, RecoveryCleanupWorkCollector recoveryCleanupWorkCollector,
            boolean readOnly )
    {
        super( SPATIAL_PROVIDER_DESCRIPTOR, 0, directoryStructure );
        this.pageCache = pageCache;
        this.fs = fs;
        this.monitor = monitor;
        this.recoveryCleanupWorkCollector = recoveryCleanupWorkCollector;
        this.readOnly = readOnly;
    }

    @Override
    public IndexPopulator getPopulator( long indexId, IndexDescriptor descriptor, IndexSamplingConfig samplingConfig )
    {
        if ( readOnly )
        {
            throw new UnsupportedOperationException( "Can't create populator for read only index" );
        }

        File storeFile = spatialIndexFileFromIndexId( indexId );
        switch ( descriptor.type() )
        {
        case GENERAL:
            return new SpatialNonUniqueSchemaIndexPopulator<>( pageCache, fs, storeFile, new NonUniqueSpatialLayout(), samplingConfig,
                    monitor, descriptor, indexId );
        case UNIQUE:
            return new SpatialUniqueSchemaIndexPopulator<>( pageCache, fs, storeFile, new UniqueSpatialLayout(), monitor, descriptor,
                    indexId );
        default:
            throw new UnsupportedOperationException( "Can not create index populator of type " + descriptor.type() );
        }
    }

    @Override
    public IndexAccessor getOnlineAccessor(
            long indexId, IndexDescriptor descriptor, IndexSamplingConfig samplingConfig ) throws IOException
    {
        File storeFile = spatialIndexFileFromIndexId( indexId );
        SpatialLayout layout;
        switch ( descriptor.type() )
        {
        case GENERAL:
            layout = new NonUniqueSpatialLayout();
            break;
        case UNIQUE:
            layout = new UniqueSpatialLayout();
            break;
        default:
            throw new UnsupportedOperationException( "Can not create index accessor of type " + descriptor.type() );
        }
        return new SpatialSchemaIndexAccessor<>(
                pageCache, fs, storeFile, layout, recoveryCleanupWorkCollector, monitor, descriptor, indexId,
                samplingConfig );
    }

    @Override
    public String getPopulationFailure( long indexId ) throws IllegalStateException
    {
        try
        {
            String failureMessage = readPopulationFailure( indexId );
            if ( failureMessage == null )
            {
                throw new IllegalStateException( "Index " + indexId + " isn't failed" );
            }
            return failureMessage;
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    private String readPopulationFailure( long indexId ) throws IOException
    {
        SpatialSchemaIndexHeaderReader headerReader = new SpatialSchemaIndexHeaderReader();
        GBPTree.readHeader( pageCache, spatialIndexFileFromIndexId( indexId ), new ReadOnlyMetaNumberLayout(),
                headerReader );
        return headerReader.failureMessage;
    }

    @Override
    public InternalIndexState getInitialState( long indexId, IndexDescriptor descriptor )
    {
        try
        {
            SpatialSchemaIndexHeaderReader
                    headerReader = new SpatialSchemaIndexHeaderReader();
            GBPTree.readHeader( pageCache, spatialIndexFileFromIndexId( indexId ), new ReadOnlyMetaNumberLayout(),
                    headerReader );
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
        catch ( IOException e )
        {
            monitor.failedToOpenIndex( indexId, descriptor, "Requesting re-population.", e );
            return InternalIndexState.POPULATING;
        }
    }

    @Override
    public IndexCapability getCapability( IndexDescriptor indexDescriptor )
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

    private File spatialIndexFileFromIndexId( long indexId )
    {
        return new File( directoryStructure().directoryForIndex( indexId ), indexFileName( indexId ) );
    }

    private static String indexFileName( long indexId )
    {
        return "index-" + indexId;
    }

    private class ReadOnlyMetaNumberLayout extends Layout.ReadOnlyMetaLayout
    {
        @Override
        public boolean compatibleWith( long layoutIdentifier, int majorVersion, int minorVersion )
        {
            return (layoutIdentifier == UniqueSpatialLayout.IDENTIFIER &&
                    majorVersion == UniqueSpatialLayout.MAJOR_VERSION &&
                    minorVersion == UniqueSpatialLayout.MINOR_VERSION) ||
                    (layoutIdentifier == NonUniqueSpatialLayout.IDENTIFIER &&
                            majorVersion == NonUniqueSpatialLayout.MAJOR_VERSION &&
                            minorVersion == NonUniqueSpatialLayout.MINOR_VERSION);
        }
    }

    private static class SpatialIndexCapability implements IndexCapability
    {
        private static final IndexOrder[] SUPPORTED_ORDER = {IndexOrder.ASCENDING};
        private static final IndexOrder[] EMPTY_ORDER = new IndexOrder[0];

        @Override
        public IndexOrder[] orderCapability( ValueGroup... valueGroups )
        {
            if ( support( valueGroups ) )
            {
                return SUPPORTED_ORDER;
            }
            return EMPTY_ORDER;
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
            return valueGroups.length == 1 && valueGroups[0] == ValueGroup.GEOMETRY;
        }
    }
}
