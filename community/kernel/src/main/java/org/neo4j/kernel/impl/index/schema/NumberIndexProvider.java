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

import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.kernel.api.IndexCapability;
import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexValueCapability;
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
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.storemigration.StoreMigrationParticipant;
import org.neo4j.values.storable.ValueGroup;

import static org.neo4j.kernel.impl.index.schema.NativeSchemaIndexPopulator.BYTE_FAILED;
import static org.neo4j.kernel.impl.index.schema.NativeSchemaIndexPopulator.BYTE_ONLINE;
import static org.neo4j.kernel.impl.index.schema.NativeSchemaIndexPopulator.BYTE_POPULATING;

/**
 * Schema index provider for native indexes backed by e.g. {@link GBPTree}.
 */
public class NumberIndexProvider extends IndexProvider<SchemaIndexDescriptor>
{
    public static final String KEY = "native";
    public static final Descriptor NATIVE_PROVIDER_DESCRIPTOR = new Descriptor( KEY, "1.0" );
    static final IndexCapability CAPABILITY = new NativeIndexCapability();

    private final PageCache pageCache;
    private final FileSystemAbstraction fs;
    private final Monitor monitor;
    private final RecoveryCleanupWorkCollector recoveryCleanupWorkCollector;
    private final boolean readOnly;

    public NumberIndexProvider( PageCache pageCache, FileSystemAbstraction fs,
            IndexDirectoryStructure.Factory directoryStructure, Monitor monitor, RecoveryCleanupWorkCollector recoveryCleanupWorkCollector,
            boolean readOnly )
    {
        super( NATIVE_PROVIDER_DESCRIPTOR, 0, directoryStructure );
        this.pageCache = pageCache;
        this.fs = fs;
        this.monitor = monitor;
        this.recoveryCleanupWorkCollector = recoveryCleanupWorkCollector;
        this.readOnly = readOnly;
    }

    @Override
    public SchemaIndexDescriptor indexDescriptorFor( SchemaDescriptor schema, String name )
    {
        return null;
    }

    @Override
    public IndexPopulator getPopulator( long indexId, SchemaIndexDescriptor descriptor, IndexSamplingConfig samplingConfig )
    {
        if ( readOnly )
        {
            throw new UnsupportedOperationException( "Can't create populator for read only index" );
        }

        File storeFile = nativeIndexFileFromIndexId( indexId );
        switch ( descriptor.type() )
        {
        case GENERAL:
            return new NativeNonUniqueSchemaIndexPopulator<>( pageCache, fs, storeFile, new NumberLayoutNonUnique(), samplingConfig,
                    monitor, descriptor, indexId );
        case UNIQUE:
            return new NativeUniqueSchemaIndexPopulator<>( pageCache, fs, storeFile, new NumberLayoutUnique(), monitor, descriptor,
                    indexId );
        default:
            throw new UnsupportedOperationException( "Can not create index populator of type " + descriptor.type() );
        }
    }

    @Override
    public IndexAccessor getOnlineAccessor(
            long indexId, SchemaIndexDescriptor descriptor, IndexSamplingConfig samplingConfig ) throws IOException
    {
        File storeFile = nativeIndexFileFromIndexId( indexId );
        NumberLayout layout;
        switch ( descriptor.type() )
        {
        case GENERAL:
            layout = new NumberLayoutNonUnique();
            break;
        case UNIQUE:
            layout = new NumberLayoutUnique();
            break;
        default:
            throw new UnsupportedOperationException( "Can not create index accessor of type " + descriptor.type() );
        }
        return new NumberSchemaIndexAccessor<>(
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
        NativeSchemaIndexHeaderReader headerReader = new NativeSchemaIndexHeaderReader();
        GBPTree.readHeader( pageCache, nativeIndexFileFromIndexId( indexId ), new ReadOnlyMetaNumberLayout(),
                headerReader );
        return headerReader.failureMessage;
    }

    @Override
    public InternalIndexState getInitialState( long indexId, SchemaIndexDescriptor descriptor )
    {
        try
        {
            NativeSchemaIndexHeaderReader headerReader = new NativeSchemaIndexHeaderReader();
            GBPTree.readHeader( pageCache, nativeIndexFileFromIndexId( indexId ), new ReadOnlyMetaNumberLayout(),
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
    public IndexCapability getCapability( SchemaIndexDescriptor schemaIndexDescriptor )
    {
        return CAPABILITY;
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

    private File nativeIndexFileFromIndexId( long indexId )
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
            return (layoutIdentifier == NumberLayoutUnique.IDENTIFIER &&
                    majorVersion == NumberLayoutUnique.MAJOR_VERSION &&
                    minorVersion == NumberLayoutUnique.MINOR_VERSION) ||
                    (layoutIdentifier == NumberLayoutNonUnique.IDENTIFIER &&
                            majorVersion == NumberLayoutNonUnique.MAJOR_VERSION &&
                            minorVersion == NumberLayoutNonUnique.MINOR_VERSION);
        }
    }

    private static class NativeIndexCapability implements IndexCapability
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
            return valueGroups.length == 1 && valueGroups[0] == ValueGroup.NUMBER;
        }
    }
}
