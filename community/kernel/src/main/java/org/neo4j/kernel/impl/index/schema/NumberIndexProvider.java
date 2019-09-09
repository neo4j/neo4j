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

import java.io.File;

import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.kernel.api.IndexCapability;
import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexValueCapability;
import org.neo4j.internal.kernel.api.schema.IndexProviderDescriptor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.storageengine.api.schema.StoreIndexDescriptor;
import org.neo4j.values.storable.ValueCategory;

/**
 * Schema index provider for native indexes backed by e.g. {@link GBPTree}.
 */
public class NumberIndexProvider extends NativeIndexProvider<NumberIndexKey,NativeIndexValue,NumberLayout>
{
    public static final String KEY = "native";
    public static final IndexProviderDescriptor NATIVE_PROVIDER_DESCRIPTOR = new IndexProviderDescriptor( KEY, "1.0" );
    static final IndexCapability CAPABILITY = new NumberIndexCapability();

    public NumberIndexProvider( PageCache pageCache, FileSystemAbstraction fs, IndexDirectoryStructure.Factory directoryStructure, Monitor monitor,
            RecoveryCleanupWorkCollector recoveryCleanupWorkCollector, boolean readOnly )
    {
        super( NATIVE_PROVIDER_DESCRIPTOR, directoryStructure, pageCache, fs, monitor, recoveryCleanupWorkCollector, readOnly );
    }

    @Override
    NumberLayout layout( StoreIndexDescriptor descriptor, File storeFile )
    {
        // split like this due to legacy reasons, there are old stores out there with these different identifiers
        switch ( descriptor.type() )
        {
        case GENERAL:
            return new NumberLayoutNonUnique();
        case UNIQUE:
            return new NumberLayoutUnique();
        default:
            throw new IllegalArgumentException( "Unknown index type " + descriptor.type() );
        }
    }

    @Override
    protected IndexPopulator newIndexPopulator( File storeFile, NumberLayout layout, StoreIndexDescriptor descriptor, ByteBufferFactory bufferFactory )
    {
        return new WorkSyncedNativeIndexPopulator<>( new NumberIndexPopulator( pageCache, fs, storeFile, layout, monitor, descriptor ) );
    }

    @Override
    protected IndexAccessor newIndexAccessor( File storeFile, NumberLayout layout, StoreIndexDescriptor descriptor, boolean readOnly )
    {
        return new NumberIndexAccessor( pageCache, fs, storeFile, layout, recoveryCleanupWorkCollector, monitor, descriptor, readOnly );
    }

    @Override
    public IndexCapability getCapability( StoreIndexDescriptor descriptor )
    {
        return CAPABILITY;
    }

    /**
     * For single property number queries capabilities are
     * Order: ASCENDING
     * Value: YES (can provide exact value)
     *
     * For other queries there is no support
     */
    private static class NumberIndexCapability implements IndexCapability
    {
        @Override
        public IndexOrder[] orderCapability( ValueCategory... valueCategories )
        {
            if ( support( valueCategories ) )
            {
                return ORDER_BOTH;
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

        @Override
        public boolean isFulltextIndex()
        {
            return false;
        }

        @Override
        public boolean isEventuallyConsistent()
        {
            return false;
        }

        private boolean support( ValueCategory[] valueCategories )
        {
            return valueCategories.length == 1 && valueCategories[0] == ValueCategory.NUMBER;
        }
    }
}
