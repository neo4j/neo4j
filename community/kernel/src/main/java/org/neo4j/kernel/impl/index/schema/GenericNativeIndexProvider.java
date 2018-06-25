/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
import java.io.IOException;

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
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.schema.index.StoreIndexDescriptor;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.values.storable.ValueCategory;

/**
 * Single-value all-in-one native index
 */
public class GenericNativeIndexProvider extends NativeIndexProvider<CompositeGenericKey,NativeIndexValue>
{
    public static final String KEY = "all-native"; // TODO should be native, but can't because number index is called that.
    public static final IndexProvider.Descriptor DESCRIPTOR = new IndexProvider.Descriptor( KEY, "1.0" );

    // TODO implement
    public static final IndexCapability CAPABILITY = new IndexCapability()
    {
        @Override
        public IndexOrder[] orderCapability( ValueCategory... valueCategories )
        {
            return new IndexOrder[0];
        }

        @Override
        public IndexValueCapability valueCapability( ValueCategory... valueCategories )
        {
            return null;
        }
    };

    public GenericNativeIndexProvider( int priority, IndexDirectoryStructure.Factory directoryStructureFactory, PageCache pageCache,
            FileSystemAbstraction fs, Monitor monitor, RecoveryCleanupWorkCollector recoveryCleanupWorkCollector, boolean readOnly )
    {
        super( DESCRIPTOR, priority, directoryStructureFactory, pageCache, fs, monitor, recoveryCleanupWorkCollector, readOnly );
    }

    @Override
    Layout<CompositeGenericKey,NativeIndexValue> layout( StoreIndexDescriptor descriptor )
    {
        int numberOfSlots = descriptor.properties().length;
        return new GenericLayout( numberOfSlots );
    }

    @Override
    protected IndexPopulator newIndexPopulator( File storeFile, Layout<CompositeGenericKey,NativeIndexValue> layout, StoreIndexDescriptor descriptor,
            IndexSamplingConfig samplingConfig )
    {
        return new GenericNativeIndexPopulator( pageCache, fs, storeFile, layout, monitor, descriptor, samplingConfig );
    }

    @Override
    protected IndexAccessor newIndexAccessor( File storeFile, Layout<CompositeGenericKey,NativeIndexValue> layout, StoreIndexDescriptor descriptor,
            IndexSamplingConfig samplingConfig ) throws IOException
    {
        return new GenericNativeIndexAccessor( pageCache, fs, storeFile, layout, recoveryCleanupWorkCollector, monitor, descriptor, samplingConfig );
    }

    @Override
    public IndexCapability getCapability()
    {
        return CAPABILITY;
    }
}
