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
import java.io.IOException;

import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.kernel.api.IndexCapability;
import org.neo4j.internal.kernel.api.IndexLimitation;
import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexValueCapability;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.values.storable.ValueCategory;

/**
 * Schema index provider for native indexes backed by e.g. {@link GBPTree}.
 */
public class StringIndexProvider extends NativeIndexProvider<StringSchemaKey,NativeSchemaValue>
{
    public static final String KEY = "string";
    static final IndexCapability CAPABILITY = new StringIndexCapability();
    private static final Descriptor STRING_PROVIDER_DESCRIPTOR = new Descriptor( KEY, "1.0" );

    public StringIndexProvider( PageCache pageCache, FileSystemAbstraction fs,
            IndexDirectoryStructure.Factory directoryStructure, Monitor monitor, RecoveryCleanupWorkCollector recoveryCleanupWorkCollector,
            boolean readOnly )
    {
        super( STRING_PROVIDER_DESCRIPTOR, 0, directoryStructure, pageCache, fs, monitor, recoveryCleanupWorkCollector, readOnly );
    }

    @Override
    Layout<StringSchemaKey,NativeSchemaValue> layout( SchemaIndexDescriptor descriptor )
    {
        return new StringLayout();
    }

    @Override
    protected IndexPopulator newIndexPopulator( File storeFile, Layout<StringSchemaKey,NativeSchemaValue> layout,
                                                SchemaIndexDescriptor descriptor, long indexId,
                                                IndexSamplingConfig samplingConfig )
    {
        return new StringSchemaIndexPopulator( pageCache, fs, storeFile, layout, monitor, descriptor, indexId, samplingConfig );
    }

    @Override
    protected IndexAccessor newIndexAccessor( File storeFile, Layout<StringSchemaKey,NativeSchemaValue> layout, SchemaIndexDescriptor descriptor,
            long indexId, IndexSamplingConfig samplingConfig ) throws IOException
    {
        return new StringSchemaIndexAccessor( pageCache, fs, storeFile, layout, recoveryCleanupWorkCollector, monitor, descriptor,
                indexId, samplingConfig );
    }

    @Override
    public IndexCapability getCapability( SchemaIndexDescriptor schemaIndexDescriptor )
    {
        return CAPABILITY;
    }

    /**
     * For single property string queries capabilities are
     * Order: ASCENDING
     * Value: YES (can provide exact value)
     *
     * For other queries there is no support
     */
    private static class StringIndexCapability implements IndexCapability
    {
        private final IndexLimitation[] limitations = {IndexLimitation.SLOW_CONTAINS};

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

        @Override
        public IndexLimitation[] limitations()
        {
            return limitations;
        }

        private boolean support( ValueCategory[] valueCategories )
        {
            return valueCategories.length == 1 && valueCategories[0] == ValueCategory.TEXT;
        }
    }
}
