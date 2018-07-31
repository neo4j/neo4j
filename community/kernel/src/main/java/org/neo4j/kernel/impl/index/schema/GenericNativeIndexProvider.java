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

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
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
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.storageengine.api.schema.StoreIndexDescriptor;
import org.neo4j.values.storable.ValueCategory;

/**
 * Single-value all-in-one native index
 *
 * A composite index query have one predicate per slot / column.
 * The predicate comes in the form of an index query. Any of "exact", "range" or "exist".
 * Other index providers have support for exact predicate on all columns or exists predicate on all columns (full scan).
 * This index provider have some additional capabilities. It can combine the slot predicates under the following rules:
 * a. Exact can only follow another Exact or be in first slot.
 * b. Range can only follow Exact or be in first slot.
 *
 * We use the following notation for the predicates:
 * x: exact predicate
 * -: exists predicate
 * >: range predicate (this could be ranges with zero or one open end)
 *
 * With an index on 5 slots as en example we can build several different composite queries:
 *     p1 p2 p3 p4 p5 (order is important)
 * 1:  x  x  x  x  x
 * 2:  -  -  -  -  -
 * 3:  x  -  -  -  -
 * 4:  x  x  x  x  -
 * 5:  >  -  -  -  -
 * 6:  x  >  -  -  -
 * 7:  x  x  x  x  >
 * 8:  >  x  -  -  - (not allowed!)
 * 9:  >  >  -  -  - (not allowed!)
 * 10: -  x  -  -  - (not allowed!)
 * 11: -  >  -  -  - (not allowed!)
 *
 * 1: Exact match on all slots. Supported by all index providers.
 * 2: Exists scan on all slots. Supported by all index providers.
 * 3: Exact match on first column and exists on the rest.
 * 4: Exact match on all columns but the last.
 * 5: Range on first column and exists on rest.
 * 6: Exact on first, range on second and exists on rest.
 * 7: Exact on all but last column. Range on last.
 * 8: Not allowed because Exact can only follow another Exact.
 * 9: Not allowed because range can only follow Exact.
 * 10: Not allowed because Exact can only follow another Exact.
 * 11: Not allowed because range can only follow Exact.
 *
 * WHY?
 * In short, we only allow "restrictive" predicates (exact or range) if they help us restrict the scan range.
 * Let's take query 11 as example
 * p1 p2 p3 p4 p5
 * -  >  -  -  -
 * Index is sorted first by p1, then p2, etc.
 * Because we have a complete scan on p1 the range predicate on p2 can not restrict the range of the index we need to scan.
 * We COULD allow this query and do filter during scan instead and take the extra cost into account when planning queries.
 * As of writing this, there is no such filtering implementation.
 */
public class GenericNativeIndexProvider extends NativeIndexProvider<CompositeGenericKey,NativeIndexValue>
{
    public static final GraphDatabaseSettings.SchemaIndex SCHEMA_INDEX = GraphDatabaseSettings.SchemaIndex.NATIVE_BTREE10;
    public static final String KEY = SCHEMA_INDEX.providerName();
    public static final IndexProviderDescriptor DESCRIPTOR = new IndexProviderDescriptor( KEY, SCHEMA_INDEX.providerVersion() );

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
    IndexLayout<CompositeGenericKey,NativeIndexValue> layout( StoreIndexDescriptor descriptor )
    {
        int numberOfSlots = descriptor.properties().length;
        return new GenericLayout( numberOfSlots );
    }

    @Override
    protected IndexPopulator newIndexPopulator( File storeFile, IndexLayout<CompositeGenericKey,NativeIndexValue> layout, StoreIndexDescriptor descriptor,
            IndexSamplingConfig samplingConfig )
    {
        return new GenericNativeIndexPopulator( pageCache, fs, storeFile, layout, monitor, descriptor, samplingConfig );
    }

    @Override
    protected IndexAccessor newIndexAccessor( File storeFile, IndexLayout<CompositeGenericKey,NativeIndexValue> layout, StoreIndexDescriptor descriptor,
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
