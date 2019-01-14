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
package org.neo4j.consistency.checking.index;

import org.eclipse.collections.api.map.primitive.MutableLongObjectMap;
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.SchemaStorage;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.storageengine.api.schema.StoreIndexDescriptor;

public class IndexAccessors implements Closeable
{
    private final MutableLongObjectMap<IndexAccessor> accessors = new LongObjectHashMap<>();
    private final List<StoreIndexDescriptor> onlineIndexRules = new ArrayList<>();
    private final List<StoreIndexDescriptor> notOnlineIndexRules = new ArrayList<>();

    public IndexAccessors( IndexProviderMap providers,
                           RecordStore<DynamicRecord> schemaStore,
                           IndexSamplingConfig samplingConfig ) throws IOException
    {
        Iterator<StoreIndexDescriptor> indexes = new SchemaStorage( schemaStore ).indexesGetAll();
        for (; ; )
        {
            try
            {
                if ( indexes.hasNext() )
                {
                    // we intentionally only check indexes that are online since
                    // - populating indexes will be rebuilt on next startup
                    // - failed indexes have to be dropped by the user anyways
                    StoreIndexDescriptor indexDescriptor = indexes.next();
                    if ( indexDescriptor.isIndexWithoutOwningConstraint() )
                    {
                        notOnlineIndexRules.add( indexDescriptor );
                    }
                    else
                    {
                        if ( InternalIndexState.ONLINE ==
                                provider( providers, indexDescriptor ).getInitialState( indexDescriptor ) )
                        {
                            onlineIndexRules.add( indexDescriptor );
                        }
                        else
                        {
                            notOnlineIndexRules.add( indexDescriptor );
                        }
                    }
                }
                else
                {
                    break;
                }
            }
            catch ( Exception e )
            {
                // ignore; inconsistencies of the schema store are specifically handled elsewhere.
            }
        }

        for ( StoreIndexDescriptor indexRule : onlineIndexRules )
        {
            long indexId = indexRule.getId();
            accessors.put( indexId, provider( providers, indexRule )
                    .getOnlineAccessor( indexRule, samplingConfig ) );
        }
    }

    private IndexProvider provider( IndexProviderMap providers, StoreIndexDescriptor indexRule )
    {
        return providers.lookup( indexRule.providerDescriptor() );
    }

    public Collection<StoreIndexDescriptor> notOnlineRules()
    {
        return notOnlineIndexRules;
    }

    public IndexAccessor accessorFor( StoreIndexDescriptor indexRule )
    {
        return accessors.get( indexRule.getId() );
    }

    public Iterable<StoreIndexDescriptor> onlineRules()
    {
        return onlineIndexRules;
    }

    @Override
    public void close() throws IOException
    {
        for ( IndexAccessor accessor : accessors )
        {
            accessor.close();
        }
        accessors.clear();
        onlineIndexRules.clear();
        notOnlineIndexRules.clear();
    }
}
