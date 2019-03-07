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
import org.neo4j.internal.recordstorage.SchemaRuleAccess;
import org.neo4j.internal.recordstorage.StoreTokens;
import org.neo4j.io.IOUtils;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.index.IndexProviderDescriptor;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.store.StoreAccess;
import org.neo4j.storageengine.api.StorageIndexReference;
import org.neo4j.token.TokenHolders;

public class IndexAccessors implements Closeable
{
    private final MutableLongObjectMap<IndexAccessor> accessors = new LongObjectHashMap<>();
    private final List<StorageIndexReference> onlineIndexRules = new ArrayList<>();
    private final List<StorageIndexReference> notOnlineIndexRules = new ArrayList<>();

    public IndexAccessors( IndexProviderMap providers,
                           StoreAccess storeAccess,
                           IndexSamplingConfig samplingConfig ) throws IOException
    {
        TokenHolders tokenHolders = StoreTokens.readOnlyTokenHolders( storeAccess.getRawNeoStores() );
        Iterator<StorageIndexReference> indexes =
                SchemaRuleAccess.getSchemaRuleAccess( storeAccess.getSchemaStore(), tokenHolders ).indexesGetAll();
        for (; ; )
        {
            try
            {
                if ( indexes.hasNext() )
                {
                    // we intentionally only check indexes that are online since
                    // - populating indexes will be rebuilt on next startup
                    // - failed indexes have to be dropped by the user anyways
                    StorageIndexReference indexDescriptor = indexes.next();
                    if ( indexDescriptor.isUnique() && !indexDescriptor.hasOwningConstraintReference() )
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

        for ( StorageIndexReference indexRule : onlineIndexRules )
        {
            long indexId = indexRule.getId();
            accessors.put( indexId, provider( providers, indexRule )
                    .getOnlineAccessor( indexRule, samplingConfig ) );
        }
    }

    private IndexProvider provider( IndexProviderMap providers, StorageIndexReference indexRule )
    {
        return providers.lookup( IndexProviderDescriptor.from( indexRule ) );
    }

    public Collection<StorageIndexReference> notOnlineRules()
    {
        return notOnlineIndexRules;
    }

    public IndexAccessor accessorFor( StorageIndexReference indexRule )
    {
        return accessors.get( indexRule.getId() );
    }

    public Iterable<StorageIndexReference> onlineRules()
    {
        return onlineIndexRules;
    }

    @Override
    public void close()
    {
        try
        {
            IOUtils.closeAllUnchecked( accessors.toList() );
        }
        finally
        {
            accessors.clear();
            onlineIndexRules.clear();
            notOnlineIndexRules.clear();
        }
    }
}
