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
package org.neo4j.consistency.checking.index;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.SchemaStorage;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.IndexRule;

public class IndexAccessors implements Closeable
{
    private final Map<Long,IndexAccessor> accessors = new HashMap<>();
    private final List<IndexRule> indexRules = new ArrayList<>();

    public IndexAccessors( SchemaIndexProvider provider,
                           RecordStore<DynamicRecord> schemaStore,
                           IndexSamplingConfig samplingConfig ) throws IOException
    {
        Iterator<IndexRule> rules = new SchemaStorage( schemaStore ).indexesGetAll();
        for (; ; )
        {
            try
            {
                if ( rules.hasNext() )
                {
                    // we intentionally only check indexes that are online since
                    // - populating indexes will be rebuilt on next startup
                    // - failed indexes have to be dropped by the user anyways
                    IndexRule indexRule = rules.next();
                    if ( InternalIndexState.ONLINE == provider.getInitialState( indexRule.getId(), indexRule.getIndexDescriptor() ) )
                    {
                        indexRules.add( indexRule );
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

        for ( IndexRule indexRule : indexRules )
        {
            long indexId = indexRule.getId();
            accessors.put( indexId, provider.getOnlineAccessor( indexId, indexRule.getIndexDescriptor(), samplingConfig ) );
        }
    }

    public IndexAccessor accessorFor( IndexRule indexRule )
    {
        return accessors.get( indexRule.getId() );
    }

    public Iterable<IndexRule> rules()
    {
        return indexRules;
    }

    @Override
    public void close() throws IOException
    {
        for ( IndexAccessor accessor : accessors.values() )
        {
            accessor.close();
        }
        accessors.clear();
        indexRules.clear();
    }
}
