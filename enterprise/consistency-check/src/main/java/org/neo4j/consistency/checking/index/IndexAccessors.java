/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.consistency.checking.index;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.neo4j.kernel.api.exceptions.schema.MalformedSchemaRuleException;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexConfiguration;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.IndexRule;
import org.neo4j.kernel.impl.nioneo.store.RecordStore;

import static org.neo4j.consistency.checking.schema.IndexRules.loadAllIndexRules;

public class IndexAccessors implements Closeable
{
    private final Map<Long,IndexAccessor> accessors = new HashMap<>();
    private final List<IndexRule> indexRules = new ArrayList<>();

    public IndexAccessors( SchemaIndexProvider provider, RecordStore<DynamicRecord> schemaStore )
            throws IOException, MalformedSchemaRuleException
    {
        Iterator<IndexRule> rules = loadAllIndexRules( schemaStore ).iterator();
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
                    if ( InternalIndexState.ONLINE == provider.getInitialState( indexRule.getId() ) )
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
            accessors.put(indexRule.getId(), provider.getOnlineAccessor(
                    indexRule.getId(), new IndexConfiguration( indexRule.isConstraintIndex() ) ) );
        }
    }

    public IndexAccessor accessorFor(IndexRule indexRule)
    {
        return accessors.get( indexRule.getId() );
    }

    // TODO: return pair of rules and accessor
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
