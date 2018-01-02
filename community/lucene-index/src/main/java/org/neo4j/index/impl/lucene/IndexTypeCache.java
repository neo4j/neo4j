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
package org.neo4j.index.impl.lucene;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.helpers.Pair;
import org.neo4j.kernel.impl.index.IndexConfigStore;

class IndexTypeCache
{
    private final Map<IndexIdentifier, Pair<Integer, IndexType>> cache = Collections.synchronizedMap(
            new HashMap<IndexIdentifier, Pair<Integer, IndexType>>() );
    private final IndexConfigStore indexStore;

    IndexTypeCache( IndexConfigStore indexStore )
    {
        this.indexStore = indexStore;
    }

    IndexType getIndexType( IndexIdentifier identifier, boolean recovery )
    {
        Pair<Integer, IndexType> type = cache.get( identifier );
        Map<String, String> config = indexStore.get( identifier.entityType.entityClass(), identifier.indexName );
        if ( type != null && config.hashCode() == type.first() )
        {
            return type.other();
        }
        if ( config == null )
        {
            if ( recovery )
            {
                return null;
            }
            throw new IllegalStateException( "Unknown index " + identifier );
        }
        type = Pair.of( config.hashCode(), IndexType.getIndexType( config ) );
        cache.put( identifier, type );
        return type.other();
    }

    void invalidate( IndexIdentifier identifier )
    {
        cache.remove( identifier );
    }
}
