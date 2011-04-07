/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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

import java.util.Collection;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.Searcher;
import org.neo4j.helpers.Pair;
import org.neo4j.index.lucene.QueryContext;

class TxDataHolder
{
    final LuceneIndex index;
    private TxData data;
    
    TxDataHolder( LuceneIndex index, TxData initialData )
    {
        this.index = index;
        this.data = initialData;
    }

    void add( Object entityId, String key, Object value )
    {
        this.data = this.data.add( entityId, key, value );
    }
    
    void remove( Object entityId, String key, Object value )
    {
        this.data = this.data.remove( entityId, key, value );
    }

    Collection<Long> query( Query query, QueryContext contextOrNull )
    {
        Pair<Collection<Long>, TxData> entry = this.data.query( query, contextOrNull );
        this.data = entry.other();
        return entry.first();
    }

    Collection<Long> get( String key, Object value )
    {
        Pair<Collection<Long>, TxData> entry = this.data.get( key, value );
        this.data = entry.other();
        return entry.first();
    }
    
    Collection<Long> getOrphans( String key )
    {
        return this.data.getOrphans( key );
    }
    
    void close()
    {
        this.data.close();
    }

    Searcher asSearcher( QueryContext context )
    {
        Pair<Searcher, TxData> entry = this.data.asSearcher( context );
        this.data = entry.other();
        return entry.first();
    }
}
