/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
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
        this.data.add( this, entityId, key, value );
    }
    
    void remove( Object entityId, String key, Object value )
    {
        this.data.remove( this, entityId, key, value );
    }

    Collection<Long> query( Query query, QueryContext contextOrNull )
    {
        return this.data.query( this, query, contextOrNull );
    }

    Collection<Long> get( String key, Object value )
    {
        return this.data.get( this, key, value );
    }
    
    Collection<Long> getOrphans( String key )
    {
        return this.data.getOrphans( key );
    }
    
    void close()
    {
        this.data.close();
    }

    IndexSearcher asSearcher( QueryContext context )
    {
        return this.data.asSearcher( this, context );
    }

    void set( TxData newData )
    {
        this.data = newData;
    }
}
