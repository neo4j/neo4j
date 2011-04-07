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

abstract class TxData
{
    final LuceneIndex index;
    
    TxData( LuceneIndex index )
    {
        this.index = index;
    }

    abstract TxData add( Object entityId, String key, Object value );
    
    abstract TxData remove( Object entityId, String key, Object value );

    abstract Pair<Collection<Long>, TxData> query( Query query, QueryContext contextOrNull );

    abstract Pair<Collection<Long>, TxData> get( String key, Object value );
    
    abstract Collection<Long> getOrphans( String key );
    
    abstract void close();

    abstract Pair<Searcher, TxData> asSearcher( QueryContext context );
}
