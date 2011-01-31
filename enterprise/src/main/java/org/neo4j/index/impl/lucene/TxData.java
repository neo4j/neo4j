/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.index.impl.lucene;

import java.util.Collection;

import org.apache.lucene.search.Query;
import org.neo4j.helpers.Pair;

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
    
    abstract void close();
}
