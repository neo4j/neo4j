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

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;

import java.util.Collection;

import org.neo4j.index.lucene.QueryContext;

abstract class TxData
{
    final LuceneIndex index;

    TxData( LuceneIndex index )
    {
        this.index = index;
    }

    abstract void add( TxDataHolder holder, EntityId entityId, String key, Object value );

    abstract void remove( TxDataHolder holder, EntityId entityId, String key, Object value );

    abstract Collection<EntityId> query( TxDataHolder holder, Query query, QueryContext contextOrNull );

    abstract Collection<EntityId> get( TxDataHolder holder, String key, Object value );

    abstract Collection<EntityId> getOrphans( String key );

    abstract void close();

    abstract IndexSearcher asSearcher( TxDataHolder holder, QueryContext context );
}
