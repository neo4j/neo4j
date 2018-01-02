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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.neo4j.index.lucene.QueryContext;
import org.neo4j.index.lucene.ValueContext;

public class ExactTxData extends TxData
{
    private Map<String, Map<Object, Set<EntityId>>> data;
    private boolean hasOrphans;

    ExactTxData( LuceneIndex index )
    {
        super( index );
    }

    @Override
    void add( TxDataHolder holder, EntityId entityId, String key, Object value )
    {
        idCollection( key, value, true ).add( entityId );
    }

    private Set<EntityId> idCollection( String key, Object value, boolean create )
    {
        Map<Object, Set<EntityId>> keyMap = keyMap( key, create );
        if ( keyMap == null )
        {
            return null;
        }

        Set<EntityId> ids = keyMap.get( value );
        if ( ids == null && create )
        {
            ids = new HashSet<>();
            keyMap.put( value, ids );
            if ( value == null )
            {
                hasOrphans = true;
            }
        }
        return ids;
    }

    private Map<Object, Set<EntityId>> keyMap( String key, boolean create )
    {
        if ( data == null )
        {
            if ( create )
            {
                data = new HashMap<>();
            }
            else
            {
                return null;
            }
        }

        Map<Object, Set<EntityId>> inner = data.get( key );
        if ( inner == null && create )
        {
            inner = new HashMap<>();
            data.put( key, inner );
            if ( key == null )
            {
                hasOrphans = true;
            }
        }
        return inner;
    }

    private TxData toFullTxData()
    {
        FullTxData data = new FullTxData( index );
        if ( this.data != null )
        {
            for ( Map.Entry<String, Map<Object, Set<EntityId>>> entry : this.data.entrySet() )
            {
                String key = entry.getKey();
                for ( Map.Entry<Object, Set<EntityId>> valueEntry : entry.getValue().entrySet() )
                {
                    Object value = valueEntry.getKey();
                    for ( EntityId id : valueEntry.getValue() )
                    {
                        data.add( null, id, key, value );
                    }
                }
            }
        }
        return data;
    }

    @Override
    void close()
    {
    }

    @Override
    Collection<EntityId> query( TxDataHolder holder, Query query, QueryContext contextOrNull )
    {
        if ( contextOrNull != null && contextOrNull.getTradeCorrectnessForSpeed() )
        {
            return Collections.emptyList();
        }

        TxData fullTxData = toFullTxData();
        holder.set( fullTxData );
        return fullTxData.query( holder, query, contextOrNull );
    }

    @Override
    void remove( TxDataHolder holder, EntityId entityId, String key, Object value )
    {
        if ( data == null )
        {
            return;
        }

        if ( key == null || value == null )
        {
            TxData fullData = toFullTxData();
            fullData.remove( holder, entityId, key, value );
            holder.set( fullData );
        }
        else
        {
            Collection<EntityId> ids = idCollection( key, value, false );
            if ( ids != null )
            {
                ids.remove( entityId );
            }
        }
    }

    @Override
    Collection<EntityId> get( TxDataHolder holder, String key, Object value )
    {
        value = value instanceof ValueContext ? ((ValueContext) value).getCorrectValue() : value.toString();
        Set<EntityId> ids = idCollection( key, value, false );
        if ( ids == null || ids.isEmpty() )
        {
            return Collections.emptySet();
        }
        return ids;
    }

    @Override
    Collection<EntityId> getOrphans( String key )
    {
        if ( !hasOrphans )
        {
            return null;
        }

        Set<EntityId> orphans = idCollection( null, null, false );
        Set<EntityId> keyOrphans = idCollection( key, null, false );
        return LuceneTransactionState.merge( orphans, keyOrphans );
    }

    @Override
    IndexSearcher asSearcher( TxDataHolder holder, QueryContext context )
    {
        if ( context != null && context.getTradeCorrectnessForSpeed() )
        {
            return null;
        }
        TxData fullTxData = toFullTxData();
        holder.set( fullTxData );
        return fullTxData.asSearcher( holder, context );
    }
}
