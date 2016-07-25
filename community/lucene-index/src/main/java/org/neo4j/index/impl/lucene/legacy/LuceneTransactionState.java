/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.index.impl.lucene.legacy;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;

import java.io.Closeable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.neo4j.index.lucene.QueryContext;

class LuceneTransactionState implements Closeable
{
    private final Map<IndexIdentifier, TxDataBoth> txData = new HashMap<>();

    void add( LuceneLegacyIndex index, EntityId entity, String key, Object value )
    {
        TxDataBoth data = getTxData( index, true );
        insert( entity, key, value, data.added( true ), data.removed( false ) );
    }

    TxDataBoth getTxData( LuceneLegacyIndex index,
            boolean createIfNotExists )
    {
        IndexIdentifier identifier = index.getIdentifier();
        TxDataBoth data = txData.get( identifier );
        if ( data == null && createIfNotExists )
        {
            data = new TxDataBoth( index );
            txData.put( identifier, data );
        }
        return data;
    }

    void remove( LuceneLegacyIndex index, EntityId entity, String key, Object value )
    {
        TxDataBoth data = getTxData( index, true );
        insert( entity, key, value, data.removed( true ), data.added( false ) );
    }

    void remove( LuceneLegacyIndex index, EntityId entity, String key )
    {
        TxDataBoth data = getTxData( index, true );
        insert( entity, key, null, data.removed( true ), data.added( false ) );
    }

    void remove( LuceneLegacyIndex index, EntityId entity )
    {
        TxDataBoth data = getTxData( index, true );
        insert( entity, null, null, data.removed( true ), data.added( false ) );
    }

    void delete( LuceneLegacyIndex index )
    {
        IndexIdentifier identifier = index.getIdentifier();
        txData.put( identifier, new DeletedTxDataBoth( index ) );
    }

    private void insert( EntityId entity, String key, Object value, TxDataHolder insertInto,
                                                       TxDataHolder removeFrom )
    {
        if ( removeFrom != null )
        {
            removeFrom.remove( entity, key, value );
        }
        insertInto.add( entity, key, value );
    }

    Collection<EntityId> getRemovedIds( LuceneLegacyIndex index, Query query )
    {
        TxDataHolder removed = removedTxDataOrNull( index );
        if ( removed == null )
        {
            return Collections.emptySet();
        }
        Collection<EntityId> ids = removed.query( query, null );
        return ids != null ? ids : Collections.<EntityId>emptySet();
    }

    Collection<EntityId> getRemovedIds( LuceneLegacyIndex index,
            String key, Object value )
    {
        TxDataHolder removed = removedTxDataOrNull( index );
        if ( removed == null )
        {
            return Collections.emptySet();
        }
        Collection<EntityId> ids = removed.get( key, value );
        Collection<EntityId> orphanIds = removed.getOrphans( key );
        return merge( ids, orphanIds );
    }

    static Collection<EntityId> merge( Collection<EntityId> c1, Collection<EntityId> c2 )
    {
        if ( c1 == null && c2 == null )
        {
            return Collections.emptySet();
        }
        else if ( c1 != null && c2 != null )
        {
            if (c1.isEmpty())
            {
                return c2;
            }
            if (c2.isEmpty())
            {
                return c1;
            }
            Collection<EntityId> result = new HashSet<>( c1.size()+c2.size(), 1 );
            result.addAll( c1 );
            result.addAll( c2 );
            return result;
        }
        else
        {
            return c1 != null ? c1 : c2;
        }
    }

    Collection<EntityId> getAddedIds( LuceneLegacyIndex index, String key, Object value )
    {
        TxDataHolder added = addedTxDataOrNull( index );
        if ( added == null )
        {
            return Collections.emptySet();
        }
        Collection<EntityId> ids = added.get( key, value );
        return ids != null ? ids : Collections.<EntityId>emptySet();
    }

    TxDataHolder addedTxDataOrNull( LuceneLegacyIndex index )
    {
        TxDataBoth data = getTxData( index, false );
        return data != null ? data.added( false ) : null;
    }

    TxDataHolder removedTxDataOrNull( LuceneLegacyIndex index )
    {
        TxDataBoth data = getTxData( index, false );
        return data != null ? data.removed( false ) : null;
    }

    @Override
    public void close()
    {
        for ( TxDataBoth data : this.txData.values() )
        {
            data.close();
        }
        this.txData.clear();
    }

    // Bad name
    private class TxDataBoth
    {
        private TxDataHolder add;
        private TxDataHolder remove;
        final LuceneLegacyIndex index;

        public TxDataBoth( LuceneLegacyIndex index )
        {
            this.index = index;
        }

        TxDataHolder added( boolean createIfNotExists )
        {
            if ( this.add == null && createIfNotExists )
            {
                this.add = new TxDataHolder( index, index.type.newTxData( index ) );
            }
            return this.add;
        }

        TxDataHolder removed( boolean createIfNotExists )
        {
            if ( this.remove == null && createIfNotExists )
            {
                this.remove = new TxDataHolder( index, index.type.newTxData( index ) );
            }
            return this.remove;
        }

        void close()
        {
            safeClose( add );
            safeClose( remove );
        }

        private void safeClose( TxDataHolder data )
        {
            if ( data != null )
            {
                data.close();
            }
        }
    }

    private class DeletedTxDataBoth extends TxDataBoth
    {
        public DeletedTxDataBoth( LuceneLegacyIndex index )
        {
            super( index );
        }

        @Override
        TxDataHolder added( boolean createIfNotExists )
        {
            throw illegalStateException();
        }

        @Override
        TxDataHolder removed( boolean createIfNotExists )
        {
            throw illegalStateException();
        }

        private IllegalStateException illegalStateException()
        {
            throw new IllegalStateException( "This index (" + index.getIdentifier() +
                    ") has been marked as deleted in this transaction" );
        }
    }

    IndexSearcher getAdditionsAsSearcher( LuceneLegacyIndex index,
            QueryContext context )
    {
        TxDataHolder data = addedTxDataOrNull( index );
        return data != null ? data.asSearcher( context ) : null;
    }
}
