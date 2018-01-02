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

import org.apache.lucene.document.Document;

import java.util.Collection;

import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.graphdb.index.IndexHits;

public class DocToIdIterator extends AbstractLegacyIndexHits
{
    private final Collection<EntityId> removedInTransactionState;
    private final EntityId.LongCostume idCostume = new EntityId.LongCostume();
    private IndexReference searcherOrNull;
    private final IndexHits<Document> source;
    private final PrimitiveLongSet idsModifiedInTransactionState;

    public DocToIdIterator( IndexHits<Document> source, Collection<EntityId> exclude, IndexReference searcherOrNull,
            PrimitiveLongSet idsModifiedInTransactionState )
    {
        this.source = source;
        this.removedInTransactionState = exclude;
        this.searcherOrNull = searcherOrNull;
        this.idsModifiedInTransactionState = idsModifiedInTransactionState;
        if ( source.size() == 0 )
        {
            close();
        }
    }

    @Override
    protected boolean fetchNext()
    {
        while ( source.hasNext() )
        {
            Document doc = source.next();
            long id = idFromDoc( doc );
            boolean documentIsFromStore = doc.getFieldable( FullTxData.TX_STATE_KEY ) == null;
            boolean idWillBeReturnedByTransactionStateInstead =
                    documentIsFromStore && idsModifiedInTransactionState.contains( id );
            if ( removedInTransactionState.contains( idCostume.setId( id ) ) ||
                    idWillBeReturnedByTransactionStateInstead )
            {
                // Skip this one, continue to the next
                continue;
            }
            return next( id );
        }
        return endReached();
    }

    static long idFromDoc( Document doc )
    {
        return Long.parseLong( doc.get( LuceneIndex.KEY_DOC_ID ) );
    }

    protected boolean endReached()
    {
        close();
        return false;
    }

    @Override
    public void close()
    {
        if ( !isClosed() )
        {
            this.searcherOrNull.close();
            this.searcherOrNull = null;
        }
    }

    @Override
    public int size()
    {
        /*
         * If stuff was removed from the index during this tx and during the same tx a query that matches them is
         * issued, then it is possible to get negative size from the IndexHits result, if exclude is larger than source.
         * To avoid such weirdness, we return at least 0. Note that the iterator will return no results, as it should.
         */
        return Math.max( 0, source.size() - removedInTransactionState.size() );
    }

    private boolean isClosed()
    {
        return searcherOrNull==null;
    }

    @Override
    public float currentScore()
    {
        return source.currentScore();
    }

    @Override
    protected void finalize() throws Throwable
    {
        close();
        super.finalize();
    }
}
