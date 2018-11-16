/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.kernel.api.impl.fulltext;

import org.apache.lucene.search.BooleanQuery;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.common.EntityType;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.QueryContext;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.io.IOUtils;
import org.neo4j.kernel.api.impl.index.SearcherReference;
import org.neo4j.kernel.api.impl.index.collector.ValuesIterator;
import org.neo4j.kernel.impl.newapi.AllStoreHolder;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;

import static java.util.Arrays.asList;
import static org.neo4j.kernel.api.impl.fulltext.ScoreEntityIterator.mergeIterators;

/**
 * Manages the transaction state of a specific individual fulltext index, in a given transaction.
 * <p>
 * This works by first querying the base index, then filtering out all results that are modified in this transaction, and then querying an in-memory Lucene
 * index, where the transaction state is indexed.
 * <p>
 * The transaction state is indexed prior to querying whenever we detect that the
 * {@link ReadableTransactionState#getDataRevision()}  transaction data revision} has changed.
 * <p>
 * The actual transaction state indexing is done by the {@link FulltextIndexTransactionStateVisitor}, which for the most part only looks at the ids, and then
 * loads the modified entities up through the existing transaction state, via the {@link AllStoreHolder} API.
 */
class FulltextIndexTransactionState implements Closeable
{
    private final List<AutoCloseable> toCloseLater;
    private final MutableLongSet modifiedEntityIdsInThisTransaction;
    private final TransactionStateLuceneIndexWriter writer;
    private final FulltextIndexTransactionStateVisitor txStateVisitor;
    private final boolean visitingNodes;
    private long lastUpdateRevision;
    private SearcherReference currentSearcher;

    FulltextIndexTransactionState( FulltextIndexDescriptor descriptor )
    {
        toCloseLater = new ArrayList<>();
        writer = new TransactionStateLuceneIndexWriter( descriptor.analyzer() );
        modifiedEntityIdsInThisTransaction = new LongHashSet();
        visitingNodes = descriptor.schema().entityType() == EntityType.NODE;
        txStateVisitor = new FulltextIndexTransactionStateVisitor( descriptor, modifiedEntityIdsInThisTransaction, writer );
    }

    void maybeUpdate( QueryContext context )
    {
        if ( currentSearcher == null || lastUpdateRevision != context.getTransactionStateOrNull().getDataRevision() )
        {
            try
            {
                updateSearcher( context );
            }
            catch ( Exception e )
            {
                throw new RuntimeException( "Could not update fulltext schema index transaction state.", e );
            }
        }
    }

    private void updateSearcher( QueryContext context ) throws Exception
    {
        Read read = context.getRead();
        CursorFactory cursors = context.cursors();
        ReadableTransactionState state = context.getTransactionStateOrNull();
        modifiedEntityIdsInThisTransaction.clear(); // Clear this so we don't filter out entities who have had their changes reversed since last time.
        writer.resetWriterState();

        try ( NodeCursor nodeCursor = visitingNodes ? cursors.allocateNodeCursor() : null;
              RelationshipScanCursor relationshipCursor = visitingNodes ? null : cursors.allocateRelationshipScanCursor();
              PropertyCursor propertyCursor = cursors.allocatePropertyCursor() )
        {
            state.accept( txStateVisitor.init( read, nodeCursor, relationshipCursor, propertyCursor ) );
        }
        currentSearcher = writer.getNearRealTimeSearcher();
        toCloseLater.add( currentSearcher );
        lastUpdateRevision = state.getDataRevision();
    }

    @Override
    public void close() throws IOException
    {
        toCloseLater.add( writer );
        IOUtils.closeAll( toCloseLater );
    }

    public ValuesIterator filter( ValuesIterator iterator, BooleanQuery query )
    {
        iterator = ScoreEntityIterator.filter( iterator, entityId -> !modifiedEntityIdsInThisTransaction.contains( entityId ) );
        iterator = mergeIterators( asList( iterator, FulltextIndexReader.searchLucene( currentSearcher, query ) ) );
        return iterator;
    }
}
