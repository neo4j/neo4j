/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.io.IOUtils;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.newapi.AllStoreHolder;
import org.neo4j.logging.Log;
import org.neo4j.storageengine.api.EntityType;
import org.neo4j.storageengine.api.schema.StoreIndexDescriptor;

/**
 * Manages the transaction state of a specific individual fulltext index, in a given transaction.
 * <p>
 * This works by first querying the base index, then filtering out all results that are modified in this transaction, and then querying an in-memory Lucene
 * index, where the transaction state is indexed. This all happens in the {@link TransactionStateFulltextIndexReader}.
 * <p>
 * The transaction state is indexed prior to querying whenever we detect that the
 * {@link KernelTransactionImplementation#getTransactionDataRevision() transaction data revision} has changed.
 * <p>
 * The actual transaction state indexing is done by the {@link FulltextIndexTransactionStateVisitor}, which for the most part only looks at the ids, and then
 * loads the modified entities up through the existing transaction state, via the {@link AllStoreHolder} API.
 */
class FulltextIndexTransactionState implements Closeable
{
    private final FulltextIndexDescriptor descriptor;
    private final List<AutoCloseable> toCloseLater;
    private final MutableLongSet modifiedEntityIdsInThisTransaction;
    private final TransactionStateLuceneIndexWriter writer;
    private final FulltextIndexTransactionStateVisitor txStateVisitor;
    private final boolean visitingNodes;
    private long lastUpdateRevision;
    private FulltextIndexReader currentReader;

    FulltextIndexTransactionState( FulltextIndexProvider provider, Log log, IndexReference indexReference )
    {
        FulltextIndexAccessor accessor = provider.getOpenOnlineAccessor( (StoreIndexDescriptor) indexReference );
        log.debug( "Acquired online fulltext schema index accessor, as base accessor for transaction state: %s", accessor );
        descriptor = accessor.getDescriptor();
        SchemaDescriptor schema = descriptor.schema();
        toCloseLater = new ArrayList<>();
        writer = accessor.getTransactionStateIndexWriter();
        modifiedEntityIdsInThisTransaction = new LongHashSet();
        visitingNodes = schema.entityType() == EntityType.NODE;
        txStateVisitor = new FulltextIndexTransactionStateVisitor( descriptor, modifiedEntityIdsInThisTransaction, writer );
    }

    FulltextIndexReader getIndexReader( KernelTransactionImplementation kti )
    {
        if ( currentReader == null || lastUpdateRevision != kti.getTransactionDataRevision() )
        {
            if ( currentReader != null )
            {
                toCloseLater.add( currentReader );
            }
            try
            {
                updateReader( kti );
            }
            catch ( Exception e )
            {
                currentReader = null;
                throw new RuntimeException( "Failed to update the fulltext schema index transaction state.", e );
            }
        }
        return currentReader;
    }

    private void updateReader( KernelTransactionImplementation kti ) throws Exception
    {
        modifiedEntityIdsInThisTransaction.clear(); // Clear this so we don't filter out entities who have had their changes reversed since last time.
        writer.resetWriterState();
        AllStoreHolder read = (AllStoreHolder) kti.dataRead();
        TransactionState transactionState = kti.txState();

        try ( NodeCursor nodeCursor = visitingNodes ? kti.cursors().allocateNodeCursor() : null;
              RelationshipScanCursor relationshipCursor = visitingNodes ? null : kti.cursors().allocateRelationshipScanCursor();
              PropertyCursor propertyCursor = kti.cursors().allocatePropertyCursor() )
        {
            transactionState.accept( txStateVisitor.init( read, nodeCursor, relationshipCursor, propertyCursor ) );
        }
        FulltextIndexReader baseReader = (FulltextIndexReader) read.indexReader( descriptor, false );
        FulltextIndexReader nearRealTimeReader = writer.getNearRealTimeReader();
        currentReader = new TransactionStateFulltextIndexReader( baseReader, nearRealTimeReader, modifiedEntityIdsInThisTransaction );
        lastUpdateRevision = kti.getTransactionDataRevision();
    }

    @Override
    public void close() throws IOException
    {
        toCloseLater.add( currentReader );
        toCloseLater.add( writer );
        IOUtils.closeAll( toCloseLater );
    }
}
