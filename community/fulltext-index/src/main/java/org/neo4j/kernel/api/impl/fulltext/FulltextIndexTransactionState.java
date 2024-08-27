/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.api.impl.fulltext;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.LongPredicate;
import org.apache.lucene.analysis.Analyzer;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;
import org.neo4j.common.EntityType;
import org.neo4j.configuration.Config;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.QueryContext;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.IOUtils;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.impl.index.SearcherReference;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;

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
 * loads the modified entities up through the existing transaction state, via the kernel API.
 */
class FulltextIndexTransactionState implements Closeable {
    private final List<AutoCloseable> toCloseLater;
    private final MutableLongSet modifiedEntityIdsInThisTransaction;
    private final TransactionStateLuceneIndexWriter writer;
    private final FulltextIndexTransactionStateVisitor txStateVisitor;
    private final boolean visitingNodes;
    private long lastUpdateRevision;
    private SearcherReference currentSearcher;

    FulltextIndexTransactionState(
            IndexDescriptor descriptor, Config config, Analyzer analyzer, String[] propertyNames) {
        toCloseLater = new ArrayList<>();
        writer = new TransactionStateLuceneIndexWriter(config, analyzer, descriptor.getIndexConfig());
        modifiedEntityIdsInThisTransaction = new LongHashSet();
        visitingNodes = descriptor.schema().entityType() == EntityType.NODE;
        txStateVisitor = new FulltextIndexTransactionStateVisitor(
                descriptor, propertyNames, modifiedEntityIdsInThisTransaction, writer);
    }

    SearcherReference maybeUpdate(QueryContext context, CursorContext cursorContext, MemoryTracker memoryTracker) {
        if (currentSearcher == null
                || lastUpdateRevision != context.getTransactionStateOrNull().getDataRevision()) {
            try {
                updateSearcher(context, cursorContext, memoryTracker);
            } catch (Exception e) {
                throw new RuntimeException("Could not update fulltext schema index transaction state.", e);
            }
        }
        return currentSearcher;
    }

    private void updateSearcher(QueryContext context, CursorContext cursorContext, MemoryTracker memoryTracker)
            throws Exception {
        Read read = context.getRead();
        CursorFactory cursors = context.cursors();
        ReadableTransactionState state = context.getTransactionStateOrNull();
        modifiedEntityIdsInThisTransaction
                .clear(); // Clear this, so we don't filter out entities who have had their changes reversed since last
        // time.
        writer.resetWriterState();

        try (NodeCursor nodeCursor =
                        visitingNodes ? cursors.allocateFullAccessNodeCursor(cursorContext, memoryTracker) : null;
                RelationshipScanCursor relationshipCursor =
                        visitingNodes ? null : cursors.allocateRelationshipScanCursor(cursorContext, memoryTracker);
                PropertyCursor propertyCursor =
                        cursors.allocateFullAccessPropertyCursor(cursorContext, memoryTracker)) {
            state.accept(txStateVisitor.init(read, nodeCursor, relationshipCursor, propertyCursor));
        }
        currentSearcher = writer.getNearRealTimeSearcher();
        toCloseLater.add(currentSearcher);
        lastUpdateRevision = state.getDataRevision();
    }

    @Override
    public void close() throws IOException {
        toCloseLater.add(writer);
        IOUtils.closeAll(toCloseLater);
    }

    public LongPredicate isModifiedInTransactionPredicate() {
        return modifiedEntityIdsInThisTransaction::contains;
    }
}
