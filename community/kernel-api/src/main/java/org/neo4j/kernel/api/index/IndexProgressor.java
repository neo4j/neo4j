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
package org.neo4j.kernel.api.index;

import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.values.storable.Value;

/**
 * The index progressor is a cursor like class, which allows controlled progression through the entries of an index.
 * In contrast to a cursor, the progressor does not hold value state, but rather attempts to write the next entry to a
 * Client. The client can them accept the entry, in which case next() returns, or reject it, in which case the
 * progression continues until an acceptable entry is found, or the progression is done.
 *
 * A Progressor is expected to feed a single client, which is set up for example in the constructor. The typical
 * interaction goes something like:
 *
 *   -- query(client) -> INDEX
 *                       progressor = new Progressor( client )
 *                       client.initialize( progressor, ... )
 *
 *   -- next() --> client
 *                 client ---- next() --> progressor
 *                        <-- accept() --
 *                                 :false
 *                        <-- accept() --
 *                                 :false
 *                        <-- accept() --
 *                                  :true
 *                 client <--------------
 *   <-----------
 */
public interface IndexProgressor extends AutoCloseable {
    /**
     * Progress through the index until the next accepted entry. Entries are feed to a Client, which
     * is setup in an implementation specific way.
     *
     * @return true if an accepted entry was found, false otherwise
     */
    boolean next();

    /**
     * Close the progressor and all attached resources. Idempotent.
     */
    @Override
    void close();

    /**
     * Client which accepts entities and some of their property values.
     */
    interface EntityValueClient {
        /**
         * Setup the client for progressing using the supplied progressor. The values feed in accept map to the
         * propertyIds provided here. Called by index implementation.
         * @param descriptor The descriptor
         * @param progressor The progressor
         * @param indexIncludesTransactionState {@code true} if the index takes transaction state into account such that the entities delivered through
         * {@link #acceptEntity(long, float, Value...)} have already been filtered through, and merged with, the transaction state. If this is {@code true},
         * then the client does not need to do its own transaction state filtering. This is the case for the fulltext schema indexes, for instance.
         * Otherwise, if this parameter is {@code false}, then the client needs to filter and merge the transaction state in on their own.
         * @param needStoreFilter {@code true} if the index might return false positives that need to be filtered through the store, otherwise {@code false}.
         * @param constraints Constraints on the produced results, like the required order the index should return entity ids in, or if the index should fetch
         * property values together with entity ids.
         * @param query The query of this progression
         */
        void initializeQuery(
                IndexDescriptor descriptor,
                IndexProgressor progressor,
                boolean indexIncludesTransactionState,
                boolean needStoreFilter,
                IndexQueryConstraints constraints,
                PropertyIndexQuery... query);

        /**
         * Accept the entity id and values of a candidate index entry. Return true if the entry is
         * accepted, false otherwise.
         * @param reference the entity id of the candidate index entry
         * @param score a score figure for the quality of the match, for indexes where this makes sense, otherwise {@link Float#NaN}.
         * @param values the values of the candidate index entry
         * @return true if the entry is accepted, false otherwise
         */
        boolean acceptEntity(long reference, float score, Value... values);

        boolean needsValues();
    }

    /**
     * Client which accepts nodes and some of their labels.
     */
    interface EntityTokenClient {
        /**
         * Setup the client for progressing using the supplied progressor. The values fed in to acceptEntity map to the
         * tokenId provided here. Called by index implementation.
         * @param progressor The progressor
         * @param token The token id to query
         * @param order Required order the index should return entity ids in.
         */
        void initializeQuery(IndexProgressor progressor, int token, IndexOrder order);

        /**
         * Setup the client for progressing using the supplied progressor. The values fed in to acceptEntity map to the
         * tokenId provided here. Called by index implementation.
         * This version of initialize should be used when transaction state shouldn't be used to find added/remove entities,
         * but instead the sent in sets. Useful for example when doing work in batches.
         * @param progressor The progressor
         * @param token The token id to query
         * @param added Added entities that should be included in the results.
         * @param removed Removed entities that should be excluded from the results.
         */
        void initializeQuery(IndexProgressor progressor, int token, LongIterator added, LongSet removed);

        /**
         * Accept the entity id and token of a candidate index entry. Return true if the entry
         * is accepted, false otherwise.
         * @param reference the entity id of the candidate index entry
         * @param tokenId the token ID of this index entry
         * @return true if the entry is accepted, false otherwise
         */
        boolean acceptEntity(long reference, int tokenId);
    }

    IndexProgressor EMPTY = new IndexProgressor() {
        @Override
        public boolean next() {
            return false;
        }

        @Override
        public void close() { // no-op
        }
    };
}
