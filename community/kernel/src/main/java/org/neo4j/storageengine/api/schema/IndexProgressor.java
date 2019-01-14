/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.storageengine.api.schema;

import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.LabelSet;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.impl.index.labelscan.LabelScanValueIndexProgressor;
import org.neo4j.kernel.impl.newapi.ExplicitIndexProgressor;
import org.neo4j.values.storable.Value;

/**
 * The index progressor is a cursor like class, which allows controlled progression through the entries of an index.
 * In contrast to a cursor, the progressor does not hold value state, but rather attempts to write the next entry to a
 * Client. The client can them accept the entry, in which case next() returns, or reject it, in which case the
 * progression continues until an acceptable entry is found or the progression is done.
 *
 * A Progressor is expected to feed a single client, which is setup for example in the constructor. The typical
 * interaction goes something like
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
public interface IndexProgressor extends AutoCloseable
{
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
     * Client which accepts nodes and some of their property values.
     */
    interface NodeValueClient
    {
        /**
         * Setup the client for progressing using the supplied progressor. The values feed in accept map to the
         * propertyIds provided here. Called by index implementation.
         * @param descriptor The descriptor
         * @param progressor The progressor
         * @param query The query of this progression
         */
        void initialize( SchemaIndexDescriptor descriptor, IndexProgressor progressor,
                         IndexQuery[] query );

        /**
         * Accept the node id and values of a candidate index entry. Return true if the entry is
         * accepted, false otherwise.
         * @param reference the node id of the candidate index entry
         * @param values the values of the candidate index entry
         * @return true if the entry is accepted, false otherwise
         */
        boolean acceptNode( long reference, Value... values );

        boolean needsValues();
    }

    /**
     * Client which accepts nodes and some of their labels.
     */
    interface NodeLabelClient
    {
        /**
         * Setup the client for progressing using the supplied progressor. Called by index implementation.
         * @param progressor the progressor
         * @param providesLabels true if the progression can provide label information
         * @param label the label to scan for
         */
        void scan( LabelScanValueIndexProgressor progressor, boolean providesLabels, int label );

        void unionScan( IndexProgressor progressor, boolean providesLabels, int... labels );

        void intersectionScan( IndexProgressor progressor, boolean providesLabels, int... labels );

        /**
         * Accept the node id and (some) labels of a candidate index entry. Return true if the entry
         * is accepted, false otherwise.
         * @param reference the node id of the candidate index entry
         * @param labels some labels of the candidate index entry
         * @return true if the entry is accepted, false otherwise
         */
        boolean acceptNode( long reference, LabelSet labels );
    }

    /**
     * Client which accepts graph entities (nodes and relationships) and a fuzzy score.
     */
    interface ExplicitClient
    {
        /**
         * Setup the client for progressing using the supplied progressor. Called by index implementation.
         * @param progressor the progressor
         * @param expectedSize expected number of entries this progressor will feed the client.
         */
        void initialize( ExplicitIndexProgressor progressor, int expectedSize );

        /**
         * Accept the entity id and a score. Return true if the entry is accepted, false otherwise
         * @param reference the node id of the candidate index entry
         * @param score score of the candidate index entry
         * @return true if the entry is accepted, false otherwise
         */
        boolean acceptEntity( long reference, float score );
    }

    IndexProgressor EMPTY = new IndexProgressor()
    {
        @Override
        public boolean next()
        {
            return false;
        }

        @Override
        public void close()
        {   // no-op
        }
    };
}
