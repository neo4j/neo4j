/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.api.scan;

import java.io.IOException;

import org.neo4j.kernel.impl.api.PrimitiveLongIterator;
import org.neo4j.kernel.impl.nioneo.store.UnderlyingStorageException;
import org.neo4j.kernel.lifecycle.Lifecycle;

import static org.neo4j.helpers.collection.IteratorUtil.emptyPrimitiveLongIterator;

/**
 * Stores label-->nodes mappings. It receives updates in the form of condensed label->node transaction data
 * and can iterate through all nodes for any given label.
 */
public interface LabelScanStore extends Lifecycle
{
    /**
     * Update the store with a stream of updates of label->node mappings.
     * 
     * @param updates the updates to store.
     * @throws IOException if there was a problem updating the store.
     */
    void updateAndCommit( Iterable<NodeLabelUpdate> updates ) throws IOException;
    
    /**
     * Recover updates the store with a stream of updates of label->node mappings. Done during the recovery
     * phase of the database startup. Updates here may contain duplicates with what's already in the store
     * so extra care needs to be taken to ensure correctness after these updates.
     * 
     * @param updates the updates to store.
     * @throws IOException if there was a problem updating the store.
     */
    void recover( Iterable<NodeLabelUpdate> updates ) throws IOException;
    
    /**
     * Forces all changes to disk. Called at certain points from within Neo4j for example when
     * rotating the logical log. After completion of this call there cannot be any essential state that
     * hasn't been forced to disk.
     * 
     * @throws UnderlyingStorageException if there was a problem forcing the state to persistent storage.
     */
    void force() throws UnderlyingStorageException;
    
    /**
     * From the point a {@link Reader} is created till it's {@link Reader#close() closed} the contents it
     * returns cannot change, i.e. it honors repeatable reads.
     * 
     * @param labelId the label id to get nodes for.
     * @return a {@link Reader} capable of retrieving nodes for labels.
     */
    Reader newReader();
    
    /**
     * Initializes the store. After this has been called recovery updates can be processed.
     */
    @Override
    void init() throws IOException;
    
    /**
     * Starts the store. After this has been called updates can be processed.
     */
    @Override
    void start() throws IOException;
    
    @Override
    void stop() throws IOException;
    
    /**
     * Shuts down the store and all resources acquired by it.
     */
    @Override
    void shutdown() throws IOException;
    
    public interface Reader
    {
        PrimitiveLongIterator nodesWithLabel( long labelId );
        
        void close();
    }
    
    public static final Reader EMPTY_READER = new Reader()
    {
        @Override
        public PrimitiveLongIterator nodesWithLabel( long labelId )
        {
            return emptyPrimitiveLongIterator();
        }
        
        @Override
        public void close()
        {   // Nothing to close
        }
    };
}
