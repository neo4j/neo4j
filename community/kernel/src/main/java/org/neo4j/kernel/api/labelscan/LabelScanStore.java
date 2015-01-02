/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.api.labelscan;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.kernel.api.direct.AllEntriesLabelScanReader;
import org.neo4j.kernel.impl.nioneo.store.UnderlyingStorageException;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.unsafe.batchinsert.LabelScanWriter;

/**
 * Stores label-->nodes mappings. It receives updates in the form of condensed label->node transaction data
 * and can iterate through all nodes for any given label.
 */
public interface LabelScanStore extends Lifecycle
{
    /**
     * From the point a {@link LabelScanReader} is created till it's {@link LabelScanReader#close() closed} the contents it
     * returns cannot change, i.e. it honors repeatable reads.
     *
     * @return a {@link LabelScanReader} capable of retrieving nodes for labels.
     */
    LabelScanReader newReader();

    /**
     * Acquire a writer for updating the store.
     */
    LabelScanWriter newWriter();

    /**
     * Recover updates the store with a stream of updates of label->node mappings. Done during the recovery
     * phase of the database startup. Updates here may contain duplicates with what's already in the store
     * so extra care needs to be taken to ensure correctness after these updates.
     *
     * @param updates the updates to store.
     * @throws IOException if there was a problem updating the store.
     */
    void recover( Iterator<NodeLabelUpdate> updates ) throws IOException;

    /**
     * Forces all changes to disk. Called at certain points from within Neo4j for example when
     * rotating the logical log. After completion of this call there cannot be any essential state that
     * hasn't been forced to disk.
     *
     * @throws UnderlyingStorageException if there was a problem forcing the state to persistent storage.
     */
    void force() throws UnderlyingStorageException;

    AllEntriesLabelScanReader newAllEntriesReader();

    ResourceIterator<File> snapshotStoreFiles() throws IOException;

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

}
