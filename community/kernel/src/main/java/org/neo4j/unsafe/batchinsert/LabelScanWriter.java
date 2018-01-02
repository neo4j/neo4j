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
package org.neo4j.unsafe.batchinsert;

import java.io.Closeable;
import java.io.IOException;

import org.neo4j.kernel.api.exceptions.index.IndexCapacityExceededException;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;

public interface LabelScanWriter extends Closeable
{
    /**
     * Store a {@link NodeLabelUpdate}. Calls to this method MUST be ordered by ascending node id.
     * 
     * @param update node label update to store
     * @throws IOException some kind of I/O exception has occurred
     * @throws IndexCapacityExceededException when no more index entries can be added
     */
    void write( NodeLabelUpdate update ) throws IOException, IndexCapacityExceededException;

    /**
     * Close this writer and flush pending changes to the store.
     */
    void close() throws IOException;

    LabelScanWriter EMPTY = new LabelScanWriter()
    {
        @Override
        public void write( NodeLabelUpdate update ) throws IOException
        {
            // do nothing
        }

        @Override
        public void close() throws IOException
        {
            // nothing to close
        }
    };
}
