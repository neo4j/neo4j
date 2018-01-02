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
package org.neo4j.kernel.impl.api.index;

import java.io.IOException;

import org.neo4j.kernel.api.exceptions.index.IndexCapacityExceededException;
import org.neo4j.kernel.api.index.IndexEntryConflictException;

/**
 * This class represents validated and prepared index updates that are ready to be flushed.
 * Flushing is performed with {@link ValidatedIndexUpdates#flush()} method.
 * Releasing of resources is performed with {@link ValidatedIndexUpdates#close()} method.
 * <p>
 * Notion of being 'prepared' indicates that index updates are placed in internal data structures and no
 * pre-processing before {@link ValidatedIndexUpdates#flush()} is needed. Such data structure might represent simply
 * a mapping 'Index -> [set of updates]'.
 * <p>
 * Notion of being 'validated' indicates that all updates in this batch can be consumed by corresponding indexes.
 * This mostly mean that index size permit new insertions {@see IndexCapacityExceededException}.
 */
public interface ValidatedIndexUpdates extends AutoCloseable
{
    ValidatedIndexUpdates NONE = new ValidatedIndexUpdates()
    {
        @Override
        public void flush()
        {
        }

        @Override
        public void close()
        {
        }

        @Override
        public boolean hasChanges()
        {
            return false;
        }
    };

    /**
     * @return whether or not there are any updates to be {@link #flush() flushed}.
     */
    boolean hasChanges();

    /**
     * Flush all validated and prepared index updates to corresponding indexes.
     */
    void flush() throws IOException, IndexEntryConflictException, IndexCapacityExceededException;

    /**
     * Release all possible resources used by this batch of index updates. Such resources might include
     * {@link org.neo4j.kernel.api.index.IndexUpdater} and {@link org.neo4j.kernel.api.index.Reservation}.
     */
    @Override
    void close();
}
