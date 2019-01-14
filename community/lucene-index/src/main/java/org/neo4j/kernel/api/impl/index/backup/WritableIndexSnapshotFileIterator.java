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
package org.neo4j.kernel.api.impl.index.backup;

import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.SnapshotDeletionPolicy;

import java.io.File;
import java.io.IOException;

/**
 * Iterator over Lucene index files for a particular {@link IndexCommit snapshot}.
 * Applicable only to a single Lucene index partition.
 * Internally uses {@link SnapshotDeletionPolicy#snapshot()} to create an {@link IndexCommit} that represents
 * consistent state of the index for a particular point in time.
 */
public class WritableIndexSnapshotFileIterator extends ReadOnlyIndexSnapshotFileIterator
{
    private final SnapshotDeletionPolicy snapshotDeletionPolicy;

    WritableIndexSnapshotFileIterator( File indexDirectory, SnapshotDeletionPolicy snapshotDeletionPolicy )
            throws IOException
    {
        super( indexDirectory, snapshotDeletionPolicy.snapshot() );
        this.snapshotDeletionPolicy = snapshotDeletionPolicy;
    }

    @Override
    public void close()
    {
        try
        {
            snapshotDeletionPolicy.release( getIndexCommit() );
        }
        catch ( IOException e )
        {
            throw new SnapshotReleaseException( "Unable to release lucene index snapshot for index in: " +
                                                getIndexDirectory(), e );
        }
    }

}
