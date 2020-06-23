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
package org.neo4j.kernel.api.impl.index.backup;

import org.apache.lucene.index.IndexCommit;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.internal.helpers.collection.PrefetchingIterator;

/**
 * Iterator over Lucene read only index files for a particular {@link IndexCommit snapshot}.
 * Applicable only to a single Lucene index partition.
 *
 */
class ReadOnlyIndexSnapshotFileIterator extends PrefetchingIterator<Path> implements ResourceIterator<Path>
{
    private final Path indexDirectory;
    private final Iterator<String> fileNames;
    private final IndexCommit indexCommit;

    ReadOnlyIndexSnapshotFileIterator( Path indexDirectory, IndexCommit indexCommit ) throws IOException
    {
        this.indexDirectory = indexDirectory;
        this.indexCommit = indexCommit;
        this.fileNames = this.indexCommit.getFileNames().iterator();
    }

    @Override
    protected Path fetchNextOrNull()
    {
        if ( !fileNames.hasNext() )
        {
            return null;
        }
        return indexDirectory.resolve( fileNames.next() );
    }

    @Override
    public void close()
    {
        // nothing by default
    }

    IndexCommit getIndexCommit()
    {
        return indexCommit;
    }

    Path getIndexDirectory()
    {
        return indexDirectory;
    }
}
