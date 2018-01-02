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
package org.neo4j.kernel.api.impl.index;

import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.SnapshotDeletionPolicy;
import org.apache.lucene.store.Directory;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.PrefetchingIterator;

import static org.neo4j.helpers.collection.IteratorUtil.emptyIterator;

public class LuceneSnapshotter
{
    private static final String NO_INDEX_COMMIT_TO_SNAPSHOT = "No index commit to snapshot";
    private static final String ID = "backup";

    ResourceIterator<File> snapshot( File indexDir, LuceneIndexWriter writer ) throws IOException
    {
        SnapshotDeletionPolicy deletionPolicy = (SnapshotDeletionPolicy) writer.getIndexDeletionPolicy();

        try
        {
            return new LuceneSnapshotIterator( indexDir, deletionPolicy.snapshot( ID ), deletionPolicy );
        }
        catch(IllegalStateException e)
        {
            if(e.getMessage().equals( NO_INDEX_COMMIT_TO_SNAPSHOT ))
            {
                return emptyIterator();
            }
            throw e;
        }
    }

    ResourceIterator<File> snapshot( File indexDir, Directory directory ) throws IOException
    {
        Collection<IndexCommit> indexCommits = IndexReader.listCommits( directory );
        IndexCommit indexCommit = Iterables.last( indexCommits );
        return new ReadOnlyIndexSnapshotIterator( indexDir, indexCommit );
    }

    private class ReadOnlyIndexSnapshotIterator extends PrefetchingIterator<File> implements ResourceIterator<File>
    {
        private final File indexDirectory;
        private final Iterator<String> fileNames;

        ReadOnlyIndexSnapshotIterator( File indexDirectory, IndexCommit indexCommit ) throws IOException
        {
            this.indexDirectory = indexDirectory;
            this.fileNames = indexCommit.getFileNames().iterator();
        }

        @Override
        protected File fetchNextOrNull()
        {
            if ( !fileNames.hasNext() )
            {
                return null;
            }
            return new File( indexDirectory, fileNames.next() );
        }

        @Override
        public void close()
        {
            // nothing by default
        }
    }

    private class LuceneSnapshotIterator extends ReadOnlyIndexSnapshotIterator implements ResourceIterator<File>
    {
        private final SnapshotDeletionPolicy deletionPolicy;

        LuceneSnapshotIterator( File indexDirectory, IndexCommit indexCommit, SnapshotDeletionPolicy deletionPolicy )
                throws IOException
        {
            super(indexDirectory, indexCommit);
            this.deletionPolicy = deletionPolicy;
        }

        @Override
        public void close()
        {
            try
            {
                deletionPolicy.release( ID );
            }
            catch ( IOException e )
            {
                throw new RuntimeException( "Unable to close lucene index snapshot", e );
            }
        }
    }

}
