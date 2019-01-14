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

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexDeletionPolicy;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SnapshotDeletionPolicy;
import org.apache.lucene.store.Directory;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.helpers.collection.Iterables;

import static org.neo4j.helpers.collection.Iterators.emptyResourceIterator;

/**
 * Create iterators over Lucene index files for a particular {@link IndexCommit index commit}.
 * Applicable only to a single Lucene index partition.
 */
public class LuceneIndexSnapshots
{
    private LuceneIndexSnapshots()
    {
    }

    /**
     * Create index snapshot iterator for a writable index.
     * @param indexFolder index location folder
     * @param indexWriter index writer
     * @return index file name iterator
     * @throws IOException
     */
    public static ResourceIterator<File> forIndex( File indexFolder, IndexWriter indexWriter ) throws IOException
    {
        IndexDeletionPolicy deletionPolicy = indexWriter.getConfig().getIndexDeletionPolicy();
        if ( deletionPolicy instanceof SnapshotDeletionPolicy )
        {
            SnapshotDeletionPolicy policy = (SnapshotDeletionPolicy) deletionPolicy;
            return hasCommits( indexWriter )
                   ? new WritableIndexSnapshotFileIterator( indexFolder, policy )
                   : emptyResourceIterator();
        }
        else
        {
            throw new UnsupportedIndexDeletionPolicy( "Can't perform index snapshot with specified index deletion " +
                                                      "policy: " + deletionPolicy.getClass().getName() + ". " +
                                                      "Only " + SnapshotDeletionPolicy.class.getName() + " is " +
                                                      "supported" );
        }
    }

    /**
     * Create index snapshot iterator for a read only index.
     * @param indexFolder index location folder
     * @param directory index directory
     * @return index file name resource iterator
     * @throws IOException
     */
    public static ResourceIterator<File> forIndex( File indexFolder, Directory directory ) throws IOException
    {
        if ( !hasCommits( directory ) )
        {
            return emptyResourceIterator();
        }
        Collection<IndexCommit> indexCommits = DirectoryReader.listCommits( directory );
        IndexCommit indexCommit = Iterables.last( indexCommits );
        return new ReadOnlyIndexSnapshotFileIterator( indexFolder, indexCommit );
    }

    private static boolean hasCommits( IndexWriter indexWriter ) throws IOException
    {
        Directory directory = indexWriter.getDirectory();
        return hasCommits( directory );
    }

    private static boolean hasCommits( Directory directory ) throws IOException
    {
        return DirectoryReader.indexExists( directory ) && SegmentInfos.readLatestCommit( directory ) != null;
    }
}
