/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
import java.util.Iterator;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.helpers.collection.PrefetchingIterator;

import static org.neo4j.helpers.collection.Iterators.emptyIterator;

/**
 * Iterator over Lucene index files for a particular {@link IndexCommit snapshot}.
 * Applicable only to a single Lucene index partition.
 * Internally uses {@link SnapshotDeletionPolicy#snapshot()} to create an {@link IndexCommit} that represents
 * consistent state of the index for a particular point in time.
 *
 * Turns to be an empty iterator if index does not have any commits yet.
 */
public class LuceneIndexSnapshotFileIterator extends PrefetchingIterator<File> implements ResourceIterator<File>
{
    private final File indexDirectory;
    private final SnapshotDeletionPolicy snapshotDeletionPolicy;
    private final Iterator<String> fileNames;
    private final IndexCommit snapshot;

    public static ResourceIterator<File> forIndex( File indexFolder, IndexWriter indexWriter ) throws IOException
    {
        IndexDeletionPolicy deletionPolicy = indexWriter.getConfig().getIndexDeletionPolicy();
        if ( deletionPolicy instanceof SnapshotDeletionPolicy )
        {
            SnapshotDeletionPolicy policy = (SnapshotDeletionPolicy) deletionPolicy;
            return hasCommits( indexWriter )
                   ? new LuceneIndexSnapshotFileIterator( indexFolder, policy )
                   : emptyIterator();
        }
        else
        {
            throw new UnsupportedIndexDeletionPolicy( "Can't perform index snapshot with specified index deleiton " +
                                                      "policy: " + deletionPolicy.getClass().getName() + ". " +
                                                      "Only " + SnapshotDeletionPolicy.class.getName() + " is " +
                                                      "supported" );
        }
    }

    private LuceneIndexSnapshotFileIterator( File indexDirectory, SnapshotDeletionPolicy snapshotDeletionPolicy )
            throws IOException
    {
        this.snapshot = snapshotDeletionPolicy.snapshot();
        this.indexDirectory = indexDirectory;
        this.snapshotDeletionPolicy = snapshotDeletionPolicy;
        this.fileNames = snapshot.getFileNames().iterator();
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
        try
        {
            snapshotDeletionPolicy.release( snapshot );
        }
        catch ( IOException e )
        {
            throw new SnapshotReleaseException( "Unable to release lucene index snapshot for index in: " +
                                                indexDirectory, e );
        }
    }

    private static boolean hasCommits( IndexWriter indexWriter ) throws IOException
    {
        Directory directory = indexWriter.getDirectory();
        return DirectoryReader.indexExists( directory ) && SegmentInfos.readLatestCommit( directory ) != null;
    }

}
