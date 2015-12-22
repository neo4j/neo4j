/*
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
package org.neo4j.kernel.api.impl.index;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexDeletionPolicy;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.store.Directory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import static java.util.Collections.singletonMap;

/**
 * A thin wrapper around {@link org.apache.lucene.index.IndexWriter} that exposes only some part of it's
 * functionality that it really needed.
 */
public class LuceneIndexWriter implements Closeable
{
    // Lucene cannot allocate a full MAX_INT of documents, the deviation differs from JVM to JVM, but according to
    // their source in future versions, the deviation can never be bigger than 128.
    private static final long MAX_DOC_LIMIT = Integer.MAX_VALUE - 128;

    private static final String KEY_STATUS = "status";
    private static final String ONLINE = "online";
    private static final Map<String,String> ONLINE_COMMIT_USER_DATA = singletonMap( KEY_STATUS, ONLINE );

    protected final IndexWriter writer;

    private final ReentrantLock commitCloseLock = new ReentrantLock();

    /**
     * Package private *only* for subclasses and testing.
     */
    LuceneIndexWriter( Directory dir, IndexWriterConfig conf ) throws IOException
    {
        this.writer = new IndexWriter( dir, conf );
    }

    public static boolean isOnline( Directory directory ) throws IOException
    {
        if ( !DirectoryReader.indexExists( directory ) )
        {
            return false;
        }

        try ( DirectoryReader reader = DirectoryReader.open( directory ) )
        {
            Map<String,String> userData = reader.getIndexCommit().getUserData();
            return ONLINE.equals( userData.get( KEY_STATUS ) );
        }
    }

    public void addDocument( Document document ) throws IOException
    {
        writer.addDocument( document );
    }

    public void updateDocument( Term term, Document document ) throws IOException
    {
        writer.updateDocument( term, document );
    }

    public void deleteDocuments( Term term ) throws IOException
    {
        writer.deleteDocuments( term );
    }

    public void deleteDocuments( Query query ) throws IOException
    {
        writer.deleteDocuments( query );
    }

    public void optimize() throws IOException
    {
        writer.forceMerge( 1, true );
    }

    public SearcherManager createSearcherManager() throws IOException
    {
        return new SearcherManager( writer, true, new SearcherFactory() );
    }

    public void commit() throws IOException
    {
        commitCloseLock.lock();
        try
        {
            writer.commit();
        }
        finally
        {
            commitCloseLock.unlock();
        }
    }

    public void commitAsOnline() throws IOException
    {
        commitCloseLock.lock();
        try
        {
            writer.setCommitData( ONLINE_COMMIT_USER_DATA );
            writer.commit();
        }
        finally
        {
            commitCloseLock.unlock();
        }
    }

    @Override
    public void close() throws IOException
    {
        commitCloseLock.lock();
        try
        {
            writer.close();
        }
        finally
        {
            commitCloseLock.unlock();
        }
    }

    IndexDeletionPolicy getIndexDeletionPolicy()
    {
        return writer.getConfig().getIndexDeletionPolicy();
    }

    long maxDocLimit()
    {
        return MAX_DOC_LIMIT;
    }
}
