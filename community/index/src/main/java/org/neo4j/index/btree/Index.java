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
package org.neo4j.index.btree;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;

import org.neo4j.index.IdProvider;
import org.neo4j.index.SCIndex;
import org.neo4j.index.SCIndexDescription;
import org.neo4j.index.SCInserter;
import org.neo4j.index.SCResult;
import org.neo4j.index.Seeker;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;

public class Index implements SCIndex, IdProvider
{
    private final File metaFile;
    private final int pageSize;

    private final PagedFile pagedFile;
    private final SCIndexDescription description;
    private long rootId;
    private long lastId;
    private final IndexInsert inserter;
    private final BTreeNode bTreeNode;

    /**
     * Initiate an already existing index from file and meta file
     * @param pageCache     {@link PageCache} to use to map index file
     * @param indexFile     {@link File} containing the actual index
     * @param metaFile      {@link File} containing the meta data about the index
     * @throws IOException on page cache error
     */
    public Index( PageCache pageCache, File indexFile, File metaFile ) throws IOException
    {
        this.metaFile = metaFile;
        SCMetaData meta = SCMetaData.parseMeta( metaFile );

        this.pageSize = meta.pageSize;
        this.pagedFile = pageCache.map( indexFile, pageSize );
        this.description = meta.description;
        this.rootId = meta.rootId;
        this.lastId = meta.lastId;
        this.bTreeNode = new BTreeNode( meta.pageSize );
        this.inserter = new IndexInsert( this, bTreeNode );
    }

    /**
     * Initiate a completely new index
     * @param pageCache     {@link PageCache} to use to map index file
     * @param indexFile     {@link File} containing the actual index
     * @param metaFile      {@link File} conaining the meta data about the index
     * @param description   {@link SCIndexDescription} description of the index
     * @param pageSize      page size to use for index
     * @throws IOException on page cache error
     */
    public Index( PageCache pageCache, File indexFile, File metaFile, SCIndexDescription description, int pageSize )
            throws IOException
    {
        this.metaFile = metaFile;
        this.pageSize = pageSize;
        this.pagedFile = pageCache.map( indexFile, pageSize, StandardOpenOption.CREATE );
        this.description = description;
        this.lastId = 0;
        this.rootId = this.lastId;
        this.bTreeNode = new BTreeNode( pageSize );
        this.inserter = new IndexInsert( this, bTreeNode );

        SCMetaData.writeMetaData( metaFile, description, pageSize, rootId, lastId );

        // Initialize index root node to a leaf node.
        try ( PageCursor cursor = pagedFile.io( rootId, PagedFile.PF_SHARED_WRITE_LOCK ) )
        {
            cursor.next();
            bTreeNode.initializeLeaf( cursor );
        }
    }

    @Override
    public SCIndexDescription getDescription()
    {
        return description;
    }

    @Override
    public void insert( long[] key, long[] value ) throws IOException
    {
        try ( PageCursor cursor = pagedFile.io( rootId, PagedFile.PF_SHARED_WRITE_LOCK ) )
        {
            insert0( cursor, key, value );
        }
    }

    private void insert0( PageCursor cursor, long[] key, long[] value ) throws IOException
    {
        cursor.next( rootId );

        SplitResult split = inserter.insert( cursor, key, value );

        if ( split != null )
        {
            // New root
            rootId = acquireNewId();
            cursor.next( rootId );

            bTreeNode.initializeInternal( cursor );
            bTreeNode.setKeyAt( cursor, split.primKey, 0 );
            bTreeNode.setKeyCount( cursor, 1 );
            bTreeNode.setChildAt( cursor, split.left, 0 );
            bTreeNode.setChildAt( cursor, split.right, 1 );
//                if ( metaFile != null )
//                {
//                    SCMetaData.writeMetaData( metaFile, description, pageSize, rootId, lastId );
//                }
        }
    }

    @Override
    public void seek( Seeker seeker, List<SCResult> resultList ) throws IOException
    {
        try ( PageCursor cursor = pagedFile.io( rootId, PagedFile.PF_SHARED_WRITE_LOCK ) )
        {
            cursor.next();

            seeker.seek( cursor, bTreeNode, resultList );
        }
    }

    @Override
    public long acquireNewId() throws FileNotFoundException
    {
        lastId++;
//        if ( metaFile != null )
//        {
//            SCMetaData.writeMetaData( metaFile, description, pageSize, rootId, lastId );
//        }
        return lastId;
    }

    // Utility method
    public void printTree() throws IOException
    {
        try ( PageCursor cursor = pagedFile.io( rootId, PagedFile.PF_SHARED_READ_LOCK ) )
        {
            cursor.next();

            int level = 0;
            long id;
            while ( bTreeNode.isInternal( cursor ) )
            {
                System.out.println( "Level " + level++ );
                id = cursor.getCurrentPageId();
                printKeysOfSiblings( cursor, bTreeNode );
                System.out.println();
                cursor.next( id );
                cursor.next( bTreeNode.childAt( cursor, 0 ) );
            }

            System.out.println( "Level " + level );
            printKeysOfSiblings( cursor, bTreeNode );
            System.out.println();
        }
    }

    /**
     * Closes this stream and releases any system resources associated
     * with it. If the stream is already closed then invoking this
     * method has no effect.
     * <p>
     * <p> As noted in {@link AutoCloseable#close()}, cases where the
     * close may fail require careful attention. It is strongly advised
     * to relinquish the underlying resources and to internally
     * <em>mark</em> the {@code Closeable} as closed, prior to throwing
     * the {@code IOException}.
     *
     * @throws java.io.IOException if an I/O error occurs
     */
    @Override
    public void close() throws IOException
    {
        pagedFile.close();
    }

    @Override
    public SCInserter inserter() throws IOException
    {
        final PageCursor cursor = pagedFile.io( rootId, PagedFile.PF_SHARED_WRITE_LOCK );
        return new SCInserter()
        {
            @Override
            public void insert( long[] key, long[] value ) throws IOException
            {
                insert0( cursor, key, value );
            }

            @Override
            public void close() throws IOException
            {
                cursor.close();
            }
        };
    }

    // Utility method
    protected static void printKeysOfSiblings( PageCursor cursor, BTreeNode bTreeNode ) throws IOException
    {
        while ( true )
        {
            printKeys( cursor, bTreeNode );
            long rightSibling = bTreeNode.rightSibling( cursor );
            if ( rightSibling == BTreeNode.NO_NODE_FLAG )
            {
                break;
            }
            cursor.next( rightSibling );
        }
    }

    // Utility method
    protected static void printKeys( PageCursor cursor, BTreeNode BTreeNode )
    {
        int keyCount = BTreeNode.keyCount( cursor );
        System.out.print( "|" );
        for ( int i = 0; i < keyCount; i++ )
        {
            System.out.print( Arrays.toString( BTreeNode.keyAt( cursor, i ) ) + " " );
        }
        System.out.print( "|" );
    }
}
