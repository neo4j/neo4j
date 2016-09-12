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
import java.nio.charset.Charset;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;

import org.neo4j.cursor.Cursor;
import org.neo4j.graphdb.Direction;
import org.neo4j.index.BTreeHit;
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
    private static final Charset UTF_8 = Charset.forName( "UTF-8" );

    private static final int META_PAGE_ID = 0;

    private final PagedFile pagedFile;
    private final SCIndexDescription description;

    // When updating these the meta page will also be kept up2date
    private long rootId;
    private long lastId = META_PAGE_ID + 1; // first page (page 0) is for meta data (even actual page size)

    private final IndexInsert inserter;
    private final BTreeNode bTreeNode;
    private final PageCursor metaCursor;

    /**
     * Initiate an already existing index from file and meta file
     * @param pageCache     {@link PageCache} to use to map index file
     * @param indexFile     {@link File} containing the actual index
     * @throws IOException on page cache error
     */
    public Index( PageCache pageCache, File indexFile ) throws IOException
    {
        PagedFile pagedFile = pageCache.map( indexFile, pageCache.pageSize() );
        SCMetaData meta = readMetaData( pagedFile );

        if ( meta.pageSize != pageCache.pageSize() )
        {
            pagedFile.close();
            pagedFile = pageCache.map( indexFile, meta.pageSize );
        }
        this.pagedFile = pagedFile;
        this.description = meta.description;
        this.rootId = meta.rootId;
        this.lastId = meta.lastId;
        this.bTreeNode = new BTreeNode( meta.pageSize );
        this.inserter = new IndexInsert( this, bTreeNode );
        this.metaCursor = openMetaPageCursor();
    }

    /**
     * Initiate a completely new index
     * @param pageCache     {@link PageCache} to use to map index file
     * @param indexFile     {@link File} containing the actual index
     * @param description   {@link SCIndexDescription} description of the index
     * @param pageSize      page size to use for index
     * @throws IOException on page cache error
     */
    public Index( PageCache pageCache, File indexFile, SCIndexDescription description, int pageSize )
            throws IOException
    {
        this.pagedFile = pageCache.map( indexFile, pageSize, StandardOpenOption.CREATE );
        this.description = description;
        this.rootId = this.lastId;
        this.bTreeNode = new BTreeNode( pageSize );
        this.inserter = new IndexInsert( this, bTreeNode );

        writeMetaData( pagedFile, new SCMetaData( description, pageSize, rootId, lastId ) );

        // Initialize index root node to a leaf node.
        try ( PageCursor cursor = pagedFile.io( rootId, PagedFile.PF_SHARED_WRITE_LOCK ) )
        {
            cursor.next();
            bTreeNode.initializeLeaf( cursor );
        }
        this.metaCursor = openMetaPageCursor();
    }

    private PageCursor openMetaPageCursor() throws IOException
    {
        PageCursor cursor = pagedFile.io( META_PAGE_ID, PagedFile.PF_SHARED_WRITE_LOCK );
        cursor.next();
        return cursor;
    }

    private static void writeMetaData( PagedFile pagedFile, SCMetaData metaData ) throws IOException
    {
        try ( PageCursor cursor = pagedFile.io( META_PAGE_ID, PagedFile.PF_SHARED_WRITE_LOCK ) )
        {
            cursor.next();
            cursor.putInt( metaData.pageSize );
            cursor.putLong( metaData.rootId );
            cursor.putLong( metaData.lastId );
            writeString( cursor, metaData.description.firstLabel );
            writeString( cursor, metaData.description.secondLabel );
            writeString( cursor, metaData.description.relationshipType );
            cursor.putByte( (byte) metaData.description.direction.ordinal() );
            writeString( cursor, metaData.description.relationshipPropertyKey );
            writeString( cursor, metaData.description.nodePropertyKey );
        }
    }

    private static void writeString( PageCursor cursor, String string )
    {
        if ( string == null )
        {
            cursor.putInt( -1 );
        }
        else
        {
            byte[] bytes = string.getBytes( UTF_8 );
            cursor.putInt( string.length() );
            cursor.putBytes( bytes );
        }
    }

    private static SCMetaData readMetaData( PagedFile pagedFile ) throws IOException
    {
        try ( PageCursor cursor = pagedFile.io( META_PAGE_ID, PagedFile.PF_SHARED_READ_LOCK ) )
        {
            cursor.next();
            int pageSize = cursor.getInt();
            long rootId = cursor.getLong();
            long lastId = cursor.getLong();
            SCIndexDescription description = new SCIndexDescription(
                    readString( cursor ), // firstLabel
                    readString( cursor ), // secondLabel
                    readString( cursor ), // relationshipType
                    Direction.values()[cursor.getByte()],
                    readString( cursor ), // nodePropertyKey
                    readString( cursor ) ); // relationshipPropertyKey
            return new SCMetaData( description, pageSize, rootId, lastId );
        }
    }

    private static String readString( PageCursor cursor )
    {
        int length = cursor.getInt();
        if ( length == -1 )
        {
            return null;
        }

        byte[] bytes = new byte[length];
        cursor.getBytes( bytes );
        return new String( bytes, UTF_8 );
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
            updateIdsInMetaPage();
        }
    }

    private void updateIdsInMetaPage()
    {
        metaCursor.putLong( 4, rootId );
        metaCursor.putLong( 12, lastId );
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
    public Cursor<BTreeHit> seek( RangePredicate fromPred, RangePredicate toPred ) throws IOException
    {
        PageCursor cursor = pagedFile.io( rootId, PagedFile.PF_SHARED_READ_LOCK );
        long[] key = new long[2], value = new long[2];
        cursor.next();

        // Find the left-most in-range leaf node, i.e. iterate through internal nodes to find it
        while ( bTreeNode.isInternal( cursor ) )
        {
            // COPY(1)
            // Find the left-most key within from-range
            int keyCount = bTreeNode.keyCount( cursor );
            int pos = 0;
            bTreeNode.keyAt( cursor, key, pos );
            while ( pos < keyCount && fromPred.inRange( key ) < 0 )
            {
                pos++;
                bTreeNode.keyAt( cursor, key, pos );
            }

            cursor.next( bTreeNode.childAt( cursor, pos ) );
        }

        // Returns cursor which is now initiated with left-most leaf node for the specified range
        return new Cursor<BTreeHit>()
        {
            private final MutableBTreeHit hit = new MutableBTreeHit( key, value );

            // data structures for the current b-tree node
            private int keyCount;
            private int pos;

            {
                initTreeNode();
            }

            private void gotoTreeNode( long id )
            {
                try
                {
                    cursor.next( id );
                }
                catch ( IOException e )
                {
                    throw new RuntimeException( e );
                }
                cursor.setOffset( 0 );
                initTreeNode();
            }

            private void initTreeNode()
            {
                // COPY(1)
                // Find the left-most key within from-range
                keyCount = bTreeNode.keyCount( cursor );
                // TODO only do this for first leaf
                pos = 0;
                bTreeNode.keyAt( cursor, key, pos );
                while ( pos < keyCount && fromPred.inRange( key ) < 0 )
                {
                    pos++;
                    bTreeNode.keyAt( cursor, key, pos );
                }
                pos--;
            }

            @Override
            public BTreeHit get()
            {
                return hit;
            }

            @Override
            public boolean next()
            {
                while ( true )
                {
                    pos++;
                    if ( pos >= keyCount )
                    {
                        long rightSibling = bTreeNode.rightSibling( cursor );
                        if ( rightSibling != BTreeNode.NO_NODE_FLAG )
                        {
                            gotoTreeNode( rightSibling );
                            continue;
                        }
                        return false;
                    }

                    // Go to the next one, so that next call to next() gets it
                    bTreeNode.keyAt( cursor, key, pos );
                    if ( toPred.inRange( key ) <= 0 )
                    {
                        // A hit
                        bTreeNode.valueAt( cursor, value, pos );
                        return true;
                    }
                    return false;
                }
            }

            @Override
            public void close()
            {
                cursor.close();
            }
        };
    }

    @Override
    public long acquireNewId() throws FileNotFoundException
    {
        lastId++;
        updateIdsInMetaPage();
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
        metaCursor.close();
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
    protected static void printKeys( PageCursor cursor, BTreeNode bTreeNode )
    {
        int keyCount = bTreeNode.keyCount( cursor );
        System.out.print( "|" );
        long[] key = new long[2];
        for ( int i = 0; i < keyCount; i++ )
        {
            System.out.print( Arrays.toString( bTreeNode.keyAt( cursor, key, i ) ) + " " );
        }
        System.out.print( "|" );
    }
}
