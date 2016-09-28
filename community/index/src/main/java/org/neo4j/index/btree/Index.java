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
import java.nio.file.NoSuchFileException;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.cursor.Cursor;
import org.neo4j.index.BTreeHit;
import org.neo4j.index.IdProvider;
import org.neo4j.index.SCIndex;
import org.neo4j.index.SCInserter;
import org.neo4j.index.ValueAmender;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;

public class Index<KEY,VALUE> implements SCIndex<KEY,VALUE>, IdProvider
{
    private static final int META_PAGE_ID = 0;

    private final PagedFile pagedFile;
    private final TreeItemLayout<KEY,VALUE> layout;
    private final TreeNode<KEY,VALUE> bTreeNode;
    private PageCursor metaCursor;
    // currently an index only supports one concurrent updater and so this reference will act both as
    // guard so that only one thread can have it at any given time and also as synchronization between threads
    // wanting it
    private final AtomicReference<TheInserter> inserter;

    // when updating these the meta page will also be kept up2date
    private int pageSize;
    private long rootId = META_PAGE_ID + 1;
    private long lastId = rootId;
    private boolean created;

    /**
     * Opens an index {@code indexFile} in the {@code pageCache}, creating and initializing it if it doesn't exist.
     * If the index doesn't exist it will be created and the {@link TreeItemLayout} and {@code pageSize} will
     * be written in index header.
     * If the index exists it will be opened and the {@link TreeItemLayout} will be matched with the information
     * in the header. At the very least {@link TreeItemLayout#identifier()} will be matched, but also if the
     * index has {@link TreeItemLayout#writeMetaData(PageCursor)} additional meta data it will be
     * {@link TreeItemLayout#readMetaData(PageCursor)}.
     *
     * @param pageCache {@link PageCache} to use to map index file
     * @param indexFile {@link File} containing the actual index
     * @param tentativePageSize page size, i.e. tree node size. Must be less than or equal to that of the page cache.
     * A pageSize of 0 means to use whatever the page cache has (at creation)
     * @param layout {@link TreeItemLayout} to use in the tree, this must match the existing layout
     * we're just opening the index
     * @throws IOException on page cache error
     */
    public Index( PageCache pageCache, File indexFile, TreeItemLayout<KEY,VALUE> layout, int tentativePageSize )
            throws IOException
    {
        this.layout = layout;
        this.pagedFile = openOrCreate( pageCache, indexFile, tentativePageSize, layout );
        this.bTreeNode = new BTreeNode<>( pageSize, layout );
        this.inserter = new AtomicReference<>( new TheInserter( new IndexInsert<>( this, bTreeNode, layout ) ) );

        if ( created )
        {
            // Initialize index root node to a leaf node.
            try ( PageCursor cursor = pagedFile.io( rootId, PagedFile.PF_SHARED_WRITE_LOCK ) )
            {
                cursor.next();
                bTreeNode.initializeLeaf( cursor );
            }
        }
    }

    private PagedFile openOrCreate( PageCache pageCache, File indexFile,
            int pageSizeForCreation, TreeItemLayout<KEY,VALUE> layout ) throws IOException
    {
        try
        {
            PagedFile pagedFile = pageCache.map( indexFile, pageCache.pageSize() );
            // This index already exists, verify the header with what we got passed into the constructor this time

            try
            {
                // Read header
                openMetaPageCursor( pagedFile ); // and keep open for later frequent updates when splitting and allocating
                long layoutIdentifier;
                do
                {
                    metaCursor.setOffset( 0 );
                    pageSize = metaCursor.getInt();
                    rootId = metaCursor.getLong();
                    lastId = metaCursor.getLong();
                    layoutIdentifier = metaCursor.getLong();
                    layout.readMetaData( metaCursor );
                }
                while ( metaCursor.shouldRetry() );
                if ( layoutIdentifier != layout.identifier() )
                {
                    throw new IllegalArgumentException( "Tried to open " + indexFile + " using different layout "
                            + layout.identifier() + " than what it was created with " + layoutIdentifier );
                }
                // This index was created with another page size, re-open with that actual page size
                if ( pageSize != pageCache.pageSize() )
                {
                    if ( pageSize > pageCache.pageSize() )
                    {
                        throw new IllegalStateException( "Index was created with page size:" + pageSize
                                + ", but page cache used to open it this time has a smaller page size:"
                                + pageCache.pageSize() + " so cannot be opened" );
                    }
                    pagedFile.close();
                    pagedFile = pageCache.map( indexFile, pageSize );
                }
                return pagedFile;
            }
            catch ( Throwable t )
            {
                try
                {
                    closeMetaCursor();
                    pagedFile.close();
                }
                catch ( IOException e )
                {
                    t.addSuppressed( e );
                }
                throw t;
            }
        }
        catch ( NoSuchFileException e )
        {
            pageSizeForCreation = pageSizeForCreation == 0 ? pageCache.pageSize() : pageSizeForCreation;
            if ( pageSizeForCreation > pageCache.pageSize() )
            {
                throw new IllegalStateException( "Index was about to be created with page size:" + pageSizeForCreation +
                        ", but page cache used to create it has a smaller page size:" +
                        pageCache.pageSize() + " so cannot be created" );
            }

            // We need to create this index
            PagedFile pagedFile = pageCache.map( indexFile, pageSizeForCreation, StandardOpenOption.CREATE );

            // Write header
            openMetaPageCursor( pagedFile ); // and keep open for later frequent updates when splitting and allocating
            metaCursor.setOffset( 0 );
            metaCursor.putInt( pageSizeForCreation );
            metaCursor.putLong( rootId );
            metaCursor.putLong( lastId );
            metaCursor.putLong( layout.identifier() );
            layout.writeMetaData( metaCursor );
            created = true;
            pageSize = pageSizeForCreation;
            return pagedFile;
        }
    }

    private void openMetaPageCursor( PagedFile pagedFile ) throws IOException
    {
        metaCursor = pagedFile.io( META_PAGE_ID, PagedFile.PF_SHARED_WRITE_LOCK );
        if ( !metaCursor.next() )
        {
            throw new IllegalStateException( "Couldn't go to meta data page " + META_PAGE_ID );
        }
    }

    private void updateIdsInMetaPage()
    {
        metaCursor.putLong( 4, rootId );
        metaCursor.putLong( 12, lastId );
    }

    @Override
    public Cursor<BTreeHit<KEY,VALUE>> seek( KEY fromInclusive, KEY toExclusive ) throws IOException
    {
        PageCursor cursor = pagedFile.io( rootId, PagedFile.PF_SHARED_READ_LOCK );
        KEY key = bTreeNode.newKey();
        VALUE value = bTreeNode.newValue();
        cursor.next();

        // Find the left-most in-range leaf node, i.e. iterate through internal nodes to find it
        while ( bTreeNode.isInternal( cursor ) )
        {
            // COPY(1)
            // Find the left-most key within from-range
            int keyCount = bTreeNode.keyCount( cursor );
            int pos = 0;
            bTreeNode.keyAt( cursor, key, pos );
            while ( pos < keyCount && layout.compare( key, fromInclusive ) < 0 )
            {
                pos++;
                bTreeNode.keyAt( cursor, key, pos );
            }

            cursor.next( bTreeNode.childAt( cursor, pos ) );
        }

        // Returns cursor which is now initiated with left-most leaf node for the specified range
        return new Cursor<BTreeHit<KEY,VALUE>>()
        {
            private final MutableBTreeHit<KEY,VALUE> hit = new MutableBTreeHit<>( key, value );

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
                while ( pos < keyCount && layout.compare( key, fromInclusive ) < 0 )
                {
                    pos++;
                    bTreeNode.keyAt( cursor, key, pos );
                }
                pos--;
            }

            @Override
            public BTreeHit<KEY,VALUE> get()
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
                        if ( bTreeNode.isNode( rightSibling ) )
                        {
                            gotoTreeNode( rightSibling );
                            continue;
                        }
                        return false;
                    }

                    // Go to the next one, so that next call to next() gets it
                    bTreeNode.keyAt( cursor, key, pos );
                    if ( layout.compare( key, toExclusive ) < 0 )
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

    @Override
    public void close() throws IOException
    {
        closeMetaCursor();
        pagedFile.close();
    }

    private void closeMetaCursor()
    {
        if ( metaCursor != null )
        {
            metaCursor.close();
            metaCursor = null;
        }
    }

    @Override
    public SCInserter<KEY,VALUE> inserter() throws IOException
    {
        TheInserter result = this.inserter.getAndSet( null );
        if ( result == null )
        {
            throw new IllegalStateException( "Only supports one concurrent writer" );
        }
        return result.take( rootId );
    }

    // Utility method
    protected static <KEY,VALUE> void printKeysOfSiblings( PageCursor cursor,
            TreeNode<KEY,VALUE> bTreeNode ) throws IOException
    {
        while ( true )
        {
            printKeys( cursor, bTreeNode );
            long rightSibling = bTreeNode.rightSibling( cursor );
            if ( !bTreeNode.isNode( rightSibling ) )
            {
                break;
            }
            cursor.next( rightSibling );
        }
    }

    // Utility method
    protected static <KEY,VALUE> void printKeys( PageCursor cursor, TreeNode<KEY,VALUE> bTreeNode )
    {
        boolean isLeaf = bTreeNode.isLeaf( cursor );
        int keyCount = bTreeNode.keyCount( cursor );
        System.out.print( (isLeaf ? "[" : "|") + "{" + cursor.getCurrentPageId() + "}" );
        KEY key = bTreeNode.newKey();
        VALUE value = bTreeNode.newValue();
        for ( int i = 0; i < keyCount; i++ )
        {
            if ( i > 0 )
            {
                System.out.print( "," );
            }

            if ( isLeaf )
            {
                System.out.print( "#" + i + ":" +
                        bTreeNode.keyAt( cursor, key, i ) + "=" +
                        bTreeNode.valueAt( cursor, value, i ) );
            }
            else
            {
                System.out.print( "#" + i + ":" +
                        "|" + bTreeNode.childAt( cursor, i ) + "|" +
                        bTreeNode.keyAt( cursor, key, i ) + "|" );

            }
        }
        if ( !isLeaf )
        {
            System.out.println( "#" + keyCount + ":|" + bTreeNode.childAt( cursor, keyCount ) + "|" );
        }
        System.out.println( (isLeaf ? "]" : "|") );
    }

    class TheInserter implements SCInserter<KEY,VALUE>
    {
        // Well, IndexInsert code lives somewhere so we need to instantiate that bastard here as well
        private final IndexInsert<KEY,VALUE> inserter;
        private PageCursor cursor;

        TheInserter( IndexInsert<KEY,VALUE> inserter )
        {
            this.inserter = inserter;
        }

        TheInserter take( long rootId ) throws IOException
        {
            cursor = pagedFile.io( rootId, PagedFile.PF_SHARED_WRITE_LOCK );
            return this;
        }

        @Override
        public void insert( KEY key, VALUE value, ValueAmender<VALUE> ammender ) throws IOException
        {
            cursor.next( rootId );

            SplitResult<KEY> split = inserter.insert( cursor, key, value, ammender );

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

        @Override
        public VALUE remove( KEY key ) throws IOException
        {
            cursor.next( rootId );

            return inserter.remove( cursor, key );
        }

        @Override
        public void close() throws IOException
        {
            try
            {
                cursor.close();
            }
            finally
            {
                if ( !Index.this.inserter.compareAndSet( null, this ) )
                {
                    throw new IllegalStateException( "Tried to give back the inserter, but somebody else already did" );
                }
            }
        }
    }
}
