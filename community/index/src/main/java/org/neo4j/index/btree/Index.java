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

import org.neo4j.cursor.Cursors;
import org.neo4j.cursor.RawCursor;
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
    private final PageCursor metaCursor;
    // currently an index only supports one concurrent updater and so this reference will act both as
    // guard so that only one thread can have it at any given time and also as synchronization between threads
    // wanting it
    private final AtomicReference<Inserter> inserter;

    // when updating these the meta page will also be kept up2date
    private int pageSize;
    private volatile long rootId = META_PAGE_ID + 1;
    private volatile long lastId = rootId;
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
        this.inserter = new AtomicReference<>( new Inserter( new IndexInsert<>( this, bTreeNode, layout ) ) );

        if ( created )
        {
            // Initialize index root node to a leaf node.
            try ( PageCursor cursor = pagedFile.io( rootId, PagedFile.PF_SHARED_WRITE_LOCK ) )
            {
                cursor.next();
                bTreeNode.initializeLeaf( cursor );
            }
        }
        this.metaCursor = openMetaPageCursor( pagedFile );
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
                long layoutIdentifier;
                int majorVersion;
                int minorVersion;
                try ( PageCursor metaCursor = openMetaPageCursor( pagedFile ) )
                {
                    do
                    {
                        pageSize = metaCursor.getInt();
                        rootId = metaCursor.getLong();
                        lastId = metaCursor.getLong();
                        layoutIdentifier = metaCursor.getLong();
                        majorVersion = metaCursor.getInt();
                        minorVersion = metaCursor.getInt();
                        layout.readMetaData( metaCursor );
                    }
                    while ( metaCursor.shouldRetry() );
                }
                if ( layoutIdentifier != layout.identifier() )
                {
                    throw new IllegalArgumentException( "Tried to open " + indexFile + " using different layout "
                            + layout.identifier() + " than what it was created with " + layoutIdentifier );
                }
                if ( majorVersion != layout.majorVersion() || minorVersion != layout.minorVersion() )
                {
                    throw new IllegalArgumentException( "Index is of another version than the layout " +
                            "it tries to be opened with. Index version is [" + majorVersion + "." + minorVersion + "]" +
                            ", but tried to load the index with version [" +
                            layout.majorVersion() + "." + layout.minorVersion() + "]" );
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
            try ( PageCursor metaCursor = openMetaPageCursor( pagedFile ) )
            {
                metaCursor.putInt( pageSizeForCreation );
                metaCursor.putLong( rootId );
                metaCursor.putLong( lastId );
                metaCursor.putLong( layout.identifier() );
                metaCursor.putInt( layout.majorVersion() );
                metaCursor.putInt( layout.minorVersion() );
                layout.writeMetaData( metaCursor );
            }
            pagedFile.flushAndForce();
            created = true;
            pageSize = pageSizeForCreation;
            return pagedFile;
        }
    }

    private PageCursor openMetaPageCursor( PagedFile pagedFile ) throws IOException
    {
        PageCursor metaCursor = pagedFile.io( META_PAGE_ID, PagedFile.PF_SHARED_WRITE_LOCK );
        if ( !metaCursor.next() )
        {
            throw new IllegalStateException( "Couldn't go to meta data page " + META_PAGE_ID );
        }
        return metaCursor;
    }

    @Override
    public RawCursor<BTreeHit<KEY,VALUE>,IOException> seek( KEY fromInclusive, KEY toExclusive ) throws IOException
    {
        PageCursor cursor = pagedFile.io( rootId, PagedFile.PF_SHARED_READ_LOCK );
        KEY key = bTreeNode.newKey();
        VALUE value = bTreeNode.newValue();
        cursor.next();

        boolean isInternal;
        int keyCount = 0; // initialized to satisfy compiler
        long childId = 0; // initialized to satisfy compiler
        int pos;
        do
        {
            do
            {
                isInternal = bTreeNode.isInternal( cursor );
                // Find the left-most key within from-range
                keyCount = bTreeNode.keyCount( cursor );
                pos = 0;
                bTreeNode.keyAt( cursor, key, pos );
                while ( pos < keyCount && layout.compare( key, fromInclusive ) < 0 )
                {
                    pos++;
                    bTreeNode.keyAt( cursor, key, pos );
                }
                if ( isInternal )
                {
                    childId = bTreeNode.childAt( cursor, pos );
                }
            }
            while ( cursor.shouldRetry() );
            cursor.checkAndClearCursorException();
            if ( cursor.checkAndClearBoundsFlag() )
            {
                // Something's wrong, get a new fresh rootId and go from the top again
                cursor.next( rootId );
                continue;
            }
            if ( isInternal )
            {
                if ( !cursor.next( childId ) )
                {
                    throw new IllegalStateException( "Couldn't go to child " + childId );
                }
            }
        }
        while ( isInternal && keyCount > 0 );

        if ( keyCount == 0 )
        {
            // This index seems to be empty
            return Cursors.emptyRawCursor();
        }

        // Returns cursor which is now initiated with left-most leaf node for the specified range
        return new SeekCursor<>( cursor, key, value, bTreeNode, fromInclusive, toExclusive, layout, pos - 1 );
    }

    @Override
    public long acquireNewId() throws FileNotFoundException
    {
        lastId++;
        metaCursor.putLong( 12, lastId );
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
        metaCursor.close();
        pagedFile.close();
    }

    @Override
    public SCInserter<KEY,VALUE> inserter() throws IOException
    {
        Inserter result = this.inserter.getAndSet( null );
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

    class Inserter implements SCInserter<KEY,VALUE>
    {
        // Well, IndexInsert code lives somewhere so we need to instantiate that bastard here as well
        private final IndexInsert<KEY,VALUE> inserter;
        private PageCursor cursor;

        Inserter( IndexInsert<KEY,VALUE> inserter )
        {
            this.inserter = inserter;
        }

        Inserter take( long rootId ) throws IOException
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
                long newRootId = acquireNewId();
                cursor.next( newRootId );

                bTreeNode.initializeInternal( cursor );
                bTreeNode.setKeyAt( cursor, split.primKey, 0 );
                bTreeNode.setKeyCount( cursor, 1 );
                bTreeNode.setChildAt( cursor, split.left, 0 );
                bTreeNode.setChildAt( cursor, split.right, 1 );
                metaCursor.putLong( 4, newRootId );
                rootId = newRootId;
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
