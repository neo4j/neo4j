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
package org.neo4j.index.gbptree;

import java.io.File;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.cursor.RawCursor;
import org.neo4j.index.Hit;
import org.neo4j.index.Index;
import org.neo4j.index.IndexWriter;
import org.neo4j.index.ValueMerger;
import org.neo4j.index.ValueMergers;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;

/**
 * A generation-aware B+tree (GB+Tree) implementation directly atop a {@link PageCache} with no caching in between.
 * Additionally internal and leaf nodes on same level are linked both left and right (sibling pointers),
 * this to provide correct reading when concurrently {@link #writer(IndexWriter.Options) modifying}
 * the tree.
 * <p>
 * Generation is incremented on {@link #flush() flushing a.k.a check-pointing}.
 * Generation awareness allows for recovery from last {@link #flush()}, provided the same updates will be
 * replayed onto the index since that point in time.
 * <p>
 * Changes to tree nodes are made so that stable nodes (i.e. nodes that have survived at least one flush)
 * are immutable w/ regards to keys values and child/sibling pointers.
 * Making a change in a stable node will copy the node to an unstable generation first and then make the change
 * in that unstable version. Further change in that node in the same generation will not require a copy since
 * it's already unstable.
 * <p>
 * Every pointer to another node (child/sibling pointer) consists of two pointers, one to a stable version and
 * one to a potentially unstable version. A stable -> unstable node copy will have its parent redirect one of its
 * two pointers to the new unstable version, redirecting readers and writers to the new unstable version,
 * while at the same time keeping one pointer to the stable version, in case there's a crash or non-clean
 * shutdown, followed by recovery.
 * <p>
 * Currently no leaves will be removed or merged as part of {@link IndexWriter#remove(Object) removals}.
 * <p>
 * A single writer w/ multiple concurrent readers is supported. Assuming usage adheres to this
 * constraint neither writer nor readers are blocking. Readers are virtually garbage-free.
 * <p>
 * An reader of GB+Tree is a {@link SeekCursor} that returns result as it finds them.
 * As the cursor move over keys/values, returned results are considered "behind" it
 * and likewise keys not yet returned "in front of".
 * Readers will always read latest written changes in front of it but will not see changes that appear behind.
 * The isolation level is thus read committed.
 * The tree have no knowledge about transactions and apply updates as isolated units of work one entry at the time.
 * Therefore, readers can see parts of transactions that are not fully applied yet.
 *
 * @param <KEY> type of keys
 * @param <VALUE> type of values
 */
public class GBPTree<KEY,VALUE> implements Index<KEY,VALUE>, IdProvider
{
    /**
     * Page id of the meta page holding information about root id and custom user meta information.
     * This page id is statically allocated throughout the life of a tree.
     */
    private static final int META_PAGE_ID = 0;

    /**
     * Paged file in a {@link PageCache} providing the means of storage.
     */
    private final PagedFile pagedFile;

    /**
     * User-provided layout of key/value as well as custom additional meta information.
     * This allows for custom key/value and comparison representation. The layout provided during index
     * creation, i.e. the first time constructor is called for the given paged file, will be stored
     * in the meta page and it's asserted that the same layout is passed to the constructor when opening the tree.
     */
    private final Layout<KEY,VALUE> layout;

    /**
     * Instance of {@link TreeNode} which handles reading/writing physical bytes from pages representing tree nodes.
     */
    private final TreeNode<KEY,VALUE> bTreeNode;

    /**
     * Currently an index only supports one concurrent writer and so this reference will act both as
     * guard so that only one thread can have it at any given time and also as synchronization between threads
     * wanting it.
     */
    private final AtomicReference<SingleIndexWriter> writer;

    /**
     * Page size, i.e. tree node size, of the tree nodes in this tree. The page size is determined on
     * tree creation, stored in meta page and read when opening tree later.
     */
    private int pageSize;

    /**
     * Whether or not the tree was created this time it was instantiated.
     */
    private boolean created;

    /**
     * Current page id which contains the root of the tree.
     */
    private volatile long rootId = META_PAGE_ID + 1;

    /**
     * Last allocated page id, used for allocating new ids as more data gets inserted into the tree.
     */
    private volatile long lastId = rootId;

    /**
     * Stable generation, i.e. generation which has survived the last {@link #flush()}.
     */
    private volatile int stableGeneration = 0;

    /**
     * Unstable generation, i.e. the current generation under evolution. This generation will be the
     * {@link #stableGeneration} in the next {@link #flush()}.
     */
    private volatile int unstableGeneration = 1;

    /**
     * Opens an index {@code indexFile} in the {@code pageCache}, creating and initializing it if it doesn't exist.
     * If the index doesn't exist it will be created and the {@link Layout} and {@code pageSize} will
     * be written in index header.
     * If the index exists it will be opened and the {@link Layout} will be matched with the information
     * in the header. At the very least {@link Layout#identifier()} will be matched, but also if the
     * index has {@link Layout#writeMetaData(PageCursor)} additional meta data it will be
     * {@link Layout#readMetaData(PageCursor)}.
     *
     * @param pageCache {@link PageCache} to use to map index file
     * @param indexFile {@link File} containing the actual index
     * @param tentativePageSize page size, i.e. tree node size. Must be less than or equal to that of the page cache.
     * A pageSize of {@code 0} means to use whatever the page cache has (at creation)
     * @param layout {@link Layout} to use in the tree, this must match the existing layout
     * we're just opening the index
     * @throws IOException on page cache error
     */
    public GBPTree( PageCache pageCache, File indexFile, Layout<KEY,VALUE> layout, int tentativePageSize )
            throws IOException
    {
        this.layout = layout;
        this.pagedFile = openOrCreate( pageCache, indexFile, tentativePageSize, layout );
        this.bTreeNode = new TreeNode<>( pageSize, layout );
        this.writer = new AtomicReference<>(
                new SingleIndexWriter( new InternalTreeLogic<>( this, bTreeNode, layout ) ) );

        if ( created )
        {
            // Initialize index root node to a leaf node.
            try ( PageCursor cursor = pagedFile.io( rootId, PagedFile.PF_SHARED_WRITE_LOCK ) )
            {
                cursor.next();
                bTreeNode.initializeLeaf( cursor, stableGeneration, unstableGeneration );
            }
        }
    }

    private PagedFile openOrCreate( PageCache pageCache, File indexFile,
            int pageSizeForCreation, Layout<KEY,VALUE> layout ) throws IOException
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
            pageSize = pageSizeForCreation == 0 ? pageCache.pageSize() : pageSizeForCreation;
            if ( pageSize > pageCache.pageSize() )
            {
                throw new IllegalStateException( "Index was about to be created with page size:" + pageSize +
                        ", but page cache used to create it has a smaller page size:" +
                        pageCache.pageSize() + " so cannot be created" );
            }

            // We need to create this index
            PagedFile pagedFile = pageCache.map( indexFile, pageSize, StandardOpenOption.CREATE );

            // Write header
            try ( PageCursor metaCursor = openMetaPageCursor( pagedFile ) )
            {
                metaCursor.putInt( pageSize );
                metaCursor.putLong( rootId );
                metaCursor.putLong( lastId );
                metaCursor.putLong( layout.identifier() );
                metaCursor.putInt( layout.majorVersion() );
                metaCursor.putInt( layout.minorVersion() );
                layout.writeMetaData( metaCursor );
            }
            pagedFile.flushAndForce();
            created = true;
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
    public RawCursor<Hit<KEY,VALUE>,IOException> seek( KEY fromInclusive, KEY toExclusive ) throws IOException
    {
        PageCursor cursor = pagedFile.io( rootId, PagedFile.PF_SHARED_READ_LOCK );
        KEY key = layout.newKey();
        VALUE value = layout.newValue();
        cursor.next();

        boolean isInternal;
        int keyCount;
        long childId = 0; // initialized to satisfy compiler
        int pos;
        do
        {
            do
            {
                isInternal = bTreeNode.isInternal( cursor );
                // Find the left-most key within from-range
                keyCount = bTreeNode.keyCount( cursor );
                int search = KeySearch.search( cursor, bTreeNode, fromInclusive, key, keyCount );
                pos = KeySearch.positionOf( search );

                // Assuming unique keys
                if ( isInternal && KeySearch.isHit( search ) )
                {
                    pos++;
                }

                if ( isInternal )
                {
                    childId = bTreeNode.childAt( cursor, pos, stableGeneration, unstableGeneration );
                }
            }
            while ( cursor.shouldRetry() );
            if ( cursor.checkAndClearBoundsFlag() )
            {
                throw new IllegalStateException( "Reading out of bounds." );

            }
            if ( isInternal )
            {
                PointerChecking.checkChildPointer( childId );

                if ( !cursor.next( childId ) )
                {
                    throw new IllegalStateException( "Couldn't go to child " + childId );
                }
            }
        }
        while ( isInternal && keyCount > 0 );

        // Returns cursor which is now initiated with left-most leaf node for the specified range
        return new SeekCursor<>( cursor, key, value, bTreeNode, fromInclusive, toExclusive, layout,
                stableGeneration, unstableGeneration, pos, keyCount );
    }

    @Override
    public long acquireNewId()
    {
        lastId++;
        return lastId;
    }

    // Utility method
    public void printTree() throws IOException
    {
        try ( PageCursor cursor = pagedFile.io( rootId, PagedFile.PF_SHARED_READ_LOCK ) )
        {
            cursor.next();
            TreePrinter.printTree( cursor, bTreeNode, layout, stableGeneration, unstableGeneration, System.out );
        }
    }

    // Utility method
    boolean consistencyCheck() throws IOException
    {
        try ( PageCursor cursor = pagedFile.io( rootId, PagedFile.PF_SHARED_READ_LOCK ) )
        {
            cursor.next();
            return new ConsistencyChecker<>( bTreeNode, layout, stableGeneration, unstableGeneration )
                    .check( cursor );
        }
    }

    @Override
    public void flush() throws IOException
    {
        try ( PageCursor cursor = openMetaPageCursor( pagedFile ) )
        {
            cursor.putLong( 4, rootId );
            cursor.putLong( 12, lastId );
            // generations should be incremented as part of flush, but this functionality doesn't exist yet.
        }
    }

    @Override
    public void close() throws IOException
    {
        flush();
        pagedFile.close();
    }

    /**
     * @return the single {@link IndexWriter} for this index. The returned writer must be
     * {@link IndexWriter#close() closed} before another caller can acquire this writer.
     * @throws IllegalStateException for calls made between a successful call to this method and closing the
     * returned writer.
     */
    @Override
    public IndexWriter<KEY,VALUE> writer( IndexWriter.Options options ) throws IOException
    {
        SingleIndexWriter result = this.writer.getAndSet( null );
        if ( result == null )
        {
            throw new IllegalStateException( "Only supports one concurrent writer" );
        }
        return result.take( rootId, options );
    }

    private class SingleIndexWriter implements IndexWriter<KEY,VALUE>
    {
        private final InternalTreeLogic<KEY,VALUE> treeLogic;
        private PageCursor cursor;
        private IndexWriter.Options options;
        private final byte[] tmp = new byte[0];

        SingleIndexWriter( InternalTreeLogic<KEY,VALUE> treeLogic )
        {
            this.treeLogic = treeLogic;
        }

        SingleIndexWriter take( long rootId, IndexWriter.Options options ) throws IOException
        {
            this.options = options;
            cursor = pagedFile.io( rootId, PagedFile.PF_SHARED_WRITE_LOCK );
            return this;
        }

        @Override
        public void put( KEY key, VALUE value ) throws IOException
        {
            merge( key, value, ValueMergers.overwrite() );
        }

        @Override
        public void merge( KEY key, VALUE value, ValueMerger<VALUE> valueMerger ) throws IOException
        {
            cursor.next( rootId );

            SplitResult<KEY> split = treeLogic.insert( cursor, key, value, valueMerger, options,
                    stableGeneration, unstableGeneration );

            if ( cursor.checkAndClearBoundsFlag() )
            {
                throw new IllegalStateException( "Some internal problem causing out of bounds" );
            }

            if ( split != null )
            {
                // New root
                long newRootId = acquireNewId();
                cursor.next( newRootId );

                bTreeNode.initializeInternal( cursor, stableGeneration, unstableGeneration );
                bTreeNode.insertKeyAt( cursor, split.primKey, 0, 0, tmp );
                bTreeNode.setKeyCount( cursor, 1 );
                bTreeNode.setChildAt( cursor, split.left, 0, stableGeneration, unstableGeneration );
                bTreeNode.setChildAt( cursor, split.right, 1, stableGeneration, unstableGeneration );
                rootId = newRootId;
            }
        }

        @Override
        public VALUE remove( KEY key ) throws IOException
        {
            cursor.next( rootId );

            return treeLogic.remove( cursor, key, layout.newValue(), stableGeneration, unstableGeneration );
        }

        @Override
        public void close() throws IOException
        {
            boolean success = false;
            try
            {
                cursor.close();
                success = true;
            }
            finally
            {
                // Check success to avoid suppressing exception from cursor.close()
                if ( success && !GBPTree.this.writer.compareAndSet( null, this ) )
                {
                    throw new IllegalStateException( "Tried to give back the writer, but somebody else already did" );
                }
            }
        }
    }
}
