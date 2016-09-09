package org.neo4j.index.btree;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.neo4j.index.IdProvider;
import org.neo4j.index.SCIndex;
import org.neo4j.index.SCIndexDescription;
import org.neo4j.index.SCResult;
import org.neo4j.index.Seeker;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;

public class Index implements SCIndex, IdProvider, Closeable
{
    private final File metaFile;
    private final int pageSize;

    private final PagedFile pagedFile;
    private final SCIndexDescription description;
    private long rootId;
    private long lastId;
    private final IndexInsert inserter;
    private final BTreeNode BTreeNode;

    /**
     * Initiate an already existing index from file and meta file
     * @param pageCache     {@link PageCache} to use to map index file
     * @param indexFile     {@link File} containing the actual index
     * @param metaFile      {@link File} containing the meta data about the index
     * @throws IOException
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
        this.BTreeNode = new BTreeNode( meta.pageSize );
        this.inserter = new IndexInsert( this, BTreeNode );
    }

    /**
     * Initiate a completely new index
     * @param pageCache     {@link PageCache} to use to map index file
     * @param indexFile     {@link File} containing the actual index
     * @param metaFile      {@link File} conaining the meta data about the index
     * @param description   {@link SCIndexDescription} description of the index
     * @param pageSize      page size to use for index
     * @throws IOException
     */
    public Index( PageCache pageCache, File indexFile, File metaFile, SCIndexDescription description, int pageSize )
            throws IOException
    {
        this.metaFile = metaFile;
        this.pageSize = pageSize;
        this.pagedFile = pageCache.map( indexFile, pageSize );
        this.description = description;
        this.lastId = 0;
        this.rootId = this.lastId;
        this.BTreeNode = new BTreeNode( pageSize );
        this.inserter = new IndexInsert( this, BTreeNode );

        SCMetaData.writeMetaData( metaFile, description, pageSize, rootId, lastId );

        // Initialize index root node to a leaf node.
        PageCursor cursor = pagedFile.io( rootId, PagedFile.PF_SHARED_WRITE_LOCK );
        cursor.next();
        BTreeNode.initializeLeaf( cursor );
        cursor.close();
    }

    @Override
    public SCIndexDescription getDescription()
    {
        return description;
    }

    @Override
    public void insert( long[] key, long[] value ) throws IOException
    {
        PageCursor cursor = pagedFile.io( rootId, PagedFile.PF_SHARED_WRITE_LOCK );
        cursor.next();

        SplitResult split = inserter.insert( cursor, key, value );

        if ( split != null )
        {
            // New root
            rootId = acquireNewId();
            cursor.next( rootId );

            BTreeNode.initializeInternal( cursor );
            BTreeNode.setKeyAt( cursor, split.primKey, 0 );
            BTreeNode.setKeyCount( cursor, 1 );
            BTreeNode.setChildAt( cursor, split.left, 0 );
            BTreeNode.setChildAt( cursor, split.right, 1 );
            if ( metaFile != null )
            {
                SCMetaData.writeMetaData( metaFile, description, pageSize, rootId, lastId );
            }
        }
        cursor.close();
    }

    @Override
    public void seek( Seeker seeker, List<SCResult> resultList ) throws IOException
    {
        PageCursor cursor = pagedFile.io( rootId, PagedFile.PF_SHARED_WRITE_LOCK );
        cursor.next();

        seeker.seek( cursor, BTreeNode, resultList );
        cursor.close();
    }

    @Override
    public long acquireNewId() throws FileNotFoundException
    {
        lastId++;
        if ( metaFile != null )
        {
            SCMetaData.writeMetaData( metaFile, description, pageSize, rootId, lastId );
        }
        return lastId;
    }

    // Utility method
    public void printTree() throws IOException
    {
        PageCursor cursor = pagedFile.io( rootId, PagedFile.PF_SHARED_READ_LOCK );
        cursor.next();

        int level = 0;
        long id;
        while ( BTreeNode.isInternal( cursor ) )
        {
            System.out.println( "Level " + level++ );
            id = cursor.getCurrentPageId();
            printKeysOfSiblings( cursor, BTreeNode );
            System.out.println();
            cursor.next( id );
            cursor.next( BTreeNode.childAt( cursor, 0 ) );
        }

        System.out.println( "Level " + level );
        printKeysOfSiblings( cursor, BTreeNode );
        System.out.println();
        cursor.close();
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

    // Utility method
    protected static void printKeysOfSiblings( PageCursor cursor, BTreeNode BTreeNode ) throws IOException
    {
        while ( true )
        {
            printKeys( cursor, BTreeNode );
            long rightSibling = BTreeNode.rightSibling( cursor );
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
