/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.impl.index.schema;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;

import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.index.internal.gbptree.Writer;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.impl.index.GBPTreeUtil;

import static org.neo4j.index.internal.gbptree.GBPTree.NO_HEADER_WRITER;

/**
 * {@link IndexPopulator} backed by a {@link GBPTree}.
 *
 * @param <KEY> type of {@link SchemaNumberKey}.
 * @param <VALUE> type of {@link SchemaNumberValue}.
 */
public abstract class NativeSchemaNumberIndexPopulator<KEY extends SchemaNumberKey, VALUE extends SchemaNumberValue>
        extends NativeSchemaNumberIndex<KEY,VALUE> implements IndexPopulator
{
    static final byte BYTE_FAILED = 0;
    static final byte BYTE_ONLINE = 1;
    static final byte BYTE_POPULATING = 2;

    private final KEY treeKey;
    private final VALUE treeValue;
    private final ConflictDetectingValueMerger<KEY,VALUE> conflictDetectingValueMerger;
    private final NativeSchemaNumberIndexUpdater<KEY,VALUE> singleUpdater;

    private Writer<KEY,VALUE> singleTreeWriter;
    private byte[] failureBytes;
    private boolean dropped;

    NativeSchemaNumberIndexPopulator( PageCache pageCache, File storeFile, Layout<KEY,VALUE> layout )
    {
        super( pageCache, storeFile, layout );
        this.treeKey = layout.newKey();
        this.treeValue = layout.newValue();
        this.conflictDetectingValueMerger = new ConflictDetectingValueMerger<>();
        singleUpdater = new NativeSchemaNumberIndexUpdater<>( layout.newKey(), layout.newValue() );
    }

    @Override
    public synchronized void create() throws IOException
    {
        GBPTreeUtil.deleteIfPresent( pageCache, storeFile );
        pageCache.getCachedFileSystem().mkdirs( storeFile.getParentFile() );
        instantiateTree( RecoveryCleanupWorkCollector.IMMEDIATE, new NativeSchemaIndexHeaderWriter( BYTE_POPULATING ) );
        instantiateWriter();
    }

    void instantiateWriter() throws IOException
    {
        assert singleTreeWriter == null;
        singleTreeWriter = tree.writer();
    }

    @Override
    public synchronized void drop() throws IOException
    {
        try
        {
            closeWriter();
            closeTree();
            GBPTreeUtil.deleteIfPresent( pageCache, storeFile );
        }
        finally
        {
            dropped = true;
        }
    }

    @Override
    public void add( Collection<? extends IndexEntryUpdate<?>> updates ) throws IndexEntryConflictException, IOException
    {
        for ( IndexEntryUpdate<?> update : updates )
        {
            add( update );
        }
    }

    @Override
    public void add( IndexEntryUpdate<?> update ) throws IndexEntryConflictException, IOException
    {
        NativeSchemaNumberIndexUpdater
                .processAdd( treeKey, treeValue, update, singleTreeWriter, conflictDetectingValueMerger );
    }

    @Override
    public void verifyDeferredConstraints( PropertyAccessor propertyAccessor )
            throws IndexEntryConflictException, IOException
    {
        // No-op, uniqueness is checked for each update in add(IndexEntryUpdate)
    }

    @Override
    public IndexUpdater newPopulatingUpdater( PropertyAccessor accessor ) throws IOException
    {
        return singleUpdater.initialize( singleTreeWriter, false );
    }

    @Override
    public synchronized void close( boolean populationCompletedSuccessfully ) throws IOException
    {
        closeWriter();
        if ( populationCompletedSuccessfully && failureBytes != null )
        {
            throw new IllegalStateException( "Can't mark index as online after it has been marked as failure" );
        }

        try
        {
            if ( populationCompletedSuccessfully )
            {
                assertPopulatorOpen();
                markTreeAsOnline();
            }
            else
            {
                assertNotDropped();
                ensureTreeInstantiated();
                markTreeAsFailed();
            }
        }
        finally
        {
            closeTree();
        }
    }

    private void assertNotDropped()
    {
        if ( dropped )
        {
            throw new IllegalStateException( "Populator has already been dropped." );
        }
    }

    @Override
    public void markAsFailed( String failure ) throws IOException
    {
        failureBytes = failure.getBytes( StandardCharsets.UTF_8 );
    }

    private void ensureTreeInstantiated() throws IOException
    {
        if ( tree == null )
        {
            instantiateTree( RecoveryCleanupWorkCollector.NULL, NO_HEADER_WRITER );
        }
    }

    private void assertPopulatorOpen()
    {
        if ( tree == null )
        {
            throw new IllegalStateException( "Populator has already been closed." );
        }
    }

    private void markTreeAsFailed() throws IOException
    {
        if ( failureBytes == null )
        {
            failureBytes = new byte[0];
        }
        tree.checkpoint( IOLimiter.unlimited(), new FailureHeaderWriter( failureBytes ) );
    }

    private void markTreeAsOnline() throws IOException
    {
        tree.checkpoint( IOLimiter.unlimited(), ( pc ) -> pc.putByte( BYTE_ONLINE ) );
    }

    void closeWriter() throws IOException
    {
        singleTreeWriter = closeIfPresent( singleTreeWriter );
    }
}
