/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.kernel.impl.index.schema;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;

import org.neo4j.common.EntityType;
import org.neo4j.configuration.Config;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexSample;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.storageengine.api.NodePropertyAccessor;
import org.neo4j.util.Preconditions;

import static org.neo4j.index.internal.gbptree.GBPTree.NO_HEADER_WRITER;

public class TokenIndexPopulator extends TokenIndex implements IndexPopulator
{
    /**
     * The type of entity this token index is backing.
     */
    private final EntityType entityType;

    /**
     * Layout of the database.
     */
    private final DatabaseLayout directoryStructure;
    private final Config config;

    private byte[] failureBytes;
    private boolean dropped;
    private boolean closed;

    TokenIndexPopulator( DatabaseIndexContext databaseIndexContext, DatabaseLayout directoryStructure, IndexFiles indexFiles, Config config,
            EntityType entityType, String tokenStoreName )
    {
        super( databaseIndexContext, indexFiles, tokenStoreName );
        this.directoryStructure = directoryStructure;
        this.config = config;
        this.entityType = entityType;
    }

    @Override
    public synchronized void create()
    {
        assertNotDropped();
        assertNotClosed();

        indexFiles.clear();
        instantiateTree( RecoveryCleanupWorkCollector.immediate(), new NativeIndexHeaderWriter( POPULATING ) );
        instantiateUpdater( config, directoryStructure, entityType );
    }

    @Override
    public synchronized void drop()
    {
        try
        {
            if ( index != null )
            {
                index.setDeleteOnClose( true );
            }
            closeResources();
            indexFiles.clear();
        }
        finally
        {
            dropped = true;
            closed = true;
        }
    }

    @Override
    public void add( Collection<? extends IndexEntryUpdate<?>> updates, PageCursorTracer cursorTracer ) throws IndexEntryConflictException
    {
        try ( TokenIndexUpdater updater = singleUpdater.initialize( index.writer( cursorTracer ) ) )
        {
            for ( IndexEntryUpdate<?> update : updates )
            {
                updater.process( update );
            }
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    @Override
    public void verifyDeferredConstraints( NodePropertyAccessor nodePropertyAccessor )
    {
        // No-op, token indexes don't have any uniqueness constraints.
    }

    @Override
    public IndexUpdater newPopulatingUpdater( NodePropertyAccessor accessor, PageCursorTracer cursorTracer )
    {
        try
        {
            return singleUpdater.initialize( index.writer( cursorTracer ) );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    @Override
    public synchronized void close( boolean populationCompletedSuccessfully, PageCursorTracer cursorTracer )
    {
        Preconditions.checkState( !(populationCompletedSuccessfully && failureBytes != null),
                "Can't mark index as online after it has been marked as failure" );

        try
        {
            assertNotDropped();
            if ( populationCompletedSuccessfully )
            {
                // Successful and completed population
                assertTreeOpen();
                flushTreeAndMarkAs( ONLINE, cursorTracer );
            }
            else if ( failureBytes != null )
            {
                // Failed population
                ensureTreeInstantiated();
                markTreeAsFailed( cursorTracer );
            }
            // else cancelled population. Here we simply close the tree w/o checkpointing it and it will look like POPULATING state on next open
        }
        finally
        {
            closeResources();
            closed = true;
        }
    }

    private void flushTreeAndMarkAs( byte state, PageCursorTracer cursorTracer )
    {
        index.checkpoint( IOLimiter.UNLIMITED, pageCursor -> pageCursor.putByte( state ), cursorTracer );
    }

    private void markTreeAsFailed( PageCursorTracer cursorTracer )
    {
        Preconditions.checkState( failureBytes != null, "markAsFailed hasn't been called, populator not actually failed?" );
        index.checkpoint( IOLimiter.UNLIMITED, new FailureHeaderWriter( failureBytes, FAILED ), cursorTracer );
    }

    @Override
    public void markAsFailed( String failure )
    {
        failureBytes = failure.getBytes( StandardCharsets.UTF_8 );
    }

    @Override
    public void includeSample( IndexEntryUpdate<?> update )
    {
        // We don't do sampling for token indexes since that information is available in other ways.
    }

    @Override
    public IndexSample sample( PageCursorTracer cursorTracer )
    {
        throw new UnsupportedOperationException( "Token indexes does not support index sampling" );
    }

    private void assertNotDropped()
    {
        Preconditions.checkState( !dropped, "Populator has already been dropped." );
    }

    private void assertNotClosed()
    {
        Preconditions.checkState( !closed, "Populator has already been closed." );
    }

    private void ensureTreeInstantiated()
    {
        if ( index == null )
        {
            instantiateTree( RecoveryCleanupWorkCollector.ignore(), NO_HEADER_WRITER );
        }
    }
}
