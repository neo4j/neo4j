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
import java.nio.file.Path;

import org.neo4j.common.EntityType;
import org.neo4j.configuration.Config;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.helpers.collection.BoundedIterable;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.TokenIndexReader;
import org.neo4j.kernel.api.index.ValueIndexReader;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.NodePropertyAccessor;

import static org.neo4j.internal.helpers.collection.Iterators.asResourceIterator;
import static org.neo4j.internal.helpers.collection.Iterators.iterator;

public class TokenIndexAccessor extends TokenIndex implements IndexAccessor
{
    public TokenIndexAccessor( boolean readOnly, Monitors monitors, String monitorTag, PageCache pageCache,
            IndexFiles indexFiles, FileSystemAbstraction fs, PageCacheTracer cacheTracer,
            String tokenStoreName, RecoveryCleanupWorkCollector recoveryCleanupWorkCollector, Config config,
            DatabaseLayout directoryStructure, EntityType entityType )
    {
        super( readOnly, monitors, monitorTag, pageCache, indexFiles, fs, cacheTracer, tokenStoreName );

        instantiateTree( recoveryCleanupWorkCollector, new NativeIndexHeaderWriter( ONLINE ) );
        instantiateUpdater( config, directoryStructure, entityType );
    }

    @Override
    public IndexUpdater newUpdater( IndexUpdateMode mode, PageCursorTracer cursorTracer )
    {
        assertTreeOpen();
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
    public void force( IOLimiter ioLimiter, PageCursorTracer cursorTracer )
    {
        index.checkpoint( ioLimiter, cursorTracer );
        writeMonitor.force();
    }

    @Override
    public void refresh()
    {
        // not required in this implementation
    }

    @Override
    public void close()
    {
        closeResources();
    }

    @Override
    public ValueIndexReader newValueReader()
    {
        throw new UnsupportedOperationException( "Not applicable for token indexes " );
    }

    @Override
    public TokenIndexReader newTokenReader()
    {
        assertTreeOpen();
        return new DefaultTokenIndexReader( index );
    }

    @Override
    public BoundedIterable<Long> newAllEntriesReader( long fromIdInclusive, long toIdExclusive, PageCursorTracer cursorTracer )
    {
        //This is just used for consistency checker and token indexes are not consistency checked the same way (not yet anyway).
        throw new UnsupportedOperationException( "Not applicable for token indexes" );
    }

    @Override
    public ResourceIterator<Path> snapshotFiles()
    {
        return asResourceIterator( iterator( indexFiles.getStoreFile() ) );
    }

    @Override
    public void verifyDeferredConstraints( NodePropertyAccessor nodePropertyAccessor )
    {
        //Not needed since constraints are not based on token indexes.
    }

    @Override
    public long estimateNumberOfEntries( PageCursorTracer cursorTracer )
    {
        //This is just used for consistency checker and token indexes are not consistency checked the same way (not yet anyway).
        throw new UnsupportedOperationException( "Not applicable for token indexes" );
    }

    @Override
    public void drop()
    {
        index.setDeleteOnClose( true );
        closeResources();
        indexFiles.clear();
    }
}
