/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.internal.index.label;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;

import org.neo4j.annotations.documented.ReporterFactory;
import org.neo4j.collection.PrimitiveLongResourceCollections;
import org.neo4j.collection.PrimitiveLongResourceIterator;
import org.neo4j.common.EntityType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.api.index.IndexProgressor;
import org.neo4j.storageengine.api.EntityTokenUpdate;
import org.neo4j.storageengine.api.EntityTokenUpdateListener;

import static org.neo4j.common.EntityType.RELATIONSHIP;

public class EmptyRelationshipTypeScanStore implements RelationshipTypeScanStore
{
    static RelationshipTypeScanStore instance = new EmptyRelationshipTypeScanStore();

    @Override
    public EntityType entityType()
    {
        return RELATIONSHIP;
    }

    @Override
    public TokenScanReader newReader()
    {
        return EmptyTokenScanReader.instance;
    }

    @Override
    public TokenScanWriter newWriter()
    {
        return EmptyTokenScanWriter.instance;
    }

    @Override
    public TokenScanWriter newBulkAppendWriter()
    {
        return EmptyTokenScanWriter.instance;
    }

    @Override
    public void force( IOLimiter limiter, PageCursorTracer cursorTracer )
    {   // no-op
    }

    @Override
    public AllEntriesTokenScanReader allEntityTokenRanges()
    {
        return EmptyAllEntriesTokenScanReader.instance;
    }

    @Override
    public AllEntriesTokenScanReader allEntityTokenRanges( long fromEntityId, long toEntityId )
    {
        return EmptyAllEntriesTokenScanReader.instance;
    }

    @Override
    public ResourceIterator<File> snapshotStoreFiles()
    {
        return Iterators.emptyResourceIterator();
    }

    @Override
    public EntityTokenUpdateListener updateListener()
    {
        return EmptyEntityTokenUpdateListener.instance;
    }

    @Override
    public boolean isEmpty()
    {
        return true;
    }

    @Override
    public void init() throws IOException
    {   // no-op
    }

    @Override
    public void start() throws IOException
    {   // no-op
    }

    @Override
    public void stop()
    {   // no-op
    }

    @Override
    public void shutdown()
    {   // no-op
    }

    @Override
    public void drop()
    {   // no-op
    }

    @Override
    public boolean isReadOnly()
    {
        return false;
    }

    @Override
    public boolean consistencyCheck( ReporterFactory reporterFactory, PageCursorTracer cursorTracer )
    {
        return true;
    }

    private static class EmptyEntityTokenUpdateListener implements EntityTokenUpdateListener
    {
        static EntityTokenUpdateListener instance = new EmptyEntityTokenUpdateListener();

        @Override
        public void applyUpdates( Iterable<EntityTokenUpdate> labelUpdates )
        {   // no-op
        }
    }

    private static class EmptyAllEntriesTokenScanReader implements AllEntriesTokenScanReader
    {
        static AllEntriesTokenScanReader instance = new EmptyAllEntriesTokenScanReader();

        @Override
        public int rangeSize()
        {
            return 0;
        }

        @Override
        public long maxCount()
        {
            return 0;
        }

        @Override
        public void close() throws Exception
        {   // no-op
        }

        @Override
        public Iterator<EntityTokenRange> iterator()
        {
            return Collections.emptyIterator();
        }
    }

    private static class EmptyTokenScanWriter implements TokenScanWriter
    {
        static TokenScanWriter instance = new EmptyTokenScanWriter();

        @Override
        public void write( EntityTokenUpdate update ) throws IOException
        {   // no-op
        }

        @Override
        public void close() throws IOException
        {   // no-op
        }
    }

    private static class EmptyTokenScanReader implements TokenScanReader
    {
        static TokenScanReader instance = new EmptyTokenScanReader();

        @Override
        public PrimitiveLongResourceIterator entityWithToken( int tokenId, PageCursorTracer cursorTracer )
        {
            return PrimitiveLongResourceCollections.emptyIterator();
        }

        @Override
        public TokenScan entityTokenScan( int tokenId, PageCursorTracer cursorTracer )
        {
            return EmptyTokenScan.instance;
        }

        @Override
        public PrimitiveLongResourceIterator entitiesWithAnyOfTokens( long fromId, int[] tokenIds, PageCursorTracer cursorTracer )
        {
            return null;
        }
    }

    private static class EmptyTokenScan implements TokenScan
    {
        static final TokenScan instance = new EmptyTokenScan();

        @Override
        public IndexProgressor initialize( IndexProgressor.EntityTokenClient client, PageCursorTracer cursorTracer )
        {
            return IndexProgressor.EMPTY;
        }

        @Override
        public IndexProgressor initializeBatch( IndexProgressor.EntityTokenClient client, int sizeHint, PageCursorTracer cursorTracer )
        {
            return IndexProgressor.EMPTY;
        }
    }
}
