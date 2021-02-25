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
import java.nio.file.Path;
import java.util.Collections;
import java.util.Iterator;

import org.neo4j.annotations.documented.ReporterFactory;
import org.neo4j.collection.PrimitiveLongResourceCollections;
import org.neo4j.collection.PrimitiveLongResourceIterator;
import org.neo4j.common.EntityType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.api.index.IndexProgressor;
import org.neo4j.storageengine.api.EntityTokenUpdate;
import org.neo4j.storageengine.api.EntityTokenUpdateListener;

/**
 * This is an almost no-op relationship type scan store and is used
 * in place of the real implementation when rtss is turned OFF.
 *
 * It does one thing and that is delete any rtss file that exists during startup.
 * In this way we make sure that when rtss is turned back ON again it will be
 * rebuilt. We need to do this because we don't know if rtss missed any updates
 * while it was turned OFF.
 */
public class EmptyingTokenScanStore implements TokenScanStore
{
    private final FileSystemAbstraction fileSystem;
    private final DatabaseLayout directoryStructure;
    private final boolean readOnly;
    private final EntityType entityType;
    private final boolean shouldDeleteExistingStore;

    private EmptyingTokenScanStore( FileSystemAbstraction fileSystem, DatabaseLayout directoryStructure, boolean readOnly,
            EntityType entityType, boolean shouldDeleteExistingStore )
    {
        this.fileSystem = fileSystem;
        this.directoryStructure = directoryStructure;
        this.readOnly = readOnly;
        this.entityType = entityType;
        this.shouldDeleteExistingStore = shouldDeleteExistingStore;
    }

    public static LabelScanStore emptyLss( FileSystemAbstraction fileSystem, DatabaseLayout directoryStructure, boolean readOnly )
    {
        return new EmptyingLabelScanStore( fileSystem, directoryStructure, readOnly );
    }

    public static RelationshipTypeScanStore emptyRtss( FileSystemAbstraction fileSystem, DatabaseLayout directoryStructure, boolean readOnly )
    {
        return new EmptyingRelationshipTypeScanStore( fileSystem, directoryStructure, readOnly );
    }

    @Override
    public EntityType entityType()
    {
        return entityType;
    }

    @Override
    public TokenScanReader newReader()
    {
        return EmptyTokenScanReader.INSTANCE;
    }

    @Override
    public TokenScanWriter newWriter( PageCursorTracer cursorTracer )
    {
        return TokenScanWriter.EMPTY_WRITER;
    }

    @Override
    public TokenScanWriter newBulkAppendWriter( PageCursorTracer cursorTracer )
    {
        return TokenScanWriter.EMPTY_WRITER;
    }

    @Override
    public void force( IOLimiter limiter, PageCursorTracer cursorTracer )
    {   // no-op
    }

    @Override
    public AllEntriesTokenScanReader allEntityTokenRanges( PageCursorTracer cursorTracer )
    {
        return EmptyAllEntriesTokenScanReader.INSTANCE;
    }

    @Override
    public AllEntriesTokenScanReader allEntityTokenRanges( long fromEntityId, long toEntityId, PageCursorTracer cursorTracer )
    {
        return EmptyAllEntriesTokenScanReader.INSTANCE;
    }

    @Override
    public ResourceIterator<Path> snapshotStoreFiles()
    {
        return Iterators.emptyResourceIterator();
    }

    @Override
    public EntityTokenUpdateListener updateListener()
    {
        return EmptyEntityTokenUpdateListener.INSTANCE;
    }

    @Override
    public boolean isEmpty( PageCursorTracer cursorTracer )
    {
        return true;
    }

    @Override
    public void init() throws IOException
    {
        if ( shouldDeleteExistingStore )
        {
            if ( readOnly && fileSystem.fileExists( directoryStructure.relationshipTypeScanStore() ) )
            {
                throw new IllegalStateException(
                        "Database was started in read only mode and with relationship type scan store turned OFF, " +
                                "but relationship type scan store file still exists and cannot be deleted in read only mode. " +
                                "Please start database with relationship type scan store turned ON or " +
                                "without read only mode to let database delete the relationship type scan store safely. " +
                                "Note that consistency check use read only mode. " +
                                "Use setting 'unsupported.dbms.enable_relationship_type_scan_store' to turn relationship type scan store ON or OFF." );
            }
            if ( fileSystem.fileExists( directoryStructure.relationshipTypeScanStore() ) )
            {
                fileSystem.deleteFile( directoryStructure.relationshipTypeScanStore() );
            }
        }
    }

    @Override
    public void start()
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
        static final EntityTokenUpdateListener INSTANCE = new EmptyEntityTokenUpdateListener();

        @Override
        public void applyUpdates( Iterable<EntityTokenUpdate> labelUpdates, PageCursorTracer cursorTracer )
        {   // no-op
        }
    }

    private static class EmptyAllEntriesTokenScanReader implements AllEntriesTokenScanReader
    {
        static final AllEntriesTokenScanReader INSTANCE = new EmptyAllEntriesTokenScanReader();

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

    private static class EmptyTokenScanReader implements TokenScanReader
    {
        static final TokenScanReader INSTANCE = new EmptyTokenScanReader();

        @Override
        public PrimitiveLongResourceIterator entitiesWithToken( int tokenId, PageCursorTracer cursorTracer )
        {
            return PrimitiveLongResourceCollections.emptyIterator();
        }

        @Override
        public TokenScan entityTokenScan( int tokenId, PageCursorTracer cursorTracer )
        {
            return EmptyTokenScan.INSTANCE;
        }

        @Override
        public PrimitiveLongResourceIterator entitiesWithAnyOfTokens( long fromId, int[] tokenIds, PageCursorTracer cursorTracer )
        {
            return PrimitiveLongResourceCollections.emptyIterator();
        }
    }

    private static class EmptyTokenScan implements TokenScan
    {
        static final TokenScan INSTANCE = new EmptyTokenScan();

        @Override
        public IndexProgressor initialize( IndexProgressor.EntityTokenClient client, IndexOrder indexOrder, PageCursorTracer cursorTracer )
        {
            return IndexProgressor.EMPTY;
        }

        @Override
        public IndexProgressor initializeBatch( IndexProgressor.EntityTokenClient client, int sizeHint, PageCursorTracer cursorTracer )
        {
            return IndexProgressor.EMPTY;
        }
    }

    private static class EmptyingLabelScanStore extends EmptyingTokenScanStore implements LabelScanStore
    {
        public EmptyingLabelScanStore( FileSystemAbstraction fileSystem, DatabaseLayout directoryStructure, boolean readOnly )
        {
            super( fileSystem, directoryStructure, readOnly, EntityType.NODE, false );
        }
    }

    private static class EmptyingRelationshipTypeScanStore extends EmptyingTokenScanStore implements RelationshipTypeScanStore
    {
        public EmptyingRelationshipTypeScanStore( FileSystemAbstraction fileSystem, DatabaseLayout directoryStructure, boolean readOnly )
        {
            super( fileSystem, directoryStructure, readOnly, EntityType.RELATIONSHIP, true );
        }
    }
}
