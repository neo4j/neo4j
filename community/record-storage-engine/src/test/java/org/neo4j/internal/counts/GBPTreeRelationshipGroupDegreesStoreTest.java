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
package org.neo4j.internal.counts;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;

import org.neo4j.internal.counts.GBPTreeRelationshipGroupDegreesStore.DegreesRebuilder;
import org.neo4j.internal.counts.RelationshipGroupDegreesStore.Updater;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.RelationshipDirection;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.internal.counts.GBPTreeCountsStore.NO_MONITOR;
import static org.neo4j.internal.counts.GBPTreeRelationshipGroupDegreesStore.EMPTY_REBUILD;
import static org.neo4j.internal.counts.GBPTreeRelationshipGroupDegreesStore.degreeKey;
import static org.neo4j.internal.counts.GBPTreeRelationshipGroupDegreesStore.keyToString;
import static org.neo4j.io.pagecache.IOLimiter.UNLIMITED;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.storageengine.api.RelationshipDirection.INCOMING;
import static org.neo4j.storageengine.api.RelationshipDirection.LOOP;
import static org.neo4j.storageengine.api.RelationshipDirection.OUTGOING;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_ID;

@PageCacheExtension
class GBPTreeRelationshipGroupDegreesStoreTest
{
    private static final long GROUP_ID_1 = 1;
    private static final long GROUP_ID_2 = 2;

    @Inject
    private TestDirectory directory;

    @Inject
    private PageCache pageCache;

    @Inject
    private FileSystemAbstraction fs;

    private GBPTreeRelationshipGroupDegreesStore countsStore;

    @BeforeEach
    void openCountsStore() throws Exception
    {
        openCountsStore( EMPTY_REBUILD );
    }

    @AfterEach
    void closeCountsStore()
    {
        countsStore.close();
    }

    @Test
    void shouldUpdateAndReadSomeCounts() throws IOException
    {
        // given
        long txId = BASE_TX_ID;
        try ( Updater updater = countsStore.apply( ++txId, NULL ) )
        {
            updater.increment( GROUP_ID_1, OUTGOING, 10 );
            updater.increment( GROUP_ID_1, INCOMING, 3 );
            updater.increment( GROUP_ID_2, OUTGOING, 7 );
            updater.increment( GROUP_ID_2, LOOP, 14 );
        }
        try ( Updater updater = countsStore.apply( ++txId, NULL ) )
        {
            updater.increment( GROUP_ID_1, OUTGOING, 5 ); // now at 15
            updater.increment( GROUP_ID_1, INCOMING, 2 ); // now at 5
        }

        countsStore.checkpoint( UNLIMITED, NULL );

        // when/then
        assertEquals( 15, countsStore.degree( GROUP_ID_1, OUTGOING, NULL ) );
        assertEquals( 5, countsStore.degree( GROUP_ID_1, INCOMING, NULL ) );
        assertEquals( 7, countsStore.degree( GROUP_ID_2, OUTGOING, NULL ) );
        assertEquals( 14, countsStore.degree( GROUP_ID_2, LOOP, NULL ) );

        // and when
        try ( Updater updater = countsStore.apply( ++txId, NULL ) )
        {
            updater.increment( GROUP_ID_1, OUTGOING, -7 );
            updater.increment( GROUP_ID_1, INCOMING, -5 );
            updater.increment( GROUP_ID_2, OUTGOING, -2 );
        }

        // then
        assertEquals( 8, countsStore.degree( GROUP_ID_1, OUTGOING, NULL ) );
        assertEquals( 0, countsStore.degree( GROUP_ID_1, INCOMING, NULL ) );
        assertEquals( 5, countsStore.degree( GROUP_ID_2, OUTGOING, NULL ) );
        assertEquals( 14, countsStore.degree( GROUP_ID_2, LOOP, NULL ) );
    }

    @Test
    void shouldUseCountsBuilderOnCreation() throws Exception
    {
        // given
        long rebuiltAtTransactionId = 5;
        long groupId1 = 3;
        long groupId2 = 6;
        closeCountsStore();
        deleteCountsStore();

        // when
        TestableCountsBuilder builder = new TestableCountsBuilder( rebuiltAtTransactionId )
        {
            @Override
            public void rebuild( Updater updater, PageCursorTracer cursorTracer, MemoryTracker memoryTracker )
            {
                super.rebuild( updater, cursorTracer, memoryTracker );
                updater.increment( groupId1, OUTGOING, 10 );
                updater.increment( groupId2, INCOMING, 14 );
            }
        };
        openCountsStore( builder );
        assertTrue( builder.lastCommittedTxIdCalled );
        assertTrue( builder.rebuildCalled );
        assertEquals( 10, countsStore.degree( groupId1, OUTGOING, NULL ) );
        assertEquals( 14, countsStore.degree( groupId2, INCOMING, NULL ) );
        assertEquals( 0, countsStore.degree( groupId1, INCOMING, NULL ) );

        // and when
        checkpointAndRestartCountsStore();
        // Re-applying a txId below or equal to the "rebuild transaction id" should not apply it
        increment( rebuiltAtTransactionId - 1, groupId1, OUTGOING, 100 );
        assertEquals( 10, countsStore.degree( groupId1, OUTGOING, NULL ) );
        increment( rebuiltAtTransactionId, groupId1, OUTGOING, 100 );
        assertEquals( 10, countsStore.degree( groupId1, OUTGOING, NULL ) );

        // then
        increment( rebuiltAtTransactionId + 1, groupId1, OUTGOING, 100 );
        assertEquals( 110, countsStore.degree( groupId1, OUTGOING, NULL ) );
    }

    @Test
    void shouldDumpCountsStore() throws IOException
    {
        // given
        long txId = BASE_TX_ID + 1;
        try ( Updater updater = countsStore.apply( txId, NULL ) )
        {
            updater.increment( GROUP_ID_1, OUTGOING, 10 );
            updater.increment( GROUP_ID_1, INCOMING, 3 );
            updater.increment( GROUP_ID_2, LOOP, 7 );
        }
        countsStore.checkpoint( UNLIMITED, NULL );
        closeCountsStore();

        // when
        ByteArrayOutputStream out = new ByteArrayOutputStream( 1024 );
        GBPTreeRelationshipGroupDegreesStore.dump( pageCache, countsStoreFile(), new PrintStream( out ), NULL );

        // then
        String dump = out.toString();
        assertThat( dump ).contains( keyToString( degreeKey( GROUP_ID_1, OUTGOING ) ) + " = 10" );
        assertThat( dump ).contains( keyToString( degreeKey( GROUP_ID_1, INCOMING ) ) + " = 3" );
        assertThat( dump ).contains( keyToString( degreeKey( GROUP_ID_2, LOOP ) ) + " = 7" );
        assertThat( dump ).contains( "Highest gap-free txId: " + txId );
    }

    private void increment( long txId, long groupId, RelationshipDirection direction, int delta )
    {
        try ( Updater updater = countsStore.apply( txId, NULL ) )
        {
            updater.increment( groupId, direction, delta );
        }
    }

    private void checkpointAndRestartCountsStore() throws Exception
    {
        countsStore.checkpoint( UNLIMITED, NULL );
        closeCountsStore();
        openCountsStore();
    }

    private void deleteCountsStore() throws IOException
    {
        directory.getFileSystem().deleteFile( countsStoreFile() );
    }

    private Path countsStoreFile()
    {
        return directory.file( "counts.db" );
    }

    private void openCountsStore( DegreesRebuilder builder ) throws IOException
    {
        instantiateCountsStore( builder, false, NO_MONITOR );
        countsStore.start( NULL, INSTANCE );
    }

    private void instantiateCountsStore( DegreesRebuilder builder, boolean readOnly, GBPTreeCountsStore.Monitor monitor ) throws IOException
    {
        countsStore = new GBPTreeRelationshipGroupDegreesStore( pageCache, countsStoreFile(), fs, immediate(), builder, readOnly, PageCacheTracer.NULL, monitor,
                DEFAULT_DATABASE_NAME );
    }

    private static class TestableCountsBuilder implements DegreesRebuilder
    {
        private final long rebuiltAtTransactionId;
        boolean lastCommittedTxIdCalled;
        boolean rebuildCalled;

        TestableCountsBuilder( long rebuiltAtTransactionId )
        {
            this.rebuiltAtTransactionId = rebuiltAtTransactionId;
        }

        @Override
        public void rebuild( Updater updater, PageCursorTracer cursorTracer, MemoryTracker memoryTracker )
        {
            rebuildCalled = true;
        }

        @Override
        public long lastCommittedTxId()
        {
            lastCommittedTxIdCalled = true;
            return rebuiltAtTransactionId;
        }
    }
}
