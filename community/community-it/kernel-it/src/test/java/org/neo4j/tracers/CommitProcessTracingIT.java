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
package org.neo4j.tracers;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.recordstorage.Command;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.api.state.TxState;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;

import static java.util.Collections.emptyList;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_BYTE_ARRAY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.kernel.impl.transaction.tracing.CommitEvent.NULL;
import static org.neo4j.lock.ResourceLocker.IGNORE;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.storageengine.api.TransactionApplicationMode.EXTERNAL;
import static org.neo4j.storageengine.api.txstate.TxStateVisitor.NO_DECORATION;

@DbmsExtension
public class CommitProcessTracingIT
{
    @Inject
    private GraphDatabaseAPI database;
    @Inject
    private TransactionCommitProcess commitProcess;
    @Inject
    private RecordStorageEngine storageEngine;

    @Test
    void tracePageCacheAccessOnCommandCreation() throws KernelException
    {
        long sourceId;
        try ( Transaction transaction = database.beginTx() )
        {
            sourceId = transaction.createNode( Label.label( "a" ) ).getId();
            transaction.commit();
        }

        var pageCacheTracer = new DefaultPageCacheTracer();
        try ( var cursorTracer = pageCacheTracer.createPageCursorTracer( "tracePageCacheAccessOnCommandCreation" );
              var reader = storageEngine.newReader() )
        {
            assertZeroCursor( cursorTracer );
            var context = storageEngine.newCommandCreationContext( PageCursorTracer.NULL, INSTANCE );
            List<StorageCommand> commands = new ArrayList<>();
            var txState = new TxState();
            txState.nodeDoAddLabel( 1, sourceId );

            storageEngine.createCommands( commands, txState, reader, context, IGNORE, 0, NO_DECORATION, cursorTracer, INSTANCE );

            assertCursor( cursorTracer, 1 );
        }
    }

    @Test
    void tracePageCacheAccessOnEmptyTransactionApply() throws TransactionFailureException
    {
        var transaction = new PhysicalTransactionRepresentation( emptyList(), EMPTY_BYTE_ARRAY, 0, 0, 0, 0 );
        var pageCacheTracer = new DefaultPageCacheTracer();
        try ( var cursorTracer = pageCacheTracer.createPageCursorTracer( "tracePageCacheAccessOnEmptyTransactionApply" ) )
        {
            assertZeroCursor( cursorTracer );

            commitProcess.commit( new TransactionToApply( transaction, cursorTracer ), NULL, EXTERNAL );

            assertCursor( cursorTracer, 2 );
        }
    }

    @Test
    void tracePageCacheAccessOnTransactionApply() throws TransactionFailureException
    {
        var transaction = new PhysicalTransactionRepresentation( List.of( new Command.NodeCountsCommand( 1, 2 ) ), EMPTY_BYTE_ARRAY, 0, 0, 0, 0 );
        var pageCacheTracer = new DefaultPageCacheTracer();
        try ( var cursorTracer = pageCacheTracer.createPageCursorTracer( "tracePageCacheAccessOnTransactionApply" ) )
        {
            assertZeroCursor( cursorTracer );

            commitProcess.commit( new TransactionToApply( transaction, cursorTracer ), NULL, EXTERNAL );

            assertCursor( cursorTracer, 3 );
        }
    }

    private void assertCursor( PageCursorTracer cursorTracer, int expected )
    {
        assertThat( cursorTracer.pins() ).isEqualTo( expected );
        assertThat( cursorTracer.unpins() ).isEqualTo( expected );
        assertThat( cursorTracer.hits() ).isEqualTo( expected );
    }

    private void assertZeroCursor( PageCursorTracer cursorTracer )
    {
        assertThat( cursorTracer.pins() ).isZero();
        assertThat( cursorTracer.unpins() ).isZero();
        assertThat( cursorTracer.hits() ).isZero();
    }
}
