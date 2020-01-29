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

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventListenerAdapter;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.graphdb.RelationshipType.withName;

@DbmsExtension
class TransactionTracingIT
{
    private static final int ENTITY_COUNT = 1_000;

    @Inject
    private GraphDatabaseAPI database;
    @Inject
    private DatabaseManagementService managementService;

    @Test
    void tracePageCacheAccessOnAllNodesAccess()
    {
        try ( Transaction transaction = database.beginTx() )
        {
            for ( int i = 0; i < ENTITY_COUNT; i++ )
            {
                transaction.createNode();
            }
            transaction.commit();
        }

        try ( InternalTransaction transaction = (InternalTransaction) database.beginTx() )
        {
            var cursorTracer = transaction.kernelTransaction().pageCursorTracer();
            assertZeroCursor( cursorTracer );

            assertEquals( ENTITY_COUNT, Iterables.count( transaction.getAllNodes() ) );

            assertThat( cursorTracer.pins() ).isEqualTo( 2 );
            assertThat( cursorTracer.unpins() ).isEqualTo( 1 );
            assertThat( cursorTracer.hits() ).isEqualTo( 2 );
        }
    }

    @Test
    void tracePageCacheAccessOnNodeCreation()
    {
        try ( InternalTransaction transaction = (InternalTransaction) database.beginTx() )
        {
            var cursorTracer = transaction.kernelTransaction().pageCursorTracer();

            var commitCursorChecker = new CommitCursorChecker( cursorTracer );
            managementService.registerTransactionEventListener( database.databaseName(), commitCursorChecker );

            for ( int i = 0; i < ENTITY_COUNT; i++ )
            {
                transaction.createNode();
            }
            assertZeroCursor( cursorTracer );

            transaction.commit();
            assertTrue( commitCursorChecker.isInvoked() );
        }
    }

    @Test
    void tracePageCacheAccessOnAllRelationshipsAccess()
    {
        try ( Transaction transaction = database.beginTx() )
        {
            for ( int i = 0; i < ENTITY_COUNT; i++ )
            {
                var source = transaction.createNode();
                source.createRelationshipTo( transaction.createNode(), withName( "connection" ) );
            }
            transaction.commit();
        }

        try ( InternalTransaction transaction = (InternalTransaction) database.beginTx() )
        {
            var cursorTracer = transaction.kernelTransaction().pageCursorTracer();
            assertZeroCursor( cursorTracer );

            assertEquals( ENTITY_COUNT, Iterables.count( transaction.getAllRelationships() ) );

            assertThat( cursorTracer.pins() ).isEqualTo( 5 );
            assertThat( cursorTracer.unpins() ).isEqualTo( 5 );
            assertThat( cursorTracer.hits() ).isEqualTo( 5 );
        }
    }

    @Test
    void tracePageCacheAccessOnFindNodes()
    {
        var marker = Label.label( "marker" );
        var type = withName( "connection" );
        try ( Transaction transaction = database.beginTx() )
        {
            for ( int i = 0; i < ENTITY_COUNT; i++ )
            {
                var source = transaction.createNode( marker );
                source.createRelationshipTo( transaction.createNode(), type );
            }
            transaction.commit();
        }

        try ( InternalTransaction transaction = (InternalTransaction) database.beginTx() )
        {
            var cursorTracer = transaction.kernelTransaction().pageCursorTracer();
            assertZeroCursor( cursorTracer );

            assertEquals( ENTITY_COUNT, Iterators.count( transaction.findNodes( marker ) ) );

            assertThat( cursorTracer.pins() ).isEqualTo( 2 );
            assertThat( cursorTracer.unpins() ).isEqualTo( 2 );
            assertThat( cursorTracer.hits() ).isEqualTo( 2 );
        }
    }

    @Test
    void tracePageCacheAccessOnDetachDelete() throws KernelException
    {
        var type = withName( "connection" );
        long sourceId;
        try ( Transaction transaction = database.beginTx() )
        {
            var source = transaction.createNode();
            for ( int i = 0; i < 10; i++ )
            {
                source.createRelationshipTo( transaction.createNode(), type );
            }
            sourceId = source.getId();
            transaction.commit();
        }

        try ( InternalTransaction transaction = (InternalTransaction) database.beginTx() )
        {
            var cursorTracer = transaction.kernelTransaction().pageCursorTracer();
            assertZeroCursor( cursorTracer );

            transaction.kernelTransaction().dataWrite().nodeDetachDelete( sourceId );

            assertThat( cursorTracer.pins() ).isEqualTo( 15 );
            assertThat( cursorTracer.unpins() ).isEqualTo( 12 );
            assertThat( cursorTracer.hits() ).isEqualTo( 15 );
        }
    }

    private void assertZeroCursor( PageCursorTracer cursorTracer )
    {
        assertThat( cursorTracer.pins() ).isZero();
        assertThat( cursorTracer.unpins() ).isZero();
        assertThat( cursorTracer.hits() ).isZero();
        assertThat( cursorTracer.faults() ).isZero();
    }

    private static class CommitCursorChecker extends TransactionEventListenerAdapter<Object>
    {
        private final PageCursorTracer cursorTracer;
        private volatile boolean invoked;

        CommitCursorChecker( PageCursorTracer cursorTracer )
        {
            this.cursorTracer = cursorTracer;
        }

        public boolean isInvoked()
        {
            return invoked;
        }

        @Override
        public void afterCommit( TransactionData data, Object state, GraphDatabaseService databaseService )
        {
            assertThat( cursorTracer.pins() ).isEqualTo( 1003 );
            assertThat( cursorTracer.unpins() ).isEqualTo( 1003 );
            assertThat( cursorTracer.hits() ).isEqualTo( 1001 );
            assertThat( cursorTracer.faults() ).isEqualTo( 2 );
            invoked = true;
        }
    }
}
