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
package org.neo4j.tracers;

import org.junit.jupiter.api.Disabled;
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
import org.neo4j.io.pagecache.tracing.cursor.CursorContext;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.index.schema.RelationshipTypeScanStoreSettings;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.allow_upgrade;
import static org.neo4j.configuration.GraphDatabaseSettings.record_format;
import static org.neo4j.graphdb.RelationshipType.withName;

@DbmsExtension( configurationCallback = "configure" )
class TransactionTracingIT
{
    private static final int ENTITY_COUNT = 1_000;

    @Inject
    private GraphDatabaseAPI database;
    @Inject
    private DatabaseManagementService managementService;

    @ExtensionCallback
    void configure( TestDatabaseManagementServiceBuilder builder )
    {
        builder.setConfig( RelationshipTypeScanStoreSettings.enable_scan_stores_as_token_indexes, true );
    }

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
            var cursorContext = transaction.kernelTransaction().cursorContext();
            assertZeroCursor( cursorContext );

            assertEquals( ENTITY_COUNT, Iterables.count( transaction.getAllNodes() ) );

            assertThat( cursorContext.getCursorTracer().pins() ).isEqualTo( 2 );
            assertThat( cursorContext.getCursorTracer().unpins() ).isEqualTo( 1 );
            assertThat( cursorContext.getCursorTracer().hits() ).isEqualTo( 2 );
        }
    }

    @Test
    void tracePageCacheAccessOnNodeCreation()
    {
        try ( InternalTransaction transaction = (InternalTransaction) database.beginTx() )
        {
            var cursorContext = transaction.kernelTransaction().cursorContext();

            var commitCursorChecker = new CommitCursorChecker( cursorContext );
            managementService.registerTransactionEventListener( database.databaseName(), commitCursorChecker );

            for ( int i = 0; i < ENTITY_COUNT; i++ )
            {
                transaction.createNode();
            }
            assertZeroCursor( cursorContext );

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
            var cursorContext = transaction.kernelTransaction().cursorContext();
            assertZeroCursor( cursorContext );

            assertEquals( ENTITY_COUNT, Iterables.count( transaction.getAllRelationships() ) );

            assertThat( cursorContext.getCursorTracer().pins() ).isEqualTo( 5 );
            assertThat( cursorContext.getCursorTracer().unpins() ).isEqualTo( 5 );
            assertThat( cursorContext.getCursorTracer().hits() ).isEqualTo( 5 );
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
            var cursorContext = transaction.kernelTransaction().cursorContext();
            assertZeroCursor( cursorContext );

            assertEquals( ENTITY_COUNT, Iterators.count( transaction.findNodes( marker ) ) );

            assertThat( cursorContext.getCursorTracer().pins() ).isEqualTo( 2 );
            assertThat( cursorContext.getCursorTracer().unpins() ).isEqualTo( 2 );
            assertThat( cursorContext.getCursorTracer().hits() ).isEqualTo( 2 );
        }
    }

    @Test
    @Disabled( "Disable until token index feature is enabled" )
    void tracePageCacheAccessOnFindRelationships()
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
            var cursorContext = transaction.kernelTransaction().cursorContext();
            assertZeroCursor( cursorContext );

            assertEquals( ENTITY_COUNT, Iterators.count( transaction.findRelationships( type ) ) );

            // 1 while setting up TokenScan, and 1 for scanning the index
            assertThat( cursorContext.getCursorTracer().pins() ).isEqualTo( 2 );
            assertThat( cursorContext.getCursorTracer().unpins() ).isEqualTo( 2 );
            assertThat( cursorContext.getCursorTracer().hits() ).isEqualTo( 2 );
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
            var cursorContext = transaction.kernelTransaction().cursorContext();
            assertZeroCursor( cursorContext );

            transaction.kernelTransaction().dataWrite().nodeDetachDelete( sourceId );

            assertThat( cursorContext.getCursorTracer().pins() ).isEqualTo( 13 );
            assertThat( cursorContext.getCursorTracer().unpins() ).isEqualTo( 11 );
            assertThat( cursorContext.getCursorTracer().hits() ).isEqualTo( 13 );
        }
    }

    private void assertZeroCursor( CursorContext cursorContext )
    {
        assertThat( cursorContext.getCursorTracer().pins() ).isZero();
        assertThat( cursorContext.getCursorTracer().unpins() ).isZero();
        assertThat( cursorContext.getCursorTracer().hits() ).isZero();
        assertThat( cursorContext.getCursorTracer().faults() ).isZero();
    }

    private static class CommitCursorChecker extends TransactionEventListenerAdapter<Object>
    {
        private final CursorContext cursorContext;
        private volatile boolean invoked;

        CommitCursorChecker( CursorContext cursorContext )
        {
            this.cursorContext = cursorContext;
        }

        public boolean isInvoked()
        {
            return invoked;
        }

        @Override
        public void afterCommit( TransactionData data, Object state, GraphDatabaseService databaseService )
        {
            assertThat( cursorContext.getCursorTracer().pins() ).isEqualTo( 1003 );
            assertThat( cursorContext.getCursorTracer().unpins() ).isEqualTo( 1003 );
            assertThat( cursorContext.getCursorTracer().hits() ).isEqualTo( 1001 );
            assertThat( cursorContext.getCursorTracer().faults() ).isEqualTo( 2 );
            invoked = true;
        }
    }
}
