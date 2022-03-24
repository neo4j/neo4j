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

import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventListenerAdapter;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;

import static org.neo4j.graphdb.RelationshipType.withName;

@ExtendWith( SoftAssertionsExtension.class )
@DbmsExtension
class TransactionTracingIT
{
    private static final int ENTITY_COUNT = 1_000;

    @Inject
    private GraphDatabaseAPI database;
    @Inject
    private DatabaseManagementService managementService;

    @InjectSoftAssertions
    private SoftAssertions softly;

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

            softly.assertThat( Iterables.count( transaction.getAllNodes() ) )
                  .as( "Number of expected nodes" ).isEqualTo( ENTITY_COUNT );

            softly.assertThat( cursorContext.getCursorTracer().pins() ).as( "Number of cursor pins" ).isEqualTo( 2 );
            softly.assertThat( cursorContext.getCursorTracer().unpins() ).as( "Number of cursor unpins" ).isEqualTo( 2 );
            softly.assertThat( cursorContext.getCursorTracer().hits() ).as( "Number of cursor hits" ).isEqualTo( 2 );
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
            softly.assertThat( commitCursorChecker.isInvoked() ).as( "Transaction committed" ).isTrue();
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

            softly.assertThat( Iterables.count( transaction.getAllRelationships() ) ).as( "Number of expected relationships" ).isEqualTo( ENTITY_COUNT );

            softly.assertThat( cursorContext.getCursorTracer().pins() ).as( "Number of cursor pins" ).isEqualTo( 5 );
            softly.assertThat( cursorContext.getCursorTracer().unpins() ).as( "Number of cursor unpins" ).isEqualTo( 5 );
            softly.assertThat( cursorContext.getCursorTracer().hits() ).as( "Number of cursor hits" ).isEqualTo( 5 );
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

            softly.assertThat( Iterators.count( transaction.findNodes( marker ) ) ).as( "Number of expected nodes" ).isEqualTo( ENTITY_COUNT );

            softly.assertThat( cursorContext.getCursorTracer().pins() ).as( "Number of cursor pins" ).isEqualTo( 1 );
            softly.assertThat( cursorContext.getCursorTracer().unpins() ).as( "Number of cursor unpins" ).isEqualTo( 1 );
            softly.assertThat( cursorContext.getCursorTracer().hits() ).as( "Number of cursor hits" ).isEqualTo( 1 );
        }
    }

    @Test
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

            softly.assertThat( Iterators.count( transaction.findRelationships( type ) ) ).as( "Number of expected relationships" ).isEqualTo( ENTITY_COUNT );

            softly.assertThat( cursorContext.getCursorTracer().pins() ).as( "Number of cursor pins" ).isEqualTo( 1 );
            softly.assertThat( cursorContext.getCursorTracer().unpins() ).as( "Number of cursor unpins" ).isEqualTo( 1 );
            softly.assertThat( cursorContext.getCursorTracer().hits() ).as( "Number of cursor hits" ).isEqualTo( 1 );
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

            softly.assertThat( cursorContext.getCursorTracer().pins() ).as( "Number of cursor pins" ).isEqualTo( 5 );
            softly.assertThat( cursorContext.getCursorTracer().unpins() ).as( "Number of cursor unpins" ).isEqualTo( 1 );
            softly.assertThat( cursorContext.getCursorTracer().hits() ).as( "Number of cursor hits" ).isEqualTo( 5 );
        }
    }

    private void assertZeroCursor( CursorContext cursorContext )
    {
        softly.assertThat( cursorContext.getCursorTracer().pins() ).as( "Number of cursor pins" ).isZero();
        softly.assertThat( cursorContext.getCursorTracer().unpins() ).as( "Number of cursor unpins" ).isZero();
        softly.assertThat( cursorContext.getCursorTracer().hits() ).as( "Number of cursor hits" ).isZero();
        softly.assertThat( cursorContext.getCursorTracer().faults() ).as( "Number of cursor faults" ).isZero();
    }

    private class CommitCursorChecker extends TransactionEventListenerAdapter<Object>
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
            softly.assertThat( cursorContext.getCursorTracer().pins() ).as( "Number of cursor pins" ).isEqualTo( 1001 );
            softly.assertThat( cursorContext.getCursorTracer().unpins() ).as( "Number of cursor unpins" ).isEqualTo( 1001 );
            softly.assertThat( cursorContext.getCursorTracer().hits() ).as( "Number of cursor hits" ).isEqualTo( 999 );
            softly.assertThat( cursorContext.getCursorTracer().faults() ).as( "Number of cursor faults" ).isEqualTo( 2 );
            invoked = true;
        }
    }
}
