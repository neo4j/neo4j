/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.newapi;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.neo4j.internal.kernel.api.CapableIndexReference;
import org.neo4j.internal.kernel.api.Cursor;
import org.neo4j.internal.kernel.api.IndexCapability;
import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.Kernel;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.NodeExplicitIndexCursor;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.RelationshipExplicitIndexCursor;
import org.neo4j.internal.kernel.api.RelationshipGroupCursor;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.internal.kernel.api.Session;
import org.neo4j.internal.kernel.api.Transaction;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.values.storable.Values;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TraceCursorTest
{
    private ReadTestSupport readTestSupport = new ReadTestSupport();
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();
    private Kernel kernel;
    private static Session session;

    @Before
    public void setup()
    {
        readTestSupport.setup( folder.getRoot(), a -> {
        } );
        kernel = readTestSupport.kernelToTest();
        session = kernel.beginSession( SecurityContext.AUTH_DISABLED );
    }

    @AfterClass
    public static void tearDown()
    {
        try
        {
            KernelTransactionImplementation transaction = (KernelTransactionImplementation) session.beginTransaction();
            ((Read) transaction.dataRead()).setFlagRecordCursorsTraces( false );
        }
        catch ( KernelException e )
        {
            e.printStackTrace();
        }
    }

    @Test
    public void verifyTracingOutput()
    {
        NodeCursor cursor = kernel.cursors().allocateNodeCursor();
        Transaction tx = openTXAndSetTraceFlag( true );

        tx.dataRead().allNodesScan( cursor );
        try
        {
            // when
            tx.close();

            // then (not expected)
            fail( "Should have failed because of open cursor" );
        }
        catch ( Exception e )
        {
            assertEquals( "Cursors were not correctly closed. Number of leaked cursors: 1.", e.getMessage() );
            assertEquals( 1, e.getSuppressed().length );
            assertTrue( "This class caused the open cursor but it wasn't named in the stack trace",
                    e.getSuppressed()[0].getMessage().contains( this.getClass().getName() ) );
        }
    }

    // AllNodesScan
    @Test
    public void shouldFailOnOpenCursorForAllNodesScanAtTXClose()
    {
        // given
        NodeCursor cursor = kernel.cursors().allocateNodeCursor();
        Transaction tx = openTXAndSetTraceFlag( true );

        tx.dataRead().allNodesScan( cursor );
        closeTXAndExpectException( tx, cursor );
    }

    // SingleNode
    @Test
    public void shouldFailOnOpenCursorForSingleNodeAtTXClose()
    {
        // given
        NodeCursor cursor = kernel.cursors().allocateNodeCursor();
        Transaction tx = openTXAndSetTraceFlag( true );

        tx.dataRead().singleNode( 0L, cursor );
        closeTXAndExpectException( tx, cursor );
    }

    // RelationshipLabelScan
    @Test
    public void shouldFailOnOpenCursorForRelationshipLabelScanAtTXClose()
    {
        // given
        RelationshipScanCursor cursor = kernel.cursors().allocateRelationshipScanCursor();
        Transaction tx = openTXAndSetTraceFlag( true );

        tx.dataRead().relationshipLabelScan( 0, cursor );
        closeTXAndExpectException( tx, cursor );
    }

    // AllRelationshipsScan
    @Test
    public void shouldFailOnOpenCursorForAllRelationshipsScanAtTXClose()
    {
        // given
        RelationshipScanCursor cursor = kernel.cursors().allocateRelationshipScanCursor();
        Transaction tx = openTXAndSetTraceFlag( true );

        tx.dataRead().allRelationshipsScan( cursor );
        closeTXAndExpectException( tx, cursor );
    }

    // SingleRelationship
    @Test
    public void shouldFailOnOpenCursorForSingleRelationshipAtTXClose()
    {
        // given
        RelationshipScanCursor cursor = kernel.cursors().allocateRelationshipScanCursor();
        Transaction tx = openTXAndSetTraceFlag( true );

        tx.dataRead().singleRelationship( 0L, cursor );
        closeTXAndExpectException( tx, cursor );
    }

    // RelationshipGroups
    @Test
    public void shouldFailOnOpenCursorForRelationshipGroupsScanAtTXClose()
    {
        // given
        RelationshipGroupCursor cursor = kernel.cursors().allocateRelationshipGroupCursor();
        Transaction tx = openTXAndSetTraceFlag( true );

        tx.dataRead().relationshipGroups( 0L, 0L, cursor );
        closeTXAndExpectException( tx, cursor );
    }

    // Relationships
    @Test
    public void shouldFailOnOpenCursorForRelationshipsAtTXClose()
    {
        // given
        RelationshipTraversalCursor cursor = kernel.cursors().allocateRelationshipTraversalCursor();
        Transaction tx = openTXAndSetTraceFlag( true );

        tx.dataRead().relationships( 0L, 0L, cursor );
        closeTXAndExpectException( tx, cursor );
    }

    // NodeLabelScan
    @Test
    public void shouldFailOnOpenCursorForNodeLabelScanAtTXClose()
    {
        // given
        NodeLabelIndexCursor cursor = kernel.cursors().allocateNodeLabelIndexCursor();
        Transaction tx = openTXAndSetTraceFlag( true );

        tx.dataRead().nodeLabelScan( 0, cursor );
        closeTXAndExpectException( tx, cursor );
    }

    // NodeLabelUnionScan
    // TODO: Unignore when unionScan on DefaultNodeLabelIndexCursor is implemented
    @Ignore
    public void shouldFailOnOpenCursorForNodeLabelUnionScanAtTXClose()
    {
        // given
        NodeLabelIndexCursor cursor = kernel.cursors().allocateNodeLabelIndexCursor();
        Transaction tx = openTXAndSetTraceFlag( true );

        try
        {
            tx.dataRead().nodeLabelUnionScan( cursor, 0 );
        }
        catch ( UnsupportedOperationException e )
        {
            // ignore this, because it is actually correct but not what we want to test here
        }
        closeTXAndExpectException( tx, cursor );
    }

    // NodeLabelIntersectionScan
    // TODO: Unignore when intersectionScan on DefaultNodeLabelIndexCursor is implemented
    @Ignore
    public void shouldFailOnOpenCursorForNodeLabelIntersectionScanAtTXClose()
    {
        // given
        NodeLabelIndexCursor cursor = kernel.cursors().allocateNodeLabelIndexCursor();
        Transaction tx = openTXAndSetTraceFlag( true );

        try
        {
            tx.dataRead().nodeLabelIntersectionScan( cursor, 0 );
        }
        catch ( UnsupportedOperationException e )
        {
            // ignore this, because it is actually correct but not what we want to test here
        }

        closeTXAndExpectException( tx, cursor );
    }

    // NodeIndexScan
    @Test
    public void shouldFailOnOpenCursorForNodeIndexScanAtTXClose()
    {
        // given

        NodeValueIndexCursor cursor = kernel.cursors().allocateNodeValueIndexCursor();
        Transaction tx = openTXAndSetTraceFlag( true );

        DefaultCapableIndexReference index = new DefaultCapableIndexReference( true, IndexCapability.NO_CAPABILITY, 0, new int[]{0} );
        try
        {
            tx.dataRead().nodeIndexScan( index, cursor, IndexOrder.NONE );
        }
        catch ( Exception e )
        {
            if ( e.getClass().getSimpleName().endsWith( "IndexNotFoundKernelException" ) ) // no index for given label
            {
                // ignore this, because it is actually correct but not what we want to test here
            }
            else
            {
                // something else gone wrong, abort!
                fail( e.getMessage() );
            }
        }
        closeTXAndExpectException( tx, cursor );
    }

    // NodeIndexSeek
    @Test
    public void shouldFailOnOpenCursorForNodeIndexSeekAtTXClose()
    {
        // given
        NodeValueIndexCursor cursor = kernel.cursors().allocateNodeValueIndexCursor();
        Transaction tx = openTXAndSetTraceFlag( true );

        try
        {
            tx.dataRead().nodeIndexSeek( CapableIndexReference.NO_INDEX, cursor, IndexOrder.NONE, IndexQuery.exists( 0 ) );
        }
        catch ( Exception e )
        {
            if ( e.getClass().getSimpleName().equals( "IllegalArgumentException" ) )
            {
                // ignore this, because it is actually correct but not what we want to test here
            }
            else
            {
                // something else gone wrong, abort!
                fail( e.getMessage() );
            }
        }
        closeTXAndExpectException( tx, cursor );
    }

    // - - - Property cursors - - -

    // NodeProperties
    @Test
    public void shouldFailOnOpenNodePropertiesAtTXClose()
    {
        // given
        PropertyCursor cursor = kernel.cursors().allocatePropertyCursor();
        Transaction tx = openTXAndSetTraceFlag( true );

        tx.dataRead().nodeProperties( 0L, 0L, cursor );
        closeTXAndExpectException( tx, cursor );
    }

    // RelationshipProperties
    @Test
    public void shouldFailOnOpenRelationshipPropertiesAtTXClose()
    {
        // given
        PropertyCursor cursor = kernel.cursors().allocatePropertyCursor();
        Transaction tx = openTXAndSetTraceFlag( true );

        tx.dataRead().relationshipProperties( 0L, 0L, cursor );
        closeTXAndExpectException( tx, cursor );
    }

    // GraphProperties
    @Test
    @Ignore
    public void shouldFailOnOpenGraphPropertiesAtTXClose()
    {
        // given
        PropertyCursor cursor = kernel.cursors().allocatePropertyCursor();
        Transaction tx = openTXAndSetTraceFlag( true );

        tx.dataRead().graphProperties( cursor );
        closeTXAndExpectException( tx, cursor );
    }

    // - - - Explicit index cursors - - -

    // NodeExplicitIndexLookup
    @Test
    public void shouldFailOnOpenCursorForNodeExplicitIndexLookupAtTXClose()
    {
        // given
        NodeExplicitIndexCursor cursor = kernel.cursors().allocateNodeExplicitIndexCursor();
        Transaction tx = openTXAndSetTraceFlag( true );

        try
        {
            tx.indexRead().nodeExplicitIndexLookup( cursor, "index", "key", Values.stringValue( "value" ) );
        }
        // this test is sensitive to changing the execution order of the initialization
        catch ( KernelException e )
        {
            if ( e.getClass().getSimpleName().equals( "ExplicitIndexNotFoundKernelException" ) )
            {
                // ignore this, because it is actually correct but not what we want to test here
            }
            else
            {
                // something else gone wrong, abort!
                fail( e.getMessage() );
            }
        }
        closeTXAndExpectException( tx, cursor );
    }

    // NodeExplicitIndexQuery
    @Test
    public void shouldFailOnOpenCursorForNodeExplicitIndexQueryAtTXClose()
    {
        // given
        NodeExplicitIndexCursor cursor = kernel.cursors().allocateNodeExplicitIndexCursor();
        Transaction tx = openTXAndSetTraceFlag( true );

        try
        {
            tx.indexRead().nodeExplicitIndexQuery( cursor, "index", "key", new Object() );
        }
        // this test is sensitive to changing the execution order of the initialization
        catch ( KernelException e )
        {
            if ( e.getClass().getSimpleName().equals( "ExplicitIndexNotFoundKernelException" ) )
            {
                // ignore this, because it is actually correct but not what we want to test here
            }
            else
            {
                // something else gone wrong, abort!
                fail( e.getMessage() );
            }
        }
        closeTXAndExpectException( tx, cursor );
    }

    // NodeExplicitIndexQuery2
    @Test
    public void shouldFailOnOpenCursorForNodeExplicitIndexQuery2AtTXClose()
    {
        // given
        NodeExplicitIndexCursor cursor = kernel.cursors().allocateNodeExplicitIndexCursor();
        Transaction tx = openTXAndSetTraceFlag( true );

        try
        {
            tx.indexRead().nodeExplicitIndexQuery( cursor, "index", new Object() );
        }
        // this test is sensitive to changing the execution order of the initialization
        catch ( KernelException e )
        {
            if ( e.getClass().getSimpleName().equals( "ExplicitIndexNotFoundKernelException" ) )
            {
                // ignore this, because it is actually correct but not what we want to test here
            }
            else
            {
                // something else gone wrong, abort!
                fail( e.getMessage() );
            }
        }
        closeTXAndExpectException( tx, cursor );
    }

    // RelationshipExplicitIndexGet
    @Test
    public void shouldFailOnOpenCursorForRelationshipExplicitIndexGetAtTXClose()
    {
        // given
        RelationshipExplicitIndexCursor cursor = kernel.cursors().allocateRelationshipExplicitIndexCursor();
        Transaction tx = openTXAndSetTraceFlag( true );

        try
        {
            tx.indexRead().relationshipExplicitIndexGet( cursor, "index", "key", Values.stringValue( "value" ), 0L, 0L );
        }
        // this test is sensitive to changing the execution order of the initialization
        catch ( KernelException e )
        {
            if ( e.getClass().getSimpleName().equals( "ExplicitIndexNotFoundKernelException" ) )
            {
                // ignore this, because it is acutally correct but not what we want to test here
            }
            else
            {
                // something else gone wrong, abort!
                fail( e.getMessage() );
            }
        }
        closeTXAndExpectException( tx, cursor );
    }

    // RelationshipExplicitIndexQuery
    @Test
    public void shouldFailOnOpenCursorForRelationshipExplicitIndexQueryAtTXClose()
    {
        // given
        RelationshipExplicitIndexCursor cursor = kernel.cursors().allocateRelationshipExplicitIndexCursor();
        Transaction tx = openTXAndSetTraceFlag( true );

        try
        {
            tx.indexRead().relationshipExplicitIndexQuery( cursor, "index", "key", Values.stringValue( "value" ), 0L, 0L );
        }
        // this test is sensitive to changing the execution order of the initialization
        catch ( KernelException e )
        {
            if ( e.getClass().getSimpleName().equals( "ExplicitIndexNotFoundKernelException" ) )
            {
                // ignore this, because it is acutally correct but not what we want to test here
            }
            else
            {
                // something else gone wrong, abort!
                fail( e.getMessage() );
            }
        }
        closeTXAndExpectException( tx, cursor );
    }

    // RelationshipExplicitIndexQuery2
    @Test
    public void shouldFailOnOpenCursorForRelationshipExplicitIndexQuery2AtTXClose()
    {
        // given
        RelationshipExplicitIndexCursor cursor = kernel.cursors().allocateRelationshipExplicitIndexCursor();
        Transaction tx = openTXAndSetTraceFlag( true );

        try
        {
            tx.indexRead().relationshipExplicitIndexQuery( cursor, "index", Values.stringValue( "value" ), 0L, 0L );
        }
        // this test is sensitive to changing the execution order of the initialization
        catch ( KernelException e )
        {
            if ( e.getClass().getSimpleName().equals( "ExplicitIndexNotFoundKernelException" ) )
            {
                // ignore this, because it is acutally correct but not what we want to test here
            }
            else
            {
                // something else gone wrong, abort!
                fail( e.getMessage() );
            }
        }
        closeTXAndExpectException( tx, cursor );
    }

    @Test
    public void cleaningUpShouldWork()
    {
        // given some cursors
        NodeCursor nodeCursor = kernel.cursors().allocateNodeCursor();
        RelationshipScanCursor relationshipCursor = kernel.cursors().allocateRelationshipScanCursor();
        PropertyCursor propertyCursor = kernel.cursors().allocatePropertyCursor();

        KernelTransactionImplementation tx = (KernelTransactionImplementation) openTXAndSetTraceFlag( false );

        // when using/acquiring some cursors
        tx.dataRead().allNodesScan( nodeCursor );
        tx.dataRead().allRelationshipsScan( relationshipCursor );
        tx.dataRead().nodeProperties( 0L, 0L, propertyCursor );
        try
        {
            // then this should work because of automated clean up
            tx.close();
        }
        catch ( TransactionFailureException e )
        {
            fail( "Cleanup did not work. Transaction failed while closing:\n" + e.getMessage() );
        }
    }

    private Transaction openTXAndSetTraceFlag( boolean b )
    {
        try
        {
            KernelTransactionImplementation transaction = (KernelTransactionImplementation) session.beginTransaction();
            ((Read) transaction.dataRead()).setFlagRecordCursorsTraces( b );
            return transaction;
        }
        catch ( KernelException e )
        {
            fail();
            return null;
        }
    }

    private void closeTXAndExpectException( Transaction tx, Cursor cursor )
    {
        try
        {
            // when
            tx.close();

            if ( cursor.isClosed() )
            {
                fail( "Cursor was not even opened in the test" );
            }
            // then (not expected)
            fail( "Should have failed because of open cursor" );
        }
        catch ( Exception e )
        {
            // then (expected)
        }
    }
}
