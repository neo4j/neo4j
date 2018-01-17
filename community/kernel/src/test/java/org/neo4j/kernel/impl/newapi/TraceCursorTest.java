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

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

import org.neo4j.internal.kernel.api.CapableIndexReference;
import org.neo4j.internal.kernel.api.IndexCapability;
import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.Kernel;
import org.neo4j.internal.kernel.api.NodeExplicitIndexCursor;
import org.neo4j.internal.kernel.api.Session;
import org.neo4j.internal.kernel.api.Transaction;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.values.storable.Values;

import static org.junit.Assert.fail;

public class TraceCursorTest
{
    private ReadTestSupport readTestSupport = new ReadTestSupport();
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();
    private Kernel kernel;
    private Session session;

    @Before
    public void setup()
    {
        try
        {
            readTestSupport.setup( folder.getRoot(), a ->
            { } );
        }
        catch ( IOException e )
        {
            fail();
        }
        kernel = readTestSupport.kernelToTest();
        session = kernel.beginSession( SecurityContext.AUTH_DISABLED );
        System.setProperty( "org.neo4j.kernel.impl.newapi.Read.trackCursors", "true" );
        System.setProperty( "org.neo4j.kernel.impl.newapi.Read.recordCursorTraces", "true" );
    }

    @After
    public void tearDown()
    {
        System.clearProperty( "org.neo4j.kernel.impl.newapi.Read.trackCursors" );
    }

    // AllNodesScan
    @Test
    public void shouldFailOnOpenCursorForAllNodesScanAtTXClose()
    {
        // given
        NodeCursor cursor = (NodeCursor) kernel.cursors().allocateNodeCursor();
        Transaction tx = openTX();

        tx.dataRead().allNodesScan( cursor );
        closeTXAndExpectException( tx );
    }

    // SingleNode
    @Test
    public void shouldFailOnOpenCursorForSingleNodeAtTXClose()
    {
        // given
        NodeCursor cursor = (NodeCursor) kernel.cursors().allocateNodeCursor();
        Transaction tx = openTX();

        tx.dataRead().singleNode( 0L, cursor );
        closeTXAndExpectException( tx );
    }

    // RelationshipLabelScan
    @Test
    public void shouldFailOnOpenCursorForRelationshipLabelScanAtTXClose()
    {
        // given
        RelationshipScanCursor cursor = (RelationshipScanCursor) kernel.cursors().allocateRelationshipScanCursor();
        Transaction tx = openTX();

        tx.dataRead().relationshipLabelScan( 0, cursor );
        closeTXAndExpectException( tx );
    }

    // AllRelationshipsScan
    @Test
    public void shouldFailOnOpenCursorForAllRelationshipsScanAtTXClose()
    {
        // given
        RelationshipScanCursor cursor = (RelationshipScanCursor) kernel.cursors().allocateRelationshipScanCursor();
        Transaction tx = openTX();

        tx.dataRead().allRelationshipsScan( cursor );
        closeTXAndExpectException( tx );
    }

    // SingleRelationship
    @Test
    public void shouldFailOnOpenCursorForSingleRelationshipAtTXClose()
    {
        // given
        RelationshipScanCursor cursor = (RelationshipScanCursor) kernel.cursors().allocateRelationshipScanCursor();
        Transaction tx = openTX();

        tx.dataRead().singleRelationship( 0L, cursor );
        closeTXAndExpectException( tx );
    }

    // RelationshipGroups
    @Test
    public void shouldFailOnOpenCursorForRelationshipGroupsScanAtTXClose()
    {
        // given
        RelationshipGroupCursor cursor = (RelationshipGroupCursor) kernel.cursors().allocateRelationshipGroupCursor();
        Transaction tx = openTX();

        tx.dataRead().relationshipGroups( 0L, 0L, cursor );
        closeTXAndExpectException( tx );
    }

    // Relationships
    @Test
    public void shouldFailOnOpenCursorForRelationshipsAtTXClose()
    {
        // given
        RelationshipTraversalCursor cursor = (RelationshipTraversalCursor) kernel.cursors().allocateRelationshipTraversalCursor();
        Transaction tx = openTX();

        tx.dataRead().relationships( 0L, 0L, cursor );
        closeTXAndExpectException( tx );
    }

    // NodeLabelScan
    @Test
    public void shouldFailOnOpenCursorForNodeLabelScanAtTXClose()
    {
        // given
        NodeLabelIndexCursor cursor = (NodeLabelIndexCursor) kernel.cursors().allocateNodeLabelIndexCursor();
        Transaction tx = openTX();

        tx.dataRead().nodeLabelScan( 0, cursor );
        closeTXAndExpectException( tx );
    }

    // NodeLabelUnionScan
    @Test
    public void shouldFailOnOpenCursorForNodeLabelUnionScanAtTXClose()
    {
        // given
        NodeLabelIndexCursor cursor = (NodeLabelIndexCursor) kernel.cursors().allocateNodeLabelIndexCursor();
        Transaction tx = openTX();

        try
        {
            tx.dataRead().nodeLabelUnionScan( cursor, 0 );
        }
        catch ( UnsupportedOperationException e )
        {
            // ignore this, because it is actually correct but not what we want to test here
        }
        closeTXAndExpectException( tx );
    }

    // NodeLabelIntersectionScan
    @Test
    public void shouldFailOnOpenCursorForNodeLabelIntersectionScanAtTXClose()
    {
        // given
        NodeLabelIndexCursor cursor = (NodeLabelIndexCursor) kernel.cursors().allocateNodeLabelIndexCursor();
        Transaction tx = openTX();

        try
        {
            tx.dataRead().nodeLabelIntersectionScan( cursor, 0 );
        }
        catch ( UnsupportedOperationException e )
        {
            // ignore this, because it is actually correct but not what we want to test here
        }

        closeTXAndExpectException( tx );
    }

    // NodeIndexScan
    @Test
    public void shouldFailOnOpenCursorForNodeIndexScanAtTXClose()
    {
        // given
        NodeValueIndexCursor cursor = (NodeValueIndexCursor) kernel.cursors().allocateNodeValueIndexCursor();
        Transaction tx = openTX();

        DefaultCapableIndexReference index = new DefaultCapableIndexReference( true, IndexCapability.NO_CAPABILITY, 0, new int[]{0} );
        try
        {
            tx.dataRead().nodeIndexScan( index, cursor, IndexOrder.NONE );
        }
        catch ( Exception e )
        {
            if ( e.getMessage().contains( "IndexNotFoundKernelException" ) ) // no index for given label
            {
                // ignore this, because it is actually correct but not what we want to test here
            }
            else
            {
                // something else gone wrong, abort!
                fail( e.getMessage() );
            }
        }
        closeTXAndExpectException( tx );
    }

    // NodeIndexSeek
    @Test
    public void shouldFailOnOpenCursorForNodeIndexSeekAtTXClose()
    {
        // given
        NodeValueIndexCursor cursor = (NodeValueIndexCursor) kernel.cursors().allocateNodeValueIndexCursor();
        Transaction tx = openTX();

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
        closeTXAndExpectException( tx );
    }

    // - - - Property cursors - - -

    // NodeProperties
    @Test
    public void shouldFailOnOpenNodePropertiesAtTXClose()
    {
        // given
        PropertyCursor cursor = (PropertyCursor) kernel.cursors().allocatePropertyCursor();
        Transaction tx = openTX();

        tx.dataRead().nodeProperties( 0L, 0L, cursor );
        closeTXAndExpectException( tx );
    }

    // RelationshipProperties
    @Test
    public void shouldFailOnOpenRelationshipPropertiesAtTXClose()
    {
        // given
        PropertyCursor cursor = (PropertyCursor) kernel.cursors().allocatePropertyCursor();
        Transaction tx = openTX();

        tx.dataRead().relationshipProperties( 0L, 0L, cursor );
        closeTXAndExpectException( tx );
    }

    // GraphProperties
    @Test
    @Ignore
    public void shouldFailOnOpenGraphPropertiesAtTXClose()
    {
        // given
        PropertyCursor cursor = (PropertyCursor) kernel.cursors().allocatePropertyCursor();
        Transaction tx = openTX();

        tx.dataRead().graphProperties( cursor );
        closeTXAndExpectException( tx );
    }

    // - - - Explicit index cursors - - -

    // NodeExplicitIndexLookup
    @Test
    public void shouldFailOnOpenCursorForNodeExplicitIndexLookupAtTXClose()
    {
        // given
        NodeExplicitIndexCursor cursor = kernel.cursors().allocateNodeExplicitIndexCursor();
        Transaction tx = openTX();

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
        closeTXAndExpectException( tx );
    }

    // NodeExplicitIndexQuery
    @Test
    public void shouldFailOnOpenCursorForNodeExplicitIndexQueryAtTXClose()
    {
        // given
        NodeExplicitIndexCursor cursor = kernel.cursors().allocateNodeExplicitIndexCursor();
        Transaction tx = openTX();

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
        closeTXAndExpectException( tx );
    }

    // NodeExplicitIndexQuery2
    @Test
    public void shouldFailOnOpenCursorForNodeExplicitIndexQuery2AtTXClose()
    {
        // given
        NodeExplicitIndexCursor cursor = kernel.cursors().allocateNodeExplicitIndexCursor();
        Transaction tx = openTX();

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
        closeTXAndExpectException( tx );
    }

    // RelationshipExplicitIndexGet
    @Test
    public void shouldFailOnOpenCursorForRelationshipExplicitIndexGetAtTXClose()
    {
        // given
        RelationshipExplicitIndexCursor cursor = (RelationshipExplicitIndexCursor) kernel.cursors().allocateRelationshipExplicitIndexCursor();
        Transaction tx = openTX();

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
        closeTXAndExpectException( tx );
    }

    // RelationshipExplicitIndexQuery
    @Test
    public void shouldFailOnOpenCursorForRelationshipExplicitIndexQueryAtTXClose()
    {
        // given
        RelationshipExplicitIndexCursor cursor = (RelationshipExplicitIndexCursor) kernel.cursors().allocateRelationshipExplicitIndexCursor();
        Transaction tx = openTX();

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
        closeTXAndExpectException( tx );
    }

    // RelationshipExplicitIndexQuery2
    @Test
    public void shouldFailOnOpenCursorForRelationshipExplicitIndexQuery2AtTXClose()
    {
        // given
        RelationshipExplicitIndexCursor cursor = (RelationshipExplicitIndexCursor) kernel.cursors().allocateRelationshipExplicitIndexCursor();
        Transaction tx = openTX();

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
        closeTXAndExpectException( tx );
    }

    private Transaction openTX()
    {
        try
        {
            return session.beginTransaction();
        }
        catch ( KernelException e )
        {
            fail();
            return null;
        }
    }

    private void closeTXAndExpectException( Transaction tx )
    {
        try
        {
            // when
            tx.close();

            // then (not expected)
            fail( "Should have failed because of open cursor" );
        }
        catch ( Exception e )
        {
            // then (expected)
        }
    }
}
