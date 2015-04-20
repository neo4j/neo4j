/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.integrationtest;

import org.junit.Test;
import org.neo4j.cursor.Cursor;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.DataWriteOperations;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.util.Cursors;
import org.neo4j.kernel.impl.util.register.NeoRegister;

import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static org.neo4j.graphdb.Direction.BOTH;
import static org.neo4j.graphdb.Direction.INCOMING;
import static org.neo4j.graphdb.Direction.OUTGOING;
import static org.neo4j.kernel.impl.util.Cursors.countDownCursor;
import static org.neo4j.kernel.impl.util.ExpandTestUtils.row;
import static org.neo4j.kernel.impl.util.ExpandTestUtils.rows;
import static org.neo4j.kernel.impl.util.register.NeoRegister.NodeRegister;
import static org.neo4j.kernel.impl.util.register.NeoRegister.RelationshipRegister;
import static org.neo4j.kernel.impl.util.register.NeoRegisters.newNodeRegister;
import static org.neo4j.kernel.impl.util.register.NeoRegisters.newRelTypeRegister;
import static org.neo4j.kernel.impl.util.register.NeoRegisters.newRelationshipRegister;
import static org.neo4j.register.Register.ObjectRegister;
import static org.neo4j.register.Registers.newObjectRegister;

public class ExpandIT extends KernelIntegrationTest
{
    private int relType1;
    private int relType2;

    @Test
    public void shouldTraverseBothDirections() throws Exception
    {
        // Given
        long nodeId = createGraph();

        ReadOperations ops = readOperationsInNewTransaction();
        RelationshipRegister relId = newRelationshipRegister();
        NodeRegister startId = newNodeRegister();
        NeoRegister.RelTypeRegister relType = newRelTypeRegister();
        ObjectRegister<Direction> direction = newObjectRegister();
        NodeRegister neighborId = newNodeRegister();

        // When
        Cursor cursor = ops.expand( countDownCursor( 1 ),
                newNodeRegister( nodeId ), newObjectRegister( new int[]{relType1} ),
                newObjectRegister( Direction.BOTH ), relId, relType, direction, startId, neighborId );

        // Then
        assertThat( rows( cursor, relId, relType, direction, startId, neighborId ),
           equalTo( asList(
                   row( 0, 0, BOTH, 0, 0), row( 1, 0, BOTH, 0, 0), row( 2, 0, OUTGOING, 0, 1),
                   row( 3, 0, OUTGOING, 0, 1), row( 4, 0, INCOMING, 0, 1), row( 5, 0, INCOMING, 0, 1),
                   row( 6, 0, OUTGOING, 0, 2), row( 7, 0, OUTGOING, 0, 2), row( 8, 0, INCOMING, 0, 2),
                   row( 9, 0, INCOMING, 0, 2)
           )) );
        assertFalse( "Should not contain any more rows.", cursor.next() );
    }

    @Test
    public void shouldTraverseIncoming() throws Exception
    {
        // Given
        long nodeId = createGraph();

        ReadOperations ops = readOperationsInNewTransaction();
        RelationshipRegister relId = newRelationshipRegister();
        NodeRegister startId = newNodeRegister();
        NeoRegister.RelTypeRegister relType = newRelTypeRegister();
        ObjectRegister<Direction> direction = newObjectRegister();
        NodeRegister neighborId = newNodeRegister();

        // When
        Cursor cursor = ops.expand( countDownCursor( 1 ),
                newNodeRegister( nodeId ), newObjectRegister( new int[]{relType1} ),
                newObjectRegister( INCOMING ), relId, relType, direction, startId, neighborId );

        // Then
        assertThat( rows( cursor, relId, relType, direction, startId, neighborId ),
                equalTo( asList(
                        row( 0, 0, BOTH, 0, 0), row( 1, 0, BOTH, 0, 0), row( 4, 0, INCOMING, 0, 1),
                        row( 5, 0, INCOMING, 0, 1), row( 8, 0, INCOMING, 0, 2), row( 9, 0, INCOMING, 0, 2)
                )) );
        assertFalse( "Should not contain any more rows.", cursor.next() );
    }

    @Test
    public void shouldTraverseOutgoingFromNodeCreatedInCurrentTx() throws Exception
    {
        // Given
        createGraph();
        DataWriteOperations ops = dataWriteOperationsInNewTransaction();

        long nodeId = ops.nodeCreate();
        ops.relationshipCreate( relType1, nodeId, ops.nodeCreate() );
        ops.relationshipDelete( ops.relationshipCreate( relType1, nodeId, ops.nodeCreate() ) ); // Shouldn't show up

        RelationshipRegister relId = newRelationshipRegister();
        NodeRegister startId = newNodeRegister();
        NeoRegister.RelTypeRegister relType = newRelTypeRegister();
        ObjectRegister<Direction> direction = newObjectRegister();
        NodeRegister neighborId = newNodeRegister();

        // When
        Cursor cursor = ops.expand( countDownCursor( 1 ),
                newNodeRegister( nodeId ), newObjectRegister( new int[]{relType1} ),
                newObjectRegister( OUTGOING ), relId, relType, direction, startId, neighborId );

        // Then
        assertThat( rows( cursor, relId, relType, direction, startId, neighborId ),
                equalTo( asList(
                        row( 12l, relType1, OUTGOING, nodeId, 6l )
                )) );
        assertFalse( "Should not contain any more rows.", cursor.next() );
    }

    @Test
    public void shouldIncludeTxLocalChangesOnIncoming() throws Exception
    {
        // Given
        long nodeId = createGraph();

        DataWriteOperations ops = dataWriteOperationsInNewTransaction();
        RelationshipRegister relId = newRelationshipRegister();
        NodeRegister startId = newNodeRegister();
        NeoRegister.RelTypeRegister relType = newRelTypeRegister();
        ObjectRegister<Direction> direction = newObjectRegister();
        NodeRegister neighborId = newNodeRegister();

        ops.relationshipDelete( 0l );
        ops.relationshipCreate( relType1, nodeId, ops.nodeCreate() );
        ops.relationshipCreate( relType1, ops.nodeCreate(), nodeId );

        // When
        Cursor cursor = ops.expand( countDownCursor( 1 ),
                newNodeRegister( nodeId ), newObjectRegister( new int[]{relType1} ),
                newObjectRegister( INCOMING ), relId, relType, direction, startId, neighborId );

        // Then
        assertThat( rows( cursor, relId, relType, direction, startId, neighborId ),
                equalTo( asList(
                        row( 1, relType1, BOTH, 0, 0), row( 4, relType1, INCOMING, 0, 1), row( 5, relType1, INCOMING, 0, 1),
                        row( 8, relType1, INCOMING, 0, 2), row( 9, relType1, INCOMING, 0, 2),
                        row( 13, relType1, INCOMING, nodeId, 6)
                )) );
        assertFalse( "Should not contain any more rows.", cursor.next() );
    }

    /**
     * This tests that we can give an input cursor that several input rows, and that the output cursor gives us
     * a single continuous stream of outputs from that.
     */
    @Test
    public void shouldAllowMultipleInputRowsWithLocalTxState() throws Exception
    {
        // Given
        final long nodeId = createGraph();

        DataWriteOperations ops = dataWriteOperationsInNewTransaction();
        RelationshipRegister relId = newRelationshipRegister();
        NodeRegister startId = newNodeRegister();
        NeoRegister.RelTypeRegister relType = newRelTypeRegister();
        ObjectRegister<Direction> direction = newObjectRegister();
        NodeRegister neighborId = newNodeRegister();

        ops.relationshipDelete( 0l );
        ops.relationshipCreate( relType1, nodeId, ops.nodeCreate() );
        ops.relationshipCreate( relType1, ops.nodeCreate(), nodeId );
        ops.relationshipCreate( relType2, nodeId, ops.nodeCreate() );

        final NodeRegister nodeRegister = newNodeRegister();
        final ObjectRegister<int[]> typesRegister = newObjectRegister();
        final ObjectRegister<Direction> directionRegister = newObjectRegister();

        // An input cursor that contains two rows
        Cursor inputCursor = new Cursor()
        {
            private int count = 0;

            @Override
            public boolean next()
            {
                switch ( count++ )
                {
                    case 0:
                        nodeRegister.write( nodeId );
                        typesRegister.write( new int[]{relType1} );
                        directionRegister.write( INCOMING );
                        return true;
                    case 1:
                        nodeRegister.write( nodeId );
                        typesRegister.write( new int[]{relType2} ); // Different rel type
                        directionRegister.write( OUTGOING );
                        return true;
                    default:
                        return false;
                }
            }

            @Override
            public void reset()
            {

            }

            @Override
            public void close()
            {

            }
        };

        // When
        Cursor cursor = ops.expand( inputCursor, nodeRegister, typesRegister, directionRegister, relId, relType, direction, startId, neighborId );

        // And it should contain this
        assertThat( rows( cursor, relId, relType, direction, startId, neighborId ),
                equalTo( asList(
                        // Note that this is not the order the rows are returned in, the rows are sorted for
                        // predictability in the test.
                        row( 1, 0, BOTH, 0, 0), row( 4, 0, INCOMING, 0, 1), row( 5, 0, INCOMING, 0, 1),
                        row( 8, 0, INCOMING, 0, 2), row( 9, 0, INCOMING, 0, 2), row( 10, 1, OUTGOING, 0, 3),
                        row( 11, 1, OUTGOING, 0, 4), row( 13, 0, INCOMING, 0, 6), row( 14, 1, OUTGOING, 0, 7)
                )) );
        assertFalse( "Should not contain any more rows.", cursor.next() );
    }

    /**
     * This tests that we can give an input cursor that several input rows, and that the output cursor gives us
     * a single continuous stream of outputs from that.
     */
    @Test
    public void shouldAllowMultipleInputRows() throws Exception
    {
        // Given
        final long nodeId = createGraph();

        // And given a node with no rels
        DataWriteOperations write = dataWriteOperationsInNewTransaction();
        final long emptyNodeId = write.nodeCreate();
        commit();

        ReadOperations ops = readOperationsInNewTransaction();
        RelationshipRegister relId = newRelationshipRegister();
        NodeRegister startId = newNodeRegister();
        NeoRegister.RelTypeRegister relType = newRelTypeRegister();
        ObjectRegister<Direction> direction = newObjectRegister();
        NodeRegister neighborId = newNodeRegister();

        final NodeRegister nodeRegister = newNodeRegister();
        final ObjectRegister<int[]> typesRegister = newObjectRegister();
        final ObjectRegister<Direction> directionRegister = newObjectRegister();

        // An input cursor that contains two rows
        Cursor inputCursor = new Cursor()
        {
            private int count = 0;

            @Override
            public boolean next()
            {
                switch ( count++ )
                {
                    case 0:
                        nodeRegister.write( nodeId );
                        typesRegister.write( new int[]{relType1} );
                        directionRegister.write( INCOMING );
                        return true;
                    case 1:
                        nodeRegister.write(emptyNodeId);
                        typesRegister.write( new int[]{relType1} );
                        directionRegister.write( INCOMING );
                        return true;
                    case 2:
                        nodeRegister.write( emptyNodeId + 999 ); // Does not exist
                        typesRegister.write( new int[]{relType1} );
                        directionRegister.write( INCOMING );
                        return true;
                    case 3:
                        nodeRegister.write( nodeId );
                        typesRegister.write( new int[]{relType2} ); // Different rel type
                        directionRegister.write( OUTGOING );
                        return true;
                    default:
                        return false;
                }
            }

            @Override
            public void reset()
            {

            }

            @Override
            public void close()
            {

            }
        };

        // When
        Cursor cursor = ops.expand( inputCursor, nodeRegister, typesRegister, directionRegister, relId, relType, direction, startId, neighborId );

        // And it should contain this
        assertThat( rows( cursor, relId, relType, direction, startId, neighborId ),
                equalTo( asList(
                        // Note that this is not the order the rows are returned in, the rows are sorted for
                        // predictability in the test.
                        row( 0, 0, BOTH, 0, 0), row( 1, 0, BOTH, 0, 0), row( 4, 0, INCOMING, 0, 1),
                        row( 5, 0, INCOMING, 0, 1), row( 8, 0, INCOMING, 0, 2), row( 9, 0, INCOMING, 0, 2),
                        row( 10, 1, OUTGOING, 0, 3), row( 11, 1, OUTGOING, 0, 4)
                )) );
        assertFalse( "Should not contain any more rows.", cursor.next() );
    }

    @Test
    public void resetAndCloseShouldPropagateWithLocalTxState() throws Exception
    {
        // Given
        long nodeId = createGraph();

        DataWriteOperations ops = dataWriteOperationsInNewTransaction();
        RelationshipRegister relId = newRelationshipRegister();
        NodeRegister startId = newNodeRegister();
        NeoRegister.RelTypeRegister relType = newRelTypeRegister();
        ObjectRegister<Direction> direction = newObjectRegister();
        NodeRegister neighborId = newNodeRegister();

        ops.relationshipDelete( 0l );
        ops.relationshipCreate( relType1, nodeId, ops.nodeCreate() );
        ops.relationshipCreate( relType1, ops.nodeCreate(), nodeId );

        Cursor inputCursor = spy( new Cursors.CountDownCursor( 1 ));

        Cursor cursor = ops.expand( inputCursor,
                newNodeRegister( nodeId ), newObjectRegister( new int[]{relType1} ),
                newObjectRegister( INCOMING ), relId, relType, direction, startId, neighborId );

        // And given I've exhausted the cursor
        while(cursor.next());

        // When
        cursor.reset();

        // Then
        verify(inputCursor).reset();
        assertThat( rows( cursor, relId, relType, direction, startId, neighborId ),
                equalTo( asList(
                        row( 1l, relType1, BOTH, nodeId, 0l ), row( 4l, relType1, INCOMING, nodeId, 1l ),
                        row( 5l, relType1, INCOMING, nodeId, 1l ), row( 8l, relType1, INCOMING, nodeId, 2l ),
                        row( 9l, relType1, INCOMING, nodeId, 2l ), row( 13l, relType1, INCOMING, nodeId, 6l )
                )) );
        assertFalse( "Should not contain any more rows.", cursor.next() );

        // And When
        cursor.close();

        // Then
        verify(inputCursor).close();
    }

    @Test
    public void resetAndCloseShouldPropagate() throws Exception
    {
        // Given
        long nodeId = createGraph();

        ReadOperations ops = readOperationsInNewTransaction();
        RelationshipRegister relId = newRelationshipRegister();
        NodeRegister startId = newNodeRegister();
        NeoRegister.RelTypeRegister relType = newRelTypeRegister();
        ObjectRegister<Direction> direction = newObjectRegister();
        NodeRegister neighborId = newNodeRegister();

        Cursor inputCursor = spy( new Cursors.CountDownCursor( 1 ));
        Cursor cursor = ops.expand( inputCursor,
                newNodeRegister( nodeId ), newObjectRegister( new int[]{relType1} ),
                newObjectRegister( INCOMING ), relId, relType, direction, startId, neighborId );

        // And given I've exhausted the cursor
        while(cursor.next());

        // When
        cursor.reset();

        // Then
        verify(inputCursor).reset();
        assertThat( rows( cursor, relId, relType, direction, startId, neighborId ),
                equalTo( asList(
                        row( 0l, relType1, BOTH, nodeId, 0l ), row( 1l, relType1, BOTH, nodeId, 0l ),
                        row( 4l, relType1, INCOMING, nodeId, 1l ), row( 5l, relType1, INCOMING, nodeId, 1l ),
                        row( 8l, relType1, INCOMING, nodeId, 2l ), row( 9l, relType1, INCOMING, nodeId, 2l )
                )) );
        assertFalse( "Should not contain any more rows.", cursor.next() );

        // And When
        cursor.close();

        // Then
        verify(inputCursor).close();
    }

    private long createGraph() throws KernelException
    {
        long nodeId;
        {
            DataWriteOperations ops = dataWriteOperationsInNewTransaction();
            relType1 = ops.relationshipTypeGetOrCreateForName( "TYPE1" );
            relType2 = ops.relationshipTypeGetOrCreateForName( "TYPE2" );
            nodeId = ops.nodeCreate();

            // Two loop rels
            ops.relationshipCreate( relType1, nodeId, nodeId );
            ops.relationshipCreate( relType1, nodeId, nodeId );

            // 2 * 4 "regular" rels
            for ( int i = 0; i < 2; i++ )
            {
                long target = ops.nodeCreate();

                // 2 outgoing
                ops.relationshipCreate( relType1, nodeId, target );
                ops.relationshipCreate( relType1, nodeId, target );

                // 2 incoming
                ops.relationshipCreate( relType1, target, nodeId );
                ops.relationshipCreate( relType1, target, nodeId );
            }

            // 2 outgoing reltype2
            ops.relationshipCreate( relType2, nodeId, ops.nodeCreate() );
            ops.relationshipCreate( relType2, nodeId, ops.nodeCreate() );

            commit();
        }
        return nodeId;
    }

}
