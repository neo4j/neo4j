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
package org.neo4j.kernel.impl.api.state;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import com.sun.xml.internal.xsom.impl.scd.Iterators;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.cursor.Cursor;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.txstate.ReadableTxState;
import org.neo4j.kernel.impl.api.RelationshipVisitor;
import org.neo4j.kernel.impl.api.store.StoreReadLayer;
import org.neo4j.kernel.impl.util.Cursors;
import org.neo4j.kernel.impl.util.register.NeoRegister;
import org.neo4j.kernel.impl.util.register.NeoRegisters;
import org.neo4j.register.Register;

import static java.util.Arrays.asList;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.neo4j.graphdb.Direction.INCOMING;
import static org.neo4j.kernel.impl.util.ExpandTestUtils.Row;
import static org.neo4j.kernel.impl.util.ExpandTestUtils.row;
import static org.neo4j.kernel.impl.util.ExpandTestUtils.rows;
import static org.neo4j.kernel.impl.util.register.NeoRegisters.newNodeRegister;
import static org.neo4j.register.Registers.newObjectRegister;

public class AugmentWithLocalStateExpandCursorTest
{

    private final NeoRegister.NodeRegister nodeId = newNodeRegister(1);
    private final Register.ObjectRegister<int[]> relTypes = newObjectRegister(new int[]{1});
    private final Register.ObjectRegister<Direction> expandDirection = newObjectRegister(INCOMING);

    private final NeoRegister.RelationshipRegister relId = NeoRegisters.newRelationshipRegister();
    private final NeoRegister.RelTypeRegister relType = NeoRegisters.newRelTypeRegister();
    private final Register.ObjectRegister<Direction> direction = newObjectRegister();
    private final NeoRegister.NodeRegister startNodeId = newNodeRegister();
    private final NeoRegister.NodeRegister neighborNodeId = newNodeRegister();

    @Test
    public void shouldRemoveRemovedRels() throws Exception
    {
        // Given
        StoreReadLayer store = mock( StoreReadLayer.class );
        ReadableTxState txState = mock( ReadableTxState.class );

        when(txState.relationshipIsDeletedInThisTx( 3l )).thenReturn( true );

        givenStoredRels( store,
                row(3, 2, INCOMING, 1, 1),
                row(4, 2, INCOMING, 1, 1));

        // When
        AugmentWithLocalStateExpandCursor cursor = newAugmentingExpandCursor( store, txState );

        // Then
        assertThat(rows(cursor, relId, relType, direction, startNodeId, neighborNodeId), equalTo(
                asList(row(4, 2, INCOMING, 1, 1)) ));
    }

    @Test
    public void shouldMixInAddedRels() throws Exception
    {
        // Given
        StoreReadLayer store = mock( StoreReadLayer.class );
        ReadableTxState txState = mock( ReadableTxState.class );
        long node = nodeId.read();

        givenTxRels( txState, node,
                row( 1337, 2, INCOMING, node, 3 ),
                row( 1338, 2, INCOMING, node, 3 ));

        givenStoredRels( store,
                row( 2, 2, INCOMING, node, 2 ),
                row( 3, 2, INCOMING, node, 2 ) );

        // When
        AugmentWithLocalStateExpandCursor cursor = newAugmentingExpandCursor( store, txState );

        // Then
        assertThat(rows(cursor, relId, relType, direction, startNodeId, neighborNodeId), equalTo( asList(
                row( 2, 2, INCOMING, 1, 2),
                row( 3, 2, INCOMING, 1, 2),
                row( 1337, 2, INCOMING, 1, 3),
                row( 1338, 2, INCOMING, 1, 3)) ));
    }

    @Test
    public void shouldIterateTxOnlyNodes() throws Exception
    {
        // Given
        StoreReadLayer store = mock( StoreReadLayer.class );
        ReadableTxState txState = mock( ReadableTxState.class );
        long node = nodeId.read();

        givenTxRels( txState, node,
                row( 1337, 2, INCOMING, node, 3 ),
                row( 1338, 2, INCOMING, node, 3 ));

        givenStoredRels( store /* none */ );
        when(txState.nodeIsAddedInThisTx( node )).thenReturn( true );

        // When
        AugmentWithLocalStateExpandCursor cursor = newAugmentingExpandCursor( store, txState );

        // Then
        assertThat(rows(cursor, relId, relType, direction, startNodeId, neighborNodeId), equalTo( asList(
                row( 1337, 2, INCOMING, 1, 3),
                row( 1338, 2, INCOMING, 1, 3)) ));
    }

    private AugmentWithLocalStateExpandCursor newAugmentingExpandCursor( StoreReadLayer store, ReadableTxState txState )
    {
        return new AugmentWithLocalStateExpandCursor( store, txState, Cursors.countDownCursor( 1 ), nodeId, relTypes, expandDirection, relId, relType, direction, startNodeId, neighborNodeId);
    }

    private void givenTxRels( ReadableTxState state, long nodeId, Row ... rows) throws Exception
    {
        Set<Long> relIds = new HashSet<>();
        for ( Row row : rows )
        {
            relIds.add( row.relId );
            final Row r = row;
            when(state.relationshipVisit( eq(row.relId), any( RelationshipVisitor.class ) )).thenAnswer( new Answer<Object>()
            {
                @Override
                public Object answer( InvocationOnMock invocation ) throws Throwable
                {
                    if(r.direction == INCOMING)
                    {
                        ((RelationshipVisitor) invocation.getArguments()[1]).visit( r.relId, r.type, r.neighborId, r.startId );
                    }
                    else
                    {
                        ((RelationshipVisitor) invocation.getArguments()[1]).visit( r.relId, r.type, r.startId, r.neighborId );
                    }
                    return true;
                }
            });
        }


        when(state.addedRelationships(nodeId, relTypes.read(), expandDirection.read() )).thenReturn( PrimitiveLongCollections.iterator( 1337, 1338 ) );
    }

    private void givenStoredRels( StoreReadLayer store, final Row... rows  )
    {
        when(store.expand( any(Cursor.class), any( NeoRegister.Node.In.class ), any(Register.Object.In.class), any(Register.Object.In.class),
                any( NeoRegister.Relationship.Out.class ), any( NeoRegister.RelType.Out.class ), any(Register.Object.Out.class),
                any( NeoRegister.Node.Out.class), any(NeoRegister.Node.Out.class) )).thenAnswer( new Answer<Cursor>()

        {
            @Override
            public Cursor answer( InvocationOnMock invocation ) throws Throwable
            {
                Object[] args = invocation.getArguments();
                final Cursor input = (Cursor) args[0];
                final NeoRegister.Relationship.Out relId = (NeoRegister.Relationship.Out) args[4];
                final NeoRegister.RelType.Out relType = (NeoRegister.RelType.Out) args[5];
                final Register.Object.Out<Direction> direction = (Register.Object.Out<Direction>) args[6];
                final NeoRegister.Node.Out startNodeId = (NeoRegister.Node.Out) args[7];
                final NeoRegister.Node.Out neighborNodeId = (NeoRegister.Node.Out) args[8];

                return new Cursor()
                {
                    private Iterator<Row> rowIter = Iterators.empty();

                    @Override
                    public boolean next()
                    {
                        if(rowIter.hasNext())
                        {
                            Row next = rowIter.next();
                            relId.write( next.relId );
                            relType.write( next.type );
                            direction.write( next.direction );
                            startNodeId.write( next.startId );
                            neighborNodeId.write( next.neighborId );
                            return true;
                        }
                        else
                        {
                            if(input.next())
                            {
                                rowIter = asList(rows).iterator();
                                return next();
                            }
                            else
                            {
                                return false;
                            }
                        }
                    }

                    @Override
                    public void reset()
                    {
                        rowIter = Iterators.empty();
                    }

                    @Override
                    public void close()
                    {

                    }
                };
            }
        });
    }
}
