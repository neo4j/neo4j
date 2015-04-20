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
package org.neo4j.kernel.impl.api.store;

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.impl.api.RelationshipVisitor;
import org.neo4j.kernel.impl.util.Cursors;
import org.neo4j.kernel.impl.util.register.NeoRegister;
import org.neo4j.register.Register;
import org.neo4j.register.Registers;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.*;
import static org.neo4j.kernel.impl.util.register.NeoRegisters.newNodeRegister;
import static org.neo4j.kernel.impl.util.register.NeoRegisters.newRelTypeRegister;
import static org.neo4j.kernel.impl.util.register.NeoRegisters.newRelationshipRegister;
import static org.neo4j.register.Registers.newObjectRegister;

public class StoreExpandCursorTest
{
    @Test
    public void shouldDelegateToGetRelsAndRelVisitor() throws Exception
    {
        // Given
        PrimitiveLongIterator rels = PrimitiveLongCollections.iterator( 1 );

        CacheLayer cache = mock(CacheLayer.class);
        when(cache.nodeListRelationships( anyLong(), any(Direction.class), any(int[].class) )).thenReturn( rels );

        doAnswer(  new Answer<Object>()
        {
            @Override
            public Object answer( InvocationOnMock invocation ) throws Throwable
            {
                RelationshipVisitor visitor = (RelationshipVisitor) invocation.getArguments()[1];
                visitor.visit( (Long)invocation.getArguments()[0], 0, 1337, 2 );
                return null;
            }
        }  ).when(cache).relationshipVisit( anyLong(), any(RelationshipVisitor.class) );

        // IO registers we'll need
        NeoRegister.RelationshipRegister relId = newRelationshipRegister();
        NeoRegister.NodeRegister startNodeId = newNodeRegister();
        NeoRegister.NodeRegister neighborNodeId = newNodeRegister();
        NeoRegister.RelTypeRegister relType = newRelTypeRegister();
        Register.ObjectRegister<Direction> direction = Registers.newObjectRegister();

        StoreExpandCursor cursor = new StoreExpandCursor( cache, Cursors.countDownCursor( 1 ),
                newNodeRegister(1337l), newObjectRegister( new int[]{1, 2, 3} ),
                newObjectRegister( Direction.BOTH ), relId,
                relType, direction, startNodeId, neighborNodeId );

        // When
        cursor.next();

        // Then
        verify( cache ).nodeListRelationships( 1337l, Direction.BOTH, new int[]{1, 2, 3} );
        assertThat( relId.read(), equalTo(1l) );
        assertThat( startNodeId.read(), equalTo(1337l) );
        assertThat( neighborNodeId.read(), equalTo(2l) );
        assertThat( relType.read(), equalTo(0) );
        assertThat( direction.read(), equalTo(Direction.OUTGOING));

        assertFalse( cursor.next() );
    }
}
