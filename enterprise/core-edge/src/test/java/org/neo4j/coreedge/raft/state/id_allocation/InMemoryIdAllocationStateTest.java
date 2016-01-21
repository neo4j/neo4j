/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.coreedge.raft.state.id_allocation;

import org.junit.Test;

import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.kernel.impl.transaction.log.InMemoryVersionableReadableClosablePositionAwareChannel;

import static org.junit.Assert.assertEquals;

public class InMemoryIdAllocationStateTest
{
    @Test
    public void shouldRoundtripToChannel() throws Exception
    {
        // given
        final InMemoryIdAllocationState state = new InMemoryIdAllocationState();

        for ( int i = 1; i <= 3; i++ )
        {
            state.firstUnallocated( IdType.NODE, 1024 * i );
            state.logIndex( i );
        }

        final InMemoryIdAllocationState.InMemoryIdAllocationStateChannelMarshal marshal = new InMemoryIdAllocationState
                .InMemoryIdAllocationStateChannelMarshal();
        // when
        InMemoryVersionableReadableClosablePositionAwareChannel channel = new
                InMemoryVersionableReadableClosablePositionAwareChannel();
        marshal.marshal( state, channel );
        InMemoryIdAllocationState unmarshalled = marshal.unmarshal( channel );

        // then
        assertEquals( state, unmarshalled );
    }

    @Test
    public void shouldReturnNullForHalfWrittenEntries() throws Exception
    {
        // given
        final InMemoryIdAllocationState state = new InMemoryIdAllocationState();

        for ( int i = 1; i <= 3; i++ )
        {
            state.firstUnallocated( IdType.NODE, 1024 * i );
            state.logIndex( i );
        }

        final InMemoryIdAllocationState.InMemoryIdAllocationStateChannelMarshal marshal = new InMemoryIdAllocationState
                .InMemoryIdAllocationStateChannelMarshal();
        // when
        InMemoryVersionableReadableClosablePositionAwareChannel channel = new
                InMemoryVersionableReadableClosablePositionAwareChannel();
        marshal.marshal( state, channel );
        // append some garbage
        channel.putInt( 1 ).putInt( 2 ).putInt( 3 ).putLong( 4L );
        // read back in the first one
        marshal.unmarshal( channel );
        // the second one will be half read (the ints and longs appended above). Result should be null
        InMemoryIdAllocationState unmarshalled = marshal.unmarshal( channel );

        // then
        // the result should be null (and not a half read entry or any exception)
        assertEquals( null, unmarshalled );
    }
}
