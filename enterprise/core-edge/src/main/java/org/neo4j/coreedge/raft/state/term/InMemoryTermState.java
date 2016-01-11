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
package org.neo4j.coreedge.raft.state.term;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

import org.neo4j.coreedge.raft.state.membership.Marshal;

public class InMemoryTermState implements TermState
{
    public static final int TERM_BYTES = 8;

    private long term = 0;

    public InMemoryTermState()
    {
    }

    public InMemoryTermState( InMemoryTermState inMemoryTermState )
    {
        this.term = inMemoryTermState.term;
    }

    private InMemoryTermState( long term )
    {
        this.term = term;
    }

    @Override
    public long currentTerm()
    {
        return term;
    }

    @Override
    public void update( long newTerm )
    {
        failIfInvalid( newTerm );
        term = newTerm;
    }

    public void failIfInvalid( long newTerm )
    {
        if ( newTerm < term )
        {
            throw new IllegalArgumentException( "Cannot move to a lower term" );
        }
    }

    public static class InMemoryTermStateMarshal implements Marshal<InMemoryTermState>
    {
        @Override
        public void marshal( InMemoryTermState inMemoryTermState, ByteBuffer buffer )
        {
            buffer.putLong( inMemoryTermState.currentTerm() );
        }

        @Override
        public InMemoryTermState unmarshal( ByteBuffer source )
        {
            try
            {
                return new InMemoryTermState( source.getLong() );
            }
            catch ( BufferUnderflowException ex )
            {
                return null;
            }
        }
    }
}