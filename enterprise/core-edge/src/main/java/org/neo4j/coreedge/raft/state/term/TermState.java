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

import java.io.IOException;

import org.neo4j.coreedge.raft.state.StateMarshal;
import org.neo4j.storageengine.api.ReadPastEndException;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;

public class TermState
{
    private long term = 0;

    public TermState()
    {
    }

    private TermState( long term )
    {
        this.term = term;
    }

    public long currentTerm()
    {
        return term;
    }

    /**
     * Updates the term to a new value. This value is generally expected, but not required, to be persisted. Consecutive
     * calls to this method should always have monotonically increasing arguments, thus maintaining the raft invariant
     * that the term is always non-decreasing. {@link IllegalArgumentException} can be thrown if an invalid value is
     * passed as argument.
     *
     * @param newTerm The new value.
     */
    public void update( long newTerm )
    {
        failIfInvalid( newTerm );
        term = newTerm;
    }

    /**
     * This method implements the invariant of this class, that term never transitions to lower values. If
     * newTerm is lower than the term already stored in this class, it will throw an
     * {@link IllegalArgumentException}.
     */
    public void failIfInvalid( long newTerm )
    {
        if ( newTerm < term )
        {
            throw new IllegalArgumentException( "Cannot move to a lower term" );
        }
    }

    public static class Marshal implements StateMarshal<TermState>
    {
        @Override
        public void marshal( TermState termState, WritableChannel channel ) throws IOException
        {
            channel.putLong( termState.currentTerm() );
        }

        @Override
        public TermState unmarshal( ReadableChannel source ) throws IOException
        {
            try
            {
                return new TermState( source.getLong() );
            }
            catch ( ReadPastEndException ex )
            {
                return null;
            }
        }

        @Override
        public TermState startState()
        {
            return new TermState();
        }

        @Override
        public long ordinal( TermState state )
        {
            return state.currentTerm();
        }
    }
}