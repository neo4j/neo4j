/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.causalclustering.core.state.snapshot;

import java.io.IOException;

import org.neo4j.causalclustering.core.consensus.membership.MembershipEntry;
import org.neo4j.causalclustering.core.state.storage.SafeStateMarshal;
import org.neo4j.causalclustering.messaging.EndOfStreamException;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;

public class RaftCoreState
{
    private MembershipEntry committed;

    public RaftCoreState( MembershipEntry committed )
    {
        this.committed = committed;
    }

    public MembershipEntry committed()
    {
        return committed;
    }

    public static class Marshal extends SafeStateMarshal<RaftCoreState>
    {
        private static MembershipEntry.Marshal membershipMarshal = new MembershipEntry.Marshal();

        @Override
        public RaftCoreState startState()
        {
            return null;
        }

        @Override
        public long ordinal( RaftCoreState raftCoreState )
        {
            return 0;
        }

        @Override
        public void marshal( RaftCoreState raftCoreState, WritableChannel channel ) throws IOException
        {

            membershipMarshal.marshal( raftCoreState.committed(), channel );
        }

        @Override
        protected RaftCoreState unmarshal0( ReadableChannel channel ) throws IOException, EndOfStreamException
        {
            return new RaftCoreState( membershipMarshal.unmarshal( channel ) );
        }
    }

    @Override
    public String toString()
    {
        return "RaftCoreState{" +
               "committed=" + committed +
               '}';
    }
}
