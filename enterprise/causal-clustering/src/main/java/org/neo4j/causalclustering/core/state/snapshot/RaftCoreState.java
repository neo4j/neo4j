/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
