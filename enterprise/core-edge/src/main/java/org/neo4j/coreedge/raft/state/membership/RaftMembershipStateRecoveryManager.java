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
package org.neo4j.coreedge.raft.state.membership;

import java.io.File;
import java.io.IOException;

import org.neo4j.coreedge.raft.state.StateRecoveryManager;
import org.neo4j.coreedge.raft.state.membership.InMemoryRaftMembershipState.InMemoryRaftMembershipStateChannelMarshal;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.log.ReadAheadChannel;
import org.neo4j.storageengine.api.ReadableChannel;

public class RaftMembershipStateRecoveryManager<MEMBER> extends StateRecoveryManager
{
    private final InMemoryRaftMembershipStateChannelMarshal<MEMBER> marshal;

    public RaftMembershipStateRecoveryManager( FileSystemAbstraction fileSystem,
                                               InMemoryRaftMembershipStateChannelMarshal<MEMBER> marshal )
    {
        super( fileSystem );
        this.marshal = marshal;
    }

    @Override
    protected long getOrdinalOfLastRecord( File file ) throws IOException
    {
        return readLastEntryFrom( file ).logIndex();
    }

    public InMemoryRaftMembershipState<MEMBER> readLastEntryFrom( File file ) throws IOException
    {
        final ReadableChannel temporaryStoreChannel = new ReadAheadChannel<>( fileSystem.open( file, "rw" ) );

        InMemoryRaftMembershipState<MEMBER> result = new InMemoryRaftMembershipState<>();
        InMemoryRaftMembershipState<MEMBER> lastRead;

        while ( (lastRead = marshal.unmarshal( temporaryStoreChannel )) != null )
        {
            result = lastRead;
        }

        return result;
    }
}
