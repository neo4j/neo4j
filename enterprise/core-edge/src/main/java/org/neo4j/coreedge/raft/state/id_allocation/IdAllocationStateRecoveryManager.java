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

import java.io.File;
import java.io.IOException;

import org.neo4j.coreedge.raft.state.StateRecoveryManager;
import org.neo4j.coreedge.raft.state.id_allocation.InMemoryIdAllocationState.InMemoryIdAllocationStateChannelMarshal;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.log.ReadAheadChannel;
import org.neo4j.storageengine.api.ReadableChannel;

public class IdAllocationStateRecoveryManager extends StateRecoveryManager
{
    private final InMemoryIdAllocationStateChannelMarshal marshal;

    public IdAllocationStateRecoveryManager( FileSystemAbstraction fileSystem,
                                             InMemoryIdAllocationStateChannelMarshal marshal )
    {
        super( fileSystem );
        this.marshal = marshal;
    }

    @Override
    protected long getOrdinalOfLastRecord( File file ) throws IOException
    {
        return readLastEntryFrom( fileSystem, file ).logIndex();
    }

    public InMemoryIdAllocationState readLastEntryFrom( FileSystemAbstraction fileSystemAbstraction, File file )
            throws IOException
    {
        final ReadableChannel channel = new ReadAheadChannel<>( fileSystemAbstraction.open( file, "r" ) );

        InMemoryIdAllocationState result = new InMemoryIdAllocationState();
        InMemoryIdAllocationState lastRead;

        while ( (lastRead = marshal.unmarshal( channel)) != null )
        {
            result = lastRead;
        }

        return result;
    }
}
