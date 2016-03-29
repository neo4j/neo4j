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
package org.neo4j.coreedge.raft.log.physical;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.log.LogVersionBridge;
import org.neo4j.kernel.impl.transaction.log.LogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogVersionedStoreChannel;

import static org.neo4j.coreedge.raft.log.physical.PhysicalRaftLogFile.openForVersion;

public class ReaderRaftLogVersionBridge implements LogVersionBridge
{
    private final FileSystemAbstraction fileSystem;
    private final PhysicalRaftLogFiles logFiles;

    public ReaderRaftLogVersionBridge( FileSystemAbstraction fileSystem, PhysicalRaftLogFiles logFiles )
    {
        this.fileSystem = fileSystem;
        this.logFiles = logFiles;
    }

    @Override
    public LogVersionedStoreChannel next( LogVersionedStoreChannel channel ) throws IOException
    {
        PhysicalLogVersionedStoreChannel nextChannel;
        try
        {
            nextChannel = openForVersion( logFiles, fileSystem, channel.getVersion() + 1 );
        }
        catch ( FileNotFoundException e )
        {
            return channel;
        }
        channel.close();
        return nextChannel;
    }
}
