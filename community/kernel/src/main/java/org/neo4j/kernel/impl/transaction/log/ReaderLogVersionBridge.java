/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.kernel.impl.transaction.log;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.neo4j.kernel.impl.transaction.log.entry.IncompleteLogHeaderException;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;

/**
 * {@link LogVersionBridge} naturally transitioning from one {@link LogVersionedStoreChannel} to the next,
 * i.e. to log version with one higher version than the current.
 */
public class ReaderLogVersionBridge implements LogVersionBridge
{
    private final LogFiles logFiles;

    public ReaderLogVersionBridge( LogFiles logFiles )
    {
        this.logFiles = logFiles;
    }

    @Override
    public LogVersionedStoreChannel next( LogVersionedStoreChannel channel ) throws IOException
    {
        PhysicalLogVersionedStoreChannel nextChannel;
        try
        {
            nextChannel = logFiles.openForVersion( channel.getVersion() + 1 );
        }
        catch ( FileNotFoundException | IncompleteLogHeaderException e )
        {
            // See PhysicalLogFile#rotate() for description as to why these exceptions are OK
            return channel;
        }
        channel.close();
        return nextChannel;
    }
}
