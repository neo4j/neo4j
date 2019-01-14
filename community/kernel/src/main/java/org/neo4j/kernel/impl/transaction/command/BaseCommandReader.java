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
package org.neo4j.kernel.impl.transaction.command;

import java.io.IOException;

import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageEngine;
import org.neo4j.kernel.impl.transaction.log.LogPositionMarker;
import org.neo4j.kernel.impl.transaction.log.PositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.ReadableClosableChannel;
import org.neo4j.storageengine.api.CommandReader;
import org.neo4j.storageengine.api.ReadableChannel;

/**
 * Basic functionality for {@link CommandReader} for {@link RecordStorageEngine}.
 */
public abstract class BaseCommandReader implements CommandReader
{
    /**
     * Handles format back to 1.9 where the command format didn't have a version.
     */
    @Override
    public final Command read( ReadableChannel channel ) throws IOException
    {
        byte commandType;
        do
        {
            commandType = channel.get();
        }
        while ( commandType == NeoCommandType.NONE );

        return read( commandType, channel );
    }

    /**
     * Reads the next {@link Command} from {@code channel}.
     *
     * @param commandType type of command to read, f.ex. node command, relationship command a.s.o.
     * @param channel     {@link ReadableClosableChannel} to read from.
     * @return {@link Command} or {@code null} if end reached.
     * @throws IOException if channel throws exception.
     */
    protected abstract Command read( byte commandType, ReadableChannel channel ) throws IOException;

    protected IOException unknownCommandType( byte commandType, ReadableChannel channel ) throws IOException
    {
        String message = "Unknown command type[" + commandType + "]";
        if ( channel instanceof PositionAwareChannel )
        {
            PositionAwareChannel logChannel = (PositionAwareChannel) channel;
            LogPositionMarker position = new LogPositionMarker();
            logChannel.getCurrentPosition( position );
            message += " near " + position.newPosition();
        }
        return new IOException( message );
    }
}
