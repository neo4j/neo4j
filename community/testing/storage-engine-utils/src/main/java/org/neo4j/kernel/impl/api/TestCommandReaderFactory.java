/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.kernel.impl.api;

import java.io.IOException;

import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.storageengine.api.CommandReader;
import org.neo4j.storageengine.api.CommandReaderFactory;
import org.neo4j.storageengine.api.StorageCommand;

public class TestCommandReaderFactory implements CommandReaderFactory
{
    private static final TestCommandReader READER = new TestCommandReader();

    @Override
    public CommandReader get( int logEntryVersion )
    {
        // At the time of writing this the act of plugging in and selecting commands and readers from different storages
        // doesn't work and it's always going to be the latest record-storage format version which LogEntryWriter
        // writes into the header. So this instance should be used when it's known that the TestCommand command is used
        // when serializing commands. And yeah... ignore the formatId.
        return READER;
    }

    private static class TestCommandReader implements CommandReader
    {
        @Override
        public StorageCommand read( ReadableChannel channel ) throws IOException
        {
            int length = channel.getInt();
            byte[] bytes = new byte[length];
            channel.get( bytes, length );
            return new TestCommand( bytes );
        }
    }
}
