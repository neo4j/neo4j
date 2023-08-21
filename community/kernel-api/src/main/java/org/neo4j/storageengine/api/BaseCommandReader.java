/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.storageengine.api;

import java.io.IOException;
import org.neo4j.io.fs.ReadableChannel;

/**
 * Basic functionality for {@link CommandReader} for {@link StorageEngine}.
 */
public abstract class BaseCommandReader implements CommandReader {
    /**
     * Handles format back to 1.9 where the command format didn't have a version.
     */
    @Override
    public final StorageCommand read(ReadableChannel channel) throws IOException {
        byte commandType;
        do {
            commandType = channel.get();
        } while (commandType == CommandReader.NONE);

        return read(commandType, channel);
    }

    /**
     * Reads the next {@link StorageCommand} from {@code channel}.
     *
     * @param commandType type of command to read, f.ex. node command, relationship command a.s.o.
     * @param channel     {@link ReadableChannel} to read from.
     * @return {@link StorageCommand} or {@code null} if end reached.
     * @throws IOException if channel throws exception.
     */
    public abstract StorageCommand read(byte commandType, ReadableChannel channel) throws IOException;
}
