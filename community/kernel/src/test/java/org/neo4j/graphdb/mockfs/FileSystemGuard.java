/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.graphdb.mockfs;

import java.io.File;
import java.io.IOException;

/**
 * A FileSystemGuard is a callback called by BreakingFileSystemAbstraction on various operations of the file channels
 * exposed by it. It is expected that a Guard will simulate failures at certain points on filesystem operations so tests
 * can be written around those.
 */
public interface FileSystemGuard
{
    public enum OperationType
    {
        WRITE, READ;
    }

    /**
     * Called back on file channel operations. Expect this to change as it is used more.
     * @param operationType The type of operation performed.
     * @param onFile The filename on which the operation happened
     * @param bytesWrittenTotal The total bytes written in this channel so far.
     * @param bytesWrittenThisCall The number of bytes written during this call.
     * @param channelPosition The current position of the file channel
     * @throws IOException If the implementation chooses so.
     */
    void checkOperation( OperationType operationType, File onFile,
                         int bytesWrittenTotal, int bytesWrittenThisCall, long channelPosition ) throws IOException;
}
