/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.log;

import java.io.File;
import java.io.IOException;

/**
 * Sees a log file as bytes, including taking care of rotation of the file into optimal chunks.
 */
public interface LogFile
{
    interface LogFileVisitor
    {
        boolean visit( LogPosition position, ReadableVersionableLogChannel channel ) throws IOException;
    }

    /**
     * @return {@link WritableLogChannel} capable of appending data to this log.
     */
    WritableLogChannel getWriter();

    /**
     * @param position {@link LogPosition} to position the returned reader at.
     * @return {@link ReadableLogChannel} capable of reading log data, starting from {@link LogPosition position}.
     * @throws IOException
     */
    ReadableVersionableLogChannel getReader( LogPosition position ) throws IOException;

    void accept( LogFileVisitor visitor, LogPosition startingFromPosition ) throws IOException;

    void accept( LogHeaderVisitor visitor ) throws IOException;

    /**
     * @return {@code true} if a rotation is needed.
     */
    boolean rotationNeeded() throws IOException;

    void rotate() throws IOException;

    File currentLogFile();

    long currentLogVersion();
}
