/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.xaframework;

import java.io.IOException;

import org.neo4j.helpers.collection.Visitor;

/**
 * Sees a log file as bytes, including taking care of rotation of the file into optimal chunks.
 * In order to get hold of {@link #getReader(LogPosition) readers} and {@link #getWriter() writer},
 * {@link #open()} needs to be called first.
 */
public interface LogFile
{
    /**
     * Opens the log file to make readers and writers available.
     */
    void open( Visitor<ReadableLogChannel, IOException> recoveredDataVisitor ) throws IOException;

    /**
     * @return {@link WritableLogChannel} capable of appending data to this log.
     */
    WritableLogChannel getWriter();

    /**
     * @param position {@link LogPosition} to position the returned reader at.
     * @return {@link ReadableLogChannel} capable of reading log data, starting from {@link LogPosition position}.
     * @throws IOException
     */
    ReadableLogChannel getReader( LogPosition position ) throws IOException;

    /**
     * A slight leak from {@link TransactionStore} since {@link LogFile} isn't supposed to know about transaction ids.
     */
    LogPosition findRoughPositionOf( long transactionId ) throws NoSuchTransactionException;

    void close() throws IOException;
}
