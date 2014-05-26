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

public class InMemoryLogFile implements LogFile
{
    private final InMemoryLogChannel channel;

    public InMemoryLogFile( InMemoryLogChannel channel )
    {
        this.channel = channel;
    }

    @Override
    public void close() throws IOException
    {   // Nothing to close
    }

    @Override
    public void open( Visitor<ReadableLogChannel, IOException> recoveredDataVisitor ) throws IOException
    {
        recoveredDataVisitor.visit( channel );
        channel.positionWriter( channel.readerPosition() );
    }

    @Override
    public WritableLogChannel getWriter()
    {
        return channel;
    }

    @Override
    public ReadableLogChannel getReader( LogPosition position ) throws IOException
    {
        channel.positionReader( (int) position.getByteOffset() );
        return channel;
    }

    @Override
    public LogPosition findRoughPositionOf( long transactionId ) throws NoSuchTransactionException
    {
        throw new UnsupportedOperationException( "Please implement" );
    }
}
