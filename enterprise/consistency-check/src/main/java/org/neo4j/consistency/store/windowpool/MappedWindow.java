/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.consistency.store.windowpool;

import java.nio.MappedByteBuffer;

import org.neo4j.kernel.impl.nioneo.store.Buffer;
import org.neo4j.kernel.impl.nioneo.store.PersistenceWindow;

class MappedWindow implements PersistenceWindow
{
    private final long startRecordId;
    private final Buffer buffer;
    private final int recordsPerPage;
    private final int recordSize;
    private static final boolean I_AM_SURE_THERE_ARE_NO_REFERENCES_TO_HERE = false;

    public MappedWindow( int recordsPerPage, int recordSize, long startRecordId, MappedByteBuffer mappedBuffer )
    {
        this.recordsPerPage = recordsPerPage;
        this.recordSize = recordSize;
        this.startRecordId = startRecordId;
        this.buffer = new Buffer( this, mappedBuffer );
    }

    @Override
    public Buffer getBuffer()
    {
        return buffer;
    }

    @Override
    public Buffer getOffsettedBuffer( long id )
    {
        int offset = (int) (id - startRecordId) * recordSize;
        buffer.setOffset( offset );
        return buffer;
    }

    @Override
    public int getRecordSize()
    {
        return recordSize;
    }

    @Override
    public long position()
    {
        return startRecordId;
    }

    @Override
    public int size()
    {
        return recordsPerPage;
    }

    @Override
    public void force()
    {
    }

    @Override
    public void close()
    {
        if ( I_AM_SURE_THERE_ARE_NO_REFERENCES_TO_HERE )
        {
            sun.nio.ch.DirectBuffer directBuffer = (sun.nio.ch.DirectBuffer) buffer.getBuffer();
            directBuffer.cleaner().clean();
        }
    }
}
