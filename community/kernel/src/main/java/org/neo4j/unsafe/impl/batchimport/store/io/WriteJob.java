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
package org.neo4j.unsafe.impl.batchimport.store.io;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.neo4j.collection.pool.Pool;
import org.neo4j.unsafe.impl.batchimport.store.BatchingPageCache.Writer;

/**
 * Represents one write job, i.e. a chunk of data to be written to a {@link Writer} at a specific position.
 */
class WriteJob
{
    private final ByteBuffer byteBuffer;
    private final long position;
    private final Writer writer;
    private final Pool<ByteBuffer> poolToReleaseBufferIn;

    WriteJob( Writer writer, ByteBuffer byteBuffer, long position, Pool<ByteBuffer> poolToReleaseBufferIn )
    {
        this.writer = writer;
        this.byteBuffer = byteBuffer;
        this.position = position;
        this.poolToReleaseBufferIn = poolToReleaseBufferIn;
    }

    public void execute() throws IOException
    {
        writer.write( byteBuffer, position, poolToReleaseBufferIn );
    }
}
