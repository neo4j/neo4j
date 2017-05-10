/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.csv.reader;

import java.io.IOException;

import org.neo4j.csv.reader.Source.Chunk;

public abstract class CharReadableChunker implements Chunker
{
    protected final CharReadable reader;
    protected final int chunkSize;
    protected volatile long position;

    public CharReadableChunker( CharReadable reader, int chunkSize )
    {
        this.reader = reader;
        this.chunkSize = chunkSize;
    }

    @Override
    public ChunkImpl newChunk()
    {
        return new ChunkImpl( new char[chunkSize] );
    }

    @Override
    public void close() throws IOException
    {
        reader.close();
    }

    public long position()
    {
        return position;
    }

    public static class ChunkImpl implements Chunk
    {
        final char[] buffer;
        private int length;
        private String sourceDescription;

        public ChunkImpl( char[] buffer )
        {
            this.buffer = buffer;
        }

        void initialize( int length, String sourceDescription )
        {
            this.length = length;
            this.sourceDescription = sourceDescription;
        }

        @Override
        public int startPosition()
        {
            return 0;
        }

        @Override
        public String sourceDescription()
        {
            return sourceDescription;
        }

        @Override
        public int maxFieldSize()
        {
            return buffer.length;
        }

        @Override
        public int length()
        {
            return length;
        }

        @Override
        public char[] data()
        {
            return buffer;
        }

        @Override
        public int backPosition()
        {
            return 0;
        }
    }
}
