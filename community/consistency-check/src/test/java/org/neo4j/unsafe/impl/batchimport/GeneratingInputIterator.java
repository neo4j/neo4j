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
package org.neo4j.unsafe.impl.batchimport;

import java.io.IOException;
import java.util.Iterator;

import org.neo4j.unsafe.impl.batchimport.input.InputChunk;
import org.neo4j.unsafe.impl.batchimport.input.InputEntityVisitor;

public abstract class GeneratingInputIterator<CHUNKSTATE> implements InputIterator
{
    private final Iterator<CHUNKSTATE> states;
    private long nextBatch;

    public GeneratingInputIterator( Iterator<CHUNKSTATE> states )
    {
        this.states = states;
    }

    @Override
    public void close() throws IOException
    {
    }

    @Override
    public String sourceDescription()
    {
        return "Generator";
    }

    @Override
    public long lineNumber()
    {
        return 0;
    }

    @Override
    public long position()
    {
        return 0;
    }

    @Override
    public InputChunk newChunk()
    {
        return new Chunk();
    }

    protected abstract boolean generateNext( CHUNKSTATE state, long batch, int itemInBatch,
            InputEntityVisitor visitor );

    @Override
    public synchronized boolean next( InputChunk chunk ) throws IOException
    {
        if ( !states.hasNext() )
        {
            return false;
        }

        ((Chunk)chunk).initialize( states.next(), nextBatch++ );
        return true;
    }

    private class Chunk implements InputChunk
    {
        private CHUNKSTATE state;
        private long batch;
        private int itemInBatch;

        @Override
        public void close() throws IOException
        {
        }

        private void initialize( CHUNKSTATE state, long batch )
        {
            this.state = state;
            this.batch = batch;
            this.itemInBatch = 0;
        }

        @Override
        public boolean next( InputEntityVisitor visitor ) throws IOException
        {
            boolean result = generateNext( state, batch, itemInBatch++, visitor );
            visitor.endOfEntity();
            return result;
        }
    }
}
