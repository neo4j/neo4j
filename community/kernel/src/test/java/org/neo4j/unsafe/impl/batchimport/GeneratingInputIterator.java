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
import java.util.Collections;
import java.util.Iterator;

import org.neo4j.unsafe.impl.batchimport.input.InputChunk;
import org.neo4j.unsafe.impl.batchimport.input.InputEntityVisitor;

/**
 * A utility to be able to write an {@link InputIterator} with low effort.
 * Since {@link InputIterator} is multi-threaded in that multiple threads can call {@link #newChunk()} and each
 * call to {@link #next(InputChunk)} handing out the next chunkstate instance from the supplied {@link Iterator}.
 *
 * @param <CHUNKSTATE> type of objects handed out from the supplied {@link Iterator}.
 */
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

    /**
     * Generates data for the current {@code state}, {@code batch} and {@code itemInBatch}.
     *
     * @param state CHUNKSTATE gotten from the state {@link Iterator}.
     * @param batch zero-based id (ordered) of the batch to generate data for.
     * @param itemInBatch zero-based index of the item in this batch to generate data for.
     * @param visitor {@link InputEntityVisitor} receiving calls which is the generated data.
     * @return {@code true} if data was generated, otherwise {@code false} meaning that this batch reached its end.
     */
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

        /**
         * @param state CHUNKSTATE which is the source of data generation for this chunk.
         * @param batch zero-based id (order) of this batch.
         */
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
            if ( result )
            {
                visitor.endOfEntity();
            }
            return result;
        }
    }

    public static final InputIterator EMPTY = new GeneratingInputIterator<Void>( Collections.emptyIterator() )
    {
        @Override
        protected boolean generateNext( Void state, long batch, int itemInBatch, InputEntityVisitor visitor )
        {
            return false;
        }
    };
}
