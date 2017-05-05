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
import java.util.concurrent.ConcurrentLinkedQueue;

import org.neo4j.unsafe.impl.batchimport.input.InputChunk;
import org.neo4j.unsafe.impl.batchimport.staging.ProducerStep;
import org.neo4j.unsafe.impl.batchimport.staging.StageControl;

import static java.lang.System.nanoTime;

public class InputReaderStep extends ProducerStep
{
    private final InputIterator input;
    private final ConcurrentLinkedQueue<InputChunk> chunks = new ConcurrentLinkedQueue<>();

    public InputReaderStep( StageControl control, Configuration config, InputIterator input )
    {
        super( control, config );
        this.input = input;
    }

    @Override
    protected void process() throws Exception
    {
        InputChunk chunk;
        long time = nanoTime();
        while ( (chunk = nextChunk()) != null )
        {
            time = nanoTime() - time;
            sendDownstream( chunk );
            time = nanoTime();
        }
    }

    private InputChunk nextChunk() throws IOException
    {
        InputChunk chunk = chunks.poll();
        if ( chunk != null )
        {
            return chunk;
        }

        chunk = input.newChunk();
        return input.next( chunk ) ? chunk : null;
    }

    @Override
    protected long position()
    {
        return input.position();
    }
}
