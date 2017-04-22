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
