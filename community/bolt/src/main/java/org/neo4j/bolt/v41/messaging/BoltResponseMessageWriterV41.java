/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.bolt.v41.messaging;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.neo4j.bolt.messaging.BoltResponseMessageWriter;
import org.neo4j.bolt.messaging.ResponseMessage;
import org.neo4j.bolt.packstream.Neo4jPack;
import org.neo4j.bolt.packstream.Neo4jPackV3;
import org.neo4j.bolt.packstream.PackOutput;
import org.neo4j.bolt.packstream.PackProvider;
import org.neo4j.bolt.v3.messaging.BoltResponseMessageWriterV3;
import org.neo4j.logging.internal.LogService;
import org.neo4j.memory.HeapEstimator;
import org.neo4j.time.SystemNanoClock;
import org.neo4j.values.AnyValue;

/**
 * Writer for Bolt request messages to be sent to a {@link Neo4jPack.Packer}. All methods need to be synchronized because both bolt worker threads and also bolt
 * keep-alive thread could try to write to the output at the same time.
 */
public class BoltResponseMessageWriterV41 implements BoltResponseMessageWriter
{
    public static final long SHALLOW_SIZE = HeapEstimator.shallowSizeOfInstance( BoltResponseMessageWriterV41.class );

    private final BoltResponseMessageWriterV3 delegator;
    private final MessageWriterTimer timer;
    private boolean inRecord;
    private boolean shouldFlushAfterRecord;
    private boolean closed;

    private final Lock lock = new ReentrantLock();

    public BoltResponseMessageWriterV41( PackProvider packerProvider, PackOutput output, LogService logService,
                                         SystemNanoClock clock, Duration keepAliveInterval )
    {
        this( new BoltResponseMessageWriterV3( packerProvider, output, logService ),
              new MessageWriterTimer( clock, keepAliveInterval ) );
    }

    BoltResponseMessageWriterV41( BoltResponseMessageWriterV3 writer, MessageWriterTimer timer )
    {
        this.delegator = writer;
        this.timer = timer;
    }

    @Override
    public void write( ResponseMessage message ) throws IOException
    {
        lock.lock();
        try
        {
            delegator.write( message );
        }
        finally
        {
            lock.unlock();
        }
    }

    @Override
    public void flush() throws IOException
    {
        lock.lock();
        try
        {
            timer.reset();
            delegator.flush();
        }
        finally
        {
            lock.unlock();
        }
    }

    @Override
    public void keepAlive() throws IOException
    {
        // Double-check locking. The timeout variable inside timer is volatile.
        if ( !this.closed && timer.isTimedOut() )
        {
            if ( lock.tryLock() )
            {
                try
                {
                    if ( !this.closed && timer.isTimedOut() )
                    {
                        flushBufferOrSendKeepAlive();
                    }
                }
                finally
                {
                    lock.unlock();
                }
            }
        }
    }

    @Override
    public void initKeepAliveTimer()
    {
        timer.reset();
    }

    @Override
    public void beginRecord( int numberOfFields ) throws IOException
    {
        lock.lock();
        try
        {
            beforeRecord();
            delegator.beginRecord( numberOfFields );
        }
        finally
        {
            lock.unlock();
        }
    }

    @Override
    public void consumeField( AnyValue value ) throws IOException
    {
        lock.lock();
        try
        {
            delegator.consumeField( value );
        }
        finally
        {
            lock.unlock();
        }
    }

    @Override
    public void endRecord() throws IOException
    {
        lock.lock();
        try
        {
            delegator.endRecord();
            afterRecord();
        }
        finally
        {
            lock.unlock();
        }
    }

    @Override
    public void onError() throws IOException
    {
        lock.lock();
        try
        {
            delegator.onError();
            afterRecord();
        }
        finally
        {
            lock.unlock();
        }
    }

    @Override
    public void close() throws IOException
    {
        lock.lock();
        try
        {
            this.closed = true;
            delegator.close();
        }
        finally
        {
            lock.unlock();
        }
    }

    private void beforeRecord()
    {
        inRecord = true;
    }

    private void afterRecord() throws IOException
    {
        inRecord = false;
        if ( shouldFlushAfterRecord )
        {
            flush();
        }
    }

    @Override
    public void flushBufferOrSendKeepAlive() throws IOException
    {
        lock.lock();
        try
        {
            this.doFlushBufferOrSendKeepAlive();
        }
        finally
        {
            lock.unlock();
        }
    }

    private void doFlushBufferOrSendKeepAlive() throws IOException
    {
        if ( inRecord )
        {
            shouldFlushAfterRecord = true;
            return;
        }

        writeNoop();
        flush();
    }

    private void writeNoop() throws IOException
    {
        try
        {
            delegator.output().beginMessage();
            delegator.output().messageSucceeded();
        }
        catch ( Throwable e )
        {
            delegator.log().error( "Failed to write NOOP", e );
            throw e;
        }
    }

    @Override
    public void handle( List<String> patches )
    {
        if ( patches.contains( UTC_PATCH ) )
        {
            delegator.updatePacker( new Neo4jPackV3() );
        }
    }
}
