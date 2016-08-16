/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.coreedge.core.consensus.log;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.neo4j.coreedge.core.consensus.ReplicatedString;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.test.rule.TestDirectory;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;

public abstract class ConcurrentStressIT<T extends RaftLog & Lifecycle>
{
    private static final int MAX_CONTENT_SIZE = 2048;
    @Rule
    public final TestDirectory dir = TestDirectory.testDirectory();

    protected abstract T createRaftLog( FileSystemAbstraction fsa, File dir ) throws Throwable;

    @Test
    public void readAndWrite() throws Throwable
    {
        readAndWrite( 5, 2, SECONDS );
    }

    private void readAndWrite( int nReaders, int time, TimeUnit unit ) throws Throwable
    {
        DefaultFileSystemAbstraction fsa = new DefaultFileSystemAbstraction();
        T raftLog = createRaftLog( fsa, dir.directory() );

        try
        {
            ExecutorService es = Executors.newCachedThreadPool();

            Collection<Future<Long>> futures = new ArrayList<>();

            futures.add( es.submit( new TimedTask( () -> {
                write( raftLog );
            }, time, unit ) ) );

            for ( int i = 0; i < nReaders; i++ )
            {
                futures.add( es.submit( new TimedTask( () -> {
                    read( raftLog );
                }, time, unit ) ) );
            }

            for ( Future<Long> f : futures )
            {
                long iterations = f.get();
            }

            es.shutdown();
        }
        finally
        {
            //noinspection ThrowFromFinallyBlock
            raftLog.shutdown();
        }
    }

    private class TimedTask implements Callable<Long>
    {
        private Runnable task;
        private final long runTimeMillis;

        TimedTask( Runnable task, int time, TimeUnit unit )
        {
            this.task = task;
            this.runTimeMillis = unit.toMillis( time );
        }

        @Override
        public Long call() throws Exception
        {
            long endTime = System.currentTimeMillis() + runTimeMillis;
            long count = 0;
            while ( endTime > System.currentTimeMillis() )
            {
                task.run();
                count++;
            }
            return count;
        }
    }

    private void read( RaftLog raftLog )
    {
        try ( RaftLogCursor cursor = raftLog.getEntryCursor( 0 ) )
        {
            while ( cursor.next() )
            {
                RaftLogEntry entry = cursor.get();
                ReplicatedString content = (ReplicatedString) entry.content();
                assertEquals( stringForIndex( cursor.index() ), content.value() );
            }
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    private void write( RaftLog raftLog )
    {
        long index = raftLog.appendIndex();
        long term = (index + 1) * 3;
        try
        {
            String data = stringForIndex( index + 1 );
            raftLog.append( new RaftLogEntry( term, new ReplicatedString( data ) ) );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    private static final CharSequence CHARS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

    private String stringForIndex( long index )
    {
        int len = ((int) index) % MAX_CONTENT_SIZE + 1;
        StringBuilder str = new StringBuilder( len );

        while ( len-- > 0 )
        {
            str.append( CHARS.charAt( len % CHARS.length() ) );
        }

        return str.toString();
    }
}
