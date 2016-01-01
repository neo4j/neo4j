/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.nioneo.store;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.test.TargetDirectory;

public class PersistenceWindowPoolRaceTestIT
{
    private static final Throwable STOP_SIGNAL = new Throwable( "Test stop signal" );

    @Rule
    public TargetDirectory.TestDirectory testDir =
            TargetDirectory.testDirForTest( PersistenceWindowPoolRaceTestIT.class );

    @Test
    public void raceOnClaimReleaseForAtMost30Seconds() throws Throwable
    {
        ExecutorService executor = Executors.newCachedThreadPool();

        File file = new File( testDir.directory(), "test.file.db" );

        int blockSize = 512;
        int maxId = 10000;
        RandomAccessFile raf = new RandomAccessFile( file, "rw" );
        FileChannel fileChannel = raf.getChannel();
        setSize( fileChannel, blockSize * maxId);
        long mappedMem = blockSize + (blockSize * (maxId / 10)) + (blockSize / 2);
        boolean useMemoryMappedBuffers = true;
        boolean readOnly = false;
        ConcurrentMap<Long, PersistenceRow> activeRowWindows = new ConcurrentHashMap<Long, PersistenceRow>();
        BrickElementFactory brickFactory = BrickElementFactory.DEFAULT;
        StringLogger log = StringLogger.DEV_NULL;

        PersistenceWindowPool pwp = new PersistenceWindowPool(
                file,
                blockSize,
                new StoreFileChannel( fileChannel ),
                mappedMem,
                useMemoryMappedBuffers,
                readOnly,
                activeRowWindows,
                brickFactory,
                log );

        for ( int i = 0; i <= 9; i++ )
        {
            PersistenceWindow window = pwp.acquire( i * 10, OperationType.WRITE );
            pwp.release( window );
        }

        AtomicReference<Throwable> mailbox = new AtomicReference<Throwable>();

        AcquireReleaseJob[] jobs = new AcquireReleaseJob[] {
                new AcquireReleaseJob( pwp, maxId, mailbox ),
                new AcquireReleaseJob( pwp, maxId, mailbox ),
                new AcquireReleaseJob( pwp, maxId, mailbox ),
                new AcquireReleaseJob( pwp, maxId, mailbox ),
                new AcquireReleaseJob( pwp, maxId, mailbox ),
                new AcquireReleaseJob( pwp, maxId, mailbox ),
                new AcquireReleaseJob( pwp, maxId, mailbox ),
                new AcquireReleaseJob( pwp, maxId, mailbox )};

        for ( AcquireReleaseJob job : jobs )
        {
            executor.submit( job );
        }

        // The test will run for at most ~30 seconds.
        long deadline = System.currentTimeMillis() + 30000;
        Throwable observedFailure;
        do
        {
            Thread.sleep( 100 );
            observedFailure = mailbox.get();
        } while ( observedFailure == null && System.currentTimeMillis() < deadline );
        executor.shutdown();
        mailbox.compareAndSet( null, STOP_SIGNAL );
        pwp.close();
        raf.close();
        if ( !executor.awaitTermination( 10, TimeUnit.SECONDS ) )
        {
            System.err.println( "WARNING: Executor did not terminate after 10 seconds." );
        }

        if ( observedFailure != null )
        {
            throw observedFailure;
        }
    }

    private void setSize( FileChannel fileChannel, long sizeInBytes ) throws IOException
    {
        fileChannel.write( ByteBuffer.wrap( new byte[] { 0 }), sizeInBytes - 1 );
    }

    private static class AcquireReleaseJob implements Runnable
    {
        private final PersistenceWindowPool pwp;
        private final AtomicReference<Throwable> mailbox;
        private final long maxId;

        public AcquireReleaseJob( PersistenceWindowPool pwp, int maxId, AtomicReference<Throwable> mailbox )
        {
            this.pwp = pwp;
            this.mailbox = mailbox;
            this.maxId = maxId - 1;
        }

        @Override
        public void run()
        {
            Random random = new Random();

            try
            {
                while ( mailbox.get() == null )
                {
                    long id = Math.abs(random.nextLong() % maxId);
                    PersistenceWindow window = pwp.acquire( id, OperationType.WRITE );
                    window.getOffsettedBuffer( id ).put( (byte) (0xFF & random.nextInt()) );
                    pwp.release( window );
                }
            }
            catch ( Throwable e )
            {
                mailbox.set( e );
            }
        }
    }
}
