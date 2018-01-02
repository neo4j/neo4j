/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.log;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.Flushable;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;

import org.neo4j.adversaries.Adversary;
import org.neo4j.adversaries.ClassGuardedAdversary;
import org.neo4j.adversaries.CountingAdversary;
import org.neo4j.adversaries.fs.AdversarialFileSystemAbstraction;
import org.neo4j.function.Predicate;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.KernelHealth;
import org.neo4j.kernel.impl.core.KernelPanicEventGenerator;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.transaction.DeadSimpleLogVersionRepository;
import org.neo4j.kernel.impl.transaction.DeadSimpleTransactionIdStore;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.log.rotation.LogRotation;
import org.neo4j.kernel.impl.transaction.tracing.LogAppendEvent;
import org.neo4j.kernel.impl.transaction.tracing.LogForceWaitEvent;
import org.neo4j.kernel.impl.util.IdOrderingQueue;
import org.neo4j.kernel.lifecycle.LifeRule;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.NullLog;
import org.neo4j.test.Race;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static java.util.Arrays.asList;

import static org.neo4j.io.ByteUnit.kibiBytes;
import static org.neo4j.test.DoubleLatch.awaitLatch;
import static org.neo4j.test.ThreadTestUtils.awaitThreadState;
import static org.neo4j.test.ThreadTestUtils.fork;

public class BatchingTransactionAppenderConcurrencyTest
{
    private enum ChannelCommand
    {
        emptyBufferIntoChannelAndClearIt,
        force,
        dummy
    }

    private static ExecutorService executor;

    @BeforeClass
    public static void setUpExecutor()
    {
        executor = Executors.newCachedThreadPool();
    }

    @AfterClass
    public static void tearDownExecutor()
    {
        executor.shutdown();
        executor = null;
    }


    @Rule
    public final LifeRule life = new LifeRule();

    private final LogAppendEvent logAppendEvent = LogAppendEvent.NULL;
    private final LogFile logFile = mock( LogFile.class );
    private final LogRotation logRotation = LogRotation.NO_ROTATION;
    private final TransactionMetadataCache transactionMetadataCache = new TransactionMetadataCache( 10, 10 );
    private final TransactionIdStore transactionIdStore = new DeadSimpleTransactionIdStore();
    private final IdOrderingQueue legacyIndexTransactionOrdering = IdOrderingQueue.BYPASS;
    private final KernelHealth kernelHealth= mock( KernelHealth.class );
    private final Semaphore forceSemaphore = new Semaphore( 0 );

    private final BlockingQueue<ChannelCommand> channelCommandQueue = new LinkedBlockingQueue<>( 2 );

    @Before
    public void setUp()
    {
        class Channel extends InMemoryLogChannel implements Flushable
        {
            @Override
            public Flushable emptyBufferIntoChannelAndClearIt()
            {
                try
                {
                    channelCommandQueue.put( ChannelCommand.emptyBufferIntoChannelAndClearIt );
                }
                catch ( InterruptedException e )
                {
                    throw new RuntimeException( e );
                }
                return this;
            }

            @Override
            public void flush() throws IOException
            {
                try
                {
                    forceSemaphore.release();
                    channelCommandQueue.put( ChannelCommand.force );
                }
                catch ( InterruptedException e )
                {
                    throw new IOException( e );
                }
            }
        };

        when( logFile.getWriter() ).thenReturn( new Channel() );
    }

    private Runnable createForceAfterAppendRunnable( final BatchingTransactionAppender appender )
    {
        return new Runnable()
        {
            @Override
            public void run()
            {
                try
                {
                    appender.forceAfterAppend( logAppendEvent );
                }
                catch ( IOException e )
                {
                    throw new RuntimeException( e );
                }
            }
        };
    }

    @Test
    public void shouldForceLogChannel() throws Throwable
    {
        BatchingTransactionAppender appender = life.add( new BatchingTransactionAppender( logFile, logRotation,
                transactionMetadataCache, transactionIdStore, legacyIndexTransactionOrdering, kernelHealth ) );
        life.start();

        appender.forceAfterAppend( logAppendEvent );

        assertThat( channelCommandQueue.take(), is( ChannelCommand.emptyBufferIntoChannelAndClearIt ) );
        assertThat( channelCommandQueue.take(), is( ChannelCommand.force ) );
        assertTrue( channelCommandQueue.isEmpty() );
    }

    @Test
    public void shouldWaitForOngoingForceToCompleteBeforeForcingAgain() throws Throwable
    {
        channelCommandQueue.put( ChannelCommand.dummy );

        // The 'emptyBuffer...' command will be put into the queue, and then it'll block on 'force' because the queue
        // will be at capacity.

        final BatchingTransactionAppender appender = life.add( new BatchingTransactionAppender( logFile, logRotation,
                transactionMetadataCache, transactionIdStore, legacyIndexTransactionOrdering, kernelHealth ) );
        life.start();

        Runnable runnable = createForceAfterAppendRunnable( appender );
        Future<?> future = executor.submit( runnable );

        forceSemaphore.acquire();

        Thread otherThread = fork( runnable );
        awaitThreadState( otherThread, 5000, Thread.State.TIMED_WAITING );

        assertThat( channelCommandQueue.take(), is( ChannelCommand.dummy ) );
        assertThat( channelCommandQueue.take(), is( ChannelCommand.emptyBufferIntoChannelAndClearIt ) );
        assertThat( channelCommandQueue.take(), is( ChannelCommand.force ) );
        assertThat( channelCommandQueue.take(), is( ChannelCommand.emptyBufferIntoChannelAndClearIt ) );
        assertThat( channelCommandQueue.take(), is( ChannelCommand.force ) );
        future.get();
        otherThread.join();
        assertTrue( channelCommandQueue.isEmpty() );
    }

    @Test
    public void shouldBatchUpMultipleWaitingForceRequests() throws Throwable
    {
        channelCommandQueue.put( ChannelCommand.dummy );

        // The 'emptyBuffer...' command will be put into the queue, and then it'll block on 'force' because the queue
        // will be at capacity.

        final BatchingTransactionAppender appender = life.add( new BatchingTransactionAppender( logFile, logRotation,
                transactionMetadataCache, transactionIdStore, legacyIndexTransactionOrdering, kernelHealth ) );
        life.start();

        Runnable runnable = createForceAfterAppendRunnable( appender );
        Future<?> future = executor.submit( runnable );

        forceSemaphore.acquire();

        Thread[] otherThreads = new Thread[10];
        for ( int i = 0; i < otherThreads.length; i++ )
        {
            otherThreads[i] = fork( runnable );
        }
        for ( Thread otherThread : otherThreads )
        {
            awaitThreadState( otherThread, 5000, Thread.State.TIMED_WAITING );
        }

        assertThat( channelCommandQueue.take(), is( ChannelCommand.dummy ) );
        assertThat( channelCommandQueue.take(), is( ChannelCommand.emptyBufferIntoChannelAndClearIt ) );
        assertThat( channelCommandQueue.take(), is( ChannelCommand.force ) );
        assertThat( channelCommandQueue.take(), is( ChannelCommand.emptyBufferIntoChannelAndClearIt ) );
        assertThat( channelCommandQueue.take(), is( ChannelCommand.force ) );
        future.get();
        for ( Thread otherThread : otherThreads )
        {
            otherThread.join();
        }
        assertTrue( channelCommandQueue.isEmpty() );
    }

    /*
     * There was an issue where if multiple concurrent appending threads did append and they moved on
     * to await a force, where the force would fail and the one doing the force would raise a panic...
     * the other threads may not notice the panic and move on to mark those transactions as committed
     * and notice the panic later (which would be too late).
     */
    @Test
    public void shouldHaveAllConcurrentAppendersSeePanic() throws Throwable
    {
        // GIVEN
        Adversary adversary = new ClassGuardedAdversary( new CountingAdversary( 1, true ),
                failMethod( BatchingTransactionAppender.class, "force" ) );
        EphemeralFileSystemAbstraction efs = new EphemeralFileSystemAbstraction();
        life.add( asLifecycle( efs ) ); // <-- so that it gets automatically shut down after the test
        File directory = new File( "dir" ).getCanonicalFile();
        efs.mkdirs( directory );
        FileSystemAbstraction fs = new AdversarialFileSystemAbstraction( adversary, efs );
        KernelHealth kernelHealth = new KernelHealth( mock( KernelPanicEventGenerator.class ), NullLog.getInstance() );
        PhysicalLogFiles logFiles = new PhysicalLogFiles( directory, fs );
        LogFile logFile = life.add( new PhysicalLogFile( fs, logFiles, kibiBytes( 10 ), transactionIdStore,
                new DeadSimpleLogVersionRepository( 0 ), new PhysicalLogFile.Monitor.Adapter(),
                transactionMetadataCache ) );
        final BatchingTransactionAppender appender = life.add( new BatchingTransactionAppender(
                logFile, logRotation, transactionMetadataCache, transactionIdStore,
                legacyIndexTransactionOrdering, kernelHealth ) );
        life.start();

        // WHEN
        int numberOfAppenders = 10;
        final CountDownLatch trap = new CountDownLatch( numberOfAppenders );
        final LogAppendEvent beforeForceTrappingEvent = new LogAppendEvent.Empty()
        {
            @Override
            public LogForceWaitEvent beginLogForceWait()
            {
                trap.countDown();
                awaitLatch( trap );
                return super.beginLogForceWait();
            }
        };
        Race race = new Race();
        for ( int i = 0; i < numberOfAppenders; i++ )
        {
            race.addContestant( new Runnable()
            {
                @Override
                public void run()
                {
                    try
                    {
                        // Append to the log, the LogAppenderEvent will have all of the appending threads
                        // do wait for all of the other threads to start the force thing
                        appender.append( tx(), beforeForceTrappingEvent );
                        fail( "No transaction should be considered appended" );
                    }
                    catch ( IOException e )
                    {
                        // Good, we know that this test uses an adversarial file system which will throw
                        // an exception in BatchingTransactionAppender#force, and since all these transactions
                        // will append and be forced in the same batch, where the force will fail then
                        // all these transactions should fail. If there's any transaction not failing then
                        // it just didn't notice the panic, which would be potentially hazardous.
                    }
                }
            } );
        }

        // THEN perform the race. The relevant assertions are made inside the contestants.
        race.go();
    }

    private Lifecycle asLifecycle( final EphemeralFileSystemAbstraction efs )
    {
        return new LifecycleAdapter()
        {
            @Override
            public void shutdown() throws Throwable
            {
                efs.shutdown();
            }
        };
    }

    protected TransactionRepresentation tx()
    {
        Command.NodeCommand nodeCommand = new Command.NodeCommand();
        NodeRecord before = new NodeRecord( 0 );
        NodeRecord after = new NodeRecord( 0 );
        after.setInUse( true );
        nodeCommand.init( before, after );
        PhysicalTransactionRepresentation tx = new PhysicalTransactionRepresentation(
                asList( (Command) nodeCommand ) );
        tx.setHeader( new byte[0], 0, 0, 0, 0, 0, 0 );
        return tx;
    }

    private Predicate<StackTraceElement> failMethod( final Class<?> klass, final String methodName )
    {
        return new Predicate<StackTraceElement>()
        {
            @Override
            public boolean test( StackTraceElement element )
            {
                return element.getClassName().equals( klass.getName() ) &&
                        element.getMethodName().equals( methodName );
            }
        };
    }
}
