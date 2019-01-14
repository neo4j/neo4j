/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.impl.transaction.log;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

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
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import org.neo4j.adversaries.Adversary;
import org.neo4j.adversaries.ClassGuardedAdversary;
import org.neo4j.adversaries.CountingAdversary;
import org.neo4j.adversaries.fs.AdversarialFileSystemAbstraction;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileSystemLifecycleAdapter;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.core.DatabasePanicEventGenerator;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.transaction.SimpleLogVersionRepository;
import org.neo4j.kernel.impl.transaction.SimpleTransactionIdStore;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFiles;
import org.neo4j.kernel.impl.transaction.log.rotation.LogRotation;
import org.neo4j.kernel.impl.transaction.tracing.LogAppendEvent;
import org.neo4j.kernel.impl.transaction.tracing.LogForceWaitEvent;
import org.neo4j.kernel.impl.util.IdOrderingQueue;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.lifecycle.LifeRule;
import org.neo4j.logging.NullLog;
import org.neo4j.test.Race;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.test.DoubleLatch.awaitLatch;
import static org.neo4j.test.ThreadTestUtils.awaitThreadState;
import static org.neo4j.test.ThreadTestUtils.fork;

public class BatchingTransactionAppenderConcurrencyTest
{

    private static final long MILLISECONDS_TO_WAIT = TimeUnit.SECONDS.toMillis( 10 );

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

    private final LifeRule life = new LifeRule();
    private final EphemeralFileSystemRule fileSystemRule = new EphemeralFileSystemRule();

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule( fileSystemRule ).around( life );

    private final LogAppendEvent logAppendEvent = LogAppendEvent.NULL;
    private final LogFiles logFiles = mock( TransactionLogFiles.class );
    private final LogFile logFile = mock( LogFile.class );
    private final LogRotation logRotation = LogRotation.NO_ROTATION;
    private final TransactionMetadataCache transactionMetadataCache = new TransactionMetadataCache( 10 );
    private final TransactionIdStore transactionIdStore = new SimpleTransactionIdStore();
    private final SimpleLogVersionRepository logVersionRepository = new SimpleLogVersionRepository();
    private final IdOrderingQueue explicitIndexTransactionOrdering = IdOrderingQueue.BYPASS;
    private final DatabaseHealth databaseHealth = mock( DatabaseHealth.class );
    private final Semaphore forceSemaphore = new Semaphore( 0 );

    private final BlockingQueue<ChannelCommand> channelCommandQueue = new LinkedBlockingQueue<>( 2 );

    @Before
    public void setUp()
    {
        when( logFiles.getLogFile() ).thenReturn( logFile );
        when( logFile.getWriter() ).thenReturn( new CommandQueueChannel() );
    }

    @Test
    public void shouldForceLogChannel() throws Throwable
    {
        BatchingTransactionAppender appender = life.add( createTransactionAppender() );
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

        final BatchingTransactionAppender appender = life.add( createTransactionAppender() );
        life.start();

        Runnable runnable = createForceAfterAppendRunnable( appender );
        Future<?> future = executor.submit( runnable );

        forceSemaphore.acquire();

        Thread otherThread = fork( runnable );
        awaitThreadState( otherThread, MILLISECONDS_TO_WAIT, Thread.State.TIMED_WAITING );

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

        final BatchingTransactionAppender appender = life.add( createTransactionAppender() );
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
            awaitThreadState( otherThread, MILLISECONDS_TO_WAIT, Thread.State.TIMED_WAITING );
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
        File directory = new File( "dir" ).getCanonicalFile();
        efs.mkdirs( directory );
        FileSystemAbstraction fs = new AdversarialFileSystemAbstraction( adversary, efs );
        life.add( new FileSystemLifecycleAdapter( fs ) );
        DatabaseHealth databaseHealth = new DatabaseHealth( mock( DatabasePanicEventGenerator.class ), NullLog.getInstance() );
        LogFiles logFiles = LogFilesBuilder.builder( directory, fs )
                .withLogVersionRepository( logVersionRepository )
                .withTransactionIdStore( transactionIdStore )
                .build();
        life.add( logFiles );
        final BatchingTransactionAppender appender = life.add(
                new BatchingTransactionAppender( logFiles, logRotation, transactionMetadataCache, transactionIdStore,
                        explicitIndexTransactionOrdering, databaseHealth ) );
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
            race.addContestant( () ->
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
            } );
        }

        // THEN perform the race. The relevant assertions are made inside the contestants.
        race.go();
    }

    protected TransactionToApply tx()
    {
        NodeRecord before = new NodeRecord( 0 );
        NodeRecord after = new NodeRecord( 0 );
        after.setInUse( true );
        Command.NodeCommand nodeCommand = new Command.NodeCommand( before, after );
        PhysicalTransactionRepresentation tx = new PhysicalTransactionRepresentation( singletonList( nodeCommand ) );
        tx.setHeader( new byte[0], 0, 0, 0, 0, 0, 0 );
        return new TransactionToApply( tx );
    }

    private Runnable createForceAfterAppendRunnable( final BatchingTransactionAppender appender )
    {
        return () ->
        {
            try
            {
                appender.forceAfterAppend( logAppendEvent );
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
        };
    }

    private Predicate<StackTraceElement> failMethod( final Class<?> klass, final String methodName )
    {
        return element -> element.getClassName().equals( klass.getName() ) &&
                          element.getMethodName().equals( methodName );
    }

    private BatchingTransactionAppender createTransactionAppender()
    {
        return new BatchingTransactionAppender( logFiles, logRotation,
                transactionMetadataCache, transactionIdStore, explicitIndexTransactionOrdering, databaseHealth );
    }

    private enum ChannelCommand
    {
        emptyBufferIntoChannelAndClearIt,
        force,
        dummy
    }

    class CommandQueueChannel extends InMemoryClosableChannel implements Flushable
    {
        @Override
        public Flushable prepareForFlush()
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
    }
}
