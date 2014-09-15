/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.xaframework;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.TransactionIdStore;
import org.neo4j.kernel.impl.nioneo.xa.command.Command;
import org.neo4j.kernel.impl.nioneo.xa.command.Command.NodeCommand;
import org.neo4j.kernel.impl.transaction.xaframework.PhysicalLogFile.Monitor;
import org.neo4j.kernel.impl.util.Providers;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.test.EphemeralFileSystemRule;

import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.locks.LockSupport.parkNanos;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import static org.neo4j.kernel.impl.nioneo.store.TransactionIdStore.BASE_TX_ID;
import static org.neo4j.kernel.impl.transaction.xaframework.log.pruning.LogPruneStrategyFactory.NO_PRUNING;

/**
 * This test verifies that there's no chance that there may be transactions appended to the log
 * that haven't made it through store application and forcefully flushed to the store, before rotated away.
 * Because if that happens then the last transactions of a log will not be recovered in the event of a crash.
 *
 * This test stress tests the interaction between LogFile and TransactionAppender, since they will have to
 * coordinate between them to make this happen.
 */
@RunWith( Parameterized.class )
public class AppendAndRotationRaceIT
{
    @Test
    public void shouldOnlyRotateAwayTransactionsThatHaveBeenFullyApliedAndForced() throws Throwable
    {
        // GIVEN a bunch of dependencies just to be able to tie the log file and appender together
        DeadSimpleTransactionIdStore transactionIdStore = new DeadSimpleTransactionIdStore( BASE_TX_ID );
        PhysicalLogFiles logFiles = new PhysicalLogFiles( directory, fsr.get() );
        LogVersionRepository logVersionRepository = new DeadSimpleLogVersionRepository( 0 );
        Monitor monitor = mock( Monitor.class );
        AtomicBoolean doneSignal = new AtomicBoolean();
        Rotator logRotationControl = new Rotator( 20, doneSignal, transactionIdStore );
        TransactionMetadataCache metadataCache = new TransactionMetadataCache( 10, 1_000 );
        TxIdGenerator txIdGenerator = new DefaultTxIdGenerator(
                Providers.<TransactionIdStore>singletonProvider( transactionIdStore ) );

        // ... and finally the two services of interest
        long rotateAtSize = forceRotate ? Integer.MAX_VALUE : 3_000;
        @SuppressWarnings( "unchecked" )
        PhysicalLogFile logFile = life.add( new PhysicalLogFile( fsr.get(), logFiles, rotateAtSize,
                NO_PRUNING, transactionIdStore, logVersionRepository, monitor, logRotationControl, metadataCache,
                mock( Visitor.class ) ) );
        life.start();
        TransactionAppender appender = appenderFactory.create(
                logFile, txIdGenerator, metadataCache, transactionIdStore );

        // WHENever stressing load on the appender where rotations are triggered (see Rotator)
        Committer[] committers = new Committer[10];
        for ( int i = 0; i < committers.length; i++ )
        {
            committers[i] = new Committer( appender, transactionIdStore, doneSignal );
        }
        if ( forceRotate )
        {
            Random random = new Random();
            while ( !doneSignal.get() )
            {
                LockSupport.parkNanos( random.nextInt( 200 ) * 1_000_000 );
                logFile.forceRotate();
            }
        }

        // THEN verify that all committed transactions rotated away have been forcefully applied to the store.
        for ( Committer committer : committers )
        {
            committer.await();
        }
        // doing Committer#await() above is enough since errors, as well as assertion failures that
        // has happened in any of the committers will be exposed there
    }

    private static class Rotator implements LogRotationControl
    {
        private final AtomicInteger rotationCounter;
        private final AtomicBoolean doneSignal;
        private final DeadSimpleTransactionIdStore transactionIdStore;

        Rotator( int numberOfRotationsToRunFor, AtomicBoolean doneSignal,
                DeadSimpleTransactionIdStore transactionIdStore )
        {
            this.doneSignal = doneSignal;
            this.transactionIdStore = transactionIdStore;
            this.rotationCounter = new AtomicInteger( numberOfRotationsToRunFor );
        }

        @Override
        public void awaitAllTransactionsClosed()
        {
            // Do some quick verification
            assertEquals( transactionIdStore.getLastCommittingTransactionId(),
                    transactionIdStore.getLastCommittedTransactionId() );

            if ( rotationCounter.decrementAndGet() < 0 )
            {
                doneSignal.set( true );
            }
        }

        @Override
        public void forceEverything()
        {   // Ignore this call
        }
    }

    public AppendAndRotationRaceIT( AppenderFactory appenderFactory, boolean forceRotate )
    {
        this.appenderFactory = appenderFactory;
        this.forceRotate = forceRotate;
    }

    public final @Rule EphemeralFileSystemRule fsr = new EphemeralFileSystemRule();
    private final LifeSupport life = new LifeSupport();
    private final File directory = new File( "dir" );
    private final AppenderFactory appenderFactory;
    private final boolean forceRotate;

    @Before
    public void before()
    {
        fsr.get().mkdirs( directory );
    }

    @After
    public void after()
    {
        life.shutdown();
    }

    public class Committer extends Thread
    {
        private final TransactionAppender appender;
        private final TransactionIdStore transactionIdStore;
        private final Random random = new Random();
        private final AtomicBoolean doneSignal;
        private volatile Throwable error;

        public Committer( TransactionAppender appender, TransactionIdStore transactionIdStore,
                AtomicBoolean doneSignal )
        {
            this.appender = appender;
            this.transactionIdStore = transactionIdStore;
            this.doneSignal = doneSignal;
            start();
        }

        public void await() throws Throwable
        {
            join();
            if ( error != null )
            {
                throw error;
            }
        }

        @Override
        public void run()
        {
            try
            {
                while ( !doneSignal.get() )
                {
                    performTheEquivalenceOfATransaction();
                }
            }
            catch ( Throwable e )
            {
                // This error will be picked up later, in await()
                this.error = null;
            }
        }

        private void performTheEquivalenceOfATransaction()
        {
            long timeStarted = currentTimeMillis();
            try
            {
                // append
                long transactionId = appender.append( bogusTransaction( timeStarted,
                        transactionIdStore.getLastCommittedTransactionId() ) );

                try
                {
                    transactionIdStore.transactionCommitted( transactionId );
                    // apply
                    parkNanos( random.nextInt( 10 ) * 1_000_000 );
                }
                finally
                {
                    // close
                    transactionIdStore.transactionClosed( transactionId );
                }
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
        }
    }

    public TransactionRepresentation bogusTransaction( long timeStarted, long transactionRightNow )
    {
        // What the transaction contains is irrelevant, just as long as it's some data so that the log will
        // eventually be filled so that it needs rotation
        Collection<Command> commands = new ArrayList<>();
        commands.add( createNodeCommand( transactionRightNow ) );

        PhysicalTransactionRepresentation transaction = new PhysicalTransactionRepresentation( commands );
        transaction.setHeader( new byte[10], 0, 0, timeStarted, transactionRightNow, currentTimeMillis() );
        return transaction;
    }

    private Command createNodeCommand( long id )
    {
        NodeCommand command = new NodeCommand();
        NodeRecord record = new NodeRecord( id );
        record.setInUse( true );
        command.init( new NodeRecord( id ), record );
        return command;
    }

    @Parameters
    public static Collection<Object[]> data()
    {
        // Each Object[] {AppenderFactory, Boolean (true for forceRotate(), false for relying on natural rotation)}
        return Arrays.asList(
                new Object[] { NON_BATCHING, false },
                new Object[] { BATCHING, false }

                // forceRotate() isn't supported, but it's not really an official feature either,
                // it's not a public method, but only really a method exposed to some of our test suites
//                new Object[] { NON_BATCHING, true },
//                new Object[] { BATCHING, true }
        );
    }

    private interface AppenderFactory
    {
        TransactionAppender create( LogFile logFile, TxIdGenerator txIdGenerator,
                TransactionMetadataCache metadataCache, TransactionIdStore transactionIdStore );
    }

    private static final AppenderFactory NON_BATCHING = new AppenderFactory()
    {
        @Override
        public TransactionAppender create( LogFile logFile, TxIdGenerator txIdGenerator,
                TransactionMetadataCache metadataCache, TransactionIdStore transactionIdStore )
        {
            return new PhysicalTransactionAppender( logFile, txIdGenerator,
                    metadataCache, transactionIdStore, IdOrderingQueue.BYPASS );
        }
    };

    private static final AppenderFactory BATCHING = new AppenderFactory()
    {
        @Override
        public TransactionAppender create( LogFile logFile, TxIdGenerator txIdGenerator,
                TransactionMetadataCache metadataCache, TransactionIdStore transactionIdStore )
        {
            return new BatchingPhysicalTransactionAppender( logFile, txIdGenerator,
                    metadataCache, transactionIdStore, IdOrderingQueue.BYPASS );
        }
    };
}
