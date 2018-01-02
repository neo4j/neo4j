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
package org.neo4j.index.impl.lucene;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.function.Predicate;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.helpers.collection.FilteringIterator;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.impl.index.IndexDefineCommand;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.log.IOCursor;
import org.neo4j.kernel.impl.transaction.log.LogEntryCursor;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFile;
import org.neo4j.kernel.impl.transaction.log.ReadableVersionableLogChannel;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommand;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
import org.neo4j.kernel.impl.transaction.log.rotation.LogRotation;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TestGraphDatabaseFactory;

import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test for a problem where multiple threads getting an index for the first time
 * and adding to or removing from it right there after. There was a race condition
 * where the transaction which created the index came after the first one using it.
 *
 * @author Mattias Persson
 */
public class IndexCreationTest
{
    @Rule
    public final TargetDirectory.TestDirectory testDirectory = TargetDirectory.testDirForTest( getClass() );

    private GraphDatabaseAPI db;

    @Before
    public void before() throws Exception
    {
        db = (GraphDatabaseAPI) new TestGraphDatabaseFactory().newEmbeddedDatabase( testDirectory.graphDbDir() );
    }

    @After
    public void after() throws Exception
    {
        db.shutdown();
    }

    @Test
    public void indexCreationConfigRaceCondition() throws Exception
    {
        // Since this is a probability test and not a precise test run do the run
        // a couple of times to be sure.
        for ( int run = 0; run < 10; run++ )
        {
            final int r = run;
            final CountDownLatch latch = new CountDownLatch( 1 );
            ExecutorService executor = newCachedThreadPool();
            for ( int thread = 0; thread < 10; thread++ )
            {
                executor.submit( new Runnable()
                {
                    @Override
                    public void run()
                    {
                        try ( Transaction tx = db.beginTx() )
                        {
                            latch.await();
                            Index<Node> index = db.index().forNodes( "index" + r );
                            Node node = db.createNode();
                            index.add( node, "name", "Name" );
                            tx.success();
                        }
                        catch ( InterruptedException e )
                        {
                            Thread.interrupted();
                        }
                    }
                } );
            }
            latch.countDown();
            executor.shutdown();
            executor.awaitTermination( 10, TimeUnit.SECONDS );

            verifyThatIndexCreationTransactionIsTheFirstOne();
        }
    }

    private void verifyThatIndexCreationTransactionIsTheFirstOne() throws Exception
    {
        NeoStoreDataSource ds = db.getDependencyResolver().resolveDependency( NeoStoreDataSource.class );
        PhysicalLogFile pLogFile = db.getDependencyResolver().resolveDependency( PhysicalLogFile.class );
        long version = ds.getCurrentLogVersion();
        db.getDependencyResolver().resolveDependency( LogRotation.class ).rotateLogFile();
        db.getDependencyResolver().resolveDependency( CheckPointer.class ).forceCheckPoint(
                new SimpleTriggerInfo( "test" )
        );

        ReadableVersionableLogChannel logChannel = pLogFile.getReader( LogPosition.start( version ) );

        final AtomicBoolean success = new AtomicBoolean( false );

        try ( IOCursor<LogEntry> cursor = new LogEntryCursor( logChannel ) )
        {
            List<Command> commandsInFirstEntry = new ArrayList<>();
            boolean startFound = false;

            while ( cursor.next() )
            {
                LogEntry entry = cursor.get();

                if ( entry instanceof LogEntryStart )
                {
                    if ( startFound )
                    {
                        throw new IllegalArgumentException( "More than one start entry" );
                    }
                    startFound = true;
                }

                if ( startFound && entry instanceof LogEntryCommand )
                {
                    commandsInFirstEntry.add( entry.<LogEntryCommand>as().getXaCommand() );
                }

                if ( entry instanceof LogEntryCommit )
                {
                    // The first COMMIT
                    assertTrue( startFound );
                    assertFalse( "Index creation transaction wasn't the first one", commandsInFirstEntry.isEmpty() );
                    List<Command> createCommands = IteratorUtil.asList( new FilteringIterator<>(
                            commandsInFirstEntry.iterator(),
                            new Predicate<Command>()
                            {
                                @Override
                                public boolean test( Command item )
                                {
                                    return item instanceof IndexDefineCommand;

                                }
                            }
                    ) );
                    assertEquals( 1, createCommands.size() );
                    success.set( true );
                    break;
                }
            }
        }


        assertTrue( "Didn't find any commit record in log " + version, success.get() );
    }
}
