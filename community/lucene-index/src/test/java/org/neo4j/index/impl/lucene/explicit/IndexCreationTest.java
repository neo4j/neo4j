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
package org.neo4j.index.impl.lucene.explicit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Resource;

import org.neo4j.cursor.IOCursor;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.helpers.collection.FilteringIterator;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.kernel.impl.index.IndexDefineCommand;
import org.neo4j.kernel.impl.transaction.log.LogEntryCursor;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogVersionRepository;
import org.neo4j.kernel.impl.transaction.log.ReadableLogChannel;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommand;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.rotation.LogRotation;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test for a problem where multiple threads getting an index for the first time
 * and adding to or removing from it right there after. There was a race condition
 * where the transaction which created the index came after the first one using it.
 *
 * @author Mattias Persson
 */
@ExtendWith( TestDirectoryExtension.class )
public class IndexCreationTest
{
    @Resource
    public TestDirectory testDirectory;

    private GraphDatabaseAPI db;

    @BeforeEach
    public void before()
    {
        db = (GraphDatabaseAPI) new TestGraphDatabaseFactory().newEmbeddedDatabase( testDirectory.graphDbDir() );
    }

    @AfterEach
    public void after()
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
                executor.submit( () ->
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
        LogFiles logFiles = db.getDependencyResolver().resolveDependency( LogFiles.class );
        long version = db.getDependencyResolver().resolveDependency( LogVersionRepository.class ).getCurrentLogVersion();
        db.getDependencyResolver().resolveDependency( LogRotation.class ).rotateLogFile();
        db.getDependencyResolver().resolveDependency( CheckPointer.class ).forceCheckPoint(
                new SimpleTriggerInfo( "test" )
        );

        ReadableLogChannel logChannel = logFiles.getLogFile().getReader( LogPosition.start( version ) );

        final AtomicBoolean success = new AtomicBoolean( false );

        try ( IOCursor<LogEntry> cursor = new LogEntryCursor( new VersionAwareLogEntryReader<>(), logChannel ) )
        {
            List<StorageCommand> commandsInFirstEntry = new ArrayList<>();
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
                    commandsInFirstEntry.add( entry.<LogEntryCommand>as().getCommand() );
                }

                if ( entry instanceof LogEntryCommit )
                {
                    // The first COMMIT
                    assertTrue( startFound );
                    assertFalse( commandsInFirstEntry.isEmpty(), "Index creation transaction wasn't the first one" );
                    List<StorageCommand> createCommands = Iterators.asList( new FilteringIterator<>(
                            commandsInFirstEntry.iterator(),
                            item -> item instanceof IndexDefineCommand
                    ) );
                    assertEquals( 1, createCommands.size() );
                    success.set( true );
                    break;
                }
            }
        }

        assertTrue( success.get(), "Didn't find any commit record in log " + version );
    }
}
