/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.kernel.api.impl.fulltext;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;

import org.neo4j.function.ThrowingAction;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.SchemaWrite;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.IndexDescriptorFactory;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.logging.Level;
import org.neo4j.logging.Log;
import org.neo4j.logging.async.AsyncLogEvent;
import org.neo4j.logging.async.AsyncLogProvider;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.schema.SchemaDescriptor;
import org.neo4j.test.Race;
import org.neo4j.test.rule.RepeatRule;
import org.neo4j.util.concurrent.AsyncEvents;

import static org.junit.Assert.assertEquals;
import static org.neo4j.common.EntityType.NODE;

/**
 * Concurrent updates and index changes should result in valid state, and not create conflicts or exceptions during
 * commit.
 */
public class ConcurrentLuceneFulltextUpdaterTest extends LuceneFulltextTestSupport
{
    private final int aliceThreads = 1;
    private final int bobThreads = 1;
    private final int nodesCreatedPerThread = 500;
    private Race race;
    private CountDownLatch aliceLatch = new CountDownLatch( 2 );
    private CountDownLatch bobLatch = new CountDownLatch( 2 );
    private JobHandle handle;
    private OutputStream logFile;
    private AsyncEvents<AsyncLogEvent> events;
    private Log log;

    @Override
    protected RepeatRule createRepeatRule()
    {
        return new RepeatRule( false, 1 );
    }

    @Before
    public void createRace() throws Exception
    {
        race = new Race();
        JobScheduler scheduler = db.resolveDependency( JobScheduler.class );
        logFile = new FileOutputStream( db.databaseLayout().file( "fts-events.txt" ) );
        FormattedLogProvider logProvider = FormattedLogProvider.withDefaultLogLevel( Level.DEBUG ).toOutputStream( logFile );
        events = new AsyncEvents<>( AsyncLogEvent::process, AsyncEvents.Monitor.NONE );
        handle = scheduler.schedule( Group.FILE_IO_HELPER, events );
        events.awaitStartup();
        AsyncLogProvider asyncLogProvider = new AsyncLogProvider( events, logProvider );
        log = asyncLogProvider.getLog( ConcurrentLuceneFulltextUpdaterTest.class );
        FulltextIndexAccessor.TRACE_LOG = asyncLogProvider.getLog( FulltextIndexAccessor.class );
        FulltextIndexPopulator.TRACE_LOG = asyncLogProvider.getLog( FulltextIndexPopulator.class );
    }

    @After
    public void stopLogger() throws IOException
    {
        handle.cancel( false );
        events.shutdown();
        events.awaitTermination();
        logFile.flush();
        logFile.close();
    }

    private SchemaDescriptor getNewDescriptor( String[] entityTokens )
    {
        return fulltextAdapter.schemaFor( NODE, entityTokens, settings, "otherProp" );
    }

    private SchemaDescriptor getExistingDescriptor( String[] entityTokens )
    {
        return fulltextAdapter.schemaFor( NODE, entityTokens, settings, PROP );
    }

    private IndexReference createInitialIndex( SchemaDescriptor descriptor ) throws Exception
    {
        IndexReference index;
        try ( KernelTransactionImplementation transaction = getKernelTransaction() )
        {
            SchemaWrite schemaWrite = transaction.schemaWrite();
            index = schemaWrite.indexCreate( descriptor, FulltextIndexProviderFactory.DESCRIPTOR.name(), Optional.of( "nodes" ) );
            transaction.success();
        }
        await( index );
        return index;
    }

    private void raceContestantsAndVerifyResults( SchemaDescriptor newDescriptor, Runnable aliceWork, Runnable changeConfig, Runnable bobWork ) throws Throwable
    {
        race.addContestants( aliceThreads, aliceWork );
        race.addContestant( changeConfig );
        race.addContestants( bobThreads, bobWork );
        race.go();
        await( IndexDescriptorFactory.forSchema( newDescriptor, Optional.of( "nodes" ), FulltextIndexProviderFactory.DESCRIPTOR ) );
        try ( Transaction tx = db.beginTx() )
        {
            KernelTransaction ktx = kernelTransaction( tx );
            IndexReadSession index = ktx.dataRead().indexReadSession( ktx.schemaRead().indexGetForName( "nodes" ) );
            try ( NodeValueIndexCursor bobCursor = ktx.cursors().allocateNodeValueIndexCursor() )
            {
                ktx.dataRead().nodeIndexSeek( index, bobCursor, IndexOrder.NONE, false, IndexQuery.fulltextSearch( "bob" ) );
                int bobCount = 0;
                while ( bobCursor.next() )
                {
                    bobCount += 1;
                }
                assertEquals( bobThreads * nodesCreatedPerThread, bobCount );
            }
            try ( NodeValueIndexCursor aliceCursor = ktx.cursors().allocateNodeValueIndexCursor() )
            {
                ktx.dataRead().nodeIndexSeek( index, aliceCursor, IndexOrder.NONE, false, IndexQuery.fulltextSearch( "alice" ) );
                int aliceCount = 0;
                while ( aliceCursor.next() )
                {
                    aliceCount += 1;
                }
                assertEquals( 0, aliceCount );
            }
        }
    }

    private Runnable work( int iterations, ThrowingAction<Exception> work )
    {
        return () ->
        {
            try
            {
                for ( int i = 0; i < iterations; i++ )
                {
                    Thread.yield();
                    try ( Transaction tx = db.beginTx() )
                    {
                        Thread.yield();
                        work.apply();
                        Thread.yield();
                        tx.success();
                    }
                }
            }
            catch ( Exception e )
            {
                throw new AssertionError( e );
            }
        };
    }

    private ThrowingAction<Exception> dropAndReCreateIndex( IndexReference descriptor, SchemaDescriptor newDescriptor )
    {
        return () ->
        {
            aliceLatch.await();
            bobLatch.await();
            try ( KernelTransactionImplementation transaction = getKernelTransaction() )
            {
                SchemaWrite schemaWrite = transaction.schemaWrite();
                schemaWrite.indexDrop( descriptor );
                schemaWrite.indexCreate( newDescriptor, FulltextIndexProviderFactory.DESCRIPTOR.name(), Optional.of( "nodes" ) );
                transaction.success();
                log.debug( "drop an recreate" );
            }
        };
    }

    @Test
    public void labelledNodesCoreAPI() throws Throwable
    {
        String[] entityTokens = {LABEL.name()};
        SchemaDescriptor descriptor = getExistingDescriptor( entityTokens );
        SchemaDescriptor newDescriptor = getNewDescriptor( entityTokens );
        IndexReference initialIndex = createInitialIndex( descriptor );

        Runnable aliceWork = work( nodesCreatedPerThread, () ->
        {
            db.getNodeById( createNodeIndexableByPropertyValue( LABEL, "alice" ) );
            log.debug( "core api created an alice" );
            aliceLatch.countDown();
        } );
        Runnable bobWork = work( nodesCreatedPerThread, () ->
        {
            db.getNodeById( createNodeWithProperty( LABEL, "otherProp", "bob" ) );
            log.debug( "core api created a bob" );
            bobLatch.countDown();
        } );
        Runnable changeConfig = work( 1, dropAndReCreateIndex( initialIndex, newDescriptor ) );
        raceContestantsAndVerifyResults( newDescriptor, aliceWork, changeConfig, bobWork );
    }

    @Test
    public void labelledNodesCypherCurrent() throws Throwable
    {
        String[] entityTokens = {LABEL.name()};
        SchemaDescriptor descriptor = getExistingDescriptor( entityTokens );
        SchemaDescriptor newDescriptor = getNewDescriptor( entityTokens );
        IndexReference initialIndex = createInitialIndex( descriptor );

        Runnable aliceWork = work( nodesCreatedPerThread, () ->
        {
            db.execute( "create (:LABEL {" + PROP + ": \"alice\"})" ).close();
            log.debug( "cypher current created an alice" );
            aliceLatch.countDown();
        } );
        Runnable bobWork = work( nodesCreatedPerThread, () ->
        {
            db.execute( "create (:LABEL {otherProp: \"bob\"})" ).close();
            log.debug( "cypher current created a bob" );
            bobLatch.countDown();
        } );
        Runnable changeConfig = work( 1, dropAndReCreateIndex( initialIndex, newDescriptor ) );
        raceContestantsAndVerifyResults( newDescriptor, aliceWork, changeConfig, bobWork );
    }
}
