/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.transaction.xaframework.LogEntry;
import org.neo4j.kernel.impl.transaction.xaframework.LogIoUtils;
import org.neo4j.kernel.impl.transaction.xaframework.XaCommand;
import org.neo4j.kernel.impl.transaction.xaframework.XaCommandFactory;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;
import org.neo4j.test.TestGraphDatabaseFactory;

import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.kernel.impl.transaction.xaframework.LogExtractor.newLogReaderBuffer;
import static org.neo4j.kernel.impl.transaction.xaframework.LogIoUtils.readLogHeader;

/**
 * Test for a problem where multiple threads getting an index for the first time
 * and adding to or removing from it right there after. There was a race condition
 * where the transaction which created the index came after the first one using it.
 * 
 * @author Mattias Persson
 */
public class TestIndexCreation
{
    private GraphDatabaseAPI db;
    
    @Before
    public void before() throws Exception
    {
        db = (GraphDatabaseAPI)new TestGraphDatabaseFactory().newImpermanentDatabase();
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
                        Transaction tx = db.beginTx();
                        try
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
                        finally
                        {
                            tx.finish();
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
        XaDataSource ds = db.getDependencyResolver().resolveDependency( XaDataSourceManager.class ).getXaDataSource(
                LuceneDataSource.DEFAULT_NAME );
        long version = ds.getCurrentLogVersion();
        ds.rotateLogicalLog();
        ReadableByteChannel log = ds.getLogicalLog( version );
        ByteBuffer buffer = newLogReaderBuffer();
        readLogHeader( buffer, log, true );
        XaCommandFactory cf = new XaCommandFactory()
        {
            @Override
            public XaCommand readCommand( ReadableByteChannel byteChannel, ByteBuffer buffer ) throws IOException
            {
                return LuceneCommand.readCommand( byteChannel, buffer, null );
            }
        };
        int creationIdentifier = -1;
        for ( LogEntry entry; (entry = LogIoUtils.readEntry( buffer, log, cf )) != null; )
        {
            if ( entry instanceof LogEntry.Command && ((LogEntry.Command) entry).getXaCommand() instanceof LuceneCommand.CreateIndexCommand )
            {
                if ( creationIdentifier != -1 )
                    throw new IllegalArgumentException( "More than one creation command" );
                creationIdentifier = entry.getIdentifier();
            }
            
            if ( entry instanceof LogEntry.Commit )
            {
                // The first COMMIT
                assertTrue( "Index creation transaction wasn't the first one", creationIdentifier != -1 );
                assertEquals( "Index creation transaction wasn't the first one", creationIdentifier, entry.getIdentifier() );
                return;
            }
        }
        
        fail( "Didn't find any commit record in log " + version );
    }
}
