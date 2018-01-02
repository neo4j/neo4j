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
package org.neo4j.index;

import java.io.File;
import java.text.DecimalFormat;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;

import static java.lang.String.format;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertThat;

public class ReadIndexWritesUnderConcurrentLoadStressIT
{
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();
    public static final int TX_COUNT = 16_000;
    public static final int THREAD_COUNT = 8;
    public static final DecimalFormat COUNT_FORMAT = new DecimalFormat( "###,###,###,###,##0" );
    public static final DecimalFormat THROUGHPUT_FORMAT = new DecimalFormat( "###,###,###,###,##0.00" );
    public static final Label LABEL = DynamicLabel.label( "Label" );
    public static final String PROPERTY_KEY = "key";

    @Test
    public void shouldReadNodeWrittenInPreviousTransaction() throws Throwable
    {
        File dbDir = temporaryFolder.newFolder();
        GraphDatabaseService db = new GraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( dbDir )
                .setConfig( GraphDatabaseSettings.pagecache_memory, "2000M" )
                .setConfig( GraphDatabaseSettings.logical_log_rotation_threshold, "500M" )
                .newGraphDatabase();

        try ( Transaction tx = db.beginTx() )
        {
            db.schema().constraintFor( LABEL ).assertPropertyIsUnique( PROPERTY_KEY ).create();
            tx.success();
        }

        int threadTxCount = TX_COUNT / THREAD_COUNT;
        int startOfRange;
        int endOfRange = -1;
        CountDownLatch startSignal = new CountDownLatch( 1 );
        CountDownLatch finishSignal = new CountDownLatch( THREAD_COUNT );
        AtomicBoolean failed = new AtomicBoolean( false );
        AtomicLong txs = new AtomicLong( 0 );
        for ( int i = 0; i < THREAD_COUNT; i++ )
        {
            startOfRange = 1 + endOfRange;
            endOfRange = startOfRange + threadTxCount - 1;
            System.out.println( format( "Thread=%s, Txs=%s, %s -> %s",
                    COUNT_FORMAT.format( i ),
                    COUNT_FORMAT.format( threadTxCount ),
                    COUNT_FORMAT.format( startOfRange ),
                    COUNT_FORMAT.format( endOfRange ) ) );
            new LostWritesThread(
                    startOfRange,
                    endOfRange,
                    failed,
                    LABEL,
                    PROPERTY_KEY,
                    db,
                    startSignal,
                    finishSignal,
                    txs
            ).start();
        }

        startSignal.countDown();
        long startTime = System.currentTimeMillis();
        long finishTime;
        long prevTxs = 0;
        while ( !finishSignal.await( 2, TimeUnit.SECONDS ) )
        {
            long currTxs = txs.get();
            finishTime = System.currentTimeMillis();
            printProgress( currTxs, prevTxs, startTime, finishTime );
            assertThat( failed.get(), is( false ) );
            prevTxs = currTxs;
            startTime = System.currentTimeMillis();
        }
        printProgress( txs.get(), prevTxs, startTime, System.currentTimeMillis() );
        assertThat( failed.get(), is( false ) );
    }

    private void printProgress( long currTxs, long prevTxs, long startTime, long finishTime )
    {
        System.out.println( format( "Processed %s tx @ %s tx/s",
                COUNT_FORMAT.format( currTxs ),
                THROUGHPUT_FORMAT.format( (double) (currTxs - prevTxs) / (finishTime - startTime) * 1_000 ) ) );
    }

    static class LostWritesThread extends Thread
    {
        private final int startOfRange;
        private final int endOfRange;
        private final AtomicBoolean failed;
        private final Label label;
        private final String propertyKey;
        private final GraphDatabaseService db;
        private final CountDownLatch startSignal;
        private final CountDownLatch finishSignal;
        private final AtomicLong txs;

        public LostWritesThread(
                int startOfRange,
                int endOfRange,
                AtomicBoolean failed,
                Label label,
                String propertyKey,
                GraphDatabaseService db,
                CountDownLatch startSignal,
                CountDownLatch finishSignal,
                AtomicLong txs )
        {
            this.startOfRange = startOfRange;
            this.endOfRange = endOfRange;
            this.failed = failed;
            this.label = label;
            this.propertyKey = propertyKey;
            this.db = db;
            this.startSignal = startSignal;
            this.finishSignal = finishSignal;
            this.txs = txs;
        }

        @Override
        public void run()
        {
            try
            {
                startSignal.await();
            }
            catch ( InterruptedException ex )
            {
                System.out.println( "Thread was interrupted" );
                return;
            }

            try ( Transaction tx = db.beginTx() )
            {
                Node node = db.createNode( label );
                node.setProperty( propertyKey, startOfRange );
                tx.success();
                txs.incrementAndGet();
            }

            for ( int i = startOfRange + 1; i <= endOfRange; i++ )
            {
                try ( Transaction tx = db.beginTx() )
                {
                    Node node = db.createNode( label );
                    node.setProperty( propertyKey, i );
                    Node previousNode = db.findNode( label, propertyKey, i - 1 );
                    assertThat( format( "Error at tx %s", i ), previousNode, not( nullValue() ) );
                    tx.success();
                    txs.incrementAndGet();
                }
                catch ( Throwable e )
                {
                    failed.set( true );
                    e.printStackTrace();
                    break;
                }
            }
            finishSignal.countDown();
        }
    }
}
