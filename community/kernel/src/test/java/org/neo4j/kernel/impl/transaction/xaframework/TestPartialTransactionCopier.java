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
package org.neo4j.kernel.impl.transaction.xaframework;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import javax.transaction.xa.Xid;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.nioneo.store.StoreChannel;
import org.neo4j.kernel.impl.util.ArrayMap;
import org.neo4j.kernel.impl.util.DumpLogicalLog.CommandFactory;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.monitoring.ByteCounterMonitor;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.LogTestUtils;
import org.neo4j.test.LogTestUtils.LogHookAdapter;
import org.neo4j.test.TestGraphDatabaseFactory;

import static java.nio.ByteBuffer.allocate;

import static org.junit.Assert.assertThat;

import static org.neo4j.kernel.impl.nioneo.xa.CommandMatchers.nodeCommandEntry;
import static org.neo4j.kernel.impl.transaction.xaframework.LogIoUtils.readLogHeader;
import static org.neo4j.kernel.impl.transaction.xaframework.LogIoUtils.writeLogHeader;
import static org.neo4j.kernel.impl.transaction.xaframework.LogMatchers.containsExactly;
import static org.neo4j.kernel.impl.transaction.xaframework.LogMatchers.doneEntry;
import static org.neo4j.kernel.impl.transaction.xaframework.LogMatchers.logEntries;
import static org.neo4j.kernel.impl.transaction.xaframework.LogMatchers.onePhaseCommitEntry;
import static org.neo4j.kernel.impl.transaction.xaframework.LogMatchers.startEntry;
import static org.neo4j.test.LogTestUtils.filterNeostoreLogicalLog;

public class TestPartialTransactionCopier
{
    @Rule public EphemeralFileSystemRule fs = new EphemeralFileSystemRule();

    @SuppressWarnings( "unchecked" )
    @Test
    public void shouldCopyRunningTransactionsToNewLog() throws Exception
    {
        // Given
        int masterId = -1;
        int meId = -1;
        String storeDir = "dir";

        // I have a log with a transaction in the middle missing a "DONE" record
        Pair<File, Integer> broken = createBrokenLogFile( storeDir );
        File brokenLogFile = broken.first();
        Integer brokenTxIdentifier = broken.other();

        // And I've read the log header on that broken file
        StoreChannel brokenLog = fs.get().open( brokenLogFile, "rw" );
        ByteBuffer buffer = allocate( 9 + Xid.MAXGTRIDSIZE + Xid.MAXBQUALSIZE * 10 );
        readLogHeader( buffer, brokenLog, true );

        // And I have an awesome partial transaction copier
        PartialTransactionCopier copier = new PartialTransactionCopier(
                buffer, new CommandFactory(),
                StringLogger.DEV_NULL, new LogExtractor.LogPositionCache(),
                null, createXidMapWithOneStartEntry( masterId, /*txId=*/brokenTxIdentifier ),
                new Monitors().newMonitor( ByteCounterMonitor.class ) );

        // When
        File newLogFile = new File( "new.log" );
        copier.copy( brokenLog, createNewLogWithHeader( newLogFile ), 1 );

        // Then
        assertThat(
                logEntries( fs.get(), newLogFile ),
                containsExactly(
                        startEntry( brokenTxIdentifier, masterId, meId ),
                        nodeCommandEntry( brokenTxIdentifier, /*nodeId=*/1 ),
                        onePhaseCommitEntry( brokenTxIdentifier, /*txid=*/3 ),
                        // Missing done entry

                        startEntry( 5, masterId, meId ),
                        nodeCommandEntry( 5, /*nodeId=*/2),
                        onePhaseCommitEntry( 5, /*txid=*/4 ),
                        doneEntry( 5 ),

                        startEntry( 6, masterId, meId ),
                        nodeCommandEntry( 6, /*nodeId=*/3 ),
                        onePhaseCommitEntry( 6, /*txid=*/5 ),
                        doneEntry( 6 )
                ));
    }



    private ArrayMap<Integer, LogEntry.Start> createXidMapWithOneStartEntry( int masterId, Integer brokenTxId )
    {
        ArrayMap<Integer, LogEntry.Start> xidentMap = new ArrayMap<Integer, LogEntry.Start>();
        xidentMap.put( brokenTxId, new LogEntry.Start( null, brokenTxId, masterId, 3, 4, 5, 6 ) );
        return xidentMap;
    }

    private LogBuffer createNewLogWithHeader( File newLogFile ) throws IOException
    {
        StoreChannel newLog = fs.get().open( newLogFile, "rw" );
        LogBuffer newLogBuffer = new DirectLogBuffer( newLog, allocate(  10000 ) );

        ByteBuffer buf = allocate( 100 );
        writeLogHeader( buf, 1, /* we don't care about this */ 4 );
        newLogBuffer.getFileChannel().write( buf );

        return newLogBuffer;
    }

    private Pair<File, Integer> createBrokenLogFile( String storeDir ) throws Exception
    {
        GraphDatabaseAPI db = (GraphDatabaseAPI) new TestGraphDatabaseFactory()
                .setFileSystem( fs.get() ).newImpermanentDatabase( storeDir );
        for ( int i = 0; i < 4; i++ )
        {
            Transaction tx = db.beginTx();
            db.createNode();
            tx.success();
            tx.finish();
        }
        db.shutdown();

        // Remove the DONE record from the second transaction
        final AtomicInteger brokenTxIdentifier = new AtomicInteger();
        LogHookAdapter<LogEntry> filter = new LogTestUtils.LogHookAdapter<LogEntry>()
        {
            int doneRecordCount = 0;

            @Override
            public boolean accept( LogEntry item )
            {
                //System.out.println(item);
                if( item instanceof LogEntry.Done)
                {
                    doneRecordCount++;

                    // Accept everything except the second done record we find
                    if( doneRecordCount == 2)
                    {
                        brokenTxIdentifier.set( item.getIdentifier() );
                        return false;
                    }
                }

                // Not a done record, not our concern
                return true;
            }
        };
        File brokenLogFile = filterNeostoreLogicalLog( fs.get(),
                new File( storeDir, "nioneo_logical.log.v0"), filter );

        return Pair.of( brokenLogFile, brokenTxIdentifier.get() );
    }
}
