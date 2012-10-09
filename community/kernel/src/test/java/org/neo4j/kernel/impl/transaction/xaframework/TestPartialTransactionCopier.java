/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import static org.junit.Assert.*;
import static org.neo4j.kernel.impl.nioneo.xa.CommandMatchers.nodeCommandEntry;
import static org.neo4j.kernel.impl.transaction.xaframework.LogMatchers.containsExactly;
import static org.neo4j.kernel.impl.transaction.xaframework.LogMatchers.doneEntry;
import static org.neo4j.kernel.impl.transaction.xaframework.LogMatchers.logEntries;
import static org.neo4j.kernel.impl.transaction.xaframework.LogMatchers.onePhaseCommitEntry;
import static org.neo4j.kernel.impl.transaction.xaframework.LogMatchers.startEntry;
import static org.neo4j.kernel.impl.util.DumpLogicalLog.CommandFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import javax.transaction.xa.Xid;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.util.ArrayMap;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.test.LogTestUtils;
import org.neo4j.test.TargetDirectory;

public class TestPartialTransactionCopier
{

    public static final String NEW_LOG_FILENAME = "new.log";
    TargetDirectory targetDir = TargetDirectory.forTest( getClass() );

    @Rule
    public TargetDirectory.TestDirectory testDir = targetDir.cleanTestDirectory();

    public class EverythingButDoneRecordFilter implements LogTestUtils.LogHook<LogEntry>
    {

        int doneRecordCount = 0;
        public Integer brokenTxIdentifier = null;

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
                    brokenTxIdentifier = item.getIdentifier();
                    return false;
                }
            }

            // Not a done record, not our concern
            return true;
        }

        @Override
        public void file( File file )
        {
        }

        @Override
        public void done( File file )
        {
        }
    };

    @Test
    public void temp() throws Exception
    {
        // Given
        int masterId = -1;
        int meId = -1;
        File newLogFile = new File( testDir.directory(), NEW_LOG_FILENAME );

        // I have a log that with a transaction in the middle missing a "DONE" record
        Pair<File, Integer> broken = createBrokenLogFile();
        File brokenLogFile = broken.first();
        Integer brokenTxIdentifier = broken.other();

        // And I've read the log header on that broken file
        FileChannel brokenLog = new RandomAccessFile( brokenLogFile, "rw" ).getChannel();
        LogIoUtils.readLogHeader( ByteBuffer.allocate( 9 + Xid.MAXGTRIDSIZE
                + Xid.MAXBQUALSIZE * 10 ), brokenLog, true );

        // And I have an awesome partial transaction copier
        PartialTransactionCopier copier = new PartialTransactionCopier( ByteBuffer.allocate( 9 + Xid.MAXGTRIDSIZE
                + Xid.MAXBQUALSIZE * 10 ),
                new CommandFactory(),
                StringLogger.DEV_NULL,
                new LogExtractor.LogPositionCache(),
                null,
                createXidMapWithOneStartEntry( masterId, /*txId=*/brokenTxIdentifier ) );

        // When
        copier.copy( brokenLog, createNewLogWithHeader(newLogFile), 1 );

        // Then
        assertThat(
                logEntries( newLogFile ),
                containsExactly(
                        startEntry( brokenTxIdentifier, masterId, meId ),
                        nodeCommandEntry( brokenTxIdentifier, /*nodeId=*/2 ),
                        onePhaseCommitEntry( brokenTxIdentifier, /*txid=*/brokenTxIdentifier ),

                        startEntry( 4, masterId, meId ),
                        nodeCommandEntry( 4, /*nodeId=*/3),
                        onePhaseCommitEntry( 4, /*txid=*/4 ),
                        doneEntry( 4 ),

                        startEntry( 5, masterId, meId ),
                        nodeCommandEntry( 5, /*nodeId=*/4 ),
                        onePhaseCommitEntry( 5, /*txid=*/5 ),
                        doneEntry( 5 )
                ));

    }



    private ArrayMap<Integer, LogEntry.Start> createXidMapWithOneStartEntry( int masterId, Integer brokenTxId )
    {
        ArrayMap<Integer, LogEntry.Start> xidentMap = new ArrayMap<Integer, LogEntry.Start>();
        xidentMap.put( brokenTxId, new LogEntry.Start( null, brokenTxId, masterId, 3, 4, 5 ) );
        return xidentMap;
    }

    private LogBuffer createNewLogWithHeader( File newLogFile ) throws IOException
    {
        FileChannel newLog = new RandomAccessFile( new File( testDir.directory(), NEW_LOG_FILENAME ), "rw" ).getChannel();
        LogBuffer newLogBuffer = new DirectLogBuffer( newLog, ByteBuffer.allocate(  10000 ) );

        ByteBuffer buf = ByteBuffer.allocate( 100 );
        LogIoUtils.writeLogHeader( buf, 1, /* we don't care about this */ 4 );
        newLogBuffer.getFileChannel().write( buf );

        return newLogBuffer;
    }

    private Pair<File, Integer> createBrokenLogFile() throws Exception
    {
        // Given a database with three committed transactions
        String storeDir = testDir.directory().getAbsolutePath();

        GraphDatabaseAPI db = (GraphDatabaseAPI) new GraphDatabaseFactory().newEmbeddedDatabase( storeDir );

        runSmallWriteTransaction( db );
        runSmallWriteTransaction( db );
        runSmallWriteTransaction( db );
        runSmallWriteTransaction( db );

        db.shutdown();

        // And the middle transaction is lacking a "done" record
        EverythingButDoneRecordFilter filter = new EverythingButDoneRecordFilter();
        File logFile = LogTestUtils.filterNeostoreLogicalLog( new File( storeDir, "nioneo_logical.log.v0"), filter );

        return Pair.of( logFile, filter.brokenTxIdentifier );
    }

    private void runSmallWriteTransaction( GraphDatabaseAPI db )
    {
        Transaction tx2 = db.beginTx();
        db.createNode();
        tx2.success();
        tx2.finish();
    }

}
