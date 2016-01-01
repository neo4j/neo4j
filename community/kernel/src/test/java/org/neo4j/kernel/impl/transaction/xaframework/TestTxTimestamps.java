/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Settings;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.nioneo.xa.LogDeserializer;
import org.neo4j.kernel.impl.nioneo.store.StoreChannel;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.nioneo.xa.XaCommandReaderFactory;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.transaction.xaframework.LogEntry.Commit;
import org.neo4j.kernel.impl.util.Consumer;
import org.neo4j.kernel.impl.util.Cursor;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.impl.EphemeralFileSystemAbstraction;

public class TestTxTimestamps
{
    private final EphemeralFileSystemAbstraction fileSystem = new EphemeralFileSystemAbstraction();
    private GraphDatabaseAPI db;
    
    @Before
    public void doBefore() throws Exception
    {
        db = (GraphDatabaseAPI) new TestGraphDatabaseFactory().setFileSystem( fileSystem ).newImpermanentDatabaseBuilder().
            setConfig( GraphDatabaseSettings.keep_logical_logs, Settings.TRUE ).newGraphDatabase();
    }
    
    @After
    public void doAfter() throws Exception
    {
        db.shutdown();
    }
    
    @Test
    public void doIt() throws Exception
    {
        long[] expectedStartTimestamps = new long[10];
        long[] expectedCommitTimestamps = new long[expectedStartTimestamps.length];
        for ( int i = 0; i < expectedStartTimestamps.length; i++ )
        {
            Transaction tx = db.beginTx();
            expectedStartTimestamps[i] = System.currentTimeMillis();
            Node node = db.createNode();
            node.setProperty( "name", "Mattias " + i );
            tx.success();
            tx.finish();
            expectedCommitTimestamps[i] = System.currentTimeMillis();
        }
        db.getDependencyResolver().resolveDependency( XaDataSourceManager.class )
                .getNeoStoreDataSource().rotateLogicalLog();
        
        ByteBuffer buffer = ByteBuffer.allocate( 1024*500 );
        StoreChannel channel = fileSystem.open( new File( db.getStoreDir(),
                NeoStoreXaDataSource.LOGICAL_LOG_DEFAULT_NAME + ".v0" ), "r" );
        try
        {
            VersionAwareLogEntryReader.readLogHeader( buffer, channel, true );

            AConsumer consumer = new AConsumer( expectedCommitTimestamps, expectedStartTimestamps );

            LogDeserializer deserializer = new LogDeserializer( buffer, XaCommandReaderFactory.DEFAULT );

            try ( Cursor<LogEntry, IOException> cursor = deserializer.cursor( channel ) )
            {
                while( cursor.next( consumer ) );
            }

            assertEquals( expectedCommitTimestamps.length, consumer.getFoundTxCount() );
        }
        finally
        {
            channel.close();
        }
    }

    private class AConsumer implements Consumer<LogEntry, IOException>
    {
        private int foundTxCount;
        private boolean skippedFirstTx = false;
        private final long[] expectedCommitTimestamps;
        private final long[] expectedStartTimestamps;

        private AConsumer( long[] expectedCommitTimestamps, long[] expectedStartTimestamps )
        {
            this.expectedCommitTimestamps = expectedCommitTimestamps;
            this.expectedStartTimestamps = expectedStartTimestamps;
        }

        public int getFoundTxCount()
        {
            return foundTxCount;
        }

        @Override
        public boolean accept( LogEntry entry ) throws IOException
        {
            if ( !skippedFirstTx )
            {   // Since it's the property index transaction
                if ( entry instanceof Commit )
                {
                    skippedFirstTx = true;
                }
                return true;
            }
            if ( entry instanceof LogEntry.Start )
            {
                long diff = ((LogEntry.Start) entry).getTimeWritten() - expectedStartTimestamps[foundTxCount];
                long exp = expectedCommitTimestamps[foundTxCount] - expectedStartTimestamps[foundTxCount];
                assertTrue( diff + " <= " + exp, diff <= exp );
            }
            else if ( entry instanceof LogEntry.Commit )
            {
                long diff = ((LogEntry.Commit) entry).getTimeWritten()-expectedCommitTimestamps[foundTxCount];
                long exp = expectedCommitTimestamps[foundTxCount] - expectedStartTimestamps[foundTxCount];
                assertTrue( diff + " <= " + exp, diff <= exp );
                foundTxCount++;
            }
            return true;
        }
    }
}
