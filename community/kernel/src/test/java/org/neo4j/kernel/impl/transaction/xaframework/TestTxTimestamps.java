/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSetting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.nioneo.xa.Command;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.transaction.xaframework.LogEntry.Commit;
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
            setConfig( GraphDatabaseSettings.keep_logical_logs, GraphDatabaseSetting.TRUE ).newGraphDatabase();
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
        db.getXaDataSourceManager().getNeoStoreDataSource().rotateLogicalLog();
        
        ByteBuffer buffer = ByteBuffer.allocate( 1024*500 );
        FileChannel channel = fileSystem.open( new File( db.getStoreDir(), NeoStoreXaDataSource.LOGICAL_LOG_DEFAULT_NAME + ".v0" ), "r" );
        try
        {
            XaCommandFactory commandFactory = new CommandFactory();
            LogIoUtils.readLogHeader( buffer, channel, true );
            LogEntry entry = null;
            int foundTxCount = 0;
            skipFirstTransaction( buffer, channel, commandFactory ); // Since it's the property index transaction
            while ( (entry = LogIoUtils.readEntry( buffer, channel, commandFactory )) != null )
            {
                if ( entry instanceof LogEntry.Start )
                {
                    long diff = ((LogEntry.Start) entry).getTimeWritten()-expectedStartTimestamps[foundTxCount];
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
            }
            assertEquals( expectedCommitTimestamps.length, foundTxCount );
        }
        finally
        {
            channel.close();
        }
    }

    private void skipFirstTransaction( ByteBuffer buffer, FileChannel channel, XaCommandFactory commandFactory ) throws IOException
    {
        LogEntry entry = null;
        while ( (entry = LogIoUtils.readEntry( buffer, channel, commandFactory )) != null )
            if ( entry instanceof Commit )
                break;
    }

    private static class CommandFactory extends XaCommandFactory
    {
        @Override
        public XaCommand readCommand( ReadableByteChannel byteChannel,
                ByteBuffer buffer ) throws IOException
        {
            return Command.readCommand( null, null, byteChannel, buffer );
        }
    }
}
