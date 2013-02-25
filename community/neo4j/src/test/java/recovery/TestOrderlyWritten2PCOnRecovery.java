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
package recovery;

import static java.nio.ByteBuffer.allocate;
import static org.junit.Assert.assertEquals;
import static org.neo4j.helpers.Exceptions.launderedException;
import static org.neo4j.kernel.impl.transaction.xaframework.LogIoUtils.readEntry;
import static org.neo4j.kernel.impl.transaction.xaframework.LogIoUtils.readLogHeader;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.util.concurrent.CountDownLatch;

import javax.transaction.xa.Xid;

import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.nioneo.xa.Command;
import org.neo4j.kernel.impl.transaction.xaframework.LogEntry;
import org.neo4j.kernel.impl.transaction.xaframework.LogEntry.TwoPhaseCommit;
import org.neo4j.kernel.impl.transaction.xaframework.XaCommand;
import org.neo4j.kernel.impl.transaction.xaframework.XaCommandFactory;
import org.neo4j.kernel.impl.transaction.xaframework.XaResourceManager;
import org.neo4j.kernel.impl.util.ArrayMap;
import org.neo4j.test.AbstractSubProcessTestBase;
import org.neo4j.test.subprocess.BreakPoint;
import org.neo4j.test.subprocess.DebugInterface;
import org.neo4j.test.subprocess.DebuggedThread;
import org.neo4j.test.subprocess.KillSubProcess;

@Ignore( "Doesn't work yet" )
public class TestOrderlyWritten2PCOnRecovery extends AbstractSubProcessTestBase
{
    private static final int TX_COUNT = 10;
    
    private DebuggedThread committer;
    private final CountDownLatch commitLatch = new CountDownLatch( 1 );
    private final CountDownLatch latch = new CountDownLatch( TX_COUNT );
    
    private final BreakPoint commit = new BreakPoint( XaResourceManager.class, "commit", Xid.class, Boolean.TYPE )
    {
        private volatile int letPass = 1;
        
        @Override
        protected void callback( DebugInterface debug ) throws KillSubProcess
        {
            int pass = letPass--;
            if ( pass > 0 ) return;
            else if ( pass == 0 )
            {
                committer = debug.thread().suspend( this );
                commitLatch.countDown();
            }
            else
            {
                latch.countDown();
            }
        }
    };
    private final BreakPoint continueCommitting = new BreakPoint( getClass(), "pleaseContinue" )
    {
        @Override
        protected void callback( DebugInterface debug ) throws KillSubProcess
        {
            System.out.println( "continue" );
            committer.resume();
            latch.countDown();
        }
    };
    
    static void pleaseContinue()
    {   // Triggers breakpoint
    }
    
    @Override
    protected BreakPoint[] breakpoints( int id )
    {
        return new BreakPoint[] { commit.enable(), continueCommitting.enable() };
    }
    
    private static class CreateIndexedNodeTask implements Task
    {
        @Override
        public void run( GraphDatabaseAPI graphdb )
        {
            Transaction tx = graphdb.beginTx();
            try
            {
                Node node = graphdb.createNode();
                graphdb.index().forNodes( "index" ).add( node, "key", "value" );
                tx.success();
            }
            finally
            {
                tx.finish();
            }
        }
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
    
    private static class MessUpTask implements Task
    {
        @Override
        public void run( GraphDatabaseAPI graphdb )
        {
            try
            {
                XaResourceManager resourceManager = graphdb.getXaDataSourceManager().getNeoStoreDataSource().getXaContainer().getResourceManager();
                ArrayMap<Xid, ?> xidMap = (ArrayMap<Xid, ?>) inaccessibleField( resourceManager, "xidMap" ).get( resourceManager );
                xidMap.clear();
            }
            catch ( Exception e )
            {
                throw launderedException( e );
            }
            finally
            {
                pleaseContinue();
            }
        }
    }
    
    @Test
    public void recovered2PCRecordsShouldBeWrittenInRisingTxIdOrder() throws Exception
    {
        /* Will start many 2PC transactions and halt them right before committing, creating
         * many 2PC transactions which will have to recovered and committed during recovery. */
        for ( int i = 0; i < TX_COUNT; i++ ) runInThread( new CreateIndexedNodeTask() );
        commitLatch.await();
        
        run( new MessUpTask() );
        latch.await();
        
        /* Restart and recover */
        restart();
        
        verifyOrderedRecords();
    }

    private void verifyOrderedRecords() throws FileNotFoundException, IOException
    {
        /* Look in the .v0 log for the 2PC records and that they are ordered by txId */
        RandomAccessFile file = new RandomAccessFile( new File( getStoreDir( this, 0 ), "nioneo_logical.log.v0" ), "r" );
        CommandFactory cf = new CommandFactory();
        try
        {
            FileChannel channel = file.getChannel();
            ByteBuffer buffer = allocate( 10000 );
            readLogHeader( buffer, channel, true );
            long lastOne = -1;
            int counted = 0;
            for ( LogEntry entry = null; (entry = readEntry( buffer, channel, cf )) != null; )
            {
                if ( entry instanceof TwoPhaseCommit )
                {
                    long txId = ((TwoPhaseCommit) entry).getTxId();
                    if ( lastOne == -1 ) txId = lastOne;
                    else assertEquals( lastOne+1, txId );
                    lastOne = txId;
                    counted++;
                }
            }
            assertEquals( TX_COUNT, counted );
        }
        finally
        {
            file.close();
        }
    }
}
