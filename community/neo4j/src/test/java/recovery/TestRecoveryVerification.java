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
package recovery;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSetting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.index.IndexProvider;
import org.neo4j.helpers.Service;
import org.neo4j.kernel.InternalAbstractGraphDatabase;
import org.neo4j.kernel.KernelExtension;
import org.neo4j.kernel.impl.cache.CacheProvider;
import org.neo4j.kernel.impl.transaction.xaframework.LogEntry;
import org.neo4j.kernel.impl.transaction.xaframework.LogEntry.TwoPhaseCommit;
import org.neo4j.kernel.impl.transaction.xaframework.RecoveryVerificationException;
import org.neo4j.kernel.impl.transaction.xaframework.RecoveryVerifier;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionInfo;
import org.neo4j.kernel.impl.util.DumpLogicalLog.CommandFactory;
import org.neo4j.kernel.lifecycle.LifecycleException;

import static java.nio.ByteBuffer.*;
import static org.junit.Assert.*;
import static org.neo4j.helpers.collection.MapUtil.*;
import static org.neo4j.kernel.impl.transaction.xaframework.LogIoUtils.*;
import static recovery.CreateTransactionsAndDie.*;

public class TestRecoveryVerification
{
    private static class TestGraphDatabase extends InternalAbstractGraphDatabase
    {
        private final RecoveryVerifier verifier;

        TestGraphDatabase( String dir, RecoveryVerifier recoveryVerifier )
        {
            super( dir, stringMap(), Service.load( IndexProvider.class ), Service.load( KernelExtension.class ),
                    Service.load( CacheProvider.class ) );
            this.verifier = recoveryVerifier;
            run();
        }
        
        @Override
        protected RecoveryVerifier createRecoveryVerifier()
        {
            return this.verifier;
        }
    }
    
    @Test
    public void recoveryVerificationShouldBeCalledForRecoveredTransactions() throws Exception
    {
        int count = 2;
        String dir = produceNonCleanDbWhichWillRecover2PCsOnStartup( "count", count );
        CountingRecoveryVerifier countingVerifier = new CountingRecoveryVerifier();
        GraphDatabaseService db = new TestGraphDatabase( dir, countingVerifier );
        assertEquals( 2, countingVerifier.count2PC );
        db.shutdown();
    }

    @Test
    public void failingRecoveryVerificationShouldThrowCorrectException() throws Exception
    {
        String dir = produceNonCleanDbWhichWillRecover2PCsOnStartup( "fail", 2 );
        RecoveryVerifier failingVerifier = new RecoveryVerifier()
        {
            @Override
            public boolean isValid( TransactionInfo txInfo )
            {
                return false;
            }
        };
        
        try
        {
            new TestGraphDatabase( dir, failingVerifier );
            fail( "Was expecting recovery exception" );
        }
        catch ( LifecycleException e )
        {
            assertEquals( RecoveryVerificationException.class, e.getCause().getClass() );
        }
    }
    
    @Test
    public void recovered2PCRecordsShouldBeWrittenInRisingTxIdOrder() throws Exception
    {
        int count = 10;
        String dir = produceNonCleanDbWhichWillRecover2PCsOnStartup( "order", count );
        // Just make it recover
        new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( dir ).setConfig( GraphDatabaseSettings.keep_logical_logs, GraphDatabaseSetting.TRUE ).newGraphDatabase().shutdown();
        verifyOrderedRecords( dir, count );
    }

    private void verifyOrderedRecords( String storeDir, int expectedCount ) throws FileNotFoundException, IOException
    {
        /* Look in the .v0 log for the 2PC records and that they are ordered by txId */
        RandomAccessFile file = new RandomAccessFile( new File( storeDir, "nioneo_logical.log.v0" ), "r" );
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
                    if ( lastOne == -1 ) lastOne = txId;
                    else assertEquals( lastOne+1, txId );
                    lastOne = txId;
                    counted++;
                }
            }
            assertEquals( expectedCount, counted );
        }
        finally
        {
            file.close();
        }
    }
    
    private static class CountingRecoveryVerifier implements RecoveryVerifier
    {
        private int count2PC;
        
        @Override
        public boolean isValid( TransactionInfo txInfo )
        {
            if ( !txInfo.isOnePhase() ) count2PC++;
            return true;
        }
    }
}
