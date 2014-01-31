/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

import javax.transaction.xa.Xid;

import org.junit.Test;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.impl.AbstractNeo4jTestCase;
import org.neo4j.kernel.impl.transaction.TxLog.Record;
import org.neo4j.kernel.impl.transaction.xaframework.ForceMode;
import org.neo4j.kernel.monitoring.Monitors;

public class TestTxLog
{
    private void assertEqualByteArray( byte a[], byte b[] )
    {
        assertTrue( a.length == b.length );
        for ( int i = 0; i < a.length; i++ )
        {
            assertEquals( a[i], b[i] );
        }
    }

    private File path()
    {
        String path = AbstractNeo4jTestCase.getStorePath( "txlog" );
        File file = new File( path );
        file.mkdirs();
        return file;
    }
    
    private File file( String name )
    {
        return new File( path(), name);
    }
    
    private File txFile()
    {
        return file( "tx_test_log.tx" );
    }

    private File tmpFile() throws IOException
    {
        File file = File.createTempFile( "tx_test_log.tx.", ".tmp", path() );
        file.deleteOnExit();
        return file;
    }
    
    @Test
    public void testTxLog() throws IOException
    {
        File file = txFile();
        if ( file.exists() )
        {
            file.delete();
        }
        try
        {
            TxLog txLog = new TxLog( txFile(), new DefaultFileSystemAbstraction(), new Monitors() );
            assertTrue( !txLog.getDanglingRecords().iterator().hasNext() );
            byte globalId[] = new byte[64];
            byte branchId[] = new byte[45];
            txLog.txStart( globalId );
            txLog.addBranch( globalId, branchId );
            assertEquals( 2, txLog.getRecordCount() );
            // Force here because we're using DirectMappedLogBuffer
            txLog.force();
            // List lists[] = txLog.getDanglingRecords();
            List<?> lists[] = getRecordLists( txLog.getDanglingRecords() );
            assertEquals( 1, lists.length );
            List<?> records = lists[0];
            assertEquals( 2, records.size() );
            TxLog.Record record = (TxLog.Record) records.get( 0 );
            assertEquals( TxLog.TX_START, record.getType() );
            assertEqualByteArray( globalId, record.getGlobalId() );
            assertTrue( null == record.getBranchId() );
            record = (TxLog.Record) records.get( 1 );
            assertEquals( TxLog.BRANCH_ADD, record.getType() );
            assertEqualByteArray( globalId, record.getGlobalId() );
            assertEqualByteArray( branchId, record.getBranchId() );
            txLog.markAsCommitting( globalId, ForceMode.unforced );
            assertEquals( 3, txLog.getRecordCount() );
            txLog.close();
            txLog = new TxLog( txFile(), new DefaultFileSystemAbstraction(), new Monitors() );
            assertEquals( 0, txLog.getRecordCount() );
            lists = getRecordLists( txLog.getDanglingRecords() );
            assertEquals( 1, lists.length );
            records = lists[0];
            assertEquals( 3, records.size() );
            record = (TxLog.Record) records.get( 0 );
            assertEquals( TxLog.TX_START, record.getType() );
            assertEqualByteArray( globalId, record.getGlobalId() );
            assertTrue( null == record.getBranchId() );
            record = (TxLog.Record) records.get( 1 );
            assertEquals( TxLog.BRANCH_ADD, record.getType() );
            assertEqualByteArray( globalId, record.getGlobalId() );
            assertEqualByteArray( branchId, record.getBranchId() );
            record = (TxLog.Record) records.get( 2 );
            assertEquals( TxLog.MARK_COMMIT, record.getType() );
            assertEqualByteArray( globalId, record.getGlobalId() );
            assertTrue( null == record.getBranchId() );
            txLog.txDone( globalId );
            // Force here because we're using DirectMappedLogBuffer
            txLog.force();
            assertEquals( 1, txLog.getRecordCount() );
            assertEquals( 0,
                getRecordLists( txLog.getDanglingRecords() ).length );
            txLog.close();
            txLog = new TxLog( txFile(), new DefaultFileSystemAbstraction(), new Monitors() );
            assertEquals( 0,
                getRecordLists( txLog.getDanglingRecords() ).length );
            txLog.close();
        }
        finally
        {
            file = txFile();
            if ( file.exists() )
            {
                file.delete();
            }
        }
    }

    private List<?>[] getRecordLists( Iterable<List<Record>> danglingRecords )
    {
        List<List<?>> list = new ArrayList<>();
        for ( List<Record> txs : danglingRecords )
        {
            list.add( txs );
        }
        return list.toArray( new List[list.size()] );
    }

    @Test
    public void testTruncateTxLog() throws IOException
    {
        File file = txFile();
        if ( file.exists() )
        {
            file.delete();
        }
        try
        {
            TxLog txLog = new TxLog( txFile(), new DefaultFileSystemAbstraction(), new Monitors() );
            byte globalId[] = new byte[64];
            byte branchId[] = new byte[45];
            txLog.txStart( globalId );
            txLog.addBranch( globalId, branchId );
            txLog.markAsCommitting( globalId, ForceMode.unforced );
            txLog.truncate();
            assertEquals( 0,
                getRecordLists( txLog.getDanglingRecords() ).length );
            txLog.close();
            txLog = new TxLog( txFile(), new DefaultFileSystemAbstraction(), new Monitors() );
            txLog.txStart( globalId );
            txLog.addBranch( globalId, branchId );
            txLog.markAsCommitting( globalId, ForceMode.unforced );
            txLog.close();
            txLog = new TxLog( txFile(), new DefaultFileSystemAbstraction(), new Monitors() );
            assertEquals( 1,
                getRecordLists( txLog.getDanglingRecords() ).length );
            txLog.truncate();
            assertEquals( 0,
                getRecordLists( txLog.getDanglingRecords() ).length );
        }
        finally
        {
            file = txFile();
            if ( file.exists() )
            {
                file.delete();
            }
        }
    }

    @Test
    public void logFilesInflatedWithZerosShouldStillBeAbleToRotate() throws IOException
    {
        File logFile = tmpFile();
        DefaultFileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();
        FileChannel ch = fileSystem.open(logFile, "rw");
        ch.write(ByteBuffer.allocate(TxLog.SCAN_WINDOW_SIZE / 2));
        ch.force(false);
        ch.close();

        TxLog log = new TxLog( logFile, fileSystem, new Monitors() );

        ByteBuffer tmp = ByteBuffer.allocate( Xid.MAXGTRIDSIZE );

        for (int i = 0; i < 2000; i++)
        {
            tmp.putInt(0, i);
            byte[] bytes = new byte[ Xid.MAXGTRIDSIZE ];
            tmp.position( 0 );
            tmp.get( bytes );
            log.txStart( bytes );
        }
        log.force();

        File rotationTarget = tmpFile();

        // Asserting that this does not throw an exception:
        log.switchToLogFile( rotationTarget );
        // ... and that the rotated log has all our started transactions:
        assertThat( log.getRecordCount(), is( 2000 ) );
    }

    @Test
    public void testTxRecovery()
    {
        // TODO
    }
}