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
package org.neo4j.kernel.impl.transaction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.junit.Test;
import org.neo4j.kernel.CommonFactories;
import org.neo4j.kernel.impl.AbstractNeo4jTestCase;
import org.neo4j.kernel.impl.transaction.TxLog.Record;
import org.neo4j.kernel.impl.transaction.xaframework.ForceMode;
import org.neo4j.kernel.impl.util.StringLogger;

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

    private String path()
    {
        String path = AbstractNeo4jTestCase.getStorePath( "txlog" );
        new File( path ).mkdirs();
        return path;
    }
    
    private String file( String name )
    {
        return path() + File.separator + name;
    }
    
    private String txFile()
    {
        return file( "tx_test_log.tx" );
    }
    
    @Test
    public void testTxLog() throws IOException
    {
        File file = new File( txFile() );
        if ( file.exists() )
        {
            file.delete();
        }
        try
        {
            TxLog txLog = new TxLog( txFile(), CommonFactories.defaultFileSystemAbstraction(), StringLogger.DEV_NULL );
            assertTrue( !txLog.getDanglingRecords().hasNext() );
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
            txLog = new TxLog( txFile(), CommonFactories.defaultFileSystemAbstraction(), StringLogger.DEV_NULL );
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
            txLog = new TxLog( txFile(), CommonFactories.defaultFileSystemAbstraction(), StringLogger.DEV_NULL );
            assertEquals( 0,
                getRecordLists( txLog.getDanglingRecords() ).length );
            txLog.close();
        }
        finally
        {
            file = new File( txFile() );
            if ( file.exists() )
            {
                file.delete();
            }
        }
    }

    private List<?>[] getRecordLists( Iterator<List<Record>> danglingRecords )
    {
        List<List<?>> list = new ArrayList<List<?>>();
        while ( danglingRecords.hasNext() )
        {
            list.add( danglingRecords.next() );
        }
        return list.toArray( new List[list.size()] );
    }

    @Test
    public void testTruncateTxLog() throws IOException
    {
        File file = new File( txFile() );
        if ( file.exists() )
        {
            file.delete();
        }
        try
        {
            TxLog txLog = new TxLog( txFile(), CommonFactories.defaultFileSystemAbstraction(), StringLogger.DEV_NULL );
            byte globalId[] = new byte[64];
            byte branchId[] = new byte[45];
            txLog.txStart( globalId );
            txLog.addBranch( globalId, branchId );
            txLog.markAsCommitting( globalId, ForceMode.unforced );
            txLog.truncate();
            assertEquals( 0,
                getRecordLists( txLog.getDanglingRecords() ).length );
            txLog.close();
            txLog = new TxLog( txFile(), CommonFactories.defaultFileSystemAbstraction(), StringLogger.DEV_NULL );
            txLog.txStart( globalId );
            txLog.addBranch( globalId, branchId );
            txLog.markAsCommitting( globalId, ForceMode.unforced );
            txLog.close();
            txLog = new TxLog( txFile(), CommonFactories.defaultFileSystemAbstraction(), StringLogger.DEV_NULL );
            assertEquals( 1,
                getRecordLists( txLog.getDanglingRecords() ).length );
            txLog.truncate();
            assertEquals( 0,
                getRecordLists( txLog.getDanglingRecords() ).length );
        }
        finally
        {
            file = new File( txFile() );
            if ( file.exists() )
            {
                file.delete();
            }
        }
    }

    @Test
    public void testTxRecovery()
    {
        // TODO
    }
}