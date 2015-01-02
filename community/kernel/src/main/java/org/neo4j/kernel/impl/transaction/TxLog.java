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
package org.neo4j.kernel.impl.transaction;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.transaction.xa.Xid;

import org.neo4j.helpers.UTF8;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.StoreChannel;
import org.neo4j.kernel.impl.transaction.xaframework.DirectMappedLogBuffer;
import org.neo4j.kernel.impl.transaction.xaframework.ForceMode;
import org.neo4j.kernel.impl.transaction.xaframework.LogBuffer;
import org.neo4j.kernel.monitoring.ByteCounterMonitor;
import org.neo4j.kernel.monitoring.Monitors;

// TODO: fixed sized logs (pre-initialize them)
// keep dangling records in memory for log switch
// batch disk forces
/**
 * This class is made public for testing purposes only, do not use.
 * <p>
 * The {@link TxManager} uses this class to keep a transaction log for
 * transaction recovery.
 * <p>
 */
public class TxLog
{
    public static final int MAX_RECORD_SIZE = 3 + Xid.MAXGTRIDSIZE + Xid.MAXBQUALSIZE;
    public static final int LOG_ROTATION_THRESHOLD = 1000; // As a count of records
    public static final int SCAN_WINDOW_SIZE = MAX_RECORD_SIZE * LOG_ROTATION_THRESHOLD;

    private final ByteCounterMonitor bufferMonitor;

    public static final byte NULL_BYTE = 0;
    public static final byte TX_START = 1;
    public static final byte BRANCH_ADD = 2;
    public static final byte MARK_COMMIT = 3;
    public static final byte TX_DONE = 4;

    private final Collection<ByteArrayKey> activeTransactions = new HashSet<>();
    private final FileSystemAbstraction fileSystem;

    private File name = null;
    private LogBuffer logBuffer;
    private int recordCount = 0;

    private static final class ByteArrayKey
    {
        private final byte[] bytes;

        private ByteArrayKey( byte[] bytes )
        {
            this.bytes = bytes;
        }

        @Override
        public int hashCode()
        {
            return Arrays.hashCode( bytes );
        }

        @Override
        public boolean equals( Object obj )
        {
            return obj instanceof ByteArrayKey && Arrays.equals( bytes, ((ByteArrayKey)obj).bytes );
        }
    }

    /**
     * Initializes a transaction log using <CODE>filename</CODE>. If the file
     * isn't empty the position will be set to size of file so new records will
     * be appended.
     *
     * @param fileName
     *            Filename of file to use
     * @param fileSystem
     *            The concrete FileSystemAbstraction to use.
     * @param monitors {@link Monitors}.
     * @throws IOException
     *             If unable to open file
     */
    public TxLog( File fileName, FileSystemAbstraction fileSystem, Monitors monitors ) throws IOException
    {
        this.bufferMonitor = monitors.newMonitor( ByteCounterMonitor.class, getClass() );
        if ( fileName == null )
        {
            throw new IllegalArgumentException( "Null filename" );
        }
        this.fileSystem = fileSystem;
        StoreChannel fileChannel = fileSystem.open( fileName, "rw" );
        fileChannel.position( fileChannel.size() );
        logBuffer = new DirectMappedLogBuffer( fileChannel, bufferMonitor );
        this.name = fileName;

        recreateActiveTransactionState();
    }

    private void recreateActiveTransactionState() throws IOException
    {
        for ( List<Record> tx : getDanglingRecords() )
        {
            for ( Record record : tx )
            {
                if ( record.getType() == TX_START )
                {
                    activeTransactions.add( new ByteArrayKey( record.getGlobalId() ) );
                }
            }
        }
    }

    /**
     * Returns the name of the transaction log.
     */
    public String getName()
    {
        return name.getPath();
    }

    /**
     * Returns the number of records (one of TX_START,BRANCH_ADD,MARK_COMMIT or
     * TX_DONE) written since this instance was created or truncated.
     */
    public int getRecordCount()
    {
        return recordCount;
    }

    /**
     * Closes the file representing the transaction log.
     */
    public synchronized void close() throws IOException
    {
        logBuffer.force();
        logBuffer.getFileChannel().close();
    }

    /**
     * Forces the log file (with metadata). Useful when switching log.
     */
    public void force() throws IOException
    {
        logBuffer.force();
    }

    /**
     * Truncates the file to zero size and sets the record count to zero.
     */
    public synchronized void truncate() throws IOException
    {
        StoreChannel fileChannel = logBuffer.getFileChannel();
        fileChannel.position( 0 );
        fileChannel.truncate( 0 );
        recordCount = 0;
        logBuffer = new DirectMappedLogBuffer( fileChannel, bufferMonitor  );
        activeTransactions.clear();
    }

    /**
     * Writes a <CODE>TX_START</CODE> record to the file.
     *
     * @param globalId
     *            The global id of the new transaction
     * @throws IOException
     *             If unable to write
     */
    // tx_start(byte)|gid_length(byte)|globalId
    public synchronized void txStart( byte globalId[] ) throws IOException
    {
        assertNotNull( globalId, "global id" );
        if ( !activeTransactions.add( new ByteArrayKey( globalId ) ) )
        {
            throw new IllegalStateException( "Global ID " + Arrays.toString( globalId ) + " already started" );
        }
        byte globalIdSize = (byte) globalId.length;
        logBuffer.put( TX_START ).put( globalIdSize ).put( globalId );
        recordCount++;
    }

    private void assertNotNull( Object obj, String name )
    {
        if ( obj == null )
        {
            throw new IllegalArgumentException( "Null " + name );
        }
    }

    /**
     * Writes a <CODE>BRANCH_ADD</CODE> record to the file.
     *
     * @param globalId
     *            The global id of the transaction
     * @param branchId
     *            The branch id for the enlisted resource
     * @throws IOException
     *             If unable to write
     */
    // add_branch(byte)|gid_length(byte)|bid_length(byte)|globalId|branchId
    public synchronized void addBranch( byte globalId[], byte branchId[] )
        throws IOException
    {
        assertNotNull( globalId, "global id" );
        assertNotNull( branchId, "branch id" );
        assertActive( globalId );
        byte globalIdSize = (byte) globalId.length;
        byte branchIdSize = (byte) branchId.length;
        logBuffer.put( BRANCH_ADD ).put( globalIdSize ).put( branchIdSize ).put( globalId ).put( branchId );
        recordCount++;
    }

    private void assertActive( byte[] globalId )
    {
        if ( !activeTransactions.contains( new ByteArrayKey( globalId ) ) )
        {
            throw new IllegalStateException( "Global ID " + Arrays.toString( globalId ) + " not active" );
        }
    }

    /**
     * Writes a <CODE>MARK_COMMIT</CODE> record to the file and forces the
     * file to disk.
     *
     * @param globalId
     *            The global id of the transaction
     * @throws IOException
     *             If unable to write
     */
    // mark_committing(byte)|gid_length(byte)|globalId
    // forces
    public synchronized void markAsCommitting( byte globalId[], ForceMode forceMode ) throws IOException
    {
        assertNotNull( globalId, "global id" );
        assertActive( globalId );

        byte globalIdSize = (byte) globalId.length;
        logBuffer.put( MARK_COMMIT ).put( globalIdSize ).put( globalId );
        forceMode.force( logBuffer );
        recordCount++;
    }

    /**
     * Writes a <CODE>TX_DONE</CODE> record to the file.
     *
     * @param globalId
     *            The global id of the transaction completed
     * @throws IOException
     *             If unable to write
     */
    // tx_done(byte)|gid_length(byte)|globalId
    public synchronized void txDone( byte globalId[] ) throws IOException
    {
        assertNotNull( globalId, "global id" );
        if ( !activeTransactions.remove( new ByteArrayKey( globalId ) ) )
        {
            throw new IllegalStateException( "Global ID " + Arrays.toString( globalId ) + " not active" );
        }

        byte globalIdSize = (byte) globalId.length;
        logBuffer.put( TX_DONE ).put( globalIdSize ).put( globalId );
        recordCount++;
    }

    /**
     * Made public for testing only.
     * <p>
     * Wraps a transaction record in the tx log file.
     */
    public static class Record
    {
        private byte type = 0;
        private byte globalId[] = null;
        private byte branchId[] = null;
        private int seqNr = -1;

        Record( byte type, byte globalId[], byte branchId[], int seqNr )
        {
            if ( type < 1 || type > 4 )
            {
                throw new IllegalArgumentException( "Illegal type: " + type );
            }
            this.type = type;
            this.globalId = globalId;
            this.branchId = branchId;
            this.seqNr = seqNr;
        }

        public byte getType()
        {
            return type;
        }

        public byte[] getGlobalId()
        {
            return globalId;
        }

        public byte[] getBranchId()
        {
            return branchId;
        }

        public int getSequenceNumber()
        {
            return seqNr;
        }

        @Override
        public String toString()
        {
            XidImpl xid = new XidImpl( globalId, branchId == null ? new byte[0] : branchId );
            int size = 1 + sizeOf( globalId ) + sizeOf( branchId );
            return "TxLogRecord[" + typeName() + "," + xid + "," + seqNr + "," + size + "]";
        }

        private int sizeOf( byte[] id )
        {
            // If id is null it means this record type doesn't have it. TX_START/MARK_COMMIT/TX_DONE
            // only has the global id, whereas BRANCH_ADD has got both the global and branch ids.
            if ( id == null )
            {
                return 0;
            }
            // The length of the array (1 byte) + the actual array
            return 1 + id.length;
        }

        String typeName()
        {
            switch ( type )
            {
            case TX_START:
                return "TX_START";
            case BRANCH_ADD:
                return "BRANCH_ADD";
            case MARK_COMMIT:
                return "MARK_COMMIT";
            case TX_DONE:
                return "TX_DONE";
            default:
                return "<unknown type>";
            }
        }
    }

    void writeRecord( Record record, ForceMode forceMode ) throws IOException
    {
        switch ( record.getType() )
        {
            case TX_START:
                txStart( record.getGlobalId() );
                break;
            case BRANCH_ADD:
                addBranch( record.getGlobalId(), record.getBranchId() );
                break;
            case MARK_COMMIT:
                markAsCommitting( record.getGlobalId(), forceMode );
                break;
            default:
                // TX_DONE should never be passed in here
                throw new IOException( "Illegal record type[" + record.getType() + "]" );
        }
    }

    /**
     * Returns an array of lists, each list contains dangling records
     * (transactions that hasn't been completed yet) grouped after global by
     * transaction id.
     */
    public synchronized Iterable<List<Record>> getDanglingRecords()
        throws IOException
    {
        StoreChannel fileChannel = logBuffer.getFileChannel();
        ByteBuffer buffer = ByteBuffer.allocateDirect(SCAN_WINDOW_SIZE);
        readFileIntoBuffer( fileChannel, buffer, 0 );

        // next record position
        long nextPosition = 0;
        // holds possible dangling records
        int seqNr = 0;
        Map<Xid,List<Record>> recordMap = new HashMap<>();

        while ( buffer.hasRemaining() )
        {
            byte recordType = buffer.get();
            int recordSize;

            switch ( recordType )
            {
                case TX_START:
                    recordSize = readTxStartRecordInto( recordMap, buffer, seqNr++ );
                    break;
                case BRANCH_ADD:
                    recordSize = readBranchAddRecordInto( recordMap, buffer, seqNr++ );
                    break;
                case MARK_COMMIT:
                    recordSize = readMarkCommitRecordInto( recordMap, buffer, seqNr++ );
                    break;
                case TX_DONE:
                    recordSize = readTxDoneAndRemoveTransactionFrom( recordMap, buffer );
                    break;
                case NULL_BYTE:
                    // We accept and ignore arbitrary null-bytes in between records.
                    // I'm not sure where they come from, though. A challenge for another day.
                    // For now we just make sure to increment nextPosition, so we skip over
                    // them in case we want to move our buffer window.
                    recordSize = 1;
                    break;
                default:
                    throw new IOException( "Unknown type: " + recordType );
            }
            if ( recordSize == 0 )
            {
                // Getting a record size of 0 means that read* methods found an incomplete or empty byte stream.
                break;
            }
            nextPosition += recordSize;

            // Reposition the scan window if we're getting to the end of it and there is more bytes in the
            // channel to be read.
            if ( buffer.remaining() < MAX_RECORD_SIZE && (fileChannel.size() - nextPosition) > buffer.remaining() )
            {
                readFileIntoBuffer( fileChannel, buffer, nextPosition );
            }

        }
        return recordMap.values();
    }

    private void readFileIntoBuffer( StoreChannel fileChannel, ByteBuffer buffer, long nextPosition ) throws IOException
    {
        buffer.clear();
        fileChannel.position( nextPosition );
        fileChannel.read( buffer );
        buffer.flip();
    }

    /**
     * Read a TX_START record from the buffer, attach the given sequence number and store it in the recordMap.
     * Returns the size of the record in bytes, or 0 if the byte stream is incomplete or empty.
     */
    private static int readTxStartRecordInto(Map<Xid, List<Record>> recordMap, ByteBuffer buffer, int seqNr)
            throws IOException
    {
        if ( !buffer.hasRemaining() )
        {
            return 0;
        }
        byte globalId[] = new byte[buffer.get()];
        if ( buffer.remaining() < globalId.length )
        {
            return 0;
        }
        buffer.get(globalId);
        Xid xid = new XidImpl( globalId, new byte[0] );
        if ( recordMap.containsKey( xid ) )
        {
            throw new IOException( "Tx start for same xid[" + xid + "] found twice" );
        }
        List<Record> recordList = new LinkedList<>();
        recordList.add( new Record( TX_START, globalId, null, seqNr ) );
        recordMap.put( xid, recordList );
        return 2 + globalId.length;
    }

    /**
     * Same as {@link #readTxStartRecordInto}, but for BRANCH_ADD records.
     */
    private static int readBranchAddRecordInto( Map<Xid, List<Record>> recordMap, ByteBuffer buffer, int seqNr)
            throws IOException
    {
        if ( buffer.remaining() < 2 )
        {
            return 0;
        }
        byte globalId[] = new byte[buffer.get()];
        byte branchId[] = new byte[buffer.get()];
        if ( buffer.remaining() < globalId.length + branchId.length )
        {
            return 0;
        }
        buffer.get( globalId );
        buffer.get( branchId );
        Xid xid = new XidImpl( globalId, new byte[0] );
        if ( !recordMap.containsKey( xid ) )
        {
            throw new IOException( String.format(
                    "Branch[%s] found for [%s] but no record list found in map",
                    UTF8.decode( branchId ), xid ) );
        }
        recordMap.get( xid ).add( new Record( BRANCH_ADD, globalId, branchId, seqNr ) );
        return 3 + globalId.length + branchId.length;
    }

    /**
     * Same as {@link #readTxStartRecordInto}, but for MARK_COMMIT records.
     */
    private static int readMarkCommitRecordInto( Map<Xid, List<Record>> recordMap, ByteBuffer buffer, int seqNr)
            throws IOException
    {
        if ( !buffer.hasRemaining() )
        {
            return 0;
        }
        byte globalId[] = new byte[buffer.get()];
        if ( buffer.remaining() < globalId.length )
        {
            return 0;
        }
        buffer.get( globalId );
        Xid xid = new XidImpl( globalId, new byte[0] );
        if ( !recordMap.containsKey( xid ) )
        {
            throw new IOException(
                    "Committing xid[" + xid + "] mark found but no record list found in map" );
        }
        List<Record> recordList = recordMap.get( xid );
        recordList.add( new Record( MARK_COMMIT, globalId, null, seqNr ) );
        recordMap.put(xid, recordList);
        return 2 + globalId.length;
    }

    /**
     * Read a TX_DONE record from the given buffer, and removes the associated transaction from the given recordMap.
     * Returns the size of the TX_DONE record in bytes, or 0 if the byte stream is incomplete of empty.
     */
    private static int readTxDoneAndRemoveTransactionFrom( Map<Xid, List<Record>> recordMap, ByteBuffer buffer )
            throws IOException
    {
        if ( !buffer.hasRemaining() )
        {
            return 0;
        }
        byte globalId[] = new byte[buffer.get()];
        if ( buffer.remaining() < globalId.length )
        {
            return 0;
        }
        buffer.get( globalId );
        Xid xid = new XidImpl( globalId, new byte[0] );
        if ( !recordMap.containsKey( xid ) )
        {
            throw new IOException(
                    "Committing xid[" + xid + "] mark found but no record list found in map" );
        }
        recordMap.remove( xid );
        return 2 + globalId.length;
    }

    /**
     * Switches log file. Copies the dangling records in current log file to the
     * <CODE>newFile</CODE> and then makes the switch closing the old log file.
     *
     * @param newFile
     *            The filename of the new file to switch to
     * @throws IOException
     *             If unable to switch log file
     */
    public synchronized void switchToLogFile( File newFile )
        throws IOException
    {
        if ( newFile == null )
        {
            throw new IllegalArgumentException( "Null filename" );
        }
        // copy all dangling records from current log to new log
        force();
        Iterable<List<Record>> itr = getDanglingRecords();
        close();
        List<Record> records = new ArrayList<>();
        for ( List<Record> tx : itr )
        {
            records.addAll( tx );
        }
        Collections.sort( records, new Comparator<Record>()
        {
            @Override
            public int compare( Record r1, Record r2 )
            {
                return r1.getSequenceNumber() - r2.getSequenceNumber();
            }
        } );
        Iterator<Record> recordItr = records.iterator();
        StoreChannel fileChannel = fileSystem.open( newFile, "rw" );
        fileChannel.position( fileChannel.size() );
        logBuffer = new DirectMappedLogBuffer( fileChannel, bufferMonitor  );
        name = newFile;
        truncate();
        while ( recordItr.hasNext() )
        {
            Record record = recordItr.next();
            writeRecord( record, ForceMode.forced );
        }
        force();
    }
}
