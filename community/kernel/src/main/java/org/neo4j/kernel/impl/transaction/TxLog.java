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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.transaction.xa.Xid;

import org.neo4j.helpers.UTF8;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.xaframework.DirectMappedLogBuffer;
import org.neo4j.kernel.impl.transaction.xaframework.ForceMode;
import org.neo4j.kernel.impl.transaction.xaframework.LogBuffer;
import org.neo4j.kernel.impl.util.StringLogger;

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
    private String name = null;
    private LogBuffer logBuffer;
    private int recordCount = 0;

    public static final byte TX_START = 1;
    public static final byte BRANCH_ADD = 2;
    public static final byte MARK_COMMIT = 3;
    public static final byte TX_DONE = 4;
    private final FileSystemAbstraction fileSystem;
    private final StringLogger msgLog;

    /**
     * Initializes a transaction log using <CODE>filename</CODE>. If the file
     * isn't empty the position will be set to size of file so new records will
     * be appended.
     * 
     * @param fileName
     *            Filename of file to use
     * @param msgLog 
     * @throws IOException
     *             If unable to open file
     */
    public TxLog( String fileName, FileSystemAbstraction fileSystem, StringLogger msgLog ) throws IOException
    {
        if ( fileName == null )
        {
            throw new IllegalArgumentException( "Null filename" );
        }
        this.fileSystem = fileSystem;
        this.msgLog = msgLog;
        FileChannel fileChannel = fileSystem.open( fileName, "rw" );
        fileChannel.position( fileChannel.size() );
        logBuffer = new DirectMappedLogBuffer( fileChannel );
        this.name = fileName;
    }

    /**
     * Returns the name of the transaction log.
     */
    public String getName()
    {
        return name;
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
    public void close() throws IOException
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
        FileChannel fileChannel = logBuffer.getFileChannel();
        fileChannel.position( 0 );
        fileChannel.truncate( 0 );
        recordCount = 0;
        logBuffer = new DirectMappedLogBuffer( fileChannel );
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
        logBuffer.put( TX_START ).put( (byte) globalId.length ).put( globalId );
        recordCount++;
    }

    private void assertNotNull( Object globalId, String name )
    {
        if ( globalId == null )
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
        logBuffer.put( BRANCH_ADD ).put( (byte) globalId.length ).put(
            (byte) branchId.length ).put( globalId ).put( branchId );
        recordCount++;
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
    public synchronized void markAsCommitting( byte globalId[], ForceMode forceMode )
        throws IOException
    {
        assertNotNull( globalId, "global id" );
        logBuffer.put( MARK_COMMIT ).put( (byte) globalId.length ).put( globalId );
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
        logBuffer.put( TX_DONE ).put( (byte) globalId.length ).put( globalId );
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

        public String toString()
        {
            XidImpl xid = new XidImpl( globalId, branchId == null ? new byte[0]
                : branchId );
            return "TxLogRecord[" + typeName() + "," + xid + "," + seqNr + "," + (1+sizeOf( globalId )+sizeOf( branchId )) + "]";
        }

        private int sizeOf( byte[] id )
        {
            // If id is null it means this record type doesn't have it. TX_START/MARK_COMMIT/TX_DONE
            // only has the global id, whereas BRANCH_ADD has got both the global and branch ids.
            if ( id == null )
                return 0;
            // The length of the array (1 byte) + the actual array
            return 1+id.length;
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
        if ( record.getType() == TX_START )
        {
            txStart( record.getGlobalId() );
        }
        else if ( record.getType() == BRANCH_ADD )
        {
            addBranch( record.getGlobalId(), record.getBranchId() );
        }
        else if ( record.getType() == MARK_COMMIT )
        {
            markAsCommitting( record.getGlobalId(), forceMode );
        }
        else
        {
            // TX_DONE should never be passed in here
            throw new IOException( "Illegal record type[" + record.getType()
                + "]" );
        }
    }

    /**
     * Returns an array of lists, each list contains dangling records
     * (transactions that han't been completed yet) grouped after global by
     * transaction id.
     */
    public synchronized Iterator<List<Record>> getDanglingRecords()
        throws IOException
    {
        FileChannel fileChannel = logBuffer.getFileChannel();
        ByteBuffer buffer = ByteBuffer
                .allocateDirect( (3 + Xid.MAXGTRIDSIZE + Xid.MAXBQUALSIZE) * 1000 );
        fileChannel.position( 0 );
        buffer.clear();
        fileChannel.read( buffer );
        buffer.flip();
        // next record position
        long nextPosition = 0;
        // holds possible dangling records
        int seqNr = 0;
        Map<Xid,List<Record>> recordMap = new HashMap<Xid,List<Record>>();
        while ( buffer.hasRemaining() )
        {
            byte recordType = buffer.get();
            if ( recordType == TX_START )
            {
                if ( !buffer.hasRemaining() )
                {
                    break;
                }
                byte globalId[] = new byte[buffer.get()];
                if ( buffer.limit() - buffer.position() < globalId.length )
                {
                    break;
                }
                buffer.get( globalId );
                Xid xid = new XidImpl( globalId, new byte[0] );
                if ( recordMap.containsKey( xid ) )
                {
                    throw new IOException( "Tx start for same xid[" + xid
                        + "] found twice" );
                }
                List<Record> recordList = new LinkedList<Record>();
                recordList.add( new Record( recordType, globalId, null, 
                    seqNr++ ) );
                recordMap.put( xid, recordList );
                nextPosition += 2 + globalId.length;
            }
            else if ( recordType == BRANCH_ADD )
            {
                if ( buffer.limit() - buffer.position() < 2 )
                {
                    break;
                }
                byte globalId[] = new byte[buffer.get()];
                byte branchId[] = new byte[buffer.get()];
                if ( buffer.limit() - buffer.position() < 
                    globalId.length + branchId.length )
                {
                    break;
                }
                buffer.get( globalId );
                buffer.get( branchId );
                Xid xid = new XidImpl( globalId, new byte[0] );
                if ( !recordMap.containsKey( xid ) )
                {
                    throw new IOException( "Branch[" + UTF8.decode( branchId )
                        + "] found for [" + xid
                        + "] but no record list found in map" );
                }
                recordMap.get( xid ).add(
                    new Record( recordType, globalId, branchId, seqNr++ ) );
                nextPosition += 3 + globalId.length + branchId.length;
            }
            else if ( recordType == MARK_COMMIT )
            {
                if ( !buffer.hasRemaining() )
                {
                    break;
                }
                byte globalId[] = new byte[buffer.get()];
                if ( buffer.limit() - buffer.position() < globalId.length )
                {
                    break;
                }
                buffer.get( globalId );
                Xid xid = new XidImpl( globalId, new byte[0] );
                if ( !recordMap.containsKey( xid ) )
                {
                    throw new IOException( "Commiting xid[" + xid
                        + "] mark found but no record list found in map" );
                }
                List<Record> recordList = recordMap.get( xid );
                recordList.add( new Record( recordType, globalId, null, 
                    seqNr++ ) );
                recordMap.put( xid, recordList );
                nextPosition += 2 + globalId.length;
            }
            else if ( recordType == TX_DONE )
            {
                if ( !buffer.hasRemaining() )
                {
                    break;
                }
                byte globalId[] = new byte[buffer.get()];
                if ( buffer.limit() - buffer.position() < globalId.length )
                {
                    break;
                }
                buffer.get( globalId );
                Xid xid = new XidImpl( globalId, new byte[0] );
                if ( !recordMap.containsKey( xid ) )
                {
                    throw new IOException( "Commiting xid[" + xid
                        + "] mark found but no record list found in map" );
                }
                recordMap.remove( xid );
                nextPosition += 2 + globalId.length;
            }
            else if ( recordType == 0 )
            {
                continue;
            }
            else
            {
                throw new IOException( "Unknown type: " + recordType );
            }
            if ( (buffer.limit() - buffer.position()) < 
                (3 + Xid.MAXGTRIDSIZE + Xid.MAXBQUALSIZE) )
            {
                // make sure we don't try to read non full entry
                buffer.clear();
                fileChannel.position( nextPosition );
                fileChannel.read( buffer );
                buffer.flip();
            }
        }
        return recordMap.values().iterator();
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
    public synchronized void switchToLogFile( String newFile )
        throws IOException
    {
        if ( newFile == null )
        {
            throw new IllegalArgumentException( "Null filename" );
        }
        // copy all dangling records from current log to new log
        force();
        Iterator<List<Record>> itr = getDanglingRecords();
        close();
        List<Record> records = new ArrayList<Record>();
        while ( itr.hasNext() )
        {
            records.addAll( itr.next() );
        }
        Collections.sort( records, new Comparator<Record>()
        {
            public int compare( Record r1, Record r2 )
            {
                return r1.getSequenceNumber() - r2.getSequenceNumber();
            }
        } );
        msgLog.logMessage( "About to rotate " + name + " to " + newFile + " with dangling records " + records, true );
        Iterator<Record> recordItr = records.iterator();
        FileChannel fileChannel = fileSystem.open( newFile, "rw" );
        fileChannel.position( fileChannel.size() );
        logBuffer = new DirectMappedLogBuffer( fileChannel );
        name = newFile;
        truncate();
        while ( recordItr.hasNext() )
        {
            Record record = recordItr.next();
            writeRecord( record, ForceMode.forced );
        }
        force();
        msgLog.logMessage( "Rotated " + name + " to, file channel now at " + fileChannel.position(), true );
    }
}