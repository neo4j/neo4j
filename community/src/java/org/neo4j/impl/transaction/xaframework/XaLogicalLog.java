/*
 * Copyright (c) 2002-2009 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 * 
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.impl.transaction.xaframework;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.transaction.xa.XAException;
import javax.transaction.xa.Xid;

import org.neo4j.impl.transaction.XidImpl;
import org.neo4j.impl.util.ArrayMap;
import org.neo4j.impl.util.FileUtils;

/**
 * <CODE>XaLogicalLog</CODE> is a transaction and logical log combined. In
 * this log information about the transaction (such as started, prepared and
 * committed) will be written. All commands participating in the transaction
 * will also be written to the log.
 * <p>
 * Normally you don't have to do anything with this log except open it after it
 * has been instanciated (see {@link XaContainer}). The only method that may be
 * of use when implementing a XA compatible resource is the
 * {@link #getCurrentTxIdentifier}. Leave everything else be unless you know
 * what you're doing.
 * <p>
 * When the log is opened it will be scaned for uncompleted transactions and
 * those transactions will be re-created. When scan of log is complete all
 * transactions that hasn't entered prepared state will be marked as done
 * (implies rolledback) and dropped. All transactions that have been prepared
 * will be held in memory until the transaction manager tells them to commit.
 * Transaction that already started commit but didn't get flagged as done will
 * be re-committed.
 */
public class XaLogicalLog
{
    private Logger log;
    // empty record due to memory mapped file
    private static final byte EMPTY = (byte) 0;
    // tx has started
    private static final byte TX_START = (byte) 1;
    // tx has been prepared
    private static final byte TX_PREPARE = (byte) 2;
    // a XaCommand in a transaction
    private static final byte COMMAND = (byte) 3;
    // done, either a read only tx or rolledback/forget
    private static final byte DONE = (byte) 4;
    // tx one-phase commit
    private static final byte TX_1P_COMMIT = (byte) 5;
    // tx two-phase commit
    private static final byte TX_2P_COMMIT = (byte) 6;
    
    private static final char CLEAN = 'C';
    private static final char LOG1 = '1';
    private static final char LOG2 = '2';

    private FileChannel fileChannel = null;
    private final ByteBuffer buffer;
    private LogBuffer writeBuffer = null;
    private long logVersion = 0;
    private ArrayMap<Integer,StartEntry> xidIdentMap = 
        new ArrayMap<Integer,StartEntry>( 4, false, true );
    private Map<Integer,XaTransaction> recoveredTxMap = 
        new HashMap<Integer,XaTransaction>();
    private int nextIdentifier = 1;
    private boolean scanIsComplete = false;

    private String fileName = null;
    private final XaResourceManager xaRm;
    private final XaCommandFactory cf;
    private final XaTransactionFactory xaTf;
    private char currentLog = CLEAN;
    private boolean keepLogs = false;
    private boolean autoRotate = true;
    private long rotateAtSize = 10*1024*1024; // 10MB
    private boolean backupSlave = false;
    private boolean useMemoryMapped = true;

    XaLogicalLog( String fileName, XaResourceManager xaRm, XaCommandFactory cf,
        XaTransactionFactory xaTf, Map<Object,Object> config )
    {
        this.fileName = fileName;
        this.xaRm = xaRm;
        this.cf = cf;
        this.xaTf = xaTf;
        this.useMemoryMapped = getMemoryMapped( config );
        log = Logger.getLogger( this.getClass().getName() + "/" + fileName );
        buffer = ByteBuffer.allocateDirect( 9 + Xid.MAXGTRIDSIZE
            + Xid.MAXBQUALSIZE * 10 );
    }
    
    private boolean getMemoryMapped( Map<Object,Object> config )
    {
        if ( config != null )
        {
            String value = (String) config.get( "use_memory_mapped_buffers" );
            if ( value != null && value.toLowerCase().equals( "false" ) )
            {
                return false;
            }
        }
        return true;
    }
    
    synchronized void open() throws IOException
    {
        String activeFileName = fileName + ".active";
        if ( !new File( activeFileName ).exists() )
        {
            if ( new File( fileName ).exists() )
            {
                // old < b8 xaframework with no log rotation and we need to 
                // do recovery on it
                open( fileName );
            }
            else
            {
                setActiveLog( LOG1 );
                open( fileName + ".1" );
            }
        }
        else
        {
            FileChannel fc = new RandomAccessFile( activeFileName , 
                "rw" ).getChannel();
            byte bytes[] = new byte[256];
            ByteBuffer buf = ByteBuffer.wrap( bytes );
            int read = fc.read( buf );
            fc.close();
            if ( read != 4 )
            {
                throw new IllegalStateException( "Read " + read + 
                    " bytes from " + activeFileName + " but expected 4" ); 
            }
            buf.flip();
            char c = buf.asCharBuffer().get();
            File copy = new File( fileName + ".copy" );
            if ( copy.exists() )
            {
                if ( !copy.delete() )
                {
                    log.warning( "Unable to delete " + copy.getName() );
                }
            }
            if ( c == CLEAN )
            {
                // clean
                String newLog = fileName + ".1";
                if ( new File( newLog ).exists() )
                {
                    throw new IllegalStateException( 
                        "Active marked as clean but log " + newLog + " exist" );
                }
                setActiveLog( LOG1 );
                open( newLog );
            }
            else if ( c == LOG1 )
            {
                String newLog = fileName + ".1";
                if ( !new File( newLog ).exists() )
                {
                    throw new IllegalStateException( 
                        "Active marked as 1 but no " + newLog + " exist" );
                }
                currentLog = LOG1;
                File otherLog = new File( fileName + ".2" );
                if ( otherLog.exists() )
                {
                    if ( !otherLog.delete() )
                    {
                        log.warning( "Unable to delete " + copy.getName() );
                    }
                }
                open( newLog );
            }
            else if ( c == LOG2 )
            {
                String newLog = fileName + ".2";
                if ( !new File( newLog ).exists() )
                {
                    throw new IllegalStateException( 
                        "Active marked as 2 but no " + newLog + " exist" );
                }
                File otherLog = new File( fileName + ".1" );
                if ( otherLog.exists() )
                {
                    if ( !otherLog.delete() )
                    {
                        log.warning( "Unable to delete " + copy.getName() );
                    }
                }
                currentLog = LOG2;
                open( newLog );
            }
            else
            {
                throw new IllegalStateException( "Unknown active log: " + c );
            }
        }
        if ( !useMemoryMapped )
        {
            writeBuffer = new DirectMappedLogBuffer( fileChannel );
        }
        else
        {
            writeBuffer = new MemoryMappedLogBuffer( fileChannel );
        }
    }
    
    private void open( String fileToOpen ) throws IOException
    {
        fileChannel = new RandomAccessFile( fileToOpen, "rw" ).getChannel();
        if ( fileChannel.size() != 0 )
        {
            doInternalRecovery( fileToOpen );
        }
        else
        {
            logVersion = xaTf.getCurrentVersion();
            buffer.clear();
            buffer.putLong( logVersion );
            buffer.flip();
            fileChannel.write( buffer );
            scanIsComplete = true;
        }
    }

    public boolean scanIsComplete()
    {
        return scanIsComplete;
    }

    private int getNextIdentifier()
    {
        nextIdentifier++;
        if ( nextIdentifier < 0 )
        {
            nextIdentifier = 1;
        }
        return nextIdentifier;
    }

    // returns identifier for transaction
    // [TX_START][xid[gid.length,bid.lengh,gid,bid]][identifier][format id]
    public synchronized int start( Xid xid ) throws XAException
    {
        if ( backupSlave )
        {
            throw new XAException( "Resource is configured as backup slave, " + 
                "no new transactions can be started for " + fileName + "." + 
                currentLog );
        }
        int xidIdent = getNextIdentifier();
        try
        {
            byte globalId[] = xid.getGlobalTransactionId();
            byte branchId[] = xid.getBranchQualifier();
            int formatId = xid.getFormatId();
            long position = writeBuffer.getFileChannelPosition();
            writeBuffer.put( TX_START ).put( (byte) globalId.length ).put(
                (byte) branchId.length ).put( globalId ).put( branchId )
                .putInt( xidIdent ).putInt( formatId );
            xidIdentMap.put( xidIdent, new StartEntry( xid, position ) );
        }
        catch ( IOException e )
        {
            throw new XAException( "Logical log couldn't start transaction: "
                + e );
        }
        return xidIdent;
    }

    private boolean readTxStartEntry() throws IOException
    {
        // get the global id
        long position = fileChannel.position();
        buffer.clear();
        buffer.limit( 1 );
        if ( fileChannel.read( buffer ) != buffer.limit() )
        {
            return false;
        }
        buffer.flip();
        byte globalIdLength = buffer.get();
        // get the branchId id
        buffer.clear();
        buffer.limit( 1 );
        if ( fileChannel.read( buffer ) != buffer.limit() )
        {
            return false;
        }
        buffer.flip();
        byte branchIdLength = buffer.get();
        byte globalId[] = new byte[globalIdLength];
        ByteBuffer tmpBuffer = ByteBuffer.wrap( globalId );
        if ( fileChannel.read( tmpBuffer ) != globalId.length )
        {
            return false;
        }
        byte branchId[] = new byte[branchIdLength];
        tmpBuffer = ByteBuffer.wrap( branchId );
        if ( fileChannel.read( tmpBuffer ) != branchId.length )
        {
            return false;
        }
        // get the neo tx identifier
        buffer.clear();
        buffer.limit( 4 );
        if ( fileChannel.read( buffer ) != buffer.limit() )
        {
            return false;
        }
        buffer.flip();
        int identifier = buffer.getInt();
        if ( identifier >= nextIdentifier )
        {
            nextIdentifier = (identifier + 1);
        }
        // get the format id
        buffer.clear();
        buffer.limit( 4 );
        if ( fileChannel.read( buffer ) != buffer.limit() )
        {
            return false;
        }
        buffer.flip();
        int formatId = buffer.getInt();
        // re-create the transaction
        Xid xid = new XidImpl( globalId, branchId, formatId );
        xidIdentMap.put( identifier, new StartEntry( xid, position ) );
        XaTransaction xaTx = xaTf.create( identifier );
        xaTx.setRecovered();
        recoveredTxMap.put( identifier, xaTx );
        xaRm.injectStart( xid, xaTx );
        return true;
    }

    // [TX_PREPARE][identifier]
    public synchronized void prepare( int identifier ) throws XAException
    {
        assert xidIdentMap.get( identifier ) != null;
        try
        {
            writeBuffer.put( TX_PREPARE ).putInt( identifier );
            writeBuffer.force();
        }
        catch ( IOException e )
        {
            throw new XAException( "Logical log unable to mark prepare ["
                + identifier + "] " + e );
        }
    }

    private boolean readTxPrepareEntry() throws IOException
    {
        // get the neo tx identifier
        buffer.clear();
        buffer.limit( 4 );
        if ( fileChannel.read( buffer ) != buffer.limit() )
        {
            return false;
        }
        buffer.flip();
        int identifier = buffer.getInt();
        StartEntry entry = xidIdentMap.get( identifier );
        if ( entry == null )
        {
            return false;
        }
        Xid xid = entry.getXid();
        if ( xaRm.injectPrepare( xid ) )
        {
            // read only we can remove
            xidIdentMap.remove( identifier );
            recoveredTxMap.remove( identifier );
        }
        return true;
    }

    // [TX_1P_COMMIT][identifier]
    public synchronized void commitOnePhase( int identifier )
        throws XAException
    {
        assert xidIdentMap.get( identifier ) != null;
        try
        {
            writeBuffer.put( TX_1P_COMMIT ).putInt( identifier );
            writeBuffer.force();
        }
        catch ( IOException e )
        {
            throw new XAException( "Logical log unable to mark 1P-commit ["
                + identifier + "] " + e );
        }
    }

    private boolean readTxOnePhaseCommit() throws IOException
    {
        // get the neo tx identifier
        buffer.clear();
        buffer.limit( 4 );
        if ( fileChannel.read( buffer ) != buffer.limit() )
        {
            return false;
        }
        buffer.flip();
        int identifier = buffer.getInt();
        StartEntry entry = xidIdentMap.get( identifier );
        if ( entry == null )
        {
            return false;
        }
        Xid xid = entry.getXid();
        try
        {
            xaRm.injectOnePhaseCommit( xid );
        }
        catch ( XAException e )
        {
            e.printStackTrace();
            throw new IOException( e.getMessage() );
        }
        return true;
    }

    // [DONE][identifier]
    public synchronized void done( int identifier ) throws XAException
    {
        if ( backupSlave )
        {
            return;
        }
        assert xidIdentMap.get( identifier ) != null;
        try
        {
            writeBuffer.put( DONE ).putInt( identifier );
            xidIdentMap.remove( identifier );
        }
        catch ( IOException e )
        {
            throw new XAException( "Logical log unable to mark as done ["
                + identifier + "] " + e );
        }
    }

    // [DONE][identifier] called from XaResourceManager during internal recovery
    synchronized void doneInternal( int identifier ) throws IOException
    {
        buffer.clear();
        buffer.put( DONE ).putInt( identifier );
        buffer.flip();
        fileChannel.write( buffer );
        xidIdentMap.remove( identifier );
    }

    private boolean readDoneEntry() throws IOException
    {
        // get the neo tx identifier
        buffer.clear();
        buffer.limit( 4 );
        if ( fileChannel.read( buffer ) != buffer.limit() )
        {
            return false;
        }
        buffer.flip();
        int identifier = buffer.getInt();
        StartEntry entry = xidIdentMap.get( identifier );
        if ( entry == null )
        {
            return false;
        }
        Xid xid = entry.getXid();
        xaRm.pruneXid( xid );
        xidIdentMap.remove( identifier );
        recoveredTxMap.remove( identifier );
        return true;
    }

    // [TX_2P_COMMIT][identifier]
    public synchronized void commitTwoPhase( int identifier ) throws XAException
    {
        assert xidIdentMap.get( identifier ) != null;
        try
        {
            writeBuffer.put( TX_2P_COMMIT ).putInt( identifier );
            writeBuffer.force();
        }
        catch ( IOException e )
        {
            throw new XAException( "Logical log unable to mark 2PC ["
                + identifier + "] " + e );
        }
    }
    
    private boolean readTxTwoPhaseCommit() throws IOException
    {
        // get the neo tx identifier
        buffer.clear();
        buffer.limit( 4 );
        if ( fileChannel.read( buffer ) != buffer.limit() )
        {
            return false;
        }
        buffer.flip();
        int identifier = buffer.getInt();
        StartEntry entry = xidIdentMap.get( identifier );
        if ( entry == null )
        {
            return false;
        }
        Xid xid = entry.getXid();
        if ( xid == null )
        {
            return false;
        }
        try
        {
            xaRm.injectTwoPhaseCommit( xid );
        }
        catch ( XAException e )
        {
            e.printStackTrace();
            throw new IOException( e.getMessage() );
        }
        return true;
    }
    
    // [COMMAND][identifier][COMMAND_DATA]
    public synchronized void writeCommand( XaCommand command, int identifier )
        throws IOException
    {
        checkLogRotation();
        assert xidIdentMap.get( identifier ) != null;
        writeBuffer.put( COMMAND ).putInt( identifier );
        command.writeToFile( writeBuffer ); // fileChannel, buffer );
    }

    private boolean readCommandEntry() throws IOException
    {
        buffer.clear();
        buffer.limit( 4 );
        if ( fileChannel.read( buffer ) != buffer.limit() )
        {
            return false;
        }
        buffer.flip();
        int identifier = buffer.getInt();
        XaCommand command = cf.readCommand( fileChannel, buffer );
        if ( command == null )
        {
            // readCommand returns null if full command couldn't be loaded
            return false;
        }
        command.setRecovered();
        XaTransaction xaTx = recoveredTxMap.get( identifier );
        xaTx.injectCommand( command );
        return true;
    }

    private void checkLogRotation() throws IOException
    {
        if ( autoRotate && 
            writeBuffer.getFileChannelPosition() >= rotateAtSize )
        {
            long currentPos = writeBuffer.getFileChannelPosition();
            long firstStartEntry = getFirstStartEntry( currentPos ); 
            // only rotate if no huge tx is running
            if ( ( currentPos - firstStartEntry ) < rotateAtSize / 2 )
            {
                rotate();
            }
        }
    }

    private void renameCurrentLogFileAndIncrementVersion( String logFileName, 
        long endPosition ) throws IOException
    {
        releaseCurrentLogFile();
        File file = new File( logFileName );
        if ( !file.exists() )
        {
            throw new IOException( "Logical log[" + logFileName + 
                "] not found" );
        }
        String newName = fileName + ".v" + xaTf.getAndSetNewVersion();
        File newFile = new File( newName );
        boolean renamed = FileUtils.renameFile( file, newFile );
        
        if ( !renamed )
        {
            throw new IOException( "Failed to rename log to: " + newName );
        }
        else
        {
            try
            {
                FileChannel channel = new RandomAccessFile( newName, 
                    "rw" ).getChannel();
                FileUtils.truncateFile( channel, endPosition );
            }
            catch ( Exception e )
            {
                log.log( Level.WARNING, 
                    "Failed to truncate log at correct size", e );
            }
        }
    }
    
    private void deleteCurrentLogFile( String logFileName ) throws IOException
    {
        releaseCurrentLogFile();
        File file = new File( logFileName );
        if ( !file.exists() )
        {
            throw new IOException( "Logical log[" + logFileName + 
                "] not found" );
        }
        boolean deleted = FileUtils.deleteFile( file );
        if ( !deleted )
        {
            log.warning( "Unable to delete clean logical log[" + logFileName +
                "]" );
        }
    }
    
    private void releaseCurrentLogFile() throws IOException
    {
        if ( writeBuffer != null )
        {
            writeBuffer.force();
            writeBuffer = null;
        }
        fileChannel.close();
        fileChannel = null;
    }

    public synchronized void close() throws IOException
    {
        if ( fileChannel == null || !fileChannel.isOpen() )
        {
            log.info( "Logical log: " + fileName + " already closed" );
            return;
        }
        long endPosition = writeBuffer.getFileChannelPosition();
        if ( xidIdentMap.size() > 0 )
        {
            log.info( "Close invoked with " + xidIdentMap.size() + 
                " running transaction(s). " );
            writeBuffer.force();
            writeBuffer = null;
            fileChannel.close();
            log.info( "Dirty log: " + fileName + "." + currentLog + 
                " now closed. Recovery will be started automatically next " + 
                "time it is opened." );
            return;
        }
        if ( !keepLogs || backupSlave )
        {
            if ( currentLog == CLEAN )
            {
                // special case going from old xa version with no log rotation
                // and we started with a recovery
                deleteCurrentLogFile( fileName );
            }
            else
            {
                deleteCurrentLogFile( fileName + "." + currentLog );
            }
        }
        else
        {
            renameCurrentLogFileAndIncrementVersion( fileName + "." + 
                currentLog, endPosition );
        }
        if ( currentLog != CLEAN ) // again special case, see above
        {
            setActiveLog( CLEAN );
        }
    }

    private void doInternalRecovery( String logFileName ) throws IOException
    {
        log.info( "Non clean shutdown detected on log [" + logFileName + 
            "]. Recovery started ..." );
        // get log creation time
        buffer.clear();
        buffer.limit( 8 );
        if ( fileChannel.read( buffer ) != 8 )
        {
            log.info( "Unable to read timestamp information, "
                + "no records in logical log." );
            fileChannel.close();
            boolean success = FileUtils.renameFile( new File( logFileName ), 
                new File( logFileName + "_unknown_timestamp_" + 
                    System.currentTimeMillis() + ".log" ) );
            assert success;
            fileChannel = new RandomAccessFile( logFileName, 
                "rw" ).getChannel();
            return;
        }
        buffer.flip();
        logVersion = buffer.getLong();
        log.fine( "Logical log version: " + logVersion );
        long logEntriesFound = 0;
        while ( readEntry() )
        {
            logEntriesFound++;
        }
        scanIsComplete = true;
        log.fine( "Internal recovery completed, scanned " + logEntriesFound
            + " log entries." );
        xaRm.checkXids();
        if ( xidIdentMap.size() == 0 )
        {
            log.fine( "Recovery completed." );
        }
        else
        {
            log.fine( "[" + logFileName + "] Found " + xidIdentMap.size()
                + " prepared 2PC transactions." );
            for ( StartEntry entry : xidIdentMap.values() )
            {
                log.fine( "[" + logFileName + "] 2PC xid[" + 
                    entry.getXid() + "]" );
            }
        }
        recoveredTxMap.clear();
    }

    // for testing, do not use!
    void reset()
    {
        xidIdentMap.clear();
        recoveredTxMap.clear();
    }

    private boolean readEntry() throws IOException
    {
        buffer.clear();
        buffer.limit( 1 );
        if ( fileChannel.read( buffer ) != buffer.limit() )
        {
            // ok no more entries we're done
            return false;
        }
        buffer.flip();
        byte entry = buffer.get();
        switch ( entry )
        {
            case TX_START:
                return readTxStartEntry();
            case TX_PREPARE:
                return readTxPrepareEntry();
            case TX_1P_COMMIT:
                return readTxOnePhaseCommit();
            case TX_2P_COMMIT:
                return readTxTwoPhaseCommit();
            case COMMAND:
                return readCommandEntry();
            case DONE:
                return readDoneEntry();
            case EMPTY:
                fileChannel.position( fileChannel.position() - 1 );
                return false;
            default:
                throw new IOException( "Internal recovery failed, "
                    + "unknown log entry[" + entry + "]" );
        }
    }

    private ArrayMap<Thread,Integer> txIdentMap = 
        new ArrayMap<Thread,Integer>( 5, true, true );

    void registerTxIdentifier( int identifier )
    {
        txIdentMap.put( Thread.currentThread(), identifier );
    }

    void unregisterTxIdentifier()
    {
        txIdentMap.remove( Thread.currentThread() );
    }

    /**
     * If the current thread is committing a transaction the identifier of that
     * {@link XaTransaction} can be obtained invoking this method.
     * 
     * @return the identifier of the transaction committing or <CODE>-1</CODE>
     *         if current thread isn't committing any transaction
     */
    public int getCurrentTxIdentifier()
    {
        Integer intValue = txIdentMap.get( Thread.currentThread() );
        if ( intValue != null )
        {
            return intValue;
        }
        return -1;
    }

    public ReadableByteChannel getLogicalLog( long version ) throws IOException
    {
        String name = fileName + ".v" + version;
        if ( !new File( name ).exists() )
        {
            throw new IOException( "No such log version:" + version );
        }
        return new RandomAccessFile( name, "r" ).getChannel();
    }
    
    public boolean hasLogicalLog( long version )
    {
        String name = fileName + ".v" + version;
        return new File( name ).exists();
    }
    
    public boolean deleteLogicalLog( long version )
    {
        String name = fileName + ".v" + version;
        File file = new File(name );
        if ( file.exists() )
        {
            return FileUtils.deleteFile( file );
        }
        return false;
    }
    
    public void makeBackupSlave()
    {
        if ( xidIdentMap.size() > 0 )
        {
            throw new IllegalStateException( "There are active transactions" );
        }
        backupSlave = true;
    }
    
    private static class LogApplier
    {
        private final ReadableByteChannel byteChannel;
        private final ByteBuffer buffer;
        private final XaTransactionFactory xaTf;
        private final XaResourceManager xaRm;
        private final XaCommandFactory xaCf;
        private final ArrayMap<Integer,StartEntry> xidIdentMap;
        private final Map<Integer,XaTransaction> recoveredTxMap;
        
        LogApplier( ReadableByteChannel byteChannel, ByteBuffer buffer, 
            XaTransactionFactory xaTf, XaResourceManager xaRm, 
            XaCommandFactory xaCf, ArrayMap<Integer,StartEntry> xidIdentMap, 
            Map<Integer,XaTransaction> recoveredTxMap )
        {
            this.byteChannel = byteChannel;
            this.buffer = buffer;
            this.xaTf = xaTf;
            this.xaRm = xaRm;
            this.xaCf = xaCf;
            this.xidIdentMap = xidIdentMap;
            this.recoveredTxMap = recoveredTxMap;
        }
        
        boolean readAndApplyEntry() throws IOException
        {
            buffer.clear();
            buffer.limit( 1 );
            if ( byteChannel.read( buffer ) != buffer.limit() )
            {
                // ok no more entries we're done
                return false;
            }
            buffer.flip();
            byte entry = buffer.get();
            switch ( entry )
            {
                case TX_START:
                    readTxStartEntry();
                    return true;
                case TX_PREPARE:
                    readTxPrepareEntry();
                    return true;
                case TX_1P_COMMIT:
                    readAndApplyTxOnePhaseCommit();
                    return true;
                case TX_2P_COMMIT:
                    readAndApplyTxTwoPhaseCommit();
                    return true;
                case COMMAND:
                    readCommandEntry();
                    return true;
                case DONE:
                    readDoneEntry();
                    return true;
                case EMPTY:
                    return false;
                default:
                    throw new IOException( "Internal recovery failed, "
                        + "unknown log entry[" + entry + "]" );
            }
        }

        private void readTxStartEntry() throws IOException
        {
            // get the global id
            buffer.clear();
            buffer.limit( 1 );
            if ( byteChannel.read( buffer ) != buffer.limit() )
            {
                throw new IOException( "Unable to read tx start entry" );
            }
            buffer.flip();
            byte globalIdLength = buffer.get();
            // get the branchId id
            buffer.clear();
            buffer.limit( 1 );
            if ( byteChannel.read( buffer ) != buffer.limit() )
            {
                throw new IOException( "Unable to read tx start entry" );
            }
            buffer.flip();
            byte branchIdLength = buffer.get();
            byte globalId[] = new byte[globalIdLength];
            ByteBuffer tmpBuffer = ByteBuffer.wrap( globalId );
            if ( byteChannel.read( tmpBuffer ) != globalId.length )
            {
                throw new IOException( "Unable to read tx start entry" );
            }
            byte branchId[] = new byte[branchIdLength];
            tmpBuffer = ByteBuffer.wrap( branchId );
            if ( byteChannel.read( tmpBuffer ) != branchId.length )
            {
                throw new IOException( "Unable to read tx start entry" );
            }
            // get the neo tx identifier
            buffer.clear();
            buffer.limit( 4 );
            if ( byteChannel.read( buffer ) != buffer.limit() )
            {
                throw new IOException( "Unable to read tx start entry" );
            }
            buffer.flip();
            int identifier = buffer.getInt();
            // get the format id
            buffer.clear();
            buffer.limit( 4 );
            if ( byteChannel.read( buffer ) != buffer.limit() )
            {
                throw new IOException( "Unable to read tx start entry" );
            }
            buffer.flip();
            int formatId = buffer.getInt();
            // re-create the transaction
            Xid xid = new XidImpl( globalId, branchId, formatId );
            xidIdentMap.put( identifier, new StartEntry( xid, -1 ) );
            XaTransaction xaTx = xaTf.create( identifier );
            xaTx.setRecovered();
            recoveredTxMap.put( identifier, xaTx );
            xaRm.injectStart( xid, xaTx );
        }
    
        private void readTxPrepareEntry() throws IOException
        {
            // get the neo tx identifier
            buffer.clear();
            buffer.limit( 4 );
            if ( byteChannel.read( buffer ) != buffer.limit() )
            {
                throw new IOException( "Unable to read tx prepare entry" );
            }
            buffer.flip();
            int identifier = buffer.getInt();
            StartEntry entry = xidIdentMap.get( identifier );
            if ( entry == null )
            {
                throw new IOException( "Unable to read tx prepeare entry" );
            }
            Xid xid = entry.getXid();
            if ( xaRm.injectPrepare( xid ) )
            {
                // read only, we can remove
                xidIdentMap.remove( identifier );
                recoveredTxMap.remove( identifier );
            }
        }

        private void readAndApplyTxOnePhaseCommit() throws IOException
        {
            // get the neo tx identifier
            buffer.clear();
            buffer.limit( 4 );
            if ( byteChannel.read( buffer ) != buffer.limit() )
            {
                throw new IOException( "Unable to read tx 1PC entry" );
            }
            buffer.flip();
            int identifier = buffer.getInt();
            StartEntry entry = xidIdentMap.get( identifier );
            if ( entry == null )
            {
                throw new IOException( "Unable to read tx prepeare entry" );
            }
            Xid xid = entry.getXid();
            try
            {
                xaRm.commit( xid, true );
            }
            catch ( XAException e )
            {
                e.printStackTrace();
                throw new IOException( e.getMessage() );
            }
        }

        private void readAndApplyTxTwoPhaseCommit() throws IOException
        {
            // get the neo tx identifier
            buffer.clear();
            buffer.limit( 4 );
            if ( byteChannel.read( buffer ) != buffer.limit() )
            {
                throw new IOException( "Unable to read tx 2PC entry" );
            }
            buffer.flip();
            int identifier = buffer.getInt();
            StartEntry entry = xidIdentMap.get( identifier );
            if ( entry == null )
            {
                throw new IOException( "Unable to read tx prepeare entry" );
            }
            Xid xid = entry.getXid();
            try
            {
                xaRm.commit( xid, true );
            }
            catch ( XAException e )
            {
                e.printStackTrace();
                throw new IOException( e.getMessage() );
            }
        }

        private void readCommandEntry() throws IOException
        {
            buffer.clear();
            buffer.limit( 4 );
            if ( byteChannel.read( buffer ) != buffer.limit() )
            {
                throw new IOException( "Unable to read tx command entry" );
            }
            buffer.flip();
            int identifier = buffer.getInt();
            XaCommand command = xaCf.readCommand( byteChannel, buffer );
            if ( command == null )
            {
                throw new IOException( "Unable to read command entry" );
            }
            command.setRecovered();
            XaTransaction xaTx = recoveredTxMap.get( identifier );
            xaTx.injectCommand( command );
        }

        private boolean readDoneEntry() throws IOException
        {
            // get the neo tx identifier
            buffer.clear();
            buffer.limit( 4 );
            if ( byteChannel.read( buffer ) != buffer.limit() )
            {
                return false;
            }
            buffer.flip();
            int identifier = buffer.getInt();
            StartEntry entry = xidIdentMap.get( identifier );
            if ( entry == null )
            {
                throw new IOException( "Unable to read tx done entry" );
            }
            Xid xid = entry.getXid();
            xaRm.pruneXidIfExist( xid );
            xidIdentMap.remove( identifier );
            recoveredTxMap.remove( identifier );
            return true;
        }
    }
    
    public synchronized void applyLog( ReadableByteChannel byteChannel ) 
        throws IOException
    {
        if ( !backupSlave )
        {
            throw new IllegalStateException( "This is not a backup slave" );
        }
        if ( xidIdentMap.size() > 0 )
        {
            throw new IllegalStateException( "There are active transactions" );
        }
        buffer.clear();
        buffer.limit( 8 );
        if ( byteChannel.read( buffer ) != 8 )
        {
            throw new IOException( "Unable to read log version" );
        }
        buffer.flip();
        logVersion = buffer.getLong();
        if ( logVersion != xaTf.getCurrentVersion() )
        {
            throw new IllegalStateException( "Tried to apply version " + 
                logVersion + " but expected version " + 
                xaTf.getCurrentVersion() );
        }
        log.fine( "Logical log version: " + logVersion );
        long logEntriesFound = 0;
        LogApplier logApplier = new LogApplier( byteChannel, buffer, xaTf, xaRm,
            cf, xidIdentMap, recoveredTxMap );
        while ( logApplier.readAndApplyEntry() )
        {
            logEntriesFound++;
        }
        byteChannel.close();
        xaTf.getAndSetNewVersion();
        xaRm.reset();
        log.info( "Log version " + logVersion + " applied successfully." );
    }
    
    public synchronized void rotate() throws IOException
    {
        xaTf.flushAll();
        String newLogFile = fileName + ".2";
        String currentLogFile = fileName + ".1";
        char newActiveLog = LOG2;
        long currentVersion = xaTf.getCurrentVersion();
        String oldCopy = fileName + ".v" + currentVersion;
        if ( currentLog == CLEAN || currentLog == LOG2 )
        {
            newActiveLog = LOG1;
            newLogFile = fileName + ".1";
            currentLogFile = fileName + ".2";
        }
        else
        {
            assert currentLog == LOG1;
        }
        if ( new File( newLogFile ).exists() )
        {
            throw new IOException( "New log file: " + newLogFile + 
                " already exist" );
        }
        if ( new File( oldCopy ).exists() )
        {
            throw new IOException( "Copy log file: " + oldCopy + 
                " already exist" );
        }
        long endPosition = writeBuffer.getFileChannelPosition();
        writeBuffer.force();
        FileChannel newLog = new RandomAccessFile( 
            newLogFile, "rw" ).getChannel();
        buffer.clear();
        buffer.putLong( currentVersion + 1 ).flip();
        if ( newLog.write( buffer ) != 8 )
        {
            throw new IOException( "Unable to write log version to new" );
        }
        fileChannel.position( 0 );
        buffer.clear();
        buffer.limit( 8 );
        if( fileChannel.read( buffer ) != 8 )
        {
            throw new IOException( "Verification of log version failed" );
        }
        buffer.flip();
        long verification = buffer.getLong();
        if ( verification != currentVersion )
        {
            throw new IOException( "Verification of log version failed, " + 
                " expected " + currentVersion + " got " + verification );
        }
        if ( xidIdentMap.size() > 0 )
        {
            fileChannel.position( getFirstStartEntry( endPosition ) );
        }
        buffer.clear();
        buffer.limit( 1 );
        boolean emptyHit = false;
        while ( fileChannel.read( buffer ) == 1 && !emptyHit )
        {
            buffer.flip();
            byte entry = buffer.get();
            switch ( entry )
            {
                case TX_START:
                    readAndWriteTxStartEntry( newLog );
                    break;
                case TX_PREPARE:
                    readAndWriteTxPrepareEntry( newLog );
                    break;
                case TX_1P_COMMIT:
                    readAndWriteTxOnePhaseCommit( newLog );
                    break;
                case TX_2P_COMMIT:
                    readAndWriteTxTwoPhaseCommit( newLog );
                    break;
                case COMMAND:
                    readAndWriteCommandEntry( newLog );
                    break;
                case DONE:
                    readAndVerifyDoneEntry();
                    break;
                case EMPTY:
                    emptyHit = true;
                    break;
                default:
                    throw new IOException( "Log rotation failed, "
                        + "unknown log entry[" + entry + "]" );
            }
            buffer.clear();
            buffer.limit( 1 );
        }
        newLog.force( false );
        setActiveLog( newActiveLog );
        if ( keepLogs )
        {
            renameCurrentLogFileAndIncrementVersion( currentLogFile, 
                endPosition );
        }
        else
        {
            deleteCurrentLogFile( currentLogFile );
            xaTf.getAndSetNewVersion();
        }
        if ( xaTf.getCurrentVersion() != ( currentVersion + 1 ) )
        {
            throw new IOException( "version change failed" );
        }
        fileChannel = newLog;
        if ( !useMemoryMapped )
        {
            writeBuffer = new DirectMappedLogBuffer( fileChannel );
        }
        else
        {
            writeBuffer = new MemoryMappedLogBuffer( fileChannel );
        }
    }
    
    private long getFirstStartEntry( long endPosition )
    {
        long firstEntryPosition = endPosition;
        for ( StartEntry entry : xidIdentMap.values() )
        {
            if ( entry.getStartPosition() < firstEntryPosition )
            {
                assert entry.getStartPosition() > 0; 
                firstEntryPosition = entry.getStartPosition();
            }
        }
        return firstEntryPosition;
    }

    private void setActiveLog( char c ) throws IOException
    {
        if ( c != CLEAN && c != LOG1 && c != LOG2 )
        {
            throw new IllegalArgumentException( "Log must be either clean, " + 
                "1 or 2" );
        }
        if ( c == currentLog )
        {
            throw new IllegalStateException( "Log should not be equal to " + 
                "current " + currentLog );
        }
        ByteBuffer bb = ByteBuffer.wrap( new byte[4] );
        bb.asCharBuffer().put( c ).flip();
        FileChannel fc = new RandomAccessFile( fileName + ".active" , 
            "rw" ).getChannel();
        int wrote = fc.write( bb );
        if ( wrote != 4 )
        {
            throw new IllegalStateException( "Expected to write 4 -> " + wrote );
        }
        fc.force( false );
        fc.close();
        currentLog = c;
    }

    // [COMMAND][identifier][COMMAND_DATA]
    private void readAndWriteCommandEntry( FileChannel newLog ) 
        throws IOException
    {
        buffer.clear();
        buffer.put( COMMAND );
        buffer.limit( 1 + 4 );
        if ( fileChannel.read( buffer ) != 4 )
        {
            throw new IllegalStateException( "Unable to read command header" );
        }
        buffer.flip();
        buffer.position( 1 );
        int identifier = buffer.getInt();
        FileChannel writeToLog = null;
        if ( xidIdentMap.get( identifier ) != null )
        {
            writeToLog = newLog;
        }
        if ( writeToLog != null )
        {
            buffer.position( 0 );
            if ( writeToLog.write( buffer ) != 5 )
            {
                throw new RuntimeException( "Unable to write command header" );
            }
        }
        XaCommand command = cf.readCommand( fileChannel, buffer );
        if ( writeToLog != null )
        {
            command.writeToFile( new DirectLogBuffer( writeToLog, buffer ) );
        }
    }

    private void readAndVerifyDoneEntry() 
        throws IOException
    {
        buffer.clear();
        buffer.limit( 4 );
        if ( fileChannel.read( buffer ) != 4 )
        {
            throw new IllegalStateException( "Unable to read done entry" );
        }
        buffer.flip();
        int identifier = buffer.getInt();
        if ( xidIdentMap.get( identifier ) != null )
        {
            throw new IllegalStateException( identifier + 
                " done entry found but still active" );
        }
    }

    // [TX_1P_COMMIT][identifier]
    private void readAndWriteTxOnePhaseCommit( FileChannel newLog ) 
        throws IOException
    {
        buffer.clear();
        buffer.limit( 1 + 4 );
        buffer.put( TX_1P_COMMIT );
        if ( fileChannel.read( buffer ) != 4 )
        {
            throw new IllegalStateException( "Unable to read 1P commit entry" );
        }
        buffer.flip();
        buffer.position( 1 );
        int identifier = buffer.getInt();
        FileChannel writeToLog = null;
        if ( xidIdentMap.get( identifier ) != null )
        {
            writeToLog = newLog;
        }
        buffer.position( 0 );
        if ( writeToLog != null && writeToLog.write( buffer ) != 5 )
        {
            throw new RuntimeException( "Unable to write 1P commit entry" );
        }
    }

    private void readAndWriteTxTwoPhaseCommit( FileChannel newLog ) 
        throws IOException
    {
        buffer.clear();
        buffer.limit( 1 + 4 );
        buffer.put( TX_2P_COMMIT );
        if ( fileChannel.read( buffer ) != 4 )
        {
            throw new IllegalStateException( "Unable to read 2P commit entry" );
        }
        buffer.flip();
        buffer.position( 1 );
        int identifier = buffer.getInt();
        FileChannel writeToLog = null;
        if ( xidIdentMap.get( identifier ) != null )
        {
//            throw new IllegalStateException( identifier + 
//                " 2PC found but still active" );
            writeToLog = newLog;
        }
        buffer.position( 0 );
        if ( writeToLog != null && writeToLog.write( buffer ) != 5 )
        {
            throw new RuntimeException( "Unable to write 2P commit entry" );
        }
    }
    
    private void readAndWriteTxPrepareEntry( FileChannel newLog ) 
        throws IOException
    {
        // get the neo tx identifier
        buffer.clear();
        buffer.limit( 1 + 4 );
        buffer.put( TX_PREPARE );
        if ( fileChannel.read( buffer ) != 4 )
        {
            throw new IllegalStateException( "Unable to read prepare entry" );
        }
        buffer.flip();
        buffer.position( 1 );
        int identifier = buffer.getInt();
        FileChannel writeToLog = null;
        if ( xidIdentMap.get( identifier ) != null )
        {
            writeToLog = newLog;
        }
        buffer.position( 0 );
        if ( writeToLog != null && writeToLog.write( buffer ) != 5 )
        {
            throw new RuntimeException( "Unable to write prepare entry" );
        }
    }

    // [TX_START][xid[gid.length,bid.lengh,gid,bid]][identifier][format id]
    private void readAndWriteTxStartEntry( FileChannel newLog ) 
        throws IOException
    {
        // get the global id
        buffer.clear();
        buffer.put( TX_START );
        buffer.limit( 3 );
        if ( fileChannel.read( buffer ) != 2 )
        {
            throw new IllegalStateException( 
                "Unable to read tx start entry xid id lengths" );
        }
        buffer.flip();
        buffer.position( 1 );
        byte globalIdLength = buffer.get();
        byte branchIdLength = buffer.get();
        int xidLength = globalIdLength + branchIdLength;
        buffer.limit( 3 + xidLength + 8 );
        buffer.position( 3 );
        if ( fileChannel.read( buffer ) != 8 + xidLength )
        {
            throw new IllegalStateException( "Unable to read xid" );
        }
        buffer.flip();
        buffer.position( 3 + xidLength );
        int identifier = buffer.getInt();
        FileChannel writeToLog = null;
        StartEntry entry = xidIdentMap.get( identifier );
        if ( entry != null )
        {
            writeToLog = newLog;
            entry.setStartPosition( newLog.position() );
        }
        buffer.position( 0 );
        if ( writeToLog != null && 
            writeToLog.write( buffer ) != 3 + 8 + xidLength )
        {
            throw new RuntimeException( "Unable to write tx start xid" );
        }
    }

    public void setKeepLogs( boolean keep )
    {
        this.keepLogs = keep;
    }
    
    public boolean isLogsKept()
    {
        return this.keepLogs;
    }
    
    public void setAutoRotateLogs( boolean autoRotate )
    {
        this.autoRotate = autoRotate;
    }
    
    public boolean isLogsAutoRotated()
    {
        return this.autoRotate;
    }
    
    public void setLogicalLogTargetSize( long size )
    {
        this.rotateAtSize = size;
    }
    
    public long getLogicalLogTargetSize()
    {
        return this.rotateAtSize;
    }
    
    private static class StartEntry
    {
        private final Xid xid;
        private long startEntryPosition;
        
        StartEntry( Xid xid, long startPosition )
        {
            this.xid = xid;
            this.startEntryPosition = startPosition;
        }
        
        Xid getXid()
        {
            return xid;
        }
        
        long getStartPosition()
        {
            return startEntryPosition;
        }
        
        void setStartPosition( long newPosition )
        {
            startEntryPosition = newPosition;
        }
    }
}