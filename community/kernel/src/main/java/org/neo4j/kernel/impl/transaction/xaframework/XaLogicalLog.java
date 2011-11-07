/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import javax.transaction.xa.XAException;
import javax.transaction.xa.Xid;

import org.neo4j.helpers.Exceptions;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.impl.cache.LruCache;
import org.neo4j.kernel.impl.transaction.xaframework.LogEntry.Commit;
import org.neo4j.kernel.impl.transaction.xaframework.LogEntry.Start;
import org.neo4j.kernel.impl.util.ArrayMap;
import org.neo4j.kernel.impl.util.BufferedFileChannel;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.kernel.impl.util.StringLogger;

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
    private final Logger log;

    private static final char CLEAN = 'C';
    private static final char LOG1 = '1';
    private static final char LOG2 = '2';

    private FileChannel fileChannel = null;
    private final ByteBuffer sharedBuffer;
    private LogBuffer writeBuffer = null;
    private long previousLogLastCommittedTx = -1;
    private long logVersion = 0;
    private final ArrayMap<Integer,LogEntry.Start> xidIdentMap =
        new ArrayMap<Integer,LogEntry.Start>( 4, false, true );
    private final Map<Integer,XaTransaction> recoveredTxMap =
        new HashMap<Integer,XaTransaction>();
    private int nextIdentifier = 1;
    private boolean scanIsComplete = false;
    private boolean nonCleanShutdown = false;

    private String fileName = null;
    private final XaResourceManager xaRm;
    private final XaCommandFactory cf;
    private final XaTransactionFactory xaTf;
    private char currentLog = CLEAN;
    private boolean keepLogs = false;
    private boolean autoRotate = true;
    private long rotateAtSize = 25*1024*1024; // 25MB

    private final String storeDir;
    private final LogBufferFactory logBufferFactory;
    private boolean doingRecovery;
    private long lastRecoveredTx = -1;
    private long recoveredTxCount;

    private final StringLogger msgLog;

    private final LruCache<Long, TxPosition> txStartPositionCache =
            new LruCache<Long, TxPosition>( "Tx start position cache", 10000, null );
    private final LruCache<Long /*log version*/, Long /*last committed tx*/> logHeaderCache =
            new LruCache<Long, Long>( "Log header cache", 1000, null );

    XaLogicalLog( String fileName, XaResourceManager xaRm, XaCommandFactory cf,
            XaTransactionFactory xaTf, Map<Object, Object> config )
    {
        this.fileName = fileName;
        this.xaRm = xaRm;
        this.cf = cf;
        this.xaTf = xaTf;
        this.logBufferFactory = (LogBufferFactory) config.get( LogBufferFactory.class );

        log = Logger.getLogger( this.getClass().getName() + File.separator + fileName );
        sharedBuffer = ByteBuffer.allocateDirect( 9 + Xid.MAXGTRIDSIZE
            + Xid.MAXBQUALSIZE * 10 );
        storeDir = (String) config.get( "store_dir" );
        msgLog = StringLogger.getLogger( storeDir);

        // We should turn keep-logs on if there are previous logs around,
        // this so that e.g. temporary shell sessions or operations don't create
        // holes in the log history, because it's just annoying.
        keepLogs = hasPreviousLogs();
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
                open( getLog1FileName() );
                setActiveLog( LOG1 );
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
            if ( c == CLEAN )
            {
                // clean
                String newLog = getLog1FileName();
                File file = new File( newLog );
                if ( file.exists() )
                {
                    renameLogFileToRightVersion( getLog1FileName(), file.length() );
                }
                file = new File( getLog2FileName() );
                if ( file.exists() )
                {
                    renameLogFileToRightVersion( getLog2FileName(), file.length() );
                }
                open( newLog );
                setActiveLog( LOG1 );
            }
            else if ( c == LOG1 )
            {
                String newLog = getLog1FileName();
                if ( !new File( newLog ).exists() )
                {
                    throw new IllegalStateException(
                        "Active marked as 1 but no " + newLog + " exist" );
                }
                File otherLog = new File( getLog2FileName() );
                if ( otherLog.exists() )
                {
                    fixDualLogFiles( getLog1FileName(), getLog2FileName() );
                }
                currentLog = LOG1;
                open( newLog );
            }
            else if ( c == LOG2 )
            {
                String newLog = getLog2FileName();
                if ( !new File( newLog ).exists() )
                {
                    throw new IllegalStateException(
                        "Active marked as 2 but no " + newLog + " exist" );
                }
                File otherLog = new File( getLog1FileName() );
                if ( otherLog.exists() )
                {
                    fixDualLogFiles( getLog2FileName(), getLog1FileName() );
                }
                currentLog = LOG2;
                open( newLog );
            }
            else
            {
                throw new IllegalStateException( "Unknown active log: " + c );
            }
        }

        instantiateCorrectWriteBuffer();
    }

    private void instantiateCorrectWriteBuffer() throws IOException
    {
        writeBuffer = instantiateCorrectWriteBuffer( fileChannel );
    }

    private LogBuffer instantiateCorrectWriteBuffer( FileChannel channel ) throws IOException
    {
        return logBufferFactory.create( channel );
    }

    private void open( String fileToOpen ) throws IOException
    {
        fileChannel = new RandomAccessFile( fileToOpen, "rw" ).getChannel();
        if ( fileChannel.size() != 0 )
        {
            nonCleanShutdown = true;
            doingRecovery = true;
            try
            {
                doInternalRecovery( fileToOpen );
            }
            finally
            {
                doingRecovery = false;
            }
        }
        else
        {
            logVersion = xaTf.getCurrentVersion();
            long lastTxId = xaTf.getLastCommittedTx();
            LogIoUtils.writeLogHeader( sharedBuffer, logVersion, lastTxId );
            previousLogLastCommittedTx = lastTxId;
            logHeaderCache.put( logVersion, previousLogLastCommittedTx );
            fileChannel.write( sharedBuffer );
            scanIsComplete = true;
            msgLog.logMessage( "Opened [" + fileToOpen + "] clean empty log, version=" + logVersion + ", lastTxId=" + lastTxId, true );
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
    public synchronized int start( Xid xid, int masterId, int myId ) throws XAException
    {
        int xidIdent = getNextIdentifier();
        try
        {
            long position = writeBuffer.getFileChannelPosition();
            long timeWritten = System.currentTimeMillis();
            LogEntry.Start start = new LogEntry.Start( xid, xidIdent, masterId, myId, position, timeWritten );
            LogIoUtils.writeStart( writeBuffer, xidIdent, xid, masterId, myId, timeWritten );
            xidIdentMap.put( xidIdent, start );
        }
        catch ( IOException e )
        {
            throw Exceptions.withCause( new XAException( "Logical log couldn't start transaction: " + e ), e );
        }
        return xidIdent;
    }

    // [TX_PREPARE][identifier]
    public synchronized void prepare( int identifier ) throws XAException
    {
        LogEntry.Start startEntry = xidIdentMap.get( identifier );
        assert startEntry != null;
        try
        {
            LogIoUtils.writePrepare( writeBuffer, identifier, System.currentTimeMillis() );
            writeBuffer.writeOut();
        }
        catch ( IOException e )
        {
            throw Exceptions.withCause( new XAException( "Logical log unable to mark prepare [" + identifier + "] " ),
                    e );
        }
    }

    // [TX_1P_COMMIT][identifier]
    public synchronized void commitOnePhase( int identifier, long txId )
        throws XAException
    {
        LogEntry.Start startEntry = xidIdentMap.get( identifier );
        assert startEntry != null;
        assert txId != -1;
        try
        {
            LogIoUtils.writeCommit( false, writeBuffer, identifier, txId, System.currentTimeMillis() );
            writeBuffer.force();
            cacheTxStartPosition( txId, startEntry.getMasterId(), startEntry );
        }
        catch ( IOException e )
        {
            throw Exceptions.withCause(
                    new XAException( "Logical log unable to mark 1P-commit [" + identifier + "] " ), e );
        }
    }

    private synchronized void cacheTxStartPosition( long txId, int masterId,
            LogEntry.Start startEntry )
    {
        cacheTxStartPosition( txId, masterId, startEntry, logVersion );
    }

    private synchronized TxPosition cacheTxStartPosition( long txId, int masterId,
            LogEntry.Start startEntry, long logVersion )
    {
        if ( startEntry.getStartPosition() == -1 )
        {
            throw new RuntimeException( "StartEntry.position is " + startEntry.getStartPosition() );
        }

        TxPosition result = new TxPosition( logVersion, masterId, startEntry.getIdentifier(),
                startEntry.getStartPosition(), startEntry.getTimeWritten() );
        txStartPositionCache.put( txId, result );
        return result;
    }

    // [DONE][identifier]
    public synchronized void done( int identifier ) throws XAException
    {
        assert xidIdentMap.get( identifier ) != null;
        try
        {
            LogIoUtils.writeDone( writeBuffer, identifier );
            xidIdentMap.remove( identifier );
        }
        catch ( IOException e )
        {
            throw Exceptions.withCause( new XAException( "Logical log unable to mark as done [" + identifier + "] " ),
                    e );
        }
    }

    // [DONE][identifier] called from XaResourceManager during internal recovery
    synchronized void doneInternal( int identifier ) throws IOException
    {
        if ( writeBuffer != null )
        {   // For 2PC
            LogIoUtils.writeDone( writeBuffer, identifier );
        }
        else
        {   // For 1PC
            sharedBuffer.clear();
            LogIoUtils.writeDone( sharedBuffer, identifier );
            sharedBuffer.flip();
            fileChannel.write( sharedBuffer );
        }

        xidIdentMap.remove( identifier );
        // force to make sure done record is there if 2PC tx and global log
        // marks tx as committed
//        fileChannel.force( false );
    }

    // [TX_2P_COMMIT][identifier]
    public synchronized void commitTwoPhase( int identifier, long txId )
        throws XAException
    {
        LogEntry.Start startEntry = xidIdentMap.get( identifier );
        assert startEntry != null;
        assert txId != -1;
        try
        {
            LogIoUtils.writeCommit( true, writeBuffer, identifier, txId, System.currentTimeMillis() );
            writeBuffer.force();
            cacheTxStartPosition( txId, startEntry.getMasterId(), startEntry );
        }
        catch ( IOException e )
        {
            throw Exceptions.withCause( new XAException( "Logical log unable to mark 2PC [" + identifier + "] " ), e );
        }
    }

    // [COMMAND][identifier][COMMAND_DATA]
    public synchronized void writeCommand( XaCommand command, int identifier )
        throws IOException
    {
        checkLogRotation();
        assert xidIdentMap.get( identifier ) != null;
        LogIoUtils.writeCommand( writeBuffer, identifier, command );
    }

    private void applyEntry( LogEntry entry ) throws IOException
    {
        if ( entry instanceof LogEntry.Start )
        {
            applyStartEntry( (LogEntry.Start) entry );
        }
        else if ( entry instanceof LogEntry.Prepare )
        {
            applyPrepareEntry( (LogEntry.Prepare ) entry );
        }
        else if ( entry instanceof LogEntry.Command )
        {
            applyCommandEntry( (LogEntry.Command ) entry );
        }
        else if ( entry instanceof LogEntry.OnePhaseCommit )
        {
            applyOnePhaseCommitEntry( (LogEntry.OnePhaseCommit ) entry );
        }
        else if ( entry instanceof LogEntry.TwoPhaseCommit )
        {
            applyTwoPhaseCommitEntry( (LogEntry.TwoPhaseCommit ) entry );
        }
        else if ( entry instanceof LogEntry.Done )
        {
            applyDoneEntry( (LogEntry.Done ) entry );
        }
        else
        {
            throw new RuntimeException( "Unrecognized log entry " + entry );
        }
    }

    private void applyStartEntry( LogEntry.Start entry) throws IOException
    {
        int identifier = entry.getIdentifier();
        if ( identifier >= nextIdentifier )
        {
            nextIdentifier = (identifier + 1);
        }
        // re-create the transaction
        Xid xid = entry.getXid();
        xidIdentMap.put( identifier, entry );
        XaTransaction xaTx = xaTf.create( identifier );
        xaTx.setRecovered();
        recoveredTxMap.put( identifier, xaTx );
        xaRm.injectStart( xid, xaTx );
        // force to make sure done record is there if 2PC tx and global log
        // marks tx as committed
        // fileChannel.force( false );
    }


    private void applyPrepareEntry( LogEntry.Prepare prepareEntry ) throws IOException
    {
        // get the tx identifier
        int identifier = prepareEntry.getIdentifier();
        LogEntry.Start entry = xidIdentMap.get( identifier );
        if ( entry == null )
        {
            throw new IOException( "Unknown xid for identifier " + identifier );
        }
        Xid xid = entry.getXid();
        if ( xaRm.injectPrepare( xid ) )
        {
            // read only we can remove
            xidIdentMap.remove( identifier );
            recoveredTxMap.remove( identifier );
        }
    }

    private void applyOnePhaseCommitEntry( LogEntry.OnePhaseCommit commit )
        throws IOException
    {
        int identifier = commit.getIdentifier();
        long txId = commit.getTxId();
        LogEntry.Start startEntry = xidIdentMap.get( identifier );
        if ( startEntry == null )
        {
            throw new IOException( "Unknown xid for identifier " + identifier );
        }
        Xid xid = startEntry.getXid();
        try
        {
            XaTransaction xaTx = xaRm.getXaTransaction( xid );
            xaTx.setCommitTxId( txId );
            xaRm.injectOnePhaseCommit( xid );
            registerRecoveredTransaction( txId );
        }
        catch ( XAException e )
        {
            throw new IOException( e );
        }
    }

    private void registerRecoveredTransaction( long txId )
    {
        if ( doingRecovery )
        {
            lastRecoveredTx = txId;
            recoveredTxCount++;
        }
    }

    private void logRecoveryMessage( String string )
    {
        if ( doingRecovery )
        {
            msgLog.logMessage( string, true );
        }
    }

    private void applyDoneEntry( LogEntry.Done done ) throws IOException
    {
        // get the tx identifier
        int identifier = done.getIdentifier();
        LogEntry.Start entry = xidIdentMap.get( identifier );
        if ( entry == null )
        {
            throw new IOException( "Unknown xid for identifier " + identifier );
        }
        Xid xid = entry.getXid();
        xaRm.pruneXid( xid );
        xidIdentMap.remove( identifier );
        recoveredTxMap.remove( identifier );
    }

    private void applyTwoPhaseCommitEntry( LogEntry.TwoPhaseCommit commit ) throws IOException
    {
        int identifier = commit.getIdentifier();
        long txId = commit.getTxId();
        LogEntry.Start startEntry = xidIdentMap.get( identifier );
        if ( startEntry == null )
        {
            throw new IOException( "Unknown xid for identifier " + identifier );
        }
        Xid xid = startEntry.getXid();
        if ( xid == null )
        {
            throw new IOException( "Xid null for identifier " + identifier );
        }
        try
        {
            XaTransaction xaTx = xaRm.getXaTransaction( xid );
            xaTx.setCommitTxId( txId );
            xaRm.injectTwoPhaseCommit( xid );
            registerRecoveredTransaction( txId );
        }
        catch ( XAException e )
        {
            throw new IOException( e );
        }
    }

    private void applyCommandEntry( LogEntry.Command entry ) throws IOException
    {
        int identifier = entry.getIdentifier();
        XaCommand command = entry.getXaCommand();
        if ( command == null )
        {
            throw new IOException( "Null command for identifier " + identifier );
        }
        command.setRecovered();
        XaTransaction xaTx = recoveredTxMap.get( identifier );
        xaTx.injectCommand( command );
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

    private void fixDualLogFiles( String activeLog, String oldLog ) throws IOException
    {
        FileChannel activeLogChannel = new RandomAccessFile( activeLog, "r" ).getChannel();
        long[] activeLogHeader = LogIoUtils.readLogHeader( ByteBuffer.allocate( 16 ), activeLogChannel, false );
        activeLogChannel.close();

        FileChannel oldLogChannel = new RandomAccessFile( oldLog, "r" ).getChannel();
        long[] oldLogHeader = LogIoUtils.readLogHeader( ByteBuffer.allocate( 16 ), oldLogChannel, false );
        oldLogChannel.close();

        if ( oldLogHeader == null )
        {
            if ( !FileUtils.deleteFile( new File( oldLog ) ) )
            {
                throw new IOException( "Unable to delete " + oldLog );
            }
        }
        else if ( activeLogHeader == null || activeLogHeader[0] > oldLogHeader[0] )
        {
            // we crashed in rotate after setActive but did not move the old log to the right name
            // (and we do not know if keepLogs is true or not so play it safe by keeping it)
            String newName = getFileName( oldLogHeader[0] );
            if ( !FileUtils.renameFile( new File( oldLog ), new File( newName ) ) )
            {
                throw new IOException( "Unable to rename " + oldLog + " to " + newName );
            }
        }
        else
        {
            assert activeLogHeader[0] < oldLogHeader[0];
            // we crashed in rotate before setActive, do the rotate work again and delete old
            if ( !FileUtils.deleteFile( new File( oldLog ) ) )
            {
                throw new IOException( "Unable to delete " + oldLog );
            }
        }
    }

    private void renameLogFileToRightVersion( String logFileName, long endPosition ) throws IOException
    {
        File file = new File( logFileName );
        if ( !file.exists() )
        {
            throw new IOException( "Logical log[" + logFileName +
                "] not found" );
        }

        FileChannel channel = new RandomAccessFile( logFileName, "rw" ).getChannel();
        long[] header = LogIoUtils.readLogHeader( ByteBuffer.allocate( 16 ), channel, false );
        try
        {
            FileUtils.truncateFile( channel, endPosition );
        }
        catch ( IOException e )
        {
            log.log( Level.WARNING,
                "Failed to truncate log at correct size", e );
        }
        channel.close();
        String newName;
        if ( header == null )
        {
            // header was never written
            newName = getFileName( -1 ) + "_empty_header_log_" + System.currentTimeMillis();
        }
        else
        {
            newName = getFileName( header[0] );
        }
        File newFile = new File( newName );
        boolean renamed = FileUtils.renameFile( file, newFile );
        if ( !renamed )
        {
            throw new IOException( "Failed to rename log to: " + newName );
        }
    }

    private void deleteLogFile( String logFileName ) throws IOException
    {
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
        }
        fileChannel.close();
        fileChannel = null;
    }

    public synchronized void close() throws IOException
    {
        if ( fileChannel == null || !fileChannel.isOpen() )
        {
            log.fine( "Logical log: " + fileName + " already closed" );
            return;
        }
        long endPosition = writeBuffer.getFileChannelPosition();
        if ( xidIdentMap.size() > 0 )
        {
            log.info( "Close invoked with " + xidIdentMap.size() +
                " running transaction(s). " );
            writeBuffer.force();
            fileChannel.close();
            log.info( "Dirty log: " + fileName + "." + currentLog +
                " now closed. Recovery will be started automatically next " +
                "time it is opened." );
            return;
        }
        releaseCurrentLogFile();
        char logWas = currentLog;
        if ( currentLog != CLEAN ) // again special case, see above
        {
            setActiveLog( CLEAN );
        }
        if ( !keepLogs )
        {
            if ( logWas == CLEAN )
            {
                // special case going from old xa version with no log rotation
                // and we started with a recovery
                deleteLogFile( fileName );
            }
            else
            {
                deleteLogFile( fileName + "." + logWas );
            }
        }
        else
        {
            renameLogFileToRightVersion( fileName + "." + logWas, endPosition );
            xaTf.getAndSetNewVersion();
        }
        msgLog.logMessage( "Closed log " + fileName, true );
    }

    private long[] readAndAssertLogHeader( ByteBuffer localBuffer,
            ReadableByteChannel channel, long expectedVersion ) throws IOException
    {
        long[] header = LogIoUtils.readLogHeader( localBuffer, channel, true );
        if ( header[0] != expectedVersion )
        {
            throw new IOException( "Wrong version in log. Expected " + expectedVersion +
                    ", but got " + header[0] );
        }
        return header;
    }

    StringLogger getStringLogger()
    {
        return msgLog;
    }

    private void doInternalRecovery( String logFileName ) throws IOException
    {
        log.info( "Non clean shutdown detected on log [" + logFileName +
            "]. Recovery started ..." );
        msgLog.logMessage( "Non clean shutdown detected on log [" + logFileName +
            "]. Recovery started ...", true );
        // get log creation time
        long[] header = readLogHeader( fileChannel, "Tried to do recovery on log with illegal format version" );
        if ( header == null )
        {
            log.info( "Unable to read header information, "
                + "no records in logical log." );
            msgLog.logMessage( "No log version found for " + logFileName, true );
            fileChannel.close();
            boolean success = FileUtils.renameFile( new File( logFileName ),
                new File( logFileName + "_unknown_timestamp_" +
                    System.currentTimeMillis() + ".log" ) );
            assert success;
            fileChannel.close();
            fileChannel = new RandomAccessFile( logFileName,
                "rw" ).getChannel();
            return;
        }
        logVersion = header[0];
        long lastCommittedTx = header[1];
        previousLogLastCommittedTx = lastCommittedTx;
        logHeaderCache.put( logVersion, previousLogLastCommittedTx );
        log.fine( "Logical log version: " + logVersion + " with committed tx[" +
            lastCommittedTx + "]" );
        msgLog.logMessage( "[" + logFileName + "] logVersion=" + logVersion +
                " with committed tx=" + lastCommittedTx, true );
        long logEntriesFound = 0;
        long lastEntryPos = fileChannel.position();
        fileChannel = new BufferedFileChannel( fileChannel );
        LogEntry entry;
        while ( (entry = readEntry()) != null )
        {
            applyEntry( entry );
            logEntriesFound++;
            lastEntryPos = fileChannel.position();
        }
        // make sure we overwrite any broken records
        fileChannel = ((BufferedFileChannel)fileChannel).getSource();
        fileChannel.position( lastEntryPos );

        msgLog.logMessage( "[" + logFileName + "] entries found=" + logEntriesFound +
                " lastEntryPos=" + lastEntryPos, true  );

        // zero out the slow way since windows don't support truncate very well
        sharedBuffer.clear();
        while ( sharedBuffer.hasRemaining() )
        {
            sharedBuffer.put( (byte)0 );
        }
        sharedBuffer.flip();
        long endPosition = fileChannel.size();
        do
        {
            long bytesLeft = fileChannel.size() - fileChannel.position();
            if ( bytesLeft < sharedBuffer.capacity() )
            {
                sharedBuffer.limit( (int) bytesLeft );
            }
            fileChannel.write( sharedBuffer );
            sharedBuffer.flip();
        } while ( fileChannel.position() < endPosition );
        fileChannel.position( lastEntryPos );
        scanIsComplete = true;
        String recoveryCompletedMessage = "Internal recovery completed, scanned " + logEntriesFound
                + " log entries. Recovered " + recoveredTxCount
                + " transactions. Last tx recovered: " + lastRecoveredTx;
        log.fine( recoveryCompletedMessage );
        msgLog.logMessage( recoveryCompletedMessage );

        xaRm.checkXids();
        if ( xidIdentMap.size() == 0 )
        {
            log.fine( "Recovery completed." );
            msgLog.logMessage( "Recovery on log [" + logFileName + "] completed." );
        }
        else
        {
            log.fine( "[" + logFileName + "] Found " + xidIdentMap.size()
                + " prepared 2PC transactions." );
            msgLog.logMessage( "Recovery on log [" + logFileName +
                    "] completed with " + xidIdentMap + " prepared transactions found." );
            for ( LogEntry.Start startEntry : xidIdentMap.values() )
            {
                log.fine( "[" + logFileName + "] 2PC xid[" +
                    startEntry.getXid() + "]" );
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

    private LogEntry readEntry() throws IOException
    {
        long position = fileChannel.position();
        LogEntry entry = LogIoUtils.readEntry( sharedBuffer, fileChannel, cf );
        if ( entry instanceof LogEntry.Start )
        {
            ((LogEntry.Start) entry).setStartPosition( position );
        }
        return entry;
    }

    private final ArrayMap<Thread,Integer> txIdentMap =
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
        return getLogicalLog( version, 0 );
    }

    public ReadableByteChannel getLogicalLog( long version, long position ) throws IOException
    {
        String name = getFileName( version );
        if ( !new File( name ).exists() )
        {
            throw new IOException( "No such log version:" + version );
        }
        FileChannel channel = new RandomAccessFile( name, "r" ).getChannel();
        channel.position( position );
        return new BufferedFileChannel( channel );
    }

    private void extractPreparedTransactionFromLog( int identifier,
            FileChannel logChannel, LogBuffer targetBuffer ) throws IOException
    {
        LogEntry.Start startEntry = xidIdentMap.get( identifier );
        logChannel.position( startEntry.getStartPosition() );
        LogEntry entry;
        boolean found = false;
        while ( (entry = LogIoUtils.readEntry( sharedBuffer, logChannel, cf )) != null )
        {
            // TODO For now just skip Prepare entries
            if ( entry.getIdentifier() != identifier )
            {
                continue;
            }
            if ( entry instanceof LogEntry.Prepare )
            {
                break;
            }
            if ( entry instanceof LogEntry.Start || entry instanceof LogEntry.Command )
            {
                LogIoUtils.writeLogEntry( entry, targetBuffer );
                found = true;
            }
            else
            {
                throw new RuntimeException( "Expected start or command entry but found: " + entry );
            }
        }
        if ( !found )
        {
            throw new IOException( "Transaction for internal identifier[" + identifier +
                    "] not found in current log" );
        }
    }

//    private void assertLogCanContainTx( long txId, long prevTxId ) throws IOException
//    {
//        if ( prevTxId >= txId )
//        {
//            throw new IOException( "Log says " + txId +
//                    " can not exist in this log (prev tx id=" + prevTxId + ")" );
//        }
//    }

    public synchronized ReadableByteChannel getPreparedTransaction( int identifier )
            throws IOException
    {
        FileChannel logChannel = (FileChannel) getLogicalLogOrMyselfPrepared( logVersion, 0 );
        InMemoryLogBuffer localBuffer = new InMemoryLogBuffer();
        extractPreparedTransactionFromLog( identifier, logChannel, localBuffer );
        logChannel.close();
        return localBuffer;
    }

    public synchronized void getPreparedTransaction( int identifier, LogBuffer targetBuffer )
            throws IOException
    {
        FileChannel logChannel = (FileChannel) getLogicalLogOrMyselfPrepared( logVersion, 0 );
        extractPreparedTransactionFromLog( identifier, logChannel, targetBuffer );
        logChannel.close();
    }

    public class LogExtractor
    {
        /**
         * If tx range is smaller than this threshold ask the position cache for the
         * start position furthest back. Otherwise jump to right log and scan.
         */
        private static final int CACHE_FIND_THRESHOLD = 100;

        private final ByteBuffer localBuffer =
                ByteBuffer.allocate( 9 + Xid.MAXGTRIDSIZE + Xid.MAXBQUALSIZE * 10 );
        private ReadableByteChannel source;
        private final LogEntryCollector collector;
        private long version;
        private LogEntry.Commit lastCommitEntry;
        private LogEntry.Commit previousCommitEntry;
        private final long startTxId;
        private long nextExpectedTxId;
        private int counter;

        public LogExtractor( long startTxId, long endTxIdHint ) throws IOException
        {
            this.startTxId = startTxId;
            this.nextExpectedTxId = startTxId;
            long diff = endTxIdHint-startTxId + 1/*since they are inclusive*/;
            if ( diff < CACHE_FIND_THRESHOLD )
            {   // Find it from cache, we must check with all the requested transactions
                // because the first committed transaction doesn't necessarily have its
                // start record before the others.
                TxPosition earliestPosition = getEarliestStartPosition( startTxId, endTxIdHint );
                if ( earliestPosition != null )
                {
                    this.version = earliestPosition.version;
                    this.source = getLogicalLogOrMyselfCommitted( version, earliestPosition.position );
                }
            }

            if ( source == null )
            {   // Find the start position by jumping to the right log and scan linearly.
                // for consecutive transaction there's no scan needed, only the first one.
                this.version = findLogContainingTxId( startTxId )[0];
                this.source = getLogicalLogOrMyselfCommitted( version, 0 );
                // To get to the right position to start reading entries from
                readAndAssertLogHeader( localBuffer, source, version );
            }
            this.collector = new KnownTxIdCollector( startTxId );
        }

        private TxPosition getEarliestStartPosition( long startTxId, long endTxIdHint )
        {
            TxPosition earliest = null;
            for ( long txId = startTxId; txId <= endTxIdHint; txId++ )
            {
                TxPosition position = txStartPositionCache.get( txId );
                if ( position == null ) return null;
                if ( earliest == null || position.earlierThan( earliest ) )
                {
                    earliest = position;
                }
            }
            return earliest;
        }

        /**
         * @return the txId for the extracted tx. Or -1 if end-of-stream was reached.
         * @throws RuntimeException if there was something unexpected with the stream.
         */
        public long extractNext( LogBuffer target ) throws IOException
        {
            try
            {
                while ( this.version <= logVersion )
                {
                    long result = collectNextFromCurrentSource( target );
                    if ( result != -1 )
                    {
                        // TODO Should be assertions?
                        if ( previousCommitEntry != null && result == previousCommitEntry.getTxId() ) continue;
                        if ( result != nextExpectedTxId )
                        {
                            throw new RuntimeException( "Expected txId " + nextExpectedTxId + ", but got " + result + " (starting from " + startTxId + ")" + " " + counter + ", " + previousCommitEntry + ", " + lastCommitEntry );
                        }
                        nextExpectedTxId++;
                        counter++;
                        return result;
                    }

                    if ( this.version < logVersion )
                    {
                        continueInNextLog();
                    }
                    else break;
                }
                return -1;
            }
            catch ( Exception e )
            {
                // Something is wrong with the cached tx start position for this (expected) tx,
                // remove it from cache so that next request will have to bypass the cache
                logHeaderCache.clear();
                txStartPositionCache.clear();
                msgLog.logMessage( fileName + ", " + e.getMessage() + ". Clearing tx start position cache" );
                if ( e instanceof IOException ) throw (IOException) e;
                else throw Exceptions.launderedException( e );
            }
        }

        private void continueInNextLog() throws IOException
        {
            ensureSourceIsClosed();
            this.source = getLogicalLogOrMyselfCommitted( ++version, 0 );
            readAndAssertLogHeader( localBuffer, source, version ); // To get to the right position to start reading entries from
        }

        private long collectNextFromCurrentSource( LogBuffer target ) throws IOException
        {
            LogEntry entry = null;
            while ( collector.hasInFutureQueue() || // if something in queue then don't read next entry
                    (entry = LogIoUtils.readEntry( localBuffer, source, cf )) != null )
            {
                LogEntry foundEntry = collector.collect( entry, target );
                if ( foundEntry != null )
                {   // It just wrote the transaction, w/o the done record though. Add it
                    previousCommitEntry = lastCommitEntry;
                    LogIoUtils.writeLogEntry( new LogEntry.Done( collector.getIdentifier() ), target );
                    lastCommitEntry = (LogEntry.Commit)foundEntry;
                    return lastCommitEntry.getTxId();
                }
            }
            return -1;
        }

        public void close()
        {
            ensureSourceIsClosed();
        }

        @Override
        protected void finalize() throws Throwable
        {
            ensureSourceIsClosed();
        }

        private void ensureSourceIsClosed()
        {
            try
            {
                if ( source != null )
                {
                    source.close();
                    source = null;
                }
            }
            catch ( IOException e )
            { // OK?
                System.out.println( "Couldn't close logical after extracting transactions from it" );
                e.printStackTrace();
            }
        }

        public LogEntry.Commit getLastCommitEntry()
        {
            return lastCommitEntry;
        }

        public long getLastTxChecksum()
        {
            return getLastStartEntry().getTimeWritten();
        }

        public Start getLastStartEntry()
        {
            return collector.getLastStartEntry();
        }
    }

    public LogExtractor getLogExtractor( long startTxId, long endTxIdHint ) throws IOException
    {
        return new LogExtractor( startTxId, endTxIdHint );
    }

    public static final int MASTER_ID_REPRESENTING_NO_MASTER = -1;

    public synchronized Pair<Integer, Long> getMasterIdForCommittedTransaction( long txId ) throws IOException
    {
        if ( txId == 1 )
        {
            return Pair.of( MASTER_ID_REPRESENTING_NO_MASTER, 0L );
        }

        TxPosition cache = txStartPositionCache.get( txId );
        if ( cache != null )
        {
            return Pair.of( cache.masterId, cache.timeWritten );
        }

        LogExtractor extractor = getLogExtractor( txId, txId );
        try
        {
            if ( extractor.extractNext( NullLogBuffer.INSTANCE ) != -1 )
            {
                return Pair.of( extractor.getLastStartEntry().getMasterId(), extractor.getLastTxChecksum() );
            }
            throw new RuntimeException( "Unable to find commit entry for txId[" + txId + "]" );// in log[" + version + "]" );
        }
        finally
        {
            extractor.close();
        }
    }

    public ReadableByteChannel getLogicalLogOrMyselfCommitted( long version, long position )
            throws IOException
    {
        synchronized ( this )
        {
            if ( version == logVersion )
            {
                String currentLogName = getCurrentLogFileName();
                FileChannel channel = new RandomAccessFile( currentLogName, "r" ).getChannel();
                channel.position( position );
                return new BufferedFileChannel( channel );
            }
        }
        if ( version < logVersion )
        {
            return getLogicalLog( version, position );
        }
        else
        {
            throw new RuntimeException( "Version[" + version +
                "] is higher then current log version[" + logVersion + "]" );
        }
    }

    private ReadableByteChannel getLogicalLogOrMyselfPrepared( long version, long position )
        throws IOException
    {
        if ( version < logVersion )
        {
            return getLogicalLog( version, position );
        }
        else if ( version == logVersion )
        {
            String currentLogName = getCurrentLogFileName();
            FileChannel channel = new RandomAccessFile( currentLogName, "r" ).getChannel();
            channel = new BufferedFileChannel( channel );

            // Combined with the writeBuffer in cases where a DirectMappedLogBuffer
            // is used, on Windows or when memory mapping is turned off.
            // Otherwise the channel is returned directly.
            channel = logBufferFactory.combine( channel, writeBuffer );
            channel.position( position );
            return channel;
        }
        else
        {
            throw new RuntimeException( "Version[" + version +
                "] is higher then current log version[" + logVersion + "]" );
        }
    }

    private String getCurrentLogFileName()
    {
        return currentLog == LOG1 ? getLog1FileName() : getLog2FileName();
    }

    private long[] findLogContainingTxId( long txId ) throws IOException
    {
        long version = logVersion;
        long committedTx = previousLogLastCommittedTx;
        while ( version >= 0 )
        {
            Long cachedLastTx = logHeaderCache.get( version );
            if ( cachedLastTx != null )
            {
                committedTx = cachedLastTx;
            }
            else
            {
                ReadableByteChannel logChannel = getLogicalLogOrMyselfCommitted( version, 0 );
                try
                {
                    ByteBuffer buf = ByteBuffer.allocate( 16 );
                    long[] header = readAndAssertLogHeader( buf, logChannel, version );
                    committedTx = header[1];
                    logHeaderCache.put( version, committedTx );
                }
                finally
                {
                    logChannel.close();
                }
            }
            if ( committedTx < txId )
            {
                break;
            }
            version--;
        }
        if ( version == -1 )
        {
            throw new RuntimeException( "txId:" + txId + " not found in any logical log "
                                        + "(starting at " + logVersion
                                        + " and searching backwards" );
        }
        return new long[] { version, committedTx };
    }

    public long getLogicalLogLength( long version )
    {
        File file = new File( getFileName( version ) );
        return file.exists() ? file.length() : -1;
    }

    public boolean hasLogicalLog( long version )
    {
        return new File( getFileName( version ) ).exists();
    }

    public boolean deleteLogicalLog( long version )
    {
        File file = new File(getFileName( version ) );
        return file.exists() ? FileUtils.deleteFile( file ) : false;
    }

    protected LogDeserializer getLogDeserializer(ReadableByteChannel byteChannel)
    {
        return new LogDeserializer( byteChannel );
    }

    protected class LogDeserializer
    {
        private final ReadableByteChannel byteChannel;
        LogEntry.Start startEntry;
        LogEntry.Commit commitEntry;

        private final List<LogEntry> logEntries;

        protected LogDeserializer( ReadableByteChannel byteChannel )
        {
            this.byteChannel = byteChannel;
            this.logEntries = new LinkedList<LogEntry>();
        }

        public boolean readAndWriteAndApplyEntry( int newXidIdentifier )
                throws IOException
        {
            LogEntry entry = LogIoUtils.readEntry( sharedBuffer, byteChannel,
                    cf );
            if ( entry == null )
            {
                try
                {
                    intercept( logEntries );
                    apply();
                    return false;
                }
                catch ( Error e )
                {
                    startEntry = null;
                    commitEntry = null;
                    throw e;
                }
            }
            entry.setIdentifier( newXidIdentifier );
            logEntries.add( entry );
            if ( entry instanceof LogEntry.Commit )
            {
                assert startEntry != null;
                commitEntry = (LogEntry.Commit) entry;
            }
            else if ( entry instanceof LogEntry.Start )
            {
                startEntry = (LogEntry.Start) entry;
            }
            return true;
        }

        protected void intercept( List<LogEntry> logEntries )
        {
            // default do nothing
        }

        private void apply() throws IOException
        {
            for ( LogEntry entry : logEntries )
            {
                LogIoUtils.writeLogEntry( entry, writeBuffer );
                applyEntry( entry );
            }
        }

        protected Start getStartEntry()
        {
            return startEntry;
        }

        protected Commit getCommitEntry()
        {
            return commitEntry;
        }
    }

    private long[] readLogHeader( ReadableByteChannel source, String message ) throws IOException
    {
        try
        {
            return LogIoUtils.readLogHeader( sharedBuffer, source, true );
        }
        catch ( IllegalLogFormatException e )
        {
            msgLog.logMessage( message, e );
            throw e;
        }
    }

    public synchronized void applyTransactionWithoutTxId( ReadableByteChannel byteChannel,
            long nextTxId, int masterId ) throws IOException
    {
        if ( nextTxId != (xaTf.getLastCommittedTx() + 1) )
        {
            throw new IllegalStateException( "Tried to apply tx " +
                nextTxId + " but expected transaction " +
                (xaTf.getCurrentVersion() + 1) );
        }

        logRecoveryMessage( "applyTxWithoutTxId log version: " + logVersion +
                ", committing tx=" + nextTxId + ") @ pos " + writeBuffer.getFileChannelPosition() );

        long logEntriesFound = 0;
        scanIsComplete = false;
        LogDeserializer logApplier = getLogDeserializer( byteChannel );
        int xidIdent = getNextIdentifier();
        long startEntryPosition = writeBuffer.getFileChannelPosition();
        while ( logApplier.readAndWriteAndApplyEntry( xidIdent ) )
        {
            logEntriesFound++;
        }
        byteChannel.close();
        LogEntry.Start startEntry = logApplier.getStartEntry();
        if ( startEntry == null )
        {
            throw new IOException( "Unable to find start entry" );
        }
        startEntry.setStartPosition( startEntryPosition );
//        System.out.println( "applyTxWithoutTxId#before 1PC @ pos: " + writeBuffer.getFileChannelPosition() );
        LogEntry.OnePhaseCommit commit = new LogEntry.OnePhaseCommit(
                xidIdent, nextTxId, System.currentTimeMillis() );
        LogIoUtils.writeLogEntry( commit, writeBuffer );
        // need to manually force since xaRm.commit will not do it (transaction marked as recovered)
        writeBuffer.force();
        Xid xid = startEntry.getXid();
        try
        {
            XaTransaction xaTx = xaRm.getXaTransaction( xid );
            xaTx.setCommitTxId( nextTxId );
            xaRm.commit( xid, true );
            LogEntry doneEntry = new LogEntry.Done( startEntry.getIdentifier() );
            LogIoUtils.writeLogEntry( doneEntry, writeBuffer );
            xidIdentMap.remove( startEntry.getIdentifier() );
            recoveredTxMap.remove( startEntry.getIdentifier() );
            cacheTxStartPosition( nextTxId, masterId, startEntry );
        }
        catch ( XAException e )
        {
            throw new IOException( e );
        }

//        LogEntry.Done done = new LogEntry.Done( entry.getIdentifier() );
//        LogIoUtils.writeLogEntry( done, writeBuffer );
        // xaTf.setLastCommittedTx( nextTxId ); // done in doCommit
        scanIsComplete = true;
//        log.info( "Tx[" + nextTxId + "] " + " applied successfully." );
        logRecoveryMessage( "Applied external tx and generated tx id=" + nextTxId );

        checkLogRotation();
//        System.out.println( "applyTxWithoutTxId#end @ pos: " + writeBuffer.getFileChannelPosition() );
    }

    public synchronized void applyTransaction( ReadableByteChannel byteChannel )
        throws IOException
    {
//        System.out.println( "applyFullTx#start @ pos: " + writeBuffer.getFileChannelPosition() );
        long logEntriesFound = 0;
        scanIsComplete = false;
        LogDeserializer logApplier = getLogDeserializer( byteChannel );
        int xidIdent = getNextIdentifier();
        long startEntryPosition = writeBuffer.getFileChannelPosition();
        boolean successfullyApplied = false;
        try
        {
            while ( logApplier.readAndWriteAndApplyEntry( xidIdent ) )
            {
                logEntriesFound++;
            }
            successfullyApplied = true;
        }
        finally
        {
            if ( !successfullyApplied && logApplier.getStartEntry() != null )
            {   // Unmap this identifier if tx not applied correctly
                xidIdentMap.remove( xidIdent );
                try
                {
                    xaRm.forget( logApplier.getStartEntry().getXid() );
                }
                catch ( XAException e )
                {
                    throw new IOException( e );
                }
            }
        }
        byteChannel.close();
        scanIsComplete = true;
        LogEntry.Start startEntry = logApplier.getStartEntry();
        if ( startEntry == null )
        {
            throw new IOException( "Unable to find start entry" );
        }
        startEntry.setStartPosition( startEntryPosition );
        cacheTxStartPosition( logApplier.getCommitEntry().getTxId(),
                startEntry.getMasterId(), startEntry );
//        System.out.println( "applyFullTx#end @ pos: " + writeBuffer.getFileChannelPosition() );
        checkLogRotation();
    }

    private String getLog1FileName()
    {
        return fileName + ".1";
    }

    private String getLog2FileName()
    {
        return fileName + ".2";
    }

    /**
     * @return the last tx in the produced log
     * @throws IOException I/O error.
     */
    public synchronized long rotate() throws IOException
    {
//        if ( writeBuffer.getFileChannelPosition() == LogIoUtils.LOG_HEADER_SIZE ) return xaTf.getLastCommittedTx();
        xaTf.flushAll();
        String newLogFile = getLog2FileName();
        String currentLogFile = getLog1FileName();
        char newActiveLog = LOG2;
        long currentVersion = xaTf.getCurrentVersion();
        String oldCopy = getFileName( currentVersion );
        if ( currentLog == CLEAN || currentLog == LOG2 )
        {
            newActiveLog = LOG1;
            newLogFile = getLog1FileName();
            currentLogFile = getLog2FileName();
        }
        else
        {
            assert currentLog == LOG1;
        }
        assertFileDoesntExist( newLogFile, "New log file" );
        assertFileDoesntExist( oldCopy, "Copy log file" );
//        System.out.println( " ---- Performing rotate on " + currentLogFile + " -----" );
//        DumpLogicalLog.main( new String[] { currentLogFile } );
//        System.out.println( " ----- end ----" );
        msgLog.logMessage( "Rotating [" + currentLogFile + "] @ version=" +
                currentVersion + " to " +  newLogFile + " from position " +
                writeBuffer.getFileChannelPosition(), true );
        long endPosition = writeBuffer.getFileChannelPosition();
        writeBuffer.force();
        FileChannel newLog = new RandomAccessFile(
            newLogFile, "rw" ).getChannel();
        long lastTx = xaTf.getLastCommittedTx();
        LogIoUtils.writeLogHeader( sharedBuffer, (currentVersion + 1), lastTx );
        previousLogLastCommittedTx = lastTx;
        if ( newLog.write( sharedBuffer ) != 16 )
        {
            throw new IOException( "Unable to write log version to new" );
        }
        long pos = fileChannel.position();
        fileChannel.position( 0 );
        readAndAssertLogHeader( sharedBuffer, fileChannel, currentVersion );
        fileChannel.position( pos );
        if ( xidIdentMap.size() > 0 )
        {
            long firstEntryPosition = getFirstStartEntry( endPosition );
            fileChannel.position( firstEntryPosition );
            msgLog.logMessage( "Rotate log first start entry @ pos=" +
                    firstEntryPosition );
        }
        LogEntry entry;
        // Set<Integer> startEntriesWritten = new HashSet<Integer>();
        LogBuffer newLogBuffer = instantiateCorrectWriteBuffer( newLog );
        while ((entry = LogIoUtils.readEntry( sharedBuffer, fileChannel, cf )) != null )
        {
            if ( xidIdentMap.get( entry.getIdentifier() ) != null )
            {
                if ( entry instanceof LogEntry.Start )
                {
                    LogEntry.Start startEntry = (LogEntry.Start) entry;
                    startEntry.setStartPosition( newLogBuffer.getFileChannelPosition() ); // newLog.position() );
                    // overwrite old start entry with new that has updated position
                    xidIdentMap.put( startEntry.getIdentifier(), startEntry );
                    // startEntriesWritten.add( entry.getIdentifier() );
                }
                else if ( entry instanceof LogEntry.Commit )
                {
                    LogEntry.Start startEntry = xidIdentMap.get( entry.getIdentifier() );
                    LogEntry.Commit commitEntry = (LogEntry.Commit) entry;
                    TxPosition oldPos = txStartPositionCache.get( commitEntry.getTxId() );
                    TxPosition newPos = cacheTxStartPosition( commitEntry.getTxId(), startEntry.getMasterId(), startEntry, logVersion+1 );
                    msgLog.logMessage( "Updated tx " + ((LogEntry.Commit) entry ).getTxId() +
                            " from " + oldPos + " to " + newPos );
                }
//                if ( !startEntriesWritten.contains( entry.getIdentifier() ) )
//                {
//                    throw new IOException( "Unable to rotate log since start entry for identifier[" +
//                            entry.getIdentifier() + "] not written" );
//                }
                LogIoUtils.writeLogEntry( entry, newLogBuffer );
            }
        }
        newLogBuffer.force();
        newLog.position( newLogBuffer.getFileChannelPosition() );
        msgLog.logMessage( "Rotate: old log scanned, newLog @ pos=" +
                newLog.position(), true );
        newLog.force( false );
        releaseCurrentLogFile();
        setActiveLog( newActiveLog );
        if ( keepLogs )
        {
            renameLogFileToRightVersion( currentLogFile, endPosition );
        }
        else
        {
            deleteLogFile( currentLogFile );
        }
        xaTf.getAndSetNewVersion();
        this.logVersion = xaTf.getCurrentVersion();
        if ( xaTf.getCurrentVersion() != ( currentVersion + 1 ) )
        {
            throw new IOException( "version change failed" );
        }
        fileChannel = newLog;
        logHeaderCache.put( logVersion, lastTx );
        instantiateCorrectWriteBuffer();
        msgLog.logMessage( "Log rotated, newLog @ pos=" +
                writeBuffer.getFileChannelPosition() + " and version " + logVersion, true );
        return lastTx;
    }

    private void assertFileDoesntExist( String file, String description ) throws IOException
    {
        if ( new File( file ).exists() )
        {
            throw new IOException( description + ": " + file + " already exist" );
        }
    }

    private long getFirstStartEntry( long endPosition )
    {
        long firstEntryPosition = endPosition;
        for ( LogEntry.Start entry : xidIdentMap.values() )
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

    /*
     * Only call this is there's an explicit property set to control it.
     * Other wise depend on the default behaviour.
     */
    public void setKeepLogs( boolean keep )
    {
        this.keepLogs = keep;
    }

    private boolean hasPreviousLogs()
    {
        File fileNameFile = new File( fileName );
        File logDirectory = fileNameFile.getParentFile();
        if ( !logDirectory.exists() ) return false;
        Pattern logFilePattern = getHistoryFileNamePattern();
        for ( File file : logDirectory.listFiles() )
        {
            if ( logFilePattern.matcher( file.getName() ).find() ) return true;
        }
        return false;
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

    public String getFileName( long version )
    {
        return fileName + ".v" + version;
    }

    public String getBaseFileName()
    {
        return fileName;
    }

    public Pattern getHistoryFileNamePattern()
    {
        return getHistoryFileNamePattern( new File( fileName ).getName() );
    }

    public static Pattern getHistoryFileNamePattern( String baseFileName )
    {
        return Pattern.compile( baseFileName + "\\.v\\d+" );
    }

    public static long getHistoryLogVersion( File historyLogFile )
    {   // Get version based on the name
        String name = historyLogFile.getName();
        String toFind = ".v";
        int index = name.lastIndexOf( toFind );
        if ( index == -1 ) throw new RuntimeException( "Invalid log file '" + historyLogFile + "'" );
        return Integer.parseInt( name.substring( index + toFind.length() ) );
    }

    public boolean wasNonClean()
    {
        return nonCleanShutdown;
    }

    private static class TxPosition
    {
        final long version;
        final int masterId;
        final int identifier;
        final long position;
        final long timeWritten;

        private TxPosition( long version, int masterId, int identifier, long position, long timeWritten )
        {
            this.version = version;
            this.masterId = masterId;
            this.identifier = identifier;
            this.position = position;
            this.timeWritten = timeWritten;
        }

        public boolean earlierThan( TxPosition other )
        {
            if ( version < other.version ) return true;
            if ( version > other.version ) return false;
            return position < other.position;
        }

        @Override
        public String toString()
        {
            return "TxPosition[version:" + version + ", pos:" + position + "]";
        }
    }

    private static interface LogEntryCollector
    {
        LogEntry collect( LogEntry entry, LogBuffer target ) throws IOException;

        LogEntry.Start getLastStartEntry();

        boolean hasInFutureQueue();

        int getIdentifier();
    }

    private static class KnownIdentifierCollector implements LogEntryCollector
    {
        private final int identifier;
        private LogEntry.Start startEntry;

        KnownIdentifierCollector( int identifier )
        {
            this.identifier = identifier;
        }

        public int getIdentifier()
        {
            return identifier;
        }

        public LogEntry collect( LogEntry entry, LogBuffer target ) throws IOException
        {
            if ( entry.getIdentifier() == identifier )
            {
                if ( entry instanceof LogEntry.Start )
                {
                    startEntry = (Start) entry;
                }
                if ( target != null )
                {
                    LogIoUtils.writeLogEntry( entry, target );
                }
                return entry;
            }
            return null;
        }

        @Override
        public boolean hasInFutureQueue()
        {
            return false;
        }

        @Override
        public LogEntry.Start getLastStartEntry()
        {
            return startEntry;
        }
    }

    private static class KnownTxIdCollector implements LogEntryCollector
    {
        private final Map<Integer,List<LogEntry>> transactions = new HashMap<Integer,List<LogEntry>>();
        private final long startTxId;
        private int identifier;
        private final Map<Long, List<LogEntry>> futureQueue = new HashMap<Long, List<LogEntry>>();
        private long nextExpectedTxId;
        private LogEntry.Start lastStartEntry;

        KnownTxIdCollector( long startTxId )
        {
            this.startTxId = startTxId;
            this.nextExpectedTxId = startTxId;
        }

        public int getIdentifier()
        {
            return identifier;
        }

        @Override
        public boolean hasInFutureQueue()
        {
            return futureQueue.containsKey( nextExpectedTxId );
        }

        @Override
        public LogEntry.Start getLastStartEntry()
        {
            return lastStartEntry;
        }

        public LogEntry collect( LogEntry entry, LogBuffer target ) throws IOException
        {
            if ( futureQueue.containsKey( nextExpectedTxId ) )
            {
                List<LogEntry> list = futureQueue.remove( nextExpectedTxId++ );
                lastStartEntry = (LogEntry.Start)list.get( 0 );
                writeToBuffer( list, target );
                return commitEntryOf( list );
            }

            if ( entry instanceof LogEntry.Start )
            {
                List<LogEntry> list = new LinkedList<LogEntry>();
                list.add( entry );
                transactions.put( entry.getIdentifier(), list );
            }
            else if ( entry instanceof LogEntry.Commit )
            {
                long commitTxId = ((LogEntry.Commit) entry).getTxId();
                if ( commitTxId < startTxId ) return null;
                identifier = entry.getIdentifier();
                List<LogEntry> entries = transactions.get( identifier );
                if ( entries == null ) return null;
                entries.add( entry );
                if ( nextExpectedTxId != startTxId )
                {   // Have returned some previous tx
                    // If we encounter an already extracted tx in the middle of the stream
                    // then just ignore it. This can happen when we do log rotation,
                    // where records are copied over from the active log to the new.
                    if ( commitTxId < nextExpectedTxId ) return null;
                }

                if ( commitTxId != nextExpectedTxId )
                {   // There seems to be a hole in the tx stream, or out-of-ordering
                    futureQueue.put( commitTxId, entries );
                    return null;
                }

                writeToBuffer( entries, target );
                nextExpectedTxId = commitTxId+1;
                lastStartEntry = (LogEntry.Start)entries.get( 0 );
                return entry;
            }
            else if ( entry instanceof LogEntry.Command || entry instanceof LogEntry.Prepare )
            {
                List<LogEntry> list = transactions.get( entry.getIdentifier() );

                // Since we can start reading at any position in the log it might be the case
                // that we come across a record which corresponding start record resides
                // before the position we started reading from. If that should be the case
                // then skip it since it isn't an important record for us here.
                if ( list != null )
                {
                    list.add( entry );
                }
            }
            else if ( entry instanceof LogEntry.Done )
            {
                transactions.remove( entry.getIdentifier() );
            }
            else
            {
                throw new RuntimeException( "Unknown entry: " + entry );
            }
            return null;
        }

        private LogEntry commitEntryOf( List<LogEntry> list )
        {
            for ( LogEntry entry : list )
            {
                if ( entry instanceof LogEntry.Commit ) return entry;
            }
            throw new RuntimeException( "No commit entry in " + list );
        }

        private void writeToBuffer( List<LogEntry> entries, LogBuffer target ) throws IOException
        {
            if ( target != null )
            {
                for ( LogEntry entry : entries )
                {
                    LogIoUtils.writeLogEntry( entry, target );
                }
            }
        }
    }

    public long getCurrentLogVersion()
    {
        return logVersion;
    }
}