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
package org.neo4j.kernel.impl.transaction.xaframework;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.StoreChannel;
import org.neo4j.kernel.impl.nioneo.store.TransactionIdStore;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.kernel.impl.util.StringLogger;

import static org.neo4j.kernel.impl.transaction.xaframework.VersionAwareLogEntryReader.readLogHeader;
import static org.neo4j.kernel.impl.transaction.xaframework.VersionAwareLogEntryReader.writeLogHeader;

/**
 * {@link LogFile} backup by one or more files in a {@link FileSystemAbstraction}.
 */
public class PhysicalLogFile implements LogFile,
        LogVersionRepository,       /* as an intermediary step, since LogPruneStrategies uses it, although log pruning should be
                            implemented inside here or something*/
        LogVersionBridge
{
    public static final String DEFAULT_NAME = "nioneo_logical.log";
    private static final char LOG1 = '1';
    private static final char LOG2 = '2';
    private static final char CLEAN = 'C';

    private char currentLog = CLEAN;
    private final long rotateAtSize;
    private final FileSystemAbstraction fileSystem;
    private final File directory;
    private final File fileName;
    private final LogPruneStrategy pruneStrategy;
    private final TransactionIdStore transactionIdStore;
    private final PhysicalLogFiles logFiles;
    private long currentLogVersion;
    private final LogPositionCache positionCache;
    private final Monitor monitor;
    private final ByteBuffer headerBuffer = ByteBuffer.allocate( 16 );
    private final LogRotationControl logRotationControl;
    private WritableLogChannel writer;

    public PhysicalLogFile( FileSystemAbstraction fileSystem, File directory, String name, long rotateAtSize,
            LogPruneStrategy pruneStrategy, TransactionIdStore transactionIdStore, Monitor monitor,
            LogRotationControl logRotationControl, LogPositionCache positionCache )
    {
        this.fileSystem = fileSystem;
        this.directory = directory;
        this.rotateAtSize = rotateAtSize;
        this.pruneStrategy = pruneStrategy;
        this.transactionIdStore = transactionIdStore;
        this.monitor = monitor;
        this.logRotationControl = logRotationControl;
        this.positionCache = positionCache;
        this.fileName = new File( directory, name );
        this.logFiles = new PhysicalLogFiles( directory, name, fileSystem );
    }

    @Override
    public void open( Visitor<ReadableLogChannel, IOException> recoveredDataVisitor ) throws IOException
    {
        PhysicalLogVersionedStoreChannel channel = null;
        switch ( logFiles.determineState() )
        {
        case NO_ACTIVE_FILE:
            channel = open( logFiles.getLog1FileName(), recoveredDataVisitor );
            setActiveLog( LOG1 );
            break;

        case CLEAN:
            File newLog = logFiles.getLog1FileName();
            renameIfExists( newLog );
            renameIfExists( logFiles.getLog2FileName() );
            channel = open( newLog, recoveredDataVisitor );
            setActiveLog( LOG1 );
            break;

        case DUAL_LOGS_LOG_1_ACTIVE:
            fixDualLogFiles( logFiles.getLog1FileName(), logFiles.getLog2FileName() );
        case LOG_1_ACTIVE:
            currentLog = LOG1;
            channel = open( logFiles.getLog1FileName(), recoveredDataVisitor );
            break;

        case DUAL_LOGS_LOG_2_ACTIVE:
            fixDualLogFiles( logFiles.getLog2FileName(), logFiles.getLog1FileName() );
        case LOG_2_ACTIVE:
            currentLog = LOG2;
            channel = open( logFiles.getLog2FileName(), recoveredDataVisitor );
            break;

        default:
            throw new IllegalStateException( "FATAL: Unrecognized logical log state." );
        }

        writer = new PhysicalWritableLogChannel( channel, new LogVersionBridge()
        {
            @Override
            public VersionedStoreChannel next( VersionedStoreChannel channel ) throws IOException
            {
                // Whereas channel.size() should be fine, we're safer calling position() due to possibility
                // of this file being memory mapped or whatever.
                if ( channel.position() >= rotateAtSize )
                {
                    return rotate( channel );
                }
                return channel;
            }
        } );
    }

    private PhysicalLogVersionedStoreChannel open( File fileToOpen, Visitor<ReadableLogChannel, IOException> recoveredDataVisitor )
            throws IOException
    {
        PhysicalLogVersionedStoreChannel channel = openFileChannel( fileToOpen, "rw" );
        if ( new XaLogicalLogRecoveryCheck( channel ).recoveryRequired() )
        {   // There are already data in here, which means recovery will need to be performed.
            long[] header = readLogHeader( headerBuffer, channel, true );
            transactionIdStore.setCurrentLogVersion( currentLogVersion = header[0] );

            ReadableLogChannel recoveredDataChannel = new ReadAheadLogChannel( channel, NO_MORE_CHANNELS,
                    ReadAheadLogChannel.DEFAULT_READ_AHEAD_SIZE );
            recoveredDataVisitor.visit( recoveredDataChannel );
            // intentionally keep it open since we're continuing using the underlying channel for the writer below

            logRotationControl.forceEverything();
            // TODO rotate after recovery?
        }
        else
        {   // This is a new log file, write a header in it
            currentLogVersion = transactionIdStore.getCurrentLogVersion();
            long version = logFiles.determineNextLogVersion(/*default=*/currentLogVersion );
            if ( version != currentLogVersion )
            {
                transactionIdStore.setCurrentLogVersion( currentLogVersion = version );
            }

            long lastTxId = transactionIdStore.getLastCommittingTransactionId();
            writeLogHeader( headerBuffer, currentLogVersion, lastTxId );
            positionCache.putHeader( currentLogVersion, lastTxId );
            channel.writeAll( headerBuffer );
            monitor.opened( fileToOpen, currentLogVersion, lastTxId, true );
        }
        channel.setVersion( currentLogVersion );
        return channel;
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
        StoreChannel fc = fileSystem.open( new File( fileName.getPath() + ".active"), "rw" );
        int wrote = fc.write( bb );
        if ( wrote != 4 )
        {
            throw new IllegalStateException( "Expected to write 4 -> " + wrote );
        }
        fc.force( false );
        fc.close();
        currentLog = c;
    }

    private void renameIfExists( File fileName ) throws IOException
    {
        if ( fileSystem.fileExists( fileName ) )
        {
            renameLogFileToRightVersion( fileName, fileSystem.getFileSize( fileName ) );
            // TODO don't do anything with the returned value?
            transactionIdStore.nextLogVersion();
        }
    }

    private void renameLogFileToRightVersion( File logFileName, long endPosition ) throws IOException
    {
        if ( !fileSystem.fileExists( logFileName ) )
        {
            throw new IOException( "Logical log[" + logFileName + "] not found" );
        }

        StoreChannel channel = fileSystem.open( logFileName, "rw" );
        long[] header = VersionAwareLogEntryReader.readLogHeader( ByteBuffer.allocate( 16 ), channel, false );
        try
        {
            FileUtils.truncateFile( channel, endPosition );
        }
        catch ( IOException e )
        {
            monitor.failureToTruncate( logFileName, e );
        }
        channel.close();
        File newName;
        newName = logFiles.getHistoryFileName( header[0] );
        if ( !fileSystem.renameFile( logFileName, newName ) )
        {
            throw new IOException( "Failed to rename log to: " + newName );
        }
    }

    private void fixDualLogFiles( File activeLog, File oldLog ) throws IOException
    {
        StoreChannel activeLogChannel = fileSystem.open( activeLog, "r" );
        long[] activeLogHeader = VersionAwareLogEntryReader.readLogHeader( ByteBuffer.allocate( 16 ),
                activeLogChannel, false );
        activeLogChannel.close();

        StoreChannel oldLogChannel = fileSystem.open( oldLog, "r" );
        long[] oldLogHeader = VersionAwareLogEntryReader.readLogHeader( ByteBuffer.allocate( 16 ), oldLogChannel, false );
        oldLogChannel.close();

        if ( oldLogHeader == null )
        {
            if ( !fileSystem.deleteFile( oldLog ) )
            {
                throw new IOException( "Unable to delete " + oldLog );
            }
        }
        else if ( activeLogHeader == null || activeLogHeader[0] > oldLogHeader[0] )
        {
            // we crashed in rotate after setActive but did not move the old log to the right name
            // (and we do not know if keepLogs is true or not so play it safe by keeping it)
            File newName = logFiles.getHistoryFileName( oldLogHeader[0] );
            if ( !fileSystem.renameFile( oldLog, newName ) )
            {
                throw new IOException( "Unable to rename " + oldLog + " to " + newName );
            }
        }
        else
        {
            assert activeLogHeader[0] < oldLogHeader[0];
            // we crashed in rotate before setActive, do the rotate work again and delete old
            if ( !fileSystem.deleteFile( oldLog ) )
            {
                throw new IOException( "Unable to delete " + oldLog );
            }
        }
    }

    // TODO 2.2-future please, for the name of $DEITY, at the very least revisit the version variable names
    // TODO 2.2-future ideally, we should reorder stuff so less variables are needed
    private synchronized VersionedStoreChannel rotate( VersionedStoreChannel channel ) throws IOException
    {
        logRotationControl.awaitAllTransactionsClosed();
        logRotationControl.forceEverything();

        File newLogFile = logFiles.getLog2FileName();
        File currentLogFile = logFiles.getLog1FileName();
        char newActiveLog = LOG2;
        long previousLogVersion = transactionIdStore.getCurrentLogVersion();

        File oldCopy = logFiles.getHistoryFileName( previousLogVersion );
        if ( currentLog == CLEAN || currentLog == LOG2 )
        {
            newActiveLog = LOG1;
            newLogFile = logFiles.getLog1FileName();
            currentLogFile = logFiles.getLog2FileName();
        }
        else
        {
            assert currentLog == LOG1;
        }
        assertFileDoesntExist( newLogFile, "New log file" );
        assertFileDoesntExist( oldCopy, "Copy log file" );
        PhysicalLogVersionedStoreChannel newLog = openFileChannel( newLogFile, "rw" );
        long lastTx = transactionIdStore.getLastCommittingTransactionId();
        VersionAwareLogEntryReader.writeLogHeader( headerBuffer, previousLogVersion + 1, lastTx );
        if ( newLog.write( headerBuffer ) != 16 )
        {
            throw new IOException( "Unable to write log version to new" );
        }

        long endPosition = channel.position();
        channel.close();
        setActiveLog( newActiveLog );

        renameLogFileToRightVersion( currentLogFile, endPosition );
        currentLogVersion = transactionIdStore.nextLogVersion();
        if ( currentLogVersion != (previousLogVersion + 1) )
        {
            throw new IOException( "Version change failed, expected " + (previousLogVersion + 1) + ", but was " +
                    currentLogVersion );
        }

        newLog.setVersion( currentLogVersion );

        pruneStrategy.prune( this );
        positionCache.putHeader( currentLogVersion, lastTx );
        return newLog;
    }

    private void assertFileDoesntExist( File file, String description ) throws IOException
    {
        if ( fileSystem.fileExists( file ) )
        {
            throw new IOException( description + ": " + file + " already exist" );
        }
    }

//    @Override
//    public Long getFirstCommittedTxId( long version )
//    {
//        if ( version == 0 )
//        {
//            return 1L;
//        }
//
//        // First committed tx for version V = last committed tx version V-1 + 1
//        Long header = positionCache.getHeader( version - 1 );
//        if ( header != null )
//        // It existed in cache
//        {
//            return header + 1;
//        }
//
//        // Wasn't cached, go look for it
//        synchronized ( this )
//        {
//            if ( version > logVersion )
//            {
//                throw new IllegalArgumentException( "Too high version " + version + ", active is " + logVersion );
//            }
//            else if ( version == logVersion )
//            {
//                throw new IllegalArgumentException( "Last committed tx for the active log isn't determined yet" );
//            }
//            else if ( version == logVersion - 1 )
//            {
//                return previousLogLastCommittedTx;
//            }
//            else
//            {
//                File file = getFileName( version );
//                if ( fileSystem.fileExists( file ) )
//                {
//                    try
//                    {
//                        long[] headerLongs = VersionAwareLogEntryReader.readLogHeader( fileSystem, file );
//                        return headerLongs[1] + 1;
//                    }
//                    catch ( IOException e )
//                    {
//                        throw new RuntimeException( e );
//                    }
//                }
//            }
//        }
//        return null;
//    }
//
//
//    @Override
//    public Long getFirstStartRecordTimestamp( long version ) throws IOException
//    {
//        ReadableByteChannel log = null;
//        try
//        {
//            ByteBuffer buffer = LogExtractor.newLogReaderBuffer();
//            log = getLogicalLog( version );
//            VersionAwareLogEntryReader.readLogHeader( buffer, log, true );
//            LogDeserializer deserializer = new LogDeserializer( buffer, commandReaderFactory );
//
//            TimeWrittenConsumer consumer = new TimeWrittenConsumer();
//
//            try ( Cursor<LogEntry, IOException> cursor = deserializer.cursor( log ) )
//            {
//                while( cursor.next( consumer ) )
//                {
//                    ;
//                }
//            }
//            return consumer.getTimeWritten();
//        }
//        finally
//        {
//            if ( log != null )
//            {
//                log.close();
//            }
//        }
//    }
//

    @Override
    public WritableLogChannel getWriter()
    {
        return writer;
    }

    @Override
    public ReadableLogChannel getReader( LogPosition position ) throws IOException
    {
        VersionedStoreChannel channel = openLogChannel( position );
        return new ReadAheadLogChannel( channel, this, 4*1024 );
    }

    @Override
    public VersionedStoreChannel next( VersionedStoreChannel channel ) throws IOException
    {
        PhysicalLogVersionedStoreChannel nextChannel;
        try
        {
            nextChannel = openLogChannel( new LogPosition( channel.getVersion()+1, 0 ) );
        }
        catch ( FileNotFoundException e )
        {
            return channel;
        }

        // TODO read header properly
        channel.close();
        nextChannel.position( VersionAwareLogEntryReader.LOG_HEADER_SIZE );
        return nextChannel;
    }

    private PhysicalLogVersionedStoreChannel openLogChannel( LogPosition position ) throws IOException
    {
        long version = position.getLogVersion();
        File fileToOpen;
        if ( version == currentLogVersion )
        {   // This is the current one
            fileToOpen = getCurrentActiveLogFileName();
        }
        else
        {   // This is a historical one
            fileToOpen = logFiles.getHistoryFileName( version );
        }

        PhysicalLogVersionedStoreChannel channel = openFileChannel( fileToOpen, "r", version );
        channel.position( position.getByteOffset() );
        return channel;
    }

    private PhysicalLogVersionedStoreChannel openFileChannel( File file, String mode ) throws IOException
    {
        return new PhysicalLogVersionedStoreChannel( fileSystem.open( file, mode ) );
    }

    private PhysicalLogVersionedStoreChannel openFileChannel( File file, String mode, long version )
            throws IOException
    {
        return new PhysicalLogVersionedStoreChannel( fileSystem.open( file, mode ), version );
    }

    private File getCurrentActiveLogFileName()
    {
        switch ( currentLog )
        {
        case LOG1:
            return logFiles.getLog1FileName();
        case LOG2:
            return logFiles.getLog2FileName();
        default:
            throw new IllegalStateException( "Invalid log " + currentLog );
        }
    }

    @Override
    public LogPosition findRoughPositionOf( long transactionId ) throws NoSuchTransactionException
    {
        throw new UnsupportedOperationException( "Please implement" );
    }

    // -- Ohoy traveller, LogVersion repo stuff ahead (temporary I hope)

    @Override
    public long getHighestLogVersion()
    {
        throw new UnsupportedOperationException( "Please implement" );
    }

    @Override
    public File getFileName( long version )
    {
        throw new UnsupportedOperationException( "Please implement" );
    }

    @Override
    public Long getFirstCommittedTxId( long version )
    {
        throw new UnsupportedOperationException( "Please implement" );
    }

    @Override
    public long getLastCommittedTxId()
    {
        throw new UnsupportedOperationException( "Please implement" );
    }

    @Override
    public Long getFirstStartRecordTimestamp( long version ) throws IOException
    {
        throw new UnsupportedOperationException( "Please implement" );
    }

    public interface Monitor
    {
        void opened( File logFile, long logVersion, long lastTransactionId, boolean clean );

        void failureToTruncate( File logFile, IOException e );
    }

    public static class LoggingMonitor implements Monitor
    {
        private final StringLogger logger;

        public LoggingMonitor( StringLogger logger )
        {
            this.logger = logger;
        }

        @Override
        public void opened( File logFile, long logVersion, long lastTransactionId, boolean clean )
        {
            logger.info( "Opened logical log [" + logFile + "] version=" + logVersion + ", lastTxId=" +
                    lastTransactionId + " (" + (clean ? "clean" : "recovered") + ")" );
        }

        @Override
        public void failureToTruncate( File logFile, IOException e )
        {
            logger.warn( "Failed to truncate " + logFile + " at correct size", e );
        }
    }

    @Override
    public synchronized void close() throws IOException
    {
        logRotationControl.awaitAllTransactionsClosed();
        logRotationControl.forceEverything();

        LogPosition endPosition = writer.getCurrentPosition();
        writer.close();
        File currentLogFile = currentLog == LOG1 ? logFiles.getLog1FileName() : logFiles.getLog2FileName();
        if ( currentLog != CLEAN )
        {
            setActiveLog( CLEAN );
        }

        renameLogFileToRightVersion( currentLogFile, endPosition.getByteOffset() );
        transactionIdStore.nextLogVersion();

        pruneStrategy.prune( this );
    }
}
