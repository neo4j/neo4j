/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import javax.transaction.xa.XAException;
import javax.transaction.xa.Xid;

import org.neo4j.helpers.Exceptions;
import org.neo4j.helpers.Function;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.StoreChannel;
import org.neo4j.kernel.impl.nioneo.xa.LogDeserializer;
import org.neo4j.kernel.impl.nioneo.xa.RecoveryLogDeserializer;
import org.neo4j.kernel.impl.nioneo.xa.SlaveLogDeserializer;
import org.neo4j.kernel.impl.nioneo.xa.XaCommandReaderFactory;
import org.neo4j.kernel.impl.nioneo.xa.XaCommandWriterFactory;
import org.neo4j.kernel.impl.nioneo.xa.command.LogFilter;
import org.neo4j.kernel.impl.nioneo.xa.command.LogHandler;
import org.neo4j.kernel.impl.nioneo.xa.command.LogReader;
import org.neo4j.kernel.impl.nioneo.xa.command.LogWriter;
import org.neo4j.kernel.impl.nioneo.xa.command.MasterLogWriter;
import org.neo4j.kernel.impl.nioneo.xa.command.PositionCacheLogHandler;
import org.neo4j.kernel.impl.nioneo.xa.command.SlaveLogWriter;
import org.neo4j.kernel.impl.transaction.KernelHealth;
import org.neo4j.kernel.impl.transaction.TransactionStateFactory;
import org.neo4j.kernel.impl.transaction.xaframework.LogEntry.Start;
import org.neo4j.kernel.impl.transaction.xaframework.LogExtractor.LogLoader;
import org.neo4j.kernel.impl.transaction.xaframework.LogExtractor.LogPositionCache;
import org.neo4j.kernel.impl.transaction.xaframework.LogExtractor.TxPosition;
import org.neo4j.kernel.impl.util.ArrayMap;
import org.neo4j.kernel.impl.util.BufferedFileChannel;
import org.neo4j.kernel.impl.util.Consumer;
import org.neo4j.kernel.impl.util.Cursor;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.kernel.monitoring.ByteCounterMonitor;
import org.neo4j.kernel.monitoring.Monitors;

import static java.lang.Math.max;
import static org.neo4j.helpers.Exceptions.launderedException;
import static org.neo4j.kernel.impl.transaction.xaframework.LogEntryWriterv1.writeLogHeader;
import static org.neo4j.kernel.impl.transaction.xaframework.XaLogicalLogTokens.CLEAN;
import static org.neo4j.kernel.impl.transaction.xaframework.XaLogicalLogTokens.LOG1;
import static org.neo4j.kernel.impl.transaction.xaframework.XaLogicalLogTokens.LOG2;

/**
 * <CODE>XaLogicalLog</CODE> is a transaction and logical log combined. In
 * this log information about the transaction (such as started, prepared and
 * committed) will be written. All commands participating in the transaction
 * will also be written to the log.
 * <p/>
 * Normally you don't have to do anything with this log except open it after it
 * has been instantiated (see {@link XaContainer}). The only method that may be
 * of use when implementing a XA compatible resource is the
 * {@link #getCurrentTxIdentifier}. Leave everything else be unless you know
 * what you're doing.
 * <p/>
 * When the log is opened it will be scanned for uncompleted transactions and
 * those transactions will be re-created. When scan of log is complete all
 * transactions that hasn't entered prepared state will be marked as done
 * (implies rolled back) and dropped. All transactions that have been prepared
 * will be held in memory until the transaction manager tells them to commit.
 * Transaction that already started commit but didn't get flagged as done will
 * be re-committed.
 */
public class XaLogicalLog implements LogLoader
{
    private final LogFilter masterHandler;
    private final LogFilter slaveHandler;
    private StoreChannel fileChannel = null;
    private final ByteBuffer sharedBuffer;
    private LogBuffer writeBuffer = null;
    private long previousLogLastCommittedTx = -1;
    private long logVersion = 0;
    private final ArrayMap<Integer, LogEntry.Start> xidIdentMap = new ArrayMap<>( (byte) 4, false, true );
    private final Map<Integer, XaTransaction> recoveredTxMap = new HashMap<>();
    private int nextIdentifier = 1;
    private boolean scanIsComplete = false;
    private boolean nonCleanShutdown = false;

    private final File fileName;
    private final XaResourceManager xaRm;
    private final XaTransactionFactory xaTf;
    private char currentLog = CLEAN;
    private boolean autoRotate;

    private long rotateAtSize;
    private boolean doingRecovery;

    private long lastRecoveredTx = -1;

    private final StringLogger msgLog;
    private final LogPositionCache positionCache = new LogPositionCache();

    private final FileSystemAbstraction fileSystem;
    private final LogPruneStrategy pruneStrategy;
    private final XaLogicalLogFiles logFiles;
    private final PartialTransactionCopier partialTransactionCopier;

    private final InjectedTransactionValidator injectedTxValidator;

    private final TransactionStateFactory stateFactory;
    // Monitors for counting bytes read/written in various parts
    // We need separate monitors to differentiate between network/disk I/O
    protected final ByteCounterMonitor bufferMonitor;

    protected final ByteCounterMonitor logDeserializerMonitor;
    private final PhysicalLogWriterSPI logWriterSPI;
    private final XaCommandReaderFactory commandReaderFactory;
    private final XaCommandWriterFactory commandWriterFactory;
    private final LogEntryWriterv1 logEntryWriter = new LogEntryWriterv1();

    /** Reusable log translation layer, can ONLY be used if you are synchronized. */
    private final TranslatingEntryConsumer translatingEntryConsumer;

    private final LogReader<ReadableByteChannel> reader;
    private final LogReader<ReadableByteChannel> slaveLogReader;

    /** Reusable done entry */
    private final LogEntry.Done doneEntry = new LogEntry.Done(-1);

    private final KernelHealth kernelHealth;
    private final LogRotationMonitor logRotationMonitor;

    public XaLogicalLog( File fileName, XaResourceManager xaRm, XaCommandReaderFactory commandReaderFactory,
                         XaCommandWriterFactory commandWriterFactory,
                         XaTransactionFactory xaTf, FileSystemAbstraction fileSystem, Monitors monitors,
                         Logging logging, LogPruneStrategy pruneStrategy, TransactionStateFactory stateFactory,
                         KernelHealth kernelHealth, long rotateAtSize, InjectedTransactionValidator injectedTxValidator,
                         Function<List<LogEntry>, List<LogEntry>> interceptor, Function<List<LogEntry>,
            List<LogEntry>> transactionTranslator )
    {
        this.fileName = fileName;
        this.xaRm = xaRm;
        this.commandReaderFactory = commandReaderFactory;
        this.commandWriterFactory = commandWriterFactory;
        this.xaTf = xaTf;
        this.fileSystem = fileSystem;
        this.kernelHealth = kernelHealth;
        this.bufferMonitor = monitors.newMonitor( ByteCounterMonitor.class, XaLogicalLog.class );
        this.logDeserializerMonitor = monitors.newMonitor( ByteCounterMonitor.class, "logdeserializer" );
        this.pruneStrategy = pruneStrategy;
        this.stateFactory = stateFactory;
        this.rotateAtSize = rotateAtSize;
        this.autoRotate = rotateAtSize > 0;
        this.logFiles = new XaLogicalLogFiles( fileName, fileSystem );

        sharedBuffer = ByteBuffer.allocateDirect( 9 + Xid.MAXGTRIDSIZE
                + Xid.MAXBQUALSIZE * 10 );

        msgLog = logging.getMessagesLog( getClass() );

        this.partialTransactionCopier = new PartialTransactionCopier( sharedBuffer, commandReaderFactory,
                commandWriterFactory, msgLog, positionCache, this, logEntryWriter, xidIdentMap );
        this.injectedTxValidator = injectedTxValidator;
        logWriterSPI = new PhysicalLogWriterSPI();
        reader = new LogDeserializer( sharedBuffer, commandReaderFactory );
        slaveLogReader = new SlaveLogDeserializer( sharedBuffer, commandReaderFactory );
        logEntryWriter.setCommandWriter( commandWriterFactory.newInstance() );

        LogApplier applier = new LogApplier();

        PositionCacheLogHandler.SPI positionCacheSPI = new PositionCacheLogHandler.SPI()
        {
            @Override
            public long getLogVersion()
            {
                return XaLogicalLog.this.logVersion;
            }
        };

        masterHandler = new LogFilter( interceptor,
                new MasterLogWriter(
                    new PositionCacheLogHandler( applier, positionCache, positionCacheSPI ),
                    logWriterSPI, injectedTxValidator, logEntryWriter ) );

        slaveHandler = new LogFilter( interceptor,
                new ForgetUnsuccessfulReceivedTransaction(
                new SlaveLogWriter(
                    new PositionCacheLogHandler( applier, positionCache, positionCacheSPI ),
                    logWriterSPI, logEntryWriter ) ) );

        translatingEntryConsumer = new TranslatingEntryConsumer( transactionTranslator );
        logRotationMonitor = monitors.newMonitor( LogRotationMonitor.class, "logicallog" );
    }

    synchronized void open() throws IOException
    {
        switch ( logFiles.determineState() )
        {
            case LEGACY_WITHOUT_LOG_ROTATION:
                open( fileName );
                break;

            case NO_ACTIVE_FILE:
                open( logFiles.getLog1FileName() );
                setActiveLog( LOG1 );
                break;

            case CLEAN:
                File newLog = logFiles.getLog1FileName();
                renameIfExists( newLog );
                renameIfExists( logFiles.getLog2FileName() );
                open( newLog );
                setActiveLog( LOG1 );
                break;

            case DUAL_LOGS_LOG_1_ACTIVE:
                fixDualLogFiles( logFiles.getLog1FileName(), logFiles.getLog2FileName() );
            case LOG_1_ACTIVE:
                currentLog = LOG1;
                open( logFiles.getLog1FileName() );
                break;

            case DUAL_LOGS_LOG_2_ACTIVE:
                fixDualLogFiles( logFiles.getLog2FileName(), logFiles.getLog1FileName() );
            case LOG_2_ACTIVE:
                currentLog = LOG2;
                open( logFiles.getLog2FileName() );
                break;

            default:
                throw new IllegalStateException( "FATAL: Unrecognized logical log state." );
        }

        instantiateCorrectWriteBuffer();
    }

    private void renameIfExists( File fileName ) throws IOException
    {
        if ( fileSystem.fileExists( fileName ) )
        {
            renameLogFileToRightVersion( fileName, fileSystem.getFileSize( fileName ) );
            xaTf.getAndSetNewVersion();
        }
    }

    private void instantiateCorrectWriteBuffer() throws IOException
    {
        writeBuffer = instantiateCorrectWriteBuffer( fileChannel );
    }

    private LogBuffer instantiateCorrectWriteBuffer( StoreChannel channel ) throws IOException
    {
        return new DirectMappedLogBuffer( channel, bufferMonitor );
    }

    private void open( File fileToOpen ) throws IOException
    {
        fileChannel = fileSystem.open( fileToOpen, "rw" );
        if ( new XaLogicalLogRecoveryCheck( fileChannel ).recoveryRequired() )
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

            determineLogVersionFromArchivedFiles();

            long lastTxId = xaTf.getLastCommittedTx();
            writeLogHeader( sharedBuffer, logVersion, lastTxId );
            previousLogLastCommittedTx = lastTxId;
            positionCache.putHeader( logVersion, previousLogLastCommittedTx );
            fileChannel.writeAll( sharedBuffer );
            scanIsComplete = true;
            msgLog.info( openedLogicalLogMessage( fileToOpen, lastTxId, true ) );
        }
    }

    private void determineLogVersionFromArchivedFiles()
    {
        long version = logFiles.determineNextLogVersion(/*default=*/logVersion );
        if(version != logVersion)
        {
            logVersion = version;
            xaTf.setVersion( version );
        }
    }

    private String openedLogicalLogMessage( File fileToOpen, long lastTxId, boolean clean )
    {
        return "Opened logical log [" + fileToOpen + "] version=" + logVersion + ", lastTxId=" +
                lastTxId + " (" + (clean ? "clean" : "recovered") + ")";
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

    /**
     * @param highestKnownCommittedTx is the highest committed tx id when this transaction *started*. This is used
     *                                to perform prepare-time checks that need to know the state of the system when
     *                                the transaction started. Specifically, it is used by constraint validation, to
     *                                ensure that transactions that began before a constraint was enabled are checked
     *                                to ensure they do not violate the constraint.
     */
    // returns identifier for transaction
    // [TX_START][xid[gid.length,bid.lengh,gid,bid]][identifier][format id]
    public synchronized int start( Xid xid, int masterId, int myId, long highestKnownCommittedTx )
    {
        int xidIdent = getNextIdentifier();
        long timeWritten = System.currentTimeMillis();
        LogEntry.Start start = new LogEntry.Start( xid, xidIdent, masterId,
                myId, -1, timeWritten, highestKnownCommittedTx );
        /*
         * We don't write the entry yet. We will store it and hope
         * that when the commands/commit/prepare/done entry are going to be
         * written, we will be asked to write the corresponding entry before.
         */
        xidIdentMap.put( xidIdent, start );
        return xidIdent;
    }

    public synchronized void writeStartEntry( int identifier )
            throws XAException
    {
        kernelHealth.assertHealthy( XAException.class );
        try
        {
            long position = writeBuffer.getFileChannelPosition();
            LogEntry.Start start = xidIdentMap.get( identifier );
            start.setStartPosition( position );
            logEntryWriter.writeLogEntry( start, writeBuffer );
        }
        catch ( IOException e )
        {
            throw Exceptions.withCause( new XAException(
                    "Logical log couldn't write transaction start entry: "
                            + e ), e );
        }
    }

    synchronized Start getStartEntry( int identifier )
    {
        Start start = xidIdentMap.get( identifier );
        if ( start == null )
        {
            throw new IllegalArgumentException( "Start entry for " + identifier + " not found" );
        }
        return start;
    }

    // [TX_PREPARE][identifier]
    public synchronized void prepare( int identifier ) throws XAException
    {
        kernelHealth.assertHealthy( XAException.class );
        LogEntry.Start startEntry = xidIdentMap.get( identifier );
        assert startEntry != null;
        try
        {
            logEntryWriter.writeLogEntry( new LogEntry.Prepare( identifier, System.currentTimeMillis() ), writeBuffer );
            /*
             * Make content visible to all readers of the file channel, so that prepared transactions
             * can be extracted. Not really necessary, since getLogicalLogOrMyselfCommitted() looks for
             * force()d content (which is forced by commit{One,Two}Phase()) and getLogicalLogOrMyselfPrepared()
             * always calls writeOut(). Leaving it here for now.
             */
            writeBuffer.writeOut();
        }
        catch ( IOException e )
        {
            throw Exceptions.withCause( new XAException( "Logical log unable to mark prepare [" + identifier + "] " ),
                    e );
        }
    }

    public void forget( int identifier )
    {
        xidIdentMap.remove( identifier );
    }

    // [DONE][identifier]
    public synchronized void done( int identifier ) throws XAException
    {
        assert xidIdentMap.get( identifier ) != null;
        try
        {
            logEntryWriter.writeLogEntry( doneEntry.reset( identifier ), writeBuffer );
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
            logEntryWriter.writeLogEntry( doneEntry.reset( identifier ), writeBuffer );
        }
        else
        {   // For 1PC
            // TODO Instantiating objects for writing a done entry is insane - fix this ASAP
            InMemoryLogBuffer buffer = new InMemoryLogBuffer();
            logEntryWriter.writeLogEntry( doneEntry.reset(identifier), buffer );
            fileChannel.writeAll( buffer.asByteBuffer() );
        }

        xidIdentMap.remove( identifier );
    }

    // [TX_1P_COMMIT][identifier]
    public synchronized void commitOnePhase( int identifier, long txId, ForceMode forceMode )
            throws XAException
    {
        kernelHealth.assertHealthy( XAException.class );
        LogEntry.Start startEntry = xidIdentMap.get( identifier );
        assert startEntry != null;
        assert txId != -1;
        try
        {
            positionCache.cacheStartPosition( txId, startEntry, logVersion );
            logEntryWriter.writeLogEntry( new LogEntry.OnePhaseCommit( identifier, txId, System.currentTimeMillis()  ),
                    writeBuffer );
            forceMode.force( writeBuffer );
        }
        catch ( IOException e )
        {
            throw Exceptions.withCause(
                    new XAException( "Logical log unable to mark 1P-commit [" + identifier + "] " ), e );
        }
    }

    // [TX_2P_COMMIT][identifier]
    public synchronized void commitTwoPhase( int identifier, long txId, ForceMode forceMode )
            throws XAException
    {
        kernelHealth.assertHealthy( XAException.class );
        LogEntry.Start startEntry = xidIdentMap.get( identifier );
        assert startEntry != null;
        assert txId != -1;
        try
        {
            positionCache.cacheStartPosition( txId, startEntry, logVersion );
            logEntryWriter.writeLogEntry( new LogEntry.TwoPhaseCommit( identifier, txId, System.currentTimeMillis() ),
                    writeBuffer );
            forceMode.force( writeBuffer );
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
        logEntryWriter.writeLogEntry( new LogEntry.Command( identifier, command ), writeBuffer );
    }

    private void registerRecoveredTransaction( long txId )
    {
        if ( doingRecovery )
        {
            lastRecoveredTx = txId;
        }
    }

    private void logRecoveryMessage( String string )
    {
        if ( doingRecovery )
        {
            msgLog.logMessage( string, true );
        }
    }

    private void checkLogRotation() throws IOException
    {
        if ( autoRotate &&
                writeBuffer.getFileChannelPosition() >= rotateAtSize )
        {
            long currentPos = writeBuffer.getFileChannelPosition();
            long firstStartEntry = getFirstStartEntry( currentPos );
            // only rotate if no huge tx is running
            if ( (currentPos - firstStartEntry) < rotateAtSize / 2 )
            {
                rotate();
            }
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
            File newName = getFileName( oldLogHeader[0] );
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
            msgLog.warn( "Failed to truncate log at correct size", e );
        }
        channel.close();
        File newName;
        if ( header == null )
        {
            // header was never written
            newName = new File( getFileName( -1 ).getPath() + "_empty_header_log_" + System.currentTimeMillis());
        }
        else
        {
            newName = getFileName( header[0] );
        }
        if ( !fileSystem.renameFile( logFileName, newName ) )
        {
            throw new IOException( "Failed to rename log to: " + newName );
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
            msgLog.debug( "Logical log: " + fileName + " already closed" );
            return;
        }
        long endPosition = writeBuffer.getFileChannelPosition();
        if ( xidIdentMap.size() > 0 )
        {
            msgLog.info( "Close invoked with " + xidIdentMap.size() +
                    " running transaction(s). " );
            writeBuffer.force();
            fileChannel.close();
            msgLog.info( "Dirty log: " + fileName + "." + currentLog +
                    " now closed. Recovery will be started automatically next " +
                    "time it is opened." );
            return;
        }
        xaTf.flushAll();
        releaseCurrentLogFile();
        char logWas = currentLog;
        if ( currentLog != CLEAN ) // again special case, see above
        {
            setActiveLog( CLEAN );
        }

        File activeLogFileName = new File( fileName.getPath() + "." + logWas);
        renameLogFileToRightVersion( activeLogFileName, endPosition );
        xaTf.getAndSetNewVersion();
        pruneStrategy.prune( this );

        msgLog.info( "Closed log " + fileName );
    }

    static long[] readAndAssertLogHeader( ByteBuffer localBuffer,
                                          ReadableByteChannel channel, long expectedVersion ) throws IOException
    {
        long[] header = VersionAwareLogEntryReader.readLogHeader( localBuffer, channel, false );
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

    private void doInternalRecovery( File logFileName ) throws IOException
    {
        msgLog.info( "Non clean shutdown detected on log [" + logFileName +
                "]. Recovery started ..." );
        // get log creation time
        long[] header = readLogHeader( fileChannel, "Tried to do recovery on log with illegal format version", true );
        if ( header == null )
        {
            msgLog.info( "Unable to read header information, "
                    + "no records in logical log." );
            msgLog.logMessage( "No log version found for " + logFileName, true );
            fileChannel.close();
            boolean success = fileSystem.renameFile( logFileName,
                    new File( logFileName.getPath() + "_unknown_timestamp_" + System.currentTimeMillis() + ".log") );
            assert success;
            fileChannel.close();
            fileChannel = fileSystem.open( logFileName, "rw" );
            return;
        }
        // Even though we use the archived files to tell the next-in-line log version, if there are no files present
        // we need a fallback. By default, we fall back to logVersion, so just set that and then run the relevant logic.
        logVersion = header[0];
        determineLogVersionFromArchivedFiles();

        // If the header contained the wrong version, we need to change it. This can happen during store copy or backup,
        // because those routines create artificial active files to trigger recovery.
        if(header[0] != logVersion)
        {
            ByteBuffer buff = ByteBuffer.allocate( 64 );
            writeLogHeader( buff, logVersion, header[1] );
            fileChannel.writeAll( buff, 0 );
        }

        long lastCommittedTx = header[1];
        previousLogLastCommittedTx = lastCommittedTx;
        positionCache.putHeader( logVersion, previousLogLastCommittedTx );
        msgLog.debug( "[" + logFileName + "] logVersion=" + logVersion +
                      " with committed tx=" + lastCommittedTx );
        fileChannel = new BufferedFileChannel( fileChannel, bufferMonitor );

        RecoveryLogDeserializer reader = new RecoveryLogDeserializer( sharedBuffer, commandReaderFactory );

        EntryCountingLogHandler counter = new EntryCountingLogHandler( new LogApplier() );
        RecoveryConsumer consumer = new RecoveryConsumer( counter );
        boolean success = true;

        consumer.startLog();
        Cursor<LogEntry, IOException> cursor = reader.cursor( fileChannel ); // no try-with-resources, we need the channel open
        try
        {
            while( cursor.next( consumer ) );
        }
        catch ( IOException e )
        {
            success = false;
        }
        finally
        {
            consumer.endLog( success );
        }

        long lastEntryPos = fileChannel.position();
        // make sure we overwrite any broken records
        fileChannel = ((BufferedFileChannel) fileChannel).getSource();
        fileChannel.position( lastEntryPos );

        msgLog.debug( "[" + logFileName + "] entries found=" + counter.getEntriesFound() +
                " lastEntryPos=" + lastEntryPos );

        // zero out the slow way since windows don't support truncate very well
        sharedBuffer.clear();
        while ( sharedBuffer.hasRemaining() )
        {
            sharedBuffer.put( (byte) 0 );
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
            fileChannel.writeAll( sharedBuffer );
            sharedBuffer.flip();
        } while ( fileChannel.position() < endPosition );
        fileChannel.position( lastEntryPos );
        scanIsComplete = true;
        String recoveryCompletedMessage = openedLogicalLogMessage( logFileName, lastRecoveredTx, false );
        msgLog.info( recoveryCompletedMessage );

        xaRm.checkXids();
        if ( xidIdentMap.size() == 0 )
        {
            msgLog.debug( "Recovery on log [" + logFileName + "] completed." );
        }
        else
        {
            msgLog.debug( "Recovery on log [" + logFileName +
                    "] completed with " + xidIdentMap + " prepared transactions found." );
            for ( LogEntry.Start startEntry : xidIdentMap.values() )
            {
                msgLog.debug( "[" + logFileName + "] 2PC xid[" +
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


    private final ArrayMap<Thread, Integer> txIdentMap =
            new ArrayMap<Thread, Integer>( (byte) 5, true, true );

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
        File name = getFileName( version );
        if ( !fileSystem.fileExists( name ) )
        {
            throw new NoSuchLogVersionException( version );
        }
        StoreChannel channel = fileSystem.open( name, "r" );
        channel.position( position );
        return new BufferedFileChannel( channel, bufferMonitor );
    }

    // All calls to this must be synchronized elsewhere
    private void extractPreparedTransactionFromLog( int identifier, StoreChannel logChannel, LogBuffer targetBuffer )
            throws IOException
    {
        LogEntry.Start startEntry = xidIdentMap.get( identifier );
        logChannel.position( startEntry.getStartPosition() );
        long startedAt = sharedBuffer.position();

        SkipPrepareLogEntryWriter consumer = new SkipPrepareLogEntryWriter( identifier, targetBuffer );
        try ( Cursor<LogEntry, IOException> cursor = reader.cursor( logChannel ) )
        {
            while ( cursor.next( consumer ) );
        }

        // position now minus position before is how much we read from disk
        bufferMonitor.bytesRead( sharedBuffer.position() - startedAt );

        if ( !consumer.hasFound() )
        {
            throw new IOException( "Transaction for internal identifier[" + identifier +
                    "] not found in current log" );
        }
    }

    public synchronized ReadableByteChannel getPreparedTransaction( int identifier )
            throws IOException
    {
        StoreChannel logChannel = (StoreChannel) getLogicalLogOrMyselfPrepared( logVersion, 0 );
        InMemoryLogBuffer localBuffer = new InMemoryLogBuffer();
        extractPreparedTransactionFromLog( identifier, logChannel, localBuffer );
        logChannel.close();
        return localBuffer;
    }

    public synchronized void getPreparedTransaction( int identifier, LogBuffer targetBuffer )
            throws IOException
    {
        StoreChannel logChannel = (StoreChannel) getLogicalLogOrMyselfPrepared( logVersion, 0 );
        extractPreparedTransactionFromLog( identifier, logChannel, targetBuffer );
        logChannel.close();
    }

    public LogExtractor getLogExtractor( long startTxId, long endTxIdHint ) throws IOException
    {
        return new LogExtractor( positionCache, this, commandReaderFactory, commandWriterFactory, logEntryWriter,
                startTxId, endTxIdHint );
    }

    public static final int MASTER_ID_REPRESENTING_NO_MASTER = -1;

    public synchronized Pair<Integer, Long> getMasterForCommittedTransaction( long txId ) throws IOException
    {
        if ( txId == 1 )
        {
            return Pair.of( MASTER_ID_REPRESENTING_NO_MASTER, 0L );
        }

        TxPosition cache = positionCache.getStartPosition( txId );
        if ( cache != null )
        {
            return Pair.of( cache.masterId, cache.checksum );
        }

        LogExtractor extractor = getLogExtractor( txId, txId );
        try
        {
            if ( extractor.extractNext( NullLogBuffer.INSTANCE ) != -1 )
            {
                return Pair.of( extractor.getLastStartEntry().getMasterId(), extractor.getLastTxChecksum() );
            }
            throw new NoSuchTransactionException( txId );
        }
        finally
        {
            extractor.close();
        }
    }

    /**
     * Return a file channel over the log file for {@code version} positioned
     * at {@code position}. If the log version is the current one all
     * committed transactions are guaranteed to be present but nothing that
     * hasn't been flushed yet.
     *
     * @param version  The version of the log to get a channel over
     * @param position The position to which to set the channel
     * @return The channel
     * @throws IOException If an IO error occurs when reading the log file
     */
    @Override
    public ReadableByteChannel getLogicalLogOrMyselfCommitted( long version, long position )
            throws IOException
    {
        synchronized ( this )
        {
            if ( version == logVersion )
            {
                File currentLogName = getCurrentLogFileName();
                StoreChannel channel = fileSystem.open( currentLogName, "r" );
                channel.position( position );
                return new BufferedFileChannel( channel, bufferMonitor );
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

    /**
     * Return a file channel over the log file for {@code version} positioned
     * at {@code position}. If the log version is the current one all
     * content is guaranteed to be present, including content just in the write
     * buffer.
     * <p/>
     * Non synchronized, though it accesses writeBuffer. Use this method only
     * through synchronized blocks or trouble will come your way.
     *
     * @param version  The version of the log to get a channel over
     * @param position The position to which to set the channel
     * @return The channel
     * @throws IOException If an IO error occurs when reading the log file
     */
    private ReadableByteChannel getLogicalLogOrMyselfPrepared( long version, long position )
            throws IOException
    {
        if ( version < logVersion )
        {
            return getLogicalLog( version, position );
        }
        else if ( version == logVersion )
        {
            File currentLogName = getCurrentLogFileName();
            StoreChannel channel = fileSystem.open( currentLogName, "r" );
            channel = new BufferedFileChannel( channel, bufferMonitor );
            /*
             * this method is called **during** commit{One,Two}Phase - i.e. before the log buffer
             * is forced and in the case of 1PC without the writeOut() done in prepare (as in 2PC).
             * So, we need to writeOut(). The content of the buffer is written out to the file channel
             * so that the new channel returned above will see the new content. This logical log can
             * continue using the writeBuffer like nothing happened - the data is in the channel and
             * will eventually be forced.
             *
             * See SlaveTxIdGenerator#generate().
             */
            writeBuffer.writeOut();
            channel.position( position );
            return channel;
        }
        else
        {
            throw new RuntimeException( "Version[" + version +
                    "] is higher then current log version[" + logVersion + "]" );
        }
    }

    private File getCurrentLogFileName()
    {
        return currentLog == LOG1 ? logFiles.getLog1FileName() : logFiles.getLog2FileName();
    }

    public long getLogicalLogLength( long version )
    {
        return fileSystem.getFileSize( getFileName( version ) );
    }

    public boolean hasLogicalLog( long version )
    {
        return fileSystem.fileExists( getFileName( version ) );
    }

    public boolean deleteLogicalLog( long version )
    {
        File file = getFileName( version );
        return fileSystem.fileExists( file ) && fileSystem.deleteFile( file );
    }

    /**
     * @param logBasePath should be the log file base name, relative to the neo4j store directory.
     */
    public LogBufferFactory createLogWriter(final Function<Config, File> logBasePath)
    {
        return new LogBufferFactory()
        {
            @Override
            public LogBuffer createActiveLogFile( Config config, long prevCommittedId ) throws IllegalStateException, IOException
            {
                File activeLogFile = new XaLogicalLogFiles( logBasePath.apply( config ), fileSystem ).getLog1FileName();
                StoreChannel channel = fileSystem.create( activeLogFile );
                ByteBuffer scratch = ByteBuffer.allocateDirect( 128 );
                writeLogHeader( scratch, 0, prevCommittedId );
                while(scratch.hasRemaining())
                {
                    channel.writeAll( scratch );
                }
                scratch.clear();
                return new DirectLogBuffer( channel, scratch );
            }
        };
    }

    private long[] readLogHeader( ReadableByteChannel source, String message, boolean strict ) throws IOException
    {
        try
        {
            return VersionAwareLogEntryReader.readLogHeader( sharedBuffer, source, strict );
        }
        catch ( IllegalLogFormatException e )
        {
            msgLog.logMessage( message, e );
            throw e;
        }
    }

    public synchronized void applyTransactionWithoutTxId( ReadableByteChannel byteChannel,
                                                          long nextTxId, ForceMode forceMode ) throws IOException
    {
        kernelHealth.assertHealthy( IOException.class );
        if ( nextTxId != (xaTf.getLastCommittedTx() + 1) )
        {
            throw new IllegalStateException( "Tried to apply tx " +
                    nextTxId + " but expected transaction " +
                    (xaTf.getCurrentVersion() + 1) );
        }

        logRecoveryMessage( "applyTxWithoutTxId log version: " + logVersion +
                ", committing tx=" + nextTxId + ") @ pos " + writeBuffer.getFileChannelPosition() );

        scanIsComplete = false;

        logWriterSPI.bind( forceMode, nextTxId );
        translatingEntryConsumer.bind( getNextIdentifier(), masterHandler );

        boolean success = true;
        masterHandler.startLog();
        try ( Cursor<LogEntry, IOException> cursor = reader.cursor( byteChannel ) )
        {
            while( cursor.next( translatingEntryConsumer ) );
        }
        catch( IOException e )
        {
            kernelHealth.panic( e );
            success = false;
            throw launderedException( IOException.class, "Failure applying transaction", e );
        }
        finally
        {
            masterHandler.endLog( success );
            scanIsComplete = true;
        }
        logRecoveryMessage( "Applied external tx and generated tx id=" + nextTxId );

        checkLogRotation();
    }

    public synchronized void applyTransaction( ReadableByteChannel byteChannel )
            throws IOException
    {
        kernelHealth.assertHealthy( IOException.class );
        scanIsComplete = false;

        translatingEntryConsumer.bind( getNextIdentifier(), slaveHandler );
        boolean success = false;

        slaveHandler.startLog();
        try ( Cursor<LogEntry, IOException> cursor = slaveLogReader.cursor( byteChannel ) )
        {
            while( cursor.next( translatingEntryConsumer ) );
            success = true;
        }
        catch( Exception e )
        {
            kernelHealth.panic( e );
            throw launderedException( IOException.class, "Failure applying transaction", e );
        }
        finally
        {
            try
            {
                slaveHandler.endLog( success );
            }
            catch( Exception e )
            {
                kernelHealth.panic( e );
                throw launderedException( IOException.class, "Failure applying transaction", e );
            }
            scanIsComplete = true;
        }

        checkLogRotation();
    }

    /**
     * Rotates this logical log. The pending transactions are moved over to a
     * new log buffer and the internal structures updated to reflect the new
     * file offsets. Additional side effects include a force() of the store and increment of the log
     * version.
     * <p/>
     * Outline of how rotation happens:
     * <p/>
     * <li>The store is flushed - can't have pending changes if there is no log
     * that contains the commands</li>
     * <p/>
     * <li>Switch current filename with old and check that new doesn't exist and
     * the versioned backup isn't there also</li>
     * <p/>
     * <li>Force the current log buffer</li>
     * <p/>
     * <li>Create new log file, write header</li>
     * <p/>
     * <li>Find the position for the first pending transaction. From there start
     * scanning, transferring the entries of the pending transactions from the
     * old log to the new, updating the start positions in the in-memory tables</li>
     * <p/>
     * <li>Keep or delete old log</li>
     * <p/>
     * <li>Update the log version stored</li>
     * <p/>
     * <li>Instantiate the new log buffer</li>
     *
     * @return the last tx in the produced log
     * @throws IOException I/O error.
     */
    public synchronized long rotate() throws IOException
    {
        File newLogFile = logFiles.getLog2FileName();
        File currentLogFile = logFiles.getLog1FileName();
        char newActiveLog = LOG2;
        final long currentVersion = xaTf.getCurrentVersion();

        logRotationMonitor.rotatingLog( currentVersion );

        rotationMessage( currentVersion, "Log rotation initiated. Starting store flush..." );
        xaTf.flushAll();
        rotationMessage( currentVersion, "Finished store flush. Preparing new log file..." );

        File oldCopy = getFileName( currentVersion );
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
        long endPosition = writeBuffer.getFileChannelPosition();
        rotationMessage( currentVersion,
                "Forcing current log (at position " + endPosition + "). New log file will be " + newLogFile );
        writeBuffer.force();
        rotationMessage( currentVersion, "Log force completed. Writing headers to new log file..." );

        StoreChannel newLog = fileSystem.open( newLogFile, "rw" );
        long lastTx = xaTf.getLastCommittedTx();
        writeLogHeader( sharedBuffer, currentVersion + 1, lastTx );
        previousLogLastCommittedTx = lastTx;
        if ( newLog.write( sharedBuffer ) != 16 )
        {
            throw new IOException( "Unable to write log version to new" );
        }
        fileChannel.position( 0 );
        readAndAssertLogHeader( sharedBuffer, fileChannel, currentVersion );
        fileChannel.position( endPosition );
        if ( xidIdentMap.size() > 0 )
        {
            long firstEntryPosition = getFirstStartEntry( endPosition );
            fileChannel.position( firstEntryPosition );
            rotationMessage( currentVersion,
                    "Rotate log first start entry @ pos=" + firstEntryPosition + " out of " + xidIdentMap );
        }

        rotationMessage( currentVersion,
                "Log header written. Starting migration of on-going transactions..." );
        LogBuffer newLogBuffer = instantiateCorrectWriteBuffer( newLog );
        partialTransactionCopier.copy(
                /*from = */fileChannel, /* to= */newLogBuffer, /* targetLogVersion= */logVersion+1);
        newLogBuffer.force();
        newLog.position( newLogBuffer.getFileChannelPosition() );
        newLog.force( false );
        rotationMessage( currentVersion,
                "Old log scanned, on-going transactions copied and forced. New log position=" + newLog.position() +
                ". Changing log version and setting new log as active..." );

        releaseCurrentLogFile();
        setActiveLog( newActiveLog );
        renameLogFileToRightVersion( currentLogFile, endPosition );
        xaTf.getAndSetNewVersion();
        this.logVersion = xaTf.getCurrentVersion();
        if ( xaTf.getCurrentVersion() != (currentVersion + 1) )
        {
            throw new IOException( "Version change failed, expected " + (currentVersion + 1) + ", but was " +
                    xaTf.getCurrentVersion() );
        }

        rotationMessage( currentVersion, "New log set as active. Pruning archived log files..." );
        pruneStrategy.prune( this );
        rotationMessage( currentVersion, "Archived log files pruned. Completing rotation..." );

        fileChannel = newLog;
        positionCache.putHeader( logVersion, lastTx );
        instantiateCorrectWriteBuffer();
        rotationMessage( currentVersion,
                "Log rotated completed. New log position=" + writeBuffer.getFileChannelPosition() +
                ", version=" + logVersion + " and lastTx=" + previousLogLastCommittedTx + "." );
        return lastTx;
    }

    private void rotationMessage( long currentVersion, String message )
    {
        String line = "Log Rotation [" + currentVersion + "]: " + message;
        msgLog.logMessage( line, true );
    }

    private void assertFileDoesntExist( File file, String description ) throws IOException
    {
        if ( fileSystem.fileExists( file ) )
        {
            throw new IOException( description + ": " + file + " already exist" );
        }
    }

    /**
     * Gets the file position of the first of the start entries searching
     * from {@code endPosition} and to smallest positions.
     *
     * @param endPosition The largest possible position for the Start entries
     * @return The smallest possible position for the Start entries, at most
     *         {@code endPosition}
     */
    private long getFirstStartEntry( long endPosition )
    {
        long firstEntryPosition = endPosition;
        for ( LogEntry.Start entry : xidIdentMap.values() )
        {
            if ( entry.getStartPosition() > 0
                    && entry.getStartPosition() < firstEntryPosition )
            {
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

    @Deprecated
    public void setAutoRotateLogs( boolean autoRotate )
    {
        this.autoRotate = autoRotate;
    }

    @Deprecated
    public boolean isLogsAutoRotated()
    {
        return this.autoRotate;
    }

    @Deprecated
    public void setLogicalLogTargetSize( long size )
    {
        this.rotateAtSize = size;
    }

    @Deprecated
    public long getLogicalLogTargetSize()
    {
        return this.rotateAtSize;
    }

    @Override
    public File getFileName( long version )
    {
        return getHistoryFileName( fileName, version );
    }

    public File getBaseFileName()
    {
        return fileName;
    }

    public Pattern getHistoryFileNamePattern()
    {
        return getHistoryFileNamePattern( fileName.getName() );
    }

    public static Pattern getHistoryFileNamePattern( String baseFileName )
    {
        return Pattern.compile( baseFileName + "\\.v\\d+" );
    }

    public static File getHistoryFileName( File baseFile, long version )
    {
        return new File( baseFile.getPath() + ".v" + version );
    }

    public static long getHistoryLogVersion( File historyLogFile )
    {   // Get version based on the name
        String name = historyLogFile.getName();
        String toFind = ".v";
        int index = name.lastIndexOf( toFind );
        if ( index == -1 )
        {
            throw new RuntimeException( "Invalid log file '" + historyLogFile + "'" );
        }
        return Integer.parseInt( name.substring( index + toFind.length() ) );
    }

    public static long getHighestHistoryLogVersion( FileSystemAbstraction fileSystem, File storeDir, String baseFileName )
    {
        Pattern logFilePattern = getHistoryFileNamePattern( baseFileName );
        long highest = -1;
        for ( File file : fileSystem.listFiles( storeDir ) )
        {
            if ( logFilePattern.matcher( file.getName() ).matches() )
            {
                highest = max( highest, getHistoryLogVersion( file ) );
            }
        }
        return highest;
    }

    public boolean wasNonClean()
    {
        return nonCleanShutdown;
    }

    @Override
    public long getHighestLogVersion()
    {
        return logVersion;
    }

    @Override
    public Long getFirstCommittedTxId( long version )
    {
        if ( version == 0 )
        {
            return 1L;
        }

        // First committed tx for version V = last committed tx version V-1 + 1
        Long header = positionCache.getHeader( version - 1 );
        if ( header != null )
        // It existed in cache
        {
            return header + 1;
        }

        // Wasn't cached, go look for it
        synchronized ( this )
        {
            if ( version > logVersion )
            {
                throw new IllegalArgumentException( "Too high version " + version + ", active is " + logVersion );
            }
            else if ( version == logVersion )
            {
                throw new IllegalArgumentException( "Last committed tx for the active log isn't determined yet" );
            }
            else if ( version == logVersion - 1 )
            {
                return previousLogLastCommittedTx;
            }
            else
            {
                File file = getFileName( version );
                if ( fileSystem.fileExists( file ) )
                {
                    try
                    {
                        long[] headerLongs = VersionAwareLogEntryReader.readLogHeader( fileSystem, file );
                        return headerLongs[1] + 1;
                    }
                    catch ( IOException e )
                    {
                        throw new RuntimeException( e );
                    }
                }
            }
        }
        return null;
    }

    @Override
    public long getLastCommittedTxId()
    {
        return xaTf.getLastCommittedTx();
    }

    @Override
    public Long getFirstStartRecordTimestamp( long version ) throws IOException
    {
        ReadableByteChannel log = null;
        try
        {
            ByteBuffer buffer = LogExtractor.newLogReaderBuffer();
            log = getLogicalLog( version );
            VersionAwareLogEntryReader.readLogHeader( buffer, log, true );
            LogDeserializer deserializer = new LogDeserializer( buffer, commandReaderFactory );

            TimeWrittenConsumer consumer = new TimeWrittenConsumer();

            try ( Cursor<LogEntry, IOException> cursor = deserializer.cursor( log ) )
            {
                while( cursor.next( consumer ) );
            }
            return consumer.getTimeWritten();
        }
        finally
        {
            if ( log != null )
            {
                log.close();
            }
        }
    }

    public class ForgetUnsuccessfulReceivedTransaction extends LogHandler.Filter
    {
        private Start startEntry;

        public ForgetUnsuccessfulReceivedTransaction( LogHandler delegate )
        {
            super( delegate );
        }

        public void setDelegate( LogHandler delegate )
        {
            this.delegate = delegate;
        }

        @Override
        public void startEntry( Start startEntry ) throws IOException
        {
            this.startEntry = startEntry;
            super.startEntry( startEntry );
        }

        @Override
        public void endLog( boolean success ) throws IOException
        {
            try
            {
                super.endLog( success );
            }
            finally
            {
                if ( !success && startEntry != null && xidIdentMap.get( startEntry.getIdentifier() ) != null )
                {   // Unmap this identifier if tx not applied correctly
                    try
                    {
                        xaRm.forget( startEntry.getXid() );
                    }
                    catch ( XAException e )
                    {
                        throw new IOException( e );
                    }
                    finally
                    {
                        xidIdentMap.remove( startEntry.getIdentifier() );
                    }
                }
            }
        }
    }

    public class LogApplier implements LogHandler
    {
        @Override
        public void startLog()
        {
        }

        @Override
        public void startEntry( LogEntry.Start startEntry ) throws IOException
        {
            int identifier = startEntry.getIdentifier();
            if ( identifier >= nextIdentifier )
            {
                nextIdentifier = identifier + 1;
            }
            // re-create the transaction
            Xid xid = startEntry.getXid();
            xidIdentMap.put( identifier, startEntry );
            XaTransaction xaTx = xaTf.create( startEntry.getLastCommittedTxWhenTransactionStarted(),
                    stateFactory.create( null ) );
            xaTx.setIdentifier( identifier );
            xaTx.setRecovered();
            recoveredTxMap.put( identifier, xaTx );
            xaRm.injectStart( xid, xaTx );
            // force to make sure done record is there if 2PC tx and global log
            // marks tx as committed
            // fileChannel.force( false );
        }

        @Override
        public void prepareEntry( LogEntry.Prepare prepareEntry ) throws IOException
        {

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

        @Override
        public void onePhaseCommitEntry( LogEntry.OnePhaseCommit onePhaseCommitEntry ) throws IOException
        {
            int identifier = onePhaseCommitEntry.getIdentifier();
            long txId = onePhaseCommitEntry.getTxId();
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
                positionCache.cacheStartPosition( txId, startEntry, logVersion );
                xaRm.injectOnePhaseCommit( xid );
                registerRecoveredTransaction( txId );
            }
            catch ( XAException e )
            {
                throw new IOException( e );
            }
        }

        @Override
        public void twoPhaseCommitEntry( LogEntry.TwoPhaseCommit twoPhaseCommitEntry ) throws IOException
        {
            int identifier = twoPhaseCommitEntry.getIdentifier();
            long txId = twoPhaseCommitEntry.getTxId();
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
                positionCache.cacheStartPosition( txId, startEntry, logVersion );
                xaRm.injectTwoPhaseCommit( xid );
                registerRecoveredTransaction( txId );
            }
            catch ( XAException e )
            {
                throw new IOException( e );
            }
        }

        @Override
        public void doneEntry( LogEntry.Done doneEntry ) throws IOException
        {
            int identifier = doneEntry.getIdentifier();
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

        @Override
        public void commandEntry( LogEntry.Command commandEntry ) throws IOException
        {
            int identifier = commandEntry.getIdentifier();
            XaCommand command = commandEntry.getXaCommand();
            if ( command == null )
            {
                throw new IOException( "Null command for identifier " + identifier );
            }
            command.setRecovered();
            XaTransaction xaTx = recoveredTxMap.get( identifier );
            xaTx.injectCommand( command );
        }

        @Override
        public void endLog( boolean success ) throws IOException
        {
        }
    }

    private class PhysicalLogWriterSPI implements LogWriter.SPI
    {
        private ForceMode forceMode;
        private long nextTxId;

        @Override
        public LogBuffer getWriteBuffer()
        {
            return writeBuffer;
        }

        @Override
        public void commitTransactionWithoutTxId( LogEntry.Start startEntry ) throws IOException
        {
            if ( startEntry == null )
            {
                throw new IOException( "Unable to find start entry" );
            }
            try
            {
                injectedTxValidator.assertInjectionAllowed( startEntry.getLastCommittedTxWhenTransactionStarted() );
            }
            catch ( XAException e )
            {
                throw new IOException( e );
            }

            LogEntry.OnePhaseCommit commit = new LogEntry.OnePhaseCommit(
                    startEntry.getIdentifier(), nextTxId, System.currentTimeMillis() );

            logEntryWriter.writeLogEntry( commit, writeBuffer );
            // need to manually force since xaRm.commit will not do it (transaction marked as recovered)
            forceMode.force( writeBuffer );
            Xid xid = startEntry.getXid();
            try
            {
                XaTransaction xaTx = xaRm.getXaTransaction( xid );
                xaTx.setCommitTxId( nextTxId );
                positionCache.cacheStartPosition( nextTxId, startEntry, logVersion );
                xaRm.commit( xid, true );
                LogEntry doneEntry = new LogEntry.Done( startEntry.getIdentifier() );
                logEntryWriter.writeLogEntry( doneEntry, writeBuffer );
                xidIdentMap.remove( startEntry.getIdentifier() );
                recoveredTxMap.remove( startEntry.getIdentifier() );
            }
            catch ( XAException e )
            {
                throw new IOException( e );
            }
        }

        public void bind( ForceMode forceMode, long nextTxId )
        {
            this.forceMode = forceMode;
            this.nextTxId = nextTxId;
        }
    }

    private class TimeWrittenConsumer implements Consumer<LogEntry, IOException>
    {
        private long timeWritten = -1;

        @Override
        public boolean accept( LogEntry logEntry ) throws IOException
        {
            if ( logEntry instanceof Start )
            {
                timeWritten = ((Start) logEntry).getTimeWritten();
                return false;
            }
            return true;
        }

        public long getTimeWritten()
        {
            return timeWritten;
        }
    }

    private class SkipPrepareLogEntryWriter implements Consumer<LogEntry, IOException>
    {
        private final int identifier;
        private final LogBuffer targetBuffer;
        private boolean found = false;

        private SkipPrepareLogEntryWriter( int identifier, LogBuffer targetBuffer )
        {
            this.identifier = identifier;
            this.targetBuffer = targetBuffer;
        }

        @Override
        public boolean accept( LogEntry logEntry ) throws IOException
        {
            // TODO For now just skip Prepare entries
            if ( logEntry.getIdentifier() != identifier )
            {
                return true;
            }
            if ( logEntry instanceof LogEntry.Prepare )
            {
                return false;
            }
            if ( logEntry instanceof LogEntry.Start || logEntry instanceof LogEntry.Command )
            {
                logEntryWriter.writeLogEntry( logEntry, targetBuffer );
                found = true;
            }
            else
            {
                throw new RuntimeException( "Expected start or command entry but found: " + logEntry );
            }
            return true;
        }

        public boolean hasFound()
        {
            return found;
        }
    }

    private class RecoveryConsumer implements Consumer<LogEntry, IOException>
    {
        private final EntryCountingLogHandler counter;

        private RecoveryConsumer( EntryCountingLogHandler counter )
        {
            this.counter = counter;
        }

        @Override
        public boolean accept( LogEntry entry ) throws IOException
        {
            switch( entry.getType() )
            {
                case LogEntry.TX_START:
                    counter.startEntry( (Start) entry );
                    break;
                case LogEntry.COMMAND:
                    counter.commandEntry( (LogEntry.Command) entry );
                    break;
                case LogEntry.TX_PREPARE:
                    counter.prepareEntry( (LogEntry.Prepare) entry );
                    break;
                case LogEntry.TX_1P_COMMIT:
                    counter.onePhaseCommitEntry( (LogEntry.OnePhaseCommit) entry );
                    break;
                case LogEntry.TX_2P_COMMIT:
                    counter.twoPhaseCommitEntry( (LogEntry.TwoPhaseCommit) entry );
                    break;
                case LogEntry.DONE:
                    counter.doneEntry( (LogEntry.Done) entry );
                    break;
            }
            return true;
        }

        public void startLog()
        {
            counter.startLog();
        }

        public void endLog( boolean success ) throws IOException
        {
            counter.endLog( success );
        }
    }
}
