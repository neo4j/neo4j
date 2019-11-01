/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.kernel.impl.transaction.log.files;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.LongSupplier;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.memory.ByteBuffers;
import org.neo4j.kernel.impl.transaction.log.LogHeaderCache;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import static org.neo4j.kernel.impl.transaction.log.entry.LogHeaderReader.readLogHeader;

/**
 * Used to figure out what logical log file to open when the database
 * starts up.
 */
public class TransactionLogFiles extends LifecycleAdapter implements LogFiles
{
    public static final FilenameFilter DEFAULT_FILENAME_FILTER = TransactionLogFilesHelper.DEFAULT_FILENAME_FILTER;

    private final TransactionLogFilesContext logFilesContext;
    private final TransactionLogFileInformation logFileInformation;

    private final LogHeaderCache logHeaderCache;
    private final FileSystemAbstraction fileSystem;
    private final TransactionLogFilesHelper fileHelper;
    private final TransactionLogFile logFile;
    private final File logsDirectory;
    private final TransactionLogChannelAllocator channelAllocator;
    private final LogFileChannelNativeAccessor nativeChannelAccessor;

    TransactionLogFiles( File logsDirectory, String name, TransactionLogFilesContext context )
    {
        this.logFilesContext = context;
        this.logsDirectory = logsDirectory;
        this.fileHelper = new TransactionLogFilesHelper( context.getFileSystem(), logsDirectory, name );
        this.fileSystem = context.getFileSystem();
        this.logHeaderCache = new LogHeaderCache( 1000 );
        this.logFileInformation = new TransactionLogFileInformation( this, logHeaderCache, context );
        this.nativeChannelAccessor = new LogFileChannelNativeAccessor( fileSystem, context );
        this.logFile = new TransactionLogFile( this, context );
        this.channelAllocator = new TransactionLogChannelAllocator( logFilesContext, fileHelper, logHeaderCache, nativeChannelAccessor );
    }

    @Override
    public void init() throws IOException
    {
        logFile.init();
    }

    @Override
    public void start() throws IOException
    {
        logFile.start();
    }

    @Override
    public void shutdown() throws IOException
    {
        logFile.shutdown();
    }

    @Override
    public long getLogVersion( File historyLogFile )
    {
        return fileHelper.getLogVersion( historyLogFile );
    }

    @Override
    public File[] logFiles()
    {
        return fileHelper.getLogFiles();
    }

    @Override
    public boolean isLogFile( File file )
    {
        return fileHelper.getLogFilenameFilter().accept( null, file.getName() );
    }

    @Override
    public File logFilesDirectory()
    {
        return logsDirectory;
    }

    @Override
    public File getLogFileForVersion( long version )
    {
        return fileHelper.getLogFileForVersion( version );
    }

    @Override
    public File getHighestLogFile()
    {
        return getLogFileForVersion( getHighestLogVersion() );
    }

    @Override
    public boolean versionExists( long version )
    {
        return fileSystem.fileExists( getLogFileForVersion( version ) );
    }

    @Override
    public LogHeader extractHeader( long version ) throws IOException
    {
        return extractHeader( version, true );
    }

    private LogHeader extractHeader( long version, boolean strict ) throws IOException
    {
        LogHeader logHeader = logHeaderCache.getLogHeader( version );
        if ( logHeader == null )
        {
            logHeader = readLogHeader( fileSystem, getLogFileForVersion( version ), strict );
            if ( !strict && logHeader == null )
            {
                return null;
            }
            logHeaderCache.putHeader( version, logHeader );
        }

        return logHeader;
    }

    @Override
    public boolean hasAnyEntries( long version )
    {
        try
        {
            File logFile = getLogFileForVersion( version );
            var logHeader = extractHeader( version, false );
            if ( logHeader == null )
            {
                return false;
            }
            int headerSize = Math.toIntExact( logHeader.getStartPosition().getByteOffset() );
            if ( fileSystem.getFileSize( logFile ) <= headerSize )
            {
                return false;
            }
            try ( StoreChannel channel = fileSystem.read( logFile ) )
            {
                ByteBuffer buffer = ByteBuffers.allocate( headerSize + 1 );
                channel.readAll( buffer );
                buffer.flip();
                return buffer.get( headerSize ) != 0;
            }
        }
        catch ( IOException e )
        {
            return false;
        }
    }

    @Override
    public long getHighestLogVersion()
    {
        RangeLogVersionVisitor visitor = new RangeLogVersionVisitor();
        accept( visitor );
        return visitor.getHighestVersion();
    }

    @Override
    public long getLowestLogVersion()
    {
        RangeLogVersionVisitor visitor = new RangeLogVersionVisitor();
        accept( visitor );
        return visitor.getLowestVersion();
    }

    @Override
    public void accept( LogVersionVisitor visitor )
    {
        for ( File file : logFiles() )
        {
            visitor.visit( file, getLogVersion( file ) );
        }
    }

    @Override
    public PhysicalLogVersionedStoreChannel openForVersion( long version ) throws IOException
    {
        return channelAllocator.openLogChannel( version );
    }

    /**
     * Creates a new channel for the specified version, creating the backing file if it doesn't already exist.
     * If the file exists then the header is verified to be of correct version. Having an existing file there
     * could happen after a previous crash in the middle of rotation, where the new file was created,
     * but the incremented log version changed hadn't made it to persistent storage.
     *
     * @param version log version for the file/channel to create.
     * @param lastTransactionIdSupplier supplier of last transaction id that was written into previous log file
     * @return {@link PhysicalLogVersionedStoreChannel} for newly created/opened log file.
     * @throws IOException if there's any I/O related error.
     */
    @Override
    public PhysicalLogVersionedStoreChannel createLogChannelForVersion( long version, LongSupplier lastTransactionIdSupplier ) throws IOException
    {
        return channelAllocator.createLogChannel( version, lastTransactionIdSupplier );
    }

    @Override
    public void accept( LogHeaderVisitor visitor ) throws IOException
    {
        // Start from the where we're currently at and go backwards in time (versions)
        long logVersion = getHighestLogVersion();
        long highTransactionId = logFilesContext.getLastCommittedTransactionId();
        while ( versionExists( logVersion ) )
        {
            LogHeader logHeader = extractHeader( logVersion, false );
            if ( logHeader != null )
            {
                long lowTransactionId = logHeader.getLastCommittedTxId() + 1;
                LogPosition position = logHeader.getStartPosition();
                if ( !visitor.visit( logHeader, position, lowTransactionId, highTransactionId ) )
                {
                    break;
                }
                highTransactionId = logHeader.getLastCommittedTxId();
            }
            logVersion--;
        }
    }

    @Override
    public LogFile getLogFile()
    {
        return logFile;
    }

    @Override
    public TransactionLogFileInformation getLogFileInformation()
    {
        return logFileInformation;
    }

    @Override
    public LogFileChannelNativeAccessor getChannelNativeAccessor()
    {
        return nativeChannelAccessor;
    }
}
