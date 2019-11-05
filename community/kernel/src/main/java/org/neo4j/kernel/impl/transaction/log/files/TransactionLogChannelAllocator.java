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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.LongSupplier;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.memory.ByteBuffers;
import org.neo4j.kernel.impl.transaction.log.LogHeaderCache;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeaderWriter;
import org.neo4j.kernel.impl.transaction.tracing.DatabaseTracer;
import org.neo4j.kernel.impl.transaction.tracing.LogFileCreateEvent;

import static java.lang.String.format;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeaderReader.readLogHeader;
import static org.neo4j.kernel.impl.transaction.log.entry.LogVersions.CURRENT_FORMAT_LOG_HEADER_SIZE;
import static org.neo4j.kernel.impl.transaction.log.entry.LogVersions.CURRENT_LOG_FORMAT_VERSION;

class TransactionLogChannelAllocator
{
    private final TransactionLogFilesContext logFilesContext;
    private final FileSystemAbstraction fileSystem;
    private final TransactionLogFilesHelper fileHelper;
    private final LogHeaderCache logHeaderCache;
    private final LogFileChannelNativeAccessor nativeChannelAccessor;
    private final DatabaseTracer databaseTracer;

    TransactionLogChannelAllocator( TransactionLogFilesContext logFilesContext, TransactionLogFilesHelper fileHelper, LogHeaderCache logHeaderCache,
            LogFileChannelNativeAccessor nativeChannelAccessor )
    {
        this.logFilesContext = logFilesContext;
        this.fileSystem = logFilesContext.getFileSystem();
        this.databaseTracer = logFilesContext.getDatabaseTracer();
        this.fileHelper = fileHelper;
        this.logHeaderCache = logHeaderCache;
        this.nativeChannelAccessor = nativeChannelAccessor;
    }

    PhysicalLogVersionedStoreChannel createLogChannel( long version, LongSupplier lastCommittedTransactionId ) throws IOException
    {
        AllocatedFile allocatedFile = allocateFile( version );
        var storeChannel = allocatedFile.getStoreChannel();
        var logFile = allocatedFile.getFile();
        ByteBuffer headerBuffer = ByteBuffers.allocate( CURRENT_FORMAT_LOG_HEADER_SIZE );
        LogHeader header = readLogHeader( headerBuffer, storeChannel, false, logFile );
        if ( header == null )
        {
            try ( LogFileCreateEvent ignored = databaseTracer.createLogFile() )
            {
                // we always write file header from the beginning of the file
                storeChannel.position( 0 );
                long lastTxId = lastCommittedTransactionId.getAsLong();
                LogHeader logHeader = new LogHeader( version, lastTxId, logFilesContext.getStoreId() );
                LogHeaderWriter.writeLogHeader( storeChannel, logHeader );
                logHeaderCache.putHeader( version, logHeader );
            }
        }
        byte formatVersion = header == null ? CURRENT_LOG_FORMAT_VERSION : header.getLogFormatVersion();
        return new PhysicalLogVersionedStoreChannel( storeChannel, version, formatVersion, logFile, nativeChannelAccessor );
    }

    PhysicalLogVersionedStoreChannel openLogChannel( long version ) throws IOException
    {
        File fileToOpen = fileHelper.getLogFileForVersion( version );

        if ( !fileSystem.fileExists( fileToOpen ) )
        {
            throw new FileNotFoundException( fileToOpen.getCanonicalPath() );
        }

        StoreChannel rawChannel = null;
        try
        {
            rawChannel = fileSystem.read( fileToOpen );
            ByteBuffer buffer = ByteBuffers.allocate( CURRENT_FORMAT_LOG_HEADER_SIZE );
            LogHeader header = readLogHeader( buffer, rawChannel, true, fileToOpen );
            if ( (header == null) || (header.getLogVersion() != version) )
            {
                throw new IllegalStateException(
                        format( "Unexpected log file header. Expected header version: %d, actual header: %s", version,
                                header != null ? header.toString() : "null header." ) );
            }
            var versionedStoreChannel = new PhysicalLogVersionedStoreChannel( rawChannel, version, header.getLogFormatVersion(),
                    fileToOpen, nativeChannelAccessor );
            nativeChannelAccessor.adviseSequentialAccessAndKeepInCache( rawChannel, version );
            return versionedStoreChannel;
        }
        catch ( FileNotFoundException cause )
        {
            throw (FileNotFoundException) new FileNotFoundException( fileToOpen.getCanonicalPath() ).initCause( cause );
        }
        catch ( Throwable unexpectedError )
        {
            if ( rawChannel != null )
            {
                // If we managed to open the file before failing, then close the channel
                try
                {
                    rawChannel.close();
                }
                catch ( IOException e )
                {
                    unexpectedError.addSuppressed( e );
                }
            }
            throw unexpectedError;
        }
    }

    private AllocatedFile allocateFile( long version ) throws IOException
    {
        File file = fileHelper.getLogFileForVersion( version );
        boolean fileExist = fileSystem.fileExists( file );
        StoreChannel storeChannel = fileSystem.write( file );
        if ( fileExist )
        {
            nativeChannelAccessor.adviseSequentialAccessAndKeepInCache( storeChannel, version );
        }
        else if ( logFilesContext.getTryPreallocateTransactionLogs().get() )
        {
            nativeChannelAccessor.preallocateSpace( storeChannel, version );
        }
        return new AllocatedFile( file, storeChannel );
    }

    private static class AllocatedFile
    {
        private final File file;
        private final StoreChannel storeChannel;

        AllocatedFile( File file, StoreChannel storeChannel )
        {
            this.file = file;
            this.storeChannel = storeChannel;
        }

        public File getFile()
        {
            return file;
        }

        public StoreChannel getStoreChannel()
        {
            return storeChannel;
        }
    }

}
