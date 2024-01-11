/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.kernel.api.database.transaction;

import org.eclipse.collections.impl.factory.primitive.LongObjectMaps;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.OptionalLong;
import java.util.concurrent.locks.Lock;

import org.neo4j.io.fs.DelegatingStoreChannel;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.NoSuchTransactionException;
import org.neo4j.kernel.impl.transaction.log.TransactionCursor;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.storageengine.api.MetadataProvider;

import static org.neo4j.util.Preconditions.checkState;
import static org.neo4j.util.Preconditions.requirePositive;

public class TransactionLogServiceImpl implements TransactionLogService
{
    private final LogFiles logFiles;
    private final LogicalTransactionStore transactionStore;
    private final MetadataProvider metadataProvider;

    private final Lock pruneLock;
    private final LogFile logFile;
    private final DatabaseAvailabilityGuard availabilityGuard;

    public TransactionLogServiceImpl( MetadataProvider metadataProvider, LogFiles logFiles, LogicalTransactionStore transactionStore, Lock pruneLock,
            DatabaseAvailabilityGuard availabilityGuard )
    {
        this.metadataProvider = metadataProvider;
        this.logFiles = logFiles;
        this.transactionStore = transactionStore;
        this.pruneLock = pruneLock;
        this.logFile = logFiles.getLogFile();
        this.availabilityGuard = availabilityGuard;
    }

    @Override
    public TransactionLogChannels logFilesChannels( long startingTxId ) throws IOException
    {
        requirePositive( startingTxId );
        LogPosition minimalLogPosition = getLogPosition( startingTxId, false );
        // prevent pruning while we build log channels to avoid cases when we will actually prevent pruning to remove files (on some file systems and OSs),
        // or unexpected exceptions while traversing files
        pruneLock.lock();
        try
        {
            long minimalVersion = minimalLogPosition.getLogVersion();
            var lastCommittedTxId = metadataProvider.getLastCommittedTransactionId();
            var lastCommittedTxIdEndPosition = getLogPosition(lastCommittedTxId, true);
            var channels = collectChannels( startingTxId, minimalLogPosition, minimalVersion, lastCommittedTxId, lastCommittedTxIdEndPosition);
            return new TransactionLogChannels( channels );
        }
        finally
        {
            pruneLock.unlock();
        }
    }

    @Override
    public LogPosition append( ByteBuffer byteBuffer, OptionalLong transactionId ) throws IOException
    {
        checkState( !availabilityGuard.isAvailable(), "Database should not be available." );
        return logFile.append( byteBuffer, transactionId );
    }

    @Override
    public void restore( LogPosition position ) throws IOException
    {
        checkState( !availabilityGuard.isAvailable(), "Database should not be available." );
        logFile.truncate( position );
    }

    private ArrayList<LogChannel> collectChannels( long startingTxId, LogPosition minimalLogPosition, long minimalVersion, long highestTxId,
                                                   LogPosition highestLogPosition ) throws IOException
    {
        var highestLogVersion = highestLogPosition.getLogVersion();
        int exposedChannels = (int) ((highestLogVersion - minimalVersion) + 1);
        var channels = new ArrayList<LogChannel>( exposedChannels );
        var internalChannels = LongObjectMaps.mutable.<StoreChannel>ofInitialCapacity( exposedChannels );
        for ( long version = minimalVersion; version <= highestLogVersion; version++ )
        {
            var startPositionTxId = logFileTransactionId( startingTxId, minimalVersion, version );
            var readOnlyStoreChannel = new ReadOnlyStoreChannel( logFile, version );
            if ( version == minimalVersion )
            {
                readOnlyStoreChannel.position( minimalLogPosition.getByteOffset() );
            }
            internalChannels.put( version, readOnlyStoreChannel );
            var endOffset = version < highestLogVersion ? readOnlyStoreChannel.size() : highestLogPosition.getByteOffset();
            var lastTxId = version < highestLogVersion ? getHeaderLastCommittedTx( version + 1 ) : highestTxId;
            channels.add( new LogChannel( startPositionTxId, readOnlyStoreChannel, endOffset, lastTxId ) );
        }
        logFile.registerExternalReaders( internalChannels );
        return channels;
    }

    private long logFileTransactionId( long startingTxId, long minimalVersion, long version ) throws IOException
    {
        return version == minimalVersion ? startingTxId : getHeaderLastCommittedTx( version ) + 1;
    }

    private long getHeaderLastCommittedTx( long version ) throws IOException
    {
        return logFile.extractHeader( version ).getLastCommittedTxId();
    }

    private LogPosition getLogPosition( long txId, boolean end ) throws IOException
    {
        try ( TransactionCursor transactionCursor = transactionStore.getTransactions( txId ) )
        {
            if ( end )
            {
                transactionCursor.next();
            }
            return transactionCursor.position();
        }
        catch ( NoSuchTransactionException e )
        {
            throw new IllegalArgumentException( "Transaction id " + txId + " not found in transaction logs.", e );
        }
    }

    private static class ReadOnlyStoreChannel extends DelegatingStoreChannel<StoreChannel>
    {
        private final LogFile logFile;
        private final long version;

        ReadOnlyStoreChannel( LogFile logFile, long version ) throws IOException
        {
            super( logFile.openForVersion( version ) );
            this.logFile = logFile;
            this.version = version;
        }

        @Override
        public long write( ByteBuffer[] srcs, int offset, int length ) throws IOException
        {
            throw new UnsupportedOperationException( "Read only channel does not support any write operations." );
        }

        @Override
        public int write( ByteBuffer src ) throws IOException
        {
            throw new UnsupportedOperationException( "Read only channel does not support any write operations." );
        }

        @Override
        public void writeAll( ByteBuffer src ) throws IOException
        {
            throw new UnsupportedOperationException( "Read only channel does not support any write operations." );
        }

        @Override
        public void writeAll( ByteBuffer src, long position ) throws IOException
        {
            throw new UnsupportedOperationException( "Read only channel does not support any write operations." );
        }

        @Override
        public StoreChannel truncate( long size ) throws IOException
        {
            throw new UnsupportedOperationException( "Read only channel does not support any write operations." );
        }

        @Override
        public long write( ByteBuffer[] srcs ) throws IOException
        {
            throw new UnsupportedOperationException( "Read only channel does not support any write operations." );
        }

        @Override
        public void close() throws IOException
        {
            logFile.unregisterExternalReader( version, this );
            super.close();
        }
    }
}
