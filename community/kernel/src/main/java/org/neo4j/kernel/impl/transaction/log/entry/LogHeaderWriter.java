/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.kernel.impl.transaction.log.entry;

import java.io.IOException;

import org.neo4j.io.fs.FlushableChannel;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.memory.HeapScopedBuffer;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.StoreId;

import static org.neo4j.kernel.impl.transaction.log.entry.LogVersions.CURRENT_FORMAT_LOG_HEADER_SIZE;

/**
 * Write the transaction log file header.
 *
 * Current format is
 * <pre>
 *  log version    7 bytes
 *  log format     1 bytes
 *  last committed 8 bytes
 *  store id       40 bytes
 *  reserved       8 bytes
 * </pre>
 */
public class LogHeaderWriter
{
    private static final long LOG_VERSION_BITS = 56;
    static final long LOG_VERSION_MASK = (1L << LOG_VERSION_BITS) - 1;

    private LogHeaderWriter()
    {
    }

    public static void writeLogHeader( FlushableChannel channel, LogHeader logHeader ) throws IOException
    {
        channel.putLong( encodeLogVersion( logHeader.getLogVersion(), logHeader.getLogFormatVersion() ) );
        channel.putLong( logHeader.getLastCommittedTxId() );
        StoreId storeId = logHeader.getStoreId();
        channel.putLong( storeId.getCreationTime() );
        channel.putLong( storeId.getRandomId() );
        channel.putLong( storeId.getStoreVersion() );
        channel.putLong( storeId.getUpgradeTime() );
        channel.putLong( storeId.getUpgradeTxId() );
        channel.putLong( 0 /* reserved */ );
    }

    public static void writeLogHeader( StoreChannel channel, LogHeader logHeader, MemoryTracker memoryTracker ) throws IOException
    {
        try ( var scopedBuffer = new HeapScopedBuffer( CURRENT_FORMAT_LOG_HEADER_SIZE, memoryTracker ) )
        {
            var buffer = scopedBuffer.getBuffer();
            buffer.putLong( encodeLogVersion( logHeader.getLogVersion(), logHeader.getLogFormatVersion() ) );
            buffer.putLong( logHeader.getLastCommittedTxId() );
            StoreId storeId = logHeader.getStoreId();
            buffer.putLong( storeId.getCreationTime() );
            buffer.putLong( storeId.getRandomId() );
            buffer.putLong( storeId.getStoreVersion() );
            buffer.putLong( storeId.getUpgradeTime() );
            buffer.putLong( storeId.getUpgradeTxId() );
            buffer.putLong( 0 /* reserved */ );
            buffer.flip();
            channel.writeAll( buffer );
        }
    }

    public static long encodeLogVersion( long logVersion, long formatVersion )
    {
        return (logVersion & LOG_VERSION_MASK) | (formatVersion << LOG_VERSION_BITS);
    }
}
