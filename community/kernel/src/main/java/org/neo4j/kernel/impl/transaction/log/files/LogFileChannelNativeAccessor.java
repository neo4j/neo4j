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

import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.internal.nativeimpl.NativeAccess;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.logging.Log;

public class LogFileChannelNativeAccessor implements ChannelNativeAccessor
{
    private final FileSystemAbstraction fileSystem;
    private final NativeAccess nativeAccess;
    private final Log log;
    private final AtomicLong rotationThreshold;

    public LogFileChannelNativeAccessor( FileSystemAbstraction fileSystem, TransactionLogFilesContext context )
    {
        this.fileSystem = fileSystem;
        this.nativeAccess = context.getNativeAccess();
        this.log = context.getLogProvider().getLog( getClass() );
        this.rotationThreshold = context.getRotationThreshold();
    }

    @Override
    public void adviseSequentialAccessAndKeepInCache( StoreChannel channel, long version )
    {
        if ( channel.isOpen() )
        {
            final int fileDescriptor = fileSystem.getFileDescriptor( channel );
            int result = nativeAccess.tryAdviseSequentialAccess( fileDescriptor );
            if ( result != 0 )
            {
                log.warn( "Unable to advise sequential access for transaction log version: " + version + ". Error code: " + result );
            }
            int keepResult = nativeAccess.tryAdviseToKeepInCache( fileDescriptor );
            if ( keepResult != 0 )
            {
                log.warn( "Unable to advise preserve data in cache for transaction log version: " + version + ". Error code: " + keepResult );
            }
        }
    }

    @Override
    public void evictFromSystemCache( StoreChannel channel, long version )
    {
        if ( channel.isOpen() )
        {
            int result = nativeAccess.tryEvictFromCache( fileSystem.getFileDescriptor( channel ) );
            if ( result != 0 )
            {
                log.warn( "Unable to evict transaction log from cache with version: " + version + ". Error code: " + result );
            }
        }
    }

    @Override
    public void preallocateSpace( StoreChannel storeChannel, long version )
    {
        int fileDescriptor = fileSystem.getFileDescriptor( storeChannel );
        int result = nativeAccess.tryPreallocateSpace( fileDescriptor, rotationThreshold.get() );
        if ( result != 0 )
        {
            log.warn( "Error on attempt to preallocate log file version: " + version + ". Error code: " + result );
        }
    }
}
