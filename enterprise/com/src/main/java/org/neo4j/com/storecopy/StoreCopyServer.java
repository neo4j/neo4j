/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.com.storecopy;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.Optional;

import org.neo4j.com.RequestContext;
import org.neo4j.com.Response;
import org.neo4j.com.ServerFailureException;
import org.neo4j.function.ThrowingAction;
import org.neo4j.graphdb.Resource;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.OpenMode;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.impl.transaction.log.checkpoint.StoreCopyCheckPointMutex;
import org.neo4j.storageengine.api.StoreFileMetadata;

import static org.neo4j.com.RequestContext.anonymous;
import static org.neo4j.io.fs.FileUtils.getMostCanonicalFile;
import static org.neo4j.io.fs.FileUtils.relativePath;

/**
 * Is able to feed store files in a consistent way to a {@link Response} to be picked up by a
 * {@link StoreCopyClient}, for example.
 *
 * @see StoreCopyClient
 */
public class StoreCopyServer
{
    public interface Monitor
    {
        void startTryCheckPoint( String storeCopyIdentifier );

        void finishTryCheckPoint( String storeCopyIdentifier );

        void startStreamingStoreFile( File file, String storeCopyIdentifier );

        void finishStreamingStoreFile( File file, String storeCopyIdentifier );

        void startStreamingStoreFiles( String storeCopyIdentifier );

        void finishStreamingStoreFiles( String storeCopyIdentifier );

        void startStreamingTransactions( long startTxId, String storeCopyIdentifier );

        void finishStreamingTransactions( long endTxId, String storeCopyIdentifier );

        class Adapter implements Monitor
        {
            @Override
            public void startTryCheckPoint( String storeCopyIdentifier )
            {   // empty
            }

            @Override
            public void finishTryCheckPoint( String storeCopyIdentifier )
            {   // empty
            }

            @Override
            public void startStreamingStoreFile( File file, String storeCopyIdentifier )
            {   // empty
            }

            @Override
            public void finishStreamingStoreFile( File file, String storeCopyIdentifier )
            {   // empty
            }

            @Override
            public void startStreamingStoreFiles( String storeCopyIdentifier )
            {   // empty
            }

            @Override
            public void finishStreamingStoreFiles( String storeCopyIdentifier )
            {   // empty
            }

            @Override
            public void startStreamingTransactions( long startTxId, String storeCopyIdentifier )
            {   // empty
            }

            @Override
            public void finishStreamingTransactions( long endTxId, String storeCopyIdentifier )
            {   // empty
            }
        }
    }

    private final NeoStoreDataSource dataSource;
    private final CheckPointer checkPointer;
    private final FileSystemAbstraction fileSystem;
    private final File storeDirectory;
    private final Monitor monitor;
    private final PageCache pageCache;
    private final StoreCopyCheckPointMutex mutex;

    public StoreCopyServer( NeoStoreDataSource dataSource, CheckPointer checkPointer, FileSystemAbstraction fileSystem,
            File storeDirectory, Monitor monitor, PageCache pageCache, StoreCopyCheckPointMutex mutex )
    {
        this.dataSource = dataSource;
        this.checkPointer = checkPointer;
        this.fileSystem = fileSystem;
        this.mutex = mutex;
        this.storeDirectory = getMostCanonicalFile( storeDirectory );
        this.monitor = monitor;
        this.pageCache = pageCache;
    }

    public Monitor monitor()
    {
        return monitor;
    }

    /**
     * Trigger store flush (checkpoint) and write {@link NeoStoreDataSource#listStoreFiles(boolean) store files} to the
     * given {@link StoreWriter}.
     *
     * @param triggerName name of the component asks for store files.
     * @param writer store writer to write files to.
     * @param includeLogs <code>true</code> if transaction logs should be copied, <code>false</code> otherwise.
     * @return a {@link RequestContext} specifying at which point the store copy started.
     */
    public RequestContext flushStoresAndStreamStoreFiles( String triggerName, StoreWriter writer, boolean includeLogs )
    {
        try
        {
            String storeCopyIdentifier = Thread.currentThread().getName();
            ThrowingAction<IOException> checkPointAction = () ->
            {
                monitor.startTryCheckPoint( storeCopyIdentifier );
                checkPointer.tryCheckPoint( new SimpleTriggerInfo( triggerName ) );
                monitor.finishTryCheckPoint( storeCopyIdentifier );
            };

            // Copy the store files
            long lastAppliedTransaction;
            try ( Resource lock = mutex.storeCopy( checkPointAction ); ResourceIterator<StoreFileMetadata> files = dataSource.listStoreFiles( includeLogs ) )
            {
                lastAppliedTransaction = checkPointer.lastCheckPointedTransactionId();
                monitor.startStreamingStoreFiles( storeCopyIdentifier );
                ByteBuffer temporaryBuffer = ByteBuffer.allocateDirect( (int) ByteUnit.mebiBytes( 1 ) );
                while ( files.hasNext() )
                {
                    StoreFileMetadata meta = files.next();
                    File file = meta.file();
                    boolean isLogFile = meta.isLogFile();
                    int recordSize = meta.recordSize();

                    if ( !pageCache.fileSystemSupportsFileOperations() )
                    {
                        // Read from paged file if mapping exists. Otherwise read through file system.
                        // A file is mapped if it is a store, and we have a running database, which will be the case for
                        // both online backup, and when we are the master of an HA cluster.
                        final Optional<PagedFile> optionalPagedFile = pageCache.getExistingMapping( file );
                        if ( optionalPagedFile.isPresent() )
                        {
                            try ( PagedFile pagedFile = optionalPagedFile.get() )
                            {
                                long fileSize = pagedFile.fileSize();
                                try ( ReadableByteChannel fileChannel = pagedFile.openReadableByteChannel() )
                                {
                                    doWrite( writer, temporaryBuffer, file, recordSize, fileChannel, fileSize,
                                            storeCopyIdentifier, false );
                                }
                                continue;
                            }
                        }
                    }

                    try ( ReadableByteChannel fileChannel = fileSystem.open( file, OpenMode.READ ) )
                    {
                        long fileSize = fileSystem.getFileSize( file );
                        doWrite( writer, temporaryBuffer, file, recordSize, fileChannel, fileSize,
                                storeCopyIdentifier, isLogFile );
                    }
                }
            }
            finally
            {
                monitor.finishStreamingStoreFiles( storeCopyIdentifier );
            }

            return anonymous( lastAppliedTransaction );
        }
        catch ( IOException e )
        {
            throw new ServerFailureException( e );
        }
    }

    private void doWrite( StoreWriter writer, ByteBuffer temporaryBuffer, File file, int recordSize,
            ReadableByteChannel fileChannel, long fileSize, String storeCopyIdentifier, boolean isLogFile ) throws IOException
    {
        monitor.startStreamingStoreFile( file, storeCopyIdentifier );
        String path = isLogFile ? file.getName() : relativePath( storeDirectory, file );
        writer.write( path, fileChannel, temporaryBuffer, fileSize > 0, recordSize );
        monitor.finishStreamingStoreFile( file, storeCopyIdentifier );
    }
}
