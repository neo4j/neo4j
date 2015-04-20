/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.com.storecopy;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.neo4j.com.RequestContext;
import org.neo4j.com.Response;
import org.neo4j.com.ServerFailureException;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.impl.transaction.log.LogRotationControl;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;

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
        void startFlushingEverything();

        void finishFlushingEverything();

        void startStreamingStoreFile( File file );

        void finishStreamingStoreFile( File file );

        void startStreamingStoreFiles();

        void finishStreamingStoreFiles();

        void startStreamingTransactions( long startTxId );

        void finishStreamingTransactions( long endTxId );

        class Adapter implements Monitor
        {
            @Override
            public void startFlushingEverything()
            {   // empty
            }

            @Override
            public void finishFlushingEverything()
            {   // empty
            }

            @Override
            public void startStreamingStoreFile( File file )
            {   // empty
            }

            @Override
            public void finishStreamingStoreFile( File file )
            {   // empty
            }

            @Override
            public void startStreamingStoreFiles()
            {   // empty
            }

            @Override
            public void finishStreamingStoreFiles()
            {   // empty
            }

            @Override
            public void startStreamingTransactions( long startTxId )
            {   // empty
            }

            @Override
            public void finishStreamingTransactions( long endTxId )
            {   // empty
            }
        }
    }

    private final TransactionIdStore transactionIdStore;
    private final NeoStoreDataSource dataSource;
    private final LogRotationControl logRotationControl;
    private final FileSystemAbstraction fileSystem;
    private final File storeDirectory;
    private final Monitor monitor;

    public StoreCopyServer( TransactionIdStore transactionIdStore,
            NeoStoreDataSource dataSource, LogRotationControl logRotationControl, FileSystemAbstraction fileSystem,
            File storeDirectory, Monitor monitor )
    {
        this.transactionIdStore = transactionIdStore;
        this.dataSource = dataSource;
        this.logRotationControl = logRotationControl;
        this.fileSystem = fileSystem;
        this.storeDirectory = getMostCanonicalFile( storeDirectory );
        this.monitor = monitor;
    }

    public Monitor monitor()
    {
        return monitor;
    }

    /**
     * @return a {@link RequestContext} specifying at which point the store copy started.
     */
    public RequestContext flushStoresAndStreamStoreFiles( StoreWriter writer, boolean includeLogs )
    {
        try
        {
            long lastAppliedTransaction = transactionIdStore.getLastClosedTransactionId();
            monitor.startFlushingEverything();
            logRotationControl.forceEverything();
            monitor.finishFlushingEverything();
            ByteBuffer temporaryBuffer = ByteBuffer.allocateDirect( 1024 * 1024 );

            // Copy the store files
            monitor.startStreamingStoreFiles();
            try ( ResourceIterator<File> files = dataSource.listStoreFiles( includeLogs ) )
            {
                while ( files.hasNext() )
                {
                    File file = files.next();
                    try ( StoreChannel fileChannel = fileSystem.open( file, "r" ) )
                    {
                        monitor.startStreamingStoreFile( file );
                        writer.write( relativePath( storeDirectory, file ), fileChannel,
                                temporaryBuffer, file.length() > 0 );
                        monitor.finishStreamingStoreFile( file );
                    }
                }
            }
            finally
            {
                monitor.finishStreamingStoreFiles();
            }

            return anonymous( lastAppliedTransaction );
        }
        catch ( IOException e )
        {
            throw new ServerFailureException( e );
        }
    }
}
